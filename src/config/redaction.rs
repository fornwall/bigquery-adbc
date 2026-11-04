//! The wrapper that keeps a remote token exchange's failure from carrying a response body, and
//! that records which of the database's credentials the failure belongs to.

use std::fmt;

use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider};
use google_cloud_gax::error::CredentialsError;
use google_cloud_gax::error::rpc::StatusDetails;
use http::{Extensions, HeaderMap};

use crate::BigQueryAuthenticationType;
use crate::error::{CredentialFailure, CredentialLayer, TokenServiceFailure};

use super::external_account::IdentityProviderError;

/// Wraps `credentials` so a failed token exchange reports only what is safe to report.
///
/// `google-cloud-auth` puts remote response bodies verbatim into `CredentialsError` messages (the
/// STS exchange, the OAuth refresh, the subject-token readers, which print a *successful* body,
/// and impersonation's `generateAccessToken`), and any of them can contain a credential.
/// [`IdentityProviderError`], which the driver builds without a body or URL, passes through, as
/// does IAM's structured refusal of an impersonation exchange ([`iam_refusal`]).
///
/// The wrapper also records `layer`, which `crate::error` uses to choose a remedy, so a failed
/// service-account key is not answered with ADC advice.
pub(crate) fn redacted_credentials(
    credentials: Credentials,
    layer: CredentialLayer,
) -> Credentials {
    Credentials::from(RedactedTokenExchangeCredentials { credentials, layer })
}

struct RedactedTokenExchangeCredentials {
    credentials: Credentials,
    layer: CredentialLayer,
}

impl fmt::Debug for RedactedTokenExchangeCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactedTokenExchangeCredentials")
            .field("credentials", &"<redacted>")
            .field("layer", &self.layer)
            .finish()
    }
}

impl CredentialsProvider for RedactedTokenExchangeCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        self.credentials
            .headers(extensions)
            .await
            .map_err(|source| redact(source, self.layer))
    }

    async fn universe_domain(&self) -> Option<String> {
        self.credentials.universe_domain().await
    }
}

/// Replaces a token-mint failure with the safe account of it, attributed to `layer`.
///
/// A failure already carrying a [`CredentialFailure`] came through an inner wrapper, from the
/// source credential beneath impersonation. It is already redacted and correctly attributed, so it
/// is returned untouched.
fn redact(source: CredentialsError, layer: CredentialLayer) -> CredentialsError {
    if crate::error::chain(&source).any(|candidate| candidate.is::<CredentialFailure>()) {
        return source;
    }
    // Transience is the SDK's own verdict on the whole failure, which already folds in the
    // identity-provider error's: only the message is replaced here.
    let transient = source.is_transient();
    let safe = crate::error::chain(&source)
        .find_map(|candidate| candidate.downcast_ref::<IdentityProviderError>())
        .map(|safe| Box::new(safe.clone()) as Box<dyn std::error::Error + Send + Sync + 'static>)
        .or_else(|| {
            (layer == CredentialLayer::Impersonation)
                .then(|| iam_refusal(&source))
                .flatten()
        });
    let unavailable = transient
        .then(|| {
            answered_status(&source)
                .map(TokenServiceFailure::Http)
                .or_else(|| {
                    // ADC without a local credential probes the metadata server even off Google Cloud.
                    // Keep the missing-ADC remedy there; an explicitly selected credential or an IAM
                    // exchange instead needs its transport to recover, not a replacement credential.
                    (!matches!(
                        layer,
                        CredentialLayer::Authentication(
                            BigQueryAuthenticationType::Default
                                | BigQueryAuthenticationType::ApplicationDefault
                        )
                    ))
                    .then_some(TokenServiceFailure::Exchange)
                })
        })
        .flatten();
    CredentialsError::from_source(
        transient,
        CredentialFailure::new(layer, safe)
            .noting_reauthentication(&source)
            .noting_unavailability(unavailable),
    )
}

/// The HTTP status a token service answered with, if it answered at all.
///
/// ADC connection failures also occur when no credentials or metadata server are available;
/// an HTTP response distinguishes a token-service outage in that case.
fn answered_status(source: &CredentialsError) -> Option<u16> {
    crate::error::chain(source).find_map(|candidate| {
        candidate
            .downcast_ref::<google_cloud_gax::error::Error>()
            .and_then(google_cloud_gax::error::Error::http_status_code)
            .or_else(|| {
                candidate
                    .downcast_ref::<reqwest::Error>()
                    .and_then(reqwest::Error::status)
                    .map(|status| status.as_u16())
            })
            .or_else(|| {
                candidate
                    .downcast_ref::<IdentityProviderError>()
                    .and_then(IdentityProviderError::status)
            })
    })
}

/// IAM's own `google.rpc.Status` verdict on a refused `generateAccessToken` call.
///
/// A refusal body carries no token (only a successful response does) and is the only place the
/// cause is named: a missing `serviceAccountTokenCreator` grant and an IAM Credentials API
/// disabled in the quota project (`SERVICE_DISABLED`) need different fixes.
fn iam_refusal(
    source: &CredentialsError,
) -> Option<Box<dyn std::error::Error + Send + Sync + 'static>> {
    let (code, payload) = crate::error::chain(source).find_map(|candidate| {
        let error = candidate.downcast_ref::<google_cloud_gax::error::Error>()?;
        Some((error.http_status_code()?, error.http_payload()?))
    })?;
    let status = google_cloud_gax::error::rpc::Status::try_from(payload).ok()?;
    let reason = status.details.iter().find_map(|detail| match detail {
        StatusDetails::ErrorInfo(info) => Some(info.reason.as_str()),
        _ => None,
    });
    let mut message = format!("IAM refused generateAccessToken with HTTP {code}");
    if let Some(reason) = reason {
        message.push_str(" (");
        message.push_str(reason);
        message.push(')');
    }
    message.push_str(": ");
    message.push_str(&status.message);
    Some(message.into())
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::Error as GoogleError;

    use super::*;
    use crate::BigQueryAuthenticationType;

    #[test]
    fn transient_transport_failures_preserve_only_the_missing_adc_exception() {
        use BigQueryAuthenticationType as A;
        use adbc_core::error::Status;

        for (layer, expected) in [
            (
                CredentialLayer::Authentication(A::Default),
                Status::Unauthenticated,
            ),
            (
                CredentialLayer::Authentication(A::ApplicationDefault),
                Status::Unauthenticated,
            ),
            (
                CredentialLayer::Authentication(A::UserAuthentication),
                Status::IO,
            ),
            (
                CredentialLayer::Authentication(A::JsonCredentialFile),
                Status::IO,
            ),
            (
                CredentialLayer::Authentication(A::JsonCredentialString),
                Status::IO,
            ),
            (
                CredentialLayer::Authentication(A::ExternalAccount),
                Status::IO,
            ),
            (CredentialLayer::Impersonation, Status::IO),
        ] {
            let source = CredentialsError::from_source(true, std::io::Error::other("secret-url"));
            let redacted = redact(source, layer);
            assert!(redacted.is_transient());
            // A source failure must keep its classification through an outer impersonation layer.
            let redacted = redact(redacted, CredentialLayer::Impersonation);
            for cause in crate::error::chain(&redacted) {
                assert!(!format!("{cause:?} {cause}").contains("secret-url"));
            }
            let failure =
                crate::error::google_error("query failed", GoogleError::authentication(redacted));
            assert_eq!(failure.status, expected, "{layer:?}: {}", failure.message);
            assert_eq!(
                failure
                    .message
                    .contains("could not complete a token exchange"),
                expected == Status::IO
            );
            assert_eq!(
                failure.message.contains("Application Default Credentials"),
                expected == Status::Unauthenticated
            );
        }
    }

    /// The OAuth refresh reports `invalid_rapt` only in the body `redact` drops, so the flag must
    /// be taken before the body goes.
    #[test]
    fn redaction_keeps_a_reauthentication_demand_but_not_the_body() {
        let source = CredentialsError::new(
            false,
            "failed to refresh user access token, body=<{\"error\": \"invalid_grant\", \
             \"error_subtype\": \"invalid_rapt\", \"secret\": \"sentinel\"}>",
            std::io::Error::other("400 Bad Request"),
        );
        let redacted = redact(
            source,
            CredentialLayer::Authentication(BigQueryAuthenticationType::UserAuthentication),
        );
        let message =
            crate::error::google_error("query failed", GoogleError::authentication(redacted))
                .message;
        assert!(
            message.contains("sign in again (invalid_rapt)"),
            "{message}"
        );
        assert!(!message.contains("sentinel"), "{message}");
    }

    /// IAM's refusal body is the only place a disabled IAM Credentials API is named; without it
    /// the caller is sent to check a grant they already hold. The body is shaped as IAM sends it.
    #[test]
    fn impersonation_refusal_keeps_iam_verdict() {
        let payload = bytes::Bytes::from_static(
            br#"{"error": {"code": 403, "status": "PERMISSION_DENIED",
                "message": "IAM Service Account Credentials API has not been used in project p before or it is disabled.",
                "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "SERVICE_DISABLED", "domain": "googleapis.com"}]}}"#,
        );
        let source = CredentialsError::new(
            false,
            "failed to fetch token",
            GoogleError::http(403, HeaderMap::new(), payload),
        );
        let redacted = redact(source, CredentialLayer::Impersonation);
        let failure =
            crate::error::google_error("query failed", GoogleError::authentication(redacted));
        // The refusal's `google.rpc.Status` sits on the scrubbed chain, not on the failure the
        // client reported, so none of its details are forwarded; the message is its one channel.
        assert_eq!(failure.details, None);
        let message = failure.message;
        assert!(
            message.contains("roles/iam.serviceAccountTokenCreator"),
            "{message}"
        );
        assert!(
            message.contains(
                "(underlying failure: IAM refused generateAccessToken with HTTP 403 \
                 (SERVICE_DISABLED): IAM Service Account Credentials API has not been used in \
                 project p before or it is disabled.)"
            ),
            "{message}"
        );
    }
}
