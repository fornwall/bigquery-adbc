//! Why a Google Cloud credential could not mint an access token, and what the caller has to
//! change about the credential they configured.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::CredentialsError;
use google_cloud_gax::error::Error as GoogleError;

use crate::BigQueryAuthenticationType;
use crate::BigQueryDatabaseOption;
use crate::config::ADC_SOURCES;

use super::error;

/// Which configured credential a failed token mint belongs to.
///
/// `CredentialsError` does not say, so the driver's credential wrapper (`crate::config::redaction`)
/// records the layer and [`credentials_failure`] picks the remedy from it. A caller with a
/// service-account key is never told to run `gcloud auth application-default login`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialLayer {
    /// The credential `bigquery.auth_type` selects.
    Authentication(BigQueryAuthenticationType),
    /// The impersonation exchange layered on top, which mints the request token once
    /// `bigquery.impersonate.target_principal` is set.
    Impersonation,
}

/// A token service that could not complete an exchange, without retaining its response or URL.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TokenServiceFailure {
    Http(u16),
    Exchange,
}

/// The error-chain entry carrying a [`CredentialLayer`], with the message left after redacting a
/// token-mint failure.
///
/// The layer travels in the error chain because the credential is the only value that reaches
/// both the configuration and the failed request; transports need not know about it.
#[derive(Debug)]
pub(crate) struct CredentialFailure {
    layer: CredentialLayer,
    /// The one diagnostic the wrapper judged safe to keep, when there is one.
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    /// Whether the redacted failure was a Google Workspace reauthentication demand.
    reauthentication: bool,
    /// An unavailable token service is not a verdict on the credential.
    unavailable: Option<TokenServiceFailure>,
}

impl CredentialFailure {
    pub(crate) fn new(
        layer: CredentialLayer,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            layer,
            source,
            reauthentication: false,
            unavailable: None,
        }
    }

    /// Records a transient exchange failure before the original error is redacted.
    pub(crate) fn noting_unavailability(mut self, failure: Option<TokenServiceFailure>) -> Self {
        self.unavailable = failure;
        self
    }

    /// Record whether `redacted`, the failure this replaces, is Google's demand that the user
    /// re-authenticate (`invalid_rapt`). That appears only in the response body the wrapper drops,
    /// and needs its own remedy since no credential setting fixes it.
    pub(crate) fn noting_reauthentication(
        mut self,
        redacted: &(dyn std::error::Error + 'static),
    ) -> Self {
        self.reauthentication =
            chain(redacted).any(|error| error.to_string().contains("invalid_rapt"));
        self
    }
}

impl std::fmt::Display for CredentialFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .write_str("the configured Google Cloud credentials could not mint an access token")
    }
}

impl std::error::Error for CredentialFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Reframe a failure to mint Google Cloud authentication headers, or return `None` for anything
/// else.
///
/// `DefaultCredentialsBuilder::build()` succeeds without any Application Default Credentials (it
/// defers to the metadata server), so a missing credential first fails here, at the first token
/// request. Upstream reports that as generic retry advice without a status, so the driver leads
/// with the remedy and quotes the chain's root cause.
///
/// The remedy comes from the innermost [`CredentialFailure`]: with impersonation there are two
/// credentials, and the innermost marker belongs to the one that failed.
pub(super) fn credentials_failure(context: &str, source: &GoogleError) -> Option<Error> {
    if !is_credentials_failure(source) {
        return None;
    }
    let failure = chain(source)
        .filter_map(|candidate| candidate.downcast_ref::<CredentialFailure>())
        .last();
    // The chain always contains `source`; the fallback keeps error reporting panic-free.
    let root = chain(source).last().unwrap_or(source);
    // A `CredentialsError` or a marker at the root of the chain is the wrapper wording itself,
    // not a cause worth reporting; anything else names the concrete failure the driver hit.
    let detail = if root.is::<CredentialsError>() || root.is::<CredentialFailure>() {
        String::new()
    } else {
        format!(" (underlying failure: {root})")
    };
    if let Some(unavailable) = failure.and_then(|failure| failure.unavailable) {
        // `IO`, which adbc.h gives to an unavailable remote service, so the REST retry loops
        // retry it.
        let cause = match unavailable {
            TokenServiceFailure::Http(code) => {
                format!("the credentials' token service answered HTTP {code}")
            }
            TokenServiceFailure::Exchange => {
                "the configured credentials could not complete a token exchange".to_owned()
            }
        };
        return Some(error(
            format!(
                "{context}: {cause}, a temporary \
                 failure rather than a rejected credential; retry later{detail}"
            ),
            Status::IO,
        ));
    }
    let remedy = match failure {
        Some(failure) if failure.reauthentication => REAUTHENTICATION_REMEDY.to_owned(),
        failure => credential_remedy(failure.map(|failure| failure.layer)),
    };
    Some(error(
        format!("{context}: {remedy}{detail}"),
        Status::Unauthenticated,
    ))
}

/// Google Workspace session control expired the user's sign-in; the Go driver's guidance, reworded.
const REAUTHENTICATION_REMEDY: &str = "Google Workspace requires the credentials' user to \
     sign in again (invalid_rapt); obtain fresh user credentials, such as with `gcloud auth \
     application-default login`, or use a service account, which reauthentication policies do \
     not cover (https://support.google.com/a/answer/9368756)";

/// What to check for a credential that could not mint an access token, naming the options the
/// caller set. `temporary_access_token` and `anonymous` never mint, so they cannot reach here.
fn credential_remedy(layer: Option<CredentialLayer>) -> String {
    let option = BigQueryDatabaseOption::AuthenticationType.as_str();
    match layer {
        Some(CredentialLayer::Impersonation) => format!(
            "the impersonation exchange for {} could not mint an access token; check that the \
             source credentials have roles/iam.serviceAccountTokenCreator on it and on each {} \
             entry, and that {} is at most 1h unless \
             constraints/iam.allowServiceAccountCredentialLifetimeExtension allows more",
            BigQueryDatabaseOption::ImpersonationTargetPrincipal.as_str(),
            BigQueryDatabaseOption::ImpersonationDelegates.as_str(),
            BigQueryDatabaseOption::ImpersonationLifetime.as_str(),
        ),
        Some(CredentialLayer::Authentication(
            BigQueryAuthenticationType::JsonCredentialFile
            | BigQueryAuthenticationType::JsonCredentialString,
        )) => format!(
            "the JSON credentials in {} could not mint an access token; check that the \
             identity still exists, that its key or refresh token is not disabled or revoked, \
             and that the document is as Google issued it",
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        ),
        Some(CredentialLayer::Authentication(BigQueryAuthenticationType::UserAuthentication)) => {
            format!(
                "the refresh token in {} could not be redeemed at the OAuth token endpoint ({}, \
                 or Google's when unset); it may be revoked or expired, or issued to a different \
                 {}",
                BigQueryDatabaseOption::AuthenticationRefreshToken.as_str(),
                BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint.as_str(),
                BigQueryDatabaseOption::AuthenticationClientId.as_str(),
            )
        }
        Some(CredentialLayer::Authentication(BigQueryAuthenticationType::ExternalAccount)) => {
            format!(
                "the external-account token exchange could not mint an access token; check {}, \
                 {} and {}, and that the workload identity pool provider accepts the token {} \
                 returns",
                BigQueryDatabaseOption::AuthenticationExternalAccountAudience.as_str(),
                BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl.as_str(),
                BigQueryDatabaseOption::AuthenticationExternalAccountSubjectTokenType.as_str(),
                BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl.as_str(),
            )
        }
        // Application Default Credentials, and anything the wrapper did not attribute: the
        // configuration the driver discovers for itself, so the remedy is to configure it.
        _ => format!(
            "no usable Google Cloud Application Default Credentials, which are read from \
             {ADC_SOURCES}; run `gcloud auth application-default login`, or set {option} to one \
             of {}",
            credential_sources(),
        ),
    }
}

/// The authentication types that can resolve a missing credential. As advice this omits
/// `anonymous`, which supplies no credential, and the discovering defaults that just failed.
pub(crate) fn credential_sources() -> String {
    crate::options::or_list(
        &[
            BigQueryAuthenticationType::JsonCredentialFile,
            BigQueryAuthenticationType::JsonCredentialString,
            BigQueryAuthenticationType::TemporaryAccessToken,
        ]
        .map(|candidate| format!("{:?}", candidate.as_str())),
    )
}

/// An error and everything it wraps, outermost first.
pub(crate) fn chain<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(source), |error| error.source())
}

/// Whether a failure came from the credential layer rather than from BigQuery.
///
/// The auth retry loop wraps the authentication error in a retry-exhausted error with no kind or
/// status, so the chain is walked; `is_authentication` covers the outermost error.
pub(super) fn is_credentials_failure(source: &GoogleError) -> bool {
    source.is_authentication()
        || chain(source)
            .skip(1)
            .any(|error| error.is::<CredentialsError>())
}

#[cfg(test)]
mod tests;
