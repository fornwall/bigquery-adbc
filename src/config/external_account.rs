//! dbt's external OAuth Workload Identity Federation credential source.
//!
//! The identity provider requires a form POST, which Google's JSON `credential_source.url`
//! cannot describe. The driver supplies that request through Google's subject-token interface;
//! the SDK's external-account credential handles the STS exchange, impersonation, refresh, and
//! caching.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::Result;
use bytes::Bytes;
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::external_account::ProgrammaticBuilder;
use google_cloud_auth::credentials::subject_token::{
    Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
};
use google_cloud_auth::errors::SubjectTokenProviderError;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};

use super::endpoints::parse_credential_url;
use super::rejections::required_option;
use super::{DatabaseConfig, Secret};
use crate::error::{driver_defect, invalid_argument};
use crate::{BigQueryAuthenticationType, BigQueryDatabaseOption};

/// The two subject-token types the form-POST supplier can produce, the first being the default:
/// a JWT, or the opaque access token an identity provider such as Okta returns for a
/// client-credentials grant. STS documents more (AIP-4117), which belong to credential sources
/// this option family does not describe.
pub(crate) const SUBJECT_TOKEN_TYPES: [&str; 2] = [
    "urn:ietf:params:oauth:token-type:jwt",
    "urn:ietf:params:oauth:token-type:access_token",
];

#[derive(Clone, Debug, Default)]
pub(crate) struct ExternalAccountConfig {
    pub(crate) audience: Option<Arc<str>>,
    pub(crate) request_url: Option<Secret>,
    pub(crate) request_data: Option<Secret>,
    pub(crate) impersonation_url: Option<Arc<str>>,
    /// How STS interprets the subject token; `None` is [`SUBJECT_TOKEN_TYPES`]`[0]`.
    pub(crate) subject_token_type: Option<Arc<str>>,
    /// The STS endpoint; `None` leaves upstream's default, which substitutes the credential's
    /// universe domain into `https://sts.googleapis.com/v1/token`.
    pub(crate) token_url: Option<Arc<str>>,
}

impl ExternalAccountConfig {
    pub(super) fn values(&self) -> [(Option<&str>, BigQueryDatabaseOption); 6] {
        use BigQueryDatabaseOption as O;
        [
            (
                self.audience.as_deref(),
                O::AuthenticationExternalAccountAudience,
            ),
            (
                self.request_url.as_deref(),
                O::AuthenticationExternalAccountRequestUrl,
            ),
            (
                self.request_data.as_deref(),
                O::AuthenticationExternalAccountRequestData,
            ),
            (
                self.impersonation_url.as_deref(),
                O::AuthenticationExternalAccountImpersonationUrl,
            ),
            (
                self.subject_token_type.as_deref(),
                O::AuthenticationExternalAccountSubjectTokenType,
            ),
            (
                self.token_url.as_deref(),
                O::AuthenticationExternalAccountTokenUrl,
            ),
        ]
    }
}

/// Refuses a workforce-pool audience, which this option family cannot complete an exchange for:
/// STS requires the workforce pool user project, and no `bigquery.auth.external_account.*` option
/// carries it. Matched as upstream's `is_valid_workforce_pool_audience` matches it, by the
/// `workforcePools` segment.
pub(crate) fn reject_workforce_pool_audience(key: &str, audience: &str) -> Result<()> {
    if !audience
        .split('/')
        .any(|segment| segment == "workforcePools")
    {
        return Ok(());
    }
    Err(invalid_argument(format!(
        "BigQuery option {key} names a workforce pool, whose token exchange needs a \
         workforce_pool_user_project that no bigquery.auth.external_account option sets; supply \
         the complete external-account credential document as {} with bigquery.auth_type {:?}",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        BigQueryAuthenticationType::JsonCredentialString.as_str(),
    )))
}

/// Refuses a `request_url` that is not a URL at all, where the option is set, without echoing it.
///
/// Only the shape: whether HTTP is permitted depends on `bigquery.allow_untrusted_endpoint`,
/// which may be set later, so [`parse_request_url`] checks that when credentials are built.
pub(crate) fn reject_malformed_request_url(key: &str, value: &str) -> Result<()> {
    if value.is_empty() || parse_credential_url(value).is_some() {
        return Ok(());
    }
    Err(invalid_argument(format!(
        "BigQuery option {key} must be an absolute HTTP or HTTPS URL without user information \
         or a fragment"
    )))
}

pub(super) fn build_external_account_credentials(
    settings: &DatabaseConfig,
) -> Result<(Credentials, Option<Arc<str>>)> {
    let (builder, principal) = external_account_builder(settings)?;
    let credentials = builder.build().map_err(|_| {
        use BigQueryDatabaseOption as O;
        invalid_argument(format!(
            "the Google auth library could not build BigQuery external-account credentials from \
             {}, {} and {}; check their values",
            O::AuthenticationExternalAccountAudience.as_str(),
            O::AuthenticationExternalAccountSubjectTokenType.as_str(),
            O::AuthenticationExternalAccountTokenUrl.as_str(),
        ))
    })?;
    Ok((credentials, principal))
}

/// The configured builder, and the principal its impersonation URL names.
fn external_account_builder<'a>(
    settings: &'a DatabaseConfig,
) -> Result<(ProgrammaticBuilder, Option<Arc<str>>)> {
    use BigQueryDatabaseOption as O;

    let config = &settings.external_account;
    let require = |value: Option<&'a str>, option| {
        required_option(value, option, BigQueryAuthenticationType::ExternalAccount)
    };
    let audience = require(
        config.audience.as_deref(),
        O::AuthenticationExternalAccountAudience,
    )?;
    let request_url = require(
        config.request_url.as_deref(),
        O::AuthenticationExternalAccountRequestUrl,
    )?;
    let request_data = require(
        config.request_data.as_deref(),
        O::AuthenticationExternalAccountRequestData,
    )?;
    let request_url = parse_request_url(request_url, settings.allow_untrusted_endpoint)?;
    let client = reqwest::Client::builder()
        // The POST contains identity-provider credentials. Never forward it to a redirect target.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|source| {
            driver_defect(format_args!(
                "failed to initialize the external-account identity-provider HTTP client: \
                 {source}"
            ))
        })?;
    let provider = IdentityProvider {
        client,
        request_url,
        request_data: Bytes::copy_from_slice(request_data.as_bytes()),
    };
    let mut builder = ProgrammaticBuilder::new(Arc::new(provider))
        .with_audience(audience)
        .with_subject_token_type(
            config
                .subject_token_type
                .as_deref()
                .unwrap_or(SUBJECT_TOKEN_TYPES[0]),
        )
        // The SDK filters permanent credential errors before applying this policy. Its default
        // makes only one attempt; allow transient IdP/STS failures to recover on the first query.
        .with_retry_policy(
            AlwaysRetry
                .with_attempt_limit(3)
                .with_time_limit(Duration::from_secs(30)),
        );
    // Only when configured: upstream's default substitutes the credential's universe domain.
    if let Some(token_url) = config.token_url.as_deref() {
        builder = builder.with_token_url(token_url);
    }
    if let Some(project) = settings.quota_project.as_deref() {
        builder = builder.with_quota_project_id(project);
    }
    // `bigquery.auth.scopes` wins; `bigquery.impersonate.scopes` is the fallback dbt-core 2.0
    // WIF profiles set.
    let scopes = if settings.scopes.is_empty() {
        &settings.impersonation.scopes
    } else {
        &settings.scopes
    };
    if !scopes.is_empty() {
        builder = builder.with_scopes(scopes.iter().map(AsRef::<str>::as_ref));
    }
    let mut principal = None;
    if let Some(url) = config.impersonation_url.as_deref() {
        let (encoded, decoded) = impersonation_principal(url)?;
        builder = builder.with_target_principal(encoded);
        principal = Some(Arc::from(decoded.as_ref()));
    }
    Ok((builder, principal))
}

fn parse_request_url(value: &str, allow_untrusted: bool) -> Result<reqwest::Url> {
    parse_credential_url(value)
        .filter(|url| url.scheme() == "https" || allow_untrusted)
        .ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery option {} must be an absolute HTTPS URL without user information or a \
                 fragment; HTTP requires {}=true",
                BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl.as_str(),
                BigQueryDatabaseOption::AllowUntrustedEndpoint.as_str(),
            ))
        })
}

/// The service account an impersonation URL names: as it appears in the URL, and decoded.
pub(crate) fn impersonation_principal(value: &str) -> Result<(&str, Cow<'_, str>)> {
    const PREFIX: &str = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
    const SUFFIX: &str = ":generateAccessToken";
    let invalid = || {
        invalid_argument(format!(
            "BigQuery option {} must be the standard HTTPS IAM Credentials generateAccessToken URL \
             for a service account",
            BigQueryDatabaseOption::AuthenticationExternalAccountImpersonationUrl.as_str()
        ))
    };
    if reqwest::Url::parse(value).map_err(|_| invalid())?.as_str() != value {
        return Err(invalid());
    }
    let principal = value
        .strip_prefix(PREFIX)
        .and_then(|value| value.strip_suffix(SUFFIX))
        .filter(|principal| !principal.is_empty())
        .ok_or_else(invalid)?;
    let decoded = percent_encoding::percent_decode_str(principal)
        .decode_utf8()
        .map_err(|_| invalid())?;
    if !super::valid_service_account_principal(&decoded) {
        return Err(invalid());
    }
    Ok((principal, decoded))
}

struct IdentityProvider {
    client: reqwest::Client,
    request_url: reqwest::Url,
    request_data: Bytes,
}

impl fmt::Debug for IdentityProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("IdentityProvider { request_url: <redacted>, request_data: <redacted> }")
    }
}

/// Contains no server body, request URL or upstream error: all can echo credential material.
#[derive(Clone, Debug)]
pub(super) struct IdentityProviderError {
    message: &'static str,
    status: Option<reqwest::StatusCode>,
    transient: bool,
}

impl fmt::Display for IdentityProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)?;
        if let Some(status) = self.status {
            write!(formatter, " (HTTP {})", status.as_u16())?;
        }
        Ok(())
    }
}

impl std::error::Error for IdentityProviderError {}

impl SubjectTokenProviderError for IdentityProviderError {
    fn is_transient(&self) -> bool {
        self.transient
    }
}

impl IdentityProviderError {
    /// The HTTP status the identity provider answered with, if it answered.
    pub(super) fn status(&self) -> Option<u16> {
        self.status.map(|status| status.as_u16())
    }

    fn response(message: &'static str) -> Self {
        Self {
            message,
            status: None,
            transient: false,
        }
    }

    /// Always transient: the URL and client are checked up front, so only the network fails here.
    /// A permanent verdict would end upstream's token refresh loop for good.
    const TRANSPORT: Self = Self {
        message: "failed to communicate with the external-account identity provider",
        status: None,
        transient: true,
    };
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
}

impl SubjectTokenProvider for IdentityProvider {
    type Error = IdentityProviderError;

    async fn subject_token(&self) -> std::result::Result<SubjectToken, Self::Error> {
        const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

        let mut response = self
            .client
            .post(self.request_url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(self.request_data.clone())
            .send()
            .await
            .map_err(|_| IdentityProviderError::TRANSPORT)?;
        let status = response.status();
        if !status.is_success() {
            return Err(IdentityProviderError {
                message: "external-account identity provider rejected the token request",
                status: Some(status),
                transient: status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::REQUEST_TIMEOUT
                    || status.is_server_error(),
            });
        }
        // Tokens are small; bound even chunked responses instead of trusting Content-Length.
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| IdentityProviderError::TRANSPORT)?
        {
            if chunk.len() > MAX_RESPONSE_BYTES - body.len() {
                return Err(IdentityProviderError::response(
                    "external-account identity-provider token response exceeds 1 MiB",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let token: TokenResponse = serde_json::from_slice(&body).map_err(|_| {
            IdentityProviderError::response("external-account identity-provider response must contain a JSON access_token string")
        })?;
        if token.access_token.trim().is_empty() {
            return Err(IdentityProviderError::response(
                "external-account identity-provider response has an empty access_token",
            ));
        }
        Ok(SubjectTokenBuilder::new(token.access_token).build())
    }
}

#[cfg(test)]
mod tests;
