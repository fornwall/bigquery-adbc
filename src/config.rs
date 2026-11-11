//! Database settings and validated Google Cloud client configuration.
//!
//! [`DatabaseConfig`] holds the caller's options; [`ClientConfiguration`] validates them and
//! pairs credentials with trusted endpoints. Secret-bearing fields use [`secret::Secret`] to
//! redact derived `Debug` output.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::Result;
use google_cloud_auth::credentials::Credentials;

pub(crate) use crate::config::endpoints::{ServiceEndpointKind, ServiceEndpoints};
use crate::config::endpoints::{
    endpoint_scheme_and_host, is_google_api_host, is_trusted_credential_url, parse_credential_url,
};
use crate::config::external_account::ExternalAccountConfig;
use crate::config::rejections::{
    reject_unused_credential, untrusted_credential_url, untrusted_endpoint, untrusted_uri_endpoint,
};
use crate::config::secret::Secret;
use crate::error::invalid_argument;
use crate::identifiers::ProjectIdentifier;
use crate::{BigQueryAuthenticationType, BigQueryCredentialsType, BigQueryDatabaseOption};

#[derive(Clone, Debug, Default)]
pub(crate) struct UserAuthenticationConfig {
    pub(crate) client_id: Option<Arc<str>>,
    pub(crate) client_secret: Option<Secret>,
    pub(crate) refresh_token: Option<Secret>,
    /// OAuth token endpoint used to redeem the refresh token; `None` uses Google's default.
    pub(crate) access_token_endpoint: Option<Arc<str>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ImpersonationConfig {
    pub(crate) target_principal: Option<Arc<str>>,
    pub(crate) delegates: Vec<Arc<str>>,
    pub(crate) scopes: Vec<Arc<str>>,
    pub(crate) lifetime: Option<Duration>,
}

impl ImpersonationConfig {
    /// Whether any option requires service-account impersonation.
    ///
    /// Scopes alone do not count: dbt 2.0 sets `adbc.bigquery.sql.impersonate.scopes` on
    /// every connection, including those with no impersonation target.
    pub(crate) fn is_configured(&self) -> bool {
        self.configured_option().is_some()
    }

    /// Identify the configured option in rejection messages.
    fn configured_option(&self) -> Option<BigQueryDatabaseOption> {
        if self.target_principal.is_some() {
            Some(BigQueryDatabaseOption::ImpersonationTargetPrincipal)
        } else if !self.delegates.is_empty() {
            Some(BigQueryDatabaseOption::ImpersonationDelegates)
        } else if self.lifetime.is_some() {
            Some(BigQueryDatabaseOption::ImpersonationLifetime)
        } else {
            None
        }
    }
}

/// Values inherited by every connection opened from a BigQuery database.
///
/// Secret-bearing fields must be stored as [`Secret`], which keeps the derived `Debug` output free
/// of credential material. No field holds an empty string: the option setters store an empty
/// value as `None`.
#[derive(Clone, Debug, Default)]
pub(crate) struct DatabaseConfig {
    pub(crate) project_id: Option<ProjectIdentifier>,
    pub(crate) dataset_id: Option<Arc<str>>,
    pub(crate) authentication_type: BigQueryAuthenticationType,
    pub(crate) credentials: Option<Secret>,
    /// Whether `credentials` is a `bigquery://` URI's `AuthCredentials` rather than an explicit
    /// option. A URI is often assembled from input the application does not control, so the
    /// document it supplies, inline or by path, may not have the auth library run a command or
    /// read an unbounded file; see [`Self::endpoint_from_uri`] for the same rule about hosts.
    pub(crate) credentials_from_uri: bool,
    pub(crate) credentials_type: Option<BigQueryCredentialsType>,
    pub(crate) user_authentication: UserAuthenticationConfig,
    pub(crate) external_account: ExternalAccountConfig,
    /// Static OAuth access token for [`BigQueryAuthenticationType::TemporaryAccessToken`].
    pub(crate) access_token: Option<Secret>,
    pub(crate) quota_project: Option<ProjectIdentifier>,
    /// OAuth scopes the credential the authentication type selects is minted with.
    ///
    /// When empty and no impersonation layer is configured, [`ImpersonationConfig::scopes`]
    /// supplies dbt's base scopes. Otherwise an empty list leaves the credential builder's default.
    pub(crate) scopes: Vec<Arc<str>>,
    pub(crate) impersonation: ImpersonationConfig,
    pub(crate) location: Option<Arc<str>>,
    pub(crate) endpoints: ServiceEndpoints,
    /// Whether the effective BigQuery REST endpoint is a `bigquery://` URI's authority rather
    /// than an explicit option. Only an option can trust a new host: a URI is often assembled
    /// from input the application does not control, and `AuthCredentials` can ride along in it.
    pub(crate) endpoint_from_uri: bool,
    /// Permits endpoints and credential destinations outside HTTPS Google API hosts.
    pub(crate) allow_untrusted_endpoint: bool,
    /// Caller text appended to the driver's own user agent; only printable ASCII.
    pub(crate) user_agent: Option<Arc<str>>,
}

/// One cloneable credential value and the endpoints paired with it.
///
/// `Credentials::clone` shares the provider and token cache, so every client built from this
/// value shares one credential state instead of loading ADC again.
#[derive(Clone)]
pub(crate) struct ClientConfiguration {
    pub(crate) credentials: Credentials,
    /// A service-account key's `client_email` or an impersonation target, if known. Not a secret.
    pub(crate) execution_principal: Option<Arc<str>>,
    pub(crate) endpoints: ServiceEndpoints,
}

impl DatabaseConfig {
    /// Explicit base scopes win. Without an impersonation layer, dbt's scope option also applies
    /// to the base credential; a target's scopes must not restrict the source token used for IAM.
    fn configured_scopes(&self) -> impl Iterator<Item = &str> {
        let scopes = if self.scopes.is_empty() && !self.impersonation.is_configured() {
            &self.impersonation.scopes
        } else {
            &self.scopes
        };
        scopes.iter().map(|scope| scope.as_ref())
    }

    /// Reject combinations where a configured credential would be silently ignored.
    ///
    /// Ignoring a credential could fall back to a more privileged ambient identity.
    fn validate_authentication(&self) -> Result<()> {
        use BigQueryAuthenticationType as A;
        use BigQueryDatabaseOption as O;
        const JSON: &[A] = &[A::JsonCredentialFile, A::JsonCredentialString];
        const USER: &[A] = &[A::UserAuthentication];
        const EXTERNAL: &[A] = &[A::ExternalAccount];
        const TOKEN: &[A] = &[A::TemporaryAccessToken];

        let authentication_type = self.authentication_type;
        let user = &self.user_authentication;
        let configured = [
            (
                self.credentials.is_some(),
                O::AuthenticationCredentials,
                JSON,
            ),
            (
                self.credentials_type.is_some(),
                O::AuthenticationCredentialsType,
                JSON,
            ),
            (user.client_id.is_some(), O::AuthenticationClientId, USER),
            (
                user.client_secret.is_some(),
                O::AuthenticationClientSecret,
                USER,
            ),
            (
                user.refresh_token.is_some(),
                O::AuthenticationRefreshToken,
                USER,
            ),
            (
                user.access_token_endpoint.is_some(),
                O::AuthenticationAccessTokenEndpoint,
                USER,
            ),
        ]
        .into_iter()
        .chain(
            self.external_account
                .values()
                .map(|(value, option)| (value.is_some(), option, EXTERNAL)),
        )
        .chain([(
            self.access_token.is_some(),
            O::AuthenticationAccessToken,
            TOKEN,
        )]);
        for (set, option, readers) in configured {
            if set && !readers.contains(&authentication_type) {
                return Err(reject_unused_credential(
                    option,
                    authentication_type,
                    readers,
                ));
            }
        }
        // These types mint no token, so a scope list would be silently dropped.
        if !self.scopes.is_empty()
            && matches!(authentication_type, A::TemporaryAccessToken | A::Anonymous)
        {
            return Err(invalid_argument(format!(
                "BigQuery option {} cannot be used with bigquery.auth_type {:?}, which mints no \
                 token; unset it and supply a token that carries the scopes, or use an \
                 authentication type that mints tokens",
                O::AuthenticationScopes.as_str(),
                authentication_type.as_str(),
            )));
        }
        if authentication_type == A::Anonymous {
            if self.quota_project.is_some() {
                return Err(invalid_argument(format!(
                    "BigQuery option {} cannot be used with bigquery.auth_type {:?}, which \
                     mints no token to attribute quota to; unset the option, or use an \
                     authentication type that mints tokens",
                    O::AuthenticationQuotaProject.as_str(),
                    A::Anonymous.as_str(),
                )));
            }
            if let Some(option) = self.impersonation.configured_option() {
                return Err(invalid_argument(format!(
                    "BigQuery option {} cannot be used with bigquery.auth_type {:?}, which has \
                     no source credential to impersonate with; set bigquery.auth_type to an \
                     authentication type that mints tokens",
                    option.as_str(),
                    A::Anonymous.as_str(),
                )));
            }
            // Accepted alone everywhere else only because dbt 2.0 sets it on every
            // connection, and no dbt profile authenticates anonymously.
            if !self.impersonation.scopes.is_empty() {
                return Err(invalid_argument(format!(
                    "BigQuery option {} cannot be used with bigquery.auth_type {:?}, which \
                     cannot impersonate; unset the option, or use an authentication type that \
                     mints tokens",
                    O::ImpersonationScopes.as_str(),
                    A::Anonymous.as_str(),
                )));
            }
        }
        Ok(())
    }

    /// Restrict the service and credential-exchange endpoints that receive credentials.
    ///
    /// An overridden endpoint still receives a bearer token from the configured credentials, and
    /// upstream never checks it against the credential's universe domain. A BigQuery REST endpoint
    /// set as an option is trusted, including dbt's `api_endpoint`. A URI's authority and every
    /// other service require HTTPS to a Google API host or `bigquery.allow_untrusted_endpoint`.
    fn validate_endpoints(&self) -> Result<()> {
        // An anonymous credential mints no token, so there is nothing an endpoint can capture.
        if self.authentication_type != BigQueryAuthenticationType::Anonymous
            && !self.allow_untrusted_endpoint
        {
            self.validate_service_endpoints()?;
        }
        self.validate_credential_urls()
    }

    /// Hold each service endpoint override to the trust rule [`Self::validate_endpoints`] states.
    /// The values are already normalized, so there is nothing else to check.
    fn validate_service_endpoints(&self) -> Result<()> {
        for kind in ServiceEndpointKind::ALL {
            let Some(endpoint) = self.endpoints.get(kind) else {
                continue;
            };
            let option = kind.option();
            let (scheme, host) = endpoint_scheme_and_host(endpoint).ok_or_else(|| {
                invalid_argument(format!(
                    "BigQuery option {} must be a valid HTTP or HTTPS endpoint",
                    option.as_str()
                ))
            })?;
            let explicit_rest_endpoint =
                kind == ServiceEndpointKind::BigQuery && !self.endpoint_from_uri;
            if explicit_rest_endpoint || (scheme == "https" && is_google_api_host(host)) {
                continue;
            }
            return Err(if kind == ServiceEndpointKind::BigQuery {
                untrusted_uri_endpoint(self.authentication_type, scheme, host)
            } else {
                untrusted_endpoint(option, self.authentication_type, scheme, host)
            });
        }
        Ok(())
    }

    /// Validate credential-exchange URLs, which include paths unlike service endpoints.
    ///
    /// The OAuth token endpoint receives `bigquery.auth.client_secret` and
    /// `bigquery.auth.refresh_token`, a permanent credential rather than a one-hour token; the
    /// Security Token Service URL receives the external-account subject token. The opt-in lifts
    /// only the trust rule: the auth library parses these URLs either way, so they must parse here
    /// first, which also keeps a host that would panic a debug build's parser out (UP-103).
    fn validate_credential_urls(&self) -> Result<()> {
        for (option, configured, exchange, google, legacy_oauth_host) in [
            (
                BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
                self.user_authentication.access_token_endpoint.as_deref(),
                format!(
                    "the refresh-token exchange would send {} and {}",
                    BigQueryDatabaseOption::AuthenticationClientSecret.as_str(),
                    BigQueryDatabaseOption::AuthenticationRefreshToken.as_str(),
                ),
                "https://oauth2.googleapis.com/token",
                true,
            ),
            (
                BigQueryDatabaseOption::AuthenticationExternalAccountTokenUrl,
                self.external_account.token_url.as_deref(),
                format!(
                    "the token exchange would send the subject token {} returns",
                    BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl.as_str(),
                ),
                "https://sts.googleapis.com/v1/token",
                // The Security Token Service has no endpoint on the accounts host, so trusting
                // it here would trust a host this exchange never talks to.
                false,
            ),
        ] {
            let Some(configured) = configured else {
                continue;
            };
            let url = parse_credential_url(configured).ok_or_else(|| {
                invalid_argument(format!(
                    "BigQuery option {} must be an absolute HTTP or HTTPS URL without user \
                     information or a fragment, such as {google}",
                    option.as_str()
                ))
            })?;
            if self.allow_untrusted_endpoint || is_trusted_credential_url(&url, legacy_oauth_host) {
                continue;
            }
            return Err(untrusted_credential_url(
                option,
                url.scheme(),
                url.host_str().unwrap_or_default(),
                &exchange,
                google,
                legacy_oauth_host,
            ));
        }
        Ok(())
    }
}

/// Whether a value is a service-account principal an IAM Credentials URL can carry.
///
/// `google-cloud-auth` interpolates the principal straight into
/// `{endpoint}/v1/projects/-/serviceAccounts/{principal}:{method}`, so a value containing `/`,
/// `?`, `#` or `%` would address a different IAM method or path than the caller asked for.
pub(crate) fn valid_service_account_principal(principal: &str) -> bool {
    !principal.is_empty()
        && principal
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'@' | b'.' | b'-' | b'_'))
}

/// Where Application Default Credentials are read from, in the order the credential builder
/// consults them (AIP-4110 and AIP-4113), named by the failures that drop the builder's own error.
pub(crate) const ADC_SOURCES: &str = if cfg!(windows) {
    "the GOOGLE_APPLICATION_CREDENTIALS environment variable, then \
     %APPDATA%\\gcloud\\application_default_credentials.json, then the Google Cloud metadata \
     server"
} else {
    "the GOOGLE_APPLICATION_CREDENTIALS environment variable, then \
     $HOME/.config/gcloud/application_default_credentials.json, then the Google Cloud metadata \
     server"
};

impl std::fmt::Debug for ClientConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientConfiguration")
            .field("credentials", &"<redacted>")
            .field("execution_principal", &self.execution_principal)
            .field("endpoints", &self.endpoints)
            .finish()
    }
}

mod client;
pub(crate) mod credentials;
pub(crate) mod endpoints;
pub(crate) mod external_account;
mod recovery;
mod redaction;
mod rejections;
pub(crate) mod secret;

#[cfg(test)]
mod tests;
