//! Build shared credentials with error redaction, optional impersonation and recovery from a
//! refused token exchange.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::Result;
use google_cloud_auth::credentials::Builder as DefaultCredentialsBuilder;
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentialsBuilder;

use super::credentials::{
    build_json_credentials, build_static_access_token_credentials,
    build_user_authentication_credentials, override_quota_project, read_credential_file,
    require_credentials, with_quota_and_scopes,
};
use super::external_account::build_external_account_credentials;
use super::recovery::recovering_credentials;
use super::redaction::redacted_credentials;
use super::rejections::{application_default_credentials_failure, credential_file_path};
use super::secret::Secret;
use super::{ClientConfiguration, DatabaseConfig};
use crate::error::{CredentialLayer, driver_defect, invalid_argument};
use crate::{BigQueryAuthenticationType, BigQueryDatabaseOption};

impl ClientConfiguration {
    pub(crate) fn build(settings: &DatabaseConfig) -> Result<Self> {
        settings.validate_authentication()?;
        settings.validate_endpoints()?;
        // Read once, so a rebuild reproduces the document validated here instead of reopening
        // a path that may have changed since, or never stop blocking.
        let document = match settings.authentication_type {
            BigQueryAuthenticationType::JsonCredentialFile => {
                let path = credential_file_path(require_credentials(settings)?)?;
                Some(Secret::from(read_credential_file(
                    path,
                    settings.credentials_from_uri,
                )?))
            }
            BigQueryAuthenticationType::JsonCredentialString => {
                Some(Secret::from(require_credentials(settings)?))
            }
            _ => None,
        };
        let (credentials, execution_principal) = credential_stack(settings, document.as_deref())?;
        // Without an exchange there is no failure to recover from.
        let credentials = if exchanges_tokens(settings.authentication_type)
            || settings.impersonation.is_configured()
        {
            let settings = settings.clone();
            recovering_credentials(credentials, move || {
                credential_stack(&settings, document.as_deref()).map(|(credentials, _)| credentials)
            })
        } else {
            credentials
        };
        Ok(Self {
            credentials,
            execution_principal,
            endpoints: settings.endpoints.clone(),
        })
    }
}

/// The credentials `settings` select, with their redaction and impersonation layers, and the
/// principal they act as, if known. `document` is the JSON credential document of the two types
/// that take one.
fn credential_stack(
    settings: &DatabaseConfig,
    document: Option<&str>,
) -> Result<(Credentials, Option<Arc<str>>)> {
    let (mut credentials, mut execution_principal) = match settings.authentication_type {
        BigQueryAuthenticationType::Default | BigQueryAuthenticationType::ApplicationDefault => {
            let builder = with_quota_and_scopes!(
                DefaultCredentialsBuilder::default(),
                settings,
                with_scopes(settings.configured_scopes())
            );
            let credentials = builder.build().map_err(|_| {
                application_default_credentials_failure(
                    std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").as_deref(),
                )
            })?;
            let credentials = match settings.quota_project.as_deref() {
                Some(project) => override_quota_project(credentials, project)?,
                None => credentials,
            };
            (credentials, None)
        }
        BigQueryAuthenticationType::JsonCredentialFile
        | BigQueryAuthenticationType::JsonCredentialString => {
            let document = document.ok_or_else(|| {
                driver_defect(format_args!(
                    "a JSON credential type was built without its document"
                ))
            })?;
            build_json_credentials(settings, document)?
        }
        BigQueryAuthenticationType::UserAuthentication => {
            (build_user_authentication_credentials(settings)?, None)
        }
        BigQueryAuthenticationType::ExternalAccount => {
            build_external_account_credentials(settings)?
        }
        BigQueryAuthenticationType::TemporaryAccessToken => {
            let token = settings.access_token.as_deref().ok_or_else(|| {
                invalid_argument(format!(
                    "BigQuery option {} is required for bigquery.auth_type {:?}; set it to an \
                     OAuth 2.0 access token with the BigQuery scope, which the driver sends \
                     unchanged and never refreshes",
                    BigQueryDatabaseOption::AuthenticationAccessToken.as_str(),
                    BigQueryAuthenticationType::TemporaryAccessToken.as_str(),
                ))
            })?;
            (
                build_static_access_token_credentials(token, settings.quota_project.as_deref())?,
                None,
            )
        }
        BigQueryAuthenticationType::Anonymous => (AnonymousCredentialsBuilder::new().build(), None),
    };
    // Token-exchange errors can contain secrets.
    if exchanges_tokens(settings.authentication_type) {
        credentials = redacted_credentials(
            credentials,
            CredentialLayer::Authentication(settings.authentication_type),
        );
    }
    if settings.impersonation.is_configured() {
        let target = settings
            .impersonation
            .target_principal
            .as_deref()
            .ok_or_else(|| {
                invalid_argument(format!(
                    "BigQuery option {} is required once any of {} or {} is set; set it to the \
                     service account to impersonate, or clear the other impersonation options",
                    BigQueryDatabaseOption::ImpersonationTargetPrincipal.as_str(),
                    BigQueryDatabaseOption::ImpersonationDelegates.as_str(),
                    BigQueryDatabaseOption::ImpersonationLifetime.as_str(),
                ))
            })?;
        credentials = impersonated_credentials(credentials, settings, target)?;
        // Impersonation overrides the source key's principal.
        execution_principal = Some(Arc::from(target));
        // Wrapped for every source: upstream puts the `generateAccessToken` response body into
        // its error, and a refused grant or lifetime is impersonation's failure. A source failure
        // keeps its own attribution.
        credentials = redacted_credentials(credentials, CredentialLayer::Impersonation);
    }
    Ok((credentials, execution_principal))
}

/// Whether the authentication type obtains its token from an exchange; a static token and
/// anonymous credentials perform none.
fn exchanges_tokens(authentication_type: BigQueryAuthenticationType) -> bool {
    !matches!(
        authentication_type,
        BigQueryAuthenticationType::TemporaryAccessToken | BigQueryAuthenticationType::Anonymous
    )
}

/// Layers impersonation of `target` over the source credentials.
fn impersonated_credentials(
    source: Credentials,
    settings: &DatabaseConfig,
    target: &str,
) -> Result<Credentials> {
    let builder = ImpersonatedCredentialsBuilder::from_source_credentials(source)
        .with_target_principal(target);
    // The builder error describes the impersonation settings (target principal, delegates,
    // scopes, lifetime), not the source credentials, so it is safe to include.
    configure_impersonation(builder, settings)
        .build()
        .map_err(|source| {
            invalid_argument(format!(
                "failed to initialize configured impersonated credentials: {source}"
            ))
        })
}

/// Applies the impersonation options other than the target principal.
pub(super) fn configure_impersonation(
    mut builder: ImpersonatedCredentialsBuilder,
    settings: &DatabaseConfig,
) -> ImpersonatedCredentialsBuilder {
    let impersonation = &settings.impersonation;
    // Unlike Go's impersonate package, google-cloud-auth refreshes tokens even with an
    // explicit lifetime. This controls each minted token, not the credential's useful life.
    builder = builder.with_lifetime(impersonation.lifetime.unwrap_or(Duration::from_secs(3_600)));
    if !impersonation.delegates.is_empty() {
        // `generateAccessToken` names each delegate by resource name, which upstream leaves to
        // the caller; Go's `impersonate` package formats the emails the same way.
        builder = builder.with_delegates(
            impersonation
                .delegates
                .iter()
                .map(|delegate| format!("projects/-/serviceAccounts/{delegate}")),
        );
    }
    if !impersonation.scopes.is_empty() {
        builder = builder.with_scopes(impersonation.scopes.iter().map(AsRef::<str>::as_ref));
    }
    if let Some(quota_project) = settings.quota_project.as_deref() {
        builder = builder.with_quota_project_id(quota_project);
    }
    builder
}
