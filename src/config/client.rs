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
        let (mut credentials, mut execution_principal) = base_layer(settings, document.as_deref())?;
        // Each layer that mints tokens recovers from its own refusal, so a refused impersonation
        // reuses the source token beneath it. Without an exchange there is nothing to recover.
        if exchanges_tokens(settings.authentication_type) {
            let rebuilt = settings.clone();
            let adc_file = is_application_default(settings.authentication_type)
                .then(application_default_credentials_file);
            credentials = recovering_credentials(credentials, move || {
                if let Some(file) = adc_file
                    && application_default_credentials_file() != file
                {
                    return Err(application_default_credentials_moved(file));
                }
                base_layer(&rebuilt, document.as_deref()).map(|(credentials, _)| credentials)
            });
        }
        // `validate_authentication` requires a target whenever impersonation is configured.
        if let Some(target) = settings.impersonation.target_principal.clone() {
            let source = credentials;
            let layer = impersonation_layer(source.clone(), settings, &target)?;
            let rebuilt = settings.clone();
            let rebuilt_target = target.clone();
            credentials = recovering_credentials(layer, move || {
                impersonation_layer(source.clone(), &rebuilt, &rebuilt_target)
            });
            // Impersonation overrides the source key's principal.
            execution_principal = Some(target);
        }
        Ok(Self {
            credentials,
            execution_principal,
            endpoints: settings.endpoints.clone(),
        })
    }
}

/// The credentials `settings` select, with their redaction layer, and the principal they act as,
/// if known. `document` is the JSON credential document of the two types that take one.
fn base_layer(
    settings: &DatabaseConfig,
    document: Option<&str>,
) -> Result<(Credentials, Option<Arc<str>>)> {
    let (mut credentials, execution_principal) = match settings.authentication_type {
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
    Ok((credentials, execution_principal))
}

/// Impersonation of `target` over `source`, with its redaction layer.
fn impersonation_layer(
    source: Credentials,
    settings: &DatabaseConfig,
    target: &str,
) -> Result<Credentials> {
    // Wrapped for every source: upstream puts the `generateAccessToken` response body into its
    // error, and a refused grant or lifetime is impersonation's failure. A source failure keeps
    // its own attribution.
    Ok(redacted_credentials(
        impersonated_credentials(source, settings, target)?,
        CredentialLayer::Impersonation,
    ))
}

fn is_application_default(authentication_type: BigQueryAuthenticationType) -> bool {
    matches!(
        authentication_type,
        BigQueryAuthenticationType::Default | BigQueryAuthenticationType::ApplicationDefault
    )
}

/// Whether Application Default Credentials come from a file rather than the metadata server, as
/// the auth library decides: `GOOGLE_APPLICATION_CREDENTIALS` names one whether or not it exists,
/// and otherwise only the file `gcloud` writes counts if it exists.
///
/// A rebuild after a refusal discovers ADC again, so a new `gcloud auth application-default
/// login` takes effect, but it must not switch source: once `gcloud auth application-default
/// revoke` removes the file, the metadata server would answer off Google Cloud with a transient
/// failure the token cache retries for the database's life, in place of the refusal.
fn application_default_credentials_file() -> bool {
    if std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").is_some() {
        return true;
    }
    let root = if cfg!(windows) { "APPDATA" } else { "HOME" };
    let file = if cfg!(windows) {
        "gcloud/application_default_credentials.json"
    } else {
        ".config/gcloud/application_default_credentials.json"
    };
    std::env::var_os(root).is_some_and(|root| std::path::Path::new(&root).join(file).exists())
}

/// Why a refused ADC credential is kept rather than rebuilt from another source.
fn application_default_credentials_moved(file: bool) -> adbc_core::error::Error {
    let (was, now) = if file {
        ("a credential file", "the metadata server")
    } else {
        ("the metadata server", "a credential file")
    };
    invalid_argument(format!(
        "Application Default Credentials came from {was} but now come from {now}, so the refused \
         credential is kept; open a new database to use {now}"
    ))
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
