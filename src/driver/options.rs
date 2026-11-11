//! The database option surface: setting one, reading one back, and the URI that sets many.

use std::sync::Arc;

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};

use super::BigQueryDatabase;
use crate::config::endpoints::normalize_endpoint;
use crate::config::external_account::{
    SUBJECT_TOKEN_TYPES, impersonation_principal, reject_malformed_request_url,
    reject_workforce_pool_audience,
};
use crate::config::secret::Secret;
use crate::config::{DatabaseConfig, ServiceEndpointKind, valid_service_account_principal};
use crate::driver::uri::{parse_bigquery_uri, redact_uri};
use crate::duration::{format_go_duration, parse_impersonation_lifetime};
use crate::error::{
    invalid_argument, not_found, option_get_refusal, option_not_set, option_set_refusal,
    unknown_or_wrong_type,
};
use crate::identifiers::{ProjectIdentifier, validate_dataset_id};
use crate::options::{
    OptionKind, boolean_option, boolean_value, enum_value, optional_enum_value, string_value,
};
use crate::{BigQueryAuthenticationType, BigQueryDatabaseOption};

impl BigQueryDatabase {
    /// Validate URI options before replacing the configuration; a rejected URI changes nothing.
    fn set_uri(&mut self, uri: &str) -> Result<()> {
        let mut staged = DatabaseConfig::default();
        for (option, value) in parse_bigquery_uri(uri)? {
            apply_option(&mut staged, option, option.as_str(), value)
                // The rejection names the option, which a caller who only set a URI never set.
                .map_err(|mut error| {
                    error
                        .message
                        .push_str("; the value came from the connection URI");
                    error
                })?;
        }
        // Applying the authority and credentials above went through the option path, which marks
        // them explicit.
        staged.endpoint_from_uri = true;
        staged.credentials_from_uri = staged.credentials.is_some();
        self.config = staged;
        // Never retain credentials in the read-back URI.
        self.uri = Some(Secret::from(redact_uri(uri)));
        self.invalidate_clients();
        Ok(())
    }
}

/// Apply one BigQuery database option to `config`, returning whether clients built from the
/// previous configuration are now stale.
///
/// An option set to the empty string clears its value, so `config` never holds an empty string.
fn apply_option(
    config: &mut DatabaseConfig,
    option: BigQueryDatabaseOption,
    key: &str,
    value: String,
) -> Result<bool> {
    use BigQueryDatabaseOption as O;
    match option {
        O::Endpoint
        | O::StorageEndpoint
        | O::GcsEndpoint
        | O::DataprocEndpoint
        | O::VertexEndpoint => {
            let kind = service_endpoint(option);
            config
                .endpoints
                .set(kind, normalize_endpoint(value, key)?.map(Arc::from));
            if kind == ServiceEndpointKind::BigQuery {
                config.endpoint_from_uri = false;
            }
        }
        // Project, dataset and location affect new connections, not cached clients.
        O::ProjectId => {
            config.project_id = project_identifier(key, value)?;
            return Ok(false);
        }
        O::DatasetId => {
            validate_dataset_id(&format!("BigQuery option {key}"), &value)?;
            config.dataset_id = nonempty_arc(value);
            return Ok(false);
        }
        O::Location => {
            reject_whitespace(
                key,
                &value,
                "a location name such as US, EU, or us-central1",
            )?;
            config.location = nonempty_arc(value);
            return Ok(false);
        }
        O::AllowUntrustedEndpoint => {
            config.allow_untrusted_endpoint = boolean_value(key, OptionValue::String(value))?;
        }
        O::AuthenticationType => {
            config.authentication_type = enum_value(key, OptionValue::String(value))?;
        }
        O::AuthenticationCredentials => {
            config.credentials = nonempty_secret(value);
            config.credentials_from_uri = false;
        }
        O::AuthenticationCredentialsType => {
            config.credentials_type = optional_enum_value(key, OptionValue::String(value))?;
        }
        O::AuthenticationClientId => {
            config.user_authentication.client_id = nonempty_arc(value);
        }
        O::AuthenticationClientSecret => {
            config.user_authentication.client_secret = nonempty_secret(value);
        }
        O::AuthenticationRefreshToken => {
            config.user_authentication.refresh_token = nonempty_secret(value);
        }
        O::AuthenticationAccessToken => config.access_token = nonempty_secret(value),
        O::AuthenticationAccessTokenEndpoint => {
            reject_whitespace(key, &value, "a token endpoint URL")?;
            config.user_authentication.access_token_endpoint = nonempty_arc(value);
        }
        O::AuthenticationQuotaProject => config.quota_project = project_identifier(key, value)?,
        O::AuthenticationScopes => {
            config.scopes = parse_comma_separated(key, &value, "OAuth scope URLs")?;
        }
        O::AuthenticationExternalAccountAudience => {
            reject_whitespace(key, &value, "a workload identity pool provider resource")?;
            reject_workforce_pool_audience(key, &value)?;
            config.external_account.audience = nonempty_arc(value);
        }
        O::AuthenticationExternalAccountRequestUrl => {
            reject_malformed_request_url(key, &value)?;
            config.external_account.request_url = nonempty_secret(value);
        }
        O::AuthenticationExternalAccountRequestData => {
            config.external_account.request_data = nonempty_secret(value);
        }
        O::AuthenticationExternalAccountImpersonationUrl => {
            if !value.is_empty() {
                impersonation_principal(&value)?;
            }
            config.external_account.impersonation_url = nonempty_arc(value);
        }
        O::AuthenticationExternalAccountSubjectTokenType => {
            if !value.is_empty() && !SUBJECT_TOKEN_TYPES.contains(&value.as_str()) {
                // Complete external-account documents support additional STS token types.
                return Err(invalid_argument(format!(
                    "BigQuery option {key} must be {:?} or {:?}; for another subject-token \
                     type, set a complete external-account credential document as {} with \
                     bigquery.auth_type {:?}",
                    SUBJECT_TOKEN_TYPES[0],
                    SUBJECT_TOKEN_TYPES[1],
                    O::AuthenticationCredentials.as_str(),
                    BigQueryAuthenticationType::JsonCredentialString.as_str(),
                )));
            }
            config.external_account.subject_token_type = nonempty_arc(value);
        }
        O::AuthenticationExternalAccountTokenUrl => {
            reject_whitespace(key, &value, "a token endpoint URL")?;
            config.external_account.token_url = nonempty_arc(value);
        }
        O::ImpersonationTargetPrincipal => {
            if !value.is_empty() && !valid_service_account_principal(&value) {
                return Err(invalid_service_account_principal(key, None));
            }
            config.impersonation.target_principal = nonempty_arc(value);
        }
        O::ImpersonationDelegates => {
            let delegates = parse_comma_separated(key, &value, "service account email addresses")?;
            if let Some(index) = delegates
                .iter()
                .position(|delegate| !valid_service_account_principal(delegate))
            {
                return Err(invalid_service_account_principal(key, Some(index + 1)));
            }
            config.impersonation.delegates = delegates;
        }
        O::ImpersonationScopes => {
            config.impersonation.scopes = parse_comma_separated(key, &value, "OAuth scope URLs")?;
        }
        O::ImpersonationLifetime => {
            config.impersonation.lifetime = if value.is_empty() {
                None
            } else {
                Some(parse_impersonation_lifetime(key, &value)?)
            };
        }
        O::UserAgent => {
            // What an HTTP header value admits, short of tabs and non-ASCII bytes that some
            // proxies mangle.
            if !value
                .bytes()
                .all(|byte| byte == b' ' || byte.is_ascii_graphic())
            {
                return Err(invalid_argument(format!(
                    "BigQuery option {key} must be printable ASCII text, such as my-app/1.0"
                )));
            }
            config.user_agent = nonempty_arc(value);
        }
    }
    Ok(true)
}

/// Report one BigQuery database option. An unset option is reported by its key, which is all a
/// caller behind a driver manager has.
fn read_option(config: &DatabaseConfig, option: BigQueryDatabaseOption) -> Result<String> {
    use BigQueryDatabaseOption as O;
    let user = &config.user_authentication;
    let external = &config.external_account;
    let impersonation = &config.impersonation;
    let value = match option {
        O::Endpoint
        | O::StorageEndpoint
        | O::GcsEndpoint
        | O::DataprocEndpoint
        | O::VertexEndpoint => config.endpoints.get(service_endpoint(option)),
        O::AuthenticationCredentials
        | O::AuthenticationClientSecret
        | O::AuthenticationRefreshToken
        | O::AuthenticationAccessToken
        | O::AuthenticationExternalAccountRequestUrl
        | O::AuthenticationExternalAccountRequestData => {
            return Err(not_found(format!(
                "BigQuery option {} is write-only and cannot be read back",
                option.as_str()
            )));
        }
        O::ProjectId => config.project_id.as_deref(),
        O::DatasetId => config.dataset_id.as_deref(),
        O::Location => config.location.as_deref(),
        O::AllowUntrustedEndpoint => return Ok(boolean_option(config.allow_untrusted_endpoint)),
        O::AuthenticationType => Some(config.authentication_type.as_str()),
        O::AuthenticationCredentialsType => config.credentials_type.map(|value| value.as_str()),
        O::AuthenticationClientId => user.client_id.as_deref(),
        O::AuthenticationAccessTokenEndpoint => user.access_token_endpoint.as_deref(),
        O::AuthenticationQuotaProject => config.quota_project.as_deref(),
        O::AuthenticationScopes => return configured_list(option, &config.scopes),
        O::AuthenticationExternalAccountAudience => external.audience.as_deref(),
        O::AuthenticationExternalAccountImpersonationUrl => external.impersonation_url.as_deref(),
        O::AuthenticationExternalAccountSubjectTokenType => external.subject_token_type.as_deref(),
        O::AuthenticationExternalAccountTokenUrl => external.token_url.as_deref(),
        O::ImpersonationTargetPrincipal => impersonation.target_principal.as_deref(),
        O::ImpersonationDelegates => return configured_list(option, &impersonation.delegates),
        O::ImpersonationScopes => return configured_list(option, &impersonation.scopes),
        O::UserAgent => config.user_agent.as_deref(),
        // Go reports the one-hour default whenever impersonation is configured.
        O::ImpersonationLifetime => match impersonation.lifetime {
            Some(lifetime) => return Ok(format_go_duration(lifetime)),
            None => impersonation.is_configured().then_some("1h0m0s"),
        },
    };
    value
        .map(str::to_owned)
        .ok_or_else(|| option_not_set(option.as_str()))
}

impl Optionable for BigQueryDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if key == OptionDatabase::Uri {
            return self.set_uri(&string_value(key.as_ref(), value)?);
        }
        match database_option_key(&key) {
            Some(option) => {
                let value = string_value(key.as_ref(), value)?;
                if apply_option(&mut self.config, option, key.as_ref(), value)? {
                    self.invalidate_clients();
                }
                Ok(())
            }
            None => Err(option_set_refusal("database", key.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        if key == OptionDatabase::Uri {
            return self
                .uri
                .as_deref()
                .map(str::to_owned)
                .ok_or_else(|| option_not_set(key.as_ref()));
        }
        match database_option_key(&key) {
            Some(option) => read_option(&self.config, option),
            None => Err(option_get_refusal("database", key.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(unknown_or_wrong_type(
            "database",
            key.as_ref(),
            database_option_kind(&key),
            "bytes",
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(unknown_or_wrong_type(
            "database",
            key.as_ref(),
            database_option_kind(&key),
            "an integer",
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(unknown_or_wrong_type(
            "database",
            key.as_ref(),
            database_option_kind(&key),
            "a double",
        ))
    }
}

fn database_option_key(key: &OptionDatabase) -> Option<BigQueryDatabaseOption> {
    match key {
        OptionDatabase::Other(name) => BigQueryDatabaseOption::from_name(name),
        _ => None,
    }
}

/// Classifies known options so FFI calls can reject unknown keys before `Init`.
pub(crate) fn database_option_kind(key: &OptionDatabase) -> Option<OptionKind> {
    if key == &OptionDatabase::Uri {
        Some(OptionKind::String)
    } else {
        database_option_key(key).map(BigQueryDatabaseOption::option_kind)
    }
}

/// The service an endpoint option configures, for callers matching on the endpoint options.
fn service_endpoint(option: BigQueryDatabaseOption) -> ServiceEndpointKind {
    ServiceEndpointKind::from_option(option).expect("the caller matched an endpoint option")
}

/// A project option's value, which the empty string clears.
fn project_identifier(key: &str, value: String) -> Result<Option<ProjectIdentifier>> {
    (!value.is_empty())
        .then(|| ProjectIdentifier::try_new_from(&format!("the {key} option"), value))
        .transpose()
}

/// A comma-separated list option, which is unset when empty.
fn configured_list(option: BigQueryDatabaseOption, values: &[Arc<str>]) -> Result<String> {
    if values.is_empty() {
        return Err(option_not_set(option.as_str()));
    }
    Ok(values.join(","))
}

/// Rejects a value that cannot be a service-account principal, naming the option and, for the
/// delegate list, the entry's position, without echoing the value.
fn invalid_service_account_principal(
    key: &str,
    position: Option<usize>,
) -> adbc_core::error::Error {
    let entry = match position {
        Some(position) => format!(" entry {position}"),
        None => String::new(),
    };
    invalid_argument(format!(
        "BigQuery option {key}{entry} must be a service account email address made of ASCII \
         letters, digits, and the characters @ . - _, such as \
         my-sa@my-project.iam.gserviceaccount.com"
    ))
}

/// Rejects an option value carrying whitespace, saying what the option must be instead.
fn reject_whitespace(key: &str, value: &str, requirement: &str) -> Result<()> {
    if value.chars().any(char::is_whitespace) {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must be {requirement}, without whitespace"
        )));
    }
    Ok(())
}

fn nonempty_arc(value: String) -> Option<Arc<str>> {
    (!value.is_empty()).then(|| Arc::from(value))
}

fn nonempty_secret(value: String) -> Option<Secret> {
    (!value.is_empty()).then(|| Secret::from(value))
}

/// Splits a list option. An empty value clears the list; an empty entry is far more often a stray
/// comma than a request for a nameless entry, so it is reported.
fn parse_comma_separated(key: &str, value: &str, entries: &str) -> Result<Vec<Arc<str>>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(str::trim)
        .map(|entry| {
            if entry.is_empty() {
                Err(invalid_argument(format!(
                    "BigQuery option {key} must be a comma-separated list of {entries}, with no \
                     empty entry; set it to the empty string to clear it"
                )))
            } else {
                Ok(Arc::from(entry))
            }
        })
        .collect()
}
