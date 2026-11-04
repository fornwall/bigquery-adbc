//! Credential builders and the static-token provider.
//!
//! `google-cloud-auth` has no static-token provider, so the driver supplies one here. Builder
//! errors can quote credential documents; rejections name the affected options instead.

use std::sync::Arc;

use adbc_core::error::Result;
use google_cloud_auth::credentials::external_account::Builder as ExternalAccountCredentialsBuilder;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentialsBuilder;
use google_cloud_auth::credentials::service_account::{
    AccessSpecifier, Builder as ServiceAccountCredentialsBuilder,
};
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentialsBuilder;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use http::Extensions;
use http::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::error::invalid_argument;
use crate::options::valid_enum_values;
use crate::{BigQueryAuthenticationType, BigQueryCredentialsType, BigQueryDatabaseOption};

use super::DatabaseConfig;
use super::endpoints::{is_trusted_credential_url, parse_credential_url};
use super::rejections::{
    malformed_document_destination, required_option, untrusted_document_destination,
};

/// Applies the quota project and effective base scopes to a credential builder.
///
/// A macro because the builder types share no trait. The scope setter is an argument because a
/// service-account key spells scopes as an access specifier and every other builder as a list.
macro_rules! with_quota_and_scopes {
    ($builder:expr, $settings:expr, $scopes:ident($argument:expr)) => {{
        let mut builder = $builder;
        if let Some(project) = $settings.quota_project.as_deref() {
            builder = builder.with_quota_project_id(project);
        }
        if $settings.configured_scopes().next().is_some() {
            builder = builder.$scopes($argument);
        }
        builder
    }};
}

pub(super) use with_quota_and_scopes;

/// An immutable access token and quota project, cached under a single [`EntityTag`].
struct StaticAccessTokenCredentials {
    /// The prebuilt `Authorization` header, marked sensitive, and the quota-project header when
    /// one is configured, validated up front so [`CredentialsProvider::headers`] is infallible.
    headers: HeaderMap,
    entity_tag: EntityTag,
}

impl std::fmt::Debug for StaticAccessTokenCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticAccessTokenCredentials")
            .field("headers", &"<redacted>")
            .finish()
    }
}

impl CredentialsProvider for StaticAccessTokenCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<CacheableResource<HeaderMap>, google_cloud_gax::error::CredentialsError>
    {
        if extensions.get::<EntityTag>() == Some(&self.entity_tag) {
            return Ok(CacheableResource::NotModified);
        }
        Ok(CacheableResource::New {
            entity_tag: self.entity_tag.clone(),
            data: self.headers.clone(),
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        // A bare token names no universe; the default Google domain matches what the other
        // credential types in the bundled google-cloud-auth report.
        Some("googleapis.com".to_owned())
    }
}

/// The quota-project header shared by the SDK and the driver's credential providers.
const QUOTA_PROJECT_HEADER: &str = "x-goog-user-project";

/// ADC's builder prefers `GOOGLE_CLOUD_QUOTA_PROJECT` over its explicit option. Override only
/// the resulting header so the driver's explicit option wins without replacing ADC discovery.
pub(super) fn override_quota_project(
    credentials: Credentials,
    project: &str,
) -> Result<Credentials> {
    Ok(Credentials::from(QuotaProjectCredentials {
        credentials,
        project: quota_project_header(project)?,
    }))
}

struct QuotaProjectCredentials {
    credentials: Credentials,
    project: HeaderValue,
}

impl std::fmt::Debug for QuotaProjectCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuotaProjectCredentials")
            .field("credentials", &"<redacted>")
            .field("project", &self.project)
            .finish()
    }
}

impl CredentialsProvider for QuotaProjectCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<CacheableResource<HeaderMap>, google_cloud_gax::error::CredentialsError>
    {
        let mut headers = self.credentials.headers(extensions).await?;
        if let CacheableResource::New { data, .. } = &mut headers {
            data.insert(QUOTA_PROJECT_HEADER, self.project.clone());
        }
        // The override is immutable: the underlying token's tag also identifies these headers.
        Ok(headers)
    }

    async fn universe_domain(&self) -> Option<String> {
        self.credentials.universe_domain().await
    }
}

fn quota_project_header(project: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(project).map_err(|_| {
        invalid_argument(
            "BigQuery option bigquery.auth.quota_project cannot appear in an HTTP header",
        )
    })
}

pub(super) fn build_static_access_token_credentials(
    token: &str,
    quota_project: Option<&str>,
) -> Result<Credentials> {
    // The failing token is never echoed: it is the secret itself.
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        invalid_argument(format!(
            "BigQuery option {} contains bytes that cannot appear in an HTTP header",
            BigQueryDatabaseOption::AuthenticationAccessToken.as_str()
        ))
    })?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, value);
    if let Some(project) = quota_project {
        headers.insert(
            HeaderName::from_static(QUOTA_PROJECT_HEADER),
            quota_project_header(project)?,
        );
    }
    Ok(Credentials::from(StaticAccessTokenCredentials {
        headers,
        entity_tag: EntityTag::new(),
    }))
}

pub(super) fn build_user_authentication_credentials(
    settings: &DatabaseConfig,
) -> Result<Credentials> {
    use BigQueryDatabaseOption as O;

    const USER: BigQueryAuthenticationType = BigQueryAuthenticationType::UserAuthentication;
    let user = &settings.user_authentication;
    let client_id = required_option(user.client_id.as_deref(), O::AuthenticationClientId, USER)?;
    let client_secret = required_option(
        user.client_secret.as_deref(),
        O::AuthenticationClientSecret,
        USER,
    )?;
    let refresh_token = required_option(
        user.refresh_token.as_deref(),
        O::AuthenticationRefreshToken,
        USER,
    )?;
    let authorized_user = serde_json::json!({
        "type": "authorized_user",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    });
    let mut builder = with_quota_and_scopes!(
        UserAccountCredentialsBuilder::new(authorized_user),
        settings,
        with_scopes(settings.configured_scopes())
    );
    if let Some(token_uri) = user.access_token_endpoint.as_deref() {
        builder = builder.with_token_uri(token_uri);
    }
    builder.build().map_err(|_| {
        invalid_argument(format!(
            "the Google auth library could not build BigQuery user credentials from {}, {} and \
             {}; check that they are the values your OAuth client issued, and that {}, if set, \
             is the token endpoint URL",
            O::AuthenticationClientId.as_str(),
            O::AuthenticationClientSecret.as_str(),
            O::AuthenticationRefreshToken.as_str(),
            O::AuthenticationAccessTokenEndpoint.as_str(),
        ))
    })
}

pub(super) fn require_credentials(settings: &DatabaseConfig) -> Result<&str> {
    settings.credentials.as_deref().ok_or_else(|| {
        invalid_argument(format!(
            "BigQuery option {} is required for {} {:?}; set it to the path of a Google Cloud \
             JSON credential file for {:?}, or to the document itself for {:?}",
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
            BigQueryDatabaseOption::AuthenticationType.as_str(),
            settings.authentication_type.as_str(),
            BigQueryAuthenticationType::JsonCredentialFile.as_str(),
            BigQueryAuthenticationType::JsonCredentialString.as_str(),
        ))
    })
}

/// The credential-document fields that name where a credential is sent: the field, the value
/// Google issues, and whether the OAuth accounts host is trusted, as older keys name it.
///
/// `token_uri` is the OAuth endpoint of `authorized_user`. The auth library ignores it in a
/// `service_account` key, signing tokens locally, but the same rule covers it there so a key's
/// acceptance does not depend on which credential flow that library implements. `token_url` is
/// STS for `external_account` and `external_account_authorized_user`, the latter reachable only
/// as a `source_credentials`.
const DOCUMENT_DESTINATIONS: [(&str, &str, bool); 3] = [
    ("token_uri", "https://oauth2.googleapis.com/token", true),
    ("token_url", "https://sts.googleapis.com/v1/token", false),
    (
        "service_account_impersonation_url",
        "https://iamcredentials.googleapis.com",
        false,
    ),
];

/// Refuses a credential document that would send a credential outside Google's API hosts, unless
/// `bigquery.allow_untrusted_endpoint` is set. Google warns against accepting these documents from
/// an untrusted source, and a URI can carry one in `AuthCredentials`.
///
/// `credential_source` is deliberately not read. Its URLs and file name the customer's own
/// identity provider, which supplies the subject token; the token leaves at `token_url`.
///
/// `universe_domain` selects a destination only under `bigquery.impersonate.*`, whose
/// `generateAccessToken` call goes to `iamcredentials.{universe_domain}` with the source token.
/// The service clients refuse a universe the machine's environment does not also name, so
/// elsewhere the field is left alone: a dbt profile has no way to set the opt-in.
///
/// Reads the parsed value the credential builder receives, so a duplicated key cannot pass here
/// and resolve differently there.
fn validate_document_destinations(settings: &DatabaseConfig, document: &Value) -> Result<()> {
    if settings.allow_untrusted_endpoint {
        return Ok(());
    }
    let check = |prefix: &str, field: &str, value: &str, google: &str, legacy_oauth_host: bool| {
        let field = format!("{prefix}{field}");
        let url = parse_credential_url(value)
            .ok_or_else(|| malformed_document_destination(&field, google))?;
        if is_trusted_credential_url(&url, legacy_oauth_host) {
            return Ok(());
        }
        Err(untrusted_document_destination(
            &field,
            url.scheme(),
            url.host_str().unwrap_or_default(),
            google,
            legacy_oauth_host,
        ))
    };
    let mut prefix = String::new();
    let mut next = Some(document);
    while let Some(document) = next {
        for (field, google, legacy_oauth_host) in DOCUMENT_DESTINATIONS {
            // A value of another JSON type names no destination; the builder refuses it.
            if let Some(value) = document.get(field).and_then(Value::as_str) {
                check(&prefix, field, value, google, legacy_oauth_host)?;
            }
        }
        if settings.impersonation.is_configured()
            && let Some(universe) = document.get("universe_domain").and_then(Value::as_str)
        {
            let destination = format!("https://iamcredentials.{universe}");
            check(
                &prefix,
                "universe_domain",
                &destination,
                "googleapis.com",
                false,
            )?;
        }
        prefix.push_str("source_credentials.");
        next = document.get("source_credentials");
    }
    Ok(())
}

/// Builds the credentials the JSON describes, and the service-account principal they name.
pub(super) fn build_json_credentials(
    settings: &DatabaseConfig,
    json: &str,
) -> Result<(Credentials, Option<Arc<str>>)> {
    /// Builds `$builder` from the parsed JSON, configured like every other credential builder.
    macro_rules! json_credentials {
        ($builder:ty, $value:expr, $scopes:ident($argument:expr)) => {
            with_quota_and_scopes!(<$builder>::new($value), settings, $scopes($argument)).build()
        };
    }
    // Only the error's position is reported: the full serde message quotes what it choked on.
    let value: Value = serde_json::from_slice(json.as_bytes()).map_err(|source| {
        invalid_argument(format!(
            "BigQuery option {} must supply a JSON credential document, but it has a syntax error \
             at line {} column {}",
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
            source.line(),
            source.column()
        ))
    })?;
    let declared = value.get("type").and_then(Value::as_str);
    let detected = declared
        .and_then(BigQueryCredentialsType::from_name)
        // Google matches `type` exactly, while `from_name` is case-insensitive (right for an
        // option value), so `"Service_Account"` must fail here rather than inside the builder.
        .filter(|detected| declared == Some(detected.as_str()))
        .ok_or_else(|| {
            invalid_argument(format!(
                "the JSON credential document in BigQuery option {} has a missing or \
                 unsupported type; its type field must be exactly one of {}",
                BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
                valid_enum_values::<BigQueryCredentialsType>()
            ))
        })?;
    if let Some(configured) = settings
        .credentials_type
        .filter(|configured| *configured != detected)
    {
        // Both are type names, not secrets, so the message can quote them.
        return Err(invalid_argument(format!(
            "BigQuery option {} is {:?} but the configured JSON credentials are {:?}; set it to \
             {:?}, or supply credentials of the type it asserts",
            BigQueryDatabaseOption::AuthenticationCredentialsType.as_str(),
            configured.as_str(),
            detected.as_str(),
            detected.as_str(),
        )));
    }

    validate_document_destinations(settings, &value)?;

    // Read before a builder consumes the value. Only a standard IAM URL identifies a target we
    // can infer locally; a custom credential endpoint still works, but names no known principal.
    let principal = match detected {
        BigQueryCredentialsType::ServiceAccount => value
            .get("client_email")
            .and_then(Value::as_str)
            .filter(|email| !email.is_empty())
            .map(Arc::from),
        BigQueryCredentialsType::ImpersonatedServiceAccount
        | BigQueryCredentialsType::ExternalAccount => value
            .get("service_account_impersonation_url")
            .and_then(Value::as_str)
            .and_then(|url| super::external_account::impersonation_principal(url).ok())
            .map(|(_, principal)| Arc::from(principal.as_ref())),
        BigQueryCredentialsType::AuthorizedUser => None,
    };

    let credentials = match detected {
        BigQueryCredentialsType::ServiceAccount => json_credentials!(
            ServiceAccountCredentialsBuilder,
            value,
            with_access_specifier(AccessSpecifier::from_scopes(settings.configured_scopes()))
        ),
        BigQueryCredentialsType::AuthorizedUser => json_credentials!(
            UserAccountCredentialsBuilder,
            value,
            with_scopes(settings.configured_scopes())
        ),
        BigQueryCredentialsType::ImpersonatedServiceAccount => json_credentials!(
            ImpersonatedCredentialsBuilder,
            value,
            with_scopes(settings.configured_scopes())
        ),
        BigQueryCredentialsType::ExternalAccount => json_credentials!(
            ExternalAccountCredentialsBuilder,
            value,
            with_scopes(settings.configured_scopes())
        ),
    };
    // In place of the dropped builder error, the fields the library requires of each type.
    let credentials = credentials.map_err(|_| {
        let required = match detected {
            BigQueryCredentialsType::ServiceAccount => {
                "client_email, private_key_id, private_key and project_id"
            }
            BigQueryCredentialsType::AuthorizedUser => "client_id, client_secret and refresh_token",
            BigQueryCredentialsType::ImpersonatedServiceAccount => {
                "service_account_impersonation_url and source_credentials"
            }
            BigQueryCredentialsType::ExternalAccount => {
                "audience, subject_token_type, token_url and credential_source"
            }
        };
        invalid_argument(format!(
            "the Google auth library could not use the {:?} credentials in BigQuery option \
             {}; check that the document is as Google issued it and has {required}",
            detected.as_str(),
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        ))
    })?;
    Ok((credentials, principal))
}
