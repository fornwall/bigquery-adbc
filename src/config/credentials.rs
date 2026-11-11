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
use super::endpoints::{is_trusted_credential_url, parse_credential_url, unsafe_url_authority};
use super::rejections::{
    executable_source_from_uri, irregular_credential_file_from_uri,
    malformed_credential_source_url, malformed_document_destination, oversized_credential_file,
    required_option, unreadable_credential_file, unsuitable_subject_token_file_from_uri,
    untrusted_document_destination,
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

/// Reads the JSON credential file at `path`, refusing one larger than 1 MiB after reading only
/// that much.
///
/// The bound matters because the path can come from a URI's `AuthCredentials`: `/dev/zero` or a
/// log file would otherwise be read into memory whole. Credential documents are a few KiB. A pipe
/// set as an option still works, since it ends at its writer's EOF, but a path the URI supplied
/// (`from_uri`) must be a regular file: a pipe, terminal or `/dev/stdin` could block the
/// connection forever, on its open or its read, as no URI's author may make the driver do.
/// `metadata` follows symbolic links and does not open the file, so the check cannot block; a path
/// it cannot inspect is left to the open, whose error gives the cause.
pub(super) fn read_credential_file(path: &str, from_uri: bool) -> Result<String> {
    use std::io::{Error, ErrorKind, Read as _};
    const LIMIT: usize = 1024 * 1024;

    if from_uri && std::fs::metadata(path).is_ok_and(|metadata| !metadata.is_file()) {
        return Err(irregular_credential_file_from_uri());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| file.take(LIMIT as u64 + 1).read_to_end(&mut bytes))
        .map_err(|source| unreadable_credential_file(&source))?;
    if bytes.len() > LIMIT {
        return Err(oversized_credential_file());
    }
    // Decoded after the bound, so a cut through a multi-byte character cannot pass for a
    // non-UTF-8 file; the cause is the one `fs::read_to_string` reports.
    String::from_utf8(bytes).map_err(|_| {
        unreadable_credential_file(&Error::new(
            ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
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
/// an untrusted source, and a URI can carry one in `AuthCredentials`. The opt-in lifts only the
/// trust rule: the auth library parses these URLs either way, so they must parse here first, which
/// also keeps a host that would panic a debug build's parser out (UP-103).
///
/// `credential_source` names the customer's own identity provider, which supplies the subject
/// token that leaves at `token_url`, so it is no destination; [`check_credential_sources`] holds
/// it to what holds whoever supplied the document.
///
/// `universe_domain` selects a destination only under `bigquery.impersonate.*`, whose
/// `generateAccessToken` call goes to `iamcredentials.{universe_domain}` with the source token.
/// The service clients refuse a universe the machine's environment does not also name, so
/// elsewhere the field is left alone: a dbt profile has no way to set the opt-in.
///
/// Reads the parsed value the credential builder receives, so a duplicated key cannot pass here
/// and resolve differently there.
fn validate_document_destinations(settings: &DatabaseConfig, document: &Value) -> Result<()> {
    let check = |prefix: &str, field: &str, value: &str, google: &str, legacy_oauth_host: bool| {
        let field = format!("{prefix}{field}");
        let url = parse_credential_url(value)
            .ok_or_else(|| malformed_document_destination(&field, google))?;
        if settings.allow_untrusted_endpoint || is_trusted_credential_url(&url, legacy_oauth_host) {
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
    for (prefix, document) in document_chain(document) {
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
    }
    Ok(())
}

/// A credential document and each `source_credentials` nested in it, with the field-path prefix
/// that names the nested one in a refusal.
fn document_chain(document: &Value) -> impl Iterator<Item = (String, &Value)> {
    std::iter::successors(Some((String::new(), document)), |(prefix, document)| {
        let source = document.get("source_credentials")?;
        Some((format!("{prefix}source_credentials."), source))
    })
}

/// The `credential_source` fields holding a URL the auth library fetches: a URL source's, and an
/// AWS source's region, metadata, session-token and request-template URLs.
const CREDENTIAL_SOURCE_URLS: [&str; 4] = [
    "url",
    "region_url",
    "regional_cred_verification_url",
    "imdsv2_session_token_url",
];

/// The largest subject-token file a URI-supplied document may name, as for a credential file.
const SUBJECT_TOKEN_FILE_LIMIT: u64 = 1024 * 1024;

/// Checks each `credential_source` in the document chain before the auth library sees it.
///
/// Every URL it fetches must have an ASCII, unescaped host, whoever supplied the document: no
/// trust rule applies, but a host the `idna` stub would panic on in a debug build must not reach
/// the library's parser (UP-103).
///
/// A document from a URI's `AuthCredentials`, inline or by path, is also held to what a URI's
/// author may make the driver do, since a URI is often assembled from input the application does
/// not control. It may not name an `executable`, which the library runs once the machine sets
/// `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`, and its `file` must be a regular file of at most
/// 1 MiB, because the library reads it whole, synchronously, at every token fetch (UP-104). Only
/// an option trusts more: `bigquery.allow_untrusted_endpoint` is about hosts and does not apply.
/// A URL source's response goes to the checked `token_url`, so where it points is not limited.
/// A field of another JSON type is left to the library, which refuses it, except that a null
/// `executable` names no command.
fn check_credential_sources(settings: &DatabaseConfig, document: &Value) -> Result<()> {
    for (prefix, document) in document_chain(document) {
        let Some(source) = document.get("credential_source") else {
            continue;
        };
        let field = |name: &str| format!("{prefix}credential_source.{name}");
        if settings.credentials_from_uri {
            if source
                .get("executable")
                .is_some_and(|executable| !executable.is_null())
            {
                return Err(executable_source_from_uri(&field("executable")));
            }
            if let Some(path) = source.get("file").and_then(Value::as_str) {
                // `metadata` follows symbolic links, as a projected token file is one, and does
                // not open the file, so a FIFO cannot block here.
                let small_regular_file = std::fs::metadata(path).is_ok_and(|metadata| {
                    metadata.is_file() && metadata.len() <= SUBJECT_TOKEN_FILE_LIMIT
                });
                if !small_regular_file {
                    return Err(unsuitable_subject_token_file_from_uri(&field("file")));
                }
            }
        }
        for name in CREDENTIAL_SOURCE_URLS {
            if source
                .get(name)
                .and_then(Value::as_str)
                .is_some_and(unsafe_url_authority)
            {
                return Err(malformed_credential_source_url(&field(name)));
            }
        }
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
        // An inline value that does not even open an object is most likely a file path, the
        // converse of the mistake `credential_file_path` answers.
        let remedy = if settings.authentication_type
            == BigQueryAuthenticationType::JsonCredentialString
            && !json.trim_start().starts_with('{')
        {
            format!(
                "; to name a credential file instead, set {} to {:?}",
                BigQueryDatabaseOption::AuthenticationType.as_str(),
                BigQueryAuthenticationType::JsonCredentialFile.as_str(),
            )
        } else {
            String::new()
        };
        invalid_argument(format!(
            "BigQuery option {} must supply a JSON credential document, but it has a syntax error \
             at line {} column {}{remedy}",
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

    check_credential_sources(settings, &value)?;
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
