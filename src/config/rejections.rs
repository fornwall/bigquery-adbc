//! Configuration errors with option names and corrective guidance.
//!
//! Do not echo option values: credentials can arrive under any key. Endpoint hosts may be included
//! where noted below. Credential file paths never are: no shape check can tell a path from a
//! misplaced key or token.

use adbc_core::error::{Result, Status};

use super::endpoints::GOOGLE_OAUTH_HOST;
use crate::error::{error, invalid_argument, unreadable_file};
use crate::options::{or_list, valid_enum_values};
use crate::{BigQueryAuthenticationType, BigQueryCredentialsType, BigQueryDatabaseOption};

/// Reports a `bigquery.auth.credentials` path the driver could not read, with the status
/// [`unreadable_file`] assigns.
///
/// Takes no path: [`credential_file_path`] passes a single-line base64 key or a bare access token,
/// so the value may be a credential. `source` must come from the filesystem, whose errors do not
/// carry the path.
pub(super) fn unreadable_credential_file(source: &std::io::Error) -> adbc_core::error::Error {
    unreadable_file(
        format!(
            "BigQuery option {} names a JSON credential file that could not be read (value not \
             echoed): {source}; set it to the path of a readable file, or set {} to {:?} to pass \
             the document itself",
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
            BigQueryDatabaseOption::AuthenticationType.as_str(),
            BigQueryAuthenticationType::JsonCredentialString.as_str(),
        ),
        source,
    )
}

/// Reports a `bigquery.auth.credentials` file too large to be a credential document, which is
/// read only up to that size. Takes no path, for the reason [`unreadable_credential_file`] gives.
pub(super) fn oversized_credential_file() -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery option {} names a file larger than 1 MiB (value not echoed), too large for a \
         JSON credential document; set it to the path of a Google Cloud JSON credential file",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
    ))
}

/// Reject likely credential documents before interpreting the option as a file path.
///
/// The option is a path under `json_credential_file` and the document itself under
/// `json_credential_string`. Confusing the two (or `OAuthType=1` with `2` in a URI) deserves the
/// remedy below rather than a failed file open. Treat a leading `{`, control characters, or
/// excessive length as signs of a document. This is a heuristic, not a secrecy boundary: a value
/// it accepts can still be a credential.
pub(super) fn credential_file_path(value: &str) -> Result<&str> {
    const MAX_PATH_BYTES: usize = 4_096;

    let json_document = value.trim_start().starts_with('{');
    if !json_document && value.len() <= MAX_PATH_BYTES && !value.contains(char::is_control) {
        return Ok(value);
    }
    Err(invalid_argument(format!(
        "BigQuery option {} must be a JSON credential file path for bigquery.auth_type {:?}, \
         but the value (not echoed) {}; set bigquery.auth_type to {:?} to pass the document \
         itself, or set the option to the document's file path",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        BigQueryAuthenticationType::JsonCredentialFile.as_str(),
        if json_document {
            "begins with a JSON object"
        } else {
            "cannot be a path"
        },
        BigQueryAuthenticationType::JsonCredentialString.as_str(),
    )))
}

/// Reports an environment where the Application Default Credentials *document* could not be
/// loaded, given what `GOOGLE_APPLICATION_CREDENTIALS` holds.
///
/// The builder falls back to the metadata server when no document is found, so it fails only on
/// a document it found and could not use. Its error can quote the document, so the cause is
/// re-derived here: the named file is unreadable, or it is not a usable document, or the variable
/// is unset and the file `gcloud` writes is broken. The variable's value is never quoted, for the
/// reason [`unreadable_credential_file`] gives.
pub(super) fn application_default_credentials_failure(
    named: Option<&std::ffi::OsStr>,
) -> adbc_core::error::Error {
    const VARIABLE: &str = "GOOGLE_APPLICATION_CREDENTIALS";
    let alternatives = format!(
        "or set {} to {}",
        BigQueryDatabaseOption::AuthenticationType.as_str(),
        crate::error::credential_sources(),
    );
    let usable = format!(
        "a usable credential document is JSON whose type field is one of {}",
        valid_enum_values::<BigQueryCredentialsType>()
    );
    let message = match named.filter(|value| !value.is_empty()) {
        None => format!(
            "{VARIABLE} is unset, and the Application Default Credentials file `gcloud` writes \
             could not be loaded; {usable}; run `gcloud auth application-default login` to \
             rewrite it, {alternatives}"
        ),
        Some(named) => match named.to_str().map(credential_file_path) {
            Some(Ok(path)) => match std::fs::File::open(path) {
                Err(source) => format!(
                    "the {VARIABLE} environment variable names a credential file that could not \
                     be read (value not echoed): {source}; point it at a credential file, or \
                     unset it and run `gcloud auth application-default login`, {alternatives}"
                ),
                Ok(_) => format!(
                    "the {VARIABLE} environment variable names a credential file that could not \
                     be loaded (value not echoed); {usable}; point it at a credential file, or \
                     unset it and run `gcloud auth application-default login`, {alternatives}"
                ),
            },
            _ => format!(
                "the {VARIABLE} environment variable is not a file path (value not echoed); \
                 set it to a credential file path, or unset it and run `gcloud auth \
                 application-default login`, {alternatives}"
            ),
        },
    };
    error(message, Status::Unauthenticated)
}

/// Reject a service endpoint outside the configured trust boundary.
///
/// The host is quoted, unlike other option values: `normalize_endpoint` has refused user
/// information, queries, and fragments, and a typo like `bigquery.googelapis.com` is otherwise
/// undiagnosable.
pub(super) fn untrusted_endpoint(
    option: BigQueryDatabaseOption,
    authentication_type: BigQueryAuthenticationType,
    scheme: &str,
    host: &str,
) -> adbc_core::error::Error {
    let cleartext = cleartext(scheme);
    invalid_argument(format!(
        "BigQuery option {} must be a Google API HTTPS endpoint, but resolves to host {host:?} \
         over {scheme}; requests there would send the bigquery.auth_type {:?} access \
         token{cleartext}; set {}=true to permit it, or use bigquery.auth_type {:?} for an \
         emulator",
        option.as_str(),
        authentication_type.as_str(),
        BigQueryDatabaseOption::AllowUntrustedEndpoint.as_str(),
        BigQueryAuthenticationType::Anonymous.as_str(),
    ))
}

/// Reject a `bigquery://` URI host outside the Google API domain, which only an option can trust.
///
/// The host is quoted for the reason [`untrusted_endpoint`] gives; the URI parser refuses user
/// information first. Worded for a caller who may have set only a URI and no `bigquery.endpoint`.
pub(super) fn untrusted_uri_endpoint(
    authentication_type: BigQueryAuthenticationType,
    scheme: &str,
    host: &str,
) -> adbc_core::error::Error {
    let cleartext = cleartext(scheme);
    invalid_argument(format!(
        "BigQuery URI host must be a Google API HTTPS endpoint, but resolves to host {host:?} \
         over {scheme}; requests there would send the bigquery.auth_type {:?} access \
         token{cleartext}; a URI cannot send credentials to a new host, so set {} as a database \
         option to trust it, set {}=true to permit it, or use bigquery.auth_type {:?} for an \
         emulator",
        authentication_type.as_str(),
        BigQueryDatabaseOption::Endpoint.as_str(),
        BigQueryDatabaseOption::AllowUntrustedEndpoint.as_str(),
        BigQueryAuthenticationType::Anonymous.as_str(),
    ))
}

/// How a rejection says that a credential would also cross the network unencrypted.
fn cleartext(scheme: &str) -> &'static str {
    if scheme == "https" {
        ""
    } else {
        " in cleartext"
    }
}

/// The hosts a credential exchange may reach, as a refusal names them.
fn trusted_hosts(legacy_oauth_host: bool) -> String {
    if legacy_oauth_host {
        format!("googleapis.com or {GOOGLE_OAUTH_HOST}")
    } else {
        "googleapis.com".to_owned()
    }
}

/// Reject an untrusted credential-exchange URL. Its host is safe to quote because
/// `parse_credential_url` rejects user information.
pub(super) fn untrusted_credential_url(
    option: BigQueryDatabaseOption,
    scheme: &str,
    host: &str,
    exchange: &str,
    google: &str,
    legacy_oauth_host: bool,
) -> adbc_core::error::Error {
    let cleartext = cleartext(scheme);
    let trusted = trusted_hosts(legacy_oauth_host);
    invalid_argument(format!(
        "BigQuery option {} must be an HTTPS endpoint on {trusted}, but resolves to host \
         {host:?} over {scheme}; {exchange} there{cleartext}; use {google}, or set {}=true to \
         permit another host",
        option.as_str(),
        BigQueryDatabaseOption::AllowUntrustedEndpoint.as_str(),
    ))
}

/// Reject a credential document that sends a credential outside the trust boundary.
///
/// `field` is the document's JSON field and `host` comes from `parse_credential_url`, which
/// refuses user information. Nothing else of the value may be quoted: a URL in a document can
/// carry a secret in its path or query, and the document itself holds the credential.
pub(super) fn untrusted_document_destination(
    field: &str,
    scheme: &str,
    host: &str,
    google: &str,
    legacy_oauth_host: bool,
) -> adbc_core::error::Error {
    let cleartext = cleartext(scheme);
    let trusted = trusted_hosts(legacy_oauth_host);
    invalid_argument(format!(
        "the JSON credential document in BigQuery option {} must send credentials over HTTPS to \
         {trusted}, but its {field} field resolves to host {host:?} over {scheme}; a credential \
         would be sent there{cleartext}; use {google}, or set {}=true as a database option to \
         permit another host",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        BigQueryDatabaseOption::AllowUntrustedEndpoint.as_str(),
    ))
}

/// Reject a document a URI supplied that names a command for the auth library to run. `field` is
/// the JSON field path; the command itself is never quoted.
pub(super) fn executable_source_from_uri(field: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "the JSON credential document the connection URI supplied names a command to run in its \
         {field} field (value not echoed); a URI cannot make the driver run a command, so set {} \
         as a database option to use an executable-sourced credential",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
    ))
}

/// Reject a document a URI supplied that names a subject-token file the driver will not have the
/// auth library read. `field` is the JSON field path; the path itself is never quoted.
pub(super) fn unsuitable_subject_token_file_from_uri(field: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "the JSON credential document the connection URI supplied names a file in its {field} \
         field (value not echoed) that is not a regular file of at most 1 MiB; a URI cannot make \
         the driver read anything else, so set {} as a database option to read it",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
    ))
}

/// Reject a URL in a document's `credential_source` whose host the URL parser cannot take.
pub(super) fn malformed_credential_source_url(field: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "the JSON credential document in BigQuery option {} has a {field} field whose host is not \
         ASCII or is percent-encoded (value not echoed); write an internationalized host in \
         Punycode (xn--)",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
    ))
}

/// Reject a credential document destination that names no host the trust rule can judge.
pub(super) fn malformed_document_destination(field: &str, google: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "the JSON credential document in BigQuery option {} has a {field} field that does not \
         resolve to a host (value not echoed); it must name an HTTP or HTTPS destination such as \
         {google}, without user information or a fragment",
        BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
    ))
}

/// Require an option for the selected authentication type.
pub(super) fn required_option(
    value: Option<&str>,
    option: BigQueryDatabaseOption,
    authentication_type: BigQueryAuthenticationType,
) -> Result<&str> {
    value.ok_or_else(|| {
        invalid_argument(format!(
            "BigQuery option {} is required for {} {:?}",
            option.as_str(),
            BigQueryDatabaseOption::AuthenticationType.as_str(),
            authentication_type.as_str()
        ))
    })
}

/// Reject ignored credentials instead of inferring an authentication type from their presence.
pub(super) fn reject_unused_credential(
    option: BigQueryDatabaseOption,
    authentication_type: BigQueryAuthenticationType,
    required: &[BigQueryAuthenticationType],
) -> adbc_core::error::Error {
    let auth_type = BigQueryDatabaseOption::AuthenticationType.as_str();
    let required: Vec<String> = required
        .iter()
        .map(|value| format!("{:?}", value.as_str()))
        .collect();
    invalid_argument(format!(
        "BigQuery option {} is set but {auth_type} is {:?}; it requires {auth_type} {}",
        option.as_str(),
        authentication_type.as_str(),
        or_list(&required),
    ))
}
