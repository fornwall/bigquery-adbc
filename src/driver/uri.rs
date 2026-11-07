//! BigQuery connection URI parsing.
//!
//! Parsing produces typed database options, which go through the same `set_option` path as
//! explicit options, so URI and option handling cannot diverge.

use std::borrow::Cow;
use std::collections::BTreeMap;

use adbc_core::error::Result;
use percent_encoding::percent_decode_str;

use crate::BigQueryAuthenticationType;
use crate::BigQueryDatabaseOption as O;
use crate::config::endpoints::{
    AuthorityProblem, default_endpoint_host, parse_endpoint_authority, parse_endpoint_port,
};
use crate::error::invalid_argument;
use crate::identifiers::ProjectIdentifier;
use crate::options::or_list;

const URI_PREFIX: &str = "bigquery://";
const DEFAULT_ENDPOINT_AUTHORITY: &str = concat!(default_endpoint_host!(), ":443");

/// One query parameter the driver accepts.
struct QueryParameter {
    name: &'static str,
    /// The option its value sets. `OAuthType` sets none: it selects `bigquery.auth_type`.
    option: Option<O>,
    /// Whether the value is a credential, whose option is write-only and which [`redact_uri`]
    /// therefore removes.
    secret: bool,
}

const fn parameter(name: &'static str, option: Option<O>, secret: bool) -> QueryParameter {
    QueryParameter {
        name,
        option,
        secret,
    }
}

/// The query parameters the driver accepts.
///
/// Matching is case-sensitive, as it is in the Go driver, but a name that differs only in case
/// is answered with the spelling that would have worked.
const QUERY_PARAMETERS: &[QueryParameter] = &[
    parameter("OAuthType", None, false),
    parameter("AuthCredentials", Some(O::AuthenticationCredentials), true),
    parameter("AuthClientId", Some(O::AuthenticationClientId), false),
    parameter(
        "AuthClientSecret",
        Some(O::AuthenticationClientSecret),
        true,
    ),
    parameter(
        "AuthRefreshToken",
        Some(O::AuthenticationRefreshToken),
        true,
    ),
    parameter("DatasetId", Some(O::DatasetId), false),
    parameter("Location", Some(O::Location), false),
    parameter("QuotaProject", Some(O::AuthenticationQuotaProject), false),
    parameter(
        "ImpersonateTargetPrincipal",
        Some(O::ImpersonationTargetPrincipal),
        false,
    ),
    parameter(
        "ImpersonateDelegates",
        Some(O::ImpersonationDelegates),
        false,
    ),
    parameter("ImpersonateScopes", Some(O::ImpersonationScopes), false),
    parameter("ImpersonateLifetime", Some(O::ImpersonationLifetime), false),
];

/// The decoded value of each query parameter present, by its name in [`QUERY_PARAMETERS`].
type QueryParameters = BTreeMap<&'static str, String>;

/// Parse a BigQuery URI into the database options it configures.
///
/// URI-level rules (shape, and which parameters each `OAuthType` admits) are enforced here; each
/// returned value is validated by its option when applied. The project, endpoint and
/// authentication type are always present.
pub(crate) fn parse_bigquery_uri(uri: &str) -> Result<Vec<(O, String)>> {
    if uri.is_empty() {
        return Err(invalid_argument("BigQuery URI must not be empty"));
    }
    let remainder = uri
        .get(URI_PREFIX.len()..)
        .filter(|_| uri[..URI_PREFIX.len()].eq_ignore_ascii_case(URI_PREFIX))
        .ok_or_else(|| invalid_argument("BigQuery URI scheme must be bigquery://"))?;
    if remainder.contains('#') {
        return Err(invalid_argument("BigQuery URI must not contain a fragment"));
    }

    let (authority_and_path, query) = remainder
        .split_once('?')
        .map_or((remainder, None), |(path, query)| (path, Some(query)));
    let (authority, encoded_project) = authority_and_path.split_once('/').ok_or_else(|| {
        invalid_argument(
            "BigQuery URI must contain a project ID path, as in bigquery:///my-project or \
             bigquery://bigquery.googleapis.com/my-project",
        )
    })?;
    // A trailing slash names no further segment, and a URI assembled by joining a base with a
    // path routinely ends in one, so it is dropped rather than reported as a second segment.
    let encoded_project = encoded_project.strip_suffix('/').unwrap_or(encoded_project);
    if encoded_project.is_empty() || encoded_project.contains('/') {
        return Err(invalid_argument(
            "BigQuery URI must contain exactly one project ID path segment, as in \
             bigquery:///my-project",
        ));
    }

    let project_id = decode_percent_encoded(encoded_project)
        .map_err(|problem| decoding_failure(format_args!("project path"), problem))?;
    ProjectIdentifier::try_new_from("the URI path", project_id.as_str())?;
    let endpoint = endpoint_from_authority(authority)?;
    let parameters = parse_query(query)?;
    validate_impersonation(&parameters)?;
    let authentication_type = authentication_type(&parameters)?;

    let mut options = vec![
        (O::ProjectId, project_id),
        (O::Endpoint, endpoint),
        (
            O::AuthenticationType,
            authentication_type.as_str().to_owned(),
        ),
    ];
    // Validated by their options when applied, which marks a rejection as coming from the URI.
    options.extend(QUERY_PARAMETERS.iter().filter_map(|parameter| {
        let value = parameters.get(parameter.name)?;
        Some((parameter.option?, value.clone())).filter(|_| !value.is_empty())
    }));
    Ok(options)
}

/// The URI with the value of every credential-bearing query parameter replaced.
///
/// The standard `uri` option can be read back, while the credential options it may carry are
/// write-only. User information needs no handling: [`endpoint_from_authority`] refuses an authority
/// containing `@`.
pub(crate) fn redact_uri(uri: &str) -> String {
    const REDACTED: &str = "<redacted>";

    let Some((prefix, query)) = uri.split_once('?') else {
        return uri.to_owned();
    };
    let mut redacted = String::with_capacity(uri.len());
    redacted.push_str(prefix);
    redacted.push('?');
    for (index, pair) in query.split('&').enumerate() {
        if index > 0 {
            redacted.push('&');
        }
        let name = pair.split_once('=').map_or(pair, |(name, _)| name);
        // Decoded exactly as `parse_query` decodes it, so a percent-encoded or `+`-spelled name
        // cannot carry a credential past the table by spelling `AuthClientSecret` differently.
        let secret = decode_query_component(name).is_ok_and(|name| {
            QUERY_PARAMETERS
                .iter()
                .any(|candidate| candidate.name == name && candidate.secret)
        });
        if secret {
            redacted.push_str(name);
            redacted.push('=');
            redacted.push_str(REDACTED);
        } else {
            redacted.push_str(pair);
        }
    }
    redacted
}

fn endpoint_from_authority(authority: &str) -> Result<String> {
    if authority.contains('@') || authority.chars().any(char::is_whitespace) {
        // The one authority that is never quoted, refused before any part of it can reach a
        // message that quotes a host or port: `user:password@host`, and `:password@host`, which
        // would otherwise read as a port.
        return Err(invalid_argument(
            "BigQuery URI endpoint authority must be host[:port], without user information or \
             whitespace",
        ));
    }
    // An empty port is the default one (RFC 3986 section 3.2.3), as Go's `url.URL.Port` reads it.
    let authority = match authority.strip_suffix(':') {
        Some(host) if !host.contains(':') || host.ends_with(']') => host,
        _ => authority,
    };
    if authority.is_empty() {
        return Ok(format!("https://{DEFAULT_ENDPOINT_AUTHORITY}"));
    }
    if let Some(port) = authority.strip_prefix(':') {
        parse_endpoint_port(port).map_err(|problem| invalid_authority(port, problem))?;
        return Ok(format!("https://{}:{port}", default_endpoint_host!()));
    }
    let (_, port) = parse_endpoint_authority(authority)
        .map_err(|problem| invalid_authority(authority, problem))?;
    if port.is_some() {
        Ok(format!("https://{authority}"))
    } else {
        Ok(format!("https://{authority}:443"))
    }
}

/// The URI's own wording for each way an authority can fail the endpoint grammar it shares with
/// `bigquery.endpoint`, so a caller who set a URI is not told to fix an option they never set.
///
/// The host, port and authority are quoted: [`endpoint_from_authority`] has already refused any
/// authority carrying user information or whitespace, so what reaches here cannot repeat a
/// credential.
fn invalid_authority(authority: &str, problem: AuthorityProblem<'_>) -> adbc_core::error::Error {
    match problem {
        AuthorityProblem::EmptyHost => {
            invalid_argument("BigQuery URI endpoint host must not be empty")
        }
        AuthorityProblem::InvalidHost(host) | AuthorityProblem::InvalidIpv6Host(host) => {
            invalid_argument(format!(
                "BigQuery URI endpoint host {host:?} must be a host name of ASCII letters, \
                 digits, dots, hyphens, and underscores, or a bracketed IPv6 address"
            ))
        }
        AuthorityProblem::MalformedBrackets => invalid_argument(format!(
            "BigQuery URI endpoint authority {authority:?} must be host[:port]"
        )),
        AuthorityProblem::UnbracketedIpv6 => invalid_argument(format!(
            "BigQuery URI endpoint authority {authority:?} must be host[:port], with an IPv6 host \
             enclosed in brackets"
        )),
        AuthorityProblem::PortNotDigits(port) => invalid_argument(format!(
            "BigQuery URI endpoint port {port:?} must be decimal digits only"
        )),
        AuthorityProblem::PortOutOfRange(_) => {
            invalid_argument("BigQuery URI endpoint port must be between 1 and 65535")
        }
    }
}

fn parse_query(query: Option<&str>) -> Result<QueryParameters> {
    let mut parameters = QueryParameters::new();
    let mut previous: Option<&QueryParameter> = None;
    // An empty query and an empty `&`-separated pair name no parameter; a URI assembled by
    // concatenation may end in one. Go's `url.ParseQuery` skips them, and so does this.
    for (position, pair) in query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .enumerate()
    {
        let Some((encoded_name, encoded_value)) = pair.split_once('=') else {
            return Err(query_component_without_a_value(pair, position + 1));
        };
        if encoded_name.is_empty() {
            return Err(invalid_argument(
                "BigQuery URI query parameter names must not be empty",
            ));
        }
        let name = decode_query_component(encoded_name).map_err(|problem| {
            decoding_failure(
                format_args!("query component {}'s name", position + 1),
                problem,
            )
        })?;
        let Some(parameter) = QUERY_PARAMETERS
            .iter()
            .find(|candidate| candidate.name == name)
        else {
            // After a credential, an unknown name is most likely the credential's own tail,
            // split off at an unencoded `&`, so it is located rather than quoted.
            if previous.is_some_and(|previous| previous.secret) {
                return Err(unencoded_ampersand(position + 1));
            }
            return Err(unknown_query_parameter(&name));
        };
        let value = decode_query_component(encoded_value).map_err(|problem| {
            decoding_failure(
                format_args!("query parameter {}'s value", parameter.name),
                problem,
            )
        })?;
        previous = Some(parameter);
        if parameters.insert(parameter.name, value).is_some() {
            return Err(invalid_argument(format!(
                "BigQuery URI query parameter {name} must not be repeated"
            )));
        }
    }
    Ok(parameters)
}

/// Reports a query component carrying no `=`, echoing it only when it is a parameter name.
///
/// Such a component can be the tail of a value with an unencoded `&`
/// (`AuthCredentials={"private_key":"a&b"}` leaves `b"}`), so anything that does not spell an
/// accepted parameter is located by position and length rather than quoted.
fn query_component_without_a_value(component: &str, position: usize) -> adbc_core::error::Error {
    if let Ok(decoded) = decode_query_component(component)
        && QUERY_PARAMETERS
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(&decoded))
    {
        return invalid_argument(format!(
            "BigQuery URI query parameter {decoded} must have a value"
        ));
    }
    let length = component.chars().count();
    let unit = if length == 1 {
        "character"
    } else {
        "characters"
    };
    invalid_argument(format!(
        "BigQuery URI query component {position} ({length} {unit}) has no value; each component \
         must be name=value, with any & in a value encoded as %26"
    ))
}

/// Reports a component that is probably the tail of a credential split at an unencoded `&`.
fn unencoded_ampersand(position: usize) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery URI query component {position} is not a parameter (its name is not echoed, as \
         it may be the tail of the credential before it); encode any & in a credential as %26, \
         or check the parameter's spelling"
    ))
}

/// Names the spelling that would have worked: the same parameter in the right case when the
/// caller only got the case wrong, and otherwise every parameter there is.
fn unknown_query_parameter(name: &str) -> adbc_core::error::Error {
    if let Some(canonical) = QUERY_PARAMETERS
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(name))
    {
        let canonical = canonical.name;
        return invalid_argument(format!(
            "BigQuery URI query parameter {name} is spelled {canonical}"
        ));
    }
    let names: Vec<&str> = QUERY_PARAMETERS
        .iter()
        .map(|parameter| parameter.name)
        .collect();
    invalid_argument(format!(
        "BigQuery URI query parameter {name} is not one of {}",
        or_list(&names)
    ))
}

/// Why a URI component could not be decoded. None quotes the component, which may be a
/// credential.
#[derive(Clone, Copy)]
enum DecodingProblem {
    /// The first percent-escape that is not two hex digits, by byte offset.
    MalformedEscape(usize),
    InvalidUtf8,
    /// No C string option can carry a NUL, and `AdbcDatabaseGetOption` could not return one.
    Nul,
}

/// Reports a component that could not be decoded, naming the part of the URI it was.
fn decoding_failure(
    part: std::fmt::Arguments<'_>,
    problem: DecodingProblem,
) -> adbc_core::error::Error {
    invalid_argument(match problem {
        DecodingProblem::MalformedEscape(offset) => {
            format!("BigQuery URI {part} has malformed percent encoding at byte offset {offset}")
        }
        DecodingProblem::InvalidUtf8 => {
            format!("BigQuery URI {part} is not valid UTF-8 once decoded")
        }
        DecodingProblem::Nul => {
            format!("BigQuery URI {part} must not contain a percent-encoded NUL character")
        }
    })
}

fn decode_query_component(value: &str) -> std::result::Result<String, DecodingProblem> {
    let value = if value.contains('+') {
        Cow::Owned(value.replace('+', " "))
    } else {
        Cow::Borrowed(value)
    };
    decode_percent_encoded(&value)
}

fn decode_percent_encoded(value: &str) -> std::result::Result<String, DecodingProblem> {
    let bytes = value.as_bytes();
    if let Some(offset) = bytes.iter().enumerate().find_map(|(offset, &byte)| {
        let escape = bytes.get(offset + 1..offset + 3);
        let well_formed = matches!(escape, Some([first, second])
            if first.is_ascii_hexdigit() && second.is_ascii_hexdigit());
        (byte == b'%' && !well_formed).then_some(offset)
    }) {
        return Err(DecodingProblem::MalformedEscape(offset));
    }
    let decoded = percent_decode_str(value)
        .decode_utf8()
        .map(Cow::into_owned)
        .map_err(|_| DecodingProblem::InvalidUtf8)?;
    if decoded.contains('\0') {
        return Err(DecodingProblem::Nul);
    }
    Ok(decoded)
}

/// The authentication type `OAuthType` selects, which also gates which credential parameters may
/// appear at all.
fn authentication_type(parameters: &QueryParameters) -> Result<BigQueryAuthenticationType> {
    use BigQueryAuthenticationType as A;
    const USER: [&str; 3] = ["AuthClientId", "AuthClientSecret", "AuthRefreshToken"];

    let selected = parameters.get("OAuthType").map_or("", String::as_str);
    let (selected, takes_json, takes_user) = match selected {
        "" | "0" => (A::ApplicationDefault, false, false),
        "1" => (A::JsonCredentialFile, true, false),
        "2" => (A::JsonCredentialString, true, false),
        "3" => (A::UserAuthentication, false, true),
        _ => {
            return Err(invalid_argument(
                "BigQuery URI OAuthType must be 0, 1, 2, or 3",
            ));
        }
    };
    let json = is_set(parameters, "AuthCredentials");
    if json && !takes_json {
        return Err(invalid_argument(
            "BigQuery URI AuthCredentials requires OAuthType=1 or 2",
        ));
    }
    if !takes_user && USER.iter().any(|name| is_set(parameters, name)) {
        return Err(invalid_argument(
            "BigQuery URI user OAuth parameters require OAuthType=3",
        ));
    }
    if takes_json && !json {
        return Err(invalid_argument(
            "BigQuery URI OAuthType=1 or 2 requires AuthCredentials",
        ));
    }
    if takes_user && !USER.iter().all(|name| is_set(parameters, name)) {
        return Err(invalid_argument(
            "BigQuery URI OAuthType=3 requires AuthClientId, AuthClientSecret, and AuthRefreshToken",
        ));
    }
    Ok(selected)
}

fn validate_impersonation(parameters: &QueryParameters) -> Result<()> {
    // Scopes alone are allowed, as `ImpersonationConfig::is_configured` allows them.
    let has_dependent_option = ["ImpersonateDelegates", "ImpersonateLifetime"]
        .iter()
        .any(|name| is_set(parameters, name));
    if has_dependent_option && !is_set(parameters, "ImpersonateTargetPrincipal") {
        return Err(invalid_argument(
            "BigQuery URI impersonation options require ImpersonateTargetPrincipal",
        ));
    }
    Ok(())
}

/// Whether a parameter carries a value at all: an empty value is the URI spelling of unset, as
/// in Go (`OAuthType=` resolves to ADC), and every check has to agree on that.
fn is_set(parameters: &QueryParameters, name: &str) -> bool {
    parameters.get(name).is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests;
