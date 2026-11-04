//! Service endpoint overrides, defaults, and credential trust checks.
//!
//! Regional defaults must pass the same trust checks as explicit overrides.

use std::sync::Arc;

use adbc_core::error::Result;

use crate::error::invalid_argument;
use crate::{BigQueryDatabaseOption, BigQueryStatementOption};

/// One authenticated Google service whose endpoint the database may override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServiceEndpointKind {
    BigQuery,
    Storage,
    Gcs,
    Dataproc,
    Vertex,
}

impl ServiceEndpointKind {
    /// Every credential-bearing endpoint.
    pub(crate) const ALL: [Self; 5] = [
        Self::BigQuery,
        Self::Storage,
        Self::Gcs,
        Self::Dataproc,
        Self::Vertex,
    ];

    pub(crate) const fn option(self) -> BigQueryDatabaseOption {
        match self {
            Self::BigQuery => BigQueryDatabaseOption::Endpoint,
            Self::Storage => BigQueryDatabaseOption::StorageEndpoint,
            Self::Gcs => BigQueryDatabaseOption::GcsEndpoint,
            Self::Dataproc => BigQueryDatabaseOption::DataprocEndpoint,
            Self::Vertex => BigQueryDatabaseOption::VertexEndpoint,
        }
    }

    pub(crate) fn from_option(option: BigQueryDatabaseOption) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.option() == option)
    }
}

/// The endpoint overrides, one per [`ServiceEndpointKind`], each field named after its option.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ServiceEndpoints {
    endpoint: Option<Arc<str>>,
    storage_endpoint: Option<Arc<str>>,
    gcs_endpoint: Option<Arc<str>>,
    dataproc_endpoint: Option<Arc<str>>,
    vertex_endpoint: Option<Arc<str>>,
}

impl ServiceEndpoints {
    pub(crate) fn get(&self, kind: ServiceEndpointKind) -> Option<&str> {
        match kind {
            ServiceEndpointKind::BigQuery => &self.endpoint,
            ServiceEndpointKind::Storage => &self.storage_endpoint,
            ServiceEndpointKind::Gcs => &self.gcs_endpoint,
            ServiceEndpointKind::Dataproc => &self.dataproc_endpoint,
            ServiceEndpointKind::Vertex => &self.vertex_endpoint,
        }
        .as_deref()
    }

    pub(crate) fn set(&mut self, kind: ServiceEndpointKind, value: Option<Arc<str>>) {
        *match kind {
            ServiceEndpointKind::BigQuery => &mut self.endpoint,
            ServiceEndpointKind::Storage => &mut self.storage_endpoint,
            ServiceEndpointKind::Gcs => &mut self.gcs_endpoint,
            ServiceEndpointKind::Dataproc => &mut self.dataproc_endpoint,
            ServiceEndpointKind::Vertex => &mut self.vertex_endpoint,
        } = value;
    }
}

/// Whether a host belongs to the Google APIs domain, the default trust boundary for auxiliary
/// services, URI hosts and credential exchanges. A BigQuery REST endpoint set as an option is
/// trusted separately.
///
/// Matching the credential's own universe would be more precise, but
/// `Credentials::universe_domain` is `async` and this runs where it cannot be awaited. Hosts in
/// other universes use `bigquery.allow_untrusted_endpoint`; see UP-43.
pub(super) fn is_google_api_host(host: &str) -> bool {
    const DOMAIN: &str = "googleapis.com";

    let host = host.trim_end_matches('.');
    host.eq_ignore_ascii_case(DOMAIN)
        || host
            .len()
            .checked_sub(DOMAIN.len() + 1)
            .and_then(|label| host.get(label..))
            .and_then(|suffix| suffix.strip_prefix('.'))
            .is_some_and(|domain| domain.eq_ignore_ascii_case(DOMAIN))
}

/// Whether a host is Google's OAuth 2.0 accounts host, whose
/// `https://accounts.google.com/o/oauth2/token` is the `token_uri` older Google-issued
/// `client_secret.json` files still name. Only this host: nothing else on `google.com` is an
/// endpoint this driver posts a credential to.
pub(super) fn is_google_oauth_host(host: &str) -> bool {
    host.trim_end_matches('.')
        .eq_ignore_ascii_case(GOOGLE_OAUTH_HOST)
}

/// The host [`is_google_oauth_host`] admits, also named in the refusal.
pub(super) const GOOGLE_OAUTH_HOST: &str = "accounts.google.com";

/// The rule for a URL from [`parse_credential_url`]: HTTPS to a Google API host, or to the OAuth
/// accounts host where the exchange is an OAuth token request.
pub(super) fn is_trusted_credential_url(url: &reqwest::Url, legacy_oauth_host: bool) -> bool {
    // `parse_credential_url` requires a host; an empty one would fail the rule anyway.
    let host = url.host_str().unwrap_or_default();
    url.scheme() == "https"
        && (is_google_api_host(host) || (legacy_oauth_host && is_google_oauth_host(host)))
}

/// Normalize a service endpoint and identify invalid values by their option key.
pub(crate) fn normalize_endpoint(mut value: String, key: &str) -> Result<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.chars().any(char::is_whitespace) || value.contains(['?', '#']) {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must not contain whitespace, a query, or a fragment"
        )));
    }
    match ["https://", "http://"].into_iter().find(|scheme| {
        value
            .get(..scheme.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(scheme))
    }) {
        Some(scheme) => value[..scheme.len()].make_ascii_lowercase(),
        None if !value.contains("://") => value.insert_str(0, "https://"),
        None => {}
    }
    let (_, authority) = split_endpoint_scheme(&value)
        .ok_or_else(|| invalid_argument(format!("BigQuery option {key} must use HTTP or HTTPS")))?;
    let authority = authority.trim_end_matches('/');
    if authority.is_empty()
        || authority.contains(['/', '@'])
        || parse_endpoint_authority(authority).is_err()
    {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must contain a valid host and optional port, with no path or user information"
        )));
    }
    // The authority above is non-empty and slash-free, so this can only take trailing slashes.
    value.truncate(value.trim_end_matches('/').len());
    Ok(Some(value))
}

/// Split an endpoint into its scheme and authority, accepting only HTTP and HTTPS.
fn split_endpoint_scheme(endpoint: &str) -> Option<(&'static str, &str)> {
    endpoint
        .strip_prefix("https://")
        .map(|authority| ("https", authority))
        .or_else(|| {
            endpoint
                .strip_prefix("http://")
                .map(|authority| ("http", authority))
        })
}

/// The host of an authority, without its port and IPv6 brackets. An authority the grammar rejects
/// (already refused by [`normalize_endpoint`]) yields itself, which cannot pass the trust rule.
fn authority_host(authority: &str) -> &str {
    parse_endpoint_authority(authority).map_or(authority, |(host, _)| host)
}

/// Split a normalized endpoint into its scheme and host, without the port.
pub(crate) fn endpoint_scheme_and_host(endpoint: &str) -> Option<(&'static str, &str)> {
    split_endpoint_scheme(endpoint).map(|(scheme, authority)| (scheme, authority_host(authority)))
}

/// Parses a URL a credential is sent to (an OAuth token endpoint or an identity-provider token
/// request) with the WHATWG parser `reqwest` uses, so the reported host is the host the request
/// reaches. User information is refused so the host can be quoted.
pub(crate) fn parse_credential_url(value: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(value).ok()?;
    let well_formed = !value.chars().any(char::is_whitespace)
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && matches!(url.scheme(), "http" | "https");
    well_formed.then_some(url)
}

/// The host names an endpoint authority may carry outside brackets.
fn valid_endpoint_hostname(host: &str) -> bool {
    !host.is_empty()
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

/// What is wrong with an endpoint authority, for the option and the `bigquery://` URI to word in
/// their own vocabulary. Each variant carries the fragment the caller quotes, where there is one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthorityProblem<'a> {
    /// Nothing precedes the port.
    EmptyHost,
    /// The host outside brackets is not a host name this grammar accepts.
    InvalidHost(&'a str),
    /// The text inside brackets is not an IPv6 literal.
    InvalidIpv6Host(&'a str),
    /// A bracketed host whose closing bracket, or whose `:` before the port, is missing.
    MalformedBrackets,
    /// More than one colon outside brackets, which is an unbracketed IPv6 address.
    UnbracketedIpv6,
    /// The port is empty or is not decimal digits.
    PortNotDigits(&'a str),
    /// The port is decimal digits, but not a port number.
    PortOutOfRange(&'a str),
}

/// Split an authority into its host (without IPv6 brackets) and its optional port.
pub(crate) fn parse_endpoint_authority(
    authority: &str,
) -> std::result::Result<(&str, Option<u16>), AuthorityProblem<'_>> {
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or(AuthorityProblem::MalformedBrackets)?;
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(AuthorityProblem::InvalidIpv6Host(host));
        }
        let port = match suffix {
            "" => None,
            suffix => Some(
                suffix
                    .strip_prefix(':')
                    .ok_or(AuthorityProblem::MalformedBrackets)?,
            ),
        };
        (host, port)
    } else {
        let mut pieces = authority.split(':');
        let host = pieces.next().unwrap_or_default();
        if host.is_empty() {
            return Err(AuthorityProblem::EmptyHost);
        }
        if !valid_endpoint_hostname(host) {
            return Err(AuthorityProblem::InvalidHost(host));
        }
        let port = pieces.next();
        if pieces.next().is_some() {
            return Err(AuthorityProblem::UnbracketedIpv6);
        }
        (host, port)
    };
    Ok((host, port.map(parse_endpoint_port).transpose()?))
}

/// The port rule both spellings share: decimal digits naming a port in 1..=65535.
pub(crate) fn parse_endpoint_port(port: &str) -> std::result::Result<u16, AuthorityProblem<'_>> {
    if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AuthorityProblem::PortNotDigits(port));
    }
    port.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or(AuthorityProblem::PortOutOfRange(port))
}

/// The public BigQuery REST host, a macro so the URI parser's default authority can `concat!` it.
macro_rules! default_endpoint_host {
    () => {
        "bigquery.googleapis.com"
    };
}

pub(crate) use default_endpoint_host;

/// The public BigQuery REST endpoint, used when no endpoint override is configured.
const DEFAULT_ENDPOINT: &str = concat!("https://", default_endpoint_host!());

/// The public Cloud Storage JSON API endpoint, used when no override is configured.
const DEFAULT_GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// The endpoint every directly issued request is built on, without its trailing slash.
pub(crate) fn endpoint(configured: Option<&str>) -> &str {
    configured.unwrap_or(DEFAULT_ENDPOINT).trim_end_matches('/')
}

impl ServiceEndpoints {
    /// The BigQuery REST endpoint, without its trailing slash.
    pub(crate) fn bigquery(&self) -> &str {
        endpoint(self.get(ServiceEndpointKind::BigQuery))
    }

    /// The Cloud Storage JSON API endpoint, without its trailing slash.
    pub(crate) fn gcs(&self) -> &str {
        self.get(ServiceEndpointKind::Gcs)
            .unwrap_or(DEFAULT_GCS_ENDPOINT)
            .trim_end_matches('/')
    }

    /// The Dataproc endpoint for `region`, which is regional: `{region}-dataproc.googleapis.com`.
    ///
    /// An override replaces the whole host, region included, so one mock server can answer for
    /// every region a test configures.
    pub(crate) fn dataproc(&self, region: &str, key: BigQueryStatementOption) -> Result<String> {
        regional_endpoint(
            self.get(ServiceEndpointKind::Dataproc),
            region,
            "dataproc",
            key,
        )
    }

    /// The Vertex AI endpoint for `region`: `{region}-aiplatform.googleapis.com`, overridable
    /// whole.
    pub(crate) fn vertex(&self, region: &str, key: BigQueryStatementOption) -> Result<String> {
        regional_endpoint(
            self.get(ServiceEndpointKind::Vertex),
            region,
            "aiplatform",
            key,
        )
    }
}

/// The regional host of one service, checked so a statement option cannot move it.
///
/// A configured override has already passed the untrusted-endpoint rules. A host built from
/// `region` also receives a bearer token, so the URL is parsed as `reqwest` parses it (ending a
/// host at `\` and `@` as well as `/`), and the resulting host must be a Google API host.
fn regional_endpoint(
    configured: Option<&str>,
    region: &str,
    service: &str,
    key: BigQueryStatementOption,
) -> Result<String> {
    if let Some(configured) = configured {
        return Ok(configured.trim_end_matches('/').to_owned());
    }
    let endpoint = format!("https://{region}-{service}.googleapis.com");
    let trusted = reqwest::Url::parse(&endpoint)
        .ok()
        .is_some_and(|url| url.host_str().is_some_and(is_google_api_host));
    if trusted {
        Ok(endpoint)
    } else {
        Err(invalid_argument(format!(
            "BigQuery option {} would address a {service} host outside googleapis.com, which \
             would send it an access token minted for the configured identity",
            key.as_str()
        )))
    }
}

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;

    use std::sync::Arc;

    use super::normalize_endpoint;
    use crate::config::{ServiceEndpointKind, ServiceEndpoints};
    use crate::{BigQueryDatabaseOption, BigQueryStatementOption};

    const KEY: &str = BigQueryDatabaseOption::Endpoint.as_str();

    #[test]
    fn endpoint_normalization_is_strict_and_clearable() {
        for (input, expected) in [
            ("bigquery.googleapis.com", "https://bigquery.googleapis.com"),
            ("http://localhost:9050/", "http://localhost:9050"),
            (
                "HTTPS://bigquery.googleapis.com/",
                "https://bigquery.googleapis.com",
            ),
            ("https://[::1]:9050", "https://[::1]:9050"),
            (
                "http://[::ffff:127.0.0.1]:9050",
                "http://[::ffff:127.0.0.1]:9050",
            ),
        ] {
            assert_eq!(
                normalize_endpoint(input.to_owned(), KEY).unwrap(),
                Some(expected.to_owned())
            );
        }
        assert_eq!(normalize_endpoint(String::new(), KEY).unwrap(), None);
        for invalid in [
            "ftp://localhost",
            "https://",
            "https://:9050",
            "https://localhost:0",
            "https://localhost:99999",
            "https://user@localhost",
            "https://localhost/path",
            "https://localhost?query",
            "https://localhost fragment",
            "https://[1234]",
            "https://[:::1]",
            "https://[1::2::3]",
            "https://[12345::1]",
            "https://[1:2:3:4:5:6:7:8:9]",
            "https://[::ffff:256.0.0.1]",
            "::1",
        ] {
            assert_eq!(
                normalize_endpoint(invalid.to_owned(), KEY)
                    .unwrap_err()
                    .status,
                Status::InvalidArguments,
                "{invalid}"
            );
        }
    }

    /// Each rejection names the option key the value arrived under, as every other option error
    /// does, rather than the service.
    #[test]
    fn a_rejected_endpoint_names_the_option_it_was_set_through() {
        let storage = BigQueryDatabaseOption::StorageEndpoint.as_str();
        assert_eq!(
            normalize_endpoint("https://host/path".to_owned(), storage)
                .unwrap_err()
                .message,
            "BigQuery option bigquery.storage_endpoint must contain a valid host and optional \
             port, with no path or user information"
        );
        assert_eq!(
            normalize_endpoint("ftp://localhost".to_owned(), storage)
                .unwrap_err()
                .message,
            "BigQuery option bigquery.storage_endpoint must use HTTP or HTTPS"
        );
        assert_eq!(
            normalize_endpoint("https://localhost?query".to_owned(), storage)
                .unwrap_err()
                .message,
            "BigQuery option bigquery.storage_endpoint must not contain whitespace, a query, or \
             a fragment"
        );

        // The key is the caller's own spelling, aliases included, so a value set through a
        // legacy name is reported under that name.
        assert_eq!(
            normalize_endpoint(
                "https://host/path".to_owned(),
                "adbc.bigquery.sql.storage_read_api_endpoint",
            )
            .unwrap_err()
            .message,
            "BigQuery option adbc.bigquery.sql.storage_read_api_endpoint must contain a valid \
             host and optional port, with no path or user information"
        );
    }

    /// A regional host built from a statement option receives a bearer token, so it follows the
    /// configured-endpoint trust rule. `reqwest`'s parser ends a special scheme's host at a
    /// backslash, so `attacker.example\` resolves to `attacker.example`, not a `googleapis.com`
    /// subdomain.
    #[test]
    fn a_regional_host_built_from_an_option_must_still_be_a_google_api_host() {
        let key = BigQueryStatementOption::DataprocComputeRegion;
        let defaults = ServiceEndpoints::default();
        assert_eq!(
            defaults.dataproc("us-central1", key).unwrap(),
            "https://us-central1-dataproc.googleapis.com"
        );
        assert_eq!(
            defaults.vertex("europe-west4", key).unwrap(),
            "https://europe-west4-aiplatform.googleapis.com"
        );

        for region in [
            "attacker.example\\",
            "attacker.example\\?",
            "attacker.example/",
            "user@attacker.example\\",
        ] {
            let failure = defaults.dataproc(region, key).unwrap_err();
            assert_eq!(failure.status, Status::InvalidArguments, "{region:?}");
            assert!(
                failure.message.contains(key.as_str()),
                "{}",
                failure.message
            );
        }

        // An override is taken as given: it has already been through the untrusted-endpoint
        // rules.
        let mut overridden = ServiceEndpoints::default();
        overridden.set(
            ServiceEndpointKind::Dataproc,
            Some(Arc::from("http://127.0.0.1:9/")),
        );
        assert_eq!(
            overridden.dataproc("us-central1", key).unwrap(),
            "http://127.0.0.1:9"
        );
    }
}
