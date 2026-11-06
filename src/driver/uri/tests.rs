use crate::BigQueryAuthenticationType;
use crate::BigQueryDatabaseOption;

use super::parse_bigquery_uri;
use crate::test_support::errors::assert_message_omits;
use crate::test_support::options::{option_string, set_option, set_option_error};

fn value(
    options: &[(BigQueryDatabaseOption, String)],
    option: BigQueryDatabaseOption,
) -> Option<&str> {
    options
        .iter()
        .find(|(candidate, _)| *candidate == option)
        .map(|(_, value)| value.as_str())
}

#[test]
fn parses_authority_project_and_non_authentication_parameters() {
    let parsed = parse_bigquery_uri(
        "bigquery://localhost:8443/my%2Dproject?DatasetId=sample_data&Location=EU&QuotaProject=quota-project&ImpersonateTargetPrincipal=service%40example.com&ImpersonateDelegates=first%40example.com%2Csecond%40example.com&ImpersonateScopes=scope%3Aone%2Cscope%3Atwo&ImpersonateLifetime=30m",
    )
    .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ProjectId),
        Some("my-project")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::Endpoint),
        Some("https://localhost:8443")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::ApplicationDefault.as_str())
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::DatasetId),
        Some("sample_data")
    );
    assert_eq!(value(&parsed, BigQueryDatabaseOption::Location), Some("EU"));
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationQuotaProject),
        Some("quota-project")
    );
    assert_eq!(
        value(
            &parsed,
            BigQueryDatabaseOption::ImpersonationTargetPrincipal
        ),
        Some("service@example.com")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ImpersonationDelegates),
        Some("first@example.com,second@example.com")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ImpersonationScopes),
        Some("scope:one,scope:two")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ImpersonationLifetime),
        Some("30m")
    );
}

#[test]
fn applies_endpoint_defaults_and_supports_port_only_authorities() {
    for (uri, endpoint) in [
        ("bigquery:///project", "https://bigquery.googleapis.com:443"),
        ("BIGQUERY:///project", "https://bigquery.googleapis.com:443"),
        ("bigquery://example.com/project", "https://example.com:443"),
        (
            "bigquery://:9050/project",
            "https://bigquery.googleapis.com:9050",
        ),
        ("bigquery://[::1]/project", "https://[::1]:443"),
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap();
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::Endpoint),
            Some(endpoint),
            "{uri}"
        );
    }
}

#[test]
fn parses_each_oauth_type_and_decodes_secrets_once() {
    let parsed = parse_bigquery_uri("bigquery:///project?OAuthType=0").unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::ApplicationDefault.as_str())
    );

    let parsed =
        parse_bigquery_uri("bigquery:///project?OAuthType=1&AuthCredentials=%2Ftmp%2Fkey%252Ejson")
            .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::JsonCredentialFile.as_str())
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationCredentials),
        Some("/tmp/key%2Ejson")
    );

    let parsed =
        parse_bigquery_uri("bigquery:///project?OAuthType=2&AuthCredentials=%257Bsecret%257D")
            .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::JsonCredentialString.as_str())
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationCredentials),
        Some("%7Bsecret%7D")
    );

    let parsed = parse_bigquery_uri(
        "bigquery:///project?OAuthType=3&AuthClientId=client%252Bid&AuthClientSecret=secret%252Bvalue&AuthRefreshToken=token%252Bvalue",
    )
    .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::UserAuthentication.as_str())
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationClientId),
        Some("client%2Bid")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationClientSecret),
        Some("secret%2Bvalue")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationRefreshToken),
        Some("token%2Bvalue")
    );
}

#[test]
fn rejects_malformed_unknown_duplicate_and_incomplete_parameters() {
    for uri in [
        "",
        "https:///project",
        "bigquery://host",
        "bigquery://host/",
        "bigquery://host/one/two",
        "bigquery:///abc%2Fdef",
        "bigquery:///abc%5Cdef",
        "bigquery:///abc%00def",
        "bigquery:///abc%7Fdef",
        "bigquery:///%2E",
        "bigquery:///%2E%2E",
        "bigquery://user@host/project",
        "bigquery://host:0/project",
        "bigquery://host:65536/project",
        "bigquery:///project#fragment",
        "bigquery:///project?Location",
        "bigquery:///project?=value",
        "bigquery:///project?Unknown=value",
        "bigquery:///project?Location=EU&Location=US",
        "bigquery:///project?Location=bad%2",
        "bigquery:///project?OAuthType=4",
        "bigquery:///project?OAuthType=1",
        "bigquery:///project?OAuthType=2&AuthCredentials=",
        "bigquery:///project?OAuthType=3&AuthClientId=id&AuthClientSecret=secret",
        "bigquery:///project?AuthCredentials=secret",
        "bigquery:///project?OAuthType=1&AuthCredentials=secret&AuthRefreshToken=token",
        "bigquery:///project?ImpersonateDelegates=first%40example.com",
        "bigquery:///project?ImpersonateLifetime=30m",
    ] {
        assert!(parse_bigquery_uri(uri).is_err(), "{uri}");
    }
}

/// An empty parameter means unset, as `OAuthType=` does in the Go driver, so the checks that
/// reject a parameter and those that require one agree on it.
#[test]
fn an_empty_query_parameter_value_means_unset() {
    for uri in [
        "bigquery:///my-project?AuthCredentials=",
        "bigquery:///my-project?OAuthType=&AuthCredentials=",
        "bigquery:///my-project?AuthClientId=&AuthClientSecret=&AuthRefreshToken=",
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap_or_else(|error| panic!("{uri}: {error}"));
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::AuthenticationType),
            Some(BigQueryAuthenticationType::ApplicationDefault.as_str()),
            "{uri}"
        );
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::AuthenticationCredentials),
            None,
            "{uri}"
        );
    }

    // An empty parameter the selected type does not take is absent, not a conflict.
    let parsed = parse_bigquery_uri(
        "bigquery:///my-project?OAuthType=1&AuthCredentials=/key.json&AuthClientId=",
    )
    .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::JsonCredentialFile.as_str())
    );

    // A parameter that carries something still has to agree with the type that was named.
    for (uri, expected) in [
        (
            "bigquery:///my-project?AuthCredentials=secret",
            "BigQuery URI AuthCredentials requires OAuthType=1 or 2",
        ),
        (
            "bigquery:///my-project?OAuthType=1&AuthCredentials=",
            "BigQuery URI OAuthType=1 or 2 requires AuthCredentials",
        ),
        (
            "bigquery:///my-project?AuthRefreshToken=token",
            "BigQuery URI user OAuth parameters require OAuthType=3",
        ),
    ] {
        assert_eq!(
            parse_bigquery_uri(uri).unwrap_err().message,
            expected,
            "{uri}"
        );
    }
}

/// One endpoint-authority grammar. The authority the URI carries becomes `bigquery.endpoint`, so
/// an authority the option would reject has to be rejected here, while the caller can still be
/// told it is the URI's authority rather than an option they never set.
#[test]
fn the_uri_endpoint_authority_uses_the_option_grammar() {
    for (uri, expected) in [
        (
            "bigquery://ex\u{e4}mple.com/my-project",
            "BigQuery URI endpoint host \"ex\u{e4}mple.com\" must be a host name of ASCII letters, \
             digits, dots, hyphens, and underscores, or a bracketed IPv6 address",
        ),
        (
            "bigquery://[zzzz]/my-project",
            "BigQuery URI endpoint host \"zzzz\" must be a host name of ASCII letters, digits, \
             dots, hyphens, and underscores, or a bracketed IPv6 address",
        ),
    ] {
        assert_eq!(
            parse_bigquery_uri(uri).unwrap_err().message,
            expected,
            "{uri}"
        );
    }

    // What the URI does accept, the option accepts unchanged: there is one grammar, not two.
    for uri in [
        "bigquery:///my-project",
        "bigquery://example.com/my-project",
        "bigquery://host_name:8443/my-project",
        "bigquery://[::1]:8443/my-project",
        "bigquery://[::1]/my-project",
        "bigquery://:8443/my-project",
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap_or_else(|error| panic!("{uri}: {error}"));
        let endpoint = value(&parsed, BigQueryDatabaseOption::Endpoint).unwrap();
        crate::config::endpoints::normalize_endpoint(endpoint.to_owned(), "bigquery.endpoint")
            .unwrap_or_else(|error| panic!("{uri}: {endpoint}: {error}"));
    }
}

/// dbt-core 2.0 sets the impersonation scopes on every connection, impersonating or not, which
/// is why `ImpersonationConfig::is_configured` ignores them. A URI carrying the same profile
/// must be accepted too, or the two option paths diverge.
#[test]
fn accepts_impersonation_scopes_without_a_target_principal() {
    let parsed = parse_bigquery_uri(
        "bigquery:///project?ImpersonateScopes=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fbigquery",
    )
    .unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ImpersonationScopes),
        Some("https://www.googleapis.com/auth/bigquery")
    );
    assert_eq!(
        value(
            &parsed,
            BigQueryDatabaseOption::ImpersonationTargetPrincipal
        ),
        None
    );
}

/// A rejected URI must show what was wrong with the caller's spelling: the parameter that
/// was not recognized and what is, the parameter left without a value, the port text that
/// was not a number, and the URI form that would have worked.
#[test]
fn rejections_name_the_parameter_or_the_form_that_would_have_worked() {
    for (uri, expected) in [
        (
            "bigquery://my-project",
            "BigQuery URI must contain a project ID path, as in bigquery:///my-project or \
             bigquery://bigquery.googleapis.com/my-project",
        ),
        (
            "bigquery:///my-project?Unknown=value",
            "BigQuery URI query parameter Unknown is not one of OAuthType, AuthCredentials, \
             AuthClientId, AuthClientSecret, AuthRefreshToken, DatasetId, Location, \
             QuotaProject, ImpersonateTargetPrincipal, ImpersonateDelegates, \
             ImpersonateScopes, or ImpersonateLifetime",
        ),
        (
            "bigquery:///my-project?Location",
            "BigQuery URI query parameter Location must have a value",
        ),
        // A component with no `=` is only sometimes a parameter name: `split('&')` also splits
        // a credential value carrying an unencoded `&`, and its tail must not be echoed back.
        (
            "bigquery:///my-project?OAuthType=2&AuthCredentials={\"private_key\":\"a&b\"}",
            "BigQuery URI query component 3 (3 characters) has no value; each component must be \
             name=value, with any & in a value encoded as %26",
        ),
        (
            "bigquery:///my-project?a",
            "BigQuery URI query component 1 (1 character) has no value; each component must be \
             name=value, with any & in a value encoded as %26",
        ),
        (
            "bigquery:///my-project?location=EU",
            "BigQuery URI query parameter location is spelled Location",
        ),
        (
            "bigquery://host:port/my-project",
            "BigQuery URI endpoint port \"port\" must be decimal digits only",
        ),
        (
            "bigquery://[::1/my-project",
            "BigQuery URI endpoint authority \"[::1\" must be host[:port]",
        ),
        (
            "bigquery://user@host/my-project",
            "BigQuery URI endpoint authority must be host[:port], without user information or \
             whitespace",
        ),
        (
            "bigquery:///my-project?OAuthType=4",
            "BigQuery URI OAuthType must be 0, 1, 2, or 3",
        ),
        (
            "bigquery:///my-project?Location=EU%2",
            "BigQuery URI query parameter Location's value has malformed percent encoding at byte \
             offset 2",
        ),
    ] {
        assert_eq!(
            parse_bigquery_uri(uri).unwrap_err().message,
            expected,
            "{uri}"
        );
    }
}

/// A URI assembled by concatenation may carry punctuation that names no parameter: an empty `?`,
/// a trailing or doubled `&`, or a trailing slash on the path. Go's `url.ParseQuery` skips the
/// empty parameters, and so does this parser.
#[test]
fn punctuation_that_names_no_parameter_or_segment_is_accepted() {
    for uri in [
        "bigquery:///my-project?",
        "bigquery:///my-project?&",
        "bigquery:///my-project?DatasetId=sample_data&",
        "bigquery:///my-project?&DatasetId=sample_data",
        "bigquery:///my-project?DatasetId=sample_data&&Location=EU",
        "bigquery:///my-project/",
        "bigquery://bigquery.googleapis.com/my-project/",
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap_or_else(|error| panic!("{uri}: {error}"));
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::ProjectId),
            Some("my-project"),
            "{uri}"
        );
    }

    // An empty OAuthType selects the default rather than failing, which is also what the Go
    // driver does with the empty string its `Get` returns for a valueless parameter.
    let parsed = parse_bigquery_uri("bigquery:///my-project?OAuthType=").unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::AuthenticationType),
        Some(BigQueryAuthenticationType::ApplicationDefault.as_str())
    );
}

/// A C string cannot carry a NUL, so `AdbcDatabaseGetOption` could not read back a value holding
/// one; a percent-encoded NUL is refused when the URI is set, whichever parameter carries it.
#[test]
fn a_percent_encoded_nul_is_rejected() {
    for (uri, part) in [
        (
            "bigquery:///my-project?Location=%00",
            "query parameter Location's value",
        ),
        (
            "bigquery:///my-project?Location=EU%00",
            "query parameter Location's value",
        ),
        (
            "bigquery:///my-project?ImpersonateScopes=%00",
            "query parameter ImpersonateScopes's value",
        ),
        (
            "bigquery:///my-project?OAuthType=3&AuthClientId=a%00b&AuthClientSecret=s&AuthRefreshToken=t",
            "query parameter AuthClientId's value",
        ),
        (
            "bigquery:///my-project?Location%00=EU",
            "query component 1's name",
        ),
        ("bigquery:///my%00project", "project path"),
    ] {
        let error = parse_bigquery_uri(uri).unwrap_err();
        assert_eq!(
            error.status,
            adbc_core::error::Status::InvalidArguments,
            "{uri}"
        );
        assert_eq!(
            error.message,
            format!("BigQuery URI {part} must not contain a percent-encoded NUL character"),
            "{uri}"
        );
    }
}

/// User information is refused before any part of the authority is parsed, so no shape of it
/// reaches a message that quotes a host or port: an empty user name would otherwise make the
/// password read as a port.
#[test]
fn a_password_in_the_authority_is_never_echoed() {
    for uri in [
        "bigquery://:hunter2-secret@bigquery.googleapis.com/my-project",
        "bigquery://:hunter2-secret@bigquery.googleapis.com:/my-project",
        "bigquery://:hunter2-secret@bigquery.googleapis.com:443/my-project",
        "bigquery://user:hunter2-secret@bigquery.googleapis.com/my-project",
        "bigquery://hunter2-secret@bigquery.googleapis.com/my-project",
        "bigquery://[::1]:hunter2-secret@host/my-project",
        "bigquery://host:hunter2 secret/my-project",
    ] {
        let error = parse_bigquery_uri(uri).unwrap_err();
        assert_eq!(
            error.message,
            "BigQuery URI endpoint authority must be host[:port], without user information or \
             whitespace",
            "{uri}"
        );
        assert_message_omits(&error, &["hunter2"]);
    }
}

#[test]
fn a_credential_in_oauth_type_is_not_echoed() {
    for value in [
        "misplaced-token",
        "%7B%22private_key%22%3A%22misplaced-key%22%7D",
    ] {
        let error =
            parse_bigquery_uri(&format!("bigquery:///my-project?OAuthType={value}")).unwrap_err();
        assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
        assert_eq!(
            error.message,
            "BigQuery URI OAuthType must be 0, 1, 2, or 3"
        );
        assert_message_omits(&error, &["misplaced-token", "misplaced-key", "private_key"]);
    }
}

/// An empty port is the default one, as RFC 3986 and Go's `url.URL.Port` read it, but a second
/// colon still makes the authority malformed.
#[test]
fn an_empty_port_is_the_default_port() {
    for (uri, endpoint) in [
        ("bigquery://example.com:/project", "https://example.com:443"),
        (
            "bigquery://:/project",
            "https://bigquery.googleapis.com:443",
        ),
        ("bigquery://[::1]:/project", "https://[::1]:443"),
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap_or_else(|error| panic!("{uri}: {error}"));
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::Endpoint),
            Some(endpoint),
            "{uri}"
        );
    }
    assert_eq!(
        parse_bigquery_uri("bigquery://example.com:443:/project")
            .unwrap_err()
            .message,
        "BigQuery URI endpoint authority \"example.com:443:\" must be host[:port], with an IPv6 \
         host enclosed in brackets"
    );
}

/// A credential value carrying an unencoded `&` is split into a parameter and a tail. When the
/// tail has an `=` it reads as a parameter whose name is part of the credential, so that name is
/// not echoed.
#[test]
fn the_tail_of_a_credential_split_at_an_ampersand_is_not_echoed() {
    let error = parse_bigquery_uri(
        "bigquery:///my-project?OAuthType=2&AuthCredentials={\"private_key\":\"secret&tail=x\"}",
    )
    .unwrap_err();
    assert_eq!(
        error.message,
        "BigQuery URI query component 3 is not a parameter (its name is not echoed, as it may be \
         the tail of the credential before it); encode any & in a credential as %26, or check the \
         parameter's spelling"
    );
    // Elsewhere an unknown name is quoted, since it is most likely a misspelled parameter.
    assert!(
        parse_bigquery_uri("bigquery:///my-project?Location=EU&Datasetid=x")
            .unwrap_err()
            .message
            .contains("Datasetid")
    );
}

/// The decoded path is an opaque project identifier. The URI grammar still separates its path
/// and query, so characters such as `?` and `#` must be encoded to reach the project unchanged.
#[test]
fn project_uri_paths_preserve_identifiers_for_service_validation() {
    for (uri, expected) in [
        (
            "bigquery:///example.com:my-project",
            "example.com:my-project",
        ),
        (
            "bigquery:///example.com%3Amy-project",
            "example.com:my-project",
        ),
        ("bigquery:///123456789012", "123456789012"),
        ("bigquery:///abcde", "abcde"),
        ("bigquery:///INVALID", "INVALID"),
        ("bigquery:///abcdef-", "abcdef-"),
        ("bigquery:///abc%3Fdef", "abc?def"),
        ("bigquery:///abc%23def", "abc#def"),
        ("bigquery:///abc%252Fdef", "abc%2Fdef"),
        ("bigquery:///%C3%A5bcdef", "åbcdef"),
    ] {
        let parsed = parse_bigquery_uri(uri).unwrap();
        assert_eq!(
            value(&parsed, BigQueryDatabaseOption::ProjectId),
            Some(expected),
            "{uri}"
        );
    }
}

/// The authority is the endpoint, never the project: `bigquery://a/b` reads `a` as a REST host
/// and `b` as the project, as the Go driver does. Other drivers' `scheme://database/name` shape
/// suggests the opposite, and both halves of `bigquery://my-project/my-dataset` are valid here;
/// the endpoint trust rule refuses it at connection time.
#[test]
fn the_authority_is_the_endpoint_and_the_path_is_the_project() {
    let parsed = parse_bigquery_uri("bigquery://my-project/my-dataset").unwrap();
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::ProjectId),
        Some("my-dataset")
    );
    assert_eq!(
        value(&parsed, BigQueryDatabaseOption::Endpoint),
        Some("https://my-project:443")
    );
}

/// The `uri` option cannot read back a value its own option refuses: `bigquery.auth.credentials`,
/// `bigquery.auth.client_secret` and `bigquery.auth.refresh_token` are write-only, and a
/// `bigquery://` URI can carry all three. In ADBC the reader is often not the setter.
#[test]
fn the_uri_option_cannot_read_back_a_secret_its_own_option_refuses() {
    use adbc_core::Optionable;
    use adbc_core::options::OptionDatabase;

    use crate::driver::tests::new_database;

    // One case per write-only option a URI can carry, each with a value that appears nowhere
    // else in the URI.
    for (uri, write_only, secrets) in [
        (
            "bigquery://uri.example/uri-project?OAuthType=3&AuthClientId=client-id\
             &AuthClientSecret=leaked-client-secret&AuthRefreshToken=leaked-refresh-token",
            &[
                BigQueryDatabaseOption::AuthenticationClientSecret,
                BigQueryDatabaseOption::AuthenticationRefreshToken,
            ][..],
            &["leaked-client-secret", "leaked-refresh-token"][..],
        ),
        (
            "bigquery:///uri-project?OAuthType=2\
             &AuthCredentials=%7B%22private_key%22%3A%22leaked-private-key%22%7D",
            &[BigQueryDatabaseOption::AuthenticationCredentials][..],
            &["leaked-private-key"][..],
        ),
    ] {
        let mut database = new_database();
        set_option(&mut database, OptionDatabase::Uri, uri);
        let read_back = option_string(&database, OptionDatabase::Uri);
        for secret in secrets {
            assert!(
                !read_back.contains(secret),
                "{secret} leaked into {read_back}"
            );
            // The option the value was applied to still refuses it, which is the rule the URI
            // getter must not undercut.
            for option in write_only {
                assert!(
                    database
                        .get_option_string((*option).into())
                        .unwrap_err()
                        .message
                        .contains("write-only"),
                    "{option:?}"
                );
            }
        }
        // Everything that is not a credential survives, which is why the answer is worth giving.
        for expected in ["bigquery:", "uri-project", "OAuthType=", "<redacted>"] {
            assert!(
                read_back.contains(expected),
                "{expected} missing from {read_back}"
            );
        }
        // The `Debug` output holds the redacted copy too.
        for secret in secrets {
            assert!(!format!("{database:?}").contains(secret), "{secret}");
        }
    }

    // A URI without a query is returned unchanged, and one whose only parameters are harmless
    // keeps every value.
    assert_eq!(
        super::redact_uri("bigquery:///uri-project"),
        "bigquery:///uri-project"
    );
    assert_eq!(
        super::redact_uri("bigquery:///p?DatasetId=d&Location=EU"),
        "bigquery:///p?DatasetId=d&Location=EU"
    );
    // A percent-encoded or `+`-spelled parameter name is decoded before it is matched, so it
    // cannot smuggle a credential past the table.
    assert_eq!(
        super::redact_uri("bigquery:///p?%41uthClientSecret=s"),
        "bigquery:///p?%41uthClientSecret=<redacted>"
    );
}

/// An impersonation principal is interpolated straight into IAM Credentials'
/// `/v1/projects/-/serviceAccounts/{principal}:{method}` URL by `google-cloud-auth`, so a value
/// carrying `/`, `?`, `#` or `%` addresses a different IAM method or path than the caller asked
/// for. It is checked against the charset the external-account path enforces.
#[test]
fn an_impersonation_principal_must_be_a_service_account_email_address() {
    use adbc_core::options::OptionDatabase;

    use crate::driver::tests::new_database;

    let refused = |uri: &str| {
        let mut database = new_database();
        set_option_error(&mut database, OptionDatabase::Uri, uri)
    };

    // `:generateIdToken?x=` percent-encoded: the shape that reaches another IAM method.
    let error = refused(
        "bigquery:///my-project?ImpersonateTargetPrincipal=sa%40p.iam.gserviceaccount.com%3AgenerateIdToken%3Fx%3D",
    );
    assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        error.message.starts_with(
            "BigQuery option bigquery.impersonate.target_principal must be a service account \
             email address made of ASCII letters, digits, and the characters @ . - _"
        ),
        "{}",
        error.message
    );

    // A delegate list names the offending entry's position, because the list itself is not
    // echoed and a chain of three otherwise says nothing about which one is wrong.
    let error = refused(
        "bigquery:///my-project?ImpersonateTargetPrincipal=sa%40p.iam.gserviceaccount.com\
         &ImpersonateDelegates=first%40example.com%2Csecond%2Fpath",
    );
    assert!(
        error.message.starts_with(
            "BigQuery option bigquery.impersonate.delegates entry 2 must be a service"
        ),
        "{}",
        error.message
    );

    // Ordinary principals still pass, and an empty value still clears the option.
    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Uri,
        "bigquery:///my-project?ImpersonateTargetPrincipal=my-sa%40my-project.iam.gserviceaccount.com\
         &ImpersonateDelegates=first%40example.com%2Csecond%40example.com",
    );
    assert_eq!(
        option_string(
            &database,
            BigQueryDatabaseOption::ImpersonationTargetPrincipal
        ),
        "my-sa@my-project.iam.gserviceaccount.com"
    );
}
