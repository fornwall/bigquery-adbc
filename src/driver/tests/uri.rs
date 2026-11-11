//! `uri.rs`: what a URI replaces, what it leaves alone, which order it loses to explicit
//! options in, and what it refuses.

use super::*;
use crate::test_support::errors::{assert_failure, assert_message_omits};
use crate::test_support::options::{option_string, set_option, set_option_error};

#[test]
fn last_uri_option_wins_and_is_applied_before_explicit_options() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("bigquery:///first-project".to_owned()),
            ),
            (
                BigQueryDatabaseOption::Location.into(),
                OptionValue::String("EU".to_owned()),
            ),
            (
                OptionDatabase::Uri,
                OptionValue::String("bigquery:///second-project".to_owned()),
            ),
        ])
        .unwrap();

    assert_eq!(
        database.config.project_id.as_deref(),
        Some("second-project")
    );
    assert_eq!(
        database.config.location.as_deref(),
        Some("EU"),
        "explicit options override the URI regardless of input order"
    );
}

#[test]
fn uri_replaces_the_complete_database_configuration() {
    let mut database = new_database();
    for (option, value) in [
        (BigQueryDatabaseOption::ProjectId, "old-project"),
        (BigQueryDatabaseOption::DatasetId, "old_dataset"),
        (
            BigQueryDatabaseOption::AuthenticationType,
            BigQueryAuthenticationType::JsonCredentialString.as_str(),
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentials,
            AUTHORIZED_USER_JSON,
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentialsType,
            BigQueryCredentialsType::AuthorizedUser.as_str(),
        ),
        (BigQueryDatabaseOption::AuthenticationClientId, "old-client"),
        (
            BigQueryDatabaseOption::AuthenticationClientSecret,
            "old-secret",
        ),
        (
            BigQueryDatabaseOption::AuthenticationRefreshToken,
            "old-token",
        ),
        (
            BigQueryDatabaseOption::AuthenticationAccessToken,
            "old-access-token",
        ),
        (
            BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
            "https://old-token.example/token",
        ),
        (
            BigQueryDatabaseOption::AuthenticationQuotaProject,
            "old-quota-project",
        ),
        (BigQueryDatabaseOption::Endpoint, "old.example.com"),
        (
            BigQueryDatabaseOption::StorageEndpoint,
            "old-storage.example.com",
        ),
        (BigQueryDatabaseOption::AllowUntrustedEndpoint, "true"),
        (BigQueryDatabaseOption::Location, "EU"),
        (
            BigQueryDatabaseOption::ImpersonationTargetPrincipal,
            "old-target@example.com",
        ),
        (
            BigQueryDatabaseOption::ImpersonationDelegates,
            "old-first@example.com,old-second@example.com",
        ),
        (
            BigQueryDatabaseOption::ImpersonationScopes,
            "old-scope-one,old-scope-two",
        ),
        (BigQueryDatabaseOption::ImpersonationLifetime, "30m"),
    ] {
        set_option(&mut database, option, value);
    }
    let old_clients = database.clients().clone();

    set_option(
        &mut database,
        OptionDatabase::Uri,
        "bigquery:///new-project",
    );

    // Field for field what the URI alone configures; `Debug` covers every field, and shows
    // whether each secret is set without its value.
    let mut fresh = new_database();
    set_option(&mut fresh, OptionDatabase::Uri, "bigquery:///new-project");
    assert_eq!(
        format!("{:?}", database.config),
        format!("{:?}", fresh.config)
    );
    assert_eq!(database.config.project_id.as_deref(), Some("new-project"));
    assert!(!Arc::ptr_eq(database.clients(), &old_clients));
}

#[test]
fn rich_uri_applies_every_supported_parameter_and_redacts_secrets() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            "bigquery://example.com:8443/uri-project?OAuthType=3&AuthClientId=client-id&AuthClientSecret=uri-secret&AuthRefreshToken=uri-token&DatasetId=uri_dataset&Location=EU&QuotaProject=quota-project&ImpersonateTargetPrincipal=target%40uri-project.iam.gserviceaccount.com&ImpersonateDelegates=first%40uri-project.iam.gserviceaccount.com%2Csecond%40uri-project.iam.gserviceaccount.com&ImpersonateScopes=scope-one%2Cscope-two&ImpersonateLifetime=30m".into(),
        )])
        .unwrap();

    assert_eq!(database.config.project_id.as_deref(), Some("uri-project"));
    assert_eq!(database.config.dataset_id.as_deref(), Some("uri_dataset"));
    assert_eq!(database.config.location.as_deref(), Some("EU"));
    assert_eq!(
        database.config.endpoints.get(ServiceEndpointKind::BigQuery),
        Some("https://example.com:8443")
    );
    assert_eq!(
        database.config.quota_project.as_deref(),
        Some("quota-project")
    );
    assert_eq!(
        database.config.authentication_type,
        BigQueryAuthenticationType::UserAuthentication
    );
    assert_eq!(
        database.config.user_authentication.client_id.as_deref(),
        Some("client-id")
    );
    assert_eq!(
        database.config.impersonation.target_principal.as_deref(),
        Some("target@uri-project.iam.gserviceaccount.com")
    );
    assert_eq!(
        database.config.impersonation.delegates.join(","),
        "first@uri-project.iam.gserviceaccount.com,second@uri-project.iam.gserviceaccount.com"
    );
    assert_eq!(
        database.config.impersonation.scopes.join(","),
        "scope-one,scope-two"
    );
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::ImpersonationLifetime),
        "30m0s"
    );
    let debug = format!("{database:?}");
    assert!(!debug.contains("uri-secret"));
    assert!(!debug.contains("uri-token"));
}

#[test]
fn explicit_initialization_options_override_uri_regardless_of_order() {
    for options in [
        vec![
            (
                BigQueryDatabaseOption::ProjectId.into(),
                "explicit-project".into(),
            ),
            (
                BigQueryDatabaseOption::DatasetId.into(),
                "explicit_dataset".into(),
            ),
            (BigQueryDatabaseOption::Location.into(), "US".into()),
            (
                BigQueryDatabaseOption::Endpoint.into(),
                "http://127.0.0.1:9050".into(),
            ),
            (
                OptionDatabase::Uri,
                "bigquery://uri.example/uri-project?DatasetId=uri_dataset&Location=EU".into(),
            ),
        ],
        vec![
            (
                OptionDatabase::Uri,
                "bigquery://uri.example/uri-project?DatasetId=uri_dataset&Location=EU".into(),
            ),
            (
                BigQueryDatabaseOption::ProjectId.into(),
                "explicit-project".into(),
            ),
            (
                BigQueryDatabaseOption::DatasetId.into(),
                "explicit_dataset".into(),
            ),
            (BigQueryDatabaseOption::Location.into(), "US".into()),
            (
                BigQueryDatabaseOption::Endpoint.into(),
                "http://127.0.0.1:9050".into(),
            ),
        ],
    ] {
        let mut driver = BigQueryDriver::try_new().unwrap();
        let database = driver.new_database_with_opts(options).unwrap();
        assert_eq!(
            database.config.project_id.as_deref(),
            Some("explicit-project")
        );
        assert_eq!(
            database.config.dataset_id.as_deref(),
            Some("explicit_dataset")
        );
        assert_eq!(database.config.location.as_deref(), Some("US"));
        assert_eq!(
            database.config.endpoints.get(ServiceEndpointKind::BigQuery),
            Some("http://127.0.0.1:9050")
        );
    }
}

#[test]
fn uri_updates_are_atomic() {
    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Uri,
        "bigquery://initial.example/initial-project?DatasetId=initial_dataset&Location=EU",
    );
    let clients = database.clients().clone();

    let error = set_option_error(
        &mut database,
        OptionDatabase::Uri,
        "bigquery://changed.example/changed-project?DatasetId=not-valid&Location=US",
    );
    assert_eq!(error.status, Status::InvalidArguments);
    // The fields the rejected URI would have changed are asserted one by one rather than through
    // `Debug`: the redacted `Debug` elides fields, so a field it hides could be rewritten by a
    // failed update without a string comparison noticing.
    assert_eq!(
        database.config.project_id.as_deref(),
        Some("initial-project")
    );
    assert_eq!(
        database.config.dataset_id.as_deref(),
        Some("initial_dataset")
    );
    assert_eq!(database.config.location.as_deref(), Some("EU"));
    assert_eq!(
        database.config.endpoints.get(ServiceEndpointKind::BigQuery),
        Some("https://initial.example:443")
    );
    assert!(Arc::ptr_eq(database.clients(), &clients));
}

/// The database URI reads back exactly as it was set, and only when it was set.
///
/// adbc.h requires a driver that supports getting options to support the standard ones,
/// including `uri`. A synthesized URI would drop settings the original configured; each of those
/// settings also reads back through its own option.
#[test]
fn the_database_uri_reads_back_exactly_as_it_was_set() {
    const URI: &str = "bigquery://uri.example/uri-project?DatasetId=uri_dataset&Location=EU";
    let mut database = new_database();
    let error = database.get_option_string(OptionDatabase::Uri).unwrap_err();
    assert_failure(&error, Status::NotFound, &["uri"]);

    set_option(&mut database, OptionDatabase::Uri, URI);
    assert_eq!(option_string(&database, OptionDatabase::Uri), URI);
    // Credential values are redacted (see
    // `the_uri_option_cannot_read_back_a_secret_its_own_option_refuses` in
    // `src/driver/uri/tests.rs`); this checks that the rest survives.
    let secret_uri = "bigquery://uri.example/uri-project?OAuthType=3&AuthClientId=client-id\
                      &AuthClientSecret=uri-secret&AuthRefreshToken=uri-token";
    set_option(&mut database, OptionDatabase::Uri, secret_uri);
    assert_eq!(
        option_string(&database, OptionDatabase::Uri),
        "bigquery://uri.example/uri-project?OAuthType=3&AuthClientId=client-id\
         &AuthClientSecret=<redacted>&AuthRefreshToken=<redacted>"
    );
    assert!(!format!("{database:?}").contains("uri-secret"));

    set_option(&mut database, OptionDatabase::Uri, URI);
    for (option, value) in [
        (BigQueryDatabaseOption::ProjectId, "uri-project"),
        (BigQueryDatabaseOption::DatasetId, "uri_dataset"),
        (BigQueryDatabaseOption::Location, "EU"),
        (BigQueryDatabaseOption::Endpoint, "https://uri.example:443"),
    ] {
        assert_eq!(
            option_string(&database, option),
            value,
            "{}",
            option.as_str()
        );
    }
}

/// The refusal a credential-bearing database with this configuration meets on connecting.
fn connection_error(database: &BigQueryDatabase) -> adbc_core::error::Error {
    database.new_connection().unwrap_err()
}

/// A database holding a real token, so that endpoint trust decides whether it can connect.
fn database_with_token(uri: &str) -> BigQueryDatabase {
    let mut database = new_database();
    set_option(&mut database, OptionDatabase::Uri, uri);
    set_real_token(&mut database);
    database
}

fn set_real_token(database: &mut BigQueryDatabase) {
    set_option(
        database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::TemporaryAccessToken,
    );
    set_option(
        database,
        BigQueryDatabaseOption::AuthenticationAccessToken,
        "synthetic-uri-token",
    );
}

/// Asserts the configuration refuses a URI host, as opposed to failing for another reason.
fn assert_uri_host_refused(database: &BigQueryDatabase, host: &str) {
    let error = connection_error(database);
    assert_failure(
        &error,
        Status::InvalidArguments,
        &[
            "BigQuery URI host must be a Google API HTTPS endpoint",
            &format!("host {host:?} over https"),
        ],
    );
}

/// A URI cannot send credentials somewhere new, whichever real credential it would send.
#[test]
fn a_uri_host_outside_the_google_api_domain_is_refused_with_real_credentials() {
    let database = database_with_token("bigquery://private.example/test-project");
    assert!(!database.config.allow_untrusted_endpoint);
    let error = connection_error(&database);
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery URI host must be a Google API HTTPS endpoint, but resolves to host \
         \"private.example\" over https; requests there would send the bigquery.auth_type \
         \"temporary_access_token\" access token; a URI cannot send credentials to a new host, \
         so set bigquery.endpoint as a database option to trust it, set \
         bigquery.allow_untrusted_endpoint=true to permit it, or use bigquery.auth_type \
         \"anonymous\" for an emulator"
    );

    for uri in [
        // The refusal comes before Application Default Credentials are looked for.
        "bigquery://private.example/test-project",
        "bigquery://private.example:8443/test-project?OAuthType=0",
        "bigquery://private.example/test-project?OAuthType=3&AuthClientId=id\
         &AuthClientSecret=uri-secret&AuthRefreshToken=uri-token",
        "bigquery://googleapis.com.private.example/test-project",
    ] {
        let mut database = new_database();
        set_option(&mut database, OptionDatabase::Uri, uri);
        let error = connection_error(&database);
        assert_failure(
            &error,
            Status::InvalidArguments,
            &["BigQuery URI host must be"],
        );
        assert_message_omits(&error, &["uri-secret", "uri-token"]);
    }
    assert_uri_host_refused(
        &database_with_token("bigquery://127.0.0.1:9050/test-project"),
        "127.0.0.1",
    );
    assert_uri_host_refused(
        &database_with_token("bigquery://[::1]:9050/test-project"),
        "::1",
    );
}

/// An anonymous credential mints no token, so a URI needs no trust to reach an emulator.
#[test]
fn a_uri_host_needs_no_trust_with_anonymous_authentication() {
    let uri = (
        OptionDatabase::Uri,
        OptionValue::from("bigquery://private.example/test-project"),
    );
    let anonymous = (
        OptionDatabase::from(BigQueryDatabaseOption::AuthenticationType),
        OptionValue::from(BigQueryAuthenticationType::Anonymous.as_str()),
    );
    for options in [vec![uri.clone(), anonymous.clone()], vec![anonymous, uri]] {
        let mut driver = BigQueryDriver::try_new().unwrap();
        let database = driver.new_database_with_opts(options).unwrap();
        database.new_connection().unwrap();
    }
}

/// The opt-in is a database option, which reaches a URI host however the two were ordered.
#[test]
fn a_uri_host_is_allowed_by_the_untrusted_endpoint_option() {
    let uri = (
        OptionDatabase::Uri,
        OptionValue::from("bigquery://private.example/test-project"),
    );
    let allow = (
        OptionDatabase::from(BigQueryDatabaseOption::AllowUntrustedEndpoint),
        OptionValue::from("true"),
    );
    let auth_type = (
        OptionDatabase::from(BigQueryDatabaseOption::AuthenticationType),
        OptionValue::from(BigQueryAuthenticationType::TemporaryAccessToken.as_str()),
    );
    let token = (
        OptionDatabase::from(BigQueryDatabaseOption::AuthenticationAccessToken),
        OptionValue::from("synthetic-uri-token"),
    );
    for options in [
        vec![uri.clone(), allow.clone(), auth_type.clone(), token.clone()],
        vec![allow, auth_type, token, uri],
    ] {
        let mut driver = BigQueryDriver::try_new().unwrap();
        let database = driver.new_database_with_opts(options).unwrap();
        database.new_connection().unwrap();
    }
}

/// Only a parameter the URI grammar names can reach an option, and the opt-in is not one. A URI
/// set later also resets an opt-in set earlier, so it cannot inherit one either.
#[test]
fn a_uri_cannot_relax_the_untrusted_endpoint_check() {
    let mut database = new_database();
    for parameter in [
        "AllowUntrustedEndpoint",
        "bigquery.allow_untrusted_endpoint",
        "allow_untrusted_endpoint",
    ] {
        let uri = format!("bigquery://private.example/test-project?{parameter}=true");
        let error = set_option_error(&mut database, OptionDatabase::Uri, uri.as_str());
        assert_failure(&error, Status::InvalidArguments, &["is not one of"]);
    }
    assert!(!database.config.allow_untrusted_endpoint);

    set_option(
        &mut database,
        BigQueryDatabaseOption::AllowUntrustedEndpoint,
        "true",
    );
    set_option(
        &mut database,
        OptionDatabase::Uri,
        "bigquery://private.example/test-project",
    );
    set_real_token(&mut database);
    assert!(!database.config.allow_untrusted_endpoint);
    assert_uri_host_refused(&database, "private.example");
}

/// A document that arrives through a URI meets the same rule as one set as an option, and the
/// URI's host being Google's does not vouch for what the document names.
#[test]
fn a_uri_cannot_smuggle_a_credential_document_past_the_destination_check() {
    let document = r#"{"type":"authorized_user","client_id":"id","client_secret":"uri-secret",
        "refresh_token":"uri-token","token_uri":"https://attacker.example/token?key=uri-key"}"#;
    let encoded =
        percent_encoding::utf8_percent_encode(document, percent_encoding::NON_ALPHANUMERIC);
    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Uri,
        format!("bigquery:///test-project?OAuthType=2&AuthCredentials={encoded}").as_str(),
    );
    let error = connection_error(&database);
    assert_failure(
        &error,
        Status::InvalidArguments,
        &["its token_uri field resolves to host \"attacker.example\""],
    );
    assert_message_omits(&error, &["uri-secret", "uri-token", "uri-key"]);
}

/// An executable credential source makes the auth library run its command once the machine sets
/// `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`. A URI is often assembled from input the
/// application does not control, so a document it supplies may not name one, at any depth, and
/// the host opt-in does not lift that. The same document set as an option builds, and an option
/// set after the URI takes the credential over.
#[test]
fn a_uri_cannot_supply_a_credential_document_that_runs_a_command() {
    const COMMAND: &str = "/usr/local/bin/uri-supplied-command";
    let external = serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {"executable": {"command": COMMAND, "timeout_millis": 5000}},
    });
    let impersonated = serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken",
        "source_credentials": external,
    });
    let uri_with = |document: &serde_json::Value| {
        let encoded = percent_encoding::utf8_percent_encode(
            &document.to_string(),
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        format!("bigquery:///test-project?OAuthType=2&AuthCredentials={encoded}")
    };

    for (document, field) in [
        (&external, "credential_source.executable"),
        (
            &impersonated,
            "source_credentials.credential_source.executable",
        ),
    ] {
        let mut database = new_database();
        set_option(
            &mut database,
            OptionDatabase::Uri,
            uri_with(document).as_str(),
        );
        set_option(
            &mut database,
            BigQueryDatabaseOption::AllowUntrustedEndpoint,
            "true",
        );
        let error = connection_error(&database);
        assert_failure(
            &error,
            Status::InvalidArguments,
            &[
                "the JSON credential document the connection URI supplied names a command to \
                 run",
                &format!("its {field} field"),
            ],
        );
        assert_message_omits(&error, &[COMMAND, "uri-supplied"]);

        // Set as an option, the same document is the application's own configuration.
        set_option(
            &mut database,
            BigQueryDatabaseOption::AuthenticationCredentials,
            document.to_string().as_str(),
        );
        database
            .new_connection()
            .unwrap_or_else(|error| panic!("{field}: {error}"));
    }

    // A URI document whose subject token comes from a file runs nothing.
    let mut file_sourced = impersonated;
    file_sourced["source_credentials"]["credential_source"] =
        serde_json::json!({"file": "/var/run/secrets/token"});
    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Uri,
        uri_with(&file_sourced).as_str(),
    );
    database.new_connection().unwrap();
}

/// A URI may still name Google's own hosts and ports with real credentials.
#[test]
fn a_google_api_host_in_a_uri_is_allowed_with_real_credentials() {
    for uri in [
        "bigquery:///test-project",
        "bigquery://bigquery.googleapis.com/test-project",
        "bigquery://BigQuery.GoogleAPIs.com:443/test-project",
        "bigquery://bigquery-private.p.googleapis.com/test-project",
        "bigquery://:8443/test-project",
    ] {
        database_with_token(uri)
            .new_connection()
            .unwrap_or_else(|error| panic!("{uri}: {error}"));
    }
}

/// The URI grammar has no scheme of its own, so a URI cannot ask for plaintext. Rule coverage of
/// an `http` endpoint attributed to a URI is in `config::tests::trust`.
#[test]
fn a_uri_cannot_spell_an_http_endpoint() {
    for uri in [
        "bigquery://http://bigquery.googleapis.com/test-project",
        "bigquery://http:bigquery.googleapis.com/test-project",
    ] {
        assert_eq!(
            set_option_error(&mut new_database(), OptionDatabase::Uri, uri).status,
            Status::InvalidArguments,
            "{uri}"
        );
    }
}

/// What an option names is the caller's own decision: each spelling of the REST endpoint, dbt's
/// included, stays trusted on any host and over HTTP, and takes precedence over a URI's host
/// whichever order the two arrive in.
#[test]
fn an_explicit_endpoint_overrides_a_refused_uri_host_in_either_order() {
    let uri = (
        OptionDatabase::Uri,
        OptionValue::from("bigquery://uri.example/test-project"),
    );
    let auth_type = (
        OptionDatabase::from(BigQueryDatabaseOption::AuthenticationType),
        OptionValue::from(BigQueryAuthenticationType::TemporaryAccessToken.as_str()),
    );
    let token = (
        OptionDatabase::from(BigQueryDatabaseOption::AuthenticationAccessToken),
        OptionValue::from("synthetic-uri-token"),
    );
    for key in [
        "bigquery.endpoint",
        "adbc.bigquery.sql.endpoint",
        "adbc.bigquery.sql.api_endpoint",
    ] {
        for endpoint in [
            "http://127.0.0.1:9050",
            "http://proxy.example",
            "https://proxy.example",
            "proxy.example:8443",
        ] {
            let explicit = (
                OptionDatabase::Other(key.to_owned()),
                OptionValue::from(endpoint),
            );
            for options in [
                vec![
                    uri.clone(),
                    explicit.clone(),
                    auth_type.clone(),
                    token.clone(),
                ],
                vec![
                    explicit.clone(),
                    auth_type.clone(),
                    token.clone(),
                    uri.clone(),
                ],
            ] {
                let mut driver = BigQueryDriver::try_new().unwrap();
                let database = driver.new_database_with_opts(options).unwrap();
                assert!(!database.config.endpoint_from_uri, "{key}: {endpoint}");
                database
                    .new_connection()
                    .unwrap_or_else(|error| panic!("{key}: {endpoint}: {error}"));
            }
        }
    }

    // After initialization a later `set_option` overrides the URI just the same.
    let mut database = database_with_token("bigquery://uri.example/test-project");
    assert_uri_host_refused(&database, "uri.example");
    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        "http://127.0.0.1:9050",
    );
    database.new_connection().unwrap();
}

/// Trust follows the effective endpoint: `uri` set directly resets the configuration, an explicit
/// endpoint included, so the host it leaves in place is the URI's own.
#[test]
fn a_uri_set_after_an_explicit_endpoint_replaces_it_and_its_trust() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        "http://127.0.0.1:9050",
    );
    set_option(
        &mut database,
        OptionDatabase::Uri,
        "bigquery://uri.example/test-project",
    );
    set_real_token(&mut database);
    assert_eq!(
        database.config.endpoints.get(ServiceEndpointKind::BigQuery),
        Some("https://uri.example:443")
    );
    assert_uri_host_refused(&database, "uri.example");
}

/// Clearing the option leaves the default endpoint, and no trace of the URI's host to refuse.
#[test]
fn clearing_the_endpoint_after_a_uri_falls_back_to_the_default_host() {
    let mut database = database_with_token("bigquery://uri.example/test-project");
    set_option(&mut database, BigQueryDatabaseOption::Endpoint, "");
    assert_eq!(
        database.config.endpoints.get(ServiceEndpointKind::BigQuery),
        None
    );
    database.new_connection().unwrap();
}

/// A rejected endpoint value changes nothing, so it cannot clear a URI's provenance either.
#[test]
fn a_rejected_endpoint_option_leaves_the_uri_host_refused() {
    let mut database = database_with_token("bigquery://uri.example/test-project");
    assert_eq!(
        set_option_error(
            &mut database,
            BigQueryDatabaseOption::Endpoint,
            "https://proxy.example/path"
        )
        .status,
        Status::InvalidArguments
    );
    assert_uri_host_refused(&database, "uri.example");
}

/// A rejected URI changes nothing, so an explicit endpoint keeps its trust.
#[test]
fn a_rejected_uri_leaves_an_explicit_endpoint_trusted() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        "http://127.0.0.1:9050",
    );
    set_real_token(&mut database);
    assert_eq!(
        set_option_error(
            &mut database,
            OptionDatabase::Uri,
            "bigquery://uri.example/test-project?DatasetId=not-valid"
        )
        .status,
        Status::InvalidArguments
    );
    database.new_connection().unwrap();
}

/// Reading options back does not touch provenance, and the `uri` read back is still a URI: setting
/// it again leaves the host refused. Writing the endpoint back is an explicit option like any
/// other, and is trusted as one.
#[test]
fn reading_options_back_does_not_change_a_uri_hosts_trust() {
    let mut database = database_with_token("bigquery://uri.example/test-project");
    let endpoint = option_string(&database, BigQueryDatabaseOption::Endpoint);
    assert_eq!(endpoint, "https://uri.example:443");
    let uri = option_string(&database, OptionDatabase::Uri);
    assert_uri_host_refused(&database, "uri.example");

    set_option(&mut database, OptionDatabase::Uri, uri.as_str());
    set_real_token(&mut database);
    assert_uri_host_refused(&database, "uri.example");

    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        endpoint.as_str(),
    );
    database.new_connection().unwrap();
}

/// A refusal is decided when clients are built, so a cached refusal must not outlive the option
/// that resolves it.
#[test]
fn an_option_set_after_a_refused_connection_takes_effect() {
    let mut database = database_with_token("bigquery://uri.example/test-project");
    assert_uri_host_refused(&database, "uri.example");
    set_option(
        &mut database,
        BigQueryDatabaseOption::AllowUntrustedEndpoint,
        "true",
    );
    database.new_connection().unwrap();
}

/// The option names itself in the rejection, and a value from a URI says so; otherwise a caller
/// who set only `uri` would read a complaint about `bigquery.dataset_id`.
#[test]
fn a_uri_value_rejected_by_its_option_is_reported_as_coming_from_the_uri() {
    let mut database = new_database();
    let error = set_option_error(
        &mut database,
        OptionDatabase::Uri,
        "bigquery:///my-project?DatasetId=not-valid",
    );
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.dataset_id may contain only ASCII letters, digits, and \
         underscores; the value came from the connection URI"
    );

    // The same value set as an option is not blamed on a URI.
    let error = set_option_error(
        &mut database,
        BigQueryDatabaseOption::DatasetId,
        "not-valid",
    );
    assert_message_omits(&error, &["connection URI"]);
}
