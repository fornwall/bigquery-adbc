//! The credential-carrying options (JSON, user authentication, impersonation, access tokens and
//! external accounts) and what they keep out of readable output.

use super::*;
use crate::test_support::UNREACHABLE_ENDPOINT;
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::options::{option_string, set_option, set_option_error};

/// A connection's derived `Debug` reaches the shared `ClientState` through `ExecutionContext` and
/// its `Cancellation`. The credential stays out of the output only because `DatabaseConfig` prints
/// `<redacted>` and `ClientState` prints which clients exist, not the clients; a field added to
/// either (a cached `ClientConfiguration`, a resolved token) would leak it from every
/// connection.
#[test]
fn a_connection_keeps_its_credential_out_of_debug_output() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (
                BigQueryDatabaseOption::ProjectId.into(),
                "fixture-project".into(),
            ),
            (
                BigQueryDatabaseOption::AuthenticationType.into(),
                BigQueryAuthenticationType::JsonCredentialString.into(),
            ),
            (
                BigQueryDatabaseOption::AuthenticationCredentials.into(),
                AUTHORIZED_USER_JSON.into(),
            ),
            (
                BigQueryDatabaseOption::AllowUntrustedEndpoint.into(),
                "true".into(),
            ),
        ])
        .unwrap();
    // Build every client the connection can reach first: a credential that only materializes
    // once a client has been constructed would otherwise never be in the state under test.
    assert_all_clients_build(&database);
    let connection = database.new_connection().unwrap();

    let debug = format!("{connection:?}");
    assert!(debug.contains("<redacted>"));
    for secret in [
        AUTHORIZED_USER_JSON,
        "fixture-client-id",
        "fixture-client-secret",
        "fixture-refresh-token",
    ] {
        assert!(
            !debug.contains(secret),
            "a connection's Debug output disclosed a credential"
        );
    }
}

/// An empty credential option clears it, so it must not trip the unused-credential rejection or
/// authenticate as anybody.
#[test]
fn empty_credential_options_are_not_treated_as_configured() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::Anonymous,
    );
    for option in [
        BigQueryDatabaseOption::AuthenticationCredentials,
        BigQueryDatabaseOption::AuthenticationClientId,
        BigQueryDatabaseOption::AuthenticationAccessToken,
        BigQueryDatabaseOption::AuthenticationExternalAccountAudience,
        BigQueryDatabaseOption::ImpersonationTargetPrincipal,
    ] {
        set_option(&mut database, option, "");
    }
    let configuration = database.clients().configuration(&database.runtime).unwrap();
    let headers = database
        .runtime
        .block_on(configuration.credentials.headers(http::Extensions::new()))
        .unwrap();
    let google_cloud_auth::credentials::CacheableResource::New { data, .. } = headers else {
        panic!("expected new headers from a fresh request");
    };
    assert_eq!(data.get(http::header::AUTHORIZATION), None, "{data:?}");
    assert_eq!(configuration.execution_principal, None);
}

#[test]
fn json_credentials_validate_type_without_disclosing_the_document() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::JsonCredentialString,
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationCredentials,
        AUTHORIZED_USER_JSON,
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationCredentialsType,
        BigQueryCredentialsType::ServiceAccount,
    );

    let error = database.new_connection().unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message_omits(&error, &["fixture-client-secret", "fixture-refresh-token"]);
}

#[test]
fn user_authentication_builds_shared_clients_and_redacts_secrets() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::ProjectId,
        "oauth-project",
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::UserAuthentication,
    );

    for (option, value) in [
        (
            BigQueryDatabaseOption::AuthenticationClientId,
            "oauth-client-id",
        ),
        (
            BigQueryDatabaseOption::AuthenticationClientSecret,
            "oauth-client-secret",
        ),
        (
            BigQueryDatabaseOption::AuthenticationRefreshToken,
            "oauth-refresh-token",
        ),
        (
            BigQueryDatabaseOption::AuthenticationQuotaProject,
            "oauth-quota-project",
        ),
        (BigQueryDatabaseOption::Endpoint, "http://127.0.0.1:1"),
        (
            BigQueryDatabaseOption::StorageEndpoint,
            "http://127.0.0.1:2",
        ),
        (BigQueryDatabaseOption::AllowUntrustedEndpoint, "true"),
        // A built client may mint a token in the background, which must not reach Google.
        (
            BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
            &format!("{UNREACHABLE_ENDPOINT}/token"),
        ),
    ] {
        let previous_clients = database.clients().clone();
        set_option(
            &mut database,
            OptionDatabase::Other(
                option
                    .legacy_aliases()
                    .first()
                    .copied()
                    .unwrap_or(option.as_str())
                    .to_owned(),
            ),
            value,
        );
        assert!(!Arc::ptr_eq(&previous_clients, database.clients()));
    }

    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::AuthenticationClientId),
        "oauth-client-id"
    );
    for option in [
        BigQueryDatabaseOption::AuthenticationClientSecret,
        BigQueryDatabaseOption::AuthenticationRefreshToken,
    ] {
        let error = database.get_option_string(option.into()).unwrap_err();
        assert_eq!(error.status, Status::NotFound);
        assert_message_omits(&error, &["oauth-client-secret", "oauth-refresh-token"]);
    }

    let debug = format!("{database:?}");
    assert!(debug.contains("oauth-client-id"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("oauth-client-secret"));
    assert!(!debug.contains("oauth-refresh-token"));

    assert_all_clients_build(&database);
}

#[test]
fn user_authentication_requires_all_discrete_inputs() {
    let inputs = [
        (
            BigQueryDatabaseOption::AuthenticationClientId,
            "oauth-client-id",
        ),
        (
            BigQueryDatabaseOption::AuthenticationClientSecret,
            "oauth-client-secret",
        ),
        (
            BigQueryDatabaseOption::AuthenticationRefreshToken,
            "oauth-refresh-token",
        ),
    ];

    for (missing_index, (missing_option, _)) in inputs.iter().enumerate() {
        let mut database = new_database();
        set_option(
            &mut database,
            BigQueryDatabaseOption::AuthenticationType,
            BigQueryAuthenticationType::UserAuthentication,
        );
        for (index, (option, value)) in inputs.iter().enumerate() {
            if index != missing_index {
                set_option(&mut database, *option, *value);
            }
        }

        let error = database.new_connection().unwrap_err();
        assert_failure(&error, Status::InvalidArguments, &[missing_option.as_str()]);
        assert_message_omits(&error, &["oauth-client-secret", "oauth-refresh-token"]);
    }
}

#[test]
fn impersonation_requires_a_target_and_valid_duration() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::JsonCredentialString,
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationCredentials,
        AUTHORIZED_USER_JSON,
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AllowUntrustedEndpoint,
        "true",
    );
    // Scopes alone are a modifier, not a request to impersonate: dbt-core 2.0 sets them on
    // every connection, so they must not require a target.
    set_option(
        &mut database,
        BigQueryDatabaseOption::ImpersonationScopes,
        "scope-one",
    );
    database.new_connection().unwrap();

    set_option(
        &mut database,
        BigQueryDatabaseOption::ImpersonationDelegates,
        "delegate@example.iam.gserviceaccount.com",
    );
    assert_eq!(
        database.new_connection().unwrap_err().status,
        Status::InvalidArguments
    );

    for invalid in ["", "0s", "-1s", "12h0m0.000000001s", "one hour"] {
        let result = database.set_option(
            BigQueryDatabaseOption::ImpersonationLifetime.into(),
            invalid.into(),
        );
        if invalid.is_empty() {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
        }
    }
    for (value, expected) in [
        ("500ms", "500ms"),
        ("1.25s", "1.25s"),
        ("2m3s", "2m3s"),
        ("12h", "12h0m0s"),
    ] {
        set_option(
            &mut database,
            BigQueryDatabaseOption::ImpersonationLifetime,
            value,
        );
        assert_eq!(
            option_string(&database, BigQueryDatabaseOption::ImpersonationLifetime),
            expected
        );
    }
}

/// A static access token is a secret: settable through the dbt-core 2.0 alias, never readable, and
/// used exactly as configured, never replaced by the ambient identity.
#[test]
fn temporary_access_token_is_write_only_and_opens_connections() {
    const SECRET: &str = "temporary-access-token-secret-sentinel";

    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Other("adbc.bigquery.sql.auth_type".to_owned()),
        "adbc.bigquery.sql.auth_type.temporary_access_token",
    );
    set_option(
        &mut database,
        OptionDatabase::Other("adbc.bigquery.sql.auth.access_token".to_owned()),
        SECRET.to_owned(),
    );

    let unreadable = database
        .get_option_string(BigQueryDatabaseOption::AuthenticationAccessToken.into())
        .unwrap_err();
    assert_eq!(unreadable.status, Status::NotFound);
    assert_message_omits(&unreadable, &[SECRET]);
    assert!(!format!("{database:?}").contains(SECRET));

    database.new_connection().unwrap();

    // Without the token the same type reports the missing option instead.
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationAccessToken,
        "",
    );
    let missing = database.new_connection().unwrap_err();
    assert_failure(
        &missing,
        Status::InvalidArguments,
        &["bigquery.auth.access_token"],
    );
}

/// The access token and its endpoint are refused when the authentication type would silently
/// ignore them, mirroring the treatment of every other credential option.
#[test]
fn access_token_options_reject_mismatched_authentication_types() {
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationAccessToken,
        "unused-token",
    );
    let error = database.new_connection().unwrap_err();
    assert_failure(
        &error,
        Status::InvalidArguments,
        &["bigquery.auth.access_token", "temporary_access_token"],
    );

    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
        "https://oauth2.example/token",
    );
    let error = database.new_connection().unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error
            .message
            .contains("bigquery.auth.access_token_endpoint")
            && error.message.contains("user_authentication"),
        "{}",
        error.message
    );
}

#[test]
fn external_account_accepts_dbt_profile_options_and_preserves_execution_principal() {
    let mut database = new_database();
    let runtime = database.runtime.clone();
    let _guard = runtime.enter();
    for (option, value) in [
        ("adbc.bigquery.sql.project_id", "wif-project"),
        (
            "adbc.bigquery.sql.auth_type",
            "adbc.bigquery.sql.auth_type.external_account",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.audience",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.request_url",
            "https://127.0.0.1:1/oauth?api_key=url-secret",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.request_data",
            "client_secret=form-secret&grant_type=client_credentials",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.impersonation_url",
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test%40project.iam.gserviceaccount.com:generateAccessToken",
        ),
        (
            "adbc.bigquery.sql.impersonate.scopes",
            "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive",
        ),
    ] {
        let previous = database.clients().clone();
        set_option(
            &mut database,
            OptionDatabase::Other(option.to_owned()),
            value,
        );
        assert!(!Arc::ptr_eq(&previous, database.clients()));
    }
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::AuthenticationType),
        "external_account"
    );
    assert_eq!(
        option_string(
            &database,
            BigQueryDatabaseOption::AuthenticationExternalAccountAudience
        ),
        database
            .config
            .external_account
            .audience
            .as_deref()
            .unwrap()
    );
    for option in [
        BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl,
        BigQueryDatabaseOption::AuthenticationExternalAccountRequestData,
    ] {
        let error = database.get_option_string(option.into()).unwrap_err();
        assert_eq!(error.status, Status::NotFound);
    }
    for secret in ["url-secret", "form-secret"] {
        assert!(!format!("{database:?}").contains(secret));
    }
    assert_all_clients_build(&database);
    let configuration = crate::config::ClientConfiguration::build(&database.config).unwrap();
    assert_eq!(
        configuration.execution_principal.as_deref(),
        Some("test@project.iam.gserviceaccount.com")
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationExternalAccountImpersonationUrl,
        "",
    );
    let configuration = crate::config::ClientConfiguration::build(&database.config).unwrap();
    assert_eq!(configuration.execution_principal, None);
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationExternalAccountRequestData,
        "",
    );
    let error = crate::config::ClientConfiguration::build(&database.config)
        .err()
        .unwrap();
    assert_failure(&error, Status::InvalidArguments, &["request_data"]);
}

/// External-account option values are rejected by the call that sets them, not later by
/// `new_connection`. No rejection echoes the value: `request_url` carries identity-provider
/// credentials in its query, and the others follow the same rule.
#[test]
fn external_account_option_values_are_rejected_where_they_are_set() {
    let mut database = new_database();
    for (option, value, expected) in [
        (
            BigQueryDatabaseOption::AuthenticationExternalAccountAudience,
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/my pool",
            "BigQuery option bigquery.auth.external_account.audience must be a workload identity \
             pool provider resource, without whitespace",
        ),
        (
            BigQueryDatabaseOption::AuthenticationExternalAccountRequestUrl,
            "idp.example/token?api_key=url-secret",
            "BigQuery option bigquery.auth.external_account.request_url must be an absolute HTTP \
             or HTTPS URL without user information or a fragment",
        ),
        (
            BigQueryDatabaseOption::AuthenticationExternalAccountImpersonationUrl,
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@example.com\
             :generateIdToken",
            "BigQuery option bigquery.auth.external_account.impersonation_url must be the \
             standard HTTPS IAM Credentials generateAccessToken URL for a service account",
        ),
    ] {
        let error = set_option_error(&mut database, option, value);
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "{}",
            option.as_str()
        );
        assert_eq!(error.message, expected);
        assert_message_omits(&error, &["url-secret"]);
    }
}

/// Google requires the workforce pool user project in the token exchange, and no
/// `bigquery.auth.external_account` option carries it (upstream sends it only from
/// `with_workforce_pool_user_project`). STS would refuse the exchange naming a field the operator
/// cannot set here, so the driver refuses the audience itself and names what does work.
#[test]
fn a_workforce_pool_audience_is_refused_with_the_configuration_that_supports_it() {
    let mut database = new_database();
    let error = set_option_error(
        &mut database,
        BigQueryDatabaseOption::AuthenticationExternalAccountAudience,
        "//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
    );
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.auth.external_account.audience names a workforce pool, whose \
         token exchange needs a workforce_pool_user_project that no \
         bigquery.auth.external_account option sets; supply the complete external-account \
         credential document as bigquery.auth.credentials with bigquery.auth_type \
         \"json_credential_string\""
    );

    // A workload identity pool audience, which this option family is for, still passes.
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationExternalAccountAudience,
        "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider"
            .to_owned(),
    );
}

/// The driver exchanges two subject-token types; Google's STS also documents `id_token`, `saml2`,
/// `aws4_request` and `mtls`. The refusal names the two and where the rest are reachable.
#[test]
fn the_subject_token_type_refusal_does_not_claim_to_be_googles_whole_set() {
    let mut database = new_database();
    let error = set_option_error(
        &mut database,
        BigQueryDatabaseOption::AuthenticationExternalAccountSubjectTokenType,
        "urn:ietf:params:oauth:token-type:id_token",
    );
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.auth.external_account.subject_token_type must be \
         \"urn:ietf:params:oauth:token-type:jwt\" or \
         \"urn:ietf:params:oauth:token-type:access_token\"; for another subject-token type, set \
         a complete external-account credential document as bigquery.auth.credentials with \
         bigquery.auth_type \"json_credential_string\""
    );
}

/// Values no shape check can tell from a credential file path, each one line of ordinary
/// characters under the path length limit: a `base64 -w0` service-account key, its `/`-free
/// variant that overflows a file name rather than naming a missing directory, and a bare access
/// token.
fn misplaced_credentials() -> [String; 3] {
    let base64 = format!(
        "{}==",
        "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50Iiwi+/ByaXZhdGVfa2V5Ijoi".repeat(56)
    );
    assert!((3_000..4_096).contains(&base64.len()));
    let unslashed = base64.replace('/', "_");
    [
        base64,
        unslashed,
        "ya29.a0AfH6SMfixture-access-token_0123456789".to_owned(),
    ]
}

/// A connection attempt's failure, whose message reaches logs such as `dbt.log`. The value must
/// not appear even in part, and the error must still give the cause and the way out.
fn assert_not_echoed(options: Vec<(OptionDatabase, OptionValue)>, credential: &str) {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let failure = driver
        .new_database_with_opts(options)
        .unwrap()
        .new_connection()
        .expect_err("a value that names no file cannot connect");
    assert_message_omits(&failure, &[credential, &credential[..16]]);
    assert!(!format!("{failure:?}").contains(&credential[..16]));
    assert_message(
        &failure,
        &["value not echoed", "os error", "could not be read"],
    );
}

#[test]
fn a_misplaced_credential_in_the_file_path_option_is_not_echoed() {
    for credential in misplaced_credentials() {
        assert_not_echoed(
            vec![
                (
                    BigQueryDatabaseOption::ProjectId.into(),
                    "fixture-project".into(),
                ),
                (
                    BigQueryDatabaseOption::AuthenticationType.into(),
                    BigQueryAuthenticationType::JsonCredentialFile.into(),
                ),
                (
                    BigQueryDatabaseOption::AuthenticationCredentials.into(),
                    credential.as_str().into(),
                ),
            ],
            &credential,
        );
    }
}

/// `OAuthType=1` takes a path where `OAuthType=2` takes the document, so one digit sends a
/// credential to the same file open.
#[test]
fn a_misplaced_credential_in_a_uri_is_not_echoed() {
    for credential in misplaced_credentials() {
        let encoded = credential
            .replace('+', "%2B")
            .replace('/', "%2F")
            .replace('=', "%3D");
        assert_not_echoed(
            vec![(
                OptionDatabase::Uri,
                OptionValue::String(format!(
                    "bigquery:///fixture-project?OAuthType=1&AuthCredentials={encoded}"
                )),
            )],
            &credential,
        );
    }
}

/// ADC discovery reads the process environment, so a child process holds the value and fails
/// through the real builder rather than a parallel test's view of the variable.
#[test]
fn a_misplaced_credential_in_the_adc_environment_variable_is_not_echoed() {
    const CHILD: &str = "BIGQUERY_ADBC_MISPLACED_ADC_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        assert_not_echoed(
            vec![(
                BigQueryDatabaseOption::ProjectId.into(),
                "fixture-project".into(),
            )],
            &std::env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap(),
        );
        return;
    }
    for credential in misplaced_credentials() {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "driver::tests::credentials::\
                 a_misplaced_credential_in_the_adc_environment_variable_is_not_echoed",
            ])
            .env(CHILD, "1")
            .env("GOOGLE_APPLICATION_CREDENTIALS", &credential)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&child.stdout);
        // A filter that matches nothing also exits successfully.
        assert!(
            child.status.success() && stdout.contains("1 passed"),
            "{stdout}"
        );
    }
}
