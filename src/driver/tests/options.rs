//! The database option surface: what each key round-trips, what it redacts, and what an error
//! is allowed to say about a rejected value.

use super::*;
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::http::ok;
use crate::test_support::options::{
    assert_string_option, option_string, set_option, set_option_error,
};

#[test]
fn project_options_accept_opaque_identifiers_and_reject_unsafe_resource_segments() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let mut database = driver.new_database().unwrap();
    for option in [
        BigQueryDatabaseOption::ProjectId,
        BigQueryDatabaseOption::AuthenticationQuotaProject,
    ] {
        // The identifier rules themselves are covered in `identifiers::tests`.
        for value in [
            "INVALID",
            "example.com:project",
            "123456789012",
            "project?#%",
        ] {
            set_option(&mut database, option, value);
            assert_eq!(option_string(&database, option), value);
        }
        for value in ["SECRET/path", "SECRET\nline", ".."] {
            let error = set_option_error(&mut database, option, value);
            assert_eq!(
                error.status,
                Status::InvalidArguments,
                "{option:?} accepted {value:?}",
            );
            assert_message_omits(&error, &["SECRET"]);
        }
        // The empty string clears it, as it clears every other database option.
        set_option(&mut database, option, "");
        assert_string_option(&database, option, None);
    }
}

/// An unset option is reported by its key, which is all a caller behind a driver manager has.
/// Write-only secrets follow the same rule, as do the two comma-separated lists, which are unset
/// when empty.
#[test]
fn unreadable_database_options_are_reported_by_their_own_key() {
    let database = new_database();
    for &option in BigQueryDatabaseOption::ALL {
        let Err(error) = database.get_option_string(option.into()) else {
            continue;
        };
        assert_failure(&error, Status::NotFound, &[option.as_str()]);
    }
}

#[test]
fn database_options_round_trip_and_redact_credentials() {
    let mut database = new_database();
    let original_clients = database.clients().clone();

    set_option(&mut database, BigQueryDatabaseOption::Location, "EU");
    set_option(
        &mut database,
        BigQueryDatabaseOption::ProjectId,
        "fixture-project",
    );
    assert!(Arc::ptr_eq(&original_clients, database.clients()));

    // One row per option: what is set, and what the getter must report after every set has
    // been applied. `None` marks a secret that must read back as NOT_FOUND.
    let rows = [
        (
            BigQueryDatabaseOption::AuthenticationType,
            BigQueryAuthenticationType::JsonCredentialString.as_str(),
            Some(BigQueryAuthenticationType::JsonCredentialString.as_str()),
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentials,
            AUTHORIZED_USER_JSON,
            None,
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentialsType,
            BigQueryCredentialsType::AuthorizedUser.as_str(),
            Some(BigQueryCredentialsType::AuthorizedUser.as_str()),
        ),
        (
            BigQueryDatabaseOption::AuthenticationQuotaProject,
            "fixture-quota-project",
            Some("fixture-quota-project"),
        ),
        (
            BigQueryDatabaseOption::Endpoint,
            "127.0.0.1:1",
            Some("https://127.0.0.1:1"),
        ),
        (
            BigQueryDatabaseOption::StorageEndpoint,
            "127.0.0.1:2",
            Some("https://127.0.0.1:2"),
        ),
        (
            BigQueryDatabaseOption::AllowUntrustedEndpoint,
            "true",
            Some("true"),
        ),
        (
            BigQueryDatabaseOption::ImpersonationTargetPrincipal,
            "target@fixture-project.iam.gserviceaccount.com",
            Some("target@fixture-project.iam.gserviceaccount.com"),
        ),
        (
            BigQueryDatabaseOption::ImpersonationDelegates,
            "first@fixture-project.iam.gserviceaccount.com, second@fixture-project.iam.gserviceaccount.com",
            Some(
                "first@fixture-project.iam.gserviceaccount.com,second@fixture-project.iam.gserviceaccount.com",
            ),
        ),
        (
            BigQueryDatabaseOption::ImpersonationScopes,
            "scope-one, scope-two",
            Some("scope-one,scope-two"),
        ),
        (
            BigQueryDatabaseOption::ImpersonationLifetime,
            "1h30m0.5s",
            Some("1h30m0.5s"),
        ),
    ];

    for (option, value, _) in rows {
        let previous_clients = database.clients().clone();
        set_option(&mut database, option, value);
        assert!(
            !Arc::ptr_eq(&previous_clients, database.clients()),
            "{}",
            option.as_str()
        );
    }

    for (option, _, expected) in rows {
        assert_string_option(&database, option, expected);
    }
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::Location),
        "EU"
    );
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::ProjectId),
        "fixture-project"
    );

    let debug = format!("{database:?}");
    assert!(debug.contains("<redacted>"));
    for secret in [
        AUTHORIZED_USER_JSON,
        "fixture-client-secret",
        "fixture-refresh-token",
    ] {
        assert!(!debug.contains(secret));
    }
    database.new_connection().unwrap();
}

#[test]
fn every_service_endpoint_option_round_trips_through_its_canonical_slot() {
    let mut database = new_database();
    let expected = ServiceEndpointKind::ALL.map(|kind| {
        format!(
            "https://{}.googleapis.com",
            kind.option().as_str().replace(['.', '_'], "-")
        )
    });

    for (kind, endpoint) in ServiceEndpointKind::ALL.into_iter().zip(&expected) {
        set_option(&mut database, kind.option(), endpoint.clone());
    }

    for (kind, endpoint) in ServiceEndpointKind::ALL.into_iter().zip(&expected) {
        assert_eq!(
            option_string(&database, kind.option()),
            *endpoint,
            "{}",
            kind.option().as_str()
        );
        assert_eq!(
            database.config.endpoints.get(kind),
            Some(endpoint.as_str()),
            "{kind:?} was not stored in its own endpoint slot"
        );
    }
}

#[test]
fn database_option_errors_do_not_disclose_payloads() {
    const SECRET: &str = "database-option-payload-secret-sentinel";

    for (key, value) in [
        (
            OptionDatabase::Uri,
            OptionValue::Bytes(SECRET.as_bytes().to_vec()),
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentials.into(),
            OptionValue::Bytes(SECRET.as_bytes().to_vec()),
        ),
        (
            BigQueryDatabaseOption::AuthenticationType.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BigQueryDatabaseOption::AuthenticationCredentialsType.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BigQueryDatabaseOption::AllowUntrustedEndpoint.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
    ] {
        let mut database = new_database();
        let rendered = format!("{value:?}");
        let error = set_option_error(&mut database, key, value);
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(&error, &[SECRET, &rendered]);
        assert!(!format!("{database:?}").contains(SECRET));
    }
}

#[test]
fn authentication_type_defaults_and_round_trips() {
    let mut database = new_database();
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::AuthenticationType),
        BigQueryAuthenticationType::Default.as_str()
    );

    // Every variant, spelled canonically and through its legacy alias, must round-trip to the
    // canonical spelling, including when set through the option key's own legacy alias.
    for &expected in BigQueryAuthenticationType::ALL {
        for value in
            std::iter::once(expected.as_str()).chain(expected.legacy_aliases().iter().copied())
        {
            set_option(
                &mut database,
                OptionDatabase::Other(
                    BigQueryDatabaseOption::AuthenticationType.legacy_aliases()[0].to_owned(),
                ),
                value.to_owned(),
            );
            assert_eq!(
                option_string(&database, BigQueryDatabaseOption::AuthenticationType),
                expected.as_str(),
                "{value}"
            );
        }
    }

    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::Anonymous,
    );
    assert_eq!(
        database.config.authentication_type,
        BigQueryAuthenticationType::Anonymous
    );
}

#[test]
fn authentication_type_rejects_invalid_values_and_types() {
    let mut database = new_database();
    let key: OptionDatabase = BigQueryDatabaseOption::AuthenticationType.into();
    assert_eq!(
        set_option_error(&mut database, key.clone(), "oauth-ish").status,
        Status::InvalidArguments
    );
    assert_eq!(
        set_option_error(&mut database, key.clone(), OptionValue::Int(1)).status,
        Status::InvalidArguments
    );
    assert_eq!(
        database.get_option_int(key).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(
        database.config.authentication_type,
        BigQueryAuthenticationType::Default
    );
}

#[test]
fn allow_untrusted_endpoint_round_trips_and_rejects_other_values() {
    let mut database = new_database();
    let key: OptionDatabase = BigQueryDatabaseOption::AllowUntrustedEndpoint.into();
    assert_eq!(
        option_string(&database, key.clone()),
        "false",
        "the opt-in defaults to off"
    );

    let previous_clients = database.clients().clone();
    set_option(&mut database, key.clone(), "true");
    assert!(database.config.allow_untrusted_endpoint);
    assert_eq!(option_string(&database, key.clone()), "true");
    assert!(!Arc::ptr_eq(&previous_clients, database.clients()));

    set_option(&mut database, key.clone(), "false");
    assert!(!database.config.allow_untrusted_endpoint);

    for value in ["1", "yes", "on", ""] {
        let error = set_option_error(&mut database, key.clone(), value);
        assert_failure(
            &error,
            Status::InvalidArguments,
            &["bigquery.allow_untrusted_endpoint must be"],
        );
    }
    assert_eq!(
        set_option_error(&mut database, key, OptionValue::Int(1)).status,
        Status::InvalidArguments
    );
    assert!(!database.config.allow_untrusted_endpoint);
}

#[test]
fn project_and_dataset_options_round_trip_under_canonical_and_legacy_names() {
    for (option, value) in [
        (BigQueryDatabaseOption::ProjectId, "test-project"),
        (BigQueryDatabaseOption::DatasetId, "Sample_Dataset1"),
    ] {
        for name in [option.as_str(), option.legacy_aliases()[0]] {
            let mut database = new_database();
            set_option(
                &mut database,
                OptionDatabase::Other(name.to_owned()),
                value.to_owned(),
            );
            assert_eq!(
                option_string(&database, OptionDatabase::Other(name.to_owned())),
                value,
                "{name}"
            );
        }
    }
}

/// A rejected option value names the key it arrived under and the form that would have been
/// accepted, and never echoes the value.
#[test]
fn rejected_option_values_name_their_key_and_the_accepted_form() {
    const SECRET: &str = "option-payload-secret-sentinel";
    let mut database = new_database();
    // The third element is the accepted form the message has to describe, not the whole
    // sentence: pinning every word would make this a change-detector for prose.
    for (option, value, accepted_form) in [
        (
            BigQueryDatabaseOption::Location,
            format!("{SECRET} US"),
            "a location name such as US, EU, or us-central1",
        ),
        (
            BigQueryDatabaseOption::ImpersonationTargetPrincipal,
            format!("{SECRET} @example.com"),
            "a service account email address",
        ),
        (
            BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
            format!("https://example.com/ {SECRET}"),
            "a token endpoint URL",
        ),
        (
            BigQueryDatabaseOption::DatasetId,
            format!("{SECRET}-dataset"),
            "ASCII letters, digits, and underscores",
        ),
        (
            BigQueryDatabaseOption::ImpersonationLifetime,
            SECRET.to_owned(),
            "a Go duration such as 30m or 1h30m",
        ),
        (
            BigQueryDatabaseOption::ImpersonationLifetime,
            "13h".to_owned(),
            "at most 12 hours",
        ),
    ] {
        let error = set_option_error(&mut database, option, value);
        assert_failure(&error, Status::InvalidArguments, &[option.as_str()]);
        assert_message(&error, &[accepted_form]);
        assert_message_omits(&error, &[SECRET]);
    }
}

#[test]
fn option_statuses_are_specific() {
    let mut database = new_database();
    let wrong_type = set_option_error(&mut database, OptionDatabase::Uri, OptionValue::Int(1));
    assert_eq!(wrong_type.status, Status::InvalidArguments);

    let unset = database
        .get_option_string(BigQueryDatabaseOption::ProjectId.into())
        .unwrap_err();
    assert_eq!(unset.status, Status::NotFound);

    let unsupported = set_option_error(&mut database, OptionDatabase::Username, "user");
    assert_eq!(unsupported.status, Status::NotImplemented);

    let unknown = OptionDatabase::Other("this_option_does_not_exist".to_owned());
    assert_eq!(
        database
            .get_option_string(unknown.clone())
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(
        database.get_option_int(unknown.clone()).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(
        database
            .get_option_double(unknown.clone())
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(
        database.get_option_bytes(unknown).unwrap_err().status,
        Status::NotFound
    );
}

/// `bigquery.user_agent` follows the driver's own user agent on every REST request of the
/// database's clients, and text a header cannot carry is refused without being echoed.
#[test]
fn the_user_agent_option_extends_the_drivers_own() {
    use crate::rest::request::{Request, send};

    const OPTION: BigQueryDatabaseOption = BigQueryDatabaseOption::UserAgent;

    let mut database = new_database();
    assert_string_option(&database, OPTION, None);
    for value in [
        "my-app/1.0\nsecret",
        "my-app/1.0\tsecret",
        "my-\u{e4}pp/secret",
    ] {
        let error = set_option_error(&mut database, OPTION, value);
        assert_eq!(error.status, Status::InvalidArguments, "{value:?}");
        assert_message_omits(&error, &["secret"]);
    }
    set_option(&mut database, OPTION, "my-app/1.0 (team data)");
    assert_string_option(&database, OPTION, Some("my-app/1.0 (team data)"));

    let (endpoint, captured) =
        crate::test_support::http::capture_requests(vec![ok(serde_json::json!({}))]);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let client = database.clients().rest_client().unwrap();
    database
        .runtime
        .block_on(send(
            None,
            &credentials,
            &client,
            Request::get(&format!("{endpoint}/agent"), "describe it"),
        ))
        .unwrap();
    assert_eq!(
        captured.join().unwrap()[0].header("user-agent"),
        Some(concat!(
            "bigquery-adbc/",
            env!("CARGO_PKG_VERSION"),
            " my-app/1.0 (team data)"
        ))
    );

    // The empty string clears it, as it clears every other database option.
    set_option(&mut database, OPTION, "");
    assert_string_option(&database, OPTION, None);
}
