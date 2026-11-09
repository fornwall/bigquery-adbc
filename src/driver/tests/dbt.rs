//! The dbt profile keys and values, and the driver options they resolve to.

use super::*;
use crate::test_support::options::{option_string, set_option};

/// Every database key dbt 2.0 sets (crates/dbt-xdbc/src/bigquery.rs plus
/// crates/dbt-auth/src/bigquery) must resolve to a driver option.
#[test]
fn dbt_database_keys_resolve_to_driver_options() {
    for (alias, option) in [
        (
            "adbc.bigquery.sql.project_id",
            BigQueryDatabaseOption::ProjectId,
        ),
        (
            "adbc.bigquery.sql.dataset_id",
            BigQueryDatabaseOption::DatasetId,
        ),
        (
            "adbc.bigquery.sql.location",
            BigQueryDatabaseOption::Location,
        ),
        (
            "adbc.bigquery.sql.api_endpoint",
            BigQueryDatabaseOption::Endpoint,
        ),
        (
            "adbc.bigquery.sql.auth.quota_project",
            BigQueryDatabaseOption::AuthenticationQuotaProject,
        ),
        (
            "adbc.bigquery.sql.auth_type",
            BigQueryDatabaseOption::AuthenticationType,
        ),
        (
            "adbc.bigquery.sql.auth_credentials",
            BigQueryDatabaseOption::AuthenticationCredentials,
        ),
        (
            "adbc.bigquery.sql.auth.client_id",
            BigQueryDatabaseOption::AuthenticationClientId,
        ),
        (
            "adbc.bigquery.sql.auth.client_secret",
            BigQueryDatabaseOption::AuthenticationClientSecret,
        ),
        (
            "adbc.bigquery.sql.auth.refresh_token",
            BigQueryDatabaseOption::AuthenticationRefreshToken,
        ),
        (
            "adbc.bigquery.sql.auth.access_token",
            BigQueryDatabaseOption::AuthenticationAccessToken,
        ),
        (
            "adbc.bigquery.sql.auth.access_token_endpoint",
            BigQueryDatabaseOption::AuthenticationAccessTokenEndpoint,
        ),
        (
            "adbc.bigquery.sql.impersonate.target_principal",
            BigQueryDatabaseOption::ImpersonationTargetPrincipal,
        ),
        (
            "adbc.bigquery.sql.impersonate.scopes",
            BigQueryDatabaseOption::ImpersonationScopes,
        ),
    ] {
        assert_eq!(
            BigQueryDatabaseOption::from_name(alias),
            Some(option),
            "{alias}"
        );
    }
}

/// The `adbc.bigquery.sql.auth_type.*` value spellings dbt 2.0 sends must map onto the
/// driver's canonical authentication-type vocabulary.
#[test]
fn dbt_auth_type_values_map_to_driver_vocabulary() {
    let mut database = new_database();
    for (value, expected) in [
        (
            "adbc.bigquery.sql.auth_type.auth_bigquery",
            BigQueryAuthenticationType::Default,
        ),
        (
            "adbc.bigquery.sql.auth_type.json_credential_file",
            BigQueryAuthenticationType::JsonCredentialFile,
        ),
        (
            "adbc.bigquery.sql.auth_type.json_credential_string",
            BigQueryAuthenticationType::JsonCredentialString,
        ),
        (
            "adbc.bigquery.sql.auth_type.user_authentication",
            BigQueryAuthenticationType::UserAuthentication,
        ),
        (
            "adbc.bigquery.sql.auth_type.temporary_access_token",
            BigQueryAuthenticationType::TemporaryAccessToken,
        ),
    ] {
        set_option(
            &mut database,
            OptionDatabase::Other("adbc.bigquery.sql.auth_type".to_owned()),
            value.to_owned(),
        );
        assert_eq!(database.config.authentication_type, expected, "{value}");
        assert_eq!(
            option_string(&database, BigQueryDatabaseOption::AuthenticationType),
            expected.as_str(),
            "get_option reports the canonical value for {value}"
        );
    }
}

#[test]
fn api_endpoint_alias_sets_the_endpoint_option() {
    let mut database = new_database();
    set_option(
        &mut database,
        OptionDatabase::Other("adbc.bigquery.sql.api_endpoint".to_owned()),
        "127.0.0.1:9050",
    );
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::Endpoint),
        "https://127.0.0.1:9050"
    );
}
