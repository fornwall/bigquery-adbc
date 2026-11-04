//! Explicit test-only endpoint selection. An unset pair keeps the real BigQuery defaults.

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_driver_bigquery::{BigQueryAuthenticationType, BigQueryDatabaseOption};

const REST_ENV: &str = "ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT";
const STORAGE_ENV: &str = "ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT";

pub(crate) fn database_options() -> Result<Vec<(OptionDatabase, OptionValue)>, Error> {
    let read = |name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(invalid(format!("{name} must be valid UTF-8"))),
    };
    emulator_options(read(REST_ENV)?.as_deref(), read(STORAGE_ENV)?.as_deref())
}

#[track_caller]
pub(crate) fn require_real_bigquery() {
    crate::project_config::project();
    assert!(
        database_options().unwrap().is_empty(),
        "this test requires real Google Cloud services; unset {REST_ENV} and {STORAGE_ENV}"
    );
}

fn invalid(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

fn emulator_options(
    rest: Option<&str>,
    storage: Option<&str>,
) -> Result<Vec<(OptionDatabase, OptionValue)>, Error> {
    let (rest, storage) = match (rest, storage) {
        (None, None) => return Ok(Vec::new()),
        (Some(rest), Some(storage)) => (rest, storage),
        _ => return Err(invalid(format!("set both {REST_ENV} and {STORAGE_ENV}"))),
    };
    for (name, endpoint) in [(REST_ENV, rest), (STORAGE_ENV, storage)] {
        let url = reqwest::Url::parse(endpoint).map_err(|_| {
            invalid(format!(
                "{name} must be an absolute http:// or https:// URL"
            ))
        })?;
        if !matches!(url.scheme(), "http" | "https")
            || endpoint.chars().any(char::is_whitespace)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !matches!(url.path(), "" | "/")
        {
            return Err(invalid(format!(
                "{name} must be an http:// or https:// origin without credentials, path, query or fragment"
            )));
        }
    }
    Ok(vec![
        (
            BigQueryDatabaseOption::AuthenticationType.into(),
            BigQueryAuthenticationType::Anonymous.into(),
        ),
        (BigQueryDatabaseOption::Endpoint.into(), rest.into()),
        (
            BigQueryDatabaseOption::StorageEndpoint.into(),
            storage.into(),
        ),
        // Unsupported auxiliary services must also stay on the emulator endpoint.
        (BigQueryDatabaseOption::GcsEndpoint.into(), rest.into()),
        (BigQueryDatabaseOption::DataprocEndpoint.into(), rest.into()),
        (BigQueryDatabaseOption::VertexEndpoint.into(), rest.into()),
    ])
}

#[test]
fn real_bigquery_is_the_default() {
    assert!(emulator_options(None, None).unwrap().is_empty());
}

#[test]
fn emulator_requires_both_valid_endpoints() {
    let rest = Some("http://127.0.0.1:9050");
    let storage = Some("http://127.0.0.1:9060");
    assert!(emulator_options(rest, None).is_err());
    assert!(emulator_options(None, storage).is_err());
    for invalid in [
        "",
        "localhost:9050",
        "ftp://localhost:9050",
        "http://user:password@localhost:9050",
        "http://localhost:9050/bigquery/v2",
        "http://localhost:9050?token=secret",
        "http://localhost:9050#fragment",
        " http://localhost:9050",
        "http://localhost:9050\n",
        "http://localhost:9050//",
    ] {
        assert!(emulator_options(Some(invalid), storage).is_err());
        assert!(emulator_options(rest, Some(invalid)).is_err());
    }
}

#[test]
fn emulator_overrides_uri_authentication_and_routes_every_service() {
    use adbc_core::{Driver, Optionable};
    use adbc_driver_bigquery::BigQueryDriver;

    let rest = "http://127.0.0.1:9050";
    let storage = "http://127.0.0.1:9060";
    let database = BigQueryDriver::try_new()
        .unwrap()
        .new_database_with_opts(
            [(
                OptionDatabase::Uri,
                "bigquery://bigquery.googleapis.com/test-project?OAuthType=0".into(),
            )]
            .into_iter()
            .chain(emulator_options(Some(rest), Some(storage)).unwrap()),
        )
        .unwrap();
    for (key, expected) in [
        (BigQueryDatabaseOption::AuthenticationType, "anonymous"),
        (BigQueryDatabaseOption::Endpoint, rest),
        (BigQueryDatabaseOption::StorageEndpoint, storage),
        (BigQueryDatabaseOption::GcsEndpoint, rest),
        (BigQueryDatabaseOption::DataprocEndpoint, rest),
        (BigQueryDatabaseOption::VertexEndpoint, rest),
    ] {
        assert_eq!(database.get_option_string(key.into()).unwrap(), expected);
    }
}
