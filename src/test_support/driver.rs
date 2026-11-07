//! Driver fixtures with anonymous credentials and an unreachable or mock endpoint.
//! Shared by the statement, connection, FFI, and dbt-core tests without Google Cloud credentials.

use adbc_core::{Connection as _, Database as _, Driver as _, Statement as _};

use crate::{
    BigQueryAuthenticationType, BigQueryConnection, BigQueryDatabaseOption, BigQueryDriver,
    BigQueryStatement,
};

pub(crate) fn test_connection() -> BigQueryConnection {
    test_connection_at(super::UNREACHABLE_ENDPOINT)
}

pub(crate) fn test_connection_at(endpoint: &str) -> BigQueryConnection {
    test_connection_with(endpoint, [])
}

/// A test connection with extra options, such as another service endpoint pointed at a mock.
pub(crate) fn test_connection_with(
    endpoint: &str,
    options: impl IntoIterator<
        Item = (
            adbc_core::options::OptionDatabase,
            adbc_core::options::OptionValue,
        ),
    >,
) -> BigQueryConnection {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts(
            [
                (
                    BigQueryDatabaseOption::ProjectId.into(),
                    "fixture-project".into(),
                ),
                (
                    BigQueryDatabaseOption::AuthenticationType.into(),
                    BigQueryAuthenticationType::Anonymous.into(),
                ),
                (
                    BigQueryDatabaseOption::Endpoint.into(),
                    endpoint.to_owned().into(),
                ),
                (BigQueryDatabaseOption::Location.into(), "EU".into()),
            ]
            .into_iter()
            .chain(options),
        )
        .unwrap();
    database.new_connection().unwrap()
}

/// A test connection at `endpoint` with no `bigquery.location`, as from a dbt profile that omits
/// `location`.
pub(crate) fn unlocated_connection_at(endpoint: &str) -> BigQueryConnection {
    test_connection_with(
        endpoint,
        [(BigQueryDatabaseOption::Location.into(), "".into())],
    )
}

pub(crate) fn test_statement() -> BigQueryStatement {
    test_connection().new_statement().unwrap()
}

/// A test statement whose connection points at `endpoint`.
pub(crate) fn test_statement_at(endpoint: &str) -> BigQueryStatement {
    test_connection_at(endpoint).new_statement().unwrap()
}

/// A [`test_statement_at`] with `sql` set.
#[allow(dead_code, reason = "adopted by the test modules that run one query")]
pub(crate) fn test_query_at(endpoint: &str, sql: &str) -> BigQueryStatement {
    let mut statement = test_statement_at(endpoint);
    statement.set_sql_query(sql).unwrap();
    statement
}

/// Publishes execution progress without running a query, for FFI getter tests.
#[cfg(feature = "ffi")]
pub(crate) fn publish_progress(statement: &BigQueryStatement, current: f64, maximum: f64) {
    statement
        .progress_handle()
        .publish_for_test(current, maximum);
}
