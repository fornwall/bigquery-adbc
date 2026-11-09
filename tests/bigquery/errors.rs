//! The ADBC status and message of queries BigQuery refuses.

use adbc_core::Database;
use adbc_core::error::Status;
use adbc_driver_bigquery::{
    BigQueryAuthenticationType, BigQueryDatabaseOption, BigQueryQueryPriority,
    BigQueryStatementOption,
};

use crate::support::{
    SCRATCH_DATASET, connect, connect_at_location, execute_failure, open_database, prepare_query,
    project, set_option,
};

/// The `src/error.rs` status classification, which dbt 2.0 branches on, for real refusals:
/// before a job exists, for a name that does not resolve, and at runtime.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reports_the_adbc_status_of_a_failing_query() {
    let project = project();
    let mut connection = connect(project);
    let missing_table =
        format!("SELECT * FROM `{project}.{SCRATCH_DATASET}.__bigquery_adbc_missing_table__`");
    for (what, sql, expected, fragment) in [
        (
            "a syntax error",
            "SELECT FROM WHERE",
            Status::InvalidArguments,
            "BigQuery rejected the query: invalidQuery: Syntax error",
        ),
        (
            // dbt 2.0 finds a missing relation by the `notFound:` spelling.
            "a missing table",
            missing_table.as_str(),
            Status::NotFound,
            "BigQuery rejected the query: notFound: Not found",
        ),
        (
            "a raised error",
            "SELECT ERROR('boom')",
            Status::InvalidArguments,
            "boom",
        ),
        (
            "a division by zero",
            "SELECT 1 / 0",
            Status::InvalidArguments,
            "zero",
        ),
    ] {
        let mut statement = prepare_query(&mut connection, sql);
        let (status, message) = execute_failure(&mut statement);
        assert_eq!(status, expected, "{what}: {message}");
        assert!(message.contains(fragment), "{what}: {message}");
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_failed_query_job_includes_its_console_link() {
    let project = project();
    let mut connection = connect_at_location(project, "US");
    let mut statement = prepare_query(&mut connection, "SELECT 1 / 0");
    // Batch priority uses jobs.insert, so the failure has a durable job. The default
    // jobs.query fast path can reject this SQL without returning any job reference.
    set_option(
        &mut statement,
        BigQueryStatementOption::Priority,
        BigQueryQueryPriority::Batch,
    );
    let (status, message) = execute_failure(&mut statement);
    assert_eq!(status, Status::InvalidArguments, "{message}");
    assert!(message.contains("division by zero"), "{message}");
    assert!(
        message.contains(&format!(
            "https://console.cloud.google.com/bigquery?project={project}&j=bq:"
        )),
        "{message}"
    );
    assert!(message.ends_with("&page=queryresults)"), "{message}");
}

/// A permission failure keeps BigQuery's status. Nobody outside Google can create jobs in
/// `bigquery-public-data`, so the denial costs nothing.
#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn a_project_the_caller_cannot_run_jobs_in_reports_the_bigquery_status() {
    crate::backend::require_real_bigquery();
    let mut connection = connect("bigquery-public-data");
    let mut statement = prepare_query(&mut connection, "SELECT 1");
    let (status, message) = execute_failure(&mut statement);
    assert_eq!(status, Status::Unauthorized, "{message}");
    assert!(message.contains("bigquery-public-data"), "{message}");
}

/// Static-token and anonymous credentials first fail at BigQuery's 401, so its message must name
/// the fix. BigQuery refuses both before creating a job.
#[test]
#[ignore = "requires network access to BigQuery"]
fn a_rejected_static_token_or_anonymous_request_says_what_to_change() {
    crate::backend::require_real_bigquery();
    for (authentication, token, remedy) in [
        (
            BigQueryAuthenticationType::TemporaryAccessToken,
            Some("ya29.not-a-real-token"),
            "the token set as bigquery.auth.access_token was rejected",
        ),
        (
            BigQueryAuthenticationType::Anonymous,
            None,
            "bigquery.auth_type is \"anonymous\", which sends no credentials",
        ),
    ] {
        let mut options = vec![
            (BigQueryDatabaseOption::ProjectId.into(), project().into()),
            (
                BigQueryDatabaseOption::AuthenticationType.into(),
                authentication.into(),
            ),
        ];
        if let Some(token) = token {
            options.push((
                BigQueryDatabaseOption::AuthenticationAccessToken.into(),
                token.into(),
            ));
        }
        let mut connection = open_database(options).new_connection().unwrap();
        let mut statement = prepare_query(&mut connection, "SELECT 1");
        let (status, message) = execute_failure(&mut statement);
        assert_eq!(status, Status::Unauthenticated, "{message}");
        assert!(message.contains(remedy), "{message}");
        assert!(!message.contains("not-a-real-token"), "{message}");
    }
}
