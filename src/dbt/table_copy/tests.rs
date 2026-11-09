use adbc_core::Statement as _;
use adbc_core::error::Status;
use serde_json::{Value as JsonValue, json};

use super::TableCopyOptions;
use crate::query::{QualifiedTableName, StatementQueryOptions};
use crate::rest::jobs::JobReference;
use crate::test_support::driver::{test_statement, test_statement_at};
use crate::test_support::errors::assert_failure;
use crate::test_support::http::{capture_requests_responding, ok};
use crate::test_support::options::{assert_string_option, option_string, set_option};
use crate::{
    BigQueryCreateDisposition, BigQueryStatement, BigQueryStatementOption, BigQueryWriteDisposition,
};

fn set_tables(statement: &mut BigQueryStatement, source: &str, destination: &str) {
    set_option(statement, BigQueryStatementOption::CopyTableSource, source);
    set_option(
        statement,
        BigQueryStatementOption::CopyTableDestination,
        destination,
    );
}

/// The copy has its own write disposition: the query's belongs to the SQL path.
#[test]
fn copy_job_takes_its_own_write_disposition_and_the_statement_create_disposition() {
    let table = |value| QualifiedTableName::parse_option("copy", value, None, None).unwrap();
    let copy = TableCopyOptions {
        source: Some(table("example-project.sq_scratch.tmp_model$20240101")),
        destination: Some(table("example-project.sq_scratch.model$20240101")),
        write_disposition: BigQueryWriteDisposition::WriteTruncate,
    };
    let job = copy
        .build_job(
            &StatementQueryOptions {
                create_disposition: BigQueryCreateDisposition::CreateNever,
                write_disposition: BigQueryWriteDisposition::WriteAppend,
                ..StatementQueryOptions::default()
            },
            copy.source.as_ref().unwrap(),
            copy.destination.as_ref().unwrap(),
            JobReference::new("example-project", Some("EU"), "job".to_owned()),
        )
        .unwrap();
    let job: JsonValue = serde_json::from_slice(job.body()).unwrap();
    let copy = &job["configuration"]["copy"];
    assert_eq!(copy["createDisposition"], "CREATE_NEVER");
    assert_eq!(copy["writeDisposition"], "WRITE_TRUNCATE");
}

/// `jobs.insert` may answer without the reference it accepted; the driver then reconciles by
/// fetching the identity it generated itself, which this server only knows from that POST.
#[test]
fn a_partial_success_response_reconciles_the_exact_copy_without_resubmitting() {
    let mut submitted = JsonValue::Null;
    let (endpoint, server) = capture_requests_responding(2, move |request| {
        if submitted.is_null() {
            submitted = request.body["jobReference"].clone();
            return Some(ok(json!({"status": {"state": "DONE"}})));
        }
        Some(ok(json!({
            "jobReference": submitted,
            "status": {"state": "DONE"},
            "statistics": {"copy": {"copiedRows": "5"}}
        })))
    });

    let mut statement = test_statement_at(&endpoint);
    set_tables(
        &mut statement,
        "data-project.analytics.source",
        "data-project.analytics.destination",
    );
    statement.set_sql_query("").unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(5));
    let captured = server.served(2);
    assert!(captured[0].line.starts_with("POST "));
    assert!(captured[1].line.starts_with("GET "));
    assert!(
        captured[1]
            .path()
            .contains(captured[0].body["jobReference"]["jobId"].as_str().unwrap())
    );
}

/// The dbt 2.0 contract end to end: three options, an empty SQL query, one copy job.
#[test]
fn dbt_partition_copy_submits_one_job_and_reports_its_rows() {
    // Echoes the caller-generated identity the way `jobs.insert` does.
    let (endpoint, captured) = capture_requests_responding(1, |request| {
        Some(ok(json!({
            "jobReference": request.body["jobReference"],
            "status": {"state": "DONE"},
            "statistics": {"copy": {"copiedRows": "5"}}
        })))
    });
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::Labels,
        r#"{"dbt_invocation_id":"abc"}"#,
    );
    set_tables(
        &mut statement,
        "data-project.analytics.tmp_model$20240101",
        "data-project.analytics.model$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableWriteDisposition,
        "WRITE_TRUNCATE",
    );
    // dbt 2.0 finalizes an option-only operation with an empty query, which must not discard
    // the copy configured just before it.
    statement.set_sql_query("").unwrap();

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, 5);
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    let request = &captured.served(1)[0];
    assert!(
        request
            .line
            .starts_with("POST /bigquery/v2/projects/fixture-project/jobs"),
        "{}",
        request.line
    );
    assert_eq!(
        request.body["configuration"]["copy"],
        json!({
            "sourceTable": {
                "projectId": "data-project",
                "datasetId": "analytics",
                "tableId": "tmp_model$20240101",
            },
            "destinationTable": {
                "projectId": "data-project",
                "datasetId": "analytics",
                "tableId": "model$20240101",
            },
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_TRUNCATE",
        }),
        "the two option-named endpoints, partition decorators unchanged, and nothing else"
    );
    assert_eq!(
        request.body["configuration"]["labels"]["dbt_invocation_id"],
        "abc"
    );
    assert_eq!(request.body["jobReference"]["location"], "EU");
    assert_eq!(request.body["configuration"].get("query"), None);
}

#[test]
fn copy_options_round_trip_resolve_defaults_and_switch_operations_cleanly() {
    let mut statement = test_statement();
    assert_eq!(
        option_string(
            &statement,
            BigQueryStatementOption::CopyTableWriteDisposition
        ),
        "WRITE_EMPTY",
        "an unset disposition reads back as the one BigQuery would apply"
    );

    // Unqualified endpoints resolve against the statement defaults, like a destination table.
    set_option(
        &mut statement,
        BigQueryStatementOption::DefaultDatasetId,
        "analytics",
    );
    set_tables(&mut statement, "tmp_model$20240101", "reporting.model");
    assert_eq!(
        option_string(&statement, BigQueryStatementOption::CopyTableSource),
        "fixture-project.analytics.tmp_model$20240101"
    );
    assert_eq!(
        option_string(&statement, BigQueryStatementOption::CopyTableDestination),
        "fixture-project.reporting.model"
    );

    // dbt 2.0 asks for an empty query, but its query-comment machinery may hand the driver a
    // comment-only statement. Like the fork, which dispatches on the copy options first, that
    // keeps the copy.
    for query in [
        "",
        "/* bigquery-adbc: model.project.copied */",
        "# a hash comment",
    ] {
        statement.set_sql_query(query).unwrap();
        assert_string_option(
            &statement,
            BigQueryStatementOption::CopyTableDestination,
            Some("fixture-project.reporting.model"),
        );
    }

    // An empty value clears one endpoint; the other keeps the operation selected so that
    // executing reports the missing endpoint instead of a missing query.
    set_option(&mut statement, BigQueryStatementOption::CopyTableSource, "");
    assert_string_option(&statement, BigQueryStatementOption::CopyTableSource, None);
    let error = statement.execute_update().unwrap_err();
    assert_failure(
        &error,
        Status::InvalidState,
        &[BigQueryStatementOption::CopyTableSource.as_str()],
    );

    // Real SQL leaves the copy behind entirely.
    statement.set_sql_query("/* comment */ select 1").unwrap();
    assert_string_option(
        &statement,
        BigQueryStatementOption::CopyTableDestination,
        None,
    );
}
