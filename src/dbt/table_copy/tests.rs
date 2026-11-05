use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use adbc_core::error::Status;
use adbc_core::{Connection as _, Statement as _};
use serde_json::{Value as JsonValue, json};

use super::{TableCopyOptions, WHAT};
use crate::cancellation::{Cancellation, Operation};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::dbt::jobs::{decode_job, validate_completed, wait_for_submitted};
use crate::query::{QualifiedTableName, StatementQueryOptions};
use crate::rest::jobs::{JobClient, JobReference};
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::driver::{test_connection_at, test_statement, test_statement_at};
use crate::test_support::errors::assert_message;
use crate::test_support::http::{CapturedRequest, capture_requests, capture_requests_responding};
use crate::test_support::options::{assert_string_option, option_string, set_option};
use crate::{BigQueryCreateDisposition, BigQueryStatementOption, BigQueryWriteDisposition};

fn table(value: &str) -> QualifiedTableName {
    QualifiedTableName::parse_option(
        BigQueryStatementOption::CopyTableSource.as_str(),
        value,
        None,
        None,
    )
    .unwrap()
}

fn options(source: &str, destination: &str) -> TableCopyOptions {
    TableCopyOptions {
        source: Some(table(source)),
        destination: Some(table(destination)),
        ..TableCopyOptions::default()
    }
}

/// Everything one `wait_for_submitted` needs against a mock server: the runtime, the
/// cancellation scope, the transport, and the still-running job `reference` names.
fn waiting_at(endpoint: &str) -> (SharedRuntime, Operation, JobClient, &'static [u8]) {
    let runtime = new_runtime().unwrap();
    let cancellation = Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        "statement",
    );
    let operation = cancellation.begin();
    let client = JobClient::for_test(endpoint);
    let running = br#"{"jobReference":{"projectId":"billing-project","jobId":"copy-job","location":"EU"},"status":{"state":"RUNNING"}}"#;
    (runtime, operation, client, running)
}

#[test]
fn copy_job_carries_both_endpoints_and_the_statement_job_options() {
    let mut copy = options(
        "example-project.sq_scratch.tmp_model$20240101",
        "example-project.sq_scratch.model$20240101",
    );
    copy.write_disposition = BigQueryWriteDisposition::WriteTruncate;
    let job = copy
        .build_job(
            &StatementQueryOptions {
                create_disposition: BigQueryCreateDisposition::CreateNever,
                job_timeout_ms: 12_345,
                labels: Some(Arc::new(BTreeMap::from([(
                    "dbt_node".to_owned(),
                    "model".to_owned(),
                )]))),
                reservation: Some(Arc::from("projects/p/locations/EU/reservations/r")),
                // The query write disposition belongs to the SQL path and must not leak into a
                // copy job that has its own option.
                write_disposition: BigQueryWriteDisposition::WriteAppend,
                ..StatementQueryOptions::default()
            },
            copy.source.as_ref().unwrap(),
            copy.destination.as_ref().unwrap(),
            JobReference::new("example-project", Some("EU"), "job".to_owned()),
        )
        .unwrap();
    let job: serde_json::Value = serde_json::from_slice(job.body()).unwrap();
    let reference = &job["jobReference"];
    assert_eq!(reference["location"], "EU");
    let configuration = &job["configuration"];
    assert_eq!(configuration["jobTimeoutMs"], "12345");
    assert_eq!(configuration["labels"]["dbt_node"], "model");
    assert_eq!(
        configuration["reservation"],
        "projects/p/locations/EU/reservations/r"
    );
    assert_eq!(configuration.get("dryRun"), None);
    let copy = &configuration["copy"];
    let source = &copy["sourceTable"];
    assert_eq!(source["projectId"], "example-project");
    assert_eq!(source["datasetId"], "sq_scratch");
    assert_eq!(
        source["tableId"], "tmp_model$20240101",
        "partition decorators must reach BigQuery unchanged"
    );
    assert_eq!(copy["destinationTable"]["tableId"], "model$20240101");
    assert_eq!(copy["createDisposition"], "CREATE_NEVER");
    assert_eq!(copy["writeDisposition"], "WRITE_TRUNCATE");
    assert_eq!(copy.get("sourceTables"), None);
}

#[test]
fn domain_scoped_copy_endpoints_reach_the_job_configuration() {
    let (endpoint, captured) = capture_copy_with_echoed_reference();
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "analytics.example.com:source.raw.events$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "example.org:destination.marts.events$20240101",
    );
    assert_eq!(statement.execute_update().unwrap(), Some(5));

    let requests = captured.join().unwrap();
    let job = &requests[0].body;
    assert_eq!(job["jobReference"]["projectId"], "fixture-project");
    let copy = &job["configuration"]["copy"];
    assert_eq!(
        copy["sourceTable"],
        json!({
            "projectId": "analytics.example.com:source",
            "datasetId": "raw",
            "tableId": "events$20240101",
        })
    );
    assert_eq!(
        copy["destinationTable"],
        json!({
            "projectId": "example.org:destination",
            "datasetId": "marts",
            "tableId": "events$20240101",
        })
    );
}

#[test]
fn copy_job_defaults_to_a_non_destructive_disposition() {
    let copy = options(
        "example-project.sq_scratch.source",
        "example-project.sq_scratch.destination",
    );
    assert_eq!(copy.write_disposition, BigQueryWriteDisposition::WriteEmpty);
    let job = copy
        .build_job(
            &StatementQueryOptions::default(),
            copy.source.as_ref().unwrap(),
            copy.destination.as_ref().unwrap(),
            JobReference::new("example-project", None, "job".to_owned()),
        )
        .unwrap();
    let job: serde_json::Value = serde_json::from_slice(job.body()).unwrap();
    let copy = &job["configuration"]["copy"];
    assert_eq!(copy["createDisposition"], "CREATE_IF_NEEDED");
    assert_eq!(copy["writeDisposition"], "WRITE_EMPTY");
}

#[test]
fn copied_rows_reports_counts_unknown_counts_and_service_failures() {
    let copied = |body: &str| {
        let mut job = decode_job(body.as_bytes(), WHAT).unwrap();
        validate_completed(&mut job, WHAT).map(|()| job.copied_rows())
    };
    assert_eq!(
        copied(r#"{"status":{"state":"DONE"},"statistics":{"copy":{"copiedRows":"7"}}}"#).unwrap(),
        Some(7)
    );
    assert_eq!(
        copied(r#"{"status":{"state":"DONE"}}"#).unwrap(),
        None,
        "a copy BigQuery satisfies as metadata reports no row count"
    );

    let error = copied(
        r#"{
            "status": {
                "state": "DONE",
                "errorResult": {
                    "reason": "notFound",
                    "message": "Not found: Table example-project:sq_scratch.missing"
                }
            }
        }"#,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(
        error.message.contains("notFound:"),
        "dbt-core 2.0 classifies a missing relation by this prefix: {}",
        error.message
    );
}

#[test]
fn copy_poll_tolerates_initial_job_visibility_delay() {
    let reference = JobReference::new("billing-project", Some("EU"), "copy-job".to_owned());
    let (endpoint, captured) = capture_requests(vec![
        (
            404,
            json!({"error":{"errors":[{"reason":"notFound","message":"not visible yet"}]}}),
        ),
        (
            200,
            json!({
                "jobReference":{"projectId":"billing-project","jobId":"copy-job","location":"EU"},
                "status":{"state":"DONE"},
                "statistics":{"copy":{"copiedRows":"8"}}
            }),
        ),
    ]);
    let (runtime, operation, client, initial) = waiting_at(&endpoint);

    let completed = runtime
        .block_on(wait_for_submitted(
            &client, &operation, &reference, initial, WHAT,
        ))
        .unwrap();
    assert_eq!(completed.copied_rows(), Some(8));
    operation.finish();
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].line, captured[1].line);
}

/// A wait that never sees a good response issues exactly `MAX_ATTEMPTS` polls, then reports the
/// last failure.
#[test]
fn copy_polls_report_the_last_transient_failure_of_the_budget() {
    let reference = JobReference::new("billing-project", Some("EU"), "copy-job".to_owned());
    let budget = usize::try_from(crate::rest::retry::MAX_ATTEMPTS).unwrap();
    let (endpoint, captured) = capture_requests(
        std::iter::repeat_with(|| {
            (
                500,
                json!({"error":{"message":"the service is unavailable"}}),
            )
        })
        .take(budget)
        .collect(),
    );
    let (runtime, operation, client, initial) = waiting_at(&endpoint);

    let failure = runtime
        .block_on(wait_for_submitted(
            &client, &operation, &reference, initial, WHAT,
        ))
        .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::IO);
    assert!(
        failure.message.starts_with(
            "failed to poll the BigQuery table copy job billing-project:EU.copy-job: "
        ),
        "{}",
        failure.message
    );
    assert_eq!(
        captured.join().unwrap().len(),
        budget,
        "four failures are absorbed and the fifth is reported, rather than a sixth poll"
    );
}

/// BigQuery rejects `dryRun` on a copy job (`400 Dry run not supported for copy job`), so a dry
/// run is refused before any request.
#[test]
fn a_dry_run_copy_is_refused_without_a_request() {
    let (endpoint, captured) = capture_requests(Vec::new());
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "data-project.analytics.source",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "data-project.analytics.destination",
    );
    set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
    statement.set_sql_query("").unwrap();

    let failure = statement.execute_update().unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_message(&failure, &[BigQueryStatementOption::DryRun.as_str()]);
    assert!(captured.join().unwrap().is_empty());
}

#[test]
fn a_partial_success_response_reconciles_the_exact_copy_without_resubmitting() {
    // `jobs.insert` may answer without the reference it accepted; the driver then reconciles by
    // fetching the identity it generated itself, which this server only knows from that POST.
    let mut submitted = JsonValue::Null;
    let (endpoint, server) = capture_requests_responding(2, move |request| {
        if submitted.is_null() {
            submitted = request.body["jobReference"].clone();
            return Some((200, json!({"status": {"state": "DONE"}})));
        }
        Some((
            200,
            json!({
                "jobReference": submitted,
                "status": {"state": "DONE"},
                "statistics": {"copy": {"copiedRows": "5"}}
            }),
        ))
    });

    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "data-project.analytics.source",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "data-project.analytics.destination",
    );
    statement.set_sql_query("").unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(5));
    let captured = server.join().unwrap();
    assert!(captured[0].line.starts_with("POST "));
    assert!(captured[1].line.starts_with("GET "));
    assert!(
        captured[1]
            .path()
            .contains(captured[0].body["jobReference"]["jobId"].as_str().unwrap())
    );
}

/// The dbt-core 2.0 contract end to end: three options, an empty SQL query, one copy job.
#[test]
fn dbt_partition_copy_submits_one_job_and_reports_its_rows() {
    let (endpoint, captured) = capture_copy_with_echoed_reference();
    let mut connection = test_connection_at(&endpoint);
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::Labels,
        r#"{"dbt_invocation_id":"abc"}"#,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "data-project.analytics.tmp_model$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "data-project.analytics.model$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableWriteDisposition,
        "WRITE_TRUNCATE",
    );
    // dbt-core 2.0 finalizes an option-only operation with an empty query, which must not discard
    // the copy configured just before it.
    statement.set_sql_query("").unwrap();

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, 5);
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 0);
    assert!(reader.next().is_none());

    let captured = captured.join().unwrap();
    let request = &captured[0];
    assert_eq!(
        schema
            .metadata()
            .get(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID),
        request.body["jobReference"]["jobId"]
            .as_str()
            .map(str::to_owned)
            .as_ref(),
        "the copy job is reported the way a query result reports its job"
    );
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
        request.body["configuration"]["labels"]["dbt_invocation_id"], "abc",
        "dbt-core 2.0 sets job labels on every statement"
    );
    assert_eq!(
        request.body["jobReference"]["location"], "EU",
        "the copy job runs where the connection is configured"
    );
    assert_eq!(request.body["configuration"].get("query"), None);
}

/// Echoes the caller-generated identity the way `jobs.insert` does, so the end-to-end copy checks
/// the same response-identity validation production uses instead of hard-coding an unknown job ID.
fn capture_copy_with_echoed_reference() -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    capture_requests_responding(1, |request| {
        Some((
            200,
            json!({
                "jobReference": request.body["jobReference"],
                "status": {"state": "DONE"},
                "statistics": {"copy": {"copiedRows": "5"}}
            }),
        ))
    })
}

#[test]
fn copy_options_round_trip_resolve_defaults_and_switch_operations_cleanly() {
    let mut statement = test_statement();
    for option in [
        BigQueryStatementOption::CopyTableSource,
        BigQueryStatementOption::CopyTableDestination,
    ] {
        assert_string_option(&statement, option, None);
    }
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
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "tmp_model$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "reporting.model",
    );
    assert_eq!(
        option_string(&statement, BigQueryStatementOption::CopyTableSource),
        "fixture-project.analytics.tmp_model$20240101"
    );
    assert_eq!(
        option_string(&statement, BigQueryStatementOption::CopyTableDestination),
        "fixture-project.reporting.model"
    );

    // An empty value clears one endpoint; the other keeps the operation selected so that
    // executing reports the missing endpoint instead of a missing query.
    set_option(&mut statement, BigQueryStatementOption::CopyTableSource, "");
    assert_string_option(&statement, BigQueryStatementOption::CopyTableSource, None);
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &[BigQueryStatementOption::CopyTableSource.as_str()]);

    // Real SQL leaves the copy behind entirely.
    statement.set_sql_query("select 1").unwrap();
    for option in [
        BigQueryStatementOption::CopyTableSource,
        BigQueryStatementOption::CopyTableDestination,
    ] {
        assert_string_option(&statement, option, None);
    }
}

/// dbt-core 2.0 asks for an empty query but its query-comment machinery prepends the node
/// comment, so the driver is handed a comment-only statement. Like the fork's driver, which
/// dispatches on the copy options before it ever looks at the query, that must run the copy.
#[test]
fn a_query_comment_finalizes_a_copy_while_real_sql_replaces_it() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "data-project.analytics.tmp_model$20240101",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "data-project.analytics.model$20240101",
    );

    for query in [
        "",
        "/* bigquery-adbc: model.project.exotic_copied_partitions */",
        "-- a line comment\n",
        "# a hash comment",
        "/* unterminated",
        "  /* one */ -- and another\n  ",
    ] {
        statement.set_sql_query(query).unwrap();
        assert_eq!(
            option_string(&statement, BigQueryStatementOption::CopyTableSource),
            "data-project.analytics.tmp_model$20240101",
            "a query carrying no statement must not discard the copy: {query:?}"
        );
    }

    statement.set_sql_query("/* comment */ select 1").unwrap();
    // A real statement leaves the copy behind.
    assert_string_option(&statement, BigQueryStatementOption::CopyTableSource, None);
}

#[test]
fn a_bulk_ingest_target_replaces_a_configured_copy() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        "data-project.analytics.model",
    );
    set_option(
        &mut statement,
        adbc_core::options::OptionStatement::TargetTable,
        "ingested",
    );
    assert_string_option(
        &statement,
        BigQueryStatementOption::CopyTableDestination,
        None,
    );
}

#[test]
fn a_copy_has_no_result_schema() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        "data-project.analytics.model",
    );
    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
}
