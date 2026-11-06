//! Running a statement: the execute entry points, bound parameters and the wire form they
//! take, prepare, and the cancellation a superseded result reports.

use super::*;
use crate::test_support::driver::test_statement_at;
use crate::test_support::errors::assert_message;
use crate::test_support::http::job_reference;

#[test]
fn reservation_reaches_both_submission_paths_and_clearing_omits_it() {
    for dry_run in [false, true] {
        let response = if dry_run {
            json!({"status":{"state":"DONE"}, "statistics":{"query":{"statementType":"DELETE"}}})
        } else {
            json!({"jobReference": job_reference("fixture-project", "execution-job-1"), "jobComplete":true})
        };
        let (endpoint, requests) = capture_requests(vec![(200, response); 4]);
        let mut statement = test_statement_at(&endpoint);
        statement
            .set_sql_query("DELETE FROM dataset.table WHERE false")
            .unwrap();
        set_option(
            &mut statement,
            BigQueryStatementOption::DryRun,
            if dry_run { "true" } else { "false" },
        );
        // Exercise the default as well as clearing on the same reusable statement.
        statement.execute_update().unwrap();
        let reservation = "projects/billing-project/locations/EU/reservations/analytics";
        for value in [reservation, "none", ""] {
            set_option(&mut statement, BigQueryStatementOption::Reservation, value);
            statement.execute_update().unwrap();
        }
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 4);
        for (request, expected) in
            requests
                .iter()
                .zip([None, Some(reservation), Some("none"), None])
        {
            let configuration = if dry_run {
                assert!(request.path().ends_with("/jobs"));
                assert!(request.body.get("reservation").is_none());
                assert!(
                    request.body["configuration"]["query"]
                        .get("reservation")
                        .is_none()
                );
                &request.body["configuration"]
            } else {
                assert!(request.path().ends_with("/queries"));
                &request.body
            };
            assert_eq!(
                configuration
                    .get("reservation")
                    .and_then(serde_json::Value::as_str),
                expected
            );
            if expected.is_none() {
                assert!(configuration.get("reservation").is_none());
            }
        }
    }
}

#[test]
fn failed_update_poll_cancels_the_unfinished_remote_job() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": false,
                "jobReference": {"projectId":"fixture-project", "jobId":"running-job", "location":"EU"}
            }),
        ),
        (
            403,
            json!({"error":{"errors":[{"reason":"accessDenied", "message":"poll denied"}]}}),
        ),
        (200, json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("DELETE FROM dataset.table WHERE true")
        .unwrap();

    let failure = statement.execute_update().unwrap_err();
    assert_eq!(failure.status, Status::Unauthorized);
    assert_message(&failure, &["poll denied"]);
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].path(),
        "/bigquery/v2/projects/fixture-project/jobs/running-job/cancel?location=EU"
    );
}

#[test]
fn completed_query_reader_cancellation_needs_no_job_cancel_permission() {
    for complete_on_submit in [true, false] {
        let reference =
            json!({"projectId":"fixture-project", "jobId":"completed-job", "location":"EU"});
        let mut responses = Vec::new();
        if !complete_on_submit {
            responses.push((200, json!({"jobComplete":false, "jobReference":reference})));
        }
        responses.push((200, json!({"jobComplete":true, "jobReference":reference})));
        let (endpoint, requests) = capture_requests(responses);
        let mut statement = test_statement_at(&endpoint);
        statement
            .set_sql_query("DELETE FROM dataset.table WHERE true")
            .unwrap();
        let reader = statement.execute().unwrap();
        let requests = requests.join().unwrap();

        // The server has closed. A cancel targeting the finished job would fail here.
        statement.get_cancel_handle().try_cancel().unwrap();
        drop(reader);
        assert_eq!(requests.len(), if complete_on_submit { 1 } else { 2 });
    }
}

#[test]
fn dry_run_execute_partitions_is_rejected_before_any_request() {
    let mut statement = test_statement();
    statement.set_sql_query("SELECT 1").unwrap();
    set_option(&mut statement, BigQueryStatementOption::DryRun, "true");

    // A dry run creates no job, and partitions are job references, so the combination is
    // rejected instead of reporting success with zero partitions.
    let error = statement.execute_partitions().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["bigquery.query.dry_run"]);
}

#[test]
fn dry_run_partition_rejection_preserves_empty_and_nonempty_bindings() {
    for values in [Vec::new(), vec![42]] {
        let mut statement = test_statement();
        statement.set_sql_query("SELECT ? AS value").unwrap();
        statement.bind(int64_column("value", values)).unwrap();
        set_option(&mut statement, BigQueryStatementOption::DryRun, "true");

        let failure = statement.execute_partitions().unwrap_err();
        assert_eq!(failure.status, Status::InvalidState);
        assert_message(&failure, &["bigquery.query.dry_run"]);
        assert!(statement.parameters.schema().is_some());
    }
}

/// A dry run executes nothing, so it types bound parameters from their schema and leaves the
/// binding for the execution it validates, as an ingest dry run keeps its data.
#[test]
fn a_dry_run_leaves_bound_parameters_for_the_next_execute() {
    let schema = json!({"fields": [{"name": "answer", "type": "INTEGER"}]});
    let dry_run = (
        200,
        json!({
            "statistics": {"query": {"statementType": "SELECT", "schema": schema}},
            "status": {"state": "DONE"}
        }),
    );
    let executed = (
        200,
        json!({
            "jobReference": job_reference("fixture-project", "execution-job-2"),
            "jobComplete": true,
            "statementType": "SELECT",
            "schema": schema,
            "rows": [{"f": [{"v": "7"}]}],
            "totalRows": "1"
        }),
    );
    for update in [false, true] {
        let (endpoint, requests) = capture_requests(vec![dry_run.clone(), executed.clone()]);
        let mut statement = test_statement_at(&endpoint);
        statement.set_sql_query("SELECT ? AS answer").unwrap();
        statement.bind(int64_column("value", vec![7])).unwrap();

        set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
        if update {
            assert_eq!(statement.execute_update().unwrap(), None);
        } else {
            let (reader, affected) = statement.execute_with_rows_affected().unwrap();
            assert_eq!(affected, -1);
            assert_eq!(reader.schema().field(0).name(), "answer");
        }

        set_option(&mut statement, BigQueryStatementOption::DryRun, "false");
        if update {
            statement.execute_update().unwrap();
        } else {
            let batches = statement
                .execute()
                .unwrap()
                .collect::<Result<Vec<_>, ArrowError>>()
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        }

        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 2, "update={update}");
        let typed = &requests[0].body["configuration"]["query"]["queryParameters"][0];
        assert_eq!(requests[0].body["configuration"]["dryRun"], true);
        assert_eq!(typed["parameterType"]["type"], "INT64");
        assert_eq!(
            requests[1].body["queryParameters"][0]["parameterValue"]["value"],
            "7"
        );
    }
}

#[test]
fn dry_runs_accept_statements_without_result_columns() {
    for statement_type in ["DROP_TABLE", "DELETE", "SCRIPT"] {
        let response = (
            200,
            json!({
                "statistics": {"query": {"statementType": statement_type}},
                "status": {"state": "DONE"}
            }),
        );
        let (endpoint, requests) = capture_requests(vec![response.clone(), response]);
        let mut statement = test_statement_at(&endpoint);
        statement
            .set_sql_query("DROP TABLE IF EXISTS dataset.table")
            .unwrap();
        assert!(statement.execute_schema().unwrap().fields().is_empty());

        set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
        let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
        assert!(reader.schema().fields().is_empty());
        assert!(reader.next().is_none());
        assert_eq!(affected, -1);
        for request in requests.join().unwrap() {
            assert_eq!(request.body["configuration"]["dryRun"], true);
        }
    }
}

#[test]
fn dry_runs_reject_missing_select_schemas_or_query_statistics() {
    for query in [json!({"statementType": "SELECT"}), serde_json::Value::Null] {
        let (endpoint, requests) = capture_requests(vec![(
            200,
            json!({"statistics": {"query": query}, "status": {"state": "DONE"}}),
        )]);
        let mut statement = test_statement_at(&endpoint);
        statement.set_sql_query("SELECT 1").unwrap();
        let failure = statement.execute_schema().unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_message(&failure, &["without a result schema"]);
        requests.join().unwrap();
    }
}

#[test]
fn parameterless_queries_have_an_empty_parameter_schema() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "statistics": {"query": {"statementType": "SELECT"}},
            "status": {"state": "DONE"}
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 1").unwrap();
    statement.prepare().unwrap();
    for _ in 0..2 {
        let schema = statement.get_parameter_schema().unwrap();
        assert!(schema.fields().is_empty());
    }
    assert_eq!(requests.join().unwrap().len(), 1);
}

#[test]
fn parameter_discovery_rejects_missing_query_statistics() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({"statistics": {}, "status": {"state": "DONE"}}),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 1").unwrap();
    let failure = statement.get_parameter_schema().unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_message(&failure, &["without query statistics"]);
    requests.join().unwrap();
}

#[test]
fn parameterized_update_reports_unknown_rows_when_a_count_is_missing() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-3"), "jobComplete": true, "numDmlAffectedRows": "3"}),
        ),
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-4"), "jobComplete": true}),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("INSERT dataset.table VALUES (?)")
        .unwrap();
    let batch = int64_column("value", vec![1_i64, 2_i64]);
    statement.bind(batch).unwrap();

    // The first row affected 3 rows, the second reported no count: the combined count is
    // unknown rather than a partial sum, matching execute_partitions.
    assert_eq!(statement.execute_update().unwrap(), None);
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.body["maxResults"] == 0)
    );
}

/// The completing response carries `numDmlAffectedRows`, so DML run through
/// `AdbcStatementExecuteQuery` reports the same count as `AdbcStatementExecuteUpdate`, which
/// Python's `cursor.rowcount` relies on. DML that matched nothing reports 0; DDL reports -1. A
/// script ending in DML is answered with neither count (verified live), so it reports -1 too.
#[test]
fn dml_through_execute_query_reports_its_affected_rows() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-5"), "jobComplete": true, "statementType": "DELETE", "numDmlAffectedRows": "7"}),
        ),
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-6"), "jobComplete": true, "statementType": "UPDATE", "numDmlAffectedRows": "0"}),
        ),
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-7"), "jobComplete": true, "statementType": "CREATE_TABLE"}),
        ),
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-8"), "jobComplete": true, "statementType": "SCRIPT"}),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    for (sql, expected) in [
        ("DELETE FROM dataset.table WHERE true", 7),
        ("UPDATE dataset.table SET value = 1 WHERE false", 0),
        ("CREATE TABLE dataset.other (value INT64)", -1),
        (
            "CREATE TEMP TABLE t AS SELECT 1 AS a; UPDATE t SET a = 2 WHERE true",
            -1,
        ),
    ] {
        statement.set_sql_query(sql).unwrap();
        let (reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
        drop(reader);
        assert_eq!(rows_affected, expected, "{sql}");
    }

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| request.body["maxResults"] == 100_000)
    );
}

#[test]
fn explicit_destination_execute_query_requests_metadata_without_rows() {
    let mut job_reference = serde_json::Value::Null;
    let (endpoint, requests) = capture_requests_responding(2, move |request| {
        if job_reference.is_null() {
            job_reference = request.body["jobReference"].clone();
            return Some((
                200,
                json!({
                    "jobReference": job_reference,
                    "statistics": {"query": {"statementType": "SELECT"}},
                    "status": {"state": "DONE"}
                }),
            ));
        }
        Some((
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference,
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "7"}]}],
                "pageToken": "must-not-be-fetched",
                // The destination table's size, not the rows this query wrote.
                "totalRows": "6"
            }),
        ))
    });
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        "fixture-project.data.result",
    );
    statement.set_sql_query("SELECT 7 AS value").unwrap();

    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert!(reader.next().is_none());
    // `totalRows` counts the whole destination table (observed, not documented), so the number
    // of rows this query wrote is unknown.
    assert_eq!(affected, -1);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].body["configuration"]["query"]["destinationTable"]["tableId"],
        "result"
    );
    assert!(requests[1].path().contains("maxResults=0"));
}

#[test]
fn an_explicit_destination_runs_every_bound_row_before_execute_returns() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let request_count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&request_count);
    let mut job_reference = serde_json::Value::Null;
    let (endpoint, requests) = capture_requests_responding(6, move |request| {
        observed.fetch_add(1, Ordering::Relaxed);
        if request.body.get("jobReference").is_some() {
            job_reference = request.body["jobReference"].clone();
            return Some((
                200,
                json!({
                    "jobReference": job_reference,
                    "statistics": {"query": {"statementType": "SELECT"}},
                    "status": {"state": "DONE"}
                }),
            ));
        }
        Some((
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference,
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "totalRows": "100"
            }),
        ))
    });
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        "fixture-project.data.result",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    statement.set_sql_query("SELECT ? AS value").unwrap();
    statement
        .bind(int64_column("value", vec![1, 2, 3]))
        .unwrap();

    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    assert_eq!(request_count.load(Ordering::Relaxed), 6);
    // Destination writes return no result rows, so callers need not drain their stream.
    drop(reader);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 6);
    for ([submitted, results], value) in requests.as_chunks::<2>().0.iter().zip(["1", "2", "3"]) {
        let query = &submitted.body["configuration"]["query"];
        assert_eq!(
            query["queryParameters"][0]["parameterValue"]["value"],
            value
        );
        assert_eq!(query["writeDisposition"], "WRITE_APPEND");
        assert!(results.path().contains("maxResults=0"));
    }
}

/// dbt-core 2.0 sends a profile's `priority: batch` on every statement, and a profile need not
/// set `location`. The job runs where BigQuery infers, and its rows are read from there.
#[test]
fn a_batch_query_without_a_location_reads_its_rows_where_bigquery_ran_it() {
    let mut job_reference = serde_json::Value::Null;
    let (endpoint, requests) = capture_requests_responding(2, move |request| {
        if job_reference.is_null() {
            job_reference = request.body["jobReference"].clone();
            job_reference["location"] = json!("europe-west2");
            return Some((
                200,
                json!({"jobReference": job_reference, "status": {"state": "PENDING"}}),
            ));
        }
        Some((
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference,
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "7"}]}],
                "totalRows": "1"
            }),
        ))
    });
    let mut connection = crate::test_support::driver::unlocated_connection_at(&endpoint);
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        "adbc.bigquery.sql.query.priority",
        BigQueryQueryPriority::Batch,
    );
    statement.set_sql_query("SELECT 7 AS value").unwrap();

    let batches = statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, ArrowError>>()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    let submitted = &requests[0].body;
    assert!(
        submitted["jobReference"].get("location").is_none(),
        "{submitted}"
    );
    assert_eq!(submitted["configuration"]["query"]["priority"], "BATCH");
    assert!(
        requests[1].path().ends_with("&location=europe-west2"),
        "{}",
        requests[1].path()
    );
}

/// Setting a documented default is semantically the same as leaving it alone. In
/// particular, it must not turn a query that can use `jobs.query` into a job insertion.
#[test]
fn explicit_query_defaults_keep_the_jobs_query_path() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({"jobReference": job_reference("fixture-project", "execution-job-9"), "jobComplete": true}),
    )]);
    let mut statement = test_statement_at(&endpoint);
    for (option, value) in [
        (
            BigQueryStatementOption::CreateDisposition,
            OptionValue::from(BigQueryCreateDisposition::CreateIfNeeded),
        ),
        (
            BigQueryStatementOption::WriteDisposition,
            BigQueryWriteDisposition::WriteEmpty.into(),
        ),
        (
            BigQueryStatementOption::Priority,
            BigQueryQueryPriority::Interactive.into(),
        ),
        (BigQueryStatementOption::MaximumBytesBilled, 0_i64.into()),
        (BigQueryStatementOption::JobTimeout, 0_i64.into()),
        (BigQueryStatementOption::CreateSession, false.into()),
        (BigQueryStatementOption::DisableQueryCache, false.into()),
        (BigQueryStatementOption::DryRun, false.into()),
    ] {
        set_option(&mut statement, option, value);
    }
    statement.set_sql_query("SELECT 1").unwrap();
    statement.execute_update().unwrap();

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].path().contains("/queries"),
        "{}",
        requests[0].path()
    );
    assert_eq!(requests[0].body["jobCreationMode"], "JOB_CREATION_OPTIONAL");
    assert_eq!(requests[0].body["timeoutMs"], 10_000);
    for elided in [
        "createDisposition",
        "writeDisposition",
        "priority",
        "maximumBytesBilled",
        "jobTimeoutMs",
        "createSession",
        "useQueryCache",
        "dryRun",
    ] {
        assert!(
            requests[0].body.get(elided).is_none(),
            "jobs.query request carried {elided}: {}",
            requests[0].body
        );
    }
}

/// Labels and the job timeout must actually reach the BigQuery job request.
#[test]
fn job_labels_and_timeout_reach_the_query_request() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({"jobReference": job_reference("fixture-project", "execution-job-10"), "jobComplete": true}),
    )]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.sql.query.labels".to_owned()),
        r#"{"dbt_invocation_id":"abc-123"}"#.to_owned(),
    );
    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.sql.query.job_timeout".to_owned()),
        OptionValue::Int(90_000),
    );
    statement.set_sql_query("SELECT 1").unwrap();
    statement.execute_update().unwrap();

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    // Both options are `jobs.query` request fields, so they must not push the query onto
    // `jobs.insert`.
    assert!(
        requests[0].path().contains("/queries"),
        "{}",
        requests[0].path()
    );
    let request = &requests[0].body;
    assert_eq!(request["labels"], json!({"dbt_invocation_id": "abc-123"}));
    assert_eq!(request["jobTimeoutMs"], json!("90000"));
}

/// Repeated execution and lazy parameter readers retain the SQL set on their statement.
#[test]
fn unparameterized_statement_reuses_its_sql_across_executions() {
    let response = || {
        (
            200,
            json!({
                "jobReference": job_reference("fixture-project", "execution-job-11"),
                "jobComplete": true,
                "queryId": "short_query",
                "totalRows": "0"
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![response(), response()]);
    let mut statement = test_statement_at(&endpoint);
    let sql = "SELECT 21 + 21 AS answer";
    statement.set_sql_query(sql).unwrap();

    assert_eq!(statement.execute_update().unwrap(), None);
    assert_eq!(statement.execute_update().unwrap(), None);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.body["query"] == sql));
}

#[test]
fn empty_parameter_stream_uses_the_borrowed_sql_for_its_schema_request() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "statistics": {
                "query": {
                    "schema": {
                        "fields": [{"name": "answer", "type": "INTEGER"}]
                    }
                }
            },
            "status": {"state": "DONE"}
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    let sql = "SELECT ? AS answer";
    statement.set_sql_query(sql).unwrap();
    let parameter_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            std::iter::empty::<Result<RecordBatch, ArrowError>>(),
            parameter_schema,
        )))
        .unwrap();

    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(reader.schema().field(0).name(), "answer");
    assert!(reader.next().is_none());
    // Nothing executed, so the count is known, as execute_update and execute_partitions say.
    assert_eq!(affected, 0);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body["configuration"]["query"]["query"], sql);
    assert_eq!(requests[0].body["configuration"]["dryRun"], true);
}

#[test]
fn lazy_parameterized_reader_keeps_its_owned_sql() {
    let response = || {
        (
            200,
            json!({
                "jobReference": job_reference("fixture-project", "execution-job-12"),
                "statementType": "SELECT",
                "jobComplete": true,
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalRows": "0"
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![response(), response()]);
    let mut statement = test_statement_at(&endpoint);
    let sql = "SELECT ? AS answer";
    statement.set_sql_query(sql).unwrap();
    let retained_sql = Arc::downgrade(statement.sql.as_ref().unwrap());
    statement
        .bind(int64_column("value", vec![1_i64, 2]))
        .unwrap();

    let mut reader = statement.execute().unwrap();
    assert_eq!(retained_sql.strong_count(), 2);
    statement.set_sql_query("SELECT 99 AS replacement").unwrap();
    assert_eq!(retained_sql.strong_count(), 1);
    assert!(reader.next().is_none());
    assert_eq!(retained_sql.strong_count(), 0);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.body["query"] == sql));
}

/// Every bound row is a separate BigQuery request. The lazy rows after the first must retain the
/// statement-scoped defaults rather than falling back to the connection's billing project and
/// unset schema when the execute call has returned.
#[test]
fn statement_defaults_apply_to_every_parameter_row() {
    let response = || {
        (
            200,
            json!({
                "jobReference": job_reference("fixture-project", "execution-job-13"),
                "statementType": "SELECT",
                "jobComplete": true,
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalRows": "0"
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![response(), response()]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::DefaultProjectId,
        "statement-project",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::DefaultDatasetId,
        "statement_dataset",
    );
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement
        .bind(int64_column("value", vec![1_i64, 2]))
        .unwrap();

    let mut reader = statement.execute().unwrap();
    assert!(reader.next().is_none());

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(
            request.body["defaultDataset"],
            json!({
                "projectId": "statement-project",
                "datasetId": "statement_dataset",
            })
        );
    }
}

/// Check the serialized parameter values, types, and mode. Positional is the default;
/// `bigquery.query.parameter_mode` selects named parameters, which carry the bound column name.
#[test]
fn parameters_reach_the_request_as_bigquery_spells_them() {
    for (mode, sql, column, expected_mode, expected_parameters) in [
        (
            None,
            "SELECT ? AS answer",
            "value",
            "POSITIONAL",
            json!([{"parameterType": {"type": "INT64"}, "parameterValue": {"value": "7"}}]),
        ),
        (
            Some(ParameterMode::Named),
            "SELECT @answer AS answer",
            "answer",
            "NAMED",
            json!([{
                "name": "answer",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": "7"},
            }]),
        ),
    ] {
        let (endpoint, requests) = capture_requests(vec![(
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-14"), "jobComplete": true, "schema": {"fields": []}, "totalRows": "0"}),
        )]);
        let mut statement = test_statement_at(&endpoint);
        if let Some(mode) = mode {
            set_option(
                &mut statement,
                BigQueryStatementOption::ParameterMode,
                mode.as_str(),
            );
        }
        statement.set_sql_query(sql).unwrap();
        statement.bind(int64_column(column, vec![7])).unwrap();
        statement.execute_update().unwrap();

        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1, "{expected_mode}");
        assert_eq!(requests[0].body["parameterMode"], expected_mode);
        assert_eq!(
            requests[0].body["queryParameters"], expected_parameters,
            "{expected_mode}"
        );
    }
}

/// The completing response carries `totalRows`, so `AdbcStatementExecuteQuery` reports the
/// result size up front, as PEP 249's `cursor.rowcount` expects. A script counts its final
/// result; one ending in anything else reports -1, as does a count too large for `int64_t`.
#[test]
fn execute_reports_a_query_results_total_rows() {
    let result = |statement_type: &str, total_rows: Option<&str>, rows: usize| {
        let mut response = json!({
            "jobReference": job_reference("fixture-project", "execution-job-15"),
            "jobComplete": true,
            "statementType": statement_type,
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "rows": vec![json!({"f": [{"v": "42"}]}); rows],
        });
        if let Some(total_rows) = total_rows {
            response["totalRows"] = json!(total_rows);
        }
        (200, response)
    };
    let cases = [
        ("SELECT", Some("3"), 3, 3),
        ("SELECT", Some("0"), 0, 0),
        ("SCRIPT", Some("2"), 2, 2),
        ("SCRIPT", None, 0, -1),
        ("SELECT", Some("18446744073709551615"), 1, -1),
    ];
    let (endpoint, requests) = capture_requests(
        cases
            .iter()
            .map(|&(statement_type, total_rows, rows, _)| result(statement_type, total_rows, rows))
            .collect(),
    );
    let mut statement = test_statement_at(&endpoint);
    for (statement_type, total_rows, _, expected) in cases {
        statement.set_sql_query("SELECT 42 AS answer").unwrap();
        let (reader, affected) = statement.execute_with_rows_affected().unwrap();
        drop(reader);
        assert_eq!(
            affected, expected,
            "{statement_type} totalRows={total_rows:?}"
        );
    }
    requests.join().unwrap();
}

/// Later parameter rows of a lazily executed query run while the stream is drained, so their
/// total is not known when `AdbcStatementExecuteQuery` returns, even though the first row's is.
#[test]
fn multi_row_parameter_queries_report_an_unknown_row_count() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobReference": job_reference("fixture-project", "execution-job-16"),
            "jobComplete": true,
            "statementType": "SELECT",
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "1"}]}],
            "totalRows": "1"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement
        .bind(int64_column("value", vec![1_i64, 2_i64]))
        .unwrap();
    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    // Only the first row has run; dropping the reader runs no other.
    drop(reader);
    assert_eq!(requests.join().unwrap().len(), 1);
}

#[test]
fn bound_procedure_calls_return_each_result_in_parameter_order() {
    let (endpoint, requests) = capture_requests(
        [1, 2]
            .into_iter()
            .map(|value| {
                (
                    200,
                    json!({
                        "jobComplete": true,
                        "statementType": "CALL",
                        "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                        "rows": [{"f": [{"v": value.to_string()}]}],
                        "totalRows": "1"
                    }),
                )
            })
            .collect(),
    );
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("CALL dataset.echo(?)").unwrap();
    statement.bind(int64_column("value", vec![1, 2])).unwrap();

    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    let values: Vec<_> = reader
        .map(|batch| {
            batch
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect();
    assert_eq!(values, [1, 2]);
    assert_eq!(requests.join().unwrap().len(), 2);
}

/// A DB-API `cursor.execute(sql, parameters)` binds one row, which has run in full when
/// `AdbcStatementExecuteQuery` returns, so `cursor.rowcount` sees its count. Trailing empty
/// batches of a bound stream add no row.
#[test]
fn a_one_row_binding_reports_its_row_count() {
    let completed = |counts: serde_json::Value| {
        let mut response = json!({
            "jobReference": job_reference("fixture-project", "execution-job-17"),
            "jobComplete": true,
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
        });
        response
            .as_object_mut()
            .unwrap()
            .extend(counts.as_object().unwrap().clone());
        (200, response)
    };
    let (endpoint, requests) = capture_requests(vec![
        completed(json!({"statementType": "UPDATE", "numDmlAffectedRows": "3"})),
        completed(json!({"statementType": "SELECT", "totalRows": "0"})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("UPDATE dataset.table SET value = 1 WHERE id = ?")
        .unwrap();
    statement.bind(int64_column("id", vec![7])).unwrap();
    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    drop(reader);
    assert_eq!(affected, 3);

    statement.set_sql_query("SELECT ? AS answer").unwrap();
    let batch = int64_column("value", vec![7]);
    let schema = batch.schema();
    let empty = RecordBatch::new_empty(schema.clone());
    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(empty.clone()), Ok(batch), Ok(empty)],
            schema,
        )))
        .unwrap();
    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, 0);
    assert!(reader.next().is_none());
    assert_eq!(requests.join().unwrap().len(), 2);
}

/// A statement that returns no rows leaves a caller nothing to read, so DB-API code that executes
/// a multi-row `INSERT` and closes the cursor never drains the stream. Every row runs before
/// `AdbcStatementExecuteQuery` returns, and the counts sum; one unknown count makes the sum
/// unknown.
#[test]
fn a_multi_row_binding_of_dml_runs_every_row_before_execute_returns() {
    // BigQuery reports DML with its target table's schema, though it returns no rows.
    let dml = |statement_type: &str, count: Option<&str>| {
        let mut response = json!({
            "jobReference": job_reference("fixture-project", "execution-job-18"),
            "jobComplete": true,
            "statementType": statement_type,
            "schema": {"fields": [{"name": "a", "type": "INTEGER"}]},
        });
        if let Some(count) = count {
            response["numDmlAffectedRows"] = json!(count);
        }
        (200, response)
    };
    let (endpoint, requests) = capture_requests(vec![
        dml("INSERT", Some("1")),
        dml("INSERT", Some("2")),
        dml("INSERT", Some("0")),
        dml("CREATE_TABLE", None),
        dml("CREATE_TABLE", None),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("INSERT INTO dataset.table (a) VALUES (?)")
        .unwrap();
    statement.bind(int64_column("a", vec![1, 2, 3])).unwrap();
    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, 3);
    drop(reader);

    statement
        .set_sql_query("CREATE TABLE IF NOT EXISTS dataset.t AS SELECT ? AS a")
        .unwrap();
    statement.bind(int64_column("a", vec![1, 2])).unwrap();
    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    assert!(reader.next().is_none());

    let requests = requests.join().unwrap();
    let values = requests
        .iter()
        .map(|request| request.body["queryParameters"][0]["parameterValue"]["value"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [json!("1"), json!("2"), json!("3"), json!("1"), json!("2")]
    );
}

#[test]
fn an_unknown_first_dml_count_keeps_the_combined_count_unknown() {
    for counts in [[None, Some("3")], [Some("-1"), Some("3")]] {
        let responses = counts
            .into_iter()
            .map(|count| {
                let mut response = json!({
                    "jobComplete": true,
                    "statementType": "INSERT",
                });
                if let Some(count) = count {
                    response["numDmlAffectedRows"] = json!(count);
                }
                (200, response)
            })
            .collect();
        let (endpoint, requests) = capture_requests(responses);
        let mut statement = test_statement_at(&endpoint);
        statement
            .set_sql_query("INSERT dataset.table VALUES (?)")
            .unwrap();
        statement.bind(int64_column("value", vec![1, 2])).unwrap();

        let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
        assert_eq!(affected, -1, "counts: {counts:?}");
        assert!(reader.next().is_none());
        assert_eq!(requests.join().unwrap().len(), 2);
    }
}

/// A DML job that outlasts the `jobs.query` wait finishes through `jobs.getQueryResults`, which
/// never names a statement type, and the follow-up `jobs.get` may fail. The DML count still marks the
/// statement as one that returns no rows, so every bound row runs before execute returns.
#[test]
fn dml_without_a_statement_type_still_runs_every_row() {
    let dml = (
        200,
        json!({
            "jobComplete": true,
            "jobReference": job_reference("fixture-project", "dml-job"),
            "schema": {"fields": [{"name": "a", "type": "INTEGER"}]},
            "numDmlAffectedRows": "1",
        }),
    );
    let denied = (
        403,
        crate::test_support::http::error_body("accessDenied", "Access Denied"),
    );
    let (endpoint, server) = crate::test_support::http::capture_requests_until_stopped(
        [dml, denied].iter().cycle().take(6).cloned().collect(),
    );
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("INSERT INTO dataset.table (a) VALUES (?)")
        .unwrap();
    statement.bind(int64_column("a", vec![1, 2, 3])).unwrap();
    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    drop(reader);
    assert_eq!(affected, 3);
    let queries = server
        .stop()
        .iter()
        .filter(|request| request.body.get("query").is_some())
        .count();
    assert_eq!(queries, 3);
}

/// `ExecutePartitions` runs every job before it returns, so it reports the summed result sizes
/// and DML counts.
#[test]
fn partitions_report_the_summed_total_rows_of_their_jobs() {
    let completed = |job_id: &str, counts: serde_json::Value| {
        let mut response = json!({
            "jobComplete": true,
            "jobReference": job_reference("fixture-project", job_id),
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
        });
        response
            .as_object_mut()
            .unwrap()
            .extend(counts.as_object().unwrap().clone());
        (200, response)
    };
    let (endpoint, requests) = capture_requests(vec![
        completed("one", json!({"statementType": "SELECT", "totalRows": "4"})),
        completed("two", json!({"statementType": "SELECT", "totalRows": "4"})),
        completed(
            "three",
            json!({"statementType": "SELECT", "totalRows": "5"}),
        ),
        completed(
            "four",
            json!({"statementType": "INSERT", "numDmlAffectedRows": "2"}),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 42 AS answer").unwrap();
    assert_eq!(statement.execute_partitions().unwrap().rows_affected, 4);

    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement
        .bind(int64_column("value", vec![1_i64, 2_i64]))
        .unwrap();
    assert_eq!(statement.execute_partitions().unwrap().rows_affected, 9);

    statement
        .set_sql_query("INSERT dataset.table (answer) VALUES (1), (2)")
        .unwrap();
    let partitioned = statement.execute_partitions().unwrap();
    assert_eq!(partitioned.rows_affected, 2);
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 4);
    for request in requests {
        assert_eq!(request.body["jobCreationMode"], "JOB_CREATION_REQUIRED");
        assert_eq!(request.body["timeoutMs"], 10_000);
    }
}

#[test]
fn partition_row_counts_preserve_unknown_totals_and_reported_zero() {
    for (counts, expected) in [
        ([Some("4"), None], -1),
        ([None, Some("4")], -1),
        ([Some("9223372036854775808"), Some("4")], -1),
        ([Some("0"), Some("0")], 0),
    ] {
        let responses = counts
            .into_iter()
            .enumerate()
            .map(|(index, count)| {
                (
                    200,
                    json!({
                        "jobComplete": true,
                        "jobReference": job_reference("fixture-project", &format!("job-{index}")),
                        "statementType": "SELECT",
                        "schema": {"fields": []},
                        "totalRows": count,
                    }),
                )
            })
            .collect();
        let (endpoint, requests) = capture_requests(responses);
        let mut statement = test_statement_at(&endpoint);
        statement.set_sql_query("SELECT ? AS answer").unwrap();
        statement.bind(int64_column("value", vec![1, 2])).unwrap();

        let result = statement.execute_partitions().unwrap();
        assert_eq!(result.rows_affected, expected, "counts: {counts:?}");
        assert_eq!(result.partitions.len(), 2);
        assert_eq!(requests.join().unwrap().len(), 2);
    }
}

/// adbc.h: `AdbcStatementPrepare` "invalidates any prior result sets", exactly as executing one
/// does, so it supersedes whatever is still running on the statement.
#[test]
fn prepare_supersedes_a_running_statement_operation() {
    let mut statement = test_statement();
    statement.set_sql_query("SELECT 1").unwrap();
    let earlier = statement.cancellation.begin();
    statement.prepare().unwrap();
    assert_eq!(earlier.check().unwrap_err().status, Status::Cancelled);
}

/// A statement with no query is refused before anything is superseded: nothing was prepared, so
/// nothing is invalidated.
#[test]
fn prepare_without_a_query_is_refused_and_supersedes_nothing() {
    let mut statement = test_statement();
    let earlier = statement.cancellation.begin();
    assert_eq!(
        statement.prepare().unwrap_err().status,
        Status::InvalidState
    );
    earlier.check().unwrap();
}

#[test]
fn option_only_execute_schema_cancels_unread_parameter_rows() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobComplete": true,
            "statementType": "SELECT",
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "1"}]}],
            "totalRows": "1"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    let mut reader = statement.execute().unwrap();

    set_option(
        &mut statement,
        BigQueryStatementOption::GetDatasetLocation,
        r#"{"project":"data-project","dataset":"analytics"}"#,
    );
    assert_eq!(
        statement.execute_schema().unwrap().field(0).name(),
        "location"
    );

    let ArrowError::ExternalError(source) = reader.next().unwrap().unwrap_err() else {
        panic!("the invalidated reader must report cancellation");
    };
    assert_eq!(
        source.downcast_ref::<Error>().unwrap().status,
        Status::Cancelled
    );
    assert!(reader.next().is_none());
    assert_eq!(requests.join().unwrap().len(), 1);
}

#[test]
fn option_only_execute_schema_refusal_keeps_the_previous_operation() {
    let mut statement = test_statement();
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    let earlier = statement.cancellation.begin();
    assert_eq!(
        statement.execute_schema().unwrap_err().status,
        Status::InvalidState
    );
    earlier.check().unwrap();
}

/// The parameter schema costs one dry run per configuration, not one per call: a driver manager
/// that asks before every execute would otherwise double the latency of every query.
#[test]
fn the_parameter_schema_is_derived_once_and_dropped_when_the_query_changes() {
    let dry_run = || {
        (
            200,
            json!({
                "statistics": {
                    "query": {
                        "undeclaredQueryParameters": [
                            {"name": "answer", "parameterType": {"type": "INT64"}}
                        ]
                    }
                },
                "status": {"state": "DONE"}
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![dry_run(), dry_run(), dry_run()]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT @answer AS answer").unwrap();

    let schema = statement.get_parameter_schema().unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "answer");
    assert_eq!(statement.get_parameter_schema().unwrap(), schema);
    assert_eq!(statement.get_parameter_schema().unwrap(), schema);

    // A new query, and any option that could reach the dry run, drop the memo.
    statement.set_sql_query("SELECT @other AS answer").unwrap();
    statement.get_parameter_schema().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::ParameterMode,
        ParameterMode::Named.as_str(),
    );
    statement.get_parameter_schema().unwrap();

    assert_eq!(requests.join().unwrap().len(), 3);
}

#[test]
fn parameter_schema_cache_tracks_the_session_and_rejects_a_released_connection() {
    let dry_run = |data_type| {
        (
            200,
            json!({
                "statistics": {"query": {"undeclaredQueryParameters": [
                    {"name": "value", "parameterType": {"type": data_type}}
                ]}},
                "status": {"state": "DONE"}
            }),
        )
    };
    let (endpoint, requests) =
        capture_requests(vec![dry_run("INT64"), dry_run("STRING"), dry_run("INT64")]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("SELECT value FROM temporary_table WHERE value = @value")
        .unwrap();
    let outside = statement.get_parameter_schema().unwrap();

    statement.transaction.disable_autocommit().unwrap();
    let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap() else {
        panic!("expected a starting transaction")
    };
    start.started(Arc::from("parameter-session")).unwrap();
    let inside = statement.get_parameter_schema().unwrap();
    assert_ne!(inside, outside);
    assert_eq!(statement.get_parameter_schema().unwrap(), inside);

    let crate::transaction::EnableAutocommit::Active(_, finish) =
        statement.transaction.begin_enable_autocommit().unwrap()
    else {
        panic!("expected an active transaction")
    };
    finish.succeeded().unwrap();
    assert_eq!(statement.get_parameter_schema().unwrap(), outside);

    statement.transaction.disable_autocommit().unwrap();
    statement.transaction.release();
    assert_eq!(
        statement.get_parameter_schema().unwrap_err().status,
        Status::InvalidState
    );

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    for (request, session) in requests.iter().zip([false, true, false]) {
        let properties = &request.body["configuration"]["query"]["connectionProperties"];
        if session {
            assert_eq!(
                properties,
                &json!([{"key": "session_id", "value": "parameter-session"}])
            );
        } else {
            assert!(properties.is_null());
        }
    }
}

#[test]
fn cached_parameter_schema_does_not_bypass_create_session_refusal() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({"statistics": {"query": {}}, "status": {"state": "DONE"}}),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 1").unwrap();
    set_option(&mut statement, BigQueryStatementOption::CreateSession, true);
    statement.get_parameter_schema().unwrap();
    statement.transaction.disable_autocommit().unwrap();
    let failure = statement.get_parameter_schema().unwrap_err();
    assert_eq!(failure.status, Status::InvalidState);
    assert!(failure.message.contains("create_session"));
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// A bound parameter set is consumed by the execute that runs it, as in the sqlite reference
/// driver; the next execute must refuse rather than send the placeholders undeclared.
#[test]
fn re_executing_consumed_parameters_is_refused_by_every_execute_entrypoint() {
    let response = || {
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-19"), "statementType": "SELECT", "jobComplete": true, "schema": {"fields": []}, "totalRows": "0"}),
        )
    };
    let parameters = || int64_column("value", vec![7_i64]);

    let (endpoint, requests) = capture_requests(std::iter::repeat_with(response).take(6).collect());
    let mut connection = test_connection_at(&endpoint);
    let bound = |statement: &mut BigQueryStatement| {
        statement.set_sql_query("SELECT ? AS answer").unwrap();
        statement.bind(parameters()).unwrap();
    };

    let mut statement = connection.new_statement().unwrap();
    bound(&mut statement);
    statement.execute_update().unwrap();
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["consumed by the previous execute"]);
    // Binding again makes the statement executable again.
    statement.bind(parameters()).unwrap();
    statement.execute_update().unwrap();

    let mut statement = connection.new_statement().unwrap();
    bound(&mut statement);
    drop(statement.execute().unwrap());
    assert_eq!(
        statement.execute().err().unwrap().status,
        Status::InvalidState
    );

    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement.bind(parameters()).unwrap();
    statement.execute_partitions().unwrap();
    assert_eq!(
        statement.execute_partitions().unwrap_err().status,
        Status::InvalidState
    );

    // An unparameterized statement is unaffected: it may be executed as often as asked.
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT 1").unwrap();
    statement.execute_update().unwrap();
    statement.execute_update().unwrap();
    // Six requests for six executes that were allowed to run; the four refusals sent nothing.
    assert_eq!(requests.join().unwrap().len(), 6);
}

/// Setting new SQL clears the previous execute's consumed binding.
#[test]
fn a_new_query_is_executable_after_a_parameterized_one() {
    let response = || {
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-20"), "jobComplete": true, "schema": {"fields": []}, "totalRows": "0"}),
        )
    };
    let (endpoint, requests) = capture_requests(std::iter::repeat_with(response).take(2).collect());
    let mut connection = test_connection_at(&endpoint);

    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("INSERT INTO t VALUES (?)").unwrap();
    statement.bind(int64_column("value", vec![7_i64])).unwrap();
    statement.execute_update().unwrap();

    statement.set_sql_query("SELECT value FROM t").unwrap();
    statement.execute_update().unwrap();

    assert_eq!(requests.join().unwrap().len(), 2);
}

#[test]
fn cancelled_result_reader_returns_cancelled_once() {
    let cancellation = Cancellation::new(
        new_runtime().unwrap(),
        crate::clients::ClientState::for_test(),
        "statement",
    );
    let operation = cancellation.begin();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let inner = Box::new(RecordBatchIterator::new([Ok(batch)], schema));
    let mut reader = operation_reader(inner, operation);

    OperationCancelHandle(cancellation.clone())
        .try_cancel()
        .unwrap();
    let ArrowError::ExternalError(source) = reader.next().unwrap().unwrap_err() else {
        panic!("expected an external ADBC cancellation error");
    };
    assert_eq!(
        source.downcast_ref::<Error>().unwrap().status,
        Status::Cancelled
    );
    assert!(reader.next().is_none());
    assert_eq!(
        OperationCancelHandle(cancellation)
            .try_cancel()
            .unwrap_err()
            .status,
        Status::InvalidState
    );
}

/// A cancel landing while a result is still being fetched has to report itself.
///
/// The job is complete, so nothing is cancelled remotely, but draining still fetches further
/// `jobs.getQueryResults` pages. Ending the stream quietly would pass a partial result off as a
/// whole one.
#[test]
fn cancelling_a_multi_page_drain_reports_the_cancellation() {
    let (release, released) = std::sync::mpsc::channel::<()>();
    // Wait until the server receives the page request before cancelling. A connection alone
    // is insufficient: reqwest can be cancelled between connecting and writing the request.
    let (reached, reached_by_the_server) = std::sync::mpsc::channel::<()>();
    let mut page = 0_usize;
    let (endpoint, requests) = capture_requests_responding(2, move |_request| {
        page += 1;
        if page == 1 {
            return Some((
                200,
                json!({
                    "jobComplete": true,
                    "jobReference": job_reference("fixture-project", "paged-job"),
                    "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                    "rows": [{"f": [{"v": "1"}]}],
                    "pageToken": "second-page",
                    // Complete statistics, so the driver needs no best-effort `jobs.get` and the
                    // held request below is the result page itself.
                    "statementType": "SELECT",
                    "totalBytesBilled": "0",
                    "totalSlotMs": "0"
                }),
            ));
        }
        // The next result page has reached the server and is held until the test has cancelled,
        // so the drain is provably still in flight when the cancellation lands, and is then
        // dropped unanswered.
        reached.send(()).ok();
        released.recv().ok();
        None
    });
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        "true",
    );
    statement
        .set_sql_query("SELECT value FROM dataset.table")
        .unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

    // The prefetched second page is now on the wire and held, so the cancel below cannot race
    // the connect.
    reached_by_the_server
        .recv()
        .expect("the prefetched second page request reached the server");
    statement.get_cancel_handle().try_cancel().unwrap();
    release.send(()).ok();

    let failure = reader
        .next()
        .expect("a cancelled drain reports the cancellation instead of ending")
        .unwrap_err();
    assert!(failure.to_string().contains("cancelled"), "{failure}");
    drop(reader);
    assert_eq!(requests.join().unwrap().len(), 2);
}

/// adbc.h lets only ExecuteQuery, ExecuteSchema and Prepare invalidate prior result sets
/// (adbc.h:2003/2024/2045), so the parameter-schema dry run must not cancel the lazy reader of a
/// parameterized execute, which runs the remaining parameter sets as it is drained.
#[test]
fn getting_the_parameter_schema_leaves_a_draining_result_alive() {
    let result = || {
        (
            200,
            json!({
                "jobReference": job_reference("fixture-project", "execution-job-21"),
                "statementType": "SELECT",
                "jobComplete": true,
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "7"}]}],
                "totalRows": "1"
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![
        result(),
        (
            200,
            json!({
                "statistics": {
                    "query": {
                        "undeclaredQueryParameters": [
                            {"name": "", "parameterType": {"type": "INT64"}}
                        ]
                    }
                },
                "status": {"state": "DONE"}
            }),
        ),
        result(),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement
        .bind(int64_column("value", vec![1_i64, 2]))
        .unwrap();

    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

    assert_eq!(statement.get_parameter_schema().unwrap().fields().len(), 1);

    // The second parameter set still runs, and the stream still ends of its own accord.
    assert_eq!(
        reader
            .next()
            .expect("the second parameter set still runs")
            .unwrap()
            .num_rows(),
        1
    );
    assert!(reader.next().is_none());
    assert_eq!(requests.join().unwrap().len(), 3);
}

/// Describing a statement needs no values, so one that was never bound, or whose binding an earlier
/// execute consumed, is still describable. Its placeholders reach the dry run undeclared under the
/// statement's parameter mode, which BigQuery types; without the mode it refuses `?`.
#[test]
fn describing_an_unbound_statement_leaves_its_placeholders_undeclared() {
    let described = || {
        (
            200,
            json!({
                "statistics": {"query": {"statementType": "SELECT", "schema": {"fields": [
                    {"name": "answer", "type": "INTEGER"}
                ]}}},
                "status": {"state": "DONE"}
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![
        described(),
        (
            200,
            json!({"jobReference": job_reference("fixture-project", "execution-job-22"), "jobComplete": true, "schema": {"fields": []}, "totalRows": "0"}),
        ),
        described(),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT ? AS answer").unwrap();
    assert_eq!(
        statement.execute_schema().unwrap().field(0).name(),
        "answer"
    );
    statement.bind(int64_column("value", vec![7_i64])).unwrap();
    statement.execute_update().unwrap();
    assert_eq!(
        statement.execute_schema().unwrap().field(0).name(),
        "answer"
    );

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    for request in [&requests[0], &requests[2]] {
        let query = &request.body["configuration"]["query"];
        assert_eq!(request.body["configuration"]["dryRun"], true);
        assert_eq!(query["parameterMode"], json!("POSITIONAL"));
        assert!(
            query["queryParameters"]
                .as_array()
                .is_none_or(Vec::is_empty),
            "{query}"
        );
    }
}
