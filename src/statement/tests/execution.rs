//! Running a statement: the execute entry points, bound parameters and the wire form they
//! take, prepare, and the cancellation a superseded result reports.

use super::*;
use crate::test_support::driver::unlocated_connection_at;
use crate::test_support::http::{capture_requests_until_stopped, error_body};

/// A staged finished statement of `statement_type`, reporting `count` as its affected rows.
/// BigQuery reports DML with its target table's schema, though it returns no rows.
fn dml_answer(statement_type: &str, count: Option<&str>) -> (u16, serde_json::Value) {
    let mut fields = json!({"statementType": statement_type, "schema": integer_schema("a")});
    if let Some(count) = count {
        fields["numDmlAffectedRows"] = json!(count);
    }
    completed_job("dml-job", fields)
}

/// Serves `count` requests of destination-table query jobs: each `jobs.insert` is answered done,
/// and each results poll with `results`.
fn capture_destination_jobs(count: usize, results: serde_json::Value) -> (String, CapturedServer) {
    let mut job_reference = serde_json::Value::Null;
    capture_requests_responding(count, move |request| {
        if request.body.get("jobReference").is_some() {
            job_reference = request.body["jobReference"].clone();
            return Some(ok(json!({
                "jobReference": job_reference,
                "statistics": {"query": {"statementType": "SELECT"}},
                "status": {"state": "DONE"}
            })));
        }
        let mut answer = results.clone();
        answer["jobComplete"] = json!(true);
        answer["jobReference"] = job_reference.clone();
        Some(ok(answer))
    })
}

#[test]
fn reservation_reaches_both_submission_paths_and_clearing_omits_it() {
    for dry_run in [false, true] {
        let response = if dry_run {
            dry_run_answer(json!({"statementType": "DELETE"}))
        } else {
            completed_job("job", json!({}))
        };
        let (endpoint, requests) = capture_requests(vec![response; 4]);
        let mut statement = test_query_at(&endpoint, "DELETE FROM dataset.table WHERE false");
        set_option(&mut statement, BqOption::DryRun, dry_run);
        // Exercise the default as well as clearing on the same reusable statement.
        statement.execute_update().unwrap();
        let reservation = "projects/billing-project/locations/EU/reservations/analytics";
        for value in [reservation, "none", ""] {
            set_option(&mut statement, BqOption::Reservation, value);
            statement.execute_update().unwrap();
        }
        let requests = requests.served(4);
        let expected = [None, Some(reservation), Some("none"), None];
        for (request, expected) in requests.iter().zip(expected) {
            let (path, configuration) = if dry_run {
                assert!(request.body.get("reservation").is_none());
                ("/jobs", &request.body["configuration"])
            } else {
                ("/queries", &request.body)
            };
            assert!(request.path().ends_with(path));
            assert!(configuration["query"].get("reservation").is_none());
            assert_eq!(
                configuration
                    .get("reservation")
                    .and_then(serde_json::Value::as_str),
                expected
            );
        }
    }
}

#[test]
fn failed_update_poll_cancels_the_unfinished_remote_job() {
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({
            "jobComplete": false,
            "jobReference": {"projectId":"fixture-project", "jobId":"running-job", "location":"EU"}
        })),
        (403, error_body("accessDenied", "poll denied")),
        ok(json!({})),
    ]);
    let mut statement = test_query_at(&endpoint, "DELETE FROM dataset.table WHERE true");

    let failure = statement.execute_update().unwrap_err();
    assert_failure(&failure, Status::Unauthorized, &["poll denied"]);
    let requests = requests.served(3);
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
            responses.push(ok(json!({"jobComplete":false, "jobReference":reference})));
        }
        responses.push(ok(json!({"jobComplete":true, "jobReference":reference})));
        let (endpoint, requests) = capture_requests(responses);
        let mut statement = test_query_at(&endpoint, "DELETE FROM dataset.table WHERE true");
        let reader = statement.execute().unwrap();
        let requests = requests.join().unwrap();

        // The server has closed. A cancel targeting the finished job would fail here.
        statement.get_cancel_handle().try_cancel().unwrap();
        drop(reader);
        assert_eq!(requests.len(), if complete_on_submit { 1 } else { 2 });
    }
}

/// A dry run creates no job, and partitions are job references, so the combination is rejected
/// instead of reporting success with zero partitions. Nothing is read, so a binding stays.
#[test]
fn dry_run_partitions_are_rejected_before_any_request_and_keep_the_binding() {
    for values in [None, Some(Vec::new()), Some(vec![42])] {
        let mut statement = test_statement();
        statement.set_sql_query("SELECT ? AS value").unwrap();
        let bound = values.is_some();
        if let Some(values) = values {
            statement.bind(int64_column("value", values)).unwrap();
        }
        set_option(&mut statement, BqOption::DryRun, "true");

        let failure = statement.execute_partitions().unwrap_err();
        assert_failure(&failure, Status::InvalidState, &["bigquery.query.dry_run"]);
        assert_eq!(statement.parameters.schema().is_some(), bound);
    }
}

/// A dry run executes nothing, so it types bound parameters from their schema and leaves the
/// binding for the execution it validates, as an ingest dry run keeps its data.
#[test]
fn a_dry_run_leaves_bound_parameters_for_the_next_execute() {
    for update in [false, true] {
        let (endpoint, requests) = capture_requests(vec![
            dry_run_answer(json!({"statementType": "SELECT", "schema": integer_schema("answer")})),
            select_answer(&["7"]),
        ]);
        let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
        statement.bind(int64_column("value", vec![7])).unwrap();

        set_option(&mut statement, BqOption::DryRun, "true");
        if update {
            assert_eq!(statement.execute_update().unwrap(), None);
        } else {
            let (reader, affected) = statement.execute_with_rows_affected().unwrap();
            assert_eq!(affected, -1);
            assert_eq!(reader.schema().field(0).name(), "answer");
        }

        set_option(&mut statement, BqOption::DryRun, "false");
        if update {
            statement.execute_update().unwrap();
        } else {
            assert_eq!(row_count(statement.execute().unwrap()), 1);
        }

        let requests = requests.served(2);
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
        let (endpoint, requests) =
            capture_requests(vec![
                dry_run_answer(json!({"statementType": statement_type}));
                2
            ]);
        let mut statement = test_query_at(&endpoint, "DROP TABLE IF EXISTS dataset.table");
        assert!(statement.execute_schema().unwrap().fields().is_empty());

        set_option(&mut statement, BqOption::DryRun, "true");
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
        let (endpoint, requests) = capture_requests(vec![dry_run_answer(query)]);
        let mut statement = test_query_at(&endpoint, "SELECT 1");
        let failure = statement.execute_schema().unwrap_err();
        assert_failure(&failure, Status::InvalidData, &["without a result schema"]);
        requests.join().unwrap();
    }
}

#[test]
fn parameterless_queries_have_an_empty_parameter_schema() {
    let (endpoint, requests) =
        capture_requests(vec![dry_run_answer(json!({"statementType": "SELECT"}))]);
    let mut statement = test_query_at(&endpoint, "SELECT 1");
    statement.prepare().unwrap();
    for _ in 0..2 {
        let schema = statement.get_parameter_schema().unwrap();
        assert!(schema.fields().is_empty());
    }
    requests.served(1);
}

#[test]
fn parameter_discovery_rejects_missing_query_statistics() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({
        "statistics": {}, "status": {"state": "DONE"}
    }))]);
    let statement = test_query_at(&endpoint, "SELECT 1");
    let failure = statement.get_parameter_schema().unwrap_err();
    assert_failure(&failure, Status::InvalidData, &["without query statistics"]);
    requests.join().unwrap();
}

/// One row's missing or negative count makes the combined count unknown rather than a partial
/// sum, through either execute and as in `execute_partitions`.
#[test]
fn an_unknown_dml_count_keeps_the_combined_count_unknown() {
    for counts in [
        [Some("3"), None],
        [None, Some("3")],
        [Some("-1"), Some("3")],
    ] {
        for update in [false, true] {
            let responses = counts
                .into_iter()
                .map(|count| dml_answer("INSERT", count))
                .collect();
            let (endpoint, requests) = capture_requests(responses);
            let mut statement = test_query_at(&endpoint, "INSERT dataset.table VALUES (?)");
            statement.bind(int64_column("value", vec![1, 2])).unwrap();

            if update {
                assert_eq!(statement.execute_update().unwrap(), None, "{counts:?}");
            } else {
                let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
                assert_eq!(affected, -1, "{counts:?}");
                assert!(reader.next().is_none());
            }
            let requests = requests.served(2);
            let max_results = if update { 0 } else { 100_000 };
            assert!(
                requests
                    .iter()
                    .all(|request| request.body["maxResults"] == max_results)
            );
        }
    }
}

/// The completing response carries `numDmlAffectedRows`, so DML run through
/// `AdbcStatementExecuteQuery` reports the same count as `AdbcStatementExecuteUpdate`, which
/// Python's `cursor.rowcount` relies on. DML that matched nothing reports 0; DDL reports -1. A
/// script ending in DML is answered with neither count (verified live), so it reports -1 too.
#[test]
fn dml_through_execute_query_reports_its_affected_rows() {
    let cases = [
        ("DELETE", Some("7"), 7),
        ("UPDATE", Some("0"), 0),
        ("CREATE_TABLE", None, -1),
        ("SCRIPT", None, -1),
    ];
    let (endpoint, requests) = capture_requests(
        cases
            .iter()
            .map(|(statement_type, count, _)| dml_answer(statement_type, *count))
            .collect(),
    );
    let mut statement = test_query_at(&endpoint, "DELETE FROM dataset.table WHERE true");
    for (statement_type, _, expected) in cases {
        let (_, rows_affected) = statement.execute_with_rows_affected().unwrap();
        assert_eq!(rows_affected, expected, "{statement_type}");
    }
    requests.join().unwrap();
}

#[test]
fn explicit_destination_execute_query_requests_metadata_without_rows() {
    let (endpoint, requests) = capture_destination_jobs(
        2,
        json!({
            "schema": integer_schema("value"),
            "rows": [{"f": [{"v": "7"}]}],
            "pageToken": "must-not-be-fetched",
            // The destination table's size, not the rows this query wrote.
            "totalRows": "6"
        }),
    );
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BqOption::DestinationTable,
        "fixture-project.data.result",
    );
    statement.set_sql_query("SELECT 7 AS value").unwrap();

    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert!(reader.next().is_none());
    // `totalRows` counts the whole destination table (observed, not documented), so the number
    // of rows this query wrote is unknown.
    assert_eq!(affected, -1);

    let requests = requests.served(2);
    assert_eq!(
        requests[0].body["configuration"]["query"]["destinationTable"]["tableId"],
        "result"
    );
    assert!(requests[1].path().contains("maxResults=0"));
}

#[test]
fn an_explicit_destination_runs_every_bound_row_before_execute_returns() {
    let (endpoint, requests) = capture_destination_jobs(
        6,
        json!({"schema": integer_schema("value"), "totalRows": "100"}),
    );
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BqOption::DestinationTable,
        "fixture-project.data.result",
    );
    set_option(
        &mut statement,
        BqOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    statement.set_sql_query("SELECT ? AS value").unwrap();
    statement
        .bind(int64_column("value", vec![1, 2, 3]))
        .unwrap();

    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    // Destination writes return no result rows, so callers need not drain their stream: the mock
    // has served every request while the reader is still undrained.
    let requests = requests.join().unwrap();
    drop(reader);

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

/// dbt 2.0 sends a profile's `priority: batch` on every statement, and a profile need not
/// set `location`. The job runs where BigQuery infers, and its rows are read from there.
#[test]
fn a_batch_query_without_a_location_reads_its_rows_where_bigquery_ran_it() {
    let mut job_reference = serde_json::Value::Null;
    let (endpoint, requests) = capture_requests_responding(2, move |request| {
        if job_reference.is_null() {
            job_reference = request.body["jobReference"].clone();
            job_reference["location"] = json!("europe-west2");
            return Some(ok(json!({
                "jobReference": job_reference, "status": {"state": "PENDING"}
            })));
        }
        Some(ok(json!({
            "jobComplete": true,
            "jobReference": job_reference,
            "schema": integer_schema("value"),
            "rows": [{"f": [{"v": "7"}]}],
            "totalRows": "1"
        })))
    });
    let mut statement = unlocated_connection_at(&endpoint).new_statement().unwrap();
    set_option(
        &mut statement,
        "adbc.bigquery.sql.query.priority",
        BigQueryQueryPriority::Batch,
    );
    statement.set_sql_query("SELECT 7 AS value").unwrap();

    assert_eq!(row_count(statement.execute().unwrap()), 1);

    let requests = requests.served(2);
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
    let (endpoint, requests) = capture_requests(vec![completed_job("job", json!({}))]);
    let mut statement = test_query_at(&endpoint, "SELECT 1");
    for (option, value) in [
        (
            BqOption::CreateDisposition,
            OptionValue::from(BigQueryCreateDisposition::CreateIfNeeded),
        ),
        (
            BqOption::WriteDisposition,
            BigQueryWriteDisposition::WriteEmpty.into(),
        ),
        (
            BqOption::Priority,
            BigQueryQueryPriority::Interactive.into(),
        ),
        (BqOption::MaximumBytesBilled, 0_i64.into()),
        (BqOption::JobTimeout, 0_i64.into()),
        (BqOption::CreateSession, false.into()),
        (BqOption::DisableQueryCache, false.into()),
        (BqOption::DryRun, false.into()),
    ] {
        set_option(&mut statement, option, value);
    }
    statement.execute_update().unwrap();

    let requests = requests.served(1);
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
    let (endpoint, requests) = capture_requests(vec![completed_job("job", json!({}))]);
    let mut statement = test_query_at(&endpoint, "SELECT 1");
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
    statement.execute_update().unwrap();

    let requests = requests.served(1);
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

#[test]
fn an_empty_parameter_stream_is_described_by_a_dry_run_typed_from_its_schema() {
    let (endpoint, requests) = capture_requests(vec![dry_run_answer(json!({
        "schema": integer_schema("answer")
    }))]);
    let sql = "SELECT ? AS answer";
    let mut statement = test_query_at(&endpoint, sql);
    bind_no_rows(&mut statement);

    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(reader.schema().field(0).name(), "answer");
    assert!(reader.next().is_none());
    // Nothing executed, so the count is known, as execute_update and execute_partitions say.
    assert_eq!(affected, 0);

    let requests = requests.served(1);
    let configuration = &requests[0].body["configuration"];
    assert_eq!(configuration["query"]["query"], sql);
    assert_eq!(configuration["dryRun"], true);
    assert_eq!(
        configuration["query"]["queryParameters"][0]["parameterType"]["type"],
        "INT64"
    );
}

/// Every bound row is a separate request. The lazy rows after the first keep the SQL and the
/// statement-scoped defaults they began with rather than falling back to the connection's, and
/// the reader holds that SQL only until it ends.
#[test]
fn lazy_parameter_rows_keep_their_sql_and_statement_defaults() {
    let (endpoint, requests) = capture_requests(vec![select_answer(&[]); 2]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BqOption::DefaultProjectId,
        "statement-project",
    );
    set_option(
        &mut statement,
        BqOption::DefaultDatasetId,
        "statement_dataset",
    );
    let sql = "SELECT ? AS answer";
    statement.set_sql_query(sql).unwrap();
    let retained_sql = Arc::downgrade(statement.sql.as_ref().unwrap());
    statement.bind(int64_column("value", vec![1, 2])).unwrap();

    let mut reader = statement.execute().unwrap();
    assert_eq!(retained_sql.strong_count(), 2);
    statement.set_sql_query("SELECT 99 AS replacement").unwrap();
    assert_eq!(retained_sql.strong_count(), 1);
    assert!(reader.next().is_none());
    assert_eq!(retained_sql.strong_count(), 0);

    let requests = requests.served(2);
    for request in requests {
        assert_eq!(request.body["query"], sql);
        assert_eq!(
            request.body["defaultDataset"],
            json!({"projectId": "statement-project", "datasetId": "statement_dataset"})
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
        let (endpoint, requests) = capture_requests(vec![completed_job("job", json!({}))]);
        let mut statement = test_query_at(&endpoint, sql);
        if let Some(mode) = mode {
            set_option(&mut statement, BqOption::ParameterMode, mode.as_str());
        }
        statement.bind(int64_column(column, vec![7])).unwrap();
        statement.execute_update().unwrap();

        let requests = requests.served(1);
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
            .map(|&(statement_type, total_rows, rows, _)| {
                let mut fields = json!({
                    "statementType": statement_type,
                    "schema": integer_schema("answer"),
                    "rows": vec![json!({"f": [{"v": "42"}]}); rows],
                });
                if let Some(total_rows) = total_rows {
                    fields["totalRows"] = json!(total_rows);
                }
                completed_job("job", fields)
            })
            .collect(),
    );
    let mut statement = test_query_at(&endpoint, "SELECT 42 AS answer");
    for (statement_type, total_rows, _, expected) in cases {
        let (_, affected) = statement.execute_with_rows_affected().unwrap();
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
    let (endpoint, requests) = capture_requests(vec![select_answer(&["1"])]);
    let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    let (reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, -1);
    // Only the first row has run; dropping the reader runs no other.
    drop(reader);
    requests.served(1);
}

#[test]
fn bound_procedure_calls_return_each_result_in_parameter_order() {
    let call = |value: &str| {
        ok(json!({
            "jobComplete": true,
            "statementType": "CALL",
            "schema": integer_schema("answer"),
            "rows": [{"f": [{"v": value}]}],
            "totalRows": "1"
        }))
    };
    let (endpoint, requests) = capture_requests(vec![call("1"), call("2")]);
    let mut statement = test_query_at(&endpoint, "CALL dataset.echo(?)");
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
    requests.served(2);
}

/// A DB-API `cursor.execute(sql, parameters)` binds one row, which has run in full when
/// `AdbcStatementExecuteQuery` returns, so `cursor.rowcount` sees its count. Trailing empty
/// batches of a bound stream add no row.
#[test]
fn a_one_row_binding_reports_its_row_count() {
    let (endpoint, requests) =
        capture_requests(vec![dml_answer("UPDATE", Some("3")), select_answer(&[])]);
    let mut statement = test_query_at(&endpoint, "UPDATE dataset.table SET value = 1 WHERE id = ?");
    statement.bind(int64_column("id", vec![7])).unwrap();
    let (_, affected) = statement.execute_with_rows_affected().unwrap();
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
    requests.served(2);
}

/// A statement that returns no rows leaves a caller nothing to read, so DB-API code that executes
/// a multi-row `INSERT` and closes the cursor never drains the stream. Every row runs before
/// `AdbcStatementExecuteQuery` returns, and the counts sum; one unknown count makes the sum
/// unknown.
#[test]
fn a_multi_row_binding_of_dml_runs_every_row_before_execute_returns() {
    let (endpoint, requests) = capture_requests(vec![
        dml_answer("INSERT", Some("1")),
        dml_answer("INSERT", Some("2")),
        dml_answer("INSERT", Some("0")),
        dml_answer("CREATE_TABLE", None),
        dml_answer("CREATE_TABLE", None),
    ]);
    let mut statement = test_query_at(&endpoint, "INSERT INTO dataset.table (a) VALUES (?)");
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

/// A DML job that outlasts the `jobs.query` wait finishes through `jobs.getQueryResults`, which
/// never names a statement type, and the follow-up `jobs.get` may fail. The DML count still marks the
/// statement as one that returns no rows, so every bound row runs before execute returns.
#[test]
fn dml_without_a_statement_type_still_runs_every_row() {
    let dml = completed_job(
        "dml-job",
        json!({"schema": integer_schema("a"), "numDmlAffectedRows": "1"}),
    );
    let denied = (403, error_body("accessDenied", "Access Denied"));
    let (endpoint, server) =
        capture_requests_until_stopped([dml, denied].iter().cycle().take(6).cloned().collect());
    let mut statement = test_query_at(&endpoint, "INSERT INTO dataset.table (a) VALUES (?)");
    statement.bind(int64_column("a", vec![1, 2, 3])).unwrap();
    let (_, affected) = statement.execute_with_rows_affected().unwrap();
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
    let select = |job_id: &str, total_rows: &str| {
        completed_job(
            job_id,
            json!({
                "statementType": "SELECT",
                "schema": integer_schema("answer"),
                "totalRows": total_rows
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![
        select("one", "4"),
        select("two", "4"),
        select("three", "5"),
        dml_answer("INSERT", Some("2")),
    ]);
    let mut statement = test_query_at(&endpoint, "SELECT 42 AS answer");
    assert_eq!(statement.execute_partitions().unwrap().rows_affected, 4);

    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    assert_eq!(statement.execute_partitions().unwrap().rows_affected, 9);

    statement
        .set_sql_query("INSERT dataset.table (answer) VALUES (1), (2)")
        .unwrap();
    assert_eq!(statement.execute_partitions().unwrap().rows_affected, 2);
    let requests = requests.served(4);
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
                completed_job(
                    &format!("job-{index}"),
                    json!({"statementType": "SELECT", "schema": {"fields": []}, "totalRows": count}),
                )
            })
            .collect();
        let (endpoint, requests) = capture_requests(responses);
        let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
        statement.bind(int64_column("value", vec![1, 2])).unwrap();

        let result = statement.execute_partitions().unwrap();
        assert_eq!(result.rows_affected, expected, "counts: {counts:?}");
        assert_eq!(result.partitions.len(), 2);
        requests.served(2);
    }
}

/// adbc.h: `AdbcStatementPrepare` "invalidates any prior result sets", exactly as executing one
/// does, so it supersedes whatever is still running on the statement. A statement with no query
/// is refused first: nothing was prepared, so nothing is invalidated.
#[test]
fn prepare_supersedes_a_running_operation_unless_it_is_refused() {
    let mut statement = test_statement();
    let earlier = statement.cancellation.begin();
    assert_eq!(
        statement.prepare().unwrap_err().status,
        Status::InvalidState
    );
    earlier.check().unwrap();

    statement.set_sql_query("SELECT 1").unwrap();
    statement.prepare().unwrap();
    assert_eq!(earlier.check().unwrap_err().status, Status::Cancelled);
}

#[test]
fn option_only_execute_schema_cancels_unread_parameter_rows() {
    let (endpoint, requests) = capture_requests(vec![select_answer(&["1"])]);
    let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    let mut reader = statement.execute().unwrap();

    set_option(
        &mut statement,
        BqOption::GetDatasetLocation,
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
    requests.served(1);
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
    let (endpoint, requests) =
        capture_requests(vec![
            dry_run_answer(json!({"undeclaredQueryParameters": [
                {"name": "answer", "parameterType": {"type": "INT64"}}
            ]}));
            3
        ]);
    let mut statement = test_query_at(&endpoint, "SELECT @answer AS answer");

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
        BqOption::ParameterMode,
        ParameterMode::Named.as_str(),
    );
    statement.get_parameter_schema().unwrap();

    requests.served(3);
}

#[test]
fn parameter_schema_cache_tracks_the_session_and_rejects_a_released_connection() {
    let inferred = |data_type| {
        dry_run_answer(json!({"undeclaredQueryParameters": [
            {"name": "value", "parameterType": {"type": data_type}}
        ]}))
    };
    let (endpoint, requests) = capture_requests(vec![
        inferred("INT64"),
        inferred("STRING"),
        inferred("INT64"),
    ]);
    let statement = test_query_at(
        &endpoint,
        "SELECT value FROM temporary_table WHERE value = @value",
    );
    let outside = statement.get_parameter_schema().unwrap();

    statement.transaction.disable_autocommit().unwrap();
    start_transaction(&statement, "parameter-session");
    let inside = statement.get_parameter_schema().unwrap();
    assert_ne!(inside, outside);
    assert_eq!(statement.get_parameter_schema().unwrap(), inside);

    let crate::transaction::EnableAutocommit::Active(_, finish) =
        statement.transaction.begin_enable_autocommit().unwrap()
    else {
        panic!("expected an active transaction")
    };
    finish.autocommit().unwrap();
    assert_eq!(statement.get_parameter_schema().unwrap(), outside);

    statement.transaction.disable_autocommit().unwrap();
    statement.transaction.release();
    assert_eq!(
        statement.get_parameter_schema().unwrap_err().status,
        Status::InvalidState
    );

    let requests = requests.served(3);
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
    let (endpoint, requests) = capture_requests(vec![dry_run_answer(json!({}))]);
    let mut statement = test_query_at(&endpoint, "SELECT 1");
    set_option(&mut statement, BqOption::CreateSession, true);
    statement.get_parameter_schema().unwrap();
    statement.transaction.disable_autocommit().unwrap();
    let failure = statement.get_parameter_schema().unwrap_err();
    assert_failure(&failure, Status::InvalidState, &["create_session"]);
    requests.served(1);
}

/// A bound parameter set is consumed by the execute that runs it, as in the sqlite reference
/// driver; the next execute must refuse rather than send the placeholders undeclared. Binding
/// again or setting new SQL makes the statement executable, and an unparameterized one may run as
/// often as asked.
#[test]
fn re_executing_consumed_parameters_is_refused_by_every_execute_entrypoint() {
    let (endpoint, requests) = capture_requests(vec![select_answer(&[]); 12]);
    let mut connection = test_connection_at(&endpoint);
    let parameters = || int64_column("value", vec![7]);

    for execute in EXECUTE_ENTRIES {
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("SELECT ? AS answer").unwrap();
        statement.bind(parameters()).unwrap();
        execute(&mut statement).unwrap();
        let failure = execute(&mut statement).unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidState,
            &["consumed by the previous execute"],
        );

        statement.bind(parameters()).unwrap();
        execute(&mut statement).unwrap();

        statement.set_sql_query("SELECT 1").unwrap();
        execute(&mut statement).unwrap();
        execute(&mut statement).unwrap();
    }
    // Twelve requests for the twelve executes allowed to run; the three refusals sent nothing.
    let requests = requests.served(12);
    for attempts in requests.chunks(4) {
        let sql = attempts.iter().map(CapturedRequest::sql);
        assert!(sql.eq([
            "SELECT ? AS answer",
            "SELECT ? AS answer",
            "SELECT 1",
            "SELECT 1"
        ]));
    }
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
            return Some(completed_job(
                "paged-job",
                json!({
                    "schema": integer_schema("value"),
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
    let mut statement = test_query_at(&endpoint, "SELECT value FROM dataset.table");
    set_option(
        &mut statement,
        BqOption::UseStorageApiDisabledClient,
        "true",
    );
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
    requests.served(2);
}

/// adbc.h lets only ExecuteQuery, ExecuteSchema and Prepare invalidate prior result sets
/// (adbc.h:2003/2024/2045), so the parameter-schema dry run must not cancel the lazy reader of a
/// parameterized execute, which runs the remaining parameter sets as it is drained.
#[test]
fn getting_the_parameter_schema_preserves_a_draining_result_and_its_progress() {
    let mut result = select_answer(&["7"]);
    result.1["totalBytesProcessed"] = json!("7");
    let (endpoint, requests) = capture_requests(vec![
        result.clone(),
        ok(json!({
            "statistics": {"query": {"statementType": "SELECT"}},
            "status": {
                "state": "DONE",
                "errorResult": {"reason": "backendError", "message": "retry"}
            }
        })),
        dry_run_answer(json!({
            "undeclaredQueryParameters": [{"name": "", "parameterType": {"type": "INT64"}}],
            "totalBytesProcessed": "999"
        })),
        result,
    ]);
    let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
    statement.bind(int64_column("value", vec![1, 2])).unwrap();

    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

    assert_eq!(statement.get_parameter_schema().unwrap().fields().len(), 1);
    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        assert_eq!(statement.get_option_double(key).unwrap(), 7.0);
    }

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
    requests.served(4);
}

/// Describing a statement needs no values, so one that was never bound, or whose binding an earlier
/// execute consumed, is still describable. Its placeholders reach the dry run undeclared under the
/// statement's parameter mode, which BigQuery types; without the mode it refuses `?`.
#[test]
fn describing_an_unbound_statement_leaves_its_placeholders_undeclared() {
    let described = dry_run_answer(json!({
        "statementType": "SELECT", "schema": integer_schema("answer")
    }));
    let (endpoint, requests) = capture_requests(vec![
        described.clone(),
        completed_job("job", json!({})),
        described,
    ]);
    let mut statement = test_query_at(&endpoint, "SELECT ? AS answer");
    assert_eq!(
        statement.execute_schema().unwrap().field(0).name(),
        "answer"
    );
    statement.bind(int64_column("value", vec![7])).unwrap();
    statement.execute_update().unwrap();
    assert_eq!(
        statement.execute_schema().unwrap().field(0).name(),
        "answer"
    );

    let requests = requests.served(3);
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
