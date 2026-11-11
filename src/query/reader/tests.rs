use std::sync::Arc;

use adbc_core::CancelHandle;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde_json::json;

use crate::cancellation::{Cancellation, OperationCancelHandle, operation_reader};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::pipeline::QueryResultOptions;
use crate::runtime::SharedRuntime;
use crate::test_support::http::{capture_requests_until_stopped, ok};

use super::*;

fn execution_at(runtime: &SharedRuntime, endpoint: &str) -> ExecutionContext {
    ExecutionContext {
        runtime: runtime.clone(),
        state: ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        billing_project_id: None,
        current_catalog: None,
        current_schema: None,
        location: None,
        result_options: QueryResultOptions::default(),
    }
}

/// An execution whose REST requests reach `endpoint` and whose read sessions reach `storage`.
fn execution_with_storage(
    runtime: &SharedRuntime,
    endpoint: &str,
    storage: &str,
) -> ExecutionContext {
    let mut config = crate::config::DatabaseConfig {
        authentication_type: crate::BigQueryAuthenticationType::Anonymous,
        ..crate::config::DatabaseConfig::default()
    };
    for kind in ServiceEndpointKind::ALL {
        let endpoint = match kind {
            ServiceEndpointKind::BigQuery => endpoint,
            ServiceEndpointKind::Storage => storage,
            _ => crate::test_support::UNREACHABLE_ENDPOINT,
        };
        config.endpoints.set(kind, Some(Arc::from(endpoint)));
    }
    ExecutionContext {
        state: ClientState::new(&config),
        ..execution_at(runtime, endpoint)
    }
}

/// An execution for results that need no further request.
fn offline_execution() -> ExecutionContext {
    let runtime = crate::runtime::new_runtime().unwrap();
    execution_at(&runtime, crate::test_support::UNREACHABLE_ENDPOINT)
}

/// A query that completed with `body` as its only response.
fn completed(body: &[u8]) -> direct::CompletedQuery {
    let mut response = response::QueryResponse::decode(body, "query response").unwrap();
    let mut metadata = response::QueryMetadata::default();
    metadata.absorb_query(&mut response);
    direct::CompletedQuery {
        metadata,
        page: response.page(),
        completed_job: None,
    }
}

fn integer_result(values: &[&str]) -> direct::CompletedQuery {
    let body = json!({
        "jobComplete": true,
        "statementType": "SELECT",
        "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
        "totalRows": values.len().to_string(),
        "rows": values.iter().map(|value| json!({"f": [{"v": value}]})).collect::<Vec<_>>()
    });
    completed(&serde_json::to_vec(&body).unwrap())
}

fn assert_cancelled(failure: ArrowError) {
    let ArrowError::ExternalError(source) = failure else {
        panic!("expected an external ADBC cancellation error, got {failure}");
    };
    assert_eq!(
        source
            .downcast_ref::<adbc_core::error::Error>()
            .unwrap()
            .status,
        Status::Cancelled
    );
}

#[test]
fn a_resident_result_reports_invalid_cells_on_iteration_once() {
    let execution = offline_execution();
    let complete = integer_result(&["private-invalid-integer"]);

    let mut reader = reader_from_rest_query(&execution, complete, None, None)
        .expect("row conversion errors belong to the result stream");
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(failure.contains("column \"value\", row 0"), "{failure}");
    assert!(!failure.contains("private-invalid-integer"), "{failure}");
    assert!(reader.next().is_none());
}

#[test]
fn results_either_side_of_the_resident_limit_keep_every_row() {
    let execution = offline_execution();
    for row_count in [1, 32, 33] {
        let values = (0..row_count)
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let values = values.iter().map(String::as_str).collect::<Vec<_>>();
        let reader =
            reader_from_rest_query(&execution, integer_result(&values), None, None).unwrap();
        let actual = reader
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, (0..row_count).collect::<Vec<i64>>(), "{row_count}");
    }
}

#[test]
fn a_wide_resident_row_without_a_page_token_remains_readable() {
    let execution = offline_execution();
    let value = "x".repeat(8 * 1024);
    let body = json!({
        "jobComplete": true,
        "statementType": "SELECT",
        "schema": {"fields": [{"name": "value", "type": "STRING"}]},
        "totalRows": "1",
        "rows": [{"f": [{"v": value}]}]
    });
    let complete = completed(&serde_json::to_vec(&body).unwrap());
    let mut reader = reader_from_rest_query(&execution, complete, None, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        value
    );
    assert!(reader.next().is_none());
}

#[test]
fn a_resident_result_reports_a_closed_conversion_pipeline_once() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let execution = execution_at(&runtime, crate::test_support::UNREACHABLE_ENDPOINT);
    let mut reader = reader_from_rest_query(&execution, integer_result(&["7"]), None, None)
        .expect("creating a resident reader must not acquire its conversion permit");
    runtime.result_conversion_permits().close();

    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        failure.contains("conversion pipeline shut down"),
        "{failure}"
    );
    assert!(reader.next().is_none());
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
}

#[test]
fn a_resident_result_reports_cancellation_before_or_after_its_batch_once() {
    let execution = offline_execution();
    for after_batch in [false, true] {
        let cancellation = Cancellation::new(
            execution.runtime.clone(),
            execution.state.clone(),
            "statement",
        );
        let operation = cancellation.begin();
        let inner = reader_from_rest_query(
            &execution,
            integer_result(&["7"]),
            Some(operation.clone()),
            None,
        )
        .unwrap();
        let mut reader = operation_reader(inner, operation);
        if after_batch {
            assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        }

        let handle = OperationCancelHandle(cancellation);
        handle.try_cancel().unwrap();

        assert_cancelled(reader.next().unwrap().unwrap_err());
        assert!(reader.next().is_none());
        assert_eq!(
            handle.try_cancel().unwrap_err().status,
            Status::InvalidState
        );
    }
}

#[test]
fn a_resident_result_waiting_for_the_shared_conversion_permit_can_be_cancelled() {
    use std::sync::mpsc;
    use std::time::Duration;

    let runtime = crate::runtime::new_runtime_with_result_conversion_concurrency(1).unwrap();
    let execution = execution_at(&runtime, crate::test_support::UNREACHABLE_ENDPOINT);
    let permit = runtime
        .result_conversion_permits()
        .try_acquire_owned()
        .unwrap();
    let cancellation = Cancellation::new(runtime, execution.state.clone(), "statement");
    let operation = cancellation.begin();
    let inner = reader_from_rest_query(
        &execution,
        integer_result(&["7"]),
        Some(operation.clone()),
        None,
    )
    .expect("creating a resident reader must not wait for row conversion");
    let mut reader = operation_reader(inner, operation);
    let (started, wait_for_start) = mpsc::sync_channel(0);
    let (finished, wait_for_finish) = mpsc::channel();
    let read = std::thread::spawn(move || {
        started.send(()).unwrap();
        let first = reader.next();
        let exhausted = reader.next().is_none();
        finished.send((first, exhausted)).unwrap();
    });
    wait_for_start.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = wait_for_finish
        .recv_timeout(Duration::from_millis(100))
        .ok();
    let finished_early = early.is_some();
    let cancelled = OperationCancelHandle(cancellation).try_cancel();
    let result = match early {
        Some(result) => Ok(result),
        None => wait_for_finish.recv_timeout(Duration::from_secs(5)),
    };
    // Release before asserting so a broken cancellation wait cannot leave the test thread stuck.
    drop(permit);
    read.join().unwrap();

    assert!(!finished_early, "row conversion bypassed the shared permit");
    cancelled.unwrap();
    let (first, exhausted) = result.expect("cancellation must stop waiting for the held permit");
    assert_cancelled(first.unwrap().unwrap_err());
    assert!(exhausted);
}

/// One transient failure must not cost the query its job metadata or a large result its
/// Storage Read.
#[test]
fn a_transient_failure_describing_the_job_is_retried() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (endpoint, server) = capture_requests_until_stopped(vec![
        (
            503,
            json!({"error":{"code":503,"message":"unavailable","errors":[{"reason":"backendError","message":"unavailable"}]}}),
        ),
        ok(json!({
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "status":{"state":"DONE"},
            "statistics":{"query":{"statementType":"SELECT"}}
        })),
    ]);
    let execution = execution_at(&runtime, &endpoint);
    // A completed, job-backed result that never named its statement type.
    let complete = completed(
        br#"{"jobComplete":true,"jobReference":{"projectId":"project","jobId":"job","location":"US"},"schema":{"fields":[{"name":"value","type":"INTEGER"}]},"rows":[{"f":[{"v":"7"}]}],"totalRows":"1"}"#,
    );

    let reader = reader_from_rest_query(&execution, complete, None, None).unwrap();

    assert_eq!(
        reader.schema().metadata()[crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE],
        "SELECT"
    );
    assert_eq!(server.stop().len(), 2);
}

#[test]
fn empty_results_keep_metadata_and_empty_pages_follow_their_token() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for paginated in [false, true] {
        let (endpoint, server) = capture_requests_until_stopped(vec![ok(
            json!({"jobComplete":true,"rows":[{"f":[{"v":"7"}]}]}),
        )]);
        let mut execution = execution_at(&runtime, &endpoint);
        execution.result_options.use_storage_read = false;
        let mut body = json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "statementType":"SELECT",
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "totalRows":"0",
            "rows":[]
        });
        if paginated {
            body["pageToken"] = json!("next");
        }
        let complete = completed(&serde_json::to_vec(&body).unwrap());
        let mut reader = reader_from_rest_query(&execution, complete, None, None).unwrap();
        assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(
            reader.schema().metadata()[crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID],
            "job"
        );
        assert_eq!(
            reader.next().map(|batch| batch.unwrap().num_rows()),
            paginated.then_some(1)
        );
        assert!(reader.next().is_none());
        assert_eq!(server.stop().len(), usize::from(paginated));
    }
}

/// BigQuery omits zero-valued statistics, so an absent cost must not trigger a `jobs.get`; it
/// is reported as absent, not as zero.
#[test]
fn query_response_statistics_avoid_fetching_a_job_unless_the_statement_type_is_missing() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (has_job, omitted, expected_requests) in [
        (false, None, 0),
        (true, None, 0),
        (true, Some("statementType"), 1),
        (true, Some("totalBytesBilled"), 0),
        (true, Some("totalSlotMs"), 0),
    ] {
        // Two staged answers, so a second fetch is captured and fails the count below.
        let job = json!({
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "status":{"state":"DONE"},
            "statistics":{"query":{
                "statementType":"SELECT", "totalBytesBilled":"0", "totalSlotMs":"0"
            }}
        });
        let (endpoint, server) = capture_requests_until_stopped(vec![(200, job); 2]);
        let execution = execution_at(&runtime, &endpoint);
        let mut body = json!({
            "jobComplete":true,
            "queryId":"query",
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "rows":[{"f":[{"v":"7"}]}],
            "totalRows":"1",
            "statementType":"SELECT", "totalBytesBilled":"0", "totalSlotMs":"0"
        });
        if has_job {
            body["jobReference"] = json!({"projectId":"project","jobId":"job","location":"US"});
        }
        if let Some(omitted) = omitted {
            body.as_object_mut().unwrap().remove(omitted);
        }
        let complete = completed(&serde_json::to_vec(&body).unwrap());
        let mut reader = reader_from_rest_query(&execution, complete, None, None).unwrap();
        assert_eq!(
            reader.schema().metadata()[crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE],
            "SELECT"
        );
        assert_eq!(
            reader
                .schema()
                .metadata()
                .get(crate::BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED)
                .map(String::as_str),
            (omitted != Some("totalBytesBilled")).then_some("0"),
            "has_job={has_job}, omitted={omitted:?}"
        );
        assert_eq!(
            reader
                .schema()
                .metadata()
                .get(crate::BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS)
                .map(String::as_str),
            (omitted != Some("totalSlotMs")).then_some("0"),
            "has_job={has_job}, omitted={omitted:?}"
        );
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert!(reader.next().is_none());
        let requests = server.stop();
        assert_eq!(
            execution.state.initialized_clients().rest,
            expected_requests != 0,
            "resident results need no REST transport"
        );
        assert_eq!(
            requests.len(),
            expected_requests,
            "has_job={has_job}, omitted={omitted:?}"
        );
        for request in requests {
            assert!(
                request
                    .line
                    .starts_with("GET /bigquery/v2/projects/project/jobs/job?")
            );
        }
    }
}

/// Storage Read rereads the whole result, so a first page that already ends a large result is
/// served as is, and only rows still to page through justify the job lookup a read session
/// starts with.
#[test]
fn only_rows_still_to_page_through_justify_a_storage_read() {
    const ROWS: usize = 120_000;
    let runtime = crate::runtime::new_runtime().unwrap();
    for (page_token, expected_requests) in [(None, 0), (Some("next"), 1)] {
        let (endpoint, server) = capture_requests_until_stopped(vec![ok(json!({
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "status":{"state":"DONE"}
        }))]);
        let execution = execution_at(&runtime, &endpoint);
        let mut body = json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "statementType":"SELECT",
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "totalRows":ROWS.to_string(),
            "rows":vec![json!({"f":[{"v":"7"}]}); ROWS - 90_000]
        });
        if let Some(page_token) = page_token {
            body["pageToken"] = json!(page_token);
        } else {
            body["rows"] = json!(vec![json!({"f":[{"v":"7"}]}); ROWS]);
        }
        let complete = completed(&serde_json::to_vec(&body).unwrap());
        let reader = reader_from_rest_query(&execution, complete, None, None).unwrap();
        if page_token.is_none() {
            let rows = reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
            assert_eq!(rows, ROWS);
        }
        let job_lookups = server
            .stop()
            .iter()
            .filter(|request| {
                request
                    .line
                    .starts_with("GET /bigquery/v2/projects/project/jobs/job?")
            })
            .count();
        assert_eq!(job_lookups, expected_requests, "page token {page_token:?}");
    }
}

/// A result with a row too large for any REST response is not an empty result: without
/// Storage Read it fails, saying how to read it.
#[test]
fn a_result_too_large_for_rest_fails_without_storage_read() {
    let mut execution = offline_execution();
    execution.result_options.use_storage_read = false;
    let mut complete = completed(
        br#"{"jobComplete":true,"jobReference":{"projectId":"project","jobId":"job","location":"US"},"statementType":"SELECT","schema":{"fields":[{"name":"big","type":"STRING"}]},"totalRows":"2"}"#,
    );
    complete.metadata.rows_exceed_rest = true;

    let failure = reader_from_rest_query(&execution, complete, None, None)
        .err()
        .unwrap();
    assert_eq!(failure.status, Status::InvalidArguments);
    crate::test_support::errors::assert_message(
        &failure,
        &["20 MiB", "bigquery.query.use_storage_api_disabled_client"],
    );
}

/// The REST size-limit context must keep the Storage Read diagnostics, including the status
/// that lets callers recognize a retryable refusal such as the read-session rate limit.
#[test]
fn a_result_too_large_for_rest_keeps_the_storage_read_failure_details() {
    for status in [Status::IO, Status::Unauthorized, Status::NotImplemented] {
        let mut reason = error("BigQuery refused a Storage Read session", status);
        reason.vendor_code = 429;
        reason.sqlstate = [72, 89, 48, 48, 48];
        reason.details = Some(vec![("reason".to_owned(), b"rateLimitExceeded".to_vec())]);
        let failure = rows_exceed_rest_error(Some(reason));
        assert_eq!(failure.status, status);
        assert_eq!(failure.vendor_code, 429);
        assert_eq!(failure.sqlstate, [72, 89, 48, 48, 48]);
        assert_eq!(
            failure.details,
            Some(vec![("reason".to_owned(), b"rateLimitExceeded".to_vec())])
        );
        crate::test_support::errors::assert_message(
            &failure,
            &["20 MiB", "BigQuery refused a Storage Read session"],
        );
    }
}

#[test]
fn a_select_identified_by_job_metadata_requires_a_result_schema() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (query_schema, job_schema) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut job = json!({
            "jobReference":{"projectId":"project","jobId":"job","location":"US"},
            "status":{"state":"DONE"},
            "statistics":{"query":{"statementType":"SELECT"}}
        });
        if job_schema {
            job["statistics"]["query"]["schema"] =
                json!({"fields":[{"name":"job_result","type":"STRING"}]});
        }
        let (endpoint, server) = capture_requests_until_stopped(vec![(200, job)]);
        let execution = execution_at(&runtime, &endpoint);
        let mut body = json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":"job","location":"US"}
        });
        if query_schema {
            body["schema"] = json!({"fields":[{"name":"query_result","type":"INTEGER"}]});
        }
        let complete = completed(&serde_json::to_vec(&body).unwrap());

        let result = reader_from_rest_query(&execution, complete, None, None);
        assert_eq!(server.stop().len(), 1);
        if !query_schema && !job_schema {
            let failure = result.err().expect("SELECT requires a result schema");
            assert_eq!(failure.status, Status::InvalidData);
            assert!(
                failure
                    .message
                    .contains("completed without a result schema")
            );
        } else {
            let mut reader = result.unwrap();
            let schema = reader.schema();
            let (name, data_type) = if job_schema {
                ("job_result", DataType::Utf8)
            } else {
                ("query_result", DataType::Int64)
            };
            assert_eq!(schema.field(0).name(), name);
            assert_eq!(schema.field(0).data_type(), &data_type);
            assert_eq!(
                schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE],
                "SELECT"
            );
            assert!(reader.next().is_none());
        }
    }
}

/// The read path is a `debug` event, once per result however many pages follow, so an operator
/// (or the dbt suite) can see which one a result took.
#[test]
fn a_result_read_over_further_rest_pages_reports_it_once() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let page = |value: &str, token: Option<&str>| {
        let mut page = json!({"jobComplete":true,"rows":[{"f":[{"v":value}]}]});
        if let Some(token) = token {
            page["pageToken"] = json!(token);
        }
        (200, page)
    };
    let (endpoint, server) =
        capture_requests_until_stopped(vec![page("8", Some("third")), page("9", None)]);
    let mut execution = execution_at(&runtime, &endpoint);
    execution.result_options.use_storage_read = false;
    let complete = completed(
        br#"{"jobComplete":true,"jobReference":{"projectId":"project","jobId":"paged-diag","location":"US"},"statementType":"SELECT","schema":{"fields":[{"name":"value","type":"INTEGER"}]},"rows":[{"f":[{"v":"7"}]}],"totalRows":"3","pageToken":"second"}"#,
    );

    let (rows, events) = crate::diag::capture(|| {
        reader_from_rest_query(&execution, complete, None, None)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>()
    });
    assert_eq!(rows, 3);
    assert_eq!(server.stop().len(), 2, "two further pages were fetched");
    let reported = events
        .iter()
        .filter(|event| event.contains("project:US.paged-diag"))
        .collect::<Vec<_>>();
    assert_eq!(
        reported,
        ["DEBUG fetching further REST pages of BigQuery job project:US.paged-diag"]
    );
}

/// A narrow first page cannot predict a later row's width. The entire result must come from
/// Storage Read before the first REST row is emitted.
#[test]
fn a_paginated_result_uses_storage_read_before_emitting_any_rest_rows() {
    use arrow_array::RecordBatch;

    use crate::test_support::grpc::{MockPlan, StorageMock, ipc_messages};
    use crate::test_support::http::{capture_requests_responding, error_body};

    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let wide = "x".repeat(21_000_000);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["first", wide.as_str()]))],
    )
    .unwrap();
    let (read_schema, messages) = ipc_messages(&schema, &[batch]);
    let storage = StorageMock::start(
        &runtime,
        MockPlan {
            read_schema,
            read_responses: messages.into_iter().map(|message| (2, message)).collect(),
            ..MockPlan::default()
        },
    );
    let (endpoint, server) = capture_requests_responding(1, |request| {
        Some(
            if request
                .path()
                .starts_with("/bigquery/v2/projects/project/jobs/job?")
            {
                ok(json!({
                    "jobReference":{"projectId":"project","jobId":"job","location":"US"},
                    "status":{"state":"DONE"},
                    "configuration":{"query":{"destinationTable":
                        {"projectId":"project","datasetId":"_anon","tableId":"results"}}}
                }))
            } else {
                (
                    403,
                    error_body("responseTooLarge", "a later row exceeds the REST limit"),
                )
            },
        )
    });
    let execution = execution_with_storage(&runtime, &endpoint, storage.endpoint());
    let body = json!({
        "jobComplete":true,
        "jobReference":{"projectId":"project","jobId":"job","location":"US"},
        "statementType":"SELECT",
        "schema":{"fields":[{"name":"value","type":"STRING"}]},
        "rows":[{"f":[{"v":"first"}]}],
        "pageToken":"wide"
    });
    let complete = completed(&serde_json::to_vec(&body).unwrap());
    let mut reader = reader_from_rest_query(&execution, complete, None, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        batch.num_rows(),
        2,
        "the first REST row was emitted separately"
    );
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "first");
    assert_eq!(values.value(1), wide);
    assert!(reader.next().is_none());
    let requests = server.join().unwrap();
    assert!(
        requests[0]
            .path()
            .starts_with("/bigquery/v2/projects/project/jobs/job?")
    );
}

/// Reads a paginated result through a scripted read session serving three rows, after a job
/// lookup that answers with `statistics`. Returns the rows read, the diagnostics, and the server
/// that recorded the session request.
fn read_through_storage_read(
    statistics: &serde_json::Value,
) -> (usize, Vec<String>, crate::test_support::grpc::StorageMock) {
    use arrow_array::{Int64Array, RecordBatch};

    use crate::test_support::grpc::{MockPlan, StorageMock, ipc_messages};

    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let (read_schema, messages) = ipc_messages(&schema, &[batch]);
    let storage = StorageMock::start(
        &runtime,
        MockPlan {
            read_schema,
            read_responses: messages.into_iter().map(|message| (3, message)).collect(),
            ..MockPlan::default()
        },
    );
    // The job lookup names the destination table the read session reads.
    let (endpoint, server) = capture_requests_until_stopped(vec![ok(json!({
        "jobReference":{"projectId":"project","jobId":"storage-diag","location":"US"},
        "status":{"state":"DONE"},
        "statistics":statistics,
        "configuration":{"query":{"destinationTable":
            {"projectId":"project","datasetId":"_anon","tableId":"results"}}}
    }))]);
    let execution = execution_with_storage(&runtime, &endpoint, storage.endpoint());
    // 100,000 rows still to page through: Storage Read rereads the result instead.
    let complete = completed(
        br#"{"jobComplete":true,"jobReference":{"projectId":"project","jobId":"storage-diag","location":"US"},"statementType":"SELECT","schema":{"fields":[{"name":"value","type":"INTEGER"}]},"rows":[{"f":[{"v":"1"}]}],"totalRows":"100001","pageToken":"next"}"#,
    );

    let (rows, events) = crate::diag::capture(|| {
        reader_from_rest_query(&execution, complete, None, None)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>()
    });
    server.stop();
    (rows, events, storage)
}

#[test]
fn a_result_read_through_storage_read_reports_it() {
    let (rows, events, _) = read_through_storage_read(&json!({}));
    assert_eq!(rows, 3, "the rows came from the read session");
    assert!(
        events.iter().any(|event| event
            == "DEBUG reading BigQuery job project:US.storage-diag through Storage Read"),
        "{events:?}"
    );
}

/// `jobs.getQueryResults` returns a destination table as its job left it, even after a later job
/// appends to it or truncates it (verified live), so the read session reads the table as of the
/// job's end. A job reporting no end reads the table as it is.
#[test]
fn a_storage_read_reads_the_result_table_as_of_the_jobs_end() {
    for (statistics, snapshot) in [
        (
            json!({"endTime": "1794392829005"}),
            Some((1_794_392_829, 5_000_000)),
        ),
        (json!({}), None),
    ] {
        let (rows, _, storage) = read_through_storage_read(&statistics);
        assert_eq!(rows, 3);
        assert_eq!(storage.session_snapshot_times(), [snapshot]);
    }
}
