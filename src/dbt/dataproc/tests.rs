use std::io::Write as _;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use adbc_core::error::Status;
use adbc_core::{Connection as _, Statement as _};
use serde_json::json;

use crate::test_support::driver::test_connection_with;
use crate::test_support::http::{capture_requests, capture_requests_gated, error_body};
use crate::{BigQueryDatabaseOption, BigQueryStatement, BigQueryStatementOption};

use super::{RunningOperation, Submission, batch_resource, finished};
use crate::dbt::{region_segment, resource_path, resource_segment};
use crate::error::rpc_code_status as rpc_status;
use crate::test_support::errors::{assert_message, assert_message_omits};
use crate::test_support::options::set_option;

/// The document `batch_to_str` produces for a serverless python model, with dbt-core 2.0's own
/// defaults and its quoted `"2"`.
const DBT_BATCH_YML: &str = "runtime_config:\n  \
    version: \"1.1\"\n  \
    properties:\n    \
    spark.executor.instances: \"2\"\n\
    pyspark_batch:\n  \
    main_python_file_uri: \"gs://dbt-python/analytics/model.py\"\n  \
    jar_file_uris:\n  \
    - \"gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar\"\n";

const OPERATION: &str = "projects/exec-project/locations/us-central1/operations/op-1";
const JOB_OPERATION: &str = "projects/exec-project/regions/us-central1/operations/op-2";
const PARENT: &str = "projects/exec-project/locations/us-central1";

/// A connection whose Dataproc host is the mock server, like a python model's connection
/// with `bigquery.dataproc_endpoint` pointed at a private deployment.
fn dataproc_connection(endpoint: &str) -> crate::connection::BigQueryConnection {
    test_connection_with(
        endpoint,
        [(
            BigQueryDatabaseOption::DataprocEndpoint.into(),
            endpoint.to_owned().into(),
        )],
    )
}

/// A statement carrying `options` and the `SELECT 1` placeholder dbt-core 2.0 finalizes every
/// python-model statement with, which must not discard the submission.
fn submission_statement(
    connection: &mut crate::connection::BigQueryConnection,
    options: &[(BigQueryStatementOption, &str)],
) -> BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    for (option, value) in options {
        set_option(&mut statement, *option, *value);
    }
    statement.set_sql_query("SELECT 1").unwrap();
    statement
}

/// Exactly what dbt-core 2.0 sets for `submission_method: serverless`, with `pooling_timeout` sent
/// only when the caller bounds the wait.
fn batch_statement(
    connection: &mut crate::connection::BigQueryConnection,
    pooling_timeout: Option<&str>,
) -> BigQueryStatement {
    let mut options = vec![
        (
            BigQueryStatementOption::DataprocComputeRegion,
            "us-central1",
        ),
        (BigQueryStatementOption::CreateBatchParent, PARENT),
        (BigQueryStatementOption::CreateBatchBatchYml, DBT_BATCH_YML),
    ];
    options.extend(
        pooling_timeout.map(|seconds| (BigQueryStatementOption::DataprocPoolingTimeout, seconds)),
    );
    submission_statement(connection, &options)
}

/// The same for `submission_method: cluster`.
fn cluster_statement(
    connection: &mut crate::connection::BigQueryConnection,
    pooling_timeout: Option<&str>,
) -> BigQueryStatement {
    let mut options = vec![
        (
            BigQueryStatementOption::DataprocComputeRegion,
            "us-central1",
        ),
        (BigQueryStatementOption::DataprocProject, "exec-project"),
        (
            BigQueryStatementOption::DataprocSubmitJobClusterName,
            "dbt-cluster",
        ),
        (
            BigQueryStatementOption::DataprocSubmitJobGcsPath,
            "gs://dbt-python/analytics/model.py",
        ),
    ];
    options.extend(
        pooling_timeout.map(|seconds| (BigQueryStatementOption::DataprocPoolingTimeout, seconds)),
    );
    submission_statement(connection, &options)
}

/// The `Operation` Dataproc returns while a submission is still running.
fn running(name: &str) -> serde_json::Value {
    json!({"name": name, "metadata": {"@type": "type.googleapis.com/x"}, "done": false})
}

/// The end-to-end serverless contract: the five options dbt-core 2.0 sends, the placeholder query,
/// one `batches.create`, and polling until the batch reports `SUCCEEDED`.
#[test]
fn dbt_serverless_python_model_creates_one_batch_and_waits_for_it() {
    let (endpoint, captured) = capture_requests(vec![
        (200, running(OPERATION)),
        (200, running(OPERATION)),
        (
            200,
            json!({
                "name": OPERATION,
                "done": true,
                "response": {
                    "name": "projects/exec-project/locations/us-central1/batches/batch-42",
                    "state": "SUCCEEDED",
                },
            }),
        ),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, Some("86400"));
    set_option(
        &mut statement,
        BigQueryStatementOption::CreateBatchBatchId,
        "batch-42",
    );

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(
        rows_affected, -1,
        "a Spark submission touches no rows, so its count is unknown rather than zero"
    );
    assert_eq!(reader.schema().fields().len(), 0);
    assert_eq!(
        reader
            .schema()
            .metadata()
            .get(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID),
        Some(&"projects/exec-project/locations/us-central1/batches/batch-42".to_owned())
    );
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 3, "one create, then polls until it is done");
    assert!(
        requests[0].line.starts_with(
            "POST /v1/projects/exec-project/locations/us-central1/batches?batchId=batch-42\
             &requestId=adbc_"
        ),
        "{}",
        requests[0].line
    );
    assert_eq!(
        requests[0].body,
        json!({
            "runtime_config": {
                "version": "1.1",
                "properties": {"spark.executor.instances": "2"},
            },
            "pyspark_batch": {
                "main_python_file_uri": "gs://dbt-python/analytics/model.py",
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"
                ],
            },
        }),
        "the batch document reaches Dataproc as the JSON its YAML describes"
    );
    for poll in &requests[1..] {
        assert_eq!(poll.line, format!("GET /v1/{OPERATION} HTTP/1.1"));
    }
}

/// The end-to-end cluster contract: only the placement and the main python file, exactly the
/// job the fork submits.
#[test]
fn dbt_cluster_python_model_submits_one_pyspark_job_and_waits_for_it() {
    let (endpoint, captured) = capture_requests(vec![
        (
            200,
            json!({"name": JOB_OPERATION, "metadata": {"jobId": "job-7"}, "done": false}),
        ),
        (
            200,
            json!({
                "name": JOB_OPERATION,
                "done": true,
                "response": {"status": {"state": "DONE"}},
            }),
        ),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = cluster_statement(&mut connection, Some("86400"));

    let partitioned = statement.execute_partitions().unwrap();
    assert_eq!(partitioned.rows_affected, -1);
    assert_eq!(
        partitioned
            .schema
            .metadata()
            .get(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID),
        Some(&"job-7".to_owned()),
        "a cluster job is reported by the ID Dataproc assigned it"
    );

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].line,
        "POST /v1/projects/exec-project/regions/us-central1/jobs:submitAsOperation HTTP/1.1"
    );
    let request_id = requests[0].body["requestId"].as_str().unwrap();
    assert!(request_id.starts_with("adbc_"), "{request_id}");
    assert_eq!(
        requests[0].body,
        json!({
            "job": {
                "placement": {"clusterName": "dbt-cluster"},
                "pysparkJob": {"mainPythonFileUri": "gs://dbt-python/analytics/model.py"},
            },
            "requestId": request_id,
        }),
        "nothing but the cluster and the main python file reaches the job, and the project and \
         region stay in the path they are bound to"
    );
    assert_eq!(
        requests[1].line,
        format!("GET /v1/{JOB_OPERATION} HTTP/1.1")
    );
}

/// The YAML document is Dataproc's `Batch` resource, so both protojson spellings of a field
/// name survive, quoting decides string versus number, and a profile's `dataproc_batch`
/// override arrives deep-merged with dbt-core 2.0's default.
#[test]
fn batch_yml_is_converted_to_the_json_document_dataproc_takes() {
    assert_eq!(
        batch_resource(DBT_BATCH_YML).unwrap()["runtime_config"]["properties"]["spark.executor.instances"],
        json!("2"),
        "dbt-core 2.0 quotes the executor count because properties is a map of strings"
    );

    let camel = "runtimeConfig:\n  version: \"1.1\"\npysparkBatch:\n  \
                 mainPythonFileUri: \"gs://b/m.py\"\n";
    assert_eq!(
        batch_resource(camel).unwrap(),
        json!({
            "runtimeConfig": {"version": "1.1"},
            "pysparkBatch": {"mainPythonFileUri": "gs://b/m.py"},
        }),
        "protojson accepts both field-name spellings, so neither is rewritten here"
    );

    // What a profile's `dataproc_batch` block looks like once dbt-core 2.0 has merged it into the
    // default document: an added section, a replaced scalar, an unquoted number.
    let merged = "runtime_config:\n  version: \"2.2\"\n  properties:\n    \
                  spark.executor.instances: \"4\"\n    spark.dynamicAllocation.enabled: \
                  \"false\"\nenvironment_config:\n  execution_config:\n    \
                  subnetwork_uri: \"default\"\n    ttl: \"3600s\"\npyspark_batch:\n  \
                  main_python_file_uri: \"gs://b/m.py\"\n  jar_file_uris: []\n\
                  labels:\n  attempts: 3\n";
    let batch = batch_resource(merged).unwrap();
    assert_eq!(batch["runtime_config"]["version"], json!("2.2"));
    assert_eq!(
        batch["environment_config"]["execution_config"]["ttl"],
        json!("3600s")
    );
    assert_eq!(batch["pyspark_batch"]["jar_file_uris"], json!([]));
    assert_eq!(
        batch["labels"]["attempts"],
        json!(3),
        "an unquoted scalar stays a number, which is what lets Dataproc reject it"
    );
}

#[test]
fn malformed_batch_yml_is_rejected_without_echoing_the_document() {
    const SECRET: &str = "create-batch-secret-sentinel";
    for (name, document) in [
        ("unterminated flow mapping", format!("{{{SECRET}")),
        (
            "a tab where YAML forbids one",
            format!("a:\n\t- {SECRET}\n"),
        ),
        ("two documents", format!("a: {SECRET}\n---\nb: 1\n")),
        ("a bare scalar", SECRET.to_owned()),
        ("a sequence", format!("- {SECRET}\n")),
    ] {
        let error = batch_resource(&document).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{name}");
        assert!(
            error
                .message
                .contains(BigQueryStatementOption::CreateBatchBatchYml.as_str()),
            "{name}: {}",
            error.message
        );
        assert!(
            !error.message.contains(SECRET),
            "{name} echoed the document: {}",
            error.message
        );
    }
}

/// A submission is finished when its resource reaches a terminal state, which the operation
/// completing does not imply. Unlike the fork, every state but the successful one fails.
#[test]
fn a_finished_operation_reports_the_state_its_resource_reached() {
    let done = |submission: Submission, response: serde_json::Value| {
        finished(
            submission,
            &json!({"name": OPERATION, "done": true, "response": response}),
            OPERATION,
        )
    };

    assert!(done(Submission::Batch, json!({"state": "SUCCEEDED"})).is_ok());
    assert!(done(Submission::ClusterJob, json!({"status": {"state": "DONE"}})).is_ok());

    let failed = done(
        Submission::Batch,
        json!({"state": "FAILED", "stateMessage": "driver exited with code 1"}),
    )
    .unwrap_err();
    assert_eq!(failed.status, Status::InvalidData);
    assert_message(&failed, &["driver exited with code 1"]);

    for (state, expected) in [
        ("CANCELLED", Status::Cancelled),
        ("CANCELLING", Status::InvalidData),
        ("PENDING", Status::InvalidData),
    ] {
        let error = done(Submission::Batch, json!({"state": state})).unwrap_err();
        assert_eq!(error.status, expected, "a batch in state {state}");
        assert_message(&error, &[state]);
    }

    // The fork fails a job only in state ERROR, so these two report success to dbt.
    for (state, expected) in [
        ("CANCELLED", Status::Cancelled),
        ("ATTEMPT_FAILURE", Status::InvalidData),
        ("ERROR", Status::InvalidData),
    ] {
        let error = done(
            Submission::ClusterJob,
            json!({"status": {"state": state, "details": "container failed"}}),
        )
        .unwrap_err();
        assert_eq!(error.status, expected, "a job in state {state}");
        assert_message(&error, &[state, "container failed"]);
    }
}

/// A failed operation carries the service's own status, which is preserved rather than
/// flattened the way the fork flattens everything to UNKNOWN.
#[test]
fn a_failed_operation_keeps_the_status_the_service_reported() {
    let failure = |code: i64, message: &str| {
        finished(
            Submission::Batch,
            &json!({
                "name": OPERATION,
                "done": true,
                "error": {"code": code, "message": message},
            }),
            OPERATION,
        )
        .unwrap_err()
    };

    let denied = failure(7, "Permission 'dataproc.batches.create' denied");
    assert_eq!(denied.status, Status::Unauthorized);
    assert_message(&denied, &["dataproc.batches.create", OPERATION]);

    assert_eq!(failure(5, "not found").status, Status::NotFound);
    assert_eq!(failure(4, "deadline").status, Status::Timeout);
    assert_eq!(failure(99, "who knows").status, Status::Unknown);

    for (code, expected) in [
        (1, Status::Cancelled),
        (3, Status::InvalidArguments),
        (6, Status::AlreadyExists),
        (8, Status::IO),
        (9, Status::InvalidState),
        (12, Status::NotImplemented),
        (13, Status::Internal),
        (16, Status::Unauthenticated),
    ] {
        assert_eq!(rpc_status(code), expected, "google.rpc.Code {code}");
    }
}

/// `pooling_timeout` expiry stops the wait, reports a timeout distinct from a failed model, and
/// asks Dataproc to stop the still-running batch.
///
/// The deadline is rechecked after a backoff sleep shortened to end at it, so no extra poll is
/// issued. One second of budget is consumed by the initial backoff, so the create is followed
/// directly by the cancel.
#[test]
fn an_expired_pooling_timeout_reports_a_timeout_without_another_poll_and_cancels_the_batch() {
    let (endpoint, captured) = capture_requests(vec![(200, running(OPERATION)), (200, json!({}))]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, Some("1"));

    let error = statement.execute_update().unwrap_err();
    assert_eq!(
        error.status,
        Status::Timeout,
        "a deadline that expired is not a model that failed: {}",
        error.message
    );
    assert_message(
        &error,
        &[BigQueryStatementOption::DataprocPoolingTimeout.as_str()],
    );
    assert!(
        error.message.ends_with(&format!(
            "; the BigQuery Dataproc batch {OPERATION} was asked to stop"
        )),
        "{}",
        error.message
    );

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "the create, then the cancel: nothing is polled once the deadline has passed"
    );
    assert_eq!(
        requests[1].line,
        format!("POST /v1/{OPERATION}:cancel HTTP/1.1"),
        "a batch has no cancel method of its own, so its operation is cancelled"
    );
}

/// While budget remains, the wait polls: two seconds fit one poll after the one-second backoff.
#[test]
fn a_pooling_timeout_with_budget_left_polls_before_it_expires() {
    let (endpoint, captured) = capture_requests(vec![
        (200, running(OPERATION)),
        (200, running(OPERATION)),
        (200, json!({})),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, Some("2"));

    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::Timeout
    );

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[1].line,
        format!("GET /v1/{OPERATION} HTTP/1.1"),
        "the second second of budget buys one poll"
    );
    assert_eq!(
        requests[2].line,
        format!("POST /v1/{OPERATION}:cancel HTTP/1.1")
    );
}

/// A refused cancel keeps `Status::Timeout`, says the batch may keep running and why, and is
/// attempted once.
#[test]
fn a_refused_cancel_still_reports_the_timeout_and_is_not_retried() {
    let (endpoint, captured) = capture_requests(vec![
        (200, running(OPERATION)),
        (
            403,
            json!({"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "denied"}}),
        ),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, Some("1"));

    let error = statement.execute_update().unwrap_err();
    assert_eq!(
        error.status,
        Status::Timeout,
        "the caller's timeout is the answer; the cancel is a courtesy: {}",
        error.message
    );
    for expected in [
        BigQueryStatementOption::DataprocPoolingTimeout.as_str(),
        "cancel the BigQuery Dataproc batch",
        "denied",
        "may keep running",
    ] {
        assert!(
            error.message.contains(expected),
            "{expected} missing from {}",
            error.message
        );
    }
    assert_message_omits(&error, &["asked to stop"]);

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "the create, then one refused cancel that is not repeated"
    );
    assert_eq!(
        requests[1].line,
        format!("POST /v1/{OPERATION}:cancel HTTP/1.1")
    );
}

/// A 200 whose body is not a JSON object fails on the first attempt without cancelling the
/// batch: a malformed response says nothing about the batch.
#[test]
fn a_poll_answered_with_a_json_array_fails_at_once_without_cancelling_the_batch() {
    let (endpoint, captured) = capture_requests(vec![(200, running(OPERATION)), (200, json!([]))]);
    let mut connection = dataproc_connection(&endpoint);
    // A minute of budget, so nothing here is the deadline expiring.
    let mut statement = batch_statement(&mut connection, Some("60"));

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidData, "{}", error.message);
    assert_message(&error, &["is not a JSON object"]);

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "the create and the one poll: a malformed body is not retried"
    );
    assert_eq!(requests[1].line, format!("GET /v1/{OPERATION} HTTP/1.1"));
}

/// A gateway timeout on a poll says nothing about the batch, so the poll is repeated and nothing
/// is cancelled. Failing the model would leave the batch running, and with `request_id` unset
/// `dbt retry` would submit a second one. Only an expired deadline or a statement cancel may
/// cancel the batch.
#[test]
fn a_gateway_timeout_while_polling_is_retried_rather_than_failing_the_batch() {
    let (endpoint, captured) = capture_requests(vec![
        (200, running(OPERATION)),
        (504, json!({"error": {"message": "the gateway timed out"}})),
        (
            200,
            json!({
                "name": OPERATION,
                "done": true,
                "response": {"state": "SUCCEEDED"},
            }),
        ),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, None);

    assert_eq!(
        statement.execute_update().unwrap(),
        None,
        "one transient poll failure must not fail a batch that went on to succeed"
    );

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        3,
        "the create, the 504, and the retried poll"
    );
    assert_eq!(requests[2].line, format!("GET /v1/{OPERATION} HTTP/1.1"));
    assert!(
        !requests
            .iter()
            .any(|request| request.line.contains(":cancel")),
        "a transient poll failure must not stop a batch that is still running"
    );
}

/// The other side of that: a status the service means as a verdict stops the wait at once,
/// rather than spending the whole `pooling_timeout` re-asking a question already answered.
#[test]
fn a_fatal_poll_rejection_fails_the_batch_immediately() {
    let (endpoint, captured) = capture_requests(vec![
        (200, running(OPERATION)),
        (
            403,
            json!({"error": {"code": 403, "message": "Permission denied on the operation"}}),
        ),
        (200, json!({})),
    ]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, None);

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
    assert_message(&error, &["Permission denied on the operation"]);

    // A cancel would have been issued before `execute_update` returned, so the next request
    // the server sees is this probe exactly when none was.
    let _probe = probe(&endpoint);
    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].line, "GET /probe HTTP/1.1",
        "the rejection stopped the wait rather than being retried: {}",
        requests[2].line
    );
}

/// A wait that gives up on its polls leaves the submission running and billing, so the error
/// says so and names it for the caller to stop.
#[test]
fn a_failed_poll_says_the_submission_may_still_be_running() {
    let denied = json!({"error": {"code": 403, "message": "Permission denied on the operation"}});
    for (cluster, operation, what) in [
        (false, OPERATION, "Dataproc batch"),
        (true, JOB_OPERATION, "Dataproc job"),
    ] {
        let (endpoint, captured) =
            capture_requests(vec![(200, running(operation)), (403, denied.clone())]);
        let mut connection = dataproc_connection(&endpoint);
        let mut statement = if cluster {
            cluster_statement(&mut connection, None)
        } else {
            batch_statement(&mut connection, None)
        };

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::Unauthorized, "{}", error.message);
        assert!(
            error.message.ends_with(&format!(
                "; the BigQuery {what} {operation} may still be running"
            )),
            "{}",
            error.message
        );
        assert_eq!(captured.join().unwrap().len(), 2);
    }
}

/// A refused submission names the operation, where it was sent, and the service's reason, so a
/// disabled API or a missing cluster can be told apart from a misconfigured project.
#[test]
fn a_refused_submission_names_where_it_was_sent() {
    let denied = json!({"error": {"code": 403, "message": "Cloud Dataproc API has not been used"}});
    let (endpoint, captured) = capture_requests(vec![(403, denied.clone()), (403, denied)]);
    let mut connection = dataproc_connection(&endpoint);

    let job = cluster_statement(&mut connection, None)
        .execute_update()
        .unwrap_err();
    assert_eq!(job.status, Status::Unauthorized, "{}", job.message);
    assert_message(
        &job,
        &[
            "failed to submit the BigQuery Dataproc job to cluster dbt-cluster in \
             projects/exec-project/regions/us-central1: Cloud Dataproc API has not been used",
        ],
    );

    let mut batch = batch_statement(&mut connection, None);
    set_option(
        &mut batch,
        BigQueryStatementOption::CreateBatchBatchId,
        "batch-1",
    );
    let batch = batch.execute_update().unwrap_err();
    assert_message(
        &batch,
        &[&format!(
            "failed to submit the BigQuery Dataproc batch batch-1 in {PARENT}: Cloud Dataproc API"
        )],
    );
    assert_eq!(
        captured.join().unwrap().len(),
        2,
        "a refused submission is not resent"
    );
}

/// A submission that failed transiently is resent under the same request ID, which Dataproc
/// answers with the operation the first request started if it got through.
#[test]
fn a_transiently_failed_submission_is_resent_under_its_request_id() {
    let (endpoint, captured) = capture_requests(vec![
        (
            503,
            error_body("backendError", "The service is unavailable."),
        ),
        (
            200,
            json!({"name": JOB_OPERATION, "metadata": {"jobId": "job-7"}, "done": true,
                "response": {"status": {"state": "DONE"}}}),
        ),
        (
            503,
            error_body("backendError", "The service is unavailable."),
        ),
        (
            200,
            json!({"name": OPERATION, "done": true,
                "response": {"name": format!("{PARENT}/batches/b"), "state": "SUCCEEDED"}}),
        ),
    ]);
    let mut connection = dataproc_connection(&endpoint);

    cluster_statement(&mut connection, None)
        .execute_update()
        .unwrap();
    batch_statement(&mut connection, None)
        .execute_update()
        .unwrap();

    let requests = captured.join().unwrap();
    let (job, batch) = requests.split_at(2);
    assert_eq!(job[0].line, job[1].line);
    assert_eq!(
        job[0].body, job[1].body,
        "the resend carries the same request ID"
    );
    assert_eq!(
        batch[0].line, batch[1].line,
        "the batch request ID is in its URL"
    );
    assert_ne!(
        job[0].body["requestId"],
        json!(
            batch[0]
                .line
                .split("requestId=")
                .nth(1)
                .unwrap()
                .split(' ')
                .next()
        ),
        "each submission has its own request ID"
    );
}

/// One bare request the mock server can capture, marking the end of an exchange whose last
/// response the driver is expected *not* to answer. The connection is returned so it outlives
/// the server's reply to it.
fn probe(endpoint: &str) -> std::net::TcpStream {
    let mut stream = std::net::TcpStream::connect(endpoint.trim_start_matches("http://"))
        .expect("the mock server had already served every response and closed");
    stream
        .write_all(b"GET /probe HTTP/1.1\r\nHost: probe\r\nContent-Length: 0\r\n\r\n")
        .unwrap();
    stream.flush().unwrap();
    stream
}

/// A `pooling_timeout` too large for the clock becomes no deadline rather than an overflow
/// panic.
#[test]
fn a_pooling_timeout_beyond_the_clock_waits_without_a_deadline() {
    let (endpoint, captured) = capture_requests(vec![(
        200,
        json!({
            "name": OPERATION,
            "done": true,
            "response": {"state": "SUCCEEDED"},
        }),
    )]);
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = batch_statement(&mut connection, Some("9223372036854775807"));

    assert_eq!(statement.execute_update().unwrap(), None);
    assert_eq!(captured.join().unwrap().len(), 1);
}

/// Cancelling the statement stops the poll loop and asks Dataproc to cancel the job, which
/// the fork never does: its jobs keep running and billing after a cancel.
#[test]
fn cancelling_a_submission_interrupts_the_poll_and_cancels_the_job() {
    let request_seen = Arc::new(Barrier::new(2));
    let server_seen = request_seen.clone();
    let (endpoint, captured) = capture_requests_gated(
        vec![
            (
                200,
                json!({
                    "name": JOB_OPERATION,
                    "metadata": {"jobId": "job-7"},
                    "done": false,
                }),
            ),
            (200, json!({})),
        ],
        move || {
            server_seen.wait();
            true
        },
    );
    let mut connection = dataproc_connection(&endpoint);
    let mut statement = cluster_statement(&mut connection, None);
    let handle = statement.get_cancel_handle();
    let canceller = thread::spawn(move || {
        // The submission is in flight; cancel once its answer has been read and the poll
        // loop is waiting, which is what leaves a job to cancel remotely.
        request_seen.wait();
        thread::sleep(Duration::from_millis(100));
        handle.try_cancel().unwrap();
    });

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Cancelled);
    assert!(
        error.message.ends_with(&format!(
            "; the BigQuery Dataproc job {JOB_OPERATION} was asked to stop"
        )),
        "the cancel names the job it stopped: {}",
        error.message
    );
    canceller.join().unwrap();

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].line,
        "POST /v1/projects/exec-project/regions/us-central1/jobs/job-7:cancel HTTP/1.1",
        "a job is cancelled by ID, the one call that stops it"
    );
}

/// A half-configured submission stays selected so that executing reports the option it is
/// missing, rather than falling through to the SQL path and reporting a missing query.
#[test]
fn a_half_configured_submission_names_the_option_it_is_missing() {
    for (configured, missing) in [
        (
            BigQueryStatementOption::CreateBatchParent,
            BigQueryStatementOption::CreateBatchBatchYml,
        ),
        (
            BigQueryStatementOption::CreateBatchBatchYml,
            BigQueryStatementOption::CreateBatchParent,
        ),
        (
            BigQueryStatementOption::DataprocSubmitJobClusterName,
            BigQueryStatementOption::DataprocSubmitJobGcsPath,
        ),
        (
            BigQueryStatementOption::DataprocSubmitJobGcsPath,
            BigQueryStatementOption::DataprocSubmitJobClusterName,
        ),
    ] {
        let mut connection = dataproc_connection("http://127.0.0.1:1");
        let mut statement = connection.new_statement().unwrap();
        set_option(&mut statement, configured, "configured");
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidState);
        assert!(
            error.message.contains(missing.as_str()),
            "{} should have been reported missing: {}",
            missing.as_str(),
            error.message
        );
    }

    // Both halves configured, but the region every Dataproc request is addressed by is not.
    let mut connection = dataproc_connection("http://127.0.0.1:1");
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::DataprocSubmitJobClusterName,
        "dbt-cluster",
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::DataprocSubmitJobGcsPath,
        "gs://b/m.py",
    );
    let error = statement.execute_update().unwrap_err();
    assert_message(
        &error,
        &[BigQueryStatementOption::DataprocComputeRegion.as_str()],
    );
}

/// These options are interpolated into the request path, and the region into the regional
/// host as well, so a value that would redirect the request is refused instead.
#[test]
fn resource_options_that_would_redirect_the_request_are_refused() {
    let key = BigQueryStatementOption::CreateBatchParent;
    assert_eq!(
        resource_path("projects/p/locations/eu", key).unwrap(),
        "projects/p/locations/eu"
    );
    for value in [
        "projects/p/locations/eu?alt=media",
        "projects/p#fragment",
        "projects/%70",
        "projects/p locations",
        // The WHATWG URL parser reqwest uses ends a special scheme's host at a backslash.
        "projects\\evil.example",
        // A relative segment reshapes the path it is placed in rather than naming anything.
        "../..",
        "projects/p/../../batches",
        "projects/./p",
        "/projects/p",
        "projects/p/",
    ] {
        let error = resource_path(value, key).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{value}");
        assert_message(&error, &[key.as_str()]);
    }

    let key = BigQueryStatementOption::DataprocProject;
    assert_eq!(
        resource_segment("exec-project", key).unwrap(),
        "exec-project"
    );
    for value in ["exec/project", "evil.example\\"] {
        let error = resource_segment(value, key).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{value}");
        assert_message(&error, &[key.as_str()]);
    }
}

/// The region becomes part of a host name, so it is held to the region-name alphabet rather than
/// a list of characters a URL parser treats specially.
#[test]
fn a_region_that_could_move_the_host_is_refused() {
    let key = BigQueryStatementOption::DataprocComputeRegion;
    assert_eq!(region_segment("us-central1", key).unwrap(), "us-central1");
    for value in [
        "",
        "us-central1/../europe-west1",
        // `https://attacker.example\-dataproc.googleapis.com` has host `attacker.example`.
        "attacker.example\\",
        "attacker.example",
        "user@attacker.example",
        "us-central1:8080",
    ] {
        let error = region_segment(value, key).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{value:?}");
        assert_message(&error, &[key.as_str()]);
    }
}

/// End to end: a backslash-bearing region never reaches the wire at all, so no bearer token
/// minted for the configured identity can be handed to a host the option chose.
#[test]
fn a_backslash_region_sends_no_request_anywhere() {
    // A port nothing listens on: a request that did escape would fail rather than succeed.
    let mut connection = dataproc_connection("http://127.0.0.1:1");
    let mut statement = batch_statement(&mut connection, None);
    set_option(
        &mut statement,
        BigQueryStatementOption::DataprocComputeRegion,
        "attacker.example\\",
    );

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
    assert_message(
        &error,
        &[BigQueryStatementOption::DataprocComputeRegion.as_str()],
    );
}

/// A `done` operation carrying neither an error nor a readable terminal state is reported as a
/// failure, not as a success.
#[test]
fn a_done_operation_with_no_terminal_state_is_not_a_success() {
    for response in [
        json!({"name": OPERATION, "done": true}),
        json!({"name": OPERATION, "done": true, "response": {}}),
        json!({"name": OPERATION, "done": true, "response": {"state": ""}}),
    ] {
        let error = finished(Submission::Batch, &response, OPERATION).unwrap_err();
        assert_eq!(error.status, Status::InvalidData, "{response}");
        assert_message(&error, &[OPERATION, "terminal state"]);
    }
}

/// The remote cancel escapes every segment it takes from the operation resource, so `?`, `#`, or
/// a dot segment cannot redirect it.
#[test]
fn the_remote_cancel_escapes_the_segments_it_addresses() {
    let running = RunningOperation {
        name: "projects/exec-project/regions/us-central1/operations/op-2".to_owned(),
        job_id: Some("job-7".to_owned()),
    };
    assert_eq!(
        running.cancel_url(Submission::ClusterJob, "https://dataproc.example"),
        "https://dataproc.example/v1/projects/exec-project/regions/us-central1/jobs/job-7:cancel"
    );

    // The job ID is the one segment that is not bounded by the operation name's own `/`
    // separators, so it is the one that can carry a whole path, a query or a fragment.
    let hostile = RunningOperation {
        name: "projects/p/regions/r?x/operations/op".to_owned(),
        job_id: Some("job#f?x=1/../../other".to_owned()),
    };
    assert_eq!(
        hostile.cancel_url(Submission::ClusterJob, "https://dataproc.example"),
        "https://dataproc.example/v1/projects/p/regions/r%3Fx/jobs/\
         job%23f%3Fx%3D1%2F..%2F..%2Fother:cancel"
    );

    // A batch has no cancel method of its own, so its whole operation name is the resource, and
    // its slashes are the path they name.
    assert_eq!(
        running.cancel_url(Submission::Batch, "https://dataproc.example"),
        "https://dataproc.example/v1/projects/exec-project/regions/us-central1/operations/\
         op-2:cancel"
    );
}
