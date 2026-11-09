use std::sync::{Arc, Barrier};
use std::thread;

use adbc_core::error::Status;
use adbc_core::{Connection as _, Statement as _};
use serde_json::json;

use crate::test_support::driver::test_connection_with;
use crate::test_support::http::{
    capture_requests, capture_requests_gated, capture_requests_until_stopped, error_body, ok,
};
use crate::{BigQueryDatabaseOption, BigQueryStatement, BigQueryStatementOption};

use super::{RunningOperation, Submission, batch_resource, finished};
use crate::dbt::{region_segment, resource_path, resource_segment};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::options::set_option;

/// The document `batch_to_str` produces for a serverless python model, with dbt 2.0's own
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

/// A statement carrying `options` and the `SELECT 1` placeholder dbt 2.0 finalizes every
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

/// Exactly what dbt 2.0 sets for `submission_method: serverless`, with `pooling_timeout` sent
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

/// The end-to-end serverless contract: the five options dbt 2.0 sends, the placeholder query,
/// one `batches.create`, and polling until the batch reports `SUCCEEDED`.
#[test]
fn dbt_serverless_python_model_creates_one_batch_and_waits_for_it() {
    let (endpoint, captured) = capture_requests(vec![
        ok(running(OPERATION)),
        ok(running(OPERATION)),
        ok(json!({
            "name": OPERATION,
            "done": true,
            "response": {
                "name": "projects/exec-project/locations/us-central1/batches/batch-42",
                "state": "SUCCEEDED",
            },
        })),
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
        ok(json!({"name": JOB_OPERATION, "metadata": {"jobId": "job-7"}, "done": false})),
        ok(json!({
            "name": JOB_OPERATION,
            "done": true,
            "response": {"status": {"state": "DONE"}},
        })),
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
/// override arrives deep-merged with dbt 2.0's default.
#[test]
fn batch_yml_is_converted_to_the_json_document_dataproc_takes() {
    assert_eq!(
        batch_resource(DBT_BATCH_YML).unwrap()["runtime_config"]["properties"]["spark.executor.instances"],
        json!("2"),
        "dbt 2.0 quotes the executor count because properties is a map of strings"
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

    // What a profile's `dataproc_batch` block looks like once dbt 2.0 has merged it into the
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
        assert_message(
            &error,
            &[BigQueryStatementOption::CreateBatchBatchYml.as_str()],
        );
        assert_message_omits(&error, &[SECRET]);
    }
}

/// A submission is finished when its resource reaches a terminal state, which the operation
/// completing does not imply. Unlike the fork, every state but the successful one fails, a `done`
/// operation that reports no state is not a success, and a failed operation keeps the status the
/// service reported rather than the fork's UNKNOWN.
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
    assert_failure(&failed, Status::InvalidData, &["driver exited with code 1"]);

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

    for completed in [
        json!({"name": OPERATION, "done": true}),
        json!({"name": OPERATION, "done": true, "response": {}}),
        json!({"name": OPERATION, "done": true, "response": {"state": ""}}),
    ] {
        let error = finished(Submission::Batch, &completed, OPERATION).unwrap_err();
        assert_failure(&error, Status::InvalidData, &[OPERATION, "terminal state"]);
    }

    let denied = finished(
        Submission::Batch,
        &json!({"name": OPERATION, "done": true,
            "error": {"code": 7, "message": "Permission 'dataproc.batches.create' denied"}}),
        OPERATION,
    )
    .unwrap_err();
    assert_failure(
        &denied,
        Status::Unauthorized,
        &["dataproc.batches.create", OPERATION],
    );
}

/// `pooling_timeout` expiry stops the wait, reports a timeout distinct from a failed model, and
/// asks Dataproc to stop the still-running batch. The deadline is rechecked after a backoff sleep
/// shortened to end at it, and the initial backoff consumes the one second of budget, so the
/// create is followed directly by the cancel. A batch has no cancel method of its own, so its
/// operation is cancelled.
///
/// The timeout stays the answer whatever the cancel does: a refused one says the batch may keep
/// running and why, and is attempted once; a transient failure has work to stop, so it is resent.
#[test]
fn an_expired_pooling_timeout_cancels_the_batch_without_another_poll() {
    let unavailable = (503, error_body("backendError", "unavailable"));
    let denied = (
        403,
        json!({"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "denied"}}),
    );
    for (cancel, sent, outcome) in [
        (ok(json!({})), 1, "was asked to stop"),
        (denied, 1, "failed (denied"),
        (unavailable, 2, "was asked to stop"),
    ] {
        // The last response answers a cancel that must not be sent, unless it is resent.
        let (endpoint, server) =
            capture_requests_until_stopped(vec![ok(running(OPERATION)), cancel, ok(json!({}))]);
        let mut connection = dataproc_connection(&endpoint);
        let mut statement = batch_statement(&mut connection, Some("1"));

        let error = statement.execute_update().unwrap_err();
        assert_failure(
            &error,
            Status::Timeout,
            &[
                BigQueryStatementOption::DataprocPoolingTimeout.as_str(),
                &format!("Dataproc batch {OPERATION} {outcome}"),
            ],
        );
        let requests = server.stop();
        assert_eq!(requests.len(), 1 + sent);
        for cancel in &requests[1..] {
            assert_eq!(cancel.line, format!("POST /v1/{OPERATION}:cancel HTTP/1.1"));
        }
    }
}

/// A gateway timeout on a poll says nothing about the batch, so the poll is repeated and nothing
/// is cancelled. Failing the model would leave the batch running, and with `request_id` unset
/// `dbt retry` would submit a second one. Only an expired deadline or a statement cancel may
/// cancel the batch.
#[test]
fn a_gateway_timeout_while_polling_is_retried_rather_than_failing_the_batch() {
    let (endpoint, captured) = capture_requests(vec![
        ok(running(OPERATION)),
        (504, json!({"error": {"message": "the gateway timed out"}})),
        ok(json!({
            "name": OPERATION,
            "done": true,
            "response": {"state": "SUCCEEDED"},
        })),
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

/// A status the service means as a verdict, or a 200 whose body is not a JSON object, stops the
/// wait at once, with nothing cancelled: neither says anything about the submission. That leaves
/// it running and billing, so the error says so and names it for the caller.
#[test]
fn a_fatal_poll_answer_says_the_submission_may_still_be_running() {
    let denied = json!({"error": {"code": 403, "message": "Permission denied on the operation"}});
    for (cluster, operation, what, poll, status) in [
        (
            false,
            OPERATION,
            "Dataproc batch",
            (403, denied.clone()),
            Status::Unauthorized,
        ),
        (
            true,
            JOB_OPERATION,
            "Dataproc job",
            (403, denied),
            Status::Unauthorized,
        ),
        (
            false,
            OPERATION,
            "Dataproc batch",
            ok(json!([])),
            Status::InvalidData,
        ),
    ] {
        // The last response answers a repeated poll or a cancel, which must never be sent.
        let (endpoint, server) =
            capture_requests_until_stopped(vec![ok(running(operation)), poll, ok(json!({}))]);
        let mut connection = dataproc_connection(&endpoint);
        let mut statement = if cluster {
            cluster_statement(&mut connection, None)
        } else {
            batch_statement(&mut connection, None)
        };

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, status, "{}", error.message);
        assert!(
            error.message.ends_with(&format!(
                "; the BigQuery {what} {operation} may still be running"
            )),
            "{}",
            error.message
        );
        assert_eq!(server.stop().len(), 2);
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
    assert_failure(
        &job,
        Status::Unauthorized,
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
        ok(
            json!({"name": JOB_OPERATION, "metadata": {"jobId": "job-7"}, "done": true,
                "response": {"status": {"state": "DONE"}}}),
        ),
        (
            503,
            error_body("backendError", "The service is unavailable."),
        ),
        ok(json!({"name": OPERATION, "done": true,
                "response": {"name": format!("{PARENT}/batches/b"), "state": "SUCCEEDED"}})),
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

/// Attempts that all failed may still have started the job, whichever way the resends end.
#[test]
fn a_deadline_during_failed_submissions_says_the_job_may_have_started() {
    let unavailable = (503, error_body("backendError", "Try again."));
    let (endpoint, server) = capture_requests_until_stopped(vec![unavailable; 5]);
    let mut connection = dataproc_connection(&endpoint);

    let error = batch_statement(&mut connection, Some("1"))
        .execute_update()
        .unwrap_err();
    assert_message(&error, &["; the BigQuery Dataproc batch may have started"]);
    server.stop();
}

/// A 2xx that cannot be read may answer a submission Dataproc accepted, so it is resent under the
/// same request ID to learn the operation, as a lost answer is.
#[test]
fn an_unreadable_submission_answer_is_resent_under_its_request_id() {
    // Valid to the mock, which serializes it, but past the decoder's recursion limit.
    let unreadable = (0..200).fold(json!(0), |value, _| json!([value]));
    let (endpoint, captured) = capture_requests(vec![
        ok(unreadable),
        ok(json!({"name": OPERATION, "done": true, "response": {"state": "SUCCEEDED"}})),
    ]);
    let mut connection = dataproc_connection(&endpoint);

    batch_statement(&mut connection, None)
        .execute_update()
        .unwrap();

    let requests = captured.join().unwrap();
    assert_eq!(requests[0].line, requests[1].line);
}

/// A `pooling_timeout` too large for the clock becomes no deadline rather than an overflow
/// panic.
#[test]
fn a_pooling_timeout_beyond_the_clock_waits_without_a_deadline() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({
        "name": OPERATION,
        "done": true,
        "response": {"state": "SUCCEEDED"},
    }))]);
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
            ok(json!({
                "name": JOB_OPERATION,
                "metadata": {"jobId": "job-7"},
                "done": false,
            })),
            ok(json!({})),
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
        // The submission is in flight, and its answer still names the job to cancel.
        request_seen.wait();
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
        assert_failure(&error, Status::InvalidState, &[missing.as_str()]);
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
/// host as well, so a value that would redirect the request is refused instead. The region is
/// held to the region-name alphabet rather than a list of characters a URL parser treats
/// specially.
#[test]
fn resource_options_that_would_redirect_the_request_are_refused() {
    let refused = |result: adbc_core::error::Result<&str>, key: BigQueryStatementOption, value| {
        let error = result.unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{value:?}");
        assert_message(&error, &[key.as_str()]);
    };
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
        refused(resource_path(value, key), key, value);
    }

    let key = BigQueryStatementOption::DataprocProject;
    assert_eq!(
        resource_segment("exec-project", key).unwrap(),
        "exec-project"
    );
    for value in ["exec/project", "evil.example\\"] {
        refused(resource_segment(value, key), key, value);
    }

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
        refused(region_segment(value, key), key, value);
    }

    // End to end, a backslash-bearing region never reaches the wire, so no bearer token minted for
    // the configured identity can be handed to a host the option chose. Nothing listens on the
    // port, so a request that did escape would fail differently.
    let mut connection = dataproc_connection("http://127.0.0.1:1");
    let mut statement = batch_statement(&mut connection, None);
    set_option(&mut statement, key, "attacker.example\\");
    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidArguments, &[key.as_str()]);
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
