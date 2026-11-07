use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use adbc_core::options::OptionValue;
use adbc_core::{CancelHandle as _, Connection as _, Statement as _};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value as JsonValue, json};

use super::{decode_polled_job, resolved_reference, validate_completed, wait_for_job};
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::rest::jobs::{JobClient, JobReference};
use crate::rest::retry::MAX_ATTEMPTS;
use crate::runtime::new_runtime;
use crate::test_support::driver::{test_connection_at, unlocated_connection_at};
use crate::test_support::http::{
    CapturedRequest, capture_requests, capture_requests_responding, capture_requests_until_stopped,
    error_body, ok,
};
use crate::test_support::options::set_option;
use crate::{BigQueryConnection, BigQueryStatementOption};

/// The two operations sharing this job, by their `what`. Every message is asserted for both, so
/// each keeps its own wording.
const OPERATIONS: [&str; 2] = ["CSV load", "table copy"];

fn requested() -> JobReference {
    JobReference::new("billing-project", Some("EU"), "job-1".to_owned())
}

fn job(body: &str, what: &str) -> super::LoadOrCopyJob {
    super::decode_job(body.as_bytes(), what).unwrap()
}

/// The identity check is what keeps a recovered submission from being some other job: only the
/// project and ID that were asked for are accepted, an inferred location is kept, and an explicit
/// one may differ only in case.
#[test]
fn a_job_response_must_identify_the_job_that_was_requested() {
    for what in OPERATIONS {
        for (body, expected) in [
            (r#"{}"#, "contains no job reference"),
            (
                r#"{"jobReference":{"projectId":"billing-project"}}"#,
                "contains an incomplete job reference billing-project:",
            ),
            (
                r#"{"jobReference":{"projectId":"billing-project","jobId":"other"}}"#,
                "names job billing-project:other, but the request was for job \
                 billing-project:EU.job-1",
            ),
            (
                r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":""}}"#,
                "names an empty location for job billing-project:EU.job-1",
            ),
            (
                r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":"US"}}"#,
                "names location US for job billing-project:EU.job-1, but the request was for \
                 location EU",
            ),
        ] {
            let failure = resolved_reference(&job(body, what), &requested(), what).unwrap_err();
            assert_eq!(failure.status, Status::InvalidData);
            assert_eq!(
                failure.message,
                format!("the BigQuery {what} job response {expected}")
            );
        }

        // A location the service inferred is adopted; one it spells differently is the same one.
        let inferred = job(
            r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1"}}"#,
            what,
        );
        let requested_without_location =
            JobReference::new("billing-project", None, "job-1".to_owned());
        assert_eq!(
            resolved_reference(&inferred, &requested_without_location, what)
                .unwrap()
                .location,
            None
        );
        assert_eq!(
            resolved_reference(&inferred, &requested(), what)
                .unwrap()
                .location
                .as_deref(),
            Some("EU")
        );
        let recased = job(
            r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":"eu"}}"#,
            what,
        );
        assert_eq!(
            resolved_reference(&recased, &requested(), what)
                .unwrap()
                .location
                .as_deref(),
            Some("eu")
        );
    }
}

/// A poll accepts only the three states BigQuery runs a job through, and says which job and which
/// states when it sees anything else.
#[test]
fn a_polled_job_reports_a_state_it_cannot_act_on() {
    for what in OPERATIONS {
        for state in ["PENDING", "RUNNING", "DONE"] {
            let body = format!(
                r#"{{"jobReference":{{"projectId":"billing-project","jobId":"job-1","location":"EU"}},"status":{{"state":"{state}"}}}}"#
            );
            decode_polled_job(body.as_bytes(), &requested(), what).unwrap();
        }

        let failure = decode_polled_job(
            br#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":"EU"},"status":{"state":"MYSTERY"}}"#,
            &requested(),
            what,
        )
        .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!(
                "BigQuery {what} job billing-project:EU.job-1 poll returned unknown state \
                 \"MYSTERY\"; expected PENDING, RUNNING, or DONE"
            )
        );

        let failure = decode_polled_job(
            br#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":"EU"}}"#,
            &requested(),
            what,
        )
        .unwrap_err();
        assert_eq!(
            failure.message,
            format!("BigQuery {what} job billing-project:EU.job-1 poll contained no state")
        );
    }
}

/// One poll's retries say nothing about the job, so failed polls neither end the wait nor cancel it:
/// the wait goes on past the retries of several, and the job that finishes is returned.
#[test]
fn a_wait_outlasts_failed_polls_and_leaves_the_job_alone() {
    let polled = |state| {
        json!({
            "jobReference": {"projectId": "billing-project", "jobId": "job-1", "location": "EU"},
            "status": {"state": state}
        })
    };
    let unavailable = (
        503,
        json!({"error": {"code": 503, "message": "unavailable", "status": "UNAVAILABLE"}}),
    );
    let mut responses = vec![unavailable; 2 * MAX_ATTEMPTS as usize + 1];
    responses.push((200, polled("DONE")));
    let staged = responses.len();
    let (endpoint, server) = capture_requests_until_stopped(responses);
    let runtime = new_runtime().unwrap();
    let client = JobClient::for_test(&endpoint).with_wait_outage(Duration::from_secs(600));
    let cancellation = Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint),
        "statement",
    );
    let operation = cancellation.begin();
    operation.register_job(&requested()).unwrap();
    let running = job(&polled("RUNNING").to_string(), "table copy");

    let finished = runtime
        .block_on(wait_for_job(
            &client,
            &operation,
            &requested(),
            running,
            "table copy",
        ))
        .unwrap();

    assert!(finished.status.as_ref().unwrap().is_done());
    // Delivers synchronously any cancellation still claimable, so none can be missed below.
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    operation.finish();
    assert_eq!(
        server.stop().len(),
        staged,
        "the finished job was cancelled"
    );
}

/// The authoritative failure is rendered first and never twice, even when its repeat in `errors`
/// carries a `debugInfo` the primary lacks. The job names itself, and an unfinished response is
/// not a result.
#[test]
fn a_completed_job_reports_its_authoritative_error_first() {
    for what in OPERATIONS {
        let mut failed = job(
            r#"{
                "jobReference":{"projectId":"billing-project","jobId":"job-1","location":"EU"},
                "status": {
                    "state": "DONE",
                    "errorResult": {"reason": "stopped", "message": "primary failure"},
                    "errors": [
                        {"reason": "invalidQuery", "message": "secondary failure"},
                        {"reason": "stopped", "message": "primary failure", "debugInfo": "x"}
                    ]
                }
            }"#,
            what,
        );
        let failure = validate_completed(&mut failed, what).unwrap_err();
        assert_eq!(failure.status, Status::Cancelled);
        assert_eq!(
            failure.message,
            format!(
                "BigQuery {what} job failed (job billing-project:EU.job-1): stopped: primary \
                 failure; invalidQuery: secondary failure (Query: \
                 https://console.cloud.google.com/bigquery?project=billing-project&j=bq:EU:job-1&page=queryresults)"
            )
        );
        assert!(
            failure.details.is_some_and(|details| details
                .iter()
                .any(|(key, value)| key == crate::error::DETAIL_JOB_ID
                    && value == b"billing-project:EU.job-1")),
            "the job ID goes out unflattened as well"
        );

        // The validator prioritizes errorResult over state. Production polling waits for DONE
        // before calling it; this checks only the validator's diagnostic precedence.
        let mut running = job(
            r#"{"status":{"state":"RUNNING","errorResult":{"reason":"invalid","message":"bad"}}}"#,
            what,
        );
        let failure = validate_completed(&mut running, what).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            format!("BigQuery {what} job failed: invalid: bad")
        );

        let mut unfinished = job(r#"{"status":{"state":"RUNNING"}}"#, what);
        let failure = validate_completed(&mut unfinished, what).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!("BigQuery {what} job response was not complete")
        );

        let mut statusless = job(r#"{}"#, what);
        assert_eq!(
            validate_completed(&mut statusless, what)
                .unwrap_err()
                .message,
            format!("completed BigQuery {what} job has no status")
        );
    }
}

/// Where the mock datasets are: a single region, which a job reaches only with an explicit
/// location.
const DATASET_LOCATION: &str = "asia-northeast1";

/// The `jobReference` the driver submitted, from a JSON `jobs.insert` body or from the metadata
/// part of a multipart upload.
fn submitted_reference(request: &CapturedRequest) -> JsonValue {
    if !request.body.is_null() {
        return request.body["jobReference"].clone();
    }
    let metadata = String::from_utf8_lossy(&request.raw_body);
    let start = metadata.find(r#""jobReference":"#).unwrap() + r#""jobReference":"#.len();
    let end = start + metadata[start..].find('}').unwrap() + 1;
    serde_json::from_str(&metadata[start..end]).unwrap()
}

/// Serves `count` requests the way BigQuery answers seeds and copies: each `datasets.get` with
/// the next of `datasets`, each job submission with that job still running, and each poll with
/// it finished in the next of `outcomes`.
fn bigquery(
    count: usize,
    datasets: Vec<(u16, JsonValue)>,
    outcomes: Vec<JsonValue>,
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    let mut datasets = datasets.into_iter();
    let mut outcomes = outcomes.into_iter();
    let mut submitted = JsonValue::Null;
    capture_requests_responding(count, move |request| {
        if request.path().contains("/datasets/") {
            return datasets.next();
        }
        if request.line.starts_with("POST ") {
            submitted = submitted_reference(request);
            return Some(ok(
                json!({"jobReference": submitted, "status": {"state": "RUNNING"}}),
            ));
        }
        Some(ok(json!({
            "jobReference": submitted,
            "status": outcomes.next()?,
            "statistics": {"load": {"outputRows": "1"}, "copy": {"copiedRows": "1"}}
        })))
    })
}

fn done() -> JsonValue {
    json!({"state": "DONE"})
}

fn located(location: &str) -> (u16, JsonValue) {
    ok(json!({"location": location}))
}

fn csv_file(name: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("bigquery-adbc-{name}-{}.csv", std::process::id()));
    std::fs::write(&path, b"id\n1\n").unwrap();
    path
}

/// One dbt-core 2.0 seed of `csv` into `destination`, on a fresh statement.
fn seed(
    connection: &mut BigQueryConnection,
    csv: &Path,
    destination: &str,
    dry_run: bool,
) -> Result<Option<i64>> {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let mut statement = connection.new_statement().unwrap();
    for (option, value) in [
        (
            BigQueryStatementOption::CsvFilepath,
            OptionValue::String(csv.to_str().unwrap().to_owned()),
        ),
        (
            BigQueryStatementOption::CsvSchema,
            OptionValue::Bytes(crate::arrow_ipc_bigquery::encode_schema_fixture(&schema)),
        ),
        (
            BigQueryStatementOption::DestinationTable,
            destination.into(),
        ),
        (BigQueryStatementOption::DryRun, dry_run.to_string().into()),
    ] {
        set_option(&mut statement, option, value);
    }
    statement.set_sql_query("").unwrap();
    statement.execute_update()
}

/// One dbt-core 2.0 `copy_partitions` copy, on a fresh statement.
fn copy(
    connection: &mut BigQueryConnection,
    source: &str,
    destination: &str,
    dry_run: bool,
) -> Result<Option<i64>> {
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        source,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        destination,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::DryRun,
        dry_run.to_string(),
    );
    statement.set_sql_query("").unwrap();
    statement.execute_update()
}

/// Asserts that `submission` and `poll` address one job in `location`.
#[track_caller]
fn assert_job_in(submission: &CapturedRequest, poll: &CapturedRequest, location: &str) {
    let reference = submitted_reference(submission);
    assert_eq!(reference["location"], location, "{}", submission.line);
    assert!(poll.line.starts_with("GET "), "{}", poll.line);
    assert_eq!(
        poll.path(),
        format!(
            "/bigquery/v2/projects/{}/jobs/{}?location={location}",
            reference["projectId"].as_str().unwrap(),
            reference["jobId"].as_str().unwrap()
        )
    );
}

/// A dbt profile may omit `location`. A seed then runs where its destination dataset is, found
/// with one `datasets.get` asking only for the location, and a second seed into the same dataset
/// reuses it.
#[test]
fn seeds_without_a_location_run_where_the_destination_dataset_is() {
    let (endpoint, server) = bigquery(5, vec![located(DATASET_LOCATION)], vec![done(), done()]);
    let mut connection = unlocated_connection_at(&endpoint);
    let csv = csv_file("unlocated-seed");

    let first = seed(&mut connection, &csv, "data_set.first", false);
    let second = seed(&mut connection, &csv, "data_set.second", false);
    std::fs::remove_file(csv).unwrap();
    assert_eq!(first.unwrap(), Some(1));
    assert_eq!(second.unwrap(), Some(1));

    let captured = server.join().unwrap();
    assert!(captured[0].line.starts_with("GET "), "{}", captured[0].line);
    assert_eq!(
        captured[0].path(),
        "/bigquery/v2/projects/fixture-project/datasets/data_set?fields=location"
    );
    for (submission, poll) in [(&captured[1], &captured[2]), (&captured[3], &captured[4])] {
        assert!(
            submission
                .line
                .starts_with("POST /upload/bigquery/v2/projects/fixture-project/jobs"),
            "{}",
            submission.line
        );
        assert_job_in(submission, poll, DATASET_LOCATION);
    }
}

/// A copy runs where its destination dataset is; a source elsewhere fails with BigQuery's own
/// error.
#[test]
fn a_copy_without_a_location_runs_where_the_destination_dataset_is() {
    let (endpoint, server) = bigquery(3, vec![located(DATASET_LOCATION)], vec![done()]);
    let mut connection = unlocated_connection_at(&endpoint);

    assert_eq!(
        copy(
            &mut connection,
            "data-project.staging.tmp_model$20240101",
            "data-project.reporting.model$20240101",
            false,
        )
        .unwrap(),
        Some(1)
    );

    let captured = server.join().unwrap();
    assert_eq!(
        captured[0].path(),
        "/bigquery/v2/projects/data-project/datasets/reporting?fields=location"
    );
    assert!(
        captured[1]
            .line
            .starts_with("POST /bigquery/v2/projects/fixture-project/jobs"),
        "{}",
        captured[1].line
    );
    assert_job_in(&captured[1], &captured[2], DATASET_LOCATION);
}

/// A configured location always wins, so no dataset is looked up.
#[test]
fn a_configured_location_is_used_without_a_dataset_lookup() {
    let (endpoint, server) = bigquery(4, Vec::new(), vec![done(), done()]);
    let mut connection = test_connection_at(&endpoint);
    let csv = csv_file("located-seed");

    let seeded = seed(&mut connection, &csv, "data_set.seed", false);
    std::fs::remove_file(csv).unwrap();
    seeded.unwrap();
    copy(
        &mut connection,
        "data-project.staging.source",
        "data-project.reporting.destination",
        false,
    )
    .unwrap();

    let captured = server.join().unwrap();
    assert_job_in(&captured[0], &captured[1], "EU");
    assert_job_in(&captured[2], &captured[3], "EU");
}

/// Neither dry run submits a job, so neither needs a location or looks one up.
#[test]
fn dry_runs_without_a_location_send_nothing() {
    let (endpoint, server) = capture_requests(Vec::new());
    let mut connection = unlocated_connection_at(&endpoint);
    let csv = csv_file("unlocated-dry-run");

    let seeded = seed(&mut connection, &csv, "data_set.seed", true);
    std::fs::remove_file(csv).unwrap();
    assert_eq!(seeded.unwrap(), None);
    let failure = copy(
        &mut connection,
        "data-project.staging.source",
        "data-project.reporting.destination",
        true,
    )
    .unwrap_err();

    assert_eq!(failure.status, Status::NotImplemented);
    assert!(server.join().unwrap().is_empty());
}

/// Only seeds and copies look a location up first. A query that needs a job is submitted without
/// one, as BigQuery infers it for `jobs.query`, and the answer's location is where it is polled
/// (G48).
#[test]
fn a_query_job_without_a_location_runs_where_bigquery_infers() {
    let mut submitted = JsonValue::Null;
    let (endpoint, server) = capture_requests_responding(2, move |request| {
        if submitted.is_null() {
            submitted = submitted_reference(request);
            submitted["location"] = json!("asia-northeast1");
            return Some(ok(
                json!({"jobReference": submitted, "status": {"state": "RUNNING"}}),
            ));
        }
        Some(ok(
            json!({"jobComplete": true, "jobReference": submitted, "schema": {"fields": []}}),
        ))
    });
    let mut connection = unlocated_connection_at(&endpoint);
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        "data_set.result",
    );
    statement.set_sql_query("SELECT 1").unwrap();

    statement.execute_update().unwrap();

    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    let reference = submitted_reference(&requests[0]);
    assert!(reference.get("location").is_none(), "{reference}");
    assert_eq!(
        requests[0].body["configuration"]["query"]["destinationTable"]["datasetId"],
        "data_set"
    );
    assert!(
        requests[1].path().starts_with(&format!(
            "/bigquery/v2/projects/fixture-project/queries/{}?",
            reference["jobId"].as_str().unwrap()
        )) && requests[1].path().ends_with("&location=asia-northeast1"),
        "{}",
        requests[1].path()
    );
}

/// A failed lookup is reported as the dataset's failure, with its status, and submits nothing.
#[test]
fn a_failed_location_lookup_names_the_dataset_and_submits_no_job() {
    let (endpoint, server) = bigquery(
        1,
        vec![(
            404,
            error_body("notFound", "Not found: Dataset data-project:missing"),
        )],
        Vec::new(),
    );
    let mut connection = unlocated_connection_at(&endpoint);

    let failure = copy(
        &mut connection,
        "data-project.staging.source",
        "data-project.missing.destination",
        false,
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(
        failure.message,
        "failed to find the location of BigQuery dataset data-project.missing for the table copy \
         job: notFound: Not found: Dataset data-project:missing (HTTP 404); set BigQuery option \
         bigquery.location to submit the job without this lookup"
    );
    assert_eq!(server.join().unwrap().len(), 1);
}

/// A dataset dropped and recreated in another location makes a cached location wrong. The job
/// that fails in it is not resubmitted, but the next job looks the location up again.
#[test]
fn a_failed_job_forgets_the_location_it_ran_in() {
    let (endpoint, server) = bigquery(
        6,
        vec![located("US"), located("EU")],
        vec![
            json!({
                "state": "DONE",
                "errorResult": {
                    "reason": "notFound",
                    "message": "Not found: Dataset data-project:reporting was not found in \
                                location US"
                }
            }),
            done(),
        ],
    );
    let mut connection = unlocated_connection_at(&endpoint);
    let mut run = || {
        copy(
            &mut connection,
            "data-project.reporting.source",
            "data-project.reporting.destination",
            false,
        )
    };

    assert_eq!(run().unwrap_err().status, Status::NotFound);
    assert_eq!(run().unwrap(), Some(1));

    let captured = server.join().unwrap();
    for (first, location) in [(0, "US"), (3, "EU")] {
        assert_eq!(
            captured[first].path(),
            "/bigquery/v2/projects/data-project/datasets/reporting?fields=location"
        );
        assert_job_in(&captured[first + 1], &captured[first + 2], location);
    }
}
