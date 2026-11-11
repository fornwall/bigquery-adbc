use std::path::{Path, PathBuf};
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
use crate::test_support::errors::assert_failure;
use crate::test_support::http::{
    CapturedRequest, CapturedServer, capture_requests, capture_requests_responding,
    capture_requests_until_stopped, error_body, ok,
};
use crate::test_support::options::set_option;
use crate::{BigQueryConnection, BigQueryStatement, BigQueryStatementOption};

const WHAT: &str = "table copy";

fn requested() -> JobReference {
    JobReference::new("billing-project", Some("EU"), "job-1".to_owned())
}

fn job(body: &str) -> super::LoadOrCopyJob {
    super::decode_job(body.as_bytes(), WHAT).unwrap()
}

/// The identity check is what keeps a recovered submission from being some other job: only the
/// project and ID that were asked for are accepted, an inferred location is kept, and an explicit
/// one may differ only in case.
#[test]
fn a_job_response_must_identify_the_job_that_was_requested() {
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
        let failure = resolved_reference(&job(body), &requested(), WHAT).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!("the BigQuery {WHAT} job response {expected}")
        );
    }

    let location = |body: &str, requested: &JobReference| {
        resolved_reference(&job(body), requested, WHAT)
            .unwrap()
            .location
    };
    let inferred = r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1"}}"#;
    let unlocated = JobReference::new("billing-project", None, "job-1".to_owned());
    assert_eq!(location(inferred, &unlocated), None);
    assert_eq!(location(inferred, &requested()).as_deref(), Some("EU"));
    let recased =
        r#"{"jobReference":{"projectId":"billing-project","jobId":"job-1","location":"eu"}}"#;
    assert_eq!(location(recased, &requested()).as_deref(), Some("eu"));
}

/// A poll accepts only the three states BigQuery runs a job through, and says which job and which
/// states when it sees anything else.
#[test]
fn a_polled_job_reports_a_state_it_cannot_act_on() {
    let polled = |status: &str| {
        let body = format!(
            r#"{{"jobReference":{{"projectId":"billing-project","jobId":"job-1","location":"EU"}}{status}}}"#
        );
        decode_polled_job(body.as_bytes(), &requested(), WHAT)
    };
    for state in ["PENDING", "RUNNING", "DONE"] {
        polled(&format!(r#","status":{{"state":"{state}"}}"#)).unwrap();
    }
    for (status, problem) in [
        (
            r#","status":{"state":"MYSTERY"}"#,
            "returned unknown state \"MYSTERY\"; expected PENDING, RUNNING, or DONE",
        ),
        ("", "contained no state"),
    ] {
        let failure = polled(status).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!("BigQuery {WHAT} job billing-project:EU.job-1 poll {problem}")
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
    let running = job(&polled("RUNNING").to_string());

    let finished = runtime
        .block_on(wait_for_job(
            &client,
            &operation,
            &requested(),
            running,
            WHAT,
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
    let mut failed = job(r#"{
        "jobReference":{"projectId":"billing-project","jobId":"job-1","location":"EU"},
        "status": {
            "state": "DONE",
            "errorResult": {"reason": "stopped", "message": "primary failure"},
            "errors": [
                {"reason": "invalidQuery", "message": "secondary failure"},
                {"reason": "stopped", "message": "primary failure", "debugInfo": "x"}
            ]
        }
    }"#);
    let failure = validate_completed(&mut failed, WHAT).unwrap_err();
    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(
        failure.message,
        format!(
            "BigQuery {WHAT} job failed (job billing-project:EU.job-1): stopped: primary \
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

    // errorResult takes precedence over the state, which BigQuery can report before DONE.
    for (body, status, message) in [
        (
            r#"{"status":{"state":"RUNNING","errorResult":{"reason":"invalid","message":"bad"}}}"#,
            Status::InvalidArguments,
            format!("BigQuery {WHAT} job failed: invalid: bad"),
        ),
        (
            r#"{"status":{"state":"RUNNING"}}"#,
            Status::InvalidData,
            format!("BigQuery {WHAT} job response was not complete"),
        ),
        (
            r#"{}"#,
            Status::InvalidData,
            format!("completed BigQuery {WHAT} job has no status"),
        ),
    ] {
        let failure = validate_completed(&mut job(body), WHAT).unwrap_err();
        assert_eq!((failure.status, failure.message), (status, message));
    }
}

/// A load reports the rows it wrote and a copy the rows it copied, or ADBC's unknown count when
/// BigQuery reports none (a copy it satisfies as metadata) or a negative one, which adbc.h's
/// `rows_affected` cannot carry.
#[test]
fn a_completed_job_reports_its_row_count_when_bigquery_gives_one() {
    for (statistics, loaded, copied) in [
        (r#"{"load":{"outputRows":"9"}}"#, Some(9), None),
        (r#"{"copy":{"copiedRows":"7"}}"#, None, Some(7)),
        (
            r#"{"load":{"outputRows":"0"},"copy":{"copiedRows":"0"}}"#,
            Some(0),
            Some(0),
        ),
        (r#"{"load":{},"copy":{}}"#, None, None),
        (
            r#"{"load":{"outputRows":"-5"},"copy":{"copiedRows":"-1"}}"#,
            None,
            None,
        ),
    ] {
        let mut job = job(&format!(
            r#"{{"status":{{"state":"DONE"}},"statistics":{statistics}}}"#
        ));
        validate_completed(&mut job, WHAT).unwrap();
        assert_eq!((job.output_rows(), job.copied_rows()), (loaded, copied));
    }

    let mut missing = job(
        r#"{"status":{"state":"DONE","errorResult":{"reason":"notFound","message":"Not found: Table p:d.missing"}}}"#,
    );
    let failure = validate_completed(&mut missing, WHAT).unwrap_err();
    // dbt 2.0 classifies a missing relation by this prefix.
    assert_failure(&failure, Status::NotFound, &["notFound:"]);
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
) -> (String, CapturedServer) {
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

/// The rows affected and the `BIGQUERY:query_id` of an option-only statement, which dbt 2.0
/// finalizes with an empty query.
fn run(mut statement: BigQueryStatement) -> Result<(i64, Option<String>)> {
    statement.set_sql_query("").unwrap();
    let result = statement.execute_partitions()?;
    let query_id = result
        .schema
        .metadata()
        .get(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID)
        .cloned();
    Ok((result.rows_affected, query_id))
}

/// One dbt 2.0 seed of `csv` into `destination`, on a fresh statement.
fn seed(
    connection: &mut BigQueryConnection,
    csv: &Path,
    destination: &str,
    dry_run: bool,
) -> Result<(i64, Option<String>)> {
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
    run(statement)
}

/// One dbt 2.0 `copy_partitions` copy, on a fresh statement.
fn copy(
    connection: &mut BigQueryConnection,
    source: &str,
    destination: &str,
    dry_run: bool,
) -> Result<(i64, Option<String>)> {
    let mut statement = connection.new_statement().unwrap();
    for (option, value) in [
        (BigQueryStatementOption::CopyTableSource, source),
        (BigQueryStatementOption::CopyTableDestination, destination),
        (
            BigQueryStatementOption::DryRun,
            if dry_run { "true" } else { "false" },
        ),
    ] {
        set_option(&mut statement, option, value);
    }
    run(statement)
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
    assert_eq!(first.unwrap().0, 1);
    assert_eq!(second.unwrap().0, 1);

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

    let (rows, _) = copy(
        &mut connection,
        "data-project.staging.tmp_model$20240101",
        "data-project.reporting.model$20240101",
        false,
    )
    .unwrap();
    assert_eq!(rows, 1);

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

/// A configured location always wins, so no dataset is looked up. Each job's ID is reported the
/// way a query result reports its job, which dbt 2.0 records as the statement's query ID.
#[test]
fn a_configured_location_is_used_without_a_dataset_lookup() {
    let (endpoint, server) = bigquery(4, Vec::new(), vec![done(), done()]);
    let mut connection = test_connection_at(&endpoint);
    let csv = csv_file("located-seed");

    let seeded = seed(&mut connection, &csv, "data_set.seed", false);
    std::fs::remove_file(csv).unwrap();
    let (_, seed_id) = seeded.unwrap();
    let (_, copy_id) = copy(
        &mut connection,
        "data-project.staging.source",
        "data-project.reporting.destination",
        false,
    )
    .unwrap();

    let captured = server.join().unwrap();
    for ((submission, poll), reported) in [
        ((&captured[0], &captured[1]), seed_id),
        ((&captured[2], &captured[3]), copy_id),
    ] {
        assert_job_in(submission, poll, "EU");
        assert_eq!(
            reported.as_deref(),
            submitted_reference(submission)["jobId"].as_str()
        );
    }
    assert!(
        captured[0].line.contains("/upload/"),
        "{}",
        captured[0].line
    );
    assert_eq!(
        captured[2].body["configuration"]["copy"]["writeDisposition"], "WRITE_EMPTY",
        "an unset copy disposition is the non-destructive one"
    );
}

/// Neither dry run submits a job, so neither needs a location or looks one up. BigQuery answers
/// `dryRun` on a copy job with `400 Dry run not supported for copy job`, so that is refused.
#[test]
fn dry_runs_without_a_location_send_nothing() {
    let (endpoint, server) = capture_requests(Vec::new());
    let mut connection = unlocated_connection_at(&endpoint);
    let csv = csv_file("unlocated-dry-run");

    let seeded = seed(&mut connection, &csv, "data_set.seed", true);
    std::fs::remove_file(csv).unwrap();
    assert_eq!(seeded.unwrap(), (-1, None));
    let failure = copy(
        &mut connection,
        "data-project.staging.source",
        "data-project.reporting.destination",
        true,
    )
    .unwrap_err();

    assert_failure(
        &failure,
        Status::NotImplemented,
        &[BigQueryStatementOption::DryRun.as_str()],
    );
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
            submitted["location"] = json!(DATASET_LOCATION);
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

    let requests = server.served(2);
    let reference = submitted_reference(&requests[0]);
    assert!(reference.get("location").is_none(), "{reference}");
    assert!(
        requests[1].path().starts_with(&format!(
            "/bigquery/v2/projects/fixture-project/queries/{}?",
            reference["jobId"].as_str().unwrap()
        )) && requests[1]
            .path()
            .ends_with(&format!("&location={DATASET_LOCATION}")),
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
    server.served(1);
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
    assert_eq!(run().unwrap().0, 1);

    let captured = server.join().unwrap();
    for (first, location) in [(0, "US"), (3, "EU")] {
        assert_eq!(
            captured[first].path(),
            "/bigquery/v2/projects/data-project/datasets/reporting?fields=location"
        );
        assert_job_in(&captured[first + 1], &captured[first + 2], location);
    }
}
