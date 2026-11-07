use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use adbc_core::CancelHandle as _;
use adbc_core::error::{Result, Status};
use serde_json::{Value as JsonValue, json};

use super::{JobClient, JobReference, JobRequest, QueryAnswer, QueryResultsRead};
use crate::cancellation::OperationCancelHandle;
use crate::clients::{ClientState, DatasetKey};
use crate::config::ServiceEndpointKind;
use crate::error::{DETAIL_JOB_ID, has_job_verdict};
use crate::rest::request::RequestStart;
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::cancellation::{job_client, operation, operation_at};
use crate::test_support::errors::{assert_failure, assert_message};
use crate::test_support::http::{
    CapturedRequest, StoppableServer, capture_requests, capture_requests_gated,
    capture_requests_responding, capture_requests_until_stopped, error_body, ok,
};
use crate::test_support::identifiers::project;

const MAX_ATTEMPTS: usize = crate::rest::retry::MAX_ATTEMPTS as usize;
const MAX_RECONCILIATION_ATTEMPTS: usize = super::MAX_RECONCILIATION_ATTEMPTS as usize;

/// A long backoff for the test that cancels a submission during backoff; cancellation cuts it
/// short. `cancellation_during_ambiguous_retry_backoff_confirms_the_requested_identity` explains
/// why the outcome holds even when jitter lands the cancel outside it.
const CANCELLED_BACKOFF: Duration = Duration::from_secs(5);

fn job_request(reference: JobReference, dry_run: bool) -> Result<JobRequest> {
    let configuration = json!({"query": {"query": "select 1", "useLegacySql": false}});
    JobRequest::new(reference, dry_run, &configuration, "the test job")
}

fn request(id: &str, location: Option<&str>, dry_run: bool) -> JobRequest {
    let reference = JobReference::new("billing/project", location, id.to_owned());
    job_request(reference, dry_run).unwrap()
}

/// `count` answers to a request for a job that is not visible.
fn invisible(count: usize) -> Vec<(u16, JsonValue)> {
    vec![(404, error_body("notFound", "Not found: Job")); count]
}

/// A provisional cancel that spends its attempts on an invisible job, then one that is accepted.
fn cancels_after_confirmation() -> Vec<(u16, JsonValue)> {
    let mut responses = invisible(MAX_ATTEMPTS);
    responses.push(ok(json!({})));
    responses
}

/// Asserts that `server` is sent nothing, allowing for a cancel sent in the background.
fn assert_unused(server: StoppableServer) {
    thread::sleep(Duration::from_millis(200));
    assert!(server.stop().is_empty());
}

/// Stages `responses` and submits one `jobs.query` for `project`, returning its answer and the
/// requests that reached the mock.
fn jobs_query(
    project: &str,
    responses: Vec<(u16, JsonValue)>,
) -> (Result<QueryAnswer>, Vec<CapturedRequest>) {
    let (endpoint, captured) = capture_requests(responses);
    let (runtime, jobs) = job_client(&endpoint);
    let body = bytes::Bytes::from_static(b"{}");
    let answer = runtime.block_on(jobs.query(None, project, body, &RequestStart::default()));
    (answer, captured.join().unwrap())
}

#[test]
fn job_references_adopt_the_service_resolved_project_number() {
    let requested = JobReference::new("123456789", Some("us"), "job-a".to_owned());
    let returned = JobReference::new("canonical-project", Some("US"), "job-a".to_owned());
    let canonical = requested.confirm(Some(&returned), "response").unwrap();
    assert_eq!(canonical, returned);

    for unexpected in [
        JobReference::new("987654321", Some("US"), "job-a".to_owned()),
        JobReference::new("canonical-project", Some("US"), "other-job".to_owned()),
        JobReference::new("canonical-project", Some("EU"), "job-a".to_owned()),
        JobReference::new("", Some("US"), "job-a".to_owned()),
    ] {
        assert_eq!(
            requested
                .confirm(Some(&unexpected), "response")
                .unwrap_err()
                .status,
            Status::InvalidData,
            "{unexpected:?}"
        );
    }
    for unexpected_project in ["other-project", "123456789"] {
        let unexpected = JobReference::new(unexpected_project, Some("US"), "job-a".to_owned());
        assert_eq!(
            canonical
                .confirm(Some(&unexpected), "response")
                .unwrap_err()
                .status,
            Status::InvalidData
        );
    }
    for unsafe_project in ["project/path", "project\\path", "project\n", ".", ".."] {
        let unexpected = JobReference::new(unsafe_project, Some("US"), "job-a".to_owned());
        assert_eq!(
            requested
                .confirm(Some(&unexpected), "response")
                .unwrap_err()
                .status,
            Status::InvalidData
        );
    }
}

/// Submits copy job `id` in `EU` to `endpoint`, under a cancellation scope nothing cancels.
fn insert_copy(endpoint: &str, id: &str) -> Result<bytes::Bytes> {
    let (runtime, client) = job_client(endpoint);
    let (_cancellation, operation) = operation(&runtime, ClientState::for_test());
    let response =
        runtime.block_on(client.insert(Some(&operation), &request(id, Some("EU"), false), "copy"));
    operation.finish();
    response
}

#[test]
fn successful_submissions_preserve_the_callers_distinct_ids_and_location() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({"status": {"state": "DONE"}, "marker": "first"})),
        ok(json!({"status": {"state": "DONE"}, "marker": "second"})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let (_cancellation, operation) = operation(&runtime, ClientState::for_test());

    let first = request("job/one", Some("EU west/1"), false);
    let second = request("job-two", Some("EU west/1"), false);
    let first_response = runtime
        .block_on(client.insert(Some(&operation), &first, "test"))
        .unwrap();
    let second_response = runtime
        .block_on(client.insert(Some(&operation), &second, "test"))
        .unwrap();
    operation.finish();

    assert_eq!(
        serde_json::from_slice::<JsonValue>(&first_response).unwrap()["marker"],
        "first"
    );
    assert_eq!(
        serde_json::from_slice::<JsonValue>(&second_response).unwrap()["marker"],
        "second"
    );
    let captured = captured.served(2);
    for request in &captured {
        assert_eq!(
            request.path(),
            "/bigquery/v2/projects/billing%2Fproject/jobs"
        );
        assert_eq!(request.body["jobReference"]["projectId"], "billing/project");
        assert_eq!(request.body["jobReference"]["location"], "EU west/1");
    }
    assert_eq!(captured[0].body["jobReference"]["jobId"], "job/one");
    assert_eq!(captured[1].body["jobReference"]["jobId"], "job-two");
    assert_eq!(
        captured[0].body["configuration"]["query"]["useLegacySql"],
        false
    );
}

#[test]
fn duplicate_submission_tolerates_visibility_and_transient_get_failures() {
    let (endpoint, captured) = capture_requests(vec![
        // A configured location wins over the one a 409 names.
        (
            409,
            error_body(
                "duplicate",
                "Already Exists: Job billing/project:asia-northeast1.job/ambiguous",
            ),
        ),
        (404, error_body("notFound", "not visible")),
        (500, error_body("backendError", "later")),
        ok(json!({"status": {"state": "DONE"}, "recovered": true})),
    ]);
    let response = insert_copy(&endpoint, "job/ambiguous").unwrap();

    assert_eq!(
        serde_json::from_slice::<JsonValue>(&response).unwrap()["recovered"],
        true
    );
    let captured = captured.served(4);
    assert!(
        captured[0]
            .line
            .starts_with("POST /bigquery/v2/projects/billing%2Fproject/jobs ")
    );
    for request in &captured[1..] {
        assert!(request.line.starts_with(
            "GET /bigquery/v2/projects/billing%2Fproject/jobs/job%2Fambiguous?location=EU "
        ));
    }
}

/// The job resource BigQuery answers for job `id` of `billing/project` running in `location`.
fn job_in(id: &str, location: &str) -> JsonValue {
    json!({
        "jobReference": {"projectId": "billing/project", "jobId": id, "location": location},
        "status": {"state": "RUNNING"}
    })
}

/// A dataset a location-less request names as where its job may be.
fn dataset(name: &str) -> DatasetKey {
    (project("data-project"), Arc::from(name))
}

/// A client whose location-less submissions find their job in dataset locations, looked up at
/// `endpoint`, and the state that records them.
fn locating_client(runtime: &SharedRuntime, endpoint: &str) -> (JobClient, Arc<ClientState>) {
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint);
    let client = JobClient::for_test(endpoint)
        .with_dataset_locations(state.dataset_locations(runtime).unwrap());
    (client, state)
}

/// Submits a query job under a cancellation scope nothing cancels.
fn insert_query(
    runtime: &SharedRuntime,
    client: &JobClient,
    request: &JobRequest,
) -> Result<JsonValue> {
    let (_cancellation, operation) = operation(runtime, ClientState::for_test());
    let response = runtime.block_on(client.insert(Some(&operation), request, "query"));
    operation.finish();
    response.map(|body| serde_json::from_slice(&body).unwrap())
}

/// Every attempt of a submission whose answers are all lost to a transient failure.
fn lost_submissions() -> Vec<(u16, JsonValue)> {
    vec![(503, error_body("backendError", "try later")); MAX_ATTEMPTS]
}

/// Every attempt to fetch a job that is not where the request looked.
fn not_found_here() -> Vec<(u16, JsonValue)> {
    invisible(MAX_RECONCILIATION_ATTEMPTS)
}

/// A job submitted without a location runs where BigQuery infers. The replay after a lost answer
/// is identical, so its 409 names the job in that location, where it is fetched.
#[test]
fn a_conflict_names_where_a_job_without_a_location_runs() {
    let mut attempt = 0;
    let (endpoint, captured) = capture_requests_responding(3, move |_| {
        attempt += 1;
        match attempt {
            1 => None,
            2 => Some((
                409,
                error_body(
                    "duplicate",
                    "Already Exists: Job billing/project:asia-northeast1.job-a",
                ),
            )),
            _ => Some(ok(job_in("job-a", "asia-northeast1"))),
        }
    });
    let (runtime, client) = job_client(&endpoint);

    let response = insert_query(&runtime, &client, &request("job-a", None, false)).unwrap();

    assert_eq!(response["jobReference"]["location"], "asia-northeast1");
    let captured = captured.served(3);
    assert!(captured[0].body["jobReference"].get("location").is_none());
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert!(
        captured[2].line.starts_with(
            "GET /bigquery/v2/projects/billing%2Fproject/jobs/job-a?location=asia-northeast1 "
        ),
        "{}",
        captured[2].line
    );
}

/// The 409 resolves a numeric project while naming the missing region. Recovery and cancellation
/// must both use that server identity rather than rejecting it or searching another region.
#[test]
fn a_numeric_project_conflict_recovers_and_cancels_the_canonical_job() {
    let reference = JobReference::new("123456789", None, "job-a".to_owned());
    let request = job_request(reference.clone(), false).unwrap();
    let duplicate_message = "Already Exists: Job domain:canonical:asia-northeast1.job-a";
    let (endpoint, captured) = capture_requests(vec![
        (409, error_body("duplicate", duplicate_message)),
        ok(json!({
            "jobReference": {
                "projectId": "domain:canonical", "jobId": "job-a",
                "location": "asia-northeast1"
            },
            "status": {"state": "RUNNING"}
        })),
        ok(json!({})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let (cancellation, operation) = operation_at(&runtime, &endpoint);
    runtime
        .block_on(client.insert(Some(&operation), &request, "test"))
        .unwrap();
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    operation.finish();

    let captured = captured.served(3);
    assert_eq!(captured[0].path(), "/bigquery/v2/projects/123456789/jobs");
    assert_eq!(
        captured[1].path(),
        "/bigquery/v2/projects/domain%3Acanonical/jobs/job-a?location=asia-northeast1"
    );
    assert_eq!(
        captured[2].path(),
        "/bigquery/v2/projects/domain%3Acanonical/jobs/job-a/cancel?location=asia-northeast1"
    );
    assert!(
        super::duplicate(
            &serde_json::to_vec(&error_body(
                "duplicate",
                "Already Exists: Job domain:canonical:asia-northeast1.other-job",
            ))
            .unwrap(),
            &reference,
        )
        .is_none()
    );
}

/// Only BigQuery's exact wording for this job names a location. Anything else is looked for as
/// though the answer were lost, starting without a location.
#[test]
fn a_conflict_that_does_not_name_this_job_exactly_names_no_location() {
    for message in [
        "exists",
        "Already Exists: Job billing/project:asia-northeast1.other-job",
        "Already Exists: Job other-project:asia-northeast1.job-a",
        "Already Exists: Job billing/project:asia northeast1.job-a",
        "Already Exists: Job billing/project:.job-a",
    ] {
        let (endpoint, captured) = capture_requests(vec![
            (409, error_body("duplicate", message)),
            ok(job_in("job-a", "US")),
        ]);
        let (runtime, client) = job_client(&endpoint);

        let response = insert_query(&runtime, &client, &request("job-a", None, false)).unwrap();

        assert_eq!(response["jobReference"]["location"], "US", "{message}");
        let captured = captured.join().unwrap();
        assert_eq!(
            captured[1].path(),
            "/bigquery/v2/projects/billing%2Fproject/jobs/job-a",
            "{message}"
        );
    }
}

/// `jobs.get` without a location finds a job in the US or EU multi-region.
#[test]
fn a_lost_answer_without_a_location_finds_a_multi_region_job() {
    let mut responses = lost_submissions();
    responses.push(ok(job_in("job-b", "EU")));
    let (endpoint, captured) = capture_requests(responses);
    let (runtime, client) = job_client(&endpoint);

    let response = insert_query(&runtime, &client, &request("job-b", None, false)).unwrap();

    assert_eq!(response["jobReference"]["location"], "EU");
    let captured = captured.served(MAX_ATTEMPTS + 1);
    assert_eq!(
        captured[MAX_ATTEMPTS].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/job-b"
    );
}

/// A single-region job is looked for in the location of each dataset the request names, skipping
/// the multi-regions the lookup without a location already covered.
#[test]
fn a_lost_answer_without_a_location_finds_the_job_in_a_named_datasets_location() {
    let mut responses = lost_submissions();
    responses.extend(not_found_here());
    responses.push(ok(json!({"location": "US"})));
    responses.push(ok(json!({"location": "asia-northeast1"})));
    responses.push(ok(job_in("job-c", "asia-northeast1")));
    let (endpoint, captured) = capture_requests(responses);
    let runtime = new_runtime().unwrap();
    let (client, state) = locating_client(&runtime, &endpoint);
    let request =
        request("job-c", None, false).locatable_in([dataset("in_us"), dataset("in_asia")]);

    let response = insert_query(&runtime, &client, &request).unwrap();

    assert_eq!(response["jobReference"]["location"], "asia-northeast1");
    let captured = captured.join().unwrap();
    let lookups = MAX_ATTEMPTS + MAX_RECONCILIATION_ATTEMPTS;
    assert_eq!(captured.len(), lookups + 3);
    assert!(
        captured[lookups]
            .path()
            .starts_with("/bigquery/v2/projects/data-project/datasets/in_us?")
    );
    assert!(
        captured[lookups + 1]
            .path()
            .starts_with("/bigquery/v2/projects/data-project/datasets/in_asia?")
    );
    assert_eq!(
        captured[lookups + 2].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/job-c?location=asia-northeast1"
    );
    // The lookups are shared with seeds and copies.
    assert_eq!(
        state.dataset_location(&dataset("in_asia")).as_deref(),
        Some("asia-northeast1")
    );
}

/// A job found nowhere may still run, so the failure names it and says how to avoid the search.
#[test]
fn a_job_without_a_location_that_cannot_be_found_may_still_run() {
    let mut responses = lost_submissions();
    responses.extend(not_found_here());
    responses.push(ok(json!({"location": "asia-northeast1"})));
    responses.extend(not_found_here());
    responses.extend(vec![ok(json!({})); MAX_RECONCILIATION_ATTEMPTS]);
    let (endpoint, captured) = capture_requests(responses);
    let runtime = new_runtime().unwrap();
    let (client, _state) = locating_client(&runtime, &endpoint);
    let request = request("job-d", None, false).locatable_in([dataset("in_asia")]);

    let failure = insert_query(&runtime, &client, &request).unwrap_err();

    assert_eq!(failure.status, Status::IO);
    assert!(
        failure
            .message
            .starts_with("failed to submit the BigQuery query job billing/project:job-d")
            || failure
                .message
                .starts_with("BigQuery rejected the query job billing/project:job-d"),
        "{}",
        failure.message
    );
    assert!(
        failure.message.ends_with(
            "; BigQuery may still run job billing/project:job-d; recovery did not complete; \
             check the job before running it again, or set bigquery.location"
        ),
        "{}",
        failure.message
    );
    assert!(
        failure
            .details
            .unwrap()
            .contains(&(DETAIL_JOB_ID.to_owned(), b"billing/project:job-d".to_vec()))
    );
    captured.served(MAX_ATTEMPTS + 3 * MAX_RECONCILIATION_ATTEMPTS + 1);
}

#[test]
fn a_lost_regional_job_is_recovered_from_a_later_list_page() {
    use std::time::{SystemTime, UNIX_EPOCH};

    for project_id in ["billing/project", "123456789"] {
        let mut responses = vec![
            (503, error_body("backendError", "response lost")),
            (409, error_body("duplicate", "no parseable job location")),
        ];
        responses.extend(not_found_here());
        responses.extend([
            (503, error_body("backendError", "list temporarily unavailable")),
            ok(json!({"jobs": [
                job_in("job-listed-prefix", "asia-northeast1"),
                {"jobReference": {"projectId": "987654321", "jobId": "job-listed", "location": "EU"}},
                {}
            ], "nextPageToken": "next /+?"})),
            ok(json!({"jobs": [job_in("job-listed", "asia-northeast1")]})),
            ok(job_in("job-listed", "asia-northeast1")),
        ]);
        // Numeric project IDs are resolved by authenticated responses to a canonical text ID.
        if project_id == "123456789" {
            for (_, body) in &mut responses {
                for entry in body["jobs"].as_array_mut().into_iter().flatten() {
                    if entry["jobReference"]["projectId"] == "billing/project" {
                        entry["jobReference"]["projectId"] = json!("billing-project");
                    }
                }
                if body["jobReference"]["projectId"] == "billing/project" {
                    body["jobReference"]["projectId"] = json!("billing-project");
                }
            }
        }
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = job_client(&endpoint);
        let reference = JobReference::new(project_id, None, "job-listed".to_owned());
        let request = job_request(reference, false).unwrap();
        let before = SystemTime::now();
        let result = insert_query(&runtime, &client, &request).unwrap();
        let after = SystemTime::now();
        assert_eq!(result["jobReference"]["location"], "asia-northeast1");
        let requests = captured.join().unwrap();
        assert_eq!(requests[0].body, requests[1].body);
        let listing = &requests[2 + MAX_RECONCILIATION_ATTEMPTS];
        let url = reqwest::Url::parse(&format!("{endpoint}{}", listing.path())).unwrap();
        let params = url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(params["allUsers"], "false");
        assert_eq!(params["projection"], "minimal");
        assert_eq!(params["maxResults"], "100");
        let earliest = params["minCreationTime"].parse::<u128>().unwrap();
        let lower_bound = |time: SystemTime| {
            time.duration_since(UNIX_EPOCH)
                .unwrap()
                .saturating_sub(super::JOB_SEARCH_CLOCK_SKEW)
                .as_millis()
        };
        assert!((lower_bound(before)..=lower_bound(after)).contains(&earliest));
        assert_eq!(
            listing.path(),
            requests[3 + MAX_RECONCILIATION_ATTEMPTS].path()
        );
        assert!(
            requests[4 + MAX_RECONCILIATION_ATTEMPTS]
                .path()
                .ends_with("&pageToken=next%20%2F%2B%3F")
        );
        let canonical = if project_id == "123456789" {
            "billing-project"
        } else {
            "billing%2Fproject"
        };
        assert_eq!(
            requests.last().unwrap().path(),
            format!("/bigquery/v2/projects/{canonical}/jobs/job-listed?location=asia-northeast1")
        );
    }
}

#[test]
fn a_failed_location_listing_reports_that_the_job_may_still_run() {
    for (status, body, expected) in [
        (
            403,
            error_body("accessDenied", "listing refused"),
            Status::Unauthorized,
        ),
        (200, json!({"jobs": "invalid"}), Status::InvalidData),
    ] {
        let mut responses = vec![(409, error_body("duplicate", "no location"))];
        responses.extend(not_found_here());
        responses.push((status, body));
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = job_client(&endpoint);
        let failure =
            insert_query(&runtime, &client, &request("lost-job", None, false)).unwrap_err();
        assert_eq!(failure.status, expected);
        assert_message(
            &failure,
            &["BigQuery may still run job billing/project:lost-job"],
        );
        captured.served(MAX_RECONCILIATION_ATTEMPTS + 2);
    }
}

fn unlocated_submission(client: JobClient) -> super::InferredSubmission {
    let request = request("lost-job", None, false);
    super::InferredSubmission {
        client,
        operation: None,
        reference: request.reference,
        body: request.body,
        datasets: request.datasets,
        what: "query".to_owned(),
        job: "query job".to_owned(),
        start: Default::default(),
        submitted_at: std::time::SystemTime::now(),
    }
}

#[test]
fn a_listed_job_fetch_failure_keeps_the_recovered_identity() {
    let mut responses = vec![(409, error_body("duplicate", "no location"))];
    responses.extend(not_found_here());
    responses.extend([
        ok(json!({"jobs": [job_in("lost-job", "asia-northeast1")]})),
        (403, error_body("accessDenied", "fetch refused")),
    ]);
    let (endpoint, captured) = capture_requests(responses);
    let (runtime, client) = job_client(&endpoint);
    let failure = insert_query(&runtime, &client, &request("lost-job", None, false)).unwrap_err();
    assert_failure(
        &failure,
        Status::Unauthorized,
        &["recovery did not complete", "j=bq:asia-northeast1:lost-job"],
    );
    assert!(failure.details.unwrap().contains(&(
        DETAIL_JOB_ID.to_owned(),
        b"billing/project:asia-northeast1.lost-job".to_vec(),
    )));
    captured.served(MAX_RECONCILIATION_ATTEMPTS + 3);
}

#[test]
fn a_location_search_bounds_repeated_pagination_tokens() {
    let (endpoint, captured) = capture_requests_responding(
        usize::try_from(super::MAX_JOB_SEARCH_PAGES).unwrap(),
        |_| Some(ok(json!({"nextPageToken": "repeated"}))),
    );
    let (runtime, client) = job_client(&endpoint);
    let failure = runtime
        .block_on(unlocated_submission(client).locate_by_listing())
        .unwrap_err();
    assert_failure(&failure, Status::IO, &["jobs.list exceeded the page limit"]);
    captured.served(usize::try_from(super::MAX_JOB_SEARCH_PAGES).unwrap());
}

#[test]
fn a_location_search_deadline_includes_credentials_and_all_pages() {
    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let (endpoint, captured) = capture_requests(vec![ok(json!({"nextPageToken": "next"}))]);
    let mut client = JobClient::for_test(&endpoint);
    let (minting, mut minted) = tokio::sync::mpsc::unbounded_channel();
    client.credentials = slow_credentials(minting, Duration::from_secs(20));
    runtime.block_on(async {
        // Prevent automatic clock advancement during socket I/O; each request takes twenty
        // seconds to mint credentials, so the second page must hit the shared deadline.
        let (hold, held) = mpsc::channel::<()>();
        let inhibitor = tokio::task::spawn_blocking(move || held.recv());
        let submission = unlocated_submission(client);
        let mut search = pin!(submission.locate_by_listing());
        let start = tokio::time::Instant::now();
        for advance in [Duration::from_secs(20), Duration::from_secs(10)] {
            tokio::select! {
                biased;
                result = &mut search => panic!("search finished before the deadline: {result:?}"),
                minted = minted.recv() => minted.unwrap(),
            }
            tokio::time::advance(advance).await;
        }
        let failure = search.await.unwrap_err();
        assert_failure(
            &failure,
            Status::Timeout,
            &["job location search deadline exceeded"],
        );
        assert_eq!(
            tokio::time::Instant::now() - start,
            super::JOB_SEARCH_TIMEOUT
        );
        drop(hold);
        let _ = inhibitor.await;
    });
    captured.served(1);
}

/// A rejection proves no job was created, so nothing is looked for.
#[test]
fn a_rejected_submission_without_a_location_is_not_looked_for() {
    let (endpoint, captured) =
        capture_requests(vec![(400, error_body("invalidQuery", "Syntax error"))]);
    let (runtime, client) = job_client(&endpoint);

    let failure = insert_query(&runtime, &client, &request("job-e", None, false)).unwrap_err();

    assert_eq!(failure.status, Status::InvalidArguments);
    captured.served(1);
}

/// A cancel before the answer is known first addresses the job without a location. The submission
/// still finishes, and the answer's location is where the job is cancelled.
#[test]
fn a_cancel_before_the_answer_cancels_the_job_where_the_answer_puts_it() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests(cancels_after_confirmation());
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let handle = OperationCancelHandle(cancellation);
    let (endpoint, inserts) =
        capture_requests_gated(vec![ok(job_in("job-f", "asia-northeast1"))], move || {
            handle.try_cancel().unwrap();
            true
        });
    let client = JobClient::for_test(&endpoint);

    let failure = runtime
        .block_on(client.insert(Some(&operation), &request("job-f", None, false), "query"))
        .unwrap_err();

    assert_eq!(failure.status, Status::Cancelled);
    inserts.served(1);
    let cancels = cancels.join().unwrap();
    operation.finish();
    for cancel in &cancels[..MAX_ATTEMPTS] {
        assert_eq!(
            cancel.path(),
            "/bigquery/v2/projects/billing%2Fproject/jobs/job-f/cancel"
        );
    }
    assert_eq!(
        cancels[MAX_ATTEMPTS].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/job-f/cancel?location=asia-northeast1"
    );
}

/// A cancel whose submission then loses its answer looks for the job as an ambiguous submission
/// does, without submitting it again, and cancels it where it is found.
#[test]
fn a_cancel_before_a_lost_answer_finds_the_job_without_resubmitting_it() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests(cancels_after_confirmation());
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let handle = OperationCancelHandle(cancellation);
    let mut lookups = not_found_here().into_iter();
    let (endpoint, requests) =
        capture_requests_responding(MAX_RECONCILIATION_ATTEMPTS + 3, move |request| {
            if request.line.starts_with("POST ") {
                handle.try_cancel().unwrap();
                return None;
            }
            if request.path().contains("/datasets/") {
                return Some(ok(json!({"location": "asia-northeast1"})));
            }
            lookups
                .next()
                .or_else(|| Some(ok(job_in("job-g", "asia-northeast1"))))
        });
    let (client, _state) = locating_client(&runtime, &endpoint);
    let request = request("job-g", None, false).locatable_in([dataset("in_asia")]);

    let failure = runtime
        .block_on(client.insert(Some(&operation), &request, "query"))
        .unwrap_err();

    assert_eq!(failure.status, Status::Cancelled);
    let cancels = cancels.join().unwrap();
    operation.finish();
    assert_eq!(
        cancels[MAX_ATTEMPTS].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/job-g/cancel?location=asia-northeast1"
    );
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.line.starts_with("POST "))
            .count(),
        1
    );
    assert_eq!(
        requests.last().unwrap().path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/job-g?location=asia-northeast1"
    );
}

#[test]
fn a_recovered_location_remains_cancellable_when_the_job_fetch_fails() {
    for duplicate in [false, true] {
        let runtime = new_runtime().unwrap();
        let (cancel_endpoint, cancels) = capture_requests(cancels_after_confirmation());
        let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
        let handle = OperationCancelHandle(cancellation);
        let mut listings = 0;
        let request_count = if duplicate {
            2
        } else {
            MAX_RECONCILIATION_ATTEMPTS + 4
        };
        let (endpoint, requests) = capture_requests_responding(request_count, move |request| {
            if request.line.starts_with("POST ") {
                handle.try_cancel().unwrap();
                return duplicate.then(|| {
                    (
                        409,
                        error_body(
                            "duplicate",
                            "Already Exists: Job billing/project:asia-northeast1.lost-job",
                        ),
                    )
                });
            }
            if request.path().contains("/jobs?") {
                listings += 1;
                return Some((
                    200,
                    if listings == 1 {
                        json!({"unreachable": ["asia-northeast1"]})
                    } else {
                        json!({"jobs": [job_in("lost-job", "asia-northeast1")]})
                    },
                ));
            }
            Some(if request.path().contains("location=asia-northeast1") {
                (403, error_body("accessDenied", "fetch refused"))
            } else {
                (404, error_body("notFound", "not in a multi-region"))
            })
        });
        let failure = runtime
            .block_on(JobClient::for_test(&endpoint).insert(
                Some(&operation),
                &request("lost-job", None, false),
                "query",
            ))
            .unwrap_err();
        assert_eq!(failure.status, Status::Cancelled);
        let cancels = cancels.join().unwrap();
        let requests = requests.join().unwrap();
        operation.finish();
        assert_eq!(
            cancels.last().unwrap().path(),
            "/bigquery/v2/projects/billing%2Fproject/jobs/lost-job/cancel?location=asia-northeast1"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.line.starts_with("POST "))
                .count(),
            1
        );
    }
}

#[test]
fn cancellation_uses_refreshed_credentials_from_the_submission_client() {
    let (token_endpoint, tokens) = capture_requests(
        ["submission-token", "refreshed-token"]
            .map(|token| {
                ok(json!({
                    "access_token": token, "expires_in": 3600, "token_type": "Bearer"
                }))
            })
            .into(),
    );
    let (endpoint, requests) =
        capture_requests(vec![ok(job_in("refresh-job", "EU")), ok(json!({}))]);
    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (hold, held) = mpsc::channel::<()>();
        let inhibitor = tokio::task::spawn_blocking(move || held.recv());
        let credentials = google_cloud_auth::credentials::user_account::Builder::new(json!({
            "type": "authorized_user", "client_id": "client-id",
            "client_secret": "client-secret", "refresh_token": "refresh-token"
        }))
        .with_token_uri(format!("{token_endpoint}/token"))
        .build()
        .unwrap();
        let client = JobClient::new(
            credentials,
            Some(&endpoint),
            crate::rest::client::rest_client("refresh test").unwrap(),
        );
        // Retain the cancellation client before submission; both clones must share refreshes.
        let cancellation_client = client.clone();
        let request = request("refresh-job", Some("EU"), false);
        client.insert(None, &request, "query").await.unwrap();
        tokio::time::advance(Duration::from_secs(3601)).await;
        cancellation_client
            .cancel(request.reference(), "query")
            .await
            .unwrap();
        drop(hold);
        let _ = inhibitor.await;
    });
    let requests = requests.join().unwrap();
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer submission-token")
    );
    assert_eq!(
        requests[1].header("authorization"),
        Some("Bearer refreshed-token")
    );
    assert_eq!(
        requests[1].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/refresh-job/cancel?location=EU"
    );
    tokens.served(2);
}

#[test]
fn jobs_query_retries_the_identical_request_body() {
    let (endpoint, captured) = capture_requests(vec![
        (500, error_body("backendError", "later")),
        ok(json!({"jobComplete": true, "rows": []})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let body = bytes::Bytes::from_static(br#"{"query":"SELECT 1","requestId":"req_same"}"#);

    runtime
        .block_on(client.query(None, "billing/project", body, &RequestStart::default()))
        .unwrap();

    let captured = captured.served(2);
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert_eq!(captured[0].body["requestId"], "req_same");
    assert_eq!(
        captured[0].path(),
        "/bigquery/v2/projects/billing%2Fproject/queries"
    );
}

#[test]
fn cancelled_queries_stop_rejected_retries_but_recover_ambiguous_submissions() {
    let limited = || (429, error_body("rateLimitExceeded", "later"));
    let duplicate = || {
        (
            409,
            error_body("duplicate", "Already Exists: Job p:EU.cancelled-job"),
        )
    };
    for (responses, recover) in [
        (vec![limited()], false),
        (
            vec![
                (503, error_body("backendError", "later")),
                limited(),
                duplicate(),
            ],
            true,
        ),
        (
            vec![
                (403, error_body("responseTooLarge", "exceeds max row limit")),
                duplicate(),
            ],
            true,
        ),
    ] {
        let count = responses.len();
        let (cancel, wait_for_cancel) = mpsc::sync_channel::<OperationCancelHandle>(1);
        let (endpoint, captured) = capture_requests_gated(responses, move || {
            wait_for_cancel.recv().unwrap().try_cancel().unwrap();
            true
        });
        let (runtime, jobs) = job_client(&endpoint);
        let (cancellation, operation) = operation_at(&runtime, &endpoint);
        cancel.send(OperationCancelHandle(cancellation)).unwrap();
        let answer = runtime.block_on(jobs.query(
            Some(&operation),
            "p",
            bytes::Bytes::from_static(b"{}"),
            &RequestStart::default(),
        ));
        operation.finish();
        if recover {
            assert!(matches!(answer.unwrap(), QueryAnswer::Replayed(job)
                if job.job_id == "cancelled-job"));
        } else {
            assert_eq!(answer.unwrap_err().status, Status::Cancelled);
        }
        captured.served(count);
    }
}

/// BigQuery answers a replayed request ID whose first submission created a job with 409
/// `duplicate` naming that job; only a replay's 409 names the job to read, and only when the name
/// parses.
#[test]
fn a_replayed_jobs_query_answered_409_names_the_job_its_first_attempt_created() {
    const DUPLICATE: &str = "Already Exists: Job billing:project:US.job_Ab-1_c";
    let (answer, requests) = jobs_query(
        "billing:project",
        vec![
            (503, error_body("backendError", "later")),
            (409, error_body("duplicate", DUPLICATE)),
        ],
    );
    assert_eq!(requests.len(), 2);
    let QueryAnswer::Replayed(job) = answer.unwrap() else {
        panic!("expected the replayed job");
    };
    assert_eq!(
        job,
        JobReference::new("billing:project", Some("US"), "job_Ab-1_c".to_owned())
    );

    for (responses, what) in [
        (
            vec![(409, error_body("duplicate", DUPLICATE))],
            "a first attempt's 409",
        ),
        (
            vec![
                (503, error_body("backendError", "later")),
                (
                    409,
                    error_body("duplicate", "Already Exists: Job other:US.job_x"),
                ),
            ],
            "another project's job",
        ),
        (
            vec![
                (503, error_body("backendError", "later")),
                (
                    409,
                    error_body("duplicate", "Already Exists: Job billing:project:US.job x"),
                ),
            ],
            "an unparseable job ID",
        ),
    ] {
        let failure = jobs_query("billing:project", responses).0.unwrap_err();
        assert_eq!(failure.status, Status::AlreadyExists, "{what}");
    }
}

#[test]
fn a_replayed_numeric_project_query_adopts_its_canonical_project() {
    let (answer, captured) = jobs_query(
        "123456789",
        vec![
            (503, error_body("backendError", "later")),
            (
                409,
                error_body(
                    "duplicate",
                    "Already Exists: Job canonical-project:US.job_Ab-1_c",
                ),
            ),
        ],
    );
    let QueryAnswer::Replayed(job) = answer.unwrap() else {
        panic!("expected the replayed job");
    };
    assert_eq!(
        job,
        JobReference::new("canonical-project", Some("US"), "job_Ab-1_c".to_owned())
    );
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert!(
        captured
            .iter()
            .all(|request| request.path() == "/bigquery/v2/projects/123456789/queries")
    );
}

/// A 403 `responseTooLarge` comes from a query that ran and kept its job, so it is replayed once
/// to learn that job; a replay refused the same way is reported.
#[test]
fn a_jobs_query_whose_row_is_too_large_is_replayed_once_to_name_its_job() {
    let too_large = || (403, error_body("responseTooLarge", "exceeds max row limit"));
    let (answer, requests) = jobs_query(
        "p",
        vec![
            too_large(),
            (
                409,
                error_body("duplicate", "Already Exists: Job p:US.job_large"),
            ),
        ],
    );
    assert_eq!(requests.len(), 2);
    assert!(
        matches!(&answer, Ok(QueryAnswer::Replayed(job)) if job.job_id == "job_large"),
        "{answer:?}"
    );

    let (answer, requests) = jobs_query("p", vec![too_large(), too_large()]);
    assert_eq!(requests.len(), 2);
    assert_eq!(answer.unwrap_err().status, Status::InvalidArguments);
}

/// A statement whose every attempt ended without a verdict may have run, and a caller that sends
/// it again under a new request ID would run it twice, so the failure says so. BigQuery's answer to
/// a last attempt says nothing about an earlier one that lost its answer, and only attempts that
/// BigQuery refused throughout leave a rejection.
#[test]
fn an_exhausted_jobs_query_that_may_have_run_says_so() {
    const SUBMIT: &str = "failed to submit the BigQuery jobs.query request: ";
    const MAY_HAVE_RUN: &str =
        "; BigQuery may still have run this statement; check before running it again";
    let unavailable = Some((503, error_body("backendError", "the answer was lost")));
    let limited = Some((429, error_body("rateLimitExceeded", "slow down")));
    let mut answered_last = vec![unavailable.clone(); MAX_ATTEMPTS - 1];
    answered_last.push(limited.clone());
    for (answers, lead, reported, suffix) in [
        (
            vec![unavailable; MAX_ATTEMPTS],
            SUBMIT,
            "backendError: the answer was lost (HTTP 503)",
            MAY_HAVE_RUN,
        ),
        (vec![None; MAX_ATTEMPTS], SUBMIT, "", MAY_HAVE_RUN),
        (
            answered_last,
            SUBMIT,
            "rateLimitExceeded: slow down (HTTP 429)",
            MAY_HAVE_RUN,
        ),
        (
            vec![limited; MAX_ATTEMPTS],
            "BigQuery rejected the query: ",
            "rateLimitExceeded: slow down (HTTP 429)",
            "",
        ),
    ] {
        let mut answers = answers.into_iter();
        let (endpoint, captured) =
            capture_requests_responding(MAX_ATTEMPTS, move |_| answers.next().unwrap());
        let (runtime, jobs) = job_client(&endpoint);
        let failure = runtime
            .block_on(jobs.query(
                None,
                "p",
                bytes::Bytes::from_static(b"{}"),
                &RequestStart::default(),
            ))
            .unwrap_err();
        captured.served(MAX_ATTEMPTS);
        assert_eq!(failure.status, Status::IO, "{}", failure.message);
        assert!(failure.message.starts_with(lead), "{}", failure.message);
        assert!(
            failure.message.ends_with(&format!("{reported}{suffix}")),
            "{}",
            failure.message
        );
    }
}

/// A definitive answer to a replay is about the replay, not the attempt that lost its answer, so
/// it is no verdict on the statement. BigQuery refusing the first attempt still is.
#[test]
fn a_definitive_answer_to_a_replay_says_the_statement_may_have_run() {
    let (answer, requests) = jobs_query(
        "p",
        vec![
            (503, error_body("backendError", "the answer was lost")),
            (
                400,
                error_body("invalidQuery", "No active transaction found"),
            ),
        ],
    );
    assert_eq!(requests.len(), 2);
    let failure = answer.unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "failed to submit the BigQuery jobs.query request: invalidQuery: No active transaction \
         found (HTTP 400); BigQuery may still have run this statement; check before running it \
         again"
    );
}

/// An error document reads as a rejection, not a failure to submit. The `{reason}:` spelling dbt
/// matches on survives either way.
#[test]
fn a_rejected_query_says_bigquery_rejected_it_and_a_transport_failure_says_it_failed_to_submit() {
    let start = RequestStart::default();
    let (answer, requests) =
        jobs_query("p", vec![(400, error_body("invalidQuery", "Syntax error"))]);
    assert_eq!(requests.len(), 1);
    let rejected = answer.unwrap_err();
    assert_eq!(rejected.status, Status::InvalidArguments);
    assert_eq!(
        rejected.message,
        "BigQuery rejected the query: invalidQuery: Syntax error (HTTP 400)"
    );

    let (runtime, jobs) = job_client(crate::test_support::UNREACHABLE_ENDPOINT);
    let unsent = runtime
        .block_on(jobs.query(None, "p", bytes::Bytes::from_static(b"{}"), &start))
        .unwrap_err();
    assert_eq!(unsent.status, Status::IO);
    assert!(
        unsent
            .message
            .starts_with("failed to submit the BigQuery jobs.query request: "),
        "{}",
        unsent.message
    );

    // A dry run's rejection names the dry run.
    let (endpoint, captured) =
        capture_requests(vec![(400, error_body("invalidQuery", "Syntax error"))]);
    let (runtime, jobs) = job_client(&endpoint);
    let dry_run = runtime
        .block_on(jobs.insert(None, &request("dry", None, true), "query"))
        .unwrap_err();
    captured.served(1);
    assert_eq!(
        dry_run.message,
        "BigQuery rejected the dry-run query job: invalidQuery: Syntax error (HTTP 400)"
    );

    // A real submission names the job it created and the table it writes.
    let (endpoint, captured) = capture_requests(vec![(400, error_body("invalid", "Bad table"))]);
    let (runtime, jobs) = job_client(&endpoint);
    let copy = request("copy_1", Some("EU"), false).writing("p.d.t");
    let rejected = runtime
        .block_on(jobs.insert(None, &copy, "copy"))
        .unwrap_err();
    captured.served(1);
    assert_eq!(
        rejected.message,
        "BigQuery rejected the copy job billing/project:EU.copy_1 into p.d.t: invalid: Bad table \
         (HTTP 400)"
    );
}

/// BigQuery maps a failed job's `errorResult` onto every read of its results, so a completion
/// read rejected with a reason only a query can earn names the failed job. A reason that may be
/// about the read itself keeps the read's wording.
#[test]
fn a_completion_read_rejected_with_a_query_verdict_names_the_failed_job() {
    let reference = JobReference::new("billing", Some("EU"), "job_1".to_owned());
    for (status, reason, expected) in [
        (
            400,
            "invalidQuery",
            "BigQuery query job billing:EU.job_1 failed: invalidQuery: late failure (HTTP 400)",
        ),
        (
            400,
            "bytesBilledLimitExceeded",
            "BigQuery query job billing:EU.job_1 failed: bytesBilledLimitExceeded: late failure \
             (HTTP 400)",
        ),
        (
            403,
            "accessDenied",
            "failed to get the BigQuery query results: accessDenied: late failure (HTTP 403)",
        ),
        // BigQuery also answers a read of a succeeded job this way when one row exceeds its
        // 20 MiB response row limit (verified live).
        (
            403,
            "responseTooLarge",
            "failed to get the BigQuery query results: responseTooLarge: late failure (HTTP 403)",
        ),
    ] {
        let (endpoint, captured) =
            capture_requests(vec![(status, error_body(reason, "late failure"))]);
        let (runtime, client) = job_client(&endpoint);
        let failure = runtime
            .block_on(client.get_query_results(
                None,
                &reference,
                None,
                0,
                QueryResultsRead::Completion,
            ))
            .unwrap_err();
        assert_eq!(captured.join().unwrap().len(), 1, "{reason}");
        assert_eq!(failure.message, expected);
    }
}

/// The job-level reasons BigQuery reports under 400 are retried on submission, but on a read of
/// the job's results they are the job's own failure: one read, still reported as `IO` so the
/// completion poll can confirm the failure against `jobs.get` and reissue the query.
#[test]
fn a_results_read_rejected_with_a_job_level_400_reason_is_not_repeated() {
    let reference = JobReference::new("billing", Some("EU"), "job_1".to_owned());
    for reason in [
        "jobRateLimitExceeded",
        "jobBackendError",
        "jobInternalError",
        "tableUnavailable",
    ] {
        let (endpoint, captured) =
            capture_requests(vec![(400, error_body(reason, "the job failed"))]);
        let (runtime, client) = job_client(&endpoint);
        let failure = runtime
            .block_on(client.get_query_results(
                None,
                &reference,
                None,
                0,
                QueryResultsRead::Completion,
            ))
            .unwrap_err();
        assert_eq!(captured.join().unwrap().len(), 1, "{reason}");
        assert_eq!(failure.status, Status::IO, "{reason}");
    }
}

#[test]
fn get_query_results_retries_and_carries_timestamp_wait_location_and_page_token() {
    let (endpoint, captured) = capture_requests(vec![
        (500, error_body("backendError", "later")),
        ok(json!({"jobComplete": true, "rows": []})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let reference = JobReference::new("billing/project", Some("EU west/1"), "job/id".to_owned());

    runtime
        .block_on(client.get_query_results(
            None,
            &reference,
            Some("page/token +"),
            10_000,
            QueryResultsRead::Page,
        ))
        .unwrap();

    let captured = captured.served(2);
    for request in captured {
        assert_eq!(
            request.path(),
            "/bigquery/v2/projects/billing%2Fproject/queries/job%2Fid?maxResults=10000&timeoutMs=45000&formatOptions.useInt64Timestamp=true&location=EU%20west%2F1&pageToken=page%2Ftoken%20%2B"
        );
        assert!(request.raw_body.is_empty());
    }
}

/// The child listing asks for the one newest child with full statistics, and retries a transient
/// failure.
#[test]
fn listing_a_scripts_latest_child_retries_and_asks_for_one_full_job() {
    let (endpoint, captured) = capture_requests(vec![
        (503, error_body("backendError", "later")),
        ok(json!({"jobs": []})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let reference = JobReference::new("billing/project", Some("EU"), "job/id".to_owned());

    runtime
        .block_on(client.latest_child_job(None, &reference))
        .unwrap();

    let captured = captured.served(2);
    for request in captured {
        assert_eq!(
            request.path(),
            "/bigquery/v2/projects/billing%2Fproject/jobs?parentJobId=job%2Fid&maxResults=1&projection=full"
        );
    }
}

#[test]
fn an_answer_lost_after_the_post_retries_the_same_mutation() {
    // The first POST is read and dropped, and the identical replay answered 409.
    let mut answers = [
        None,
        Some((409, error_body("duplicate", "exists"))),
        Some(ok(json!({"status": {"state": "DONE"}, "recovered": true}))),
    ]
    .into_iter();
    let (endpoint, captured) = capture_requests_responding(3, move |_| answers.next().unwrap());
    insert_copy(&endpoint, "transport-ambiguous").unwrap();

    let captured = captured.served(3);
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert_eq!(
        captured[0].body["jobReference"]["jobId"],
        "transport-ambiguous"
    );
    assert!(captured[2].raw_body.is_empty(), "jobs.get has no body");
}

#[test]
fn a_final_ambiguous_post_is_reconciled_before_its_failure_is_returned() {
    // Every POST is read and dropped, so any of them may be the one BigQuery accepted.
    let mut attempt = 0;
    let (endpoint, captured) = capture_requests_responding(MAX_ATTEMPTS + 1, move |_| {
        attempt += 1;
        (attempt > MAX_ATTEMPTS)
            .then(|| ok(json!({"status": {"state": "DONE"}, "recovered": true})))
    });
    let response = insert_copy(&endpoint, "final-ambiguous").unwrap();

    assert!(serde_json::from_slice::<JsonValue>(&response).unwrap()["recovered"] == true);
    let captured = captured.served(MAX_ATTEMPTS + 1);
    assert!(
        captured[..MAX_ATTEMPTS]
            .windows(2)
            .all(|requests| requests[0].raw_body == requests[1].raw_body)
    );
    assert!(captured[MAX_ATTEMPTS].raw_body.is_empty());
}

/// A retryable rejection replays the identical mutation (same body and job ID); any other gets
/// exactly one request. For a 403 the `reason` decides; a 429 is always a rate limit.
#[test]
fn a_retryable_rejection_replays_the_identical_submission_and_a_denial_does_not() {
    for (status, reason) in [
        (500, "backendError"),
        (429, "rateLimitExceeded"),
        (403, "rateLimitExceeded"),
        // The four retryable reasons BigQuery reports under a 400.
        (400, "jobRateLimitExceeded"),
        (400, "jobBackendError"),
        (400, "jobInternalError"),
        (400, "tableUnavailable"),
    ] {
        let (endpoint, captured) = capture_requests(vec![
            (status, error_body(reason, "try later")),
            ok(json!({"status": {"state": "DONE"}})),
        ]);
        insert_copy(&endpoint, "same-id").unwrap();

        let captured = captured.join().unwrap();
        assert_eq!(captured.len(), 2, "HTTP {status} {reason} must be retried");
        assert_eq!(captured[0].raw_body, captured[1].raw_body);
        assert_eq!(captured[0].body["jobReference"]["jobId"], "same-id");
    }

    // Every other 403 is final and sent once: `blocked` is a policy denial, and
    // `responseTooLarge` needs a different request.
    for (reason, message, status) in [
        ("accessDenied", "Access Denied", Status::Unauthorized),
        ("blocked", "Blocked by policy", Status::Unauthorized),
        (
            "responseTooLarge",
            "Response too large",
            Status::InvalidArguments,
        ),
    ] {
        let (endpoint, captured) = capture_requests(vec![(403, error_body(reason, message))]);
        let failure = insert_copy(&endpoint, "denied").unwrap_err();

        assert_eq!(failure.status, status);
        assert!(
            failure.message.contains(&format!("{reason}: {message}")),
            "{failure:?}"
        );
        assert_eq!(
            captured.join().unwrap().len(),
            1,
            "HTTP 403 {reason} is a verdict on the request, not a quota to wait out"
        );
    }
}

/// Submission and reconciliation are both bounded. Only a job that never became visible lets the
/// original submission failure stand. A reconciliation that never got an answer reports its own
/// failure, since the job may exist.
#[test]
fn an_exhausted_submission_reports_what_its_reconciliation_learned() {
    for (reconciled, lead, reported) in [
        (
            (404, error_body("notFound", "absent")),
            "BigQuery rejected the copy job ",
            "backendError: submit",
        ),
        (
            (503, error_body("backendError", "reconcile")),
            "failed to reconcile the submission of BigQuery copy job ",
            "backendError: reconcile",
        ),
    ] {
        let mut responses = vec![(500, error_body("backendError", "submit")); MAX_ATTEMPTS];
        responses.extend(vec![reconciled; MAX_RECONCILIATION_ATTEMPTS]);
        let (endpoint, captured) = capture_requests(responses);
        let failure = insert_copy(&endpoint, "bounded").unwrap_err();

        assert_failure(&failure, Status::IO, &[reported]);
        assert!(failure.message.starts_with(lead), "{}", failure.message);
        captured.served(MAX_ATTEMPTS + MAX_RECONCILIATION_ATTEMPTS);
    }
}

/// A job poll retries a quota failure as it does a 5xx, and a 404 as a job not yet visible, so a
/// load, copy, or python model survives the same rate-limit episode as a concurrent query.
#[test]
fn a_job_poll_absorbs_a_quota_and_a_visibility_delay_within_its_budget() {
    let throttled = || (429, error_body("rateLimitExceeded", "slow down"));
    let reference = JobReference::new("billing/project", Some("EU"), "polled".to_owned());
    let (endpoint, captured) = capture_requests(vec![
        throttled(),
        (403, error_body("quotaExceeded", "over quota")),
        (404, error_body("notFound", "not visible")),
        ok(json!({"status":{"state":"DONE"}})),
    ]);
    let (runtime, polling) = job_client(&endpoint);

    runtime
        .block_on(polling.poll(None, &reference, "copy"))
        .unwrap();

    let captured = captured.served(4);
    for request in &captured {
        assert!(
            request.line.starts_with(
                "GET /bigquery/v2/projects/billing%2Fproject/jobs/polled?location=EU "
            ),
            "{}",
            request.line
        );
    }

    // The last attempt's failure is reported.
    let budget = MAX_ATTEMPTS;
    let (throttled_endpoint, captured) =
        capture_requests(std::iter::repeat_with(throttled).take(budget).collect());
    let (runtime, throttled_polling) = job_client(&throttled_endpoint);

    let failure = runtime
        .block_on(throttled_polling.poll(None, &reference, "copy"))
        .unwrap_err();

    assert_failure(&failure, Status::IO, &["rateLimitExceeded: slow down"]);
    captured.served(budget);
}

/// Every cancel attempt, retries included, addresses the exact job by project, ID, and location
/// with the framed empty body BigQuery requires. An accepted cancel sends one request
/// (`AdbcStatementCancel` blocks on this loop); a transient failure repeats the identical request.
#[test]
fn every_cancel_attempt_addresses_the_exact_job_with_a_framed_empty_body() {
    let accepted = || ok(json!({"job": {}}));
    let unavailable = || (500, error_body("backendError", "later"));
    for (staged, attempts) in [(vec![accepted()], 1), (vec![unavailable(), accepted()], 2)] {
        let (endpoint, captured) = capture_requests(staged);
        let (runtime, client) = job_client(&endpoint);
        let reference = JobReference::new("billing/project", Some("EU"), "job/one".to_owned());

        runtime
            .block_on(client.cancel(&reference, "query"))
            .unwrap();

        let captured = captured.join().unwrap();
        assert_eq!(
            captured.len(),
            attempts,
            "a cancel issues one request per staged rejection and no more"
        );
        for request in captured {
            assert_eq!(
                request.path(),
                "/bigquery/v2/projects/billing%2Fproject/jobs/job%2Fone/cancel?location=EU"
            );
            assert!(request.line.starts_with("POST "));
            assert_eq!(request.header("content-length"), Some("0"));
            assert!(request.raw_body.is_empty());
        }
    }
}

#[test]
fn a_non_dry_run_is_registered_before_the_post_can_answer() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests(vec![ok(json!({}))]);
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let handle = OperationCancelHandle(cancellation);
    let (endpoint, captured) =
        capture_requests_gated(vec![ok(json!({"status": {"state": "DONE"}}))], move || {
            handle.try_cancel().unwrap();
            true
        });
    let client = JobClient::for_test(&endpoint);

    let failure = runtime
        .block_on(client.insert(
            Some(&operation),
            &request("registered", Some("EU"), false),
            "copy",
        ))
        .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    captured.served(1);
    let cancels = cancels.served(1);
    assert_eq!(
        cancels[0].path(),
        "/bigquery/v2/projects/billing%2Fproject/jobs/registered/cancel?location=EU"
    );
}

/// A refusal proves no job exists only if no earlier attempt could have created one, on both
/// submission routes. The verdict is what a caller may trust; the status alone cannot say.
#[test]
fn only_a_submission_with_no_ambiguous_attempt_proves_its_job_was_not_created() {
    let denied = (403, error_body("accessDenied", "denied"));
    let unavailable = (503, error_body("backendError", "unavailable"));
    for location in [Some("EU"), None] {
        for (answers, proven) in [
            (vec![denied.clone()], true),
            (vec![unavailable.clone(), denied.clone()], false),
        ] {
            let count = answers.len();
            let (endpoint, requests) = capture_requests(answers);
            let (runtime, client) = job_client(&endpoint);
            let failure = runtime
                .block_on(client.insert(None, &request("refused", location, false), "query"))
                .unwrap_err();
            assert_eq!(has_job_verdict(&failure), proven, "{location:?} {count}");
            requests.served(count);
        }
    }
}

#[test]
fn an_accepted_insert_transitions_its_requested_identity_to_confirmed() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests(invisible(MAX_ATTEMPTS));
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let (endpoint, inserts) = capture_requests(vec![ok(
        json!({"jobReference":{"projectId":"billing/project","jobId":"accepted","location":"EU"},"status":{"state":"DONE"}}),
    )]);
    let client = JobClient::for_test(&endpoint);

    runtime
        .block_on(client.insert(
            Some(&operation),
            &request("accepted", Some("EU"), false),
            "copy",
        ))
        .unwrap();
    let failure = OperationCancelHandle(cancellation)
        .try_cancel()
        .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Unknown);
    inserts.served(1);
    cancels.served(MAX_ATTEMPTS);
}

#[test]
fn cancellation_during_ambiguous_retry_backoff_confirms_the_requested_identity() {
    let (insert_endpoint, inserts) =
        capture_requests(vec![(500, error_body("backendError", "ambiguous"))]);
    let (claimed, cancel_claimed) = mpsc::channel();
    let (cancel_endpoint, cancels) =
        capture_requests_gated(cancels_after_confirmation(), move || {
            let _ = claimed.send(());
            true
        });
    let runtime = new_runtime().unwrap();
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let client = JobClient::for_test(&insert_endpoint).with_flat_delays(CANCELLED_BACKOFF);
    let insert_runtime = runtime;
    let insert_operation = operation.clone();
    let insert = thread::spawn(move || {
        let request = request("backoff-race", Some("EU"), false);
        let mut submission = pin!(client.insert(Some(&insert_operation), &request, "copy"));
        let mut cancel_claimed = Some(cancel_claimed);
        // `try_cancel` sets the local flag before claiming the remote cancellation. Hold the
        // insert until the cancel request arrives so `try_cancel` claims it before the insert
        // confirms its identity; this exercises the post-confirmation retry path.
        insert_runtime.block_on(poll_fn(|context| {
            if insert_operation.is_cancelled()
                && let Some(cancel_claimed) = cancel_claimed.take()
            {
                cancel_claimed.recv().unwrap();
            }
            submission.as_mut().poll(context)
        }))
    });

    // The mock finishes once it has written the 500.
    inserts.join().unwrap();
    // Allow reqwest to deliver the 500 and enter retry backoff before cancelling. If that
    // backoff has already ended, the closed mock endpoint forces another failed attempt and
    // backoff, which exercises the same `confirm_submission` path.
    thread::sleep(Duration::from_millis(25));
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    assert_eq!(
        insert.join().unwrap().unwrap_err().status,
        Status::Cancelled
    );
    // Counted before `finish`, which re-claims a failed provisional cancel on its own and would
    // otherwise supply the sixth request for an insert that never confirmed its identity.
    cancels.served(MAX_ATTEMPTS + 1);
    operation.finish();
}

#[test]
fn cancellation_retries_not_found_until_the_in_flight_insert_is_visible() {
    let (post_seen, post_received) = mpsc::channel();
    let (accept_insert, insert_allowed) = mpsc::channel();
    // The locally cancelled sender drops its socket, but BigQuery may still have accepted the
    // request, which is exactly the ambiguous race under test.
    let (insert_endpoint, inserts) =
        capture_requests_gated(vec![ok(json!({"status": {"state": "DONE"}}))], move || {
            post_seen.send(()).unwrap();
            insert_allowed.recv().unwrap();
            true
        });
    let (first_cancel, first_cancelled) = mpsc::channel();
    let mut inserts = Some(inserts);
    let mut attempt = 0;
    let (cancel_endpoint, cancels) = capture_requests_responding(2, move |_| {
        attempt += 1;
        if attempt == 1 {
            first_cancel.send(()).unwrap();
            return Some((404, error_body("notFound", "not visible")));
        }
        // That mock finishes once it has answered the insert.
        inserts.take().unwrap().join().unwrap();
        Some(ok(json!({})))
    });

    let runtime = new_runtime().unwrap();
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let client = JobClient::for_test(&insert_endpoint);
    let inserting_operation = operation.clone();
    let insert = thread::spawn(move || {
        runtime.block_on(client.insert(
            Some(&inserting_operation),
            &request("visibility-race", Some("EU"), false),
            "copy",
        ))
    });
    // Handshakes, not deadlines: the joins below already block on the driver, and a panicking mock
    // drops its sender rather than leaving anyone waiting.
    post_received.recv().unwrap();

    let cancel = thread::spawn(move || OperationCancelHandle(cancellation).try_cancel());
    first_cancelled.recv().unwrap();
    accept_insert.send(()).unwrap();

    cancel.join().unwrap().unwrap();
    assert_eq!(
        insert.join().unwrap().unwrap_err().status,
        Status::Cancelled
    );
    operation.finish();
    let cancels = cancels.join().unwrap();
    assert!(cancels.iter().all(|request| request.raw_body.is_empty()));
}

#[test]
fn a_dry_run_is_not_registered_for_remote_cancellation() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({"status": {"state": "DONE"}}))]);
    let (runtime, client) = job_client(&endpoint);
    let (cancel_endpoint, cancels) = capture_requests(Vec::new());
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);

    runtime
        .block_on(client.insert(Some(&operation), &request("dry", None, true), "dry run"))
        .unwrap();
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    operation.finish();

    captured.served(1);
    assert!(cancels.join().unwrap().is_empty());
}

/// Nor does cancelling a dry run in flight, on its first attempt or its last, send anything more:
/// the job it names never existed, so nothing is left to look for or cancel.
#[test]
fn a_dry_run_cancelled_in_flight_sends_nothing_more() {
    for attempts in [1, MAX_ATTEMPTS] {
        let runtime = new_runtime().unwrap();
        let (cancel_endpoint, cancels) = capture_requests_until_stopped(vec![ok(json!({}))]);
        let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
        let handle = OperationCancelHandle(cancellation);
        let mut attempt = 0;
        let (endpoint, captured) = capture_requests_responding(attempts, move |_| {
            attempt += 1;
            if attempt == attempts {
                handle.try_cancel().unwrap();
            }
            Some((503, error_body("backendError", "later")))
        });

        let failure = runtime
            .block_on(JobClient::for_test(&endpoint).insert(
                Some(&operation),
                &request("dry", None, true),
                "dry run",
            ))
            .unwrap_err();
        operation.finish();

        assert_eq!(failure.status, Status::Cancelled);
        let captured = captured.served(attempts);
        assert!(
            captured
                .iter()
                .all(|request| request.line.starts_with("POST "))
        );
        assert_unused(cancels);
    }
}

/// An operation cancelled before its submission sends neither the job nor a cancel for it.
#[test]
fn a_submission_under_an_already_cancelled_operation_sends_nothing() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests_until_stopped(invisible(2 * MAX_ATTEMPTS));
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    let (endpoint, inserts) = capture_requests_until_stopped(vec![ok(json!({}))]);

    for location in [Some("EU"), None] {
        let failure = runtime
            .block_on(JobClient::for_test(&endpoint).insert(
                Some(&operation),
                &request("never-sent", location, false),
                "query",
            ))
            .unwrap_err();
        assert_eq!(failure.status, Status::Cancelled, "{location:?}");
    }
    operation.finish();

    assert_unused(inserts);
    assert_unused(cancels);
}

/// Credentials that say when a request asks for a token, then take `delay` to mint it, as the first
/// request of a connection or an impersonation exchange can.
#[derive(Debug)]
struct SlowCredentials {
    minting: tokio::sync::mpsc::UnboundedSender<()>,
    delay: Duration,
}

fn slow_credentials(
    minting: tokio::sync::mpsc::UnboundedSender<()>,
    delay: Duration,
) -> google_cloud_auth::credentials::Credentials {
    google_cloud_auth::credentials::Credentials::from(SlowCredentials { minting, delay })
}

impl google_cloud_auth::credentials::CredentialsProvider for SlowCredentials {
    async fn headers(
        &self,
        _extensions: http::Extensions,
    ) -> std::result::Result<
        google_cloud_auth::credentials::CacheableResource<http::HeaderMap>,
        google_cloud_gax::error::CredentialsError,
    > {
        let _ = self.minting.send(());
        tokio::time::sleep(self.delay).await;
        Ok(google_cloud_auth::credentials::CacheableResource::New {
            entity_tag: google_cloud_auth::credentials::EntityTag::new(),
            data: http::HeaderMap::new(),
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".to_owned())
    }
}

/// A cancel that lands while the submission is still minting its token stops it before any POST,
/// so the job it would have named is never confirmed: the cancel spends only its own attempts on
/// the provisional identity and succeeds, instead of starting a second generation and reporting
/// the never-created job's 404.
#[test]
fn a_cancel_while_the_token_is_minted_confirms_no_job() {
    let runtime = new_runtime().unwrap();
    let (cancel_endpoint, cancels) = capture_requests_until_stopped(invisible(3 * MAX_ATTEMPTS));
    let (cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
    let (endpoint, inserts) = capture_requests_until_stopped(vec![ok(json!({}))]);
    let (minting, mut minting_started) = tokio::sync::mpsc::unbounded_channel();
    let client = JobClient::new(
        slow_credentials(minting, Duration::from_secs(10)),
        Some(&endpoint),
        crate::rest::client::rest_client("job client test").unwrap(),
    );
    let canceller = thread::spawn(move || {
        minting_started.blocking_recv().unwrap();
        OperationCancelHandle(cancellation).try_cancel()
    });

    let failure = runtime
        .block_on(client.insert(
            Some(&operation),
            &request("never-sent", Some("EU"), false),
            "query",
        ))
        .unwrap_err();
    let cancelled = canceller.join().unwrap();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    assert!(cancelled.is_ok(), "{cancelled:?}");
    // A second generation would be sent in the background as soon as the identity was confirmed.
    thread::sleep(Duration::from_millis(200));
    assert!(inserts.stop().is_empty());
    let cancels = cancels.stop().len();
    assert!(cancels <= MAX_ATTEMPTS, "{cancels} jobs.cancel requests");
}

/// A submission BigQuery refused created no job, so the operation's end sends no `jobs.cancel`
/// for it, with or without a location. A copy from a missing table answers 404 with none, and an
/// expired `temporary_access_token` reaches the caller through `send`'s 401 remedy.
#[test]
fn a_refused_submission_leaves_no_job_to_cancel() {
    for (code, reason, expected) in [
        (404, "notFound", Status::NotFound),
        (401, "authError", Status::Unauthenticated),
    ] {
        for location in [Some("EU"), None] {
            let runtime = new_runtime().unwrap();
            let (cancel_endpoint, cancels) =
                capture_requests_until_stopped(invisible(MAX_ATTEMPTS));
            let (_cancellation, operation) = operation_at(&runtime, &cancel_endpoint);
            let (endpoint, inserts) = capture_requests(vec![(code, error_body(reason, "refused"))]);
            let mut client = JobClient::for_test(&endpoint);
            client.http = client
                .http
                .with_unauthenticated_remedy(Some("set a fresh token".to_owned()));
            let failure = runtime
                .block_on(client.insert(
                    Some(&operation),
                    &request("refused", location, false),
                    "copy",
                ))
                .unwrap_err();
            operation.finish();

            assert_eq!(failure.status, expected, "{location:?}");
            assert_eq!(inserts.join().unwrap().len(), 1, "{location:?}");
            assert_unused(cancels);
        }
    }
}

/// A dry run creates no job, so neither its 409 (BigQuery's answer to a dry-run `CREATE TABLE` of
/// an existing table) nor its last ambiguous attempt is followed by a `jobs.get` for it.
#[test]
fn a_dry_run_failure_is_reported_without_looking_for_a_job() {
    let mut responses = vec![(
        409,
        error_body("duplicate", "Already Exists: Table p:d.existing"),
    )];
    responses.extend((0..MAX_ATTEMPTS).map(|_| (503, error_body("backendError", "later"))));
    let (endpoint, captured) = capture_requests(responses);
    let (runtime, client) = job_client(&endpoint);
    let insert = |id| runtime.block_on(client.insert(None, &request(id, None, true), "query"));

    let conflict = insert("dry-conflict").unwrap_err();
    assert_failure(
        &conflict,
        Status::AlreadyExists,
        &[
            "BigQuery rejected the dry-run query job",
            "duplicate: Already Exists: Table p:d.existing (HTTP 409)",
        ],
    );
    let unavailable = insert("dry-unavailable").unwrap_err();
    assert_eq!(unavailable.status, Status::IO);

    let captured = captured.served(1 + MAX_ATTEMPTS);
    assert!(
        captured
            .iter()
            .all(|request| request.line.starts_with("POST "))
    );
}

#[test]
fn request_construction_requires_the_callers_job_identity() {
    for reference in [
        JobReference::new("", None, "job".to_owned()),
        JobReference::new("project", None, String::new()),
    ] {
        let failure = job_request(reference, false).err().unwrap();
        assert_eq!(failure.status, Status::InvalidArguments);
    }
}

/// The window opens at the first failure and any answer closes it, so only failures with no answer
/// between them use it up. A failure that retrying cannot help ends the wait unchanged.
#[test]
fn a_wait_outage_bounds_only_consecutive_retryable_failures() {
    const LIMIT: Duration = Duration::from_secs(600);
    let unavailable = || crate::error::error("unavailable", Status::IO);
    let job = JobReference::new("project", Some("EU"), "job".to_owned());
    let mut outage = super::WaitOutage {
        limit: LIMIT,
        since: None,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
        .block_on(async {
            outage.outlast(unavailable(), &job).unwrap();
            tokio::time::advance(LIMIT - Duration::from_secs(1)).await;
            outage.outlast(unavailable(), &job).unwrap();

            outage.end();
            tokio::time::advance(LIMIT - Duration::from_secs(1)).await;
            outage.outlast(unavailable(), &job).unwrap();

            tokio::time::advance(LIMIT).await;
            let spent = outage.outlast(unavailable(), &job).unwrap_err();
            assert_failure(
                &spent,
                Status::IO,
                &["project:EU.job", "after 600 s of failed requests"],
            );

            let denied = crate::error::error("denied", Status::Unauthorized);
            assert_eq!(outage.outlast(denied, &job).unwrap_err().message, "denied");
        });
}
