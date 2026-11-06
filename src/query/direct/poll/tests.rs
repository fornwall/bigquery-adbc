//! How a completion poll classifies BigQuery's answers: a rejected results read, and a complete
//! response carrying `errors`. Both decide whether a query may run again.

use serde_json::json;

use super::*;
use crate::query::QueryResultMode;
use crate::query::response::retryable_job_errors;
use crate::rest::retry::MAX_ATTEMPTS;
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::http::{CapturedRequest, capture_requests, error_body};

/// The job every fixture below polls.
fn reference() -> JobReference {
    JobReference::new("project", Some("EU"), "polled-job".to_owned())
}

fn client(endpoint: &str) -> (SharedRuntime, JobClient) {
    let runtime = new_runtime().unwrap();
    let client = JobClient::for_test(endpoint);
    (runtime, client)
}

/// Polls the fixture job to completion, ungated, and returns the outcome and what was requested.
fn poll(
    responses: Vec<(u16, serde_json::Value)>,
) -> (
    Result<QueryPage, AttemptFailure>,
    Vec<CapturedRequest>,
    QueryMetadata,
) {
    let (endpoint, requests) = capture_requests(responses);
    let (runtime, client) = client(&endpoint);
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);
    let mut metadata = QueryMetadata::default();
    let outcome = runtime.block_on(poll_until_complete(
        &client,
        &decoder,
        &reference(),
        None,
        None,
        &mut metadata,
        // The production string, which is a whole sentence: a results read must never render it
        // into a noun slot of its own message. See `JobClient::get_query_results`.
        "BigQuery query execution failed",
    ));
    (outcome, requests.join().unwrap(), metadata)
}

/// How many identical reads the transport loop spends before the rejection reaches the caller.
const READS: usize = MAX_ATTEMPTS as usize;

/// The rejection the "the read failed" fixtures answer every read with.
///
/// `rateLimitExceeded` is a reason a failed job would be rerun for, so no test below can pass by
/// trusting the rejection; only the job resource says what the query did.
fn rejected_reads() -> Vec<(u16, serde_json::Value)> {
    std::iter::repeat_n((403, error_body("rateLimitExceeded", "read denied")), READS).collect()
}

fn after_rejected_reads(job: (u16, serde_json::Value)) -> Vec<(u16, serde_json::Value)> {
    let mut responses = rejected_reads();
    responses.push(job);
    responses
}

fn failed_job(reason: &str) -> (u16, serde_json::Value) {
    (
        200,
        json!({
            "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
            "status": {
                "state": "DONE",
                "errorResult": {"reason": reason, "message": "the job failed"}
            }
        }),
    )
}

/// BigQuery reports a terminally failed job as an HTTP rejection of the results read, so the
/// rejection is checked against the job's `status.errorResult`, and a reissuable reason there
/// reaches the reissue loop.
#[test]
fn a_rejected_read_of_a_failed_job_becomes_the_terminal_failure_the_job_reports() {
    let (outcome, requests, _) = poll(after_rejected_reads(failed_job("backendError")));

    let AttemptFailure::Terminal(terminal) = outcome.expect_err("the poll must fail") else {
        panic!("a DONE job with an errorResult is a terminal query failure");
    };
    assert_eq!(terminal.errors.len(), 1);
    assert_eq!(
        terminal.errors[0].reason, "backendError",
        "the job's reason is reported, not the read's"
    );
    assert!(
        retryable_job_errors(&terminal.errors),
        "backendError is reissuable"
    );
    assert_eq!(
        terminal.job.as_ref().map(|job| job.job_id.as_str()),
        Some("polled-job")
    );
    assert_eq!(requests.len(), READS + 1, "the reads and one jobs.get");
    assert!(requests[READS].line.contains("/jobs/polled-job"));
}

#[test]
fn verified_terminal_jobs_are_not_cancelled_again() {
    use crate::cancellation::{Cancellation, OperationCancelHandle};
    use crate::clients::ClientState;
    use crate::config::ServiceEndpointKind;
    use crate::test_support::http::capture_requests_until_stopped;
    use adbc_core::CancelHandle as _;

    for (state, failed) in [("DONE", false), ("DONE", true), ("RUNNING", false)] {
        let (endpoint, server) = capture_requests_until_stopped(vec![
            (
                200,
                json!({
                    "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                    "status": {
                        "state": state,
                        "errorResult": failed.then(|| json!({"reason": "backendError"}))
                    }
                }),
            ),
            (200, json!({})),
        ]);
        let (runtime, client) = client(&endpoint);
        let cancellation = Cancellation::new(
            runtime.clone(),
            ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint),
            "statement",
        );
        let operation = cancellation.begin();
        let reference = JobReference::new("project", None, "polled-job".to_owned());
        operation.register_job(&reference).unwrap();
        let verdict = runtime.block_on(verify_job(&client, Some(&operation), &reference, "SELECT"));
        match verdict {
            JobVerdict::Failed(terminal) => {
                assert!(failed);
                assert_eq!(terminal.job.unwrap().location.as_deref(), Some("EU"));
            }
            JobVerdict::Succeeded => assert_eq!((state, failed), ("DONE", false)),
            JobVerdict::Unverified => assert_eq!(state, "RUNNING"),
            JobVerdict::Cancelled(_) => panic!("the operation has not been cancelled"),
        }
        OperationCancelHandle(cancellation).try_cancel().unwrap();
        operation.finish();
        let requests = server.stop();
        assert_eq!(requests.len(), if state == "DONE" { 1 } else { 2 });
        if state == "RUNNING" {
            assert!(
                requests[1]
                    .path()
                    .ends_with("/jobs/polled-job/cancel?location=EU")
            );
        }
    }
}

/// The rejection's own reason never decides: a `rateLimitExceeded` on the read says nothing about
/// the job, and rerunning DML on it could duplicate writes. Any `jobs.get` answer short of "DONE
/// and failed" reports the original rejection.
#[test]
fn only_a_done_job_with_an_error_result_may_turn_a_rejection_into_a_rerun() {
    for (what, job) in [
        (
            "a job still running",
            json!({
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "status": {"state": "RUNNING"}
            }),
        ),
        (
            "a job that finished without an errorResult",
            json!({
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "status": {"state": "DONE", "errors": [{"reason": "stopped", "message": "w"}]}
            }),
        ),
        (
            "a fetched resource that is a different job",
            json!({
                "jobReference": {"projectId": "project", "jobId": "other-job", "location": "EU"},
                "status": {
                    "state": "DONE",
                    "errorResult": {"reason": "backendError", "message": "the job failed"}
                }
            }),
        ),
    ] {
        let (outcome, requests, _) = poll(after_rejected_reads((200, job)));

        let AttemptFailure::Request(failure) = outcome.expect_err("the poll must fail") else {
            panic!("{what} must not reissue the query");
        };
        assert!(
            failure.message.contains("rateLimitExceeded: read denied"),
            "{what}: {failure:?}"
        );
        assert_eq!(requests.len(), READS + 1, "{what}");
    }
}

/// A failed verification is not evidence: the rejection stands and the query is not rerun.
#[test]
fn a_rejection_whose_job_cannot_be_fetched_reports_the_rejection() {
    let unavailable = std::iter::repeat_n(
        (
            503,
            json!({"error": {"code": 503, "message": "unavailable", "status": "UNAVAILABLE"}}),
        ),
        READS,
    )
    .collect::<Vec<_>>();
    // A 200 that is not a job resource establishes nothing, like a failed fetch.
    let undecodable = vec![(200, json!([]))];

    for (what, fetches, requests_expected) in [
        ("a fetch that never answers", unavailable, READS * 2),
        ("a body that is not a job", undecodable, READS + 1),
    ] {
        let mut responses = rejected_reads();
        responses.extend(fetches);

        let (outcome, requests, _) = poll(responses);

        let AttemptFailure::Request(failure) = outcome.expect_err("the poll must fail") else {
            panic!("{what} may not reissue the query");
        };
        // The read names itself, not the caller's failure sentence ("BigQuery query execution
        // failed"), which does not fit the message's noun slot.
        assert!(
            failure
                .message
                .starts_with("failed to get the BigQuery query results: "),
            "{what}: {failure:?}"
        );
        assert!(
            failure.message.contains("rateLimitExceeded: read denied"),
            "{what}: {failure:?}"
        );
        assert_eq!(requests.len(), requests_expected, "{what}");
    }
}

/// A rejection a fresh job could not fix (a denial, a malformed request, a missing job) costs no
/// verification request. The first two are final on the first read; the transport loop retries a
/// 404 as propagation delay until its budget is spent.
#[test]
fn a_rejection_no_fresh_job_could_answer_is_reported_without_describing_the_job() {
    for (status, reason, reads) in [
        (403, "accessDenied", 1),
        (400, "invalidQuery", 1),
        (404, "notFound", READS),
    ] {
        let (outcome, requests, _) =
            poll(std::iter::repeat_n((status, error_body(reason, "no")), reads).collect());

        let AttemptFailure::Request(failure) = outcome.expect_err("the poll must fail") else {
            panic!("{reason} must not reissue the query");
        };
        assert!(failure.message.contains(reason), "{failure:?}");
        assert_eq!(requests.len(), reads, "{reason} spent a verification read");
    }
}

/// `errors` on a complete response may be warnings, so the job decides. A failed verification
/// would report a successful query as failed, so it gets the completion poll's transient retries
/// rather than a single attempt.
#[test]
fn a_transient_failure_of_the_verification_fetch_does_not_fail_a_successful_query() {
    let (outcome, requests, metadata) = poll(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "7"}]}],
                "totalRows": "1",
                "errors": [{"reason": "stopped", "message": "a warning, not a failure"}]
            }),
        ),
        (
            503,
            json!({"error": {"code": 503, "message": "unavailable", "status": "UNAVAILABLE"}}),
        ),
        (
            200,
            json!({
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "status": {"state": "DONE"}
            }),
        ),
    ]);

    let page = match outcome {
        Ok(page) => page,
        Err(AttemptFailure::Request(failure)) => {
            panic!("one 503 on the verification fetch failed a successful query: {failure:?}")
        }
        Err(AttemptFailure::Terminal(terminal)) => {
            panic!(
                "a warning was reported as a terminal failure: {:?}",
                terminal.errors
            )
        }
    };
    assert_eq!(page.rows.len(), 1);
    assert_eq!(metadata.total_rows, Some(1));
    assert_eq!(requests.len(), 3, "the results read and two job fetches");
}

/// A verification answer that is not a job resource establishes nothing, and there is no
/// rejection to fall back on: the HTTP-200 `errors` are reported unverified, never as success, and
/// never reach the reissue loop.
#[test]
fn an_undecodable_verification_body_reports_the_unverified_errors() {
    let (outcome, requests, _) = poll(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "7"}]}],
                "errors": [{"reason": "backendError", "message": "unverified"}]
            }),
        ),
        (200, json!([])),
    ]);

    let AttemptFailure::Request(failure) = outcome.expect_err("the poll must fail") else {
        panic!("an unverified HTTP-200 error may not reissue the query");
    };
    assert!(
        failure
            .message
            .starts_with("BigQuery query execution failed"),
        "{failure:?}"
    );
    assert!(
        failure.message.contains("backendError: unverified"),
        "{failure:?}"
    );
    assert_eq!(requests.len(), 2, "the results read and one job fetch");
}
