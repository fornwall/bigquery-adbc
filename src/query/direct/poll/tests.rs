//! How a completion poll classifies BigQuery's answers: a rejected results read, and a complete
//! response carrying `errors`. Both decide whether a query may run again.

use std::sync::Arc;

use adbc_core::CancelHandle as _;
use serde_json::json;

use super::*;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::query::QueryResultMode;
use crate::query::response::retryable_job_errors;
use crate::rest::retry::MAX_ATTEMPTS;
use crate::runtime::SharedRuntime;
use crate::test_support::cancellation::{job_client, operation_at};
use crate::test_support::http::{
    CapturedRequest, capture_requests, capture_requests_until_stopped, error_body, ok,
};

/// The job every fixture below polls.
fn reference() -> JobReference {
    JobReference::new("project", Some("EU"), "polled-job".to_owned())
}

type Polled = (
    Result<QueryPage, AttemptFailure>,
    Vec<CapturedRequest>,
    QueryMetadata,
);

/// Polls the fixture job for its rows. See [`poll_in`].
fn poll(responses: Vec<(u16, serde_json::Value)>) -> Polled {
    poll_in(QueryResultMode::Rows, responses)
}

/// Polls the fixture job to completion, ungated, and returns the outcome and what was requested.
fn poll_in(mode: QueryResultMode, responses: Vec<(u16, serde_json::Value)>) -> Polled {
    let (endpoint, requests) = capture_requests(responses);
    let (runtime, client) = job_client(&endpoint);
    let decoder = QueryResponseDecoder::new(&runtime, mode);
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
    ok(json!({
        "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
        "status": {
            "state": "DONE",
            "errorResult": {"reason": reason, "message": "the job failed"}
        }
    }))
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
    for (state, failed) in [("DONE", false), ("DONE", true), ("RUNNING", false)] {
        let (endpoint, server) = capture_requests_until_stopped(vec![
            ok(json!({
                "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
                "status": {
                    "state": state,
                    "errorResult": failed.then(|| json!({"reason": "backendError"}))
                }
            })),
            ok(json!({})),
        ]);
        let (runtime, client) = job_client(&endpoint);
        let (cancellation, operation) = operation_at(&runtime, &endpoint);
        let reference = JobReference::new("project", None, "polled-job".to_owned());
        operation.register_job(&reference).unwrap();
        let verdict = runtime.block_on(verify_job(&client, Some(&operation), &reference, "SELECT"));
        match verdict {
            JobVerdict::Failed(terminal) => {
                assert!(failed);
                assert_eq!(terminal.job.unwrap().location.as_deref(), Some("EU"));
            }
            JobVerdict::Succeeded(_) => assert_eq!((state, failed), ("DONE", false)),
            JobVerdict::Running => assert_eq!(state, "RUNNING"),
            JobVerdict::Unverified | JobVerdict::Cancelled(_) => {
                panic!("the job was fetched, and the operation has not been cancelled")
            }
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
/// the job, and rerunning DML on it could duplicate writes. With no outage budget left, a `jobs.get`
/// answer short of "DONE and failed" reports the original rejection.
#[test]
fn only_a_done_job_with_an_error_result_may_turn_a_rejection_into_a_rerun() {
    for (what, job) in [
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
    let unavailable = std::iter::repeat_n(unavailable(), READS).collect::<Vec<_>>();
    // A 200 that is not a job resource establishes nothing, like a failed fetch.
    let undecodable = vec![ok(json!([]))];

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

/// An answer to the job's `jobs.get`, in `state`.
fn job_in(state: &str) -> (u16, serde_json::Value) {
    ok(json!({
        "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
        "status": {"state": state}
    }))
}

fn unavailable() -> (u16, serde_json::Value) {
    (
        503,
        json!({"error": {"code": 503, "message": "unavailable", "status": "UNAVAILABLE"}}),
    )
}

/// Every request of one poll round failing: the reads, then the `jobs.get` that follows them.
fn failed_round() -> Vec<(u16, serde_json::Value)> {
    vec![unavailable(); 2 * READS]
}

/// The fixture job, finished without an error and reporting `statistics`.
fn finished_with(statistics: &serde_json::Value) -> (u16, serde_json::Value) {
    ok(json!({
        "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
        "status": {"state": "DONE"},
        "statistics": statistics
    }))
}

/// Once `jobs.get` proves a job succeeded, a read that could add nothing must not decide the
/// statement. Reporting committed DML as failed invites a rerun that applies it twice, and a
/// `COMMIT` whose read keeps failing would otherwise lose the verdict the job has already given.
#[test]
fn a_finished_job_with_nothing_left_to_read_completes_without_a_read() {
    let select_schema = json!({"fields": [{"name": "value", "type": "INTEGER"}]});
    for (what, mode, statistics) in [
        (
            "DML run for its count",
            QueryResultMode::MetadataOnly,
            json!({"query": {"statementType": "UPDATE", "numDmlAffectedRows": "3"}}),
        ),
        (
            "DML run through ExecuteQuery, which asks for rows it cannot have",
            QueryResultMode::Rows,
            json!({"query": {"statementType": "UPDATE", "numDmlAffectedRows": "3"}}),
        ),
        (
            "a commit",
            QueryResultMode::MetadataOnly,
            json!({"query": {"statementType": "COMMIT_TRANSACTION"}}),
        ),
        (
            "a SELECT whose job names its schema",
            QueryResultMode::MetadataOnly,
            json!({"query": {"statementType": "SELECT", "schema": select_schema}}),
        ),
        (
            "a statement the job does not name, with no rows asked for",
            QueryResultMode::MetadataOnly,
            json!({}),
        ),
    ] {
        let expected_type = statistics["query"]["statementType"].as_str().unwrap_or("");
        let expected_rows = statistics["query"]["numDmlAffectedRows"]
            .as_str()
            .map(|rows| rows.parse().unwrap());

        let (outcome, requests, metadata) =
            poll_in(mode, after_rejected_reads(finished_with(&statistics)));

        let Ok(page) = outcome else {
            panic!("{what}: a job proven successful was reported failed");
        };
        assert!(page.rows.is_empty() && page.page_token.is_none(), "{what}");
        assert_eq!(requests.len(), READS + 1, "{what}");
        assert_eq!(metadata.statement_type, expected_type, "{what}");
        assert_eq!(metadata.num_dml_affected_rows, expected_rows, "{what}");
    }
}

/// A read still decides what only it can supply: the rows of a statement that may return some, and
/// a schema `result_schema` requires. Finishing without one would lose them, so with no outage
/// budget the rejection stands, rather than an empty result or a false "without a result schema".
#[test]
fn a_finished_job_still_waits_for_a_read_that_supplies_what_the_caller_needs() {
    let statement =
        |statement_type| finished_with(&json!({"query": {"statementType": statement_type}}));
    for (what, mode, job) in [
        (
            "rows of a SELECT",
            QueryResultMode::Rows,
            statement("SELECT"),
        ),
        (
            "rows of a script",
            QueryResultMode::Rows,
            statement("SCRIPT"),
        ),
        (
            "rows of a procedure",
            QueryResultMode::Rows,
            statement("CALL"),
        ),
        (
            "the schema of a SELECT run for its metadata",
            QueryResultMode::MetadataOnly,
            statement("SELECT"),
        ),
    ] {
        let (outcome, requests, _) = poll_in(mode, after_rejected_reads(job));

        assert!(
            matches!(outcome, Err(AttemptFailure::Request(_))),
            "{what} was given up on"
        );
        assert_eq!(requests.len(), READS + 1, "{what}");
    }

    // Asking for no rows after a page proved too large leaves the rows to Storage Read, which still
    // needs the result's schema.
    let mut responses = vec![(403, error_body("responseTooLarge", "exceeds max row limit"))];
    responses.extend(after_rejected_reads(statement("SCRIPT")));
    let (outcome, requests, metadata) = poll(responses);
    assert!(matches!(outcome, Err(AttemptFailure::Request(_))));
    assert_eq!(requests.len(), READS + 2);
    assert!(metadata.rows_exceed_rest);
}

/// A job finished this way never reaches the read that normally publishes its cost.
#[test]
fn a_finished_job_completed_without_a_read_publishes_its_cost() {
    let (endpoint, requests) = capture_requests(after_rejected_reads(finished_with(&json!({
        "query": {"statementType": "DELETE", "totalBytesProcessed": "42"}
    }))));
    let (runtime, client) = job_client(&endpoint);
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::MetadataOnly);
    let progress = ExecutionProgress::default();
    let outcome = runtime.block_on(poll_until_complete(
        &client,
        &decoder,
        &reference(),
        None,
        Some(&progress),
        &mut QueryMetadata::default(),
        "BigQuery query execution failed",
    ));

    assert!(outcome.is_ok());
    assert_eq!((progress.current(), progress.maximum()), (42.0, 42.0));
    requests.served(READS + 1);
}

/// A cancellable operation on the fixture job, whose cancellation reaches `endpoint`.
fn tracked_job(runtime: &SharedRuntime, endpoint: &str) -> (Arc<Cancellation>, Operation) {
    let (cancellation, operation) = operation_at(runtime, endpoint);
    operation.register_job(&reference()).unwrap();
    (cancellation, operation)
}

/// The retry budget is per request, not per job: running out of it says nothing about the job, and
/// ending the wait would cancel it. So however the requests fail, the wait goes on, and a job that
/// finishes is read without having been cancelled.
#[test]
fn a_wait_outlasts_failed_requests_and_leaves_the_job_alone() {
    let complete = ok(json!({
        "jobComplete": true,
        "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
        "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
        "rows": [{"f": [{"v": "7"}]}]
    }));
    for (what, mut responses) in [
        (
            "every request failing",
            [failed_round(), failed_round()].concat(),
        ),
        (
            "reads failing for a job still running",
            [
                after_rejected_reads(job_in("RUNNING")),
                after_rejected_reads(job_in("RUNNING")),
            ]
            .concat(),
        ),
        (
            "reads failing for a job that finished",
            after_rejected_reads(job_in("DONE")),
        ),
    ] {
        responses.push(complete.clone());
        let staged = responses.len();
        let (endpoint, server) = capture_requests_until_stopped(responses);
        let (runtime, client) = job_client(&endpoint);
        let client = client.with_wait_outage(Duration::from_secs(600));
        let (cancellation, operation) = tracked_job(&runtime, &endpoint);
        let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);
        let outcome = runtime.block_on(poll_until_complete(
            &client,
            &decoder,
            &reference(),
            Some(&operation),
            None,
            &mut QueryMetadata::default(),
            "BigQuery query execution failed",
        ));

        let Ok(page) = outcome else {
            panic!("{what} ended the wait");
        };
        assert_eq!(page.rows.len(), 1, "{what}");
        // Delivers synchronously any cancellation still claimable, so none can be missed below.
        OperationCancelHandle(cancellation).try_cancel().unwrap();
        operation.finish();
        let requests = server.stop();
        assert_eq!(requests.len(), staged, "{what}");
        assert!(
            requests
                .iter()
                .all(|request| !request.path().contains("/cancel")),
            "{what} cancelled the job"
        );
    }
}

/// A job `jobs.get` reports unfinished needs no outage budget: nothing is known to be wrong, and
/// the job's end bounds the wait.
#[test]
fn a_rejected_read_of_an_unfinished_job_keeps_waiting_without_an_outage_budget() {
    for state in ["PENDING", "RUNNING"] {
        let mut responses = after_rejected_reads(job_in(state));
        responses.push(ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
            "schema": {"fields": []},
            "rows": []
        })));

        let (outcome, requests, _) = poll(responses);

        assert!(outcome.is_ok(), "{state}");
        assert_eq!(requests.len(), READS + 2, "{state}");
    }
}

/// Giving up on an outage cannot tell the caller what the job did, so the error says that, and the
/// operation's end still tries to stop the job.
#[test]
fn a_wait_that_gives_up_says_the_job_may_still_be_running() {
    let mut responses = failed_round();
    responses.push(ok(json!({})));
    let (endpoint, requests) = capture_requests(responses);
    let (runtime, client) = job_client(&endpoint);
    let (_cancellation, operation) = tracked_job(&runtime, &endpoint);
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);
    let outcome = runtime.block_on(poll_until_complete(
        &client,
        &decoder,
        &reference(),
        Some(&operation),
        None,
        &mut QueryMetadata::default(),
        "BigQuery query execution failed",
    ));

    let Err(AttemptFailure::Request(failure)) = outcome else {
        panic!("a spent outage budget must end the wait");
    };
    assert_eq!(failure.status, Status::IO);
    assert!(
        failure
            .message
            .contains("the driver stopped waiting for BigQuery job project:EU.polled-job after ")
            && failure.message.contains(
                " s of failed requests and is cancelling it if unfinished; check the job before \
                 running this again"
            ),
        "{failure:?}"
    );
    operation.finish();
    let requests = requests.served(2 * READS + 1);
    assert!(
        requests[2 * READS]
            .path()
            .ends_with("/jobs/polled-job/cancel?location=EU")
    );
}

/// With no operation no job is registered, so nothing cancels it and the error cannot say so.
#[test]
fn a_wait_with_no_operation_does_not_claim_to_cancel_the_job() {
    let (outcome, _, _) = poll(failed_round());

    let Err(AttemptFailure::Request(failure)) = outcome else {
        panic!("a spent outage budget must end the wait");
    };
    assert!(
        failure
            .message
            .contains("s of failed requests; check the job"),
        "{failure:?}"
    );
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
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
            "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "7"}]}],
            "totalRows": "1",
            "errors": [{"reason": "stopped", "message": "a warning, not a failure"}]
        })),
        unavailable(),
        job_in("DONE"),
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
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "project", "jobId": "polled-job", "location": "EU"},
            "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "7"}]}],
            "errors": [{"reason": "backendError", "message": "unverified"}]
        })),
        ok(json!([])),
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
