//! The one named `BEGIN TRANSACTION` job that creates a transaction's session and begins the
//! transaction in it.

use super::*;
use crate::test_support::http::{CapturedRequest, capture_requests_responding, ok};

/// Begins a transaction in a new session at `endpoint`, in `location`.
fn begin_in_new_session_at(endpoint: &str, location: Option<&str>) -> Result<Arc<str>> {
    let runtime = new_runtime().unwrap();
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint);
    crate::query::begin_in_new_session(
        &runtime,
        &state,
        Some(&ProjectIdentifier::try_new_from("test", "project").unwrap()),
        location,
    )
}

/// The job ID a `jobs.get` or `jobs.getQueryResults` request names.
fn requested_job_id(request: &CapturedRequest) -> String {
    let path = request.path();
    let after = path
        .split("/queries/")
        .nth(1)
        .or_else(|| path.split("/jobs/").nth(1))
        .unwrap();
    after.split('?').next().unwrap().to_owned()
}

/// The completed results of the job `request` polls, in `location`.
fn completed_poll(request: &CapturedRequest, location: &str) -> (u16, serde_json::Value) {
    ok(json!({
        "jobComplete": true,
        "jobReference": {
            "projectId": "project",
            "jobId": requested_job_id(request),
            "location": location
        }
    }))
}

/// One `BEGIN` job creates the session: its RUNNING answer already names the session and the
/// statement, so the poll that completes it is the only other request. Without a location, the
/// job runs where BigQuery places it, and the poll goes there.
#[test]
fn one_job_creates_the_session_and_begins_its_transaction() {
    for location in [Some("EU"), None] {
        let placed = location.unwrap_or("US");
        let mut index = 0;
        let (endpoint, captured) = capture_requests_responding(2, move |request| {
            index += 1;
            if index > 1 {
                return Some(completed_poll(request, placed));
            }
            let mut reference = request.body["jobReference"].clone();
            reference["location"] = json!(placed);
            Some(ok(json!({
                "jobReference": reference,
                "status": {"state": "RUNNING"},
                "statistics": {
                    "query": {"statementType": "BEGIN_TRANSACTION"},
                    "sessionInfo": {"sessionId": "session-created"}
                }
            })))
        });
        let session = begin_in_new_session_at(&endpoint, location).unwrap();
        assert_eq!(&*session, "session-created", "{location:?}");

        let captured = captured.served(2);
        let insert = &captured[0];
        assert!(
            insert
                .line
                .starts_with("POST /bigquery/v2/projects/project/jobs"),
            "{}",
            insert.line
        );
        let query = &insert.body["configuration"]["query"];
        assert_eq!(query["query"], "BEGIN TRANSACTION");
        assert_eq!(query["createSession"], true);
        assert!(query["connectionProperties"].is_null(), "{query}");
        assert_eq!(insert.body["jobReference"]["location"].as_str(), location);
        let poll = captured[1].path();
        assert!(poll.contains(&format!("location={placed}")), "{poll}");
    }
}

/// When the answer to the `BEGIN` that creates the session is lost, its replay's 409 leads to the
/// job, whose resource names the session: the transaction is recovered rather than begun again in
/// a second session.
#[test]
fn a_lost_answer_to_the_session_creating_begin_recovers_its_session() {
    let mut index = 0;
    let (endpoint, captured) = capture_requests_responding(4, move |request| {
        index += 1;
        Some(match index {
            1 => (503, error_body("backendError", "lost")),
            2 => (
                409,
                json!({"error": {"code": 409, "errors": [{"reason": "duplicate",
                    "message": "Already Exists: Job"}]}}),
            ),
            3 => ok(json!({
                "jobReference": {
                    "projectId": "project",
                    "jobId": requested_job_id(request),
                    "location": "EU"
                },
                "status": {"state": "DONE"},
                "statistics": {
                    "query": {"statementType": "BEGIN_TRANSACTION"},
                    "sessionInfo": {"sessionId": "session-created"}
                }
            })),
            _ => completed_poll(request, "EU"),
        })
    });
    let session = begin_in_new_session_at(&endpoint, Some("EU")).unwrap();
    assert_eq!(&*session, "session-created");

    let captured = captured.served(4);
    let inserts = captured
        .iter()
        .filter(|request| request.line.starts_with("POST"))
        .collect::<Vec<_>>();
    assert_eq!(inserts.len(), 2, "the replay is the only resubmission");
    assert_eq!(
        inserts[0].body["jobReference"]["jobId"],
        inserts[1].body["jobReference"]["jobId"]
    );
}

/// A `BEGIN` whose job names no session leaves the caller no session to use.
#[test]
fn a_session_creating_begin_that_names_no_session_is_invalid_data() {
    let mut index = 0;
    let (endpoint, captured) = capture_requests_responding(2, move |request| {
        index += 1;
        if index > 1 {
            return Some(completed_poll(request, "EU"));
        }
        Some(ok(json!({
            "jobReference": request.body["jobReference"].clone(),
            "status": {"state": "RUNNING"},
            "statistics": {"query": {"statementType": "BEGIN_TRANSACTION"}}
        })))
    });
    let failure = begin_in_new_session_at(&endpoint, Some("EU")).unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidData,
        &["without returning the session's ID"],
    );
    captured.served(2);
}
