use adbc_core::CancelHandle as _;
use serde_json::{Value, json};

use super::*;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::runtime::new_runtime;
use crate::test_support::UNREACHABLE_ENDPOINT;
use crate::test_support::http::{capture_requests, error_body};

fn metadata(statement_type: &str, job: bool) -> QueryMetadata {
    QueryMetadata {
        statement_type: statement_type.to_owned(),
        job_reference: job.then(|| JobReference::new("project", Some("US"), "script".to_owned())),
        ..QueryMetadata::default()
    }
}

/// The newest child as `jobs.list` with the full projection reports it.
fn child(statement_type: &str, in_transaction: bool) -> Value {
    let mut statistics = json!({"query": {"statementType": statement_type}});
    if in_transaction {
        statistics["transactionInfo"] = json!({"transactionId": "transaction"});
    }
    json!({"jobs": [{"statistics": statistics}]})
}

/// Decides for `metadata` against a mock answering `responses`, and returns the decision with
/// the number of requests made.
fn decide(metadata: &QueryMetadata, responses: Vec<(u16, Value)>) -> (Result<bool>, usize) {
    let (endpoint, captured) = capture_requests(responses);
    let runtime = new_runtime().unwrap();
    let decision = runtime.block_on(ended_transaction(
        &JobClient::for_test(&endpoint),
        None,
        metadata,
    ));
    (decision, captured.join().unwrap().len())
}

#[test]
fn a_lone_statement_needs_no_request() {
    for (statement_type, ended) in [
        ("COMMIT_TRANSACTION", true),
        ("ROLLBACK_TRANSACTION", true),
        ("SELECT", false),
        ("INSERT", false),
        ("BEGIN_TRANSACTION", false),
    ] {
        let (decision, requests) = decide(&metadata(statement_type, true), Vec::new());
        assert_eq!(decision.unwrap(), ended, "{statement_type}");
        assert_eq!(requests, 0, "{statement_type}");
    }
    // A jobless script has no children to inspect.
    let (decision, requests) = decide(&metadata("SCRIPT", false), Vec::new());
    assert!(!decision.unwrap());
    assert_eq!(requests, 0);
}

/// The shapes observed live: a script ends the transaction when its newest child commits, rolls
/// back, or runs outside any transaction; a child inside one, including a transaction the script
/// began itself, leaves a transaction active.
#[test]
fn a_scripts_newest_child_decides() {
    for (newest, ended) in [
        (child("COMMIT_TRANSACTION", true), true),
        (child("ROLLBACK_TRANSACTION", true), true),
        (child("SELECT", false), true),
        (child("SELECT", true), false),
        (child("BEGIN_TRANSACTION", true), false),
        (child("CREATE_TABLE_AS_SELECT", true), false),
        // Nothing ran in a child job, or nothing says what the child was.
        (json!({}), false),
        (json!({"jobs": [{}]}), false),
        (json!({"jobs": [{"statistics": {}}]}), false),
    ] {
        for statement_type in ["SCRIPT", "CALL"] {
            let (decision, requests) =
                decide(&metadata(statement_type, true), vec![(200, newest.clone())]);
            assert_eq!(decision.unwrap(), ended, "{statement_type} {newest}");
            assert_eq!(requests, 1);
        }
    }
}

/// `bigquery.jobs.list` is a permission running queries does not need, so a refused listing
/// leaves the transaction as the driver knew it rather than failing a statement that ran.
#[test]
fn a_refused_or_unreadable_listing_leaves_the_transaction_active() {
    for response in [
        (403, error_body("accessDenied", "no bigquery.jobs.list")),
        (200, json!({"jobs": "not a list"})),
    ] {
        let (decision, requests) = decide(&metadata("SCRIPT", true), vec![response]);
        assert!(!decision.unwrap());
        assert_eq!(requests, 1);
    }
}

#[test]
fn a_cancelled_listing_reports_the_cancellation() {
    let runtime = new_runtime().unwrap();
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, UNREACHABLE_ENDPOINT);
    let cancellation = Cancellation::new(runtime.clone(), state, "statement");
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    let failure = runtime
        .block_on(ended_transaction(
            &JobClient::for_test(UNREACHABLE_ENDPOINT),
            Some(&operation),
            &metadata("SCRIPT", true),
        ))
        .unwrap_err();
    assert_eq!(failure.status, Status::Cancelled);
}
