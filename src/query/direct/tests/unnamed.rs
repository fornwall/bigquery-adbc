//! Statements whose responses never named their statement type or session: a replayed
//! `jobs.query` and a `jobs.insert` answered RUNNING, both completed by `jobs.getQueryResults`,
//! which carries neither.

use super::*;
use crate::query::{ExecutionContext, StatementQueryContext};
use crate::test_support::http::{capture_requests_responding, ok};
use crate::transaction::{QueryTransaction, TransactionState};

fn active_transaction() -> TransactionState {
    let transaction = TransactionState::default();
    transaction.disable_autocommit().unwrap();
    match transaction.begin_query().unwrap() {
        QueryTransaction::Start(None, start) => start.started(Arc::from("session")).unwrap(),
        other => panic!("{other:?}"),
    }
    transaction
}

fn still_active(transaction: &TransactionState) -> bool {
    matches!(
        transaction.begin_query().unwrap(),
        QueryTransaction::Active(_)
    )
}

/// Runs `COMMIT TRANSACTION` in the transaction's session against `endpoint`.
fn run_commit(endpoint: &str, options: &StatementQueryOptions, transaction: &TransactionState) {
    let runtime = new_runtime().unwrap();
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint);
    let execution = ExecutionContext {
        runtime: runtime.clone(),
        state: state.clone(),
        billing_project_id: Some(ProjectIdentifier::try_new_from("test", "project").unwrap()),
        current_catalog: None,
        current_schema: None,
        location: Some(Arc::from("EU")),
        result_options: crate::pipeline::QueryResultOptions::default(),
    };
    let operation = Cancellation::new(runtime, state, "statement").begin();
    let context = StatementQueryContext {
        execution: &execution,
        session_id: Some("session"),
        operation: &operation,
        options,
        transaction,
        progress: None,
    };
    complete_statement_query(
        context,
        &Arc::from("COMMIT TRANSACTION"),
        None,
        QueryJobCreation::Optional,
        QueryResultMode::MetadataOnly,
        "test",
    )
    .unwrap();
}

/// A lost answer, the replay's 409 naming its job, and that job's results.
fn replayed(then: Vec<(u16, serde_json::Value)>) -> Vec<(u16, serde_json::Value)> {
    let mut responses = vec![
        (503, error_body("backendError", "lost")),
        (
            409,
            json!({"error": {"code": 409, "errors": [{"reason": "duplicate",
                "message": "Already Exists: Job project:EU.job_first"}]}}),
        ),
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job_first","location":"EU"}
        })),
    ];
    responses.extend(then);
    responses
}

fn job_first(statistics: &serde_json::Value) -> (u16, serde_json::Value) {
    ok(json!({
        "jobReference": {"projectId":"project","jobId":"job_first","location":"EU"},
        "status": {"state":"DONE"},
        "statistics": statistics
    }))
}

/// The replay names only the job, so one `jobs.get` learns that it committed.
#[test]
fn a_replayed_commit_ends_the_transaction() {
    let (endpoint, captured) = capture_requests(replayed(vec![job_first(
        &json!({"query": {"statementType": "COMMIT_TRANSACTION"}}),
    )]));
    let transaction = active_transaction();
    run_commit(&endpoint, &StatementQueryOptions::default(), &transaction);

    let captured = captured.served(4);
    assert!(
        captured[3]
            .path()
            .starts_with("/bigquery/v2/projects/project/jobs/job_first?")
    );
    assert!(!still_active(&transaction));
}

/// A RUNNING `jobs.insert` answer already names the statement type, which the poll that completes
/// it does not, so no `jobs.get` is needed.
#[test]
fn an_inserted_commit_ends_the_transaction_from_its_running_answer() {
    let mut index = 0;
    let (endpoint, captured) = capture_requests_responding(2, move |request| {
        index += 1;
        Some(if index == 1 {
            ok(json!({
                "jobReference": request.body["jobReference"].clone(),
                "status": {"state": "RUNNING"},
                "statistics": {"query": {"statementType": "COMMIT_TRANSACTION"}}
            }))
        } else {
            let job_id = request.path().split("/queries/").nth(1).unwrap();
            let job_id = job_id.split('?').next().unwrap().to_owned();
            ok(json!({
                "jobComplete": true,
                "jobReference": {"projectId":"project","jobId":job_id,"location":"EU"}
            }))
        })
    });
    let transaction = active_transaction();
    let options = StatementQueryOptions {
        priority: BigQueryQueryPriority::Batch,
        ..StatementQueryOptions::default()
    };
    run_commit(&endpoint, &options, &transaction);

    captured.served(2);
    assert!(!still_active(&transaction));
}
