//! Manual transactions at statement scope: what joins the connection's session, what is refused
//! outside autocommit, and what a failed lazy begin leaves behind.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use super::*;
use crate::BigQueryConnection;
use crate::test_support::http::{capture_statements, capture_statements_gated};

const INSERT: &str = "INSERT dataset.table VALUES (42)";

/// The staged answer to the `BEGIN TRANSACTION` that creates `mock-session`.
fn session_created() -> (u16, serde_json::Value) {
    completed_job(
        "session-job",
        json!({"sessionInfo": {"sessionId": "mock-session"}}),
    )
}

fn finished() -> (u16, serde_json::Value) {
    completed_job("job", json!({}))
}

fn sql_of(requests: &[CapturedRequest]) -> Vec<&str> {
    requests.iter().map(CapturedRequest::sql).collect()
}

/// A manual-commit connection at `endpoint`, and a statement of it running [`INSERT`].
fn manual_insert_at(endpoint: &str) -> (BigQueryConnection, BigQueryStatement) {
    let mut connection = test_connection_at(endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(INSERT).unwrap();
    (connection, statement)
}

/// A statement at `endpoint` whose transaction has begun in `session`.
fn statement_in_session(endpoint: &str, session: &str) -> BigQueryStatement {
    let statement = test_statement_at(endpoint);
    statement.transaction.disable_autocommit().unwrap();
    start_transaction(&statement, session);
    statement
}

/// Serves `responses`, reporting the first request and then holding its answer until released.
fn capture_statements_held(
    responses: Vec<(u16, serde_json::Value)>,
) -> (String, CapturedServer, Receiver<()>, SyncSender<()>) {
    let (request_seen, wait_for_request) = sync_channel(0);
    let (release_response, response_released) = sync_channel(0);
    let (endpoint, requests) = capture_statements_gated(responses, move || {
        request_seen.send(()).unwrap();
        response_released.recv().unwrap();
        true
    });
    (endpoint, requests, wait_for_request, release_response)
}

#[test]
fn enabling_autocommit_commits_pending_manual_work() {
    let (endpoint, requests) =
        capture_statements(vec![session_created(), finished(), finished(), finished()]);
    let (mut connection, mut statement) = manual_insert_at(&endpoint);
    assert_eq!(statement.execute_update().unwrap(), None);

    set_option(&mut connection, OptionConnection::AutoCommit, true);
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );

    let requests = requests.join().unwrap();
    assert_eq!(
        sql_of(&requests),
        [
            "BEGIN TRANSACTION",
            INSERT,
            "COMMIT TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
    // One job creates the session and begins the transaction in it.
    assert_eq!(
        requests[0].body["configuration"]["query"]["createSession"],
        true
    );
    assert!(requests[0].connection_properties().is_null());
}

/// A `BEGIN` refused in the session it created leaves no transaction and no session to reuse:
/// the caller's query does not run, and the next statement begins in a new session.
#[test]
fn failed_lazy_begin_does_not_execute_the_caller_query_or_enable_autocommit() {
    let (status, mut refusal) =
        google_rejection(400, "INVALID_ARGUMENT", "", "injected BEGIN failure");
    refusal["sessionInfo"] = json!({"sessionId": "mock-session"});
    let (endpoint, requests) = capture_statements(vec![(status, refusal)]);
    let (connection, mut statement) = manual_insert_at(&endpoint);

    assert!(statement.execute_update().is_err());
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_DISABLED
    );
    assert_eq!(sql_of(&requests.join().unwrap()), ["BEGIN TRANSACTION"]);
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// A statement that cannot run for want of a project says so with the same status in manual-commit
/// mode, where the failure comes from the `BEGIN` that would create the session, as in autocommit.
#[test]
fn a_manual_commit_statement_without_a_project_fails_as_in_autocommit() {
    let project = (crate::BigQueryDatabaseOption::ProjectId.into(), "".into());
    let mut connection = crate::test_support::driver::test_connection_with(
        crate::test_support::UNREACHABLE_ENDPOINT,
        [project],
    );
    for autocommit in [true, false] {
        set_option(&mut connection, OptionConnection::AutoCommit, autocommit);
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query(INSERT).unwrap();
        let failure = statement.execute_update().unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidState,
            &["a BigQuery project must be configured"],
        );
    }
}

/// A `BEGIN` whose job names no session fails the statement with `INVALID_DATA`, before the
/// caller's query runs, and leaves no session to end or reuse.
#[test]
fn a_session_creating_begin_that_names_no_session_fails_the_statement() {
    let (endpoint, requests) = capture_statements(vec![finished()]);
    let (_connection, mut statement) = manual_insert_at(&endpoint);

    let failure = statement.execute_update().unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidData,
        &["without returning the session's ID"],
    );
    assert_eq!(sql_of(&requests.join().unwrap()), ["BEGIN TRANSACTION"]);
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// A cancel that lands while the `BEGIN` creating the session is in flight still ends the
/// session BigQuery creates, and leaves the next statement to open a fresh one.
#[test]
fn cancelling_while_the_session_is_created_still_ends_the_session() {
    let (endpoint, requests, wait_for_request, release_response) =
        capture_statements_held(vec![session_created(), finished()]);
    let (_connection, mut statement) = manual_insert_at(&endpoint);
    let cancel = statement.get_cancel_handle();
    let canceller = std::thread::spawn(move || {
        wait_for_request.recv().unwrap();
        cancel.try_cancel().unwrap();
        release_response.send(()).unwrap();
    });

    let failure = statement.execute_update().unwrap_err();
    canceller.join().unwrap();

    assert_eq!(failure.status, Status::Cancelled, "{failure}");
    let requests = requests.join().unwrap();
    assert_eq!(
        sql_of(&requests),
        ["BEGIN TRANSACTION", "CALL BQ.ABORT_SESSION('mock-session')"]
    );
    assert_eq!(
        *requests[1].connection_properties(),
        json!([{"key": "session_id", "value": "mock-session"}])
    );
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// A connection released while a statement opens its transaction leaves nothing to commit that
/// transaction, so the statement reports the release and ends the session it opened.
#[test]
fn releasing_the_connection_while_a_statement_opens_its_transaction_ends_the_session() {
    let (endpoint, requests, wait_for_request, release_response) =
        capture_statements_held(vec![session_created(), finished()]);
    let (connection, mut statement) = manual_insert_at(&endpoint);
    let releaser = std::thread::spawn(move || {
        wait_for_request.recv().unwrap();
        drop(connection);
        release_response.send(()).unwrap();
    });

    let failure = statement.execute_update().unwrap_err();
    releaser.join().unwrap();

    assert_failure(&failure, Status::InvalidState, &["released"]);
    let requests = requests.join().unwrap();
    assert_eq!(
        sql_of(&requests),
        ["BEGIN TRANSACTION", "CALL BQ.ABORT_SESSION('mock-session')"]
    );
    assert_eq!(
        *requests[1].connection_properties(),
        json!([{"key": "session_id", "value": "mock-session"}])
    );
    // The mock has finished, so a request would fail with an I/O error rather than this one.
    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::InvalidState
    );
}

/// Every execute refuses an ingest in manual-commit mode, before it looks for bound data. So does
/// a dry run, which could only report this same failure.
#[test]
fn ingest_inside_a_transaction_is_rejected_by_every_execute_entrypoint() {
    for dry_run in [false, true] {
        for execute in EXECUTE_ENTRIES {
            let mut statement = test_statement();
            statement.transaction.disable_autocommit().unwrap();
            set_option(&mut statement, OptionStatement::TargetTable, "events");
            set_option(&mut statement, BqOption::DryRun, dry_run);
            let error = execute(&mut statement).unwrap_err();
            assert_failure(
                &error,
                Status::InvalidState,
                &["transaction", "adbc.connection.autocommit"],
            );
        }
    }
}

/// A read has nothing a later commit or rollback could appear to cover, so it runs where the
/// option-only operations that change something are refused.
#[test]
fn a_dataset_location_lookup_runs_with_autocommit_disabled() {
    let (endpoint, captured) = capture_statements(vec![ok(json!({"location": "EU"}))]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BqOption::GetDatasetLocation,
        r#"{"project":"data-project","dataset":"analytics"}"#,
    );
    statement.transaction.disable_autocommit().unwrap();

    let (_, rows_affected) = statement.execute_with_rows_affected().unwrap();

    assert_eq!(rows_affected, 1);
    captured.join().unwrap();
}

/// The manual-transaction refusal is about ingest alone: a query inside a transaction still runs,
/// and runs in the session the transaction opened.
#[test]
fn queries_are_unaffected_by_the_ingest_transaction_rejection() {
    let (endpoint, requests) = capture_statements(vec![completed_job(
        "job",
        json!({"numDmlAffectedRows": "1"}),
    )]);
    let mut statement = statement_in_session(&endpoint, "fixture-session");
    statement.set_sql_query(INSERT).unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(1));
    let requests = requests.join().unwrap();
    assert_eq!(sql_of(&requests), [INSERT]);
    assert_eq!(
        *requests[0].connection_properties(),
        json!([{"key": "session_id", "value": "fixture-session"}])
    );
}

#[test]
fn empty_parameter_update_does_not_start_a_manual_transaction() {
    let mut statement = test_statement();
    statement.transaction.disable_autocommit().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (?)")
        .unwrap();
    bind_no_rows(&mut statement);

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    assert!(!statement.transaction.is_autocommit().unwrap());
    assert!(statement.transaction.session_id().unwrap().is_none());
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// Every query in manual-commit mode runs in the transaction's session, and BigQuery refuses a
/// job that names a session and asks for a new one. Each query entry point refuses before it
/// begins the transaction or consumes the binding.
#[test]
fn session_creation_is_refused_inside_a_transaction_before_anything_runs() {
    let mut statement = test_statement();
    statement.transaction.disable_autocommit().unwrap();
    statement.set_sql_query("SELECT ?").unwrap();
    set_option(
        &mut statement,
        BqOption::CreateSession,
        ADBC_OPTION_VALUE_ENABLED,
    );
    statement.bind(int64_column("value", vec![1])).unwrap();

    let failures = [
        statement.execute().err().unwrap(),
        statement.execute_update().unwrap_err(),
        statement.execute_partitions().unwrap_err(),
        statement.execute_schema().unwrap_err(),
        statement.get_parameter_schema().unwrap_err(),
    ];
    for failure in failures {
        assert_eq!(failure.status, Status::InvalidState, "{}", failure.message);
        assert_eq!(
            failure.message,
            "BigQuery option bigquery.query.create_session cannot be enabled while \
             adbc.connection.autocommit is disabled: the statement runs in the transaction's \
             BigQuery session; set bigquery.query.create_session to false or re-enable autocommit"
        );
    }
    assert!(
        statement.parameters.schema().is_some(),
        "the binding was consumed"
    );
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

#[test]
fn each_bound_transaction_boundary_starts_the_next_transaction() {
    for boundary in ["COMMIT", "ROLLBACK"] {
        for execute in EXECUTE_ENTRIES {
            let ended = ok(json!({
                "jobComplete": true,
                "statementType": format!("{boundary}_TRANSACTION"),
                "jobReference": {"projectId": "fixture-project", "jobId": "boundary", "location": "EU"}
            }));
            let (endpoint, requests) = capture_statements(vec![ended.clone(), finished(), ended]);
            let mut statement = statement_in_session(&endpoint, "bound-session");
            let sql = format!("{boundary} TRANSACTION");
            statement.set_sql_query(&sql).unwrap();
            statement
                .bind(
                    RecordBatch::try_new_with_options(
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(2)),
                    )
                    .unwrap(),
                )
                .unwrap();

            execute(&mut statement).unwrap();
            let requests = requests.join().unwrap();
            assert_eq!(
                sql_of(&requests),
                [sql.as_str(), "BEGIN TRANSACTION", sql.as_str()]
            );
            for request in &requests {
                assert_eq!(
                    *request.connection_properties(),
                    json!([{"key": "session_id", "value": "bound-session"}])
                );
            }
        }
    }
}

#[test]
fn releasing_a_manual_connection_prevents_later_parameter_rows() {
    let (endpoint, requests) = capture_statements(vec![select_answer(&["1"])]);
    let mut statement = statement_in_session(&endpoint, "bound-session");
    statement.set_sql_query("SELECT ? AS value").unwrap();
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
    statement.transaction.release().unwrap();

    let ArrowError::ExternalError(failure) = reader.next().unwrap().unwrap_err() else {
        panic!("expected transaction state error")
    };
    assert_failure(
        failure.downcast_ref::<Error>().unwrap(),
        Status::InvalidState,
        &["released", "parameter row 1"],
    );
    assert!(reader.next().is_none());
    requests.served(1);
}

#[test]
fn a_lazy_parameter_row_joins_the_transaction_after_an_adbc_boundary() {
    for boundary in ["commit", "rollback"] {
        let (endpoint, requests) = capture_statements(vec![
            select_answer(&["1"]),
            finished(),
            select_answer(&["2"]),
        ]);
        let mut statement = statement_in_session(&endpoint, "bound-session");
        statement.set_sql_query("SELECT ? AS value").unwrap();
        statement.bind(int64_column("value", vec![1, 2])).unwrap();
        let mut reader = statement.execute().unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

        // The state transition performed by a successful connection commit/rollback.
        let crate::transaction::FinishTransaction::Active(session, finish) =
            statement.transaction.begin_finish(boundary).unwrap()
        else {
            panic!("expected an active transaction")
        };
        finish.succeeded(session).unwrap();

        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().is_none());
        assert_eq!(
            sql_of(&requests.join().unwrap()),
            [
                "SELECT ? AS value",
                "BEGIN TRANSACTION",
                "SELECT ? AS value"
            ]
        );
        assert!(matches!(
            statement.transaction.begin_query().unwrap(),
            QueryTransaction::Active(_)
        ));
    }
}
