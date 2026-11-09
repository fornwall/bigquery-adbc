//! Connection release and session lifecycle: what a released connection still owes BigQuery,
//! and what it must not issue.

use super::*;
use crate::test_support::driver::unlocated_connection_at;
use crate::test_support::errors::assert_message_omits;
use crate::test_support::http::{capture_statements, capture_statements_responding, done_job};
use crate::test_support::options::{option_string, set_option, set_option_error};

/// What every [`connection_with_open_transaction`] exchange opens with.
const PREAMBLE: [&str; 3] = [
    "SELECT 1",
    "BEGIN TRANSACTION",
    "INSERT dataset.table VALUES (42)",
];

/// A connection carrying an open transaction with one statement already executed inside it,
/// and the handle that yields the requests the mock captured.
///
/// The five queued responses answer the preamble's three requests (the session-creating query,
/// `BEGIN TRANSACTION`, the INSERT) and the two a release can issue.
fn connection_with_open_transaction() -> (crate::BigQueryConnection, CapturedServer) {
    connection_with_open_transaction_then(vec![(200, complete()), (200, complete())])
}

fn complete() -> serde_json::Value {
    json!({
        "jobComplete": true,
        "jobReference": {"projectId":"fixture-project","jobId":"lifecycle-job"}
    })
}

fn session_created(session_id: &str) -> serde_json::Value {
    let mut response = complete();
    response["sessionInfo"] = json!({"sessionId": session_id});
    response
}

/// [`connection_with_open_transaction`], with `then` answering the requests after the preamble.
fn connection_with_open_transaction_then(
    then: Vec<(u16, serde_json::Value)>,
) -> (crate::BigQueryConnection, CapturedServer) {
    let mut responses = vec![
        (200, session_created("mock-session")),
        (200, complete()),
        (200, complete()),
    ];
    responses.extend(then);
    let (endpoint, requests) = capture_statements(responses);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    (connection, requests)
}

/// Runs `sql` in a statement of its own.
fn execute(
    connection: &mut crate::BigQueryConnection,
    sql: &str,
) -> adbc_core::error::Result<Option<i64>> {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement.execute_update()
}

/// Asserts that the exchange was exactly the preamble, `finish` (the statement that ended the
/// transaction), and the session abort.
fn assert_transaction_cleanup(requests: CapturedServer, finish: &str) {
    assert_sql_after_preamble(requests, &[finish, "CALL BQ.ABORT_SESSION('mock-session')"]);
}

/// [`assert_sql`] for an exchange that opens with [`PREAMBLE`].
fn assert_sql_after_preamble(requests: CapturedServer, rest: &[&str]) {
    assert_sql(requests, &[&PREAMBLE, rest].concat());
}

/// Asserts the SQL of every statement the mock captured, in order.
fn assert_sql(requests: CapturedServer, expected: &[&str]) {
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.line.starts_with("POST"))
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        expected
    );
}

/// A released connection ends its session rather than leaving its open transaction's locks to
/// BigQuery's session timeout.
#[test]
fn releasing_a_connection_rolls_back_and_aborts_its_open_transaction() {
    let (connection, requests) = connection_with_open_transaction();
    drop(connection);
    assert_transaction_cleanup(requests, "ROLLBACK TRANSACTION");
}

/// A committed transaction leaves its session for the next one, so release still ends it, with
/// nothing to roll back.
#[test]
fn releasing_a_connection_aborts_an_idle_session_without_a_rollback() {
    let (mut connection, requests) = connection_with_open_transaction();
    connection.commit().unwrap();
    drop(connection);
    assert_transaction_cleanup(requests, "COMMIT TRANSACTION");
}

/// A `fork` child inherits the connection but not the session, which the parent may still be
/// using, so a release there issues nothing.
#[test]
fn releasing_a_connection_in_a_forked_child_leaves_the_session_to_the_parent() {
    let (mut connection, requests) = connection_with_open_transaction_then(Vec::new());
    connection.context.runtime = crate::runtime::orphaned_runtime();
    drop(connection);
    requests.served(3);
}

/// An autocommit connection opened no session, so releasing it must issue nothing at all.
#[test]
fn releasing_an_autocommit_connection_issues_no_cleanup() {
    let (endpoint, requests) = capture_requests(Vec::new());
    drop(test_connection_at(&endpoint));
    assert!(requests.join().unwrap().is_empty());
}

/// dbt 2.0 consumes the driver as an rlib inside its own Tokio runtime, so a release can
/// land on a worker thread where `block_on` panics. The cleanup still has to reach BigQuery.
#[test]
fn releasing_a_connection_inside_a_tokio_host_still_ends_its_session() {
    let (connection, requests) = connection_with_open_transaction();
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async move {
        drop(connection);
    });
    assert_transaction_cleanup(requests, "ROLLBACK TRANSACTION");
}

/// BigQuery's answer to `COMMIT` or `ROLLBACK` once the session's transaction is gone: the
/// session expired, or a script's own `COMMIT`/`ROLLBACK` ended it.
fn transaction_lost() -> serde_json::Value {
    error_body("invalidQuery", "No active transaction found")
}

/// BigQuery's answer to any query in a session that has ended: after 24 idle hours, or once
/// `BQ.ABORT_SESSION` ended it (verified live).
fn session_expired() -> serde_json::Value {
    error_body(
        "resourcesExceeded",
        "Session mock-session has expired and is no longer available.",
    )
}

/// The answers of a fresh session: its creation, `BEGIN`, one statement, and the release's
/// `ROLLBACK` and abort.
fn a_fresh_session() -> Vec<(u16, serde_json::Value)> {
    let mut answers = vec![(200, session_created("fresh-session"))];
    answers.extend(vec![(200, complete()); 4]);
    answers
}

/// The responses after the preamble when `finish` (a `COMMIT` or `ROLLBACK`) finds the
/// transaction gone with `failure`: the session abort, a fresh session for the next statement's
/// transaction, and the release that ends it.
fn transaction_lost_then_a_fresh_session(
    failure: serde_json::Value,
) -> Vec<(u16, serde_json::Value)> {
    [vec![(400, failure), (200, complete())], a_fresh_session()].concat()
}

/// Runs one more INSERT on `connection` after `finish` found the transaction gone, releases the
/// connection, and asserts the INSERT ran in a fresh session with a transaction of its own.
fn assert_the_next_statement_runs_in_a_fresh_session(
    mut connection: crate::BigQueryConnection,
    requests: CapturedServer,
    finish: &str,
) {
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            &format!("{finish} TRANSACTION"),
            "CALL BQ.ABORT_SESSION('mock-session')",
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('fresh-session')",
        ],
    );
}

/// A rollback that finds the transaction gone ends the session; otherwise later statements would
/// autocommit there and every rollback would fail the same way. The next statement opens a fresh
/// session with its own transaction.
#[test]
fn a_failed_rollback_ends_the_session_and_the_next_statement_starts_afresh() {
    let (mut connection, requests) = connection_with_open_transaction_then(
        transaction_lost_then_a_fresh_session(transaction_lost()),
    );

    let failure = connection.rollback().unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidState,
        &["No active transaction found", "session was ended"],
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_DISABLED
    );
    // Nothing is left to roll back.
    connection.rollback().unwrap();

    assert_the_next_statement_runs_in_a_fresh_session(connection, requests, "ROLLBACK");
}

/// A `COMMIT` inside a script ends BigQuery's transaction unseen, and every later statement
/// autocommits in the session. The ADBC commit that then finds no transaction must say so, and
/// end the session rather than keep it for a retry that fails the same way.
#[test]
fn a_commit_that_finds_the_transaction_gone_ends_the_session() {
    let (mut connection, requests) = connection_with_open_transaction_then(
        transaction_lost_then_a_fresh_session(transaction_lost()),
    );

    let failure = connection.commit().unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidState,
        &[
            "No active transaction found",
            "a script ran COMMIT or ROLLBACK",
            "next statement starts a new transaction",
        ],
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_DISABLED
    );
    assert_the_next_statement_runs_in_a_fresh_session(connection, requests, "COMMIT");
}

/// A session that expired with its transaction open took the transaction with it. The commit
/// must say so and leave manual mode without a session, rather than keep the dead session as an
/// active transaction that every later statement and commit fails in.
#[test]
fn a_commit_in_an_expired_session_reports_the_transaction_gone() {
    let (mut connection, requests) = connection_with_open_transaction_then(
        transaction_lost_then_a_fresh_session(session_expired()),
    );

    let failure = connection.commit().unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidState,
        &[
            "has expired and is no longer available",
            "its session expired, which rolled it back",
            "next statement starts a new transaction",
        ],
    );
    assert_the_next_statement_runs_in_a_fresh_session(connection, requests, "COMMIT");
}

/// A dry run describes a statement in the idle session a committed transaction left. Once
/// BigQuery has ended that session, the dry run is repeated outside it and the session is
/// forgotten, so the next statement opens a new one without first sending `BEGIN` to the ended
/// session.
#[test]
fn a_dry_run_in_an_expired_idle_session_describes_outside_it() {
    let described = json!({
        "statistics": {"query": {"statementType": "INSERT"}},
        "status": {"state": "DONE"}
    });
    let (mut connection, requests) = connection_with_open_transaction_then(
        [
            vec![
                (200, complete()),
                (400, session_expired()),
                (200, described),
            ],
            a_fresh_session(),
        ]
        .concat(),
    );
    connection.commit().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (43)")
        .unwrap();
    assert!(statement.execute_schema().unwrap().fields().is_empty());
    statement.execute_update().unwrap();
    drop(statement);
    drop(connection);

    let requests = requests.join().unwrap();
    // A dry run is a `jobs.insert`, which nests the session in its configuration.
    let session = |index: usize| requests[index].connection_properties().clone();
    assert_eq!(
        session(4),
        json!([{"key": "session_id", "value": "mock-session"}])
    );
    assert_eq!(session(5), serde_json::Value::Null);
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (42)",
            "COMMIT TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "INSERT dataset.table VALUES (43)",
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('fresh-session')",
        ]
    );
}

/// A committed transaction leaves its session for the next one, and BigQuery may end that session
/// before then (after 24 idle hours). Nothing of the transaction is lost with it, so the next
/// statement begins its transaction in a new session instead of failing.
#[test]
fn a_transaction_begins_in_a_new_session_when_the_previous_one_expired() {
    let (mut connection, requests) = connection_with_open_transaction_then(
        [
            vec![(200, complete()), (400, session_expired())],
            a_fresh_session(),
        ]
        .concat(),
    );
    connection.commit().unwrap();

    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('fresh-session')",
        ],
    );
}

/// BigQuery ends a session asynchronously, so it can accept the next transaction's `BEGIN` in a
/// session it is ending and then refuse the statement that sent it. That transaction held nothing,
/// so the statement begins it again in a new session and runs there, once, as a new request.
#[test]
fn a_statement_refused_right_after_its_begin_runs_again_in_a_new_session() {
    let (mut connection, requests) = connection_with_open_transaction_then(
        [
            vec![
                (200, complete()),
                (200, complete()),
                (400, session_expired()),
            ],
            a_fresh_session(),
        ]
        .concat(),
    );
    connection.commit().unwrap();
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (42)",
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('fresh-session')",
        ]
    );
    let session = |index: usize| requests[index].connection_properties()[0]["value"].clone();
    assert_eq!(session(5), "mock-session");
    assert_eq!(session(8), "fresh-session");
    assert_ne!(requests[5].body["requestId"], requests[8].body["requestId"]);
}

/// A statement after the one that began the transaction may follow work BigQuery ended with the
/// session, so its refusal reports the lost transaction instead of running again.
#[test]
fn a_later_statement_refused_in_an_ended_session_is_not_run_again() {
    let (mut connection, requests) = connection_with_open_transaction_then(vec![
        (400, session_expired()),
        (200, complete()),
        (200, complete()),
    ]);
    let failure = execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap_err();
    assert_message(&failure, &["has expired and is no longer available"]);
    drop(connection);
    assert_sql_after_preamble(
        requests,
        &[
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// Runs `sql` as the first statement of the transaction after the preamble's committed one,
/// binding `parameters` if given, and expects it to fail as `refusal` answers it; the BEGIN in
/// the previous session succeeds, and with a binding so does the first row. Returns the failure
/// and the SQL of every request, the release's included.
fn first_statement_refused(
    sql: &str,
    parameters: Option<Vec<i64>>,
    refusal: serde_json::Value,
) -> (adbc_core::error::Error, Vec<String>) {
    let mut responses = vec![(200, complete()), (200, complete())];
    if parameters.is_some() {
        responses.push((200, complete()));
    }
    responses.extend([(400, refusal), (200, complete()), (200, complete())]);
    let (mut connection, requests) = connection_with_open_transaction_then(responses);
    connection.commit().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    if let Some(values) = parameters {
        let column: arrow_array::ArrayRef = std::sync::Arc::new(Int64Array::from(values));
        let batch = arrow_array::RecordBatch::try_from_iter([("value", column)]).unwrap();
        statement.bind(batch).unwrap();
    }
    let failure = statement.execute_update().unwrap_err();
    drop(statement);
    drop(connection);
    let requests = requests.join().unwrap();
    let sql = requests.iter().map(|request| request.sql().to_owned());
    (failure, sql.collect())
}

/// Once a statement's earlier row ran in the transaction, the session ending took that row with
/// it: the refusal of the next row reports the lost transaction instead of running again.
#[test]
fn a_later_row_refused_in_an_ended_session_is_not_run_again() {
    let (failure, sql) = first_statement_refused(
        "INSERT dataset.table VALUES (?)",
        Some(vec![43, 44]),
        session_expired(),
    );
    assert_message(&failure, &["has expired and is no longer available"]);
    assert_eq!(
        sql[4..],
        [
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (?)",
            "INSERT dataset.table VALUES (?)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
}

/// A script the session's end stopped partway fails with the same message, reported as
/// `invalidQuery`, after its earlier statements ran and perhaps committed: it never runs again.
#[test]
fn a_script_stopped_by_its_session_ending_is_not_run_again() {
    let stopped = error_body(
        "invalidQuery",
        "Session mock-session has expired and is no longer available. at [2:1]",
    );
    let script = "INSERT dataset.table VALUES (43); COMMIT TRANSACTION";
    let (failure, sql) = first_statement_refused(script, None, stopped);
    assert_message(&failure, &["has expired and is no longer available"]);
    assert_eq!(
        sql[4..],
        [
            "BEGIN TRANSACTION",
            script,
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
}

/// Re-enabling autocommit commits the open transaction; when that finds the transaction gone,
/// the caller learns it, and autocommit is still enabled with the session ended.
#[test]
fn enabling_autocommit_over_a_lost_transaction_reports_it_and_enables_autocommit() {
    let (mut connection, requests) =
        connection_with_open_transaction_then(vec![(400, transaction_lost()), (200, complete())]);

    let failure = set_option_error(&mut connection, OptionConnection::AutoCommit, "true");
    assert_failure(
        &failure,
        Status::InvalidState,
        &["autocommit is now enabled"],
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );
    drop(connection);
    assert_transaction_cleanup(requests, "COMMIT TRANSACTION");
}

/// Releasing a manual-commit connection rolls its transaction back, so a statement that outlives
/// it must refuse to run rather than silently autocommit what its caller meant for a transaction.
#[test]
fn a_statement_outliving_a_manual_commit_connection_refuses_to_run() {
    let (endpoint, requests) = capture_requests(Vec::new());
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    drop(connection);

    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();
    let failure = statement.execute_update().unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidState,
        &["was released with autocommit disabled"],
    );
    assert!(requests.join().unwrap().is_empty());
}

/// Runs one statement in manual-commit mode on a connection without `bigquery.location` whose
/// default dataset is `analytics`, after `dataset` answers the lookup of that dataset's location,
/// and returns every request: the lookup, the session creation, `BEGIN`, the statement, and the
/// release's rollback and abort.
fn first_transaction_without_a_location(dataset: (u16, serde_json::Value)) -> Vec<CapturedRequest> {
    let mut responses = vec![dataset, (200, session_created("mock-session"))];
    responses.extend(std::iter::repeat_n((200, complete()), 4));
    let (endpoint, requests) = capture_statements(responses);
    let mut connection = crate::test_support::driver::test_connection_with(
        &endpoint,
        [
            (BigQueryDatabaseOption::Location.into(), "".into()),
            (BigQueryDatabaseOption::DatasetId.into(), "analytics".into()),
        ],
    );
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT events VALUES (42)").unwrap();
    drop(connection);

    let requests = requests.join().unwrap();
    assert!(
        requests[0]
            .path()
            .contains("/projects/fixture-project/datasets/analytics?fields=location"),
        "{}",
        requests[0].path()
    );
    assert_eq!(
        requests[1..]
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT events VALUES (42)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
    requests
}

/// Without `bigquery.location`, BigQuery creates a session in the US, and every query in it then
/// fails on data elsewhere with `Access Denied` (verified live with an EU dataset). The session
/// is created in the default dataset's location instead; the queries in it need none.
#[test]
fn a_session_without_a_configured_location_is_created_where_the_default_dataset_is() {
    let requests = first_transaction_without_a_location((200, json!({"location": "europe-west4"})));
    assert_eq!(requests[1].body["location"], "europe-west4");
    for request in &requests[2..] {
        assert!(request.body.get("location").is_none(), "{}", request.sql());
    }
}

/// The two ends of a request a [`holding`] mock is sitting on.
struct Held {
    seen: std::sync::mpsc::Receiver<()>,
    resume: std::sync::mpsc::SyncSender<()>,
}

impl Held {
    /// Cancels through `cancel` once the request arrives, then lets it finish.
    fn cancel_with(self, cancel: Box<dyn adbc_core::CancelHandle>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            self.seen.recv().unwrap();
            cancel.try_cancel().unwrap();
            self.resume.send(()).unwrap();
        })
    }
}

/// A mock of `count` statements that opens a session and holds `sql` until told to answer it.
fn holding(sql: &'static str, count: usize) -> (String, CapturedServer, Held) {
    let (arrived, seen) = std::sync::mpsc::sync_channel(0);
    let (resume, resumed) = std::sync::mpsc::sync_channel(0);
    let mut served = 0;
    let (endpoint, requests) = capture_statements_responding(count, move |request| {
        served += 1;
        if served == 1 {
            return Some((200, session_created("mock-session")));
        }
        if request.sql() == sql {
            arrived.send(()).unwrap();
            resumed.recv().unwrap();
        }
        Some((200, complete()))
    });
    (endpoint, requests, Held { seen, resume })
}

/// A `COMMIT` already sent can land after its caller is cancelled. Reporting it cancelled would
/// leave the transaction active in the driver while BigQuery autocommits every later statement in
/// the session, so a cancel waits for the commit and the next statement begins a new transaction.
#[test]
fn a_cancel_during_commit_waits_for_its_outcome() {
    let (endpoint, requests, held) = holding("COMMIT TRANSACTION", 8);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    let canceller = held.cancel_with(connection.get_cancel_handle());

    connection.commit().unwrap();
    canceller.join().unwrap();
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// A `BEGIN` BigQuery refuses in the session a committed transaction left leaves nothing
/// started, so the session, with its temporary tables and variables, stays for the next
/// statement instead of being ended.
#[test]
fn a_refused_begin_keeps_the_session_for_the_next_statement() {
    let (mut connection, requests) = connection_with_open_transaction_then(vec![
        (200, complete()),
        (400, error_body("invalidQuery", "mock refusal")),
        (200, complete()),
        (200, complete()),
        (200, complete()),
        (200, complete()),
    ]);
    connection.commit().unwrap();
    let sql = "INSERT dataset.table VALUES (43)";
    assert_message(
        &execute(&mut connection, sql).unwrap_err(),
        &["mock refusal"],
    );
    execute(&mut connection, sql).unwrap();
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// A cancel that lands while `BEGIN` is in flight waits for it: once sent it can begin the
/// transaction, which the driver must then know about. The statement reports the cancel, and
/// the next one runs in the transaction that began.
#[test]
fn a_cancel_during_begin_keeps_the_transaction_it_began() {
    let (endpoint, requests, held) = holding("BEGIN TRANSACTION", 5);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();
    let canceller = held.cancel_with(statement.get_cancel_handle());

    let failure = statement.execute_update().unwrap_err();
    canceller.join().unwrap();
    assert_eq!(failure.status, Status::Cancelled, "{failure}");
    statement.execute_update().unwrap();
    drop(statement);
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// The answers to a `COMMIT` that meets a server error on every attempt and whose job never
/// appears, which leaves BigQuery's verdict unknown.
fn commit_without_a_verdict() -> Vec<(u16, serde_json::Value)> {
    let unavailable = error_body("backendError", "Service unavailable");
    let mut answers = vec![(503, unavailable); crate::rest::retry::MAX_ATTEMPTS as usize];
    answers.extend(vec![
        (404, not_found_response());
        crate::rest::jobs::MAX_RECONCILIATION_ATTEMPTS as usize
    ]);
    answers
}

/// Re-enabling autocommit commits the open transaction; when that commit gets no verdict, the
/// session is ended as after a lost transaction, and the caller learns the outcome is unknown.
#[test]
fn enabling_autocommit_over_a_commit_without_a_verdict_says_the_outcome_is_unknown() {
    let mut then = commit_without_a_verdict();
    then.push((200, complete()));
    let (mut connection, requests) = connection_with_open_transaction_then(then);

    let failure = set_option_error(&mut connection, OptionConnection::AutoCommit, "true");
    assert_failure(
        &failure,
        Status::IO,
        &[
            "did not say whether the transaction was committed",
            "autocommit is now enabled",
        ],
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );
    drop(connection);
    let requests = requests.join().unwrap();
    assert_eq!(
        requests.last().map(CapturedRequest::sql),
        Some("CALL BQ.ABORT_SESSION('mock-session')")
    );
}

/// A `COMMIT` that only ever met server errors may still have landed. Keeping the transaction
/// active would let later statements autocommit unseen if it did, so the session is ended, the
/// caller is told the outcome is unknown, and the next statement begins in a fresh session.
#[test]
fn a_commit_without_a_verdict_ends_the_session_and_says_the_outcome_is_unknown() {
    let then = [
        commit_without_a_verdict(),
        vec![(200, complete())],
        a_fresh_session(),
    ];
    let (mut connection, requests) = connection_with_open_transaction_then(then.concat());

    let failure = connection.commit().unwrap_err();
    assert_failure(
        &failure,
        Status::IO,
        &[
            "did not say whether the transaction was committed",
            "next statement starts a new transaction",
        ],
    );
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    let commits = vec!["COMMIT TRANSACTION"; crate::rest::retry::MAX_ATTEMPTS as usize];
    let mut expected = vec![
        "SELECT 1",
        "BEGIN TRANSACTION",
        "INSERT dataset.table VALUES (42)",
    ];
    expected.extend(commits);
    expected.extend([
        "CALL BQ.ABORT_SESSION('mock-session')",
        "SELECT 1",
        "BEGIN TRANSACTION",
        "INSERT dataset.table VALUES (43)",
        "ROLLBACK TRANSACTION",
        "CALL BQ.ABORT_SESSION('fresh-session')",
    ]);
    assert_sql(requests, &expected);
}

/// BigQuery does not deduplicate `BEGIN`, `COMMIT` or `ROLLBACK` by `jobs.query` request ID, so
/// replaying one whose answer was lost runs it again, and the second run's failure hides that the
/// first took effect (verified live). A job ID does hold: the replay answers 409, and the job says
/// what happened. Serves `count` requests, running `lost` once and losing its first answer.
fn bigquery_losing_the_answer_to(lost: &'static str, count: usize) -> (String, CapturedServer) {
    let mut served = 0;
    let mut ran = false;
    capture_statements_responding(count, move |request| {
        served += 1;
        if request.line.starts_with("GET") {
            let job = request.path().rsplit('/').next().unwrap();
            return Some(done_job(job.split('?').next().unwrap(), json!({})));
        }
        if served == 1 {
            return Some((200, session_created("mock-session")));
        }
        if request.sql() != lost {
            return Some((200, complete()));
        }
        if !std::mem::replace(&mut ran, true) {
            return None;
        }
        Some(match request.body["jobReference"]["jobId"].as_str() {
            Some(job) => (
                409,
                error_body(
                    "duplicate",
                    &format!("Already Exists: Job fixture-project:EU.{job}"),
                ),
            ),
            None => (400, transaction_lost()),
        })
    })
}

/// A lost answer leaves the statement run exactly once, under one job, and reports what it did:
/// the caller sees neither a transaction it committed as lost nor one it began as refused.
#[test]
fn a_control_statement_whose_answer_is_lost_reports_what_it_did() {
    for (lost, rollback) in [
        ("BEGIN TRANSACTION", false),
        ("COMMIT TRANSACTION", false),
        ("ROLLBACK TRANSACTION", true),
    ] {
        let (endpoint, requests) = bigquery_losing_the_answer_to(lost, 7);
        let mut connection = test_connection_at(&endpoint);
        set_option(&mut connection, OptionConnection::AutoCommit, false);
        execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
        if rollback {
            connection.rollback().unwrap();
        } else {
            connection.commit().unwrap();
        }
        drop(connection);

        let requests = requests.join().unwrap();
        let attempts = requests
            .iter()
            .filter(|request| request.line.starts_with("POST") && request.sql() == lost)
            .map(|request| request.body["jobReference"]["jobId"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(attempts, [attempts[0]; 2], "{lost}");
    }
}

/// How BigQuery answers a request about `sql` in `mock-session` after its first `skip` runs, and
/// every `GET`, through `answer`. Serves `count` requests, handing out two sessions.
fn bigquery_giving_no_verdict_on(
    sql: &'static str,
    skip: usize,
    count: usize,
    mut answer: impl FnMut(&CapturedRequest) -> Option<(u16, serde_json::Value)> + Send + 'static,
) -> (String, CapturedServer) {
    let mut sessions = vec!["fresh-session", "mock-session"];
    let mut runs = 0;
    capture_statements_responding(count, move |request| {
        if request.line.starts_with("GET") {
            return answer(request);
        }
        if request.sql() == "SELECT 1" {
            return Some((200, session_created(sessions.pop().unwrap())));
        }
        if request.sql() == sql && request.connection_properties()[0]["value"] == "mock-session" {
            runs += 1;
            if runs > skip {
                return answer(request);
            }
        }
        Some((200, complete()))
    })
}

/// A statement whose job BigQuery accepted but never shows: its first answer is lost, the replay's
/// 409 names the job, and `jobs.get` keeps answering 404 through the propagation delay.
fn a_lost_answer_and_an_invisible_job()
-> impl FnMut(&CapturedRequest) -> Option<(u16, serde_json::Value)> + Send + 'static {
    let mut sent = false;
    move |request| {
        if request.line.starts_with("GET") {
            return Some((404, not_found_response()));
        }
        if !std::mem::replace(&mut sent, true) {
            return None;
        }
        let job = request.body["jobReference"]["jobId"].as_str().unwrap();
        let message = format!("Already Exists: Job fixture-project:EU.{job}");
        Some((409, error_body("duplicate", &message)))
    }
}

/// Runs a transaction whose `COMMIT`, sent `commits` times, gets no verdict, and asserts the session
/// ended: a commit that landed would leave every later statement autocommitting in it.
fn assert_a_commit_without_a_verdict_ends_the_session(
    mut connection: crate::BigQueryConnection,
    requests: CapturedServer,
    commits: usize,
) {
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    let failure = connection.commit().unwrap_err();
    assert_failure(
        &failure,
        Status::IO,
        &[
            "did not say whether the transaction was committed",
            "The session was ended",
        ],
    );
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    let mut expected = PREAMBLE.to_vec();
    expected.extend(vec!["COMMIT TRANSACTION"; commits]);
    expected.extend([
        "CALL BQ.ABORT_SESSION('mock-session')",
        "SELECT 1",
        "BEGIN TRANSACTION",
        "INSERT dataset.table VALUES (43)",
        "ROLLBACK TRANSACTION",
        "CALL BQ.ABORT_SESSION('fresh-session')",
    ]);
    assert_sql(requests, &expected);
}

/// The 409 proves the job exists, so a job `jobs.get` cannot see yet says nothing about the commit.
#[test]
fn a_commit_whose_job_stays_invisible_has_no_verdict() {
    let (endpoint, requests) = bigquery_giving_no_verdict_on(
        "COMMIT TRANSACTION",
        0,
        14,
        a_lost_answer_and_an_invisible_job(),
    );
    assert_a_commit_without_a_verdict_ends_the_session(test_connection_at(&endpoint), requests, 2);
}

/// Without a location the job is searched for, and a principal without `bigquery.jobs.list` is
/// refused the search. That refusal is about the listing, not the commit.
#[test]
fn a_refused_search_for_a_commit_is_no_verdict() {
    let (endpoint, requests) =
        bigquery_giving_no_verdict_on("COMMIT TRANSACTION", 0, 18, |request| {
            Some(if !request.line.starts_with("GET") {
                (503, error_body("backendError", "Service unavailable"))
            } else if request.path().contains("/jobs/") {
                (404, not_found_response())
            } else {
                (403, error_body("accessDenied", "needs bigquery.jobs.list"))
            })
        });
    assert_a_commit_without_a_verdict_ends_the_session(
        unlocated_connection_at(&endpoint),
        requests,
        crate::rest::retry::MAX_ATTEMPTS as usize,
    );
}

/// Kept, a session whose `BEGIN` landed would hold an open transaction the driver does not know
/// of, and every later `BEGIN` there would be refused with "Only one transaction at a time".
#[test]
fn a_begin_without_a_verdict_ends_the_session_rather_than_wedging_it() {
    let (endpoint, requests) = bigquery_giving_no_verdict_on(
        "BEGIN TRANSACTION",
        1,
        15,
        a_lost_answer_and_an_invisible_job(),
    );
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    connection.commit().unwrap();

    let sql = "INSERT dataset.table VALUES (43)";
    let failure = execute(&mut connection, sql).unwrap_err();
    assert_failure(
        &failure,
        Status::IO,
        &["whether the transaction began is unknown"],
    );
    execute(&mut connection, sql).unwrap();
    drop(connection);
    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "BEGIN TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
            "SELECT 1",
            "BEGIN TRANSACTION",
            sql,
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('fresh-session')",
        ],
    );
}

/// A job that reports its own failure is a verdict: the transaction stays open for a retry or a
/// rollback, and the next statement joins it without a `BEGIN`.
#[test]
fn a_commit_bigquery_refused_keeps_the_transaction() {
    let (mut connection, requests) = connection_with_open_transaction_then(vec![
        (400, error_body("invalidQuery", "mock refusal")),
        (200, complete()),
        (200, complete()),
        (200, complete()),
    ]);
    let failure = connection.commit().unwrap_err();
    assert_message(&failure, &["mock refusal"]);
    assert_message_omits(&failure, &["session was ended"]);
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);
    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// A `COMMIT TRANSACTION` in the caller's SQL runs on `jobs.query`, so losing its answer reports
/// "No active transaction found" from the replay. That answer proves the session has no
/// transaction, so the next statement begins one rather than autocommitting there unseen. It says
/// nothing of the lost attempt, which may have committed: a host that took it for a refusal and
/// ran the transaction again would duplicate its writes.
#[test]
fn a_statement_told_no_transaction_is_active_makes_the_next_one_begin_again() {
    let (endpoint, requests) = bigquery_losing_the_answer_to("COMMIT TRANSACTION", 9);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    let failure = execute(&mut connection, "COMMIT TRANSACTION").unwrap_err();
    assert_message(
        &failure,
        &[
            "No active transaction found",
            "BigQuery may still have run this statement",
        ],
    );
    assert_message_omits(&failure, &["BigQuery rejected the query"]);
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);
    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}

/// `CALL BQ.ABORT_SESSION` queues behind whatever its session is running, and no one can cancel
/// it, so the caller stops waiting for it. The job keeps running, so the session may yet end, and
/// the caller claims neither outcome.
#[test]
fn a_session_end_that_gets_no_answer_does_not_hold_the_caller_or_read_as_done() {
    let (release, released) = std::sync::mpsc::channel();
    let mut served = 0;
    let (endpoint, requests) = capture_statements_responding(5, move |request| {
        served += 1;
        if served == 1 {
            return Some((200, session_created("mock-session")));
        }
        if request.sql() == "COMMIT TRANSACTION" {
            return Some((400, transaction_lost()));
        }
        if request.sql().starts_with("CALL") {
            released.recv().unwrap();
        }
        Some((200, complete()))
    });
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();

    let failure = connection.commit().unwrap_err();
    release.send(()).unwrap();
    assert_message(
        &failure,
        &[
            "could not confirm that the session ended",
            "keeps its locks",
        ],
    );
    assert_message_omits(
        &failure,
        &["session was ended", "could not end the session"],
    );
    assert_transaction_cleanup(requests, "COMMIT TRANSACTION");
}

/// BigQuery refusing to end the session proves it lives on, unlike a wait that ran out.
#[test]
fn a_refused_session_end_says_the_session_lives_on() {
    let (mut connection, requests) = connection_with_open_transaction_then(vec![
        (400, transaction_lost()),
        (403, error_body("accessDenied", "mock refusal")),
    ]);
    let failure = connection.commit().unwrap_err();
    assert_message(&failure, &["could not end the session", "keeps its locks"]);
    assert_transaction_cleanup(requests, "COMMIT TRANSACTION");
}

/// A session placed by its default dataset's location runs its jobs there, where `jobs.get`
/// without a location answers 404 (verified live in `asia-northeast1`). The same dataset says where
/// to look for a commit whose answers were all lost, without needing `bigquery.jobs.list`.
#[test]
fn a_lost_commit_is_found_in_the_default_datasets_location() {
    let (endpoint, requests) =
        bigquery_giving_no_verdict_on("COMMIT TRANSACTION", 0, 14, |request| {
            let path = request.path();
            Some(if !request.line.starts_with("GET") {
                (503, error_body("backendError", "Service unavailable"))
            } else if path.contains("/datasets/analytics") {
                ok(json!({"location": "asia-northeast1"}))
            } else if path.contains("location=asia-northeast1") {
                let job = path.rsplit('/').next().unwrap().split('?').next().unwrap();
                ok(json!({
                    "jobReference": {
                        "projectId": "fixture-project",
                        "jobId": job,
                        "location": "asia-northeast1"
                    },
                    "status": {"state": "DONE"}
                }))
            } else {
                (404, not_found_response())
            })
        });
    let mut connection = crate::test_support::driver::test_connection_with(
        &endpoint,
        [
            (BigQueryDatabaseOption::Location.into(), "".into()),
            (BigQueryDatabaseOption::DatasetId.into(), "analytics".into()),
        ],
    );
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    execute(&mut connection, "INSERT dataset.table VALUES (42)").unwrap();
    connection.commit().unwrap();
    drop(connection);

    let mut expected = PREAMBLE.to_vec();
    expected.extend(vec![
        "COMMIT TRANSACTION";
        crate::rest::retry::MAX_ATTEMPTS as usize
    ]);
    expected.push("CALL BQ.ABORT_SESSION('mock-session')");
    assert_sql(requests, &expected);
}

/// The location is a placement hint: a default dataset that cannot be read leaves the session
/// where BigQuery puts it, and the dataset's own failure to the queries that use it.
#[test]
fn a_default_dataset_whose_location_is_unknown_leaves_the_session_location_to_bigquery() {
    let requests = first_transaction_without_a_location((404, not_found_response()));
    assert!(requests[1].body.get("location").is_none());
}

/// A `COMMIT TRANSACTION` statement in the caller's SQL ends BigQuery's transaction, so the next
/// statement must begin a new one in the same session rather than autocommit there unseen.
#[test]
fn a_commit_statement_in_the_callers_sql_makes_the_next_statement_begin_again() {
    let (mut connection, requests) = connection_with_open_transaction_then(vec![
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"fixture-project","jobId":"commit-job"},
            "statementType": "COMMIT_TRANSACTION"
        })),
        (200, complete()),
        (200, complete()),
        (200, complete()),
        (200, complete()),
    ]);
    execute(&mut connection, "COMMIT TRANSACTION").unwrap();
    execute(&mut connection, "INSERT dataset.table VALUES (43)").unwrap();
    drop(connection);

    assert_sql_after_preamble(
        requests,
        &[
            "COMMIT TRANSACTION",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (43)",
            "ROLLBACK TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ],
    );
}
