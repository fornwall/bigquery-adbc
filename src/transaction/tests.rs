use std::sync::Arc;

use adbc_core::error::Status;

use super::{
    EnableAutocommit, FinishTransaction, QueryStart, QueryTransaction, State, TransactionState,
    googlesql_string_literal, unexpected_transition,
};
use crate::test_support::errors::{assert_message, assert_message_omits};

/// GoogleSQL escapes with backslashes: a doubled quote lexes as two adjacent literals, and an
/// unescaped trailing backslash would escape the closing quote.
#[test]
fn a_session_id_is_spelled_as_a_googlesql_literal() {
    for (value, literal) in [
        ("mock-session", "'mock-session'"),
        ("it's", r"'it\'s'"),
        (r"trailing\", r"'trailing\\'"),
        ("two\nlines", r"'two\nlines'"),
        ("carriage\r\nreturn", r"'carriage\r\nreturn'"),
        // A session ID that tries to close the literal and append a statement of its own stays
        // one literal: `CALL BQ.ABORT_SESSION` takes exactly one argument.
        (
            "s'); DROP ALL ROW ACCESS POLICIES ON t; --",
            r"'s\'); DROP ALL ROW ACCESS POLICIES ON t; --'",
        ),
    ] {
        assert_eq!(googlesql_string_literal(value), literal, "{value:?}");
    }
}

#[test]
fn manual_transactions_move_between_idle_and_active() {
    let state = TransactionState::default();
    assert!(state.is_autocommit().unwrap());

    state.disable_autocommit().unwrap();
    assert!(!state.is_autocommit().unwrap());
    let (previous, start) = start_query(&state);
    assert!(previous.is_none());

    start.started(Arc::from("shared-session")).unwrap();
    assert_eq!(
        state.session_id().unwrap().as_deref(),
        Some("shared-session")
    );

    let FinishTransaction::Active(session_id, finish) = state.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.succeeded(session_id).unwrap();
    assert!(!state.is_autocommit().unwrap());
    assert_eq!(
        state.session_id().unwrap().as_deref(),
        Some("shared-session")
    );
    assert!(matches!(
        state.begin_finish("roll back").unwrap(),
        FinishTransaction::Idle
    ));
    let (previous, _start) = start_query(&state);
    assert_eq!(previous.as_deref(), Some("shared-session"));
}

#[test]
fn failed_begin_remains_in_manual_mode_without_a_session() {
    let state = TransactionState::default();
    state.disable_autocommit().unwrap();
    let (previous, start) = start_query(&state);
    assert!(previous.is_none());
    start.failed(None).unwrap();

    assert!(!state.is_autocommit().unwrap());
    assert!(state.session_id().unwrap().is_none());
    let (previous, _start) = start_query(&state);
    assert!(previous.is_none());
}

#[test]
fn failed_finish_restores_the_active_transaction() {
    let state = active_state();
    let FinishTransaction::Active(session_id, finish) = state.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.failed(session_id).unwrap();

    assert!(!state.is_autocommit().unwrap());
    assert!(matches!(
        state.begin_query().unwrap(),
        QueryTransaction::Active(session_id) if session_id.as_ref() == "shared-session"
    ));
}

#[test]
fn an_abandoned_transaction_stays_manual_without_a_session() {
    let state = active_state();
    let FinishTransaction::Active(_, finish) = state.begin_finish("roll back").unwrap() else {
        panic!("active transaction was not returned")
    };
    finish.abandoned().unwrap();

    assert!(!state.is_autocommit().unwrap());
    assert!(state.session_id().unwrap().is_none());
    let (previous, _start) = start_query(&state);
    assert!(previous.is_none());
}

#[test]
fn enabling_autocommit_distinguishes_idle_and_active_transactions() {
    let idle = TransactionState::default();
    assert!(matches!(
        idle.begin_enable_autocommit().unwrap(),
        EnableAutocommit::AlreadyEnabled
    ));
    idle.disable_autocommit().unwrap();
    assert!(matches!(
        idle.begin_enable_autocommit().unwrap(),
        EnableAutocommit::Idle(None)
    ));
    assert!(idle.is_autocommit().unwrap());

    let retained = active_state();
    let FinishTransaction::Active(session_id, finish) = retained.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.succeeded(session_id).unwrap();
    assert!(matches!(
        retained.begin_enable_autocommit().unwrap(),
        EnableAutocommit::Idle(Some(session_id))
            if session_id.as_ref() == "shared-session"
    ));
    assert!(retained.is_autocommit().unwrap());

    let active = active_state();
    let EnableAutocommit::Active(_, finish) = active.begin_enable_autocommit().unwrap() else {
        panic!("active transaction was not returned")
    };
    finish.succeeded().unwrap();
    assert!(active.is_autocommit().unwrap());

    let active = active_state();
    let EnableAutocommit::Active(session_id, finish) = active.begin_enable_autocommit().unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.failed(session_id).unwrap();
    assert!(!active.is_autocommit().unwrap());
}

#[test]
fn transaction_transitions_reject_competing_operations() {
    let state = TransactionState::default();
    state.disable_autocommit().unwrap();
    let (_, start) = start_query(&state);

    for error in [
        state.begin_query().unwrap_err(),
        state.begin_finish("commit").unwrap_err(),
        state.begin_enable_autocommit().unwrap_err(),
        state.disable_autocommit().unwrap_err(),
    ] {
        assert_eq!(error.status, Status::InvalidState);
    }
    start.failed(None).unwrap();

    let state = active_state();
    let FinishTransaction::Active(session_id, finish) = state.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    for error in [
        state.begin_query().unwrap_err(),
        state.begin_finish("roll back").unwrap_err(),
        state.begin_enable_autocommit().unwrap_err(),
        state.disable_autocommit().unwrap_err(),
    ] {
        assert_eq!(error.status, Status::InvalidState);
    }
    finish.failed(session_id).unwrap();
}

#[test]
fn dropped_transitions_restore_a_settled_state() {
    // Dropping an uncompleted transition is what a contained panic unwinding through the
    // caller does; the state must settle instead of refusing every later transaction call.
    let state = TransactionState::default();
    state.disable_autocommit().unwrap();
    let (_, start) = start_query(&state);
    drop(start);
    assert!(state.session_id().unwrap().is_none());
    let (previous, start) = start_query(&state);
    assert!(previous.is_none());
    start.started(Arc::from("shared-session")).unwrap();

    let FinishTransaction::Active(_, finish) = state.begin_finish("commit").unwrap() else {
        panic!("active transaction was not returned")
    };
    drop(finish);
    assert!(matches!(
        state.begin_query().unwrap(),
        QueryTransaction::Active(session_id) if session_id.as_ref() == "shared-session"
    ));
}

/// The message names the state in words, never as `Debug` of the private enum, which would
/// expose the BigQuery session ID.
#[test]
fn transaction_state_errors_name_the_state_without_the_session_id() {
    let session_id = || Arc::from("secret-session-id");
    for (state, label) in [
        (State::Autocommit, "autocommit"),
        (
            State::ManualIdle {
                session_id: Some(session_id()),
            },
            "an idle transaction",
        ),
        (State::ManualStarting, "a transaction being started"),
        (
            State::ManualActive {
                session_id: session_id(),
                joined: false,
            },
            "an active transaction",
        ),
        (State::ManualFinishing, "a transaction being finished"),
    ] {
        let error = unexpected_transition("commit", &state);
        assert_eq!(
            error.message,
            format!(
                "cannot commit: the BigQuery connection is in {label}; this is a driver defect, \
                 report it at https://github.com/fornwall/bigquery-adbc/issues"
            )
        );
        assert_message_omits(&error, &["secret-session-id", "session_id"]);
    }
}

/// Releasing a connection hands back the session to end and leaves nothing behind that a
/// later call could reuse.
#[test]
fn releasing_reports_the_session_to_end_and_refuses_later_work() {
    let active = active_state();
    let release = active.release().expect("an active session to end");
    assert_eq!(release.session_id.as_ref(), "shared-session");
    assert!(release.open_transaction, "the transaction is still open");
    assert!(active.release().is_none(), "the session is handed out once");
    // A statement outliving its connection must not fall back to autocommit.
    for failure in [
        active.is_autocommit().unwrap_err(),
        active.session_id().unwrap_err(),
        active.begin_query().unwrap_err(),
    ] {
        assert_eq!(failure.status, Status::InvalidState);
        assert_message(&failure, &["was released"]);
    }

    let idle = active_state();
    let FinishTransaction::Active(session_id, finish) = idle.begin_finish("commit").unwrap() else {
        panic!("active transaction was not returned")
    };
    finish.succeeded(session_id).unwrap();
    let release = idle.release().expect("an idle session to end");
    assert_eq!(release.session_id.as_ref(), "shared-session");
    assert!(!release.open_transaction, "the transaction was committed");

    let autocommit = TransactionState::default();
    assert!(
        autocommit.release().is_none(),
        "autocommit opened no session"
    );
    // Autocommit work does not depend on the connection, so it outlives the release.
    assert!(autocommit.is_autocommit().unwrap());
    let manual = TransactionState::default();
    manual.disable_autocommit().unwrap();
    assert!(
        manual.release().is_none(),
        "manual mode alone opens no session"
    );
    assert!(manual.begin_query().is_err());
}

/// A commit in progress means another thread is inside a call on the connection being released;
/// BigQuery's session timeout is safer than tearing the session away.
#[test]
fn releasing_during_a_transition_leaves_the_session_alone() {
    let state = active_state();
    let FinishTransaction::Active(session_id, finish) = state.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    assert!(state.release().is_none());
    finish.failed(session_id).unwrap();
    assert!(state.release().is_some(), "the restored session still ends");
}

/// Only the idle session named is forgotten. An active transaction ended with its session and is
/// left for the next commit or rollback to report, and a session already replaced stays replaced.
#[test]
fn only_the_named_idle_session_is_forgotten() {
    let state = active_state();
    assert!(!state.forget_ended_session("shared-session"));
    assert_eq!(
        state.session_id().unwrap().as_deref(),
        Some("shared-session")
    );

    let FinishTransaction::Active(session_id, finish) = state.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.succeeded(session_id).unwrap();
    assert!(!state.forget_ended_session("another-session"));
    assert!(state.forget_ended_session("shared-session"));
    assert!(state.session_id().unwrap().is_none());
    assert!(!state.is_autocommit().unwrap());
    assert!(!state.forget_ended_session("shared-session"));
}

/// Only a transaction no statement but the one that began it was handed is begun again, and only
/// for the session it is active in. Restored transactions count as joined.
#[test]
fn only_a_transaction_no_other_statement_joined_is_begun_again() {
    let state = active_state();
    assert!(state.begin_replacing("another-session").is_none());
    let start = state.begin_replacing("shared-session").unwrap();
    assert_eq!(
        state.begin_query().unwrap_err().status,
        Status::InvalidState,
        "the replacement is in progress"
    );
    drop(start);
    assert!(state.session_id().unwrap().is_none(), "nothing reuses it");

    let joined = active_state();
    assert!(matches!(
        joined.begin_query().unwrap(),
        QueryTransaction::Active(_)
    ));
    assert!(joined.begin_replacing("shared-session").is_none());

    let restored = active_state();
    let FinishTransaction::Active(session_id, finish) = restored.begin_finish("commit").unwrap()
    else {
        panic!("active transaction was not returned")
    };
    finish.failed(session_id).unwrap();
    assert!(restored.begin_replacing("shared-session").is_none());
}

fn start_query(state: &TransactionState) -> (Option<Arc<str>>, QueryStart<'_>) {
    match state.begin_query().unwrap() {
        QueryTransaction::Start(previous, start) => (previous, start),
        other => panic!("expected a starting transaction, got {other:?}"),
    }
}

fn active_state() -> TransactionState {
    let state = TransactionState::default();
    state.disable_autocommit().unwrap();
    let (_, start) = start_query(&state);
    start.started(Arc::from("shared-session")).unwrap();
    state
}
