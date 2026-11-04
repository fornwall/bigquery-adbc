//! Shared ADBC transaction state used by a connection and its statements.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT;
use adbc_core::error::{Result, Status};
use tokio::sync::oneshot;

use crate::cancellation::Operation;
use crate::clients::DatasetKey;
use crate::diag::diag;
use crate::error::{driver_defect, error, invalid_state};
use crate::query::{ExecutionContext, QueryContext, QueryDefaults, create_session, execute_update};
use crate::rest::retry::retryable_failure;

#[derive(Debug, Default)]
pub(crate) struct TransactionState {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Autocommit,
    ManualIdle {
        session_id: Option<Arc<str>>,
    },
    ManualStarting,
    ManualActive {
        session_id: Arc<str>,
        /// Whether a statement besides the one that began the transaction has been handed it.
        /// Until then, that statement's first query is the only one that can run in the
        /// transaction: every other query that is not a dry run takes it from `begin_query`.
        joined: bool,
    },
    ManualFinishing,
    /// The connection was released in manual-commit mode, which ended its transaction; the
    /// statements that outlive it may not run anything.
    Released,
}

impl State {
    /// An active transaction other statements may have run in.
    fn joined(session_id: Arc<str>) -> Self {
        Self::ManualActive {
            session_id,
            joined: true,
        }
    }

    /// The user-facing name of this state.
    ///
    /// `Debug` must never reach a message: it would expose the private variant and the BigQuery
    /// session ID, which names a live server-side resource.
    fn label(&self) -> &'static str {
        match self {
            Self::Autocommit => "autocommit",
            Self::ManualIdle { .. } => "an idle transaction",
            Self::ManualStarting => "a transaction being started",
            Self::ManualActive { .. } => "an active transaction",
            Self::ManualFinishing => "a transaction being finished",
            Self::Released => "a released transaction",
        }
    }
}

#[derive(Debug)]
pub(crate) enum QueryTransaction<'a> {
    Autocommit,
    Active(Arc<str>),
    Start(Option<Arc<str>>, QueryStart<'a>),
}

#[derive(Debug)]
pub(crate) enum FinishTransaction<'a> {
    Idle,
    Active(Arc<str>, TransactionFinish<'a>),
}

#[derive(Debug)]
pub(crate) enum EnableAutocommit<'a> {
    AlreadyEnabled,
    Idle(Option<Arc<str>>),
    Active(Arc<str>, AutocommitFinish<'a>),
}

/// An in-progress two-phase transition.
///
/// The `begin_*` methods leave the state on an in-progress marker while the caller runs the
/// fallible remote work. Completing consumes the transition; dropping it uncompleted (only when a
/// contained panic unwinds through the caller) restores the settled state it carries, so the
/// state never stays stuck on a marker that every later transaction call would refuse.
#[derive(Debug)]
struct Transition<'a> {
    owner: &'a TransactionState,
    on_drop: Option<State>,
}

impl<'a> Transition<'a> {
    fn new(owner: &'a TransactionState, on_drop: State) -> Self {
        Self {
            owner,
            on_drop: Some(on_drop),
        }
    }

    /// Completes the transition: the state must still be an in-progress marker, which is
    /// replaced by `next`, unless the connection was released meanwhile.
    fn complete(mut self, operation: &str, next: State) -> Result<()> {
        self.on_drop = None;
        let mut state = self.owner.state();
        if matches!(*state, State::Released) {
            return Err(connection_released());
        }
        if !matches!(*state, State::ManualStarting | State::ManualFinishing) {
            return Err(unexpected_transition(operation, &state));
        }
        *state = next;
        Ok(())
    }
}

impl Drop for Transition<'_> {
    fn drop(&mut self) {
        let Some(restore) = self.on_drop.take() else {
            return;
        };
        let mut state = self.owner.state();
        if matches!(*state, State::ManualStarting | State::ManualFinishing) {
            *state = restore;
        }
    }
}

/// The starting-a-transaction half of [`TransactionState::begin_query`].
#[derive(Debug)]
pub(crate) struct QueryStart<'a>(Transition<'a>);

impl QueryStart<'_> {
    pub(crate) fn started(self, session_id: Arc<str>) -> Result<()> {
        self.0.complete(
            "finish starting a transaction",
            State::ManualActive {
                session_id,
                joined: false,
            },
        )
    }

    /// Stays in manual mode with no transaction, keeping `session_id` for the next statement.
    pub(crate) fn failed(self, session_id: Option<Arc<str>>) -> Result<()> {
        self.0.complete(
            "fail starting a transaction",
            State::ManualIdle { session_id },
        )
    }
}

/// The commit/rollback half of [`TransactionState::begin_finish`].
#[derive(Debug)]
pub(crate) struct TransactionFinish<'a>(Transition<'a>);

impl TransactionFinish<'_> {
    pub(crate) fn succeeded(self, session_id: Arc<str>) -> Result<()> {
        self.0.complete(
            "finish a transaction",
            State::ManualIdle {
                session_id: Some(session_id),
            },
        )
    }

    pub(crate) fn failed(self, session_id: Arc<str>) -> Result<()> {
        self.0
            .complete("restore a failed transaction", State::joined(session_id))
    }

    /// Stays in manual mode without a session, for a caller that ends the session itself.
    pub(crate) fn abandoned(self) -> Result<()> {
        self.0.complete(
            "abandon a transaction",
            State::ManualIdle { session_id: None },
        )
    }
}

/// The committing half of [`TransactionState::begin_enable_autocommit`].
#[derive(Debug)]
pub(crate) struct AutocommitFinish<'a>(Transition<'a>);

impl AutocommitFinish<'_> {
    pub(crate) fn succeeded(self) -> Result<()> {
        self.0.complete("enable autocommit", State::Autocommit)
    }

    pub(crate) fn failed(self, session_id: Arc<str>) -> Result<()> {
        self.0
            .complete("restore autocommit state", State::joined(session_id))
    }
}

impl TransactionState {
    fn state(&self) -> MutexGuard<'_, State> {
        // The lock guards one enum value written only in whole assignments, so a panicking holder
        // cannot leave a torn value and a poisoned guard is recovered rather than reported.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether autocommit is enabled; an error once the connection is released.
    pub(crate) fn is_autocommit(&self) -> Result<bool> {
        match *self.state() {
            State::Autocommit => Ok(true),
            State::Released => Err(connection_released()),
            _ => Ok(false),
        }
    }

    /// Rejects `subject` in manual-transaction mode: every caller runs outside the transaction's
    /// BigQuery session, so a later commit or rollback would appear to cover work that was
    /// already committed.
    #[inline(never)]
    pub(crate) fn require_autocommit(&self, subject: &str) -> Result<()> {
        if self.is_autocommit()? {
            return Ok(());
        }
        Err(invalid_state(format!(
            "the BigQuery {subject} cannot participate in manual transactions while {ADBC_CONNECTION_OPTION_AUTOCOMMIT} is disabled; re-enable autocommit or use a separate connection",
        )))
    }

    pub(crate) fn session_id(&self) -> Result<Option<Arc<str>>> {
        match &*self.state() {
            State::Autocommit | State::ManualIdle { session_id: None } => Ok(None),
            State::ManualIdle {
                session_id: Some(session_id),
            }
            | State::ManualActive { session_id, .. } => Ok(Some(session_id.clone())),
            State::ManualStarting | State::ManualFinishing => Err(transition_in_progress()),
            State::Released => Err(connection_released()),
        }
    }

    pub(crate) fn disable_autocommit(&self) -> Result<()> {
        let mut state = self.state();
        match &*state {
            State::Autocommit => {
                *state = State::ManualIdle { session_id: None };
                Ok(())
            }
            State::ManualIdle { .. } | State::ManualActive { .. } => Ok(()),
            State::ManualStarting | State::ManualFinishing => Err(transition_in_progress()),
            State::Released => Err(connection_released()),
        }
    }

    pub(crate) fn begin_query(&self) -> Result<QueryTransaction<'_>> {
        let mut state = self.state();
        match &mut *state {
            State::Autocommit => Ok(QueryTransaction::Autocommit),
            State::ManualActive { session_id, joined } => {
                *joined = true;
                Ok(QueryTransaction::Active(session_id.clone()))
            }
            State::ManualIdle { session_id } => {
                let session_id = session_id.clone();
                *state = State::ManualStarting;
                drop(state);
                // The drop restoration matches `QueryStart::failed`: whether the session survived
                // the interrupted start is unknown, so it is not reused.
                let transition = Transition::new(self, State::ManualIdle { session_id: None });
                Ok(QueryTransaction::Start(session_id, QueryStart(transition)))
            }
            State::ManualStarting | State::ManualFinishing => Err(transition_in_progress()),
            State::Released => Err(connection_released()),
        }
    }

    /// Settles the state after a `COMMIT` or `ROLLBACK` statement in the caller's SQL ended the
    /// transaction active in `session_id`: the session stays, and the next statement begins a new
    /// transaction in it.
    pub(crate) fn ended_by_statement(&self, session_id: &str) {
        let mut state = self.state();
        if let State::ManualActive {
            session_id: active, ..
        } = &*state
            && **active == *session_id
        {
            *state = State::ManualIdle {
                session_id: Some(active.clone()),
            };
        }
    }

    /// Forgets the idle `session_id` after BigQuery reported it ended, and says whether it did.
    ///
    /// Only an idle session is forgotten: it holds no transaction, so the next statement simply
    /// begins in a new session. A transaction active in it ended with it, and is left for the next
    /// commit or rollback to report. Once a statement has started replacing the session (see
    /// `TransactionContext::start`), the state no longer names it and nothing changes.
    pub(crate) fn forget_ended_session(&self, session_id: &str) -> bool {
        let mut state = self.state();
        let State::ManualIdle {
            session_id: Some(idle),
        } = &*state
        else {
            return false;
        };
        if **idle != *session_id {
            return false;
        }
        *state = State::ManualIdle { session_id: None };
        true
    }

    /// Starts beginning the transaction active in `ended` again, after BigQuery refused the first
    /// query of the statement that began it because the session had ended. `None` once another
    /// statement has been handed the transaction, which then ended with whatever it ran.
    fn begin_replacing(&self, ended: &str) -> Option<QueryStart<'_>> {
        let mut state = self.state();
        let State::ManualActive {
            session_id,
            joined: false,
        } = &*state
        else {
            return None;
        };
        if **session_id != *ended {
            return None;
        }
        *state = State::ManualStarting;
        drop(state);
        let transition = Transition::new(self, State::ManualIdle { session_id: None });
        Some(QueryStart(transition))
    }

    pub(crate) fn begin_finish(&self, operation: &str) -> Result<FinishTransaction<'_>> {
        let mut state = self.state();
        match &*state {
            State::Autocommit => Err(invalid_state(format!(
                "cannot {operation} while BigQuery autocommit is enabled; set \
                 {ADBC_CONNECTION_OPTION_AUTOCOMMIT} to false before the first statement of a \
                 transaction"
            ))),
            State::ManualIdle { .. } => Ok(FinishTransaction::Idle),
            State::ManualActive { session_id, .. } => {
                let session_id = session_id.clone();
                *state = State::ManualFinishing;
                drop(state);
                let transition = Transition::new(self, State::joined(session_id.clone()));
                Ok(FinishTransaction::Active(
                    session_id,
                    TransactionFinish(transition),
                ))
            }
            State::ManualStarting | State::ManualFinishing => Err(transition_in_progress()),
            State::Released => Err(connection_released()),
        }
    }

    /// Takes the server-side session a released connection owns.
    ///
    /// A connection released in manual-commit mode leaves its state [`State::Released`], so a
    /// statement that outlives it refuses to run rather than silently autocommitting work meant
    /// for a transaction. A statement outliving an autocommit connection keeps working.
    ///
    /// Returns `None` when there is nothing to clean up, or when a transition is in progress. A
    /// statement still starting a transaction owns the session it opens or reuses, so it finds
    /// the state released when it completes and ends that session itself. A commit, rollback or
    /// autocommit change in progress is a call on the connection being released, which ADBC
    /// forbids; BigQuery's session timeout is safer than tearing that session away.
    pub(crate) fn release(&self) -> Option<SessionRelease> {
        let mut state = self.state();
        let release = match &*state {
            State::Autocommit | State::ManualFinishing | State::Released => return None,
            State::ManualStarting => None,
            State::ManualIdle { session_id } => {
                session_id.clone().map(|session_id| SessionRelease {
                    session_id,
                    open_transaction: false,
                })
            }
            State::ManualActive { session_id, .. } => Some(SessionRelease {
                session_id: session_id.clone(),
                open_transaction: true,
            }),
        };
        *state = State::Released;
        release
    }

    pub(crate) fn begin_enable_autocommit(&self) -> Result<EnableAutocommit<'_>> {
        let mut state = self.state();
        match &*state {
            State::Autocommit => Ok(EnableAutocommit::AlreadyEnabled),
            State::ManualIdle { session_id } => {
                let session_id = session_id.clone();
                *state = State::Autocommit;
                Ok(EnableAutocommit::Idle(session_id))
            }
            State::ManualActive { session_id, .. } => {
                let session_id = session_id.clone();
                *state = State::ManualFinishing;
                drop(state);
                let transition = Transition::new(self, State::joined(session_id.clone()));
                Ok(EnableAutocommit::Active(
                    session_id,
                    AutocommitFinish(transition),
                ))
            }
            State::ManualStarting | State::ManualFinishing => Err(transition_in_progress()),
            State::Released => Err(connection_released()),
        }
    }
}

/// What releasing a connection has left running on the service.
#[derive(Debug)]
pub(crate) struct SessionRelease {
    pub(crate) session_id: Arc<str>,
    /// Whether a transaction is still open in that session and has to be rolled back before the
    /// session is aborted.
    pub(crate) open_transaction: bool,
}

/// `value` as a GoogleSQL single-quoted string literal, with backslash escapes.
///
/// Standard SQL's doubled quote would lex as two adjacent literals in GoogleSQL. The only caller
/// passes BigQuery session IDs, but the escaping does not rely on that.
fn googlesql_string_literal(value: &str) -> String {
    let mut literal = String::with_capacity(value.len() + 2);
    literal.push('\'');
    for character in value.chars() {
        match character {
            '\'' | '\\' => {
                literal.push('\\');
                literal.push(character);
            }
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            _ => literal.push(character),
        }
    }
    literal.push('\'');
    literal
}

/// Where to create a transaction session when no `bigquery.location` is configured: the default
/// dataset's location, best effort.
///
/// Every query in a session runs in its location, and BigQuery creates a session whose creating
/// query names no table in the US, where a query on data elsewhere fails with `Access Denied`.
/// Unqualified names resolve in the default dataset, so its location is the likeliest home of the
/// transaction's data. Without one, or when it cannot be read, BigQuery's default applies.
fn dataset_location(context: &ExecutionContext, dataset: &DatasetKey) -> Option<Arc<str>> {
    context
        .state
        .dataset_locations(&context.runtime)
        .and_then(|locations| {
            context
                .runtime
                .block_on(locations.get(None, dataset))
                .map_err(|failure| failure.into_error(""))
        })
        .inspect_err(|failure| {
            diag!(
                Debug,
                "creating the BigQuery transaction session where BigQuery chooses: the default \
                 dataset's location is unknown (status {:?})",
                failure.status
            );
        })
        .ok()
}

/// How long a `BEGIN`, `COMMIT` or `ROLLBACK` may take, retries included. It runs outside caller
/// cancellation, so this also bounds how long a cancel waits for it.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);

/// Whether a failed `BEGIN`, `COMMIT` or `ROLLBACK` may still have taken effect: BigQuery gave
/// no verdict on it, only a transport failure, a server error or a timeout
/// ([`retryable_failure`]).
pub(crate) fn outcome_unknown(failure: &adbc_core::error::Error) -> bool {
    retryable_failure(failure.status)
}

/// Whether a query failed because its session has ended. The message is the only signal: BigQuery
/// refuses a query in an ended session with the generic `resourcesExceeded`, and stops a script
/// running when its session ends with `invalidQuery`.
pub(crate) fn session_expired(failure: &adbc_core::error::Error) -> bool {
    failure
        .message
        .contains("has expired and is no longer available")
}

fn connection_released() -> adbc_core::error::Error {
    invalid_state(
        "the BigQuery connection this statement belongs to was released with autocommit \
         disabled, which ended its transaction; create the statement on an open connection",
    )
}

fn transition_in_progress() -> adbc_core::error::Error {
    invalid_state(
        "the BigQuery transaction state is being changed by another operation on this connection; \
         retry once it has finished",
    )
}

fn unexpected_transition(operation: &str, state: &State) -> adbc_core::error::Error {
    driver_defect(format_args!(
        "cannot {operation}: the BigQuery connection is in {}",
        state.label()
    ))
}

pub(crate) struct TransactionContext<'a> {
    context: &'a ExecutionContext,
    defaults: QueryDefaults<'a>,
}

impl<'a> TransactionContext<'a> {
    /// The transaction context an execution context describes, resolving names against
    /// `defaults` (the connection's, or the statement's shadowing copy).
    pub(crate) fn new(context: &'a ExecutionContext, defaults: QueryDefaults<'a>) -> Self {
        Self { context, defaults }
    }

    /// Resolves the session for each query, including later bound parameter rows.
    pub(crate) fn execution_session(
        &self,
        transaction: &TransactionState,
        operation: &Operation,
        dry_run: bool,
    ) -> Result<Option<Arc<str>>> {
        // Describing a query must not open a transaction.
        if dry_run {
            return transaction.session_id();
        }
        match transaction.begin_query()? {
            QueryTransaction::Autocommit => Ok(None),
            QueryTransaction::Active(session_id) => Ok(Some(session_id)),
            QueryTransaction::Start(previous_session_id, start) => {
                self.begin(start, operation, previous_session_id).map(Some)
            }
        }
    }

    /// Begins the transaction again in a new session after BigQuery refused the first query of
    /// the statement that began it, because its session `ended` in between (BigQuery ends a
    /// session asynchronously). The transaction then held only its `BEGIN`, which ended with the
    /// session, so the statement can run in the new one instead. `None` once another statement
    /// has been handed the transaction.
    pub(crate) fn begin_again(
        &self,
        transaction: &TransactionState,
        operation: &Operation,
        ended: &str,
    ) -> Result<Option<Arc<str>>> {
        let Some(start) = transaction.begin_replacing(ended) else {
            return Ok(None);
        };
        diag!(
            Warn,
            "the BigQuery session ended right after the transaction began in it; beginning the \
             transaction again in a new session"
        );
        self.begin(start, operation, None).map(Some)
    }

    /// Completes `start` with a transaction begun by [`Self::start`].
    fn begin(
        &self,
        start: QueryStart<'_>,
        operation: &Operation,
        previous: Option<Arc<str>>,
    ) -> Result<Arc<str>> {
        match self.start(operation, previous) {
            Ok(session_id) => {
                if let Err(source) = start.started(session_id.clone()) {
                    // The state did not take the session (the connection was released
                    // meanwhile), so nothing else ends it. Its transaction has run nothing, so
                    // there is nothing to roll back first.
                    self.abort(&session_id);
                    return Err(source);
                }
                Ok(session_id)
            }
            Err((source, kept)) => {
                if let Err(released) = start.failed(kept.clone()) {
                    if let Some(session_id) = kept {
                        self.abort(&session_id);
                    }
                    return Err(released);
                }
                Err(source)
            }
        }
    }

    /// Begins a transaction in the session the previous one ended in, or in a new session. A
    /// failure returns the session to keep for the next statement, if any.
    ///
    /// `BEGIN TRANSACTION` runs to a verdict like a commit (see [`Self::run_to_verdict`]): once
    /// sent it can land, and a session with a transaction the driver does not know about would
    /// fail the next `BEGIN`. A previous session BigQuery has since ended (after 24 idle hours,
    /// or through `BQ.ABORT_SESSION`) took its temporary tables and variables with it, so the
    /// transaction begins in a new session. One BigQuery refused to begin in keeps them; only
    /// when the outcome is unknown is it ended, and the error says so.
    fn start(
        &self,
        operation: &Operation,
        previous: Option<Arc<str>>,
    ) -> std::result::Result<Arc<str>, (adbc_core::error::Error, Option<Arc<str>>)> {
        if let Some(session_id) = previous {
            match self.run_to_verdict(&session_id, "BEGIN TRANSACTION") {
                Ok(()) => return Ok(session_id),
                Err(source) if session_expired(&source) => diag!(
                    Warn,
                    "the BigQuery session ended with its temporary tables; beginning the \
                     transaction in a new session"
                ),
                Err(source) if !outcome_unknown(&source) => return Err((source, Some(session_id))),
                Err(mut source) => {
                    self.abort(&session_id);
                    source.message.push_str(
                        "; whether the transaction began is unknown, so the BigQuery session was \
                         ended, with its temporary tables and variables",
                    );
                    return Err((source, None));
                }
            }
        }
        let session_id = self
            .create_session(operation)
            .map_err(|source| (source, None))?;
        if let Err(source) = self.run_to_verdict(&session_id, "BEGIN TRANSACTION") {
            self.abort(&session_id);
            return Err((source, None));
        }
        Ok(session_id)
    }

    /// Creates a session for the caller, or ends it if the caller is cancelled first.
    ///
    /// Cancelling the request could drop the only response that names the session, so creation
    /// runs to completion on the blocking pool and cancellation ends only the caller's wait.
    /// Whichever side holds the session ID once the caller stops waiting ends the session.
    fn create_session(&self, operation: &Operation) -> Result<Arc<str>> {
        operation.check()?;
        let context = self.context.clone();
        let default_dataset = self
            .defaults
            .catalog
            .zip(self.defaults.db_schema)
            .map(|(project_id, dataset_id)| (project_id.clone(), Arc::from(dataset_id)));
        let (created, mut receiver) = oneshot::channel();
        self.context.runtime.spawn_blocking(move || {
            let location = context
                .location
                .clone()
                .or_else(|| dataset_location(&context, &default_dataset?));
            let session = create_session(
                &context.runtime,
                &context.state,
                context.billing_project_id.as_ref(),
                location.as_deref(),
            );
            if let Err(Ok(session_id)) = created.send(session) {
                TransactionContext::new(&context, context.defaults()).abort(&session_id);
            }
        });
        match self.context.runtime.block_on(operation.wait(&mut receiver)) {
            Ok(Ok(session)) => session,
            Ok(Err(_)) => Err(driver_defect(format_args!(
                "creating a BigQuery transaction session ended without a result"
            ))),
            Err(cancelled) => {
                // A session sent before `close` is the caller's to end; a later one fails to
                // send and the creating task ends it.
                receiver.close();
                if let Ok(Ok(session_id)) = receiver.try_recv() {
                    self.abort(&session_id);
                }
                Err(cancelled)
            }
        }
    }

    /// Runs `BEGIN`, `COMMIT` or `ROLLBACK TRANSACTION` in `session_id` until BigQuery answers,
    /// for at most [`CONTROL_TIMEOUT`].
    ///
    /// Not cancellable: a request already sent can still land, and a commit reported as cancelled
    /// after it landed would leave the transaction active in the driver while BigQuery autocommits
    /// every later statement in the session. [`outcome_unknown`] says when a failure leaves the
    /// outcome open.
    pub(crate) fn run_to_verdict(&self, session_id: &Arc<str>, sql: &'static str) -> Result<()> {
        let context = self.context.clone();
        let session_id = session_id.clone();
        let (finished, receiver) = oneshot::channel();
        self.context.runtime.spawn_blocking(move || {
            let outcome = TransactionContext::new(&context, context.defaults()).execute(
                None,
                &session_id,
                sql,
            );
            // The caller stopped waiting at the deadline and reports the outcome as unknown.
            let _ = finished.send(outcome);
        });
        // The timer is created inside the runtime, which it needs.
        match self
            .context
            .runtime
            .block_on(async { tokio::time::timeout(CONTROL_TIMEOUT, receiver).await })
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(driver_defect(format_args!(
                "BigQuery {sql} ended without a result"
            ))),
            Err(_) => Err(error(
                format!(
                    "BigQuery did not finish {sql} within {} s",
                    CONTROL_TIMEOUT.as_secs()
                ),
                Status::Timeout,
            )),
        }
    }

    /// Ends the session, best-effort: every caller is already reporting something else, or no
    /// one, so a failure is only written as a diagnostic.
    pub(crate) fn abort(&self, session_id: &str) {
        let sql = format!(
            "CALL BQ.ABORT_SESSION({})",
            googlesql_string_literal(session_id)
        );
        if let Err(failure) = self.execute(None, session_id, &sql) {
            // Service error messages may contain SQL or other user data, so only the status.
            diag!(
                Warn,
                "failed to end a BigQuery session (status {:?}); BigQuery ends it when it expires",
                failure.status
            );
        }
    }

    fn execute(&self, operation: Option<&Operation>, session_id: &str, sql: &str) -> Result<()> {
        execute_update(
            &self.context.runtime,
            &self.context.state,
            self.context.billing_project_id.as_ref(),
            QueryContext {
                defaults: self.defaults,
                location: self.context.location.as_deref(),
                session_id: Some(session_id),
                operation,
            },
            sql,
        )
    }
}

#[cfg(test)]
mod tests;
