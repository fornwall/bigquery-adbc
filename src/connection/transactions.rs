//! The connection's transaction calls: `adbc.connection.autocommit`, commit, rollback, and the
//! session a released connection still owns. [`crate::transaction`] holds the state they move.

use adbc_core::error::{Result, Status};

use super::BigQueryConnection;
use crate::query::ExecutionContext;
use crate::transaction::{
    EnableAutocommit, FinishTransaction, SessionEnd, SessionRelease, TransactionContext,
    no_active_transaction, outcome_unknown, session_expired,
};

impl BigQueryConnection {
    pub(super) fn set_autocommit(&self, enabled: bool) -> Result<()> {
        if !enabled {
            return self.transaction.disable_autocommit();
        }
        match self.transaction.begin_enable_autocommit()? {
            EnableAutocommit::Idle(session_id) => {
                if let Some(session_id) = session_id {
                    self.transaction_context().abort(&session_id);
                }
                Ok(())
            }
            EnableAutocommit::Active(session_id, finish) => {
                let context = self.transaction_context();
                match context.run_to_verdict(&session_id, "COMMIT TRANSACTION") {
                    Err(source) if commit_refused(&source) => {
                        finish.failed(session_id)?;
                        Err(source)
                    }
                    outcome => {
                        finish.autocommit()?;
                        let ended = context.abort(&session_id);
                        outcome.map_err(|source| {
                            ended_session_error(source, true, ended, "autocommit is now enabled")
                        })
                    }
                }
            }
        }
    }

    /// Ends the active transaction with `COMMIT TRANSACTION` or `ROLLBACK TRANSACTION`.
    ///
    /// A commit BigQuery refused keeps the transaction active so the caller can retry or roll
    /// back. Any other failure ends the session, which discards whatever was not committed; the
    /// next statement opens a fresh session.
    pub(super) fn finish_transaction(&self, rollback: bool) -> Result<()> {
        let (sql, operation_name) = if rollback {
            ("ROLLBACK TRANSACTION", "roll back")
        } else {
            ("COMMIT TRANSACTION", "commit")
        };
        let FinishTransaction::Active(session_id, finish) =
            self.transaction.begin_finish(operation_name)?
        else {
            return Ok(());
        };
        let context = self.transaction_context();
        match context.run_to_verdict(&session_id, sql) {
            Ok(()) => finish.succeeded(session_id),
            Err(source) if !rollback && commit_refused(&source) => {
                finish.failed(session_id)?;
                Err(source)
            }
            Err(source) => {
                finish.abandoned()?;
                let ended = context.abort(&session_id);
                Err(ended_session_error(
                    source,
                    !rollback,
                    ended,
                    "the next statement starts a new transaction",
                ))
            }
        }
    }

    fn transaction_context(&self) -> TransactionContext<'_> {
        TransactionContext::new(&self.context, self.context.defaults())
    }

    /// Ends the BigQuery session a released connection still owns, best effort.
    ///
    /// An abandoned session keeps its open transaction's locks until BigQuery times it out,
    /// which can block DML on the tables it touched.
    fn release_session(context: &ExecutionContext, release: &SessionRelease) {
        let transaction = TransactionContext::new(context, context.defaults());
        if release.open_transaction {
            // Aborting the session leaves its transaction for the service to roll back on its
            // own schedule.
            let _ = transaction.run_to_verdict(&release.session_id, "ROLLBACK TRANSACTION");
        }
        transaction.abort(&release.session_id);
    }
}

impl Drop for BigQueryConnection {
    /// Releasing a connection ends the BigQuery session it opened.
    ///
    /// The cleanup runs queries, and `block_on` panics on a Tokio worker thread (Tokio hosts link
    /// the driver as an rlib). As in `Cancellation::try_cancel`, it runs inline on a host thread
    /// and on the driver's blocking pool otherwise, so drop never panics or blocks a worker.
    fn drop(&mut self) {
        // A `fork` child inherits the session but does not own it: the parent may still use it.
        if self.context.runtime.orphaned() {
            return;
        }
        let Some(release) = self.transaction.release() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let context = self.context.clone();
            self.context
                .runtime
                .spawn_detached_blocking(Box::new(move || {
                    Self::release_session(&context, &release);
                }));
        } else {
            Self::release_session(&self.context, &release);
        }
    }
}

/// Whether BigQuery answered a `COMMIT` or `ROLLBACK` with the transaction already gone, or with
/// its session ended. Neither answer has a reason of its own, so the message is the only signal.
fn transaction_lost(failure: &adbc_core::error::Error) -> bool {
    no_active_transaction(failure) || session_expired(failure)
}

/// Whether BigQuery refused a `COMMIT`, which leaves its transaction open.
fn commit_refused(failure: &adbc_core::error::Error) -> bool {
    !transaction_lost(failure) && !outcome_unknown(failure)
}

/// Explains `failure` of a `COMMIT` (`committing`) or `ROLLBACK` after which the driver tried to
/// end the session, says what became of it (`ended`), and what the connection does `next`.
///
/// A lost transaction ended outside the driver's view: its session expired, which rolled it
/// back, or a script ended it and then failed or could not be inspected (see
/// `crate::query::transaction_end`), after which BigQuery autocommits every later statement in
/// the session.
fn ended_session_error(
    mut failure: adbc_core::error::Error,
    committing: bool,
    ended: SessionEnd,
    next: &str,
) -> adbc_core::error::Error {
    let what = if transaction_lost(&failure) {
        failure.status = Status::InvalidState;
        "the BigQuery transaction had already ended: its session expired, which rolled it back, \
         or a script ran COMMIT or ROLLBACK, after which later statements committed individually \
         (use the ADBC commit and rollback calls instead)"
    } else if committing {
        "BigQuery did not say whether the transaction was committed, so check the data before \
         retrying; ending the session discards its changes unless the commit took effect"
    } else {
        "ending the session discards the transaction's changes"
    };
    failure.message = format!(
        "{}; {what}. {} and {next}",
        failure.message,
        ended.describe()
    );
    failure
}
