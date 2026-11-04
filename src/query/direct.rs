//! Direct REST submission and completion of one logical query.
//!
//! [`JobClient`] retries resend an identical request. This module is the only place that reissues
//! the query itself, always with a fresh request/job identity: for documented terminal BigQuery
//! reasons, only for a statement type a failed job cannot have partly committed, and once in
//! another session when BigQuery refused the query because its session had ended.
//!
//! * [`attempt`]: one submission, through to a result.
//! * [`poll`]: waiting for a submitted job, and telling a failure from a warning.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};

use crate::BigQueryStatementOption;
use crate::cancellation::{self, Operation, gate};
use crate::clients::ClientState;
use crate::error::{append_remedy, driver_defect, failure_reason, job_error, with_job_link};
use crate::identifiers::ProjectIdentifier;
use crate::query::ExecutionProgress;
use crate::rest::jobs::{JobClient, JobReference};
use crate::rest::model::ErrorProto;
use crate::runtime::SharedRuntime;
use crate::transaction::{TransactionContext, session_expired};
use tokio::sync::Semaphore;

mod attempt;
mod poll;

use attempt::execute_once;
pub(super) use poll::fetch_job;
use poll::poll_until_complete;

use super::plan::{QueryJobCreation, QueryPlan};
use super::response::{
    JobResponse, QueryMetadata, QueryPage, reissuable_statement, retryable_job_errors,
};
use super::transaction_end::ended_transaction;
use super::{QueryContext, QueryResultMode, StatementQueryContext, StatementQueryOptions};

/// The upstream default: three reissues after the initial execution.
const MAX_JOB_REISSUES: usize = 3;

#[derive(Debug)]
pub(super) struct CompletedQuery {
    pub(super) metadata: QueryMetadata,
    pub(super) page: QueryPage,
    /// A final `jobs.insert` response can avoid the metadata GET used by Storage Read.
    pub(super) completed_job: Option<JobResponse>,
    /// Whether the caller requested an ADBC result stream rather than completion metadata alone.
    pub(super) returns_rows: bool,
}

pub(super) fn complete_statement_query(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<crate::rest::model::QueryParameter>)>,
    job_creation: QueryJobCreation,
    result_mode: QueryResultMode,
    what: &str,
) -> Result<CompletedQuery> {
    let plan = QueryPlan::new(
        context.execution.billing_project_id.as_ref(),
        context.execution.statement_defaults(context.options),
        context.execution.location.as_deref(),
        context.session_id,
        sql,
        parameters,
        context.options,
        job_creation,
    )?;
    let Some(ended) = context.session_id else {
        return complete_in_session(context, plan, result_mode, what);
    };
    let failure = match complete_in_session(context, plan.clone(), result_mode, what) {
        Err(failure) if session_expired(&failure) => failure,
        outcome => return outcome,
    };
    let session_id = if context.options.dry_run {
        // A dry run in the idle session a committed transaction left fails once BigQuery has
        // ended that session, though it describes the statement as well outside it.
        if !context.transaction.forget_ended_session(ended) {
            return Err(failure);
        }
        None
    } else {
        // The statement that began the transaction can find its session ended right after the
        // `BEGIN` (see `TransactionContext::begin_again`). Only BigQuery's refusal
        // (`resourcesExceeded`) proves none of the statement ran: a script the session's end
        // stopped partway fails with the same message as `invalidQuery`, and may have committed.
        if failure_reason(&failure) != Some("resourcesExceeded") {
            return Err(failure);
        }
        let transaction = TransactionContext::new(
            context.execution,
            context.execution.statement_defaults(context.options),
        );
        let Some(session_id) =
            transaction.begin_again(context.transaction, context.operation, ended)?
        else {
            return Err(failure);
        };
        if let Some(progress) = context.progress {
            progress.restart_job();
        }
        Some(session_id)
    };
    let mut plan = plan.reissue();
    plan.session_id.clone_from(&session_id);
    let context = StatementQueryContext {
        session_id: session_id.as_deref(),
        ..context
    };
    complete_in_session(context, plan, result_mode, what)
}

fn complete_in_session(
    context: StatementQueryContext<'_>,
    plan: QueryPlan,
    result_mode: QueryResultMode,
    what: &str,
) -> Result<CompletedQuery> {
    let mut client = context
        .execution
        .state
        .rest_job_client(&context.execution.runtime)?;
    if plan.infers_job_location() {
        client = client.with_dataset_locations(
            context
                .execution
                .state
                .dataset_locations(&context.execution.runtime)?,
        );
    }
    let runtime = &context.execution.runtime;
    runtime.block_on(async {
        let complete = complete_query_with_progress(
            runtime,
            client.clone(),
            plan,
            result_mode,
            Some(context.operation),
            context.progress,
            what,
        )
        .await?;
        // A statement carries a session only inside the driver's transaction. When the caller's
        // SQL ends that transaction, the next statement must begin a new one.
        if let Some(session_id) = context.session_id
            && !context.options.dry_run
            && ended_transaction(&client, Some(context.operation), &complete.metadata).await?
        {
            context.transaction.ended_by_statement(session_id);
        }
        Ok(complete)
    })
}

/// Completes a query the driver issues itself (session control, ingest DDL) for its metadata
/// alone.
pub(super) fn complete_context_query(
    runtime: &SharedRuntime,
    state: &ClientState,
    billing_project_id: Option<&ProjectIdentifier>,
    context: QueryContext<'_>,
    sql: &str,
    options: &StatementQueryOptions,
    what: &str,
) -> Result<CompletedQuery> {
    let plan = QueryPlan::new(
        billing_project_id,
        context.defaults,
        context.location,
        context.session_id,
        sql,
        None,
        options,
        QueryJobCreation::Optional,
    )?;
    let client = state.rest_job_client(runtime)?;
    runtime.block_on(complete_query_with_progress(
        runtime,
        client,
        plan,
        QueryResultMode::MetadataOnly,
        context.operation,
        None,
        what,
    ))
}

/// Executes a fresh query, rerunning the SQL only for terminal transient failures, and publishes
/// each response's reported cost into `progress`.
pub(super) async fn complete_query_with_progress(
    runtime: &SharedRuntime,
    client: JobClient,
    mut plan: QueryPlan,
    result_mode: QueryResultMode,
    operation: Option<&Operation>,
    progress: Option<&ExecutionProgress>,
    context: &str,
) -> Result<CompletedQuery> {
    if let Some(operation) = operation {
        operation.check()?;
    }
    if !plan.dry_run
        && let Some(progress) = progress
    {
        progress.begin_job();
    }
    let decoder = QueryResponseDecoder::new(runtime, result_mode);
    let job_timeout_ms = plan.job_timeout_ms;
    for reissues in 0..=MAX_JOB_REISSUES {
        let failure = match Box::pin(execute_once(
            &client, &decoder, &plan, operation, progress, context,
        ))
        .await
        {
            Ok(completed) => return Ok(completed),
            Err(AttemptFailure::Request(failure)) => failure,
            Err(AttemptFailure::Terminal(terminal))
                if reissues < MAX_JOB_REISSUES && terminal.may_reissue() =>
            {
                if let Some(progress) = progress {
                    progress.restart_job();
                }
                pause_before_reissue(operation, reissues)
                    .await
                    .map_err(|failure| with_job_link(failure, terminal.job.as_ref()))?;
                plan = plan.reissue();
                continue;
            }
            Err(AttemptFailure::Terminal(terminal)) => {
                job_error(context, terminal.job.as_ref(), &terminal.errors)
            }
        };
        return Err(explain_job_timeout(failure, job_timeout_ms));
    }
    unreachable!("the bounded query reissue loop returns on its final attempt")
}

/// Adds the option name to BigQuery's "Job timed out" error for a job stopped at its
/// `jobTimeoutMs`.
fn explain_job_timeout(mut failure: Error, job_timeout_ms: i64) -> Error {
    if job_timeout_ms > 0
        && failure.status == Status::Timeout
        && failure_reason(&failure) == Some("stopped")
    {
        append_remedy(
            &mut failure,
            &format!(
                "BigQuery stops a job that runs longer than {} ({job_timeout_ms} ms)",
                BigQueryStatementOption::JobTimeout.as_str()
            ),
        );
    }
    failure
}

/// Completes a query job named by a partition descriptor without ever reissuing its SQL.
pub(super) async fn complete_attached_query(
    runtime: &SharedRuntime,
    client: JobClient,
    reference: JobReference,
    operation: Option<&Operation>,
    context: &str,
) -> Result<CompletedQuery> {
    if let Some(operation) = operation {
        operation
            .register_job(&reference)
            .map_err(|failure| with_job_link(failure, Some(&reference)))?;
    }
    let mut metadata = QueryMetadata {
        job_reference: Some(reference.clone()),
        ..QueryMetadata::default()
    };
    let decoder = QueryResponseDecoder::new(runtime, QueryResultMode::Rows);
    match poll_until_complete(
        &client,
        &decoder,
        &reference,
        operation,
        None,
        &mut metadata,
        context,
    )
    .await
    {
        Ok(page) => Ok(CompletedQuery {
            metadata,
            page,
            completed_job: None,
            returns_rows: true,
        }),
        Err(AttemptFailure::Request(failure)) => Err(failure),
        Err(AttemptFailure::Terminal(terminal)) => {
            Err(job_error(context, terminal.job.as_ref(), &terminal.errors))
        }
    }
}

/// Decodes a query response according to whether its request was allowed to return rows.
///
/// Responses that may carry rows retain the driver-wide conversion permit. Small envelopes decode
/// inline; larger pages use the blocking pool. A `maxResults=0` response is metadata alone and
/// needs no permit. Cancellation is checked around inline decoding and raced against the permit
/// and blocking work; a dropped join future leaves its permit held until decoding ends.
#[derive(Clone)]
pub(super) struct QueryResponseDecoder {
    permits: Arc<Semaphore>,
    result_mode: QueryResultMode,
}

impl QueryResponseDecoder {
    pub(super) fn new(runtime: &SharedRuntime, result_mode: QueryResultMode) -> Self {
        Self {
            permits: runtime.result_conversion_permits(),
            result_mode,
        }
    }

    pub(super) async fn decode(
        &self,
        body: bytes::Bytes,
        what: &'static str,
        operation: Option<&Operation>,
    ) -> Result<super::response::QueryResponse> {
        if !self.result_mode.returns_rows() {
            if let Some(operation) = operation {
                operation.check()?;
            }
            let response = super::response::QueryResponse::decode(&body, what)?;
            if let Some(operation) = operation {
                operation.check()?;
            }
            return Ok(response);
        }

        let permit = gate(operation, self.permits.clone().acquire_owned())
            .await?
            .map_err(|_| {
                driver_defect(format_args!(
                    "BigQuery result decoding stopped because the driver's result-work limit \
                     shut down"
                ))
            })?;
        let response = if body.len() <= super::response::INLINE_RESULT_BYTES {
            let _permit = permit;
            super::response::QueryResponse::decode(&body, what)?
        } else {
            let decode = super::response::QueryResponse::decode_bounded(body, what, permit);
            gate(operation, decode).await??
        };
        if let Some(operation) = operation {
            operation.check()?;
        }
        Ok(response)
    }
}

async fn pause_before_reissue(operation: Option<&Operation>, reissues: usize) -> Result<()> {
    let delay = if cfg!(test) {
        Duration::ZERO
    } else {
        Duration::from_secs(1_u64 << reissues.min(5))
    };
    cancellation::sleep(operation, delay).await
}

pub(super) enum AttemptFailure {
    Request(Error),
    Terminal(TerminalFailure),
}

impl AttemptFailure {
    fn with_job_link(self, reference: Option<&JobReference>) -> Self {
        match self {
            Self::Request(failure) => Self::Request(with_job_link(failure, reference)),
            // Terminal failures retain their reference until the reissue policy is resolved.
            terminal @ Self::Terminal(_) => terminal,
        }
    }
}

/// A job BigQuery itself reported as failed, and everything the reissue policy decides on.
pub(super) struct TerminalFailure {
    pub(super) errors: Vec<ErrorProto>,
    pub(super) job: Option<JobReference>,
    /// The failed job's `statementType`, or empty if no response named one.
    /// [`reissuable_statement`] uses it to keep a failed script from running again.
    statement_type: String,
}

impl TerminalFailure {
    /// `reported` is the verified job resource's statement type; `fallback`, accumulated from the
    /// query responses, applies when a sparse job resource names none.
    pub(super) fn new(
        errors: Vec<ErrorProto>,
        job: Option<JobReference>,
        reported: String,
        fallback: &str,
    ) -> Self {
        let statement_type = if reported.is_empty() {
            fallback.to_owned()
        } else {
            reported
        };
        Self {
            errors,
            job,
            statement_type,
        }
    }

    /// Whether the SQL may run again under a fresh job identity: BigQuery's reason must permit a
    /// fresh job, and the statement must be one a failed job cannot have partly committed.
    fn may_reissue(&self) -> bool {
        retryable_job_errors(&self.errors) && reissuable_statement(&self.statement_type)
    }
}

impl From<Error> for AttemptFailure {
    fn from(value: Error) -> Self {
        Self::Request(value)
    }
}

#[cfg(test)]
mod tests;
