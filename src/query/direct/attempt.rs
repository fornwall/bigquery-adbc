//! One query attempt: submission through to a result page or a failure classified as reissuable
//! or not.
//!
//! The plan's identity selects `jobs.query` or `jobs.insert`. The reissue loop in [`super`]
//! decides whether to try again.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use bytes::Bytes;

use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{error, with_job_link};
use crate::identifiers::ProjectIdentifier;
use crate::query::ExecutionProgress;
use crate::rest::jobs::{JobClient, JobReference, JobRequest, QueryAnswer};
use crate::rest::request::RequestStart;
use crate::runtime::DetachedTask;

use crate::query::plan::{QueryIdentity, QueryJobCreation, QueryPlan};
use crate::query::request::JobsQueryRequest;
use crate::query::response::{JobResponse, QueryMetadata, QueryPage, QueryResponse};

use super::{
    AttemptFailure, CompletedQuery, QueryResponseDecoder, TerminalFailure,
    poll::{fetch_job, poll_until_complete},
};
use crate::query::direct::poll::confirmed_terminal_query_errors;

pub(super) async fn execute_once(
    client: &JobClient,
    decoder: &QueryResponseDecoder,
    plan: &QueryPlan,
    operation: Option<&Operation>,
    progress: Option<&ExecutionProgress>,
    context: &str,
) -> std::result::Result<CompletedQuery, AttemptFailure> {
    match plan.identity() {
        QueryIdentity::Request(request_id) => {
            let request = JobsQueryRequest::new(plan, request_id, decoder.result_mode);
            execute_jobs_query(
                client, decoder, plan, &request, operation, progress, context,
            )
            .await
        }
        QueryIdentity::Job(job_id) => {
            let request = plan.job_request(job_id)?;
            execute_jobs_insert(
                client, decoder, plan, &request, operation, progress, context,
            )
            .await
        }
    }
}

/// One `jobs.query` submission, whose response may be the whole result. Only a job-backed
/// response has an identity to register for cancellation or to verify a terminal failure against.
async fn execute_jobs_query(
    client: &JobClient,
    decoder: &QueryResponseDecoder,
    plan: &QueryPlan,
    request: &JobsQueryRequest<'_>,
    operation: Option<&Operation>,
    progress: Option<&ExecutionProgress>,
    context: &str,
) -> std::result::Result<CompletedQuery, AttemptFailure> {
    let body = request.encode()?;
    let mut response = submit_jobs_query(
        client.clone(),
        decoder,
        plan.project_id.clone(),
        plan.location.clone(),
        body.into(),
        operation,
    )
    .await?;
    let mut metadata = QueryMetadata::default();
    metadata.absorb_query(&mut response);
    // `submit_jobs_query` already confirmed this reference against the plan.
    if let Some(reference) = metadata.job_reference.as_ref() {
        if let Some(operation) = operation {
            register_query_job(operation, reference, response.job_complete)
                .map_err(|failure| with_job_link(failure, Some(reference)))?;
        }
    } else {
        // A job-backed response observes cancellation through registration above; a jobless
        // one checks it explicitly before accepting the result.
        if let Some(operation) = operation {
            operation.check()?;
        }
        if plan.job_creation == QueryJobCreation::Required {
            return Err(AttemptFailure::Request(error(
                "BigQuery query requiring a durable job returned without a job reference",
                Status::InvalidData,
            )));
        }
    }
    if let (Some(progress), Some(value)) = (progress, metadata.total_bytes_processed) {
        progress.publish_current(value);
    }
    if let Some(terminal) = confirmed_terminal_query_errors(
        client,
        &mut response,
        operation,
        metadata.job_reference.as_ref(),
        &metadata.statement_type,
        context,
    )
    .await
    .map_err(|failure| failure.with_job_link(metadata.job_reference.as_ref()))?
    {
        return Err(AttemptFailure::Terminal(terminal));
    }
    if response.job_complete {
        if let Some(progress) = progress {
            progress.complete(response.total_bytes_processed());
        }
        return Ok(completed(decoder, metadata, response.page(), None));
    }
    let reference = metadata.job_reference.clone().ok_or_else(|| {
        error(
            "an incomplete BigQuery jobs.query response has no job reference to poll",
            Status::InvalidData,
        )
    })?;
    let page = poll_until_complete(
        client,
        decoder,
        &reference,
        operation,
        progress,
        &mut metadata,
        context,
    )
    .await?;
    let job = describe_unnamed_statement(client, operation, &mut metadata).await?;
    Ok(completed(decoder, metadata, page, job))
}

/// Fetches the job of a completed query whose responses never named its statement type, which
/// happens when the initial `jobs.query` answer precedes execution, or a replay's 409 stood in
/// for its lost or refused answer: `jobs.getQueryResults` carries neither the statement type nor
/// the session, and the transaction and session bookkeeping need both. The job also serves the
/// result's metadata.
///
/// Best effort, since the query already ran: only cancellation propagates.
async fn describe_unnamed_statement(
    client: &JobClient,
    operation: Option<&Operation>,
    metadata: &mut QueryMetadata,
) -> std::result::Result<Option<JobResponse>, AttemptFailure> {
    let Some(reference) = metadata
        .job_reference
        .clone()
        .filter(|_| metadata.statement_type.is_empty())
    else {
        return Ok(None);
    };
    match fetch_job(client, operation, &reference, "query").await {
        Ok(mut job) => {
            metadata.absorb_job(&mut job);
            Ok(Some(job))
        }
        Err(source) if source.status == Status::Cancelled => Err(AttemptFailure::Request(
            with_job_link(source, Some(&reference)),
        )),
        Err(source) => {
            diag!(
                Warn,
                "the BigQuery query's statement type and session are unknown: failed to \
                 describe its job ({source})"
            );
            Ok(None)
        }
    }
}

/// One `jobs.insert` submission: the only route that can dry-run, and the only one whose
/// response is a job resource rather than a result.
async fn execute_jobs_insert(
    client: &JobClient,
    decoder: &QueryResponseDecoder,
    plan: &QueryPlan,
    request: &JobRequest,
    operation: Option<&Operation>,
    progress: Option<&ExecutionProgress>,
    context: &str,
) -> std::result::Result<CompletedQuery, AttemptFailure> {
    let body = client.insert(operation, request, "query").await?;
    let mut response = JobResponse::decode(&body, "the BigQuery jobs.insert response")?;
    // A dry run's synchronous response may carry only a project and location in `jobReference`,
    // with no job ID: it names no durable job, so it is neither confirmed nor published.
    let canonical = (!plan.dry_run)
        .then(|| {
            request.reference().confirm(
                response.job_reference.as_ref(),
                "the BigQuery jobs.insert response",
            )
        })
        .transpose()?;
    if response.is_done()
        && let (Some(operation), Some(reference)) = (operation, canonical.as_ref())
    {
        operation.register_completed_job(reference)?;
    }
    if let (Some(progress), Some(value)) = (progress, response.total_bytes_processed()) {
        progress.publish_current(value);
    }
    if let Some(errors) = response.take_terminal_errors() {
        return Err(AttemptFailure::Terminal(TerminalFailure::new(
            errors,
            canonical,
            response.statement_type().to_owned(),
            "",
        )));
    }
    if response.is_done()
        && let Some(progress) = progress
    {
        progress.complete(response.total_bytes_processed());
    }
    let mut metadata = QueryMetadata::default();
    // A RUNNING job's statistics are partial, so publish them only for a dry run or a finished
    // job; otherwise the poll and a later jobs.get fill the metadata. Keep the statement type and
    // session ID regardless: getQueryResults returns neither, and the transaction bookkeeping and
    // a session this insert created need both.
    if plan.dry_run || response.is_done() {
        metadata.absorb_job(&mut response);
    } else {
        metadata.absorb_running_job(&response);
    }
    metadata.job_reference.clone_from(&canonical);
    if plan.dry_run {
        if !response.is_done() {
            return Err(AttemptFailure::Request(error(
                "a BigQuery dry-run jobs.insert response is missing a completed status",
                Status::InvalidData,
            )));
        }
        return Ok(completed(
            decoder,
            metadata,
            QueryPage::default(),
            Some(response),
        ));
    }
    let canonical = canonical
        .expect("a successful non-dry jobs.insert response was required to carry a job reference");
    if let Some(operation) = operation {
        operation
            .register_job(&canonical)
            .map_err(|failure| with_job_link(failure, Some(&canonical)))?;
    }
    let page = poll_until_complete(
        client,
        decoder,
        &canonical,
        operation,
        progress,
        &mut metadata,
        context,
    )
    .await?;
    let completed_job = if response.is_done() {
        Some(response)
    } else {
        describe_unnamed_statement(client, operation, &mut metadata).await?
    };
    Ok(completed(decoder, metadata, page, completed_job))
}

/// A page is kept only where the caller asked for rows; `maxResults=0` returns none to keep.
fn completed(
    decoder: &QueryResponseDecoder,
    metadata: QueryMetadata,
    page: QueryPage,
    completed_job: Option<JobResponse>,
) -> CompletedQuery {
    let returns_rows = decoder.result_mode.returns_rows();
    CompletedQuery {
        metadata,
        page: if returns_rows {
            page
        } else {
            QueryPage::default()
        },
        completed_job,
        returns_rows,
    }
}

/// Registers the job a `jobs.query` response names for cancellation, as already finished when the
/// response completed it.
fn register_query_job(
    operation: &Operation,
    reference: &JobReference,
    complete: bool,
) -> Result<()> {
    if complete {
        operation.register_completed_job(reference)
    } else {
        operation.register_job(reference)
    }
}

/// Confirms the job a `jobs.query` response names, if any, against the requested project and
/// location, taking the location from the response or else the request. BigQuery generates the
/// job ID, so only the project and location are checked.
pub(super) fn canonicalize_query_response_reference(
    response: &mut QueryResponse,
    project_id: &str,
    location: Option<&str>,
) -> Result<()> {
    const RESPONSE: &str = "the BigQuery jobs.query response";
    let Some(returned) = response.job_reference.as_ref() else {
        return Ok(());
    };
    let requested = JobReference::new(project_id, location, returned.job_id.clone());
    let canonical = requested.confirm(Some(returned), RESPONSE)?.with_location(
        (!response.location.is_empty()).then_some(response.location.as_str()),
        RESPONSE,
    )?;
    response.job_reference = Some(canonical);
    Ok(())
}

/// Sends `jobs.query` outside the operation gate so cancellation cannot discard the only response
/// that names a server-generated job. If the caller cancels first, a detached continuation waits
/// for that same submission and registers any returned reference; registration sees
/// the already-cancelled operation and immediately issues the remote cancel.
///
/// The race is biased toward cancellation, so an operation cancelled before it never sends the
/// request.
pub(super) async fn submit_jobs_query(
    client: JobClient,
    decoder: &QueryResponseDecoder,
    project_id: ProjectIdentifier,
    location: Option<Arc<str>>,
    body: Bytes,
    operation: Option<&Operation>,
) -> Result<QueryResponse> {
    let decoder = decoder.clone();
    let request_start = RequestStart::default();
    let request_start_for_send = request_start.clone();
    let request_operation = operation.cloned();
    let mut request = Box::pin(async move {
        let mut response = match client
            .query(
                request_operation.as_ref(),
                project_id.as_str(),
                body,
                &request_start_for_send,
            )
            .await?
        {
            QueryAnswer::Response(body) => {
                decoder
                    .decode(body, "the BigQuery jobs.query response", None)
                    .await?
            }
            // The job an earlier attempt created, whose results the poll reads instead.
            QueryAnswer::Replayed(job) => QueryResponse::naming(job),
        };
        canonicalize_query_response_reference(
            &mut response,
            project_id.as_str(),
            location.as_deref(),
        )?;
        Ok(response)
    });
    let Some(operation) = operation else {
        return request.await;
    };
    tokio::select! {
        biased;
        () = operation.cancelled() => {
            let cancelled = operation.cancelled_error();
            if request_start.started() {
                let operation = operation.clone();
                let task: DetachedTask = Box::pin(async move {
                    // The request canonicalized the reference, location included.
                    if let Ok(response) = request.await
                        && let Some(reference) = &response.job_reference
                    {
                        let _ = register_query_job(&operation, reference, response.job_complete);
                    }
                });
                tokio::spawn(task);
            }
            Err(cancelled)
        }
        result = &mut request => result,
    }
}
