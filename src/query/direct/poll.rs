//! Poll submitted jobs and distinguish terminal failures from warnings.
//!
//! Neither HTTP errors nor a successful response's `errors` field prove that the job failed.
//! Confirm terminal errors with `jobs.get` before reissuing a query; retrying a running or
//! successful job could duplicate DML.

use std::time::{Duration, Instant};

use adbc_core::error::Status;

use crate::cancellation::{self, Operation};
use crate::error::{failure_reason, job_error};
use crate::query::ExecutionProgress;
use crate::rest::jobs::{JobClient, JobReference, QueryResultsRead};
use crate::rest::retry::retryable_failure;

use crate::query::response::{JobResponse, QueryMetadata, QueryPage, QueryResponse};

use super::{AttemptFailure, QueryResponseDecoder, TerminalFailure};

/// Minimum interval between completion polls.
///
/// BigQuery normally holds each request for
/// [`QUERY_COMPLETION_WAIT_MS`](crate::rest::jobs::QUERY_COMPLETION_WAIT_MS). Pace early
/// `jobComplete:false` responses to avoid exhausting permits and API quota. No failure occurred,
/// so this uses a fixed interval rather than retry backoff.
pub(super) const MINIMUM_POLL_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(20)
} else {
    Duration::from_secs(1)
};

pub(super) async fn poll_until_complete(
    client: &JobClient,
    decoder: &QueryResponseDecoder,
    reference: &JobReference,
    operation: Option<&Operation>,
    progress: Option<&ExecutionProgress>,
    metadata: &mut QueryMetadata,
    context: &str,
) -> Result<QueryPage, AttemptFailure> {
    let mut reference = reference.clone();
    let mut max_results = decoder.result_mode.max_results();
    async {
        loop {
            let started = Instant::now();
            let body = match client
                .get_query_results(
                    operation,
                    &reference,
                    None,
                    max_results,
                    QueryResultsRead::Completion,
                )
                .await
            {
                Ok(body) => body,
                // BigQuery refuses any page holding a row over 20 MiB, so wait for completion
                // without rows and leave them to Storage Read.
                Err(failure)
                    if max_results > 0 && failure_reason(&failure) == Some("responseTooLarge") =>
                {
                    max_results = 0;
                    metadata.rows_exceed_rest = true;
                    continue;
                }
                Err(failure) => {
                    return Err(rejected_poll_failure(
                        client,
                        operation,
                        &reference,
                        &metadata.statement_type,
                        failure,
                    )
                    .await);
                }
            };
            let mut response = decoder
                .decode(
                    body,
                    "the BigQuery jobs.getQueryResults response",
                    operation,
                )
                .await?;
            let canonical = metadata.absorb_polled_query(&mut response, &reference)?;
            if let Some(progress) = progress
                && let Some(value) = metadata.total_bytes_processed
            {
                progress.publish_current(value);
            }
            if let Some(operation) = operation {
                if response.job_complete {
                    operation.register_completed_job(&canonical)?;
                } else if canonical != reference {
                    operation.register_job(&canonical)?;
                }
            }
            reference = canonical;
            if let Some(terminal) = confirmed_terminal_query_errors(
                client,
                &mut response,
                operation,
                Some(&reference),
                &metadata.statement_type,
                context,
            )
            .await?
            {
                return Err(AttemptFailure::Terminal(terminal));
            }
            if response.job_complete {
                if let Some(progress) = progress {
                    progress.complete(response.total_bytes_processed());
                }
                metadata.completed_by_poll = true;
                return Ok(response.page());
            }
            // Pace only an answer that came back early.
            if let Some(remaining) = MINIMUM_POLL_INTERVAL.checked_sub(started.elapsed()) {
                cancellation::sleep(operation, remaining).await?;
            }
        }
    }
    .await
    .map_err(|failure: AttemptFailure| failure.with_job_link(Some(&reference)))
}

/// Classifies a rejected `jobs.getQueryResults` against the job's own status.
///
/// BigQuery maps a failed job's `status.errorResult.reason` onto the HTTP status of reads of its
/// results, so most terminal failures arrive here rather than as the HTTP-200 `errors` that
/// [`confirmed_terminal_query_errors`] handles (`stopped` is the only reason documented at 200).
///
/// The rejection's own reason is not trusted, since a `rateLimitExceeded` may concern the read
/// itself. Only [`verify_job`] decides, and anything short of a verified failure returns the
/// original rejection.
///
/// [`retryable_failure`] decides whether verification is worth a request: a fresh job cannot fix a
/// denial, a malformed request or a missing job. Every reason
/// `crate::query::response::retryable_job_errors` reruns a job for arrives as [`Status::IO`]: HTTP
/// 400 reasons after one read, 403 and 5xx reasons after the transport retries are spent.
async fn rejected_poll_failure(
    client: &JobClient,
    operation: Option<&Operation>,
    reference: &JobReference,
    statement_type: &str,
    failure: adbc_core::error::Error,
) -> AttemptFailure {
    // Cancellation is not retryable, so a cancelled statement skips verification too.
    if !retryable_failure(failure.status) {
        return AttemptFailure::Request(failure);
    }
    match verify_job(client, operation, reference, statement_type).await {
        JobVerdict::Failed(terminal) => AttemptFailure::Terminal(terminal),
        JobVerdict::Cancelled(cancelled) => AttemptFailure::Request(cancelled),
        // Nothing the job said establishes that it failed, so the caller keeps its rejection.
        JobVerdict::Succeeded | JobVerdict::Unverified => AttemptFailure::Request(failure),
    }
}

/// Distinguishes terminal job failures from the warnings carried in the same `errors` field.
///
/// An incomplete response carries only warnings. A complete job-backed response is verified
/// against `status.errorResult`; if verification is unavailable, the HTTP-200 error is reported
/// without reissuing the query.
pub(super) async fn confirmed_terminal_query_errors(
    client: &JobClient,
    response: &mut QueryResponse,
    operation: Option<&Operation>,
    reference: Option<&JobReference>,
    statement_type: &str,
    context: &str,
) -> Result<Option<TerminalFailure>, AttemptFailure> {
    if response.errors.is_empty() || !response.job_complete {
        return Ok(None);
    }
    let unverified = std::mem::take(&mut response.errors);
    let Some(reference) = reference else {
        // `jobs.query` reports validation failures as HTTP errors, so a complete jobless 200
        // response succeeded, even for DDL and scripts without a result schema; its `errors` are
        // warnings.
        return Ok(None);
    };
    match verify_job(client, operation, reference, statement_type).await {
        JobVerdict::Failed(terminal) => Ok(Some(terminal)),
        // A DONE job without `errorResult` succeeded; its `errors` are warnings.
        JobVerdict::Succeeded => Ok(None),
        JobVerdict::Cancelled(cancelled) => Err(AttemptFailure::Request(cancelled)),
        JobVerdict::Unverified => Err(AttemptFailure::Request(job_error(
            context,
            Some(reference),
            &unverified,
        ))),
    }
}

/// What one verification `jobs.get` established about a job whose results answered ambiguously.
enum JobVerdict {
    /// The fetched resource is the same job, `DONE`, with a `status.errorResult` (which leads
    /// `errors`).
    Failed(TerminalFailure),
    /// The same job, `DONE`, with no `errorResult`.
    Succeeded,
    /// The fetch failed or was undecodable, the resource named another job, or the job is not
    /// `DONE`; the caller reports what it already had.
    Unverified,
    /// The caller cancelled during verification; this replaces the original failure.
    Cancelled(adbc_core::error::Error),
}

/// Fetches the job `reference` names with a retried `jobs.get`, as `what` names it, confirming
/// the resource is that job before anything it says is used.
pub(in crate::query) async fn fetch_job(
    client: &JobClient,
    operation: Option<&Operation>,
    reference: &JobReference,
    what: &str,
) -> adbc_core::error::Result<JobResponse> {
    const RESPONSE: &str = "the BigQuery jobs.get response";
    let body = client.poll(operation, reference, what).await?;
    let mut job = JobResponse::decode(&body, RESPONSE)?;
    job.job_reference = Some(reference.confirm(job.job_reference.as_ref(), RESPONSE)?);
    Ok(job)
}

/// Asks `jobs.get` whether the job failed. A results read cannot tell (a rejection maps the job's
/// reason onto the read, and a 200 body's `errors` may be warnings), and rerunning DML on either
/// could duplicate writes. Uses a retried poll so one 503 cannot decide the verdict.
async fn verify_job(
    client: &JobClient,
    operation: Option<&Operation>,
    reference: &JobReference,
    statement_type: &str,
) -> JobVerdict {
    let mut job = match fetch_job(client, operation, reference, "failed query").await {
        Ok(job) if job.is_done() => job,
        Err(source) if source.status == Status::Cancelled => return JobVerdict::Cancelled(source),
        Ok(_) | Err(_) => return JobVerdict::Unverified,
    };
    match job.take_terminal_errors() {
        Some(errors) => JobVerdict::Failed(TerminalFailure::new(
            errors,
            Some(reference.clone()),
            job.statement_type(),
            statement_type,
        )),
        None => JobVerdict::Succeeded,
    }
}

#[cfg(test)]
mod tests;
