//! Submission, polling, and result checks shared by CSV load and table copy jobs.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use serde::{Deserialize, Serialize};

use super::csv_load::LoadConfiguration;
use super::lro::PollBackoff;
use super::table_copy::CopyConfiguration;
use crate::BigQueryDatabaseOption;
use crate::cancellation::Operation;
use crate::error::{append_remedy, error, job_error, job_name, with_job_link};
use crate::query::{ExecutionContext, QualifiedTableName, StatementQueryOptions};
use crate::rest::jobs::{JobClient, JobReference, may_still_run, next_identity};
use crate::rest::json::decode_json;
use crate::rest::metadata::DatasetId;
use crate::rest::model::{JobOptions, JobStatus};

/// Submits a fresh job billed to the connection's billing project, or the destination's project.
///
/// Without `bigquery.location`, look up and cache the destination dataset's location: polling,
/// reconciliation and cancellation need it outside US/EU (G48). Any failure forgets the cached
/// location, in case the dataset was recreated elsewhere: the failed job is not resubmitted, but
/// the next one looks the location up again.
pub(super) fn in_job_location<T>(
    context: &ExecutionContext,
    operation: &Operation,
    destination: &QualifiedTableName,
    kind: &str,
    what: &str,
    submit: impl FnOnce(JobReference) -> Result<T>,
) -> Result<T> {
    let project_id = context
        .billing_project_id
        .as_ref()
        .unwrap_or_else(|| destination.project_id())
        .as_str();
    let job_id = next_identity(&format!("adbc_{kind}_"));
    if let Some(location) = context.location.as_deref() {
        return submit(JobReference::new(project_id, Some(location), job_id));
    }
    let dataset = destination.dataset_key();
    let location = dataset_location(context, operation, destination, what)?;
    let result = submit(JobReference::new(project_id, Some(&location), job_id));
    if result.is_err() {
        context.state.forget_dataset_location(&dataset);
    }
    result
}

fn dataset_location(
    context: &ExecutionContext,
    operation: &Operation,
    destination: &QualifiedTableName,
    what: &str,
) -> Result<Arc<str>> {
    let dataset = DatasetId {
        project_id: destination.project_id().as_str(),
        dataset_id: destination.dataset_id(),
    };
    let locations = context.state.dataset_locations(&context.runtime)?;
    context
        .runtime
        .block_on(locations.get(Some(operation), &destination.dataset_key()))
        .map_err(|failure| {
            let mut failure = failure.into_error(&format!(
                "failed to find the location of BigQuery dataset {dataset} for the {what} job"
            ));
            append_remedy(
                &mut failure,
                &format!(
                    "set BigQuery option {} to submit the job without this lookup",
                    BigQueryDatabaseOption::Location.as_str()
                ),
            );
            failure
        })
}

/// A successful load or copy job. Its ID becomes the result's `BIGQUERY:query_id`.
pub(crate) struct FinishedJob {
    pub(crate) rows: Option<i64>,
    pub(crate) job_id: String,
}

/// BigQuery accepts `reservation` for both operations: loads use it; copies only record it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobConfiguration<'a> {
    #[serde(flatten)]
    operation: JobOperation<'a>,
    #[serde(flatten)]
    options: JobOptions<'a>,
}

/// Exactly one operation block is required in a submitted job's configuration.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum JobOperation<'a> {
    Load(LoadConfiguration<'a>),
    Copy(CopyConfiguration<'a>),
}

impl<'a> JobConfiguration<'a> {
    pub(super) fn new(options: &'a StatementQueryOptions, operation: JobOperation<'a>) -> Self {
        Self {
            operation,
            options: JobOptions::new(
                options.job_timeout_ms,
                options.labels.as_deref(),
                options.reservation.as_deref(),
            ),
        }
    }
}

/// Fields read from load and copy job responses.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LoadOrCopyJob {
    job_reference: Option<JobReference>,
    status: Option<JobStatus>,
    statistics: Option<JobStatistics>,
}

#[derive(Debug, Deserialize)]
struct JobStatistics {
    load: Option<LoadStatistics>,
    copy: Option<CopyStatistics>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadStatistics {
    #[serde(default, deserialize_with = "crate::rest::json::optional_json_int")]
    output_rows: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyStatistics {
    #[serde(default, deserialize_with = "crate::rest::json::optional_json_int")]
    copied_rows: Option<i64>,
}

/// The row counts below become adbc.h's `rows_affected`, a count or -1 for unknown, so a negative
/// count from the wire is unknown.
impl LoadOrCopyJob {
    pub(super) fn output_rows(&self) -> Option<i64> {
        let rows = self.statistics.as_ref()?.load.as_ref()?.output_rows?;
        (rows >= 0).then_some(rows)
    }

    /// BigQuery omits the count for metadata-only copies.
    pub(super) fn copied_rows(&self) -> Option<i64> {
        let rows = self.statistics.as_ref()?.copy.as_ref()?.copied_rows?;
        (rows >= 0).then_some(rows)
    }
}

pub(super) fn decode_job(body: &[u8], what: &str) -> Result<LoadOrCopyJob> {
    decode_json(body, &format!("the BigQuery {what} job response"))
}

/// Waits for the job whose `jobs.insert` answered `body`, and checks that it succeeded.
///
/// A 2xx proves the job was accepted, but its body may be truncated or malformed. A response that
/// does not decode or names another job is reconciled by fetching the requested job ID, never by
/// resubmitting; if that fails too, the job may still run. A wait that fails links the job, as a
/// query's does.
pub(super) async fn wait_for_submitted(
    client: &JobClient,
    operation: &Operation,
    requested: &JobReference,
    body: &[u8],
    what: &str,
) -> Result<LoadOrCopyJob> {
    let (job, reference) = match decode_resolved_job(body, requested, what) {
        Ok(resolved) => resolved,
        Err(failure) => {
            let unrecovered = |failure| may_still_run(failure, requested, false);
            let recovered = client
                .reconcile_submission(Some(operation), requested, what, Some(failure))
                .await
                .map_err(unrecovered)?;
            decode_resolved_job(&recovered, requested, what).map_err(unrecovered)?
        }
    };
    // Retain the canonical identity before polling or a later cancellation addresses it.
    operation.register_job(&reference)?;
    let mut job = Box::pin(wait_for_job(client, operation, &reference, job, what))
        .await
        .map_err(|failure| with_job_link(failure, Some(&reference)))?;
    validate_completed(&mut job, what)?;
    Ok(job)
}

fn decode_resolved_job(
    body: &[u8],
    requested: &JobReference,
    what: &str,
) -> Result<(LoadOrCopyJob, JobReference)> {
    let job = decode_job(body, what)?;
    let reference = resolved_reference(&job, requested, what)?;
    Ok((job, reference))
}

/// The canonical identity of the accepted job `job` describes. See [`JobReference::confirm`].
fn resolved_reference(
    job: &LoadOrCopyJob,
    requested: &JobReference,
    what: &str,
) -> Result<JobReference> {
    requested.confirm(
        job.job_reference.as_ref(),
        &format!("the BigQuery {what} job response"),
    )
}

/// [`JobClient::poll`] handles transient failures, including a newly submitted job's 404, for one
/// poll. This loop sets the polling cadence and outlasts polls that run out of retries, since
/// ending the wait cancels the job.
async fn wait_for_job(
    client: &JobClient,
    operation: &Operation,
    reference: &JobReference,
    mut job: LoadOrCopyJob,
    what: &str,
) -> Result<LoadOrCopyJob> {
    let mut backoff = PollBackoff::new(Duration::from_millis(100), Duration::from_secs(1), None);
    let mut outage = client.wait_outage(Some(operation));
    loop {
        if job.status.as_ref().is_some_and(JobStatus::is_done) {
            operation.register_completed_job(reference)?;
            return Ok(job);
        }
        backoff.pause(operation, Duration::ZERO).await?;
        let started = tokio::time::Instant::now();
        match client.poll(Some(operation), reference, what).await {
            Ok(body) => {
                outage.end();
                job = decode_polled_job(&body, reference, what)?;
            }
            Err(failure) => outage.outlast(failure, reference, started)?,
        }
    }
}

fn decode_polled_job(body: &[u8], reference: &JobReference, what: &str) -> Result<LoadOrCopyJob> {
    let job = decode_job(body, what)?;
    resolved_reference(&job, reference, what)?;
    let problem = match job.status.as_ref().map(|status| status.state.as_str()) {
        Some("PENDING" | "RUNNING" | "DONE") => return Ok(job),
        Some(state) if !state.is_empty() => {
            format!("returned unknown state {state:?}; expected PENDING, RUNNING, or DONE")
        }
        _ => "contained no state".to_owned(),
    };
    Err(error(
        format!("BigQuery {what} job {} poll {problem}", job_name(reference)),
        Status::InvalidData,
    ))
}

/// `errorResult` takes precedence over the state: BigQuery can report it before `DONE`.
pub(super) fn validate_completed(job: &mut LoadOrCopyJob, what: &str) -> Result<()> {
    let status = job.status.as_mut().ok_or_else(|| {
        error(
            format!("completed BigQuery {what} job has no status"),
            Status::InvalidData,
        )
    })?;
    if let Some(errors) = status.take_terminal_errors() {
        return Err(job_error(
            &format!("BigQuery {what} job failed"),
            job.job_reference.as_ref(),
            &errors,
        ));
    }
    if !status.is_done() {
        return Err(error(
            format!("BigQuery {what} job response was not complete"),
            Status::InvalidData,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
