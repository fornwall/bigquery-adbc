//! dbt 2.0's option-only Dataproc python-model submissions.
//!
//! The Spark submission methods share region, project and timeout options:
//!
//! * `serverless` creates a Dataproc batch, selected by `adbc.bigquery.create_batch.parent`
//!   or `.batch_yml`.
//! * `cluster` submits a job to an existing cluster, selected by
//!   `adbc.bigquery.dataproc.submit_job.cluster_name` or `.gcs_path`.
//!
//! `adbc.bigquery.create_batch.batch_id` and `adbc.bigquery.dataproc.pooling_timeout` are also
//! read by the Vertex AI notebook job in `src/dbt/vertex.rs`, so they select nothing on their own.
//!
//! Both are long-running operations polled until `done`, which only means Dataproc stopped
//! working; the batch's or job's own terminal state decides success. See `DBT.md`.

use std::time::Duration;

use adbc_core::error::{Result, Status};
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use super::lro::{Delivery, LroPoller, PollBudget, operation_failure, resource_name, resource_url};
use super::{region_segment, resource_path, resource_segment, text_slots};
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::error::{error, invalid_argument};
use crate::query::ExecutionContext;
use crate::rest::jobs::next_identity;
use crate::rest::json::encode_json;
use crate::rest::request::Request;
use crate::rest::url::{escape_resource_path, escape_url_component};

/// A `requestId` allows letters, digits, `_` and `-`, at most 40 characters, which this prefix and
/// [`next_identity`]'s 32 hex digits keep to.
const REQUEST_ID_PREFIX: &str = "adbc_";

/// Poll from 1 s to 10 s; Spark jobs often run for minutes or hours.
const INITIAL_POLL_DELAY: Duration = Duration::from_secs(1);
const MAX_POLL_DELAY: Duration = Duration::from_secs(10);

/// Shared options for dbt's Dataproc submissions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DataprocOptions {
    compute_region: Option<String>,
    project: Option<String>,
    /// Client-side deadline in seconds, positive when present. Zero, empty, and unset all mean no
    /// deadline; a negative value is rejected when set.
    pooling_timeout_seconds: Option<i64>,
    submit_job_cluster_name: Option<String>,
    submit_job_gcs_path: Option<String>,
    create_batch_parent: Option<String>,
    create_batch_batch_yml: Option<String>,
    create_batch_batch_id: Option<String>,
}

// `pooling_timeout`, the one integer option, has its own accessors. Mode switching resets all
// three python-model option structures together, including this timeout and the shared batch ID.
text_slots!(DataprocOptions {
    DataprocComputeRegion => compute_region,
    DataprocProject => project,
    DataprocSubmitJobClusterName => submit_job_cluster_name,
    DataprocSubmitJobGcsPath => submit_job_gcs_path,
    CreateBatchParent => create_batch_parent,
    CreateBatchBatchYml => create_batch_batch_yml,
    CreateBatchBatchId => create_batch_batch_id,
});

impl DataprocOptions {
    /// `batch_id` alone does not select this operation because notebook jobs also use it.
    pub(crate) fn batch_is_active(&self) -> bool {
        self.create_batch_parent.is_some() || self.create_batch_batch_yml.is_some()
    }

    pub(crate) fn cluster_job_is_active(&self) -> bool {
        self.submit_job_cluster_name.is_some() || self.submit_job_gcs_path.is_some()
    }

    /// Applies a positive `pooling_timeout`, in seconds. `None` is no deadline at all.
    pub(crate) fn set_pooling_timeout_seconds(&mut self, seconds: Option<i64>) {
        self.pooling_timeout_seconds = seconds;
    }

    pub(crate) fn pooling_timeout_option_value(&self) -> i64 {
        self.pooling_timeout_seconds.unwrap_or_default()
    }

    /// The timeout starts here and covers submission and polling, including for Vertex AI.
    pub(super) fn poll_budget<'a>(
        &self,
        what: &'a str,
        initial: Duration,
        maximum: Duration,
    ) -> PollBudget<'a> {
        // An unrepresentable deadline would never be reached, so it becomes no deadline rather
        // than an overflow panic.
        let deadline = self.pooling_timeout_seconds.and_then(|seconds| {
            Instant::now().checked_add(Duration::from_secs(seconds.unsigned_abs()))
        });
        PollBudget {
            initial,
            maximum,
            deadline,
            seconds: self.pooling_timeout_option_value(),
            what,
        }
    }

    pub(crate) fn create_batch_batch_id(&self) -> Option<&str> {
        self.create_batch_batch_id.as_deref()
    }

    /// Region is part of the host: reject values that could redirect credentials.
    fn region(&self, what: &str) -> Result<&str> {
        let region = self.required(what, BigQueryStatementOption::DataprocComputeRegion)?;
        region_segment(region, BigQueryStatementOption::DataprocComputeRegion)
    }

    /// Creates a Dataproc batch from the configured `batch_yml` and waits for it, answering the
    /// batch's resource name. `batch_id` is optional: Dataproc names the batch when it is unset.
    pub(crate) fn execute_batch(
        &self,
        context: &ExecutionContext,
        operation: &Operation,
    ) -> Result<String> {
        let what = Submission::Batch.what();
        let parent = self.required(what, BigQueryStatementOption::CreateBatchParent)?;
        let batch_yml = self.required(what, BigQueryStatementOption::CreateBatchBatchYml)?;
        let parent = resource_path(parent, BigQueryStatementOption::CreateBatchParent)?;
        let batch = batch_resource(batch_yml)?;
        let (target, batch_id) = match self.create_batch_batch_id.as_deref() {
            Some(batch_id) => (
                format!("{batch_id} in {parent}"),
                format!("batchId={}&", escape_url_component(batch_id)),
            ),
            None => (format!("in {parent}"), String::new()),
        };
        let path = format!(
            "{}/batches?{batch_id}requestId={}",
            escape_resource_path(parent),
            next_identity(REQUEST_ID_PREFIX)
        );
        self.submit(
            context,
            operation,
            Submission::Batch,
            &target,
            &path,
            encode(&batch)?,
        )
    }

    /// Submits a PySpark job to an existing Dataproc cluster and waits for it, answering the job
    /// ID. Only the cluster and the main python file are configured, as in the fork: dbt 2.0
    /// sends no jars, arguments, properties or labels.
    pub(crate) fn execute_cluster_job(
        &self,
        context: &ExecutionContext,
        operation: &Operation,
    ) -> Result<String> {
        let what = Submission::ClusterJob.what();
        let cluster_name =
            self.required(what, BigQueryStatementOption::DataprocSubmitJobClusterName)?;
        let gcs_path = self.required(what, BigQueryStatementOption::DataprocSubmitJobGcsPath)?;
        let region = self.region(what)?;
        let project = self.required(what, BigQueryStatementOption::DataprocProject)?;
        let project = resource_segment(project, BigQueryStatementOption::DataprocProject)?;
        let path = format!(
            "projects/{}/regions/{}/jobs:submitAsOperation",
            escape_url_component(project),
            escape_url_component(region)
        );
        // `SubmitJobRequest` binds `projectId` and `region` to the path with `body: "*"`, so the
        // body is the request minus those fields: the `Job` under a `job` key, unlike
        // `batches.create` (`body: "batch"`). Repeating the bound fields in the body may be
        // refused as a duplicate binding.
        let body = encode(&serde_json::json!({
            "job": {
                "placement": {"clusterName": cluster_name},
                "pysparkJob": {"mainPythonFileUri": gcs_path},
            },
            "requestId": next_identity(REQUEST_ID_PREFIX),
        }))?;
        self.submit(
            context,
            operation,
            Submission::ClusterJob,
            &format!("to cluster {cluster_name} in projects/{project}/regions/{region}"),
            &path,
            body,
        )
    }

    /// Both submission methods deduplicate `requestId`, so retries after transient or lost
    /// responses recover the original operation without starting another job. `path` follows
    /// `/v1/`, escaped.
    fn submit(
        &self,
        context: &ExecutionContext,
        operation: &Operation,
        submission: Submission,
        target: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<String> {
        let what = submission.what();
        let region = self.region(what)?;
        let configuration = context.state.configuration(&context.runtime)?;
        let endpoint = configuration
            .endpoints
            .dataproc(region, BigQueryStatementOption::DataprocComputeRegion)?;
        let url = format!("{endpoint}/v1/{path}");
        let body = bytes::Bytes::from(body);
        let mut poller = LroPoller::new(
            operation,
            &configuration.credentials,
            context.state.rest_client()?,
            endpoint,
            self.poll_budget(what, INITIAL_POLL_DELAY, MAX_POLL_DELAY),
        );

        context.runtime.block_on(Box::pin(async move {
            let context = format!("submit the BigQuery {what} {target}");
            let submitted = Box::pin(poller.request(Delivery::Deduplicated, &|| {
                Request::post_json(&url, body.clone(), &context)
            }))
            .await?;

            let running = RunningOperation::new(&submitted, what)?;
            poller.set_running(running.name.clone());
            let poll_url = poller.resource_url(&running.name, "");
            let poll_context = format!("poll the BigQuery {what} {}", running.name);
            // The first poll checks whether submission exhausted the timeout.
            let completed =
                match Box::pin(poller.wait_until_done(&poll_url, &poll_context, submitted)).await {
                    Ok(completed) => completed,
                    Err(abandoned) if poller.abandoned(&abandoned) => {
                        let url = running.cancel_url(submission, poller.endpoint());
                        return Err(Box::pin(poller.cancel(&url, abandoned)).await);
                    }
                    Err(failure) => return Err(failure),
                };
            finished(submission, &completed, &running.name)?;
            Ok(running.remote_job(&completed))
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Submission {
    Batch,
    ClusterJob,
}

impl Submission {
    fn what(self) -> &'static str {
        match self {
            Self::Batch => "Dataproc batch",
            Self::ClusterJob => "Dataproc job",
        }
    }
}

struct RunningOperation {
    /// The operation resource name, e.g. `projects/p/regions/r/operations/1234`.
    name: String,
    /// The job ID `jobs.submitAsOperation` reports in the operation metadata, which is what
    /// `jobs.cancel` addresses. Absent for a batch, which is cancelled through its operation.
    job_id: Option<String>,
}

impl RunningOperation {
    fn new(submitted: &JsonValue, what: &str) -> Result<Self> {
        Ok(Self {
            name: resource_name(submitted, what)?,
            job_id: submitted
                .pointer("/metadata/jobId")
                .and_then(JsonValue::as_str)
                .filter(|job_id| !job_id.is_empty())
                .map(ToOwned::to_owned),
        })
    }

    /// What the statement reports as `BIGQUERY:query_id` once the submission succeeded: the
    /// cluster job's ID, or the batch's resource name, which Dataproc chooses when `batch_id` is
    /// unset. The operation name stands in when the service reported neither.
    fn remote_job(self, completed: &JsonValue) -> String {
        let batch = completed
            .pointer("/response/name")
            .and_then(JsonValue::as_str)
            .filter(|name| !name.is_empty());
        self.job_id
            .or_else(|| batch.map(ToOwned::to_owned))
            .unwrap_or(self.name)
    }

    /// `jobs.cancel` for a cluster job (by the job ID in the operation metadata), or
    /// `operations.cancel` for a batch, which has no cancel method of its own (`gcloud dataproc
    /// batches cancel` uses it).
    fn cancel_url(&self, submission: Submission, endpoint: &str) -> String {
        // `jobs.cancel` needs the project and region the operation name carries, in the shape
        // `projects/{project}/regions/{region}/operations/{id}`; anything else falls back to
        // cancelling the operation itself.
        if submission == Submission::ClusterJob
            && let Some(job_id) = self.job_id.as_deref()
        {
            let mut segments = self.name.split('/');
            if let (Some("projects"), Some(project), Some("regions"), Some(region)) = (
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
            ) {
                // Each is one URL segment, so `/` is escaped too: a job ID with `?`, `#`, or a
                // dot segment must not redirect the cancel.
                return format!(
                    "{endpoint}/v1/projects/{}/regions/{}/jobs/{}:cancel",
                    escape_url_component(project),
                    escape_url_component(region),
                    escape_url_component(job_id),
                );
            }
        }
        resource_url(endpoint, &self.name, ":cancel")
    }
}

/// The `Batch` resource the configured YAML document describes, as JSON.
///
/// The profile's `dataproc_batch` block reaches `batch_yml` verbatim, so it needs a real YAML
/// parser. Only the top-level mapping is checked; Dataproc validates its own fields. Quoting
/// survives, so `spark.executor.instances: "2"` stays the string `runtime_config.properties`
/// requires.
///
/// The parser's message is omitted because it may quote profile content; the error names only
/// the option and position.
fn batch_resource(batch_yml: &str) -> Result<JsonValue> {
    let key = BigQueryStatementOption::CreateBatchBatchYml.as_str();
    let batch: JsonValue = serde_norway::from_str(batch_yml).map_err(|source| {
        let location = source.location().map_or_else(String::new, |location| {
            format!(" at line {}, column {}", location.line(), location.column())
        });
        invalid_argument(format!(
            "BigQuery option {key} is not a valid YAML document{location}"
        ))
    })?;
    if !batch.is_object() {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must be a YAML mapping describing a Dataproc batch"
        )));
    }
    Ok(batch)
}

/// Checks the terminal state of a finished batch or job. Every state but the success state
/// fails, including `CANCELLED` and a job's `ATTEMPT_FAILURE`, which the fork reports as success.
fn finished(submission: Submission, completed: &JsonValue, name: &str) -> Result<()> {
    let what = submission.what();
    let (state_pointer, message_pointer, succeeded) = match submission {
        Submission::Batch => ("/state", "/stateMessage", "SUCCEEDED"),
        Submission::ClusterJob => ("/status/state", "/status/details", "DONE"),
    };
    if let Some(failure) = operation_failure(
        completed,
        &format!("the BigQuery {what} {name} failed"),
        "the service reported no reason",
    ) {
        return Err(failure);
    }
    // A `done` operation with neither an error nor a readable state reports no outcome, which is
    // an error rather than an assumption of success.
    let reached = completed.get("response").and_then(|response| {
        let state = response
            .pointer(state_pointer)
            .and_then(JsonValue::as_str)
            .filter(|state| !state.is_empty())?;
        Some((response, state))
    });
    let Some((response, state)) = reached else {
        return Err(error(
            format!("the BigQuery {what} {name} completed without reporting a terminal state"),
            Status::InvalidData,
        ));
    };
    if state == succeeded {
        return Ok(());
    }
    let detail = response
        .pointer(message_pointer)
        .and_then(JsonValue::as_str)
        .filter(|message| !message.is_empty())
        .map_or_else(String::new, |message| format!(": {message}"));
    Err(error(
        format!("the BigQuery {what} {name} finished in state {state}{detail}"),
        if state == "CANCELLED" {
            Status::Cancelled
        } else {
            Status::InvalidData
        },
    ))
}

fn encode(request: &JsonValue) -> Result<Vec<u8>> {
    encode_json(request, "the BigQuery Dataproc request")
}

#[cfg(test)]
mod tests;
