//! dbt-core 2.0's option-only Vertex AI notebook execution job.
//!
//! A dbt python model with `submission_method: bigframes` is compiled to a notebook in Cloud
//! Storage and executed by Vertex AI. Any `adbc.bigquery.notebook_execute_job.*` option selects
//! the operation. Two inputs arrive under Dataproc keys (`src/dbt/dataproc.rs`):
//! `adbc.bigquery.create_batch.batch_id` is the display name and
//! `adbc.bigquery.dataproc.pooling_timeout` bounds the wait. The `gsc` misspellings are part of
//! the dbt-core 2.0 option contract.
//!
//! The create returns a long-running operation, polled until `done`; the job is then polled until
//! a terminal `JOB_STATE_*`, because the operation reports only that the create finished. One
//! client-side deadline covers all of it. See `DBT-CORE.md`.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use serde_json::Value as JsonValue;

use super::dataproc::DataprocOptions;
use super::lro::{Delivery, LroPoller, operation_failure, resource_name, rpc_status_error};
use super::{region_segment, resource_path, resource_segment, text_slots};
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::error::{error, invalid_state, not_found};
use crate::query::ExecutionContext;
use crate::rest::json::encode_json;
use crate::rest::request::Request;
use crate::rest::url::escape_url_component;

const OPERATION: &str = "Vertex AI notebook execution job";

/// Detect startup failures quickly, then back off to avoid exhausting the polling quota
/// when several dbt threads run notebooks concurrently.
const INITIAL_POLL_DELAY: Duration = Duration::from_millis(100);
const MAX_POLL_DELAY: Duration = Duration::from_secs(10);

const NO_FAILURE_DETAIL: &str = "the service reported no failure detail";

/// The `ListNotebookRuntimeTemplates` filter the fork sends, percent-encoded for the query
/// string: `notebookRuntimeType = ONE_CLICK`.
const ONE_CLICK_FILTER: &str = "notebookRuntimeType%20%3D%20ONE_CLICK";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NotebookExecutionOptions {
    gsc_path: Option<String>,
    gsc_bucket: Option<String>,
    model_file_name: Option<String>,
    /// Accepted but unused: the fork reads it only to find the notebook output it logs.
    model_name: Option<String>,
    parent: Option<String>,
    project: Option<String>,
    region: Option<String>,
    /// An explicit execution principal, overriding the one the credentials name.
    service_account: Option<String>,
    template_id: Option<String>,
}

text_slots!(NotebookExecutionOptions {
    NotebookExecuteJobGscPath => gsc_path,
    NotebookExecuteJobGscBucket => gsc_bucket,
    NotebookExecuteJobModelFileName => model_file_name,
    NotebookExecuteJobModelName => model_name,
    NotebookExecuteJobParent => parent,
    NotebookExecuteJobProject => project,
    NotebookExecuteJobRegion => region,
    NotebookExecuteJobServiceAccount => service_account,
    NotebookExecuteJobTemplateId => template_id,
});

impl NotebookExecutionOptions {
    pub(crate) fn is_active(&self) -> bool {
        self.any_text_option_set()
    }

    /// Returns the job resource name for `BIGQUERY:query_id` and the Colab Enterprise console.
    /// `pooling_timeout` covers template resolution, creation and job polling. Requests already
    /// in flight may overrun it; no further submission or poll starts after expiry.
    pub(crate) fn execute(
        &self,
        context: &ExecutionContext,
        dataproc: &DataprocOptions,
        operation: &Operation,
    ) -> Result<String> {
        use BigQueryStatementOption as Key;
        let required = |key| self.required(OPERATION, key);
        let parent = required(Key::NotebookExecuteJobParent)?;
        let gsc_path = required(Key::NotebookExecuteJobGscPath)?;
        let gsc_bucket = required(Key::NotebookExecuteJobGscBucket)?;
        let model_file_name = required(Key::NotebookExecuteJobModelFileName)?;
        let project = required(Key::NotebookExecuteJobProject)?;
        let region = required(Key::NotebookExecuteJobRegion)?;
        // These reach the request path, and the region also the host, so a value that would
        // redirect the credentialed request is refused.
        let parent = resource_path(parent, Key::NotebookExecuteJobParent)?;
        let project = resource_segment(project, Key::NotebookExecuteJobProject)?;
        let region = region_segment(region, Key::NotebookExecuteJobRegion)?;
        // The display name arrives under a Dataproc key, so the error explains its role here.
        // Unlike in the fork it is required: without it the job cannot be found in the Colab
        // Enterprise console.
        let display_name = dataproc.create_batch_batch_id().ok_or_else(|| {
            invalid_state(format!(
                "BigQuery option {} is required for submission_method: bigframes, as the \
                 Vertex AI notebook execution job's display name in the Colab Enterprise \
                 console",
                BigQueryStatementOption::CreateBatchBatchId.as_str()
            ))
        })?;

        let configuration = context.state.configuration(&context.runtime)?;
        // An explicit principal overrides the one derived from the credentials, which may name a
        // different principal or none.
        let service_account = self
            .service_account
            .as_deref()
            .or(configuration.execution_principal.as_deref());
        let request = NotebookExecutionJob {
            parent,
            project,
            region,
            template_id: self.template_id.as_deref(),
            gcs_notebook_uri: gsc_path,
            gcs_output_uri: format!("gs://{gsc_bucket}/{model_file_name}/logs"),
            display_name,
            service_account,
        };
        let mut poller = LroPoller::new(
            operation,
            &configuration.credentials,
            context.state.rest_client()?,
            configuration
                .endpoints
                .vertex(region, BigQueryStatementOption::NotebookExecuteJobRegion)?,
            dataproc.poll_budget(OPERATION, INITIAL_POLL_DELAY, MAX_POLL_DELAY),
        );
        context
            .runtime
            .block_on(Box::pin(run(&mut poller, &request)))
    }
}

struct NotebookExecutionJob<'a> {
    parent: &'a str,
    project: &'a str,
    region: &'a str,
    template_id: Option<&'a str>,
    gcs_notebook_uri: &'a str,
    gcs_output_uri: String,
    display_name: &'a str,
    /// The `execution_identity` to send, or `None` to leave the choice to the runtime template.
    service_account: Option<&'a str>,
}

async fn run(poller: &mut LroPoller<'_>, request: &NotebookExecutionJob<'_>) -> Result<String> {
    let template = Box::pin(runtime_template(poller, request)).await?;
    let submitted = Box::pin(create(poller, request, &template)).await?;
    let operation_name = resource_name(&submitted, "Vertex AI notebook execution operation")?;
    let job_name = operation_name
        .split_once("/operations/")
        .map(|(job, _)| job.to_owned())
        .ok_or_else(|| {
            error(
                format!(
                    "the BigQuery Vertex AI notebook execution operation {operation_name} does \
                     not name a job"
                ),
                Status::InvalidData,
            )
        })?;
    poller.set_running(job_name.clone());
    match Box::pin(wait_for_create(
        poller,
        submitted,
        &operation_name,
        &job_name,
    ))
    .await
    {
        Ok(()) => {}
        // Until the create operation is done, cancelling it is the only way to stop the job.
        Err(abandoned) if poller.abandoned(&abandoned) => {
            let url = poller.resource_url(&operation_name, ":cancel");
            return Err(Box::pin(poller.cancel(&url, abandoned)).await);
        }
        Err(failure) => return Err(failure),
    }
    match Box::pin(wait_for_job(poller, &job_name)).await {
        Ok(()) => Ok(job_name),
        // A done operation cannot be cancelled, and Vertex AI has no method that cancels a
        // notebook execution job, so nothing can be asked to stop it.
        Err(mut abandoned) if poller.abandoned(&abandoned) => {
            let _ = write!(
                abandoned.message,
                "; the BigQuery {} may still be running",
                poller.running()
            );
            Err(abandoned)
        }
        Err(failure) => Err(failure),
    }
}

async fn wait_for_create(
    poller: &mut LroPoller<'_>,
    submitted: JsonValue,
    operation_name: &str,
    job_name: &str,
) -> Result<()> {
    let url = poller.resource_url(operation_name, "");
    let created = poller
        .wait_until_done(
            &url,
            &format!("poll the BigQuery Vertex AI notebook execution operation {operation_name}"),
            submitted,
        )
        .await?;
    match operation_failure(
        &created,
        &format!("the BigQuery {OPERATION} {job_name} failed"),
        NO_FAILURE_DETAIL,
    ) {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

async fn wait_for_job(poller: &mut LroPoller<'_>, job_name: &str) -> Result<()> {
    let url = poller.resource_url(job_name, "");
    let context = format!("read the BigQuery {OPERATION} {job_name}");
    loop {
        let job = poller.poll(&url, &context).await?;
        // Proto3 JSON omits an enum's zero value, so an absent state is the unspecified one.
        let state = job
            .get("jobState")
            .map_or(Some("JOB_STATE_UNSPECIFIED"), JsonValue::as_str)
            .unwrap_or_default();
        if let Some(outcome) = job_outcome(state, &job, job_name) {
            return outcome;
        }
    }
}

/// The runtime template the job runs on: the configured one, or the first `ONE_CLICK`
/// template the project already has.
///
/// Where the fork silently creates a durable `default-one-click-notebook` template on the
/// project's default VPC (with no timeout and no cleanup), this reports the missing template.
async fn runtime_template(
    poller: &LroPoller<'_>,
    request: &NotebookExecutionJob<'_>,
) -> Result<String> {
    if let Some(template_id) = request.template_id {
        // Interpolated into a resource name, so a `/` would name a different resource.
        let template_id = resource_segment(
            template_id,
            BigQueryStatementOption::NotebookExecuteJobTemplateId,
        )?;
        return Ok(format!(
            "projects/{}/locations/{}/notebookRuntimeTemplates/{template_id}",
            request.project, request.region
        ));
    }
    let mut url = poller.resource_url(
        request.parent,
        &format!("/notebookRuntimeTemplates?filter={ONE_CLICK_FILTER}"),
    );
    let first_page_url_len = url.len();
    let mut page_tokens = HashSet::new();
    let context = format!(
        "list the BigQuery Vertex AI notebook runtime templates in {}",
        request.parent
    );
    loop {
        let listed =
            Box::pin(poller.request(Delivery::Idempotent, &|| Request::get(&url, &context)))
                .await?;
        let templates = match listed.get("notebookRuntimeTemplates") {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::Array(templates)) => templates.first(),
            Some(_) => {
                return Err(error(
                    format!(
                        "the response to {context} has a non-array notebookRuntimeTemplates field"
                    ),
                    Status::InvalidData,
                ));
            }
        };
        if let Some(template) = templates {
            return resource_name(template, "Vertex AI notebook runtime template");
        }
        // A filtered page can be empty before the end of the collection (AIP-158).
        match listed.get("nextPageToken") {
            None | Some(JsonValue::Null) => break,
            Some(JsonValue::String(token)) if token.is_empty() => break,
            Some(JsonValue::String(token)) => {
                if !page_tokens.insert(token.to_owned()) {
                    return Err(error(
                        format!("the response to {context} repeated a page token"),
                        Status::InvalidData,
                    ));
                }
                url.truncate(first_page_url_len);
                let _ = write!(url, "&pageToken={}", escape_url_component(token));
            }
            Some(_) => {
                return Err(error(
                    format!("the response to {context} has a non-string nextPageToken field"),
                    Status::InvalidData,
                ));
            }
        }
    }
    Err(not_found(format!(
        "{} has no ONE_CLICK notebook runtime template; set {} to an existing \
         template, or create a ONE_CLICK template in that project and region",
        request.parent,
        BigQueryStatementOption::NotebookExecuteJobTemplateId.as_str()
    )))
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct NotebookExecutionJobResource<'a> {
    gcs_notebook_source: GcsNotebookSource<'a>,
    gcs_output_uri: &'a str,
    display_name: &'a str,
    notebook_runtime_template_resource_name: &'a str,
    /// One half of the `execution_identity` oneof, sent only when a principal is named; otherwise
    /// the runtime template decides. `executionUser` is never sent: the fork guesses it from a
    /// userinfo lookup, and the driver does not guess the execution principal.
    #[serde(skip_serializing_if = "Option::is_none")]
    service_account: Option<&'a str>,
}

#[derive(serde::Serialize)]
struct GcsNotebookSource<'a> {
    uri: &'a str,
}

async fn create(
    poller: &LroPoller<'_>,
    request: &NotebookExecutionJob<'_>,
    template: &str,
) -> Result<JsonValue> {
    // The fork's own request shape and `gs://{bucket}/{model_file_name}/logs` output URI. No
    // `executionTimeout` is set: `pooling_timeout` is a client-side deadline.
    let body = NotebookExecutionJobResource {
        gcs_notebook_source: GcsNotebookSource {
            uri: request.gcs_notebook_uri,
        },
        gcs_output_uri: &request.gcs_output_uri,
        display_name: request.display_name,
        notebook_runtime_template_resource_name: template,
        service_account: request.service_account,
    };
    let url = poller.resource_url(request.parent, "/notebookExecutionJobs");
    let body = encode_json(&body, "the BigQuery Vertex AI notebook execution job")?;
    let context = format!(
        "create the BigQuery {OPERATION} {} in {}",
        request.display_name, request.parent
    );
    let body = bytes::Bytes::from(body);
    Box::pin(poller.request(Delivery::Once, &|| {
        Request::post_json(&url, body.clone(), &context)
    }))
    .await
}

/// The `JobState` values while the job may still progress. Any other state, known or not, ends
/// the poll loop: without a `pooling_timeout`, waiting on an unknown state might never end.
const NON_TERMINAL_JOB_STATES: [&str; 7] = [
    "JOB_STATE_UNSPECIFIED",
    "JOB_STATE_QUEUED",
    "JOB_STATE_PENDING",
    "JOB_STATE_RUNNING",
    "JOB_STATE_CANCELLING",
    "JOB_STATE_PAUSED",
    "JOB_STATE_UPDATING",
];

/// The outcome of a terminal `JobState`, or `None` while the job is still running.
///
/// `JOB_STATE_PARTIALLY_SUCCEEDED` is a failure: some cells raised, so the relation is not what
/// the model describes. The fork also fails it, through an "unexpected state" error.
fn job_outcome(state: &str, job: &JsonValue, job_name: &str) -> Option<Result<()>> {
    let context = || format!("the BigQuery {OPERATION} {job_name}");
    match state {
        "JOB_STATE_SUCCEEDED" => Some(Ok(())),
        "JOB_STATE_CANCELLED" => Some(Err(error(
            format!("{} was cancelled", context()),
            Status::Cancelled,
        ))),
        "JOB_STATE_EXPIRED" => Some(Err(error(
            format!("{} expired before it finished", context()),
            Status::Timeout,
        ))),
        "JOB_STATE_FAILED" | "JOB_STATE_PARTIALLY_SUCCEEDED" => Some(Err(rpc_status_error(
            &format!("{} finished as {state}", context()),
            job.get("status"),
            NO_FAILURE_DETAIL,
        ))),
        state if NON_TERMINAL_JOB_STATES.contains(&state) => None,
        state => Some(Err(error(
            format!(
                "{} reported the unrecognized state {state:?}; the driver knows no outcome for \
                 it and waits only on {}",
                context(),
                NON_TERMINAL_JOB_STATES.join(", "),
            ),
            Status::InvalidData,
        ))),
    }
}

#[cfg(test)]
mod tests;
