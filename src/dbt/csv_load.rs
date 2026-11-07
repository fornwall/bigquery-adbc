//! Raw CSV media upload for dbt-core 2.0 seeds.
//!
//! The upload has no whole-request deadline; reconciliation, polling, and cancellation use the
//! normal JSON deadline.

use adbc_core::error::{Result, Status};
use serde::Serialize;

use super::csv_seed::CsvSeedFile;
use super::jobs::{
    FinishedJob, JobConfiguration, JobOperation, LoadOrCopyJob, in_job_location, wait_for_submitted,
};
use crate::cancellation::Operation;
use crate::error::job_name;
use crate::query::{ExecutionContext, QualifiedTableName, StatementQueryOptions};
use crate::rest::client::RestClient;
use crate::rest::jobs::{JobClient, JobReference, JobRequest, may_have_created, settle_submission};
use crate::rest::metadata::TableId;
use crate::rest::model::TableSchema;
use crate::rest::rejection::service_error;
use crate::rest::request::{Request, RequestStart, multipart_related_file, send};
use crate::rest::url::escape_url_component;

/// The job's name in error messages.
const WHAT: &str = "CSV load";

pub(crate) fn execute_csv_load(
    execution: &ExecutionContext,
    options: &StatementQueryOptions,
    destination: &QualifiedTableName,
    operation: &Operation,
    csv: CsvSeedFile,
    schema: &TableSchema,
    delimiter: &str,
) -> Result<FinishedJob> {
    in_job_location(
        execution,
        operation,
        destination,
        "csv",
        WHAT,
        |reference| {
            let request = build_load_request(options, destination, schema, delimiter, reference)?;
            let configuration = execution.state.configuration(&execution.runtime)?;
            let endpoint = configuration.endpoints.bigquery();
            let url = format!(
                "{endpoint}/upload/bigquery/v2/projects/{}/jobs?uploadType=multipart",
                escape_url_component(&request.reference().project_id)
            );
            let boundary = format!("adbc_csv_boundary_{}", request.reference().job_id);
            let body = multipart_related_file(
                &boundary,
                request.body(),
                csv.file,
                csv.length,
                "text/csv",
            )?;
            let content_type = format!("multipart/related; boundary={boundary}");
            let context = format!(
                "submit the BigQuery CSV load job {} into {}",
                job_name(request.reference()),
                destination.id()
            );

            // A seed can be gigabytes, so the upload has no whole-request deadline.
            let upload = execution.state.rest_client()?;
            let jobs = execution.state.rest_job_client(&execution.runtime)?;
            let job = execution.runtime.block_on(submit_csv_load(
                operation,
                &configuration.credentials,
                &upload,
                &jobs,
                Request::post_media(&url, &content_type, body, &context),
                request.reference(),
            ))?;
            Ok(FinishedJob {
                rows: job.output_rows(),
                job_id: request.reference().job_id.clone(),
            })
        },
    )
}

/// Sends the one-shot media `jobs.insert` and waits for the load it submitted.
///
/// The media upload is never replayed, so `requested` carries a regional identity from
/// [`in_job_location`], and an outcome that may have created the job (a transport failure, a 5xx,
/// or a 409) is reconciled by fetching it.
async fn submit_csv_load(
    operation: &Operation,
    credentials: &google_cloud_auth::credentials::Credentials,
    upload: &RestClient,
    jobs: &JobClient,
    upload_request: Request<'_>,
    requested: &JobReference,
) -> Result<LoadOrCopyJob> {
    let context = upload_request.context;
    // Register before the upload so cancellation can address the job if the response is lost or
    // the caller cancels mid-upload.
    operation.register_provisional_job(requested)?;
    let start = RequestStart::default();
    let upload_request = upload_request.with_start_signal(&start);
    let answer = send(Some(operation), credentials, upload, upload_request).await;
    let may_exist = may_have_created(&start, &answer);
    let failure = match answer {
        Ok((status, body)) if status.is_success() => {
            operation.register_job(requested)?;
            return wait_for_submitted(jobs, operation, requested, &body, WHAT).await;
        }
        Ok((status, body)) => service_error(&format!("failed to {context}"), status, &body),
        Err(failure) => failure,
    };
    // Either the job cannot exist and no cancel should address it, or a cancel that raced its
    // visibility gets its post-confirm retry, during the recovery GET too.
    settle_submission(Some(operation), requested, may_exist);
    if !may_exist || failure.status == Status::Cancelled {
        return Err(failure);
    }
    let recovered = jobs
        .reconcile_submission(Some(operation), requested, WHAT, Some(failure))
        .await?;
    wait_for_submitted(jobs, operation, requested, &recovered, WHAT).await
}

fn build_load_request(
    options: &StatementQueryOptions,
    destination: &QualifiedTableName,
    schema: &TableSchema,
    delimiter: &str,
    reference: JobReference,
) -> Result<JobRequest> {
    let configuration = JobConfiguration::new(
        options,
        JobOperation::Load(LoadConfiguration {
            schema,
            destination_table: destination.id(),
            create_disposition: options.create_disposition.as_str(),
            write_disposition: options.write_disposition.as_str(),
            field_delimiter: delimiter,
            skip_leading_rows: 1,
            allow_quoted_newlines: true,
            source_format: "CSV",
            autodetect: false,
        }),
    );
    JobRequest::new(
        reference,
        false,
        &configuration,
        "the BigQuery CSV load job",
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LoadConfiguration<'a> {
    schema: &'a TableSchema,
    destination_table: TableId<'a>,
    create_disposition: &'static str,
    write_disposition: &'static str,
    field_delimiter: &'a str,
    skip_leading_rows: i32,
    allow_quoted_newlines: bool,
    source_format: &'static str,
    autodetect: bool,
}

#[cfg(test)]
mod tests;
