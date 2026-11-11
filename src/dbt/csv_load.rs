//! Raw CSV media upload for dbt 2.0 seeds.
//!
//! The upload has no whole-request deadline; reconciliation, polling, and cancellation use the
//! normal JSON deadline.

use adbc_core::error::{Result, Status};
use serde::Serialize;

use super::csv_seed::CsvSeedFile;
use super::jobs::{
    FinishedJob, JobConfiguration, JobOperation, LoadOrCopyJob, in_job_location, wait_for_submitted,
};
use crate::cancellation::{self, Operation};
use crate::diag::diag;
use crate::error::job_name;
use crate::query::{ExecutionContext, QualifiedTableName, StatementQueryOptions};
use crate::rest::client::RestClient;
use crate::rest::jobs::{
    JobClient, JobReference, JobRequest, may_have_created, may_still_run, settle_submission,
};
use crate::rest::metadata::TableId;
use crate::rest::model::TableSchema;
use crate::rest::request::{Request, RequestStart, multipart_related_file, send};
use crate::rest::retry::{MAX_ATTEMPTS, RetryClass, RetryDelays, classify};
use crate::rest::url::escape_url_component;

/// The job's name in error messages.
const WHAT: &str = "CSV load";

pub(crate) fn execute_csv_load(
    execution: &ExecutionContext,
    options: &StatementQueryOptions,
    destination: &QualifiedTableName,
    operation: &Operation,
    csv: CsvSeedFile<'_>,
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
            let content_type = format!("multipart/related; boundary={boundary}");
            let context = format!(
                "submit the BigQuery CSV load job {} into {}",
                job_name(request.reference()),
                destination.id()
            );
            let path = csv.path;
            let mut opened = Some(csv);
            let mut upload_request = || {
                let csv = match opened.take() {
                    Some(csv) => csv,
                    None => CsvSeedFile::open(path)?,
                };
                let body = multipart_related_file(
                    &boundary,
                    request.body(),
                    csv.file,
                    csv.length,
                    "text/csv",
                )?;
                Ok(Request::post_media(&url, &content_type, body, &context))
            };

            // A seed can be gigabytes, so the upload has no whole-request deadline.
            let upload = execution.state.rest_client()?;
            let jobs = execution.state.rest_job_client(&execution.runtime)?;
            let job = execution.runtime.block_on(submit_csv_load(
                operation,
                &configuration.credentials,
                &upload,
                &jobs,
                &mut upload_request,
                request.reference(),
                RetryDelays::SERVICE,
            ))?;
            Ok(FinishedJob {
                rows: job.output_rows(),
                job_id: request.reference().job_id.clone(),
            })
        },
    )
}

/// Sends the media `jobs.insert` and waits for the load it submitted.
///
/// A replay resends the whole seed, so `requested` carries a regional identity from
/// [`in_job_location`], and an outcome that may have created the job (a transport failure after
/// sending, a 5xx or a 408) is reconciled by fetching it. Only an upload that created nothing
/// and may yet succeed, because it could not connect or was rate-limited, is sent again, as
/// `jobs.insert` would, each time from a fresh `upload_request`.
async fn submit_csv_load<'r>(
    operation: &Operation,
    credentials: &google_cloud_auth::credentials::Credentials,
    upload: &RestClient,
    jobs: &JobClient,
    upload_request: &mut dyn FnMut() -> Result<Request<'r>>,
    requested: &JobReference,
    delays: RetryDelays,
) -> Result<LoadOrCopyJob> {
    // Register before the upload so cancellation can address the job if the response is lost or
    // the caller cancels mid-upload.
    operation.register_provisional_job(requested)?;
    let mut attempt = 0;
    let (failure, may_exist) = loop {
        attempt += 1;
        let request = match upload_request() {
            Ok(request) => request,
            Err(failure) => break (failure, false),
        };
        let context = request.context;
        let start = RequestStart::default();
        let answer = send(
            Some(operation),
            credentials,
            upload,
            request.with_start_signal(&start),
        )
        .await;
        // Unlike `jobs.insert`'s, this 409 creates no job: BigQuery refuses a `WRITE_EMPTY` load
        // into a table with rows with `duplicate: Already Exists: Table` before creating one.
        // Nor can it name this upload's job, since only an attempt that created nothing is resent.
        let refused =
            matches!(&answer, Ok((status, _)) if *status == reqwest::StatusCode::CONFLICT);
        let may_exist = may_have_created(&start, &answer) && !refused;
        let (class, failure) = match classify(answer, &format!("failed to {context}")) {
            Ok(body) => {
                operation.register_job(requested)?;
                return wait_for_submitted(jobs, operation, requested, &body, WHAT).await;
            }
            Err(rejected) => rejected,
        };
        let resend = !may_exist
            && class != RetryClass::Final
            && failure.status != Status::Cancelled
            && attempt < MAX_ATTEMPTS;
        if !resend {
            break (failure, may_exist);
        }
        diag!(
            Debug,
            "resending a BigQuery CSV load upload that created no job after attempt {attempt} \
             ({:?})",
            failure.status
        );
        if let Err(cancelled) =
            cancellation::sleep(Some(operation), delays.delay(class, attempt)).await
        {
            break (cancelled, false);
        }
    };
    // Either the job cannot exist and no cancel should address it, or a cancel that raced its
    // visibility gets its post-confirm retry, during the recovery GET too.
    settle_submission(Some(operation), requested, may_exist);
    if !may_exist || failure.status == Status::Cancelled {
        return Err(failure);
    }
    let recovered = jobs
        .reconcile_submission(Some(operation), requested, WHAT, Some(failure))
        .await
        .map_err(|failure| may_still_run(failure, requested, false))?;
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
