//! dbt-core 2.0's option-only BigQuery table copy operation.
//!
//! A copy job bills no bytes, keeps the source's partitioning, clustering, and column metadata,
//! and can address one partition with a `table$20240101` decorator, which GoogleSQL cannot name.
//! dbt's `bq_copy_partitions` macro relies on that. See `DBT-CORE.md`.

use adbc_core::error::Result;
use serde::Serialize;

use super::jobs::{
    FinishedJob, JobConfiguration, JobOperation, in_job_location, wait_for_submitted,
};
use super::missing_option;
use crate::cancellation::Operation;
use crate::error::not_implemented;
use crate::query::{DestinationTable, ExecutionContext, StatementQueryOptions};
use crate::rest::jobs::{JobReference, JobRequest};
use crate::rest::metadata::TableId;
use crate::{BigQueryStatementOption, BigQueryWriteDisposition};

/// A table copy configured on an ADBC statement by dbt-core 2.0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TableCopyOptions {
    pub(crate) source: Option<DestinationTable>,
    pub(crate) destination: Option<DestinationTable>,
    /// Defaults to `WRITE_EMPTY`, independently of the SQL query's write disposition.
    pub(crate) write_disposition: BigQueryWriteDisposition,
}

impl TableCopyOptions {
    /// Whether either endpoint is configured.
    pub(crate) fn is_active(&self) -> bool {
        self.source.is_some() || self.destination.is_some()
    }

    /// Submits the copy job and waits for it, returning the rows BigQuery reports copying, if
    /// any.
    ///
    /// A dry run is refused before any request: BigQuery answers `dryRun` on a copy job with
    /// `400 Dry run not supported for copy job`, so there is nothing it could validate.
    pub(crate) fn execute(
        &self,
        context: &ExecutionContext,
        options: &StatementQueryOptions,
        operation: &Operation,
    ) -> Result<FinishedJob> {
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| missing_option(WHAT, BigQueryStatementOption::CopyTableSource))?;
        let destination = self
            .destination
            .as_ref()
            .ok_or_else(|| missing_option(WHAT, BigQueryStatementOption::CopyTableDestination))?;
        if options.dry_run {
            return Err(not_implemented(format!(
                "a BigQuery table copy cannot be dry-run: BigQuery does not support {} on copy \
                 jobs; clear the option to run the copy",
                BigQueryStatementOption::DryRun.as_str()
            )));
        }
        // The copy runs where its destination is; a source elsewhere fails with BigQuery's own
        // error.
        in_job_location(context, operation, destination, "copy", WHAT, |reference| {
            let request = self.build_job(options, source, destination, reference)?;
            context.runtime.block_on(async {
                let client = context.state.rest_job_client(&context.runtime)?;
                let body = client.insert(Some(operation), &request, WHAT).await?;
                let job = wait_for_submitted(&client, operation, request.reference(), &body, WHAT)
                    .await?;
                Ok(FinishedJob {
                    rows: job.copied_rows(),
                    job_id: request.reference().job_id.clone(),
                })
            })
        })
    }

    fn build_job(
        &self,
        options: &StatementQueryOptions,
        source: &DestinationTable,
        destination: &DestinationTable,
        reference: JobReference,
    ) -> Result<JobRequest> {
        let configuration = JobConfiguration::new(
            options,
            JobOperation::Copy(CopyConfiguration {
                source_table: source.id(),
                destination_table: destination.id(),
                create_disposition: options.create_disposition.as_str(),
                write_disposition: self.write_disposition.as_str(),
            }),
        );
        Ok(JobRequest::new(
            reference,
            false,
            &configuration,
            "the BigQuery table copy job",
        )?
        .writing(destination.id()))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CopyConfiguration<'a> {
    source_table: TableId<'a>,
    destination_table: TableId<'a>,
    create_disposition: &'static str,
    write_disposition: &'static str,
}

/// The job's name in error messages.
const WHAT: &str = "table copy";

#[cfg(test)]
mod tests;
