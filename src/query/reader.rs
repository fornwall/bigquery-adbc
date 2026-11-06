//! The reader for a completed query's rows: Storage Read for a paginated result or a row too
//! large for REST, and the resident REST response for a complete result.

use adbc_core::error::{Result, Status};
use arrow_array::RecordBatchReader;

use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{error, job_name, with_job_link};
use crate::pipeline::spawn_result_reader;
use crate::rest::jobs::{JobClient, JobReference};
use crate::storage_read::Opened;

use super::job_metadata::{result_schema, schema_with_rest_metadata};
use super::pager::QueryPager;
use super::rest_buffer::{buffer_rest_query_results, empty_reader, resident_reader};
use super::{ExecutionContext, ExecutionProgress, direct, response};

pub(super) fn reader_from_rest_query(
    execution: &ExecutionContext,
    complete: direct::CompletedQuery,
    operation: Option<Operation>,
    progress: Option<&ExecutionProgress>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let direct::CompletedQuery {
        mut metadata,
        page,
        completed_job,
        returns_rows,
    } = complete;
    let result_options = execution.result_options;
    let total_rows = metadata
        .total_rows
        .and_then(|total_rows| usize::try_from(total_rows).ok());
    let rest_reference = metadata.job_reference.clone();
    let link = |failure| with_job_link(failure, rest_reference.as_ref());
    let preliminary_schema = result_schema(
        metadata.schema.as_ref(),
        &metadata.statement_type,
        "BigQuery query",
    )
    .map_err(link)?;
    let rows_exceed_rest = returns_rows && metadata.rows_exceed_rest;
    // Later rows can exceed REST's row limit even when the first page is narrow. Choose the
    // reader before emitting any rows: switching midway would require matching row order across
    // the two APIs to avoid omissions or duplicates.
    let storage_read = returns_rows
        && (rows_exceed_rest || page.page_token.is_some())
        && rest_reference.as_ref().is_some_and(|reference| {
            crate::storage_read::eligible(
                &execution.state,
                result_options,
                reference,
                &preliminary_schema,
            )
        });

    // A best-effort jobs.get is needed only for a Storage Read destination table, a response that
    // never named its statement type, or the costs a completing poll omits; a final jobs.insert
    // response supplies all of them. BigQuery omits zero-valued statistics, so a missing cost
    // statistic alone does not justify the request and stays unreported rather than published as
    // zero.
    let fetch_job = completed_job.is_none()
        && rest_reference.is_some()
        && (storage_read || metadata.statement_type.is_empty() || metadata.completed_by_poll);
    let client = fetch_job
        .then(|| execution.state.rest_job_client(&execution.runtime))
        .transpose()
        .map_err(link)?;
    // The attempt that received a final `jobs.insert` response already absorbed it and published
    // its cost; only a fetched job is absorbed here.
    let mut job = completed_job;
    let mut job_enriched_schema = false;
    if let Some(client) = client.as_ref()
        && let Some(reference) = rest_reference.as_ref()
        && let Some(mut fetched) =
            describe_query_job(execution, client, operation.as_ref(), reference)?
    {
        job_enriched_schema = metadata.absorb_job(&mut fetched);
        if fetched.is_done()
            && let Some(progress) = progress
        {
            progress.complete(fetched.total_bytes_processed());
        }
        job = Some(fetched);
    }
    // A job can identify a SELECT after the preliminary conversion accepted an absent schema.
    // Recheck that case, but reuse a present schema unless the job supplied a replacement.
    let schema = if job_enriched_schema || metadata.schema.is_none() {
        result_schema(
            metadata.schema.as_ref(),
            &metadata.statement_type,
            "BigQuery query",
        )
        .map_err(link)?
    } else {
        preliminary_schema
    };
    let schema = schema_with_rest_metadata(schema, &metadata);

    // DML, DDL and empty SELECT results need no producer, queue or conversion permits.
    // An empty page with a continuation token can still lead to rows.
    if !returns_rows || (page.rows.is_empty() && page.page_token.is_none() && !rows_exceed_rest) {
        return Ok(empty_reader(schema));
    }

    let destination = job
        .as_ref()
        .and_then(response::JobResponse::destination_table);
    match (storage_read, rest_reference.as_ref(), destination) {
        (true, Some(reference), Some(destination)) => {
            match crate::storage_read::reader(
                &execution.runtime,
                &execution.state,
                operation.as_ref(),
                result_options,
                reference,
                destination.clone(),
                schema.clone(),
            )
            .map_err(link)?
            {
                Opened::Reader(reader) => {
                    diag!(
                        Debug,
                        "reading BigQuery job {} through Storage Read",
                        job_name(reference)
                    );
                    return Ok(reader);
                }
                Opened::Fallback(reason) if rows_exceed_rest => {
                    return Err(link(rows_exceed_rest_error(Some(reason))));
                }
                Opened::Fallback(reason) => diag!(
                    Warn,
                    "reading BigQuery results over REST: {}",
                    reason.message
                ),
            }
        }
        // Describing the job, which names its result table, failed; that was reported already.
        (true, _, _) if rows_exceed_rest => {
            return Err(link(rows_exceed_rest_error(Some(error(
                "describing the BigQuery job, which names its result table, failed",
                Status::IO,
            )))));
        }
        _ if rows_exceed_rest => return Err(link(rows_exceed_rest_error(None))),
        _ => {}
    }

    // Only a terminal page proves that all rows are resident; totalRows may be stale. Bound
    // both rows and encoded bytes so a wide single row still uses the buffered pipeline.
    if page.page_token.is_none()
        && page.rows.len() <= response::INLINE_RESULT_ROWS
        && page.encoded_len <= response::INLINE_RESULT_BYTES
    {
        return Ok(resident_reader(
            execution.runtime.clone(),
            schema,
            operation,
            page.rows,
        ));
    }

    let client = match client {
        Some(client) => client,
        None => execution
            .state
            .rest_job_client(&execution.runtime)
            .map_err(link)?,
    };
    let pager = QueryPager::new(client, rest_reference, operation.clone(), page);
    Ok(spawn_result_reader(
        execution.runtime.clone(),
        schema,
        operation,
        |sink| buffer_rest_query_results(pager, sink, result_options, total_rows),
    ))
}

/// Reports a result only Storage Read can return, when it did not. With the reason a read session
/// failed, the error keeps that reason's status, so a transient refusal stays retryable; without
/// one, Storage Read was disabled.
fn rows_exceed_rest_error(
    storage_read_failure: Option<adbc_core::error::Error>,
) -> adbc_core::error::Error {
    const CONTEXT: &str =
        "BigQuery cannot return this result over REST because a row exceeds its 20 MiB limit";
    match storage_read_failure {
        Some(mut reason) => {
            reason.message = format!(
                "{CONTEXT}, and Storage Read could not read it: {}",
                reason.message
            );
            reason
        }
        None => error(
            format!(
                "{CONTEXT}; it can only be read through the Storage Read API: set {} to false and \
                 grant bigquery.readsessions.create",
                crate::BigQueryStatementOption::UseStorageApiDisabledClient.as_str()
            ),
            Status::InvalidArguments,
        ),
    }
}

/// Describes the query's job, best effort; `None` means only the query response's metadata is
/// available.
///
/// The query has already executed, so only cancellation propagates: a failed, undecodable or
/// mismatched job resource must not report executed DML as failed. The request is a retried poll
/// so that one transient error does not cost a large result its Storage Read.
fn describe_query_job(
    execution: &ExecutionContext,
    client: &JobClient,
    operation: Option<&Operation>,
    reference: &JobReference,
) -> Result<Option<response::JobResponse>> {
    match execution.runtime.block_on(direct::fetch_job(
        client,
        operation,
        reference,
        "query metadata",
    )) {
        Ok(job) => Ok(Some(job)),
        Err(source) if source.status == Status::Cancelled => {
            Err(with_job_link(source, Some(reference)))
        }
        Err(source) => {
            // This request names the destination table a read session opens, so a denial
            // reflects the caller's grants and would recur on every query. Disable Storage Read
            // for the project rather than paying for the denial per query.
            if source.status == Status::Unauthorized {
                execution.state.disable_storage_read(&reference.project_id);
            }
            diag!(
                Warn,
                "reporting only query-response metadata and reading over REST: failed to describe \
                 the BigQuery query job ({source})"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests;
