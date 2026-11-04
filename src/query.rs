//! BigQuery query execution: building and sending queries, describing their jobs, and turning
//! their results into readers.
//!
//! REST result schemas and rows are converted to Arrow by [`crate::rows`]; large results are
//! served as Arrow directly by [`crate::storage_read`], and [`reader`] picks one per query. Small
//! complete REST results convert lazily; other results use the pipeline in [`crate::pipeline`].

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use crate::cancellation::Operation;
use crate::clients::ClientState;
use crate::error::error;
use crate::identifiers::ProjectIdentifier;
use crate::parameters::parameter_schema;
use crate::rest::model::QueryParameter;
use crate::runtime::SharedRuntime;

mod context;
mod direct;
mod job_metadata;
mod options;
mod pager;
mod partition;
mod plan;
mod progress;
mod reader;
mod request;
mod response;
mod rest_buffer;
mod transaction_end;

#[cfg(feature = "benchmark")]
pub(crate) mod benchmark;

use job_metadata::{result_schema, schema_with_rest_metadata};
use partition::{decode_partition, encode_partition};
use plan::QueryJobCreation;
use reader::reader_from_rest_query;

pub(crate) use context::{ExecutionContext, QueryContext, StatementQueryContext};
pub(crate) use options::{DestinationTable, QueryDefaults, StatementQueryOptions};
pub(crate) use progress::ExecutionProgress;
pub(crate) use rest_buffer::empty_reader;

/// Whether the caller consumes the query's result rows.
///
/// `ExecuteUpdate`, partition creation, dry-run schema discovery and explicit-destination
/// execution need only completion metadata, so they request no result page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryResultMode {
    Rows,
    MetadataOnly,
}

impl QueryResultMode {
    const fn returns_rows(self) -> bool {
        matches!(self, Self::Rows)
    }

    const fn max_results(self) -> u32 {
        match self {
            Self::Rows => response::RESULT_PAGE_ROWS,
            Self::MetadataOnly => 0,
        }
    }
}

/// Runs a statement's query, returning its reader and the row count `AdbcStatementExecuteQuery`
/// reports (see [`reported_rows`]). The count is `None` for DDL, a dry run, and a query that
/// writes to a destination table.
pub(crate) fn execute_query(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<QueryParameter>)>,
) -> Result<(Box<dyn RecordBatchReader + Send + 'static>, Option<i64>)> {
    let complete = direct::complete_statement_query(
        context,
        sql,
        parameters,
        QueryJobCreation::Optional,
        if context.options.dry_run || context.options.destination_table.is_some() {
            QueryResultMode::MetadataOnly
        } else {
            QueryResultMode::Rows
        },
        if context.options.dry_run {
            "BigQuery dry run failed"
        } else {
            "BigQuery query execution failed"
        },
    )?;
    let rows_affected = reported_rows(&complete.metadata, context.options);
    if context.options.dry_run {
        let schema = dry_run_schema(&complete.metadata, "BigQuery dry run")?;
        return Ok((empty_reader(schema), None));
    }
    let reader = reader_from_rest_query(
        context.execution,
        complete,
        Some(context.operation.clone()),
        context.progress,
    )?;
    Ok((reader, rows_affected))
}

#[derive(Debug)]
pub(crate) struct QueryPartition {
    pub(crate) descriptor: Vec<u8>,
    pub(crate) schema: Schema,
    pub(crate) rows_affected: Option<i64>,
}

pub(crate) fn execute_query_partition_with_parameters(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<QueryParameter>)>,
) -> Result<QueryPartition> {
    let complete = direct::complete_statement_query(
        context,
        sql,
        parameters,
        QueryJobCreation::Required,
        QueryResultMode::MetadataOnly,
        "BigQuery partitioned query execution failed",
    )?;
    let metadata = &complete.metadata;
    // `QueryJobCreation::Required` rejects a jobless `jobs.query` response, and the `jobs.insert`
    // route reports the reference it validated, so a completed partitioned query has a job.
    let job_reference = metadata.job_reference.as_ref().expect(
        "a completed partitioned query requires a durable job reference and validates the one \
         its response named",
    );
    let descriptor = encode_partition(job_reference, context.execution.result_options)?;
    let schema = result_schema(
        metadata.schema.as_ref(),
        &metadata.statement_type,
        "BigQuery partitioned query",
    )?;
    Ok(QueryPartition {
        descriptor,
        schema: schema.as_ref().clone(),
        rows_affected: reported_rows(metadata, context.options),
    })
}

/// The row count a completed, non-dry query reports: DML's `numDmlAffectedRows` (0 included), else
/// the result's `totalRows`, as PEP 249 defines `cursor.rowcount` for a result set.
///
/// BigQuery sends `totalRows` on every completed response that describes a result, including
/// `maxResults=0` and a script's final `SELECT`, and omits it for DML, DDL and other scripts. For a
/// query with a destination table it counts the whole table (observed, not documented), so that
/// count is unknown.
fn reported_rows(
    metadata: &response::QueryMetadata,
    options: &StatementQueryOptions,
) -> Option<i64> {
    if metadata.num_dml_affected_rows.is_some() {
        return metadata.num_dml_affected_rows;
    }
    if options.destination_table.is_some() {
        return None;
    }
    // A count past `i64::MAX` is reported as unknown.
    metadata
        .total_rows
        .and_then(|total_rows| i64::try_from(total_rows).ok())
}

/// Reads a partition with the result options of the statement that created it, not the reading
/// connection's.
pub(crate) fn read_query_partition(
    execution: &ExecutionContext,
    operation: &Operation,
    partition: &[u8],
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let (reference, result_options) = decode_partition(partition)?;
    let execution = &ExecutionContext {
        result_options,
        ..execution.clone()
    };
    operation.check()?;
    let client = execution.state.rest_job_client(&execution.runtime)?;
    let complete = execution.runtime.block_on(direct::complete_attached_query(
        &execution.runtime,
        client,
        reference,
        Some(operation),
        "reading BigQuery query partition failed",
    ))?;
    reader_from_rest_query(execution, complete, Some(operation.clone()), None)
}

/// Runs a statement the driver issues itself (session control, ingest DDL) for its effect.
pub(crate) fn execute_update(
    runtime: &SharedRuntime,
    state: &ClientState,
    billing_project_id: Option<&ProjectIdentifier>,
    context: QueryContext<'_>,
    sql: &str,
) -> Result<()> {
    direct::complete_context_query(
        runtime,
        state,
        billing_project_id,
        context,
        sql,
        &StatementQueryOptions::default(),
        "BigQuery query execution failed",
    )?;
    Ok(())
}

pub(crate) fn execute_update_with_parameters(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<QueryParameter>)>,
) -> Result<Option<i64>> {
    let complete = direct::complete_statement_query(
        context,
        sql,
        parameters,
        QueryJobCreation::Optional,
        QueryResultMode::MetadataOnly,
        if context.options.dry_run {
            "BigQuery update dry run failed"
        } else {
            "BigQuery query execution failed"
        },
    )?;
    Ok((!context.options.dry_run)
        .then_some(complete.metadata.num_dml_affected_rows)
        .flatten())
}

pub(crate) fn execute_schema(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<QueryParameter>)>,
) -> Result<Schema> {
    let metadata = describe_query(context, sql, parameters, "BigQuery dry run failed")?;
    dry_run_schema(&metadata, "BigQuery dry run").map(|schema| schema.as_ref().clone())
}

pub(crate) fn execute_parameter_schema(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameter_mode: &str,
) -> Result<Schema> {
    let metadata = describe_query(
        context,
        sql,
        Some((parameter_mode, Vec::new())),
        "BigQuery parameter schema dry run failed",
    )?;
    let parameters = metadata
        .undeclared_query_parameters
        .as_deref()
        .ok_or_else(|| {
            error(
                "BigQuery parameter schema dry run completed without query statistics",
                Status::InvalidData,
            )
        })?;
    parameter_schema(parameters)
}

/// Both schema discovery calls describe the query without consuming rows or changing the
/// statement's configured dry-run setting. An empty parameter list leaves placeholders for
/// BigQuery to infer; typed parameters constrain the result schema.
fn describe_query(
    context: StatementQueryContext<'_>,
    sql: &str,
    parameters: Option<(&str, Vec<QueryParameter>)>,
    failure_context: &str,
) -> Result<response::QueryMetadata> {
    let options = StatementQueryOptions {
        dry_run: true,
        ..context.options.clone()
    };
    direct::complete_statement_query(
        StatementQueryContext {
            options: &options,
            ..context
        },
        sql,
        parameters,
        QueryJobCreation::Optional,
        QueryResultMode::MetadataOnly,
        failure_context,
    )
    .map(|complete| complete.metadata)
}

fn dry_run_schema(
    metadata: &response::QueryMetadata,
    context: &str,
) -> Result<arrow_schema::SchemaRef> {
    if metadata.schema.is_none() && metadata.statement_type.is_empty() {
        return Err(error(
            format!("{context} completed without a result schema"),
            Status::InvalidData,
        ));
    }
    result_schema(metadata.schema.as_ref(), &metadata.statement_type, context)
        .map(|schema| schema_with_rest_metadata(schema, metadata))
}

/// Creates a BigQuery session, outside any cancellation, and returns its ID.
pub(crate) fn create_session(
    runtime: &SharedRuntime,
    state: &ClientState,
    billing_project_id: Option<&ProjectIdentifier>,
    location: Option<&str>,
) -> Result<Arc<str>> {
    let options = StatementQueryOptions {
        create_session: true,
        ..StatementQueryOptions::default()
    };
    let complete = direct::complete_context_query(
        runtime,
        state,
        billing_project_id,
        QueryContext {
            defaults: QueryDefaults::default(),
            location,
            session_id: None,
            operation: None,
        },
        "SELECT 1",
        &options,
        "creating BigQuery transaction session failed",
    )?;
    complete
        .metadata
        .session_id
        .as_deref()
        .filter(|session_id| !session_id.is_empty())
        .map(Arc::from)
        .ok_or_else(|| {
            error(
                "BigQuery created a transaction session without returning a session ID",
                Status::InvalidData,
            )
        })
}
