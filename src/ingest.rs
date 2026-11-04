//! BigQuery bulk ingestion: validate the target and schema, prepare the table, then write and
//! commit rows through the Storage Write API.

use std::sync::Arc;

use adbc_core::constants::{
    ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
};
use adbc_core::error::{Result, Status};
use adbc_core::options::IngestMode;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use crate::BigQueryBulkIngestMethod;
use crate::cancellation::Operation;
use crate::clients::ClientState;
use crate::error::{error, invalid_argument, not_implemented};
use crate::identifiers::{ProjectIdentifier, write_quoted_identifier};
use crate::query::{QueryContext, QueryDefaults, execute_update};
use crate::rest::metadata::{MetadataClient, MetadataError, Table, TableId, TableView};
use crate::rest::model::TableSchema;
use crate::rest::rejection::service_error;
use crate::runtime::SharedRuntime;

mod align;
pub(crate) mod normalize;
pub(crate) mod schema;
mod target;
mod write;

use align::{align_with_table, has_null_field};
use schema::{
    arrow_schema_to_bigquery, create_or_replace_table_ddl, normalize_schema,
    reject_repeated_timestamps, schemaless_target, schemas_match, storage_write_schema,
    validate_existing_schema, validate_ingestible_target,
};
use target::resolve_target;
use write::{FirstBatch, read_batch, write_batches};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IngestOptions {
    pub(crate) method: BigQueryBulkIngestMethod,
    pub(crate) mode: IngestMode,
    pub(crate) target_catalog: Option<ProjectIdentifier>,
    pub(crate) target_db_schema: Option<String>,
    pub(crate) target_table: Option<String>,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            method: BigQueryBulkIngestMethod::default(),
            mode: IngestMode::Create,
            target_catalog: None,
            target_db_schema: None,
            target_table: None,
        }
    }
}

pub(crate) fn parse_ingest_method(key: &str, value: &str) -> Result<BigQueryBulkIngestMethod> {
    if let Some(method) = BigQueryBulkIngestMethod::from_name(value) {
        return Ok(method);
    }
    // Match unsupported `load` case-insensitively, like supported enum values.
    if value.eq_ignore_ascii_case("load") {
        return Err(not_implemented(
            "BigQuery load-job bulk ingestion is not implemented; use storage_write",
        ));
    }
    Err(invalid_argument(format!(
        "BigQuery option {key} must be one of {}",
        crate::options::valid_enum_values::<BigQueryBulkIngestMethod>(),
    )))
}

#[derive(Clone, Copy)]
pub(crate) struct IngestContext<'a> {
    pub(crate) runtime: &'a SharedRuntime,
    pub(crate) state: &'a Arc<ClientState>,
    pub(crate) billing_project_id: Option<&'a ProjectIdentifier>,
    /// The project an unqualified target lands in: the statement's default project, else the
    /// connection's current catalog, else the billing project.
    pub(crate) current_catalog: Option<&'a ProjectIdentifier>,
    /// The dataset an unqualified target lands in: the statement's default dataset, else the
    /// connection's current schema.
    pub(crate) current_schema: Option<&'a str>,
    /// The database location: the one execution setting ingest DDL carries (see
    /// [`run_ingest_ddl`]).
    pub(crate) location: Option<&'a str>,
    pub(crate) operation: &'a Operation,
}

pub(crate) fn execute_ingest(
    context: IngestContext<'_>,
    options: &IngestOptions,
    reader: Box<dyn RecordBatchReader + Send>,
) -> Result<i64> {
    match options.method {
        BigQueryBulkIngestMethod::StorageWrite => {
            execute_storage_write_ingest(context, options, reader)
        }
    }
}

/// Checks the target, schema and ingest mode for `bigquery.query.dry_run`.
/// Reads table metadata but preserves the bound stream, so row values are not validated.
pub(crate) fn validate_ingest(
    context: IngestContext<'_>,
    options: &IngestOptions,
    schema: &Schema,
) -> Result<()> {
    match options.method {
        BigQueryBulkIngestMethod::StorageWrite => {
            context.operation.check()?;
            let table = resolve_target(context.current_catalog, context.current_schema, options)?;
            let metadata_client = context.state.metadata_client(context.runtime)?;
            ingest_schema(&context, &metadata_client, table, options.mode, schema)?
                .plan_target(&context, &metadata_client, table, options.mode, true)
                .map(|_| ())
        }
    }
}

fn execute_storage_write_ingest(
    context: IngestContext<'_>,
    options: &IngestOptions,
    mut reader: Box<dyn RecordBatchReader + Send>,
) -> Result<i64> {
    context.operation.check()?;
    let table = resolve_target(context.current_catalog, context.current_schema, options)?;
    let input_schema = reader.schema();
    let metadata_client = context.state.metadata_client(context.runtime)?;
    let settled = ingest_schema(
        &context,
        &metadata_client,
        table,
        options.mode,
        &input_schema,
    )?;
    let normalized_schema = Arc::new(storage_write_schema(&settled.normalized_schema));
    let first = loop {
        context.operation.check()?;
        match reader.next() {
            Some(batch) => {
                let batch = read_batch(batch, &input_schema)?;
                if batch.num_rows() != 0 {
                    // Validate the first data before CREATE or REPLACE can mutate the target.
                    break Some(FirstBatch::new(
                        normalized_schema,
                        batch,
                        context.operation,
                    )?);
                }
            }
            None => break None,
        }
    };
    let plan = settled.plan_target(&context, &metadata_client, table, options.mode, false)?;
    let table_schema = settled.table_schema;
    let plan = prepare_table(
        &context,
        &metadata_client,
        table,
        options.mode,
        &table_schema,
        plan,
    )?;
    let table_path = table.storage_path();

    // New rows become visible only at commit; Recreate has already removed the old rows.
    let staged: Result<_> = (|| {
        context.operation.check()?;
        let Some(first) = first else {
            if plan == TablePlan::Truncate {
                truncate_table(&context, table, Some(context.operation))?;
            }
            return Ok(None);
        };
        let write_client = context.state.write_client(context.runtime)?;
        let stream = context.runtime.block_on(write_batches(
            write_client,
            &table_path,
            input_schema,
            first,
            reader,
            context.operation,
        ))?;
        // Check cancellation before Truncate removes the old rows.
        context.operation.check()?;
        Ok(Some(stream))
    })();
    let stream = match staged {
        Ok(Some(stream)) => stream,
        Ok(None) => return Ok(0),
        Err(mut failure) if options.mode == IngestMode::Create => {
            // Even an empty-table check followed by DROP can race a concurrent writer's commit.
            // Keep the table; this ingest has not committed any rows, so retrying append is safe.
            failure.message = format!(
                "{}; the ingest committed no rows but kept the table it created, {table}; retry \
                 with {ADBC_INGEST_OPTION_MODE}={ADBC_INGEST_OPTION_MODE_APPEND:?}, or drop the \
                 table and retry",
                failure.message
            );
            return Err(failure);
        }
        Err(failure) if plan == TablePlan::Recreate => return Err(recreated(failure, table)),
        Err(failure) => return Err(failure),
    };

    if plan == TablePlan::Truncate {
        // The target is emptied only once every row is on the finalized stream (which TRUNCATE
        // leaves intact), so a replace that fails while uploading keeps the old rows. From here
        // the ingest runs to its commit uncancellable: a cancel interrupting a TRUNCATE that had
        // landed would leave the table empty with the new rows uncommitted.
        truncate_table(&context, table, None)?;
    }
    context
        .runtime
        .block_on(stream.commit(&table_path))
        .map_err(|mut failure| match plan {
            TablePlan::Truncate => {
                failure.message = format!(
                    "{}; this replace had already emptied {table}",
                    failure.message
                );
                failure
            }
            TablePlan::Recreate => recreated(failure, table),
            TablePlan::Reuse | TablePlan::Create => failure,
        })
}

/// Tells the caller of a `replace` that failed after recreating its target that the old table's
/// rows are gone, since nothing else in the failure would.
fn recreated(mut failure: adbc_core::error::Error, table: TableId<'_>) -> adbc_core::error::Error {
    failure.message = format!(
        "{}; this replace had already recreated {table} with the bound schema, so its previous \
         rows are gone",
        failure.message
    );
    failure
}

/// Table action derived from read-only metadata and schema comparison.
/// Create-only execution skips this lookup and lets the insert check existence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TablePlan {
    /// The target exists and its schema already accepts the bound data.
    Reuse,
    /// The target does not exist and must be created.
    Create,
    /// A `replace` over a table whose schema matches: empty it in place, once the rows are staged.
    Truncate,
    /// A `replace` over a table with a different schema: use `CREATE OR REPLACE TABLE`.
    Recreate,
}

/// The schemas an ingest writes under, and its table action if schema resolution planned it.
struct IngestSchema {
    /// What every batch is normalized to.
    normalized_schema: Schema,
    /// The BigQuery schema the rows are written under.
    table_schema: TableSchema,
    /// Appends inspect and validate the target before reading rows. Other modes defer planning
    /// until the first batch is valid, or until a dry run has checked the schema.
    plan: Option<TablePlan>,
}

impl IngestSchema {
    /// Decides what the ingest does with its target, reading it unless settling the schema
    /// already did. A `create` reads it only to `inspect` it (a dry run), since its insert
    /// learns of an existing table atomically.
    fn plan_target(
        &self,
        context: &IngestContext<'_>,
        metadata_client: &MetadataClient,
        table: TableId<'_>,
        mode: IngestMode,
        inspect: bool,
    ) -> Result<TablePlan> {
        match self.plan {
            Some(plan) => Ok(plan),
            None if mode == IngestMode::Create && !inspect => Ok(TablePlan::Create),
            None => plan_table(context, metadata_client, table, mode, &self.table_schema),
        }
    }
}

/// Converts the bound schema for an ingest in `mode`.
///
/// Every check the bound schema allows on its own runs before any request. An append then reads
/// its target, which settles what the bound schema leaves open (see [`align`]); a `Null` field
/// waits for that. Other modes read the target, if at all, only once the first batch is known to
/// be valid.
fn ingest_schema(
    context: &IngestContext<'_>,
    metadata_client: &MetadataClient,
    table: TableId<'_>,
    mode: IngestMode,
    schema: &Schema,
) -> Result<IngestSchema> {
    let mut normalized_schema = normalize_schema(schema)?;
    let appends = matches!(mode, IngestMode::Append | IngestMode::CreateAppend);
    if !appends || !has_null_field(normalized_schema.fields()) {
        let table_schema = checked_table_schema(&normalized_schema)?;
        if !appends {
            return Ok(IngestSchema {
                normalized_schema,
                table_schema,
                plan: None,
            });
        }
    }
    let existing = fetch_target(context, metadata_client, table)?;
    if let Some(actual) = existing.as_ref().and_then(|table| table.schema.as_ref()) {
        normalized_schema = align_with_table(&normalized_schema, actual)?;
    }
    // A target that cannot take the append at all is the error to report, not the `Null` field
    // it left untyped.
    if has_null_field(normalized_schema.fields()) {
        match &existing {
            None if mode == IngestMode::Append => return Err(missing_target(table)),
            Some(found) => {
                validate_ingestible_target(table, &found.table_type)?;
                if found.schema.is_none() {
                    return Err(schemaless_target(table));
                }
            }
            None => {}
        }
    }
    let table_schema = checked_table_schema(&normalized_schema)?;
    let plan = decide_plan(table, mode, existing, &table_schema)?;
    Ok(IngestSchema {
        table_schema,
        normalized_schema,
        plan: Some(plan),
    })
}

/// The BigQuery schema a normalized schema is written under, checked for what Storage Write
/// cannot append.
fn checked_table_schema(normalized_schema: &Schema) -> Result<TableSchema> {
    let table_schema = arrow_schema_to_bigquery(normalized_schema)?;
    reject_repeated_timestamps(&table_schema)?;
    Ok(table_schema)
}

/// Reads the target's metadata; `None` if it does not exist.
fn fetch_target(
    context: &IngestContext<'_>,
    metadata_client: &MetadataClient,
    table: TableId<'_>,
) -> Result<Option<Table>> {
    // Basic includes the object type, needed to reject views and external tables.
    match context.runtime.block_on(metadata_client.get_table(
        Some(context.operation),
        table,
        TableView::Basic,
    )) {
        Ok(existing) => Ok(Some(existing)),
        Err(source) if source.is_not_found() => Ok(None),
        Err(source) => {
            Err(source.into_error(&format!("failed to inspect BigQuery ingest target {table}")))
        }
    }
}

/// Reads the target and decides what an ingest in `mode` does with it.
fn plan_table(
    context: &IngestContext<'_>,
    metadata_client: &MetadataClient,
    table: TableId<'_>,
    mode: IngestMode,
    expected_schema: &TableSchema,
) -> Result<TablePlan> {
    let existing = fetch_target(context, metadata_client, table)?;
    decide_plan(table, mode, existing, expected_schema)
}

fn decide_plan(
    table: TableId<'_>,
    mode: IngestMode,
    existing: Option<Table>,
    expected_schema: &TableSchema,
) -> Result<TablePlan> {
    match (mode, existing) {
        (IngestMode::Create, Some(_)) => Err(error(already_exists(table), Status::AlreadyExists)),
        (IngestMode::Append, None) => Err(missing_target(table)),
        (IngestMode::Append | IngestMode::CreateAppend, Some(existing)) => {
            validate_ingestible_target(table, &existing.table_type)?;
            validate_existing_schema(table, existing.schema.as_ref(), expected_schema)
                .map(|()| TablePlan::Reuse)
        }
        (IngestMode::Replace, Some(existing)) => {
            // Reject non-table objects before issuing CREATE OR REPLACE TABLE.
            validate_ingestible_target(table, &existing.table_type)?;
            Ok(
                if existing
                    .schema
                    .as_ref()
                    .is_some_and(|actual| schemas_match(actual, expected_schema))
                {
                    TablePlan::Truncate
                } else {
                    TablePlan::Recreate
                },
            )
        }
        (IngestMode::Create | IngestMode::Replace | IngestMode::CreateAppend, None) => {
            Ok(TablePlan::Create)
        }
    }
}

/// Creates or recreates the target as `plan` says, and returns the plan the target ended up
/// with. A `Truncate` is left to the caller, which runs it only once the rows are ready to
/// commit.
fn prepare_table(
    context: &IngestContext<'_>,
    metadata_client: &MetadataClient,
    table: TableId<'_>,
    mode: IngestMode,
    expected_schema: &TableSchema,
    plan: TablePlan,
) -> Result<TablePlan> {
    match plan {
        TablePlan::Reuse | TablePlan::Truncate => {}
        TablePlan::Create => {
            if !create_table(context, metadata_client, table, mode, expected_schema)? {
                // Another writer created the target between the read above and the insert.
                // Every mode but `create` writes into a table that exists, so plan once more
                // against the one now there.
                return match plan_table(context, metadata_client, table, mode, expected_schema)? {
                    TablePlan::Create => Err(error(
                        format!(
                            "BigQuery ingest target {table} was created by another writer and \
                             then removed while this ingest prepared it; retry the ingest"
                        ),
                        Status::IO,
                    )),
                    TablePlan::Recreate => recreate_table(context, table, expected_schema)
                        .map(|()| TablePlan::Recreate),
                    plan => Ok(plan),
                };
            }
        }
        TablePlan::Recreate => recreate_table(context, table, expected_schema)?,
    }
    Ok(plan)
}

/// The table name as a GoogleSQL statement has to spell it: one backtick-quoted
/// `project.dataset.table`.
fn quoted_table_name(table: TableId<'_>) -> String {
    let mut quoted = String::new();
    write_quoted_identifier(
        &mut quoted,
        &[table.project_id, table.dataset_id, table.table_id],
    );
    quoted
}

fn truncate_table(
    context: &IngestContext<'_>,
    table: TableId<'_>,
    operation: Option<&Operation>,
) -> Result<()> {
    run_ingest_ddl(
        context,
        operation,
        &format!("TRUNCATE TABLE {}", quoted_table_name(table)),
        table,
        DdlEffect::Empty,
    )
}

/// Atomically replaces a target whose schema differs, avoiding a delete/create gap.
fn recreate_table(
    context: &IngestContext<'_>,
    table: TableId<'_>,
    expected_schema: &TableSchema,
) -> Result<()> {
    let ddl = create_or_replace_table_ddl(&quoted_table_name(table), expected_schema)?;
    run_ingest_ddl(
        context,
        Some(context.operation),
        &ddl,
        table,
        DdlEffect::Replace,
    )
}

/// What an ingest statement does to its target's rows.
#[derive(Clone, Copy)]
enum DdlEffect {
    Empty,
    Replace,
}

/// Runs ingest DDL with only the configured location. User query options such as
/// destination tables, batch priority and session creation do not apply to these statements.
///
/// A cancel that arrives once the statement may have been sent cannot say the target is
/// untouched: BigQuery can finish a statement it was asked to cancel. One that arrived before
/// changed nothing, and says nothing more.
fn run_ingest_ddl(
    context: &IngestContext<'_>,
    operation: Option<&Operation>,
    statement: &str,
    table: TableId<'_>,
    effect: DdlEffect,
) -> Result<()> {
    let (verb, done) = match effect {
        DdlEffect::Empty => ("empty", "emptied"),
        DdlEffect::Replace => ("replace", "replaced"),
    };
    let labelled = |mut source: adbc_core::error::Error| {
        source.message = format!(
            "failed to {verb} BigQuery ingest target {table}: {}",
            source.message
        );
        source
    };
    if let Some(operation) = operation {
        operation.check().map_err(labelled)?;
    }
    execute_update(
        context.runtime,
        context.state,
        context.billing_project_id,
        QueryContext {
            defaults: QueryDefaults::default(),
            location: context.location,
            session_id: None,
            operation,
        },
        statement,
    )
    .map_err(|source| {
        let mut failure = labelled(source);
        if failure.status == Status::Cancelled {
            failure.message = format!(
                "{}; BigQuery may have {done} {table} before the cancel took effect, in which \
                 case its previous rows are gone",
                failure.message
            );
        }
        failure
    })
}

/// Creates the target, answering `false` when it already exists under a mode other than
/// `create`, all of which can write into it instead.
fn create_table(
    context: &IngestContext<'_>,
    metadata_client: &MetadataClient,
    table: TableId<'_>,
    mode: IngestMode,
    schema: &TableSchema,
) -> Result<bool> {
    match context.runtime.block_on(metadata_client.insert_table(
        Some(context.operation),
        table,
        schema,
    )) {
        Ok(()) => Ok(true),
        Err(MetadataError::Rejected(status, body)) if status == reqwest::StatusCode::CONFLICT => {
            if mode != IngestMode::Create {
                return Ok(false);
            }
            // Create mode skips the preflight read, so the insert's conflict is where it learns
            // the table exists. The rendered rejection is kept only for its reason and HTTP
            // details.
            let mut failure = service_error("", status, &body);
            failure.message = format!("{} (HTTP 409)", already_exists(table));
            failure.status = Status::AlreadyExists;
            Err(failure)
        }
        Err(source) => {
            Err(source.into_error(&format!("failed to create BigQuery ingest target {table}")))
        }
    }
}

fn missing_target(table: TableId<'_>) -> adbc_core::error::Error {
    error(
        format!("BigQuery ingest target {table} does not exist"),
        Status::NotFound,
    )
}

fn already_exists(table: TableId<'_>) -> String {
    format!(
        "BigQuery ingest target {table} already exists; set {ADBC_INGEST_OPTION_MODE} to \
         {ADBC_INGEST_OPTION_MODE_APPEND:?} or {ADBC_INGEST_OPTION_MODE_REPLACE:?}"
    )
}

#[cfg(test)]
mod tests;
