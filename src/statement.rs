//! ADBC statement construction and execution.
//!
//! [`options`] implements ADBC `Optionable` and dispatches statement option keys.

mod binding;
mod operations;
mod options;
mod parameter_stream;
pub(crate) mod sql_text;

use std::sync::Arc;

use adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT;
use adbc_core::error::Result;
use adbc_core::{CancelHandle, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::Schema;

use crate::cancellation::{Cancellation, Operation, OperationCancelHandle, operation_reader};
use crate::connection::{CurrentNamespace, Namespace};
use crate::dbt::statement::{DbtStatementState, SqlDisposition};
use crate::error::{invalid_state, not_implemented};
use crate::ingest::{IngestContext, IngestOptions};
use crate::parameters::{ParameterMode, parameters_for_schema};
use crate::query::{
    ExecutionContext, ExecutionProgress, QueryDefaults, StatementQueryContext,
    StatementQueryOptions, empty_reader, execute_parameter_schema, execute_query,
    execute_query_partition_with_parameters, execute_schema, execute_update_with_parameters,
};
use crate::rest::model::QueryParameter;
use crate::transaction::{TransactionContext, TransactionState};
use crate::{
    BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS, BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE,
    BigQueryStatementOption,
};

use binding::ParameterBinding;
pub(crate) use operations::OptionOnlyOutcome;
use operations::StatementMode;
use parameter_stream::{
    ParameterRows, ParameterizedRecordBatchReader, add_rows_affected, divergent_parameter_schema,
    result_schemas_match,
};
use sql_text::{normalize_sql, require_sql};

struct CachedParameterSchema {
    namespace: Namespace,
    session_id: Option<Arc<str>>,
    schema: Schema,
}

/// An ADBC statement: GoogleSQL text or a bulk ingest, with its bound data and options.
///
/// Created by [`Connection::new_statement`](adbc_core::Connection::new_statement).
pub struct BigQueryStatement {
    /// A snapshot of the connection's context, which statement options shadow. Its current
    /// catalog and schema are refreshed from `namespace` on each use.
    context: ExecutionContext,
    namespace: Arc<CurrentNamespace>,
    query_options: StatementQueryOptions,
    transaction: Arc<TransactionState>,
    cancellation: Arc<Cancellation>,
    sql: Option<Arc<str>>,
    ingest: IngestOptions,
    dbt: DbtStatementState,
    parameter_mode: ParameterMode,
    parameters: ParameterBinding,
    /// The memoized [`Statement::get_parameter_schema`] dry run, with the namespace and session it
    /// resolved names against. `set_option` and `set_sql_query` drop it; a `Mutex` because the
    /// accessor takes `&self`.
    parameter_schema: std::sync::Mutex<Option<CachedParameterSchema>>,
    /// Published through `adbc.statement.exec.progress` and `adbc.statement.exec.max_progress`.
    /// Shared with the export layer, which reads it without taking the statement's state lock.
    progress: Arc<ExecutionProgress>,
    /// Stored and read back but inert: a failed job's error carries its console link regardless.
    /// It cannot be refused, because dbt-core 2.0 sets it on every BigQuery statement it executes;
    /// see `DBT-CORE.md`.
    link_failed_job: bool,
}

impl std::fmt::Debug for BigQueryStatement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("BigQueryStatement");
        debug
            .field("context", &self.context)
            .field("query_options", &self.query_options)
            .field("sql", &self.sql)
            .field("ingest", &self.ingest);
        self.dbt.debug_fields(&mut debug);
        debug
            .field("parameter_mode", &self.parameter_mode)
            .field("parameters", &self.parameters)
            .field("progress", &self.progress.current())
            .field("max_progress", &self.progress.maximum())
            .finish()
    }
}

impl BigQueryStatement {
    pub(crate) fn new(
        context: ExecutionContext,
        namespace: Arc<CurrentNamespace>,
        transaction: Arc<TransactionState>,
    ) -> Self {
        let cancellation =
            Cancellation::new(context.runtime.clone(), context.state.clone(), "statement");
        Self {
            context,
            namespace,
            query_options: StatementQueryOptions::default(),
            transaction,
            cancellation,
            sql: None,
            ingest: IngestOptions::default(),
            dbt: DbtStatementState::default(),
            parameter_mode: ParameterMode::default(),
            parameters: ParameterBinding::default(),
            parameter_schema: std::sync::Mutex::new(None),
            progress: Arc::default(),
            link_failed_job: false,
        }
    }

    /// The cell this statement publishes its execution progress into, for the export layer to
    /// read while an execute holds the statement itself.
    #[cfg(feature = "ffi")]
    pub(crate) fn progress_handle(&self) -> Arc<ExecutionProgress> {
        Arc::clone(&self.progress)
    }

    /// Brings the context up to date with the connection's current catalog and schema. Every
    /// `&mut self` entry point that resolves a name calls this first.
    fn refresh_namespace(&mut self) {
        self.namespace.get().apply_to(&mut self.context);
    }

    /// The context as [`Self::refresh_namespace`] would leave it, for the `&self` entry points.
    fn current_context(&self) -> ExecutionContext {
        let mut context = self.context.clone();
        self.namespace.get().apply_to(&mut context);
        context
    }

    fn execution_session(&self, operation: &Operation) -> Result<Option<Arc<str>>> {
        TransactionContext::new(&self.context, self.query_defaults()).execution_session(
            &self.transaction,
            operation,
            self.query_options.dry_run,
        )
    }

    /// Refuses `bigquery.query.create_session` in manual-commit mode, where every query runs in
    /// the transaction's session. BigQuery rejects a job that names a session and asks for a new
    /// one, but only after the driver has begun the transaction. Query entry points check this
    /// before consuming bound parameters.
    #[inline(never)]
    fn refuse_session_creation(&self) -> Result<()> {
        if self.query_options.create_session && !self.transaction.is_autocommit()? {
            return Err(invalid_state(format!(
                "BigQuery option {} cannot be enabled while {ADBC_CONNECTION_OPTION_AUTOCOMMIT} \
                 is disabled: the statement runs in the transaction's BigQuery session; set {} to \
                 false or re-enable autocommit",
                BigQueryStatementOption::CreateSession.as_str(),
                BigQueryStatementOption::CreateSession.as_str(),
            )));
        }
        Ok(())
    }

    fn query_defaults(&self) -> QueryDefaults<'_> {
        self.context.statement_defaults(&self.query_options)
    }

    fn query_context<'a>(
        &'a self,
        session_id: Option<&'a str>,
        operation: &'a Operation,
    ) -> StatementQueryContext<'a> {
        StatementQueryContext {
            execution: &self.context,
            session_id,
            operation,
            options: &self.query_options,
            transaction: &self.transaction,
            progress: Some(&self.progress),
        }
    }

    /// An unqualified ingest target lands where an unqualified name in this statement's SQL
    /// resolves, so the statement's default project and dataset shadow the connection's, as in
    /// the Go driver.
    fn ingest_context<'a>(&'a self, operation: &'a Operation) -> IngestContext<'a> {
        let defaults = self.query_defaults();
        IngestContext {
            runtime: &self.context.runtime,
            state: &self.context.state,
            billing_project_id: self.context.billing_project_id.as_ref(),
            current_catalog: defaults.catalog,
            current_schema: defaults.db_schema,
            location: self.context.location.as_deref(),
            operation,
        }
    }

    /// Runs the statement and returns the result stream with the affected-row count
    /// `AdbcStatementExecuteQuery` reports: a query's result size, DML's affected rows, or -1 when
    /// unknown (DDL, a dry run, a destination-table query, or parameter rows still to run).
    ///
    /// An option-only statement (bulk ingest, table copy, python model) returns an empty-schema
    /// stream rather than a refusal: adbc.h asks such a caller to pass `NULL` for `out`, but the
    /// Rust `ManagedStatement::execute` never does.
    pub(crate) fn execute_with_rows_affected(
        &mut self,
    ) -> Result<(Box<dyn RecordBatchReader + Send + 'static>, i64)> {
        const UNKNOWN_ROWS_AFFECTED: i64 = -1;
        self.progress.reset();
        self.refresh_namespace();
        if let Some(operation) = self.active_option_only_operation() {
            return self
                .execute_option_only_operation(operation)
                .map(OptionOnlyOutcome::into_result);
        }
        let sql = require_sql(self.sql.as_ref(), "execute")?;
        self.parameters.require_available()?;
        self.refuse_session_creation()?;
        if let Some(parameters) = self.dry_run_parameters()? {
            let operation = self.cancellation.begin();
            let session_id = self.execution_session(&operation)?;
            let (reader, _) = execute_query(
                self.query_context(session_id.as_deref(), &operation),
                sql,
                Some(parameters),
            )
            .map_err(|failure| self.parameter_mode.explain_refusal(failure))?;
            return Ok((operation_reader(reader, operation), UNKNOWN_ROWS_AFFECTED));
        }
        let Some(parameters) = self.parameters.take() else {
            let operation = self.cancellation.begin();
            let session_id = self.execution_session(&operation)?;
            // The query completes before this returns, and the completing response carries
            // `totalRows` for a result and `numDmlAffectedRows` for DML.
            let (reader, rows_affected) = execute_query(
                self.query_context(session_id.as_deref(), &operation),
                sql,
                None,
            )?;
            // Draining still issues requests (`jobs.getQueryResults` pages, Storage Read
            // `ReadRows`), so the reader takes prompt cancellation: a cancelled drain must
            // report itself, not end cleanly over a partial result.
            return Ok((
                operation_reader(reader, operation),
                rows_affected.unwrap_or(UNKNOWN_ROWS_AFFECTED),
            ));
        };
        let operation = self.cancellation.begin();
        let mut rows = ParameterRows::new(parameters, self.parameter_mode)?;
        let Some(parameters) = rows.next(&operation)? else {
            // No parameter row means nothing executed, so the count is known: none, as
            // `execute_update` and `execute_partitions` report it.
            let schema = self.empty_parameter_schema(&rows, &operation, sql)?;
            operation.finish();
            return Ok((empty_reader(Arc::new(schema)), 0));
        };

        let session_id = self.execution_session(&operation)?;
        let (current, rows_affected) = execute_query(
            self.query_context(session_id.as_deref(), &operation),
            sql,
            Some((self.parameter_mode.as_bigquery_value(), parameters)),
        )
        .map_err(|failure| self.parameter_mode.explain_refusal(failure))?;
        // A one-row binding, such as a DB-API `execute(sql, parameters)`, ran in full, so its
        // count is the statement's.
        if !rows.has_next(&operation)? {
            return Ok((
                operation_reader(current, operation),
                rows_affected.unwrap_or(UNKNOWN_ROWS_AFFECTED),
            ));
        }
        let schema = current.schema();
        // DML, DDL and destination writes return no rows: run every binding before returning,
        // because callers need not drain an empty stream.
        if self.query_options.destination_table.is_some() || !returns_rows(&schema) {
            let mut total = rows_affected;
            rows.for_each(&operation, |parameters| {
                let session_id = self.execution_session(&operation)?;
                let (reader, affected) = execute_query(
                    self.query_context(session_id.as_deref(), &operation),
                    sql,
                    Some((self.parameter_mode.as_bigquery_value(), parameters)),
                )
                .map_err(|failure| self.parameter_mode.explain_refusal(failure))?;
                if !result_schemas_match(&schema, &reader.schema()) {
                    return Err(divergent_parameter_schema(&schema, &reader.schema()));
                }
                total = add_rows_affected(total, affected)?;
                Ok(())
            })?;
            return Ok((
                operation_reader(current, operation),
                total.unwrap_or(UNKNOWN_ROWS_AFFECTED),
            ));
        }
        // Later parameter rows execute while the reader is drained, after this call returns, so
        // the reader owns its SQL; progress stays the first job's.
        let reader = Box::new(ParameterizedRecordBatchReader {
            context: self.context.clone(),
            query_options: self.query_options.clone(),
            transaction: Arc::clone(&self.transaction),
            sql: sql.clone(),
            parameter_mode: self.parameter_mode,
            rows,
            current,
            schema,
            operation: operation.clone(),
        });
        // Later rows have not run yet, so the total is unknown even for DML.
        Ok((operation_reader(reader, operation), UNKNOWN_ROWS_AFFECTED))
    }

    /// The parameters a dry run sends for a bound stream: typed nulls from its schema, so no row
    /// is read and the binding stays for the execution the dry run validates, as an ingest dry
    /// run keeps its data. `None` outside a dry run or without a binding.
    fn dry_run_parameters(&self) -> Result<Option<(&'static str, Vec<QueryParameter>)>> {
        if !self.query_options.dry_run {
            return Ok(None);
        }
        let Some(schema) = self.parameters.schema() else {
            return Ok(None);
        };
        let parameters = parameters_for_schema(&schema, self.parameter_mode)?;
        Ok(Some((self.parameter_mode.as_bigquery_value(), parameters)))
    }

    /// Drops the memoized parameter schema, because what the dry run would report has changed.
    fn forget_parameter_schema(&mut self) {
        *self
            .parameter_schema
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// The result schema of a statement whose bound parameter stream holds no rows: nothing
    /// executes, so a dry run with parameters typed from the stream's schema describes it.
    fn empty_parameter_schema(
        &self,
        rows: &ParameterRows,
        operation: &Operation,
        sql: &Arc<str>,
    ) -> Result<Schema> {
        let session_id = self.transaction.session_id()?;
        let parameters = parameters_for_schema(rows.schema().as_ref(), self.parameter_mode)?;
        execute_schema(
            self.query_context(session_id.as_deref(), operation),
            sql,
            Some((self.parameter_mode.as_bigquery_value(), parameters)),
        )
        .map_err(|failure| self.parameter_mode.explain_refusal(failure))
    }
}

impl Statement for BigQueryStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        self.bind_stream(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )))
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.parameters = ParameterBinding::Bound(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // The C ABI entry point calls `execute_with_rows_affected` to report the count.
        self.execute_with_rows_affected().map(|(reader, _)| reader)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.progress.reset();
        self.refresh_namespace();
        if let Some(operation) = self.active_option_only_operation() {
            return self
                .execute_option_only_operation(operation)
                .map(|outcome| outcome.rows_affected);
        }
        let sql = require_sql(self.sql.as_ref(), "execute")?;
        self.parameters.require_available()?;
        self.refuse_session_creation()?;
        if let Some(parameters) = self.dry_run_parameters()? {
            return self.cancellation.run(|operation| {
                let session_id = self.execution_session(operation)?;
                execute_update_with_parameters(
                    self.query_context(session_id.as_deref(), operation),
                    sql,
                    Some(parameters),
                )
                .map_err(|failure| self.parameter_mode.explain_refusal(failure))
            });
        }
        let parameters = self.parameters.take();
        self.cancellation.run(|operation| {
            let Some(parameters) = parameters else {
                let session_id = self.execution_session(operation)?;
                return execute_update_with_parameters(
                    self.query_context(session_id.as_deref(), operation),
                    sql,
                    None,
                );
            };
            // An empty binding runs nothing, so it affects no row and opens no transaction.
            let mut total = Some(0_i64);
            ParameterRows::new(parameters, self.parameter_mode)?.for_each(
                operation,
                |parameters| {
                    let session_id = self.execution_session(operation)?;
                    let affected = execute_update_with_parameters(
                        self.query_context(session_id.as_deref(), operation),
                        sql,
                        Some((self.parameter_mode.as_bigquery_value(), parameters)),
                    )
                    .map_err(|failure| self.parameter_mode.explain_refusal(failure))?;
                    total = add_rows_affected(total, affected)?;
                    Ok(())
                },
            )?;
            Ok(total)
        })
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // Describing executes nothing, so the last execute's progress no longer applies.
        self.progress.reset();
        self.refresh_namespace();
        if let Some(operation) = self.active_option_only_operation() {
            let schema = operation.result_schema().ok_or_else(|| {
                invalid_state(format!(
                    "a BigQuery {} statement returns no rows, so it has no result schema; clear \
                     {} to describe a SQL query instead",
                    operation.subject(),
                    operation.selecting_options(),
                ))
            })?;
            self.cancellation.begin().finish();
            return Ok(schema);
        }
        let sql = require_sql(self.sql.as_ref(), "describe")?;
        self.refuse_session_creation()?;
        let session_id = self.transaction.session_id()?;
        // Describing needs no values: an unbound statement's placeholders, including those of a
        // binding an earlier execute consumed, are left undeclared for the dry run to type, as
        // `get_parameter_schema` does. Without the parameter mode BigQuery refuses `?`.
        let parameters = match self.parameters.schema() {
            Some(schema) => parameters_for_schema(&schema, self.parameter_mode)?,
            None => Vec::new(),
        };
        self.cancellation.run(|operation| {
            execute_schema(
                self.query_context(session_id.as_deref(), operation),
                sql,
                Some((self.parameter_mode.as_bigquery_value(), parameters)),
            )
            .map_err(|failure| self.parameter_mode.explain_refusal(failure))
        })
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        self.progress.reset();
        self.refresh_namespace();
        if let Some(operation) = self.active_option_only_operation() {
            // Refused rather than run: its rows have no job for a partition to read back.
            if operation.result_schema().is_some() {
                return Err(invalid_state(format!(
                    "a BigQuery {} creates no job to partition; clear {} or use \
                     AdbcStatementExecuteQuery",
                    operation.subject(),
                    operation.selecting_options(),
                )));
            }
            let outcome = self.execute_option_only_operation(operation)?;
            return Ok(PartitionedResult {
                partitions: Vec::new(),
                schema: outcome.schema(),
                rows_affected: outcome.rows_affected.unwrap_or(-1),
            });
        }
        let sql = require_sql(self.sql.as_ref(), "execute")?;
        // Reject before reading parameters, including an empty stream's schema dry run.
        if self.query_options.dry_run {
            return Err(invalid_state(format!(
                "BigQuery option {} is set, and a dry run creates no job to partition; unset it, or \
                 validate the query with AdbcStatementExecuteSchema",
                BigQueryStatementOption::DryRun.as_str()
            )));
        }
        self.parameters.require_available()?;
        self.refuse_session_creation()?;
        let parameters = self.parameters.take();
        self.cancellation.run(|operation| {
            let Some(parameters) = parameters else {
                let session_id = self.execution_session(operation)?;
                let partition = execute_query_partition_with_parameters(
                    self.query_context(session_id.as_deref(), operation),
                    sql,
                    None,
                )?;
                return Ok(PartitionedResult {
                    partitions: vec![partition.descriptor],
                    schema: partition.schema,
                    rows_affected: partition.rows_affected.unwrap_or(-1),
                });
            };

            let mut rows = ParameterRows::new(parameters, self.parameter_mode)?;
            let mut partitions = Vec::new();
            let mut schema: Option<Schema> = None;
            let mut rows_affected = Some(0_i64);
            rows.for_each(operation, |parameters| {
                let session_id = self.execution_session(operation)?;
                let partition = execute_query_partition_with_parameters(
                    self.query_context(session_id.as_deref(), operation),
                    sql,
                    Some((self.parameter_mode.as_bigquery_value(), parameters)),
                )
                .map_err(|failure| self.parameter_mode.explain_refusal(failure))?;
                match &schema {
                    Some(first) if !result_schemas_match(first, &partition.schema) => {
                        return Err(divergent_parameter_schema(first, &partition.schema));
                    }
                    Some(_) => {}
                    None => schema = Some(partition.schema),
                }
                partitions.push(partition.descriptor);
                rows_affected = add_rows_affected(rows_affected, partition.rows_affected)?;
                Ok(())
            })?;
            // An empty binding runs no job, so a dry run typed from the binding's schema describes
            // the result.
            let schema = match schema {
                Some(schema) => schema,
                None => self.empty_parameter_schema(&rows, operation, sql)?,
            };
            Ok(PartitionedResult {
                partitions,
                schema,
                rows_affected: rows_affected.unwrap_or(-1),
            })
        })
    }

    /// The schema of the statement's undeclared query parameters, from a dry run. A driver
    /// manager may ask before every execute, so the answer is memoized until the next
    /// `set_option` or `set_sql_query`, or until the connection's current catalog or schema
    /// or BigQuery session changes.
    fn get_parameter_schema(&self) -> Result<Schema> {
        let namespace = self.namespace.get();
        self.refuse_session_creation()?;
        let session_id = self.transaction.session_id()?;
        // The memo is plain data a panic cannot leave torn, so a poisoned guard is recovered.
        if let Some(cached) = &*self
            .parameter_schema
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            && cached.namespace == namespace
            && cached.session_id == session_id
        {
            return Ok(cached.schema.clone());
        }
        let sql = require_sql(self.sql.as_ref(), "describe the parameters of")?;
        let mut execution = self.context.clone();
        namespace.clone().apply_to(&mut execution);
        // Concurrent, not superseding: adbc.h lets only ExecuteQuery, ExecuteSchema and Prepare
        // invalidate prior result sets, and a parameterized execute's lazy reader may still be
        // draining.
        let operation = self.cancellation.begin_concurrent();
        let context = StatementQueryContext {
            execution: &execution,
            ..self.query_context(session_id.as_deref(), &operation)
        };
        let result =
            execute_parameter_schema(context, sql, self.parameter_mode.as_bigquery_value())
                .map_err(|failure| self.parameter_mode.explain_refusal(failure));
        operation.finish();
        let schema = result?;
        *self
            .parameter_schema
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(CachedParameterSchema {
            namespace,
            session_id,
            schema: schema.clone(),
        });
        Ok(schema)
    }

    /// BigQuery has no server-side prepare, so this only validates that a query is set, as the
    /// Go driver does. The dry run [`Self::get_parameter_schema`] needs is left to that call, so
    /// prepare-then-execute costs no extra round trip.
    fn prepare(&mut self) -> Result<()> {
        require_sql(self.sql.as_ref(), "prepare")?;
        // adbc.h: preparing a statement "invalidates any prior result sets", exactly as executing
        // one does, so it supersedes whatever is still running on this statement.
        self.cancellation.begin().finish();
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        let query = normalize_sql(query.as_ref());
        self.forget_parameter_schema();
        // A previous execute's consumed binding does not apply to new SQL.
        self.parameters.reset_consumed();
        match self.dbt.sql_disposition(query) {
            SqlDisposition::Drop => self.sql = None,
            SqlDisposition::Store => self.sql = Some(Arc::from(query)),
            SqlDisposition::Replace => {
                self.enter_mode(StatementMode::SqlQuery);
                self.sql = Some(Arc::from(query));
            }
        }
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(not_implemented(
            "BigQuery Substrait plans are not implemented; set the statement's GoogleSQL text \
             with AdbcStatementSetSqlQuery instead",
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(OperationCancelHandle(self.cancellation.clone()))
    }
}

/// Whether a query, script or procedure returns rows. DML and DDL return no rows, though BigQuery
/// reports DML with its target table's schema.
///
/// The statement type is missing when a job outlasted the `jobs.query` wait
/// (`jobs.getQueryResults` never carries it) and the follow-up `jobs.get` failed; a DML count
/// still identifies DML then.
fn returns_rows(schema: &Schema) -> bool {
    let metadata = schema.metadata();
    !schema.fields().is_empty()
        && match metadata.get(BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE) {
            Some(statement_type) => matches!(statement_type.as_str(), "SELECT" | "SCRIPT" | "CALL"),
            None => !metadata.contains_key(BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS),
        }
}

#[cfg(test)]
mod tests;
