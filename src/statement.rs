//! ADBC statement construction and execution.
//!
//! [`options`] implements ADBC `Optionable` and dispatches statement option keys.

mod binding;
mod operations;
mod options;
mod parameter_stream;
pub(crate) mod sql_text;

use std::cell::RefCell;
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
use crate::types::validate_schema_nesting;
use crate::{
    BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS, BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE,
    BigQueryStatementOption,
};

use binding::ParameterBinding;
pub(crate) use operations::OptionOnlyOutcome;
use operations::StatementMode;
use parameter_stream::{
    ParameterRows, ParameterizedRecordBatchReader, add_rows_affected, require_same_result_schema,
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
    /// resolved names against. `set_option` and `set_sql_query` drop it; a cell because the
    /// accessor takes `&self`.
    parameter_schema: RefCell<Option<CachedParameterSchema>>,
    /// Published through `adbc.statement.exec.progress` and `adbc.statement.exec.max_progress`.
    /// Shared with the export layer, which reads it without taking the statement's state lock.
    progress: Arc<ExecutionProgress>,
    /// Stored and read back but inert: a failed job's error carries its console link regardless.
    /// It cannot be refused, because dbt 2.0 sets it on every BigQuery statement it executes;
    /// see `DBT.md`.
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
            parameter_schema: RefCell::new(None),
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

    /// Refuses `bigquery.query.create_session` in manual-commit mode, where every query runs in
    /// the transaction's session. BigQuery rejects a job that names a session and asks for a new
    /// one, but only after the driver has begun the transaction. Query entry points check this
    /// before consuming bound parameters.
    #[inline(never)]
    fn refuse_session_creation(&self) -> Result<()> {
        if self.query_options.create_session && !self.transaction.is_autocommit()? {
            let option = BigQueryStatementOption::CreateSession.as_str();
            return Err(invalid_state(format!(
                "BigQuery option {option} cannot be enabled while \
                 {ADBC_CONNECTION_OPTION_AUTOCOMMIT} is disabled: the statement runs in the \
                 transaction's BigQuery session; set {option} to false or re-enable autocommit",
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
        // A dry run reads no row, so the binding stays for the execution it validates.
        let stream = (!self.query_options.dry_run)
            .then(|| self.parameters.take())
            .flatten();
        let Some(stream) = stream else {
            let parameters = self.typed_nulls()?;
            let operation = self.cancellation.begin();
            // The query completes before this returns, and the completing response carries
            // `totalRows` for a result and `numDmlAffectedRows` for DML.
            let (reader, rows_affected) =
                self.run_query(&operation, sql, parameters, execute_query)?;
            // Draining still issues requests (`jobs.getQueryResults` pages, Storage Read
            // `ReadRows`), so the reader takes prompt cancellation: a cancelled drain must
            // report itself, not end cleanly over a partial result.
            return Ok((
                operation_reader(reader, operation),
                rows_affected.unwrap_or(UNKNOWN_ROWS_AFFECTED),
            ));
        };
        let operation = self.cancellation.begin();
        let mut rows = ParameterRows::new(stream, self.parameter_mode)?;
        let Some(parameters) = rows.next(&operation)? else {
            // No parameter row means nothing executed, so the count is known: none, as
            // `execute_update` and `execute_partitions` report it.
            let schema = self.empty_binding_schema(&rows, &operation, sql)?;
            operation.finish();
            return Ok((empty_reader(Arc::new(schema)), 0));
        };

        let (current, rows_affected) =
            self.run_query(&operation, sql, Some(parameters), execute_query)?;
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
                let (reader, affected) =
                    self.run_query(&operation, sql, Some(parameters), execute_query)?;
                require_same_result_schema(&schema, &reader.schema())?;
                total = add_rows_affected(total, affected);
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
        // Later rows have not run yet, so the total is unknown.
        Ok((operation_reader(reader, operation), UNKNOWN_ROWS_AFFECTED))
    }

    /// Runs `query` once in the statement's session.
    fn run_query<T>(
        &self,
        operation: &Operation,
        sql: &Arc<str>,
        parameters: Option<Vec<QueryParameter>>,
        query: impl FnOnce(
            StatementQueryContext<'_>,
            &Arc<str>,
            Option<(&str, Vec<QueryParameter>)>,
        ) -> Result<T>,
    ) -> Result<T> {
        let session_id = TransactionContext::new(&self.context, self.query_defaults())
            .execution_session(&self.transaction, operation, self.query_options.dry_run)?;
        let mode = parameters.is_some().then_some(self.parameter_mode);
        query(
            self.query_context(session_id.as_deref(), operation),
            sql,
            parameters.map(|parameters| (self.parameter_mode.as_bigquery_value(), parameters)),
        )
        .map_err(|failure| ParameterMode::explain_refusal(mode, failure))
    }

    /// Typed nulls from the bound stream's schema, which is all a dry run or a description of the
    /// statement sends, so no row is read and the binding stays. `None` without a binding.
    fn typed_nulls(&self) -> Result<Option<Vec<QueryParameter>>> {
        self.parameters
            .schema()
            .map(|schema| parameters_for_schema(&schema, self.parameter_mode))
            .transpose()
    }

    /// The result schema a dry run reports for `parameters`.
    fn describe(
        &self,
        operation: &Operation,
        sql: &Arc<str>,
        parameters: Vec<QueryParameter>,
    ) -> Result<Schema> {
        let session_id = self.transaction.session_id()?;
        execute_schema(
            self.query_context(session_id.as_deref(), operation),
            sql,
            Some((self.parameter_mode.as_bigquery_value(), parameters)),
        )
        .map_err(|failure| ParameterMode::explain_refusal(Some(self.parameter_mode), failure))
    }

    /// The result schema of a statement whose bound parameter stream holds no rows: nothing
    /// executes, so a dry run with parameters typed from the stream's schema describes it.
    fn empty_binding_schema(
        &self,
        rows: &ParameterRows,
        operation: &Operation,
        sql: &Arc<str>,
    ) -> Result<Schema> {
        let parameters = parameters_for_schema(rows.schema().as_ref(), self.parameter_mode)?;
        self.describe(operation, sql, parameters)
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
        // The bound used everywhere the binding is read later, checked here as the C ABI's import
        // checks it, so a refused schema keeps the previous binding instead of failing at execute.
        validate_schema_nesting(reader.schema().fields(), "bound Arrow")?;
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
        // A dry run reads no row, so the binding stays for the execution it validates.
        let stream = (!self.query_options.dry_run)
            .then(|| self.parameters.take())
            .flatten();
        let parameters = self.typed_nulls()?;
        self.cancellation.run(|operation| {
            let Some(stream) = stream else {
                return self.run_query(operation, sql, parameters, execute_update_with_parameters);
            };
            // An empty binding runs nothing, so it affects no row and opens no transaction.
            let mut total = Some(0_i64);
            ParameterRows::new(stream, self.parameter_mode)?.for_each(operation, |parameters| {
                let affected = self.run_query(
                    operation,
                    sql,
                    Some(parameters),
                    execute_update_with_parameters,
                )?;
                total = add_rows_affected(total, affected);
                Ok(())
            })?;
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
        // Describing needs no values: an unbound statement's placeholders, including those of a
        // binding an earlier execute consumed, are left undeclared for the dry run to type, as
        // `get_parameter_schema` does. Without the parameter mode BigQuery refuses `?`.
        let parameters = self.typed_nulls()?.unwrap_or_default();
        self.cancellation
            .run(|operation| self.describe(operation, sql, parameters))
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
        let stream = self.parameters.take();
        self.cancellation.run(|operation| {
            let Some(stream) = stream else {
                let partition = self.run_query(
                    operation,
                    sql,
                    None,
                    execute_query_partition_with_parameters,
                )?;
                return Ok(PartitionedResult {
                    partitions: vec![partition.descriptor],
                    schema: partition.schema,
                    rows_affected: partition.rows_affected.unwrap_or(-1),
                });
            };

            let mut rows = ParameterRows::new(stream, self.parameter_mode)?;
            let mut partitions = Vec::new();
            let mut schema: Option<Schema> = None;
            let mut rows_affected = Some(0_i64);
            rows.for_each(operation, |parameters| {
                let partition = self.run_query(
                    operation,
                    sql,
                    Some(parameters),
                    execute_query_partition_with_parameters,
                )?;
                match &schema {
                    Some(first) => require_same_result_schema(first, &partition.schema)?,
                    None => schema = Some(partition.schema),
                }
                partitions.push(partition.descriptor);
                rows_affected = add_rows_affected(rows_affected, partition.rows_affected);
                Ok(())
            })?;
            // An empty binding runs no job, so a dry run typed from the binding's schema describes
            // the result.
            let schema = match schema {
                Some(schema) => schema,
                None => self.empty_binding_schema(&rows, operation, sql)?,
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
        if let Some(cached) = &*self.parameter_schema.borrow()
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
                .map_err(|failure| {
                    ParameterMode::explain_refusal(Some(self.parameter_mode), failure)
                });
        operation.finish();
        let schema = result?;
        *self.parameter_schema.borrow_mut() = Some(CachedParameterSchema {
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
        *self.parameter_schema.get_mut() = None;
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
