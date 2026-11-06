//! Parameter-row iteration and lazy execution of bound queries.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{RecordBatch, RecordBatchOptions, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::cancellation::Operation;
use crate::error::error;
use crate::parameters::{ParameterMode, ParameterPlan};
use crate::query::{ExecutionContext, StatementQueryContext, StatementQueryOptions, execute_query};
use crate::rest::model::QueryParameter;
use crate::transaction::{TransactionContext, TransactionState};

pub(super) struct ParameterRows {
    batches: Batches,
    /// Rows yielded across batches: the stream position of the next row, which errors name.
    yielded: usize,
    plan: ParameterPlan,
}

impl ParameterRows {
    pub(super) fn new(
        reader: Box<dyn RecordBatchReader + Send>,
        parameter_mode: ParameterMode,
    ) -> Result<Self> {
        let schema = reader.schema();
        let plan = ParameterPlan::try_new(schema.as_ref(), parameter_mode)?;
        Ok(Self {
            batches: Batches {
                reader,
                schema,
                current: None,
                row: 0,
            },
            yielded: 0,
            plan,
        })
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.batches.schema
    }

    /// Runs `body` for every remaining parameter row in the stream.
    pub(super) fn for_each(
        &mut self,
        operation: &Operation,
        mut body: impl FnMut(Vec<QueryParameter>) -> Result<()>,
    ) -> Result<()> {
        while let Some(parameters) = self.next(operation)? {
            body(parameters).map_err(|failure| self.failed_row(failure))?;
        }
        Ok(())
    }

    /// Identifies a failed execution of the last yielded row and the earlier rows that ran.
    pub(super) fn failed_row(&self, failure: Error) -> Error {
        after_executed_rows(self.yielded.saturating_sub(1), failure)
    }

    /// Converts the next parameter row, or `None` at the end of the stream.
    pub(super) fn next(&mut self, operation: &Operation) -> Result<Option<Vec<QueryParameter>>> {
        let next = match self.batches.peek(operation) {
            Ok(Some((batch, row))) => self.plan.row(batch, row, self.yielded),
            Ok(None) => return Ok(None),
            Err(failure) => Err(failure),
        };
        let parameters = next.map_err(|failure| after_executed_rows(self.yielded, failure))?;
        self.batches.row += 1;
        if self
            .batches
            .current
            .as_ref()
            .is_some_and(|batch| self.batches.row == batch.num_rows())
        {
            // Converted values own their data, so the final row's remote execution need not
            // retain its input buffers, including allocations shared by a short Arrow slice.
            self.batches.current = None;
        }
        self.yielded += 1;
        Ok(Some(parameters))
    }

    /// Whether the stream holds another row. Reads ahead past empty batches, but converts nothing.
    pub(super) fn has_next(&mut self, operation: &Operation) -> Result<bool> {
        match self.batches.peek(operation) {
            Ok(next) => Ok(next.is_some()),
            Err(failure) => Err(after_executed_rows(self.yielded, failure)),
        }
    }
}

/// A bound stream, read one row at a time.
struct Batches {
    reader: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    /// The batch holding row `row`, unless none has been read or it is exhausted.
    current: Option<RecordBatch>,
    row: usize,
}

impl Batches {
    /// The batch holding the next unread row and that row's index in it, reading past empty
    /// batches; `None` at the end of the stream.
    fn peek(&mut self, operation: &Operation) -> Result<Option<(&RecordBatch, usize)>> {
        loop {
            operation.check()?;
            if self
                .current
                .as_ref()
                .is_some_and(|batch| self.row < batch.num_rows())
            {
                break;
            }
            // Empty batches can still own large sliced buffers; drop them before the next
            // input callback allocates or waits for another batch.
            self.current = None;
            let next = self.reader.next();
            operation.check()?;
            match next {
                Some(Ok(batch)) if batch.schema_ref() == &self.schema => {
                    self.current = Some(batch);
                    self.row = 0;
                }
                Some(Ok(batch)) => {
                    return Err(error(
                        format!(
                            "a bound BigQuery parameter stream returned a batch whose schema \
                             differs from the one it declared: expected {}, got {}; every batch \
                             of bound parameters must match the stream schema",
                            describe_schema(&self.schema),
                            describe_schema(batch.schema_ref())
                        ),
                        Status::InvalidData,
                    ));
                }
                Some(Err(source)) => {
                    return Err(error(
                        format!("failed to read the bound BigQuery parameter stream: {source}"),
                        Status::InvalidData,
                    ));
                }
                None => return Ok(None),
            }
        }
        Ok(self.current.as_ref().map(|batch| (batch, self.row)))
    }
}

/// Prefixes the failure of the bound row at stream position `row` with the rows before it, which
/// already executed: a statement they modified is partly applied.
fn after_executed_rows(row: usize, mut failure: Error) -> Error {
    if row > 0 {
        let plural = if row == 1 { "" } else { "s" };
        failure.message = format!(
            "bound BigQuery parameter row {row} failed after {row} earlier row{plural} had \
             already executed: {}",
            failure.message
        );
    }
    failure
}

pub(super) struct ParameterizedRecordBatchReader {
    pub(super) context: ExecutionContext,
    pub(super) query_options: StatementQueryOptions,
    pub(super) transaction: Arc<TransactionState>,
    pub(super) sql: Arc<str>,
    pub(super) parameter_mode: ParameterMode,
    pub(super) rows: ParameterRows,
    pub(super) current: Box<dyn RecordBatchReader + Send>,
    pub(super) schema: SchemaRef,
    pub(super) failed: bool,
    pub(super) operation: Operation,
}

impl Iterator for ParameterizedRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let batch = self.next_batch().transpose();
        self.failed = batch.as_ref().is_some_and(|batch| batch.is_err());
        batch
    }
}

impl ParameterizedRecordBatchReader {
    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, ArrowError> {
        loop {
            if let Some(batch) = self.current.next() {
                let batch = batch.map_err(|source| match source {
                    ArrowError::ExternalError(source) => {
                        ArrowError::ExternalError(match source.downcast::<Error>() {
                            Ok(failure) => Box::new(self.rows.failed_row(*failure)),
                            Err(source) => source,
                        })
                    }
                    source => source,
                })?;
                if batch.schema_ref() == &self.schema {
                    return Ok(Some(batch));
                }
                // Each parameter row runs as its own job. Keep the reader's metadata while
                // preserving the columns, whose compatibility was checked below.
                let (_, columns, row_count) = batch.into_parts();
                return RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(row_count)),
                )
                .map(Some);
            }

            let Some(parameters) = self
                .rows
                .next(&self.operation)
                .map_err(|source| ArrowError::ExternalError(Box::new(source)))?
            else {
                return Ok(None);
            };
            self.current = TransactionContext::new(
                &self.context,
                self.context.statement_defaults(&self.query_options),
            )
            .execution_session(
                &self.transaction,
                &self.operation,
                self.query_options.dry_run,
            )
            .and_then(|session_id| {
                execute_query(
                    StatementQueryContext {
                        execution: &self.context,
                        session_id: session_id.as_deref(),
                        operation: &self.operation,
                        options: &self.query_options,
                        transaction: &self.transaction,
                        // Keep progress tied to the initial execute call.
                        progress: None,
                    },
                    &self.sql,
                    Some((self.parameter_mode.as_bigquery_value(), parameters)),
                )
            })
            .and_then(|(reader, _)| {
                let schema = reader.schema();
                if result_schemas_match(&schema, &self.schema) {
                    Ok(reader)
                } else {
                    Err(divergent_parameter_schema(&self.schema, &schema))
                }
            })
            .map_err(|source| ArrowError::ExternalError(Box::new(self.rows.failed_row(source))))?;
        }
    }
}

impl RecordBatchReader for ParameterizedRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Whether two result schemas agree apart from their per-job metadata.
///
/// Each parameter row of a parameterized statement runs as its own query job, so identical results
/// legitimately differ in the query ID and in every job statistic derived from it.
pub(super) fn result_schemas_match(left: &Schema, right: &Schema) -> bool {
    fn metadata_matches(left: &Schema, right: &Schema) -> bool {
        left.metadata().iter().all(|(key, value)| {
            crate::BIGQUERY_SCHEMA_METADATA_JOB_KEYS.contains(&key.as_str())
                || right.metadata().get(key) == Some(value)
        })
    }
    left.fields() == right.fields()
        && metadata_matches(left, right)
        && metadata_matches(right, left)
}

/// One execute runs every bound parameter row into one result stream, so every row has to produce
/// the same result schema. [`ParameterRows::failed_row`] adds which row broke that.
pub(super) fn divergent_parameter_schema(expected: &Schema, found: &Schema) -> Error {
    error(
        format!(
            "its result schema differs from that of parameter row 0: expected {}, got {}; every \
             parameter row bound to one execute must yield the same result schema",
            describe_schema(expected),
            describe_schema(found)
        ),
        Status::InvalidData,
    )
}

/// A result schema as a diagnostic names it: its fields, in order, as `name: type`. Only the
/// shape is rendered, never a value.
fn describe_schema(schema: &Schema) -> String {
    let fields = schema
        .fields()
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({fields})")
}

/// Sums two affected-row counts; either one unknown (`None`, or adbc.h's negative) makes the sum
/// unknown rather than a partial total.
pub(super) fn add_rows_affected(total: Option<i64>, affected: Option<i64>) -> Result<Option<i64>> {
    let (Some(total), Some(affected)) = (total, affected) else {
        return Ok(None);
    };
    if total < 0 || affected < 0 {
        return Ok(None);
    }
    total.checked_add(affected).map(Some).ok_or_else(|| {
        error(
            "combined BigQuery affected row count overflows i64",
            Status::InvalidData,
        )
    })
}

#[cfg(test)]
mod tests;
