//! Statement mode switching and execution of operations configured through options.

use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{Fields, Metadata, Schema};

use crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID;
use crate::dbt::statement::{DbtExecution, DbtMode, DbtOperation};
use crate::error::invalid_state;
use crate::ingest::{execute_ingest, validate_ingest};
use crate::query::empty_reader;

use super::BigQueryStatement;

/// Mutually exclusive execution modes, selected by [`BigQueryStatement::enter_mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatementMode {
    /// An ADBC bulk ingest into `adbc.ingest.target_table`.
    Ingest,
    /// A dbt operation.
    Dbt(DbtMode),
    /// A SQL query.
    SqlQuery,
}

/// Operations selected without SQL, in execution precedence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OptionOnlyOperation {
    Ingest,
    Dbt(DbtOperation),
}

impl OptionOnlyOperation {
    pub(super) fn subject(self) -> &'static str {
        match self {
            Self::Ingest => "bulk ingest",
            Self::Dbt(operation) => operation.subject(),
        }
    }

    /// Options to clear when an operation conflicts with another setting.
    pub(super) fn selecting_options(self) -> &'static str {
        match self {
            Self::Ingest => "adbc.ingest.target_table",
            Self::Dbt(operation) => operation.selecting_options(),
        }
    }

    /// The schema of the rows this operation returns; `None` when it returns none.
    pub(super) fn result_schema(self) -> Option<Schema> {
        match self {
            Self::Ingest => None,
            Self::Dbt(operation) => operation.result_schema(),
        }
    }
}

pub(crate) struct OptionOnlyOutcome {
    /// `None` becomes ADBC's unknown row count, -1.
    pub(crate) rows_affected: Option<i64>,
    /// The job it ran, which only a result schema can carry: a load or copy job's BigQuery job
    /// ID, or a python model's remote job.
    pub(crate) job: Option<String>,
    /// What a lookup read. Every other operation returns no rows.
    pub(crate) rows: Option<RecordBatch>,
}

impl OptionOnlyOutcome {
    pub(crate) fn rows(rows_affected: Option<i64>) -> Self {
        Self {
            rows_affected,
            job: None,
            rows: None,
        }
    }

    /// The result stream of an execute that returns one, with its ADBC row count. A lookup counts
    /// the rows it returns, as a query does.
    pub(super) fn into_result(self) -> (Box<dyn RecordBatchReader + Send + 'static>, i64) {
        match self.rows {
            Some(rows) => {
                let count = i64::try_from(rows.num_rows()).unwrap_or(-1);
                let schema = rows.schema();
                let rows = std::iter::once(Ok(rows));
                (Box::new(RecordBatchIterator::new(rows, schema)), count)
            }
            None => (
                empty_reader(Arc::new(self.schema())),
                self.rows_affected.unwrap_or(-1),
            ),
        }
    }

    /// Empty schema carrying the job ID as `BIGQUERY:query_id` for dbt 2.0.
    pub(super) fn schema(&self) -> Schema {
        let metadata = self
            .job
            .as_ref()
            .map(|job| Metadata::new().with(BIGQUERY_SCHEMA_METADATA_QUERY_ID, job.clone()))
            .unwrap_or_default();
        Schema::new_with_metadata(Fields::empty(), metadata)
    }
}

impl BigQueryStatement {
    fn execute_ingest(&mut self) -> Result<i64> {
        self.dbt.reject_ingest_conflict()?;
        // Storage Write has no session, and BigQuery rejects the table DDL inside a transaction.
        // A dry run is refused too: it could only report this same failure.
        self.transaction.require_autocommit("bulk ingest")?;
        // A dry run borrows the bound reader for its schema, so the statement stays bound for
        // the real ingest.
        if self.query_options.dry_run {
            let schema = self.parameters.schema().ok_or_else(unbound_ingest_data)?;
            return self
                .cancellation
                .run(|operation| {
                    validate_ingest(self.ingest_context(operation), &self.ingest, &schema)
                })
                .map(|()| 0);
        }
        let parameters = self.parameters.take().ok_or_else(unbound_ingest_data)?;
        self.cancellation.run(|operation| {
            execute_ingest(self.ingest_context(operation), &self.ingest, parameters)
        })
    }

    /// The option-only operation this statement runs, or `None` when it runs its SQL query. Shared
    /// by every execute entry point; a bulk ingest takes precedence over any dbt operation.
    pub(super) fn active_option_only_operation(&self) -> Option<OptionOnlyOperation> {
        if self.ingest.target_table.is_some() {
            Some(OptionOnlyOperation::Ingest)
        } else {
            self.dbt.active_operation().map(OptionOnlyOperation::Dbt)
        }
    }

    pub(super) fn execute_option_only_operation(
        &mut self,
        operation: OptionOnlyOperation,
    ) -> Result<OptionOnlyOutcome> {
        match operation {
            OptionOnlyOperation::Ingest => {
                let rows_affected = self.execute_ingest()?;
                Ok(OptionOnlyOutcome::rows(Some(rows_affected)))
            }
            OptionOnlyOperation::Dbt(operation) => self.dbt.execute(
                operation,
                &DbtExecution {
                    context: &self.context,
                    query_options: &self.query_options,
                    transaction: &self.transaction,
                    cancellation: &self.cancellation,
                    sql: self.sql.as_deref(),
                },
            ),
        }
    }

    /// Enters `mode`, clearing the options of every other execution mode.
    ///
    /// Entering [`StatementMode::Ingest`] keeps the ingest target's catalog and schema, which the
    /// caller is about to complete with a table name; every other mode clears the whole target
    /// rather than leaving a stale catalog or schema behind for a later ingest.
    pub(super) fn enter_mode(&mut self, mode: StatementMode) {
        if mode != StatementMode::Ingest {
            self.ingest.target_table = None;
            self.ingest.target_catalog = None;
            self.ingest.target_db_schema = None;
        }
        self.dbt.enter_mode(match mode {
            StatementMode::Dbt(mode) => Some(mode),
            StatementMode::Ingest | StatementMode::SqlQuery => None,
        });
        if mode != StatementMode::SqlQuery {
            self.sql = None;
        }
    }
}

fn unbound_ingest_data() -> adbc_core::error::Error {
    invalid_state(
        "no Arrow data is bound to the BigQuery bulk ingest statement; bind it with AdbcStatementBind or AdbcStatementBindStream first",
    )
}
