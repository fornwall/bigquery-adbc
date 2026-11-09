//! The dbt state of one statement, which `BigQueryStatement` holds as a single field.

use std::fmt::DebugStruct;
use std::sync::Arc;

use adbc_core::error::{Error, Result};
use adbc_core::options::OptionValue;
use arrow_schema::Schema;

use crate::BigQueryStatementOption;
use crate::cancellation::Cancellation;
use crate::error::{driver_defect, invalid_argument, invalid_state, wrong_option_type};
use crate::options::{bytes_value, enum_value, integer_value, string_value};
use crate::query::{ExecutionContext, QualifiedTableName, QueryDefaults, StatementQueryOptions};
use crate::statement::OptionOnlyOutcome;
use crate::statement::sql_text::{has_no_statement, skip_comments};
use crate::transaction::TransactionState;

use super::FinishedJob;
use super::csv_load::execute_csv_load;
use super::csv_seed::CsvSeedOptions;
use super::dataproc::DataprocOptions;
use super::dataset_authorization::{DatasetAuthorizationOptions, parse_view_authorizations};
use super::dataset_location::{DatasetLocationOptions, location_schema};
use super::gcs::GcsWriteOptions;
use super::table_copy::TableCopyOptions;
use super::table_mutation::TableMutationOptions;
use super::vertex::NotebookExecutionOptions;

/// A pattern matching every statement option [`DbtStatementState`] serves, which lets the generic
/// option dispatch route them without naming them and still match exhaustively.
macro_rules! dbt_statement_option {
    () => {
        $crate::BigQueryStatementOption::AuthorizeViewToDatasets
            | $crate::BigQueryStatementOption::GetDatasetLocation
            | $crate::BigQueryStatementOption::CsvDelimiter
            | $crate::BigQueryStatementOption::CsvFilepath
            | $crate::BigQueryStatementOption::CsvSchema
            | $crate::BigQueryStatementOption::CopyTableSource
            | $crate::BigQueryStatementOption::CopyTableDestination
            | $crate::BigQueryStatementOption::CopyTableWriteDisposition
            | $crate::BigQueryStatementOption::UpdateColumnsDescription
            | $crate::BigQueryStatementOption::UpdateColumnsPolicyTags
            | $crate::BigQueryStatementOption::UpdateDescription
            | $crate::BigQueryStatementOption::DataprocPoolingTimeout
            | $crate::BigQueryStatementOption::WriteGcsBucket
            | $crate::BigQueryStatementOption::WriteGcsObjectName
            | $crate::BigQueryStatementOption::WriteGcsContent
            | $crate::BigQueryStatementOption::DataprocComputeRegion
            | $crate::BigQueryStatementOption::DataprocProject
            | $crate::BigQueryStatementOption::DataprocSubmitJobClusterName
            | $crate::BigQueryStatementOption::DataprocSubmitJobGcsPath
            | $crate::BigQueryStatementOption::CreateBatchParent
            | $crate::BigQueryStatementOption::CreateBatchBatchYml
            | $crate::BigQueryStatementOption::CreateBatchBatchId
            | $crate::BigQueryStatementOption::NotebookExecuteJobGscPath
            | $crate::BigQueryStatementOption::NotebookExecuteJobGscBucket
            | $crate::BigQueryStatementOption::NotebookExecuteJobModelFileName
            | $crate::BigQueryStatementOption::NotebookExecuteJobModelName
            | $crate::BigQueryStatementOption::NotebookExecuteJobParent
            | $crate::BigQueryStatementOption::NotebookExecuteJobProject
            | $crate::BigQueryStatementOption::NotebookExecuteJobRegion
            | $crate::BigQueryStatementOption::NotebookExecuteJobServiceAccount
            | $crate::BigQueryStatementOption::NotebookExecuteJobTemplateId
    };
}

pub(crate) use dbt_statement_option;

/// Mutually exclusive dbt execution modes, which also exclude ingest and a SQL query.
///
/// A table metadata update is not one of these: it names the statement's own destination table,
/// so combining it with another operation is rejected at execution instead; see `DBT.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbtMode {
    /// The dbt 2.0 CSV seed load.
    CsvSeed,
    /// The authorized-view payload.
    DatasetAuthorization,
    /// The dataset location lookup.
    DatasetLocation,
    /// A BigQuery table copy job.
    TableCopy,
    /// Any of the three dbt 2.0 python-model operations.
    PythonModel,
}

/// Operations a dbt statement runs without SQL, in execution precedence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbtOperation {
    DatasetAuthorization,
    DatasetLocation,
    CsvSeed,
    TableMutation,
    TableCopy,
    PythonModel(PythonModelOperation),
}

impl DbtOperation {
    pub(crate) fn subject(self) -> &'static str {
        match self {
            Self::DatasetAuthorization => "authorized-view update",
            Self::DatasetLocation => "dataset location lookup",
            Self::CsvSeed => "CSV seed load",
            Self::TableMutation => "table metadata update",
            Self::TableCopy => "table copy",
            Self::PythonModel(_) => "python model",
        }
    }

    /// Options to clear when an operation conflicts with another setting.
    pub(crate) fn selecting_options(self) -> &'static str {
        match self {
            Self::DatasetAuthorization => "adbc.bigquery.dataset.authorize_view_to_datasets",
            Self::DatasetLocation => "bigquery.dataset.get_location",
            Self::CsvSeed => "adbc.bigquery.ingest.csv_filepath",
            Self::TableMutation => "the adbc.bigquery.table.update_* options",
            Self::TableCopy => "adbc.bigquery.copy_table.source or .destination",
            Self::PythonModel(_) => {
                "the adbc.bigquery.write_gcs.*, adbc.bigquery.dataproc.* and \
                 adbc.bigquery.notebook_execute_job.* options"
            }
        }
    }

    /// The schema of the rows this operation returns; `None` when it returns none.
    pub(crate) fn result_schema(self) -> Option<Schema> {
        (self == Self::DatasetLocation).then(location_schema)
    }
}

/// What setting a SQL query does, given the dbt operation configured on the statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlDisposition {
    /// A dbt placeholder, dropped rather than stored: were the option cleared afterwards,
    /// executing must report a missing query, not send the placeholder to BigQuery.
    Drop,
    /// Stored without leaving the configured operation.
    Store,
    /// Real SQL, which leaves every other execution mode.
    Replace,
}

/// The selected Python-model operation, shared by execution and conflict diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PythonModelOperation {
    DataprocBatch,
    DataprocClusterJob,
    Notebook,
    GcsWrite,
}

impl PythonModelOperation {
    fn subject(self) -> &'static str {
        match self {
            Self::DataprocBatch => "Dataproc batch submission",
            Self::DataprocClusterJob => "Dataproc job submission",
            Self::Notebook => "Vertex AI notebook execution",
            Self::GcsWrite => "Cloud Storage object write",
        }
    }

    fn option_family(self) -> &'static str {
        match self {
            Self::DataprocBatch => "adbc.bigquery.create_batch.*",
            Self::DataprocClusterJob => "adbc.bigquery.dataproc.submit_job.*",
            Self::Notebook => "adbc.bigquery.notebook_execute_job.*",
            Self::GcsWrite => "adbc.bigquery.write_gcs.*",
        }
    }
}

/// What a dbt operation needs of the statement running it.
pub(crate) struct DbtExecution<'a> {
    pub(crate) context: &'a ExecutionContext,
    pub(crate) query_options: &'a StatementQueryOptions,
    pub(crate) transaction: &'a TransactionState,
    pub(crate) cancellation: &'a Arc<Cancellation>,
    pub(crate) sql: Option<&'a str>,
}

/// The configuration of every dbt operation a statement can run.
#[derive(Default)]
pub(crate) struct DbtStatementState {
    csv_seed: CsvSeedOptions,
    dataset_authorization: DatasetAuthorizationOptions,
    dataset_location: DatasetLocationOptions,
    table_mutation: TableMutationOptions,
    table_copy: TableCopyOptions,
    /// The three dbt 2.0 python-model operations share one mode because their option
    /// families overlap (see [`crate::dbt::dataproc`]).
    gcs_write: GcsWriteOptions,
    dataproc: DataprocOptions,
    notebook: NotebookExecutionOptions,
}

impl DbtStatementState {
    /// Writes this state's fields into the statement's debug output, at the level of its own.
    pub(crate) fn debug_fields(&self, debug: &mut DebugStruct<'_, '_>) {
        debug
            .field("csv_seed", &self.csv_seed)
            .field("dataset_authorization", &self.dataset_authorization)
            .field("dataset_location", &self.dataset_location)
            .field("table_mutation", &self.table_mutation)
            .field("table_copy", &self.table_copy)
            .field("gcs_write", &self.gcs_write)
            .field("dataproc", &self.dataproc)
            .field("notebook", &self.notebook);
    }

    /// Applies one of the [`dbt_statement_option`] options, answering the mode the statement must
    /// now enter, if any. A failure leaves the statement's mode as it was.
    pub(crate) fn set_option(
        &mut self,
        option: BigQueryStatementOption,
        key: &str,
        value: OptionValue,
        defaults: QueryDefaults<'_>,
    ) -> Result<Option<DbtMode>> {
        Ok(match option {
            BigQueryStatementOption::AuthorizeViewToDatasets => {
                let value = string_value(key, value)?;
                let views =
                    parse_view_authorizations(&value, defaults.catalog, defaults.db_schema)?;
                self.dataset_authorization.set(views);
                self.dataset_authorization
                    .is_active()
                    .then_some(DbtMode::DatasetAuthorization)
            }
            BigQueryStatementOption::GetDatasetLocation => {
                self.dataset_location.set(string_value(key, value)?)?;
                self.dataset_location
                    .is_active()
                    .then_some(DbtMode::DatasetLocation)
            }
            BigQueryStatementOption::CsvDelimiter => {
                let value = string_value(key, value)?;
                self.csv_seed.set_delimiter(&value)?;
                None
            }
            BigQueryStatementOption::CsvFilepath => {
                let value = string_value(key, value)?;
                self.csv_seed.set_filepath(value);
                self.csv_seed.is_active().then_some(DbtMode::CsvSeed)
            }
            BigQueryStatementOption::CsvSchema => {
                self.csv_seed.set_schema(bytes_value(key, value)?)?;
                None
            }
            BigQueryStatementOption::CopyTableSource => {
                let value = string_value(key, value)?;
                self.table_copy.source = defaults.table_option(option.as_str(), &value)?;
                self.table_copy_mode()
            }
            BigQueryStatementOption::CopyTableDestination => {
                let value = string_value(key, value)?;
                self.table_copy.destination = defaults.table_option(option.as_str(), &value)?;
                self.table_copy_mode()
            }
            BigQueryStatementOption::CopyTableWriteDisposition => {
                self.table_copy.write_disposition = enum_value(key, value)?;
                None
            }
            BigQueryStatementOption::UpdateColumnsDescription => {
                let value = string_value(key, value)?;
                self.table_mutation.set_descriptions(key, &value)?;
                None
            }
            BigQueryStatementOption::UpdateColumnsPolicyTags => {
                let value = string_value(key, value)?;
                self.table_mutation.set_policy_tags(key, &value)?;
                None
            }
            BigQueryStatementOption::UpdateDescription => {
                self.table_mutation
                    .set_description(string_value(key, value)?);
                None
            }
            BigQueryStatementOption::DataprocPoolingTimeout => {
                self.dataproc
                    .set_pooling_timeout_seconds(parse_pooling_timeout(key, value)?);
                self.python_model_mode()
            }
            // The python-model text options: one string into the field the key names (an empty
            // value clears it), through the same slot table the getter reads.
            _ => {
                let value = string_value(key, value)?;
                let Some(slot) = self.python_model_text_slot_mut(option) else {
                    return Err(unwired_python_model_option(option));
                };
                *slot = (!value.is_empty()).then_some(value);
                self.python_model_mode()
            }
        })
    }

    /// Reads one of the [`dbt_statement_option`] options as a string: `None` while it is unset,
    /// the effective value when it has a default.
    pub(crate) fn option_string(
        &self,
        option: BigQueryStatementOption,
        key: &str,
    ) -> Result<Option<String>> {
        Ok(match option {
            BigQueryStatementOption::AuthorizeViewToDatasets => {
                return self.dataset_authorization.option_value();
            }
            BigQueryStatementOption::GetDatasetLocation => {
                self.dataset_location.option_value().map(str::to_owned)
            }
            BigQueryStatementOption::CsvDelimiter => Some(self.csv_seed.delimiter()),
            BigQueryStatementOption::CsvFilepath => self.csv_seed.filepath().map(str::to_owned),
            BigQueryStatementOption::CopyTableSource => self
                .table_copy
                .source
                .as_ref()
                .map(QualifiedTableName::as_option_value),
            BigQueryStatementOption::CopyTableDestination => self
                .table_copy
                .destination
                .as_ref()
                .map(QualifiedTableName::as_option_value),
            BigQueryStatementOption::CopyTableWriteDisposition => {
                Some(self.table_copy.write_disposition.as_str().to_owned())
            }
            BigQueryStatementOption::UpdateColumnsDescription => {
                return self.table_mutation.descriptions_json();
            }
            BigQueryStatementOption::UpdateColumnsPolicyTags => {
                return self.table_mutation.policy_tags_json();
            }
            BigQueryStatementOption::UpdateDescription => {
                self.table_mutation.description().map(str::to_owned)
            }
            // These read back through `option_bytes` and `option_int`.
            BigQueryStatementOption::CsvSchema
            | BigQueryStatementOption::DataprocPoolingTimeout => {
                return Err(wrong_option_type(key, option.option_kind(), "a string"));
            }
            _ => {
                let Some(slot) = self.python_model_text_slot(option) else {
                    return Err(unwired_python_model_option(option));
                };
                slot.clone()
            }
        })
    }

    /// The value of a bytes option, itself `None` while unset; `None` for any other option.
    pub(crate) fn option_bytes(&self, option: BigQueryStatementOption) -> Option<Option<&[u8]>> {
        (option == BigQueryStatementOption::CsvSchema).then(|| self.csv_seed.encoded_schema())
    }

    /// The value of an integer option; `None` for any other option.
    pub(crate) fn option_int(&self, option: BigQueryStatementOption) -> Option<i64> {
        (option == BigQueryStatementOption::DataprocPoolingTimeout)
            .then(|| self.dataproc.pooling_timeout_option_value())
    }

    /// Enters `mode`, clearing the options of every other dbt mode; `None` clears them all.
    pub(crate) fn enter_mode(&mut self, mode: Option<DbtMode>) {
        if mode != Some(DbtMode::CsvSeed) {
            self.csv_seed = Default::default();
        }
        if mode != Some(DbtMode::DatasetAuthorization) {
            self.dataset_authorization = Default::default();
        }
        if mode != Some(DbtMode::DatasetLocation) {
            self.dataset_location = Default::default();
        }
        if mode != Some(DbtMode::TableCopy) {
            self.table_copy = Default::default();
        }
        if mode != Some(DbtMode::PythonModel) {
            self.gcs_write = Default::default();
            self.dataproc = Default::default();
            self.notebook = Default::default();
        }
    }

    /// A configured copy endpoint leaves every other execution mode, exactly as a CSV seed does.
    fn table_copy_mode(&self) -> Option<DbtMode> {
        self.table_copy.is_active().then_some(DbtMode::TableCopy)
    }

    /// A configured python-model operation leaves every other execution mode. The three
    /// python-model families do not clear one another: execution precedence picks between them.
    fn python_model_mode(&self) -> Option<DbtMode> {
        self.python_model_is_active()
            .then_some(DbtMode::PythonModel)
    }

    /// Classifies a query set while this state is configured.
    pub(crate) fn sql_disposition(&self, query: &str) -> SqlDisposition {
        if (self.dataset_authorization.is_active() || self.dataset_location.is_active())
            && is_placeholder_statement(query)
        {
            return SqlDisposition::Drop;
        }
        if self.python_model_is_active() && is_select_one_statement(query) {
            return SqlDisposition::Drop;
        }
        // dbt 2.0 finalizes option-only operations with empty SQL, which its query-comment
        // machinery may turn into a lone comment. Neither may discard the CSV seed, table copy or
        // python-model state configured before it; otherwise, setting SQL still exits ingest.
        if has_no_statement(query)
            && (self.csv_seed.is_active()
                || self.table_copy.is_active()
                || self.python_model_is_active())
        {
            return SqlDisposition::Store;
        }
        SqlDisposition::Replace
    }

    /// The dbt operation this statement runs, if any. The one place their precedence is written.
    pub(crate) fn active_operation(&self) -> Option<DbtOperation> {
        if self.dataset_authorization.is_active() {
            Some(DbtOperation::DatasetAuthorization)
        } else if self.dataset_location.is_active() {
            Some(DbtOperation::DatasetLocation)
        } else if self.csv_seed.is_active() {
            Some(DbtOperation::CsvSeed)
        } else if self.table_mutation.is_active() {
            Some(DbtOperation::TableMutation)
        } else if self.table_copy.is_active() {
            Some(DbtOperation::TableCopy)
        } else {
            self.python_model_operation().map(DbtOperation::PythonModel)
        }
    }

    /// Runs `operation`, which [`Self::active_operation`] selected.
    pub(crate) fn execute(
        &self,
        operation: DbtOperation,
        statement: &DbtExecution<'_>,
    ) -> Result<OptionOnlyOutcome> {
        let rows_affected = match operation {
            DbtOperation::DatasetAuthorization => self.execute_dataset_authorization(statement)?,
            DbtOperation::TableMutation => self.execute_table_mutation(statement)?,
            DbtOperation::DatasetLocation => return self.execute_dataset_location(statement),
            DbtOperation::CsvSeed => return self.execute_csv_seed(statement),
            DbtOperation::TableCopy => return self.execute_table_copy(statement),
            // None of the python models touches a table, so the count is unknown.
            DbtOperation::PythonModel(selected) => {
                return Ok(OptionOnlyOutcome {
                    rows_affected: None,
                    job: self.execute_python_model(selected, statement)?,
                    rows: None,
                });
            }
        };
        Ok(OptionOnlyOutcome::rows(Some(rows_affected)))
    }

    /// Refuses a bulk ingest while a table metadata update is also configured.
    pub(crate) fn reject_ingest_conflict(&self) -> Result<()> {
        self.reject_pending_table_mutation("adbc.ingest.*")
    }

    /// Rejects the `family` operation while a table metadata update is also configured: no setter
    /// clears one for the other, and running one would silently drop the other.
    fn reject_pending_table_mutation(&self, family: &str) -> Result<()> {
        if self.table_mutation.is_active() {
            return Err(invalid_state(format!(
                "the {family} options cannot be combined with {} on one BigQuery statement",
                self.table_mutation.active_option_keys().join(" or "),
            )));
        }
        Ok(())
    }

    fn execute_csv_seed(&self, statement: &DbtExecution<'_>) -> Result<OptionOnlyOutcome> {
        self.reject_pending_table_mutation("adbc.bigquery.ingest.csv_*")?;
        statement
            .transaction
            .require_autocommit("CSV seed ingest")?;
        let destination = statement
            .query_options
            .destination_table
            .as_ref()
            .ok_or_else(|| {
                invalid_state(format!(
                    "BigQuery CSV seed ingest requires the {} option",
                    BigQueryStatementOption::DestinationTable.as_str()
                ))
            })?;
        let (csv, schema) = self.csv_seed.open_seed()?;
        // A dry run stops at the seed's schema, reading and sending nothing; nothing is loaded, so
        // the count is unknown rather than 0.
        if statement.query_options.dry_run {
            return Ok(OptionOnlyOutcome::rows(None));
        }
        statement
            .cancellation
            .run(|operation| {
                execute_csv_load(
                    statement.context,
                    statement.query_options,
                    destination,
                    operation,
                    csv,
                    schema,
                    &self.csv_seed.delimiter(),
                )
            })
            .map(OptionOnlyOutcome::from)
    }

    fn execute_dataset_authorization(&self, statement: &DbtExecution<'_>) -> Result<i64> {
        self.reject_pending_table_mutation(
            BigQueryStatementOption::AuthorizeViewToDatasets.as_str(),
        )?;
        statement
            .transaction
            .require_autocommit("view authorization")?;
        statement
            .cancellation
            .run(|operation| {
                self.dataset_authorization
                    .execute(statement.context, operation)
            })
            .map(|()| 0)
    }

    /// Reads the configured dataset's location. Unlike the operations that change something it
    /// needs no autocommit: a read has nothing for a later commit or rollback to appear to cover.
    fn execute_dataset_location(&self, statement: &DbtExecution<'_>) -> Result<OptionOnlyOutcome> {
        self.reject_pending_table_mutation(BigQueryStatementOption::GetDatasetLocation.as_str())?;
        let rows = statement
            .cancellation
            .run(|operation| self.dataset_location.execute(statement.context, operation))?;
        Ok(OptionOnlyOutcome {
            rows_affected: None,
            job: None,
            rows: Some(rows),
        })
    }

    /// Updates the configured table's metadata, refusing a configured python model, table copy
    /// or SQL query rather than silently dropping it. Operations of higher precedence refuse
    /// through [`Self::reject_pending_table_mutation`].
    fn execute_table_mutation(&self, statement: &DbtExecution<'_>) -> Result<i64> {
        let competing = self
            .python_model_operation()
            .map(PythonModelOperation::option_family)
            .or_else(|| {
                self.table_copy
                    .is_active()
                    .then_some("adbc.bigquery.copy_table.*")
            });
        if let Some(family) = competing {
            self.reject_pending_table_mutation(family)?;
        }
        // dbt 2.0 finalizes an option-only statement with a comment-only placeholder.
        if statement.sql.is_some_and(|sql| !has_no_statement(sql)) {
            return Err(invalid_state(format!(
                "a SQL query cannot be combined with {} on one BigQuery statement; clear the \
                 query or the option",
                self.table_mutation.active_option_keys().join(" or "),
            )));
        }
        statement
            .transaction
            .require_autocommit("table metadata updates")?;
        let table = statement
            .query_options
            .destination_table
            .as_ref()
            .ok_or_else(|| {
                invalid_state(format!(
                    "BigQuery table metadata updates require the {} option",
                    BigQueryStatementOption::DestinationTable.as_str()
                ))
            })?;
        statement
            .cancellation
            .run(|operation| {
                self.table_mutation
                    .execute(statement.context, table, operation)
            })
            .map(|()| 0)
    }

    fn execute_table_copy(&self, statement: &DbtExecution<'_>) -> Result<OptionOnlyOutcome> {
        statement.transaction.require_autocommit("table copy")?;
        statement
            .cancellation
            .run(|operation| {
                self.table_copy
                    .execute(statement.context, statement.query_options, operation)
            })
            .map(OptionOnlyOutcome::from)
    }

    /// Runs the selected Python-model operation, answering its remote job; a Cloud Storage
    /// write runs no job.
    fn execute_python_model(
        &self,
        selected: PythonModelOperation,
        statement: &DbtExecution<'_>,
    ) -> Result<Option<String>> {
        statement
            .transaction
            .require_autocommit(selected.subject())?;
        statement.cancellation.run(|operation| match selected {
            PythonModelOperation::DataprocBatch => self
                .dataproc
                .execute_batch(statement.context, operation)
                .map(Some),
            PythonModelOperation::DataprocClusterJob => self
                .dataproc
                .execute_cluster_job(statement.context, operation)
                .map(Some),
            PythonModelOperation::Notebook => self
                .notebook
                .execute(statement.context, &self.dataproc, operation)
                .map(Some),
            PythonModelOperation::GcsWrite => self
                .gcs_write
                .execute(statement.context, operation)
                .map(|()| None),
        })
    }

    fn python_model_is_active(&self) -> bool {
        self.python_model_operation().is_some()
    }

    /// The configured Python-model operation in execution precedence order. Shared Dataproc
    /// options (region, project, batch ID, timeout) select no operation on their own.
    fn python_model_operation(&self) -> Option<PythonModelOperation> {
        if self.dataproc.batch_is_active() {
            Some(PythonModelOperation::DataprocBatch)
        } else if self.dataproc.cluster_job_is_active() {
            Some(PythonModelOperation::DataprocClusterJob)
        } else if self.notebook.is_active() {
            Some(PythonModelOperation::Notebook)
        } else if self.gcs_write.is_active() {
            Some(PythonModelOperation::GcsWrite)
        } else {
            None
        }
    }

    /// Looks up a text option across the disjoint python-model option families.
    fn python_model_text_slot(&self, option: BigQueryStatementOption) -> Option<&Option<String>> {
        self.gcs_write
            .text_slot(option)
            .or_else(|| self.dataproc.text_slot(option))
            .or_else(|| self.notebook.text_slot(option))
    }

    fn python_model_text_slot_mut(
        &mut self,
        option: BigQueryStatementOption,
    ) -> Option<&mut Option<String>> {
        let Self {
            gcs_write,
            dataproc,
            notebook,
            ..
        } = self;
        gcs_write
            .text_slot_mut(option)
            .or_else(|| dataproc.text_slot_mut(option))
            .or_else(|| notebook.text_slot_mut(option))
    }
}

/// Decodes `adbc.bigquery.dataproc.pooling_timeout`, in seconds.
///
/// dbt 2.0 sends the string form; the integer form is accepted too. Zero and the empty string
/// mean no client-side deadline, so the unset value round-trips through `get_option_int` as `0`.
/// In the dbt-labs fork, zero is an already-expired deadline instead. Negative values are
/// rejected.
fn parse_pooling_timeout(key: &str, value: OptionValue) -> Result<Option<i64>> {
    if matches!(&value, OptionValue::String(value) if value.trim().is_empty()) {
        return Ok(None);
    }
    let seconds = integer_value(key, value)?;
    if seconds < 0 {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must not be a negative number of seconds; zero and the empty string clear it"
        )));
    }
    Ok((seconds > 0).then_some(seconds))
}

/// A python-model text option [`dbt_statement_option`] lists but no slot table claims. Unreachable
/// (a round-trip test walks every key), but reported as an error rather than a panic.
fn unwired_python_model_option(option: BigQueryStatementOption) -> Error {
    driver_defect(format_args!(
        "BigQuery option {} was accepted but not applied to the python model configuration",
        option.as_str()
    ))
}

impl From<FinishedJob> for OptionOnlyOutcome {
    fn from(job: FinishedJob) -> Self {
        Self {
            rows_affected: job.rows,
            job: Some(job.job_id),
            rows: None,
        }
    }
}

/// Recognizes dbt's `none` placeholder, allowing comments and a terminator. Other SQL
/// replaces the configured view authorization or dataset location lookup.
fn is_placeholder_statement(query: &str) -> bool {
    const PLACEHOLDER: &str = "none";
    match next_token(query) {
        None => true,
        Some((token, tail)) => token.eq_ignore_ascii_case(PLACEHOLDER) && is_statement_end(tail),
    }
}

/// Recognizes dbt's Python-model placeholder without discarding additional SQL.
fn is_select_one_statement(query: &str) -> bool {
    let Some((keyword, tail)) = next_token(query) else {
        return false;
    };
    if !keyword.eq_ignore_ascii_case("select") {
        return false;
    }
    let Some((literal, tail)) = next_token(tail) else {
        return false;
    };
    literal == "1" && is_statement_end(tail)
}

/// Comments may appear on either side of the optional statement terminator.
fn is_statement_end(query: &str) -> bool {
    let tail = skip_comments(query);
    has_no_statement(tail.strip_prefix(';').unwrap_or(tail))
}

/// Reads a token ending at whitespace, a semicolon or a comment; returns the unconsumed suffix.
fn next_token(query: &str) -> Option<(&str, &str)> {
    let rest = skip_comments(query);
    if rest.is_empty() {
        return None;
    }
    let end = rest
        .char_indices()
        .find(|(index, character)| {
            character.is_whitespace()
                || matches!(character, '#' | ';')
                || rest
                    .get(*index..)
                    .is_some_and(|tail| tail.starts_with("--") || tail.starts_with("/*"))
        })
        .map_or(rest.len(), |(index, _)| index);
    Some(rest.split_at(end))
}
