//! Statement option dispatch and validation.

use std::sync::Arc;

use adbc_core::Optionable;
use adbc_core::constants::ADBC_OPTION_VALUE_DISABLED;
use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};

use crate::dbt::statement::dbt_statement_option;
use crate::error::{
    invalid_argument, invalid_state, not_found, not_implemented, option_not_set,
    option_set_refusal, option_value_json, unknown_or_wrong_type, wrong_option_type,
};
use crate::identifiers::{ProjectIdentifier, validate_dataset_id, validate_table_id};
use crate::ingest::parse_ingest_method;
use crate::options::OptionKind;
use crate::options::keys::OPTION_BIND_BY_NAME;
use crate::options::{
    boolean_option, boolean_value, dataset_needs_a_project, enum_value, ingest_mode_value,
    integer_value, json_object_value, string_value,
};
use crate::parameters::ParameterMode;
use crate::query::QualifiedTableName;
use crate::{BigQueryBulkIngestMethod, BigQueryStatementOption};

use super::BigQueryStatement;
use super::operations::StatementMode;

impl Optionable for BigQueryStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        // Many options affect the parameter dry run (the parameter mode, the name-resolution
        // defaults), so every option drops the memo rather than keeping a list that could drift.
        self.forget_parameter_schema();
        // Options that name tables resolve them against the connection's namespace as it is now.
        self.refresh_namespace();
        match &key {
            key if is_bind_by_name_option(key) => {
                self.parameter_mode = if boolean_value(key.as_ref(), value)? {
                    ParameterMode::Named
                } else {
                    ParameterMode::Positional
                };
                Ok(())
            }
            key if let Some(option) = statement_option_key(key) => {
                self.set_bigquery_option(option, key, value)
            }
            OptionStatement::IngestMode => {
                self.ingest.mode = ingest_mode_value(key.as_ref(), value)?;
                Ok(())
            }
            OptionStatement::TargetTable => {
                let value = string_value(key.as_ref(), value)?;
                if value.is_empty() {
                    // An empty value only clears the ingest target; a SQL query that was
                    // already set on the statement stays configured.
                    self.ingest.target_table = None;
                } else {
                    // Checked before the statement enters ingest mode, so a name that cannot
                    // work leaves the statement exactly as it was.
                    validate_table_id(&format!("BigQuery option {}", key.as_ref()), &value)?;
                    self.enter_mode(StatementMode::Ingest);
                    self.ingest.target_table = Some(value);
                }
                Ok(())
            }
            OptionStatement::TargetCatalog => {
                let value = string_value(key.as_ref(), value)?;
                self.ingest.target_catalog = if value.is_empty() {
                    None
                } else {
                    Some(ProjectIdentifier::try_new_from(
                        "the adbc.ingest.target_catalog option",
                        value,
                    )?)
                };
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                let value = string_value(key.as_ref(), value)?;
                // Validated when set, like the other dataset options, so an unusable name fails
                // here rather than at the first append.
                validate_dataset_id(&format!("BigQuery option {}", key.as_ref()), &value)?;
                self.ingest.target_db_schema = (!value.is_empty()).then_some(value);
                Ok(())
            }
            OptionStatement::Temporary => {
                if boolean_value(key.as_ref(), value)? {
                    Err(not_implemented(format!(
                        "BigQuery option {} cannot be enabled: temporary tables are not \
                         implemented for BigQuery bulk ingest; ingest into a named table instead",
                        key.as_ref()
                    )))
                } else {
                    Ok(())
                }
            }
            // The spec default "false" is what the statement already does.
            OptionStatement::Incremental => {
                if boolean_value(key.as_ref(), value)? {
                    Err(not_implemented(format!(
                        "BigQuery option {} cannot be enabled; AdbcStatementExecutePartitions \
                         returns all partitions together once the query completes",
                        key.as_ref()
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Err(option_set_refusal("statement", key.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            key if is_bind_by_name_option(key) => {
                Ok(boolean_option(self.parameter_mode == ParameterMode::Named))
            }
            key if let Some(option) = statement_option_key(key) => self
                .bigquery_option_string(option, key.as_ref())?
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionStatement::IngestMode => Ok(String::from(self.ingest.mode)),
            OptionStatement::TargetTable => self
                .ingest
                .target_table
                .clone()
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionStatement::TargetCatalog => self
                .ingest
                .target_catalog
                .as_deref()
                .map(str::to_owned)
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionStatement::TargetDbSchema => self
                .ingest
                .target_db_schema
                .clone()
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionStatement::Temporary | OptionStatement::Incremental => {
                Ok(ADBC_OPTION_VALUE_DISABLED.to_owned())
            }
            _ => Err(wrong_accessor(&key, "a string")),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        match statement_option_key(&key).and_then(|option| self.dbt.option_bytes(option)) {
            Some(value) => value
                .map(<[u8]>::to_vec)
                .ok_or_else(|| option_not_set(key.as_ref())),
            None => Err(wrong_accessor(&key, "bytes")),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match statement_option_key(&key) {
            Some(BigQueryStatementOption::MaximumBytesBilled) => {
                Ok(self.query_options.maximum_bytes_billed)
            }
            Some(BigQueryStatementOption::JobTimeout) => Ok(self.query_options.job_timeout_ms),
            Some(BigQueryStatementOption::MaximumBillingTier) => {
                Err(not_found(maximum_billing_tier_unsupported(key.as_ref())))
            }
            Some(BigQueryStatementOption::PrefetchConcurrency) => Ok(self
                .context
                .result_options
                .prefetch_concurrency_option_value()),
            Some(option) if let Some(value) = self.dbt.option_int(option) => Ok(value),
            _ => Err(wrong_accessor(&key, "an integer")),
        }
    }

    /// `adbc.statement.exec.progress` and `adbc.statement.exec.max_progress`: bytes processed as
    /// reported by responses already received, the maximum staying adbc.h's "not known" 0.0 until
    /// a terminal response. Neither getter sends a request, and the export layer answers both
    /// without the statement's state lock, so a monitoring thread may poll during an execute.
    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match key {
            OptionStatement::Progress => Ok(self.progress.current()),
            OptionStatement::MaxProgress => Ok(self.progress.maximum()),
            key => Err(wrong_accessor(&key, "a double")),
        }
    }
}

impl BigQueryStatement {
    fn set_bigquery_option(
        &mut self,
        option: BigQueryStatementOption,
        key: &OptionStatement,
        value: OptionValue,
    ) -> Result<()> {
        match option {
            dbt_statement_option!() => {
                let defaults = self.context.statement_defaults(&self.query_options);
                let mode = self.dbt.set_option(option, key.as_ref(), value, defaults)?;
                if let Some(mode) = mode {
                    self.enter_mode(StatementMode::Dbt(mode));
                }
            }
            BigQueryStatementOption::BulkIngestMethod => {
                let value = string_value(key.as_ref(), value)?;
                parse_ingest_method(key.as_ref(), &value)?;
            }
            BigQueryStatementOption::ParameterMode => {
                self.parameter_mode = enum_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::DestinationTable => {
                let value = string_value(key.as_ref(), value)?;
                self.query_options.destination_table =
                    self.query_defaults().table_option(key.as_ref(), &value)?;
            }
            BigQueryStatementOption::DefaultProjectId => {
                let value = string_value(key.as_ref(), value)?;
                if value.is_empty()
                    && self.query_options.default_dataset_id.is_some()
                    && self.context.current_catalog.is_none()
                    && self.context.billing_project_id.is_none()
                {
                    return Err(invalid_state(format!(
                        "BigQuery option {} cannot be cleared while the statement option {} is \
                         set and no other project is configured; clear {} first, or set the \
                         connection option adbc.connection.catalog or the database option \
                         bigquery.project_id",
                        key.as_ref(),
                        BigQueryStatementOption::DefaultDatasetId.as_str(),
                        BigQueryStatementOption::DefaultDatasetId.as_str(),
                    )));
                }
                self.query_options.default_project_id = if value.is_empty() {
                    None
                } else {
                    Some(ProjectIdentifier::try_new_from(
                        &format!("the {} option", key.as_ref()),
                        value,
                    )?)
                };
            }
            BigQueryStatementOption::DefaultDatasetId => {
                let value = string_value(key.as_ref(), value)?;
                if !value.is_empty() && self.query_defaults().catalog.is_none() {
                    return Err(dataset_needs_a_project(
                        &format!("the statement option {}", key.as_ref()),
                        &[
                            "the statement option bigquery.query.default_project_id",
                            "the connection option adbc.connection.catalog",
                            "the database option bigquery.project_id",
                        ],
                    ));
                }
                // Validated when set, like the other dataset options, so the error names the
                // option rather than surfacing from the service later.
                validate_dataset_id(&format!("BigQuery option {}", key.as_ref()), &value)?;
                self.query_options.default_dataset_id =
                    (!value.is_empty()).then(|| Arc::from(value));
            }
            BigQueryStatementOption::CreateDisposition => {
                self.query_options.create_disposition = enum_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::WriteDisposition => {
                self.query_options.write_disposition = enum_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::Priority => {
                self.query_options.priority = enum_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::MaximumBytesBilled => {
                self.query_options.maximum_bytes_billed = override_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::JobTimeout => {
                self.query_options.job_timeout_ms = override_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::Reservation => {
                let value = string_value(key.as_ref(), value)?;
                self.query_options.reservation = (!value.is_empty()).then(|| Arc::from(value));
            }
            BigQueryStatementOption::Labels => {
                let value = string_value(key.as_ref(), value)?;
                self.query_options.labels = json_object_value(
                    key.as_ref(),
                    &value,
                    "a JSON object of string label values",
                )?;
            }
            BigQueryStatementOption::LinkFailedJob => {
                self.link_failed_job = boolean_value(key.as_ref(), value)?;
            }
            // Refused before its value is parsed: no value of a removed option is valid.
            BigQueryStatementOption::MaximumBillingTier => {
                return Err(not_implemented(maximum_billing_tier_unsupported(
                    key.as_ref(),
                )));
            }
            BigQueryStatementOption::PrefetchConcurrency => {
                self.context
                    .result_options
                    .set_prefetch_concurrency(key.as_ref(), integer_value(key.as_ref(), value)?)?;
            }
            BigQueryStatementOption::UnorderedRead => {
                self.context
                    .result_options
                    .set_unordered_read(boolean_value(key.as_ref(), value)?);
            }
            BigQueryStatementOption::UseStorageApiDisabledClient => {
                // Only the connection setter clears the shared per-project Storage Read refusal.
                self.context
                    .result_options
                    .set_use_storage_read(!boolean_value(key.as_ref(), value)?);
            }
            BigQueryStatementOption::CreateSession => {
                self.query_options.create_session = boolean_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::DisableQueryCache => {
                self.query_options.disable_query_cache = boolean_value(key.as_ref(), value)?;
            }
            BigQueryStatementOption::DryRun => {
                self.query_options.dry_run = boolean_value(key.as_ref(), value)?;
            }
        }
        Ok(())
    }

    /// Returns `None` for unset options and the effective value for options with defaults.
    fn bigquery_option_string(
        &self,
        option: BigQueryStatementOption,
        key: &str,
    ) -> Result<Option<String>> {
        let value = match option {
            dbt_statement_option!() => return self.dbt.option_string(option, key),
            BigQueryStatementOption::BulkIngestMethod => {
                BigQueryBulkIngestMethod::StorageWrite.as_str().to_owned()
            }
            BigQueryStatementOption::ParameterMode => self.parameter_mode.as_str().to_owned(),
            BigQueryStatementOption::DestinationTable => {
                return Ok(self
                    .query_options
                    .destination_table
                    .as_ref()
                    .map(QualifiedTableName::as_option_value));
            }
            // The defaults a query resolves names against, the connection's included.
            BigQueryStatementOption::DefaultProjectId => {
                return Ok(self
                    .current_context()
                    .statement_defaults(&self.query_options)
                    .catalog
                    .map(|project_id| project_id.as_str().to_owned()));
            }
            BigQueryStatementOption::DefaultDatasetId => {
                return Ok(self
                    .current_context()
                    .statement_defaults(&self.query_options)
                    .db_schema
                    .map(str::to_owned));
            }
            BigQueryStatementOption::CreateDisposition => {
                self.query_options.create_disposition.as_str().to_owned()
            }
            BigQueryStatementOption::WriteDisposition => {
                self.query_options.write_disposition.as_str().to_owned()
            }
            BigQueryStatementOption::Priority => self.query_options.priority.as_str().to_owned(),
            BigQueryStatementOption::CreateSession => {
                boolean_option(self.query_options.create_session)
            }
            BigQueryStatementOption::DisableQueryCache => {
                boolean_option(self.query_options.disable_query_cache)
            }
            BigQueryStatementOption::DryRun => boolean_option(self.query_options.dry_run),
            BigQueryStatementOption::Reservation => {
                return Ok(self.query_options.reservation.as_deref().map(str::to_owned));
            }
            BigQueryStatementOption::Labels => {
                return self
                    .query_options
                    .labels
                    .as_ref()
                    .map(|labels| option_value_json(labels.as_ref(), "job labels"))
                    .transpose();
            }
            BigQueryStatementOption::LinkFailedJob => boolean_option(self.link_failed_job),
            BigQueryStatementOption::UnorderedRead => {
                boolean_option(self.context.result_options.unordered_read())
            }
            BigQueryStatementOption::UseStorageApiDisabledClient => {
                boolean_option(!self.context.result_options.use_storage_read())
            }
            // The options this driver does not spell as a string: the integer ones, which read
            // back through `get_option_int`.
            BigQueryStatementOption::JobTimeout
            | BigQueryStatementOption::MaximumBillingTier
            | BigQueryStatementOption::MaximumBytesBilled
            | BigQueryStatementOption::PrefetchConcurrency => {
                return Err(wrong_option_type(key, option.option_kind(), "a string"));
            }
        };
        Ok(Some(value))
    }
}

/// An integer query option whose override zero clears and whose negative values mean nothing.
fn override_value(key: &str, value: OptionValue) -> Result<i64> {
    let value = integer_value(key, value)?;
    if value < 0 {
        return Err(invalid_argument(format!(
            "BigQuery option {key} must not be negative; zero clears the override"
        )));
    }
    Ok(value)
}

fn statement_option_key(key: &OptionStatement) -> Option<BigQueryStatementOption> {
    let OptionStatement::Other(name) = key else {
        return None;
    };
    BigQueryStatementOption::from_name(name)
}

fn is_bind_by_name_option(key: &OptionStatement) -> bool {
    matches!(key, OptionStatement::Other(name) if name == OPTION_BIND_BY_NAME)
}

fn statement_option_kind(key: &OptionStatement) -> Option<OptionKind> {
    statement_option_key(key)
        .map(BigQueryStatementOption::option_kind)
        .or_else(|| {
            (is_bind_by_name_option(key)
                || matches!(
                    key,
                    OptionStatement::IngestMode
                        | OptionStatement::TargetTable
                        | OptionStatement::TargetCatalog
                        | OptionStatement::TargetDbSchema
                        | OptionStatement::Temporary
                        | OptionStatement::Incremental
                ))
            .then_some(OptionKind::String)
        })
}

/// A getter's refusal of a key it does not serve: a known option of another type reports both
/// types, anything else is an unknown option.
///
/// The double-typed progress options are handled here because [`OptionKind`] has no double
/// variant.
fn wrong_accessor(key: &OptionStatement, requested: &str) -> adbc_core::error::Error {
    if matches!(
        key,
        OptionStatement::Progress | OptionStatement::MaxProgress
    ) {
        return not_found(format!(
            "BigQuery option {} has type double, but was requested as {requested}",
            key.as_ref()
        ));
    }
    unknown_or_wrong_type(
        "statement",
        key.as_ref(),
        statement_option_kind(key),
        requested,
    )
}

/// The deprecated option's refusal: `NOT_IMPLEMENTED` from the setter, and `NOT_FOUND` from the
/// getters, the only failure adbc.h documents for `AdbcStatementGetOptionInt`.
fn maximum_billing_tier_unsupported(key: &str) -> String {
    format!(
        "BigQuery option {key} is deprecated by Google and is not supported; cap query cost with \
         {} instead",
        BigQueryStatementOption::MaximumBytesBilled.as_str()
    )
}
