//! Connection option dispatch and pre-initialization validation.

use adbc_core::Optionable;
use adbc_core::constants::{
    ADBC_OPTION_ISOLATION_LEVEL_DEFAULT, ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
    ADBC_OPTION_VALUE_DISABLED,
};
use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionValue};

use super::BigQueryConnection;
use crate::error::{
    not_implemented, option_get_refusal, option_not_set, option_set_refusal, unknown_or_wrong_type,
    wrong_option_type,
};
use crate::ingest::parse_ingest_method;
use crate::options::OptionKind;
use crate::options::{boolean_option, boolean_value, integer_value, string_value};
use crate::{BigQueryConnectionOption, BigQueryDatabaseOption};

impl Optionable for BigQueryConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        if let Some(refusal) = connection_set_refusal(&key) {
            return Err(refusal);
        }
        if let Some(option) = connection_option_key(&key) {
            match option {
                BigQueryConnectionOption::GetObjectsSkipTableMetadata => {
                    self.get_objects_skip_table_metadata = boolean_value(key.as_ref(), value)?;
                }
                BigQueryConnectionOption::GetObjectsDisableWildcards => {
                    self.get_objects_disable_wildcards = boolean_value(key.as_ref(), value)?;
                }
                BigQueryConnectionOption::BulkIngestMethod => {
                    let value = string_value(key.as_ref(), value)?;
                    self.ingest_method = parse_ingest_method(key.as_ref(), &value)?;
                }
                BigQueryConnectionOption::PrefetchConcurrency => {
                    self.context.result_options.set_prefetch_concurrency(
                        key.as_ref(),
                        integer_value(key.as_ref(), value)?,
                    )?;
                }
                BigQueryConnectionOption::UnorderedRead => {
                    self.context
                        .result_options
                        .set_unordered_read(boolean_value(key.as_ref(), value)?);
                }
                BigQueryConnectionOption::UseStorageApiDisabledClient => {
                    self.context
                        .result_options
                        .set_use_storage_read(!boolean_value(key.as_ref(), value)?);
                    // Re-enabling on a connection retries Storage Read after a permission change.
                    // Statement overrides must not reset this shared per-project refusal.
                    if self.context.result_options.use_storage_read()
                        && let Some(project) = self.context.billing_project_id.as_ref()
                    {
                        self.context.state.enable_storage_read(project.as_str());
                    }
                }
            }
            return Ok(());
        }
        let operation = self.cancellation.begin();
        let result = match key {
            OptionConnection::AutoCommit => {
                self.set_autocommit(boolean_value(key.as_ref(), value)?)
            }
            OptionConnection::CurrentCatalog => {
                self.set_current_catalog(&operation, string_value(key.as_ref(), value)?)
            }
            OptionConnection::CurrentSchema => {
                self.set_current_schema(&operation, string_value(key.as_ref(), value)?)
            }
            // Accept the supported default; enabling read-only mode is unsupported.
            OptionConnection::ReadOnly => {
                if boolean_value(key.as_ref(), value)? {
                    Err(not_implemented(format!(
                        "BigQuery option {} cannot be enabled: read-only connections are not \
                         implemented; restrict the credential's IAM permissions instead",
                        key.as_ref()
                    )))
                } else {
                    Ok(())
                }
            }
            // Both spellings describe BigQuery's snapshot isolation.
            OptionConnection::IsolationLevel => {
                let value = string_value(key.as_ref(), value)?;
                if value == ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
                    || value == ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT
                {
                    Ok(())
                } else {
                    Err(not_implemented(format!(
                        "BigQuery option {} must be \"{}\" or \"{}\", because BigQuery \
                         transactions are snapshot-isolated",
                        key.as_ref(),
                        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
                        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
                    )))
                }
            }
            _ => Err(option_set_refusal("connection", key.as_ref())),
        };
        operation.finish();
        result
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        if let Some(standard) = current_namespace_alias(&key) {
            return self.get_option_string(standard);
        }
        if let Some(option) = connection_option_key(&key) {
            return match option {
                BigQueryConnectionOption::GetObjectsSkipTableMetadata => {
                    Ok(boolean_option(self.get_objects_skip_table_metadata))
                }
                BigQueryConnectionOption::GetObjectsDisableWildcards => {
                    Ok(boolean_option(self.get_objects_disable_wildcards))
                }
                BigQueryConnectionOption::BulkIngestMethod => {
                    Ok(self.ingest_method.as_str().to_owned())
                }
                BigQueryConnectionOption::PrefetchConcurrency => {
                    Err(wrong_option_type(key.as_ref(), OptionKind::Int, "a string"))
                }
                BigQueryConnectionOption::UnorderedRead => {
                    Ok(boolean_option(self.context.result_options.unordered_read()))
                }
                BigQueryConnectionOption::UseStorageApiDisabledClient => Ok(boolean_option(
                    !self.context.result_options.use_storage_read(),
                )),
            };
        }
        match key {
            OptionConnection::AutoCommit => Ok(boolean_option(self.transaction.is_autocommit()?)),
            // The effective value: a cleared catalog resolves names in the billing project. Only
            // a connection with neither reads as NOT_FOUND, never "", as every unset option does.
            OptionConnection::CurrentCatalog => self
                .context
                .current_catalog
                .as_deref()
                .or(self.context.billing_project_id.as_deref())
                .map(str::to_owned)
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionConnection::CurrentSchema => self
                .context
                .current_schema
                .as_deref()
                .map(str::to_owned)
                .ok_or_else(|| option_not_set(key.as_ref())),
            OptionConnection::ReadOnly => Ok(ADBC_OPTION_VALUE_DISABLED.to_owned()),
            OptionConnection::IsolationLevel => Ok(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT.to_owned()),
            _ => Err(option_get_refusal("connection", key.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(unknown_or_wrong_type(
            "connection",
            key.as_ref(),
            connection_option_kind(&key),
            "bytes",
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match connection_option_key(&key) {
            Some(BigQueryConnectionOption::PrefetchConcurrency) => Ok(self
                .context
                .result_options
                .prefetch_concurrency_option_value()),
            _ => Err(unknown_or_wrong_type(
                "connection",
                key.as_ref(),
                connection_option_kind(&key),
                "an integer",
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(unknown_or_wrong_type(
            "connection",
            key.as_ref(),
            connection_option_kind(&key),
            "a double",
        ))
    }
}

/// Classifies known options so FFI calls can reject unknown keys before `Init`.
pub(crate) fn connection_option_kind(key: &OptionConnection) -> Option<OptionKind> {
    connection_option_key(key)
        .map(BigQueryConnectionOption::option_kind)
        // Read-only namespace aliases are recognized before Init, but cannot be buffered for set.
        .or_else(|| current_namespace_alias(key).map(|_| OptionKind::String))
        .or_else(|| {
            matches!(
                key,
                OptionConnection::AutoCommit
                    | OptionConnection::CurrentCatalog
                    | OptionConnection::CurrentSchema
                    | OptionConnection::ReadOnly
                    | OptionConnection::IsolationLevel
            )
            .then_some(OptionKind::String)
        })
}

/// Rejects read-only aliases consistently before and after FFI initialization.
pub(crate) fn connection_set_refusal(key: &OptionConnection) -> Option<adbc_core::error::Error> {
    let standard = current_namespace_alias(key)?;
    Some(not_implemented(format!(
        "the BigQuery connection option {} is read-only; set {} instead",
        key.as_ref(),
        standard.as_ref(),
    )))
}

/// The standard current-namespace option a BigQuery-spelled key is another name for, if it is one.
fn current_namespace_alias(key: &OptionConnection) -> Option<OptionConnection> {
    let OptionConnection::Other(name) = key else {
        return None;
    };
    match BigQueryDatabaseOption::from_name(name)? {
        BigQueryDatabaseOption::ProjectId => Some(OptionConnection::CurrentCatalog),
        BigQueryDatabaseOption::DatasetId => Some(OptionConnection::CurrentSchema),
        _ => None,
    }
}

fn connection_option_key(key: &OptionConnection) -> Option<BigQueryConnectionOption> {
    let OptionConnection::Other(name) = key else {
        return None;
    };
    BigQueryConnectionOption::from_name(name)
}

#[cfg(test)]
mod tests {
    use crate::{BigQueryConnectionOption, BigQueryStatementOption};

    /// Query and ingest options have statement overrides; metadata options are connection-only.
    #[test]
    fn connection_options_have_statement_overrides_except_for_metadata() {
        for &option in BigQueryConnectionOption::ALL {
            assert_eq!(
                BigQueryStatementOption::from_name(option.as_str())
                    .map(BigQueryStatementOption::as_str),
                (!matches!(
                    option,
                    BigQueryConnectionOption::GetObjectsSkipTableMetadata
                        | BigQueryConnectionOption::GetObjectsDisableWildcards
                ))
                .then_some(option.as_str()),
                "{}",
                option.as_str()
            );
        }
    }
}
