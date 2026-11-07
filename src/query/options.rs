//! Query defaults, statement options and qualified table names.

use std::sync::Arc;

use adbc_core::error::Result;

use crate::clients::DatasetKey;
use crate::error::invalid_argument;
use crate::identifiers::{ProjectIdentifier, validate_dataset_id, validate_table_id};
use crate::rest::metadata::TableId;
use crate::{BigQueryCreateDisposition, BigQueryQueryPriority, BigQueryWriteDisposition};
use crate::{BigQueryDatabaseOption, BigQueryStatementOption};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryDefaults<'a> {
    pub(crate) catalog: Option<&'a ProjectIdentifier>,
    pub(crate) db_schema: Option<&'a str>,
}

impl QueryDefaults<'_> {
    /// Resolves a table option against these defaults; an empty value clears it.
    pub(crate) fn table_option(self, key: &str, value: &str) -> Result<Option<QualifiedTableName>> {
        if value.is_empty() {
            return Ok(None);
        }
        QualifiedTableName::parse_option(key, value, self.catalog, self.db_schema).map(Some)
    }
}

/// Caller-controlled options for executing SQL set on an ADBC statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StatementQueryOptions {
    pub(crate) default_project_id: Option<ProjectIdentifier>,
    pub(crate) default_dataset_id: Option<Arc<str>>,
    pub(crate) destination_table: Option<QualifiedTableName>,
    /// Effective creation behavior, omitted from query requests at its default so it does not
    /// select the `jobs.insert` path.
    pub(crate) create_disposition: BigQueryCreateDisposition,
    /// Effective write behavior, likewise omitted from query requests at its default.
    pub(crate) write_disposition: BigQueryWriteDisposition,
    pub(crate) maximum_bytes_billed: i64,
    pub(crate) job_timeout_ms: i64,
    pub(crate) reservation: Option<Arc<str>>,
    /// Job labels, kept sorted so `get_option` and the request body are deterministic.
    pub(crate) labels: Option<Arc<std::collections::BTreeMap<String, String>>>,
    /// Effective scheduling priority, omitted from query requests at its default.
    pub(crate) priority: BigQueryQueryPriority,
    pub(crate) create_session: bool,
    pub(crate) disable_query_cache: bool,
    pub(crate) dry_run: bool,
    /// Submit through `jobs.insert` under a driver-chosen job ID, whatever else is set.
    pub(crate) named_job: bool,
}

/// A validated `project.dataset.table` name set through an option: a destination, a table copy's
/// source, or an authorized view. [`Self::id`] borrows it as the [`TableId`] a request carries.
/// Ordering makes authorized-view option output deterministic.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct QualifiedTableName {
    project_id: ProjectIdentifier,
    dataset_id: Arc<str>,
    table_id: Arc<str>,
}

impl QualifiedTableName {
    /// Parses a `[[project.]dataset.]table` reference set through `key`. Errors name `key`, so a
    /// copy endpoint is never reported as a bad destination table.
    ///
    /// Split from the right because the project can contain dots, as in a domain-scoped ID.
    /// Project validity belongs to BigQuery; only resource-segment safety is checked locally.
    /// The table ID gets [`validate_table_id`]'s checks, which admit partition decorators
    /// (`model$20240101`).
    pub(crate) fn parse_option(
        key: &str,
        value: &str,
        default_project_id: Option<&ProjectIdentifier>,
        default_dataset_id: Option<&str>,
    ) -> Result<Self> {
        let mut parts = value.rsplitn(3, '.');
        let table_id = parts.next().expect("split always returns one element");
        let dataset_id = parts.next();
        let project_id = parts.next();
        if table_id.is_empty()
            || dataset_id.is_some_and(str::is_empty)
            || project_id.is_some_and(str::is_empty)
        {
            return Err(invalid_table_reference(key));
        }

        let project_id = match project_id {
            Some(project_id) => {
                ProjectIdentifier::try_new_from(&format!("the {key} option"), project_id)?
            }
            None => default_project_id
                .cloned()
                .ok_or_else(|| missing_table_default(key, MissingDefault::Project))?,
        };
        let dataset_id = match dataset_id {
            Some(dataset_id) => {
                validate_dataset_id(
                    &format!("the dataset ID in BigQuery option {key}"),
                    dataset_id,
                )?;
                dataset_id
            }
            None => default_dataset_id
                .ok_or_else(|| missing_table_default(key, MissingDefault::Dataset))?,
        };
        validate_table_id(&format!("the table ID in BigQuery option {key}"), table_id)?;
        Ok(Self {
            project_id,
            dataset_id: Arc::from(dataset_id),
            table_id: Arc::from(table_id),
        })
    }

    pub(crate) fn as_option_value(&self) -> String {
        format!("{}.{}.{}", self.project_id, self.dataset_id, self.table_id)
    }

    pub(crate) fn project_id(&self) -> &ProjectIdentifier {
        &self.project_id
    }

    pub(crate) fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    pub(crate) fn table_id(&self) -> &str {
        &self.table_id
    }

    pub(crate) fn dataset_key(&self) -> DatasetKey {
        (self.project_id.clone(), self.dataset_id.clone())
    }

    pub(crate) fn id(&self) -> TableId<'_> {
        TableId {
            project_id: self.project_id.as_str(),
            dataset_id: &self.dataset_id,
            table_id: &self.table_id,
        }
    }
}

#[derive(Clone, Copy)]
enum MissingDefault {
    Project,
    Dataset,
}

fn missing_table_default(key: &str, missing: MissingDefault) -> adbc_core::error::Error {
    let (name, form, statement_option, database_option) = match missing {
        MissingDefault::Project => (
            "project",
            "project.dataset.table",
            BigQueryStatementOption::DefaultProjectId,
            BigQueryDatabaseOption::ProjectId,
        ),
        MissingDefault::Dataset => (
            "dataset",
            "dataset.table",
            BigQueryStatementOption::DefaultDatasetId,
            BigQueryDatabaseOption::DatasetId,
        ),
    };
    invalid_argument(format!(
        "BigQuery option {key} names no {name}, and no default is configured; write it as {form}, \
         or set {} (or {})",
        statement_option.as_str(),
        database_option.as_str()
    ))
}

fn invalid_table_reference(key: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery option {key} is not a table reference; expected table, dataset.table, or project.dataset.table"
    ))
}

#[cfg(test)]
mod tests;
