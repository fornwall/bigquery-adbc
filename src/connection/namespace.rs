//! The connection's current catalog and schema: the `adbc.connection.catalog` and
//! `adbc.connection.db_schema` options, and how statements see them.

use std::sync::{Arc, PoisonError, RwLock};

use adbc_core::error::Result;

use super::BigQueryConnection;
use crate::cancellation::Operation;
use crate::identifiers::{ProjectIdentifier, validate_dataset_id};
use crate::options::dataset_needs_a_project;
use crate::query::ExecutionContext;
use crate::rest::metadata::DatasetId;

/// The connection's current catalog and schema, shared with every statement it created.
///
/// Statements read it when they execute, not when they are created, so a change also applies to
/// existing statements (Python's dbapi keeps one statement per cursor).
#[derive(Debug, Default)]
pub(crate) struct CurrentNamespace(RwLock<Namespace>);

/// The value of a [`CurrentNamespace`] at one point in time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Namespace {
    catalog: Option<ProjectIdentifier>,
    schema: Option<Arc<str>>,
}

impl CurrentNamespace {
    // The lock guards a value written only in whole assignments, so a poisoned guard is
    // recovered rather than reported.
    pub(crate) fn get(&self) -> Namespace {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn publish(&self, context: &ExecutionContext) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Namespace {
            catalog: context.current_catalog.clone(),
            schema: context.current_schema.clone(),
        };
    }
}

impl Namespace {
    pub(crate) fn apply_to(self, context: &mut ExecutionContext) {
        context.current_catalog = self.catalog;
        context.current_schema = self.schema;
    }
}

impl BigQueryConnection {
    /// Sets the current catalog after proving the project exists and is readable (see
    /// `probe_project`). The empty string clears it.
    pub(super) fn set_current_catalog(
        &mut self,
        operation: &Operation,
        project_id: String,
    ) -> Result<()> {
        if self.context.current_catalog.as_deref().unwrap_or_default() == project_id {
            return Ok(());
        }
        let catalog = if project_id.is_empty() {
            None
        } else {
            let project_id =
                ProjectIdentifier::try_new_from("the adbc.connection.catalog option", project_id)?;
            self.probe_project(operation, &project_id)
                .map_err(|source| {
                    source.into_error(&format!(
                        "failed to validate the current BigQuery catalog {project_id}"
                    ))
                })?;
            Some(project_id)
        };
        self.context.current_catalog = catalog;
        self.schema_predates_catalog = self.context.current_schema.is_some();
        self.namespace.publish(&self.context);
        Ok(())
    }

    /// Sets the current schema after proving the dataset exists in [`Self::name_catalog`]. The
    /// empty string clears it.
    pub(super) fn set_current_schema(
        &mut self,
        operation: &Operation,
        dataset_id: String,
    ) -> Result<()> {
        validate_dataset_id("BigQuery option adbc.connection.db_schema", &dataset_id)?;
        if self.context.current_schema.as_deref().unwrap_or_default() == dataset_id
            && (dataset_id.is_empty() || !self.schema_predates_catalog)
        {
            return Ok(());
        }
        if !dataset_id.is_empty() {
            let project_id = self.name_catalog().ok_or_else(|| {
                dataset_needs_a_project(
                    "the connection option adbc.connection.db_schema",
                    &[
                        "the connection option adbc.connection.catalog",
                        "the database option bigquery.project_id",
                    ],
                )
            })?;
            self.context
                .runtime
                .block_on(self.metadata_client()?.get_dataset(
                    Some(operation),
                    DatasetId {
                        project_id: project_id.as_str(),
                        dataset_id: &dataset_id,
                    },
                ))
                .map_err(|source| {
                    source.into_error(&format!(
                        "failed to validate the current BigQuery schema {project_id}.{dataset_id}"
                    ))
                })?;
        }
        self.context.current_schema = (!dataset_id.is_empty()).then(|| Arc::from(dataset_id));
        self.schema_predates_catalog = false;
        self.namespace.publish(&self.context);
        Ok(())
    }

    /// The project an unqualified dataset resolves in: the current catalog, else the billing
    /// project, as for a statement's query defaults.
    pub(super) fn name_catalog(&self) -> Option<&ProjectIdentifier> {
        self.context
            .current_catalog
            .as_ref()
            .or(self.context.billing_project_id.as_ref())
    }
}
