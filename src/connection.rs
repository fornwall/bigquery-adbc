//! The ADBC connection: metadata calls, the current catalog and schema, transactions, and
//! partition reads.

mod arrow;
mod info;
mod namespace;
mod objects;
mod options;
mod sql_pattern;
mod statistics;
mod table_metadata;
mod table_schema;
mod transactions;

pub(crate) use namespace::{CurrentNamespace, Namespace};
#[cfg(feature = "ffi")]
pub(crate) use options::{connection_option_kind, connection_set_refusal};

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth};
use adbc_core::{CancelHandle, Connection};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use crate::BigQueryBulkIngestMethod;
use crate::cancellation::{
    Cancellation, Operation, OperationCancelHandle, buffered_operation_reader, operation_reader,
};
use crate::clients::ClientState;
use crate::connection::info::{
    MATERIALIZED_VIEW, SNAPSHOT, TABLE, VIEW, info_reader, table_types_reader,
};
use crate::connection::objects::{ObjectCatalog, ObjectDbSchema, object_table, objects_reader};
use crate::connection::sql_pattern::SqlPattern;
use crate::connection::statistics::{
    StatisticCatalog, StatisticDbSchema, statistic_names_reader, statistics_reader,
    table_statistics,
};
use crate::error::error;
use crate::identifiers::ProjectIdentifier;
use crate::pipeline::QueryResultOptions;
use crate::query::{ExecutionContext, read_query_partition};
use crate::rest::metadata::{MetadataClient, MetadataError, TableId, TableView};
use crate::rest::model::TableSchema;
use crate::runtime::SharedRuntime;
use crate::statement::BigQueryStatement;
use crate::transaction::TransactionState;
use crate::types::schema::convert_schema;
use table_metadata::{DatasetScan, ScannedTable, TableScan, names_no_table, table_metadata_error};
use table_schema::{resolve_table_reference, table_schema_metadata};

/// An ADBC connection: a current catalog and schema, transaction state, and the metadata calls.
///
/// Connections from one [`BigQueryDatabase`](crate::BigQueryDatabase) share its clients.
#[derive(Debug)]
pub struct BigQueryConnection {
    /// The connection-scoped execution state each new statement snapshots at creation, except for
    /// the current catalog and schema, which statements read through `namespace`.
    context: ExecutionContext,
    namespace: Arc<CurrentNamespace>,
    /// The current catalog changed after the current schema was set, so naming the same schema
    /// again validates it in the new catalog instead of being a no-op.
    schema_predates_catalog: bool,
    ingest_method: BigQueryBulkIngestMethod,
    get_objects_skip_table_metadata: bool,
    get_objects_disable_wildcards: bool,
    transaction: Arc<TransactionState>,
    cancellation: Arc<Cancellation>,
}

impl BigQueryConnection {
    pub(crate) fn new(
        runtime: SharedRuntime,
        state: Arc<ClientState>,
        billing_project_id: Option<ProjectIdentifier>,
        dataset_id: Option<Arc<str>>,
        location: Option<Arc<str>>,
    ) -> Self {
        let current_catalog = billing_project_id.clone();
        let cancellation = Cancellation::new(runtime.clone(), state.clone(), "connection");
        let context = ExecutionContext {
            runtime,
            state,
            billing_project_id,
            current_catalog,
            current_schema: dataset_id,
            location,
            result_options: QueryResultOptions::default(),
        };
        let namespace = Arc::new(CurrentNamespace::default());
        namespace.publish(&context);
        Self {
            context,
            namespace,
            schema_predates_catalog: false,
            ingest_method: BigQueryBulkIngestMethod::default(),
            get_objects_skip_table_metadata: true,
            get_objects_disable_wildcards: false,
            transaction: Arc::new(TransactionState::default()),
            cancellation,
        }
    }

    fn metadata_client(&self) -> Result<MetadataClient> {
        self.context.state.metadata_client(&self.context.runtime)
    }

    /// Proves `project_id` exists and is readable with a one-dataset listing.
    fn probe_project(
        &self,
        operation: &Operation,
        project_id: &str,
    ) -> std::result::Result<(), MetadataError> {
        let client = self.metadata_client().map_err(MetadataError::Failed)?;
        self.context
            .runtime
            .block_on(client.list_datasets(Some(operation), project_id, Some(1), ""))
            .map(drop)
    }

    /// The single catalog the metadata listings enumerate, or `None` when they list nothing.
    ///
    /// A wildcard-free filter enumerates that project, current or not (G75); a foreign project is
    /// first probed with a one-item dataset listing so an absent one yields no catalog rather than
    /// an empty one. A wildcard filter, or none, considers only the current project (G34):
    /// discovering visible projects needs the Resource Manager API. The current project is
    /// [`Self::name_catalog`], the one `adbc.connection.catalog` reads back.
    fn metadata_catalog(
        &self,
        operation: &Operation,
        catalog: Option<SqlPattern<'_>>,
    ) -> Result<Option<ProjectIdentifier>> {
        let current = self.name_catalog();
        let Some(pattern) = catalog else {
            return Ok(current.cloned());
        };
        // An empty filter matches no name, so it lists nothing rather than failing validation.
        if let Some(literal) = pattern.exact().filter(|it| !it.is_empty()) {
            if current.is_some_and(|project| project.as_str() == literal) {
                return Ok(current.cloned());
            }
            // Structurally unsafe names cannot identify a catalog. BigQuery validates other
            // spellings, including numeric and domain-scoped projects.
            let Ok(project_id) = ProjectIdentifier::try_new(literal) else {
                return Ok(None);
            };
            return match self.probe_project(operation, &project_id) {
                Ok(()) => Ok(Some(project_id)),
                Err(source) if source.is_not_found() || source.is_invalid_project() => Ok(None),
                Err(source) => Err(source.into_error(&format!(
                    "failed to validate the BigQuery catalog filter {project_id}"
                ))),
            };
        }
        Ok(current
            .filter(|project_id| pattern.matches(project_id))
            .cloned())
    }

    /// The datasets a metadata listing covers, each with the tables `scan` kept from it, or with
    /// `None` when there is no scan (schema depth).
    ///
    /// When the schema filter names one dataset, its tables are scanned directly, since a
    /// successful scan also proves the dataset exists, and the tables it finds name the dataset as
    /// BigQuery resolved the filter. The `datasets.get` a listing starts with is spent only when
    /// the scan could not prove that or found no table.
    fn scan_schema_objects(
        &self,
        operation: &Operation,
        project_id: &ProjectIdentifier,
        db_schema: Option<SqlPattern<'_>>,
        scan: Option<TableScan<'_>>,
    ) -> Result<Vec<(String, Option<Vec<ScannedTable>>)>> {
        // A scan that can match nothing sends no request, so it proves nothing about the dataset.
        let direct = scan.filter(|scan| {
            !scan.table_types.is_some_and(HashSet::is_empty)
                && !scan
                    .table_name
                    .and_then(SqlPattern::exact)
                    .is_some_and(names_no_table)
        });
        let exact_dataset = db_schema
            .and_then(SqlPattern::exact)
            .filter(|dataset_id| !dataset_id.is_empty());
        if let (Some(scan), Some(dataset_id)) = (direct, exact_dataset) {
            let scanned = self
                .scan_tables(operation, project_id, &[dataset_id.to_owned()], scan)?
                .pop();
            return match scanned {
                Some(DatasetScan::Tables {
                    tables,
                    dataset_id: Some(dataset_id),
                }) => Ok(vec![(dataset_id, Some(tables))]),
                // The dataset has no table to name it, or only the named table is missing or
                // unreadable; the dataset may still exist.
                Some(DatasetScan::Tables {
                    dataset_id: None, ..
                })
                | Some(DatasetScan::ExactTableUnavailable) => Ok(self
                    .list_datasets(operation, project_id, db_schema)?
                    .into_iter()
                    .map(|name| (name, Some(Vec::new())))
                    .collect()),
                Some(DatasetScan::Missing) => Ok(Vec::new()),
                // Left out, as a listing would; its `datasets.get` still runs so that a refusal
                // naming it is reported.
                Some(DatasetScan::Unlistable) | None => {
                    self.list_datasets(operation, project_id, db_schema)?;
                    Ok(Vec::new())
                }
            };
        }

        let mut dataset_ids = self.list_datasets(operation, project_id, db_schema)?;
        dataset_ids.sort_unstable();
        let Some(scan) = scan else {
            return Ok(dataset_ids.into_iter().map(|name| (name, None)).collect());
        };
        let scanned = self.scan_tables(operation, project_id, &dataset_ids, scan)?;
        Ok(dataset_ids
            .into_iter()
            .zip(scanned)
            .filter_map(|(name, scanned)| match scanned {
                DatasetScan::Tables { tables, .. } => Some((name, Some(tables))),
                // The dataset was just listed, so only the table is missing.
                DatasetScan::ExactTableUnavailable => Some((name, Some(Vec::new()))),
                DatasetScan::Missing | DatasetScan::Unlistable => None,
            })
            .collect())
    }
}

impl Connection for BigQueryConnection {
    type StatementType = BigQueryStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(BigQueryStatement::new(
            self.context.clone(),
            self.namespace.clone(),
            self.ingest_method,
            self.transaction.clone(),
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        Box::new(OperationCancelHandle(self.cancellation.clone()))
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let operation = self.cancellation.begin();
        Ok(buffered_operation_reader(
            info_reader(codes.as_ref())?,
            operation,
        ))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let operation = self.cancellation.begin();
        let filter = if self.get_objects_disable_wildcards {
            SqlPattern::literal
        } else {
            SqlPattern::new
        };
        let Some(project_id) = self.metadata_catalog(&operation, catalog.map(filter))? else {
            return Ok(buffered_operation_reader(objects_reader(&[])?, operation));
        };

        if depth == ObjectDepth::Catalogs {
            return Ok(buffered_operation_reader(
                objects_reader(&[ObjectCatalog {
                    name: project_id.to_string(),
                    db_schemas: None,
                }])?,
                operation,
            ));
        }

        let table_types = table_type.map(|types| types.into_iter().collect::<HashSet<_>>());
        let include_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
        let scan = (depth != ObjectDepth::Schemas).then(|| TableScan {
            table_name: table_name.map(filter),
            table_types: table_types.as_ref(),
            describe: if include_columns {
                Some(TableView::Basic)
            } else if !self.get_objects_skip_table_metadata {
                Some(TableView::Constraints)
            } else {
                None
            },
            item: "table metadata",
        });
        let column_name = column_name.map(filter);
        let db_schemas = self
            .scan_schema_objects(&operation, &project_id, db_schema.map(filter), scan)?
            .into_iter()
            .map(|(name, tables)| {
                let tables = tables
                    .map(|tables| {
                        tables
                            .into_iter()
                            .map(|table| {
                                object_table(
                                    table.name,
                                    table.table_type,
                                    table.metadata,
                                    include_columns,
                                    column_name,
                                )
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(ObjectDbSchema { name, tables })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(buffered_operation_reader(
            objects_reader(&[ObjectCatalog {
                name: project_id.to_string(),
                db_schemas: Some(db_schemas),
            }])?,
            operation,
        ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let operation = self.cancellation.begin();
        let (project_id, dataset_id, table_name) = resolve_table_reference(
            self.name_catalog(),
            self.context.current_schema.as_deref(),
            catalog,
            db_schema,
            table_name,
        )?;
        // BASIC includes the schema and dbt's relation configuration. Requesting storage
        // statistics also makes BigQuery collect accounting data this schema never reports.
        let table = self
            .context
            .runtime
            .block_on(self.metadata_client()?.get_table(
                Some(&operation),
                TableId {
                    project_id: project_id.as_str(),
                    dataset_id,
                    table_id: table_name,
                },
                TableView::Configuration,
            ))
            .map_err(|source| {
                table_metadata_error(project_id.as_str(), dataset_id, table_name, source)
            })?;
        // A table created without columns has no `schema`; a view has none only when its SQL no
        // longer compiles, which an empty schema would hide.
        let no_columns = TableSchema::default();
        let table_schema = match &table.schema {
            Some(table_schema) => table_schema,
            None if ![VIEW, MATERIALIZED_VIEW].contains(&table.table_type.as_str()) => &no_columns,
            None => {
                return Err(error(
                    format!(
                        "BigQuery returned view metadata without a schema for \
                         {project_id}.{dataset_id}.{table_name}; its SQL may no longer compile"
                    ),
                    Status::InvalidData,
                ));
            }
        };
        let schema = convert_schema(table_schema)
            .map(|schema| Arc::unwrap_or_clone(schema).with_metadata(table_schema_metadata(table)));
        operation.finish();
        schema
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let operation = self.cancellation.begin();
        Ok(buffered_operation_reader(table_types_reader()?, operation))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let operation = self.cancellation.begin();
        Ok(buffered_operation_reader(
            statistic_names_reader()?,
            operation,
        ))
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        // Statistics come from table metadata; exact values would need a query. adbc.h allows
        // approximate values regardless, and each row reports its own exactness.
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let operation = self.cancellation.begin();
        let Some(project_id) = self.metadata_catalog(&operation, catalog.map(SqlPattern::new))?
        else {
            return Ok(buffered_operation_reader(
                statistics_reader(&[])?,
                operation,
            ));
        };

        // BigQuery describes views and external tables with zero rows and zero bytes, even over
        // a non-empty source (observed, not documented), which would read as exact empty tables.
        // They get no statistics, as with Go's `INFORMATION_SCHEMA.PARTITIONS` source.
        let table_types = HashSet::from([TABLE, MATERIALIZED_VIEW, SNAPSHOT]);
        let scan = TableScan {
            table_name: table_name.map(SqlPattern::new),
            table_types: Some(&table_types),
            describe: Some(TableView::StorageStats),
            item: "table statistics",
        };
        let db_schemas = self
            .scan_schema_objects(
                &operation,
                &project_id,
                db_schema.map(SqlPattern::new),
                Some(scan),
            )?
            .into_iter()
            .map(|(name, tables)| {
                let mut statistics = Vec::new();
                for table in tables.unwrap_or_default() {
                    if let Some(metadata) = &table.metadata {
                        table_statistics(table.name, metadata, &mut statistics);
                    }
                }
                StatisticDbSchema { name, statistics }
            })
            .collect();

        Ok(buffered_operation_reader(
            statistics_reader(&[StatisticCatalog {
                name: project_id.to_string(),
                db_schemas,
            }])?,
            operation,
        ))
    }

    // A cancel finds an operation to accept, but neither call is cancellable; see
    // `TransactionContext::finish`.
    fn commit(&mut self) -> Result<()> {
        let operation = self.cancellation.begin();
        let result = self.finish_transaction(false);
        operation.finish();
        result
    }

    fn rollback(&mut self) -> Result<()> {
        let operation = self.cancellation.begin();
        let result = self.finish_transaction(true);
        operation.finish();
        result
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // A partition stream is meant to be read alongside the connection's other partition
        // streams, so it is not superseded by whatever the connection is asked to do next.
        let operation = self.cancellation.begin_concurrent();
        let reader = read_query_partition(&self.context, &operation, partition.as_ref())?;
        Ok(operation_reader(reader, operation))
    }
}

#[cfg(test)]
mod tests;
