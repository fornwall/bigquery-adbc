//! The dataset and table walks behind `get_objects` and `get_statistics`: `datasets.list`, then
//! `tables.list` and the `tables.get` describe requests, fanned out across every dataset at once.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use tokio::task::JoinSet;

use super::BigQueryConnection;
use crate::cancellation::Operation;
use crate::connection::info::{TABLE, TABLE_TYPES};
use crate::connection::sql_pattern::SqlPattern;
use crate::diag::diag;
use crate::error::{driver_defect, error};
use crate::identifiers::ProjectIdentifier;
use crate::rest::metadata::{
    DatasetId, ListedTable, METADATA_PAGE_SIZE, MetadataClient, MetadataError, Page, Table,
    TableId, TableView,
};
use crate::rest::model::TableReference;
use crate::rest::rejection::{service_error, service_failure};

/// The metadata requests one scan keeps in flight, across all of its datasets.
///
/// Shared rather than per dataset: listing many small datasets is bound by round trips.
const TABLE_METADATA_CONCURRENCY: usize = 16;

/// Guards a paginated listing against a repeated page token, which would otherwise loop forever
/// while the result grows without bound.
#[derive(Debug, Default)]
struct PageTokens(HashSet<String>);

impl PageTokens {
    /// Accepts `token` as the next page to request, or fails if it has already been served.
    fn accept(&mut self, token: &str, subject: &str) -> Result<()> {
        if self.0.insert(token.to_owned()) {
            return Ok(());
        }
        Err(error(
            format!("BigQuery repeated a page token while listing {subject}"),
            Status::InvalidData,
        ))
    }
}

/// Which of a dataset's tables a scan keeps, and what it asks BigQuery about each.
#[derive(Clone, Copy)]
pub(super) struct TableScan<'a> {
    pub(super) table_name: Option<SqlPattern<'a>>,
    pub(super) table_types: Option<&'a [&'a str]>,
    /// The projection each kept table is described with, or `None` to report the listing entry
    /// alone.
    pub(super) describe: Option<TableView>,
    /// What is being read, named in the failures, so a describe that fails under
    /// `get_statistics` does not report itself as a `get_objects` one.
    pub(super) item: &'static str,
}

/// One table a scan kept.
pub(super) struct ScannedTable {
    /// The table's ID as BigQuery reported it; see [`resolved_name`].
    pub(super) name: String,
    /// Normalized by [`bigquery_table_type`].
    pub(super) table_type: &'static str,
    /// The `tables.get` answer, present exactly when the scan describes tables.
    pub(super) metadata: Option<Table>,
}

/// What a scan found in one dataset.
pub(super) enum DatasetScan {
    /// The kept tables, sorted by name.
    Tables {
        tables: Vec<ScannedTable>,
        /// The dataset's ID as the first table BigQuery listed or described named it (see
        /// [`resolved_name`]), or `None` if no table was listed or described.
        dataset_id: Option<String>,
    },
    /// An exact table filter named a table that is missing or unreadable. Unlike an empty
    /// [`Self::Tables`], this does not show that the dataset exists.
    ExactTableUnavailable,
    /// The table listing answered 404: the dataset does not exist, or was dropped after it was
    /// listed.
    Missing,
    /// The principal may see the dataset but not list its tables. It is left out, since an empty
    /// table list would claim it has none.
    Unlistable,
}

enum Request {
    List {
        page_token: String,
    },
    /// `listed_type` is the type from the `tables.list` entry, or `None` for an exact table
    /// filter, which is looked up without a listing.
    Get {
        table_id: String,
        listed_type: Option<&'static str>,
    },
}

enum Response {
    List(std::result::Result<Page<ListedTable>, MetadataError>),
    Get {
        table_id: String,
        listed_type: Option<&'static str>,
        result: std::result::Result<Box<Table>, MetadataError>,
    },
}

impl BigQueryConnection {
    pub(super) fn list_datasets(
        &self,
        operation: &Operation,
        project_id: &str,
        db_schema: Option<SqlPattern<'_>>,
    ) -> Result<Vec<String>> {
        if let Some(dataset_id) = db_schema.and_then(SqlPattern::exact) {
            if dataset_id.is_empty() {
                return Ok(Vec::new());
            }
            let result = self
                .context
                .runtime
                .block_on(self.metadata_client()?.get_dataset(
                    Some(operation),
                    DatasetId {
                        project_id,
                        dataset_id,
                    },
                ));
            return match result {
                Ok(resolved) => Ok(vec![resolved_name(resolved, dataset_id)]),
                Err(source) if source.is_not_found() => Ok(Vec::new()),
                Err(source) => Err(source.into_error(&format!(
                    "failed to get BigQuery dataset {project_id}.{dataset_id}"
                ))),
            };
        }

        let metadata_client = self.metadata_client()?;
        self.context.runtime.block_on(async {
            let mut datasets = Vec::new();
            let mut page_token = String::new();
            let mut seen_tokens = PageTokens::default();
            loop {
                let page = metadata_client
                    .list_datasets(
                        Some(operation),
                        project_id,
                        Some(METADATA_PAGE_SIZE),
                        &page_token,
                    )
                    .await
                    .map_err(|source| {
                        source.into_error(&format!(
                            "failed to list BigQuery datasets in project {project_id}"
                        ))
                    })?;
                for dataset_id in page.items {
                    if dataset_id.is_empty() {
                        return Err(error(
                            format!(
                                "BigQuery returned a dataset without an ID while listing project {project_id}"
                            ),
                            Status::InvalidData,
                        ));
                    }
                    if db_schema.is_none_or(|pattern| pattern.matches(&dataset_id)) {
                        datasets.push(dataset_id);
                    }
                }
                if page.next_page_token.is_empty() {
                    break;
                }
                seen_tokens.accept(
                    &page.next_page_token,
                    &format!("datasets in project {project_id}"),
                )?;
                page_token = page.next_page_token;
            }
            Ok::<_, adbc_core::error::Error>(datasets)
        })
    }

    /// Finds the tables `scan` keeps in each of `dataset_ids`, one result per dataset in the same
    /// order.
    ///
    /// An exact table name is one `tables.get` per dataset; otherwise each `tables.list` is walked
    /// and each kept table described if the scan asks. All requests share one task set bounded by
    /// [`TABLE_METADATA_CONCURRENCY`], with describes scheduled ahead of further listing pages so
    /// the describe queue stays near one page per slot.
    ///
    /// A table that vanishes or denies access to its metadata is dropped; see [`skip_unreadable`].
    pub(super) fn scan_tables(
        &self,
        operation: &Operation,
        project_id: &ProjectIdentifier,
        dataset_ids: &[String],
        scan: TableScan<'_>,
    ) -> Result<Vec<DatasetScan>> {
        let metadata_client = self.metadata_client()?;
        let exact_table = scan.table_name.and_then(SqlPattern::exact);
        let mut datasets = dataset_ids
            .iter()
            .map(|dataset_id| DatasetWalk {
                id: Arc::from(dataset_id.as_str()),
                page_tokens: PageTokens::default(),
                scan: DatasetScan::Tables {
                    tables: Vec::new(),
                    dataset_id: None,
                },
            })
            .collect::<Vec<_>>();
        // Such a table name, or an empty type set, matches nothing.
        if exact_table.is_some_and(names_no_table) || scan.table_types.is_some_and(<[_]>::is_empty)
        {
            return Ok(datasets.into_iter().map(|dataset| dataset.scan).collect());
        }
        let mut lists = VecDeque::new();
        let mut gets = VecDeque::new();
        for index in 0..datasets.len() {
            match exact_table {
                Some(table_id) => gets.push_back((
                    index,
                    Request::Get {
                        table_id: table_id.to_owned(),
                        listed_type: None,
                    },
                )),
                None => lists.push_back((
                    index,
                    Request::List {
                        page_token: String::new(),
                    },
                )),
            }
        }

        self.context.runtime.block_on(async {
            let mut tasks = JoinSet::new();
            loop {
                // Let each in-flight request report cancellation with its retry diagnostics.
                // An outer operation.wait would discard those errors when it drops the tasks.
                while tasks.len() < TABLE_METADATA_CONCURRENCY && !operation.is_cancelled() {
                    let Some((index, request)) = gets.pop_front().or_else(|| lists.pop_front())
                    else {
                        break;
                    };
                    let Some(dataset) = datasets.get(index) else {
                        continue;
                    };
                    if matches!(dataset.scan, DatasetScan::Tables { .. }) {
                        tasks.spawn(send(
                            metadata_client.clone(),
                            project_id.clone(),
                            dataset.id.clone(),
                            scan.describe,
                            index,
                            request,
                            operation.clone(),
                        ));
                    }
                }
                let Some(joined) = tasks.join_next().await else {
                    operation.check()?;
                    break;
                };
                let (index, response) = joined.map_err(|source| {
                    driver_defect(format_args!(
                        "the BigQuery driver's {} task failed: {source}",
                        scan.item
                    ))
                })?;
                let Some(dataset) = datasets.get_mut(index) else {
                    continue;
                };
                let dataset_id = &dataset.id;
                let DatasetScan::Tables {
                    tables,
                    dataset_id: resolved_dataset_id,
                } = &mut dataset.scan
                else {
                    continue;
                };
                match response {
                    Response::List(Err(source)) => {
                        // `tables.list` answers a name that cannot be a dataset ID with 400
                        // `invalid` rather than 404; only an exact filter can supply one. Pages
                        // are requested one after another, so no token yet means the first page.
                        if source.is_not_found()
                            || (dataset.page_tokens.0.is_empty() && rejected_as_invalid(&source))
                        {
                            dataset.scan = DatasetScan::Missing;
                            continue;
                        }
                        dataset.scan = DatasetScan::Unlistable;
                        skip_unreadable(
                            source,
                            &format!("failed to list BigQuery tables in {project_id}.{dataset_id}"),
                        )?;
                    }
                    Response::List(Ok(page)) => {
                        for table in page.items {
                            let reference =
                                listed_table_reference(table.reference, project_id, dataset_id)?;
                            resolved_dataset_id
                                .get_or_insert_with(|| resolved_name(reference.dataset_id, dataset_id));
                            let table_id = reference.table_id;
                            let table_type = bigquery_table_type(&table.table_type);
                            if scan.table_name.is_some_and(|pattern| !pattern.matches(&table_id))
                                || scan
                                    .table_types
                                    .is_some_and(|types| !types.contains(&table_type))
                            {
                                continue;
                            }
                            if scan.describe.is_some() {
                                gets.push_back((
                                    index,
                                    Request::Get {
                                        table_id,
                                        listed_type: Some(table_type),
                                    },
                                ));
                            } else {
                                tables.push(ScannedTable {
                                    name: table_id,
                                    table_type,
                                    metadata: None,
                                });
                            }
                        }
                        if !page.next_page_token.is_empty() {
                            dataset.page_tokens.accept(
                                &page.next_page_token,
                                &format!("tables in {project_id}.{dataset_id}"),
                            )?;
                            lists.push_back((
                                index,
                                Request::List {
                                    page_token: page.next_page_token,
                                },
                            ));
                        }
                    }
                    Response::Get {
                        table_id,
                        listed_type,
                        result,
                    } => {
                        let mut metadata = match result {
                            Ok(metadata) => metadata,
                            Err(source) => {
                                // `tables.get` answers a name that cannot be a table ID with
                                // 400 `invalid` rather than 404, and an exact filter naming one
                                // matches no table.
                                if !(listed_type.is_none() && rejected_as_invalid(&source)) {
                                    skip_unreadable(
                                        source,
                                        &format!(
                                            "failed to get BigQuery {} for {project_id}.{dataset_id}.{table_id}",
                                            scan.item
                                        ),
                                    )?;
                                }
                                if listed_type.is_none() {
                                    dataset.scan = DatasetScan::ExactTableUnavailable;
                                }
                                continue;
                            }
                        };
                        let reference = metadata.table_reference.take().unwrap_or_default();
                        resolved_dataset_id
                            .get_or_insert_with(|| resolved_name(reference.dataset_id, dataset_id));
                        // A table may have been replaced since it was listed. Keep its type
                        // consistent with the schema and statistics from this description.
                        let table_type = if metadata.table_type.is_empty() {
                            listed_type.unwrap_or(TABLE)
                        } else {
                            bigquery_table_type(&metadata.table_type)
                        };
                        if scan
                            .table_types
                            .is_some_and(|types| !types.contains(&table_type))
                        {
                            continue;
                        }
                        tables.push(ScannedTable {
                            name: resolved_name(reference.table_id, &table_id),
                            table_type,
                            metadata: scan.describe.is_some().then_some(*metadata),
                        });
                    }
                }
            }
            Ok::<_, adbc_core::error::Error>(())
        })?;

        Ok(datasets
            .into_iter()
            .map(|mut dataset| {
                if let DatasetScan::Tables { tables, .. } = &mut dataset.scan {
                    tables.sort_unstable_by(|left, right| left.name.cmp(&right.name));
                }
                dataset.scan
            })
            .collect())
    }
}

/// Whether BigQuery answered 400 `invalid`, its answer to a name that cannot be an object ID.
/// Other 400s are requests the driver got wrong.
fn rejected_as_invalid(source: &MetadataError) -> bool {
    matches!(
        source,
        MetadataError::Rejected(status, body)
            if *status == reqwest::StatusCode::BAD_REQUEST
                && service_failure("", *status, body).1.as_deref() == Some("invalid")
    )
}

/// Whether an exact table filter matches no table without asking: BigQuery rejects an empty table
/// ID, and a table ID cannot contain `$` or `@`, which `tables.get` resolves as a partition or
/// snapshot decorator of another table.
pub(super) fn names_no_table(table_id: &str) -> bool {
    table_id.is_empty() || table_id.contains(['$', '@'])
}

/// One dataset's progress through [`BigQueryConnection::scan_tables`].
struct DatasetWalk {
    id: Arc<str>,
    page_tokens: PageTokens,
    scan: DatasetScan,
}

/// Issues one scan request on a spawned task. An exact table is looked up with the scan's own
/// view, or with just its type when the scan only lists.
async fn send(
    client: MetadataClient,
    project_id: ProjectIdentifier,
    dataset_id: Arc<str>,
    describe: Option<TableView>,
    index: usize,
    request: Request,
    operation: Operation,
) -> (usize, Response) {
    let dataset = DatasetId {
        project_id: project_id.as_str(),
        dataset_id: &dataset_id,
    };
    let response = match request {
        Request::List { page_token } => Response::List(
            client
                .list_tables(Some(&operation), dataset, &page_token)
                .await,
        ),
        Request::Get {
            table_id,
            listed_type,
        } => {
            let result = client
                .get_table(
                    Some(&operation),
                    TableId {
                        project_id: dataset.project_id,
                        dataset_id: dataset.dataset_id,
                        table_id: &table_id,
                    },
                    describe.unwrap_or(TableView::Type),
                )
                .await
                .map(Box::new);
            Response::Get {
                table_id,
                listed_type,
                result,
            }
        }
    };
    (index, response)
}

fn listed_table_reference(
    reference: Option<TableReference>,
    project_id: &ProjectIdentifier,
    dataset_id: &str,
) -> Result<TableReference> {
    let invalid = |problem: &str| {
        error(
            format!(
                "BigQuery returned a table without {problem} while listing {project_id}.{dataset_id}"
            ),
            Status::InvalidData,
        )
    };
    match reference {
        None => Err(invalid("a reference")),
        Some(reference) if reference.table_id.is_empty() => Err(invalid("an ID")),
        Some(reference) => Ok(reference),
    }
}

/// The ID BigQuery reported for an object it listed or looked up, or the requested one when the
/// response named none.
///
/// An exact filter reports the object BigQuery resolved it to, as a listing would. The two
/// spellings differ only in case, in a dataset created with `isCaseInsensitive`, where BigQuery
/// resolves dataset and table names without regard to case.
pub(super) fn resolved_name(reported: String, requested: &str) -> String {
    if reported.is_empty() {
        requested.to_owned()
    } else {
        reported
    }
}

/// Whether a failed request for one listed object means only that there is nothing to report:
/// `Ok(())` for a 404 (the object vanished after it was listed) or an access denial; every other
/// failure is returned.
///
/// Discovery races concurrent DDL and meets objects the principal may not read: other users'
/// anonymous result tables, and datasets (`datasets.list` sends `all=true`) whose tables a custom
/// role without `bigquery.tables.list` cannot enumerate. HTTP 403 also covers quota, billing and
/// service restrictions, so only an `accessDenied` reason, or none, hides an object.
///
/// Direct schema requests and the dataset walk itself propagate every failure, since they name
/// what the caller asked for.
fn skip_unreadable(source: MetadataError, context: &str) -> Result<()> {
    match source {
        source if source.is_not_found() => Ok(()),
        MetadataError::Rejected(status, body) if status == reqwest::StatusCode::FORBIDDEN => {
            let (failure, reason) = service_failure(context, status, &body);
            if reason
                .as_deref()
                .is_none_or(|reason| reason == "accessDenied")
            {
                Ok(())
            } else {
                Err(failure)
            }
        }
        source => Err(source.into_error(context)),
    }
}

/// Maps a `tables.get` failure for `project.dataset.table` to an ADBC error.
///
/// The `404 Not Found:` text is load-bearing: dbt-core 2.0 detects a missing relation by that
/// substring (`is_bigquery_not_found_error` in `crates/dbt-adapter/src/metadata/bigquery/mod.rs`),
/// not by the ADBC status. It is only a compatibility marker, so the rejection's usual details
/// and `(HTTP n)` still follow, and a rejected table ID shows both the 404 and the service's 400.
pub(super) fn table_metadata_error(
    project_id: &str,
    dataset_id: &str,
    table_name: &str,
    source: MetadataError,
) -> adbc_core::error::Error {
    let context =
        format!("failed to get BigQuery table metadata for {project_id}.{dataset_id}.{table_name}");
    let invalid_name = rejected_as_invalid(&source);
    match source {
        MetadataError::Rejected(status, body) if status == reqwest::StatusCode::NOT_FOUND => {
            service_error(&format!("{context}: 404 Not Found"), status, &body)
        }
        // ADBC GetTableSchema treats an invalid object name as a missing object; BigQuery answers
        // an invalid table ID with 400 `invalid` rather than 404. Other 400s stay errors.
        MetadataError::Rejected(status, body) if invalid_name => {
            let mut failure = service_error(
                &format!("{context}: 404 Not Found: BigQuery rejected the table ID"),
                status,
                &body,
            );
            failure.status = Status::NotFound;
            failure
        }
        source => source.into_error(&context),
    }
}

/// The table type a listing reports, restricted to the five [`TABLE_TYPES`].
///
/// An omitted type, or one the REST API added later, is reported as `TABLE`, which keeps the
/// relation listed: dbt-core 2.0's `RelationType::from_adapter_type`
/// (`crates/dbt-schemas/src/dbt_types.rs`) panics on an unknown BigQuery type.
pub(super) fn bigquery_table_type(table_type: &str) -> &'static str {
    if let Some(known) = TABLE_TYPES.iter().find(|&&known| known == table_type) {
        return known;
    }
    if !table_type.is_empty() {
        diag!(
            Debug,
            "BigQuery reported the unknown table type {table_type:?}; reporting it as TABLE"
        );
    }
    TABLE
}
