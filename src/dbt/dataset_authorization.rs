//! dbt-core 2.0's option-only BigQuery authorized-view operation.
//!
//! A view that reads another dataset must be listed in that dataset's access control list. dbt's
//! `grant_access_to` model config sets this option. See `DBT-CORE.md`.

use std::sync::Arc;

use adbc_core::error::Result;

use super::{parse_json_option, required_string};
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{invalid_argument, option_value_json};
use crate::identifiers::ProjectIdentifier;
use crate::identifiers::validate_dataset_id;
use crate::query::{ExecutionContext, QualifiedTableName};
use crate::rest::json::{encode_failure, encode_json};
use crate::rest::metadata::{DatasetId, MetadataClient, TableId};
use crate::rest::model::TableReference;
use crate::rest::retry::retryable_failure;

/// The datasets each resolved view is authorized on.
///
/// Both levels are sorted and deduplicated, so requests and read-back are deterministic and a
/// repeated entry is sent once. Sorted vectors rather than B-trees: a payload holds a handful of
/// entries, and the B-tree insert and drop code costs kilobytes of binary.
pub(crate) type ViewAuthorizations = Vec<(QualifiedTableName, Vec<SourceDataset>)>;

/// A dataset whose tables an authorized view is granted read access to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceDataset {
    project_id: ProjectIdentifier,
    dataset_id: Arc<str>,
}

/// Authorized views configured on an ADBC statement by dbt-core 2.0.
///
/// The payload is parsed, validated, and resolved against the statement defaults when set, so a
/// malformed one fails immediately. An empty string or an empty JSON object clears it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DatasetAuthorizationOptions {
    views: Option<Arc<ViewAuthorizations>>,
}

impl DatasetAuthorizationOptions {
    pub(crate) fn is_active(&self) -> bool {
        self.views.is_some()
    }

    pub(crate) fn set(&mut self, views: Option<ViewAuthorizations>) {
        self.views = views.map(Arc::new);
    }

    /// Serializes configured authorizations with fully qualified names in sorted order.
    pub(crate) fn option_value(&self) -> Result<Option<String>> {
        let Some(views) = self.views.as_deref() else {
            return Ok(None);
        };
        // Filled with `insert` rather than `collect`: `serde_json::Map` is a `BTreeMap`, whose
        // `FromIterator` sorts and so instantiates driftsort for this element type (kilobytes of
        // code). See BINARY-SIZE.md.
        let mut object = serde_json::Map::new();
        for (view, datasets) in views {
            let mut authorized = Vec::with_capacity(datasets.len());
            for dataset in datasets {
                authorized.push(serde_json::json!({
                    "project": dataset.project_id.as_str(),
                    "dataset": dataset.dataset_id.as_ref(),
                }));
            }
            object.insert(view.as_option_value(), serde_json::Value::Array(authorized));
        }
        option_value_json(&serde_json::Value::Object(object), "view authorizations").map(Some)
    }

    /// Adds each view to the access control list of every dataset it is configured for. Views
    /// already listed are skipped, so a re-run issues no patch.
    pub(crate) fn execute(&self, context: &ExecutionContext, operation: &Operation) -> Result<()> {
        let Some(views) = self.views.as_deref() else {
            return Ok(());
        };
        let client = context.state.metadata_client(&context.runtime)?;
        context.runtime.block_on(async {
            for (view, datasets) in views {
                for dataset in datasets {
                    let dataset = DatasetId {
                        project_id: dataset.project_id.as_str(),
                        dataset_id: dataset.dataset_id.as_ref(),
                    };
                    Box::pin(authorize(&client, operation, view, dataset)).await?;
                }
            }
            Ok(())
        })
    }
}

/// Read-modify-write rounds one grant may take. See [`authorize`].
const MAX_GRANT_ROUNDS: u32 = 3;

/// Lists `view` in the access control list of `dataset`.
///
/// The whole list is sent back, so the patch carries the fetched ETag as `If-Match` and another
/// writer's entries are never dropped. A patch that lost that race (412), or that failed in a way
/// that may have applied it unseen, starts another round: the fresh read shows whether the view is
/// listed, and otherwise supplies the ETag to patch under.
async fn authorize(
    client: &MetadataClient,
    operation: &Operation,
    view: &QualifiedTableName,
    dataset: DatasetId<'_>,
) -> Result<()> {
    let view_name = view.as_option_value();
    let action = format!("authorize view {view_name} on BigQuery dataset {dataset}");
    let mut round = 1;
    loop {
        operation.check()?;
        let metadata = Box::pin(client.get_dataset_access(Some(operation), dataset))
            .await
            .map_err(|source| {
                source.into_error(&format!(
                    "failed to fetch BigQuery dataset metadata for {dataset} before authorizing \
                     view {view_name}"
                ))
            })?;
        if metadata.access.iter().any(|entry| {
            authorized_view(entry).is_some_and(|existing| {
                existing.project_id == view.project_id().as_str()
                    && existing.dataset_id == view.dataset_id()
                    && existing.table_id == view.table_id()
            })
        }) {
            return Ok(());
        }
        let mut access = metadata.access;
        // An authorized view carries no role: BigQuery grants it READ implicitly.
        access.push(
            serde_json::value::to_raw_value(&AccessEntry { view: view.id() })
                .map_err(|source| encode_failure("the BigQuery dataset access entry", &source))?,
        );
        let body = encode_json(
            &AccessList { access: &access },
            "the BigQuery dataset access list",
        )?;
        let failure = match Box::pin(client.patch_dataset_access(
            operation,
            dataset,
            &metadata.etag,
            body,
            &action,
            "the dataset access list kept changing concurrently; retry the authorization",
        ))
        .await
        {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        let reread = failure.vendor_code == 412 || retryable_failure(failure.status);
        if !reread || round == MAX_GRANT_ROUNDS {
            return Err(failure);
        }
        diag!(
            Debug,
            "re-reading the access list of {dataset} after: {}",
            failure.message
        );
        round += 1;
    }
}

/// The entry this operation adds. Existing entries are sent back as BigQuery's raw bytes, so this
/// is the only entry shape the driver encodes.
#[derive(Debug, serde::Serialize)]
struct AccessEntry<'a> {
    view: TableId<'a>,
}

/// The patch body: the whole access list, existing entries verbatim.
#[derive(Debug, serde::Serialize)]
struct AccessList<'a> {
    access: &'a [Box<serde_json::value::RawValue>],
}

/// The table one existing access entry authorizes, or `None` when it authorizes something else.
///
/// Only `view` is decoded; the raw entry is what is sent back.
fn authorized_view(entry: &serde_json::value::RawValue) -> Option<TableReference> {
    #[derive(serde::Deserialize)]
    struct Entry {
        #[serde(default)]
        view: Option<TableReference>,
    }

    serde_json::from_slice::<Entry>(entry.get().as_bytes())
        .ok()?
        .view
}

/// Parses `{"[[project.]dataset.]view": [{"project": ..., "dataset": ...}, ...]}`, resolving
/// every view name against the statement defaults exactly as a destination table resolves.
///
/// An empty payload clears the option, as does one whose every view lists no source dataset.
pub(crate) fn parse_view_authorizations(
    value: &str,
    default_project_id: Option<&ProjectIdentifier>,
    default_dataset_id: Option<&str>,
) -> Result<Option<ViewAuthorizations>> {
    if value.is_empty() {
        return Ok(None);
    }
    let serde_json::Value::Object(object) =
        parse_json_option(value, BigQueryStatementOption::AuthorizeViewToDatasets)?
    else {
        return Err(invalid_payload());
    };
    let mut views = ViewAuthorizations::new();
    for (view_name, datasets) in object {
        let view = QualifiedTableName::parse_option(
            BigQueryStatementOption::AuthorizeViewToDatasets.as_str(),
            &view_name,
            default_project_id,
            default_dataset_id,
        )?;
        let serde_json::Value::Array(datasets) = datasets else {
            return Err(invalid_payload());
        };
        // A view with no source datasets grants nothing. Dropping it keeps an all-empty payload
        // from selecting this operation (deselecting any other one configured) only to send no
        // request; such a payload clears the option, as `{}` does.
        if datasets.is_empty() {
            continue;
        }
        let mut sources = Vec::with_capacity(datasets.len());
        for dataset in datasets {
            insert_sorted(&mut sources, parse_source_dataset(&dataset)?);
        }
        match views.binary_search_by(|(existing, _)| existing.cmp(&view)) {
            // Two spellings of one view, such as `view` and `project.dataset.view`, merge.
            Ok(index) => {
                if let Some((_, existing)) = views.get_mut(index) {
                    for source in sources {
                        insert_sorted(existing, source);
                    }
                }
            }
            Err(index) => views.insert(index, (view, sources)),
        }
    }
    Ok((!views.is_empty()).then_some(views))
}

/// Inserts `item` into the sorted, repeat-free `items` unless it is already there.
fn insert_sorted<T: Ord>(items: &mut Vec<T>, item: T) {
    if let Err(index) = items.binary_search(&item) {
        items.insert(index, item);
    }
}

fn parse_source_dataset(value: &serde_json::Value) -> Result<SourceDataset> {
    let serde_json::Value::Object(object) = value else {
        return Err(invalid_payload());
    };
    let project_id = required_string(
        object,
        "project",
        BigQueryStatementOption::AuthorizeViewToDatasets,
    )?;
    let dataset_id = required_string(
        object,
        "dataset",
        BigQueryStatementOption::AuthorizeViewToDatasets,
    )?;
    validate_dataset_id(
        &format!(
            "the dataset in BigQuery option {}",
            BigQueryStatementOption::AuthorizeViewToDatasets.as_str()
        ),
        dataset_id,
    )?;
    Ok(SourceDataset {
        project_id: ProjectIdentifier::try_new_from(
            &format!(
                "BigQuery option {}",
                BigQueryStatementOption::AuthorizeViewToDatasets.as_str()
            ),
            project_id,
        )?,
        dataset_id: Arc::from(dataset_id),
    })
}

fn invalid_payload() -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery option {} must be a JSON object mapping each view name to an array of \
         {{\"project\": ..., \"dataset\": ...}} objects",
        BigQueryStatementOption::AuthorizeViewToDatasets.as_str()
    ))
}

#[cfg(test)]
mod tests;
