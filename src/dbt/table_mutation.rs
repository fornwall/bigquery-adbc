//! dbt-core 2.0's option-only BigQuery table metadata operations.
//!
//! Two operations against one relation, both naming it with `bigquery.query.destination_table`:
//! the column metadata update behind `persist_docs: {columns: true}`, and the table description
//! update behind `persist_docs: {relation: true}`. See `DBT-CORE.md`.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::error::{Result, Status};

use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::error::{error, option_value_json};
use crate::options::json_object_value;
use crate::query::{ExecutionContext, QualifiedTableName};
use crate::rest::metadata::TableView;
use crate::rest::model::{PolicyTagList, TableFieldSchema, TableSchema};
use crate::types::{BigQueryType, folded_name};

type ColumnDescriptions = BTreeMap<String, String>;
type ColumnPolicyTags = BTreeMap<String, Vec<String>>;

/// Table metadata configured on an ADBC statement by dbt-core 2.0.
///
/// Payloads are parsed when set, so a malformed one fails immediately, and kept in sorted maps so
/// read-back is deterministic. An empty string or empty object clears a value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TableMutationOptions {
    description: Option<Arc<str>>,
    descriptions: Option<Arc<ColumnDescriptions>>,
    policy_tags: Option<Arc<ColumnPolicyTags>>,
}

impl TableMutationOptions {
    pub(crate) fn is_active(&self) -> bool {
        self.description.is_some() || self.descriptions.is_some() || self.policy_tags.is_some()
    }

    /// The option keys currently selecting this operation, for a message that has to name them.
    pub(crate) fn active_option_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::with_capacity(3);
        if self.description.is_some() {
            keys.push(BigQueryStatementOption::UpdateDescription.as_str());
        }
        if self.descriptions.is_some() {
            keys.push(BigQueryStatementOption::UpdateColumnsDescription.as_str());
        }
        if self.policy_tags.is_some() {
            keys.push(BigQueryStatementOption::UpdateColumnsPolicyTags.as_str());
        }
        keys
    }

    /// Sets the table description; an empty value clears the option. This operation cannot clear
    /// a table's description, and neither can the fork, which dispatches on a non-empty string.
    pub(crate) fn set_description(&mut self, value: &str) {
        self.description = (!value.is_empty()).then(|| Arc::from(value));
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn set_descriptions(&mut self, key: &str, value: &str) -> Result<()> {
        self.descriptions = json_object_value(
            key,
            value,
            "a JSON object mapping column paths to string descriptions",
        )?;
        Ok(())
    }

    pub(crate) fn set_policy_tags(&mut self, key: &str, value: &str) -> Result<()> {
        self.policy_tags = json_object_value(
            key,
            value,
            "a JSON object mapping column paths to arrays of policy tag names",
        )?;
        Ok(())
    }

    /// The canonical JSON of the column descriptions, or `None` while unset.
    pub(crate) fn descriptions_json(&self) -> Result<Option<String>> {
        self.descriptions
            .as_deref()
            .map(|descriptions| option_value_json(descriptions, "column descriptions"))
            .transpose()
    }

    /// The canonical JSON of the column policy tags, or `None` while unset.
    pub(crate) fn policy_tags_json(&self) -> Result<Option<String>> {
        self.policy_tags
            .as_deref()
            .map(|policy_tags| option_value_json(policy_tags, "column policy tags"))
            .transpose()
    }

    /// Runs whichever table metadata updates this statement configured.
    ///
    /// The two are independent patches of the same table, so a statement holding both runs both,
    /// columns first as dbt's `persist_docs` does.
    pub(crate) fn execute(
        &self,
        context: &ExecutionContext,
        table: &QualifiedTableName,
        operation: &Operation,
    ) -> Result<()> {
        let client = context.state.metadata_client(&context.runtime)?;
        let table = table.id();
        context.runtime.block_on(async {
            if self.descriptions.is_some() || self.policy_tags.is_some() {
                let metadata = client
                    .get_table(Some(operation), table, TableView::Schema)
                    .await
                    .map_err(|source| {
                        source.into_error(&format!(
                            "failed to fetch BigQuery table metadata for {table} before updating its \
                             columns"
                        ))
                    })?;
                let mut schema = metadata.schema.ok_or_else(|| {
                    error(
                        format!("BigQuery returned table metadata without a schema for {table}"),
                        Status::InvalidData,
                    )
                })?;
                apply_column_metadata(
                    &mut schema,
                    self.descriptions.as_deref(),
                    self.policy_tags.as_deref(),
                );
                // Preserve the complete schema, including REPEATED modes. Like dbt Core, write
                // it without an ETag precondition.
                client
                    .patch_table_schema(Some(operation), table, &schema)
                    .await
                    .map_err(|source| {
                        source.into_error(&format!(
                            "failed to update BigQuery column metadata for {table}"
                        ))
                    })?;
            }
            if let Some(description) = self.description.as_deref() {
                // Last write wins for this scalar field. An ETag precondition can fail after
                // a materialized view's automatic refresh while reads still return its old ETag.
                client
                    .patch_table_description(Some(operation), table, description)
                    .await
                    .map_err(|source| {
                        source.into_error(&format!(
                            "failed to update the description of BigQuery table {table}"
                        ))
                    })?;
            }
            Ok(())
        })
    }
}

/// The configured description and policy tags of each column, keyed by [`folded_name`] path. One
/// map for both payloads, so each field is looked up once.
type PathIndex<'a> = BTreeMap<String, (Option<&'a str>, Option<&'a [String]>)>;

/// Applies the configured metadata to a table's schema, keyed by dotted column path.
///
/// A `STRUCT` field's children are named `parent.child`, the paths BigQuery reports in
/// `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` and dbt uses in `persist_docs`; a field name never
/// contains a dot. Paths naming nothing are ignored, as in dbt Core.
///
/// Paths match case-insensitively under BigQuery's own rule (see [`folded_name`]), since dbt
/// forwards the model's casing and BigQuery resolves field names case-insensitively but preserves
/// their case. The match cannot be ambiguous: BigQuery rejects sibling fields that fold alike, at
/// every depth.
fn apply_column_metadata(
    schema: &mut TableSchema,
    descriptions: Option<&ColumnDescriptions>,
    policy_tags: Option<&ColumnPolicyTags>,
) {
    let index = path_index(descriptions, policy_tags);
    // One buffer for the whole walk: each field appends its own name and truncates back.
    let mut path = String::new();
    apply_field_metadata(&mut schema.fields, &mut path, &index);
}

/// Indexes both payloads by folded path. Keys that fold alike name one field; the first in sort
/// order wins.
fn path_index<'a>(
    descriptions: Option<&'a ColumnDescriptions>,
    policy_tags: Option<&'a ColumnPolicyTags>,
) -> PathIndex<'a> {
    let mut index = PathIndex::new();
    for (path, description) in descriptions.into_iter().flatten() {
        let entry = index.entry(folded_name(path)).or_default();
        entry.0.get_or_insert(description);
    }
    for (path, names) in policy_tags.into_iter().flatten() {
        let entry = index.entry(folded_name(path)).or_default();
        entry.1.get_or_insert(names);
    }
    index
}

/// Applies metadata to `fields` and, recursively, to their nested schemas. `path` names the
/// `STRUCT` these fields belong to, folded, and is empty at the top level. BigQuery caps
/// `STRUCT` nesting at 15 levels, so the recursion is shallow.
fn apply_field_metadata(fields: &mut [TableFieldSchema], path: &mut String, index: &PathIndex<'_>) {
    let parent = path.len();
    for field in fields {
        if parent != 0 {
            path.push('.');
        }
        // Folding is per character, so folding each segment equals folding the whole path.
        path.push_str(&folded_name(&field.name));
        let (description, policy_tags) = index.get(path.as_str()).copied().unwrap_or_default();
        if let Some(description) = description {
            // The patch replaces the whole schema, and BigQuery clears the description of a
            // field that omits it, so an empty description is omitted (as the fork's Go client
            // serializes it).
            field.description = (!description.is_empty()).then(|| description.to_owned());
        }
        // Match dbt Core and dbt-core 2.0's bundled driver: policy tags belong to leaf columns,
        // not to the RECORDs containing them.
        if !is_record(field)
            && let Some(names) = policy_tags
        {
            field.policy_tags = Some(PolicyTagList {
                names: names.to_vec(),
            });
        }
        apply_field_metadata(&mut field.fields, path, index);
        path.truncate(parent);
    }
}

/// Whether a field contains a nested schema rather than a value.
///
/// The type name is checked too, in case a `RECORD` arrives without children, in both spellings
/// because the REST API accepts and echoes the GoogleSQL one (see `BigQueryType::from_rest`).
fn is_record(field: &TableFieldSchema) -> bool {
    !field.fields.is_empty()
        || field
            .r#type
            .eq_ignore_ascii_case(BigQueryType::Struct.rest())
        || field
            .r#type
            .eq_ignore_ascii_case(BigQueryType::Struct.sql())
}

#[cfg(test)]
mod tests;
