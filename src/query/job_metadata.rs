//! Result schemas and the compact query statistics published as Arrow schema metadata.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_schema::SchemaRef;

use crate::error::error;
use crate::rest::model::TableSchema;
use crate::schema_metadata::legacy;
use crate::types::schema::convert_schema;

use super::response::QueryMetadata;

/// Fills missing job metadata without overwriting values already attached to the schema.
pub(super) fn schema_with_rest_metadata(
    mut schema: SchemaRef,
    metadata: &QueryMetadata,
) -> SchemaRef {
    let query_id = metadata
        .job_reference
        .as_ref()
        .map(|reference| reference.job_id.as_str())
        .filter(|job_id| !job_id.is_empty())
        .or_else(|| (!metadata.query_id.is_empty()).then_some(metadata.query_id.as_str()));
    let mut fill = |key: &str, legacy_key: Option<&str>, value: Option<String>| {
        let Some(value) = value else {
            return;
        };
        if let Some(legacy_key) = legacy_key
            && !schema.metadata().contains_key(legacy_key)
        {
            Arc::make_mut(&mut schema)
                .metadata
                .insert(legacy_key, value.clone());
        }
        if !schema.metadata().contains_key(key) {
            Arc::make_mut(&mut schema).metadata.insert(key, value);
        }
    };
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID,
        None,
        query_id.map(ToOwned::to_owned),
    );
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE,
        Some(legacy::STATEMENT_TYPE),
        (!metadata.statement_type.is_empty()).then(|| metadata.statement_type.clone()),
    );
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
        Some(legacy::TOTAL_BYTES_BILLED),
        metadata.total_bytes_billed.map(|value| value.to_string()),
    );
    let processed = metadata
        .total_bytes_processed
        .map(|value| value.to_string());
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED,
        Some(legacy::QUERY_TOTAL_BYTES_PROCESSED),
        processed.clone(),
    );
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_JOB_TOTAL_BYTES_PROCESSED,
        Some(legacy::JOB_TOTAL_BYTES_PROCESSED),
        processed,
    );
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
        Some(legacy::SLOT_MILLIS),
        metadata.total_slot_ms.map(|value| value.to_string()),
    );
    fill(
        crate::BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS,
        Some(legacy::NUM_DML_AFFECTED_ROWS),
        metadata
            .num_dml_affected_rows
            .map(|value| value.to_string()),
    );
    schema
}

/// Converts a completed query's result schema, or reports a `SELECT` that came back without one.
/// Every other statement type legitimately has no result columns.
pub(super) fn result_schema(
    schema: Option<&TableSchema>,
    statement_type: &str,
    context: &str,
) -> Result<SchemaRef> {
    let Some(schema) = schema else {
        if statement_type == "SELECT" {
            return Err(error(
                format!("{context} completed without a result schema"),
                Status::InvalidData,
            ));
        }
        return convert_schema(&TableSchema::default());
    };
    convert_schema(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_metadata_publishes_query_id_and_reported_statistics() {
        let metadata = QueryMetadata {
            query_id: "query-id".to_owned(),
            statement_type: "UPDATE".to_owned(),
            total_bytes_billed: Some(11),
            total_bytes_processed: Some(12),
            total_slot_ms: Some(13),
            num_dml_affected_rows: Some(14),
            ..QueryMetadata::default()
        };
        let schema = schema_with_rest_metadata(Arc::new(arrow_schema::Schema::empty()), &metadata);
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID],
            "query-id"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE],
            "UPDATE"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED],
            "11"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED],
            "12"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_JOB_TOTAL_BYTES_PROCESSED],
            "12"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS],
            "13"
        );
        assert_eq!(
            schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS],
            "14"
        );
        assert_eq!(
            schema.metadata().len(),
            crate::BIGQUERY_SCHEMA_METADATA_JOB_KEYS.len()
        );
        for key in crate::BIGQUERY_SCHEMA_METADATA_JOB_KEYS {
            assert!(schema.metadata().contains_key(key), "missing {key}");
        }
    }

    #[test]
    fn statistics_aliases_preserve_reported_zero_and_missing_values() {
        let original = Arc::new(arrow_schema::Schema::empty());
        let empty = schema_with_rest_metadata(original.clone(), &QueryMetadata::default());
        assert!(Arc::ptr_eq(&original, &empty));

        let metadata = QueryMetadata {
            total_bytes_processed: Some(0),
            num_dml_affected_rows: Some(0),
            ..QueryMetadata::default()
        };
        let schema = schema_with_rest_metadata(original, &metadata);
        assert_eq!(
            schema.metadata(),
            &std::collections::HashMap::from([
                (
                    "BIGQUERY:statistics:query:total_bytes_processed".to_owned(),
                    "0".to_owned()
                ),
                (
                    "BIGQUERY:Statistics:Query:TotalBytesProcessed".to_owned(),
                    "0".to_owned()
                ),
                (
                    "BIGQUERY:statistics:total_bytes_processed".to_owned(),
                    "0".to_owned()
                ),
                (
                    "BIGQUERY:Statistics:TotalBytesProcessed".to_owned(),
                    "0".to_owned()
                ),
                (
                    "BIGQUERY:statistics:query:num_dml_affected_rows".to_owned(),
                    "0".to_owned()
                ),
                (
                    "BIGQUERY:Statistics:Query:NumDMLAffectedRows".to_owned(),
                    "0".to_owned()
                ),
            ]),
        );
    }

    #[test]
    fn filling_metadata_preserves_existing_values_and_shared_schemas() {
        let original = Arc::new(
            arrow_schema::Schema::empty().with_metadata(
                arrow_schema::Metadata::new()
                    .with(crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID, "original-id")
                    .with(crate::BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, "SELECT")
                    .with("application", "value"),
            ),
        );
        let metadata = QueryMetadata {
            query_id: "later-id".to_owned(),
            statement_type: "UPDATE".to_owned(),
            ..QueryMetadata::default()
        };

        let filled = schema_with_rest_metadata(original.clone(), &metadata);
        for (key, value) in original.metadata() {
            assert_eq!(filled.metadata().get(key), Some(value));
        }
        assert_eq!(filled.metadata()[legacy::STATEMENT_TYPE], "UPDATE");
        assert!(!original.metadata().contains_key(legacy::STATEMENT_TYPE));

        // Reattaching already present values leaves the shared schema untouched.
        let unchanged = schema_with_rest_metadata(filled.clone(), &metadata);
        assert!(Arc::ptr_eq(&filled, &unchanged));
    }

    #[test]
    fn select_requires_a_schema_but_dml_may_omit_it() {
        assert_eq!(
            result_schema(None, "SELECT", "query").unwrap_err().status,
            Status::InvalidData
        );
        assert!(
            result_schema(None, "UPDATE", "query")
                .unwrap()
                .fields()
                .is_empty()
        );
    }
}
