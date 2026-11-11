//! Result schemas and the compact query statistics published as Arrow schema metadata.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_schema::SchemaRef;

use crate::error::error;
use crate::rest::model::TableSchema;
use crate::schema_metadata::legacy;
use crate::types::schema::convert_schema;

use super::response::QueryMetadata;

/// Attaches the query's identity and the statistics BigQuery reported to a result schema that
/// [`result_schema`] converted; an unreported statistic gets no key.
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
        let entries = &mut Arc::make_mut(&mut schema).metadata;
        if let Some(legacy_key) = legacy_key {
            entries.insert(legacy_key, value.clone());
        }
        entries.insert(key, value);
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
        for (key, expected) in [
            ("BIGQUERY:query_id", "query-id"),
            ("BIGQUERY:statistics:query:statement_type", "UPDATE"),
            ("BIGQUERY:Statistics:Query:StatementType", "UPDATE"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "11"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "11"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "12"),
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "12"),
            ("BIGQUERY:statistics:total_bytes_processed", "12"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "12"),
            ("BIGQUERY:statistics:query:slot_millis", "13"),
            ("BIGQUERY:Statistics:Query:SlotMillis", "13"),
            ("BIGQUERY:statistics:query:num_dml_affected_rows", "14"),
            ("BIGQUERY:Statistics:Query:NumDMLAffectedRows", "14"),
        ] {
            assert_eq!(schema.metadata()[key], expected, "{key}");
        }
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
        let keys = [
            "BIGQUERY:statistics:query:total_bytes_processed",
            "BIGQUERY:Statistics:Query:TotalBytesProcessed",
            "BIGQUERY:statistics:total_bytes_processed",
            "BIGQUERY:Statistics:TotalBytesProcessed",
            "BIGQUERY:statistics:query:num_dml_affected_rows",
            "BIGQUERY:Statistics:Query:NumDMLAffectedRows",
        ];
        assert_eq!(schema.metadata().len(), keys.len());
        for key in keys {
            assert_eq!(schema.metadata()[key], "0", "{key}");
        }
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
