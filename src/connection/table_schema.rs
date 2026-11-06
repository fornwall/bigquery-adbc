//! The table description `get_table_schema` reports: the dbt-core 2.0 schema metadata built from a
//! `tables.get` response, and the resolution of a table reference from its arguments.

use std::collections::BTreeMap;

use adbc_core::error::Result;
use arrow_schema::Metadata;

use super::table_metadata::bigquery_table_type;
use crate::error::{invalid_argument, invalid_state};
use crate::identifiers::{ProjectIdentifier, is_dot_segment};
use crate::rest::metadata::Table;
use crate::rest::model::{
    ExternalDataConfiguration, MaterializedViewDefinition, RangePartitioning, TimePartitioning,
};
use crate::types::temporal::civil_from_days;

/// Schema-level metadata mirroring the dbt-labs Go driver's `GetTableSchema` contract.
///
/// Moves text from the consumed response, including potentially large view definitions.
/// Missing values are omitted except `Type`, which dbt-core 2.0 requires.
pub(super) fn table_schema_metadata(table: Table) -> Metadata {
    let mut metadata = Metadata::new();
    // Always present, since dbt-core 2.0's get_relation fails without it; an empty type becomes
    // TABLE, as in GetObjects.
    metadata.insert("Type", bigquery_table_type(table.table_type));
    if !table.description.is_empty() {
        metadata.insert("Description", table.description);
    }
    if let Some(view) = table.view {
        if !view.query.is_empty() {
            metadata.insert("ViewQuery", view.query);
        }
        if let Some(use_legacy_sql) = view.use_legacy_sql {
            metadata.insert("UseLegacySQL", use_legacy_sql.to_string());
        }
    }
    if !table.labels.is_empty() {
        metadata.insert("Labels", sorted_json_object(&table.labels));
    }
    if !table.resource_tags.is_empty() {
        metadata.insert("ResourceTags", sorted_json_object(&table.resource_tags));
    }
    if let Some(clustering) = table
        .clustering
        .as_ref()
        .filter(|clustering| !clustering.fields.is_empty())
    {
        metadata.insert("Clustering.Fields", json_array(&clustering.fields));
    }
    insert_partitioning(
        &mut metadata,
        table.time_partitioning,
        table.range_partitioning,
        table.require_partition_filter,
    );
    if let Some(formatted) = table
        .expiration_time
        .filter(|&epoch_ms| epoch_ms > 0)
        .map(format_rfc3339_millis)
    {
        metadata.insert("ExpirationTime", formatted);
    }
    insert_external_data_config(&mut metadata, table.external_data_configuration);
    if let Some(kms_key) = table
        .encryption_configuration
        .and_then(|configuration| configuration.kms_key_name)
        .filter(|kms_key| !kms_key.is_empty())
    {
        metadata.insert("EncryptionConfig.KMSKeyName", kms_key);
    }
    insert_materialized_view(&mut metadata, table.materialized_view, table.max_staleness);
    metadata
}

/// The time and range partitioning keys, and the partition-filter requirement that governs both.
fn insert_partitioning(
    metadata: &mut Metadata,
    time: Option<TimePartitioning>,
    range: Option<RangePartitioning>,
    require_partition_filter: Option<bool>,
) {
    if let Some(partitioning) = time {
        if !partitioning.r#type.is_empty() {
            metadata.insert("TimePartitioning.Type", partitioning.r#type);
        }
        if let Some(field) = partitioning.field.filter(|field| !field.is_empty()) {
            metadata.insert("TimePartitioning.Field", field);
        }
        if let Some(expiration_ms) = partitioning
            .expiration_ms
            .filter(|&expiration_ms| expiration_ms > 0)
        {
            metadata.insert(
                "TimePartitioning.Expiration",
                crate::duration::format_go_duration(std::time::Duration::from_millis(
                    expiration_ms,
                )),
            );
        }
    }
    if let Some(partitioning) = range {
        if !partitioning.field.is_empty() {
            metadata.insert("RangePartitioning.Field", partitioning.field);
        }
        if let Some(range) = partitioning.range {
            // Start, end, and interval arrive as INT64 values already rendered as strings.
            for (key, value) in [
                ("RangePartitioning.Range.Start", range.start),
                ("RangePartitioning.Range.End", range.end),
                ("RangePartitioning.Range.Interval", range.interval),
            ] {
                if !value.is_empty() {
                    metadata.insert(key, value);
                }
            }
        }
    }
    if let Some(require_partition_filter) = require_partition_filter {
        metadata.insert(
            "RequirePartitionFilter",
            require_partition_filter.to_string(),
        );
    }
}

/// External sources and their parsing options.
fn insert_external_data_config(
    metadata: &mut Metadata,
    external: Option<ExternalDataConfiguration>,
) {
    let Some(external) = external else {
        return;
    };
    for (key, value) in [
        ("ExternalDataConfig.SourceFormat", external.source_format),
        ("ExternalDataConfig.Compression", external.compression),
        ("ExternalDataConfig.ConnectionID", external.connection_id),
        (
            "ExternalDataConfig.ReferenceFileSchemaURI",
            external.reference_file_schema_uri,
        ),
        (
            "ExternalDataConfig.MetadataCacheMode",
            external.metadata_cache_mode,
        ),
    ] {
        if !value.is_empty() {
            metadata.insert(key, value);
        }
    }
    if !external.source_uris.is_empty() {
        metadata.insert(
            "ExternalDataConfig.SourceURIs",
            json_array(&external.source_uris),
        );
    }
    if !external.decimal_target_types.is_empty() {
        metadata.insert(
            "ExternalDataConfig.DecimalTargetTypes",
            json_array(&external.decimal_target_types),
        );
    }
    for (key, value) in [
        ("ExternalDataConfig.AutoDetect", external.auto_detect),
        (
            "ExternalDataConfig.IgnoreUnknownValues",
            external.ignore_unknown_values,
        ),
    ] {
        if let Some(value) = value {
            metadata.insert(key, value.to_string());
        }
    }
    if let Some(max_bad_records) = external.max_bad_records {
        metadata.insert(
            "ExternalDataConfig.MaxBadRecords",
            max_bad_records.to_string(),
        );
    }
    if let Some(hive) = external.hive_partitioning_options {
        if !hive.mode.is_empty() {
            metadata.insert("ExternalDataConfig.HivePartitioningOptions.Mode", hive.mode);
        }
        if !hive.source_uri_prefix.is_empty() {
            metadata.insert(
                "ExternalDataConfig.HivePartitioningOptions.SourceURIPrefix",
                hive.source_uri_prefix,
            );
        }
        if let Some(require_partition_filter) = hive.require_partition_filter {
            metadata.insert(
                "ExternalDataConfig.HivePartitioningOptions.RequirePartitionFilter",
                require_partition_filter.to_string(),
            );
        }
    }
}

/// The materialized view's definition and refresh policy.
///
/// `max_staleness` is a table-level field, but the Go driver publishes it under the
/// `MaterializedView.` prefix, and only for a materialized view.
fn insert_materialized_view(
    metadata: &mut Metadata,
    view: Option<MaterializedViewDefinition>,
    max_staleness: String,
) {
    let Some(materialized_view) = view else {
        return;
    };
    if !materialized_view.query.is_empty() {
        metadata.insert("MaterializedView.Query", materialized_view.query);
    }
    if let Some(enable_refresh) = materialized_view.enable_refresh {
        metadata.insert("MaterializedView.EnableRefresh", enable_refresh.to_string());
    }
    if let Some(allow_non_incremental) = materialized_view.allow_non_incremental_definition {
        metadata.insert(
            "MaterializedView.AllowNonIncrementalDefinition",
            allow_non_incremental.to_string(),
        );
    }
    if let Some(interval_ms) = materialized_view
        .refresh_interval_ms
        .filter(|&interval_ms| interval_ms > 0)
    {
        metadata.insert(
            "MaterializedView.RefreshInterval",
            crate::duration::format_go_duration(std::time::Duration::from_millis(interval_ms)),
        );
    }
    if !max_staleness.is_empty() {
        metadata.insert("MaterializedView.MaxStaleness", max_staleness);
    }
}

/// Serializes a list of strings as a JSON array, as the Go driver does. The order is significant
/// (clustering key order, declared source URIs) and is kept.
fn json_array(values: &[String]) -> String {
    serde_json::to_string(values).expect("a vector of strings serializes to JSON")
}

/// Serializes labels and resource tags with deterministic key order.
fn sorted_json_object(map: &BTreeMap<String, String>) -> String {
    serde_json::to_string(map).expect("a map of strings serializes to JSON")
}

/// Formats Unix epoch milliseconds as RFC 3339 UTC, e.g. `2026-01-01T00:00:00Z`.
///
/// dbt-core 2.0 parses `ExpirationTime` with an RFC 3339 parser, so the rendering must be exact.
pub(super) fn format_rfc3339_millis(epoch_ms: i64) -> String {
    let seconds = epoch_ms.div_euclid(1_000);
    let millis = epoch_ms.rem_euclid(1_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let second_of_day = seconds.rem_euclid(86_400);
    let fraction = if millis == 0 {
        String::new()
    } else {
        format!(".{millis:03}")
    };
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}{fraction}Z",
        second_of_day / 3_600,
        second_of_day % 3_600 / 60,
        second_of_day % 60,
    )
}

pub(super) fn resolve_table_reference<'a>(
    configured_project: Option<&ProjectIdentifier>,
    configured_schema: Option<&'a str>,
    catalog: Option<&'a str>,
    db_schema: Option<&'a str>,
    table_name: &'a str,
) -> Result<(ProjectIdentifier, &'a str, &'a str)> {
    let project_id = match catalog {
        Some(project_id) => ProjectIdentifier::try_new_from("the catalog argument", project_id)?,
        None => configured_project.cloned().ok_or_else(|| {
            invalid_state(
                "a BigQuery project must be supplied as the catalog argument, or set with the \
                 connection option adbc.connection.catalog or the database option \
                 bigquery.project_id",
            )
        })?,
    };
    let dataset_id = match db_schema {
        // BigQuery has no table outside a dataset, as an empty name would ask for.
        Some("") => {
            return Err(invalid_argument(
                "the BigQuery db_schema argument must not be empty; pass NULL to use the \
                 connection's current schema",
            ));
        }
        Some(dataset_id) => dataset_id,
        // Never empty: setting either option to "" clears it.
        None => configured_schema.ok_or_else(|| {
            invalid_state(
                "a BigQuery dataset must be supplied as the db_schema argument, or set with the \
                 connection option adbc.connection.db_schema or the database option \
                 bigquery.dataset_id",
            )
        })?,
    };
    if table_name.is_empty() {
        return Err(invalid_argument("BigQuery table name must not be empty"));
    }
    // A dot segment would address the parent resource, and unlike get_objects this call cannot
    // answer with an empty listing.
    for (subject, name) in [("dataset", dataset_id), ("table name", table_name)] {
        if is_dot_segment(name) {
            return Err(invalid_argument(format!(
                "BigQuery {subject} must not be \".\" or \"..\""
            )));
        }
    }
    Ok((project_id, dataset_id, table_name))
}
