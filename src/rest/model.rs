//! The BigQuery REST message shapes the driver builds and reads itself.
//!
//! Hand-written `serde` types keep only the needed fields of `google_cloud_bigquery_v2::model`,
//! avoiding the generated visitors' duplicate-key sets and unknown-field maps (allocations and
//! binary size; see `docs/BINARY-SIZE.md`). Wire conventions match the generated types:
//!
//! * A defaulted field is omitted, never sent as `null`. `Option` marks fields whose empty value
//!   is meaningful and must be sent, such as `description: Some("")`.
//! * `int64` fields are written as decimal strings and read from a string or a number
//!   ([`JsonInt`]).
//! * JSON `null` reads as the field's default, the same as an omitted field.
//! * Unknown fields never fail a decode. The two schema types keep them in a `#[serde(flatten)]`
//!   map, because the whole-schema `tables.patch` (`src/dbt/table_mutation.rs`) is a
//!   read-modify-write that would otherwise delete them; other types ignore them. Schema
//!   properties the driver reads but does not interpret stay raw JSON so they round-trip exactly.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::rest::json::JsonInt;

/// Job settings accepted by `jobs.query` and by the query, load and copy configurations in
/// `jobs.insert`. Borrowed so building a request does not copy labels or the reservation name.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    job_timeout_ms: Option<JsonInt<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<&'a std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reservation: Option<&'a str>,
}

impl<'a> JobOptions<'a> {
    pub(crate) fn new(
        job_timeout_ms: i64,
        labels: Option<&'a std::collections::BTreeMap<String, String>>,
        reservation: Option<&'a str>,
    ) -> Self {
        Self {
            job_timeout_ms: (job_timeout_ms != 0).then_some(JsonInt(job_timeout_ms)),
            labels: labels.filter(|labels| !labels.is_empty()),
            reservation,
        }
    }
}

/// Whether an integer field holds its default and is therefore left out of a request body.
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's skip_serializing_if predicate is always called with a reference"
)]
fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// Serializes an `int64` the way BigQuery's JSON API spells one: a decimal string.
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's serialize_with signature is always called with a reference"
)]
fn serialize_int64<S: serde::Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    JsonInt(*value).serialize(serializer)
}

/// Reads an `int64` written as either a decimal string or a JSON number, `null` as zero.
fn deserialize_int64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    Ok(Option::<JsonInt<i64>>::deserialize(deserializer)?.map_or(0, |value| value.0))
}

/// Reads any field whose `null` means "absent", which is every non-`Option` field here.
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// The columns of a table, as `tables.get` reports them and `tables.insert` and `tables.patch`
/// declare them.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableSchema {
    /// The top-level columns, in order.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub(crate) fields: Vec<TableFieldSchema>,
    /// Every schema property the driver does not name, carried verbatim.
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub(crate) other: serde_json::Map<String, JsonValue>,
}

/// One column of a table schema, at any nesting depth.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableFieldSchema {
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) name: String,
    /// The BigQuery type name, in either its REST or its GoogleSQL spelling.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) r#type: String,
    /// `NULLABLE`, `REQUIRED` or `REPEATED`; an absent mode means `NULLABLE`.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) mode: String,
    /// The nested columns of a `RECORD`.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub(crate) fields: Vec<Self>,
    /// The column description. Omitting it from a complete schema patch clears an existing one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// The field-level access-control tags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) policy_tags: Option<PolicyTagList>,
    /// The `STRING` or `BYTES` length constraint; zero when unconstrained.
    #[serde(
        default,
        deserialize_with = "deserialize_int64",
        serialize_with = "serialize_int64",
        skip_serializing_if = "is_zero"
    )]
    pub(crate) max_length: i64,
    /// The `NUMERIC` or `BIGNUMERIC` precision; zero when unconstrained.
    #[serde(
        default,
        deserialize_with = "deserialize_int64",
        serialize_with = "serialize_int64",
        skip_serializing_if = "is_zero"
    )]
    pub(crate) precision: i64,
    /// The `NUMERIC` or `BIGNUMERIC` scale; zero when unconstrained.
    #[serde(
        default,
        deserialize_with = "deserialize_int64",
        serialize_with = "serialize_int64",
        skip_serializing_if = "is_zero"
    )]
    pub(crate) scale: i64,
    /// The SQL expression a `DEFAULT` clause names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_value_expression: Option<String>,
    /// The element type of a `RANGE` column, required when the type is `RANGE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) range_element_type: Option<FieldElementType>,
    /// The collation specification, as raw JSON: read for the `BIGQUERY:collation` field
    /// metadata (`src/types/schema.rs`), never interpreted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) collation: Option<JsonValue>,
    /// The sub-second digits a temporal column declares, as raw JSON because BigQuery spells an
    /// int64 as a string and a schema patch has to echo it back exactly
    /// (`declared_subsecond_digits`, `src/types/schema.rs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp_precision: Option<JsonValue>,
    /// The generating expression, read only for `xdbc_is_generatedcolumn`
    /// (`build_columns`, `src/connection/objects.rs`); its shape is never inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generated_column: Option<JsonValue>,
    /// Column properties the driver does not name (`roundingMode`, `dataPolicies`,
    /// `foreignTypeDefinition`, ...), carried verbatim so the `persist_docs` schema patch does not
    /// strip them.
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub(crate) other: serde_json::Map<String, JsonValue>,
}

/// The element type of a `RANGE` column: `DATE`, `DATETIME` or `TIMESTAMP`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FieldElementType {
    /// The BigQuery type name of the range's bounds.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) r#type: String,
}

/// The Data Catalog policy tags attached to one column.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PolicyTagList {
    /// The fully qualified policy tag names.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub(crate) names: Vec<String>,
}

/// One `jobs.query` query parameter: a name (empty when positional), a type, and a value.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryParameter {
    /// The parameter name; empty for a positional parameter.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) parameter_type: Option<QueryParameterType>,
    /// The parameter value. An `Option` because an empty value is SQL `NULL` and must be sent.
    ///
    /// Never decoded: the driver reads back only a dry run's `undeclaredQueryParameters`, which
    /// have no value, and skipping the field keeps the recursive value decoder out of the binary.
    #[serde(default, skip_serializing_if = "Option::is_none", skip_deserializing)]
    pub(crate) parameter_value: Option<QueryParameterValue>,
}

/// The type of one query parameter, recursive through `ARRAY`, `STRUCT` and `RANGE`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryParameterType {
    /// The GoogleSQL type name.
    #[serde(
        rename = "type",
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) r#type: String,
    /// The seconds precision BigQuery inferred for a `TIMESTAMP` parameter.
    #[serde(
        default,
        deserialize_with = "crate::rest::json::optional_json_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) timestamp_precision: Option<i64>,
    /// The element type of an `ARRAY`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) array_type: Option<Box<Self>>,
    /// The members of a `STRUCT`, in order.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub(crate) struct_types: Vec<QueryParameterStructType>,
    /// The bound type of a `RANGE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) range_element_type: Option<Box<Self>>,
}

/// One member of a `STRUCT` parameter type.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryParameterStructType {
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) name: String,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub(crate) r#type: Option<Box<QueryParameterType>>,
}

/// The value of one query parameter. Every field absent is SQL `NULL`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryParameterValue {
    /// The scalar value, already rendered the way BigQuery spells its literal form.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) array_values: Vec<Self>,
    /// The members of a `STRUCT`, keyed by member name.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(crate) struct_values: std::collections::HashMap<String, Self>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) range_value: Option<Box<RangeValue>>,
}

/// The two bounds of a `RANGE` parameter value; an absent bound is unbounded.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RangeValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start: Option<Box<QueryParameterValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end: Option<Box<QueryParameterValue>>,
}

/// The `project.dataset.table` triple that names one table in a request or response body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableReference {
    /// The project that bills and owns the table.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) project_id: String,
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) dataset_id: String,
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub(crate) table_id: String,
}

/// One structured error entry, as every BigQuery job status and REST rejection carries it.
///
/// Decode-only: the driver reports these, never sends them.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorProto {
    /// BigQuery's own machine-readable cause, such as `notFound` or `rateLimitExceeded`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) reason: String,
    /// Where the failure occurred, such as a query position or a field name.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) location: String,
    /// The human-readable explanation.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) message: String,
    // `debugInfo` is Google-internal troubleshooting text, never rendered, so it is not read.
}

/// The status of a BigQuery job, as every `jobs.*` response carries it.
///
/// Shared by the query path and the dbt load and copy jobs so only one deserializer is linked.
/// Decode-only.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobStatus {
    /// The authoritative failure, when the job has one.
    #[serde(default)]
    pub(crate) error_result: Option<ErrorProto>,
    /// Every error and warning the job accumulated, the authoritative one among them.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) errors: Vec<ErrorProto>,
    /// `PENDING`, `RUNNING` or `DONE`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) state: String,
}

impl JobStatus {
    pub(crate) fn is_done(&self) -> bool {
        self.state == "DONE"
    }

    /// Takes a terminal error list only when `errorResult` says the job failed.
    ///
    /// `errors` usually repeats the primary error but is documented as errors *or warnings*, so it
    /// cannot establish failure alone. A sparse response may carry only `errorResult`.
    pub(crate) fn take_terminal_errors(&mut self) -> Option<Vec<ErrorProto>> {
        let primary = self.error_result.take()?;
        // The primary goes first, so status classification cannot pick a warning or a secondary
        // failure. A repeat compares equal because `debugInfo`, where it may differ, is not read.
        self.errors.retain(|error| *error != primary);
        self.errors.insert(0, primary);
        Some(std::mem::take(&mut self.errors))
    }
}

/// The clustering columns of a table, in order.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Clustering {
    /// The clustered column names.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) fields: Vec<String>,
}

/// A table's ingestion-time or column-based time partitioning.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimePartitioning {
    /// The partition granularity: `HOUR`, `DAY`, `MONTH` or `YEAR`.
    #[serde(rename = "type", default, deserialize_with = "null_as_default")]
    pub(crate) r#type: String,
    /// The partitioning column, absent for ingestion-time partitioning.
    #[serde(default)]
    pub(crate) field: Option<String>,
    /// How long a partition is kept, in milliseconds, absent when partitions never expire.
    #[serde(default, deserialize_with = "crate::rest::json::optional_json_int")]
    pub(crate) expiration_ms: Option<u64>,
}

/// A table's integer-range partitioning.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RangePartitioning {
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) field: String,
    #[serde(default)]
    pub(crate) range: Option<PartitionRange>,
}

/// The bounds and width of an integer-range partitioning.
///
/// BigQuery sends all three as `int64` strings, which the driver reports verbatim.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartitionRange {
    /// The inclusive lower bound.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) start: String,
    /// The exclusive upper bound.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) end: String,
    /// The width of one partition.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) interval: String,
}

/// A table's customer-managed encryption key.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EncryptionConfiguration {
    #[serde(default)]
    pub(crate) kms_key_name: Option<String>,
}

/// The refresh settings of a materialized view.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MaterializedViewDefinition {
    /// The GoogleSQL the view is defined by.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) enable_refresh: Option<bool>,
    /// The automatic refresh interval, in milliseconds.
    #[serde(default, deserialize_with = "crate::rest::json::optional_json_int")]
    pub(crate) refresh_interval_ms: Option<u64>,
    /// Whether the definition may be one BigQuery cannot refresh incrementally.
    #[serde(default)]
    pub(crate) allow_non_incremental_definition: Option<bool>,
}

/// The definition of a logical view.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ViewDefinition {
    /// The query the view is defined by.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) query: String,
    /// Whether the query is legacy SQL. Reported, not acted on: it tells a caller why a visible
    /// view cannot be read through this GoogleSQL-only driver.
    #[serde(default)]
    pub(crate) use_legacy_sql: Option<bool>,
}

/// Where an external table's data lives and how BigQuery reads it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalDataConfiguration {
    /// The data format, such as `CSV`, `PARQUET` or `GOOGLE_SHEETS`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) source_format: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) source_uris: Vec<String>,
    /// Whether BigQuery infers the schema from the data. Spelled `autodetect` on the wire.
    #[serde(default, rename = "autodetect")]
    pub(crate) auto_detect: Option<bool>,
    /// The compression of the source data, such as `GZIP`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) compression: String,
    /// Whether rows with extra values are read rather than rejected.
    #[serde(default)]
    pub(crate) ignore_unknown_values: Option<bool>,
    /// How many bad records a scan tolerates before it fails.
    #[serde(default, deserialize_with = "crate::rest::json::optional_json_int")]
    pub(crate) max_bad_records: Option<i64>,
    /// How the source URIs encode partition columns.
    #[serde(default)]
    pub(crate) hive_partitioning_options: Option<HivePartitioningOptions>,
    /// The types a source decimal may be read as, in preference order.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) decimal_target_types: Vec<String>,
    /// The connection the data is read through, for a source that needs one.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) connection_id: String,
    /// The file whose schema describes the whole source, for self-describing formats.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) reference_file_schema_uri: String,
    /// How long BigQuery may serve cached metadata for the source: `AUTOMATIC` or `MANUAL`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) metadata_cache_mode: String,
}

/// How an external table's source URIs encode its partition columns.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HivePartitioningOptions {
    /// How the partition keys are detected: `AUTO`, `STRINGS` or `CUSTOM`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) mode: String,
    /// The URI prefix the partition path follows.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) source_uri_prefix: String,
    /// Whether a query must filter on a partition column.
    #[serde(default)]
    pub(crate) require_partition_filter: Option<bool>,
}

/// A table's declared key constraints. BigQuery does not enforce them; it reports them.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableConstraints {
    /// The primary key, when one is declared.
    #[serde(default)]
    pub(crate) primary_key: Option<PrimaryKey>,
    /// The declared foreign keys.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) foreign_keys: Vec<ForeignKey>,
}

/// The columns of a declared primary key, in key order.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PrimaryKey {
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) columns: Vec<String>,
}

/// One declared foreign key.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ForeignKey {
    /// The constraint name, empty when BigQuery reports none.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) referenced_table: Option<TableReference>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) column_references: Vec<ColumnReference>,
}

/// One referencing/referenced column pair of a foreign key.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ColumnReference {
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) referencing_column: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) referenced_column: String,
}

/// The `project.dataset` pair one `datasets.list` entry names.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatasetReference {
    /// The dataset, which is all a listing reports.
    #[serde(default, deserialize_with = "null_as_default")]
    pub(crate) dataset_id: String,
}

#[cfg(test)]
mod tests;
