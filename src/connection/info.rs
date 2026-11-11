//! Static `GetInfo` and `GetTableTypes` results; neither needs a BigQuery request.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::constants;
use adbc_core::error::Result;
use adbc_core::options::InfoCode;
use adbc_core::schemas;
use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray, UInt32Array,
};

use super::arrow::{dense_union, single_batch_reader, union_offset};

/// The REST table types, which `GetTableTypes` lists and `GetObjects` reports (see
/// `bigquery_table_type`). They differ from `INFORMATION_SCHEMA.TABLES`: clones are `TABLE`, for
/// example.
pub(super) const TABLE: &str = "TABLE";
pub(super) const VIEW: &str = "VIEW";
pub(super) const EXTERNAL: &str = "EXTERNAL";
pub(super) const MATERIALIZED_VIEW: &str = "MATERIALIZED_VIEW";
pub(super) const SNAPSHOT: &str = "SNAPSHOT";
pub(super) const TABLE_TYPES: [&str; 5] = [TABLE, VIEW, EXTERNAL, MATERIALIZED_VIEW, SNAPSHOT];

/// The BigQuery client library release the driver is built against, reported as the vendor
/// version like the Go driver reports its `cloud.google.com/go/bigquery` dependency.
///
/// Kept in lockstep with the `google-cloud-bigquery` pin in `Cargo.toml` by hand;
/// `vendor_version_matches_the_pinned_client_library` fails when the pin moves without this.
const VENDOR_VERSION: &str = "google-cloud-bigquery 1.0.0";

enum InfoValue {
    String(&'static str),
    /// The linked Arrow crate's version with a `v` prefix, the module-version form Go drivers
    /// report and Driver Foundry's `test_get_info_arrow_version` requires.
    DriverArrowVersion,
    Bool(bool),
    Int64(i64),
    /// A recognized code whose value is unknown or inapplicable: the spec's "or null" cases.
    Null,
}

/// Recognized codes in ascending order. Unknown codes are omitted; recognized codes with
/// no applicable value return null, as adbc.h requires.
const INFO: [(InfoCode, InfoValue); 26] = [
    (InfoCode::VendorName, InfoValue::String("Google BigQuery")),
    (InfoCode::VendorVersion, InfoValue::String(VENDOR_VERSION)),
    // BigQuery publishes no server-side Arrow version; the linked Arrow crate's version is
    // `DriverArrowVersion`.
    (InfoCode::VendorArrowVersion, InfoValue::Null),
    (InfoCode::VendorSql, InfoValue::Bool(true)),
    (InfoCode::VendorSubstrait, InfoValue::Bool(false)),
    // adbc.h types these as "utf8, or null if Substrait is not supported".
    (InfoCode::VendorSubstraitMinVersion, InfoValue::Null),
    (InfoCode::VendorSubstraitMaxVersion, InfoValue::Null),
    (InfoCode::DriverName, InfoValue::String("bigquery-adbc")),
    (
        InfoCode::DriverVersion,
        InfoValue::String(env!("CARGO_PKG_VERSION")),
    ),
    (InfoCode::DriverArrowVersion, InfoValue::DriverArrowVersion),
    (
        InfoCode::DriverAdbcVersion,
        InfoValue::Int64(constants::ADBC_VERSION_1_1_0 as i64),
    ),
    // ADBC 1.1 reserves [500, 1000) for Flight SQL's XDBC codes. `adbc_core` has no variants
    // for these. Include only documented answers with an unambiguous ADBC union type.
    // SQL_DDL_CATALOG: GoogleSQL has no CREATE/DROP for projects.
    (InfoCode::Other(500), InfoValue::Bool(false)),
    // SQL_DDL_SCHEMA: CREATE SCHEMA and DROP SCHEMA manage datasets.
    (InfoCode::Other(501), InfoValue::Bool(true)),
    // SQL_DDL_TABLE: CREATE TABLE and DROP TABLE.
    (InfoCode::Other(502), InfoValue::Bool(true)),
    // SQL_IDENTIFIER_QUOTE_CHAR: GoogleSQL quotes identifiers with backticks.
    (InfoCode::Other(504), InfoValue::String("`")),
    // Omit SQL_SEARCH_STRING_ESCAPE (513): it describes metadata filters, for which ADBC
    // defines no escaping. GoogleSQL's LIKE escape would mislead metadata callers.
    // SQL_SUPPORTS_COLUMN_ALIASING: GoogleSQL's SELECT list takes `expression AS alias`.
    (InfoCode::Other(515), InfoValue::Bool(true)),
    // SQL_NULL_PLUS_NULL_IS_NULL: `||`, CONCAT and `+` all return NULL for a NULL argument.
    (InfoCode::Other(516), InfoValue::Bool(true)),
    // SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE: GoogleSQL's LIKE takes no ESCAPE clause at all; a
    // wildcard is escaped with a backslash inside the pattern itself.
    (InfoCode::Other(523), InfoValue::Bool(false)),
    // SQL_SUPPORTS_NON_NULLABLE_COLUMNS: CREATE TABLE takes a NOT NULL column attribute.
    (InfoCode::Other(524), InfoValue::Bool(true)),
    // SQL_SCHEMA_TERM: BigQuery's name for an ADBC db_schema.
    (InfoCode::Other(529), InfoValue::String("dataset")),
    // SQL_CATALOG_TERM: BigQuery's name for an ADBC catalog.
    (InfoCode::Other(531), InfoValue::String("project")),
    // SQL_CATALOG_AT_START: qualified names read `project.dataset.table`.
    (InfoCode::Other(532), InfoValue::Bool(true)),
    // SQL_MAX_COLUMN_NAME_LENGTH: BigQuery column names are at most 300 characters.
    (InfoCode::Other(543), InfoValue::Int64(300)),
    // SQL_MAX_COLUMNS_IN_TABLE: BigQuery allows at most 10,000 columns in a table.
    (InfoCode::Other(548), InfoValue::Int64(10_000)),
    // SQL_DB_SCHEMA_NAME_LENGTH: dataset IDs are at most 1,024 characters, as
    // `validate_dataset_id` enforces.
    (InfoCode::Other(552), InfoValue::Int64(1_024)),
    // Omit SQL_MAX_CATALOG_NAME_LENGTH (554): the modern project-ID limit does not cover
    // legacy domain-scoped IDs, and project identifier validation belongs to BigQuery.
    // SQL_MAX_TABLE_NAME_LENGTH (559) is left out: BigQuery documents the table-name limit in
    // UTF-8 bytes, and the info code counts characters.
    // SQL_TRANSACTIONS_SUPPORTED: manual-commit mode runs BEGIN/COMMIT/ROLLBACK TRANSACTION in a
    // session.
    (InfoCode::Other(563), InfoValue::Bool(true)),
];

pub(crate) fn info_reader(
    requested: Option<&HashSet<InfoCode>>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let capacity = requested.map_or(INFO.len(), |codes| codes.len().min(INFO.len()));
    let driver_arrow_version = format!("v{}", arrow_array::ARROW_VERSION);
    let selected = INFO
        .iter()
        .filter(|(code, _)| requested.is_none_or(|requested| requested.contains(code)));
    let mut names = Vec::with_capacity(capacity);
    let mut strings: Vec<Option<&str>> = Vec::new();
    let mut booleans = Vec::new();
    let mut integers = Vec::new();
    let mut type_ids = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(capacity);

    for (code, value) in selected {
        names.push(u32::from(code));
        let (type_id, offset) = match value {
            InfoValue::String(value) => (0, union_offset(&mut strings, Some(*value))),
            InfoValue::DriverArrowVersion => (
                0,
                union_offset(&mut strings, Some(driver_arrow_version.as_str())),
            ),
            // A null value rides in the string member: the union's children are nullable, and
            // every spec code with an "or null" reading is typed utf8.
            InfoValue::Null => (0, union_offset(&mut strings, None)),
            InfoValue::Bool(value) => (1, union_offset(&mut booleans, *value)),
            InfoValue::Int64(value) => (2, union_offset(&mut integers, *value)),
        };
        type_ids.push(type_id);
        offsets.push(offset);
    }

    // The int32-bitmask, string-list and int32-to-int32-list-map members carry nothing the
    // driver reports, so `dense_union` builds them empty from the schema constant.
    let values = dense_union(
        schemas::GET_INFO_SCHEMA.field(1).data_type(),
        type_ids,
        offsets,
        &mut |type_id| match type_id {
            0 => Some(Arc::new(StringArray::from(std::mem::take(&mut strings))) as ArrayRef),
            1 => Some(Arc::new(BooleanArray::from(std::mem::take(&mut booleans)))),
            2 => Some(Arc::new(Int64Array::from(std::mem::take(&mut integers)))),
            _ => None,
        },
    )?;

    let batch = RecordBatch::try_new(
        schemas::GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(values)],
    )?;
    Ok(single_batch_reader(batch))
}

pub(crate) fn table_types_reader() -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let batch = RecordBatch::try_new(
        schemas::GET_TABLE_TYPES_SCHEMA.clone(),
        vec![Arc::new(StringArray::from_iter_values(TABLE_TYPES))],
    )?;
    Ok(single_batch_reader(batch))
}

#[cfg(test)]
mod tests;
