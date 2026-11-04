//! REST schema fixtures.
//!
//! Use struct updates for additional properties, for example:
//! `TableFieldSchema { max_length: 64, ..field("label", "STRING") }`.

use crate::rest::model::{FieldElementType, TableFieldSchema, TableSchema};

/// A column of `kind`, in BigQuery's default `NULLABLE` mode.
pub(crate) fn field(name: impl Into<String>, kind: impl Into<String>) -> TableFieldSchema {
    TableFieldSchema {
        name: name.into(),
        r#type: kind.into(),
        ..TableFieldSchema::default()
    }
}

pub(crate) fn field_mode(
    name: impl Into<String>,
    kind: impl Into<String>,
    mode: impl Into<String>,
) -> TableFieldSchema {
    TableFieldSchema {
        mode: mode.into(),
        ..field(name, kind)
    }
}

/// A column whose nested columns are `children`, under either the REST spelling of its type
/// (`RECORD`) or the GoogleSQL one (`STRUCT`).
pub(crate) fn record(
    name: impl Into<String>,
    kind: impl Into<String>,
    children: impl IntoIterator<Item = TableFieldSchema>,
) -> TableFieldSchema {
    TableFieldSchema {
        fields: children.into_iter().collect(),
        ..field(name, kind)
    }
}

/// BigQuery reports both precision and scale, including zero scale for `NUMERIC(10)`.
pub(crate) fn decimal(base: TableFieldSchema, precision: i64, scale: i64) -> TableFieldSchema {
    TableFieldSchema {
        precision,
        scale,
        ..base
    }
}

/// A nullable `RANGE` column over `element_type`.
pub(crate) fn range_field(name: &str, element_type: &str) -> TableFieldSchema {
    TableFieldSchema {
        mode: "NULLABLE".to_owned(),
        range_element_type: Some(FieldElementType {
            r#type: element_type.to_owned(),
        }),
        ..field(name, "RANGE")
    }
}

pub(crate) fn table_schema(fields: impl IntoIterator<Item = TableFieldSchema>) -> TableSchema {
    TableSchema {
        fields: fields.into_iter().collect(),
        ..TableSchema::default()
    }
}
