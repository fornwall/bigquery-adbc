//! The REST result schema, converted into the driver's canonical Arrow schema.
//!
//! Each [`TableFieldSchema`] becomes an Arrow [`Field`] carrying the GoogleSQL type literal that
//! the decoder and consumers read, so the converted schema alone describes a result.

use std::borrow::Cow;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_schema::{DataType, Field, Fields, Metadata, Schema, SchemaRef};

use crate::error::{error, not_implemented};
use crate::identifiers::write_quoted_identifier;
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::types::decimal::{BigQueryDecimalType, bigquery_decimal_data_type};
use crate::types::{
    ARROW_EXTENSION_METADATA_KEY, ARROW_EXTENSION_NAME_KEY, BIGQUERY_TYPE_METADATA_KEY,
    BigQueryType, MAX_SCHEMA_NESTING_DEPTH, RangeElement, insert_extension_metadata,
};
use crate::{
    BIGQUERY_FIELD_METADATA_MAX_LENGTH, BIGQUERY_FIELD_METADATA_PRECISION,
    BIGQUERY_FIELD_METADATA_SCALE,
};

pub(crate) fn convert_schema(table_schema: &TableSchema) -> Result<SchemaRef> {
    let fields = table_schema
        .fields
        .iter()
        .map(|field| convert_field_inner(field, 1))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Converts one BigQuery REST field into its Arrow field.
///
/// The [`crate::rows`] decoder reads the Arrow field alone, so it must say everything the decoder
/// distinguishes: TIMESTAMP from DATETIME by the timezone, the decimals by their width, and RANGE
/// by its `BIGQUERY:type` metadata. That metadata is a GoogleSQL type literal, not the REST
/// spelling (`FLOAT`, `RECORD`), so a consumer can use it in a `CAST` or DDL.
///
/// A decimal is always NUMERIC's or BIGNUMERIC's full type, whatever precision and scale a table
/// declares: query results never report a declared shape, and a table's schema must match what
/// reading it returns, which clients such as DuckDB's `adbc_scan_table` check type for type. The
/// declared shape is metadata instead (see [`field_metadata`]).
pub(crate) fn convert_field(field: &TableFieldSchema) -> Result<Field> {
    convert_field_inner(field, 1)
}

fn convert_field_inner(field: &TableFieldSchema, depth: usize) -> Result<Field> {
    // Bound recursion before visiting children: schemas come from the network, and a stack
    // overflow aborts the host rather than unwinding through the FFI guard. The later type
    // literal and row-builder walks see only fields that passed this check.
    if depth > MAX_SCHEMA_NESTING_DEPTH {
        return Err(error(
            format!(
                "BigQuery result field {:?} is nested {depth} levels deep; the maximum is \
                 {MAX_SCHEMA_NESTING_DEPTH}",
                field.name
            ),
            Status::InvalidData,
        ));
    }
    if !matches!(
        field.mode.as_str(),
        "" | "NULLABLE" | "REQUIRED" | "REPEATED"
    ) {
        return Err(error(
            format!(
                "BigQuery returned unknown mode {:?} for field {:?}; expected NULLABLE, \
                 REQUIRED, or REPEATED",
                field.mode, field.name
            ),
            Status::InvalidData,
        ));
    }

    let kind = BigQueryType::from_rest(&field.r#type).ok_or_else(|| {
        not_implemented(format!(
            "BigQuery column {:?} has type {}, which this driver does not implement; exclude it \
             from the query or CAST it to a supported type",
            field.name, field.r#type
        ))
    })?;

    if kind != BigQueryType::Struct && !field.fields.is_empty() {
        return Err(error(
            format!(
                "BigQuery field {:?} of type {} unexpectedly has nested fields",
                field.name, field.r#type
            ),
            Status::InvalidData,
        ));
    }
    if kind != BigQueryType::Range && field.range_element_type.is_some() {
        return Err(error(
            format!(
                "BigQuery field {:?} of type {} unexpectedly has a RANGE element type",
                field.name, field.r#type
            ),
            Status::InvalidData,
        ));
    }
    // The canonical temporal types are microsecond, so a column declaring more sub-second digits
    // would decode wrongly rather than fail. BigQuery does not yet accept such columns ("Timestamp
    // precision type parameter is not supported"); this covers the field arriving populated.
    if matches!(
        kind,
        BigQueryType::Timestamp | BigQueryType::DateTime | BigQueryType::Time
    ) && let Some(digits) = declared_subsecond_digits(field)
        && digits > MAX_SUBSECOND_DIGITS
    {
        return Err(not_implemented(format!(
            "BigQuery column {:?} is {}({digits}), and the driver supports at most \
             microsecond precision; CAST it to {}({MAX_SUBSECOND_DIGITS}) or narrower",
            field.name, field.r#type, field.r#type
        )));
    }

    let data_type = match kind {
        BigQueryType::Range => {
            let element_name = field
                .range_element_type
                .as_ref()
                .map(|element| element.r#type.as_str())
                .ok_or_else(|| {
                    error(
                        format!(
                            "BigQuery RANGE field {:?} is missing its element type",
                            field.name
                        ),
                        Status::InvalidData,
                    )
                })?;
            let Some(element) = RangeElement::from_rest(element_name) else {
                return Err(error(
                    format!(
                        "BigQuery RANGE field {:?} has unsupported element type \
                         {element_name}; use DATE, DATETIME, or TIMESTAMP",
                        field.name
                    ),
                    Status::InvalidData,
                ));
            };
            let element_data_type = element.canonical_arrow_type();
            let fields: Fields = vec![
                Field::new("start", element_data_type.clone(), true),
                Field::new("end", element_data_type, true),
            ]
            .into();
            DataType::Struct(fields)
        }
        BigQueryType::Struct => DataType::Struct(
            field
                .fields
                .iter()
                .map(|child| convert_field_inner(child, depth + 1))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        scalar => scalar
            .canonical_data_type()
            .expect("Range and Struct, the only composite types, are matched above"),
    };
    let rich_type = match kind {
        BigQueryType::Struct | BigQueryType::Range => {
            let mut literal = String::new();
            write_type_literal(&mut literal, field, false).expect(
                "the field and every child were converted, so each has an implemented type",
            );
            Cow::Owned(literal)
        }
        scalar => Cow::Borrowed(scalar.sql()),
    };
    let mut metadata = field_metadata(field, kind, &rich_type)?;

    let data_type = if field.mode == "REPEATED" {
        let mut item_metadata =
            Metadata::new().with(BIGQUERY_TYPE_METADATA_KEY, rich_type.as_ref());
        for key in [ARROW_EXTENSION_NAME_KEY, ARROW_EXTENSION_METADATA_KEY] {
            if let Some(value) = metadata.remove(key) {
                item_metadata.insert(key.to_owned(), value);
            }
        }
        metadata.insert(
            BIGQUERY_TYPE_METADATA_KEY.to_owned(),
            format!("ARRAY<{rich_type}>"),
        );
        metadata.insert("Repeated".to_owned(), "true".to_owned());
        DataType::List(Arc::new(
            Field::new("item", data_type, true).with_metadata(item_metadata),
        ))
    } else {
        data_type
    };

    Ok(Field::new(&field.name, data_type, field.mode != "REQUIRED").with_metadata(metadata))
}

/// The GoogleSQL type literal of a REST field, exactly as its converted field's `BIGQUERY:type`
/// metadata spells it, without converting it: `None` for a type the driver does not implement,
/// at any depth.
pub(crate) fn googlesql_type_literal(field: &TableFieldSchema) -> Option<String> {
    let mut literal = String::new();
    write_type_literal(&mut literal, field, field.mode == "REPEATED")?;
    Some(literal)
}

/// Writes `field`'s type literal: the full `STRUCT<...>` or `RANGE<...>` shape, inside
/// `ARRAY<...>` when `repeated`.
fn write_type_literal(
    literal: &mut String,
    field: &TableFieldSchema,
    repeated: bool,
) -> Option<()> {
    if repeated {
        literal.push_str("ARRAY<");
    }
    match BigQueryType::from_rest(&field.r#type)? {
        BigQueryType::Range => {
            let element = RangeElement::from_rest(&field.range_element_type.as_ref()?.r#type)?;
            literal.push_str("RANGE<");
            literal.push_str(element.sql());
            literal.push('>');
        }
        BigQueryType::Struct => {
            literal.push_str("STRUCT<");
            for (index, child) in field.fields.iter().enumerate() {
                if index != 0 {
                    literal.push_str(", ");
                }
                write_quoted_identifier(literal, &[&child.name]);
                literal.push(' ');
                write_type_literal(literal, child, child.mode == "REPEATED")?;
            }
            literal.push('>');
        }
        scalar => literal.push_str(scalar.sql()),
    }
    if repeated {
        literal.push('>');
    }
    Some(())
}

/// The Arrow metadata one converted BigQuery field carries: this driver's `BIGQUERY:*` keys, the
/// Go driver's legacy spellings of the same values, and the column properties BigQuery reported.
/// Each property key is present only where BigQuery reported a value (G66).
fn field_metadata(
    field: &TableFieldSchema,
    kind: BigQueryType,
    rich_type: &str,
) -> Result<Metadata> {
    if field.max_length < 0
        || (field.max_length > 0 && !matches!(kind, BigQueryType::String | BigQueryType::Bytes))
    {
        return Err(error(
            format!(
                "BigQuery field {:?} of type {} has invalid max length {}",
                field.name, field.r#type, field.max_length
            ),
            Status::InvalidData,
        ));
    }

    let mut metadata = Metadata::new()
        .with(BIGQUERY_TYPE_METADATA_KEY, rich_type)
        // The Go driver's legacy spelling: the REST type name exactly as BigQuery reported it
        // (`INTEGER`, `FLOAT`, `BOOLEAN`, `RECORD`, ...). dbt-core 2.0 parses `Type`, `Repeated`,
        // and `Description` to reconstruct composite type strings and column comments.
        .with("Type", field.r#type.clone());
    if let Some(description) = field
        .description
        .as_ref()
        .filter(|description| !description.is_empty())
    {
        metadata.insert("Description".to_owned(), description.clone());
    }
    insert_extension_metadata(kind, &mut metadata);
    if field.max_length > 0 {
        let max_length = field.max_length.to_string();
        // `MaxLength` is the Go driver's key for the same value.
        metadata.insert("MaxLength".to_owned(), max_length.clone());
        metadata.insert(BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(), max_length);
    }
    if let Some((precision, scale)) = declared_decimal_shape(field, kind)? {
        metadata.insert(BIGQUERY_FIELD_METADATA_PRECISION.to_owned(), precision);
        metadata.insert(BIGQUERY_FIELD_METADATA_SCALE.to_owned(), scale);
    }
    if let Some(collation) = field
        .collation
        .as_ref()
        .and_then(|collation| collation.as_str())
        .filter(|collation| !collation.is_empty())
    {
        metadata.insert("Collation".to_owned(), collation.to_owned());
    }
    // BigQuery's own `{"names":[...]}`, where Go writes its client struct's `{"Names":[...]}`.
    if let Some(policy_tags) = field
        .policy_tags
        .as_ref()
        .filter(|policy_tags| !policy_tags.names.is_empty())
        && let Ok(encoded) = serde_json::to_string(policy_tags)
    {
        metadata.insert("PolicyTags".to_owned(), encoded);
    }
    Ok(metadata)
}

/// The precision and scale a NUMERIC or BIGNUMERIC column declares, `None` for one that declares
/// none. BigQuery omits a zero scale, so `NUMERIC(10)` reports precision 10 alone.
fn declared_decimal_shape(
    field: &TableFieldSchema,
    kind: BigQueryType,
) -> Result<Option<(String, String)>> {
    let bigquery_type = match kind {
        BigQueryType::Numeric => BigQueryDecimalType::Numeric,
        BigQueryType::BigNumeric => BigQueryDecimalType::BigNumeric,
        _ => return Ok(None),
    };
    if field.precision == 0 && field.scale == 0 {
        return Ok(None);
    }
    bigquery_decimal_data_type(bigquery_type, field.precision, field.scale).map_err(|reason| {
        error(
            format!(
                "BigQuery {} field {:?} has invalid decimal shape: {reason}",
                bigquery_type.name(),
                field.name
            ),
            Status::InvalidData,
        )
    })?;
    Ok(Some((field.precision.to_string(), field.scale.to_string())))
}

/// The sub-second digits an Arrow microsecond temporal type holds.
const MAX_SUBSECOND_DIGITS: i64 = 6;

/// The sub-second digits a temporal column declares in `timestampPrecision`, which stays raw
/// JSON so a schema patch can echo it back verbatim; an int64 may be a JSON string or number.
fn declared_subsecond_digits(field: &TableFieldSchema) -> Option<i64> {
    let declared = field.timestamp_precision.as_ref()?;
    declared
        .as_i64()
        .or_else(|| declared.as_str()?.parse().ok())
}

#[cfg(test)]
mod tests;
