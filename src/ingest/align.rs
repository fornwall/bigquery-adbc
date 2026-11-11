//! Resolve an append's unspecified types and constraints from the destination table.
//!
//! Null fields and empty list elements inherit the column's type. Decimals inherit its type and
//! shape; STRING and BYTES fields without `BIGQUERY:max_length` inherit its length limit.
//! Normalization enforces these constraints because Storage Write can round decimals and accept
//! overlong strings. Schema comparison reports fields missing from the table or of another shape.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_schema::{DataType, Field, Fields, Schema};

use crate::error::{error, invalid_argument};
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::types::decimal::{BigQueryDecimalType, bigquery_decimal_data_type};
use crate::types::schema::convert_field;
use crate::types::{BIGQUERY_TYPE_METADATA_KEY, BigQueryType, has_bigquery_extension};
use crate::{
    BIGQUERY_FIELD_METADATA_MAX_LENGTH, BIGQUERY_FIELD_METADATA_PRECISION,
    BIGQUERY_FIELD_METADATA_SCALE,
};

use super::schema::{canonical_mode, resolve_column};

/// Whether a normalized schema contains `Null` fields that need the destination table's types.
pub(super) fn has_null_field(fields: &Fields) -> bool {
    fields.iter().any(|field| has_null_type(field.data_type()))
}

fn has_null_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Struct(children) => has_null_field(children),
        DataType::List(item) => has_null_type(item.data_type()),
        _ => false,
    }
}

/// Fill unspecified types and constraints in an append's schema from the destination table.
pub(super) fn align_with_table(schema: &Schema, table: &TableSchema) -> Result<Schema> {
    Ok(Schema::new_with_metadata(
        align_fields(schema.fields(), &table.fields)?,
        schema.metadata().clone(),
    ))
}

fn align_fields(fields: &Fields, columns: &[TableFieldSchema]) -> Result<Fields> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let aligned = match resolve_column(columns, index, field.name()) {
                Some(column) => align_field(field, column)?,
                None => field.as_ref().clone(),
            };
            // A `Null` the table left untyped, in a field it lacks or one whose column has
            // another shape, stands in as a string, so schema comparison reports the difference
            // rather than conversion refusing the type.
            Ok(if has_null_type(aligned.data_type()) {
                null_stand_in(&aligned)
            } else {
                aligned
            })
        })
        .collect()
}

/// `field` with each `Null` in it a string. Every field on the way to a `Null` drops its metadata,
/// such as a typed NULL's `BIGQUERY:type`, which describes a type the stand-in no longer has.
fn null_stand_in(field: &Field) -> Field {
    let child = |child: &Arc<Field>| {
        if has_null_type(child.data_type()) {
            Arc::new(null_stand_in(child))
        } else {
            child.clone()
        }
    };
    let data_type = match field.data_type() {
        DataType::Null => DataType::Utf8,
        DataType::Struct(children) => DataType::Struct(children.iter().map(child).collect()),
        DataType::List(item) => DataType::List(child(item)),
        data_type => data_type.clone(),
    };
    Field::new(field.name(), data_type, field.is_nullable())
}

fn align_field(field: &Field, column: &TableFieldSchema) -> Result<Field> {
    let repeated = canonical_mode(&column.mode) == "REPEATED";
    match field.data_type() {
        DataType::Null if canonical_mode(&column.mode) == "REQUIRED" => {
            Err(invalid_argument(format!(
                "bound column {:?} has Arrow type Null, so every value is NULL, but column {:?} \
                 of the table is REQUIRED",
                field.name(),
                column.name
            )))
        }
        DataType::Null => align_field(&convert_field(column)?.with_name(field.name()), column),
        DataType::List(item) if repeated => match item.data_type() {
            DataType::Null => align_field(&convert_field(column)?.with_name(field.name()), column),
            DataType::Struct(children) if is_record(column) => {
                let item = item
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(align_fields(children, &column.fields)?));
                Ok(field.clone().with_data_type(DataType::List(Arc::new(item))))
            }
            _ => Ok(field
                .clone()
                .with_data_type(DataType::List(Arc::new(settle_values(
                    item, field, column,
                )?)))),
        },
        DataType::Struct(children) if !repeated && is_record(column) => Ok(field
            .clone()
            .with_data_type(DataType::Struct(align_fields(children, &column.fields)?))),
        _ if !repeated => settle_values(field, field, column),
        _ => Ok(field.clone()),
    }
}

fn is_record(column: &TableFieldSchema) -> bool {
    BigQueryType::from_rest(&column.r#type) == Some(BigQueryType::Struct)
}

/// Apply the column's decimal shape or maximum length to a scalar or list item.
/// `field` carries the enclosing column's metadata when `values` is a list item.
fn settle_values(values: &Field, field: &Field, column: &TableFieldSchema) -> Result<Field> {
    match values.data_type() {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            with_column_decimal(values, column)
        }
        _ => Ok(with_column_length(values, field, column)),
    }
}

/// Adopt the destination decimal type, precision and scale. Normalization checks that values
/// fit exactly; sending them directly to Storage Write could round them.
fn with_column_decimal(values: &Field, column: &TableFieldSchema) -> Result<Field> {
    let family = match BigQueryType::from_rest(&column.r#type) {
        Some(BigQueryType::Numeric) => BigQueryDecimalType::Numeric,
        Some(BigQueryType::BigNumeric) => BigQueryDecimalType::BigNumeric,
        _ => return Ok(values.clone()),
    };
    let data_type =
        bigquery_decimal_data_type(family, column.precision, column.scale).map_err(|reason| {
            error(
                format!(
                    "BigQuery {} column {:?} has an invalid decimal shape: {reason}",
                    family.name(),
                    column.name
                ),
                Status::InvalidData,
            )
        })?;
    let mut values = values.clone().with_data_type(data_type);
    let metadata = values.metadata_mut();
    metadata.insert(
        BIGQUERY_TYPE_METADATA_KEY.to_owned(),
        family.name().to_owned(),
    );
    for (key, value) in [
        (BIGQUERY_FIELD_METADATA_PRECISION, column.precision),
        (BIGQUERY_FIELD_METADATA_SCALE, column.scale),
    ] {
        if column.precision == 0 {
            metadata.remove(key);
        } else {
            metadata.insert(key.to_owned(), value.to_string());
        }
    }
    Ok(values)
}

/// Inherit the column's maximum length for plain strings and bytes with no declared limit.
/// Metadata on either the value or enclosing column can exclude logical types such as JSON.
fn with_column_length(values: &Field, field: &Field, column: &TableFieldSchema) -> Field {
    let typed = |field: &Field| {
        has_bigquery_extension(field)
            || field
                .metadata()
                .get(BIGQUERY_TYPE_METADATA_KEY)
                .is_some_and(|name| {
                    !matches!(
                        name.as_str(),
                        "STRING" | "BYTES" | "ARRAY<STRING>" | "ARRAY<BYTES>"
                    )
                })
    };
    if column.max_length == 0
        || !matches!(values.data_type(), DataType::Utf8 | DataType::Binary)
        || values
            .metadata()
            .contains_key(BIGQUERY_FIELD_METADATA_MAX_LENGTH)
        || typed(field)
        || typed(values)
    {
        return values.clone();
    }
    let mut values = values.clone();
    values.metadata_mut().insert(
        BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
        column.max_length.to_string(),
    );
    values
}

#[cfg(test)]
mod tests;
