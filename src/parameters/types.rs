//! The BigQuery parameter type of a bound Arrow field, derived once per binding.

use adbc_core::error::Result;
use arrow_schema::{DataType, Field, Fields};

use crate::error::{invalid_argument, not_implemented};
use crate::rest::model::{QueryParameterStructType, QueryParameterType};
use crate::types::decimal::{BigQueryDecimalType, decimal_mapping};
use crate::types::{
    BIGQUERY_TYPE_METADATA_KEY, BigQueryType, PARAMETER_FIELD, array_item_field, case_duplicate,
    decimal_metadata_type_hint, incompatible_metadata_error, metadata_type, nested_array_error,
    range_field_element, scalar_arrow_field_type,
};

pub(super) fn parameter_type(field: &Field) -> Result<QueryParameterType> {
    parameter_type_for(field.data_type(), field, None)
}

fn named(name: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: name.to_owned(),
        ..Default::default()
    }
}

/// Resolves a field's `BIGQUERY:type` metadata against the BigQuery type its Arrow type implies,
/// for the `Null` and `Duration` types [`scalar_arrow_field_type`] leaves to this module; without
/// it their metadata would be silently dropped rather than honored or refused.
///
/// A `Null` array holds only NULLs, which every scalar type has, so its metadata may name any
/// scalar type: a typed NULL needs no typed array. Unannotated, it binds as a `STRING` NULL.
fn implied_scalar_type(
    field: &Field,
    data_type: &DataType,
    implied: BigQueryType,
) -> Result<BigQueryType> {
    let Some(name) = field.metadata().get(BIGQUERY_TYPE_METADATA_KEY) else {
        return Ok(implied);
    };
    let implied = match data_type {
        DataType::Null => BigQueryType::from_sql(name),
        _ => Some(implied),
    };
    metadata_type(field, name, implied, PARAMETER_FIELD)
}

fn parameter_type_for(
    data_type: &DataType,
    field: &Field,
    inherited_decimal_type: Option<BigQueryDecimalType>,
) -> Result<QueryParameterType> {
    if let Some(kind) = scalar_arrow_field_type(data_type, field, PARAMETER_FIELD)? {
        return Ok(named(kind.sql()));
    }
    match data_type {
        DataType::Null => Ok(named(
            implied_scalar_type(field, data_type, BigQueryType::String)?.sql(),
        )),
        // Bulk ingest maps every Arrow duration to INTERVAL, so a column the caller ingested has
        // to be bindable as a parameter to filter against it.
        DataType::Duration(_) => Ok(named(
            implied_scalar_type(field, data_type, BigQueryType::Interval)?.sql(),
        )),
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            let requested = decimal_metadata_type_hint(
                data_type,
                field,
                inherited_decimal_type,
                PARAMETER_FIELD,
            )?;
            let mapping = decimal_mapping(data_type, requested).map_err(|reason| {
                invalid_argument(format!(
                    "Arrow decimal parameter field {:?} is invalid: {reason}",
                    field.name()
                ))
            })?;
            Ok(named(mapping.bigquery_type.name()))
        }
        DataType::List(item)
        | DataType::ListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::LargeList(item)
        | DataType::LargeListView(item) => {
            let item = array_item_field(field, item)?;
            let decimal_type = decimal_metadata_type_hint(data_type, field, None, PARAMETER_FIELD)?;
            let element_type = parameter_type_for(item.data_type(), &item, decimal_type)?;
            if element_type.r#type == "ARRAY" {
                return Err(nested_array_error(field));
            }
            Ok(QueryParameterType {
                array_type: Some(Box::new(element_type)),
                ..named("ARRAY")
            })
        }
        DataType::Dictionary(key_type, value_type) if key_type.is_dictionary_key_type() => {
            parameter_type_for(value_type, field, inherited_decimal_type)
        }
        DataType::Dictionary(key_type, _) => Err(invalid_argument(format!(
            "Arrow dictionary parameter field {:?} has key type {key_type}, which is not an \
             integer",
            field.name()
        ))),
        DataType::Struct(fields) => {
            if let Some(element) = range_field_element(field, fields, PARAMETER_FIELD)? {
                return Ok(QueryParameterType {
                    range_element_type: Some(Box::new(named(element.sql()))),
                    ..named(BigQueryType::Range.sql())
                });
            }
            if let Some(rich_type) = field.metadata().get(BIGQUERY_TYPE_METADATA_KEY)
                && !rich_type.starts_with("STRUCT<")
            {
                return Err(incompatible_metadata_error(field, rich_type));
            }
            struct_parameter_type(field, fields)
        }
        data_type => Err(not_implemented(format!(
            "Arrow field {:?} has type {data_type}, which no BigQuery query parameter type covers",
            field.name()
        ))),
    }
}

fn struct_parameter_type(parent: &Field, fields: &Fields) -> Result<QueryParameterType> {
    if fields.iter().any(|field| field.name().is_empty()) {
        return Err(invalid_argument(format!(
            "Arrow struct parameter field {:?} has a child with an empty name; BigQuery struct \
             fields must be named",
            parent.name()
        )));
    }
    if let Some(((_, first), (_, duplicate))) = case_duplicate(fields) {
        let named = if first == duplicate {
            format!("more than one child named {first:?}")
        } else {
            format!("children named {first:?} and {duplicate:?}")
        };
        return Err(invalid_argument(format!(
            "Arrow struct parameter field {:?} has {named}; BigQuery struct field names are \
             case-insensitive and must be unique",
            parent.name()
        )));
    }
    let struct_types = fields
        .iter()
        .map(|field| {
            Ok(QueryParameterStructType {
                name: field.name().clone(),
                r#type: Some(Box::new(parameter_type(field)?)),
            })
        })
        .collect::<Result<_>>()?;
    Ok(QueryParameterType {
        struct_types,
        ..named("STRUCT")
    })
}
