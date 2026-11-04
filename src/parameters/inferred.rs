//! The Arrow schema of the parameters a dry run infers, for `AdbcStatementGetParameterSchema`.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_schema::{DataType, Field, Metadata, Schema};

use crate::error::{error, not_implemented};
use crate::rest::model::{QueryParameter, QueryParameterType};
use crate::types::{
    BIGQUERY_TYPE_METADATA_KEY, BigQueryType, RangeElement, insert_extension_metadata,
};

/// Convert BigQuery's server-inferred query parameters to the ADBC parameter schema.
pub(crate) fn parameter_schema(parameters: &[QueryParameter]) -> Result<Schema> {
    parameters
        .iter()
        .map(|parameter| parameter_field(&parameter.name, parameter.parameter_type.as_ref()))
        .collect::<Result<Vec<_>>>()
        .map(Schema::new)
}

fn parameter_field(name: &str, parameter_type: Option<&QueryParameterType>) -> Result<Field> {
    let Some(parameter_type) = parameter_type else {
        return Ok(Field::new(name, DataType::Null, true));
    };
    let mut metadata = Metadata::new();
    let data_type = match parameter_type.r#type.as_str() {
        "" => DataType::Null,
        "ARRAY" => {
            let item = parameter_field("item", parameter_type.array_type.as_deref())?;
            if let Some(item_type) = item.metadata().get(BIGQUERY_TYPE_METADATA_KEY) {
                metadata.insert(
                    BIGQUERY_TYPE_METADATA_KEY.to_owned(),
                    format!("ARRAY<{item_type}>"),
                );
            }
            DataType::List(Arc::new(item))
        }
        "STRUCT" | "RECORD" => DataType::Struct(
            parameter_type
                .struct_types
                .iter()
                .map(|field| parameter_field(&field.name, field.r#type.as_deref()).map(Arc::new))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        "RANGE" => {
            let element_type = parameter_type
                .range_element_type
                .as_deref()
                .ok_or_else(|| {
                    error(
                        format!("BigQuery inferred RANGE parameter {name:?} has no element type"),
                        Status::InvalidData,
                    )
                })?;
            let Some(element) = RangeElement::from_rest(&element_type.r#type) else {
                return Err(error(
                    format!(
                        "BigQuery inferred RANGE parameter {name:?} has unsupported element type \
                         {}; use DATE, DATETIME, or TIMESTAMP",
                        element_type.r#type
                    ),
                    Status::InvalidData,
                ));
            };
            // The bounds go through the same recursion as any other scalar, so a RANGE<TIMESTAMP>
            // whose element declares sub-microsecond precision is refused the same way a bare
            // TIMESTAMP parameter of that precision is.
            let start = parameter_field("start", Some(element_type))?;
            let end = parameter_field("end", Some(element_type))?;
            metadata.insert(
                BIGQUERY_TYPE_METADATA_KEY.to_owned(),
                format!("RANGE<{}>", element.sql()),
            );
            DataType::Struct(vec![Arc::new(start), Arc::new(end)].into())
        }
        type_name => {
            let unimplemented = || {
                not_implemented(format!(
                    "BigQuery query parameter {name:?} has inferred type {type_name}, which this \
                     driver does not implement"
                ))
            };
            // Every scalar spelling BigQuery may report (REST, GoogleSQL, or alias) resolves
            // through the one type table; ARRAY, RANGE, and STRUCT are matched above.
            let Some(kind) = BigQueryType::from_rest(type_name) else {
                return Err(unimplemented());
            };
            if kind == BigQueryType::Timestamp
                && let Some(precision) = parameter_type.timestamp_precision
                && precision > 6
            {
                return Err(not_implemented(format!(
                    "BigQuery query parameter {name:?} is TIMESTAMP({precision}), and the driver \
                     supports at most microsecond precision; CAST it to TIMESTAMP(6) or narrower"
                )));
            }
            if matches!(kind, BigQueryType::Geography | BigQueryType::Json) {
                metadata.insert(BIGQUERY_TYPE_METADATA_KEY.to_owned(), kind.sql().to_owned());
                insert_extension_metadata(kind, &mut metadata);
            }
            // Every composite spelling is matched above, so this resolves; a BigQuery type that
            // grows a composite spelling later refuses rather than aborting the host process.
            kind.canonical_data_type().ok_or_else(unimplemented)?
        }
    };
    Ok(Field::new(name, data_type, true).with_metadata(metadata))
}
