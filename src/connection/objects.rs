//! ADBC object metadata representation and Arrow conversion.
//!
//! The model here is what `GetObjects` reports; [`rows`] translates one listed BigQuery table
//! into it.

mod rows;

pub(super) use rows::object_table;

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::schemas;
use arrow_array::{ArrayRef, Int16Array, RecordBatchReader, StringArray, StructArray};
use arrow_schema::DataType;

use super::arrow::{
    boolean_column, catalog_batch, int16_column, int32_column, nested_list_array, nested_struct,
    optional_int16_column, optional_int32_column, optional_string_column, string_column,
    struct_fields,
};
use crate::rest::model::TableFieldSchema;
use crate::types::BigQueryType;

#[derive(Debug)]
pub(crate) struct ObjectCatalog {
    pub(crate) name: String,
    pub(crate) db_schemas: Option<Vec<ObjectDbSchema>>,
}

#[derive(Debug)]
pub(crate) struct ObjectDbSchema {
    pub(crate) name: String,
    pub(crate) tables: Option<Vec<ObjectTable>>,
}

#[derive(Debug)]
pub(crate) struct ObjectTable {
    pub(crate) name: String,
    pub(crate) table_type: String,
    pub(crate) columns: Option<Vec<ObjectColumn>>,
    pub(crate) constraints: Option<Vec<ObjectConstraint>>,
}

#[derive(Debug)]
pub(crate) struct ObjectColumn {
    pub(crate) field: TableFieldSchema,
    pub(crate) ordinal_position: i32,
    /// The GoogleSQL type literal `xdbc_type_name` reports, which is the same spelling
    /// `get_table_schema` publishes for the column; see [`crate::types::schema::googlesql_type_literal`].
    pub(crate) type_name: String,
}

impl ObjectColumn {
    fn is_nullable(&self) -> bool {
        // A repeated column can be empty, but cannot contain a NULL array.
        !matches!(self.field.mode.as_str(), "REQUIRED" | "REPEATED")
    }
}

#[derive(Debug)]
pub(crate) struct ObjectConstraint {
    pub(crate) name: Option<String>,
    pub(crate) constraint_type: &'static str,
    pub(crate) column_names: Vec<String>,
    pub(crate) column_usage: Option<Vec<ObjectColumnUsage>>,
}

#[derive(Debug)]
pub(crate) struct ObjectColumnUsage {
    /// `None` where BigQuery named no project, which `USAGE_SCHEMA` types as nullable so the
    /// consumer resolves the reference against the current catalog.
    pub(crate) catalog: Option<String>,
    /// `None` where BigQuery named no dataset, nullable for the same reason as `catalog`.
    pub(crate) db_schema: Option<String>,
    pub(crate) table: String,
    pub(crate) column: String,
}

pub(crate) fn objects_reader(
    catalogs: &[ObjectCatalog],
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    catalog_batch(
        &schemas::GET_OBJECTS_SCHEMA,
        catalogs,
        |catalog| catalog.name.as_str(),
        |catalog| catalog.db_schemas.as_deref(),
        &schemas::OBJECTS_DB_SCHEMA_SCHEMA,
        |db_schemas| Ok(Arc::new(build_db_schemas(&db_schemas)?) as ArrayRef),
    )
}

fn build_db_schemas(db_schemas: &[&ObjectDbSchema]) -> Result<StructArray> {
    nested_struct(
        &schemas::OBJECTS_DB_SCHEMA_SCHEMA,
        db_schemas,
        |schema| schema.name.as_str(),
        |schema| schema.tables.as_deref(),
        &schemas::TABLE_SCHEMA,
        |tables| Ok(Arc::new(build_tables(&tables)?) as ArrayRef),
    )
}

fn build_tables(tables: &[&ObjectTable]) -> Result<StructArray> {
    let names = string_column(&mut tables.iter().map(|table| table.name.as_str()));
    let types = string_column(&mut tables.iter().map(|table| table.table_type.as_str()));

    let table_columns = nested_list_array(
        tables,
        |table| table.columns.as_deref(),
        &schemas::COLUMN_SCHEMA,
        |columns| Ok(Arc::new(build_columns(&columns)?) as ArrayRef),
    )?;
    let table_constraints = nested_list_array(
        tables,
        |table| table.constraints.as_deref(),
        &schemas::CONSTRAINT_SCHEMA,
        |constraints| Ok(Arc::new(build_constraints(&constraints)?) as ArrayRef),
    )?;

    Ok(StructArray::try_new(
        struct_fields(&schemas::TABLE_SCHEMA),
        vec![
            Arc::new(names),
            Arc::new(types),
            Arc::new(table_columns),
            Arc::new(table_constraints),
        ],
        None,
    )?)
}

fn build_columns(columns: &[&ObjectColumn]) -> Result<StructArray> {
    let names = string_column(&mut columns.iter().map(|column| column.field.name.as_str()));
    let ordinal_positions = int32_column(&mut columns.iter().map(|column| column.ordinal_position));
    let remarks = optional_string_column(
        &mut columns
            .iter()
            .map(|column| column.field.description.as_deref()),
    );
    // The GoogleSQL type literal `get_table_schema` publishes as `BIGQUERY:type`, not the REST
    // spelling the Go driver reports (G77).
    let xdbc_type_names =
        string_column(&mut columns.iter().map(|column| column.type_name.as_str()));
    // A declared STRING/BYTES length, or a decimal precision; a type carrying neither has no
    // column size to report.
    let column_sizes = optional_int32_column(&mut columns.iter().map(|column| {
        let size = if column.field.max_length != 0 {
            column.field.max_length
        } else {
            column.field.precision
        };
        i32::try_from(size).ok().filter(|size| *size != 0)
    }));
    let decimal_digits = optional_int16_column(&mut columns.iter().map(|column| {
        // A parameter-free NUMERIC has no known scale, so null. With a precision, BigQuery omits
        // a zero scale from its JSON (NUMERIC(P) is NUMERIC(P, 0)), so 0 is known.
        match BigQueryType::from_rest(&column.field.r#type) {
            Some(BigQueryType::Numeric | BigQueryType::BigNumeric)
                if column.field.precision != 0 =>
            {
                i16::try_from(column.field.scale).ok()
            }
            _ => None,
        }
    }));
    // A `BYTES(L)` length counts bytes, so it is the octet length too. `STRING(L)` counts
    // characters, which bounds its UTF-8 size without stating it.
    let char_octet_lengths = optional_int32_column(&mut columns.iter().map(|column| {
        i32::try_from(column.field.max_length)
            .ok()
            .filter(|length| {
                *length != 0
                    && BigQueryType::from_rest(&column.field.r#type) == Some(BigQueryType::Bytes)
            })
    }));
    let nullable = int16_column(&mut columns.iter().map(|column| i16::from(column.is_nullable())));
    let defaults = optional_string_column(
        &mut columns
            .iter()
            .map(|column| column.field.default_value_expression.as_deref()),
    );
    let is_nullable = string_column(
        &mut columns
            .iter()
            .map(|column| if column.is_nullable() { "YES" } else { "NO" }),
    );
    // Known false rather than null: GoogleSQL has no identity or autoincrement columns.
    let is_autoincrement = boolean_column(&mut std::iter::repeat_n(false, columns.len()));
    let is_generated = boolean_column(
        &mut columns
            .iter()
            .map(|column| column.field.generated_column.is_some()),
    );

    let count = columns.len();
    let null_i16 = || Arc::new(Int16Array::new_null(count)) as ArrayRef;
    let null_string = || Arc::new(StringArray::new_null(count)) as ArrayRef;
    Ok(StructArray::try_new(
        struct_fields(&schemas::COLUMN_SCHEMA),
        vec![
            Arc::new(names),
            Arc::new(ordinal_positions),
            Arc::new(remarks),
            null_i16(),
            Arc::new(xdbc_type_names),
            Arc::new(column_sizes),
            Arc::new(decimal_digits),
            null_i16(),
            Arc::new(nullable),
            Arc::new(defaults),
            null_i16(),
            null_i16(),
            Arc::new(char_octet_lengths),
            Arc::new(is_nullable),
            null_string(),
            null_string(),
            null_string(),
            Arc::new(is_autoincrement),
            Arc::new(is_generated),
        ],
        None,
    )?)
}

fn build_constraints(constraints: &[&ObjectConstraint]) -> Result<StructArray> {
    let names = optional_string_column(
        &mut constraints
            .iter()
            .map(|constraint| constraint.name.as_deref()),
    );
    let types = string_column(
        &mut constraints
            .iter()
            .map(|constraint| constraint.constraint_type),
    );

    let column_names = nested_list_array(
        constraints,
        |constraint| Some(constraint.column_names.as_slice()),
        &DataType::Utf8,
        |names| Ok(Arc::new(string_column(&mut names.into_iter().map(String::as_str))) as ArrayRef),
    )?;
    let column_usage = nested_list_array(
        constraints,
        |constraint| constraint.column_usage.as_deref(),
        &schemas::USAGE_SCHEMA,
        |usages| Ok(Arc::new(build_column_usage(&usages)?) as ArrayRef),
    )?;

    Ok(StructArray::try_new(
        struct_fields(&schemas::CONSTRAINT_SCHEMA),
        vec![
            Arc::new(names),
            Arc::new(types),
            Arc::new(column_names),
            Arc::new(column_usage),
        ],
        None,
    )?)
}

fn build_column_usage(usages: &[&ObjectColumnUsage]) -> Result<StructArray> {
    Ok(StructArray::try_new(
        struct_fields(&schemas::USAGE_SCHEMA),
        vec![
            Arc::new(optional_string_column(
                &mut usages.iter().map(|usage| usage.catalog.as_deref()),
            )),
            Arc::new(optional_string_column(
                &mut usages.iter().map(|usage| usage.db_schema.as_deref()),
            )),
            Arc::new(string_column(
                &mut usages.iter().map(|usage| usage.table.as_str()),
            )),
            Arc::new(string_column(
                &mut usages.iter().map(|usage| usage.column.as_str()),
            )),
        ],
        None,
    )?)
}

#[cfg(test)]
mod tests;
