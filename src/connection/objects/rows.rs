//! The `get_objects` table rows: one listed BigQuery table, its columns, and its key
//! constraints, translated into the ADBC objects hierarchy.

use adbc_core::error::{Result, Status};

use super::{ObjectColumn, ObjectColumnUsage, ObjectConstraint, ObjectTable};
use crate::connection::sql_pattern::SqlPattern;
use crate::error::error;
use crate::rest::metadata::Table;
use crate::rest::model::{TableConstraints, TableSchema};
use crate::types::schema::googlesql_type_literal;

/// Builds a table row; skipped metadata yields null columns and constraints.
/// `table_type` must already be normalized by [`bigquery_table_type`] so it agrees with
/// `GetTableTypes` and the scan's type filter. See G73.
///
/// [`bigquery_table_type`]: crate::connection::table_metadata::bigquery_table_type
pub(in crate::connection) fn object_table(
    name: String,
    table_type: &'static str,
    metadata: Option<Table>,
    include_columns: bool,
    column_name: Option<SqlPattern<'_>>,
) -> Result<ObjectTable> {
    let Some(metadata) = metadata else {
        return Ok(ObjectTable {
            name,
            table_type,
            columns: None,
            constraints: None,
        });
    };
    Ok(ObjectTable {
        name,
        table_type,
        columns: include_columns.then(|| object_columns(metadata.schema, column_name)),
        constraints: Some(object_constraints(
            metadata.table_constraints.unwrap_or_default(),
        )?),
    })
}

/// The columns `column_name` keeps of a described table, numbered by their place in the whole
/// table.
fn object_columns(
    schema: Option<TableSchema>,
    column_name: Option<SqlPattern<'_>>,
) -> Vec<ObjectColumn> {
    // adbc.h gives get_objects no per-object failure, so an object described without a schema
    // (such as a view whose SQL no longer compiles) is listed with no columns. get_table_schema
    // reports the missing schema as an error.
    let fields = schema.unwrap_or_default().fields;
    (1..)
        .zip(fields)
        .filter(|(_, field)| column_name.is_none_or(|pattern| pattern.matches(&field.name)))
        .map(|(ordinal_position, field)| {
            // `get_table_schema`'s GoogleSQL spelling, or the REST one for a type the driver does
            // not implement: `get_objects` has no per-object failure to report it with.
            let type_name = googlesql_type_literal(&field).unwrap_or_else(|| field.r#type.clone());
            ObjectColumn {
                field,
                ordinal_position,
                type_name,
            }
        })
        .collect()
}

/// A reference component BigQuery omitted, as the null the ADBC schema reserves for it.
///
/// An omitted `TableReference` field decodes to the empty string, which a consumer would resolve
/// as a name instead of falling back to the current catalog or schema.
fn absent_as_null(component: &str) -> Option<String> {
    (!component.is_empty()).then(|| component.to_owned())
}

fn object_constraints(constraints: TableConstraints) -> Result<Vec<ObjectConstraint>> {
    let mut objects = Vec::with_capacity(
        usize::from(constraints.primary_key.is_some()) + constraints.foreign_keys.len(),
    );
    if let Some(primary_key) = constraints.primary_key {
        objects.push(ObjectConstraint {
            name: None,
            constraint_type: "PRIMARY KEY",
            column_names: primary_key.columns,
            column_usage: None,
        });
    }
    for foreign_key in constraints.foreign_keys {
        let referenced_table = foreign_key.referenced_table.ok_or_else(|| {
            error(
                format!(
                    "BigQuery returned foreign key {:?} without a referenced table",
                    foreign_key.name
                ),
                Status::InvalidData,
            )
        })?;
        let mut column_names = Vec::with_capacity(foreign_key.column_references.len());
        let mut column_usage = Vec::with_capacity(foreign_key.column_references.len());
        for reference in foreign_key.column_references {
            column_names.push(reference.referencing_column);
            column_usage.push(ObjectColumnUsage {
                catalog: absent_as_null(&referenced_table.project_id),
                db_schema: absent_as_null(&referenced_table.dataset_id),
                table: referenced_table.table_id.clone(),
                column: reference.referenced_column,
            });
        }
        objects.push(ObjectConstraint {
            name: (!foreign_key.name.is_empty()).then_some(foreign_key.name),
            constraint_type: "FOREIGN KEY",
            column_names,
            column_usage: Some(column_usage),
        });
    }
    Ok(objects)
}
