use adbc_core::schemas;
use arrow_array::{Array, Int16Array, Int32Array, RecordBatch, StructArray};

use crate::rest::model::TableFieldSchema;
use crate::test_support::arrow::{child_structs, list, string_at, typed};
use crate::test_support::schema::{decimal, field, field_mode};

use super::{
    ObjectCatalog, ObjectColumn, ObjectColumnUsage, ObjectConstraint, ObjectDbSchema, ObjectTable,
    objects_reader,
};

fn project_catalog(db_schemas: Option<Vec<ObjectDbSchema>>) -> ObjectCatalog {
    ObjectCatalog {
        name: "project".to_owned(),
        db_schemas,
    }
}

fn dataset_schema(tables: Option<Vec<ObjectTable>>) -> ObjectDbSchema {
    ObjectDbSchema {
        name: "dataset".to_owned(),
        tables,
    }
}

/// The one `get_objects` batch for a `records` table with `columns` and `constraints`.
fn records_batch(
    columns: Option<Vec<ObjectColumn>>,
    constraints: Option<Vec<ObjectConstraint>>,
) -> RecordBatch {
    let table = ObjectTable {
        name: "records".to_owned(),
        table_type: "TABLE",
        columns,
        constraints,
    };
    let catalogs = [project_catalog(Some(vec![dataset_schema(Some(vec![
        table,
    ]))]))];
    let mut reader = objects_reader(&catalogs).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}

/// The table struct array of a [`records_batch`].
fn tables(batch: &RecordBatch) -> StructArray {
    child_structs(child_structs(batch.column(1)).column(1)).clone()
}

#[test]
fn builds_complete_get_objects_hierarchy() {
    let columns = vec![
        ObjectColumn {
            field: TableFieldSchema {
                description: Some("row identifier".to_owned()),
                ..field_mode("required_id", "INTEGER", "REQUIRED")
            },
            ordinal_position: 1,
            type_name: "INT64".to_owned(),
        },
        ObjectColumn {
            field: TableFieldSchema {
                max_length: 64,
                ..field_mode("label", "STRING", "NULLABLE")
            },
            ordinal_position: 4,
            type_name: "STRING".to_owned(),
        },
    ];
    let constraints = vec![ObjectConstraint {
        name: Some("label_fk".to_owned()),
        constraint_type: "FOREIGN KEY",
        column_names: vec!["label".to_owned()],
        column_usage: Some(vec![ObjectColumnUsage {
            catalog: Some("project".to_owned()),
            db_schema: Some("dataset".to_owned()),
            table: "labels".to_owned(),
            column: "label".to_owned(),
        }]),
    }];
    let batch = records_batch(Some(columns), Some(constraints));
    assert_eq!(batch.schema(), schemas::GET_OBJECTS_SCHEMA.clone());
    assert_eq!(string_at(batch.column(0), 0), "project");

    let tables = tables(&batch);
    let columns = child_structs(tables.column(2));
    assert_eq!(string_at(columns.column(0), 1), "label");
    assert_eq!(string_at(columns.column(4), 1), "STRING");
    assert_eq!(typed::<Int32Array>(columns.column(1)).value(1), 4);

    let constraints = child_structs(tables.column(3));
    let usage = child_structs(constraints.column(3));
    assert_eq!(string_at(usage.column(2), 0), "labels");
}

#[test]
fn column_nullability_distinguishes_repeated_from_nullable() {
    let columns = ["", "NULLABLE", "REQUIRED", "REPEATED"]
        .into_iter()
        .map(|mode| ObjectColumn {
            field: field_mode("value", "INTEGER", mode),
            ordinal_position: 1,
            type_name: "INT64".to_owned(),
        })
        .collect();
    let batch = records_batch(Some(columns), None);
    let columns = child_structs(tables(&batch).column(2)).clone();
    let nullable = typed::<Int16Array>(columns.column(8));
    for (row, expected) in [true, true, false, false].into_iter().enumerate() {
        assert_eq!(nullable.value(row), i16::from(expected));
        assert_eq!(
            string_at(columns.column(13), row),
            if expected { "YES" } else { "NO" }
        );
    }
}

/// An empty `fk_catalog` or `fk_db_schema` would make a consumer resolve `""."dataset"."table"`
/// instead of falling back to the current catalog or schema.
#[test]
fn an_unnamed_foreign_key_component_is_null_rather_than_empty() {
    let usage = |catalog: Option<&str>, db_schema: Option<&str>| ObjectColumnUsage {
        catalog: catalog.map(str::to_owned),
        db_schema: db_schema.map(str::to_owned),
        table: "labels".to_owned(),
        column: "label".to_owned(),
    };
    let batch = records_batch(
        None,
        Some(vec![ObjectConstraint {
            name: None,
            constraint_type: "FOREIGN KEY",
            column_names: vec!["label".to_owned(), "label".to_owned()],
            column_usage: Some(vec![
                usage(Some("project"), Some("dataset")),
                usage(None, None),
            ]),
        }]),
    );
    let constraints = child_structs(tables(&batch).column(3)).clone();
    let usages = child_structs(constraints.column(3));
    assert_eq!(string_at(usages.column(0), 0), "project");
    assert_eq!(string_at(usages.column(1), 0), "dataset");
    assert!(usages.column(0).is_null(1), "an unnamed project is null");
    assert!(usages.column(1).is_null(1), "an unnamed dataset is null");
    for row in 0..2 {
        assert_eq!(string_at(usages.column(2), row), "labels");
        assert_eq!(string_at(usages.column(3), row), "label");
    }
}

/// A bare `NUMERIC` reports no precision or scale (its real scale is 9), so both are null.
/// BigQuery reads `NUMERIC(10)` as `NUMERIC(10, 0)` and omits the zero scale from its JSON, so
/// there the scale is a known 0.
#[test]
fn numeric_parameters_are_reported_only_where_bigquery_has_them() {
    let column = |name: &str, r#type: &str, precision: i64, scale: i64| ObjectColumn {
        field: decimal(field(name, r#type), precision, scale),
        ordinal_position: 1,
        type_name: r#type.to_owned(),
    };
    let batch = records_batch(
        Some(vec![
            column("bare", "NUMERIC", 0, 0),
            column("parameterized", "NUMERIC", 38, 9),
            column("integral", "BIGNUMERIC", 10, 0),
            ObjectColumn {
                field: TableFieldSchema {
                    max_length: 64,
                    ..field("bounded_string", "STRING")
                },
                ordinal_position: 4,
                type_name: "STRING".to_owned(),
            },
        ]),
        None,
    );
    let columns = child_structs(tables(&batch).column(2)).clone();
    let column_sizes = typed::<Int32Array>(columns.column(5));
    let decimal_digits = typed::<Int16Array>(columns.column(6));
    let reported = |index: usize| {
        (
            (!column_sizes.is_null(index)).then(|| column_sizes.value(index)),
            (!decimal_digits.is_null(index)).then(|| decimal_digits.value(index)),
        )
    };
    assert_eq!(reported(0), (None, None), "a bare NUMERIC has neither");
    assert_eq!(reported(1), (Some(38), Some(9)));
    assert_eq!(
        reported(2),
        (Some(10), Some(0)),
        "BIGNUMERIC(10) is scale 0"
    );
    assert_eq!(
        reported(3),
        (Some(64), None),
        "a string length is a column size, never a decimal scale"
    );
}

/// A `BYTES(L)` length is a byte count, so it is the column's octet length as well as its size;
/// a `STRING(L)` length counts characters, and an undeclared length is unknown.
#[test]
fn only_a_declared_bytes_length_is_an_octet_length() {
    let column = |name: &str, r#type: &str, max_length: i64| ObjectColumn {
        field: TableFieldSchema {
            max_length,
            ..field(name, r#type)
        },
        ordinal_position: 1,
        type_name: r#type.to_owned(),
    };
    let batch = records_batch(
        Some(vec![
            column("bounded_bytes", "BYTES", 16),
            column("bytes", "BYTES", 0),
            column("bounded_string", "STRING", 16),
        ]),
        None,
    );
    let columns = child_structs(tables(&batch).column(2)).clone();
    let octet_lengths = typed::<Int32Array>(columns.column(12));
    assert_eq!(
        octet_lengths.iter().collect::<Vec<_>>(),
        [Some(16), None, None]
    );
}

#[test]
fn requested_depth_is_represented_by_null_child_lists() {
    let mut catalogs = objects_reader(&[project_catalog(None)]).unwrap();
    let catalog_batch = catalogs.next().unwrap().unwrap();
    assert!(list(catalog_batch.column(1)).is_null(0));

    let mut schemas = objects_reader(&[project_catalog(Some(vec![dataset_schema(None)]))]).unwrap();
    let schema_batch = schemas.next().unwrap().unwrap();
    let schemas = child_structs(schema_batch.column(1));
    assert!(list(schemas.column(1)).is_null(0));

    let mut tables = objects_reader(&[project_catalog(Some(vec![dataset_schema(Some(vec![
        ObjectTable {
            name: "table".to_owned(),
            table_type: "TABLE",
            columns: None,
            constraints: None,
        },
    ]))]))])
    .unwrap();
    let table_batch = tables.next().unwrap().unwrap();
    let schemas = child_structs(table_batch.column(1));
    let tables = child_structs(schemas.column(1));
    assert!(list(tables.column(2)).is_null(0));
    assert!(list(tables.column(3)).is_null(0));
}
