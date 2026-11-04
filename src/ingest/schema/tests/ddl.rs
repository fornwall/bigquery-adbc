//! The `CREATE OR REPLACE TABLE` DDL an atomic replace renders.

use super::*;

/// The atomic replace renders every schema property the ingest's schema conversion can produce,
/// in the GoogleSQL spellings BigQuery reports back. A dropped property would make every later
/// run replace the table again; a wrong one is DDL BigQuery refuses.
#[test]
fn an_atomic_replace_renders_every_property_the_ingest_schema_carries() {
    let schema = table_schema([
        field_mode("id", "INTEGER", "REQUIRED"),
        field_mode("name", "STRING", "NULLABLE"),
        TableFieldSchema {
            max_length: 64,
            ..field_mode("label", "STRING", "NULLABLE")
        },
        decimal(field_mode("price", "NUMERIC", "NULLABLE"), 10, 2),
        decimal(field_mode("huge", "BIGNUMERIC", "REQUIRED"), 75, 38),
        // The conversion leaves the parameters off the widest BIGNUMERIC, which is the only
        // spelling whose column accepts every `i256` a BIGNUMERIC read hands back.
        field_mode("widest", "BIGNUMERIC", "REQUIRED"),
        field_mode("tags", "STRING", "REPEATED"),
        record(
            "customer",
            "RECORD",
            [
                field_mode("first", "STRING", "REQUIRED"),
                field_mode("age", "INTEGER", "NULLABLE"),
            ],
        ),
        TableFieldSchema {
            mode: "REPEATED".to_owned(),
            ..record(
                "visits",
                "RECORD",
                [field_mode("at", "TIMESTAMP", "REQUIRED")],
            )
        },
        TableFieldSchema {
            mode: "REPEATED".to_owned(),
            ..range("windows", "DATETIME")
        },
        // A GoogleSQL reserved word is only an identifier inside backticks.
        field_mode("select", "BOOLEAN", "NULLABLE"),
    ]);

    assert_eq!(
        create_or_replace_table_ddl("`p.d.t`", &schema).unwrap(),
        "CREATE OR REPLACE TABLE `p.d.t` (\
         `id` INT64 NOT NULL, \
         `name` STRING, \
         `label` STRING(64), \
         `price` NUMERIC(10, 2), \
         `huge` BIGNUMERIC(75, 38) NOT NULL, \
         `widest` BIGNUMERIC NOT NULL, \
         `tags` ARRAY<STRING>, \
         `customer` STRUCT<`first` STRING NOT NULL, `age` INT64>, \
         `visits` ARRAY<STRUCT<`at` TIMESTAMP NOT NULL>>, \
         `windows` ARRAY<RANGE<DATETIME>>, \
         `select` BOOL)"
    );
}

/// Every column the DDL declares round-trips: the schema BigQuery would report for the table it
/// creates is the schema the ingest asked for, so the next run reuses the table rather than
/// replacing it again.
#[test]
fn the_atomic_replace_declares_the_schema_the_ingest_compares_against() {
    let schema = arrow_schema_to_bigquery(
        &normalize_schema(&Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "customer",
                DataType::Struct(Fields::from(vec![Field::new("age", DataType::Int64, true)])),
                true,
            ),
        ]))
        .unwrap(),
    )
    .unwrap();
    let ddl = create_or_replace_table_ddl("`p.d.t`", &schema).unwrap();
    assert_eq!(
        ddl,
        "CREATE OR REPLACE TABLE `p.d.t` (`id` INT64 NOT NULL, `name` STRING, \
         `customer` STRUCT<`age` INT64>)"
    );
}

/// A table with no columns is refused here rather than as a BigQuery syntax error on `()`.
#[test]
fn an_atomic_replace_refuses_a_schema_with_no_columns() {
    let error = create_or_replace_table_ddl("`p.d.t`", &table_schema([])).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "the bound data has no fields; a BigQuery table or STRUCT needs at least one"
    );

    let error =
        create_or_replace_table_ddl("`p.d.t`", &table_schema([record("customer", "RECORD", [])]))
            .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "column \"customer\" has no fields; a BigQuery table or STRUCT needs at least one"
    );
}
