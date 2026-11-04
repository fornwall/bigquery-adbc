//! Comparing an ingest schema against the table BigQuery already has, and what a mismatch
//! reports.

use super::*;

fn one_field_schema(column: TableFieldSchema) -> TableSchema {
    table_schema([column])
}

fn fields_match(actual: &TableFieldSchema, expected: &TableFieldSchema) -> bool {
    schemas_match(
        &one_field_schema(actual.clone()),
        &one_field_schema(expected.clone()),
    )
}

#[test]
fn compares_bigquery_schema_semantically() {
    let mut actual = field("amount", "DECIMAL");
    let expected = decimal(field_mode("amount", "NUMERIC", "NULLABLE"), 38, 9);
    assert!(fields_match(&actual, &expected));
    actual.mode = "REQUIRED".to_owned();
    assert!(!fields_match(&actual, &expected));

    let actual = range("period", "DATE");
    let expected = range("period", "TIMESTAMP");
    assert!(!fields_match(&actual, &expected));
}

/// adbc.h mandates `ALREADY_EXISTS` for an append target whose schema does not match the bound
/// data; the message adds which columns disagree and how to proceed.
#[test]
fn appending_ignores_a_required_or_nullable_difference_in_either_direction() {
    let target = TableId {
        project_id: "project",
        dataset_id: "dataset",
        table_id: "orders",
    };
    // DuckDB, pyarrow and pandas bind every column as nullable, so the mode is not compared;
    // otherwise they could not append to a table with a REQUIRED column.
    let required = table_schema([
        field_mode("order_id", "INT64", "REQUIRED"),
        record(
            "customer",
            "STRUCT",
            [field_mode("id", "INT64", "REQUIRED")],
        ),
    ]);
    let nullable = table_schema([
        field_mode("order_id", "INT64", "NULLABLE"),
        record(
            "customer",
            "STRUCT",
            [field_mode("id", "INT64", "NULLABLE")],
        ),
    ]);
    validate_existing_schema(target, Some(&required), &nullable)
        .expect("a REQUIRED table accepts nullable data");
    // The safe direction, non-null data into a nullable column, is accepted too.
    validate_existing_schema(target, Some(&nullable), &required)
        .expect("a NULLABLE table accepts non-nullable data");
    // A column BigQuery reports with no mode at all means NULLABLE, and still matches.
    validate_existing_schema(
        target,
        Some(&table_schema([field_mode("order_id", "INT64", "REQUIRED")])),
        &table_schema([field("order_id", "INT64")]),
    )
    .expect("a column reported without a mode is NULLABLE");
}

#[test]
fn appending_still_refuses_an_array_where_the_table_holds_a_scalar() {
    let target = TableId {
        project_id: "project",
        dataset_id: "dataset",
        table_id: "orders",
    };
    let scalar = table_schema([field_mode("tags", "STRING", "REQUIRED")]);
    let repeated = table_schema([field_mode("tags", "STRING", "REPEATED")]);

    let failure = validate_existing_schema(target, Some(&scalar), &repeated).unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists);
    // The message spells out the real modes, not the ones the comparison collapsed.
    assert!(
        failure.message.contains(
            "column \"tags\" is STRING REQUIRED in the table but STRING REPEATED in the bound data"
        ),
        "unexpected message: {}",
        failure.message
    );
    validate_existing_schema(target, Some(&repeated), &scalar).unwrap_err();
}

#[test]
fn replacing_still_recreates_a_table_whose_mode_differs() {
    // REPLACE compares exactly: it is choosing between truncating the table it found and
    // creating the one the bound data describes, and those differ here.
    let required = table_schema([field_mode("order_id", "INT64", "REQUIRED")]);
    let nullable = table_schema([field_mode("order_id", "INT64", "NULLABLE")]);
    assert!(!schemas_match(&required, &nullable));
    assert!(!schemas_match(&nullable, &required));
    assert!(schemas_match(&required, &required));
}

const ORDERS: TableId<'static> = TableId {
    project_id: "project",
    dataset_id: "dataset",
    table_id: "orders",
};

/// The differences `validate_existing_schema` reports, one per column.
fn append_differences(actual: &TableSchema, expected: &TableSchema) -> Vec<String> {
    let message = validate_existing_schema(ORDERS, Some(actual), expected)
        .unwrap_err()
        .message;
    message
        .strip_prefix("BigQuery ingest target project.dataset.orders has an incompatible schema: ")
        .and_then(|rest| rest.split_once("; change the bound schema"))
        .map_or_else(
            || panic!("unexpected message: {message}"),
            |(differences, _)| differences.split("; ").map(str::to_owned).collect(),
        )
}

#[test]
fn incompatible_existing_schema_names_every_offending_column() {
    let actual = table_schema([
        field("order_id", "INT64"),
        field_mode("shipped_at", "TIMESTAMP", "REQUIRED"),
        record(
            "customer",
            "RECORD",
            [field("id", "INT64"), field("name", "STRING")],
        ),
    ]);
    let expected = table_schema([
        field("order_id", "STRING"),
        record("customer", "STRUCT", [field("id", "INT64")]),
        field("note", "STRING"),
    ]);

    let failure = validate_existing_schema(ORDERS, Some(&actual), &expected).unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists);
    assert_eq!(
        failure.message,
        "BigQuery ingest target project.dataset.orders has an incompatible schema: column \
         \"order_id\" is INT64 NULLABLE in the table but STRING NULLABLE in the bound data; \
         bound column \"note\" is not in the table; REQUIRED column \"shipped_at\" is missing \
         from the bound data; change the bound schema to match, or set adbc.ingest.mode to \
         \"adbc.ingest.mode.replace\" to recreate the table"
    );

    // A nested difference is reported by its dotted path, and an extra bound column by name.
    let actual = table_schema([record(
        "customer",
        "STRUCT",
        [
            field("id", "INT64"),
            field_mode("tier", "STRING", "REQUIRED"),
        ],
    )]);
    let expected = table_schema([record(
        "customer",
        "STRUCT",
        [field("id", "STRING"), field("name", "STRING")],
    )]);
    assert_eq!(
        append_differences(&actual, &expected),
        [
            "column \"customer.id\" is INT64 NULLABLE in the table but STRING NULLABLE in the \
             bound data",
            "bound column \"customer.name\" is not in the table",
            "REQUIRED column \"customer.tier\" is missing from the bound data",
        ]
    );
}

/// Appends resolve columns as Storage Write does (see `collect_append_differences`): by name,
/// ignoring case and order, at every struct level; omitted NULLABLE and REPEATED columns are
/// allowed.
#[test]
fn appending_resolves_columns_by_name_like_storage_write() {
    let actual = table_schema([
        field_mode("id", "INT64", "REQUIRED"),
        record(
            "customer",
            "RECORD",
            [field("id", "INT64"), field("name", "STRING")],
        ),
        field_mode("tags", "STRING", "REPEATED"),
        field("note", "STRING"),
    ]);
    let expected = table_schema([
        record("Customer", "STRUCT", [field("NAME", "STRING")]),
        field("ID", "INT64"),
    ]);
    validate_existing_schema(ORDERS, Some(&actual), &expected).unwrap();

    // `replace` still recreates rather than truncates a table that is not the one the bound data
    // describes: it would keep columns, an order and a spelling the data does not have.
    assert!(!schemas_match(&actual, &expected));
    let reordered = table_schema([field("b", "STRING"), field("a", "INT64")]);
    assert!(!schemas_match(
        &table_schema([field("a", "INT64"), field("b", "STRING")]),
        &reordered
    ));
    assert!(!schemas_match(
        &table_schema([field("A", "INT64")]),
        &table_schema([field("a", "INT64")])
    ));
}

/// A target with no schema is not a table at all, since BigQuery gives every real table one. The
/// `replace` remedy would run `CREATE OR REPLACE TABLE` over a view, so the failure says what the
/// target is not and names the option that chose it.
#[test]
fn a_schemaless_target_is_reported_as_the_wrong_kind_of_object() {
    let failure = validate_existing_schema(
        TableId {
            project_id: "project",
            dataset_id: "dataset",
            table_id: "orders",
        },
        None,
        &TableSchema::default(),
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "BigQuery ingest target project.dataset.orders has no schema, so it is not a writable \
         table; set adbc.ingest.target_table to a table, or drop it so the ingest can create one"
    );
}

/// Storage Write fails every append to a table with a flexible column name, at any depth and
/// whether or not the append binds it, and only after its retries, as `INTERNAL`. The append is
/// refused up front instead, naming the column.
#[test]
fn a_target_with_a_column_storage_write_cannot_write_is_refused() {
    let bound = table_schema([field("id", "INTEGER")]);
    for (table, path, reason) in [
        (
            table_schema([field("id", "INTEGER"), field("first name", "STRING")]),
            "first name",
            "contains \" \", but Storage Write accepts only ASCII letters, digits, and \
             underscores in a column name",
        ),
        (
            table_schema([
                field("id", "INTEGER"),
                record("customer", "RECORD", [field("2nd", "STRING")]),
            ]),
            "customer.2nd",
            "starts with \"2\", but Storage Write needs a column name that starts with an ASCII \
             letter or underscore",
        ),
    ] {
        let failure = validate_existing_schema(ORDERS, Some(&table), &bound).unwrap_err();
        assert_eq!(failure.status, Status::NotImplemented);
        assert_eq!(
            failure.message,
            format!(
                "BigQuery ingest target project.dataset.orders has column {path:?}, whose name \
                 {reason}; Storage Write fails every append to such a table, so rename the \
                 column, or set adbc.ingest.mode to \"adbc.ingest.mode.replace\" to recreate \
                 the table"
            )
        );
    }
}

/// A view carries a schema like a table, so without the `type` check it would pass the schema
/// comparison and fail inside Storage Write, while `replace` would run `CREATE OR REPLACE TABLE`
/// over it. Only `TABLE` is admitted.
#[test]
fn a_target_that_is_not_a_table_is_refused_by_its_type() {
    let target = TableId {
        project_id: "project",
        dataset_id: "dataset",
        table_id: "orders",
    };
    validate_ingestible_target(target, "TABLE").unwrap();
    // BigQuery declining to say is left to the schema check, which has its own account of it.
    validate_ingestible_target(target, "").unwrap();

    for kind in ["VIEW", "MATERIALIZED_VIEW", "EXTERNAL", "SNAPSHOT", "MODEL"] {
        let failure = validate_ingestible_target(target, kind).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments, "{kind}");
        assert_eq!(
            failure.message,
            format!(
                "BigQuery ingest target project.dataset.orders is a {kind}, not a table; set \
                 adbc.ingest.target_table to a table, or drop it so the ingest can create one"
            )
        );
    }
}

/// An append is admitted only when the bound column's declared domain fits the table's (see
/// `FieldShape::accepts`).
#[test]
fn appending_admits_parameterized_data_whose_domain_fits_the_column() {
    let admits = |table: TableFieldSchema, data: TableFieldSchema| {
        validate_existing_schema(ORDERS, Some(&table_schema([table])), &table_schema([data]))
            .is_ok()
    };
    let numeric = |precision, scale| decimal(field("amount", "NUMERIC"), precision, scale);
    let string = |max_length| TableFieldSchema {
        max_length,
        ..field("code", "STRING")
    };

    // A plain NUMERIC is (38, 9): a (10, 2) fits, a (38, 10) would lose its tenth digit.
    assert!(admits(field("amount", "NUMERIC"), numeric(10, 2)));
    assert!(admits(field("amount", "NUMERIC"), numeric(38, 9)));
    assert!(!admits(field("amount", "NUMERIC"), numeric(38, 10)));
    // Fewer fractional or integer digits fit a parameterized column; more of either do not.
    assert!(admits(numeric(10, 2), numeric(5, 1)));
    assert!(admits(numeric(10, 2), numeric(10, 2)));
    assert!(!admits(numeric(10, 2), numeric(10, 3)));
    assert!(!admits(numeric(10, 2), numeric(11, 2)));
    assert!(!admits(numeric(10, 2), field("amount", "NUMERIC")));
    // NUMERIC and BIGNUMERIC stay distinct: the service refuses a Decimal128 for BIGNUMERIC.
    assert!(!admits(field("amount", "BIGNUMERIC"), numeric(10, 2)));

    // An unbounded STRING takes any string; a bounded one only a declared length that fits.
    assert!(admits(string(0), string(0)));
    assert!(admits(string(0), string(8)));
    assert!(admits(string(8), string(3)));
    assert!(!admits(string(8), string(9)));
    assert!(!admits(string(8), string(0)));
}

/// Storage Write parses a plain string bound for a JSON or GEOGRAPHY column as JSON text or WKT,
/// exactly as it parses one annotated `arrow.json` or `geoarrow.wkt`, and stores a `geoarrow.wkt`
/// string in a STRING column as it is; it refuses an `arrow.json` field for a STRING column.
#[test]
fn appending_admits_strings_that_storage_write_parses_or_keeps_unchanged() {
    let admits = |table: TableFieldSchema, data: TableFieldSchema| {
        validate_existing_schema(ORDERS, Some(&table_schema([table])), &table_schema([data]))
            .is_ok()
    };
    for kind in ["JSON", "GEOGRAPHY"] {
        assert!(
            admits(field("value", kind), field("value", "STRING")),
            "{kind}"
        );
        assert!(
            admits(
                field_mode("value", kind, "REPEATED"),
                field_mode("value", "STRING", "REPEATED")
            ),
            "{kind}"
        );
        assert!(
            admits(
                record("outer", "RECORD", [field("value", kind)]),
                record("outer", "STRUCT", [field("value", "STRING")])
            ),
            "{kind}"
        );
        // `replace` still recreates the table: the bound data describes a STRING column.
        assert!(!schemas_match(
            &table_schema([field("value", kind)]),
            &table_schema([field("value", "STRING")])
        ));
    }
    assert!(admits(
        field("value", "STRING"),
        field("value", "GEOGRAPHY")
    ));
    assert!(!admits(field("value", "STRING"), field("value", "JSON")));
    assert!(!admits(field("value", "JSON"), field("value", "GEOGRAPHY")));
    assert!(!admits(field("value", "GEOGRAPHY"), field("value", "JSON")));
    // A bounded STRING column still takes only data whose declared length fits.
    let bounded = TableFieldSchema {
        max_length: 8,
        ..field("value", "STRING")
    };
    assert!(!admits(bounded, field("value", "GEOGRAPHY")));
}

#[test]
fn plain_bignumeric_is_wider_than_every_parameterized_bignumeric() {
    let plain = table_schema([field("amount", "BIGNUMERIC")]);
    let bounded = table_schema([decimal(field("amount", "BIGNUMERIC"), 76, 38)]);
    assert!(!schemas_match(&plain, &bounded));
    assert!(!schemas_match(&bounded, &plain));
    assert!(schemas_match(&plain, &plain));
    assert!(schemas_match(&bounded, &bounded));
    validate_existing_schema(ORDERS, Some(&plain), &bounded).unwrap();
    validate_existing_schema(ORDERS, Some(&plain), &plain).unwrap();
    assert_eq!(
        append_differences(&bounded, &plain),
        [
            "column \"amount\" is BIGNUMERIC(76, 38) NULLABLE in the table but BIGNUMERIC NULLABLE in the bound data"
        ]
    );

    // The same distinction applies recursively and to the BIGDECIMAL REST alias.
    let nested = |column| table_schema([record("outer", "STRUCT", [column])]);
    let plain = nested(field_mode("amount", "BIGDECIMAL", "REPEATED"));
    let bounded = nested(decimal(
        field_mode("amount", "BIGNUMERIC", "REPEATED"),
        76,
        38,
    ));
    validate_existing_schema(ORDERS, Some(&plain), &bounded).unwrap();
    assert_eq!(
        append_differences(&bounded, &plain),
        [
            "column \"outer.amount\" is BIGNUMERIC(76, 38) REPEATED in the table but BIGNUMERIC REPEATED in the bound data"
        ]
    );
}
