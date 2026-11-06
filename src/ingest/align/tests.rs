use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::{Array, ArrayRef, ListArray, NullArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use super::*;
use crate::ingest::normalize::normalize_batch;
use crate::test_support::schema::{field, field_mode, record, table_schema};
use crate::types::BIGQUERY_TYPE_METADATA_KEY;

fn max_length(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get(BIGQUERY_FIELD_METADATA_MAX_LENGTH)
        .map(String::as_str)
}

fn with_max_length(column: TableFieldSchema, length: i64) -> TableFieldSchema {
    TableFieldSchema {
        max_length: length,
        ..column
    }
}

fn item_type(field: &Field) -> &DataType {
    match field.data_type() {
        DataType::List(item) => item.data_type(),
        other => panic!("{} is not a list: {other}", field.name()),
    }
}

/// pandas and Polars type a column that holds only nulls `Null`. An append takes the type from
/// the column it fills, at any depth, and writes NULLs there, or empty arrays for a list.
#[test]
fn a_null_field_takes_the_type_of_the_column_it_fills() {
    let table = table_schema([
        field("seen", "TIMESTAMP"),
        field_mode("tags", "STRING", "REPEATED"),
        record("detail", "RECORD", [field("note", "STRING")]),
    ]);
    let detail = Fields::from(vec![Field::new("note", DataType::Null, true)]);
    let bound = Schema::new(vec![
        Field::new("SEEN", DataType::Null, true),
        Field::new_list("tags", Field::new("item", DataType::Null, true), true),
        Field::new_struct("detail", detail.clone(), true),
    ]);
    assert!(has_null_field(bound.fields()));

    let aligned = align_with_table(&bound, &table).unwrap();
    assert!(!has_null_field(aligned.fields()));
    assert_eq!(aligned.field(0).name(), "SEEN");
    assert_eq!(
        aligned.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(item_type(aligned.field(1)), &DataType::Utf8);
    let DataType::Struct(children) = aligned.field(2).data_type() else {
        panic!("detail is not a struct");
    };
    assert_eq!(children[0].data_type(), &DataType::Utf8);

    let batch = RecordBatch::try_new(
        Arc::new(bound),
        vec![
            Arc::new(NullArray::new(2)) as ArrayRef,
            Arc::new(ListArray::new_null(
                Arc::new(Field::new("item", DataType::Null, true)),
                2,
            )) as ArrayRef,
            Arc::new(StructArray::new_null(detail, 2)) as ArrayRef,
        ],
    )
    .unwrap();
    let normalized = normalize_batch(batch, Arc::new(aligned)).unwrap();
    assert_eq!(normalized.column(0).null_count(), 2);
    let tags = normalized
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(tags.null_count(), 0);
    assert!((0..2).all(|row| tags.value(row).is_empty()));
    assert_eq!(normalized.column(2).null_count(), 2);
}

/// A field the table lacks is left for the comparison to report as missing, so one holding a
/// `Null` stands in as a string; and a REQUIRED column cannot be filled with NULLs.
#[test]
fn a_null_field_is_refused_where_the_table_cannot_take_nulls() {
    let bound = Schema::new(vec![
        Field::new("extra", DataType::Null, true),
        Field::new_struct(
            "nested",
            vec![Field::new("note", DataType::Null, true)],
            true,
        ),
    ]);
    let aligned = align_with_table(&bound, &table_schema([field("id", "INTEGER")])).unwrap();
    assert!(!has_null_field(aligned.fields()));
    assert_eq!(aligned.field(1).data_type(), &DataType::Utf8);

    let bound = Schema::new(vec![Field::new("extra", DataType::Null, true)]);

    let required = table_schema([field_mode("extra", "INTEGER", "REQUIRED")]);
    let failure = align_with_table(&bound, &required).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "bound column \"extra\" has Arrow type Null, so every value is NULL, but column \
         \"extra\" of the table is REQUIRED"
    );
}

/// A STRING or BYTES field without a declared length takes the column's, which normalization
/// then checks every value against, rather than the append being refused for want of it.
#[test]
fn plain_strings_take_the_columns_maximum_length() {
    let table = table_schema([
        with_max_length(field("name", "STRING"), 5),
        with_max_length(field("code", "BYTES"), 3),
        with_max_length(field_mode("tags", "STRING", "REPEATED"), 4),
        with_max_length(field("declared", "STRING"), 5),
        with_max_length(field("geo", "STRING"), 5),
        field("free", "STRING"),
    ]);
    let declared = Field::new("declared", DataType::Utf8, true).with_metadata(HashMap::from([(
        BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
        "2".to_owned(),
    )]));
    let geography = Field::new("geo", DataType::Utf8, true).with_metadata(HashMap::from([(
        BIGQUERY_TYPE_METADATA_KEY.to_owned(),
        "GEOGRAPHY".to_owned(),
    )]));
    let bound = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("code", DataType::Binary, true),
        Field::new_list("tags", Field::new("item", DataType::Utf8, false), true),
        declared,
        geography,
        Field::new("free", DataType::Utf8, true),
    ]);

    let aligned = align_with_table(&bound, &table).unwrap();
    assert_eq!(max_length(aligned.field(0)), Some("5"));
    assert_eq!(max_length(aligned.field(1)), Some("3"));
    // A list's length is checked against each element, so it goes on the item.
    let DataType::List(item) = aligned.field(2).data_type() else {
        panic!("tags is not a list");
    };
    assert_eq!(max_length(item), Some("4"));
    // A declared length stands, for the comparison to judge; a GEOGRAPHY has no length.
    assert_eq!(max_length(aligned.field(3)), Some("2"));
    assert_eq!(max_length(aligned.field(4)), None);
    assert_eq!(max_length(aligned.field(5)), None);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![bound.field(0).clone()])),
        vec![Arc::new(StringArray::from(vec!["short", "longer"])) as ArrayRef],
    )
    .unwrap();
    let failure =
        normalize_batch(batch, Arc::new(Schema::new(vec![aligned.field(0).clone()]))).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "BigQuery ingest field \"name\" holds a value of 6 characters at row 1, longer than its \
         declared maximum length of 5"
    );
}

#[test]
fn unknown_extensions_do_not_hide_column_length_limits() {
    use crate::ingest::schema::{
        arrow_schema_to_bigquery, normalize_schema, validate_existing_schema,
    };
    use crate::rest::metadata::TableId;
    use crate::types::ARROW_EXTENSION_NAME_KEY;

    let target = TableId {
        project_id: "p",
        dataset_id: "d",
        table_id: "t",
    };
    for (data_type, kind) in [(DataType::Utf8, "STRING"), (DataType::Binary, "BYTES")] {
        for repeated in [false, true] {
            let mut value = Field::new("value", data_type.clone(), true)
                .with_metadata([(ARROW_EXTENSION_NAME_KEY, "example.label")]);
            if repeated {
                value = Field::new_list("value", value, true);
            }
            let table = table_schema([with_max_length(
                field_mode(
                    "value",
                    kind,
                    if repeated { "REPEATED" } else { "NULLABLE" },
                ),
                5,
            )]);
            let bound = normalize_schema(&Schema::new(vec![value])).unwrap();
            let aligned = align_with_table(&bound, &table).unwrap();
            let values = match aligned.field(0).data_type() {
                DataType::List(item) => item,
                _ => aligned.field(0),
            };
            assert_eq!(max_length(values), Some("5"));
            let checked = arrow_schema_to_bigquery(&aligned).unwrap();
            validate_existing_schema(target, Some(&table), &checked).unwrap();
        }
    }
}

fn decimal_column(name: &str, kind: &str, precision: i64, scale: i64) -> TableFieldSchema {
    TableFieldSchema {
        precision,
        scale,
        ..field(name, kind)
    }
}

/// A decimal takes the type and declared shape of the NUMERIC or BIGNUMERIC column it fills,
/// whatever shape its Arrow type has: a value read back as NUMERIC's full `(38, 9)` from a
/// `NUMERIC(10, 2)` column appends to it, and normalization checks that each value fits exactly.
#[test]
fn a_decimal_takes_the_shape_of_the_column_it_fills() {
    let table = table_schema([
        decimal_column("amount", "NUMERIC", 10, 2),
        decimal_column("big", "BIGNUMERIC", 0, 0),
        TableFieldSchema {
            mode: "REPEATED".to_owned(),
            ..decimal_column("amounts", "NUMERIC", 10, 2)
        },
    ]);
    let bound = Schema::new(vec![
        Field::new("amount", DataType::Decimal128(38, 9), true),
        Field::new("big", DataType::Decimal128(38, 9), true),
        Field::new_list(
            "amounts",
            Field::new("item", DataType::Decimal128(38, 9), true),
            true,
        ),
    ]);
    let aligned = align_with_table(&bound, &table).unwrap();
    let shape = |field: &Field| {
        let metadata = |key: &str| field.metadata().get(key).cloned();
        (
            field.data_type().clone(),
            metadata(BIGQUERY_TYPE_METADATA_KEY),
            metadata(BIGQUERY_FIELD_METADATA_PRECISION),
            metadata(BIGQUERY_FIELD_METADATA_SCALE),
        )
    };
    let declared = |data_type, kind: &str, shape: Option<(&str, &str)>| {
        (
            data_type,
            Some(kind.to_owned()),
            shape.map(|(precision, _)| precision.to_owned()),
            shape.map(|(_, scale)| scale.to_owned()),
        )
    };
    assert_eq!(
        shape(aligned.field(0)),
        declared(DataType::Decimal128(10, 2), "NUMERIC", Some(("10", "2")))
    );
    assert_eq!(
        shape(aligned.field(1)),
        declared(DataType::Decimal256(76, 38), "BIGNUMERIC", None)
    );
    let DataType::List(item) = aligned.field(2).data_type() else {
        panic!("amounts is not a list");
    };
    assert_eq!(
        shape(item),
        declared(DataType::Decimal128(10, 2), "NUMERIC", Some(("10", "2")))
    );
}

/// A `Null` field takes its decimal column's declared shape, not just the full type reading the
/// column returns, which the comparison would find a different shape.
#[test]
fn a_null_field_takes_its_decimal_columns_declared_shape() {
    let table = table_schema([
        decimal_column("amount", "NUMERIC", 10, 2),
        TableFieldSchema {
            mode: "REPEATED".to_owned(),
            ..decimal_column("amounts", "NUMERIC", 10, 2)
        },
    ]);
    let bound = Schema::new(vec![
        Field::new("amount", DataType::Null, true),
        Field::new_list("amounts", Field::new("item", DataType::Null, true), true),
    ]);
    let aligned = align_with_table(&bound, &table).unwrap();
    assert_eq!(aligned.field(0).data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(item_type(aligned.field(1)), &DataType::Decimal128(10, 2));
    let checked = crate::ingest::schema::arrow_schema_to_bigquery(&aligned).unwrap();
    assert!(checked.fields.iter().all(|field| field.precision == 10));
}

/// A `BIGNUMERIC(76, 38)` column has the Arrow type of plain BIGNUMERIC; the declared shape that
/// goes with it keeps it parameterized, so an append into it is admitted.
#[test]
fn an_append_into_a_bignumeric_76_38_column_keeps_its_shape() {
    use crate::ingest::schema::{arrow_schema_to_bigquery, validate_existing_schema};
    use crate::rest::metadata::TableId;

    let table = table_schema([decimal_column("big", "BIGNUMERIC", 76, 38)]);
    let bound = Schema::new(vec![Field::new("big", DataType::Decimal128(10, 2), true)]);
    let aligned = align_with_table(&bound, &table).unwrap();
    assert_eq!(aligned.field(0).data_type(), &DataType::Decimal256(76, 38));
    let checked = arrow_schema_to_bigquery(&aligned).unwrap();
    assert_eq!(
        (checked.fields[0].precision, checked.fields[0].scale),
        (76, 38)
    );
    let target = TableId {
        project_id: "p",
        dataset_id: "d",
        table_id: "t",
    };
    validate_existing_schema(target, Some(&table), &checked).unwrap();
}
