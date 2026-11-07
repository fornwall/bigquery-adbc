//! `RANGE` ingest columns: the element type they convert to, and the shapes they reject.

use super::*;

#[test]
fn converts_range_ingest_schema() {
    let range = |name: &str, element_type: &str, data_type: DataType| {
        bigquery_typed(
            Field::new(name, DataType::Struct(range_bounds(data_type)), true),
            format!("RANGE<{element_type}>"),
        )
    };
    let date = range("date_range", "DATE", DataType::Date32);
    let datetime = range(
        "datetime_range",
        "DATETIME",
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let timestamp = range(
        "timestamp_range",
        "TIMESTAMP",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );
    let repeated_date = bigquery_typed(
        Field::new(
            "date_ranges",
            DataType::new_list(date.data_type().clone(), true),
            true,
        ),
        "ARRAY<RANGE<DATE>>",
    );
    let bigquery =
        arrow_schema_to_bigquery(&Schema::new(vec![date, datetime, timestamp, repeated_date]))
            .unwrap();

    for (field, element_type, mode) in [
        (&bigquery.fields[0], "DATE", "NULLABLE"),
        (&bigquery.fields[1], "DATETIME", "NULLABLE"),
        (&bigquery.fields[2], "TIMESTAMP", "NULLABLE"),
        (&bigquery.fields[3], "DATE", "REPEATED"),
    ] {
        assert_eq!(field.r#type, "RANGE");
        assert_eq!(field.mode, mode);
        assert!(field.fields.is_empty());
        assert_eq!(
            field
                .range_element_type
                .as_ref()
                .map(|element| element.r#type.as_str()),
            Some(element_type)
        );
    }
}

#[test]
fn validates_range_ingest_schema() {
    let range_field = |metadata: &str, fields: Fields| {
        bigquery_typed(
            Field::new("period", DataType::Struct(fields), true),
            metadata,
        )
    };
    for field in [
        range_field("RANGE<STRING>", range_bounds(DataType::Date32)),
        range_field("RANGE<DATE", range_bounds(DataType::Date32)),
        range_field(
            "RANGE<DATE>",
            vec![
                Field::new("lower", DataType::Date32, true),
                Field::new("end", DataType::Date32, true),
            ]
            .into(),
        ),
        range_field(
            "RANGE<TIMESTAMP>",
            range_bounds(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ),
    ] {
        assert_eq!(
            arrow_schema_to_bigquery(&Schema::new(vec![field]))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }
}

fn range_batch(
    kind: &str,
    start: ArrayRef,
    end: ArrayRef,
    nulls: Option<NullBuffer>,
) -> RecordBatch {
    let bounds = range_bounds(start.data_type().clone());
    let field = bigquery_typed(
        Field::new("period", DataType::Struct(bounds.clone()), true),
        format!("RANGE<{kind}>"),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StructArray::new(bounds, vec![start, end], nulls))],
    )
    .unwrap()
}

#[test]
fn rejects_empty_and_reversed_ranges_after_normalization() {
    for end in [0, -1] {
        for (kind, start, end) in [
            (
                "DATE",
                Arc::new(Date32Array::from(vec![0])) as ArrayRef,
                Arc::new(Date32Array::from(vec![end])) as ArrayRef,
            ),
            (
                "DATETIME",
                Arc::new(TimestampMicrosecondArray::from(vec![0])),
                Arc::new(TimestampMicrosecondArray::from(vec![i64::from(end)])),
            ),
            (
                "TIMESTAMP",
                Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC")),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![i64::from(end)]).with_timezone("UTC"),
                ),
            ),
        ] {
            let batch = range_batch(kind, start, end, None);
            let schema = Arc::new(normalize_schema(batch.schema_ref()).unwrap());
            let failure = normalize_batch(batch, schema).unwrap_err();
            assert_failure(
                &failure,
                Status::InvalidData,
                &["RANGE", "period", "lower bound", "row 0"],
            );
        }
    }

    let batch = range_batch(
        "TIMESTAMP",
        Arc::new(TimestampNanosecondArray::from(vec![1]).with_timezone("UTC")),
        Arc::new(TimestampNanosecondArray::from(vec![999]).with_timezone("UTC")),
        None,
    );
    let schema = Arc::new(normalize_schema(batch.schema_ref()).unwrap());
    assert_eq!(
        normalize_batch(batch, schema).unwrap_err().status,
        Status::InvalidData,
        "bounds that become equal at microsecond precision must be rejected"
    );
}

#[test]
fn ranges_allow_unbounded_ends_and_ignore_bounds_of_null_ranges() {
    let batch = range_batch(
        "DATE",
        Arc::new(Date32Array::from(vec![
            Some(0),
            None,
            Some(0),
            None,
            Some(9),
        ])),
        Arc::new(Date32Array::from(vec![
            Some(1),
            Some(0),
            None,
            None,
            Some(-9),
        ])),
        Some(NullBuffer::from(vec![true, true, true, true, false])),
    );
    let schema = batch.schema();
    let normalized = normalize_batch(batch, schema).unwrap();
    assert_eq!(normalized.num_rows(), 5);
    assert!(normalized.column(0).is_null(4));

    // A plain STRUCT has no range ordering contract.
    let batch = range_batch(
        "DATE",
        Arc::new(Date32Array::from(vec![9])),
        Arc::new(Date32Array::from(vec![-9])),
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "period",
        batch.column(0).data_type().clone(),
        true,
    )]));
    normalize_batch(batch, schema).unwrap();
}

#[test]
fn invalid_repeated_ranges_report_the_sliced_input_row() {
    let batch = range_batch(
        "DATE",
        Arc::new(Date32Array::from(vec![0, 0, 3])),
        Arc::new(Date32Array::from(vec![1, 1, 2])),
        None,
    );
    let field = bigquery_typed(
        Field::new_list("periods", batch.schema().field(0).clone(), false),
        "ARRAY<RANGE<DATE>>",
    );
    let DataType::List(item) = field.data_type() else {
        panic!("periods must be a list");
    };
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0, 1, 1, 3].into()),
        batch.column(0).clone(),
        None,
    );
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let failure =
        crate::ingest::normalize::normalize_rows(batch.slice(1, 2), normalized_schema, 20)
            .unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidData,
        &["periods", "row 21", "lower bound"],
    );
}
