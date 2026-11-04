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
            DataType::List(Arc::new(Field::new("item", date.data_type().clone(), true))),
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
