//! The `BIGQUERY:type` annotation an ingest schema carries: which spellings the driver accepts,
//! and what it refuses to let contradict an Arrow type.

use super::*;

#[test]
fn ingest_metadata_must_use_the_googlesql_names_the_driver_emits() {
    let schema_of = |data_type: DataType, rich_type: &str| {
        Schema::new(vec![bigquery_typed(
            Field::new("value", data_type, true),
            rich_type,
        )])
    };

    for (data_type, rich_type, rest) in [
        (DataType::Int64, "INT64", "INTEGER"),
        (DataType::Float64, "FLOAT64", "FLOAT"),
        (DataType::Boolean, "BOOL", "BOOLEAN"),
        (DataType::Date32, "DATE", "DATE"),
        (DataType::Binary, "BYTES", "BYTES"),
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            "INTERVAL",
            "INTERVAL",
        ),
    ] {
        let accepted = arrow_schema_to_bigquery(&schema_of(data_type.clone(), rich_type)).unwrap();
        assert_eq!(accepted.fields[0].r#type, rest);

        if rich_type == rest {
            continue;
        }
        let rejected = arrow_schema_to_bigquery(&schema_of(data_type, rest)).unwrap_err();
        assert_eq!(rejected.status, Status::InvalidArguments);
        assert!(
            rejected.message.contains(rich_type),
            "{} should name the GoogleSQL spelling: {}",
            rest,
            rejected.message
        );
    }
}

#[test]
fn ingest_rejects_scalar_metadata_that_contradicts_its_arrow_type() {
    let mislabelled = Schema::new(vec![bigquery_typed(
        Field::new("value", DataType::Float64, true),
        "STRING",
    )]);
    let failure = arrow_schema_to_bigquery(&mislabelled).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_message(&failure, &["incompatible"]);

    let unknown = Schema::new(vec![bigquery_typed(
        Field::new("value", DataType::Int64, true),
        "NOT_A_TYPE",
    )]);
    assert_eq!(
        arrow_schema_to_bigquery(&unknown).unwrap_err().status,
        Status::NotImplemented
    );

    // A composite field's metadata must describe a composite, not be quietly ignored.
    let list = Schema::new(vec![bigquery_typed(
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        "FLOAT64",
    )]);
    assert_eq!(
        arrow_schema_to_bigquery(&list).unwrap_err().status,
        Status::InvalidArguments
    );

    let record = Schema::new(vec![bigquery_typed(
        Field::new(
            "value",
            DataType::Struct(vec![Field::new("child", DataType::Int64, true)].into()),
            true,
        ),
        "INT64",
    )]);
    assert_eq!(
        arrow_schema_to_bigquery(&record).unwrap_err().status,
        Status::InvalidArguments
    );
}

#[test]
fn ingest_array_element_metadata_must_use_googlesql_names() {
    let list = |rich_type: &str| {
        Schema::new(vec![bigquery_typed(
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            rich_type,
        )])
    };
    let accepted = arrow_schema_to_bigquery(&list("ARRAY<FLOAT64>")).unwrap();
    assert_eq!(accepted.fields[0].r#type, "FLOAT");
    assert_eq!(accepted.fields[0].mode, "REPEATED");

    let rejected = arrow_schema_to_bigquery(&list("ARRAY<FLOAT>")).unwrap_err();
    assert_eq!(rejected.status, Status::InvalidArguments);
    assert_message(&rejected, &["FLOAT64"]);
}
