//! The Arrow schema of the parameters a dry run infers.

use super::*;

#[test]
fn converts_inferred_parameter_schema() {
    let parameters = vec![
        param("integer", scalar_type("INT64")),
        QueryParameter {
            name: "unknown".to_owned(),
            ..Default::default()
        },
        param(
            "",
            QueryParameterType {
                array_type: Some(Box::new(QueryParameterType {
                    struct_types: vec![QueryParameterStructType {
                        name: "timestamp".to_owned(),
                        r#type: Some(Box::new(scalar_type("TIMESTAMP"))),
                    }],
                    ..scalar_type("STRUCT")
                })),
                ..scalar_type("ARRAY")
            },
        ),
        param(
            "date_range",
            QueryParameterType {
                range_element_type: Some(Box::new(scalar_type("DATE"))),
                ..scalar_type("RANGE")
            },
        ),
        param("document", scalar_type("JSON")),
        param("location", scalar_type("GEOGRAPHY")),
        param(
            "documents",
            QueryParameterType {
                array_type: Some(Box::new(scalar_type("JSON"))),
                ..scalar_type("ARRAY")
            },
        ),
        param("duration", scalar_type("INTERVAL")),
    ];

    let schema = parameter_schema(&parameters).unwrap();
    assert_eq!(
        schema.field(0),
        &Field::new("integer", DataType::Int64, true)
    );
    assert_eq!(
        schema.field(1),
        &Field::new("unknown", DataType::Null, true)
    );
    assert_eq!(schema.field(2).name(), "");
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ))]
                .into(),
            ),
            true,
        )))
    );
    assert_eq!(
        schema.field(3).data_type(),
        &DataType::Struct(
            vec![
                Arc::new(Field::new("start", DataType::Date32, true)),
                Arc::new(Field::new("end", DataType::Date32, true)),
            ]
            .into(),
        )
    );
    assert_eq!(
        schema.field(3).metadata().get(BIGQUERY_TYPE_METADATA_KEY),
        Some(&"RANGE<DATE>".to_owned())
    );
    assert_eq!(
        schema.field(4).metadata().get(BIGQUERY_TYPE_METADATA_KEY),
        Some(&"JSON".to_owned())
    );
    assert_eq!(
        schema.field(4).metadata().get("ARROW:extension:name"),
        Some(&"arrow.json".to_owned())
    );
    assert_eq!(
        schema.field(5).metadata().get(BIGQUERY_TYPE_METADATA_KEY),
        Some(&"GEOGRAPHY".to_owned())
    );
    assert_eq!(
        schema.field(6).metadata().get(BIGQUERY_TYPE_METADATA_KEY),
        Some(&"ARRAY<JSON>".to_owned())
    );
    let DataType::List(item) = schema.field(6).data_type() else {
        panic!("expected a list field");
    };
    assert_eq!(
        item.metadata().get(BIGQUERY_TYPE_METADATA_KEY),
        Some(&"JSON".to_owned())
    );
    assert_eq!(
        schema.field(7),
        &Field::new(
            "duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        )
    );
}

/// A parameter BigQuery describes but Arrow cannot carry is refused by name, with the value that
/// forced the refusal and the way out; nothing here may leak a Rust `Debug` rendering.
#[test]
fn inferred_parameter_schema_rejections_name_the_parameter_and_the_way_out() {
    let timestamp = |precision: i64| {
        vec![param(
            "moment",
            QueryParameterType {
                timestamp_precision: Some(precision),
                ..scalar_type("TIMESTAMP")
            },
        )]
    };
    let failure = parameter_schema(&timestamp(9)).unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "BigQuery query parameter \"moment\" is TIMESTAMP(9), and the driver supports at most \
         microsecond precision; CAST it to TIMESTAMP(6) or narrower"
    );
    // Microsecond precision is exactly what an Arrow timestamp carries, so it is accepted.
    assert_eq!(
        parameter_schema(&timestamp(6))
            .unwrap()
            .field(0)
            .data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    let range = vec![param(
        "span",
        QueryParameterType {
            range_element_type: Some(Box::new(scalar_type("INT64"))),
            ..scalar_type("RANGE")
        },
    )];
    let failure = parameter_schema(&range).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "BigQuery inferred RANGE parameter \"span\" has unsupported element type INT64; use \
         DATE, DATETIME, or TIMESTAMP"
    );
}
