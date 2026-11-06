//! The Arrow schema of the parameters a dry run infers.

use super::*;

fn inferred_schema(parameters: serde_json::Value) -> adbc_core::error::Result<Schema> {
    let parameters: Vec<QueryParameter> = serde_json::from_value(parameters).unwrap();
    parameter_schema(&parameters)
}

#[test]
fn converts_inferred_parameter_schema() {
    let schema = inferred_schema(json!([
        {"name": "integer", "parameterType": {"type": "INT64"}},
        {"name": "unknown"},
        {"parameterType": {"type": "ARRAY", "arrayType": {
            "type": "STRUCT", "structTypes": [
                {"name": "timestamp", "type": {"type": "TIMESTAMP"}}
            ]
        }}},
        {"name": "date_range", "parameterType": {
            "type": "RANGE", "rangeElementType": {"type": "DATE"}
        }},
        {"name": "document", "parameterType": {"type": "JSON"}},
        {"name": "location", "parameterType": {"type": "GEOGRAPHY"}},
        {"name": "documents", "parameterType": {
            "type": "ARRAY", "arrayType": {"type": "JSON"}
        }},
        {"name": "duration", "parameterType": {"type": "INTERVAL"}}
    ]))
    .unwrap();
    let json_field = |name| {
        Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([
            ("BIGQUERY:type".into(), "JSON".into()),
            ("ARROW:extension:name".into(), "arrow.json".into()),
        ]))
    };
    let timestamp = Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    );
    let expected = Schema::new(vec![
        Field::new("integer", DataType::Int64, true),
        Field::new("unknown", DataType::Null, true),
        Field::new(
            "",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![timestamp].into()),
                true,
            ))),
            true,
        ),
        bigquery_typed(
            Field::new(
                "date_range",
                DataType::Struct(range_bounds(DataType::Date32)),
                true,
            ),
            "RANGE<DATE>",
        ),
        json_field("document"),
        Field::new("location", DataType::Utf8, true).with_metadata(HashMap::from([
            ("BIGQUERY:type".into(), "GEOGRAPHY".into()),
            ("ARROW:extension:name".into(), "geoarrow.wkt".into()),
            (
                "ARROW:extension:metadata".into(),
                r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#.into(),
            ),
        ])),
        bigquery_typed(
            Field::new(
                "documents",
                DataType::List(Arc::new(json_field("item"))),
                true,
            ),
            "ARRAY<JSON>",
        ),
        Field::new(
            "duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
    ]);
    assert_eq!(schema, expected);
}

#[test]
fn inferred_parameter_schema_rejections_name_the_parameter_and_the_way_out() {
    let timestamp = |precision: i64| {
        json!([{
            "name": "moment", "parameterType": {"type": "TIMESTAMP", "timestampPrecision": precision}
        }])
    };
    let failure = inferred_schema(timestamp(9)).unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "BigQuery query parameter \"moment\" is TIMESTAMP(9), and the driver supports at most \
         microsecond precision; CAST it to TIMESTAMP(6) or narrower"
    );
    // Microsecond precision is exactly what an Arrow timestamp carries, so it is accepted.
    assert_eq!(
        inferred_schema(timestamp(6)).unwrap().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    let failure = inferred_schema(json!([{
        "name": "span", "parameterType": {
            "type": "RANGE", "rangeElementType": {"type": "INT64"}
        }
    }]))
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "BigQuery inferred RANGE parameter \"span\" has unsupported element type INT64; use \
         DATE, DATETIME, or TIMESTAMP"
    );
}
