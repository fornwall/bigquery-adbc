//! Temporal value normalization: intervals, durations, and the sub-second precision BigQuery
//! carries.

use super::*;

#[test]
fn normalizes_day_time_intervals_to_month_day_nano() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "year_month",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
        Field::new("day_time", DataType::Interval(IntervalUnit::DayTime), false),
        Field::new(
            "month_day_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new(
            "submicrosecond_month_day_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]));
    let year_month = Arc::new(IntervalYearMonthArray::from(vec![-25])) as ArrayRef;
    let day_time = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
        -3,
        -14_706_008,
    )])) as ArrayRef;
    let month_day_nano = Arc::new(IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(14, 3, 14_706_007_008_000),
    ])) as ArrayRef;
    let submicrosecond_month_day_nano = Arc::new(IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(-14, -3, -1_999),
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            year_month.clone(),
            day_time,
            month_day_nano.clone(),
            submicrosecond_month_day_nano,
        ],
    )
    .unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let normalized = normalize_batch(batch, normalized_schema).unwrap();

    assert!(Arc::ptr_eq(normalized.column(0), &year_month));
    assert_eq!(
        column_as::<IntervalMonthDayNanoArray>(&normalized, 1).value(0),
        IntervalMonthDayNano::new(0, -3, -14_706_008_000_000)
    );
    assert!(Arc::ptr_eq(normalized.column(2), &month_day_nano));
    assert_eq!(
        column_as::<IntervalMonthDayNanoArray>(&normalized, 3).value(0),
        IntervalMonthDayNano::new(-14, -3, -1_000)
    );
}

#[test]
fn normalizes_and_validates_same_type_intervals() {
    let column = Arc::new(IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 1_999),
    ])) as ArrayRef;
    let schema = one_column_schema("duration", column.data_type(), false);
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    let normalized = normalize_batch(batch, schema).unwrap();
    assert_eq!(
        column_as::<IntervalMonthDayNanoArray>(&normalized, 0).value(0),
        IntervalMonthDayNano::new(1, 2, 1_000)
    );

    for column in [
        Arc::new(IntervalYearMonthArray::from(vec![120_001])) as ArrayRef,
        Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            3_660_001, 0,
        )])) as ArrayRef,
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(120_001, 0, 0),
        ])) as ArrayRef,
    ] {
        let schema = one_column_schema("duration", column.data_type(), false);
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        assert_eq!(
            normalize_batch(batch, normalized_schema)
                .unwrap_err()
                .status,
            Status::InvalidData
        );
    }
}

#[test]
fn normalizes_duration_values_to_month_day_nano() {
    let repeated = ListArray::from_iter_primitive::<DurationSecondType, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("seconds", DataType::Duration(TimeUnit::Second), true),
        Field::new(
            "milliseconds",
            DataType::Duration(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "microseconds",
            DataType::Duration(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "nanoseconds",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new("repeated", repeated.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(DurationSecondArray::from(vec![Some(259_200), None])),
            Arc::new(DurationMillisecondArray::from(vec![
                Some(-273_906_008),
                None,
            ])),
            Arc::new(DurationMicrosecondArray::from(vec![
                Some(273_906_007_008),
                None,
            ])),
            Arc::new(DurationNanosecondArray::from(vec![
                Some(-273_906_007_008_999),
                None,
            ])),
            Arc::new(repeated),
        ],
    )
    .unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let normalized = normalize_batch(batch, normalized_schema).unwrap();
    let value =
        |column: usize| column_as::<IntervalMonthDayNanoArray>(&normalized, column).value(0);
    // A duration is an exact elapsed time, so its whole span is the interval's time component:
    // three days are 72 hours, as the parameter path binds it.
    assert_eq!(
        value(0),
        IntervalMonthDayNano::new(0, 0, 259_200_000_000_000)
    );
    assert_eq!(
        value(1),
        IntervalMonthDayNano::new(0, 0, -273_906_008_000_000)
    );
    assert_eq!(
        value(2),
        IntervalMonthDayNano::new(0, 0, 273_906_007_008_000)
    );
    assert_eq!(
        value(3),
        IntervalMonthDayNano::new(0, 0, -273_906_007_008_000)
    );
    for column in 0..4 {
        assert!(normalized.column(column).is_null(1));
    }
    let repeated = column_as::<ListArray>(&normalized, 4);
    let values = array_as::<IntervalMonthDayNanoArray>(repeated.values());
    assert_eq!(
        values.value(0),
        IntervalMonthDayNano::new(0, 0, 1_000_000_000)
    );
    assert_eq!(
        values.value(1),
        IntervalMonthDayNano::new(0, 0, 2_000_000_000)
    );
    assert!(!repeated.is_null(1));
    assert_eq!(repeated.value_length(1), 0);
}

#[test]
fn narrows_nanosecond_temporals_to_microseconds() {
    let time = Arc::new(Time64NanosecondArray::from(vec![
        Some(3_723_456_789_999),
        None,
    ])) as ArrayRef;
    let timestamp = Arc::new(
        TimestampNanosecondArray::from(vec![Some(-1_999), None]).with_timezone("Europe/Stockholm"),
    ) as ArrayRef;
    let repeated = Arc::new(
        ListArray::from_iter_primitive::<Time64NanosecondType, _, _>(vec![
            Some(vec![Some(42_999), Some(1_999)]),
            None,
        ]),
    ) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("time_value", time.data_type().clone(), true),
        Field::new("timestamp_value", timestamp.data_type().clone(), true),
        Field::new("repeated_time", repeated.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![time, timestamp, repeated]).unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let normalized = normalize_batch(batch, normalized_schema).unwrap();

    let time = column_as::<Time64MicrosecondArray>(&normalized, 0);
    assert_eq!(time.value(0), 3_723_456_789);
    assert!(time.is_null(1));

    // A time of day truncates, while an instant rounds down: -1,999 ns is 1969-12-31
    // 23:59:59.999998001, whose microsecond is ...998.
    let timestamp = column_as::<TimestampMicrosecondArray>(&normalized, 1);
    assert_eq!(timestamp.value(0), -2);
    assert!(timestamp.is_null(1));
    assert_eq!(
        timestamp.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    let repeated = column_as::<ListArray>(&normalized, 2);
    let values = array_as::<Time64MicrosecondArray>(repeated.values());
    assert_eq!(values.value(0), 42);
    assert_eq!(values.value(1), 1);
    assert!(!repeated.is_null(0));
    assert!(!repeated.is_null(1));
    assert_eq!(repeated.value_length(1), 0);
}

#[test]
fn only_packs_datetime_values_inside_ranges() {
    let datetime = Field::new(
        "datetime",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    );
    let range_fields = range_bounds(DataType::Timestamp(TimeUnit::Microsecond, None));
    let range = bigquery_typed(
        Field::new(
            "datetime_range",
            DataType::Struct(range_fields.clone()),
            false,
        ),
        "RANGE<DATETIME>",
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![datetime, range])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_001])) as ArrayRef,
            Arc::new(StructArray::new(
                range_fields,
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_001)])),
                    Arc::new(TimestampMicrosecondArray::from(vec![None])),
                ],
                None,
            )),
        ],
    )
    .unwrap();
    let schema = Arc::new(normalize_schema(batch.schema().as_ref()).unwrap());
    let normalized = normalize_batch(batch, schema).unwrap();

    assert_eq!(
        column_as::<TimestampMicrosecondArray>(&normalized, 0).value(0),
        1_000_001
    );
    let range = column_as::<StructArray>(&normalized, 1);
    assert_eq!(
        array_as::<TimestampMicrosecondArray>(range.column(0)).value(0),
        138_630_961_516_511_233
    );
    assert!(range.column(1).is_null(0));
}

/// `ARRAY<RANGE<DATETIME>>` metadata sits on the LIST field, but only the normalized *item*
/// field decides whether `normalize_column` packs a range bound into BigQuery's civil-datetime
/// encoding. Without propagating the element type the destination column is still created as
/// `RANGE<DATETIME> REPEATED` and then filled with raw epoch microseconds: silently wrong dates.
#[test]
fn packs_datetime_bounds_inside_repeated_ranges() {
    let range_fields = range_bounds(DataType::Timestamp(TimeUnit::Microsecond, None));
    let item = Field::new("item", DataType::Struct(range_fields.clone()), true);
    let ranges = bigquery_typed(
        Field::new(
            "datetime_ranges",
            DataType::List(Arc::new(item.clone())),
            true,
        ),
        "ARRAY<RANGE<DATETIME>>",
    );
    let bounds = StructArray::new(
        range_fields,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_001)])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![None])) as ArrayRef,
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![ranges])),
        vec![Arc::new(ListArray::new(
            Arc::new(item),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(bounds) as ArrayRef,
            None,
        )) as ArrayRef],
    )
    .unwrap();

    let schema = Arc::new(normalize_schema(batch.schema().as_ref()).unwrap());
    let bigquery = arrow_schema_to_bigquery(&schema).unwrap();
    assert_eq!(bigquery.fields[0].r#type, "RANGE");
    assert_eq!(bigquery.fields[0].mode, "REPEATED");

    let normalized = normalize_batch(batch, schema).unwrap();
    let elements = array_as::<StructArray>(column_as::<ListArray>(&normalized, 0).values()).clone();
    assert_eq!(
        array_as::<TimestampMicrosecondArray>(elements.column(0)).value(0),
        138_630_961_516_511_233
    );
    assert!(elements.column(1).is_null(0));
}
