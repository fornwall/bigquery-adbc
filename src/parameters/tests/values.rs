//! The literal each bound value renders as, and the values that are refused.

use super::*;

/// The largest INTERVAL time component BigQuery accepts, in microseconds: the documented second
/// range ends at .999999, but `INTERVAL '0-0 0 87840000:0:0.000001'` is refused while
/// `INTERVAL '0-0 0 87840000:0:0'` is accepted (observed).
const MAX_INTERVAL_MICROS: i64 = 316_224_000_000_000_000;

#[track_caller]
fn assert_renders(array: ArrayRef, expected_type: &str, expected: &str) {
    let label = format!("{} -> {expected}", array.data_type());
    let parameter = one_parameter(array);
    assert_eq!(ptype(&parameter).r#type, expected_type, "{label}");
    assert_eq!(value_of(&parameter), Some(expected), "{label}");
}

#[track_caller]
fn assert_refused(array: ArrayRef, fragment: &str) {
    let label = array.data_type().to_string();
    let message = one_parameter_error(array);
    assert!(message.contains(fragment), "{label}: {message}");
}

/// Floats spell their non-finite values and keep a signed zero. A narrower float widens exactly,
/// as ingest widens it, rather than being respelled by its shortest single-precision decimal
/// (`0.1`), which is a different FLOAT64.
#[test]
fn float_values_spell_non_finite_values_and_keep_a_signed_zero() {
    for (value, expected) in [
        (f64::NAN, "NaN"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
        (-0.0, "-0"),
        (0.0, "0"),
    ] {
        assert_renders(col(Float64Array::from(vec![value])), "FLOAT64", expected);
    }
    for (value, expected) in [
        (f32::NAN, "NaN"),
        (f32::NEG_INFINITY, "-Infinity"),
        (-0.0, "-0"),
        (0.1, "0.10000000149011612"),
    ] {
        assert_renders(col(Float32Array::from(vec![value])), "FLOAT64", expected);
    }
    for (value, expected) in [
        (F16::NAN, "NaN"),
        (F16::INFINITY, "Infinity"),
        (-F16::ZERO, "-0"),
    ] {
        assert_renders(col(Float16Array::from(vec![value])), "FLOAT64", expected);
    }
}

/// BigQuery's DATE range is 0001-01-01 to 9999-12-31, and a TIME is one day. A Date64 carries
/// milliseconds, but a DATE has no time of day, so anything but an exact UTC midnight is refused
/// rather than truncated.
#[test]
fn dates_and_times_are_bounded_by_bigquerys_calendar_and_a_day() {
    for (days, expected) in [
        (-719_162, "0001-01-01"),
        (0, "1970-01-01"),
        (2_932_896, "9999-12-31"),
    ] {
        assert_renders(col(Date32Array::from(vec![days])), "DATE", expected);
    }
    assert_renders(
        col(Date64Array::from(vec![86_400_000])),
        "DATE",
        "1970-01-02",
    );
    for days in [-719_163, 2_932_897] {
        assert_refused(
            col(Date32Array::from(vec![days])),
            "outside BigQuery's supported year range 1 to 9999",
        );
    }
    for millis in [86_400_001, -1, i64::MIN] {
        assert_refused(
            col(Date64Array::from(vec![millis])),
            "not aligned to a UTC day",
        );
    }
    // An aligned Date64 outside the calendar is a calendar failure, not an arithmetic one.
    let aligned = i64::MIN / 86_400_000 * 86_400_000;
    assert_refused(
        col(Date64Array::from(vec![aligned])),
        "supported year range",
    );

    let times: [(ArrayRef, &str, ArrayRef, ArrayRef); 5] = [
        (
            col(Time32SecondArray::from(vec![86_399])),
            "23:59:59",
            col(Time32SecondArray::from(vec![86_400])),
            col(Time32SecondArray::from(vec![-1])),
        ),
        (
            col(Time32MillisecondArray::from(vec![86_399_999])),
            "23:59:59.999000",
            col(Time32MillisecondArray::from(vec![86_400_000])),
            col(Time32MillisecondArray::from(vec![-1])),
        ),
        (
            col(Time64MicrosecondArray::from(vec![86_399_999_999])),
            "23:59:59.999999",
            col(Time64MicrosecondArray::from(vec![86_400_000_000])),
            col(Time64MicrosecondArray::from(vec![-1])),
        ),
        (
            col(Time64NanosecondArray::from(vec![86_399_999_999_999])),
            "23:59:59.999999",
            col(Time64NanosecondArray::from(vec![86_400_000_000_000])),
            col(Time64NanosecondArray::from(vec![-1])),
        ),
        (
            col(Time64NanosecondArray::from(vec![3_723_000_004_999])),
            "01:02:03.000004",
            col(Time64NanosecondArray::from(vec![i64::MAX])),
            col(Time64NanosecondArray::from(vec![i64::MIN])),
        ),
    ];
    for (last, expected, past_end, negative) in times {
        assert_renders(last, "TIME", expected);
        assert_refused(past_end, "outside a day");
        assert_refused(negative, "outside a day");
    }
}

/// A zoned timestamp is a TIMESTAMP and a naive one a DATETIME. Instants round sub-microsecond
/// precision down, so a value just before the epoch stays there. Scaling a second or millisecond
/// extreme to microseconds leaves i64 and is refused as such; a microsecond extreme needs no
/// scaling and is refused by the calendar; nanoseconds span only 1677 to 2262, so both extremes
/// bind.
#[test]
fn timestamps_round_down_to_microseconds_within_bigquerys_calendar() {
    let utc = || Some(Arc::<str>::from("UTC"));
    for (micros, timezone, expected_type, expected) in [
        (-1, utc(), "TIMESTAMP", "1969-12-31T23:59:59.999999Z"),
        (1_000_002, None, "DATETIME", "1970-01-01T00:00:01.000002"),
    ] {
        let array = TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(timezone);
        assert_renders(col(array), expected_type, expected);
    }
    for (nanos, timezone, expected) in [
        (-1_999, utc(), "1969-12-31T23:59:59.999998Z"),
        (1_999, None, "1970-01-01T00:00:00.000001"),
        (-500, None, "1969-12-31T23:59:59.999999"),
        (i64::MIN, None, "1677-09-21T00:12:43.145224"),
        (i64::MAX, None, "2262-04-11T23:47:16.854775"),
    ] {
        let zoned = timezone.is_some();
        let array = TimestampNanosecondArray::from(vec![nanos]).with_timezone_opt(timezone);
        let expected_type = if zoned { "TIMESTAMP" } else { "DATETIME" };
        assert_renders(col(array), expected_type, expected);
    }
    for value in [i64::MIN, i64::MAX] {
        let overflow = "overflows microsecond precision";
        assert_refused(col(TimestampSecondArray::from(vec![value])), overflow);
        assert_refused(col(TimestampMillisecondArray::from(vec![value])), overflow);
        assert_refused(
            col(TimestampMicrosecondArray::from(vec![value])),
            "supported year range",
        );
    }
}

/// Intervals and durations bind as INTERVAL in GoogleSQL's `Y-M D H:M:S[.F]` spelling, each sign
/// governing its group: ZetaSQL prints -1 microsecond as `0-0 0 -0:0:0.000001`, so a negative
/// sub-hour span carries its sign on a zero hour. Sub-microsecond precision truncates toward zero,
/// keeping a span's magnitude symmetric. A duration puts its whole span in the time component, as
/// ingest does, so two days are 48 hours on both paths.
#[test]
fn intervals_and_durations_render_in_googlesql_interval_form() {
    let day_time = |days, millis| {
        col(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            days, millis,
        )]))
    };
    let month_day_nano = |months, days, nanos| {
        col(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(months, days, nanos),
        ]))
    };
    let cases = [
        (col(IntervalYearMonthArray::from(vec![-25])), "-2-1 0 0:0:0"),
        (day_time(-3, -14_706_008), "0-0 -3 -4:5:6.008000"),
        // Milliseconds cannot leave the range a day-time interval's days already bound.
        (day_time(0, i32::MIN), "0-0 0 -596:31:23.648000"),
        (
            month_day_nano(14, 3, 14_706_007_008_999),
            "1-2 3 4:5:6.007008",
        ),
        (month_day_nano(0, 0, -1_999), "0-0 0 -0:0:0.000001"),
        (
            month_day_nano(0, 0, i64::MIN),
            "0-0 0 -2562047:47:16.854775",
        ),
        (col(DurationSecondArray::from(vec![-1])), "0-0 0 -0:0:1"),
        (
            col(DurationSecondArray::from(vec![172_800])),
            "0-0 0 48:0:0",
        ),
        (
            col(DurationSecondArray::from(vec![-172_800])),
            "0-0 0 -48:0:0",
        ),
        (col(DurationSecondArray::from(vec![90_061])), "0-0 0 25:1:1"),
        (
            col(DurationMillisecondArray::from(vec![14_706_008])),
            "0-0 0 4:5:6.008000",
        ),
        (
            col(DurationMicrosecondArray::from(vec![-14_706_007_008])),
            "0-0 0 -4:5:6.007008",
        ),
        (
            col(DurationMicrosecondArray::from(vec![MAX_INTERVAL_MICROS])),
            "0-0 0 87840000:0:0",
        ),
        (
            col(DurationMicrosecondArray::from(vec![-MAX_INTERVAL_MICROS])),
            "0-0 0 -87840000:0:0",
        ),
        (
            col(DurationNanosecondArray::from(vec![1_999])),
            "0-0 0 0:0:0.000001",
        ),
        // A nanosecond duration cannot overflow: it only ever truncates.
        (
            col(DurationNanosecondArray::from(vec![i64::MIN])),
            "0-0 0 -2562047:47:16.854775",
        ),
    ];
    for (array, expected) in cases {
        assert_renders(array, "INTERVAL", expected);
    }

    // BigQuery bounds an INTERVAL to +/-10,000 years, in months and in days alike.
    let out_of_range = "outside BigQuery's supported interval range";
    assert_refused(
        col(IntervalYearMonthArray::from(vec![120_001])),
        out_of_range,
    );
    assert_refused(day_time(3_660_001, 0), out_of_range);
    for months in [i32::MIN, i32::MAX] {
        assert_refused(month_day_nano(months, 0, i64::MIN), out_of_range);
    }
    let overflow = "overflows microsecond precision";
    assert_refused(col(DurationSecondArray::from(vec![i64::MAX])), overflow);
    assert_refused(
        col(DurationMillisecondArray::from(vec![i64::MIN])),
        overflow,
    );
    // An Arrow duration is the only bound value wider than an INTERVAL's time component, so the
    // driver refuses it rather than leaving BigQuery to refuse the formatted interval.
    for micros in [
        MAX_INTERVAL_MICROS + 1,
        -MAX_INTERVAL_MICROS - 1,
        i64::MAX,
        i64::MIN,
    ] {
        assert_refused(
            col(DurationMicrosecondArray::from(vec![micros])),
            &format!("time component {out_of_range}"),
        );
    }
}

#[test]
fn converts_scalar_parameter_rows() {
    let fields = vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("integer", DataType::Int64, true),
        Field::new("float", DataType::Float64, true),
        Field::new("boolean", DataType::Boolean, true),
        Field::new("bytes", DataType::Binary, true),
        Field::new("numeric", DataType::Decimal128(10, 2), true),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("hello"), None])),
        Arc::new(Int64Array::from(vec![Some(42), None])),
        Arc::new(Float64Array::from(vec![Some(f64::INFINITY), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(BinaryArray::from(vec![Some(&[1, 2, 3][..]), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(12_345), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

    let parameters = parameters_for_row(&batch, 0, ParameterMode::Named).unwrap();
    assert_eq!(
        parameters
            .iter()
            .map(|parameter| parameter.name.as_str())
            .collect::<Vec<_>>(),
        ["text", "integer", "float", "boolean", "bytes", "numeric"]
    );
    assert_eq!(
        parameters
            .iter()
            .map(|parameter| ptype(parameter).r#type.as_str())
            .collect::<Vec<_>>(),
        ["STRING", "INT64", "FLOAT64", "BOOL", "BYTES", "NUMERIC"]
    );
    assert_eq!(
        values(&parameters),
        [
            Some("hello"),
            Some("42"),
            Some("Infinity"),
            Some("true"),
            Some("AQID"),
            Some("123.45"),
        ]
    );

    let nulls = parameters_for_row(&batch, 1, ParameterMode::Positional).unwrap();
    assert!(nulls.iter().all(|parameter| parameter.name.is_empty()));
    assert_eq!(values(&nulls), [None::<&str>; 6]);
}

/// BigQuery's parameter-free BIGNUMERIC spans one significant digit more than
/// `Decimal256(76, 38)` can declare, so the declared precision understates the type's own domain.
/// The read paths return the whole `i256` range and ingest accepts it back; binding a value the
/// driver itself just returned must not be refused by a third, stricter rule.
#[test]
fn bignumeric_parameters_accept_the_whole_i256_domain() {
    let bignumeric = |value: i256| {
        Arc::new(
            Decimal256Array::from(vec![value])
                .with_precision_and_scale(76, 38)
                .unwrap(),
        ) as ArrayRef
    };

    for (label, value, expected) in [
        (
            "i256::MAX",
            i256::MAX,
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967",
        ),
        (
            "i256::MIN",
            i256::MIN,
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
        ),
    ] {
        let parameter = one_parameter(bignumeric(value));
        assert_eq!(ptype(&parameter).r#type, "BIGNUMERIC", "{label}");
        assert_eq!(value_of(&parameter), Some(expected), "{label}");
    }

    // `1e38` is the smallest BIGNUMERIC value whose unscaled integer needs the 77th digit.
    let one_e38 = i256::from_string(&format!("1{}", "0".repeat(76))).unwrap();
    let expected_one_e38 = format!("1{}.{}", "0".repeat(38), "0".repeat(38));
    let parameter = one_parameter(bignumeric(one_e38));
    assert_eq!(value_of(&parameter), Some(expected_one_e38.as_str()));

    // Values inside the declared precision are spelled exactly as the validated path spells
    // them, so widening the budget changes nothing but the 77-digit outliers.
    for value in [
        i256::from_i128(0),
        i256::from_i128(-1),
        i256::from_i128(10_i128.pow(38)),
    ] {
        let array = Decimal256Array::from(vec![value])
            .with_precision_and_scale(76, 38)
            .unwrap();
        assert_eq!(
            value_of(&one_parameter(Arc::new(array.clone()))).map(str::to_owned),
            Some(array.value_as_string(0))
        );
    }

    // The exemption is exactly BIGNUMERIC's own shape: any other Decimal256 still has its
    // declared precision enforced.
    let message = one_parameter_error(decimal256(12_345_678, 5, 2));
    assert!(message.contains("precision 5"), "{message}");

    let outside_precision = i256::from_string(&format!("1{}", "0".repeat(75))).unwrap();
    let ordinary = Arc::new(
        Decimal256Array::from(vec![outside_precision])
            .with_precision_and_scale(75, 38)
            .unwrap(),
    ) as ArrayRef;
    let message = one_parameter_error(ordinary);
    assert!(message.contains("precision 75"), "{message}");
}

/// Base64 pads to a multiple of four characters, so a byte string's length modulo three picks the
/// encoding's padding.
#[test]
fn bytes_parameters_encode_every_base64_padding_length() {
    for (label, bytes, expected) in [
        ("empty", &[][..], ""),
        ("one byte", &[1][..], "AQ=="),
        ("two bytes", &[1, 2][..], "AQI="),
        ("three bytes", &[1, 2, 3][..], "AQID"),
    ] {
        let columns: [(&str, ArrayRef); 4] = [
            ("binary", Arc::new(BinaryArray::from(vec![bytes]))),
            (
                "large binary",
                Arc::new(LargeBinaryArray::from(vec![bytes])),
            ),
            ("binary view", Arc::new(BinaryViewArray::from(vec![bytes]))),
            (
                "fixed size binary",
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(bytes.to_vec())).unwrap(),
                ),
            ),
        ];
        for (kind, array) in columns {
            // A zero-width FixedSizeBinary is not a legal Arrow layout.
            if kind == "fixed size binary" && bytes.is_empty() {
                continue;
            }
            let parameter = one_parameter(array);
            assert_eq!(ptype(&parameter).r#type, "BYTES", "{label}/{kind}");
            assert_eq!(value_of(&parameter), Some(expected), "{label}/{kind}");
        }
    }
}

#[test]
fn converts_dictionary_parameter_rows() {
    let dictionary: DictionaryArray<Int8Type> = vec![Some("first"), None, Some("second")]
        .into_iter()
        .collect();
    let batch =
        RecordBatch::try_from_iter(vec![("label", Arc::new(dictionary) as ArrayRef)]).unwrap();

    for (row, expected) in [Some("first"), None, Some("second")]
        .into_iter()
        .enumerate()
    {
        let parameters = parameters_for_row(&batch, row, ParameterMode::Named).unwrap();
        assert_eq!(ptype(&parameters[0]).r#type, "STRING");
        assert_eq!(value_of(&parameters[0]), expected, "row {row}");
    }
}

/// Arrow has four list layouts (two offset widths, each with consecutive offsets or explicit
/// sizes) and BigQuery has one ARRAY. The shared conversions must read the same two elements from
/// each, including a view whose rows sit out of order.
#[test]
fn every_arrow_list_layout_binds_the_same_array_parameter() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let values = Arc::new(Int64Array::from(vec![3, 4, 1, 2])) as ArrayRef;
    let offsets = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;

    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
        offsets.clone(),
        None,
    );
    let large_list = LargeListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 4])),
        offsets,
        None,
    );
    // The views address the same two elements, but from the second half of the child array.
    let list_view = ListViewArray::new(
        item.clone(),
        ScalarBuffer::from(vec![2, 0]),
        ScalarBuffer::from(vec![2, 2]),
        values.clone(),
        None,
    );
    let large_list_view = LargeListViewArray::new(
        item,
        ScalarBuffer::from(vec![2_i64, 0]),
        ScalarBuffer::from(vec![2_i64, 2]),
        values,
        None,
    );

    for (label, array) in [
        ("List", Arc::new(list) as ArrayRef),
        ("LargeList", Arc::new(large_list) as ArrayRef),
        ("ListView", Arc::new(list_view) as ArrayRef),
        ("LargeListView", Arc::new(large_list_view) as ArrayRef),
    ] {
        let parameter = one_parameter(array);
        assert_eq!(
            json!(parameter),
            json!({
                "parameterType": {"type": "ARRAY", "arrayType": {"type": "INT64"}},
                "parameterValue": {"arrayValues": [{"value": "1"}, {"value": "2"}]}
            }),
            "{label}"
        );
    }
}

#[test]
fn converts_rich_logical_parameter_rows() {
    let range_fields = range_bounds(DataType::Date32);
    let range = StructArray::new(
        range_fields.clone(),
        vec![
            Arc::new(Date32Array::from(vec![Some(0), None, Some(4)])),
            Arc::new(Date32Array::from(vec![Some(2), Some(3), Some(5)])),
        ],
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let range_field = bigquery_typed(
        Field::new("period", DataType::Struct(range_fields), true),
        "RANGE<DATE>",
    );
    let json_field =
        Field::new("document", DataType::Utf8, false).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_owned(),
            "arrow.json".to_owned(),
        )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![range_field, json_field])),
        vec![
            Arc::new(range),
            Arc::new(StringArray::from(vec!["{\"id\":1}", "null", "{}"])),
        ],
    )
    .unwrap();

    for (row, expected) in [
        json!({"rangeValue": {"start": {"value": "1970-01-01"}, "end": {"value": "1970-01-03"}}}),
        json!({"rangeValue": {"end": {"value": "1970-01-04"}}}),
        json!({}),
    ]
    .into_iter()
    .enumerate()
    {
        let parameters = parameters_for_row(&batch, row, ParameterMode::Named).unwrap();
        assert_eq!(
            json!(ptype(&parameters[0])),
            json!({
                "type": "RANGE", "rangeElementType": {"type": "DATE"}
            })
        );
        assert_eq!(json!(value(&parameters[0])), expected, "row {row}");
        assert_eq!(ptype(&parameters[1]).r#type, "JSON");
    }
    let schema_parameters =
        parameters_for_schema(batch.schema().as_ref(), ParameterMode::Named).unwrap();
    assert_eq!(ptype(&schema_parameters[0]).r#type, "RANGE");
}

#[test]
fn converts_nested_parameter_rows() {
    let integers = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
    ]);
    let fixed = FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
        vec![Some(vec![Some(7), Some(8)]), None],
        2,
    );
    let labels = Arc::new(StringArray::from(vec!["first", "ignored"])) as ArrayRef;
    let flags = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(4), Some(5)]),
        Some(vec![Some(6)]),
    ])) as ArrayRef;
    let record = StructArray::new(
        vec![
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(Field::new("values", flags.data_type().clone(), false)),
        ]
        .into(),
        vec![labels, flags],
        Some(NullBuffer::from(vec![true, false])),
    );
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("integers", Arc::new(integers)),
        ("fixed", Arc::new(fixed)),
        ("record", Arc::new(record)),
    ];
    let batch = RecordBatch::try_from_iter(columns).unwrap();

    let parameters = parameters_for_row(&batch, 0, ParameterMode::Named).unwrap();
    let array_type = json!({"type": "ARRAY", "arrayType": {"type": "INT64"}});
    assert_eq!(json!(ptype(&parameters[0])), array_type);
    assert_eq!(json!(ptype(&parameters[1])), array_type);
    assert_eq!(
        json!(ptype(&parameters[2])),
        json!({
            "type": "STRUCT", "structTypes": [
                {"name": "label", "type": {"type": "STRING"}},
                {"name": "values", "type": array_type}
            ]
        })
    );
    assert_eq!(
        json!(parameters.iter().map(value).collect::<Vec<_>>()),
        json!([
            {"arrayValues": [{"value": "1"}, {"value": "2"}, {"value": "3"}]},
            {"arrayValues": [{"value": "7"}, {"value": "8"}]},
            {"structValues": {
                "label": {"value": "first"},
                "values": {"arrayValues": [{"value": "4"}, {"value": "5"}]}
            }}
        ])
    );
    let nulls = parameters_for_row(&batch, 1, ParameterMode::Named).unwrap();
    assert_eq!(
        json!(nulls.iter().map(value).collect::<Vec<_>>()),
        json!([{}, {}, {}])
    );
}

/// Every value error names the parameter and the row it came from, and is `INVALID_DATA`.
///
/// The two array rows also cover the rule that a BigQuery array holds no NULL element: an array
/// parameter carries no per-element nullability, so a null child would reach the service as an
/// empty value. Both offset layouts are covered because they address their child differently.
#[test]
fn value_errors_name_their_parameter_and_row() {
    let unsigned: ArrayRef = Arc::new(UInt64Array::from(vec![0, u64::MAX]));
    let misaligned: ArrayRef = Arc::new(Date64Array::from(vec![0, 1]));
    let overflowing: ArrayRef = Arc::new(TimestampSecondArray::from(vec![0, i64::MAX]));
    let with_null_element: ArrayRef =
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2), None]),
        ]));
    let fixed_with_null_element: ArrayRef =
        Arc::new(FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(
            vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), None])],
            2,
        ));
    for (name, column, fragment) in [
        ("huge", unsigned, "exceeds BigQuery INT64"),
        ("day", misaligned, "aligned to a UTC day"),
        ("moment", overflowing, "overflows microsecond precision"),
        ("items", with_null_element, "element 1 is NULL"),
        ("fixed", fixed_with_null_element, "element 1 is NULL"),
    ] {
        let batch = RecordBatch::try_from_iter([(name, column)]).unwrap();
        let failure = parameters_for_row(&batch, 1, ParameterMode::Named).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData, "{name}");
        assert_message(&failure, &[&format!("{name:?}"), "at row 1", fragment]);
    }
}

#[test]
fn invalid_parameter_values_do_not_appear_in_errors() {
    let future_day = crate::types::temporal::days_from_civil(31_337, 1, 1);
    let cases = [
        (
            col(UInt64Array::from(vec![u64::MAX])),
            "18446744073709551615",
            "UInt64",
        ),
        (
            col(Date32Array::from(vec![i32::try_from(future_day).unwrap()])),
            "31337",
            "year range",
        ),
        (
            col(Date64Array::from(vec![86_411_223])),
            "86411223",
            "not aligned",
        ),
        (
            col(Time32SecondArray::from(vec![123_456])),
            "123456000000",
            "outside a day",
        ),
        (
            col(Time32MillisecondArray::from(vec![123_456_789])),
            "123456789000",
            "outside a day",
        ),
        (
            col(Time64MicrosecondArray::from(vec![123_456_789_012])),
            "123456789012",
            "outside a day",
        ),
        (
            col(Time64NanosecondArray::from(vec![123_456_789_012_345])),
            "123456789012345",
            "outside a day",
        ),
        (
            col(TimestampMicrosecondArray::from(vec![
                future_day * 86_400_000_000,
            ])),
            "31337",
            "year range",
        ),
        (
            col(DurationMicrosecondArray::from(vec![
                MAX_INTERVAL_MICROS + 123,
            ])),
            "316224000000000123",
            "time component",
        ),
        (
            col(IntervalYearMonthArray::from(vec![234_567])),
            "234567",
            "month component",
        ),
        (
            col(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                4_567_890, 0,
            )])),
            "4567890",
            "day component",
        ),
        (
            col(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(234_567, 0, 0),
            ])),
            "234567",
            "month component",
        ),
        (
            col(Decimal32Array::from(vec![654_321])
                .with_precision_and_scale(3, 0)
                .unwrap()),
            "654321",
            "precision 3",
        ),
        (
            col(Decimal64Array::from(vec![654_321])
                .with_precision_and_scale(3, 0)
                .unwrap()),
            "654321",
            "precision 3",
        ),
        (decimal128(654_321, 3, 0), "654321", "precision 3"),
        (decimal256(654_321, 3, 0), "654321", "precision 3"),
    ];
    for (array, secret, reason) in cases {
        let data_type = array.data_type().clone();
        let message = one_parameter_error(array);
        assert!(
            message.contains("\"value\" at row 0"),
            "{data_type}: {message}"
        );
        assert!(message.contains(reason), "{data_type}: {message}");
        assert!(!message.contains(secret), "{data_type}: {message}");
    }
}

#[test]
fn array_parameters_reject_logical_null_elements() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![Some("visible"), None])),
        )
        .unwrap(),
    );
    let nested: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), dictionary.clone())
            .unwrap(),
    );
    for values in [dictionary, nested, Arc::new(NullArray::new(2))] {
        let list = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1]),
            values.clone(),
            None,
        );
        let batch = RecordBatch::try_from_iter([("items", Arc::new(list) as ArrayRef)]).unwrap();
        if !matches!(values.data_type(), DataType::Null) {
            let parameters = parameters_for_row(&batch, 0, ParameterMode::Named).unwrap();
            assert_eq!(
                value(&parameters[0]).array_values[0].value.as_deref(),
                Some("visible")
            );
        }
        let failure = parameters_for_row(&batch, 1, ParameterMode::Named).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_message(&failure, &["\"items\"", "at row 1", "element 0 is NULL"]);
    }
}

/// A failure inside an array element or a struct field names the parameter's index, the row and
/// the element or field within it, not the child array's position, because a positional binding's
/// field names are arbitrary.
#[test]
fn nested_value_errors_name_the_parameter_index_the_element_and_the_field() {
    let overflowing = Arc::new(UInt64Array::from(vec![1, 2, 3, u64::MAX])) as ArrayRef;
    let nested_list = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        OffsetBuffer::from_lengths([1, 3]),
        Arc::clone(&overflowing),
        None,
    );
    let record = StructArray::new(
        vec![Arc::new(Field::new("amount", DataType::UInt64, true))].into(),
        vec![Arc::new(UInt64Array::from(vec![0, u64::MAX]))],
        None,
    );
    let batch = RecordBatch::try_from_iter([
        ("ok", Arc::new(UInt64Array::from(vec![0, 0])) as ArrayRef),
        ("amounts", Arc::new(nested_list)),
        ("record", Arc::new(record)),
    ])
    .unwrap();

    // Element 2 of the second row, not "row 3" of the flattened child array.
    let failure = parameters_for_row(&batch, 1, ParameterMode::Positional).unwrap_err();
    assert_eq!(
        failure.message,
        "bound BigQuery parameter 1 \"amounts\" at row 1, element 2 is an Arrow UInt64 that \
         exceeds BigQuery INT64; bind it as a STRING or NUMERIC parameter instead"
    );

    let without_list = batch.project(&[0, 2]).unwrap();
    let failure = parameters_for_row(&without_list, 1, ParameterMode::Positional).unwrap_err();
    assert!(
        failure
            .message
            .starts_with("bound BigQuery parameter 1 \"record\" at row 1, field \"amount\" is an"),
        "{failure}"
    );
}

#[test]
fn preserves_empty_non_null_composite_parameter_values() {
    let values = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(Vec::<Option<i64>>::new()),
        None,
    ]));
    let nested = StructArray::new(
        vec![Arc::new(Field::new(
            "values",
            values.data_type().clone(),
            true,
        ))]
        .into(),
        vec![values.clone()],
        None,
    );
    let empty_records = StructArray::new_empty_fields(2, Some(NullBuffer::from(vec![true, false])));
    let batch = RecordBatch::try_from_iter([
        ("values", values as ArrayRef),
        ("nested", Arc::new(nested) as ArrayRef),
        ("empty_record", Arc::new(empty_records) as ArrayRef),
    ])
    .unwrap();

    for (row, expected) in [
        json!([
            {"value": "[]"}, {"structValues": {"values": {"value": "[]"}}}, {"value": "{}"}
        ]),
        json!([{}, {"structValues": {"values": {}}}, {}]),
    ]
    .into_iter()
    .enumerate()
    {
        let parameters = parameters_for_row(&batch, row, ParameterMode::Named).unwrap();
        assert_eq!(
            json!(parameters.iter().map(value).collect::<Vec<_>>()),
            expected,
            "row {row}"
        );
    }
}
