use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::{Float16Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
    StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
    UInt64Array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer, i256};
use arrow_schema::{DataType, TimeUnit};

use super::{ArrowPrimitiveType, MILLISECONDS_PER_DAY, cast_column};
use crate::test_support::arrow::typed;

/// `half::f16`, reached through arrow rather than through a dev-dependency on `half`.
type F16 = <Float16Type as ArrowPrimitiveType>::Native;

/// Casts `column` the way ingest does, panicking with the driver message on failure.
#[expect(
    clippy::needless_pass_by_value,
    reason = "taking ownership keeps the many `Arc::new` call sites brief"
)]
fn cast(column: ArrayRef, data_type: &DataType) -> ArrayRef {
    cast_column(&column, data_type, "field").unwrap()
}

/// Returns the driver message for a cast ingest must reject.
#[expect(
    clippy::needless_pass_by_value,
    reason = "taking ownership keeps the many `Arc::new` call sites brief"
)]
fn cast_error(column: ArrayRef, data_type: &DataType) -> String {
    let error = cast_column(&column, data_type, "field").unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    error.message
}

#[test]
fn rejected_cast_values_do_not_appear_in_errors() {
    let decimal = || {
        Arc::new(
            Decimal128Array::from(vec![654_321])
                .with_precision_and_scale(6, 0)
                .unwrap(),
        ) as ArrayRef
    };
    let cases = [
        (
            decimal(),
            DataType::Decimal128(3, 0),
            "654321",
            "precision 3",
        ),
        (
            decimal(),
            DataType::Decimal256(3, 0),
            "654321",
            "precision 3",
        ),
        (
            Arc::new(Date64Array::from(vec![86_411_223])) as ArrayRef,
            DataType::Date32,
            "86411223",
            "not aligned",
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![i64::MAX])) as ArrayRef,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "9223372036854775807",
            "out of range",
        ),
    ];
    for (column, target, secret, reason) in cases {
        let message = cast_error(column, &target);
        assert!(message.contains("\"field\""), "{message}");
        assert!(message.contains(reason), "{message}");
        assert!(!message.contains(secret), "{message}");
    }
}

#[test]
fn integers_whose_complete_domain_fits_widen_to_int64() {
    let widened = cast(
        Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
        &DataType::Int64,
    );
    assert_eq!(
        widened.as_ref(),
        &Int64Array::from(vec![Some(-128), None, Some(127)]) as &dyn Array
    );

    let widened = cast(
        Arc::new(UInt32Array::from(vec![Some(0), None, Some(u32::MAX)])),
        &DataType::Int64,
    );
    assert_eq!(
        widened.as_ref(),
        &Int64Array::from(vec![Some(0), None, Some(i64::from(u32::MAX))]) as &dyn Array
    );

    let message = cast_error(Arc::new(UInt64Array::from(vec![0])), &DataType::Int64);
    assert_eq!(
        message,
        "failed to normalize BigQuery ingest column \"field\" from UInt64 to Int64: \
         the cast is not supported"
    );
}

#[test]
fn floats_widen_to_float64() {
    let widened = cast(
        Arc::new(Float16Array::from(vec![
            Some(F16::from_f32(0.5)),
            None,
            Some(F16::MAX),
        ])),
        &DataType::Float64,
    );
    assert_eq!(
        widened.as_ref(),
        &Float64Array::from(vec![Some(0.5), None, Some(65504.0)]) as &dyn Array
    );

    let widened = cast(
        Arc::new(Float32Array::from(vec![0.25_f32])),
        &DataType::Float64,
    );
    assert_eq!(
        widened.as_ref(),
        &Float64Array::from(vec![0.25]) as &dyn Array
    );
}

#[test]
fn large_byte_offset_narrowing_preserves_slices_at_vector_boundaries() {
    let strings = LargeStringArray::from_iter((0..65_560).map(|row| match row % 4 {
        0 => Some("large offset benchmark"),
        1 => None,
        2 => Some(""),
        _ => Some("non-ASCII: é中"),
    }));
    let (offsets, values, nulls) = strings.clone().into_parts();
    let binaries = LargeBinaryArray::new(offsets, values, nulls);
    for start in [0, 1, 2, 3, 7, 8, 15] {
        for length in [0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 65_536] {
            let source = strings.slice(start, length);
            let narrowed = cast(Arc::new(source.clone()), &DataType::Utf8);
            let narrowed = typed::<StringArray>(&narrowed);
            assert!(narrowed.iter().eq(source.iter()));
            assert_eq!(narrowed.nulls(), source.nulls());
            assert_eq!(
                narrowed.values().as_ptr(),
                source.values().as_ptr().wrapping_add(
                    usize::try_from(*source.value_offsets().first().unwrap()).unwrap()
                )
            );

            let source = binaries.slice(start, length);
            let narrowed = cast(Arc::new(source.clone()), &DataType::Binary);
            let narrowed = typed::<BinaryArray>(&narrowed);
            assert!(narrowed.iter().eq(source.iter()));
            assert_eq!(narrowed.nulls(), source.nulls());
            assert_eq!(
                narrowed.values().as_ptr(),
                source.values().as_ptr().wrapping_add(
                    usize::try_from(*source.value_offsets().first().unwrap()).unwrap()
                )
            );
        }
    }
}

#[test]
fn views_and_fixed_size_binaries_materialize() {
    let view = StringViewArray::from(vec![
        Some("short"),
        None,
        Some("a string too long for an inline view"),
    ]);
    let materialized = cast(Arc::new(view), &DataType::Utf8);
    assert_eq!(
        materialized.as_ref(),
        &StringArray::from(vec![
            Some("short"),
            None,
            Some("a string too long for an inline view"),
        ]) as &dyn Array
    );

    let view = BinaryViewArray::from(vec![Some(b"ab".as_slice()), None]);
    let materialized = cast(Arc::new(view), &DataType::Binary);
    assert_eq!(
        materialized.as_ref(),
        &BinaryArray::from_opt_vec(vec![Some(b"ab".as_slice()), None]) as &dyn Array
    );

    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some(b"ab"), None, Some(b"cd"), Some(b"ef")].into_iter(),
        2,
    )
    .unwrap();
    let expanded = cast(Arc::new(fixed.slice(1, 3)), &DataType::Binary);
    assert!(expanded.is_null(0));
    let expanded = typed::<BinaryArray>(&expanded);
    assert_eq!(expanded.len(), 3);
    assert_eq!(expanded.value(1), b"cd");
    assert_eq!(expanded.value(2), b"ef");
}

#[test]
fn repeated_views_reject_offset_overflow_before_allocating() {
    let source = BinaryViewArray::from_iter_values([vec![b'x'; 65_536]]);
    let repeated = BinaryViewArray::new(
        vec![source.views()[0]; 32_768].into(),
        source.data_buffers().to_vec(),
        None,
    );
    let message = cast_error(Arc::new(repeated), &DataType::Binary);
    assert!(message.ends_with("view values do not fit 32-bit offsets"));
}

#[test]
fn view_materialization_does_not_reserve_null_payloads() {
    let source = StringViewArray::from_iter_values(["x".repeat(65_536)]);
    let mut validity = vec![false; 1_024];
    validity[0] = true;
    let repeated = StringViewArray::new(
        vec![source.views()[0]; validity.len()].into(),
        source.data_buffers().to_vec(),
        Some(NullBuffer::from(validity)),
    );
    let materialized = cast(Arc::new(repeated), &DataType::Utf8);
    assert_eq!(materialized.null_count(), 1_023);
    assert_eq!(typed::<StringArray>(&materialized).value(0).len(), 65_536);
    assert!(materialized.get_buffer_memory_size() < 131_072);
}

#[test]
fn date64_requires_exact_days_and_rejects_out_of_range_days() {
    let dates = cast(
        Arc::new(Date64Array::from(vec![
            Some(0),
            None,
            Some(86_400_000),
            Some(-86_400_000),
        ])),
        &DataType::Date32,
    );
    assert_eq!(
        dates.as_ref(),
        &Date32Array::from(vec![Some(0), None, Some(1), Some(-1)]) as &dyn Array
    );

    for value in [86_400_001, -1] {
        let message = cast_error(Arc::new(Date64Array::from(vec![value])), &DataType::Date32);
        assert!(
            message.ends_with("value is not aligned to an Arrow Date64 day"),
            "unexpected message: {message}"
        );
    }

    let message = cast_error(
        Arc::new(Date64Array::from(vec![
            (i64::from(i32::MAX) + 1) * MILLISECONDS_PER_DAY,
        ])),
        &DataType::Date32,
    );
    assert!(
        message.ends_with("value is out of range"),
        "unexpected message: {message}"
    );
}

#[test]
fn decimals_widen_to_a_wider_physical_type_and_a_larger_scale() {
    let decimal128 = Decimal128Array::from(vec![Some(123), None])
        .with_precision_and_scale(38, 20)
        .unwrap();
    let decimal256 = cast(Arc::new(decimal128), &DataType::Decimal256(38, 20));
    let decimal256 = typed::<Decimal256Array>(&decimal256);
    assert_eq!(decimal256.value(0), i256::from_i128(123));
    assert!(decimal256.is_null(1));

    let negative_scale = Decimal128Array::from(vec![-12])
        .with_precision_and_scale(2, -3)
        .unwrap();
    let normalized = cast(Arc::new(negative_scale), &DataType::Decimal128(5, 0));
    assert_eq!(typed::<Decimal128Array>(&normalized).value(0), -12_000);
}

/// A null slot's payload is arbitrary after an FFI import or a `MutableArrayData` gather.
/// Widening a decimal's precision at an unchanged scale keeps the buffers, so its validation must
/// skip null slots like the rescaling kernels do.
#[test]
fn decimal_relabelling_ignores_the_payload_under_a_null_slot() {
    let with_garbage_null = Decimal128Array::new(
        vec![123_i128, i128::MAX].into(),
        Some(NullBuffer::from(vec![true, false])),
    )
    .with_precision_and_scale(10, 2)
    .unwrap();
    let widened = cast(Arc::new(with_garbage_null), &DataType::Decimal128(38, 2));
    let widened = typed::<Decimal128Array>(&widened);
    assert_eq!(widened.value(0), 123);
    assert!(widened.is_null(1));

    let with_garbage_null = Decimal256Array::new(
        vec![i256::from_i128(123), i256::MAX].into(),
        Some(NullBuffer::from(vec![true, false])),
    )
    .with_precision_and_scale(10, 2)
    .unwrap();
    let widened = cast(Arc::new(with_garbage_null), &DataType::Decimal256(38, 2));
    let widened = typed::<Decimal256Array>(&widened);
    assert_eq!(widened.value(0), i256::from_i128(123));
    assert!(widened.is_null(1));
}

/// A parameter-free BIGNUMERIC's domain is the whole `i256`, one digit past
/// `Decimal256(76, 38)`, which no precision `validate_decimal_precision` accepts can express. Both
/// the relabelling and the rescaling kernels skip the per-value check for that shape, so the
/// driver never rejects a value BigQuery stores.
#[test]
fn bignumeric_casts_span_its_full_physical_domain() {
    let relabelled = cast(
        Arc::new(
            Decimal256Array::from(vec![i256::MIN, i256::MAX])
                .with_precision_and_scale(38, 38)
                .unwrap(),
        ),
        &DataType::Decimal256(76, 38),
    );
    let relabelled = typed::<Decimal256Array>(&relabelled);
    assert_eq!(relabelled.value(0), i256::MIN);
    assert_eq!(relabelled.value(1), i256::MAX);

    // Rescaling reaches the same domain: 10^38 times `i128::MAX` overruns precision 76 and
    // still fits an `i256`.
    let rescaled = cast(
        Arc::new(
            Decimal128Array::from(vec![i128::MAX])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
        &DataType::Decimal256(76, 38),
    );
    assert_eq!(
        typed::<Decimal256Array>(&rescaled).value(0),
        i256::from_i128(i128::MAX) * i256::from_string(&format!("1{}", "0".repeat(38))).unwrap()
    );
}

/// These casts only widen: dropping a fractional digit or an integer one could change the value,
/// so a pair that needs to is refused even when the values would survive.
#[test]
fn a_decimal_cast_that_would_narrow_is_refused() {
    let at_scale_two = Decimal128Array::from(vec![120])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let message = cast_error(Arc::new(at_scale_two), &DataType::Decimal128(10, 1));
    assert_eq!(
        message,
        "failed to normalize BigQuery ingest column \"field\" from Decimal128(10, 2) to \
         Decimal128(10, 1): the cast is not supported"
    );

    let wide = || {
        Decimal256Array::from(vec![i256::from_i128(1)])
            .with_precision_and_scale(10, 9)
            .unwrap()
    };
    for target in [DataType::Decimal256(10, 0), DataType::Decimal128(10, 9)] {
        let message = cast_error(Arc::new(wide()), &target);
        assert!(message.ends_with("the cast is not supported"), "{message}");
    }
}

#[test]
fn times_rescale_to_microseconds() {
    let micros = DataType::Time64(TimeUnit::Microsecond);

    let times = cast(
        Arc::new(Time32SecondArray::from(vec![Some(0), None, Some(86_399)])),
        &micros,
    );
    assert_eq!(
        times.as_ref(),
        &Time64MicrosecondArray::from(vec![Some(0), None, Some(86_399_000_000)]) as &dyn Array
    );

    let times = cast(
        Arc::new(Time32MillisecondArray::from(vec![Some(1), None])),
        &micros,
    );
    assert_eq!(
        times.as_ref(),
        &Time64MicrosecondArray::from(vec![Some(1_000), None]) as &dyn Array
    );

    let times = cast(
        Arc::new(Time64NanosecondArray::from(vec![Some(1_999), None])),
        &micros,
    );
    assert_eq!(
        times.as_ref(),
        &Time64MicrosecondArray::from(vec![Some(1), None]) as &dyn Array
    );
}

#[test]
fn timestamps_rescale_and_relabel_without_shifting_the_instant() {
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);

    let stamps = cast(
        Arc::new(TimestampSecondArray::from(vec![Some(1), None, Some(-1)])),
        &naive,
    );
    assert_eq!(
        stamps.as_ref(),
        &TimestampMicrosecondArray::from(vec![Some(1_000_000), None, Some(-1_000_000)])
            as &dyn Array
    );

    let stamps = cast(
        Arc::new(TimestampMillisecondArray::from(vec![Some(1)])),
        &naive,
    );
    assert_eq!(
        stamps.as_ref(),
        &TimestampMicrosecondArray::from(vec![1_000]) as &dyn Array
    );

    let stamps = cast(
        Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_999),
            None,
            Some(-1_999),
        ])),
        &naive,
    );
    assert_eq!(
        stamps.as_ref(),
        // An instant rounds down, so a pre-1970 one does not move into a later microsecond.
        &TimestampMicrosecondArray::from(vec![Some(1), None, Some(-2)]) as &dyn Array
    );

    // Relabelling a zone keeps the epoch value: only the metadata changes.
    let stamps = cast(
        Arc::new(
            TimestampMicrosecondArray::from(vec![7]).with_timezone("America/New_York".to_string()),
        ),
        &utc,
    );
    assert_eq!(stamps.data_type(), &utc);
    assert_eq!(
        stamps.as_ref(),
        &TimestampMicrosecondArray::from(vec![7]).with_timezone("UTC".to_string()) as &dyn Array
    );

    // An empty zone is dropped rather than relabelled, and must not shift either.
    let stamps = cast(
        Arc::new(TimestampMicrosecondArray::from(vec![7]).with_timezone(String::new())),
        &naive,
    );
    assert_eq!(stamps.data_type(), &naive);
    assert_eq!(
        stamps.as_ref(),
        &TimestampMicrosecondArray::from(vec![7]) as &dyn Array
    );

    let message = cast_error(Arc::new(TimestampSecondArray::from(vec![i64::MIN])), &naive);
    assert!(
        message.ends_with("value is out of range"),
        "unexpected message: {message}"
    );
}

#[test]
fn narrower_intervals_become_month_day_nano_intervals() {
    let intervals = cast(
        Arc::new(IntervalYearMonthArray::from(vec![
            Some(14),
            None,
            Some(-25),
        ])),
        &DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
    );
    assert_eq!(
        intervals.as_ref(),
        &IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(14, 0, 0)),
            None,
            Some(IntervalMonthDayNano::new(-25, 0, 0)),
        ]) as &dyn Array
    );

    let intervals = cast(
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(2, -3)),
            None,
        ])),
        &DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
    );
    assert_eq!(
        intervals.as_ref(),
        &IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 2, -3_000_000)),
            None,
        ]) as &dyn Array
    );
}

/// The values themselves are cast after expansion, so a dictionary of a narrower type still lands
/// on the normalized BigQuery type.
#[test]
fn dictionary_values_are_cast_after_expansion() {
    let dictionary = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), None]),
        Arc::new(Int8Array::from(vec![7])),
    );
    let expanded = cast(Arc::new(dictionary), &DataType::Int64);
    assert_eq!(
        expanded.as_ref(),
        &Int64Array::from(vec![Some(7), None]) as &dyn Array
    );
}

#[test]
fn dictionary_expansion_matches_arrow_select_take() {
    use arrow_array::types::{Int8Type, Int64Type, UInt64Type};
    use arrow_array::{Int8Array, ListArray};
    use arrow_select::take::take;

    // One representative per value layout the type-erased gather must copy correctly:
    // variable-width bytes, primitives, decimals with parameters, zoned timestamps, lists.
    let value_arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("a"), None, Some("bcd")])),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(i64::MAX)])),
        Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"ab".as_slice()),
            None,
            Some(b"c"),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(1), None, Some(-2)])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        ),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1), None, Some(-1)])
                .with_timezone("UTC".to_string()),
        ),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![]),
        ])),
    ];
    let keys = Int32Array::from(vec![
        Some(2),
        None,
        Some(0),
        Some(1),
        Some(2),
        None,
        None,
        Some(2),
        Some(0),
    ]);
    for values in value_arrays {
        let dictionary = DictionaryArray::new(keys.clone(), values.clone());
        let expanded = cast(Arc::new(dictionary.clone()), values.data_type());
        let expected = take(values.as_ref(), &keys, None).unwrap();
        assert_eq!(
            expanded.as_ref(),
            expected.as_ref(),
            "{}",
            values.data_type()
        );

        // A sliced dictionary gathers through its logical key window.
        let sliced = dictionary.slice(1, 3);
        let expanded = cast(Arc::new(sliced), values.data_type());
        let expected = take(values.as_ref(), &keys.slice(1, 3), None).unwrap();
        assert_eq!(
            expanded.as_ref(),
            expected.as_ref(),
            "sliced {}",
            values.data_type()
        );
    }

    // The other key widths the driver accepts, including the widest unsigned one.
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let keys = Int8Array::from(vec![Some(1), None, Some(0)]);
    let dictionary = DictionaryArray::<Int8Type>::new(keys.clone(), values.clone());
    let expanded = cast(Arc::new(dictionary), &DataType::Utf8);
    let expected = take(values.as_ref(), &keys, None).unwrap();
    assert_eq!(expanded.as_ref(), expected.as_ref());

    let keys = UInt64Array::from(vec![Some(1), None, Some(0)]);
    let dictionary = DictionaryArray::<UInt64Type>::new(keys.clone(), values.clone());
    let expanded = cast(Arc::new(dictionary), &DataType::Utf8);
    let expected = take(values.as_ref(), &keys, None).unwrap();
    assert_eq!(expanded.as_ref(), expected.as_ref());
}

#[test]
fn contiguous_dictionary_keys_reuse_the_values_buffers() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("outside"),
        Some("first"),
        None,
        Some("last"),
        Some("outside"),
    ]));
    let dictionary =
        DictionaryArray::<Int32Type>::new(Int32Array::from(vec![4, 1, 2, 3, 0]), values.clone());
    let expanded = cast(Arc::new(dictionary.slice(1, 3)), &DataType::Utf8);
    let expected = values.slice(1, 3);
    assert_eq!(expanded.as_ref(), expected.as_ref());
    assert_eq!(
        typed::<StringArray>(&expanded).value_data().as_ptr(),
        typed::<StringArray>(&values).value_data().as_ptr(),
    );

    let empty = super::expand_dictionary(&values, &Int32Array::from(Vec::<i32>::new())).unwrap();
    assert_eq!(empty.as_ref(), values.slice(0, 0).as_ref());
}

#[test]
fn noncontiguous_dictionary_keys_match_arrow_take() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
    for keys in [
        vec![Some(0), Some(1), Some(2), Some(0)],
        vec![Some(0), Some(1), Some(1)],
        vec![Some(0), None, Some(2)],
    ] {
        let keys = Int32Array::from(keys);
        let expanded = super::expand_dictionary(&values, &keys).unwrap();
        let expected = arrow_select::take::take(values.as_ref(), &keys, None).unwrap();
        assert_eq!(expanded.as_ref(), expected.as_ref());
    }
}

#[test]
fn nullable_dictionary_runs_match_arrow_take_across_slices() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("zero"),
        None,
        Some("two"),
        Some("three"),
        Some("four"),
    ]));
    for valid in [
        vec![false; 160],
        (0..160).map(|row| row % 32 < 28).collect(),
        (0..160).map(|row| row % 2 == 0).collect(),
    ] {
        let keys = Int32Array::new(
            valid
                .iter()
                .enumerate()
                .map(|(row, &valid)| {
                    if valid {
                        i32::try_from(row % 5).unwrap()
                    } else {
                        -1
                    }
                })
                .collect::<Vec<_>>()
                .into(),
            Some(NullBuffer::from(valid)),
        );
        for start in [0, 1, 7, 8, 31, 63, 64] {
            for length in [0, 1, 3, 32, 65] {
                let keys = keys.slice(start, length);
                let expanded = super::expand_dictionary(&values, &keys).unwrap();
                let expected = arrow_select::take::take(values.as_ref(), &keys, None).unwrap();
                assert_eq!(expanded.as_ref(), expected.as_ref(), "{start}/{length}");
            }
        }
    }
}

#[test]
fn dictionary_with_out_of_bounds_keys_is_rejected_not_expanded() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    for key in [5, -1] {
        // Invalid live keys cannot be hidden by an earlier null run or a coalesced valid run.
        for keys in [vec![Some(key)], vec![None, Some(0), Some(key), None]] {
            let message = super::expand_dictionary(&values, &Int32Array::from(keys)).unwrap_err();
            assert_eq!(
                message,
                format!("dictionary key {key} is out of bounds for 1 dictionary values")
            );
        }
    }
}
