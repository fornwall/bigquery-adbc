use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal256Type, Int64Type, IntervalMonthDayNanoType, Time64MicrosecondType,
    TimestampMicrosecondType,
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Int64Array,
    IntervalMonthDayNanoArray, LargeListArray, LargeListViewArray, ListArray, RecordBatch,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_buffer::{IntervalMonthDayNano, OffsetBuffer, i256};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};

use super::normalize_batch;
use crate::test_support::errors::assert_failure;

fn one_field(name: &str, data_type: ArrowDataType, nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![ArrowField::new(
        name, data_type, nullable,
    )]))
}

#[test]
fn canonical_validated_batch_preserves_schema_and_column_identity() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Time64MicrosecondArray::from(vec![3_723_000_004])),
        Arc::new(
            Decimal128Array::from(vec![12_345_i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3_000),
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ])),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                ArrowField::new(format!("column_{index}"), column.data_type().clone(), false)
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns.clone()).unwrap();

    let normalized = normalize_batch(batch, schema.clone()).unwrap();

    assert!(Arc::ptr_eq(normalized.schema_ref(), &schema));
    for (normalized, original) in normalized.columns().iter().zip(&columns) {
        assert!(Arc::ptr_eq(normalized, original));
    }
}

#[test]
fn changed_column_rebuilds_batch_and_preserves_unchanged_columns() {
    let time = Arc::new(Time64MicrosecondArray::from(vec![3_723_000_004])) as ArrayRef;
    let interval = Arc::new(IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 3_999),
    ])) as ArrayRef;
    let list = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
    ])) as ArrayRef;
    let columns = vec![time.clone(), interval.clone(), list.clone()];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                ArrowField::new(format!("column_{index}"), column.data_type().clone(), false)
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let normalized = normalize_batch(batch, schema).unwrap();

    assert!(Arc::ptr_eq(normalized.column(0), &time));
    assert!(!Arc::ptr_eq(normalized.column(1), &interval));
    assert!(Arc::ptr_eq(normalized.column(2), &list));
    assert_eq!(
        normalized
            .column(1)
            .as_primitive::<IntervalMonthDayNanoType>()
            .value(0),
        IntervalMonthDayNano::new(1, 2, 3_000)
    );
}

#[test]
fn changed_target_schema_is_attached_to_unchanged_columns() {
    let column = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
    let input_schema = one_field("value", ArrowDataType::Int64, false);
    let target_schema = Arc::new(Schema::new(vec![
        ArrowField::new("value", ArrowDataType::Int64, false).with_metadata(HashMap::from([(
            "normalized".to_owned(),
            "true".to_owned(),
        )])),
    ]));
    let batch = RecordBatch::try_new(input_schema, vec![column.clone()]).unwrap();

    let normalized = normalize_batch(batch, target_schema.clone()).unwrap();

    assert!(Arc::ptr_eq(normalized.schema_ref(), &target_schema));
    assert!(Arc::ptr_eq(normalized.column(0), &column));
    assert_eq!(
        normalized
            .schema()
            .field(0)
            .metadata()
            .get("normalized")
            .map(String::as_str),
        Some("true")
    );
}

fn single_column_batch(name: &str, column: ArrayRef) -> (RecordBatch, SchemaRef) {
    let schema = one_field(name, column.data_type().clone(), false);
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    (batch, schema)
}

#[test]
fn bignumeric_ingest_uses_its_physical_domain_only_for_its_canonical_shape() {
    let one_e38 = i256::from_string(&format!("1{}", "0".repeat(76))).unwrap();
    let column = Arc::new(
        Decimal256Array::from(vec![i256::MIN, one_e38, i256::MAX])
            .with_precision_and_scale(76, 38)
            .unwrap(),
    ) as ArrayRef;
    let (batch, schema) = single_column_batch("value", column);
    let normalized = normalize_batch(batch, schema).unwrap();
    let values = normalized.column(0).as_primitive::<Decimal256Type>();
    assert_eq!(values.values(), &[i256::MIN, one_e38, i256::MAX]);

    // A non-BIGNUMERIC Decimal256 retains its declared precision even though its physical
    // integer can hold the value.
    let outside_precision = i256::from_string(&format!("1{}", "0".repeat(75))).unwrap();
    let column = Arc::new(
        Decimal256Array::from(vec![outside_precision])
            .with_precision_and_scale(75, 38)
            .unwrap(),
    ) as ArrayRef;
    let (batch, schema) = single_column_batch("ordinary", column);
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_failure(&error, Status::InvalidData, &["declared precision"]);
}

/// A `Date32` or a microsecond `Timestamp` that already matches the bound schema is still checked
/// against BigQuery's calendar before it reaches Storage Write, so an out-of-range DATE fails
/// before it can create or replace the target table.
#[test]
fn rejects_dates_and_timestamps_outside_the_bigquery_calendar() {
    for (name, column) in [
        (
            "date_after_9999",
            Arc::new(Date32Array::from(vec![3_000_000])) as ArrayRef,
        ),
        (
            "date_before_0001",
            Arc::new(Date32Array::from(vec![-800_000])) as ArrayRef,
        ),
        (
            "timestamp_after_9999",
            Arc::new(
                TimestampMicrosecondArray::from(vec![999_999_999_999_999_999]).with_timezone_utc(),
            ) as ArrayRef,
        ),
        (
            "datetime_before_0001",
            Arc::new(TimestampMicrosecondArray::from(vec![
                -70_000_000_000_000_000,
            ])) as ArrayRef,
        ),
    ] {
        let (batch, schema) = single_column_batch(name, column);
        let error = normalize_batch(batch, schema).unwrap_err();
        assert_failure(&error, Status::InvalidData, &[name, "row 0"]);
    }

    // 0001-01-01 and 9999-12-31 are inside the range, at both ends and in both types.
    let last_micro_of_9999 = 2_932_896 * 86_400_000_000 + 86_399_999_999;
    for (name, column) in [
        (
            "dates",
            Arc::new(Date32Array::from(vec![-719_162, 2_932_896])) as ArrayRef,
        ),
        (
            "timestamps",
            Arc::new(TimestampMicrosecondArray::from(vec![
                -719_162 * 86_400_000_000,
                last_micro_of_9999,
            ])) as ArrayRef,
        ),
    ] {
        let (batch, schema) = single_column_batch(name, column);
        normalize_batch(batch, schema).unwrap();
    }
}

/// Nanosecond instants narrow to microseconds by rounding down, as bound parameters do.
#[test]
fn negative_nanosecond_timestamps_round_down() {
    let column = Arc::new(TimestampNanosecondArray::from(vec![
        -1_999,
        -86_400_000_000_999,
        1_999,
    ])) as ArrayRef;
    let (batch, _) = single_column_batch("instant", column);
    let schema = one_field(
        "instant",
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    );

    let normalized = normalize_batch(batch, schema).unwrap();
    let instants = normalized
        .column(0)
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(instants.values(), &[-2, -86_400_000_001, 1]);
}

/// BigQuery arrays cannot contain NULL, and an ingest array is described to the service only
/// as a `REPEATED` field, so the driver has to reject the element itself.
#[test]
fn rejects_null_elements_inside_ingested_arrays() {
    let canonical = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3), None]),
    ])) as ArrayRef;
    let (batch, schema) = single_column_batch("values", canonical);
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_failure(&error, Status::InvalidData, &["values", "row 1"]);

    // The same holds for a layout that has to be rebuilt rather than passed through.
    let large = Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>(
        vec![Some(vec![Some(1), None])],
    )) as ArrayRef;
    let (batch, _) = single_column_batch("values", large);
    let schema = one_field(
        "values",
        ArrowDataType::new_list(ArrowDataType::Int64, true),
        false,
    );
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_failure(&error, Status::InvalidData, &["row 0"]);

    let view = Arc::new(LargeListViewArray::new(
        Arc::new(ArrowField::new("item", ArrowDataType::Int64, true)),
        vec![1_i64].into(),
        vec![2_i64].into(),
        Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
        None,
    )) as ArrayRef;
    let (batch, _) = single_column_batch("values", view);
    let schema = one_field(
        "values",
        ArrowDataType::new_list(ArrowDataType::Int64, true),
        false,
    );
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_failure(&error, Status::InvalidData, &["row 0"]);
}

#[test]
fn mismatched_batch_column_counts_are_rejected() {
    let schema = |count| {
        Arc::new(Schema::new(
            (0..count)
                .map(|index| ArrowField::new(format!("field_{index}"), ArrowDataType::Int64, false))
                .collect::<Vec<_>>(),
        ))
    };
    for (source_count, target_count) in [(1, 2), (2, 1)] {
        let batch = RecordBatch::try_new(
            schema(source_count),
            (0..source_count)
                .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        let failure = normalize_batch(batch, schema(target_count)).unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidData,
            &["failed to normalize BigQuery ingest batch"],
        );
    }
}

#[test]
fn mismatched_struct_child_counts_are_rejected() {
    let fields = |count| {
        (0..count)
            .map(|index| ArrowField::new(format!("field_{index}"), ArrowDataType::Int64, false))
            .collect::<Vec<_>>()
            .into()
    };
    // Too few children must not panic; too many must not silently discard input fields.
    for (source_count, target_count) in [(1, 2), (2, 1)] {
        let record = Arc::new(StructArray::new(
            fields(source_count),
            (0..source_count)
                .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
                .collect(),
            None,
        )) as ArrayRef;
        let (batch, _) = single_column_batch("record", record);
        let schema = one_field("record", ArrowDataType::Struct(fields(target_count)), false);
        let error = normalize_batch(batch, schema).unwrap_err();
        assert_failure(
            &error,
            Status::InvalidData,
            &[
                "BigQuery ingest struct field \"record\" is invalid: ",
                "the bound column needs one child array per struct field",
            ],
        );
    }
}

/// Every value-domain rejection names the offending row, not just the column: a caller binding
/// a batch of thousands of rows cannot act on "this column has a bad value somewhere".
#[test]
fn value_domain_rejections_name_the_offending_row() {
    let (batch, schema) = single_column_batch(
        "clock",
        Arc::new(Time64MicrosecondArray::from(vec![0, 1, 86_400_000_000])),
    );
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_eq!(
        error.message,
        "Arrow time ingest field \"clock\" contains an out-of-range value at row 2; \
         a time must be within one day"
    );

    let (batch, schema) = single_column_batch(
        "span",
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(0, 0, 0),
            IntervalMonthDayNano::new(120_001, 0, 0),
        ])),
    );
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_eq!(
        error.message,
        "Arrow interval ingest field \"span\" has a month component at row 1, outside \
         BigQuery's supported range"
    );

    let (batch, schema) = single_column_batch(
        "amount",
        Arc::new(
            Decimal128Array::from(vec![1_i128, 100_000_i128])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
    );
    let error = normalize_batch(batch, schema).unwrap_err();
    assert!(
        error.message.starts_with(
            "Arrow decimal ingest field \"amount\" contains a value outside its declared \
             precision 5 at row 1"
        ),
        "{}",
        error.message
    );
}

/// A `Date64` outside the BigQuery calendar is rejected before the cast that would let it
/// through, so nothing reaches Storage Write after a create or replace has mutated the target.
#[test]
fn rejects_date64_values_outside_the_bigquery_calendar() {
    for value in [253_402_300_800_000_i64, -62_167_219_200_000 - 86_400_000] {
        let (batch, _) = single_column_batch("day", Arc::new(Date64Array::from(vec![value])));
        let schema = one_field("day", ArrowDataType::Date32, false);
        let error = normalize_batch(batch, schema).unwrap_err();
        assert_eq!(error.status, Status::InvalidData, "{value}");
        assert_eq!(
            error.message,
            "Arrow date ingest field \"day\" contains an out-of-range value at row 0; \
             BigQuery supports years 1 to 9999"
        );
    }

    // Both ends of the calendar still normalize to their `Date32` day counts.
    let (batch, _) = single_column_batch(
        "day",
        Arc::new(Date64Array::from(vec![
            -62_135_596_800_000,
            253_402_214_400_000,
        ])),
    );
    let schema = one_field("day", ArrowDataType::Date32, false);
    let normalized = normalize_batch(batch, schema).unwrap();
    assert_eq!(
        normalized.column(0).as_primitive::<Date32Type>().values(),
        &[-719_162, 2_932_896]
    );
}

/// A sliced list is judged by the child values its own offsets address, not by the whole
/// child `values()` still hands back.
#[test]
fn a_sliced_list_is_judged_only_by_the_values_its_offsets_address() {
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![None]),
        Some(vec![Some(1), Some(2)]),
        Some(vec![None]),
    ]);
    let sliced = Arc::new(list.slice(1, 1)) as ArrayRef;
    let (batch, schema) = single_column_batch("values", sliced);
    let normalized = normalize_batch(batch, schema).unwrap();
    let values = normalized.column(0).as_list::<i32>();
    assert_eq!(values.len(), 1);
    assert_eq!(
        values.value(0).as_primitive::<Int64Type>().values(),
        &[1, 2]
    );

    // A date the window does not address must not be range-checked either.
    let dates = Date32Array::from(vec![3_000_000, 0, 1]);
    let item = Arc::new(ArrowField::new("item", ArrowDataType::Date32, true));
    let list = ListArray::new(
        item,
        OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0, 1, 3])),
        Arc::new(dates) as ArrayRef,
        None,
    );
    let sliced = Arc::new(list.slice(1, 1)) as ArrayRef;
    let (batch, schema) = single_column_batch("days", sliced.clone());
    let normalized = normalize_batch(batch, schema).unwrap();
    assert!(Arc::ptr_eq(normalized.column(0), &sliced));

    // A NULL element inside the window is still rejected, at its slice-relative row.
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2)]),
        Some(vec![Some(3), None]),
    ]);
    let sliced = Arc::new(list.slice(1, 2)) as ArrayRef;
    let (batch, schema) = single_column_batch("values", sliced);
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert_eq!(
        error.message,
        "BigQuery ingest list field \"values\" contains a NULL array element at row 1; a \
         BigQuery array cannot contain NULL"
    );
}

#[test]
fn nested_sliced_list_validation_preserves_batch_row_locations() {
    // Empty entries repeat offsets at both levels. Only the final date is invalid within the
    // selected outer window; the earlier invalid date belongs to a different batch window.
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![3_000_000, 0, 3_000_000]));
    let inner: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", ArrowDataType::Date32, false)),
        OffsetBuffer::new(vec![0, 1, 1, 2, 3].into()),
        dates,
        None,
    ));
    let records: ArrayRef = Arc::new(StructArray::new(
        vec![ArrowField::new("days", inner.data_type().clone(), false)].into(),
        vec![inner],
        None,
    ));
    let outer = ListArray::new(
        Arc::new(ArrowField::new("item", records.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1, 1, 3, 4].into()),
        records,
        None,
    );
    let (batch, schema) = single_column_batch("records", Arc::new(outer.slice(1, 3)));
    let error = super::normalize_rows(batch, schema, 100).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert_eq!(
        error.message,
        "Arrow date ingest field \"days\" contains an out-of-range value at row 102; \
         BigQuery supports years 1 to 9999"
    );
}

/// Values only a NULL list entry addresses are gathered away before validation, in the
/// canonical layout as in the non-canonical ones.
#[test]
fn values_behind_a_null_list_entry_are_never_validated() {
    let item = Arc::new(ArrowField::new("item", ArrowDataType::Date32, true));
    // The second date is year 10186, outside the BigQuery calendar, and addressed only by the
    // NULL entry.
    let dates = Arc::new(Date32Array::from(vec![0, 3_000_000])) as ArrayRef;
    let entries = arrow_buffer::NullBuffer::from(vec![true, false]);
    let canonical = Arc::new(ListArray::new(
        item.clone(),
        OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0_i32, 1, 2])),
        dates.clone(),
        Some(entries.clone()),
    )) as ArrayRef;
    let large = Arc::new(LargeListArray::new(
        item.clone(),
        OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0_i64, 1, 2])),
        dates,
        Some(entries),
    )) as ArrayRef;

    let schema: SchemaRef = one_field("days", ArrowDataType::List(item), true);
    let normalize = |column: ArrayRef| {
        let source = one_field("days", column.data_type().clone(), true);
        let batch = RecordBatch::try_new(source, vec![column]).unwrap();
        let normalized = normalize_batch(batch, schema.clone()).unwrap();
        normalized.column(0).clone()
    };
    let canonical = normalize(canonical);
    let large = normalize(large);
    assert_eq!(&canonical, &large, "the two list layouts must agree");

    let list = canonical.as_list::<i32>();
    assert_eq!(list.len(), 2);
    assert_eq!(list.value(0).len(), 1);
    assert!(list.value(1).is_empty());
}

#[test]
fn sliced_lists_gather_visible_ranges_before_validating() {
    let item = Arc::new(ArrowField::new("item", ArrowDataType::Date32, true));
    for visible in [Some(2), None, Some(3_000_000)] {
        // Exclude the outer rows and the NULL middle row, even though their children are invalid.
        let dates = Date32Array::from(vec![Some(3_000_000), Some(1), None, visible, None]);
        let list = ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([1, 1, 1, 1, 1]),
            Arc::new(dates),
            Some(arrow_buffer::NullBuffer::from(vec![
                true, true, false, true, true,
            ])),
        );
        let schema = one_field("days", list.data_type().clone(), true);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list.slice(1, 3))]).unwrap();
        let normalized = normalize_batch(batch, schema);
        if visible == Some(2) {
            let normalized = normalized.unwrap();
            let list = normalized.column(0).as_list::<i32>();
            assert_eq!(list.value_offsets(), &[0, 1, 1, 2]);
            assert_eq!(list.null_count(), 0);
            assert_eq!(list.values().as_primitive::<Date32Type>().values(), &[1, 2]);
        } else {
            let failure = normalized.unwrap_err();
            assert_eq!(failure.status, Status::InvalidData);
            assert!(failure.message.contains("row 2"), "{}", failure.message);
            assert!(failure.message.contains("days"), "{}", failure.message);
        }
    }
}

/// GoogleSQL has no NULL ARRAY to preserve: BigQuery stores one as an empty ARRAY, and an
/// ingested array reaches the service as a `REPEATED` field, which has no NULL state at all.
/// A NULL list entry therefore normalizes to an empty array rather than surviving as NULL.
#[test]
fn a_null_array_entry_normalizes_to_an_empty_array() {
    let column = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        None,
    ])) as ArrayRef;
    let schema: SchemaRef = one_field("values", column.data_type().clone(), true);
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    let normalized = normalize_batch(batch, schema).unwrap();
    let list = normalized.column(0).as_list::<i32>();
    assert_eq!(list.len(), 2);
    assert_eq!(list.null_count(), 0);
    assert!(list.is_valid(1));
    assert!(list.value(1).is_empty());
}

/// A value-domain rejection inside a list names the caller's column and the batch row whose
/// entry holds the offending element, not the synthetic `"item"` field and a position in the
/// flattened child array.
#[test]
fn value_domain_rejections_inside_a_list_name_the_batch_row() {
    const MIDNIGHT: i64 = 86_400_000_000;
    let expected = "Arrow time ingest field \"clock\" contains an out-of-range value at row 1; \
                    a time must be within one day";
    let entries = vec![
        Some(vec![Some(0), Some(0), Some(0)]),
        Some(vec![Some(MIDNIGHT)]),
    ];
    let item = Arc::new(ArrowField::new(
        "item",
        ArrowDataType::Time64(TimeUnit::Microsecond),
        true,
    ));
    let schema: SchemaRef = one_field("clock", ArrowDataType::List(item), false);
    let normalize = |column: ArrayRef| {
        let source = one_field("clock", column.data_type().clone(), false);
        let batch = RecordBatch::try_new(source, vec![column]).unwrap();
        normalize_batch(batch, schema.clone()).unwrap_err()
    };

    // A canonical list passes its child through untouched, so an element index addresses the
    // caller's own array.
    let canonical =
        Arc::new(ListArray::from_iter_primitive::<Time64MicrosecondType, _, _>(entries.clone()))
            as ArrayRef;
    assert_eq!(normalize(canonical).message, expected);

    // A `LargeList` gathers its child first, so the index does not address the caller's array
    // at all.
    let large = Arc::new(LargeListArray::from_iter_primitive::<
        Time64MicrosecondType,
        _,
        _,
    >(entries.clone())) as ArrayRef;
    assert_eq!(normalize(large).message, expected);

    // A sliced list is judged only by the window its offsets address, and reports the row
    // within the slice.
    let sliced = ListArray::from_iter_primitive::<Time64MicrosecondType, _, _>(
        std::iter::once(Some(vec![Some(0)])).chain(entries),
    );
    assert_eq!(normalize(Arc::new(sliced.slice(1, 2))).message, expected);

    // A list view may address its child out of order, so the gathered child's own order is the
    // rows', and neither is the caller's child order: the same value now sits in row 0.
    let view = Arc::new(LargeListViewArray::new(
        Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Time64(TimeUnit::Microsecond),
            true,
        )),
        vec![3_i64, 0].into(),
        vec![1_i64, 3].into(),
        Arc::new(Time64MicrosecondArray::from(vec![0, 0, 0, MIDNIGHT])),
        None,
    )) as ArrayRef;
    assert_eq!(
        normalize(view).message,
        "Arrow time ingest field \"clock\" contains an out-of-range value at row 0; \
         a time must be within one day"
    );
}
