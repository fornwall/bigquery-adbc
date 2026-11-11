//! Value normalization: what a batch is rewritten to before it is written, and which canonical
//! columns are passed through without a copy.

use arrow_array::{Date64Array, Time32SecondArray};

use super::*;
use crate::test_support::errors::assert_message_omits;

#[test]
fn invalid_ingest_values_do_not_appear_in_errors() {
    let cases: [(ArrayRef, &str, &str); 12] = [
        (
            Arc::new(Date32Array::from(vec![12_345_678])),
            "12345678",
            "years 1 to 9999",
        ),
        (
            Arc::new(Date64Array::from(vec![86_411_223])),
            "86411223",
            "not a whole day",
        ),
        (
            Arc::new(Time32SecondArray::from(vec![123_456])),
            "123456",
            "within one day",
        ),
        (
            Arc::new(Time64MicrosecondArray::from(vec![123_456_789_012])),
            "123456789012",
            "within one day",
        ),
        (
            Arc::new(Time64NanosecondArray::from(vec![123_456_789_012_345])),
            "123456789012345",
            "within one day",
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![
                876_543_210_987_654_321,
            ])),
            "876543210987654321",
            "years 1 to 9999",
        ),
        // The multiply into microseconds would overflow, so the range check comes first.
        (
            Arc::new(TimestampSecondArray::from(vec![i64::MAX]).with_timezone_utc()),
            "9223372036854775807",
            "years 1 to 9999",
        ),
        (
            Arc::new(DurationSecondArray::from(vec![123_456_789_012])),
            "123456789012",
            "maximum",
        ),
        (
            Arc::new(IntervalYearMonthArray::from(vec![234_567])),
            "234567",
            "month component",
        ),
        (
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                4_567_890, 0,
            )])),
            "4567890",
            "day component",
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![654_321])
                    .with_precision_and_scale(3, 0)
                    .unwrap(),
            ),
            "654321",
            "precision 3",
        ),
        (
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(654_321)])
                    .with_precision_and_scale(3, 0)
                    .unwrap(),
            ),
            "654321",
            "precision 3",
        ),
    ];
    for (column, secret, reason) in cases {
        let schema = one_column_schema("value", column.data_type(), false);
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
        let normalized = Arc::new(normalize_schema(&schema).unwrap());
        let failure = normalize_batch(batch, normalized).unwrap_err();
        assert_failure(&failure, Status::InvalidData, &["\"value\"", reason]);
        assert_message_omits(&failure, &[secret]);
    }
}

#[test]
fn normalizes_null_and_noncanonical_lists() {
    let int64_item = || Arc::new(Field::new("item", DataType::Int64, true));
    let int32_item = || Arc::new(Field::new("item", DataType::Int32, true));
    let in_struct = |list: ArrayRef| -> ArrayRef {
        let fields: Fields = vec![Field::new("values", list.data_type().clone(), true)].into();
        Arc::new(StructArray::new(fields, vec![list], None))
    };
    let sliced = {
        let list = ListArray::new(
            int64_item(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 6, 9, 10])),
            Arc::new(Int64Array::from(vec![70, 71, 90, 91, 1, 2, 3, 4, 5, 80])),
            Some(NullBuffer::from(vec![true, false, true, true, false])),
        )
        .slice(1, 3);
        assert_eq!(list.value_offsets(), &[2, 4, 6, 9]);
        Arc::new(list) as ArrayRef
    };
    let cases: Vec<(&str, ArrayRef, &[i32], &[i64])> = vec![
        (
            "null lists normalize to empty lists",
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                None,
                Some(vec![Some(1), Some(2)]),
            ])),
            &[0, 0, 2],
            &[1, 2],
        ),
        (
            "values hidden by null lists are compacted away",
            Arc::new(ListArray::new(
                int64_item(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5, 6])),
                Arc::new(Int64Array::from(vec![90, 91, 1, 2, 80, 3])),
                Some(NullBuffer::from(vec![false, true, false, true])),
            )),
            &[0, 0, 2, 2, 3],
            &[1, 2, 3],
        ),
        (
            "sliced null lists with non-zero offsets",
            sliced,
            &[0, 0, 2, 5],
            &[1, 2, 3, 4, 5],
        ),
        (
            "all values hidden by null lists are discarded",
            Arc::new(ListArray::new(
                int64_item(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 5, 6])),
                Arc::new(Int64Array::from(vec![90, 91, 80, 81, 82, 70])),
                Some(NullBuffer::from(vec![false, false, false])),
            )),
            &[0, 0, 0, 0],
            &[],
        ),
        (
            "null lists nested in structs",
            in_struct(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                vec![None, Some(vec![Some(1)])],
            ))),
            &[0, 0, 1],
            &[1],
        ),
        (
            "large lists discard hidden values",
            Arc::new(LargeListArray::new(
                int32_item(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 4, 7])),
                Arc::new(Int32Array::from(vec![90, 91, 1, 2, 3, 4, 5])),
                Some(NullBuffer::from(vec![false, true, true])),
            )),
            &[0, 0, 2, 5],
            &[1, 2, 3, 4, 5],
        ),
        (
            "non-contiguous list views are gathered in order",
            Arc::new(ListViewArray::new(
                int32_item(),
                ScalarBuffer::from(vec![4_i32, 0, 2, 1]),
                ScalarBuffer::from(vec![2_i32, 2, 2, 2]),
                Arc::new(Int32Array::from(vec![10, 11, 20, 21, 40, 41])),
                Some(NullBuffer::from(vec![true, false, true, true])),
            )),
            &[0, 2, 2, 4, 6],
            &[40, 41, 20, 21, 11, 20],
        ),
        (
            "large list views preserve empty, overlapping, and non-monotonic ranges",
            Arc::new(LargeListViewArray::new(
                int32_item(),
                ScalarBuffer::from(vec![4_i64, 0, 2, 5, 1]),
                ScalarBuffer::from(vec![2_i64, 2, 0, 1, 3]),
                Arc::new(Int32Array::from(vec![10, 11, 20, 21, 40, 41])),
                Some(NullBuffer::from(vec![true, false, true, true, true])),
            )),
            &[0, 2, 2, 2, 3, 6],
            &[40, 41, 41, 11, 20, 21],
        ),
        (
            "fixed-size lists nested in structs",
            in_struct(Arc::new(FixedSizeListArray::new(
                int32_item(),
                2,
                Arc::new(Int32Array::from(vec![1, 2, 90, 91, 3, 4])),
                Some(NullBuffer::from(vec![true, false, true])),
            ))),
            &[0, 2, 2, 4],
            &[1, 2, 3, 4],
        ),
    ];
    for (name, column, expected_offsets, expected_values) in cases {
        let schema = one_column_schema("values", column.data_type(), true);
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        let normalized = normalize_batch(batch, normalized_schema).unwrap();
        let column = normalized.column(0);
        let column = match column.as_any().downcast_ref::<StructArray>() {
            Some(records) => records.column(0),
            None => column,
        };
        let list = array_as::<ListArray>(column);
        let values = array_as::<Int64Array>(list.values());
        assert_eq!(list.null_count(), 0, "{name}");
        assert_eq!(list.value_offsets(), expected_offsets, "{name}");
        assert_eq!(values.values(), expected_values, "{name}");
    }
}

#[test]
fn preserves_canonical_list_values_without_copying() {
    // Null lists that hide no values rebuild the list around the same values array.
    let list = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        None,
        Some(vec![Some(1), Some(2)]),
    ]));
    let original_values = list.values().clone();
    let schema = one_column_schema("values", list.data_type(), true);
    let batch = RecordBatch::try_new(schema.clone(), vec![list]).unwrap();
    let normalized = normalize_batch(batch, schema).unwrap();
    let list = column_as::<ListArray>(&normalized, 0);
    assert_eq!(list.value_length(0), 0);
    assert!(Arc::ptr_eq(list.values(), &original_values));

    // A canonical large list keeps its values without a gather.
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
    let list = Arc::new(LargeListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 3])),
        values.clone(),
        None,
    ));
    let schema = one_column_schema("values", list.data_type(), false);
    let batch = RecordBatch::try_new(schema.clone(), vec![list]).unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let normalized = normalize_batch(batch, normalized_schema).unwrap();
    let list = column_as::<ListArray>(&normalized, 0);
    assert_eq!(list.value_offsets(), &[0, 2, 3]);
    assert!(Arc::ptr_eq(list.values(), &values));
}

#[test]
fn canonical_decimal_lists_retain_inherited_item_metadata_without_copying_values() {
    let numeric = Arc::new(
        Decimal128Array::from(vec![1, 2])
            .with_precision_and_scale(38, 9)
            .unwrap(),
    ) as ArrayRef;
    let bignumeric = Arc::new(
        Decimal256Array::from(vec![i256::ONE, i256::MINUS_ONE])
            .with_precision_and_scale(76, 38)
            .unwrap(),
    ) as ArrayRef;

    for (element_type, values) in [("NUMERIC", numeric), ("BIGNUMERIC", bignumeric)] {
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            values.clone(),
            None,
        )) as ArrayRef;
        let field = bigquery_typed(
            Field::new("values", list.data_type().clone(), false),
            format!("ARRAY<{element_type}>"),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        let batch = RecordBatch::try_new(schema, vec![list]).unwrap();

        let normalized = normalize_batch(batch, normalized_schema.clone()).unwrap();
        let list = column_as::<ListArray>(&normalized, 0);
        assert_eq!(list.data_type(), normalized_schema.field(0).data_type());
        let DataType::List(item) = list.data_type() else {
            panic!("expected list");
        };
        assert_eq!(item.metadata()[BIGQUERY_TYPE_METADATA_KEY], element_type);
        assert!(Arc::ptr_eq(list.values(), &values));
    }
}

#[test]
fn normalizes_batches_without_copying_canonical_columns() {
    let categories: DictionaryArray<Int8Type> = vec![Some("first"), None].into_iter().collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("category", categories.data_type().clone(), true),
    ]));
    let names = Arc::new(StringArray::from(vec![Some("one"), None])) as ArrayRef;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            names.clone(),
            Arc::new(categories),
        ],
    )
    .unwrap();
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    let normalized = normalize_batch(batch, normalized_schema).unwrap();
    assert_eq!(normalized.column(0).data_type(), &DataType::Int64);
    assert!(Arc::ptr_eq(normalized.column(1), &names));
    assert_eq!(normalized.column(2).data_type(), &DataType::Utf8);
    assert_eq!(
        column_as::<StringArray>(&normalized, 2),
        &StringArray::from(vec![Some("first"), None])
    );
}

/// A declared maximum length is enforced during normalization: characters for strings, bytes for
/// binary, per element for lists.
#[test]
fn rejects_values_longer_than_their_declared_maximum_length() {
    let limited = |name: &str, data_type: DataType, max_length: &str| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
            max_length.to_owned(),
        )]))
    };
    let normalize = |field: Field, column: ArrayRef| {
        let schema = Arc::new(Schema::new(vec![field]));
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        arrow_schema_to_bigquery(&normalized_schema).unwrap();
        normalize_batch(
            RecordBatch::try_new(schema, vec![column]).unwrap(),
            normalized_schema,
        )
    };

    // Three characters fit a limit of three however many bytes they take.
    let fits = Arc::new(StringArray::from(vec![Some("abc"), Some("äöü"), None])) as ArrayRef;
    normalize(limited("code", DataType::Utf8, "3"), fits).unwrap();

    let failure = normalize(
        limited("code", DataType::LargeUtf8, "3"),
        Arc::new(arrow_array::LargeStringArray::from(vec!["abc", "abcd"])),
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "BigQuery ingest field \"code\" holds a value of 4 characters at row 1, longer than its \
         declared maximum length of 3"
    );

    let failure = normalize(
        limited("hash", DataType::Binary, "2"),
        Arc::new(arrow_array::BinaryArray::from(vec![
            &b"ab"[..],
            &b"abc"[..],
        ])),
    )
    .unwrap_err();
    assert_message(&failure, &["3 bytes at row 1"]);

    let mut builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    builder.append_value([Some("a")]);
    builder.append_value([Some("b"), Some("toolong")]);
    let tags = Arc::new(builder.finish()) as ArrayRef;
    let failure = normalize(limited("tags", tags.data_type().clone(), "4"), tags).unwrap_err();
    assert_eq!(
        failure.message,
        "BigQuery ingest field \"tags\" holds a value of 7 characters at row 1, longer than its \
         declared maximum length of 4"
    );
}
