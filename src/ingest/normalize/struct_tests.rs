use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int8Type};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, DictionaryArray, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampSecondArray,
};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::normalize_batch;
use crate::arrow_ipc_bigquery::{reader::decode_record_batch_message, writer::Writer};

fn nullable_record(column: ArrayRef) -> StructArray {
    StructArray::new(
        vec![Arc::new(Field::new(
            "value",
            column.data_type().clone(),
            false,
        ))]
        .into(),
        vec![column],
        Some(NullBuffer::from(vec![false, true, false, true])),
    )
}

/// A batch of `record` as one nullable column, and its schema.
fn record_batch(record: StructArray) -> (RecordBatch, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "record",
        record.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(record)]).unwrap();
    (batch, schema)
}

fn normalize_record(record: StructArray, child_type: DataType) -> RecordBatch {
    let source = Arc::new(Schema::new(vec![Field::new(
        "record",
        record.data_type().clone(),
        true,
    )]));
    let target = Arc::new(Schema::new(vec![Field::new(
        "record",
        DataType::Struct(vec![Field::new("value", child_type, false)].into()),
        true,
    )]));
    let batch = RecordBatch::try_new(source, vec![Arc::new(record)]).unwrap();
    let normalized = normalize_batch(batch, target).unwrap();
    let encoded = Writer::new().encode_batch(&normalized).unwrap();
    let decoded = decode_record_batch_message(&Buffer::from(encoded), normalized.schema()).unwrap();
    assert_eq!(decoded, normalized);
    normalized
}

#[test]
fn null_structs_mask_invalid_child_values_before_validation_and_casts() {
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Date32Array::from(vec![i32::MAX, 0, i32::MIN, 1])),
            Arc::new(Date32Array::from(vec![Some(0), None, Some(1)])),
        ),
        (
            Arc::new(Date64Array::from(vec![i64::MAX, 0, i64::MIN, 86_400_000])),
            Arc::new(Date32Array::from(vec![Some(0), None, Some(1)])),
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![i64::MAX, 0, i64::MIN, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                None,
                Some(1_000_000),
            ])),
        ),
        (
            Arc::new(DictionaryArray::<Int8Type>::new(
                Int8Array::from(vec![0, 1, 0, 2]),
                Arc::new(Date32Array::from(vec![i32::MAX, 0, 1])),
            )),
            Arc::new(Date32Array::from(vec![Some(0), None, Some(1)])),
        ),
    ];
    for (column, expected) in cases {
        let record = nullable_record(column).slice(1, 3);
        let values = record.column(0).to_data().buffers()[0].clone();
        let preserves_values = record.column(0).data_type() == expected.data_type();
        let normalized = normalize_record(record, expected.data_type().clone());
        let record = normalized.column(0).as_struct();
        assert!(record.is_null(1));
        assert_eq!(record.column(0).as_ref(), expected.as_ref());
        if preserves_values {
            assert!(record.column(0).to_data().buffers()[0].ptr_eq(&values));
        }
    }
}

#[test]
fn null_structs_mask_nested_structs_and_list_elements() {
    let list = Arc::new(ListArray::from_iter_primitive::<Date32Type, _, _>([
        Some(vec![Some(i32::MAX)]),
        Some(vec![Some(0)]),
        Some(vec![None, Some(i32::MIN)]),
        Some(vec![Some(1)]),
    ])) as ArrayRef;
    let nested = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "days",
            list.data_type().clone(),
            false,
        ))]
        .into(),
        vec![list],
        None,
    )) as ArrayRef;
    let child_type = nested.data_type().clone();
    let record = nullable_record(nested).slice(1, 3);
    let normalized = normalize_record(record, child_type);
    let outer = normalized.column(0).as_struct();
    let inner = outer.column(0).as_struct();
    assert!(inner.is_null(1));
    let list = inner.column(0).as_list::<i32>();
    assert_eq!(list.null_count(), 0);
    assert_eq!(list.value_offsets(), &[0, 1, 1, 2]);
    assert_eq!(list.values().as_ref(), &Date32Array::from(vec![0, 1]));
}

#[test]
fn visible_struct_child_errors_keep_the_batch_row() {
    let column = Arc::new(Date32Array::from(vec![i32::MAX, 0, i32::MIN, i32::MAX]));
    let record = nullable_record(column).slice(1, 3);
    let (batch, schema) = record_batch(record);
    let error = normalize_batch(batch, schema).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert_eq!(
        error.message,
        "Arrow date ingest field \"value\" contains an out-of-range value at row 2; \
         BigQuery supports years 1 to 9999"
    );
}

#[test]
fn struct_mask_preserves_child_nulls_and_reuses_an_existing_combined_mask() {
    for first in [Some(i32::MAX), None] {
        let child = Arc::new(Date32Array::from(vec![first, None, Some(0)])) as ArrayRef;
        let record = StructArray::new(
            vec![Arc::new(Field::new("day", DataType::Date32, true))].into(),
            vec![child.clone()],
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let (batch, schema) = record_batch(record);
        let normalized = normalize_batch(batch, schema).unwrap();
        let record = normalized.column(0).as_struct();
        assert_eq!(
            record.column(0).as_ref(),
            &Date32Array::from(vec![None, None, Some(0)])
        );
        if first.is_none() {
            assert!(Arc::ptr_eq(record.column(0), &child));
        }
    }
}

#[test]
fn unchanged_nested_structs_preserve_array_identity_after_validation() {
    for nulls in [None, Some(NullBuffer::from(vec![true, false, true]))] {
        let day = Arc::new(Date32Array::from(vec![Some(0), None, Some(1)])) as ArrayRef;
        // Needs no check, so it is not masked (which would rebuild and revalidate it) though a
        // NULL struct hides one of its values.
        let label = Arc::new(StringArray::from(vec!["a", "hidden", "c"])) as ArrayRef;
        let fields = vec![
            Field::new("day", DataType::Date32, true),
            Field::new("label", DataType::Utf8, false),
        ];
        let mut column: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![day, label],
            nulls.clone(),
        ));
        column = Arc::new(StructArray::new(
            vec![Arc::new(Field::new(
                "inner",
                column.data_type().clone(),
                true,
            ))]
            .into(),
            vec![column],
            nulls.clone(),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column.clone()]).unwrap();
        let normalized = normalize_batch(batch, schema).unwrap();
        assert!(Arc::ptr_eq(normalized.column(0), &column));
    }
}

#[test]
fn changed_struct_field_metadata_rebuilds_the_struct_and_reuses_values() {
    let child = Arc::new(Date32Array::from(vec![0, 1])) as ArrayRef;
    let field = Field::new("day", DataType::Date32, false);
    let column = Arc::new(StructArray::new(
        vec![Arc::new(field.clone())].into(),
        vec![child.clone()],
        None,
    )) as ArrayRef;
    let source = Arc::new(Schema::new(vec![Field::new(
        "record",
        column.data_type().clone(),
        false,
    )]));
    let field = field.with_metadata(std::collections::HashMap::from([(
        "description".to_owned(),
        "Calendar day".to_owned(),
    )]));
    let target = Arc::new(Schema::new(vec![Field::new(
        "record",
        DataType::Struct(vec![field].into()),
        false,
    )]));
    let batch = RecordBatch::try_new(source, vec![column.clone()]).unwrap();
    let normalized = normalize_batch(batch, target.clone()).unwrap();
    assert!(!Arc::ptr_eq(normalized.column(0), &column));
    assert_eq!(normalized.schema_ref(), &target);
    assert_eq!(
        normalized.column(0).data_type(),
        target.field(0).data_type()
    );
    assert!(Arc::ptr_eq(
        normalized.column(0).as_struct().column(0),
        &child
    ));
}
