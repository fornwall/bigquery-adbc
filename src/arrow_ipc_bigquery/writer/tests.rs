use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, ListArray, RecordBatch, make_array, new_null_array,
};
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};

use super::super::reader::{decode_record_batch_message, decode_schema};

use super::{Writer, encode_schema};

/// Every Arrow type BigQuery ingest may put on the wire, one nesting layer included.
fn canonical_ingest_types() -> Vec<DataType> {
    let leaves = vec![
        DataType::Boolean,
        DataType::Int64,
        DataType::Float64,
        DataType::Binary,
        DataType::Utf8,
        DataType::Date32,
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, 38),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::MonthDayNano),
    ];
    let mut types = leaves.clone();
    for leaf in leaves {
        let item = Arc::new(Field::new("item", leaf.clone(), true));
        types.push(DataType::List(Arc::clone(&item)));
        types.push(DataType::Struct(Fields::from(vec![Field::new(
            "member", leaf, true,
        )])));
        types.push(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![Field::new(
                "member",
                item.data_type().clone(),
                true,
            )])),
            true,
        ))));
    }
    types
}

/// One nullable field per [`canonical_ingest_types`].
fn canonical_fields() -> Vec<Field> {
    canonical_ingest_types()
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| Field::new(format!("field_{index}"), data_type, true))
        .collect()
}

/// Arrow types outside BigQuery's canonical ingest surface, at the top level and nested.
fn noncanonical_ingest_types() -> Vec<DataType> {
    vec![
        DataType::Int32,
        DataType::Float32,
        DataType::Date64,
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::FixedSizeBinary(16),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
        DataType::Struct(Fields::from(vec![Field::new(
            "member",
            DataType::Int32,
            true,
        )])),
    ]
}

/// Schema encoding and batch planning must agree on supported types and refusal wording.
#[test]
fn schema_encoder_and_batch_planner_accept_the_same_types() {
    for data_type in canonical_ingest_types() {
        let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
        assert!(
            encode_schema(&schema).is_ok(),
            "the schema encoder refused canonical type {data_type}"
        );
        for length in [0, 3] {
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![new_null_array(&data_type, length)],
            )
            .unwrap();
            assert!(
                Writer::new().encode_batch(&batch).is_ok(),
                "the batch planner refused canonical type {data_type} with {length} rows"
            );
        }
    }
    for data_type in noncanonical_ingest_types() {
        let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
        let refused = encode_schema(&schema).unwrap_err();
        // Ingest reports this to the caller, naming the column type it must convert.
        assert_eq!(
            refused.to_string(),
            format!(
                "Not yet implemented: Arrow IPC encoding of noncanonical BigQuery ingest type {}",
                innermost_refused(&data_type)
            )
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![new_null_array(&data_type, 3)]).unwrap();
        assert_eq!(
            Writer::new().encode_batch(&batch).unwrap_err().to_string(),
            refused.to_string(),
            "the batch planner must refuse the same noncanonical type"
        );
    }
}

/// The type both encoders name: they descend into List and Struct children, so a nested
/// offender is reported as itself rather than as its container.
fn innermost_refused(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => innermost_refused(field.data_type()),
        DataType::Struct(fields) => innermost_refused(fields[0].data_type()),
        other => other.clone(),
    }
}

fn framed_body(message: &[u8]) -> &[u8] {
    let metadata_length = u32::from_le_bytes(message[4..8].try_into().unwrap()) as usize;
    &message[super::MESSAGE_PREFIX_BYTES + metadata_length..]
}

fn assert_golden_body(
    batch: &RecordBatch,
    expected_length: usize,
    nonzero_regions: &[(usize, &[u8])],
) {
    let mut writer = Writer::new();
    let schema_message = encode_schema(batch.schema().as_ref()).unwrap();
    assert!(schema_message.len().is_multiple_of(super::ALIGNMENT));

    let planned = writer.plan_batch(batch).unwrap();
    let batch_size = planned.framed_size();
    let framed_batch = planned.write().unwrap();
    assert_eq!(framed_batch.len(), batch_size);
    assert!(framed_batch.len().is_multiple_of(super::ALIGNMENT));
    let actual_body = framed_body(&framed_batch);
    assert_eq!(actual_body.len(), expected_length);
    for (index, actual) in actual_body.iter().copied().enumerate() {
        let expected = nonzero_regions
            .iter()
            .find_map(|(start, bytes)| {
                index
                    .checked_sub(*start)
                    .and_then(|relative| bytes.get(relative))
            })
            .copied()
            .unwrap_or(0);
        assert_eq!(actual, expected, "body byte {index}");
    }

    let schema = decode_schema(&schema_message).unwrap();
    assert_eq!(
        &decode_record_batch_message(&ArrowBuffer::from(framed_batch), schema).unwrap(),
        batch
    );
}

#[test]
fn schema_round_trips_every_canonical_bigquery_type_and_metadata() {
    let metadata = HashMap::from([
        ("source".to_owned(), "writer-test".to_owned()),
        ("empty".to_owned(), String::new()),
    ]);
    let item = Field::new("element", DataType::Int64, true).with_metadata(HashMap::from([(
        "item-key".to_owned(),
        "item-value".to_owned(),
    )]));
    let mut fields = canonical_fields();
    fields[1] = fields[1].clone().with_nullable(false);
    fields.push(Field::new("array", DataType::List(Arc::new(item)), false));
    fields.push(
        Field::new(
            "record",
            DataType::Struct(
                vec![
                    Field::new("child", DataType::Utf8, true),
                    Field::new("flag", DataType::Boolean, false),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(HashMap::from([(
            "field-key".to_owned(),
            "field-value".to_owned(),
        )])),
    );
    let schema = Schema::new_with_metadata(fields, metadata);
    let mut stream = encode_schema(&schema).unwrap();
    stream.extend_from_slice(&super::super::END_OF_STREAM);

    assert_eq!(decode_schema(&stream).unwrap().as_ref(), &schema);
}

#[test]
fn exact_plans_round_trip_every_canonical_batch_layout_and_reuse_writer() {
    let fields = canonical_fields();
    let columns = fields
        .iter()
        .map(|field| new_null_array(field.data_type(), 3))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let sliced = batch.slice(1, 1);

    let mut writer = Writer::new();
    let schema_message = encode_schema(&schema).unwrap();
    let decoded_schema = decode_schema(&schema_message).unwrap();
    for expected in [&batch, &sliced] {
        let planned = writer.plan_batch(expected).unwrap();
        let predicted = planned.framed_size();
        let framed = planned.write().unwrap();
        assert_eq!(framed.len(), predicted);
        assert_eq!(
            &decode_record_batch_message(&ArrowBuffer::from(framed), Arc::clone(&decoded_schema))
                .unwrap(),
            expected
        );
    }
    assert_eq!(writer.body_materializations(), 2);
}

#[test]
fn written_and_discarded_plans_release_their_input_buffers() {
    let mut writer = Writer::new();
    for write in [false, true] {
        let values = arrow_array::Int64Array::from(vec![42; 1024]);
        let observer = values.values().inner().clone();
        let batch = RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)])
            .unwrap()
            .slice(7, 1);
        let plan = writer.plan_batch(&batch).unwrap();
        drop(batch);
        assert!(observer.strong_count() > 1, "the plan retains its source");
        if write {
            let encoded = plan.write().unwrap();
            assert!(!encoded.is_empty());
        } else {
            drop(plan);
        }
        assert_eq!(
            observer.strong_count(),
            1,
            "only the observer retains the input allocation"
        );
    }
}

#[test]
fn non_byte_aligned_boolean_and_validity_slices_match_oracle_golden() {
    let values = BooleanArray::from(vec![
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(true),
        Some(false),
        Some(true),
        Some(true),
    ]);
    let batch =
        RecordBatch::try_from_iter([("flag", Arc::new(values.slice(1, 7)) as ArrayRef)]).unwrap();
    // Captured from the Arrow Rust 59.3.0 encoder: validity=0b01101101 and
    // values=0b00101001, each padded to the requested 64-byte alignment.
    assert_golden_body(&batch, 128, &[(0, &[109]), (64, &[41])]);
}

#[test]
fn bitmap_rebasing_preserves_bits_at_every_offset_and_buffer_tail() {
    for bit_offset in (1_usize..32).filter(|offset| !offset.is_multiple_of(8)) {
        for bit_length in [0_usize, 1, 7, 8, 9, 63, 64, 65, 1023, 1024, 1025, 65_536] {
            for extra_bytes in [0, 1, 32] {
                let bytes = (bit_offset + bit_length).div_ceil(8) + extra_bytes;
                let input: Vec<u8> = (0..bytes)
                    .map(|index| u8::try_from((index * 37 + 173) % 256).unwrap())
                    .collect();
                let buffer = ArrowBuffer::from(input.clone());
                let mut writer = Writer::new();
                writer.plan_bitmap(&buffer, bit_offset, bit_length).unwrap();
                let mut actual = vec![0xaa, 0x55];
                writer.sources.last().unwrap().write(&mut actual);
                let mut expected = vec![0xaa, 0x55];
                expected.resize(2 + bit_length.div_ceil(8), 0);
                for bit in 0..bit_length {
                    let source = bit_offset + bit;
                    if input[source / 8] & (1 << (source % 8)) != 0 {
                        expected[2 + bit / 8] |= 1 << (bit % 8);
                    }
                }
                assert_eq!(
                    actual, expected,
                    "offset {bit_offset}, length {bit_length}, extra bytes {extra_bytes}"
                );
            }
        }
    }
}

#[test]
fn null_free_boolean_and_nested_slice_omit_validity_body_regions() {
    let booleans = make_array(
        ArrayData::builder(DataType::Boolean)
            .len(10)
            .add_buffer(ArrowBuffer::from([0b1011_0110, 0]))
            .build()
            .unwrap(),
    );
    let booleans = booleans.slice(1, 7);
    assert_eq!(booleans.null_count(), 0);

    let fields = Fields::from(vec![Field::new("flag", DataType::Boolean, true)]);
    let records = make_array(
        ArrayData::builder(DataType::Struct(fields))
            .len(7)
            .add_child_data(booleans.to_data())
            .build()
            .unwrap(),
    );
    assert_eq!(records.null_count(), 0);
    let batch = RecordBatch::try_from_iter([("record", records)]).unwrap();

    let mut writer = Writer::new();
    let planned = writer.plan_batch(&batch).unwrap();
    assert_eq!(
        planned
            .writer
            .buffers
            .iter()
            .map(|buffer| buffer.length)
            .collect::<Vec<_>>(),
        [0, 0, 1]
    );
    let encoded = planned.write().unwrap();
    assert_eq!(framed_body(&encoded).len(), 64);
    assert_eq!(framed_body(&encoded)[0], 0b0101_1011);

    // The in-tree reader must consume both zero-length validity descriptors and reconstruct
    // the same logical nested Boolean values without attaching a null bitmap.
    assert_golden_body(&batch, 64, &[(0, &[0b0101_1011])]);
}

#[test]
fn sliced_binary_and_list_offsets_match_oracle_golden() {
    let binary = BinaryArray::from(vec![
        Some(b"unselected prefix".as_slice()),
        Some(b"a".as_slice()),
        None,
        Some(b"selected tail".as_slice()),
        Some(b"unselected suffix".as_slice()),
    ]);
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(91), Some(92), Some(93)]),
        Some(vec![Some(1)]),
        None,
        Some(vec![Some(2), Some(3)]),
        Some(vec![Some(99)]),
    ]);
    let batch = RecordBatch::try_from_iter([
        ("bytes", Arc::new(binary.slice(1, 3)) as ArrayRef),
        ("items", Arc::new(lists.slice(1, 3)) as ArrayRef),
    ])
    .unwrap();
    assert_golden_body(
        &batch,
        384,
        &[
            (0, &[5]),
            (64, &[0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 14, 0, 0, 0]),
            (128, b"aselected tail"),
            (192, &[5]),
            (256, &[0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0]),
            (
                320,
                &[
                    1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
        ],
    );
}

#[test]
fn large_sliced_variable_width_batches_match_arrow_ipc() {
    let binary = BinaryArray::from_iter((0..1033).map(|row| match row % 5 {
        0 => None,
        1 => Some(b"".as_slice()),
        _ => Some(b"variable-width value".as_slice()),
    }));
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>((0_u32..1033).map(|row| {
        (!row.is_multiple_of(7)).then(|| {
            (0..row % 5)
                .map(|value| (value != 2).then_some(i64::from(row + value)))
                .collect::<Vec<_>>()
        })
    }));
    let batch = RecordBatch::try_from_iter([
        ("bytes", Arc::new(binary) as ArrayRef),
        ("items", Arc::new(lists) as ArrayRef),
    ])
    .unwrap();
    let mut writer = Writer::new();
    for first in 1..8 {
        for length in [0, 1, 7, 8, 9, 1023, 1024, 1025] {
            let expected = batch.slice(first, length);
            let mut stream = encode_schema(expected.schema_ref()).unwrap();
            stream.extend_from_slice(&writer.encode_batch(&expected).unwrap());
            stream.extend_from_slice(&super::super::END_OF_STREAM);
            let actual =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(stream), None)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            assert_eq!(actual, [expected], "slice {first}..{}", first + length);
        }
    }
}

/// `offset_range` inspects only the first and last offsets, and rebasing a negative interior
/// offset does not overflow, so without this check it would point before the values buffer in
/// the Storage Write body.
#[test]
fn offset_validation_checks_every_lane_and_tail_without_subtraction_overflow() {
    let data = BinaryArray::from(vec![Some(b"value".as_slice())]).to_data();
    let mut writer = Writer::new();
    for length in [3, 7, 8, 9, 31, 32, 33, 1025] {
        for first in [1_i32, i32::MAX] {
            let mut offsets = vec![first; length];
            for position in 1..length - 1 {
                for invalid in [first - 1, -1, i32::MIN] {
                    offsets[position] = invalid;
                    let error = writer
                        .plan_rebased_offsets(
                            Some(ArrowBuffer::from_slice_ref(&offsets)),
                            &data,
                            usize::try_from(first).unwrap(),
                        )
                        .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains(&format!("offset {invalid} precedes first offset {first}")),
                        "length {length}, position {position}: {error}"
                    );
                }
                offsets[position] = first;
            }
            writer
                .plan_rebased_offsets(
                    Some(ArrowBuffer::from_slice_ref(offsets)),
                    &data,
                    usize::try_from(first).unwrap(),
                )
                .unwrap();
        }
    }
}

fn empty_variable_width(data_type: DataType) -> ArrayData {
    ArrayData::builder(data_type)
        .len(0)
        .add_buffer(ArrowBuffer::default())
        .add_buffer(ArrowBuffer::default())
        .build()
        .unwrap()
}

fn list_data(child: ArrayData, length: usize, offsets: &[i32]) -> ArrayData {
    let data_type = DataType::List(Arc::new(Field::new(
        "element",
        child.data_type().clone(),
        true,
    )));
    ArrayData::builder(data_type)
        .len(length)
        .add_buffer(ArrowBuffer::from_slice_ref(offsets))
        .add_child_data(child)
        .build()
        .unwrap()
}

#[test]
fn empty_variable_width_children_with_empty_offsets_get_an_ipc_zero_offset() {
    let strings = list_data(empty_variable_width(DataType::Utf8), 1, &[0, 0]);
    let bytes = list_data(empty_variable_width(DataType::Binary), 1, &[0, 0]);

    // BigQuery forbids ARRAY<ARRAY<T>>, but an empty repeated record can itself contain a
    // repeated field. That makes an empty List child reachable in its canonical Arrow schema.
    let nested_strings = list_data(empty_variable_width(DataType::Utf8), 0, &[]);
    let record = ArrayData::builder(DataType::Struct(
        vec![Field::new(
            "strings",
            nested_strings.data_type().clone(),
            true,
        )]
        .into(),
    ))
    .len(0)
    .add_child_data(nested_strings)
    .build()
    .unwrap();
    let records = list_data(record, 1, &[0, 0]);

    assert!(strings.child_data()[0].buffers()[0].is_empty());
    assert!(bytes.child_data()[0].buffers()[0].is_empty());
    let nested_list = &records.child_data()[0].child_data()[0];
    assert!(nested_list.buffers()[0].is_empty());
    assert!(nested_list.child_data()[0].buffers()[0].is_empty());

    let expected = RecordBatch::try_from_iter([
        ("strings", make_array(strings)),
        ("bytes", make_array(bytes)),
        ("records", make_array(records.clone())),
    ])
    .unwrap();

    // Each implicit offset becomes one four-byte zero buffer, padded to 64 bytes. All validity
    // descriptors remain present but need no body region because every logical value is valid.
    let mut writer = Writer::new();
    let planned = writer.plan_batch(&expected).unwrap();
    assert!(
        planned
            .writer
            .buffers
            .iter()
            .map(|buffer| buffer.length)
            .eq([
                0, 8, 0, 4, 0, // ARRAY<STRING>
                0, 8, 0, 4, 0, // ARRAY<BYTES>
                0, 8, 0, 0, 4, 0, 4, 0, // ARRAY<STRUCT<ARRAY<STRING>>>
            ])
    );
    assert_eq!(planned.writer.body_length, 448);
    let framed_batch = planned.write().unwrap();
    assert_eq!(framed_body(&framed_batch), vec![0; 448]);

    let schema = encode_schema(expected.schema().as_ref()).unwrap();
    let schema = decode_schema(&schema).unwrap();
    assert_eq!(
        &decode_record_batch_message(&ArrowBuffer::from(framed_batch), schema).unwrap(),
        &expected
    );
}

#[test]
fn empty_binary_and_list_keep_required_empty_buffers() {
    let binary = BinaryArray::from(Vec::<Option<&[u8]>>::new());
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), Some(2)])]);
    let empty_list = Arc::new(lists.slice(1, 0)) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("bytes", binary.data_type().clone(), true),
            Field::new("items", empty_list.data_type().clone(), true),
        ])),
        vec![Arc::new(binary) as ArrayRef, empty_list],
    )
    .unwrap();
    // The two required zero offsets each occupy a 64-byte-aligned buffer.
    assert_golden_body(&batch, 128, &[]);
}
