//! Cross-checks of the in-tree Arrow IPC codec against `arrow-ipc`, an independent implementation.
//!
//! The Storage mocks use this crate's own [`writer`](super::writer) and [`reader`](super::reader),
//! so matching bugs in both could pass their round-trip tests. These tests exchange bytes with
//! `arrow-ipc` in both directions for every Arrow type used by BigQuery Storage Read and Storage
//! Write. `arrow-ipc` is a dev-dependency and is excluded from the shipped driver.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float64Array, Int64Array, IntervalMonthDayNanoArray, IntervalYearMonthArray, ListArray,
    RecordBatch, StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_buffer::{Buffer, IntervalMonthDayNano, NullBuffer, OffsetBuffer, i256};
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_select::concat::concat_batches;

use super::reader::{decode_record_batch_message, decode_schema};
use super::{canonical_schema, encode_stream, message_length};

/// One row per column value below, so a buffer read one element short still decodes.
const ROWS: usize = 3;

/// A populated batch of [`canonical_schema`], one null in every column.
///
/// Values rather than an empty batch: an empty column has no value bytes, so a writer that
/// emitted a type's buffers in the wrong order would still match. Every column also carries a
/// null, because the validity buffer is the one buffer whose descriptor is emitted even when it
/// is empty, and a reader that mispaired it with the values would read the values as validity.
fn canonical_batch() -> RecordBatch {
    let schema = canonical_schema();
    let nested = match schema.field_with_name("struct").unwrap().data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => unreachable!("the canonical struct column is a struct, got {other}"),
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(7)])),
        Arc::new(Float64Array::from(vec![Some(-1.5), None, Some(2.25)])),
        Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"ab".as_slice()),
            None,
            Some(b"".as_slice()),
        ])),
        Arc::new(StringArray::from(vec![
            Some(r#"{"a":1}"#),
            None,
            Some("\u{1f600}"),
        ])),
        Arc::new(Date32Array::from(vec![Some(-1), None, Some(19_000)])),
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(0),
            None,
            Some(86_399_999_999),
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(-1), None, Some(1_700_000_000_000_000)])
                .with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(-1_i128), None, Some(i128::MAX)])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![Some(i256::MINUS_ONE), None, Some(i256::MAX)])
                .with_precision_and_scale(76, 38)
                .unwrap(),
        ),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, -2, 3)),
            None,
            Some(IntervalMonthDayNano::new(0, 0, i64::MIN)),
        ])),
        Arc::new(int64_list()),
        Arc::new(StructArray::new(
            nested,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("ccc")])),
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        )),
    ];
    RecordBatch::try_new(schema, columns).expect("the canonical fixture matches its schema")
}

/// The canonical `array` column: a nullable list whose items are themselves nullable.
fn int64_list() -> ListArray {
    let mut builder = ListBuilder::new(Int64Builder::new());
    builder.values().append_value(10);
    builder.values().append_null();
    builder.append(true);
    builder.append(false);
    builder.values().append_value(-30);
    builder.append(true);
    let list = builder.finish();
    // The builder names the item field `item` and marks it nullable, which is the canonical
    // shape; assert it rather than rebuilding the array, so a builder change is caught here.
    assert_eq!(list.data_type(), &DataType::List(item_field()));
    list
}

fn item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Int64, true))
}

/// `LIST<STRUCT<..>>` and `STRUCT<LIST<..>>`, the two nesting shapes ingest can put on the wire.
///
/// The flat fixture above covers `LIST` and `STRUCT` one level deep. These are the combinations
/// where a buffer-order mistake is easiest to make and hardest to see, because the child's
/// buffers sit between the parent's in the body.
fn nested_batch() -> RecordBatch {
    let members = Fields::from(vec![
        Field::new("number", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]);
    let values = StructArray::new(
        members.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
            Arc::new(StringArray::from(vec![
                None,
                Some("b"),
                Some(""),
                Some("d"),
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let item = Arc::new(Field::new("item", DataType::Struct(members), true));
    let list_of_struct = ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(vec![0, 2, 2, 4].into()),
        Arc::new(values),
        Some(NullBuffer::from(vec![true, false, true])),
    );

    let inner = ListArray::new(
        item_field(),
        OffsetBuffer::new(vec![0, 1, 1, 3].into()),
        Arc::new(Int64Array::from(vec![Some(9), None, Some(-9)])),
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let struct_of_list = StructArray::new(
        Fields::from(vec![Field::new(
            "items",
            DataType::List(item_field()),
            true,
        )]),
        vec![Arc::new(inner) as ArrayRef],
        Some(NullBuffer::from(vec![false, true, true])),
    );

    RecordBatch::try_from_iter_with_nullable([
        ("list_of_struct", Arc::new(list_of_struct) as ArrayRef, true),
        ("struct_of_list", Arc::new(struct_of_list) as ArrayRef, true),
    ])
    .expect("the nested fixture is a valid batch")
}

/// The complete stream `arrow-ipc` produces for `batches` under `schema`.
fn encode_with_arrow_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer =
        StreamWriter::try_new(Vec::new(), schema).expect("arrow-ipc accepts the canonical schema");
    for batch in batches {
        writer.write(batch).expect("arrow-ipc encodes the batch");
    }
    writer.into_inner().expect("arrow-ipc finishes the stream")
}

/// Decodes a whole stream with the in-tree reader, one message at a time.
fn decode_in_tree(stream: &[u8]) -> (SchemaRef, Vec<RecordBatch>) {
    let schema_length = message_length(stream);
    let schema = decode_schema(&stream[..schema_length]).expect("the schema message decodes");
    let mut batches = Vec::new();
    let mut cursor = schema_length;
    while cursor < stream.len() {
        // A zero metadata length is the end-of-stream marker rather than another message.
        if u32::from_le_bytes(stream[cursor + 4..cursor + 8].try_into().unwrap()) == 0 {
            assert_eq!(
                cursor + 8,
                stream.len(),
                "trailing bytes after the end marker"
            );
            break;
        }
        let length = message_length(&stream[cursor..]);
        let message = Buffer::from(stream[cursor..cursor + length].to_vec());
        batches.push(
            decode_record_batch_message(&message, Arc::clone(&schema))
                .expect("the record batch decodes"),
        );
        cursor += length;
    }
    (schema, batches)
}

/// Decodes a whole stream with `arrow-ipc`.
fn decode_with_arrow_ipc(stream: Vec<u8>) -> (SchemaRef, Vec<RecordBatch>) {
    let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(stream), None)
        .expect("arrow-ipc parses the stream schema");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("arrow-ipc parses every batch");
    (schema, batches)
}

/// Asserts that both implementations agree on `batches` in both directions.
fn assert_both_directions_agree(schema: &SchemaRef, batches: &[RecordBatch]) {
    let (in_tree_schema, in_tree_batches) = decode_with_arrow_ipc(encode_stream(schema, batches));
    assert_eq!(in_tree_schema.as_ref(), schema.as_ref());
    assert_eq!(in_tree_batches, batches);

    let (oracle_schema, oracle_batches) = decode_in_tree(&encode_with_arrow_ipc(schema, batches));
    assert_eq!(oracle_schema.as_ref(), schema.as_ref());
    assert_eq!(oracle_batches, batches);
}

/// Every Arrow type BigQuery Storage Read and Storage Write carry, checked against `arrow-ipc`.
#[test]
fn every_canonical_column_agrees_with_arrow_ipc_in_both_directions() {
    let batch = canonical_batch();
    assert_both_directions_agree(&batch.schema(), std::slice::from_ref(&batch));
}

/// The same, one column at a time: a whole-batch comparison can be satisfied by two columns whose
/// mistakes cancel in the body, and per-column messages cannot.
#[test]
fn each_canonical_column_on_its_own_agrees_with_arrow_ipc() {
    let batch = canonical_batch();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![field.as_ref().clone()]));
        let column =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(batch.column(index))])
                .expect("one canonical column is a batch");
        assert_both_directions_agree(&schema, std::slice::from_ref(&column));
    }
}

/// `LIST<STRUCT<..>>` and `STRUCT<LIST<..>>`: the child buffers sit between the parent's, so a
/// traversal that visited them in the wrong order is invisible to a self-consistent round trip.
#[test]
fn nested_columns_agree_with_arrow_ipc_in_both_directions() {
    let batch = nested_batch();
    assert_both_directions_agree(&batch.schema(), std::slice::from_ref(&batch));
}

/// A sliced batch, where the writer rebases offsets and repacks unaligned bitmaps rather than
/// copying arrow's buffers verbatim. That rewriting is the writer's own, so the in-tree reader
/// reading it back proves only that the two agree; `arrow-ipc` is what says it is Arrow IPC.
#[test]
fn a_sliced_batch_agrees_with_arrow_ipc_in_both_directions() {
    let batch = canonical_batch();
    assert_eq!(batch.num_rows(), ROWS);
    let sliced = batch.slice(1, 2);
    assert_both_directions_agree(&sliced.schema(), std::slice::from_ref(&sliced));
}

/// Several batches on one stream, encoded through a reused [`Writer`](super::writer::Writer):
/// the scratch buffers it keeps between batches are cleared, not carried over.
#[test]
fn a_multi_batch_stream_agrees_with_arrow_ipc_in_both_directions() {
    let batch = canonical_batch();
    let batches = [batch.clone(), batch.slice(0, 1), batch.slice(2, 1)];
    assert_both_directions_agree(&batch.schema(), &batches);
}

/// An empty batch still declares every buffer its schema calls for, which is the case where a
/// missing descriptor is easiest to miss: nothing reads the bytes it would have pointed at.
#[test]
fn an_empty_batch_agrees_with_arrow_ipc_in_both_directions() {
    let batch = canonical_batch();
    assert_both_directions_agree(&batch.schema(), &[batch.slice(0, 0)]);
}

/// The two canonical ingest types [`canonical_schema`] has no column for.
///
/// `canonical_schema` is a Storage Read result shape (`TIMESTAMP` with a zone, `INTERVAL` as
/// `MonthDayNano`). Ingest also accepts a zoneless `DATETIME` and the four-byte `YearMonth`
/// interval, whose width (4 bytes against 16) is where a length-per-element mistake would hide.
#[test]
fn the_ingest_only_canonical_types_agree_with_arrow_ipc() {
    let year_month = IntervalYearMonthArray::from(vec![Some(14), None, Some(-25)]);
    let datetime = TimestampMicrosecondArray::from(vec![Some(i64::MIN + 1), None, Some(0)]);
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("year_month", Arc::new(year_month) as ArrayRef, true),
        ("datetime", Arc::new(datetime) as ArrayRef, true),
    ])
    .expect("the ingest-only fixture is a valid batch");
    assert_both_directions_agree(&batch.schema(), std::slice::from_ref(&batch));
}

/// The compressed direction, which is the one Storage Read takes: every read session asks
/// BigQuery for `LZ4_FRAME` buffers. `arrow-ipc` stores a buffer behind the uncompressed sentinel
/// where LZ4 would grow it, as it does for every buffer of the three-row fixtures, so each fixture
/// is also repeated until its buffers compress.
#[test]
fn an_lz4_compressed_arrow_ipc_stream_decodes_in_tree() {
    for batch in [canonical_batch(), nested_batch()] {
        let schema = batch.schema();
        let repeated = concat_batches(&schema, std::iter::repeat_n(&batch, 1000))
            .expect("repeating a batch keeps its schema");
        let batches = [batch, repeated];
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .expect("arrow-ipc is built with LZ4");
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), &schema, options)
            .expect("arrow-ipc accepts the canonical schema");
        for batch in &batches {
            writer.write(batch).expect("arrow-ipc compresses the batch");
        }
        let stream = writer.into_inner().expect("arrow-ipc finishes the stream");

        let (decoded_schema, decoded) = decode_in_tree(&stream);
        assert_eq!(decoded_schema.as_ref(), schema.as_ref());
        assert_eq!(decoded, batches);
    }
}
