use arrow_array::types::IntervalDayTimeType;
use arrow_array::{
    ArrayRef, IntervalDayTimeArray, IntervalYearMonthArray, ListArray, RecordBatch, StringArray,
    new_null_array,
};
use arrow_buffer::IntervalDayTime;
use arrow_schema::{UnionFields, UnionMode};

use super::*;
use crate::arrow_ipc_bigquery::format::FieldNode;
use crate::arrow_ipc_bigquery::writer::{Writer, frame_message};
use crate::arrow_ipc_bigquery::{
    CONTINUATION_MARKER, END_OF_STREAM, canonical_schema, encode_schema_fixture, encode_stream,
    message_length,
};

// A `CreateReadSession` schema and the one `ReadRowsResponse` batch BigQuery sent for a
// three-row table holding every GoogleSQL type, nulls, empty and nested arrays, and a
// compressible STRING column, read with LZ4_FRAME buffer compression.
const BIGQUERY_SCHEMA: &[u8] = include_bytes!("../testdata/all_types_lz4_schema.arrow");
const BIGQUERY_BATCH: &[u8] = include_bytes!("../testdata/all_types_lz4_batch.arrow");

/// What BigQuery sends, rather than what an Arrow writer does: its LZ4 buffers are frames even
/// when LZ4 cannot shrink them (a stored block, where `arrow-ipc` writes the uncompressed
/// sentinel), and its schema carries extension metadata. `arrow-ipc` reading the same bytes is
/// the oracle.
#[test]
fn a_bigquery_storage_read_capture_decodes_as_arrow_ipc_decodes_it() {
    let schema = decode_schema(BIGQUERY_SCHEMA).unwrap();
    let batch =
        decode_record_batch_message(&Buffer::from(BIGQUERY_BATCH), Arc::clone(&schema)).unwrap();

    let stream = [BIGQUERY_SCHEMA, BIGQUERY_BATCH].concat();
    let mut oracle = arrow_ipc::reader::StreamReader::try_new(stream.as_slice(), None).unwrap();
    assert_eq!(oracle.schema(), schema);
    assert_eq!(oracle.next().unwrap().unwrap(), batch);

    // Both readers agreeing on garbage would pass the above, so pin a stored-block column and
    // the one column whose buffer LZ4 actually compressed.
    let column = |name| batch.column_by_name(name).unwrap().as_ref();
    assert_eq!(
        column("label"),
        &StringArray::from(vec![Some("\u{e9}\u{1f600}"), Some("first"), None])
            as &dyn arrow_array::Array
    );
    assert_eq!(
        column("text"),
        &StringArray::from(vec!["compressible ".repeat(40); 3]) as &dyn arrow_array::Array
    );
}

#[test]
fn complete_schema_message_decodes_without_an_end_marker() {
    let schema = canonical_schema();
    let encoded = encode_stream(&schema, &[]);
    let schema_message = &encoded[..message_length(&encoded)];

    assert_eq!(
        decode_schema(schema_message).unwrap().as_ref(),
        schema.as_ref()
    );
    assert_eq!(
        decode_schema(&schema_message[4..]).unwrap().as_ref(),
        schema.as_ref()
    );
    let mut legacy_with_end = schema_message[4..].to_vec();
    legacy_with_end.extend_from_slice(&[0; 4]);
    assert_eq!(
        decode_schema(&legacy_with_end).unwrap().as_ref(),
        schema.as_ref()
    );
}

#[test]
fn standalone_schema_rejects_a_partial_trailing_message() {
    let schema = canonical_schema();
    let mut encoded = encode_stream(&schema, &[]);
    // Leave two bytes of the eight-byte stream end marker after the complete schema.
    encoded.truncate(encoded.len() - 6);
    assert!(decode_schema(&encoded).is_err());
}

/// The physical types dbt-core 2.0 CSV seeds declare that no Storage Read batch carries.
fn csv_seed_only_types() -> Vec<DataType> {
    vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(4),
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Decimal32(7, 2),
        DataType::Decimal64(16, 4),
    ]
}

#[test]
fn standalone_schema_supports_csv_seed_physical_type_inputs() {
    let intervals = [IntervalUnit::YearMonth, IntervalUnit::DayTime];
    let fields = csv_seed_only_types()
        .into_iter()
        .chain(intervals.map(DataType::Interval))
        .enumerate()
        .map(|(index, data_type)| Field::new(format!("field_{index}"), data_type, true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let encoded = encode_schema_fixture(&schema);

    assert_eq!(decode_schema(&encoded).unwrap().as_ref(), schema.as_ref());
}

#[test]
fn record_batch_decodes_every_documented_interval_layout() {
    let year_month = IntervalYearMonthArray::from(vec![Some(14), None, Some(-25)]);
    let day_time = IntervalDayTimeArray::from(vec![
        Some(IntervalDayTime::new(2, 3)),
        None,
        Some(IntervalDayTime::new(-4, -5)),
    ]);
    let nested = ListArray::from_iter_primitive::<IntervalDayTimeType, _, _>([
        Some(vec![
            Some(IntervalDayTime::new(7, 8)),
            Some(IntervalDayTime::new(-9, -10)),
        ]),
        None,
        Some(vec![None, Some(IntervalDayTime::new(11, 12))]),
    ]);
    let expected = RecordBatch::try_from_iter([
        ("year_month", Arc::new(year_month) as ArrayRef),
        ("day_time", Arc::new(day_time) as ArrayRef),
        ("nested", Arc::new(nested) as ArrayRef),
    ])
    .unwrap();
    let year_month = expected.column(0).to_data();
    let day_time = expected.column(1).to_data();
    let nested = expected.column(2).to_data();
    let nested_values = &nested.child_data()[0];

    let mut body = Vec::new();
    let buffers = [
        append_test_buffer(&mut body, &[0b101]),
        append_test_buffer(&mut body, year_month.buffers()[0].as_slice()),
        append_test_buffer(&mut body, &[0b101]),
        append_test_buffer(&mut body, day_time.buffers()[0].as_slice()),
        append_test_buffer(&mut body, &[0b101]),
        append_test_buffer(&mut body, nested.buffers()[0].as_slice()),
        append_test_buffer(&mut body, &[0b1011]),
        append_test_buffer(&mut body, nested_values.buffers()[0].as_slice()),
    ];
    let nodes = [(3, 1), (3, 1), (3, 1), (4, 1)]
        .map(|(length, null_count)| FieldNode { length, null_count });
    let body_length = i64::try_from(body.len()).unwrap();
    let encoded_schema = frame_message(
        &format::encode_schema_fixture(expected.schema().as_ref()).unwrap(),
        &[],
    )
    .unwrap();
    let encoded_batch = frame_message(
        &format::encode_record_batch(3, &nodes, &buffers, body_length),
        &body,
    )
    .unwrap();
    let decoded_schema = decode_schema(&encoded_schema).unwrap();
    assert_eq!(
        &decode_record_batch_message(&Buffer::from(encoded_batch), decoded_schema).unwrap(),
        &expected
    );
}

#[test]
fn complete_messages_decode_all_bigquery_types_and_reject_fragments_and_trailing_bytes() {
    let schema = canonical_schema();
    let batch = RecordBatch::new_empty(Arc::clone(&schema));
    let encoded = encode_stream(&schema, std::slice::from_ref(&batch));
    let (encoded_schema, encoded_batch) = schema_and_batch(&encoded);

    let decoded_schema = decode_schema(encoded_schema).unwrap();
    assert_eq!(decoded_schema.as_ref(), schema.as_ref());
    assert_eq!(
        &decode_record_batch_message(
            &Buffer::from(encoded_batch.to_vec()),
            Arc::clone(&decoded_schema)
        )
        .unwrap(),
        &batch
    );
    assert_eq!(
        &decode_record_batch_message(
            &Buffer::from(encoded_batch[4..].to_vec()),
            Arc::clone(&decoded_schema)
        )
        .unwrap(),
        &batch
    );

    assert!(decode_schema(&encoded_schema[..3]).is_err());
    assert!(
        decode_record_batch_message(
            &Buffer::from(encoded_batch[..encoded_batch.len() - 1].to_vec()),
            Arc::clone(&decoded_schema),
        )
        .is_err()
    );
    let mut trailing = encoded_batch.to_vec();
    trailing.push(0);
    assert!(decode_record_batch_message(&Buffer::from(trailing), decoded_schema).is_err());
}

#[test]
fn dictionary_batch_is_rejected_without_parsing_unused_nested_data() {
    let encoded = frame_message(&format::encode_dictionary_batch_fixture(), &[]).unwrap();
    let error =
        decode_record_batch_message(&Buffer::from(encoded), Arc::new(Schema::empty())).unwrap_err();
    assert!(matches!(error, ArrowError::NotYetImplemented(_)));
    assert!(error.to_string().contains("dictionary batches"));
}

/// Well-formed Arrow outside the BigQuery subset is `NotYetImplemented`, naming the field and
/// the type, which is what lets a Storage Read session declaring it fall back to REST rather
/// than fail as malformed.
#[test]
fn a_well_formed_schema_outside_the_bigquery_subset_is_not_yet_implemented() {
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        false,
    );
    let union = UnionFields::try_new([0], [Field::new("a", DataType::Int64, true)]).unwrap();
    for (data_type, expected) in [
        (DataType::Null, "type Null"),
        (DataType::Union(union, UnionMode::Dense), "type Union"),
        (DataType::FixedSizeList(item(), 2), "type FixedSizeList"),
        (DataType::Map(Arc::new(entries), false), "type Map"),
        (DataType::Duration(TimeUnit::Second), "type Duration"),
        (DataType::LargeList(item()), "type LargeList"),
        (
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                item(),
            ),
            "type RunEndEncoded",
        ),
        (DataType::ListView(item()), "type ListView"),
        (DataType::LargeListView(item()), "type LargeListView"),
    ] {
        let schema = Schema::new(vec![Field::new("column", data_type, true)]);
        let error = decode_schema(&arrow_ipc_schema(&schema)).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Not yet implemented: field \"column\" has an unsupported Arrow IPC {expected}"
            )
        );
    }

    let dictionary = Schema::new(vec![Field::new(
        "column",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]);
    assert_eq!(
        decode_schema(&arrow_ipc_schema(&dictionary))
            .unwrap_err()
            .to_string(),
        "Not yet implemented: field \"column\" is dictionary-encoded, which BigQuery's \
         documented Arrow mappings never use"
    );
}

/// Neither Arrow's validation errors nor the LZ4 decoder's know which column they are in, so
/// the reader names the field: here for invalid UTF-8, and for a corrupted frame in the
/// BigQuery capture, whose first body buffer holds the `id` column's values.
#[test]
fn a_refused_buffer_names_its_field() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8,
        false,
    )]));
    let mut body = Vec::new();
    let offsets = [0_i32, 1].map(i32::to_le_bytes).concat();
    let buffers = [
        append_test_buffer(&mut body, b""),
        append_test_buffer(&mut body, &offsets),
        append_test_buffer(&mut body, b"\xff"),
    ];
    let error = decode_body(1, &[(1, 0)], &buffers, body, schema).unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("Ipc error: field \"label\" holds invalid Arrow data: "),
        "{error}"
    );

    let schema = decode_schema(BIGQUERY_SCHEMA).unwrap();
    let mut corrupted = BIGQUERY_BATCH.to_vec();
    let metadata_length = u32::from_le_bytes(corrupted[4..8].try_into().unwrap()) as usize;
    // The body, then the buffer's eight-byte length prefix, then the frame's magic number.
    corrupted[8 + metadata_length + 8] ^= 1;
    let error = decode_record_batch_message(&Buffer::from(corrupted), schema).unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("Ipc error: field \"id\": LZ4 Arrow IPC buffer holds magic number"),
        "{error}"
    );
}

/// The schema message `arrow-ipc` writes for `schema`.
fn arrow_ipc_schema(schema: &Schema) -> Vec<u8> {
    arrow_ipc::writer::StreamWriter::try_new(Vec::new(), schema)
        .unwrap()
        .into_inner()
        .unwrap()
}

/// Only the declared offsets are meaningful; trailing bytes need not fill a whole offset.
/// Short buffers must return a field-specific error.
#[test]
fn a_misdeclared_offset_buffer_length_is_an_error_rather_than_a_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    for (offsets_length, expected_error) in [
        (0_usize, Some("Need at least 4 bytes")),
        (7, Some("Expected 8 bytes got")),
        (8, None),
        // A ragged buffer with enough bytes for the declared offsets.
        (10, None),
        (12, None),
    ] {
        let mut body = Vec::new();
        let mut offsets = 0_i32.to_le_bytes().to_vec();
        offsets.extend_from_slice(&5_i32.to_le_bytes());
        offsets.resize(offsets_length, 0);
        let buffers = [
            append_test_buffer(&mut body, b""),
            append_test_buffer(&mut body, &offsets),
            append_test_buffer(&mut body, b"value"),
        ];
        let decoded = decode_body(1, &[(1, 0)], &buffers, body, schema.clone());
        match expected_error {
            Some(expected) => {
                let error = decoded.unwrap_err();
                assert!(error.to_string().contains(expected), "{error}");
                assert!(error.to_string().contains("field \"value\""), "{error}");
            }
            None => {
                let decoded = decoded.unwrap();
                assert_eq!(
                    decoded.column(0).as_ref(),
                    &StringArray::from(vec!["value"]) as &dyn arrow_array::Array
                );
            }
        }
    }
}

#[test]
fn malformed_buffer_region_is_an_error() {
    // The metadata declares a one-byte body, which its values buffer occupies, but the body
    // handed over is empty: the metadata parser cannot see that, so the reader must.
    let nodes = [FieldNode {
        length: 1,
        null_count: 0,
    }];
    let buffers = [(0, 0), (0, 1)].map(|(offset, length)| format::Buffer { offset, length });
    let metadata = format::encode_record_batch(1, &nodes, &buffers, 1);
    let MessageHeader::RecordBatch(batch) = format::decode_message(&metadata).unwrap() else {
        panic!("expected record batch");
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let error = decode_record_batch(&Buffer::from([]), &batch, schema).unwrap_err();
    assert!(
        error.to_string().contains("beyond the 0-byte message body"),
        "{error}"
    );
}

#[test]
fn validity_bitmap_length_and_null_count_are_checked() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    for (length, null_count, validity, expected_error) in [
        (9, 1, b"".as_slice(), Some("2 bytes")),
        (9, 1, b"\xff".as_slice(), Some("2 bytes")),
        (9, 1, b"\xff\x00".as_slice(), None),
        (9, 1, b"\xff\x01".as_slice(), Some("null_count")),
        (9, 0, b"".as_slice(), None),
        (0, 0, b"".as_slice(), None),
    ] {
        let mut body = Vec::new();
        let buffers = [
            append_test_buffer(&mut body, validity),
            append_test_buffer(&mut body, &vec![0; length * 8]),
        ];
        let rows = i64::try_from(length).unwrap();
        let decoded = decode_body(rows, &[(rows, null_count)], &buffers, body, schema.clone());
        if let Some(expected) = expected_error {
            let error = decoded.unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        } else {
            let decoded = decoded.unwrap();
            assert_eq!(decoded.num_rows(), length);
            assert_eq!(
                decoded.column(0).null_count(),
                usize::try_from(null_count).unwrap()
            );
        }
    }
}

/// Every framing refusal that a `ReadRowsResponse` or a `CreateReadSession` reply can reach,
/// by handing each decoder the other one's valid message, a bare end marker, and the two
/// malformed prefixes the wire allows.
#[test]
fn each_framing_refusal_names_the_message_it_turned_down() {
    let schema = canonical_schema();
    let stream = encode_stream(&schema, &[RecordBatch::new_empty(Arc::clone(&schema))]);
    let (schema_message, batch_message) = schema_and_batch(&stream);

    // A continuation marker where the metadata length belongs: the prefix repeats forever.
    let mut repeated = CONTINUATION_MARKER.to_vec();
    repeated.extend_from_slice(&CONTINUATION_MARKER);
    assert_eq!(
        decode_schema(&repeated).unwrap_err().to_string(),
        "Ipc error: repeated Arrow IPC continuation marker"
    );

    assert_eq!(
        decode_schema(&END_OF_STREAM).unwrap_err().to_string(),
        "Ipc error: BigQuery read-session schema contained only an Arrow IPC end marker"
    );
    assert_eq!(
        decode_schema(batch_message).unwrap_err().to_string(),
        "Ipc error: BigQuery read-session schema contained a record batch"
    );

    // A schema message never has a body, so a declared one means the envelope and the header
    // disagree, and the body bytes would be read as the next message.
    let with_body = frame_message(&format::encode_schema_with_body_fixture(64), &[0; 64]).unwrap();
    assert_eq!(
        decode_schema(&with_body).unwrap_err().to_string(),
        "Ipc error: Arrow IPC schema message unexpectedly has a body"
    );

    // Anything after the schema other than exactly one end marker: here the batch that a
    // caller handing over the whole stream would have left attached.
    let mut trailing = schema_message.to_vec();
    trailing.extend_from_slice(batch_message);
    assert_eq!(
        decode_schema(&trailing).unwrap_err().to_string(),
        "Ipc error: BigQuery read-session schema contains trailing bytes"
    );

    assert_eq!(
        decode_record_batch_message(&Buffer::from(END_OF_STREAM.to_vec()), Arc::clone(&schema))
            .unwrap_err()
            .to_string(),
        "Ipc error: BigQuery Storage Read record batch contained an Arrow IPC end marker"
    );
    assert_eq!(
        decode_record_batch_message(&Buffer::from(schema_message.to_vec()), schema)
            .unwrap_err()
            .to_string(),
        "Ipc error: BigQuery Storage Read record batch contained an Arrow IPC schema message"
    );
}

/// `build_nullable` needs `align_buffers(true)`: an uncompressed buffer is a zero-copy slice of
/// the protobuf frame and may be misaligned, and `ScalarBuffer::from` panics on that. Other
/// reader tests decode 64-byte-aligned bodies, and `benches/ipc.rs` is built with
/// `test = false`, so only this test catches removing the call.
#[test]
fn a_message_at_a_misaligned_base_address_decodes_rather_than_panicking() {
    let schema = canonical_schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        columns.push(new_null_array(field.data_type(), 3));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let encoded = Writer::new().encode_batch(&batch).unwrap();

    // One byte of lead-in, then a slice past it: the base address of every buffer the
    // decoder carves out of the body inherits that odd offset.
    let mut shifted = vec![0_u8; 1];
    shifted.extend_from_slice(&encoded);
    let misaligned = Buffer::from(shifted).slice(1);
    assert_ne!(
        misaligned.as_ptr().align_offset(8),
        0,
        "the shifted message must actually be misaligned"
    );

    assert_eq!(
        &decode_record_batch_message(&misaligned, schema).unwrap(),
        &batch
    );
}

/// A standalone schema may declare a type that no record batch against it can decode.
///
/// The schema decoder accepts every physical type CSV seeds need, but a Storage Read batch
/// decodes only BigQuery's own mappings, so a read session declaring `INT32` parses and then
/// fails on every batch. The error names the type so the caller can find the column.
#[test]
fn a_schema_type_no_batch_can_decode_is_refused_by_name() {
    for data_type in csv_seed_only_types() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )]));
        let decoded = decode_schema(&encode_schema_fixture(&schema)).unwrap();
        assert_eq!(decoded.as_ref(), schema.as_ref());

        let empty = format::Buffer {
            offset: 0,
            length: 0,
        };
        let error = decode_body(0, &[(0, 0)], &[empty], Vec::new(), decoded).unwrap_err();
        assert!(matches!(error, ArrowError::NotYetImplemented(_)), "{error}");
        assert_eq!(
            error.to_string(),
            format!(
                "Not yet implemented: Arrow array type {data_type} is outside the BigQuery \
                 Storage Read type mapping"
            )
        );
    }
}

/// Variadic buffer counts belong to the view types, which are outside the BigQuery mapping,
/// so nothing this driver encodes can produce them. Any count list but an empty one is
/// refused while parsing metadata, before any buffer is read: a negative count as malformed,
/// a valid one as unsupported.
#[test]
fn variadic_buffer_counts_are_refused() {
    for (counts, expected) in [
        (
            &[1][..],
            Some(
                "Not yet implemented: variadic Arrow IPC buffers are outside the BigQuery \
                 Storage Read type mapping",
            ),
        ),
        (
            &[1, -1],
            Some(
                "Parser error: invalid Arrow IPC metadata: negative Arrow IPC variadic \
                 buffer count",
            ),
        ),
        (&[], None),
    ] {
        let encoded =
            frame_message(&format::encode_variadic_record_batch_fixture(counts), &[]).unwrap();
        let decoded =
            decode_record_batch_message(&Buffer::from(encoded), Arc::new(Schema::empty()));
        match expected {
            Some(expected) => assert_eq!(decoded.unwrap_err().to_string(), expected),
            None => assert_eq!(decoded.unwrap().num_rows(), 0),
        }
    }
}

/// The schema message and the first record-batch message of `stream`.
fn schema_and_batch(stream: &[u8]) -> (&[u8], &[u8]) {
    let (schema, rest) = stream.split_at(message_length(stream));
    (schema, &rest[..message_length(rest)])
}

/// Decodes `body` against `schema` under hand-built record-batch metadata.
fn decode_body(
    row_count: i64,
    nodes: &[(i64, i64)],
    buffers: &[format::Buffer],
    body: Vec<u8>,
    schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let nodes = nodes
        .iter()
        .map(|&(length, null_count)| FieldNode { length, null_count })
        .collect::<Vec<_>>();
    let body_length = i64::try_from(body.len()).unwrap();
    let metadata = format::encode_record_batch(row_count, &nodes, buffers, body_length);
    let MessageHeader::RecordBatch(batch) = format::decode_message(&metadata).unwrap() else {
        panic!("expected record batch");
    };
    decode_record_batch(&Buffer::from(body), &batch, schema)
}

fn append_test_buffer(body: &mut Vec<u8>, bytes: &[u8]) -> format::Buffer {
    let offset = i64::try_from(body.len()).unwrap();
    let length = i64::try_from(bytes.len()).unwrap();
    body.extend_from_slice(bytes);
    body.resize(body.len().next_multiple_of(64), 0);
    format::Buffer { offset, length }
}
