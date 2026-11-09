use flatbuffers::FlatBufferBuilder;

use super::*;
use crate::arrow_ipc_bigquery::format::encode_compressed_record_batch_fixture;

// Captured from arrow-ipc 59.3.0. This independently pins the FlatBuffer slots and
// discriminants needed to decode `Schema<id: Int64 not null>`.
const ORACLE_INT64_SCHEMA: &[u8] = &[
    16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 10, 0, 9, 0, 4, 0, 10, 0, 0, 0, 16, 0, 0, 0, 0, 1, 4, 0, 8, 0,
    8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0,
    15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 2, 28, 0, 0, 0, 8, 0,
    12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 105, 100, 0,
    0,
];
const ORACLE_INT64_BATCH: &[u8] = &[
    16, 0, 0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0, 32, 0, 0, 0, 128, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 44, 0, 0,
    0, 16, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 64,
    0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0,
];

#[test]
fn oracle_schema_metadata_decodes() {
    let decoded = decode_message(ORACLE_INT64_SCHEMA).unwrap();
    let MessageHeader::Schema(decoded) = decoded else {
        panic!("expected schema message");
    };
    let [field] = decoded.fields.as_slice() else {
        panic!("expected one field");
    };
    assert_eq!(field.name, "id");
    assert!(!field.nullable);
    assert_eq!(
        field.data_type,
        IpcType::Int {
            bit_width: 64,
            is_signed: true,
        }
    );
}

#[test]
fn oracle_record_batch_metadata_decodes() {
    let (decoded, body_length) = decode_message_and_body_length(ORACLE_INT64_BATCH).unwrap();
    assert_eq!(body_length, 128);
    let MessageHeader::RecordBatch(decoded) = decoded else {
        panic!("expected record-batch message");
    };
    assert_eq!(decoded.length, 2);
    assert_eq!(decoded.nodes.iter().collect::<Vec<_>>(), [(2, 0)]);
    assert_eq!(
        decoded.buffers.iter().collect::<Vec<_>>(),
        [(0, 1), (64, 16)]
    );
    // No `compression` table at all means raw buffers.
    assert!(!decoded.lz4_compressed);
}

/// LZ4_FRAME is the only codec `create_read_session` requests and the only one decoded, so
/// every other codec is refused while parsing metadata.
#[test]
fn only_the_lz4_frame_compression_codec_is_accepted() {
    let lz4 = encode_compressed_record_batch_fixture(0);
    let MessageHeader::RecordBatch(decoded) = decode_message(&lz4).unwrap() else {
        panic!("expected record-batch message");
    };
    assert!(decoded.lz4_compressed);

    // 1 is ZSTD; 2 and above are codecs no Arrow release has assigned.
    for codec in [1_i8, 2, 127, -2] {
        let refused = decode_message(&encode_compressed_record_batch_fixture(codec)).unwrap_err();
        assert_eq!(
            refused.to_string(),
            format!("unsupported Arrow IPC compression codec {codec}")
        );
    }
}

/// `parse_fields`, the reader's `decode_field` and the encoder's `encode_field` recurse once per
/// nesting level of a server- or caller-supplied schema, and a stack overflow aborts the host
/// process instead of unwinding through the FFI guard. This pins where `MAX_SCHEMA_NESTING_DEPTH`
/// cuts off on both sides. The REST counterpart is
/// `types::schema::tests::a_result_schema_nested_past_the_supported_depth_is_refused_rather_than_recursed_into`.
#[test]
fn a_schema_nested_past_the_driver_limit_is_refused_rather_than_recursed_into() {
    use crate::arrow_ipc_bigquery::format::{encode_schema, encode_schema_fixture};

    let deepest = encode_schema(&nested_schema(MAX_SCHEMA_NESTING_DEPTH)).unwrap();
    assert!(matches!(
        decode_message(&deepest).unwrap(),
        MessageHeader::Schema(_)
    ));

    let too_deep = nested_schema(MAX_SCHEMA_NESTING_DEPTH + 1);
    assert_eq!(
        encode_schema(&too_deep).unwrap_err().to_string(),
        format!(
            "Arrow schema nesting exceeds the driver limit of {MAX_SCHEMA_NESTING_DEPTH} levels"
        )
    );
    // The fixture encoder widens the accepted types, not the depth bound.
    assert!(encode_schema_fixture(&too_deep).is_err());
    // So the over-limit bytes come from `arrow-ipc`.
    assert_eq!(
        decode_message(&arrow_ipc_schema_metadata(&too_deep))
            .unwrap_err()
            .to_string(),
        format!(
            "Arrow IPC schema nesting exceeds the driver limit of \
             {MAX_SCHEMA_NESTING_DEPTH} levels"
        )
    );
}

/// A schema whose single field is `depth` levels of `STRUCT` ending in a `STRING` leaf.
fn nested_schema(depth: usize) -> arrow_schema::Schema {
    use arrow_schema::{DataType, Field, Fields};

    let mut field = Field::new("leaf", DataType::Utf8, true);
    for level in (1..depth).rev() {
        field = Field::new(
            format!("level{level}"),
            DataType::Struct(Fields::from(vec![field])),
            true,
        );
    }
    arrow_schema::Schema::new(vec![field])
}

/// The `Message` FlatBuffer `arrow-ipc` writes for `schema`, unwrapped from its stream frame.
fn arrow_ipc_schema_metadata(schema: &arrow_schema::Schema) -> Vec<u8> {
    let stream = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), schema)
        .unwrap()
        .into_inner()
        .unwrap();
    let metadata_length =
        usize::try_from(u32::from_le_bytes(stream[4..8].try_into().unwrap())).unwrap();
    stream[8..8 + metadata_length].to_vec()
}

/// Finishes a `Message` around a schema listing `fields`.
fn schema_message(
    builder: &mut FlatBufferBuilder<'static>,
    fields: &[flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>],
) -> Vec<u8> {
    let fields = builder.create_vector(fields);
    let start = builder.start_table();
    builder.push_slot_always(slot::SCHEMA_FIELDS, fields);
    let schema = builder.end_table(start);
    let start = builder.start_table();
    builder.push_slot(slot::MESSAGE_VERSION, 4_i16, 0);
    builder.push_slot(slot::MESSAGE_HEADER_TYPE, 1_u8, 0);
    builder.push_slot_always(slot::MESSAGE_HEADER, schema);
    let message = builder.end_table(start);
    builder.finish_minimal(message);
    builder.finished_data().to_vec()
}

/// Each level's children vector lists the level below eight times, so 20 levels in about a
/// kilobyte describe 8^20 fields. Without the expansion budget this would not finish.
#[test]
fn aliased_field_offsets_cannot_expand_a_small_schema_exponentially() {
    let mut builder = FlatBufferBuilder::new();
    let empty_type = builder.start_table();
    let empty_type = builder.end_table(empty_type);
    let mut field = None;
    for _ in 0..20 {
        let children = field.map(|field| builder.create_vector(&[field; 8]));
        let start = builder.start_table();
        builder.push_slot(slot::FIELD_TYPE_TYPE, 13_u8, 0);
        builder.push_slot_always(slot::FIELD_TYPE, empty_type);
        if let Some(children) = children {
            builder.push_slot_always(slot::FIELD_CHILDREN, children);
        }
        field = Some(builder.end_table(start));
    }
    let message = schema_message(&mut builder, &[field.unwrap()]);

    let error = decode_message(&message).unwrap_err();
    assert!(
        error.to_string().contains("aliased FlatBuffer offsets"),
        "{error}"
    );
}

/// A schema whose fields vector lists one field table `copies` times, the field built by
/// `field` around one `length`-byte string.
fn aliased_schema(
    copies: usize,
    length: usize,
    field: impl FnOnce(
        &mut FlatBufferBuilder<'static>,
        flatbuffers::WIPOffset<&str>,
    ) -> flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let long = builder.create_string(&"x".repeat(length));
    let field = field(&mut builder, long);
    schema_message(&mut builder, &vec![field; copies])
}

/// A field table of `tag` with an empty type table and the given name, metadata and type.
fn field_table(
    builder: &mut FlatBufferBuilder<'static>,
    tag: u8,
    name: Option<flatbuffers::WIPOffset<&str>>,
    timezone: Option<flatbuffers::WIPOffset<&str>>,
    key: Option<flatbuffers::WIPOffset<&str>>,
) -> flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let start = builder.start_table();
    if let Some(timezone) = timezone {
        builder.push_slot_always(slot::TYPE_VALUE_1, timezone);
    }
    let data_type = builder.end_table(start);
    let metadata = key.map(|key| {
        let start = builder.start_table();
        builder.push_slot_always(slot::KEY_VALUE_KEY, key);
        let entry = builder.end_table(start);
        builder.create_vector(&[entry])
    });
    let start = builder.start_table();
    if let Some(name) = name {
        builder.push_slot_always(slot::FIELD_NAME, name);
    }
    builder.push_slot(slot::FIELD_TYPE_TYPE, tag, 0);
    builder.push_slot_always(slot::FIELD_TYPE, data_type);
    if let Some(metadata) = metadata {
        builder.push_slot_always(slot::FIELD_METADATA, metadata);
    }
    builder.end_table(start)
}

/// Every alias of a field table can name the same long string, which each alias would
/// validate and the reader would copy again: 4,096 aliases of a 64 KiB name in about 80 KiB
/// would cost 256 MiB. The budget charges string bytes as well as entries, for field names,
/// metadata keys and timestamp zones alike.
#[test]
fn aliased_strings_cannot_expand_a_schema_quadratically() {
    let (copies, length) = (4096, 64 << 10);
    let schemas = [
        aliased_schema(copies, length, |builder, long| {
            field_table(builder, 5, Some(long), None, None)
        }),
        aliased_schema(copies, length, |builder, long| {
            field_table(builder, 5, None, None, Some(long))
        }),
        aliased_schema(copies, length, |builder, long| {
            field_table(builder, 10, None, Some(long), None)
        }),
    ];
    for schema in &schemas {
        // Not `unwrap_err`, whose panic would print every copied name.
        let Err(error) = decode_message(schema) else {
            panic!("a schema of aliased long strings decoded");
        };
        assert!(
            error.to_string().contains("aliased FlatBuffer offsets"),
            "{error}"
        );
    }

    // Listed once, a field naming one shared long string as its name, zone and metadata key,
    // as a writer sharing strings might, stays within the budget.
    let honest = aliased_schema(1, length, |builder, long| {
        field_table(builder, 10, Some(long), Some(long), Some(long))
    });
    assert!(matches!(
        decode_message(&honest).unwrap(),
        MessageHeader::Schema(_)
    ));
}

/// A writer that shares repeated strings, as `create_shared_string` does, stores each once
/// but has every field name it: 1,000 JSON columns naming one shared extension key and value
/// must still decode.
#[test]
fn a_schema_sharing_its_repeated_strings_decodes() {
    let mut builder = FlatBufferBuilder::new();
    let key = builder.create_shared_string("ARROW:extension:name");
    let value = builder.create_shared_string("google:sqlType:json");
    let fields: Vec<_> = (0..1000)
        .map(|index| {
            let name = builder.create_string(&format!("c{index}"));
            let start = builder.start_table();
            builder.push_slot_always(slot::KEY_VALUE_KEY, key);
            builder.push_slot_always(slot::KEY_VALUE_VALUE, value);
            let entry = builder.end_table(start);
            let metadata = builder.create_vector(&[entry]);
            let start = builder.start_table();
            let data_type = builder.end_table(start);
            let start = builder.start_table();
            builder.push_slot_always(slot::FIELD_NAME, name);
            builder.push_slot(slot::FIELD_TYPE_TYPE, 5_u8, 0);
            builder.push_slot_always(slot::FIELD_TYPE, data_type);
            builder.push_slot_always(slot::FIELD_METADATA, metadata);
            builder.end_table(start)
        })
        .collect();
    let message = schema_message(&mut builder, &fields);

    let MessageHeader::Schema(schema) = decode_message(&message).unwrap() else {
        panic!("expected schema message");
    };
    assert_eq!(schema.fields.len(), 1000);
}

/// No caller reads a message's own custom metadata, so a batch whose metadata aliases one long
/// key thousands of times decodes without that walk: it would otherwise run on every batch.
#[test]
fn message_custom_metadata_is_not_walked() {
    let mut builder = FlatBufferBuilder::new();
    let key = builder.create_string(&"k".repeat(64 << 10));
    let start = builder.start_table();
    builder.push_slot_always(slot::KEY_VALUE_KEY, key);
    let entry = builder.end_table(start);
    let metadata = builder.create_vector(&vec![entry; 4096]);
    let nodes = builder.create_vector::<i64>(&[]);
    let buffers = builder.create_vector::<i64>(&[]);
    let start = builder.start_table();
    builder.push_slot_always(slot::RECORD_NODES, nodes);
    builder.push_slot_always(slot::RECORD_BUFFERS, buffers);
    let batch = builder.end_table(start);
    let start = builder.start_table();
    builder.push_slot(slot::MESSAGE_VERSION, 4_i16, 0);
    builder.push_slot(slot::MESSAGE_HEADER_TYPE, 3_u8, 0);
    builder.push_slot_always(slot::MESSAGE_HEADER, batch);
    builder.push_slot_always(slot::MESSAGE_METADATA, metadata);
    let message = builder.end_table(start);
    builder.finish_minimal(message);

    assert!(matches!(
        decode_message(builder.finished_data()).unwrap(),
        MessageHeader::RecordBatch(_)
    ));
}

#[test]
fn malformed_offsets_are_rejected_without_panicking() {
    let corruptions = [
        Vec::new(),
        vec![0xff; 4],
        vec![4, 0, 0, 0, 0xff, 0xff, 0xff, 0x7f],
        vec![4, 0, 0, 0, 4, 0, 4, 0],
    ];
    for bytes in &corruptions {
        assert!(decode_message(bytes).is_err());
    }
}
