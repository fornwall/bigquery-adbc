//! BigQuery-scoped Arrow IPC stream encoding and decoding.
//!
//! This in-tree codec replaces `arrow-ipc`, whose reader it adapts, because linking the
//! general-purpose crate adds about 1.3 MB to the shipped library (`docs/BINARY-SIZE.md`). It
//! covers only what BigQuery puts on the wire: Storage Read schemas and record batches, with or
//! without LZ4 frame compression, and Storage Write's canonical layouts. Standalone schemas also
//! accept dbt 2.0's CSV seed types. Dictionaries, ZSTD and the rest of Arrow IPC are refused.
//!
//! `arrow-ipc` stays a dev-dependency as an independent test oracle: `oracle` exchanges bytes with
//! it in both directions, and a captured BigQuery stream must decode identically under both. The
//! `ipc_*` targets in `fuzz/` exercise the decoders. The codec contains no `unsafe`, which the
//! crate's `deny(unsafe_code)` enforces; FlatBuffers are read with checked byte access.

use arrow_schema::ArrowError;

mod compression;
mod format;
#[cfg(test)]
mod oracle;
pub(crate) mod reader;
pub(crate) mod writer;

#[cfg(feature = "fuzz-shim")]
pub(crate) use compression::decompress_buffer;

/// The four `0xff` bytes that introduce every framed IPC message, and the stream's end marker.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// The eight bytes that end a stream: a continuation marker and a zero metadata length.
#[cfg(test)]
const END_OF_STREAM: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// Encodes a complete schema-only IPC stream for tests, bypassing Storage Write's canonical-type
/// guard because CSV seed schemas accept a wider set of physical Arrow types.
#[cfg(test)]
pub(crate) fn encode_schema_fixture(schema: &arrow_schema::Schema) -> Vec<u8> {
    let mut stream =
        writer::frame_message(&format::encode_schema_fixture(schema).unwrap(), &[]).unwrap();
    stream.extend_from_slice(&END_OF_STREAM);
    stream
}

/// The complete stream this crate's writer produces for `batches` under `schema`.
#[cfg(test)]
fn encode_stream(schema: &arrow_schema::Schema, batches: &[arrow_array::RecordBatch]) -> Vec<u8> {
    let mut writer = writer::Writer::new();
    let mut stream = writer::encode_schema(schema).unwrap();
    for batch in batches {
        stream.extend(writer.encode_batch(batch).unwrap());
    }
    stream.extend_from_slice(&END_OF_STREAM);
    stream
}

/// The byte length of the framed message at the start of `stream`, padding included.
///
/// Both this crate's writer and `arrow-ipc` state the padded metadata length in the continuation
/// prefix and the exact body length in the envelope, so this splits either one's stream.
#[cfg(test)]
fn message_length(stream: &[u8]) -> usize {
    assert_eq!(stream.get(..4), Some(CONTINUATION_MARKER.as_slice()));
    let metadata_length =
        usize::try_from(u32::from_le_bytes(stream[4..8].try_into().unwrap())).unwrap();
    let (_, body_length) =
        format::decode_message_and_body_length(&stream[8..8 + metadata_length]).unwrap();
    8 + metadata_length + usize::try_from(body_length).unwrap()
}

/// Every Arrow type a BigQuery Storage Read result can carry, once, with one nesting layer.
/// Shared by the reader's round trips and the [`oracle`] cross-checks so both cover the same set.
#[cfg(test)]
pub(crate) fn canonical_schema() -> arrow_schema::SchemaRef {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};

    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let nested = Fields::from(vec![
        Field::new("number", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]);
    let fields = vec![
        Field::new("bool", DataType::Boolean, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("float64", DataType::Float64, true),
        Field::new("bytes", DataType::Binary, true),
        Field::new("string", DataType::Utf8, true).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_owned(),
            "google:sqlType:json".to_owned(),
        )])),
        Field::new("date", DataType::Date32, true),
        Field::new("time", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("numeric", DataType::Decimal128(38, 9), true),
        Field::new("bignumeric", DataType::Decimal256(76, 38), true),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new("array", DataType::List(item), true),
        Field::new("struct", DataType::Struct(nested), true),
    ];
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("schema-key".to_owned(), "schema-value".to_owned())]),
    ))
}

#[cold]
#[inline(never)]
fn ipc_error(message: impl Into<String>) -> ArrowError {
    ArrowError::IpcError(message.into())
}
