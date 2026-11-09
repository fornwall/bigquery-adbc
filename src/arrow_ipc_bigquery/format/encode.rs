//! Encoding of the Arrow IPC metadata this driver sends to Storage Write.
//!
//! Everything encoded here was built by `Writer` from an `ArrayData` the driver already checked,
//! so this half validates types rather than offsets: the accepted set is BigQuery's canonical
//! Arrow layouts, widened only under `fixture` to the CSV seed's physical types.

use arrow_schema::{DataType, Field as ArrowField, IntervalUnit as ArrowIntervalUnit};
use arrow_schema::{Schema as ArrowSchema, TimeUnit as ArrowTimeUnit};
use flatbuffers::{FlatBufferBuilder, TableFinishedWIPOffset, WIPOffset};

use crate::types::MAX_SCHEMA_NESTING_DEPTH;

use super::{Buffer, FieldNode, FormatError, slot};

/// Encode an Arrow schema as one V5 `Message` FlatBuffer (without stream framing).
pub(crate) fn encode_schema(schema: &ArrowSchema) -> Result<Vec<u8>, FormatError> {
    encode_schema_with(schema, false)
}

/// Encode the broader scalar schema surface accepted only by CSV seed tests.
#[cfg(test)]
pub(crate) fn encode_schema_fixture(schema: &ArrowSchema) -> Result<Vec<u8>, FormatError> {
    encode_schema_with(schema, true)
}

fn encode_schema_with(schema: &ArrowSchema, fixture: bool) -> Result<Vec<u8>, FormatError> {
    let mut builder = FlatBufferBuilder::new();
    let fields = encode_fields(&mut builder, schema.fields(), fixture)?;
    let metadata = encode_metadata(&mut builder, schema.metadata());

    let start = builder.start_table();
    builder.push_slot_always(slot::SCHEMA_FIELDS, fields);
    if let Some(metadata) = metadata {
        builder.push_slot_always(slot::SCHEMA_METADATA, metadata);
    }
    // Endianness Little is the zero/default value; no features are needed for an uncompressed,
    // dictionary-free Storage Write stream.
    let schema = builder.end_table(start);
    finish_message(&mut builder, 1, schema, 0);
    Ok(builder.finished_data().to_vec())
}

/// Encode one V5 record-batch `Message` FlatBuffer (without stream framing or body bytes).
///
/// The descriptors come from `Writer::push_source`, which makes them valid by construction.
pub(crate) fn encode_record_batch_into(
    builder: &mut FlatBufferBuilder<'static>,
    row_count: i64,
    nodes: &[FieldNode],
    buffers: &[Buffer],
    body_length: i64,
) {
    builder.reset();
    // FieldNode and Buffer are both FlatBuffer structs containing two little-endian i64s and
    // aligned to eight bytes. Building a vector of i64 pairs produces precisely that layout,
    // without an unsafe handwritten `flatbuffers::Push` implementation.
    let encoded_nodes = encode_i64_pairs(
        builder,
        nodes.iter().map(|node| (node.length, node.null_count)),
    );
    let encoded_buffers = encode_i64_pairs(
        builder,
        buffers.iter().map(|buffer| (buffer.offset, buffer.length)),
    );
    let start = builder.start_table();
    builder.push_slot(slot::RECORD_LENGTH, row_count, 0_i64);
    builder.push_slot_always(slot::RECORD_NODES, encoded_nodes);
    builder.push_slot_always(slot::RECORD_BUFFERS, encoded_buffers);
    let batch = builder.end_table(start);
    finish_message(builder, 3, batch, body_length);
}

/// Convenience wrapper for checked metadata fixtures that do not reuse a writer.
#[cfg(test)]
pub(crate) fn encode_record_batch(
    row_count: i64,
    nodes: &[FieldNode],
    buffers: &[Buffer],
    body_length: i64,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    encode_record_batch_into(&mut builder, row_count, nodes, buffers, body_length);
    builder.finished_data().to_vec()
}

/// Builds an empty record-batch message declaring `codec` as its body compression.
///
/// The writer never compresses, so this is the only way to reach the decoder's codec check.
#[cfg(test)]
pub(crate) fn encode_compressed_record_batch_fixture(codec: i8) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = encode_i64_pairs(&mut builder, std::iter::empty());
    let buffers = encode_i64_pairs(&mut builder, std::iter::empty());
    let compression_start = builder.start_table();
    builder.push_slot(slot::COMPRESSION_CODEC, codec, -1_i8);
    let compression = builder.end_table(compression_start);
    let start = builder.start_table();
    builder.push_slot(slot::RECORD_LENGTH, 0_i64, -1_i64);
    builder.push_slot_always(slot::RECORD_NODES, nodes);
    builder.push_slot_always(slot::RECORD_BUFFERS, buffers);
    builder.push_slot_always(slot::RECORD_COMPRESSION, compression);
    let batch = builder.end_table(start);
    finish_message(&mut builder, 3, batch, 0);
    builder.finished_data().to_vec()
}

/// Builds an empty record-batch message declaring `counts` as its variadic buffer counts.
///
/// Nothing this driver encodes is variadic (those types are outside the BigQuery mapping), so
/// this is the only way to reach the decoder's variadic checks.
#[cfg(test)]
pub(crate) fn encode_variadic_record_batch_fixture(counts: &[i64]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = encode_i64_pairs(&mut builder, std::iter::empty());
    let buffers = encode_i64_pairs(&mut builder, std::iter::empty());
    let variadic = builder.create_vector(counts);
    let start = builder.start_table();
    builder.push_slot(slot::RECORD_LENGTH, 0_i64, -1_i64);
    builder.push_slot_always(slot::RECORD_NODES, nodes);
    builder.push_slot_always(slot::RECORD_BUFFERS, buffers);
    builder.push_slot_always(slot::RECORD_VARIADIC_COUNTS, variadic);
    let batch = builder.end_table(start);
    finish_message(&mut builder, 3, batch, 0);
    builder.finished_data().to_vec()
}

/// Builds an empty schema message whose envelope declares a `body_length`-byte body.
///
/// A schema message never has one, so nothing this crate encodes can reach the reader's refusal
/// of a schema that claims to carry body bytes.
#[cfg(test)]
pub(crate) fn encode_schema_with_body_fixture(body_length: i64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let fields = builder.create_vector::<flatbuffers::ForwardsUOffset<TableFinishedWIPOffset>>(&[]);
    let start = builder.start_table();
    builder.push_slot_always(slot::SCHEMA_FIELDS, fields);
    let schema = builder.end_table(start);
    finish_message(&mut builder, 1, schema, body_length);
    builder.finished_data().to_vec()
}

/// Builds the smallest checked dictionary-batch envelope for reader rejection tests.
#[cfg(test)]
pub(crate) fn encode_dictionary_batch_fixture() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let start = builder.start_table();
    let batch = builder.end_table(start);
    finish_message(&mut builder, 2, batch, 0);
    builder.finished_data().to_vec()
}

fn finish_message(
    builder: &mut FlatBufferBuilder<'_>,
    header_type: u8,
    header: WIPOffset<TableFinishedWIPOffset>,
    body_length: i64,
) {
    let start = builder.start_table();
    builder.push_slot(slot::MESSAGE_VERSION, 4_i16, 0_i16);
    builder.push_slot(slot::MESSAGE_HEADER_TYPE, header_type, 0_u8);
    builder.push_slot_always(slot::MESSAGE_HEADER, header.as_union_value());
    builder.push_slot(slot::MESSAGE_BODY_LENGTH, body_length, 0_i64);
    let message = builder.end_table(start);
    builder.finish_minimal(message);
}

fn encode_i64_pairs<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    pairs: impl ExactSizeIterator<Item = (i64, i64)> + DoubleEndedIterator,
) -> WIPOffset<flatbuffers::Vector<'a, i64>> {
    let pair_count = pairs.len();
    builder.start_vector::<i64>(pair_count * 2);
    for (first, second) in pairs.rev() {
        builder.push(second);
        builder.push(first);
    }
    // The wire vector's element is the 16-byte struct, even though the builder was told about
    // twice as many i64s to get the same byte width and alignment.
    builder.end_vector(pair_count)
}

fn encode_fields<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    fields: &arrow_schema::Fields,
    fixture: bool,
) -> Result<
    WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TableFinishedWIPOffset>>>,
    FormatError,
> {
    let offsets = fields
        .iter()
        .map(|field| encode_field(builder, field, 0, fixture))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(builder.create_vector(&offsets))
}

fn encode_field<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    field: &ArrowField,
    depth: usize,
    fixture: bool,
) -> Result<WIPOffset<TableFinishedWIPOffset>, FormatError> {
    if depth >= MAX_SCHEMA_NESTING_DEPTH {
        return Err(FormatError::invalid(format!(
            "Arrow schema nesting exceeds the driver limit of {MAX_SCHEMA_NESTING_DEPTH} levels"
        )));
    }
    let data_type = field.data_type();
    let (type_tag, type_table) = encode_type(builder, data_type, fixture)?;

    let child_offsets = match data_type {
        DataType::List(child) => vec![encode_field(builder, child, depth + 1, fixture)?],
        DataType::Struct(children) => children
            .iter()
            .map(|child| encode_field(builder, child, depth + 1, fixture))
            .collect::<Result<Vec<_>, _>>()?,
        _ => Vec::new(),
    };
    let children = builder.create_vector(&child_offsets);
    let metadata = encode_metadata(builder, field.metadata());
    let name = builder.create_string(field.name());

    let start = builder.start_table();
    builder.push_slot_always(slot::FIELD_NAME, name);
    builder.push_slot(slot::FIELD_NULLABLE, field.is_nullable(), false);
    builder.push_slot(slot::FIELD_TYPE_TYPE, type_tag, 0_u8);
    builder.push_slot_always(slot::FIELD_TYPE, type_table.as_union_value());
    builder.push_slot_always(slot::FIELD_CHILDREN, children);
    if let Some(metadata) = metadata {
        builder.push_slot_always(slot::FIELD_METADATA, metadata);
    }
    Ok(builder.end_table(start))
}

/// `fixture` widens the accepted set to the CSV seed's physical types; production encoding stays
/// restricted to BigQuery's canonical Arrow layouts.
fn encode_type<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    data_type: &DataType,
    fixture: bool,
) -> Result<(u8, WIPOffset<TableFinishedWIPOffset>), FormatError> {
    Ok(match data_type {
        DataType::Int8 if fixture => (2, int_table(builder, 8, true)),
        DataType::Int16 if fixture => (2, int_table(builder, 16, true)),
        DataType::Int32 if fixture => (2, int_table(builder, 32, true)),
        DataType::Int64 => (2, int_table(builder, 64, true)),
        DataType::UInt8 if fixture => (2, int_table(builder, 8, false)),
        DataType::UInt16 if fixture => (2, int_table(builder, 16, false)),
        DataType::UInt32 if fixture => (2, int_table(builder, 32, false)),
        DataType::UInt64 if fixture => (2, int_table(builder, 64, false)),
        DataType::Float16 if fixture => (3, one_i16_table(builder, 0)),
        DataType::Float32 if fixture => (3, one_i16_table(builder, 1)),
        DataType::Float64 => (3, one_i16_table(builder, 2)),
        DataType::Binary => (4, empty_table(builder)),
        DataType::Utf8 => (5, empty_table(builder)),
        DataType::Boolean => (6, empty_table(builder)),
        DataType::Decimal32(precision, scale) if fixture => {
            (7, decimal_table(builder, *precision, *scale, 32))
        }
        DataType::Decimal64(precision, scale) if fixture => {
            (7, decimal_table(builder, *precision, *scale, 64))
        }
        DataType::Decimal128(precision, scale) => {
            (7, decimal_table(builder, *precision, *scale, 128))
        }
        DataType::Decimal256(precision, scale) => {
            (7, decimal_table(builder, *precision, *scale, 256))
        }
        DataType::Date32 => (8, one_i16_table(builder, 0)),
        DataType::Date64 if fixture => (8, one_i16_table(builder, 1)),
        DataType::Time32(unit) if fixture => (9, time_table(builder, *unit, 32)),
        DataType::Time64(unit) if fixture || *unit == ArrowTimeUnit::Microsecond => {
            (9, time_table(builder, *unit, 64))
        }
        DataType::Timestamp(unit, timezone) if fixture || *unit == ArrowTimeUnit::Microsecond => {
            let timezone = timezone
                .as_ref()
                .map(|timezone| builder.create_string(timezone));
            let start = builder.start_table();
            builder.push_slot(slot::TYPE_VALUE_0, encode_time_unit(*unit), 0_i16);
            if let Some(timezone) = timezone {
                builder.push_slot_always(slot::TYPE_VALUE_1, timezone);
            }
            (10, builder.end_table(start))
        }
        DataType::Interval(ArrowIntervalUnit::YearMonth) => (11, one_i16_table(builder, 0)),
        DataType::Interval(ArrowIntervalUnit::DayTime) if fixture => {
            (11, one_i16_table(builder, 1))
        }
        DataType::Interval(ArrowIntervalUnit::MonthDayNano) => (11, one_i16_table(builder, 2)),
        DataType::List(_) => (12, empty_table(builder)),
        DataType::Struct(_) => (13, empty_table(builder)),
        DataType::FixedSizeBinary(width) if fixture => {
            let start = builder.start_table();
            builder.push_slot(slot::TYPE_VALUE_0, *width, 0_i32);
            (15, builder.end_table(start))
        }
        DataType::LargeBinary if fixture => (19, empty_table(builder)),
        DataType::LargeUtf8 if fixture => (20, empty_table(builder)),
        DataType::BinaryView if fixture => (23, empty_table(builder)),
        DataType::Utf8View if fixture => (24, empty_table(builder)),
        // Ingest normalization expands dictionaries before they reach the Storage Write encoder,
        // which never emits dictionary batches.
        other => return Err(FormatError::Unsupported(noncanonical_type(other))),
    })
}

/// How both the schema encoder and the batch planner refuse a type outside BigQuery's canonical
/// ingest layouts, so the caller sees the same words whichever one notices first.
pub(crate) fn noncanonical_type(data_type: &DataType) -> String {
    format!("Arrow IPC encoding of noncanonical BigQuery ingest type {data_type}")
}

fn empty_table(builder: &mut FlatBufferBuilder<'_>) -> WIPOffset<TableFinishedWIPOffset> {
    let start = builder.start_table();
    builder.end_table(start)
}

fn int_table(
    builder: &mut FlatBufferBuilder<'_>,
    bit_width: i32,
    is_signed: bool,
) -> WIPOffset<TableFinishedWIPOffset> {
    let start = builder.start_table();
    builder.push_slot(slot::TYPE_VALUE_0, bit_width, 0_i32);
    builder.push_slot(slot::TYPE_VALUE_1, is_signed, false);
    builder.end_table(start)
}

fn decimal_table(
    builder: &mut FlatBufferBuilder<'_>,
    precision: u8,
    scale: i8,
    bit_width: i32,
) -> WIPOffset<TableFinishedWIPOffset> {
    let start = builder.start_table();
    builder.push_slot(slot::TYPE_VALUE_0, i32::from(precision), 0_i32);
    builder.push_slot(slot::TYPE_VALUE_1, i32::from(scale), 0_i32);
    builder.push_slot(slot::TYPE_VALUE_2, bit_width, 128_i32);
    builder.end_table(start)
}

fn time_table(
    builder: &mut FlatBufferBuilder<'_>,
    unit: ArrowTimeUnit,
    bit_width: i32,
) -> WIPOffset<TableFinishedWIPOffset> {
    let start = builder.start_table();
    builder.push_slot(slot::TYPE_VALUE_0, encode_time_unit(unit), 1_i16);
    builder.push_slot(slot::TYPE_VALUE_1, bit_width, 32_i32);
    builder.end_table(start)
}

fn one_i16_table(
    builder: &mut FlatBufferBuilder<'_>,
    value: i16,
) -> WIPOffset<TableFinishedWIPOffset> {
    let start = builder.start_table();
    // Date and Duration default this physical slot to MILLISECOND (1), while the other
    // one-i16 tables default to zero. Storing it unconditionally is correct for all of them.
    builder.push_slot_always(slot::TYPE_VALUE_0, value);
    builder.end_table(start)
}

fn encode_time_unit(unit: ArrowTimeUnit) -> i16 {
    match unit {
        ArrowTimeUnit::Second => 0,
        ArrowTimeUnit::Millisecond => 1,
        ArrowTimeUnit::Microsecond => 2,
        ArrowTimeUnit::Nanosecond => 3,
    }
}

fn encode_metadata<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    metadata: &arrow_schema::Metadata,
) -> Option<WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TableFinishedWIPOffset>>>>
{
    if metadata.is_empty() {
        return None;
    }
    let mut offsets = Vec::with_capacity(metadata.len());
    for (key, value) in metadata {
        let key = builder.create_string(key);
        let value = builder.create_string(value);
        let start = builder.start_table();
        builder.push_slot_always(slot::KEY_VALUE_KEY, key);
        builder.push_slot_always(slot::KEY_VALUE_VALUE, value);
        offsets.push(builder.end_table(start));
    }
    Some(builder.create_vector(&offsets))
}
