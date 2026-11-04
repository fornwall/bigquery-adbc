// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Arrow Rust's IPC reader for BigQuery Storage Read.

//! Arrow IPC stream reader for BigQuery Storage Read.
//!
//! Record batches support BigQuery's documented Storage Read types and LZ4 frame buffer
//! compression. Standalone schemas also accept the scalar physical types used by dbt-core 2.0
//! CSV seeds. Extend this subset only when required for BigQuery compatibility.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions, make_array};
use arrow_buffer::Buffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{
    ArrowError, DataType, Field, Fields, IntervalUnit as ArrowIntervalUnit, Schema, SchemaRef,
    TimeUnit as ArrowTimeUnit,
};

use super::compression::decompress_buffer;
use super::format::{
    self, DateUnit, FormatError, I64Pairs, IntervalUnit, IpcType, MessageHeader, Precision,
    RecordBatchMessage, TimeUnit,
};
use super::ipc_error;

#[derive(Debug)]
struct FramedMessage<'a> {
    header: MessageHeader<'a>,
    body_start: usize,
    body_length: usize,
    length: usize,
}

#[derive(Debug)]
enum Frame<'a> {
    Message(FramedMessage<'a>),
    End { length: usize },
}

/// Splits one complete IPC message from a BigQuery protobuf bytes field without copying it.
fn parse_frame(encoded: &[u8]) -> Result<Frame<'_>, ArrowError> {
    let first = encoded
        .get(..4)
        .ok_or_else(|| ipc_error("truncated Arrow IPC message prefix"))?;
    let first = u32::from_le_bytes(first.try_into().expect("four-byte slice"));
    let (prefix_length, metadata_length) = if first == u32::MAX {
        let length = encoded
            .get(4..8)
            .ok_or_else(|| ipc_error("truncated Arrow IPC continuation prefix"))?;
        let length = u32::from_le_bytes(length.try_into().expect("four-byte slice"));
        if length == u32::MAX {
            return Err(ipc_error("repeated Arrow IPC continuation marker"));
        }
        (8_usize, length)
    } else {
        (4_usize, first)
    };
    if metadata_length == 0 {
        return Ok(Frame::End {
            length: prefix_length,
        });
    }

    let metadata_length = usize::try_from(metadata_length).map_err(|_| {
        ipc_error(format!(
            "Arrow IPC metadata length {metadata_length} does not fit in usize"
        ))
    })?;
    let metadata_end = prefix_length
        .checked_add(metadata_length)
        .ok_or_else(|| ipc_error("Arrow IPC metadata length overflow"))?;
    let metadata = encoded.get(prefix_length..metadata_end).ok_or_else(|| {
        ipc_error(format!(
            "truncated Arrow IPC metadata: expected {metadata_length} bytes"
        ))
    })?;
    let (header, body_length) =
        format::decode_message_and_body_length(metadata).map_err(|source| match source {
            FormatError::Unsupported(message) => ArrowError::NotYetImplemented(message),
            FormatError::Invalid(message) => {
                ArrowError::ParseError(format!("invalid Arrow IPC metadata: {message}"))
            }
        })?;
    let body_length = usize::try_from(body_length).map_err(|_| {
        ipc_error(format!(
            "invalid Arrow IPC message body length {body_length}"
        ))
    })?;
    let length = metadata_end
        .checked_add(body_length)
        .ok_or_else(|| ipc_error("Arrow IPC message length overflow"))?;
    if length > encoded.len() {
        return Err(ipc_error(format!(
            "truncated Arrow IPC body: expected {body_length} bytes"
        )));
    }
    Ok(Frame::Message(FramedMessage {
        header,
        body_start: metadata_end,
        body_length,
        length,
    }))
}

/// Decodes the standalone stream schema returned by `CreateReadSession`.
pub(crate) fn decode_schema(encoded: &[u8]) -> Result<SchemaRef, ArrowError> {
    let Frame::Message(frame) = parse_frame(encoded)? else {
        return Err(ipc_error(
            "BigQuery read-session schema contained only an Arrow IPC end marker",
        ));
    };
    let schema = match frame.header {
        MessageHeader::Schema(schema) => {
            if frame.body_length != 0 {
                return Err(ipc_error(
                    "Arrow IPC schema message unexpectedly has a body",
                ));
            }
            Arc::new(decode_arrow_schema(schema)?)
        }
        MessageHeader::RecordBatch(_) => {
            return Err(ipc_error(
                "BigQuery read-session schema contained a record batch",
            ));
        }
    };

    let trailing = encoded
        .get(frame.length..)
        .ok_or_else(|| ipc_error("invalid Arrow IPC schema message length"))?;
    if !trailing.is_empty() {
        match parse_frame(trailing)? {
            Frame::End { length } if length == trailing.len() => {}
            _ => {
                return Err(ipc_error(
                    "BigQuery read-session schema contains trailing bytes",
                ));
            }
        }
    }
    Ok(schema)
}

/// Decodes the one complete record-batch message carried by a BigQuery `ReadRowsResponse`.
pub(crate) fn decode_record_batch_message(
    encoded: &Buffer,
    schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let Frame::Message(frame) = parse_frame(encoded)? else {
        return Err(ipc_error(
            "BigQuery Storage Read record batch contained an Arrow IPC end marker",
        ));
    };
    if frame.length != encoded.len() {
        return Err(ipc_error(format!(
            "BigQuery Storage Read record batch contains {} trailing Arrow IPC bytes",
            encoded.len() - frame.length
        )));
    }
    let body = encoded.slice_with_length(frame.body_start, frame.body_length);
    match frame.header {
        MessageHeader::RecordBatch(batch) => decode_record_batch(&body, &batch, schema),
        MessageHeader::Schema(_) => Err(ipc_error(
            "BigQuery Storage Read record batch contained an Arrow IPC schema message",
        )),
    }
}

fn decode_arrow_schema(encoded: format::SchemaMessage<'_>) -> Result<Schema, ArrowError> {
    let fields = encoded
        .fields
        .into_iter()
        .map(decode_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        decode_metadata(encoded.metadata),
    ))
}

fn decode_field(encoded: format::Field<'_>) -> Result<Field, ArrowError> {
    let children = encoded
        .children
        .into_iter()
        .map(decode_field)
        .collect::<Result<Vec<_>, _>>()?;
    let data_type = decode_data_type(encoded.data_type, children)?;
    Ok(Field::new(encoded.name, data_type, encoded.nullable)
        .with_metadata(decode_metadata(encoded.metadata)))
}

/// Builds the Arrow type for one parsed IPC field from its already-decoded children, whose count
/// the format parser has already validated for the type.
fn decode_data_type(encoded: IpcType<'_>, children: Vec<Field>) -> Result<DataType, ArrowError> {
    let data_type = match encoded {
        IpcType::List => {
            let [child] = <[Field; 1]>::try_from(children).unwrap_or_else(|children| {
                unreachable!(
                    "format::validate_child_count admits a List only with exactly one child, got {}",
                    children.len()
                )
            });
            return Ok(DataType::List(Arc::new(child)));
        }
        IpcType::Struct => return Ok(DataType::Struct(Fields::from(children))),
        IpcType::Bool => DataType::Boolean,
        IpcType::Int {
            bit_width,
            is_signed,
        } => match (bit_width, is_signed) {
            (8, true) => DataType::Int8,
            (16, true) => DataType::Int16,
            (32, true) => DataType::Int32,
            (64, true) => DataType::Int64,
            (8, false) => DataType::UInt8,
            (16, false) => DataType::UInt16,
            (32, false) => DataType::UInt32,
            (64, false) => DataType::UInt64,
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "unsupported Arrow IPC integer bit width {bit_width}"
                )));
            }
        },
        IpcType::FloatingPoint(precision) => match precision {
            Precision::Half => DataType::Float16,
            Precision::Single => DataType::Float32,
            Precision::Double => DataType::Float64,
        },
        IpcType::Binary => DataType::Binary,
        IpcType::LargeBinary => DataType::LargeBinary,
        IpcType::BinaryView => DataType::BinaryView,
        IpcType::FixedSizeBinary(width) => DataType::FixedSizeBinary(width),
        IpcType::Utf8 => DataType::Utf8,
        IpcType::LargeUtf8 => DataType::LargeUtf8,
        IpcType::Utf8View => DataType::Utf8View,
        IpcType::Date(DateUnit::Day) => DataType::Date32,
        IpcType::Date(DateUnit::Millisecond) => DataType::Date64,
        IpcType::Time { unit, bit_width } => match (unit, bit_width) {
            (TimeUnit::Second, 32) => DataType::Time32(ArrowTimeUnit::Second),
            (TimeUnit::Millisecond, 32) => DataType::Time32(ArrowTimeUnit::Millisecond),
            (TimeUnit::Microsecond, 64) => DataType::Time64(ArrowTimeUnit::Microsecond),
            (TimeUnit::Nanosecond, 64) => DataType::Time64(ArrowTimeUnit::Nanosecond),
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "invalid Arrow IPC time unit {unit:?} for bit width {bit_width}"
                )));
            }
        },
        IpcType::Timestamp { unit, timezone } => DataType::Timestamp(
            match unit {
                TimeUnit::Second => ArrowTimeUnit::Second,
                TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
                TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
                TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
            },
            timezone.map(Into::into),
        ),
        IpcType::Decimal {
            precision,
            scale,
            bit_width,
        } => {
            let precision = u8::try_from(precision).map_err(|_| {
                ArrowError::SchemaError(format!(
                    "Arrow IPC decimal precision {precision} does not fit in u8"
                ))
            })?;
            let scale = i8::try_from(scale).map_err(|_| {
                ArrowError::SchemaError(format!(
                    "Arrow IPC decimal scale {scale} does not fit in i8"
                ))
            })?;
            match bit_width {
                32 => DataType::Decimal32(precision, scale),
                64 => DataType::Decimal64(precision, scale),
                128 => DataType::Decimal128(precision, scale),
                256 => DataType::Decimal256(precision, scale),
                _ => {
                    return Err(ArrowError::SchemaError(format!(
                        "unsupported Arrow IPC decimal bit width {bit_width}"
                    )));
                }
            }
        }
        IpcType::Interval(unit) => DataType::Interval(match unit {
            IntervalUnit::YearMonth => ArrowIntervalUnit::YearMonth,
            IntervalUnit::DayTime => ArrowIntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => ArrowIntervalUnit::MonthDayNano,
        }),
    };
    Ok(data_type)
}

fn decode_metadata(encoded: Vec<format::KeyValue<'_>>) -> HashMap<String, String> {
    encoded
        .into_iter()
        .filter_map(|entry| {
            entry
                .value
                .map(|value| (entry.key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn decode_record_batch(
    body: &Buffer,
    encoded: &RecordBatchMessage<'_>,
    schema: SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let row_count = usize::try_from(encoded.length).map_err(|_| {
        ipc_error(format!(
            "invalid Arrow IPC record batch length {}",
            encoded.length
        ))
    })?;
    let mut decoder = BatchDecoder {
        body,
        nodes: encoded.nodes,
        node_index: 0,
        buffers: encoded.buffers,
        buffer_index: 0,
        lz4_compressed: encoded.lz4_compressed,
    };
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(make_array(decoder.decode_array_data(field)?));
    }
    // The schema is the one the read session declared, so a leftover node or buffer means the
    // batch describes more arrays than the schema; the error attributes each count to its side.
    if decoder.node_index != decoder.nodes.len() {
        return Err(ipc_error(format!(
            "Arrow IPC record batch declares {} field nodes, but decoding it against this result's schema consumed only {}",
            decoder.nodes.len(),
            decoder.node_index
        )));
    }
    if decoder.buffer_index != decoder.buffers.len() {
        return Err(ipc_error(format!(
            "Arrow IPC record batch declares {} buffers, but decoding it against this result's schema consumed only {}",
            decoder.buffers.len(),
            decoder.buffer_index
        )));
    }

    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
}

struct BatchDecoder<'a> {
    body: &'a Buffer,
    nodes: I64Pairs<'a>,
    node_index: usize,
    buffers: I64Pairs<'a>,
    buffer_index: usize,
    /// Whether every buffer in the body carries an LZ4 frame after its length prefix; the
    /// metadata parser accepts no other codec.
    lz4_compressed: bool,
}

impl BatchDecoder<'_> {
    // Children stay validated `ArrayData` until the whole column is materialized once. Building
    // an `ArrayRef` per child would make every ancestor convert it back and rebuild its wrappers.
    fn decode_array_data(&mut self, field: &Field) -> Result<ArrayData, ArrowError> {
        let (length, null_count) = self.next_node(field)?;
        let data_type = field.data_type();
        match data_type {
            DataType::Utf8 | DataType::Binary => {
                let validity = self.next_buffer(field)?;
                let offsets = self.next_buffer(field)?;
                let values = self.next_buffer(field)?;
                let builder = ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(offsets)
                    .add_buffer(values);
                build_nullable(builder, field, validity, length, null_count)
            }
            DataType::List(child_field) => {
                let validity = self.next_buffer(field)?;
                let offsets = self.next_buffer(field)?;
                let child = self.decode_array_data(child_field)?;
                let builder = ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(offsets)
                    .add_child_data(child);
                build_nullable(builder, field, validity, length, null_count)
            }
            DataType::Struct(child_fields) => {
                let validity = self.next_buffer(field)?;
                let mut builder = ArrayData::builder(data_type.clone()).len(length);
                for child_field in child_fields {
                    builder = builder.add_child_data(self.decode_array_data(child_field)?);
                }
                build_nullable(builder, field, validity, length, null_count)
            }
            DataType::Boolean
            | DataType::Int64
            | DataType::Float64
            | DataType::Date32
            | DataType::Time64(ArrowTimeUnit::Microsecond)
            | DataType::Timestamp(ArrowTimeUnit::Microsecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Interval(ArrowIntervalUnit::YearMonth)
            | DataType::Interval(ArrowIntervalUnit::DayTime)
            | DataType::Interval(ArrowIntervalUnit::MonthDayNano) => {
                let validity = self.next_buffer(field)?;
                let values = self.next_buffer(field)?;
                let builder = ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(values);
                build_nullable(builder, field, validity, length, null_count)
            }
            unsupported => Err(ArrowError::NotYetImplemented(format!(
                "Arrow array type {unsupported} is outside the BigQuery Storage Read type mapping"
            ))),
        }
    }

    /// The next field node's `(length, null_count)`.
    fn next_node(&mut self, field: &Field) -> Result<(usize, usize), ArrowError> {
        let (length, null_count) = self.nodes.get(self.node_index).ok_or_else(|| {
            ipc_error(format!(
                "field {:?} has no Arrow IPC field node",
                field.name()
            ))
        })?;
        self.node_index += 1;
        let length = usize::try_from(length).map_err(|_| {
            ipc_error(format!(
                "field {:?} has invalid Arrow IPC length {length}",
                field.name()
            ))
        })?;
        let null_count = usize::try_from(null_count).map_err(|_| {
            ipc_error(format!(
                "field {:?} has invalid Arrow IPC null count {null_count}",
                field.name()
            ))
        })?;
        if null_count > length {
            return Err(ipc_error(format!(
                "field {:?} has null count {null_count} greater than length {length}",
                field.name()
            )));
        }
        Ok((length, null_count))
    }

    fn next_buffer(&mut self, field: &Field) -> Result<Buffer, ArrowError> {
        let (offset, length) = self.buffers.get(self.buffer_index).ok_or_else(|| {
            ipc_error(format!(
                "field {:?} has no Arrow IPC buffer: the record batch declares only {}",
                field.name(),
                self.buffers.len()
            ))
        })?;
        self.buffer_index += 1;
        let offset = usize::try_from(offset).map_err(|_| {
            ipc_error(format!(
                "field {:?} has invalid Arrow IPC buffer offset {offset}",
                field.name()
            ))
        })?;
        let length = usize::try_from(length).map_err(|_| {
            ipc_error(format!(
                "field {:?} has invalid Arrow IPC buffer length {length}",
                field.name()
            ))
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            ipc_error(format!(
                "field {:?} has an Arrow IPC buffer range {offset}+{length} that overflows usize",
                field.name()
            ))
        })?;
        if end > self.body.len() {
            return Err(ipc_error(format!(
                "field {:?} has an Arrow IPC buffer range {offset}..{end} beyond the {}-byte message body",
                field.name(),
                self.body.len()
            )));
        }
        let buffer = self.body.slice_with_length(offset, length);
        if !self.lz4_compressed {
            return Ok(buffer);
        }
        decompress_buffer(buffer).map_err(|error| match error {
            ArrowError::IpcError(message) => {
                ipc_error(format!("field {:?}: {message}", field.name()))
            }
            other => other,
        })
    }
}

/// Validates and builds one array, naming `field` in every refusal: Arrow's own validation errors
/// (invalid UTF-8, out-of-range offsets, a wrong null count) do not say which column they are in.
fn build_nullable(
    mut builder: ArrayDataBuilder,
    field: &Field,
    validity: Buffer,
    length: usize,
    null_count: usize,
) -> Result<ArrayData, ArrowError> {
    if null_count != 0 {
        let required = length.div_ceil(8);
        if validity.len() < required {
            return Err(ipc_error(format!(
                "field {:?} has a {}-byte Arrow IPC validity buffer, expected at least {required} bytes for {length} values",
                field.name(),
                validity.len()
            )));
        }
        builder = builder.null_bit_buffer(Some(validity));
    }
    // `align_buffers` is required: an uncompressed buffer is a zero-copy slice of the protobuf
    // frame and may be misaligned, and `ScalarBuffer::from` panics on that. It copies only
    // fixed-width buffers; aligning the whole body instead copies 1.6x the bytes for no time gain
    // (`benches/ipc.rs` decode-misaligned).
    builder
        .null_count(null_count)
        .align_buffers(true)
        .build()
        .map_err(|source| {
            ipc_error(format!(
                "field {:?} holds invalid Arrow data: {source}",
                field.name()
            ))
        })
}

#[cfg(test)]
mod tests;
