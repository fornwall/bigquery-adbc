//! Arrow IPC encoding specialized for BigQuery Storage Write.
//!
//! This is not a general Arrow IPC writer. Ingest normalizes every column before it reaches this
//! module, so the encoder only accepts BigQuery's canonical physical layouts. It emits one
//! standalone stream message at a time: metadata V5, an eight-byte continuation prefix, 64-byte
//! alignment, no dictionaries, and no body compression.

use std::mem::size_of;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, IntervalUnit, Schema, TimeUnit};
use flatbuffers::FlatBufferBuilder;

use super::format::{self, Buffer, FieldNode, FormatError};
use super::{CONTINUATION_MARKER, ipc_error};

const ALIGNMENT: usize = 64;
const MESSAGE_PREFIX_BYTES: usize = 8;

// Every schema declares little-endian bodies while the body bytes are arrow's native-endian
// buffers copied verbatim (hence the `_ne_bytes` offset arithmetic below). arrow-rs does not
// support big-endian targets anyway; this makes such a build fail here rather than mislabel data.
const _: () = assert!(
    cfg!(target_endian = "little"),
    "the BigQuery Arrow IPC writer declares little-endian bodies and copies arrow's native-endian \
     buffers into them verbatim; a big-endian target would need byte swapping throughout"
);

/// Reusable metadata scratch for encoding batches from one Storage Write stream.
#[derive(Default)]
pub(crate) struct Writer {
    nodes: Vec<FieldNode>,
    buffers: Vec<Buffer>,
    sources: Vec<BodySource>,
    body_length: usize,
    metadata_builder: FlatBufferBuilder<'static>,
    #[cfg(test)]
    body_materializations: std::cell::Cell<usize>,
}

/// A validated record-batch layout whose body has not been materialized yet.
///
/// Admission can inspect the exact framed size and discard this value without copying the Arrow
/// body. Consuming it writes metadata and body directly into one final allocation.
pub(crate) struct PlannedBatch<'a> {
    writer: &'a mut Writer,
    framed_size: usize,
}

#[derive(Debug)]
enum BodySource {
    /// An omitted validity bitmap needs a descriptor but no reference-counted buffer.
    Empty,
    /// An already byte-aligned source range.
    Slice(ArrowBuffer),
    /// A sliced bitmap whose first bit is not byte aligned.
    Bitmap {
        buffer: ArrowBuffer,
        bit_offset: usize,
        bit_length: usize,
    },
    /// An i32 offsets buffer whose first logical offset must be subtracted.
    RebasedOffsets { offsets: ArrowBuffer, first: i32 },
    /// The required single zero offset for an empty variable-width array with no physical offsets.
    ZeroOffset,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Validates and exactly sizes a normalized record batch without materializing its body.
    pub(crate) fn plan_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<PlannedBatch<'_>, ArrowError> {
        self.nodes.clear();
        self.buffers.clear();
        self.sources.clear();
        self.body_length = 0;
        for array in batch.columns() {
            self.plan_array_data(&array.to_data())?;
        }

        let row_count = i64::try_from(batch.num_rows())
            .map_err(|_| ipc_error("Arrow IPC record-batch row count exceeds i64::MAX"))?;
        let body_length = i64::try_from(self.body_length)
            .map_err(|_| ipc_error("Arrow IPC record-batch body exceeds i64::MAX bytes"))?;
        format::encode_record_batch_into(
            &mut self.metadata_builder,
            row_count,
            &self.nodes,
            &self.buffers,
            body_length,
        );
        let framed_size = framed_size(
            self.metadata_builder.finished_data().len(),
            self.body_length,
        )?;

        Ok(PlannedBatch {
            writer: self,
            framed_size,
        })
    }

    /// Convenience for tests and mock fixtures that do not need a separate admission step.
    #[cfg(test)]
    pub(crate) fn encode_batch(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
        self.plan_batch(batch)?.write()
    }

    fn plan_array_data(&mut self, data: &ArrayData) -> Result<(), ArrowError> {
        let length = i64::try_from(data.len())
            .map_err(|_| ipc_error("Arrow IPC array length exceeds i64::MAX"))?;
        let null_count = i64::try_from(data.null_count())
            .map_err(|_| ipc_error("Arrow IPC array null count exceeds i64::MAX"))?;
        self.nodes.push(FieldNode { length, null_count });
        self.plan_validity(data)?;

        match data.data_type() {
            DataType::Boolean => {
                let values = one_buffer(data, "Boolean")?;
                self.plan_bitmap(values, data.offset(), data.len())?;
            }
            DataType::Int64
            | DataType::Float64
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _) => {
                self.plan_fixed_width(data, 8)?;
            }
            DataType::Date32 | DataType::Interval(IntervalUnit::YearMonth) => {
                self.plan_fixed_width(data, 4)?;
            }
            DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => {
                self.plan_fixed_width(data, 16)?;
            }
            DataType::Decimal256(_, _) => self.plan_fixed_width(data, 32)?,
            DataType::Binary | DataType::Utf8 => self.plan_bytes(data)?,
            DataType::List(_) => self.plan_list(data)?,
            DataType::Struct(fields) => {
                if !data.buffers().is_empty() || data.child_data().len() != fields.len() {
                    return Err(invalid_layout(
                        data,
                        format!(
                            "expected no value buffers and {} child arrays, found {} buffers and {} children",
                            fields.len(),
                            data.buffers().len(),
                            data.child_data().len()
                        ),
                    ));
                }
                for child in data.child_data() {
                    if child.len() != data.len() {
                        return Err(invalid_layout(
                            data,
                            format!(
                                "child array has {} values but the struct has {}",
                                child.len(),
                                data.len()
                            ),
                        ));
                    }
                    self.plan_array_data(child)?;
                }
            }
            data_type => {
                return Err(ArrowError::NotYetImplemented(format::noncanonical_type(
                    data_type,
                )));
            }
        }
        Ok(())
    }

    fn plan_validity(&mut self, data: &ArrayData) -> Result<(), ArrowError> {
        if data.null_count() == 0 {
            // IPC still requires the validity buffer descriptor, but its body region may be empty
            // when the field node proves every value valid.
            return self.push_source(BodySource::Empty);
        }

        let nulls = data
            .nulls()
            .ok_or_else(|| invalid_layout(data, "nonzero null count without a validity bitmap"))?;
        let validity = nulls.inner();
        self.plan_bitmap(validity.inner(), validity.offset(), validity.len())
    }

    fn plan_fixed_width(&mut self, data: &ArrayData, byte_width: usize) -> Result<(), ArrowError> {
        let values = one_buffer(data, "fixed-width")?;
        let byte_offset = data
            .offset()
            .checked_mul(byte_width)
            .ok_or_else(|| ipc_error("Arrow IPC fixed-width buffer offset overflow"))?;
        let byte_length = data
            .len()
            .checked_mul(byte_width)
            .ok_or_else(|| ipc_error("Arrow IPC fixed-width buffer length overflow"))?;
        let end = byte_offset
            .checked_add(byte_length)
            .ok_or_else(|| ipc_error("Arrow IPC fixed-width buffer range overflow"))?;
        values.as_slice().get(byte_offset..end).ok_or_else(|| {
            invalid_layout(
                data,
                format!(
                    "value buffer has {} bytes but slice needs bytes {byte_offset}..{end}",
                    values.len()
                ),
            )
        })?;
        self.push_source(BodySource::Slice(
            values.slice_with_length(byte_offset, byte_length),
        ))
    }

    fn plan_bytes(&mut self, data: &ArrayData) -> Result<(), ArrowError> {
        let [offsets, values] = data.buffers() else {
            return Err(invalid_layout(
                data,
                format!(
                    "expected two buffers (offsets and values), found {}",
                    data.buffers().len()
                ),
            ));
        };
        let (range, selected) = offset_range(data, offsets)?;
        self.plan_rebased_offsets(selected, data, range.start)?;
        values.as_slice().get(range.clone()).ok_or_else(|| {
            invalid_layout(
                data,
                format!(
                    "value buffer has {} bytes but offsets select bytes {}..{}",
                    values.len(),
                    range.start,
                    range.end
                ),
            )
        })?;
        self.push_source(BodySource::Slice(
            values.slice_with_length(range.start, range.len()),
        ))
    }

    fn plan_list(&mut self, data: &ArrayData) -> Result<(), ArrowError> {
        let offsets = one_buffer(data, "List offsets")?;
        let [child] = data.child_data() else {
            return Err(invalid_layout(
                data,
                format!(
                    "expected one child array, found {}",
                    data.child_data().len()
                ),
            ));
        };
        let (range, selected) = offset_range(data, offsets)?;
        if range.end > child.len() {
            return Err(invalid_layout(
                data,
                format!(
                    "child array has {} values but offsets select values {}..{}",
                    child.len(),
                    range.start,
                    range.end
                ),
            ));
        }
        self.plan_rebased_offsets(selected, data, range.start)?;
        if range.start == 0 && range.end == child.len() {
            return self.plan_array_data(child);
        }
        // Bounds were checked above, avoiding ArrayData::slice's panicking error path.
        let child = child.slice(range.start, range.len());
        self.plan_array_data(&child)
    }

    fn plan_rebased_offsets(
        &mut self,
        selected: Option<ArrowBuffer>,
        data: &ArrayData,
        first: usize,
    ) -> Result<(), ArrowError> {
        let Some(selected) = selected else {
            return self.push_source(BodySource::ZeroOffset);
        };
        if first == 0 {
            return self.push_source(BodySource::Slice(selected));
        }

        let first = i32::try_from(first)
            .map_err(|_| invalid_layout(data, "first offset exceeds i32::MAX"))?;
        let (selected_offsets, remainder) = selected.as_slice().as_chunks::<{ size_of::<i32>() }>();
        if !remainder.is_empty() {
            return Err(invalid_layout(data, "incomplete i32 offset"));
        }
        // A minimum reduction lets LLVM vectorize the validation scan. Since `first` is
        // nonnegative, every offset at least `first` rebases without overflow or a negative
        // result. `offset_range` alone checks only the first and last offsets.
        let minimum = selected_offsets
            .iter()
            .map(|bytes| i32::from_ne_bytes(*bytes))
            .fold(first, i32::min);
        if minimum < first {
            return Err(invalid_layout(
                data,
                format!("offset {minimum} precedes first offset {first}"),
            ));
        }
        self.push_source(BodySource::RebasedOffsets {
            offsets: selected,
            first,
        })
    }

    fn plan_bitmap(
        &mut self,
        buffer: &ArrowBuffer,
        bit_offset: usize,
        bit_length: usize,
    ) -> Result<(), ArrowError> {
        let end = bit_offset
            .checked_add(bit_length)
            .ok_or_else(|| ipc_error("Arrow IPC bitmap range overflow"))?;
        let available_bits = buffer
            .len()
            .checked_mul(8)
            .ok_or_else(|| ipc_error("Arrow IPC bitmap length overflow"))?;
        if end > available_bits {
            return Err(ipc_error(format!(
                "Arrow IPC bitmap has {available_bits} bits but slice needs bits {bit_offset}..{end}"
            )));
        }
        if bit_offset.is_multiple_of(8) {
            return self.push_source(BodySource::Slice(
                buffer.slice_with_length(bit_offset / 8, bit_length.div_ceil(8)),
            ));
        }
        self.push_source(BodySource::Bitmap {
            buffer: buffer.clone(),
            bit_offset,
            bit_length,
        })
    }

    fn push_source(&mut self, source: BodySource) -> Result<(), ArrowError> {
        let length = source.len();
        let offset = i64::try_from(self.body_length)
            .map_err(|_| ipc_error("Arrow IPC body offset exceeds i64::MAX"))?;
        let encoded_length = i64::try_from(length)
            .map_err(|_| ipc_error("Arrow IPC buffer length exceeds i64::MAX"))?;
        let padded_length = length
            .checked_add(padding_for(length))
            .ok_or_else(|| ipc_error("Arrow IPC buffer padded length overflow"))?;
        self.body_length = self
            .body_length
            .checked_add(padded_length)
            .ok_or_else(|| ipc_error("Arrow IPC body length overflow"))?;
        self.buffers.push(Buffer {
            offset,
            length: encoded_length,
        });
        self.sources.push(source);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn body_materializations(&self) -> usize {
        self.body_materializations.get()
    }
}

impl BodySource {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Slice(buffer)
            | Self::RebasedOffsets {
                offsets: buffer, ..
            } => buffer.len(),
            Self::Bitmap { bit_length, .. } => bit_length.div_ceil(8),
            Self::ZeroOffset => size_of::<i32>(),
        }
    }

    fn write(&self, output: &mut Vec<u8>) {
        match self {
            Self::Empty => {}
            Self::Slice(buffer) => output.extend_from_slice(buffer.as_slice()),
            Self::Bitmap {
                buffer,
                bit_offset,
                bit_length,
            } => {
                let source = buffer.as_slice();
                let byte_offset = bit_offset / 8;
                let shift = bit_offset % 8;
                let byte_length = bit_length.div_ceil(8);
                // `plan_bitmap` bounds the whole byte range. Copying it first lets the shift
                // loop vectorize without checking capacity or growing the vector per byte.
                let source = source
                    .get(byte_offset..)
                    .expect("the planned bitmap starts inside its buffer");
                let low = source
                    .get(..byte_length)
                    .expect("the planned bitmap's bytes fit its buffer");
                let start = output.len();
                output.extend_from_slice(low);
                let destination = output.split_at_mut(start).1;
                for (byte, high) in destination.iter_mut().zip(source.iter().skip(1)) {
                    *byte = (*byte >> shift) | (*high << (8 - shift));
                }
                // A slice ending in the buffer's last byte has no following byte to combine.
                if byte_length == source.len()
                    && let Some(last) = destination.last_mut()
                {
                    *last >>= shift;
                }
                if !bit_length.is_multiple_of(8)
                    && let Some(last) = destination.last_mut()
                {
                    *last &= (1_u8 << (bit_length % 8)) - 1;
                }
            }
            Self::RebasedOffsets { offsets, first } => {
                // `plan_rebased_offsets` proved a whole number of offsets, each at least `first`.
                // Copy once so the rebase loop can vectorize without growing the vector per
                // offset. The validated subtraction cannot wrap.
                let start = output.len();
                output.extend_from_slice(offsets.as_slice());
                let (offsets, _) = output
                    .split_at_mut(start)
                    .1
                    .as_chunks_mut::<{ size_of::<i32>() }>();
                for bytes in offsets {
                    *bytes = i32::from_ne_bytes(*bytes)
                        .wrapping_sub(*first)
                        .to_ne_bytes();
                }
            }
            Self::ZeroOffset => output.extend_from_slice(&0_i32.to_ne_bytes()),
        }
    }
}

impl PlannedBatch<'_> {
    /// Exact number of bytes the framed record-batch message will occupy.
    pub(crate) fn framed_size(&self) -> usize {
        self.framed_size
    }

    /// Materializes the planned body directly into its final framed message allocation.
    pub(crate) fn write(self) -> Result<Vec<u8>, ArrowError> {
        #[cfg(test)]
        self.writer
            .body_materializations
            .set(self.writer.body_materializations.get() + 1);

        let ipc_message = self.writer.metadata_builder.finished_data();
        let mut framed = start_framed_message(ipc_message, self.framed_size)?;

        // `PlannedBatch` borrows the writer, so the plan cannot change between planning and here.
        for (region, source) in self.writer.buffers.iter().zip(&self.writer.sources) {
            let before = framed.len();
            source.write(&mut framed);
            // Guards `BodySource::len` and `write` disagreeing, which would shift every later
            // buffer away from the offset the metadata declares.
            let written = framed.len() - before;
            if i64::try_from(written).ok() != Some(region.length) {
                return Err(ipc_error(format!(
                    "Arrow IPC body source wrote {written} bytes into a {}-byte buffer region",
                    region.length
                )));
            }
            let padding = padding_for(written);
            let padded_end = framed
                .len()
                .checked_add(padding)
                .ok_or_else(|| ipc_error("Arrow IPC body padding overflow"))?;
            framed.resize(padded_end, 0);
        }
        Ok(framed)
    }
}

impl Drop for PlannedBatch<'_> {
    fn drop(&mut self) {
        // A plan's buffers can retain the whole input batch behind a small slice. Release them
        // after writing or discarding the plan, keeping the reusable metadata scratch capacity.
        self.writer.sources.clear();
    }
}

/// The framed V5 schema message that opens a stream of `schema`'s batches. A schema message has
/// no body; record batches go through [`Writer::plan_batch`], which sizes theirs before writing.
pub(crate) fn encode_schema(schema: &Schema) -> Result<Vec<u8>, ArrowError> {
    let ipc_message = format::encode_schema(schema).map_err(|source| match source {
        FormatError::Unsupported(message) => ArrowError::NotYetImplemented(message),
        FormatError::Invalid(message) => {
            ipc_error(format!("failed to encode Arrow IPC metadata: {message}"))
        }
    })?;
    start_framed_message(&ipc_message, framed_size(ipc_message.len(), 0)?)
}

/// Frames hand-built `ipc_message` metadata and an already padded `body` as one stream message.
#[cfg(test)]
pub(crate) fn frame_message(ipc_message: &[u8], body: &[u8]) -> Result<Vec<u8>, ArrowError> {
    if !body.len().is_multiple_of(ALIGNMENT) {
        return Err(ipc_error(
            "Arrow IPC message body is not aligned to 64 bytes",
        ));
    }
    let mut framed =
        start_framed_message(ipc_message, framed_size(ipc_message.len(), body.len())?)?;
    framed.extend_from_slice(body);
    Ok(framed)
}

/// Reserves the exact `framed_size` and writes the message prefix and padded metadata.
fn start_framed_message(ipc_message: &[u8], framed_size: usize) -> Result<Vec<u8>, ArrowError> {
    let padded_metadata_length = padded_metadata_length(ipc_message.len())?;
    let body_start = MESSAGE_PREFIX_BYTES
        .checked_add(padded_metadata_length)
        .ok_or_else(|| ipc_error("Arrow IPC framed metadata size overflow"))?;
    let prefix_length = i32::try_from(padded_metadata_length)
        .map_err(|_| ipc_error("Arrow IPC metadata exceeds i32::MAX bytes"))?;
    let mut framed = Vec::new();
    framed
        .try_reserve_exact(framed_size)
        .map_err(allocation_error)?;
    framed.extend_from_slice(&CONTINUATION_MARKER);
    framed.extend_from_slice(&prefix_length.to_le_bytes());
    framed.extend_from_slice(ipc_message);
    framed.resize(body_start, 0);
    Ok(framed)
}

fn framed_size(metadata_length: usize, body_length: usize) -> Result<usize, ArrowError> {
    padded_metadata_length(metadata_length)?
        .checked_add(MESSAGE_PREFIX_BYTES)
        .and_then(|size| size.checked_add(body_length))
        .ok_or_else(|| ipc_error("Arrow IPC framed message size overflow"))
}

fn padded_metadata_length(metadata_length: usize) -> Result<usize, ArrowError> {
    metadata_length
        .checked_add(MESSAGE_PREFIX_BYTES)
        .and_then(|length| length.checked_add(ALIGNMENT - 1))
        .map(|length| length & !(ALIGNMENT - 1))
        .and_then(|framed_metadata_length| framed_metadata_length.checked_sub(MESSAGE_PREFIX_BYTES))
        .ok_or_else(|| ipc_error("Arrow IPC metadata length overflow"))
}

fn one_buffer<'a>(data: &'a ArrayData, role: &str) -> Result<&'a ArrowBuffer, ArrowError> {
    let [buffer] = data.buffers() else {
        return Err(invalid_layout(
            data,
            format!("expected one {role} buffer, found {}", data.buffers().len()),
        ));
    };
    Ok(buffer)
}

/// Returns the value or child range the offsets select, with the offsets slice they occupy.
///
/// Arrow permits an empty offsets buffer for an empty variable-width array. IPC still requires the
/// one logical zero offset, which `plan_rebased_offsets` synthesizes when no slice is returned.
fn offset_range(
    data: &ArrayData,
    offsets: &ArrowBuffer,
) -> Result<(std::ops::Range<usize>, Option<ArrowBuffer>), ArrowError> {
    if data.is_empty() && offsets.is_empty() {
        return Ok((0..0, None));
    }
    let count = data
        .len()
        .checked_add(1)
        .ok_or_else(|| ipc_error("Arrow IPC offset count overflow"))?;
    let byte_offset = data
        .offset()
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| ipc_error("Arrow IPC offset-buffer start overflow"))?;
    let byte_length = count
        .checked_mul(size_of::<i32>())
        .ok_or_else(|| ipc_error("Arrow IPC offset-buffer length overflow"))?;
    let byte_end = byte_offset
        .checked_add(byte_length)
        .ok_or_else(|| ipc_error("Arrow IPC offset-buffer range overflow"))?;
    let selected = offsets
        .as_slice()
        .get(byte_offset..byte_end)
        .ok_or_else(|| {
            invalid_layout(
                data,
                format!(
                    "offset buffer has {} bytes but slice needs bytes {byte_offset}..{byte_end}",
                    offsets.len()
                ),
            )
        })?;
    let first = selected
        .first_chunk::<{ size_of::<i32>() }>()
        .map(|bytes| i32::from_ne_bytes(*bytes))
        .ok_or_else(|| invalid_layout(data, "offset buffer has no first offset"))?;
    let last = selected
        .last_chunk::<{ size_of::<i32>() }>()
        .map(|bytes| i32::from_ne_bytes(*bytes))
        .ok_or_else(|| invalid_layout(data, "offset buffer has no last offset"))?;
    let start = usize::try_from(first)
        .map_err(|_| invalid_layout(data, format!("first offset is negative ({first})")))?;
    let end = usize::try_from(last)
        .map_err(|_| invalid_layout(data, format!("last offset is negative ({last})")))?;
    if end < start {
        return Err(invalid_layout(
            data,
            format!("last offset {last} precedes first offset {first}"),
        ));
    }
    Ok((
        start..end,
        Some(offsets.slice_with_length(byte_offset, byte_length)),
    ))
}

#[inline]
fn padding_for(length: usize) -> usize {
    (ALIGNMENT - length % ALIGNMENT) % ALIGNMENT
}

#[cold]
#[inline(never)]
fn invalid_layout(data: &ArrayData, detail: impl std::fmt::Display) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "invalid {} array layout for BigQuery Arrow IPC: {detail}",
        data.data_type()
    ))
}

fn allocation_error(source: impl std::fmt::Display) -> ArrowError {
    ArrowError::MemoryError(format!("failed to allocate Arrow IPC message: {source}"))
}

#[cfg(test)]
mod tests;
