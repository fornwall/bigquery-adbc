//! Splits input batches into normalized Arrow IPC chunks that fit an AppendRows request.
//!
//! Each chunk is normalized on its own, from a window of input rows sized by the chunk before it
//! and capped by the bytes those rows address (see [`super::window`]), so normalization copies
//! about a request at a time however large or skewed the input batch, and no window's values come
//! near the 2 GiB that canonical 32-bit offsets address. Chunk sizing reserves room for the schema
//! and protobuf envelope before allocating payloads.

use adbc_core::error::{Result, Status};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use google_cloud_bigquery::model::{ArrowRecordBatch, ArrowSchema};

use crate::arrow_ipc_bigquery::writer::{PlannedBatch, Writer, encode_schema};
use crate::error::{driver_defect, error, invalid_argument};
use crate::ingest::normalize::normalize_rows;

use super::window::{scaled, window_rows};

/// Maximum complete protobuf request size: the published and enforced quota is 10 MB, although
/// `storage.proto` claims 20 MB.
const APPEND_ROWS_REQUEST_LIMIT_BYTES: usize = 10_000_000;

/// Reserve for the stream name, offset and protobuf framing, excluding Arrow payloads.
/// 64 KiB comfortably covers these small fields and costs less than 0.7% of the limit.
const APPEND_ROWS_REQUEST_ENVELOPE_BYTES: usize = 65_536;

/// Ordinary chunks target 9 MB, subject to the schema and envelope fitting the request limit.
/// Every chunk reserves schema space because replay may open a new connection.
const APPEND_ROWS_TARGET_BATCH_BYTES: usize = 9_000_000;

pub(in crate::ingest) fn read_batch(
    batch: std::result::Result<RecordBatch, arrow_schema::ArrowError>,
    input_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let batch = batch.map_err(|source| {
        error(
            format!("failed to read BigQuery ingest input: {source}"),
            Status::InvalidData,
        )
    })?;
    if batch.schema_ref() != input_schema {
        return Err(error(
            format!(
                "BigQuery ingest input changed schema mid-stream: expected {input_schema}, got {}",
                batch.schema_ref()
            ),
            Status::InvalidData,
        ));
    }
    Ok(batch)
}

/// Verifies that serialization matches the size used to admit the chunk.
fn admitted_batch_bytes(
    written: std::result::Result<Vec<u8>, arrow_schema::ArrowError>,
    serialized_size: usize,
) -> Result<Vec<u8>> {
    let bytes = written.map_err(|source| {
        error(
            format!("failed to serialize BigQuery ingest Arrow record batch: {source}"),
            Status::Internal,
        )
    })?;
    if bytes.len() != serialized_size {
        let actual_size = bytes.len();
        return Err(driver_defect(format_args!(
            "BigQuery ingest Arrow record batch framed size was {actual_size} bytes, predicted \
             {serialized_size}"
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
std::thread_local! {
    static RECORD_BATCH_SERIALIZATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static REJECTED_CANDIDATE_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
pub(super) struct EncodedBatchChunk {
    pub(super) rows: ArrowRecordBatch,
    pub(super) row_count: usize,
}

/// Returns the ordinary chunk target and the hard limit for an indivisible row.
/// Both leave room for the schema and request envelope.
fn append_batch_size_limits(schema_size: usize) -> Result<(usize, usize)> {
    let max_batch_size = APPEND_ROWS_REQUEST_LIMIT_BYTES
        .checked_sub(APPEND_ROWS_REQUEST_ENVELOPE_BYTES)
        .and_then(|remaining| remaining.checked_sub(schema_size))
        .filter(|size| *size != 0)
        .ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery ingest Arrow schema serializes to {schema_size} bytes, leaving no room \
                 for rows in a {APPEND_ROWS_REQUEST_LIMIT_BYTES}-byte AppendRows request; ingest \
                 fewer columns, or shorten column names and field metadata"
            ))
        })?;
    Ok((
        APPEND_ROWS_TARGET_BATCH_BYTES.min(max_batch_size),
        max_batch_size,
    ))
}

/// Normalizes and encodes an ingest's batches chunk by chunk, reusing IPC metadata scratch and
/// the request budgets.
pub(super) struct ChunkEncoder {
    writer: Writer,
    /// The normalized schema every window is converted to before it is sized.
    schema: SchemaRef,
    pub(super) target_batch_bytes: usize,
    max_batch_bytes: usize,
    /// The row count and framed size of the last chunk admitted, which size the next window.
    last_chunk: Option<(usize, usize)>,
}

impl ChunkEncoder {
    /// Serializes the schema and computes the remaining request budget.
    pub(super) fn new(schema: SchemaRef) -> Result<(Self, ArrowSchema)> {
        let schema_bytes = encode_schema(&schema).map_err(|source| {
            error(
                format!("failed to serialize BigQuery ingest Arrow schema: {source}"),
                Status::InvalidData,
            )
        })?;
        let (target_batch_bytes, max_batch_bytes) = append_batch_size_limits(schema_bytes.len())?;
        Ok((
            Self {
                writer: Writer::new(),
                schema,
                target_batch_bytes,
                max_batch_bytes,
                last_chunk: None,
            },
            ArrowSchema::new().set_serialized_schema(schema_bytes),
        ))
    }

    /// Normalizes the rows of `input` from `offset` that should make a chunk of about
    /// `target_size` bytes: as many as the last chunk's density predicts, and no more than the
    /// bytes they address allow.
    pub(super) fn window(
        &self,
        input: &RecordBatch,
        offset: usize,
        target_size: usize,
    ) -> Result<RecordBatch> {
        let suggested = match self.last_chunk {
            Some((rows, size)) => scaled(rows, target_size, size),
            None => input.num_rows(),
        };
        let rows = window_rows(input, offset, suggested, target_size);
        let window = if rows == input.num_rows() {
            input.clone()
        } else {
            input.slice(offset, rows)
        };
        normalize_rows(window, self.schema.clone(), offset)
    }

    /// Encodes the longest prefix of the normalized `window` that fits a request; `first_row` is
    /// the window's first row in its input batch. A single row may use the hard request limit.
    /// Consumes the window so its normalization buffers are released before append can wait.
    /// Oversized candidates are never materialized.
    pub(super) fn encode(
        &mut self,
        window: RecordBatch,
        first_row: usize,
    ) -> Result<EncodedBatchChunk> {
        let result = self.admit(
            &window,
            first_row,
            |planned_batch, row_count, serialized_size| {
                #[cfg(test)]
                RECORD_BATCH_SERIALIZATIONS.with(|count| count.set(count.get() + 1));
                let batch_bytes = admitted_batch_bytes(planned_batch.write(), serialized_size)?;
                Ok(EncodedBatchChunk {
                    rows: ArrowRecordBatch::new().set_serialized_record_batch(batch_bytes),
                    row_count,
                })
            },
        );
        drop(window);
        result
    }

    /// Sizes the prefix of `window` that [`Self::encode`] would send, without materializing it,
    /// and returns its row count.
    pub(super) fn plan(&mut self, window: &RecordBatch, first_row: usize) -> Result<usize> {
        self.admit(window, first_row, |_, row_count, _| Ok(row_count))
    }

    /// Plans the longest prefix of `window` that fits a request, and hands it to `admitted` with
    /// its row count and framed size.
    fn admit<T>(
        &mut self,
        window: &RecordBatch,
        first_row: usize,
        admitted: impl FnOnce(PlannedBatch<'_>, usize, usize) -> Result<T>,
    ) -> Result<T> {
        let mut row_count = window.num_rows().max(1);
        loop {
            let candidate = window.slice(0, row_count);
            let planned_batch = self.writer.plan_batch(&candidate).map_err(|source| {
                error(
                    format!("failed to serialize BigQuery ingest batch: {source}"),
                    Status::InvalidData,
                )
            })?;
            let serialized_size = planned_batch.framed_size();
            if serialized_size <= self.target_batch_bytes
                || (row_count == 1 && serialized_size <= self.max_batch_bytes)
            {
                self.last_chunk = Some((row_count, serialized_size));
                return admitted(planned_batch, row_count, serialized_size);
            }
            #[cfg(test)]
            REJECTED_CANDIDATE_BYTES.with(|bytes| bytes.set(bytes.get() + serialized_size));
            if row_count == 1 {
                let max_size = self.max_batch_bytes;
                return Err(invalid_argument(format!(
                    "BigQuery ingest row {first_row} of its input batch serializes to \
                     {serialized_size} bytes, more than the {max_size} bytes of row data an \
                     AppendRows request can carry; shorten its STRING or BYTES values, or split \
                     them across rows"
                )));
            }
            row_count = scaled(row_count, self.target_batch_bytes, serialized_size)
                .max(1)
                .min(row_count - 1);
        }
    }

    /// Builds an encoder with explicit budgets and a fresh Arrow IPC stream state.
    #[cfg(test)]
    fn with_limits(schema: SchemaRef, target_batch_bytes: usize, max_batch_bytes: usize) -> Self {
        Self {
            writer: Writer::new(),
            schema,
            target_batch_bytes,
            max_batch_bytes,
            last_chunk: None,
        }
    }
}

#[cfg(test)]
mod tests;
