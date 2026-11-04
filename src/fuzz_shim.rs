//! The entry points `fuzz/` drives, behind the developer-only `fuzz-shim` feature.
//!
//! `fuzz/` is a separate crate and can only call public items, while the decoders it targets stay
//! `pub(crate)`; this module is the whole bridge. Nothing here ships: no shipped configuration
//! enables `fuzz-shim`.
//!
//! See `CONTRIBUTING.md`, "Fuzzing and Miri", for what is fuzzed and what is not.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, Schema, SchemaRef};

/// Decodes a standalone Arrow IPC schema message, as `CreateReadSession` returns one.
pub fn ipc_decode_schema(bytes: &[u8]) -> Result<SchemaRef, ArrowError> {
    crate::arrow_ipc_bigquery::reader::decode_schema(bytes)
}

/// Decompresses one `LZ4_FRAME` body buffer (the eight-byte length prefix and the frame behind
/// it), as the record-batch decoder does for every buffer of a compressed batch.
///
/// Frame header checksums keep `ipc_batch` mutations from reaching the frame parser, so it gets its
/// own entry.
pub fn ipc_decompress_buffer(bytes: &[u8]) -> Result<Buffer, ArrowError> {
    crate::arrow_ipc_bigquery::decompress_buffer(Buffer::from(bytes.to_vec()))
}

/// Decodes one Arrow IPC record-batch message, as a `ReadRowsResponse` carries one, against a
/// schema the caller supplies rather than one decoded from the same input.
///
/// Splitting the two lets a fuzzer spend its whole budget on the record-batch body, which is where
/// the buffer-length arithmetic lives, instead of on producing a schema message that parses.
pub fn ipc_decode_record_batch(
    body: &[u8],
    schema: Arc<Schema>,
) -> Result<RecordBatch, ArrowError> {
    crate::arrow_ipc_bigquery::reader::decode_record_batch_message(
        &Buffer::from(body.to_vec()),
        schema,
    )
}

/// Decodes one REST result page as the query path does a `jobs.query` or `jobs.getQueryResults`
/// response: `schema` is the response's `schema` JSON, converted into the canonical Arrow schema,
/// and `rows` its `rows` JSON array, checked as UTF-8 once and converted into one batch.
///
/// Returns the raw serde diagnostic so the fuzzer can detect cell leaks in the row decoder
/// independently of the driver's response-error redaction. `None` would be a defect: nothing
/// cancels this conversion.
pub fn rest_rows(schema: &[u8], rows: &[u8]) -> Result<Option<RecordBatch>, String> {
    let schema = serde_json::from_slice(schema).map_err(|source| source.to_string())?;
    let schema = crate::types::schema::convert_schema(&schema).map_err(|source| source.message)?;
    let rows = std::str::from_utf8(rows).map_err(|source| source.to_string())?;
    let rows =
        serde_json::from_str::<crate::rows::RestRows>(rows).map_err(|source| source.to_string())?;
    crate::rows::rest_rows_to_record_batch(
        rows.into_vec(),
        0,
        schema,
        &crate::pipeline::ResultPipelineCancellation::new(None),
    )
    .map_err(|source| source.to_string())
}
