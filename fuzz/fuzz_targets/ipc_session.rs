//! Decode an Arrow IPC schema followed by a record batch, as in a Storage Read session.
//!
//! Schema decoding accepts types the batch decoder rejects, such as `Int32`, `Utf8View`, and
//! `Decimal64` for CSV seeds. Unlike `ipc_batch`, this target exercises that boundary.
//!
//! Input: a two-byte little-endian schema length, the schema message, then the record-batch
//! message. Invalid schemas return before batch decoding.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((length, rest)) = data.split_first_chunk::<2>() else {
        return;
    };
    let head = usize::from(u16::from_le_bytes(*length));
    if head > rest.len() {
        return;
    }
    let (schema, batch) = rest.split_at(head);
    let Ok(schema) = adbc_driver_bigquery::fuzz_shim::ipc_decode_schema(schema) else {
        return;
    };
    let _ = adbc_driver_bigquery::fuzz_shim::ipc_decode_record_batch(batch, schema);
});
