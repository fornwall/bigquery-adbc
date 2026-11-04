//! `decode_schema` over the standalone Arrow IPC schema message `CreateReadSession` returns.
//!
//! Exercises recursive field parsing and decoding, including the `MAX_SCHEMA_NESTING_DEPTH`
//! bound that prevents stack exhaustion on deeply nested input.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = adbc_driver_bigquery::fuzz_shim::ipc_decode_schema(data);
});
