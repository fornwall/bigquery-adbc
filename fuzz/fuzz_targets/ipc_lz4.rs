//! `decompress_buffer` over one `LZ4_FRAME` body buffer of a BigQuery `ReadRowsResponse`.
//!
//! Exercises frame headers, block sizes, checksums, and decoding into the output allocation.
//! Valid frames in the seed corpus let mutations reach block decoding past the header checksum.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = adbc_driver_bigquery::fuzz_shim::ipc_decompress_buffer(data);
});
