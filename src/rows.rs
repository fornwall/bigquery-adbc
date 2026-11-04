//! Decoding of BigQuery REST JSON result rows into Arrow.
//!
//! [`crate::query`] fetches rows and runs the buffered pipeline; this module decodes JSON rows
//! against the canonical Arrow schema [`crate::types::schema`] converts a REST `TableSchema` into.
//! [`crate::storage_read`] reshapes its batches into the same canonical schema, so both paths
//! produce identical results.
//!
//! [`rest`] reads a page's `rows` into positional cells, [`values`] decodes cells into Arrow
//! arrays, and [`scalars`] parses BigQuery scalar spellings.

mod rest;
mod scalars;
mod values;

pub(crate) use rest::{RestRow, RestRows, RestValue};
pub(crate) use values::rest_rows_to_record_batch;
