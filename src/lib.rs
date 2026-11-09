//! An Arrow Database Connectivity (ADBC) driver for Google BigQuery.
//!
//! The driver follows the standard ADBC object hierarchy:
//!
//! ```text
//! BigQueryDriver -> BigQueryDatabase -> BigQueryConnection -> BigQueryStatement
//! ```
//!
//! Configure the Google Cloud project with [`OptionDatabase::Uri`] using a
//! `bigquery://[host][:port]/project-id` URI, or with [`BigQueryDatabaseOption::ProjectId`]. The
//! URI also accepts the Go ADBC driver's OAuth, dataset, location, quota-project, and
//! impersonation query parameters. [`BigQueryDatabaseOption::DatasetId`] sets the initial dataset.
//! The standard ADBC current catalog and schema options override both query defaults per
//! connection, and [`BigQueryStatementOption`] overrides them per statement. Authentication uses
//! Google Cloud Application Default Credentials (ADC) unless the URI or authentication-type option
//! selects JSON credentials, OAuth user credentials, or anonymous access for an emulator or mock.
//!
//! `examples/query.rs` is a complete program, and `docs/ARCHITECTURE.md` describes how the code
//! fits together.
//!
//! [`OptionDatabase::Uri`]: adbc_core::options::OptionDatabase::Uri

// Proving the Storage Read pipeline's outermost spawned future `Send` overflows the default
// 128-step limit, which is a `future_incompatible` warning slated to become an error
// (<https://github.com/rust-lang/rust/issues/159228>).
#![recursion_limit = "256"]
#![deny(unsafe_code)]
#![cfg_attr(not(feature = "ffi"), forbid(unsafe_code))]

// ADBC objects.
mod connection;
mod driver;
mod statement;
mod transaction;

// C ABI export.
#[cfg(feature = "ffi")]
#[allow(
    unsafe_code,
    reason = "the C ABI export layer is the one place unsafe is permitted"
)]
mod ffi;

// Options, configuration and errors.
mod config;
mod diag;
mod duration;
mod error;
mod options;

// Service transport and wire codecs.
mod arrow_ipc_bigquery;
mod cancellation;
mod clients;
mod rest;
mod runtime;

// Query execution and results.
mod parameters;
mod pipeline;
mod query;
mod rows;
mod storage_read;

// Bulk ingest.
mod ingest;

// BigQuery types and metadata.
mod identifiers;
mod schema_metadata;
mod types;

// dbt operations.
mod dbt;

// Developer-only.
#[cfg(feature = "benchmark")]
pub mod benchmark;
#[cfg(feature = "fuzz-shim")]
pub mod fuzz_shim;
#[cfg(any(test, feature = "python-option-codegen"))]
#[doc(hidden)]
pub mod python_options;
#[cfg(test)]
mod test_support;

pub use connection::BigQueryConnection;
pub use driver::{BigQueryDatabase, BigQueryDriver};
pub use options::keys::{
    BigQueryConnectionOption, BigQueryDatabaseOption, BigQueryStatementOption,
};
pub use options::values::{
    BigQueryAuthenticationType, BigQueryBulkIngestMethod, BigQueryCreateDisposition,
    BigQueryCredentialsType, BigQueryQueryPriority, BigQueryWriteDisposition,
};
pub use schema_metadata::{
    BIGQUERY_FIELD_METADATA_MAX_LENGTH, BIGQUERY_FIELD_METADATA_PRECISION,
    BIGQUERY_FIELD_METADATA_SCALE, BIGQUERY_SCHEMA_METADATA_JOB_KEYS,
    BIGQUERY_SCHEMA_METADATA_JOB_TOTAL_BYTES_PROCESSED,
    BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS, BIGQUERY_SCHEMA_METADATA_QUERY_ID,
    BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED, BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
};
pub use statement::BigQueryStatement;
