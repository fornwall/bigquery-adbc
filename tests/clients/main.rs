//! Third-party ADBC clients driven against this repository's driver.
//!
//! Each suite's tests stay in the client's own language; this target builds what the client
//! needs, owns the BigQuery resources of a run and judges its results. Suites run under the
//! `#[ignore]` convention of `tests/bigquery/`, and only checks that read checked-in files run
//! by default.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    reason = "test helpers panic on failure by design; only the library must never panic"
)]
#![allow(
    clippy::panic,
    reason = "test helpers panic on failure by design; only the library must never panic"
)]
#![allow(
    clippy::indexing_slicing,
    reason = "indexing a serde_json::Value by key yields null for an absent key instead of panicking"
)]

mod adbc_validation;
mod datasets;
mod dbt;
mod duckdb;
mod foundry;
#[path = "../support/project.rs"]
mod project_config;
mod pytest;
mod support;
