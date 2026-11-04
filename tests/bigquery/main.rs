//! End-to-end tests against BigQuery, with opt-in emulator endpoints.

#![allow(
    clippy::unwrap_used,
    reason = "test helpers panic on failure by design; only the library must never panic"
)]
#![allow(
    clippy::panic,
    reason = "test helpers panic on failure by design; only the library must never panic"
)]
#![allow(
    clippy::items_after_statements,
    reason = "tests declare helper items next to the step that uses them"
)]

mod backend;
mod cancellation;
mod connection;
mod dbt;
mod errors;
mod ingest;
mod metadata;
mod oauth_scopes;
mod parameters;
#[path = "../support/project.rs"]
mod project_config;
mod query;
mod storage_read;
mod support;
