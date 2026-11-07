//! Direct query tests, split by subject.
//!
//! Shared imports and fixtures live here; each submodule covers one subject.

use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use adbc_core::CancelHandle as _;
use adbc_core::error::Status;
use bytes::Bytes;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use http::HeaderMap;
use serde_json::json;

use super::attempt::{canonicalize_query_response_reference, submit_jobs_query};
use super::poll::MINIMUM_POLL_INTERVAL;
use super::*;
use crate::BigQueryQueryPriority;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::identifiers::ProjectIdentifier;
use crate::query::plan::QueryRoute;
use crate::query::response::QueryResponse;
use crate::query::{QueryDefaults, StatementQueryOptions};
use crate::rest::client::rest_client;
use crate::runtime::{SharedRuntime, new_runtime, new_runtime_with_result_conversion_concurrency};
use crate::test_support::errors::assert_message;
use crate::test_support::http::{capture_requests, capture_requests_gated, error_body};

mod attempt;
mod cancellation;
mod poll;
mod progress;
mod reference;
mod reissue;
mod unnamed;

#[allow(
    clippy::needless_pass_by_value,
    reason = "test call sites construct distinct option fixtures inline"
)]
fn plan_at(options: StatementQueryOptions, location: Option<&str>) -> QueryPlan {
    QueryPlan::new(
        Some(&ProjectIdentifier::try_new_from("test", "project").unwrap()),
        QueryDefaults::default(),
        location,
        None,
        "SELECT 1".into(),
        None,
        &options,
        QueryJobCreation::Optional,
    )
    .unwrap()
}

fn plan(options: StatementQueryOptions) -> QueryPlan {
    plan_at(options, Some("EU"))
}

fn client(endpoint: &str) -> (SharedRuntime, JobClient) {
    (new_runtime().unwrap(), JobClient::for_test(endpoint))
}

fn client_with_one_result_worker(endpoint: &str) -> (SharedRuntime, JobClient) {
    (
        new_runtime_with_result_conversion_concurrency(1).unwrap(),
        JobClient::for_test(endpoint),
    )
}

/// Blocks on one direct query execution, which every test below runs ungated.
fn complete(
    runtime: &SharedRuntime,
    client: JobClient,
    plan: QueryPlan,
    result_mode: QueryResultMode,
    context: &str,
) -> Result<CompletedQuery> {
    runtime.block_on(complete_query_with_progress(
        runtime,
        client,
        plan,
        result_mode,
        None,
        None,
        context,
    ))
}
