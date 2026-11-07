//! REST job clients and cancellation scopes, for tests below the statement layer.

use std::sync::Arc;

use crate::cancellation::{Cancellation, Operation};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::rest::jobs::JobClient;
use crate::runtime::{SharedRuntime, new_runtime};

/// A fresh runtime with a job client for `endpoint`.
pub(crate) fn job_client(endpoint: &str) -> (SharedRuntime, JobClient) {
    (new_runtime().unwrap(), JobClient::for_test(endpoint))
}

/// A statement's cancellation scope, whose remote cancels go to `endpoint`.
pub(crate) fn cancellation_at(runtime: &SharedRuntime, endpoint: &str) -> Arc<Cancellation> {
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint);
    Cancellation::new(runtime.clone(), state, "statement")
}

/// A statement's cancellation scope over `state`, with an operation begun in it.
pub(crate) fn operation(
    runtime: &SharedRuntime,
    state: Arc<ClientState>,
) -> (Arc<Cancellation>, Operation) {
    let cancellation = Cancellation::new(runtime.clone(), state, "statement");
    let operation = cancellation.begin();
    (cancellation, operation)
}

/// [`cancellation_at`], with an operation begun in it.
pub(crate) fn operation_at(
    runtime: &SharedRuntime,
    endpoint: &str,
) -> (Arc<Cancellation>, Operation) {
    let cancellation = cancellation_at(runtime, endpoint);
    let operation = cancellation.begin();
    (cancellation, operation)
}
