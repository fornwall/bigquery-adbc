//! Fixtures shared by tests across the crate.

pub(crate) mod arrow;
#[allow(
    dead_code,
    reason = "adopted by the test modules below the statement layer"
)]
pub(crate) mod cancellation;
pub(crate) mod credentials;
pub(crate) mod driver;
pub(crate) mod errors;
pub(crate) mod grpc;
pub(crate) mod http;
pub(crate) mod identifiers;
pub(crate) mod options;
pub(crate) mod schema;

/// An endpoint that refuses every connection at once. No ephemeral bind is ever assigned port 1,
/// so unlike a freed ephemeral port it can never turn into another test's mock.
pub(crate) const UNREACHABLE_ENDPOINT: &str = "http://127.0.0.1:1";
