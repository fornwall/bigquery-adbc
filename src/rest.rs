//! Directly issued Google JSON API requests.
//!
//! * [`client`]: the shared `reqwest` client and its per-request timeout budgets.
//! * [`request`]: one authenticated request (credential, cancellation gate, body).
//! * [`url`]: percent-escaping for every interpolated URL component.
//! * [`rejection`]: a service rejection as a driver error, never echoing the body.
//! * [`json`]: the shared JSON codec, including Google's string-encoded 64-bit integers.
//! * [`retry`]: which failures are retried, the backoff, and the attempt limits.
//! * [`jobs`], [`metadata`]: the BigQuery job and metadata operations.
//! * [`model`]: their request and response documents.
//!
//! Endpoints are resolved in `crate::config::endpoints`.
//!
//! These requests bypass `google-cloud-bigquery-v2`'s generated clients, which are `dyn` service
//! stubs: one call links every RPC of the service and every model it reaches (see
//! `docs/BINARY-SIZE.md`).
//!
//! Job insertion sends a caller-generated job ID, so an ambiguous insert is recovered by fetching
//! that job. Guarded metadata patches send `If-Match`, because BigQuery ignores an `etag` in the
//! request body and applies a stale patch (observed, not documented). The dbt python-model
//! operations reach Cloud Storage, Dataproc, and Vertex AI over the same transport and token.

use std::sync::Arc;

use crate::config::endpoints::endpoint;

pub(crate) mod client;
pub(crate) mod jobs;
pub(crate) mod json;
pub(crate) mod metadata;
pub(crate) mod model;
pub(crate) mod rejection;
pub(crate) mod request;
pub(crate) mod retry;
pub(crate) mod url;

/// `{endpoint}/bigquery/v2/projects` without a trailing slash, the prefix of every URL that
/// [`jobs::JobClient`] and [`metadata::MetadataClient`] build. `configured` has already passed the
/// untrusted-endpoint checks in `ClientConfiguration::build`.
fn projects_prefix(configured: Option<&str>) -> Arc<str> {
    Arc::from(format!("{}/bigquery/v2/projects", endpoint(configured)))
}
