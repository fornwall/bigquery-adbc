//! Google JSON API rejections as driver errors.
//!
//! The raw body never reaches the error. Only the parts written for display are read: the
//! `error.errors` entries, else `error.message`, else the HTTP status name. Echoed request fields
//! and debugging payloads stay out.

use std::fmt::Write as _;

use adbc_core::error::Error;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::error::{
    bigquery_errors, error, http_status, named, record_http_status, service_status,
};
use crate::rest::model::ErrorProto;

#[cfg(test)]
mod tests;

/// Renders a rejected request as the caller's context followed by the service's account, or the
/// account alone under an empty context (see [`crate::error::named`]).
///
/// Structured entries render as a failed job's errors do, `{reason}: {message} (field: ...)`
/// joined with `; `, so a 404 reads the same from a metadata `PATCH` and from a query job.
pub(crate) fn service_error(context: &str, status: reqwest::StatusCode, body: &[u8]) -> Error {
    service_failure(context, status, body).0
}

/// [`service_error`], also returning the first entry's BigQuery `reason` for retry
/// classification: HTTP 403 is both `rateLimitExceeded` (retryable) and `blocked` (permanent).
/// See `crate::rest::retry::retry_class`.
pub(crate) fn service_failure(
    context: &str,
    status: reqwest::StatusCode,
    body: &[u8],
) -> (Error, Option<String>) {
    let rejection = Rejection::read(body);
    if rejection.errors.is_empty() {
        let google_document = rejection.message.is_some();
        // Without `errors` entries, a `google.rpc.ErrorInfo` reason is the one refinement.
        let refined = service_status(status, rejection.error_info_reason.as_deref());
        let mut rendered = named(context, rejection.detail(status));
        append_http_status(&mut rendered, status);
        // A bare 404 would otherwise read exactly like a missing table.
        if !google_document && status == reqwest::StatusCode::NOT_FOUND {
            write!(
                rendered,
                "; the response is not a Google API error, so the URL may not be a Google API; \
                 check {} and the other endpoint options",
                crate::BigQueryDatabaseOption::Endpoint.as_str()
            )
            .expect("writing to a String cannot fail");
        }
        let mut failure = error(rendered, refined);
        record_http_status(&mut failure, status);
        return (failure, None);
    }
    // Refining by the first entry's reason keeps a 403 rate limit from reading as a permission
    // failure.
    let refined = service_status(
        status,
        rejection.errors.first().map(|entry| entry.reason.as_str()),
    );
    let mut failure = bigquery_errors(context, &rejection.errors, refined);
    append_http_status(&mut failure.message, status);
    record_http_status(&mut failure, status);
    let reason = rejection
        .errors
        .into_iter()
        .next()
        .map(|entry| entry.reason);
    (failure, reason)
}

/// The message of the first error entry whose `reason` is `reason`, if the body has one.
pub(crate) fn reason_message(body: &[u8], reason: &str) -> Option<String> {
    Rejection::read(body)
        .errors
        .into_iter()
        .find(|entry| entry.reason == reason)
        .map(|entry| entry.message)
}

/// The flat `error.message`, if the body has one.
pub(crate) fn service_message(body: &[u8]) -> Option<String> {
    Rejection::read(body).message
}

/// The service's message (or the status name) followed by ` (HTTP {status})`, without context.
///
/// Every rendered rejection ends with the numeric status and shows it nowhere else.
pub(crate) fn service_rejection(status: reqwest::StatusCode, body: &[u8]) -> String {
    let mut rendered = Rejection::read(body).detail(status);
    append_http_status(&mut rendered, status);
    rendered
}

fn append_http_status(rendered: &mut String, status: reqwest::StatusCode) {
    write!(rendered, " (HTTP {})", status.as_u16()).expect("writing to a String cannot fail");
}

/// Renders a request whose `If-Match` precondition the service refused.
///
/// A 412 means the resource changed between the read and the write, so it reports `changed`
/// (what moved and what to do) instead of the service's message. Any other status is an ordinary
/// rejection.
pub(super) fn precondition_error(
    context: &str,
    status: reqwest::StatusCode,
    body: &[u8],
    changed: &str,
) -> Error {
    if status == reqwest::StatusCode::PRECONDITION_FAILED {
        let mut failure = error(format!("{context}: {changed}"), http_status(status));
        record_http_status(&mut failure, status);
        return failure;
    }
    service_error(context, status, body)
}

/// A Google error document.
///
/// The entries stay raw and are decoded with `from_slice`, not through a `serde_json::Value`:
/// `from_value` would link a second `ErrorProto` deserializer beside the slice one `JobStatus`
/// already links (measured in `BINARY-SIZE.md`).
#[derive(Default, Deserialize)]
struct ErrorDocument<'a> {
    #[serde(default, borrow)]
    error: Option<ErrorEnvelope<'a>>,
}

#[derive(Deserialize)]
struct ErrorEnvelope<'a> {
    #[serde(default, borrow)]
    errors: Option<&'a RawValue>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default, borrow)]
    details: Option<&'a RawValue>,
}

/// The one field of a `google.rpc.Status` detail read here. Of the standard detail types only
/// `google.rpc.ErrorInfo` has a `reason`.
#[derive(Deserialize)]
struct DetailReason {
    #[serde(default)]
    reason: Option<String>,
}

/// The displayable parts of a Google error document.
struct Rejection {
    /// The `error.errors` entries, which carry the `reason` and often a `location` that
    /// `error.message` lacks.
    errors: Vec<ErrorProto>,
    /// The flat `error.message`, which stands in when there are no structured entries.
    message: Option<String>,
    /// The `google.rpc.ErrorInfo` reason, such as `SERVICE_DISABLED`, which APIs without
    /// `errors` entries report instead.
    error_info_reason: Option<String>,
}

impl Rejection {
    /// Reads one error document. A body that is not one (an HTML gateway page, a truncated
    /// response) yields neither part.
    fn read(body: &[u8]) -> Self {
        let document = serde_json::from_slice::<ErrorDocument<'_>>(body).unwrap_or_default();
        let Some(envelope) = document.error else {
            return Self {
                errors: Vec::new(),
                message: None,
                error_info_reason: None,
            };
        };
        // A malformed `errors` drops only the entries.
        let mut errors = envelope
            .errors
            .and_then(|entries| {
                serde_json::from_slice::<Vec<ErrorProto>>(entries.get().as_bytes()).ok()
            })
            .unwrap_or_default();
        // An entry with only a reason would otherwise render the generic "BigQuery gave no
        // message".
        if let Some(message) = envelope.message.as_deref() {
            for entry in errors.iter_mut().filter(|entry| entry.message.is_empty()) {
                message.clone_into(&mut entry.message);
            }
        }
        let error_info_reason = envelope
            .details
            .and_then(|details| {
                serde_json::from_slice::<Vec<DetailReason>>(details.get().as_bytes()).ok()
            })
            .and_then(|details| details.into_iter().find_map(|detail| detail.reason));
        Self {
            errors,
            message: envelope.message,
            error_info_reason,
        }
    }

    /// The service's message, else the status name. The caller appends the numeric status.
    fn detail(self, status: reqwest::StatusCode) -> String {
        self.message.unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("the service sent no message")
                .to_owned()
        })
    }
}
