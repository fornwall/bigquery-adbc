//! Driver errors: building an ADBC [`Error`] with its status, message, SQLSTATE and details.
//!
//! [`status`] maps service failures to ADBC statuses, [`credentials`] explains why a credential
//! failed, and [`options`] words option refusals.

use std::ffi::c_char;
use std::fmt::Write as _;

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::Error as GoogleError;

use crate::rest::jobs::JobReference;
use crate::rest::model::ErrorProto;

mod credentials;
mod options;
mod status;

pub(crate) use credentials::{
    CredentialFailure, CredentialLayer, TokenServiceFailure, chain, credential_sources,
};
pub(crate) use options::{
    option_get_refusal, option_not_set, option_set_refusal, unknown_or_wrong_type,
    wrong_option_type,
};
pub(crate) use status::{
    google_error, google_error_code, google_error_is_not_found, google_error_is_transient,
    http_status, request_error, rpc_code_status, service_status,
};

use status::bigquery_reason_status;

// Out of line to keep construction off callers' success paths, even under fat LTO.
#[cold]
#[inline(never)]
pub(crate) fn error(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

/// What every message about a broken driver invariant ends with.
pub(crate) const DRIVER_DEFECT: &str =
    "this is a driver defect, report it at https://github.com/fornwall/bigquery-adbc/issues";

/// A broken invariant of the driver's own, reported rather than panicking: `what` failed, and no
/// caller action can fix it.
#[cold]
#[inline(never)]
pub(crate) fn driver_defect(what: std::fmt::Arguments<'_>) -> Error {
    error(format!("{what}; {DRIVER_DEFECT}"), Status::Internal)
}

/// `detail` named by `context`, as `{context}: {detail}`, or alone when `context` is empty.
///
/// An empty context marks a request whose caller names the failure itself, as
/// `crate::rest::metadata::MetadataError::into_error` does, so the operation is named once.
pub(crate) fn named(context: &str, detail: impl std::fmt::Display) -> String {
    if context.is_empty() {
        detail.to_string()
    } else {
        format!("{context}: {detail}")
    }
}

/// `detail` under `failed to {action}`, or alone when `action` is empty. See [`named`].
pub(crate) fn failed_to(action: &str, detail: impl std::fmt::Display) -> String {
    if action.is_empty() {
        detail.to_string()
    } else {
        format!("failed to {action}: {detail}")
    }
}

pub(crate) fn invalid_argument(message: impl Into<String>) -> Error {
    error(message, Status::InvalidArguments)
}

pub(crate) fn invalid_state(message: impl Into<String>) -> Error {
    error(message, Status::InvalidState)
}

pub(crate) fn not_found(message: impl Into<String>) -> Error {
    error(message, Status::NotFound)
}

pub(crate) fn not_implemented(message: impl Into<String>) -> Error {
    error(message, Status::NotImplemented)
}

/// The failure to read a file the caller named, such as a credential file or a dbt seed.
///
/// A path that names nothing readable, or a file that is not what the option describes, does not
/// clear on its own, so it is `INVALID_ARGUMENT`; only a fault of the filesystem itself is the
/// retryable [`Status::IO`].
pub(crate) fn unreadable_file(message: String, source: &std::io::Error) -> Error {
    use std::io::ErrorKind as Kind;

    let status = match source.kind() {
        // `InvalidInput` is a path with an interior NUL, and `InvalidData` a file whose contents
        // are not what the reader takes, such as a non-UTF-8 key: values the caller chose rather
        // than filesystem conditions.
        Kind::NotFound
        | Kind::PermissionDenied
        | Kind::IsADirectory
        | Kind::NotADirectory
        | Kind::InvalidFilename
        | Kind::InvalidInput
        | Kind::InvalidData => Status::InvalidArguments,
        _ => Status::IO,
    };
    error(message, status)
}

/// The canonical JSON spelling of one option's configured payload.
///
/// Callers pass maps or arrays of strings the driver built, which `serde_json` cannot fail to
/// render, so a failure is reported as a driver defect. `what` names the payload, such as "job
/// labels". Shared by the dbt-core 2.0 options and the statement's `labels` readback.
pub(crate) fn option_value_json<T: serde::Serialize + ?Sized>(
    payload: &T,
    what: &str,
) -> adbc_core::error::Result<String> {
    serde_json::to_string(payload).map_err(|source| {
        driver_defect(format_args!(
            "BigQuery {what} could not be serialized: {source}"
        ))
    })
}

/// Convert BigQuery's structured error entries into one ADBC error with `status`, keeping the
/// first entry's `reason` and `location` as ADBC 1.1.0 details, so consumers need not parse them
/// out of the message.
///
/// Jobs derive `status` from the `reason`; REST responses from the HTTP status, with the reason
/// refining an ambiguous 403.
pub(crate) fn bigquery_errors(context: &str, errors: &[ErrorProto], status: Status) -> Error {
    let Some(first) = errors.first() else {
        return error(
            named(context, "the job failed without error details"),
            status,
        );
    };
    // BigQuery reports a `jobTimeoutMs` expiry as the same `stopped` cancellation a caller's
    // cancel produces, differing only in the message. adbc.h reserves CANCELLED for requested
    // cancels, so a deadline is TIMEOUT.
    let timed_out = status == Status::Cancelled
        && first.reason == "stopped"
        && first.message.contains("timed out");
    let mut failure = error(
        named(
            context,
            summarize_errors(errors.iter().map(format_error_proto)),
        ),
        if timed_out { Status::Timeout } else { status },
    );
    extend_details(
        &mut failure,
        [
            (DETAIL_REASON, &first.reason),
            (DETAIL_LOCATION, &first.location),
        ]
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| (key.to_owned(), value.clone().into_bytes())),
    );
    failure.sqlstate = sqlstate(&first.reason);
    failure
}

/// The full name of a BigQuery job, `project:location.job_id`, as the job resource's `id`, the
/// console and `bq show -j` spell it. A reference without a location omits it.
#[inline(never)]
pub(crate) fn job_name(reference: &JobReference) -> String {
    match reference
        .location
        .as_deref()
        .filter(|location| !location.is_empty())
    {
        Some(location) => format!("{}:{location}.{}", reference.project_id, reference.job_id),
        None => format!("{}:{}", reference.project_id, reference.job_id),
    }
}

/// Convert a failed query or job's service errors, naming the job (if any) in the message and as
/// [`DETAIL_JOB_ID`]. `reference` is `None` for a query BigQuery answered without a job.
pub(crate) fn job_error(
    context: &str,
    reference: Option<&JobReference>,
    errors: &[ErrorProto],
) -> Error {
    // A failure with no error entries maps like an unrecognized reason.
    let status = errors.first().map_or(Status::Unknown, |error| {
        bigquery_reason_status(&error.reason)
    });
    let failure = match reference {
        Some(reference) => bigquery_errors(
            &format!("{context} (job {})", job_name(reference)),
            errors,
            status,
        ),
        None => bigquery_errors(context, errors, status),
    };
    with_job_verdict(with_job_link(failure, reference), "failed")
}

/// Name the job a failure came from: [`DETAIL_JOB_ID`] when the job is known, plus a console link
/// when its location is. Idempotent, since nested completion and polling boundaries may see the
/// same failure.
pub(crate) fn with_job_link(mut failure: Error, reference: Option<&JobReference>) -> Error {
    let Some(reference) = reference else {
        return failure;
    };
    if reference.project_id.is_empty() || reference.job_id.is_empty() {
        return failure;
    }
    let details = failure.details.get_or_insert_default();
    if !details.iter().any(|(key, _)| key == DETAIL_JOB_ID) {
        details.push((DETAIL_JOB_ID.to_owned(), job_name(reference).into_bytes()));
    }
    let Some(location) = reference
        .location
        .as_deref()
        .filter(|value| !value.is_empty())
    else {
        return failure;
    };
    let suffix = format!(
        "{JOB_LINK_PREFIX}{}&j=bq:{}:{}&page=queryresults)",
        crate::rest::url::escape_url_component(&reference.project_id),
        crate::rest::url::escape_url_component(location),
        crate::rest::url::escape_url_component(&reference.job_id),
    );
    if !failure.message.ends_with(&suffix) {
        failure.message.push_str(&suffix);
    }
    failure
}

/// How [`with_job_link`]'s console link starts, which is how [`append_remedy`] finds it.
const JOB_LINK_PREFIX: &str = " (Query: https://console.cloud.google.com/bigquery?project=";

/// Appends `; {remedy}` to a failure's message, ahead of the console link when it has one, so the
/// link stays last.
pub(crate) fn append_remedy(failure: &mut Error, remedy: &str) {
    let at = failure
        .message
        .rfind(JOB_LINK_PREFIX)
        .unwrap_or(failure.message.len());
    failure.message.insert_str(at, &format!("; {remedy}"));
}

/// Records what `failure` proves about its job; see [`DETAIL_JOB_VERDICT`].
pub(crate) fn with_job_verdict(mut failure: Error, verdict: &str) -> Error {
    failure
        .details
        .get_or_insert_default()
        .push((DETAIL_JOB_VERDICT.to_owned(), verdict.as_bytes().to_vec()));
    failure
}

/// Whether `failure` proves what became of its job; see [`DETAIL_JOB_VERDICT`].
pub(crate) fn has_job_verdict(failure: &Error) -> bool {
    failure
        .details
        .as_ref()
        .is_some_and(|details| details.iter().any(|(key, _)| key == DETAIL_JOB_VERDICT))
}

/// The BigQuery `reason` a failure carries as [`DETAIL_REASON`], if any.
pub(crate) fn failure_reason(failure: &Error) -> Option<&str> {
    failure
        .details
        .as_ref()?
        .iter()
        .find(|(key, _)| key == DETAIL_REASON)
        .and_then(|(_, value)| std::str::from_utf8(value).ok())
}

/// The ADBC 1.1.0 detail key carrying BigQuery's own machine-readable `reason`.
pub(crate) const DETAIL_REASON: &str = "bigquery.error.reason";

/// The ADBC 1.1.0 detail key carrying the part of the request the service blamed.
pub(crate) const DETAIL_LOCATION: &str = "bigquery.error.location";

/// The ADBC 1.1.0 detail key carrying the job a failure came from, as [`job_name`] spells it.
pub(crate) const DETAIL_JOB_ID: &str = "bigquery.job_id";

/// The ADBC 1.1.0 detail key saying what a failure proves about its job: `failed` when the job
/// reported it, `not_created` when BigQuery refused the submission or never received it. A failure
/// without one proves nothing, whatever its status: the job may have run.
pub(crate) const DETAIL_JOB_VERDICT: &str = "bigquery.job.verdict";

/// The ADBC 1.1.0 detail key carrying, in decimal, the HTTP status the service answered with.
pub(crate) const DETAIL_HTTP_STATUS: &str = "bigquery.error.http_status";

/// The ADBC 1.1.0 detail key carrying, in decimal, the [`google.rpc.Code`] a gRPC failure named.
///
/// [`google.rpc.Code`]: https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
pub(crate) const DETAIL_RPC_CODE: &str = "bigquery.error.rpc_code";

/// Record the numeric status a failure arrived with, as `vendor_code` and as a detail.
///
/// 1.1.0 callers see only the detail, because their `vendor_code` holds the layout sentinel. HTTP
/// statuses (three digits) and `google.rpc.Code` numbers (0..=16) cannot collide.
fn record_status_code(failure: &mut Error, key: &str, code: i32) {
    failure.vendor_code = code;
    failure
        .details
        .get_or_insert_default()
        .push((key.to_owned(), code.to_string().into_bytes()));
}

/// Records the HTTP status a directly issued request was rejected with.
pub(crate) fn record_http_status(failure: &mut Error, status: reqwest::StatusCode) {
    record_status_code(failure, DETAIL_HTTP_STATUS, i32::from(status.as_u16()));
}

/// Records the `google.rpc.Code` number a JSON-reported `google.rpc.Status` named.
pub(crate) fn record_rpc_code(failure: &mut Error, code: i64) {
    if let Ok(code) = i32::try_from(code) {
        record_status_code(failure, DETAIL_RPC_CODE, code);
    }
}

/// Records whichever number a failure from a generated client carries: the HTTP status when the
/// transport reported one, and otherwise the gRPC code the service named.
fn record_google_error_code(failure: &mut Error, source: &GoogleError) {
    if let Some(status) = source.http_status_code() {
        record_status_code(failure, DETAIL_HTTP_STATUS, i32::from(status));
    } else if let Some(code) = google_error_code(source) {
        record_status_code(failure, DETAIL_RPC_CODE, code as i32);
    }
}

/// Append every detail of the `google.rpc.Status` a generated client's failure carries.
///
/// Each detail becomes one entry keyed by its fully qualified protobuf type name (the `@type` URL
/// after its last `/`, such as `google.rpc.ErrorInfo`) and valued with its UTF-8 ProtoJSON,
/// `@type` included. Types may repeat, in the service's order. Untyped details are skipped; on the
/// gRPC path `google-cloud-gax` already drops types outside `google.rpc` (UP-41).
///
/// Only the outermost error's status is read. A credential failure's chain may hold the token
/// endpoint's status, which `crate::config::redaction` scrubs, so it is never forwarded.
fn record_status_details(failure: &mut Error, source: &GoogleError) {
    if let Some(status) = source.status() {
        extend_details(failure, status.details.iter().filter_map(status_detail));
    }
}

/// [`record_status_details`] for a `google.rpc.Status` a JSON API reported in a response body,
/// whose `details` are ProtoJSON already.
pub(crate) fn record_json_status_details(failure: &mut Error, status: &serde_json::Value) {
    let Some(details) = status.get("details").and_then(serde_json::Value::as_array) else {
        return;
    };
    extend_details(
        failure,
        details.iter().filter_map(|detail| {
            let name = status_detail_name(detail.get("@type")?.as_str()?)?;
            Some((name.to_owned(), serde_json::to_vec(detail).ok()?))
        }),
    );
}

/// Appends `entries` to a failure's details, leaving a failure that had none at `None` rather
/// than `Some(vec![])` when there is nothing to add.
fn extend_details(failure: &mut Error, entries: impl Iterator<Item = (String, Vec<u8>)>) {
    let mut entries = entries.peekable();
    if entries.peek().is_some() {
        failure.details.get_or_insert_default().extend(entries);
    }
}

/// One typed status detail as its `(type name, ProtoJSON)` pair.
///
/// The name comes from the serialized `@type`, not a table over the `#[non_exhaustive]` variants,
/// so new and `Other` types need no change here.
fn status_detail(
    detail: &google_cloud_gax::error::rpc::StatusDetails,
) -> Option<(String, Vec<u8>)> {
    let value = serde_json::to_vec(detail).ok()?;
    let name =
        status_detail_name(&serde_json::from_slice::<DetailType<'_>>(&value).ok()?.url)?.to_owned();
    Some((name, value))
}

/// The `@type` of a serialized detail, borrowed from its bytes; every other field is skipped.
#[derive(serde::Deserialize)]
struct DetailType<'a> {
    #[serde(rename = "@type", borrow)]
    url: std::borrow::Cow<'a, str>,
}

/// The fully qualified type name a detail's type URL ends in, or `None` for one that names none.
fn status_detail_name(url: &str) -> Option<&str> {
    Some(url.rsplit_once('/').map_or(url, |(_, name)| name)).filter(|name| !name.is_empty())
}

/// `42000`, "syntax error or access rule violation", as ASCII code points, which fit `c_char`
/// whatever its signedness.
const SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: [c_char; 5] = [0x34, 0x32, 0x30, 0x30, 0x30];

/// The SQLSTATE for a BigQuery `reason`, or all-NUL for a reason that has none.
///
/// Only class 42 is claimed: an invalid query is a syntax error, and `accessDenied` an access rule
/// violation. Other reasons name service conditions (rate limits, backend errors, missing
/// datasets) that the standard has no class for, or, like `invalid`, which BigQuery documents as
/// "any type of invalid input other than an invalid query", no single class.
fn sqlstate(reason: &str) -> [c_char; 5] {
    match reason {
        "invalidQuery" | "accessDenied" => SQLSTATE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
        _ => [0; 5],
    }
}

/// How many entries of a server-supplied error list are rendered before the rest are counted.
///
/// BigQuery reports one entry per rejected row, so a failed load or append can return thousands;
/// an uncapped join would let the server choose the message length.
pub(crate) const REPORTED_ERRORS: usize = 20;

/// Joins error entries for display, rendering at most [`REPORTED_ERRORS`] and counting the rest.
pub(crate) fn summarize_errors(mut entries: impl ExactSizeIterator<Item = String>) -> String {
    summarize_error_entries(&mut entries)
}

#[cold]
#[inline(never)]
fn summarize_error_entries(entries: &mut dyn ExactSizeIterator<Item = String>) -> String {
    let total = entries.len();
    let mut reported = entries.next().unwrap_or_default();
    for entry in entries.take(REPORTED_ERRORS - 1) {
        reported.push_str("; ");
        reported.push_str(&entry);
    }
    let elided = total.saturating_sub(REPORTED_ERRORS);
    if elided != 0 {
        write!(reported, "; ... and {elided} more").expect("writing to a String cannot fail");
    }
    reported
}

/// Render one job error as `{reason}: {message}`, the dbt-labs Go fork's spelling.
///
/// dbt-core 2.0 detects missing relations by searching the message for `notFound:`
/// (`is_bigquery_not_found_error` in `crates/dbt-adapter/src/metadata/bigquery/mod.rs`), not by
/// the ADBC status, so the prefix must keep this spelling.
fn format_error_proto(source: &ErrorProto) -> String {
    // REST rejections render their entries here too, so the fallback must not say "job".
    let message = if source.message.is_empty() {
        "BigQuery gave no message"
    } else {
        &source.message
    };
    let mut text = if source.reason.is_empty() {
        message.to_owned()
    } else {
        format!("{}: {message}", source.reason)
    };
    // Not "location": beside "not found in location US" that would read as a region.
    if !source.location.is_empty() {
        text.push_str(" (field: ");
        text.push_str(&source.location);
        text.push(')');
    }
    text
}

#[cfg(test)]
mod tests;
