//! Mapping of HTTP codes, `google.rpc.Code` numbers and BigQuery `reason` strings to ADBC
//! statuses, and the conversions that use those tables.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::Error as GoogleError;
use google_cloud_gax::error::rpc::{Code, StatusDetails};

use super::credentials::{chain, credentials_failure, is_credentials_failure};
use super::{error, failed_to, record_google_error_code, record_status_details};

/// Converts a generated client's failure: the classified status, the service's own code and
/// message, the number the status was decided from, and every `google.rpc.Status` detail.
///
/// A credential failure is reported by its remedy alone and forwards no details: it never
/// reached the service, and the status on its chain is the scrubbed token-endpoint answer.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the conversion consumes the source error; callers never reuse it"
)]
pub(crate) fn google_error(context: &str, source: GoogleError) -> Error {
    if let Some(failure) = credentials_failure(context, &source) {
        return failure;
    }
    let source = last_attempt(&source);
    let status = google_error_status(source);
    // gax renders a service answer as "the service reports an error with code X described as: m".
    let message = match source.status() {
        Some(answer) if answer.message.is_empty() => format!("{context}: {}", answer.code),
        Some(answer) => format!("{context}: {}: {}", answer.code, answer.message),
        None => format!("{context}: {source}"),
    };
    let mut failure = error(message, status);
    record_google_error_code(&mut failure, source);
    record_status_details(&mut failure, source);
    failure
}

pub(crate) fn google_error_code(source: &GoogleError) -> Option<Code> {
    source.status().map(|status| status.code)
}

/// Retry exhaustion wraps the last failure, sometimes through a policy-specific error.
/// Unwrap only that kind: authentication and timeout wrappers keep their own meaning.
fn last_attempt(mut source: &GoogleError) -> &GoogleError {
    while source.is_exhausted() {
        let Some(attempt) = chain(source)
            .skip(1)
            .find_map(|cause| cause.downcast_ref::<GoogleError>())
        else {
            break;
        };
        source = attempt;
    }
    source
}

pub(crate) fn google_error_is_not_found(source: &GoogleError) -> bool {
    google_error_code(source) == Some(Code::NotFound)
}

/// Whether a failed Storage Read or Storage Write call may be sent again: it ended before the
/// service decided it, or the service answered with a status upstream's
/// `write::retry_policy::RetryableErrors` recommends retrying for these APIs.
///
/// `INTERNAL` is among those, although [`code_status`] reports it as a defect once retries are
/// spent. `CANCELLED` never is, because the driver's own cancel produces it. Each caller adds its
/// own exclusions.
pub(crate) fn google_error_is_transient(source: &GoogleError) -> bool {
    // tonic attaches the h2 or hyper error to a stream that breaks mid-flight, and
    // `google-cloud-gax-internal` turns any status with a source into a transport error without
    // the service's status, so a reset `ReadRows` or `AppendRows` call carries no code at all.
    if source.is_transient_and_before_rpc()
        || source.is_io()
        || source.is_timeout()
        || source.is_connect()
        || (source.is_transport() && source.http_status_code().is_none())
    {
        return true;
    }
    matches!(source.http_status_code(), Some(429 | 500 | 502 | 503 | 504))
        || matches!(
            google_error_code(source),
            Some(
                Code::Aborted
                    | Code::DeadlineExceeded
                    | Code::Internal
                    | Code::ResourceExhausted
                    | Code::Unavailable
            )
        )
}

/// Classifies a failed `reqwest` request the driver issues directly, where the generated
/// BigQuery client cannot express what the request needs (a media upload body, an `If-Match`
/// precondition). An empty `context` leaves the request unnamed; see [`super::named`].
pub(crate) fn request_error(context: &str, source: &reqwest::Error) -> Error {
    let status = if source.is_timeout() {
        Status::Timeout
    } else {
        Status::IO
    };
    error(
        failed_to(
            context,
            format_args!("{source}{}", transport_causes(source)),
        ),
        status,
    )
}

/// Appends transport causes that reqwest's Display omits, such as DNS, TLS and connection errors.
/// Credentials travel in headers, which this chain does not render. Credential-builder errors
/// are handled separately because their causes may quote secrets.
fn transport_causes(source: &reqwest::Error) -> String {
    let mut causes: Vec<String> = Vec::new();
    for cause in chain(source).skip(1) {
        let text = cause.to_string();
        // Hyper's wrappers often repeat their source's text; skip causes already reported.
        if text.is_empty() || causes.iter().any(|reported| reported.contains(&text)) {
            continue;
        }
        causes.push(text);
    }
    if causes.is_empty() {
        return String::new();
    }
    format!(" (caused by: {})", causes.join(": "))
}

/// Maps a REST status code to the status [`code_status`] gives the gRPC code it canonically
/// stands for, with one deliberate exception: BigQuery documents its HTTP 500 `internalError` and
/// `backendError` as retryable, so a 500 is `IO`, while a gRPC `INTERNAL` is a defect to surface.
pub(crate) fn http_status(status: reqwest::StatusCode) -> Status {
    match status.as_u16() {
        400 | 405 | 411 | 413 | 414 | 415 | 422 => Status::InvalidArguments,
        // 407 requests proxy credentials, before the request reaches BigQuery.
        401 | 407 => Status::Unauthenticated,
        403 => Status::Unauthorized,
        404 => Status::NotFound,
        408 | 504 => Status::Timeout,
        409 => Status::AlreadyExists,
        412 => Status::InvalidState,
        // An unsupported operation cannot be recovered by retrying.
        501 => Status::NotImplemented,
        429 | 500 | 502 | 503 | 505..=599 => Status::IO,
        499 => Status::Cancelled,
        _ => Status::Unknown,
    }
}

/// The status for a directly issued request the service rejected, refined by the `reason` of the
/// first structured error in the response body.
///
/// BigQuery uses 403 for denials, rate limits, quotas and oversized responses, and 400 for job
/// failures that do not judge the request ([`BAD_REQUEST_REASONS`]); only the `reason` separates
/// them. Only a reason the [error table] documents for that code may refine it, so `notFound`
/// under a 403 cannot reclassify it.
///
/// [error table]: https://cloud.google.com/bigquery/docs/error-messages
pub(crate) fn service_status(status: reqwest::StatusCode, reason: Option<&str>) -> Status {
    let transport = http_status(status);
    let refinable: &[&str] = match status {
        reqwest::StatusCode::FORBIDDEN => &FORBIDDEN_REASONS,
        reqwest::StatusCode::BAD_REQUEST => &BAD_REQUEST_REASONS,
        _ => return transport,
    };
    match reason.filter(|reason| refinable.contains(reason)) {
        Some(reason) => bigquery_reason_status(reason),
        None => transport,
    }
}

/// The `reason` values BigQuery's error table documents under HTTP 403, and the only ones that
/// may refine one. `jobRateLimitExceeded` is documented under 400 but listed so that a 403
/// carrying it is treated as a rate limit, not a denial. `accessNotConfigured` and
/// `SERVICE_DISABLED` (a `google.rpc.ErrorInfo` reason) are how Google APIs say the API is not
/// enabled in the project.
const FORBIDDEN_REASONS: [&str; 9] = [
    "accessDenied",
    "accessNotConfigured",
    "SERVICE_DISABLED",
    "billingNotEnabled",
    "blocked",
    "jobRateLimitExceeded",
    "quotaExceeded",
    "rateLimitExceeded",
    "responseTooLarge",
];

/// The `reason` values BigQuery's error table documents under HTTP 400 that are not about the
/// request, and the only ones that may refine one.
///
/// Other 400 reasons (`badRequest`, `invalid`, `invalidQuery`, ...) judge what was sent and keep
/// [`Status::InvalidArguments`]. The table marks the `job*` reasons and `tableUnavailable`
/// retryable; reporting them as invalid arguments would make a host such as dbt abandon work on a
/// transient condition. `timeout` and `resourceInUse` must read as they do on a failed job, since
/// a rejected completion read carries the job's own reason.
const BAD_REQUEST_REASONS: [&str; 6] = [
    "jobBackendError",
    "jobInternalError",
    "jobRateLimitExceeded",
    "resourceInUse",
    "tableUnavailable",
    "timeout",
];

/// Maps a job's BigQuery reason, or refines the permitted HTTP 400/403 reasons in
/// [`service_status`]. Reserve IO for conditions the [error table] documents as retryable;
/// caller limits and denied operations must not prompt retries.
///
/// [error table]: https://cloud.google.com/bigquery/docs/error-messages
pub(super) fn bigquery_reason_status(reason: &str) -> Status {
    match reason {
        // 403s. `blocked` is an outright denial; the documented remedy is contacting support.
        "accessDenied" | "billingNotEnabled" | "blocked" => Status::Unauthorized,
        // An API not enabled in the project: a precondition of the project rather than a missing
        // grant, so it does not read as an authorization failure. The service's message says
        // how to enable it.
        "accessNotConfigured" | "SERVICE_DISABLED" => Status::InvalidState,
        // 400s, plus `responseTooLarge` (a 403). Like `resourcesExceeded`, it is fixed by a
        // request that asks for less.
        "attributeError"
        | "badRequest"
        | "billingTierLimitExceeded"
        | "invalid"
        | "invalidQuery"
        | "invalidUser"
        | "resourcesExceeded"
        | "responseTooLarge" => Status::InvalidArguments,
        "notFound" => Status::NotFound,
        "duplicate" => Status::AlreadyExists,
        "notImplemented" => Status::NotImplemented,
        "stopped" => Status::Cancelled,
        "timeout" => Status::Timeout,
        // A 407 from an outbound HTTP proxy. ADBC defines Unauthenticated for the database, but
        // IO would invite retries that fail until the environment is fixed.
        "proxyAuthenticationRequired" => Status::Unauthenticated,
        // Everything BigQuery documents as retryable: a backend that faltered, a limit that
        // refills, an externally managed table that is temporarily away.
        "backendError"
        | "internalError"
        | "jobBackendError"
        | "jobInternalError"
        | "rateLimitExceeded"
        | "jobRateLimitExceeded"
        | "quotaExceeded"
        | "tableUnavailable" => Status::IO,
        "resourceInUse" => Status::InvalidState,
        // An unrecognized reason may be retryable, so it is not InvalidData.
        _ => Status::Unknown,
    }
}

/// Whether a gRPC refusal's `google.rpc.ErrorInfo` says the API is not enabled in the project.
fn names_disabled_service(source: &GoogleError) -> bool {
    source.status().is_some_and(|status| {
        status.details.iter().any(|detail| {
            matches!(detail, StatusDetails::ErrorInfo(info) if info.reason == "SERVICE_DISABLED")
        })
    })
}

/// Maps a canonical [`google.rpc.Code`] number to the ADBC status the driver reports for it, for
/// the JSON APIs that report a `google.rpc.Status` in a response body rather than through a
/// generated client. Codes outside the enum are `Unknown`, as on the typed path.
///
/// [`google.rpc.Code`]: https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
pub(crate) fn rpc_code_status(code: i64) -> Status {
    code_status(i32::try_from(code).map_or(Code::Unknown, Code::from))
}

/// The one table from `google.rpc.Code` to ADBC status, shared by the typed and the numeric path.
fn code_status(code: Code) -> Status {
    match code {
        Code::Cancelled => Status::Cancelled,
        Code::InvalidArgument | Code::OutOfRange => Status::InvalidArguments,
        Code::DeadlineExceeded => Status::Timeout,
        Code::NotFound => Status::NotFound,
        Code::AlreadyExists => Status::AlreadyExists,
        Code::PermissionDenied => Status::Unauthorized,
        // `Aborted` reports a concurrency conflict, which callers handle like
        // `FailedPrecondition`.
        Code::FailedPrecondition | Code::Aborted => Status::InvalidState,
        Code::Unimplemented => Status::NotImplemented,
        // AIP-194: "usually means a bug should be filed against the system", to be surfaced
        // rather than retried. The Storage API callers retry it first anyway (see
        // `google_error_is_transient`), as upstream recommends for those APIs.
        Code::Internal => Status::Internal,
        Code::Unauthenticated => Status::Unauthenticated,
        // Unrecoverable, so not `IO`, which the driver's retry loops treat as retryable.
        Code::DataLoss => Status::Internal,
        Code::ResourceExhausted | Code::Unavailable => Status::IO,
        _ => Status::Unknown,
    }
}

pub(super) fn google_error_status(source: &GoogleError) -> Status {
    let source = last_attempt(source);
    if source.is_timeout() {
        return Status::Timeout;
    }
    match google_error_code(source).map(code_status) {
        // A Storage API not enabled in the project; see `bigquery_reason_status`.
        Some(Status::Unauthorized) if names_disabled_service(source) => Status::InvalidState,
        Some(status) if status != Status::Unknown => status,
        // A credential failure never reaches the service, so it carries no status of its own.
        _ if is_credentials_failure(source) => Status::Unauthenticated,
        // These failures have no HTTP or RPC status: DNS, connection refusal, a broken connection,
        // or a gRPC stream reset mid-flight. Match the direct REST transport's classification.
        _ if source.is_connect()
            || source.is_io()
            || (source.is_transport() && source.http_status_code().is_none()) =>
        {
            Status::IO
        }
        // An HTTP rejection without a `google.rpc.Status` body, such as a proxy's error page.
        _ => source
            .http_status_code()
            .and_then(|code| reqwest::StatusCode::from_u16(code).ok())
            .map_or(Status::Unknown, http_status),
    }
}

#[cfg(test)]
mod tests;
