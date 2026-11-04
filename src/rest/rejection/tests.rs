//! Tests for how a service rejection reads: what it says, and what it never echoes.

use adbc_core::error::Status;

use super::{precondition_error, service_error, service_rejection};
use crate::error::{DETAIL_HTTP_STATUS, DETAIL_LOCATION, DETAIL_REASON};
use crate::test_support::errors::assert_message_omits;

/// A rate limit and a permission failure are both 403; the `reason` tells them apart.
#[test]
fn a_throttled_request_is_retryable_rather_than_a_permissions_failure() {
    let body = br#"{
        "error": {
            "code": 403,
            "message": "Quota exceeded",
            "errors": [{"reason": "rateLimitExceeded", "message": "Quota exceeded"}]
        }
    }"#;
    let failure = service_error(
        "failed to describe it",
        reqwest::StatusCode::FORBIDDEN,
        body,
    );
    assert_eq!(failure.status, Status::IO);
    assert_eq!(
        failure.message,
        "failed to describe it: rateLimitExceeded: Quota exceeded (HTTP 403)"
    );
    assert_eq!(
        failure.details,
        Some(vec![
            (DETAIL_REASON.to_owned(), b"rateLimitExceeded".to_vec()),
            (DETAIL_HTTP_STATUS.to_owned(), b"403".to_vec()),
        ])
    );
    assert_eq!(failure.sqlstate, [0; 5]);

    // A genuine 403, or one with an unknown reason, reports a permission failure rather than
    // the `Unknown` an unknown reason maps to.
    for reason in ["accessDenied", "someFutureReason"] {
        let body = format!(r#"{{"error": {{"code": 403, "errors": [{{"reason": "{reason}"}}]}}}}"#);
        let failure = service_error(
            "failed to describe it",
            reqwest::StatusCode::FORBIDDEN,
            body.as_bytes(),
        );
        assert_eq!(failure.status, Status::Unauthorized, "{reason}");
    }
}

/// Every entry renders the way a failed query job's does, and upstream's `debug_info` never
/// appears.
#[test]
fn service_errors_render_every_entry_the_way_a_failed_job_does() {
    let body = br#"{
        "error": {
            "code": 400,
            "message": "flattened first line only",
            "status": "INVALID_ARGUMENT",
            "errors": [
                {
                    "reason": "invalidQuery",
                    "location": "query",
                    "debugInfo": "google-internal",
                    "message": "Unrecognized name: colum"
                },
                {"reason": "invalid", "message": "secondary failure"}
            ]
        },
        "secret": "do-not-echo"
    }"#;
    let failure = service_error("failed to patch it", reqwest::StatusCode::BAD_REQUEST, body);
    assert_eq!(
        failure.message,
        "failed to patch it: invalidQuery: Unrecognized name: colum (field: query); \
         invalid: secondary failure (HTTP 400)"
    );
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.details,
        Some(vec![
            (DETAIL_REASON.to_owned(), b"invalidQuery".to_vec()),
            (DETAIL_LOCATION.to_owned(), b"query".to_vec()),
            (DETAIL_HTTP_STATUS.to_owned(), b"400".to_vec()),
        ])
    );
    assert_eq!(failure.sqlstate, [0x34, 0x32, 0x30, 0x30, 0x30]);
    assert_message_omits(&failure, &["google-internal", "do-not-echo"]);
}

/// A 404 from a metadata patch carries the `notFound:` spelling a job error carries.
#[test]
fn a_not_found_rejection_keeps_the_spelling_dbt_matches() {
    let body = br#"{
        "error": {
            "code": 404,
            "message": "Not found: Table proj:ds.missing",
            "errors": [
                {"reason": "notFound", "message": "Not found: Table proj:ds.missing"}
            ]
        }
    }"#;
    let failure = service_error(
        "failed to update the description of BigQuery table proj.ds.missing",
        reqwest::StatusCode::NOT_FOUND,
        body,
    );
    assert_eq!(failure.status, Status::NotFound);
    assert!(
        failure.message.contains("notFound:"),
        "dbt-core 2.0's not-found matcher needs the notFound: substring: {}",
        failure.message
    );
}

/// The transport's own verdict outranks the first entry's `reason`, which maps to `Unknown`
/// for anything the driver has not seen before.
#[test]
fn an_unrecognized_reason_still_reports_the_http_status() {
    let body = br#"{
        "error": {"code": 503, "errors": [{"reason": "someFutureReason", "message": "later"}]}
    }"#;
    let failure = service_error(
        "failed to poll it",
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        body,
    );
    assert_eq!(failure.status, Status::IO);
    assert_eq!(
        failure.message,
        "failed to poll it: someFutureReason: later (HTTP 503)"
    );
}

/// An API not enabled in the project is a precondition, not a missing grant. Dataproc reports it
/// only in a `google.rpc.ErrorInfo` detail, and BigQuery also as an `accessNotConfigured` entry;
/// the service's message, which says how to enable the API, is kept.
#[test]
fn a_disabled_api_is_a_precondition_rather_than_a_denial() {
    let dataproc = br#"{"error": {"code": 403, "status": "PERMISSION_DENIED",
        "message": "Cloud Dataproc API has not been used in project 123 before or it is disabled.",
        "details": [
            {"@type": "type.googleapis.com/google.rpc.Help", "links": []},
            {"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "SERVICE_DISABLED",
             "domain": "googleapis.com"}
        ]}}"#;
    let bigquery = br#"{"error": {"code": 403, "message": "BigQuery API has not been used",
        "errors": [{"message": "BigQuery API has not been used", "domain": "usageLimits",
            "reason": "accessNotConfigured"}]}}"#;
    for body in [&dataproc[..], &bigquery[..]] {
        let failure = service_error("failed to submit it", reqwest::StatusCode::FORBIDDEN, body);
        assert_eq!(failure.status, Status::InvalidState, "{}", failure.message);
        assert!(
            failure.message.contains("has not been used"),
            "{}",
            failure.message
        );
    }
}

#[test]
fn rejections_without_structured_errors_fall_back_to_the_message_then_the_status() {
    let body = br#"{"error": {"code": 403, "message": "Access Denied"}, "s": "do-not-echo"}"#;
    let failure = service_error("failed to write it", reqwest::StatusCode::FORBIDDEN, body);
    assert_eq!(
        failure.message,
        "failed to write it: Access Denied (HTTP 403)"
    );
    assert_eq!(failure.status, Status::Unauthorized);
    assert_message_omits(&failure, &["do-not-echo"]);

    // A malformed `errors` falls back to the flat message the same way.
    let body = br#"{"error": {"code": 403, "message": "Access Denied", "errors": {}}}"#;
    let failure = service_error("failed to write it", reqwest::StatusCode::FORBIDDEN, body);
    assert_eq!(
        failure.message,
        "failed to write it: Access Denied (HTTP 403)"
    );

    // An unparseable body leaves the status, named as well as numbered.
    let failure = service_error(
        "failed to write it",
        reqwest::StatusCode::NOT_FOUND,
        b"<html>gateway</html>",
    );
    // A 404 that is not a Google API error document says so, rather than reading like a
    // missing table.
    assert_eq!(
        failure.message,
        "failed to write it: Not Found (HTTP 404); the response is not a Google API error, so \
         the URL may not be a Google API; check bigquery.endpoint and the other endpoint options"
    );
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(
        service_rejection(reqwest::StatusCode::from_u16(599).unwrap(), b""),
        "the service sent no message (HTTP 599)",
        "a code with no canonical reason is still reported"
    );
}

/// Every rejection records its HTTP status outside the message: in `vendor_code` for an ADBC
/// 1.0.0 caller (what a JDBC or ODBC bridge reports as `SQLException.getErrorCode()`), and in a
/// detail for a 1.1.0 caller, whose `vendor_code` is the layout sentinel.
#[test]
fn every_rejection_reports_its_http_status_as_a_number_a_consumer_can_branch_on() {
    let structured = br#"{"error": {"code": 404, "errors": [{"reason": "notFound"}]}}"#;
    let bare = br#"{"error": {"code": 503, "message": "Service Unavailable"}}"#;
    for (body, status) in [(structured.as_slice(), 404), (bare.as_slice(), 503)] {
        let code = reqwest::StatusCode::from_u16(status).unwrap();
        let failure = service_error("failed to describe it", code, body);
        assert_eq!(failure.vendor_code, i32::from(status), "HTTP {status}");
        assert!(
            failure.details.unwrap_or_default().contains(&(
                DETAIL_HTTP_STATUS.to_owned(),
                status.to_string().into_bytes()
            )),
            "HTTP {status}"
        );
    }
}

/// A 412 reports the concurrent change; every other status falls through to the ordinary
/// rejection.
#[test]
fn a_precondition_failure_reports_the_concurrent_change() {
    let body = br#"{
        "error": {
            "code": 412,
            "message": "Precondition check failed.",
            "errors": [{"reason": "conditionNotMet", "message": "Precondition check failed."}]
        }
    }"#;
    let failure = precondition_error(
        "failed to authorize view v on BigQuery dataset p.d",
        reqwest::StatusCode::PRECONDITION_FAILED,
        body,
        "the dataset access list changed concurrently; retry the authorization",
    );
    assert_eq!(failure.status, Status::InvalidState);
    assert_eq!(
        failure.message,
        "failed to authorize view v on BigQuery dataset p.d: the dataset access list \
         changed concurrently; retry the authorization"
    );

    // Every other status is an ordinary rejection, rendered like any other.
    let body = br#"{"error": {"code": 403, "message": "Access Denied"}}"#;
    let failure = precondition_error(
        "failed to authorize view v on BigQuery dataset p.d",
        reqwest::StatusCode::FORBIDDEN,
        body,
        "the dataset access list changed concurrently; retry the authorization",
    );
    assert_eq!(failure.status, Status::Unauthorized);
    assert!(failure.message.ends_with(": Access Denied (HTTP 403)"));
}

/// A structured entry that names only its reason borrows the envelope's message rather than
/// reading "BigQuery gave no message".
#[test]
fn an_entry_without_a_message_reads_the_envelope_message() {
    let body = br#"{"error": {"code": 404, "message": "Not found: Table p:d.t",
        "errors": [{"reason": "notFound"}]}}"#;
    let failure = service_error(
        "failed to describe it",
        reqwest::StatusCode::NOT_FOUND,
        body,
    );
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(
        failure.message,
        "failed to describe it: notFound: Not found: Table p:d.t (HTTP 404)"
    );
}
