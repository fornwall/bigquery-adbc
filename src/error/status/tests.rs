use super::*;
use google_cloud_gax::error::rpc::Status as GoogleStatus;

use crate::error::{DETAIL_HTTP_STATUS, DETAIL_RPC_CODE};

#[test]
fn generated_transport_failures_report_io_with_their_cause() {
    for source in [
        GoogleError::connect("connection refused"),
        GoogleError::io("connection reset"),
        // A gRPC stream reset mid-flight: `google-cloud-gax-internal` reports a tonic status that
        // carries a source as a transport error, without the status.
        GoogleError::transport(http::HeaderMap::new(), "connection reset"),
        GoogleError::exhausted(GoogleError::connect("connection refused")),
        GoogleError::exhausted(GoogleError::io("connection reset")),
    ] {
        let failure = google_error("read BigQuery rows", source);
        assert_eq!(failure.status, Status::IO);
        assert!(failure.message.contains("connection"));
        assert_eq!(failure.details, None);
    }
}

/// The one rule for replaying a Storage Read or Storage Write call: anything that ended before
/// the service decided it, and the statuses upstream recommends retrying for these APIs, but
/// never the driver's own `CANCELLED` or a verdict on the request.
#[test]
fn a_storage_call_is_retried_only_when_no_verdict_was_reached() {
    for source in [
        GoogleError::connect("connection refused"),
        GoogleError::io("connection reset"),
        GoogleError::timeout("attempt deadline elapsed"),
        GoogleError::transport(http::HeaderMap::new(), "connection reset"),
        GoogleError::http(503, http::HeaderMap::new(), bytes::Bytes::new()),
    ] {
        assert!(google_error_is_transient(&source), "{source}");
    }
    let service = |code| GoogleError::service(GoogleStatus::default().set_code(code));
    for code in [
        Code::Aborted,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::ResourceExhausted,
        Code::Unavailable,
    ] {
        assert!(google_error_is_transient(&service(code)), "{code:?}");
    }
    for code in [
        Code::Cancelled,
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::OutOfRange,
        Code::Unauthenticated,
    ] {
        assert!(!google_error_is_transient(&service(code)), "{code:?}");
    }
    assert!(!google_error_is_transient(&GoogleError::http(
        400,
        http::HeaderMap::new(),
        bytes::Bytes::new()
    )));
}

#[test]
fn exhausted_retries_keep_the_last_attempts_status_and_details() {
    for source in [
        (|| GoogleError::service(GoogleStatus::default().set_code(Code::Unavailable)))
            as fn() -> GoogleError,
        || GoogleError::service(GoogleStatus::default().set_code(Code::Cancelled)),
        || GoogleError::service(GoogleStatus::default().set_code(Code::PermissionDenied)),
        || {
            GoogleError::service(
                GoogleStatus::default()
                    .set_code(Code::ResourceExhausted)
                    .set_details([detail(&serde_json::json!({
                        "@type": "type.googleapis.com/google.rpc.RetryInfo",
                        "retryDelay": "30s",
                    }))]),
            )
        },
        || GoogleError::http(503, http::HeaderMap::new(), bytes::Bytes::new()),
        || GoogleError::timeout("request deadline elapsed"),
    ] {
        let expected = google_error("read BigQuery rows", source());
        let actual = google_error(
            "read BigQuery rows",
            GoogleError::exhausted(GoogleError::exhausted(source())),
        );
        assert_eq!(actual.status, expected.status);
        assert_eq!(actual.vendor_code, expected.vendor_code);
        assert_eq!(actual.details, expected.details);
        assert_eq!(actual.message, expected.message);
    }
    assert_eq!(
        google_error_status(&GoogleError::timeout(GoogleError::io("connection reset"))),
        Status::Timeout
    );
    assert_eq!(
        google_error_status(&GoogleError::exhausted("no last attempt")),
        Status::Unknown
    );
}

/// A rejection whose body carried no `google.rpc.Status` is classified by its HTTP status, so a
/// proxy's 503 page stays retryable rather than becoming `Unknown`.
#[test]
fn a_bare_http_rejection_is_classified_by_its_status() {
    for (code, expected) in [
        (503, Status::IO),
        (404, Status::NotFound),
        (403, Status::Unauthorized),
    ] {
        let failure = google_error(
            "BigQuery request failed",
            GoogleError::http(code, http::HeaderMap::new(), bytes::Bytes::new()),
        );
        assert_eq!(failure.status, expected, "{code}");
        assert_eq!(failure.vendor_code, i32::from(code));
    }
}

/// The status for each `reason` the BigQuery [error table] documents, which is the whole verdict
/// for a failed job (it has no HTTP code).
///
/// Only retryable conditions are `IO`: ADBC consumers and the driver's retry loops repeat a
/// request on it, so an oversized result, an over-budget query or a denylisted operation must not
/// be `IO`.
///
/// [error table]: https://cloud.google.com/bigquery/docs/error-messages
#[test]
fn bigquery_reasons_map_to_honest_statuses() {
    for (reason, expected) in [
        ("accessDenied", Status::Unauthorized),
        ("billingNotEnabled", Status::Unauthorized),
        // An outright refusal; the documented remedy is contacting support, not retrying.
        ("blocked", Status::Unauthorized),
        ("attributeError", Status::InvalidArguments),
        ("badRequest", Status::InvalidArguments),
        ("billingTierLimitExceeded", Status::InvalidArguments),
        ("invalid", Status::InvalidArguments),
        ("invalidQuery", Status::InvalidArguments),
        ("invalidUser", Status::InvalidArguments),
        // Limits on the request as written: an oversized result and an over-budget query.
        ("responseTooLarge", Status::InvalidArguments),
        ("resourcesExceeded", Status::InvalidArguments),
        ("notFound", Status::NotFound),
        ("duplicate", Status::AlreadyExists),
        ("notImplemented", Status::NotImplemented),
        ("stopped", Status::Cancelled),
        ("timeout", Status::Timeout),
        // An outbound proxy demanding credentials; the request never reached BigQuery.
        ("proxyAuthenticationRequired", Status::Unauthenticated),
        // What BigQuery documents as retryable.
        ("backendError", Status::IO),
        ("internalError", Status::IO),
        ("jobBackendError", Status::IO),
        ("jobInternalError", Status::IO),
        ("rateLimitExceeded", Status::IO),
        ("jobRateLimitExceeded", Status::IO),
        ("quotaExceeded", Status::IO),
        ("tableUnavailable", Status::IO),
        // The request is valid but the named resource is in the wrong state.
        ("resourceInUse", Status::InvalidState),
        // An unrecognized reason must not be reported as InvalidData.
        ("someFutureReason", Status::Unknown),
    ] {
        assert_eq!(bigquery_reason_status(reason), expected, "{reason}");
    }
}

/// Maps documented error reasons together with their HTTP status codes.
///
/// HTTP 403 covers access denials, quotas and oversized results. HTTP 400 also covers retryable
/// job failures: `jobBackendError`, `jobInternalError`, `jobRateLimitExceeded` and
/// `tableUnavailable`. Those four map to [`Status::IO`] both here and in
/// [`bigquery_reason_status`], so callers can retry with a new job. `timeout` and `resourceInUse`
/// also keep their job statuses, because a rejected completion read reports the job's own reason.
#[test]
fn a_rejection_reports_what_its_reason_and_code_together_earn() {
    for (reason, code, expected) in [
        ("accessDenied", 403, Status::Unauthorized),
        ("billingNotEnabled", 403, Status::Unauthorized),
        ("blocked", 403, Status::Unauthorized),
        ("responseTooLarge", 403, Status::InvalidArguments),
        // An API not enabled in the project, in its legacy and `ErrorInfo` spellings.
        ("accessNotConfigured", 403, Status::InvalidState),
        ("SERVICE_DISABLED", 403, Status::InvalidState),
        ("rateLimitExceeded", 403, Status::IO),
        ("quotaExceeded", 403, Status::IO),
        ("attributeError", 400, Status::InvalidArguments),
        ("badRequest", 400, Status::InvalidArguments),
        ("billingTierLimitExceeded", 400, Status::InvalidArguments),
        ("invalid", 400, Status::InvalidArguments),
        ("invalidQuery", 400, Status::InvalidArguments),
        ("invalidUser", 400, Status::InvalidArguments),
        ("resourcesExceeded", 400, Status::InvalidArguments),
        ("timeout", 400, Status::Timeout),
        ("resourceInUse", 400, Status::InvalidState),
        ("tableUnavailable", 400, Status::IO),
        ("jobBackendError", 400, Status::IO),
        ("jobInternalError", 400, Status::IO),
        ("jobRateLimitExceeded", 400, Status::IO),
        ("notFound", 404, Status::NotFound),
        ("proxyAuthenticationRequired", 407, Status::Unauthenticated),
        ("duplicate", 409, Status::AlreadyExists),
        ("backendError", 500, Status::IO),
        ("internalError", 500, Status::IO),
        // A 501 is the one 5xx the driver does not offer to retry.
        ("notImplemented", 501, Status::NotImplemented),
    ] {
        let code = reqwest::StatusCode::from_u16(code).unwrap();
        assert_eq!(service_status(code, Some(reason)), expected, "{reason}");
    }
    // `stopped` has no row: BigQuery reports a cancelled job in a successful response.

    // A 403 with a missing, unknown, or mismatched reason stays a denial, so a mismatched body
    // cannot reclassify it.
    let forbidden = reqwest::StatusCode::FORBIDDEN;
    assert_eq!(service_status(forbidden, None), Status::Unauthorized);
    for reason in [
        "someFutureReason",
        "notFound",
        "resourcesExceeded",
        "backendError",
        "internalError",
    ] {
        assert_eq!(
            service_status(forbidden, Some(reason)),
            Status::Unauthorized,
            "{reason}"
        );
    }

    // No other code is refined, whatever the reason.
    assert_eq!(
        service_status(reqwest::StatusCode::NOT_FOUND, Some("rateLimitExceeded")),
        Status::NotFound
    );
}

#[test]
fn bigquerys_nonstandard_cancelled_response_maps_narrowly() {
    let cancelled = reqwest::StatusCode::from_u16(499).unwrap();
    assert_eq!(http_status(cancelled), Status::Cancelled);
    assert_eq!(
        service_status(cancelled, Some("stopped")),
        Status::Cancelled
    );

    // The reason cannot turn an unrelated rejection into a cancellation; BigQuery's
    // job-timeout response is identified by its HTTP 499.
    assert_eq!(
        service_status(reqwest::StatusCode::BAD_REQUEST, Some("stopped")),
        Status::InvalidArguments
    );
}

/// A generated client's failure reports the number it was classified from: the HTTP status when
/// the transport reported one, otherwise the `google.rpc.Code`.
#[test]
fn a_service_failure_reports_the_number_its_status_was_decided_from() {
    let grpc = google_error(
        "failed to append rows",
        GoogleError::service(
            GoogleStatus::default()
                .set_code(Code::PermissionDenied)
                .set_message("no write access"),
        ),
    );
    // The service's own code and message, without gax's "the service reports an error" prose.
    assert_eq!(
        grpc.message,
        "failed to append rows: PERMISSION_DENIED: no write access"
    );
    assert_eq!(grpc.status, Status::Unauthorized);
    assert_eq!(grpc.vendor_code, 7);
    assert_eq!(
        grpc.details,
        Some(vec![(DETAIL_RPC_CODE.to_owned(), b"7".to_vec())])
    );

    let http = google_error(
        "failed to describe it",
        GoogleError::http(404, http::HeaderMap::new(), bytes::Bytes::new()),
    );
    assert_eq!(http.vendor_code, 404);
    assert_eq!(
        http.details,
        Some(vec![(DETAIL_HTTP_STATUS.to_owned(), b"404".to_vec())])
    );
}

#[test]
fn the_reported_rpc_code_numbers_are_the_ones_the_protocol_assigns() {
    for number in (0..=16).chain([99]) {
        let failure = google_error(
            "test RPC code",
            GoogleError::service(GoogleStatus::default().set_code(Code::from(number))),
        );
        let expected = if number == 99 { 2 } else { number };
        assert_eq!(failure.vendor_code, expected, "{number}");
        assert_eq!(
            failure.details,
            Some(vec![(
                DETAIL_RPC_CODE.to_owned(),
                expected.to_string().into_bytes()
            )]),
        );
    }
}

/// `Aborted` is BigQuery's concurrent-modification verdict, not `Unknown`. The typed and numeric
/// code paths must agree on it and on every other code.
#[test]
fn the_typed_and_numeric_code_tables_agree() {
    for (code, number, expected) in [
        (Code::Cancelled, 1, Status::Cancelled),
        (Code::Unknown, 2, Status::Unknown),
        (Code::InvalidArgument, 3, Status::InvalidArguments),
        (Code::DeadlineExceeded, 4, Status::Timeout),
        (Code::NotFound, 5, Status::NotFound),
        (Code::AlreadyExists, 6, Status::AlreadyExists),
        (Code::PermissionDenied, 7, Status::Unauthorized),
        (Code::ResourceExhausted, 8, Status::IO),
        (Code::FailedPrecondition, 9, Status::InvalidState),
        (Code::Aborted, 10, Status::InvalidState),
        (Code::OutOfRange, 11, Status::InvalidArguments),
        (Code::Unimplemented, 12, Status::NotImplemented),
        (Code::Internal, 13, Status::Internal),
        (Code::Unavailable, 14, Status::IO),
        (Code::DataLoss, 15, Status::Internal),
        (Code::Unauthenticated, 16, Status::Unauthenticated),
    ] {
        assert_eq!(code_status(code), expected, "{code:?}");
        assert_eq!(rpc_code_status(number), expected, "{number}");
    }
    // Success is not an error, and a number outside the enum is `Unknown`.
    assert_eq!(rpc_code_status(0), Status::Unknown);
    assert_eq!(rpc_code_status(99), Status::Unknown);
    assert_eq!(rpc_code_status(i64::from(i32::MAX) + 1), Status::Unknown);
}

#[test]
fn an_aborted_service_error_reports_invalid_state() {
    let converted = google_error(
        "BigQuery query execution failed",
        GoogleError::service(GoogleStatus::default().set_code(Code::Aborted)),
    );
    assert_eq!(converted.status, Status::InvalidState);
}

/// A Storage API not enabled in the project is refused with `PERMISSION_DENIED` and an
/// `ErrorInfo` reason `SERVICE_DISABLED`: a precondition of the project, not a missing grant.
#[test]
fn a_disabled_storage_api_is_a_precondition_rather_than_a_denial() {
    let refusal = |reason: &str| {
        GoogleError::service(
            GoogleStatus::default()
                .set_code(Code::PermissionDenied)
                .set_message("BigQuery Storage API has not been used in project 123")
                .set_details([detail(&serde_json::json!({
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": reason,
                    "domain": "googleapis.com",
                }))]),
        )
    };
    let disabled = google_error("read BigQuery rows", refusal("SERVICE_DISABLED"));
    assert_eq!(disabled.status, Status::InvalidState);
    assert!(
        disabled.message.contains("has not been used"),
        "{}",
        disabled.message
    );
    assert_eq!(
        google_error("read BigQuery rows", refusal("IAM_PERMISSION_DENIED")).status,
        Status::Unauthorized
    );
}

/// `Display for reqwest::Error` names only the request kind and URL, so refused connections, bad
/// certificates and DNS failures read alike. The underlying cause must reach the message.
#[test]
fn a_transport_failure_names_the_cause_under_reqwests_own_wording() {
    // Port 1 refuses the connection at once. Do not bind and release a port instead: a released
    // port returns to the ephemeral pool, and a parallel test's mock listener can take it and
    // answer (consuming that mock's staged response). Port 1 is below every platform's ephemeral
    // range and privileged on Unix.
    let runtime = crate::runtime::new_runtime().unwrap();
    let source = runtime.block_on(async {
        reqwest::Client::new()
            .get("http://127.0.0.1:1/")
            .send()
            .await
            .expect_err("nothing serves the reserved port 1")
    });

    let failure = request_error("submit the BigQuery jobs.query request", &source);
    assert_eq!(failure.status, Status::IO);
    assert!(
        failure
            .message
            .starts_with("failed to submit the BigQuery jobs.query request: "),
        "{}",
        failure.message
    );
    // The operating system words the refusal, and differently on each platform.
    let refusal = std::net::TcpStream::connect("127.0.0.1:1").unwrap_err();
    assert_eq!(refusal.kind(), std::io::ErrorKind::ConnectionRefused);
    assert!(
        failure.message.contains(&refusal.to_string()),
        "the refusal the operating system reported is what the caller can act on: {}",
        failure.message
    );
}

/// A status detail built from its ProtoJSON, the shape the client decodes and the driver forwards.
fn detail(value: &serde_json::Value) -> StatusDetails {
    serde_json::from_value(value.clone()).expect("a valid StatusDetails ProtoJSON")
}

/// Every `google.rpc.Status` detail reaches `AdbcErrorGetDetail`, after the code the failure
/// already reports, keyed by the type name protobuf's own descriptors report and valued with the
/// ProtoJSON a caller can decode. Repeated types stay separate entries, in the service's order.
#[test]
fn a_service_failure_forwards_every_status_detail_under_its_type_name() {
    let sent = [
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "RATE_LIMIT_EXCEEDED",
            "domain": "bigquerystorage.googleapis.com",
            "metadata": {"quota_limit": "AppendRowsPerProject"},
        }),
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.RetryInfo",
            "retryDelay": "30s",
        }),
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.QuotaFailure",
            "violations": [{"subject": "project:p", "description": "append throughput"}],
        }),
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.BadRequest",
            "fieldViolations": [{"field": "write_stream", "description": "malformed"}],
        }),
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "SECOND",
            "domain": "googleapis.com",
        }),
    ];
    let failure = google_error(
        "failed to append rows",
        GoogleError::service(
            GoogleStatus::default()
                .set_code(Code::ResourceExhausted)
                .set_message("quota exceeded")
                .set_details(sent.iter().map(detail)),
        ),
    );
    // The message, status and code are exactly what a detail-free status earns.
    assert_eq!(
        failure.message,
        "failed to append rows: RESOURCE_EXHAUSTED: quota exceeded"
    );
    assert_eq!(failure.status, Status::IO);
    assert_eq!(failure.vendor_code, 8);

    let details = failure.details.expect("the code and the status details");
    let keys: Vec<&str> = details.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(
        keys,
        [
            DETAIL_RPC_CODE,
            "google.rpc.ErrorInfo",
            "google.rpc.RetryInfo",
            "google.rpc.QuotaFailure",
            "google.rpc.BadRequest",
            "google.rpc.ErrorInfo",
        ]
    );
    for ((_, value), sent) in details[1..].iter().zip(&sent) {
        let forwarded: serde_json::Value =
            serde_json::from_slice(value).expect("a detail value is UTF-8 ProtoJSON");
        assert_eq!(&forwarded, sent);
    }
    // ProtoJSON, not a Rust rendering: a Duration is the protobuf string form.
    assert_eq!(
        details[2].1,
        br#"{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"30s"}"#
    );
}

/// A type outside `google.rpc` is forwarded under its own name. An untyped detail cannot be
/// decoded, so it is dropped rather than given an invented key.
#[test]
fn an_unrecognized_detail_type_is_forwarded_and_an_untyped_one_skipped() {
    let storage_error = serde_json::json!({
        "@type": "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError",
        "code": "STREAM_NOT_FOUND",
        "entity": "projects/p/datasets/d/tables/t/streams/s",
    });
    let untyped = serde_json::json!({"code": "STREAM_NOT_FOUND"});
    let failure = google_error(
        "failed to append rows",
        GoogleError::service(
            GoogleStatus::default()
                .set_code(Code::NotFound)
                .set_details([detail(&untyped), detail(&storage_error)]),
        ),
    );
    let details = failure.details.expect("the code and the typed detail");
    assert_eq!(details.len(), 2, "{details:?}");
    assert_eq!(details[0], (DETAIL_RPC_CODE.to_owned(), b"5".to_vec()));
    assert_eq!(
        details[1].0,
        "google.cloud.bigquery.storage.v1.StorageError"
    );
    let forwarded: serde_json::Value = serde_json::from_slice(&details[1].1).unwrap();
    assert_eq!(forwarded, storage_error);
}

/// A status without details, and a failure with no status, gain no details.
#[test]
fn a_failure_without_status_details_gains_no_details() {
    let bare = google_error(
        "failed to append rows",
        GoogleError::service(GoogleStatus::default().set_code(Code::Unavailable)),
    );
    assert_eq!(
        bare.details,
        Some(vec![(DETAIL_RPC_CODE.to_owned(), b"14".to_vec())])
    );

    let timeout = google_error(
        "failed to append rows",
        GoogleError::timeout(std::io::Error::other("deadline")),
    );
    assert_eq!(timeout.status, Status::Timeout);
    assert_eq!(timeout.details, None);
}
