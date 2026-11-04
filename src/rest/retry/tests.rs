//! The retry classification and the schedules, pinned against the tables they depend on.

use adbc_core::error::Status;

use super::{MAX_ATTEMPTS, RATE_LIMIT_REASONS, RetryClass, RetryDelays, retry_class};

/// 403 pacing depends on [`RATE_LIMIT_REASONS`] agreeing with `service_status` in `crate::error`,
/// which sets the status [`retry_class`] checks first. Every rate-limit reason must refine to a
/// retryable status and classify as `RateLimited`; every other documented 403 reason is final.
#[test]
fn a_403_is_paced_as_a_quota_for_exactly_the_rate_limit_reasons() {
    let forbidden = reqwest::StatusCode::FORBIDDEN;
    for reason in [
        "rateLimitExceeded",
        "jobRateLimitExceeded",
        "quotaExceeded",
        "accessDenied",
        "billingNotEnabled",
        "blocked",
        "responseTooLarge",
        "someFutureReason",
    ] {
        let status = crate::error::service_status(forbidden, Some(reason));
        let expected = if RATE_LIMIT_REASONS.contains(&reason) {
            RetryClass::RateLimited
        } else {
            RetryClass::Final
        };
        assert_eq!(
            retry_class(Some(forbidden), Some(reason), status),
            expected,
            "{reason} reported as {status:?}"
        );
    }
}

/// Only the four retryable 400 reasons in the error table are retried; any other 400 is final.
/// `jobRateLimitExceeded` takes the rate-limit schedule, the three backend failures the transient
/// one.
#[test]
fn a_400_is_retried_for_exactly_the_four_job_reasons_the_error_table_documents() {
    let bad_request = reqwest::StatusCode::BAD_REQUEST;
    for (reason, expected) in [
        ("jobRateLimitExceeded", RetryClass::RateLimited),
        ("jobBackendError", RetryClass::Transient),
        ("jobInternalError", RetryClass::Transient),
        ("tableUnavailable", RetryClass::Transient),
        ("invalidQuery", RetryClass::Final),
        ("resourcesExceeded", RetryClass::Final),
        ("badRequest", RetryClass::Final),
        ("timeout", RetryClass::Final),
        // A reason BigQuery documents for another code may not reclassify this one, and neither
        // may a reason this driver has never seen.
        ("rateLimitExceeded", RetryClass::Final),
        ("backendError", RetryClass::Final),
        ("someFutureReason", RetryClass::Final),
    ] {
        let status = crate::error::service_status(bad_request, Some(reason));
        assert_eq!(
            retry_class(Some(bad_request), Some(reason), status),
            expected,
            "{reason} reported as {status:?}"
        );
    }
    // A 400 with no structured reason at all stays the malformed request its code claims.
    let status = crate::error::service_status(bad_request, None);
    assert_eq!(status, Status::InvalidArguments);
    assert_eq!(
        retry_class(Some(bad_request), None, status),
        RetryClass::Final
    );
}

/// A job stopped by `bigquery.query.job_timeout` answers HTTP 499 `stopped`. It reads TIMEOUT,
/// not a cancel, and unlike other timeouts is not retried: a retry would hit the same deadline.
#[test]
fn a_job_timeout_is_a_final_timeout_and_a_remote_cancel_stays_cancelled() {
    let stopped = reqwest::StatusCode::from_u16(499).unwrap();
    let body = |message: &str| {
        bytes::Bytes::from(format!(
            r#"{{"error":{{"errors":[{{"reason":"stopped","message":"{message}"}}]}}}}"#
        ))
    };
    for (message, expected) in [
        (
            "Job execution was cancelled: Job timed out after 2 sec",
            Status::Timeout,
        ),
        (
            "Job execution was cancelled: User requested cancellation",
            Status::Cancelled,
        ),
    ] {
        let (class, failure) =
            super::classify(Ok((stopped, body(message))), "BigQuery rejected the query")
                .unwrap_err();
        assert_eq!(failure.status, expected, "{message}");
        assert_eq!(class, RetryClass::Final, "{message}");
    }
}

/// Pins the documented schedule numbers. Each maximum must be reached within the four retries,
/// or it bounds nothing, and the total window must stay bounded because the loops have an
/// attempt count but no deadline.
#[test]
fn every_retry_schedule_binds_its_ceiling_and_bounds_its_whole_window() {
    let retries = MAX_ATTEMPTS - 1;

    for (name, schedule, window) in [
        ("service transient", RetryDelays::SERVICE.transient, 5.25),
        (
            "service rate limited",
            RetryDelays::SERVICE.rate_limited,
            21.0,
        ),
        ("prompt transient", RetryDelays::PROMPT.transient, 1.05),
        (
            "prompt rate limited",
            RetryDelays::PROMPT.rate_limited,
            1.05,
        ),
        ("poll transient", RetryDelays::POLL.transient, 1.5),
        ("poll rate limited", RetryDelays::POLL.rate_limited, 21.0),
    ] {
        let maximum = schedule.maximum.as_secs_f64();
        let nominal = |retry: u32| {
            (schedule.initial.as_secs_f64() * schedule.scaling.powi(i32::try_from(retry).unwrap()))
                .min(maximum)
        };
        let last = retries - 1;
        assert!(
            nominal(last) >= maximum,
            "the {name} schedule never reaches its {maximum} s ceiling in {retries} retries"
        );
        let total: f64 = (0..retries).map(nominal).sum();
        assert!(
            (total - window).abs() < 0.001,
            "the {name} schedule spends at most {total} s, not the documented {window} s"
        );
    }
}
