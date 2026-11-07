//! Failure classification and backoff for directly issued REST requests.

use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_state::RetryState;

use crate::rest::rejection::service_failure;

#[cfg(test)]
mod tests;

/// Maximum consecutive attempts, including the first. Exhaustion reports the last failure.
/// Request timeouts and backoff bound each attempt; the loop has no separate deadline.
pub(crate) const MAX_ATTEMPTS: u32 = 5;

/// Candidate failures for retry; HTTP status and reason may still rule them out.
pub(crate) fn retryable_failure(status: Status) -> bool {
    matches!(status, Status::IO | Status::Timeout)
}

/// Retry eligibility and backoff class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryClass {
    /// Not retryable.
    Final,
    /// A server/transport failure, timeout, or propagation delay.
    Transient,
    /// HTTP 429, or a 400/403 quota reason. Uses a longer backoff.
    RateLimited,
}

/// Capacity limits; `jobRateLimitExceeded` can accompany HTTP 400.
const RATE_LIMIT_REASONS: [&str; 3] =
    ["rateLimitExceeded", "jobRateLimitExceeded", "quotaExceeded"];

/// HTTP status and reason are absent for transport failures.
pub(crate) fn retry_class(
    http: Option<reqwest::StatusCode>,
    reason: Option<&str>,
    status: Status,
) -> RetryClass {
    // Retrying a job that ran out of time would hit the same deadline.
    if !retryable_failure(status) || matches!(reason, Some("stopped" | "timeout")) {
        return RetryClass::Final;
    }
    let Some(http) = http else {
        return RetryClass::Transient;
    };
    let rate_limited = reason.is_some_and(|reason| RATE_LIMIT_REASONS.contains(&reason));
    match http {
        reqwest::StatusCode::TOO_MANY_REQUESTS => RetryClass::RateLimited,
        reqwest::StatusCode::FORBIDDEN if !rate_limited => RetryClass::Final,
        reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::BAD_REQUEST if rate_limited => {
            RetryClass::RateLimited
        }
        _ => RetryClass::Transient,
    }
}

/// Return a successful response body or a classified failure, rendered under `context`.
pub(crate) fn classify(
    answer: Result<(reqwest::StatusCode, Bytes)>,
    context: &str,
) -> std::result::Result<Bytes, (RetryClass, Error)> {
    let (status, body) =
        answer.map_err(|failure| (retry_class(None, None, failure.status), failure))?;
    if status.is_success() {
        return Ok(body);
    }
    let (failure, reason) = service_failure(context, status, &body);
    Err((
        retry_class(Some(status), reason.as_deref(), failure.status),
        failure,
    ))
}

/// Exponential backoff with full jitter to spread concurrent retries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Schedule {
    initial: Duration,
    maximum: Duration,
    scaling: f64,
}

impl Schedule {
    pub(crate) const fn new(initial: Duration, maximum: Duration, scaling: f64) -> Self {
        Self {
            initial,
            maximum,
            scaling,
        }
    }

    /// The jittered delay to wait after `failures` failed attempts.
    pub(crate) fn delay(self, failures: u32) -> Duration {
        ExponentialBackoffBuilder::new()
            .with_initial_delay(self.initial)
            .with_maximum_delay(self.maximum)
            .with_scaling(self.scaling)
            .build()
            .expect("a BigQuery retry schedule names a valid backoff")
            .on_failure(&RetryState::new(true).set_attempt_count(failures))
    }
}

/// Backoff schedules per failure class. Tests substitute short, flat delays.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryDelays {
    transient: Schedule,
    rate_limited: Schedule,
}

impl RetryDelays {
    /// Submission, result retrieval, and metadata requests. Four retries wait at most 5.25 s
    /// for transient failures or 21 s for rate limits shared by concurrent dbt models.
    pub(crate) const SERVICE: Self = Self {
        transient: Schedule::new(Duration::from_millis(250), Duration::from_secs(2), 4.0),
        rate_limited: Schedule::new(Duration::from_secs(1), Duration::from_secs(8), 4.0),
    };

    /// Synchronous `jobs.cancel` and ambiguous-submission reconciliation: at most 1.05 s of backoff.
    pub(crate) const PROMPT: Self = {
        let blocking = Schedule::new(Duration::from_millis(50), Duration::from_millis(400), 4.0);
        Self {
            transient: blocking,
            rate_limited: blocking,
        }
    };

    /// Job and LRO polls: at most 1.5 s transient or 21 s rate-limited backoff, matching the poll
    /// cadence and [`Self::SERVICE`]'s quota window respectively.
    pub(crate) const POLL: Self = Self {
        transient: Schedule::new(Duration::from_millis(100), Duration::from_millis(800), 2.0),
        rate_limited: Schedule::new(Duration::from_secs(1), Duration::from_secs(8), 4.0),
    };

    pub(crate) fn delay(self, class: RetryClass, failures: u32) -> Duration {
        match class {
            RetryClass::RateLimited => self.rate_limited.delay(failures),
            _ => self.transient.delay(failures),
        }
    }

    /// Every class on one flat schedule, for tests.
    #[cfg(test)]
    pub(crate) const fn flat(delay: Duration) -> Self {
        let schedule = Schedule::new(delay, delay, 2.0);
        Self {
            transient: schedule,
            rate_limited: schedule,
        }
    }
}
