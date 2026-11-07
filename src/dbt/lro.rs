//! Polling, cancellation and error handling for Google long-running operations.

use std::fmt::Write as _;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use google_cloud_auth::credentials::Credentials;
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{error, record_json_status_details, record_rpc_code, rpc_code_status};
use crate::rest::client::RestClient;
use crate::rest::json::decode_json;
use crate::rest::request::{Request, send};
use crate::rest::retry::{MAX_ATTEMPTS, RetryClass, RetryDelays, classify};
use crate::rest::url::escape_resource_path;

#[cfg(test)]
mod tests;

/// Exponential poll backoff, bounded by an optional deadline and interruptible by cancellation.
pub(super) struct PollBackoff {
    delay: Duration,
    maximum: Duration,
    /// The `adbc.bigquery.dataproc.pooling_timeout` deadline, or `None` to wait indefinitely.
    deadline: Option<Instant>,
}

impl PollBackoff {
    pub(super) fn new(initial: Duration, maximum: Duration, deadline: Option<Instant>) -> Self {
        Self {
            delay: initial,
            maximum,
            deadline,
        }
    }

    fn expired(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Sleeps and doubles the backoff, clamped to the deadline. Returns whether time remains
    /// for another poll. `floor` applies a quota delay without extending the deadline.
    pub(super) async fn pause(&mut self, operation: &Operation, floor: Duration) -> Result<bool> {
        let mut nap = self.delay.max(floor);
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            nap = nap.min(remaining);
        }
        operation.wait(tokio::time::sleep(nap)).await?;
        self.delay = (self.delay * 2).min(self.maximum);
        Ok(!self.expired())
    }
}

/// How [`LroPoller::request`] sends a request and what it may resend.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Delivery {
    /// A read or a repeatable write: resent after a transient failure, dropped on cancellation.
    Idempotent,
    /// A submission the service deduplicates by request ID: resent likewise, but never dropped
    /// in flight, because only its answer names the work a cancel has to stop.
    Deduplicated,
    /// A submission without a request ID: like [`Self::Deduplicated`], but a transient failure
    /// may have started the work, so only a rate-limit rejection, which applied nothing, resends.
    Once,
}

/// A cancellable poll loop and shared client for one regional service.
pub(super) struct LroPoller<'a> {
    operation: &'a Operation,
    credentials: &'a Credentials,
    http: RestClient,
    /// The regional host every URL is built on, without its trailing slash.
    endpoint: String,
    backoff: PollBackoff,
    what: &'a str,
    seconds: i64,
    /// The submitted resource, once the submission response names it.
    name: Option<String>,
}

/// Backoff, deadline, and the names a long-running operation's messages use.
#[derive(Clone, Copy, Default)]
pub(super) struct PollBudget<'a> {
    /// The first delay between polls, doubled up to `maximum`.
    pub(super) initial: Duration,
    pub(super) maximum: Duration,
    /// The `adbc.bigquery.dataproc.pooling_timeout` deadline, or `None` to wait indefinitely.
    pub(super) deadline: Option<Instant>,
    /// The `pooling_timeout` in seconds, which an expired deadline names.
    pub(super) seconds: i64,
    /// The kind of work waited on, such as "Dataproc batch".
    pub(super) what: &'a str,
}

impl<'a> LroPoller<'a> {
    pub(super) fn new(
        operation: &'a Operation,
        credentials: &'a Credentials,
        http: RestClient,
        endpoint: String,
        budget: PollBudget<'a>,
    ) -> Self {
        Self {
            operation,
            credentials,
            http,
            endpoint,
            backoff: PollBackoff::new(budget.initial, budget.maximum, budget.deadline),
            what: budget.what,
            seconds: budget.seconds,
            name: None,
        }
    }

    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(super) fn resource_url(&self, name: &str, suffix: &str) -> String {
        resource_url(&self.endpoint, name, suffix)
    }

    /// Only statement cancellation or an expired deadline justifies cancelling remote work.
    /// A gateway timeout also has [`Status::Timeout`], but may leave a healthy job running.
    pub(super) fn abandoned(&self, failure: &Error) -> bool {
        match failure.status {
            Status::Cancelled => self.operation.is_cancelled(),
            Status::Timeout => self.backoff.expired(),
            _ => false,
        }
    }

    pub(super) fn set_running(&mut self, name: String) {
        self.name = Some(name);
    }

    pub(super) fn running(&self) -> String {
        match &self.name {
            Some(name) => format!("{} {name}", self.what),
            None => self.what.to_owned(),
        }
    }

    /// What an expired `pooling_timeout` reports; [`Self::cancel`] appends whether the work was
    /// stopped.
    pub(super) fn timeout(&self) -> Error {
        error(
            format!(
                "the BigQuery {} did not complete within the {} option's {} s",
                self.running(),
                BigQueryStatementOption::DataprocPoolingTimeout.as_str(),
                self.seconds
            ),
            Status::Timeout,
        )
    }

    /// Abandoned waits are handled separately by the caller, which attempts cancellation.
    fn left_running(&self, mut failure: Error) -> Error {
        if self.name.is_some() && !self.abandoned(&failure) {
            let _ = write!(
                failure.message,
                "; the BigQuery {} may still be running",
                self.running()
            );
        }
        failure
    }

    /// Sends `request` under `delivery`, within the deadline and the [`MAX_ATTEMPTS`] limit. Errors
    /// must not expose raw response bodies.
    /// The borrowed factory shares one retry state machine across all callers.
    pub(super) async fn request<'r>(
        &self,
        delivery: Delivery,
        request: &dyn Fn() -> Request<'r>,
    ) -> Result<JsonValue> {
        let gate = (delivery == Delivery::Idempotent).then_some(self.operation);
        let mut failures = 0;
        loop {
            self.operation.check()?;
            if self.backoff.expired() {
                return Err(self.timeout());
            }
            let (class, failure) = match Box::pin(self.attempt(gate, request())).await {
                Ok(answer) => return Ok(answer),
                Err(rejected) => rejected,
            };
            failures += 1;
            let resend = match class {
                RetryClass::Final => false,
                RetryClass::Transient => delivery != Delivery::Once,
                RetryClass::RateLimited => true,
            };
            if !resend || failures >= MAX_ATTEMPTS || self.backoff.expired() {
                return Err(failure);
            }
            diag!(
                Debug,
                "resending a request after transient failure {failures}: {}",
                failure.message
            );
            let delay = RetryDelays::SERVICE.delay(class, failures);
            let wake = Instant::now() + delay;
            let wake = self
                .backoff
                .deadline
                .map_or(wake, |deadline| wake.min(deadline));
            self.operation.wait(tokio::time::sleep_until(wake)).await?;
        }
    }

    /// One send, with the failure classified for a retry loop. A 2xx body that does not decode
    /// may be cut short, so it is transient; a JSON document of the wrong shape is final.
    async fn attempt(
        &self,
        gate: Option<&Operation>,
        request: Request<'_>,
    ) -> std::result::Result<JsonValue, (RetryClass, Error)> {
        let context = request.context;
        let answer = send(gate, self.credentials, &self.http, request).await;
        let body = classify(answer, &format!("failed to {context}"))?;
        let value: JsonValue = decode_json(&body, &format!("the response to {context}"))
            .map_err(|failure| (RetryClass::Transient, failure))?;
        if value.is_object() {
            Ok(value)
        } else {
            Err((
                RetryClass::Final,
                error(
                    format!("the response to {context} is not a JSON object"),
                    Status::InvalidData,
                ),
            ))
        }
    }

    /// Polls until `done`, starting with the submission response to avoid polling completed work.
    pub(super) async fn wait_until_done(
        &mut self,
        url: &str,
        context: &str,
        mut latest: JsonValue,
    ) -> Result<JsonValue> {
        loop {
            match latest.get("done") {
                Some(JsonValue::Bool(true)) => return Ok(latest),
                None | Some(JsonValue::Null | JsonValue::Bool(false)) => {
                    latest = self.poll(url, context).await?;
                }
                Some(_) => {
                    return Err(self.left_running(error(
                        format!("the response to {context} has a non-boolean done field"),
                        Status::InvalidData,
                    )));
                }
            }
        }
    }

    pub(super) async fn poll(&mut self, url: &str, context: &str) -> Result<JsonValue> {
        let mut transient = 0_u32;
        let mut quota = Duration::ZERO;
        loop {
            if !self.backoff.pause(self.operation, quota).await? {
                return Err(self.timeout());
            }
            let poll = self.attempt(Some(self.operation), Request::get(url, context));
            let (class, failure) = match Box::pin(poll).await {
                Ok(polled) => return Ok(polled),
                Err(rejected) => rejected,
            };
            transient += 1;
            if class == RetryClass::Final || transient >= MAX_ATTEMPTS {
                return Err(self.left_running(failure));
            }
            quota = match class {
                RetryClass::RateLimited => {
                    RetryDelays::POLL.delay(RetryClass::RateLimited, transient)
                }
                _ => Duration::ZERO,
            };
            diag!(
                Debug,
                "retrying a poll of {url} after transient failure {transient}: {}",
                failure.message
            );
        }
    }

    /// Attempts remote cancellation within [`REMOTE_CANCEL_TIMEOUT`], bypassing local
    /// cancellation and resending after transient failures. Appends the outcome without changing
    /// the original status: a failed cancellation may leave work running and billing.
    pub(super) async fn cancel(&self, url: &str, mut abandoned: Error) -> Error {
        let what = self.running();
        let context = format!("cancel the BigQuery {what}");
        let stop = async {
            let mut failures = 0;
            loop {
                let request = Request::post_json(url, &b"{}"[..], &context);
                let answer = send(None, self.credentials, &self.http, request).await;
                let Err((class, failure)) = classify(answer, "") else {
                    return None;
                };
                failures += 1;
                if class == RetryClass::Final || failures >= MAX_ATTEMPTS {
                    return Some(failure.message);
                }
                tokio::time::sleep(RetryDelays::PROMPT.delay(class, failures)).await;
            }
        };
        let detail = match tokio::time::timeout(REMOTE_CANCEL_TIMEOUT, Box::pin(stop)).await {
            Ok(None) => {
                let _ = write!(abandoned.message, "; the BigQuery {what} was asked to stop");
                return abandoned;
            }
            Ok(Some(detail)) => detail,
            Err(_) => format!("no answer within {} s", REMOTE_CANCEL_TIMEOUT.as_secs()),
        };
        let _ = write!(
            abandoned.message,
            "; {context} failed ({detail}), so it may keep running"
        );
        abandoned
    }
}

/// Builds a URL from a resource name validated at the option or response boundary.
pub(super) fn resource_url(endpoint: &str, name: &str, suffix: &str) -> String {
    format!("{endpoint}/v1/{}{suffix}", escape_resource_path(name))
}

/// Reads a resource name, rejecting segments that URL parsing would normalize away.
pub(super) fn resource_name(response: &JsonValue, what: &str) -> Result<String> {
    let name = response
        .get("name")
        .and_then(JsonValue::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            error(
                format!("the BigQuery {what} response carries no resource name"),
                Status::InvalidData,
            )
        })?;
    if name
        .split('/')
        .any(|segment| matches!(segment, "" | "." | ".."))
    {
        return Err(error(
            format!("the BigQuery {what} response carries an invalid resource name"),
            Status::InvalidData,
        ));
    }
    Ok(name.to_owned())
}

/// Bounds the extra wait for remote cancellation before returning the caller's error.
const REMOTE_CANCEL_TIMEOUT: Duration = Duration::from_secs(30);

/// Maps a `google.rpc.Status` and preserves its structured details.
pub(super) fn rpc_status_error(context: &str, status: Option<&JsonValue>, fallback: &str) -> Error {
    let detail = status
        .and_then(|status| status.get("message"))
        .and_then(JsonValue::as_str)
        .filter(|message| !message.is_empty())
        .unwrap_or(fallback);
    let code = status
        .and_then(|status| status.get("code"))
        .and_then(JsonValue::as_i64);
    let mut failure = error(
        format!("{context}: {detail}"),
        code.map_or(Status::Unknown, rpc_code_status),
    );
    if let Some(code) = code {
        record_rpc_code(&mut failure, code);
    }
    if let Some(status) = status {
        record_json_status_details(&mut failure, status);
    }
    failure
}

/// An operation-level error takes precedence over the resource's state.
pub(super) fn operation_failure(
    completed: &JsonValue,
    context: &str,
    fallback: &str,
) -> Option<Error> {
    completed
        .get("error")
        .filter(|failure| !failure.is_null())
        .map(|failure| rpc_status_error(context, Some(failure), fallback))
}
