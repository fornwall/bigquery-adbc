//! Tests for the shared wait: the backoff's growth and deadline, and the failure budget.

use std::time::Duration;

use adbc_core::error::Status;
use google_cloud_auth::credentials::Credentials;
use tokio::time::Instant;

use super::PollBackoff;
use crate::rest::client::rest_client;
use crate::rest::request::Request;
use crate::rest::retry::MAX_ATTEMPTS;
use crate::test_support::errors::assert_message;

#[test]
fn an_expired_request_budget_sends_nothing() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let mut poller = test_poller(&operation, &credentials, "http://unused".to_owned());
    poller.backoff.deadline = Some(Instant::now());
    let attempts = std::cell::Cell::new(0);

    for idempotent in [true, false] {
        let failure = runtime
            .block_on(poller.request(idempotent, &|| {
                attempts.set(attempts.get() + 1);
                Request::get("invalid-url", "test the deadline")
            }))
            .unwrap_err();
        assert_eq!(failure.status, Status::Timeout);
        assert_message(&failure, &["pooling_timeout"]);
    }
    assert_eq!(attempts.get(), 0);
}

#[test]
fn request_retries_stop_when_the_deadline_expires_during_backoff() {
    use std::future::Future as _;
    use std::task::{Context, Waker};

    let driver_runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&driver_runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut poller = test_poller(&operation, &credentials, "http://unused".to_owned());
        // Tokio rounds sleep deadlines to milliseconds. Start between ticks so even zero
        // retry jitter yields a pending timer, then expire on the next tick.
        tokio::time::advance(Duration::from_micros(1)).await;
        let started = Instant::now();
        let budget = Duration::from_micros(999);
        poller.backoff.deadline = Some(started + budget);
        let attempts = std::cell::Cell::new(0);
        // An invalid URL fails synchronously as a retryable transport error, so the first
        // pending poll is the retry sleep. No sockets or wall-clock timing are involved.
        let factory = || {
            attempts.set(attempts.get() + 1);
            Request::get("invalid-url", "test the retry deadline")
        };
        let request = poller.request(true, &factory);
        tokio::pin!(request);
        assert!(
            request
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(attempts.get(), 1);

        tokio::time::advance(budget).await;
        let failure = request.await.unwrap_err();
        assert_eq!(failure.status, Status::Timeout);
        assert_eq!(attempts.get(), 1);
        assert_eq!(started.elapsed(), budget);
    });
}

#[test]
fn malformed_completion_flags_fail_without_polling() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let (endpoint, server) = crate::test_support::http::capture_requests_until_stopped(vec![(
        200,
        serde_json::json!({"done": true}),
    )]);
    let mut poller = test_poller(&operation, &credentials, endpoint.clone());

    for done in [
        serde_json::json!("true"),
        serde_json::json!(0),
        serde_json::json!([]),
        serde_json::json!({}),
    ] {
        let failure = runtime
            .block_on(poller.wait_until_done(
                &format!("{endpoint}/v1/operations/1"),
                "poll the operation",
                serde_json::json!({"done": done}),
            ))
            .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_message(&failure, &["non-boolean done field"]);
    }
    assert!(server.stop().is_empty());
}

#[test]
fn unset_completion_flags_poll_until_done() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let (endpoint, captured) = crate::test_support::http::capture_requests(vec![
        (200, serde_json::json!({"done": null})),
        (200, serde_json::json!({"done": false})),
        (200, serde_json::json!({"done": true})),
    ]);
    let mut poller = test_poller(&operation, &credentials, endpoint.clone());
    let completed = runtime
        .block_on(poller.wait_until_done(
            &format!("{endpoint}/v1/operations/1"),
            "poll the operation",
            serde_json::json!({}),
        ))
        .unwrap();
    assert_eq!(completed, serde_json::json!({"done": true}));
    assert_eq!(captured.join().unwrap().len(), 3);
}

#[test]
fn response_resource_names_cannot_change_the_request_path() {
    for name in [
        "",
        ".",
        "..",
        "/operations/1",
        "operations/",
        "operations//1",
        "projects/p/../operations/1",
        "projects/./operations/1",
        "operations/..",
    ] {
        let failure =
            super::resource_name(&serde_json::json!({"name": name}), "operation").unwrap_err();
        assert_eq!(failure.status, Status::InvalidData, "{name:?}");
    }
    for name in [
        "operations/1",
        "projects/p/locations/r/operations/1",
        "operations/a.b",
    ] {
        assert_eq!(
            super::resource_name(&serde_json::json!({"name": name}), "operation").unwrap(),
            name,
        );
    }
}

/// `PollBackoff` doubles, caps, never sleeps past the deadline, and returns `true` at the
/// deadline, which `poll` must recheck. The deadline half pairs a ten-second delay with a
/// five-millisecond budget, so sleeping the full delay would hang the test.
#[test]
fn the_poll_backoff_doubles_caps_and_stops_exactly_at_the_deadline() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&runtime);

    runtime.block_on(async {
        let mut backoff =
            PollBackoff::new(Duration::from_millis(1), Duration::from_millis(4), None);
        for expected in [2, 4, 4, 4] {
            assert!(backoff.pause(&operation, Duration::ZERO).await.unwrap());
            assert_eq!(
                backoff.delay,
                Duration::from_millis(expected),
                "the backoff doubles up to its ceiling and then stays there"
            );
        }
        assert!(!backoff.expired(), "a wait with no deadline never expires");

        // A sleep that would overrun the deadline is shortened to exactly what is left, so
        // the timeout is reported neither early nor an iteration late.
        let budget = Duration::from_millis(5);
        let mut backoff = PollBackoff::new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Some(Instant::now() + budget),
        );
        let before = Instant::now();
        assert!(
            backoff
                .pause(&operation, Duration::from_secs(10))
                .await
                .unwrap(),
            "the shortened sleep still ran, so it answers true"
        );
        assert!(
            Instant::now() - before < Duration::from_secs(1),
            "the sleep was cut to the remaining budget, not the ten-second delay or the quota \
             floor raising it"
        );

        // Landing on the deadline returns `true`, so `LroPoller::poll` must check `expired`
        // before polling again.
        assert!(backoff.expired());
        assert!(
            !backoff.pause(&operation, Duration::ZERO).await.unwrap(),
            "past the deadline there is nothing left to sleep"
        );
    });
}

/// Four consecutive transient failures are retried and the fifth is reported; both sides of the
/// boundary are pinned.
#[test]
fn a_poll_absorbs_every_transient_failure_but_the_last_of_its_budget() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let absorbed = usize::try_from(MAX_ATTEMPTS).unwrap() - 1;
    let unavailable = || {
        (
            500,
            serde_json::json!({"error": {"message": "the service is unavailable"}}),
        )
    };

    // One short of the budget, followed by the answer the wait was after.
    let mut responses: Vec<_> = std::iter::repeat_with(unavailable).take(absorbed).collect();
    responses.push((200, serde_json::json!({"done": true})));
    let (endpoint, captured) = crate::test_support::http::capture_requests(responses);
    let (cancellation, operation) = poller_operation(&runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let mut poller = test_poller(&operation, &credentials, endpoint.clone());
    let polled = runtime
        .block_on(poller.poll(&format!("{endpoint}/v1/operations/1"), "poll it"))
        .unwrap();
    assert_eq!(polled, serde_json::json!({"done": true}));
    assert_eq!(captured.join().unwrap().len(), absorbed + 1);
    drop(cancellation);

    // One more, and the last failure is the answer rather than another retry.
    let responses: Vec<_> = std::iter::repeat_with(unavailable)
        .take(absorbed + 1)
        .collect();
    let (endpoint, captured) = crate::test_support::http::capture_requests(responses);
    let (_cancellation, operation) = poller_operation(&runtime);
    let mut poller = test_poller(&operation, &credentials, endpoint.clone());
    let failure = runtime
        .block_on(poller.poll(&format!("{endpoint}/v1/operations/1"), "poll it"))
        .unwrap_err();
    assert_eq!(failure.status, Status::IO);
    assert_message(&failure, &["the service is unavailable"]);
    assert_eq!(
        captured.join().unwrap().len(),
        usize::try_from(MAX_ATTEMPTS).unwrap(),
        "the budget is a count of attempts, not of retries after the first"
    );
}

/// A rate-limited poll uses the shared quota backoff, bounded by `pooling_timeout`.
///
/// Create the 20 ms budget inside the task so runtime startup cannot exhaust it before the first
/// request. The deadline is shorter than the quota delay cap, but jitter can allow multiple polls;
/// assert that a 429 was received and the deadline ended the wait, without fixing the poll count.
#[test]
fn a_rate_limited_poll_waits_the_quota_window_without_outliving_the_deadline() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (_cancellation, operation) = poller_operation(&runtime);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let (endpoint, server) = crate::test_support::http::capture_requests_until_stopped(
        (0..MAX_ATTEMPTS)
            .map(|_| (429, serde_json::json!({"error": {"message": "slow down"}})))
            .collect(),
    );
    let client = rest_client("poll quota test").unwrap();

    let (failure, waited) = runtime.block_on(async {
        let mut poller = super::LroPoller::new(
            &operation,
            &credentials,
            client,
            endpoint.clone(),
            super::PollBudget {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(1),
                deadline: Some(Instant::now() + Duration::from_millis(20)),
                seconds: 0,
                what: "test wait",
            },
        );
        let started = Instant::now();
        let failure = poller
            .poll(&format!("{endpoint}/v1/operations/1"), "poll it")
            .await
            .unwrap_err();
        (failure, started.elapsed())
    });
    let polls = server.stop();

    assert_eq!(failure.status, Status::Timeout);
    assert_eq!(
        failure.message,
        "the BigQuery test wait did not complete within the \
         adbc.bigquery.dataproc.pooling_timeout option's 0 s"
    );
    assert!(
        waited < Duration::from_secs(1),
        "the quota window was clamped to the deadline, not slept in full"
    );
    assert!(
        !polls.is_empty(),
        "the 429 that raises the wait to the quota window has to reach the poller"
    );
}

/// An operation for a poller under test, cancelled when the returned handle is dropped.
fn poller_operation(
    runtime: &crate::runtime::SharedRuntime,
) -> (
    std::sync::Arc<crate::cancellation::Cancellation>,
    crate::cancellation::Operation,
) {
    let cancellation = crate::cancellation::Cancellation::new(
        runtime.clone(),
        crate::clients::ClientState::for_test(),
        "statement",
    );
    let operation = cancellation.begin();
    (cancellation, operation)
}

/// A poller whose backoff is short enough that a whole budget of failures costs milliseconds.
fn test_poller<'a>(
    operation: &'a crate::cancellation::Operation,
    credentials: &'a Credentials,
    endpoint: String,
) -> super::LroPoller<'a> {
    super::LroPoller::new(
        operation,
        credentials,
        rest_client("poll budget test").unwrap(),
        endpoint,
        super::PollBudget {
            initial: Duration::from_millis(1),
            maximum: Duration::from_millis(1),
            deadline: None,
            seconds: 0,
            what: "test wait",
        },
    )
}

/// A finished operation's `google.rpc.Status` forwards its details after the code, as a generated
/// client does: keyed by type name, valued with the ProtoJSON the service sent.
#[test]
fn a_failed_operation_forwards_its_status_details() {
    for completed in [
        serde_json::json!({"done": true}),
        serde_json::json!({"done": true, "error": null, "response": {}}),
    ] {
        assert!(super::operation_failure(&completed, "operation", "no message").is_none());
    }
    let error_info = serde_json::json!({
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "QUOTA_EXCEEDED",
        "domain": "dataproc.googleapis.com",
    });
    let completed = serde_json::json!({
        "done": true,
        "error": {
            "code": 8,
            "message": "Insufficient CPUS quota",
            "details": [error_info, {"reason": "untyped, so skipped"}],
        },
    });
    let failure = super::operation_failure(&completed, "the Dataproc batch failed", "no message")
        .expect("a done operation with an error failed");
    assert_eq!(failure.status, Status::IO);
    let details = failure.details.expect("the code and the typed detail");
    assert_eq!(details.len(), 2, "{details:?}");
    assert_eq!(details[0].0, "bigquery.error.rpc_code");
    assert_eq!(details[1].0, "google.rpc.ErrorInfo");
    let forwarded: serde_json::Value = serde_json::from_slice(&details[1].1).unwrap();
    assert_eq!(forwarded, error_info);

    // A status without details records only the code.
    let bare = super::operation_failure(
        &serde_json::json!({"done": true, "error": {"code": 5, "message": "gone"}}),
        "the Dataproc batch failed",
        "no message",
    )
    .unwrap();
    assert_eq!(
        bare.details,
        Some(vec![("bigquery.error.rpc_code".to_owned(), b"5".to_vec())])
    );
}
