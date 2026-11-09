use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use adbc_core::CancelHandle;
use adbc_core::error::Status;
use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::Schema;
use serde_json::json;

use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::rest::jobs::JobReference;
use crate::runtime::new_runtime;
use crate::test_support::http::{
    CapturedServer, capture_requests, capture_requests_gated, capture_requests_until_stopped,
    error_body, mock_listener, ok, retire,
};

use super::{Cancellation, OperationCancelHandle, buffered_operation_reader, operation_reader};
use crate::test_support::errors::assert_failure;

/// How long a test waits for an event that must happen: a deadlock guard, not a timing assertion,
/// so it is far longer than any delay a loaded parallel `cargo test` adds.
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

fn cancellation() -> Arc<Cancellation> {
    Cancellation::new(new_runtime().unwrap(), ClientState::for_test(), "statement")
}

fn cancellation_at(endpoint: &str) -> Arc<Cancellation> {
    Cancellation::new(
        new_runtime().unwrap(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        "statement",
    )
}

fn job(id: &str) -> JobReference {
    JobReference::new("billing-project", None, id.to_owned())
}

/// Every attempt to find a job that is not visible yet, then the cancel that finds it.
fn invisible_then_cancelled() -> Vec<(u16, serde_json::Value)> {
    let mut responses = vec![
        (404, error_body("notFound", "not visible"));
        crate::rest::retry::MAX_ATTEMPTS as usize
    ];
    responses.push(ok(json!({})));
    responses
}

/// Asserts the mock received exactly one request: the cancel of `job_id`.
#[track_caller]
fn assert_one_cancel(requests: CapturedServer, job_id: &str) {
    let requests = requests.served(1);
    assert!(
        requests[0]
            .path()
            .ends_with(&format!("/jobs/{job_id}/cancel")),
        "{}",
        requests[0].line
    );
}

/// Cancels from a task on the driver's own runtime, as an rlib Tokio host would.
fn cancel_from_a_runtime_thread(cancellation: &Arc<Cancellation>) {
    let handle = OperationCancelHandle(cancellation.clone());
    cancellation
        .runtime
        .block_on(async move {
            tokio::task::spawn(async move { handle.try_cancel() })
                .await
                .unwrap()
        })
        .unwrap();
}

#[track_caller]
fn assert_status<T: std::fmt::Debug>(result: adbc_core::error::Result<T>, status: Status) {
    assert_eq!(result.unwrap_err().status, status);
}

#[track_caller]
fn assert_reports_cancellation(
    item: Option<Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
) {
    let arrow_schema::ArrowError::ExternalError(source) = item.unwrap().unwrap_err() else {
        panic!("expected the reader to report ADBC cancellation");
    };
    let source = source.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(source.status, Status::Cancelled);
}

#[test]
fn cancellation_tracks_active_operation_lifecycle() {
    let cancellation = cancellation();
    let handle = OperationCancelHandle(cancellation.clone());
    assert_status(handle.try_cancel(), Status::InvalidState);

    let first = cancellation.begin();
    let second = cancellation.begin();
    assert_status(first.check(), Status::Cancelled);
    assert!(second.check().is_ok());

    handle.try_cancel().unwrap();
    assert_status(second.check(), Status::Cancelled);
    second.finish();
    assert_status(handle.try_cancel(), Status::InvalidState);

    let abandoned = cancellation.begin();
    drop(abandoned);
    assert_status(handle.try_cancel(), Status::InvalidState);
}

/// A cancel's claim records whether the identity was still provisional. The submission it stops
/// may settle as finished at once, before the cancel is sent; that must leave the claim
/// provisional, so the cancel's 404 for a job that was never created is not a failure.
#[test]
fn a_cancel_claim_keeps_the_state_it_claimed() {
    let cancellation = cancellation();
    let submitting = cancellation.begin();
    submitting
        .register_provisional_job(&job("never-created"))
        .unwrap();
    submitting.inner.cancel_local();
    let (claimed, provisional) = submitting.inner.job_to_cancel().unwrap();
    assert_eq!(claimed, job("never-created"));
    assert!(provisional);
    assert_status(
        submitting.register_completed_job(&claimed),
        Status::Cancelled,
    );
    assert!(submitting.inner.job_to_cancel().is_none());

    let running = cancellation.begin();
    running.register_job(&job("running")).unwrap();
    let (claimed, provisional) = running.inner.job_to_cancel().unwrap();
    assert_eq!(claimed, job("running"));
    assert!(!provisional);
}

#[test]
fn synchronous_work_finishes_retained_operations_and_cancels_unfinished_jobs() {
    for succeeds in [true, false] {
        let (endpoint, requests) = capture_requests(vec![ok(json!({}))]);
        let cancellation = cancellation_at(&endpoint);
        let previous = cancellation.begin();
        let mut retained = None;
        let result = cancellation.run(|operation| {
            assert!(previous.is_cancelled());
            operation.register_job(&job("unfinished"))?;
            retained = Some(operation.clone());
            if succeeds {
                Ok(42)
            } else {
                Err(crate::error::invalid_state("work failed"))
            }
        });

        if succeeds {
            assert_eq!(result.unwrap(), 42);
        } else {
            let failure = result.unwrap_err();
            assert_eq!(failure.status, Status::InvalidState);
            assert_eq!(failure.message, "work failed");
        }
        assert!(retained.unwrap().is_cancelled());
        assert_status(
            OperationCancelHandle(cancellation).try_cancel(),
            Status::InvalidState,
        );
        assert_one_cancel(requests, "unfinished");
    }
}

/// A detached jobs.query continuation can outlive the statement that owned it. Its operation
/// retains enough state to cancel a job whose reference only arrives after that release.
#[test]
fn a_late_job_reference_retains_its_released_cancellation_owner() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({}))]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation.clone())
        .try_cancel()
        .unwrap();
    drop(cancellation);

    let error = operation.register_job(&job("query-job")).unwrap_err();
    assert_eq!(error.status, Status::Cancelled);
    let requests = requests.served(1);
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/billing-project/jobs/query-job/cancel"
    );
}

#[test]
fn dropping_an_incomplete_reader_cancels_retained_operation_clones() {
    let cancellation = cancellation();
    let operation = cancellation.begin();
    let retained = operation.clone();
    let schema = Arc::new(Schema::empty());
    let inner: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(std::iter::empty(), schema));

    drop(operation_reader(inner, operation));

    assert!(retained.is_cancelled());
    assert_status(
        OperationCancelHandle(cancellation).try_cancel(),
        Status::InvalidState,
    );
}

#[test]
fn buffered_result_reader_remains_readable_after_cancellation() {
    let cancellation = cancellation();
    let operation = cancellation.begin();
    let schema = Arc::new(Schema::empty());
    let batch = arrow_array::RecordBatch::new_empty(schema.clone());
    let inner: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch.clone())),
        schema,
    ));
    let mut reader = buffered_operation_reader(inner, operation);

    OperationCancelHandle(cancellation).try_cancel().unwrap();

    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
}

fn retained_result_batch() -> (arrow_array::RecordBatch, arrow_buffer::Buffer) {
    let values = arrow_array::Int64Array::from(vec![7; 1024 * 1024]).slice(0, 1);
    let batch = arrow_array::RecordBatch::try_from_iter([(
        "value",
        Arc::new(values) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let observer = batch.column(0).to_data().buffers()[0].clone();
    assert_eq!(observer.capacity(), 8 * 1024 * 1024);
    (batch, observer)
}

#[test]
fn terminal_operation_readers_release_retained_buffers() {
    for buffered in [false, true] {
        for fails in [false, true] {
            let (batch, observer) = retained_result_batch();
            let schema = batch.schema();
            let inner = Box::new(RecordBatchIterator::new(
                std::iter::from_fn(move || {
                    assert_eq!(batch.num_rows(), 1);
                    fails.then(|| Err(arrow_schema::ArrowError::ComputeError("failed".into())))
                }),
                schema.clone(),
            ));
            let operation = cancellation().begin();
            let mut reader = if buffered {
                buffered_operation_reader(inner, operation)
            } else {
                operation_reader(inner, operation)
            };
            assert!(observer.strong_count() > 1);
            let next = reader.next();
            if fails {
                assert_eq!(
                    next.unwrap().unwrap_err().to_string(),
                    "Compute error: failed"
                );
            } else {
                assert!(next.is_none());
            }
            assert_eq!(
                observer.strong_count(),
                1,
                "buffered={buffered}, fails={fails}"
            );
            assert_eq!(reader.schema(), schema);
            assert!(reader.next().is_none());
        }
    }
}

#[test]
fn cancelled_operation_readers_release_retained_buffers() {
    for during_read in [false, true] {
        let (batch, observer) = retained_result_batch();
        let schema = batch.schema();
        let cancellation = cancellation();
        let operation = cancellation.begin();
        let reading_operation = operation.clone();
        let inner = Box::new(RecordBatchIterator::new(
            std::iter::from_fn(move || {
                assert!(during_read, "a cancelled reader must not be polled");
                reading_operation.inner.cancel_local();
                Some(Ok(batch.clone()))
            }),
            schema.clone(),
        ));
        let mut reader = operation_reader(inner, operation);
        if !during_read {
            OperationCancelHandle(cancellation).try_cancel().unwrap();
        }
        assert!(observer.strong_count() > 1);
        assert_reports_cancellation(reader.next());
        assert_eq!(observer.strong_count(), 1, "during_read={during_read}");
        assert_eq!(reader.schema(), schema);
        assert!(reader.next().is_none());
    }
}

/// An rlib host may issue Cancel from one of its own Tokio worker threads, where a
/// `block_on` would panic and silently drop the remote cancel. The cancel must still reach
/// the service, delivered through the driver's runtime instead.
#[test]
fn cancellation_from_a_runtime_thread_still_delivers_the_remote_cancel() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({}))]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = job("query-job");
    operation.register_job(&reference).unwrap();

    cancel_from_a_runtime_thread(&cancellation);
    assert_one_cancel(requests, "query-job");
}

#[test]
fn failed_runtime_thread_cancel_rearms_the_same_confirmed_reference() {
    let (first_seen, seen) = mpsc::channel();
    let (endpoint, requests) = capture_requests_gated(
        vec![
            (400, json!({"error":{"message":"not cancellable yet"}})),
            ok(json!({})),
        ],
        move || {
            first_seen.send(()).unwrap();
            true
        },
    );
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = job("query-job");
    operation.register_job(&reference).unwrap();

    cancel_from_a_runtime_thread(&cancellation);
    seen.recv_timeout(EVENT_TIMEOUT).unwrap();
    let deadline = std::time::Instant::now() + EVENT_TIMEOUT;
    while operation
        .inner
        .job()
        .as_ref()
        .is_some_and(|job| job.cancel_started)
    {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }

    assert_status(operation.register_job(&reference), Status::Cancelled);
    requests.served(2);
}

#[test]
fn cancellation_sends_one_location_aware_job_request() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({}))]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = JobReference::new(
        "billing-project",
        Some("europe-north1"),
        "query-job".to_owned(),
    );
    operation.register_job(&reference).unwrap();

    let handle = OperationCancelHandle(cancellation);
    handle.try_cancel().unwrap();
    handle.try_cancel().unwrap();

    let requests = requests.served(1);
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/billing-project/jobs/query-job/cancel?location=europe-north1"
    );
    assert!(requests[0].raw_body.is_empty());
}

#[test]
fn late_registration_returns_cancelled_before_remote_cancellation_answers() {
    let (release, released) = mpsc::channel();
    let (endpoint, requests) = capture_requests_gated(vec![ok(json!({}))], move || {
        released.recv().unwrap();
        true
    });
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    let (result, completed) = mpsc::channel();
    let late = operation;
    std::thread::spawn(move || {
        result
            .send(late.register_job(&job("late-query-job")))
            .unwrap();
    });
    let failure = completed
        .recv_timeout(EVENT_TIMEOUT)
        .expect("local cancellation must not wait for the remote cancel")
        .unwrap_err();
    assert_eq!(failure.status, Status::Cancelled);

    release.send(()).unwrap();
    assert_eq!(
        requests.served(1)[0].path(),
        "/bigquery/v2/projects/billing-project/jobs/late-query-job/cancel"
    );
}

/// A caller cancelled before its submission sends no job, so a provisional identity registered
/// then is refused, leaving no job for this cancel or the operation's end to cancel.
#[test]
fn a_cancelled_operation_refuses_a_provisional_job_and_cancels_nothing() {
    let (endpoint, requests) =
        capture_requests_until_stopped(vec![(404, error_body("notFound", "Not found: Job")); 2]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    assert_status(
        operation.register_provisional_job(&job("never-sent")),
        Status::Cancelled,
    );
    operation.finish();
    drop(operation);

    std::thread::sleep(Duration::from_millis(200));
    assert!(requests.stop().is_empty());
}

#[test]
fn a_more_specific_reference_rearms_an_already_cancelled_job() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({})), ok(json!({}))]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let provisional = job("query-job");
    operation.register_provisional_job(&provisional).unwrap();
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    let canonical = JobReference::new(
        "billing-project",
        Some("europe-north1"),
        "query-job".to_owned(),
    );
    assert_status(operation.register_job(&canonical), Status::Cancelled);

    let requests = requests.served(2);
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/billing-project/jobs/query-job/cancel"
    );
    assert_eq!(
        requests[1].path(),
        "/bigquery/v2/projects/billing-project/jobs/query-job/cancel?location=europe-north1"
    );
}

#[test]
fn failed_provisional_cancel_retries_after_submission_confirmation() {
    let responses = invisible_then_cancelled();
    let (endpoint, requests) = capture_requests(responses);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = JobReference::new("billing-project", Some("EU"), "query-job".to_owned());
    operation.register_provisional_job(&reference).unwrap();

    OperationCancelHandle(cancellation).try_cancel().unwrap();
    assert_status(operation.register_job(&reference), Status::Cancelled);

    requests.served(crate::rest::retry::MAX_ATTEMPTS as usize + 1);
}

#[test]
fn in_flight_provisional_cancel_retains_confirmation_after_the_caller_drops() {
    let responses = invisible_then_cancelled();
    let (first_seen, first_received) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (endpoint, requests) = capture_requests_gated(responses, move || {
        first_seen.send(()).unwrap();
        released.recv().unwrap();
        true
    });
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = JobReference::new("billing-project", Some("EU"), "query-job".to_owned());
    operation.register_provisional_job(&reference).unwrap();

    // A Tokio-host Cancel hands remote delivery to the driver's runtime and returns locally.
    cancellation.runtime.block_on(async {
        OperationCancelHandle(cancellation.clone())
            .try_cancel()
            .unwrap();
    });
    first_received.recv_timeout(EVENT_TIMEOUT).unwrap();
    assert_status(operation.register_job(&reference), Status::Cancelled);
    operation.finish();
    drop(operation);
    release.send(()).unwrap();

    requests.served(crate::rest::retry::MAX_ATTEMPTS as usize + 1);
}

/// A concurrent operation is not superseded by anything that follows it, and it does not
/// supersede what came before: adbc.h's partition streams run side by side.
#[test]
fn concurrent_operations_outlive_the_calls_that_follow_them() {
    let cancellation = cancellation();
    let superseded = cancellation.begin();
    let first = cancellation.begin_concurrent();
    let second = cancellation.begin_concurrent();
    assert!(
        superseded.check().is_ok(),
        "a concurrent begin supersedes nothing"
    );

    let later = cancellation.begin();
    assert_status(superseded.check(), Status::Cancelled);
    assert!(first.check().is_ok());
    assert!(second.check().is_ok());

    // Cancel ends every live operation, not just the most recent one.
    OperationCancelHandle(cancellation.clone())
        .try_cancel()
        .unwrap();
    for operation in [&first, &second, &later] {
        assert_status(operation.check(), Status::Cancelled);
    }

    first.finish();
    second.finish();
    later.finish();
    assert_status(
        OperationCancelHandle(cancellation).try_cancel(),
        Status::InvalidState,
    );
}

#[test]
fn every_concurrent_operation_is_locally_cancelled_before_the_first_remote_answer() {
    let (first_seen, seen) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (endpoint, requests) =
        capture_requests_gated(vec![ok(json!({})), ok(json!({}))], move || {
            first_seen.send(()).unwrap();
            released.recv().unwrap();
            true
        });
    let cancellation = cancellation_at(&endpoint);
    let first = cancellation.begin_concurrent();
    let second = cancellation.begin_concurrent();
    first.register_job(&job("first")).unwrap();
    second.register_job(&job("second")).unwrap();

    let handle = OperationCancelHandle(cancellation);
    let cancel = std::thread::spawn(move || handle.try_cancel());
    seen.recv_timeout(EVENT_TIMEOUT)
        .expect("the first remote cancellation did not arrive");
    assert_status(first.check(), Status::Cancelled);
    assert_status(second.check(), Status::Cancelled);

    release.send(()).unwrap();
    cancel.join().unwrap().unwrap();
    requests.served(2);
}

/// An operation a later one supersedes, or one dropped unfinished, cancels its job.
#[test]
fn a_superseded_or_abandoned_operation_cancels_its_registered_job() {
    for supersede in [true, false] {
        let (endpoint, requests) = capture_requests(vec![ok(json!({}))]);
        let cancellation = cancellation_at(&endpoint);
        let operation = cancellation.begin();
        operation.register_job(&job("query-job")).unwrap();
        if supersede {
            cancellation.begin().finish();
            assert_status(operation.check(), Status::Cancelled);
        } else {
            drop(operation);
        }
        assert_one_cancel(requests, "query-job");
    }
}

#[test]
fn a_terminal_job_leaves_its_reader_cancellable_without_a_remote_request() {
    // No credentials are available: an attempted remote cancel would fail this call.
    let cancellation = cancellation();
    let operation = cancellation.begin();
    let reference = job("completed-job");
    operation.register_job(&reference).unwrap();
    operation.register_completed_job(&reference).unwrap();
    // A repeated publication must not forget terminal status.
    operation.register_job(&reference).unwrap();
    let schema = Arc::new(Schema::empty());
    let inner = Box::new(RecordBatchIterator::new(std::iter::empty(), schema));
    let mut reader = operation_reader(inner, operation);
    let handle = OperationCancelHandle(cancellation);

    handle.try_cancel().unwrap();
    assert_reports_cancellation(reader.next());
    assert!(reader.next().is_none());
    assert_status(handle.try_cancel(), Status::InvalidState);
}

#[test]
fn canonical_references_preserve_terminal_status() {
    for (project, location, completed) in [
        ("billing-project", None, true),
        ("123456789", None, true),
        ("billing-project", Some("eu"), true),
        ("billing-project", Some("US"), false),
    ] {
        let cancellation = cancellation();
        let operation = cancellation.begin();
        let initial = JobReference::new(project, location, "job".to_owned());
        operation.register_completed_job(&initial).unwrap();
        let canonical = JobReference::new("billing-project", Some("EU"), "job".to_owned());
        operation.register_job(&canonical).unwrap();
        {
            let job = operation.inner.job();
            let job = job.as_ref().unwrap();
            assert_eq!(job.reference, canonical);
            assert_eq!(job.state == super::JobState::Finished, completed);
        }
        // Retire the distinct-region control without issuing an unrelated network request.
        operation.register_completed_job(&canonical).unwrap();
        OperationCancelHandle(cancellation).try_cancel().unwrap();
    }
}

#[test]
fn cancel_failure_reports_unknown_after_stopping_local_work() {
    let (endpoint, requests) =
        capture_requests(vec![(403, error_body("accessDenied", "cancel denied"))]);
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    operation.register_job(&job("query-job")).unwrap();

    let failure = OperationCancelHandle(cancellation)
        .try_cancel()
        .unwrap_err();
    assert_failure(&failure, Status::Unknown, &["cancel denied"]);
    assert_status(operation.check(), Status::Cancelled);
    requests.served(1);
}

#[test]
fn cancellation_deadline_bounds_a_stalled_remote_request() {
    // TCP may connect, but this listener never reads or responds. Cancellation can therefore
    // interrupt any transport stage, including a write that has not sent complete HTTP headers.
    let (listener, endpoint) = mock_listener();
    let mut cancellation = cancellation_at(&endpoint);
    Arc::get_mut(&mut cancellation).unwrap().cancel_timeout = Duration::from_millis(100);
    let operation = cancellation.begin();
    operation.register_job(&job("query-job")).unwrap();
    let (done, completed) = mpsc::channel();
    let cancel = std::thread::spawn(move || {
        done.send(OperationCancelHandle(cancellation).try_cancel())
            .unwrap();
    });

    // A cancel that ignored its 100 ms deadline would wait out the stalled listener until
    // `crate::rest::client::REQUEST_TIMEOUT` (a minute). Half a minute stays well under that
    // while tolerating a loaded `cargo test`.
    let result = completed.recv_timeout(Duration::from_secs(30));
    // Retired rather than dropped: dropping `operation` sends one more background cancel, which
    // a freed port would deliver to another test's mock.
    retire(listener);
    let failure = result
        .expect("Cancel did not respect its deadline")
        .unwrap_err();
    assert_failure(
        &failure,
        Status::Unknown,
        &["cancellation deadline exceeded"],
    );
    assert!(operation.is_cancelled());
    cancel.join().unwrap();
}

#[test]
fn terminal_status_during_a_provisional_cancel_suppresses_confirmation_retry() {
    let (seen, received) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (endpoint, requests) = capture_requests_gated(
        vec![(400, json!({"error":{"message":"cannot cancel"}}))],
        move || {
            seen.send(()).unwrap();
            released.recv().unwrap();
            true
        },
    );
    let cancellation = cancellation_at(&endpoint);
    let operation = cancellation.begin();
    let reference = job("query-job");
    operation.register_provisional_job(&reference).unwrap();
    let cancel = std::thread::spawn(move || OperationCancelHandle(cancellation).try_cancel());
    received.recv_timeout(EVENT_TIMEOUT).unwrap();
    assert_status(
        operation.register_completed_job(&reference),
        Status::Cancelled,
    );
    release.send(()).unwrap();
    assert_eq!(cancel.join().unwrap().unwrap_err().status, Status::Unknown);
    requests.served(1);
}

/// A cancellation landing while an operation is parked in `wait` has to wake it.
#[test]
fn a_cancellation_wakes_an_operation_parked_in_wait() {
    let cancellation = cancellation();
    let operation = cancellation.begin();
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let handle = OperationCancelHandle(cancellation.clone());
        let cancel = async {
            // `join!` polls in order, so the wait above has already parked on `cancelled()` by
            // the time this resumes.
            tokio::task::yield_now().await;
            handle.try_cancel().unwrap();
        };
        let (waited, ()) = tokio::join!(operation.wait(std::future::pending::<()>()), cancel);
        assert_status(waited, Status::Cancelled);
    });
}

/// No worker survives a `fork` to feed or close a buffered reader's channel, so a reader inherited
/// with a running producer must refuse rather than wait forever.
#[test]
fn a_reader_inherited_across_fork_fails_instead_of_waiting_for_its_producer() {
    let runtime = crate::runtime::orphaned_runtime();
    let (_cancellation, operation) =
        crate::test_support::cancellation::operation(&runtime, ClientState::for_test());
    let schema = Arc::new(Schema::empty());
    let reader = crate::pipeline::spawn_result_reader(runtime, schema, None, |sink| async move {
        let _sink = sink;
        std::future::pending::<Result<(), arrow_schema::ArrowError>>().await
    });
    let mut reader = operation_reader(reader, operation);
    let arrow_schema::ArrowError::ExternalError(failure) = reader.next().unwrap().unwrap_err()
    else {
        panic!("the refusal must carry its ADBC status");
    };
    let failure = failure.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(failure.status, Status::InvalidState);
    assert_eq!(failure.message, crate::runtime::INHERITED_ACROSS_FORK);
}
