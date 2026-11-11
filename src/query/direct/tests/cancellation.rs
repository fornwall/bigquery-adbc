//! Cancelling a direct query: what each cancellation point leaves unsent, and what a
//! cancellation after the POST still has to clean up.

use super::*;
use crate::test_support::cancellation::cancellation_at;
use crate::test_support::http::{
    CapturedRequest, capture_requests_until_stopped, job_reference, mock_listener, ok, retire,
};
use crate::test_support::identifiers::project;

fn assert_no_request(listener: TcpListener) {
    thread::sleep(Duration::from_millis(100));
    match listener.accept() {
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {}
        Ok(_) => panic!("a jobs.query request started after local cancellation"),
        Err(source) => panic!("checking for an unexpected jobs.query request failed: {source}"),
    }
    retire(listener);
}

#[derive(Debug)]
struct CancelDuringCredentials(Arc<Cancellation>);

impl CredentialsProvider for CancelDuringCredentials {
    async fn headers(
        &self,
        _extensions: http::Extensions,
    ) -> std::result::Result<CacheableResource<HeaderMap>, google_cloud_gax::error::CredentialsError>
    {
        OperationCancelHandle(self.0.clone())
            .try_cancel()
            .expect("the test operation is active");
        // Make cancellation win the orchestration select before credentials become ready.
        // An incorrectly detached request would resume here and issue its first POST.
        tokio::task::yield_now().await;
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: HeaderMap::new(),
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".to_owned())
    }
}

/// The race against cancellation is biased, so a cancelled operation never polls the request.
#[test]
fn cancellation_before_the_query_future_is_polled_sends_no_request() {
    let (listener, endpoint) = mock_listener();
    let (runtime, client) = job_client(&endpoint);
    let cancellation = cancellation_at(&runtime, &endpoint);
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);

    let failure = runtime
        .block_on(submit_jobs_query(
            client,
            &decoder,
            project("project"),
            None,
            Bytes::from_static(b"{}"),
            Some(&operation),
        ))
        .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    assert_no_request(listener);
}

/// A cancel that finds the submission connecting returns at once, leaving the request to a
/// detached continuation, so a connection that then fails, having sent nothing, cannot replace
/// the caller's `CANCELLED` with its transport error.
#[test]
fn cancellation_during_a_connection_that_then_fails_reports_the_cancellation() {
    let (endpoint, connecting, release) = crate::test_support::http::stalled_tls_handshake();
    let (runtime, client) = job_client(&endpoint);
    let cancellation = cancellation_at(&runtime, &endpoint);
    let operation = cancellation.begin();
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);
    let canceller = thread::spawn(move || {
        connecting.recv().unwrap();
        OperationCancelHandle(cancellation).try_cancel().unwrap();
        release.send(()).unwrap();
    });

    let failure = runtime
        .block_on(submit_jobs_query(
            client,
            &decoder,
            project("project"),
            None,
            Bytes::from_static(b"{}"),
            Some(&operation),
        ))
        .unwrap_err();
    canceller.join().unwrap();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled, "{}", failure.message);
}

#[test]
fn cancellation_during_credentials_sends_no_request() {
    let (listener, endpoint) = mock_listener();
    let runtime = new_runtime().unwrap();
    let cancellation = cancellation_at(&runtime, &endpoint);
    let operation = cancellation.begin();
    let decoder = QueryResponseDecoder::new(&runtime, QueryResultMode::Rows);
    let client = JobClient::new(
        Credentials::from(CancelDuringCredentials(cancellation)),
        Some(&endpoint),
        rest_client("direct query test").unwrap(),
    );

    let failure = runtime
        .block_on(submit_jobs_query(
            client,
            &decoder,
            project("project"),
            None,
            Bytes::from_static(b"{}"),
            Some(&operation),
        ))
        .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    assert_no_request(listener);
}

#[test]
fn cancellation_after_the_post_keeps_the_late_response_and_cancels_its_job() {
    let (request_seen, wait_for_request) = mpsc::sync_channel(0);
    let (release_response, response_released) = mpsc::sync_channel(0);
    let (endpoint, captured) = capture_requests_gated(
        vec![
            ok(json!({
                "jobComplete":false,
                "jobReference": job_reference("project", "late-job")
            })),
            ok(json!({})),
        ],
        move || {
            request_seen.send(()).unwrap();
            response_released.recv().unwrap();
            true
        },
    );
    let (runtime, client) = client_with_one_result_worker(&endpoint);
    // Cancellation wins after the POST while its response's row-capable decode is still unable to
    // start. The detached continuation must retain both the body and job-registration duty until
    // this permit is released, otherwise the remote job escapes cancellation.
    let decode_permit = runtime
        .result_conversion_permits()
        .try_acquire_owned()
        .unwrap();
    let cancellation = cancellation_at(&runtime, &endpoint);
    let operation = cancellation.begin();
    let handle = OperationCancelHandle(cancellation);
    let cancel = thread::spawn(move || {
        wait_for_request.recv().unwrap();
        handle.try_cancel().unwrap();
        release_response.send(()).unwrap();
    });

    let failure = runtime
        .block_on(complete_query_with_progress(
            &runtime,
            client,
            plan(StatementQueryOptions::default()),
            QueryResultMode::Rows,
            Some(&operation),
            None,
            "test",
        ))
        .unwrap_err();
    drop(decode_permit);
    operation.finish();
    cancel.join().unwrap();

    assert_eq!(failure.status, Status::Cancelled);
    let captured = captured.served(2);
    assert_eq!(captured[0].path(), "/bigquery/v2/projects/project/queries");
    assert_eq!(captured[0].body["jobCreationMode"], "JOB_CREATION_OPTIONAL");
    assert_eq!(captured[0].body["timeoutMs"], 10_000);
    assert_eq!(
        captured[1].path(),
        "/bigquery/v2/projects/project/jobs/late-job/cancel?location=EU"
    );
    assert!(captured[1].raw_body.is_empty());
}

/// A partition descriptor names a job that `ExecutePartitions` already ran to completion, perhaps
/// for another principal. Its reader stops locally, but asks BigQuery to cancel nothing, even when
/// it fails or is cancelled before a results read reports the job done: there is nothing left to
/// stop, and a refused cancel would make `AdbcConnectionCancel` report `UNKNOWN`.
#[test]
fn a_partition_reader_never_cancels_the_job_it_reads() {
    let (endpoint, server) = capture_requests_until_stopped(vec![
        (
            403,
            error_body("accessDenied", "no access to the job's results"),
        ),
        ok(json!({})),
    ]);
    let (runtime, client) = job_client(&endpoint);
    let cancellation = cancellation_at(&runtime, &endpoint);
    let operation = cancellation.begin_concurrent();

    let failure = runtime
        .block_on(complete_attached_query(
            &runtime,
            client,
            JobReference::new("project", Some("EU"), "partition-job".to_owned()),
            Some(&operation),
            "test",
        ))
        .unwrap_err();
    assert_eq!(failure.status, Status::Unauthorized, "{failure}");
    OperationCancelHandle(cancellation).try_cancel().unwrap();
    operation.finish();

    let requests = server.stop();
    let paths = requests
        .iter()
        .map(CapturedRequest::path)
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "only the results read: {paths:?}");
    assert!(paths[0].contains("/queries/partition-job"), "{paths:?}");
}
