//! Cancelling a direct query: what each cancellation point leaves unsent, and what a
//! cancellation after the POST still has to clean up.

use super::*;
use crate::test_support::http::{job_reference, mock_listener, ok, retire};
use crate::test_support::identifiers::project;

fn cancellation_at(runtime: &SharedRuntime, endpoint: &str) -> Arc<Cancellation> {
    Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        "statement",
    )
}

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
    let (runtime, client) = client(&endpoint);
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
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].path(), "/bigquery/v2/projects/project/queries");
    assert_eq!(captured[0].body["jobCreationMode"], "JOB_CREATION_OPTIONAL");
    assert_eq!(captured[0].body["timeoutMs"], 10_000);
    assert_eq!(
        captured[1].path(),
        "/bigquery/v2/projects/project/jobs/late-job/cancel?location=EU"
    );
    assert!(captured[1].raw_body.is_empty());
}
