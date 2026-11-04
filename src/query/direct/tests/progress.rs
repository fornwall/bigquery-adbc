//! The driver-wide result-decode permit, and the progress a query publishes without spending
//! an extra request on it.

use super::*;
use crate::test_support::http::{job_reference, write_response};

#[test]
fn row_returning_jobs_query_decode_waits_for_the_driver_wide_permit() {
    for row_count in [1, 1_024] {
        let rows = (0..row_count)
            .map(|value| json!({"f":[{"v":value.to_string()}]}))
            .collect::<Vec<_>>();
        let (request_seen, wait_for_request) = mpsc::sync_channel(0);
        let (release_response, response_released) = mpsc::sync_channel(0);
        let (endpoint, captured) = capture_requests_gated(
            vec![(
                200,
                json!({
                    "jobComplete":true,
                    "jobReference": job_reference("project", "decode-job"),
                    "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
                    "rows":rows
                }),
            )],
            move || {
                request_seen.send(()).unwrap();
                response_released.recv().unwrap();
                true
            },
        );
        let (runtime, client) = client_with_one_result_worker(&endpoint);
        let permit = runtime
            .result_conversion_permits()
            .try_acquire_owned()
            .unwrap();
        let execute_runtime = runtime;
        let execute = thread::spawn(move || {
            complete(
                &execute_runtime,
                client,
                plan_at(StatementQueryOptions::default(), None),
                QueryResultMode::Rows,
                "test",
            )
        });

        wait_for_request
            .recv_timeout(Duration::from_secs(5))
            .expect("jobs.query did not reach the test server");
        release_response.send(()).unwrap();
        let captured = captured.join().unwrap();
        assert_eq!(captured.len(), 1);
        thread::sleep(Duration::from_millis(100));
        assert!(
            !execute.is_finished(),
            "the initial row page decoded without the shared result-work permit"
        );

        drop(permit);
        let completed = execute.join().unwrap().unwrap();
        assert_eq!(completed.page.rows.len(), row_count);
    }
}

#[test]
fn row_returning_completion_poll_decode_waits_for_the_driver_wide_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (poll_seen, wait_for_poll) = mpsc::sync_channel(0);
    let (release_poll, poll_released) = mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let (mut initial, _) = listener.accept().unwrap();
        read_captured_request(&mut initial).expect("the initial request reached the mock");
        write_response(
            &mut initial,
            200,
            &json!({
                "jobComplete":false,
                "jobReference":{"projectId":"project","jobId":"job-1","location":"EU"},
                "statementType":"SELECT"
            }),
        );

        let (mut poll, _) = listener.accept().unwrap();
        let poll_path = read_captured_request(&mut poll)
            .expect("the poll reached the mock")
            .path()
            .to_owned();
        poll_seen.send(()).unwrap();
        poll_released.recv().unwrap();
        let rows = (0..1_024)
            .map(|value| json!({"f":[{"v":value.to_string()}]}))
            .collect::<Vec<_>>();
        write_response(
            &mut poll,
            200,
            &json!({
                "jobComplete":true,
                "jobReference":{"projectId":"project","jobId":"job-1","location":"EU"},
                "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
                "rows":rows
            }),
        );
        poll_path
    });
    let (runtime, client) = client_with_one_result_worker(&endpoint);
    let execute_runtime = runtime.clone();
    let execute = thread::spawn(move || {
        complete(
            &execute_runtime,
            client,
            plan(StatementQueryOptions::default()),
            QueryResultMode::Rows,
            "test",
        )
    });

    wait_for_poll
        .recv_timeout(Duration::from_secs(5))
        .expect("getQueryResults did not reach the test server");
    let permit = runtime
        .result_conversion_permits()
        .try_acquire_owned()
        .expect("the initial response must release its decode permit before polling");
    release_poll.send(()).unwrap();
    let poll_path = server.join().unwrap();
    assert!(poll_path.contains("maxResults=100000"));
    thread::sleep(Duration::from_millis(100));
    assert!(
        !execute.is_finished(),
        "the completion row page decoded without the shared result-work permit"
    );

    drop(permit);
    let completed = execute.join().unwrap().unwrap();
    assert_eq!(completed.page.rows.len(), 1_024);
}

#[test]
fn natural_query_responses_publish_progress_without_an_extra_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (first_poll_seen, wait_for_first_poll) = mpsc::sync_channel(0);
    let (release_first_poll, first_poll_released) = mpsc::sync_channel(0);
    let (final_poll_seen, wait_for_final_poll) = mpsc::sync_channel(0);
    let (release_final_poll, final_poll_released) = mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let (mut initial, _) = listener.accept().unwrap();
        let initial_path = read_captured_request(&mut initial)
            .expect("the initial request reached the mock")
            .path()
            .to_owned();
        write_response(
            &mut initial,
            200,
            &json!({
                "jobComplete": false,
                "jobReference": job_reference("project", "progress-job"),
                "statementType": "SELECT",
                "totalBytesProcessed": "17"
            }),
        );

        let (mut first_poll, _) = listener.accept().unwrap();
        let first_poll_path = read_captured_request(&mut first_poll)
            .expect("the first poll reached the mock")
            .path()
            .to_owned();
        first_poll_seen.send(()).unwrap();
        first_poll_released.recv().unwrap();
        write_response(
            &mut first_poll,
            200,
            &json!({
                "jobComplete": false,
                "jobReference": job_reference("project", "progress-job"),
                "totalBytesProcessed": "23"
            }),
        );

        let (mut final_poll, _) = listener.accept().unwrap();
        let final_poll_path = read_captured_request(&mut final_poll)
            .expect("the final poll reached the mock")
            .path()
            .to_owned();
        final_poll_seen.send(()).unwrap();
        final_poll_released.recv().unwrap();
        write_response(
            &mut final_poll,
            200,
            &json!({
                "jobComplete": true,
                "jobReference": job_reference("project", "progress-job"),
                "schema": {"fields": []},
                "totalBytesProcessed": "29"
            }),
        );
        vec![initial_path, first_poll_path, final_poll_path]
    });

    let (runtime, client) = client(&endpoint);
    let progress = Arc::new(ExecutionProgress::default());
    let executing_progress = Arc::clone(&progress);
    let execute = thread::spawn(move || {
        runtime.block_on(complete_query_with_progress(
            &runtime,
            client,
            plan(StatementQueryOptions::default()),
            QueryResultMode::Rows,
            None,
            Some(&executing_progress),
            "test",
        ))
    });

    wait_for_first_poll
        .recv_timeout(Duration::from_secs(5))
        .expect("getQueryResults did not start after the incomplete jobs.query response");
    assert_eq!(progress.current(), 17.0);
    assert_eq!(progress.maximum(), 0.0);

    release_first_poll.send(()).unwrap();
    wait_for_final_poll
        .recv_timeout(Duration::from_secs(5))
        .expect("the next long poll did not start after incomplete getQueryResults");
    assert_eq!(progress.current(), 23.0);
    assert_eq!(progress.maximum(), 0.0);

    release_final_poll.send(()).unwrap();
    let completed = execute.join().unwrap().unwrap();
    assert_eq!(completed.metadata.total_bytes_processed, Some(29));
    assert_eq!(progress.current(), 29.0);
    assert_eq!(progress.maximum(), 29.0);

    let paths = server.join().unwrap();
    assert_eq!(paths.len(), 3);
    assert_eq!(paths[0], "/bigquery/v2/projects/project/queries");
    for path in &paths[1..] {
        assert!(path.starts_with(
            "/bigquery/v2/projects/project/queries/progress-job?maxResults=100000&timeoutMs=45000"
        ));
    }
}
