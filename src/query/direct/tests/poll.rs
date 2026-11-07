//! `poll.rs`: waiting for a submitted job, and telling a real failure from a warning.

use super::*;

#[test]
fn incomplete_warning_polls_same_default_location_job() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobComplete": false,
            "jobReference": {"projectId":"project","jobId":"job-1"},
            "statementType": "SELECT",
            "errors": [{"reason":"backendError","message":"warning"}]
        })),
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job-1"},
            "schema": {"fields":[]},
            "rows": []
        })),
    ]);
    let completed = run(&endpoint, plan_at(StatementQueryOptions::default(), None)).unwrap();
    assert_eq!(
        completed.metadata.job_reference.as_ref().unwrap().location,
        None
    );

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].line.starts_with("POST "));
    assert_eq!(captured[0].body["jobCreationMode"], "JOB_CREATION_OPTIONAL");
    assert_eq!(captured[0].body["timeoutMs"], 10_000);
    assert!(captured[1].line.starts_with("GET "));
    assert!(captured[1].path().starts_with(
        "/bigquery/v2/projects/project/queries/job-1?maxResults=100000&timeoutMs=45000&formatOptions.useInt64Timestamp=true"
    ));
    assert!(!captured[1].path().contains("location="));
}

/// BigQuery refuses every response holding a row over 20 MiB but keeps the job: the replayed
/// `jobs.query` names it, and the completion poll then waits without rows, marking the result as
/// one only Storage Read can return.
#[test]
fn a_row_too_large_for_any_response_leaves_the_rows_to_storage_read() {
    let too_large = || (403, error_body("responseTooLarge", "exceeds max row limit"));
    let (endpoint, captured) = capture_requests(vec![
        too_large(),
        (
            409,
            json!({"error": {"code": 409, "errors": [{"reason": "duplicate",
                "message": "Already Exists: Job project:EU.job_large"}]}}),
        ),
        too_large(),
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job_large","location":"EU"},
            "schema": {"fields":[{"name":"big","type":"STRING"}]},
            "totalRows": "2"
        })),
        ok(json!({
            "jobReference": {"projectId":"project","jobId":"job_large","location":"EU"},
            "status": {"state":"DONE"},
            "statistics": {"query": {"statementType": "SELECT"}}
        })),
    ]);
    let completed = run(&endpoint, plan(StatementQueryOptions::default())).unwrap();

    assert!(completed.metadata.rows_exceed_rest);
    assert_eq!(completed.metadata.total_rows, Some(2));
    assert!(completed.page.rows.is_empty() && completed.page.page_token.is_none());
    let captured = captured.join().unwrap();
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert!(
        captured[2]
            .path()
            .starts_with("/bigquery/v2/projects/project/queries/job_large?maxResults=100000&")
    );
    assert!(
        captured[3]
            .path()
            .starts_with("/bigquery/v2/projects/project/queries/job_large?maxResults=0&")
    );
}

/// An incomplete answer that comes back early is paced, not reissued at once: nothing enforces
/// BigQuery's long-poll `timeoutMs`, so a mock answering `jobComplete:false` immediately must still
/// take at least `MINIMUM_POLL_INTERVAL` per poll.
#[test]
fn instant_incomplete_answers_are_paced_rather_than_spun_on() {
    let incomplete = json!({
        "jobComplete": false,
        "jobReference": {"projectId":"project","jobId":"job-1","location":"EU"},
        "statementType": "SELECT"
    });
    let (endpoint, captured) = capture_requests(vec![
        (200, incomplete.clone()),
        (200, incomplete.clone()),
        (200, incomplete.clone()),
        (200, incomplete),
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job-1","location":"EU"},
            "schema": {"fields":[]},
            "rows": []
        })),
    ]);
    let (runtime, client) = client(&endpoint);
    let started = std::time::Instant::now();
    complete(
        &runtime,
        client,
        plan(StatementQueryOptions::default()),
        QueryResultMode::Rows,
        "test",
    )
    .unwrap();
    let elapsed = started.elapsed();

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 5);
    // Three incomplete polls follow the incomplete submission, so three intervals are floored.
    assert!(
        elapsed >= 3 * MINIMUM_POLL_INTERVAL,
        "the completion poll spun instead of pacing itself: {elapsed:?}"
    );
}

#[test]
fn completed_warning_is_verified_as_success_without_reissue() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job-1","location":"EU"},
            "schema": {"fields":[]},
            "errors": [{"reason":"backendError","message":"warning"}]
        })),
        ok(json!({
            "jobReference": {"projectId":"project","jobId":"job-1","location":"EU"},
            "status": {"state":"DONE","errors":[{"reason":"backendError","message":"warning"}]}
        })),
    ]);
    run(&endpoint, plan(StatementQueryOptions::default())).unwrap();
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].line.starts_with("POST "));
    assert!(captured[1].line.starts_with("GET "));
}

#[test]
fn completed_no_schema_response_verifies_errors_as_warnings() {
    let reference = json!({"projectId":"project","jobId":"job-1","location":"EU"});
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobComplete": true,
            "jobReference": reference,
            "errors": [{"reason":"backendError","message":"warning"}]
        })),
        ok(json!({"jobReference":reference,"status":{"state":"DONE"}})),
    ]);
    let completed = run(&endpoint, plan_at(StatementQueryOptions::default(), None)).unwrap();
    assert!(completed.metadata.job_reference.is_some());
    assert!(completed.metadata.schema.is_none());
    assert_eq!(captured.join().unwrap().len(), 2);
}
