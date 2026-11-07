//! `attempt.rs`: `jobs.query`, `jobs.insert`, and the wire invariants of each.

use super::*;

/// A transport failure can lose the response to a `jobs.query` BigQuery accepted; its replay then
/// gets 409 `duplicate` naming the job, whose results the query reads instead of failing.
#[test]
fn a_replayed_jobs_query_reads_the_job_its_first_attempt_created() {
    let (endpoint, captured) = capture_requests(vec![
        (503, error_body("backendError", "lost")),
        (
            409,
            json!({"error": {"code": 409, "errors": [{"reason": "duplicate",
                "message": "Already Exists: Job project:EU.job_first"}]}}),
        ),
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":"job_first","location":"EU"},
            "schema": {"fields":[{"name":"value","type":"INTEGER"}]},
            "rows": [{"f":[{"v":"7"}]}],
            "totalRows": "1"
        })),
        // The results never name the statement type, so the job is described once.
        ok(json!({
            "jobReference": {"projectId":"project","jobId":"job_first","location":"EU"},
            "status": {"state":"DONE"},
            "statistics": {"query": {"statementType": "SELECT"}}
        })),
    ]);
    let completed = run(&endpoint, plan(StatementQueryOptions::default())).unwrap();

    assert_eq!(completed.metadata.statement_type, "SELECT");
    assert!(completed.completed_job.is_some());
    assert_eq!(
        completed.metadata.job_reference,
        Some(JobReference::new(
            "project",
            Some("EU"),
            "job_first".to_owned()
        ))
    );
    assert_eq!(completed.page.rows.len(), 1);
    let captured = captured.join().unwrap();
    assert_eq!(captured[0].raw_body, captured[1].raw_body);
    assert!(
        captured[2]
            .path()
            .starts_with("/bigquery/v2/projects/project/queries/job_first?"),
        "{}",
        captured[2].path()
    );
}

#[test]
fn jobs_query_jobless_result_keeps_initial_page_and_wire_invariants() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({
        "jobComplete": true,
        "queryId": "query-1",
        "schema": {"fields":[{"name":"value","type":"INTEGER"}]},
        "rows": [{"f":[{"v":"7"}]}],
        "totalRows": "1",
        "statementType": "SELECT",
        "sessionInfo": {"sessionId":"session-1"}
    }))]);
    let completed = run(&endpoint, plan_at(StatementQueryOptions::default(), None)).unwrap();

    assert!(completed.metadata.job_reference.is_none());
    assert_eq!(completed.metadata.query_id, "query-1");
    assert_eq!(completed.metadata.statement_type, "SELECT");
    assert_eq!(completed.metadata.session_id.as_deref(), Some("session-1"));
    assert_eq!(completed.page.rows.len(), 1);
    let cells = completed.page.rows.into_iter().next().unwrap().into_cells();
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].as_str(), Some("7"));
    assert!(completed.page.page_token.is_none());

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].path(), "/bigquery/v2/projects/project/queries");
    assert_eq!(captured[0].body["query"], "SELECT 1");
    assert_eq!(captured[0].body["useLegacySql"], false);
    assert_eq!(captured[0].body["jobCreationMode"], "JOB_CREATION_OPTIONAL");
    assert_eq!(captured[0].body["timeoutMs"], 10_000);
    assert_eq!(captured[0].body["formatOptions"]["useInt64Timestamp"], true);
    assert_eq!(captured[0].body["maxResults"], 100_000);
    assert!(captured[0].body["requestId"].as_str().is_some());
}

#[test]
fn jobs_query_metadata_only_requests_no_rows_and_drops_an_unexpected_page() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({
        "jobComplete": true,
        "jobReference": {"projectId":"project","jobId":"query-1","location":"EU"},
        "schema": {"fields":[{"name":"value","type":"INTEGER"}]},
        "rows": [{"f":[{"v":"7"}]}],
        "pageToken": "must-not-be-fetched",
        "statementType": "SELECT"
    }))]);
    let (runtime, client) = client_with_one_result_worker(&endpoint);
    let permit = runtime
        .result_conversion_permits()
        .try_acquire_owned()
        .unwrap();
    let execute_runtime = runtime;
    let (finished, completion) = mpsc::sync_channel(0);
    let execute = thread::spawn(move || {
        let result = complete(
            &execute_runtime,
            client,
            plan_at(StatementQueryOptions::default(), None),
            QueryResultMode::MetadataOnly,
            "test",
        );
        finished.send(result).unwrap();
    });
    let completed = match completion.recv_timeout(Duration::from_secs(5)) {
        Ok(result) => result.unwrap(),
        Err(source) => {
            drop(permit);
            execute.join().unwrap();
            panic!("metadata-only decode waited for a result-work permit: {source}");
        }
    };
    drop(permit);
    execute.join().unwrap();

    assert!(!completed.returns_rows);
    assert!(completed.metadata.schema.is_some());
    assert!(completed.page.rows.is_empty());
    assert!(completed.page.page_token.is_none());

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].body["maxResults"], 0);
}

#[test]
fn required_job_creation_rejects_responses_without_a_job_reference() {
    for complete_response in [false, true] {
        let (endpoint, captured) = capture_requests(vec![ok(
            json!({"jobComplete": complete_response, "queryId": "jobless-query"}),
        )]);
        let (runtime, client) = client(&endpoint);
        let mut plan = plan(StatementQueryOptions::default());
        plan.job_creation = QueryJobCreation::Required;
        let failure = complete(&runtime, client, plan, QueryResultMode::Rows, "test").unwrap_err();

        assert_failure(
            &failure,
            Status::InvalidData,
            &["returned without a job reference"],
        );
        let captured = captured.join().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].body["jobCreationMode"], "JOB_CREATION_REQUIRED");
        assert_eq!(captured[0].body["timeoutMs"], 10_000);
    }
}

#[test]
fn optional_job_creation_rejects_an_incomplete_response_without_a_job_reference() {
    let (endpoint, captured) = capture_requests(vec![ok(
        json!({"jobComplete": false, "queryId": "jobless-query"}),
    )]);
    let failure = run(&endpoint, plan(StatementQueryOptions::default())).unwrap_err();

    assert_failure(
        &failure,
        Status::InvalidData,
        &["incomplete", "no job reference to poll"],
    );
    assert_eq!(captured.join().unwrap().len(), 1);
}

#[test]
fn dry_insert_without_location_is_synchronous_and_does_not_invent_a_job() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({
        "configuration":{"dryRun":true},
        "jobReference":{"projectId":"project","location":"US"},
        "statistics":{"query":{"schema":{"fields":[]},"undeclaredQueryParameters":[]}},
        "status":{"state":"DONE"}
    }))]);
    let (runtime, client) = client(&endpoint);
    let completed = complete(
        &runtime,
        client,
        plan_at(
            StatementQueryOptions {
                dry_run: true,
                ..StatementQueryOptions::default()
            },
            None,
        ),
        QueryResultMode::MetadataOnly,
        "test",
    )
    .unwrap();
    assert!(completed.metadata.job_reference.is_none());
    assert!(completed.metadata.schema.is_some());
    assert_eq!(
        completed.metadata.undeclared_query_parameters,
        Some(Vec::new())
    );

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(
        captured[0]
            .path()
            .starts_with("/bigquery/v2/projects/project/jobs")
    );
    assert!(captured[0].body["jobReference"].get("location").is_none());
    assert_eq!(captured[0].body["configuration"]["dryRun"], true);
}

/// Without a location, BigQuery infers where the job runs, and every later request carries the
/// location its answer names.
#[test]
fn non_dry_insert_without_location_polls_where_bigquery_ran_the_job() {
    let plan = plan_at(
        StatementQueryOptions {
            priority: BigQueryQueryPriority::Batch,
            ..StatementQueryOptions::default()
        },
        None,
    );
    let job_id = plan.job_id().unwrap().to_owned();
    let located = json!({"projectId":"project","jobId":job_id,"location":"asia-northeast1"});
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobReference":located,
            "status":{"state":"RUNNING"},
            "statistics":{"query":{"statementType":"SELECT"}}
        })),
        ok(json!({
            "jobComplete":true,
            "jobReference":located,
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "rows":[{"f":[{"v":"7"}]}]
        })),
    ]);
    let completed = run(&endpoint, plan).unwrap();

    assert_eq!(completed.page.rows.len(), 1);
    assert_eq!(
        completed
            .metadata
            .job_reference
            .unwrap()
            .location
            .as_deref(),
        Some("asia-northeast1")
    );
    assert!(completed.metadata.completed_by_poll);
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].body["jobReference"].get("location").is_none());
    assert_eq!(
        captured[0].body["configuration"]["query"]["priority"],
        "BATCH"
    );
    assert!(
        captured[1]
            .path()
            .starts_with(&format!("/bigquery/v2/projects/project/queries/{job_id}?"))
            && captured[1].path().ends_with("&location=asia-northeast1"),
        "{}",
        captured[1].path()
    );
}

/// A destination query whose answer was lost is looked for in its destination dataset's location
/// once the lookup without a location finds nothing there.
#[test]
fn a_lost_destination_insert_without_location_is_found_in_the_destination_dataset() {
    let plan = plan_at(
        StatementQueryOptions {
            destination_table: Some(
                crate::query::QualifiedTableName::parse_option(
                    BigQueryStatementOption::DestinationTable.as_str(),
                    "project.dataset.destination",
                    None,
                    None,
                )
                .unwrap(),
            ),
            ..StatementQueryOptions::default()
        },
        None,
    );
    let job_id = plan.job_id().unwrap().to_owned();
    let located = json!({"projectId":"project","jobId":job_id,"location":"asia-northeast1"});
    let attempts = crate::rest::retry::MAX_ATTEMPTS as usize;
    let mut responses = vec![(503, error_body("backendError", "try later")); attempts];
    // Each of the three reconciliation attempts without a location finds nothing.
    responses.extend(vec![(404, error_body("notFound", "Not found: Job")); 3]);
    responses.push(ok(json!({"location":"asia-northeast1"})));
    responses.push(ok(
        json!({"jobReference":located,"status":{"state":"DONE"}}),
    ));
    responses.push(ok(
        json!({"jobComplete":true,"jobReference":located,"schema":{"fields":[]}}),
    ));
    let (endpoint, captured) = capture_requests(responses);
    let (runtime, client) = client(&endpoint);
    let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
    let client = client.with_dataset_locations(state.dataset_locations(&runtime).unwrap());
    complete(
        &runtime,
        client,
        plan,
        QueryResultMode::MetadataOnly,
        "test",
    )
    .unwrap();

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), attempts + 6);
    assert!(
        captured[attempts + 3]
            .path()
            .starts_with("/bigquery/v2/projects/project/datasets/dataset?"),
        "{}",
        captured[attempts + 3].path()
    );
    assert_eq!(
        captured[attempts + 4].path(),
        format!("/bigquery/v2/projects/project/jobs/{job_id}?location=asia-northeast1")
    );
    assert!(
        captured[attempts + 5]
            .path()
            .ends_with("&location=asia-northeast1")
    );
}

#[test]
fn non_dry_insert_requires_and_uses_the_exact_caller_job_id() {
    let plan = plan(StatementQueryOptions {
        priority: BigQueryQueryPriority::Batch,
        reservation: Some(Arc::from(
            "projects/project/locations/EU/reservations/analytics",
        )),
        ..StatementQueryOptions::default()
    });
    let job_id = plan.job_id().unwrap().to_owned();
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobReference":{"projectId":"project","jobId":job_id,"location":"EU"},
            "statistics":{"query":{"statementType":"SELECT","totalBytesBilled":"9"}},
            "status":{"state":"DONE"}
        })),
        ok(json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":plan.job_id().unwrap(),"location":"EU"},
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "rows":[{"f":[{"v":"7"}]}],
            "pageToken":"next-page"
        })),
    ]);
    let completed = run(&endpoint, plan).unwrap();
    assert!(completed.completed_job.is_some());
    assert_eq!(completed.metadata.total_bytes_billed, Some(9));
    assert_eq!(completed.page.rows.len(), 1);
    assert_eq!(completed.page.page_token.as_deref(), Some("next-page"));

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].body["jobReference"]["jobId"], job_id);
    // A configured location is sent, and every later request carries it.
    assert_eq!(captured[0].body["jobReference"]["location"], "EU");
    assert!(captured[1].path().ends_with("&location=EU"));
    assert_eq!(
        captured[0].body["configuration"]["reservation"],
        "projects/project/locations/EU/reservations/analytics"
    );
    assert!(
        captured[0]
            .path()
            .starts_with("/bigquery/v2/projects/project/jobs")
    );
    assert!(
        captured[1]
            .path()
            .contains(&format!("/queries/{job_id}?maxResults=100000&"))
    );
}

#[test]
fn destination_insert_requests_no_rows_and_drops_an_unexpected_page() {
    let plan = plan(StatementQueryOptions {
        destination_table: Some(
            crate::query::QualifiedTableName::parse_option(
                BigQueryStatementOption::DestinationTable.as_str(),
                "project.dataset.destination",
                None,
                None,
            )
            .unwrap(),
        ),
        ..StatementQueryOptions::default()
    });
    let job_id = plan.job_id().unwrap().to_owned();
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobReference":{"projectId":"project","jobId":job_id,"location":"EU"},
            "statistics":{"query":{"statementType":"SELECT"}},
            "status":{"state":"DONE"}
        })),
        ok(json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":plan.job_id().unwrap(),"location":"EU"},
            "schema":{"fields":[{"name":"value","type":"INTEGER"}]},
            "rows":[{"f":[{"v":"7"}]}],
            "pageToken":"must-not-be-fetched"
        })),
    ]);
    let (runtime, client) = client(&endpoint);
    let completed = complete(
        &runtime,
        client,
        plan,
        QueryResultMode::MetadataOnly,
        "test",
    )
    .unwrap();

    assert!(!completed.returns_rows);
    assert!(completed.metadata.schema.is_some());
    assert!(completed.page.rows.is_empty());
    assert!(completed.page.page_token.is_none());
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(
        captured[1]
            .path()
            .contains(&format!("/queries/{job_id}?maxResults=0&"))
    );
}

#[test]
fn running_insert_preserves_the_session_its_result_poll_cannot_return() {
    let plan = plan(StatementQueryOptions {
        priority: BigQueryQueryPriority::Batch,
        create_session: true,
        ..StatementQueryOptions::default()
    });
    let job_id = plan.job_id().unwrap().to_owned();
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "jobReference":{"projectId":"project","jobId":job_id,"location":"EU"},
            "statistics":{
                "totalBytesProcessed":"1",
                "query":{"statementType":"SELECT"},
                "sessionInfo":{"sessionId":"created-session"}
            },
            "status":{"state":"RUNNING"}
        })),
        ok(json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":plan.job_id().unwrap(),"location":"EU"},
            "schema":{"fields":[]},
            "totalBytesProcessed":"2"
        })),
    ]);
    let completed = run(&endpoint, plan).unwrap();

    assert_eq!(
        completed.metadata.session_id.as_deref(),
        Some("created-session")
    );
    assert_eq!(
        completed.metadata.total_bytes_processed,
        Some(2),
        "the RUNNING insert's partial costs must not replace the completed poll"
    );
    assert_eq!(captured.join().unwrap().len(), 2);
}

#[test]
fn dry_insert_requires_a_done_status() {
    let (endpoint, captured) = capture_requests(vec![ok(
        json!({"configuration":{"dryRun":true},"status":{"state":"RUNNING"}}),
    )]);
    let (runtime, client) = client(&endpoint);
    let failure = complete(
        &runtime,
        client,
        plan_at(
            StatementQueryOptions {
                dry_run: true,
                ..StatementQueryOptions::default()
            },
            None,
        ),
        QueryResultMode::MetadataOnly,
        "test",
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(captured.join().unwrap().len(), 1);
}

/// A job stopped by `bigquery.query.job_timeout` is a TIMEOUT naming the option; a `stopped` job
/// someone cancelled stays CANCELLED.
#[test]
fn a_job_the_job_timeout_stopped_is_a_timeout_naming_the_option() {
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
        let (endpoint, captured) = capture_requests(vec![(499, error_body("stopped", message))]);
        let failure = run(
            &endpoint,
            plan(StatementQueryOptions {
                job_timeout_ms: 2000,
                ..StatementQueryOptions::default()
            }),
        )
        .unwrap_err();
        let failure = explain_job_timeout(failure, 2000);
        assert_eq!(captured.join().unwrap().len(), 1, "{message}");
        assert_eq!(failure.status, expected, "{message}");
        assert!(
            failure
                .message
                .starts_with("BigQuery stopped the query: stopped: "),
            "{}",
            failure.message
        );
        let remedy = "; BigQuery stops a job that runs longer than bigquery.query.job_timeout \
                      (2000 ms)";
        assert_eq!(
            failure.message.ends_with(remedy),
            expected == Status::Timeout,
            "{}",
            failure.message
        );
    }
}
