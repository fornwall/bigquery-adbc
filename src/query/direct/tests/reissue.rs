//! The reissue loop: which failures earn another attempt, and what a fresh attempt changes.

use super::*;

/// One failed attempt: the `jobs.query` response carrying the errors, then the `jobs.get` whose
/// `status.errorResult` confirms them. `statement_type` feeds `reissuable_statement`.
fn failed_attempt(
    job_id: &str,
    reason: &str,
    statement_type: &str,
) -> Vec<(u16, serde_json::Value)> {
    vec![
        ok(json!({
            "jobComplete": true,
            "jobReference": {"projectId":"project","jobId":job_id,"location":"EU"},
            "statementType": statement_type,
            "errors": [{"reason":reason,"message":"temporary"}]
        })),
        ok(json!({
            "jobReference": {"projectId":"project","jobId":job_id,"location":"EU"},
            "status": {
                "state":"DONE",
                "errorResult":{"reason":reason,"message":"temporary"}
            }
        })),
    ]
}

#[test]
fn terminal_transient_failures_reissue_three_times_with_fresh_request_ids() {
    let mut responses = Vec::new();
    for attempt in 0..=MAX_JOB_REISSUES {
        let job_id = format!("job-{attempt}");
        let reason = if attempt % 2 == 0 {
            "jobInternalError"
        } else {
            "tableUnavailable"
        };
        responses.extend(failed_attempt(&job_id, reason, "SELECT"));
    }
    let (endpoint, captured) = capture_requests(responses);
    let failure = run(
        &endpoint,
        plan(StatementQueryOptions {
            reservation: Some(Arc::from("none")),
            ..StatementQueryOptions::default()
        }),
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::IO);
    assert!(failure.message.ends_with(
        " (Query: https://console.cloud.google.com/bigquery?project=project&j=bq:EU:job-3&page=queryresults)"
    ));
    assert_eq!(
        failure
            .message
            .matches("https://console.cloud.google.com/")
            .count(),
        1
    );

    let captured = captured.served(2 * (MAX_JOB_REISSUES + 1));
    let request_ids: HashSet<_> = captured
        .iter()
        .filter(|request| request.line.starts_with("POST "))
        .map(|request| {
            assert_eq!(request.body["reservation"], "none");
            request.body["requestId"].as_str().unwrap()
        })
        .collect();
    assert_eq!(request_ids.len(), MAX_JOB_REISSUES + 1);
}

/// A reissue reruns the whole SQL, which is safe only for a single statement: a script's earlier
/// statements autocommit, so rerunning `CREATE TEMP TABLE ...; INSERT ...; INSERT ...;` after the
/// last statement fails would apply the middle `INSERT` twice. A missing statement type is not
/// reissued either, because `reissuable_statement` is an allowlist.
#[test]
fn a_failed_script_is_reported_rather_than_run_again() {
    for (what, statement_type) in [
        ("a multi-statement query", "SCRIPT"),
        ("a procedure call, whose body is one", "CALL"),
        ("a statement BigQuery did not name", ""),
    ] {
        // Only one attempt's worth of responses: a second submission has nothing to answer it.
        let (endpoint, captured) =
            capture_requests(failed_attempt("job-0", "backendError", statement_type));
        let failure = run(&endpoint, plan(StatementQueryOptions::default())).unwrap_err();

        assert_eq!(failure.status, Status::IO, "{what}");
        assert!(
            failure.message.contains("backendError: temporary"),
            "{what}"
        );
        assert_eq!(
            captured.join().unwrap().len(),
            2,
            "{what} was submitted more than once"
        );
    }
}

#[test]
fn reissue_keeps_route_and_query_but_refreshes_identity() {
    for mut options in [
        StatementQueryOptions::default(),
        StatementQueryOptions {
            dry_run: true,
            ..StatementQueryOptions::default()
        },
    ] {
        options.reservation = Some(Arc::from("none"));
        let original = plan(options);
        let reissued = original.clone().reissue();
        assert_eq!(reissued.inserts_job(), original.inserts_job());
        assert_eq!(reissued.sql, original.sql);
        assert_eq!(reissued.reservation, original.reservation);
        assert_ne!(reissued.identity(), original.identity());
    }
}
