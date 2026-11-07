//! `reference.rs`: the job reference a response names, the location it is bound to, and the
//! job link a failure carries.

use super::*;
use crate::test_support::errors::assert_message_omits;
use crate::test_support::identifiers::project;

#[test]
fn failed_jobs_link_to_their_server_returned_location() {
    for polled in [false, true] {
        let reference =
            json!({"projectId":"project","jobId":"failed-job","location":"us-central1"});
        let mut responses = Vec::new();
        if polled {
            responses.push((200, json!({"jobComplete":false,"jobReference":reference})));
        }
        responses.extend([
            (200, json!({
                "jobComplete":true,"jobReference":reference,
                "errors":[{"reason":"invalidQuery","message":"division by zero","location":"query"}]
            })),
            (200, json!({
                "jobReference":reference,
                "status":{"state":"DONE","errorResult":{"reason":"invalidQuery","message":"division by zero","location":"query"}}
            })),
        ]);
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = client(&endpoint);
        let failure = complete(
            &runtime,
            client,
            plan_at(StatementQueryOptions::default(), None),
            QueryResultMode::Rows,
            "query failed",
        )
        .unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_message(&failure, &["invalidQuery: division by zero (field: query)"]);
        assert!(failure.message.ends_with(" (Query: https://console.cloud.google.com/bigquery?project=project&j=bq:us-central1:failed-job&page=queryresults)"));
        assert_eq!(failure.sqlstate, [0x34, 0x32, 0x30, 0x30, 0x30]);
        assert_eq!(
            failure
                .message
                .matches("https://console.cloud.google.com/")
                .count(),
            1
        );
        assert_eq!(captured.join().unwrap().len(), if polled { 3 } else { 2 });
    }
}

#[test]
fn polling_failures_link_to_the_known_job_but_submission_rejections_do_not() {
    for polled in [false, true] {
        let mut responses = Vec::new();
        if polled {
            responses.push((
                200,
                json!({
                    "jobComplete":false,
                    "jobReference":{"projectId":"project","jobId":"running-job","location":"EU"}
                }),
            ));
        }
        responses.push((
            403,
            json!({"error":{"errors":[{"reason":"accessDenied","message":"access denied"}]}}),
        ));
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = client(&endpoint);
        let failure = complete(
            &runtime,
            client,
            plan(StatementQueryOptions::default()),
            QueryResultMode::Rows,
            "query failed",
        )
        .unwrap_err();
        assert_eq!(failure.status, Status::Unauthorized);
        assert_message(&failure, &["access denied"]);
        if polled {
            assert!(failure.message.ends_with(" (Query: https://console.cloud.google.com/bigquery?project=project&j=bq:EU:running-job&page=queryresults)"));
        } else {
            assert_message_omits(&failure, &["https://console.cloud.google.com/"]);
        }
        assert_eq!(captured.join().unwrap().len(), if polled { 2 } else { 1 });
    }
}

/// On either route, later requests address the job by the location BigQuery returned (`US`), not
/// the configured one (`us`).
#[test]
fn both_routes_keep_the_services_canonical_location_casing() {
    for priority in [
        BigQueryQueryPriority::Interactive,
        BigQueryQueryPriority::Batch,
    ] {
        let plan = plan_at(
            StatementQueryOptions {
                priority,
                ..StatementQueryOptions::default()
            },
            Some("us"),
        );
        // Assert each priority's route rather than reading it off the plan, so a routing
        // regression cannot send both cases down the same arm.
        let inserts = priority == BigQueryQueryPriority::Batch;
        assert_eq!(plan.inserts_job(), inserts, "{priority:?}");
        // Only the insert route names its job before submitting, which is also the only route
        // that has a second request to check the canonical casing on.
        let job_id = plan.job_id().map(ToOwned::to_owned);
        let reference = json!({
            "projectId": "project",
            "jobId": job_id.as_deref().unwrap_or("job-1"),
            "location": "US"
        });
        let results = json!({
            "jobComplete": true,
            "jobReference": reference,
            "schema": {"fields":[]}
        });
        let responses = if inserts {
            vec![
                (
                    200,
                    json!({"jobReference": reference, "status": {"state":"DONE"}}),
                ),
                (200, results),
            ]
        } else {
            vec![(200, results)]
        };
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = client(&endpoint);
        let completed = complete(&runtime, client, plan, QueryResultMode::Rows, "test").unwrap();

        assert_eq!(
            completed
                .metadata
                .job_reference
                .as_ref()
                .and_then(|reference| reference.location.as_deref()),
            Some("US")
        );
        let captured = captured.join().unwrap();
        if inserts {
            let job_id = job_id.expect("the insert route names its job before submitting");
            assert_eq!(captured.len(), 2);
            assert_eq!(captured[0].body["jobReference"]["location"], "us");
            assert_eq!(captured[0].body["jobReference"]["jobId"], job_id);
            assert!(captured[1].path().contains("&location=US"));
        } else {
            assert_eq!(captured.len(), 1);
            assert_eq!(captured[0].body["location"], "us");
        }
    }
}

/// jobs.insert echoes the requested project number, whereas jobs.query/getQueryResults resolve
/// it to the project ID. Once resolved, later polls must address and verify that canonical ID.
#[test]
fn both_query_routes_follow_a_numeric_projects_canonical_identity() {
    for priority in [
        BigQueryQueryPriority::Interactive,
        BigQueryQueryPriority::Batch,
    ] {
        let mut plan = plan(StatementQueryOptions {
            priority,
            ..StatementQueryOptions::default()
        });
        plan.project_id = project("123456789");
        let job_id = plan.job_id().unwrap_or("query-job").to_owned();
        let canonical = json!({
            "projectId": "canonical-project", "jobId": job_id, "location": "EU"
        });
        let mut responses = Vec::new();
        if priority == BigQueryQueryPriority::Batch {
            responses.push((
                200,
                json!({
                    "jobReference": {"projectId": "123456789", "jobId": job_id, "location": "EU"},
                    "status": {"state": "RUNNING"},
                    "statistics": {"query": {"statementType": "SELECT"}}
                }),
            ));
        }
        responses.extend([
            (
                200,
                json!({
                    "jobComplete": false, "jobReference": canonical,
                    "statementType": "SELECT"
                }),
            ),
            (
                200,
                json!({
                    "jobComplete": true, "jobReference": canonical,
                    "statementType": "SELECT", "schema": {"fields": []}
                }),
            ),
        ]);
        let (endpoint, captured) = capture_requests(responses);
        let (runtime, client) = client(&endpoint);
        let completed = complete(&runtime, client, plan, QueryResultMode::Rows, "test").unwrap();
        assert_eq!(
            completed.metadata.job_reference.unwrap(),
            JobReference::new("canonical-project", Some("EU"), job_id.clone())
        );

        let captured = captured.join().unwrap();
        let first_route = if priority == BigQueryQueryPriority::Batch {
            assert_eq!(captured.len(), 3);
            assert_eq!(
                captured[1].path(),
                format!(
                    "/bigquery/v2/projects/123456789/queries/{job_id}?maxResults=100000&timeoutMs=45000&formatOptions.useInt64Timestamp=true&location=EU"
                )
            );
            "jobs"
        } else {
            assert_eq!(captured.len(), 2);
            "queries"
        };
        assert_eq!(
            captured[0].path(),
            format!("/bigquery/v2/projects/123456789/{first_route}")
        );
        assert_eq!(
            captured.last().unwrap().path(),
            format!(
                "/bigquery/v2/projects/canonical-project/queries/{job_id}?maxResults=100000&timeoutMs=45000&formatOptions.useInt64Timestamp=true&location=EU"
            )
        );
    }
}

#[test]
fn jobs_query_reference_is_bound_to_the_request_but_default_location_is_optional() {
    let mut default_location = QueryResponse::decode(
        br#"{"jobComplete":false,"jobReference":{"projectId":"project","jobId":"job"}}"#,
        "locationless response",
    )
    .unwrap();
    canonicalize_query_response_reference(&mut default_location, "project", None).unwrap();
    assert!(default_location.job_reference.unwrap().location.is_none());

    let mut wrong_project = QueryResponse::decode(
        br#"{"jobComplete":false,"jobReference":{"projectId":"other","jobId":"job","location":"EU"}}"#,
        "wrong project response",
    )
    .unwrap();
    assert_eq!(
        canonicalize_query_response_reference(&mut wrong_project, "project", Some("EU"))
            .unwrap_err()
            .status,
        Status::InvalidData
    );
}
