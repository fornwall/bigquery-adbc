use super::*;
use crate::test_support::errors::assert_message_omits;
use serde_json::json;

fn job_error(reason: &str) -> ErrorProto {
    ErrorProto {
        reason: reason.to_owned(),
        ..Default::default()
    }
}

#[test]
fn query_response_accepts_string_and_number_int64s() {
    let mut response = QueryResponse::decode(
        br#"{
            "jobComplete": true,
            "totalRows": "3",
            "totalBytesProcessed": 4,
            "totalBytesBilled": "5",
            "totalSlotMs": 6,
            "numDmlAffectedRows": "7"
        }"#,
        "test query response",
    )
    .unwrap();
    let mut metadata = QueryMetadata::default();
    metadata.absorb_query(&mut response);
    assert_eq!(metadata.total_rows, Some(3));
    assert_eq!(metadata.total_bytes_processed, Some(4));
    assert_eq!(metadata.total_bytes_billed, Some(5));
    assert_eq!(metadata.total_slot_ms, Some(6));
    assert_eq!(metadata.num_dml_affected_rows, Some(7));
}

#[test]
fn metadata_keeps_submission_fields_missing_from_poll_response() {
    let mut initial = QueryResponse::decode(
        br#"{
            "jobComplete": false,
            "queryId": "query-id",
            "jobReference": {"projectId":"p","jobId":"j"},
            "statementType": "SCRIPT",
            "totalBytesBilled": "11",
            "totalSlotMs": "12",
            "sessionInfo": {"sessionId":"session"}
        }"#,
        "initial response",
    )
    .unwrap();
    let mut final_response = QueryResponse::decode(
        br#"{
            "jobComplete": true,
            "totalRows": "1",
            "totalBytesProcessed": "10",
            "schema": {"fields":[]},
            "rows": []
        }"#,
        "poll response",
    )
    .unwrap();
    let mut metadata = QueryMetadata::default();
    metadata.absorb_query(&mut initial);
    metadata.absorb_query(&mut final_response);
    assert_eq!(metadata.query_id, "query-id");
    assert_eq!(metadata.statement_type, "SCRIPT");
    assert_eq!(metadata.job_reference.unwrap().job_id, "j");
    assert_eq!(metadata.total_bytes_billed, Some(11));
    assert_eq!(metadata.total_slot_ms, Some(12));
    assert_eq!(metadata.total_bytes_processed, Some(10));
    assert_eq!(metadata.session_id.as_deref(), Some("session"));
}

#[test]
fn job_response_projects_schema_parameters_statistics_and_destination() {
    let mut response = JobResponse::decode(
        br#"{
            "jobReference":{"projectId":"p","jobId":"j","location":"us"},
            "configuration":{"dryRun":true,"query":{"destinationTable":{"projectId":"p","datasetId":"d","tableId":"t"}}},
            "statistics":{
                "totalBytesProcessed":"21",
                "totalSlotMs":22,
                "sessionInfo":{"sessionId":"s"},
                "query":{
                    "statementType":"SELECT",
                    "totalBytesBilled":"23",
                    "numDmlAffectedRows":"24",
                    "schema":{"fields":[]},
                    "undeclaredQueryParameters":[]
                }
            },
            "status":{"state":"DONE"}
        }"#,
        "job response",
    )
    .unwrap();
    let mut metadata = QueryMetadata::default();
    metadata.absorb_job(&mut response);
    assert!(
        response
            .configuration
            .as_ref()
            .unwrap()
            .query
            .as_ref()
            .unwrap()
            .destination_table
            .is_some()
    );
    assert_eq!(metadata.total_bytes_processed, Some(21));
    assert_eq!(metadata.total_slot_ms, Some(22));
    assert_eq!(metadata.total_bytes_billed, Some(23));
    assert_eq!(metadata.num_dml_affected_rows, Some(24));
    assert_eq!(metadata.statement_type, "SELECT");
    assert_eq!(metadata.session_id.as_deref(), Some("s"));
    assert!(metadata.schema.is_some());
}

#[test]
fn malformed_response_fields_do_not_echo_values() {
    let secret = "private-response-value";
    for body in [
        format!(r#"{{"totalBytesProcessed":"{secret}"}}"#),
        format!(r#"{{"jobComplete":"{secret}"}}"#),
        format!(r#"{{"jobReference":"{secret}"}}"#),
        format!(r#"{{"rows":[{{"f":"{secret}"}}]}}"#),
    ] {
        let failure = QueryResponse::decode(body.as_bytes(), "query response").unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert!(
            failure.message.starts_with(
                "failed to decode query response: unexpected JSON structure at line 1 column "
            ),
            "{}",
            failure.message,
        );
        assert_message_omits(&failure, &[secret]);
    }
}

/// The body is validated as UTF-8 up front, so a bad byte is still rejected as invalid data.
#[test]
fn a_response_that_is_not_utf8_is_invalid_data() {
    let failure =
        QueryResponse::decode(b"{\"statementType\":\"\xff\"}", "query response").unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert!(
        failure
            .message
            .starts_with("failed to decode query response: the response is not UTF-8"),
        "{}",
        failure.message
    );
}

/// Only the reasons BigQuery documents as worth another job are rerun, and only the first entry
/// (`status.errorResult`) decides, as in `crate::error::job_error`. The rest of `status.errors`
/// may be warnings or a script's `stopped` entries for its other statements.
#[test]
fn the_authoritative_first_error_alone_decides_whether_a_job_is_rerun() {
    let retryable = [
        "backendError",
        "jobBackendError",
        "jobInternalError",
        "tableUnavailable",
        "rateLimitExceeded",
        "jobRateLimitExceeded",
        "internalError",
    ];
    for reason in retryable {
        assert!(retryable_job_errors(&[job_error(reason)]));
    }
    assert!(!retryable_job_errors(&[]));
    assert!(!retryable_job_errors(&[job_error("invalidQuery")]));
    // `quotaExceeded` is retryable as a 403 on a request, not as a failed job: see
    // `retryable_job_errors`.
    assert!(!retryable_job_errors(&[job_error("quotaExceeded")]));

    // A padded or warning entry beside the authoritative reason changes nothing either way.
    assert!(retryable_job_errors(&[
        job_error("backendError"),
        job_error("stopped"),
        job_error(""),
    ]));
    assert!(!retryable_job_errors(&[
        job_error("invalidQuery"),
        job_error("backendError"),
    ]));
}

/// Only `errorResult` marks a job failed (`errors` may hold warnings alone), and it is put first,
/// whatever else `status.errors` holds, with its repeat there dropped.
#[test]
fn terminal_errors_lead_with_the_error_result() {
    let timed_out = json!({"reason": "stopped", "message": "Job timed out"});
    for (status, expected) in [
        (json!({"errors": [{"reason": "warning"}]}), None),
        (
            json!({"errorResult": {"reason": "invalidQuery"}}),
            Some(vec!["invalidQuery"]),
        ),
        (
            json!({
                "errorResult": {"reason": "backendError"},
                "errors": [{"message": "Job execution was cancelled"}, {"reason": "warning"}]
            }),
            Some(vec!["backendError", "", "warning"]),
        ),
        (
            json!({
                "errorResult": timed_out,
                "errors": [{"message": "secondary failure"}, timed_out]
            }),
            Some(vec!["stopped", ""]),
        ),
    ] {
        let body = serde_json::to_vec(&json!({"status": status})).unwrap();
        let mut response = JobResponse::decode(&body, "job response").unwrap();
        let errors = response.take_terminal_errors();
        let reasons = errors
            .as_ref()
            .map(|errors| errors.iter().map(|error| error.reason.as_str()).collect());
        assert_eq!(reasons, expected, "{status}");
    }
}

#[test]
fn polled_identity_keeps_location_and_rejects_replacement() {
    let expected = JobReference::new("p", Some("EU"), "j".to_owned());
    let sparse = QueryResponse::decode(
        br#"{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"j"}}"#,
        "sparse poll",
    )
    .unwrap();
    assert_eq!(
        sparse
            .canonical_polled_reference(&expected)
            .unwrap()
            .location
            .as_deref(),
        Some("EU")
    );

    let mismatch = QueryResponse::decode(
        br#"{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"other","location":"EU"}}"#,
        "mismatched poll",
    )
    .unwrap();
    assert_eq!(
        mismatch
            .canonical_polled_reference(&expected)
            .unwrap_err()
            .status,
        Status::InvalidData
    );
}

#[test]
fn omitted_parameters_are_empty_but_missing_query_statistics_are_unavailable() {
    for (body, expected) in [
        (r#"{}"#, None),
        (r#"{"statistics":{}}"#, None),
        (r#"{"statistics":{"query":{}}}"#, Some(Vec::new())),
        (
            r#"{"statistics":{"query":{"undeclaredQueryParameters":[]}}}"#,
            Some(Vec::new()),
        ),
    ] {
        let mut response = JobResponse::decode(body.as_bytes(), "parameter discovery").unwrap();
        let mut metadata = QueryMetadata::default();
        metadata.absorb_job(&mut response);
        assert_eq!(metadata.undeclared_query_parameters, expected, "{body}");
    }
}

#[test]
fn missing_parameter_list_preserves_previously_discovered_parameters() {
    let mut response = JobResponse::decode(
        br#"{"statistics":{"query":{"undeclaredQueryParameters":[{"name":"value","parameterType":{"type":"INT64"}}]}}}"#,
        "discovered parameters",
    )
    .unwrap();
    let mut metadata = QueryMetadata::default();
    metadata.absorb_job(&mut response);
    let expected = metadata.undeclared_query_parameters.clone();

    let mut later = JobResponse::decode(
        br#"{"statistics":{"query":{"totalBytesProcessed":"1"}}}"#,
        "later statistics",
    )
    .unwrap();
    metadata.absorb_job(&mut later);
    assert_eq!(metadata.undeclared_query_parameters, expected);
    assert_eq!(metadata.total_bytes_processed, Some(1));
}
