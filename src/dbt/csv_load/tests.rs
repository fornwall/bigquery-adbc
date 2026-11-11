use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::CancelHandle as _;
use adbc_core::error::Status;
use serde_json::{Value as JsonValue, json};

use super::{build_load_request, submit_csv_load};
use crate::cancellation::{Cancellation, Operation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::dbt::jobs::LoadOrCopyJob;
use crate::query::{QualifiedTableName, StatementQueryOptions};
use crate::rest::client::rest_client;
use crate::rest::jobs::{JobClient, JobReference};
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::rest::request::{Request, RequestBody, multipart_related, multipart_related_file};
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::http::{
    capture_requests, capture_requests_gated, capture_requests_until_stopped, error_body,
    job_reference, ok,
};
use crate::{BigQueryCreateDisposition, BigQueryWriteDisposition};

fn destination() -> QualifiedTableName {
    QualifiedTableName::parse_option(
        crate::BigQueryStatementOption::DestinationTable.as_str(),
        "example-project.sq_scratch.seed",
        None,
        None,
    )
    .unwrap()
}

fn reference() -> JobReference {
    JobReference::new("example-project", Some("EU"), "csv-job".to_owned())
}

/// `csv-job` in `state`, with the `statistics` BigQuery reports for it.
fn job(state: &str, statistics: &JsonValue) -> (u16, JsonValue) {
    ok(json!({
        "jobReference": job_reference("example-project", "csv-job"),
        "status": {"state": state},
        "statistics": statistics,
    }))
}

fn cancellable_at(endpoint: &str) -> (SharedRuntime, Arc<Cancellation>, Operation) {
    let runtime = new_runtime().unwrap();
    let cancellation = Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        "statement",
    );
    let operation = cancellation.begin();
    (runtime, cancellation, operation)
}

/// Sends one media upload through `submit_csv_load`, as `execute_csv_load` does.
fn submit(
    runtime: &SharedRuntime,
    operation: &Operation,
    jobs: &JobClient,
    url: &str,
    body: RequestBody,
) -> adbc_core::error::Result<LoadOrCopyJob> {
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let upload = rest_client("CSV load test").unwrap();
    runtime.block_on(submit_csv_load(
        operation,
        &credentials,
        &upload,
        jobs,
        Request::post_media(
            url,
            "multipart/related; boundary=test",
            body,
            "submit the BigQuery CSV load job",
        ),
        &reference(),
    ))
}

#[test]
fn load_metadata_is_compact_explicit_and_keeps_job_options() {
    let schema = TableSchema {
        fields: vec![TableFieldSchema {
            name: "id".to_owned(),
            r#type: "INTEGER".to_owned(),
            ..TableFieldSchema::default()
        }],
        ..TableSchema::default()
    };
    let options = StatementQueryOptions {
        create_disposition: BigQueryCreateDisposition::CreateNever,
        write_disposition: BigQueryWriteDisposition::WriteTruncate,
        job_timeout_ms: 12_345,
        labels: Some(Arc::new(BTreeMap::from([(
            "dbt_node".to_owned(),
            "seed".to_owned(),
        )]))),
        reservation: Some(Arc::from("projects/p/locations/EU/reservations/r")),
        ..StatementQueryOptions::default()
    };
    let request = build_load_request(&options, &destination(), &schema, ";", reference()).unwrap();
    let body: serde_json::Value = serde_json::from_slice(request.body()).unwrap();

    assert_eq!(body["jobReference"]["projectId"], "example-project");
    assert_eq!(body["jobReference"]["jobId"], "csv-job");
    assert_eq!(body["jobReference"]["location"], "EU");
    assert_eq!(body["configuration"]["jobTimeoutMs"], "12345");
    assert_eq!(body["configuration"]["labels"]["dbt_node"], "seed");
    assert_eq!(
        body["configuration"]["reservation"],
        "projects/p/locations/EU/reservations/r"
    );
    let load = &body["configuration"]["load"];
    assert_eq!(load["createDisposition"], "CREATE_NEVER");
    assert_eq!(load["writeDisposition"], "WRITE_TRUNCATE");
    assert_eq!(load["fieldDelimiter"], ";");
    assert_eq!(load["skipLeadingRows"], 1);
    assert_eq!(load["allowQuotedNewlines"], true);
    assert_eq!(load["sourceFormat"], "CSV");
    assert_eq!(load["autodetect"], false);
    assert_eq!(load["schema"]["fields"][0]["type"], "INTEGER");
}

#[test]
fn successful_upload_polls_the_exact_regional_job_and_reads_string_counts() {
    let (endpoint, captured) = capture_requests(vec![
        job("RUNNING", &json!({})),
        (
            404,
            json!({"error":{"errors":[{"reason":"notFound","message":"not visible yet"}]}}),
        ),
        job("DONE", &json!({"load": {"outputRows": "42"}})),
    ]);
    let (runtime, _cancellation, operation) = cancellable_at(&endpoint);
    let jobs = JobClient::for_test(&endpoint);
    let url =
        format!("{endpoint}/upload/bigquery/v2/projects/example-project/jobs?uploadType=multipart");
    let metadata = br#"{"configuration":{"load":{"sourceFormat":"CSV"}}}"#;
    let csv = b"id,label\r\n1,\"exact \x00 bytes\nwith LF\r\nwith CRLF\"\r\n2,final\n";
    let path = std::env::temp_dir().join(format!("bigquery-adbc-csv-{}.csv", std::process::id()));
    std::fs::write(&path, csv).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let body =
        multipart_related_file("test", metadata, file, csv.len() as u64, "text/csv").unwrap();
    assert!(body.is_streaming(), "the complete request is not buffered");
    let expected = multipart_related("test", metadata, csv, "text/csv");

    let rows = submit(&runtime, &operation, &jobs, &url, body)
        .unwrap()
        .output_rows();
    operation.finish();
    assert_eq!(rows, Some(42));

    let captured = captured.served(3);
    assert_eq!(
        captured[0].path(),
        "/upload/bigquery/v2/projects/example-project/jobs?uploadType=multipart"
    );
    assert_eq!(
        captured[0].header("content-type"),
        Some("multipart/related; boundary=test")
    );
    assert_eq!(
        captured[0].header("content-length"),
        Some(expected.len().to_string().as_str())
    );
    assert_eq!(captured[0].raw_body, expected);
    assert_eq!(
        captured[1].path(),
        "/bigquery/v2/projects/example-project/jobs/csv-job?location=EU"
    );
    assert_eq!(captured[1].line, captured[2].line);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn ambiguous_upload_is_reconciled_without_replaying_the_media() {
    let (endpoint, captured) = capture_requests(vec![
        (
            500,
            json!({"error":{"errors":[{"reason":"backendError","message":"later"}]}}),
        ),
        job("DONE", &json!({"load": {"outputRows": 7}})),
    ]);
    let (runtime, _cancellation, operation) = cancellable_at(&endpoint);
    let jobs = JobClient::for_test(&endpoint);
    let url =
        format!("{endpoint}/upload/bigquery/v2/projects/example-project/jobs?uploadType=multipart");

    assert_eq!(
        submit(
            &runtime,
            &operation,
            &jobs,
            &url,
            bytes::Bytes::from_static(b"large-media").into(),
        )
        .unwrap()
        .output_rows(),
        Some(7)
    );
    operation.finish();

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2, "the media POST is never replayed");
    assert!(captured[0].line.starts_with("POST "));
    assert!(captured[1].line.starts_with("GET "));
    assert_eq!(captured[0].raw_body, b"large-media");
    assert!(captured[1].raw_body.is_empty());
}

#[test]
fn cancellation_during_reconciliation_stops_after_confirming_the_one_shot_upload() {
    let (cancel_endpoint, cancels) = capture_requests(vec![ok(json!({}))]);
    let (runtime, cancellation, operation) = cancellable_at(&cancel_endpoint);
    let handle = OperationCancelHandle(cancellation);
    let (jobs_endpoint, reconciliations) =
        capture_requests_gated(vec![job("DONE", &json!({}))], move || {
            handle.try_cancel().unwrap();
            true
        });
    let jobs = JobClient::for_test(&jobs_endpoint);
    let (upload_endpoint, uploads) = capture_requests(vec![(
        500,
        json!({"error":{"errors":[{"reason":"backendError","message":"ambiguous"}]}}),
    )]);

    let failure = submit(
        &runtime,
        &operation,
        &jobs,
        &format!("{upload_endpoint}/upload"),
        bytes::Bytes::from_static(b"large-media").into(),
    )
    .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(uploads.join().unwrap().len(), 1);
    assert_eq!(reconciliations.join().unwrap().len(), 1);
    assert_eq!(cancels.join().unwrap().len(), 1);
}

/// An upload BigQuery may have accepted, whose job recovery then cannot fetch or read, names that
/// job, with its console link, as one that may still run: after a lost answer, after an answer
/// that does not decode, after a fetched job that does not decode, and after a 409 for a job that
/// never shows, which is no rejection, as for a `jobs.insert`.
#[test]
fn an_unrecovered_upload_says_its_job_may_still_run() {
    let attempts = crate::rest::jobs::MAX_RECONCILIATION_ATTEMPTS as usize;
    let unavailable = || vec![(503, error_body("backendError", "reconcile")); attempts];
    let lost = (500, error_body("backendError", "ambiguous"));
    let malformed = ok(json!({"jobReference": 5}));
    for (upload, fetched, reported, status) in [
        (
            lost.clone(),
            unavailable(),
            "backendError: reconcile (HTTP 503)",
            Status::IO,
        ),
        (
            malformed.clone(),
            unavailable(),
            "backendError: reconcile (HTTP 503)",
            Status::IO,
        ),
        // The fetched job is read like an accepted answer, which is reconciled once more.
        (
            lost,
            vec![malformed.clone(), malformed],
            "unexpected JSON structure",
            Status::InvalidData,
        ),
        (
            (
                409,
                error_body(
                    "duplicate",
                    "Already Exists: Job example-project:EU.csv-job",
                ),
            ),
            vec![(404, error_body("notFound", "Not found: Job")); attempts],
            "failed to submit the BigQuery CSV load job: duplicate: Already Exists: Job \
             example-project:EU.csv-job (HTTP 409)",
            Status::Unknown,
        ),
    ] {
        let (cancel_endpoint, cancels) = capture_requests_until_stopped(vec![ok(json!({}))]);
        let (runtime, _cancellation, operation) = cancellable_at(&cancel_endpoint);
        let fetches = fetched.len();
        let (jobs_endpoint, reconciliations) = capture_requests(fetched);
        let (upload_endpoint, uploads) = capture_requests(vec![upload]);

        let failure = submit(
            &runtime,
            &operation,
            &JobClient::for_test(&jobs_endpoint),
            &format!("{upload_endpoint}/upload"),
            bytes::Bytes::from_static(b"large-media").into(),
        )
        .unwrap_err();
        operation.finish();

        crate::test_support::errors::assert_failure(
            &failure,
            status,
            &[
                reported,
                "; BigQuery may still run job example-project:EU.csv-job; recovery did not \
                 complete; check the job before running it again (Query: ",
                "j=bq:EU:csv-job",
            ],
        );
        assert!(failure.details.unwrap().contains(&(
            crate::error::DETAIL_JOB_ID.to_owned(),
            b"example-project:EU.csv-job".to_vec(),
        )));
        uploads.served(1);
        reconciliations.served(fetches);
        cancels.stop();
    }
}

/// A refused upload is never replayed, and created no job, so a later cancel has nothing to
/// address: it would answer 404 and warn that the job may keep running.
#[test]
fn a_refused_upload_leaves_no_job_to_cancel() {
    for (code, reason, status) in [
        (400, "invalid", Status::InvalidArguments),
        (429, "rateLimitExceeded", Status::IO),
    ] {
        let (endpoint, server) =
            capture_requests_until_stopped(vec![(code, error_body(reason, "refused"))]);
        let (runtime, cancellation, operation) = cancellable_at(&endpoint);

        let failure = submit(
            &runtime,
            &operation,
            &JobClient::for_test(&endpoint),
            &format!("{endpoint}/upload"),
            bytes::Bytes::new().into(),
        )
        .unwrap_err();
        assert_eq!(failure.status, status);
        OperationCancelHandle(cancellation).try_cancel().unwrap();

        assert_eq!(server.stop().len(), 1, "only the upload reached the mock");
    }
}
