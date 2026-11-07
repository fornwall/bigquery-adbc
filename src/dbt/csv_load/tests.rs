use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::CancelHandle as _;
use adbc_core::error::Status;
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;

use super::{WHAT, build_load_request, submit_csv_load};
use crate::cancellation::Operation;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::dbt::jobs::{LoadOrCopyJob, decode_job, validate_completed};
use crate::query::{QualifiedTableName, StatementQueryOptions};
use crate::rest::client::rest_client;
use crate::rest::jobs::{JobClient, JobReference};
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::rest::request::{Request, RequestBody, multipart_related, multipart_related_file};
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::errors::assert_message;
use crate::test_support::http::{
    capture_requests, capture_requests_gated, capture_requests_until_stopped, error_body,
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

fn reference(location: Option<&str>) -> JobReference {
    JobReference::new("example-project", location, "csv-job".to_owned())
}

fn operation_at(endpoint: &str) -> (SharedRuntime, Operation) {
    let runtime = new_runtime().unwrap();
    let cancellation = Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, endpoint),
        "statement",
    );
    let operation = cancellation.begin();
    (runtime, operation)
}

fn submit_at(endpoint: &str) -> (SharedRuntime, Operation, JobClient) {
    let (runtime, operation) = operation_at(endpoint);
    let client = JobClient::for_test(endpoint);
    (runtime, operation, client)
}

/// Sends one media upload through `submit_csv_load`, as `execute_csv_load` does.
fn submit(
    runtime: &SharedRuntime,
    operation: &Operation,
    jobs: &JobClient,
    url: &str,
    body: RequestBody,
    requested: &JobReference,
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
        requested,
    ))
}

fn seed_file(name: &str, contents: &[u8]) -> (std::path::PathBuf, std::fs::File, u64) {
    let path = std::env::temp_dir().join(format!(
        "bigquery-adbc-{name}-{}-{}.csv",
        std::process::id(),
        contents.len()
    ));
    std::fs::write(&path, contents).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    (path, file, length)
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
    let request = build_load_request(
        &options,
        &destination(),
        &schema,
        ";",
        reference(Some("EU")),
    )
    .unwrap();
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
        (
            200,
            json!({
                "jobReference":{"projectId":"example-project","jobId":"csv-job","location":"EU"},
                "status":{"state":"RUNNING"}
            }),
        ),
        (
            404,
            json!({"error":{"errors":[{"reason":"notFound","message":"not visible yet"}]}}),
        ),
        (
            200,
            json!({
                "jobReference":{"projectId":"example-project","jobId":"csv-job","location":"EU"},
                "status":{"state":"DONE"},
                "statistics":{"load":{"outputRows":"42"}}
            }),
        ),
    ]);
    let requested = reference(Some("EU"));
    let (runtime, operation, jobs) = submit_at(&endpoint);
    let url =
        format!("{endpoint}/upload/bigquery/v2/projects/example-project/jobs?uploadType=multipart");
    let metadata = br#"{"configuration":{"load":{"sourceFormat":"CSV"}}}"#;
    let csv = b"id,label\r\n1,\"exact \x00 bytes\nwith LF\r\nwith CRLF\"\r\n2,final\n";
    let (path, file, csv_length) = seed_file("streamed-multipart", csv);
    let body = multipart_related_file("test", metadata, file, csv_length, "text/csv").unwrap();
    assert!(body.is_streaming(), "the complete request is not buffered");
    let expected = multipart_related("test", metadata, csv, "text/csv");

    let rows = submit(&runtime, &operation, &jobs, &url, body, &requested)
        .unwrap()
        .output_rows();
    operation.finish();
    assert_eq!(rows, Some(42));

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 3);
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
        (
            200,
            json!({
                "jobReference":{"projectId":"example-project","jobId":"csv-job","location":"EU"},
                "status":{"state":"DONE"},
                "statistics":{"load":{"outputRows":7}}
            }),
        ),
    ]);
    let requested = reference(Some("EU"));
    let (runtime, operation, jobs) = submit_at(&endpoint);
    let url =
        format!("{endpoint}/upload/bigquery/v2/projects/example-project/jobs?uploadType=multipart");

    assert_eq!(
        submit(
            &runtime,
            &operation,
            &jobs,
            &url,
            bytes::Bytes::from_static(b"large-media").into(),
            &requested
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
    let (cancel_endpoint, cancels) = capture_requests(vec![(200, json!({}))]);
    let runtime = new_runtime().unwrap();
    let cancellation = Cancellation::new(
        runtime.clone(),
        ClientState::for_test_at(ServiceEndpointKind::BigQuery, &cancel_endpoint),
        "statement",
    );
    let operation = cancellation.begin();
    let handle = OperationCancelHandle(cancellation);
    let (jobs_endpoint, reconciliations) = capture_requests_gated(
        vec![(
            200,
            json!({
                "jobReference":{"projectId":"example-project","jobId":"csv-job","location":"EU"},
                "status":{"state":"DONE"}
            }),
        )],
        move || {
            handle.try_cancel().unwrap();
            true
        },
    );
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
        &reference(Some("EU")),
    )
    .unwrap_err();
    operation.finish();

    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(uploads.join().unwrap().len(), 1);
    assert_eq!(reconciliations.join().unwrap().len(), 1);
    assert_eq!(cancels.join().unwrap().len(), 1);
}

#[test]
fn partial_success_response_is_reconciled_as_an_accepted_upload() {
    let (endpoint, captured) = capture_requests(vec![
        (200, json!({"status":{"state":"DONE"}})),
        (
            200,
            json!({
                "jobReference":{"projectId":"example-project","jobId":"csv-job","location":"EU"},
                "status":{"state":"DONE"},
                "statistics":{"load":{"outputRows":"9"}}
            }),
        ),
    ]);
    let requested = reference(Some("EU"));
    let (runtime, operation, jobs) = submit_at(&endpoint);

    assert_eq!(
        submit(
            &runtime,
            &operation,
            &jobs,
            &format!("{endpoint}/upload"),
            bytes::Bytes::from_static(b"large-media").into(),
            &requested
        )
        .unwrap()
        .output_rows(),
        Some(9)
    );
    operation.finish();
    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 2);
    assert!(captured[0].line.starts_with("POST /upload "));
    assert!(
        captured[1]
            .line
            .starts_with("GET /bigquery/v2/projects/example-project/jobs/csv-job?location=EU ")
    );
}

#[test]
fn nonretryable_errors_never_replay_the_upload() {
    let (endpoint, captured) = capture_requests(vec![(
        400,
        json!({"error":{"errors":[{"reason":"invalid","message":"bad load"}]}}),
    )]);
    let requested = reference(Some("EU"));
    let (runtime, operation, jobs) = submit_at(&endpoint);
    let failure = submit(
        &runtime,
        &operation,
        &jobs,
        &format!("{endpoint}/upload"),
        bytes::Bytes::new().into(),
        &requested,
    )
    .unwrap_err();
    operation.finish();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(captured.join().unwrap().len(), 1);
}

/// A refused upload created no job, so a later cancel has nothing to address: it would answer 404
/// and warn that the job may keep running.
#[test]
fn a_refused_upload_leaves_no_job_to_cancel() {
    for (status, reason) in [(400, "invalid"), (429, "rateLimitExceeded")] {
        let (endpoint, server) =
            capture_requests_until_stopped(vec![(status, error_body(reason, "refused"))]);
        let runtime = new_runtime().unwrap();
        let cancellation = Cancellation::new(
            runtime.clone(),
            ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint),
            "statement",
        );
        let operation = cancellation.begin();

        submit(
            &runtime,
            &operation,
            &JobClient::for_test(&endpoint),
            &format!("{endpoint}/upload"),
            bytes::Bytes::new().into(),
            &reference(Some("EU")),
        )
        .unwrap_err();
        OperationCancelHandle(cancellation).try_cancel().unwrap();

        assert_eq!(server.stop().len(), 1, "only the upload reached the mock");
    }
}

/// A load reports the rows it wrote, or ADBC's unknown count when BigQuery reports none. The
/// shared status and identity checks are covered in `src/dbt/jobs/tests.rs`.
#[test]
fn completion_reports_written_rows_and_unknown_counts() {
    let completed = |body: &str| {
        let mut job = decode_job(body.as_bytes(), WHAT).unwrap();
        validate_completed(&mut job, WHAT).map(|()| job.output_rows())
    };
    assert_eq!(
        completed(r#"{"status":{"state":"DONE"},"statistics":{"load":{"outputRows":"9"}}}"#)
            .unwrap(),
        Some(9)
    );
    assert_eq!(
        completed(r#"{"status":{"state":"DONE"},"statistics":{"load":{}}}"#).unwrap(),
        None
    );

    let failure = completed(
        r#"{"status":{"state":"DONE","errorResult":{"reason":"invalid","message":"CSV row 2 is malformed"}}}"#,
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_message(&failure, &["CSV row 2 is malformed"]);
}

/// The dbt-core 2.0 seed end to end: the load job's ID is reported the way a query result reports
/// its job, which dbt-core 2.0 records as the statement's query ID; a dry run submits no job and
/// reports none.
#[test]
fn a_dbt_seed_reports_its_load_job_as_the_query_id() {
    use adbc_core::options::OptionValue;
    use adbc_core::{Connection as _, Statement as _};

    use crate::BigQueryStatementOption;
    use crate::test_support::driver::test_connection_at;
    use crate::test_support::http::capture_requests_responding;
    use crate::test_support::options::set_option;

    // The upload is multipart, so the caller-generated identity is echoed from its raw metadata.
    let (endpoint, captured) = capture_requests_responding(1, |request| {
        let metadata = String::from_utf8_lossy(&request.raw_body);
        let start = metadata.find(r#""jobReference":"#)? + r#""jobReference":"#.len();
        let end = start + metadata[start..].find('}')? + 1;
        let reference: serde_json::Value = serde_json::from_str(&metadata[start..end]).ok()?;
        Some((
            200,
            json!({
                "jobReference": reference,
                "status": {"state": "DONE"},
                "statistics": {"load": {"outputRows": "1"}}
            }),
        ))
    });
    let (path, _, _) = seed_file("query-id", b"id\n1\n");
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let mut connection = test_connection_at(&endpoint);
    let mut statement = connection.new_statement().unwrap();
    for (option, value) in [
        (
            BigQueryStatementOption::CsvFilepath,
            OptionValue::String(path.to_str().unwrap().to_owned()),
        ),
        (
            BigQueryStatementOption::CsvSchema,
            OptionValue::Bytes(crate::arrow_ipc_bigquery::encode_schema_fixture(&schema)),
        ),
        (
            BigQueryStatementOption::DestinationTable,
            "data_set.seed".into(),
        ),
    ] {
        set_option(&mut statement, option, value);
    }
    statement.set_sql_query("").unwrap();

    let loaded = statement.execute_partitions().unwrap();
    assert_eq!(loaded.rows_affected, 1);
    let captured = captured.join().unwrap();
    let metadata = String::from_utf8_lossy(&captured[0].raw_body);
    let job_id = loaded.schema.metadata()[crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID].as_str();
    assert!(job_id.starts_with("adbc_csv_"), "{job_id}");
    assert!(
        metadata.contains(&format!(r#""jobId":"{job_id}""#)),
        "the reported ID is the submitted job's: {metadata}"
    );

    set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
    assert!(
        statement
            .execute_partitions()
            .unwrap()
            .schema
            .metadata()
            .is_empty()
    );
    std::fs::remove_file(path).unwrap();
}
