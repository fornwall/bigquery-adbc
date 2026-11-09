use adbc_core::Statement as _;
use adbc_core::error::Status;
use arrow_array::{Array as _, RecordBatchReader, StringArray};
use serde_json::{Value, json};

use crate::test_support::driver::{test_statement, test_statement_at};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::http::{capture_requests, ok};
use crate::test_support::options::{assert_string_option, set_option, set_option_error};
use crate::{BigQueryStatement, BigQueryStatementOption};

const OPTION: BigQueryStatementOption = BigQueryStatementOption::GetDatasetLocation;
const PAYLOAD: &str = r#"{"project":"data-project","dataset":"analytics"}"#;

/// BigQuery's responses, as captured from `datasets.get`.
fn google_error(code: u16, reason: &str, message: &str) -> Value {
    json!({"error": {
        "code": code,
        "message": message,
        "errors": [{"message": message, "domain": "global", "reason": reason}],
    }})
}

fn missing_dataset() -> Value {
    google_error(404, "notFound", "Not found: Dataset data-project:analytics")
}

/// A statement against `endpoint`, finalized as dbt 2.0 finalizes an option-only statement.
fn lookup_at(endpoint: &str) -> BigQueryStatement {
    let mut statement = test_statement_at(endpoint);
    set_option(&mut statement, OPTION, PAYLOAD);
    statement
        .set_sql_query("/* {\"app\": \"dbt\"} */\n")
        .unwrap();
    statement
}

fn locations(reader: impl RecordBatchReader) -> Vec<String> {
    assert_eq!(reader.schema().field(0).name(), "location");
    let mut locations = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.null_count(), 0);
        locations.extend(column.iter().flatten().map(str::to_owned));
    }
    locations
}

#[test]
fn an_existing_dataset_returns_one_location_row() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({"location": "us-west1"}))]);
    let mut statement = lookup_at(&endpoint);

    let (reader, rows_affected) = statement.execute_with_rows_affected().unwrap();

    assert_eq!(rows_affected, 1);
    assert_eq!(locations(reader), ["us-west1"]);
    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].line,
        "GET /bigquery/v2/projects/data-project/datasets/analytics?fields=location HTTP/1.1"
    );
}

#[test]
fn a_missing_dataset_returns_no_rows() {
    let (endpoint, captured) = capture_requests(vec![(404, missing_dataset())]);
    let mut statement = lookup_at(&endpoint);

    let (reader, rows_affected) = statement.execute_with_rows_affected().unwrap();

    assert_eq!(rows_affected, 0);
    assert!(locations(reader).is_empty());
    captured.join().unwrap();
}

/// A missing project is a 404 with the same reason as a missing dataset, but it is a configuration
/// error: reporting no rows would read as "this dataset does not exist yet".
#[test]
fn other_rejections_are_errors() {
    for (code, reason, message, status) in [
        (403, "accessDenied", "Access Denied", Status::Unauthorized),
        (
            404,
            "notFound",
            "Project data-project is not found. Make sure it references valid GCP project that \
             hasn't been deleted.; Project id: data-project",
            Status::NotFound,
        ),
        // A 404 that names neither, as a malformed path draws.
        (
            404,
            "notFound",
            "Request couldn't be served.",
            Status::NotFound,
        ),
    ] {
        let (endpoint, captured) =
            capture_requests(vec![(code, google_error(code, reason, message))]);
        let mut statement = lookup_at(&endpoint);

        let error = statement.execute_with_rows_affected().err().unwrap();

        assert_failure(
            &error,
            status,
            &[
                "failed to get the location of BigQuery dataset data-project.analytics",
                message,
            ],
        );
        captured.join().unwrap();
    }
}

#[test]
fn malformed_payloads_are_rejected_when_set_without_echoing_them() {
    const SECRET: &str = "get-location-secret-sentinel";
    let mut statement = test_statement();
    set_option(&mut statement, OPTION, PAYLOAD);

    for (name, value, expected) in [
        ("not JSON", format!("{{{SECRET}"), "must be valid JSON"),
        ("null", "null".to_owned(), "must be a JSON object"),
        (
            "not an object",
            json!([SECRET]).to_string(),
            "must be a JSON object",
        ),
        ("empty object", "{}".to_owned(), "non-empty project"),
        (
            "missing project",
            json!({"dataset": SECRET}).to_string(),
            "non-empty project",
        ),
        (
            "empty project",
            json!({"project": "", "dataset": SECRET}).to_string(),
            "non-empty project",
        ),
        (
            "non-string project",
            json!({"project": 1, "dataset": SECRET}).to_string(),
            "non-empty project",
        ),
        (
            "missing dataset",
            json!({"project": SECRET}).to_string(),
            "non-empty dataset",
        ),
        (
            "empty dataset",
            json!({"project": SECRET, "dataset": ""}).to_string(),
            "non-empty dataset",
        ),
        (
            "project outside one path segment",
            json!({"project": format!("a/{SECRET}"), "dataset": "d"}).to_string(),
            "cannot contain slashes",
        ),
        (
            "dot-segment dataset",
            json!({"project": SECRET, "dataset": ".."}).to_string(),
            "cannot be",
        ),
    ] {
        let error = set_option_error(&mut statement, OPTION, value);
        assert_eq!(error.status, Status::InvalidArguments, "{name}");
        assert_message(&error, &["bigquery.dataset.get_location", expected]);
        assert_message_omits(&error, &[SECRET]);
        // A rejected value leaves the configured lookup in place.
        assert_string_option(&statement, OPTION, Some(PAYLOAD));
    }
}

/// BigQuery answers `Not found: Dataset` for an ID it could never hold, so the lookup leaves that
/// verdict to it instead of holding the name to BigQuery's naming rules.
#[test]
fn a_dataset_id_outside_bigquerys_naming_rules_is_looked_up() {
    let (endpoint, captured) = capture_requests(vec![(404, missing_dataset())]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        OPTION,
        r#"{"project":"data-project","dataset":"not/a-dataset"}"#,
    );

    let (reader, rows_affected) = statement.execute_with_rows_affected().unwrap();

    assert_eq!(rows_affected, 0);
    assert!(locations(reader).is_empty());
    let requests = captured.join().unwrap();
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/data-project/datasets/not%2Fa-dataset?fields=location"
    );
}

#[test]
fn the_payload_reads_back_as_set_and_clearing_it_leaves_no_query() {
    let mut statement = test_statement();
    assert_string_option(&statement, OPTION, None);

    // Key order and unknown keys survive, as the caller wrote them.
    let payload = r#"{"dataset":"d", "project":"p", "note":1}"#;
    set_option(&mut statement, OPTION, payload);
    assert_string_option(&statement, OPTION, Some(payload));

    set_option(&mut statement, OPTION, "");
    assert_string_option(&statement, OPTION, None);
    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["without a SQL query"]);
}

#[test]
fn execute_update_reads_the_location_and_reports_an_unknown_count_as_a_query_does() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({"location": "EU"}))]);
    let mut statement = lookup_at(&endpoint);

    assert_eq!(statement.execute_update().unwrap(), None);
    assert_eq!(captured.join().unwrap().len(), 1);
}

#[test]
fn execute_schema_describes_the_result_without_a_request() {
    let mut statement = test_statement();
    set_option(&mut statement, OPTION, PAYLOAD);

    let schema = statement.execute_schema().unwrap();

    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "location");
    assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Utf8);
}

/// The lookup is the only option-only operation with rows to describe.
#[test]
fn other_operations_have_no_result_schema() {
    for (option, value) in [
        (BigQueryStatementOption::WriteGcsBucket, "dbt-python"),
        (
            BigQueryStatementOption::AuthorizeViewToDatasets,
            r#"{"data-project.analytics.model":[{"project":"data-project","dataset":"raw"}]}"#,
        ),
        (
            BigQueryStatementOption::CopyTableSource,
            "data-project.analytics.model",
        ),
    ] {
        let mut statement = test_statement();
        set_option(&mut statement, option, value);
        assert_eq!(
            statement.execute_schema().unwrap_err().status,
            Status::InvalidState,
            "{}",
            option.as_str()
        );
    }
}

#[test]
fn execute_partitions_is_refused_before_any_request() {
    let mut statement = test_statement();
    set_option(&mut statement, OPTION, PAYLOAD);

    let error = statement.execute_partitions().unwrap_err();

    assert_failure(
        &error,
        Status::InvalidState,
        &["creates no job to partition", OPTION.as_str()],
    );
}

#[test]
fn a_pending_table_description_update_is_refused() {
    let mut statement = test_statement();
    set_option(&mut statement, OPTION, PAYLOAD);
    set_option(
        &mut statement,
        BigQueryStatementOption::UpdateDescription,
        "a description",
    );

    let error = statement.execute_with_rows_affected().err().unwrap();

    assert_failure(
        &error,
        Status::InvalidState,
        &["cannot be combined", OPTION.as_str()],
    );
}

#[test]
fn the_lookup_replaces_and_is_replaced_by_other_operations() {
    let copy_source = BigQueryStatementOption::CopyTableSource;
    let mut statement = test_statement();

    set_option(&mut statement, copy_source, "data-project.analytics.source");
    set_option(&mut statement, OPTION, PAYLOAD);
    assert_string_option(&statement, copy_source, None);

    set_option(&mut statement, copy_source, "data-project.analytics.source");
    assert_string_option(&statement, OPTION, None);
}
