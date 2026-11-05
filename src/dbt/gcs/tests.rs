use adbc_core::error::Status;
use adbc_core::options::OptionStatement;
use adbc_core::{Connection as _, Statement as _};
use serde_json::json;

use crate::rest::url::escape_url_component as escape;
use crate::test_support::driver::{test_connection_with, test_statement};
use crate::test_support::errors::assert_message;
use crate::test_support::http::capture_requests;
use crate::test_support::options::set_option;
use crate::{BigQueryDatabaseOption, BigQueryStatementOption};

/// A test connection whose Cloud Storage calls go to `endpoint`. BigQuery points at a closed
/// port, so a stray BigQuery request fails rather than reaching the mock.
fn connection_writing_objects_to(endpoint: String) -> crate::BigQueryConnection {
    test_connection_with(
        "http://127.0.0.1:1",
        [(BigQueryDatabaseOption::GcsEndpoint.into(), endpoint.into())],
    )
}

/// The object write dbt-core 2.0 configures for a python model: the bucket, the object name, and
/// the source when it is non-empty. dbt finalizes the statement with a real, executable query,
/// which must not run.
fn write_statement(
    connection: &mut crate::BigQueryConnection,
    object_name: &str,
    content: Option<&str>,
) -> crate::BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    let mut options = vec![
        (BigQueryStatementOption::WriteGcsBucket, "dbt-python"),
        (BigQueryStatementOption::WriteGcsObjectName, object_name),
    ];
    options.extend(content.map(|content| (BigQueryStatementOption::WriteGcsContent, content)));
    for (option, value) in options {
        set_option(&mut statement, option, value.to_owned());
    }
    statement.set_sql_query("SELECT 1").unwrap();
    statement
}

/// The dbt-core 2.0 contract end to end: three options, the `SELECT 1` placeholder, one multipart
/// object insert, and no BigQuery query at all.
#[test]
fn dbt_write_gcs_uploads_the_model_source_in_one_multipart_insert() {
    const CONTENT: &str = "def model(dbt, session):\n    return session.table(\"raw\")\n";
    let (endpoint, captured) = capture_requests(vec![(
        200,
        json!({"kind": "storage#object", "bucket": "dbt-python", "name": "analytics/m.py"}),
    )]);
    let mut connection = connection_writing_objects_to(endpoint);
    let mut statement = write_statement(&mut connection, "analytics/my model.py", Some(CONTENT));

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(
        rows_affected, -1,
        "an object write affects no rows, which ADBC spells as unknown rather than zero"
    );
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 1, "one upload, and no BigQuery query");
    let request = &requests[0];
    assert!(
        request
            .line
            .starts_with("POST /upload/storage/v1/b/dbt-python/o?"),
        "{}",
        request.line
    );
    assert!(request.line.ends_with(" HTTP/1.1"), "{}", request.line);
    assert_eq!(
        request.path(),
        "/upload/storage/v1/b/dbt-python/o?uploadType=multipart\
         &name=analytics%2Fmy%20model.py&projection=full&alt=json&prettyPrint=false",
        "the object name is percent-encoded into the query string"
    );

    let content_type = request.header("content-type").unwrap();
    let boundary = content_type
        .strip_prefix("multipart/related; boundary=")
        .unwrap_or_else(|| panic!("unexpected content type {content_type}"));
    let body = std::str::from_utf8(&request.raw_body).unwrap();
    let Some(inner) = body
        .strip_prefix(&format!("--{boundary}\r\n"))
        .and_then(|body| body.strip_suffix(&format!("\r\n--{boundary}--\r\n")))
    else {
        panic!("body is not one multipart/related document: {body}")
    };
    let parts = inner
        .split(&format!("\r\n--{boundary}\r\n"))
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 2, "{body}");

    let (headers, metadata) = parts[0].split_once("\r\n\r\n").unwrap();
    assert_eq!(headers, "Content-Type: application/json; charset=UTF-8");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(metadata).unwrap(),
        json!({"bucket": "dbt-python", "name": "analytics/my model.py"}),
        "the metadata part carries the object name unescaped"
    );

    let (headers, media) = parts[1].split_once("\r\n\r\n").unwrap();
    assert_eq!(headers, "Content-Type: text/plain; charset=utf-8");
    assert_eq!(media, CONTENT, "the model source is uploaded verbatim");
}

/// A half-configured write stays active so it names the option it is missing, rather than
/// falling through to the SQL path and complaining about a missing query.
#[test]
fn a_half_configured_write_names_the_option_it_is_missing() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteGcsContent,
        "print(1)",
    );
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["adbc.bigquery.write_gcs.bucket"]);

    set_option(
        &mut statement,
        BigQueryStatementOption::WriteGcsBucket,
        "dbt-python",
    );
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["adbc.bigquery.write_gcs.object_name"]);
}

/// Cloud Storage's own status survives rather than being flattened the way the fork flattens
/// every python-model failure to UNKNOWN.
#[test]
fn a_failed_upload_reports_the_service_status_over_the_wire() {
    let (endpoint, captured) = capture_requests(vec![(
        403,
        json!({"error": {"code": 403, "message": "storage.objects.create denied"}}),
    )]);
    let mut connection = connection_writing_objects_to(endpoint);
    let mut statement = write_statement(&mut connection, "a/b.py", None);
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Unauthorized);
    assert_message(&error, &["storage.objects.create denied"]);
    assert_eq!(captured.join().unwrap().len(), 1);
}

/// An unset content option writes the empty object rather than refusing, which is what a
/// dbt model compiling to nothing produces.
#[test]
fn an_unset_content_option_uploads_an_empty_object() {
    let (endpoint, captured) = capture_requests(vec![(200, json!({"kind": "storage#object"}))]);
    let mut connection = connection_writing_objects_to(endpoint);
    let mut statement = write_statement(&mut connection, "a/b.py", None);
    assert_eq!(statement.execute_update().unwrap(), None);

    let requests = captured.join().unwrap();
    let body = std::str::from_utf8(&requests[0].raw_body).unwrap();
    // The boundary carries a process-unique ID rather than a fixed suffix: that is what
    // lets the body be assembled without first scanning the media for a collision.
    let boundary = body
        .strip_prefix("--")
        .and_then(|body| body.split_once("\r\n"))
        .map(|(boundary, _)| boundary)
        .expect("a multipart body opens with its boundary");
    assert!(boundary.starts_with("adbc_gcs_"), "{boundary}");
    assert!(
        body.ends_with(&format!(
            "Content-Type: text/plain; charset=utf-8\r\n\r\n\r\n--{boundary}--\r\n"
        )),
        "{body}"
    );
}

#[test]
fn a_write_has_no_result_schema() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.write_gcs.bucket".to_owned()),
        "dbt-python",
    );
    assert_eq!(
        statement.execute_schema().unwrap_err().status,
        Status::InvalidState,
        "an object write produces no rows to describe"
    );
}

#[test]
fn object_names_are_escaped_into_the_query_string() {
    assert_eq!(
        escape("analytics/my model.py").to_string(),
        "analytics%2Fmy%20model.py",
        "an object name is one opaque key, so its separators are escaped too"
    );
    assert_eq!(escape("plain-name_1.py").to_string(), "plain-name_1.py");
}
