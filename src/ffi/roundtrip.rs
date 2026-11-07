//! End-to-end tests that drive this export layer through a real ADBC driver manager.
//!
//! Other `src/ffi` tests use the raw C structs. These load the exported entry point as a host
//! would, exercising vtable wiring, option plumbing and stream export together.

use std::collections::HashSet;
use std::ffi::{c_int, c_void};

use adbc_core::error::Status;
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow_array::Int64Array;
use arrow_schema::DataType;
use serde_json::json;

use crate::test_support::http::{capture_requests, completed_job, error_body};

use super::abi::{AdbcError, AdbcStatusCode};

#[path = "../../tests/support/project.rs"]
mod project_config;

fn load_driver() -> ManagedDriver {
    // `load_static` wants `adbc_ffi`'s spelling of the init function. Ours differs only in the
    // nominal type of the error pointer, and the two structs are the same `#[repr(C)]` layout, so
    // the pointers are interchangeable at the ABI level.
    type OurInit = unsafe extern "C" fn(c_int, *mut c_void, *mut AdbcError) -> AdbcStatusCode;
    let ours: OurInit = super::AdbcDriverBigqueryInit;
    // SAFETY: Both function types use the same C ABI, and their error types have identical repr(C)
    // layouts.
    let init: adbc_ffi::FFI_AdbcDriverInitFunc = unsafe { std::mem::transmute(ours) };
    ManagedDriver::load_static(&init, AdbcVersion::V110).unwrap()
}

/// A database pointed at an endpoint nothing listens on, authenticating anonymously: enough to
/// reach every option and metadata path through the manager without credentials or network access.
fn anonymous_database() -> impl Database {
    anonymous_database_at(crate::test_support::UNREACHABLE_ENDPOINT)
}

fn anonymous_database_at(endpoint: &str) -> impl Database {
    load_driver()
        .new_database_with_opts([
            (OptionDatabase::Uri, "bigquery:///fixture-project".into()),
            (
                crate::BigQueryDatabaseOption::AuthenticationType.into(),
                crate::BigQueryAuthenticationType::Anonymous.into(),
            ),
            (
                crate::BigQueryDatabaseOption::Endpoint.into(),
                endpoint.to_owned().into(),
            ),
        ])
        .unwrap()
}

/// The option set before ConnectionInit must reach GetObjects through the exported API, stay
/// local to that connection, and switch back to pattern matching when disabled after Init.
#[test]
fn exported_get_objects_accepts_disable_wildcards_before_and_after_init() {
    let (endpoint, requests) = capture_requests(vec![(
        400,
        error_body("invalid", "Invalid project ID supplied"),
    )]);
    let database = anonymous_database_at(&endpoint);
    let key = crate::BigQueryConnectionOption::GetObjectsDisableWildcards;
    let mut connection = database
        .new_connection_with_opts([(key.into(), "true".into())])
        .unwrap();
    assert_eq!(connection.get_option_string(key.into()).unwrap(), "true");
    assert_eq!(
        database
            .new_connection()
            .unwrap()
            .get_option_string(key.into())
            .unwrap(),
        "false"
    );
    for (enabled, expected_rows) in [("true", 0), ("false", 1)] {
        connection.set_option(key.into(), enabled.into()).unwrap();
        let batch = connection
            .get_objects(ObjectDepth::Catalogs, Some("%"), None, None, None, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), expected_rows);
    }
    assert!(
        requests.served(1)[0]
            .path()
            .starts_with("/bigquery/v2/projects/%25/datasets?")
    );
}

/// Exercise the real REST producer and C Stream importer together: a result with no batches
/// still has fields and metadata, both before and after EOF.
#[test]
fn exported_empty_rest_result_preserves_schema_through_eof() {
    let (endpoint, requests) = capture_requests(vec![completed_job(
        "empty-query",
        json!({
            "statementType": "SELECT",
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "totalRows": "0"
        }),
    )]);
    let database = anonymous_database_at(&endpoint);
    let mut statement = database.new_connection().unwrap().new_statement().unwrap();
    statement
        .set_sql_query("SELECT 42 AS answer WHERE FALSE")
        .unwrap();
    let mut reader = statement.execute().unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "answer");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(0).metadata()["BIGQUERY:type"], "INT64");
    assert_eq!(schema.metadata()["BIGQUERY:query_id"], "empty-query");
    assert_eq!(
        schema.metadata()["BIGQUERY:statistics:query:statement_type"],
        "SELECT"
    );
    assert!(reader.next().is_none());
    assert!(reader.next().is_none());
    assert_eq!(reader.schema(), schema);
    requests.served(1);
}

/// A setup error must cross the ADBC boundary as an error, with no usable result published.
/// Reusing the same statement also checks that failed setup leaves execution state usable.
#[test]
fn exported_rest_schema_failure_is_reported_and_statement_recovers() {
    let (endpoint, requests) = capture_requests(vec![
        completed_job(
            "schema-failure",
            json!({"statementType": "SELECT", "totalRows": "0"}),
        ),
        completed_job(
            "schema-recovered",
            json!({
                "statementType": "SELECT",
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "42"}]}],
                "totalRows": "1"
            }),
        ),
    ]);
    let database = anonymous_database_at(&endpoint);
    let mut statement = database.new_connection().unwrap().new_statement().unwrap();
    statement.set_sql_query("SELECT 42 AS answer").unwrap();
    let Err(failure) = statement.execute() else {
        panic!("a SELECT without a schema published a reader");
    };
    assert_eq!(failure.status, Status::InvalidData);
    assert!(
        failure.message.contains("without a result schema"),
        "{failure}"
    );

    let mut reader = statement.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[42]
    );
    assert!(reader.next().is_none());
    requests.served(2);
}

/// A valid schema does not make a failed first conversion a successful empty result. Check this
/// through the manager so the producer error must survive the Arrow C Stream callbacks too.
#[test]
fn exported_rest_first_batch_failure_is_not_eof() {
    let (endpoint, requests) = capture_requests(vec![completed_job(
        "bad-row-query",
        json!({
            "statementType": "SELECT",
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "not-an-integer"}]}],
            "totalRows": "1"
        }),
    )]);
    let database = anonymous_database_at(&endpoint);
    let mut statement = database.new_connection().unwrap().new_statement().unwrap();
    statement.set_sql_query("SELECT 42 AS answer").unwrap();
    let mut reader = statement.execute().unwrap();
    let schema = reader.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    let failure = reader
        .next()
        .expect("conversion failure must not become EOF")
        .unwrap_err();
    assert!(failure.to_string().contains("INT64"), "{failure}");
    assert_eq!(reader.schema(), schema);
    assert_eq!(schema.metadata()["BIGQUERY:query_id"], "bad-row-query");
    assert!(reader.next().is_none());
    requests.served(1);
}

/// Per-option behavior is tested natively. Here, one database option crosses the C boundary; the
/// statement's string and integer options cross it below.
#[test]
fn exported_driver_database_options_round_trip() {
    assert_eq!(
        anonymous_database()
            .get_option_string(crate::BigQueryDatabaseOption::Endpoint.into())
            .unwrap(),
        "http://127.0.0.1:1"
    );
}

/// An unknown option must surface as a driver error through the C boundary rather than a panic or
/// a bare status code with no message.
#[test]
fn exported_driver_reports_unknown_options_with_a_message() {
    let mut driver = load_driver();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, "bigquery:///example-project".into())]);
    let database = database.unwrap();
    let error = database
        .get_option_string(OptionDatabase::Other("adbc.bigquery.nope".into()))
        .unwrap_err();
    assert!(!error.message.is_empty(), "the message must survive export");
}

#[test]
fn exported_driver_option_getters_report_type_mismatches_as_not_found() {
    let database = anonymous_database();
    let mut statement = database.new_connection().unwrap().new_statement().unwrap();

    let string_key: OptionStatement = crate::BigQueryStatementOption::Priority.into();
    statement
        .set_option(
            string_key.clone(),
            crate::BigQueryQueryPriority::Batch.into(),
        )
        .unwrap();
    assert_eq!(
        statement.get_option_string(string_key.clone()).unwrap(),
        crate::BigQueryQueryPriority::Batch.as_str()
    );

    let integer_key: OptionStatement = crate::BigQueryStatementOption::MaximumBytesBilled.into();
    statement
        .set_option(integer_key.clone(), OptionValue::Int(1_000_000))
        .unwrap();
    assert_eq!(
        statement.get_option_int(integer_key.clone()).unwrap(),
        1_000_000
    );

    // Every getter whose type does not match the stored value reports NotFound.
    let string = |key| statement.get_option_string(key).unwrap_err().status;
    let bytes = |key| statement.get_option_bytes(key).unwrap_err().status;
    let int = |key| statement.get_option_int(key).unwrap_err().status;
    let double = |key| statement.get_option_double(key).unwrap_err().status;
    for (label, status) in [
        (
            "string option through get_option_bytes",
            bytes(string_key.clone()),
        ),
        (
            "string option through get_option_int",
            int(string_key.clone()),
        ),
        (
            "string option through get_option_double",
            double(string_key),
        ),
        (
            "integer option through get_option_string",
            string(integer_key.clone()),
        ),
        (
            "integer option through get_option_bytes",
            bytes(integer_key.clone()),
        ),
        (
            "integer option through get_option_double",
            double(integer_key),
        ),
    ] {
        assert_eq!(status, Status::NotFound, "{label}");
    }
}

#[test]
#[ignore = "requires Google Cloud ADC"]
fn exported_driver_cancels_connection_stream() {
    let mut driver = load_driver();
    let database = driver
        .new_database_with_opts([(
            crate::BigQueryDatabaseOption::ProjectId.into(),
            project_config::project().into(),
        )])
        .unwrap();
    let connection = database.new_connection().unwrap();
    let cancellation = connection.get_cancel_handle();
    assert_eq!(
        cancellation.try_cancel().unwrap_err().status,
        Status::InvalidState
    );

    let mut info = connection.get_info(None).unwrap();
    cancellation.try_cancel().unwrap();
    assert!(info.next().unwrap().is_ok());
    assert!(info.next().is_none());
    assert_eq!(
        cancellation.try_cancel().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn exported_driver_streams_metadata_and_query() {
    let project = project_config::project();
    let mut driver = load_driver();
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, format!("bigquery:///{project}").into())])
        .unwrap();
    let mut connection = database.new_connection().unwrap();

    let mut info = connection
        .get_info(Some(HashSet::from([
            InfoCode::VendorName,
            InfoCode::DriverName,
        ])))
        .unwrap();
    assert_eq!(info.next().unwrap().unwrap().num_rows(), 2);
    assert!(info.next().is_none());

    let mut table_types = connection.get_table_types().unwrap();
    assert_eq!(table_types.next().unwrap().unwrap().num_rows(), 5);
    assert!(table_types.next().is_none());

    let mut statistic_names = connection.get_statistic_names().unwrap();
    assert_eq!(statistic_names.next().unwrap().unwrap().num_rows(), 7);
    assert!(statistic_names.next().is_none());

    // The catalog filter matches only the connection's own project, so build the pattern from it.
    let mut objects = connection
        .get_objects(ObjectDepth::Catalogs, Some(project), None, None, None, None)
        .unwrap();
    assert_eq!(objects.next().unwrap().unwrap().num_rows(), 1);
    assert!(objects.next().is_none());

    let mut unmatched = connection
        .get_objects(
            ObjectDepth::Catalogs,
            Some(&format!("{project}-no-such-project")),
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(unmatched.next().unwrap().unwrap().num_rows(), 0);
    assert!(unmatched.next().is_none());

    let table_schema = connection
        .get_table_schema(Some("bigquery-public-data"), Some("samples"), "shakespeare")
        .unwrap();
    assert_eq!(table_schema.field(0).name(), "word");
    assert_eq!(table_schema.field(0).data_type(), &DataType::Utf8);

    connection
        .set_option(
            OptionConnection::CurrentCatalog,
            "bigquery-public-data".into(),
        )
        .unwrap();
    connection
        .set_option(OptionConnection::CurrentSchema, "samples".into())
        .unwrap();

    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("SELECT COUNT(*) AS answer FROM shakespeare")
        .unwrap();
    let mut reader = statement.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
            > 0
    );
    assert!(reader.next().is_none());

    let partitioned = statement.execute_partitions().unwrap();
    assert_eq!(partitioned.partitions.len(), 1);
    // The one-row `COUNT(*)` result's size.
    assert_eq!(partitioned.rows_affected, 1);
    let mut reader = connection
        .read_partition(&partitioned.partitions[0])
        .unwrap();
    // The partition reader's schema also carries metadata (query ID, statistics) from the job
    // that read the partition, which `execute_partitions` cannot know. Only the fields agree.
    let read_schema = reader.schema();
    assert_eq!(read_schema.fields(), partitioned.schema.fields());
    assert!(
        read_schema
            .metadata()
            .get("BIGQUERY:query_id")
            .is_some_and(|query_id| !query_id.is_empty()),
        "partition reader schema is missing a non-empty BIGQUERY:query_id"
    );
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().is_none());
}
