//! What a result schema reports about the job that produced it, and when the driver spends its
//! one `jobs.get` to find out.

use super::*;
use crate::test_support::driver::test_statement_at;
use crate::test_support::http::job_reference;
use crate::{
    BIGQUERY_SCHEMA_METADATA_QUERY_ID, BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
};

/// dbt-core 2.0 reads `BIGQUERY:query_id` from the result schema for telemetry, and parses the
/// legacy `Type` field metadata; a dropped, undrained reader must also clean up quietly.
#[test]
fn result_schema_reports_the_query_job_id_and_field_metadata() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobComplete": true,
            "jobReference": job_reference("fixture-project", "job_dbt_123"),
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "totalBytesBilled": "0",
            "totalBytesProcessed": "0",
            "totalSlotMs": "1",
            "totalRows": "0"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let reader = statement.execute().unwrap();
    let schema = reader.schema();
    assert_eq!(
        schema
            .metadata()
            .get("BIGQUERY:query_id")
            .map(String::as_str),
        Some("job_dbt_123")
    );
    let field = schema.field(0);
    assert_eq!(field.data_type(), &DataType::Int64);
    assert_eq!(
        field.metadata().get("BIGQUERY:type").map(String::as_str),
        Some("INT64")
    );
    assert_eq!(
        field.metadata().get("Type").map(String::as_str),
        Some("INTEGER"),
        "the legacy REST spelling dbt-core 2.0 parses"
    );
    // Dropped without draining, the way dbt-core 2.0 abandons fetch=false readers.
    drop(reader);
    requests.join().unwrap();
}

/// The durable job ID identifies the result even if the query response also carries a query ID.
/// The job resource supplies the statement type omitted from the query response.
#[test]
fn result_schema_reports_the_durable_job_id_instead_of_the_query_id() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_abc"),
                "queryId": "query_abc",
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalRows": "0"
            }),
        ),
        (
            200,
            job_resource("job_abc", &json!({"statementType": "SELECT"})),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(
        schema_metadata(&reader.schema(), BIGQUERY_SCHEMA_METADATA_QUERY_ID),
        Some("job_abc")
    );
    assert_eq!(
        schema_metadata(&reader.schema(), BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
        Some("SELECT")
    );
    assert!(reader.next().is_none(), "an empty result stays empty");
    let requests = requests.join().unwrap();
    assert_eq!(
        job_gets(&requests),
        1,
        "the job supplies its statement type"
    );
    assert_eq!(requests.len(), 2, "the query and its metadata fetch");
}

#[test]
fn a_jobless_result_reports_its_query_id_and_statistics_without_follow_up_requests() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobComplete": true,
            "queryId": "query_inline",
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "42"}]}],
            "totalRows": "1",
            "statementType": "SELECT",
            "totalSlotMs": "1",
            "totalBytesProcessed": "0",
            "totalBytesBilled": "0"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 42 AS answer").unwrap();
    let mut reader = statement.execute().unwrap();
    let schema = reader.schema();
    for (key, value) in [
        (BIGQUERY_SCHEMA_METADATA_QUERY_ID, "query_inline"),
        (BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, "SELECT"),
        (BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS, "1"),
        (BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED, "0"),
        ("BIGQUERY:Statistics:Query:StatementType", "SELECT"),
    ] {
        assert_eq!(schema_metadata(&schema, key), Some(value));
    }
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert!(reader.next().is_none());
    drop(reader);
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// dbt-core 2.0 reads the job's statistics off the same schema, as decimal strings.
///
/// `statementType` is part of the response schema even though BigQuery generally omits it.
/// Pin the mapping, and avoid a redundant job fetch if the service starts filling it in.
#[test]
fn result_schema_reports_the_job_statistics() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobComplete": true,
            "jobReference": job_reference("fixture-project", "job_stats_1"),
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "statementType": "SELECT",
            "totalBytesBilled": "10485760",
            "totalBytesProcessed": "1536",
            "totalSlotMs": "7167",
            "totalRows": "0"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:Statistics:Query:SlotMillis", "7167"),
            ("BIGQUERY:Statistics:Query:StatementType", "SELECT"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "10485760"),
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "1536"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "1536"),
            ("BIGQUERY:query_id", "job_stats_1"),
            ("BIGQUERY:statistics:query:slot_millis", "7167"),
            ("BIGQUERY:statistics:query:statement_type", "SELECT"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10485760"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "1536"),
            ("BIGQUERY:statistics:total_bytes_processed", "1536"),
        ],
        "every statistic BigQuery reported, and no DML row count for a SELECT"
    );
    drop(reader);
    assert_eq!(job_gets(&requests.join().unwrap()), 0);
}

/// A DML statement returns no rows, so its job is fetched for the statement type, without which
/// dbt-core 2.0 never reads the row count.
///
/// The `jobs.query` response is the one BigQuery sends for an `UPDATE`: the target
/// table's schema, the affected-row count, the cost statistics, no `totalRows`, and no
/// `statementType`, which only the job resource carries.
#[test]
fn dml_result_schema_reports_the_affected_row_count() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_stats_2"),
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "numDmlAffectedRows": "3",
                "totalBytesBilled": "10485760",
                "totalBytesProcessed": "2048",
                "totalSlotMs": "4282"
            }),
        ),
        (
            200,
            job_resource(
                "job_stats_2",
                &json!({
                    "statementType": "MERGE",
                    "numDmlAffectedRows": "3",
                    "totalBytesBilled": "10485760",
                    "totalBytesProcessed": "2048",
                    "totalSlotMs": "4282"
                }),
            ),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("MERGE t USING s ON FALSE WHEN NOT MATCHED THEN INSERT ROW")
        .unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:Statistics:Query:NumDMLAffectedRows", "3"),
            ("BIGQUERY:Statistics:Query:SlotMillis", "4282"),
            ("BIGQUERY:Statistics:Query:StatementType", "MERGE"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "10485760"),
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "2048"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "2048"),
            ("BIGQUERY:query_id", "job_stats_2"),
            ("BIGQUERY:statistics:query:num_dml_affected_rows", "3"),
            ("BIGQUERY:statistics:query:slot_millis", "4282"),
            ("BIGQUERY:statistics:query:statement_type", "MERGE"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10485760"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "2048"),
            ("BIGQUERY:statistics:total_bytes_processed", "2048"),
        ],
        "the response's statistics, plus the statement type only the job reports"
    );
    drop(reader);
    assert_eq!(job_gets(&requests.join().unwrap()), 1);
}

/// A query that completed inline still fetches its job when the response omitted the type.
#[test]
fn a_complete_row_returning_result_fetches_the_job_for_its_statement_type() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_stats_3"),
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalBytesBilled": "0",
                "totalBytesProcessed": "0",
                "totalSlotMs": "11",
                "totalRows": "0"
            }),
        ),
        (
            200,
            job_resource(
                "job_stats_3",
                &json!({
                    "statementType": "SELECT",
                    "totalBytesBilled": "0",
                    "totalBytesProcessed": "0",
                    "totalSlotMs": "11"
                }),
            ),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        schema_metadata(&reader.schema(), BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
        Some("SELECT")
    );
    drop(reader);
    let requests = requests.join().unwrap();
    assert_eq!(job_gets(&requests), 1);
    assert_eq!(requests.len(), 2, "the query and its one metadata fetch");
}

/// A polled query is completed by `jobs.getQueryResults`, whose response reports neither the
/// bytes billed nor the slot milliseconds. The job fills them in, and the statement type too.
#[test]
fn a_polled_result_fetches_the_job_for_the_missing_cost_statistics() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_stats_4"),
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalBytesProcessed": "0",
                "totalRows": "0"
            }),
        ),
        (
            200,
            job_resource(
                "job_stats_4",
                &json!({
                    "statementType": "SELECT",
                    "totalBytesBilled": "10485760",
                    "totalBytesProcessed": "1536",
                    "totalSlotMs": "7167"
                }),
            ),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:Statistics:Query:SlotMillis", "7167"),
            ("BIGQUERY:Statistics:Query:StatementType", "SELECT"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "10485760"),
            // The completed job is authoritative over an earlier response snapshot.
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "1536"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "1536"),
            ("BIGQUERY:query_id", "job_stats_4"),
            ("BIGQUERY:statistics:query:slot_millis", "7167"),
            ("BIGQUERY:statistics:query:statement_type", "SELECT"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10485760"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "1536"),
            ("BIGQUERY:statistics:total_bytes_processed", "1536"),
        ]
    );
    drop(reader);
    assert_eq!(job_gets(&requests.join().unwrap()), 1);
}

/// A complete job-backed response that named its statement type is authoritative, and BigQuery
/// omits a statistic whose value is zero. An absent `totalSlotMs` costs no `jobs.get`, and no
/// slot time is reported rather than an invented zero.
#[test]
fn an_omitted_cost_statistic_is_left_unreported_rather_than_described_again() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobComplete": true,
            "jobReference": job_reference("fixture-project", "job_sparse_costs"),
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "statementType": "SELECT",
            "totalBytesBilled": "10",
            "totalBytesProcessed": "1",
            "totalRows": "0"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();

    let reader = statement.execute().unwrap();
    assert_eq!(
        job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:Statistics:Query:StatementType", "SELECT"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "10"),
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "1"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "1"),
            ("BIGQUERY:query_id", "job_sparse_costs"),
            ("BIGQUERY:statistics:query:statement_type", "SELECT"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "1"),
            ("BIGQUERY:statistics:total_bytes_processed", "1"),
        ]
    );
    drop(reader);
    let requests = requests.join().unwrap();
    assert_eq!(job_gets(&requests), 0);
    assert_eq!(requests.len(), 1);
}

/// An incomplete jobs.query response is only a snapshot. The completed result is described once
/// to obtain statistics that neither the initial response nor getQueryResults supplied.
#[test]
fn an_incomplete_jobs_query_fetches_final_job_metadata() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": false,
                "jobReference": job_reference("fixture-project", "job_query_running")
            }),
        ),
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_query_running"),
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "totalBytesProcessed": "1536",
                "totalRows": "0"
            }),
        ),
        (
            200,
            job_resource(
                "job_query_running",
                &json!({
                    "statementType": "SELECT",
                    "totalBytesBilled": "10485760",
                    "totalBytesProcessed": "1536",
                    "totalSlotMs": "7167"
                }),
            ),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();

    let reader = statement.execute().unwrap();
    assert_eq!(
        schema_metadata(
            &reader.schema(),
            BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED
        ),
        Some("10485760"),
        "the final job must replace the partial insert response"
    );
    assert_eq!(
        schema_metadata(&reader.schema(), BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS),
        Some("7167")
    );
    drop(reader);

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(job_gets(&requests), 1);
}

/// A result large enough for Storage Read needs the job to resolve its destination table, and
/// the statistics come from that same single fetch.
///
/// The job here has no destination table, so the path gives up and the rows are read over
/// REST. What it harvested is on the schema regardless.
#[test]
fn a_storage_read_result_fetches_the_job_exactly_once() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_stats_5"),
                "schema": {"fields": [{"name": "n", "type": "INTEGER"}]},
                "totalBytesBilled": "10485760",
                "totalBytesProcessed": "1536",
                "totalSlotMs": "7167",
                "totalRows": "60000"
            }),
        ),
        (
            200,
            job_resource("job_stats_5", &json!({"statementType": "SELECT"})),
        ),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement.set_sql_query("SELECT n FROM t").unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        schema_metadata(&reader.schema(), BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
        Some("SELECT")
    );
    drop(reader);
    assert_eq!(
        job_gets(&requests.join().unwrap()),
        1,
        "the Storage Read path must read the job the caller already fetched"
    );
}

/// A polled result whose `jobs.get` fails still reads: the fetch is metadata-only.
///
/// The statement has already completed by the time the job is described, so a failure there
/// may not fail it. The statistics the response never carried are simply absent.
#[test]
fn a_failed_job_fetch_leaves_the_result_readable_without_the_job_statistics() {
    for (status, body) in [
        (500, backend_error()),
        (
            404,
            json!({"error": {"code": 404, "message": "job not found", "status": "NOT_FOUND"}}),
        ),
    ] {
        let (endpoint, requests) = capture_requests(vec![
            // A polled result: no bytes billed and no slot milliseconds to be had from the
            // response, so the job is worth fetching.
            (
                200,
                json!({
                    "jobComplete": true,
                    "jobReference": job_reference("fixture-project", "job_fetch_fails"),
                    "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                    "totalBytesProcessed": "1536",
                    "totalRows": "0"
                }),
            ),
            (status, body),
        ]);
        let mut statement = test_statement_at(&endpoint);
        statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
        let mut reader = statement
            .execute()
            .unwrap_or_else(|error| panic!("a {status} on jobs.get failed the query: {error}"));
        assert_eq!(
            job_metadata(reader.schema().as_ref()),
            vec![
                ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "1536"),
                ("BIGQUERY:Statistics:TotalBytesProcessed", "1536"),
                ("BIGQUERY:query_id", "job_fetch_fails"),
                ("BIGQUERY:statistics:query:total_bytes_processed", "1536"),
                ("BIGQUERY:statistics:total_bytes_processed", "1536"),
            ],
            "only what the query response itself reported"
        );
        assert!(reader.next().is_none(), "the result still reads");
        drop(reader);
        assert_eq!(job_gets(&requests.join().unwrap()), 1);
    }
}

/// A DML statement has committed by the time its job is described, so a failed `jobs.get` must
/// not turn a successful `MERGE` into an error a caller would retry.
#[test]
fn a_dml_result_survives_a_failed_job_fetch() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", "job_dml_fetch_fails"),
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "numDmlAffectedRows": "3",
                "totalBytesBilled": "10485760",
                "totalBytesProcessed": "2048",
                "totalSlotMs": "4282"
            }),
        ),
        (500, backend_error()),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("MERGE t USING s ON FALSE WHEN NOT MATCHED THEN INSERT ROW")
        .unwrap();
    let reader = statement
        .execute()
        .unwrap_or_else(|error| panic!("a failed jobs.get failed a committed MERGE: {error}"));
    assert_eq!(
        job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:Statistics:Query:NumDMLAffectedRows", "3"),
            ("BIGQUERY:Statistics:Query:SlotMillis", "4282"),
            ("BIGQUERY:Statistics:Query:TotalBytesBilled", "10485760"),
            ("BIGQUERY:Statistics:Query:TotalBytesProcessed", "2048"),
            ("BIGQUERY:Statistics:TotalBytesProcessed", "2048"),
            ("BIGQUERY:query_id", "job_dml_fetch_fails"),
            ("BIGQUERY:statistics:query:num_dml_affected_rows", "3"),
            ("BIGQUERY:statistics:query:slot_millis", "4282"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10485760"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "2048"),
            ("BIGQUERY:statistics:total_bytes_processed", "2048"),
        ],
        "the row count survives; only the statement type the job alone reports is missing"
    );
    drop(reader);
    assert_eq!(job_gets(&requests.join().unwrap()), 1);
}

/// PERMISSION_DENIED disables Storage Read, since the same GET gates that path for every later
/// query. No other failure may, or a transient one would push every later query onto REST.
#[test]
fn only_a_refused_job_fetch_disables_storage_read() {
    for (status, body, disabled) in [
        (
            403,
            json!({
                "error": {
                    "code": 403,
                    "message": "caller cannot get the job",
                    "status": "PERMISSION_DENIED"
                }
            }),
            true,
        ),
        (500, backend_error(), false),
    ] {
        let (endpoint, requests) = capture_requests(vec![
            (
                200,
                json!({
                    "jobComplete": true,
                    "jobReference": job_reference("fixture-project", "job_fetch_refused"),
                    "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                    "totalBytesProcessed": "1536",
                    "totalRows": "0"
                }),
            ),
            (status, body),
        ]);
        let mut statement = test_statement_at(&endpoint);
        statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
        let reader = statement.execute().unwrap();
        drop(reader);
        assert_eq!(
            statement
                .context
                .state
                .storage_read_disabled("fixture-project"),
            disabled,
            "a {status} on jobs.get recorded the wrong Storage Read state"
        );
        requests.join().unwrap();
    }
}

/// The `jobs.get` resource of query job `job_id`, carrying `query` as its query statistics.
fn job_resource(job_id: &str, query: &serde_json::Value) -> serde_json::Value {
    json!({
        "jobReference": job_reference("fixture-project", job_id),
        "statistics": {"query": query}
    })
}

/// A `jobs.get` failure BigQuery answers with an HTTP 500.
fn backend_error() -> serde_json::Value {
    json!({"error": {"code": 500, "message": "backend error", "status": "INTERNAL"}})
}

/// One entry of the result schema's metadata.
fn schema_metadata<'a>(schema: &'a Schema, key: &str) -> Option<&'a str> {
    schema.metadata().get(key).map(String::as_str)
}

/// How many times the driver described a query job.
fn job_gets(requests: &[CapturedRequest]) -> usize {
    requests
        .iter()
        .filter(|request| request.line.starts_with("GET ") && request.line.contains("/jobs/"))
        .count()
}

/// The job metadata a result schema carries, sorted so a test can compare it whole.
fn job_metadata(schema: &Schema) -> Vec<(&str, &str)> {
    let mut entries: Vec<_> = schema
        .metadata()
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    entries.sort_unstable();
    entries
}
