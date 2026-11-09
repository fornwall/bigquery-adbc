//! What a result schema reports about the job that produced it, and when the driver spends its
//! one `jobs.get` to find out.

use super::*;
use crate::{
    BIGQUERY_SCHEMA_METADATA_QUERY_ID, BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
};

const SELECT: &str = "SELECT 21 + 21 AS answer";
const MERGE: &str = "MERGE t USING s ON FALSE WHEN NOT MATCHED THEN INSERT ROW";

/// A staged `jobs.query` answer of job `job_id` with an `answer` column, plus `fields`.
fn answer(job_id: &str, mut fields: serde_json::Value) -> (u16, serde_json::Value) {
    fields["schema"] = integer_schema("answer");
    completed_job(job_id, fields)
}

/// The `jobs.get` resource of query job `job_id`, carrying `query` as its query statistics.
fn job_resource(job_id: &str, query: &serde_json::Value) -> (u16, serde_json::Value) {
    done_job(job_id, json!({"statistics": {"query": query}}))
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

/// The job metadata a result schema carries, sorted and without the PascalCase aliases every
/// value is repeated under.
fn canonical_job_metadata(schema: &Schema) -> Vec<(&str, &str)> {
    let mut entries: Vec<_> = schema
        .metadata()
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .filter(|(key, _)| !key.starts_with("BIGQUERY:Statistics:"))
        .collect();
    entries.sort_unstable();
    entries
}

/// dbt 2.0 reads `BIGQUERY:query_id` from the result schema for telemetry, and parses the
/// legacy `Type` field metadata; a dropped, undrained reader must also clean up quietly.
#[test]
fn result_schema_reports_the_query_job_id_and_field_metadata() {
    let (endpoint, requests) = capture_requests(vec![answer(
        "job_dbt_123",
        json!({
            "totalBytesBilled": "0",
            "totalBytesProcessed": "0",
            "totalSlotMs": "1",
            "totalRows": "0"
        }),
    )]);
    let mut statement = test_query_at(&endpoint, SELECT);
    let reader = statement.execute().unwrap();
    let schema = reader.schema();
    assert_eq!(
        schema_metadata(&schema, BIGQUERY_SCHEMA_METADATA_QUERY_ID),
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
        "the legacy REST spelling dbt 2.0 parses"
    );
    // Dropped without draining, the way dbt 2.0 abandons fetch=false readers.
    drop(reader);
    requests.join().unwrap();
}

/// The durable job ID identifies the result even if the query response also carries a query ID.
/// The job resource supplies the statement type omitted from the query response.
#[test]
fn result_schema_reports_the_durable_job_id_instead_of_the_query_id() {
    let (endpoint, requests) = capture_requests(vec![
        answer("job_abc", json!({"queryId": "query_abc", "totalRows": "0"})),
        job_resource("job_abc", &json!({"statementType": "SELECT"})),
    ]);
    let mut statement = test_query_at(&endpoint, SELECT);
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
    let (endpoint, requests) = capture_requests(vec![ok(json!({
        "jobComplete": true,
        "queryId": "query_inline",
        "schema": integer_schema("answer"),
        "rows": [{"f": [{"v": "42"}]}],
        "totalRows": "1",
        "statementType": "SELECT",
        "totalSlotMs": "1",
        "totalBytesProcessed": "0",
        "totalBytesBilled": "0"
    }))]);
    let mut statement = test_query_at(&endpoint, "SELECT 42 AS answer");
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
    requests.served(1);
}

/// A DML statement returns no rows, so its job is fetched for the statement type, without which
/// dbt 2.0 never reads the row count.
///
/// The `jobs.query` response is the one BigQuery sends for an `UPDATE`: the target
/// table's schema, the affected-row count, the cost statistics, no `totalRows`, and no
/// `statementType`, which only the job resource carries.
#[test]
fn dml_result_schema_reports_the_affected_row_count() {
    let statistics = json!({
        "numDmlAffectedRows": "3",
        "totalBytesBilled": "10485760",
        "totalBytesProcessed": "2048",
        "totalSlotMs": "4282"
    });
    let mut job_statistics = statistics.clone();
    job_statistics["statementType"] = json!("MERGE");
    let (endpoint, requests) = capture_requests(vec![
        answer("job_stats_2", statistics),
        job_resource("job_stats_2", &job_statistics),
    ]);
    let mut statement = test_query_at(&endpoint, MERGE);
    let reader = statement.execute().unwrap();
    let schema = reader.schema();
    assert_eq!(
        canonical_job_metadata(&schema),
        vec![
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
    assert_eq!(
        schema_metadata(&schema, "BIGQUERY:Statistics:Query:NumDMLAffectedRows"),
        Some("3")
    );
    drop(reader);
    assert_eq!(job_gets(&requests.join().unwrap()), 1);
}

/// An incomplete jobs.query response is only a snapshot, and a polled query is completed by
/// `jobs.getQueryResults`, which reports neither the bytes billed nor the slot milliseconds. The
/// result is described once for them, though the statement type is already known, and the
/// completed job is authoritative over the earlier snapshot.
#[test]
fn a_polled_result_fetches_the_job_for_the_final_statistics() {
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({
            "jobComplete": false,
            "jobReference": job_reference("fixture-project", "job_query_running"),
            "statementType": "SELECT"
        })),
        answer(
            "job_query_running",
            json!({"totalBytesProcessed": "0", "totalRows": "0"}),
        ),
        job_resource(
            "job_query_running",
            &json!({
                "statementType": "SELECT",
                "totalBytesBilled": "10485760",
                "totalBytesProcessed": "1536",
                "totalSlotMs": "7167"
            }),
        ),
    ]);
    let mut statement = test_query_at(&endpoint, SELECT);

    let reader = statement.execute().unwrap();
    assert_eq!(
        canonical_job_metadata(reader.schema().as_ref()),
        vec![
            ("BIGQUERY:query_id", "job_query_running"),
            ("BIGQUERY:statistics:query:slot_millis", "7167"),
            ("BIGQUERY:statistics:query:statement_type", "SELECT"),
            ("BIGQUERY:statistics:query:total_bytes_billed", "10485760"),
            ("BIGQUERY:statistics:query:total_bytes_processed", "1536"),
            ("BIGQUERY:statistics:total_bytes_processed", "1536"),
        ]
    );
    drop(reader);

    let requests = requests.served(3);
    assert_eq!(job_gets(&requests), 1);
}

/// A result whose `jobs.get` fails still reads: the fetch is metadata-only, and the statistics
/// the response never carried are simply absent.
///
/// PERMISSION_DENIED also disables Storage Read, since the same GET gates that path for every
/// later query. No other failure may, or a transient one would push every later query onto REST.
#[test]
fn a_failed_job_fetch_leaves_the_result_readable_and_only_a_refusal_disables_storage_read() {
    for (failure, disabled) in [
        (
            google_rejection(500, "INTERNAL", "", "backend error"),
            false,
        ),
        (
            google_rejection(404, "NOT_FOUND", "", "job not found"),
            false,
        ),
        (
            google_rejection(403, "PERMISSION_DENIED", "", "denied"),
            true,
        ),
    ] {
        let status = failure.0;
        let (endpoint, requests) = capture_requests(vec![
            answer(
                "job_fetch_fails",
                json!({"totalBytesProcessed": "1536", "totalRows": "0"}),
            ),
            failure,
        ]);
        let mut statement = test_query_at(&endpoint, SELECT);
        let mut reader = statement
            .execute()
            .unwrap_or_else(|error| panic!("a {status} on jobs.get failed the query: {error}"));
        assert_eq!(
            canonical_job_metadata(reader.schema().as_ref()),
            vec![
                ("BIGQUERY:query_id", "job_fetch_fails"),
                ("BIGQUERY:statistics:query:total_bytes_processed", "1536"),
                ("BIGQUERY:statistics:total_bytes_processed", "1536"),
            ],
            "only what the query response itself reported"
        );
        assert!(reader.next().is_none(), "the result still reads");
        drop(reader);
        assert_eq!(
            statement
                .context
                .state
                .storage_read_disabled("fixture-project"),
            disabled,
            "a {status} on jobs.get recorded the wrong Storage Read state"
        );
        assert_eq!(job_gets(&requests.join().unwrap()), 1);
    }
}
