//! The Arrow metadata keys the driver attaches to results.
//!
//! These are public API; dbt-core 2.0 reads several of them for telemetry and response codes. The
//! field-level keys preserve the type parameters a table declares beyond its Arrow type; the
//! schema-level keys carry the service-reported query identity and statistics.
//!
//! Statistics use the Go driver's lowercase, snake_case keys. Result schemas also carry its
//! PascalCase aliases, which dbt-core 2.0 reads.

/// Arrow field metadata key preserving a BigQuery `STRING(L)` or `BYTES(L)` maximum length.
///
/// The value is the positive decimal `L`. For a repeated BigQuery field, it is stored on the outer
/// Arrow list field. The Go driver's `MaxLength` key is emitted alongside with the same value.
/// Both are absent where BigQuery declared no length.
pub const BIGQUERY_FIELD_METADATA_MAX_LENGTH: &str = "BIGQUERY:max_length";

/// Arrow field metadata key preserving a BigQuery `NUMERIC(P, S)` or `BIGNUMERIC(P, S)` precision.
///
/// The value is the decimal `P`, with `S` under [`BIGQUERY_FIELD_METADATA_SCALE`]. The Arrow type
/// stays NUMERIC's or BIGNUMERIC's full decimal type, because reading the column returns that type.
/// For a repeated field, both keys are on the outer Arrow list field, as
/// [`BIGQUERY_FIELD_METADATA_MAX_LENGTH`] is. Both are absent where BigQuery declared no
/// precision, which includes every query result.
pub const BIGQUERY_FIELD_METADATA_PRECISION: &str = "BIGQUERY:precision";

/// Arrow field metadata key preserving a BigQuery `NUMERIC(P, S)` or `BIGNUMERIC(P, S)` scale; see
/// [`BIGQUERY_FIELD_METADATA_PRECISION`].
pub const BIGQUERY_FIELD_METADATA_SCALE: &str = "BIGQUERY:scale";

/// Arrow schema metadata key carrying the ID of the BigQuery job or query that produced a result.
///
/// Result readers report the job ID when the query created a job, and otherwise the `queryId` of a
/// jobless `jobs.query` response. The empty result of a dbt-core 2.0 CSV seed load or table copy
/// carries its job ID too, and a python model's carries its remote job: the Vertex AI notebook
/// execution job's or Dataproc batch's resource name, or the Dataproc cluster job's ID. dbt-core
/// 2.0 reads this key for telemetry and `run_results.json`.
pub const BIGQUERY_SCHEMA_METADATA_QUERY_ID: &str = "BIGQUERY:query_id";

/// Arrow schema metadata key carrying the GoogleSQL statement type of the query that produced a
/// result, as BigQuery reported it: `SELECT`, `INSERT`, `MERGE`, `CREATE_TABLE_AS_SELECT`,
/// `SCRIPT`, and so on.
///
/// dbt-core 2.0 derives its response code and row count from this key's PascalCase alias.
///
/// Read directly from `jobs.query`, including jobless results, and retained across result polls.
/// A job resource supplies it when the query response omits it. The value is never inferred from
/// SQL or job creation: BigQuery can classify `SET @@dataset_id = '...'; SELECT 1` as `SELECT`
/// when jobless and `SCRIPT` when a job is required.
pub const BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE: &str =
    "BIGQUERY:statistics:query:statement_type";

/// Arrow schema metadata key carrying the bytes BigQuery billed for the query, as a decimal
/// integer.
///
/// Only on-demand pricing bills for bytes; under flat-rate pricing the value is informational.
pub const BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED: &str =
    "BIGQUERY:statistics:query:total_bytes_billed";

/// Arrow schema metadata key carrying the bytes the query processed, as a decimal integer.
pub const BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED: &str =
    "BIGQUERY:statistics:query:total_bytes_processed";

/// Arrow schema metadata key carrying the bytes the job processed, as a decimal integer.
///
/// A query job processes exactly what its query processes, so this repeats
/// [`BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED`]. dbt-core 2.0 reads the query-level
/// PascalCase alias first and falls back to this key's alias.
pub const BIGQUERY_SCHEMA_METADATA_JOB_TOTAL_BYTES_PROCESSED: &str =
    "BIGQUERY:statistics:total_bytes_processed";

/// Arrow schema metadata key carrying the slot milliseconds the query was billed for, as a decimal
/// integer.
pub const BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS: &str = "BIGQUERY:statistics:query:slot_millis";

/// Arrow schema metadata key carrying the number of rows a DML statement affected, as a decimal
/// integer.
///
/// BigQuery reports it only for DML, so the key is absent from every other result.
pub const BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS: &str =
    "BIGQUERY:statistics:query:num_dml_affected_rows";

/// Go driver aliases consumed by dbt-core 2.0.
pub(crate) mod legacy {
    pub(crate) const STATEMENT_TYPE: &str = "BIGQUERY:Statistics:Query:StatementType";
    pub(crate) const TOTAL_BYTES_BILLED: &str = "BIGQUERY:Statistics:Query:TotalBytesBilled";
    pub(crate) const QUERY_TOTAL_BYTES_PROCESSED: &str =
        "BIGQUERY:Statistics:Query:TotalBytesProcessed";
    pub(crate) const JOB_TOTAL_BYTES_PROCESSED: &str = "BIGQUERY:Statistics:TotalBytesProcessed";
    pub(crate) const SLOT_MILLIS: &str = "BIGQUERY:Statistics:Query:SlotMillis";
    pub(crate) const NUM_DML_AFFECTED_ROWS: &str = "BIGQUERY:Statistics:Query:NumDMLAffectedRows";
}

/// Every Arrow schema metadata key a result reader fills in from the job that produced it.
///
/// Two runs of the same query return the same fields and the same non-job metadata, but each runs
/// as its own job with its own ID and statistics. Comparing result schemas for equality means
/// ignoring these keys, including the PascalCase aliases consumed by dbt-core 2.0.
pub const BIGQUERY_SCHEMA_METADATA_JOB_KEYS: &[&str] = &[
    BIGQUERY_SCHEMA_METADATA_QUERY_ID,
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE,
    BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
    BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED,
    BIGQUERY_SCHEMA_METADATA_JOB_TOTAL_BYTES_PROCESSED,
    BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS,
    legacy::STATEMENT_TYPE,
    legacy::TOTAL_BYTES_BILLED,
    legacy::QUERY_TOTAL_BYTES_PROCESSED,
    legacy::JOB_TOTAL_BYTES_PROCESSED,
    legacy::SLOT_MILLIS,
    legacy::NUM_DML_AFFECTED_ROWS,
];
