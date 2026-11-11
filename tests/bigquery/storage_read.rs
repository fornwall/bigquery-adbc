//! Storage Read API results, checked value for value against the REST path.

use std::sync::Arc;

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::{Error, Status};
use adbc_core::options::OptionConnection;
use adbc_core::{Connection, Database, Optionable, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, BigQueryAuthenticationType, BigQueryConnection,
    BigQueryConnectionOption, BigQueryDatabaseOption, BigQueryStatement, BigQueryStatementOption,
    BigQueryWriteDisposition,
};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Schema};

use crate::support::{
    DropTable, assert_harvested_job_statistics, concatenated, connect, connect_with_dataset_at,
    db_option, execute_sql, int64_column, open_database, prepare_query, project, read_all,
    row_count, schema_without_job_metadata, scratch_fixture, scratch_table, set_option, typed,
};

/// Fixture row count. With [`PADDING`], the first `jobs.query` response (about 10 MB of JSON)
/// holds about 20,000 of these rows and leaves a continuation token, selecting Storage Read.
/// [`assert_paths_match`] checks batch boundaries to detect REST fallback if that changes.
const STORAGE_READ_ROWS: usize = 60_000;

/// About 500 bytes a row, so that a narrow fixture does not fit in the first response.
const PADDING: &str = "REPEAT('x', 500) AS padding";

/// The Storage Read equivalence query: enough rows for Storage Read, and every BigQuery type the
/// driver maps, including composites whose Arrow shape each path derives independently.
/// `GENERATE_ARRAY` scans no bytes.
const STORAGE_READ_TYPE_MATRIX_TEMPLATE: &str = r#"
SELECT
  n AS int64_col,
  CAST(n AS FLOAT64) AS float64_col,
  CAST(n AS NUMERIC) AS numeric_col,
  CAST(n AS BIGNUMERIC) AS bignumeric_col,
  -- BIGNUMERIC reaches one significant digit further than the Decimal256(76, 38) that describes
  -- it, so its extremes are where two independent decoders are likeliest to disagree.
  BIGNUMERIC '578960446186580977117854925043439539266.34992332820282019728792003956564819967' AS bignumeric_max_col,
  BIGNUMERIC '-578960446186580977117854925043439539266.34992332820282019728792003956564819968' AS bignumeric_min_col,
  BIGNUMERIC '100000000000000000000000000000000000000' AS bignumeric_scaled_col,
  CAST(n AS STRING) AS string_col,
  CAST(CAST(n AS STRING) AS BYTES) AS bytes_col,
  MOD(n, 2) = 0 AS bool_col,
  DATE '2026-08-28' AS date_col,
  TIME '12:34:56.789012' AS time_col,
  DATETIME '2026-08-28 12:34:56' AS datetime_col,
  TIMESTAMP '2026-08-28 12:34:56+00' AS timestamp_col,
  ST_GEOGPOINT(1, 2) AS geography_col,
  JSON '{"a":1}' AS json_col,
  MAKE_INTERVAL(1, 2, 3, 4, 5, 6) AS interval_col,
  [n, n + 1] AS array_col,
  STRUCT(n AS a, 'x' AS b) AS struct_col,
  [STRUCT(n AS a, [n, n + 1] AS b)] AS array_of_struct_col,
  STRUCT([n, n + 1] AS xs, STRUCT(n AS deep) AS nested) AS struct_of_array_col,
  RANGE(DATE '2026-01-01', DATE '2026-02-01') AS range_date_col,
  RANGE(DATETIME '2026-01-01 00:00:00', DATETIME '2026-02-01 00:00:00') AS range_datetime_col,
  RANGE(TIMESTAMP '2026-01-01 00:00:00+00', TIMESTAMP '2026-02-01 00:00:00+00') AS range_timestamp_col,
  RANGE(DATE '2026-01-01', NULL) AS range_unbounded_end_col,
  RANGE(CAST(NULL AS DATE), NULL) AS range_unbounded_col,
  [RANGE(DATE '2026-01-01', DATE '2026-02-01')] AS array_of_range_col,
  [JSON '{"a":1}', JSON 'null'] AS array_of_json_col,
  [ST_GEOGPOINT(1, 2)] AS array_of_geography_col,
  [STRUCT(RANGE(DATE '2026-01-01', DATE '2026-02-01') AS r, MAKE_INTERVAL(1, 2, 3, 4, 5, 6) AS i)]
    AS array_of_struct_of_range_col,
  STRUCT(CAST(NULL AS RANGE<DATE>) AS r, n AS a) AS struct_with_null_range_col,
  -- Unnamed and repeated field names, which both paths must rename the same way.
  STRUCT(n, 'x') AS anonymous_struct_col,
  STRUCT(n AS a, n + 1 AS a) AS duplicate_field_struct_col,
  IF(MOD(n, 3) = 0, NULL, n) AS nullable_col,
  IF(MOD(n, 5) = 0, NULL, [n]) AS nullable_array_col,
  IF(MOD(n, 7) = 0, NULL, STRUCT(n AS a)) AS nullable_struct_col,
  -- Domain boundaries and special values exercise both decoders on every row.
  IEEE_DIVIDE(0, 0) AS float64_nan_col,
  IEEE_DIVIDE(1, 0) AS float64_infinity_col,
  IEEE_DIVIDE(-1, 0) AS float64_negative_infinity_col,
  CAST('-0' AS FLOAT64) AS float64_negative_zero_col,
  CAST('9223372036854775807' AS INT64) AS int64_max_col,
  -- Spelled as a string because GoogleSQL reads `-9223372036854775808` as a negation of a
  -- literal that does not fit in an INT64.
  CAST('-9223372036854775808' AS INT64) AS int64_min_col,
  NUMERIC '99999999999999999999999999999.999999999' AS numeric_max_col,
  NUMERIC '-99999999999999999999999999999.999999999' AS numeric_min_col,
  DATE '0001-01-01' AS date_min_col,
  DATE '9999-12-31' AS date_max_col,
  DATETIME '0001-01-01 00:00:00' AS datetime_min_col,
  DATETIME '9999-12-31 23:59:59.999999' AS datetime_max_col,
  TIMESTAMP '0001-01-01 00:00:00+00' AS timestamp_min_col,
  TIMESTAMP '9999-12-31 23:59:59.999999+00' AS timestamp_max_col,
  TIME '00:00:00' AS time_min_col,
  TIME '23:59:59.999999' AS time_max_col,
  MAKE_INTERVAL(-1, -2, -3, -4, -5, -6) AS interval_negative_col,
  '' AS empty_string_col,
  b'' AS empty_bytes_col,
  -- A NUL byte is the one BYTES value a decoder that reaches for a C string truncates.
  b'\x00\x01\x00' AS bytes_with_nul_col,
  ARRAY<INT64>[] AS empty_array_col,
  CAST(NULL AS ARRAY<INT64>) AS null_array_col,
  CAST(NULL AS STRING) AS all_null_col,
  CAST(NULL AS STRUCT<a INT64, b STRING>) AS all_null_struct_col,
  -- REST spells these as text that must parse back to the exact binary value Storage Read sends.
  CAST('5e-324' AS FLOAT64) AS float64_subnormal_col,
  CAST('1.7976931348623157e308' AS FLOAT64) AS float64_max_col,
  0.1 + 0.2 AS float64_shortest_round_trip_col,
  NUMERIC '-0.000000001' AS numeric_smallest_col,
  BIGNUMERIC '0.00000000000000000000000000000000000001' AS bignumeric_smallest_col,
  CODE_POINTS_TO_STRING([97, 0, 233, 128013]) AS string_with_nul_col,
  DATETIME '1969-12-31 23:59:59.999999' AS datetime_before_epoch_col,
  TIMESTAMP '1969-12-31 23:59:59.999999+00' AS timestamp_before_epoch_col,
  INTERVAL '-1-2 3 -4:5:6.000001' YEAR TO SECOND AS interval_mixed_signs_col,
  INTERVAL '10000-0 -3660000 0:0:0' YEAR TO SECOND AS interval_extreme_parts_col,
  TIMESTAMP '2020-02-01 00:00:00+00' - TIMESTAMP '2020-01-01 00:00:00.5+00' AS interval_difference_col,
  RANGE(TIMESTAMP '0001-01-01 00:00:00+00', TIMESTAMP '9999-12-31 23:59:59.999999+00') AS range_timestamp_extremes_col,
  RANGE(CAST(NULL AS DATETIME), DATETIME '1969-12-31 23:59:59.999999') AS range_datetime_before_epoch_col,
  JSON '{"b":[1,{"c":null}],"a":"\\u00e9\\ud83d\\udc0d\\n"}' AS json_escapes_col,
  JSON '18446744073709551615' AS json_u64_max_col,
  ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(3 4, 5 6))') AS geography_collection_col,
  ST_GEOGFROMTEXT('POINT EMPTY') AS geography_empty_col,
  [CAST(n AS NUMERIC), NUMERIC '-99999999999999999999999999999.999999999'] AS array_of_numeric_col,
  [BIGNUMERIC '578960446186580977117854925043439539266.34992332820282019728792003956564819967', CAST(n AS BIGNUMERIC)] AS array_of_bignumeric_col,
  [TIMESTAMP '0001-01-01 00:00:00+00', TIMESTAMP '1969-12-31 23:59:59.999999+00'] AS array_of_timestamp_col,
  [DATETIME '9999-12-31 23:59:59.999999'] AS array_of_datetime_col,
  [DATE '0001-01-01'] AS array_of_date_col,
  [TIME '23:59:59.999999'] AS array_of_time_col,
  [MAKE_INTERVAL(-1, -2, -3, -4, -5, -6)] AS array_of_interval_col,
  [b'\x00', b''] AS array_of_bytes_col,
  [IEEE_DIVIDE(0, 0), CAST('-0' AS FLOAT64)] AS array_of_float_col,
  [TRUE, FALSE] AS array_of_bool_col,
  ['', 'x'] AS array_of_string_col,
  STRUCT(TIMESTAMP '2026-08-28 12:34:56+00' AS ts, CAST(n AS BIGNUMERIC) AS b, JSON '[1]' AS j, ST_GEOGPOINT(1, 2) AS g) AS struct_of_scalars_col,
  STRUCT(STRUCT(STRUCT(CAST(n AS NUMERIC) AS x) AS c) AS b) AS deep_struct_col,
  IF(MOD(n, 2) = 0, NULL, STRUCT(CAST(NULL AS INT64) AS a, [n] AS b)) AS nullable_struct_with_array_col,
  [STRUCT(CAST(NULL AS TIMESTAMP) AS t, CAST(NULL AS ARRAY<INT64>) AS xs)] AS array_of_struct_with_nulls_col
FROM UNNEST(GENERATE_ARRAY(1, $ROWS)) AS n"#;

/// [`STORAGE_READ_TYPE_MATRIX_TEMPLATE`] with its row count filled in.
fn storage_read_type_matrix() -> String {
    STORAGE_READ_TYPE_MATRIX_TEMPLATE.replace("$ROWS", &STORAGE_READ_ROWS.to_string())
}

/// A `STORAGE_READ_ROWS`-row query of `n` and [`PADDING`], for tests that only need the rows.
pub(crate) fn storage_read_row_sequence(order_by: &str) -> String {
    format!(
        "SELECT n, {PADDING} FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n{order_by}"
    )
}

/// `1..=STORAGE_READ_ROWS`, the values of [`storage_read_row_sequence`] in ascending order.
fn ascending() -> Vec<i64> {
    (1..=i64::try_from(STORAGE_READ_ROWS).unwrap()).collect()
}

/// The batches of the single partition `statement` executes to.
fn read_only_partition(
    connection: &BigQueryConnection,
    statement: &mut BigQueryStatement,
) -> Vec<RecordBatch> {
    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.partitions.len(), 1);
    connection
        .read_partition(result.partitions.first().unwrap())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Reads `sql` with Storage Read explicitly permitted or disabled.
fn read_storage_path(
    connection: &mut BigQueryConnection,
    sql: &str,
    storage_read: bool,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let value = if storage_read {
        ADBC_OPTION_VALUE_DISABLED
    } else {
        ADBC_OPTION_VALUE_ENABLED
    };
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        value,
    );
    statement.set_sql_query(sql).unwrap();
    read_all(&mut statement)
}

/// Detects a fallback from Storage Read to REST by batch boundaries: REST assembles fixed-size
/// batches, while Storage Read keeps BigQuery's. Fixtures must produce different layouts.
fn assert_storage_read_layout(storage: &[RecordBatch], rest: &[RecordBatch], what: &str) {
    let layout = |batches: &[RecordBatch]| {
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        row_count(storage),
        row_count(rest),
        "{what} and the REST control read different row counts"
    );
    assert_ne!(
        layout(storage),
        layout(rest),
        "{what} chunked the rows exactly as the REST reader did, so it fell back to REST (was the \
         result jobless?) and asserts nothing about Storage Read"
    );
}

/// Asserts `batches` came off the Storage Read path, by reading `sql` again over REST.
fn assert_read_through_storage_read(
    connection: &mut BigQueryConnection,
    sql: &str,
    batches: &[RecordBatch],
    what: &str,
) {
    let (_, rest_batches) = read_storage_path(connection, sql, false);
    assert_storage_read_layout(batches, &rest_batches, what);
}

/// Checks distinct batch layouts, equal schemas, and equal values across both read paths.
fn assert_paths_match(
    storage: &(Arc<Schema>, Vec<RecordBatch>),
    rest: &(Arc<Schema>, Vec<RecordBatch>),
) {
    let (storage_schema, storage_batches) = storage;
    let (rest_schema, rest_batches) = rest;
    assert_storage_read_layout(storage_batches, rest_batches, "the Storage Read side");
    assert_eq!(row_count(storage_batches), STORAGE_READ_ROWS);
    for (path, schema) in [("Storage Read", storage_schema), ("REST", rest_schema)] {
        assert_eq!(
            schema
                .metadata()
                .get(BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
            Some(&"SELECT".to_owned()),
            "{path} lost the statement type"
        );
    }
    assert_eq!(
        schema_without_job_metadata(storage_schema),
        schema_without_job_metadata(rest_schema),
        "Storage Read and REST advertised different schemas"
    );

    let storage_columns = concatenated(storage_schema, storage_batches);
    let rest_columns = concatenated(rest_schema, rest_batches);
    for ((storage_column, rest_column), field) in storage_columns
        .iter()
        .zip(&rest_columns)
        .zip(storage_schema.fields())
    {
        assert_eq!(
            storage_column,
            rest_column,
            "column {} differs between Storage Read and REST",
            field.name()
        );
    }
}

/// Storage Read and REST must return equal schemas and values for every mapped type.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn storage_read_matches_rest_for_every_mapped_type() {
    let mut connection = connect(project());

    let matrix = storage_read_type_matrix();
    let storage = read_storage_path(&mut connection, &matrix, true);
    let rest = read_storage_path(&mut connection, &matrix, false);
    assert_paths_match(&storage, &rest);
}

/// Reads inside a host's multi-threaded runtime, through the default single-stream Storage Read
/// session, which must preserve `ORDER BY`.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_ordered_results_inside_a_multi_threaded_host_runtime() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    host.block_on(host.spawn(async {
        let mut connection = connect(project());
        let sql = storage_read_row_sequence(" ORDER BY n DESC");
        let storage = read_storage_path(&mut connection, &sql, true);
        let rest = read_storage_path(&mut connection, &sql, false);
        let mut descending = ascending();
        descending.reverse();
        assert_eq!(int64_column(&storage.1), descending);
        assert_paths_match(&storage, &rest);
    }))
    .unwrap();
}

/// A missing Storage Read permission falls back to a complete REST read instead of failing, and
/// leaves the configured opt-out unchanged. The runner impersonates a keyless service account
/// with `roles/bigquery.jobUser` but without `bigquery.readsessions.create`.
#[test]
#[ignore = "requires the keyless no-readsession service account in the integration project"]
fn storage_read_permission_denied_falls_back_to_rest() {
    crate::backend::require_real_bigquery();
    let access_token = std::env::var("BIGQUERY_ADBC_STORAGE_READ_DENIED_ACCESS_TOKEN")
        .expect("BIGQUERY_ADBC_STORAGE_READ_DENIED_ACCESS_TOKEN must be set");
    let database = open_database([
        db_option(BigQueryDatabaseOption::ProjectId, project()),
        db_option(
            BigQueryDatabaseOption::AuthenticationType,
            BigQueryAuthenticationType::TemporaryAccessToken,
        ),
        db_option(
            BigQueryDatabaseOption::AuthenticationAccessToken,
            access_token,
        ),
    ]);
    let mut connection = database.new_connection().unwrap();
    let option: OptionConnection = BigQueryConnectionOption::UseStorageApiDisabledClient.into();
    assert_eq!(
        connection.get_option_string(option.clone()).unwrap(),
        ADBC_OPTION_VALUE_DISABLED
    );

    let mut statement = prepare_query(&mut connection, &storage_read_row_sequence(" ORDER BY n"));
    let (schema, batches) = read_all(&mut statement);
    assert_eq!(
        schema
            .metadata()
            .get(BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
        Some(&"SELECT".to_owned())
    );
    assert_eq!(int64_column(&batches), ascending());

    assert_eq!(
        connection.get_option_string(option).unwrap(),
        ADBC_OPTION_VALUE_DISABLED,
        "automatic fallback must not change the configured opt-out"
    );
}

/// `bigquery.query.unordered_read` trades ordering for parallel streams; the rows must not change.
///
/// The server chooses the stream count. Comparing sorted values checks completeness whether
/// it grants one stream or several.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn unordered_storage_read_returns_the_same_rows() {
    let sql = storage_read_row_sequence("");
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, &sql);
    set_option(
        &mut statement,
        BigQueryStatementOption::UnorderedRead,
        "true",
    );

    let (_, batches) = read_all(&mut statement);
    let mut values = int64_column(&batches);
    values.sort_unstable();
    assert_eq!(values, ascending());

    // Equal sorted values alone would not detect a fallback to REST.
    assert_read_through_storage_read(&mut connection, &sql, &batches, "the unordered read");
}

/// A large query partition uses Storage Read, since `read_partition` completes the job through
/// the ordinary reader. The two-row partitions of the parameterized partition test reach only REST.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_a_storage_read_sized_query_partition() {
    let sql = storage_read_row_sequence(" ORDER BY n");
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, &sql);

    let result = statement.execute_partitions().unwrap();
    assert_eq!(result.schema.fields().len(), 2);
    assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(result.partitions.len(), 1);

    let reader = connection.read_partition(&result.partitions[0]).unwrap();
    let schema = reader.schema();
    assert_eq!(schema_without_job_metadata(&schema), result.schema);
    let batches = reader.collect::<Result<Vec<_>, ArrowError>>().unwrap();

    // Compare the sequence to catch lost or repeated chunks, including after a resume.
    assert_eq!(int64_column(&batches), ascending());

    assert_read_through_storage_read(&mut connection, &sql, &batches, "the partition read");
}

/// A partition reads with the result options of the statement that created it, not those of the
/// connection reading it: one created with Storage Read disabled is read over REST, whose batches
/// hold at most 1,024 rows where Storage Read keeps BigQuery's larger ones.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_partition_keeps_the_result_options_of_its_statement() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, &storage_read_row_sequence(" ORDER BY n"));
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        ADBC_OPTION_VALUE_ENABLED,
    );
    let batches = read_only_partition(&connection, &mut statement);
    assert_eq!(row_count(&batches), STORAGE_READ_ROWS);
    assert!(
        batches.iter().all(|batch| batch.num_rows() <= 1024),
        "the partition was read through Storage Read"
    );
}

/// `CreateReadSession` refuses a destination naming one partition, which carries a table
/// decorator, and a table with `require_partition_filter` (`INVALID_ARGUMENT`), though REST reads
/// both, so a Storage Read-sized partition written to either is read over REST.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn reads_partitions_written_where_storage_read_refuses_a_session() {
    let mut connection = connect_with_dataset_at("US");
    for (options, decorator) in [
        ("", "$20240101"),
        (" OPTIONS(require_partition_filter = true)", ""),
    ] {
        let table = scratch_table("storage_read_partition");
        let _drop_table = DropTable::table(&table);
        execute_sql(
            &mut connection,
            &format!(
                "CREATE TABLE `{table}` (n INT64, d DATE, padding STRING) PARTITION BY d{options}"
            ),
        );
        let mut statement = prepare_query(
            &mut connection,
            &format!(
                "SELECT n, DATE '2024-01-01' AS d, {PADDING} \
                 FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n"
            ),
        );
        set_option(
            &mut statement,
            BigQueryStatementOption::DestinationTable,
            format!("{table}{decorator}"),
        );
        set_option(
            &mut statement,
            BigQueryStatementOption::WriteDisposition,
            BigQueryWriteDisposition::WriteTruncate,
        );
        let batches = read_only_partition(&connection, &mut statement);
        assert_eq!(
            row_count(&batches),
            STORAGE_READ_ROWS,
            "{options}{decorator}"
        );
    }
}

/// Reads stored `NUMERIC(10, 2)` / `BIGNUMERIC(20, 4)` columns; `CAST` cannot produce these
/// parameterized types. BigQuery reports plain NUMERIC/BIGNUMERIC in results. The layout check
/// detects REST fallback if the Storage Read schema is rejected.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn storage_read_matches_rest_for_parameterized_decimals() {
    let (table, _drop_table, mut connection) = scratch_fixture("storage_read");

    execute_sql(
        &mut connection,
        &format!(
            "CREATE OR REPLACE TABLE `{table}` (\
               narrow NUMERIC(10, 2), \
               wide BIGNUMERIC(20, 4), \
               plain NUMERIC, \
               n INT64)"
        ),
    );
    execute_sql(
        &mut connection,
        &format!(
            "INSERT INTO `{table}` (narrow, wide, plain, n) \
             SELECT CAST(n / 100 AS NUMERIC), CAST(n / 1000 AS BIGNUMERIC), CAST(n AS NUMERIC), n \
             FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n"
        ),
    );

    let query = format!("SELECT narrow, wide, plain, n, {PADDING} FROM `{table}` ORDER BY n");
    let storage = read_storage_path(&mut connection, &query, true);
    let rest = read_storage_path(&mut connection, &query, false);

    assert_eq!(
        storage.0.field(0).data_type(),
        rest.0.field(0).data_type(),
        "the two paths disagreed on the decimal width of a stored NUMERIC(10, 2)"
    );
    assert_paths_match(&storage, &rest);
}

/// Query results report every field `NULLABLE`, but a job appending to a table reports the table's
/// `REQUIRED` modes, top-level and nested, and its partition reads that table. Storage Read must
/// reconcile those non-nullable fields, and a flexible column name, rather than fall back to REST.
/// A table read has no order, so the values are compared sorted.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn reads_a_partition_of_required_fields_and_flexible_names_through_storage_read() {
    let mut connection = connect_with_dataset_at("US");
    let table = scratch_table("storage_read_required");
    let _drop_table = DropTable::table(&table);
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (\
               n INT64 NOT NULL, \
               s STRUCT<x INT64 NOT NULL, y STRING> NOT NULL, \
               r ARRAY<STRUCT<z INT64 NOT NULL>>, \
               `flexible name-é` INT64, \
               padding STRING)"
        ),
    );
    let mut statement = prepare_query(
        &mut connection,
        &format!(
            "SELECT n, STRUCT(n AS x, IF(MOD(n, 2) = 0, NULL, 'y') AS y) AS s, \
               [STRUCT(n AS z)] AS r, n AS `flexible name-é`, {PADDING} \
             FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n"
        ),
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        table.as_str(),
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    let batches = read_only_partition(&connection, &mut statement);

    let schema = batches.first().unwrap().schema();
    let first_child = |field: &arrow_schema::Field| match field.data_type() {
        DataType::Struct(fields) => fields.first().unwrap().clone(),
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => fields.first().unwrap().clone(),
            other => panic!("{} is a list of {other}", field.name()),
        },
        other => panic!("{} is {other}", field.name()),
    };
    for field in [
        schema.field(0),
        schema.field(1),
        &first_child(schema.field(1)),
        &first_child(schema.field(2)),
    ] {
        assert!(
            !field.is_nullable(),
            "{} lost its REQUIRED mode",
            field.name()
        );
    }
    assert_eq!(schema.field(3).name(), "flexible name-é");
    assert!(
        batches.iter().any(|batch| batch.num_rows() > 1024),
        "the partition fell back to REST, whose batches hold at most 1,024 rows"
    );
    let mut values = int64_column(&batches);
    values.sort_unstable();
    assert_eq!(values, ascending());
}

/// Cost statistics survive into a Storage Read result schema of a stored table, which bills bytes
/// where generated rows bill none. Padding pushes the result past one `jobs.query` response (about
/// 10 MB of JSON), so `JOB_CREATION_OPTIONAL` still creates the job and destination table Storage
/// Read needs.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn storage_read_results_report_the_job_statistics() {
    let (table, _drop_table, mut connection) = scratch_fixture("storage_read_stats");
    execute_sql(
        &mut connection,
        &format!(
            "CREATE OR REPLACE TABLE `{table}` AS \
             SELECT n, REPEAT('x', 500) AS padding \
             FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n"
        ),
    );

    let sql = format!("SELECT n, padding FROM `{table}`");
    let mut statement = prepare_query(&mut connection, &sql);
    let (schema, batches) = read_all(&mut statement);
    assert_read_through_storage_read(&mut connection, &sql, &batches, "the default read");
    assert_harvested_job_statistics(schema.metadata(), "the Storage Read query");
}

/// BigQuery refuses every REST response holding a row over 20 MiB, even for a two-row result, but
/// keeps the job, whose rows Storage Read returns. With Storage Read disabled the error says so.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_a_row_too_large_for_rest_through_storage_read() {
    const LARGE_ROWS: &str = "SELECT n, \
        (SELECT STRING_AGG(REPEAT('x', 1000000), '') FROM UNNEST(GENERATE_ARRAY(1, 21))) AS big \
        FROM UNNEST([1, 2]) AS n ORDER BY n";
    let mut connection = connect(project());

    let (schema, batches) = read_storage_path(&mut connection, LARGE_ROWS, true);
    assert_eq!(int64_column(&batches), [1, 2]);
    let columns = concatenated(&schema, &batches);
    let big = typed::<arrow_array::StringArray>(columns[1].as_ref());
    assert!((0..2).all(|row| big.value(row).len() == 21_000_000));

    let mut statement = prepare_query(&mut connection, LARGE_ROWS);
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        ADBC_OPTION_VALUE_ENABLED,
    );
    let failure = match statement.execute() {
        Ok(reader) => reader
            .collect::<Result<Vec<_>, ArrowError>>()
            .expect_err("REST cannot return the row")
            .to_string(),
        Err(error) => error.message,
    };
    assert!(
        failure.contains("20 MiB") && failure.contains("use_storage_api_disabled_client"),
        "{failure}"
    );
}

/// The first page has ordinary rows, but a later row exceeds REST's limit. Selecting Storage
/// Read before delivering the first page preserves the complete ordered result exactly once.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_a_later_row_too_large_for_rest_through_storage_read() {
    const SQL: &str = "SELECT i, CASE WHEN i = 2001 THEN \
        ARRAY(SELECT REPEAT('x', 800000) FROM UNNEST(GENERATE_ARRAY(1, 30))) \
        ELSE [REPEAT('y', 8192)] END AS payload \
        FROM UNNEST(GENERATE_ARRAY(1, 2001)) i ORDER BY i";
    let mut connection = connect(project());
    let (schema, batches) = read_storage_path(&mut connection, SQL, true);
    assert_eq!(int64_column(&batches), (1..=2_001).collect::<Vec<i64>>());
    let columns = concatenated(&schema, &batches);
    let payloads = typed::<arrow_array::ListArray>(columns[1].as_ref());
    for row in 0..2_001 {
        let payload = payloads.value(row);
        let values = typed::<arrow_array::StringArray>(payload.as_ref());
        let (count, width, character) = if row == 2_000 {
            (30, 800_000, b'x')
        } else {
            (1, 8_192, b'y')
        };
        assert_eq!(values.len(), count);
        for value in values {
            let value = value.unwrap();
            assert_eq!(value.len(), width);
            assert!(value.bytes().all(|byte| byte == character));
        }
    }
}

/// Empty results leave Storage Read nothing to read and use REST under both read settings.
/// Both settings must preserve the schema and return no rows.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_an_empty_result_through_both_read_settings() {
    const EMPTY: &str =
        "SELECT n, CAST(n AS STRING) AS s FROM UNNEST(GENERATE_ARRAY(1, 10)) AS n WHERE n < 0";

    let mut connection = connect(project());
    for storage_read in [true, false] {
        let (schema, batches) = read_storage_path(&mut connection, EMPTY, storage_read);
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            [("n", &DataType::Int64), ("s", &DataType::Utf8)],
            "use_storage_read={storage_read}"
        );
        assert_eq!(row_count(&batches), 0, "use_storage_read={storage_read}");
    }
}

/// Arrow's INTERVAL holds its time part in i64 nanoseconds, about 2,562,047 hours, where BigQuery's
/// reaches 87,840,000. The longest representable one reads alike on both paths; past it, REST names
/// the `CAST` that reads the value and Storage Read relays BigQuery's own refusal, both as invalid
/// data (a REST `ParseError` is `INVALID_DATA` through the C API).
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn an_interval_past_arrows_time_part_is_invalid_data_on_both_paths() {
    let sql = |time: &str| {
        format!(
            "SELECT n, INTERVAL '0-0 0 {time}' YEAR TO SECOND AS i, {PADDING} \
             FROM UNNEST(GENERATE_ARRAY(1, {STORAGE_READ_ROWS})) AS n"
        )
    };
    let mut connection = connect(project());
    let longest = sql("2562047:47:16.854775");
    let storage = read_storage_path(&mut connection, &longest, true);
    let rest = read_storage_path(&mut connection, &longest, false);
    assert_paths_match(&storage, &rest);

    let past = sql("2562048:0:0");
    for storage_read in [true, false] {
        let mut statement = connection.new_statement().unwrap();
        set_option(
            &mut statement,
            BigQueryStatementOption::UseStorageApiDisabledClient,
            if storage_read {
                ADBC_OPTION_VALUE_DISABLED
            } else {
                ADBC_OPTION_VALUE_ENABLED
            },
        );
        statement.set_sql_query(&past).unwrap();
        let failure = statement
            .execute()
            .unwrap()
            .find_map(Result::err)
            .expect("no Arrow INTERVAL holds the value");
        match failure {
            ArrowError::ExternalError(source) if storage_read => {
                let failure = source.downcast::<Error>().unwrap();
                assert_eq!(failure.status, Status::InvalidData, "{}", failure.message);
                assert!(
                    failure.message.contains("Storage Read rows")
                        && failure.message.contains("OUT_OF_RANGE"),
                    "{}",
                    failure.message
                );
            }
            ArrowError::ParseError(message) if !storage_read => {
                assert!(message.contains("CAST(x AS STRING)"), "{message}");
            }
            other => panic!("use_storage_read={storage_read}: {other}"),
        }
    }
}
