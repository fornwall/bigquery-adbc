//! Query execution, dry runs, destinations and result-schema job statistics.

use std::collections::BTreeMap;

use adbc_core::constants::ADBC_OPTION_VALUE_DISABLED;
use adbc_core::error::Status;
use adbc_core::options::OptionConnection;
use adbc_core::{Connection, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS, BIGQUERY_SCHEMA_METADATA_QUERY_ID,
    BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE, BigQueryConnection, BigQueryCreateDisposition,
    BigQueryQueryPriority, BigQueryStatementOption, BigQueryWriteDisposition,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, Float64Array, Int64Array,
    ListArray, RecordBatchReader, StructArray, Time64MicrosecondArray,
};
use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::cancellation::CANCELLABLE_QUERY;
use crate::support::{
    DropTable, SCRATCH_DATASET, assert_harvested_job_statistics, assert_matches_dry_run, bool_at,
    connect, connect_at_location, connect_with_dataset, connect_with_dataset_at, count_rows,
    date_at, date_value, execute_failure, execute_sql, i64_at, int64_column, interval_at, list_at,
    prepare_query, project, read_all, read_single_i64, scratch_fixture, scratch_table, set_option,
    single_batch, str_at, str_value, struct_at, ts_at, ts_value, typed,
};

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn configures_query_destinations_dispositions_and_dry_runs() {
    let table = scratch_table("query_options");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset_at("US");

    let mut destination = connection.new_statement().unwrap();
    set_option(
        &mut destination,
        BigQueryStatementOption::DefaultProjectId,
        project(),
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::DefaultDatasetId,
        SCRATCH_DATASET,
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::DestinationTable,
        table.clone(),
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::CreateDisposition,
        BigQueryCreateDisposition::CreateIfNeeded,
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteTruncate,
    );
    destination.set_sql_query("SELECT 7 AS value").unwrap();
    assert!(destination.execute().unwrap().next().is_none());

    let mut verify = prepare_query(&mut connection, &format!("SELECT value FROM `{table}`"));
    assert_eq!(read_single_i64(&mut verify), 7);

    set_option(
        &mut destination,
        BigQueryStatementOption::CreateDisposition,
        BigQueryCreateDisposition::CreateNever,
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    destination.set_sql_query("SELECT 8 AS value").unwrap();
    assert!(destination.execute().unwrap().next().is_none());

    assert_eq!(count_rows(&mut connection, &table), 2);

    let mut dry_run = connection.new_statement().unwrap();
    set_option(&mut dry_run, BigQueryStatementOption::DryRun, "true");
    dry_run.set_sql_query("SELECT 42 AS answer").unwrap();
    let mut reader = dry_run.execute().unwrap();
    assert_eq!(reader.schema().field(0).name(), "answer");
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Int64);
    assert!(reader.next().is_none());
    assert_eq!(dry_run.execute_update().unwrap(), None);
}

/// Bounds a test BATCH job: BigQuery may queue it, and stops it at this timeout.
const BATCH_JOB_TIMEOUT_MS: i64 = 10 * 60 * 1000;

/// A dbt profile may set `priority: batch` without `location`. The job runs where BigQuery
/// infers, and its rows are read from there (G48).
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_batch_query_without_a_location_returns_its_rows() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, "SELECT 7 AS value");
    set_option(
        &mut statement,
        BigQueryStatementOption::Priority,
        BigQueryQueryPriority::Batch,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::JobTimeout,
        BATCH_JOB_TIMEOUT_MS,
    );
    let (schema, batches) = read_all(&mut statement);
    assert_eq!(batches.len(), 1);
    assert_eq!(i64_at(&batches[0], 0), 7);
    // `jobs.insert` names its job with the driver's prefix; `jobs.query` would not.
    let query_id = &schema.metadata()[BIGQUERY_SCHEMA_METADATA_QUERY_ID];
    assert!(query_id.starts_with("job_"), "{query_id}");
}

/// A destination query without a location writes its table where BigQuery infers (G48).
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn a_destination_query_without_a_location_writes_its_table() {
    let (table, _drop_table, mut connection) = scratch_fixture("unlocated_destination");
    let mut statement = prepare_query(&mut connection, "SELECT 7 AS value");
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        table.clone(),
    );
    assert!(statement.execute().unwrap().next().is_none());

    let mut verify = prepare_query(&mut connection, &format!("SELECT value FROM `{table}`"));
    assert_eq!(read_single_i64(&mut verify), 7);
}

/// A `project.catalog.namespace.table` destination names the BigLake dataset `catalog.namespace`,
/// the dataset BigQuery's SQL resolves the same path to. No such catalog exists, and
/// `CREATE_NEVER` keeps the job from creating anything.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_four_part_destination_names_the_dataset_sql_resolves() {
    let project = project();
    let path = format!("{project}.adbc_missing_catalog.adbc_missing_namespace.t");
    let dataset = format!("Dataset {project}:adbc_missing_catalog.adbc_missing_namespace ");
    let mut connection = connect_at_location(project, "US");

    let mut destination = prepare_query(&mut connection, "SELECT 1 AS value");
    set_option(
        &mut destination,
        BigQueryStatementOption::DestinationTable,
        path.clone(),
    );
    set_option(
        &mut destination,
        BigQueryStatementOption::CreateDisposition,
        BigQueryCreateDisposition::CreateNever,
    );
    let mut read = prepare_query(&mut connection, &format!("SELECT * FROM `{path}`"));
    for (what, statement) in [("the destination", &mut destination), ("SQL", &mut read)] {
        let (status, message) = execute_failure(statement);
        assert_eq!(status, Status::NotFound, "{what}: {message}");
        assert!(message.contains(&dataset), "{what}: {message}");
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn dry_runs_accept_missing_optional_schema_and_parameter_fields() {
    let project = project();
    let mut connection = connect(project);
    let mut statement = prepare_query(&mut connection, "SELECT 1 AS value");
    statement.prepare().unwrap();
    let parameters = statement.get_parameter_schema().unwrap();
    assert!(parameters.fields().is_empty());

    // Both calls are dry runs: the table is never dropped or created.
    statement
        .set_sql_query(format!(
            "DROP TABLE IF EXISTS `{project}.{SCRATCH_DATASET}.adbc_dry_run_schema_probe`"
        ))
        .unwrap();
    assert!(statement.execute_schema().unwrap().fields().is_empty());
    set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
    let mut reader = statement.execute().unwrap();
    assert!(reader.schema().fields().is_empty());
    assert!(reader.next().is_none());
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_scalar_query_and_dry_run_schema() {
    let mut connection = connect(project());
    let mut statement = prepare_query(
        &mut connection,
        r#"
            SELECT
              CAST(42 AS INT64) AS integer_value,
              CAST(1.25 AS FLOAT64) AS float_value,
              TRUE AS boolean_value,
              'hello' AS string_value,
              FROM_BASE64('AQID') AS binary_value,
              TIMESTAMP '1970-01-01 00:00:01.123456+00' AS timestamp_value,
              DATE '1970-01-02' AS date_value,
              TIME '00:00:01.123456' AS time_value,
              DATETIME '1970-01-01 00:00:01.123456' AS datetime_value,
              NUMERIC '123.456' AS numeric_value,
              BIGNUMERIC '1.25' AS bignumeric_value,
              INTERVAL '1-2 3 4:05:06.007008' YEAR TO SECOND AS interval_value,
              RANGE(DATE '1970-01-01', DATE '1970-01-03') AS date_range,
              RANGE(DATETIME '1970-01-01 00:00:01.123456', NULL) AS datetime_range,
              RANGE(TIMESTAMP '1970-01-01 00:00:01.000001+00',
                    TIMESTAMP '1970-01-01 00:00:02.123456+00') AS timestamp_range,
              ST_GEOGFROMTEXT('POINT(1 2)') AS geography_value,
              JSON '{"a":1}' AS json_value,
              CAST(NULL AS STRING) AS null_value
            "#,
    );

    let dry_run_schema = statement.execute_schema().unwrap();
    assert_eq!(dry_run_schema.fields().len(), 18);
    assert_eq!(dry_run_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        dry_run_schema.field(5).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    let mut reader = statement.execute().unwrap();
    assert_matches_dry_run(&dry_run_schema, reader.schema().as_ref());
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(i64_at(&batch, 0), 42);
    assert_eq!(
        typed::<Float64Array>(batch.column(1).as_ref()).value(0),
        1.25
    );
    assert!(bool_at(&batch, 2));
    assert_eq!(str_at(&batch, 3), "hello");
    assert_eq!(
        typed::<BinaryArray>(batch.column(4).as_ref()).value(0),
        &[1, 2, 3]
    );
    assert_eq!(ts_at(&batch, 5), 1_123_456);
    assert_eq!(date_at(&batch, 6), 1);
    assert_eq!(
        typed::<Time64MicrosecondArray>(batch.column(7).as_ref()).value(0),
        1_123_456
    );
    assert_eq!(ts_at(&batch, 8), 1_123_456);
    assert_eq!(
        typed::<Decimal128Array>(batch.column(9).as_ref()).value(0),
        123_456_000_000
    );
    assert_eq!(
        typed::<Decimal256Array>(batch.column(10).as_ref()).value(0),
        i256::from_string("125000000000000000000000000000000000000").unwrap()
    );
    assert_eq!(
        interval_at(&batch, 11),
        IntervalMonthDayNano::new(14, 3, 14_706_007_008_000)
    );
    assert_eq!(
        batch.column(11).data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );

    let date_range = struct_at(&batch, 12);
    assert_eq!(date_value(date_range.column(0)), 0);
    assert_eq!(date_value(date_range.column(1)), 2);

    let datetime_range = struct_at(&batch, 13);
    assert_eq!(ts_value(datetime_range.column(0)), 1_123_456);
    assert!(datetime_range.column(1).is_null(0));

    let timestamp_range = struct_at(&batch, 14);
    assert_eq!(ts_value(timestamp_range.column(0)), 1_000_001);
    assert_eq!(ts_value(timestamp_range.column(1)), 2_123_456);
    assert_eq!(str_at(&batch, 15), "POINT(1 2)");
    assert_eq!(str_at(&batch, 16), r#"{"a":1}"#);
    assert!(batch.column(17).is_null(0));
    assert!(reader.next().is_none());
}

/// Query limits produce a refusal or timeout. Cache-option forwarding is checked by
/// `src/query/request/tests.rs`: BigQuery's best-effort cache does not guarantee a hit.
#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn query_limits_change_what_bigquery_does() {
    const REFUSED_SCAN: &str =
        "SELECT COUNT(word) FROM `bigquery-public-data.samples.shakespeare` WHERE word != ''";

    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, REFUSED_SCAN);

    // MaximumBytesBilled: no table scan fits in one byte. A constant query or a cache hit bills
    // nothing, so this scans a table with the cache off.
    set_option(
        &mut statement,
        BigQueryStatementOption::MaximumBytesBilled,
        1_i64,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::DisableQueryCache,
        "true",
    );
    let (status, message) = execute_failure(&mut statement);
    assert!(
        message.contains("bytes billed"),
        "the byte limit never reached BigQuery: {status:?} {message}"
    );
    assert_eq!(status, Status::InvalidArguments, "{message}");

    // JobTimeout: a 100 ms deadline must end the job. The cache is off because the cancellation
    // test runs this same text and may let it finish.
    let mut timed_out = prepare_query(&mut connection, CANCELLABLE_QUERY);
    set_option(&mut timed_out, BigQueryStatementOption::JobTimeout, 100_i64);
    set_option(
        &mut timed_out,
        BigQueryStatementOption::DisableQueryCache,
        "true",
    );
    let (status, message) = execute_failure(&mut timed_out);
    assert!(
        message.contains("timed out"),
        "the job timeout never reached BigQuery: {status:?} {message}"
    );
    assert_eq!(status, Status::Timeout, "{message}");
    assert!(
        message.contains("bigquery.query.job_timeout (100 ms)"),
        "the failure names the option that stopped the job: {message}"
    );
}

/// dbt 2.0 derives its response code, row count, and cost telemetry from the job statistics on
/// the result schema, so every key has to arrive with a real BigQuery job behind it. Results
/// without rows report `StatementType` too: without it, dbt 2.0's `code_and_rows` returns early
/// and never reads `NumDMLAffectedRows`. jobs.query supplies the type, with the job resource as
/// the fallback.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn result_schemas_report_the_job_statistics() {
    /// The job statistics on the result schema of `sql`, with the query ID checked and dropped.
    fn statistics(connection: &mut BigQueryConnection, sql: &str) -> BTreeMap<String, String> {
        let mut statement = prepare_query(connection, sql);
        let schema = statement.execute().unwrap().schema();
        let mut statistics: BTreeMap<_, _> = schema
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let query_id = statistics.remove(BIGQUERY_SCHEMA_METADATA_QUERY_ID);
        assert!(
            query_id.is_some_and(|query_id| !query_id.is_empty()),
            "result schema is missing BIGQUERY:query_id"
        );
        statistics
    }

    /// The value of `key`, parsed as the decimal integer dbt 2.0 expects.
    fn integer(statistics: &BTreeMap<String, String>, key: &str) -> i64 {
        let value = &statistics[key];
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} is {value:?}, not a decimal integer"))
    }

    let table = scratch_table("job_stats");
    let view = format!("{table}_view");
    let _drop_table = DropTable::table(&table);
    let _drop_view = DropTable::new("VIEW", &view);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{table}` AS SELECT * FROM UNNEST([1, 2, 3]) AS value"),
    );

    // A SELECT reads a real table, so BigQuery has bytes and slots to report; a SELECT has no
    // affected-row count. jobs.query supplies the statement type even for jobless results.
    let select = statistics(&mut connection, &format!("SELECT value FROM `{table}`"));
    assert_eq!(
        select.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "BIGQUERY:Statistics:Query:SlotMillis",
            "BIGQUERY:Statistics:Query:StatementType",
            "BIGQUERY:Statistics:Query:TotalBytesBilled",
            "BIGQUERY:Statistics:Query:TotalBytesProcessed",
            "BIGQUERY:Statistics:TotalBytesProcessed",
            "BIGQUERY:statistics:query:slot_millis",
            "BIGQUERY:statistics:query:statement_type",
            "BIGQUERY:statistics:query:total_bytes_billed",
            "BIGQUERY:statistics:query:total_bytes_processed",
            "BIGQUERY:statistics:total_bytes_processed",
        ]
    );
    assert_eq!(select[BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE], "SELECT");
    assert!(integer(&select, "BIGQUERY:Statistics:Query:TotalBytesProcessed") > 0);
    assert_eq!(
        integer(&select, "BIGQUERY:Statistics:TotalBytesProcessed"),
        integer(&select, "BIGQUERY:Statistics:Query:TotalBytesProcessed"),
        "a query job processes exactly what its query processes"
    );
    // A query that read a real table billed bytes and used slots.
    assert!(integer(&select, "BIGQUERY:Statistics:Query:TotalBytesBilled") > 0);
    assert!(integer(&select, "BIGQUERY:Statistics:Query:SlotMillis") > 0);

    // DML additionally reports the row count dbt 2.0 turns into `rows_affected`, and the
    // statement type it gates that read on, both available directly from jobs.query.
    let update = statistics(
        &mut connection,
        &format!("UPDATE `{table}` SET value = value + 1 WHERE TRUE"),
    );
    assert_eq!(
        update.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "BIGQUERY:Statistics:Query:NumDMLAffectedRows",
            "BIGQUERY:Statistics:Query:SlotMillis",
            "BIGQUERY:Statistics:Query:StatementType",
            "BIGQUERY:Statistics:Query:TotalBytesBilled",
            "BIGQUERY:Statistics:Query:TotalBytesProcessed",
            "BIGQUERY:Statistics:TotalBytesProcessed",
            "BIGQUERY:statistics:query:num_dml_affected_rows",
            "BIGQUERY:statistics:query:slot_millis",
            "BIGQUERY:statistics:query:statement_type",
            "BIGQUERY:statistics:query:total_bytes_billed",
            "BIGQUERY:statistics:query:total_bytes_processed",
            "BIGQUERY:statistics:total_bytes_processed",
        ]
    );
    assert_eq!(update[BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE], "UPDATE");
    assert_eq!(
        integer(&update, BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS),
        3
    );

    for (sql, statement_type, affected_rows) in [
        (
            format!(
                "MERGE `{table}` AS target USING (SELECT 9 AS value) AS source \
                 ON target.value = source.value \
                 WHEN NOT MATCHED THEN INSERT (value) VALUES (source.value)"
            ),
            "MERGE",
            Some("1"),
        ),
        (
            format!("DELETE FROM `{table}` WHERE value < 9"),
            "DELETE",
            Some("3"),
        ),
        // BigQuery reports a schema for a CREATE VIEW, so "no rows" is not "no columns". DDL
        // affects no rows, and an unreported statistic stays unreported.
        (
            format!("CREATE VIEW `{view}` AS SELECT value FROM `{table}`"),
            "CREATE_VIEW",
            None,
        ),
        (format!("DROP VIEW `{view}`"), "DROP_VIEW", None),
    ] {
        let statistics = statistics(&mut connection, &sql);
        assert_eq!(
            statistics[BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE], statement_type,
            "{sql}"
        );
        assert_eq!(
            statistics
                .get(BIGQUERY_SCHEMA_METADATA_NUM_DML_AFFECTED_ROWS)
                .map(String::as_str),
            affected_rows,
            "{sql}"
        );
    }
}

/// A polled query reports the statistics its response omits. A destination table forces
/// `jobs.insert` and `jobs.getQueryResults`, whose response lacks bytes billed, slot milliseconds,
/// and the statement type.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn polled_results_report_the_statistics_their_response_omits() {
    let table = scratch_table("polled_stats");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset_at("US");
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        table,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CreateDisposition,
        BigQueryCreateDisposition::CreateIfNeeded,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteTruncate,
    );
    // A real table, so the statistics are nonzero.
    statement
        .set_sql_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 100")
        .unwrap();
    let metadata = statement.execute().unwrap().schema().metadata().clone();

    assert_eq!(
        metadata.get(BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE),
        Some(&"SELECT".to_owned())
    );
    assert_harvested_job_statistics(&metadata, "the polled query");
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_nested_and_repeated_query() {
    let mut connection = connect(project());
    let mut statement = prepare_query(
        &mut connection,
        r#"
            SELECT
              [1, 2, 3] AS integers,
              STRUCT('first' AS label, [TRUE, FALSE] AS flags) AS record,
              [STRUCT(10 AS id), STRUCT(20 AS id)] AS records,
              CAST(NULL AS STRUCT<label STRING, flags ARRAY<BOOL>>) AS null_record
            "#,
    );

    let dry_run_schema = statement.execute_schema().unwrap();
    let mut reader = statement.execute().unwrap();
    assert_matches_dry_run(&dry_run_schema, reader.schema().as_ref());
    let batch = reader.next().unwrap().unwrap();

    let integers = list_at(&batch, 0).value(0);
    assert_eq!(typed::<Int64Array>(integers.as_ref()).values(), &[1, 2, 3]);

    let record = struct_at(&batch, 1);
    assert_eq!(str_value(record.column(0)), "first");
    let flags = typed::<ListArray>(record.column(1).as_ref()).value(0);
    let flag_values = typed::<BooleanArray>(flags.as_ref());
    assert!(flag_values.value(0));
    assert!(!flag_values.value(1));

    let records = list_at(&batch, 2).value(0);
    let record_values = typed::<StructArray>(records.as_ref());
    assert_eq!(
        typed::<Int64Array>(record_values.column(0).as_ref()).values(),
        &[10, 20]
    );

    let null_record = struct_at(&batch, 3);
    assert!(null_record.is_null(0));
    assert!(reader.next().is_none());
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn query_result_type_metadata_is_valid_googlesql() {
    let mut connection = connect(project());
    let mut statement = connection.new_statement().unwrap();
    set_option(&mut statement, BigQueryStatementOption::DryRun, "true");
    statement
        .set_sql_query(
            r#"
            SELECT
              42 AS integer_value,
              1.25 AS float_value,
              TRUE AS boolean_value,
              'hello' AS string_value,
              FROM_BASE64('AQID') AS binary_value,
              TIMESTAMP '1970-01-01 00:00:01+00' AS timestamp_value,
              DATE '1970-01-02' AS date_value,
              TIME '00:00:01' AS time_value,
              DATETIME '1970-01-01 00:00:01' AS datetime_value,
              NUMERIC '123.456' AS numeric_value,
              BIGNUMERIC '1.25' AS bignumeric_value,
              INTERVAL 1 DAY AS interval_value,
              ST_GEOGFROMTEXT('POINT(1 2)') AS geography_value,
              JSON '{"a":1}' AS json_value,
              RANGE(DATE '1970-01-01', DATE '1970-01-03') AS range_value,
              [1.25] AS array_value,
              STRUCT(1.25 AS x, [42] AS ys) AS struct_value
            "#,
        )
        .unwrap();

    let schema = statement.execute_schema().unwrap();
    let rich_types = schema
        .fields()
        .iter()
        .map(|field| {
            field
                .metadata()
                .get("BIGQUERY:type")
                .unwrap_or_else(|| panic!("field {} has no type metadata", field.name()))
                .clone()
        })
        .collect::<Vec<_>>();
    // Column for column: a valid type on the wrong column (DATE for DATETIME, say) would pass a
    // membership check and the CAST probe below.
    assert_eq!(
        rich_types,
        [
            "INT64",
            "FLOAT64",
            "BOOL",
            "STRING",
            "BYTES",
            "TIMESTAMP",
            "DATE",
            "TIME",
            "DATETIME",
            "NUMERIC",
            "BIGNUMERIC",
            "INTERVAL",
            "GEOGRAPHY",
            "JSON",
            "RANGE<DATE>",
            "ARRAY<FLOAT64>",
            "STRUCT<`x` FLOAT64, `ys` ARRAY<INT64>>",
        ]
    );

    // Every emitted literal must be usable as a GoogleSQL type, so that a caller can cast to it,
    // rebind it, or repeat it in DDL. BigQuery's REST spellings FLOAT and RECORD are not.
    for rich_type in rich_types {
        let mut probe = connection.new_statement().unwrap();
        set_option(&mut probe, BigQueryStatementOption::DryRun, "true");
        probe
            .set_sql_query(format!("SELECT CAST(NULL AS {rich_type})"))
            .unwrap();
        if let Err(failure) = probe.execute_schema() {
            panic!("BigQuery rejected type metadata {rich_type:?}: {failure}");
        }
    }
}

/// A multi-statement script, as dbt 2.0 sends, returns one result set from its last
/// statement, with statement type `SCRIPT`. `python/tests/test_bigquery.py` covers the DB-API side.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_a_multi_statement_script() {
    let mut connection = connect(project());
    let mut statement = prepare_query(
        &mut connection,
        "DECLARE total INT64 DEFAULT 0; \
         SET total = (SELECT SUM(n) FROM UNNEST(GENERATE_ARRAY(1, 10)) AS n); \
         SELECT total AS total;",
    );

    let (schema, batches) = read_all(&mut statement);
    assert_eq!(
        schema.metadata()[BIGQUERY_SCHEMA_METADATA_STATEMENT_TYPE],
        "SCRIPT"
    );
    assert_eq!(schema.field(0).name(), "total");
    // A script returns the one result its last statement did.
    assert_eq!(int64_column(&batches), [55]);
}

/// A script that commits ends the driver's transaction although it reports `SCRIPT`, so the next
/// statement begins a new transaction that a rollback undoes. A script that only reads leaves the
/// transaction open.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_script_that_commits_ends_the_drivers_transaction() {
    let mut connection = connect(project());
    set_option(
        &mut connection,
        OptionConnection::AutoCommit,
        ADBC_OPTION_VALUE_DISABLED,
    );
    let mut statement = prepare_query(
        &mut connection,
        "CREATE TEMP TABLE script_commit_values (value INT64)",
    );
    statement.execute_update().unwrap();
    for sql in [
        "INSERT script_commit_values VALUES (1); SELECT COUNT(*) FROM script_commit_values",
        "INSERT script_commit_values VALUES (2); COMMIT TRANSACTION; SELECT 1",
        "INSERT script_commit_values VALUES (3)",
    ] {
        statement.set_sql_query(sql).unwrap();
        read_all(&mut statement);
    }
    connection.rollback().unwrap();

    statement
        .set_sql_query("SELECT COUNT(*) FROM script_commit_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 2);
}

/// Narrow rows over REST arrive in pages of 100,000, the first `jobs.query` response included, and
/// follow their page tokens without losing, repeating or reordering a row. A batch never spans
/// pages, so each page boundary shows as a short batch.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_a_rest_result_across_row_capped_pages() {
    const ROWS: i64 = 250_000;
    let mut connection = connect(project());
    let mut statement = prepare_query(
        &mut connection,
        &format!("SELECT n FROM UNNEST(GENERATE_ARRAY(1, {ROWS})) AS n ORDER BY n"),
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        "true",
    );
    let (_, batches) = read_all(&mut statement);

    assert_eq!(int64_column(&batches), (1..=ROWS).collect::<Vec<_>>());
    let mut boundaries = Vec::new();
    let mut read = 0;
    for batch in &batches {
        read += batch.num_rows();
        boundaries.push(read);
    }
    for page_end in [100_000, 200_000] {
        assert!(
            boundaries.contains(&page_end),
            "no batch ends at the page boundary at row {page_end}"
        );
    }
}

/// Text has to come back byte for byte through the REST path's JSON decoding: multi-byte UTF-8,
/// an astral-plane character (a surrogate pair when escaped in JSON), the control characters JSON
/// must escape, a flexible column name, and a value of several megabytes in one cell.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_unicode_control_characters_and_large_values() {
    /// `REPEAT` refuses output over 1 MB, so the large value is aggregated from 1,000-character
    /// pieces instead: 4 MB of two-byte characters.
    const PIECES: usize = 2_000;
    const LARGE_CHARS: usize = PIECES * 1_000;

    let mut connection = connect(project());
    let mut statement = prepare_query(
        &mut connection,
        &format!(
            "SELECT 'naïve café — ✅ 汉字 🐍' AS `名前 text`, \
             CODE_POINTS_TO_STRING([9, 10, 13, 34, 92, 127, 8232]) AS control, \
             (SELECT STRING_AGG(REPEAT('é', 1000), '') \
              FROM UNNEST(GENERATE_ARRAY(1, {PIECES}))) AS large, \
             CAST(NULL AS STRING) AS missing"
        ),
    );
    let batch = single_batch(&mut statement);
    assert_eq!(batch.schema().field(0).name(), "名前 text");
    assert_eq!(str_at(&batch, 0), "naïve café — ✅ 汉字 🐍");
    assert_eq!(str_at(&batch, 1), "\t\n\r\"\\\u{7f}\u{2028}");
    let large = str_at(&batch, 2);
    assert_eq!(large.len(), 2 * LARGE_CHARS, "é is two UTF-8 bytes");
    assert!(large.chars().all(|character| character == 'é'));
    assert!(batch.column(3).is_null(0));
}
