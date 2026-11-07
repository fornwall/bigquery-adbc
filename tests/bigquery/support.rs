//! Connection builders, scratch-resource guards and result accessors shared by every module.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_SCHEMA_METADATA_JOB_KEYS, BIGQUERY_SCHEMA_METADATA_QUERY_ID,
    BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED, BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED, BigQueryConnection, BigQueryDatabase,
    BigQueryDatabaseOption, BigQueryDriver, BigQueryStatement, BigQueryStatementOption,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal32Array, Decimal64Array, Decimal128Array,
    Decimal256Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch,
    RecordBatchReader, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::{ArrowError, DataType, Field, Schema};

pub(crate) use crate::project_config::project;
/// The row count of `bigquery-public-data.samples.shakespeare`.
///
/// An exact count checks that an unqualified table name resolved to this fixture.
pub(crate) const SHAKESPEARE_ROWS: i64 = 164_656;
pub(crate) const SCRATCH_DATASET: &str = "sq_scratch";
/// Cloud Storage bucket for dbt-core 2.0 object-write tests. Each test deletes its own objects.
pub(crate) fn scratch_bucket() -> &'static str {
    static BUCKET: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BUCKET.get_or_init(|| format!("{}-adbc-scratch", project()))
}

pub(crate) fn project_and_dataset_options() -> [(OptionDatabase, OptionValue); 2] {
    [
        (BigQueryDatabaseOption::ProjectId.into(), project().into()),
        (
            BigQueryDatabaseOption::DatasetId.into(),
            SCRATCH_DATASET.into(),
        ),
    ]
}

#[track_caller]
pub(crate) fn open_database(
    options: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
) -> BigQueryDatabase {
    try_open_database(options).unwrap()
}

fn try_open_database(
    options: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
) -> Result<BigQueryDatabase, Error> {
    project();
    BigQueryDriver::try_new()?.new_database_with_opts(
        options
            .into_iter()
            .chain(crate::backend::database_options()?),
    )
}

#[track_caller]
pub(crate) fn connect(project: &str) -> BigQueryConnection {
    open_database([(BigQueryDatabaseOption::ProjectId.into(), project.into())])
        .new_connection()
        .unwrap()
}

#[track_caller]
pub(crate) fn connect_with_dataset() -> BigQueryConnection {
    open_database(project_and_dataset_options())
        .new_connection()
        .unwrap()
}

/// A scratch-dataset connection with a location, for tests that use `jobs.insert`: reconciling or
/// cancelling a job after a lost response needs its region.
#[track_caller]
pub(crate) fn connect_with_dataset_at(location: &str) -> BigQueryConnection {
    open_database(
        project_and_dataset_options()
            .into_iter()
            .chain([(BigQueryDatabaseOption::Location.into(), location.into())]),
    )
    .new_connection()
    .unwrap()
}

#[track_caller]
pub(crate) fn connect_at_location(project: &str, location: &str) -> BigQueryConnection {
    open_database([
        (BigQueryDatabaseOption::ProjectId.into(), project.into()),
        (BigQueryDatabaseOption::Location.into(), location.into()),
    ])
    .new_connection()
    .unwrap()
}

#[track_caller]
pub(crate) fn prepare_query(connection: &mut BigQueryConnection, sql: &str) -> BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement
}

/// Sets one option, panicking on failure.
#[track_caller]
pub(crate) fn set_option<T: Optionable>(
    object: &mut T,
    key: impl Into<T::Option>,
    value: impl Into<OptionValue>,
) {
    object.set_option(key.into(), value.into()).unwrap();
}

/// Reads one option as a string, panicking on failure.
#[track_caller]
pub(crate) fn option_string<T: Optionable>(object: &T, key: impl Into<T::Option>) -> String {
    object.get_option_string(key.into()).unwrap()
}

/// A statement in named-parameter mode carrying `sql`.
#[track_caller]
pub(crate) fn named_parameter_query(
    connection: &mut BigQueryConnection,
    sql: &str,
) -> BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::ParameterMode,
        "named",
    );
    statement.set_sql_query(sql).unwrap();
    statement
}

/// The row count of `table`, a name or dotted path the query resolves.
#[track_caller]
pub(crate) fn count_rows(connection: &mut BigQueryConnection, table: &str) -> i64 {
    let mut statement = prepare_query(connection, &format!("SELECT COUNT(*) FROM `{table}`"));
    read_single_i64(&mut statement)
}

#[track_caller]
pub(crate) fn execute_sql(connection: &mut BigQueryConnection, sql: &str) {
    let mut statement = prepare_query(connection, sql);
    if let Err(error) = statement.execute_update() {
        panic!("{sql}: {error}");
    }
}

/// A unique table name for parallel tests in the scratch dataset.
///
/// The counter prevents collisions between threads that read the same clock tick.
pub(crate) fn scratch_table(prefix: &str) -> String {
    static SEQUENCE: AtomicI64 = AtomicI64::new(0);
    format!(
        "bigquery_adbc_{prefix}_{}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

/// Drops a scratch table or view when the test ends, including during unwinding.
///
/// The guard opens its own connection because the test's connection may still be borrowed.
pub(crate) struct DropTable {
    object: &'static str,
    name: String,
}

impl DropTable {
    /// Drops `name` with `DROP {object} IF EXISTS`, `object` being `VIEW`, `EXTERNAL TABLE`, ...
    pub(crate) fn new(object: &'static str, name: &str) -> Self {
        project();
        Self {
            object,
            name: name.to_owned(),
        }
    }

    pub(crate) fn table(name: &str) -> Self {
        Self::new("TABLE", name)
    }
}

impl Drop for DropTable {
    fn drop(&mut self) {
        let project = project();
        let dropped = || -> Result<(), Error> {
            let mut connection =
                try_open_database(project_and_dataset_options())?.new_connection()?;
            let mut statement = connection.new_statement()?;
            statement.set_sql_query(format!("DROP {} IF EXISTS `{}`", self.object, self.name))?;
            statement.execute_update()?;
            Ok(())
        };
        if let Err(error) = dropped() {
            report_failed_cleanup(
                &format!(
                    "drop {} `{project}.{SCRATCH_DATASET}.{}`",
                    self.object, self.name
                ),
                &error,
            );
        }
    }
}

/// A unique scratch table, the guard that drops it, and a connection to the scratch dataset.
#[track_caller]
pub(crate) fn scratch_fixture(prefix: &str) -> (String, DropTable, BigQueryConnection) {
    let table = scratch_table(prefix);
    let guard = DropTable::table(&table);
    (table, guard, connect_with_dataset())
}

/// Reports a scratch resource that could not be cleaned up.
///
/// Fails the test unless it is already unwinding. During unwinding, prints the leaked resource
/// instead of causing a double panic that would abort the process.
#[expect(
    clippy::print_stderr,
    reason = "a leaked scratch resource has to be named, and a drop guard has no other channel"
)]
pub(crate) fn report_failed_cleanup(what: &str, error: &dyn std::fmt::Display) {
    assert!(std::thread::panicking(), "failed to {what}: {error}");
    eprintln!("leaked: failed to {what} while the test was already failing: {error}");
}

/// Ingests one bound batch into `table` and returns the affected-row count.
#[track_caller]
pub(crate) fn ingest_batch(
    connection: &mut BigQueryConnection,
    table: &str,
    batch: RecordBatch,
) -> Option<i64> {
    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.to_owned());
    ingest.bind(batch).unwrap();
    ingest.execute_update().unwrap()
}

#[track_caller]
pub(crate) fn typed<T: 'static>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "expected a {}, found {}",
            std::any::type_name::<T>(),
            array.data_type()
        )
    })
}

#[track_caller]
pub(crate) fn str_value(array: &ArrayRef) -> &str {
    assert!(array.is_valid(0), "expected a non-null string");
    typed::<StringArray>(array.as_ref()).value(0)
}

#[track_caller]
pub(crate) fn date_value(array: &ArrayRef) -> i32 {
    assert!(array.is_valid(0), "expected a non-null date");
    typed::<Date32Array>(array.as_ref()).value(0)
}

#[track_caller]
pub(crate) fn ts_value(array: &ArrayRef) -> i64 {
    assert!(array.is_valid(0), "expected a non-null timestamp");
    typed::<TimestampMicrosecondArray>(array.as_ref()).value(0)
}

#[track_caller]
pub(crate) fn i64_at(batch: &RecordBatch, column: usize) -> i64 {
    assert!(
        batch.column(column).is_valid(0),
        "expected a non-null integer"
    );
    typed::<Int64Array>(batch.column(column).as_ref()).value(0)
}

#[track_caller]
pub(crate) fn str_at(batch: &RecordBatch, column: usize) -> &str {
    str_value(batch.column(column))
}

#[track_caller]
pub(crate) fn bool_at(batch: &RecordBatch, column: usize) -> bool {
    assert!(
        batch.column(column).is_valid(0),
        "expected a non-null boolean"
    );
    typed::<BooleanArray>(batch.column(column).as_ref()).value(0)
}

#[track_caller]
pub(crate) fn date_at(batch: &RecordBatch, column: usize) -> i32 {
    date_value(batch.column(column))
}

#[track_caller]
pub(crate) fn ts_at(batch: &RecordBatch, column: usize) -> i64 {
    ts_value(batch.column(column))
}

#[track_caller]
pub(crate) fn interval_at(batch: &RecordBatch, column: usize) -> IntervalMonthDayNano {
    assert!(
        batch.column(column).is_valid(0),
        "expected a non-null interval"
    );
    typed::<IntervalMonthDayNanoArray>(batch.column(column).as_ref()).value(0)
}

#[track_caller]
pub(crate) fn struct_at(batch: &RecordBatch, column: usize) -> &StructArray {
    typed::<StructArray>(batch.column(column).as_ref())
}

#[track_caller]
pub(crate) fn list_at(batch: &RecordBatch, column: usize) -> &ListArray {
    typed::<ListArray>(batch.column(column).as_ref())
}

/// The struct items of a list column's first row.
#[track_caller]
pub(crate) fn nested_struct(array: &ArrayRef) -> StructArray {
    assert!(array.is_valid(0), "expected a non-null list");
    typed::<StructArray>(typed::<ListArray>(array.as_ref()).value(0).as_ref()).clone()
}

/// The struct items of a list column, flattened across every row.
#[track_caller]
pub(crate) fn all_struct_values(array: &ArrayRef) -> StructArray {
    typed::<StructArray>(typed::<ListArray>(array.as_ref()).values().as_ref()).clone()
}

/// Executes `statement` and returns its single batch, asserting the reader is then exhausted.
#[track_caller]
pub(crate) fn single_batch(statement: &mut impl Statement) -> RecordBatch {
    let mut reader = statement.execute().unwrap();
    let batch = reader.next().expect("the result has no batch").unwrap();
    assert!(
        reader.next().is_none(),
        "the result has more than one batch"
    );
    batch
}

#[track_caller]
pub(crate) fn read_single_string(statement: &mut impl Statement) -> String {
    let batch = single_batch(statement);
    assert_eq!(batch.num_rows(), 1, "the result is not a single row");
    str_at(&batch, 0).to_owned()
}

#[track_caller]
pub(crate) fn read_single_i64(statement: &mut impl Statement) -> i64 {
    let batch = single_batch(statement);
    assert_eq!(batch.num_rows(), 1, "the result is not a single row");
    i64_at(&batch, 0)
}

#[test]
fn scalar_assertions_reject_values_hidden_by_the_null_mask() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use arrow_buffer::NullBuffer;

    let nulls = Some(NullBuffer::new_null(1));
    let (offsets, values, _) = StringArray::from(vec!["hidden"]).into_parts();
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "string",
            Arc::new(StringArray::new(offsets, values, nulls.clone())),
        ),
        (
            "integer",
            Arc::new(Int64Array::new(vec![42].into(), nulls.clone())),
        ),
        (
            "boolean",
            Arc::new(BooleanArray::new(vec![true].into(), nulls.clone())),
        ),
        (
            "date",
            Arc::new(Date32Array::new(vec![42].into(), nulls.clone())),
        ),
        (
            "timestamp",
            Arc::new(TimestampMicrosecondArray::new(
                vec![42].into(),
                nulls.clone(),
            )),
        ),
        (
            "interval",
            Arc::new(IntervalMonthDayNanoArray::new(
                vec![IntervalMonthDayNano::new(1, 2, 3)].into(),
                nulls,
            )),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let checks: [fn(&RecordBatch); 6] = [
        |batch| {
            str_at(batch, 0);
        },
        |batch| {
            i64_at(batch, 1);
        },
        |batch| {
            bool_at(batch, 2);
        },
        |batch| {
            date_at(batch, 3);
        },
        |batch| {
            ts_at(batch, 4);
        },
        |batch| {
            interval_at(batch, 5);
        },
    ];
    for (index, check) in checks.into_iter().enumerate() {
        assert!(
            catch_unwind(AssertUnwindSafe(|| check(&batch))).is_err(),
            "{} assertion accepted a null cell",
            batch.schema().field(index).name(),
        );
    }
}

/// Checks that execution returns the fields described by its dry run.
///
/// Job metadata differs: execution reports its job ID and actual usage; a dry run estimates usage.
#[track_caller]
pub(crate) fn assert_matches_dry_run(dry_run_schema: &Schema, executed: &Schema) {
    assert!(
        dry_run_schema
            .metadata()
            .contains_key(BIGQUERY_SCHEMA_METADATA_QUERY_TOTAL_BYTES_PROCESSED),
        "a dry run must report the bytes the query would process"
    );
    assert_eq!(
        schema_without_job_metadata(executed),
        schema_fields_only(dry_run_schema)
    );
}

/// Checks that billed bytes and slot milliseconds are positive.
///
/// Callers query stored tables, so zero would indicate missing usage statistics.
#[track_caller]
pub(crate) fn assert_harvested_job_statistics(metadata: &arrow_schema::Metadata, what: &str) {
    for key in [
        BIGQUERY_SCHEMA_METADATA_TOTAL_BYTES_BILLED,
        BIGQUERY_SCHEMA_METADATA_SLOT_MILLIS,
    ] {
        let value = metadata
            .get(key)
            .unwrap_or_else(|| panic!("{what}: result schema is missing {key}"));
        let value: i64 = value
            .parse()
            .unwrap_or_else(|_| panic!("{what}: {key} is {value:?}, not a decimal integer"));
        assert!(
            value > 0,
            "{what}: {key} is {value}, but the query read a table"
        );
    }
}

/// Returns an immediate execution error or the first stream error.
///
/// Job failures can arrive after `execute` returns. Preserves embedded ADBC errors and maps
/// other Arrow errors to `Status::Unknown` with their original message.
pub(crate) fn outcome<R>(result: Result<R, Error>) -> Result<(), Error>
where
    R: Iterator<Item = Result<RecordBatch, ArrowError>>,
{
    for item in result? {
        match item {
            Ok(_) => {}
            Err(ArrowError::ExternalError(source)) => {
                return Err(match source.downcast::<Error>() {
                    Ok(error) => *error,
                    Err(source) => {
                        Error::with_message_and_status(source.to_string(), Status::Unknown)
                    }
                });
            }
            Err(source) => {
                return Err(Error::with_message_and_status(
                    source.to_string(),
                    Status::Unknown,
                ));
            }
        }
    }
    Ok(())
}

/// The ADBC status and message of a statement that must fail, at `execute` or on its stream.
#[track_caller]
pub(crate) fn execute_failure(statement: &mut impl Statement) -> (Status, String) {
    let error = outcome(statement.execute()).expect_err("the statement was expected to fail");
    (error.status, error.message)
}

/// The total row count of `batches`.
pub(crate) fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

pub(crate) fn decimal_domain_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal32", DataType::Decimal32(3, 2), false),
        Field::new("negative_scale", DataType::Decimal64(2, -3), false),
        Field::new("decimal128", DataType::Decimal128(38, 20), false),
        Field::new("decimal256", DataType::Decimal256(38, 9), false),
        Field::new("explicit_bignumeric", DataType::Decimal256(38, 9), false).with_metadata(
            HashMap::from([("BIGQUERY:type".to_owned(), "BIGNUMERIC".to_owned())]),
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                Decimal32Array::from(vec![123])
                    .with_precision_and_scale(3, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal64Array::from(vec![-12])
                    .with_precision_and_scale(2, -3)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![123])
                    .with_precision_and_scale(38, 20)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(123)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(123)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

#[track_caller]
pub(crate) fn assert_decimal_domain_result(batch: &RecordBatch) {
    let values = (0..batch.num_columns())
        .map(|column| str_at(batch, column))
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [
            "1.23",
            "-12000",
            "0.00000000000000000123",
            "0.000000123",
            "0.000000123",
            "NUMERIC",
            "NUMERIC",
            "BIGNUMERIC",
            "NUMERIC",
            "BIGNUMERIC",
        ]
    );
}

/// BIGNUMERIC's largest and smallest values: `i256::MAX` and `i256::MIN` at scale 38, one
/// significant digit more than `Decimal256(76, 38)` can declare.
pub(crate) const BIGNUMERIC_MAX: &str =
    "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
/// See [`BIGNUMERIC_MAX`].
pub(crate) const BIGNUMERIC_MIN: &str =
    "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";

/// One row holding BIGNUMERIC's extremes, as columns `largest` and `smallest`.
pub(crate) fn bignumeric_extremes_batch() -> RecordBatch {
    let bignumeric = |value: i256| {
        Arc::new(
            Decimal256Array::from(vec![value])
                .with_precision_and_scale(76, 38)
                .unwrap(),
        ) as ArrayRef
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("largest", DataType::Decimal256(76, 38), false),
        Field::new("smallest", DataType::Decimal256(76, 38), false),
    ]));
    RecordBatch::try_new(schema, vec![bignumeric(i256::MAX), bignumeric(i256::MIN)]).unwrap()
}

/// The select [`assert_bignumeric_extremes`] checks, over the expressions holding the extremes.
pub(crate) fn bignumeric_extremes_select(largest: &str, smallest: &str) -> String {
    format!(
        "SELECT CAST({largest} AS STRING), CAST({smallest} AS STRING), TYPEOF({largest}), \
         {largest} = BIGNUMERIC '{BIGNUMERIC_MAX}', {smallest} = BIGNUMERIC '{BIGNUMERIC_MIN}'"
    )
}

#[track_caller]
pub(crate) fn assert_bignumeric_extremes(batch: &RecordBatch) {
    assert_eq!(str_at(batch, 0), BIGNUMERIC_MAX);
    assert_eq!(str_at(batch, 1), BIGNUMERIC_MIN);
    assert_eq!(str_at(batch, 2), "BIGNUMERIC");
    // The equality checks hold even if BigQuery's STRING spelling differs from the driver's.
    assert!(bool_at(batch, 3));
    assert!(bool_at(batch, 4));
}

#[track_caller]
pub(crate) fn read_all(statement: &mut impl Statement) -> (Arc<Schema>, Vec<RecordBatch>) {
    let reader = statement.execute().unwrap();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, ArrowError>>().unwrap();
    (schema, batches)
}

#[track_caller]
pub(crate) fn concatenated(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<ArrayRef> {
    (0..schema.fields().len())
        .map(|column| {
            let arrays = batches
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).unwrap()
        })
        .collect()
}

/// Asserts `schema` reports a non-empty `BIGQUERY:query_id` and returns a copy without any of the
/// per-job metadata, which two reads of the same query legitimately differ in.
#[track_caller]
pub(crate) fn schema_without_job_metadata(schema: &Schema) -> Schema {
    let query_id = schema
        .metadata()
        .get(BIGQUERY_SCHEMA_METADATA_QUERY_ID)
        .expect("result schema is missing BIGQUERY:query_id");
    assert!(!query_id.is_empty(), "BIGQUERY:query_id is empty");
    schema_fields_only(schema)
}

/// `schema` with every per-job metadata key removed.
fn schema_fields_only(schema: &Schema) -> Schema {
    let mut metadata = schema.metadata().clone();
    for key in BIGQUERY_SCHEMA_METADATA_JOB_KEYS {
        metadata.remove(key);
    }
    Schema::new_with_metadata(schema.fields().clone(), metadata)
}
