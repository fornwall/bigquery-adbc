use std::sync::Weak;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Metadata, TimeUnit};

use super::*;
use crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID;
use crate::cancellation::operation_reader;
use crate::query::empty_reader;
use crate::test_support::driver::test_statement;

fn operation() -> Operation {
    test_statement().cancellation.begin()
}

fn int64_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn int64_batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(int64_schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn parameter_rows(
    batches: impl Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send + 'static,
    schema: SchemaRef,
) -> ParameterRows {
    let reader = RecordBatchIterator::new(batches, schema);
    ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap()
}

/// `batch` as a stream that checks, at the read after it, that nothing still retains `array`.
fn released_before_next_read(batch: RecordBatch, array: Weak<Int64Array>) -> ParameterRows {
    let schema = batch.schema();
    let mut first = Some(batch);
    let batches = std::iter::from_fn(move || {
        if let Some(batch) = first.take() {
            return Some(Ok(batch));
        }
        assert_eq!(array.strong_count(), 0, "retained a batch at the next read");
        None
    });
    parameter_rows(batches, schema)
}

/// The reader `execute` hands out, partway through `rows` and reading `current`.
fn lazy_reader(
    rows: ParameterRows,
    current: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send> {
    let statement = test_statement();
    let operation = statement.cancellation.begin();
    lazy_reader_under(statement, operation, rows, current, schema)
}

fn lazy_reader_under(
    statement: crate::BigQueryStatement,
    operation: Operation,
    rows: ParameterRows,
    current: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send> {
    let reader = ParameterizedRecordBatchReader {
        context: statement.context,
        query_options: statement.query_options,
        transaction: statement.transaction,
        sql: "SELECT 1".into(),
        parameter_mode: ParameterMode::Positional,
        rows,
        current,
        schema,
        operation: operation.clone(),
    };
    operation_reader(Box::new(reader), operation)
}

fn value(parameters: &[QueryParameter]) -> &str {
    let value = parameters[0].parameter_value.as_ref().unwrap();
    value.value.as_deref().unwrap()
}

#[test]
fn bound_parameter_batches_release_their_buffers_after_the_last_row() {
    let operation = operation();
    // A short slice can retain a large imported allocation while its remote query runs.
    let array = Arc::new(Int64Array::from(vec![7; 1024 * 1024]).slice(0, 2));
    let retained = Arc::downgrade(&array);
    let retained_bytes = || {
        retained
            .upgrade()
            .map_or(0, |array| array.get_buffer_memory_size())
    };
    let batch = RecordBatch::try_from_iter([("value", array as ArrayRef)]).unwrap();
    let mut rows = released_before_next_read(batch, retained.clone());

    assert!(rows.has_next(&operation).unwrap());
    assert_eq!(retained_bytes(), 8 * 1024 * 1024);
    rows.next(&operation).unwrap().unwrap();
    assert_eq!(retained_bytes(), 8 * 1024 * 1024);
    let parameters = rows.next(&operation).unwrap().unwrap();
    assert_eq!(retained_bytes(), 0);
    assert_eq!(value(&parameters), "7");
    assert!(!rows.has_next(&operation).unwrap());
    assert!(rows.next(&operation).unwrap().is_none());
}

#[test]
fn empty_bound_parameter_batches_are_dropped_before_reading_again() {
    let array = Arc::new(Int64Array::from(vec![1; 1024]).slice(0, 0));
    let retained = Arc::downgrade(&array);
    let batch = RecordBatch::try_from_iter([("value", array as ArrayRef)]).unwrap();
    let mut rows = released_before_next_read(batch, retained);

    assert!(rows.next(&operation()).unwrap().is_none());
}

#[test]
fn cancellation_stops_a_bound_stream_between_empty_batches() {
    let operation = operation();
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(schema.clone());
    let cancel = operation.clone();
    let batches = std::iter::from_fn(move || {
        assert!(!cancel.is_cancelled(), "read parameters after cancellation");
        cancel.finish();
        Some(Ok(batch.clone()))
    });
    let mut rows = parameter_rows(batches, schema);

    let failure = rows.next(&operation).unwrap_err();
    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(
        failure.message,
        "BigQuery statement operation was cancelled"
    );
}

#[test]
fn cancellation_after_reading_parameters_is_not_reported_as_successful_eof() {
    let statement = test_statement();
    let operation = statement.cancellation.begin();
    let cancel = operation.clone();
    let batches = std::iter::from_fn(move || {
        cancel.finish();
        None
    });
    let schema = Arc::new(Schema::empty());
    let rows = parameter_rows(batches, schema.clone());
    let current = empty_reader(schema.clone());
    let mut reader = lazy_reader_under(statement, operation, rows, current, schema);

    let ArrowError::ExternalError(failure) = reader.next().unwrap().unwrap_err() else {
        panic!("expected the cancellation error");
    };
    assert_eq!(
        failure.downcast_ref::<Error>().unwrap().status,
        Status::Cancelled
    );
    assert!(reader.next().is_none());
}

#[test]
fn cancellation_does_not_consume_another_bound_parameter_row() {
    let operation = operation();
    let mut rows = parameter_rows(std::iter::once(Ok(int64_batch(vec![1, 2]))), int64_schema());
    rows.next(&operation).unwrap().unwrap();
    operation.finish();

    assert_eq!(rows.next(&operation).unwrap_err().status, Status::Cancelled);
    assert_eq!(rows.yielded, 1);
}

#[test]
fn result_stream_failures_identify_prior_parameter_executions_and_keep_details() {
    for (yielded, message) in [
        (1, "page request failed"),
        (
            2,
            "bound BigQuery parameter row 1 failed after 1 earlier row had already executed: page request failed",
        ),
        (
            3,
            "bound BigQuery parameter row 2 failed after 2 earlier rows had already executed: page request failed",
        ),
    ] {
        let schema = Arc::new(Schema::empty());
        let mut rows = parameter_rows(
            std::iter::from_fn(|| panic!("read parameters after a result stream failure")),
            schema.clone(),
        );
        rows.yielded = yielded;
        let mut failure = Error::with_message_and_status("page request failed", Status::Timeout);
        failure.vendor_code = 504;
        failure.sqlstate = (*b"HYT00").map(|byte| byte.try_into().unwrap());
        failure.details = Some(vec![("job_id".to_owned(), b"later-job".to_vec())]);
        let current = RecordBatchIterator::new(
            [Err(ArrowError::ExternalError(Box::new(failure)))],
            schema.clone(),
        );
        let mut reader = lazy_reader(rows, Box::new(current), schema);

        let ArrowError::ExternalError(source) = reader.next().unwrap().unwrap_err() else {
            panic!("expected the original ADBC error");
        };
        let failure = source.downcast_ref::<Error>().unwrap();
        assert_eq!(failure.message, message);
        assert_eq!(failure.status, Status::Timeout);
        assert_eq!(failure.vendor_code, 504);
        assert_eq!(failure.sqlstate, [72, 89, 84, 48, 48]);
        assert_eq!(
            failure.details,
            Some(vec![("job_id".to_owned(), b"later-job".to_vec())])
        );
        assert!(reader.next().is_none());
    }
}

#[test]
fn replacing_per_job_metadata_preserves_zero_column_rows() {
    let schema = |job: &str| {
        Arc::new(Schema::empty().with_metadata(Metadata::from_iter([(
            BIGQUERY_SCHEMA_METADATA_QUERY_ID,
            job,
        )])))
    };
    let first = schema("first");
    let later = schema("later");
    let batch = RecordBatch::try_new_with_options(
        later.clone(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let rows = parameter_rows(std::iter::empty(), Arc::new(Schema::empty()));
    let current = RecordBatchIterator::new(std::iter::once(Ok(batch)), later);
    let mut reader = lazy_reader(rows, Box::new(current), first.clone());

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema(), first);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 0);
    assert!(reader.next().is_none());
}

/// Every job's schema metadata differs, but a field's, like its name, type and nullability, is
/// part of the result.
#[test]
fn result_schemas_are_compared_by_their_fields_alone() {
    let field = Field::new("value", DataType::Int64, true);
    let schema = |field: &Field| Schema::new(vec![field.clone()]);
    let job = Metadata::from_iter([(BIGQUERY_SCHEMA_METADATA_QUERY_ID, "first")]);
    let with_job = schema(&field).with_metadata(job.clone());
    require_same_result_schema(&schema(&field), &with_job).unwrap();
    require_same_result_schema(&with_job, &schema(&field)).unwrap();

    for different in [
        field.clone().with_name("other"),
        field.clone().with_data_type(DataType::Utf8),
        field.clone().with_nullable(false),
        field.clone().with_metadata(job),
    ] {
        let (expected, found) = (schema(&field), schema(&different));
        require_same_result_schema(&expected, &found).unwrap_err();
        require_same_result_schema(&found, &expected).unwrap_err();
    }
}

/// A stream that fails mid-way says how far it got, since every row before it already executed.
#[test]
fn a_failing_parameter_stream_says_how_many_rows_already_executed() {
    let operation = operation();
    let batches = vec![
        Ok(int64_batch(vec![1, 2])),
        Err(ArrowError::ComputeError("source went away".to_owned())),
    ];
    let mut rows = parameter_rows(batches.into_iter(), int64_schema());
    rows.next(&operation).unwrap().unwrap();
    rows.next(&operation).unwrap().unwrap();
    let failure = rows.has_next(&operation).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter row 2 failed after 2 earlier rows had already executed: failed \
         to read the bound BigQuery parameter stream: Compute error: source went away"
    );
}

/// Arrow cannot build an array of every type a schema can name, such as a `Time32(Microsecond)`,
/// and would panic rather than fail. Such a stream is refused before any batch is read.
#[test]
fn a_stream_of_a_type_arrow_cannot_build_is_refused_before_it_is_read() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Time32(TimeUnit::Microsecond),
        true,
    )]));
    let reader = RecordBatchIterator::new(std::iter::empty(), schema);
    let Err(failure) = ParameterRows::new(Box::new(reader), ParameterMode::Positional) else {
        panic!("a Time32(Microsecond) parameter stream was accepted");
    };
    assert_eq!(failure.status, Status::NotImplemented);
    assert!(failure.message.contains("Time32"), "{}", failure.message);
}

#[test]
fn bound_stream_rows_become_parameter_sets() {
    let operation = operation();
    let batches = [vec![], vec![1, 2], vec![3]].map(|values| Ok(int64_batch(values)));
    let mut rows = parameter_rows(batches.into_iter(), int64_schema());

    let mut values = Vec::new();
    while let Some(parameters) = rows.next(&operation).unwrap() {
        values.push(value(&parameters).to_owned());
    }
    assert_eq!(values, ["1", "2", "3"]);
}

/// A bound stream's rows are numbered across all of its batches, so a failure names a row the
/// caller can find and the rows before it, which already ran; the first row's failure is its own.
#[test]
fn parameter_row_failures_are_numbered_across_the_whole_stream() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let batch = |values: Vec<u64>| {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(values))]).unwrap()
    };
    let batches = [batch(vec![1, 2]), batch(vec![u64::MAX])].map(Ok);
    let operation = operation();
    let mut rows = parameter_rows(batches.into_iter(), schema.clone());

    rows.next(&operation).unwrap().unwrap();
    let failure = rows.failed_row(Error::with_message_and_status("boom", Status::IO));
    assert_eq!(failure.message, "boom");
    let mut executed = 1;
    let failure = rows
        .for_each(&operation, |_| {
            executed += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(executed, 2);
    assert_eq!(failure.status, Status::InvalidData);
    assert!(
        failure.message.starts_with(
            "bound BigQuery parameter row 2 failed after 2 earlier rows had already executed: \
             bound BigQuery parameter 0 \"value\" at row 2 is an Arrow UInt64"
        ),
        "{failure}"
    );
    let failure = rows.failed_row(Error::with_message_and_status("boom", Status::IO));
    assert_eq!(failure.status, Status::IO);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter row 1 failed after 1 earlier row had already executed: boom"
    );
}

/// The parameter plan is built from the stream's declared schema, so a batch that does not match
/// it is rejected as it arrives.
#[test]
fn a_bound_batch_that_diverges_from_the_stream_schema_names_both_schemas() {
    let divergent = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from(vec!["text"])) as ArrayRef,
    )])
    .unwrap();
    let mut rows = parameter_rows(std::iter::once(Ok(divergent)), int64_schema());

    let failure = rows.next(&operation()).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "a bound BigQuery parameter stream returned a batch whose schema differs from the one it \
         declared: expected (value: Int64), got (value: Utf8); every batch of bound parameters \
         must match the stream schema"
    );
}

/// A parameter row whose result schema differs is reported with both schemas, the rule, and the
/// rows that already ran.
#[test]
fn a_divergent_parameter_result_schema_is_described_in_full() {
    let mut rows = parameter_rows(
        std::iter::once(Ok(int64_batch(vec![1, 2, 3]))),
        int64_schema(),
    );
    let expected = Schema::new(vec![Field::new("answer", DataType::Int64, true)]);
    let found = Schema::new(vec![Field::new("answer", DataType::Utf8, true)]);
    let mut executed = 0;
    let failure = rows
        .for_each(&operation(), |_| {
            executed += 1;
            if executed == 3 {
                return require_same_result_schema(&expected, &found);
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter row 2 failed after 2 earlier rows had already executed: its \
         result schema differs from that of parameter row 0: expected (answer: Int64), got \
         (answer: Utf8); every parameter row bound to one execute must yield the same result \
         schema"
    );
}

#[test]
fn add_rows_affected_treats_negative_counts_and_overflow_as_unknown() {
    assert_eq!(add_rows_affected(Some(2), Some(3)), Some(5));
    assert_eq!(add_rows_affected(Some(2), Some(0)), Some(2));
    for unknown in [None, Some(-1), Some(i64::MIN)] {
        assert_eq!(add_rows_affected(Some(2), unknown), None);
        assert_eq!(add_rows_affected(unknown, Some(3)), None);
    }
    assert_eq!(add_rows_affected(Some(i64::MAX), Some(1)), None);
}
