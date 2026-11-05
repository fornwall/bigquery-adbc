use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchIterator};
use arrow_schema::{DataType, Field, Metadata};

use super::*;

#[test]
fn bound_parameter_batches_release_their_buffers_after_the_last_row() {
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    // A short slice can retain a large imported allocation while its remote query runs.
    let array = Arc::new(Int64Array::from(vec![7; 1024 * 1024]).slice(0, 2));
    let retained = Arc::downgrade(&array);
    let retained_bytes = || {
        retained
            .upgrade()
            .map_or(0, |array| array.get_buffer_memory_size())
    };
    let batch = RecordBatch::try_from_iter([("value", array as ArrayRef)]).unwrap();
    let schema = batch.schema();
    let mut first = Some(batch);
    let observed = retained.clone();
    let batches = std::iter::from_fn(move || {
        if let Some(batch) = first.take() {
            return Some(Ok(batch));
        }
        assert_eq!(
            observed.strong_count(),
            0,
            "retained the previous batch at the next read"
        );
        None
    });
    let reader = RecordBatchIterator::new(batches, schema);
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

    assert!(rows.has_next(&operation).unwrap());
    assert_eq!(retained_bytes(), 8 * 1024 * 1024);
    rows.next(&operation).unwrap().unwrap();
    assert_eq!(retained_bytes(), 8 * 1024 * 1024);
    let parameters = rows.next(&operation).unwrap().unwrap();
    assert_eq!(retained_bytes(), 0);
    assert_eq!(
        parameters[0]
            .parameter_value
            .as_ref()
            .unwrap()
            .value
            .as_deref(),
        Some("7")
    );
    assert!(!rows.has_next(&operation).unwrap());
    assert!(rows.next(&operation).unwrap().is_none());
}

#[test]
fn empty_bound_parameter_batches_are_dropped_before_reading_again() {
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let array = Arc::new(Int64Array::from(vec![1; 1024]).slice(0, 0));
    let retained = Arc::downgrade(&array);
    let batch = RecordBatch::try_from_iter([("value", array as ArrayRef)]).unwrap();
    let schema = batch.schema();
    let mut first = Some(batch);
    let batches = std::iter::from_fn(move || {
        if let Some(batch) = first.take() {
            return Some(Ok(batch));
        }
        assert_eq!(
            retained.strong_count(),
            0,
            "retained an empty input batch at the next read"
        );
        None
    });
    let reader = RecordBatchIterator::new(batches, schema);
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

    assert!(rows.next(&operation).unwrap().is_none());
}

#[test]
fn cancellation_stops_a_bound_stream_between_empty_batches() {
    let statement = crate::test_support::driver::test_statement();
    let operation = statement.cancellation.begin();
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(schema.clone());
    let cancel = operation.clone();
    let batches = std::iter::from_fn(move || {
        assert!(!cancel.is_cancelled(), "read parameters after cancellation");
        cancel.finish();
        Some(Ok(batch.clone()))
    });
    let reader = RecordBatchIterator::new(batches, schema);
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

    let failure = rows.next(&operation).unwrap_err();
    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(
        failure.message,
        "BigQuery statement operation was cancelled"
    );
}

#[test]
fn cancellation_after_reading_parameters_is_not_reported_as_successful_eof() {
    let statement = crate::test_support::driver::test_statement();
    let operation = statement.cancellation.begin();
    let cancel = operation.clone();
    let batches = std::iter::from_fn(move || {
        cancel.finish();
        None
    });
    let schema = Arc::new(Schema::empty());
    let reader = RecordBatchIterator::new(batches, schema.clone());
    let rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();
    let mut reader = ParameterizedRecordBatchReader {
        context: statement.context,
        query_options: statement.query_options,
        transaction: statement.transaction,
        sql: "SELECT 1".into(),
        parameter_mode: ParameterMode::Positional,
        rows,
        current: crate::query::empty_reader(schema.clone()),
        schema,
        failed: false,
        operation,
    };

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
    let statement = crate::test_support::driver::test_statement();
    let operation = statement.cancellation.begin();
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let reader = RecordBatchIterator::new([Ok(batch)], schema);
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();
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
        let statement = crate::test_support::driver::test_statement();
        let operation = statement.cancellation.begin();
        let schema = Arc::new(Schema::empty());
        let mut rows = ParameterRows::new(
            crate::query::empty_reader(schema.clone()),
            ParameterMode::Positional,
        )
        .unwrap();
        rows.yielded = yielded;
        let mut failure = Error::with_message_and_status("page request failed", Status::Timeout);
        failure.vendor_code = 504;
        failure.sqlstate = (*b"HYT00").map(|byte| byte.try_into().unwrap());
        failure.details = Some(vec![("job_id".to_owned(), b"later-job".to_vec())]);
        let current = RecordBatchIterator::new(
            [Err(ArrowError::ExternalError(Box::new(failure)))],
            schema.clone(),
        );
        let mut reader = ParameterizedRecordBatchReader {
            context: statement.context,
            query_options: statement.query_options,
            transaction: statement.transaction,
            sql: "SELECT 1".into(),
            parameter_mode: ParameterMode::Positional,
            rows,
            current: Box::new(current),
            schema,
            failed: false,
            operation,
        };

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
        Arc::new(
            Schema::empty().with_metadata(std::collections::HashMap::from([(
                crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID.to_owned(),
                job.to_owned(),
            )])),
        )
    };
    let first = schema("first");
    let later = schema("later");
    let batch = RecordBatch::try_new_with_options(
        later.clone(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let statement = crate::test_support::driver::test_statement();
    let operation = statement.cancellation.begin();
    let rows = ParameterRows::new(
        Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            Arc::new(Schema::empty()),
        )),
        ParameterMode::Positional,
    )
    .unwrap();
    let mut reader = ParameterizedRecordBatchReader {
        context: statement.context,
        query_options: statement.query_options,
        transaction: statement.transaction,
        sql: "SELECT 1".into(),
        parameter_mode: ParameterMode::Positional,
        rows,
        current: Box::new(RecordBatchIterator::new(std::iter::once(Ok(batch)), later)),
        schema: first.clone(),
        failed: false,
        operation,
    };
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema(), first);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 0);
    assert!(reader.next().is_none());
}

#[test]
fn schema_comparison_ignores_only_top_level_job_metadata() {
    let fields = vec![Field::new("value", DataType::Int64, true)];
    let schema = |metadata: &[(&str, &str)]| {
        Arc::new(Schema::new_with_metadata(
            fields.clone(),
            metadata.iter().copied().collect::<Metadata>(),
        ))
    };
    let plain = schema(&[]);
    for key in crate::BIGQUERY_SCHEMA_METADATA_JOB_KEYS {
        let first = schema(&[(key, "first")]);
        let second = schema(&[(key, "second")]);
        assert!(result_schemas_match(&first, &second));
        assert!(result_schemas_match(&first, &plain));
        assert!(result_schemas_match(&plain, &first));
    }

    let custom = schema(&[("BIGQUERY:custom", "first")]);
    assert!(result_schemas_match(
        &custom,
        &schema(&[("BIGQUERY:custom", "first")])
    ));
    for other in [
        plain,
        schema(&[("BIGQUERY:custom", "second")]),
        schema(&[("other", "first")]),
    ] {
        assert!(!result_schemas_match(&custom, &other));
        assert!(!result_schemas_match(&other, &custom));
    }
}

#[test]
fn schema_comparison_preserves_field_names_types_nullability_and_metadata() {
    let field = Field::new("value", DataType::Int64, true);
    let expected = Arc::new(Schema::new(vec![field.clone()]));
    for different in [
        field.clone().with_name("other"),
        field.clone().with_data_type(DataType::Utf8),
        field.clone().with_nullable(false),
        field.with_metadata(std::collections::HashMap::from([(
            crate::BIGQUERY_SCHEMA_METADATA_QUERY_ID.to_owned(),
            "field metadata is significant".to_owned(),
        )])),
    ] {
        let found = Arc::new(Schema::new(vec![different]));
        assert!(!result_schemas_match(&expected, &found));
        assert!(!result_schemas_match(&found, &expected));
    }
}

/// A stream that fails mid-way says how far it got, since every row before it already executed.
#[test]
fn a_failing_parameter_stream_says_how_many_rows_already_executed() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )
    .unwrap();
    let batches = vec![
        Ok(batch),
        Err(ArrowError::ComputeError("source went away".to_owned())),
    ];
    let reader = RecordBatchIterator::new(batches, schema);
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();
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
/// and would panic rather than fail. Such a stream is refused when it is bound, before any batch
/// is read.
#[test]
fn a_stream_of_a_type_arrow_cannot_build_is_refused_when_bound() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Time32(arrow_schema::TimeUnit::Microsecond),
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batches = vec![
        RecordBatch::new_empty(schema.clone()),
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        )
        .unwrap(),
    ];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

    let mut values = Vec::new();
    while let Some(parameters) = rows.next(&operation).unwrap() {
        values.push(
            parameters[0]
                .parameter_value
                .as_ref()
                .unwrap()
                .value
                .as_deref()
                .unwrap()
                .to_owned(),
        );
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
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::UInt64Array::from(values)) as ArrayRef],
        )
        .unwrap()
    };
    let batches = vec![batch(vec![1, 2]), batch(vec![u64::MAX])];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

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
    let declared = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let divergent = RecordBatch::try_from_iter([(
        "value",
        Arc::new(arrow_array::StringArray::from(vec!["text"])) as ArrayRef,
    )])
    .unwrap();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(divergent)), declared);
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();

    let failure = rows.next(&operation).unwrap_err();
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);
    let operation = crate::test_support::driver::test_statement()
        .cancellation
        .begin();
    let mut rows = ParameterRows::new(Box::new(reader), ParameterMode::Positional).unwrap();
    let expected = Schema::new(vec![Field::new("answer", DataType::Int64, true)]);
    let found = Schema::new(vec![Field::new("answer", DataType::Utf8, true)]);
    let mut executed = 0;
    let failure = rows
        .for_each(&operation, |_| {
            executed += 1;
            if executed == 3 {
                return Err(divergent_parameter_schema(&expected, &found));
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
fn add_rows_affected_treats_negative_counts_as_unknown() {
    assert_eq!(add_rows_affected(Some(2), Some(3)).unwrap(), Some(5));
    assert_eq!(add_rows_affected(Some(2), Some(0)).unwrap(), Some(2));
    for unknown in [None, Some(-1), Some(i64::MIN)] {
        assert_eq!(add_rows_affected(Some(2), unknown).unwrap(), None);
        assert_eq!(add_rows_affected(unknown, Some(3)).unwrap(), None);
    }
    let overflow = add_rows_affected(Some(i64::MAX), Some(1)).unwrap_err();
    assert_eq!(overflow.status, Status::InvalidData);
}
