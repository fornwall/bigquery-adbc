//! Bound query parameters, positional and named, scalar and composite.

use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::{Connection, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_SCHEMA_METADATA_QUERY_ID, BigQueryQueryPriority, BigQueryStatementOption,
    BigQueryWriteDisposition,
};
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float64Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray, ListArray,
    NullArray, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer};
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Metadata, Schema};

use crate::support::{
    DropTable, assert_bignumeric_extremes, assert_decimal_domain_result, bignumeric_extremes_batch,
    bignumeric_extremes_select, bool_at, connect, connect_at_location, connect_with_dataset,
    connect_with_dataset_at, count_rows, date_value, decimal_domain_batch, execute_sql, i64_at,
    interval_at, list_at, named_parameter_query, prepare_query, project,
    schema_without_job_metadata, scratch_table, set_option, single_batch, str_at, struct_at,
    ts_value, typed,
};

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_and_reads_parameterized_query_partitions() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, "SELECT ? AS value");
    statement
        .bind(
            RecordBatch::try_from_iter([("", Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef)])
                .unwrap(),
        )
        .unwrap();

    let result = statement.execute_partitions().unwrap();
    // Each parameter row's job has completed, and each returned one row.
    assert_eq!(result.rows_affected, 2);
    assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(result.partitions.len(), 2);

    let mut values = Vec::new();
    for partition in &result.partitions {
        let mut reader = connection.read_partition(partition).unwrap();
        // Each partition reader's schema additionally carries its job's metadata.
        assert_eq!(
            schema_without_job_metadata(reader.schema().as_ref()),
            result.schema
        );
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        values.push(i64_at(&batch, 0));
    }
    assert_eq!(values, [7, 8]);
    assert_eq!(
        connection
            .read_partition(b"not a partition")
            .err()
            .unwrap()
            .status,
        Status::InvalidArguments
    );
}

/// A DML statement returns no rows, so a caller has no reason to read its stream: every bound row
/// has run when `execute` returns, as a DB-API `execute` that closes the cursor relies on.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_every_bound_dml_row_before_execute_returns() {
    let mut connection = connect_with_dataset();
    let table = scratch_table("multi_row_dml");
    let _drop = DropTable::table(&table);
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{table}` (a INT64)"),
    );
    let mut statement = prepare_query(
        &mut connection,
        &format!("INSERT INTO `{table}` (a) VALUES (?)"),
    );
    statement
        .bind(
            RecordBatch::try_from_iter([(
                "",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            )])
            .unwrap(),
        )
        .unwrap();
    drop(statement.execute().unwrap());
    assert_eq!(count_rows(&mut connection, &table), 3);
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn writes_every_bound_row_to_an_explicit_destination_before_execute_returns() {
    let mut connection = connect_with_dataset_at("US");
    let table = scratch_table("bound_destination");
    let _drop = DropTable::table(&table);
    let mut statement = prepare_query(&mut connection, "SELECT ? AS value");
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        table.clone(),
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    statement
        .bind(
            RecordBatch::try_from_iter([(
                "",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            )])
            .unwrap(),
        )
        .unwrap();

    drop(statement.execute().unwrap());
    assert_eq!(count_rows(&mut connection, &table), 3);
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_named_parameters_for_each_bound_row() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT @integer_value * 2 AS doubled, @string_value AS string_value, \
        @timestamp_value AS timestamp_value",
    );

    let columns: Vec<(&str, ArrayRef)> = vec![
        ("integer_value", Arc::new(Int64Array::from(vec![21, -5]))),
        (
            "string_value",
            Arc::new(
                vec![Some("first"), None]
                    .into_iter()
                    .collect::<DictionaryArray<Int8Type>>(),
            ),
        ),
        (
            "timestamp_value",
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_000_001, 2_000_002]).with_timezone_utc(),
            ),
        ),
    ];
    statement
        .bind(RecordBatch::try_from_iter(columns).unwrap())
        .unwrap();

    let schema = statement.execute_schema().unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    let mut reader = statement.execute().unwrap();
    let mut doubled = Vec::new();
    let mut strings = Vec::new();
    let mut timestamps = Vec::new();
    for batch in &mut reader {
        let batch = batch.unwrap();
        let integer_array = typed::<Int64Array>(batch.column(0).as_ref());
        let string_array = typed::<StringArray>(batch.column(1).as_ref());
        let timestamp_array = typed::<TimestampMicrosecondArray>(batch.column(2).as_ref());
        for row in 0..batch.num_rows() {
            doubled.push(integer_array.value(row));
            strings.push((!string_array.is_null(row)).then(|| string_array.value(row).to_owned()));
            timestamps.push(timestamp_array.value(row));
        }
    }
    assert_eq!(doubled, [42, -10]);
    assert_eq!(strings, [Some("first".to_owned()), None]);
    assert_eq!(timestamps, [1_000_001, 2_000_002]);

    let mut positional = prepare_query(&mut connection, "SELECT ? AS value");
    let stream_schema = Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false)]));
    let batches = [7, 8].map(|value| {
        Ok::<_, ArrowError>(
            RecordBatch::try_new(
                stream_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .unwrap(),
        )
    });
    positional
        .bind_stream(Box::new(RecordBatchIterator::new(
            batches.into_iter(),
            stream_schema,
        )))
        .unwrap();
    let values = positional
        .execute()
        .unwrap()
        .map(|batch| i64_at(&batch.unwrap(), 0))
        .collect::<Vec<_>>();
    assert_eq!(values, [7, 8]);
}

/// BigQuery answers placeholders of the other style than the parameter mode with a bare
/// "Named/Positional parameters are not supported"; the driver adds which option to set.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_placeholder_style_mismatch_names_the_parameter_mode_to_set() {
    let mut connection = connect(project());
    let batch =
        || RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]);
    for (mut statement, remedy) in [
        (
            prepare_query(&mut connection, "SELECT @x + 1"),
            "Named parameters are not supported at [1:8]",
        ),
        (
            named_parameter_query(&mut connection, "SELECT ? + 1"),
            "Positional parameters are not supported at [1:8]",
        ),
    ] {
        statement.bind(batch().unwrap()).unwrap();
        let failure = statement.execute().err().unwrap();
        assert_eq!(failure.status, Status::InvalidArguments);
        let expected_mode = if remedy.starts_with("Named") {
            "named"
        } else {
            "positional"
        };
        assert!(
            failure.message.contains(remedy)
                && failure.message.ends_with(&format!(
                    "; set bigquery.query.parameter_mode to \"{expected_mode}\""
                )),
            "message: {}",
            failure.message
        );
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_decimal_parameters_by_value_domain() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT CAST(@decimal32 AS STRING), CAST(@negative_scale AS STRING), \
        CAST(@decimal128 AS STRING), CAST(@decimal256 AS STRING), \
        CAST(@explicit_bignumeric AS STRING), TYPEOF(@decimal32), \
        TYPEOF(@negative_scale), TYPEOF(@decimal128), TYPEOF(@decimal256), \
        TYPEOF(@explicit_bignumeric)",
    );

    statement.bind(decimal_domain_batch()).unwrap();

    let batch = single_batch(&mut statement);
    assert_decimal_domain_result(&batch);
}

/// Both read paths return BIGNUMERIC's full domain, so parameters must accept all of it too.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_bignumeric_parameters_across_the_whole_domain() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        &bignumeric_extremes_select("@largest", "@smallest"),
    );
    statement.bind(bignumeric_extremes_batch()).unwrap();
    assert_bignumeric_extremes(&single_batch(&mut statement));
}

/// A negative sub-hour interval carries its sign on a zero hour (`0-0 0 -0:0:0.000001`), because
/// canonical GoogleSQL signs the whole `H:M:S` group. ZetaSQL does; this checks the service agrees.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_negative_sub_hour_interval_parameters() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT CAST(@microsecond AS STRING), CAST(@minutes AS STRING), \
        CAST(@mixed AS STRING), @microsecond = INTERVAL -1 MICROSECOND, \
        @minutes = INTERVAL -90 SECOND, \
        @mixed = MAKE_INTERVAL(1, 2, -3, 0, -4, -5)",
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "microsecond",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new("minutes", DataType::Interval(IntervalUnit::DayTime), false),
        Field::new(
            "mixed",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(0, 0, -1_000),
            ])) as ArrayRef,
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                0, -90_000,
            )])) as ArrayRef,
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(14, -3, -245_000_000_000),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    statement.bind(batch).unwrap();
    let batch = single_batch(&mut statement);
    assert_eq!(str_at(&batch, 0), "0-0 0 -0:0:0.000001");
    assert_eq!(str_at(&batch, 1), "0-0 0 -0:1:30");
    assert_eq!(str_at(&batch, 2), "1-2 -3 -0:4:5");
    for column in 3..6 {
        assert!(bool_at(&batch, column), "column {column}");
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn gets_server_inferred_parameter_schemas() {
    let mut connection = connect(project());

    let mut named = named_parameter_query(
        &mut connection,
        "SELECT @integer = 1, @text = '', 1 IN UNNEST(@integers), \
        @date = DATE '1970-01-01', \
        @duration = INTERVAL 1 DAY, \
        @date_range = RANGE(DATE '1970-01-01', DATE '1970-01-02'), \
        JSON_TYPE(@document), ST_X(@location)",
    );
    named.prepare().unwrap();

    let schema = named.get_parameter_schema().unwrap();
    assert_eq!(schema.fields().len(), 8);
    for (name, data_type) in [
        ("integer", DataType::Int64),
        ("text", DataType::Utf8),
        (
            "integers",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        ),
        ("date", DataType::Date32),
        ("duration", DataType::Interval(IntervalUnit::MonthDayNano)),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &data_type
        );
    }
    for (name, rich_type) in [
        ("date_range", "RANGE<DATE>"),
        ("document", "JSON"),
        ("location", "GEOGRAPHY"),
    ] {
        let metadata = schema.field_with_name(name).unwrap().metadata();
        assert_eq!(metadata["BIGQUERY:type"], rich_type, "{name}");
    }

    let mut positional = prepare_query(
        &mut connection,
        "SELECT ? = 1, ? = '', ? = DATE '1970-01-01'",
    );
    positional.prepare().unwrap();

    let schema = positional.get_parameter_schema().unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert!(schema.fields().iter().all(|field| field.name().is_empty()));
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).data_type(), &DataType::Date32);
    // Describing needs no binding: BigQuery types the undeclared `?`s.
    let result = positional.execute_schema().unwrap();
    assert_eq!(result.fields().len(), 3);
    assert!(
        result
            .fields()
            .iter()
            .all(|field| field.data_type() == &DataType::Boolean)
    );

    // After execution consumes the binding, the statement is still describable.
    let mut consumed = prepare_query(&mut connection, "SELECT ? + 1 AS answer");
    consumed
        .bind(
            RecordBatch::try_from_iter([("", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
                .unwrap(),
        )
        .unwrap();
    consumed.execute_update().unwrap();
    let schema = consumed.execute_schema().unwrap();
    assert_eq!(schema.field(0).name(), "answer");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_nested_parameters() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT ARRAY(SELECT value * 2 FROM UNNEST(@integers) AS value) AS doubled, \
        @record.label AS label, @record.count AS count",
    );

    let integers = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
        Some(1),
        Some(2),
        Some(3),
    ])]);
    let record = StructArray::new(
        vec![
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(Field::new("count", DataType::Int64, false)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["nested"])),
            Arc::new(Int64Array::from(vec![42])),
        ],
        None,
    );
    statement
        .bind(
            RecordBatch::try_from_iter(vec![
                ("integers", Arc::new(integers) as ArrayRef),
                ("record", Arc::new(record) as ArrayRef),
            ])
            .unwrap(),
        )
        .unwrap();

    let schema = statement.execute_schema().unwrap();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::List(Arc::new(
            Field::new("item", DataType::Int64, true)
                .with_metadata(Metadata::new().with("BIGQUERY:type", "INT64"))
        ))
    );
    assert_eq!(
        schema.field(0).metadata().get("BIGQUERY:type"),
        Some(&"ARRAY<INT64>".to_owned())
    );
    let batch = single_batch(&mut statement);
    let doubled = list_at(&batch, 0).value(0);
    assert_eq!(typed::<Int64Array>(doubled.as_ref()).values(), &[2, 4, 6]);
    assert_eq!(str_at(&batch, 1), "nested");
    assert_eq!(i64_at(&batch, 2), 42);
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn distinguishes_empty_and_null_composite_parameters_across_query_paths() {
    // A non-default priority routes through jobs.insert (src/query/plan.rs); the default uses
    // jobs.query. Both are exercised because their request types and conversions differ.
    for priority in [None, Some(BigQueryQueryPriority::Batch)] {
        let mut connection = if priority.is_some() {
            connect_at_location(project(), "US")
        } else {
            // Location-less, so jobs.query relies on the service's location inference.
            connect(project())
        };
        let mut statement = named_parameter_query(
            &mut connection,
            "SELECT ARRAY_LENGTH(@empty_array) AS empty_array_length, \
            @empty_array IS NULL AS empty_array_is_null, \
            @null_array IS NULL AS null_array_is_null, \
            ARRAY_LENGTH(@nested.values) AS nested_array_length, \
            @nested.values IS NULL AS nested_array_is_null, \
            @empty_record IS NULL AS empty_record_is_null, \
            TO_JSON_STRING(@empty_record) AS empty_record_json, \
            @null_empty_record IS NULL AS null_empty_record_is_null",
        );
        if let Some(priority) = priority {
            set_option(&mut statement, BigQueryStatementOption::Priority, priority);
        }

        let empty_array = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(Vec::<Option<i64>>::new()),
        ]));
        let null_array =
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![None::<Vec<Option<i64>>>]);
        let nested = StructArray::new(
            vec![Arc::new(Field::new(
                "values",
                empty_array.data_type().clone(),
                false,
            ))]
            .into(),
            vec![empty_array.clone()],
            None,
        );
        let empty_record = StructArray::new_empty_fields(1, None);
        let null_empty_record =
            StructArray::new_empty_fields(1, Some(NullBuffer::from(vec![false])));
        statement
            .bind(
                RecordBatch::try_from_iter([
                    ("empty_array", empty_array as ArrayRef),
                    ("null_array", Arc::new(null_array) as ArrayRef),
                    ("nested", Arc::new(nested) as ArrayRef),
                    ("empty_record", Arc::new(empty_record) as ArrayRef),
                    ("null_empty_record", Arc::new(null_empty_record) as ArrayRef),
                ])
                .unwrap(),
            )
            .unwrap();

        let mut reader = statement.execute().unwrap();
        let metadata = reader.schema().metadata().clone();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(i64_at(&batch, 0), 0);
        for (column, expected) in [(1, false), (2, true), (4, false), (5, false), (7, true)] {
            assert_eq!(
                bool_at(&batch, column),
                expected,
                "unexpected boolean result in column {column} with priority {priority:?}"
            );
        }
        assert_eq!(i64_at(&batch, 3), 0);
        assert_eq!(str_at(&batch, 6), "{}");

        // Only jobs.insert runs under the driver's `job_<32 lowercase hex digits>` ID, which
        // proves the batch half did not fall back to jobs.query.
        if priority.is_some() {
            let query_id = &metadata[BIGQUERY_SCHEMA_METADATA_QUERY_ID];
            let hex = query_id.strip_prefix("job_").unwrap_or_default();
            assert!(
                hex.len() == 32
                    && hex
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')),
                "the batch-priority query ran as {query_id}, not under a jobs.insert job ID"
            );
        }
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_rich_logical_parameters() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT @period AS period, @duration AS duration, \
        JSON_VALUE(@document, '$.id') AS document_id, ST_ASTEXT(@location) AS location, \
        @instants AS instants, CAST(RANGE_START(@instants) AS STRING) AS instants_start, \
        @civil AS civil, CAST(RANGE_END(@civil) AS STRING) AS civil_end",
    );

    let range_fields: arrow_schema::Fields = vec![
        Arc::new(Field::new("start", DataType::Date32, true)),
        Arc::new(Field::new("end", DataType::Date32, true)),
    ]
    .into();
    let range = StructArray::new(
        range_fields.clone(),
        vec![
            Arc::new(Date32Array::from(vec![Some(0)])),
            Arc::new(Date32Array::from(vec![None])),
        ],
        None,
    );
    // BigQuery's `CAST` of a bound shows what the parameter encoded; selecting the range shows
    // how REST rows decode it (epoch microseconds for TIMESTAMP, a civil literal for DATETIME).
    let timestamp_range = |data_type: DataType, start: Option<i64>, end: Option<i64>| {
        let fields: arrow_schema::Fields = vec![
            Arc::new(Field::new("start", data_type.clone(), true)),
            Arc::new(Field::new("end", data_type.clone(), true)),
        ]
        .into();
        let bound = |value: Option<i64>| -> ArrayRef {
            Arc::new(TimestampMicrosecondArray::from(vec![value]).with_data_type(data_type.clone()))
        };
        StructArray::new(fields, vec![bound(start), bound(end)], None)
    };
    let range_field = |name: &str, range: &StructArray, element: &str| {
        Field::new(name, range.data_type().clone(), false)
            .with_metadata(Metadata::new().with("BIGQUERY:type", format!("RANGE<{element}>")))
    };
    // 2020-01-01 00:00:00.000001 UTC, and 2021-01-01 00:00:00.5 as a civil datetime.
    let instant = 1_577_836_800_000_001;
    let civil = 1_609_459_200_500_000;
    let instants = timestamp_range(
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        Some(instant),
        None,
    );
    let civils = timestamp_range(
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        None,
        Some(civil),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("period", DataType::Struct(range_fields), false)
            .with_metadata(Metadata::new().with("BIGQUERY:type", "RANGE<DATE>")),
        Field::new(
            "duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new("document", DataType::Utf8, false)
            .with_metadata(Metadata::new().with("BIGQUERY:type", "JSON")),
        Field::new("location", DataType::Utf8, false)
            .with_metadata(Metadata::new().with("BIGQUERY:type", "GEOGRAPHY")),
        range_field("instants", &instants, "TIMESTAMP"),
        range_field("civil", &civils, "DATETIME"),
    ]));
    statement
        .bind(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(range),
                    Arc::new(IntervalMonthDayNanoArray::from(vec![
                        IntervalMonthDayNano::new(-25, -3, -14_706_007_008_999),
                    ])),
                    Arc::new(StringArray::from(vec![r#"{"id":7}"#])),
                    Arc::new(StringArray::from(vec!["POINT(1 2)"])),
                    Arc::new(instants),
                    Arc::new(civils),
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let result_schema = statement.execute_schema().unwrap();
    assert_eq!(
        result_schema.field(0).metadata().get("BIGQUERY:type"),
        Some(&"RANGE<DATE>".to_owned())
    );
    assert_eq!(
        result_schema.field(1).data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    let batch = single_batch(&mut statement);
    let period = struct_at(&batch, 0);
    assert_eq!(date_value(period.column(0)), 0);
    assert!(period.column(1).is_null(0));
    assert_eq!(
        interval_at(&batch, 1),
        IntervalMonthDayNano::new(-25, -3, -14_706_007_008_000)
    );
    assert_eq!(str_at(&batch, 2), "7");
    assert_eq!(str_at(&batch, 3), "POINT(1 2)");

    let instants = struct_at(&batch, 4);
    assert_eq!(ts_value(instants.column(0)), instant);
    assert!(instants.column(1).is_null(0));
    assert_eq!(str_at(&batch, 5), "2020-01-01 00:00:00.000001+00");
    let civils = struct_at(&batch, 6);
    assert!(civils.column(0).is_null(0));
    assert_eq!(ts_value(civils.column(1)), civil);
    assert_eq!(str_at(&batch, 7), "2021-01-01 00:00:00.500");
    for (column, element) in [(4, "TIMESTAMP"), (6, "DATETIME")] {
        assert_eq!(
            result_schema.field(column).metadata().get("BIGQUERY:type"),
            Some(&format!("RANGE<{element}>"))
        );
    }
}

/// A NULL parameter keeps its type: pandas and polars bind missing values as typed nulls, and
/// BigQuery resolves function signatures from the declared type. Each expression type-checks only
/// for its own type (an untyped or `STRING` NULL fails with "No matching signature").
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn binds_typed_null_scalars() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT [@integer + 1 IS NULL, @float * 2 IS NULL, (NOT @boolean) IS NULL, \
         CONCAT(@text, 'x') IS NULL, BYTE_LENGTH(@bytes) IS NULL, \
         DATE_ADD(@date, INTERVAL 1 DAY) IS NULL, \
         TIMESTAMP_ADD(@instant, INTERVAL 1 SECOND) IS NULL, \
         DATETIME_ADD(@civil, INTERVAL 1 SECOND) IS NULL, @numeric + 1 IS NULL] AS null_results",
    );
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("integer", Arc::new(Int64Array::from(vec![None]))),
        ("float", Arc::new(Float64Array::from(vec![None]))),
        ("boolean", Arc::new(BooleanArray::from(vec![None]))),
        ("text", Arc::new(StringArray::from(vec![None::<&str>]))),
        ("bytes", Arc::new(BinaryArray::from(vec![None::<&[u8]>]))),
        ("date", Arc::new(Date32Array::from(vec![None]))),
        (
            "instant",
            Arc::new(TimestampMicrosecondArray::from(vec![None]).with_timezone_utc()),
        ),
        (
            "civil",
            Arc::new(TimestampMicrosecondArray::from(vec![None])),
        ),
        (
            "numeric",
            Arc::new(
                Decimal128Array::from(vec![None])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ),
    ];
    statement
        .bind(RecordBatch::try_from_iter(columns).unwrap())
        .unwrap();

    let batch = single_batch(&mut statement);
    let nulls = list_at(&batch, 0).value(0);
    assert_eq!(
        typed::<BooleanArray>(nulls.as_ref())
            .iter()
            .collect::<Vec<_>>(),
        [Some(true); 9]
    );
}

/// An Arrow `Null` column, which pyarrow infers for an all-`None` one, binds as a NULL of the type
/// its `BIGQUERY:type` names; each expression type-checks only for that type.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn binds_null_columns_as_the_type_their_metadata_names() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT [@integer + 1 IS NULL, DATE_ADD(@date, INTERVAL 1 DAY) IS NULL, \
         JSON_TYPE(@document) IS NULL] AS null_results",
    );
    let field = |name: &str, rich_type: &str| {
        Field::new(name, DataType::Null, true)
            .with_metadata(Metadata::new().with("BIGQUERY:type", rich_type))
    };
    let schema = Arc::new(Schema::new(vec![
        field("integer", "INT64"),
        field("date", "DATE"),
        field("document", "JSON"),
    ]));
    let columns = vec![Arc::new(NullArray::new(1)) as ArrayRef; 3];
    statement
        .bind(RecordBatch::try_new(schema, columns).unwrap())
        .unwrap();

    let batch = single_batch(&mut statement);
    let nulls = list_at(&batch, 0).value(0);
    assert_eq!(
        typed::<BooleanArray>(nulls.as_ref())
            .iter()
            .collect::<Vec<_>>(),
        [Some(true); 3]
    );
}
