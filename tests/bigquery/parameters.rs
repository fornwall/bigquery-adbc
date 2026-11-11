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
    DurationMicrosecondArray, DurationSecondArray, Float64Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, ListArray, NullArray, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer};
use arrow_schema::{DataType, Field, IntervalUnit, Metadata, Schema};

use crate::support::{
    DropTable, assert_bignumeric_extremes, assert_decimal_domain_result, bignumeric_extremes_batch,
    bignumeric_extremes_select, bool_at, connect, connect_at_location, connect_with_dataset_at,
    count_rows, date_value, decimal_domain_batch, execute_sql, i64_at, int64_batch, interval_at,
    list_at, named_parameter_query, prepare_query, project, schema_without_job_metadata,
    scratch_table, set_option, single_batch, str_at, struct_at, ts_value, typed,
};

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_and_reads_parameterized_query_partitions() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, "SELECT ? AS value");
    statement.bind(int64_batch("", vec![7, 8])).unwrap();

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

/// Every bound row has run when `execute` returns, for DML and for a query writing to an explicit
/// destination. DML returns no rows, so a caller has no reason to read its stream, as a DB-API
/// `execute` that closes the cursor relies on.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn executes_every_bound_row_before_execute_returns() {
    let mut connection = connect_with_dataset_at("US");
    let target = scratch_table("multi_row_dml");
    let destination = scratch_table("bound_destination");
    let _drop_target = DropTable::table(&target);
    let _drop_destination = DropTable::table(&destination);
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{target}` (a INT64)"),
    );
    let insert = prepare_query(
        &mut connection,
        &format!("INSERT INTO `{target}` (a) VALUES (?)"),
    );
    let mut select = prepare_query(&mut connection, "SELECT ? AS value");
    set_option(
        &mut select,
        BigQueryStatementOption::DestinationTable,
        destination.clone(),
    );
    set_option(
        &mut select,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteAppend,
    );
    for (mut statement, table) in [(insert, &target), (select, &destination)] {
        statement.bind(int64_batch("", vec![1, 2, 3])).unwrap();
        drop(statement.execute().unwrap());
        assert_eq!(count_rows(&mut connection, table), 3, "{table}");
    }
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
    let batches = [7, 8].map(|value| int64_batch("", vec![value]));
    let schema = batches[0].schema();
    positional
        .bind_stream(Box::new(RecordBatchIterator::new(batches.map(Ok), schema)))
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
        statement.bind(int64_batch("x", vec![1])).unwrap();
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
    consumed.bind(int64_batch("", vec![7])).unwrap();
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
/// BigQuery resolves function signatures from the declared type. An Arrow `Null` column, which
/// pyarrow infers for an all-`None` one, binds as a NULL of the type its `BIGQUERY:type` names.
/// Each expression type-checks only for its own type (an untyped or `STRING` NULL fails with "No
/// matching signature").
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn binds_typed_null_parameters() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT [@integer + 1 IS NULL, @float * 2 IS NULL, (NOT @boolean) IS NULL, \
         CONCAT(@text, 'x') IS NULL, BYTE_LENGTH(@bytes) IS NULL, \
         DATE_ADD(@date, INTERVAL 1 DAY) IS NULL, \
         TIMESTAMP_ADD(@instant, INTERVAL 1 SECOND) IS NULL, \
         DATETIME_ADD(@civil, INTERVAL 1 SECOND) IS NULL, @numeric + 1 IS NULL, \
         @null_integer + 1 IS NULL, DATE_ADD(@null_date, INTERVAL 1 DAY) IS NULL, \
         JSON_TYPE(@null_document) IS NULL] AS null_results",
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
    let untyped = [
        ("null_integer", "INT64"),
        ("null_date", "DATE"),
        ("null_document", "JSON"),
    ];
    let (fields, columns): (Vec<_>, Vec<_>) = columns
        .into_iter()
        .map(|(name, column)| (Field::new(name, column.data_type().clone(), true), column))
        .chain(untyped.map(|(name, rich_type)| {
            let field = Field::new(name, DataType::Null, true)
                .with_metadata(Metadata::new().with("BIGQUERY:type", rich_type));
            (field, Arc::new(NullArray::new(1)) as ArrayRef)
        }))
        .unzip();
    statement
        .bind(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
        .unwrap();

    let batch = single_batch(&mut statement);
    let nulls = list_at(&batch, 0).value(0);
    assert_eq!(
        typed::<BooleanArray>(nulls.as_ref())
            .iter()
            .collect::<Vec<_>>(),
        [Some(true); 12]
    );
}

/// One row of every type the driver maps, at its extremes and special values: what a result
/// decodes into must bind back as a parameter BigQuery reads as the same value.
const ROUND_TRIP_SELECT: &str = r#"
SELECT
  CAST('9223372036854775807' AS INT64) AS int64_max,
  CAST('-9223372036854775808' AS INT64) AS int64_min,
  IEEE_DIVIDE(0, 0) AS float64_nan,
  IEEE_DIVIDE(1, 0) AS float64_infinity,
  IEEE_DIVIDE(-1, 0) AS float64_negative_infinity,
  CAST('-0' AS FLOAT64) AS float64_negative_zero,
  CAST('5e-324' AS FLOAT64) AS float64_subnormal,
  CAST('1.7976931348623157e308' AS FLOAT64) AS float64_max,
  CAST('-2.2250738585072014e-308' AS FLOAT64) AS float64_negative_min_normal,
  0.1 + 0.2 AS float64_shortest_round_trip,
  NUMERIC '99999999999999999999999999999.999999999' AS numeric_max,
  NUMERIC '-0.000000001' AS numeric_smallest,
  BIGNUMERIC '578960446186580977117854925043439539266.34992332820282019728792003956564819967' AS bignumeric_max,
  BIGNUMERIC '-578960446186580977117854925043439539266.34992332820282019728792003956564819968' AS bignumeric_min,
  BIGNUMERIC '0.00000000000000000000000000000000000001' AS bignumeric_smallest,
  FALSE AS bool_false,
  '' AS empty_string,
  CODE_POINTS_TO_STRING([97, 0, 233, 128013]) AS string_with_nul,
  b'' AS empty_bytes,
  b'\x00\x01\xff' AS bytes_with_nul,
  DATE '0001-01-01' AS date_min,
  DATE '9999-12-31' AS date_max,
  TIME '00:00:00' AS time_min,
  TIME '23:59:59.999999' AS time_max,
  DATETIME '0001-01-01 00:00:00' AS datetime_min,
  DATETIME '9999-12-31 23:59:59.999999' AS datetime_max,
  DATETIME '1969-12-31 23:59:59.999999' AS datetime_before_epoch,
  TIMESTAMP '0001-01-01 00:00:00+00' AS timestamp_min,
  TIMESTAMP '9999-12-31 23:59:59.999999+00' AS timestamp_max,
  TIMESTAMP '1969-12-31 23:59:59.999999+00' AS timestamp_before_epoch,
  MAKE_INTERVAL(-1, -2, -3, -4, -5, -6) AS interval_negative,
  INTERVAL '-1-2 3 -4:5:6.000001' YEAR TO SECOND AS interval_mixed_signs,
  INTERVAL '10000-0 -3660000 0:0:0' YEAR TO SECOND AS interval_extreme_parts,
  INTERVAL '0-0 0 -2562047:47:16.854775' YEAR TO SECOND AS interval_widest_decodable_time,
  JSON '{"b":[1,{"c":null}],"a":"\\u00e9\\ud83d\\udc0d\\n"}' AS json_escapes,
  JSON '18446744073709551615' AS json_u64_max,
  JSON 'null' AS json_null,
  ST_GEOGPOINT(1, 2) AS geography_point,
  ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(3 4, 5 6))') AS geography_collection,
  RANGE(DATE '0001-01-01', DATE '9999-12-31') AS range_date_extremes,
  RANGE(CAST(NULL AS DATETIME), DATETIME '1969-12-31 23:59:59.999999') AS range_unbounded_start,
  RANGE(TIMESTAMP '0001-01-01 00:00:00+00', NULL) AS range_unbounded_end,
  RANGE(CAST(NULL AS DATE), NULL) AS range_unbounded,
  CAST(NULL AS RANGE<TIMESTAMP>) AS range_null,
  ARRAY<INT64>[] AS empty_array,
  [IEEE_DIVIDE(0, 0), CAST('-0' AS FLOAT64), CAST('5e-324' AS FLOAT64)] AS array_of_float64,
  [NUMERIC '-99999999999999999999999999999.999999999', 0] AS array_of_numeric,
  [JSON '{"a":1}', JSON 'null'] AS array_of_json,
  [RANGE(DATE '2026-01-01', NULL)] AS array_of_range,
  [b'\x00', b''] AS array_of_bytes,
  ['', 'x'] AS array_of_string,
  [STRUCT(CAST(NULL AS TIMESTAMP) AS t, ARRAY<INT64>[] AS xs), STRUCT(TIMESTAMP '9999-12-31 23:59:59.999999+00', [1])] AS array_of_struct,
  STRUCT(STRUCT(STRUCT(NUMERIC '1.5' AS x) AS c) AS b) AS deep_struct,
  STRUCT(CAST(NULL AS INT64) AS a, CAST(NULL AS RANGE<DATE>) AS r) AS struct_of_nulls,
  STRUCT(ARRAY<STRING>[] AS xs, [STRUCT(JSON '[1]' AS j)] AS ys) AS struct_of_arrays,
  CAST(NULL AS STRUCT<a INT64, b STRING>) AS null_struct,
  CAST(NULL AS INT64) AS null_int64,
  CAST(NULL AS BIGNUMERIC) AS null_bignumeric,
  CAST(NULL AS JSON) AS null_json,
  CAST(NULL AS INTERVAL) AS null_interval
"#;

/// A result binds back as parameters BigQuery reads as the same values and types, so echoing
/// every column of [`ROUND_TRIP_SELECT`] through a named parameter returns the original batch: the
/// parameter spellings of each type's extremes, its special values and its empty and NULL
/// composites all parse back exactly.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn result_columns_bind_back_as_the_same_parameter_values() {
    let mut connection = connect(project());
    let original = single_batch(&mut prepare_query(&mut connection, ROUND_TRIP_SELECT));
    assert_eq!(original.num_rows(), 1);

    let echo = original
        .schema()
        .fields()
        .iter()
        .map(|field| format!("@{name} AS {name}", name = field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = named_parameter_query(&mut connection, &format!("SELECT {echo}"));
    statement.bind(original.clone()).unwrap();
    let echoed = single_batch(&mut statement);

    assert_eq!(echoed.num_columns(), original.num_columns());
    for (index, field) in original.schema().fields().iter().enumerate() {
        let name = field.name();
        assert_eq!(echoed.schema().field(index), field.as_ref(), "{name}");
        assert_eq!(
            echoed.column(index).to_data(),
            original.column(index).to_data(),
            "{name}"
        );
    }
}

/// A duration binds as an INTERVAL's time part, which BigQuery bounds at 87,840,000 hours: far
/// past the 2,562,047 hours an Arrow month-day-nanosecond interval, and so a result, can hold.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn binds_durations_as_interval_time_up_to_bigquerys_bound() {
    let mut connection = connect(project());
    let mut statement = named_parameter_query(
        &mut connection,
        "SELECT CAST(@two_days AS STRING), CAST(@longest AS STRING), \
        CAST(@most_negative AS STRING), @two_days = INTERVAL 48 HOUR",
    );
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "two_days",
            Arc::new(DurationSecondArray::from(vec![172_800])),
        ),
        (
            "longest",
            Arc::new(DurationSecondArray::from(vec![316_224_000_000])),
        ),
        (
            "most_negative",
            Arc::new(DurationMicrosecondArray::from(vec![
                -316_224_000_000_000_000,
            ])),
        ),
    ];
    statement
        .bind(RecordBatch::try_from_iter(columns).unwrap())
        .unwrap();
    let batch = single_batch(&mut statement);
    assert_eq!(str_at(&batch, 0), "0-0 0 48:0:0");
    assert_eq!(str_at(&batch, 1), "0-0 0 87840000:0:0");
    assert_eq!(str_at(&batch, 2), "0-0 0 -87840000:0:0");
    assert!(bool_at(&batch, 3));
}

/// BigQuery counts no parameters: it ignores bound ones the SQL never references, in either mode,
/// and refuses a placeholder nothing binds. The driver sends what is bound and reports the
/// refusal as `INVALID_ARGUMENT`, naming the placeholder.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn unreferenced_parameters_are_ignored_and_unbound_placeholders_refused() {
    let mut connection = connect(project());
    let two_columns = || {
        RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
        ])
        .unwrap()
    };
    for mut statement in [
        prepare_query(&mut connection, "SELECT ?"),
        named_parameter_query(&mut connection, "SELECT @a"),
    ] {
        statement.bind(two_columns()).unwrap();
        assert_eq!(i64_at(&single_batch(&mut statement), 0), 1);
    }
    for (mut statement, refusal) in [
        (
            prepare_query(&mut connection, "SELECT ?, ?, ?"),
            "Query parameter number 3 is not defined (2 provided)",
        ),
        (
            named_parameter_query(&mut connection, "SELECT @a, @c"),
            "Query parameter 'c' not found",
        ),
    ] {
        statement.bind(two_columns()).unwrap();
        let failure = statement.execute().err().unwrap();
        assert_eq!(
            failure.status,
            Status::InvalidArguments,
            "{}",
            failure.message
        );
        assert!(failure.message.contains(refusal), "{}", failure.message);
    }
}
