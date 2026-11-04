//! Storage Write API bulk ingestion.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::Status;
use adbc_core::options::{IngestMode, OptionStatement};
use adbc_core::{CancelHandle, Connection, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_FIELD_METADATA_MAX_LENGTH, BIGQUERY_FIELD_METADATA_PRECISION,
    BIGQUERY_FIELD_METADATA_SCALE, BigQueryBulkIngestMethod, BigQueryConnection,
    BigQueryConnectionOption, BigQueryStatement, BigQueryStatementOption,
};
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Date32Array, Decimal32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Int8Array, Int32Array, Int64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, ListArray,
    RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer, i256};
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema, TimeUnit};

use crate::support::{
    DropTable, SCRATCH_DATASET, assert_bignumeric_extremes, assert_decimal_domain_result,
    bignumeric_extremes_batch, bignumeric_extremes_select, connect, connect_with_dataset,
    count_rows, date_at, decimal_domain_batch, execute_sql, i64_at, ingest_batch, interval_at,
    option_string, prepare_query, project, read_all, read_single_i64, read_single_string,
    scratch_table, set_option, single_batch, str_at, struct_at, ts_at, typed,
};

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_arrow_with_standard_lifecycle_modes() {
    let table = scratch_table("ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    set_option(
        &mut connection,
        BigQueryConnectionOption::BulkIngestMethod,
        BigQueryBulkIngestMethod::StorageWrite,
    );

    let constrained = |name: &str, data_type: DataType, max_length: &str| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
            max_length.to_owned(),
        )]))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        constrained("name", DataType::Utf8, "16"),
        constrained(
            "category",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            "8",
        ),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]));
    let batch = |ids: Vec<i32>,
                 names: Vec<Option<&str>>,
                 categories: Vec<Option<&str>>,
                 amounts: Vec<Option<i128>>| {
        let categories: DictionaryArray<Int8Type> = categories.into_iter().collect();
        let amounts = Decimal128Array::from(amounts)
            .with_precision_and_scale(10, 2)
            .unwrap();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(categories) as ArrayRef,
                Arc::new(amounts) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let mut ingest = connection.new_statement().unwrap();
    assert_eq!(
        option_string(&ingest, BigQueryStatementOption::BulkIngestMethod),
        BigQueryBulkIngestMethod::StorageWrite.as_str()
    );
    set_option(
        &mut ingest,
        BigQueryStatementOption::BulkIngestMethod,
        BigQueryBulkIngestMethod::StorageWrite,
    );
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    assert_eq!(
        option_string(&ingest, OptionStatement::IngestMode),
        String::from(IngestMode::Create)
    );
    ingest
        .bind(batch(
            vec![1, 2],
            vec![Some("one"), None],
            vec![Some("odd"), Some("even")],
            vec![Some(12_345), None],
        ))
        .unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(2));
    let table_schema = connection.get_table_schema(None, None, &table).unwrap();
    // The decimal column is NUMERIC(10, 2), but it reads back as NUMERIC's full type, so that is
    // its Arrow type; the declared shape is metadata.
    let amount = table_schema.field(3);
    assert_eq!(amount.data_type(), &DataType::Decimal128(38, 9));
    assert_eq!(
        [
            BIGQUERY_FIELD_METADATA_PRECISION,
            BIGQUERY_FIELD_METADATA_SCALE
        ]
        .map(|key| amount.metadata().get(key).map(String::as_str)),
        [Some("10"), Some("2")]
    );
    assert_eq!(
        table_schema
            .field(1)
            .metadata()
            .get(BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some(&"16".to_owned())
    );
    assert_eq!(
        table_schema
            .field(2)
            .metadata()
            .get(BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some(&"8".to_owned())
    );
    // A batch built from the table's own schema appends to it: -1.00 read as NUMERIC's full
    // type fits the column's declared NUMERIC(10, 2) exactly.
    let round_trip_batch = RecordBatch::try_new(
        Arc::new(table_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("three")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("odd")])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![Some(-1_000_000_000)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ) as ArrayRef,
        ],
    )
    .unwrap();

    ingest.bind(round_trip_batch.clone()).unwrap();
    let failure = ingest.execute_update().unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists);
    // Create mode learns of the table from the insert's conflict, which still names the remedy.
    assert!(
        failure
            .message
            .contains("already exists; set adbc.ingest.mode to \"adbc.ingest.mode.append\""),
        "{}",
        failure.message
    );
    set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
    ingest.bind(round_trip_batch).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(1));

    let mismatched = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![4]))],
    )
    .unwrap();
    ingest.bind(mismatched).unwrap();
    assert_eq!(
        ingest.execute_update().unwrap_err().status,
        Status::AlreadyExists
    );

    set_option(
        &mut ingest,
        OptionStatement::IngestMode,
        IngestMode::Replace,
    );
    ingest
        .bind(batch(
            vec![7],
            vec![Some("seven")],
            vec![Some("odd")],
            vec![Some(700)],
        ))
        .unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(1));

    let mut query = prepare_query(&mut connection, &format!("SELECT id FROM `{table}`"));
    assert_eq!(read_single_i64(&mut query), 7);
    query
        .set_sql_query(format!("SELECT category FROM `{table}`"))
        .unwrap();
    assert_eq!(read_single_string(&mut query), "odd");
    query
        .set_sql_query(format!("SELECT amount FROM `{table}`"))
        .unwrap();
    let mut reader = query.execute().unwrap();
    assert_eq!(
        reader.schema().field(0).data_type(),
        &DataType::Decimal128(38, 9)
    );
    let amounts = reader.next().unwrap().unwrap();
    // One row: `Replace` truncated the table.
    assert_eq!(amounts.num_rows(), 1);
    assert!(reader.next().is_none());
    assert_eq!(
        typed::<Decimal128Array>(amounts.column(0).as_ref()).value(0),
        7_000_000_000
    );
}

/// Recreates one table name 16 times, one temporal schema per generation, with no pause between
/// the SQL `DROP` and the driver's create-and-ingest, and checks each generation reads back only
/// its own rows. Unique names would hide the stale Storage Write lookup (docs/UPSTREAM.md UP-26),
/// which can outlast the driver's retries. Run this service-race reproducer separately from the
/// ordinary live suite.
#[test]
#[ignore = "known Storage Write service race (UP-26); requires ADC and write access to sq_scratch"]
fn bulk_ingests_after_rapid_table_recreation() {
    let table = scratch_table("recreate_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    for generation in 0_i64..16 {
        let unit = match generation % 4 {
            0 => TimeUnit::Second,
            1 => TimeUnit::Millisecond,
            2 => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        };
        // Keep nanosecond inputs whole microseconds (UP-27) so rounding cannot mask a recreation
        // failure.
        let magnitude = (42 + generation)
            * if unit == TimeUnit::Nanosecond {
                1_000
            } else {
                1
            };
        let values = [None, Some(-magnitude), Some(0), Some(magnitude)];
        let microseconds = values.map(|value| {
            value.map(|value| match unit {
                TimeUnit::Second => value * 1_000_000,
                TimeUnit::Millisecond => value * 1_000,
                TimeUnit::Microsecond => value,
                TimeUnit::Nanosecond => value / 1_000,
            })
        });
        let timezone = match generation {
            8..12 => Some("UTC"),
            12..16 => Some("America/Los_Angeles"),
            _ => None,
        };
        let column: ArrayRef = if generation < 4 {
            match unit {
                TimeUnit::Second => Arc::new(DurationSecondArray::from(values.to_vec())),
                TimeUnit::Millisecond => Arc::new(DurationMillisecondArray::from(values.to_vec())),
                TimeUnit::Microsecond => Arc::new(DurationMicrosecondArray::from(values.to_vec())),
                TimeUnit::Nanosecond => Arc::new(DurationNanosecondArray::from(values.to_vec())),
            }
        } else {
            match unit {
                TimeUnit::Second => Arc::new(
                    TimestampSecondArray::from(values.to_vec()).with_timezone_opt(timezone),
                ),
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(values.to_vec()).with_timezone_opt(timezone),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(values.to_vec()).with_timezone_opt(timezone),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(values.to_vec()).with_timezone_opt(timezone),
                ),
            }
        };
        let expected: ArrayRef = if generation < 4 {
            Arc::new(IntervalMonthDayNanoArray::from_iter(microseconds.map(
                |value| value.map(|value| IntervalMonthDayNano::new(0, 0, value * 1_000)),
            )))
        } else {
            Arc::new(
                TimestampMicrosecondArray::from(microseconds.to_vec())
                    .with_timezone_opt(timezone.map(|_| "UTC")),
            )
        };
        let input_type = column.data_type().clone();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("generation", DataType::Int64, false),
            Field::new("value", input_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![generation; 4])),
                column,
            ],
        )
        .unwrap();
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Create);
        ingest.bind(batch).unwrap();

        // Prepare the payload before DROP so there is no test-side delay, metadata polling or
        // retry between DROP completing and the driver's create-and-ingest operation.
        eprintln!("recreating {table}, generation {generation}: {input_type:?}");
        execute_sql(&mut connection, &format!("DROP TABLE IF EXISTS `{table}`"));
        let started = std::time::Instant::now();
        let rows = ingest.execute_update().unwrap_or_else(|error| {
            panic!(
                "generation {generation} failed after {:?} on {table}: {error}",
                started.elapsed()
            )
        });
        eprintln!(
            "generation {generation} ingested in {:?}",
            started.elapsed()
        );
        assert_eq!(rows, Some(4), "generation {generation}");

        let mut query = prepare_query(
            &mut connection,
            &format!("SELECT id, generation, value FROM `{table}` ORDER BY id"),
        );
        let (_, batches) = read_all(&mut query);
        let mut checked = 0;
        for batch in batches {
            assert!(
                checked + batch.num_rows() <= 4,
                "generation {generation}: extra rows"
            );
            let ids = typed::<Int64Array>(batch.column(0).as_ref());
            let generations = typed::<Int64Array>(batch.column(1).as_ref());
            for (row, (id, actual_generation)) in ids.iter().zip(generations.iter()).enumerate() {
                assert_eq!(
                    id,
                    Some(i64::try_from(checked + row).unwrap()),
                    "generation {generation} returned an unexpected row ID on {table}"
                );
                assert_eq!(
                    actual_generation,
                    Some(generation),
                    "generation {generation} returned stale rows on {table}"
                );
            }
            assert_eq!(
                batch.column(2).to_data(),
                expected.slice(checked, batch.num_rows()).to_data(),
                "generation {generation} returned stale or corrupted values on {table}"
            );
            checked += batch.num_rows();
        }
        assert_eq!(checked, 4, "generation {generation} lost rows on {table}");
    }
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_null_structs_with_hidden_invalid_children() {
    let table = scratch_table("null_struct_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let instant = TimestampSecondArray::from(vec![i64::MAX, 86_400]).with_timezone("UTC");
    let fields = vec![
        Arc::new(Field::new("day", DataType::Date32, true)),
        Arc::new(Field::new("instant", instant.data_type().clone(), true)),
    ];
    // These physical values are outside BigQuery's domain, but belong to a NULL parent.
    // The second row checks that masking also preserves visible siblings.
    let payload = StructArray::new(
        fields.into(),
        vec![
            Arc::new(Date32Array::from(vec![i32::MAX, 1])),
            Arc::new(instant),
        ],
        Some(NullBuffer::from(vec![false, true])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", payload.data_type().clone(), true),
        ])),
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(payload)],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(2));

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT id, payload FROM `{table}` ORDER BY id"),
    );
    let batch = single_batch(&mut query);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        typed::<Int64Array>(batch.column(0).as_ref()).values(),
        &[1, 2]
    );
    let payload = struct_at(&batch, 1);
    assert!(payload.is_null(0));
    assert!(payload.is_valid(1));
    assert_eq!(typed::<Date32Array>(payload.column(0).as_ref()).value(1), 1);
    assert_eq!(
        typed::<TimestampMicrosecondArray>(payload.column(1).as_ref()).value(1),
        86_400_000_000
    );
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn dry_runs_a_bulk_ingest_without_touching_the_target() {
    let table = scratch_table("dry_run_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = |ids: Vec<i64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
        )
        .unwrap()
    };

    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    let dry_run = |ingest: &mut BigQueryStatement, enabled: bool| {
        set_option(
            ingest,
            BigQueryStatementOption::DryRun,
            if enabled {
                ADBC_OPTION_VALUE_ENABLED
            } else {
                ADBC_OPTION_VALUE_DISABLED
            },
        );
    };

    // A dry-run create reports zero rows and creates nothing.
    dry_run(&mut ingest, true);
    ingest.bind(batch(vec![1, 2])).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(0));
    assert_eq!(
        connection
            .get_table_schema(None, None, &table)
            .unwrap_err()
            .status,
        Status::NotFound
    );

    // A dry run keeps the binding, so the real run ingests the validated data without rebinding.
    dry_run(&mut ingest, false);
    assert_eq!(ingest.execute_update().unwrap(), Some(2));
    assert_eq!(count_rows(&mut connection, &table), 2);

    // Now that the target exists, a dry-run create reports the same conflict a real one would.
    dry_run(&mut ingest, true);
    ingest.bind(batch(vec![3])).unwrap();
    assert_eq!(
        ingest.execute_update().unwrap_err().status,
        Status::AlreadyExists
    );

    // A dry-run append over a compatible schema succeeds and appends nothing.
    set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
    assert_eq!(ingest.execute_update().unwrap(), Some(0));
    assert_eq!(count_rows(&mut connection, &table), 2);

    // A schema the existing table cannot accept is refused before anything is written.
    ingest
        .bind(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "different",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![4])) as ArrayRef],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        ingest.execute_update().unwrap_err().status,
        Status::AlreadyExists
    );
    assert_eq!(count_rows(&mut connection, &table), 2);

    // A dry-run append over a table that does not exist reports the same NOT_FOUND.
    let missing = scratch_table("dry_run_missing");
    set_option(&mut ingest, OptionStatement::TargetTable, missing);
    ingest.bind(batch(vec![5])).unwrap();
    assert_eq!(
        ingest.execute_update().unwrap_err().status,
        Status::NotFound
    );
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_a_batch_larger_than_the_append_rows_request_limit() {
    let table = scratch_table("large_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    // Three 8 MB values produce one Arrow IPC batch larger than BigQuery's strict
    // 10,000,000-byte AppendRows request limit, while every individual row remains shippable.
    const ROW_BYTES: usize = 8_000_000;
    let value = vec![b'x'; ROW_BYTES];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(BinaryArray::from(vec![
            value.as_slice(),
            value.as_slice(),
            value.as_slice(),
        ]))],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(3));

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT COUNT(*), SUM(BYTE_LENGTH(payload)) FROM `{table}`"),
    );
    let result = single_batch(&mut query);
    assert_eq!(i64_at(&result, 0), 3);
    assert_eq!(i64_at(&result, 1), 3 * i64::try_from(ROW_BYTES).unwrap());
}

/// Full-size chunks must stay under BigQuery's 10,000,000-byte AppendRows limit, which covers the
/// whole request: payload plus the repeated Arrow schema and protobuf envelope. The oversized-batch
/// test sends one 8 MB row per request, so it never fills that budget.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_chunks_that_fill_the_append_rows_size_budget() {
    let table = scratch_table("target_sized_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    // About 10.5 MB in 32 KiB rows, so the encoder must split at the size target, not per row.
    const ROW_BYTES: usize = 32 * 1024;
    const ROWS: usize = 320;
    let value = "x".repeat(ROW_BYTES);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![value.as_str(); ROWS]))],
    )
    .unwrap();
    assert_eq!(
        ingest_batch(&mut connection, &table, batch),
        Some(i64::try_from(ROWS).unwrap())
    );

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT COUNT(*), SUM(BYTE_LENGTH(payload)) FROM `{table}`"),
    );
    let result = single_batch(&mut query);
    assert_eq!(i64_at(&result, 0), i64::try_from(ROWS).unwrap());
    assert_eq!(i64_at(&result, 1), i64::try_from(ROWS * ROW_BYTES).unwrap());
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_decimals_by_value_domain() {
    let table = scratch_table("decimal_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    assert_eq!(
        ingest_batch(&mut connection, &table, decimal_domain_batch()),
        Some(1)
    );

    // Each column reads back as its family's full type; the declared shape is metadata.
    let table_schema = connection.get_table_schema(None, None, &table).unwrap();
    let numeric = DataType::Decimal128(38, 9);
    let bignumeric = DataType::Decimal256(76, 38);
    assert_eq!(
        table_schema
            .fields()
            .iter()
            .map(|field| {
                let metadata = |key| field.metadata().get(key).map(String::as_str);
                (
                    field.data_type(),
                    metadata(BIGQUERY_FIELD_METADATA_PRECISION),
                    metadata(BIGQUERY_FIELD_METADATA_SCALE),
                )
            })
            .collect::<Vec<_>>(),
        [
            (&numeric, Some("3"), Some("2")),
            (&numeric, Some("5"), Some("0")),
            (&bignumeric, Some("38"), Some("20")),
            (&numeric, Some("38"), Some("9")),
            (&bignumeric, Some("38"), Some("9")),
        ]
    );

    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT CAST(decimal32 AS STRING), CAST(negative_scale AS STRING), \
             CAST(decimal128 AS STRING), CAST(decimal256 AS STRING), \
             CAST(explicit_bignumeric AS STRING), TYPEOF(decimal32), \
             TYPEOF(negative_scale), TYPEOF(decimal128), TYPEOF(decimal256), \
             TYPEOF(explicit_bignumeric) FROM `{table}`"
        ),
    );
    let batch = single_batch(&mut query);
    assert_decimal_domain_result(&batch);
}

/// BIGNUMERIC values read from BigQuery ingest back. A `BIGNUMERIC(76, 38)` column rejects the
/// extremes (`400 Field ...: BIGNUMERIC(76, 38) has ...`), so the driver creates a
/// parameter-free column.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_bignumeric_across_the_whole_domain() {
    let table = scratch_table("bignumeric_domain_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let batch = bignumeric_extremes_batch();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    // A parameter-free BIGNUMERIC column reads back as the ingested Arrow type.
    let table_schema = connection.get_table_schema(None, None, &table).unwrap();
    assert_eq!(
        table_schema
            .fields()
            .iter()
            .map(|field| field.data_type())
            .collect::<Vec<_>>(),
        [&DataType::Decimal256(76, 38), &DataType::Decimal256(76, 38)]
    );

    let mut query = prepare_query(
        &mut connection,
        &format!(
            "{} FROM `{table}`",
            bignumeric_extremes_select("largest", "smallest")
        ),
    );
    assert_bignumeric_extremes(&single_batch(&mut query));
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bignumeric_ingest_distinguishes_parameterized_targets() {
    let table = scratch_table("bignumeric_bounded_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (largest BIGNUMERIC(76, 38) NOT NULL, \
             smallest BIGNUMERIC(76, 38) NOT NULL); INSERT INTO `{table}` VALUES (0, 0)"
        ),
    );
    let ingest = |connection: &mut BigQueryConnection, mode| {
        let mut statement = connection.new_statement().unwrap();
        set_option(&mut statement, OptionStatement::TargetTable, table.clone());
        set_option(&mut statement, OptionStatement::IngestMode, mode);
        statement.bind(bignumeric_extremes_batch()).unwrap();
        statement.execute_update()
    };

    // An append takes the column's BIGNUMERIC(76, 38), which lacks plain BIGNUMERIC's partial
    // 77th digit, so the extremes are refused by row before anything is written.
    for mode in [IngestMode::Append, IngestMode::CreateAppend] {
        let failure = ingest(&mut connection, mode).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData, "{failure}");
        assert!(
            failure.message.contains(
                "\"largest\" holds a value at row 0 with more integer digits than its column's \
                 BIGNUMERIC(76, 38) holds"
            ),
            "{failure}"
        );
    }
    assert_eq!(count_rows(&mut connection, &table), 1);

    // Replace must recreate the schema; truncating the bounded table would still reject the rows.
    assert_eq!(
        ingest(&mut connection, IngestMode::Replace).unwrap(),
        Some(1)
    );
    let mut query = prepare_query(
        &mut connection,
        &format!(
            "{} FROM `{table}`",
            bignumeric_extremes_select("largest", "smallest")
        ),
    );
    assert_bignumeric_extremes(&single_batch(&mut query));
    assert_eq!(
        ingest(&mut connection, IngestMode::Append).unwrap(),
        Some(1)
    );
    assert_eq!(count_rows(&mut connection, &table), 2);
}

/// Storage Write reads a `REPEATED` NUMERIC or BIGNUMERIC element at the type's full scale
/// whatever scale its Arrow type declares (UP-89 in docs/UPSTREAM.md), while a scalar or STRUCT
/// field keeps its declared scale. Elements must keep their values, and the column its parameters.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_decimal_arrays_at_their_declared_scale() {
    let table = scratch_table("decimal_array_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let one_row_list = |values: ArrayRef| -> ArrayRef {
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        let length = i32::try_from(values.len()).unwrap();
        Arc::new(ListArray::new(
            item,
            OffsetBuffer::new(vec![0, length].into()),
            values,
            None,
        ))
    };
    let numeric = one_row_list(Arc::new(
        Decimal128Array::from(vec![1, -150, 9_999_999_999])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ));
    let bignumeric = one_row_list(Arc::new(
        Decimal256Array::from(vec![i256::ONE, i256::from_i128(-15_000_000_000)])
            .with_precision_and_scale(48, 10)
            .unwrap(),
    ));
    let nested_values = one_row_list(Arc::new(
        Decimal32Array::from(vec![1])
            .with_precision_and_scale(9, 2)
            .unwrap(),
    ));
    let nested = StructArray::new(
        vec![Arc::new(Field::new(
            "xs",
            nested_values.data_type().clone(),
            true,
        ))]
        .into(),
        vec![nested_values],
        None,
    );
    let batch = RecordBatch::try_from_iter([
        ("numeric", numeric),
        ("bignumeric", bignumeric),
        ("nested", Arc::new(nested) as ArrayRef),
    ])
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    // The elements read back as their family's full type; the declared shape is metadata on the
    // array column.
    let table_schema = connection.get_table_schema(None, None, &table).unwrap();
    let element = |index: usize| {
        let field = table_schema.field(index);
        let DataType::List(item) = field.data_type() else {
            panic!("expected an array column, found {}", field.data_type());
        };
        let metadata = |key| field.metadata().get(key).map(String::as_str);
        (
            item.data_type().clone(),
            metadata(BIGQUERY_FIELD_METADATA_PRECISION),
            metadata(BIGQUERY_FIELD_METADATA_SCALE),
        )
    };
    assert_eq!(
        element(0),
        (DataType::Decimal128(38, 9), Some("10"), Some("2"))
    );
    assert_eq!(
        element(1),
        (DataType::Decimal256(76, 38), Some("48"), Some("10"))
    );

    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT TO_JSON_STRING(numeric), TO_JSON_STRING(bignumeric), \
             TO_JSON_STRING(nested) FROM `{table}`"
        ),
    );
    let batch = single_batch(&mut query);
    assert_eq!(str_at(&batch, 0), r#"["0.01","-1.5","99999999.99"]"#);
    assert_eq!(str_at(&batch, 1), r#"["0.0000000001","-1.5"]"#);
    assert_eq!(str_at(&batch, 2), r#"{"xs":["0.01"]}"#);
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_range_values() {
    let table = scratch_table("range_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let range = |name: &str, element_type: &str, data_type: DataType| {
        Field::new(
            name,
            DataType::Struct(
                vec![
                    Field::new("start", data_type.clone(), true),
                    Field::new("end", data_type, true),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(HashMap::from([(
            "BIGQUERY:type".to_owned(),
            format!("RANGE<{element_type}>"),
        )]))
    };
    let date = range("date_range", "DATE", DataType::Date32);
    let datetime = range(
        "datetime_range",
        "DATETIME",
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let timestamp = range(
        "timestamp_range",
        "TIMESTAMP",
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
    );
    let date_values = StructArray::new(
        match date.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(Date32Array::from(vec![Some(1)])),
            Arc::new(Date32Array::from(vec![Some(3)])),
        ],
        None,
    );
    let datetime_values = StructArray::new(
        match datetime.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_001)])),
            Arc::new(TimestampMicrosecondArray::from(vec![None])),
        ],
        None,
    );
    let timestamp_values = StructArray::new(
        match timestamp.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_001)]).with_timezone_utc()),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(2_000_002)]).with_timezone_utc()),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![date, datetime, timestamp])),
        vec![
            Arc::new(date_values),
            Arc::new(datetime_values),
            Arc::new(timestamp_values),
        ],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT RANGE_START(date_range), RANGE_END(date_range), \
             RANGE_START(datetime_range), RANGE_END(datetime_range), \
             RANGE_START(timestamp_range), RANGE_END(timestamp_range) FROM `{table}`"
        ),
    );
    let batch = single_batch(&mut query);
    assert_eq!(date_at(&batch, 0), 1);
    assert_eq!(date_at(&batch, 1), 3);
    assert_eq!(ts_at(&batch, 2), 1_000_001);
    assert!(batch.column(3).is_null(0));
    assert_eq!(ts_at(&batch, 4), 1_000_001);
    assert_eq!(ts_at(&batch, 5), 2_000_002);
}

/// Storage Write fails every append carrying an `ARRAY<TIMESTAMP>` field with INTERNAL after
/// about 130 s of retries (UP-90), so the ingest is refused up front: without creating the table,
/// and without touching an existing one. The workaround the refusal names keeps element order.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn refuses_array_timestamp_ingest_and_its_staging_workaround_works() {
    let table = scratch_table("array_timestamp");
    let staging = scratch_table("array_timestamp_staging");
    let _drop_table = DropTable::table(&table);
    let _drop_staging = DropTable::table(&staging);
    let mut connection = connect_with_dataset();

    let row = |values: ArrayRef| {
        let list = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::from_lengths([values.len()]),
            values,
            None,
        );
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("ts", list.data_type().clone(), true),
            ])),
            vec![Arc::new(Int64Array::from(vec![1])), Arc::new(list)],
        )
        .unwrap()
    };
    let timestamps = || -> ArrayRef {
        Arc::new(
            TimestampMicrosecondArray::from(vec![3_000_000, 1_000_000, 2_000_000])
                .with_timezone_utc(),
        )
    };
    let refused = |connection: &mut BigQueryConnection, mode: IngestMode| {
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(&mut ingest, OptionStatement::IngestMode, mode);
        ingest.bind(row(timestamps())).unwrap();
        let started = std::time::Instant::now();
        let failure = ingest.execute_update().unwrap_err();
        assert_eq!(failure.status, Status::NotImplemented, "{failure}");
        assert!(failure.message.contains("\"ts\""), "{failure}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
    };

    refused(&mut connection, IngestMode::Create);
    assert_eq!(
        connection
            .get_table_schema(None, None, &table)
            .unwrap_err()
            .status,
        Status::NotFound
    );

    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{table}` (id INT64, ts ARRAY<TIMESTAMP>)"),
    );
    refused(&mut connection, IngestMode::Append);
    assert_eq!(count_rows(&mut connection, &table), 0);

    let values = timestamps();
    let items = StructArray::new(
        vec![Arc::new(Field::new("ts", values.data_type().clone(), true))].into(),
        vec![values],
        None,
    );
    assert_eq!(
        ingest_batch(&mut connection, &staging, row(Arc::new(items))),
        Some(1)
    );
    execute_sql(
        &mut connection,
        &format!(
            "INSERT INTO `{table}` (id, ts) SELECT id, ARRAY(SELECT item.ts FROM UNNEST(ts) AS \
             item WITH OFFSET AS position ORDER BY position) FROM `{staging}`"
        ),
    );
    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT TO_JSON_STRING(ts) FROM `{table}`"),
    );
    assert_eq!(
        read_single_string(&mut query),
        r#"["1970-01-01T00:00:03Z","1970-01-01T00:00:01Z","1970-01-01T00:00:02Z"]"#
    );
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_interval_values() {
    let table = scratch_table("interval_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "year_month",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
        Field::new("day_time", DataType::Interval(IntervalUnit::DayTime), false),
        Field::new(
            "month_day_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(IntervalYearMonthArray::from(vec![-25])) as ArrayRef,
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                -3,
                -14_706_008,
            )])) as ArrayRef,
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(14, 3, 14_706_007_008_999),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT year_month, day_time, month_day_nano FROM `{table}`"),
    );
    let batch = single_batch(&mut query);
    for column in batch.columns() {
        assert_eq!(
            column.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }
    assert_eq!(interval_at(&batch, 0), IntervalMonthDayNano::new(-25, 0, 0));
    assert_eq!(
        interval_at(&batch, 1),
        IntervalMonthDayNano::new(0, -3, -14_706_008_000_000)
    );
    assert_eq!(
        interval_at(&batch, 2),
        IntervalMonthDayNano::new(14, 3, 14_706_007_008_000)
    );
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_duration_values() {
    let table = scratch_table("duration_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();

    let schema = Arc::new(Schema::new(vec![
        Field::new("seconds", DataType::Duration(TimeUnit::Second), false),
        Field::new(
            "milliseconds",
            DataType::Duration(TimeUnit::Millisecond),
            false,
        ),
        Field::new(
            "microseconds",
            DataType::Duration(TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "nanoseconds",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(DurationSecondArray::from(vec![-14_706])) as ArrayRef,
            Arc::new(DurationMillisecondArray::from(vec![-14_706_008])) as ArrayRef,
            Arc::new(DurationMicrosecondArray::from(vec![14_706_007_008])) as ArrayRef,
            Arc::new(DurationNanosecondArray::from(vec![-14_706_007_008_999])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT seconds, milliseconds, microseconds, nanoseconds FROM `{table}`"),
    );
    let batch = single_batch(&mut query);
    for (column, expected) in [
        -14_706_000_000_000,
        -14_706_008_000_000,
        14_706_007_008_000,
        -14_706_007_008_000,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            interval_at(&batch, column),
            IntervalMonthDayNano::new(0, 0, expected)
        );
    }

    // Two days of duration read back as 48 hours, not a calendar day, as for a bound parameter.
    let two_days = scratch_table("duration_hours_ingest");
    let _drop_two_days = DropTable::table(&two_days);
    let batch = RecordBatch::try_from_iter([(
        "span",
        Arc::new(DurationSecondArray::from(vec![172_800])) as ArrayRef,
    )])
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &two_days, batch), Some(1));
    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT FORMAT('%d/%d', EXTRACT(DAY FROM span), EXTRACT(HOUR FROM span)) \
             FROM `{two_days}`"
        ),
    );
    assert_eq!(read_single_string(&mut query), "0/48");
}

/// An empty stream (an empty DataFrame, or a header-only dbt seed) still creates the table with the
/// stream's schema and reports zero rows; appending an empty batch adds nothing.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_an_empty_stream() {
    let table = scratch_table("empty_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    ingest
        .bind_stream(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            schema.clone(),
        )))
        .unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(0));
    let created = connection.get_table_schema(None, None, &table).unwrap();
    assert_eq!(
        created
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type()))
            .collect::<Vec<_>>(),
        [("id", &DataType::Int64), ("label", &DataType::Utf8)]
    );

    set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
    ingest.bind(RecordBatch::new_empty(schema)).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(0));
    assert_eq!(count_rows(&mut connection, &table), 0);
}

/// pyarrow's `pa.uuid()` is `FixedSizeBinary(16)` annotated `arrow.uuid`, which ingest writes as
/// `Binary`. Storage Write refuses that annotation on any other storage, so ingest drops it. It
/// also refuses `arrow.json` on a STRING column, which `BIGQUERY:type` metadata can choose.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_extension_columns_whose_storage_type_changes() {
    let table = scratch_table("extension_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    let extension = |field: Field, name: &str| {
        field.with_metadata(HashMap::from([(
            "ARROW:extension:name".to_owned(),
            name.to_owned(),
        )]))
    };
    let mut document = extension(Field::new("document", DataType::Utf8, false), "arrow.json");
    document
        .metadata_mut()
        .insert("BIGQUERY:type".to_owned(), "STRING".to_owned());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            extension(
                Field::new("id", DataType::FixedSizeBinary(16), false),
                "arrow.uuid",
            ),
            extension(Field::new("flag", DataType::Int8, false), "arrow.bool8"),
            document,
        ])),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter([[7_u8; 16]].into_iter()).unwrap()),
            Arc::new(Int8Array::from(vec![1])),
            Arc::new(StringArray::from(vec![r#"{"b": 1,  "a": 2}"#])),
        ],
    )
    .unwrap();
    assert_eq!(ingest_batch(&mut connection, &table, batch), Some(1));

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT TO_HEX(id), flag, document FROM `{table}`"),
    );
    let batch = single_batch(&mut query);
    assert_eq!(str_at(&batch, 0), "07".repeat(16));
    assert_eq!(i64_at(&batch, 1), 1);
    assert_eq!(str_at(&batch, 2), r#"{"b": 1,  "a": 2}"#);
    assert_eq!(
        batch.schema().field(2).metadata().get("BIGQUERY:type"),
        Some(&"STRING".to_owned())
    );
}

/// The Python driver manager's `adbc_ingest(..., mode="create_append", catalog_name=...,
/// db_schema_name=...)`: the statement names the target on a connection without a default
/// dataset; the first run creates it and the second appends.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_into_an_explicit_namespace_with_create_append() {
    let project = project();
    const LABELS: [Option<&str>; 3] = [Some("naïve café — ✅ 汉字 🐍"), Some(""), None];

    let table = scratch_table("namespace_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect(project);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "label",
            Arc::new(StringArray::from(LABELS.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap();
    let qualified = format!("{project}.{SCRATCH_DATASET}.{table}");

    for (run, expected_rows) in [(1, 3), (2, 6)] {
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetCatalog, project);
        set_option(
            &mut ingest,
            OptionStatement::TargetDbSchema,
            SCRATCH_DATASET,
        );
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(
            &mut ingest,
            OptionStatement::IngestMode,
            IngestMode::CreateAppend,
        );
        ingest.bind(batch.clone()).unwrap();
        assert_eq!(ingest.execute_update().unwrap(), Some(3), "run {run}");
        assert_eq!(
            count_rows(&mut connection, &qualified),
            expected_rows,
            "run {run}"
        );
    }

    // Both runs' rows, each id twice.
    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT label FROM `{qualified}` ORDER BY id"),
    );
    let result = single_batch(&mut query);
    assert_eq!(
        typed::<StringArray>(result.column(0).as_ref())
            .iter()
            .collect::<Vec<_>>(),
        LABELS
            .iter()
            .flat_map(|label| [*label; 2])
            .collect::<Vec<_>>()
    );
}

/// A producer of small batches (DuckDB, a streaming reader) sends one append per batch. Hundreds
/// of them are queued on the stream at once rather than a few, and every row lands exactly once.
/// The elapsed time it prints is the measurement in docs/PERFORMANCE.md.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn bulk_ingests_many_small_batches() {
    const BATCHES: i64 = 200;
    const ROWS_PER_BATCH: i64 = 1_024;

    let table = scratch_table("small_batches_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batches = (0..BATCHES)
        .map(|batch| {
            let ids = (batch * ROWS_PER_BATCH..(batch + 1) * ROWS_PER_BATCH).collect::<Vec<_>>();
            let labels = ids.iter().map(|id| format!("label-{id:012}"));
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                    Arc::new(StringArray::from_iter_values(labels)) as ArrayRef,
                ],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    ingest
        .bind_stream(Box::new(RecordBatchIterator::new(batches, schema)))
        .unwrap();
    let started = std::time::Instant::now();
    let rows = BATCHES * ROWS_PER_BATCH;
    assert_eq!(ingest.execute_update().unwrap(), Some(rows));
    eprintln!(
        "ingested {BATCHES} batches of {ROWS_PER_BATCH} rows in {:?}",
        started.elapsed()
    );

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT COUNT(DISTINCT id) * 1000000 + COUNT(*) FROM `{table}`"),
    );
    assert_eq!(read_single_i64(&mut query), rows * 1_000_000 + rows);
}

/// BigQuery accepts a flexible column name such as `first name`, but Storage Write fails every
/// append to a table that has one, even an append that leaves that column out. The append is
/// refused before a stream opens, and `replace`, the suggested remedy, recreates the table.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn appends_to_a_table_with_a_flexible_column_name_are_refused() {
    let table = scratch_table("flexible_name_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{table}` (id INT64, customer STRUCT<`first name` STRING>)"),
    );
    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    ingest.bind(batch.clone()).unwrap();
    let failure = ingest.execute_update().unwrap_err();
    assert_eq!(
        failure.status,
        Status::NotImplemented,
        "{}",
        failure.message
    );
    assert!(
        failure
            .message
            .contains("has column \"customer.first name\", whose name contains \" \""),
        "{}",
        failure.message
    );

    set_option(
        &mut ingest,
        OptionStatement::IngestMode,
        IngestMode::Replace,
    );
    ingest.bind(batch).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(1));
    assert_eq!(count_rows(&mut connection, &table), 1);
}

/// Storage Write matches fields by name, ignoring case and order at every struct level, and
/// writes an omitted NULLABLE column as NULL and an omitted REPEATED one as `[]`. Appends the
/// service would reject or silently alter (it rounds decimals to the column's scale) are refused
/// before anything is written.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn appends_resolve_columns_by_name_and_refuse_what_storage_write_would_alter() {
    let table = scratch_table("by_name_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (id INT64 NOT NULL, amount NUMERIC(10, 2), \
             customer STRUCT<id INT64, name STRING>, tags ARRAY<STRING>, note STRING)"
        ),
    );
    let append = |connection: &mut BigQueryConnection, columns: Vec<(&str, ArrayRef)>| {
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
        ingest
            .bind(RecordBatch::try_from_iter(columns).unwrap())
            .unwrap();
        ingest.execute_update()
    };
    let amount = |value, precision, scale| {
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ) as ArrayRef
    };
    let customer = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("Name", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["Ada"])) as ArrayRef,
    )])) as ArrayRef;

    let appended = append(
        &mut connection,
        vec![
            ("CUSTOMER", customer),
            ("amount", amount(123, 5, 1)),
            ("ID", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ],
    );
    assert_eq!(appended.unwrap(), Some(1));

    // 1.234567890 would be stored as 1.23, and a row without `id` breaks its NOT NULL.
    let failure = append(
        &mut connection,
        vec![
            ("id", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ("amount", amount(1_234_567_890, 38, 9)),
        ],
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData, "{}", failure.message);
    assert!(
        failure.message.contains(
            "\"amount\" holds a value at row 0 with more than 2 fractional digits, which its \
             column's NUMERIC(10, 2) cannot store without rounding"
        ),
        "{}",
        failure.message
    );
    let failure = append(&mut connection, vec![("amount", amount(1, 5, 1))]).unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists, "{}", failure.message);

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT TO_JSON_STRING(t) FROM `{table}` AS t"),
    );
    assert_eq!(
        read_single_string(&mut query),
        r#"{"id":1,"amount":"12.3","customer":{"id":null,"name":"Ada"},"tags":[],"note":null}"#
    );
}

/// An append takes what the bound data leaves open from the table: a column pandas types `Null`
/// (it holds only nulls) fills the table's column with NULLs, or empty arrays, and a plain string
/// takes the column's maximum length, checked for every value.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn appends_take_null_column_types_and_string_lengths_from_the_table() {
    let table = scratch_table("aligned_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (id INT64 NOT NULL, name STRING(5), seen TIMESTAMP, \
             tags ARRAY<STRING>)"
        ),
    );
    let append = |connection: &mut BigQueryConnection, names: Vec<Option<&str>>| {
        let rows = names.len();
        let ids = (1..=i64::try_from(rows).unwrap()).collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("name", Arc::new(StringArray::from(names)) as ArrayRef),
            (
                "seen",
                Arc::new(arrow_array::NullArray::new(rows)) as ArrayRef,
            ),
            (
                "tags",
                Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", DataType::Null, true)),
                    rows,
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
        ingest.bind(batch).unwrap();
        ingest.execute_update()
    };

    assert_eq!(
        append(&mut connection, vec![Some("ab"), None]).unwrap(),
        Some(2)
    );
    let failure = append(&mut connection, vec![Some("ok"), Some("toolong")]).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData, "{}", failure.message);
    assert!(
        failure.message.contains(
            "\"name\" holds a value of 7 characters at row 1, longer than its declared maximum \
             length of 5"
        ),
        "{}",
        failure.message
    );

    let mut query = prepare_query(
        &mut connection,
        &format!("SELECT TO_JSON_STRING(ARRAY_AGG(t ORDER BY id)) FROM `{table}` AS t"),
    );
    assert_eq!(
        read_single_string(&mut query),
        r#"[{"id":1,"name":"ab","seen":null,"tags":[]},{"id":2,"name":null,"seen":null,"tags":[]}]"#
    );
}

/// What DuckDB does to copy a table: read its rows under the schema `GetTableSchema` reports,
/// which gives a parameterized decimal its family's full type and keeps the declared shape in
/// metadata, then ingest them. The copy declares the same shapes, and appending the rows back
/// into the original checks that each value fits its column exactly.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn a_table_read_under_its_schema_ingests_into_the_same_declared_decimals() {
    let source = scratch_table("decimal_source");
    let copy = scratch_table("decimal_copy");
    let _drop_source = DropTable::table(&source);
    let _drop_copy = DropTable::table(&copy);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{source}` (amount NUMERIC(10, 2), big BIGNUMERIC(40, 2), \
             widest BIGNUMERIC(76, 38), parts ARRAY<NUMERIC(5, 1)>) AS SELECT NUMERIC '12.34', \
             BIGNUMERIC '-5.5', BIGNUMERIC '0.25', [NUMERIC '1.5', NUMERIC '-2']"
        ),
    );
    let schema = Arc::new(connection.get_table_schema(None, None, &source).unwrap());
    let mut query = prepare_query(&mut connection, &format!("SELECT * FROM `{source}`"));
    let rows = single_batch(&mut query);
    let rows = RecordBatch::try_new(schema, rows.columns().to_vec()).unwrap();

    assert_eq!(ingest_batch(&mut connection, &copy, rows.clone()), Some(1));
    let declared = |table: &str| {
        connection
            .get_table_schema(None, None, table)
            .unwrap()
            .fields()
            .iter()
            .map(|field| {
                let metadata = |key| field.metadata().get(key).cloned();
                (
                    metadata(BIGQUERY_FIELD_METADATA_PRECISION),
                    metadata(BIGQUERY_FIELD_METADATA_SCALE),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(declared(&copy), declared(&source));

    let mut append = connection.new_statement().unwrap();
    set_option(&mut append, OptionStatement::TargetTable, source.clone());
    set_option(&mut append, OptionStatement::IngestMode, IngestMode::Append);
    append.bind(rows).unwrap();
    assert_eq!(append.execute_update().unwrap(), Some(1));
    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT TO_JSON_STRING(ARRAY_AGG(t)) FROM (SELECT * FROM `{source}` UNION ALL \
             SELECT * FROM `{copy}`) AS t"
        ),
    );
    assert_eq!(
        read_single_string(&mut query),
        r#"[{"amount":"12.34","big":"-5.5","widest":"0.25","parts":["1.5",-2]},{"amount":"12.34","big":"-5.5","widest":"0.25","parts":["1.5",-2]},{"amount":"12.34","big":"-5.5","widest":"0.25","parts":["1.5",-2]}]"#
    );
}

/// pandas binds JSON and GEOGRAPHY values as plain strings, which Storage Write parses exactly as
/// it parses `arrow.json` and `geoarrow.wkt` strings, nested ones included. A `geoarrow.wkt`
/// string is kept as it is in a STRING column, but an `arrow.json` one is refused.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn appends_plain_strings_to_json_and_geography_columns() {
    let table = scratch_table("json_geography_ingest");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (j JSON, g GEOGRAPHY, s STRING, aj ARRAY<JSON>, \
             st STRUCT<j JSON, g GEOGRAPHY>)"
        ),
    );
    let append = |connection: &mut BigQueryConnection, columns: Vec<(Field, ArrayRef)>| {
        let (fields, columns): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let mut ingest = connection.new_statement().unwrap();
        set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
        set_option(&mut ingest, OptionStatement::IngestMode, IngestMode::Append);
        ingest
            .bind(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
            .unwrap();
        ingest.execute_update()
    };
    let string = |name: &str| Field::new(name, DataType::Utf8, true);
    let strings = |values: Vec<&str>| Arc::new(StringArray::from(values)) as ArrayRef;
    let extension = |extension: &str| {
        string("s").with_metadata(HashMap::from([(
            "ARROW:extension:name".to_owned(),
            extension.to_owned(),
        )]))
    };
    let tags = ListArray::new(
        Arc::new(string("item")),
        OffsetBuffer::from_lengths([2]),
        strings(vec![r#"{"x": 1}"#, "[1, 2]"]),
        None,
    );
    let nested = StructArray::from(vec![
        (Arc::new(string("j")), strings(vec![r#"{"y": 2}"#])),
        (Arc::new(string("g")), strings(vec!["POINT(3 4)"])),
    ]);
    let appended = append(
        &mut connection,
        vec![
            (string("j"), strings(vec![r#"{"b": 1,  "a": [1, 2.50]}"#])),
            (string("g"), strings(vec!["POINT(1 2)"])),
            (extension("geoarrow.wkt"), strings(vec!["POINT(1   2)"])),
            (
                Field::new("aj", tags.data_type().clone(), true),
                Arc::new(tags) as ArrayRef,
            ),
            (
                Field::new("st", nested.data_type().clone(), true),
                Arc::new(nested) as ArrayRef,
            ),
        ],
    );
    assert_eq!(appended.unwrap(), Some(1));
    let failure = append(
        &mut connection,
        vec![(extension("arrow.json"), strings(vec!["{}"]))],
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists, "{}", failure.message);

    let mut query = prepare_query(
        &mut connection,
        &format!(
            "SELECT TO_JSON_STRING(STRUCT(JSON_TYPE(j) AS jt, ST_ASTEXT(g) AS g, \
             ST_ASTEXT(st.g) AS stg, ARRAY(SELECT JSON_TYPE(x) FROM UNNEST(aj) AS x) AS ajt)), \
             TO_JSON_STRING(j), TO_JSON_STRING(aj), TO_JSON_STRING(st.j), s FROM `{table}`"
        ),
    );
    let batch = single_batch(&mut query);
    assert_eq!(
        (0..5)
            .map(|column| str_at(&batch, column))
            .collect::<Vec<_>>(),
        [
            r#"{"jt":"object","g":"POINT(1 2)","stg":"POINT(3 4)","ajt":["object","array"]}"#,
            r#"{"a":[1,2.5],"b":1}"#,
            r#"[{"x":1},[1,2]]"#,
            r#"{"y":2}"#,
            "POINT(1   2)",
        ]
    );
}

/// A stream that yields `first` and then ends the ingest: with a reader error, or by cancelling
/// the statement and offering one more batch, which the driver must refuse.
struct FailsAfterFirstBatch {
    schema: Arc<Schema>,
    first: Option<RecordBatch>,
    cancel: Option<Box<dyn CancelHandle>>,
    concurrent_append: Option<String>,
}

impl Iterator for FailsAfterFirstBatch {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        if let Some(table) = self.concurrent_append.take() {
            // Another connection commits into the newly created table before this ingest fails.
            std::thread::spawn(move || {
                execute_sql(
                    &mut connect_with_dataset(),
                    &format!("INSERT INTO `{table}` VALUES (99)"),
                );
            })
            .join()
            .unwrap();
        }
        let Some(cancel) = &self.cancel else {
            return Some(Err(ArrowError::ComputeError("the source broke".to_owned())));
        };
        // Off the ingest's own thread, which is inside the driver's runtime.
        std::thread::scope(|scope| scope.spawn(|| cancel.try_cancel()).join())
            .unwrap()
            .unwrap();
        Some(Ok(int64_batch(&self.schema, 99)))
    }
}

impl RecordBatchReader for FailsAfterFirstBatch {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

fn int64_batch(schema: &Arc<Schema>, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
    )
    .unwrap()
}

/// Runs an ingest of one batch holding `value` that then fails, with a reader error or by
/// cancellation, and returns the failure.
fn failed_ingest(
    connection: &mut BigQueryConnection,
    table: &str,
    mode: IngestMode,
    value: i64,
    cancel: bool,
    concurrent_append: bool,
) -> adbc_core::error::Error {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.to_owned());
    set_option(&mut ingest, OptionStatement::IngestMode, mode);
    let cancel = cancel.then(|| ingest.get_cancel_handle());
    ingest
        .bind_stream(Box::new(FailsAfterFirstBatch {
            first: Some(int64_batch(&schema, value)),
            schema,
            cancel,
            concurrent_append: concurrent_append.then(|| table.to_owned()),
        }))
        .unwrap();
    ingest.execute_update().unwrap_err()
}

/// A `replace` that fails while uploading leaves the target's old rows in place: the TRUNCATE
/// waits until every row is on the finalized stream.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn a_failed_replace_keeps_the_rows_it_would_have_replaced() {
    let table = scratch_table("failed_replace");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset();
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{table}` AS SELECT id FROM UNNEST([1, 2]) AS id"),
    );
    for (cancel, status) in [(false, Status::InvalidData), (true, Status::Cancelled)] {
        let failure = failed_ingest(
            &mut connection,
            &table,
            IngestMode::Replace,
            3,
            cancel,
            false,
        );
        assert_eq!(failure.status, status, "{}", failure.message);
        let mut query = prepare_query(
            &mut connection,
            &format!("SELECT TO_JSON_STRING(ARRAY_AGG(id ORDER BY id)) FROM `{table}`"),
        );
        assert_eq!(read_single_string(&mut query), "[1,2]", "cancel: {cancel}");
    }
}

/// A failed or cancelled `create` leaves its table available for append retries and other writers.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn a_failed_create_retains_its_table_and_can_retry_as_append() {
    let mut connection = connect_with_dataset();
    for (cancel, status) in [(false, Status::InvalidData), (true, Status::Cancelled)] {
        for concurrent_append in [false, true] {
            let table = scratch_table("failed_create");
            let _drop_table = DropTable::table(&table);
            let failure = failed_ingest(
                &mut connection,
                &table,
                IngestMode::Create,
                1,
                cancel,
                concurrent_append,
            );
            assert_eq!(failure.status, status, "{}", failure.message);
            assert!(failure.message.contains("committed no rows"), "{failure}");
            assert!(
                failure.message.contains("adbc.ingest.mode.append"),
                "{failure}"
            );
            assert_eq!(
                count_rows(&mut connection, &table),
                i64::from(concurrent_append)
            );

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            let mut retry = connection.new_statement().unwrap();
            set_option(&mut retry, OptionStatement::TargetTable, table.clone());
            set_option(&mut retry, OptionStatement::IngestMode, IngestMode::Append);
            retry.bind(int64_batch(&schema, 2)).unwrap();
            assert_eq!(retry.execute_update().unwrap(), Some(1));

            let mut query = prepare_query(
                &mut connection,
                &format!("SELECT TO_JSON_STRING(ARRAY_AGG(id ORDER BY id)) FROM `{table}`"),
            );
            assert_eq!(
                read_single_string(&mut query),
                if concurrent_append { "[2,99]" } else { "[2]" }
            );
        }
    }
}
