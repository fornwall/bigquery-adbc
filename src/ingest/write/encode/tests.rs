use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray};
use arrow_buffer::{Buffer, IntervalMonthDayNano};
use arrow_schema::{DataType, Field, IntervalUnit, Schema};

use crate::arrow_ipc_bigquery::reader::{decode_record_batch_message, decode_schema};

use super::*;
use crate::test_support::errors::assert_message;

#[test]
fn encoded_chunks_release_their_normalized_window() {
    use arrow_array::{Float32Array, Float64Array};

    let input = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Float32Array::from(vec![1.25; 1024 * 1024])) as ArrayRef,
    )])
    .unwrap();
    let normalized = Arc::new(crate::ingest::schema::normalize_schema(&input.schema()).unwrap());
    for reject in [false, true] {
        let (mut encoder, _) = ChunkEncoder::new(normalized.clone()).unwrap();
        let window = encoder
            .window(&input, 0, encoder.target_batch_bytes)
            .unwrap();
        let observer = window.column(0).to_data().buffers()[0].clone();
        assert_eq!(observer.capacity(), 8 * 1024 * 1024);
        assert!(observer.strong_count() > 1);
        if reject {
            encoder.target_batch_bytes = 0;
            encoder.max_batch_bytes = 0;
        }

        let chunk = encoder.encode(window, 0);
        assert_eq!(
            observer.strong_count(),
            1,
            "only the observer may retain the normalization buffer after encoding"
        );
        if reject {
            assert_eq!(chunk.unwrap_err().status, Status::InvalidArguments);
            continue;
        }
        let chunk = chunk.unwrap();
        let decoded = decode_record_batch_message(
            &Buffer::from(chunk.rows.serialized_record_batch),
            normalized.clone(),
        )
        .unwrap();
        assert_eq!(decoded.num_rows(), input.num_rows());
        assert_eq!(chunk.row_count, input.num_rows());
        assert!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .all(|value| value == Some(1.25))
        );
    }
}

/// A materialized batch that differs from the size it was admitted at means the size prediction
/// drifted from the writer, a driver defect, and the message says so.
#[test]
fn a_batch_that_differs_from_its_admitted_size_is_a_driver_defect() {
    let error = admitted_batch_bytes(Ok(vec![0; 64]), 128).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert_eq!(
        error.message,
        "BigQuery ingest Arrow record batch framed size was 64 bytes, predicted 128; this is a \
         driver defect, report it at https://github.com/fornwall/bigquery-adbc/issues"
    );
}

/// A reader that changes its schema mid-stream is unfixable without knowing which schema arrived.
#[test]
fn a_batch_with_a_foreign_schema_reports_both_schemas() {
    let bound = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["1"])) as ArrayRef],
    )
    .unwrap();

    let error = read_batch(Ok(batch), &bound).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert!(
        error
            .message
            .starts_with("BigQuery ingest input changed schema mid-stream: expected "),
        "{}",
        error.message
    );
    assert_message(&error, &["Int64", "Utf8"]);
}

#[test]
fn splits_ingest_batches_by_serialized_size_without_losing_rows() {
    let values = (0..8)
        .map(|index| format!("{index}:{}", "x".repeat(4096)))
        .collect::<Vec<_>>();
    // A month-day-nano interval column proves the standalone IPC messages carry non-trivial Arrow
    // layouts through the split, not just variable-length strings.
    let intervals = (0..8)
        .map(|index| {
            IntervalMonthDayNano::new(
                2 - index,
                3 - index,
                14_706_000 - i64::from(index) * 4_000_000,
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(values.clone())),
            Arc::new(IntervalMonthDayNanoArray::from(intervals.clone())),
        ],
    )
    .unwrap();
    let one_row_size = ChunkEncoder::with_limits(schema.clone(), usize::MAX, usize::MAX)
        .encode(batch.slice(0, 1), 0)
        .unwrap()
        .rows
        .serialized_record_batch
        .len();
    let target_size = one_row_size + 1;

    let (mut encoder, arrow_schema) = ChunkEncoder::new(schema).unwrap();
    encoder.target_batch_bytes = target_size;
    encoder.max_batch_bytes = target_size;
    let schema_bytes = arrow_schema.serialized_schema.to_vec();
    let mut chunks = Vec::new();
    let mut offset = 0;
    assert!(window_rows(&batch, 0, batch.num_rows(), target_size) < batch.num_rows());
    // Start with the whole skewed candidate: the encoder must shrink it without
    // materializing the oversized framed payload it discards.
    let mut window = batch.clone();
    RECORD_BATCH_SERIALIZATIONS.with(|count| count.set(0));
    REJECTED_CANDIDATE_BYTES.with(|bytes| bytes.set(0));
    loop {
        let chunk = encoder.encode(window, offset).unwrap();
        offset += chunk.row_count;
        chunks.push(chunk);
        if offset == batch.num_rows() {
            break;
        }
        window = encoder.window(&batch, offset, target_size).unwrap();
    }

    assert!(chunks.len() > 1);
    RECORD_BATCH_SERIALIZATIONS.with(|count| assert_eq!(count.get(), chunks.len()));
    assert_eq!(encoder.writer.body_materializations(), chunks.len());
    REJECTED_CANDIDATE_BYTES.with(|bytes| {
        assert!(
            bytes.get() > target_size,
            "at least one oversized candidate must be rejected before materialization"
        );
    });
    assert_eq!(
        chunks.iter().map(|chunk| chunk.row_count).sum::<usize>(),
        batch.num_rows()
    );
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.rows.serialized_record_batch.len() <= target_size)
    );

    let decoded_schema = decode_schema(&schema_bytes).unwrap();
    let mut round_trip_values = Vec::new();
    let mut round_trip_intervals = Vec::new();
    for chunk in chunks {
        let batch = decode_record_batch_message(
            &Buffer::from(chunk.rows.serialized_record_batch),
            Arc::clone(&decoded_schema),
        )
        .unwrap();
        round_trip_values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned()),
        );
        round_trip_intervals.extend(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap()),
        );
    }
    assert_eq!(round_trip_values, values);
    assert_eq!(round_trip_intervals, intervals);
}

#[test]
fn predicts_exact_framed_size_for_varied_batches() {
    let strings = [0, 1, 7, 63, 64, 65, 127, 128].map(|length| "x".repeat(length));
    let scalar = RecordBatch::try_from_iter([
        (
            "number",
            Arc::new(Int64Array::from_iter_values(0_i64..8)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from_iter_values(strings)) as ArrayRef,
        ),
    ])
    .unwrap();
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1_i64), Some(2), Some(3)]),
        None,
        Some(Vec::<Option<i64>>::new()),
        Some(vec![None, Some(4_i64)]),
    ]);
    let nested = RecordBatch::try_from_iter([("items", Arc::new(lists) as ArrayRef)]).unwrap();

    for batch in [&scalar, &nested] {
        let mut writer = Writer::new();
        let planned = writer.plan_batch(batch).unwrap();
        let predicted = planned.framed_size();
        assert_eq!(planned.write().unwrap().len(), predicted);
    }
}

#[test]
fn oversized_skewed_candidates_materialize_only_the_accepted_payload() {
    let mut values = Vec::with_capacity(64);
    values.push("x".repeat(2_000_000));
    values.extend((1..64).map(|index| format!("{index}:{}", "y".repeat(1024))));
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from_iter_values(values)) as ArrayRef,
    )])
    .unwrap();
    RECORD_BATCH_SERIALIZATIONS.with(|count| count.set(0));
    REJECTED_CANDIDATE_BYTES.with(|bytes| bytes.set(0));
    let mut encoder = ChunkEncoder::with_limits(batch.schema(), 1_000_000, 3_000_000);
    let chunk = encoder.encode(batch, 0).unwrap();

    assert_eq!(chunk.row_count, 1);
    assert!((2_000_000..=3_000_000).contains(&chunk.rows.serialized_record_batch.len()));
    RECORD_BATCH_SERIALIZATIONS.with(|count| assert_eq!(count.get(), 1));
    assert_eq!(encoder.writer.body_materializations(), 1);
    REJECTED_CANDIDATE_BYTES.with(|bytes| {
        assert!(
            bytes.get() >= 8_000_000,
            "skewed admission avoided only {} discarded payload bytes",
            bytes.get()
        );
    });
}

#[test]
fn permits_one_row_above_the_target_but_rejects_one_above_the_request_budget() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["x".repeat(4096)]))],
    )
    .unwrap();
    let encode = |target_size, max_size| {
        ChunkEncoder::with_limits(schema.clone(), target_size, max_size).encode(batch.clone(), 0)
    };
    let serialized_size = encode(usize::MAX, usize::MAX)
        .unwrap()
        .rows
        .serialized_record_batch
        .len();
    let chunk = encode(serialized_size - 1, serialized_size).unwrap();
    assert_eq!(chunk.row_count, 1);
    assert_eq!(chunk.rows.serialized_record_batch.len(), serialized_size);

    let mut rejected_encoder =
        ChunkEncoder::with_limits(schema.clone(), serialized_size - 1, serialized_size - 1);
    let error = rejected_encoder.encode(batch, 0).unwrap_err();
    assert_eq!(rejected_encoder.writer.body_materializations(), 0);
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        format!(
            "BigQuery ingest row 0 of its input batch serializes to {serialized_size} bytes, \
             more than the {} bytes of row data an AppendRows request can carry; shorten its \
             STRING or BYTES values, or split them across rows",
            serialized_size - 1
        )
    );
}

#[test]
fn reserves_request_and_schema_overhead_when_sizing_append_batches() {
    let payload_room = APPEND_ROWS_REQUEST_LIMIT_BYTES - APPEND_ROWS_REQUEST_ENVELOPE_BYTES;
    assert_eq!(
        append_batch_size_limits(0).unwrap(),
        (APPEND_ROWS_TARGET_BATCH_BYTES, payload_room)
    );
    let remaining = APPEND_ROWS_TARGET_BATCH_BYTES / 2;
    let schema_size = payload_room - remaining;
    assert_eq!(
        append_batch_size_limits(schema_size).unwrap(),
        (remaining, remaining)
    );
    let refused = append_batch_size_limits(payload_room).unwrap_err();
    assert_eq!(refused.status, Status::InvalidArguments);
    assert_eq!(
        refused.message,
        format!(
            "BigQuery ingest Arrow schema serializes to {payload_room} bytes, leaving no room for \
             rows in a 10000000-byte AppendRows request; ingest fewer columns, or shorten column \
             names and field metadata"
        )
    );
}

/// The whole request must fit, not just the record batch: a chunk at the target plus the schema
/// message every request repeats and the envelope stays under BigQuery's limit.
#[test]
fn a_chunk_encoded_at_the_target_size_leaves_a_whole_request_under_the_service_limit() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let (mut encoder, encoded_schema) = ChunkEncoder::new(schema.clone()).unwrap();
    let schema_size = encoded_schema.serialized_schema.len();
    assert!(schema_size > 0, "the schema message must be nonempty");

    // Many medium rows: enough total payload to force a split, with no single row near the
    // budget.
    let row_count = 512;
    let values: Vec<String> = (0..row_count).map(|_| "v".repeat(32 * 1024)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..i64::from(row_count))),
            Arc::new(StringArray::from_iter_values(values)),
        ],
    )
    .unwrap();

    let mut offset = 0;
    let mut chunks = 0;
    let mut largest_chunk = 0;
    while offset < batch.num_rows() {
        let window = encoder
            .window(&batch, offset, encoder.target_batch_bytes)
            .unwrap();
        let chunk = encoder.encode(window, offset).unwrap();
        let chunk_size = chunk.rows.serialized_record_batch.len();
        largest_chunk = largest_chunk.max(chunk_size);
        // What the service measures: the record batch, the repeated schema, and the envelope.
        let request_size = chunk_size + schema_size + APPEND_ROWS_REQUEST_ENVELOPE_BYTES;
        assert!(
            request_size < APPEND_ROWS_REQUEST_LIMIT_BYTES,
            "chunk of {} rows encodes a {request_size}-byte request, which reaches the \
             {APPEND_ROWS_REQUEST_LIMIT_BYTES}-byte AppendRows limit",
            chunk.row_count
        );
        chunks += 1;
        offset += chunk.row_count;
    }
    assert!(
        chunks > 1,
        "the input must be large enough to split, but it produced {chunks} chunk(s)"
    );
    // Without this the assertion above could pass on chunks nowhere near the budget.
    assert!(
        largest_chunk > encoder.target_batch_bytes * 9 / 10,
        "the largest chunk was {largest_chunk} bytes, which does not reach the {}-byte target",
        encoder.target_batch_bytes
    );
}

/// A large input batch is normalized one request-sized window at a time, never whole: a
/// `LargeUtf8` column over 2 GiB in one batch (pandas 3 and Polars produce them) would not fit the
/// 32-bit offsets of its normalized `Utf8` form, and a whole-batch copy would double the memory
/// an ingest needs. A rejection in a later window still names the row of the input batch.
#[test]
fn a_large_batch_is_normalized_one_request_sized_window_at_a_time() {
    use arrow_array::{Array as _, Date32Array, LargeStringArray};

    const ROWS: usize = 20_000;
    const BAD_ROW: usize = 15_000;
    let label = "x".repeat(100);
    let input = RecordBatch::try_from_iter([
        (
            "label",
            Arc::new(LargeStringArray::from_iter_values(
                (0..ROWS).map(|_| &label),
            )) as ArrayRef,
        ),
        (
            "day",
            Arc::new(Date32Array::from_iter_values(
                (0..ROWS).map(|row| if row == BAD_ROW { 3_000_000 } else { 19_000 }),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let normalized = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
    ]));
    let target = 100_000;
    let mut encoder = ChunkEncoder::with_limits(normalized, target, 2 * target);

    let mut offset = 0;
    let mut windows = 0;
    let failure = loop {
        let window = match encoder.window(&input, offset, target) {
            Ok(window) => window,
            Err(failure) => break failure,
        };
        let labels = window
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            labels.value_data().len() <= 2 * target,
            "a window addressed {} bytes of labels",
            labels.value_data().len()
        );
        offset += encoder.encode(window, offset).unwrap().row_count;
        windows += 1;
    };
    assert!(
        windows > 10,
        "only {windows} windows preceded the rejection"
    );
    assert!(offset <= BAD_ROW);
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        format!(
            "Arrow date ingest field \"day\" contains an out-of-range value at row \
             {BAD_ROW}; BigQuery supports years 1 to 9999"
        )
    );
}

#[test]
fn repeated_dictionary_values_and_list_views_normalize_in_bounded_windows() {
    use arrow_array::cast::AsArray as _;
    use arrow_array::{Array as _, DictionaryArray, LargeListViewArray, ListViewArray};

    let label = "x".repeat(1_000);
    let values = Arc::new(StringArray::from_iter_values(
        std::iter::once(label.as_str()).chain(std::iter::repeat_n("", 99)),
    ));
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(DictionaryArray::new(
            Int64Array::from(vec![0; 1_000]),
            values.clone(),
        )),
        Arc::new(ListViewArray::new(
            item.clone(),
            vec![0; 1_000].into(),
            vec![1; 1_000].into(),
            values.clone(),
            None,
        )),
        Arc::new(LargeListViewArray::new(
            item,
            vec![0; 1_000].into(),
            vec![1; 1_000].into(),
            values,
            None,
        )),
    ];
    for column in inputs {
        let input = RecordBatch::try_from_iter([("value", column)]).unwrap();
        let normalized =
            Arc::new(crate::ingest::schema::normalize_schema(&input.schema()).unwrap());
        let target = 10_000;
        let mut encoder = ChunkEncoder::with_limits(normalized, target, 2 * target);
        let mut offset = 0;
        let mut windows = 0;
        while offset < input.num_rows() {
            let window = encoder.window(&input, offset, target).unwrap();
            let column = window.column(0);
            let strings = match column.data_type() {
                DataType::List(_) => column.as_list::<i32>().values().as_string::<i32>(),
                _ => column.as_string::<i32>(),
            };
            assert!(strings.value_data().len() <= 2 * target);
            assert!(strings.iter().all(|value| value == Some(label.as_str())));
            offset += encoder.encode(window, offset).unwrap().row_count;
            windows += 1;
        }
        assert!(windows > 50, "the input used only {windows} windows");
    }
}

#[test]
fn repeated_empty_list_values_bound_their_normalized_offsets() {
    use arrow_array::cast::AsArray as _;
    use arrow_array::{
        Array as _, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray,
        ListViewArray, StringViewArray,
    };

    let values: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![""; 1_000])),
        Arc::new(LargeStringArray::from(vec![""; 1_000])),
        Arc::new(StringViewArray::from(vec![""; 1_000])),
        Arc::new(BinaryArray::from(vec![b"".as_slice(); 1_000])),
        Arc::new(LargeBinaryArray::from(vec![b"".as_slice(); 1_000])),
        Arc::new(BinaryViewArray::from(vec![b"".as_slice(); 1_000])),
    ];
    for values in values {
        let column = Arc::new(ListViewArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), false)),
            vec![0; 100].into(),
            vec![1_000; 100].into(),
            values,
            None,
        ));
        let input = RecordBatch::try_from_iter([("value", column as ArrayRef)]).unwrap();
        let normalized =
            Arc::new(crate::ingest::schema::normalize_schema(&input.schema()).unwrap());
        let target = 10_000;
        let mut encoder = ChunkEncoder::with_limits(normalized, target, 2 * target);
        let mut offset = 0;
        while offset < input.num_rows() {
            let window = encoder.window(&input, offset, target).unwrap();
            let values = window.column(0).as_list::<i32>().values();
            let bytes: usize = values.to_data().buffers().iter().map(Buffer::len).sum();
            assert!(
                bytes <= 2 * target + 4,
                "normalized {bytes} bytes of offsets"
            );
            assert_eq!(values.len(), 1_000 * window.num_rows());
            offset += encoder.encode(window, offset).unwrap().row_count;
        }
    }
}
