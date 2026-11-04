use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StructArray, UInt8Array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::ArrowError;
use tokio::sync::{Semaphore, mpsc};

use super::{BufferedBigQueryRecordBatchReader, ResultPipelineCancellation, buffer_batch};

enum TestProducerOutcome {
    Success,
    Error,
    Panic,
}

fn test_buffered_reader(
    outcome: TestProducerOutcome,
) -> (RecordBatch, BufferedBigQueryRecordBatchReader) {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
            .unwrap();
    let runtime = crate::runtime::new_runtime().unwrap();
    let (sender, receiver) = mpsc::channel(1);
    let produced_batch = batch.clone();
    let buffer_budget = Arc::new(Semaphore::new(super::QUERY_RESULT_BUFFER_BYTES));
    let producer = runtime.spawn(async move {
        sender
            .send(buffer_batch(produced_batch, buffer_budget).await.unwrap())
            .await
            .unwrap();
        match outcome {
            TestProducerOutcome::Success => Ok(()),
            TestProducerOutcome::Error => {
                Err(ArrowError::ComputeError("query producer failed".to_owned()))
            }
            TestProducerOutcome::Panic => panic!("query producer panicked"),
        }
    });
    let reader = BufferedBigQueryRecordBatchReader {
        receiver,
        schema: batch.schema(),
        producer: Some(producer),
        runtime,
        cancellation: ResultPipelineCancellation::new(None),
    };
    (batch, reader)
}

/// Cloned sinks (one per parallel Storage Read stream) share one pipeline: every clone delivers
/// into the same channel under the same byte budget, and one cancellation stops them all.
#[test]
fn cloned_sinks_share_the_channel_budget_and_cancellation() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
            .unwrap();
    let runtime = crate::runtime::new_runtime().unwrap();
    let (sender, mut receiver) = mpsc::channel(4);
    let sink = super::ResultSink {
        schema: batch.schema(),
        cancellation: ResultPipelineCancellation::new(None),
        sender,
        buffer_budget: Arc::new(Semaphore::new(super::QUERY_RESULT_BUFFER_BYTES)),
        delivery_slots: Arc::new(Semaphore::new(1)),
        conversion_permits: Arc::new(Semaphore::new(1)),
    };
    let clone = sink.clone();

    assert!(runtime.block_on(sink.deliver(batch.clone())));
    assert!(runtime.block_on(clone.deliver(batch.clone())));
    assert_eq!(receiver.blocking_recv().unwrap().batch, batch);
    assert_eq!(receiver.blocking_recv().unwrap().batch, batch);

    // Cancelling through one clone stops every producer feeding the pipeline.
    sink.cancellation.cancel_local();
    assert!(!runtime.block_on(clone.deliver(batch)));
    assert!(
        runtime
            .block_on(clone.conversion_permit())
            .unwrap()
            .is_none()
    );
    assert!(runtime.block_on(clone.delivery_slot()).unwrap().is_none());
}

/// The byte budget bounds what a parallel producer holds, not just what it has handed over.
///
/// A batch can be charged only once it is decoded, so without delivery slots every producer task
/// (up to 32 under `bigquery.query.unordered_read`) could hold an uncharged batch. A stream takes
/// a slot before decoding and keeps it until `deliver` charges the batch, capping uncharged
/// batches at the slot count.
///
/// The slot count itself is pinned by
/// `a_pipelines_delivery_slots_match_its_runtimes_conversion_concurrency`; this test uses a small
/// fixed count, spends every slot, and asserts that the next stream is refused.
#[test]
fn the_delivery_slot_bounds_the_batches_outside_the_byte_budget() {
    let slots = 3;
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
            .unwrap();
    let runtime = crate::runtime::new_runtime().unwrap();
    let (sender, mut receiver) = mpsc::channel(slots + 1);
    let sink = super::ResultSink {
        schema: batch.schema(),
        cancellation: ResultPipelineCancellation::new(None),
        sender,
        // Room for exactly one batch, so no delivery after the first can be charged until the
        // reader has taken one.
        buffer_budget: Arc::new(Semaphore::new(super::buffered_batch_size(&batch))),
        delivery_slots: Arc::new(Semaphore::new(slots)),
        conversion_permits: Arc::new(Semaphore::new(slots)),
    };
    let waiting = sink.clone();

    runtime.block_on(async move {
        // One stream's batch is charged and sitting in the channel; the budget is now full.
        assert!(sink.deliver(batch.clone()).await);

        // Every slot is now held by a stream that has decoded and is blocked in `deliver`.
        let mut blocked = Vec::with_capacity(slots);
        for _ in 0..slots {
            let slot = sink.delivery_slot().await.unwrap().unwrap();
            let (stream, batch) = (sink.clone(), batch.clone());
            blocked.push(tokio::spawn(async move {
                let _slot = slot;
                stream.deliver(batch).await
            }));
        }

        // One more stream cannot begin decoding: its batch would be another outside the budget.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), waiting.delivery_slot())
                .await
                .is_err(),
            "a batch was decoded past the {slots} already waiting outside the byte budget"
        );

        // Draining releases the room those deliveries need and takes neither a slot nor budget,
        // so holding a slot across `deliver` cannot wedge even when every slot is held.
        for _ in 0..slots {
            receiver.recv().await.unwrap();
        }
        for delivery in blocked {
            assert!(delivery.await.unwrap());
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(5), waiting.delivery_slot())
                .await
                .unwrap()
                .unwrap()
                .is_some(),
            "the delivery slots were not released after the batches were charged"
        );
    });
}

/// A finished batch holds exactly the bytes it was charged, and holds them until the reader
/// takes it off the channel.
///
/// The permit travels with the batch, so draining, not the producer moving on, releases room for
/// waiting deliveries.
#[test]
fn completed_batch_holds_its_memory_budget_until_dequeued() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
            .unwrap();
    let batch_size = super::buffered_batch_size(&batch);
    let budget = Arc::new(Semaphore::new(batch_size));
    let runtime = crate::runtime::new_runtime().unwrap();
    let buffered = runtime
        .block_on(buffer_batch(batch, budget.clone()))
        .unwrap();

    // Exactly the batch's own size: nothing is left over, and nothing beyond it was taken.
    assert_eq!(budget.available_permits(), 0);
    assert!(budget.clone().try_acquire_owned().is_err());
    drop(buffered);
    assert_eq!(budget.available_permits(), batch_size);
    assert!(
        budget
            .try_acquire_many_owned(u32::try_from(batch_size).unwrap())
            .is_ok()
    );
}

/// A batch bigger than the whole budget is charged the whole budget rather than its own size.
///
/// The clamp lets a producer hold its delivery slot across `deliver`: an unclamped request could
/// never be satisfied, and the pipeline would wedge on that slot. A single row can retain more
/// than the budget when its buffer has spare capacity.
#[test]
fn a_batch_larger_than_the_budget_is_charged_the_whole_budget() {
    let mut values = Vec::with_capacity(super::QUERY_RESULT_BUFFER_BYTES / size_of::<i64>() + 1);
    values.push(42_i64);
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(values)) as ArrayRef)])
            .unwrap();
    assert!(
        super::buffered_batch_size(&batch) > super::QUERY_RESULT_BUFFER_BYTES,
        "the fixture no longer exceeds the budget it is meant to overflow"
    );

    let budget = Arc::new(Semaphore::new(super::QUERY_RESULT_BUFFER_BYTES));
    let runtime = crate::runtime::new_runtime().unwrap();
    let buffered = runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), buffer_batch(batch, budget.clone())).await
        })
        .expect("an oversize batch asked for more bytes than the budget can ever hold")
        .unwrap();

    assert_eq!(budget.available_permits(), 0);
    drop(buffered);
    assert_eq!(
        budget.available_permits(),
        super::QUERY_RESULT_BUFFER_BYTES,
        "the clamped charge was not returned in full"
    );
}

/// A pipeline's delivery slots are its runtime's conversion concurrency.
///
/// Fewer slots would throttle decoding; more would let uncharged batches outnumber the
/// conversions that can run at once. A semaphore reports only free permits, so both counts come
/// from a runtime nothing has taken a permit from.
#[test]
fn a_pipelines_delivery_slots_match_its_runtimes_conversion_concurrency() {
    let runtime = crate::runtime::new_runtime_with_result_conversion_concurrency(3).unwrap();
    assert_eq!(runtime.result_conversion_concurrency(), 3);
    let mut reader = super::spawn_result_reader(
        runtime,
        Arc::new(arrow_schema::Schema::empty()),
        None,
        |sink| async move {
            assert_eq!(sink.delivery_slots.available_permits(), 3);
            Ok(())
        },
    );
    // Draining joins the producer, so a failed assertion inside it surfaces here as its panic.
    assert!(reader.next().is_none());
}

/// Dropping a reader mid-stream ends its producer, whether the producer is delivering or parked
/// on something unrelated to the pipeline, such as a page request that never answers.
#[test]
fn dropping_a_reader_mid_stream_stops_its_producer() {
    struct Stopped(std::sync::mpsc::Sender<()>);
    impl Drop for Stopped {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    for parked in [false, true] {
        let runtime = crate::runtime::new_runtime().unwrap();
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
        )])
        .unwrap();
        let (stopped, producer_stopped) = std::sync::mpsc::channel();
        let mut reader = super::spawn_result_reader(
            runtime.clone(),
            batch.schema(),
            None,
            move |sink| async move {
                let _stopped = Stopped(stopped);
                while sink.deliver(batch.clone()).await {
                    if parked {
                        std::future::pending::<()>().await;
                    }
                }
                Ok(())
            },
        );
        reader.next().unwrap().unwrap();
        drop(reader);
        producer_stopped
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("the producer outlived its reader (parked: {parked})"));
        // Held to here, so the runtime shutting down cannot be what stopped the producer.
        drop(runtime);
    }
}

#[test]
fn buffered_reader_works_inside_a_multi_threaded_host_runtime() {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    for spawned in [false, true] {
        let consume = async {
            buffered_reader_returns_eof_after_successful_producer_completion();
            buffered_reader_returns_the_producer_error_after_buffered_data();
        };
        if spawned {
            host.block_on(host.spawn(consume)).unwrap();
        } else {
            host.block_on(consume);
        }
    }
}

#[test]
fn buffered_reader_returns_eof_after_successful_producer_completion() {
    let (batch, mut reader) = test_buffered_reader(TestProducerOutcome::Success);
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
    assert!(reader.next().is_none());
}

#[test]
fn buffered_reader_returns_the_producer_error_after_buffered_data() {
    let (batch, mut reader) = test_buffered_reader(TestProducerOutcome::Error);
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    let error = reader.next().unwrap().unwrap_err();
    assert!(
        error.to_string().contains("query producer failed"),
        "{error}"
    );
    assert!(reader.next().is_none());
}

/// A producer panic reaches the reader as the driver defect it is, not as a bare `JoinError`
/// without a status.
#[test]
fn buffered_reader_returns_a_task_panic_after_buffered_data() {
    let (batch, mut reader) = test_buffered_reader(TestProducerOutcome::Panic);
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    let error = reader.next().unwrap().unwrap_err();
    let ArrowError::ExternalError(source) = &error else {
        panic!("expected an embedded driver error, got {error:?}");
    };
    let source = source.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(source.status, adbc_core::error::Status::Internal);
    assert!(
        source.message.contains("query producer panicked")
            && source.message.contains(crate::error::DRIVER_DEFECT),
        "{}",
        source.message
    );
    assert!(reader.next().is_none());
}

/// Every column of a decoded Storage Read batch can slice the same IPC message body. The whole
/// backing allocation stays live, but sharing it among columns must not multiply its charge.
#[test]
fn a_sliced_batch_is_charged_its_backing_allocation_once() {
    const COLUMNS: usize = 9;
    const ROWS: usize = 16;
    let allocation = Int64Array::from((0..1024 * 1024_i64).collect::<Vec<i64>>());
    let batch = RecordBatch::try_from_iter((0..COLUMNS).map(|index| {
        (
            format!("v{index}"),
            Arc::new(allocation.slice(index * ROWS, ROWS)) as ArrayRef,
        )
    }))
    .unwrap();

    assert_eq!(
        super::buffered_batch_size(&batch),
        allocation.values().inner().capacity()
    );
    assert!(
        batch.get_array_memory_size() > super::QUERY_RESULT_BUFFER_BYTES,
        "the fixture no longer shows the difference the slice accounting makes"
    );
}

#[test]
fn retained_buffer_accounting_includes_nested_values_and_shared_validity() {
    let allocation = Int64Array::from((0..1024 * 1024_i64).collect::<Vec<i64>>());
    let nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false]));
    let retained = allocation.values().inner().capacity() + nulls.buffer().capacity();
    let column = Arc::new(Int64Array::new(
        allocation.values().slice(0, nulls.len()),
        Some(nulls.clone()),
    )) as ArrayRef;
    let structure = StructArray::from(
        RecordBatch::try_from_iter([("a", column.clone()), ("b", column)]).unwrap(),
    );
    let structure = StructArray::new(
        structure.fields().clone(),
        structure.columns().to_vec(),
        Some(nulls),
    );
    let batch = RecordBatch::try_from_iter([("nested", Arc::new(structure) as ArrayRef)]).unwrap();

    assert_eq!(super::buffered_batch_size(&batch), retained);
}

#[test]
fn retained_buffer_accounting_includes_an_external_response_allocation() {
    let response = Buffer::from(bytes::Bytes::from(vec![7_u8; 8 * 1024 * 1024]));
    let column = UInt8Array::new(
        ScalarBuffer::new(response.slice_with_length(32, 1), 0, 1),
        None,
    );
    let batch = RecordBatch::try_from_iter([("value", Arc::new(column) as ArrayRef)]).unwrap();

    assert_eq!(super::buffered_batch_size(&batch), response.capacity());
    assert_eq!(response.capacity(), 8 * 1024 * 1024);
}

#[test]
fn spare_buffer_capacity_applies_backpressure_until_a_batch_is_released() {
    const RETAINED_BYTES: usize = 8192;
    let make_batch = || {
        let mut values = Vec::with_capacity(RETAINED_BYTES / size_of::<i64>());
        values.push(42_i64);
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(values)) as ArrayRef)])
            .unwrap()
    };
    let runtime = crate::runtime::new_runtime().unwrap();
    let budget = Arc::new(Semaphore::new(RETAINED_BYTES));
    runtime.block_on(async {
        let first = buffer_batch(make_batch(), budget.clone()).await.unwrap();
        assert_eq!(budget.available_permits(), 0);
        let second = buffer_batch(make_batch(), budget.clone());
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut second)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), 0);
        drop(second);
        assert_eq!(budget.available_permits(), RETAINED_BYTES);
    });
}

/// Like every option error, the message names the option as the caller spelled it, and what is
/// accepted without echoing the value.
#[test]
fn a_nonpositive_prefetch_concurrency_names_the_option_and_what_it_accepts() {
    for (key, value) in [
        ("bigquery.query.prefetch_concurrency", 0),
        ("adbc.bigquery.sql.query.prefetch_concurrency", -3),
    ] {
        let failure = super::QueryResultOptions::default()
            .set_prefetch_concurrency(key, value)
            .unwrap_err();
        assert_eq!(failure.status, adbc_core::error::Status::InvalidArguments);
        assert_eq!(
            failure.message,
            format!("BigQuery option {key} must be a positive integer")
        );
    }
}
