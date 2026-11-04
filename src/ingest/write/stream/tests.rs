use std::time::Duration;

use super::*;

/// Upstream's non-exhaustive builder errors must be obtained through its public API.
#[test]
fn a_stream_type_mismatch_is_not_a_retryable_transport_failure() {
    use google_cloud_bigquery::model::ArrowSchema;
    use google_cloud_bigquery::write::stream_type::CommittedStream;

    use crate::clients::ClientState;
    use crate::config::ServiceEndpointKind;
    use crate::test_support::grpc::{MockPlan, StorageMock};

    let runtime = crate::runtime::new_runtime().unwrap();
    let mock = StorageMock::start(&runtime, MockPlan::default());
    let clients = ClientState::for_test_at(ServiceEndpointKind::Storage, mock.endpoint());
    let client = clients.write_client(&runtime).unwrap();
    let writer = runtime
        .block_on(
            client
                .create_stream::<PendingStream>("projects/p/datasets/d/tables/t")
                .build_arrow(ArrowSchema::new()),
        )
        .unwrap();
    let failure = runtime
        .block_on(
            client
                .attach_to_stream::<CommittedStream>(writer.write_stream())
                .build_arrow(ArrowSchema::new()),
        )
        .unwrap_err();
    assert!(matches!(failure, WriterBuilderError::TypeMismatch { .. }));
    assert!(!attach_error_is_transient(&failure));
    assert!(!attach_error_is_not_found(&failure));
    assert_eq!(
        reconnect_failure(writer.write_stream(), failure).status,
        Status::Internal
    );
    assert_eq!(mock.write_streams(), (1, 1));
}

/// The table-visibility backoff is upstream's full-jitter policy: each retry waits a uniform draw
/// from `[0, ceiling]`, with the ceiling doubling from `TABLE_VISIBILITY_RETRY_DELAY` until
/// `TABLE_VISIBILITY_MAX_RETRY_DELAY` clamps it. One draw proves little, so each attempt is
/// sampled and the observed maximum is checked against the ceiling from both sides.
#[test]
fn table_visibility_backoff_grows_and_saturates_like_its_reference_policy() {
    /// Draws per attempt. A uniform maximum falls short of nine tenths of its ceiling with
    /// probability `0.9^200`, about seven in ten billion, so the lower bound below is an
    /// assertion about the policy rather than about luck.
    const SAMPLES: u32 = 200;

    assert_eq!(TABLE_VISIBILITY_ATTEMPTS, 20);
    assert_eq!(TABLE_VISIBILITY_RETRY_DELAY, Duration::from_millis(100));
    assert_eq!(TABLE_VISIBILITY_MAX_RETRY_DELAY, Duration::from_secs(15));

    let mut observed_maxima =
        Vec::with_capacity(usize::try_from(TABLE_VISIBILITY_ATTEMPTS - 1).unwrap());
    for failure_count in 1..TABLE_VISIBILITY_ATTEMPTS {
        let multiplier = 1_u32 << (failure_count - 1).min(31);
        let ceiling = TABLE_VISIBILITY_RETRY_DELAY
            .saturating_mul(multiplier)
            .min(TABLE_VISIBILITY_MAX_RETRY_DELAY);
        let mut maximum = Duration::ZERO;
        for _ in 0..SAMPLES {
            let delay = TABLE_VISIBILITY_BACKOFF.delay(failure_count);
            assert!(
                delay <= ceiling,
                "failure {failure_count} delay {delay:?} exceeds {ceiling:?}"
            );
            maximum = maximum.max(delay);
        }
        assert!(
            maximum * 10 >= ceiling * 9,
            "failure {failure_count} never drew past {maximum:?} in {SAMPLES} samples, well \
             short of its {ceiling:?} ceiling"
        );
        observed_maxima.push(maximum);
    }

    // The delay really grows with the failure count: a `with_scaling` that stopped scaling would
    // leave every attempt drawing from the initial 100ms.
    assert!(
        observed_maxima[7] > observed_maxima[0],
        "the eighth attempt drew no longer than the first: {:?} against {:?}",
        observed_maxima[7],
        observed_maxima[0]
    );
    // From the ninth failure on, the doubling ceiling is past the maximum and stays clamped
    // there, which is what bounds the total wait across all twenty attempts.
    for (index, maximum) in observed_maxima.iter().enumerate().skip(8) {
        assert!(
            *maximum <= TABLE_VISIBILITY_MAX_RETRY_DELAY
                && *maximum * 10 >= TABLE_VISIBILITY_MAX_RETRY_DELAY * 9,
            "failure {} drew {maximum:?}, which is not saturated at \
             {TABLE_VISIBILITY_MAX_RETRY_DELAY:?}",
            index + 1
        );
    }
}

/// Creating the stream and landing the first payload share one budget; a budget each would double
/// the documented three-minute ceiling that callers size statement timeouts against.
#[test]
fn the_table_visibility_budget_spans_every_step_of_one_wait() {
    let mut budget = VisibilityBudget::new();
    let mut waits = 0;
    while budget.has_attempt_left() {
        budget.failures += 1;
        waits += 1;
        assert!(
            waits < TABLE_VISIBILITY_ATTEMPTS,
            "the budget never ran out"
        );
    }
    assert_eq!(waits, TABLE_VISIBILITY_ATTEMPTS - 1);
    // The other step now finds nothing left.
    assert!(!budget.has_attempt_left());
}

#[test]
fn cancelling_visibility_reattachment_finalizes_the_pending_stream() {
    use std::sync::Arc;

    use adbc_core::CancelHandle as _;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use tokio::sync::Notify;

    use crate::cancellation::{Cancellation, OperationCancelHandle};
    use crate::clients::ClientState;
    use crate::config::ServiceEndpointKind;
    use crate::test_support::grpc::{MockPlan, StorageMock, ipc_messages};

    let runtime = crate::runtime::new_runtime().unwrap();
    let gate = Arc::new((Notify::new(), Notify::new()));
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            reject_at: Some((0, 5)), // NOT_FOUND: the new stream is not visible yet.
            attach_gate: Some(gate.clone()),
            ..MockPlan::default()
        },
    );
    let clients = ClientState::for_test_at(ServiceEndpointKind::Storage, mock.endpoint());
    let client = clients.write_client(&runtime).unwrap();
    let cancellation = Cancellation::new(runtime.clone(), clients, "statement");
    let operation = cancellation.begin();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let (schema, mut batches) = ipc_messages(&schema, &[batch]);
    let target = AppendTarget {
        client: &client,
        schema: google_cloud_bigquery::model::ArrowSchema::new().set_serialized_schema(schema),
        table_path: "projects/p/datasets/d/tables/t",
        operation: &operation,
    };
    let rows = ArrowRecordBatch::new().set_serialized_record_batch(batches.remove(0));
    let outcome = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(pending_writer(&target, rows), async {
                gate.0.notified().await;
                OperationCancelHandle(cancellation).try_cancel().unwrap();
            })
            .0
        })
        .await
        .expect("ingest did not finish after cancelling reattachment")
    });
    let Err(failure) = outcome else {
        panic!("cancelled initialization unexpectedly succeeded");
    };
    assert_eq!(failure.status, Status::Cancelled);
    assert_eq!(mock.append_offsets(), vec![0]);
    assert_eq!(mock.finalize_calls(), 1);
    assert_eq!(mock.finalized(), (Some(0), false));
    gate.1.notify_one();
}
