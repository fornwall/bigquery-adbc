use std::sync::Arc;
use std::time::Duration;

use adbc_core::CancelHandle as _;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_bigquery::model::ArrowSchema;
use google_cloud_bigquery::write::stream_type::CommittedStream;
use tokio::sync::Notify;

use super::*;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::test_support::grpc::{MockPlan, StorageMock, ipc_messages};

/// Upstream's non-exhaustive builder errors must be obtained through its public API.
#[test]
fn a_stream_type_mismatch_is_not_a_retryable_transport_failure() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let mock = StorageMock::start(&runtime, MockPlan::default());
    let clients = ClientState::for_test_at(ServiceEndpointKind::Storage, mock.endpoint());
    let client = clients.write_client(&runtime, None).unwrap();
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

    for failure_count in 1..TABLE_VISIBILITY_ATTEMPTS {
        let multiplier = 1_u32 << (failure_count - 1).min(31);
        let ceiling = TABLE_VISIBILITY_RETRY_DELAY
            .saturating_mul(multiplier)
            .min(TABLE_VISIBILITY_MAX_RETRY_DELAY);
        let maximum = (0..SAMPLES)
            .map(|_| TABLE_VISIBILITY_BACKOFF.delay(failure_count))
            .max()
            .unwrap();
        assert!(
            maximum <= ceiling && maximum * 10 >= ceiling * 9,
            "failure {failure_count} drew up to {maximum:?} against a {ceiling:?} ceiling"
        );
    }
}

/// Creating the stream and landing the first payload share one budget; a budget each would double
/// the documented three-minute ceiling that callers size statement timeouts against.
#[test]
fn the_table_visibility_budget_spans_every_step_of_one_wait() {
    let mut budget = VisibilityBudget::default();
    let mut waits = 0;
    while budget.has_attempt_left() {
        budget.failures += 1;
        waits += 1;
    }
    assert_eq!(waits, TABLE_VISIBILITY_ATTEMPTS - 1);
}

#[test]
fn cancelling_visibility_reattachment_finalizes_the_pending_stream() {
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
    let client = clients.write_client(&runtime, None).unwrap();
    let cancellation = Cancellation::new(runtime.clone(), clients, "statement");
    let operation = cancellation.begin();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let (schema, mut batches) = ipc_messages(&schema, &[batch]);
    let target = AppendTarget {
        client: &client,
        schema: ArrowSchema::new().set_serialized_schema(schema),
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
