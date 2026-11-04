use std::cell::Cell;

use adbc_core::CancelHandle as _;
use adbc_core::error::Status;
use google_cloud_bigquery::model::StorageError;
use google_cloud_bigquery::model::storage_error::StorageErrorCode;
use google_cloud_gax::error::Error as GoogleError;
use google_cloud_gax::error::rpc::{Code, Status as GoogleStatus};

use crate::cancellation::{Cancellation, OperationCancelHandle};

use super::{STREAM_CLOSE_TIMEOUT, close_stream_call, commit_already_landed};
use crate::test_support::errors::assert_message;

/// One live `Operation`, already cancelled, standing in for a statement whose caller called
/// `AdbcStatementCancel` while the ingest was closing its stream.
fn cancelled_operation() -> (crate::cancellation::Operation, std::sync::Arc<Cancellation>) {
    let runtime = crate::runtime::new_runtime().unwrap();
    let clients = crate::clients::ClientState::new(&crate::config::DatabaseConfig::default());
    let cancellation = Cancellation::new(runtime, clients, "statement");
    let operation = cancellation.begin();
    OperationCancelHandle(cancellation.clone())
        .try_cancel()
        .unwrap();
    (operation, cancellation)
}

/// One service failure carrying `code`, as the generated client reports it.
fn refusal(code: Code) -> GoogleError {
    GoogleError::service(
        GoogleStatus::default()
            .set_code(code)
            .set_message("the service said so"),
    )
}

/// A cancel that lands while `BatchCommitWriteStreams` is in flight must not discard its answer.
///
/// `Operation::wait` is biased toward cancellation, so a gated commit could report `CANCELLED`
/// over durable rows, and a retry in `append` or `create_append` mode would duplicate them.
#[test]
fn a_cancel_does_not_discard_a_commit_that_already_succeeded() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (operation, _cancellation) = cancelled_operation();
    assert!(
        operation.check().is_err(),
        "the operation under test is not cancelled"
    );

    let committed = runtime.block_on(close_stream_call(
        || std::future::ready(Ok::<_, GoogleError>(7_i64)),
        "commit",
        "projects/p/datasets/d/tables/t/streams/s",
        "projects/p/datasets/d/tables/t",
    ));
    operation.finish();
    assert_eq!(
        committed.unwrap(),
        7,
        "a cancelled operation swallowed a successful commit"
    );
}

/// A failed close names the action, stream and table, and keeps the service's status.
///
/// As `Status::IO`, a table dropped mid-ingest would read as a retryable transport fault, and dbt
/// would rerun the whole ingest. A NOT_FOUND is not retried.
#[test]
fn a_refused_close_keeps_the_services_own_verdict() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let attempts = Cell::new(0_usize);
    let failure = runtime
        .block_on(close_stream_call(
            || {
                attempts.set(attempts.get() + 1);
                std::future::ready(Err::<(), _>(refusal(Code::NotFound)))
            },
            "commit",
            "projects/p/datasets/d/tables/t/streams/s",
            "projects/p/datasets/d/tables/t",
        ))
        .unwrap_err();
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(attempts.get(), 1, "a NOT_FOUND close was replayed");
    assert_message(&failure, &["commit", "streams/s", "the service said so"]);
    // The gRPC code goes out unflattened for a caller that cannot see the message.
    assert_eq!(failure.vendor_code, 5);
}

/// The ungated close is still bounded, so a service that never answers cannot hang a cancelled
/// ingest indefinitely. A paused clock auto-advances to the budget instead of waiting it out.
#[test]
fn an_unanswered_close_times_out() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let started = runtime.block_on(async { tokio::time::Instant::now() });
    let failure = runtime
        .block_on(close_stream_call(
            std::future::pending::<Result<(), GoogleError>>,
            "commit",
            "projects/p/datasets/d/tables/t/streams/s",
            "projects/p/datasets/d/tables/t",
        ))
        .unwrap_err();
    assert_eq!(failure.status, Status::Timeout);
    assert_message(&failure, &["commit", "streams/s", "no answer within 60 s"]);
    let elapsed = runtime.block_on(async { started.elapsed() });
    assert_eq!(elapsed, STREAM_CLOSE_TIMEOUT);
}

/// One `UNAVAILABLE` on the last call of an ingest is retried by the driver, since the pinned
/// client attaches no retry policy to the control-plane RPCs.
#[test]
fn a_transient_close_failure_is_replayed_before_it_is_reported() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let attempts = Cell::new(0_usize);
    let committed = runtime.block_on(close_stream_call(
        || {
            attempts.set(attempts.get() + 1);
            let outcome = if attempts.get() < 3 {
                Err(refusal(Code::Unavailable))
            } else {
                Ok(11_i64)
            };
            std::future::ready(outcome)
        },
        "commit",
        "projects/p/datasets/d/tables/t/streams/s",
        "projects/p/datasets/d/tables/t",
    ));
    assert_eq!(committed.unwrap(), 11);
    assert_eq!(attempts.get(), 3);

    // The budget is bounded: a service that is down stays down, and the caller hears about it.
    let attempts = Cell::new(0_usize);
    let failure = runtime
        .block_on(close_stream_call(
            || {
                attempts.set(attempts.get() + 1);
                std::future::ready(Err::<(), _>(refusal(Code::Unavailable)))
            },
            "finalize",
            "projects/p/datasets/d/tables/t/streams/s",
            "projects/p/datasets/d/tables/t",
        ))
        .unwrap_err();
    assert_eq!(failure.status, Status::IO);
    assert_eq!(attempts.get(), 3);
}

/// A replayed commit that already landed answers `STREAM_ALREADY_COMMITTED`. Only this ingest
/// knows the stream, so that is its own commit; reporting a refusal would invite a retry that
/// duplicates every row.
#[test]
fn a_commit_replayed_onto_its_own_success_is_a_success() {
    let already_committed = |code| vec![StorageError::new().set_code(code)];
    assert!(commit_already_landed(&already_committed(
        StorageErrorCode::StreamAlreadyCommitted
    )));
    // Nothing else is absorbed: an empty list is the ordinary success, and a real refusal, alone
    // or beside an already-committed entry, is still reported.
    assert!(!commit_already_landed(&[]));
    assert!(!commit_already_landed(&already_committed(
        StorageErrorCode::StreamFinalized
    )));
    assert!(!commit_already_landed(&[
        StorageError::new().set_code(StorageErrorCode::StreamAlreadyCommitted),
        StorageError::new().set_code(StorageErrorCode::TableNotFound),
    ]));
}
