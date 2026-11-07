use adbc_core::error::Status;
use google_cloud_bigquery::error::{AppendError, WriterBuilderError};
use google_cloud_bigquery::model::StorageError;
use google_cloud_bigquery::model::row_error::RowErrorCode;
use google_cloud_bigquery::model::storage_error::StorageErrorCode;
use google_cloud_gax::error::Error as GoogleError;
use google_cloud_gax::error::rpc::{Code, Status as GoogleStatus};

use super::*;
use crate::error::{REPORTED_ERRORS, summarize_errors};
use crate::ingest::write::stream::attach_error_is_not_found;
use crate::test_support::errors::assert_message_omits;

/// One `AppendError::Rpc` carrying `code`.
fn rpc_append_error(code: Code) -> AppendError {
    AppendError::from(GoogleError::service(GoogleStatus::default().set_code(code)))
}

/// A transport failure ends the writer's AppendRows call, so every request still riding on it
/// resolves to `UnexpectedEndOfStream`, which reconnecting exists to replay.
#[test]
fn classifies_retryable_append_errors_for_replay() {
    // The explicit offset makes a replay exactly-once, so every transient status upstream's own
    // append retry policy replays is replayed here too, INTERNAL and ABORTED included.
    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::Aborted,
        Code::ResourceExhausted,
    ] {
        assert!(
            append_error_is_retryable(&rpc_append_error(code)),
            "{code:?}"
        );
    }
    assert!(append_error_is_retryable(
        &AppendError::UnexpectedEndOfStream
    ));
    for code in [
        Code::NotFound,
        Code::InvalidArgument,
        Code::AlreadyExists,
        Code::Cancelled,
    ] {
        assert!(
            !append_error_is_retryable(&rpc_append_error(code)),
            "{code:?}"
        );
    }

    let attach = |code| {
        WriterBuilderError::from(GoogleError::service(GoogleStatus::default().set_code(code)))
    };
    assert!(attach_error_is_not_found(&attach(Code::NotFound)));
    assert!(!attach_error_is_not_found(&attach(Code::Unavailable)));
}

/// An append left unanswered counts as its call ending, which reconnecting replays: a connection
/// that dies silently never resolves its appends, and no keepalive would notice (UP-102). A paused
/// clock auto-advances to the deadline instead of waiting it out.
#[test]
fn an_unanswered_append_ends_its_call() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let elapsed = runtime.block_on(async {
        let started = tokio::time::Instant::now();
        let unanswered = std::future::pending::<std::result::Result<(), AppendError>>();
        let failure = append_answer(unanswered).await.unwrap_err();
        assert!(append_error_is_retryable(&failure));
        started.elapsed()
    });
    assert_eq!(elapsed, APPEND_ANSWER_TIMEOUT);
}

/// The offsets are driver-assigned and never reused, so the only write that can already occupy
/// one is this driver's own earlier send of the very same request.
#[test]
fn recognizes_an_append_that_already_landed() {
    let landed = rpc_append_error(Code::AlreadyExists);
    assert!(replay_landed(true, &landed));
    assert!(!replay_landed(false, &landed));
    assert!(!replay_landed(
        true,
        &rpc_append_error(Code::DeadlineExceeded)
    ));
    assert!(!replay_landed(true, &AppendError::UnexpectedEndOfStream));
}

/// The pipeline is bounded by the bytes its queued payloads pin, not by a handful of requests:
/// a few requests' worth of small appends would leave the stream waiting on round trips. Its first
/// append is always admitted.
#[test]
fn the_append_pipeline_is_bounded_by_queued_bytes_and_requests() {
    const LARGE: usize = 9_000_000;
    const SMALL: usize = 60_000;

    assert!(pipeline_has_room(0, 0, APPEND_PIPELINE_BYTES + 1));
    assert!(pipeline_has_room(3, 3 * LARGE, 3_000_000));
    assert!(!pipeline_has_room(3, 3 * LARGE, LARGE));
    assert!(pipeline_has_room(
        APPEND_PIPELINE_REQUESTS - 1,
        (APPEND_PIPELINE_REQUESTS - 1) * SMALL / 10,
        SMALL / 10
    ));
    assert!(!pipeline_has_room(
        APPEND_PIPELINE_REQUESTS,
        APPEND_PIPELINE_REQUESTS * SMALL / 10,
        SMALL / 10
    ));
    assert!(pipeline_has_room(100, 100 * SMALL, SMALL));
}

/// A drift in the driver's offset accounting would otherwise surface only at the finalize
/// row-count check, after every byte has shipped.
#[test]
fn a_mismatched_acknowledged_offset_names_itself_as_a_driver_defect() {
    check_acknowledged_offset(Some(4_096), 4_096, "p.d.t").unwrap();
    // Only the default stream leaves the offset unset, and this driver never opens one; an
    // absent offset is nothing to accuse the accounting of.
    check_acknowledged_offset(None, 4_096, "p.d.t").unwrap();

    let error = check_acknowledged_offset(Some(4_096), 8_192, "p.d.t").unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert_eq!(
        error.message,
        "BigQuery acknowledged an ingest append to p.d.t at stream offset 4096, but it was sent \
         at offset 8192; this is a driver defect, report it at \
         https://github.com/fornwall/bigquery-adbc/issues"
    );
}

/// A load that fails on every row would otherwise build an unbounded message, copied again
/// into a `CString` at the FFI boundary; the tail says how many were left out.
#[test]
fn stream_error_summaries_are_capped_and_count_the_rest() {
    let entries = |count: usize| (0..count).map(|index| format!("e{index}"));
    assert_eq!(summarize_errors(entries(0)), "");
    assert_eq!(summarize_errors(entries(2)), "e0; e1");

    let full = summarize_errors(entries(REPORTED_ERRORS));
    assert_eq!(full.split("; ").count(), REPORTED_ERRORS);
    assert!(!full.contains("more"), "{full}");

    let capped = summarize_errors(entries(REPORTED_ERRORS + 3));
    assert!(capped.starts_with("e0; e1; "), "{capped}");
    assert!(
        capped.ends_with(&format!("e{}; ... and 3 more", REPORTED_ERRORS - 1)),
        "{capped}"
    );
    assert!(!capped.contains(&format!("e{REPORTED_ERRORS}")), "{capped}");
}

/// A commit-time stream error is a data or stream-state failure carrying a structured code, not
/// I/O: reporting it as IO invites retrying an append that can never succeed. The rendering also
/// has to name the stream that failed.
#[test]
fn commit_stream_errors_are_classified_by_their_storage_code() {
    for (code, expected) in [
        (
            StorageErrorCode::SchemaMismatchExtraFields,
            Status::InvalidData,
        ),
        (StorageErrorCode::Unspecified, Status::InvalidData),
        (StorageErrorCode::TableNotFound, Status::NotFound),
        (StorageErrorCode::StreamNotFound, Status::NotFound),
        (StorageErrorCode::InvalidStreamState, Status::InvalidState),
        (
            StorageErrorCode::StreamAlreadyCommitted,
            Status::InvalidState,
        ),
        (StorageErrorCode::OffsetOutOfRange, Status::InvalidState),
        (StorageErrorCode::CmekNotProvided, Status::InvalidArguments),
        (StorageErrorCode::KmsPermissionDenied, Status::Unauthorized),
        (StorageErrorCode::KmsServiceError, Status::IO),
    ] {
        assert_eq!(storage_error_status(&code), expected, "{code}");
    }
    // A code this driver has never seen must not be claimed as retryable, or as bad data.
    assert_eq!(
        storage_error_status(&StorageErrorCode::from("SOME_FUTURE_CODE")),
        Status::Unknown
    );

    let failed = StorageError::new()
        .set_code(StorageErrorCode::SchemaMismatchExtraFields)
        .set_entity("projects/p/datasets/d/tables/t/streams/s")
        .set_error_message("Input schema has more fields than BigQuery schema");
    assert_eq!(
        format_storage_error(&failed),
        "SCHEMA_MISMATCH_EXTRA_FIELDS on projects/p/datasets/d/tables/t/streams/s: Input schema \
         has more fields than BigQuery schema"
    );
    assert_eq!(
        format_storage_error(&StorageError::new().set_error_message("no entity")),
        "STORAGE_ERROR_CODE_UNSPECIFIED: no entity"
    );
}

/// Upstream's own policy reaches an ingest's control-plane RPCs only in part (UP-83), and
/// `Write::builder()` exposes none. This classifier mirrors upstream's `RetryableErrors` minus
/// `CANCELLED`, which here is the caller's own `AdbcStatementCancel`.
#[test]
fn classifies_the_control_plane_statuses_worth_another_attempt() {
    let refused = |code| GoogleError::service(GoogleStatus::default().set_code(code));
    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::ResourceExhausted,
        Code::Aborted,
    ] {
        assert!(google_error_is_transient(&refused(code)), "{code:?}");
    }
    // A verdict the service reached and stands by, and the one code that is the caller's own
    // cancellation rather than a blip: replaying either is wrong.
    for code in [
        Code::NotFound,
        Code::PermissionDenied,
        Code::InvalidArgument,
        Code::AlreadyExists,
        Code::Unauthenticated,
        Code::Cancelled,
    ] {
        assert!(!google_error_is_transient(&refused(code)), "{code:?}");
    }

    // The attach classifier applies the same rule through `WriterBuilderError`; a NOT_FOUND
    // stream is not a transient fault.
    let attach = |code| WriterBuilderError::from(refused(code));
    assert!(attach_error_is_transient(&attach(Code::Unavailable)));
    assert!(!attach_error_is_transient(&attach(Code::NotFound)));
}

/// A refused append keeps the service's verdict and code, and is rendered from the RPC error
/// itself rather than through upstream's `AppendError` wrapper prose.
#[test]
fn a_refused_append_keeps_the_rpc_status_and_code() {
    let failure = append_failure(
        "projects/p/datasets/d/tables/t",
        rpc_append_error(Code::NotFound),
        0,
    );
    assert_eq!(failure.status, Status::NotFound);
    assert!(
        failure.message.starts_with(
            "failed to append rows to BigQuery ingest target projects/p/datasets/d/tables/t: "
        ),
        "{}",
        failure.message
    );
    assert_message_omits(&failure, &["the operation failed"]);
    assert_eq!(
        failure.details,
        Some(vec![(
            crate::error::DETAIL_RPC_CODE.to_owned(),
            b"5".to_vec()
        )])
    );
}

#[test]
fn a_closed_append_stream_is_io() {
    let closed = append_failure("p.d.t", AppendError::UnexpectedEndOfStream, 0);
    assert_eq!(closed.status, Status::IO);
    assert!(
        closed
            .message
            .starts_with("failed to append rows to BigQuery ingest target p.d.t: "),
        "{}",
        closed.message
    );
}

#[test]
fn a_rejection_naming_no_row_says_so() {
    assert_eq!(
        row_failure_message(&[], 0),
        "BigQuery rejected the batch without naming a row, and appended none"
    );
}

/// A wholesale rejection renders the service's own per-row account, capped, never a `Debug`
/// dump of the rows.
#[test]
fn rejected_rows_are_summarized_without_debug_noise() {
    let rows: Vec<RowError> = (0..25)
        .map(|index| {
            RowError::new()
                .set_index(index)
                .set_code(RowErrorCode::FieldsError)
                .set_message(format!("field {index} is not a valid TIMESTAMP"))
        })
        .collect();

    let message = row_failure_message(&rows, 0);
    assert!(
        message.starts_with(
            "BigQuery rejected 25 rows and appended none; correct or remove them and retry: \
             input row 0 (row 0 of the append at stream offset 0): FIELDS_ERROR: field 0 is not \
             a valid TIMESTAMP; input row 1 "
        ),
        "{message}"
    );
    // Capped like a job failure's error list.
    assert!(message.ends_with("; ... and 5 more"), "{message}");
    assert!(!message.contains("_unknown_fields"));
}

#[test]
fn a_single_rejected_row_reads_in_the_singular() {
    let rows = vec![RowError::new().set_index(7).set_message("bad value")];

    assert_eq!(
        row_failure_message(&rows, 0),
        "BigQuery rejected 1 row and appended none; correct or remove them and retry: input row \
         7 (row 7 of the append at stream offset 0): bad value"
    );
}

/// A rejected row is reported by both the caller's row number and the service's chunk-relative
/// index, and by the latter alone when the sum overflows.
#[test]
fn a_rejected_row_is_reported_by_the_callers_own_row_number() {
    let rows = vec![
        RowError::new()
            .set_index(7)
            .set_code(RowErrorCode::FieldsError)
            .set_message("bad value"),
    ];

    assert_eq!(
        row_failure_message(&rows, 40_960),
        "BigQuery rejected 1 row and appended none; correct or remove them and retry: input row \
         40967 (row 7 of the append at stream offset 40960): FIELDS_ERROR: bad value"
    );

    // An index that cannot be made absolute keeps the account the service actually gave.
    let overflowing = vec![RowError::new().set_index(i64::MAX).set_message("bad value")];
    assert_eq!(
        row_failure_message(&overflowing, 1),
        format!(
            "BigQuery rejected 1 row and appended none; correct or remove them and retry: row {} \
             of the append at stream offset 1: bad value",
            i64::MAX
        )
    );
}
