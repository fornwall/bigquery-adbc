//! Bounded append pipelining and retries. Explicit offsets prevent replay from duplicating rows.
//! The pending writer's control-plane RPCs get little or no SDK retry (UP-83), so they retry here.
//!
//! Nothing below this module notices a connection that dies silently, since the gRPC channel has
//! no keepalive (UP-102), so every wait on the service here has a deadline.

use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use google_cloud_bigquery::error::AppendError;
use google_cloud_bigquery::model::storage_error::StorageErrorCode;
use google_cloud_bigquery::model::{ArrowRecordBatch, RowError, StorageError};
use google_cloud_bigquery::write::AppendFuture;
use google_cloud_bigquery::write::PendingWriter;
use google_cloud_bigquery::write::format::Arrow;
use google_cloud_gax::error::rpc::Code;

use crate::cancellation::{self, Operation, gate};
use crate::diag::diag;
use crate::error::{
    driver_defect, error, google_error, google_error_code, google_error_is_transient,
    summarize_errors,
};

use super::AppendTarget;
use super::stream::{attach_error_is_transient, reattach, reconnect_failure};

// One short round of retries per append, with a doubling delay.
const APPEND_RETRY_ATTEMPTS: usize = 4;
const APPEND_RETRY_DELAY: Duration = Duration::from_millis(250);
// How much may be queued on the pending stream at once. Every queued request pins its serialized
// payload until it is acknowledged, so the bytes bound memory (three full requests); the count
// lets small appends overlap their round trips, as in Google's own writers (1,000 requests).
const APPEND_PIPELINE_BYTES: usize = 30_000_000;
const APPEND_PIPELINE_REQUESTS: usize = 1_000;
// One short round of retries per control-plane RPC, enough to survive a load-balancer drain;
// waiting for a new table to become visible is the budget in [`super::stream`].
const CONTROL_PLANE_RETRY_ATTEMPTS: usize = 3;
const CONTROL_PLANE_RETRY_DELAY: Duration = Duration::from_millis(200);
/// Total timeout for one control-plane RPC, including its retries. Finalize and commit run outside
/// caller cancellation, so this also bounds their delay after a statement is cancelled.
pub(super) const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the oldest append may go unanswered before its call counts as ended. Replaying costs
/// nothing if the service was only slow, so this need not be generous.
const APPEND_ANSWER_TIMEOUT: Duration = Duration::from_secs(300);

/// Retries transient control-plane failures. The outer error is cancellation or the timeout,
/// reported under `context`, and the inner the RPC failure. Stream-closing RPCs pass no operation;
/// see [`super::close_stream_call`].
pub(super) async fn control_plane_retry<T, E, Call, Fut>(
    context: &str,
    operation: Option<&Operation>,
    is_transient: impl Fn(&E) -> bool,
    mut call: Call,
) -> Result<std::result::Result<T, E>>
where
    Call: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let attempts = async {
        let mut attempts_left = CONTROL_PLANE_RETRY_ATTEMPTS - 1;
        let mut delay = CONTROL_PLANE_RETRY_DELAY;
        loop {
            match gate(operation, call()).await? {
                Err(source) if attempts_left > 0 && is_transient(&source) => {
                    attempts_left -= 1;
                    back_off(operation, &mut delay).await?;
                }
                outcome => return Ok(outcome),
            }
        }
    };
    tokio::time::timeout(CONTROL_PLANE_TIMEOUT, attempts)
        .await
        .unwrap_or_else(|_| {
            Err(error(
                format!(
                    "{context}: no answer within {} s",
                    CONTROL_PLANE_TIMEOUT.as_secs()
                ),
                Status::Timeout,
            ))
        })
}

async fn back_off(operation: Option<&Operation>, delay: &mut Duration) -> Result<()> {
    cancellation::sleep(operation, *delay).await?;
    *delay = delay.saturating_mul(2);
    Ok(())
}

/// An append's answer, or the end of its call once [`APPEND_ANSWER_TIMEOUT`] passes without one.
async fn append_answer<T>(
    answer: impl Future<Output = std::result::Result<T, AppendError>>,
) -> std::result::Result<T, AppendError> {
    tokio::time::timeout(APPEND_ANSWER_TIMEOUT, answer)
        .await
        .unwrap_or(Err(AppendError::UnexpectedEndOfStream))
}

/// Explicit offsets make ambiguous INTERNAL and ABORTED outcomes safe to replay.
fn append_error_is_retryable(source: &AppendError) -> bool {
    match source {
        AppendError::Rpc { source, .. } => google_error_is_transient(source),
        // The runner's exit drops every request still queued on that writer.
        AppendError::UnexpectedEndOfStream => true,
        _ => false,
    }
}

/// ALREADY_EXISTS acknowledges a replay that landed. OFFSET_ALREADY_EXISTS is an undecoded
/// status detail, so match its transport code, used for nothing else on offset-carrying appends.
fn replay_landed(replayed: bool, source: &AppendError) -> bool {
    replayed
        && matches!(source, AppendError::Rpc { source, .. }
        if matches!(google_error_code(source), Some(Code::AlreadyExists)))
}

/// Log the code, never the service message, which can quote row values.
fn append_error_cause(source: &AppendError) -> &'static str {
    match source {
        AppendError::Rpc { source, .. } => {
            google_error_code(source).map_or("a transport failure", |code| code.name())
        }
        AppendError::UnexpectedEndOfStream => "the end of its AppendRows call",
        _ => "a failure",
    }
}

/// Sends one append and retries it at its original offset on the same stream, replacing the dead
/// AppendRows connection. `replay` says its call already ended with the append sent, so it
/// reconnects first and has spent an attempt; only a prior send makes `ALREADY_EXISTS` success,
/// since otherwise it signals an offset defect.
pub(super) async fn append_with_retry(
    target: &AppendTarget<'_>,
    writer: &mut PendingWriter<Arrow>,
    rows: ArrowRecordBatch,
    offset: i64,
    mut replay: bool,
) -> Result<()> {
    let mut attempts_left = APPEND_RETRY_ATTEMPTS - 1 - usize::from(replay);
    let mut delay = APPEND_RETRY_DELAY;
    loop {
        if replay {
            // Reconnection failures spend the same retry budget as append failures.
            match reattach(target, writer).await? {
                Ok(()) => {}
                Err(source) if attempts_left > 0 && attach_error_is_transient(&source) => {
                    attempts_left -= 1;
                    back_off(Some(target.operation), &mut delay).await?;
                    continue;
                }
                Err(source) => return Err(reconnect_failure(writer.write_stream(), source)),
            }
        }
        match target
            .operation
            .wait(append_answer(
                writer.append(rows.clone()).set_offset(offset).send(),
            ))
            .await?
        {
            Ok(response) => {
                return check_acknowledged_offset(response.offset, offset, target.table_path);
            }
            Err(source) if replay_landed(replay, &source) => return Ok(()),
            Err(source) if attempts_left > 0 && append_error_is_retryable(&source) => {
                diag!(
                    Debug,
                    "replaying a BigQuery ingest append to {} at stream offset {offset} on \
                     a reconnected stream, after {}",
                    target.table_path,
                    append_error_cause(&source)
                );
                attempts_left -= 1;
                replay = true;
                back_off(Some(target.operation), &mut delay).await?;
            }
            Err(source) => return Err(append_failure(target.table_path, source, offset)),
        }
    }
}

/// The acknowledged offset is the append's first row, not the post-append cursor.
fn check_acknowledged_offset(
    acknowledged: Option<i64>,
    offset: i64,
    table_path: &str,
) -> Result<()> {
    match acknowledged {
        Some(acknowledged) if acknowledged != offset => Err(driver_defect(format_args!(
            "BigQuery acknowledged an ingest append to {table_path} at stream offset \
             {acknowledged}, but it was sent at offset {offset}"
        ))),
        _ => Ok(()),
    }
}

/// Keep the failure prefix stable for dbt's message matching.
fn append_failure(table_path: &str, source: AppendError, offset: i64) -> adbc_core::error::Error {
    let context = format!("failed to append rows to BigQuery ingest target {table_path}");
    match source {
        AppendError::Rpc { source, .. } => google_error(&context, source),
        // Avoid upstream's uncapped Debug dump of rejected rows and internal fields.
        AppendError::RowErrors { row_errors, .. } => error(
            format!("{context}: {}", row_failure_message(&row_errors, offset)),
            Status::InvalidData,
        ),
        source => {
            let status = if matches!(source, AppendError::UnexpectedEndOfStream) {
                Status::IO
            } else {
                Status::Unknown
            };
            error(format!("{context}: {source}"), status)
        }
    }
}

fn row_failure_message(rows: &[RowError], offset: i64) -> String {
    if rows.is_empty() {
        return "BigQuery rejected the batch without naming a row, and appended none".to_owned();
    }
    format!(
        "BigQuery rejected {} {} and appended none; correct or remove them and retry: {}",
        rows.len(),
        if rows.len() == 1 { "row" } else { "rows" },
        summarize_errors(rows.iter().map(|row| format_row_error(row, offset))),
    )
}

/// Report both the caller's row number and the chunk-relative index used by the service.
fn format_row_error(source: &RowError, offset: i64) -> String {
    let message = if source.message.is_empty() {
        "the row was rejected"
    } else {
        &source.message
    };
    let index = source.index;
    let position = match offset.checked_add(index) {
        Some(absolute) => {
            format!("input row {absolute} (row {index} of the append at stream offset {offset})")
        }
        None => format!("row {index} of the append at stream offset {offset}"),
    };
    match source.code.value() {
        Some(0) => format!("{position}: {message}"),
        _ => format!("{position}: {}: {message}", source.code),
    }
}

pub(super) fn format_storage_error(source: &StorageError) -> String {
    if source.entity.is_empty() {
        format!("{}: {}", source.code, source.error_message)
    } else {
        format!(
            "{} on {}: {}",
            source.code, source.entity, source.error_message
        )
    }
}

/// Distinguish data and stream-state failures from retryable service errors.
pub(super) fn storage_error_status(code: &StorageErrorCode) -> Status {
    match code {
        StorageErrorCode::TableNotFound | StorageErrorCode::StreamNotFound => Status::NotFound,
        StorageErrorCode::StreamAlreadyCommitted
        | StorageErrorCode::StreamFinalized
        | StorageErrorCode::InvalidStreamType
        | StorageErrorCode::InvalidStreamState
        | StorageErrorCode::OffsetAlreadyExists
        | StorageErrorCode::OffsetOutOfRange => Status::InvalidState,
        StorageErrorCode::CmekNotProvided | StorageErrorCode::InvalidCmekProvided => {
            Status::InvalidArguments
        }
        StorageErrorCode::KmsPermissionDenied => Status::Unauthorized,
        // The service documents its own KMS failures as retryable.
        StorageErrorCode::CmekEncryptionError | StorageErrorCode::KmsServiceError => Status::IO,
        // A code this mapping has never seen must not be claimed as retryable.
        StorageErrorCode::UnknownValue(_) => Status::Unknown,
        _ => Status::InvalidData,
    }
}

/// Retain rows for replay until their append is acknowledged.
struct InFlightAppend {
    offset: i64,
    rows: ArrowRecordBatch,
    future: AppendFuture,
    /// Whether an earlier send of this append may have landed; see [`replay_landed`].
    replayed: bool,
}

/// A single request always fits the pipeline budget.
fn pipeline_has_room(requests: usize, bytes: usize, size: usize) -> bool {
    requests == 0 || (requests < APPEND_PIPELINE_REQUESTS && bytes + size <= APPEND_PIPELINE_BYTES)
}

/// Bounded appends, acknowledged in offset order. A failure's successors are replayed or dropped
/// with the pipeline; their offset errors must never hide the original failure.
#[derive(Default)]
pub(super) struct AppendPipeline {
    in_flight: VecDeque<InFlightAppend>,
    /// The serialized size of every payload in `in_flight`.
    in_flight_bytes: usize,
}

impl AppendPipeline {
    /// Wait for room, then queue a chunk at its explicit offset.
    pub(super) async fn submit(
        &mut self,
        target: &AppendTarget<'_>,
        writer: &mut PendingWriter<Arrow>,
        rows: ArrowRecordBatch,
        offset: i64,
    ) -> Result<()> {
        let size = rows.serialized_record_batch.len();
        while !pipeline_has_room(self.in_flight.len(), self.in_flight_bytes, size) {
            self.complete_oldest(target, writer).await?;
        }
        let future = writer.append(rows.clone()).set_offset(offset).send();
        self.in_flight_bytes += size;
        self.in_flight.push_back(InFlightAppend {
            offset,
            rows,
            future,
            replayed: false,
        });
        Ok(())
    }

    /// Await acknowledgement of every queued append.
    pub(super) async fn finish(
        &mut self,
        target: &AppendTarget<'_>,
        writer: &mut PendingWriter<Arrow>,
    ) -> Result<()> {
        while !self.in_flight.is_empty() {
            self.complete_oldest(target, writer).await?;
        }
        Ok(())
    }

    async fn complete_oldest(
        &mut self,
        target: &AppendTarget<'_>,
        writer: &mut PendingWriter<Arrow>,
    ) -> Result<()> {
        let Some(InFlightAppend {
            offset,
            rows,
            future,
            replayed,
        }) = self.in_flight.pop_front()
        else {
            return Ok(());
        };
        self.in_flight_bytes -= rows.serialized_record_batch.len();
        match target.operation.wait(append_answer(future)).await? {
            Ok(response) => check_acknowledged_offset(response.offset, offset, target.table_path),
            Err(source) if replay_landed(replayed, &source) => Ok(()),
            Err(source) if append_error_is_retryable(&source) => {
                diag!(
                    Debug,
                    "replaying BigQuery ingest appends to {} from stream offset {offset}, {} of \
                     them, on a reconnected stream, after {}",
                    target.table_path,
                    self.in_flight.len() + 1,
                    append_error_cause(&source)
                );
                self.resend(target, writer, offset, rows).await
            }
            Err(source) => Err(append_failure(target.table_path, source, offset)),
        }
    }

    /// Reconnect and replay the failed append, then requeue its successors in offset order.
    async fn resend(
        &mut self,
        target: &AppendTarget<'_>,
        writer: &mut PendingWriter<Arrow>,
        offset: i64,
        rows: ArrowRecordBatch,
    ) -> Result<()> {
        append_with_retry(target, writer, rows, offset, true).await?;
        for successor in &mut self.in_flight {
            successor.future = writer
                .append(successor.rows.clone())
                .set_offset(successor.offset)
                .send();
            successor.replayed = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
