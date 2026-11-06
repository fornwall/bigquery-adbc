//! Which append failures may be replayed, and the pipeline of in-flight appends that replays them.
//!
//! Every append carries an explicit offset, so a replayed request that had landed is answered
//! `ALREADY_EXISTS` instead of being written twice; that makes a retry safe, and is what
//! [`replay_landed`] recognizes. The pipeline keeps a small bounded queue of
//! unacknowledged appends so the next chunk is encoded while earlier ones are in flight, and it
//! performs the retries.
//!
//! The control-plane RPCs around those appends retry through [`control_plane_retry`]: the pinned
//! client gives them no retry policy, so one transient status on the last call would fail an
//! ingest that has shipped every byte.

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
use crate::error::{driver_defect, error, google_error, google_error_code, summarize_errors};

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

/// Classifies the control-plane and append failures worth another attempt.
///
/// Upstream applies its `RetryableErrors` policy only in the default writer's `Dispatcher`
/// (`write/arrow/default.rs`); a pending-stream ingest never opens one, so its control-plane RPCs
/// run with `RequestOptions::retry_policy` left at `None`.
pub(super) use crate::error::google_error_is_transient;

/// Runs one Storage Write control-plane RPC, replaying it while the service answers transiently.
///
/// The outer `Result` is the caller's cancellation and the inner one the RPC's answer, so a call
/// site can still handle a non-transient status itself, such as the NOT_FOUND of a new table that
/// is not visible yet. `operation` is `None` for the two stream-closing RPCs, which must not be
/// cancelled (see [`super::close_stream_call`]).
pub(super) async fn control_plane_retry<T, E, Call, Fut>(
    operation: Option<&Operation>,
    is_transient: impl Fn(&E) -> bool,
    mut call: Call,
) -> Result<std::result::Result<T, E>>
where
    Call: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
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
}

/// Sleeps `delay` under the caller's cancellation, then doubles it for the next retry.
async fn back_off(operation: Option<&Operation>, delay: &mut Duration) -> Result<()> {
    cancellation::sleep(operation, *delay).await?;
    *delay = delay.saturating_mul(2);
    Ok(())
}

/// Classifies append failures worth replaying: a transient status by the control-plane rule, or
/// a request dropped when the AppendRows call carrying it ended. Explicit offsets make INTERNAL
/// and ABORTED, whose outcome is unknown, as safe to replay as UNAVAILABLE.
fn append_error_is_retryable(source: &AppendError) -> bool {
    match source {
        AppendError::Rpc { source, .. } => google_error_is_transient(source),
        // Once the runner's task exits, every request still queued on that writer resolves to
        // this, including the successors of whichever failure ended the stream.
        AppendError::UnexpectedEndOfStream => true,
        _ => false,
    }
}

/// A retry of an append that actually landed is answered ALREADY_EXISTS / OFFSET_ALREADY_EXISTS.
/// The rows are on the stream at the offset asked for, so that is a success, not a failure.
///
/// `StorageErrorCode::OffsetAlreadyExists` arrives as an undecoded status detail, so the transport
/// code is matched instead; BigQuery uses it for nothing else on an offset-carrying append.
fn replay_landed(replayed: bool, source: &AppendError) -> bool {
    replayed
        && matches!(source, AppendError::Rpc { source, .. }
        if matches!(google_error_code(source), Some(Code::AlreadyExists)))
}

/// A retryable append failure in a few words for a diagnostic: its gRPC code, never its message,
/// which can quote row values.
fn append_error_cause(source: &AppendError) -> &'static str {
    match source {
        AppendError::Rpc { source, .. } => {
            google_error_code(source).map_or("a transport failure", |code| code.name())
        }
        AppendError::UnexpectedEndOfStream => "the end of its AppendRows call",
        _ => "a failure",
    }
}

/// What one call of [`append_with_retry`] already knows about the request it is about to send.
#[derive(Clone, Copy, Debug)]
pub(super) struct Replay {
    /// Whether this exact request has been sent before. Only then is `ALREADY_EXISTS` success;
    /// on a first send it means the driver's offset accounting is wrong.
    pub(super) already_sent: bool,
    /// Whether the stream this request rides on is already known to be dead, so the first
    /// attempt has to reconnect before sending anything.
    pub(super) reconnect: bool,
}

/// Appends one chunk of rows at an explicit offset, reconnecting to the same stream before each
/// replay.
///
/// The failure being retried has already ended the writer's AppendRows call (see
/// [`AppendTarget`]), so a re-send on the same writer could only fail with
/// `UnexpectedEndOfStream`. `writer` is replaced in place so the caller keeps appending to, and
/// finalizes, the live stream.
pub(super) async fn append_with_retry(
    target: &AppendTarget<'_>,
    writer: &mut PendingWriter<Arrow>,
    rows: ArrowRecordBatch,
    offset: i64,
    replay: Replay,
) -> Result<()> {
    let Replay {
        mut already_sent,
        mut reconnect,
    } = replay;
    // Pipeline recovery has already spent its first append attempt.
    let mut attempts_left = APPEND_RETRY_ATTEMPTS - 1 - usize::from(reconnect);
    let mut delay = APPEND_RETRY_DELAY;
    loop {
        if reconnect {
            // The drain that ended the appends can refuse the `GetWriteStream` too, so a
            // transiently refused reconnect spends one attempt of the same budget, like a refused
            // append.
            match reattach(target, writer).await? {
                // The flag stays set: whatever failure sent this replay here ended the
                // AppendRows call, and so does each later one, so every replay reconnects.
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
            .wait(writer.append(rows.clone()).set_offset(offset).send())
            .await?
        {
            Ok(response) => {
                return check_acknowledged_offset(response.offset, offset, target.table_path);
            }
            Err(source) if replay_landed(already_sent, &source) => return Ok(()),
            Err(source) if attempts_left > 0 && append_error_is_retryable(&source) => {
                diag!(
                    Debug,
                    "replaying a BigQuery ingest append to {} at stream offset {offset} on \
                     a reconnected stream, after {}",
                    target.table_path,
                    append_error_cause(&source)
                );
                attempts_left -= 1;
                already_sent = true;
                reconnect = true;
                back_off(Some(target.operation), &mut delay).await?;
            }
            Err(source) => return Err(append_failure(target.table_path, source, offset)),
        }
    }
}

/// Checks the offset the service acknowledged against the one requested.
///
/// For any stream but the default one, `AppendResult.offset` echoes the request offset (the
/// append's first row, not a post-append cursor). A mismatch means the driver's offset accounting
/// drifted, which would otherwise surface only at the finalize row-count check.
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

/// The one wording every failed append reports. dbt matches it loosely, so it lives in one place.
///
/// `offset` is the count of rows appended before this chunk, which is what turns a rejected row's
/// chunk-relative index into the caller's own row number.
fn append_failure(table_path: &str, source: AppendError, offset: i64) -> adbc_core::error::Error {
    let context = format!("failed to append rows to BigQuery ingest target {table_path}");
    match source {
        // Upstream's `Display` only prefixes "the operation failed. RPC error:" to the RPC error.
        AppendError::Rpc { source, .. } => google_error(&context, source),
        // Upstream's `Display` is an uncapped `{:?}` dump of every rejected row, Google-internal
        // fields included; render the service's per-row messages instead, capped like a job's.
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

/// Renders a wholesale row rejection: how many rows the service refused, what to do about it, and
/// the service's own account of the first [`crate::error::REPORTED_ERRORS`] of them.
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

/// Renders one rejected row as `input row {absolute} (row {index} of the append at stream offset
/// {offset}): {code}: {message}`, dropping the code when it is the unspecified sentinel.
///
/// The service's index is relative to the appended chunk, and the driver re-chunks the caller's
/// batches, so `offset + index` is the caller's own row number. Both are reported, since only the
/// chunk-relative one matches the service's message.
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

/// Renders one commit-time stream error as `{code} on {entity}: {message}`.
///
/// The entity names the write stream that failed, which is the only way to tell one entry of a
/// multi-stream commit from another.
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

/// Classifies a commit-time stream error by its structured code.
///
/// These are data and stream-state failures, not I/O: reporting them as IO invites a caller to
/// retry an append that can never succeed.
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

/// One append queued on the pending stream whose response has not been awaited yet. The rows are
/// retained so a transient failure can re-send them: the stream rejects every request queued
/// behind a failed append once the offsets stop matching, so recovery replays from the failure.
struct InFlightAppend {
    offset: i64,
    rows: ArrowRecordBatch,
    future: AppendFuture,
    /// Whether this request was sent before, on a call that ended before answering it. Only then
    /// is `ALREADY_EXISTS` the acknowledgement of rows that landed (see [`Replay::already_sent`]).
    replayed: bool,
}

/// Whether an append of `size` bytes may join `requests` queued appends of `bytes` in total. An
/// empty pipeline takes any append, since a request is never larger than the budget.
fn pipeline_has_room(requests: usize, bytes: usize, size: usize) -> bool {
    requests == 0 || (requests < APPEND_PIPELINE_REQUESTS && bytes + size <= APPEND_PIPELINE_BYTES)
}

/// A bounded pipeline of AppendRows requests on one pending stream.
///
/// Every request carries an explicit, pre-assigned offset and the service responds in request
/// order, so several appends can be queued before the first response arrives. Failures surface
/// strictly in offset order: the first failed append is the error reported, and the requests
/// queued behind it are either re-sent (after a transient failure) or dropped, because their
/// rejections are consequences of the first failure rather than independent errors.
pub(super) struct AppendPipeline {
    in_flight: VecDeque<InFlightAppend>,
    /// The serialized size of every payload in `in_flight`.
    in_flight_bytes: usize,
}

impl AppendPipeline {
    pub(super) fn new() -> Self {
        Self {
            in_flight: VecDeque::new(),
            in_flight_bytes: 0,
        }
    }

    /// Queues one chunk at its explicit offset, first awaiting the oldest in-flight appends while
    /// the pipeline has no room for it.
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

    /// Awaits every queued append; a successful return means the stream acknowledged all
    /// submitted rows.
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

    fn pop_oldest(&mut self) -> Option<InFlightAppend> {
        let oldest = self.in_flight.pop_front()?;
        self.in_flight_bytes -= oldest.rows.serialized_record_batch.len();
        Some(oldest)
    }

    /// Drops every queued append unread.
    fn clear(&mut self) {
        self.in_flight.clear();
        self.in_flight_bytes = 0;
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
        }) = self.pop_oldest()
        else {
            return Ok(());
        };
        let outcome = match target.operation.wait(future).await {
            Ok(Ok(response)) => {
                check_acknowledged_offset(response.offset, offset, target.table_path)
            }
            Ok(Err(source)) if replay_landed(replayed, &source) => Ok(()),
            Ok(Err(source)) if append_error_is_retryable(&source) => {
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
            Ok(Err(source)) => Err(append_failure(target.table_path, source, offset)),
            Err(cancelled) => Err(cancelled),
        };
        if outcome.is_err() {
            // The stream refuses every append queued behind a failed one (OFFSET_OUT_OF_RANGE),
            // so drop them unread; no later drain may report one in place of the real failure.
            self.clear();
        }
        outcome
    }

    /// Re-sends a retryable append, then queues everything behind it again. Whatever ended the
    /// oldest append also ended its successors' AppendRows call, so their responses are dropped
    /// unread and their rows re-sent, in order and pipelined, on the reconnected stream.
    async fn resend(
        &mut self,
        target: &AppendTarget<'_>,
        writer: &mut PendingWriter<Arrow>,
        offset: i64,
        rows: ArrowRecordBatch,
    ) -> Result<()> {
        // The reconnect is the first replay's first step, so it shares that replay's retry
        // budget, and its success shows that the reconnected stream takes appends.
        let first = Replay {
            already_sent: true,
            reconnect: true,
        };
        append_with_retry(target, writer, rows, offset, first).await?;
        for successor in &mut self.in_flight {
            successor.future = writer
                .append(successor.rows.clone())
                .set_offset(successor.offset)
                .send();
            successor.replayed = true;
        }
        Ok(())
    }

    /// Awaits the appends still queued when production stopped for its own reason (a reader
    /// error, a rejected batch, cancellation) and reports the earliest append failure among
    /// them, which takes precedence over that reason. A retryable failure does not: nothing
    /// replays it once production has stopped, so it says nothing about the data.
    ///
    /// After a pipeline failure nothing is left to await: [`Self::complete_oldest`] already
    /// dropped the appends queued behind it.
    pub(super) async fn abort(&mut self, target: &AppendTarget<'_>) -> Result<()> {
        while let Some(InFlightAppend {
            offset,
            future,
            replayed,
            ..
        }) = self.pop_oldest()
        {
            match target.operation.wait(future).await? {
                Ok(_) => {}
                Err(source) if replay_landed(replayed, &source) => {}
                // Its successors' failures are consequences of it, so none of them is reported.
                Err(source) if append_error_is_retryable(&source) => break,
                Err(source) => return Err(append_failure(target.table_path, source, offset)),
            }
        }
        self.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
