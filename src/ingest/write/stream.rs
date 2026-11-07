//! The pending write stream's lifecycle.
//!
//! A new table is not immediately visible to the Storage Write API, and a transport failure ends
//! google-cloud-rust's AppendRows runner rather than failing one request. Both recover onto the
//! same stream.

use std::time::Duration;

use adbc_core::error::{Result, Status};
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_bigquery::model::ArrowRecordBatch;
use google_cloud_bigquery::write::PendingWriter;
use google_cloud_bigquery::write::format::Arrow;
use google_cloud_bigquery::write::stream_type::PendingStream;

use crate::cancellation;
use crate::diag::diag;
use crate::error::{error, google_error, google_error_is_not_found};
use crate::rest::retry::Schedule;

use super::AppendTarget;
use super::retry::{Replay, append_with_retry, control_plane_retry, google_error_is_transient};

// A new or replaced table can stay invisible to the Storage Write API for over a minute,
// especially after a dataset is deleted and recreated under the same name. As in the Go driver:
// 20 attempts with full-jitter exponential backoff from 100 ms, capped at 15 s, just over three
// minutes in the worst case. The attempts budget the whole wait, not each step (see
// [`VisibilityBudget`]).
const TABLE_VISIBILITY_ATTEMPTS: u32 = 20;
const TABLE_VISIBILITY_RETRY_DELAY: Duration = Duration::from_millis(100);
const TABLE_VISIBILITY_MAX_RETRY_DELAY: Duration = Duration::from_secs(15);
const TABLE_VISIBILITY_BACKOFF: Schedule = Schedule::new(
    TABLE_VISIBILITY_RETRY_DELAY,
    TABLE_VISIBILITY_MAX_RETRY_DELAY,
    2.0,
);

// Bounds the best-effort finalize of a stream abandoned by a failed ingest, so cleanup cannot
// stall reporting the error that caused it.
const ABANDONED_STREAM_FINALIZE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) fn attach_error_is_not_found(source: &WriterBuilderError) -> bool {
    matches!(source, WriterBuilderError::Rpc { source, .. } if google_error_is_not_found(source))
}

/// Whether a refused attach is worth another `GetWriteStream`, by the same rule every other
/// control-plane RPC of an ingest is judged by.
pub(super) fn attach_error_is_transient(source: &WriterBuilderError) -> bool {
    matches!(source, WriterBuilderError::Rpc { source, .. } if google_error_is_transient(source))
}

/// One bounded budget of table-visibility retries, spanning every step of one such wait.
///
/// Creating the stream and landing the first payload both wait on the same new table, and callers
/// size statement timeouts against the documented budget, so both steps charge one
/// [`TABLE_VISIBILITY_ATTEMPTS`] budget.
struct VisibilityBudget {
    failures: u32,
}

impl VisibilityBudget {
    fn new() -> Self {
        Self { failures: 0 }
    }

    /// Whether the failure just observed may be waited out rather than reported.
    fn has_attempt_left(&self) -> bool {
        self.failures + 1 < TABLE_VISIBILITY_ATTEMPTS
    }

    /// Charges one failure to the budget and waits out its backoff.
    async fn back_off(&mut self, target: &AppendTarget<'_>) -> Result<()> {
        self.failures += 1;
        let delay = TABLE_VISIBILITY_BACKOFF.delay(self.failures);
        diag!(
            Debug,
            "waiting {delay:?} for BigQuery ingest target {} to become visible to Storage \
             Write (wait {} of {})",
            target.table_path,
            self.failures,
            TABLE_VISIBILITY_ATTEMPTS - 1
        );
        cancellation::sleep(Some(target.operation), delay).await
    }
}

/// Creates a pending Storage Write stream, retrying NOT_FOUND with backoff while a freshly
/// created or replaced table becomes visible, and appends `first_rows` at offset zero.
pub(super) async fn pending_writer(
    target: &AppendTarget<'_>,
    first_rows: ArrowRecordBatch,
) -> Result<PendingWriter<Arrow>> {
    let AppendTarget {
        client,
        schema,
        table_path,
        operation,
    } = target;
    let context = format!("failed to create BigQuery pending ingest stream for {table_path}");
    let mut budget = VisibilityBudget::new();
    let mut writer = loop {
        // `CreateWriteStream` is not idempotent: a retry after a lost response leaves an extra
        // pending stream. That is harmless: an empty pending stream bills nothing and expires,
        // and only the stream whose creation was answered is committed.
        let created =
            control_plane_retry(&context, Some(operation), attach_error_is_transient, || {
                client
                    .create_stream::<PendingStream>(*table_path)
                    .build_arrow(schema.clone())
            })
            .await?;
        match created {
            Ok(writer) => break writer,
            Err(source) if budget.has_attempt_left() && attach_error_is_not_found(&source) => {
                budget.back_off(target).await?;
            }
            Err(source) => return Err(writer_builder_failure(&context, source)),
        }
    };

    // AppendRows may still report NOT_FOUND after creation succeeds. Reattach to the same
    // stream and replay offset zero within the remaining visibility budget. Keep ownership
    // here so cancellation during backoff or reattachment also finalizes the abandoned stream.
    let initialized = async {
        let mut reconnect = false;
        let mut already_sent = false;
        loop {
            if reconnect {
                let stream_name = writer.write_stream().to_owned();
                match attach_writer(target, &stream_name).await? {
                    Ok(attached) => {
                        writer = attached;
                    }
                    Err(source)
                        if budget.has_attempt_left() && attach_error_is_not_found(&source) =>
                    {
                        budget.back_off(target).await?;
                        continue;
                    }
                    Err(source) => {
                        return Err(reconnect_failure(&stream_name, source));
                    }
                }
            }
            // ALREADY_EXISTS acknowledges a lost response only when replaying a prior send.
            // The attach above already reconnected, so the replay starts with its append.
            let replay = Replay {
                already_sent,
                reconnect: false,
            };
            match append_with_retry(target, &mut writer, first_rows.clone(), 0, replay).await {
                Ok(()) => return Ok(()),
                Err(source) if budget.has_attempt_left() && source.status == Status::NotFound => {
                    already_sent = true;
                    budget.back_off(target).await?;
                    reconnect = true;
                }
                Err(source) => return Err(source),
            }
        }
    }
    .await;
    match initialized {
        Ok(()) => Ok(writer),
        Err(source) => {
            finalize_abandoned_writer(&writer).await;
            Err(source)
        }
    }
}

/// Reconnects to an existing pending stream by name.
///
/// `GetWriteStream` is a read, so retrying it is always safe.
async fn attach_writer(
    target: &AppendTarget<'_>,
    stream_name: &str,
) -> Result<std::result::Result<PendingWriter<Arrow>, WriterBuilderError>> {
    control_plane_retry(
        &reconnect_context(stream_name),
        Some(target.operation),
        attach_error_is_transient,
        || {
            target
                .client
                .attach_to_stream::<PendingStream>(stream_name)
                .build_arrow(target.schema.clone())
        },
    )
    .await
}

/// Reports a pending stream this ingest could not get back onto.
///
/// The gRPC code tells the caller whether rerunning the ingest can help (NOT_FOUND cannot,
/// UNAVAILABLE can), so the failure goes through the Google-error path, which keeps the
/// `bigquery.error.rpc_code` detail and the sign-in remedy for an expired credential. Any other
/// builder error, such as a PENDING stream coming back as another type, is `INTERNAL`.
pub(super) fn reconnect_failure(
    stream_name: &str,
    source: WriterBuilderError,
) -> adbc_core::error::Error {
    writer_builder_failure(&reconnect_context(stream_name), source)
}

fn reconnect_context(stream_name: &str) -> String {
    format!("failed to reconnect BigQuery ingest stream {stream_name}")
}

fn writer_builder_failure(context: &str, source: WriterBuilderError) -> adbc_core::error::Error {
    match source {
        WriterBuilderError::Rpc { source, .. } => google_error(context, source),
        source => error(format!("{context}: {source}"), Status::Internal),
    }
}

/// Replaces `writer` with a fresh connection to the same pending stream.
///
/// A new stream would restart the table-propagation window and leak the old one; reattaching by
/// name keeps the driver-assigned offsets, and with them exactly-once replay.
///
/// The attach failure is returned unconverted so the enclosing append retry can spend an attempt
/// on it rather than end the ingest.
pub(super) async fn reattach(
    target: &AppendTarget<'_>,
    writer: &mut PendingWriter<Arrow>,
) -> Result<std::result::Result<(), WriterBuilderError>> {
    let stream_name = writer.write_stream().to_owned();
    Ok(attach_writer(target, &stream_name).await?.map(|attached| {
        *writer = attached;
    }))
}

/// Best-effort finalize of a pending stream whose ingest already failed, so the server-side
/// stream is not left open. The caller reports the error that abandoned the stream; if this
/// cleanup fails too, the stream merely lingers until BigQuery expires it.
pub(super) async fn finalize_abandoned_writer(writer: &PendingWriter<Arrow>) {
    let _ = tokio::time::timeout(ABANDONED_STREAM_FINALIZE_TIMEOUT, writer.finalize()).await;
}

#[cfg(test)]
mod tests;
