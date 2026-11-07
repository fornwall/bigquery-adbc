//! Storage Write orchestration: append batches to a pending stream, finalize it, then commit.
//! Stream lifecycle, append retries and IPC chunk sizing live in their respective submodules.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::CommitError;
use google_cloud_bigquery::model::storage_error::StorageErrorCode;
use google_cloud_bigquery::model::{ArrowSchema, StorageError};
use google_cloud_bigquery::write::PendingWriter;
use google_cloud_bigquery::write::format::Arrow;
use google_cloud_gax::error::Error as GoogleError;

use crate::cancellation::Operation;
use crate::error::{driver_defect, error, google_error, summarize_errors};

mod encode;
mod retry;
mod stream;
mod window;

pub(super) use encode::read_batch;

use encode::ChunkEncoder;
use retry::{
    AppendPipeline, control_plane_retry, format_storage_error, google_error_is_transient,
    storage_error_status,
};
use stream::{finalize_abandoned_writer, pending_writer};

/// An ingest's first non-empty batch, checked to normalize and to fit AppendRows requests, with
/// the encoder that will send it.
pub(super) struct FirstBatch {
    batch: RecordBatch,
    encoder: ChunkEncoder,
    schema: ArrowSchema,
}

impl FirstBatch {
    /// Normalizes every chunk of `batch` and sizes it, and the schema, against the request limit,
    /// so data BigQuery would refuse or no request can carry fails before the target is created
    /// or replaced. The chunks are normalized again when they are sent rather than kept, so
    /// memory stays bounded by a chunk however large the batch.
    pub(super) fn new(
        normalized_schema: SchemaRef,
        batch: RecordBatch,
        operation: &Operation,
    ) -> Result<Self> {
        let (mut encoder, schema) = ChunkEncoder::new(normalized_schema)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            operation.check()?;
            let window = encoder.window(&batch, offset, encoder.target_batch_bytes)?;
            offset += encoder.plan(&window, offset)?;
        }
        Ok(Self {
            batch,
            encoder,
            schema,
        })
    }
}

/// Appends every batch the reader yields to one pending Storage Write stream and finalizes it.
///
/// None of the rows is visible until [`FinalizedStream::commit`]: that split is what lets a
/// `replace` empty its target only once every row is safely on the stream.
pub(super) async fn write_batches(
    client: Arc<Write>,
    table_path: &str,
    input_schema: SchemaRef,
    first: FirstBatch,
    mut reader: Box<dyn RecordBatchReader + Send>,
    operation: &Operation,
) -> Result<FinalizedStream> {
    operation.check()?;
    let FirstBatch {
        batch: first,
        encoder,
        schema: arrow_schema,
    } = first;
    let mut stream = StreamWriter {
        target: AppendTarget {
            client: &client,
            schema: arrow_schema,
            table_path,
            operation,
        },
        encoder,
        writer: None,
        pipeline: AppendPipeline::new(),
        total_rows: 0,
    };
    let outcome = match stream
        .append_all(&input_schema, first, reader.as_mut())
        .await
    {
        Ok(()) => stream.finalize().await,
        Err(source) => Err(source),
    };
    // An input failure already ends the ingest. Release queued appends without waiting for
    // their responses before the bounded cleanup of its uncommitted stream.
    drop(stream.pipeline);
    if outcome.is_err()
        && let Some(writer) = &stream.writer
    {
        // A failed ingest must not leave its pending stream open: BigQuery bills the buffered
        // rows until it expires. Re-finalizing is a no-op, so this also covers a failed
        // finalize. The failure that abandoned the stream is what gets reported.
        finalize_abandoned_writer(writer).await;
    }
    outcome
}

/// Total timeout for finalize or commit, including retries. These calls run outside caller
/// cancellation, so this also bounds their delay after a statement is cancelled.
const STREAM_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Completes finalize or commit without racing caller cancellation against its response.
/// Reporting a landed commit as cancelled could make callers retry and duplicate rows.
/// Both RPCs are idempotent, so transient failures can be retried within the timeout.
async fn close_stream_call<T, Call, Fut>(
    call: Call,
    what: &str,
    stream: &str,
    table_path: &str,
) -> Result<T>
where
    Call: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, GoogleError>>,
{
    let context = format!("failed to {what} BigQuery ingest stream {stream} for {table_path}");
    match tokio::time::timeout(
        STREAM_CLOSE_TIMEOUT,
        control_plane_retry(None, google_error_is_transient, call),
    )
    .await
    {
        // An ungated `control_plane_retry` cannot be cancelled; `?` propagates rather than
        // asserts that.
        Ok(outcome) => outcome?.map_err(|source| google_error(&context, source)),
        Err(_) => Err(error(
            format!(
                "{context}: no answer within {} s",
                STREAM_CLOSE_TIMEOUT.as_secs()
            ),
            Status::Timeout,
        )),
    }
}

/// A retried commit may return STREAM_ALREADY_COMMITTED instead of its original commit time.
/// This ingest owns the stream, so that status confirms its rows are durable.
fn commit_already_landed(errors: &[StorageError]) -> bool {
    !errors.is_empty()
        && errors
            .iter()
            .all(|source| source.code == StorageErrorCode::StreamAlreadyCommitted)
}

/// The size the stream's first append aims for. It is sent alone and awaited, since a new table
/// may not be visible to Storage Write yet (see [`pending_writer`]), so a full-sized first request
/// would hold back the rest of the upload for its whole round trip.
const FIRST_APPEND_TARGET_BYTES: usize = 1_000_000;

/// Append destination and reconnection state. A transport failure terminates the upstream
/// AppendRows runner, so recovery must reattach to the same stream with its schema.
struct AppendTarget<'a> {
    client: &'a Write,
    schema: ArrowSchema,
    table_path: &'a str,
    operation: &'a Operation,
}

/// One pending Storage Write stream and everything appended to it so far.
///
/// The stream is created lazily, by the first chunk that has rows to send. A NOT_FOUND from that
/// first append can mean the new table or stream is not visible yet, so [`pending_writer`] keeps
/// the first payload to reattach to the same stream and replay offset zero.
struct StreamWriter<'a> {
    target: AppendTarget<'a>,
    encoder: ChunkEncoder,
    writer: Option<PendingWriter<Arrow>>,
    pipeline: AppendPipeline,
    total_rows: i64,
}

impl StreamWriter<'_> {
    /// Appends remaining batches and drains the pipeline.
    async fn append_all(
        &mut self,
        input_schema: &SchemaRef,
        first: RecordBatch,
        reader: &mut dyn RecordBatchReader,
    ) -> Result<()> {
        // `first` was checked before table preparation, so invalid input cannot create or
        // truncate the target.
        self.append_batch(&first).await?;
        // Queued appends own their IPC bytes, so the first input's buffers need not remain
        // alive while the rest of the source is read or the pending stream is finalized.
        drop(first);
        loop {
            self.target.operation.check()?;
            let Some(batch) = reader.next() else {
                break;
            };
            let batch = read_batch(batch, input_schema)?;
            if batch.num_rows() != 0 {
                self.append_batch(&batch).await?;
            }
        }
        match &mut self.writer {
            Some(writer) => self.pipeline.finish(&self.target, writer).await,
            None => Ok(()),
        }
    }

    /// Normalizes one input batch in request-sized chunks and queues each of them.
    async fn append_batch(&mut self, input: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < input.num_rows() {
            self.target.operation.check()?;
            let target_size = if self.writer.is_none() {
                FIRST_APPEND_TARGET_BYTES
            } else {
                self.encoder.target_batch_bytes
            };
            let window = self.encoder.window(input, offset, target_size)?;
            let chunk = self.encoder.encode(window, offset)?;
            let row_count = i64::try_from(chunk.row_count).map_err(|_| {
                error(
                    "BigQuery ingest cannot represent this many rows in one chunk; \
                     split the input into smaller batches",
                    Status::InvalidData,
                )
            })?;
            if let Some(writer) = &mut self.writer {
                // Later chunks are pipelined: the submit only awaits a response once the pipeline
                // is full, so this chunk's request goes out while the next chunk is being encoded.
                self.pipeline
                    .submit(&self.target, writer, chunk.rows, self.total_rows)
                    .await?;
            } else {
                self.writer = Some(pending_writer(&self.target, chunk.rows).await?);
            }
            self.target.operation.check()?;
            self.total_rows = self.total_rows.checked_add(row_count).ok_or_else(|| {
                error(
                    "BigQuery ingest cannot represent this many rows in one ingestion; \
                     split the input into smaller batches",
                    Status::InvalidData,
                )
            })?;
            offset += chunk.row_count;
        }
        Ok(())
    }

    /// Finalizes and checks the row count. Empty ingests return before reaching this method.
    /// On failure, retain the writer for cleanup in [`write_batches`].
    async fn finalize(&mut self) -> Result<FinalizedStream> {
        let table_path = self.target.table_path;
        let total_rows = self.total_rows;
        self.target.operation.check()?;
        let Some(writer) = &self.writer else {
            return Err(driver_defect(format_args!(
                "BigQuery ingest reached finalize for {table_path} without ever opening a \
                 write stream"
            )));
        };
        let stream = writer.write_stream();
        let finalized =
            close_stream_call(|| writer.finalize(), "finalize", stream, table_path).await?;
        if finalized.row_count != total_rows {
            return Err(error(
                format!(
                    "BigQuery finalized ingest stream {stream} for {table_path} with {} rows, \
                     but {total_rows} were appended; no rows were committed, so the ingest \
                     can be retried",
                    finalized.row_count
                ),
                Status::InvalidData,
            ));
        }
        Ok(FinalizedStream {
            writer: self.writer.take().expect("the writer was borrowed above"),
            total_rows,
        })
    }
}

/// A finalized pending stream: every row is on it, and none is visible until it is committed.
///
/// An uncommitted finalized stream needs no cleanup (BigQuery expires it), so the caller may
/// still back out of the ingest at no cost up to [`Self::commit`].
pub(super) struct FinalizedStream {
    writer: PendingWriter<Arrow>,
    total_rows: i64,
}

impl FinalizedStream {
    /// Commits all rows atomically and returns their count. Callers check cancellation before
    /// entering; an in-flight commit completes independently (see [`close_stream_call`]).
    pub(super) async fn commit(&self, table_path: &str) -> Result<i64> {
        let Self { writer, total_rows } = self;
        let total_rows = *total_rows;
        let stream = writer.write_stream();
        // A commit that did not answer may still have been applied; say so, or a caller may
        // rerun the ingest and duplicate every row.
        let unknown = format!(
            "whether its {total_rows} rows were committed is unknown; check {table_path} before \
             retrying"
        );
        let outcome = close_stream_call(
            || async {
                // Only transport failures are retryable. Keep stream-level refusals intact:
                // STREAM_ALREADY_COMMITTED can acknowledge our own previous commit.
                match writer.commit().await {
                    Err(CommitError::Rpc { source, .. }) => Err(source),
                    outcome => Ok(outcome),
                }
            },
            "commit",
            stream,
            table_path,
        )
        .await
        .map_err(|mut source| {
            source.message = format!("{}; {unknown}", source.message);
            source
        })?;
        let stream_errors = match outcome {
            Ok(_) => return Ok(total_rows),
            Err(CommitError::FailedTransaction { stream_errors, .. }) => stream_errors,
            Err(source) => {
                return Err(error(
                    format!(
                        "failed to commit BigQuery ingest stream {stream} for {table_path}: \
                         {source}; {unknown}"
                    ),
                    Status::Unknown,
                ));
            }
        };
        if commit_already_landed(&stream_errors) {
            return Ok(total_rows);
        }
        let Some(first) = stream_errors.first() else {
            return Err(error(
                format!(
                    "BigQuery reported no commit time for ingest stream {stream} on \
                     {table_path}, so {unknown}"
                ),
                Status::IO,
            ));
        };
        // Cap the service-supplied list before crossing the FFI boundary.
        let messages = summarize_errors(stream_errors.iter().map(format_storage_error));
        Err(error(
            format!(
                "BigQuery refused to commit ingest stream {stream} for {table_path}: {messages}; \
                 none of its {total_rows} rows were written, so the ingest can be retried"
            ),
            storage_error_status(&first.code),
        ))
    }
}

#[cfg(test)]
mod tests;
