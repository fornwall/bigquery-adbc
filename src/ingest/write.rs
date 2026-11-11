//! Storage Write orchestration: append batches to a pending stream, finalize it, then commit.
//! Stream lifecycle, append retries and IPC chunk sizing live in their respective submodules.

use std::future::Future;

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
use crate::error::{error, google_error, google_error_is_transient, summarize_errors};

mod encode;
mod retry;
mod stream;
mod window;

#[cfg(test)]
pub(super) use encode::NORMALIZED_ROWS;
pub(super) use encode::read_batch;

use encode::ChunkEncoder;
use retry::{AppendPipeline, control_plane_retry, format_storage_error, storage_error_status};
use stream::{finalize_abandoned_writer, pending_writer};

/// An ingest's first non-empty batch, with the encoder that will send it.
pub(super) struct FirstBatch {
    batch: RecordBatch,
    encoder: ChunkEncoder,
    schema: ArrowSchema,
}

impl FirstBatch {
    /// Sizes the schema against the request limit and, with `check_rows`, normalizes and sizes
    /// every chunk of `batch` too, so data BigQuery would refuse or no request can carry fails
    /// before the target is created or replaced. The chunks are normalized again when they are
    /// sent rather than kept, so memory stays bounded by a chunk however large the batch.
    pub(super) fn new(
        normalized_schema: SchemaRef,
        batch: RecordBatch,
        check_rows: bool,
        operation: &Operation,
    ) -> Result<Self> {
        let (mut encoder, schema) = ChunkEncoder::new(normalized_schema)?;
        let mut offset = 0;
        while check_rows && offset < batch.num_rows() {
            operation.check()?;
            let window = encoder.window(&batch, offset, offset, encoder.target_batch_bytes)?;
            offset += encoder.plan(&window, offset)?;
        }
        Ok(Self {
            batch,
            encoder,
            schema,
        })
    }
}

/// Checks that `normalized_schema` leaves room for rows in an AppendRows request, as
/// [`FirstBatch::new`] does for an ingest, for a dry run that has no batch to check.
pub(super) fn check_append_schema(normalized_schema: SchemaRef) -> Result<()> {
    ChunkEncoder::new(normalized_schema).map(drop)
}

/// Encodes `batch` chunk by chunk as the appends after a stream's first carry it (window sizing,
/// normalization and IPC framing, without the network), and returns the IPC bytes framed.
#[cfg(feature = "benchmark")]
pub(crate) fn encode_for_benchmark(
    normalized_schema: SchemaRef,
    batch: &RecordBatch,
) -> Result<usize> {
    let (mut encoder, _) = ChunkEncoder::new(normalized_schema)?;
    let (mut offset, mut bytes) = (0, 0);
    while offset < batch.num_rows() {
        let window = encoder.window(batch, offset, offset, encoder.target_batch_bytes)?;
        let chunk = encoder.encode(window, offset)?;
        bytes += chunk.rows.serialized_record_batch.len();
        offset += chunk.row_count;
    }
    Ok(bytes)
}

/// Appends every batch the reader yields to one pending Storage Write stream and finalizes it.
///
/// None of the rows is visible until [`FinalizedStream::commit`]: that split is what lets a
/// `replace` empty its target only once every row is safely on the stream.
pub(super) async fn write_batches(
    client: &Write,
    table_path: &str,
    input_schema: SchemaRef,
    first: FirstBatch,
    mut reader: Box<dyn RecordBatchReader + Send>,
    operation: &Operation,
) -> Result<FinalizedStream> {
    operation.check()?;
    let FirstBatch {
        batch: first,
        mut encoder,
        schema,
    } = first;
    let target = AppendTarget {
        client,
        schema,
        table_path,
        operation,
    };
    // `first` was checked before table preparation, so invalid input cannot create or truncate
    // the target.
    let window = encoder.window(&first, 0, 0, FIRST_APPEND_TARGET_BYTES)?;
    let chunk = encoder.encode(window, 0)?;
    let writer = pending_writer(&target, chunk.rows).await?;
    let mut stream = StreamWriter {
        target,
        encoder,
        writer,
        pipeline: AppendPipeline::default(),
        total_rows: chunk.row_count,
    };
    let outcome = match stream
        .append_all(&input_schema, first, chunk.row_count, reader.as_mut())
        .await
    {
        Ok(()) => stream.finalize().await,
        Err(source) => Err(source),
    };
    // An input failure already ends the ingest. Release queued appends without waiting for
    // their responses before the bounded cleanup of its uncommitted stream.
    drop(stream.pipeline);
    if outcome.is_err() {
        // A failed ingest must not leave its pending stream open: BigQuery bills the buffered
        // rows until it expires. Re-finalizing is a no-op, so this also covers a failed
        // finalize. The failure that abandoned the stream is what gets reported.
        finalize_abandoned_writer(&stream.writer).await;
    }
    outcome.map(|()| FinalizedStream {
        writer: stream.writer,
        total_rows: stream_rows(stream.total_rows),
    })
}

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
    control_plane_retry(&context, None, google_error_is_transient, call)
        .await?
        .map_err(|source| google_error(&context, source))
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

/// A row count as the stream's offsets count them: an ingest could not send `i64::MAX` rows in a
/// lifetime, so this cannot overflow.
fn stream_rows(rows: usize) -> i64 {
    i64::try_from(rows).expect("an ingest's rows fit a stream offset")
}

/// Append destination and reconnection state. A transport failure terminates the upstream
/// AppendRows runner, so recovery must reattach to the same stream with its schema.
struct AppendTarget<'a> {
    client: &'a Write,
    schema: ArrowSchema,
    table_path: &'a str,
    operation: &'a Operation,
}

/// One pending Storage Write stream and everything appended to it so far.
struct StreamWriter<'a> {
    target: AppendTarget<'a>,
    encoder: ChunkEncoder,
    writer: PendingWriter<Arrow>,
    pipeline: AppendPipeline,
    /// Rows queued so far, which is also the stream position of the next row.
    total_rows: usize,
}

impl StreamWriter<'_> {
    /// Appends the rest of `first` from row `first_unsent`, then every later batch, and drains
    /// the pipeline.
    async fn append_all(
        &mut self,
        input_schema: &SchemaRef,
        first: RecordBatch,
        first_unsent: usize,
        reader: &mut dyn RecordBatchReader,
    ) -> Result<()> {
        self.append_batch(&first, first_unsent).await?;
        // Queued appends own their IPC bytes, so the first input's buffers need not remain
        // alive while the rest of the source is read or the pending stream is finalized.
        drop(first);
        loop {
            self.target.operation.check()?;
            let Some(batch) = reader.next() else {
                break;
            };
            let batch = read_batch(batch, input_schema)?;
            self.append_batch(&batch, 0).await?;
        }
        self.pipeline.finish(&self.target, &mut self.writer).await
    }

    /// Normalizes `input` from row `offset` in request-sized chunks and queues each of them. The
    /// submit only awaits a response once the pipeline is full, so a chunk's request goes out
    /// while the next chunk is being encoded.
    async fn append_batch(&mut self, input: &RecordBatch, mut offset: usize) -> Result<()> {
        while offset < input.num_rows() {
            self.target.operation.check()?;
            let target_size = self.encoder.target_batch_bytes;
            let window = self
                .encoder
                .window(input, offset, self.total_rows, target_size)?;
            let chunk = self.encoder.encode(window, self.total_rows)?;
            self.pipeline
                .submit(
                    &self.target,
                    &mut self.writer,
                    chunk.rows,
                    stream_rows(self.total_rows),
                )
                .await?;
            self.total_rows += chunk.row_count;
            offset += chunk.row_count;
        }
        Ok(())
    }

    /// Finalizes the stream and checks its row count.
    async fn finalize(&self) -> Result<()> {
        let Self {
            target,
            writer,
            total_rows,
            ..
        } = self;
        let table_path = target.table_path;
        target.operation.check()?;
        let stream = writer.write_stream();
        let finalized =
            close_stream_call(|| writer.finalize(), "finalize", stream, table_path).await?;
        if finalized.row_count != stream_rows(*total_rows) {
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
        Ok(())
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

/// A commit that did not succeed, and whether its rows may have been committed anyway.
pub(super) struct CommitFailure {
    pub(super) error: adbc_core::error::Error,
    /// The commit ended without BigQuery's verdict, so every row may be committed. A caller must
    /// then not suggest writing the rows again.
    pub(super) outcome_unknown: bool,
}

impl CommitFailure {
    fn refused(error: adbc_core::error::Error) -> Self {
        Self {
            error,
            outcome_unknown: false,
        }
    }

    fn unknown(error: adbc_core::error::Error) -> Self {
        Self {
            error,
            outcome_unknown: true,
        }
    }
}

impl FinalizedStream {
    /// Commits all rows atomically and returns their count. Callers check cancellation before
    /// entering; an in-flight commit completes independently (see [`close_stream_call`]).
    pub(super) async fn commit(&self, table_path: &str) -> std::result::Result<i64, CommitFailure> {
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
            CommitFailure::unknown(source)
        })?;
        let stream_errors = match outcome {
            Ok(_) => return Ok(total_rows),
            Err(CommitError::FailedTransaction { stream_errors, .. }) => stream_errors,
            Err(source) => {
                return Err(CommitFailure::unknown(error(
                    format!(
                        "failed to commit BigQuery ingest stream {stream} for {table_path}: \
                         {source}; {unknown}"
                    ),
                    Status::Unknown,
                )));
            }
        };
        if commit_already_landed(&stream_errors) {
            return Ok(total_rows);
        }
        let Some(first) = stream_errors.first() else {
            return Err(CommitFailure::unknown(error(
                format!(
                    "BigQuery reported no commit time for ingest stream {stream} on \
                     {table_path}, so {unknown}"
                ),
                Status::IO,
            )));
        };
        // Cap the service-supplied list before crossing the FFI boundary.
        let messages = summarize_errors(stream_errors.iter().map(format_storage_error));
        Err(CommitFailure::refused(error(
            format!(
                "BigQuery refused to commit ingest stream {stream} for {table_path}: {messages}; \
                 none of its {total_rows} rows were written, so the ingest can be retried"
            ),
            storage_error_status(&first.code),
        )))
    }
}

#[cfg(test)]
mod tests;
