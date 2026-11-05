//! REST rows delivered through the result pipeline, or directly by a small resident reader.

use std::collections::VecDeque;

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use tokio::task::JoinHandle;

use crate::cancellation::Operation;
use crate::pipeline::{
    BATCH_SIZE, QueryResultOptions, ResultPipelineCancellation, ResultSink, task_failure,
};
use crate::rows::{RestRow, rest_rows_to_record_batch};
use crate::runtime::SharedRuntime;

use super::pager::QueryPager;

/// Encoded bytes allowed in pending conversions, which the pipeline's completed-batch budget
/// does not cover. A row can be megabytes wide, so prefetch concurrency alone is insufficient.
/// One conversion always proceeds, even when it exceeds this window.
const CONVERSION_WINDOW_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn empty_reader(schema: SchemaRef) -> Box<dyn RecordBatchReader + Send + 'static> {
    Box::new(RecordBatchIterator::new(
        std::iter::empty::<Result<RecordBatch, ArrowError>>(),
        schema,
    ))
}

/// A complete small page needs no producer task or queue. Conversion stays lazy so malformed
/// cells are stream errors, and uses the same cancellation-aware driver-wide permit as a pipeline.
pub(super) fn resident_reader(
    runtime: SharedRuntime,
    schema: SchemaRef,
    operation: Option<Operation>,
    rows: Vec<RestRow>,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    Box::new(ResidentReader {
        runtime,
        schema,
        cancellation: ResultPipelineCancellation::new(operation),
        rows: Some(rows),
    })
}

struct ResidentReader {
    runtime: SharedRuntime,
    schema: SchemaRef,
    cancellation: ResultPipelineCancellation,
    rows: Option<Vec<RestRow>>,
}

impl Iterator for ResidentReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rows = self.rows.take()?;
        if self.cancellation.is_cancelled() {
            return None;
        }
        let permits = self.runtime.result_conversion_permits();
        // An available permit needs no runtime entry. Contention still uses the cancellable
        // wait, which also reports an unexpectedly closed conversion semaphore.
        let _permit = match permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => match self.runtime.block_on(self.cancellation.permit(&permits)) {
                Ok(Some(permit)) => permit,
                Ok(None) => return None,
                Err(source) => return Some(Err(source)),
            },
        };
        // Convert on the caller's thread: even a small page can inherit a large schema from an
        // earlier response, and its builders must not occupy an asynchronous runtime worker.
        crate::runtime::block_in_place(|| {
            rest_rows_to_record_batch(rows, 0, self.schema.clone(), &self.cancellation).transpose()
        })
    }
}

impl RecordBatchReader for ResidentReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Converts the pager's rows in parallel and delivers the batches to `sink` in result order.
pub(super) async fn buffer_rest_query_results(
    mut pager: QueryPager,
    sink: ResultSink,
    options: QueryResultOptions,
    mut remaining_rows: Option<usize>,
) -> Result<(), ArrowError> {
    let mut conversions = ConversionTasks::new(sink.cancellation.clone());
    let mut exhausted = false;
    // The result row number of the next batch's first row, which its decode failures report.
    let mut first_row = 0_usize;

    loop {
        while !exhausted && conversions.may_start(options.prefetch_concurrency()) {
            // `totalRows` only sizes the batch vector. An exhausted count falls back to the full
            // batch size, because asking for zero rows would drop rows a stale count missed.
            let capacity = remaining_rows
                .filter(|rows| *rows > 0)
                .map_or(BATCH_SIZE, |rows| rows.min(BATCH_SIZE));
            let next = {
                let next_batch = pager.next_batch(&sink, capacity);
                tokio::pin!(next_batch);
                loop {
                    tokio::select! {
                        // Fill from resident rows first, keeping parallel conversion available.
                        biased;
                        result = &mut next_batch => break result?,
                        conversion = conversions.next(), if !conversions.is_empty() => {
                            // Delivery waits for the reader, so the page request is polled
                            // meanwhile: an unpolled request still runs out its timeout.
                            let delivery = deliver_conversion(conversion, &sink);
                            tokio::pin!(delivery);
                            let mut fetched = None;
                            let delivered = loop {
                                tokio::select! {
                                    biased;
                                    delivered = &mut delivery => break delivered?,
                                    result = &mut next_batch, if fetched.is_none() => {
                                        fetched = Some(result);
                                    }
                                }
                            };
                            if !delivered {
                                return Ok(());
                            }
                            if let Some(result) = fetched {
                                break result?;
                            }
                        }
                    }
                }
            };
            // The same page future stays alive while completed conversions are delivered:
            // dropping and restarting it would lose a token already taken by the pager, or repeat
            // a GET.
            let Some((batch_rows, input_exhausted)) = next else {
                return Ok(());
            };
            exhausted = input_exhausted;
            if batch_rows.is_empty() {
                break;
            }
            if let Some(remaining_rows) = &mut remaining_rows {
                *remaining_rows = remaining_rows.saturating_sub(batch_rows.len());
            }
            let bytes = batch_rows.len().saturating_mul(pager.row_bytes());
            let batch_first_row = first_row;
            first_row += batch_rows.len();

            let Some(conversion_permit) = sink.conversion_permit().await? else {
                return Ok(());
            };
            let schema = sink.schema.clone();
            let conversion_cancellation = sink.cancellation.clone();
            conversions.push(
                tokio::task::spawn_blocking(move || {
                    let _conversion_permit = conversion_permit;
                    rest_rows_to_record_batch(
                        batch_rows,
                        batch_first_row,
                        schema,
                        &conversion_cancellation,
                    )
                }),
                bytes,
            );
        }

        if !deliver_conversion(conversions.next().await, &sink).await? {
            return Ok(());
        }
    }
}

/// Delivers a conversion; `false` means the queue is empty, cancellation stopped conversion,
/// or the reader was released.
async fn deliver_conversion(
    conversion: Option<Result<ConversionResult, tokio::task::JoinError>>,
    sink: &ResultSink,
) -> Result<bool, ArrowError> {
    match conversion {
        Some(Ok(Ok(Some(batch)))) => Ok(sink.deliver(batch).await),
        Some(Ok(Err(source))) => Err(source),
        Some(Err(source)) => Err(task_failure("result conversion", &source)),
        Some(Ok(Ok(None))) | None => Ok(false),
    }
}

/// What one spawned conversion returns: a batch, or `None` for a conversion that stopped on
/// cancellation.
type ConversionResult = Result<Option<RecordBatch>, ArrowError>;

/// Pending conversions in delivery order, charged by encoded input bytes.
/// Drop cooperatively cancels running blocking work and aborts tasks that have not started.
/// Drop also runs on successful completion, so local cancellation does not imply truncation.
struct ConversionTasks {
    handles: VecDeque<(JoinHandle<ConversionResult>, usize)>,
    bytes: usize,
    cancellation: ResultPipelineCancellation,
}

impl ConversionTasks {
    fn new(cancellation: ResultPipelineCancellation) -> Self {
        Self {
            handles: VecDeque::new(),
            bytes: 0,
            cancellation,
        }
    }

    fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Whether another conversion fits the reader's prefetch width and the byte window.
    fn may_start(&self, prefetch_concurrency: usize) -> bool {
        self.is_empty()
            || (self.handles.len() < prefetch_concurrency && self.bytes < CONVERSION_WINDOW_BYTES)
    }

    fn push(&mut self, handle: JoinHandle<ConversionResult>, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.handles.push_back((handle, bytes));
    }

    async fn next(&mut self) -> Option<Result<ConversionResult, tokio::task::JoinError>> {
        let result = (&mut self.handles.front_mut()?.0).await;
        let (_, bytes) = self.handles.pop_front()?;
        self.bytes -= bytes;
        Some(result)
    }
}

impl Drop for ConversionTasks {
    fn drop(&mut self) {
        self.cancellation.cancel_local();
        for (handle, _) in &self.handles {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests;
