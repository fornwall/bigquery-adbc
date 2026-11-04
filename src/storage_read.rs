//! Completed query results read as Arrow through the BigQuery Storage Read API.
//!
//! Batches are reshaped into the canonical REST-derived schema before delivery. Incompatible
//! session schemas fall back to REST. Query execution supplies the destination table and reuses
//! its job metadata, avoiding a separate lookup here. See [`reshape`](mod@reshape).

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use arrow_array::RecordBatchReader;
use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, SchemaRef};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::{
    ArrowSerializationOptions, DataFormat, ReadRowsResponse, ReadSession,
    arrow_serialization_options::CompressionCodec, read_rows_response::Rows,
    read_session::TableReadOptions,
};
use google_cloud_gax::error::Error as GoogleError;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::streaming::ResponseStream;

use crate::arrow_ipc_bigquery::reader::{decode_record_batch_message, decode_schema};
use crate::cancellation::{Operation, gate};
use crate::clients::ClientState;
use crate::error::{error, google_error, google_error_code, google_error_is_transient};
use crate::pipeline::{
    ProducerTask, QueryResultOptions, ResultSink, spawn_result_reader, task_failure,
};
use crate::rest::jobs::JobReference;
use crate::rest::metadata::TableId;
use crate::rest::model::TableReference;
use crate::rest::retry::Schedule;
use crate::runtime::SharedRuntime;
use reshape::{ReshapePlan, reshape, reshape_plans};

mod reshape;

/// One stream preserves SQL ordering. Parallel reads require the caller's explicit opt-in;
/// inferring ordering from SQL text is unreliable (see comparison G26).
const ORDERED_MAX_STREAM_COUNT: i32 = 1;

/// Consecutive resume attempts allowed; a response carrying rows resets the allowance.
const READ_ROWS_RESUME_ATTEMPTS: u32 = 4;
/// Full-jitter exponential backoff, as the REST retries use, so the streams of a parallel read
/// that one outage broke do not all reopen at once: up to 250 ms, doubling to at most 2 s.
const READ_ROWS_RESUME_BACKOFF: Schedule =
    Schedule::new(Duration::from_millis(250), Duration::from_secs(2), 2.0);

/// Aggregate backoff limit across all resumes. Once spent, retries proceed immediately;
/// the consecutive-failure limit still stops streams that make no progress.
const READ_ROWS_RESUME_DELAY_BUDGET: Duration = Duration::from_secs(30);

/// Transient failures, including quota/fairness closures and a stream reset at the transport,
/// permit an exact-offset resume. Cancellation must stop the read.
fn read_rows_is_resumable(source: &GoogleError) -> bool {
    google_error_is_transient(source)
}

/// Request no more streams than the available decode parallelism, capped at 32.
/// The service may grant fewer depending on table size.
fn unordered_max_stream_count() -> i32 {
    let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
    i32::try_from(parallelism.clamp(1, 32)).unwrap_or(32)
}

/// Whether a completed query's results are worth reading as Arrow.
///
/// This decides everything except whether the job resolves to a readable destination table:
/// [`crate::query`] asks first, because the answer decides whether a job lookup is needed, and
/// then passes the destination to [`reader`].
///
/// A result is ineligible when the caller disabled the path, when DDL or DML left nothing to read,
/// or when a read session was already refused for this project, so later queries skip the
/// rejected RPC. Whether the result needs another REST page is the caller's half of the decision.
pub(crate) fn eligible(
    clients: &ClientState,
    options: QueryResultOptions,
    job_reference: &JobReference,
    schema: &SchemaRef,
) -> bool {
    options.use_storage_read()
        && !schema.fields().is_empty()
        && !clients.storage_read_disabled(&job_reference.project_id)
}

/// Reads a completed query's results as Arrow, or reports that REST must be used.
///
/// Every fallback happens before the first batch reaches the caller, so REST can re-read the
/// destination table from the start at the cost of one wasted unary RPC. Once [`stream_batches`]
/// has delivered a batch, REST would duplicate rows, so later failures are reported.
pub(crate) fn reader(
    runtime: &SharedRuntime,
    clients: &Arc<ClientState>,
    operation: Option<&Operation>,
    options: QueryResultOptions,
    job_reference: &JobReference,
    destination: TableReference,
    schema: SchemaRef,
) -> Result<Opened> {
    let runtime = runtime.clone();
    runtime.clone().block_on(open_reader(
        runtime,
        clients.clone(),
        operation.cloned(),
        options,
        job_reference.clone(),
        destination,
        schema,
    ))
}

/// What [`reader`] opened: a Storage Read stream, or the reason the result must be read over
/// REST instead.
pub(crate) enum Opened {
    Reader(Box<dyn RecordBatchReader + Send + 'static>),
    /// A destination naming one partition, a refused or empty read session, or a result whose
    /// Arrow shape cannot be reconciled with the canonical schema. The error says which, with the
    /// status it would have if REST could not read the result either.
    Fallback(adbc_core::error::Error),
}

async fn open_reader(
    runtime: SharedRuntime,
    clients: Arc<ClientState>,
    operation: Option<Operation>,
    options: QueryResultOptions,
    job_reference: JobReference,
    destination: TableReference,
    schema: SchemaRef,
) -> Result<Opened> {
    // `CreateReadSession` refuses table decorators, which a destination naming one partition
    // (`table$20240101`) carries; no table name may contain `$`.
    if destination.table_id.contains('$') {
        return Ok(Opened::Fallback(error(
            "the destination table names a partition, which Storage Read cannot read",
            Status::NotImplemented,
        )));
    }
    let max_streams = if options.unordered_read() {
        unordered_max_stream_count()
    } else {
        ORDERED_MAX_STREAM_COUNT
    };
    let read_client = clients.read_client(&runtime).await?;
    let session = match create_session(
        &read_client,
        &clients,
        &operation,
        &job_reference,
        &destination,
        max_streams,
    )
    .await?
    {
        Ok(session) => session,
        Err(refusal) => return Ok(Opened::Fallback(refusal)),
    };
    if session.streams.is_empty() {
        // An empty session has nothing to read; REST reports the same emptiness for less work.
        return Ok(Opened::Fallback(error(
            "the read session has no streams to read",
            Status::InvalidData,
        )));
    }
    let Some(arrow_schema) = session.arrow_schema() else {
        return Ok(Opened::Fallback(error(
            "the read session declared no Arrow schema",
            Status::InvalidData,
        )));
    };
    // Check the session's Arrow schema (a lone IPC schema message) before any reader reaches the
    // caller, so a shape that cannot be reconciled with the canonical schema falls back to REST.
    let session_schema = match decode_schema(&arrow_schema.serialized_schema) {
        Ok(schema) => schema,
        Err(ArrowError::NotYetImplemented(reason)) => {
            return Ok(Opened::Fallback(error(
                format!("the Storage Read schema uses an unsupported Arrow encoding ({reason})"),
                Status::NotImplemented,
            )));
        }
        Err(source) => {
            return Err(error(
                format!("failed to decode the BigQuery Storage Read session schema: {source}"),
                Status::Internal,
            ));
        }
    };
    let plan = match reshape_plans(&session_schema, &schema) {
        Ok(plan) => Arc::new(plan),
        Err(reason) => {
            return Ok(Opened::Fallback(error(
                format!(
                    "the Storage Read schema cannot be reconciled with the driver's Arrow schema \
                     ({reason})"
                ),
                Status::NotImplemented,
            )));
        }
    };
    let streams = session
        .streams
        .into_iter()
        .map(|stream| stream.name)
        .collect();

    Ok(Opened::Reader(spawn_result_reader(
        runtime,
        schema,
        operation,
        |sink| read_streams(read_client, streams, session_schema, plan, sink),
    )))
}

/// Read granted streams concurrently under the shared pipeline limits.
/// Multiple streams require `unordered_read`; the first failure aborts the others.
async fn read_streams(
    client: Read,
    streams: Vec<String>,
    session_schema: SchemaRef,
    plan: Arc<ReshapePlan>,
    sink: ResultSink,
) -> std::result::Result<(), ArrowError> {
    if let [stream] = streams.as_slice() {
        return stream_batches(client, stream.clone(), session_schema, plan, sink).await;
    }
    let mut readers = tokio::task::JoinSet::new();
    for stream in streams {
        let reader: ProducerTask = Box::pin(stream_batches(
            client.clone(),
            stream,
            session_schema.clone(),
            plan.clone(),
            sink.clone(),
        ));
        readers.spawn(reader);
    }
    while let Some(joined) = readers.join_next().await {
        let result = joined
            .map_err(|source| task_failure("Storage Read stream", &source))
            .and_then(|result| result);
        if result.is_err() {
            readers.abort_all();
            return result;
        }
    }
    Ok(())
}

/// What a refused `CreateReadSession` means for this query and for the ones after it.
enum SessionRefusal {
    /// Refused for a reason that will stand: stop attempting read sessions for this project.
    Permanent,
    /// Refused for this read: read this result over REST, but let later queries try again.
    Transient,
}

/// Only refusals that leave the result readable over REST permit fallback. Exhausted transport
/// retries surface as errors instead of hiding behind a slower read.
fn session_refusal(source: &GoogleError) -> Option<SessionRefusal> {
    match google_error_code(source)? {
        // The service also refuses tables REST reads fine, such as a destination with
        // `require_partition_filter`. That is a property of the table, not of the credentials.
        Code::InvalidArgument => Some(SessionRefusal::Transient),
        // `bigquery.readsessions.create` is a permission beyond the one that ran the query, and it
        // is granted per project, so the next query would be refused identically.
        Code::PermissionDenied => Some(SessionRefusal::Permanent),
        // An emulator, or any endpoint serving only part of the API. It will not grow the Storage
        // Read service between one query and the next.
        Code::Unimplemented => Some(SessionRefusal::Permanent),
        // `CreateReadSession` has a per-project rate limit. REST is a good degradation, and the
        // quota refills, so this must not turn the path off for the rest of the session.
        Code::ResourceExhausted => Some(SessionRefusal::Transient),
        _ => None,
    }
}

async fn create_session(
    client: &Read,
    clients: &Arc<ClientState>,
    operation: &Option<Operation>,
    job_reference: &JobReference,
    destination: &TableReference,
    max_streams: i32,
) -> Result<std::result::Result<ReadSession, adbc_core::error::Error>> {
    let table = TableId {
        project_id: &destination.project_id,
        dataset_id: &destination.dataset_id,
        table_id: &destination.table_id,
    }
    .storage_path();
    // LZ4 frame keeps the wire small without the C toolchain a ZSTD decoder would add.
    let serialization =
        ArrowSerializationOptions::new().set_buffer_compression(CompressionCodec::Lz4Frame);
    let session = ReadSession::new()
        .set_data_format(DataFormat::Arrow)
        .set_table(table)
        .set_read_options(TableReadOptions::new().set_arrow_serialization_options(serialization));
    let request = client
        .create_read_session()
        .set_parent(format!("projects/{}", job_reference.project_id))
        .set_read_session(session)
        .set_max_stream_count(max_streams)
        // The audit-logged call of a read, so it names the driver and `bigquery.user_agent`.
        .with_user_agent(clients.user_agent())
        .send();
    let response = gate(operation.as_ref(), request).await?;
    match response {
        Ok(session) => Ok(Ok(session)),
        Err(source) => match session_refusal(&source) {
            Some(refusal) => {
                if matches!(refusal, SessionRefusal::Permanent) {
                    // Cache the refusal across connections so later queries skip this RPC.
                    // Setting the opt-out to `false` on a connection clears the cache.
                    clients.disable_storage_read(&job_reference.project_id);
                }
                Ok(Err(google_error(
                    "BigQuery refused a Storage Read session",
                    source,
                )))
            }
            None => Err(google_error(
                "failed to create a BigQuery Storage Read session",
                source,
            )),
        },
    }
}

async fn stream_batches(
    client: Read,
    stream: String,
    session_schema: SchemaRef,
    plan: Arc<ReshapePlan>,
    sink: ResultSink,
) -> std::result::Result<(), ArrowError> {
    // Overlap receiving with decoding, retaining at most one queued wire payload outside the
    // pipeline's byte budget. A full result buffer stalls both stages through this channel.
    let (sender, receiver) = tokio::sync::mpsc::channel::<(Buffer, i64)>(1);
    let (client, stream, sink) = (&client, stream.as_str(), &sink);
    let receive = async move {
        let result = receive_rows(client, stream, &sender, sink).await;
        // Dropping the sender is what ends the decode loop; `try_join!` holds this future alive
        // until both stages finish, so it cannot be left to the future's own destructor.
        drop(sender);
        result
    };
    let decode = async move {
        let mut receiver = receiver;
        let result = decode_batches(&mut receiver, session_schema, &plan, sink).await;
        // Closing the receiver is what ends the receive loop when decoding stops first, for the
        // same reason the sender above is dropped by hand.
        receiver.close();
        result
    };
    tokio::try_join!(receive, decode)?;
    Ok(())
}

/// Receive payloads and resume transient failures at the next row offset.
/// The SDK retries neither stream opens nor interrupted streams, so both share one budget here.
async fn receive_rows(
    client: &Read,
    stream: &str,
    sender: &tokio::sync::mpsc::Sender<(Buffer, i64)>,
    sink: &ResultSink,
) -> std::result::Result<(), ArrowError> {
    let mut offset = 0_i64;
    let mut budget = ResumeBudget::new();
    let Some(mut responses) =
        open_with_resumes(client, stream, offset, sink, &mut budget, None).await?
    else {
        return Ok(());
    };
    loop {
        // Reserve before polling the SDK: receiving first would materialize another wire
        // payload while the single queued payload still waits for decoding.
        let slot = tokio::select! {
            biased;
            () = sink.cancellation.cancelled() => return Ok(()),
            slot = sender.reserve() => match slot {
                Ok(slot) => slot,
                Err(_) => return Ok(()),
            },
        };
        let response = tokio::select! {
            biased;
            () = sink.cancellation.cancelled() => None,
            () = sender.closed() => None,
            response = responses.next() => response,
        };
        // Cancellation and the end of the stream stop this stage the same way: by returning,
        // which ends the channel and lets the decode stage finish what it already has.
        let Some(response) = response else {
            return Ok(());
        };
        let response = match response {
            Ok(response) => response,
            Err(source) => {
                let reopened =
                    open_with_resumes(client, stream, offset, sink, &mut budget, Some(source))
                        .await?;
                let Some(reopened) = reopened else {
                    return Ok(());
                };
                responses = reopened;
                continue;
            }
        };
        let first_row = offset;
        offset = offset
            .checked_add(response.row_count)
            .filter(|_| response.row_count >= 0)
            .ok_or_else(|| {
                dropped_rows_failure(
                    stream,
                    first_row,
                    response.row_count,
                    "has an invalid row count",
                )
            })?;
        if response.row_count > 0 {
            budget.earn_by_progress();
        }
        // Schema/statistics responses may carry no rows. A positive count without a payload
        // would advance the resume offset past missing rows, so it must fail.
        let serialized = match response.rows {
            Some(Rows::ArrowRecordBatch(batch)) if !batch.serialized_record_batch.is_empty() => {
                batch.serialized_record_batch
            }
            _ if response.row_count > 0 => {
                return Err(dropped_rows_failure(
                    stream,
                    first_row,
                    response.row_count,
                    "carried no Arrow record batch",
                ));
            }
            _ => continue,
        };
        slot.send((Buffer::from(serialized), response.row_count));
    }
}

/// Consecutive-failure allowance and aggregate backoff budget for one stream.
#[derive(Debug)]
struct ResumeBudget {
    /// Consecutive failed opens, which also step the backoff. Reset by any response that carried
    /// rows.
    failures: u32,
    /// Unlike `failures`, this is never reset: it is the read's aggregate bound on time spent
    /// waiting, however many fresh budgets progress earns.
    delay_budget: Duration,
}

impl ResumeBudget {
    fn new() -> Self {
        Self {
            failures: 0,
            delay_budget: READ_ROWS_RESUME_DELAY_BUDGET,
        }
    }

    fn has_attempt_left(&self) -> bool {
        self.failures < READ_ROWS_RESUME_ATTEMPTS
    }

    /// Progress resets consecutive failures, but not the aggregate backoff budget.
    fn earn_by_progress(&mut self) {
        self.failures = 0;
    }

    /// Charge one attempt and clamp its wait to the remaining aggregate budget.
    fn spend_attempt(&mut self) -> Duration {
        self.failures += 1;
        let waited = READ_ROWS_RESUME_BACKOFF
            .delay(self.failures)
            .min(self.delay_budget);
        self.delay_budget -= waited;
        waited
    }
}

/// Open or resume at `offset`, retrying transient failures under the shared budget.
/// `failure` is the error that interrupted an existing stream; the first open has none.
/// `None` means cancellation ended the stage.
async fn open_with_resumes(
    client: &Read,
    stream: &str,
    offset: i64,
    sink: &ResultSink,
    budget: &mut ResumeBudget,
    failure: Option<GoogleError>,
) -> std::result::Result<Option<ResponseStream<ReadRowsResponse>>, ArrowError> {
    let mut failure = match failure {
        Some(failure) => failure,
        None => match open_read_rows(client, stream, offset, sink).await {
            Ok(opened) => return Ok(opened),
            Err(source) => source,
        },
    };
    loop {
        if !budget.has_attempt_left() || !read_rows_is_resumable(&failure) {
            return Err(read_rows_failure(failure));
        }
        let waited = budget.spend_attempt();
        tokio::select! {
            biased;
            () = sink.cancellation.cancelled() => return Ok(None),
            () = tokio::time::sleep(waited) => {}
        }
        match open_read_rows(client, stream, offset, sink).await {
            Ok(opened) => return Ok(opened),
            Err(reopened) => failure = reopened,
        }
    }
}

/// Opens one read stream at `offset`. `None` means the pipeline was cancelled while the call was
/// in flight.
async fn open_read_rows(
    client: &Read,
    stream: &str,
    offset: i64,
    sink: &ResultSink,
) -> std::result::Result<Option<ResponseStream<ReadRowsResponse>>, GoogleError> {
    let request = client
        .read_rows()
        .set_read_stream(stream)
        .set_offset(offset)
        .send();
    tokio::select! {
        biased;
        () = sink.cancellation.cancelled() => Ok(None),
        opened = request => opened.map(Some),
    }
}

/// An inconsistent response cannot be resumed safely: its row count determines the next offset.
fn dropped_rows_failure(stream: &str, first_row: i64, rows: i64, what: &str) -> ArrowError {
    ArrowError::ExternalError(Box::new(error(
        format!(
            "BigQuery Storage Read stream {stream} reported {rows} rows at offset {first_row} in \
             a response that {what}"
        ),
        Status::InvalidData,
    )))
}

fn read_rows_failure(source: GoogleError) -> ArrowError {
    ArrowError::ExternalError(Box::new(google_error(
        "failed to read BigQuery Storage Read rows",
        source,
    )))
}

/// Decode and reshape one stream's batches in order, reusing its session schema.
async fn decode_batches(
    batches: &mut tokio::sync::mpsc::Receiver<(Buffer, i64)>,
    session_schema: SchemaRef,
    plan: &Arc<ReshapePlan>,
    sink: &ResultSink,
) -> std::result::Result<(), ArrowError> {
    loop {
        let serialized = tokio::select! {
            biased;
            () = sink.cancellation.cancelled() => return Ok(()),
            serialized = batches.recv() => serialized,
        };
        let Some((serialized, row_count)) = serialized else {
            return Ok(());
        };

        // Hold a delivery slot until the decoded batch is charged to the byte budget. Take it
        // before the conversion permit to avoid deadlock; see `ResultSink::delivery_slot`.
        let Some(_slot) = sink.delivery_slot().await? else {
            return Ok(());
        };

        // Decompression and array construction are CPU work, bounded by the same driver-wide
        // limit the REST path uses so concurrent readers cannot oversubscribe the machine.
        let Some(permit) = sink.conversion_permit().await? else {
            return Ok(());
        };

        let target = sink.schema.clone();
        let source = Arc::clone(&session_schema);
        let plan = plan.clone();
        let decoded = crate::runtime::run_bounded_result_work(permit, move || {
            let batch = decode_record_batch_message(&serialized, source)?;
            if i64::try_from(batch.num_rows()) != Ok(row_count) {
                return Err(ArrowError::ExternalError(Box::new(error(
                    format!(
                        "BigQuery Storage Read reported {row_count} rows but its Arrow batch contains {}",
                        batch.num_rows()
                    ),
                    Status::InvalidData,
                ))));
            }
            reshape(batch, &target, &plan)
        })
        .await
        .map_err(|source| task_failure("Storage Read decoding", &source))?;
        let batch = decoded?;

        if !sink.deliver(batch).await {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests;
