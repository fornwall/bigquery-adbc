//! A scripted `google.cloud.bigquery.storage.v1` server on a loopback socket.
//!
//! Exercises the generated Storage Read and Storage Write clients, protobuf codec and gRPC
//! framing over HTTP/2 on 127.0.0.1. Tests can interrupt a stream and verify its resumption.
//!
//! Supports cleartext HTTP/2 with a five-byte message prefix and `grpc-status` trailers. [`pb`]
//! encodes and decodes the protobuf fields used by these tests.
//!
//! The write side maintains a cursor, accepts appends only at that cursor, and returns
//! `ALREADY_EXISTS` for earlier offsets. The finalized row count independently checks that
//! recovery writes each row exactly once.
//!
//! Read and write requests must name a stream issued by this server; other names return
//! `NOT_FOUND`. This verifies that recovery resumes the original stream.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_schema::Schema;
use bytes::Bytes;
use h2::RecvStream;
use h2::server::SendResponse;
use http::{HeaderMap, Request, Response};

use crate::arrow_ipc_bigquery::reader::{decode_record_batch_message, decode_schema};
use crate::arrow_ipc_bigquery::writer::{Writer, encode_schema};
use crate::runtime::SharedRuntime;

/// gRPC status codes this mock hands out. `OK` ends a stream that ran to completion.
const OK: u32 = 0;
pub(crate) const INVALID_ARGUMENT: u32 = 3;
pub(crate) const RESOURCE_EXHAUSTED: u32 = 8;
pub(crate) const ABORTED: u32 = 10;
/// A resource name this server never issued. Kept distinct from every other code the mock can
/// answer with, so a test asserting on a scripted refusal cannot pass on a rejected name instead.
const NOT_FOUND: u32 = 5;
const ALREADY_EXISTS: u32 = 6;
const OUT_OF_RANGE: u32 = 11;
const UNIMPLEMENTED: u32 = 12;
/// The mock could not read a request it was sent. That is a defect in the mock or in the framing,
/// never something a test scripts, so it must not share a code with anything a test asserts on.
const INTERNAL: u32 = 13;
pub(crate) const UNAVAILABLE: u32 = 14;
/// Not a gRPC status: end the call with an HTTP/2 `RST_STREAM` instead of trailers, as a dropped
/// proxy or load-balancer connection does. The client sees a transport error with no status.
pub(crate) const RESET: u32 = u32::MAX;

/// Names a read stream issued by the scripted session. `ReadRows` rejects other names.
fn read_stream_name(index: usize) -> String {
    format!("projects/p/locations/l/sessions/mock/streams/{index}")
}

/// What one mock server does with the calls it receives.
///
/// Everything is optional: a read test leaves the append fields alone and a write test leaves the
/// read ones alone, so `..MockPlan::default()` names only what the test is about.
#[derive(Clone, Debug, Default)]
pub(crate) struct MockPlan {
    /// The serialized Arrow IPC schema message the read session declares.
    pub(crate) read_schema: Vec<u8>,
    /// Status of each `CreateReadSession` call; later calls grant the session.
    pub(crate) session_refusals: Vec<u32>,
    /// One `ReadRowsResponse` per entry: the row count it reports, and the serialized Arrow IPC
    /// record batch message it carries. An empty payload is a response carrying no batch at all,
    /// which the driver must still count rows for.
    pub(crate) read_responses: Vec<(i64, Vec<u8>)>,
    /// One entry per `ReadRows` call, in call order: send this many responses, then end the call
    /// with this gRPC status, or [`RESET`]. Calls past the end of the list run to the end of the
    /// stream.
    pub(crate) read_breaks: Vec<(usize, u32)>,
    /// How many read streams `CreateReadSession` grants. Zero (the default) grants one stream.
    ///
    /// The grant ignores the request's `max_stream_count`, which the driver derives from
    /// `available_parallelism`; honouring it would make the tested branch of `read_streams`
    /// depend on the machine.
    pub(crate) read_stream_count: usize,
    /// One entry per `ReadRows` call, in call order: refuse the call with this gRPC status before
    /// any response headers, which is how gRPC reports a failure to open a server stream (the
    /// client's open future resolves to an error). `OK` opens the call normally, and calls past
    /// the end of the list are never refused. [`read_breaks`](Self::read_breaks) instead ends an
    /// established stream, failing its first `next()`.
    pub(crate) read_open_refusals: Vec<u32>,
    /// End the `AppendRows` call once this many appends have been written, without acknowledging
    /// the last of them: the append landed, its acknowledgement did not. Fires once.
    pub(crate) tear_append_after: Option<usize>,
    /// Reject the first append sent at this offset with this gRPC status and do not write it:
    /// `INVALID_ARGUMENT` is how BigQuery refuses rows it cannot accept, and a transient status
    /// is one the driver must replay. Everything queued behind it is then past the stream's
    /// cursor, which is what the service answers `OUT_OF_RANGE` for.
    ///
    /// A later refusal is held until the next append on the call has arrived and been recorded
    /// (or [`HELD_REFUSAL_TIMEOUT`] passes): otherwise the driver can act on the refusal and return
    /// before this server has even read the successors it pipelined, and whether a competing
    /// rejection existed would depend on scheduling. Offset zero has none: the first append is sent
    /// alone and awaited.
    pub(crate) reject_at: Option<(i64, u32)>,
    /// Reject every append at this offset, including replays, with this gRPC status.
    pub(crate) repeat_append_refusal: Option<(i64, u32)>,
    /// Row-level details to include with the rejected append's status.
    pub(crate) rejected_rows: Vec<google_cloud_bigquery::model::RowError>,
    /// Status of each `FinalizeWriteStream` call; later calls succeed.
    pub(crate) finalize_refusals: Vec<u32>,
    /// Commit response errors; an empty list also omits the commit time.
    pub(crate) commit_errors: Option<Vec<google_cloud_bigquery::model::StorageError>>,
    /// Apply the first commit but lose its response; answer the retry with `commit_errors`.
    pub(crate) lose_commit_response: bool,
    /// Answer the append sent at this offset with `ALREADY_EXISTS`, as if its rows had reached
    /// the stream by some other route: the rows are written, and the sender is told they were
    /// already there. Only the driver knows whether it sent the append before.
    pub(crate) already_exists_at: Option<i64>,
    /// Signal the first notification when GetWriteStream arrives, then await the second
    /// before responding. This lets cancellation tests stop an in-flight reattachment.
    pub(crate) attach_gate: Option<Arc<(tokio::sync::Notify, tokio::sync::Notify)>>,
    /// Signal arrival of the append at this offset, then wait for release before answering.
    pub(crate) append_gate: Option<(i64, Arc<(tokio::sync::Notify, tokio::sync::Notify)>)>,
}

/// What the server saw, and the pending write stream it is keeping.
#[derive(Debug, Default)]
struct MockState {
    /// The `offset` of every `ReadRows` request, in call order.
    read_offsets: Vec<i64>,
    /// The stream name of every `ReadRows` request, in the same order. A name appearing twice is
    /// a reopened stream: a resume, or a task that outlived an abort.
    read_stream_calls: Vec<String>,
    /// The `user-agent` header of every `CreateReadSession` request.
    session_user_agents: Vec<String>,
    /// The `user-agent` header of every `ReadRows` request, including refused opens and resumes.
    read_user_agents: Vec<String>,
    /// The `offset` of every `AppendRows` request, in arrival order.
    append_offsets: Vec<i64>,
    /// The offsets answered `ALREADY_EXISTS`, which the driver has to absorb rather than report.
    already_exists: Vec<i64>,
    /// The offsets answered `OUT_OF_RANGE`: appends that arrived past the stream's cursor because
    /// an earlier one was refused. The driver must report the refusal instead of these.
    out_of_range: Vec<i64>,
    /// Rows actually written to the pending stream, which is also its next free offset.
    written_rows: i64,
    /// The row count the last `FinalizeWriteStream` reported.
    finalized_rows: Option<i64>,
    finalize_calls: usize,
    committed: bool,
    commit_calls: usize,
    /// Appends written so far, which is what [`MockPlan::tear_append_after`] counts.
    applied_appends: usize,
    torn: bool,
    /// Whether [`MockPlan::reject_at`] has fired.
    rejected: bool,
    /// `CreateWriteStream` and `GetWriteStream` calls: recovery must reattach, not recreate.
    created_streams: usize,
    attached_streams: usize,
    /// The write stream name this server issued, and the only one it answers for. Every later
    /// call naming a stream is checked against it, so the server enforces that recovery
    /// reattaches to the same stream.
    write_stream: Option<String>,
}

/// A running mock server. Dropping it leaves the serving task to end with the runtime.
#[derive(Debug)]
pub(crate) struct StorageMock {
    endpoint: String,
    state: Arc<Mutex<MockState>>,
}

impl StorageMock {
    /// Binds a loopback port and starts serving `plan` on `runtime`.
    pub(crate) fn start(runtime: &SharedRuntime, plan: MockPlan) -> Self {
        let state = Arc::new(Mutex::new(MockState::default()));
        let (listener, endpoint) = super::http::mock_listener();
        runtime.spawn(serve(listener, Arc::new(plan), state.clone()));
        Self { endpoint, state }
    }

    /// The `bigquery.storage_endpoint` value that addresses this server.
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The `offset` every `ReadRows` call asked to start at, in call order.
    pub(crate) fn read_offsets(&self) -> Vec<i64> {
        lock(&self.state).read_offsets.clone()
    }

    /// The stream every `ReadRows` call named, in call order.
    pub(crate) fn read_stream_calls(&self) -> Vec<String> {
        lock(&self.state).read_stream_calls.clone()
    }

    /// The `user-agent` every `CreateReadSession` call sent.
    pub(crate) fn session_user_agents(&self) -> Vec<String> {
        lock(&self.state).session_user_agents.clone()
    }

    /// The `user-agent` every `ReadRows` call sent, including refused opens and resumes.
    pub(crate) fn read_user_agents(&self) -> Vec<String> {
        lock(&self.state).read_user_agents.clone()
    }

    /// The `offset` every `AppendRows` request carried, in arrival order.
    pub(crate) fn append_offsets(&self) -> Vec<i64> {
        lock(&self.state).append_offsets.clone()
    }

    /// The offsets the server answered `ALREADY_EXISTS` for, in arrival order.
    pub(crate) fn already_exists_offsets(&self) -> Vec<i64> {
        lock(&self.state).already_exists.clone()
    }

    /// The offsets the server answered `OUT_OF_RANGE` for, in arrival order.
    pub(crate) fn out_of_range_offsets(&self) -> Vec<i64> {
        lock(&self.state).out_of_range.clone()
    }

    /// Rows the pending write stream actually holds, counted from the Arrow payloads themselves.
    pub(crate) fn written_rows(&self) -> i64 {
        lock(&self.state).written_rows
    }

    /// The row count `FinalizeWriteStream` reported, and whether the stream was committed.
    pub(crate) fn finalized(&self) -> (Option<i64>, bool) {
        let state = lock(&self.state);
        (state.finalized_rows, state.committed)
    }

    pub(crate) fn finalize_calls(&self) -> usize {
        lock(&self.state).finalize_calls
    }

    /// How many write streams were created, and how many times one was reattached to.
    pub(crate) fn write_streams(&self) -> (usize, usize) {
        let state = lock(&self.state);
        (state.created_streams, state.attached_streams)
    }
}

fn lock(state: &Arc<Mutex<MockState>>) -> MutexGuard<'_, MockState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn serve(listener: std::net::TcpListener, plan: Arc<MockPlan>, state: Arc<Mutex<MockState>>) {
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    while let Ok((socket, _)) = listener.accept().await {
        let (plan, state) = (plan.clone(), state.clone());
        tokio::spawn(async move {
            let Ok(mut connection) = h2::server::handshake(socket).await else {
                return;
            };
            while let Some(Ok((request, responder))) = connection.accept().await {
                let (plan, state) = (plan.clone(), state.clone());
                tokio::spawn(serve_call(request, responder, plan, state));
            }
        });
    }
}

/// Serves one gRPC call: response headers, then whatever the method produces, then the trailers
/// that carry its status. Every method answers on the same shape, so a streaming break is just a
/// non-`OK` status after some data.
async fn serve_call(
    request: Request<RecvStream>,
    mut responder: SendResponse<Bytes>,
    plan: Arc<MockPlan>,
    state: Arc<Mutex<MockState>>,
) {
    let method = request
        .uri()
        .path()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned();
    if matches!(method.as_str(), "CreateReadSession" | "ReadRows") {
        let user_agent = request
            .headers()
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let mut state = lock(&state);
        if method == "CreateReadSession" {
            state.session_user_agents.push(user_agent);
        } else {
            state.read_user_agents.push(user_agent);
        }
    }
    let mut body = request.into_body();
    // `ReadRows` reads its request before answering, because a plan can refuse it at the open,
    // which gRPC reports as a trailers-only response. Every other call answers headers first and
    // carries its status in the trailers.
    let read_call = if method == "ReadRows" {
        let Some(request) = next_message(&mut body, &mut Vec::new()).await else {
            return;
        };
        let call = {
            let mut state = lock(&state);
            state.read_offsets.push(read_offset(&request));
            state
                .read_stream_calls
                .push(pb::field_string(&request, 1).unwrap_or_default().to_owned());
            state.read_offsets.len() - 1
        };
        match plan.read_open_refusals.get(call) {
            Some(&status) if status != OK => {
                let mut refusal = Response::new(());
                refusal
                    .headers_mut()
                    .insert("content-type", "application/grpc".parse().unwrap());
                refusal
                    .headers_mut()
                    .insert("grpc-status", status.to_string().parse().unwrap());
                let _ = responder.send_response(refusal, true);
                return;
            }
            _ => Some((request, call)),
        }
    } else {
        None
    };
    let mut response = Response::new(());
    response
        .headers_mut()
        .insert("content-type", "application/grpc".parse().unwrap());
    let Ok(mut sender) = responder.send_response(response, false) else {
        return;
    };

    let status = match method.as_str() {
        "CreateReadSession" => {
            let call = lock(&state).session_user_agents.len() - 1;
            unary(&mut body, &mut sender, |request| {
                match plan.session_refusals.get(call) {
                    Some(&status) => Err(status),
                    None => Ok(create_read_session(&plan, &request)),
                }
            })
            .await
        }
        "ReadRows" => match read_call {
            Some((request, call)) => read_rows(&request, &mut sender, &plan, call),
            // Unreachable: the block above builds this for exactly this method.
            None => INTERNAL,
        },
        "CreateWriteStream" => {
            unary(&mut body, &mut sender, |request| {
                let mut state = lock(&state);
                state.created_streams += 1;
                // The stream name a pending writer is handed, and the one it reattaches by.
                let name = format!(
                    "{}/streams/mock",
                    pb::field_string(&request, 1).unwrap_or_default()
                );
                state.write_stream = Some(name.clone());
                Ok(write_stream(&name))
            })
            .await
        }
        "GetWriteStream" => {
            if let Some(gate) = &plan.attach_gate {
                gate.0.notify_one();
                gate.1.notified().await;
            }
            unary(&mut body, &mut sender, |request| {
                let mut state = lock(&state);
                let name = issued_stream(&state, pb::field_string(&request, 1))?;
                state.attached_streams += 1;
                Ok(write_stream(&name))
            })
            .await
        }
        "AppendRows" => append_rows(&mut body, &mut sender, &plan, &state).await,
        "FinalizeWriteStream" => {
            unary(&mut body, &mut sender, |request| {
                let mut state = lock(&state);
                issued_stream(&state, pb::field_string(&request, 1))?;
                let call = state.finalize_calls;
                state.finalize_calls += 1;
                if let Some(&status) = plan.finalize_refusals.get(call)
                    && status != OK
                {
                    return Err(status);
                }
                state.finalized_rows = Some(state.written_rows);
                Ok(pb::int64_message(1, state.written_rows))
            })
            .await
        }
        "BatchCommitWriteStreams" => {
            unary(&mut body, &mut sender, |request| {
                let mut state = lock(&state);
                // `write_streams` is repeated; this ingest commits the one stream it opened.
                issued_stream(&state, pb::field_string(&request, 2))?;
                state.commit_calls += 1;
                if plan.lose_commit_response && state.commit_calls == 1 {
                    state.committed = true;
                    return Err(UNAVAILABLE);
                }
                if let Some(errors) = &plan.commit_errors {
                    let mut response = Vec::new();
                    for error in errors {
                        let mut entry =
                            pb::int64_message(1, i64::from(error.code.value().unwrap()));
                        pb::string_field(&mut entry, 2, &error.entity);
                        pb::string_field(&mut entry, 3, &error.error_message);
                        response.extend(pb::message(2, &entry));
                    }
                    return Ok(response);
                }
                state.committed = true;
                // `commit_time`, whose absence the driver reads as an uncommitted stream.
                Ok(pb::message(1, &pb::int64_message(1, 1)))
            })
            .await
        }
        _ => UNIMPLEMENTED,
    };
    if status == RESET {
        sender.send_reset(h2::Reason::INTERNAL_ERROR);
    } else {
        let _ = sender.send_trailers(trailers(status));
    }
}

/// Reads one request message and answers with one response message, or with the responder's
/// refusal status (as for a resource this server never issued).
async fn unary(
    body: &mut RecvStream,
    sender: &mut h2::SendStream<Bytes>,
    respond: impl FnOnce(Vec<u8>) -> Result<Vec<u8>, u32>,
) -> u32 {
    let Some(request) = next_message(body, &mut Vec::new()).await else {
        return INTERNAL;
    };
    let response = match respond(request) {
        Ok(response) => response,
        Err(status) => return status,
    };
    match sender.send_data(frame(&response), false) {
        Ok(()) => OK,
        Err(_) => UNAVAILABLE,
    }
}

fn create_read_session(plan: &MockPlan, request: &[u8]) -> Vec<u8> {
    let table = pb::field_bytes(request, 2)
        .and_then(|session| pb::field_string(session, 6))
        .unwrap_or_default();
    let mut session = Vec::new();
    pb::string_field(&mut session, 1, "projects/p/locations/l/sessions/mock");
    pb::int64_field(&mut session, 3, 2); // DataFormat::ARROW
    // The oneof's `ArrowSchema` arm, whose sole field is the serialized schema.
    pb::bytes_field(&mut session, 5, &pb::message(1, &plan.read_schema));
    pb::string_field(&mut session, 6, table);
    for index in 0..granted_read_streams(plan) {
        let mut stream = Vec::new();
        pb::string_field(&mut stream, 1, &read_stream_name(index));
        pb::bytes_field(&mut session, 10, &stream);
    }
    session
}

/// How many read streams this plan's session hands out.
fn granted_read_streams(plan: &MockPlan) -> usize {
    plan.read_stream_count.max(1)
}

/// Serves one `ReadRows` call from the offset it asked for, breaking where the plan says to.
///
/// The offset is resolved against the plan's own row counts, so a driver that resumed at the
/// wrong row gets `OUT_OF_RANGE` rather than a plausible-looking stream.
fn read_rows(
    request: &[u8],
    sender: &mut h2::SendStream<Bytes>,
    plan: &MockPlan,
    call: usize,
) -> u32 {
    // Only a stream the session handed out is served, so a resume that lost or rewrote the name
    // is refused rather than answered from the same script as the original call.
    let name = pb::field_string(request, 1).unwrap_or_default();
    if !(0..granted_read_streams(plan)).any(|index| read_stream_name(index) == name) {
        return NOT_FOUND;
    }
    let Some(start) = response_index(plan, read_offset(request)) else {
        return OUT_OF_RANGE;
    };

    let (limit, status) = plan
        .read_breaks
        .get(call)
        .copied()
        .unwrap_or((usize::MAX, OK));
    for (rows, batch) in plan.read_responses.iter().skip(start).take(limit) {
        let mut response = Vec::new();
        pb::int64_field(&mut response, 6, *rows);
        if !batch.is_empty() {
            pb::bytes_field(&mut response, 4, &pb::message(1, batch));
        }
        if sender.send_data(frame(&response), false).is_err() {
            return UNAVAILABLE;
        }
    }
    status
}

/// The row a `ReadRowsRequest` asks to start at. An absent `offset` is the start of the stream.
fn read_offset(request: &[u8]) -> i64 {
    pb::field_varint(request, 2).map_or(0, u64::cast_signed)
}

/// The response index whose first row sits at `offset`, or `None` when no response starts there.
fn response_index(plan: &MockPlan, offset: i64) -> Option<usize> {
    let mut cursor = 0;
    for (index, (rows, _)) in plan.read_responses.iter().enumerate() {
        if cursor == offset {
            return Some(index);
        }
        cursor += rows;
    }
    (cursor == offset).then_some(plan.read_responses.len())
}

/// How long [`MockPlan::reject_at`] waits for a successor before answering the refusal anyway, so a
/// driver that pipelines nothing behind the refused append gets its answer instead of hanging.
const HELD_REFUSAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Serves one `AppendRows` call as a pending stream with a write cursor.
async fn append_rows(
    body: &mut RecvStream,
    sender: &mut h2::SendStream<Bytes>,
    plan: &MockPlan,
    state: &Arc<Mutex<MockState>>,
) -> u32 {
    let mut buffer = Vec::new();
    let mut named_stream = false;
    let mut call_schema: Option<Vec<u8>> = None;
    let mut held_refusal: Option<Vec<u8>> = None;
    loop {
        let next = next_message(body, &mut buffer);
        let request = match held_refusal.take() {
            None => next.await,
            // `next_message` is cancel-safe: a partial message stays in `buffer`.
            Some(refusal) => match tokio::time::timeout(HELD_REFUSAL_TIMEOUT, next).await {
                Ok(request) => {
                    held_refusal = Some(refusal);
                    request
                }
                Err(_) => {
                    if sender.send_data(frame(&refusal), false).is_err() {
                        return UNAVAILABLE;
                    }
                    continue;
                }
            },
        };
        let Some(request) = request else { break };
        // `write_stream` identifies the stream the whole call appends to, so BigQuery requires it
        // on the first request and ignores it afterwards. A call that names a stream this server
        // never issued (a recreated one, or a stale name kept across a reattach) is refused.
        match pb::field_string(&request, 1) {
            Some(named) => {
                if issued_stream(&lock(state), Some(named)).is_err() {
                    return NOT_FOUND;
                }
                named_stream = true;
            }
            None if !named_stream => return NOT_FOUND,
            None => {}
        }
        // `AppendRowsRequest.offset` is a `google.protobuf.Int64Value`, so offset zero arrives as
        // a present but empty wrapper; reading the missing inner field as an absent offset would
        // misplace every stream's first append.
        let offset = pb::field_bytes(&request, 2).map_or(-1, |wrapper| {
            pb::field_varint(wrapper, 1).map_or(0, u64::cast_signed)
        });
        // The writer schema follows the same rule as `write_stream`: required on the call's first
        // request, then omitted by a client that has nothing new to declare.
        let Some(rows) = appended_row_count(&request, &mut call_schema) else {
            return INVALID_ARGUMENT;
        };
        if let Some((_, gate)) = plan.append_gate.as_ref().filter(|(at, _)| *at == offset) {
            gate.0.notify_one();
            gate.1.notified().await;
        }

        let mut state = lock(state);
        state.append_offsets.push(offset);
        if let Some((_, code)) = plan.repeat_append_refusal.filter(|&(at, _)| at == offset) {
            return code;
        }
        let refusal = plan
            .reject_at
            .filter(|&(at, _)| at == offset && !state.rejected);
        let response = if let Some((_, code)) = refusal {
            state.rejected = true;
            let mut response = append_status(code);
            for row in &plan.rejected_rows {
                let mut entry = pb::int64_message(1, row.index);
                pb::int64_field(&mut entry, 2, i64::from(row.code.value().unwrap()));
                pb::string_field(&mut entry, 3, &row.message);
                response.extend(pb::message(4, &entry));
            }
            response
        } else if offset < state.written_rows {
            // Behind the cursor: these rows are already on the stream, which is what a replay of
            // an append whose acknowledgement was lost must be told.
            state.already_exists.push(offset);
            append_status(ALREADY_EXISTS)
        } else if offset > state.written_rows {
            state.out_of_range.push(offset);
            append_status(OUT_OF_RANGE)
        } else {
            state.written_rows += rows;
            state.applied_appends += 1;
            if !state.torn && plan.tear_append_after == Some(state.applied_appends) {
                state.torn = true;
                // The append is written; the call ends before it is acknowledged.
                return UNAVAILABLE;
            }
            if plan.already_exists_at == Some(offset) {
                state.already_exists.push(offset);
                append_status(ALREADY_EXISTS)
            } else {
                append_result(offset)
            }
        };
        drop(state);
        if refusal.is_some() && offset != 0 {
            held_refusal = Some(response);
            continue;
        }
        for response in held_refusal.take().into_iter().chain([response]) {
            if sender.send_data(frame(&response), false).is_err() {
                return UNAVAILABLE;
            }
        }
    }
    match held_refusal {
        Some(response) if sender.send_data(frame(&response), false).is_err() => UNAVAILABLE,
        _ => OK,
    }
}

/// The rows one `AppendRowsRequest` carries, counted by decoding its own Arrow payload against
/// the schema it declares, or else the one its call declared earlier; `None` if neither exists.
fn appended_row_count(request: &[u8], call_schema: &mut Option<Vec<u8>>) -> Option<i64> {
    let Some(arrow) = pb::field_bytes(request, 5) else {
        return Some(0);
    };
    if let Some(schema) = pb::field_bytes(arrow, 1).and_then(|schema| pb::field_bytes(schema, 1)) {
        *call_schema = Some(schema.to_vec());
    }
    let batch = pb::field_bytes(arrow, 2)
        .and_then(|rows| pb::field_bytes(rows, 1))
        .unwrap_or_default();
    Some(ipc_row_count(call_schema.as_deref()?, batch))
}

/// The write stream name this server issued, if `named` is that name.
///
/// Every call naming a stream goes through here, so a recreated, stale or invented name is
/// refused.
fn issued_stream(state: &MockState, named: Option<&str>) -> Result<String, u32> {
    match (state.write_stream.as_deref(), named) {
        (Some(issued), Some(named)) if issued == named => Ok(issued.to_owned()),
        _ => Err(NOT_FOUND),
    }
}

fn write_stream(name: &str) -> Vec<u8> {
    let mut stream = Vec::new();
    pb::string_field(&mut stream, 1, name);
    pb::int64_field(&mut stream, 2, 2); // WriteStream::Type::PENDING
    stream
}

/// An `AppendRowsResponse` acknowledging an append at `offset`. The acknowledged offset is a
/// `google.protobuf.Int64Value`, so it nests one message deeper than the field it sits in.
fn append_result(offset: i64) -> Vec<u8> {
    pb::message(1, &pb::message(1, &pb::int64_message(1, offset)))
}

/// An `AppendRowsResponse` whose `error` arm carries `code`, which is how BigQuery reports a
/// per-append failure without ending the call.
fn append_status(code: u32) -> Vec<u8> {
    pb::message(2, &pb::int64_message(1, i64::from(code)))
}

// -- transport ------------------------------------------------------------------------------

/// One gRPC message on the wire: an uncompressed flag, a length, and the encoded message.
fn frame(message: &[u8]) -> Bytes {
    let mut framed = Vec::with_capacity(message.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
    framed.extend_from_slice(message);
    Bytes::from(framed)
}

fn trailers(status: u32) -> HeaderMap {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", status.to_string().parse().unwrap());
    trailers
}

/// Reads the next whole gRPC message off a request body, releasing flow control as it consumes.
///
/// `buffer` carries whatever arrived past the end of that message, because one HTTP/2 data frame
/// may hold several requests; dropping the remainder would silently lose an append.
async fn next_message(body: &mut RecvStream, buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if buffer.len() >= 5 {
            let length = usize::try_from(u32::from_be_bytes(buffer[1..5].try_into().unwrap()))
                .expect("a gRPC message length fits a usize");
            if buffer.len() >= 5 + length {
                let message = buffer[5..5 + length].to_vec();
                buffer.drain(..5 + length);
                return Some(message);
            }
        }
        let chunk = body.data().await?.ok()?;
        let _ = body.flow_control().release_capacity(chunk.len());
        buffer.extend_from_slice(&chunk);
    }
}

// -- Arrow IPC ------------------------------------------------------------------------------

/// The Arrow IPC messages a Storage Read session and its responses are made of: the schema
/// message the session declares, then one record batch message per response.
pub(crate) fn ipc_messages(schema: &Schema, batches: &[RecordBatch]) -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut writer = Writer::new();
    let schema_message = encode_schema(schema).unwrap();
    let batch_messages = batches
        .iter()
        .map(|batch| writer.encode_batch(batch).unwrap())
        .collect();
    (schema_message, batch_messages)
}

/// Counts the rows in one `schema ++ batch` Arrow IPC pair.
fn ipc_row_count(schema: &[u8], batch: &[u8]) -> i64 {
    let schema = decode_schema(schema).unwrap();
    let batch = decode_record_batch_message(&Buffer::from(batch.to_vec()), schema).unwrap();
    i64::try_from(batch.num_rows()).unwrap()
}

// -- protobuf -------------------------------------------------------------------------------

/// Just enough protobuf to build and read the messages these two services exchange.
///
/// `*_field` and `*_message` build; `field_*` read. Only length-delimited and varint fields appear
/// in these messages, so those are the only two wire types encoded; anything else met on the wire
/// is skipped as an unknown field.
mod pb {
    fn varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        out.push(u8::try_from(value).unwrap());
    }

    fn tag(out: &mut Vec<u8>, number: u32, wire_type: u32) {
        varint(out, u64::from(number) << 3 | u64::from(wire_type));
    }

    pub(super) fn int64_field(out: &mut Vec<u8>, number: u32, value: i64) {
        tag(out, number, 0);
        varint(out, value.cast_unsigned());
    }

    pub(super) fn bytes_field(out: &mut Vec<u8>, number: u32, value: &[u8]) {
        tag(out, number, 2);
        varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value);
    }

    pub(super) fn string_field(out: &mut Vec<u8>, number: u32, value: &str) {
        bytes_field(out, number, value.as_bytes());
    }

    /// A message holding one length-delimited field, which is most of what these responses are.
    pub(super) fn message(number: u32, value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        bytes_field(&mut out, number, value);
        out
    }

    /// A message holding one varint field, which is also the shape of `google.protobuf.Int64Value`.
    pub(super) fn int64_message(number: u32, value: i64) -> Vec<u8> {
        let mut out = Vec::new();
        int64_field(&mut out, number, value);
        out
    }

    fn read_varint(message: &[u8], cursor: &mut usize) -> Option<u64> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = *message.get(*cursor)?;
            *cursor += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Some(value);
            }
        }
        None
    }

    /// Calls `visit` with every field's number and payload until one of them answers.
    fn find<'a, T>(
        message: &'a [u8],
        mut visit: impl FnMut(u32, WireValue<'a>) -> Option<T>,
    ) -> Option<T> {
        let mut cursor = 0;
        while cursor < message.len() {
            let key = read_varint(message, &mut cursor)?;
            let (number, wire_type) = (u32::try_from(key >> 3).ok()?, key & 7);
            let value = match wire_type {
                0 => WireValue::Varint(read_varint(message, &mut cursor)?),
                1 | 5 => {
                    let width = if wire_type == 1 { 8 } else { 4 };
                    cursor += width;
                    WireValue::Skipped
                }
                2 => {
                    let length = usize::try_from(read_varint(message, &mut cursor)?).ok()?;
                    let bytes = message.get(cursor..cursor + length)?;
                    cursor += length;
                    WireValue::Bytes(bytes)
                }
                _ => return None,
            };
            if let Some(found) = visit(number, value) {
                return Some(found);
            }
        }
        None
    }

    enum WireValue<'a> {
        Varint(u64),
        Bytes(&'a [u8]),
        Skipped,
    }

    pub(super) fn field_varint(message: &[u8], number: u32) -> Option<u64> {
        find(message, |field, value| match value {
            WireValue::Varint(value) if field == number => Some(value),
            _ => None,
        })
    }

    /// The payload of a length-delimited field: a nested message, a string, or bytes.
    pub(super) fn field_bytes(message: &[u8], number: u32) -> Option<&[u8]> {
        find(message, |field, value| match value {
            WireValue::Bytes(bytes) if field == number => Some(bytes),
            _ => None,
        })
    }

    pub(super) fn field_string(message: &[u8], number: u32) -> Option<&str> {
        field_bytes(message, number).and_then(|bytes| std::str::from_utf8(bytes).ok())
    }
}
