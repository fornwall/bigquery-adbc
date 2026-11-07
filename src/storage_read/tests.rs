use super::*;
use crate::test_support::errors::assert_failure;

/// What one `ReadRows` call does.
#[derive(Clone, Copy, Debug)]
enum ReadCall {
    /// The open never answers.
    Hang,
    /// This many one-row responses, then silence.
    Stall(usize),
    /// This many one-row responses, then the end of the stream.
    Finish(usize),
    /// The open fails with this status.
    Refuse(Code),
}

/// What a [`ScriptedRead`] saw: each call's offset, and how many responses were asked for.
#[derive(Debug, Default)]
struct ReadLog {
    offsets: std::sync::Mutex<Vec<i64>>,
    polls: std::sync::atomic::AtomicUsize,
}

impl ReadLog {
    fn offsets(&self) -> Vec<i64> {
        self.offsets.lock().unwrap().clone()
    }

    fn polls(&self) -> usize {
        self.polls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Serves `ReadRows` calls from a script. Responses exist only once polled, as the SDK
/// materializes each protobuf message.
#[derive(Debug)]
struct ScriptedRead {
    calls: std::sync::Mutex<std::collections::VecDeque<ReadCall>>,
    log: Arc<ReadLog>,
}

impl google_cloud_bigquery::stub::Read for ScriptedRead {
    async fn read_rows(
        &self,
        request: google_cloud_bigquery::model::ReadRowsRequest,
        _options: google_cloud_gax::options::RequestOptions,
    ) -> std::result::Result<ResponseStream<ReadRowsResponse>, GoogleError> {
        self.log.offsets.lock().unwrap().push(request.offset);
        let call = self.calls.lock().unwrap().pop_front();
        let (rows, stall) = match call.expect("the script covers every call") {
            ReadCall::Hang => std::future::pending().await,
            ReadCall::Stall(rows) => (rows, true),
            ReadCall::Finish(rows) => (rows, false),
            ReadCall::Refuse(code) => return Err(service_failure(code)),
        };
        let log = self.log.clone();
        Ok(ResponseStream::from_stream(ScriptedRows {
            rows,
            stall,
            log,
        }))
    }
}

struct ScriptedRows {
    rows: usize,
    stall: bool,
    log: Arc<ReadLog>,
}

impl futures_core::Stream for ScriptedRows {
    type Item = std::result::Result<ReadRowsResponse, GoogleError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        self.log
            .polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.rows == 0 {
            return if self.stall {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        }
        self.rows -= 1;
        Poll::Ready(Some(Ok(ReadRowsResponse::new()
            .set_row_count(1)
            .set_arrow_record_batch(
                google_cloud_bigquery::model::ArrowRecordBatch::new()
                    .set_serialized_record_batch(vec![0_u8; 1]),
            ))))
    }
}

/// The ADBC error a failed read carries.
fn adbc_failure(failure: &ArrowError) -> &adbc_core::error::Error {
    let ArrowError::ExternalError(source) = failure else {
        panic!("expected an ADBC error: {failure}");
    };
    source.downcast_ref().expect("expected an ADBC error")
}

/// One scripted client and the log of what it saw.
fn scripted_client(script: &[ReadCall]) -> (Read, Arc<ReadLog>) {
    let log = Arc::<ReadLog>::default();
    let client = Read::from_stub(ScriptedRead {
        calls: std::sync::Mutex::new(script.iter().copied().collect()),
        log: log.clone(),
    });
    (client, log)
}

/// A runtime whose clock jumps to the next timer whenever every task is idle.
fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Reserving a queue slot before polling means a full wire queue materializes no further response.
/// Either way of stopping the stage then releases a pending poll without making another.
#[test]
fn a_stopped_stage_polls_no_further_responses() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    // A stream that always has another response, then one that never does.
    for (call, queued) in [(ReadCall::Finish(usize::MAX), 1), (ReadCall::Stall(0), 0)] {
        for cancel in [false, true] {
            let (client, log) = scripted_client(&[call]);
            crate::runtime::new_runtime().unwrap().block_on(async {
                let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
                let sink = ResultSink::for_test();
                let request = client.read_rows().set_read_stream("stream");
                let mut receiving = std::pin::pin!(receive_rows(&request, "stream", sender, &sink));
                let mut context = Context::from_waker(Waker::noop());

                assert!(receiving.as_mut().poll(&mut context).is_pending());
                assert_eq!((receiver.len(), log.polls()), (queued, 1));
                if queued == 1 {
                    assert_eq!(receiver.try_recv().unwrap().1, 1);
                    assert!(receiving.as_mut().poll(&mut context).is_pending());
                    assert_eq!((receiver.len(), log.polls()), (1, 2));
                }

                let polls = log.polls();
                if cancel {
                    sink.cancellation.cancel_local();
                } else {
                    receiver.close();
                }
                assert!(matches!(
                    receiving.as_mut().poll(&mut context),
                    Poll::Ready(Ok(()))
                ));
                assert_eq!(log.polls(), polls);
            });
        }
    }
}

/// Neither the SDK nor its channel bounds a silent call, so a connection that died without a reset
/// would block the reader forever. A silent open or stream is a failed attempt like any other: it
/// resumes at the next unread row, and spends the same allowance.
#[test]
fn a_silent_read_stream_resumes_and_then_exhausts_its_allowance() {
    use ReadCall::{Finish, Hang, Stall};

    let attempts = usize::try_from(READ_ROWS_RESUME_ATTEMPTS).unwrap();
    let stalled = vec![Stall(0); attempts + 3];
    let exhausted = vec![0; attempts + 1];
    let cases: [(&[ReadCall], &[i64], i64, bool); 4] = [
        (&[Stall(2), Finish(1)], &[0, 2], 3, true),
        (&[Hang, Finish(1)], &[0, 0], 1, true),
        (
            &[Stall(1), Stall(0), Hang, Finish(1)],
            &[0, 1, 1, 1],
            2,
            true,
        ),
        (&stalled, &exhausted, 0, false),
    ];
    for (script, expected_offsets, expected_rows, completes) in cases {
        let (client, log) = scripted_client(script);
        let (result, rows) = paused_runtime().block_on(async {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let sink = ResultSink::for_test();
            let request = client.read_rows().set_read_stream("stream");
            let draining = async {
                let mut rows = 0;
                while let Some((_, count)) = receiver.recv().await {
                    rows += count;
                }
                rows
            };
            tokio::join!(receive_rows(&request, "stream", sender, &sink), draining)
        });
        assert_eq!(log.offsets(), expected_offsets, "{script:?}");
        assert_eq!(rows, expected_rows, "{script:?}");
        if completes {
            result.unwrap();
        } else {
            assert_failure(adbc_failure(&result.unwrap_err()), Status::Timeout, &[]);
        }
    }
}

/// A reader that stopped reading is not a stalled service. Streams of one read share a connection's
/// flow-control window, so a stream can go silent because its siblings' rows sit unread, and a
/// queue the decoder has not emptied stops this stream being polled at all. Only once the reader
/// makes room does silence count.
#[test]
fn a_read_stream_is_not_stalled_while_the_reader_holds_it_back() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let tiny = arrow_array::RecordBatch::new_empty(schema.clone());
    let huge = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![
            0_i64;
            8 * 1024 * 1024
        ]))],
    )
    .unwrap();
    let silence = STALL_TIMEOUT * (READ_ROWS_RESUME_ATTEMPTS + 2);
    let full_pipelines = [
        vec![tiny; crate::pipeline::QUERY_RESULT_BUFFER_SIZE],
        vec![huge],
    ];
    for batches in full_pipelines {
        let (client, log) = scripted_client(&[ReadCall::Stall(0), ReadCall::Finish(1)]);
        paused_runtime().block_on(async {
            let (sink, mut delivered) = ResultSink::for_test_reading(schema.clone());
            for batch in batches {
                assert!(sink.deliver(batch).await);
            }
            // Room for the payload and for the reservation that then finds the stream's end.
            let (sender, _receiver) = tokio::sync::mpsc::channel(2);
            let request = client.read_rows().set_read_stream("stream");
            let mut receiving = std::pin::pin!(receive_rows(&request, "stream", sender, &sink));
            tokio::select! {
                result = &mut receiving => panic!("the read ended early: {result:?}"),
                () = tokio::time::sleep(silence) => {}
            }
            assert_eq!(log.offsets(), [0]);

            delivered.next().await.unwrap();
            receiving.await.unwrap();
            assert_eq!(log.offsets(), [0, 0]);
        });
    }

    // One response fills the wire queue, so the next one is not even being waited for.
    let (client, log) = scripted_client(&[ReadCall::Finish(2)]);
    paused_runtime().block_on(async {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let sink = ResultSink::for_test();
        let request = client.read_rows().set_read_stream("stream");
        tokio::select! {
            result = receive_rows(&request, "stream", sender, &sink) => {
                panic!("the read ended early: {result:?}")
            }
            () = tokio::time::sleep(silence) => {}
        }
    });
    assert_eq!(log.offsets(), [0]);
}

/// One stream of a parallel read failing unresumably fails the whole read and stops the others.
///
/// Calls take the script in the order they arrive, so the refusal reaches whichever stream opens
/// last, after the other two are silent. Each of those would reopen at the stall limit if it were
/// still running, and on the paused clock that limit cannot pass before the failure lands.
#[test]
fn one_stream_failing_unresumably_fails_the_parallel_read_and_stops_the_rest() {
    let script = [
        ReadCall::Stall(0),
        ReadCall::Stall(0),
        ReadCall::Refuse(Code::InvalidArgument),
    ];
    let (client, log) = scripted_client(&script);
    paused_runtime().block_on(async {
        let schema = Arc::new(arrow_schema::Schema::empty());
        let plan = Arc::new(reshape_plans(&schema, &schema).unwrap());
        let streams = (0..script.len()).map(|index| index.to_string()).collect();
        let sink = ResultSink::for_test();
        let failure = read_streams(client.read_rows(), streams, schema, plan, sink)
            .await
            .unwrap_err()
            .to_string();
        // The scripted status itself, not a `JoinError` from the abort and not a cancellation:
        // the failing stream's own error is what the caller has to be told about.
        assert!(failure.contains("Storage Read rows"), "{failure}");
        assert!(failure.contains("INVALID_ARGUMENT"), "{failure}");

        tokio::time::sleep(STALL_TIMEOUT * 2).await;
    });
    assert_eq!(log.offsets().len(), script.len());
}

/// The unordered stream request stays a sane "units of work" bound on any machine.
#[test]
fn unordered_stream_request_is_positive_and_clamped() {
    let count = unordered_max_stream_count();
    assert!((1..=32).contains(&count), "{count}");
}

fn service_failure(code: Code) -> GoogleError {
    GoogleError::service(google_cloud_gax::error::rpc::Status::default().set_code(code))
}

fn refusal(code: Code) -> Option<SessionRefusal> {
    session_refusal(&service_failure(code))
}

/// Only refusals that leave the result readable over REST may become a REST read. A transport
/// failure the client already retried has to reach the caller rather than hide behind a slower
/// path that happens to work.
#[test]
fn only_refusals_rest_can_read_past_fall_back_to_rest() {
    for code in [Code::PermissionDenied, Code::Unimplemented] {
        assert!(matches!(refusal(code), Some(SessionRefusal::Permanent)));
    }
    for code in [Code::ResourceExhausted, Code::InvalidArgument] {
        assert!(matches!(refusal(code), Some(SessionRefusal::Transient)));
    }

    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::Internal,
        Code::Unauthenticated,
    ] {
        assert!(
            refusal(code).is_none(),
            "{code:?} must be reported, not silently read over REST"
        );
    }
}

/// A `ReadRows` stream that breaks partway through resumes at the row offset already received
/// (`ReadRowsRequest.offset`), neither repeating nor dropping a row. Only failures unrelated to the
/// request resume; the driver's own cancellation and a request the service rejected are reported.
#[test]
fn transient_and_quota_failures_resume_a_broken_read_stream() {
    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::Internal,
        Code::Aborted,
        Code::ResourceExhausted,
    ] {
        assert!(
            google_error_is_transient(&service_failure(code)),
            "{code:?} ends a read that could have continued"
        );
    }
    for code in [
        Code::Cancelled,
        Code::InvalidArgument,
        Code::PermissionDenied,
        Code::NotFound,
        Code::OutOfRange,
        Code::Unauthenticated,
    ] {
        assert!(
            !google_error_is_transient(&service_failure(code)),
            "{code:?} must be reported, not replayed"
        );
    }
}

/// The resume waits are bounded in aggregate, not only within one burst of failures.
///
/// A peer that breaks the stream after every response earns a fresh backoff each time, because
/// rows arrived in between. The bound is on waiting, not on resumes, so it cannot fail a slow read:
/// once it is spent, resumes continue without waiting.
#[test]
fn the_aggregate_resume_wait_is_bounded_however_often_progress_resets_the_backoff() {
    let mut budget = ResumeBudget::new();
    let mut total = Duration::ZERO;
    for _ in 0..10_000 {
        // Each break follows a row-carrying response, which is what restarts the backoff.
        budget.earn_by_progress();
        total += budget.spend_attempt();
    }
    assert_eq!(total, READ_ROWS_RESUME_DELAY_BUDGET);
    assert_eq!(budget.delay_budget, Duration::ZERO);
    // Spent is not failed: the next resume still happens, it just no longer waits.
    budget.earn_by_progress();
    assert_eq!(budget.spend_attempt(), Duration::ZERO);
}

/// The consecutive-failure allowance ends a stream that cannot make progress, and opens spend it
/// as well as resumes, whether or not a response ever arrived.
#[test]
fn the_consecutive_failure_allowance_is_exhausted_by_attempts_alone() {
    let mut budget = ResumeBudget::new();
    for _ in 0..READ_ROWS_RESUME_ATTEMPTS {
        assert!(budget.has_attempt_left());
        budget.spend_attempt();
    }
    assert!(!budget.has_attempt_left());
    budget.earn_by_progress();
    assert!(budget.has_attempt_left());
}

/// Each wait is jittered below its step's cap, so streams broken together do not reopen together,
/// and the cap doubles per consecutive failure from 250 ms to at most 2 s.
#[test]
fn resume_waits_are_jittered_below_a_doubling_cap() {
    let mut budget = ResumeBudget::new();
    let waits: Vec<Duration> = (0..READ_ROWS_RESUME_ATTEMPTS)
        .map(|_| budget.spend_attempt())
        .collect();
    for (wait, cap_ms) in waits.iter().zip([250, 500, 1_000, 2_000]) {
        assert!(*wait <= Duration::from_millis(cap_ms), "{waits:?}");
    }
    let firsts: std::collections::HashSet<Duration> = (0..32)
        .map(|_| ResumeBudget::new().spend_attempt())
        .collect();
    assert!(firsts.len() > 1, "32 first waits were all {firsts:?}");
}

/// Everything one scripted Storage Read needs: a mock server, a client state pointed at it, and
/// the canonical schema its session declares.
mod mock_reads {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_buffer::Buffer;
    use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

    use adbc_core::CancelHandle;
    use adbc_core::error::Status;
    use arrow_array::RecordBatchReader;

    use super::{
        Opened, READ_ROWS_RESUME_ATTEMPTS, ReshapePlan, adbc_failure, assert_failure,
        create_read_session, decode_batches, reader, reshape_plans,
    };
    use crate::cancellation::{Cancellation, Operation, OperationCancelHandle, operation_reader};
    use crate::clients::ClientState;
    use crate::config::ServiceEndpointKind;
    use crate::pipeline::{QueryResultOptions, ResultSink};
    use crate::rest::jobs::JobReference;
    use crate::rest::model::TableReference;
    use crate::runtime::SharedRuntime;
    use crate::test_support::grpc::{
        INVALID_ARGUMENT, MockPlan, RESET, RESOURCE_EXHAUSTED, StorageMock, UNAVAILABLE,
        ipc_messages,
    };

    /// Four batches of uneven width, so a resume offset that counted responses or batches
    /// instead of rows lands somewhere the mock refuses rather than somewhere plausible.
    const BATCH_ROWS: [i64; 4] = [2, 3, 4, 1];

    fn fixture_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn fixture_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
        let mut next_id = 0;
        BATCH_ROWS
            .iter()
            .map(|rows| {
                let ids = (next_id..next_id + rows).collect::<Vec<_>>();
                next_id += rows;
                let names = ids
                    .iter()
                    .map(|id| (id % 3 != 0).then(|| format!("row-{id}")))
                    .collect::<Vec<_>>();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(names)),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    /// The plan a whole read runs from: the four row-carrying responses, with a stats-only
    /// response spliced in after the first. A response reporting no rows must not move the
    /// resume offset, and BigQuery sends them.
    fn read_plan(schema: &SchemaRef, batches: &[RecordBatch]) -> MockPlan {
        let (read_schema, messages) = ipc_messages(schema, batches);
        let mut read_responses = messages
            .into_iter()
            .zip(BATCH_ROWS)
            .map(|(message, rows)| (rows, message))
            .collect::<Vec<_>>();
        read_responses.insert(1, (0, Vec::new()));
        MockPlan {
            read_schema,
            read_responses,
            ..MockPlan::default()
        }
    }

    /// The client state addressing one scripted server.
    fn clients(mock: &StorageMock) -> Arc<ClientState> {
        ClientState::for_test_at(ServiceEndpointKind::Storage, mock.endpoint())
    }

    /// The table a scripted session reads.
    fn destination() -> TableReference {
        TableReference {
            project_id: "project".to_owned(),
            dataset_id: "dataset".to_owned(),
            table_id: "results".to_owned(),
        }
    }

    /// Opens the reader one scripted session hands out, under `operation` when the test is about
    /// what cancelling it does.
    fn storage_reader(
        runtime: &SharedRuntime,
        clients: &Arc<ClientState>,
        operation: Option<&Operation>,
        options: QueryResultOptions,
        schema: SchemaRef,
    ) -> Box<dyn RecordBatchReader + Send + 'static> {
        match reader(
            runtime,
            clients,
            operation,
            options,
            &JobReference::new("project", None, "job".to_owned()),
            destination(),
            schema,
        )
        .unwrap()
        {
            Opened::Reader(reader) => reader,
            Opened::Fallback(reason) => panic!("the scripted session fell back: {reason:?}"),
        }
    }

    /// The status a read that fell back to REST would report if REST could not read it either.
    fn fallback_status(opened: adbc_core::error::Result<Opened>) -> Status {
        match opened.unwrap() {
            Opened::Fallback(reason) => reason.status,
            Opened::Reader(_) => panic!("expected a fallback to REST"),
        }
    }

    /// A runtime, the fixture schema, and the batches every scripted session here serves.
    struct Fixture {
        runtime: SharedRuntime,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Fixture {
        fn new() -> Self {
            let schema = fixture_schema();
            let batches = fixture_batches(&schema);
            Self {
                runtime: crate::runtime::new_runtime().unwrap(),
                schema,
                batches,
            }
        }

        /// Opens a session on `table` at `clients`, keeping the fallback decision for the test.
        fn open(
            &self,
            clients: &Arc<ClientState>,
            table: TableReference,
        ) -> adbc_core::error::Result<Opened> {
            reader(
                &self.runtime,
                clients,
                None,
                QueryResultOptions::default(),
                &JobReference::new("project", None, "job".to_owned()),
                table,
                self.schema.clone(),
            )
        }

        /// A server scripted with the whole-read plan, after `edit` has adjusted it.
        fn mock(&self, edit: impl FnOnce(&mut MockPlan)) -> StorageMock {
            let mut plan = read_plan(&self.schema, &self.batches);
            edit(&mut plan);
            StorageMock::start(&self.runtime, plan)
        }

        /// Reads one scripted session to completion, returning the batches it delivered.
        fn read(&self, mock: &StorageMock) -> Result<Vec<RecordBatch>, ArrowError> {
            self.read_with(mock, QueryResultOptions::default())
        }

        /// The same, under caller-chosen result options (`unordered_read` enables multi-stream
        /// reads).
        fn read_with(
            &self,
            mock: &StorageMock,
            options: QueryResultOptions,
        ) -> Result<Vec<RecordBatch>, ArrowError> {
            storage_reader(
                &self.runtime,
                &clients(mock),
                None,
                options,
                self.schema.clone(),
            )
            .collect()
        }
    }

    /// The options a parallel read runs under.
    fn unordered() -> QueryResultOptions {
        let mut options = QueryResultOptions::default();
        options.unordered_read = true;
        options
    }

    #[test]
    fn unsupported_session_schemas_fall_back_but_malformed_schemas_fail() {
        let fixture = Fixture::new();
        let dictionary_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        let mut dictionary_ipc = Vec::new();
        arrow_ipc::writer::StreamWriter::try_new(&mut dictionary_ipc, &dictionary_schema)
            .unwrap()
            .finish()
            .unwrap();

        for (encoded, fallback) in [(dictionary_ipc, true), (vec![0xff; 3], false)] {
            let mock = fixture.mock(|plan| plan.read_schema = encoded);
            let result = fixture.open(&clients(&mock), destination());
            if fallback {
                assert_eq!(fallback_status(result), Status::NotImplemented);
            } else {
                let Err(failure) = result else {
                    panic!("malformed IPC must remain an error");
                };
                assert_eq!(failure.status, Status::Internal);
                assert!(
                    failure
                        .message
                        .contains("truncated Arrow IPC message prefix")
                );
            }
            // Both decisions precede reading rows; the completed query remains available to REST.
            assert!(mock.read_offsets().is_empty());
        }
    }

    /// Both RPCs identify the driver and caller, including parallel streams, refused opens and
    /// mid-stream resumes. Observe HTTP/2 headers, not only the SDK's request options.
    #[test]
    fn every_storage_read_request_carries_the_driver_and_caller_user_agent() {
        let fixture = Fixture::new();
        for caller in [None, Some("my-app/1.0")] {
            for (streams, interrupted) in [(1, false), (1, true), (3, false)] {
                let mock = fixture.mock(|plan| {
                    plan.read_stream_count = streams;
                    if interrupted {
                        plan.read_open_refusals = vec![UNAVAILABLE];
                        plan.read_breaks = vec![(0, UNAVAILABLE), (3, UNAVAILABLE)];
                    }
                });
                let mut config = crate::config::DatabaseConfig {
                    authentication_type: crate::BigQueryAuthenticationType::Anonymous,
                    user_agent: caller.map(Arc::from),
                    ..crate::config::DatabaseConfig::default()
                };
                for kind in ServiceEndpointKind::ALL {
                    config.endpoints.set(
                        kind,
                        Some(Arc::from(crate::test_support::UNREACHABLE_ENDPOINT)),
                    );
                }
                config.endpoints.set(
                    ServiceEndpointKind::Storage,
                    Some(Arc::from(mock.endpoint())),
                );
                let clients = ClientState::new(&config);
                let options = if streams > 1 {
                    unordered()
                } else {
                    QueryResultOptions::default()
                };
                let batches = storage_reader(
                    &fixture.runtime,
                    &clients,
                    None,
                    options,
                    fixture.schema.clone(),
                )
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

                assert_eq!(batches.len(), BATCH_ROWS.len() * streams);
                let sessions = mock.session_user_agents();
                let reads = mock.read_user_agents();
                assert_eq!(sessions.len(), 1);
                assert_eq!(reads.len(), if interrupted { 3 } else { streams });
                if interrupted {
                    assert_eq!(
                        mock.read_offsets(),
                        vec![0, 0, BATCH_ROWS[0] + BATCH_ROWS[1]]
                    );
                }
                let expected = match caller {
                    Some(caller) => format!("bigquery-adbc/{} {caller}", env!("CARGO_PKG_VERSION")),
                    None => format!("bigquery-adbc/{}", env!("CARGO_PKG_VERSION")),
                };
                for agent in sessions.iter().chain(&reads) {
                    assert!(
                        agent.starts_with(&expected),
                        "{agent:?} did not start with {expected:?}"
                    );
                }
            }
        }
    }

    /// A destination naming one partition carries a table decorator, which `CreateReadSession`
    /// refuses with `INVALID_ARGUMENT` (verified live), so its rows are read over REST instead.
    #[test]
    fn a_destination_naming_one_partition_is_read_over_rest() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|_| {});
        let table = TableReference {
            table_id: "results$20240101".to_owned(),
            ..destination()
        };
        let result = fixture.open(&clients(&mock), table);
        assert_eq!(fallback_status(result), Status::NotImplemented);
        assert!(mock.read_offsets().is_empty());
    }

    /// `CreateReadSession` answers `INVALID_ARGUMENT` for some tables REST reads fine, such as a
    /// destination with `require_partition_filter` (verified live), so the result is read over
    /// REST. The refusal concerns that table, not the credentials, so it is not cached.
    #[test]
    fn an_invalid_argument_session_refusal_reads_over_rest_without_disabling_storage_read() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| plan.session_refusals = vec![INVALID_ARGUMENT]);
        let clients = clients(&mock);
        let result = fixture.open(&clients, destination());
        assert_eq!(fallback_status(result), Status::InvalidArguments);
        assert!(!clients.storage_read_disabled("project"));
        assert!(mock.read_offsets().is_empty());
    }

    /// An endpoint that takes the connection and never answers the TLS handshake, as a wedged
    /// proxy can. The SDK's deadline lives inside each connection, so it never starts. A listener
    /// that never accepts is enough: the kernel completes the TCP half.
    #[test]
    fn a_session_create_stalled_in_the_tls_handshake_times_out() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let clients = ClientState::for_test_at(ServiceEndpointKind::Storage, &endpoint);
        let runtime = crate::runtime::new_runtime().unwrap();
        let paused = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let job = JobReference::new("project", None, "job".to_owned());
        let table = destination();
        let failure = paused.block_on(async {
            let client = clients.read_client(&runtime, None).await.unwrap();
            let creating = create_read_session(&client, &clients, None, &job, &table, 1);
            tokio::time::timeout(Duration::from_secs(3600), creating)
                .await
                .expect("the stalled call was never bounded")
                .unwrap_err()
        });
        assert_eq!(failure.status, Status::Timeout, "{failure}");
    }

    /// The query has already finished, so one lost `CreateReadSession` must not fail it. The SDK
    /// retries only calls marked idempotent, and the generated call is not.
    #[test]
    fn an_unavailable_session_create_is_retried() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| plan.session_refusals = vec![UNAVAILABLE]);
        assert_eq!(fixture.read(&mock).unwrap(), fixture.batches);
        assert_eq!(mock.session_user_agents().len(), 2);
    }

    /// A resumed stream preserves every row, including across statistics-only responses.
    #[test]
    fn a_broken_read_stream_delivers_the_unbroken_row_sequence() {
        let fixture = Fixture::new();

        let whole = fixture.mock(|_| ());
        let unbroken = fixture.read(&whole).unwrap();
        assert_eq!(unbroken, fixture.batches);
        assert_eq!(whole.read_offsets(), vec![0]);

        // Break after the first three responses: two rows, a stats-only response carrying none,
        // and three more. The resume therefore has to ask for row five.
        for code in [UNAVAILABLE, RESOURCE_EXHAUSTED] {
            let broken = fixture.mock(|plan| plan.read_breaks = vec![(3, code)]);
            assert_eq!(fixture.read(&broken).unwrap(), unbroken);
            assert_eq!(
                broken.read_offsets(),
                vec![0, BATCH_ROWS[0] + BATCH_ROWS[1]]
            );
        }
    }

    /// A stream reset at the transport, as when a proxy drops the connection, carries no gRPC
    /// status at all, and is resumed like an `UNAVAILABLE` one. The reset may discard responses
    /// the client had not read yet, so the resume starts at whichever row it had reached.
    #[test]
    fn a_read_stream_reset_at_the_transport_is_resumed() {
        let fixture = Fixture::new();
        let broken = fixture.mock(|plan| plan.read_breaks = vec![(3, RESET)]);
        assert_eq!(fixture.read(&broken).unwrap(), fixture.batches);
        let offsets = broken.read_offsets();
        // A reset may arrive after only the first data batch; the stats-only response does
        // not advance that two-row offset.
        let resume_offsets = [0, BATCH_ROWS[0], BATCH_ROWS[0] + BATCH_ROWS[1]];
        assert!(
            matches!(offsets.as_slice(), [0, resumed] if resume_offsets.contains(resumed)),
            "{offsets:?}"
        );
    }

    /// A status unrelated to a transient failure is reported, not replayed, whether it ends an
    /// established stream or refuses the first open: no second `ReadRows` is opened.
    #[test]
    fn an_unresumable_status_is_reported_without_reopening() {
        let fixture = Fixture::new();
        let plans: [fn(&mut MockPlan); 2] = [
            |plan| plan.read_breaks = vec![(2, INVALID_ARGUMENT)],
            |plan| plan.read_open_refusals = vec![INVALID_ARGUMENT],
        ];
        for edit in plans {
            let mock = fixture.mock(edit);
            let failure = fixture.read(&mock).unwrap_err().to_string();
            assert!(failure.contains("Storage Read rows"), "{failure}");
            assert_eq!(mock.read_offsets(), vec![0]);
        }
    }

    /// A session granting several streams reads them all in parallel, through the `JoinSet`
    /// branch of `read_streams` (other offline reads take the single-stream path). Each stream
    /// serves the same responses, and rows are compared as a multiset because streams interleave
    /// in any order.
    #[test]
    fn a_session_granting_several_streams_reads_every_one_of_them() {
        const STREAMS: usize = 3;

        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| plan.read_stream_count = STREAMS);

        let delivered = fixture.read_with(&mock, unordered()).unwrap();
        let mut delivered_rows = delivered
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>();
        delivered_rows.sort_unstable();
        let mut expected_rows = fixture
            .batches
            .iter()
            .flat_map(|batch| std::iter::repeat_n(batch.num_rows(), STREAMS))
            .collect::<Vec<_>>();
        expected_rows.sort_unstable();
        assert_eq!(delivered_rows, expected_rows);
        for batch in &delivered {
            assert!(
                fixture.batches.contains(batch),
                "unexpected batch {batch:?}"
            );
        }

        // One call per granted stream, each naming a different one: nothing was reopened, and
        // nothing was read twice.
        let mut calls = mock.read_stream_calls();
        calls.sort();
        calls.dedup();
        assert_eq!(calls.len(), STREAMS);
        assert_eq!(mock.read_offsets(), vec![0; STREAMS]);
    }

    /// A stream delivers nothing without the pipeline's delivery slot.
    ///
    /// The byte budget cannot charge a decoded batch until it exists, so the slot keeps every
    /// stream of a parallel read from holding one at once. This checks that the slot is on the
    /// path; that it is taken before decoding is documented on `ResultSink::delivery_slot`.
    ///
    /// The wire channel holds one buffer, so the second send completes only after the decode has
    /// taken the first. From then on the decode holds a batch it cannot deliver without the slot,
    /// regardless of scheduling.
    #[test]
    fn a_stream_decodes_only_while_it_holds_the_delivery_slot() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let schema = fixture_schema();
        let batches = fixture_batches(&schema);
        let (_, messages) = ipc_messages(&schema, &batches[..2]);
        let (sink, mut delivered) = ResultSink::for_test_reading(schema.clone());
        let plan: Arc<ReshapePlan> = Arc::new(reshape_plans(&schema, &schema).unwrap());

        runtime.block_on(async move {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            sender
                .send((Buffer::from(messages[0].clone()), BATCH_ROWS[0]))
                .await
                .unwrap();

            let slot = sink.delivery_slot().await.unwrap().unwrap();
            let decoding =
                tokio::spawn(async move { decode_batches(receiver, schema, &plan, &sink).await });

            // The rendezvous: this returns only once the decode has dequeued the first buffer.
            sender
                .send((Buffer::from(messages[1].clone()), BATCH_ROWS[1]))
                .await
                .unwrap();
            drop(sender);

            // The decode is inside the loop holding a batch's worth of wire bytes and no slot, so
            // nothing can reach the reader until the slot is released. A decode loop that never
            // took the slot would deliver both batches and end here.
            assert!(
                tokio::time::timeout(Duration::from_millis(250), delivered.next())
                    .await
                    .is_err(),
                "a batch was decoded without the pipeline's delivery slot"
            );

            drop(slot);
            tokio::time::timeout(Duration::from_secs(5), decoding)
                .await
                .expect("the decode did not resume once the slot was free")
                .unwrap()
                .unwrap();
            assert_eq!(delivered.next().await.unwrap(), batches[0]);
            assert_eq!(delivered.next().await.unwrap(), batches[1]);
        });
    }

    /// A transient failure on the call that opens `ReadRows` is retried, like every later one.
    ///
    /// `execute_server_streaming` does not run the client's retry loop, and by the time the open
    /// runs, `spawn_result_reader` has handed the reader to the caller and REST fallback is gone.
    ///
    /// The mock refuses the first call before any response headers, as a failed open looks on the
    /// wire (unlike `read_breaks`, which ends an established stream).
    #[test]
    fn a_refused_first_read_rows_open_is_retried_rather_than_failing_the_read() {
        let fixture = Fixture::new();
        for code in [UNAVAILABLE, RESOURCE_EXHAUSTED] {
            let mock = fixture.mock(|plan| plan.read_open_refusals = vec![code]);
            assert_eq!(fixture.read(&mock).unwrap(), fixture.batches);
            assert_eq!(
                mock.read_offsets(),
                vec![0, 0],
                "the retried open did not restart at the first row"
            );
        }
    }

    /// The resume budget is finite. A stream that fails again every time it is reopened, with no
    /// row in between to earn a fresh budget, ends the read after a bounded number of attempts
    /// instead of reconnecting forever.
    #[test]
    fn consecutive_read_failures_exhaust_a_finite_resume_budget() {
        let fixture = Fixture::new();
        let attempts = usize::try_from(READ_ROWS_RESUME_ATTEMPTS).unwrap();
        for code in [UNAVAILABLE, RESOURCE_EXHAUSTED] {
            let mock = fixture.mock(|plan| {
                // More breaks than the budget allows, each before any rows arrive.
                plan.read_breaks = vec![(0, code); attempts + 3];
            });
            let failure = fixture.read(&mock).unwrap_err();
            assert!(
                failure.to_string().contains("Storage Read rows"),
                "{failure}"
            );
            assert_eq!(
                mock.read_offsets(),
                vec![0; attempts + 1],
                "the read did not spend exactly its resume budget"
            );
        }
    }

    /// A response that reports rows and carries no Arrow batch is a short read, not a stats-only
    /// response.
    ///
    /// The resume offset already counts the reported rows, so skipping the response would drop
    /// them and report the rest as complete. Only a response reporting no rows may be skipped;
    /// BigQuery sends those for schema and statistics.
    #[test]
    fn a_response_reporting_rows_without_a_batch_fails_the_read() {
        let fixture = Fixture::new();
        // The spliced-in stats-only response, now claiming rows it carries nothing for.
        let mock = fixture.mock(|plan| plan.read_responses[1] = (3, Vec::new()));

        assert_failure(
            adbc_failure(&fixture.read(&mock).unwrap_err()),
            Status::InvalidData,
            &["carried no Arrow record batch", "3 rows at offset 2"],
        );
        assert_eq!(
            mock.read_offsets(),
            vec![0],
            "a self-contradicting response is not something to resume from"
        );
    }

    #[test]
    fn invalid_response_row_counts_fail_before_delivering_the_batch() {
        let fixture = Fixture::new();
        for row_count in [-1, 0, 1, 3, i64::MAX] {
            let mock = fixture.mock(|plan| {
                plan.read_responses.truncate(1);
                plan.read_responses[0].0 = row_count;
            });
            let mut reader = storage_reader(
                &fixture.runtime,
                &clients(&mock),
                None,
                QueryResultOptions::default(),
                fixture.schema.clone(),
            );
            let failure = reader.next().expect("invalid count must fail").unwrap_err();
            assert_failure(
                adbc_failure(&failure),
                Status::InvalidData,
                &[&format!("{row_count} rows")],
            );
            assert_eq!(
                mock.read_offsets(),
                vec![0],
                "invalid data must not be retried"
            );
            assert!(reader.next().is_none());
        }
    }

    #[test]
    fn overflowing_resume_offset_fails_instead_of_saturating() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| {
            plan.read_responses.remove(1);
            plan.read_responses.truncate(2);
            plan.read_responses[0].0 = i64::MAX;
        });
        fixture.runtime.block_on(async {
            let client = clients(&mock)
                .read_client(&fixture.runtime, None)
                .await
                .unwrap();
            let (sender, _receiver) = tokio::sync::mpsc::channel(2);
            let stream = "projects/p/locations/l/sessions/mock/streams/0";
            let request = client.read_rows().set_read_stream(stream);
            let failure = super::receive_rows(&request, stream, sender, &ResultSink::for_test())
                .await
                .unwrap_err();
            assert!(
                failure.to_string().contains("invalid row count"),
                "{failure}"
            );
        });
        assert_eq!(mock.read_offsets(), vec![0]);
    }

    /// A read cut short by cancellation reports the cancellation, never a clean end of stream.
    ///
    /// adbc.h requires `ECANCELED`; its exemption for a result "buffered in memory already" covers
    /// a complete result, not a truncated one. Stages in this module stop on cancellation by
    /// returning `Ok(())` so buffered batches stay readable, and the statement's
    /// `operation_reader` turns the truncated tail into an error. The test runs that whole path
    /// over a scripted session.
    #[test]
    fn a_cancelled_read_reports_the_cancellation_rather_than_a_short_result() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|_| ());
        let clients = clients(&mock);
        let cancellation = Cancellation::new(fixture.runtime.clone(), clients.clone(), "statement");
        let operation = cancellation.begin();
        let inner = storage_reader(
            &fixture.runtime,
            &clients,
            Some(&operation),
            QueryResultOptions::default(),
            fixture.schema.clone(),
        );
        let mut reader = operation_reader(inner, operation);

        let first = reader
            .next()
            .expect("the scripted session delivers four batches")
            .unwrap();
        assert_eq!(first, fixture.batches[0]);
        OperationCancelHandle(cancellation).try_cancel().unwrap();

        let failure = reader
            .next()
            .expect("a cancelled read must report the cancellation, not end")
            .unwrap_err();
        assert_failure(adbc_failure(&failure), Status::Cancelled, &[]);
    }
}
