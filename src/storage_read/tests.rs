use super::*;

/// Allocate responses only when polled, as the SDK materializes each protobuf message.
#[derive(Debug)]
struct LazyRead {
    responses: Arc<std::sync::atomic::AtomicUsize>,
    payload_bytes: usize,
    stalled: bool,
}

impl google_cloud_bigquery::stub::Read for LazyRead {
    async fn read_rows(
        &self,
        request: google_cloud_bigquery::model::ReadRowsRequest,
        _options: google_cloud_gax::options::RequestOptions,
    ) -> std::result::Result<ResponseStream<ReadRowsResponse>, GoogleError> {
        assert_eq!(request.offset, 0);
        Ok(ResponseStream::from_stream(Self {
            responses: self.responses.clone(),
            payload_bytes: self.payload_bytes,
            stalled: self.stalled,
        }))
    }
}

impl futures_core::Stream for LazyRead {
    type Item = std::result::Result<ReadRowsResponse, GoogleError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.responses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.stalled {
            return std::task::Poll::Pending;
        }
        std::task::Poll::Ready(Some(Ok(ReadRowsResponse::new()
            .set_row_count(1)
            .set_arrow_record_batch(
                google_cloud_bigquery::model::ArrowRecordBatch::new()
                    .set_serialized_record_batch(vec![0_u8; self.payload_bytes]),
            ))))
    }
}

#[test]
fn a_full_wire_queue_does_not_materialize_another_response() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    const PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        for cancel in [false, true] {
            let responses = Arc::new(AtomicUsize::new(0));
            let client = Read::from_stub(LazyRead {
                responses: responses.clone(),
                payload_bytes: PAYLOAD_BYTES,
                stalled: false,
            });
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let sink = ResultSink::for_test();
            let request = client.read_rows().set_read_stream("stream");
            let mut receiving = std::pin::pin!(receive_rows(&request, "stream", sender, &sink));
            let mut context = Context::from_waker(Waker::noop());

            assert!(receiving.as_mut().poll(&mut context).is_pending());
            assert_eq!(receiver.len(), 1);
            assert_eq!(responses.load(Ordering::Relaxed), 1);
            let (payload, rows) = receiver.try_recv().unwrap();
            assert_eq!((payload.len(), rows), (PAYLOAD_BYTES, 1));
            drop(payload);

            assert!(receiving.as_mut().poll(&mut context).is_pending());
            assert_eq!(receiver.len(), 1);
            assert_eq!(responses.load(Ordering::Relaxed), 2);

            if cancel {
                sink.cancellation.cancel_local();
            } else {
                receiver.close();
            }
            assert!(matches!(
                receiving.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(responses.load(Ordering::Relaxed), 2);
        }
    });
}

#[test]
fn a_stopped_decoder_releases_a_pending_wire_response() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        for cancel in [false, true] {
            let responses = Arc::new(AtomicUsize::new(0));
            let client = Read::from_stub(LazyRead {
                responses: responses.clone(),
                payload_bytes: 0,
                stalled: true,
            });
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let sink = ResultSink::for_test();
            let request = client.read_rows().set_read_stream("stream");
            let mut receiving = std::pin::pin!(receive_rows(&request, "stream", sender, &sink));
            let mut context = Context::from_waker(Waker::noop());
            assert!(receiving.as_mut().poll(&mut context).is_pending());
            assert_eq!(responses.load(Ordering::Relaxed), 1);
            assert!(receiver.is_empty());

            if cancel {
                sink.cancellation.cancel_local();
            } else {
                receiver.close();
            }
            assert!(matches!(
                receiving.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(responses.load(Ordering::Relaxed), 1);
        }
    });
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
    assert!(matches!(
        refusal(Code::PermissionDenied),
        Some(SessionRefusal::Permanent)
    ));
    assert!(matches!(
        refusal(Code::Unimplemented),
        Some(SessionRefusal::Permanent)
    ));
    assert!(matches!(
        refusal(Code::ResourceExhausted),
        Some(SessionRefusal::Transient)
    ));
    assert!(matches!(
        refusal(Code::InvalidArgument),
        Some(SessionRefusal::Transient)
    ));

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
            read_rows_is_resumable(&service_failure(code)),
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
            !read_rows_is_resumable(&service_failure(code)),
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
        Opened, READ_ROWS_RESUME_ATTEMPTS, ReshapePlan, decode_batches, reader, reshape_plans,
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

    /// Opens the reader one scripted session hands out, under `operation` when the test is about
    /// what cancelling it does.
    fn storage_reader(
        runtime: &SharedRuntime,
        clients: &Arc<ClientState>,
        operation: Option<&Operation>,
        options: QueryResultOptions,
        schema: SchemaRef,
    ) -> Box<dyn RecordBatchReader + Send + 'static> {
        let destination = TableReference {
            project_id: "project".to_owned(),
            dataset_id: "dataset".to_owned(),
            table_id: "results".to_owned(),
        };
        match reader(
            runtime,
            clients,
            operation,
            options,
            &JobReference::new("project", None, "job".to_owned()),
            destination,
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
            let result = reader(
                &fixture.runtime,
                &clients(&mock),
                None,
                QueryResultOptions::default(),
                &JobReference::new("project", None, "job".to_owned()),
                TableReference {
                    project_id: "project".to_owned(),
                    dataset_id: "dataset".to_owned(),
                    table_id: "results".to_owned(),
                },
                fixture.schema.clone(),
            );
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
        let result = reader(
            &fixture.runtime,
            &clients(&mock),
            None,
            QueryResultOptions::default(),
            &JobReference::new("project", None, "job".to_owned()),
            TableReference {
                project_id: "project".to_owned(),
                dataset_id: "dataset".to_owned(),
                table_id: "results$20240101".to_owned(),
            },
            fixture.schema.clone(),
        );
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
        let result = reader(
            &fixture.runtime,
            &clients,
            None,
            QueryResultOptions::default(),
            &JobReference::new("project", None, "job".to_owned()),
            TableReference {
                project_id: "project".to_owned(),
                dataset_id: "dataset".to_owned(),
                table_id: "results".to_owned(),
            },
            fixture.schema.clone(),
        );
        assert_eq!(fallback_status(result), Status::InvalidArguments);
        assert!(!clients.storage_read_disabled("project"));
        assert!(mock.read_offsets().is_empty());
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

    /// A status that does not say the stream ended for a reason unrelated to the request is
    /// reported, not replayed: the read fails and no second `ReadRows` is opened.
    #[test]
    fn a_read_stream_ended_by_an_unresumable_status_is_not_reopened() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| plan.read_breaks = vec![(2, INVALID_ARGUMENT)]);

        let failure = fixture.read(&mock).unwrap_err();
        assert!(
            failure.to_string().contains("Storage Read rows"),
            "{failure}"
        );
        assert_eq!(mock.read_offsets(), vec![0]);
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

    /// One stream of a parallel read failing unresumably fails the whole read once and stops the
    /// others. The other streams break resumably, so without cancellation each would open a second
    /// `ReadRows`.
    #[test]
    fn one_stream_failing_unresumably_fails_the_parallel_read_and_stops_the_rest() {
        const STREAMS: usize = 3;

        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| {
            plan.read_stream_count = STREAMS;
            plan.read_stream_breaks = vec![
                (0, usize::MAX, UNAVAILABLE),
                (1, 1, INVALID_ARGUMENT),
                (2, usize::MAX, UNAVAILABLE),
            ];
        });

        let failure = fixture.read_with(&mock, unordered()).unwrap_err();
        // The scripted status itself, not a `JoinError` from the abort and not a cancellation:
        // the failing stream's own error is what the caller has to be told about.
        let failure = failure.to_string();
        assert!(failure.contains("Storage Read rows"), "{failure}");
        assert!(failure.contains("INVALID_ARGUMENT"), "{failure}");

        // The failing stream is not retried (`INVALID_ARGUMENT` is not resumable), and the two
        // resumable ones were aborted before they could reopen, so no stream appears twice.
        let calls = mock.read_stream_calls();
        let mut distinct = calls.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            calls.len(),
            "a stream was reopened: {calls:?}"
        );
        assert!(calls.len() <= STREAMS, "{calls:?}");
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

    /// An open refused for a reason a second attempt cannot improve on is reported, not retried:
    /// the same classification the mid-stream resumes use, applied to the first call.
    #[test]
    fn a_first_read_rows_open_refused_unresumably_is_reported_without_a_retry() {
        let fixture = Fixture::new();
        let mock = fixture.mock(|plan| plan.read_open_refusals = vec![INVALID_ARGUMENT]);

        let failure = fixture.read(&mock).unwrap_err();
        assert!(
            failure.to_string().contains("Storage Read rows"),
            "{failure}"
        );
        assert_eq!(mock.read_offsets(), vec![0]);
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

        let failure = fixture.read(&mock).unwrap_err();
        let ArrowError::ExternalError(source) = &failure else {
            panic!("expected an ADBC error: {failure}");
        };
        let source = source
            .downcast_ref::<adbc_core::error::Error>()
            .expect("expected an ADBC error");
        assert_eq!(source.status, Status::InvalidData, "{source}");
        assert!(
            source.message.contains("carried no Arrow record batch")
                && source.message.contains("3 rows at offset 2"),
            "{source}"
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
            let ArrowError::ExternalError(source) = &failure else {
                panic!("expected an ADBC error: {failure}");
            };
            let source = source.downcast_ref::<adbc_core::error::Error>().unwrap();
            assert_eq!(source.status, Status::InvalidData, "{source}");
            assert!(
                source.message.contains(&format!("{row_count} rows")),
                "{source}"
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
            let client = clients(&mock).read_client(&fixture.runtime).await.unwrap();
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
        let ArrowError::ExternalError(source) = &failure else {
            panic!("expected an ADBC cancellation: {failure}");
        };
        assert_eq!(
            source
                .downcast_ref::<adbc_core::error::Error>()
                .expect("expected an ADBC cancellation")
                .status,
            Status::Cancelled,
            "{source}"
        );
    }
}
