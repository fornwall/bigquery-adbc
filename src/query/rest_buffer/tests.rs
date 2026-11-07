#[test]
fn delivers_resident_rows_while_the_next_page_is_waiting() {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde_json::json;

    use super::{QueryPager, QueryResultOptions, buffer_rest_query_results};
    use crate::pipeline::{BATCH_SIZE, ResultSink};
    use crate::query::response::QueryPage;
    use crate::rest::jobs::{JobClient, JobReference};
    use crate::rows::{RestRow, RestValue};
    use crate::runtime::new_runtime;
    use crate::test_support::http::capture_requests_gated;

    for concurrency in [1, 10] {
        for row_count in [1, BATCH_SIZE, 10_000] {
            let (requested, request_received) = tokio::sync::oneshot::channel();
            let (release, wait_for_release) = std::sync::mpsc::channel();
            let (endpoint, captured) = capture_requests_gated(
                vec![(
                    200,
                    json!({"jobComplete":true,"rows":[{"f":[{"v":"10000"}]}]}),
                )],
                // No wall-clock bound on the reader: a pager that withheld resident rows until
                // this page arrived would never send the release, and the gate's own timeout
                // then drops the page whose row the test waits for below.
                move || {
                    requested.send(()).unwrap();
                    wait_for_release
                        .recv_timeout(Duration::from_secs(60))
                        .is_ok()
                },
            );
            let runtime = new_runtime().unwrap();
            let client = JobClient::for_test(&endpoint);
            let pager = QueryPager::new(
                client,
                Some(JobReference::new("project", Some("EU"), "job".to_owned())),
                None,
                QueryPage {
                    rows: (0..row_count)
                        .map(|index| RestRow::new(vec![RestValue::String(index.to_string())]))
                        .collect(),
                    page_token: Some("next".to_owned()),
                    encoded_len: 0,
                },
            );
            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
            let (sink, mut batches) = ResultSink::for_test_reading(schema);
            let mut options = QueryResultOptions::default();
            options
                .set_prefetch_concurrency("prefetch", concurrency)
                .unwrap();
            let producer = runtime.spawn(buffer_rest_query_results(pager, sink, options, None));
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), request_received)
                    .await
                    .unwrap()
                    .unwrap();
                // The server cannot answer until every resident row has reached the reader.
                // This detects both cross-page batch filling and conversion head-of-line waits.
                let mut next = 0;
                while next < row_count {
                    let batch = batches.next().await.unwrap();
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for &value in values.values() {
                        assert_eq!(value, i64::try_from(next).unwrap());
                        next += 1;
                    }
                }
                release.send(()).unwrap();
                assert_eq!(next, row_count);
                let last = batches.next().await.unwrap();
                assert_eq!(last.num_rows(), 1);
                assert_eq!(
                    last.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    10_000
                );
                assert!(batches.next().await.is_none());
                producer.await.unwrap().unwrap();
            });
            assert_eq!(captured.join().unwrap().len(), 1);
        }
    }
}

/// `totalRows` only sizes the first batch; a short or stale count must not end the result early.
#[test]
fn a_short_total_row_count_does_not_truncate_the_result() {
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::{QueryPager, QueryResultOptions, buffer_rest_query_results};
    use crate::pipeline::ResultSink;
    use crate::query::response::QueryPage;
    use crate::rest::jobs::JobClient;
    use crate::rows::{RestRow, RestValue};
    use crate::runtime::new_runtime;

    let runtime = new_runtime().unwrap();
    // The single resident page carries no token, so no request is ever sent here.
    let client = JobClient::for_test(crate::test_support::UNREACHABLE_ENDPOINT);
    let pager = QueryPager::new(
        client,
        None,
        None,
        QueryPage {
            rows: (0..3)
                .map(|index| RestRow::new(vec![RestValue::String(index.to_string())]))
                .collect(),
            page_token: None,
            encoded_len: 0,
        },
    );
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let (sink, mut batches) = ResultSink::for_test_reading(schema);
    let producer = runtime.spawn(buffer_rest_query_results(
        pager,
        sink,
        QueryResultOptions::default(),
        Some(1),
    ));
    runtime.block_on(async {
        let mut values: Vec<i64> = Vec::new();
        while let Some(batch) = batches.next().await {
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
        producer.await.unwrap().unwrap();
        assert_eq!(values, [0_i64, 1, 2]);
    });
}

#[test]
fn dropping_conversion_tasks_stops_started_blocking_work_cooperatively() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let cancellation = crate::pipeline::ResultPipelineCancellation::new(None);
    let conversion_cancellation = cancellation.clone();
    let (started_sender, started_receiver) = std::sync::mpsc::channel();
    let (stopped_sender, stopped_receiver) = std::sync::mpsc::channel();
    let mut conversions = super::ConversionTasks::new(cancellation);
    conversions.push(
        runtime.spawn_blocking(move || {
            started_sender.send(()).unwrap();
            while !conversion_cancellation.is_cancelled() {
                std::thread::yield_now();
            }
            stopped_sender.send(()).unwrap();
            Ok(None)
        }),
        0,
    );
    started_receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    drop(conversions);

    stopped_receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
}

#[test]
fn operation_cancellation_stops_started_result_conversion() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let operation_cancellation = crate::cancellation::Cancellation::new(
        runtime.clone(),
        crate::clients::ClientState::for_test(),
        "statement",
    );
    let operation = operation_cancellation.begin();
    let cancellation = crate::pipeline::ResultPipelineCancellation::new(Some(operation));
    let conversion_cancellation = cancellation.clone();
    let (started_sender, started_receiver) = std::sync::mpsc::channel();
    let (stopped_sender, stopped_receiver) = std::sync::mpsc::channel();
    let mut conversions = super::ConversionTasks::new(cancellation);
    conversions.push(
        runtime.spawn_blocking(move || {
            started_sender.send(()).unwrap();
            while !conversion_cancellation.is_cancelled() {
                std::thread::yield_now();
            }
            stopped_sender.send(()).unwrap();
            Ok(None)
        }),
        0,
    );
    started_receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    adbc_core::CancelHandle::try_cancel(&crate::cancellation::OperationCancelHandle(
        operation_cancellation,
    ))
    .unwrap();

    stopped_receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(
        runtime
            .block_on(conversions.next())
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none()
    );
}

/// One conversion always proceeds; beyond it, neither the prefetch width nor the byte window may
/// be exceeded, and delivering a conversion returns its bytes to the window.
#[test]
fn the_conversion_window_bounds_in_flight_rows_by_bytes_as_well_as_count() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let spawn = || runtime.spawn(async { Ok(None) });
    let mut conversions =
        super::ConversionTasks::new(crate::pipeline::ResultPipelineCancellation::new(None));
    assert!(conversions.may_start(1));
    conversions.push(spawn(), 2 * super::CONVERSION_WINDOW_BYTES);
    assert!(
        !conversions.may_start(10),
        "a full byte window admitted a conversion"
    );

    runtime
        .block_on(conversions.next())
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(conversions.may_start(1));
    conversions.push(spawn(), 1);
    assert!(conversions.may_start(2));
    assert!(!conversions.may_start(1), "the prefetch width was exceeded");
    conversions.push(spawn(), super::CONVERSION_WINDOW_BYTES - 1);
    assert!(!conversions.may_start(10));
    runtime
        .block_on(conversions.next())
        .unwrap()
        .unwrap()
        .unwrap();
    runtime
        .block_on(conversions.next())
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(conversions.is_empty());
    assert_eq!(conversions.bytes, 0);
}

#[test]
fn a_skewed_page_charges_the_wide_batch_its_retained_allocation() {
    use super::conversion_input_bytes;
    use crate::pipeline::{BATCH_SIZE, ResultSink};
    use crate::query::pager::QueryPager;
    use crate::query::response::QueryResponse;
    use crate::rest::jobs::JobClient;
    use crate::rows::{RestRow, RestValue};

    const PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
    const ROWS: usize = 16 * BATCH_SIZE;
    let body = format!(
        r#"{{"rows":[{{"f":[{{"v":"{}"}}]}}{}]}}"#,
        "x".repeat(PAYLOAD_BYTES),
        r#",{"f":[{"v":null}]}"#.repeat(ROWS - 1),
    );
    let page = QueryResponse::decode(body.as_bytes(), "skewed fixture")
        .unwrap()
        .page();
    let previous_charge = (page.encoded_len / page.rows.len()) * BATCH_SIZE;
    let runtime = crate::runtime::new_runtime().unwrap();
    let mut pager = QueryPager::new(
        JobClient::for_test(crate::test_support::UNREACHABLE_ENDPOINT),
        None,
        None,
        page,
    );
    let (batch, exhausted) = runtime
        .block_on(pager.next_batch(&ResultSink::for_test(), BATCH_SIZE))
        .unwrap()
        .unwrap();
    assert!(!exhausted);
    // The first row's initially unsized cell vector has capacity four; every later row learns
    // its single-cell width. Inline nulls allocate nothing outside those vectors.
    let expected = PAYLOAD_BYTES
        + (BATCH_SIZE + 3) * size_of::<RestValue>()
        + BATCH_SIZE * size_of::<RestRow>();
    let charged = conversion_input_bytes(&batch);
    assert_eq!(charged, expected);
    assert!(previous_charge < PAYLOAD_BYTES / 8);

    let mut conversions =
        super::ConversionTasks::new(crate::pipeline::ResultPipelineCancellation::new(None));
    for _ in 0..8 {
        assert!(conversions.may_start(10));
        conversions.push(runtime.spawn(async { Ok(None) }), charged);
    }
    assert!(!conversions.may_start(10));
    assert!(8 * previous_charge < super::CONVERSION_WINDOW_BYTES);
}

/// A conversion that panicked reaches the reader as the driver defect it is, not as a bare
/// `JoinError` without a status.
#[test]
fn a_panicked_conversion_is_reported_as_an_internal_driver_defect() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (sink, _batches) = crate::pipeline::ResultSink::for_test_reading(std::sync::Arc::new(
        arrow_schema::Schema::empty(),
    ));
    let mut conversions = super::ConversionTasks::new(sink.cancellation.clone());
    conversions.push(
        runtime.spawn_blocking(|| -> super::ConversionResult { panic!("conversion exploded") }),
        0,
    );

    let failure = runtime
        .block_on(async { super::deliver_conversion(conversions.next().await, &sink).await })
        .unwrap_err();
    let arrow_schema::ArrowError::ExternalError(source) = failure else {
        panic!("expected an embedded driver error, got {failure:?}");
    };
    let error = source.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(error.status, adbc_core::error::Status::Internal);
    assert!(
        error.message.contains("conversion exploded")
            && error.message.ends_with(
                "; this is a driver defect, report it at \
                 https://github.com/fornwall/bigquery-adbc/issues"
            ),
        "{}",
        error.message
    );
}

/// A reader pausing past the HTTP request timeout while a page request is in flight must not cost
/// that request: an unpolled request still runs out its timeout and is retried.
///
/// Time is paused. Tokio does not advance it while a blocking task runs, so each real-time wait
/// runs on the blocking pool, and a paused `sleep` returns only once the runtime is idle, here
/// with the producer parked on delivery.
#[test]
fn a_reader_pausing_past_the_request_timeout_does_not_repeat_a_page_request() {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde_json::json;

    use super::{QueryPager, QueryResultOptions, buffer_rest_query_results};
    use crate::pipeline::{QUERY_RESULT_BUFFER_SIZE, ResultSink};
    use crate::query::response::QueryPage;
    use crate::rest::jobs::{JobClient, JobReference};
    use crate::rows::{RestRow, RestValue};
    use crate::test_support::http::{
        accept, mock_listener, read_captured_request, retire, write_response,
    };

    const EVENT_TIMEOUT: Duration = Duration::from_secs(30);
    let (listener, endpoint) = mock_listener();
    let (stop, stopped) = mpsc::channel::<()>();
    let (received_sender, received) = mpsc::channel();
    let (answer, answered) = mpsc::channel::<()>();
    let (hung_up_sender, hung_up) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut served = 0;
        while let Some(mut stream) = accept(&listener, Some(&stopped)) {
            if read_captured_request(&mut stream).is_none() {
                continue;
            }
            served += 1;
            if served == 1 {
                let _ = received_sender.send(());
                let _ = answered.recv();
            }
            write_response(
                &mut stream,
                200,
                &json!({"jobComplete": true, "rows": [{"f": [{"v": "2"}]}]}),
            );
            if served == 1 {
                // The client closes a `Connection: close` exchange once it has the response.
                let _ = std::io::Read::read(&mut stream, &mut [0; 1]);
                let _ = hung_up_sender.send(());
            }
        }
        retire(listener);
        served
    });

    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let pager = QueryPager::new(
        JobClient::for_test(&endpoint).with_flat_delays(Duration::ZERO),
        Some(JobReference::new("project", Some("EU"), "job".to_owned())),
        None,
        QueryPage {
            rows: vec![RestRow::new(vec![RestValue::String("1".to_owned())])],
            page_token: Some("next".to_owned()),
            encoded_len: 0,
        },
    );
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let filler =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0]))]).unwrap();
    let (sink, mut batches) = ResultSink::for_test_reading(schema);
    let values = runtime.block_on(async move {
        // Fill the reader's queue, so the first converted batch waits for the reader.
        for _ in 0..QUERY_RESULT_BUFFER_SIZE {
            assert!(sink.deliver(filler.clone()).await);
        }
        let producer = tokio::spawn(buffer_rest_query_results(
            pager,
            sink,
            QueryResultOptions::default(),
            None,
        ));
        tokio::task::spawn_blocking(move || received.recv_timeout(EVENT_TIMEOUT))
            .await
            .unwrap()
            .expect("the page request never reached the server");
        // The producer is now parked delivering the first page's batch.
        tokio::time::sleep(Duration::from_millis(1)).await;
        answer.send(()).unwrap();
        let _ = tokio::task::spawn_blocking(move || hung_up.recv_timeout(EVENT_TIMEOUT)).await;
        tokio::time::sleep(Duration::from_millis(1)).await;

        // The reader resumes long after the request timeout. A retry needs the socket again, so
        // the clock stays where it is until the queue is drained.
        let (resume, resumed) = mpsc::channel::<()>();
        let hold = tokio::task::spawn_blocking(move || resumed.recv());
        tokio::time::advance(Duration::from_secs(61)).await;
        let mut values = Vec::new();
        while let Some(batch) = batches.next().await {
            let column = batch.column(0).as_any().downcast_ref::<Int64Array>();
            values.extend(column.unwrap().values().iter().copied());
        }
        producer.await.unwrap().unwrap();
        resume.send(()).unwrap();
        hold.await.unwrap().unwrap();
        values
    });
    assert_eq!(values.len(), QUERY_RESULT_BUFFER_SIZE + 2);
    assert_eq!(values[QUERY_RESULT_BUFFER_SIZE..], [1, 2]);
    stop.send(()).unwrap();
    assert_eq!(server.join().unwrap(), 1, "the page was requested again");
}
