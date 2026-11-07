//! Cancellation of running metadata walks, query jobs and bulk ingests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::{Error, Status};
use adbc_core::options::{ObjectDepth, OptionStatement};
use adbc_core::{CancelHandle, Connection, Statement};
use adbc_driver_bigquery::{BigQueryConnection, BigQueryStatementOption};
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use crate::storage_read::storage_read_row_sequence;
use crate::support::{
    connect, count_rows, outcome, prepare_query, project, read_all, read_single_i64,
    scratch_fixture, scratch_table, set_option, str_at,
};

/// Runs `execute` on a scoped thread, cancels it in flight, and checks for `Status::Cancelled`.
/// `execute` should drain its result with [`outcome`] so stream errors count.
///
/// `try_cancel` returns `InvalidState` both before the operation registers and after it finishes;
/// `started` and `finished` tell a startup race from an early finish. `in_flight` runs once
/// `execute` has started and must block until the operation is observably running, because
/// cancelling before submission tests nothing remote. Pass a no-op if nothing is observable.
///
/// Returns `false` if the operation finished before cancellation could be delivered.
#[must_use]
fn assert_cancels(
    what: &str,
    cancellation: &dyn CancelHandle,
    in_flight: impl FnOnce(),
    execute: impl FnOnce() -> Result<(), Error> + Send,
) -> bool {
    let started = AtomicBool::new(false);
    let finished = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let execution = scope.spawn(|| {
            started.store(true, Ordering::Release);
            let result = execute();
            finished.store(true, Ordering::Release);
            result
        });

        let mut in_flight = Some(in_flight);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if !started.load(Ordering::Acquire) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the BigQuery {what} never started"
                );
                std::thread::yield_now();
                continue;
            }
            if let Some(in_flight) = in_flight.take() {
                in_flight();
            }
            match cancellation.try_cancel() {
                Ok(()) => break,
                Err(error) if error.status == Status::InvalidState => {
                    if finished.load(Ordering::Acquire) {
                        if let Err(error) = execution.join().unwrap() {
                            panic!(
                                "the BigQuery {what} refused the cancellation as finished, yet \
                                 failed: {error}"
                            );
                        }
                        #[expect(
                            clippy::print_stderr,
                            reason = "a skipped assertion has to say so, and the test harness \
                                      offers no other channel"
                        )]
                        {
                            eprintln!(
                                "skipping: the BigQuery {what} completed before the \
                                 cancellation could be delivered"
                            );
                        }
                        return false;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the BigQuery {what} refused the cancellation for 30s while still running"
                    );
                    std::thread::yield_now();
                }
                Err(error) => panic!("failed to cancel BigQuery {what}: {error}"),
            }
        }
        let error = execution.join().unwrap().err().unwrap_or_else(|| {
            panic!("the BigQuery {what} accepted the cancellation, yet ran to completion")
        });
        assert_eq!(
            error.status,
            Status::Cancelled,
            "the BigQuery {what}: {error}"
        );
        true
    })
}

/// `get_objects` runs REST calls but no BigQuery job, so only the local result is checked.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn cancels_running_connection_metadata() {
    /// Walks that may finish before cancellation lands. There is no running state to wait for,
    /// so a small project can win the race; losing every attempt fails the test.
    const ATTEMPTS: usize = 10;

    for attempt in 1..=ATTEMPTS {
        let connection = connect(project());
        let cancellation = connection.get_cancel_handle();

        let cancelled = assert_cancels(
            "connection",
            cancellation.as_ref(),
            || (),
            || outcome(connection.get_objects(ObjectDepth::All, None, None, None, None, None)),
        );
        if cancelled {
            return;
        }
        assert!(
            attempt < ATTEMPTS,
            "every one of the {ATTEMPTS} connection metadata walks finished before its \
             cancellation could be delivered, so nothing about cancelling one was asserted"
        );
    }
}

/// Label key for finding the cancelled job in `INFORMATION_SCHEMA`; a cancelled execution returns
/// no result to carry its job ID.
const CANCEL_LABEL_KEY: &str = "bigquery_adbc_test";

/// A long-running query that scans no bytes, so on-demand pricing bills nothing: it counts the
/// 90 billion rows of a cross join over two generated arrays. A 400 million row join was already
/// `DONE` at the first poll.
pub(crate) const CANCELLABLE_QUERY: &str = "SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 300000)) AS a \
                                 CROSS JOIN UNNEST(GENERATE_ARRAY(1, 300000)) AS b";

/// The state and error of the query job labelled `label`, or `None` while BigQuery reports no
/// such job yet.
fn labelled_job(connection: &mut BigQueryConnection, label: &str) -> Option<(String, String)> {
    let sql = format!(
        "SELECT state, IFNULL(error_result.reason, '') AS reason \
         FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
         WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) \
           AND EXISTS (SELECT 1 FROM UNNEST(labels) AS l \
                       WHERE l.key = '{CANCEL_LABEL_KEY}' AND l.value = '{label}')"
    );
    let mut statement = prepare_query(connection, &sql);
    let (_, batches) = read_all(&mut statement);
    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
    Some((str_at(batch, 0).to_owned(), str_at(batch, 1).to_owned()))
}

/// Polls `INFORMATION_SCHEMA` until the labelled job's state satisfies `accept`. Both edges lag:
/// a new job takes a moment to appear, and `jobs.cancel` is a request the service acts on later.
fn await_labelled_job(
    connection: &mut BigQueryConnection,
    label: &str,
    what: &str,
    accept: impl Fn(&str) -> bool,
) -> (String, String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let seen = labelled_job(connection, label);
        if let Some((state, reason)) = seen.clone()
            && accept(&state)
        {
            return (state, reason);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the labelled BigQuery job never {what}; it was {}",
            seen.map_or_else(
                || "never reported at all".to_owned(),
                |(state, _)| format!("last seen {state}")
            )
        );
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// Cancels a query once BigQuery reports its job running, then checks both the local
/// `Status::Cancelled` and the job's `stopped` error reason.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn cancels_running_query_job() {
    /// Queries that may finish before cancellation lands; losing every attempt fails the test.
    const ATTEMPTS: usize = 3;

    for attempt in 1..=ATTEMPTS {
        let label = scratch_table("cancel");
        let mut connection = connect(project());
        let mut statement = prepare_query(&mut connection, CANCELLABLE_QUERY);
        set_option(
            &mut statement,
            BigQueryStatementOption::Labels,
            format!(r#"{{"{CANCEL_LABEL_KEY}":"{label}"}}"#),
        );
        // A cached result is DONE immediately, and a lost race would cache one for the next try.
        set_option(
            &mut statement,
            BigQueryStatementOption::DisableQueryCache,
            "true",
        );
        // A failed check leaves the job uncancelled, and `assert_cancels` joins `execute` while
        // unwinding: without a deadline, that waits on a query that can run for hours.
        set_option(
            &mut statement,
            BigQueryStatementOption::JobTimeout,
            300_000_i64,
        );
        let cancellation = statement.get_cancel_handle();

        let cancelled = assert_cancels(
            "statement",
            cancellation.as_ref(),
            // A job already DONE lost the race, which `assert_cancels` reports for a retry.
            || {
                await_labelled_job(&mut connection, &label, "reached the service", |_| true);
            },
            || outcome(statement.execute()),
        );
        if !cancelled {
            assert!(
                attempt < ATTEMPTS,
                "every one of the {ATTEMPTS} query jobs finished before its cancellation could \
                 be delivered, so nothing about cancelling one was asserted"
            );
            continue;
        }

        let (_, reason) =
            await_labelled_job(&mut connection, &label, "finished", |state| state == "DONE");
        assert_eq!(
            reason, "stopped",
            "BigQuery finished the cancelled job with error reason {reason:?}: the job outlived \
             the cancellation the driver asked for"
        );
        return;
    }
}

/// Cancelling a bulk ingest stops the write, not just the local loop.
///
/// The Storage Write API creates no job to inspect, so the target table is the evidence: rows
/// appended to a pending stream become visible only on commit. The paced reader reports progress,
/// which puts the cancellation window under the test's control.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn cancels_a_running_bulk_ingest() {
    /// Enough batches that the paced stream outlives the cancellation by a wide margin.
    const BATCHES: i64 = 100;
    /// Cancel once the driver requests this batch: the table exists and the second batch's append
    /// has opened the write stream, so there is a server-side write to stop.
    const APPENDING_BATCH: i64 = 3;

    struct PacedBatches {
        schema: SchemaRef,
        produced: i64,
        read: Arc<AtomicI64>,
    }

    impl Iterator for PacedBatches {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.produced == BATCHES {
                return None;
            }
            // Unpaced, the whole stream commits in well under a second.
            if self.produced > 0 {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            self.produced += 1;
            self.read.store(self.produced, Ordering::Release);
            Some(RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![self.produced]))],
            ))
        }
    }

    impl RecordBatchReader for PacedBatches {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    let (table, _drop_table, mut connection) = scratch_fixture("cancel_ingest");
    let mut ingest = connection.new_statement().unwrap();
    set_option(&mut ingest, OptionStatement::TargetTable, table.clone());
    let read = Arc::new(AtomicI64::new(0));
    ingest
        .bind_stream(Box::new(PacedBatches {
            schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            produced: 0,
            read: read.clone(),
        }))
        .unwrap();
    let cancellation = ingest.get_cancel_handle();

    let cancelled = assert_cancels(
        "ingest",
        cancellation.as_ref(),
        || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while read.load(Ordering::Acquire) < APPENDING_BATCH {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the BigQuery ingest never reached batch {APPENDING_BATCH}"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        },
        || ingest.execute_update().map(drop),
    );
    // About 10 s of paced batches remain when the gate opens, so this cannot race.
    assert!(
        cancelled,
        "the ingest finished before the cancellation could be delivered, yet the bound stream \
         paces {BATCHES} batches over roughly ten seconds"
    );

    // A cancelled `create` keeps its table for append retries but commits none of its rows.
    assert_eq!(count_rows(&mut connection, &table), 0);
}

/// The ADBC error a stream item carries, or a panic naming what came instead.
fn stream_error(item: Option<Result<RecordBatch, ArrowError>>, what: &str) -> Error {
    match item {
        Some(Err(ArrowError::ExternalError(source))) => match source.downcast::<Error>() {
            Ok(error) => *error,
            Err(source) => panic!("{what}: a non-ADBC stream error: {source}"),
        },
        other => panic!("{what}: expected an ADBC stream error, got {other:?}"),
    }
}

/// adbc.h Cancel on a result stream, for both read paths: `INVALID_STATE` before execution and
/// after the stream ends, `CANCELLED` on the next read, a harmless repeated Cancel, and a statement
/// that runs its next query normally (dbt reuses a connection after a cancelled model).
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn cancels_a_result_stream_mid_read() {
    let sql = storage_read_row_sequence("");
    let mut connection = connect(project());
    for disabled in [ADBC_OPTION_VALUE_ENABLED, ADBC_OPTION_VALUE_DISABLED] {
        let what = format!("use_storage_api_disabled_client={disabled}");
        let mut statement = prepare_query(&mut connection, &sql);
        set_option(
            &mut statement,
            BigQueryStatementOption::UseStorageApiDisabledClient,
            disabled,
        );
        let cancellation = statement.get_cancel_handle();
        assert_eq!(
            cancellation.try_cancel().unwrap_err().status,
            Status::InvalidState,
            "{what}: Cancel before any execution"
        );

        let mut reader = statement.execute().unwrap();
        assert!(reader.next().unwrap().unwrap().num_rows() > 0, "{what}");
        cancellation.try_cancel().unwrap();
        cancellation
            .try_cancel()
            .unwrap_or_else(|error| panic!("{what}: a repeated Cancel failed: {error}"));
        let error = stream_error(reader.next(), &what);
        assert_eq!(error.status, Status::Cancelled, "{what}: {error}");
        assert!(
            reader.next().is_none(),
            "{what}: a cancelled stream ends after reporting the cancellation"
        );
        drop(reader);
        assert_eq!(
            cancellation.try_cancel().unwrap_err().status,
            Status::InvalidState,
            "{what}: Cancel after the cancelled stream ended"
        );

        statement.set_sql_query("SELECT 1").unwrap();
        assert_eq!(read_single_i64(&mut statement), 1, "{what}");
        assert_eq!(
            cancellation.try_cancel().unwrap_err().status,
            Status::InvalidState,
            "{what}: Cancel after a result was read to its end"
        );
    }
}

/// adbc.h: executing a statement again "invalidates any prior result sets". The earlier stream
/// must fail with `CANCELLED` rather than end early as if complete.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn a_new_execution_cancels_the_previous_stream() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, &storage_read_row_sequence(""));
    let mut previous = statement.execute().unwrap();
    assert!(previous.next().unwrap().unwrap().num_rows() > 0);

    statement.set_sql_query("SELECT 2").unwrap();
    assert_eq!(read_single_i64(&mut statement), 2);
    let error = stream_error(previous.next(), "the superseded stream");
    assert_eq!(error.status, Status::Cancelled, "{error}");
    assert!(previous.next().is_none());
}

/// A partition stream belongs to the connection that read it (adbc.h: the connection "must
/// outlive the returned ArrowArrayStream"), so `AdbcConnectionCancel` is what stops it.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn connection_cancel_stops_a_partition_stream() {
    let mut connection = connect(project());
    let partitions = prepare_query(&mut connection, &storage_read_row_sequence(""))
        .execute_partitions()
        .unwrap();
    let cancellation = connection.get_cancel_handle();
    let mut reader = connection
        .read_partition(&partitions.partitions[0])
        .unwrap();
    assert!(reader.next().unwrap().unwrap().num_rows() > 0);

    cancellation.try_cancel().unwrap();
    let error = stream_error(reader.next(), "the partition stream");
    assert_eq!(error.status, Status::Cancelled, "{error}");
    drop(reader);
    assert_eq!(
        cancellation.try_cancel().unwrap_err().status,
        Status::InvalidState,
        "Cancel after the partition stream ended"
    );
}
