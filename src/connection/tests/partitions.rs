//! Partition readers: reading two of a connection's partitions at once, and ending them all.

use super::*;

#[test]
fn partition_job_ids_cannot_change_the_request_path() {
    let (endpoint, requests) = capture_requests(Vec::new());
    let connection = anonymous_connection_at(&endpoint);

    for job_id in [".", ".."] {
        let Err(failure) = connection.read_partition(partition_descriptor(job_id)) else {
            panic!("a URL dot segment was accepted as a partition job ID");
        };
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_message(
            &failure,
            &["invalid BigQuery partition descriptor", "job ID"],
        );
    }
    assert!(requests.join().unwrap().is_empty());
}

/// adbc.h's partitioned-results section exists so partitions "can be divided among threads or
/// machines and fetched in parallel": a second partition read, and any later connection call,
/// must leave an already-returned partition stream readable.
#[test]
fn concurrent_partition_readers_both_complete() {
    let (connection, requests, mut first, mut second) = two_partition_readers();
    // A metadata call on the same connection begins an operation of its own.
    drop(connection.get_table_types().unwrap());

    assert_eq!(partition_value(&mut first), 1);
    assert!(first.next().is_none());
    assert_eq!(partition_value(&mut second), 2);
    assert!(second.next().is_none());
    requests.served(6);
}

/// adbc.h: Cancel "can be called during AdbcConnectionGetObjects (or similar), or while
/// consuming an ArrowArrayStream returned from such". With several partition streams live at
/// once, it ends all of them rather than only the most recent.
#[test]
fn cancelling_a_connection_ends_every_partition_reader() {
    let (connection, requests, mut first, mut second) = two_partition_readers();
    let requests = requests.join().unwrap();
    connection.get_cancel_handle().try_cancel().unwrap();

    for reader in [&mut first, &mut second] {
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("cancelled"), "{error}");
    }
    // Both readers stop, but their jobs already reached DONE and need no remote cancellation.
    let cancelled = requests
        .into_iter()
        .filter(|request| request.path().ends_with("/cancel"))
        .count();
    assert_eq!(cancelled, 0);
}

/// Partitions may be fetched in parallel, so threads call `read_partition` on one connection at
/// once. A failed read drops its unfinished operation while another thread starts one; that start
/// must not deadlock on the connection's operation list. The race window is narrow, so the threads
/// repeat until it has been crossed many times.
#[test]
fn parallel_failing_partition_reads_do_not_deadlock() {
    const THREADS: usize = 4;
    const ROUNDS: usize = 20_000;
    let connection = std::sync::Arc::new(anonymous_connection_at(
        crate::test_support::UNREACHABLE_ENDPOINT,
    ));
    let (done, finished) = std::sync::mpsc::channel();
    for _ in 0..THREADS {
        let connection = connection.clone();
        let done = done.clone();
        thread::spawn(move || {
            for _ in 0..ROUNDS {
                let Err(failure) = connection.read_partition(b"not a partition") else {
                    panic!("an invalid partition descriptor was read");
                };
                assert_eq!(failure.status, Status::InvalidArguments);
            }
            done.send(()).unwrap();
        });
    }
    drop(done);
    for _ in 0..THREADS {
        finished
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("a partition read deadlocked or failed");
    }
}

type Reader = Box<dyn RecordBatchReader + Send>;

/// A connection reading two partitions at once, each a one-row job.
fn two_partition_readers() -> (crate::BigQueryConnection, CapturedServer, Reader, Reader) {
    let (endpoint, requests) = capture_requests(vec![
        (200, partition_job("partition-one")),
        (200, partition_rows(1)),
        (200, partition_job("partition-one")),
        (200, partition_job("partition-two")),
        (200, partition_rows(2)),
        (200, partition_job("partition-two")),
    ]);
    let connection = anonymous_connection_at(&endpoint);
    let first = connection
        .read_partition(partition_descriptor("partition-one"))
        .unwrap();
    let second = connection
        .read_partition(partition_descriptor("partition-two"))
        .unwrap();
    (connection, requests, first, second)
}

fn partition_job(job_id: &str) -> serde_json::Value {
    json!({
        "jobReference": {"projectId": "test-project", "jobId": job_id},
        "status": {"state": "DONE"},
        "configuration": {"query": {"query": "SELECT 1"}}
    })
}

fn partition_rows(value: i64) -> serde_json::Value {
    json!({
        "jobComplete": true,
        "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
        "rows": [{"f": [{"v": value.to_string()}]}],
        "totalRows": "1"
    })
}

/// The wire form of a partition descriptor: the magic header, then the project, job and
/// location as big-endian length-prefixed strings.
fn partition_descriptor(job_id: &str) -> Vec<u8> {
    let mut descriptor = b"SQBQJOB\x02".to_vec();
    for field in ["test-project", job_id, ""] {
        descriptor.extend_from_slice(&u32::try_from(field.len()).unwrap().to_be_bytes());
        descriptor.extend_from_slice(field.as_bytes());
    }
    // The default result options: Storage Read allowed, ordered, a prefetch of 10.
    descriptor.extend_from_slice(&[1, 0, 0, 0, 10]);
    descriptor
}

fn partition_value(reader: &mut Reader) -> i64 {
    typed::<Int64Array>(reader.next().unwrap().unwrap().column(0)).value(0)
}
