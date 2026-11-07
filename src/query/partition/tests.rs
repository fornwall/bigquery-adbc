use adbc_core::error::Status;

use crate::pipeline::QueryResultOptions;
use crate::rest::jobs::JobReference;

use super::{decode_partition, encode_partition};
use crate::test_support::errors::assert_failure;

fn options(use_storage_read: bool, unordered_read: bool, prefetch: i64) -> QueryResultOptions {
    let mut options = QueryResultOptions::default();
    options.use_storage_read = use_storage_read;
    options.unordered_read = unordered_read;
    options
        .set_prefetch_concurrency("prefetch", prefetch)
        .unwrap();
    options
}

/// The descriptor carries the creating statement's result options, so its partitions read the
/// way its `ExecuteQuery` results would.
#[test]
fn partition_descriptors_round_trip_the_job_and_result_options() {
    let reference = JobReference::new(
        "billing-project",
        Some("europe-north1"),
        "adbc_job_123".to_owned(),
    );
    for options in [
        QueryResultOptions::default(),
        options(false, true, 3),
        options(true, true, 1),
        options(false, false, i64::from(u32::MAX)),
    ] {
        let descriptor = encode_partition(&reference, options).unwrap();
        assert_eq!(
            decode_partition(&descriptor).unwrap(),
            (reference.clone(), options)
        );
    }
    // A wider prefetch than the descriptor holds reads as the widest it does.
    let wide = encode_partition(&reference, options(true, false, i64::MAX)).unwrap();
    assert_eq!(
        decode_partition(&wide).unwrap().1,
        options(true, false, i64::from(u32::MAX))
    );
}

#[test]
fn partition_descriptors_preserve_server_resolved_project_spellings() {
    for project in [
        "123456789012",
        "example.com:project",
        "FutureProjectSpelling",
    ] {
        let reference = JobReference::new(project, Some("US"), "adbc_job_123".to_owned());
        let options = QueryResultOptions::default();
        let descriptor = encode_partition(&reference, options).unwrap();
        assert_eq!(decode_partition(&descriptor).unwrap(), (reference, options));
    }
}

#[test]
fn partition_descriptors_reject_invalid_data() {
    let defaults = QueryResultOptions::default();
    let descriptor = encode_partition(
        &JobReference::new("billing-project", Some("EU"), "adbc_job_123".to_owned()),
        defaults,
    )
    .unwrap();
    let empty_job_id = encode_partition(
        &JobReference::new("billing-project", None, String::new()),
        defaults,
    )
    .unwrap();
    let invalid_project = encode_partition(
        &JobReference::new("path/segment", None, "adbc_job_123".to_owned()),
        defaults,
    )
    .unwrap();
    let with_options = |flags: u8, prefetch: u32| {
        let mut changed = descriptor[..descriptor.len() - 5].to_vec();
        changed.push(flags);
        changed.extend_from_slice(&prefetch.to_be_bytes());
        changed
    };
    // A length prefix cut in half, and a field whose bytes are not text: neither can be built by
    // `encode_partition`, so both are written out here exactly as a corrupted descriptor arrives.
    let truncated_length = [&super::PARTITION_MAGIC[..], b"\x00\x00"].concat();
    let not_utf8 = [
        &super::PARTITION_MAGIC[..],
        b"\x00\x00\x00\x01\xff",
        b"\x00\x00\x00\x03job",
        b"\x00\x00\x00\x00",
        b"\x01\x00\x00\x00\x0a",
    ]
    .concat();

    // Each refusal names the defect, because the descriptor is opaque to the caller holding it.
    for (invalid, expected) in [
        (Vec::new(), "invalid header"),
        (b"SQBQJOB\x01".to_vec(), "invalid header"),
        (
            descriptor[..descriptor.len() - 1].to_vec(),
            "truncated result options",
        ),
        ([&descriptor[..], b"trailing"].concat(), "trailing data"),
        (with_options(4, 10), "unknown result option flags"),
        (with_options(1, 0), "its prefetch concurrency is zero"),
        (empty_job_id, "job ID must not be empty"),
        (truncated_length, "missing project ID length"),
        (not_utf8, "project ID is not valid UTF-8"),
        (
            invalid_project,
            "BigQuery project identifier from the partition descriptor",
        ),
    ] {
        let failure = decode_partition(&invalid).unwrap_err();
        assert_failure(&failure, Status::InvalidArguments, &[expected]);
    }
}
