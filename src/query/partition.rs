//! Encode and validate an ADBC partition descriptor: the job whose results it reads, and the
//! result options of the statement that ran the job.
//!
//! The options travel with the descriptor, so a statement's `bigquery.query.unordered_read`,
//! `bigquery.query.use_storage_api_disabled_client` and `bigquery.query.prefetch_concurrency`
//! apply to its partitions as to its `ExecuteQuery` results, whichever connection reads them.

use adbc_core::error::{Result, Status};

use crate::error::{error, invalid_argument};
use crate::identifiers::ProjectIdentifier;
use crate::pipeline::QueryResultOptions;
use crate::rest::jobs::JobReference;

/// The prefix of every partition descriptor. Its last byte is the format version, so a descriptor
/// in another layout is refused rather than misread.
const PARTITION_MAGIC: &[u8; 8] = b"SQBQJOB\x02";

/// Bits of the result options flag byte.
const USE_STORAGE_READ: u8 = 1;
const UNORDERED_READ: u8 = 2;

pub(super) fn encode_partition(
    reference: &JobReference,
    options: QueryResultOptions,
) -> Result<Vec<u8>> {
    let fields = [
        reference.project_id.as_str(),
        reference.job_id.as_str(),
        reference.location.as_deref().unwrap_or_default(),
    ];
    let mut partition = Vec::with_capacity(
        PARTITION_MAGIC.len()
            + 3 * size_of::<u32>()
            + fields.iter().map(|s| s.len()).sum::<usize>()
            + 1
            + size_of::<u32>(),
    );
    partition.extend_from_slice(PARTITION_MAGIC);
    for field in fields {
        let length = u32::try_from(field.len()).map_err(|_| {
            error(
                "BigQuery job reference is too large for a partition descriptor",
                Status::Internal,
            )
        })?;
        partition.extend_from_slice(&length.to_be_bytes());
        partition.extend_from_slice(field.as_bytes());
    }
    let mut flags = 0;
    if options.use_storage_read() {
        flags |= USE_STORAGE_READ;
    }
    if options.unordered_read() {
        flags |= UNORDERED_READ;
    }
    partition.push(flags);
    // A prefetch wider than `u32` reads the same as the widest that fits.
    let prefetch = u32::try_from(options.prefetch_concurrency()).unwrap_or(u32::MAX);
    partition.extend_from_slice(&prefetch.to_be_bytes());
    Ok(partition)
}

pub(super) fn decode_partition(mut partition: &[u8]) -> Result<(JobReference, QueryResultOptions)> {
    partition = partition
        .strip_prefix(PARTITION_MAGIC)
        .ok_or_else(|| invalid_partition("invalid header"))?;
    let project_id = decode_partition_field(&mut partition, "project ID")?;
    let job_id = decode_partition_field(&mut partition, "job ID")?;
    let location = decode_partition_field(&mut partition, "location")?;
    let &[flags, p0, p1, p2, p3] = partition else {
        return Err(invalid_partition(if partition.len() < 5 {
            "truncated result options"
        } else {
            "trailing data"
        }));
    };
    if job_id.is_empty() {
        return Err(invalid_partition("job ID must not be empty"));
    }
    if flags & !(USE_STORAGE_READ | UNORDERED_READ) != 0 {
        return Err(invalid_partition("unknown result option flags"));
    }
    let mut options = QueryResultOptions::default();
    options.set_use_storage_read(flags & USE_STORAGE_READ != 0);
    options.set_unordered_read(flags & UNORDERED_READ != 0);
    options
        .set_prefetch_concurrency("", i64::from(u32::from_be_bytes([p0, p1, p2, p3])))
        .map_err(|_| invalid_partition("its prefetch concurrency is zero"))?;
    let project_id = ProjectIdentifier::try_new_from("the partition descriptor", project_id)?;

    Ok((
        JobReference::new(project_id.as_str(), Some(location), job_id.to_owned()),
        options,
    ))
}

fn decode_partition_field<'a>(partition: &mut &'a [u8], name: &str) -> Result<&'a str> {
    let (length, remaining) = partition
        .split_first_chunk::<4>()
        .ok_or_else(|| invalid_partition(format!("missing {name} length")))?;
    let (value, remaining) = remaining
        .split_at_checked(u32::from_be_bytes(*length) as usize)
        .ok_or_else(|| invalid_partition(format!("truncated {name}")))?;
    *partition = remaining;
    std::str::from_utf8(value).map_err(|_| invalid_partition(format!("{name} is not valid UTF-8")))
}

/// Include the descriptor's origin because its bytes are opaque to the caller.
fn invalid_partition(reason: impl std::fmt::Display) -> adbc_core::error::Error {
    invalid_argument(format!(
        "invalid BigQuery partition descriptor: {reason}; pass back a descriptor returned by \
         AdbcStatementExecutePartitions on this driver, unmodified"
    ))
}

#[cfg(test)]
mod tests;
