# Changelog

Notable changes to bigquery-adbc. The project is experimental, so any release may include breaking
changes.

## [Unreleased]

### Fixed

- A transient `CreateReadSession` failure is retried for up to a minute instead of failing a query
  that already finished.
- A REST rejection with reason `timeout` now returns `TIMEOUT` and one with `resourceInUse` returns
  `INVALID_STATE`, as both do on a failed job, instead of `INVALID_ARGUMENT`. A job's timeout read
  through a rejected `jobs.query` or `jobs.getQueryResults` answer now reads as a timeout.
- A python-model job can no longer outlive a cancel that arrives during its submission: the
  submission finishes, and the job its answer names is asked to stop. A transient failure of that
  remote cancel is resent instead of leaving the job running.
- Python-model waits tolerate more glitches: Cloud Storage uploads are resent after a transient
  failure, a Dataproc submission or poll whose 2xx answer cannot be read is repeated, a rate-limited
  Vertex AI create is resent, and a notebook job with no reported state keeps being polled.
- `grant_access_to` backs off between lost `If-Match` races and allows 16 rounds rather than 3, so
  dbt threads granting on one dataset no longer fail with `INVALID_STATE`.
- A refused CSV seed upload no longer sends a `jobs.cancel` for a job that does not exist, nor warns
  that it may keep running.
- A line comment now ends at a carriage return as well as a newline, as in GoogleSQL. A statement
  after a `\r`-terminated comment was refused as comment-only text.

## [0.2.0] - 2026-11-06

### Added

- Runnable examples for C, Python and Rust in `examples/`.
- GitHub releases carry the shared library for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
  Windows x86-64, with SHA-256 checksums. Each archive has a driver manifest for loading the driver
  by name.

### Changed

- Statements share their SQL text with execution plans and parameter readers, avoiding repeated
  copies of compiled queries.

- The native library shares transaction-control and connection-cleanup task machinery to reduce
  binary size.

- Storage Write validation reuses sliced list offsets unless the list needs normalization,
  avoiding offset-buffer copies for canonical arrays.

- Diagnostics lines start with a UTC timestamp and are tagged `[bigquery-adbc]`.
- An empty `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` disables diagnostics.

### Fixed

- Partition reads reject malformed descriptors containing `.` or `..` job IDs before sending a
  request, preventing URL path normalization from selecting a different resource.
- The emulator test runner drains stdout after startup, preventing request logs from filling the
  pipe and stalling tests.
- Queries identified as `SELECT` by a later job-metadata lookup reject a missing result schema
  with `INVALID_DATA` instead of silently returning an empty result.
- Query metadata preserves completed status and resolved job identities, avoiding redundant
  cancellation of finished jobs and directing cancellation of running jobs to the correct region.
- Ingest window sizing accounts for resolved Null fields and widened primitive types before
  allocating normalized buffers.
- Regional job recovery preserves cancellation after a duplicate-job response even when fetching
  the job's details fails.
- Failed or cancelled ingests release queued append payloads before remote stream cleanup;
  input failures no longer wait for unacknowledged appends.
- Storage Read sends the driver and custom user agent on every row-stream request, including
  parallel reads and resumed streams.
- CSV seed validation rejects Unix FIFOs without waiting for a writer.
- Pipelined Storage Write appends honor the same three-retry limit as the first append.
- Cancelled queries stop retrying definitive submission rejections, while ambiguous submissions
  still recover their job reference for cancellation.
- Decimal ingestion validates source precision at all four Arrow widths and accepts zero at extreme
  negative scales when the field declares a valid BigQuery decimal shape.
- Metadata discovery uses a described table's current type after concurrent replacement, keeping
  type filters and statistics consistent with its schema.
- Parameter-schema discovery preserves prior execution progress even when its dry run retries.
- Parameterized stored-procedure calls preserve every returned result in bound-row order.
- Storage Read treats an empty Arrow timezone as DATETIME, rejecting conversions to TIMESTAMP.
- Appends inherit STRING/BYTES length limits through unrelated Arrow extension annotations.
- URI validation no longer echoes rejected `OAuthType` values, which may contain credentials.
- Metadata discovery preserves `google.rpc.ErrorInfo` reasons, so disabled APIs fail instead of
  appearing as inaccessible tables.
- Notebook-template lookup follows empty result pages before reporting `NOT_FOUND` and rejects
  malformed listings as `INVALID_DATA`.
- REST requests refuse redirects, preventing query and upload bodies from being forwarded beyond
  the configured endpoint.
- Ingest rejects equal or reversed RANGE bounds before an invalid first batch can create or
  replace the target table.
- `GetObjects` reports repeated columns as non-nullable in both XDBC nullability fields.
- Dataset-location `ExecuteSchema` invalidates prior statement results, including unread bound
  parameter rows.
- Native archives package an explicitly supplied library under the filename expected by the
  bundled driver manifest, even when the source file has a different name.
- The REST conversion window counts retained row and nested-value allocations, including spare
  capacity, so uneven pages with a few wide rows no longer evade its memory accounting.

## [0.1.1] - 2026-11-05

### Fixed

- The source distribution puts its license files where its metadata declares them, so PyPI accepts
  it. Release 0.1.0 has wheels only.

## [0.1.0] - 2026-11-04

First public release, with wheels on PyPI for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
Windows x86-64.

[Unreleased]: https://github.com/fornwall/bigquery-adbc/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fornwall/bigquery-adbc/tree/v0.1.0
