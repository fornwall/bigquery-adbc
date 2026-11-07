# Changelog

Notable changes to bigquery-adbc. The project is experimental, so any release may include breaking
changes.

## [Unreleased]

### Fixed

- A query result page repeating an earlier page token now returns `INVALID_DATA` rather than `IO`:
  reading it again gets the same answer, so it is a malformed response, not a transient failure.
- A transient `CreateReadSession` failure is retried for up to a minute instead of failing a query
  that already finished.
- A job stopped by `bigquery.query.max_bytes_billed` now returns `INVALID_ARGUMENT` instead of
  `UNKNOWN`, as the same reason already did on a rejected REST answer.
- `CreateReadSession` fails with `TIMEOUT` after two minutes instead of blocking forever on an
  endpoint that stalls during the TLS handshake.
- A REST rejection with reason `timeout` now returns `TIMEOUT` and one with `resourceInUse` returns
  `INVALID_STATE`, as both do on a failed job, instead of `INVALID_ARGUMENT`. A job's timeout read
  through a rejected `jobs.query` or `jobs.getQueryResults` answer now reads as a timeout.
- A python-model job can no longer outlive a cancel that arrives while its submission is in flight:
  the submission finishes, and the job its answer names is asked to stop. A transient failure of
  that remote cancel is resent instead of leaving the job running.
- Python-model waits tolerate more glitches: Cloud Storage uploads are resent after a transient
  failure, a Dataproc submission or poll whose 2xx answer cannot be read is repeated, a rate-limited
  Vertex AI create is resent, and a notebook job with no reported state keeps being polled.
- A short outage no longer fails a python model while its job keeps running. Dataproc and Vertex AI
  polls outlast ten minutes of consecutive transient failures, as a BigQuery job wait does, instead
  of stopping after five.
- A python-model submission that ends without a verdict, such as a Vertex AI create whose answer
  was lost, says the job may have started.
- `grant_access_to` backs off between lost `If-Match` races and allows 16 rounds rather than 3, so
  dbt threads granting on one dataset no longer fail with `INVALID_STATE`.
- A refused CSV seed upload no longer sends a `jobs.cancel` for a job that does not exist, nor warns
  that it may keep running.
- A line comment now ends at a carriage return as well as a newline, as in GoogleSQL. A statement
  after a `\r`-terminated comment was refused as comment-only text.
- A connection the external-account identity provider dropped before answering is treated as
  transient. It previously ended credential refresh for good, failing every later query once the
  cached token expired.
- A second `AdbcErrorFromArrayStream` call on a stream no longer frees the message and detail
  pointers the first one returned.
- A network outage or a run of 5xx responses no longer fails a query, table copy or CSV load after a
  few seconds and cancels its healthy job. Waiting for a job now outlasts ten minutes of consecutive
  retryable failures, and for as long as `jobs.get` reports the job unfinished. If the wait does give
  up, the error says the job's outcome is unknown.
- A stalled Storage Read stream no longer blocks its reader forever. After two minutes of silence
  while the reader has room, it resumes at the next unread row, and it fails with `TIMEOUT` once
  its attempts are spent.
- A `jobs.query` submission whose retries end without a verdict now reads `failed to submit` and
  says BigQuery may still have run the statement. It read as a rejection, inviting a resubmission
  that could apply DML twice.
- REST retries leave an HTTP/2 connection whose peer keeps its socket open but stops answering,
  such as a wedged proxy. They used to reuse it until every attempt had timed out.
- A job submission refused with HTTP 401 under `temporary_access_token` or `anonymous`
  authentication no longer sends a `jobs.cancel` for a job that was never created.
- A `replace` whose `TRUNCATE` or `CREATE OR REPLACE TABLE` gets no answer says the old rows may be
  gone, as a cancelled one already did, rather than implying they stand.
- The `TRUNCATE` before a `replace` commits carries a ten-minute job timeout, since nothing can
  cancel it: one queued behind a busy reservation held the statement forever, even after a Cancel
  reported success.
- A bulk ingest no longer hangs on a connection that dies silently: an append unanswered for five
  minutes is replayed on a new connection, and a stream call unanswered for a minute fails with
  `TIMEOUT`.
- A token service that accepts a connection and never answers fails a request with `IO` after about
  two minutes instead of hanging it. Retries and later requests then fail within seconds until a
  token arrives, and a wait for a running job outlasts it like any other outage. This covers the
  OAuth, STS and metadata-server exchanges.
- A cancel takes effect while a Storage client is being built, instead of waiting for a stalled
  metadata-server lookup.
- A database opened in a `fork` child works instead of hanging forever on a runtime whose worker
  threads did not survive the fork. Using a handle or result stream inherited from the parent
  returns an error, unless another thread was inside a call on it at the fork, and releasing one
  leaves the parent's BigQuery session open.
- A `BEGIN`, `COMMIT` or `ROLLBACK` whose answer is lost now reports what BigQuery did. BigQuery does
  not deduplicate these by request ID, so a retry ran them again: a commit that took effect failed
  as a lost transaction, and a transaction that began left every later statement failing. They now
  run as named jobs, and a retry reads the first job's verdict.
- Ending a BigQuery session no longer blocks without limit: the caller waits at most ten seconds,
  which bounds commit, rollback, autocommit changes and connection release. An error now says when
  the session could not be ended, instead of claiming it was.
- A `BEGIN`, `COMMIT` or `ROLLBACK` now counts as refused only on proof: the job's own failure, or a
  submission BigQuery refused or never received. Anything else, such as an unreadable answer or a
  failed job lookup, is an unknown outcome that returns `IO` and ends the session, instead of
  keeping a possibly committed transaction open. The new `bigquery.job.verdict` error detail shows
  which it was.
- After a statement fails with "No active transaction found", the next one begins a new transaction
  instead of autocommitting in the session.
- A metadata listing whose page token repeats now returns `INVALID_DATA` instead of `IO`, which
  reads as a transient failure.

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
