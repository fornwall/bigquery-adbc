# Changelog

Notable changes to bigquery-adbc. The project is experimental, so any release may include breaking
changes.

## [Unreleased]

### Added

- `bigquery.query.destination_table`, the dbt copy options and authorized views accept
  `project.catalog.namespace.table` for a BigLake Iceberg table, sent as dataset
  `catalog.namespace`. A project with dots but no domain-scoped colon is no longer read as one
  project, since BigQuery refuses it.

### Changed

- The native library is 3% smaller (266 KB on x86-64 Linux): IAM impersonation refusals decode
  without the SDK's generic `google.rpc.Status` parser, URL parsing links no Unicode host-name
  tables, and `adbc_core`, `tokio-util` and `flatbuffers` compile for size.
- A URL in a credential document naming a non-ASCII host is refused; write an internationalized
  host in Punycode (`xn--`).
- The dbt installer accepts dbt's Rust target triples with `--target`, and rejects unknown
  options and missing values before installing.
- Python's `dbapi.connect()` takes `backend` as its only keyword beyond `autocommit`, so a
  misspelled keyword raises `TypeError` before the driver loads, instead of after opening the
  database and connection.

### Fixed

- Cancel no longer fails with `UNKNOWN` and BigQuery's 404 after stopping a job submission before
  the job was created, when that submission ended just as the cancel claimed it.
- A named parameter longer than BigQuery's 127-character limit is refused before sending when its
  name has characters beyond U+FFFF, which BigQuery counts twice.
- A partition stream that fails, or is cancelled, before its first results read no longer asks
  BigQuery to cancel the partition's already finished job. That request could fail for a reader
  without permission to cancel the job, making `AdbcConnectionCancel` return `UNKNOWN`.

## [0.5.0] - 2026-11-09

### Added

- The patched dbt is also built for Windows x86-64.
- Each release carries `install.sh`, which installs the patched dbt on Linux and macOS:
  `curl -fsSL https://github.com/fornwall/bigquery-adbc/releases/latest/download/install.sh | sh`.
- `BIGQUERY_ADBC_USE_DBT_DRIVER=1` makes a patched dbt run use dbt's own BigQuery driver, resolved
  as stock dbt resolves it. Both drivers stay cached, so runs can switch back and forth.

### Changed

- The patched dbt never loads BigQuery from stock dbt's `lib/dbt/`, so it can share an install
  prefix with stock dbt. Non-release builds carry the driver in `lib/bigquery-adbc/`.
- The patched dbt's binary is named `dbt-bigquery-adbc`, so it no longer replaces dbt Labs' `dbt`.
  Its `system update` installs the latest bigquery-adbc release instead of saying to update dbt with
  a package manager.

### Fixed

- A row that bulk ingest refuses in a later batch is named by its position in the whole bound
  stream, as BigQuery's own row errors are, rather than by its position within that batch.

## [0.4.0] - 2026-11-07

### Added

- A dbt patched to use this driver is built for each release and push to `main`. A release's build
  needs only its `dbt` binary, which downloads and verifies this driver on first use.
- Releases include the compressed library that those builds download.

## [0.3.0] - 2026-11-07

0.2.0 was tagged but never published (its wheel tests failed), so this release also carries its
changes.

### Added

- The `bigquery.job.verdict` error detail says what a failure proves about its job: `failed` when
  the job reported its own failure, `not_created` when the submission never created one.

### Changed

- A query result or metadata listing whose page token repeats returns `INVALID_DATA` rather than
  `IO`: rereading gets the same malformed answer.
- A job stopped by `bigquery.query.max_bytes_billed` returns `INVALID_ARGUMENT` instead of
  `UNKNOWN`, matching a rejected REST answer with the same reason.
- A REST rejection with reason `timeout` returns `TIMEOUT` and one with `resourceInUse` returns
  `INVALID_STATE`, as on a failed job, instead of `INVALID_ARGUMENT`. A job timeout read through a
  rejected `jobs.query` or `jobs.getQueryResults` answer reads as a timeout.

### Fixed

- A network outage or a run of 5xx responses no longer fails a query, table copy or CSV load after a
  few seconds and cancels its healthy job. Job waits outlast ten minutes of consecutive retryable
  failures, and continue while `jobs.get` reports the job unfinished; if a wait gives up, the error
  says the job's outcome is unknown.
- A statement that returns no rows, such as DML or `COMMIT`, succeeds once `jobs.get` reports its job
  finished without an error, even if reading its results keeps failing. It used to fail with `IO`
  after the outage window, inviting a rerun of committed work.
- A `jobs.query` submission whose retries end without a verdict reads `failed to submit` and says
  BigQuery may still have run the statement, instead of reading as a rejection that invites a
  resubmission that could apply DML twice.
- A `jobs.query` refused on a retry says the statement may still have run, so a `COMMIT` that took
  effect on a lost attempt no longer looks invalid.
- A job submission refused with HTTP 401 under `temporary_access_token` or `anonymous`
  authentication no longer sends `jobs.cancel` for a job that was never created.
- A `BEGIN`, `COMMIT` or `ROLLBACK` whose answer is lost reports what BigQuery did. BigQuery does not
  deduplicate these by request ID, so retries ran them again: a commit that took effect failed as a
  lost transaction, and a transaction that began left every later statement failing. They now run
  as named jobs, and a retry reads the first job's verdict.
- A `BEGIN`, `COMMIT` or `ROLLBACK` counts as refused only on proof: the job's own failure, or a
  submission BigQuery refused or never received. Anything else, such as an unreadable answer or a
  failed job lookup, is an unknown outcome that returns `IO` and ends the session instead of keeping
  a possibly committed transaction open; `bigquery.job.verdict` shows which it was.
- A transaction control statement whose answers were all lost is looked for in the default
  dataset's location, so recovery no longer needs `bigquery.jobs.list` there.
- Ending a BigQuery session waits at most ten seconds, bounding commit, rollback, autocommit changes
  and connection release. The error says when the session could not be ended or that could not be
  confirmed, instead of claiming it was.
- After a statement fails with "No active transaction found", the next one begins a new transaction
  instead of autocommitting in the session.
- An OAuth, STS or metadata-server token exchange that connects and never answers fails with `IO`
  after about two minutes instead of hanging. Retries and later requests then fail within seconds
  until a token arrives, and a running job's wait outlasts it like any other outage.
- REST retries leave an HTTP/2 connection whose peer keeps its socket open but stops answering (such
  as a wedged proxy) instead of reusing it until every attempt times out.
- A cancel takes effect while a Storage client is being built, instead of waiting for a stalled
  metadata-server lookup.
- A connection the external-account identity provider dropped before answering is transient,
  instead of ending credential refresh for good and failing every query once the cached token
  expired.
- A database opened in a `fork` child works instead of hanging on a runtime whose worker threads did
  not survive the fork. Using a handle or result stream inherited from the parent returns
  `INVALID_STATE`, and releasing one leaves the parent's BigQuery session open.
- A stalled Storage Read stream no longer blocks its reader forever. After two minutes without a
  complete response while the reader has room, it resumes at the next unread row, each consecutive
  failure doubling that limit; once attempts are spent it fails with `TIMEOUT`, after about 10
  minutes on a dead connection and up to 62 on a stream that opens and then stays silent.
- A transient `CreateReadSession` failure is retried for up to a minute instead of failing a query
  that already finished.
- `CreateReadSession` fails with `TIMEOUT` after two minutes instead of blocking forever on an
  endpoint that stalls during the TLS handshake.
- A bulk ingest no longer hangs on a silently dead connection: an append unanswered for five minutes
  is replayed on a new connection, and a stream call unanswered for a minute fails with `TIMEOUT`.
- The `TRUNCATE` before a `replace` commits carries a ten-minute job timeout, since nothing can
  cancel it; one queued behind a busy reservation held the statement forever, even after a Cancel
  reported success.
- A `replace` whose `TRUNCATE` or `CREATE OR REPLACE TABLE` gets no answer says the old rows may be
  gone, as a cancelled one already did.
- A short outage no longer fails a python model while its job keeps running: Dataproc and Vertex AI
  polls outlast ten minutes of consecutive transient failures, like a BigQuery job wait, instead of
  stopping after five.
- A python-model job no longer outlives a cancel that arrives during its submission: the submission
  finishes and the named job is asked to stop, resending that remote cancel after a transient
  failure.
- A python-model submission that ends without a verdict, such as a Vertex AI create whose answer was
  lost, says the job may have started.
- Python-model runs tolerate more glitches: Cloud Storage uploads are resent after a transient
  failure, a Dataproc submission or poll whose 2xx answer cannot be read is repeated, a rate-limited
  Vertex AI create is resent, and a notebook job with no reported state keeps being polled.
- `grant_access_to` backs off between lost `If-Match` races and allows 16 rounds rather than 3, so
  dbt threads granting on one dataset no longer fail with `INVALID_STATE`.
- A refused CSV seed upload no longer sends `jobs.cancel` for a nonexistent job or warns that it may
  keep running.
- A second `AdbcErrorFromArrayStream` call on a stream no longer frees the message and detail
  pointers the first one returned.
- A line comment ends at a carriage return as well as a newline, as in GoogleSQL; a statement after a
  `\r`-terminated comment was refused as comment-only text.

## [0.2.0] - 2026-11-06

### Added

- Runnable examples for C, Python and Rust in `examples/`.
- GitHub releases carry the shared library for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
  Windows x86-64, with SHA-256 checksums and a driver manifest for loading the driver by name.

### Changed

- Statements share their SQL text with execution plans and parameter readers instead of copying
  compiled queries.
- Transaction-control and connection-cleanup tasks share machinery, reducing binary size.
- Storage Write validation reuses sliced list offsets unless the list needs normalization.
- Diagnostics lines start with a UTC timestamp and are tagged `[bigquery-adbc]`; an empty
  `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` disables them.

### Fixed

- Partition reads reject descriptors with `.` or `..` job IDs before sending a request, so URL path
  normalization cannot select a different resource.
- The emulator test runner drains stdout after startup, so request logs no longer fill the pipe and
  stall tests.
- A query identified as `SELECT` by a later job-metadata lookup rejects a missing result schema with
  `INVALID_DATA` instead of returning an empty result.
- Query metadata keeps completed status and resolved job identities, so finished jobs are not
  cancelled again and running jobs are cancelled in the correct region.
- Ingest window sizing accounts for resolved Null fields and widened primitive types before
  allocating normalized buffers.
- Regional job recovery keeps cancellation after a duplicate-job response even when fetching the
  job's details fails.
- Failed or cancelled ingests release queued append payloads before remote stream cleanup; input
  failures no longer wait for unacknowledged appends.
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
- Parameter-schema discovery keeps prior execution progress even when its dry run retries.
- Parameterized stored-procedure calls return every result in bound-row order.
- Storage Read treats an empty Arrow timezone as DATETIME, rejecting conversions to TIMESTAMP.
- Appends inherit STRING/BYTES length limits through unrelated Arrow extension annotations.
- URI validation no longer echoes rejected `OAuthType` values, which may contain credentials.
- Metadata discovery keeps `google.rpc.ErrorInfo` reasons, so disabled APIs fail instead of
  appearing as inaccessible tables.
- Notebook-template lookup follows empty result pages before reporting `NOT_FOUND` and rejects
  malformed listings as `INVALID_DATA`.
- REST requests refuse redirects, so query and upload bodies are never forwarded beyond the
  configured endpoint.
- Ingest rejects equal or reversed RANGE bounds before an invalid first batch can create or replace
  the target table.
- `GetObjects` reports repeated columns as non-nullable in both XDBC nullability fields.
- Dataset-location `ExecuteSchema` invalidates prior statement results, including unread bound
  parameter rows.
- Native archives package an explicitly supplied library under the filename the bundled driver
  manifest expects, whatever the source file's name.
- The REST conversion window counts retained row and nested-value allocations, including spare
  capacity, so uneven pages with a few wide rows no longer evade its memory accounting.

## [0.1.1] - 2026-11-05

### Fixed

- The source distribution puts its license files where its metadata declares them, so PyPI accepts
  it. Release 0.1.0 has wheels only.

## [0.1.0] - 2026-11-04

First public release, with wheels on PyPI for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
Windows x86-64.

[Unreleased]: https://github.com/fornwall/bigquery-adbc/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/fornwall/bigquery-adbc/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/fornwall/bigquery-adbc/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/fornwall/bigquery-adbc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fornwall/bigquery-adbc/tree/v0.1.0
