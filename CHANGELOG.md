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

- C API calls no longer make a `getpid` system call each to detect `fork`. On x86-64 Linux that
  was most of their overhead: setting or getting an option takes about a fifth of the time.
- The native library is 3% smaller (250 KB on x86-64 Linux): IAM impersonation refusals decode
  without the SDK's generic `google.rpc.Status` parser, URL parsing links no Unicode host-name
  tables, and `adbc_core`, `tokio-util` and `flatbuffers` compile for size.
- A query whose request exceeds BigQuery's 10 MB cap, such as very long SQL or a large bound
  value, still returns `INVALID_ARGUMENT`, but the message now names the cap after
  "Payload Too Large (HTTP 413)" and says to shorten the SQL, bind smaller values, or bulk-ingest
  large values.
- A manual-commit transaction that needs a new session creates it with its `BEGIN TRANSACTION`
  instead of a separate query, so the first statement on a connection with autocommit disabled
  (the Python DB-API default) makes one request fewer: about 0.65 s faster in measurements
  (median 1.24 s instead of 1.90 s for `SELECT 1`). A cancel during that `BEGIN` now returns
  `CANCELLED` at once and ends the session.
- A credential URL option or a URL in a credential document, `credential_source` included,
  naming a non-ASCII or percent-encoded host is refused; write an internationalized host in
  Punycode (`xn--`). `bigquery.allow_untrusted_endpoint` no longer skips parsing those options and
  a document's destinations, so a malformed one fails when the connection opens.
- A `json_credential_string` credential that does not begin a JSON object, usually a file path,
  is refused with how to select `json_credential_file` instead.
- The dbt installer accepts dbt's Rust target triples with `--target`, and rejects unknown
  options and missing values before installing.
- A gRPC `OUT_OF_RANGE`, such as a Storage Write append past the end of its stream, returns
  `INVALID_STATE` rather than `INVALID_ARGUMENT`, as the same condition reported by a stream
  commit already did.
- `import bigquery_adbc` takes about 60% less time (27 to 10 ms on Linux x86-64), since
  `__version__` reads the package metadata on first use.
- Python's `dbapi.connect()` takes `backend` as its only keyword beyond `autocommit`, so a
  misspelled keyword raises `TypeError` before the driver loads, instead of after opening the
  database and connection.
- A bulk-ingest `Date64` value that is not a whole day is refused with its row in the bound stream,
  like every other refused value.
- The patched dbt is built from dbt's `main` at `ef0aa4db3a` (2026-11-11).
- The driver uses `google-cloud-bigquery` 1.0.0 for Storage Read and Write, which
  `ADBC_INFO_VENDOR_VERSION` reports.
- An Arrow `Null` parameter annotated `ARRAY<...>`, `STRUCT<...>` or `RANGE<...>` still returns
  `NOT_IMPLEMENTED`, but the error now says to bind a null `List` or `Struct` field of that type
  instead of calling the type unimplemented.
- `GetObjects` and `GetStatistics` with a catalog filter naming another project no longer check
  that the project exists before walking it, saving a request whenever the walk finds a dataset.
  A walk that finds nothing still checks, so a missing project still yields no catalog row.
- Executing a `?` placeholder with no parameters bound still returns BigQuery's
  `INVALID_ARGUMENT` "Positional parameters are not supported", but the error now says to bind
  the parameters, since positional parameters are the default.
- When BigQuery refuses a query because of a `STRING`, such as inserting into an `INT64` column,
  and a bound parameter came from an unannotated Arrow `Null` field (a DB-API `None`), the error
  names that field and says to `CAST` its placeholder, since such a field binds as a `STRING` NULL.

### Fixed

- A REST result row of up to BigQuery's 20 MiB limit is read, rather than failing with
  `INVALID_DATA` as "over the driver's fixed 67108864-byte limit", when its JSON exceeds 64 MiB:
  BigQuery spells each control character, `<` or `>` in six bytes, so such a row can arrive as a
  120 MB response. The limit on a buffered response is now 128 MiB.
- A token exchange refused permanently, such as an impersonation before its IAM grant propagates or
  a revoked OAuth refresh token, no longer fails every later request from the database: each
  request asks again with a rebuilt credential, as the Go driver does, so a refusal resolved after
  the connection opened lets the next request through.
- A process forked while another thread was in a C API call no longer hangs in the child: the
  child could inherit the driver's handle lock held, so creating or releasing any handle there,
  including a new database, waited forever.
- A credential document that a URI supplies through `AuthCredentials`, inline or by path, can no
  longer name a `credential_source.executable`, which the auth library runs when
  `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`, nor a `credential_source.file` other than a
  regular file of at most 1 MiB, which the library reads whole at every token fetch (so
  `/dev/zero` exhausted memory). Set `bigquery.auth.credentials` as an option to go beyond this.
- A `bigquery.auth.credentials` file over 1 MiB is refused after reading that much instead of
  being read whole, so a path such as `/dev/zero`, which a URI's `AuthCredentials` can name, no
  longer exhausts memory.
- A credential file path a URI's `AuthCredentials` supplies must be a regular file, so a URI can
  no longer block the connection forever on a pipe, terminal or `/dev/stdin`. A path set as
  `bigquery.auth.credentials` can still name a pipe.
- Cancel no longer fails with `UNKNOWN` and BigQuery's 404 after stopping a job submission before
  the job was created, when that submission ended just as the cancel claimed it.
- A named parameter longer than BigQuery's 127-character limit is refused before sending when its
  name has characters beyond U+FFFF, which BigQuery counts twice.
- In the Rust API, `bind` and `bind_stream` refuse a schema nested more than 32 levels with
  `INVALID_ARGUMENT` and keep the previous binding, as the C API does, rather than accepting it
  and failing the next execute.
- A partition stream that fails, or is cancelled, before its first results read no longer asks
  BigQuery to cancel the partition's already finished job. That request could fail for a reader
  without permission to cancel the job, making `AdbcConnectionCancel` return `UNKNOWN`.
- Releasing a handle or result stream that a `fork` child inherited leaks it instead of running
  its destructors, which could wait forever on locks held by threads the child does not have.
- The `AdbcError` that `AdbcErrorFromArrayStream` returns stays valid when other stream callbacks
  run before the caller reads it; Rust's aliasing rules had invalidated the pointer.
- A connection released and recreated on the same `AdbcConnection` while another thread's
  `AdbcConnectionInit` waited on BigQuery can be cancelled; it had received the first connection's
  cancellation handle, so `AdbcConnectionCancel` found nothing to cancel.
- With `bigquery.query.dry_run`, the dbt authorized-view, table metadata and python-model
  operations return `NOT_IMPLEMENTED` before any request instead of making their change, as a
  table copy already did.
- A python-model upload refuses a Cloud Storage bucket of `.` or `..`, which addressed a
  different resource, and one containing `/`, `?`, `#`, `%`, `\` or whitespace, which no bucket
  name has.
- A job submission none of whose attempts could connect (DNS, TCP or TLS) reports
  `bigquery.job.verdict` `not_created` instead of failing to look for its job, and such a
  `jobs.query` no longer says BigQuery may have run the statement.
- A dbt 2.0 Dataproc or Vertex AI submission cancelled while it was connecting, whose connection
  then failed, reports `CANCELLED` instead of the connect failure and no longer says the work may
  have started.
- A dbt 2.0 Dataproc or Vertex AI submission that could not connect no longer says the work may
  have started, and a Vertex AI notebook execution is resent after such a failure, since nothing
  was sent.
- A `create` bulk ingest whose commit is refused says it kept the table it created and suggests
  retrying with `append`, rather than suggesting a retry that would fail on that table. A commit
  that ends without an answer may have committed every row, so it says to check the table first.
- When looking for a job submitted without `bigquery.location` fails partway, for example on a
  transient `jobs.get` failure, the error now says the job may still run and names it in
  `bigquery.job_id`, as it does when the job is found nowhere. So do a submission with a location,
  including a dbt table copy, whose job a 409 or a lost answer leaves unfetched, and a dbt seed
  load whose job a lost answer leaves unfetched. Such a failure reads `failed to submit`, not
  `BigQuery rejected`, and when the job a 409 names cannot be fetched it is `UNKNOWN` instead of
  `ALREADY_EXISTS`. A seed's 409, as for a `WRITE_EMPTY` load into a table with rows, still
  reports `ALREADY_EXISTS`: BigQuery creates no job for it.
- A dbt 2.0 seed load or table copy whose wait for its job ends on failed polls links the job and
  names it in `bigquery.job_id`, as a query's wait does.
- A dbt 2.0 seed upload that could not connect, or that BigQuery refused as rate-limited, is sent
  again from the start of its file, as other job submissions are, instead of failing the seed.
- An append to a view, snapshot or other object that is not a table reports that, rather than
  what one of its columns makes of a bound all-null field.
- A Storage Read stream that BigQuery fails with `OUT_OF_RANGE`, for a value Arrow cannot hold
  such as an `INTERVAL` past 2,562,047 hours, reports `INVALID_DATA`, as REST does for the same
  value, instead of `INVALID_ARGUMENT`.
- Named parameters whose names differ beyond ASCII case, such as `é` and `É`, bind as the separate
  parameters BigQuery reads them as, instead of being refused as duplicates.
- A bulk-ingest dry run refuses a schema that leaves an append request no room for rows, as the
  ingest it validates does.
- When an ingest's table creation reached BigQuery but failed without its verdict, such as a lost
  response, the error says the table may exist, and in `create` mode suggests retrying with
  `create_append`. A creation that could not connect says nothing more.
- The `.tar.gz` native release archives name root as their files' owner, with mode 0755 for the
  library and 0644 for the rest, instead of the build machine's user and modes: extracting as root
  kept that user, such as uid 1001 on Linux, as the library's owner.
- The dbt installer takes no option or temporary path from the environment. A `td` variable named
  a directory it deleted on exit, even when the installed dbt was already current, and `prefix`,
  `dest`, `version`, `target` or `package` changed what it installed where.

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
