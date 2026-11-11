# Go driver comparison

Differences from the active [Go BigQuery ADBC
driver](https://github.com/adbc-drivers/bigquery/tree/main/go), the reference implementation, in its
default `bigquery.query.job_creation_mode=required` (G87 and G88 cover its opt-in mode). "The fork"
is [`dbt-labs/bigquery-adbc`](https://github.com/dbt-labs/bigquery-adbc), the Go fork [dbt
2.0](DBT.md) loads; it usually behaves like Go, and its own differences are in [DBT.md](DBT.md).
"Driverbase" is [`driverbase-go`](https://github.com/adbc-drivers/driverbase-go), the library Go
builds on. A refused option returns `NOT_IMPLEMENTED` when set and `NOT_FOUND` when read. Entry IDs
are stable; other documents and code comments cite them.

## Summary

| Area | Main differences |
| --- | --- |
| [Bulk ingestion](#bulk-ingestion) | Storage Write only, rejected inside transactions, validated before the target changes. Go also supports load jobs, its default. |
| [Query parameters](#query-parameters) | Type support, precision checks, parameter-schema inference, and ordering of bound executions. |
| [Query execution and results](#query-execution-and-results) | `ExecutePartitions`, `jobs.query` for ordinary queries, REST rows or Storage Read, job recovery. No inline Arrow or job-default streams. |
| [Errors and diagnostics](#errors-and-diagnostics) | Some status mappings; structured details and SQLSTATEs; plain-text logs. |
| [Transactions and sessions](#transactions-and-sessions) | Lazy next transaction, snapshot isolation, session ended on close. |
| [Metadata](#metadata) | Filter matching, null fields, statistics and schema metadata; other projects are discoverable. |
| [Configuration and authentication](#configuration-and-authentication) | Option scopes, validation, credential handling, URI replacement; no Legacy-SQL-only options. |
| [C interface and object lifecycle](#c-interface-and-object-lifecycle) | Init entrypoints only; input and handle-state checks. |

## Bulk ingestion

### G1. Bulk ingest is rejected inside a transaction

With autocommit disabled, bulk ingest returns `INVALID_STATE` before sending a request or consuming
bound data. Go ingests outside the transaction, so the rows survive a rollback that reports success.
Neither driver can make ingestion transactional: Storage Write streams carry no session, and
BigQuery refuses permanent-table DDL inside a transaction.

### G2. Bulk ingest defaults to Storage Write, and load jobs are not implemented

`bigquery.bulk_ingest.method` defaults to `storage_write`, its only supported value; `load`, or
setting the option on a database, returns `NOT_IMPLEMENTED`. Go defaults to `load` (a temporary
Parquet file and a load job), so a client that leaves it unset sees different latency and different
DML restrictions on recently streamed rows.

### G7. Ingest schema mismatches return `ALREADY_EXISTS`

For `append` and `create_append`, a bound schema the existing table cannot accept returns
`ALREADY_EXISTS` before a stream opens, as adbc.h requires, naming every column at fault
([rules](REFERENCE.md#bulk-ingest)). Decimal and length-limited string values are also checked,
because Storage Write silently rounds and stores overlong values. Go never compares schemas and
reports the later append failure as `IO`.

### G11. Arrow interval bulk ingestion is supported

All three Arrow interval layouts ingest into `INTERVAL` (day-time via month-day-nanosecond, UP-19;
sub-microsecond parts truncate toward zero). Go cannot ingest intervals, including the interval
schemas it returns.

### G22. Oversized Arrow ingest batches are split into bounded append requests

Batches are normalized and sent in slices of about 9 MB, under BigQuery's 10,000,000-byte request
limit, sized by the normalized output; only a single row too large for one request fails, locally,
with `INVALID_ARGUMENT`. Appends carry offsets on a pending stream: the oldest failed append gets
three retries, queued successors are replayed at their offsets, each later failure starts a new
budget (so an append may be sent more than four times), and a replay answered `ALREADY_EXISTS`
counts as written. Go sends each batch as one request, so a large batch fails at the service, and
retries only a first append on `NotFound`, so a transport failure mid-ingest fails it.

### G23. The first ingest batch is validated before target-table mutation

The first non-empty batch, including RANGE bound ordering and request-size limits, is validated
before `create` or `replace` changes the target; a schema with no columns is refused before any
request. Go's driverbase prepares the table first, so an invalid batch can leave a new or recreated
table. In both drivers a later batch can fail after preparation, and a failed ingest keeps any table
it created.

### G24. Storage Write ingestion supports exact Arrow Date64 values

Whole-day Date64 values ingest as `DATE`; others return `INVALID_DATA`. Go's Storage Write path does
not support Date64.

### G29. The bulk-ingest compression option is refused

`bigquery.bulk_ingest.compression` is refused at every scope. Go accepts `none`, `lz4` and `zstd`
but never applies them, so appends are uncompressed in both. The one inert Go option accepted here
is `bigquery.query.link_failed_job`, which dbt 2.0 sets on every statement; failed jobs carry their
console link regardless (G51).

### G30. A dry run on a bulk-ingest statement validates instead of writing

With `bigquery.query.dry_run`, an ingest checks the target and bound schema with one read-only
lookup and reports zero rows without writing, keeping the data bound. Go and the fork ignore the
option and write the data.

### G38. Rejected ingest rows are named by the caller's own row number

Storage Write row errors name each rejected row by its number in the caller's bound stream and its
position in the append, with BigQuery's code and message. Go never reads `row_errors` and reports
only the RPC status message.

### G53. Lossless numeric widening accepts more Arrow input types

Ingest and parameters widen `Int8`, `UInt8`, `UInt16` and `UInt32` to `INT64` and `Float16` to
`FLOAT64`, which Go rejects. Ingest, like Go, rejects `UInt64` (cast it to `Int64` or
`Decimal128(20, 0)`); a `UInt64` parameter binds as `INT64` and fails only above `i64::MAX`. A
`Float32` or `Float16` parameter binds as its exact value (`0.1f32` as `0.10000000149011612`), where
Go sends the shortest decimal (`0.1`).

### G63. Storage Write ingestion uses the configured credentials and Storage endpoint

Storage Write uses the configured credentials, impersonation, quota project and
`bigquery.storage_endpoint`. Go's Storage Write client ignores them and uses ambient ADC against
`bigquerystorage.googleapis.com`.

### G71. `replace` ingestion avoids a separate table deletion

A different schema is recreated with `CREATE OR REPLACE TABLE` before uploading; a matching one is
truncated after every row is on the finalized pending stream, just before commit, so an upload
failure keeps the old rows. A failure after recreation or truncation can still leave an empty table,
which the error reports (or, after an ambiguous cancellation, reports as possible). Go deletes then
creates the table, so a failure in between leaves no table; `replace` here needs permission to run
query jobs.

### G78. Storage Write's control-plane calls are retried, and their answers are read

Stream create, get, finalize and commit calls are retried on transient statuses (3 attempts, 200 ms
doubling; UP-83), commit `stream_errors` fail the ingest (except `STREAM_ALREADY_COMMITTED` on a
replay), and finalize and commit ignore cancellation for up to 60 seconds so a cancel cannot report
failure for committed rows. Go retries only `CreateWriteStream` on `NotFound` and discards the
commit response, so a declined commit is reported as success.

### G79. An ingest target that exists but is not a table is refused before anything is written

A view, materialized view, external table or snapshot target returns `INVALID_ARGUMENT` naming its
type. Go treats it as a table, so `append` reaches the service and `replace` deletes the object.

### G92. Array decimal elements keep their declared scale on ingest

Storage Write reads `ARRAY<NUMERIC>` and `ARRAY<BIGNUMERIC>` elements at scale 9 or 38 whatever
their Arrow scale (UP-89), so ingest rescales them. Go sends them unchanged, so `0.01` in a
`List<Decimal128(10, 2)>` is stored as `0.000000001`.

### G93. Repeated TIMESTAMP ingest fields are refused before any request

An `ARRAY<TIMESTAMP>` field at any depth returns `NOT_IMPLEMENTED` before a table is created or
written, since Storage Write fails every such append (UP-90). Go's Storage Write path creates the
table first and reports `IO`.

## Query parameters

### G9. Query parameters preserve rich logical types

Fields with `BIGQUERY:type` metadata for `JSON`, `GEOGRAPHY` or `RANGE` bind as those types, so a
result schema binds back unchanged, and a `Null` field binds as a NULL of its metadata's type. Go
sends strings as `STRING` and structs as `STRUCT`, so `RANGE` fails, and every `Null` as a `STRING`
NULL, which BigQuery refuses where the SQL needs another type (`UPDATE t SET n = ?` on `INT64`).
An unannotated `Null` field binds as a `STRING` NULL here too, but such a refusal names each such
field.

### G10. Arrow interval query parameters are supported

All three interval layouts and `Duration` bind as `INTERVAL` (a duration is the time part, so two
days is `0-0 0 48:0:0`); a duration beyond BigQuery's 87,840,000 hours fails locally. Go rejects
both. Results beyond 2,562,047 hours, the most an Arrow interval holds, fail on REST and on Storage
Read alike.

### G17. Out-of-precision decimal values are rejected locally

Decimal parameters and ingest values outside their declared precision, nested ones included, return
`INVALID_DATA` before any request, naming the field but not the value. Go formats such a parameter
to the declared digits, so it can send a rounded value, and passes ingest values unchecked.

### G18. Empty composite parameter support

Empty ARRAY and zero-field STRUCT parameters are sent as `[]` and `{}`, since BigQuery reads an
empty `arrayValues` as NULL; Go fails with `bigquery: nil parameter`. Arrays with NULL elements
return `INVALID_DATA`, and directly nested arrays return `NOT_IMPLEMENTED` advising a STRUCT
wrapper; Go checks neither locally.

### G21. Decimal types are selected by declared value domain

Parameters and ingest choose `NUMERIC` when the precision and scale fit 29 integer and 9 fractional
digits, `BIGNUMERIC` when they fit 38 of each, and fail locally otherwise; all four Arrow decimal
widths and negative scales are accepted. Go chooses by Arrow width (`decimal128(38, 20)` is sent as
`NUMERIC`), maps both widths to `NUMERIC` for ingest, and lacks `Decimal32` and `Decimal64`.

### G50. Bound query results stream in parameter order

With bound parameters, a row-returning `ExecuteQuery` (including `CALL`) returns after the first
row's query and runs each later row once the previous result is read, so results keep parameter
order and cancellation or failure stops later rows. DML, DDL and explicit-destination queries run
every row first, as in Go ([bound parameters](REFERENCE.md#bound-parameters)). Go buffers every
row's results in one bounded channel before returning, so a large result blocks `ExecuteQuery` until
cancelled, and results can arrive out of order.

### G61. Parameter schemas are inferred by a dry run

`GetParameterSchema` infers names and types with a cached dry run; named parameters come back
lowercased and sorted, as BigQuery reports them. Go returns `NOT_IMPLEMENTED`.

### G70. `adbc.statement.bind_by_name` is accepted as a parameter-mode alias

`adbc.statement.bind_by_name`, the SQLite driver's key, aliases `bigquery.query.parameter_mode`
(`true` is `named`, `false` is `positional`). Go returns `NOT_IMPLEMENTED`.

## Query execution and results

### G8. Partitioned execution is implemented

`ExecutePartitions` runs the query as a job and returns a compact job reference that `ReadPartition`
can read repeatedly; bound parameter rows become separate partitions when their schemas agree. Go
returns `NOT_IMPLEMENTED`.

### G25. REST result conversion is cooperatively cancelled and driver-wide CPU bounded

Small resident REST results convert lazily on the first read; larger or paged results convert in
blocking tasks that release or cancellation stop mid-conversion. Conversion shares driver-wide
permits equal to the available parallelism, so `bigquery.query.prefetch_concurrency` cannot
oversubscribe the CPU, and pending input is capped at 64 MiB. REST batches end at page boundaries
and can hold fewer than 1,024 rows; Go reads through Storage Read and keeps its batch sizes.

### G26. Storage Read uses one stream by default

One stream preserves any `ORDER BY`; `bigquery.query.unordered_read`, which Go lacks, reads all
granted streams in parallel. Go's SDK scans the SQL for a top-level `ORDER BY`, and the scan fails
on unbalanced parentheses, so `SELECT x FROM t WHERE s = '(' ORDER BY x` returns rows out of order.
Batch row counts are checked, and transient failures and stalls resume at the next unread row; Go's
SDK does neither.

### G27. Resolving the result table costs one round trip instead of two

Before a Storage Read this driver fetches the job once and reuses the query response's schema; Go's
SDK fetches the job again and then the destination table.

### G31. Result-schema job statistics are limited to the keys a consumer reads, and never fabricated

Result schemas carry `BIGQUERY:query_id` and six statistics under Go's keys and PascalCase aliases
([result metadata](REFERENCE.md#result-schema-metadata)); Go emits about 30, writing `"0"` for
unreported ones where this driver omits them. The statement type always comes from BigQuery, which
can classify jobless queries differently (`SET @@dataset_id` then `SELECT` is `SELECT` without a job
and `SCRIPT` with one). Go's Storage Read adds a `jobs.get` and a `tables.get` per result.

### G32. Row counts: DDL stays unknown, and DML never reports the table's size

DDL reports `-1` rows affected, where Go reports `0`; DML reports `numDmlAffectedRows`, where Go's
`ExecuteQuery` returns the target table's size afterwards. Dry runs, scripts that do not end in a
query and destination-table queries also report `-1`, where Go reports a dry run's bytes processed
([row counts](REFERENCE.md#row-counts)).

### G45. Scripts use the ordinary result path instead of interpreting an SDK failure

A script returns BigQuery's result for it, or an empty schema and zero rows, with `SCRIPT` metadata.
For a script without a result, Go's SDK reads the newest child query's table instead: Go and the
fork hang on a query followed by DML, DDL or `ASSERT`, and Go matches the SDK's error message when
no child query ran. A denied
Storage Read (`PERMISSION_DENIED`) falls back to REST and is remembered for the billing project,
where Go returns `UNAUTHORIZED`, and an `INVALID_ARGUMENT` refusal falls back for that result only,
where Go errors.

### G46. Ordinary queries use `jobs.query`; job-only configurations use `jobs.insert`

Ordinary GoogleSQL runs through `jobs.query` with optional job creation and a ten-second wait, so a
short query may complete without a job; partitioned execution requires a job, and dry runs,
destination tables and non-default dispositions or priority use `jobs.insert`. Go uses `jobs.insert`
by default (G87). A destination query returns the schema and no rows, where Go reads the table back.
Complete first responses stay on REST and any continuation selects Storage Read before rows are
delivered, so REST's 20 MiB row limit only applies when Storage Read is disabled or unavailable; Go
uses Storage Read for every nonempty result.

### G47. Submission retries preserve identity, and named jobs can be recovered

Each execution keeps one request or job ID across retries, and an ambiguous `jobs.insert` or a
replayed `jobs.query` answered with a 409 recovers that job, where Go's SDK errors (BigQuery may
ignore `requestId` for read-only queries, so recovery is not guaranteed there, and ignores it for
transaction control, which is therefore submitted as named jobs). Jobs confirmed failed with a
transient reason are rerun with a fresh ID up to three times, except scripts, `CALL` and statements
of unknown type; Go never reruns a failed job. Waiting for a job gives up after ten minutes of
consecutive failures, where Go polls until the caller's context ends.

### G48. Jobs submitted without `bigquery.location`

Without `bigquery.location`, BigQuery infers a `jobs.insert` job's location (G46), as in Go. When
the answer is lost, the driver searches the 409's location, location-less `jobs.get`, the dataset's
location and finally a bounded `jobs.list` across regions; if that fails, the error names the job as
possibly still running, where Go's SDK errors at once. dbt 2.0 seed loads and table copies run in
the destination dataset's location, from a cached `datasets.get` (at most 32 per database), where
the fork lets BigQuery infer it.

### G49. Remote job cancellation and operation lifetime

As in Go, cancellation, supersession, reader release or an early end stops local work and requests
cancellation of an unfinished job; remote cancellation can be delayed by the ten-second completion
wait (G46), and queries that finish during it cannot be stopped. Unlike Go, a cancel during a
pending submission lets it finish in the background, then cancels the job it names or G48 finds,
and a failed remote cancel is retried. A failed explicit cancel returns `UNKNOWN` within 30 seconds; a
`jobs.query` whose retries ended without a job reference cannot be cancelled, and its error says the
statement may still have run.

### G52. Canonical statement progress reports response-boundary work without extra polling

`adbc.statement.exec.progress` reports processed bytes from each query response, about every 45
seconds while a query runs, and `adbc.statement.exec.max_progress` is `0` until the total is known;
reading either never waits or sends a request. Go returns `NOT_FOUND` for both.

### G58. Arrow IPC supports a BigQuery-specific subset

Storage Read, Storage Write and the dbt 2.0 CSV seed schema use an in-tree Arrow IPC reader and
writer (`src/arrow_ipc_bigquery`) for BigQuery's subset, where Go uses Arrow Go's. A Storage Read
schema outside the subset falls back to REST before any rows are read.

### G87. Optional jobs use REST rows here; Go adds inline Arrow and job-default streams

Go's opt-in mode reads jobless results as inline Arrow and job-backed results from the job's
`_default` stream, avoiding the `bigquery.readsessions.create` permission. This driver reads jobless
results as REST rows (one response, at most 100,000 rows) and large job-backed results through a
table-backed Storage Read session. Go's mode options `bigquery.query.job_creation_mode`,
`bigquery.query.results_format` and `bigquery.query.arrow_serialization_options.buffer_compression`
are refused.

### G88. Inline Arrow has a different schema and metadata contract

Go's inline Arrow schemas come from the service, normalized by `unmangleBigQueryArrowType`, with
`BIGQUERY:type` guessed where the BigQuery schema is unavailable; the metadata adds
`BIGQUERY:job_creation_reason` but omits `BIGQUERY:query_id`, the statement type and slot
milliseconds. This driver derives fields from BigQuery's `TableSchema` on every path. Inline Arrow
also fails with HTTP 400 `jobInternalError` for a collated column (`SELECT COLLATE('hello',
'und:ci') AS s`, also nested), which REST and this driver's Storage Read return with `Collation`
metadata.

### G91. Reader construction failures release owned resources

A failed Storage Read reader construction drops owned resources. Go's `ipcReaderFromArrowIterator`
leaks its IPC reader on later `buildField` or metadata failures; the fork fixes this.

## Errors and diagnostics

### G51. Query errors carry ADBC-faithful status and structured details

Stable reasons such as `accessDenied`, `invalidQuery` and `notFound` keep Go's statuses, and `IO` is
reserved for failures BigQuery documents as retryable. This driver also maps HTTP 501 to
`NOT_IMPLEMENTED` and `responseTooLarge`, `resourcesExceeded` and `bytesBilledLimitExceeded` to
`INVALID_ARGUMENT`, and keeps an unavailable ADC metadata server `UNAUTHENTICATED`
([errors](REFERENCE.md#errors)). Errors carry structured details and, for `invalidQuery` and
`accessDenied`, SQLSTATE `42000`; Go's carry only a status and message. Both end a failed job's
message with `(Query: <console URL>)`. These statuses differ:

| Condition | This driver | Go driver |
| --- | --- | --- |
| Backend, rate-limit and quota reasons | `IO` | `INTERNAL` |
| `tableUnavailable` | `IO` | `NOT_FOUND` |
| `resourceInUse` | `INVALID_STATE` | `INTERNAL` |
| `stopped` by `bigquery.query.job_timeout` | `TIMEOUT` | `CANCELLED` |
| HTTP 401 | `UNAUTHENTICATED` | `UNAUTHORIZED` |
| HTTP 403 without a recognized reason | `UNAUTHORIZED` | Call-site default, usually `INTERNAL` |
| Connection, transport and transient token-exchange failures | `IO` | `IO` for discrete OAuth transport failures, otherwise the call-site default |
| gRPC `OUT_OF_RANGE`, such as a Storage Write append past its stream's end | `INVALID_STATE`; `INVALID_DATA` for a Storage Read value Arrow cannot hold | `INVALID_ARGUMENT` |

### G86. Diagnostics use timestamped plain text and a fixed stderr sink

Both read `ADBC_DRIVER_BIGQUERY_LOG_LEVEL`. This driver reads it once per process, is silent when it
is unset, and writes `TIMESTAMP [bigquery-adbc] LEVEL message` lines to stderr
([diagnostics](REFERENCE.md#diagnostics)). Go reads it at every `AdbcDatabaseInit`, writes slog JSON
to stderr or a temporary file (`ADBC_DRIVER_BIGQUERY_LOG_SINK=tempfile`), logs errors when unset,
and accepts a custom logger from native Go callers.

## Transactions and sessions

### G16. The next transaction begins lazily after commit or rollback

A commit or rollback sends nothing more; the next query begins the next transaction, in a new
session if BigQuery ended the old one. Go sends `BEGIN TRANSACTION` right after, so a failed `BEGIN`
reports an error for a commit that succeeded. A new session is created by the `BEGIN` itself, one
request fewer than Go's separate `SELECT 1`. A cancel, or a minute without an answer, ends only the
wait for that `BEGIN`: the driver ends the session once BigQuery names it, and a `BEGIN` that fails
leaves any session it created to expire. Otherwise `BEGIN`, `COMMIT` and `ROLLBACK` run to a
verdict as named jobs, ignoring cancellation for up to a minute, and only a refused `BEGIN` or
`COMMIT` keeps the session; Go cancels them with its context and keeps the session after any
failure. Without `bigquery.location` the session uses the default dataset's location, where Go's
lands in the US.

### G40. Snapshot isolation is an accepted and reported isolation level

The isolation level accepts `default` and `snapshot` as no-ops and reads back snapshot, which is
what BigQuery transactions provide; other levels return `NOT_IMPLEMENTED`. Go refuses every value.

### G64. Closing a connection ends its transaction and session

Releasing a manual-commit connection rolls back an open transaction and ends the session with `CALL
BQ.ABORT_SESSION`; a statement that outlives it returns `INVALID_STATE` instead of autocommitting.
Go leaves the work and session to BigQuery's idle timeout.

## Metadata

### G33. Unsupported or inapplicable XDBC column fields remain null

`GetObjects` fills XDBC column fields only with real values; unknown sizes, SQL type codes and
reference scopes are null. Go fills unknown sizes and digits with zero, puts an Arrow type ID in
`xdbc_data_type`, and points `xdbc_scope_*` at the column's own table. `REPEATED` columns are
non-nullable here, since BigQuery arrays cannot be NULL; Go and the fork report them nullable.

### G34. Metadata listings with no configured catalog return an empty result

A connection can open without a project and set `adbc.connection.catalog` later; until then only a
catalog filter naming a project lists anything. Go refuses to open it (`[bq] ProjectID is empty`).

### G35. `GetStatistics` row counts come from `tables.get`

`adbc.statistic.row_count` is BigQuery's exact `numRows` as `int64`, or an approximate `float64`
while a streaming buffer holds rows or for a materialized view ([table
statistics](REFERENCE.md#table-statistics)). Go always reports an approximate `float64` from
`INFORMATION_SCHEMA.PARTITIONS`, and also publishes `bigquery.statistic.total_billable_bytes` (key
1025), which only that view reports.

### G36. `GetInfo` answers the XDBC range and never omits a recognized code

`GetInfo` answers 26 codes, including 15 XDBC codes that JDBC and ODBC bridges read; Go answers 9.
An unrecognized requested code is omitted, as adbc.h requires, where Go returns a null row, and
`VendorArrowVersion` is null, where Go reports a placeholder.

### G39. A view without a schema is an error from `GetTableSchema`

A view whose SQL no longer compiles has no schema; `GetTableSchema` returns `INVALID_DATA` naming
it, where Go returns a schema with no fields.

### G55. `GetObjects` at table depth defaults to null constraints without describes

At tables depth, rows come from `tables.list` alone, with null `table_columns` and
`table_constraints` and no per-table request, as in the ADBC C++ framework. Go calls `tables.get`
for every table at any depth. Column depth, or the fork's
`bigquery.get_objects.skip_table_metadata=false` (default `true` here), returns constraints.

### G56. Metadata filter patterns are matched case-sensitively

Filters match case-sensitively, so `table_name="EVENT%"` does not match `events`; Go's driverbase
matches case-insensitively. A wildcard-free filter becomes a direct lookup (G74), which BigQuery
resolves case-sensitively except in a dataset created with `isCaseInsensitive`, as in Go.

### G60. Hidden datasets are discoverable and retain their table statistics

`datasets.list` is sent with `all=true`, so hidden datasets (names starting with `_`) are listed
with their tables' statistics. Go lists them too but returns no statistics, because its
`INFORMATION_SCHEMA` query cannot see them.

### G62. Metadata discovery skips inaccessible tables without suppressing service restrictions

Discovery skips a table or dataset only on HTTP 403 with `accessDenied` or no reason; other 403s,
such as rate limits or a disabled API, fail the call. Go skips every 403, so throttling can silently
shorten a listing. A tables-depth listing (G55) can include tables whose metadata is inaccessible,
which Go omits.

### G65. Table-schema metadata describes the table, not its storage accounting

`GetTableSchema` metadata holds up to 35 table-definition keys from `tables.get(view=BASIC)` in the
fork's spellings, which dbt 2.0 reads ([DBT.md](DBT.md#metadata)); only `Type` is always present. Go
and the fork use the default view, which also collects storage statistics. Go writes
`BIGQUERY:<snake_case>` keys, lacks spellings dbt reads such as `Labels` and
`RequirePartitionFilter`, and adds storage counters and other fields that here come from
`GetStatistics` and `GetObjects`.

### G66. Field metadata omits what the Arrow field already says

Field metadata has `BIGQUERY:type`, `Type` and the extension keys, as in Go, but no `Required`,
`Repeated` only when true, `Description` and `Collation` only when non-empty, and `MaxLength` only
when declared, where Go writes `"0"` ([supported types](REFERENCE.md#supported-types)). Decimal
columns add `BIGQUERY:precision` and `BIGQUERY:scale`, and `PolicyTags` uses BigQuery's
`{"names":[…]}` rather than Go's `{"Names":[…]}`. A column default is `GetObjects`'
`xdbc_column_def`, not Go's `DefaultValueExpression` key, and a list's item field carries the
element's `BIGQUERY:type`, which Go's lacks.

### G69. An empty `table_type` list matches no tables

An empty, non-NULL `table_type` list returns no tables and sends no request, because adbc.h gives
only NULL the meaning "any type"; Go's driverbase treats it like NULL.

### G72. A table or dataset dropped mid-listing is skipped, not reported as a missing dataset

A table or dataset that returns 404 mid-listing is skipped. Go skips a vanished table, but a dataset
dropped between `datasets.list` and its `tables.list` fails the call with `NOT_FOUND`.

### G73. An unrecognized table type is reported as `TABLE` rather than passed through

Only the five types `GetTableTypes` publishes are reported; any other or missing value becomes
`TABLE` with a `debug` diagnostic. Go passes the value through, and an unknown type would make dbt
2.0 panic (UP-79).

### G74. Default metadata patterns cannot take an exact dataset lookup for a name containing `_`

adbc.h makes `_` a single-character wildcard with no escape, so a dataset filter containing `_`, as
dbt names usually do, walks `datasets.list` instead of taking a direct lookup, as every Go filter
does. The fork's connection option `bigquery.get_objects.disable_wildcards=true` makes filters
literal, so such names get direct lookups.

### G75. A catalog filter naming one project enumerates that project

A wildcard-free catalog filter naming a valid project lists it. Only a walk that finds nothing
sends a one-result `datasets.list` to tell a missing project from an empty one. Go lists only the
connection's project. In both drivers a wildcard filter, or none, covers only the current project
([metadata catalogs](REFERENCE.md#metadata-catalogs)).

### G77. `xdbc_type_name` reports the GoogleSQL type, not the REST spelling

`xdbc_type_name` is the GoogleSQL type (`INT64`, `BOOL`, `STRUCT<...>`, `ARRAY<STRING>`), where Go
reports the REST name (`INTEGER`, `BOOLEAN`, `RECORD`) and drops the repeated mode.

### G82. Metadata reads and blind patches retry a bounded number of times; guarded writes never replay blindly

Metadata reads and blind patches retry transient failures up to five attempts; `tables.insert`
retries only rejections that applied nothing, and the authorized-view patch re-reads the access list
([authorized views](DBT.md#authorized-views)). Go's SDK retries reads, inserts and patches on HTTP 500, 502, 503 and 504,
`backendError` and `rateLimitExceeded` until the context ends.

## Configuration and authentication

### G5. Current-schema validation is complete and atomic

Setting the current schema validates the whole dataset ID and updates the connection only after
`datasets.get` confirms it. Go checks only the first character and assigns before the lookup, so a
failed lookup leaves the connection on the rejected dataset.

### G6. Current-catalog assignment validates the project

Setting the current catalog checks the project and updates the connection only on success; Go
assigns it unchecked. Clearing it resolves names in `bigquery.project_id`; Go then addresses an
empty project.

### G12. `bigquery.query.max_billing_tier` is not supported

`bigquery.query.max_billing_tier` is refused, pointing to `bigquery.query.max_bytes_billed`. Go
accepts it, but BigQuery ignores the deprecated `maximumBillingTier`.

### G13. Endpoints outside Google require explicit trust

Non-REST endpoint overrides, token endpoints, URI hosts and destinations in a credentials document
must be HTTPS on `googleapis.com` or a subdomain (or `accounts.google.com` for OAuth) unless
authentication is `anonymous` or `bigquery.allow_untrusted_endpoint=true` ([endpoint
overrides](REFERENCE.md#endpoint-overrides)). A BigQuery REST endpoint option is trusted without
opt-in, as in Go and the fork. Go sends the caller's token to any configured host, and the fork
accepts credential documents naming an emulator or proxy. Driver-issued REST requests refuse
redirects; SDK token exchanges still follow them
([UP-100](UPSTREAM.md#up-100-sdk-token-exchanges-follow-redirects)).

### G15. The batch-count result buffer option is not exposed

`bigquery.query.result_buffer_size` is refused at every scope. Go uses it (default 200) as its
completed-batch channel capacity; this driver bounds retained completed batches at 64 MiB and tunes
overlap with `bigquery.query.prefetch_concurrency`.

### G19. Setting a URI replaces the database configuration

Setting `uri` resets the configuration to defaults before applying the URI; before initialization
the URI is applied first and explicit options override it ([connection
URIs](REFERENCE.md#connection-uris)). Go applies only the fields the URI contains.

### G20. Project identifiers have structural checks only

Like Go, this driver leaves project validity, including project numbers and domain-scoped IDs, to
BigQuery; it refuses only `/`, `\`, control characters, `.` and `..`, which Go stores unchecked.
In qualified table options a domain-scoped project ends at the first dot after its colon, so it can
precede a dataset or a BigLake catalog and namespace. Go splits at every dot, reading
`example.com:project.dataset.table` as project `example` and dataset `com:project.dataset`; the
fork parses domain-scoped projects but rejects `project.catalog.namespace.table`.

### G28. Legacy-SQL-only query options are not supported

`bigquery.query.use_legacy_sql`, `bigquery.query.allow_large_results` and
`bigquery.query.disable_flattened_results` are refused at every scope; Go accepts them. This driver
is GoogleSQL only, and BigQuery applies the latter two only to legacy SQL.

### G41. Authentication and impersonation are database-scoped only

`bigquery.auth_type`, `bigquery.auth.*` and `bigquery.impersonate.*` are database options only,
because connections share their database's clients; on a connection they return `NOT_IMPLEMENTED`.
Go also accepts them per connection.

### G42. Boolean options accept only the two specification spellings, in any case

Boolean options accept only `true` and `false` (ignoring ASCII case). Go's `strconv.ParseBool` also
accepts `1`, `0`, `t`, `T`, `f` and `F`. The driver's own option keys and enum values ignore ASCII
case too, so `bigquery.Query.Priority=batch` works; Go matches both exactly.

### G43. The specification-default no-op options are accepted rather than refused

`adbc.ingest.temporary`, `adbc.connection.readonly` and `adbc.statement.exec.incremental` accept
`false` as a no-op; `true` returns `NOT_IMPLEMENTED`. Go refuses the first two whatever the value.

### G44. The endpoint options accept the fork's spellings as well as the Go driver's

`bigquery.endpoint` also accepts `adbc.bigquery.sql.api_endpoint`, which dbt 2.0 sends, and
`bigquery.storage_endpoint` the fork's `adbc.bigquery.sql.storage_read_api_endpoint`; Go rejects
both. A bare `host:port` means HTTPS here, while Go dials the Storage endpoint over plaintext gRPC,
so Go's emulator setting `localhost:9060` is `http://localhost:9060` here.

### G54. Go durations are rendered in ASCII, without a sub-second fraction

Durations read back in ASCII units and, below one second, the largest whole unit (`1500ns`, `2us`),
where Go gives `1.5µs` and `2µs`. Parsing accepts `us`, `µs` and `μs`.

### G57. `bigquery.project_id` and `bigquery.dataset_id` are unreadable on a statement

On a connection these options read the current catalog and schema, as in Go, and setting them
returns `NOT_IMPLEMENTED`. On a statement, reading them returns `NOT_FOUND` naming
`bigquery.query.default_project_id` or `bigquery.query.default_dataset_id`; Go asks the connection.

### G68. The connection URI rejects contradictory and ambiguous inputs, and cannot run a command

The URI returns `INVALID_ARGUMENT` for unused credential parameters, repeated parameters, user
information, a fragment, extra path segments, and impersonation parameters without a target. Go
ignores or drops these, or folds extra segments into the project ID, and decodes `AuthCredentials`
twice, so an encoded `+` becomes a space. An IPv6 host without a port keeps its brackets here
(`bigquery://[::1]/p` is `https://[::1]:443`); Go drops them and builds `::1:443`, which it cannot
dial. A credential document from `AuthCredentials` that names a `credential_source.executable`,
or a `credential_source.file` that is not a regular file of at most 1 MiB, returns
`INVALID_ARGUMENT`, as does an `AuthCredentials` file path that is not a regular file, so a URI
cannot run a command, read `/dev/zero` or block on a pipe; Go runs the command once the machine sets
`GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`, and reads any file.

### G76. Scopes can shape the credential, not only the impersonated token

`bigquery.auth.scopes` configures service-account, impersonated and external-account credentials,
for example to add Drive access for external tables over Google Sheets; Go always requests
`cloud-platform`. It can also narrow user tokens to a subset of the consent grant ([authentication
scopes](REFERENCE.md#authentication-scopes)).

### G80. A NULL string option value is refused rather than read as `""`

A NULL key or value passed to a string `SetOption` returns `INVALID_ARGUMENT`; Go treats NULL as
`""`.

### G81. Option getters withhold secrets and report unset options as `NOT_FOUND`

`GetOption` returns `NOT_FOUND` for write-only credentials, which Go returns, and for unset options,
where Go returns `""`. `uri` reads back redacted, and the impersonation target, delegates and scopes
read back, where Go returns `NOT_FOUND`.

### G83. A connection's current catalog and schema reach its existing statements

Changing the connection's catalog or schema also affects existing statements that do not set their
own defaults. Go copies the values at statement creation, so a dbapi cursor opened earlier keeps
using the old dataset.

### G89. Impersonated credentials refresh even with an explicit lifetime

An explicit `bigquery.impersonate.lifetime` sets each token's requested lifetime, and tokens still
refresh. In Go any explicit lifetime produces a static token, so the connection fails with HTTP 401
once it expires.

### G90. Configured credentials are never silently replaced or ignored

With explicit credentials plus an impersonation target, the configured credential is the
impersonation source and `bigquery.auth.quota_project` applies; Go impersonates from ambient ADC and
drops the quota project, while the fork passes the configured source ([dbt
authentication](DBT.md#database-options)). Credential options the selected auth type does not read,
or impersonation under `anonymous`, return `INVALID_ARGUMENT`; Go ignores them.

### G94. The custom user agent is sent with REST and Storage Read requests only

`bigquery.user_agent` is appended on REST and Storage Read requests but not Storage Write (UP-93).
Go also accepts it per connection and sends it on every request.

### G96. Credential file errors do not quote the path, and reads are bounded

Credential file errors name the option or variable, the OS cause and the remedy, but not the value,
which may be a misplaced key or token. Go's client library errors name the path. A
`bigquery.auth.credentials` file over 1 MiB is refused after reading that much; Go reads any file
whole, so a URI that names `/dev/zero` exhausts Go's memory.

### G97. URL hosts in credential documents must be ASCII

A credential document's `token_uri`, `token_url`, `service_account_impersonation_url`,
`credential_source.url` or AWS source URL naming a non-ASCII host is refused, so an
internationalized host must be written in Punycode (`xn--`); Go's HTTP client converts such a host
to Punycode itself. Without Unicode mapping, a fullwidth spelling of a Google host cannot pass the
[endpoint rules](REFERENCE.md#endpoint-overrides) either.

### G98. JSON credential options take four document types

`json_credential_file` and `json_credential_string` accept `service_account`, `authorized_user`,
`impersonated_service_account` and `external_account` documents. Go hands any type to its auth
library, which also takes `external_account_authorized_user`, the Workforce Identity Federation
user credentials `gcloud auth application-default login --login-config` writes. Here such a file
works only as Application Default Credentials, where the bundled SDK ignores
`bigquery.auth.scopes`.

### G135. A refused token exchange is asked again of a rebuilt layer

After a token exchange the auth library treats as permanent, such as an IAM refusal or
`invalid_grant`, the next request rebuilds the refused layer from the same configuration and asks
again, as Go's token sources ask on each request (UP-140). A rebuilt impersonation reuses its
source token, as Go's does; a credential the library builds whole (an
`impersonated_service_account` document, or an external account with
`service_account_impersonation_url`) repeats every exchange, and a refresh token or document set
as an option is sent again unchanged, so a revoked one keeps failing. Application Default
Credentials are read again, so a new `gcloud auth application-default login` reaches the open
database, where Go reads them once per connection; they are not rebuilt when that would switch
between a file and the metadata server, as removing the file would off Google Cloud.

## C interface and object lifecycle

### G14. Only the driver-manager entrypoint is exported

The library exports only `AdbcDriverBigqueryInit` and `AdbcDriverInit`, so it loads through a driver
manager or `AdbcLoadDriverFromInitFunc` but cannot be linked directly against the C API, as Go's
can.

### G59. Bound C input validation

Before Arrow imports bound data, this driver checks stream callbacks, descriptors, nesting depth,
alignment and lengths; defects return `INVALID_ARGUMENT`, the statement stays usable and rejected
inputs are released. The checks still require valid producer-owned memory ([native API
limitations](LIMITATIONS.md#native-api)). A null handle returns `INVALID_ARGUMENT` (Go:
`INVALID_STATE`), and a panic returns `INTERNAL` and poisons the affected handles. Go passes input
to Arrow Go unchecked and drops a bound array's top-level null bitmap, so invalid input can crash it
or lose null rows.

### G84. Connection option, cancel and `GetInfo` edge cases

| Case | This driver | Go driver |
| --- | --- | --- |
| Unknown key set before `AdbcConnectionInit` | `NOT_IMPLEMENTED` from `SetOption` | Accepted; fails at `Init` |
| Order buffered options apply at `Init` | Catalog before schema, otherwise as set; last value wins across aliases | Random, so `db_schema` can be validated against the wrong project (bug) |
| A buffered option fails at `Init` | `Init` fails and can be retried | A retry returns `INVALID_STATE` (bug) |
| `GetOption` before `Init` | `NOT_FOUND` | `INVALID_STATE` |
| Setting autocommit to its current value | No-op | `INVALID_STATE` (bug) |
| Unfiltered `GetObjects` or `GetStatistics` | The current catalog, else `bigquery.project_id` | The project the client was built with |
| `AdbcConnectionCancel` with nothing running | `INVALID_STATE`, as adbc.h specifies | `OK` |
| `GetInfo` with a non-NULL, zero-length `info_codes` | No rows | Every code |

### G85. Statement lifecycle and ingest option edge cases

| Case | This driver | Go driver |
| --- | --- | --- |
| `AdbcStatementRelease` with a result stream open | Stream stays readable | Stream fails |
| `Release` racing another call on another thread | The other call completes, or reports `INVALID_STATE` | Can panic, after which every call returns `INTERNAL` (bug) |
| Executing again after the binding was consumed | `INVALID_STATE` until rebound | Reuses the last row's parameters after a bound `ExecuteUpdate` (bug) |
| A dry run with bound parameters | Types them from the bound schema and keeps the binding | Consumes the binding |
| `ExecuteSchema` without a binding | Dry run that types `?` placeholders | BigQuery refuses `?` |
| Executing `?` without a binding | BigQuery's "Positional parameters are not supported", plus a hint to bind | BigQuery's message alone |
| `SetSqlQuery` | Clears the `adbc.ingest.target_*` options | Clears only `target_table` |
| Reading `adbc.ingest.target_*` and `mode` | The value set (`NOT_FOUND` while unset); `mode` defaults to `create` | `NOT_FOUND` |
| Invalid `adbc.ingest.target_*` value | `INVALID_ARGUMENT` from `SetOption` | Fails when writing |
| `ExecutePartitions` on an ingest statement | Runs the ingest; no partitions | `NOT_IMPLEMENTED` (G8) |
| `bigquery.query.create_session=true` with autocommit disabled | `INVALID_STATE` before any request | HTTP 400 from BigQuery (bug) |
| `create_append` or `replace` when another writer creates the target first | Appends to, truncates or recreates it | Fails with the `tables.insert` conflict |

### G95. `AdbcErrorFromArrayStream` reports a stream without an error as `OK`

For this driver's stream with no pending error, `AdbcErrorFromArrayStream` returns an empty
`AdbcError` with status `OK`, as adbc.h describes and the C++ drivers do; Go returns NULL, which
adbc.h reserves for an unsupported call.
