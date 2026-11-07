# Go driver comparison

For anyone moving from the Go driver or comparing the two: the differences between this driver and
the active [Go BigQuery ADBC driver](https://github.com/adbc-drivers/bigquery/tree/main/go), the
reference implementation, in Go's default `bigquery.query.job_creation_mode=required` (G87 and G88
cover its opt-in mode). The dbt-labs fork of the Go driver,
[`dbt-labs/bigquery-adbc`](https://github.com/dbt-labs/bigquery-adbc), which
[dbt-core 2.0](DBT-CORE.md), dbt Labs' Rust engine, can load, usually behaves like Go; its own
differences are in [DBT-CORE.md](DBT-CORE.md). "The fork" means `dbt-labs/bigquery-adbc`, and
"driverbase" means [`driverbase-go`](https://github.com/adbc-drivers/driverbase-go), the library the
Go driver builds on.

An option described as refused returns `NOT_IMPLEMENTED` with the reason when set and
`NOT_FOUND` when read. Entry IDs are stable references used by the other guides.

## Summary

| Area | Main differences |
| --- | --- |
| [Bulk ingestion](#bulk-ingestion) | This driver uses Storage Write only and rejects ingest inside transactions. Replacement avoids a separate table deletion, but a failed replacement can still lose the old rows. Go also supports load jobs, which are its default. |
| [Query parameters](#query-parameters) | Type support, precision checks, parameter-schema inference, and the order and scheduling of bound executions differ. |
| [Query execution and results](#query-execution-and-results) | This driver supports `ExecutePartitions`, uses `jobs.query` for ordinary queries, and reads REST rows or Storage Read results. Go's optional inline Arrow results and job-default streams are unsupported here. |
| [Errors and diagnostics](#errors-and-diagnostics) | Some ADBC status mappings differ. This driver also returns structured error details and SQLSTATEs. |
| [Transactions and sessions](#transactions-and-sessions) | This driver starts the next transaction lazily after commit or rollback, accepts snapshot isolation, and ends the session when its connection closes. |
| [Metadata](#metadata) | Filter matching, null fields, statistics, and schema metadata differ. This driver can discover tables in a project other than the configured project. |
| [Configuration and authentication](#configuration-and-authentication) | Option scopes, validation, credential handling, and URI replacement differ. Legacy-SQL-only options are unsupported. |
| [C interface and object lifecycle](#c-interface-and-object-lifecycle) | This driver exports init entrypoints only; callers use a driver manager. Invalid inputs and handle-state checks also differ. |
| [Artifact size](#artifact-size) | How to compare release builds. |

## Bulk ingestion

### G1. Bulk ingest is rejected inside a transaction

With `adbc.connection.autocommit` disabled, bulk ingest returns `INVALID_STATE` before it sends a
request or consumes bound data ([bulk ingest](REFERENCE.md#bulk-ingest)). Go ingests outside the
transaction, so the rows remain after a rollback that reports success. Neither driver can make
ingestion transactional: Storage Write streams carry no session, and BigQuery refuses DDL that
creates or drops permanent tables inside a transaction.

### G2. Bulk ingest defaults to Storage Write, and load jobs are not implemented

`bigquery.bulk_ingest.method` defaults to `storage_write`, its only supported value; `load` returns
`NOT_IMPLEMENTED`, as does setting the option on a database rather than a connection or statement.
Go defaults to `load`, which writes a temporary Parquet file and runs a load job, and also accepts
the option on a database. A client that leaves it unset therefore sees different latency and
different BigQuery DML restrictions on recently streamed rows.

### G7. Ingest schema mismatches return `ALREADY_EXISTS`

For `append` and `create_append`, a bound schema the existing table cannot accept returns
`ALREADY_EXISTS` before a stream opens, as adbc.h requires, naming every column at fault; the
matching rules are in [bulk ingest](REFERENCE.md#bulk-ingest). Because Storage Write silently
rounds decimals and stores overlong strings, every decimal and length-limited string value is also
checked against its column. Go never compares the schemas, and its Storage Write path reports the
later append failure as `IO`.

### G11. Arrow interval bulk ingestion is supported

Arrow year-month, day-time and month-day-nanosecond arrays ingest into `INTERVAL` columns. Day-time
arrays are converted to month-day-nanosecond (UP-19), and sub-microsecond components truncate
toward zero. Go has no interval mapping for ingestion, so it cannot ingest the interval schemas it
returns.

### G22. Oversized Arrow ingest batches are split into bounded append requests

Each ingest batch is normalized and sent in slices of about 9 MB, under BigQuery's
10,000,000-byte request limit, so a batch of any size is converted a request at a time and only a
single row too large for one request fails, locally, with `INVALID_ARGUMENT`. Appends carry explicit
offsets on a pending stream. Recovery of the oldest failed append allows up to three retries;
queued successors are then replayed at their original offsets. A later append's failure starts a
new recovery budget, so a queued append may be sent more than four times overall. A replay answered
`ALREADY_EXISTS` counts as written. Go sends each batch as one request, so a large
batch fails at the service, and its appends carry no offset and only the first is retried, on
`NotFound`, so a transport failure mid-ingest fails it.

Window sizing counts destination type widths, expanded dictionary values, overlapping list-view
ranges and offsets for empty or null values. This includes Null fields resolved from the destination
schema and primitive widening, so small input buffers cannot hide a large normalization allocation.

### G23. The first ingest batch is validated before target-table mutation

The first non-empty batch is fully validated before `create` or `replace` changes the target, so
invalid values, and a row or schema too large for an append request, fail before any table is
created or replaced; a schema with no columns is refused before any request. Go's driverbase
prepares the table first, so an invalid batch can leave a new or recreated table. In both drivers
a later batch can still fail after preparation, and a failed ingest keeps any table it created.

Local validation also rejects equal or reversed RANGE bounds after precision conversion,
following BigQuery's [RANGE ordering rule](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range).

### G24. Storage Write ingestion supports exact Arrow Date64 values

Arrow Date64 values that are whole days (multiples of 86,400,000 ms) ingest as `DATE`. Other values
return `INVALID_DATA`, and out-of-range day counts fail locally. Go's Storage Write path does not
support Date64.

### G29. The bulk-ingest compression option is refused

`bigquery.bulk_ingest.compression` is refused at every scope, with a message that appends are sent
uncompressed. Go accepts `none`, `lz4` and `zstd` but never applies them, so appends are
uncompressed under both drivers. The one inert Go option accepted here is
`bigquery.query.link_failed_job`, because dbt-core 2.0 sets it on every statement; failed jobs carry
their console link regardless (G51).

### G30. A dry run on a bulk-ingest statement validates instead of writing

With `bigquery.query.dry_run`, an ingest checks the target and bound schema against the ingest mode
with one read-only lookup and reports zero rows without writing, keeping the data bound
([bulk ingest](REFERENCE.md#bulk-ingest)). Go and the dbt-labs fork accept the option on an ingest
statement, ignore it, and write the data.

### G38. Rejected ingest rows are named by the caller's own row number

When Storage Write rejects rows, the error names each one by its row number in the caller's bound
stream and its position in the append, with BigQuery's code and message. Go never reads
`row_errors` and reports only the RPC status message.

### G53. Lossless numeric widening accepts more Arrow input types

Ingest widens Arrow `Int8`, `UInt8`, `UInt16` and `UInt32` to `INT64` and `Float16` to `FLOAT64`,
which Go rejects with `NOT_IMPLEMENTED`; both drivers reject a `UInt64` column with
`NOT_IMPLEMENTED` (cast it to `Int64`, or to `Decimal128(20, 0)` for `NUMERIC`). Parameters accept
the same types, and a `Float32` or `Float16` parameter binds as its exact value (`0.1f32` as
`0.10000000149011612`), where Go sends the shortest decimal (`0.1`).

### G63. Storage Write ingestion uses the configured credentials and Storage endpoint

Storage Write uses the database's configured credentials, impersonation, quota project and
`bigquery.storage_endpoint`, like every other client. Go creates its Storage Write client without
options, so with `bigquery.bulk_ingest.method=storage_write` (G2) it authenticates with ambient
Application Default Credentials (ADC) against `bigquerystorage.googleapis.com`, whatever those options
say.

### G71. `replace` ingestion avoids a separate table deletion

If the schema differs, `CREATE OR REPLACE TABLE` recreates the target before uploading. If it
matches, `TRUNCATE` runs after every row is on the finalized pending stream, just before commit,
so an upload failure keeps the old rows. Neither path makes the whole ingest atomic: a failure
after recreation or truncation can leave an empty table. The error reports that loss or, after
an ambiguous cancellation, the possibility of it ([bulk ingest](REFERENCE.md#bulk-ingest)).
Go's driverbase deletes the table and then creates it, so a failure between those operations can
leave no table. `replace` here needs permission to run query jobs in the billing project.

### G78. Storage Write's control-plane calls are retried, and their answers are read

`CreateWriteStream`, `GetWriteStream`, `FinalizeWriteStream` and `BatchCommitWriteStreams` are
retried on transient statuses (3 attempts, 200 ms doubling; UP-83). `stream_errors` in the commit
response fail the ingest, except `STREAM_ALREADY_COMMITTED` on a replayed commit, and finalize and
commit ignore cancellation for up to 60 seconds, so a cancel cannot report failure for committed
rows. Go retries only `CreateWriteStream` on `NotFound`, sends finalize and commit once, and
discards the commit response, so a commit BigQuery declined is reported as a successful ingest.
Input failures drop queued appends without waiting for their responses, then attempt to finalize
the abandoned stream with a 10-second timeout.

### G79. An ingest target that exists but is not a table is refused before anything is written

An ingest whose target is a view, materialized view, external table or snapshot returns
`INVALID_ARGUMENT` naming the object's type, from the `tables.get` the ingest already sends. Go
treats any existing object as a table, so `append` reaches the service and `replace` deletes the
object.

### G92. Array decimal elements keep their declared scale on ingest

Storage Write reads the elements of an `ARRAY<NUMERIC>` or `ARRAY<BIGNUMERIC>` column at scale 9 or
38, whatever scale their Arrow type declares (UP-89 in [UPSTREAM.md](UPSTREAM.md)), so ingest sends
array elements rescaled to it. Go's Storage Write path sends Arrow decimals unchanged, so `0.01` in
a `List<Decimal128(10, 2)>` is stored as `0.000000001`.

### G93. Repeated TIMESTAMP ingest fields are refused before any request

An `ARRAY<TIMESTAMP>` field at any depth returns `NOT_IMPLEMENTED` naming the field before a table
is created or written, since Storage Write fails every such append with `INTERNAL` (UP-90 in
[UPSTREAM.md](UPSTREAM.md)). Go's Storage Write path creates the table first and then reports the
append failure as `IO`; its default `load` method does not use Storage Write (G2).

## Query parameters

### G9. Query parameters preserve rich logical types

Parameters whose Arrow fields carry `BIGQUERY:type` metadata for `JSON`, `GEOGRAPHY` or `RANGE` are
sent as those types, so a result schema from this driver binds back unchanged, and an Arrow `Null`
field binds as a NULL of the scalar type its metadata names. Go maps every Arrow string to `STRING`
and every struct to `STRUCT`, so its `RANGE` parameters fail, and sends every `Null` field as a
`STRING` NULL, which BigQuery refuses where the SQL needs another type, such as
`UPDATE t SET n = ?` on an `INT64` column.

### G10. Arrow interval query parameters are supported

All three Arrow interval layouts, and `Duration` in any unit, bind as `INTERVAL`: a duration is the
time part (two days is `0-0 0 48:0:0`), sub-microsecond components truncate toward zero, and a
duration beyond 2,562,047 hours fails locally. Go rejects interval and duration parameters. An
`INTERVAL` result beyond that bound fails on the REST path with a message suggesting
`CAST(x AS STRING)` or `EXTRACT`. Storage Read, which Go reads through, refuses it too, with
`OUT_OF_RANGE`.

### G17. Out-of-precision decimal values are rejected locally

All four Arrow decimal widths reject values outside their declared precision, including nested
ones, before any request with `INVALID_DATA`, for parameters as for ingest. Go
formats such a parameter with the declared number of digits, so it can send a rounded value, and
passes ingest values to Storage Write unchecked. This driver's validation errors name the field
and declared precision without quoting the rejected value. A wider ingest destination does not
bypass source precision validation.

### G18. Empty composite parameter support

Non-null empty ARRAY and zero-field STRUCT parameters are sent as the scalar markers `[]` and `{}`,
because BigQuery reads an empty `arrayValues` as NULL; Go fails before sending with `bigquery: nil
parameter`. Array parameters with NULL elements return `INVALID_DATA` naming the parameter, row and
element, and directly nested ARRAY parameters return `NOT_IMPLEMENTED` with advice to wrap the
inner array in a STRUCT, as GoogleSQL requires; Go checks neither locally.

### G21. Decimal types are selected by declared value domain

Parameters and ingest choose `NUMERIC` when the Arrow precision and scale fit 29 integer and 9
fractional digits, `BIGNUMERIC` when they fit 38 of each, and fail locally otherwise; all four Arrow
decimal widths and negative scales are accepted, and `BIGQUERY:type` metadata can select either
type ([supported types](REFERENCE.md#supported-types)). Go picks parameter types by Arrow width (a
`decimal128(38, 20)` is sent as `NUMERIC`), maps both widths to `NUMERIC` for ingest, and lacks
`Decimal32` and `Decimal64`.

### G50. Bound query results stream in parameter order

With bound parameters, `ExecuteQuery` of a statement that returns rows, including a stored-procedure
`CALL`, returns when the first row's query completes and runs each later row after the previous
result is read, so results keep parameter order and cancellation or failure stops later rows.
DML, DDL and queries with an explicit
destination table run every row before returning, as Go does
([bound parameters](REFERENCE.md#bound-parameters)). Go runs
every row's read into one bounded channel and waits for all of them before returning the reader, so
a result larger than the channel blocks `ExecuteQuery` until it is cancelled, and results can
arrive out of order. An error in a later row names its zero-based parameter row and how many rows
already ran.

### G61. Parameter schemas are inferred by a dry run

`GetParameterSchema` infers parameter names and Arrow types with a dry run, cached on the statement
per namespace and session; a query without parameters returns an empty schema. Named parameters
come back as BigQuery reports them, lowercased and sorted by name. Go returns `NOT_IMPLEMENTED`.

### G70. `adbc.statement.bind_by_name` is accepted as a parameter-mode alias

`adbc.statement.bind_by_name`, the SQLite driver's cross-driver key, is an alias of
`bigquery.query.parameter_mode`: `true` means `named`, `false` means `positional`, and both keys read
back the current mode. Go returns `NOT_IMPLEMENTED` for it.

## Query execution and results

### G8. Partitioned execution is implemented

`ExecutePartitions` runs the query as a job and returns a compact job reference as the partition
descriptor, which `ReadPartition` can read repeatedly; bound parameter rows become separate
partitions when their result schemas agree. Go returns `NOT_IMPLEMENTED`. A partitioned dry run is
rejected before bound parameters are consumed, since a dry run creates no job.

### G25. REST result conversion is cooperatively cancelled and driver-wide CPU bounded

Complete resident REST results of at most 32 rows and 4 KiB convert lazily on the caller's first
read. Larger or pageable results convert to Arrow in blocking tasks that reader release or
cancellation stop, even mid-conversion. Both paths share driver-wide conversion permits equal to
the available parallelism, so a high `bigquery.query.prefetch_concurrency` cannot oversubscribe
the CPU. Pending REST conversions stop accepting batches once their retained input allocations
reach 64 MiB, counting nested values and spare capacity rather than page-average wire bytes.
REST batches end at page boundaries and can hold fewer than 1,024 rows. Go reads results through
Storage Read, keeps its batch sizes, and does not apply `prefetch_concurrency` to plain queries.

### G26. Storage Read uses one stream by default

Storage Read uses one stream, which preserves any `ORDER BY`; `bigquery.query.unordered_read`, which
Go lacks, reads all granted streams in parallel (up to the available parallelism, at most 32). Go's
SDK scans the SQL for a top-level `ORDER BY` and reads other queries over several streams, and the
scan fails on unbalanced parentheses, so `SELECT x FROM t WHERE s = '(' ORDER BY x` returns rows
out of order. Each batch's row count is checked (`INVALID_DATA` on mismatch), and a transient
failure, including `RESOURCE_EXHAUSTED`, a transport reset and two minutes of silence, resumes at
the next unread row within four consecutive attempts and 30 seconds of backoff; Go's SDK does
neither.

### G27. Resolving the result table costs one round trip instead of two

Before a Storage Read, this driver fetches the job once and takes the schema from the query response
it already holds; Go's SDK fetches the job again and then the destination table's metadata. The
only visible effect is one round trip fewer before the first row.

### G31. Result-schema job statistics are limited to the keys a consumer reads, and never fabricated

Result schemas carry `BIGQUERY:query_id` and six statistics under Go's lowercase keys and their
PascalCase aliases ([result metadata](REFERENCE.md#result-schema-metadata)); Go emits about 30. An
unreported statistic is absent, where Go writes `"0"` (such as `NumDMLAffectedRows` on a `SELECT`),
and the statement type always comes from BigQuery, also for jobless queries, which BigQuery can
classify differently (`SET @@dataset_id` followed by `SELECT` is `SELECT` without a job and
`SCRIPT` with one). A result costs `jobs.query`, with `jobs.getQueryResults` only if incomplete or
paged, or `jobs.insert` and `jobs.getQueryResults` when an option needs a job (G46). Job-backed
results may add `jobs.get` for metadata. Go uses `jobs.insert` and at least one `jobs.get`, and its
Storage Read adds another `jobs.get` and a `tables.get`.

### G32. Row counts: DDL stays unknown, and DML never reports the table's size

DDL reports `-1` rows affected, because BigQuery reports no count, where Go reports `0`. DML reports
BigQuery's `numDmlAffectedRows`, while Go's `ExecuteQuery` returns `TotalRows`, which for DML is the
target table's size afterwards (a 1-row `UPDATE` on a 5-row table returns 5). Dry runs, scripts
that do not end in a query and queries with `bigquery.query.destination_table` also report `-1`
here, where Go reports a dry run's bytes processed; [row counts](REFERENCE.md#row-counts) lists
every case.

### G45. Scripts use the ordinary result path instead of interpreting an SDK failure

Every completed query, scripts included, uses the ordinary result path: a script returns its final
result set, read through Storage Read when paginated because the script job names it as its
destination, or an empty schema and zero rows, with `SCRIPT` statement-type metadata. Go resolves a
script's child result table for Storage Read and matches an SDK error message for scripts without
one. A denied Storage Read (`PERMISSION_DENIED`) falls back to REST and is remembered for the
billing project, where Go returns `UNAUTHORIZED`, and an `INVALID_ARGUMENT` refusal of a table REST
reads falls back for that result only, where Go returns an error
([Storage Read](REFERENCE.md#reading-large-results-through-the-storage-read-api)).

### G46. Ordinary queries use `jobs.query`; job-only configurations use `jobs.insert`

Ordinary GoogleSQL runs through `jobs.query` with optional job creation and a ten-second completion
wait, so a short query may return all its results without a job. Partitioned execution requires
a job for its descriptors. Subsequent `jobs.getQueryResults` requests wait up to 45 seconds.
Dry runs, destination tables, and non-default create or write dispositions or priority use
`jobs.insert`; explicitly setting the defaults does not. Go uses `jobs.insert` by default; its
optional-job mode is opt-in (G87). `ExecuteQuery` with `bigquery.query.destination_table` returns
the schema and no rows, where Go reads the destination table back.

Complete first responses stay on REST here. Any continuation token selects Storage Read before
rows are delivered, so a later row exceeding REST's 20 MiB limit cannot break an eligible result
with working Storage Read. Go uses Storage Read for ordinary nonempty query results even when
they fit in one REST response. Disabled or unavailable Storage Read retains REST's row-size limit.
When an oversized REST row requires Storage Read and that attempt fails, this driver adds the
REST size-limit explanation while preserving the Storage Read error's status and diagnostics.

### G47. Submission retries preserve identity, and named jobs can be recovered

Each execution keeps one request or job ID across transport retries: after an ambiguous
`jobs.insert` (a lost response or a 409) the driver fetches exactly that job. If a replayed
`jobs.query` receives a 409 naming a job, the driver reads its results; Go's SDK returns an error.
BigQuery may ignore `requestId` for a read-only query, so repeating that request does not guarantee
recovery of the original execution. It ignores it for `BEGIN`, `COMMIT` and `ROLLBACK TRANSACTION`
too, so the driver submits those as named jobs. A job confirmed failed with `backendError`, `jobBackendError`,
`jobInternalError`, `rateLimitExceeded`, `jobRateLimitExceeded`, `internalError` or
`tableUnavailable` is rerun with a fresh ID at most three times, except scripts, `CALL` and
statements of unknown type, whose earlier statements may have committed; Go never reruns a failed
job. Requests give up after five consecutive transient failures. Waiting for a job goes on past
that for ten minutes of consecutive failures, or for as long as `jobs.get` reports the job
unfinished, where Go polls until the caller's context ends; the bound stops a dead network blocking
a caller that has no way to cancel.

### G48. Jobs submitted without `bigquery.location`

Without `bigquery.location`, a query that needs `jobs.insert` (G46) lets BigQuery infer the
location, as Go does, and later requests use the location the answer names. When that answer is
lost, the driver looks for the job in the location the replay's 409 names, then with `jobs.get`
without a location (which finds `US` and `EU` multi-region jobs), then in the destination or
default dataset's location. If those lookups fail to find it, `jobs.list` searches the caller's
recent jobs across regions for that exact ID and recovers the full job reference. The search
starts five minutes before submission to allow clock skew, retries an incomplete search up to
three scans, and stops at 100 pages or 30 seconds including credentials and the final job fetch.
It registers references found through duplicate responses or listings for cancellation before
fetching the job, so a failed fetch does not prevent cancellation. If recovery fails, the error
names the job as possibly still running; Go's SDK returns an error at once. dbt-core 2.0 seed loads
and table copies run in the destination dataset's location, found by one cached `datasets.get`
per dataset, where the dbt-labs fork lets BigQuery infer it. The database shares at most 32 cached
locations across its connections, evicting the oldest inserted entry when full; revisiting an
evicted dataset repeats the lookup.

### G49. Remote job cancellation and operation lifetime

As in Go, cancellation, statement supersession, reader release or an operation ending early stops
local work and requests cancellation of any unfinished job. Cancellation while the initial
`jobs.query` is pending keeps its response alive in the background and requests cancellation when
an unfinished job's reference arrives. Retries after definitive rejections stop on cancellation;
ambiguous submissions keep recovering their job reference. Remote cancellation can be delayed by the
ten-second completion wait (G46) and transport delays, even though local work stops immediately.
Queries that finish during that wait cannot be stopped this way.
Verified job metadata updates cancellation's regional identity and completed status. Resolving a
project number or location preserves completion, even if a later response still reports RUNNING.
Unlike Go, a failed remote cancel is retried, and a cancel during a `jobs.insert` without a location
lets the submission finish in the background, then requests cancellation where its answer or the
search in G48 puts the job.
A failed explicit cancel returns `UNKNOWN` within 30 seconds, and a failed background cancel is a
`warn` diagnostic. A `jobs.query` submission whose retries ended without a job reference cannot be
cancelled, and its error says the statement may still have run.

### G52. Canonical statement progress reports response-boundary work without extra polling

`adbc.statement.exec.progress` reports processed bytes from each `jobs.query`, `jobs.insert` or
`jobs.getQueryResults` response, about every 45 seconds while a query runs, and
`adbc.statement.exec.max_progress` is `0` until the final total is known; reading either never
waits for a running execute or sends a request. Parameter-schema discovery preserves these values,
including when its dry run retries
([standard options](REFERENCE.md#standard-adbc-options)). Go returns `NOT_FOUND` for both.

### G58. Arrow IPC supports a BigQuery-specific subset

Storage Read, Storage Write and the dbt-core 2.0 CSV seed schema use an in-tree Arrow IPC reader
and writer (`src/arrow_ipc_bigquery`) for BigQuery's type and encoding subset, where Go uses Arrow
Go's general implementation. A Storage Read schema outside the supported subset falls back to
REST before any rows are read; malformed schemas or unsupported batch encodings are errors.

### G87. Optional jobs use REST rows here; Go adds inline Arrow and job-default streams

Go has an opt-in mode that reads jobless results as inline Arrow and job-backed results from the
job's `_default` read stream, avoiding `CreateReadSession` and so the `bigquery.readsessions.create`
permission. This driver enables optional jobs for ordinary execution and reads jobless results
as REST rows in one response of at most 100,000 rows. Large job-backed results use a table-backed
Storage Read session; inline Arrow and job-default streams are not implemented.
Go's options for the mode, `bigquery.query.job_creation_mode`, `bigquery.query.results_format` and
`bigquery.query.arrow_serialization_options.buffer_compression`, are refused here.

### G88. Inline Arrow has a different schema and metadata contract

In Go's optional-job mode, inline Arrow schemas come from the service, normalized by
`unmangleBigQueryArrowType` (JSON to `arrow.json`, GEOGRAPHY to `geoarrow.wkt`), with
`BIGQUERY:type` guessed where the BigQuery schema is unavailable, and the metadata adds
`BIGQUERY:job_creation_reason` but omits `BIGQUERY:query_id`, the statement type and slot
milliseconds. This driver derives fields from BigQuery's `TableSchema` on every path and does not
expose `BIGQUERY:job_creation_reason`. Inline Arrow for a collated column, such as
`SELECT COLLATE('hello', 'und:ci') AS s`, fails with HTTP 400 `jobInternalError`, while REST rows
and this driver's Storage Read path return it with its `Collation` metadata.

To reproduce the failure with ADC, POST this body to
[`jobs.query`](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query)
(`https://bigquery.googleapis.com/bigquery/v2/projects/example-project/queries`, with your own
project):

```json
{
  "query": "SELECT COLLATE('hello', 'und:ci') AS s",
  "useLegacySql": false,
  "useQueryCache": false,
  "location": "US",
  "jobCreationMode": "JOB_CREATION_OPTIONAL",
  "queryResultsFormat": "ARROW"
}
```

Both `JOB_CREATION_OPTIONAL` and `JOB_CREATION_REQUIRED` return HTTP 400, reason
`jobInternalError`, status `INVALID_ARGUMENT`: "The job encountered an internal error during
execution and was unable to complete successfully." Omitting `queryResultsFormat` succeeds and
returns `hello` with `collation: "und:ci"`. A nested result,
`SELECT STRUCT(COLLATE('hello', 'und:ci') AS s) AS nested`, has the same failure and JSON success.
Arrow results for `'hello'`, `COLLATE(COLLATE('hello', 'und:ci'), '')`, and
`CAST(COLLATE('hello', 'und:ci') AS STRING)` succeed in both job modes, with no collation metadata.

### G91. Reader construction failures release owned resources

If building a Storage Read result fails during schema decoding or reshape planning, no reader is
published and owned resources are dropped. Go's `ipcReaderFromArrowIterator` returns from later
`buildField` or metadata failures without releasing the IPC reader it built, which the dbt-labs
fork fixes.

## Errors and diagnostics

### G51. Query errors carry ADBC-faithful status and structured details

Stable reasons such as `accessDenied`, `invalidQuery`, `notFound`, `duplicate`, `notImplemented`,
`stopped` and `timeout` keep Go's statuses, and `IO` is reserved for failures BigQuery documents as
retryable. These statuses differ:

| Condition | This driver | Go driver |
| --- | --- | --- |
| Backend, rate-limit and quota reasons | `IO` | `INTERNAL` |
| `tableUnavailable` | `IO` | `NOT_FOUND` |
| `resourceInUse` | `INVALID_STATE` | `INTERNAL` |
| `stopped` because `bigquery.query.job_timeout` expired | `TIMEOUT` | `CANCELLED` |
| HTTP 401 | `UNAUTHENTICATED` | `UNAUTHORIZED` |
| HTTP 403 without a recognized reason | `UNAUTHORIZED` | The call site's default, usually `INTERNAL` |
| Connection and transport failures | `IO` | The call site's default |

Transient token-exchange failures for explicit credentials or impersonation also return `IO`.
Go returns `IO` for discrete OAuth transport failures, but JSON and impersonation request-time
transport errors keep the call site's default, commonly `INTERNAL` during queries. An unavailable
ambient ADC metadata server remains `UNAUTHENTICATED` here with credential setup advice.

This driver also maps HTTP 501 to `NOT_IMPLEMENTED`, `responseTooLarge`, `resourcesExceeded` and
`bytesBilledLimitExceeded` to `INVALID_ARGUMENT`, and a disabled API and unusable credentials as
[errors](REFERENCE.md#errors) describes. Its errors carry structured details and, for `invalidQuery` and `accessDenied`, SQLSTATE
`42000`; Go errors carry only a status and message. Both drivers end a failed job's message with
`(Query: <console URL>)`.

### G86. Diagnostics use timestamped plain text and a fixed stderr sink

Both drivers read `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` and accept `error`, `warn`/`warning`, `info`
and `debug`. This driver reads it once per process, disables diagnostics when it is unset or
empty, and writes `TIMESTAMP [bigquery-adbc] LEVEL message` lines to stderr. Timestamps are UTC
in RFC 3339 format with millisecond precision ([diagnostics](REFERENCE.md#diagnostics)).

Go reads the variable at every `AdbcDatabaseInit` and writes timestamped slog JSON lines to
stderr, or to a temporary file with `ADBC_DRIVER_BIGQUERY_LOG_SINK=tempfile`. Its
[C wrapper](https://github.com/adbc-drivers/bigquery/blob/5f836152af0103f04c8e0770a7830f4e3535404f/go/pkg/driver.go#L232)
selects warnings for both `warn` and `warning`. An unset or empty value retains
driverbase's [default error-level stderr logger](https://github.com/adbc-drivers/driverbase-go/blob/5cee18f5413f/driverbase/logging.go#L33).
Native Go callers can also supply a custom slog logger through `DatabaseLogging.SetLogger`;
this driver has no logger injection API or configurable sink.

Go's events include failed job cancels and Storage Write stream closes, plus credential choice,
ingest steps and retries at `debug`.

## Transactions and sessions

### G16. The next transaction begins lazily after commit or rollback

A commit or rollback sends nothing more; the next query begins the next transaction, in a new
session if BigQuery has ended the old one, even right after accepting the `BEGIN` there
([autocommit](REFERENCE.md#standard-adbc-options)). Go sends `BEGIN TRANSACTION` right after
`COMMIT` or `ROLLBACK`, so a failed `BEGIN` reports an error for a commit that succeeded. Here
`BEGIN`, `COMMIT` and `ROLLBACK` run to a verdict as named jobs, ignoring cancellation for up to a
minute, and only a refused `BEGIN` or `COMMIT` keeps the session; Go cancels all three with its
context and keeps the session after any failure. Without `bigquery.location`, the session is created in the
default dataset's location, where Go lets BigQuery place it in the US.

### G40. Snapshot isolation is an accepted and reported isolation level

`adbc.connection.transaction.isolation_level` accepts `adbc.connection.transaction.isolation.default`
and `adbc.connection.transaction.isolation.snapshot` as no-ops and reads back the snapshot level,
which is what BigQuery transactions provide; other levels return `NOT_IMPLEMENTED`. Go returns
`NOT_IMPLEMENTED` for every value.

### G64. Closing a connection ends its transaction and session

Releasing a connection in manual-commit mode rolls back an open transaction and ends the session
with `CALL BQ.ABORT_SESSION`, reporting a failure only as a `warn` diagnostic; a statement that
outlives it returns `INVALID_STATE` instead of autocommitting. Go's `Close` only closes its client,
leaving the uncommitted work and the session to BigQuery's idle timeout, and its statements fail
once their connection is closed.

## Metadata

### G33. Unsupported or inapplicable XDBC column fields remain null

`GetObjects` fills XDBC column fields only with real values: the type name, nullability,
auto-increment and generated status for every column, `xdbc_column_size` and `xdbc_decimal_digits`
for parameterized types, and `xdbc_char_octet_length` for `BYTES(L)`; unknown sizes, SQL type codes
and reference scopes are null, as the ADBC schema asks. Go fills unknown sizes and digits with
zero, puts an Arrow type ID in `xdbc_data_type`, and sets the `xdbc_scope_*` fields to the column's
own table.

Both XDBC nullability fields report `REPEATED` columns as non-nullable, since BigQuery
[arrays can be empty but cannot be NULL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#column_name_and_column_schema).
Go checks only `Required` and reports repeated columns as nullable; the dbt-labs fork inherits
that behavior.

### G34. Metadata listings with no configured catalog return an empty result

A connection can open without a project and set `adbc.connection.catalog` later; until then only a
catalog filter naming a project lists anything. Go refuses to open such a connection with
`INVALID_ARGUMENT` (`[bq] ProjectID is empty`). Both drivers answer an unfiltered or wildcard
catalog filter with only the current project ([metadata catalogs](REFERENCE.md#metadata-catalogs)).

### G35. `GetStatistics` row counts come from `tables.get`

`adbc.statistic.row_count` comes from one `tables.get` per table: BigQuery's exact `numRows` as an
`int64`, or an approximate `float64` while a streaming buffer holds rows or for a materialized view
([table statistics](REFERENCE.md#table-statistics)). Go always reports an approximate `float64`
from an `INFORMATION_SCHEMA.PARTITIONS` query. Of Go's vendor statistics, key 1025,
`bigquery.statistic.total_billable_bytes`, is not published, because only
`INFORMATION_SCHEMA.PARTITIONS` reports it.

### G36. `GetInfo` answers the XDBC range and never omits a recognized code

`GetInfo` without arguments answers 26 codes, including 15 XDBC codes in `[500, 1000)` that JDBC and
ODBC bridges read; Go answers 9 and no XDBC codes. A requested code the driver does not recognize is
omitted, as adbc.h requires, where Go returns a null row. `VendorArrowVersion` is null here, since
BigQuery has no server Arrow version; Go reports the placeholder `(unknown or development build)`.

### G39. A view without a schema is an error from `GetTableSchema`

BigQuery returns a view without a schema when its SQL no longer compiles; `GetTableSchema` then
returns `INVALID_DATA` naming the view, where Go returns a schema with no fields. A table created
without columns has an empty schema in both drivers, and `GetObjects` lists either with no columns.

### G55. `GetObjects` at table depth defaults to null constraints without describes

At tables depth, `GetObjects` builds each row from `tables.list` alone, so `table_columns` and
`table_constraints` are null and no per-table request is sent, as in the ADBC C++ framework. Go
calls `tables.get` for every listed table at any depth and returns a constraint list. Column depth,
or the dbt-labs fork's connection option `bigquery.get_objects.skip_table_metadata=false` (default
`true` here), returns constraints ([options](REFERENCE.md#query-and-statement-options)).

### G56. Metadata filter patterns are matched case-sensitively

Catalog, schema, table and column filters match case-sensitively, so `table_name="EVENT%"` does not
match `events`; Go's driverbase matches every filter case-insensitively. A wildcard-free filter
becomes a direct lookup (G74), which BigQuery resolves case-sensitively except in a dataset created
with `isCaseInsensitive`, where `EVENTS` finds and reports `events`, as in Go.

### G60. Hidden datasets are discoverable and retain their table statistics

`datasets.list` is sent with `all=true`, so `GetObjects` and `GetStatistics` include hidden datasets
(names starting with `_`) and report their tables' statistics from `tables.get`. Go also lists
hidden datasets but returns no statistics for them, because BigQuery hides them from the
`INFORMATION_SCHEMA` views its statistics query reads.

### G62. Metadata discovery skips inaccessible tables without suppressing service restrictions

`GetObjects` and `GetStatistics` skip a table, or a dataset whose `tables.list` is refused, only
when BigQuery answers HTTP 403 with `accessDenied` or no reason; other 403 reasons, such as rate
limits or a disabled API, fail the call with their own status (G51). Go skips every table whose
metadata returns HTTP 403, so throttling can silently shorten a listing. At tables depth the
listing uses `tables.list` alone (G55), so it can include a table whose metadata is inaccessible,
which Go omits.

### G65. Table-schema metadata describes the table, not its storage accounting

`GetTableSchema` metadata holds up to 35 table-definition keys from `tables.get(view=BASIC)` in the
dbt-labs fork's spellings, which dbt-core 2.0 reads ([DBT-CORE.md](DBT-CORE.md#metadata)); only
`Type` is always present. The request selects only the schema and configuration fields;
`GetStatistics` separately requests `STORAGE_STATS`. Go and the dbt-labs fork use the default
metadata view, which also collects storage statistics. Go writes `BIGQUERY:<snake_case>` keys with
PascalCase copies of only some, so it lacks spellings dbt reads such as `Labels` and
`RequirePartitionFilter`, and writes a missing expiration as `0001-01-01T00:00:00Z`. Go also emits
storage counters, modification times,
`ETag`, streaming-buffer estimates, primary keys and snapshot and clone definitions, several as
empty strings when unset; here those come from `GetStatistics` and `GetObjects`.

### G66. Field metadata omits what the Arrow field already says

Field metadata has `BIGQUERY:type`, `Type` and the `geoarrow.wkt` and `arrow.json` extension keys,
as in Go, but omits keys that restate the Arrow field or hold no value: no `Required` (use the
field's nullability), `Repeated` only when true, `Description` and `Collation` only when non-empty,
and `BIGQUERY:max_length` and `MaxLength` only when declared, where Go writes `"0"`
([supported types](REFERENCE.md#supported-types)). A `NUMERIC(P, S)` or `BIGNUMERIC(P, S)` column
adds `BIGQUERY:precision` and `BIGQUERY:scale`, which Go does not write, and `PolicyTags` is
BigQuery's `{"names":[…]}` rather than Go's `{"Names":[…]}`. The column default is `GetObjects`'
`xdbc_column_def` rather than `DefaultValueExpression` metadata, and a list's item field carries the
element's `BIGQUERY:type`, which Go's lacks.

### G69. An empty `table_type` list matches no tables

An empty, non-NULL `table_type` list returns no tables and sends no request, because adbc.h gives
only NULL the meaning "any type"; Go's driverbase treats an empty list like NULL. A non-empty list
is applied before any per-table request, so excluded tables cost nothing. A described table's
current type is checked again, in case it was replaced after listing.

### G72. A table or dataset dropped mid-listing is skipped, not reported as a missing dataset

A table that returns 404 when described during `GetObjects` or `GetStatistics` is skipped, and a
dataset whose `tables.list` returns 404 is omitted; an exact lookup of a vanished table does not
report its dataset as missing. Go skips a vanished table, but a dataset dropped between the
`datasets.list` walk and its `tables.list` fails the whole call with `NOT_FOUND`.

### G73. An unrecognized table type is reported as `TABLE` rather than passed through

A table type is reported only if it is one of the five `GetTableTypes` publishes (`TABLE`, `VIEW`,
`EXTERNAL`, `MATERIALIZED_VIEW`, `SNAPSHOT`); any other or missing value is reported as `TABLE` and
logged as a `debug` diagnostic, in `GetObjects` and in `GetTableSchema`'s `Type` metadata. Go
passes the client library's value through. BigQuery returns only those five today, and an
unannounced type would make dbt-core 2.0 panic (UP-79 in [UPSTREAM.md](UPSTREAM.md)).

### G74. Default metadata patterns cannot take an exact dataset lookup for a name containing `_`

A wildcard-free metadata filter becomes a direct lookup (`tables.get` for a table; for a dataset,
`tables.list` at tables depth, plus `datasets.get` if it finds no table, or `datasets.get` at schema
depth), which reports the name BigQuery resolves the filter to (G56). adbc.h makes `_` a
single-character wildcard with no escape, so a dataset filter containing `_`, as dbt dataset names
usually do, takes the full `datasets.list` walk, as every Go filter does. The dbt-labs fork's
connection option `bigquery.get_objects.disable_wildcards=true` makes filters literal, so such names
get direct lookups ([options](REFERENCE.md#query-and-statement-options)).

### G75. A catalog filter naming one project enumerates that project

A wildcard-free catalog filter naming any valid project lists that project after a one-result
`datasets.list` confirms it exists; HTTP 404 yields an empty result, and other errors are reported.
Go lists only the connection's project, so another project's objects and statistics are
unreachable. In both drivers a wildcard filter, or none, covers only the current project
([metadata catalogs](REFERENCE.md#metadata-catalogs)).

### G77. `xdbc_type_name` reports the GoogleSQL type, not the REST spelling

`xdbc_type_name` in `GetObjects` is the GoogleSQL type that `GetTableSchema` publishes as
`BIGQUERY:type`: `INT64`, `FLOAT64`, `BOOL` and `STRUCT<...>` rather than `INTEGER`, `FLOAT`,
`BOOLEAN` and `RECORD`, and `ARRAY<STRING>` for a repeated `STRING`. Go reports the REST name and
drops the repeated mode. A column whose type the driver does not implement keeps its REST name.

### G82. Metadata reads and blind patches retry a bounded number of times; guarded writes never replay blindly

Metadata `GET`s and the table description and column metadata patches, which carry no `If-Match`,
are resent after 5xx responses other than 501, HTTP 408 and 429, quota reasons, transport failures
and timeouts, for at most five attempts. `tables.insert`, whose replay could turn an unseen success
into a 409, is resent only after a 429 or quota rejection, which applied nothing, and the
authorized-view access-list patch re-reads the list after a lost race or an ambiguous failure
([authorized views](DBT-CORE.md#authorized-views)). Go's SDK retries metadata reads,
`tables.insert` and patches on HTTP 500, 502, 503 and 504, `backendError` and `rateLimitExceeded`
until the context ends.

## Configuration and authentication

### G5. Current-schema validation is complete and atomic

Setting the current schema validates the whole dataset ID locally and updates the connection only
after a `datasets.get` confirms the dataset exists. Go's `sanitizeDataset` checks only the first
character and allows hyphens, and `SetCurrentDbSchema` assigns the schema before the lookup, so a
failed lookup leaves the connection on the rejected dataset.

### G6. Current-catalog assignment validates the project

Setting a non-empty current catalog checks the project with a one-result `datasets.list` and updates
the connection only on success, so a missing project returns `NOT_FOUND`; Go assigns it unchecked
and defers the error to a later call. Clearing it makes unqualified names resolve in
`bigquery.project_id`, which the option then reads back; Go then addresses an empty project.

### G12. `bigquery.query.max_billing_tier` is not supported

`bigquery.query.max_billing_tier` and its alias `adbc.bigquery.sql.query.max_billing_tier` return
`NOT_IMPLEMENTED` for every value and `NOT_FOUND` when read, with a message pointing to
`bigquery.query.max_bytes_billed`. Go accepts the option, but Google deprecated
`maximumBillingTier`, so the cap does nothing.

### G13. Endpoints outside Google require explicit trust

Storage, Cloud Storage, Dataproc and Vertex AI endpoint overrides,
`bigquery.auth.access_token_endpoint` and `bigquery.auth.external_account.token_url` return
`INVALID_ARGUMENT` when clients are built unless they are HTTPS on `googleapis.com` or a subdomain
(also `accounts.google.com` for OAuth), authentication is `anonymous`, or
`bigquery.allow_untrusted_endpoint=true` ([endpoint overrides](REFERENCE.md#endpoint-overrides)).
The error names the option, the resolved host and both remedies. A BigQuery REST endpoint set as
an option, including either Go/dbt alias, is explicitly trusted without this opt-in, matching Go
and the dbt-labs fork. Go sends the caller's token to any configured service host and has neither
the token-endpoint options nor the opt-in.

A URI host and the destinations in a `bigquery.auth.credentials` document meet the same rule here,
with no equivalent in Go: it uses a URI host as its endpoint and hands documents to Google's auth
library unchecked, and the fork inherits both. dbt sets no URI, but the fork passes on a document
naming an emulator or proxy, which is refused here.

Driver-issued REST requests refuse HTTP redirects, keeping SQL and upload bodies at the configured
endpoint. Go inherits its HTTP client's default redirect policy. SDK OAuth and STS exchanges remain
an exception here ([UP-100](UPSTREAM.md#up-100-sdk-token-exchanges-follow-redirects)).

### G15. The batch-count result buffer option is not exposed

`bigquery.query.result_buffer_size` and its alias `adbc.bigquery.sql.query.result_buffer_size` are
refused at every scope. Go uses the option, unvalidated and by default 200, as the capacity of its
completed-batch channel; this driver also applies a 64 MiB budget to retained buffer allocations
of completed batches and tunes overlap with `bigquery.query.prefetch_concurrency`. Shared buffers
are counted once within each batch, including the unused capacity retained by slices. A batch
larger than the budget proceeds alone; response pages, conversions and application-owned batches
remain outside this completed-buffer budget.
Each Storage Read stream also reserves its one wire-queue slot before receiving the next SDK
response, avoiding an additional materialized payload waiting to enter a full queue.

### G19. Setting a URI replaces the database configuration

Setting `uri` resets the database configuration to defaults before applying the URI, so settings
applied before it, such as on an initialized database, do not survive; before initialization the
URI is applied first and explicit options override it ([connection URIs](REFERENCE.md#connection-uris)).
Go applies only the fields the URI contains. Connections already open keep their previous
configuration.

### G20. Project identifiers have structural checks only

Like Go, this driver leaves project validity to BigQuery, including project numbers and legacy
domain-scoped IDs. Local checks only keep each identifier a nonempty resource-name segment:
`/`, `\`, ASCII control characters, `.` and `..` are refused with `INVALID_ARGUMENT`; an empty
option value clears the option. Go stores these values unchecked. Qualified table options split
at the final two dots so a domain-scoped project's dots remain in the project component.
The dbt-labs fork also supports domain-scoped references; its stricter component counting and
the corresponding regression coverage are described in [DBT-CORE.md](DBT-CORE.md#8-project-identifiers-are-validated-by-bigquery).
`GetInfo` omits the XDBC maximum catalog-name length rather than applying the modern project-ID
limit to all project forms.

### G28. Legacy-SQL-only query options are not supported

`bigquery.query.use_legacy_sql`, `bigquery.query.allow_large_results` and
`bigquery.query.disable_flattened_results`, with their `adbc.bigquery.sql.query.*` aliases, are
refused at every scope; Go accepts all three. This driver supports GoogleSQL only, and BigQuery
applies the other two only to legacy SQL.

### G41. Authentication and impersonation are database-scoped only

`bigquery.auth_type`, the `bigquery.auth.*` options and the `bigquery.impersonate.*` options are
database options only; on a connection, setting one returns `NOT_IMPLEMENTED` naming the database as
the object that takes it, and reading one returns `NOT_FOUND`. Go also accepts them per connection.
Connections share their database's clients, so a connection-level credential could not apply
consistently.

### G42. Boolean options accept only the two specification spellings

Boolean options accept only `true` and `false`, ignoring ASCII case, and return `INVALID_ARGUMENT`
naming the key for anything else. Go uses `strconv.ParseBool`, which also accepts `1`, `0`, `t`,
`T`, `f` and `F`.

### G43. The specification-default no-op options are accepted rather than refused

`adbc.ingest.temporary`, `adbc.connection.readonly` and `adbc.statement.exec.incremental` accept
`false` as a no-op and read back `false`, and `true` returns `NOT_IMPLEMENTED`; the isolation level
follows G40. Go refuses `adbc.ingest.temporary`, `adbc.connection.readonly` and the isolation level
with `NOT_IMPLEMENTED` whatever the value.

### G44. The endpoint options accept the fork's spellings as well as the Go driver's

`bigquery.endpoint` also accepts `adbc.bigquery.sql.api_endpoint`, which dbt-core 2.0 sends, and
`bigquery.storage_endpoint` the dbt-labs fork's `adbc.bigquery.sql.storage_read_api_endpoint`,
alongside Go's `adbc.bigquery.sql.endpoint` and `adbc.bigquery.sql.storage_endpoint`; Go rejects
both extra names. A bare `host:port` means HTTPS for every endpoint option here, while Go dials
`bigquery.storage_endpoint` over plaintext gRPC, so Go's emulator setting `localhost:9060` is
`http://localhost:9060` here, and Go cannot use a TLS Storage endpoint override.

### G54. Go durations are rendered in ASCII, without a sub-second fraction

Durations read back from options, such as `bigquery.impersonate.lifetime`, use ASCII unit names
and, below one second, the largest whole unit: 1,500 ns reads back as `1500ns` and 2,000 ns as
`2us`, where Go gives `1.5µs` and `2µs`. Parsing accepts `us`, `µs` and `μs`. Only a lifetime with a
sub-millisecond part reads back differently, and the requested lifetime is the same.

### G57. `bigquery.project_id` and `bigquery.dataset_id` are unreadable on a statement

On a connection, `bigquery.project_id`, `bigquery.dataset_id` and their `adbc.bigquery.sql.*`
spellings read the current catalog and schema, as in Go, and setting them returns `NOT_IMPLEMENTED`
naming `adbc.connection.catalog` or `adbc.connection.db_schema`. On a statement, reading them
returns `NOT_FOUND` naming `bigquery.query.default_project_id` or
`bigquery.query.default_dataset_id`, which fall back to the connection's values; Go asks the
connection.

### G68. The connection URI rejects contradictory and ambiguous inputs

The connection URI returns `INVALID_ARGUMENT` for credential parameters the selected `OAuthType`
does not use (such as `AuthCredentials` without `OAuthType`), repeated parameters, user information,
a fragment, more than one path segment, and `ImpersonateDelegates` or `ImpersonateLifetime` without
`ImpersonateTargetPrincipal` ([connection URIs](REFERENCE.md#connection-uris)). Go accepts these:
unused credentials are stored and ignored, the first repeated value wins, user information and
fragments are dropped, and extra path segments become part of the project ID. Go also decodes
`AuthCredentials` twice, so an encoded `+` becomes a space and `%` fails.

### G76. Scopes can shape the credential, not only the impersonated token

`bigquery.auth.scopes` configures service-account, impersonated and external-account credentials,
including those loaded through ADC or JSON, so they can request `https://www.googleapis.com/auth/drive`
to query external tables over Google Sheets or Drive. Go has no such option and requests
`https://www.googleapis.com/auth/cloud-platform`. User refresh tokens require prior consent for
extra scopes in both drivers. Here, `bigquery.auth.scopes` can narrow user tokens to a subset of
the original consent grant.
Without an impersonation layer, `bigquery.impersonate.scopes` also supplies the base scopes when
`bigquery.auth.scopes` is unset, matching the dbt-labs fork. An explicit impersonation layer keeps
its source credential's default scopes unless `bigquery.auth.scopes` overrides them; its target
uses `bigquery.impersonate.scopes`. Discrete `external_account` credentials use the fallback even
before an explicit impersonation layer, as described in [authentication scopes](REFERENCE.md#authentication-scopes).

### G80. A NULL string option value is refused rather than read as `""`

A NULL key or value passed to a string `SetOption` returns `INVALID_ARGUMENT` naming the argument;
Go turns NULL into `""`. For example, `adbc_driver_manager`'s `set_options(key=None)` fails here and
sets `""` in Go.

### G81. Option getters withhold secrets and report unset options as `NOT_FOUND`

Database `GetOption` returns `NOT_FOUND` for write-only credential options such as
`bigquery.auth.credentials` and `bigquery.auth.client_secret`, which Go returns, and for unset
options, where Go returns `""`. `uri` reads back with credentials replaced by `<redacted>`, and the
impersonation target, delegates and scopes return their values, where Go returns `NOT_FOUND` for
these four; with only `bigquery.impersonate.scopes` set, `bigquery.impersonate.lifetime` is
`NOT_FOUND` here and `1h0m0s` in Go. On a connection, `adbc.connection.catalog` reads the project
unqualified names resolve in, and `adbc.connection.db_schema` without a dataset is `NOT_FOUND`, as
are their `bigquery.*` aliases; Go returns `""` for a cleared catalog or an unset schema.

### G83. A connection's current catalog and schema reach its existing statements

Changing `adbc.connection.catalog` or `adbc.connection.db_schema` affects the connection's existing
statements as well as new ones, unless a statement sets its own `bigquery.query.default_*` options;
a running parameter stream keeps the values it started with. Go copies the values into each
statement when it is created, so a dbapi cursor opened before the change keeps querying and
ingesting into the old dataset.

### G89. Impersonated credentials refresh even with an explicit lifetime

An explicit `bigquery.impersonate.lifetime` sets the requested lifetime of each impersonated token
(one hour by default), and tokens still refresh before the `expireTime` IAM returns. In Go, any
explicit lifetime produces a static token, so the connection fails with HTTP 401 once it expires.

### G90. Configured credentials are never silently replaced or ignored

With explicit credentials plus `bigquery.impersonate.target_principal`, the configured credential
is the impersonation source (including `temporary_access_token` and `external_account`), and
`bigquery.auth.quota_project` applies to the impersonated credential; Go impersonates from ambient
ADC and drops the quota project. Impersonation or a quota project under `anonymous`, and any
credential option the selected `bigquery.auth_type` does not read (such as
`bigquery.auth.credentials` under ADC), return `INVALID_ARGUMENT` naming the option. Go impersonates
from ADC under `anonymous` and stores and ignores unused credential options.

For ADC, an explicitly configured quota project overrides `GOOGLE_CLOUD_QUOTA_PROJECT`, matching
Go and the dbt-labs fork. With no explicit value, the SDK's normal ADC quota selection remains.

The dbt-labs fork passes the configured source to impersonation, unlike the Go driver; see
[dbt authentication](DBT-CORE.md#database-options).

### G94. The custom user agent is sent with REST and Storage Read requests only

`bigquery.user_agent` is a database option of printable ASCII text that follows
`bigquery-adbc/<version>` on REST and all Storage Read requests, including resumed row streams
([diagnostics](REFERENCE.md#diagnostics)); Storage Write requests cannot carry it (UP-93 in
[UPSTREAM.md](UPSTREAM.md)). Go also accepts it per connection, appends it to
`Google-BigQuery-ADBC-Go/<version>` on every request, Storage Write included, and reads `""` when
unset, where this driver returns `NOT_FOUND`.

### G96. Credential file errors do not quote the path

A `bigquery.auth.credentials` path or `GOOGLE_APPLICATION_CREDENTIALS` value that cannot be read or
loaded returns an error with the option or variable, the operating system's cause and the remedy,
but not the value, which may be a misplaced base64 key or access token. Go hands the value to the
Google client library, whose file errors name the path.

## C interface and object lifecycle

### G14. Only the driver-manager entrypoint is exported

The library exports only `AdbcDriverBigqueryInit` and `AdbcDriverInit`, so it loads through an ADBC
driver manager or `AdbcLoadDriverFromInitFunc` but cannot be linked directly against the C API. The
Go library also exports every C API function, so direct linking works there.

### G59. Bound C input validation

Before Arrow imports bound data, this driver checks stream callbacks, schema and array descriptors,
schema nesting depth (at most 32 levels), buffer alignment and representable lengths. Detected
defects return `INVALID_ARGUMENT`, naming the column where possible; the statement stays usable and
rejected inputs are released, as adbc.h requires. Imported values then undergo Arrow's full
validation. These checks still require
valid producer-owned memory: the C Data Interface carries no allocation lengths, so the driver
cannot prove buffer capacity or make arbitrary pointers safe ([native API limitations](LIMITATIONS.md#native-api)).

Required output pointers are checked for null before any request. A null handle returns
`INVALID_ARGUMENT` where Go returns `INVALID_STATE`; rows that are null as a whole are rejected,
and a driver panic returns `INTERNAL` and poisons the affected handles. Go passes bound input and
pointers to Arrow Go unchecked and drops a bound array's top-level null bitmap, so invalid input
can crash it, send a request before failing, or lose null rows.

### G84. Connection option, cancel and `GetInfo` edge cases

| Case | This driver | Go driver |
| --- | --- | --- |
| Unknown key set before `AdbcConnectionInit` | `NOT_IMPLEMENTED` from `SetOption` | Accepted; fails at `Init` |
| Order in which buffered options apply at `Init` | The current catalog before the current schema, otherwise the order set; the last value wins across aliases | Random, so `db_schema` can be validated against the wrong project (bug) |
| A buffered option fails at `Init` | `Init` fails and can be retried | A retry returns `INVALID_STATE` "already initialized" (bug) |
| `GetOption` before `Init` | `NOT_FOUND` | `INVALID_STATE` |
| `adbc.connection.autocommit=true` when already on, or `false` twice | No-op | `INVALID_STATE` (bug) |
| Unfiltered `GetObjects` or `GetStatistics` | Lists the current catalog, else `bigquery.project_id` | Lists the project the client was built with |
| `AdbcConnectionCancel` with nothing running | `INVALID_STATE`, as adbc.h specifies | `OK` |
| `GetInfo` with a non-NULL, zero-length `info_codes` | No rows | Every code |

### G85. Statement lifecycle and ingest option edge cases

| Case | This driver | Go driver |
| --- | --- | --- |
| `AdbcStatementRelease` with a result stream open | Stream stays readable | Stream fails |
| `Release` racing `Cancel` or an option getter on another thread | The other call completes, or reports `INVALID_STATE` | The cgo handle lookup can panic, after which every call on every handle returns `INTERNAL` (bug) |
| Executing again after the binding was consumed | `INVALID_STATE` until rebound | Executes, reusing the last row's parameters after a bound `ExecuteUpdate` (bug) |
| A dry run with bound parameters | Types them from the bound schema and keeps the binding | Consumes the binding |
| `ExecuteSchema` with no binding, or after it was consumed | Dry run with placeholders undeclared under the parameter mode, so `?` is typed | Never bound: BigQuery refuses `?`; consumed: dry run of the stored configuration |
| `SetSqlQuery` | Clears the `adbc.ingest.target_*` options | Clears only `target_table` |
| Reading the `adbc.ingest.target_*` and `mode` options | `target_*`: the value set, or `NOT_FOUND` while unset; `mode`: `adbc.ingest.mode.create` until set | `NOT_FOUND` |
| Invalid `adbc.ingest.target_*` value | `INVALID_ARGUMENT` from `SetOption` | Fails when writing |
| `ExecutePartitions` on an ingest statement | Runs the ingest; no partitions, empty schema, row count | `NOT_IMPLEMENTED` (G8) |
| `bigquery.query.create_session=true` with autocommit disabled | `INVALID_STATE` before any request | BigQuery rejects the request with HTTP 400 (bug) |
| `create_append` or `replace` when another writer creates the missing target first | Appends to, truncates or recreates it | Fails with the `tables.insert` conflict |

### G95. `AdbcErrorFromArrayStream` reports a stream without an error as `OK`

For a stream this driver produced that holds no pending error, `AdbcErrorFromArrayStream` returns
an empty, non-null `AdbcError` and sets the status to `OK`, as adbc.h describes and the reference
C++ drivers do. Go returns NULL, which adbc.h reserves for an unsupported call. A stream from
another producer, or a released one, returns NULL and leaves the status unwritten.

## Artifact size

Build both libraries on the same target with symbols stripped. Record each source revision and
toolchain alongside the result. From the Go driver's `go/` directory on Linux:

```sh
git rev-parse HEAD
go version
go build -tags driverlib -buildmode=c-shared -ldflags="-s -w" -o libadbc_driver_bigquery.so ./pkg
stat -c '%s' libadbc_driver_bigquery.so
size -A libadbc_driver_bigquery.so
```

Use the [binary-size report](BINARY-SIZE.md#usage) for this driver. Compare total file bytes and
section sizes separately; a smaller `.text` section does not necessarily mean a smaller library.
