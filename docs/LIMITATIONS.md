# Known limitations and validation gaps

Open limitations and missing test coverage, for evaluators deciding whether the driver fits and
contributors looking for gaps. User-facing behavior is documented in the
[driver reference](REFERENCE.md).

## Limitations

### Transactions and query execution

- Manual-commit mode can silently become autocommit if SQL ends the transaction and the driver
  cannot detect it. This happens when a script commits or rolls back and then fails, or when
  credentials lack `bigquery.jobs.list` for inspecting its child jobs. It can also happen when
  `jobs.query` loses the response to a transaction-ending statement and the follow-up `jobs.get`
  fails. Later statements then autocommit until an ADBC commit, rollback or autocommit change
  receives "No active transaction found" (`INVALID_STATE`) and ends the session.
- With `adbc.connection.autocommit` set to `false` and no `bigquery.location`, the transaction
  session is created in the default dataset's location, or where BigQuery places it (the US)
  without one. A query on data in another location then fails with a misleading `Access Denied`,
  although it succeeds in autocommit mode. Set `bigquery.location` for such transactions.
- During initial `jobs.query` submission, remote cancellation must wait for the response to name
  an unfinished job. The request waits up to ten seconds for completion, plus transport delays;
  local work stops immediately, but a query that finishes during the wait cannot be stopped.
  If submission retries never return a job reference, the remote work cannot be cancelled
  ([submission and cancellation](REFERENCE.md#query-submission-and-cancellation)).
- `adbc.statement.exec.incremental` is unsupported. `ExecutePartitions` waits for the job to
  complete before returning its schema and partitions.
- The driver ignores `Retry-After`, as the Go client does; BigQuery does not document sending it.

### Reading and writing data

- When Storage Read is disabled or unavailable, a row over 20 MiB on a later REST page fails the
  read. Readers cannot switch after delivering rows without proving identical order across APIs.
  With Storage Read available, [paginated results select it before delivery](DBT-CORE.md#2-paginated-results-select-storage-read-before-delivering-rows).
- All Storage Read traffic on a database shares one HTTP/2 connection, so three paused readers
  holding unread rows can starve its other reads and new read sessions, which then fail with
  `TIMEOUT` ([details](REFERENCE.md#reading-large-results-through-the-storage-read-api)). A healthy
  read can fail this way even though BigQuery is fine; finish or release a result first.
- Storage Write schemas are fixed for an ingestion. Like the Go driver, the driver ignores
  `AppendRowsResponse.updated_schema`, so a concurrent incompatible change fails with
  `SCHEMA_MISMATCH_*` before commit.

See [bulk ingest](REFERENCE.md#bulk-ingest) for unsupported types and column names, and for the
failure behavior of each ingest mode.

### Authentication and access

- Authorized-view grants compare project spellings literally. BigQuery stores numeric view
  project numbers as canonical IDs, so repeating a grant with a numeric view project can append
  duplicate ACL entries, as in the [dbt-labs fork](DBT-CORE.md). Use the canonical project ID in the
  view name.
- The discrete `bigquery.auth.external_account.*` options cannot configure workforce pools, which
  need a workforce user project; use a complete credential document in
  `bigquery.auth.credentials` with `bigquery.auth_type=json_credential_string`.

### Native API

- Option getters other than the progress getters wait for a running execution, because FFI calls
  serialize on the handle's state mutex.
- The shared library must stay loaded for the life of the process, because `AdbcDriver.release`
  does not join the runtime's worker threads.
- Handles and result streams do not survive `fork`, because the runtime's worker threads do not. A
  child must open its own database. Through the C API, using an inherited one returns
  `INVALID_STATE`, but a call can block forever if another thread was creating or releasing a handle
  when the process forked, since that lock stays held. In Rust, a blocking call panics.
- FFI imports trust the producer's memory. The C Data Interface carries no allocation lengths, so
  the driver cannot check buffer capacity, and even `validate_full` can read out of bounds if
  producer-supplied offsets overstate an allocation.

## Validation gaps

- Federation and impersonation token exchanges have mock coverage, including refresh and failure
  handling, but no live workload/workforce identity provider or driver-managed impersonation
  coverage. JSON impersonation principal inference is also checked locally only.
- URI host and credential-document trust checks have unit coverage only. That impersonation sends
  the source token to `iamcredentials.{universe_domain}` comes from reading the auth library; no
  test observes the request.
- Successful Dataproc and Vertex AI execution and polling in `src/dbt` have mock coverage only.
  The live dbt suite checks a real Cloud Storage upload followed by a rejected Dataproc
  submission; it does not validate a successful Python model or Dataproc `requestId` deduplication.
- CI checks that each [patched dbt](../tools/dbt-build/README.md) loads this driver, and on a tag
  that it downloads and verifies the release's asset, but never runs it against BigQuery. The dbt
  integration suite, which does, runs only on Linux.
- The dbt integration suite runs manually against BigQuery with ADC. CI tests its assertion
  helpers, but does not run dbt or the warehouse fixtures. Other dbt profile authentication
  methods are not covered end to end by this suite.
- Transient query failures have mock coverage only: the HTTP 400 retries for
  `jobRateLimitExceeded`, `jobBackendError`, `jobInternalError` and `tableUnavailable`,
  terminal-failure confirmation through `jobs.get`, and retry classification from the first job
  error (`src/error/status.rs`, `src/query/direct/poll.rs`, `src/query/response.rs`).
- Recovery of a replayed [`jobs.query`](GO-DRIVER-COMPARISON.md#g47-submission-retries-preserve-identity-and-named-jobs-can-be-recovered)
  through BigQuery's undocumented 409 wording has mock coverage only. The corresponding
  `jobs.insert` conflict, cross-region `jobs.list` recovery, and cancellation after a lost
  submission response have [live coverage in `asia-northeast1`](../tests/bigquery/regional_recovery.rs).
  That test drops real responses in one region; it does not induce delayed listing propagation,
  regional outages, or multi-page recovery searches, which still have mock coverage only.
- Storage Read reshaping beyond the types BigQuery sends (decimal widening, timestamp-zone
  conversion, interval reshaping) has unit coverage only (`src/storage_read/reshape.rs`).
- Exact metadata filters in a dataset created with `isCaseInsensitive` have mock coverage only
  ([case matching](GO-DRIVER-COMPARISON.md#g56-metadata-filter-patterns-are-matched-case-sensitively),
  [dataset lookup](GO-DRIVER-COMPARISON.md#g74-default-metadata-patterns-cannot-take-an-exact-dataset-lookup-for-a-name-containing-_)).
- The [Driver Foundry suite](../integration/foundry-validation/README.md)'s live
  `test_get_statistics` checks only `row_count >= 0`
  ([UP-38](UPSTREAM.md#up-38-driver-foundry-assumes-approximate-statistics-are-immediately-current)).
- That suite's two scratch datasets share a project, so its catalog tests do not cover switching
  projects. The separate Python `test_current_catalog_and_schema_changes` covers that path.

## Emulator coverage

The emulator is a local stand-in for BigQuery. Its
[opt-in runner](../CONTRIBUTING.md#bigquery-emulator) supports the Rust and Python DBAPI suites.
The other client integration runners and ignored FFI live tests have not been adapted.

`storage_read_matches_rest_for_every_mapped_type` and
`storage_read_matches_rest_for_parameterized_decimals` can fail in the concurrent Rust suite if the
emulator evicts completed results before they are read.

The runner seeds a synthetic one-row `bigquery-public-data.samples.shakespeare` table with the
public schema for catalog-switching tests. `queries_through_rich_uri_endpoint_configuration`,
`uses_current_catalog_and_schema_as_query_defaults` and `gets_public_table_statistics` still
require its 164,656 rows. `creates_the_transaction_session_where_the_default_dataset_is` needs
`bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` in `EU`.
These four tests need their full fixtures or real BigQuery. IAM, real credentials and Cloud
Storage need real services. No emulator-only fixture substitutes for those checks.
