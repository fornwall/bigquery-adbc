# Known limitations and validation gaps

Open limitations and missing test coverage. Behavior is documented in the
[driver reference](REFERENCE.md); `G<n>` and `UP-<n>` IDs refer to
[GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md) and [UPSTREAM.md](UPSTREAM.md).

## Limitations

### Transactions and query execution

- Manual-commit mode silently becomes autocommit when the driver misses SQL that ends the
  transaction: a script commits or rolls back and then fails; the credentials lack
  `bigquery.jobs.list` to inspect the script's child jobs; a cancel interrupts that inspection (the
  script then reports `CANCELLED` though it ran); or `jobs.query` loses the response to a
  transaction-ending statement and the follow-up `jobs.get` fails. Later statements autocommit
  until an ADBC commit, rollback or autocommit change gets "No active transaction found"
  (`INVALID_STATE`) and ends the session.
- Without `bigquery.location`, a transaction session is created in the default dataset's location,
  or where BigQuery places it (the US) without one. A manual-commit query on data in another
  location then fails with a misleading `Access Denied`, though it succeeds in autocommit mode. Set
  `bigquery.location` for such transactions.
- A query cancelled during its initial `jobs.query` cannot be stopped remotely if it finishes within
  the ten-second completion wait, or if submission retries never return a job reference
  ([submission and cancellation](REFERENCE.md#query-submission-and-cancellation)).
- `adbc.statement.exec.incremental` is unsupported: `ExecutePartitions` waits for the job to
  complete before returning its schema and partitions.
- The driver ignores `Retry-After`, as the Go client does; BigQuery does not document sending it.

### Reading and writing data

- With Storage Read disabled or unavailable, a row over 20 MiB on a later REST page fails the read,
  since readers never switch after delivering rows
  ([Storage Read](REFERENCE.md#reading-large-results-through-the-storage-read-api)).
- All Storage Read traffic on a database shares one HTTP/2 connection, so three paused readers can
  starve its other reads and new read sessions into `TIMEOUT` while BigQuery is healthy
  ([flow control](REFERENCE.md#reading-large-results-through-the-storage-read-api)).
- A REST request, response body included, must finish within 60 seconds, so a result page needs
  its size in 60 seconds of bandwidth: about 1.4 Mbit/s for an ordinary 10 MB page, but 16 Mbit/s
  for a row of control characters, `<` or `>` near the 20 MiB limit, which BigQuery sends as 120 MB
  of JSON. A completion read that BigQuery answered late leaves less time.
- An `INTERVAL` whose time part exceeds 2,562,047:47:16.854775, the most Arrow's nanosecond field
  holds, fails the read with `INVALID_DATA` on both paths, though BigQuery allows 87,840,000
  hours. Select it as `CAST(x AS STRING)` or as the `EXTRACT` parts you need.
- Storage Write schemas are fixed for an ingestion. Like the Go driver, the driver ignores
  `AppendRowsResponse.updated_schema`, so a concurrent incompatible change fails with
  `SCHEMA_MISMATCH_*` before commit.
- [Bulk ingest](REFERENCE.md#bulk-ingest) lists unsupported types and column names, and each
  mode's failure behavior.

### Authentication and access

- [Authorized-view grants](DBT.md#authorized-views) compare view projects literally, but BigQuery
  stores a numeric view project as its canonical ID, so repeating a grant with a numeric view
  project can append duplicate ACL entries, as in the dbt-labs fork. Numeric source-dataset projects
  do not cause this. Use the canonical project ID in view names.
- The discrete `bigquery.auth.external_account.*` options cannot configure workforce pools, which
  need a workforce user project; use a complete credential document with
  `bigquery.auth_type=json_credential_string`.

### Native API

- Option getters other than the progress getters wait for a running execution, because FFI calls
  serialize on the handle's state mutex.
- The shared library must stay loaded for the life of the process, because `AdbcDriver.release`
  does not join the runtime's worker threads.
- Handles and result streams do not survive `fork`, because the runtime's worker threads do not; a
  child must open its own database. Through the C API, an inherited handle returns `INVALID_STATE`,
  and releasing an inherited handle or result stream leaks it, since its destructors could wait on
  locks held by threads the fork did not copy. In Rust, a blocking call panics.
- FFI imports trust the producer's memory. The C Data Interface carries no allocation lengths, so
  even `validate_full` can read out of bounds if producer-supplied offsets overstate an allocation.

## Validation gaps

- No live test uses a workload or workforce identity provider or driver-managed impersonation.
  Their token exchanges, refresh and failure handling have mock coverage only, and JSON
  impersonation principal inference unit coverage only.
- URI host and credential-document trust checks have unit coverage only. No test observes
  impersonation sending the source token to `iamcredentials.{universe_domain}`; that is known from
  the auth library's source.
- Successful Dataproc and Vertex AI execution and polling in `src/dbt` have mock coverage only. The
  live dbt suite checks a real Cloud Storage upload followed by a rejected Dataproc submission, not a
  successful Python model or Dataproc `requestId` deduplication.
- CI never runs dbt against BigQuery. It checks that each
  [patched dbt](../tools/dbt-build/README.md) loads this driver, and on a tag that it downloads and
  verifies the release's asset, and it tests the dbt integration suite's assertion helpers, but not
  dbt or the warehouse fixtures. The suite runs manually with ADC, so other dbt profile
  authentication methods are not covered end to end. It and the only tests of the patched dbt's
  `install.sh` and `system update` run only on Linux.
- Linux compatibility with a stock dbt needs validation: release builds target `manylinux_2_28`,
  dbt's libraries `manylinux_2_17`.
- Transient query failures have mock coverage only: HTTP 400 retries for `jobRateLimitExceeded`,
  `jobBackendError`, `jobInternalError` and `tableUnavailable`, terminal-failure confirmation through
  `jobs.get`, and retry classification from the first job error (`src/error/status.rs`,
  `src/query/direct/poll.rs`, `src/query/response.rs`).
- Recovery of a replayed `jobs.query` through BigQuery's undocumented 409 wording (G47) has mock
  coverage only, as does recovery after BigQuery refuses a `jobs.insert` replay, which assumes a
  refusal rather than a 409 can follow a lost attempt that created the job. The `jobs.insert`
  conflict, cross-region `jobs.list` recovery, and cancellation after a lost submission response
  have [live coverage in `asia-northeast1`](../tests/bigquery/regional_recovery.rs), which drops
  real responses in one region; delayed listing propagation, regional outages and multi-page
  recovery searches have mock coverage only.
- Storage Read reshaping beyond the types BigQuery sends (decimal widening, timestamp-zone
  conversion, interval reshaping) has unit coverage only (`src/storage_read/reshape.rs`).
- No test writes a `project.catalog.namespace.table` destination, because the integration project
  has no BigLake catalog; the live test only shows that BigQuery resolves one to the dataset its SQL
  names. Writing to Iceberg tables and reading their results are unverified.
- The [Driver Foundry suite](../integration/foundry-validation/README.md)'s live
  `test_get_statistics` checks only `row_count >= 0` (UP-38). Its two scratch datasets share a
  project, so its catalog tests do not cover switching projects; the Python
  [`test_current_catalog_and_schema_changes`](../python/tests/test_bigquery.py) does.

## Emulator coverage

The emulator's [opt-in runner](../CONTRIBUTING.md#bigquery-emulator) supports the Rust and Python
DB-API suites; the other client suites and the ignored FFI live tests have not been adapted.

- `storage_read_matches_rest_for_every_mapped_type` and
  `storage_read_matches_rest_for_parameterized_decimals` can fail in the concurrent Rust suite if the
  emulator evicts completed results before they are read.
- The runner seeds a synthetic one-row `bigquery-public-data.samples.shakespeare` table with the
  public schema for catalog-switching tests. `queries_through_rich_uri_endpoint_configuration`,
  `uses_current_catalog_and_schema_as_query_defaults` and `gets_public_table_statistics` still need
  its 164,656 rows, and `creates_the_transaction_session_where_the_default_dataset_is` needs
  `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` in `EU`, so
  these four need real BigQuery or full fixtures.
- Tests that need real IAM, credentials, Cloud Storage, error responses, dataset metadata or regional
  job placement call `require_real_bigquery` (`tests/bigquery/backend.rs`) and refuse emulator mode.
