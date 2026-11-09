# Known limitations and validation gaps

Open limitations and missing test coverage. Behavior is documented in the
[driver reference](REFERENCE.md); `G<n>` and `UP-<n>` IDs refer to
[GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md) and [UPSTREAM.md](UPSTREAM.md).

## Limitations

### Transactions and query execution

- Manual-commit mode can silently become autocommit when SQL ends the transaction undetected: a
  script commits or rolls back and then fails, credentials lack `bigquery.jobs.list` for inspecting
  its child jobs, or `jobs.query` loses the response to a transaction-ending statement and the
  follow-up `jobs.get` fails. Later statements then autocommit until an ADBC commit, rollback or
  autocommit change receives "No active transaction found" (`INVALID_STATE`) and ends the session.
- In manual-commit mode without `bigquery.location`, the transaction session is created in the
  default dataset's location, or where BigQuery places it (the US) without one. A query on data in
  another location then fails with a misleading `Access Denied`, although it succeeds in autocommit
  mode. Set `bigquery.location` for such transactions.
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
  starve its other reads and new read sessions into `TIMEOUT`, even though BigQuery is healthy
  ([flow control](REFERENCE.md#reading-large-results-through-the-storage-read-api)).
- Storage Write schemas are fixed for an ingestion. Like the Go driver, the driver ignores
  `AppendRowsResponse.updated_schema`, so a concurrent incompatible change fails with
  `SCHEMA_MISMATCH_*` before commit.
- [Bulk ingest](REFERENCE.md#bulk-ingest) lists unsupported types and column names, and each
  mode's failure behavior.

### Authentication and access

- [Authorized-view grants](DBT.md#authorized-views) compare view project spellings literally.
  BigQuery stores numeric view project numbers as canonical IDs, so repeating a grant with a numeric
  view project can append duplicate ACL entries, as in the dbt-labs fork; source-dataset project
  numbers do not cause this. Use the canonical project ID in the view name.
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
  but a call can block forever if another thread was creating or releasing a handle during the
  fork, since that lock stays held. In Rust, a blocking call panics.
- FFI imports trust the producer's memory. The C Data Interface carries no allocation lengths, so
  even `validate_full` can read out of bounds if producer-supplied offsets overstate an allocation.

## Validation gaps

- Federation and impersonation token exchanges, including refresh and failure handling, have mock
  coverage only, and JSON impersonation principal inference has unit coverage only; no live
  workload or workforce identity provider or driver-managed impersonation is tested.
- URI host and credential-document trust checks have unit coverage only. No test observes
  impersonation sending the source token to `iamcredentials.{universe_domain}`; that comes from
  reading the auth library.
- Successful Dataproc and Vertex AI execution and polling in `src/dbt` have mock coverage only. The
  live dbt suite checks a real Cloud Storage upload followed by a rejected Dataproc submission, not a
  successful Python model or Dataproc `requestId` deduplication.
- CI checks that each [patched dbt](../tools/dbt-build/README.md) loads this driver, and on a tag
  that it downloads and verifies the release's asset, but never runs it against BigQuery. The dbt
  integration suite, which does, runs only on Linux, as do the only tests of the patched dbt's
  `install.sh` and `system update`. Linux compatibility with a stock dbt still needs validation:
  release builds target `manylinux_2_28`, dbt's libraries `manylinux_2_17`.
- The dbt integration suite runs manually against BigQuery with ADC. CI tests its assertion helpers
  but not dbt or the warehouse fixtures. Other dbt profile authentication methods are not covered
  end to end.
- Transient query failures have mock coverage only: HTTP 400 retries for `jobRateLimitExceeded`,
  `jobBackendError`, `jobInternalError` and `tableUnavailable`, terminal-failure confirmation through
  `jobs.get`, and retry classification from the first job error (`src/error/status.rs`,
  `src/query/direct/poll.rs`, `src/query/response.rs`).
- Recovery of a replayed `jobs.query` through BigQuery's undocumented 409 wording (G47) has mock
  coverage only. The `jobs.insert` conflict, cross-region `jobs.list` recovery, and cancellation
  after a lost submission response have [live coverage in `asia-northeast1`](../tests/bigquery/regional_recovery.rs),
  which drops real responses in one region; delayed listing propagation, regional outages and
  multi-page recovery searches have mock coverage only.
- Storage Read reshaping beyond the types BigQuery sends (decimal widening, timestamp-zone
  conversion, interval reshaping) has unit coverage only (`src/storage_read/reshape.rs`).
- Exact metadata filters in a dataset created with `isCaseInsensitive` (G56, G74) have mock coverage
  only.
- The [Driver Foundry suite](../integration/foundry-validation/README.md)'s live
  `test_get_statistics` checks only `row_count >= 0` (UP-38). Its two scratch datasets share a
  project, so its catalog tests do not cover switching projects; the Python
  [`test_current_catalog_and_schema_changes`](../python/tests/test_bigquery.py) does.

## Emulator coverage

The emulator's [opt-in runner](../CONTRIBUTING.md#bigquery-emulator) supports the Rust and Python
DBAPI suites; the other client integration runners and ignored FFI live tests have not been adapted.

- `storage_read_matches_rest_for_every_mapped_type` and
  `storage_read_matches_rest_for_parameterized_decimals` can fail in the concurrent Rust suite if the
  emulator evicts completed results before they are read.
- The runner seeds a synthetic one-row `bigquery-public-data.samples.shakespeare` table with the
  public schema for catalog-switching tests. `queries_through_rich_uri_endpoint_configuration`,
  `uses_current_catalog_and_schema_as_query_defaults` and `gets_public_table_statistics` still need
  its 164,656 rows, and `creates_the_transaction_session_where_the_default_dataset_is` needs
  `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` in `EU`; these
  four need their full fixtures or real BigQuery.
- IAM, real credentials and Cloud Storage need real services; no emulator fixture substitutes for
  them.
