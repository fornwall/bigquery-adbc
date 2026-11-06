# Driver Foundry validation

The pinned [Driver Foundry validation suite](https://github.com/adbc-drivers/validation) runs
against an installed `bigquery-adbc` wheel, loading its library through `driver_path()`.
For dbt's separate `bigquery_foundry` channel, see [DBT-CORE.md](../../docs/DBT-CORE.md#installing).

## Running

Inspect the suite without credentials, a native library, or BigQuery resources:

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients foundry::collection_matches_baseline -- --ignored
cargo test --locked --test clients foundry::show_queries -- --ignored
```

Both commands synchronize the locked Python 3.15 environment and use placeholder dataset names.
Collection still checks the collection baseline. Run the live suite with:

```sh
gcloud auth application-default login
cargo test --locked --test clients foundry::suite_passes -- --ignored
PYTEST_ADDOPTS='-k ingest' cargo test --locked --test clients foundry::suite_passes -- --ignored
PYTEST_ADDOPTS='-k test_create_dictionary --durations=10' \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

[`tests/clients/foundry.rs`](../../tests/clients/foundry.rs) builds and installs a wheel, creates
isolated datasets, runs pytest with `PYTEST_ADDOPTS`, and cleans up on success, failure or interruption.
Ctrl-C stops pytest within five seconds; dataset cleanup finishes after Cargo returns.
The project variable is required even for collection and query inspection; there is no default.

Every live run rebuilds and reinstalls the wheel, reusing `target/` unless `CARGO_TARGET_DIR` is set.
See [build-cache measurements](../../docs/PERFORMANCE.md#foundry-validation-build-cache).

Run one invocation per checkout: the wheel and environment are shared. Parallel pytest workers
would also need separate datasets and aggregate coverage checks.

JUnit paths are relative to `integration/foundry-validation/`:

```sh
PYTEST_ADDOPTS=--junitxml=foundry-results.xml \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

## Coverage

- [`GetObjects` type filtering](https://github.com/adbc-drivers/validation/pull/324) checks
  inclusion and exclusion of tables and views, including combined filters.
- Ingestion modes run with ordinary strings, string views, binary views, INT64, decimals
  (including negative scales), nanosecond times, and microsecond and nanosecond timestamps. They
  cover append, replacement, replacement with an explicit catalog and schema, schema conflicts,
  large batches and multiple batches. The upstream mode helpers require two-column fixtures, so the
  three-column timezone-aware timestamp fixtures run in `test_create`. Long-value cases cover
  ordinary, large and view string and binary inputs from 1 to 128 KiB.
- Dictionary binding covers all 28 scalar fixtures, including Float16, through INSERT and
  `ExecuteUpdate`. [Dictionary ingestion](https://github.com/adbc-drivers/validation/pull/317)
  covers all 30 ingestion fixtures, repeated indices and nulls; the second timezone-aware timestamp
  column stays plain.
- [Stream binding](https://github.com/adbc-drivers/validation/pull/306) covers all 28 scalar fixtures
  in one or multiple batches, including interspersed empty batches. One INSERT `ExecuteUpdate`
  consumes each stream, submitting one job per row. Native and installed-wheel tests cover
  `ExecuteQuery` and entirely empty inputs.

Dictionary ingestion and dictionary/stream binding select their fixtures from the query set,
so adding a fixture also exercises those paths. Ingestion mode and long-value selections remain
explicit because their upstream helpers require particular column shapes and types.

Negative-scale decimal fixtures keep their original inputs and expect exact NUMERIC values.
Nanosecond fixtures also keep their original inputs, including the minimum and maximum signed
64-bit timestamps, and expect BigQuery's microsecond precision: times drop sub-microsecond digits
and timestamps round down, including before the Unix epoch. These test conversion, not lossless
nanosecond storage.

All applicable capability flags in the pinned suite are enabled. The disabled flags are CHECK
and UNIQUE constraints, temporary bulk-ingest tables, and DDL affected-row counts. The 86 expected
skips cover the first three plus SQL output types BigQuery cannot represent (narrow integers,
FLOAT32, and non-microsecond timestamp precision); DDL row-count assertions are gated within tests.
These skips do not apply to Arrow inputs that the driver widens or converts.

Both scratch datasets are in the same project. Catalog tests validate explicit qualification and
setting the current project back to itself, not switching between projects. The Python
[`test_current_catalog_and_schema_changes`](../../python/tests/test_bigquery.py) covers switching
to `bigquery-public-data`. The statistics override accepts stale approximate counts (UP-38);
see [UPSTREAM.md](../../docs/UPSTREAM.md).

## Baselines

An unfiltered run checks the collected and skipped test node IDs against the committed
`collection_baseline.txt` and `skip_baseline.txt`. Selecting the whole suite, including with an
explicit `tests` path or its parent directory, keeps these checks on. Filtering with `-k`, `-m`,
`-x`, or a path or node ID below the suite root turns them off.

After reviewing an intended coverage change, regenerate the baselines with a full run:

```sh
ADBC_FOUNDRY_REGENERATE_COLLECTION_BASELINE=1 \
ADBC_FOUNDRY_REGENERATE_SKIP_BASELINE=1 \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

Collection-only changes can regenerate `collection_baseline.txt` by setting
`ADBC_FOUNDRY_REGENERATE_COLLECTION_BASELINE=1` for `foundry::collection_matches_baseline`.
Collection errors never overwrite it. Skip regeneration requires a successful, unfiltered live
run; offline collection cannot validate runtime skips.

## Offline tests

CI runs all 111 upstream `test_lint_query` cases, validating the merged upstream and BigQuery
fixture metadata without opening a connection, through `scripts/run-offline-python-tests.sh`.
An explicit test path turns off the coverage guards, so these checks can also be run alone:

```sh
uv run --project integration/foundry-validation --locked python -m pytest \
    integration/foundry-validation/tests/test_query.py::TestQuery::test_lint_query
```
