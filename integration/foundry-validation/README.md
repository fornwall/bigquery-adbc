# Driver Foundry validation

[Driver Foundry](https://github.com/adbc-drivers) is a collection of vendor- and community-built
ADBC drivers, including the Go BigQuery driver, with a shared validation suite. This project, for
contributors, runs the pinned [`adbc-drivers/validation`](https://github.com/adbc-drivers/validation)
suite against an installed `bigquery-adbc` wheel. The native library is always found through the
package's public `driver_path()` API. dbt's `bigquery_foundry` driver channel is a separate thing;
see [DBT-CORE.md](../../docs/DBT-CORE.md#installing).

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

[`tests/clients/foundry.rs`](../../tests/clients/foundry.rs) drives the suite, and pytest reads its
options from `PYTEST_ADDOPTS`. The test builds and installs a wheel, creates isolated BigQuery
datasets, runs pytest sequentially with the required environment variables, and drops the
datasets, also when a test fails or the run is interrupted. After Ctrl-C, the test stops pytest
within five seconds, and Cargo returns to the prompt at once, while the drop finishes.
`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` is required, including
for collection and query inspection. Missing or empty values fail; there is no project default.

Cargo reuses this checkout's `target/` by default, checking source and dependency freshness before
each wheel build. Set `CARGO_TARGET_DIR` to use another build cache or a fresh directory for a
clean build. The wheel is rebuilt and reinstalled on every live run. Build timing and reproduction
commands are in [PERFORMANCE.md](../../docs/PERFORMANCE.md#foundry-validation-build-cache).

Run one invocation at a time per checkout: the installed wheel and Python environment are shared.
Pytest workers are not configured; the upstream fixtures share table names and connections within
each dataset. Parallel execution would need separate datasets per worker and aggregate coverage
guards, so adding `pytest-xdist` alone is insufficient.

Pass `--junitxml` to write a JUnit report. Relative paths are resolved from
`integration/foundry-validation/`:

```sh
PYTEST_ADDOPTS=--junitxml=foundry-results.xml \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

## Coverage

- `GetObjects` table-type filtering uses the upstream test from
  [adbc-drivers/validation#324](https://github.com/adbc-drivers/validation/pull/324), with
  `get_objects_tabletypes_filter` enabled. It checks inclusion and exclusion of both tables and
  views, including filters that accept both types.
- Ingestion modes run with ordinary strings, string views, binary views, INT64, decimals
  (including negative scales), nanosecond times, and microsecond and nanosecond timestamps. They
  cover append, replacement, replacement with an explicit catalog and schema, schema conflicts,
  large batches and multiple batches. The upstream mode helpers require two-column fixtures, so the
  three-column timezone-aware timestamp fixtures run in `test_create`. Long-value cases cover
  ordinary, large and view string and binary inputs from 1 to 128 KiB.
- Dictionary binding covers all 28 scalar input cases: strings and binary values (including large,
  fixed-size and view representations), booleans, integers, floats (including Float16), decimals,
  dates, times and timestamps, through the INSERT and `ExecuteUpdate` binding path.
- Dictionary ingestion uses the upstream test from
  [adbc-drivers/validation#317](https://github.com/adbc-drivers/validation/pull/317) for all 30
  ingestion fixtures, including repeated indices and nulls, through the Storage Write API.
  It encodes the first value column; the second timezone-aware timestamp column stays plain.
- Stream binding uses the upstream tests from
  [adbc-drivers/validation#306](https://github.com/adbc-drivers/validation/pull/306): all 28 scalar
  fixtures, including Float16, bound as a single batch, multiple batches, and multiple batches with
  empty batches before, between and after the data. Each stream is consumed by one INSERT
  `ExecuteUpdate` call; the driver submits one BigQuery job per parameter row. Native and
  installed-wheel tests cover `ExecuteQuery` and entirely empty inputs.

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
