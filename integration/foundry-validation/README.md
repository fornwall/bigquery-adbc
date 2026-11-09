# Driver Foundry validation

The pinned [Driver Foundry validation suite](https://github.com/adbc-drivers/validation) runs
against an installed `bigquery-adbc` wheel, loading its library through `driver_path()`. For dbt's
separate `bigquery_foundry` channel, see [DBT.md](../../docs/DBT.md#installing).

## Running

The project variable is required for every command, with no default. Inspect the suite without
credentials, a native library or BigQuery resources (both commands sync the locked Python 3.15
environment and use placeholder dataset names; collection still checks the collection baseline):

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients foundry::collection_matches_baseline -- --ignored
cargo test --locked --test clients foundry::show_queries -- --ignored
```

Live runs, with ADC:

```sh
cargo test --locked --test clients foundry::suite_passes -- --ignored
PYTEST_ADDOPTS='-k ingest' cargo test --locked --test clients foundry::suite_passes -- --ignored
PYTEST_ADDOPTS='-k test_create_dictionary --durations=10' \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
# JUnit paths are relative to integration/foundry-validation/.
PYTEST_ADDOPTS=--junitxml=foundry-results.xml \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

[`tests/clients/foundry.rs`](../../tests/clients/foundry.rs) rebuilds and installs the wheel
(reusing `target/` unless `CARGO_TARGET_DIR` is set; see
[build-cache measurements](../../docs/PERFORMANCE.md#foundry-validation-build-cache)), creates
isolated datasets, runs pytest and cleans up on success, failure or interruption. Ctrl-C stops
pytest within five seconds; dataset cleanup finishes after Cargo returns. Run one invocation per
checkout, since the wheel and environment are shared; parallel pytest workers would also need
separate datasets and aggregated coverage checks.

## Coverage

- [`GetObjects` filters](https://github.com/adbc-drivers/validation/pull/324) include and exclude
  tables and views individually and together.
- Ingestion covers modes, explicit namespaces, schema conflicts, large/multiple batches and
  1–128 KiB string/binary values. Mode helpers require two columns, so three-column timezone
  fixtures run only in `test_create`.
- [Dictionary ingestion](https://github.com/adbc-drivers/validation/pull/317) runs all 30 ingest
  fixtures with repeated indices and nulls; the second timezone-aware column stays plain.
- Dictionary and [stream binding](https://github.com/adbc-drivers/validation/pull/306) run all 28
  scalar fixtures through INSERT `ExecuteUpdate`, one job per row. Streams include empty batches;
  native and wheel tests separately cover entirely empty input and `ExecuteQuery`.

Dictionary/stream cases derive from the query set; mode and long-value cases select compatible
column shapes explicitly. Negative-scale decimals must keep exact values. Nanosecond inputs,
including signed timestamp extremes, test conversion to microseconds: times truncate and timestamps
round down, including before the epoch.

Disabled capabilities are CHECK/UNIQUE constraints, temporary ingest tables and DDL row counts. The
86 expected skips cover the first three and unsupported SQL output types (narrow integers, FLOAT32,
non-microsecond timestamps); DDL row counts are gated inside tests. Arrow inputs that the driver
widens or converts stay covered.

Both scratch datasets are in one project, so catalog tests validate explicit qualification and
setting the current project back to itself, not switching projects; the Python
[`test_current_catalog_and_schema_changes`](../../python/tests/test_bigquery.py) covers switching
to `bigquery-public-data`. The statistics override accepts stale approximate counts
([UP-38](../../docs/UPSTREAM.md#up-38-driver-foundry-assumes-approximate-statistics-are-immediately-current)).

## Baselines

A run selecting the whole suite (including with an explicit `tests` path or its parent directory)
checks the collected and skipped node IDs against `collection_baseline.txt` and
`skip_baseline.txt`; `-k`, `-m`, `-x`, or a path or node ID below the suite root turns the checks
off. After reviewing an intended coverage change, regenerate both with a full run:

```sh
ADBC_FOUNDRY_REGENERATE_COLLECTION_BASELINE=1 \
ADBC_FOUNDRY_REGENERATE_SKIP_BASELINE=1 \
    cargo test --locked --test clients foundry::suite_passes -- --ignored
```

Collection-only changes can set `ADBC_FOUNDRY_REGENERATE_COLLECTION_BASELINE=1` for
`foundry::collection_matches_baseline`; collection errors never overwrite the file. Skip
regeneration needs a successful, unfiltered live run, since offline collection cannot validate
runtime skips.

## Offline tests

`scripts/run-offline-python-tests.sh` (and so CI) runs all 111 upstream `test_lint_query` cases,
validating the merged upstream and BigQuery fixture metadata without opening a connection. An
explicit test path turns off the coverage guards, so they can run alone:

```sh
uv run --project integration/foundry-validation --locked python -m pytest \
    integration/foundry-validation/tests/test_query.py::TestQuery::test_lint_query
```
