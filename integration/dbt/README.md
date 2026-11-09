# dbt 2.0 integration project

Runs [dbt 2.0](../../docs/DBT.md), built by [`tools/dbt-build`](../../tools/dbt-build/README.md),
against BigQuery with ADC, using this driver or dbt's
[canonical driver](../../docs/DBT.md#installing). [KNOWN_ISSUES.md](KNOWN_ISSUES.md) lists the
issues the suite pins; [PERFORMANCE.md](PERFORMANCE.md) describes the separate benchmark.

## Coverage

- Seeds, SQL materializations, snapshots and two-wave incremental updates, including nested
  schema changes, type parameters and CSV edge cases.
- Unit/data tests, source freshness, lineage and hooks. An isolated failing-test probe must report
  `fail`, one failing row and a nonzero exit; skips and execution errors do not pass it.
- Table configuration and metadata readback, including unchanged materialized-view refreshes
  (not for the canonical driver, whose zero `ExpirationTime` triggers alteration).
- Rich types and boundary values, plus confirmed REST pagination at 60,005 rows and Storage Read
  at 200,000 rows without fallback.
- Copies, authorized views, documentation patches, and a Cloud Storage upload followed by a
  rejected Dataproc submission. Successful Python models remain unvalidated.
- Adapter row counts, costs, IDs and locations. MERGE and copy jobs must preserve dbt's labels and
  timeouts; the canonical driver omits copy options. Script-response differences are pinned.
- `run-operation --sql` parity: JSON events and warehouse results for DDL, DML, MERGE, SELECT and
  Jinja-rendered statements through both drivers.

The runner checks warehouse objects through BigQuery, validates `catalog.json`, and requires macros
to log completion in the current invocation. Each build, seed and freshness phase must return
exactly [coverage.json](coverage.json)'s node IDs, successfully and once each; only the named
ephemeral model, which dbt inlines into separately tested consumers, may be skipped.

## Running

Needs `uv`, rustup, `protoc` and ADC with dataset/job access and upload/read/delete access to
`DBT_BIGQUERY_GCS_BUCKET`.

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients dbt -- --ignored                 # this repository's driver
uv run --directory integration/dbt --locked run.py                  # the same, without Cargo
uv run --directory integration/dbt --locked run.py --upstream-adbc  # dbt's canonical driver
uv run --directory integration/dbt --locked run.py --compare        # both, then compare results
uv run --directory integration/dbt --locked run.py --dbt-archive PATH  # an existing tools/dbt-build archive
```

Each run builds dbt unless given `--dbt-archive`, unpacks the archive into a project-local cache and
runs what it ships. A non-release archive loads its own `lib/bigquery-adbc/`; a release archive has
none, so the run removes any cached copy and requires dbt to download and report the driver.
`--upstream-adbc` sets
[`BIGQUERY_ADBC_USE_DBT_DRIVER=1`](../../tools/dbt-build/README.md#switching-to-dbts-driver) and
requires dbt to report resolving its own driver. `--compare` requires identical `dbt show` values,
`catalog.json` metadata, relations, schemas, descriptions, labels, partitioning, clustering,
constraints, view definitions and row multisets, except the precision and scale of the seed column
declared `numeric(20,4)`, which only this driver keeps.

| Variable | Default | Controls |
| --- | --- | --- |
| `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` | Required; no default | Project for every dataset and job. |
| `DBT_BIGQUERY_LOCATION` | `US` | Dataset and job location. |
| `DBT_BIGQUERY_SCHEMA` | `bigquery_adbc_integration_local` or `_upstream` (`_compare` for `--compare`) | Main dataset; must start with `bigquery_adbc_integration_`. |
| `DBT_BIGQUERY_GCS_BUCKET` | `<project>-adbc-scratch` | Writable bucket for the python model upload. |
| `DBT_BIGQUERY_COMPUTE_REGION` | `us-central1` | Dataproc region of the python-model submission. |
| `DBT_BIGQUERY_DATAPROC_CLUSTER` | `bigquery-adbc-integration-absent-cluster` | Cluster the submission targets; keep it nonexistent. |
| `DBT_FOUNDRY_DRIVER_VERSION` | the pinned dbt version's | [Foundry driver](../../docs/DBT.md#installing) version the [benchmark](PERFORMANCE.md) stages; set it for another dbt 2.0 build. |

The profile uses the integration project as `quota_project` and requests only `cloud-platform`,
because dbt's default Drive scope makes ordinary ADC refresh fail with `invalid_scope`. Runs verify
the driver file's hash and dbt's resolution log, and run from the project directory for relative
seed paths.

Successful runs delete their datasets, including the `_authorized_source` companion and the
`run-operation` parity datasets, unless given `--keep-datasets`; failed and interrupted runs keep
them, and the next run drops them first. Logs go to `integration/dbt/logs/` and dbt artifacts to
`integration/dbt/target/`, with each phase's saved under `target/<mode>/phases/<phase>/`, even on
failure. For an intended selection change, review missing or unexpected IDs against those artifacts
and update `coverage.json` in the same commit; do not remove tests merely to pass the baseline.

## Offline regression tests

[tests/harness/test_dbt_runner.py](../../tests/harness/test_dbt_runner.py) checks, with local
fixtures and mocks, that a run cannot pass with the wrong driver, stale artifacts or completion
logs, or missing or unsuccessful nodes. CI runs it through `scripts/run-offline-python-tests.sh`;
to run it alone from the repository root:

```sh
uv run --directory integration/dbt --locked \
  python -m unittest discover -s ../../tests/harness -p test_dbt_runner.py -q -b
```
