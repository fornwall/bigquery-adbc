# dbt 2.0 integration project

This suite runs [dbt-core 2.0](../../docs/DBT-CORE.md) (pinned to 2.0.11) against BigQuery with ADC,
using this driver or dbt's [canonical driver](../../docs/DBT-CORE.md#installing).
[KNOWN_ISSUES.md](KNOWN_ISSUES.md) lists the current issues the suite pins.

## Coverage

- Seeds, SQL materializations, snapshots and two-wave incremental updates, including nested
  schema changes, type parameters and CSV edge cases.
- Unit/data tests, source freshness, lineage and hooks. An isolated failing-test probe must report
  `fail`, one failing row and a nonzero exit; skips and execution errors do not pass it.
- Table configuration and metadata readback, including unchanged materialized-view refreshes.
  The latter excludes the canonical driver because its zero `ExpirationTime` triggers alteration.
- Rich types and boundary values, plus confirmed REST pagination at 60,005 rows and Storage Read
  at 200,000 rows without fallback.
- Copies, authorized views, documentation patches, and a Cloud Storage upload followed by a
  rejected Dataproc submission. Successful Python models remain unvalidated.
- Adapter row counts, costs, IDs and locations. MERGE and copy jobs must preserve dbt's labels and
  timeouts; the canonical driver omits copy options. Script-response differences are pinned.
- `run-operation --sql` parity: compare JSON events and warehouse results for DDL, DML, MERGE,
  SELECT and Jinja-rendered statements through the canonical driver and this one.

The runner independently checks warehouse objects through BigQuery and validates `catalog.json`.
Macros must log completion in the current invocation. Each build, seed and freshness phase must
return exactly [coverage.json](coverage.json)'s node IDs, successfully and once each. Only the named
ephemeral model may be skipped; dbt inlines it into separately tested consumers.

## Running

Install `uv` and dbt 2.0, and log in with ADC; local-driver runs also need Cargo. The project below
is required, with no default. Credentials need dataset/job access and upload/read/delete access to
`DBT_BIGQUERY_GCS_BUCKET` (default `<project>-adbc-scratch`).

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients dbt -- --ignored                 # this repository's driver
uv run --directory integration/dbt --locked run.py                  # the same, without Cargo
uv run --directory integration/dbt --locked run.py --upstream-adbc  # dbt's canonical driver
uv run --directory integration/dbt --locked run.py --compare        # both, then compare results
```

`--compare` requires identical `dbt show` values, `catalog.json` metadata, relations, schemas,
descriptions, labels, partitioning, clustering, constraints, view definitions and row multisets. The
only allowed difference is the precision and scale of the seed column declared `numeric(20,4)`,
which this driver keeps and the canonical driver drops.

| Variable | Default | Controls |
| --- | --- | --- |
| `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` | Required; no default | Project for every dataset and job. |
| `DBT_BIGQUERY_LOCATION` | `US` | Dataset and job location. |
| `DBT_BIGQUERY_SCHEMA` | `bigquery_adbc_integration_local` or `_upstream` (`_compare` for `--compare`) | Main dataset; must start with `bigquery_adbc_integration_`. |
| `DBT_BIGQUERY_GCS_BUCKET` | `<project>-adbc-scratch` | Writable bucket for the python model upload. |
| `DBT_BIGQUERY_COMPUTE_REGION` | `us-central1` | Dataproc region of the python-model submission. |
| `DBT_BIGQUERY_DATAPROC_CLUSTER` | `bigquery-adbc-integration-absent-cluster` | Cluster the submission targets; keep it nonexistent. |
| `DBT_FOUNDRY_DRIVER_VERSION` | the pinned dbt version's | [Foundry driver](../../docs/DBT-CORE.md#installing) version to stage; set it for another dbt 2.0 build. |

The profile uses the integration project as `quota_project` and requests only `cloud-platform`:
dbt's default Drive scope makes ordinary ADC refresh fail with `invalid_scope`. Local runs build
and stage the library in a project-local [Foundry cache](../../docs/DBT-CORE.md#installing), verify
its hash and dbt's resolution log, and run from the project directory for relative seed paths.

Successful runs delete their datasets, including the `_authorized_source` companion and the
`run-operation` parity datasets; pass `--keep-datasets` to retain them. Failed and interrupted runs
always retain them, and the next run starts by dropping them. Logs are under `integration/dbt/logs/` and dbt artifacts under `integration/dbt/target/`.
Each phase removes stale artifacts and saves new ones under `target/<mode>/phases/<phase>/`, even
on failure. For an intended selection change, review missing/unexpected IDs against those artifacts
and update `coverage.json` in the same commit; do not remove tests merely to pass the baseline.

The separate performance benchmark is described in [PERFORMANCE.md](PERFORMANCE.md).

## Offline regression tests

The unit tests in [tests/harness/test_dbt_runner.py](../../tests/harness/test_dbt_runner.py)
cover conditions that could let a run pass without valid coverage: the wrong driver,
stale artifacts or completion logs, and missing or unsuccessful dbt nodes. They use local
fixtures and mocks and need no dbt, BigQuery or credentials. CI runs them through
`scripts/run-offline-python-tests.sh`. To run them alone from the repository root:

```sh
uv run --directory integration/dbt --locked \
  python -m unittest discover -s ../../tests/harness -p test_dbt_runner.py -q -b
```
