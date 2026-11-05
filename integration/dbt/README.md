# dbt 2.0 integration project

This project, for contributors, runs [dbt-core 2.0](../../docs/DBT-CORE.md), dbt Labs' Rust engine
(pinned to `dbt 2.0.11`), against BigQuery with Application Default Credentials (ADC). It uses this
repository's driver by default, or dbt's canonical `bigquery` driver, its
[default channel](../../docs/DBT-CORE.md#installing), for comparison.
[KNOWN_ISSUES.md](KNOWN_ISSUES.md) lists the current issues the suite pins.

## Coverage

- Seeds: typed, pipe-delimited and header-only CSV input, with parameterized decimals, quoted
  delimiters, escaped quotes, Unicode, null and empty values, percentage floats and time-zone
  normalization. `column_types` also load BYTES, JSON and GEOGRAPHY columns from Base64, JSON
  and WKT text.
- Models: views, tables, materialized views, ephemeral and versioned models, snapshots, incremental
  MERGE and static and dynamic insert-overwrite. A two-wave incremental fixture starts with four
  rows and ends with six: two updated rows whose individual key components repeat, an omitted
  source row retained in the destination, a historical row excluded by the MERGE predicate and
  two inserted rows. Protected columns differ in the incoming wave, so selective updates must
  preserve their old values. `sync_all_columns` removes a column and adds a nested one, which
  stays null on matched rows excluded from its update. A full row-multiset assertion detects
  missing, extra, duplicated, stale and unexpectedly null values in both waves.
- Tests and lineage: unit tests, generic and singular data tests, source freshness through both
  `loaded_at_field` and `__TABLES__`, exposures, hooks with audited DDL and DML, and
  `adapter.dispatch`.
- Failure reporting: an isolated, deliberately failing data test must exit nonzero and record
  status `fail` with exactly one failing row. A successful run, skipped test, SQL error or transport
  error fails this check. The probe is disabled during ordinary builds.
- Table configuration: time and integer-range partitioning, clustering, labels, expiration,
  contracts and column documentation. Rebuild phases make dbt read partitioning, clustering and
  materialized-view refresh settings back through `is_replaceable` and `describe_relation`. A
  materialized view rebuilt unchanged must be refreshed with `CALL bq.refresh_materialized_view`,
  not altered, so dbt must read every option back as configured. Only this driver is checked:
  the canonical driver reports a zero `ExpirationTime`, which dbt reads as a change.
- Values: JSON, GEOGRAPHY, BYTES, BIGNUMERIC, DATETIME, TIME and nested ARRAY/STRUCT values,
  boundary and null values, NUL bytes, reserved and Unicode identifiers, a typed zero-row relation,
  a 60,005-row `adapter.execute` with Storage Read explicitly disabled to exercise REST pagination,
  and a 200,000-row `run_query` read through the Storage Read API with default settings, in a phase
  that enables the driver's diagnostics, requires the debug event of each
  read path, and fails on any warning, such as a fallback to REST.
- Option-only driver operations: `copy_partitions` table copies; a `grant_access_to` view on a
  second dataset, rebuilt to check that the grant is idempotent and that its column descriptions
  are patched onto the replaced view; `persist_docs` through the seed, incremental and
  `run-operation` paths; and a python model that uploads its source to Cloud Storage and then
  fails to submit to a nonexistent Dataproc cluster.
- Adapter responses: the incremental wave must report `MERGE`, five affected rows, processed and
  billed bytes, slot milliseconds, matching query and job IDs, project and location. The dynamic
  insert-overwrite model reports `SCRIPT` with no row count, because BigQuery counts DML rows on the
  script's child jobs. Under `--upstream-adbc`, the canonical driver's `SUCCESS` with zero rows is
  pinned instead.
- Job options: the incremental MERGE job and the `copy_partitions` copy jobs must carry exactly
  dbt's `dbt_invocation_id` and `query_comment` labels. Their `jobTimeoutMs` must come from the
  profile's `job_execution_timeout_seconds` or, for the copies, the model's override. The
  canonical driver sets neither on copy jobs, so only this driver's jobs are checked.
- `run-operation --sql` parity: in the default run, the same DDL, DML, MERGE, SELECT and Jinja-rendered statements run
  through the canonical driver and then this driver, and the runner compares dbt's JSON events and
  the resulting tables.

The runner checks warehouse objects, including seed types, nested schemas and row counts,
independently through the BigQuery API, and writes and checks `catalog.json`.
The adapter-surface and description-update macros must log their completion during the current
invocation, so a successful command that skipped their assertions fails the suite.
Every build, seed and freshness phase must return exactly the node IDs in
[coverage.json](coverage.json), with successful statuses. This includes every data and unit test
and both freshness paths; missing, duplicated, unexpected, warned or skipped results fail the
run. The only skipped result allowed is the explicitly named ephemeral model, which dbt inlines
into its consumers and tests separately.

## Running

Install `uv` and the dbt 2.0 binary, and log in with ADC; local-driver runs also need `cargo`. Set
`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` to a project your principal can use. This is the same required
variable as every other integration suite; missing or empty values fail, and there is no default.
Set `DBT_BIGQUERY_GCS_BUCKET` to a bucket where it can upload, read and delete objects; if omitted,
the bucket name is `<project>-adbc-scratch`.

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

The profile sets `quota_project` to the integration project, because Cloud Storage bills requests
to the credentials' quota project. It requests only the `cloud-platform` OAuth scope used by the
suite's Google Cloud services. dbt's default scope list also requests Drive access, which ordinary
`gcloud auth application-default login` credentials do not grant; requesting it makes token
refresh fail with `invalid_scope`.
A local-driver run builds the release library, stages it in a
project-local dbt cache under the [Foundry channel](../../docs/DBT-CORE.md#installing), and checks
dbt's resolution log and the library hash; the global dbt cache is not modified. dbt runs with the project as its working directory,
because dbt passes seed paths relative to it.

Successful runs delete their datasets, including the `_authorized_source` companion and the
`run-operation` parity datasets; pass `--keep-datasets` to retain them. Failed and interrupted runs
always retain them, and the next run starts by dropping them. Logs are under `integration/dbt/logs/` and dbt artifacts under `integration/dbt/target/`.
Each checked phase removes its old result artifact before running and saves the new artifact under
`target/<mode>/phases/<phase>/`, including when dbt fails. A stale successful artifact cannot satisfy
the next invocation's checks. When intentionally changing the suite's selection, review the reported
missing and unexpected IDs against these artifacts and update `coverage.json` in the same commit.
Do not remove missing tests from the baseline just to make a run pass.

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
