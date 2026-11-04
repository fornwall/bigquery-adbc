# dbt BigQuery driver benchmark

`benchmark.py` compares dbt-core 2.0's opt-in Foundry BigQuery driver with this repository's driver
on the same generated dbt project. It is separate from `run.py` and enforces no timing threshold.

```sh
uv run --directory integration/dbt --locked benchmark.py
```

The default workload is one partitioned and clustered base table, twelve aggregate tables that can
run concurrently, and two generic data tests per model. Each sample starts in an empty dataset.
The script builds or downloads the libraries and verifies connections outside the timed region;
each timed build starts a new dbt process and includes its connection startup. It alternates driver
order across three rounds, checks the base table and pooled metric-table aggregate signature, and
deletes successful datasets (`--keep-datasets` retains them).
`--rounds`, `--models`, `--rows` and `--threads` adjust the run;
`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` is required and has no default, for both benchmarks.
`DBT_BIGQUERY_LOCATION` selects the location.
`--mode both` (the default) compares downloaded `foundry` and staged `local` libraries in separate
caches, both with `DBT_ADBC_BIGQUERY_USE_FOUNDRY=1`. `--mode foundry`, `--mode local` and
`--mode upstream` run one driver; `upstream` selects dbt's default, non-Foundry channel.
`--local-library /absolute/path/to/library.so` stages a saved release library without rebuilding,
so the same harness can measure earlier revisions. Each run records and checks the resolved
driver name, version and SHA-256, and refuses identical Foundry/local binaries.

Pass `--existing` to measure a rebuild of existing relations. Before each timed build, an untimed
`dbt run --full-refresh` creates the model tables, so the build exercises dbt's configuration
lookups and replaceability checks. Preparation logs and artifacts live under the sample's
`prepare/` directory. Distinct query comments exclude preparation jobs from the timed metrics.

The summary reports `dbt build` wall time, completed BigQuery query jobs per second, the warehouse
span from the first job's start to the last job's end, and the remaining client envelope. Raw
samples, service time, slot time, cache hits and statement counts are saved to
`integration/dbt/logs/performance/<timestamp>/results.json`, along with driver hashes and environment
details. Each sample's dbt artifacts remain under `round-<index>-<mode>/target/` beside its log.
Every sample must have the expected CTAS and SELECT job counts, complete timestamps and slot
times, no cache hits, and the expected warehouse signature. The metrics query waits for missing
jobs and timing data; an incomplete result fails the sample. The client envelope excludes client
delays between warehouse jobs; it is not a measurement of total driver overhead. BigQuery and
network variance are large, so compare paired medians across rounds.

Current measurements against the opt-in Foundry driver and the metadata optimization are in
[docs/PERFORMANCE.md](../../docs/PERFORMANCE.md#live-dbt-workload).

To isolate the metadata view used by relation-configuration lookups:

```sh
uv run --directory integration/dbt --locked benchmark_metadata.py --include-materialized-view
```

This creates a scratch partitioned and clustered table, a view and optionally a materialized view.
It alternates 24 `tables.get` requests per view (`BASIC`/`FULL`) after warming the connection,
compares the projected responses, and deletes the scratch dataset. Samples and summaries are
saved to `logs/performance/metadata-<timestamp>-<id>.json`. It uses the same project and location
environment variables as the dbt benchmark; `--pairs` changes the number of pairs (at least 20).

## Historical default-driver result

dbt-core `2.0.0-preview.212` ran the default 13-model, 26-test workload for three alternating
rounds with eight threads:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span | Median client envelope |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bundled `bigquery` | 24.07s | 23.31–26.26s | 1.62 | 20.50s | 3.57s |
| Local `bigquery_foundry` | 10.74s | 9.50–11.29s | 3.63 | 8.99s | 1.76s |

The paired local/bundled wall-time ratio was `0.461`, a 53.9% reduction. Every sample ran the same
39 jobs (13 `CREATE_TABLE_AS_SELECT`, 26 `SELECT`) with no cache hits and the same warehouse
results. Median summed BigQuery service time was similar, 25.00s bundled and 23.91s local; the
bundled driver's per-node timings show client polling latency after BigQuery's short jobs.

A separate three-round local run with 16 threads lowered median wall time from 10.74s to 8.29s
(22.8%).
