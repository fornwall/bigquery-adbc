# dbt BigQuery driver benchmark

`benchmark.py` times `dbt build` with dbt 2.0's opt-in Foundry BigQuery driver and with this
repository's driver on the same generated project. It is separate from `run.py` and has no timing
threshold. Results are in [docs/PERFORMANCE.md](../../docs/PERFORMANCE.md#live-dbt-workload).

```sh
uv run --directory integration/dbt --locked benchmark.py
uv run --directory integration/dbt --locked benchmark_metadata.py --include-materialized-view
```

Both need `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` (no default); `DBT_BIGQUERY_LOCATION`
selects the location.

## `benchmark.py`

The workload is one partitioned, clustered base table, twelve aggregate tables that can run
concurrently, and two generic data tests per model, in an empty dataset per sample. Libraries are
built or downloaded and connections verified outside the timed region; each timed build is a new
dbt process including connection startup. Driver order alternates across three rounds.

| Flag | Effect |
| --- | --- |
| `--rounds`, `--models`, `--rows`, `--threads` | Adjust the workload. |
| `--mode both` (default) | Downloaded `foundry` and staged `local` libraries in separate caches, both with `DBT_ADBC_BIGQUERY_USE_FOUNDRY=1`. |
| `--mode foundry`, `local`, `upstream` | One driver; `upstream` is dbt's default, non-Foundry channel. |
| `--local-library /absolute/path/to/library.so` | Stage a saved release library instead of building, to measure earlier revisions. |
| `--existing` | Before each timed build, an untimed `dbt run --full-refresh` creates the tables (logs under the sample's `prepare/`), so the build exercises configuration lookups and replaceability checks. Distinct query comments keep preparation jobs out of the metrics. |
| `--keep-datasets` | Keep datasets that would otherwise be deleted after a successful sample. |

Each run records and checks the resolved driver name, version and SHA-256, and refuses identical
Foundry and local binaries. Every sample must have the expected CTAS and SELECT job counts,
complete timestamps and slot times, no cache hits, and the expected base-table and pooled
metric-table aggregate signature; the metrics query waits for missing jobs and timing data, and an
incomplete result fails the sample.

The summary reports `dbt build` wall time, completed query jobs per second, the warehouse span
(first job start to last job end) and the remaining client envelope, which excludes client delays
between jobs and so is not total driver overhead. Raw samples, service and slot time, cache hits,
statement counts, driver hashes and environment go to
`integration/dbt/logs/performance/<timestamp>/results.json`; each sample's dbt artifacts stay under
`round-<index>-<mode>/target/`. Variance is large, so compare paired medians across rounds.

## `benchmark_metadata.py`

Isolates the `tables.get` view used by relation-configuration lookups. It creates a scratch
partitioned, clustered table, a view and optionally a materialized view, warms the connection,
alternates 24 `BASIC`/`FULL` request pairs per relation (`--pairs`, at least 20), compares the
projected responses and deletes the dataset. Output goes to
`logs/performance/metadata-<timestamp>-<id>.json`.

## Historical default-driver result

dbt `2.0.0-preview.212`, default 13-model, 26-test workload, three alternating rounds, eight
threads:

| Driver | Median wall | Range | Median jobs/s | Median warehouse span | Median client envelope |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bundled `bigquery` | 24.07s | 23.31–26.26s | 1.62 | 20.50s | 3.57s |
| Local `bigquery_foundry` | 10.74s | 9.50–11.29s | 3.63 | 8.99s | 1.76s |

Paired local/bundled wall ratio `0.461` (53.9% lower). Every sample ran the same 39 jobs (13
`CREATE_TABLE_AS_SELECT`, 26 `SELECT`), no cache hits, same warehouse results. Median summed
service time: 25.00s bundled, 23.91s local; the bundled driver's per-node timings show client
polling latency after short jobs. With 16 threads, local median wall fell from 10.74s to 8.29s
(22.8%, three rounds).
