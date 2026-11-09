"""Benchmark dbt 2.0 with its opt-in Foundry and local BigQuery drivers."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import local_driver
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from config import require_project

PROJECT_DIR = Path(__file__).resolve().parent
GENERATED_DIR = PROJECT_DIR / "target" / "performance" / "workload"
RESULTS_DIR = PROJECT_DIR / "logs" / "performance"
_JOB_METRIC_POLLS = 8
_JOB_METRIC_POLL_SECONDS = 1.0


@dataclass(frozen=True)
class JobMetrics:
    count: int
    service_seconds: float
    warehouse_span_seconds: float
    total_slot_ms: int
    statement_types: dict[str, int]
    cache_hits: int


@dataclass(frozen=True)
class DriverArtifact:
    name: str
    version: str
    path: str
    sha256: str


@dataclass(frozen=True)
class Sample:
    round: int
    mode: str
    dataset: str
    wall_seconds: float
    jobs: JobMetrics
    signature: dict[str, int]

    @property
    def jobs_per_second(self) -> float:
        return self.jobs.count / self.wall_seconds

    @property
    def client_envelope_seconds(self) -> float:
        """Time outside the span from the first job start to the last job end."""
        return max(0.0, self.wall_seconds - self.jobs.warehouse_span_seconds)


def log(message: str = "") -> None:
    print(message, flush=True)


def command(arguments: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    log(f"  $ {' '.join(arguments)}")
    subprocess.run(arguments, cwd=cwd, env=env, check=True)


def stage_local_driver(cache_home: Path, library: Path | None = None) -> tuple[Path, str]:
    log("\n==> Build and stage the local driver (not timed)")
    if library is None:
        log(f"  $ {' '.join(local_driver.BUILD_COMMAND)}")
        destination, digest = local_driver.stage(cache_home)
    else:
        destination = local_driver.cache_path(cache_home)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, destination)
        digest = local_driver.sha256(destination)
    log(f"  -> staged {destination} ({digest[:12]})")
    return destination, digest


def write_workload(model_count: int, row_count: int) -> None:
    if GENERATED_DIR.exists():
        # Generated under target/; holds no user files.
        shutil.rmtree(GENERATED_DIR)
    models = GENERATED_DIR / "models"
    models.mkdir(parents=True, exist_ok=True)

    (GENERATED_DIR / "dbt_project.yml").write_text(
        """name: bigquery_adbc_performance
version: '1.0.0'
config-version: 2
profile: bigquery_adbc_performance
model-paths: [models]
target-path: target
clean-targets: [target, logs]
query-comment: "bigquery-adbc performance {{ env_var('DBT_PERF_RUN_ID') }}"
"""
    )
    (GENERATED_DIR / "profiles.yml").write_text(
        """bigquery_adbc_performance:
  target: performance
  outputs:
    performance:
      type: bigquery
      method: oauth
      project: "{{ env_var('BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT') }}"
      dataset: "{{ env_var('DBT_BIGQUERY_SCHEMA') }}"
      location: "{{ env_var('DBT_BIGQUERY_LOCATION') }}"
      threads: "{{ env_var('DBT_PERF_THREADS') | int }}"
      job_execution_timeout_seconds: 300
      job_retries: 1
"""
    )
    (models / "base_events.sql").write_text(
        f"""{{{{ config(
    materialized='table',
    partition_by={{'field': 'event_date', 'data_type': 'date'}},
    cluster_by=['group_id'],
    tags=['performance']
) }}}}

select
    event_id,
    mod(event_id, 97) as group_id,
    date_add(date '2020-01-01', interval mod(event_id, 31) day) as event_date
from unnest(generate_array(1, {row_count})) as event_id
"""
    )

    # Each model's unique key, which gets not_null and unique data tests.
    keys = {"base_events": "event_id"}
    for index in range(1, model_count + 1):
        name = f"metric_{index:03d}"
        (models / f"{name}.sql").write_text(
            f"""{{{{ config(materialized='table', tags=['performance']) }}}}

select
    format('%03d|%s', group_id, cast(event_date as string)) as metric_key,
    group_id,
    event_date,
    count(*) as row_count,
    sum(event_id * {index}) as weighted_sum
from {{{{ ref('base_events') }}}}
group by group_id, event_date
"""
        )
        keys[name] = "metric_key"

    (models / "schema.yml").write_text(
        "version: 2\n\nmodels:\n"
        + "".join(
            f"  - name: {model}\n    columns:\n      - name: {key}\n        data_tests: [not_null, unique]\n"
            for model, key in keys.items()
        )
    )


def dbt_arguments(*arguments: str, target_dir: Path, log_dir: Path) -> list[str]:
    return [
        "dbt",
        *arguments,
        *("--project-dir", str(GENERATED_DIR), "--profiles-dir", str(GENERATED_DIR)),
        *("--target-path", str(target_dir), "--log-path", str(log_dir), "--no-version-check"),
    ]


def performance_cache(mode: str) -> Path:
    return PROJECT_DIR / ".cache" / "performance" / mode


def mode_environment(
    mode: str,
    *,
    project: str,
    location: str,
    dataset: str,
    threads: int,
    run_id: str,
) -> dict[str, str]:
    env = os.environ | {
        "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": project,
        "DBT_BIGQUERY_LOCATION": location,
        "DBT_BIGQUERY_SCHEMA": dataset,
        "DBT_PERF_THREADS": str(threads),
        "DBT_PERF_RUN_ID": run_id,
        "XDG_CACHE_HOME": str(performance_cache(mode).resolve()),
    }
    if mode in ("foundry", "local"):
        env["DBT_ADBC_BIGQUERY_USE_FOUNDRY"] = "1"
    else:
        env.pop("DBT_ADBC_BIGQUERY_USE_FOUNDRY", None)
    return env


def resolved_driver(mode: str, log_path: Path, cache_home: Path) -> DriverArtifact:
    resolutions = set(re.findall(r"BigQuery driver resolved: (\S+) (\S+)", log_path.read_text()))
    if len(resolutions) != 1:
        raise RuntimeError(f"expected one {mode} driver resolution, found {resolutions}")
    name, version = resolutions.pop()
    expected_name = "bigquery" if mode == "upstream" else "bigquery_foundry"
    if name != expected_name or (mode == "local" and version != local_driver.version()):
        raise RuntimeError(f"unexpected {mode} driver resolution: {name} {version}")
    libraries = list(cache_home.rglob(f"libadbc_driver_{name}-{version}.so"))
    if len(libraries) != 1:
        raise RuntimeError(f"expected one cached {mode} driver, found {libraries}")
    path = libraries[0].resolve()
    return DriverArtifact(name, version, str(path), local_driver.sha256(path))


def assert_driver_unchanged(mode: str, log_path: Path, artifact: DriverArtifact) -> None:
    actual = resolved_driver(mode, log_path, performance_cache(mode))
    if actual != artifact:
        raise RuntimeError(f"dbt changed the {mode} driver during the benchmark")


def warm_driver(mode: str, env: dict[str, str]) -> DriverArtifact:
    log(f"\n==> Warm {mode} driver and verify the connection (not timed)")
    target_dir = GENERATED_DIR / "target" / f"warm-{mode}"
    log_dir = RESULTS_DIR / env["DBT_PERF_RUN_ID"] / f"warm-{mode}"
    log_dir.mkdir(parents=True, exist_ok=True)
    command(
        dbt_arguments("debug", "--connection", target_dir=target_dir, log_dir=log_dir),
        cwd=GENERATED_DIR,
        env=env,
    )
    artifact = resolved_driver(mode, log_dir / "dbt.log", performance_cache(mode))
    log(f"  -> {artifact.name} {artifact.version} ({artifact.sha256[:12]})")
    return artifact


def collect_job_metrics(
    client: bigquery.Client,
    *,
    project: str,
    location: str,
    dataset: str,
    run_id: str,
    expected_statements: dict[str, int],
    started: datetime,
    finished: datetime,
) -> JobMetrics:
    sql = f"""
        select start_time, end_time, total_slot_ms, statement_type, cache_hit
        from `region-{location}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        where project_id = @project
          and creation_time between @started and @finished
          and (
            destination_table.dataset_id = @dataset
            or strpos(coalesce(query, ''), @dataset) > 0
          )
          and job_type = 'QUERY'
          and state = 'DONE'
          and strpos(coalesce(query, ''), @query_comment) > 0
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project", "STRING", project),
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
            bigquery.ScalarQueryParameter("query_comment", "STRING", f"/* bigquery-adbc performance {run_id} */"),
            bigquery.ScalarQueryParameter("started", "TIMESTAMP", started - timedelta(seconds=2)),
            # Unpadded: the signature query runs right after dbt returns and must not be counted.
            bigquery.ScalarQueryParameter("finished", "TIMESTAMP", finished),
        ]
    )

    # A stable count can still be a partial view of this known workload.
    # Wait for every expected job and the timing data used in the report.
    rows: list[Any] = []
    statement_types: Counter[str] = Counter()
    incomplete = 0
    for attempt in range(_JOB_METRIC_POLLS):
        rows = list(client.query(sql, job_config=config, location=location).result())
        statement_types = Counter(row.statement_type or "UNKNOWN" for row in rows)
        incomplete = sum(row.start_time is None or row.end_time is None or row.total_slot_ms is None for row in rows)
        if statement_types == expected_statements and not incomplete:
            break
        if attempt < _JOB_METRIC_POLLS - 1:
            time.sleep(_JOB_METRIC_POLL_SECONDS)
    else:
        raise RuntimeError(
            f"job metadata for {dataset} is incomplete after {_JOB_METRIC_POLLS} reads "
            f"{_JOB_METRIC_POLL_SECONDS}s apart: expected {expected_statements}, "
            f"found {dict(statement_types)}, with {incomplete} jobs missing timing data"
        )

    return JobMetrics(
        count=len(rows),
        service_seconds=sum((row.end_time - row.start_time).total_seconds() for row in rows),
        warehouse_span_seconds=(
            max(row.end_time for row in rows) - min(row.start_time for row in rows)
        ).total_seconds(),
        total_slot_ms=sum(row.total_slot_ms for row in rows),
        statement_types=dict(sorted(statement_types.items())),
        cache_hits=sum(row.cache_hit is True for row in rows),
    )


def warehouse_signature(
    client: bigquery.Client,
    *,
    project: str,
    dataset: str,
    model_count: int,
    location: str,
) -> dict[str, int]:
    metric_parts = [
        f"select row_count, weighted_sum from `{project}.{dataset}.metric_{index:03d}`"
        for index in range(1, model_count + 1)
    ]
    sql = f"""
        with metrics as (
          {" union all ".join(metric_parts)}
        )
        select
          (select count(*) from `{project}.{dataset}.base_events`) as base_rows,
          (select sum(event_id) from `{project}.{dataset}.base_events`) as base_id_sum,
          (select count(*) from metrics) as metric_rows,
          (select sum(row_count) from metrics) as represented_rows,
          (select sum(weighted_sum) from metrics) as weighted_sum
    """
    row = next(iter(client.query(sql, location=location).result()))
    # Row.items() provides field names; iterating a BigQuery Row yields values alone.
    return {key: int(value) for key, value in row.items()}


def run_sample(
    *,
    client: bigquery.Client,
    mode: str,
    round_index: int,
    project: str,
    location: str,
    dataset: str,
    threads: int,
    model_count: int,
    row_count: int,
    run_id: str,
    keep_datasets: bool,
    artifact: DriverArtifact,
    existing: bool = False,
) -> Sample:
    sample_run_id = f"{run_id}-r{round_index + 1}-{mode}"
    env = mode_environment(
        mode,
        project=project,
        location=location,
        dataset=dataset,
        threads=threads,
        run_id=f"{sample_run_id}-build",
    )
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref)

    log_dir = RESULTS_DIR / run_id / f"round-{round_index}-{mode}"
    target_dir = log_dir / "target"
    log_dir.mkdir(parents=True, exist_ok=True)
    arguments = dbt_arguments("build", "--full-refresh", target_dir=target_dir, log_dir=log_dir)
    log(f"\n==> Round {round_index + 1}: {mode} driver ({dataset})")
    try:
        if existing:
            preparation_dir = log_dir / "prepare"
            preparation_dir.mkdir(parents=True, exist_ok=True)
            log("  -> create existing relations (not timed)")
            command(
                dbt_arguments(
                    "run",
                    "--full-refresh",
                    target_dir=preparation_dir / "target",
                    log_dir=preparation_dir,
                ),
                cwd=GENERATED_DIR,
                env=env | {"DBT_PERF_RUN_ID": f"{sample_run_id}-prepare"},
            )
            assert_driver_unchanged(mode, preparation_dir / "dbt.log", artifact)

        started = datetime.now(UTC)
        start_clock = time.perf_counter()
        command(arguments, cwd=GENERATED_DIR, env=env)
        wall_seconds = time.perf_counter() - start_clock
        finished = datetime.now(UTC)

        assert_driver_unchanged(mode, log_dir / "dbt.log", artifact)

        jobs = collect_job_metrics(
            client,
            project=project,
            location=location,
            dataset=dataset,
            run_id=env["DBT_PERF_RUN_ID"],
            expected_statements={
                "CREATE_TABLE_AS_SELECT": model_count + 1,
                "SELECT": 2 * (model_count + 1),
            },
            started=started,
            finished=finished,
        )
        if jobs.cache_hits:
            raise RuntimeError(f"benchmark jobs include {jobs.cache_hits} cache hits; expected none")
        signature = warehouse_signature(
            client,
            project=project,
            dataset=dataset,
            model_count=model_count,
            location=location,
        )
        base_sum = row_count * (row_count + 1) // 2
        expected_signature = {
            "base_rows": row_count,
            "base_id_sum": base_sum,
            "metric_rows": model_count * min(row_count, 97 * 31),
            "represented_rows": model_count * row_count,
            "weighted_sum": base_sum * model_count * (model_count + 1) // 2,
        }
        if signature != expected_signature:
            raise RuntimeError(f"unexpected warehouse result for {mode}: {signature!r} != {expected_signature!r}")
        sample = Sample(
            round=round_index + 1,
            mode=mode,
            dataset=dataset,
            wall_seconds=wall_seconds,
            jobs=jobs,
            signature=signature,
        )
        log(
            f"  -> {wall_seconds:.2f}s wall, {jobs.count} jobs, "
            f"{jobs.warehouse_span_seconds:.2f}s warehouse span, "
            f"{sample.jobs_per_second:.2f} jobs/s"
        )
    except BaseException:
        # Also on Ctrl-C, which leaves the dataset behind just the same.
        log(f"  !! retained failed dataset {project}.{dataset}")
        raise
    else:
        if keep_datasets:
            log(f"  -> retained {project}.{dataset} (--keep-datasets)")
        else:
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        return sample


def summarize(samples: list[Sample]) -> None:
    log("\n==> Effective dbt performance")
    log("driver    median wall   range             jobs/s   warehouse span   client envelope")
    by_mode = {mode: [sample for sample in samples if sample.mode == mode] for mode in ("foundry", "upstream", "local")}
    for mode, mode_samples in by_mode.items():
        if not mode_samples:
            continue
        walls = [sample.wall_seconds for sample in mode_samples]
        rates = [sample.jobs_per_second for sample in mode_samples]
        spans = [sample.jobs.warehouse_span_seconds for sample in mode_samples]
        envelopes = [sample.client_envelope_seconds for sample in mode_samples]
        log(
            f"{mode:<10}{statistics.median(walls):>10.2f}s   "
            f"{min(walls):>6.2f}-{max(walls):<6.2f}s   "
            f"{statistics.median(rates):>6.2f}   "
            f"{statistics.median(spans):>10.2f}s   "
            f"{statistics.median(envelopes):>10.2f}s"
        )

    local = {sample.round: sample for sample in by_mode["local"]}
    for reference in ("foundry", "upstream"):
        reference_samples = {sample.round: sample for sample in by_mode[reference]}
        paired = [
            local[index].wall_seconds / reference_samples[index].wall_seconds
            for index in reference_samples.keys() & local.keys()
        ]
        if paired:
            ratio = statistics.median(paired)
            direction = "lower" if ratio < 1 else "higher"
            percent = abs(1 - ratio) * 100
            log(f"\n  local/{reference} paired median: {ratio:.3f} ({percent:.1f}% {direction} wall time)")
    log("  Lower wall time and higher jobs/s are better; no performance threshold is enforced.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--models", type=int, default=12)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument(
        "--mode",
        choices=("both", "foundry", "upstream", "local"),
        default="both",
        help="both compares the downloaded opt-in Foundry driver with the local driver",
    )
    parser.add_argument("--keep-datasets", action="store_true")
    parser.add_argument(
        "--existing",
        action="store_true",
        help="create the relations with dbt run before each timed build",
    )
    parser.add_argument(
        "--local-library",
        type=Path,
        help="stage an existing release shared library instead of building the local driver",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project = require_project()
    if args.rounds < 1 or args.models < 1 or args.rows < 1 or args.threads < 1:
        raise RuntimeError("rounds, models, rows, and threads must all be positive")
    if shutil.which("dbt") is None:
        raise RuntimeError("dbt is not on PATH")
    if args.local_library is not None:
        args.local_library = args.local_library.resolve(strict=True)
        if args.mode not in ("both", "local"):
            raise RuntimeError("--local-library requires --mode both or --mode local")
    if shutil.which("cargo") is None and args.mode in ("both", "local") and args.local_library is None:
        raise RuntimeError("cargo is not on PATH")
    dbt_version = subprocess.run(["dbt", "--version"], check=True, text=True, capture_output=True).stdout.strip()
    if not dbt_version.startswith("dbt 2."):
        raise RuntimeError(f"dbt 2.x is required; found {dbt_version}")

    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments], cwd=local_driver.REPO_DIR, check=True, text=True, capture_output=True
        ).stdout

    environment = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "dbt_path": shutil.which("dbt"),
        "git_revision": git("rev-parse", "HEAD").strip(),
        "git_status": git("status", "--short").splitlines(),
    }

    location = os.environ.get("DBT_BIGQUERY_LOCATION", "US")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    if not location.replace("-", "").isalnum():
        raise RuntimeError(f"unsafe BigQuery location: {location!r}")
    timestamp = run_id.lower().replace("t", "_").replace("z", "")
    dataset_prefix = f"bigquery_adbc_perf_{timestamp}_{os.getpid()}"
    modes = ["foundry", "local"] if args.mode == "both" else [args.mode]

    log("==> dbt BigQuery driver benchmark")
    log(f"  -> {dbt_version}")
    log(
        f"  -> {args.models + 1} models, {2 * (args.models + 1)} data tests, "
        f"{args.rows:,} generated base rows, {args.threads} threads, {args.rounds} rounds"
    )
    log(f"  -> {'existing relations' if args.existing else 'empty dataset'} before each timed build")
    write_workload(args.models, args.rows)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    local_artifact: tuple[Path, str] | None = None
    if "local" in modes:
        local_artifact = stage_local_driver(performance_cache("local"), args.local_library)

    artifacts = {
        mode: warm_driver(
            mode,
            mode_environment(
                mode,
                project=project,
                location=location,
                dataset=f"{dataset_prefix}_warm_{mode}",
                threads=args.threads,
                run_id=run_id,
            ),
        )
        for mode in modes
    }
    if local_artifact is not None and artifacts["local"].sha256 != local_artifact[1]:
        raise RuntimeError("dbt replaced the staged local driver during warmup")
    if "foundry" in artifacts and "local" in artifacts and artifacts["foundry"].sha256 == artifacts["local"].sha256:
        raise RuntimeError("Foundry and local caches contain the same driver binary")

    client = bigquery.Client(project=project, location=location)
    samples: list[Sample] = []
    for round_index in range(args.rounds):
        order = list(modes)
        if len(order) == 2 and round_index % 2 == 1:
            order.reverse()
        for mode in order:
            sample = run_sample(
                client=client,
                mode=mode,
                round_index=round_index,
                project=project,
                location=location,
                dataset=f"{dataset_prefix}_r{round_index + 1}_{mode}",
                threads=args.threads,
                model_count=args.models,
                row_count=args.rows,
                run_id=run_id,
                keep_datasets=args.keep_datasets,
                artifact=artifacts[mode],
                existing=args.existing,
            )
            samples.append(sample)

    if local_artifact is not None:
        path, digest = local_artifact
        if local_driver.sha256(path) != digest:
            raise RuntimeError("dbt replaced the staged local driver during the benchmark")

    output = RESULTS_DIR / run_id / "results.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "dbt_version": dbt_version,
                "drivers": {mode: asdict(artifact) for mode, artifact in artifacts.items()},
                "environment": environment,
                "configuration": {
                    "rounds": args.rounds,
                    "models": args.models,
                    "rows": args.rows,
                    "threads": args.threads,
                    "project": project,
                    "location": location,
                    "modes": modes,
                    "local_library": str(args.local_library) if args.local_library else None,
                    "existing": args.existing,
                },
                "samples": [
                    {
                        **asdict(sample),
                        "jobs_per_second": sample.jobs_per_second,
                        "client_envelope_seconds": sample.client_envelope_seconds,
                    }
                    for sample in samples
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    summarize(samples)
    log(f"\n  -> raw results: {output}")
    log(f"  -> warehouse signature: {samples[0].signature}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError, GoogleAPICallError) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
