"""Run the dbt 2.0 BigQuery integration suite."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import google.auth
import local_driver
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

from config import require_project

PROJECT_DIR = Path(__file__).resolve().parent
REPO_DIR = PROJECT_DIR.parent.parent
DBT_BUILD_SCRIPT = REPO_DIR / "tools" / "dbt-build" / "build.sh"
# tools/dbt-build does not name its binary dbt, so it can sit beside dbt Labs' dbt on PATH.
DBT_EXECUTABLE = "dbt-bigquery-adbc"
# The driver version in `dbt --version`, which tools/dbt-build/build.sh checks in full.
DBT_IDENTIFIER = re.compile(r"dbt \(modified with bigquery-adbc (?P<driver>\S+)\) \S+ \S+")
LIBRARY_NAME = "libadbc_driver_bigquery.so"
# Selects dbt's own BigQuery driver in a tools/dbt-build binary, resolved as stock dbt resolves it.
USE_DBT_DRIVER = "BIGQUERY_ADBC_USE_DBT_DRIVER"
# What dbt logs on resolving its canonical or Foundry driver; never bigquery_adbc or `installed`.
DBT_DRIVER_RESOLUTION = re.compile(r"BigQuery driver resolved: bigquery(?:_foundry)? \S+$", re.MULTILINE)
# The authorized view reads from a second dataset, removed by drop_integration_schema.
AUTHORIZED_SOURCE_SUFFIX = "_authorized_source"
# Only adbc.bigquery.table.update_description writes this text; no DDL in the project sets it.
TABLE_DESCRIPTION_SENTINEL = "described through the option-only BigQuery table description update"
# persist_docs descriptions from seeds/schema.yml and models/schema.yml. The seed materialization
# and later incremental waves set them through the driver's description update, not DDL.
SEED_DESCRIPTION = "Seeded customer dimension."
INCREMENTAL_DESCRIPTION = "Incremental order totals."
INCREMENTAL_COLUMN_DESCRIPTION = "Surrogate order key, documented through the column metadata update."
# From models/exotic/schema.yml, patched onto a view's schema after each CREATE OR REPLACE VIEW.
AUTHORIZED_VIEW_COLUMN_DESCRIPTIONS = {
    "order_date": "Order day, documented through the view's column metadata update.",
    "order_count": "Orders per day, documented through the view's column metadata update.",
}
# Descriptions from models/known_issues/schema.yml. dbt 2.0 persists top-level
# descriptions but loses nested paths (UPSTREAM.md UP-77); assertions detect a change to either.
NESTED_TOP_LEVEL_DESCRIPTIONS = {
    "id": "Required identifier.",
    "array_of_structs": "Struct fields declared out of alphabetical order.",
    "struct_column": "Nested struct with separately documented fields.",
}
NESTED_FIELD_PATHS = (
    "array_of_structs.d",
    "array_of_structs.e",
    "array_of_structs.b_id",
    "array_of_structs.a_color",
    "struct_column.id",
    "struct_column.taste",
)
NODE_PREFIX = "model.bigquery_adbc_integration."
REFRESHED_VIEW = "exotic_refreshed_materialized_view"
# The driver writes diagnostics to standard error when this is set (REFERENCE.md, "Diagnostics").
# A warning marks a degraded path that still succeeds, such as a Storage Read fallback to REST.
DRIVER_LOG_LEVEL = "ADBC_DRIVER_BIGQUERY_LOG_LEVEL"
# The debug events naming the read path a result took, which the adapter-surface phase must show
# both of. The macro explicitly disables Storage Read for its REST pagination statement, then
# uses default routing for a separate result that also requires pagination.
READ_PATH_EVENTS = (
    re.compile(r"\[bigquery-adbc\] DEBUG fetching further REST pages of BigQuery job "),
    re.compile(r"\[bigquery-adbc\] DEBUG reading BigQuery job \S+ through Storage Read"),
)
PYTHON_MODEL_ALIAS = "exotic_python_model"
# Identifies models/exotic/exotic_python_model.py in the uploaded source.
PYTHON_MODEL_MARKER = "uploaded through adbc.bigquery.write_gcs"
# The driver's context for a rejected Dataproc submission (src/dbt/dataproc.rs). Failing with
# it proves the driver accepted every python-model option.
DATAPROC_SUBMISSION_ERROR = "failed to submit the BigQuery Dataproc job"
# Substrings that would mean the driver rejected one of dbt 2.0's python-model options instead
# of reaching Dataproc, from option_rejection in src/error/options.rs.
OPTION_REJECTION_MARKERS = ("unsupported BigQuery", "unknown BigQuery", "NOT_IMPLEMENTED", "write_gcs")
# Compare values rendered by dbt through both drivers, including empty results with a schema.
# The ordinary data tests separately check the values stored in BigQuery.
SHOW_MODELS = ("exotic_bigquery_types", "exotic_result_edges", "exotic_empty_result", "exotic_quoted_identifiers")
# dbt_project.yml's query-comment before its node.unique_id. With job-label: true, dbt 2.0
# sends the rendered comment as the query_comment job label, beside dbt_invocation_id.
QUERY_COMMENT_PREFIX = "bigquery-adbc integration: "
# profiles.yml's job_execution_timeout_seconds, and the model-level override on
# exotic_copied_partitions, as the jobTimeoutMs dbt 2.0 sends through the driver.
PROFILE_JOB_TIMEOUT_MS = 300_000
COPIED_PARTITIONS_JOB_TIMEOUT_MS = 900_000
INCREMENTAL_UNIQUE_ID = f"{NODE_PREFIX}incremental_order_totals"
# dbt reports this inlined model as skipped; its unit test must still pass.
EPHEMERAL_UNIQUE_ID = f"{NODE_PREFIX}int_order_items"
# The partitions exotic_copied_partitions lists, as copy-job partition decorators.
COPIED_PARTITIONS = {"20260110", "20260111"}
# dbt's bundled driver drops decimal parameters from CSV seed schemas; this driver loads each
# column as declared (DBT.md), so the one parameterized declaration is the only permitted
# schema difference. A plain `numeric` stays unparameterized under both drivers.
EXPECTED_DECIMAL_PARAMETER_DIFFERENCES = {
    ("main", "exotic_wide_types", "precise_amount"): ((None, None), (20, 4)),
}
MODES = ("upstream", "local")


def _style(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if sys.stdout.isatty() else text


def section(message: str) -> None:
    print(f"\n{_style('1;34', f'==> {message}')}", flush=True)


def note(message: str) -> None:
    print(f"{_style('32', '  ->')} {message}", flush=True)


def show_command(command: Sequence[str | Path]) -> None:
    print(f"{_style('33', '  $')} {shlex.join(map(str, command))}", flush=True)


def expect(what: str, got: object, want: object) -> None:
    if got != want:
        raise RuntimeError(f"{what} is {got!r}, expected {want!r}")


def make_dbt_command(*arguments: str, target_dir: Path, log_dir: Path) -> list[str]:
    return [
        DBT_EXECUTABLE,
        *arguments,
        *("--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)),
        *("--target-path", str(target_dir), "--log-path", str(log_dir), "--no-version-check"),
    ]


def failing_dbt_command(*arguments: str, target_dir: Path, log_dir: Path) -> None:
    """Run dbt and require it to fail; callers check the error in run_results.json."""
    command = make_dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    (target_dir / "run_results.json").unlink(missing_ok=True)
    show_command(command)
    if subprocess.run(command, cwd=PROJECT_DIR, check=False).returncode == 0:
        raise RuntimeError(f"expected {shlex.join(command)} to fail, but it succeeded")


def driver_warnings(stderr: str) -> list[str]:
    return [line for line in stderr.splitlines() if re.search(r"\[bigquery-adbc\] (?:WARN|ERROR) ", line)]


def dbt_command(
    *arguments: str,
    target_dir: Path,
    log_dir: Path,
    required_events: tuple[re.Pattern[str], ...] | None = None,
    completion: str | None = None,
) -> None:
    """Run dbt, optionally requiring clean driver diagnostics and a macro completion log."""
    log_path = log_dir / "dbt.log"
    log_offset = log_path.stat().st_size if completion and log_path.exists() else 0
    command = make_dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    show_command(command)
    diagnose = required_events is not None
    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        env=os.environ | {DRIVER_LOG_LEVEL: "debug"} if diagnose else None,
        stderr=subprocess.PIPE if diagnose else None,
        text=True,
        check=False,
    )
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    result.check_returncode()
    if required_events is not None:
        if warnings := driver_warnings(result.stderr):
            raise RuntimeError(f"the driver reported a degraded path during dbt {arguments[0]}: {warnings!r}")
        if missing := [event.pattern for event in required_events if not event.search(result.stderr)]:
            raise RuntimeError(
                f"the driver did not report {missing!r} during dbt {arguments[0]}, so the phase "
                "no longer exercises that read path"
            )
    if completion is not None:
        # dbt appends to this file across invocations. Earlier successes and debug dumps of
        # macro source cannot prove that the current invocation executed its assertions.
        try:
            with log_path.open("rb") as log:
                log.seek(log_offset)
                current_log = log.read().decode("utf-8", errors="replace")
        except FileNotFoundError:
            current_log = ""
        if not re.search(rf"(?m)^[^\n]*\[info \]: {re.escape(completion)}", current_log):
            raise RuntimeError(
                f"dbt {shlex.join(arguments)} exited successfully without logging {completion!r}; "
                "the macro's assertions may not have executed"
            )


def captured_dbt_command(*arguments: str, target_dir: Path, log_dir: Path, env: dict[str, str]) -> str:
    command = make_dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    show_command(command)
    result = subprocess.run(command, cwd=PROJECT_DIR, env=env, text=True, capture_output=True, check=False)
    if result.returncode != 0 and result.stdout:
        print(result.stdout, end="", file=sys.stderr)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    result.check_returncode()
    return result.stdout


def _dbt_rows_number(rows: int) -> str:
    """Render a row count the same way dbt 2.0's BigQuery response does."""
    value = float(rows)
    for suffix in ("", "k", "m", "b", "t"):
        if abs(value) < 1_000:
            return f"{value:.1f}{suffix}"
        value /= 1_000
    return f"{value:.1f}q"


def _check_job_ids(response: dict, what: str) -> None:
    query_id = response.get("query_id")
    if not isinstance(query_id, str) or not query_id or response.get("job_id") != query_id:
        raise RuntimeError(
            f"{what} query_id/job_id are not equal non-empty strings: {query_id!r} / {response.get('job_id')!r}"
        )


def assert_adapter_response_contract(
    response: object,
    *,
    expected_code: str,
    expected_rows_affected: int | None,
    expected_project: str,
    expected_location: str,
) -> None:
    """Assert the user-visible BigQuery fields dbt writes to run_results.json.

    `expected_rows_affected` is None for a statement dbt reports no count for, such as a script.
    """
    if not isinstance(response, dict):
        raise RuntimeError(f"adapter_response is not an object: {response!r}")
    expect("adapter_response code", response.get("code"), expected_code)
    rows_affected = response.get("rows_affected")
    if rows_affected != expected_rows_affected or isinstance(rows_affected, bool):
        raise RuntimeError(f"adapter_response rows_affected is {rows_affected!r}, expected {expected_rows_affected}")

    message = response.get("message")
    if response.get("_message") != message or not isinstance(message, str):
        raise RuntimeError(
            f"adapter_response message/_message are missing or disagree: {message!r} / {response.get('_message')!r}"
        )
    processed = r"(?:0|[0-9]+(?:\.[0-9]+)? (?:Bytes|KiB|MiB|GiB|TiB|PiB))"
    rows = f"{re.escape(_dbt_rows_number(expected_rows_affected))} rows, " if expected_rows_affected else ""
    expected_message = rf"{re.escape(expected_code)} \({rows}{processed} processed\)"
    if re.fullmatch(expected_message, message) is None:
        raise RuntimeError(f"adapter_response message is {message!r}, expected {expected_message!r}")

    for key in ("bytes_processed", "bytes_billed", "slot_ms"):
        value = response.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(f"adapter_response {key} is {value!r}, expected a non-negative integer")
    _check_job_ids(response, "adapter_response")
    expect("adapter_response project_id", response.get("project_id"), expected_project)
    expect("adapter_response location", response.get("location"), expected_location)


def assert_run_result_adapter_response(
    run_results_path: Path, *, unique_id: str, code: str, rows: int | None, project: str, location: str
) -> None:
    """Validate one node's adapter response before dbt overwrites the artifact."""
    response = _successful_adapter_response(run_results_path, unique_id=unique_id)
    try:
        assert_adapter_response_contract(
            response,
            expected_code=code,
            expected_rows_affected=rows,
            expected_project=project,
            expected_location=location,
        )
    except RuntimeError as error:
        raise RuntimeError(f"{unique_id}: {error}; observed {json.dumps(response, sort_keys=True)}") from error


def read_artifact(path: Path) -> dict:
    try:
        artifact = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"failed to read {path}: {error}") from error
    if not isinstance(artifact, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return artifact


def _node_result(run_results_path: Path, *, unique_id: str) -> dict:
    results = read_artifact(run_results_path).get("results", ())
    matches = [result for result in results if result.get("unique_id") == unique_id]
    if len(matches) != 1:
        raise RuntimeError(f"{run_results_path} contains {len(matches)} results for {unique_id!r}, expected one")
    return matches[0]


def _successful_adapter_response(run_results_path: Path, *, unique_id: str) -> object:
    result = _node_result(run_results_path, unique_id=unique_id)
    expect(f"{unique_id} status", result.get("status"), "success")
    return result.get("adapter_response")


# dbt 2.0's sources.json uses title case, unlike run_results.json.
SUCCESSFUL_STATUS = {
    "model": "success",
    "seed": "success",
    "snapshot": "success",
    "test": "pass",
    "unit_test": "pass",
    "source": "Pass",
}


def assert_nodes_ran(run_results_path: Path, *, after: str, expected_nodes: list[str]) -> int:
    """Require the complete phase selection and successful execution of every node."""
    results = read_artifact(run_results_path).get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError(f"{after!r} selected no dbt nodes, so the phase asserted nothing")
    if not expected_nodes or len(set(expected_nodes)) != len(expected_nodes):
        raise RuntimeError(f"{after!r} has an empty or duplicated coverage baseline")
    observed = set()
    for result in results:
        if not isinstance(result, dict):
            raise RuntimeError(f"{after!r} has a malformed node result: {result!r}")
        unique_id = result.get("unique_id")
        if not isinstance(unique_id, str) or not unique_id:
            raise RuntimeError(f"{after!r} has a result without a unique_id")
        if unique_id in observed:
            raise RuntimeError(f"{after!r} has duplicate results for {unique_id}")
        observed.add(unique_id)
        expected_status = SUCCESSFUL_STATUS.get(unique_id.split(".", 1)[0])
        if unique_id == EPHEMERAL_UNIQUE_ID:
            expected_status = "skipped"
        if expected_status is None or result.get("status") != expected_status:
            raise RuntimeError(
                f"{after!r}: {unique_id} status is {result.get('status')!r}, expected {expected_status!r}"
            )
    expected = set(expected_nodes)
    if observed != expected:
        raise RuntimeError(
            f"{after!r} coverage changed; missing: {sorted(expected - observed)!r}; "
            f"unexpected: {sorted(observed - expected)!r}"
        )
    return len(observed)


def checked_phase_command(
    *arguments: str, target_dir: Path, log_dir: Path, coverage: str, expected_nodes: list[str]
) -> int:
    """Discard stale results and preserve this phase's artifact, even on failure."""
    filename = "sources.json" if arguments[:2] == ("source", "freshness") else "run_results.json"
    artifact = target_dir / filename
    saved = target_dir / "phases" / coverage / filename
    artifact.unlink(missing_ok=True)
    saved.unlink(missing_ok=True)
    try:
        dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    finally:
        if artifact.exists():
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(artifact, saved)
    return assert_nodes_ran(artifact, after=coverage, expected_nodes=expected_nodes)


def assert_canonical_incremental_adapter_response(
    response: object, *, expected_project: str, expected_location: str
) -> None:
    """Pin dbt's canonical driver's known incomplete response for the MERGE wave."""
    if not isinstance(response, dict):
        raise RuntimeError(f"canonical adapter_response is not an object: {response!r}")
    expected = {
        "_message": "SUCCESS (0.0 rows)",
        "code": "SUCCESS",
        "location": expected_location,
        "message": "SUCCESS (0.0 rows)",
        "project_id": expected_project,
    }
    expected_keys = {*expected, "job_id", "query_id"}
    if response.keys() != expected_keys:
        raise RuntimeError(
            f"canonical adapter_response fields changed: {sorted(response)}, expected {sorted(expected_keys)}"
        )
    for key, want in expected.items():
        expect(f"canonical adapter_response {key}", response[key], want)
    _check_job_ids(response, "canonical adapter_response")


def expected_job_labels(*, invocation_id: str, unique_id: str) -> dict[str, str]:
    """The job labels dbt 2.0 sends with every statement it runs for one node.

    dbt sanitizes a label value: lowercase, `[^a-z0-9_-]` to `_`, at most 63 characters.
    """
    comment = re.sub(r"[^a-z0-9_-]", "_", f"{QUERY_COMMENT_PREFIX}{unique_id}".lower())[:63]
    return {"dbt_invocation_id": invocation_id, "query_comment": comment}


def assert_job_configuration(resource: dict, *, what: str, labels: dict[str, str], timeout_ms: int) -> None:
    """Assert the labels and timeout of one BigQuery job resource.

    The labels must match exactly, because the driver adds none of its own: an extra label came
    from another statement's options.
    """
    configuration = resource.get("configuration") or {}
    expect(f"{what}'s job labels", configuration.get("labels") or {}, labels)
    # The REST API encodes the int64 jobTimeoutMs as a string.
    expect(f"{what}'s jobTimeoutMs", str(configuration.get("jobTimeoutMs")), str(timeout_ms))


def assert_partition_copy_jobs(
    resources: list[dict], *, project: str, dataset: str, table: str, labels: dict[str, str], timeout_ms: int
) -> None:
    """Assert that copy_partitions copied exactly the listed partitions with dbt's job options.

    dbt 2.0 discards a copy job's ID (UPSTREAM.md UP-86), so the jobs are found by their
    destination. Each copies one partition of dbt's temporary table onto the same partition
    with WRITE_TRUNCATE.
    """
    copied = set()
    for resource in resources:
        configuration = resource.get("configuration") or {}
        copy = configuration.get("copy") or {}
        destination = copy.get("destinationTable") or {}
        if configuration.get("jobType") != "COPY" or (
            destination.get("projectId"),
            destination.get("datasetId"),
        ) != (project, dataset):
            continue
        base, _, partition = str(destination.get("tableId", "")).partition("$")
        if base != table:
            continue
        what = f"copy job {(resource.get('jobReference') or {}).get('jobId')} onto {table}${partition}"
        source = copy.get("sourceTable") or {}
        if (
            not partition
            or (source.get("projectId"), source.get("datasetId")) != (project, dataset)
            or not str(source.get("tableId", "")).endswith(f"${partition}")
        ):
            raise RuntimeError(f"{what} copies {source!r}, not the same partition of a table in {dataset}")
        expect(f"{what}'s write disposition", copy.get("writeDisposition"), "WRITE_TRUNCATE")
        assert_job_configuration(resource, what=what, labels=labels, timeout_ms=timeout_ms)
        copied.add(partition)
    expect(f"the {table} partitions copy_partitions copied", copied, COPIED_PARTITIONS)


def assert_incremental_job_options(
    client: bigquery.Client, run_results_path: Path, *, project: str, dataset: str, location: str, started: datetime
) -> None:
    """Check the job labels and timeouts dbt 2.0 sent with the incremental phase's jobs.

    The MERGE takes the profile's timeout; exotic_copied_partitions' copy jobs take the model's
    override. dbt's canonical driver sets neither on copy jobs, so only this driver is checked.
    """
    # dbt 2.0 also sends the phase's invocation ID as the dbt_invocation_id label.
    invocation_id = (read_artifact(run_results_path).get("metadata") or {}).get("invocation_id")
    if not isinstance(invocation_id, str) or not invocation_id:
        raise RuntimeError(f"{run_results_path} records no invocation_id: {invocation_id!r}")
    response = _successful_adapter_response(run_results_path, unique_id=INCREMENTAL_UNIQUE_ID)
    job_id = response.get("job_id") if isinstance(response, dict) else None
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(f"{INCREMENTAL_UNIQUE_ID} reported no job_id to look up: {response!r}")
    assert_job_configuration(
        client.get_job(job_id, project=project, location=location).to_api_repr(),
        what=f"the incremental MERGE job {job_id}",
        labels=expected_job_labels(invocation_id=invocation_id, unique_id=INCREMENTAL_UNIQUE_ID),
        timeout_ms=PROFILE_JOB_TIMEOUT_MS,
    )
    # The margin absorbs clock skew between this host and BigQuery.
    jobs = client.list_jobs(project=project, min_creation_time=started - timedelta(minutes=1))
    assert_partition_copy_jobs(
        [job.to_api_repr() for job in jobs],
        project=project,
        dataset=dataset,
        table="exotic_copied_partitions",
        labels=expected_job_labels(invocation_id=invocation_id, unique_id=f"{NODE_PREFIX}exotic_copied_partitions"),
        timeout_ms=COPIED_PARTITIONS_JOB_TIMEOUT_MS,
    )


def build_dbt(output_dir: Path) -> Path:
    section("Build dbt with this repository's driver")
    command = [str(DBT_BUILD_SCRIPT), "--output-dir", str(output_dir)]
    show_command(command)
    stdout = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE).stdout
    return Path(stdout.splitlines()[-1])


def install_dbt(archive: Path, directory: Path) -> Path:
    """Unpack a tools/dbt-build archive, so the suite runs what the archive ships."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    with tarfile.open(archive) as source:
        source.extractall(directory, filter="data")
    (install,) = directory.iterdir()
    # 0.4.0's archives named the binary dbt.
    if not (install / DBT_EXECUTABLE).exists() and (install / "dbt").is_file():
        (install / "dbt").rename(install / DBT_EXECUTABLE)
    return install


@dataclass(frozen=True)
class BuiltDriver:
    """Where a dbt build gets this repository's driver, and what proves a run used it."""

    library: Path
    resolution: str
    downloaded: bool


def built_driver(install: Path, cache_home: Path, driver_version: str) -> BuiltDriver:
    """Non-release builds load the library in the archive; release builds download theirs."""
    library = (install / "lib" / "bigquery-adbc" / LIBRARY_NAME).resolve()
    if library.is_file():
        return BuiltDriver(library, f"BigQuery driver resolved: installed {library}", downloaded=False)
    return BuiltDriver(
        local_driver.release_cache_path(cache_home, driver_version),
        f"BigQuery driver resolved: bigquery_adbc {driver_version}",
        downloaded=True,
    )


def assert_local_driver_resolution(log_dir: Path, driver: BuiltDriver) -> None:
    if driver.resolution not in (log_dir / "dbt.log").read_text():
        raise RuntimeError(f"dbt did not report {driver.resolution!r}")
    if not driver.library.is_file():
        raise RuntimeError(f"dbt did not leave its driver at {driver.library}")
    note(f"dbt {'downloaded' if driver.downloaded else 'loaded'} the driver at {driver.library}")


def assert_dbt_driver_resolution(log_dir: Path) -> None:
    if DBT_DRIVER_RESOLUTION.search((log_dir / "dbt.log").read_text()) is None:
        raise RuntimeError(f"dbt did not report resolving its own BigQuery driver ({USE_DBT_DRIVER})")
    note("dbt resolved its own BigQuery driver")


def run_operation_cases(project: str, dataset: str) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    """(name, SQL, extra dbt arguments) for each `dbt run-operation --sql` parity case."""
    table = f"`{project}.{dataset}.run_operation_events`"
    return (
        (
            "table DDL",
            f"""
create or replace table {table} (
  operation_id int64 not null,
  operation_kind string not null,
  amount numeric,
  context_value string,
  attributes array<struct<source string, score int64>>,
  created_at timestamp not null
)
partition by date(created_at)
cluster by operation_kind
options (
  description = 'Created by dbt run-operation --sql parity coverage',
  labels = [('component', 'adbc_integration'), ('entrypoint', 'run_operation')]
)
""",
            (),
        ),
        (
            "INSERT DML",
            f"""
insert into {table}
  (operation_id, operation_kind, amount, context_value, attributes, created_at)
values
  (1, 'insert', numeric '12.50', 'literal',
   [struct('literal' as source, 1 as score)], timestamp '2025-01-01 00:00:00+00'),
  (2, 'insert', numeric '20.00', 'literal',
   [struct('first' as source, 2 as score), struct('second' as source, 3 as score)],
   timestamp '2025-01-02 00:00:00+00')
""",
            (),
        ),
        (
            "Jinja context and project macro",
            """
insert into `{{ target.database }}.{{ target.schema }}.run_operation_events`
  (operation_id, operation_kind, amount, context_value, attributes, created_at)
select
  {{ var('run_operation_id') }},
  'jinja_' || '{{ target.name }}',
  cast({{ discounted_amount(5, 7.50, 2.50) }} as numeric),
  '{{ var("run_operation_marker") }}',
  [struct('macro' as source, {{ var('run_operation_id') }} as score)],
  timestamp '2025-01-03 00:00:00+00'
""",
            ("--vars", "{run_operation_id: 3, run_operation_marker: context}"),
        ),
        (
            "comment-prefixed UPDATE DML",
            f"""
/* Leading comments must not obscure the statement type from an ADBC driver. */
update {table}
set
  amount = numeric '13.75',
  context_value = 'commented_update',
  attributes = array_concat(
    attributes,
    [struct('update' as source, 10 as score)]
  )
where operation_id = 1
""",
            (),
        ),
        (
            "MERGE DML",
            f"""
merge {table} as target
using (
  select 2 as operation_id, 'merge_update' as operation_kind,
         numeric '21.25' as amount, 'matched' as context_value,
         [struct('merge' as source, 20 as score)] as attributes,
         timestamp '2025-01-02 00:00:00+00' as created_at
  union all
  select 4, 'merge_insert', numeric '40.00', 'not_matched',
         [struct('merge' as source, 40 as score)],
         timestamp '2025-01-04 00:00:00+00'
) as source
on target.operation_id = source.operation_id
when matched then update set
  operation_kind = source.operation_kind,
  amount = source.amount,
  context_value = source.context_value,
  attributes = source.attributes
when not matched then insert
  (operation_id, operation_kind, amount, context_value, attributes, created_at)
values
  (source.operation_id, source.operation_kind, source.amount, source.context_value,
   source.attributes, source.created_at)
""",
            (),
        ),
        (
            "nested window SELECT",
            f"""
select
  operation_id,
  operation_kind,
  amount,
  sum(amount) over () as total_amount,
  array(
    select as struct attribute.source, attribute.score
    from unnest(attributes) as attribute
    order by attribute.score
  ) as sorted_attributes
from {table}
qualify row_number() over (order by operation_id desc) <= 3
order by operation_id
""",
            (),
        ),
    )


def normalized_run_operation_output(stdout: str, *, datasets: tuple[str, str]) -> tuple[tuple[object, ...], ...]:
    """Keep deterministic, semantic dbt JSON events and discard timings/IDs."""

    def normalize_text(value: str) -> str:
        for dataset in datasets:
            value = value.replace(dataset, "<dataset>")
        return value.strip()

    normalized: list[tuple[object, ...]] = []
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            if line.strip():
                normalized.append(("text", normalize_text(line)))
            continue
        data = event.get("data", {})
        info = event.get("info", {})
        name = info.get("name")
        if any(data.get(key) for key in ("rows", "records", "results")):
            raise RuntimeError(f"run-operation unexpectedly emitted database rows in {name}: {data!r}")
        if name == "MainReportVersion":
            normalized.append(("version", data.get("version")))
        elif name == "MainReportArgs":
            args = data.get("args", {})
            normalized.append(("arguments", *(args.get(key) for key in ("debug", "log_format", "quiet", "write_json"))))
        elif name == "ArtifactWritten":
            normalized.append(("artifact", data.get("artifact_type"), Path(data.get("artifact_path", "")).name))
        elif name == "SQLQuery":
            message = info.get("msg", "")
            fields = {
                key: re.search(rf"^-- {key}: (.+)$", message, re.MULTILINE) for key in ("outcome", "dialect", "desc")
            }
            # Every SQLQuery comment header carries all three. A missing field would read None
            # under both drivers and the comparison would pass vacuously.
            if absent := sorted(key for key, match in fields.items() if match is None):
                raise RuntimeError(
                    "dbt's SQLQuery event no longer carries the comment fields the "
                    f"run-operation parity comparison reads ({', '.join(absent)}): {message!r}"
                )
            values = (match.group(1) for match in fields.values() if match is not None)
            normalized.append(("sql", normalize_text(data.get("sql", "")), *values))
        elif name == "CommandCompleted":
            normalized.append(("completed", data.get("success")))
        elif name == "Generic":
            message = data.get("msg", info.get("msg", ""))
            if re.match(r"dbt \d", message):
                normalized.append(("dbt", message))
            elif message == "Loading profiles.yml":
                normalized.append(("profiles", "loaded"))
            elif message.startswith("BigQuery driver resolved:"):
                normalized.append(("driver", "resolved"))

    sql_events = [event for event in normalized if event[0] == "sql"]
    completions = [event for event in normalized if event[0] == "completed"]
    if len(sql_events) != 1 or completions != [("completed", True)]:
        raise RuntimeError(f"run-operation output did not contain one successful SQL event: {normalized!r}")
    return tuple(normalized)


def run_operation_state(client: bigquery.Client, project: str, dataset: str) -> dict:
    table = client.get_table(f"{project}.{dataset}.run_operation_events")

    def rows(sql: str) -> tuple[tuple, ...]:
        return tuple(tuple(row) for row in client.query(sql).result())

    partition = table.time_partitioning
    return {
        "schema": tuple(
            (field.name, field.field_type, field.mode, tuple((f.name, f.field_type, f.mode) for f in field.fields))
            for field in table.schema
        ),
        "partition": (partition.type_, partition.field) if partition is not None else (None, None),
        "clustering": tuple(table.clustering_fields or ()),
        "labels": tuple(sorted((table.labels or {}).items())),
        "description": table.description,
        "rows": rows(f"""
select
  operation_id,
  operation_kind,
  cast(amount as string) as amount,
  context_value,
  to_json_string(attributes) as attributes,
  format_timestamp('%FT%TZ', created_at, 'UTC') as created_at
from `{project}.{dataset}.run_operation_events`
order by operation_id
"""),
        "select": rows(f"""
select
  operation_id,
  operation_kind,
  cast(amount as string) as amount,
  cast(sum(amount) over () as string) as total_amount,
  to_json_string(array(
    select as struct attribute.source, attribute.score
    from unnest(attributes) as attribute
    order by attribute.score
  )) as sorted_attributes
from `{project}.{dataset}.run_operation_events`
qualify row_number() over (order by operation_id desc) <= 3
order by operation_id
"""),
    }


EXPECTED_RUN_OPERATION_STATE = {
    "schema": (
        ("operation_id", "INTEGER", "REQUIRED", ()),
        ("operation_kind", "STRING", "REQUIRED", ()),
        ("amount", "NUMERIC", "NULLABLE", ()),
        ("context_value", "STRING", "NULLABLE", ()),
        ("attributes", "RECORD", "REPEATED", (("source", "STRING", "NULLABLE"), ("score", "INTEGER", "NULLABLE"))),
        ("created_at", "TIMESTAMP", "REQUIRED", ()),
    ),
    "partition": ("DAY", "created_at"),
    "clustering": ("operation_kind",),
    "labels": (("component", "adbc_integration"), ("entrypoint", "run_operation")),
    "description": "Created by dbt run-operation --sql parity coverage",
    "rows": (
        (
            1,
            "insert",
            "13.75",
            "commented_update",
            '[{"source":"literal","score":1},{"source":"update","score":10}]',
            "2025-01-01T00:00:00Z",
        ),
        (2, "merge_update", "21.25", "matched", '[{"source":"merge","score":20}]', "2025-01-02T00:00:00Z"),
        (3, "jinja_integration", "35", "context", '[{"source":"macro","score":3}]', "2025-01-03T00:00:00Z"),
        (4, "merge_insert", "40", "not_matched", '[{"source":"merge","score":40}]', "2025-01-04T00:00:00Z"),
    ),
    "select": (
        (2, "merge_update", "21.25", "110", '[{"source":"merge","score":20}]'),
        (3, "jinja_integration", "35", "110", '[{"source":"macro","score":3}]'),
        (4, "merge_insert", "40", "110", '[{"source":"merge","score":40}]'),
    ),
}


def assert_run_operation_parity(
    *, cache_home: Path, staged_driver: BuiltDriver, target_dir: Path, log_dir: Path
) -> None:
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    location = os.environ["DBT_BIGQUERY_LOCATION"]
    base_dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    datasets = (f"{base_dataset}_run_operation_upstream", f"{base_dataset}_run_operation_local")
    client = bigquery.Client(project=project, location=location)
    outputs = {}
    states = {}

    for mode, dataset in zip(MODES, datasets, strict=True):
        dataset_resource = bigquery.Dataset(f"{project}.{dataset}")
        dataset_resource.location = location
        client.create_dataset(dataset_resource, exists_ok=True)

        env = os.environ.copy()
        env["DBT_BIGQUERY_SCHEMA"] = dataset
        if mode == "upstream":
            env[USE_DBT_DRIVER] = "1"
            env["XDG_CACHE_HOME"] = str((PROJECT_DIR / ".cache" / "upstream").resolve())
        else:
            env.pop(USE_DBT_DRIVER, None)
            env["XDG_CACHE_HOME"] = str(cache_home)

        mode_target = (target_dir / "run-operation" / mode).resolve()
        mode_log = (log_dir / "run-operation" / mode).resolve()
        mode_target.mkdir(parents=True, exist_ok=True)
        mode_log.mkdir(parents=True, exist_ok=True)
        (mode_log / "dbt.log").unlink(missing_ok=True)
        mode_outputs = []
        for name, sql, arguments in run_operation_cases(project, dataset):
            section(f"run-operation parity ({mode}): {name}")
            stdout = captured_dbt_command(
                *("run-operation", "--sql", sql.strip(), *arguments, "--debug", "--log-format", "json"),
                target_dir=mode_target,
                log_dir=mode_log,
                env=env,
            )
            mode_outputs.append(normalized_run_operation_output(stdout, datasets=datasets))
            note(f"captured {len(mode_outputs[-1])} deterministic dbt output events")

        if mode == "local":
            assert_local_driver_resolution(mode_log, staged_driver)
        else:
            assert_dbt_driver_resolution(mode_log)
        outputs[mode] = mode_outputs
        states[mode] = run_operation_state(client, project, dataset)
        for key, want in EXPECTED_RUN_OPERATION_STATE.items():
            expect(f"run-operation {key}", states[mode][key], want)
        note(f"BigQuery metadata, rows and SELECT result are correct for {mode}")

    if outputs["upstream"] != outputs["local"]:
        raise RuntimeError(f"normalized run-operation output differs between drivers: {outputs!r}")
    if states["upstream"] != states["local"]:
        raise RuntimeError(f"run-operation BigQuery state differs between drivers: {states!r}")
    for dataset in datasets:
        client.delete_dataset(f"{project}.{dataset}", delete_contents=True, not_found_ok=True)
    note("canonical and local run-operation output and side effects are identical")


def assert_table_layout(
    table: bigquery.Table, *, partition_field: str | None = None, clustering_fields: list[str] | None = None
) -> None:
    partitioning = table.time_partitioning
    expect(f"{table.table_id} partition", partitioning and partitioning.field, partition_field)
    expect(f"{table.table_id} clustering", table.clustering_fields, clustering_fields)


def assert_field_types(table: bigquery.Table, expected: dict[str, str]) -> None:
    expect(f"{table.table_id} field types", {field.name: field.field_type for field in table.schema}, expected)


def assert_column_description(table: bigquery.Table, column: str, expected: str) -> None:
    field = next((field for field in table.schema if field.name == column), None)
    if field is None:
        raise RuntimeError(f"{table.table_id} has no {column} column")
    expect(f"{table.table_id}.{column} description", field.description, expected)


def assert_nested_persist_docs(client: bigquery.Client, project: str, dataset: str) -> None:
    """Check top-level descriptions and the known loss of nested descriptions.

    dbt 2.0's do_nest_column_data_types drops nested paths before sending
    update_columns_descriptions (docs/UPSTREAM.md UP-77). If nested descriptions
    arrive, update the expectations and the corresponding KNOWN_ISSUES.md entry.
    """
    descriptions = {
        row.field_path: row.description
        for row in client.query(f"""
select field_path, description
from `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name = 'known_issues_contract_nested_types'
""").result()
    }
    if not descriptions:
        raise RuntimeError(
            "known_issues_contract_nested_types reported no column field paths, so persist_docs could not be checked"
        )
    for path, want in NESTED_TOP_LEVEL_DESCRIPTIONS.items():
        expect(f"known_issues_contract_nested_types.{path} description", descriptions.get(path), want)
    if landed := {path: descriptions[path] for path in NESTED_FIELD_PATHS if descriptions.get(path)}:
        raise RuntimeError(
            "dbt 2.0 delivered nested persist_docs descriptions, which UPSTREAM.md UP-77 "
            f"says it cannot: {landed!r}. Upstream has fixed it; assert them here, using "
            "models/known_issues/schema.yml, and remove the KNOWN_ISSUES.md entry."
        )


def assert_table_descriptions(client: bigquery.Client, expected: dict[str, str | None], *, after: str) -> None:
    """Check descriptions after each phase, before later DDL can restore them.

    `None` requires an absent description, as on fct_orders before run-operation.
    """
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    for identifier, want in sorted(expected.items()):
        try:
            table = client.get_table(f"{project}.{dataset}.{identifier}")
        except NotFound as error:
            raise RuntimeError(
                f"{identifier} is missing after {after!r}, so its description could not be checked"
            ) from error
        expect(f"{identifier} description after {after!r}", table.description or None, want)


def assert_authorized_view(
    access_entries: list[bigquery.AccessEntry], *, project: str, dataset: str, view: str
) -> None:
    """Assert the source dataset authorizes the view exactly once, keeping its other grants.

    dbt reapplies grant_access_to after every build of the view. An unconditional append
    would duplicate the entry; replacing the access list would drop the owner and readers.
    """
    views = [entry.entity_id for entry in access_entries if entry.entity_type == "view"]
    matches = [
        entity
        for entity in views
        if isinstance(entity, dict)
        and (entity.get("projectId"), entity.get("datasetId"), entity.get("tableId")) == (project, dataset, view)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one authorized-view entry for {project}.{dataset}.{view}, "
            f"found {len(matches)} among {views!r}"
        )
    if len(views) == len(access_entries):
        raise RuntimeError(
            "authorizing the view replaced the dataset's other access entries instead of "
            f"extending them: {[entry.entity_type for entry in access_entries]!r}"
        )


def assert_python_model_upload(content: str, *, project: str, dataset: str) -> None:
    """Check that write_gcs uploaded the model body and dbt's generated context.

    dbt appends py_script_postfix during compilation, so the uploaded object must
    include the generated relation identifier and resolved ref as well as the source.
    """
    required = {
        "the model's own marker": PYTHON_MODEL_MARKER,
        "the model function": "def model(dbt, session):",
        "the model body": 'orders.groupBy("customer_id").sum("order_total")',
        "dbt 2.0's generated ref function": "def ref(*args, **kwargs):",
        "dbt 2.0's generated dbt object": "class dbtObj:",
        "the resolved relation identifier": f'identifier = "{PYTHON_MODEL_ALIAS}"',
        "the resolved relation dataset": f'schema = "{dataset}"',
        "the resolved relation project": f'database = "{project}"',
        "the resolved upstream relation": f"{project}.{dataset}.fct_orders",
    }
    if missing := sorted(name for name, text in required.items() if text not in content):
        raise RuntimeError(
            f"the uploaded python model object is missing {', '.join(missing)}; "
            f"it holds {len(content)} characters: {content[:400]!r}"
        )


def assert_python_model_submission(run_results_path: Path, *, unique_id: str) -> None:
    """Require the Python model to reach Dataproc submission before failing.

    Reject unsupported-option errors. Dataproc may reject the request because its
    API is disabled or because the configured cluster does not exist.
    """
    result = _node_result(run_results_path, unique_id=unique_id)
    expect(f"{unique_id} status (from the absent cluster)", result.get("status"), "error")
    message = result.get("message")
    if not isinstance(message, str):
        raise RuntimeError(f"{unique_id} reported no error message: {message!r}")
    if rejected := [marker for marker in OPTION_REJECTION_MARKERS if marker in message]:
        raise RuntimeError(
            f"{unique_id} failed on a python-model option the driver does not implement "
            f"({', '.join(rejected)}) rather than at the Dataproc submission: {message!r}"
        )
    if DATAPROC_SUBMISSION_ERROR not in message:
        raise RuntimeError(f"{unique_id} did not fail at the Dataproc job submission: {message!r}")


def storage_object(session: AuthorizedSession, method: str, bucket: str, name: str, ok: tuple[int, ...]) -> bytes:
    """Read or delete a Cloud Storage object through the JSON API."""
    url = f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o/{quote(name, safe='')}"
    response = session.request(method, url, params={"alt": "media"} if method == "GET" else None)
    if response.status_code not in ok:
        action = "readable" if method == "GET" else "deletable"
        raise RuntimeError(f"gs://{bucket}/{name} was not {action}: HTTP {response.status_code} {response.text[:400]}")
    return response.content


def assert_python_model(*, target_dir: Path, log_dir: Path) -> None:
    """Build the python model and check both statements it runs through the driver.

    dbt 2.0 uploads the compiled source with write_gcs, then submits it to Dataproc. The
    bucket is real and the cluster is not, so the upload must leave {schema}/{alias}.py and
    the submission must fail inside Dataproc.
    """
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    bucket = os.environ["DBT_BIGQUERY_GCS_BUCKET"]
    cluster = os.environ["DBT_BIGQUERY_DATAPROC_CLUSTER"]
    name = f"{dataset}/{PYTHON_MODEL_ALIAS}.py"
    # Cloud Storage bills the credentials' quota project, not the bucket's project, and the
    # ADC file may name an unrelated one. The dbt profile pins the driver's quota project too.
    credentials, _ = google.auth.default()
    with AuthorizedSession(credentials.with_quota_project(project)) as session:
        storage_object(session, "DELETE", bucket, name, ok=(200, 204, 404))
        try:
            failing_dbt_command("build", "--select", PYTHON_MODEL_ALIAS, target_dir=target_dir, log_dir=log_dir)
            assert_python_model_submission(
                target_dir / "run_results.json", unique_id=f"{NODE_PREFIX}{PYTHON_MODEL_ALIAS}"
            )
            note(
                f"dbt reported the {cluster} Dataproc job submission failing, so the driver "
                "accepted every python-model option and reached the submission"
            )
            content = storage_object(session, "GET", bucket, name, ok=(200,)).decode()
            assert_python_model_upload(content, project=project, dataset=dataset)
            note(f"gs://{bucket}/{name} holds the compiled python for {PYTHON_MODEL_ALIAS}")
        finally:
            storage_object(session, "DELETE", bucket, name, ok=(200, 204, 404))
    note(f"removed gs://{bucket}/{name}")


# Aggregates over the final warehouse state, each beside its expected value.
EXPECTED_AGGREGATES = {
    "orders": ("fct_orders", "count(*)", 5),
    "incremental_orders": ("incremental_order_totals", "count(*)", 5),
    "revenue_days": ("daily_revenue", "count(*)", 3),
    "snapshot_versions": ("customers_snapshot", "count(*)", 4),
    "typed_rows": ("typed_features", "count(*)", 4),
    "customers": ("customers", "count(*)", 4),
    "seed_orders": ("orders", "count(*)", 5),
    "order_items": ("order_items", "count(*)", 6),
    "merged_rows": ("incremental_order_totals", "countif(load_wave = 2)", 5),
    "mv_statuses": ("exotic_materialized_view", "count(*)", 3),
    "refreshed_mv_statuses": (REFRESHED_VIEW, "count(*)", 3),
    "range_rows": ("exotic_range_partitioned", "count(*)", 5),
    "overwrite_days": ("exotic_dynamic_overwrite", "count(*)", 3),
    "overwrite_wave2": ("exotic_dynamic_overwrite", "countif(load_wave = 2)", 3),
    "evolved_rows": ("exotic_schema_evolution", "count(*)", 5),
    "evolved_filled": ("exotic_schema_evolution", "countif(doubled_total is not null)", 5),
    "wide_rows": ("exotic_wide_types", "count(*)", 3),
    "wide_nulls": ("exotic_wide_types", "countif(label is null)", 1),
    "empty_rows": ("exotic_empty", "count(*)", 0),
    "encoded_rows": ("exotic_encoded_types", "count(*)", 3),
    "encoded_null_rows": (
        "exotic_encoded_types",
        "countif(payload is null and document is null and location is null)",
        1,
    ),
    # The second wave replaces two of three partitions.
    "copied_days": ("exotic_copied_partitions", "count(*)", 3),
    "copied_wave2": ("exotic_copied_partitions", "countif(load_wave = 2)", 2),
    "copied_untouched": ("exotic_copied_partitions", "countif(load_wave = 1 and order_date = date '2026-01-12')", 1),
    "csv_edge_rows": ("exotic_csv_edges", "count(*)", 3),
    "csv_edge_nulls": ("exotic_csv_edges", "countif(optional_label is null)", 1),
    "csv_edge_empty_strings": ("exotic_csv_edges", "countif(label = '')", 1),
    "result_edge_rows": ("exotic_result_edges", "count(*)", 1),
    "empty_result_rows": ("exotic_empty_result", "count(*)", 0),
    "quoted_identifier_rows": ("select", "count(*)", 1),
    "synchronized_rows": ("exotic_schema_sync", "count(*)", 6),
    "synchronized_updates": ("exotic_schema_sync", "countif(load_wave = 2 and original_wave = 1)", 2),
}
# Field types of seeds and models whose declared types must reach BigQuery.
EXPECTED_FIELD_TYPES = {
    "customers": {
        "customer_id": "INTEGER",
        "customer_name": "STRING",
        "region": "STRING",
        "signup_date": "DATE",
        "is_active": "BOOLEAN",
    },
    "orders": {
        "order_id": "INTEGER",
        "customer_id": "INTEGER",
        "ordered_at": "TIMESTAMP",
        "status": "STRING",
        "shipping_amount": "NUMERIC",
    },
    "order_items": {
        "order_id": "INTEGER",
        "line_number": "INTEGER",
        "sku": "STRING",
        "quantity": "INTEGER",
        "unit_price": "NUMERIC",
        "discount_amount": "NUMERIC",
    },
    "exotic_wide_types": {
        "row_id": "INTEGER",
        "label": "STRING",
        "event_date": "DATE",
        "event_datetime": "DATETIME",
        "event_time": "TIME",
        "event_ts": "TIMESTAMP",
        "precise_amount": "NUMERIC",
        "huge_amount": "BIGNUMERIC",
        "ratio": "FLOAT",
        "flag": "BOOLEAN",
    },
    "exotic_empty": {"row_id": "INTEGER", "label": "STRING"},
    # The seed's column_types reach the driver as BIGQUERY:type metadata on string fields.
    "exotic_encoded_types": {"row_id": "INTEGER", "payload": "BYTES", "document": "JSON", "location": "GEOGRAPHY"},
    "exotic_csv_edges": {
        "row_id": "INTEGER",
        "label": "STRING",
        "optional_label": "STRING",
        "literal_null": "STRING",
        "signed_value": "INTEGER",
        "percent_ratio": "FLOAT",
        "event_ts": "TIMESTAMP",
    },
    "select": {"order": "INTEGER", "Mixed Case": "STRING", "dash-name": "STRING", "名前-列": "STRING"},
}


def assert_bigquery_state(*, expected_mv_refresh_minutes: int, local: bool) -> None:
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    client = bigquery.Client(project=project, location=os.environ["DBT_BIGQUERY_LOCATION"])

    def table(identifier: str) -> bigquery.Table:
        return client.get_table(f"{project}.{dataset}.{identifier}")

    fct_orders = table("fct_orders")
    assert_table_layout(fct_orders, partition_field="order_date", clustering_fields=["status", "customer_id"])
    if not {"component": "adbc_integration", "materialization": "table"}.items() <= (fct_orders.labels or {}).items():
        raise RuntimeError(f"unexpected fct_orders labels: {fct_orders.labels!r}")
    # No DDL in this project sets this text; only the option-only update can have persisted it.
    expect("fct_orders description", fct_orders.description, TABLE_DESCRIPTION_SENTINEL)
    line_items = next((field for field in fct_orders.schema if field.name == "line_items"), None)
    if line_items is None:
        raise RuntimeError("fct_orders has no line_items field")
    expect("fct_orders.line_items type and mode", (line_items.field_type, line_items.mode), ("RECORD", "REPEATED"))
    expect(
        "fct_orders.line_items struct fields",
        [field.name for field in line_items.fields],
        ["line_number", "sku", "quantity", "net_amount"],
    )

    assert_table_layout(table("daily_revenue"), partition_field="order_date")
    assert_table_layout(table("dim_customers"), clustering_fields=["region"])
    incremental = table("incremental_order_totals")
    assert_table_layout(incremental, partition_field="order_date", clustering_fields=["customer_id"])
    # The second wave merges into the existing relation, so persist_docs must use the
    # description update rather than DDL.
    expect("incremental_order_totals description", incremental.description, INCREMENTAL_DESCRIPTION)
    assert_column_description(incremental, "order_id", INCREMENTAL_COLUMN_DESCRIPTION)
    assert_nested_persist_docs(client, project, dataset)

    tables = {identifier: table(identifier) for identifier in EXPECTED_FIELD_TYPES}
    for identifier, types in EXPECTED_FIELD_TYPES.items():
        assert_field_types(tables[identifier], types)
    # The seed materialization calls update_table_description directly once the rows are in.
    expect("customers description", tables["customers"].description, SEED_DESCRIPTION)
    if local:
        # dbt's bundled Go drivers drop decimal precision and scale from CSV loads, so under
        # --upstream-adbc `numeric(20,4)` reads back as (None, None).
        assert_decimal_parameters(tables["exotic_wide_types"], {"precise_amount": (20, 4), "huge_amount": (None, None)})
    # A plain `numeric` declaration loads as a parameter-free NUMERIC under both drivers.
    assert_decimal_parameters(tables["orders"], {"shipping_amount": (None, None)})
    assert_column_description(
        tables["select"], "名前-列", "Unicode flexible column, persisted through the metadata update."
    )

    assert_materialized_view_state(
        table("exotic_materialized_view"), expected_refresh_minutes=expected_mv_refresh_minutes
    )
    assert_materialized_view_state(table(REFRESHED_VIEW), expected_refresh_minutes=90)
    assert_range_partitioning(table("exotic_range_partitioned"))
    # copy_partitions replaces partitions with table copies, which must keep the partitioning.
    assert_table_layout(table("exotic_copied_partitions"), partition_field="order_date")

    evolved_columns = [field.name for field in table("exotic_schema_evolution").schema]
    if "doubled_total" not in evolved_columns:
        raise RuntimeError(f"on_schema_change=append_new_columns did not add doubled_total: {evolved_columns!r}")
    expect(
        "exotic_schema_sync columns after on_schema_change=sync_all_columns replaced retired_column",
        [field.name for field in table("exotic_schema_sync").schema],
        ["customer_id", "order_id", "order_date", "load_wave", "wave_payload", "original_wave", "current_details"],
    )

    query = "select " + ", ".join(
        f"(select {aggregate} from `{project}.{dataset}.{name}`) as {key}"
        for key, (name, aggregate, _) in EXPECTED_AGGREGATES.items()
    )
    counts = dict(next(iter(client.query(query).result())).items())
    expected = {key: count for key, (_, _, count) in EXPECTED_AGGREGATES.items()}
    if differences := {key: (counts.get(key), want) for key, want in expected.items() if counts.get(key) != want}:
        raise RuntimeError(f"unexpected warehouse row counts (actual, expected): {differences!r}")

    source_dataset = client.get_dataset(f"{project}.{dataset}{AUTHORIZED_SOURCE_SUFFIX}")
    assert_authorized_view(
        list(source_dataset.access_entries), project=project, dataset=dataset, view="exotic_authorized_view"
    )
    note(f"{source_dataset.dataset_id} authorizes exotic_authorized_view exactly once after two builds of the view")
    authorized_view = table("exotic_authorized_view")
    for column, description in AUTHORIZED_VIEW_COLUMN_DESCRIPTIONS.items():
        assert_column_description(authorized_view, column, description)
    note("persist_docs patched the column descriptions onto the rebuilt view's schema")
    note(
        "table descriptions persisted through the option-only update for the "
        "run-operation, the incremental rebuild and the seed"
    )
    note("BigQuery REST metadata and row-count assertions passed")


def assert_materialized_view_state(table: bigquery.Table, *, expected_refresh_minutes: int) -> None:
    """Assert the MaterializedView metadata dbt reads back through GetTableSchema."""
    view = table.table_id
    expect(f"{view} table_type", table.table_type, "MATERIALIZED_VIEW")
    expect(f"{view} enable_refresh", table.mview_enable_refresh, True)
    interval = table.mview_refresh_interval
    expect(f"{view} refresh interval in minutes", interval and interval.total_seconds() / 60, expected_refresh_minutes)
    expect(f"{view} clustering", table.clustering_fields, ["status"])
    note(f"{view} reports enable_refresh, a {expected_refresh_minutes}-minute refresh interval and clustering")


def assert_materialized_view_refresh(
    client: bigquery.Client, run_results_path: Path, *, project: str, dataset: str, location: str
) -> None:
    """Require dbt to refresh an unchanged materialized view rather than alter or replace it.

    dbt refreshes only when the configuration it reads back through the driver's table metadata
    matches the model's, so this checks that metadata end to end. dbt's canonical driver reports
    a zero expiration time, which dbt reads as a change, so only this driver is checked.
    """
    unique_id = f"{NODE_PREFIX}{REFRESHED_VIEW}"
    changed = "describe_relation read back a configuration change, so dbt altered or replaced the view"
    response = _successful_adapter_response(run_results_path, unique_id=unique_id)
    if not isinstance(response, dict) or response.get("code") != "SCRIPT":
        raise RuntimeError(
            f"{REFRESHED_VIEW} reported a statement other than the SCRIPT of its refresh call; {changed}: {response!r}"
        )
    assert_run_result_adapter_response(
        run_results_path, unique_id=unique_id, code="SCRIPT", rows=None, project=project, location=location
    )
    query = client.get_job(response["job_id"], project=project, location=location).query or ""
    if f"call bq.refresh_materialized_view('{project}.{dataset}.{REFRESHED_VIEW}')" not in query:
        raise RuntimeError(f"dbt reported a job that does not refresh {REFRESHED_VIEW}; {changed}: {query[-400:]!r}")


def assert_range_partitioning(table: bigquery.Table) -> None:
    """Assert the RangePartitioning metadata that only an integer-range table produces."""
    expect("exotic_range_partitioned time partitioning", table.time_partitioning, None)
    partitioning = table.range_partitioning
    if partitioning is None or partitioning.field != "bucket":
        raise RuntimeError(f"unexpected range partitioning: {partitioning!r}")
    bounds = partitioning.range_
    expect("range partition bounds", (bounds.start, bounds.end, bounds.interval), (0, 100, 10))
    expect("range-partitioned clustering", table.clustering_fields, ["parity"])
    if table.expires is None:
        raise RuntimeError("hours_to_expiration did not set an expiration time")
    note("integer-range partitioning, clustering and table expiration round-tripped")


def assert_decimal_parameters(table: bigquery.Table, expected: dict[str, tuple[int | None, int | None]]) -> None:
    """Assert that parameterized decimal precision and scale survived CSV ingestion."""
    parameters = _decimal_parameters(table.schema)
    for name, want in expected.items():
        if name not in parameters:
            raise RuntimeError(f"{table.table_id} has no {name} decimal field")
        expect(f"{table.table_id}.{name} precision/scale", parameters[name], want)


def assert_catalog_artifact(catalog_path: Path, *, expected_nodes: tuple[str, ...]) -> None:
    """Assert dbt's catalog.json describes the models the INFORMATION_SCHEMA reads covered."""
    nodes = read_artifact(catalog_path).get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        raise RuntimeError(f"{catalog_path} has no catalog nodes")
    for unique_id in expected_nodes:
        if unique_id not in nodes:
            raise RuntimeError(f"{catalog_path} has no catalog entry for {unique_id}")
        columns = nodes[unique_id].get("columns")
        if not isinstance(columns, dict) or not columns:
            raise RuntimeError(f"{unique_id} has no catalogued columns")
    fct_orders = nodes[f"{NODE_PREFIX}fct_orders"]["columns"]
    if "order_id" not in {name.split(".")[0] for name in fct_orders}:
        raise RuntimeError(f"fct_orders catalog columns lack order_id: {sorted(fct_orders)!r}")
    note(f"catalog.json describes {len(nodes)} nodes with column metadata")


def capture_show_results(*, target_dir: Path, log_dir: Path) -> None:
    """Save the result values dbt fetched through the selected driver for each SHOW_MODELS model."""
    results = {}
    for model in SHOW_MODELS:
        stdout = captured_dbt_command(
            *("show", "--select", model, "--output", "json", "--quiet"),
            target_dir=target_dir,
            log_dir=log_dir,
            env=os.environ.copy(),
        )
        # `dbt show --quiet --output json` emits exactly one JSON array of row objects.
        candidates = []
        for line in stdout.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, list) and all(isinstance(row, dict) for row in value):
                candidates.append(value)
        if len(candidates) != 1:
            raise RuntimeError(f"dbt show emitted {len(candidates)} JSON row arrays, expected one: {stdout[-1_000:]!r}")
        results[model] = candidates[0]
        note(f"captured {len(candidates[0])} ADBC rows from {model}")
    (target_dir / "show_results.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")


def _without_decimal_parameters(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _without_decimal_parameters(child) for key, child in value.items() if key not in ("precision", "scale")
        }
    if isinstance(value, list):
        return [_without_decimal_parameters(child) for child in value]
    return value


def _decimal_parameters(
    fields: Sequence[bigquery.SchemaField], prefix: str = ""
) -> dict[str, tuple[int | None, int | None]]:
    parameters = {}
    for field in fields:
        path = f"{prefix}{field.name}"
        if field.field_type.upper() in ("NUMERIC", "BIGNUMERIC"):
            parameters[path] = (field.precision, field.scale)
        parameters.update(_decimal_parameters(field.fields, f"{path}."))
    return parameters


def warehouse_snapshot(
    client: bigquery.Client, *, project: str, main_dataset: str, location: str
) -> dict[tuple[str, str], dict]:
    """Collect stable metadata and the complete row multiset from both suite datasets."""
    authorized_dataset = f"{main_dataset}{AUTHORIZED_SOURCE_SUFFIX}"

    def anonymized(value: object) -> object:
        # Dataset names are plain identifiers, so they need no JSON escaping.
        text = json.dumps(value).replace(authorized_dataset, "<authorized_source>").replace(main_dataset, "<main>")
        return json.loads(text)

    snapshot = {}
    for scope, dataset in (("main", main_dataset), ("authorized_source", authorized_dataset)):
        for item in client.list_tables(f"{project}.{dataset}"):
            table = client.get_table(item.reference)
            resource = table.to_api_repr()
            refresh = table.mview_refresh_interval
            metadata = {
                "table_type": table.table_type,
                "description": table.description,
                "friendly_name": table.friendly_name,
                "labels": dict(sorted((table.labels or {}).items())),
                "expires": table.expires is not None,
                "time_partitioning": resource.get("timePartitioning"),
                "range_partitioning": resource.get("rangePartitioning"),
                "clustering": resource.get("clustering"),
                "require_partition_filter": table.require_partition_filter,
                "table_constraints": anonymized(resource.get("tableConstraints")),
                "view_query": anonymized(table.view_query),
                "view_use_legacy_sql": table.view_use_legacy_sql,
                "materialized_view_query": anonymized(table.mview_query),
                "materialized_view_enable_refresh": table.mview_enable_refresh,
                "materialized_view_refresh_seconds": refresh.total_seconds() if refresh is not None else None,
                "materialized_view_allow_non_incremental": table.mview_allow_non_incremental_definition,
                "max_staleness": table.max_staleness,
            }
            row_value = "t"
            # loaded_at uses current_timestamp(), so it differs across sequential driver runs.
            if scope == "main" and table.table_id == "incremental_order_totals":
                columns = ", ".join(f"t.`{field.name}`" for field in table.schema if field.name != "loaded_at")
                row_value = f"(select as struct {columns})"
            path = f"`{table.project}.{table.dataset_id}.{table.table_id}`"
            query = f"select to_json_string({row_value}) as row_json from {path} as t order by row_json"
            snapshot[(scope, table.table_id)] = {
                "metadata": metadata,
                "schema": _without_decimal_parameters([field.to_api_repr() for field in table.schema]),
                "decimal_parameters": _decimal_parameters(table.schema),
                "rows": tuple(row.row_json for row in client.query(query, location=location).result()),
            }
    return snapshot


def compare_warehouse_snapshots(upstream: dict[tuple[str, str], dict], local: dict[tuple[str, str], dict]) -> list[str]:
    """Require identical semantics except for the documented seed decimal parameters."""
    if upstream.keys() != local.keys():
        raise RuntimeError(
            f"warehouse relations differ: missing upstream={sorted(local.keys() - upstream.keys())!r}, "
            f"missing local={sorted(upstream.keys() - local.keys())!r}"
        )
    differences = {}
    for relation in sorted(upstream):
        for part in ("metadata", "schema", "rows"):
            if upstream[relation][part] != local[relation][part]:
                raise RuntimeError(
                    f"warehouse {part} differs for {relation}: "
                    f"upstream={upstream[relation][part]!r}, local={local[relation][part]!r}"
                )
        upstream_parameters = upstream[relation]["decimal_parameters"]
        local_parameters = local[relation]["decimal_parameters"]
        for field in upstream_parameters.keys() | local_parameters.keys():
            values = (upstream_parameters.get(field), local_parameters.get(field))
            if values[0] != values[1]:
                differences[(*relation, field)] = values
    expect("warehouse decimal-parameter differences", differences, EXPECTED_DECIMAL_PARAMETER_DIFFERENCES)
    return [
        f"{scope}.{relation}.{field}: upstream={upstream_value}, local={local_value}"
        for (scope, relation, field), (upstream_value, local_value) in sorted(differences.items())
    ]


def semantic_catalog(catalog_path: Path) -> dict:
    """Keep catalog fields whose values should agree across driver implementations."""
    catalog = read_artifact(catalog_path)
    result = {}
    for part in ("nodes", "sources"):
        entries = catalog.get(part)
        if not isinstance(entries, dict):
            raise RuntimeError(f"{catalog_path} has no {part} map")
        result[part] = {
            unique_id: {
                "metadata": {
                    key: entry.get("metadata", {}).get(key) for key in ("type", "name", "database", "comment", "owner")
                },
                "columns": {
                    name: {
                        "type": re.sub(r"\b(BIGNUMERIC|NUMERIC)\(\d+,\s*\d+\)", r"\1", column.get("type"))
                        if isinstance(column.get("type"), str)
                        else column.get("type"),
                        **{key: column.get(key) for key in ("index", "name", "comment")},
                    }
                    for name, column in entry.get("columns", {}).items()
                },
                "stats": {
                    name: stat.get("value")
                    for name, stat in entry.get("stats", {}).items()
                    if name not in ("bytes", "num_rows")
                },
            }
            for unique_id, entry in entries.items()
        }
    return result


def assert_full_project_parity(
    client: bigquery.Client, *, project: str, location: str, datasets: dict[str, str]
) -> list[str]:
    """Compare complete warehouse rows/metadata, dbt show values and catalog metadata."""
    show_results = {mode: read_artifact(PROJECT_DIR / "target" / mode / "show_results.json") for mode in MODES}
    if show_results["upstream"] != show_results["local"]:
        raise RuntimeError(f"dbt show results differ between drivers: {show_results!r}")
    catalogs = [semantic_catalog(PROJECT_DIR / "target" / mode / "catalog.json") for mode in MODES]
    if catalogs[0] != catalogs[1]:
        raise RuntimeError("semantic catalog metadata differs between drivers")
    upstream, local = (
        warehouse_snapshot(client, project=project, main_dataset=datasets[mode], location=location) for mode in MODES
    )
    return compare_warehouse_snapshots(upstream, local)


def _validate_integration_schema(schema: str) -> None:
    if not schema.startswith("bigquery_adbc_integration_") or not schema.replace("_", "").isalnum():
        raise RuntimeError(f"refusing unsafe integration schema name: {schema}")


def compare_drivers(*, keep_datasets: bool, dbt_archive: Path | None) -> int:
    """Run the complete project twice and compare its stable observable results."""
    project = require_project()
    location = os.environ.get("DBT_BIGQUERY_LOCATION", "US")
    configured_base = os.environ.get("DBT_BIGQUERY_SCHEMA", "bigquery_adbc_integration_compare")
    _validate_integration_schema(configured_base)
    base = f"{configured_base}_{datetime.now(UTC):%Y%m%d_%H%M%S}_{os.getpid()}"
    datasets = {mode: f"{base}_{mode}" for mode in MODES}
    for dataset in datasets.values():
        _validate_integration_schema(dataset)

    client = bigquery.Client(project=project, location=location)
    targets = [name for dataset in datasets.values() for name in (dataset, f"{dataset}{AUTHORIZED_SOURCE_SUFFIX}")]
    for dataset in targets:
        try:
            client.get_dataset(f"{project}.{dataset}")
        except NotFound:
            continue
        raise RuntimeError(f"refusing to reuse existing comparison dataset {project}.{dataset}")

    # Both runs must use one build, and only the first would otherwise pay for it.
    archive = dbt_archive or build_dbt(PROJECT_DIR / ".cache" / "dbt-build")

    section("Run the complete dbt project with both BigQuery ADBC drivers")
    failures = []
    for mode in MODES:
        env = os.environ | {
            "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": project,
            "DBT_BIGQUERY_LOCATION": location,
            "DBT_BIGQUERY_SCHEMA": datasets[mode],
            "DBT_ADBC_CACHE_DIR": str((PROJECT_DIR / ".cache" / "compare" / mode).resolve()),
        }
        command = [sys.executable, str(Path(__file__).resolve()), "--keep-datasets", "--dbt-archive", str(archive)]
        if mode == "upstream":
            command.append("--upstream-adbc")
        show_command(command)
        if (returncode := subprocess.run(command, cwd=REPO_DIR, env=env, check=False).returncode) != 0:
            failures.append((mode, returncode))
    if failures:
        raise RuntimeError(f"driver comparison runs failed: {failures!r}; retained datasets {targets!r}")

    section("Compare complete dbt results across drivers")
    differences = assert_full_project_parity(client, project=project, location=location, datasets=datasets)
    note("dbt show values, complete row multisets and stable metadata are identical")
    for difference in differences:
        note(f"expected decimal metadata difference: {difference}")
    if keep_datasets:
        note(f"retained comparison datasets: {', '.join(targets)}")
    else:
        for dataset in targets:
            client.delete_dataset(f"{project}.{dataset}", delete_contents=True, not_found_ok=True)
        note("removed all four comparison datasets")
    print(f"\n{_style('1;32', 'The local and default dbt BigQuery drivers produced equivalent results.')}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--upstream-adbc", action="store_true", help="use dbt 2.0's canonical BigQuery ADBC driver")
    mode.add_argument(
        "--compare",
        action="store_true",
        help="run the complete project with both drivers and compare their results",
    )
    parser.add_argument(
        "--dbt-archive",
        type=Path,
        help="run this tools/dbt-build archive instead of building one; a release archive downloads its driver",
    )
    parser.add_argument(
        "--keep-datasets", action="store_true", help="retain successful integration datasets for inspection"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project = require_project()
    if args.compare:
        return compare_drivers(keep_datasets=args.keep_datasets, dbt_archive=args.dbt_archive)
    mode = "upstream" if args.upstream_adbc else "local"

    archive = (args.dbt_archive or build_dbt(PROJECT_DIR / ".cache" / "dbt-build")).resolve()
    install = install_dbt(archive, PROJECT_DIR / ".cache" / "dbt" / mode)
    dbt_version = subprocess.run(
        [install / DBT_EXECUTABLE, "--version"], check=True, text=True, capture_output=True
    ).stdout.strip()
    identifier = DBT_IDENTIFIER.fullmatch(dbt_version)
    if identifier is None:
        raise RuntimeError(f"a tools/dbt-build archive is required; found: {dbt_version}")

    os.environ.setdefault("DBT_BIGQUERY_LOCATION", "US")
    os.environ.setdefault("DBT_BIGQUERY_SCHEMA", f"bigquery_adbc_integration_{mode}")
    # Match profiles.yml so upload and submission assertions inspect the configured resources.
    os.environ.setdefault("DBT_BIGQUERY_GCS_BUCKET", f"{project}-adbc-scratch")
    os.environ.setdefault("DBT_BIGQUERY_DATAPROC_CLUSTER", "bigquery-adbc-integration-absent-cluster")
    os.environ.setdefault("DBT_BIGQUERY_COMPUTE_REGION", "us-central1")
    location = os.environ["DBT_BIGQUERY_LOCATION"]
    schema = os.environ["DBT_BIGQUERY_SCHEMA"]
    _validate_integration_schema(schema)

    cache_home = Path(os.environ.get("DBT_ADBC_CACHE_DIR", PROJECT_DIR / ".cache" / mode)).resolve()
    os.environ["XDG_CACHE_HOME"] = str(cache_home)
    target_dir = PROJECT_DIR / "target" / mode
    log_dir = PROJECT_DIR / "logs" / mode
    target_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "dbt.log").unlink(missing_ok=True)
    (target_dir / "show_results.json").unlink(missing_ok=True)
    (target_dir / "sources.json").unlink(missing_ok=True)
    run_results = target_dir / "run_results.json"

    section(f"dbt BigQuery integration ({mode} driver)")
    note(dbt_version)
    note(f"project={project} dataset={schema} location={location}")

    os.environ["PATH"] = f"{install}{os.pathsep}{os.environ['PATH']}"
    # This repository's driver, or None when dbt resolves its own.
    staged_driver = None
    if args.upstream_adbc:
        os.environ[USE_DBT_DRIVER] = "1"
        note("using dbt 2.0's canonical BigQuery ADBC driver")
    else:
        os.environ.pop(USE_DBT_DRIVER, None)
        staged_driver = built_driver(install, cache_home, identifier["driver"])
        if staged_driver.downloaded:
            # Only this file: the cache directory can be the caller's. It reappearing is what
            # proves the download, so it must not survive from an earlier run.
            staged_driver.library.unlink(missing_ok=True)
            note(f"release build: dbt must download {staged_driver.library.name}")
    expected_hash = None

    client = bigquery.Client(project=project, location=location)
    coverage_baseline = read_artifact(PROJECT_DIR / "coverage.json")
    expected_descriptions: dict[str, str | None] = {}

    def run_phase(
        title: str,
        *arguments: str,
        required_events: tuple[re.Pattern[str], ...] | None = None,
        completion: str | None = None,
        coverage: str | None = None,
    ) -> datetime:
        section(title)
        started = datetime.now(UTC)
        if arguments[0] in ("build", "seed", "source"):
            if coverage is None or coverage not in coverage_baseline:
                raise RuntimeError(f"{title!r} needs a coverage baseline")
            nodes = checked_phase_command(
                *arguments,
                target_dir=target_dir,
                log_dir=log_dir,
                coverage=coverage,
                expected_nodes=coverage_baseline[coverage],
            )
            note(f"all {nodes} expected dbt results have the required status")
        else:
            dbt_command(
                *arguments,
                target_dir=target_dir,
                log_dir=log_dir,
                required_events=required_events,
                completion=completion,
            )
        if expected_descriptions:
            assert_table_descriptions(client, expected_descriptions, after=title)
            note(f"table descriptions are as expected for {', '.join(sorted(expected_descriptions))}")
        return started

    try:
        run_phase("Check credentials and connection", "debug", "--connection")
        if staged_driver is not None:
            assert_local_driver_resolution(log_dir, staged_driver)
            expected_hash = local_driver.sha256(staged_driver.library)
        else:
            assert_dbt_driver_resolution(log_dir)
        run_phase("Start from an empty integration dataset", "run-operation", "drop_integration_schema")

        # Recheck these after every phase: rebuilds must preserve persist_docs.
        # fct_orders stays undescribed until assert_table_description_update below.
        expected_descriptions = {
            "customers": SEED_DESCRIPTION,
            "incremental_order_totals": INCREMENTAL_DESCRIPTION,
            "fct_orders": None,
        }
        # The Python model's expected Dataproc failure is checked separately below.
        run_phase(
            "Build seeds, models, unit tests and data tests",
            *("build", "--full-refresh", "--exclude", "tag:snapshot", "tag:python_model"),
            coverage="initial_build",
        )
        run_phase(
            "Exercise BigQuery source freshness metadata",
            *("source", "freshness", "--select"),
            *("source:known_issues_seed_tables.orders", "source:exotic_seed_tables.fct_orders_metadata"),
            coverage="freshness",
        )
        run_phase("Rebuild seeds to exercise replacement ingestion", "seed", "--full-refresh", coverage="seeds")
        phase_started = run_phase(
            "Exercise incremental MERGE and insert-overwrite updates",
            *("build", "--select", "tag:incremental", "--vars", "{incremental_wave: 2}"),
            coverage="incremental",
        )
        if staged_driver is None:
            assert_canonical_incremental_adapter_response(
                _successful_adapter_response(run_results, unique_id=INCREMENTAL_UNIQUE_ID),
                expected_project=project,
                expected_location=location,
            )
            note("canonical dbt driver retained its known SUCCESS/zero-row MERGE response")
        else:
            for unique_id, code, rows in (
                (INCREMENTAL_UNIQUE_ID, "MERGE", 5),
                (f"{NODE_PREFIX}exotic_dynamic_overwrite", "SCRIPT", None),
            ):
                assert_run_result_adapter_response(
                    run_results, unique_id=unique_id, code=code, rows=rows, project=project, location=location
                )
            note(
                "dbt surfaced MERGE with its five affected rows, the dynamic insert-overwrite SCRIPT "
                "with no row count, and both statements' job statistics"
            )
            assert_incremental_job_options(
                client, run_results, project=project, dataset=schema, location=location, started=phase_started
            )
            note(
                "the MERGE and copy_partitions' copy jobs carry dbt's job labels, and "
                "the profile's and the model's job timeouts"
            )

        run_phase(
            "Rebuild tables and an unchanged materialized view over existing relations",
            *("build", "--select", "dim_customers", "exotic_range_partitioned", REFRESHED_VIEW),
            coverage="replace_relations",
        )
        if staged_driver is not None:
            assert_materialized_view_refresh(client, run_results, project=project, dataset=schema, location=location)
            note(f"dbt read {REFRESHED_VIEW}'s configuration back unchanged and refreshed it with a SCRIPT job")
        run_phase(
            "Rebuild the authorized view to reapply its dataset grant",
            *("build", "--select", "exotic_authorized_view"),
            coverage="authorized_view",
        )
        run_phase(
            "Alter the materialized view configuration",
            *("build", "--select", "exotic_materialized_view", "--vars", "{mv_refresh_minutes: 60}"),
            coverage="alter_materialized_view",
        )
        run_phase(
            "Exercise the adapter metadata surface",
            *("run-operation", "assert_adapter_surface"),
            *(() if staged_driver is not None else ("--vars", "{canonical_driver: true}")),
            # Only this driver writes the read-path events.
            required_events=READ_PATH_EVENTS if staged_driver is not None else (),
            completion="adapter surface checks passed for ",
        )
        note("the driver reported no Storage Read fallback or other warning")
        if staged_driver is not None:
            note("the driver read results both over further REST pages and through Storage Read")
        expected_descriptions["fct_orders"] = TABLE_DESCRIPTION_SENTINEL
        run_phase(
            "Describe a relation through the option-only table description update",
            *("run-operation", "assert_table_description_update"),
            *("--vars", f"{{table_description_sentinel: '{TABLE_DESCRIPTION_SENTINEL}'}}"),
            completion="update_table_description persisted the sentinel on ",
        )
        run_phase(
            "Build the first snapshot wave",
            *("build", "--select", "tag:snapshot", "--exclude", "tag:snapshot_final", "--vars", "{snapshot_wave: 1}"),
            coverage="snapshot_initial",
        )
        run_phase(
            "Apply and test the changed snapshot wave",
            *("build", "--select", "tag:snapshot", "--vars", "{snapshot_wave: 2}"),
            coverage="snapshot_changed",
        )

        section("Require dbt to report an intentionally failing data test")
        saved_failure = target_dir / "phases" / "failure_probe" / "run_results.json"
        saved_failure.unlink(missing_ok=True)
        failing_dbt_command(
            *("test", "--select", "exotic_failure_probe", "--vars", "{validate_failure_reporting: true}"),
            target_dir=target_dir,
            log_dir=log_dir,
        )
        # A one-row failing test must report a data failure, not a SQL or transport error.
        probe = _node_result(run_results, unique_id="test.bigquery_adbc_integration.exotic_failure_probe")
        if probe.get("status") != "fail" or type(probe.get("failures")) is not int or probe["failures"] != 1:
            raise RuntimeError(
                "exotic_failure_probe must report status 'fail' and exactly one failing row; "
                f"got status {probe.get('status')!r}, failures {probe.get('failures')!r}"
            )
        saved_failure.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run_results, saved_failure)
        note("dbt failed with exactly one failing row from the selected test")

        section("Capture BigQuery edge values through ADBC and dbt show")
        capture_show_results(target_dir=target_dir, log_dir=log_dir)

        if staged_driver is not None:
            if local_driver.sha256(staged_driver.library) != expected_hash:
                raise RuntimeError(f"dbt replaced its driver at {staged_driver.library}")
            note("verified that the driver file remained intact")
            section("Upload and submit a python model through write_gcs and Dataproc")
            assert_python_model(target_dir=target_dir, log_dir=log_dir)

        section("Assert BigQuery state through the REST API")
        assert_bigquery_state(expected_mv_refresh_minutes=60, local=staged_driver is not None)

        section("Compile the complete project and write its catalog")
        (target_dir / "catalog.json").unlink(missing_ok=True)
        dbt_command("compile", "--write-catalog", target_dir=target_dir, log_dir=log_dir)
        assert_catalog_artifact(
            target_dir / "catalog.json",
            expected_nodes=(
                f"{NODE_PREFIX}fct_orders",
                f"{NODE_PREFIX}exotic_materialized_view",
                f"{NODE_PREFIX}exotic_range_partitioned",
                "seed.bigquery_adbc_integration.exotic_wide_types",
            ),
        )

        if staged_driver is not None:
            section("Compare dbt run-operation --sql across canonical and local drivers")
            assert_run_operation_parity(
                cache_home=cache_home, staged_driver=staged_driver, target_dir=target_dir, log_dir=log_dir
            )
            if local_driver.sha256(staged_driver.library) != expected_hash:
                raise RuntimeError(f"dbt replaced its driver at {staged_driver.library} during parity")

        if args.keep_datasets:
            note(f"retained {schema} and {schema}{AUTHORIZED_SOURCE_SUFFIX} (--keep-datasets)")
        else:
            section("Remove integration warehouse objects")
            dbt_command("run-operation", "drop_integration_schema", target_dir=target_dir, log_dir=log_dir)
    except (subprocess.CalledProcessError, RuntimeError, GoogleAPICallError) as error:
        if not isinstance(error, subprocess.CalledProcessError):
            print(f"\n{error}", file=sys.stderr)
        print(
            f"\nIntegration failed. Datasets {project}.{schema} and "
            f"{project}.{schema}{AUTHORIZED_SOURCE_SUFFIX} were retained for inspection.",
            file=sys.stderr,
        )
        if staged_driver is not None:
            print(
                "Run-operation parity datasets with suffixes _run_operation_upstream and "
                "_run_operation_local were also retained when created.",
                file=sys.stderr,
            )
        return error.returncode if isinstance(error, subprocess.CalledProcessError) else 1

    print(f"\n{_style('1;32', f'All dbt integration checks passed with the {mode} driver.')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
