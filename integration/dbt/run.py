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
from collections.abc import Mapping, Sequence
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
# The driver version in `dbt --version`, which tools/dbt-build/build.sh checks in full.
DBT_IDENTIFIER = re.compile(r"dbt \(modified with bigquery-adbc (?P<driver>\S+)\) \S+ \S+")
LIBRARY_NAME = "libadbc_driver_bigquery.so"
# The authorized view reads from a second dataset, removed by drop_integration_schema.
AUTHORIZED_SOURCE_SUFFIX = "_authorized_source"
# Only adbc.bigquery.table.update_description writes this text; no DDL in the project sets it.
TABLE_DESCRIPTION_SENTINEL = "described through the option-only BigQuery table description update"
# persist_docs descriptions from seeds/schema.yml and models/schema.yml. The seed materialization
# and later incremental waves set them through the driver's description update, not DDL.
SEED_DESCRIPTION = "Seeded customer dimension."
INCREMENTAL_DESCRIPTION = "Incremental order totals."
INCREMENTAL_COLUMN_DESCRIPTION = (
    "Surrogate order key, documented through the column metadata update."
)
# From models/exotic/schema.yml, patched onto a view's schema after each CREATE OR REPLACE VIEW.
AUTHORIZED_VIEW_COLUMN_DESCRIPTIONS = {
    "order_date": "Order day, documented through the view's column metadata update.",
    "order_count": "Orders per day, documented through the view's column metadata update.",
}
# Descriptions from models/known_issues/schema.yml. dbt-core 2.0 persists top-level
# descriptions but loses nested paths (UPSTREAM.md UP-77); assertions detect a change to either.
NESTED_TOP_LEVEL_DESCRIPTIONS = {
    "id": "Required identifier.",
    "array_of_structs": "Struct fields declared out of alphabetical order.",
    "struct_column": "Nested struct with separately documented fields.",
}
NESTED_FIELD_DESCRIPTIONS = {
    "array_of_structs.d": "First nested field.",
    "array_of_structs.e": "Second nested field.",
    "array_of_structs.b_id": "Third nested field.",
    "array_of_structs.a_color": "Fourth nested field.",
    "struct_column.id": "Nested identifier.",
    "struct_column.taste": "Nested text.",
}
REFRESHED_VIEW = "exotic_refreshed_materialized_view"
REFRESHED_VIEW_UNIQUE_ID = f"model.bigquery_adbc_integration.{REFRESHED_VIEW}"
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
PYTHON_MODEL_UNIQUE_ID = f"model.bigquery_adbc_integration.{PYTHON_MODEL_ALIAS}"
FAILURE_PROBE_UNIQUE_ID = "test.bigquery_adbc_integration.exotic_failure_probe"
# Identifies models/exotic/exotic_python_model.py in the uploaded source.
PYTHON_MODEL_MARKER = "uploaded through adbc.bigquery.write_gcs"
# The driver's context for a rejected Dataproc submission (src/dbt/dataproc.rs). Failing with
# it proves the driver accepted every python-model option.
DATAPROC_SUBMISSION_ERROR = "failed to submit the BigQuery Dataproc job"
# Substrings that would mean the driver rejected one of dbt-core 2.0's python-model options instead
# of reaching Dataproc, from option_rejection in src/error/options.rs.
OPTION_REJECTION_MARKERS = (
    "unsupported BigQuery",
    "unknown BigQuery",
    "NOT_IMPLEMENTED",
    "write_gcs",
)
# Compare values rendered by dbt through both drivers, including empty results with a schema.
# The ordinary data tests separately check the values stored in BigQuery.
SHOW_MODELS = (
    "exotic_bigquery_types",
    "exotic_result_edges",
    "exotic_empty_result",
    "exotic_quoted_identifiers",
)
# dbt_project.yml's query-comment before its node.unique_id. With job-label: true, dbt-core 2.0
# sends the rendered comment as the query_comment job label, beside dbt_invocation_id.
QUERY_COMMENT_PREFIX = "bigquery-adbc integration: "
# profiles.yml's job_execution_timeout_seconds, and the model-level override on
# exotic_copied_partitions, as the jobTimeoutMs dbt-core 2.0 sends through the driver.
PROFILE_JOB_TIMEOUT_MS = 300_000
COPIED_PARTITIONS_JOB_TIMEOUT_MS = 900_000
INCREMENTAL_UNIQUE_ID = "model.bigquery_adbc_integration.incremental_order_totals"
# dbt reports this inlined model as skipped; its unit test must still pass.
EPHEMERAL_UNIQUE_ID = "model.bigquery_adbc_integration.int_order_items"
COPIED_PARTITIONS_UNIQUE_ID = "model.bigquery_adbc_integration.exotic_copied_partitions"
# The partitions exotic_copied_partitions lists, as copy-job partition decorators.
COPIED_PARTITIONS = ("20260110", "20260111")
# dbt's bundled driver drops decimal parameters from CSV seed schemas; this driver loads each
# column as declared (DBT-CORE.md), so the one parameterized declaration is the only permitted
# schema difference. A plain `numeric` stays unparameterized under both drivers.
EXPECTED_DECIMAL_PARAMETER_DIFFERENCES = {
    ("main", "exotic_wide_types", "precise_amount"): ((None, None), (20, 4)),
}


class Log:
    def __init__(self) -> None:
        self.color = sys.stdout.isatty()

    def _wrap(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def section(self, message: str) -> None:
        print(f"\n{self._wrap('1;34', f'==> {message}')}", flush=True)

    def note(self, message: str) -> None:
        print(f"{self._wrap('32', '  ->')} {message}", flush=True)

    def command(self, command: list[str]) -> None:
        print(f"{self._wrap('33', '  $')} {shlex.join(command)}", flush=True)

    def success(self, message: str) -> None:
        print(f"\n{self._wrap('1;32', message)}", flush=True)


LOG = Log()


def make_dbt_command(*arguments: str, target_dir: Path, log_dir: Path) -> list[str]:
    return [
        "dbt",
        *arguments,
        "--project-dir",
        str(PROJECT_DIR),
        "--profiles-dir",
        str(PROJECT_DIR),
        "--target-path",
        str(target_dir),
        "--log-path",
        str(log_dir),
        "--no-version-check",
    ]


def failing_dbt_command(*arguments: str, target_dir: Path, log_dir: Path) -> None:
    """Run a dbt command and require a nonzero exit status.

    Subsequent assertions check the error details in run_results.json.
    """

    command = make_dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    (target_dir / "run_results.json").unlink(missing_ok=True)
    LOG.command(command)
    result = subprocess.run(command, cwd=PROJECT_DIR, check=False)
    if result.returncode == 0:
        raise RuntimeError(f"expected {shlex.join(command)} to fail, but it succeeded")


def driver_warnings(stderr: str) -> list[str]:
    """Return the driver's warn and error diagnostic lines from a process's standard error."""

    return [
        line
        for line in stderr.splitlines()
        if re.search(r"\[bigquery-adbc\] (?:WARN|ERROR) ", line)
    ]


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
    LOG.command(command)
    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        env=os.environ | {DRIVER_LOG_LEVEL: "debug"} if required_events is not None else None,
        stderr=subprocess.PIPE if required_events is not None else None,
        text=True,
        check=False,
    )
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    result.check_returncode()
    if required_events is not None:
        warnings = driver_warnings(result.stderr)
        if warnings:
            raise RuntimeError(
                f"the driver reported a degraded path during dbt {arguments[0]}: {warnings!r}"
            )
        missing = [event.pattern for event in required_events if not event.search(result.stderr)]
        if missing:
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


def captured_dbt_command(
    *arguments: str,
    target_dir: Path,
    log_dir: Path,
    env: dict[str, str],
) -> str:
    command = make_dbt_command(*arguments, target_dir=target_dir, log_dir=log_dir)
    LOG.command(command)
    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0 and result.stdout:
        print(result.stdout, end="", file=sys.stderr)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    result.check_returncode()
    return result.stdout


def _dbt_rows_number(rows: int) -> str:
    """Render a row count the same way dbt-core 2.0's BigQuery response does."""

    value = float(rows)
    for suffix in ("", "k", "m", "b", "t"):
        if abs(value) < 1_000:
            return f"{value:.1f}{suffix}"
        value /= 1_000
    return f"{value:.1f}q"


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

    code = response.get("code")
    if code != expected_code:
        raise RuntimeError(
            f"adapter_response code is {code!r}, expected {expected_code!r}"
        )
    rows_affected = response.get("rows_affected")
    if rows_affected != expected_rows_affected or isinstance(rows_affected, bool):
        raise RuntimeError(
            "adapter_response rows_affected is "
            f"{rows_affected!r}, expected {expected_rows_affected}"
        )

    message = response.get("message")
    if response.get("_message") != message or not isinstance(message, str):
        raise RuntimeError(
            "adapter_response message/_message are missing or disagree: "
            f"{response.get('message')!r} / {response.get('_message')!r}"
        )
    processed = r"(?:0|[0-9]+(?:\.[0-9]+)? (?:Bytes|KiB|MiB|GiB|TiB|PiB))"
    if not expected_rows_affected:
        expected_message = rf"{re.escape(expected_code)} \({processed} processed\)"
    else:
        expected_message = (
            rf"{re.escape(expected_code)} "
            rf"\({re.escape(_dbt_rows_number(expected_rows_affected))} rows, {processed} processed\)"
        )
    if re.fullmatch(expected_message, message) is None:
        raise RuntimeError(
            f"adapter_response message is {message!r}, expected {expected_message!r}"
        )

    statistic_keys = ("bytes_processed", "bytes_billed", "slot_ms")
    for key in statistic_keys:
        value = response.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(
                f"adapter_response {key} is {value!r}, expected a non-negative integer"
            )

    query_id = response.get("query_id")
    job_id = response.get("job_id")
    if not isinstance(query_id, str) or not query_id or job_id != query_id:
        raise RuntimeError(
            "adapter_response query_id/job_id are not equal non-empty strings: "
            f"{query_id!r} / {job_id!r}"
        )

    for key, expected in (
        ("project_id", expected_project),
        ("location", expected_location),
    ):
        if response.get(key) != expected:
            raise RuntimeError(
                f"adapter_response {key} is {response.get(key)!r}, expected {expected!r}"
            )


def assert_run_result_adapter_response(
    run_results_path: Path,
    *,
    unique_id: str,
    expected_code: str,
    expected_rows_affected: int | None,
    expected_project: str,
    expected_location: str,
) -> None:
    """Find one dbt result and validate its adapter response before dbt overwrites it."""

    response = _successful_adapter_response(run_results_path, unique_id=unique_id)
    try:
        assert_adapter_response_contract(
            response,
            expected_code=expected_code,
            expected_rows_affected=expected_rows_affected,
            expected_project=expected_project,
            expected_location=expected_location,
        )
    except RuntimeError as error:
        observed = json.dumps(response, sort_keys=True)
        raise RuntimeError(f"{unique_id}: {error}; observed {observed}") from error


def read_artifact(path: Path) -> dict:
    """Read a dbt JSON artifact, failing the run with its path if it is missing or malformed."""

    try:
        artifact = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"failed to read {path}: {error}") from error
    if not isinstance(artifact, dict):
        raise RuntimeError(f"{path} must contain a JSON object")
    return artifact


def _node_result(run_results_path: Path, *, unique_id: str) -> dict:
    """Return the single dbt result for one node, or explain why it is not there."""

    artifact = read_artifact(run_results_path)
    matches = [
        result
        for result in artifact.get("results", ())
        if result.get("unique_id") == unique_id
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"{run_results_path} contains {len(matches)} results for {unique_id!r}, expected one"
        )
    return matches[0]


def assert_nodes_ran(
    run_results_path: Path, *, after: str, expected_nodes: list[str]
) -> int:
    """Require the complete phase selection and successful execution of every node."""

    artifact = read_artifact(run_results_path)
    results = artifact.get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError(
            f"{after!r} selected no dbt nodes, so the phase asserted nothing"
        )
    if not expected_nodes or len(set(expected_nodes)) != len(expected_nodes):
        raise RuntimeError(f"{after!r} has an empty or duplicated coverage baseline")
    successful_status = {
        "model": "success",
        "seed": "success",
        "snapshot": "success",
        "test": "pass",
        "unit_test": "pass",
        # dbt-core 2.0's sources.json uses title case, unlike run_results.json.
        "source": "Pass",
    }
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
        expected_status = successful_status.get(unique_id.split(".", 1)[0])
        if unique_id == EPHEMERAL_UNIQUE_ID:
            expected_status = "skipped"
        if expected_status is None or result.get("status") != expected_status:
            raise RuntimeError(
                f"{after!r}: {unique_id} status is {result.get('status')!r}, "
                f"expected {expected_status!r}"
            )
    expected = set(expected_nodes)
    if observed != expected:
        raise RuntimeError(
            f"{after!r} coverage changed; missing: {sorted(expected - observed)!r}; "
            f"unexpected: {sorted(observed - expected)!r}"
        )
    return len(observed)


def checked_phase_command(
    *arguments: str,
    target_dir: Path,
    log_dir: Path,
    coverage: str,
    expected_nodes: list[str],
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


def assert_data_test_failure(run_results_path: Path, *, unique_id: str) -> None:
    """A one-row failing test must report a data failure, not a SQL or transport error."""

    result = _node_result(run_results_path, unique_id=unique_id)
    failures = result.get("failures")
    if result.get("status") != "fail" or type(failures) is not int or failures != 1:
        raise RuntimeError(
            f"{unique_id} must report status 'fail' and exactly one failing row; "
            f"got status {result.get('status')!r}, failures {failures!r}"
        )


def _successful_adapter_response(run_results_path: Path, *, unique_id: str) -> object:
    """Return one successful node's response before another dbt phase replaces the artifact."""

    result = _node_result(run_results_path, unique_id=unique_id)
    if result.get("status") != "success":
        raise RuntimeError(
            f"{unique_id} status is {result.get('status')!r}, expected 'success'"
        )
    return result.get("adapter_response")


def assert_canonical_incremental_adapter_response(
    response: object, *, expected_project: str, expected_location: str
) -> None:
    """Pin dbt's canonical driver's known incomplete response for the MERGE wave."""

    if not isinstance(response, dict):
        raise RuntimeError(f"canonical adapter_response is not an object: {response!r}")
    expected_keys = {
        "_message",
        "code",
        "job_id",
        "location",
        "message",
        "project_id",
        "query_id",
    }
    if response.keys() != expected_keys:
        raise RuntimeError(
            "canonical adapter_response fields changed: "
            f"{sorted(response)}, expected {sorted(expected_keys)}"
        )
    expected_values = {
        "_message": "SUCCESS (0.0 rows)",
        "code": "SUCCESS",
        "location": expected_location,
        "message": "SUCCESS (0.0 rows)",
        "project_id": expected_project,
    }
    for key, expected in expected_values.items():
        if response.get(key) != expected or isinstance(response.get(key), bool):
            raise RuntimeError(
                f"canonical adapter_response {key} is {response.get(key)!r}, "
                f"expected {expected!r}"
            )
    query_id = response.get("query_id")
    if (
        not isinstance(query_id, str)
        or not query_id
        or response.get("job_id") != query_id
    ):
        raise RuntimeError(
            "canonical adapter_response query_id/job_id are not equal non-empty strings: "
            f"{query_id!r} / {response.get('job_id')!r}"
        )


def dbt_job_label(value: str) -> str:
    """Sanitize a job label value as dbt-core 2.0 does: lowercase, `[^a-z0-9_-]` to `_`, 63 chars."""

    return re.sub(r"[^a-z0-9_-]", "_", value.lower())[:63]


def expected_job_labels(*, invocation_id: str, unique_id: str) -> dict[str, str]:
    """The job labels dbt-core 2.0 sends with every statement it runs for one node."""

    return {
        "dbt_invocation_id": invocation_id,
        "query_comment": dbt_job_label(f"{QUERY_COMMENT_PREFIX}{unique_id}"),
    }


def run_invocation_id(run_results_path: Path) -> str:
    """The phase's invocation ID, which dbt-core 2.0 also sends as the dbt_invocation_id label."""

    artifact = read_artifact(run_results_path)
    invocation_id = (artifact.get("metadata") or {}).get("invocation_id")
    if not isinstance(invocation_id, str) or not invocation_id:
        raise RuntimeError(
            f"{run_results_path} records no invocation_id: {invocation_id!r}"
        )
    return invocation_id


def assert_job_configuration(
    resource: dict,
    *,
    what: str,
    expected_labels: dict[str, str],
    expected_timeout_ms: int,
) -> None:
    """Assert the labels and timeout of one BigQuery job resource.

    The labels must match exactly, because the driver adds none of its own: an extra label came
    from another statement's options.
    """

    configuration = resource.get("configuration") or {}
    labels = configuration.get("labels") or {}
    if labels != expected_labels:
        raise RuntimeError(
            f"{what} carries the job labels {labels!r}, expected {expected_labels!r}"
        )
    # The REST API encodes the int64 jobTimeoutMs as a string.
    timeout = configuration.get("jobTimeoutMs")
    if str(timeout) != str(expected_timeout_ms):
        raise RuntimeError(
            f"{what} has jobTimeoutMs {timeout!r}, expected {expected_timeout_ms}"
        )


def assert_partition_copy_jobs(
    resources: list[dict],
    *,
    project: str,
    dataset: str,
    table: str,
    partitions: tuple[str, ...],
    expected_labels: dict[str, str],
    expected_timeout_ms: int,
) -> None:
    """Assert that copy_partitions copied exactly the listed partitions with dbt's job options.

    dbt-core 2.0 discards a copy job's ID (UPSTREAM.md UP-86), so the jobs are found by their
    destination. Each copies one partition of dbt's temporary table onto the same partition
    with WRITE_TRUNCATE.
    """

    copied: set[str] = set()
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
        job_id = (resource.get("jobReference") or {}).get("jobId")
        what = f"copy job {job_id} onto {table}${partition}"
        source = copy.get("sourceTable") or {}
        if (
            not partition
            or (source.get("projectId"), source.get("datasetId")) != (project, dataset)
            or not str(source.get("tableId", "")).endswith(f"${partition}")
        ):
            raise RuntimeError(
                f"{what} copies {source!r}, not the same partition of a table in {dataset}"
            )
        if copy.get("writeDisposition") != "WRITE_TRUNCATE":
            raise RuntimeError(
                f"{what} has write disposition {copy.get('writeDisposition')!r}, "
                "expected 'WRITE_TRUNCATE'"
            )
        assert_job_configuration(
            resource,
            what=what,
            expected_labels=expected_labels,
            expected_timeout_ms=expected_timeout_ms,
        )
        copied.add(partition)
    if copied != set(partitions):
        raise RuntimeError(
            f"copy_partitions copied the {table} partitions {sorted(copied)!r}, "
            f"expected {sorted(partitions)!r}"
        )


def assert_incremental_job_options(
    client: bigquery.Client,
    run_results_path: Path,
    *,
    project: str,
    dataset: str,
    location: str,
    phase_started: datetime,
) -> None:
    """Check the job labels and timeouts dbt-core 2.0 sent with the incremental phase's jobs.

    The MERGE takes the profile's timeout; exotic_copied_partitions' copy jobs take the model's
    override. dbt's canonical driver sets neither on copy jobs, so only this driver is checked.
    """

    invocation_id = run_invocation_id(run_results_path)
    response = _successful_adapter_response(
        run_results_path, unique_id=INCREMENTAL_UNIQUE_ID
    )
    job_id = response.get("job_id") if isinstance(response, dict) else None
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(
            f"{INCREMENTAL_UNIQUE_ID} reported no job_id to look up: {response!r}"
        )
    merge = client.get_job(job_id, project=project, location=location)
    assert_job_configuration(
        merge.to_api_repr(),
        what=f"the incremental MERGE job {job_id}",
        expected_labels=expected_job_labels(
            invocation_id=invocation_id, unique_id=INCREMENTAL_UNIQUE_ID
        ),
        expected_timeout_ms=PROFILE_JOB_TIMEOUT_MS,
    )
    # The margin absorbs clock skew between this host and BigQuery.
    jobs = client.list_jobs(
        project=project, min_creation_time=phase_started - timedelta(minutes=1)
    )
    assert_partition_copy_jobs(
        [job.to_api_repr() for job in jobs],
        project=project,
        dataset=dataset,
        table="exotic_copied_partitions",
        partitions=COPIED_PARTITIONS,
        expected_labels=expected_job_labels(
            invocation_id=invocation_id, unique_id=COPIED_PARTITIONS_UNIQUE_ID
        ),
        expected_timeout_ms=COPIED_PARTITIONS_JOB_TIMEOUT_MS,
    )


def build_dbt(output_dir: Path) -> Path:
    LOG.section("Build dbt with this repository's driver")
    command = [str(DBT_BUILD_SCRIPT), "--output-dir", str(output_dir)]
    LOG.command(command)
    stdout = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE).stdout
    return Path(stdout.splitlines()[-1])


def install_dbt(archive: Path, directory: Path) -> Path:
    """Unpack a tools/dbt-build archive, so the suite runs what the archive ships."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    with tarfile.open(archive) as source:
        source.extractall(directory, filter="data")
    (install,) = directory.iterdir()
    return install


def install_canonical_dbt(install: Path, directory: Path) -> Path:
    """Copy dbt away from its library, which it would otherwise prefer to dbt's own driver."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    shutil.copy2(install / "dbt", directory / "dbt")
    return directory


def on_path(directory: Path, env: Mapping[str, str]) -> str:
    return f"{directory}{os.pathsep}{env['PATH']}"


@dataclass(frozen=True)
class BuiltDriver:
    """Where a dbt build gets this repository's driver, and what proves a run used it."""

    library: Path
    resolution: str
    downloaded: bool


def built_driver(install: Path, cache_home: Path, driver_version: str) -> BuiltDriver:
    """Non-release builds load the library in the archive; release builds download theirs."""
    library = install / "lib" / "dbt" / LIBRARY_NAME
    if library.is_file():
        return BuiltDriver(
            library, "BigQuery driver resolved: installed adbc_driver_bigquery", downloaded=False
        )
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
    source = "downloaded" if driver.downloaded else "loaded"
    LOG.note(f"dbt {source} the driver at {driver.library}")


@dataclass(frozen=True)
class RunOperationCase:
    name: str
    sql: str
    arguments: tuple[str, ...] = ()


def run_operation_cases(project: str, dataset: str) -> tuple[RunOperationCase, ...]:
    table = f"`{project}.{dataset}.run_operation_events`"
    return (
        RunOperationCase(
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
""".strip(),
        ),
        RunOperationCase(
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
""".strip(),
        ),
        RunOperationCase(
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
""".strip(),
            (
                "--vars",
                "{run_operation_id: 3, run_operation_marker: context}",
            ),
        ),
        RunOperationCase(
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
""".strip(),
        ),
        RunOperationCase(
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
""".strip(),
        ),
        RunOperationCase(
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
""".strip(),
        ),
    )


def normalized_run_operation_output(
    stdout: str, *, datasets: tuple[str, str]
) -> tuple[tuple[object, ...], ...]:
    """Keep deterministic, semantic dbt JSON events and discard timings/IDs."""

    normalized: list[tuple[object, ...]] = []

    def normalize_text(value: str) -> str:
        for dataset in datasets:
            value = value.replace(dataset, "<dataset>")
        return value.strip()

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
            raise RuntimeError(
                f"run-operation unexpectedly emitted database rows in {name}: {data!r}"
            )
        if name == "MainReportVersion":
            normalized.append(("version", data.get("version")))
        elif name == "MainReportArgs":
            args = data.get("args", {})
            normalized.append(
                (
                    "arguments",
                    args.get("debug"),
                    args.get("log_format"),
                    args.get("quiet"),
                    args.get("write_json"),
                )
            )
        elif name == "ArtifactWritten":
            normalized.append(
                (
                    "artifact",
                    data.get("artifact_type"),
                    Path(data.get("artifact_path", "")).name,
                )
            )
        elif name == "SQLQuery":
            message = info.get("msg", "")
            fields = {}
            for key in ("outcome", "dialect", "desc"):
                match = re.search(rf"^-- {key}: (.+)$", message, re.MULTILINE)
                fields[key] = match.group(1) if match else None
            # Every SQLQuery comment header carries all three. A missing field would read None
            # under both drivers and the comparison would pass vacuously.
            absent = sorted(key for key, value in fields.items() if value is None)
            if absent:
                raise RuntimeError(
                    "dbt's SQLQuery event no longer carries the comment fields the "
                    f"run-operation parity comparison reads ({', '.join(absent)}): {message!r}"
                )
            normalized.append(
                (
                    "sql",
                    normalize_text(data.get("sql", "")),
                    fields["outcome"],
                    fields["dialect"],
                    fields["desc"],
                )
            )
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
        raise RuntimeError(
            f"run-operation output did not contain one successful SQL event: {normalized!r}"
        )
    return tuple(normalized)


def run_operation_state(client: bigquery.Client, project: str, dataset: str) -> dict:
    table = client.get_table(f"{project}.{dataset}.run_operation_events")
    fields = tuple(
        (
            field.name,
            field.field_type,
            field.mode,
            tuple(
                (nested.name, nested.field_type, nested.mode) for nested in field.fields
            ),
        )
        for field in table.schema
    )
    rows = tuple(tuple(row) for row in client.query(f"""
select
  operation_id,
  operation_kind,
  cast(amount as string) as amount,
  context_value,
  to_json_string(attributes) as attributes,
  format_timestamp('%FT%TZ', created_at, 'UTC') as created_at
from `{project}.{dataset}.run_operation_events`
order by operation_id
""").result())
    selected = tuple(tuple(row) for row in client.query(f"""
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
""").result())
    partition = table.time_partitioning
    return {
        "schema": fields,
        "partition": (
            partition.type_ if partition is not None else None,
            partition.field if partition is not None else None,
        ),
        "clustering": tuple(table.clustering_fields or ()),
        "labels": tuple(sorted((table.labels or {}).items())),
        "description": table.description,
        "rows": rows,
        "select": selected,
    }


def assert_expected_run_operation_state(state: dict) -> None:
    expected_schema = (
        ("operation_id", "INTEGER", "REQUIRED", ()),
        ("operation_kind", "STRING", "REQUIRED", ()),
        ("amount", "NUMERIC", "NULLABLE", ()),
        ("context_value", "STRING", "NULLABLE", ()),
        (
            "attributes",
            "RECORD",
            "REPEATED",
            (("source", "STRING", "NULLABLE"), ("score", "INTEGER", "NULLABLE")),
        ),
        ("created_at", "TIMESTAMP", "REQUIRED", ()),
    )
    expected_rows = (
        (
            1,
            "insert",
            "13.75",
            "commented_update",
            '[{"source":"literal","score":1},{"source":"update","score":10}]',
            "2025-01-01T00:00:00Z",
        ),
        (
            2,
            "merge_update",
            "21.25",
            "matched",
            '[{"source":"merge","score":20}]',
            "2025-01-02T00:00:00Z",
        ),
        (
            3,
            "jinja_integration",
            "35",
            "context",
            '[{"source":"macro","score":3}]',
            "2025-01-03T00:00:00Z",
        ),
        (
            4,
            "merge_insert",
            "40",
            "not_matched",
            '[{"source":"merge","score":40}]',
            "2025-01-04T00:00:00Z",
        ),
    )
    expected_select = (
        (
            2,
            "merge_update",
            "21.25",
            "110",
            '[{"source":"merge","score":20}]',
        ),
        (
            3,
            "jinja_integration",
            "35",
            "110",
            '[{"source":"macro","score":3}]',
        ),
        (
            4,
            "merge_insert",
            "40",
            "110",
            '[{"source":"merge","score":40}]',
        ),
    )
    for key, want in (
        ("schema", expected_schema),
        ("rows", expected_rows),
        ("partition", ("DAY", "created_at")),
        ("clustering", ("operation_kind",)),
        (
            "labels",
            (("component", "adbc_integration"), ("entrypoint", "run_operation")),
        ),
        ("description", "Created by dbt run-operation --sql parity coverage"),
        ("select", expected_select),
    ):
        if state[key] != want:
            raise RuntimeError(
                f"unexpected run-operation {key}: {state[key]!r}, expected {want!r}"
            )


def assert_run_operation_parity(
    *,
    cache_home: Path,
    staged_driver: BuiltDriver,
    canonical_dbt: Path,
    target_dir: Path,
    log_dir: Path,
) -> None:
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    location = os.environ["DBT_BIGQUERY_LOCATION"]
    base_dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    datasets = (
        f"{base_dataset}_run_operation_upstream",
        f"{base_dataset}_run_operation_local",
    )
    client = bigquery.Client(project=project, location=location)
    outputs: dict[str, tuple[tuple[tuple[object, ...], ...], ...]] = {}
    states: dict[str, dict] = {}

    for mode, dataset in zip(("upstream", "local"), datasets, strict=True):
        dataset_resource = bigquery.Dataset(f"{project}.{dataset}")
        dataset_resource.location = location
        client.create_dataset(dataset_resource, exists_ok=True)

        env = os.environ.copy()
        env["DBT_BIGQUERY_SCHEMA"] = dataset
        if mode == "upstream":
            env["BIGQUERY_ADBC_USE_DBT_DRIVER"] = "1"
            env["PATH"] = on_path(canonical_dbt, env)
            env["XDG_CACHE_HOME"] = str((PROJECT_DIR / ".cache" / "upstream").resolve())
        else:
            env["XDG_CACHE_HOME"] = str(cache_home)

        mode_target = (target_dir / "run-operation" / mode).resolve()
        mode_log = (log_dir / "run-operation" / mode).resolve()
        mode_target.mkdir(parents=True, exist_ok=True)
        mode_log.mkdir(parents=True, exist_ok=True)
        (mode_log / "dbt.log").unlink(missing_ok=True)
        normalized_outputs = []
        for case in run_operation_cases(project, dataset):
            LOG.section(f"run-operation parity ({mode}): {case.name}")
            stdout = captured_dbt_command(
                "run-operation",
                "--sql",
                case.sql,
                *case.arguments,
                "--debug",
                "--log-format",
                "json",
                target_dir=mode_target,
                log_dir=mode_log,
                env=env,
            )
            normalized = normalized_run_operation_output(stdout, datasets=datasets)
            normalized_outputs.append(normalized)
            LOG.note(f"captured {len(normalized)} deterministic dbt output events")

        if mode == "local":
            assert_local_driver_resolution(mode_log, staged_driver)
        outputs[mode] = tuple(normalized_outputs)
        states[mode] = run_operation_state(client, project, dataset)
        assert_expected_run_operation_state(states[mode])
        LOG.note(f"BigQuery metadata, rows and SELECT result are correct for {mode}")

    if outputs["upstream"] != outputs["local"]:
        raise RuntimeError(
            "normalized run-operation output differs between drivers: "
            f"upstream={outputs['upstream']!r}, local={outputs['local']!r}"
        )
    if states["upstream"] != states["local"]:
        raise RuntimeError(
            "run-operation BigQuery state differs between drivers: "
            f"upstream={states['upstream']!r}, local={states['local']!r}"
        )

    for dataset in datasets:
        client.delete_dataset(
            f"{project}.{dataset}", delete_contents=True, not_found_ok=True
        )
    LOG.note("canonical and local run-operation output and side effects are identical")


def assert_table_layout(
    table: bigquery.Table,
    *,
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> None:
    actual_partition = (
        table.time_partitioning.field if table.time_partitioning is not None else None
    )
    if actual_partition != partition_field:
        raise RuntimeError(
            f"unexpected {table.table_id} partition: {actual_partition!r}"
        )
    if table.clustering_fields != clustering_fields:
        raise RuntimeError(
            f"unexpected {table.table_id} clustering: {table.clustering_fields!r}"
        )


def assert_field_types(table: bigquery.Table, expected: dict[str, str]) -> None:
    actual = {field.name: field.field_type for field in table.schema}
    if actual != expected:
        raise RuntimeError(
            f"unexpected {table.table_id} field types: {actual!r}, expected {expected!r}"
        )


def assert_table_description(table: bigquery.Table, expected: str, *, why: str) -> None:
    if table.description != expected:
        raise RuntimeError(
            f"unexpected {table.table_id} description ({why}): "
            f"{table.description!r}, expected {expected!r}"
        )


def assert_column_description(
    table: bigquery.Table, column: str, expected: str
) -> None:
    field = next((field for field in table.schema if field.name == column), None)
    if field is None:
        raise RuntimeError(f"{table.table_id} has no {column} column")
    if field.description != expected:
        raise RuntimeError(
            f"unexpected {table.table_id}.{column} description: "
            f"{field.description!r}, expected {expected!r}"
        )


def assert_nested_persist_docs(client: bigquery.Client) -> None:
    """Check top-level descriptions and the known loss of nested descriptions.

    dbt-core 2.0's do_nest_column_data_types drops nested paths before sending
    update_columns_descriptions (docs/UPSTREAM.md UP-77). If nested descriptions
    arrive, update the expectations and the corresponding KNOWN_ISSUES.md entry.
    """

    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    descriptions = {
        row.field_path: row.description
        for row in client.query(
            f"""
select field_path, description
from `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
where table_name = 'known_issues_contract_nested_types'
"""
        ).result()
    }
    if not descriptions:
        raise RuntimeError(
            "known_issues_contract_nested_types reported no column field paths, so persist_docs "
            "could not be checked"
        )
    for path, want in sorted(NESTED_TOP_LEVEL_DESCRIPTIONS.items()):
        got = descriptions.get(path)
        if got != want:
            raise RuntimeError(
                f"unexpected known_issues_contract_nested_types.{path} description: "
                f"{got!r}, expected {want!r}"
            )
    landed = {
        path: descriptions.get(path)
        for path in sorted(NESTED_FIELD_DESCRIPTIONS)
        if descriptions.get(path)
    }
    if landed:
        raise RuntimeError(
            "dbt-core 2.0 delivered nested persist_docs descriptions, which UPSTREAM.md UP-77 "
            f"says it cannot: {landed!r}. Upstream has fixed it; assert "
            "NESTED_FIELD_DESCRIPTIONS here and remove the KNOWN_ISSUES.md entry."
        )


def assert_table_descriptions(
    client: bigquery.Client, expected: dict[str, str | None], *, after: str
) -> None:
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
        got = table.description or None
        if got != want:
            raise RuntimeError(
                f"unexpected {identifier} description after {after!r}: {got!r}, expected {want!r}"
            )


def assert_authorized_view(
    access_entries: list[bigquery.AccessEntry],
    *,
    project: str,
    dataset: str,
    view: str,
) -> None:
    """Assert the source dataset authorizes the view exactly once, keeping its other grants.

    dbt reapplies grant_access_to after every build of the view. An unconditional append
    would duplicate the entry; replacing the access list would drop the owner and readers.
    """

    views = [entry for entry in access_entries if entry.entity_type == "view"]
    matches = [
        entry
        for entry in views
        if isinstance(entry.entity_id, dict)
        and (
            entry.entity_id.get("projectId"),
            entry.entity_id.get("datasetId"),
            entry.entity_id.get("tableId"),
        )
        == (project, dataset, view)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one authorized-view entry for {project}.{dataset}.{view}, "
            f"found {len(matches)} among {[entry.entity_id for entry in views]!r}"
        )
    if not any(entry.entity_type != "view" for entry in access_entries):
        raise RuntimeError(
            "authorizing the view replaced the dataset's other access entries instead of "
            f"extending them: {[entry.entity_type for entry in access_entries]!r}"
        )


def assert_python_model_upload(
    content: str, *, project: str, dataset: str, alias: str
) -> None:
    """Check that write_gcs uploaded the model body and dbt's generated context.

    dbt appends py_script_postfix during compilation, so the uploaded object must
    include the generated relation identifier and resolved ref as well as the source.
    """

    required = {
        "the model's own marker": PYTHON_MODEL_MARKER,
        "the model function": "def model(dbt, session):",
        "the model body": 'orders.groupBy("customer_id").sum("order_total")',
        "dbt-core 2.0's generated ref function": "def ref(*args, **kwargs):",
        "dbt-core 2.0's generated dbt object": "class dbtObj:",
        "the resolved relation identifier": f'identifier = "{alias}"',
        "the resolved relation dataset": f'schema = "{dataset}"',
        "the resolved relation project": f'database = "{project}"',
        "the resolved upstream relation": f"{project}.{dataset}.fct_orders",
    }
    missing = sorted(name for name, text in required.items() if text not in content)
    if missing:
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
    status = result.get("status")
    if status != "error":
        raise RuntimeError(
            f"{unique_id} status is {status!r}, expected 'error' from the absent cluster"
        )
    message = result.get("message")
    if not isinstance(message, str):
        raise RuntimeError(f"{unique_id} reported no error message: {message!r}")
    rejected = sorted(
        marker for marker in OPTION_REJECTION_MARKERS if marker in message
    )
    if rejected:
        raise RuntimeError(
            f"{unique_id} failed on a python-model option the driver does not implement "
            f"({', '.join(rejected)}) rather than at the Dataproc submission: {message!r}"
        )
    if DATAPROC_SUBMISSION_ERROR not in message:
        raise RuntimeError(
            f"{unique_id} did not fail at the Dataproc job submission: {message!r}"
        )


def storage_session(quota_project: str) -> AuthorizedSession:
    """An authorized Cloud Storage JSON API session billed to the integration project.

    Cloud Storage bills the credentials' quota project, not the bucket's project, and the
    ADC file may name an unrelated one. The dbt profile pins the driver's quota project too.
    """

    credentials, _ = google.auth.default()
    return AuthorizedSession(credentials.with_quota_project(quota_project))


def storage_object_url(bucket: str, object_name: str) -> str:
    return (
        "https://storage.googleapis.com/storage/v1/b/"
        f"{quote(bucket, safe='')}/o/{quote(object_name, safe='')}"
    )


def read_storage_object(
    session: AuthorizedSession, *, bucket: str, object_name: str
) -> str:
    response = session.get(
        storage_object_url(bucket, object_name), params={"alt": "media"}
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"gs://{bucket}/{object_name} was not readable: HTTP {response.status_code} "
            f"{response.text[:400]}"
        )
    return response.content.decode("utf-8")


def delete_storage_object(
    session: AuthorizedSession, *, bucket: str, object_name: str
) -> None:
    response = session.delete(storage_object_url(bucket, object_name))
    if response.status_code not in (200, 204, 404):
        raise RuntimeError(
            f"gs://{bucket}/{object_name} was not deletable: HTTP {response.status_code} "
            f"{response.text[:400]}"
        )


def assert_python_model(*, target_dir: Path, log_dir: Path) -> None:
    """Build the python model and check both statements it runs through the driver.

    dbt-core 2.0 uploads the compiled source with write_gcs, then submits it to Dataproc. The
    bucket is real and the cluster is not, so the upload must leave {schema}/{alias}.py and
    the submission must fail inside Dataproc.
    """

    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    bucket = os.environ["DBT_BIGQUERY_GCS_BUCKET"]
    cluster = os.environ["DBT_BIGQUERY_DATAPROC_CLUSTER"]
    object_name = f"{dataset}/{PYTHON_MODEL_ALIAS}.py"

    with storage_session(project) as session:
        delete_storage_object(session, bucket=bucket, object_name=object_name)
        try:
            failing_dbt_command(
                "build",
                "--select",
                PYTHON_MODEL_ALIAS,
                target_dir=target_dir,
                log_dir=log_dir,
            )
            assert_python_model_submission(
                target_dir / "run_results.json", unique_id=PYTHON_MODEL_UNIQUE_ID
            )
            LOG.note(
                f"dbt reported the {cluster} Dataproc job submission failing, so the driver "
                "accepted every python-model option and reached the submission"
            )
            assert_python_model_upload(
                read_storage_object(session, bucket=bucket, object_name=object_name),
                project=project,
                dataset=dataset,
                alias=PYTHON_MODEL_ALIAS,
            )
            LOG.note(
                f"gs://{bucket}/{object_name} holds the compiled python for {PYTHON_MODEL_ALIAS}"
            )
        finally:
            delete_storage_object(session, bucket=bucket, object_name=object_name)
    LOG.note(f"removed gs://{bucket}/{object_name}")


def assert_bigquery_state(
    *, expected_mv_refresh_minutes: int, local_driver: bool
) -> None:
    project = os.environ["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"]
    dataset = os.environ["DBT_BIGQUERY_SCHEMA"]
    client = bigquery.Client(
        project=project, location=os.environ["DBT_BIGQUERY_LOCATION"]
    )

    def table(identifier: str) -> bigquery.Table:
        return client.get_table(f"{project}.{dataset}.{identifier}")

    fct_orders = table("fct_orders")
    assert_table_layout(
        fct_orders,
        partition_field="order_date",
        clustering_fields=["status", "customer_id"],
    )
    expected_labels = {
        "component": "adbc_integration",
        "materialization": "table",
    }
    if not expected_labels.items() <= (fct_orders.labels or {}).items():
        raise RuntimeError(f"unexpected fct_orders labels: {fct_orders.labels!r}")

    # No DDL in this project sets this text; only the option-only update can have persisted it.
    assert_table_description(
        fct_orders,
        TABLE_DESCRIPTION_SENTINEL,
        why="set by the update_table_description run-operation",
    )

    line_items = next(
        (field for field in fct_orders.schema if field.name == "line_items"), None
    )
    if line_items is None:
        raise RuntimeError("fct_orders has no line_items field")
    if line_items.field_type != "RECORD" or line_items.mode != "REPEATED":
        raise RuntimeError(f"unexpected line_items field: {line_items!r}")
    if [field.name for field in line_items.fields] != [
        "line_number",
        "sku",
        "quantity",
        "net_amount",
    ]:
        raise RuntimeError(f"unexpected line_items struct: {line_items.fields!r}")

    assert_table_layout(table("daily_revenue"), partition_field="order_date")
    assert_table_layout(table("dim_customers"), clustering_fields=["region"])
    incremental_order_totals = table("incremental_order_totals")
    assert_table_layout(
        incremental_order_totals,
        partition_field="order_date",
        clustering_fields=["customer_id"],
    )
    # The second wave merges into the existing relation, so persist_docs must use the
    # description update rather than DDL.
    assert_table_description(
        incremental_order_totals,
        INCREMENTAL_DESCRIPTION,
        why="persist_docs on an incremental rebuild",
    )
    assert_column_description(
        incremental_order_totals, "order_id", INCREMENTAL_COLUMN_DESCRIPTION
    )
    assert_nested_persist_docs(client)

    customers = table("customers")
    assert_field_types(
        customers,
        {
            "customer_id": "INTEGER",
            "customer_name": "STRING",
            "region": "STRING",
            "signup_date": "DATE",
            "is_active": "BOOLEAN",
        },
    )
    # The seed materialization calls update_table_description directly once the rows are in.
    assert_table_description(customers, SEED_DESCRIPTION, why="persist_docs on a seed")
    assert_field_types(
        table("orders"),
        {
            "order_id": "INTEGER",
            "customer_id": "INTEGER",
            "ordered_at": "TIMESTAMP",
            "status": "STRING",
            "shipping_amount": "NUMERIC",
        },
    )
    assert_field_types(
        table("order_items"),
        {
            "order_id": "INTEGER",
            "line_number": "INTEGER",
            "sku": "STRING",
            "quantity": "INTEGER",
            "unit_price": "NUMERIC",
            "discount_amount": "NUMERIC",
        },
    )

    assert_materialized_view_state(
        table("exotic_materialized_view"),
        expected_refresh_minutes=expected_mv_refresh_minutes,
    )
    assert_materialized_view_state(table(REFRESHED_VIEW), expected_refresh_minutes=90)
    assert_range_partitioning(table("exotic_range_partitioned"))
    # copy_partitions replaces partitions with table copies, which must keep the partitioning.
    assert_table_layout(table("exotic_copied_partitions"), partition_field="order_date")

    wide_types = table("exotic_wide_types")
    assert_field_types(
        wide_types,
        {
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
    )
    if local_driver:
        # dbt's bundled Go drivers drop decimal precision and scale from CSV loads, so under
        # --upstream-adbc `numeric(20,4)` reads back as (None, None).
        assert_decimal_parameters(
            wide_types, {"precise_amount": (20, 4), "huge_amount": (None, None)}
        )
    # A plain `numeric` declaration loads as a parameter-free NUMERIC under both drivers.
    assert_decimal_parameters(table("orders"), {"shipping_amount": (None, None)})
    assert_field_types(table("exotic_empty"), {"row_id": "INTEGER", "label": "STRING"})
    # The seed's column_types reach the driver as BIGQUERY:type metadata on string fields.
    assert_field_types(
        table("exotic_encoded_types"),
        {
            "row_id": "INTEGER",
            "payload": "BYTES",
            "document": "JSON",
            "location": "GEOGRAPHY",
        },
    )
    assert_field_types(
        table("exotic_csv_edges"),
        {
            "row_id": "INTEGER",
            "label": "STRING",
            "optional_label": "STRING",
            "literal_null": "STRING",
            "signed_value": "INTEGER",
            "percent_ratio": "FLOAT",
            "event_ts": "TIMESTAMP",
        },
    )

    evolved = table("exotic_schema_evolution")
    evolved_columns = [field.name for field in evolved.schema]
    if "doubled_total" not in evolved_columns:
        raise RuntimeError(
            f"on_schema_change=append_new_columns did not add doubled_total: {evolved_columns!r}"
        )

    synchronized = table("exotic_schema_sync")
    synchronized_columns = [field.name for field in synchronized.schema]
    expected_synchronized_columns = [
        "customer_id",
        "order_id",
        "order_date",
        "load_wave",
        "wave_payload",
        "original_wave",
        "current_details",
    ]
    if synchronized_columns != expected_synchronized_columns:
        raise RuntimeError(
            "on_schema_change=sync_all_columns did not replace retired_column with "
            f"current_details: {synchronized_columns!r}"
        )

    quoted = table("select")
    assert_field_types(
        quoted,
        {
            "order": "INTEGER",
            "Mixed Case": "STRING",
            "dash-name": "STRING",
            "名前-列": "STRING",
        },
    )
    assert_column_description(
        quoted,
        "名前-列",
        "Unicode flexible column, persisted through the metadata update.",
    )

    # Keep each aggregate beside its expected value so additions cannot lose an assertion.
    checks = {
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
            "exotic_encoded_types", "countif(payload is null and document is null and location is null)", 1,
        ),
        # The second wave replaces two of three partitions.
        "copied_days": ("exotic_copied_partitions", "count(*)", 3),
        "copied_wave2": ("exotic_copied_partitions", "countif(load_wave = 2)", 2),
        "copied_untouched": (
            "exotic_copied_partitions", "countif(load_wave = 1 and order_date = date '2026-01-12')", 1,
        ),
        "csv_edge_rows": ("exotic_csv_edges", "count(*)", 3),
        "csv_edge_nulls": ("exotic_csv_edges", "countif(optional_label is null)", 1),
        "csv_edge_empty_strings": ("exotic_csv_edges", "countif(label = '')", 1),
        "result_edge_rows": ("exotic_result_edges", "count(*)", 1),
        "empty_result_rows": ("exotic_empty_result", "count(*)", 0),
        "quoted_identifier_rows": ("select", "count(*)", 1),
        "synchronized_rows": ("exotic_schema_sync", "count(*)", 6),
        "synchronized_updates": ("exotic_schema_sync", "countif(load_wave = 2 and original_wave = 1)", 2),
    }
    query = "select " + ", ".join(
        f"(select {aggregate} from `{project}.{dataset}.{table}`) as {name}"
        for name, (table, aggregate, _) in checks.items()
    )
    counts = dict(next(iter(client.query(query).result())).items())
    expected = {name: count for name, (_, _, count) in checks.items()}
    if counts != expected:
        differences = {
            key: (counts.get(key), expected.get(key))
            for key in sorted(counts.keys() | expected.keys())
            if counts.get(key) != expected.get(key)
        }
        raise RuntimeError(
            f"unexpected warehouse row counts (actual, expected): {differences!r}"
        )
    source_dataset = client.get_dataset(
        f"{project}.{dataset}{AUTHORIZED_SOURCE_SUFFIX}"
    )
    assert_authorized_view(
        list(source_dataset.access_entries),
        project=project,
        dataset=dataset,
        view="exotic_authorized_view",
    )
    LOG.note(
        f"{source_dataset.dataset_id} authorizes exotic_authorized_view exactly once "
        "after two builds of the view"
    )
    authorized_view = table("exotic_authorized_view")
    for column, description in AUTHORIZED_VIEW_COLUMN_DESCRIPTIONS.items():
        assert_column_description(authorized_view, column, description)
    LOG.note("persist_docs patched the column descriptions onto the rebuilt view's schema")

    LOG.note(
        "table descriptions persisted through the option-only update for the "
        "run-operation, the incremental rebuild and the seed"
    )

    LOG.note("BigQuery REST metadata and row-count assertions passed")


def assert_materialized_view_state(
    table: bigquery.Table, *, expected_refresh_minutes: int
) -> None:
    """Assert the MaterializedView metadata dbt reads back through GetTableSchema."""

    view = table.table_id
    if table.table_type != "MATERIALIZED_VIEW":
        raise RuntimeError(
            f"{view} table_type is {table.table_type!r}, expected 'MATERIALIZED_VIEW'"
        )
    if table.mview_enable_refresh is not True:
        raise RuntimeError(
            f"{view} enable_refresh is {table.mview_enable_refresh!r}, expected True"
        )
    interval = table.mview_refresh_interval
    if interval is None or interval.total_seconds() != expected_refresh_minutes * 60:
        raise RuntimeError(
            f"{view} refresh interval is {interval!r}, "
            f"expected {expected_refresh_minutes} minutes"
        )
    if table.clustering_fields != ["status"]:
        raise RuntimeError(
            f"{view} clustering is {table.clustering_fields!r}, expected ['status']"
        )
    LOG.note(
        f"{view} reports enable_refresh, a "
        f"{expected_refresh_minutes}-minute refresh interval and clustering"
    )


def assert_materialized_view_refresh_query(
    query: str, *, project: str, dataset: str, view: str
) -> None:
    """Require the job dbt reported for a materialized view to be its refresh call."""

    call = f"call bq.refresh_materialized_view('{project}.{dataset}.{view}')"
    if call not in query:
        raise RuntimeError(
            f"dbt reported a job that does not refresh {view}; describe_relation read back a "
            f"configuration change, so dbt altered or replaced the view: {query[-400:]!r}"
        )


def assert_materialized_view_refresh(
    client: bigquery.Client,
    run_results_path: Path,
    *,
    project: str,
    dataset: str,
    location: str,
) -> None:
    """Require dbt to refresh an unchanged materialized view rather than alter or replace it.

    dbt refreshes only when the configuration it reads back through the driver's table metadata
    matches the model's, so this checks that metadata end to end. dbt's canonical driver reports
    a zero expiration time, which dbt reads as a change, so only this driver is checked.
    """

    response = _successful_adapter_response(
        run_results_path, unique_id=REFRESHED_VIEW_UNIQUE_ID
    )
    if not isinstance(response, dict) or response.get("code") != "SCRIPT":
        raise RuntimeError(
            f"{REFRESHED_VIEW} reported a statement other than the SCRIPT of its refresh call; "
            "describe_relation read back a configuration change, so dbt altered or replaced "
            f"the view: {response!r}"
        )
    assert_run_result_adapter_response(
        run_results_path,
        unique_id=REFRESHED_VIEW_UNIQUE_ID,
        expected_code="SCRIPT",
        expected_rows_affected=None,
        expected_project=project,
        expected_location=location,
    )
    job = client.get_job(response["job_id"], project=project, location=location)
    assert_materialized_view_refresh_query(
        job.query or "", project=project, dataset=dataset, view=REFRESHED_VIEW
    )


def assert_range_partitioning(table: bigquery.Table) -> None:
    """Assert the RangePartitioning metadata that only an integer-range table produces."""

    if table.time_partitioning is not None:
        raise RuntimeError(
            f"exotic_range_partitioned is time partitioned: {table.time_partitioning!r}"
        )
    partitioning = table.range_partitioning
    if partitioning is None or partitioning.field != "bucket":
        raise RuntimeError(f"unexpected range partitioning: {partitioning!r}")
    bounds = partitioning.range_
    if (bounds.start, bounds.end, bounds.interval) != (0, 100, 10):
        raise RuntimeError(
            f"range partition bounds are {(bounds.start, bounds.end, bounds.interval)!r}, "
            "expected (0, 100, 10)"
        )
    if table.clustering_fields != ["parity"]:
        raise RuntimeError(
            f"range-partitioned clustering is {table.clustering_fields!r}, expected ['parity']"
        )
    if table.expires is None:
        raise RuntimeError("hours_to_expiration did not set an expiration time")
    LOG.note(
        "integer-range partitioning, clustering and table expiration round-tripped"
    )


def assert_decimal_parameters(
    table: bigquery.Table, expected: dict[str, tuple[int | None, int | None]]
) -> None:
    """Assert that parameterized decimal precision and scale survived CSV ingestion."""

    fields = {field.name: field for field in table.schema}
    for name, (precision, scale) in expected.items():
        field = fields.get(name)
        if field is None:
            raise RuntimeError(f"{table.table_id} has no {name} field")
        actual = (
            None if field.precision is None else int(field.precision),
            None if field.scale is None else int(field.scale),
        )
        if actual != (precision, scale):
            raise RuntimeError(
                f"{table.table_id}.{name} precision/scale is {actual!r}, "
                f"expected {(precision, scale)!r}"
            )


def assert_catalog_artifact(
    catalog_path: Path, *, expected_nodes: tuple[str, ...]
) -> None:
    """Assert dbt's catalog.json describes the models the INFORMATION_SCHEMA reads covered."""

    catalog = read_artifact(catalog_path)

    nodes = catalog.get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        raise RuntimeError(f"{catalog_path} has no catalog nodes")
    for unique_id in expected_nodes:
        entry = nodes.get(unique_id)
        if entry is None:
            raise RuntimeError(f"{catalog_path} has no catalog entry for {unique_id}")
        columns = entry.get("columns")
        if not isinstance(columns, dict) or not columns:
            raise RuntimeError(f"{unique_id} has no catalogued columns")

    fct_orders = nodes["model.bigquery_adbc_integration.fct_orders"]["columns"]
    if "order_id" not in {name.split(".")[0] for name in fct_orders}:
        raise RuntimeError(
            f"fct_orders catalog columns lack order_id: {sorted(fct_orders)!r}"
        )
    LOG.note(f"catalog.json describes {len(nodes)} nodes with column metadata")


def parse_dbt_show_rows(stdout: str) -> list[dict]:
    """Extract the single JSON row array emitted by `dbt show --quiet --output json`."""

    candidates: list[list[dict]] = []
    for line in stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, list) and all(isinstance(row, dict) for row in value):
            candidates.append(value)
    if len(candidates) != 1:
        raise RuntimeError(
            f"dbt show emitted {len(candidates)} JSON row arrays, expected one: {stdout[-1_000:]!r}"
        )
    return candidates[0]


def capture_show_results(*, target_dir: Path, log_dir: Path) -> dict[str, list[dict]]:
    """Capture result values that dbt fetched through the selected ADBC driver."""

    results: dict[str, list[dict]] = {}
    env = os.environ.copy()
    for model in SHOW_MODELS:
        stdout = captured_dbt_command(
            "show",
            "--select",
            model,
            "--output",
            "json",
            "--quiet",
            target_dir=target_dir,
            log_dir=log_dir,
            env=env,
        )
        results[model] = parse_dbt_show_rows(stdout)
        LOG.note(f"captured {len(results[model])} ADBC rows from {model}")
    output = target_dir / "show_results.json"
    output.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return results


def _normalized_schema(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _normalized_schema(child)
            for key, child in value.items()
            if key not in ("precision", "scale")
        }
    if isinstance(value, list):
        return [_normalized_schema(child) for child in value]
    return value


def _decimal_parameters(
    fields: Sequence[bigquery.SchemaField], *, prefix: str = ""
) -> dict[str, tuple[int | None, int | None]]:
    parameters: dict[str, tuple[int | None, int | None]] = {}
    for field in fields:
        path = f"{prefix}.{field.name}" if prefix else field.name
        if field.field_type.upper() in ("NUMERIC", "BIGNUMERIC"):
            parameters[path] = (field.precision, field.scale)
        parameters.update(_decimal_parameters(field.fields, prefix=path))
    return parameters


def _relation_rows(
    client: bigquery.Client,
    table: bigquery.Table,
    *,
    scope: str,
    location: str,
) -> tuple[str, ...]:
    escaped_table = table.table_id.replace("`", "``")
    path = f"`{table.project}.{table.dataset_id}.{escaped_table}`"
    # loaded_at uses current_timestamp(), so it differs across sequential driver runs.
    excluded = (
        {"loaded_at"}
        if scope == "main" and table.table_id == "incremental_order_totals"
        else set()
    )
    if excluded:
        included = [field.name for field in table.schema if field.name not in excluded]
        projection = ", ".join(f"t.`{name.replace('`', '``')}`" for name in included)
        row_value = f"(select as struct {projection})"
    else:
        row_value = "t"
    query = f"select to_json_string({row_value}) as row_json from {path} as t order by row_json"
    return tuple(
        row.row_json for row in client.query(query, location=location).result()
    )


def warehouse_snapshot(
    client: bigquery.Client,
    *,
    project: str,
    main_dataset: str,
    location: str,
) -> dict[tuple[str, str], dict]:
    """Collect stable metadata and the complete row multiset from both suite datasets."""

    authorized_dataset = f"{main_dataset}{AUTHORIZED_SOURCE_SUFFIX}"

    def normalized(value: object) -> object:
        if isinstance(value, dict):
            return {key: normalized(child) for key, child in value.items()}
        if isinstance(value, list):
            return [normalized(child) for child in value]
        if isinstance(value, str):
            return value.replace(authorized_dataset, "<authorized_source>").replace(main_dataset, "<main>")
        return value

    snapshot: dict[tuple[str, str], dict] = {}
    for scope, dataset in (
        ("main", main_dataset),
        ("authorized_source", authorized_dataset),
    ):
        for item in client.list_tables(f"{project}.{dataset}"):
            table = client.get_table(item.reference)
            resource = table.to_api_repr()
            stable_metadata = {
                "table_type": table.table_type,
                "description": table.description,
                "friendly_name": table.friendly_name,
                "labels": dict(sorted((table.labels or {}).items())),
                "expires": table.expires is not None,
                "time_partitioning": resource.get("timePartitioning"),
                "range_partitioning": resource.get("rangePartitioning"),
                "clustering": resource.get("clustering"),
                "require_partition_filter": table.require_partition_filter,
                "table_constraints": normalized(resource.get("tableConstraints")),
                "view_query": normalized(table.view_query),
                "view_use_legacy_sql": table.view_use_legacy_sql,
                "materialized_view_query": normalized(table.mview_query),
                "materialized_view_enable_refresh": table.mview_enable_refresh,
                "materialized_view_refresh_seconds": (
                    table.mview_refresh_interval.total_seconds()
                    if table.mview_refresh_interval is not None
                    else None
                ),
                "materialized_view_allow_non_incremental": (
                    table.mview_allow_non_incremental_definition
                ),
                "max_staleness": table.max_staleness,
            }
            raw_schema = [field.to_api_repr() for field in table.schema]
            snapshot[(scope, table.table_id)] = {
                "metadata": stable_metadata,
                "schema": _normalized_schema(raw_schema),
                "decimal_parameters": _decimal_parameters(table.schema),
                "rows": _relation_rows(client, table, scope=scope, location=location),
            }
    return snapshot


def compare_warehouse_snapshots(
    upstream: dict[tuple[str, str], dict], local: dict[tuple[str, str], dict]
) -> tuple[str, ...]:
    """Require identical semantics except for the documented seed decimal parameters."""

    if upstream.keys() != local.keys():
        missing = sorted(local.keys() - upstream.keys())
        extra = sorted(upstream.keys() - local.keys())
        raise RuntimeError(
            f"warehouse relations differ: missing upstream={missing!r}, missing local={extra!r}"
        )

    actual_parameter_differences = {}
    for relation in sorted(upstream):
        upstream_relation = upstream[relation]
        local_relation = local[relation]
        for section in ("metadata", "schema", "rows"):
            if upstream_relation[section] != local_relation[section]:
                raise RuntimeError(
                    f"warehouse {section} differs for {relation}: "
                    f"upstream={upstream_relation[section]!r}, "
                    f"local={local_relation[section]!r}"
                )
        upstream_parameters = upstream_relation["decimal_parameters"]
        local_parameters = local_relation["decimal_parameters"]
        for field in sorted(upstream_parameters.keys() | local_parameters.keys()):
            values = (
                upstream_parameters.get(field),
                local_parameters.get(field),
            )
            if values[0] != values[1]:
                actual_parameter_differences[(*relation, field)] = values

    if actual_parameter_differences != EXPECTED_DECIMAL_PARAMETER_DIFFERENCES:
        raise RuntimeError(
            "warehouse decimal-parameter differences changed: "
            f"actual={actual_parameter_differences!r}, "
            f"expected={EXPECTED_DECIMAL_PARAMETER_DIFFERENCES!r}"
        )
    return tuple(
        f"{scope}.{relation}.{field}: upstream={upstream_value}, local={local_value}"
        for (scope, relation, field), (upstream_value, local_value) in sorted(
            actual_parameter_differences.items()
        )
    )


def _catalog_type_without_decimal_parameters(type_name: object) -> object:
    if not isinstance(type_name, str):
        return type_name
    return re.sub(r"\b(BIGNUMERIC|NUMERIC)\(\d+,\s*\d+\)", r"\1", type_name)


def semantic_catalog(catalog_path: Path) -> dict:
    """Keep catalog fields whose values should agree across driver implementations."""

    catalog = read_artifact(catalog_path)
    result = {}
    for section in ("nodes", "sources"):
        entries = catalog.get(section)
        if not isinstance(entries, dict):
            raise RuntimeError(f"{catalog_path} has no {section} map")
        normalized_entries = {}
        for unique_id, entry in entries.items():
            metadata = entry.get("metadata", {})
            columns = entry.get("columns", {})
            stats = entry.get("stats", {})
            normalized_entries[unique_id] = {
                "metadata": {
                    key: metadata.get(key)
                    for key in ("type", "name", "database", "comment", "owner")
                },
                "columns": {
                    name: {
                        "type": _catalog_type_without_decimal_parameters(
                            column.get("type")
                        ),
                        "index": column.get("index"),
                        "name": column.get("name"),
                        "comment": column.get("comment"),
                    }
                    for name, column in columns.items()
                },
                "stats": {
                    name: stat.get("value")
                    for name, stat in stats.items()
                    if name not in ("bytes", "num_rows")
                },
            }
        result[section] = normalized_entries
    return result


def assert_full_project_parity(
    client: bigquery.Client,
    *,
    project: str,
    location: str,
    datasets: dict[str, str],
) -> tuple[str, ...]:
    """Compare complete warehouse rows/metadata, dbt show values and catalog metadata."""

    show_results = {
        mode: json.loads(
            (PROJECT_DIR / "target" / mode / "show_results.json").read_text()
        )
        for mode in ("upstream", "local")
    }
    if show_results["upstream"] != show_results["local"]:
        raise RuntimeError(
            "dbt show results differ between drivers: "
            f"upstream={show_results['upstream']!r}, local={show_results['local']!r}"
        )

    catalogs = {
        mode: semantic_catalog(PROJECT_DIR / "target" / mode / "catalog.json")
        for mode in ("upstream", "local")
    }
    if catalogs["upstream"] != catalogs["local"]:
        raise RuntimeError("semantic catalog metadata differs between drivers")

    snapshots = {
        mode: warehouse_snapshot(
            client,
            project=project,
            main_dataset=datasets[mode],
            location=location,
        )
        for mode in ("upstream", "local")
    }
    return compare_warehouse_snapshots(snapshots["upstream"], snapshots["local"])


def _validate_integration_schema(schema: str) -> None:
    if (
        not schema.startswith("bigquery_adbc_integration_")
        or not schema.replace("_", "").isalnum()
    ):
        raise RuntimeError(f"refusing unsafe integration schema name: {schema}")


def compare_drivers(*, keep_datasets: bool, dbt_archive: Path | None) -> int:
    """Run the complete project twice and compare its stable observable results."""

    project = require_project()
    location = os.environ.get("DBT_BIGQUERY_LOCATION", "US")
    configured_base = os.environ.get(
        "DBT_BIGQUERY_SCHEMA", "bigquery_adbc_integration_compare"
    )
    _validate_integration_schema(configured_base)
    token = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    base = f"{configured_base}_{token}_{os.getpid()}"
    datasets = {mode: f"{base}_{mode}" for mode in ("upstream", "local")}
    for dataset in datasets.values():
        _validate_integration_schema(dataset)

    client = bigquery.Client(project=project, location=location)
    targets = [
        dataset
        for main_dataset in datasets.values()
        for dataset in (main_dataset, f"{main_dataset}{AUTHORIZED_SOURCE_SUFFIX}")
    ]
    for dataset in targets:
        try:
            client.get_dataset(f"{project}.{dataset}")
        except NotFound:
            continue
        raise RuntimeError(
            f"refusing to reuse existing comparison dataset {project}.{dataset}"
        )

    # Both runs must use one build, and only the first would otherwise pay for it.
    archive = dbt_archive or build_dbt(PROJECT_DIR / ".cache" / "dbt-build")

    LOG.section("Run the complete dbt project with both BigQuery ADBC drivers")
    failures = []
    for mode in ("upstream", "local"):
        env = os.environ.copy()
        env["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"] = project
        env["DBT_BIGQUERY_LOCATION"] = location
        env["DBT_BIGQUERY_SCHEMA"] = datasets[mode]
        env["DBT_ADBC_CACHE_DIR"] = str(
            (PROJECT_DIR / ".cache" / "compare" / mode).resolve()
        )
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--keep-datasets",
            "--dbt-archive",
            str(archive),
        ]
        if mode == "upstream":
            command.append("--upstream-adbc")
        LOG.command(command)
        result = subprocess.run(command, cwd=REPO_DIR, env=env, check=False)
        if result.returncode != 0:
            failures.append((mode, result.returncode))

    if failures:
        raise RuntimeError(
            f"driver comparison runs failed: {failures!r}; retained datasets {targets!r}"
        )

    LOG.section("Compare complete dbt results across drivers")
    expected_differences = assert_full_project_parity(
        client,
        project=project,
        location=location,
        datasets=datasets,
    )
    LOG.note(
        "dbt show values, complete row multisets and stable metadata are identical"
    )
    for difference in expected_differences:
        LOG.note(f"expected decimal metadata difference: {difference}")

    if keep_datasets:
        LOG.note(f"retained comparison datasets: {', '.join(targets)}")
    else:
        for dataset in targets:
            client.delete_dataset(
                f"{project}.{dataset}", delete_contents=True, not_found_ok=True
            )
        LOG.note("removed all four comparison datasets")
    LOG.success(
        "The local and default dbt BigQuery drivers produced equivalent results."
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--upstream-adbc",
        action="store_true",
        help="use dbt-core 2.0's canonical BigQuery ADBC driver",
    )
    mode.add_argument(
        "--compare",
        action="store_true",
        help="run the complete project with both drivers and compare their results",
    )
    parser.add_argument(
        "--dbt-archive",
        type=Path,
        help="run this tools/dbt-build archive instead of building one; "
        "a release archive downloads its driver",
    )
    parser.add_argument(
        "--keep-datasets",
        action="store_true",
        help="retain successful integration datasets for inspection",
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
        [install / "dbt", "--version"], check=True, text=True, capture_output=True
    ).stdout.strip()
    identifier = DBT_IDENTIFIER.fullmatch(dbt_version)
    if identifier is None:
        raise RuntimeError(f"a tools/dbt-build archive is required; found: {dbt_version}")

    os.environ.setdefault("DBT_BIGQUERY_LOCATION", "US")
    os.environ.setdefault("DBT_BIGQUERY_SCHEMA", f"bigquery_adbc_integration_{mode}")
    # Match profiles.yml so upload and submission assertions inspect the configured resources.
    os.environ.setdefault("DBT_BIGQUERY_GCS_BUCKET", f"{project}-adbc-scratch")
    os.environ.setdefault(
        "DBT_BIGQUERY_DATAPROC_CLUSTER", "bigquery-adbc-integration-absent-cluster"
    )
    os.environ.setdefault("DBT_BIGQUERY_COMPUTE_REGION", "us-central1")
    location = os.environ["DBT_BIGQUERY_LOCATION"]
    schema = os.environ["DBT_BIGQUERY_SCHEMA"]
    _validate_integration_schema(schema)

    cache_home = Path(
        os.environ.get("DBT_ADBC_CACHE_DIR", PROJECT_DIR / ".cache" / mode)
    ).resolve()
    os.environ["XDG_CACHE_HOME"] = str(cache_home)
    target_dir = PROJECT_DIR / "target" / mode
    log_dir = PROJECT_DIR / "logs" / mode
    target_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "dbt.log").unlink(missing_ok=True)
    (target_dir / "show_results.json").unlink(missing_ok=True)
    (target_dir / "sources.json").unlink(missing_ok=True)

    LOG.section(f"dbt BigQuery integration ({mode} driver)")
    LOG.note(dbt_version)
    LOG.note(f"project={project} dataset={schema} location={location}")

    built = built_driver(install, cache_home, identifier["driver"])
    canonical_dbt = None
    if not built.downloaded:
        canonical_dbt = install_canonical_dbt(install, PROJECT_DIR / ".cache" / "dbt" / "canonical")
    staged_driver: BuiltDriver | None = None
    if args.upstream_adbc:
        if canonical_dbt is None:
            raise RuntimeError("a release build cannot load dbt's canonical driver")
        os.environ["BIGQUERY_ADBC_USE_DBT_DRIVER"] = "1"
        os.environ["PATH"] = on_path(canonical_dbt, os.environ)
        LOG.note("using dbt-core 2.0's canonical BigQuery ADBC driver")
    else:
        staged_driver = built
        os.environ["PATH"] = on_path(install, os.environ)
        if built.downloaded:
            # Only this file: the cache directory can be the caller's. It reappearing is what
            # proves the download, so it must not survive from an earlier run.
            built.library.unlink(missing_ok=True)
            LOG.note(f"release build: dbt must download {built.library.name}")
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
        LOG.section(title)
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
            LOG.note(f"all {nodes} expected dbt results have the required status")
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
            LOG.note(f"table descriptions are as expected for {', '.join(sorted(expected_descriptions))}")
        return started

    try:
        run_phase("Check credentials and connection", "debug", "--connection")
        if staged_driver is not None:
            assert_local_driver_resolution(log_dir, staged_driver)
            expected_hash = local_driver.sha256(staged_driver.library)
        run_phase(
            "Start from an empty integration dataset",
            "run-operation",
            "drop_integration_schema",
        )

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
            "build",
            "--full-refresh",
            "--exclude",
            "tag:snapshot",
            "tag:python_model",
            coverage="initial_build",
        )
        run_phase(
            "Exercise BigQuery source freshness metadata",
            "source",
            "freshness",
            "--select",
            "source:known_issues_seed_tables.orders",
            "source:exotic_seed_tables.fct_orders_metadata",
            coverage="freshness",
        )
        run_phase("Rebuild seeds to exercise replacement ingestion", "seed", "--full-refresh", coverage="seeds")
        phase_started = run_phase(
            "Exercise incremental MERGE and insert-overwrite updates",
            "build",
            "--select",
            "tag:incremental",
            "--vars",
            "{incremental_wave: 2}",
            coverage="incremental",
        )
        if staged_driver is None:
            response = _successful_adapter_response(
                target_dir / "run_results.json",
                unique_id=INCREMENTAL_UNIQUE_ID,
            )
            assert_canonical_incremental_adapter_response(
                response, expected_project=project, expected_location=location
            )
            LOG.note("canonical dbt driver retained its known SUCCESS/zero-row MERGE response")
        else:
            assert_run_result_adapter_response(
                target_dir / "run_results.json",
                unique_id=INCREMENTAL_UNIQUE_ID,
                expected_code="MERGE",
                expected_rows_affected=5,
                expected_project=project,
                expected_location=location,
            )
            LOG.note("dbt surfaced MERGE, its five affected rows and its job statistics")
            assert_run_result_adapter_response(
                target_dir / "run_results.json",
                unique_id="model.bigquery_adbc_integration.exotic_dynamic_overwrite",
                expected_code="SCRIPT",
                expected_rows_affected=None,
                expected_project=project,
                expected_location=location,
            )
            LOG.note(
                "dbt surfaced the dynamic insert-overwrite SCRIPT statement type, "
                "its job statistics and that a script reports no row count"
            )
            assert_incremental_job_options(
                client,
                target_dir / "run_results.json",
                project=project,
                dataset=schema,
                location=location,
                phase_started=phase_started,
            )
            LOG.note(
                "the MERGE and copy_partitions' copy jobs carry dbt's job labels, and "
                "the profile's and the model's job timeouts"
            )

        run_phase(
            "Rebuild tables and an unchanged materialized view over existing relations",
            "build",
            "--select",
            "dim_customers",
            "exotic_range_partitioned",
            REFRESHED_VIEW,
            coverage="replace_relations",
        )
        if staged_driver is not None:
            assert_materialized_view_refresh(
                client,
                target_dir / "run_results.json",
                project=project,
                dataset=schema,
                location=location,
            )
            LOG.note(f"dbt read {REFRESHED_VIEW}'s configuration back unchanged and refreshed it with a SCRIPT job")
        run_phase(
            "Rebuild the authorized view to reapply its dataset grant",
            "build",
            "--select",
            "exotic_authorized_view",
            coverage="authorized_view",
        )
        run_phase(
            "Alter the materialized view configuration",
            "build",
            "--select",
            "exotic_materialized_view",
            "--vars",
            "{mv_refresh_minutes: 60}",
            coverage="alter_materialized_view",
        )
        run_phase(
            "Exercise the adapter metadata surface",
            "run-operation",
            "assert_adapter_surface",
            *(() if staged_driver is not None else ("--vars", "{canonical_driver: true}")),
            # Only this driver writes the read-path events.
            required_events=READ_PATH_EVENTS if staged_driver is not None else (),
            completion="adapter surface checks passed for ",
        )
        LOG.note("the driver reported no Storage Read fallback or other warning")
        if staged_driver is not None:
            LOG.note("the driver read results both over further REST pages and through Storage Read")
        expected_descriptions["fct_orders"] = TABLE_DESCRIPTION_SENTINEL
        run_phase(
            "Describe a relation through the option-only table description update",
            "run-operation",
            "assert_table_description_update",
            "--vars",
            f"{{table_description_sentinel: '{TABLE_DESCRIPTION_SENTINEL}'}}",
            completion="update_table_description persisted the sentinel on ",
        )
        run_phase(
            "Build the first snapshot wave",
            "build",
            "--select",
            "tag:snapshot",
            "--exclude",
            "tag:snapshot_final",
            "--vars",
            "{snapshot_wave: 1}",
            coverage="snapshot_initial",
        )
        run_phase(
            "Apply and test the changed snapshot wave",
            "build",
            "--select",
            "tag:snapshot",
            "--vars",
            "{snapshot_wave: 2}",
            coverage="snapshot_changed",
        )

        LOG.section("Require dbt to report an intentionally failing data test")
        failure_dir = target_dir / "phases" / "failure_probe"
        (failure_dir / "run_results.json").unlink(missing_ok=True)
        failing_dbt_command(
            "test", "--select", "exotic_failure_probe",
            "--vars", "{validate_failure_reporting: true}",
            target_dir=target_dir, log_dir=log_dir,
        )
        assert_data_test_failure(
            target_dir / "run_results.json", unique_id=FAILURE_PROBE_UNIQUE_ID
        )
        failure_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target_dir / "run_results.json", failure_dir / "run_results.json")
        LOG.note("dbt failed with exactly one failing row from the selected test")

        LOG.section("Capture BigQuery edge values through ADBC and dbt show")
        capture_show_results(target_dir=target_dir, log_dir=log_dir)

        if staged_driver is not None:
            if local_driver.sha256(staged_driver.library) != expected_hash:
                raise RuntimeError(f"dbt replaced its driver at {staged_driver.library}")
            LOG.note("verified that the driver file remained intact")

            LOG.section("Upload and submit a python model through write_gcs and Dataproc")
            assert_python_model(target_dir=target_dir, log_dir=log_dir)

        LOG.section("Assert BigQuery state through the REST API")
        assert_bigquery_state(
            expected_mv_refresh_minutes=60, local_driver=staged_driver is not None
        )

        LOG.section("Compile the complete project and write its catalog")
        (target_dir / "catalog.json").unlink(missing_ok=True)
        dbt_command(
            "compile", "--write-catalog", target_dir=target_dir, log_dir=log_dir
        )
        assert_catalog_artifact(
            target_dir / "catalog.json",
            expected_nodes=(
                "model.bigquery_adbc_integration.fct_orders",
                "model.bigquery_adbc_integration.exotic_materialized_view",
                "model.bigquery_adbc_integration.exotic_range_partitioned",
                "seed.bigquery_adbc_integration.exotic_wide_types",
            ),
        )

        # A release build cannot load dbt's canonical driver, so a non-release run covers this.
        if staged_driver is not None and canonical_dbt is not None:
            LOG.section(
                "Compare dbt run-operation --sql across canonical and local drivers"
            )
            assert_run_operation_parity(
                cache_home=cache_home,
                staged_driver=staged_driver,
                canonical_dbt=canonical_dbt,
                target_dir=target_dir,
                log_dir=log_dir,
            )
            if local_driver.sha256(staged_driver.library) != expected_hash:
                raise RuntimeError(
                    f"dbt replaced its driver at {staged_driver.library} during parity"
                )

        if args.keep_datasets:
            LOG.note(
                f"retained {schema} and {schema}{AUTHORIZED_SOURCE_SUFFIX} (--keep-datasets)"
            )
        else:
            LOG.section("Remove integration warehouse objects")
            dbt_command(
                "run-operation",
                "drop_integration_schema",
                target_dir=target_dir,
                log_dir=log_dir,
            )
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
                "Run-operation parity datasets with suffixes "
                "_run_operation_upstream and _run_operation_local were also retained "
                "when created.",
                file=sys.stderr,
            )
        return (
            error.returncode if isinstance(error, subprocess.CalledProcessError) else 1
        )

    LOG.success(f"All dbt integration checks passed with the {mode} driver.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
