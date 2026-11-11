"""Own a BigQuery emulator subprocess and expose its endpoints to an existing test command."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from typing import BinaryIO

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SMOKE_TESTS = (
    "a_batch_query_without_a_location_returns_its_rows",
    "executes_scalar_query_and_dry_run_schema",
    "executes_nested_and_repeated_query",
    "executes_named_parameters_for_each_bound_row",
    "bulk_ingests_arrow_with_standard_lifecycle_modes",
    "unordered_storage_read_returns_the_same_rows",
)


def emulator_executable() -> str:
    executable = os.environ.get("ADBC_BIGQUERY_EMULATOR_BIN")
    if not executable:
        raise ValueError("ADBC_BIGQUERY_EMULATOR_BIN must be set to a BigQuery emulator executable")
    return str(Path(executable).expanduser().resolve())


def endpoints(server: subprocess.Popen[bytes], log: BinaryIO) -> tuple[str, str]:
    assert server.stdout is not None
    deadline = time.monotonic() + 30
    output = b""
    with selectors.DefaultSelector() as selector:
        selector.register(server.stdout, selectors.EVENT_READ)
        while output.count(b"\n") < 2:
            check_interrupted()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("emulator did not announce both endpoints within 30 seconds")
            if not selector.select(min(remaining, 0.1)):
                continue
            chunk = os.read(server.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError("emulator exited before announcing both endpoints")
            log.write(chunk)
            output += chunk
            # A read may include request logs after the two announcements.
            if output.count(b"\n") < 2 and len(output) > 4096:
                raise RuntimeError("unexpected emulator startup output")
    announcements = output.split(b"\n", 2)[:2]
    if sum(len(line) for line in announcements) > 4096:
        raise RuntimeError("unexpected emulator startup output")
    lines = [line.decode().removesuffix("\r") for line in announcements]
    rest = re.fullmatch(r"REST listening on (http://127\.0\.0\.1:([0-9]+))", lines[0])
    storage = re.fullmatch(r"Storage gRPC listening on (127\.0\.0\.1:([0-9]+))", lines[1])
    if not rest or not storage or not all(0 < int(match[2]) < 65536 for match in (rest, storage)):
        raise RuntimeError(f"unexpected emulator endpoint announcements: {lines!r}")
    return rest[1], f"http://{storage[1]}"


def drain_stdout(source: BinaryIO, log: BinaryIO, stopped: threading.Event) -> None:
    # Bootstrap and tests can block the caller. Polling also lets cleanup stop the
    # reader if a descendant of the emulator keeps the pipe open after it exits.
    with selectors.DefaultSelector() as selector:
        selector.register(source, selectors.EVENT_READ)
        while not stopped.is_set():
            if selector.select(0.1):
                chunk = os.read(source.fileno(), 65536)
                if not chunk:
                    return
                log.write(chunk)


def bootstrap(rest: str, integration_project: str) -> None:
    # Local fixture traffic must not depend on proxy settings inherited from the shell.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post(path: str, body: dict[str, object]) -> dict[str, object]:
        check_interrupted()
        request = urllib.request.Request(
            f"{rest}/bigquery/v2/projects/{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with opener.open(request, timeout=10) as response:
            return json.load(response)

    for project, dataset in [
        (integration_project, "sq_scratch"),
        ("bigquery-public-data", "samples"),
    ]:
        post(
            f"{project}/datasets",
            {
                "datasetReference": {"projectId": project, "datasetId": dataset},
                "location": "US",
            },
        )
    # Catalog-switching tests need a nonempty table in a second project. This is
    # synthetic data with the public table's schema, not a copy of its contents.
    table_path = "bigquery-public-data/datasets/samples/tables"
    post(
        table_path,
        {
            "tableReference": {
                "projectId": "bigquery-public-data",
                "datasetId": "samples",
                "tableId": "shakespeare",
            },
            "description": "Synthetic one-row fixture for ADBC catalog-switching tests.",
            "schema": {
                "fields": [
                    {"name": "word", "type": "STRING"},
                    {"name": "word_count", "type": "INTEGER"},
                    {"name": "corpus", "type": "STRING"},
                    {"name": "corpus_date", "type": "INTEGER"},
                ]
            },
        },
    )
    inserted = post(
        f"{table_path}/shakespeare/insertAll",
        {
            "rows": [
                {
                    "json": {
                        "word": "fixture",
                        "word_count": 1,
                        "corpus": "local",
                        "corpus_date": 0,
                    }
                }
            ]
        },
    )
    if inserted.get("insertErrors"):
        raise RuntimeError(f"emulator fixture insert failed: {inserted['insertErrors']}")


def stop(process: subprocess.Popen[bytes] | None) -> None:
    """End `process` and the rest of its process group, which can outlive it.

    The group gets five seconds to exit after SIGTERM, then SIGKILL and five more to be gone.
    """
    if process is None:
        return
    for signum in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signum)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            # Reap the leader: its zombie would keep the group alive.
            process.poll()
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)


def show_log(log: BinaryIO) -> None:
    log.seek(max(0, log.seek(0, os.SEEK_END) - 65536))
    if contents := log.read().decode(errors="replace"):
        print(f"\n>> emulator log (last 64 KiB):\n{contents}", file=sys.stderr, flush=True)


# The first SIGINT or SIGTERM. The handler only records it, because an exception raised between
# Popen's fork and the assignment of its result would leave the new process unowned. main() raises
# it where everything it started is owned, and cleanup has no such point, so later signals cannot
# interrupt it.
received: list[int] = []


def record_signal(signum: int, _frame: object) -> None:
    received.append(signum)


def check_interrupted() -> None:
    if received:
        raise SystemExit(128 + received[0])


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run-emulator-tests.sh",
        description="Start an emulator on ephemeral loopback ports and run a test command from the repository root.",
        epilog=(
            "Environment: ADBC_BIGQUERY_EMULATOR_BIN names the emulator executable and "
            "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT the project; both are required (no default). "
            "BIGQUERY_ADBC_PYTHON selects this runner's Python interpreter. "
            "The fresh emulator contains its sq_scratch dataset in US and a synthetic one-row "
            "bigquery-public-data.samples.shakespeare fixture. "
            "Without a command, run six existing tests covering batch queries, scalar queries and dry runs, "
            "nested results, named parameters, Storage Write ingestion, and Storage Read. "
            "Example: scripts/run-emulator-tests.sh -- "
            "scripts/run-one-live-test.sh executes_nested_and_repeated_query"
        ),
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="test command after --")
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    commands = [command] if command else [["scripts/run-one-live-test.sh", name] for name in SMOKE_TESTS]

    signal.signal(signal.SIGINT, record_signal)
    signal.signal(signal.SIGTERM, record_signal)
    server = None
    test = None
    stdout_thread = None
    stdout_stopped = threading.Event()
    status = 1
    with tempfile.TemporaryFile(buffering=0) as log:
        try:
            project = os.environ.get("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT")
            if not project:
                raise ValueError("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set to a non-empty project ID")
            if any(c.isspace() or c in "`/?#" for c in project):
                raise ValueError("unsafe BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT value")
            executable = emulator_executable()
            server = subprocess.Popen(
                [
                    executable,
                    "serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "0",
                    "--grpc-port",
                    "0",
                    "--project",
                    project,
                    "--project",
                    "bigquery-public-data",
                    "--log-requests",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=log,
                start_new_session=True,
            )
            check_interrupted()
            rest, storage = endpoints(server, log)
            assert server.stdout is not None
            stdout_thread = threading.Thread(target=drain_stdout, args=(server.stdout, log, stdout_stopped))
            stdout_thread.start()
            bootstrap(rest, project)
            print(f">> emulator REST: {rest}; Storage: {storage}", flush=True)
            environment = {
                **os.environ,
                "ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT": rest,
                "ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT": storage,
                "ADBC_BIGQUERY_RUN_INTEGRATION": "1",
            }
            for command in commands:
                test = subprocess.Popen(command, cwd=REPOSITORY_ROOT, env=environment, start_new_session=True)
                while True:
                    check_interrupted()
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        test.wait(timeout=0.2)
                    if server.poll() is not None:
                        raise RuntimeError(f"emulator exited while tests were running (status {server.returncode})")
                    if test.returncode is not None:
                        status = test.returncode if test.returncode >= 0 else 128 - test.returncode
                        break
                stop(test)
                test = None
                check_interrupted()
                if status:
                    return status
            return 0
        except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
            status = 1
            print(f"error: {error}", file=sys.stderr, flush=True)
            return 1
        finally:
            stop(test)
            stop(server)
            stdout_stopped.set()
            if stdout_thread is not None:
                stdout_thread.join()
            if server is not None and server.stdout is not None:
                server.stdout.close()
            if status:
                show_log(log)


if __name__ == "__main__":
    status = main()
    # A signal that arrived after the last check, during cleanup, still marks the run interrupted.
    sys.exit(128 + received[0] if received else status)
