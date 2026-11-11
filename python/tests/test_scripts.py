from __future__ import annotations

import hashlib
import http.server
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DBT_INSTALLER = REPOSITORY_ROOT / "tools/dbt-build/install.sh"
posix = pytest.mark.skipif(sys.platform == "win32" or shutil.which("bash") is None, reason="requires POSIX Bash")


def fake_path(tmp_path: Path, **tools: str) -> str:
    """Write executable stand-ins for `tools` and return a PATH that finds them first."""
    directory = tmp_path / "bin"
    directory.mkdir()
    for name, script in tools.items():
        (directory / name).write_text(script, encoding="utf-8")
        (directory / name).chmod(0o755)
    return f"{directory}{os.pathsep}{os.environ['PATH']}"


@pytest.mark.parametrize(
    "lock",
    sorted(
        path
        for path in REPOSITORY_ROOT.glob("integration/*/uv.lock")
        if 'name = "duckdb"' in path.read_text(encoding="utf-8")
    ),
    ids=lambda path: path.parent.name,
)
def test_suites_using_duckdb_select_a_python_build_with_the_gil(lock: Path) -> None:
    version_file = lock.with_name(".python-version")
    lines = version_file.read_text(encoding="utf-8").splitlines()
    requests = [line for line in lines if not line.startswith("#")]
    assert len(requests) == 1 and requests[0].endswith("+gil")


@posix
@pytest.mark.parametrize("value", [None, "", "project/dataset"])
@pytest.mark.parametrize(
    "command",
    [
        ["run-one-live-test.sh", "executes_scalar_query_and_dry_run_schema"],
        ["run-python-tests.sh"],
        ["run-service-account-live-test.sh"],
        ["run-storage-read-permission-fallback-live-test.sh"],
        ["run-oauth-live-test.sh"],
        ["run-emulator-tests.sh"],
    ],
)
def test_integration_runners_require_project_before_external_work(
    tmp_path: Path, command: list[str], value: str | None
) -> None:
    variable = "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"
    environment = {key: value for key, value in os.environ.items() if key != variable}
    environment.update(
        BIGQUERY_ADBC_PYTHON=sys.executable,
        # Old variables and ambient gcloud configuration must not select a project.
        ADBC_BIGQUERY_PROJECT="old-adbc-project",
        DBT_BIGQUERY_PROJECT="old-dbt-project",
        GOOGLE_CLOUD_PROJECT="ambient-project",
        TMPDIR=str(tmp_path / "must-not-create-a-workspace"),
    )
    if value is not None:
        environment[variable] = value
    result = subprocess.run(
        [str(REPOSITORY_ROOT / "scripts" / command[0]), *command[1:]],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode != 0
    assert variable in result.stderr


@posix
def test_project_preflight_exports_the_explicit_value() -> None:
    result = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail\nsource "$1"\nrequire_bigquery_project\n'
            'bash -c \'printf "%s" "$BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"\'\n',
            "bash",
            str(REPOSITORY_ROOT / "scripts/lib/google-cloud-preflight.sh"),
        ],
        env={**os.environ, "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": "chosen-project"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "chosen-project"


@posix
@pytest.mark.parametrize(
    ("output", "cargo_status", "status"),
    [
        pytest.param("running 1 test\ntest result: ok. 1 passed\n", 0, 0, id="passed"),
        # Under --nocapture, stderr lands inside libtest's result line.
        pytest.param("running 1 test\ntest result: ok.. 1 passed\n", 0, 0, id="interleaved"),
        pytest.param("running 1 test\ntest result: ok\nnoise\n. 1 passed\n", 0, 0, id="split"),
        pytest.param("running 1 test\ntest result: FAILED. 0 passed\n", 101, 101, id="failed"),
        pytest.param("running 0 tests\ntest result: ok. 0 passed\n", 0, 1, id="nothing-ran"),
    ],
)
def test_one_live_test_runner_trusts_cargo_once_the_test_ran(
    tmp_path: Path, output: str, cargo_status: int, status: int
) -> None:
    path = fake_path(
        tmp_path,
        cargo="#!/usr/bin/env bash\n"
        'if [[ " $* " == *" --list "* ]]; then echo "query::case: test"; exit; fi\n'
        'printf "%s" "$CARGO_OUTPUT"\n'
        'exit "$CARGO_STATUS"\n',
    )
    result = subprocess.run(
        [str(REPOSITORY_ROOT / "scripts/run-one-live-test.sh"), "case", "--nocapture"],
        env={
            **os.environ,
            "PATH": path,
            "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": "chosen-project",
            "CARGO_OUTPUT": output,
            "CARGO_STATUS": str(cargo_status),
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == status, result.stderr


def run_oauth_runner(tmp_path: Path, document: str | None, cargo: str) -> subprocess.CompletedProcess[str]:
    """Run the OAuth runner on an ADC file holding `document`, with `cargo` standing in for Cargo."""
    credentials = tmp_path / "application_default_credentials.json"
    if document is not None:
        credentials.write_text(document, encoding="utf-8")
    environment = {key: value for key, value in os.environ.items() if not key.startswith("BIGQUERY_ADBC_OAUTH_")}
    environment.update(
        BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT="chosen-project",
        BIGQUERY_ADBC_PYTHON=sys.executable,
        GOOGLE_APPLICATION_CREDENTIALS=str(credentials),
        PATH=fake_path(tmp_path, cargo=cargo),
        FAKE_ROOT=str(tmp_path),
    )
    return subprocess.run(
        [str(REPOSITORY_ROOT / "scripts/run-oauth-live-test.sh")],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


@posix
@pytest.mark.parametrize(
    ("document", "problem"),
    [
        (None, "No such file"),
        ("{not json", "Expecting property name"),
        ('["authorized_user"]', "holds a list, not"),
        ('{"type": "service_account"}', "holds 'service_account' credentials"),
        ('{"type": "authorized_user", "client_id": "id", "client_secret": "secret"}', "has no refresh_token"),
        (
            '{"type": "authorized_user", "client_id": 7, "client_secret": "s", "refresh_token": "t"}',
            "client_id is not a string",
        ),
    ],
)
def test_oauth_runner_explains_an_unusable_adc_file(tmp_path: Path, document: str | None, problem: str) -> None:
    # Reaching Cargo would be a failure to stop.
    result = run_oauth_runner(tmp_path, document, cargo="#!/bin/sh\nexit 99\n")

    assert result.returncode == 1, result.stderr
    assert problem in result.stderr
    assert "Traceback" not in result.stderr
    assert "secret" not in result.stderr


@posix
def test_oauth_runner_passes_adc_fields_through_the_environment(tmp_path: Path) -> None:
    fields = {"client_id": "id with 'quotes'", "client_secret": "$(touch pwned) secret", "refresh_token": "a\nb"}
    variables = " ".join(f'"$BIGQUERY_ADBC_OAUTH_{field.upper()}"' for field in fields)
    # The runner calls Cargo from the repository root.
    cargo = f"#!/usr/bin/env bash\nprintf '%s\\0' {variables} > \"$FAKE_ROOT/seen\"\nexit 99\n"

    result = run_oauth_runner(tmp_path, json.dumps({"type": "authorized_user", **fields}), cargo)

    # The runner's first Cargo call saw the variables.
    assert result.returncode == 99, result.stderr
    assert (tmp_path / "seen").read_text(encoding="utf-8").split("\0")[:-1] == list(fields.values())
    assert not (REPOSITORY_ROOT / "pwned").exists()
    assert "secret" not in result.stdout + result.stderr


FAKE_EMULATOR = """\
import http.server
import json
import os
import pathlib
import socket
import sys
import time

root = pathlib.Path(os.environ["FAKE_ROOT"])
(root / "server.pid").write_text(str(os.getpid()))
if os.environ.get("FAKE_FAILURE") == "startup":
    print("startup diagnostic", flush=True)
    print("startup failed", file=sys.stderr)
    sys.exit(3)
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with (root / "requests.jsonl").open("a") as log:
            log.write(json.dumps({"path": self.path, "body": body}) + "\\n")
        self.send_response(500 if os.environ.get("FAKE_FAILURE") == "bootstrap" else 200)
        self.end_headers()
        self.wfile.write(b"{}")
server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
storage = socket.socket()
storage.bind(("127.0.0.1", 0))
storage.listen()
print(f"REST listening on http://127.0.0.1:{server.server_port}", end="\\r\\n", flush=True)
if os.environ.get("FAKE_STDOUT_LOGGING"):
    time.sleep(0.05)
print(f"Storage gRPC listening on 127.0.0.1:{storage.getsockname()[1]}", end="\\r\\n", flush=True)
if os.environ.get("FAKE_STDOUT_LOGGING"):
    print("request log café ☃ " * 65536, flush=True)
server.serve_forever()
"""


def emulator_environment(tmp_path: Path) -> dict[str, str]:
    executable = tmp_path / "emulator"
    executable.write_text(f"#!{sys.executable}\n{FAKE_EMULATOR}", encoding="utf-8")
    executable.chmod(0o755)
    return {
        **os.environ,
        "ADBC_BIGQUERY_EMULATOR_BIN": str(executable),
        "FAKE_ROOT": str(tmp_path),
        "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": "emulator-project",
    }


def run_emulator(code: str, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(REPOSITORY_ROOT / "scripts/run-emulator-tests.sh"), "--", sys.executable, "-c", code],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def assert_process_stopped(pid_path: Path) -> None:
    pid = int(pid_path.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@posix
@pytest.mark.parametrize("value", [None, ""])
def test_emulator_runner_requires_an_explicit_executable(tmp_path: Path, value: str | None) -> None:
    environment = emulator_environment(tmp_path)
    del environment["ADBC_BIGQUERY_EMULATOR_BIN"]
    if value is not None:
        environment["ADBC_BIGQUERY_EMULATOR_BIN"] = value
    result = run_emulator('raise AssertionError("test command must not run")', environment)
    assert result.returncode == 1
    assert "ADBC_BIGQUERY_EMULATOR_BIN must be set" in result.stderr
    assert "test command must not run" not in result.stderr
    assert not (tmp_path / "server.pid").exists()


@posix
@pytest.mark.parametrize("status", [0, 7])
@pytest.mark.parametrize("stdout_logging", [False, True])
def test_emulator_runner_bootstraps_exports_and_cleans_up(tmp_path: Path, status: int, stdout_logging: bool) -> None:
    result = run_emulator(
        "import json, os, pathlib, sys; "
        'pathlib.Path(os.environ["FAKE_ROOT"], "child.json").write_text('
        'json.dumps({"cwd": str(pathlib.Path.cwd()), "environment": dict(os.environ)})); '
        f"sys.exit({status})",
        {**emulator_environment(tmp_path), "FAKE_STDOUT_LOGGING": "1" if stdout_logging else ""},
    )
    assert result.returncode == status, result.stderr
    if status and stdout_logging:
        assert "request log café ☃" in result.stderr
    child = json.loads((tmp_path / "child.json").read_text())
    assert child["cwd"] == str(REPOSITORY_ROOT)
    for key in ("ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT", "ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT"):
        assert child["environment"][key].startswith("http://127.0.0.1:")
        assert int(child["environment"][key].rsplit(":", 1)[1]) > 0
    assert child["environment"]["BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"] == "emulator-project"
    assert child["environment"]["ADBC_BIGQUERY_RUN_INTEGRATION"] == "1"
    requests = [json.loads(line) for line in (tmp_path / "requests.jsonl").read_text().splitlines()]
    assert requests[0] == {
        "path": "/bigquery/v2/projects/emulator-project/datasets",
        "body": {"datasetReference": {"projectId": "emulator-project", "datasetId": "sq_scratch"}, "location": "US"},
    }
    assert requests[1]["body"]["datasetReference"] == {
        "projectId": "bigquery-public-data",
        "datasetId": "samples",
    }
    assert requests[2]["body"]["tableReference"]["tableId"] == "shakespeare"
    assert [field["name"] for field in requests[2]["body"]["schema"]["fields"]] == [
        "word",
        "word_count",
        "corpus",
        "corpus_date",
    ]
    assert (
        requests[3]["path"]
        == "/bigquery/v2/projects/bigquery-public-data/datasets/samples/tables/shakespeare/insertAll"
    )
    assert len(requests[3]["body"]["rows"]) == 1
    assert_process_stopped(tmp_path / "server.pid")


@posix
@pytest.mark.parametrize("failure", ["startup", "bootstrap"])
def test_emulator_runner_cleans_up_a_failed_start_without_running_tests(tmp_path: Path, failure: str) -> None:
    result = run_emulator(
        'raise AssertionError("test command must not run")',
        {**emulator_environment(tmp_path), "FAKE_FAILURE": failure},
    )
    assert result.returncode == 1
    assert "test command must not run" not in result.stderr
    assert "startup failed" in result.stderr if failure == "startup" else "HTTP Error 500" in result.stderr
    if failure == "startup":
        assert "startup diagnostic" in result.stderr
    assert_process_stopped(tmp_path / "server.pid")


@posix
def test_emulator_runner_terminates_its_server_and_test_on_signal(tmp_path: Path) -> None:
    with subprocess.Popen(
        [
            str(REPOSITORY_ROOT / "scripts/run-emulator-tests.sh"),
            "--",
            sys.executable,
            "-c",
            "import os, pathlib, time; "
            'pathlib.Path(os.environ["FAKE_ROOT"], "child.pid").write_text(str(os.getpid())); time.sleep(60)',
        ],
        env=emulator_environment(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as runner:
        try:
            deadline = time.monotonic() + 10
            while not (tmp_path / "child.pid").exists():
                assert runner.poll() is None, runner.communicate()
                assert time.monotonic() < deadline, "test command did not start"
                time.sleep(0.01)
            runner.send_signal(signal.SIGTERM)
            runner.communicate(timeout=10)
            assert runner.returncode == 143
        finally:
            if runner.poll() is None:
                runner.terminate()
                runner.communicate(timeout=10)
    assert_process_stopped(tmp_path / "server.pid")
    assert_process_stopped(tmp_path / "child.pid")


# Starts a process that ignores SIGTERM and records its PID once it does.
IGNORE_SIGTERM = (
    "import os, pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    'pathlib.Path(os.environ["FAKE_ROOT"], "{name}.pid").write_text(str(os.getpid())); time.sleep(60)'
)


@posix
def test_emulator_runner_kills_a_test_descendant_that_ignores_sigterm(tmp_path: Path) -> None:
    # The test command exits at once, leaving its child in the test's process group.
    result = run_emulator(
        "import os, pathlib, subprocess, sys, time; "
        f"subprocess.Popen([sys.executable, '-c', {IGNORE_SIGTERM.format(name='grandchild')!r}]); "
        'pid = pathlib.Path(os.environ["FAKE_ROOT"], "grandchild.pid")\n'
        "while not pid.exists(): time.sleep(0.01)",
        emulator_environment(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    assert_process_stopped(tmp_path / "grandchild.pid")
    assert_process_stopped(tmp_path / "server.pid")


@posix
def test_emulator_runner_finishes_cleanup_despite_a_second_signal(tmp_path: Path) -> None:
    with subprocess.Popen(
        [
            str(REPOSITORY_ROOT / "scripts/run-emulator-tests.sh"),
            "--",
            sys.executable,
            "-c",
            IGNORE_SIGTERM.format(name="child"),
        ],
        env=emulator_environment(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as runner:
        try:
            deadline = time.monotonic() + 10
            while not (tmp_path / "child.pid").exists():
                assert runner.poll() is None, runner.communicate()
                assert time.monotonic() < deadline, "test command did not start"
                time.sleep(0.01)
            runner.send_signal(signal.SIGTERM)
            # Cleanup now waits out the child's ignored SIGTERM before killing it.
            time.sleep(1)
            runner.send_signal(signal.SIGINT)
            runner.communicate(timeout=15)
            assert runner.returncode == 143
        finally:
            if runner.poll() is None:
                runner.kill()
                runner.communicate(timeout=10)
    assert_process_stopped(tmp_path / "child.pid")
    assert_process_stopped(tmp_path / "server.pid")


@posix
@pytest.mark.parametrize("count", [0, 1, 2])
@pytest.mark.parametrize("nullglob", ["-s", "-u"])
def test_built_wheel_helper_under_errexit(tmp_path: Path, count: int, nullglob: str) -> None:
    wheel_dir = tmp_path / "wheel directory"
    wheel_dir.mkdir()
    for index in range(count):
        (wheel_dir / f"driver-{index}.whl").touch()

    result = subprocess.run(
        [
            "bash",
            "-c",
            "set -euo pipefail\n"
            'source "$1"\n'
            'shopt "$3" nullglob\n'
            "before=$(shopt -p nullglob || true)\n"
            'built_wheel "$2"\n'
            '[[ "$(shopt -p nullglob || true)" == "$before" ]]\n',
            "bash",
            str(REPOSITORY_ROOT / "scripts/lib/built-wheel.sh"),
            str(wheel_dir),
            nullglob,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if count == 1:
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == str(wheel_dir / "driver-0.whl")
    else:
        assert result.returncode == 1
        assert f"expected exactly one built wheel in {wheel_dir}, found {count}" in result.stderr


@posix
@pytest.mark.parametrize("libraries", [0, 1, 2, None])
@pytest.mark.parametrize("target_directory", ["target dir", 'target ["quoted"] \\ café'])
def test_cargo_driver_library_helper_reports_the_built_library(
    tmp_path: Path, libraries: int | None, target_directory: str
) -> None:
    # A target triple, like CARGO_BUILD_TARGET or build.target, moves the library below target/<triple>/.
    directory = tmp_path / target_directory / "x86_64-unknown-linux-gnu" / "release"
    built = [directory / "libadbc_driver_bigquery.so", directory / "libadbc_driver_bigquery.dylib"][: libraries or 0]
    messages = [
        {
            "reason": "compiler-artifact",
            "target": {"kind": ["lib"], "crate_types": ["lib"], "name": "dependency"},
            "filenames": [str(directory / "deps" / "libdependency.so")],
        },
        {
            "reason": "compiler-artifact",
            "target": {"kind": ["cdylib"], "crate_types": ["cdylib"], "name": "adbc_driver_bigquery"},
            "filenames": list(map(str, built)),
        },
        {"reason": "build-finished", "success": libraries is not None},
    ]
    path = fake_path(
        tmp_path,
        cargo="#!/usr/bin/env bash\n"
        '[[ $1 == rustc && " $* " == *" --crate-type cdylib "* ]] || exit 3\n'
        '[[ " $* " == *" --manifest-path $EXPECTED_MANIFEST "* && " $* " == *" --release "* ]] || exit 3\n'
        "printf 'macro output\\n\\n'\n"
        'printf "%s\\n" "$CARGO_MESSAGES"\n'
        "[[ $CARGO_MESSAGES == *'\"success\":true'* ]] || exit 101\n",
    )
    manifest = tmp_path / "Cargo.toml"

    result = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail\nsource "$1"\ncargo_build_driver_library "$2" --release\n',
            "bash",
            str(REPOSITORY_ROOT / "scripts/lib/cargo-driver-library.sh"),
            str(manifest),
        ],
        env={
            **os.environ,
            "PATH": path,
            "BIGQUERY_ADBC_PYTHON": sys.executable,
            "EXPECTED_MANIFEST": str(manifest),
            "CARGO_MESSAGES": "\n".join(json.dumps(message, separators=(",", ":")) for message in messages),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    if libraries == 1:
        assert result.returncode == 0, result.stderr
        assert result.stdout == f"{built[0]}\n"
    elif libraries is None:
        assert result.returncode == 1
        assert "cargo build failed" in result.stderr
    else:
        assert result.returncode == 1
        assert "cargo did not report exactly one driver library" in result.stderr


@pytest.mark.skipif(
    sys.platform != "linux" or not all(shutil.which(tool) for tool in ("bash", "size", "strip", "nm")),
    reason="requires Bash and binutils on Linux",
)
@pytest.mark.parametrize(("profile", "directory"), [("release", "release"), ("dev", "debug")])
@pytest.mark.parametrize("build", [True, False])
def test_binary_size_report_distinguishes_artifact_from_reporting_context(
    tmp_path: Path, profile: str, directory: str, build: bool
) -> None:
    # Any ELF file will do for the size tools; the path is outside the repository's target/.
    library = tmp_path / "configured target" / directory / "libadbc_driver_bigquery.so"
    library.parent.mkdir(parents=True)
    shutil.copyfile(Path(sys.executable).resolve(), library)
    artifact = {
        "reason": "compiler-artifact",
        "target": {"kind": ["cdylib"], "crate_types": ["cdylib"], "name": "adbc_driver_bigquery"},
        "filenames": [str(library)],
    }
    path = fake_path(
        tmp_path,
        cargo="#!/usr/bin/env bash\n"
        'if [[ $1 == -V ]]; then echo "cargo 1.0.0"; exit; fi\n'
        "[[ $EXPECT_BUILD == 1 ]] || exit 3\n"
        '[[ $1 == rustc && " $* " == *" --crate-type cdylib "* ]] || exit 3\n'
        f'[[ " $* " == *" --lib "* && " $* " == *" --profile {profile} "* ]] || exit 3\n'
        'printf "%s\\n" "$CARGO_MESSAGE"\n',
        rustc="#!/usr/bin/env bash\n"
        "printf 'release: 1.0.0\\nhost: x86_64-unknown-linux-gnu\\ncommit-hash: unknown\\n'\n",
    )

    command = [str(REPOSITORY_ROOT / "scripts/report-binary-size.sh"), "--profile", profile, "--top", "1"]
    if not build:
        command.extend(["--artifact", str(library)])
    result = subprocess.run(
        command,
        env={
            **os.environ,
            "PATH": path,
            "BIGQUERY_ADBC_PYTHON": sys.executable,
            "EXPECT_BUILD": "1" if build else "0",
            "CARGO_BUILD_TARGET": "",
            # Cargo emits compact JSON, one message per line.
            "CARGO_MESSAGE": json.dumps(artifact, separators=(",", ":")),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    version, *rows = result.stdout.splitlines()
    assert version == "binary_size_report_version\t2"
    report = {(kind, key): value for kind, key, value in (row.split("\t", 2) for row in rows)}
    assert report["identity", "artifact"] == str(library)
    assert report["identity", "build_mode"] == ("built" if build else "provided")
    assert int(report["metric", "artifact_bytes"]) == library.stat().st_size
    assert report["metric", "artifact_sha256"] == hashlib.sha256(library.read_bytes()).hexdigest()
    assert report["context", "rustc_version"] == "1.0.0"
    assert report["context", "requested_profile"] == profile
    assert report["context", "cargo_build_target"] == "unset"
    assert {key for kind, key in report if kind == "identity"} == {"artifact", "build_mode", "binary_format"}
    assert (
        report["context", "git_commit"]
        == subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPOSITORY_ROOT, text=True).strip()
    )


@pytest.fixture
def dbt_release(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """Serve release archives and GitHub's latest-tag redirect through real curl requests."""
    release = tmp_path / "releases/download/v1.2.3"
    release.mkdir(parents=True)
    tag = tmp_path / "releases/tag/v1.2.3"
    tag.parent.mkdir()
    tag.touch()
    checksums = []
    for target in ("linux-x86_64", "linux-aarch64", "macos-x86_64", "macos-aarch64"):
        name = f"dbt-bigquery-adbc-1.2.3-{target}"
        package = tmp_path / name
        package.mkdir()
        binary = package / "dbt-bigquery-adbc"
        binary.write_text("#!/bin/sh\necho 'dbt (modified with bigquery-adbc 1.2.3) test-commit'\n")
        binary.chmod(0o755)
        sidecar = package / "lib/bigquery-adbc/driver.so"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_bytes(b"test driver")
        archive = release / f"{name}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(package, arcname=name)
        checksums.append(f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n")
    (release / "dbt-SHA256SUMS").write_text("".join(checksums))

    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_HEAD(self):
            if self.path == "/releases/latest":
                self.send_response(302)
                self.send_header("Location", "/releases/tag/v1.2.3")
                self.end_headers()
            else:
                super().do_HEAD()

        def log_message(self, *_args):
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, directory=str(tmp_path))) as server:
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/releases", release
        finally:
            server.shutdown()
            worker.join()


def run_dbt_installer(tmp_path: Path, releases_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    # Isolate shell startup changes and the default prefix from the developer's installation.
    return subprocess.run(
        ["sh", "-s", "--", *args],
        input=DBT_INSTALLER.read_text(),
        env={
            **os.environ,
            "HOME": str(tmp_path),
            "SHELL": "/bin/bash",
            "BIGQUERY_ADBC_RELEASES_URL": releases_url,
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


@posix
@pytest.mark.parametrize(
    "target",
    ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-apple-darwin", "aarch64-apple-darwin"],
)
def test_dbt_installer_updates_beside_stock_dbt(tmp_path: Path, dbt_release: tuple[str, Path], target: str) -> None:
    prefix = tmp_path / ".local"
    binary = prefix / "bin/dbt-bigquery-adbc"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\necho 'dbt (modified with bigquery-adbc 1.2.2) old-commit'\n")
    binary.chmod(0o755)
    stock = prefix / "bin/dbt"
    stock.write_bytes(b"stock dbt")
    stock_driver = prefix / "lib/dbt/driver.so"
    stock_driver.parent.mkdir(parents=True)
    stock_driver.write_bytes(b"stock driver")
    shell_config = tmp_path / ".bashrc"
    shell_config.write_text(f'export PATH="$PATH:{prefix}/bin"\nalias dbtf={stock}\n')
    original_config = shell_config.read_bytes()

    refused = run_dbt_installer(tmp_path, dbt_release[0], "--target", target)
    assert refused.returncode != 0
    assert "use the --update flag" in refused.stdout
    assert "1.2.2" in binary.read_text()
    installed = run_dbt_installer(tmp_path, dbt_release[0], "--update", "--target", target)
    assert installed.returncode == 0, installed.stdout + installed.stderr
    assert "1.2.3" in binary.read_text()
    assert os.access(binary, os.X_OK)
    assert (prefix / "lib/bigquery-adbc/driver.so").read_bytes() == b"test driver"
    assert stock.read_bytes() == b"stock dbt"
    assert stock_driver.read_bytes() == b"stock driver"
    assert shell_config.read_bytes() == original_config
    repeated = run_dbt_installer(tmp_path, dbt_release[0], "--update", "--target", target)
    assert repeated.returncode == 0, repeated.stdout + repeated.stderr
    assert "already installed" in repeated.stdout
    assert shell_config.read_bytes() == original_config


@posix
@pytest.mark.parametrize("location", ["--prefix", "--to"])
@pytest.mark.parametrize("valid_checksum", [True, False])
def test_dbt_installer_verifies_download_before_installing(
    tmp_path: Path, dbt_release: tuple[str, Path], location: str, valid_checksum: bool
) -> None:
    if not valid_checksum:
        (dbt_release[1] / "dbt-SHA256SUMS").write_text("corrupt checksums\n")
    destination = tmp_path / "custom install"
    binary = destination / ("bin/dbt-bigquery-adbc" if location == "--prefix" else "dbt-bigquery-adbc")
    result = run_dbt_installer(
        tmp_path,
        dbt_release[0],
        "--update",
        "--version",
        "v1.2.3",
        "--target",
        "linux-x86_64",
        location,
        str(destination),
    )
    if valid_checksum:
        assert result.returncode == 0, result.stdout + result.stderr
        assert binary.is_file()
        assert (destination / "lib/bigquery-adbc/driver.so").exists() == (location == "--prefix")
    else:
        assert result.returncode != 0
        assert "does not match dbt-SHA256SUMS" in result.stdout
        assert not binary.exists()
        assert not (tmp_path / ".bashrc").exists()


@posix
@pytest.mark.parametrize(
    "args",
    [
        ["--udpate"],
        ["--prefix"],
        ["--prefix", ""],
        ["--prefix", "--update"],
        ["--version"],
        ["--target"],
        ["--to"],
        ["--package"],
    ],
)
def test_dbt_installer_rejects_invalid_arguments_before_network_access(tmp_path: Path, args: list[str]) -> None:
    result = run_dbt_installer(tmp_path, "http://127.0.0.1:1/must-not-fetch", *args)
    assert result.returncode != 0
    assert "Unknown option" in result.stdout or "requires a value" in result.stdout
    assert not (tmp_path / ".local").exists()
