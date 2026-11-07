from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX Bash")
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX Bash")
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX Bash")
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
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "cargo").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ " $* " == *" --list "* ]]; then echo "query::case: test"; exit; fi\n'
        'printf "%s" "$CARGO_OUTPUT"\n'
        'exit "$CARGO_STATUS"\n',
        encoding="utf-8",
    )
    (tools / "cargo").chmod(0o755)

    result = subprocess.run(
        [str(REPOSITORY_ROOT / "scripts/run-one-live-test.sh"), "case", "--nocapture"],
        env={
            **os.environ,
            "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}",
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX process groups")
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX process groups")
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX process groups")
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


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX process groups")
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


@pytest.mark.skipif(sys.platform == "win32" or shutil.which("bash") is None, reason="requires POSIX Bash")
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


@pytest.mark.skipif(sys.platform == "win32" or shutil.which("bash") is None, reason="requires POSIX Bash")
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
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "cargo").write_text(
        "#!/usr/bin/env bash\n"
        '[[ $1 == rustc && " $* " == *" --crate-type cdylib "* ]] || exit 3\n'
        '[[ " $* " == *" --manifest-path $EXPECTED_MANIFEST "* && " $* " == *" --release "* ]] || exit 3\n'
        "printf 'macro output\\n\\n'\n"
        'printf "%s\\n" "$CARGO_MESSAGES"\n'
        "[[ $CARGO_MESSAGES == *'\"success\":true'* ]] || exit 101\n",
        encoding="utf-8",
    )
    (tools / "cargo").chmod(0o755)
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
            "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}",
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
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "cargo").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ $1 == -V ]]; then echo "cargo 1.0.0"; exit; fi\n'
        "[[ $EXPECT_BUILD == 1 ]] || exit 3\n"
        '[[ $1 == rustc && " $* " == *" --crate-type cdylib "* ]] || exit 3\n'
        f'[[ " $* " == *" --lib "* && " $* " == *" --profile {profile} "* ]] || exit 3\n'
        'printf "%s\\n" "$CARGO_MESSAGE"\n',
        encoding="utf-8",
    )
    (tools / "rustc").write_text(
        "#!/usr/bin/env bash\nprintf 'release: 1.0.0\\nhost: x86_64-unknown-linux-gnu\\ncommit-hash: unknown\\n'\n",
        encoding="utf-8",
    )
    for tool in tools.iterdir():
        tool.chmod(0o755)

    command = [str(REPOSITORY_ROOT / "scripts/report-binary-size.sh"), "--profile", profile, "--top", "1"]
    if not build:
        command.extend(["--artifact", str(library)])
    result = subprocess.run(
        command,
        env={
            **os.environ,
            "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}",
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
