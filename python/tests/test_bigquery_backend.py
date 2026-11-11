from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from bigquery_backend import REST_ENDPOINT, STORAGE_ENDPOINT, emulator_options, integration_project


@pytest.mark.parametrize("value", [None, "", "project/dataset"])
def test_live_fixture_requires_project_before_connecting(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    import test_bigquery

    variable = "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"
    monkeypatch.delenv(variable, raising=False)
    if value is not None:
        monkeypatch.setenv(variable, value)
    monkeypatch.setenv("ADBC_BIGQUERY_RUN_INTEGRATION", "1")
    fixture = test_bigquery.bigquery_dataset.__wrapped__()
    with pytest.raises(ValueError, match=variable):
        next(fixture)


def test_project_selection_uses_only_the_explicit_variable() -> None:
    assert integration_project({"BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": "chosen-project"}) == "chosen-project"
    with pytest.raises(ValueError, match="BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"):
        integration_project({"ADBC_BIGQUERY_PROJECT": "old-project", "GOOGLE_CLOUD_PROJECT": "ambient-project"})


def test_default_backend_keeps_real_bigquery_and_adc() -> None:
    assert emulator_options({}) == {}
    assert emulator_options({"BIGQUERY_EMULATOR_HOST": "localhost:9050"}) == {}


def test_emulator_overrides_every_service_and_ignores_ambient_credentials() -> None:
    assert emulator_options(
        {
            REST_ENDPOINT: "http://127.0.0.1:9050/",
            STORAGE_ENDPOINT: "http://[::1]:9060",
            "GOOGLE_APPLICATION_CREDENTIALS": "/credentials/never-read.json",
        }
    ) == {
        "bigquery.auth_type": "anonymous",
        "bigquery.endpoint": "http://127.0.0.1:9050",
        "bigquery.storage_endpoint": "http://[::1]:9060",
        "bigquery.gcs_endpoint": "http://127.0.0.1:9050",
        "bigquery.dataproc_endpoint": "http://127.0.0.1:9050",
        "bigquery.vertex_endpoint": "http://127.0.0.1:9050",
    }


@pytest.mark.parametrize(
    "environment",
    [
        {REST_ENDPOINT: "http://localhost:9050"},
        {STORAGE_ENDPOINT: "http://localhost:9060"},
        {REST_ENDPOINT: "", STORAGE_ENDPOINT: ""},
        {REST_ENDPOINT: "", STORAGE_ENDPOINT: "http://localhost:9060"},
        {REST_ENDPOINT: "http://localhost:9050", STORAGE_ENDPOINT: ""},
    ],
)
def test_partial_or_empty_emulator_configuration_fails(environment: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="set both"):
        emulator_options(environment)


@pytest.mark.parametrize("name", [REST_ENDPOINT, STORAGE_ENDPOINT])
@pytest.mark.parametrize(
    "endpoint",
    [
        "localhost:9050",
        "ftp://localhost:9050",
        "http://",
        "http://localhost:bad",
        "http://localhost:65536",
        "http://localhost/path",
        "http://localhost//",
        "http://localhost?query",
        "http://localhost#fragment",
        "http://user:password@localhost",
        "http://local host",
        "http://localhost\n",
    ],
)
def test_invalid_emulator_endpoint_fails(name: str, endpoint: str) -> None:
    environment = {REST_ENDPOINT: "http://localhost:9050", STORAGE_ENDPOINT: "http://localhost:9060"}
    environment[name] = endpoint
    with pytest.raises(ValueError, match=name):
        emulator_options(environment)


@pytest.mark.parametrize("emulator", [False, True])
def test_runner_preflight_reports_backend_without_loading_the_driver(emulator: bool) -> None:
    environment = {key: value for key, value in os.environ.items() if key not in {REST_ENDPOINT, STORAGE_ENDPOINT}}
    if emulator:
        environment |= {REST_ENDPOINT: "http://localhost:9050", STORAGE_ENDPOINT: "http://localhost:9060"}
    result = subprocess.run(
        [sys.executable, "-I", str(Path(__file__).with_name("bigquery_backend.py"))],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ("emulator" if emulator else "bigquery")


@pytest.mark.skipif(sys.platform == "win32" or shutil.which("bash") is None, reason="requires POSIX Bash")
@pytest.mark.parametrize("backend", ["bigquery", "emulator", "partial"])
def test_python_runner_only_requires_adc_for_real_bigquery(tmp_path: Path, backend: str) -> None:
    tools = tmp_path / "bin"
    tools.mkdir()
    credential_log = tmp_path / "credential-check"
    (tools / "gcloud").write_text('#!/bin/sh\nprintf checked > "$PREFLIGHT_CREDENTIAL_LOG"\n', encoding="utf-8")
    # Stop at the wheel build: preflight is complete, and nothing is installed.
    (tools / "uv").write_text("#!/bin/sh\nexit 42\n", encoding="utf-8")
    for tool in tools.iterdir():
        tool.chmod(0o755)
    environment = {key: value for key, value in os.environ.items() if key not in {REST_ENDPOINT, STORAGE_ENDPOINT}}
    environment |= {
        "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}",
        "BIGQUERY_ADBC_PYTHON": sys.executable,
        "PREFLIGHT_CREDENTIAL_LOG": str(credential_log),
        "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT": "test-project",
        "TMPDIR": str(tmp_path),
    }
    if backend != "bigquery":
        environment[REST_ENDPOINT] = "http://localhost:9050"
    if backend == "emulator":
        environment[STORAGE_ENDPOINT] = "http://localhost:9060"
    result = subprocess.run(
        [str(Path(__file__).resolve().parents[2] / "scripts/run-python-tests.sh")],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if backend == "partial":
        assert result.returncode == 1
        assert "set both" in result.stderr
    else:
        assert result.returncode == 42, result.stderr
    assert credential_log.exists() == (backend == "bigquery")


@pytest.mark.parametrize(
    "labels",
    [
        {},
        {"bigquery_adbc": "another_suite", "run_id": "run_1"},
        {"bigquery_adbc": "python_tests", "run_id": "another_run"},
        {"bigquery_adbc": "python_tests", "run_id": "run_1"},
    ],
)
def test_cleanup_requires_both_ownership_labels(monkeypatch: pytest.MonkeyPatch, labels: dict[str, str]) -> None:
    import test_bigquery

    statements: list[tuple[str, str]] = []
    sql_labels = "[" + ", ".join(f'STRUCT("{key}", "{value}")' for key, value in labels.items()) + "]"
    monkeypatch.setattr(test_bigquery, "_dataset_exists", lambda project, dataset: True)
    monkeypatch.setattr(test_bigquery, "_dataset_label_value", lambda project, dataset: sql_labels)
    monkeypatch.setattr(test_bigquery, "_execute_update", lambda project, sql: statements.append((project, sql)))
    if labels == {"bigquery_adbc": "python_tests", "run_id": "run_1"}:
        test_bigquery._cleanup_dataset("test-project", "bigquery_adbc_py_run_1", "run_1")
        assert statements == [("test-project", "DROP SCHEMA `test-project.bigquery_adbc_py_run_1` CASCADE")]
    else:
        with pytest.raises(RuntimeError, match="ownership labels do not match"):
            test_bigquery._cleanup_dataset("test-project", "bigquery_adbc_py_run_1", "run_1")
        assert statements == []


def test_cleanup_of_a_dataset_that_does_not_exist_does_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    import test_bigquery

    def unexpected(*args: object) -> None:
        raise AssertionError(f"unexpected call {args!r}")

    monkeypatch.setattr(test_bigquery, "_dataset_exists", lambda project, dataset: False)
    monkeypatch.setattr(test_bigquery, "_dataset_label_value", unexpected)
    monkeypatch.setattr(test_bigquery, "_execute_update", unexpected)
    test_bigquery._cleanup_dataset("test-project", "bigquery_adbc_py_run_1", "run_1")


@pytest.mark.parametrize("create_fails", [False, True])
def test_live_fixture_owns_its_dataset_before_creating_it(monkeypatch: pytest.MonkeyPatch, create_fails: bool) -> None:
    # A CREATE that fails on the client can still land in BigQuery, so cleanup runs either way.
    import test_bigquery

    failure = RuntimeError("lost response")
    cleanups: list[tuple[str, str, str]] = []

    def execute_update(project: str, sql: str) -> int:
        if create_fails:
            raise failure
        return 0

    monkeypatch.setenv("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT", "test-project")
    monkeypatch.setenv("ADBC_BIGQUERY_RUN_INTEGRATION", "1")
    monkeypatch.setattr(test_bigquery, "_execute_update", execute_update)
    monkeypatch.setattr(test_bigquery, "_cleanup_dataset", lambda *args: cleanups.append(args))
    fixture = test_bigquery.bigquery_dataset.__wrapped__()

    if create_fails:
        with pytest.raises(RuntimeError) as caught:
            next(fixture)
        assert caught.value is failure
    else:
        project, dataset = next(fixture)
        assert cleanups == []
        fixture.close()
    [(project, dataset, run_label)] = cleanups
    assert project == "test-project"
    assert dataset == "bigquery_adbc_py_" + run_label
