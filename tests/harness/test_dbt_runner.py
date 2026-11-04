"""Regression tests for dbt runs that could otherwise report success without valid coverage."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar
from unittest import mock

from run import (
    EPHEMERAL_UNIQUE_ID,
    assert_nodes_ran,
    assert_python_model_submission,
    checked_phase_command,
    dbt_command,
    failing_dbt_command,
)

import benchmark


class BenchmarkDriverSelectionTest(unittest.TestCase):
    def test_modes_override_inherited_opt_in_and_isolate_caches(self) -> None:
        caches = set()
        for inherited in ("1", "0"):
            for mode in ("foundry", "local", "upstream"):
                with (
                    self.subTest(inherited=inherited, mode=mode),
                    mock.patch.dict(os.environ, {"DBT_ADBC_BIGQUERY_USE_FOUNDRY": inherited}),
                ):
                    env = benchmark.mode_environment(
                        mode,
                        project="project",
                        location="US",
                        dataset="dataset",
                        threads=8,
                        run_id="run",
                    )
                    if mode == "upstream":
                        self.assertNotIn("DBT_ADBC_BIGQUERY_USE_FOUNDRY", env)
                    else:
                        self.assertEqual(env["DBT_ADBC_BIGQUERY_USE_FOUNDRY"], "1")
                    caches.add(env["XDG_CACHE_HOME"])
        self.assertEqual(len(caches), 3)

    def test_resolved_driver_checks_channel_version_and_binary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            log_path = cache / "dbt.log"
            library = cache / f"libadbc_driver_bigquery_foundry-{benchmark.local_driver.version()}.so"
            library.write_bytes(b"original driver")
            marker = f"BigQuery driver resolved: bigquery_foundry {benchmark.local_driver.version()}"
            log_path.write_text(f"[debug]: {marker}\n[debug]: {marker}\n")
            artifact = benchmark.resolved_driver("local", log_path, cache)
            self.assertEqual(artifact.path, str(library))
            with self.assertRaisesRegex(RuntimeError, "unexpected upstream driver resolution"):
                benchmark.resolved_driver("upstream", log_path, cache)
            with mock.patch("benchmark.performance_cache", return_value=cache):
                benchmark.assert_driver_unchanged("local", log_path, artifact)
                library.write_bytes(b"replacement driver")
                with self.assertRaisesRegex(RuntimeError, "changed the local driver"):
                    benchmark.assert_driver_unchanged("local", log_path, artifact)
            log_path.write_text("BigQuery driver resolved: bigquery_foundry other-version\n")
            with self.assertRaisesRegex(RuntimeError, "unexpected local driver resolution"):
                benchmark.resolved_driver("local", log_path, cache)

    def test_missing_or_mixed_resolution_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            log_path = cache / "dbt.log"
            for content in (
                "no driver was loaded",
                "BigQuery driver resolved: bigquery_foundry 1\nBigQuery driver resolved: bigquery_foundry 2\n",
            ):
                with self.subTest(content=content):
                    log_path.write_text(content)
                    with self.assertRaisesRegex(RuntimeError, "expected one foundry driver resolution"):
                        benchmark.resolved_driver("foundry", log_path, cache)


class MacroCompletionTest(unittest.TestCase):
    COMPLETION = "adapter surface checks passed for "
    SUCCESS = "12:00:00.123456 [info ]: adapter surface checks passed for `p`.`d`.`fct_orders`\n"

    def test_requires_a_current_info_log_with_or_without_driver_diagnostics(self) -> None:
        cases = (
            (None, None, False),
            (self.SUCCESS, "12:00:01 [info ]: finished parsing\n", False),
            (None, self.SUCCESS.replace("[info ]", "[debug]"), False),
            (None, self.SUCCESS, True),
            ("previous invocation: café\n" + self.SUCCESS, self.SUCCESS, True),
        )
        for diagnostics in (None, ()):
            for before, appended, succeeds in cases:
                with (
                    self.subTest(diagnostics=diagnostics, before=before, appended=appended),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    log_path = Path(directory) / "dbt.log"
                    if before is not None:
                        log_path.write_text(before, encoding="utf-8")
                    script = "from pathlib import Path\n"
                    if appended is not None:
                        script += (
                            f"with Path({str(log_path)!r}).open('a', encoding='utf-8') as log:\n"
                            f"    log.write({appended!r})\n"
                        )
                    with mock.patch("run.make_dbt_command", return_value=[sys.executable, "-c", script]):
                        arguments = {
                            "target_dir": Path(directory),
                            "log_dir": Path(directory),
                            "required_events": diagnostics,
                            "completion": self.COMPLETION,
                        }
                        if succeeds:
                            dbt_command("run-operation", "assert_adapter_surface", **arguments)
                        else:
                            with self.assertRaisesRegex(RuntimeError, "assertions may not have executed"):
                                dbt_command("run-operation", "assert_adapter_surface", **arguments)


class NodesRanTest(unittest.TestCase):
    """Successful exit alone must not hide lost, skipped or failed coverage."""

    NODES: ClassVar[list[str]] = [
        "model.project.orders",
        "seed.project.customers",
        "snapshot.project.customers",
        "test.project.unique_orders.hash",
        "unit_test.project.orders.filters_cancelled",
        "source.project.raw.orders",
    ]
    RESULTS: ClassVar[list[dict]] = [
        {"unique_id": node, "status": status}
        for node, status in zip(NODES, ("success", "success", "success", "pass", "pass", "Pass"), strict=True)
    ]

    def _path(self, directory: str, artifact: object) -> Path:
        path = Path(directory) / "run_results.json"
        path.write_text(json.dumps(artifact))
        return path

    def test_counts_the_nodes_a_phase_built(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._path(directory, {"results": list(reversed(self.RESULTS))})
            self.assertEqual(assert_nodes_ran(path, after="build", expected_nodes=self.NODES), len(self.NODES))

    def test_rejects_missing_unexpected_and_duplicate_nodes(self) -> None:
        cases = (
            self.RESULTS[:-1],
            [*self.RESULTS, {"unique_id": "model.project.unexpected", "status": "success"}],
            [*self.RESULTS, self.RESULTS[0]],
        )
        for results in cases:
            with tempfile.TemporaryDirectory() as directory, self.subTest(results=results):
                path = self._path(directory, {"results": results})
                with self.assertRaisesRegex(RuntimeError, "coverage changed|duplicate results"):
                    assert_nodes_ran(path, after="build", expected_nodes=self.NODES)

    def test_only_the_known_ephemeral_model_can_be_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._path(directory, {"results": [{"unique_id": EPHEMERAL_UNIQUE_ID, "status": "skipped"}]})
            self.assertEqual(assert_nodes_ran(path, after="build", expected_nodes=[EPHEMERAL_UNIQUE_ID]), 1)

    def test_rejects_skipped_or_failed_nodes(self) -> None:
        for index, result in enumerate(self.RESULTS):
            for status in ("skipped", "warn", "error", "fail"):
                with tempfile.TemporaryDirectory() as directory, self.subTest(node=result["unique_id"], status=status):
                    results = list(self.RESULTS)
                    results[index] = result | {"status": status}
                    path = self._path(directory, {"results": results})
                    with self.assertRaisesRegex(RuntimeError, "status is"):
                        assert_nodes_ran(path, after="build", expected_nodes=self.NODES)

    def test_rejects_a_phase_that_selected_nothing(self) -> None:
        for artifact in ({"results": []}, {}, {"results": None}):
            with tempfile.TemporaryDirectory() as directory, self.subTest(artifact=artifact):
                path = self._path(directory, artifact)
                with self.assertRaisesRegex(RuntimeError, "selected no dbt nodes"):
                    assert_nodes_ran(path, after="Rebuild seeds", expected_nodes=self.NODES)


class PhaseArtifactTest(unittest.TestCase):
    def test_expected_failure_cannot_reuse_a_previous_submission_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            artifact = target / "run_results.json"
            artifact.write_text(
                json.dumps(
                    {
                        "results": [
                            {
                                "unique_id": "model.project.python_model",
                                "status": "error",
                                "message": "failed to submit the BigQuery Dataproc job",
                            }
                        ]
                    }
                )
            )
            with mock.patch("run.subprocess.run", return_value=subprocess.CompletedProcess(["dbt"], 1)):
                failing_dbt_command("build", target_dir=target, log_dir=target)
            with self.assertRaisesRegex(RuntimeError, "failed to read"):
                assert_python_model_submission(artifact, unique_id="model.project.python_model")

    def test_requires_new_artifacts_and_preserves_phase_results(self) -> None:
        for arguments, filename, node, status in (
            (("build",), "run_results.json", "model.project.orders", "success"),
            (("seed",), "run_results.json", "seed.project.orders", "success"),
            (("source", "freshness"), "sources.json", "source.project.raw.orders", "Pass"),
        ):
            for outcome in ("success", "no artifact", "wrong selection", "command failure"):
                self._case(arguments, filename, node, status, outcome)

    def _case(self, arguments: tuple[str, ...], filename: str, node: str, status: str, outcome: str) -> None:
        with tempfile.TemporaryDirectory() as directory, self.subTest(command=arguments, outcome=outcome):
            target = Path(directory)
            artifact = target / filename
            saved = target / "phases" / "phase" / filename
            valid = {"results": [{"unique_id": node, "status": status}]}
            artifact.write_text(json.dumps(valid))
            saved.parent.mkdir(parents=True)
            saved.write_text(json.dumps(valid))

            def command(*args: object, **kwargs: object) -> None:
                self.assertFalse(artifact.exists())
                self.assertFalse(saved.exists())
                if outcome == "no artifact":
                    return
                content = valid if outcome == "success" else {"results": []}
                artifact.write_text(json.dumps(content))
                if outcome == "command failure":
                    raise subprocess.CalledProcessError(1, ["dbt"])

            def check() -> int:
                return checked_phase_command(
                    *arguments,
                    target_dir=target,
                    log_dir=target,
                    coverage="phase",
                    expected_nodes=[node],
                )

            with mock.patch("run.dbt_command", side_effect=command):
                if outcome == "success":
                    self.assertEqual(check(), 1)
                else:
                    with self.assertRaises((RuntimeError, subprocess.CalledProcessError)):
                        check()
            if outcome == "no artifact":
                self.assertFalse(saved.exists())
            else:
                self.assertEqual(saved.read_text(), artifact.read_text())


if __name__ == "__main__":
    unittest.main()
