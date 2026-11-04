from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
)

from . import bigquery

_SUITE_ROOT = Path(__file__).resolve().parent
_COLLECTION_BASELINE = Path(__file__).parents[1] / "collection_baseline.txt"
_SKIP_BASELINE = Path(__file__).parents[1] / "skip_baseline.txt"
_REGENERATE_COLLECTION = "ADBC_FOUNDRY_REGENERATE_COLLECTION_BASELINE"
_REGENERATE_SKIPS = "ADBC_FOUNDRY_REGENERATE_SKIP_BASELINE"
# The upstream suite skips these as "not implemented"; BigQuery cannot implement them at all.
_KNOWN_SKIP_REASONS = {
    "::test_get_objects_constraints_check[": "BigQuery does not support CHECK constraints",
    "::test_get_objects_constraints_unique[": "BigQuery does not support UNIQUE constraints",
    "::test_temporary[": "BigQuery does not support temporary bulk-ingest tables",
    "::test_temporary_get_objects[": "BigQuery does not support temporary bulk-ingest tables",
}
# Options that narrow, reorder or cut a run short, so the baselines do not apply.
_FILTERING_OPTIONS = (
    "keyword",
    "markexpr",
    "deselect",
    "ignore",
    "ignore_glob",
    "lf",
    "failedfirst",
    "stepwise",
    "maxfail",
    "repl",
    "show_queries",
)


@dataclasses.dataclass
class _CoverageState:
    enabled: bool
    passed: set[str] = dataclasses.field(default_factory=set)
    skipped: set[str] = dataclasses.field(default_factory=set)


_coverage_state: _CoverageState | None = None


def _requested(name: str) -> bool:
    return os.environ.get(name, "").lower() in {"1", "true", "yes"}


def _selects_less_than_the_suite(config: pytest.Config, argument: str) -> bool:
    """Return whether one selected path covers only part of the suite.

    A narrower directory, file, or `::` nodeid disables the coverage guards.
    Selecting the suite directory or any ancestor keeps them enabled.
    """

    path_argument, nodeid_separator, _ = argument.partition("::")
    if nodeid_separator:
        return True
    path = Path(path_argument)
    if not path.is_absolute():
        path = Path(config.invocation_params.dir) / path
    resolved = path.resolve()
    return resolved != _SUITE_ROOT and resolved not in _SUITE_ROOT.parents


def _is_unfiltered(config: pytest.Config) -> bool:
    """Return whether pytest will attempt at least the complete Foundry suite.

    pytest fills config.args from testpaths when no path is given, and an
    explicit path may still select the complete suite.
    """

    if any(
        _selects_less_than_the_suite(config, argument) for argument in config.args
    ):
        return False
    return not any(getattr(config.option, name, None) for name in _FILTERING_OPTIONS)


def _read_baseline(path: Path) -> set[str]:
    if not path.exists():
        regenerate = (
            _REGENERATE_COLLECTION
            if path == _COLLECTION_BASELINE
            else _REGENERATE_SKIPS
        )
        raise pytest.UsageError(
            f"Foundry coverage baseline is missing: {path.name}; "
            f"set {regenerate}=1 "
            "on a reviewed full run to create it"
        )
    return {line for line in path.read_text().splitlines() if line}


def _write_baseline(path: Path, nodeids: set[str]) -> None:
    path.write_text("".join(f"{nodeid}\n" for nodeid in sorted(nodeids)))


def _format_drift(kind: str, added: set[str], removed: set[str]) -> str:
    lines = [
        f"Foundry {kind} baseline drifted; review the changes before regenerating:"
    ]
    lines.extend(f"  + {nodeid}" for nodeid in sorted(added))
    lines.extend(f"  - {nodeid}" for nodeid in sorted(removed))
    return "\n".join(lines)


def _clarify_known_skip_reasons(items: list[pytest.Item]) -> set[str]:
    """Replace the upstream "not implemented" skip reason with BigQuery's limitation.

    Returns the nodeid fragments that matched, so a full run can fail on a stale entry.
    """

    clarified: set[str] = set()
    for item in items:
        match = next(
            (
                (fragment, reason)
                for fragment, reason in _KNOWN_SKIP_REASONS.items()
                if fragment in item.nodeid
            ),
            None,
        )
        if match is None:
            continue
        fragment, reason = match
        for marker in item.iter_markers(name="skip"):
            if marker.kwargs.get("reason") == "not implemented":
                marker.kwargs["reason"] = reason
                clarified.add(fragment)
    return clarified


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    global _coverage_state

    adbc_drivers_validation.tests.conftest.pytest_collection_modifyitems(
        session, config, items
    )
    clarified = _clarify_known_skip_reasons(items)

    if not _is_unfiltered(config):
        requested = [
            name
            for name in (_REGENERATE_COLLECTION, _REGENERATE_SKIPS)
            if _requested(name)
        ]
        if requested:
            raise pytest.UsageError(
                f"{', '.join(requested)} requires an unfiltered Foundry run"
            )
        _coverage_state = _CoverageState(enabled=False)
        return

    if not items:
        raise pytest.UsageError("full Foundry run collected no tests")
    if session.testsfailed:
        raise pytest.UsageError("refusing to check or regenerate Foundry baselines after collection errors")

    collected = {item.nodeid for item in items}
    if not _requested(_REGENERATE_COLLECTION):
        expected = _read_baseline(_COLLECTION_BASELINE)
        added = collected - expected
        removed = expected - collected
        if added or removed:
            raise pytest.UsageError(_format_drift("collection", added, removed))

    # After the collection check, which reports renames in more detail, this catches a pinned
    # suite that keeps a test but changes the skip reason matched above.
    stale = sorted(set(_KNOWN_SKIP_REASONS) - clarified)
    if stale:
        raise pytest.UsageError(
            "these known-BigQuery-limitation skip reasons clarified nothing on a full run, so "
            f"the pinned suite renamed the tests or stopped skipping them: {stale}"
        )

    if config.option.collectonly and _requested(_REGENERATE_SKIPS):
        raise pytest.UsageError(
            f"{_REGENERATE_SKIPS}=1 requires executing the full suite"
        )

    if _requested(_REGENERATE_COLLECTION):
        _write_baseline(_COLLECTION_BASELINE, collected)
    _coverage_state = _CoverageState(enabled=not config.option.collectonly)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _coverage_state is None or not _coverage_state.enabled:
        return
    if report.skipped:
        _coverage_state.skipped.add(report.nodeid)
    elif report.when == "call" and report.passed and not hasattr(
        report, "wasxfail"
    ):
        _coverage_state.passed.add(report.nodeid)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _coverage_state is None or not _coverage_state.enabled:
        return

    errors: list[str] = []
    if not _coverage_state.passed:
        errors.append("full Foundry run had no actual passing tests")

    if exitstatus in {pytest.ExitCode.OK, pytest.ExitCode.TESTS_FAILED}:
        if _requested(_REGENERATE_SKIPS):
            if exitstatus == pytest.ExitCode.OK and _coverage_state.passed:
                _write_baseline(_SKIP_BASELINE, _coverage_state.skipped)
            else:
                errors.append(
                    f"refusing to regenerate {_SKIP_BASELINE.name} because the suite failed"
                )
        else:
            try:
                expected = _read_baseline(_SKIP_BASELINE)
            except pytest.UsageError as error:
                errors.append(str(error))
            else:
                added = _coverage_state.skipped - expected
                removed = expected - _coverage_state.skipped
                if added or removed:
                    errors.append(_format_drift("skip", added, removed))

    if errors:
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_sep("=", "Foundry coverage guard")
            for error in errors:
                reporter.write_line(error)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_addoption(parser: pytest.Parser) -> None:
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)


@pytest.fixture(scope="session")
def driver(
    request: pytest.FixtureRequest,
) -> adbc_drivers_validation.model.DriverQuirks:
    assert request.param == "bigquery:current"
    return bigquery.get_quirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    del driver
    from bigquery_adbc import driver_path

    return driver_path()
