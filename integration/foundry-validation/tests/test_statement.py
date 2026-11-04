from __future__ import annotations

from adbc_drivers_validation.tests.statement import TestStatement, generate_tests  # noqa: F401

from . import bigquery


def pytest_generate_tests(metafunc) -> None:
    generate_tests([bigquery.get_quirks()], metafunc)
