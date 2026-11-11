"""Fixtures wiring DuckDB, the `adbc` extension, and this checkout's driver.

The runner creates the run's two BigQuery datasets and exports them with the
built driver library. Profiles go into one session-scoped directory on
`ADBC_PROFILE_PATH`, so a test needing a driver option writes another profile.
"""

from __future__ import annotations

import itertools
import os
import re
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path

import duckdb
import extension
import profiles
import pytest
import sql
from sql import Session

ATTACHED_CATALOG = "bq"

MakeProfile = Callable[..., str]

_UNSAFE_NAME = re.compile(r"[^a-z0-9_]+")
_counter = itertools.count()


def _required(variable: str) -> str:
    value = os.environ.get(variable)
    if not value:
        raise RuntimeError(
            f"{variable} is not set; run the suite through cargo test --test clients duckdb -- --ignored"
        )
    return value


@pytest.fixture(scope="session")
def project() -> str:
    return _required("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT")


@pytest.fixture(scope="session")
def dataset() -> str:
    """Dropped by the runner when the run ends."""
    return _required("ADBC_BIGQUERY_DATASET")


@pytest.fixture(scope="session")
def secondary_dataset() -> str:
    """For anything that must cross a schema boundary."""
    return _required("ADBC_BIGQUERY_SECONDARY_DATASET")


@pytest.fixture(scope="session")
def driver_library() -> str:
    """The built cdylib; the extension loads the driver by absolute path."""
    path = Path(_required("ADBC_BIGQUERY_LIBRARY"))
    if not path.is_file():
        raise RuntimeError(f"ADBC_BIGQUERY_LIBRARY is not a file: {path}")
    return str(path.resolve())


@pytest.fixture(scope="session")
def profile_directory(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("adbc-profiles")
    os.environ["ADBC_PROFILE_PATH"] = str(directory)
    return directory


@pytest.fixture(scope="session")
def make_profile(profile_directory: Path, driver_library: str, project: str, dataset: str) -> MakeProfile:
    """Write a profile for the run's project and dataset; an option passed as `None` drops a default."""

    def make(name_hint: str, /, **options: profiles.ProfileOption | None) -> str:
        name = f"{_UNSAFE_NAME.sub('_', name_hint.lower())}_{next(_counter)}"
        resolved = {"uri": f"bigquery:///{project}", "bigquery.dataset_id": dataset, **options}
        return profiles.write_profile(
            profile_directory,
            name,
            driver=driver_library,
            options={key: value for key, value in resolved.items() if value is not None},
        )

    return make


@pytest.fixture(scope="session")
def profile(make_profile: MakeProfile) -> str:
    return make_profile("default")


@pytest.fixture
def duck() -> Iterator[duckdb.DuckDBPyConnection]:
    """One per test: cached catalog entries and `SET` values last as long as the connection."""
    with closing(extension.connect()) as connection:
        yield connection


@pytest.fixture
def session(duck: duckdb.DuckDBPyConnection, profile: str) -> Session:
    return Session(duck, profile)


@pytest.fixture(scope="session")
def bigquery(profile: str) -> Iterator[Session]:
    """A session-scoped client for building test fixtures."""
    with closing(extension.connect()) as connection:
        yield Session(connection, profile)


@pytest.fixture
def attached(duck: duckdb.DuckDBPyConnection, profile: str) -> duckdb.DuckDBPyConnection:
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    return duck


@pytest.fixture
def name(request: pytest.FixtureRequest) -> Callable[[str], str]:
    """Return a factory for unique BigQuery object names that embed the creating test."""
    stem = _UNSAFE_NAME.sub("_", request.node.name.lower()).strip("_")
    return lambda suffix="t": f"{stem}_{suffix}_{next(_counter)}"
