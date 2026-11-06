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

_UNSAFE_NAME = re.compile(r"[^a-z0-9_]+")
_counter = itertools.count()


def _required(variable: str) -> str:
    value = os.environ.get(variable)
    if not value:
        raise RuntimeError(
            f"{variable} is not set; run the suite through "
            "cargo test --test clients duckdb -- --ignored"
        )
    return value


@pytest.fixture(scope="session")
def project() -> str:
    """The BigQuery project that the run queries and bills."""

    return _required("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT")


@pytest.fixture(scope="session")
def dataset() -> str:
    """The run's primary dataset, dropped by the runner when the run ends."""

    return _required("ADBC_BIGQUERY_DATASET")


@pytest.fixture(scope="session")
def secondary_dataset() -> str:
    """A second dataset, for anything that must cross a schema boundary."""

    return _required("ADBC_BIGQUERY_SECONDARY_DATASET")


@pytest.fixture(scope="session")
def driver_library() -> str:
    """The absolute path of the driver cdylib built from this checkout."""

    return profiles.driver_library()


@pytest.fixture(scope="session")
def profile_directory(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The one directory the driver manager searches for profiles."""

    directory = tmp_path_factory.mktemp("adbc-profiles")
    os.environ["ADBC_PROFILE_PATH"] = str(directory)
    return directory


MakeProfile = Callable[..., str]


@pytest.fixture(scope="session")
def make_profile(
    profile_directory: Path, driver_library: str, project: str, dataset: str
) -> MakeProfile:
    """Return a factory writing one connection profile per option set.

    Options default to the run's project and primary dataset; pass an option as
    a keyword to set it, or as `None` to drop a default.
    """

    def _make(
        name_hint: str,
        /,
        driver: str | None = None,
        **options: profiles.ProfileOption | None,
    ) -> str:
        name = f"{_UNSAFE_NAME.sub('_', name_hint.lower())}_{next(_counter)}"
        resolved: dict[str, profiles.ProfileOption | None] = {
            "uri": f"bigquery:///{project}",
            "bigquery.dataset_id": dataset,
        }
        resolved.update(options)
        return profiles.write_profile(
            profile_directory,
            name,
            # A profile key, not an option; in `options` it would reach the driver.
            driver=driver_library if driver is None else driver,
            options={key: value for key, value in resolved.items() if value is not None},
        )

    return _make


@pytest.fixture(scope="session")
def profile(make_profile: MakeProfile) -> str:
    """The profile every test uses unless it is about a driver option."""

    return make_profile("default")


@pytest.fixture
def duck() -> Iterator[duckdb.DuckDBPyConnection]:
    """A fresh in-memory DuckDB connection with the extension loaded.

    One per test: cached catalog entries and `SET` values last as long as the
    connection.
    """

    with closing(extension.connect()) as connection:
        yield connection


@pytest.fixture
def session(duck: duckdb.DuckDBPyConnection, profile: str) -> Session:
    """Run GoogleSQL through `read_adbc()` and `adbc_execute()`."""

    return Session(duck, profile)


@pytest.fixture(scope="session")
def bigquery(profile: str) -> Iterator[Session]:
    """A session-scoped client for building and tearing down test fixtures."""

    with closing(extension.connect()) as connection:
        yield Session(connection, profile)


@pytest.fixture
def attached(duck: duckdb.DuckDBPyConnection, profile: str) -> duckdb.DuckDBPyConnection:
    """A connection with the run's project attached as the `bq` catalog."""

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    return duck


@pytest.fixture
def name(request: pytest.FixtureRequest) -> Callable[[str], str]:
    """Return a factory for BigQuery object names unique to this run.

    Names embed the creating test, so a leaked dataset shows which test owns
    each table.
    """

    stem = _UNSAFE_NAME.sub("_", request.node.name.lower()).strip("_")

    def _name(suffix: str = "t") -> str:
        return f"{stem}_{suffix}_{next(_counter)}"

    return _name
