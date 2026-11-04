"""Open DuckDB connections with the `adbc` community extension loaded."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

EXTENSION_NAME = "adbc"

# Read from the process environment whenever the extension resolves a
# `profile://` URI.
PROFILE_PATH_VARIABLE = "ADBC_PROFILE_PATH"

# The value types DuckDB accepts in its connection configuration.
DuckDBSetting = str | bool | int | float | list[str]


def use_profile_directory(directory: Path) -> None:
    """Point the driver manager's profile search at `directory` alone."""

    os.environ[PROFILE_PATH_VARIABLE] = str(directory)


def connect(**config: DuckDBSetting) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the extension loaded.

    `INSTALL ... FROM community` is a no-op once the extension is present in
    the extension directory, which the runner shares across a whole run.
    """

    settings: dict[str, DuckDBSetting] = dict(config)
    extension_directory = os.environ.get("ADBC_DUCKDB_EXTENSION_DIRECTORY")
    if extension_directory:
        settings.setdefault("extension_directory", extension_directory)
    connection = duckdb.connect(config=settings)
    try:
        connection.execute(f"INSTALL {EXTENSION_NAME} FROM community")
        connection.execute(f"LOAD {EXTENSION_NAME}")
    except BaseException:
        connection.close()
        raise
    return connection
