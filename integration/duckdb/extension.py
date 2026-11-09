"""Open DuckDB connections with the `adbc` community extension loaded."""

from __future__ import annotations

import os

import duckdb

EXTENSION_NAME = "adbc"


def connect(**config: str | bool | int | float | list[str]) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the extension loaded.

    `INSTALL ... FROM community` is a no-op once the extension is present in
    the extension directory, which the runner shares across a whole run.
    """
    extension_directory = os.environ.get("ADBC_DUCKDB_EXTENSION_DIRECTORY")
    if extension_directory:
        config.setdefault("extension_directory", extension_directory)
    connection = duckdb.connect(config=config)
    try:
        connection.execute(f"INSTALL {EXTENSION_NAME} FROM community")
        connection.execute(f"LOAD {EXTENSION_NAME}")
    except BaseException:
        connection.close()
        raise
    return connection
