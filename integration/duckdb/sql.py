"""Build the SQL that reaches BigQuery through the DuckDB ADBC extension.

Every statement nests two dialects: DuckDB, which receives the `profile://` URI
and the GoogleSQL text as single-quoted literals, and GoogleSQL, which delimits
identifiers with backticks.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

# The extension quotes attached table names with the ATTACH-time delimiter pair,
# without escaping. GoogleSQL needs backticks; the default `""` makes a string
# literal and the query fails.
GOOGLESQL_DELIMITER = "``"


def identifier(value: str) -> str:
    """Quote one GoogleSQL identifier."""
    if not value or "`" in value:
        raise ValueError(f"unsafe BigQuery identifier: {value!r}")
    return f"`{value}`"


def qualified(*parts: str) -> str:
    """Quote and join a dotted GoogleSQL path such as project.dataset.table."""
    return ".".join(identifier(part) for part in parts)


def string_literal(value: str) -> str:
    """Quote one GoogleSQL string literal."""
    escaped = (
        value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    )
    return f"'{escaped}'"


def duckdb_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def duckdb_identifier(value: str) -> str:
    if not value:
        raise ValueError("empty DuckDB identifier")
    return '"' + value.replace('"', '""') + '"'


def attached(catalog: str, db_schema: str, table: str) -> str:
    """Name one table of an attached ADBC catalog, in DuckDB's own dialect."""
    return ".".join(duckdb_identifier(part) for part in (catalog, db_schema, table))


def read_adbc(profile: str, query: str, *, columns: Iterable[str] | None = None) -> str:
    projection = "*" if columns is None else ", ".join(columns)
    return f"SELECT {projection} FROM read_adbc({duckdb_literal(profile)}, {duckdb_literal(query)})"


def adbc_execute(profile: str, statement: str) -> str:
    return f"CALL adbc_execute({duckdb_literal(profile)}, {duckdb_literal(statement)})"


def attach(profile: str, alias: str, *, delimiter: str | None = GOOGLESQL_DELIMITER) -> str:
    """Build an `ATTACH`; `delimiter=None` keeps the extension's default `""` pair."""
    options = "TYPE adbc" if delimiter is None else f"TYPE adbc, DELIMITER {duckdb_literal(delimiter)}"
    return f"ATTACH {duckdb_literal(profile)} AS {duckdb_identifier(alias)} ({options})"


class Session:
    """Run GoogleSQL on one profile through a DuckDB connection."""

    def __init__(self, connection: Any, profile: str) -> None:
        self.connection = connection
        self.profile = profile

    def execute(self, statement: str) -> None:
        self.connection.execute(adbc_execute(self.profile, statement))

    def query(self, query: str) -> list[tuple]:
        return self.connection.execute(read_adbc(self.profile, query)).fetchall()

    def scalar(self, query: str) -> Any:
        [(value,)] = self.query(query)
        return value

    def columns(self, project: str, dataset: str, table: str) -> list[tuple]:
        """Return a table's `(column_name, data_type)` pairs in ordinal order."""
        return self.query(
            f"SELECT column_name, data_type FROM {qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = {string_literal(table)} ORDER BY ordinal_position"
        )
