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
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f"'{escaped}'"


def duckdb_literal(value: str) -> str:
    """Quote one DuckDB string literal."""

    return "'" + value.replace("'", "''") + "'"


def duckdb_identifier(value: str) -> str:
    """Quote one DuckDB identifier."""

    if not value:
        raise ValueError("empty DuckDB identifier")
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def attached(catalog: str, db_schema: str, table: str) -> str:
    """Name one table of an attached ADBC catalog, in DuckDB's own dialect."""

    return ".".join(duckdb_identifier(part) for part in (catalog, db_schema, table))


def read_adbc(profile: str, query: str, *, columns: Iterable[str] | None = None) -> str:
    """Build a DuckDB `read_adbc()` scan of one GoogleSQL query."""

    projection = "*" if columns is None else ", ".join(columns)
    return (
        f"SELECT {projection} FROM "
        f"read_adbc({duckdb_literal(profile)}, {duckdb_literal(query)})"
    )


def adbc_execute(profile: str, statement: str) -> str:
    """Build a DuckDB `adbc_execute()` call running one GoogleSQL statement."""

    return f"CALL adbc_execute({duckdb_literal(profile)}, {duckdb_literal(statement)})"


def attach(profile: str, alias: str, *, delimiter: str | None = GOOGLESQL_DELIMITER) -> str:
    """Build an `ATTACH` of one profile as a DuckDB catalog.

    `delimiter=None` keeps the extension's default `""` pair.
    """

    options = ["TYPE adbc"]
    if delimiter is not None:
        options.append(f"DELIMITER {duckdb_literal(delimiter)}")
    return (
        f"ATTACH {duckdb_literal(profile)} AS {duckdb_identifier(alias)} "
        f"({', '.join(options)})"
    )


class Session:
    """A DuckDB connection that reaches one BigQuery project as a client."""

    def __init__(self, connection: Any, profile: str) -> None:
        self.connection = connection
        self.profile = profile

    def execute(self, statement: str) -> None:
        """Run one GoogleSQL statement through `adbc_execute()`."""

        self.connection.execute(adbc_execute(self.profile, statement))

    def query(self, query: str) -> list[tuple]:
        """Run one GoogleSQL query through `read_adbc()` and fetch every row."""

        return self.connection.execute(read_adbc(self.profile, query)).fetchall()

    def scalar(self, query: str) -> Any:
        """Run one GoogleSQL query expected to produce a single value."""

        rows = self.query(query)
        if len(rows) != 1 or len(rows[0]) != 1:
            raise AssertionError(f"expected exactly one value, got {rows!r}")
        return rows[0][0]
