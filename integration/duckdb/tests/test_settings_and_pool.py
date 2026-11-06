"""The extension's settings, connection pool, and concurrent reads and writes.

Each pooled connection owns an independent `AdbcDatabase`; `read_adbc()` opens
another connection outside the pool. The extension exposes no connection counts
or scheduling, so tests assert only completion and correct rows.
"""

from __future__ import annotations

import concurrent.futures
from collections.abc import Callable
from contextlib import closing

import duckdb
import extension
import pytest
import sql
from sql import Session

from tests.conftest import ATTACHED_CATALOG

# `duckdb_settings()` reports every value as a string, alongside the type
# DuckDB parses it as.
SETTING_DEFAULTS = {
    "adbc_connection_pool_size": ("50", "BIGINT"),
    "adbc_insert_buffer_size": ("1000", "BIGINT"),
    "adbc_materialize_insert_rows": ("false", "BOOLEAN"),
    "adbc_mix_reads_writes": ("false", "BOOLEAN"),
}

# One non-default value per setting, for the SET/RESET round trip.
SETTING_OVERRIDES = {
    "adbc_connection_pool_size": "4",
    "adbc_insert_buffer_size": "16",
    "adbc_materialize_insert_rows": "true",
    "adbc_mix_reads_writes": "true",
}

# Spans several of the extension's 2048-row input chunks.
MULTI_CHUNK_ROWS = 5000
MULTI_CHUNK_SUM = MULTI_CHUNK_ROWS * (MULTI_CHUNK_ROWS - 1) // 2


def _setting(connection: duckdb.DuckDBPyConnection, name: str) -> str:
    rows = connection.execute(
        "SELECT value FROM duckdb_settings() WHERE name = ?", [name]
    ).fetchall()
    assert len(rows) == 1, f"{name} is not a known setting: {rows!r}"
    return rows[0][0]


def _counted_table(
    bigquery: Session, project: str, dataset: str, table: str, rows: int
) -> None:
    """Create a BigQuery table holding `i` = 1..rows.

    Call it before ATTACH: the extension caches catalog tables on first lookup.
    """

    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, table)} AS "
        f"SELECT n AS i FROM UNNEST(GENERATE_ARRAY(1, {rows})) AS n"
    )


def test_settings_report_their_documented_defaults(duck: duckdb.DuckDBPyConnection) -> None:
    rows = duck.execute(
        "SELECT name, value, input_type FROM duckdb_settings() "
        "WHERE name IN ('adbc_connection_pool_size', 'adbc_insert_buffer_size', "
        "'adbc_materialize_insert_rows', 'adbc_mix_reads_writes')"
    ).fetchall()
    assert {name: (value, kind) for name, value, kind in rows} == SETTING_DEFAULTS


def test_each_setting_takes_a_value_and_returns_to_its_default(
    duck: duckdb.DuckDBPyConnection,
) -> None:
    for name, expected in SETTING_OVERRIDES.items():
        duck.execute(f"SET {name} = {expected}")
        assert _setting(duck, name) == expected
        duck.execute(f"RESET {name}")
        assert _setting(duck, name) == SETTING_DEFAULTS[name][0]


def test_a_pool_of_one_serves_repeated_scans(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """The pool is lazy and never evicts, so repeated scans reuse one connection."""

    table = name("scan")
    _counted_table(bigquery, project, dataset, table, 5)

    duck.execute("SET adbc_connection_pool_size = 1")
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    scan = f"SELECT COUNT(*), SUM(i) FROM {sql.attached(ATTACHED_CATALOG, dataset, table)}"
    for _ in range(3):
        assert duck.execute(scan).fetchall() == [(5, 15)]


def test_a_pool_of_one_serves_several_scans_in_one_statement(
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """A statement needing more connections than the pool holds still runs.

    The extension falls back to ephemeral connections when the pool is exhausted.
    """

    left = name("left")
    right = name("right")
    _counted_table(bigquery, project, dataset, left, 5)
    _counted_table(bigquery, project, dataset, right, 5)

    with closing(extension.connect(threads=4)) as connection:
        connection.execute("SET adbc_connection_pool_size = 1")
        connection.execute(sql.attach(profile, ATTACHED_CATALOG))
        left_table = sql.attached(ATTACHED_CATALOG, dataset, left)
        right_table = sql.attached(ATTACHED_CATALOG, dataset, right)

        joined = connection.execute(
            f"SELECT COUNT(*), SUM(x.i) FROM {left_table} x "
            f"JOIN {right_table} y ON x.i = y.i"
        ).fetchall()
        assert joined == [(5, 15)]

        unioned = connection.execute(
            "SELECT COUNT(*), SUM(i) FROM ("
            f"SELECT i FROM {left_table} UNION ALL SELECT i FROM {right_table}"
            ")"
        ).fetchall()
        assert unioned == [(10, 30)]


def test_a_larger_pool_scans_several_tables_across_worker_threads(
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Four scans on four workers, each with its own pooled connection and client."""

    tables = [name(f"part{index}") for index in range(4)]
    for index, table in enumerate(tables, start=1):
        _counted_table(bigquery, project, dataset, table, index)

    with closing(extension.connect(threads=4)) as connection:
        connection.execute("SET adbc_connection_pool_size = 8")
        connection.execute(sql.attach(profile, ATTACHED_CATALOG))
        branches = " UNION ALL ".join(
            f"SELECT i FROM {sql.attached(ATTACHED_CATALOG, dataset, table)}"
            for table in tables
        )
        # Rows: 1+2+3+4. Sum: 1 + (1+2) + (1+2+3) + (1+2+3+4).
        assert connection.execute(
            f"SELECT COUNT(*), SUM(i) FROM ({branches})"
        ).fetchall() == [(10, 20)]


def test_many_read_adbc_calls_in_one_statement(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """`read_adbc()` bypasses the pool, so six scans initialize six databases."""

    branches = " UNION ALL ".join(
        f"({sql.read_adbc(profile, f'SELECT {index} AS i')})" for index in range(1, 7)
    )
    assert duck.execute(f"SELECT COUNT(*), SUM(i) FROM ({branches})").fetchall() == [(6, 21)]


def test_parallel_duckdb_connections_share_one_profile(profile: str) -> None:
    """Six DuckDB connections query through one profile at once.

    DuckDB connections are not thread-safe, so each worker opens its own and
    drives its own driver instance from the same loaded library.
    """

    def run(index: int) -> list[tuple]:
        with closing(extension.connect()) as connection:
            return connection.execute(
                sql.read_adbc(profile, f"SELECT {index} AS i")
            ).fetchall()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(run, range(6)))
    assert results == [[(index,)] for index in range(6)]


def test_a_read_and_a_write_run_concurrently_on_one_dataset(
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Two driver instances run a query and an ingest against one dataset at once.

    They touch different tables, so the results do not depend on ordering.
    """

    source = name("source")
    destination = name("destination")
    _counted_table(bigquery, project, dataset, source, 10)
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, destination)} (i BIGINT)"
    )

    def read() -> list[tuple]:
        with closing(extension.connect()) as connection:
            return connection.execute(
                sql.read_adbc(
                    profile,
                    f"SELECT SUM(i) AS total FROM {sql.qualified(project, dataset, source)}",
                )
            ).fetchall()

    def write() -> list[tuple]:
        with closing(extension.connect()) as connection:
            connection.execute(sql.attach(profile, ATTACHED_CATALOG))
            return connection.execute(
                f"INSERT INTO {sql.attached(ATTACHED_CATALOG, dataset, destination)} "
                "SELECT i FROM range(1, 6) t(i)"
            ).fetchall()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        reader = pool.submit(read)
        writer = pool.submit(write)
        assert reader.result() == [(55,)]
        assert writer.result() == [(5,)]

    assert bigquery.query(
        f"SELECT COUNT(*), SUM(i) FROM {sql.qualified(project, dataset, destination)}"
    ) == [(5, 15)]


def test_mixing_a_read_and_a_write_needs_the_setting(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """`INSERT ... SELECT FROM read_adbc()` needs `adbc_mix_reads_writes`.

    The extension cannot know the target's isolation guarantees, so it refuses
    the statement by default.
    """

    source = name("source")
    destination = name("destination")
    _counted_table(bigquery, project, dataset, source, 4)
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, destination)} (i INT64)"
    )

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    statement = (
        f"INSERT INTO {sql.attached(ATTACHED_CATALOG, dataset, destination)} "
        + sql.read_adbc(profile, f"SELECT i FROM {sql.qualified(project, dataset, source)}")
    )

    with pytest.raises(
        duckdb.NotImplementedException, match="mixes ADBC reads and writes"
    ):
        duck.execute(statement)
    # The refusal is a plan-time check, so nothing reached BigQuery.
    assert bigquery.query(
        f"SELECT COUNT(*) FROM {sql.qualified(project, dataset, destination)}"
    ) == [(0,)]

    duck.execute("SET adbc_mix_reads_writes = true")
    assert duck.execute(statement).fetchall() == [(4,)]
    assert bigquery.query(
        f"SELECT COUNT(*), SUM(i) FROM {sql.qualified(project, dataset, destination)}"
    ) == [(4, 10)]


def test_an_insert_buffer_of_one_chunk_still_inserts_every_row(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Flushing after every 2048-row chunk must not drop or duplicate rows.

    At the default of 1000 chunks this insert is one flush; at 1 it is several.
    """

    table = name("buffered")
    bigquery.execute(f"CREATE TABLE {sql.qualified(project, dataset, table)} (i BIGINT)")

    duck.execute("SET adbc_insert_buffer_size = 1")
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert duck.execute(
        f"INSERT INTO {sql.attached(ATTACHED_CATALOG, dataset, table)} "
        f"SELECT i FROM range({MULTI_CHUNK_ROWS}) t(i)"
    ).fetchall() == [(MULTI_CHUNK_ROWS,)]

    assert bigquery.query(
        f"SELECT COUNT(*), SUM(i) FROM {sql.qualified(project, dataset, table)}"
    ) == [(MULTI_CHUNK_ROWS, MULTI_CHUNK_SUM)]


def test_materializing_the_input_still_inserts_every_row(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Every row is collected before the driver sees one."""

    table = name("materialized")
    bigquery.execute(f"CREATE TABLE {sql.qualified(project, dataset, table)} (i BIGINT)")

    duck.execute("SET adbc_materialize_insert_rows = true")
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert duck.execute(
        f"INSERT INTO {sql.attached(ATTACHED_CATALOG, dataset, table)} "
        f"SELECT i FROM range({MULTI_CHUNK_ROWS}) t(i)"
    ).fetchall() == [(MULTI_CHUNK_ROWS,)]

    assert bigquery.query(
        f"SELECT COUNT(*), SUM(i) FROM {sql.qualified(project, dataset, table)}"
    ) == [(MULTI_CHUNK_ROWS, MULTI_CHUNK_SUM)]


def test_settings_do_not_leak_between_duckdb_connections() -> None:
    """Settings belong to a database instance.

    Each `extension.connect()` creates a separate in-memory database; cursors
    share their connection's settings.
    """

    with closing(extension.connect()) as first, closing(extension.connect()) as second:
        first.execute("SET adbc_connection_pool_size = 3")
        assert _setting(first, "adbc_connection_pool_size") == "3"
        assert _setting(second, "adbc_connection_pool_size") == "50"

        cursor = second.cursor()
        try:
            assert _setting(cursor, "adbc_connection_pool_size") == "50"
        finally:
            cursor.close()


def test_changing_the_pool_size_after_attach_leaves_the_attachment_working(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """The pool size is read at ATTACH, so an existing attachment keeps its pool.

    SQL does not expose the capacity, so the test checks only that scans work.
    """

    table = name("after")
    _counted_table(bigquery, project, dataset, table, 5)

    duck.execute("SET adbc_connection_pool_size = 1")
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    scan = f"SELECT COUNT(*), SUM(i) FROM {sql.attached(ATTACHED_CATALOG, dataset, table)}"
    assert duck.execute(scan).fetchall() == [(5, 15)]

    duck.execute("SET adbc_connection_pool_size = 16")
    assert _setting(duck, "adbc_connection_pool_size") == "16"
    assert duck.execute(scan).fetchall() == [(5, 15)]


def test_clearing_the_cache_with_nothing_attached_is_safe(
    duck: duckdb.DuckDBPyConnection,
) -> None:
    """`adbc_clear_cache()` has nothing to drop on a bare connection."""

    assert duck.execute("CALL adbc_clear_cache()").fetchall() == [(True,)]


def test_clearing_the_cache_keeps_an_attachment_usable(
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """After `adbc_clear_cache()`, the next scan fetches metadata from the driver again."""

    table = name("cached")
    _counted_table(bigquery, project, dataset, table, 5)

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    scan = f"SELECT COUNT(*), SUM(i) FROM {sql.attached(ATTACHED_CATALOG, dataset, table)}"
    assert duck.execute(scan).fetchall() == [(5, 15)]

    assert duck.execute("CALL adbc_clear_cache()").fetchall() == [(True,)]
    assert duck.execute(scan).fetchall() == [(5, 15)]
