"""Storage Write ingestion through DuckDB INSERT, CREATE TABLE, CTAS, and COPY.

Read back stored values through an independent read_adbc() session."""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
import sql
from sql import Session

from tests.conftest import ATTACHED_CATALOG

# DuckDB streams an attached-catalog write in chunks of this many rows.
DUCKDB_CHUNK_ROWS = 2048


def _create_outside_attachment(
    bigquery: Session,
    attached: duckdb.DuckDBPyConnection,
    project: str,
    dataset: str,
    table: str,
    columns: str,
) -> str:
    """Clear the attachment cache so it sees the table created outside it."""

    bigquery.execute(f"CREATE TABLE {sql.qualified(project, dataset, table)} ({columns})")
    attached.execute("CALL adbc_clear_cache()")
    return sql.attached(ATTACHED_CATALOG, dataset, table)


def test_insert_values_into_an_existing_table(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("target")
    target = _create_outside_attachment(bigquery, attached, project, dataset, table, "i INT64, s STRING")

    inserted = attached.execute(f"INSERT INTO {target} VALUES (1, 'one'), (2, 'two'), (3, NULL)").fetchall()

    assert inserted == [(3,)]
    assert bigquery.query(f"SELECT i, s FROM {sql.qualified(project, dataset, table)} ORDER BY i") == [
        (1, "one"),
        (2, "two"),
        (3, None),
    ]


def test_insert_select_from_a_local_duckdb_table(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("target")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute("CREATE TABLE local_rows (i BIGINT, s VARCHAR)")
    attached.execute("INSERT INTO local_rows VALUES (1, 'a'), (2, 'b'), (3, NULL)")
    attached.execute(f"CREATE TABLE {target} (i BIGINT, s VARCHAR)")

    inserted = attached.execute(f"INSERT INTO {target} SELECT i, s FROM local_rows").fetchall()

    assert inserted == [(3,)]
    assert bigquery.query(f"SELECT i, s FROM {sql.qualified(project, dataset, table)} ORDER BY i") == [
        (1, "a"),
        (2, "b"),
        (3, None),
    ]


def test_a_second_insert_appends_rather_than_replaces(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("target")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (i BIGINT)")

    assert attached.execute(f"INSERT INTO {target} VALUES (1), (2)").fetchall() == [(2,)]
    assert attached.execute(f"INSERT INTO {target} VALUES (3)").fetchall() == [(1,)]

    assert bigquery.query(f"SELECT i FROM {sql.qualified(project, dataset, table)} ORDER BY i") == [(1,), (2,), (3,)]


def test_ingest_type_fidelity(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Naive TIMESTAMP maps to DATETIME; TIMESTAMPTZ maps to TIMESTAMP."""

    table = name("types")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(
        rf"""
        CREATE TABLE {target} AS SELECT * FROM (VALUES (
            1::BIGINT, 2::INTEGER, 3::SMALLINT, 4::TINYINT, 5::UINTEGER,
            1.5::DOUBLE, 2.5::FLOAT, TRUE, 'text', '\x00\x01\xff'::BLOB,
            DATE '2024-02-29', TIME '01:02:03.456789',
            TIMESTAMP '2024-06-01 12:00:00.123456',
            TIMESTAMPTZ '2024-06-01 12:00:00+02',
            (-1.25)::DECIMAL(10, 2), 1.25::DECIMAL(38, 9), 123::HUGEINT
        )) AS t(i64, i32, i16, i8, u32, f64, f32, boolean, string, bytes,
                date, time, timestamp, timestamptz, decimal, wide_decimal, hugeint)
        """
    )

    assert bigquery.columns(project, dataset, table) == [
        ("i64", "INT64"),
        ("i32", "INT64"),
        ("i16", "INT64"),
        ("i8", "INT64"),
        # Unsigned types that fit are widened; UBIGINT is rejected
        # (test_driver_rejects_an_unsupported_arrow_type).
        ("u32", "INT64"),
        ("f64", "FLOAT64"),
        ("f32", "FLOAT64"),
        ("boolean", "BOOL"),
        ("string", "STRING"),
        ("bytes", "BYTES"),
        ("date", "DATE"),
        ("time", "TIME"),
        ("timestamp", "DATETIME"),
        ("timestamptz", "TIMESTAMP"),
        ("decimal", "NUMERIC(10, 2)"),
        ("wide_decimal", "NUMERIC(38, 9)"),
        # A 128-bit DuckDB integer needs more digits than NUMERIC has.
        ("hugeint", "BIGNUMERIC(38)"),
    ]

    # DuckDB has no type for BIGNUMERIC's 256-bit Arrow decimal, so it is read
    # back as a string.
    assert bigquery.query(
        "SELECT i64, i32, i16, i8, u32, f64, f32, boolean, string, bytes, date, time, "
        "timestamp, timestamptz, decimal, wide_decimal, CAST(hugeint AS STRING) "
        f"FROM {sql.qualified(project, dataset, table)}"
    ) == [
        (
            1,
            2,
            3,
            4,
            5,
            1.5,
            2.5,
            True,
            "text",
            b"\x00\x01\xff",
            datetime.date(2024, 2, 29),
            datetime.time(1, 2, 3, 456789),
            # DATETIME is civil, so the value is naive; `fromisoformat` avoids
            # the lint ban on naive `datetime()` calls.
            datetime.datetime.fromisoformat("2024-06-01T12:00:00.123456"),
            # The zoned literal is +02:00, so the stored instant is 10:00 UTC.
            datetime.datetime(2024, 6, 1, 10, tzinfo=datetime.UTC),
            Decimal("-1.25"),
            Decimal("1.25"),
            "123",
        )
    ]


def test_ingest_nulls_in_every_type(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """BigQuery has no NULL array; a NULL LIST becomes an empty array."""

    table = name("nulls")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(
        f"""
        CREATE TABLE {target} AS SELECT * FROM (VALUES (
            NULL::BIGINT, NULL::DOUBLE, NULL::BOOLEAN, NULL::VARCHAR, NULL::BLOB,
            NULL::DATE, NULL::TIME, NULL::TIMESTAMP, NULL::DECIMAL(10, 2),
            NULL::BIGINT[], NULL::STRUCT(a BIGINT)
        )) AS t(i64, f64, boolean, string, bytes, date, time, timestamp, decimal,
                list, record)
        """
    )

    assert bigquery.query(f"SELECT * FROM {sql.qualified(project, dataset, table)}") == [
        (None, None, None, None, None, None, None, None, None, [], None)
    ]


def test_ingest_nested_list_and_struct(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("nested")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(
        f"""
        CREATE TABLE {target} AS SELECT * FROM (VALUES
            (1::BIGINT, [10::BIGINT, 20], {{'a': 1::BIGINT, 'b': 'x'}},
             [{{'n': 1::BIGINT}}], {{'outer': {{'deep': 'y'}}}}),
            (2::BIGINT, []::BIGINT[], {{'a': NULL::BIGINT, 'b': NULL::VARCHAR}},
             []::STRUCT(n BIGINT)[], {{'outer': NULL::STRUCT(deep VARCHAR)}})
        ) AS t(k, numbers, record, records, nested)
        """
    )

    assert bigquery.columns(project, dataset, table) == [
        ("k", "INT64"),
        ("numbers", "ARRAY<INT64>"),
        ("record", "STRUCT<a INT64, b STRING>"),
        ("records", "ARRAY<STRUCT<n INT64>>"),
        # BigQuery backtick-quotes a nested field name that is a reserved word.
        ("nested", "STRUCT<`outer` STRUCT<deep STRING>>"),
    ]
    assert bigquery.query(
        f"SELECT k, numbers, record, records, nested FROM {sql.qualified(project, dataset, table)} ORDER BY k"
    ) == [
        (1, [10, 20], {"a": 1, "b": "x"}, [{"n": 1}], {"outer": {"deep": "y"}}),
        (2, [], {"a": None, "b": None}, [], {"outer": None}),
    ]


def test_ingest_unicode_and_awkward_strings(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("strings")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (k BIGINT, s VARCHAR)")
    values = [
        "héllo wörld",
        "日本語テキスト",
        "emoji \U0001f600\U0001f1f8\U0001f1ea",
        "quote ' double \" backtick ` backslash \\",
        "line1\nline2\r\ttab",
        "x" * 100_000,
        "",
    ]
    rows = ", ".join(f"({index}, {sql.duckdb_literal(value)})" for index, value in enumerate(values))

    assert attached.execute(f"INSERT INTO {target} VALUES {rows}").fetchall() == [(len(values),)]
    assert bigquery.query(f"SELECT k, s FROM {sql.qualified(project, dataset, table)} ORDER BY k") == list(
        enumerate(values)
    )


def test_multi_batch_insert(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Cross DuckDB's chunk boundary, and the Storage Write API's, many times."""

    rows = 20_000
    assert rows > DUCKDB_CHUNK_ROWS * 8
    table = name("bulk")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (i BIGINT, s VARCHAR)")

    inserted = attached.execute(f"INSERT INTO {target} SELECT i, 'row ' || i FROM range({rows}) AS t(i)").fetchall()

    assert inserted == [(rows,)]
    # The aggregates catch a dropped or duplicated row without reading 20k rows back.
    assert bigquery.query(
        f"SELECT COUNT(*), SUM(i), COUNT(DISTINCT s) FROM {sql.qualified(project, dataset, table)}"
    ) == [(rows, rows * (rows - 1) // 2, rows)]


@pytest.mark.parametrize(
    "setting",
    [
        # One chunk in flight: producer and consumer hand off on every chunk.
        "SET adbc_insert_buffer_size = 1",
        # Collect every row before binding the stream.
        "SET adbc_materialize_insert_rows = true",
    ],
)
def test_buffering_settings_drive_a_multi_chunk_insert(
    setting: str,
    duck: duckdb.DuckDBPyConnection,
    profile: str,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Set before ATTACH, so the setting applies whether the extension reads it then or per insert."""
    rows = DUCKDB_CHUNK_ROWS * 3
    table = name("buffered")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    duck.execute(setting)
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    duck.execute(f"CREATE TABLE {target} (i BIGINT)")

    assert duck.execute(f"INSERT INTO {target} SELECT i FROM range({rows}) AS t(i)").fetchall() == [(rows,)]
    assert bigquery.query(f"SELECT COUNT(*), SUM(i) FROM {sql.qualified(project, dataset, table)}") == [
        (rows, rows * (rows - 1) // 2)
    ]


def test_ctas_creates_the_table_and_fills_it(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """CTAS is two ingests: an empty create-mode one, then an append with the rows."""

    table = name("ctas")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute("CREATE TABLE local_rows (i BIGINT, s VARCHAR, d DATE)")
    attached.execute("INSERT INTO local_rows VALUES (1, 'a', DATE '2024-01-01'), (2, 'b', NULL)")

    created = attached.execute(f"CREATE TABLE {target} AS SELECT i, s, d FROM local_rows").fetchall()

    assert created == [(2,)]
    assert bigquery.columns(project, dataset, table) == [
        ("i", "INT64"),
        ("s", "STRING"),
        ("d", "DATE"),
    ]
    assert bigquery.query(f"SELECT i, s, d FROM {sql.qualified(project, dataset, table)} ORDER BY i") == [
        (1, "a", datetime.date(2024, 1, 1)),
        (2, "b", None),
    ]


def test_ctas_with_zero_rows_still_creates_the_table(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """The create-mode ingest binds an empty stream, so the schema alone decides the table."""

    table = name("empty")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)

    created = attached.execute(f"CREATE TABLE {target} AS SELECT 1::BIGINT AS i, 'x' AS s WHERE FALSE").fetchall()

    assert created == [(0,)]
    assert bigquery.columns(project, dataset, table) == [
        ("i", "INT64"),
        ("s", "STRING"),
    ]
    assert bigquery.scalar(f"SELECT COUNT(*) FROM {sql.qualified(project, dataset, table)}") == 0


def test_create_table_with_an_explicit_column_list(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """A bare `CREATE TABLE` reaches BigQuery as a create-mode ingest of no rows."""

    table = name("declared")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)

    # Unlike CTAS, this reports no row count.
    assert attached.execute(f"CREATE TABLE {target} (i BIGINT, s VARCHAR)").fetchall() == []

    assert bigquery.columns(project, dataset, table) == [
        ("i", "INT64"),
        ("s", "STRING"),
    ]
    # The attachment created the table, so it needs no cache clear.
    assert attached.execute(f"INSERT INTO {target} VALUES (1, 'a')").fetchall() == [(1,)]


def test_copy_and_read_csv_both_reach_the_ingest_path(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
    tmp_path: Path,
) -> None:
    """COPY and read_csv take different DuckDB paths to AdbcStatementBindStream."""

    table = name("loaded")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (i BIGINT, s VARCHAR)")
    csv = tmp_path / "rows.csv"
    csv.write_text("i,s\n1,a\n2,b\n", encoding="utf-8")
    literal = sql.duckdb_literal(str(csv))

    copied = attached.execute(f"COPY {target} FROM {literal} (FORMAT csv, HEADER)").fetchall()
    scanned = attached.execute(f"INSERT INTO {target} SELECT i, s FROM read_csv({literal})").fetchall()

    assert copied == [(2,)]
    assert scanned == [(2,)]
    assert bigquery.query(f"SELECT i, s FROM {sql.qualified(project, dataset, table)} ORDER BY i, s") == [
        (1, "a"),
        (1, "a"),
        (2, "b"),
        (2, "b"),
    ]


def test_write_into_a_dataset_other_than_the_profile_default(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    secondary_dataset: str,
) -> None:
    """The extension sets target_db_schema, but never target_catalog."""

    table = name("elsewhere")
    target = sql.attached(ATTACHED_CATALOG, secondary_dataset, table)

    created = attached.execute(f"CREATE TABLE {target} AS SELECT 1::BIGINT AS i").fetchall()

    assert created == [(1,)]
    assert bigquery.query(f"SELECT i FROM {sql.qualified(project, secondary_dataset, table)}") == [(1,)]


def test_rollback_does_not_undo_a_remote_write(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """The extension ingests in ADBC autocommit; DuckDB's ROLLBACK is local only."""

    table = name("committed")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (i BIGINT)")

    attached.execute("BEGIN")
    attached.execute(f"INSERT INTO {target} VALUES (7)")
    attached.execute("ROLLBACK")

    assert bigquery.query(f"SELECT i FROM {sql.qualified(project, dataset, table)}") == [(7,)]


def test_driver_rejects_an_unsupported_arrow_type(
    attached: duckdb.DuckDBPyConnection,
    name: Callable[[str], str],
    dataset: str,
) -> None:
    """UBIGINT is Arrow UInt64; the driver rejects it after DuckDB binding succeeds."""

    target = sql.attached(ATTACHED_CATALOG, dataset, name("unsigned"))

    with pytest.raises(
        duckdb.IOException,
        match=re.escape('Arrow ingest field "u" has type UInt64, but BigQuery has no unsigned integer type'),
    ):
        attached.execute(f"CREATE TABLE {target} AS SELECT 1::UBIGINT AS u")


def test_driver_rejects_a_column_name_bigquery_would_not_accept(
    attached: duckdb.DuckDBPyConnection,
    name: Callable[[str], str],
    dataset: str,
) -> None:
    """DuckDB allows a hyphen in a quoted column name; BigQuery does not."""

    target = sql.attached(ATTACHED_CATALOG, dataset, name("hyphen"))

    with pytest.raises(
        duckdb.IOException,
        match=re.escape('BigQuery column name "a-b" contains "-"'),
    ):
        attached.execute(f'CREATE TABLE {target} AS SELECT 1::BIGINT AS "a-b"')


def test_append_reaches_a_table_with_a_required_column(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """DuckDB binds nullable fields; append must accept them unless an actual null violates REQUIRED."""

    table = name("required")
    target = _create_outside_attachment(bigquery, attached, project, dataset, table, "i INT64 NOT NULL, s STRING")

    attached.execute(f"INSERT INTO {target} VALUES (1, 'x')")
    assert bigquery.query(f"SELECT i, s FROM {sql.qualified(project, dataset, table)}") == [(1, "x")]

    with pytest.raises(duckdb.IOException, match=re.escape("Field 'i' is required but has null value")):
        attached.execute(f"INSERT INTO {target} VALUES (NULL, 'y')")
    assert bigquery.query(f"SELECT i, s FROM {sql.qualified(project, dataset, table)}") == [(1, "x")]


def test_driver_rejects_an_append_whose_schema_does_not_match(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Replace the table behind the attachment so its cached schema shapes an incompatible stream."""

    table = name("mismatch")
    target = _create_outside_attachment(bigquery, attached, project, dataset, table, "i INT64, s STRING")
    # Cache the columns in the attachment, then replace the table behind it.
    attached.execute(f"SELECT * FROM {target}").fetchall()
    bigquery.execute(f"CREATE OR REPLACE TABLE {sql.qualified(project, dataset, table)} (i INT64, s TIMESTAMP)")

    with pytest.raises(
        duckdb.IOException,
        match=re.escape('column "s" is TIMESTAMP NULLABLE in the table but STRING NULLABLE in the bound data'),
    ):
        attached.execute(f"INSERT INTO {target} VALUES (1, 'one')")


def test_duckdb_rejects_a_write_before_the_driver_sees_it(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """The cached schema lets DuckDB reject these before contacting BigQuery."""

    table = name("guarded")
    target = _create_outside_attachment(bigquery, attached, project, dataset, table, "i INT64")
    missing = sql.attached(ATTACHED_CATALOG, dataset, name("absent"))

    with pytest.raises(duckdb.BinderException, match='does not have a column with name "nope"'):
        attached.execute(f"INSERT INTO {target} (nope) VALUES (1)")
    with pytest.raises(duckdb.ConversionException, match=re.escape("Could not convert string 'abc' to INT64")):
        attached.execute(f"INSERT INTO {target} VALUES ('abc')")
    # Name suggestions resolve tables across the project through remote queries.
    attached.execute("SET catalog_error_max_schemas = 0")
    with pytest.raises(duckdb.CatalogException, match="does not exist"):
        attached.execute(f"INSERT INTO {missing} VALUES (1)")

    assert bigquery.scalar(f"SELECT COUNT(*) FROM {sql.qualified(project, dataset, table)}") == 0


def test_write_statements_the_extension_does_not_implement(
    attached: duckdb.DuckDBPyConnection,
    session: Session,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """session shares the attachment connection; execute DELETE there after catalog DELETE fails."""

    table = name("limits")
    target = sql.attached(ATTACHED_CATALOG, dataset, table)
    attached.execute(f"CREATE TABLE {target} (i BIGINT)")
    attached.execute(f"INSERT INTO {target} VALUES (1), (2)")

    with pytest.raises(duckdb.BinderException, match="RETURNING clause not yet supported"):
        attached.execute(f"INSERT INTO {target} VALUES (3) RETURNING i")
    with pytest.raises(
        duckdb.BinderException,
        match="CREATE TABLE commands not yet supported with IF NOT EXISTS or REPLACE",
    ):
        attached.execute(
            f"CREATE TABLE IF NOT EXISTS {sql.attached(ATTACHED_CATALOG, dataset, name('never'))} AS SELECT 1 AS i"
        )
    with pytest.raises(duckdb.BinderException, match="Can only delete from base table"):
        attached.execute(f"DELETE FROM {target}")
    with pytest.raises(duckdb.BinderException, match="Can only update base table"):
        attached.execute(f"UPDATE {target} SET i = 3")
    with pytest.raises(duckdb.NotImplementedException, match="DropEntry not yet supported with the ADBC extension"):
        attached.execute(f"DROP TABLE {target}")

    session.execute(f"DELETE FROM {sql.qualified(project, dataset, table)} WHERE i = 1")
    assert bigquery.query(f"SELECT i FROM {sql.qualified(project, dataset, table)}") == [(2,)]
