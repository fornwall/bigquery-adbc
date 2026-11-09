"""read_adbc() binds via ExecuteSchema, then executes and drains the Arrow stream."""

from __future__ import annotations

import datetime
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import duckdb
import pytest
import sql
from sql import Session

# Spans several Arrow batches; `GENERATE_ARRAY` rows cost no scanned bytes.
LARGE_ROW_COUNT = 300_000


def _scan(
    duck: duckdb.DuckDBPyConnection, profile: str, query: str, *, columns: list[str] | None = None
) -> duckdb.DuckDBPyRelation:
    """Return an unexecuted `read_adbc()` relation; binding ran `AdbcStatementExecuteSchema`."""
    return duck.sql(sql.read_adbc(profile, query, columns=columns))


def _types(relation: duckdb.DuckDBPyRelation) -> list[str]:
    return [str(kind) for kind in relation.types]


def _one_row(relation: duckdb.DuckDBPyRelation) -> tuple[Any, ...]:
    [row] = relation.fetchall()
    return row


@pytest.fixture
def sample_table(bigquery: Session, name: Callable[[str], str], project: str, dataset: str) -> str:
    table = name("sample")
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, table)} AS "
        "SELECT * FROM UNNEST([ "
        "  STRUCT(1 AS id, 'one' AS label, [STRUCT(10 AS weight)] AS parts), "
        "  STRUCT(2 AS id, 'two' AS label, CAST([] AS ARRAY<STRUCT<weight INT64>>) AS parts)])"
    )
    return table


def test_scalar_types_reach_duckdb(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    relation = _scan(
        duck,
        profile,
        """
        SELECT
          CAST(-9223372036854775808 AS INT64) AS int64_value,
          CAST(1.5 AS FLOAT64) AS float64_value,
          NUMERIC '-1.25' AS numeric_value,
          TRUE AS bool_value,
          'hi' AS string_value,
          b'\\x00\\x01\\xff' AS bytes_value,
          DATE '2024-02-29' AS date_value,
          DATETIME '2024-02-29 12:34:56.789' AS datetime_value,
          TIME '01:02:03.456789' AS time_value,
          INTERVAL '1-2 3 4:5:6.7' YEAR TO SECOND AS interval_value,
          JSON '{"a":[1,2]}' AS json_value,
          ST_GEOGPOINT(1, 2) AS geography_value
        """,
    )
    assert _types(relation) == [
        "BIGINT",
        "DOUBLE",
        # Unparameterized NUMERIC keeps BigQuery's (38, 9).
        "DECIMAL(38,9)",
        "BOOLEAN",
        "VARCHAR",
        "BLOB",
        "DATE",
        # DATETIME is a civil time: an Arrow timestamp with no zone.
        "TIMESTAMP",
        "TIME",
        "INTERVAL",
        # DuckDB honours the `arrow.json` extension type.
        "JSON",
        # DuckDB does not know `geoarrow.wkt`, so GEOGRAPHY stays a WKT string.
        "VARCHAR",
    ]
    assert _one_row(relation) == (
        -9223372036854775808,
        1.5,
        Decimal("-1.250000000"),
        True,
        "hi",
        b"\x00\x01\xff",
        datetime.date(2024, 2, 29),
        # DATETIME has no zone, so the value is naive.
        datetime.datetime(2024, 2, 29, 12, 34, 56, 789000),  # noqa: DTZ001
        datetime.time(1, 2, 3, 456789),
        # DuckDB's Python conversion flattens an interval's months into
        # 30-day units: 14 months and 3 days become 423 days.
        datetime.timedelta(days=423, seconds=14706, microseconds=700000),
        '{"a":[1,2]}',
        "POINT(1 2)",
    )


def test_timestamp_is_an_instant(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    """The +02 input offset catches a dropped or misapplied zone."""

    query = "SELECT TIMESTAMP '2024-06-01 12:00:00.000001+02' AS ts"
    assert _types(_scan(duck, profile, query)) == ["TIMESTAMP WITH TIME ZONE"]

    microseconds, matches_utc_literal = _one_row(
        _scan(
            duck,
            profile,
            query,
            columns=[
                "epoch_us(ts) AS microseconds",
                "(ts = TIMESTAMPTZ '2024-06-01 10:00:00.000001Z') AS matches_utc_literal",
            ],
        )
    )
    assert microseconds == 1_717_236_000_000_001
    assert matches_utc_literal is True


def test_bignumeric_is_wider_than_duckdb_can_represent(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """DuckDB supports only 128-bit DECIMAL, so Decimal256 fails binding before query execution."""

    with pytest.raises(duckdb.NotImplementedException) as failure:
        _scan(duck, profile, "SELECT BIGNUMERIC '1.25' AS value")
    assert "Decimal d:76,38,256" in str(failure.value)


def test_null_of_every_type(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    query = """
        SELECT
          CAST(NULL AS INT64) AS int64_value,
          CAST(NULL AS FLOAT64) AS float64_value,
          CAST(NULL AS NUMERIC) AS numeric_value,
          CAST(NULL AS BOOL) AS bool_value,
          CAST(NULL AS STRING) AS string_value,
          CAST(NULL AS BYTES) AS bytes_value,
          CAST(NULL AS DATE) AS date_value,
          CAST(NULL AS DATETIME) AS datetime_value,
          CAST(NULL AS TIME) AS time_value,
          CAST(NULL AS TIMESTAMP) AS timestamp_value,
          CAST(NULL AS INTERVAL) AS interval_value,
          CAST(NULL AS JSON) AS json_value,
          CAST(NULL AS GEOGRAPHY) AS geography_value,
          CAST(NULL AS STRUCT<x INT64>) AS struct_value,
          CAST(NULL AS ARRAY<INT64>) AS array_value
    """
    assert _types(_scan(duck, profile, query)) == [
        "BIGINT",
        "DOUBLE",
        "DECIMAL(38,9)",
        "BOOLEAN",
        "VARCHAR",
        "BLOB",
        "DATE",
        "TIMESTAMP",
        "TIME",
        "TIMESTAMP WITH TIME ZONE",
        "INTERVAL",
        "JSON",
        "VARCHAR",
        "STRUCT(x BIGINT)",
        "BIGINT[]",
    ]
    row = _one_row(_scan(duck, profile, query))
    # BigQuery returns a NULL array as empty.
    assert row == (None,) * 14 + ([],)


def test_all_null_column(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    query = (
        "SELECT CAST(NULL AS STRING) AS label, x "
        "FROM UNNEST(GENERATE_ARRAY(1, 500)) AS x"
    )
    assert _types(_scan(duck, profile, query)) == ["VARCHAR", "BIGINT"]
    assert _one_row(
        _scan(
            duck,
            profile,
            query,
            columns=["count(*) AS rows", "count(label) AS labels"],
        )
    ) == (500, 0)


def test_nested_values(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    relation = _scan(
        duck,
        profile,
        """
        SELECT
          [1, 2, 3] AS ints,
          CAST([] AS ARRAY<INT64>) AS empty,
          [STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, CAST(NULL AS STRING) AS b)] AS records,
          STRUCT(1 AS a, STRUCT('x' AS b, DATE '2024-01-02' AS c) AS detail) AS nested,
          STRUCT([1, 2] AS xs, CAST([] AS ARRAY<STRING>) AS ys) AS holder
        """,
    )
    assert _types(relation) == [
        "BIGINT[]",
        "BIGINT[]",
        "STRUCT(a BIGINT, b VARCHAR)[]",
        "STRUCT(a BIGINT, detail STRUCT(b VARCHAR, c DATE))",
        "STRUCT(xs BIGINT[], ys VARCHAR[])",
    ]
    assert _one_row(relation) == (
        [1, 2, 3],
        [],
        [{"a": 1, "b": "x"}, {"a": 2, "b": None}],
        {"a": 1, "detail": {"b": "x", "c": datetime.date(2024, 1, 2)}},
        {"xs": [1, 2], "ys": []},
    )


def test_unicode_and_long_strings(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    """Literals pass through both GoogleSQL and DuckDB quoting."""

    text = "héllo 🚀 'quoted' \"double\" \n tab\there"
    long_text = "ab" * 25_000
    relation = _scan(
        duck,
        profile,
        f"SELECT {sql.string_literal(text)} AS text, "
        f"REPEAT('ab', 25000) AS long_text",
    )
    assert _types(relation) == ["VARCHAR", "VARCHAR"]
    assert _one_row(relation) == (text, long_text)


def test_wide_row(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    width = 200
    projection = ", ".join(f"x * {index} AS c{index}" for index in range(width))
    relation = _scan(
        duck,
        profile,
        f"SELECT {projection} FROM UNNEST(GENERATE_ARRAY(1, 50)) AS x",
    )
    assert relation.columns == [f"c{index}" for index in range(width)]
    assert _types(relation) == ["BIGINT"] * width
    rows = relation.fetchall()
    assert len(rows) == 50
    assert sorted(row[7] for row in rows) == [7 * x for x in range(1, 51)]


def test_empty_result(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    relation = _scan(
        duck,
        profile,
        "SELECT x, CAST(x AS STRING) AS label FROM UNNEST(GENERATE_ARRAY(1, 0)) AS x",
    )
    assert _types(relation) == ["BIGINT", "VARCHAR"]
    assert relation.fetchall() == []


def test_large_result_streams_every_row(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """A multi-batch result reaches DuckDB whole; the aggregates run in DuckDB."""

    total, checksum, distinct_remainders = _one_row(
        _scan(
            duck,
            profile,
            "SELECT x, CAST(x AS FLOAT64) / 8 AS half "
            f"FROM UNNEST(GENERATE_ARRAY(1, {LARGE_ROW_COUNT})) AS x",
            columns=[
                "count(*) AS total",
                "sum(x) AS checksum",
                "count(DISTINCT x % 97) AS distinct_remainders",
            ],
        )
    )
    assert total == LARGE_ROW_COUNT
    assert checksum == LARGE_ROW_COUNT * (LARGE_ROW_COUNT + 1) // 2
    assert distinct_remainders == 97


def test_large_result_consumed_partially(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """`LIMIT 1` closes the statement and connection mid-result without hanging."""

    query = f"SELECT x FROM UNNEST(GENERATE_ARRAY(1, {LARGE_ROW_COUNT})) AS x ORDER BY x"
    assert _one_row(duck.sql(sql.read_adbc(profile, query) + " LIMIT 1")) == (1,)


def test_two_scans_joined(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
    """Two `read_adbc()` scans in one query pull two streams at once."""

    left = sql.read_adbc(profile, "SELECT x FROM UNNEST(GENERATE_ARRAY(1, 10)) AS x")
    right = sql.read_adbc(
        profile, "SELECT x AS y, CAST(x AS STRING) AS label FROM UNNEST(GENERATE_ARRAY(5, 15)) AS x"
    )
    rows = duck.sql(
        f"SELECT l.x, r.label FROM ({left}) AS l JOIN ({right}) AS r ON l.x = r.y "
        "ORDER BY l.x"
    ).fetchall()
    assert rows == [(value, str(value)) for value in range(5, 11)]


def test_scan_joined_with_local_table(
    duck: duckdb.DuckDBPyConnection, profile: str, sample_table: str
) -> None:
    duck.execute("CREATE TABLE notes (id BIGINT, note VARCHAR)")
    duck.execute("INSERT INTO notes VALUES (1, 'first'), (2, 'second'), (3, 'absent')")
    remote = sql.read_adbc(
        profile, f"SELECT id, label FROM {sql.identifier(sample_table)}"
    )
    rows = duck.sql(
        f"SELECT n.note, r.label FROM notes AS n JOIN ({remote}) AS r ON n.id = r.id "
        "ORDER BY n.id"
    ).fetchall()
    assert rows == [("first", "one"), ("second", "two")]


def test_table_by_qualified_and_relative_name(
    duck: duckdb.DuckDBPyConnection, profile: str, project: str, dataset: str, sample_table: str
) -> None:
    qualified = _scan(
        duck,
        profile,
        f"SELECT * FROM {sql.qualified(project, dataset, sample_table)} ORDER BY id",
    )
    relative = _scan(
        duck, profile, f"SELECT * FROM {sql.identifier(sample_table)} ORDER BY id"
    )
    assert _types(qualified) == ["BIGINT", "VARCHAR", "STRUCT(weight BIGINT)[]"]
    assert _types(relative) == _types(qualified)
    expected = [(1, "one", [{"weight": 10}]), (2, "two", [])]
    assert qualified.fetchall() == expected
    assert relative.fetchall() == expected


def test_information_schema_columns(
    duck: duckdb.DuckDBPyConnection, profile: str, project: str, dataset: str, sample_table: str
) -> None:
    relation = _scan(
        duck,
        profile,
        "SELECT column_name, data_type, is_nullable FROM "
        f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = {sql.string_literal(sample_table)} "
        "ORDER BY ordinal_position",
    )
    assert _types(relation) == ["VARCHAR", "VARCHAR", "VARCHAR"]
    assert relation.fetchall() == [
        ("id", "INT64", "YES"),
        ("label", "STRING", "YES"),
        ("parts", "ARRAY<STRUCT<weight INT64>>", "NO"),
    ]


def test_information_schema_schemata(
    duck: duckdb.DuckDBPyConnection, profile: str, project: str, dataset: str
) -> None:
    """sync_status is checked by type only because its value is outside the test's control."""

    query = (
        "SELECT catalog_name, schema_name, location, creation_time, sync_status FROM "
        f"{sql.identifier(project)}.`region-us`.INFORMATION_SCHEMA.SCHEMATA "
        f"WHERE schema_name = {sql.string_literal(dataset)}"
    )
    assert _types(_scan(duck, profile, query)) == [
        "VARCHAR",
        "VARCHAR",
        "VARCHAR",
        "TIMESTAMP WITH TIME ZONE",
        "STRUCT(last_completion_time TIMESTAMP WITH TIME ZONE, "
        'error_time TIMESTAMP WITH TIME ZONE, "error" '
        "STRUCT(reason VARCHAR, \"location\" VARCHAR, message VARCHAR))",
    ]
    *identity, creation_time = _one_row(
        _scan(
            duck,
            profile,
            query,
            columns=["catalog_name", "schema_name", "location", "creation_time"],
        )
    )
    assert identity == [project, dataset, "US"]
    # The runner creates the dataset at the start of the run.
    now = datetime.datetime.now(datetime.UTC)
    assert now - datetime.timedelta(days=1) < creation_time
    assert creation_time < now + datetime.timedelta(minutes=5)


def test_invalid_syntax_fails_at_bind_time(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """BigQuery rejects `AdbcStatementExecuteSchema`, so a parse error is a binder error."""

    with pytest.raises(duckdb.BinderException) as failure:
        _scan(duck, profile, "SELEC 1")
    message = str(failure.value)
    assert 'Syntax error: Unexpected identifier "SELEC"' in message
    assert "invalidQuery" in message


def test_missing_table_fails_at_bind_time(
    duck: duckdb.DuckDBPyConnection, profile: str, dataset: str
) -> None:
    with pytest.raises(duckdb.BinderException) as failure:
        _scan(duck, profile, "SELECT * FROM no_such_table_at_all")
    message = str(failure.value)
    assert "Not found: Table" in message
    assert f"{dataset}.no_such_table_at_all" in message


def test_runtime_failure_surfaces_while_scanning(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    relation = _scan(duck, profile, "SELECT 1 / 0 AS value")
    assert _types(relation) == ["DOUBLE"]
    with pytest.raises(duckdb.IOException) as failure:
        relation.fetchall()
    assert "division by zero: 1 / 0" in str(failure.value)
