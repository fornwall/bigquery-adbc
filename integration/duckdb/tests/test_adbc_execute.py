"""DDL, DML, scripts, and routines through adbc_execute().

ExecuteQuery receives null stream and row-count pointers; verify effects via
read_adbc(). Profile/load failures raise BinderException; execution failures
raise IOException with the driver message but no status, SQLSTATE, or vendor code."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb
import profiles
import pytest
import sql
from sql import Session

# Same location as the run's datasets, so one job can read across them.
LOCATION = "US"


def _columns(session: Session, project: str, dataset: str, table: str) -> list[tuple]:
    """Return one table's columns, in ordinal order, as (name, type) pairs."""

    return session.query(
        "SELECT column_name, data_type FROM "
        f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = {sql.string_literal(table)} "
        "ORDER BY ordinal_position"
    )


def _table_count(session: Session, project: str, dataset: str, table: str) -> int:
    """Report how many objects of `table`'s name the dataset holds (0 or 1)."""

    return session.scalar(
        "SELECT COUNT(*) AS n FROM "
        f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES "
        f"WHERE table_name = {sql.string_literal(table)}"
    )


def _table_option(
    session: Session, project: str, dataset: str, table: str, option: str
) -> str | None:
    """Return one `TABLE_OPTIONS` value, still in its GoogleSQL literal form."""

    rows = session.query(
        "SELECT option_value FROM "
        f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLE_OPTIONS "
        f"WHERE table_name = {sql.string_literal(table)} "
        f"AND option_name = {sql.string_literal(option)}"
    )
    return None if not rows else str(rows[0][0])


def test_call_reports_a_single_true_success_row(
    duck: duckdb.DuckDBPyConnection, profile: str
) -> None:
    """The extension never requests a stream, so even a `SELECT` reports only `Success`."""

    relation = duck.sql(sql.adbc_execute(profile, "SELECT 1 AS discarded"))

    assert relation.columns == ["Success"]
    assert [str(kind) for kind in relation.types] == ["BOOLEAN"]
    assert relation.fetchall() == [(True,)]


def test_create_alter_and_drop_a_table(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("table")
    path = sql.qualified(project, dataset, table)

    session.execute(f"CREATE TABLE {path} (id INT64, label STRING)")
    assert _columns(session, project, dataset, table) == [
        ("id", "INT64"),
        ("label", "STRING"),
    ]

    session.execute(f"ALTER TABLE {path} ADD COLUMN score FLOAT64")
    session.execute(f"ALTER TABLE {path} RENAME COLUMN label TO title")
    assert _columns(session, project, dataset, table) == [
        ("id", "INT64"),
        ("title", "STRING"),
        ("score", "FLOAT64"),
    ]

    session.execute(f"DROP TABLE {path}")
    assert _table_count(session, project, dataset, table) == 0


def test_create_table_as_select_replace_and_if_not_exists(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("ctas")
    path = sql.qualified(project, dataset, table)

    session.execute(f"CREATE TABLE {path} AS SELECT 1 AS id, 'first' AS label")
    assert session.query(f"SELECT id, label FROM {path}") == [(1, "first")]

    session.execute(
        f"CREATE OR REPLACE TABLE {path} AS SELECT 2 AS id, 'second' AS label"
    )
    assert session.query(f"SELECT id, label FROM {path}") == [(2, "second")]

    session.execute(f"CREATE TABLE IF NOT EXISTS {path} (id INT64, label STRING)")
    session.execute(f"CREATE TABLE IF NOT EXISTS {path} (id INT64, label STRING)")
    assert session.query(f"SELECT id, label FROM {path}") == [(2, "second")]

    session.execute(f"DROP TABLE {path}")


def test_drop_table_if_exists_on_a_missing_table_succeeds(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    path = sql.qualified(project, dataset, name("never_created"))

    session.execute(f"DROP TABLE IF EXISTS {path}")


def test_view_lifecycle(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("base")
    view = name("view")
    table_path = sql.qualified(project, dataset, table)
    view_path = sql.qualified(project, dataset, view)

    session.execute(
        f"CREATE TABLE {table_path} AS "
        "SELECT * FROM UNNEST([STRUCT(1 AS id, 10 AS value), (2, 20)])"
    )
    session.execute(f"CREATE VIEW {view_path} AS SELECT id FROM {table_path}")

    assert session.query(f"SELECT id FROM {view_path} ORDER BY id") == [(1,), (2,)]
    assert (
        session.scalar(
            "SELECT table_type FROM "
            f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_name = {sql.string_literal(view)}"
        )
        == "VIEW"
    )

    session.execute(
        f"CREATE OR REPLACE VIEW {view_path} AS "
        f"SELECT id FROM {table_path} WHERE value > 15"
    )
    assert session.query(f"SELECT id FROM {view_path}") == [(2,)]

    session.execute(f"DROP VIEW {view_path}")
    assert _table_count(session, project, dataset, view) == 0
    session.execute(f"DROP TABLE {table_path}")


def test_materialized_view_lifecycle(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """A materialized view's DDL job also writes data; the tiny base table keeps it cheap."""

    table = name("mvbase")
    view = name("mv")
    table_path = sql.qualified(project, dataset, table)
    view_path = sql.qualified(project, dataset, view)

    session.execute(
        f"CREATE TABLE {table_path} AS "
        "SELECT * FROM UNNEST([STRUCT('eu' AS region, 1 AS value), ('eu', 2), ('us', 4)])"
    )
    session.execute(
        f"CREATE MATERIALIZED VIEW {view_path} AS "
        f"SELECT region, SUM(value) AS total FROM {table_path} GROUP BY region"
    )

    assert session.query(f"SELECT region, total FROM {view_path} ORDER BY region") == [
        ("eu", 3),
        ("us", 4),
    ]
    assert (
        session.scalar(
            "SELECT table_type FROM "
            f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_name = {sql.string_literal(view)}"
        )
        == "MATERIALIZED VIEW"
    )

    session.execute(f"DROP MATERIALIZED VIEW {view_path}")
    assert _table_count(session, project, dataset, view) == 0
    session.execute(f"DROP TABLE {table_path}")


def test_insert_update_and_delete(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """`adbc_execute()` reports no row count, so each statement's effect is read back."""

    table = name("dml")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} (id INT64, label STRING)")

    session.execute(f"INSERT INTO {path} (id, label) VALUES (1, 'one'), (2, 'two')")
    assert session.query(f"SELECT id, label FROM {path} ORDER BY id") == [
        (1, "one"),
        (2, "two"),
    ]

    session.execute(f"UPDATE {path} SET label = 'renamed' WHERE id = 2")
    assert session.query(f"SELECT label FROM {path} WHERE id = 2") == [("renamed",)]

    session.execute(f"DELETE FROM {path} WHERE id = 1")
    assert session.query(f"SELECT id FROM {path}") == [(2,)]

    session.execute(f"DROP TABLE {path}")


def test_merge_updates_and_inserts(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("merge")
    path = sql.qualified(project, dataset, table)
    session.execute(
        f"CREATE TABLE {path} AS "
        "SELECT * FROM UNNEST([STRUCT(1 AS id, 'one' AS label), (2, 'two')])"
    )

    session.execute(
        f"MERGE {path} AS target "
        "USING UNNEST([STRUCT(2 AS id, 'TWO' AS label), (3, 'three')]) AS source "
        "ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET label = source.label "
        "WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label)"
    )

    assert session.query(f"SELECT id, label FROM {path} ORDER BY id") == [
        (1, "one"),
        (2, "TWO"),
        (3, "three"),
    ]
    session.execute(f"DROP TABLE {path}")


def test_truncate_table_empties_it(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """TRUNCATE is DML in GoogleSQL, and leaves the table itself behind."""

    table = name("truncate")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} AS SELECT id FROM UNNEST([1, 2, 3]) AS id")

    session.execute(f"TRUNCATE TABLE {path}")

    assert session.scalar(f"SELECT COUNT(*) AS n FROM {path}") == 0
    assert _table_count(session, project, dataset, table) == 1
    session.execute(f"DROP TABLE {path}")


def test_multi_statement_script(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """A `BEGIN ... END` script job has its own statement type and no result schema."""

    table = name("script")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} (id INT64)")

    session.execute(
        "BEGIN\n"
        f"  INSERT INTO {path} (id) VALUES (1);\n"
        f"  INSERT INTO {path} (id) VALUES (2);\n"
        "END"
    )

    assert session.query(f"SELECT id FROM {path} ORDER BY id") == [(1,), (2,)]
    session.execute(f"DROP TABLE {path}")


def test_script_with_declared_variables(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """`DECLARE` and `SET` need the scripting path, not the query path."""

    table = name("declare")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} (id INT64)")

    session.execute(
        "DECLARE threshold INT64 DEFAULT 0;\n"
        "SET threshold = 2;\n"
        f"INSERT INTO {path} (id) SELECT id FROM UNNEST([1, 2, 3]) AS id "
        "WHERE id > threshold;"
    )

    assert session.query(f"SELECT id FROM {path}") == [(3,)]
    session.execute(f"DROP TABLE {path}")


def test_a_failing_script_keeps_what_ran_before_it(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """A script is not a transaction; statements before the failure commit."""

    table = name("partial")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} (id INT64)")

    with pytest.raises(duckdb.IOException) as caught:
        session.execute(
            "BEGIN\n"
            f"  INSERT INTO {path} (id) VALUES (1);\n"
            "  SELECT ERROR('deliberate failure halfway through the script');\n"
            "END"
        )

    assert "deliberate failure halfway through the script" in str(caught.value)
    assert session.query(f"SELECT id FROM {path}") == [(1,)]
    session.execute(f"DROP TABLE {path}")


def test_create_and_drop_a_schema(
    session: Session, project: str, name: Callable[[str], str]
) -> None:
    """The runner does not clean up this separate dataset; drop it in finally."""

    own_dataset = name("dataset")
    path = sql.qualified(project, own_dataset)
    exists = (
        "SELECT COUNT(*) AS n FROM "
        f"{sql.identifier(project)}.INFORMATION_SCHEMA.SCHEMATA "
        f"WHERE schema_name = {sql.string_literal(own_dataset)}"
    )

    session.execute(
        f"CREATE SCHEMA {path} OPTIONS(location = {sql.string_literal(LOCATION)})"
    )
    try:
        assert session.scalar(exists) == 1
    finally:
        session.execute(f"DROP SCHEMA {path}")
    assert session.scalar(exists) == 0


def test_ddl_in_the_secondary_dataset(
    session: Session,
    project: str,
    dataset: str,
    secondary_dataset: str,
    name: Callable[[str], str],
) -> None:
    """`bigquery.dataset_id` supplies a default dataset but does not confine statements to it."""

    table = name("secondary")
    path = sql.qualified(project, secondary_dataset, table)

    session.execute(f"CREATE TABLE {path} AS SELECT 1 AS id")
    try:
        assert _table_count(session, project, secondary_dataset, table) == 1
        assert _table_count(session, project, dataset, table) == 0
        assert session.query(f"SELECT id FROM {path}") == [(1,)]
    finally:
        session.execute(f"DROP TABLE {path}")


def test_sql_udf_created_by_execute_and_called_by_a_scan(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """`CREATE FUNCTION` has no result; its effect shows in a scan."""

    function = name("udf")
    path = sql.qualified(project, dataset, function)

    session.execute(f"CREATE FUNCTION {path}(value INT64) AS (value + 1)")
    try:
        assert session.scalar(f"SELECT {path}(41) AS answer") == 42
        assert (
            session.scalar(
                "SELECT routine_type FROM "
                f"{sql.qualified(project, dataset)}.INFORMATION_SCHEMA.ROUTINES "
                f"WHERE routine_name = {sql.string_literal(function)}"
            )
            == "FUNCTION"
        )
    finally:
        session.execute(f"DROP FUNCTION {path}")


def test_stored_procedure_created_and_called_by_execute(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("proctarget")
    procedure = name("proc")
    table_path = sql.qualified(project, dataset, table)
    procedure_path = sql.qualified(project, dataset, procedure)
    session.execute(f"CREATE TABLE {table_path} (id INT64)")

    session.execute(
        f"CREATE PROCEDURE {procedure_path}(IN id INT64) "
        f"BEGIN INSERT INTO {table_path} (id) VALUES (id); END"
    )
    try:
        session.execute(f"CALL {procedure_path}(7)")
        assert session.query(f"SELECT id FROM {table_path}") == [(7,)]
    finally:
        session.execute(f"DROP PROCEDURE {procedure_path}")
        session.execute(f"DROP TABLE {table_path}")


def test_table_options_are_set_and_altered(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """TABLE_OPTIONS returns GoogleSQL literals: quoted descriptions and STRUCT arrays for labels."""

    table = name("options")
    path = sql.qualified(project, dataset, table)

    session.execute(
        f"CREATE TABLE {path} (id INT64) "
        f"OPTIONS(description = {sql.string_literal('created with a description')}, "
        "labels = [('suite', 'duckdb')])"
    )
    assert (
        _table_option(session, project, dataset, table, "description")
        == '"created with a description"'
    )
    assert (
        _table_option(session, project, dataset, table, "labels")
        == '[STRUCT("suite", "duckdb")]'
    )

    session.execute(
        f"ALTER TABLE {path} SET OPTIONS(description = "
        f"{sql.string_literal('altered afterwards')})"
    )
    assert (
        _table_option(session, project, dataset, table, "description")
        == '"altered afterwards"'
    )
    # SET OPTIONS replaces only the options it names.
    assert (
        _table_option(session, project, dataset, table, "labels")
        == '[STRUCT("suite", "duckdb")]'
    )

    session.execute(f"DROP TABLE {path}")


def test_partitioned_and_clustered_table(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("partitioned")
    path = sql.qualified(project, dataset, table)

    session.execute(
        f"CREATE TABLE {path} (day DATE, region STRING, value INT64) "
        "PARTITION BY day CLUSTER BY region"
    )
    assert session.query(
        "SELECT column_name, is_partitioning_column, clustering_ordinal_position "
        f"FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = {sql.string_literal(table)} ORDER BY ordinal_position"
    ) == [("day", "YES", None), ("region", "NO", 1), ("value", "NO", None)]

    session.execute(
        f"INSERT INTO {path} (day, region, value) VALUES "
        "(DATE '2024-01-01', 'eu', 1), (DATE '2024-02-01', 'us', 2)"
    )
    assert session.query(
        f"SELECT value FROM {path} WHERE day = DATE '2024-02-01' AND region = 'us'"
    ) == [(2,)]

    session.execute(f"DROP TABLE {path}")


def test_invalid_syntax_reports_the_bigquery_message(session: Session) -> None:
    """The extension forwards only the message; invalidQuery is all that remains of the driver status."""

    with pytest.raises(duckdb.IOException) as caught:
        session.execute("CREATE TABLE SELECT WHERE")

    message = str(caught.value)
    assert "invalidQuery" in message
    assert "Syntax error" in message


def test_dml_against_a_missing_table_reports_the_bigquery_message(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("absent")
    path = sql.qualified(project, dataset, table)

    with pytest.raises(duckdb.IOException) as caught:
        session.execute(f"INSERT INTO {path} (id) VALUES (1)")

    message = str(caught.value)
    assert "notFound" in message
    assert table in message


def test_a_refused_permission_reports_the_bigquery_message(
    session: Session, name: Callable[[str], str]
) -> None:
    """Creating a table in a public dataset is refused before any job runs."""

    path = f"`bigquery-public-data`.`samples`.{sql.identifier(name('denied'))}"

    with pytest.raises(duckdb.IOException) as caught:
        session.execute(f"CREATE TABLE {path} (id INT64)")

    message = str(caught.value)
    assert "accessDenied" in message
    assert "bigquery.tables.create" in message


def test_an_empty_statement_is_refused_before_any_request(session: Session) -> None:
    """The driver, not BigQuery, rejects a comment-only statement."""

    with pytest.raises(duckdb.IOException) as caught:
        session.execute("-- nothing but a comment\n")

    assert "without a SQL query" in str(caught.value)


def test_an_unknown_profile_fails_while_the_call_is_bound(
    duck: duckdb.DuckDBPyConnection,
) -> None:
    with pytest.raises(duckdb.BinderException) as caught:
        duck.execute(sql.adbc_execute("profile://no_such_profile", "SELECT 1"))

    message = str(caught.value)
    assert "Profile not found" in message
    assert "no_such_profile" in message


def test_a_missing_driver_library_fails_while_the_call_is_bound(
    duck: duckdb.DuckDBPyConnection, profile_directory: Path, project: str
) -> None:
    """Write the profile directly because make_profile always uses the built driver."""

    missing = profile_directory / "not_a_driver_library.so"
    uri = profiles.write_profile(
        profile_directory,
        "missing_driver_library",
        driver=str(missing),
        options={"uri": f"bigquery:///{project}"},
    )

    with pytest.raises(duckdb.BinderException) as caught:
        duck.execute(sql.adbc_execute(uri, "SELECT 1"))

    message = str(caught.value)
    assert "Could not load" in message
    assert str(missing) in message


def test_execute_and_read_do_not_share_an_adbc_connection(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """Calls use separate connections, so only committed tables survive.

    read_adbc resolves its schema while binding, hence BinderException for the temporary table."""

    table = name("shared")
    temporary = name("temp")
    path = sql.qualified(project, dataset, table)

    session.execute(f"CREATE TABLE {path} AS SELECT 1 AS id")
    assert session.query(f"SELECT id FROM {path}") == [(1,)]

    session.execute(
        f"BEGIN\n  CREATE TEMP TABLE {sql.identifier(temporary)} AS SELECT 1 AS id;\n"
        f"  INSERT INTO {path} (id) SELECT id + 1 FROM {sql.identifier(temporary)};\n"
        "END"
    )
    assert session.query(f"SELECT id FROM {path} ORDER BY id") == [(1,), (2,)]

    with pytest.raises(duckdb.BinderException) as caught:
        session.query(f"SELECT id FROM {sql.identifier(temporary)}")
    message = str(caught.value)
    assert "notFound" in message
    assert temporary in message

    session.execute(f"DROP TABLE {path}")
