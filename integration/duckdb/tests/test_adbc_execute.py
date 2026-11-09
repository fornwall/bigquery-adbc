"""DDL, DML, scripts, and routines through adbc_execute().

ExecuteQuery receives null stream and row-count pointers; verify effects via
read_adbc(). Profile/load failures raise BinderException (test_driver_options.py);
execution failures raise IOException with the driver message but no status,
SQLSTATE, or vendor code."""

from __future__ import annotations

from collections.abc import Callable

import duckdb
import pytest
import sql
from sql import Session

# Same location as the run's datasets, so one job can read across them.
LOCATION = "US"


def _columns(session: Session, project: str, dataset: str, table: str) -> list[tuple]:
    return session.query(
        f"SELECT column_name, data_type FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = {sql.string_literal(table)} ORDER BY ordinal_position"
    )


def _table_type(session: Session, project: str, dataset: str, table: str) -> str | None:
    """Return the object's INFORMATION_SCHEMA.TABLES type, or None if it does not exist."""
    rows = session.query(
        f"SELECT table_type FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES "
        f"WHERE table_name = {sql.string_literal(table)}"
    )
    return rows[0][0] if rows else None


def _table_option(session: Session, project: str, dataset: str, table: str, option: str) -> str | None:
    """Return one `TABLE_OPTIONS` value, still in its GoogleSQL literal form."""
    rows = session.query(
        f"SELECT option_value FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLE_OPTIONS "
        f"WHERE table_name = {sql.string_literal(table)} AND option_name = {sql.string_literal(option)}"
    )
    return str(rows[0][0]) if rows else None


def test_call_reports_a_single_true_success_row(duck: duckdb.DuckDBPyConnection, profile: str) -> None:
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
    assert _columns(session, project, dataset, table) == [("id", "INT64"), ("label", "STRING")]

    session.execute(f"ALTER TABLE {path} ADD COLUMN score FLOAT64")
    session.execute(f"ALTER TABLE {path} RENAME COLUMN label TO title")
    assert _columns(session, project, dataset, table) == [("id", "INT64"), ("title", "STRING"), ("score", "FLOAT64")]

    session.execute(f"DROP TABLE {path}")
    assert _table_type(session, project, dataset, table) is None


def test_create_table_as_select_replace_and_if_not_exists(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    path = sql.qualified(project, dataset, name("ctas"))

    session.execute(f"CREATE TABLE {path} AS SELECT 1 AS id, 'first' AS label")
    assert session.query(f"SELECT id, label FROM {path}") == [(1, "first")]

    session.execute(f"CREATE OR REPLACE TABLE {path} AS SELECT 2 AS id, 'second' AS label")
    assert session.query(f"SELECT id, label FROM {path}") == [(2, "second")]

    session.execute(f"CREATE TABLE IF NOT EXISTS {path} (id INT64, label STRING)")
    session.execute(f"CREATE TABLE IF NOT EXISTS {path} (id INT64, label STRING)")
    assert session.query(f"SELECT id, label FROM {path}") == [(2, "second")]

    session.execute(f"DROP TABLE {path}")


def test_drop_table_if_exists_on_a_missing_table_succeeds(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    session.execute(f"DROP TABLE IF EXISTS {sql.qualified(project, dataset, name('never_created'))}")


def test_view_lifecycle(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    table_path = sql.qualified(project, dataset, name("base"))
    view = name("view")
    view_path = sql.qualified(project, dataset, view)

    session.execute(f"CREATE TABLE {table_path} AS SELECT * FROM UNNEST([STRUCT(1 AS id, 10 AS value), (2, 20)])")
    session.execute(f"CREATE VIEW {view_path} AS SELECT id FROM {table_path}")

    assert session.query(f"SELECT id FROM {view_path} ORDER BY id") == [(1,), (2,)]
    assert _table_type(session, project, dataset, view) == "VIEW"

    session.execute(f"CREATE OR REPLACE VIEW {view_path} AS SELECT id FROM {table_path} WHERE value > 15")
    assert session.query(f"SELECT id FROM {view_path}") == [(2,)]

    session.execute(f"DROP VIEW {view_path}")
    assert _table_type(session, project, dataset, view) is None
    session.execute(f"DROP TABLE {table_path}")


def test_materialized_view_lifecycle(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    """A materialized view's DDL job also writes data; the tiny base table keeps it cheap."""
    table_path = sql.qualified(project, dataset, name("mvbase"))
    view = name("mv")
    view_path = sql.qualified(project, dataset, view)

    session.execute(
        f"CREATE TABLE {table_path} AS SELECT * FROM UNNEST([STRUCT('eu' AS region, 1 AS value), ('eu', 2), ('us', 4)])"
    )
    session.execute(
        f"CREATE MATERIALIZED VIEW {view_path} AS SELECT region, SUM(value) AS total FROM {table_path} GROUP BY region"
    )

    assert session.query(f"SELECT region, total FROM {view_path} ORDER BY region") == [("eu", 3), ("us", 4)]
    assert _table_type(session, project, dataset, view) == "MATERIALIZED VIEW"

    session.execute(f"DROP MATERIALIZED VIEW {view_path}")
    assert _table_type(session, project, dataset, view) is None
    session.execute(f"DROP TABLE {table_path}")


def test_insert_update_and_delete(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    """`adbc_execute()` reports no row count, so each statement's effect is read back."""
    path = sql.qualified(project, dataset, name("dml"))
    session.execute(f"CREATE TABLE {path} (id INT64, label STRING)")

    session.execute(f"INSERT INTO {path} (id, label) VALUES (1, 'one'), (2, 'two')")
    assert session.query(f"SELECT id, label FROM {path} ORDER BY id") == [(1, "one"), (2, "two")]

    session.execute(f"UPDATE {path} SET label = 'renamed' WHERE id = 2")
    assert session.query(f"SELECT label FROM {path} WHERE id = 2") == [("renamed",)]

    session.execute(f"DELETE FROM {path} WHERE id = 1")
    assert session.query(f"SELECT id FROM {path}") == [(2,)]

    session.execute(f"DROP TABLE {path}")


def test_merge_updates_and_inserts(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    path = sql.qualified(project, dataset, name("merge"))
    session.execute(f"CREATE TABLE {path} AS SELECT * FROM UNNEST([STRUCT(1 AS id, 'one' AS label), (2, 'two')])")

    session.execute(
        f"MERGE {path} AS target "
        "USING UNNEST([STRUCT(2 AS id, 'TWO' AS label), (3, 'three')]) AS source "
        "ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET label = source.label "
        "WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label)"
    )

    assert session.query(f"SELECT id, label FROM {path} ORDER BY id") == [(1, "one"), (2, "TWO"), (3, "three")]
    session.execute(f"DROP TABLE {path}")


def test_truncate_table_empties_it(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    """TRUNCATE is DML in GoogleSQL, and leaves the table itself behind."""
    table = name("truncate")
    path = sql.qualified(project, dataset, table)
    session.execute(f"CREATE TABLE {path} AS SELECT id FROM UNNEST([1, 2, 3]) AS id")

    session.execute(f"TRUNCATE TABLE {path}")

    assert session.scalar(f"SELECT COUNT(*) AS n FROM {path}") == 0
    assert _table_type(session, project, dataset, table) == "BASE TABLE"
    session.execute(f"DROP TABLE {path}")


def test_multi_statement_script(session: Session, project: str, dataset: str, name: Callable[[str], str]) -> None:
    """A `BEGIN ... END` script job has its own statement type and no result schema."""
    path = sql.qualified(project, dataset, name("script"))
    session.execute(f"CREATE TABLE {path} (id INT64)")

    session.execute(f"BEGIN\n  INSERT INTO {path} (id) VALUES (1);\n  INSERT INTO {path} (id) VALUES (2);\nEND")

    assert session.query(f"SELECT id FROM {path} ORDER BY id") == [(1,), (2,)]
    session.execute(f"DROP TABLE {path}")


def test_script_with_declared_variables(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """`DECLARE` and `SET` need the scripting path, not the query path."""
    path = sql.qualified(project, dataset, name("declare"))
    session.execute(f"CREATE TABLE {path} (id INT64)")

    session.execute(
        "DECLARE threshold INT64 DEFAULT 0;\n"
        "SET threshold = 2;\n"
        f"INSERT INTO {path} (id) SELECT id FROM UNNEST([1, 2, 3]) AS id WHERE id > threshold;"
    )

    assert session.query(f"SELECT id FROM {path}") == [(3,)]
    session.execute(f"DROP TABLE {path}")


def test_a_failing_script_keeps_what_ran_before_it(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """A script is not a transaction; statements before the failure commit."""
    path = sql.qualified(project, dataset, name("partial"))
    session.execute(f"CREATE TABLE {path} (id INT64)")

    with pytest.raises(duckdb.IOException, match="deliberate failure halfway through the script"):
        session.execute(
            f"BEGIN\n  INSERT INTO {path} (id) VALUES (1);\n"
            "  SELECT ERROR('deliberate failure halfway through the script');\nEND"
        )

    assert session.query(f"SELECT id FROM {path}") == [(1,)]
    session.execute(f"DROP TABLE {path}")


def test_create_and_drop_a_schema(session: Session, project: str, name: Callable[[str], str]) -> None:
    """The runner does not clean up this separate dataset; drop it in finally."""
    own_dataset = name("dataset")
    path = sql.qualified(project, own_dataset)
    exists = (
        f"SELECT COUNT(*) AS n FROM {sql.identifier(project)}.INFORMATION_SCHEMA.SCHEMATA "
        f"WHERE schema_name = {sql.string_literal(own_dataset)}"
    )

    session.execute(f"CREATE SCHEMA {path} OPTIONS(location = {sql.string_literal(LOCATION)})")
    try:
        assert session.scalar(exists) == 1
    finally:
        session.execute(f"DROP SCHEMA {path}")
    assert session.scalar(exists) == 0


def test_ddl_in_the_secondary_dataset(
    session: Session, project: str, dataset: str, secondary_dataset: str, name: Callable[[str], str]
) -> None:
    """`bigquery.dataset_id` supplies a default dataset but does not confine statements to it."""
    table = name("secondary")
    path = sql.qualified(project, secondary_dataset, table)

    session.execute(f"CREATE TABLE {path} AS SELECT 1 AS id")
    try:
        assert _table_type(session, project, secondary_dataset, table) == "BASE TABLE"
        assert _table_type(session, project, dataset, table) is None
        assert session.query(f"SELECT id FROM {path}") == [(1,)]
    finally:
        session.execute(f"DROP TABLE {path}")


def test_sql_udf_created_by_execute_and_called_by_a_scan(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    function = name("udf")
    path = sql.qualified(project, dataset, function)

    session.execute(f"CREATE FUNCTION {path}(value INT64) AS (value + 1)")
    try:
        assert session.scalar(f"SELECT {path}(41) AS answer") == 42
        assert (
            session.scalar(
                f"SELECT routine_type FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.ROUTINES "
                f"WHERE routine_name = {sql.string_literal(function)}"
            )
            == "FUNCTION"
        )
    finally:
        session.execute(f"DROP FUNCTION {path}")


def test_stored_procedure_created_and_called_by_execute(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table_path = sql.qualified(project, dataset, name("proctarget"))
    procedure_path = sql.qualified(project, dataset, name("proc"))
    session.execute(f"CREATE TABLE {table_path} (id INT64)")

    session.execute(
        f"CREATE PROCEDURE {procedure_path}(IN id INT64) BEGIN INSERT INTO {table_path} (id) VALUES (id); END"
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
    labels = '[STRUCT("suite", "duckdb")]'

    session.execute(
        f"CREATE TABLE {path} (id INT64) "
        f"OPTIONS(description = {sql.string_literal('created with a description')}, labels = [('suite', 'duckdb')])"
    )
    assert _table_option(session, project, dataset, table, "description") == '"created with a description"'
    assert _table_option(session, project, dataset, table, "labels") == labels

    session.execute(f"ALTER TABLE {path} SET OPTIONS(description = {sql.string_literal('altered afterwards')})")
    assert _table_option(session, project, dataset, table, "description") == '"altered afterwards"'
    # SET OPTIONS replaces only the options it names.
    assert _table_option(session, project, dataset, table, "labels") == labels

    session.execute(f"DROP TABLE {path}")


def test_partitioned_and_clustered_table(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    table = name("partitioned")
    path = sql.qualified(project, dataset, table)

    session.execute(f"CREATE TABLE {path} (day DATE, region STRING, value INT64) PARTITION BY day CLUSTER BY region")
    assert session.query(
        "SELECT column_name, is_partitioning_column, clustering_ordinal_position "
        f"FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name = {sql.string_literal(table)} ORDER BY ordinal_position"
    ) == [("day", "YES", None), ("region", "NO", 1), ("value", "NO", None)]

    session.execute(
        f"INSERT INTO {path} (day, region, value) VALUES (DATE '2024-01-01', 'eu', 1), (DATE '2024-02-01', 'us', 2)"
    )
    assert session.query(f"SELECT value FROM {path} WHERE day = DATE '2024-02-01' AND region = 'us'") == [(2,)]

    session.execute(f"DROP TABLE {path}")


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        # The extension forwards only the message; the reason is all that remains of the driver status.
        ("CREATE TABLE SELECT WHERE", ["invalidQuery", "Syntax error"]),
        ("INSERT INTO `{project}`.`{dataset}`.`{table}` (id) VALUES (1)", ["notFound", "{table}"]),
        # Refused before any job runs.
        (
            "CREATE TABLE `bigquery-public-data`.`samples`.`{table}` (id INT64)",
            ["accessDenied", "bigquery.tables.create"],
        ),
        # The driver, not BigQuery, rejects a comment-only statement.
        ("-- nothing but a comment\n", ["without a SQL query"]),
    ],
    ids=["syntax", "missing_table", "permission", "empty"],
)
def test_a_failed_statement_reports_the_message(
    session: Session, project: str, dataset: str, name: Callable[[str], str], statement: str, expected: list[str]
) -> None:
    values = {"project": project, "dataset": dataset, "table": name("absent")}

    with pytest.raises(duckdb.IOException) as caught:
        session.execute(statement.format(**values))

    for text in expected:
        assert text.format(**values) in str(caught.value)


def test_execute_and_read_do_not_share_an_adbc_connection(
    session: Session, project: str, dataset: str, name: Callable[[str], str]
) -> None:
    """Calls use separate connections, so only committed tables survive.

    read_adbc resolves its schema while binding, hence BinderException for the temporary table."""
    path = sql.qualified(project, dataset, name("shared"))
    temporary = name("temp")

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
    assert "notFound" in str(caught.value)
    assert temporary in str(caught.value)

    session.execute(f"DROP TABLE {path}")
