"""Metadata and reads through an attached DuckDB catalog.

ATTACH reads the current catalog and GetObjects(DB_SCHEMAS). Table resolution
uses SELECT * with the ATTACH delimiter, not GetTableSchema, and masks every
resolution error as a missing table. Whole-catalog listings resolve every table
in every dataset; disable missing-table suggestions to avoid that scan."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import duckdb
import pytest
import sql
from sql import Session

from tests.conftest import ATTACHED_CATALOG, MakeProfile

# A second catalog alias, for the tests that attach the same project twice.
OTHER_CATALOG = "bq_other"

PEOPLE = "attached_catalog_people"
NESTED = "attached_catalog_nested"
EMPTY = "attached_catalog_empty"
ACTIVE_PEOPLE = "attached_catalog_active_people"
REGIONS = "attached_catalog_regions"

# Expected `people` rows in id order, using DuckDB's Python result types.
PEOPLE_ROWS = [
    (1, "alice", 1.5, True, date(2024, 1, 1)),
    (2, "bob", 2.5, False, date(2024, 2, 2)),
    (3, "carol", 3.5, True, date(2024, 3, 3)),
    (4, "dave", 4.5, False, date(2024, 4, 4)),
]
REGION_ROWS = [(1, "north"), (2, "south"), (3, "east")]
PEOPLE_REGIONS = [("alice", "north"), ("bob", "south"), ("carol", "east")]

# DuckDB's "Did you mean ...?" suggestion for a failed lookup resolves every
# table in the project, one BigQuery query each. This disables it but keeps the
# error message.
CATALOG_ERROR_SUGGESTIONS_OFF = "SET catalog_error_max_schemas = 0"


@pytest.fixture(scope="module", autouse=True)
def tables(bigquery: Session, project: str, dataset: str, secondary_dataset: str) -> None:
    """Create the BigQuery objects every test in this module shares."""
    people = sql.qualified(project, dataset, PEOPLE)
    bigquery.execute(
        f"CREATE TABLE {people} AS "
        "SELECT 1 AS id, 'alice' AS name, 1.5 AS score, TRUE AS active, DATE '2024-01-01' AS day "
        "UNION ALL SELECT 2, 'bob', 2.5, FALSE, DATE '2024-02-02' "
        "UNION ALL SELECT 3, 'carol', 3.5, TRUE, DATE '2024-03-03' "
        "UNION ALL SELECT 4, 'dave', 4.5, FALSE, DATE '2024-04-04'"
    )
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, NESTED)} AS "
        "SELECT 1 AS id, [10, 20, 30] AS tags, STRUCT(7 AS a, 'seven' AS b) AS info"
    )
    # Its scan yields no batch.
    bigquery.execute(f"CREATE TABLE {sql.qualified(project, dataset, EMPTY)} (id INT64, label STRING)")
    bigquery.execute(
        f"CREATE VIEW {sql.qualified(project, dataset, ACTIVE_PEOPLE)} AS SELECT id, name FROM {people} WHERE active"
    )
    # In the secondary dataset, so a query can cross schemas in one catalog.
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, secondary_dataset, REGIONS)} AS "
        "SELECT 1 AS id, 'north' AS region UNION ALL SELECT 2, 'south' UNION ALL SELECT 3, 'east'"
    )


@pytest.fixture
def people(dataset: str) -> str:
    return sql.attached(ATTACHED_CATALOG, dataset, PEOPLE)


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[tuple]:
    return connection.execute(query).fetchall()


def _described(connection: duckdb.DuckDBPyConnection, table: str) -> list[tuple]:
    return [(row[0], row[1]) for row in _rows(connection, f"DESCRIBE {table}")]


def _physical_plan(connection: duckdb.DuckDBPyConnection, query: str) -> str:
    """Return plan metadata without depending on DuckDB's text rendering."""
    return "\n".join(str(row[-1]) for row in _rows(connection, f"EXPLAIN (FORMAT JSON) {query}"))


def test_attach_exposes_the_projects_datasets_as_schemas(
    attached: duckdb.DuckDBPyConnection, dataset: str, secondary_dataset: str
) -> None:
    """`duckdb_schemas()` lists the dataset names cached during `ATTACH`."""
    rows = _rows(
        attached,
        "SELECT schema_name FROM duckdb_schemas() "
        f"WHERE database_name = {sql.duckdb_literal(ATTACHED_CATALOG)} "
        f"AND schema_name IN ({sql.duckdb_literal(dataset)}, {sql.duckdb_literal(secondary_dataset)}) "
        "ORDER BY schema_name",
    )

    assert rows == sorted([(dataset,), (secondary_dataset,)])


def test_scan_and_describe_every_column(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    assert _rows(attached, f"SELECT * FROM {people} ORDER BY id") == PEOPLE_ROWS
    # DuckDB column types after the extension binds the driver's Arrow schema.
    assert _described(attached, people) == [
        ("id", "BIGINT"),
        ("name", "VARCHAR"),
        ("score", "DOUBLE"),
        ("active", "BOOLEAN"),
        ("day", "DATE"),
    ]


def test_order_by_sorts_the_scanned_rows(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    """DuckDB sorts locally; the driver is only asked for an unordered scan."""
    rows = _rows(attached, f"SELECT name FROM {people} ORDER BY score DESC")

    assert rows == [(row[1],) for row in reversed(PEOPLE_ROWS)]


def test_where_filter_is_evaluated_locally(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    """filter_pushdown is disabled; a Filters entry in scan metadata would mean that changed."""
    query = f"SELECT name, score FROM {people} WHERE active AND id > 1"

    assert _rows(attached, query) == [("carol", 3.5)]
    plan = _physical_plan(attached, query)
    assert "READ_ADBC" in plan
    assert "FILTER" in plan
    assert '"Filters"' not in plan


def test_projection_is_pruned_after_decoding(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    """The extension sends SELECT * and prunes decoded columns; the plan shows the projection."""
    query = f"SELECT name FROM {people} ORDER BY name"

    assert _rows(attached, query) == [("alice",), ("bob",), ("carol",), ("dave",)]
    plan = _physical_plan(attached, query)
    assert "name" in plan
    assert "score" not in plan


def test_aggregates_run_over_the_scanned_rows(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    """The extension reads only float64 statistics, so the driver's int64 numRows is unused."""
    assert _rows(attached, f"SELECT COUNT(*), SUM(id), MAX(score) FROM {people}") == [(4, 10, 4.5)]
    assert _rows(attached, f"SELECT active, COUNT(*) FROM {people} GROUP BY active ORDER BY active") == [
        (False, 2),
        (True, 2),
    ]


def test_table_in_the_secondary_dataset_is_scannable(
    attached: duckdb.DuckDBPyConnection, secondary_dataset: str
) -> None:
    regions = sql.attached(ATTACHED_CATALOG, secondary_dataset, REGIONS)

    assert _rows(attached, f"SELECT * FROM {regions} ORDER BY id") == REGION_ROWS


def test_use_makes_the_dataset_the_default_schema(attached: duckdb.DuckDBPyConnection, dataset: str) -> None:
    attached.execute(f"USE {sql.duckdb_identifier(ATTACHED_CATALOG)}.{sql.duckdb_identifier(dataset)}")

    assert _rows(attached, f"SELECT name FROM {sql.duckdb_identifier(PEOPLE)} WHERE id = 2") == [("bob",)]


def test_view_is_resolved_and_scanned_like_a_table(attached: duckdb.DuckDBPyConnection, dataset: str) -> None:
    """Resolution runs `SELECT *`, so a view binds exactly like a table."""
    view = sql.attached(ATTACHED_CATALOG, dataset, ACTIVE_PEOPLE)

    assert _rows(attached, f"SELECT * FROM {view} ORDER BY id") == [(row[0], row[1]) for row in PEOPLE_ROWS if row[3]]


def test_nested_column_types_survive_resolution(attached: duckdb.DuckDBPyConnection, dataset: str) -> None:
    nested = sql.attached(ATTACHED_CATALOG, dataset, NESTED)

    assert _described(attached, nested) == [
        ("id", "BIGINT"),
        ("tags", "BIGINT[]"),
        ("info", "STRUCT(a BIGINT, b VARCHAR)"),
    ]
    assert _rows(attached, f"SELECT * FROM {nested}") == [(1, [10, 20, 30], {"a": 7, "b": "seven"})]
    # The nested values are ordinary DuckDB values once decoded.
    assert _rows(attached, f"SELECT tags[2], info.b FROM {nested}") == [(20, "seven")]


def test_empty_table_resolves_and_scans_to_no_rows(attached: duckdb.DuckDBPyConnection, dataset: str) -> None:
    empty = sql.attached(ATTACHED_CATALOG, dataset, EMPTY)

    assert _described(attached, empty) == [("id", "BIGINT"), ("label", "VARCHAR")]
    assert _rows(attached, f"SELECT * FROM {empty}") == []
    assert _rows(attached, f"SELECT COUNT(*) FROM {empty}") == [(0,)]


def test_table_created_after_attach_is_visible_without_clearing_the_cache(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    people: str,
    project: str,
    dataset: str,
    name: Callable[[str], str],
) -> None:
    """Only resolved tables are cached unless a whole-catalog scan has run."""
    # Cache the schema entry before the new table exists.
    assert _rows(attached, f"SELECT COUNT(*) FROM {people}") == [(4,)]

    created = name("late")
    bigquery.execute(f"CREATE TABLE {sql.qualified(project, dataset, created)} AS SELECT 'late' AS label")

    assert _rows(attached, f"SELECT * FROM {sql.attached(ATTACHED_CATALOG, dataset, created)}") == [("late",)]


def test_adbc_clear_cache_refreshes_a_stale_table_entry(
    attached: duckdb.DuckDBPyConnection,
    bigquery: Session,
    project: str,
    dataset: str,
    name: Callable[[str], str],
) -> None:
    """adbc_clear_cache discards every attachment's cached entries; the next reference rebinds."""
    table = name("evolving")
    qualified = sql.qualified(project, dataset, table)
    bigquery.execute(f"CREATE TABLE {qualified} AS SELECT 1 AS id")
    evolving = sql.attached(ATTACHED_CATALOG, dataset, table)
    assert _rows(attached, f"SELECT * FROM {evolving}") == [(1,)]

    bigquery.execute(f"ALTER TABLE {qualified} ADD COLUMN label STRING")
    assert [column for column, _ in _described(attached, evolving)] == ["id"]

    attached.execute("CALL adbc_clear_cache()")
    assert [column for column, _ in _described(attached, evolving)] == ["id", "label"]
    assert _rows(attached, f"SELECT * FROM {evolving}") == [(1, None)]


def test_detach_and_reattach_on_the_same_connection(duck: duckdb.DuckDBPyConnection, profile: str, people: str) -> None:
    """Reattach repeats GetOption and GetObjects, catching stale per-process state."""
    count = f"SELECT COUNT(*) FROM {people}"
    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert _rows(duck, count) == [(4,)]

    duck.execute(f"DETACH {sql.duckdb_identifier(ATTACHED_CATALOG)}")
    with pytest.raises(duckdb.CatalogException, match="does not exist because schema"):
        duck.execute(count)

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert _rows(duck, count) == [(4,)]


def test_two_catalogs_attached_from_two_profiles(
    attached: duckdb.DuckDBPyConnection, make_profile: MakeProfile, people: str, secondary_dataset: str
) -> None:
    """The second profile differs in bigquery.dataset_id."""
    attached.execute(
        sql.attach(make_profile("attached_other", **{"bigquery.dataset_id": secondary_dataset}), OTHER_CATALOG)
    )
    regions = sql.attached(OTHER_CATALOG, secondary_dataset, REGIONS)

    assert _rows(attached, f"SELECT * FROM {regions} ORDER BY id") == REGION_ROWS
    assert (
        _rows(attached, f"SELECT p.name, r.region FROM {people} AS p JOIN {regions} AS r USING (id) ORDER BY p.id")
        == PEOPLE_REGIONS
    )


def test_join_of_two_tables_in_one_attached_catalog(
    attached: duckdb.DuckDBPyConnection, people: str, secondary_dataset: str
) -> None:
    """Each side is its own `SELECT *` on the attachment's pool; DuckDB joins them."""
    regions = sql.attached(ATTACHED_CATALOG, secondary_dataset, REGIONS)

    assert (
        _rows(attached, f"SELECT p.name, r.region FROM {people} AS p JOIN {regions} AS r ON p.id = r.id ORDER BY p.id")
        == PEOPLE_REGIONS
    )


def test_join_with_a_local_duckdb_table(attached: duckdb.DuckDBPyConnection, people: str) -> None:
    """Qualify memory.main because ATTACH may make the ADBC catalog the default."""
    attached.execute(
        "CREATE TABLE memory.main.local_scores AS SELECT * FROM (VALUES (1, 'gold'), (3, 'silver')) AS t(id, medal)"
    )

    rows = _rows(
        attached,
        f"SELECT p.name, l.medal FROM {people} AS p JOIN memory.main.local_scores AS l ON p.id = l.id ORDER BY p.id",
    )
    assert rows == [("alice", "gold"), ("carol", "silver")]


def test_default_delimiter_resolves_no_table(
    attached: duckdb.DuckDBPyConnection, profile: str, dataset: str, people: str
) -> None:
    """Default double quotes become GoogleSQL strings; resolution masks the error as a missing table."""
    attached.execute(CATALOG_ERROR_SUGGESTIONS_OFF)
    attached.execute(sql.attach(profile, "raw", delimiter=None))

    # Listing is unaffected: only the resolution query uses the delimiter.
    assert _rows(
        attached,
        "SELECT COUNT(*) FROM duckdb_schemas() "
        f"WHERE database_name = 'raw' AND schema_name = {sql.duckdb_literal(dataset)}",
    ) == [(1,)]
    # The backtick-delimited attachment resolves the same table.
    assert _rows(attached, f"SELECT COUNT(*) FROM {people}") == [(4,)]
    with pytest.raises(duckdb.CatalogException, match=f"Table with name {PEOPLE} does not exist"):
        attached.execute(f"SELECT * FROM {sql.attached('raw', dataset, PEOPLE)}")


def test_missing_table_reports_a_catalog_error(
    attached: duckdb.DuckDBPyConnection, dataset: str, name: Callable[[str], str]
) -> None:
    attached.execute(CATALOG_ERROR_SUGGESTIONS_OFF)
    missing = name("absent")

    with pytest.raises(duckdb.CatalogException, match=f"Table with name {missing} does not exist"):
        attached.execute(f"SELECT * FROM {sql.attached(ATTACHED_CATALOG, dataset, missing)}")


def test_creating_a_view_through_the_catalog_is_unsupported(
    attached: duckdb.DuckDBPyConnection, dataset: str, name: Callable[[str], str]
) -> None:
    """Dropping a table is in test_write.py's test_write_statements_the_extension_does_not_implement."""
    view = sql.attached(ATTACHED_CATALOG, dataset, name("view"))
    with pytest.raises(duckdb.NotImplementedException, match="CreateView not yet supported with the ADBC extension"):
        attached.execute(f"CREATE VIEW {view} AS SELECT 1 AS id")

    schema = f"{sql.duckdb_identifier(ATTACHED_CATALOG)}.{sql.duckdb_identifier(name('schema'))}"
    with pytest.raises(duckdb.NotImplementedException, match="CREATE SCHEMA not yet supported with the ADBC extension"):
        attached.execute(f"CREATE SCHEMA {schema}")
