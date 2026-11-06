"""Metadata and reads through an attached DuckDB catalog.

ATTACH reads the current catalog and GetObjects(DB_SCHEMAS). Table resolution
uses SELECT * with the ATTACH delimiter, not GetTableSchema, and masks every
resolution error as a missing table. Whole-catalog listings resolve every table
in every dataset; disable missing-table suggestions to avoid that scan."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import duckdb
import pytest
import sql
from sql import Session

from tests.conftest import ATTACHED_CATALOG

# A second catalog alias, for the tests that attach the same project twice.
OTHER_CATALOG = "bq_other"

# Expected `people` rows in id order, using DuckDB's Python result types.
PEOPLE_ROWS = (
    (1, "alice", 1.5, True, date(2024, 1, 1)),
    (2, "bob", 2.5, False, date(2024, 2, 2)),
    (3, "carol", 3.5, True, date(2024, 3, 3)),
    (4, "dave", 4.5, False, date(2024, 4, 4)),
)

# DuckDB column types after the extension binds the driver's Arrow schema.
PEOPLE_COLUMNS = (
    ("id", "BIGINT"),
    ("name", "VARCHAR"),
    ("score", "DOUBLE"),
    ("active", "BOOLEAN"),
    ("day", "DATE"),
)

REGION_ROWS = ((1, "north"), (2, "south"), (3, "east"))

# DuckDB's "Did you mean ...?" suggestion for a failed lookup resolves every
# table in the project, one BigQuery query each. This disables it but keeps the
# error message.
CATALOG_ERROR_SUGGESTIONS_OFF = "SET catalog_error_max_schemas = 0"


@dataclass(frozen=True)
class Tables:
    """The BigQuery objects every test in this module shares."""

    people: str
    nested: str
    empty: str
    active_people: str
    regions: str


def _shared_name(suffix: str) -> str:
    """Name a shared fixture within the run's isolated dataset."""

    return f"attached_catalog_{suffix}"


@pytest.fixture(scope="module")
def tables(
    bigquery: Session, project: str, dataset: str, secondary_dataset: str
) -> Tables:
    """Create the module's shared BigQuery objects once."""

    people = _shared_name("people")
    nested = _shared_name("nested")
    empty = _shared_name("empty")
    active_people = _shared_name("active_people")
    regions = _shared_name("regions")

    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, people)} AS "
        "SELECT 1 AS id, 'alice' AS name, 1.5 AS score, "
        "TRUE AS active, DATE '2024-01-01' AS day "
        "UNION ALL SELECT 2, 'bob', 2.5, FALSE, DATE '2024-02-02' "
        "UNION ALL SELECT 3, 'carol', 3.5, TRUE, DATE '2024-03-03' "
        "UNION ALL SELECT 4, 'dave', 4.5, FALSE, DATE '2024-04-04'"
    )
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, nested)} AS "
        "SELECT 1 AS id, [10, 20, 30] AS tags, "
        "STRUCT(7 AS a, 'seven' AS b) AS info"
    )
    # Its scan yields no batch.
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, empty)} "
        "(id INT64, label STRING)"
    )
    bigquery.execute(
        f"CREATE VIEW {sql.qualified(project, dataset, active_people)} AS "
        f"SELECT id, name FROM {sql.qualified(project, dataset, people)} "
        "WHERE active"
    )
    # In the secondary dataset, so a query can cross schemas in one catalog.
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, secondary_dataset, regions)} AS "
        "SELECT 1 AS id, 'north' AS region "
        "UNION ALL SELECT 2, 'south' "
        "UNION ALL SELECT 3, 'east'"
    )
    return Tables(
        people=people,
        nested=nested,
        empty=empty,
        active_people=active_people,
        regions=regions,
    )


@pytest.fixture
def people(tables: Tables, dataset: str) -> str:
    """The shared `people` table, named for DuckDB as `bq.<dataset>.people`."""

    return sql.attached(ATTACHED_CATALOG, dataset, tables.people)


def physical_plan(connection: duckdb.DuckDBPyConnection, query: str) -> str:
    """Return plan metadata without depending on DuckDB's text rendering."""

    rows = connection.execute(f"EXPLAIN (FORMAT JSON) {query}").fetchall()
    return "\n".join(str(row[-1]) for row in rows)


def test_attach_exposes_the_projects_datasets_as_schemas(
    attached: duckdb.DuckDBPyConnection, dataset: str, secondary_dataset: str
) -> None:
    """`duckdb_schemas()` lists the dataset names cached during `ATTACH`."""

    rows = attached.execute(
        "SELECT schema_name FROM duckdb_schemas() "
        f"WHERE database_name = {sql.duckdb_literal(ATTACHED_CATALOG)} "
        f"AND schema_name IN ({sql.duckdb_literal(dataset)}, "
        f"{sql.duckdb_literal(secondary_dataset)}) "
        "ORDER BY schema_name"
    ).fetchall()

    assert rows == sorted([(dataset,), (secondary_dataset,)])


def test_scan_returns_every_row(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    rows = attached.execute(f"SELECT * FROM {people} ORDER BY id").fetchall()

    assert rows == list(PEOPLE_ROWS)


def test_describe_reports_the_duckdb_types_of_every_column(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    described = attached.execute(f"DESCRIBE {people}").fetchall()

    assert tuple((row[0], row[1]) for row in described) == PEOPLE_COLUMNS


def test_order_by_sorts_the_scanned_rows(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    """DuckDB sorts locally; the driver is only asked for an unordered scan."""

    rows = attached.execute(f"SELECT name FROM {people} ORDER BY score DESC").fetchall()

    assert rows == [(row[1],) for row in reversed(PEOPLE_ROWS)]


def test_where_filter_is_evaluated_locally(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    """filter_pushdown is disabled; a Filters entry in scan metadata would mean that changed."""

    query = f"SELECT name, score FROM {people} WHERE active AND id > 1"
    rows = attached.execute(query).fetchall()

    assert rows == [("carol", 3.5)]

    plan = physical_plan(attached, query)
    assert "READ_ADBC" in plan
    assert "FILTER" in plan
    assert '"Filters"' not in plan


def test_projection_is_pruned_after_decoding(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    """The extension sends SELECT * and prunes decoded columns; the plan shows the projection."""

    query = f"SELECT name FROM {people} ORDER BY name"
    rows = attached.execute(query).fetchall()

    assert rows == [("alice",), ("bob",), ("carol",), ("dave",)]

    plan = physical_plan(attached, query)
    assert "name" in plan
    assert "score" not in plan


def test_aggregates_run_over_the_scanned_rows(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    total, ids, highest = attached.execute(
        f"SELECT COUNT(*), SUM(id), MAX(score) FROM {people}"
    ).fetchone()

    assert total == len(PEOPLE_ROWS)
    assert ids == sum(row[0] for row in PEOPLE_ROWS)
    assert highest == max(row[2] for row in PEOPLE_ROWS)

    grouped = attached.execute(
        f"SELECT active, COUNT(*) FROM {people} GROUP BY active ORDER BY active"
    ).fetchall()

    assert grouped == [(False, 2), (True, 2)]


def test_scan_of_a_table_with_a_known_row_count(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    """The extension reads only float64 statistics, so the driver's int64 numRows is unavailable."""

    scanned = attached.execute(f"SELECT COUNT(*) FROM {people}").fetchone()

    assert scanned == (len(PEOPLE_ROWS),)


def test_table_in_the_secondary_dataset_is_scannable(
    attached: duckdb.DuckDBPyConnection, tables: Tables, secondary_dataset: str
) -> None:
    regions = sql.attached(ATTACHED_CATALOG, secondary_dataset, tables.regions)
    rows = attached.execute(f"SELECT * FROM {regions} ORDER BY id").fetchall()

    assert rows == list(REGION_ROWS)


def test_use_makes_the_dataset_the_default_schema(
    attached: duckdb.DuckDBPyConnection, tables: Tables, dataset: str
) -> None:
    attached.execute(
        f"USE {sql.duckdb_identifier(ATTACHED_CATALOG)}."
        f"{sql.duckdb_identifier(dataset)}"
    )

    rows = attached.execute(
        f"SELECT name FROM {sql.duckdb_identifier(tables.people)} WHERE id = 2"
    ).fetchall()

    assert rows == [("bob",)]


def test_view_is_resolved_and_scanned_like_a_table(
    attached: duckdb.DuckDBPyConnection, tables: Tables, dataset: str
) -> None:
    """Resolution runs `SELECT *`, so a view binds exactly like a table."""

    view = sql.attached(ATTACHED_CATALOG, dataset, tables.active_people)
    rows = attached.execute(f"SELECT * FROM {view} ORDER BY id").fetchall()

    assert rows == [(row[0], row[1]) for row in PEOPLE_ROWS if row[3]]


def test_nested_column_types_survive_resolution(
    attached: duckdb.DuckDBPyConnection, tables: Tables, dataset: str
) -> None:
    nested = sql.attached(ATTACHED_CATALOG, dataset, tables.nested)

    described = attached.execute(f"DESCRIBE {nested}").fetchall()
    assert tuple((row[0], row[1]) for row in described) == (
        ("id", "BIGINT"),
        ("tags", "BIGINT[]"),
        ("info", "STRUCT(a BIGINT, b VARCHAR)"),
    )

    assert attached.execute(f"SELECT * FROM {nested}").fetchall() == [
        (1, [10, 20, 30], {"a": 7, "b": "seven"})
    ]

    # The nested values are ordinary DuckDB values once decoded.
    assert attached.execute(
        f"SELECT tags[2], info.b FROM {nested}"
    ).fetchall() == [(20, "seven")]


def test_empty_table_resolves_and_scans_to_no_rows(
    attached: duckdb.DuckDBPyConnection, tables: Tables, dataset: str
) -> None:
    empty = sql.attached(ATTACHED_CATALOG, dataset, tables.empty)

    described = attached.execute(f"DESCRIBE {empty}").fetchall()
    assert tuple((row[0], row[1]) for row in described) == (
        ("id", "BIGINT"),
        ("label", "VARCHAR"),
    )
    assert attached.execute(f"SELECT * FROM {empty}").fetchall() == []
    assert attached.execute(f"SELECT COUNT(*) FROM {empty}").fetchone() == (0,)


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
    assert attached.execute(f"SELECT COUNT(*) FROM {people}").fetchone() == (
        len(PEOPLE_ROWS),
    )

    created = name("late")
    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, created)} AS "
        "SELECT 'late' AS label"
    )

    late = sql.attached(ATTACHED_CATALOG, dataset, created)
    assert attached.execute(f"SELECT * FROM {late}").fetchall() == [("late",)]


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
    assert attached.execute(f"SELECT * FROM {evolving}").fetchall() == [(1,)]

    bigquery.execute(f"ALTER TABLE {qualified} ADD COLUMN label STRING")

    stale = attached.execute(f"DESCRIBE {evolving}").fetchall()
    assert [row[0] for row in stale] == ["id"]

    attached.execute("CALL adbc_clear_cache()")

    refreshed = attached.execute(f"DESCRIBE {evolving}").fetchall()
    assert [row[0] for row in refreshed] == ["id", "label"]
    assert attached.execute(f"SELECT * FROM {evolving}").fetchall() == [(1, None)]


def test_detach_and_reattach_on_the_same_connection(
    duck: duckdb.DuckDBPyConnection, profile: str, people: str
) -> None:
    """Reattach repeats GetOption and GetObjects, catching stale per-process state."""

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert duck.execute(f"SELECT COUNT(*) FROM {people}").fetchone() == (
        len(PEOPLE_ROWS),
    )

    duck.execute(f"DETACH {sql.duckdb_identifier(ATTACHED_CATALOG)}")
    with pytest.raises(duckdb.CatalogException, match="does not exist because schema"):
        duck.execute(f"SELECT COUNT(*) FROM {people}")

    duck.execute(sql.attach(profile, ATTACHED_CATALOG))
    assert duck.execute(f"SELECT COUNT(*) FROM {people}").fetchone() == (
        len(PEOPLE_ROWS),
    )


def test_two_catalogs_attached_from_two_profiles(
    attached: duckdb.DuckDBPyConnection,
    make_profile: Callable[..., str],
    tables: Tables,
    people: str,
    dataset: str,
    secondary_dataset: str,
) -> None:
    """The second profile differs in bigquery.dataset_id."""

    other_profile = make_profile(
        "attached_other", **{"bigquery.dataset_id": secondary_dataset}
    )
    attached.execute(sql.attach(other_profile, OTHER_CATALOG))

    regions = sql.attached(OTHER_CATALOG, secondary_dataset, tables.regions)
    assert attached.execute(f"SELECT * FROM {regions} ORDER BY id").fetchall() == list(
        REGION_ROWS
    )

    joined = attached.execute(
        f"SELECT p.name, r.region FROM {people} AS p "
        f"JOIN {regions} AS r USING (id) ORDER BY p.id"
    ).fetchall()

    assert joined == [("alice", "north"), ("bob", "south"), ("carol", "east")]


def test_join_of_two_tables_in_one_attached_catalog(
    attached: duckdb.DuckDBPyConnection,
    tables: Tables,
    people: str,
    secondary_dataset: str,
) -> None:
    """Each side is its own `SELECT *` on the attachment's pool; DuckDB joins them."""

    regions = sql.attached(ATTACHED_CATALOG, secondary_dataset, tables.regions)
    rows = attached.execute(
        f"SELECT p.name, r.region FROM {people} AS p "
        f"JOIN {regions} AS r ON p.id = r.id ORDER BY p.id"
    ).fetchall()

    assert rows == [("alice", "north"), ("bob", "south"), ("carol", "east")]


def test_join_with_a_local_duckdb_table(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    """Qualify memory.main because ATTACH may make the ADBC catalog the default."""

    attached.execute(
        "CREATE TABLE memory.main.local_scores AS "
        "SELECT * FROM (VALUES (1, 'gold'), (3, 'silver')) AS t(id, medal)"
    )

    rows = attached.execute(
        f"SELECT p.name, l.medal FROM {people} AS p "
        "JOIN memory.main.local_scores AS l ON p.id = l.id ORDER BY p.id"
    ).fetchall()

    assert rows == [("alice", "gold"), ("carol", "silver")]


def test_default_delimiter_resolves_no_table(
    attached: duckdb.DuckDBPyConnection,
    profile: str,
    tables: Tables,
    dataset: str,
    people: str,
) -> None:
    """Default double quotes become GoogleSQL strings; resolution masks the error as a missing table."""

    attached.execute(CATALOG_ERROR_SUGGESTIONS_OFF)

    default_delimiter = "raw"
    attached.execute(sql.attach(profile, default_delimiter, delimiter=None))

    # Listing is unaffected: only the resolution query uses the delimiter.
    assert attached.execute(
        "SELECT COUNT(*) FROM duckdb_schemas() "
        f"WHERE database_name = {sql.duckdb_literal(default_delimiter)} "
        f"AND schema_name = {sql.duckdb_literal(dataset)}"
    ).fetchone() == (1,)

    # The backtick-delimited attachment resolves the same table.
    assert attached.execute(f"SELECT COUNT(*) FROM {people}").fetchone() == (
        len(PEOPLE_ROWS),
    )

    with pytest.raises(
        duckdb.CatalogException,
        match=f"Table with name {tables.people} does not exist",
    ):
        attached.execute(
            f"SELECT * FROM {sql.attached(default_delimiter, dataset, tables.people)}"
        )


def test_missing_table_reports_a_catalog_error(
    attached: duckdb.DuckDBPyConnection, dataset: str, name: Callable[[str], str]
) -> None:
    """Name suggestions are disabled: otherwise this failure resolves every table in the project."""

    attached.execute(CATALOG_ERROR_SUGGESTIONS_OFF)

    missing = name("absent")
    with pytest.raises(
        duckdb.CatalogException, match=f"Table with name {missing} does not exist"
    ):
        attached.execute(f"SELECT * FROM {sql.attached(ATTACHED_CATALOG, dataset, missing)}")


def test_dropping_a_table_through_the_catalog_is_unsupported(
    attached: duckdb.DuckDBPyConnection, people: str
) -> None:
    with pytest.raises(
        duckdb.NotImplementedException,
        match="DropEntry not yet supported with the ADBC extension",
    ):
        attached.execute(f"DROP TABLE {people}")


def test_creating_a_view_through_the_catalog_is_unsupported(
    attached: duckdb.DuckDBPyConnection, dataset: str, name: Callable[[str], str]
) -> None:
    view = sql.attached(ATTACHED_CATALOG, dataset, name("view"))
    with pytest.raises(
        duckdb.NotImplementedException,
        match="CreateView not yet supported with the ADBC extension",
    ):
        attached.execute(f"CREATE VIEW {view} AS SELECT 1 AS id")

    with pytest.raises(
        duckdb.NotImplementedException,
        match="CREATE SCHEMA not yet supported with the ADBC extension",
    ):
        attached.execute(
            f"CREATE SCHEMA {sql.duckdb_identifier(ATTACHED_CATALOG)}."
            f"{sql.duckdb_identifier(name('schema'))}"
        )
