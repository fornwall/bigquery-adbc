from __future__ import annotations

import contextlib
import os
import re
import secrets
import time
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from decimal import Decimal
from typing import Any, TypeVar

import adbc_driver_manager
import pyarrow as pa
import pytest
from bigquery_backend import emulator_options, integration_project

import bigquery_adbc as driver
from bigquery_adbc import dbapi

pytestmark = pytest.mark.bigquery

_DATASET_PREFIX = "bigquery_adbc_py_"
_OWNER_LABEL = ("bigquery_adbc", "python_tests")
_RUN_LABEL_KEY = "run_id"
_METADATA_TIMEOUT_SECONDS = 45
_QUERY_ID_KEY = b"BIGQUERY:query_id"
_STATEMENT_TYPE_KEY = b"BIGQUERY:statistics:query:statement_type"
_Value = TypeVar("_Value")


def _identifier(value: str) -> str:
    if not value or "`" in value:
        raise ValueError(f"unsafe BigQuery identifier: {value!r}")
    return f"`{value}`"


def _string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _uri(project: str) -> str:
    return f"bigquery:///{project}"


def _db_options(dataset: str | None = None) -> dict[str, str]:
    options = emulator_options()
    if dataset is not None:
        options[driver.DatabaseOptions.DATASET_ID] = dataset
    return options


def _execute_update(project: str, sql: str) -> int:
    with (
        dbapi.connect(_uri(project), _db_options(), autocommit=True) as connection,
        adbc_driver_manager.AdbcStatement(connection.adbc_connection) as statement,
    ):
        statement.set_sql_query(sql)
        return statement.execute_update()


def _dataset_label_value(project: str, dataset: str) -> str | None:
    sql = f"""
        SELECT option_value
        FROM {_identifier(project)}.`region-us`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE schema_name = {_string_literal(dataset)}
          AND option_name = 'labels'
    """
    with (
        dbapi.connect(_uri(project), _db_options(), autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(sql)
        row = cursor.fetchone()
    return None if row is None else row[0]


def _has_label(labels: str, key: str, value: str) -> bool:
    pair = re.compile(rf'STRUCT\(\s*"{re.escape(key)}"\s*,\s*"{re.escape(value)}"\s*\)')
    return pair.search(labels) is not None


def _cleanup_dataset(project: str, dataset: str, run_label: str) -> None:
    expected_dataset = _DATASET_PREFIX + run_label
    if dataset != expected_dataset or not re.fullmatch(r"[a-z0-9_]+", dataset):
        raise RuntimeError(f"refusing to clean up unsafe dataset ID: {dataset!r}")

    expected_labels = (_OWNER_LABEL, (_RUN_LABEL_KEY, run_label))
    labels = _dataset_label_value(project, dataset)
    owned = labels is not None and all(_has_label(labels, key, value) for key, value in expected_labels)
    if not owned:
        raise RuntimeError(f"refusing to clean up {project}.{dataset}: ownership labels do not match")

    _execute_update(
        project,
        f"DROP SCHEMA {_identifier(f'{project}.{dataset}')} CASCADE",
    )


@pytest.fixture(scope="session")
def bigquery_dataset() -> Iterator[tuple[str, str]]:
    if os.environ.get("ADBC_BIGQUERY_RUN_INTEGRATION") != "1":
        pytest.skip("set ADBC_BIGQUERY_RUN_INTEGRATION=1 to run BigQuery tests")

    project = integration_project()

    run_label = (datetime.now(UTC).strftime("%Y%m%d_%H%M%S") + f"_{os.getpid()}_{secrets.token_hex(4)}").lower()
    dataset = _DATASET_PREFIX + run_label
    created = False
    try:
        _execute_update(
            project,
            f"""
                CREATE SCHEMA {_identifier(f"{project}.{dataset}")}
                OPTIONS (
                    location = 'US',
                    default_table_expiration_days = 1,
                    labels = [
                        ({_string_literal(_OWNER_LABEL[0])},
                         {_string_literal(_OWNER_LABEL[1])}),
                        ({_string_literal(_RUN_LABEL_KEY)},
                         {_string_literal(run_label)})
                    ]
                )
            """,
        )
        created = True
        yield project, dataset
    finally:
        if created:
            _cleanup_dataset(project, dataset, run_label)


@contextlib.contextmanager
def _low_level_connection(project: str, dataset: str) -> Iterator[adbc_driver_manager.AdbcConnection]:
    with (
        driver.connect(_uri(project), _db_options(dataset)) as database,
        adbc_driver_manager.AdbcConnection(database) as connection,
    ):
        yield connection


@contextlib.contextmanager
def _cursor(project: str, dataset: str) -> Iterator[dbapi.Cursor]:
    with (
        dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        yield cursor


def _read_stream(stream: adbc_driver_manager.ArrowArrayStreamHandle) -> pa.Table:
    return pa.RecordBatchReader.from_stream(stream).read_all()


def _metadata_table(values: list[dict[str, Any]], dataset: str, table: str) -> dict[str, Any] | None:
    for catalog in values:
        for schema in catalog.get("catalog_db_schemas") or []:
            if schema["db_schema_name"] != dataset:
                continue
            for candidate in schema.get("db_schema_tables") or []:
                if candidate["table_name"] == table:
                    return candidate
    return None


def _wait_for_metadata(call: Callable[[], _Value], predicate: Callable[[_Value], bool]) -> _Value:
    deadline = time.monotonic() + _METADATA_TIMEOUT_SECONDS
    value: _Value | None = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = call()
            if predicate(value):
                return value
        except Exception as error:  # noqa: BLE001 - retry transient metadata failures
            last_error = error
        time.sleep(1)
    if last_error is not None:
        raise AssertionError("BigQuery metadata did not propagate") from last_error
    raise AssertionError(f"BigQuery metadata did not propagate; last value: {value!r}")


def _wait_for_table(connection: dbapi.Connection, project: str, dataset: str, table_name: str) -> None:
    _wait_for_metadata(
        lambda: (
            connection.adbc_get_objects(
                depth="tables",
                catalog_filter=project,
                db_schema_filter=dataset,
                table_name_filter=table_name,
            )
            .read_all()
            .to_pylist()
        ),
        lambda values: _metadata_table(values, dataset, table_name) is not None,
    )


def test_select_through_low_level_and_dbapi(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with (
        _low_level_connection(project, dataset) as connection,
        adbc_driver_manager.AdbcStatement(connection) as statement,
    ):
        statement.set_sql_query("SELECT 1 AS answer")
        stream, rows_affected = statement.execute_query()
        table = _read_stream(stream)
    # The query completed before execute returned, so its result's size is known.
    assert rows_affected == 1
    assert table.to_pylist() == [{"answer": 1}]

    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT 1 AS answer")
        # PEP 249: rowcount is the number of rows the execute produced.
        assert cursor.rowcount == 1
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None


def test_description_type_codes_match_pep249_type_objects(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    columns = {
        "string": ("'a'", dbapi.STRING),
        "json": ("JSON '{}'", dbapi.STRING),
        "geography": ("ST_GEOGPOINT(1, 2)", dbapi.STRING),
        "bytes": ("b'a'", dbapi.BINARY),
        "int64": ("1", dbapi.NUMBER),
        "float64": ("1.5", dbapi.NUMBER),
        "numeric": ("NUMERIC '1.5'", dbapi.NUMBER),
        "bignumeric": ("BIGNUMERIC '1.5'", dbapi.NUMBER),
        "date": ("DATE '2026-01-02'", dbapi.DATETIME),
        "time": ("TIME '03:04:05'", dbapi.DATETIME),
        "datetime": ("DATETIME '2026-01-02 03:04:05'", dbapi.DATETIME),
        "timestamp": ("TIMESTAMP '2026-01-02 03:04:05+00'", dbapi.DATETIME),
    }
    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT " + ", ".join(f"{sql} AS {name}" for name, (sql, _) in columns.items()))
        type_codes = {name: type_code for name, type_code, *_ in cursor.description}
    assert [name for name, (_, type_object) in columns.items() if type_codes[name] != type_object] == []


def test_rowcount_reports_result_size_and_dml_count(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    table = _identifier(f"rowcount_{secrets.token_hex(8)}")
    with _cursor(project, dataset) as cursor:
        # DDL reports no count, even when it runs a query.
        cursor.execute(f"CREATE TABLE {table} AS SELECT x FROM UNNEST([1, 2, 3]) AS x")
        assert cursor.rowcount == -1
        cursor.execute(f"SELECT x FROM {table}")
        assert cursor.rowcount == 3
        assert len(cursor.fetchall()) == 3
        cursor.execute(f"SELECT x FROM {table} WHERE x > 3")
        assert cursor.rowcount == 0
        # DML reports the rows it affected, not the table's size.
        cursor.execute(f"UPDATE {table} SET x = x + 10 WHERE x = 1")
        assert cursor.rowcount == 1
        cursor.execute(f"DELETE FROM {table} WHERE x > 100")
        assert cursor.rowcount == 0


def test_go_compatible_host_uri_connects(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    uri = f"bigquery://bigquery.googleapis.com/{project}?DatasetId={dataset}"
    with (
        dbapi.connect(uri, _db_options(), autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)


@pytest.mark.parametrize(
    "option",
    [
        driver.DatabaseOptions.IMPERSONATION_DELEGATES,
        driver.DatabaseOptions.IMPERSONATION_SCOPES,
    ],
)
def test_empty_impersonation_lists_do_not_enable_impersonation(bigquery_dataset: tuple[str, str], option: str) -> None:
    project, dataset = bigquery_dataset
    with driver.connect(_uri(project), {**_db_options(dataset), option: ""}) as database:
        for unset_option in (
            option,
            driver.DatabaseOptions.IMPERSONATION_LIFETIME,
            driver.DatabaseOptions.IMPERSONATION_TARGET_PRINCIPAL,
        ):
            with pytest.raises(adbc_driver_manager.ProgrammingError) as error:
                database.get_option(unset_option)
            assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND

        with (
            adbc_driver_manager.AdbcConnection(database) as connection,
            adbc_driver_manager.AdbcStatement(connection) as statement,
        ):
            statement.set_sql_query("SELECT 1 AS value")
            stream, _ = statement.execute_query()
            assert _read_stream(stream).to_pylist() == [{"value": 1}]


def test_arrow_fetching_and_parameter_binding(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 7 AS value UNION ALL SELECT 8 ORDER BY value")
            assert cursor.fetch_arrow_table().to_pylist() == [
                {"value": 7},
                {"value": 8},
            ]

            cursor.execute("SELECT 'batch' AS kind")
            reader = cursor.fetch_record_batch()
            assert reader.read_next_batch().to_pylist() == [{"kind": "batch"}]

        positional = pa.record_batch([pa.array([7, 8])], names=[""])
        with connection.cursor() as cursor:
            cursor.execute("SELECT ? * 2 AS doubled", positional)
            assert cursor.fetch_arrow_table().column("doubled").to_pylist() == [
                14,
                16,
            ]

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT @number AS number, @text AS text",
                {"number": 42, "text": "named"},
            )
            # One bound row has run in full when execute returns, so its count is known.
            assert cursor.rowcount == 1
            assert cursor.fetchone() == (42, "named")

        with connection.cursor() as cursor:
            cursor.executemany("CREATE TABLE multirow_values (value INT64)", None)
            cursor.executemany("INSERT INTO multirow_values VALUES (?)", [(3,), (1,), (2,)])
            assert cursor.rowcount == 3
            cursor.execute("SELECT value FROM multirow_values ORDER BY value")
            assert cursor.fetchall() == [(1,), (2,), (3,)]


def test_parameter_bind_execution_shapes(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    parameter_schema = pa.schema([pa.field("", pa.int64(), nullable=False)])

    def batch(values: list[int]) -> pa.RecordBatch:
        return pa.RecordBatch.from_arrays(
            [pa.array(values, type=pa.int64())],
            schema=parameter_schema,
        )

    with _low_level_connection(project, dataset) as connection:
        with adbc_driver_manager.AdbcStatement(connection) as statement:
            statement.set_sql_query("SELECT ? AS value")
            statement.bind_stream(
                pa.RecordBatchReader.from_batches(
                    parameter_schema,
                    [batch([7]), batch([8, 9])],
                )
            )
            stream, rows_affected = statement.execute_query()
            result = _read_stream(stream)
        # Later parameter rows run while the stream is drained, so the total is unknown.
        assert rows_affected == -1
        assert result.column("value").to_pylist() == [7, 8, 9]

        empty_inputs = [
            ("batch", batch([])),
            (
                "empty stream",
                pa.RecordBatchReader.from_batches(parameter_schema, []),
            ),
            (
                "stream with empty batch",
                pa.RecordBatchReader.from_batches(parameter_schema, [batch([])]),
            ),
        ]
        for input_name, parameters in empty_inputs:
            with adbc_driver_manager.AdbcStatement(connection) as statement:
                statement.set_sql_query("SELECT ? AS value")
                if isinstance(parameters, pa.RecordBatch):
                    statement.bind(parameters)
                else:
                    statement.bind_stream(parameters)
                stream, rows_affected = statement.execute_query()
                result = _read_stream(stream)
            # Nothing executed, so the count is known.
            assert rows_affected == 0, input_name
            assert result.num_rows == 0, input_name
            assert result.schema.names == ["value"], input_name
            assert result.schema.field("value").type == pa.int64(), input_name

        table_name = "bind_execution_shapes"
        with adbc_driver_manager.AdbcStatement(connection) as statement:
            statement.set_sql_query(f"CREATE TABLE {_identifier(table_name)} (value INT64)")
            assert statement.execute_update() == -1

        inserts = [
            ("batch", batch([1, 2]), 2),
            (
                "stream",
                pa.RecordBatchReader.from_batches(parameter_schema, [batch([3]), batch([4, 5])]),
                3,
            ),
            ("empty stream", pa.RecordBatchReader.from_batches(parameter_schema, []), 0),
        ]
        for input_name, parameters, expected_rows in inserts:
            with adbc_driver_manager.AdbcStatement(connection) as statement:
                statement.set_sql_query(f"INSERT INTO {_identifier(table_name)} VALUES (?)")
                if isinstance(parameters, pa.RecordBatch):
                    statement.bind(parameters)
                else:
                    statement.bind_stream(parameters)
                assert statement.execute_update() == expected_rows, input_name

        with adbc_driver_manager.AdbcStatement(connection) as statement:
            statement.set_sql_query(f"SELECT value FROM {_identifier(table_name)} ORDER BY value")
            stream, _ = statement.execute_query()
            result = _read_stream(stream)
        assert result.column("value").to_pylist() == [1, 2, 3, 4, 5]


def test_dry_run_reports_schema_without_query_id(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with _cursor(project, dataset) as cursor:
        cursor.adbc_statement.set_options(**{driver.StatementOptions.QUERY_DRY_RUN: True})
        cursor.execute("SELECT 1 AS a, 'foobar' AS b")
        assert [description[0] for description in cursor.description] == ["a", "b"]
        # A dry run executes nothing, so it has no count.
        assert cursor.rowcount == -1
        # PEP 249 fixes the shape of a description entry at seven items.
        assert all(len(description) == 7 for description in cursor.description)

        cursor.execute(
            "SELECT ? AS a, 'foobar' AS b",
            pa.record_batch([pa.array([1, 2])], names=[""]),
        )
        assert [description[0] for description in cursor.description] == ["a", "b"]

        result = cursor.fetch_arrow_table()
        assert result.num_rows == 0
        assert result.schema.metadata[_STATEMENT_TYPE_KEY] == b"SELECT"
        assert _QUERY_ID_KEY not in result.schema.metadata


def test_script_without_results_returns_empty_arrow_result(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    table_name = f"script_no_results_{secrets.token_hex(8)}"
    with _cursor(project, dataset) as cursor:
        cursor.execute(f"CREATE TABLE {_identifier(table_name)} (idx INT64, val INT64)")
        cursor.execute(
            f"""
                CREATE TEMP TABLE staging AS SELECT 1 AS idx, 2 AS val;
                MERGE INTO {_identifier(table_name)} AS destination
                USING (SELECT * FROM staging) AS source
                ON destination.idx = source.idx
                WHEN MATCHED THEN
                    UPDATE SET val = source.val
                WHEN NOT MATCHED THEN
                    INSERT (idx, val) VALUES (source.idx, source.val);
                DROP TABLE IF EXISTS staging;
            """
        )
        assert not cursor.description
        # A script ending in neither a query nor DML reports no count.
        assert cursor.rowcount == -1
        result = cursor.fetch_arrow_table()
        assert result.num_rows == 0
        assert result.schema.metadata[_STATEMENT_TYPE_KEY] == b"SCRIPT"

        cursor.execute(f"SELECT val FROM {_identifier(table_name)} WHERE idx = 1")
        assert cursor.fetchone() == (2,)


def test_script_with_results_returns_one_result_set(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT 1 AS first_value; SELECT 'foobar' AS second_value")
        # A script counts its final result.
        assert cursor.rowcount == 1
        result = cursor.fetch_arrow_table()

    assert result.num_rows == 1
    assert result.column_names == ["second_value"]
    assert result.column("second_value").to_pylist() == ["foobar"]
    assert result.schema.metadata[_STATEMENT_TYPE_KEY] == b"SCRIPT"


def test_query_id_metadata_is_present_and_unique(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT 1 AS value")
        first_query_id = cursor.fetch_arrow_table().schema.metadata[_QUERY_ID_KEY]
        assert first_query_id

        cursor.execute("SELECT 1 AS value")
        second_query_id = cursor.fetch_arrow_table().schema.metadata[_QUERY_ID_KEY]
        assert second_query_id
        assert second_query_id != first_query_id


def test_bound_query_has_query_id_metadata(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT ? AS value", (1,))
        assert cursor.fetch_arrow_table().schema.metadata[_QUERY_ID_KEY]


def test_bulk_ingest_and_metadata(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    table_name = "ingested_values"
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["one", "two", None], type=pa.string()),
        }
    )

    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        with connection.cursor() as cursor:
            assert (
                cursor.adbc_ingest(
                    table_name,
                    data,
                    mode="create",
                    catalog_name=project,
                    db_schema_name=dataset,
                )
                == 3
            )
            cursor.execute(f"SELECT id, name FROM {_identifier(table_name)} ORDER BY id")
            assert cursor.fetch_arrow_table().to_pylist() == data.to_pylist()

        info = connection.adbc_get_info()
        assert info["driver_name"] == "bigquery-adbc"
        assert info["driver_version"]
        assert info["vendor_name"] == "Google BigQuery"

        schema = _wait_for_metadata(
            lambda: connection.adbc_get_table_schema(
                table_name,
                catalog_filter=project,
                db_schema_filter=dataset,
            ),
            lambda value: value.names == ["id", "name"],
        )
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()

        objects = _wait_for_metadata(
            lambda: (
                connection.adbc_get_objects(
                    depth="all",
                    catalog_filter=project,
                    db_schema_filter=dataset,
                    table_name_filter=table_name,
                )
                .read_all()
                .to_pylist()
            ),
            lambda value: _metadata_table(value, dataset, table_name) is not None,
        )
        metadata = _metadata_table(objects, dataset, table_name)
        assert metadata is not None
        assert [column["column_name"] for column in metadata["table_columns"]] == [
            "id",
            "name",
        ]
        assert [
            {
                key: column[key]
                for key in (
                    "xdbc_type_name",
                    "xdbc_nullable",
                    "xdbc_is_nullable",
                    "xdbc_is_autoincrement",
                    "xdbc_is_generatedcolumn",
                )
            }
            for column in metadata["table_columns"]
        ] == [
            {
                "xdbc_type_name": "INT64",
                "xdbc_nullable": 1,
                "xdbc_is_nullable": "YES",
                "xdbc_is_autoincrement": False,
                "xdbc_is_generatedcolumn": False,
            },
            {
                "xdbc_type_name": "STRING",
                "xdbc_nullable": 1,
                "xdbc_is_nullable": "YES",
                "xdbc_is_autoincrement": False,
                "xdbc_is_generatedcolumn": False,
            },
        ]

        def filtered_object(table_types: list[str]) -> dict[str, Any] | None:
            values = (
                connection.adbc_get_objects(
                    depth="tables",
                    catalog_filter=project,
                    db_schema_filter=dataset,
                    table_name_filter=table_name,
                    table_types_filter=table_types,
                )
                .read_all()
                .to_pylist()
            )
            return _metadata_table(values, dataset, table_name)

        assert filtered_object(["VIEW"]) is None
        table_only = filtered_object(["TABLE"])
        table_or_view = filtered_object(["TABLE", "VIEW"])
        assert table_only is not None and table_only["table_type"] == "TABLE"
        assert table_or_view is not None and table_or_view["table_type"] == "TABLE"

        statistics = _wait_for_metadata(
            lambda: (
                connection.adbc_get_statistics(
                    catalog_filter=project,
                    db_schema_filter=dataset,
                    table_name_filter=table_name,
                    approximate=True,
                )
                .read_all()
                .to_pylist()
            ),
            lambda value: bool(value),
        )
        assert statistics[0]["catalog_name"] == project


def test_current_catalog_and_schema_changes(
    bigquery_dataset: tuple[str, str],
) -> None:
    project, dataset = bigquery_dataset
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        assert connection.adbc_current_catalog == project
        assert connection.adbc_current_db_schema == dataset

        # A cursor keeps one statement, so one opened before the change must still see it.
        with connection.cursor() as cursor:
            connection.adbc_current_catalog = "bigquery-public-data"
            connection.adbc_current_db_schema = "samples"
            with pytest.raises(adbc_driver_manager.Error) as excinfo:
                connection.adbc_current_catalog = "thiscatalogdoesnotexist"
            assert excinfo.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
            assert connection.adbc_current_catalog == "bigquery-public-data"
            cursor.execute("SELECT COUNT(*) FROM shakespeare")
            assert cursor.fetchone()[0] > 0

        connection.adbc_current_catalog = project
        connection.adbc_current_db_schema = dataset
        assert connection.adbc_current_catalog == project
        assert connection.adbc_current_db_schema == dataset

        # The empty string clears the catalog, so names resolve in the billing project, which
        # is then the current catalog.
        connection.adbc_current_catalog = ""
        assert connection.adbc_current_catalog == project
        assert connection.adbc_current_db_schema == dataset


def test_commit_and_rollback(bigquery_dataset: tuple[str, str]) -> None:
    project, dataset = bigquery_dataset
    connection = dbapi.connect(_uri(project), _db_options(dataset), autocommit=False)
    try:
        with connection.cursor() as cursor:
            cursor.executemany("CREATE TEMP TABLE transaction_values (value INT64)", None)
            connection.commit()

            cursor.executemany("INSERT transaction_values VALUES (42)", None)
            connection.rollback()
            cursor.execute("SELECT COUNT(*) FROM transaction_values")
            assert cursor.fetchone() == (0,)

            cursor.executemany("INSERT transaction_values VALUES (7)", None)
            connection.commit()
            cursor.execute("SELECT value FROM transaction_values")
            assert cursor.fetchone() == (7,)
    finally:
        connection.adbc_connection.set_autocommit(True)
        connection.close()


def test_partitioned_execution(bigquery_dataset: tuple[str, str]) -> None:
    project, dataset = bigquery_dataset
    parameters = pa.record_batch([pa.array([7, 8])], names=[""])
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        with connection.cursor() as cursor:
            partitions, schema = cursor.adbc_execute_partitions("SELECT ? AS value", parameters)
        assert schema.field("value").type == pa.int64()
        assert len(partitions) == 2

        values = []
        for partition in partitions:
            with connection.cursor() as cursor:
                cursor.adbc_read_partition(partition)
                values.append(cursor.fetchone()[0])
                assert cursor.fetchone() is None
        assert values == [7, 8]


def test_driver_manager_fetch_frame_helpers(
    bigquery_dataset: tuple[str, str],
) -> None:
    import pandas
    import polars

    project, dataset = bigquery_dataset
    with _cursor(project, dataset) as cursor:
        cursor.execute("SELECT 1 AS value")
        frame = cursor.fetch_df()
        assert isinstance(frame, pandas.DataFrame)
        assert frame.to_dict(orient="records") == [{"value": 1}]

        cursor.execute("SELECT 2 AS value")
        frame = cursor.fetch_polars()
        assert isinstance(frame, polars.DataFrame)
        assert frame.to_dicts() == [{"value": 2}]


def test_pandas_read_sql(bigquery_dataset: tuple[str, str]) -> None:
    import pandas

    project, dataset = bigquery_dataset
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        frame = pandas.read_sql(
            """
                SELECT
                    42 AS value,
                    NUMERIC '123.456789012' AS decimal_value,
                    BIGNUMERIC '1.00000000000000000000000000000000000001'
                        AS big_decimal_value,
                    FROM_HEX('0001ff') AS binary_value,
                    DATE '2024-02-29' AS date_value,
                    TIME '23:59:58.123456' AS time_value,
                    DATETIME '2024-02-29 23:59:58.123456' AS datetime_value,
                    TIMESTAMP '2024-02-29 23:59:58.123456+00' AS timestamp_value,
                    CAST(NULL AS STRING) AS optional_text
            """,
            connection,
            dtype_backend="pyarrow",
        )
        empty_frame = pandas.read_sql(
            """
                SELECT
                    CAST(NULL AS INT64) AS value,
                    CAST(NULL AS BYTES) AS binary_value,
                    CAST(NULL AS NUMERIC) AS decimal_value,
                    CAST(NULL AS BIGNUMERIC) AS big_decimal_value,
                    CAST(NULL AS DATE) AS date_value,
                    CAST(NULL AS TIME) AS time_value,
                    CAST(NULL AS DATETIME) AS datetime_value,
                    CAST(NULL AS TIMESTAMP) AS timestamp_value
                FROM UNNEST([0])
                WHERE FALSE
            """,
            connection,
            dtype_backend="pyarrow",
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

    assert frame.drop(columns="optional_text").iloc[0].to_dict() == {
        "value": 42,
        "decimal_value": Decimal("123.456789012"),
        "big_decimal_value": Decimal("1.00000000000000000000000000000000000001"),
        "binary_value": b"\x00\x01\xff",
        "date_value": date(2024, 2, 29),
        "time_value": datetime_time(23, 59, 58, 123456),
        "datetime_value": datetime.fromisoformat("2024-02-29 23:59:58.123456"),
        "timestamp_value": datetime(2024, 2, 29, 23, 59, 58, 123456, UTC),
    }
    assert pandas.isna(frame.at[0, "optional_text"])
    assert frame.dtypes["value"].pyarrow_dtype == pa.int64()
    assert frame.dtypes["decimal_value"].pyarrow_dtype == pa.decimal128(38, 9)
    assert frame.dtypes["big_decimal_value"].pyarrow_dtype == pa.decimal256(76, 38)
    assert frame.dtypes["binary_value"].pyarrow_dtype == pa.binary()
    assert frame.dtypes["date_value"].pyarrow_dtype == pa.date32()
    assert frame.dtypes["time_value"].pyarrow_dtype == pa.time64("us")
    assert frame.dtypes["datetime_value"].pyarrow_dtype == pa.timestamp("us")
    assert frame.dtypes["timestamp_value"].pyarrow_dtype == pa.timestamp("us", "UTC")
    assert frame.dtypes["optional_text"].pyarrow_dtype == pa.string()
    assert empty_frame.empty
    assert empty_frame.columns.tolist() == [
        "value",
        "binary_value",
        "decimal_value",
        "big_decimal_value",
        "date_value",
        "time_value",
        "datetime_value",
        "timestamp_value",
    ]
    assert [dtype.pyarrow_dtype for dtype in empty_frame.dtypes] == [
        pa.int64(),
        pa.binary(),
        pa.decimal128(38, 9),
        pa.decimal256(76, 38),
        pa.date32(),
        pa.time64("us"),
        pa.timestamp("us"),
        pa.timestamp("us", "UTC"),
    ]


def test_pandas_to_sql(bigquery_dataset: tuple[str, str]) -> None:
    import pandas

    project, dataset = bigquery_dataset
    table_name = "pandas_to_sql_values"
    first = pandas.DataFrame(
        {
            "id": pandas.array([2, 1], dtype="int64[pyarrow]"),
            "name": pandas.array(["two", "one"], dtype="string[pyarrow]"),
            "exact": pandas.array(
                [Decimal("2.5"), Decimal("1.25")],
                dtype=pandas.ArrowDtype(pa.decimal128(38, 9)),
            ),
            "payload": pandas.array([b"\x02", b"\x01"], dtype=pandas.ArrowDtype(pa.binary())),
        }
    )
    empty = first.iloc[:0]
    second = pandas.DataFrame(
        {
            "id": pandas.array([3], dtype="int64[pyarrow]"),
            "name": pandas.array([None], dtype="string[pyarrow]"),
            "exact": pandas.array([None], dtype=pandas.ArrowDtype(pa.decimal128(38, 9))),
            "payload": pandas.array([None], dtype=pandas.ArrowDtype(pa.binary())),
        }
    )

    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        assert (
            empty.to_sql(
                table_name,
                connection,
                schema=dataset,
                if_exists="fail",
                index=False,
            )
            == 0
        )
        _wait_for_table(connection, project, dataset, table_name)
        with pytest.raises(ValueError, match=r"Table 'pandas_to_sql_values' already"):
            first.head(1).to_sql(
                table_name,
                connection,
                schema=dataset,
                if_exists="fail",
                index=False,
            )
        assert (
            first.to_sql(
                table_name,
                connection,
                schema=dataset,
                if_exists="append",
                index=False,
            )
            == 2
        )
        assert (
            second.to_sql(
                table_name,
                connection,
                schema=dataset,
                if_exists="append",
                index=False,
            )
            == 1
        )

    with _cursor(project, dataset) as cursor:
        cursor.execute(f"SELECT id, name, exact, payload FROM {_identifier(table_name)} ORDER BY id")
        assert cursor.fetchall() == [
            (1, "one", Decimal("1.25"), b"\x01"),
            (2, "two", Decimal("2.5"), b"\x02"),
            (3, None, None, None),
        ]


def test_polars_read_database(bigquery_dataset: tuple[str, str]) -> None:
    import polars

    project, dataset = bigquery_dataset
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        frame = polars.read_database(
            """
                SELECT
                    @value AS value,
                    ['alpha', 'beta'] AS tags,
                    STRUCT(TRUE AS enabled, 3 AS count) AS metadata,
                    NUMERIC '12.34' AS decimal_value,
                    FROM_HEX('0001ff') AS binary_value,
                    DATE '2024-02-29' AS date_value,
                    TIME '23:59:58.123456' AS time_value,
                    DATETIME '2024-02-29 23:59:58.123456' AS datetime_value,
                    TIMESTAMP '2024-02-29 23:59:58.123456+00' AS timestamp_value
            """,
            connection,
            execute_options={"parameters": {"value": 42}},
        )
        empty_frame = polars.read_database(
            """
                SELECT
                    CAST(NULL AS INT64) AS value,
                    CAST(NULL AS BYTES) AS binary_value,
                    CAST(NULL AS NUMERIC) AS decimal_value,
                    CAST(NULL AS DATE) AS date_value,
                    CAST(NULL AS TIME) AS time_value,
                    CAST(NULL AS DATETIME) AS datetime_value,
                    CAST(NULL AS TIMESTAMP) AS timestamp_value
                FROM UNNEST([0])
                WHERE FALSE
            """,
            connection,
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

    assert frame.to_dicts() == [
        {
            "value": 42,
            "tags": ["alpha", "beta"],
            "metadata": {"enabled": True, "count": 3},
            "decimal_value": Decimal("12.340000000"),
            "binary_value": b"\x00\x01\xff",
            "date_value": date(2024, 2, 29),
            "time_value": datetime_time(23, 59, 58, 123456),
            "datetime_value": datetime.fromisoformat("2024-02-29 23:59:58.123456"),
            "timestamp_value": datetime(2024, 2, 29, 23, 59, 58, 123456, UTC),
        }
    ]
    assert frame.schema == {
        "value": polars.Int64,
        "tags": polars.List(polars.String),
        "metadata": polars.Struct({"enabled": polars.Boolean, "count": polars.Int64}),
        "decimal_value": polars.Decimal(38, 9),
        "binary_value": polars.Binary,
        "date_value": polars.Date,
        "time_value": polars.Time,
        "datetime_value": polars.Datetime("us"),
        "timestamp_value": polars.Datetime("us", "UTC"),
    }
    assert empty_frame.is_empty()
    assert empty_frame.schema == {
        "value": polars.Int64,
        "binary_value": polars.Binary,
        "decimal_value": polars.Decimal(38, 9),
        "date_value": polars.Date,
        "time_value": polars.Time,
        "datetime_value": polars.Datetime("us"),
        "timestamp_value": polars.Datetime("us", "UTC"),
    }


def test_polars_read_database_batches(bigquery_dataset: tuple[str, str]) -> None:
    import polars

    project, dataset = bigquery_dataset
    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        batches = list(
            polars.read_database(
                """
                    SELECT value
                    FROM UNNEST(GENERATE_ARRAY(0, 256)) AS value
                    ORDER BY value
                """,
                connection,
                iter_batches=True,
                batch_size=4,
            )
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

    assert batches
    assert all(isinstance(batch, polars.DataFrame) for batch in batches)
    assert all(batch.schema == {"value": polars.Int64} for batch in batches)
    assert polars.concat(batches).get_column("value").to_list() == list(range(257))


def test_polars_write_database(bigquery_dataset: tuple[str, str]) -> None:
    import polars

    project, dataset = bigquery_dataset
    table_name = "polars_write_database_values"
    qualified_table_name = f"{project}.{dataset}.{table_name}"
    append_missing_table_name = "polars_append_missing_values"
    qualified_append_missing_table_name = f"{project}.{dataset}.{append_missing_table_name}"
    first = polars.DataFrame(
        {"id": [2, 1], "name": ["two", "one"]},
        schema={"id": polars.Int64, "name": polars.String},
    )
    second = polars.DataFrame(
        {"id": [3], "name": [None]},
        schema={"id": polars.Int64, "name": polars.String},
    )
    empty_replacement = polars.DataFrame(schema={"replacement_id": polars.Int64, "payload": polars.Binary})

    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        assert (
            first.head(1).write_database(
                qualified_append_missing_table_name,
                connection,
                if_table_exists="append",
                engine="adbc",
            )
            == 1
        )
        _wait_for_table(connection, project, dataset, append_missing_table_name)
        assert (
            first.write_database(
                qualified_table_name,
                connection,
                if_table_exists="fail",
                engine="adbc",
            )
            == 2
        )
        _wait_for_table(connection, project, dataset, table_name)
        with pytest.raises(adbc_driver_manager.Error) as error:
            first.head(1).write_database(
                qualified_table_name,
                connection,
                if_table_exists="fail",
                engine="adbc",
            )
        assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.ALREADY_EXISTS
        assert (
            second.write_database(
                qualified_table_name,
                connection,
                if_table_exists="append",
                engine="adbc",
            )
            == 1
        )

    with dbapi.connect(_uri(project), _db_options(dataset), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, name FROM {_identifier(table_name)} ORDER BY id")
            assert cursor.fetchall() == [(1, "one"), (2, "two"), (3, None)]

            cursor.execute(f"SELECT id, name FROM {_identifier(append_missing_table_name)}")
            assert cursor.fetchall() == [(2, "two")]

        assert (
            empty_replacement.write_database(
                qualified_table_name,
                connection,
                if_table_exists="replace",
                engine="adbc",
            )
            == 0
        )

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT replacement_id, payload FROM {_identifier(table_name)}")
            assert cursor.fetchall() == []
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
