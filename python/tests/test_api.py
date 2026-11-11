from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock, call

import pyarrow
import pytest

import bigquery_adbc as driver
from bigquery_adbc import ConnectionOptions, dbapi


def test_public_low_level_api() -> None:
    assert driver.ENTRYPOINT == "AdbcDriverBigqueryInit"
    assert isinstance(driver.__version__, str)
    assert driver.__version__
    assert set(driver.__all__) == {
        "AuthenticationType",
        "BulkIngestMethod",
        "ConnectionOptions",
        "CreateDisposition",
        "CredentialsType",
        "DatabaseOptions",
        "ENTRYPOINT",
        "ParameterMode",
        "QueryPriority",
        "StatementOptions",
        "WriteDisposition",
        "__version__",
        "connect",
        "driver_path",
    }


def test_import_defers_distribution_metadata_until_version_is_read() -> None:
    # importlib.metadata costs more to import than the rest of the package.
    script = (
        "import sys, bigquery_adbc as driver\n"
        "assert 'importlib.metadata' not in sys.modules\n"
        "version = driver.__version__\n"
        "assert vars(driver)['__version__'] == version\n"
        "assert not hasattr(driver, 'missing')\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_driver_path_rereads_the_development_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first = tmp_path / "first.so"
    second = tmp_path / "second.so"
    first.touch()
    second.touch()
    monkeypatch.chdir(tmp_path)

    monkeypatch.setenv("BIGQUERY_ADBC_LIBRARY", first.name)
    assert driver.driver_path() == str(first.resolve())
    monkeypatch.setenv("BIGQUERY_ADBC_LIBRARY", str(second))
    assert driver.driver_path() == str(second.resolve())


@pytest.mark.parametrize("kind", ["missing", "directory"])
def test_driver_path_rejects_invalid_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str) -> None:
    path = tmp_path / kind
    if kind == "directory":
        path.mkdir()
    monkeypatch.setenv("BIGQUERY_ADBC_LIBRARY", str(path))

    with pytest.raises(FileNotFoundError, match="BIGQUERY_ADBC_LIBRARY is not a regular file") as caught:
        driver.driver_path()
    assert str(path) in str(caught.value)


def test_driver_path_requires_exactly_one_bundled_library(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BIGQUERY_ADBC_LIBRARY", raising=False)
    monkeypatch.setattr(driver, "_PACKAGE_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="Install a platform wheel"):
        driver.driver_path()

    library = tmp_path / "libadbc_driver_bigquery.so"
    library.touch()
    assert driver.driver_path() == str(library.resolve())

    (tmp_path / "libadbc_driver_bigquery.dylib").touch()
    with pytest.raises(RuntimeError, match="exactly one"):
        driver.driver_path()


@pytest.mark.parametrize(
    ("uri", "options", "expected_uri"),
    [
        pytest.param(
            "bigquery:///data-project",
            {"bigquery.project_id": "billing-project"},
            "bigquery:///data-project",
            id="uri-and-options",
        ),
        pytest.param("bigquery:///project", {"uri": "bigquery:///project"}, "bigquery:///project", id="matching-uris"),
        pytest.param(None, {"bigquery.project_id": "billing-project"}, None, id="options-only"),
        pytest.param(None, {"uri": "bigquery:///project"}, "bigquery:///project", id="uri-in-options"),
    ],
)
def test_low_level_connect_passes_options_without_mutating_them(
    monkeypatch: pytest.MonkeyPatch,
    uri: str | None,
    options: dict[str, str],
    expected_uri: str | None,
) -> None:
    make_database = Mock()
    monkeypatch.setattr(driver, "driver_path", lambda: "/absolute/driver.so")
    monkeypatch.setattr(driver.adbc_driver_manager, "AdbcDatabase", make_database)
    original = options.copy()
    expected = {"driver": "/absolute/driver.so", "entrypoint": "AdbcDriverBigqueryInit", **original}
    if expected_uri is not None:
        expected["uri"] = expected_uri

    actual = driver.connect(db_kwargs=options) if uri is None else driver.connect(uri, options)
    assert actual is make_database.return_value
    assert options == original
    make_database.assert_called_once_with(**expected)


def test_low_level_connect_rejects_conflicting_uri() -> None:
    with pytest.raises(ValueError, match="Conflicting BigQuery URIs") as caught:
        driver.connect(
            "bigquery:///one?client_secret=first-secret",
            {"uri": "bigquery:///two?refresh_token=second-secret"},
        )
    message = str(caught.value)
    assert "db_kwargs['uri']" in message
    assert "first-secret" not in message
    assert "second-secret" not in message


@pytest.mark.parametrize("reserved", ["driver", "entrypoint"])
def test_low_level_connect_rejects_loading_overrides(reserved: str) -> None:
    with pytest.raises(ValueError, match=reserved):
        driver.connect("bigquery:///project", {reserved: "unexpected"})


def test_dbapi_reexports_manager_facade() -> None:
    expected = {
        "BINARY",
        "DATETIME",
        "NUMBER",
        "ROWID",
        "STRING",
        "Connection",
        "Cursor",
        "DataError",
        "DatabaseError",
        "Date",
        "DateFromTicks",
        "Error",
        "IntegrityError",
        "InterfaceError",
        "InternalError",
        "NotSupportedError",
        "OperationalError",
        "ProgrammingError",
        "Time",
        "TimeFromTicks",
        "Timestamp",
        "TimestampFromTicks",
        "Warning",
        "apilevel",
        "paramstyle",
        "threadsafety",
    }
    type_objects = {"BINARY", "DATETIME", "NUMBER", "ROWID", "STRING"}
    # Binary is defined locally: adbc_driver_manager.dbapi does not provide it.
    # The type objects are wrapped locally: see the type-object tests below.
    assert set(dbapi.__all__) == expected | {"Binary", "connect"}
    for name in expected - type_objects:
        assert getattr(dbapi, name) is getattr(dbapi._dbapi, name)
    for name in type_objects:
        value, upstream = getattr(dbapi, name), getattr(dbapi._dbapi, name)
        assert set(value) >= set(upstream)
        if name != "NUMBER":
            assert value == upstream
            assert upstream == value
    assert dbapi.paramstyle == "qmark"


@pytest.mark.parametrize(
    ("arrow_type", "type_object", "equal"),
    [
        (pyarrow.string(), "STRING", True),
        (pyarrow.large_string(), "STRING", True),
        (pyarrow.json_(), "STRING", True),
        (pyarrow.binary(), "STRING", False),
        (pyarrow.decimal128(38, 9), "NUMBER", True),
        (pyarrow.decimal256(76, 38), "NUMBER", True),
        (pyarrow.decimal128(9, 2), "NUMBER", True),
        (pyarrow.int64(), "NUMBER", True),
        (pyarrow.float64(), "NUMBER", True),
        (pyarrow.string(), "NUMBER", False),
        (pyarrow.binary(), "NUMBER", False),
        (pyarrow.json_(), "NUMBER", False),
        (pyarrow.int64(), "STRING", False),
        (pyarrow.binary(), "BINARY", True),
        (pyarrow.string(), "BINARY", False),
        (pyarrow.timestamp("us"), "DATETIME", True),
        (pyarrow.int64(), "DATETIME", False),
        (pyarrow.int64(), "ROWID", True),
        (pyarrow.string(), "ROWID", False),
    ],
)
def test_dbapi_type_objects_support_both_comparison_operators(
    arrow_type: pyarrow.DataType, type_object: str, equal: bool
) -> None:
    # The manager's type objects answer `!=` with True for every Arrow type.
    value = getattr(dbapi, type_object)
    assert (arrow_type == value) is equal
    assert (value == arrow_type) is equal
    assert (arrow_type != value) is not equal
    assert (value != arrow_type) is not equal


def test_dbapi_binary_returns_independent_bytes() -> None:
    class BytesSubclass(bytes):
        pass

    source = bytearray(b"\x00\xff")
    # PEP 249 defines Binary(string); `bytes("abc")` would raise TypeError.
    values = ["abc", "smörgås", b"\x00\xff", source, memoryview(source), BytesSubclass(b"\x00\xff")]
    results = [dbapi.Binary(value) for value in values]
    source[0] = 1
    assert results == [b"abc", "smörgås".encode(), *[b"\x00\xff"] * 4]
    assert all(type(result) is bytes for result in results)


@pytest.mark.parametrize(
    "connect_options",
    [
        pytest.param({}, id="default-uri-and-autocommit"),
        pytest.param({"uri": "bigquery:///data", "autocommit": True}, id="explicit-uri-and-autocommit"),
    ],
)
def test_dbapi_connect_returns_manager_objects_directly(
    monkeypatch: pytest.MonkeyPatch,
    connect_options: dict[str, Any],
) -> None:
    factory = Mock()
    monkeypatch.setattr(dbapi, "_driver_connect", factory.database)
    monkeypatch.setattr(dbapi.adbc_driver_manager, "AdbcConnection", factory.connection)
    monkeypatch.setattr(dbapi, "Connection", factory.dbapi)
    db_options = {"bigquery.project_id": "billing"}
    conn_options = {"adbc.connection.catalog": "data"}

    actual = dbapi.connect(
        db_kwargs=db_options,
        conn_kwargs=conn_options,
        backend=factory.backend,
        **connect_options,
    )

    assert actual is factory.dbapi.return_value
    assert db_options == {"bigquery.project_id": "billing"}
    assert conn_options == {"adbc.connection.catalog": "data"}
    assert factory.mock_calls == [
        call.database(connect_options.get("uri"), db_kwargs=db_options),
        call.connection(factory.database.return_value, **conn_options),
        call.dbapi(
            factory.database.return_value,
            factory.connection.return_value,
            conn_kwargs=conn_options,
            autocommit=connect_options.get("autocommit", False),
            backend=factory.backend,
        ),
    ]


def test_dbapi_connect_rejects_unknown_keywords_before_loading_the_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    # The manager's Connection would raise only after both handles were open, and its __del__
    # then fails on the half-built object.
    load = Mock()
    monkeypatch.setattr(dbapi, "_driver_connect", load)
    misspelled: dict[str, Any] = {"autocomit": True}

    with pytest.raises(TypeError, match="autocomit"):
        dbapi.connect("bigquery:///project", **misspelled)
    load.assert_not_called()


def test_connect_accepts_option_enum_mappings(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pyrefly checks this file: Mapping keys are invariant, so these calls only type-check
    # when the signatures accept enum-keyed mappings as well as str-keyed ones.
    # Database option values may be any type AdbcDatabase converts, such as a path.
    db_options = {
        driver.DatabaseOptions.PROJECT_ID: "billing-project",
        driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.ANONYMOUS,
        driver.DatabaseOptions.AUTHENTICATION_CREDENTIALS: Path("credentials.json"),
    }
    conn_options = {ConnectionOptions.CURRENT_DB_SCHEMA: "dataset"}
    databases: list[dict[str, object]] = []
    connections: list[dict[str, object]] = []
    monkeypatch.setattr(driver, "driver_path", lambda: "/absolute/driver.so")
    monkeypatch.setattr(driver.adbc_driver_manager, "AdbcDatabase", lambda **kwargs: databases.append(kwargs))
    monkeypatch.setattr(dbapi.adbc_driver_manager, "AdbcConnection", lambda db, **kwargs: connections.append(kwargs))
    monkeypatch.setattr(dbapi, "Connection", lambda *args, **kwargs: None)

    driver.connect(db_kwargs=db_options)
    dbapi.connect(db_kwargs=db_options, conn_kwargs=conn_options)

    assert databases == [{"driver": "/absolute/driver.so", "entrypoint": driver.ENTRYPOINT, **db_options}] * 2
    assert connections == [conn_options]


@pytest.mark.parametrize("failure_stage", ["connection", "dbapi"])
@pytest.mark.parametrize("close_fails", [False, True])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_dbapi_connect_cleans_up_partial_initialization(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    close_fails: bool,
    error_type: type[BaseException],
) -> None:
    failure = error_type("initialization failed")
    resources = Mock()
    for resource in (resources.database, resources.connection):
        resource.close.side_effect = RuntimeError("close failed") if close_fails else None
    monkeypatch.setattr(dbapi, "_driver_connect", Mock(return_value=resources.database))
    monkeypatch.setattr(
        dbapi.adbc_driver_manager,
        "AdbcConnection",
        Mock(return_value=resources.connection, side_effect=failure if failure_stage == "connection" else None),
    )
    monkeypatch.setattr(dbapi, "Connection", Mock(side_effect=failure))

    with pytest.raises(error_type) as caught:
        dbapi.connect("bigquery:///project")

    assert caught.value is failure
    expected = [call.database.close()]
    if failure_stage == "dbapi":
        expected.insert(0, call.connection.close())
    assert resources.mock_calls == expected


def test_dbapi_connect_rejects_autocommit_in_conn_kwargs() -> None:
    with pytest.raises(ValueError, match=re.escape("adbc.connection.autocommit")):
        dbapi.connect(
            "bigquery:///project",
            conn_kwargs={ConnectionOptions.AUTOCOMMIT: "false"},
            autocommit=True,
        )
