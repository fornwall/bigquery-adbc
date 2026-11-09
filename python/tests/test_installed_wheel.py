from __future__ import annotations

import importlib.metadata
import inspect
import json
import os
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import adbc_driver_manager
import pytest

import bigquery_adbc as driver
from bigquery_adbc import dbapi


def test_installed_distribution_metadata() -> None:
    distribution = importlib.metadata.distribution("bigquery-adbc")

    assert distribution.version == driver.__version__
    assert distribution.metadata["Name"] == "bigquery-adbc"


def _offline_options(**options: object) -> dict[str, object]:
    return {
        "driver": driver.driver_path(),
        "entrypoint": driver.ENTRYPOINT,
        "uri": "bigquery:///offline-wheel-test",
        driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.ANONYMOUS,
        **options,
    }


def test_public_function_signatures() -> None:
    def defaults(function: object) -> list[tuple[str, object]]:
        return [(name, parameter.default) for name, parameter in inspect.signature(function).parameters.items()]

    assert defaults(driver.connect) == [("uri", None), ("db_kwargs", None)]
    assert defaults(dbapi.connect) == [
        ("uri", None),
        ("db_kwargs", None),
        ("conn_kwargs", None),
        ("autocommit", False),
        ("kwargs", inspect.Parameter.empty),
    ]


def test_wheel_ships_the_checked_in_option_enums() -> None:
    # `cargo test --all-features` holds python/src/bigquery_adbc/_options.py to the compiled
    # Rust option metadata; this holds the installed copy to that file.
    source = Path(__file__).resolve().parents[1] / "src" / "bigquery_adbc" / "_options.py"
    installed = Path(driver.__file__).resolve().parent / "_options.py"

    assert installed.read_bytes() == source.read_bytes()


def test_wheel_bundles_exactly_one_native_library() -> None:
    names = {"libadbc_driver_bigquery.so", "libadbc_driver_bigquery.dylib", "adbc_driver_bigquery.dll"}
    libraries = [path for path in Path(driver.__file__).resolve().parent.iterdir() if path.name in names]

    assert libraries == [Path(driver.driver_path())]
    assert libraries[0].is_absolute()


@pytest.mark.parametrize("explicit_entrypoint", [True, False])
def test_native_database_handle_loads_and_closes(explicit_entrypoint: bool) -> None:
    options = _offline_options()
    if not explicit_entrypoint:
        del options["entrypoint"]

    database = adbc_driver_manager.AdbcDatabase(**options)
    try:
        assert database.get_option("uri") == options["uri"]
        assert database.get_option(driver.DatabaseOptions.AUTHENTICATION_TYPE) == driver.AuthenticationType.ANONYMOUS
    finally:
        database.close()


def test_native_database_reports_an_unset_uri_as_not_found() -> None:
    """The public connect() API supports configuration entirely through options."""

    database = driver.connect(
        db_kwargs={
            driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.ANONYMOUS,
            driver.DatabaseOptions.PROJECT_ID: "offline-wheel-test",
        }
    )
    try:
        with pytest.raises(adbc_driver_manager.ProgrammingError) as error:
            database.get_option("uri")
        assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
    finally:
        database.close()


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        pytest.param(
            "bigquery:///dummyproject"
            "?OAuthType=3"
            "&AuthClientId=test-client-id"
            "&AuthClientSecret=test-client-secret"
            "&AuthRefreshToken=test-refresh-token"
            "&DatasetId=dummydataset"
            "&Location=dummylocation",
            {
                driver.DatabaseOptions.AUTHENTICATION_TYPE: (driver.AuthenticationType.USER_AUTHENTICATION),
                driver.DatabaseOptions.URI: (
                    "bigquery:///dummyproject?OAuthType=3&AuthClientId=test-client-id"
                    "&AuthClientSecret=<redacted>&AuthRefreshToken=<redacted>"
                    "&DatasetId=dummydataset&Location=dummylocation"
                ),
                driver.DatabaseOptions.AUTHENTICATION_CLIENT_ID: "test-client-id",
                driver.DatabaseOptions.PROJECT_ID: "dummyproject",
                driver.DatabaseOptions.DATASET_ID: "dummydataset",
                driver.DatabaseOptions.LOCATION: "dummylocation",
            },
            id="user-oauth",
        ),
        pytest.param(
            "bigquery://bigquery.dummyapis.com:445/dummyproject?DatasetId=dummydataset&QuotaProject=billing-project",
            {
                driver.DatabaseOptions.AUTHENTICATION_TYPE: (driver.AuthenticationType.APPLICATION_DEFAULT),
                driver.DatabaseOptions.PROJECT_ID: "dummyproject",
                driver.DatabaseOptions.DATASET_ID: "dummydataset",
                driver.DatabaseOptions.AUTHENTICATION_QUOTA_PROJECT: "billing-project",
                driver.DatabaseOptions.ENDPOINT: "https://bigquery.dummyapis.com:445",
            },
            id="custom-endpoint",
        ),
        pytest.param(
            "bigquery:///dummyproject",
            {
                driver.DatabaseOptions.AUTHENTICATION_TYPE: (driver.AuthenticationType.APPLICATION_DEFAULT),
                driver.DatabaseOptions.PROJECT_ID: "dummyproject",
                driver.DatabaseOptions.ENDPOINT: "https://bigquery.googleapis.com:443",
            },
            id="minimal",
        ),
    ],
)
def test_native_database_parses_go_compatible_uris(uri: str, expected: dict[str, str]) -> None:
    with driver.connect(uri) as database:
        assert database.get_option("uri") == expected.get(driver.DatabaseOptions.URI, uri)
        assert {option: database.get_option(option) for option in expected} == expected


def test_native_database_secrets_are_write_only() -> None:
    # test_native_database_parses_go_compatible_uris checks that the URI redacts them.
    uri = "bigquery:///dummyproject?OAuthType=3&AuthClientId=id&AuthClientSecret=secret&AuthRefreshToken=token"
    with driver.connect(uri) as database:
        for option in (
            driver.DatabaseOptions.AUTHENTICATION_CLIENT_SECRET,
            driver.DatabaseOptions.AUTHENTICATION_REFRESH_TOKEN,
        ):
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="write-only"):
                database.get_option(option)


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        pytest.param(
            "bigquery:///?OAuthType=0",
            "must contain exactly one project ID path segment",
            id="missing-project",
        ),
        pytest.param(
            "bigquery:///my-project-123?OAuthType=999",
            "OAuthType must be 0, 1, 2, or 3",
            id="invalid-oauth-type",
        ),
        pytest.param(
            "mysql://localhost/database",
            "scheme must be bigquery://",
            id="invalid-scheme",
        ),
        pytest.param(
            "bigquery:///my-project-123?OAuthType=1",
            "OAuthType=1 or 2 requires AuthCredentials",
            id="missing-service-account-credentials",
        ),
        pytest.param(
            "bigquery:///my-project-123?OAuthType=3&AuthClientId=client-id",
            "OAuthType=3 requires AuthClientId, AuthClientSecret, and AuthRefreshToken",
            id="incomplete-user-oauth",
        ),
    ],
)
def test_native_database_rejects_invalid_go_compatible_uris(uri: str, message: str) -> None:
    with pytest.raises(adbc_driver_manager.ProgrammingError, match=message) as error:
        driver.connect(uri)
    assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT


def test_native_user_authentication_options_build_credentials() -> None:
    options = _offline_options(
        **{
            driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.USER_AUTHENTICATION,
            driver.DatabaseOptions.AUTHENTICATION_CLIENT_ID: "wheel-client-id",
            driver.DatabaseOptions.AUTHENTICATION_CLIENT_SECRET: "wheel-client-secret",
            driver.DatabaseOptions.AUTHENTICATION_REFRESH_TOKEN: "wheel-refresh-token",
        }
    )
    with (
        adbc_driver_manager.AdbcDatabase(**options) as database,
        adbc_driver_manager.AdbcConnection(database),
    ):
        assert (
            database.get_option(driver.DatabaseOptions.AUTHENTICATION_TYPE)
            == driver.AuthenticationType.USER_AUTHENTICATION
        )
        assert database.get_option(driver.DatabaseOptions.AUTHENTICATION_CLIENT_ID) == "wheel-client-id"


def test_invalid_database_option_has_a_useful_error() -> None:
    with pytest.raises(adbc_driver_manager.NotSupportedError) as error:
        adbc_driver_manager.AdbcDatabase(
            driver=driver.driver_path(),
            entrypoint=driver.ENTRYPOINT,
            uri="bigquery:///offline-wheel-test",
            **{"bigquery.invalid_option": "invalid"},
        )

    assert "bigquery.invalid_option" in str(error.value)
    assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


def test_native_ingest_method_option_is_storage_write_only() -> None:
    key = driver.ConnectionOptions.BULK_INGEST_METHOD
    value = driver.BulkIngestMethod.STORAGE_WRITE
    with (
        adbc_driver_manager.AdbcDatabase(**_offline_options()) as database,
        adbc_driver_manager.AdbcConnection(database) as connection,
        adbc_driver_manager.AdbcStatement(connection) as statement,
    ):
        assert connection.get_option(key) == value
        connection.set_options(**{key: value})
        assert statement.get_option(key) == value
        statement.set_options(**{key: value})

        with pytest.raises(adbc_driver_manager.NotSupportedError) as error:
            statement.set_options(**{key: "load"})
        assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


def test_native_ingest_target_is_cleared_by_an_empty_value() -> None:
    # The documented workaround for UP-95: the manager's adbc_ingest leaves an omitted
    # catalog or schema from an earlier call on the statement, and "" clears it.
    keys = (driver.StatementOptions.INGEST_TARGET_CATALOG, driver.StatementOptions.INGEST_TARGET_DB_SCHEMA)
    with (
        _offline_connection() as connection,
        connection.cursor() as cursor,
    ):
        statement = cursor.adbc_statement
        statement.set_options(**dict(zip(keys, ("other-project", "other_dataset"), strict=True)))
        assert [statement.get_option(key) for key in keys] == ["other-project", "other_dataset"]
        statement.set_options(**dict.fromkeys(keys, ""))
        for key in keys:
            with pytest.raises(adbc_driver_manager.ProgrammingError) as error:
                statement.get_option(key)
            assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND


def test_native_query_prefetch_concurrency_round_trip_and_snapshot() -> None:
    options = _offline_options(
        **{
            driver.DatabaseOptions.ENDPOINT: "http://127.0.0.1:1",
            driver.DatabaseOptions.STORAGE_ENDPOINT: "http://127.0.0.1:2",
        }
    )
    # Cython types `get_option_int(key)` as `str` and rejects str subclasses, so it
    # needs `.value`; dict keys and **kwargs accept the enum member.
    prefetch_key = driver.ConnectionOptions.QUERY_PREFETCH_CONCURRENCY.value
    with (
        adbc_driver_manager.AdbcDatabase(**options) as database,
        adbc_driver_manager.AdbcConnection(database) as connection,
    ):
        assert database.get_option(driver.DatabaseOptions.STORAGE_ENDPOINT) == "http://127.0.0.1:2"
        assert connection.get_option_int(prefetch_key) == 10
        connection.set_options(**{prefetch_key: 3})

        with adbc_driver_manager.AdbcStatement(connection) as statement:
            assert statement.get_option_int(prefetch_key) == 3
            statement.set_options(**{prefetch_key: 2})
            assert statement.get_option_int(prefetch_key) == 2


def test_formatted_enums_and_standard_options_reach_native_driver() -> None:
    with (
        driver.connect(
            "bigquery:///offline-wheel-test",
            db_kwargs={f"{driver.DatabaseOptions.AUTHENTICATION_TYPE}": f"{driver.AuthenticationType.ANONYMOUS}"},
        ) as database,
        adbc_driver_manager.AdbcConnection(database) as connection,
        adbc_driver_manager.AdbcStatement(connection) as statement,
    ):
        assert database.get_option(driver.DatabaseOptions.AUTHENTICATION_TYPE) == "anonymous"
        connection.set_options(
            **{
                driver.ConnectionOptions.READ_ONLY: "false",
                driver.ConnectionOptions.ISOLATION_LEVEL: "adbc.connection.transaction.isolation.snapshot",
            }
        )
        assert connection.get_option(driver.ConnectionOptions.READ_ONLY) == "false"
        assert connection.get_option(driver.ConnectionOptions.ISOLATION_LEVEL) == (
            "adbc.connection.transaction.isolation.snapshot"
        )
        statement.set_options(**{driver.StatementOptions.INCREMENTAL: "false"})
        assert statement.get_option(driver.StatementOptions.INCREMENTAL) == "false"
        assert statement.get_option_float(driver.StatementOptions.PROGRESS.value) == 0.0
        assert statement.get_option_float(driver.StatementOptions.MAX_PROGRESS.value) == 0.0

        for handle, key in (
            (connection, driver.ConnectionOptions.READ_ONLY),
            (statement, driver.StatementOptions.INCREMENTAL),
        ):
            with pytest.raises(adbc_driver_manager.NotSupportedError):
                handle.set_options(**{key: "true"})


def _offline_connection(*, autocommit: bool = True) -> dbapi.Connection:
    # Anonymous authentication needs no credentials, and no test sends a query through this.
    return dbapi.connect(
        "bigquery:///offline-wheel-test",
        db_kwargs={driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.ANONYMOUS},
        autocommit=autocommit,
    )


@pytest.fixture
def offline_cursor() -> Iterator[dbapi.Cursor]:
    with _offline_connection() as connection, connection.cursor() as cursor:
        yield cursor


def test_dbapi_cursor_has_no_result_set_before_execute(
    offline_cursor: dbapi.Cursor,
) -> None:
    # PEP 249 values for a cursor with no result set.
    assert offline_cursor.description is None
    assert offline_cursor.rowcount == -1
    assert offline_cursor.rownumber is None
    assert offline_cursor.connection is not None


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_dbapi_fetch_before_execute_raises_programming_error(offline_cursor: dbapi.Cursor, method: str) -> None:
    with pytest.raises(dbapi.ProgrammingError, match="before execute"):
        getattr(offline_cursor, method)()


@pytest.mark.parametrize(
    ("method", "arguments"),
    [("callproc", ("procedure", ())), ("nextset", ())],
)
def test_dbapi_optional_methods_raise_not_supported_error(
    offline_cursor: dbapi.Cursor, method: str, arguments: tuple[object, ...]
) -> None:
    # PEP 249 allows omitting these, but requires NotSupportedError rather than
    # AttributeError or a silent no-op.
    with pytest.raises(dbapi.NotSupportedError, match=method):
        getattr(offline_cursor, method)(*arguments)


@pytest.mark.parametrize("autocommit", [True, False])
def test_dbapi_close_is_idempotent(autocommit: bool) -> None:
    # `with` blocks and explicit close() calls are often combined. With
    # autocommit=False, the connection's BigQuery session was never opened.
    connection = _offline_connection(autocommit=autocommit)
    cursor = connection.cursor()

    cursor.close()
    cursor.close()
    connection.close()
    connection.close()


def test_dbapi_use_after_close_raises() -> None:
    # PEP 249 requires an Error here. The C driver manager clears a released
    # handle's private_driver and returns INVALID_STATE instead of using freed memory.
    connection = _offline_connection()
    cursor = connection.cursor()
    cursor.close()

    with pytest.raises(dbapi.ProgrammingError):
        cursor.execute("SELECT 1")
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchone()

    connection.close()
    with pytest.raises(dbapi.ProgrammingError):
        connection.cursor()


class _SecondPageDenied(BaseHTTPRequestHandler):
    """BigQuery REST: a query that answers its first page and refuses the next."""

    def _reply(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers["Content-Length"]))
        self._reply(
            200,
            {
                "jobComplete": True,
                "jobReference": {"projectId": "offline-wheel-test", "jobId": "job", "location": "EU"},
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": "1"}]}],
                "totalRows": "2",
                "pageToken": "next",
            },
        )

    def do_GET(self) -> None:
        error = {"reason": "accessDenied", "message": "denied by the mock"}
        self._reply(403, {"error": {"code": 403, "message": error["message"], "errors": [error]}})

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def second_page_denied() -> Iterator[str]:
    with ThreadingHTTPServer(("127.0.0.1", 0), _SecondPageDenied) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        yield f"http://127.0.0.1:{server.server_port}"
        server.shutdown()


def test_mid_stream_failure_details_reach_the_caller(second_page_denied: str) -> None:
    # Only the C driver manager covers this: its AdbcErrorFromArrayStream sets the private_driver
    # that routes AdbcErrorGetDetail* back to this driver. The Rust manager never calls it.
    with (
        dbapi.connect(
            "bigquery:///offline-wheel-test",
            db_kwargs={
                driver.DatabaseOptions.AUTHENTICATION_TYPE: driver.AuthenticationType.ANONYMOUS,
                driver.DatabaseOptions.ENDPOINT: second_page_denied,
            },
            conn_kwargs={driver.ConnectionOptions.QUERY_USE_STORAGE_API_DISABLED_CLIENT: "true"},
            autocommit=True,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        with pytest.raises(dbapi.ProgrammingError, match="denied by the mock") as failure:
            cursor.fetchall()

    # The manager's stubs declare str keys, but it returns bytes.
    assert {os.fsdecode(key): value for key, value in failure.value.details} == {
        "bigquery.error.reason": b"accessDenied",
        "bigquery.error.http_status": b"403",
        "bigquery.job_id": b"offline-wheel-test:EU.job",
    }
