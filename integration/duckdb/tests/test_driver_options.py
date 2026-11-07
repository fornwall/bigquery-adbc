"""Database options applied by profiles before AdbcDatabaseInit.

The extension exposes no connection- or statement-level option setters."""

from __future__ import annotations

import re
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import duckdb
import pytest
import sql
from sql import Session

# Database options accept strings only. A TOML integer exercises rejection
# through AdbcDatabaseSetOptionInt; profiles cannot set statement integer options.
MakeProfile = Callable[..., str]

# `INFORMATION_SCHEMA.TABLES` is a dataset-scoped view, so an unqualified
# reference to it resolves exactly when a default dataset is configured, and
# reading it costs nothing but the 10 MB query minimum.
UNQUALIFIED_TABLE_COUNT = "SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES"

NO_DEFAULT_DATASET = 'Table "INFORMATION_SCHEMA.TABLES" must be qualified with a dataset'


def _create_probe_table(bigquery: Session, project: str, dataset: str, table: str) -> None:
    """Create the one-row table a test reads back through its own profile."""

    bigquery.execute(
        f"CREATE TABLE {sql.qualified(project, dataset, table)} AS SELECT 42 AS n"
    )


def _qualified_table_count(project: str, dataset: str, table: str) -> str:
    """Check the profile's project without relying on a default dataset."""

    return (
        f"SELECT COUNT(*) AS n FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES "
        f"WHERE table_name = {sql.string_literal(table)}"
    )


def _write_profile_document(directory: Path, name: str, document: str) -> str:
    """Write malformed or incomplete profiles that profiles.py cannot produce."""

    (directory / f"{name}.toml").write_text(document, encoding="utf-8")
    return f"profile://{name}"


def test_uri_and_discrete_forms_reach_the_same_table(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Neither profile sets a dataset, so use a qualified query to isolate project selection."""

    table = name("both_forms")
    _create_probe_table(bigquery, project, dataset, table)
    query = _qualified_table_count(project, dataset, table)

    uri_only = make_profile(
        "uri_only", uri=f"bigquery:///{project}", **{"bigquery.dataset_id": None}
    )
    discrete = make_profile(
        "discrete",
        uri=None,
        **{"bigquery.project_id": project, "bigquery.dataset_id": None},
    )

    assert Session(duck, uri_only).scalar(query) == 1
    assert Session(duck, discrete).scalar(query) == 1


@pytest.mark.parametrize(
    "parameters", ["DatasetId={dataset}", "OAuthType=0&DatasetId={dataset}&Location=US"]
)
def test_uri_options_resolve_an_unqualified_table_name(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
    parameters: str,
) -> None:
    table = name("uri_dataset")
    _create_probe_table(bigquery, project, dataset, table)
    profile = make_profile(
        "uri_dataset",
        uri=f"bigquery:///{project}?{parameters.format(dataset=dataset)}",
        # Dropped, so the URI parameter is the only thing naming a dataset.
        **{"bigquery.dataset_id": None},
    )

    assert Session(duck, profile).scalar(f"SELECT n FROM {sql.identifier(table)}") == 42


def test_dataset_id_option_resolves_an_unqualified_table_name(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    configured = make_profile("dataset_id_set")
    assert Session(duck, configured).scalar(UNQUALIFIED_TABLE_COUNT) is not None

    absent = make_profile("dataset_id_absent", **{"bigquery.dataset_id": None})
    with pytest.raises(duckdb.Error, match=re.escape(NO_DEFAULT_DATASET)):
        Session(duck, absent).scalar(UNQUALIFIED_TABLE_COUNT)


def test_legacy_aliases_configure_the_same_database(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    table = name("legacy_aliases")
    _create_probe_table(bigquery, project, dataset, table)
    profile = make_profile(
        "legacy_aliases",
        uri=None,
        **{
            "bigquery.dataset_id": None,
            "adbc.bigquery.sql.project_id": project,
            "adbc.bigquery.sql.dataset_id": dataset,
            "adbc.bigquery.sql.location": "US",
            "adbc.bigquery.sql.auth_type": "app_default_credentials",
        },
    )

    assert Session(duck, profile).scalar(f"SELECT n FROM {sql.identifier(table)}") == 42


def test_location_matching_the_dataset_is_applied(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    profile = make_profile("location_us", **{"bigquery.location": "US"})

    assert Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT) is not None


def test_location_pointing_at_the_wrong_region_is_rejected(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    project: str,
    dataset: str,
) -> None:
    profile = make_profile("location_eu", **{"bigquery.location": "europe-west4"})
    query = (
        f"SELECT COUNT(*) AS n FROM {sql.qualified(project, dataset)}.INFORMATION_SCHEMA.TABLES"
    )

    with pytest.raises(duckdb.Error, match=re.escape("was not found in location europe-west4")):
        Session(duck, profile).scalar(query)


def test_application_default_credentials_can_be_named_explicitly(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    """auth_bigquery is the default; app_default_credentials is its Go-compatible alias."""

    for auth_type in ("auth_bigquery", "app_default_credentials"):
        profile = make_profile(f"auth_{auth_type}", **{"bigquery.auth_type": auth_type})
        assert Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT) is not None


def test_quota_project_is_applied_to_the_credentials(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile, project: str
) -> None:
    """Checks option acceptance, not billing: BigQuery bills the job project."""

    profile = make_profile("quota_project", **{"bigquery.auth.quota_project": project})

    assert Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT) is not None


def test_a_boolean_option_is_accepted_as_a_TOML_boolean(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    for value in (True, False):
        profile = make_profile(
            f"allow_untrusted_{value}",
            **{
                "bigquery.storage_endpoint": "http://127.0.0.1:9",
                "bigquery.allow_untrusted_endpoint": value,
            },
        )
        if value:
            # A complete REST response needs no connection to the Storage endpoint.
            assert Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT) is not None
        else:
            with pytest.raises(
                duckdb.Error,
                match=re.escape(
                    "BigQuery option bigquery.storage_endpoint must be a Google API HTTPS endpoint"
                ),
            ):
                Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT)


def test_an_integer_value_reaches_the_driver_as_an_integer(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    """No database option accepts integers; the wrong-type error proves TOML did not stringify it."""

    profile = make_profile("integer_valued", **{"bigquery.allow_untrusted_endpoint": 1})

    with pytest.raises(
        duckdb.Error,
        match=re.escape(
            "BigQuery option bigquery.allow_untrusted_endpoint requires a string value, "
            "received integer"
        ),
    ):
        Session(duck, profile).scalar(UNQUALIFIED_TABLE_COUNT)


def test_env_var_interpolation_supplies_options(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    monkeypatch: pytest.MonkeyPatch,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
) -> None:
    """Set variables before the first statement loads the profile; missing variables expand to empty."""

    monkeypatch.setenv("DUCKDB_SUITE_DATASET", dataset)
    monkeypatch.setenv("DUCKDB_SUITE_URI", f"bigquery:///{project}")
    table = name("interpolated")
    _create_probe_table(bigquery, project, dataset, table)

    from_dataset_variable = make_profile(
        "env_dataset", **{"bigquery.dataset_id": "{{ env_var(DUCKDB_SUITE_DATASET) }}"}
    )
    assert (
        Session(duck, from_dataset_variable).scalar(f"SELECT n FROM {sql.identifier(table)}") == 42
    )

    from_uri_variable = make_profile("env_uri", uri="{{ env_var(DUCKDB_SUITE_URI) }}")
    assert (
        Session(duck, from_uri_variable).scalar(
            _qualified_table_count(project, dataset, table)
        )
        == 1
    )


def test_an_unknown_bigquery_option_key_is_rejected(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    profile = make_profile("unknown_key", **{"bigquery.datasset_id": "whatever"})

    with pytest.raises(
        duckdb.Error,
        match=re.escape(
            'unknown BigQuery database option "bigquery.datasset_id"; '
            'did you mean "bigquery.dataset_id"?'
        ),
    ):
        Session(duck, profile).scalar("SELECT 1 AS n")


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("not a uri", "BigQuery URI scheme must be bigquery://"),
        ("postgresql://host/db", "BigQuery URI scheme must be bigquery://"),
        ("bigquery://", "BigQuery URI must contain a project ID path"),
        ("bigquery:///", "BigQuery URI must contain exactly one project ID path segment"),
        (
            "bigquery://bigquery.googleapis.com:port/some-project",
            'BigQuery URI endpoint port "port" must be decimal digits only',
        ),
        (
            "bigquery:///some-project?DatasetId=a&DatasetId=b",
            "BigQuery URI query parameter DatasetId must not be repeated",
        ),
        (
            "bigquery:///some-project?Nope=1",
            "BigQuery URI query parameter Nope is not one of OAuthType",
        ),
        (
            "bigquery:///some-project?OAuthType=9",
            "BigQuery URI OAuthType must be 0, 1, 2, or 3",
        ),
    ],
)
def test_a_malformed_uri_is_rejected(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile, uri: str, message: str
) -> None:
    """URI validation fails during database initialization, before contacting BigQuery."""

    profile = make_profile("malformed_uri", uri=uri, **{"bigquery.dataset_id": None})

    with pytest.raises(duckdb.Error, match=re.escape(message)):
        Session(duck, profile).scalar("SELECT 1 AS n")


def test_a_nonexistent_project_is_rejected(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    """Assert the driver context: BigQuery can report an unknown project as an empty project ID."""

    profile = make_profile(
        "nonexistent_project",
        uri="bigquery:///adbc-no-such-project-1",
        **{"bigquery.dataset_id": None},
    )

    # DuckDB binds through a dry run, so the refusal is the dry-run job's.
    with pytest.raises(
        duckdb.Error, match=r"BigQuery rejected the (dry-run )?query job"
    ):
        Session(duck, profile).scalar("SELECT 1 AS n")


def test_a_bad_value_for_an_enumerated_option_is_rejected(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile
) -> None:
    profile = make_profile("bad_auth_type", **{"bigquery.auth_type": "nope"})

    with pytest.raises(
        duckdb.Error,
        match=re.escape('BigQuery option bigquery.auth_type must be one of "auth_bigquery"'),
    ):
        Session(duck, profile).scalar("SELECT 1 AS n")


def test_an_explicit_rest_endpoint_is_used_without_opt_in(
    duck: duckdb.DuckDBPyConnection, make_profile: MakeProfile, project: str
) -> None:
    requested_paths: list[str] = []

    class Endpoint(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requested_paths.append(self.path.partition("?")[0])
            body = b'{"error":{"code":400,"message":"configured endpoint reached"}}'
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Endpoint) as server:
        worker = Thread(target=server.serve_forever)
        worker.start()
        try:
            profile = make_profile(
                "explicit_rest_endpoint",
                **{
                    "bigquery.endpoint": f"http://127.0.0.1:{server.server_port}",
                    "bigquery.allow_untrusted_endpoint": False,
                    "bigquery.auth_type": "anonymous",
                },
            )
            with pytest.raises(duckdb.Error, match="configured endpoint reached"):
                Session(duck, profile).scalar("SELECT 1 AS n")
            assert requested_paths == [f"/bigquery/v2/projects/{project}/jobs"]
        finally:
            server.shutdown()
            worker.join()


def test_a_profile_name_with_no_file_is_reported(
    duck: duckdb.DuckDBPyConnection, profile_directory: Path
) -> None:
    """profile_directory sets ADBC_PROFILE_PATH even though its value is unused here."""

    with pytest.raises(duckdb.Error, match=re.escape("Profile not found: no_such_profile")):
        duck.execute(sql.read_adbc("profile://no_such_profile", "SELECT 1 AS n"))


def test_a_profile_with_an_unsupported_version_is_reported(
    duck: duckdb.DuckDBPyConnection, profile_directory: Path, driver_library: str, project: str
) -> None:
    profile = _write_profile_document(
        profile_directory,
        "unsupported_version",
        f'profile_version = 99\ndriver = "{driver_library}"\n\n'
        f'[Options]\nuri = "bigquery:///{project}"\n',
    )

    with pytest.raises(duckdb.Error, match=re.escape("Profile version '99' is not supported")):
        duck.execute(sql.read_adbc(profile, "SELECT 1 AS n"))


def test_a_profile_naming_a_driver_that_is_not_there_is_reported(
    duck: duckdb.DuckDBPyConnection, profile_directory: Path, project: str
) -> None:
    """The driver path, unlike Options, does not support environment interpolation."""

    missing = profile_directory / "not_a_driver.so"
    profile = _write_profile_document(
        profile_directory,
        "missing_driver",
        f'profile_version = 1\ndriver = "{missing}"\n\n'
        f'[Options]\nuri = "bigquery:///{project}"\n',
    )

    with pytest.raises(duckdb.Error, match=re.escape(f"Could not load `{missing}`")):
        duck.execute(sql.read_adbc(profile, "SELECT 1 AS n"))


def test_a_profile_without_an_options_table_is_reported(
    duck: duckdb.DuckDBPyConnection, profile_directory: Path, driver_library: str
) -> None:
    profile = _write_profile_document(
        profile_directory,
        "no_options_table",
        f'profile_version = 1\ndriver = "{driver_library}"\n',
    )

    with pytest.raises(duckdb.Error, match=re.escape("Profile options is not a table")):
        duck.execute(sql.read_adbc(profile, "SELECT 1 AS n"))


def test_two_profiles_on_one_connection_do_not_leak(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
    secondary_dataset: str,
) -> None:
    """Revisit the first profile to catch the second overwriting its cached database."""

    table = name("primary_only")
    _create_probe_table(bigquery, project, dataset, table)
    query = (
        f"SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE table_name = {sql.string_literal(table)}"
    )

    primary = make_profile("leak_primary")
    secondary = make_profile("leak_secondary", **{"bigquery.dataset_id": secondary_dataset})

    assert Session(duck, primary).scalar(query) == 1
    assert Session(duck, secondary).scalar(query) == 0
    assert Session(duck, primary).scalar(query) == 1


def test_two_profiles_attach_as_two_catalogs_at_once(
    duck: duckdb.DuckDBPyConnection,
    make_profile: MakeProfile,
    bigquery: Session,
    name: Callable[[str], str],
    project: str,
    dataset: str,
    secondary_dataset: str,
) -> None:
    primary_table = name("attached_primary")
    secondary_table = name("attached_secondary")
    _create_probe_table(bigquery, project, dataset, primary_table)
    _create_probe_table(bigquery, project, secondary_dataset, secondary_table)

    duck.execute(sql.attach(make_profile("attach_primary"), "bq_primary"))
    duck.execute(
        sql.attach(
            make_profile("attach_secondary", **{"bigquery.dataset_id": secondary_dataset}),
            "bq_secondary",
        )
    )

    attached = {
        row[0] for row in duck.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    }
    assert {"bq_primary", "bq_secondary"} <= attached

    for catalog, db_schema, table in (
        ("bq_primary", dataset, primary_table),
        ("bq_secondary", secondary_dataset, secondary_table),
    ):
        rows = duck.execute(
            f"SELECT n FROM {sql.attached(catalog, db_schema, table)}"
        ).fetchall()
        assert rows == [(42,)]
