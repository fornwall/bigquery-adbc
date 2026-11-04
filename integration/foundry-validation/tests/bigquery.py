from __future__ import annotations

import functools
import tomllib
from pathlib import Path

import adbc_driver_manager
from adbc_drivers_validation import model, quirks


def _vendor_version() -> str:
    """The pinned BigQuery client library, which GetInfo reports as the vendor version."""

    with (Path(__file__).resolve().parents[3] / "Cargo.lock").open("rb") as lock:
        packages = tomllib.load(lock)["package"]
    [version] = {
        package["version"]
        for package in packages
        if package["name"] == "google-cloud-bigquery"
    }
    return f"google-cloud-bigquery {version}"


class BigQueryQuirks(model.DriverQuirks):
    """Driver Foundry configuration for the BigQuery ADBC driver."""

    name = "bigquery"
    driver = "adbc_driver_bigquery"
    driver_name = "bigquery-adbc"
    vendor_name = "Google BigQuery"
    vendor_version = _vendor_version()
    short_version = "current"

    # DDL row counts stay disabled because BigQuery does not report them.
    features = model.DriverFeatures(
        connection_get_statistics=True,
        connection_get_table_schema=True,
        connection_set_current_catalog=True,
        connection_set_current_schema=True,
        connection_transactions=True,
        get_objects=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        get_objects_tabletypes_filter=True,
        metadata_type_name=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=True,
        statement_bulk_ingest_schema=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        current_catalog=model.FromEnv("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"),
        current_schema=model.FromEnv("ADBC_BIGQUERY_DATASET"),
        secondary_schema=model.FromEnv("ADBC_BIGQUERY_SECONDARY_DATASET"),
        secondary_catalog=model.FromEnv("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT"),
        secondary_catalog_schema=model.FromEnv("ADBC_BIGQUERY_SECONDARY_DATASET"),
        supported_xdbc_fields=[
            "xdbc_type_name",
            "xdbc_nullable",
            "xdbc_is_nullable",
            "xdbc_is_autoincrement",
            "xdbc_is_generatedcolumn",
        ],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv(
                "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT", template="bigquery:///{}"
            ),
            "bigquery.dataset_id": model.FromEnv("ADBC_BIGQUERY_DATASET"),
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries" / "bigquery",)

    def is_table_not_found(
        self, table_name: str | None, error: Exception
    ) -> bool:
        del table_name
        return (
            isinstance(error, adbc_driver_manager.Error)
            and error.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
        )

    def is_retryable(self, error: Exception) -> bool:
        message = str(error).lower()
        return "ratelimitexceeded" in message or "rate limit" in message

    def quote_one_identifier(self, identifier: str) -> str:
        if "`" in identifier:
            raise ValueError(f"unsafe BigQuery identifier: {identifier!r}")
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def query_override(self, context: str, default: str) -> str:
        overrides = {
            "TestConnection.test_get_table_schema_catalog": (
                "(spam INT, eggs VARCHAR)",
                "(spam INT64, eggs STRING)",
            ),
            "TestConnection.test_get_table_schema_schema": (
                "(spam INT, eggs VARCHAR)",
                "(spam INT64, eggs STRING)",
            ),
            "TestStatement.sample_table": (
                "(id INT, value VARCHAR)",
                "(id INT64, value STRING)",
            ),
            "TestStatement.test_rows_affected.create_table": (
                "(id INT)",
                "(id INT64)",
            ),
        }
        if context in overrides:
            original, replacement = overrides[context]
            assert original in default, f"pinned Driver Foundry changed {context}: {default!r}"
            return default.replace(original, replacement)
        return super().query_override(context, default)

    @property
    def sample_ddl_constraints(self) -> list[str]:
        return [
            "CREATE TABLE constraint_primary "
            "(z INT64, a INT64, b STRING, PRIMARY KEY (a) NOT ENFORCED)",
            "CREATE TABLE constraint_primary_multi "
            "(z INT64, a INT64, b STRING, PRIMARY KEY (b, a) NOT ENFORCED)",
            "CREATE TABLE constraint_primary_multi2 "
            "(z INT64, a STRING, b INT64, PRIMARY KEY (a, b) NOT ENFORCED)",
            "CREATE TABLE constraint_foreign "
            "(z INT64, a INT64, b INT64, FOREIGN KEY (b) "
            "REFERENCES constraint_primary(a) NOT ENFORCED)",
            "CREATE TABLE constraint_foreign_multi "
            "(z INT64, a INT64, b INT64, c STRING, FOREIGN KEY (c, b) "
            "REFERENCES constraint_primary_multi2(a, b) NOT ENFORCED)",
            "ALTER TABLE constraint_primary DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi2 DROP COLUMN z",
            "ALTER TABLE constraint_foreign DROP COLUMN z",
            "ALTER TABLE constraint_foreign_multi DROP COLUMN z",
        ]


@functools.cache
def get_quirks() -> BigQueryQuirks:
    return BigQueryQuirks()
