from __future__ import annotations

import inspect

import adbc_driver_manager
import adbc_drivers_validation.tests.connection as connection_tests
import pyarrow
import pytest
from adbc_drivers_validation import model

from . import bigquery

_UPSTREAM_TEST_GET_STATISTICS = connection_tests.TestConnection.__dict__.get("test_get_statistics")
assert _UPSTREAM_TEST_GET_STATISTICS is not None, (
    "pinned Driver Foundry renamed or removed TestConnection.test_get_statistics; "
    "review the UP-38 approximate row-count workaround"
)
assert "assert row_count_value >= 3" in inspect.getsource(_UPSTREAM_TEST_GET_STATISTICS), (
    "pinned Driver Foundry changed its approximate row-count assertion; "
    "review and remove the UP-38 workaround if it is fixed"
)

_GET_STATISTIC_NAMES_SCHEMA = pyarrow.schema(
    [
        pyarrow.field("statistic_name", pyarrow.string(), nullable=False),
        pyarrow.field("statistic_key", pyarrow.int16(), nullable=False),
    ]
)


def pytest_generate_tests(metafunc) -> None:
    connection_tests.generate_tests([bigquery.get_quirks()], metafunc)


class TestConnection(connection_tests.TestConnection):
    @pytest.mark.requires_features(["connection_get_statistics"])
    def test_get_statistics(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_statistics_table: tuple[str, str, str],
    ) -> None:
        """Run the upstream checks without demanding current approximate counts."""
        table_name = get_statistics_table[-1]
        table = conn.adbc_get_statistics(
            catalog_filter=driver.features.current_catalog,
            db_schema_filter=driver.features.current_schema,
            table_name_filter=table_name,
            approximate=True,
        ).read_all()

        assert table.schema.equals(connection_tests._EXPECTED_GET_STATISTICS_SCHEMA)

        table_found = False
        table_stats = []
        column_stats = {}
        for row in table.to_pylist():
            if driver.features.current_catalog:
                assert row["catalog_name"] == driver.features.current_catalog

            for schema in row["catalog_db_schemas"]:
                if driver.features.current_schema:
                    assert schema["db_schema_name"] == driver.features.current_schema

                for statistic in schema["db_schema_statistics"]:
                    if statistic["table_name"] != table_name:
                        continue
                    table_found = True
                    column_name = statistic["column_name"]
                    if column_name is None:
                        table_stats.append(statistic)
                    else:
                        column_stats.setdefault(column_name, []).append(statistic)

        assert table_found, f"Table {table_name} not found in statistics results"

        all_stats = table_stats + [statistic for statistics in column_stats.values() for statistic in statistics]
        for statistic in all_stats:
            key = statistic["statistic_key"]
            assert key >= 0, f"Invalid negative statistic key: {key}"

        names = conn.adbc_get_statistic_names().read_all()
        assert names.schema.equals(_GET_STATISTIC_NAMES_SCHEMA), (
            "GetStatisticNames returned schema does not match ADBC specification"
        )
        vendor_keys = {statistic["statistic_key"] for statistic in all_stats if statistic["statistic_key"] >= 1024}
        missing_keys = vendor_keys - set(names.column("statistic_key").to_pylist())
        assert not missing_keys, (
            "GetStatistics returned implementation-specific statistic key(s) not "
            f"advertised by GetStatisticNames: {sorted(missing_keys)}"
        )

        row_count_stat = next(
            (statistic for statistic in table_stats if statistic["statistic_key"] == 6),
            None,
        )
        if row_count_stat is not None:
            # Upstream demands at least the three ingested rows; BigQuery's
            # approximate table metadata converges long after the ingest commits.
            assert row_count_stat["statistic_value"] >= 0

        if "name" in column_stats:
            null_count_stat = next(
                (statistic for statistic in column_stats["name"] if statistic["statistic_key"] == 5),
                None,
            )
            if null_count_stat is not None:
                assert null_count_stat["statistic_value"] >= 1
