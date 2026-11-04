from __future__ import annotations

import adbc_drivers_validation.tests.ingest as ingest_tests
from adbc_drivers_validation import model

from . import bigquery


def pytest_generate_tests(metafunc) -> None:
    ingest_tests.generate_tests(
        [bigquery.get_quirks()],
        metafunc,
        ingest_dictionary_queries={
            query.name
            for query in bigquery.get_quirks().query_set.queries.values()
            if isinstance(query.query, model.IngestQuery)
        },
        # Upstream mode helpers require an index and one value column; the
        # three-column timestamptz fixtures are still covered by test_create.
        ingest_mode_queries={
            "ingest/string",
            "ingest/string_view",
            "ingest/binary_view",
            "ingest/int64",
            "ingest/decimal",
            "ingest/decimal_scale_negative",
            "ingest/time_ns",
            "ingest/timestamp_ns",
            "ingest/timestamp_us",
        },
        long_value_queries={
            "ingest/string",
            "ingest/binary",
            "ingest/large_string",
            "ingest/large_binary",
            "ingest/string_view",
            "ingest/binary_view",
        },
    )


class TestIngest(ingest_tests.TestIngest):
    pass
