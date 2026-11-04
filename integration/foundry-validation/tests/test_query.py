from __future__ import annotations

import adbc_drivers_validation.tests.query as query_tests
from adbc_drivers_validation import model

from . import bigquery

# Exercise every scalar binding fixture through both array and stream inputs.
_BIND_QUERIES = {
    query.name
    for query in bigquery.get_quirks().query_set.queries.values()
    if isinstance(query.query, model.SelectQuery) and query.query.bind_path is not None
}


def pytest_generate_tests(metafunc) -> None:
    query_tests.generate_tests(
        [bigquery.get_quirks()],
        metafunc,
        bind_dictionary_queries=_BIND_QUERIES,
        bind_stream_queries=_BIND_QUERIES,
    )


class TestQuery(query_tests.TestQuery):
    pass
