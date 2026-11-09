# bigquery-adbc

[![CI](https://github.com/fornwall/bigquery-adbc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fornwall/bigquery-adbc/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/bigquery-adbc)](https://pypi.org/project/bigquery-adbc/)
[![Python versions](https://img.shields.io/pypi/pyversions/bigquery-adbc)](https://pypi.org/project/bigquery-adbc/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

> [!WARNING]
> **Experimental.** This project is AI-generated and has not seen real-world use.
> For an established alternative, use the [Go Bigquery ADBC driver](https://github.com/adbc-drivers/bigquery/tree/main/go).

An [ADBC](https://arrow.apache.org/adbc/current/format/specification.html) driver for Google
BigQuery, written in Rust, for Python, C, C++, Rust, DuckDB and dbt. Python wheels bundle the
native library for Linux, macOS and Windows.

## Status

The driver supports the ADBC 1.0 and 1.1 C interfaces and GoogleSQL:

- Queries and scripts with Arrow results. Complete result pages are decoded from REST JSON;
  paginated results use the BigQuery Storage Read API when available.
- Query parameters bound from Arrow, positional (`?`) or named (`@name`).
- Bulk ingestion through the BigQuery Storage Write API, in all four ADBC ingest modes.
- Metadata: `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetStatistics` and `GetInfo`.
- Manual-commit transactions, cancellation, `ExecutePartitions`, dry runs and progress reporting.
- The option keys and option-only operations that dbt 2.0 uses
  ([DBT.md](docs/DBT.md)).

Legacy SQL, load jobs for ADBC bulk ingestion, incremental execution and temporary-table ingestion
are not supported; dbt CSV seeds use a separate [load-job path](docs/DBT.md#csv-seeds). See the
[known limitations](docs/LIMITATIONS.md) and the
[differences from the Go driver](docs/GO-DRIVER-COMPARISON.md), the reference implementation.

## Quickstart

Python 3.11 or newer is required:

```sh
python3 -m venv .venv
. .venv/bin/activate                         # Windows: .venv\Scripts\Activate.ps1
python -m pip install "bigquery-adbc[dbapi]"
```

The `dbapi` extra installs PyArrow; the `pandas` extra also adds pandas for `cursor.fetch_df()`.
To build from source, see the [Python package](python/README.md#install). Configure Application
Default Credentials (ADC):

```sh
gcloud auth application-default login
```

Replace `my-project` with a project where those credentials can run BigQuery jobs:

```python
from bigquery_adbc import dbapi

with dbapi.connect("bigquery:///my-project", autocommit=True) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS n")
        print(cursor.fetchone())  # (1,)
```

Use `cursor.fetch_arrow_table()` for Arrow or `cursor.fetch_df()` for pandas. Use
`autocommit=True` for independent queries: the DB-API default, `False`, opens a BigQuery
transaction session when a query needs one, which adds round trips, and leaving the `with` block
rolls back uncommitted work unless you call `connection.commit()`.

The URI can select a dataset and location: `bigquery:///my-project?DatasetId=my_dataset&Location=US`.
See [connection URIs](docs/REFERENCE.md#connection-uris),
[database options](docs/REFERENCE.md#database-options) and the [Python guide](python/README.md).

## dbt 2

Try the driver with `dbt-bigquery-adbc`, a patched dbt 2 that uses your existing project and
profiles. On Linux or macOS, install or update it alongside your normal `dbt`:

```sh
curl -fsSL https://raw.githubusercontent.com/fornwall/bigquery-adbc/main/tools/dbt-build/install.sh | sh -s -- --update
```

This installs the latest release into `~/.local/bin`. Run `dbt-bigquery-adbc build` from your dbt
project. See [DBT.md](docs/DBT.md) for usage, compatibility notes and developer instructions.

## More examples

Bulk ingestion streams Arrow data through the Storage Write API. The dataset must exist, and
`mode="create"` requires a new table:

```python
import pyarrow as pa
from bigquery_adbc import dbapi

data = pa.table({
    "id": pa.array([1, 2, 3], type=pa.int64()),
    "name": pa.array(["one", "two", None], type=pa.string()),
})

with dbapi.connect("bigquery:///my-project", autocommit=True) as connection:
    with connection.cursor() as cursor:
        rows = cursor.adbc_ingest(
            "ingested_values", data, mode="create",
            catalog_name="my-project", db_schema_name="my_dataset",
        )
        print(rows)  # 3
```

Ingestion requires autocommit; see [bulk ingest](docs/REFERENCE.md#bulk-ingest). Parameters are
bound as an Arrow record batch and the statement runs once per row. Placeholders are positional
(`?`) by default; in named mode each field fills the `@name` placeholder matching its name:

```python
import pyarrow as pa
from bigquery_adbc import ParameterMode, StatementOptions, dbapi

with dbapi.connect("bigquery:///my-project", autocommit=True) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT ? AS n", pa.record_batch([pa.array([7])], names=[""]))
        print(cursor.fetchone())  # (7,)
        cursor.adbc_statement.set_options(
            **{StatementOptions.QUERY_PARAMETER_MODE: ParameterMode.NAMED}
        )
        cursor.execute("SELECT @n AS n", pa.record_batch([pa.array([7])], names=["n"]))
        print(cursor.fetchone())  # (7,)
```

`adbc.statement.bind_by_name=true` also selects named mode. See
[bound parameters](docs/REFERENCE.md#bound-parameters).

## Documentation

| Guide | Contents |
| --- | --- |
| [Driver reference](docs/REFERENCE.md) | Options, type mappings, row counts, result metadata and diagnostics. |
| [Python](python/README.md) | Installation, DB-API usage and options. |
| [Architecture](docs/ARCHITECTURE.md) | How a query and a bulk load move through the code. |
| [Contributing](CONTRIBUTING.md) | Building and loading the native library from C, C++ and Rust, local checks, live tests and contribution guidelines. |
| [Changelog](CHANGELOG.md) | Changes in each release. |
| [Known limitations](docs/LIMITATIONS.md) | Open limitations and validation gaps. |
| [Go driver comparison](docs/GO-DRIVER-COMPARISON.md) | Differences from the Go implementation. |
| [dbt 2.0](docs/DBT.md) | Loading the driver in dbt, supported operations and known gaps. |
| [Performance](docs/PERFORMANCE.md) and [binary size](docs/BINARY-SIZE.md) | Measurements and how to reproduce them. |
| [Upstream issues](docs/UPSTREAM.md) | Issues in dependencies and services. |
| [Security policy](SECURITY.md) | Reporting vulnerabilities, and what is in scope. |

## License

[Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for adapted-source attribution.
Wheels and native release archives include the
[dependency licenses](python/licenses/THIRD_PARTY_LICENSES.txt) and
[Rust standard-library notices](python/licenses/RUST-STD.html); see
[distribution packaging](CONTRIBUTING.md#distribution-packaging) before distributing a native build.
