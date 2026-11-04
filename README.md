# bigquery-adbc

> [!WARNING]
> **Experimental.** This project is AI-generated and has not seen real-world use.

An [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/current/format/specification.html)
driver for Google BigQuery, written in Rust. Build it as a shared library for ADBC driver managers,
install the Python package from PyPI, or use it as a Rust dependency from git. The driver is not on
crates.io yet.

## Status

The driver supports the ADBC 1.0 and 1.1 C interfaces and GoogleSQL:

- Queries and scripts with Arrow results, read over REST or, for large results, the BigQuery
  Storage Read API.
- Query parameters bound from Arrow, positional (`?`) or named (`@name`).
- Bulk ingestion through the BigQuery Storage Write API, in all four ADBC ingest modes.
- Metadata: `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetStatistics` and `GetInfo`.
- Manual-commit transactions, cancellation, `ExecutePartitions`, dry runs and progress reporting.
- The option keys and option-only operations that dbt-core 2.0 uses
  ([DBT-CORE.md](docs/DBT-CORE.md)).

CI runs offline tests and checks built Python wheels. Separate live suites exercise the driver
through ADBC C++, Python, Driver Foundry, DuckDB and dbt-core 2.0. See the
[runners and coverage notes](CONTRIBUTING.md#live-tests) for setup, exclusions and local patches.

Legacy SQL, load jobs for ADBC bulk ingestion, incremental execution and temporary-table ingestion
are not supported. dbt CSV seeds use a separate [load-job path](docs/DBT-CORE.md#csv-seeds).
See the [known limitations](docs/REVIEW.md) and the
[differences from the Go driver](docs/GO-DRIVER-COMPARISON.md), the reference implementation.

## Python quickstart

Python 3.11 or newer is required:

```sh
python3 -m venv .venv
. .venv/bin/activate                         # Windows: .venv\Scripts\Activate.ps1
python -m pip install "bigquery-adbc[dbapi]"
```

The `dbapi` extra installs PyArrow; the `pandas` extra also adds pandas for `cursor.fetch_df()`. To
build from source instead, see the [Python package](python/README.md#install).
Configure Application Default Credentials (ADC) with the Google Cloud CLI:

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

Use `cursor.fetch_arrow_table()` to read a result as Arrow instead of Python tuples.

Use `autocommit=True` for independent queries. The DB-API default, `False`, opens a BigQuery
transaction session when a query needs one, which adds round trips, and leaving the `with` block
rolls back uncommitted work unless you call `connection.commit()`.

The URI can also select a dataset and location, for example
`bigquery:///my-project?DatasetId=my_dataset&Location=US`. See
[connection URIs](docs/REFERENCE.md#connection-uris) and
[database options](docs/REFERENCE.md#database-options) for other credentials and settings, and
the [Python guide](python/README.md) for wheels and development setup.

## C and Rust

Build the shared library from the repository root:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
```

This writes `target/release/libadbc_driver_bigquery.so` on Linux, `libadbc_driver_bigquery.dylib`
on macOS, or `adbc_driver_bigquery.dll` on Windows. `cargo build` produces only the Rust library.

The library exports `AdbcDriverBigqueryInit` and the generic `AdbcDriverInit`. Keep it loaded for
the life of the process: the `AdbcDriver.release` callback does not stop its worker threads.
The reference C driver manager never unloads drivers.

Link your application with `-ladbc_driver_manager` and include `<arrow-adbc/adbc.h>`. Before
`AdbcDatabaseInit`, set the `driver` database option to the shared library's path and `uri` to
`bigquery:///my-project`. The manager resolves `AdbcDriverBigqueryInit` automatically.

Check each call's `AdbcStatusCode` before using its output. Release returned Arrow streams and
ADBC handles, including on error paths; `AdbcError.release` frees the error when non-null. The
[C++ validation harness](integration/adbc-validation/README.md) shows how this repository loads
and tests the library through the C driver manager.

The C ABI and the Python package are the supported interfaces. The Rust crate is available only
from Git, is unstable, and may change without notice. To use it anyway, see the
[Rust API reference](docs/REFERENCE.md#rust-api), which lists the `adbc_core` revision and Arrow
patches your workspace needs, with an example.

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

Ingestion requires autocommit. See [bulk ingest](docs/REFERENCE.md#bulk-ingest) for the other
modes, column matching and failure handling.

Parameters are bound as an Arrow record batch, and the statement runs once per row. Placeholders
are positional (`?`) by default; in named mode each field fills the `@name` placeholder matching
its name:

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

The standard `adbc.statement.bind_by_name=true` also selects named mode. See
[bound parameters](docs/REFERENCE.md#bound-parameters) for the Arrow type mapping.

## Documentation

| Guide | Contents |
| --- | --- |
| [Driver reference](docs/REFERENCE.md) | Options, type mappings, row counts, result metadata and diagnostics. |
| [Python](python/README.md) | DB-API usage, package builds and Python development. |
| [Contributing](CONTRIBUTING.md) | Local checks, live tests and contribution guidelines. |
| [Known limitations](docs/REVIEW.md) | Open limitations and validation gaps. |
| [Go driver comparison](docs/GO-DRIVER-COMPARISON.md) | Differences from the Go implementation. |
| [dbt-core 2.0](docs/DBT-CORE.md) | Loading the driver in dbt, supported operations and known gaps. |
| [Performance](docs/PERFORMANCE.md) and [binary size](docs/BINARY-SIZE.md) | Measurements and how to reproduce them. |
| [Upstream issues](docs/UPSTREAM.md) | Issues in dependencies and services. |
| [Security policy](SECURITY.md) | Reporting vulnerabilities, and what is in scope. |

## License

[Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for adapted-source attribution.
Wheels and native release archives include the
[dependency licenses](python/licenses/THIRD_PARTY_LICENSES.txt) and
[Rust standard-library notices](python/licenses/RUST-STD.html).
See [distribution packaging](CONTRIBUTING.md#distribution-packaging) before distributing a native build.
