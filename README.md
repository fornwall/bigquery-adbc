# bigquery-adbc

[![CI](https://github.com/fornwall/bigquery-adbc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fornwall/bigquery-adbc/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/bigquery-adbc)](https://pypi.org/project/bigquery-adbc/)
[![Python versions](https://img.shields.io/pypi/pyversions/bigquery-adbc)](https://pypi.org/project/bigquery-adbc/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

> [!WARNING]
> **Experimental.** This project is AI-generated and has not seen real-world use.

An [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/current/format/specification.html)
driver for Google BigQuery, written in Rust. One native library gives Python, C, C++ and Rust
programs, and ADBC clients such as DuckDB and dbt, columnar access to BigQuery. Python wheels
bundle the library for Linux, macOS and Windows.

## Status

The driver supports the ADBC 1.0 and 1.1 C interfaces and GoogleSQL:

- Queries and scripts with Arrow results. Complete result pages are decoded from REST JSON;
  paginated results use the BigQuery Storage Read API when available.
- Query parameters bound from Arrow, positional (`?`) or named (`@name`).
- Bulk ingestion through the BigQuery Storage Write API, in all four ADBC ingest modes.
- Metadata: `GetObjects`, `GetTableSchema`, `GetTableTypes`, `GetStatistics` and `GetInfo`.
- Manual-commit transactions, cancellation, `ExecutePartitions`, dry runs and progress reporting.
- The option keys and option-only operations that dbt-core 2.0 uses
  ([DBT-CORE.md](docs/DBT-CORE.md)).

Legacy SQL, load jobs for ADBC bulk ingestion, incremental execution and temporary-table ingestion
are not supported. dbt CSV seeds use a separate [load-job path](docs/DBT-CORE.md#csv-seeds).
See the [known limitations](docs/LIMITATIONS.md) and the
[differences from the Go driver](docs/GO-DRIVER-COMPARISON.md), the reference implementation.

For measured library sizes and dbt timings, see [binary size](docs/BINARY-SIZE.md) and
[performance](docs/PERFORMANCE.md#live-dbt-workload). Those results describe the recorded builds
and workloads, not performance guarantees.

## Quickstart

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

Use `cursor.fetch_arrow_table()` to read a result as Arrow instead of Python tuples, and
`cursor.fetch_df()` for pandas.

Use `autocommit=True` for independent queries. The DB-API default, `False`, opens a BigQuery
transaction session when a query needs one, which adds round trips, and leaving the `with` block
rolls back uncommitted work unless you call `connection.commit()`.

The URI can also select a dataset and location, for example
`bigquery:///my-project?DatasetId=my_dataset&Location=US`. See
[connection URIs](docs/REFERENCE.md#connection-uris) and
[database options](docs/REFERENCE.md#database-options) for other credentials and settings, and
the [Python guide](python/README.md) for more on the package.

## C and C++

Each [GitHub release](https://github.com/fornwall/bigquery-adbc/releases) has an archive of the
shared library for Linux x86-64 and AArch64 (glibc 2.28 or newer), macOS x86-64 and arm64, and
Windows x86-64, with a `SHA256SUMS` file. To build the library yourself, run this from the
repository root:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
```

This writes `target/release/libadbc_driver_bigquery.so` on Linux, `libadbc_driver_bigquery.dylib`
on macOS, or `adbc_driver_bigquery.dll` on Windows. `cargo build` produces only the Rust library.

The library exports `AdbcDriverBigqueryInit` and the generic `AdbcDriverInit`. Link your
application with `-ladbc_driver_manager`, include `<arrow-adbc/adbc.h>`, and point the driver
manager at the library before `AdbcDatabaseInit`:

```c
AdbcDatabaseNew(&database, &error);
AdbcDatabaseSetOption(&database, "driver", "/path/to/libadbc_driver_bigquery.so", &error);
AdbcDatabaseSetOption(&database, "uri", "bigquery:///my-project", &error);
AdbcDatabaseInit(&database, &error);
```

[`examples/query.c`](examples/query.c) is a complete program. Check each call's `AdbcStatusCode`
before using its output, and release returned Arrow streams and ADBC handles, including on error
paths; `AdbcError.release` frees the error when non-null. Keep the library loaded for the life of
the process, because the `AdbcDriver.release` callback does not stop its worker threads. The
reference C driver manager never unloads drivers. The
[C++ validation harness](integration/adbc-validation/README.md) shows how this repository loads and
tests the library.

### Loading by name

Each release archive also has `bigquery-adbc.toml`, a
[driver manifest](https://arrow.apache.org/adbc/current/format/driver_manifests.html) that lets ADBC
clients load the driver by name instead of by path. It expects the library in `/usr/local/lib` on
Linux and macOS, and in `C:\Program Files\ADBC\Drivers` on Windows. For any other location, edit
`Driver.shared`: a path must be absolute or on the dynamic loader's search path, because it is not
resolved relative to the manifest.

Install the manifest in `~/.config/adbc/drivers` on Linux,
`~/Library/Application Support/ADBC/Drivers` on macOS, or `%LOCALAPPDATA%\ADBC\Drivers` on Windows.
On Linux:

```sh
sudo cp libadbc_driver_bigquery.so /usr/local/lib/
mkdir -p ~/.config/adbc/drivers
cp bigquery-adbc.toml ~/.config/adbc/drivers/
```

Then set `driver` to `bigquery-adbc`. The C driver manager looks for manifests only when asked to,
so in C either set `driver` to the manifest's path or include `<arrow-adbc/adbc_driver_manager.h>`
and enable the lookup first:

```c
AdbcDriverManagerDatabaseSetLoadFlags(&database, ADBC_LOAD_FLAG_DEFAULT, &error);
AdbcDatabaseSetOption(&database, "driver", "bigquery-adbc", &error);
```

A driver's name is its manifest's file name, so this one can be installed beside a different
`bigquery` driver. To also select it from a `bigquery://` URI with no `driver` option, install the
manifest as `bigquery.toml` instead.

## Rust

The C ABI and the Python package are the supported interfaces. The Rust crate is available only
from Git, is unstable, and may change without notice. For Rust integration, see the
[Rust API reference](docs/REFERENCE.md#rust-api), which lists the `adbc_core` revision and Arrow
patches your workspace needs. From a checkout, [`examples/query.rs`](examples/query.rs) runs with:

```sh
cargo run --locked --example query -- my-project "SELECT 1 AS n"
```

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

## Testing

CI runs Rust unit and mock tests on Linux, macOS and Windows, Python unit tests on Linux, and
installation tests for each platform's Python wheel. It also runs the FFI layer under Miri and
fuzzes the Arrow IPC and REST decoders.

Separate live suites run against real BigQuery: the Rust tests, the Python package, Apache Arrow's
[ADBC C++ validation suite](integration/adbc-validation/README.md),
[Driver Foundry validation](integration/foundry-validation/README.md),
[DuckDB](integration/duckdb/README.md) and [dbt-core 2.0](integration/dbt/README.md). They need a
Google Cloud project, so CI does not run them. See the
[runners and coverage notes](CONTRIBUTING.md#live-tests) for setup, exclusions and local patches.

## Documentation

| Guide | Contents |
| --- | --- |
| [Driver reference](docs/REFERENCE.md) | Options, type mappings, row counts, result metadata and diagnostics. |
| [Python](python/README.md) | Installation, DB-API usage and options. |
| [Architecture](docs/ARCHITECTURE.md) | How a query and a bulk load move through the code. |
| [Contributing](CONTRIBUTING.md) | Local checks, live tests and contribution guidelines. |
| [Changelog](CHANGELOG.md) | Changes in each release. |
| [Known limitations](docs/LIMITATIONS.md) | Open limitations and validation gaps. |
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
