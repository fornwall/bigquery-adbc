# BigQuery ADBC driver for Python

`bigquery-adbc` bundles the native BigQuery ADBC driver and loads it through
`adbc-driver-manager`. Requires Python 3.11 or newer.

This project is experimental and AI-generated, and has not seen real-world use.
See the [known limitations](https://github.com/fornwall/bigquery-adbc/blob/main/docs/REVIEW.md).

## Install

Nothing is published to PyPI yet. Install [Rust](https://rustup.rs/), then run this from the
[repository](https://github.com/fornwall/bigquery-adbc) root to compile the native driver and
install the package. The build uses the repository's pinned Rust toolchain:

```sh
python3 -m pip install "./python[dbapi]"
```

To install a built wheel, replace `./python` with its path. The `dbapi` extra installs PyArrow; the
`pandas` extra also adds pandas for `cursor.fetch_df()`. The low-level `bigquery_adbc.connect()` API
needs neither.

## Usage

The driver uses Application Default Credentials (ADC) by default. For local development, set
them up with:

```sh
gcloud auth application-default login
```

Replace `my-gcp-project` with a project where those credentials can run BigQuery jobs:

```python
from bigquery_adbc import dbapi

with dbapi.connect("bigquery:///my-gcp-project", autocommit=True) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        print(cursor.fetchone())
```

The low-level ADBC API can take its whole configuration as options:

```python
import bigquery_adbc as bigquery

with bigquery.connect(
    db_kwargs={
        bigquery.DatabaseOptions.PROJECT_ID: "my-gcp-project",
    }
) as database:
    print(database.get_option(bigquery.DatabaseOptions.PROJECT_ID))
```

`bigquery.connect()` returns an `adbc_driver_manager.AdbcDatabase`; `dbapi.connect()` returns a
DB-API connection. Both accept a URI, `db_kwargs`, or both, and explicit options override URI
parameters. See [connection URIs](https://github.com/fornwall/bigquery-adbc/blob/main/docs/REFERENCE.md#connection-uris)
for dataset, location, quota-project and impersonation settings.

### Transactions, batching, and errors

Use `autocommit=True` for independent queries. With the PEP 249 default, `autocommit=False`, the
first query that needs a transaction opens a BigQuery session. BigQuery rejects permanent-table
DDL such as `CREATE TABLE` inside a transaction, and the driver rejects `adbc_ingest`. Leaving
the `with` block rolls back uncommitted work; call `connection.commit()` first to keep it.

`executemany` runs one BigQuery job per parameter row. For bulk inserts, use `cursor.adbc_ingest`,
which streams rows through the Storage Write API. On a reused cursor, an `adbc_ingest` call that
omits `catalog_name` or `db_schema_name` keeps the previous call's value; pass `""` for the
connection's default
([UP-95](https://github.com/fornwall/bigquery-adbc/blob/main/docs/UPSTREAM.md#up-95-cursoradbc_ingest-keeps-an-earlier-calls-catalog-and-schema)).

Invalid credentials raise `ProgrammingError`; transient token-exchange failures raise
`OperationalError`. Catch `dbapi.DatabaseError` to handle both. See
[error statuses](https://github.com/fornwall/bigquery-adbc/blob/main/docs/REFERENCE.md#errors).

Set `ADBC_DRIVER_BIGQUERY_LOG_LEVEL=warn` before the first `connect()` to log Storage Read fallbacks
and failed cancellations to standard error; `debug` also logs retries. Jupyter shows these in the
kernel console. See [diagnostics](https://github.com/fornwall/bigquery-adbc/blob/main/docs/REFERENCE.md#diagnostics).

## Options

Configure database and connection options when connecting, and statement options on a cursor:

| Enum | Set with | Examples |
| --- | --- | --- |
| `DatabaseOptions` | `connect(db_kwargs={...})` | Credentials, project, dataset and location. |
| `ConnectionOptions` | `dbapi.connect(conn_kwargs={...})` | Catalog, schema and result reading. |
| `StatementOptions` | `cursor.adbc_statement.set_options(**{...})` | Parameters, query limits and ingestion. |

Pass `autocommit` as a keyword to `dbapi.connect()`, not in `conn_kwargs`. New statements inherit
their connection's read controls and bulk-ingest method.

Option enums are `StrEnum`s and work directly as keys and values:
`str(DatabaseOptions.PROJECT_ID)` is `"bigquery.project_id"`.
The manager's typed getters need `.value`, for example
`statement.get_option_float(StatementOptions.PROGRESS.value)`.

`BulkIngestMethod.STORAGE_WRITE` is the only supported bulk-ingest method; the raw value `"load"`
returns `NOT_IMPLEMENTED`. `StatementOptions.QUERY_MAXIMUM_BILLING_TIER` also returns
`NOT_IMPLEMENTED`; use `QUERY_MAXIMUM_BYTES_BILLED` to cap query cost.

See the [option reference](https://github.com/fornwall/bigquery-adbc/blob/main/docs/REFERENCE.md#options)
for defaults, constraints, and write-only credential options. Reading `uri` returns the configured
URI with credentials redacted; read individual options to see the effective configuration.

## Local builds

Use [uv](https://docs.astral.sh/uv/) for builds and development. From the repository root:

```sh
scripts/build-python-package.sh
uvx twine check dist/*
uvx check-wheel-contents dist/*.whl
```

The script writes a source distribution and a wheel to `dist/`; `uv build` inside `python/` writes
them to `python/dist/`.

Wheels include `LICENSE`, `NOTICE`, the complete collected dependency license texts, and the
Rust standard-library notices under their `.dist-info/licenses/` directory. Source distributions
carry the same bundle. Builds reject a bundle that no longer matches the dependency manifests,
lockfile, or Rust version. See [distribution packaging](../CONTRIBUTING.md#distribution-packaging)
for regeneration and native release archives.

Wheel builds run `cargo rustc --release --locked --lib --crate-type cdylib`. The build checks the
library's format, architecture and ADBC exports, then derives the wheel's platform tag from it.
Explicit platform tags and multi-architecture libraries are rejected.

| Environment variable | Effect |
| --- | --- |
| `BIGQUERY_ADBC_BUILD_LIBRARY` | At build time, package the library at this absolute path instead of running Cargo. The same checks run. |
| `BIGQUERY_ADBC_LIBRARY` | At runtime, load a development library from this path instead of the bundled one. |

The enums in `python/src/bigquery_adbc/_options.py` are generated from the Rust option
declarations, and `cargo test` checks that they are current. After changing an option key, value
or Python exposure, regenerate them from the repository root:

```sh
cargo run --locked --features python-option-codegen --bin generate-python-options
```

Local Linux builds carry a `linux_<arch>` development tag. Build portable release wheels with
cibuildwheel:

```sh
uvx --from 'cibuildwheel[uv]==4.3.5' cibuildwheel --output-dir target/wheelhouse python
```

The [wheel workflow](https://github.com/fornwall/bigquery-adbc/blob/main/.github/workflows/wheels.yml)
builds and install-tests `py3-none` wheels for Linux x86-64 and AArch64 (manylinux 2.28), macOS
x86-64 and arm64, and Windows x86-64. Each wheel contains one architecture and works across the
supported Python versions.

## Local tests

From the repository root, run the same offline checks as CI:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
```

The scripts install locked dependencies and check the package and integration helpers. They need
neither credentials nor a native driver build. To run an individual package test or apply
formatting:

```sh
uv run --project python --locked pytest -q python/tests/test_api.py
uv run --project python --locked ruff format python
```

Ruff is configured in `python/ruff.toml` and Pyrefly in `python/pyproject.toml`.

An editable install does not compile Rust. To use a native driver from it, build the library from
the repository root and set its path:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
export BIGQUERY_ADBC_LIBRARY="$PWD/target/release/libadbc_driver_bigquery.so"
uv run --project python --locked python
```

Use `libadbc_driver_bigquery.dylib` on macOS or `adbc_driver_bigquery.dll` on Windows.

The live suite builds a fresh wheel and tests it in an isolated environment, using ADC. It
installs the driver manager from the same pinned ADBC revision as the Rust tests; CI's
installed-wheel tests separately cover the released driver manager.

```sh
gcloud auth application-default login
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-project
scripts/run-python-tests.sh
scripts/run-python-tests.sh -k parameter_binding
```

`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` must name a project where your credentials can run queries and
create datasets. It is required, including in emulator mode; missing or empty values fail, with
no default or fallback to the active gcloud project. Each run creates a uniquely named,
labelled dataset whose tables expire after one day. Cleanup drops a dataset only if its prefix and
ownership labels match.

To run the same Python suite against a running BigQuery emulator instead, explicitly set both
service endpoints. Real BigQuery remains the default:

```sh
ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT=http://127.0.0.1:9050 \
ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT=http://127.0.0.1:9060 \
scripts/run-python-tests.sh -k select_through_low_level_and_dbapi
```

Set both variables to HTTP or HTTPS origins, without paths or credentials; a partial or invalid
configuration fails before the build. The runner bypasses ADC checks and directs all service
requests to the emulator using anonymous authentication. The package itself does not read these
test-only variables. Dataset cleanup still checks ownership labels, but emulator fixtures omit
table expiration. Remove `-k` to attempt the complete suite; emulator incompatibilities remain
test failures. See the repository's
[emulator instructions](https://github.com/fornwall/bigquery-adbc/blob/main/CONTRIBUTING.md#bigquery-emulator)
for starting one and the current validation scope.
