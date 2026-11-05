# BigQuery ADBC driver for Python

`bigquery-adbc` gives Python columnar access to Google BigQuery, through a DB-API 2.0 interface or
the low-level ADBC API. Results arrive as Arrow, and bulk ingestion streams Arrow to the BigQuery
Storage Write API. The wheels bundle the native Rust driver; a Rust toolchain is needed only when
building from source.
Requires Python 3.11 or newer.

This project is experimental and AI-generated, and has not seen real-world use.
See the [known limitations](https://github.com/fornwall/bigquery-adbc/blob/main/docs/LIMITATIONS.md).

## Install

```sh
python3 -m pip install "bigquery-adbc[dbapi]"
```

The `dbapi` extra installs PyArrow; the `pandas` extra also adds pandas for `cursor.fetch_df()`. The
low-level `bigquery_adbc.connect()` API needs neither.

To build from source, install [Rust](https://rustup.rs/), then run this from the
[repository](https://github.com/fornwall/bigquery-adbc) root. The build uses the repository's pinned
Rust toolchain:

```sh
python3 -m pip install "./python[dbapi]"
```

To install a built wheel, replace `./python` with its path.

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

`cursor.fetch_arrow_table()` reads a result as Arrow and `cursor.fetch_df()` as pandas. See
[more examples](https://github.com/fornwall/bigquery-adbc#more-examples) of bulk ingestion and
query parameters.

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

## Development

Package builds, local tests and loading a development driver are covered in the
[Python development guide](https://github.com/fornwall/bigquery-adbc/blob/main/python/DEVELOPMENT.md).
