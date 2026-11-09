# DuckDB ADBC extension integration

This suite drives `bigquery-adbc` through the
[`adbc` DuckDB community extension](https://duckdb.org/community_extensions/extensions/adbc)
([source](https://github.com/columnar-tech/duckdb-adbc-client)), so a third-party client controls
the ADBC calls.

| Extension surface | Driver surface it drives |
| --- | --- |
| `read_adbc(profile, sql)` | `StatementSetSqlQuery`, `StatementExecuteSchema`, `StatementExecuteQuery`, the Arrow stream |
| `ATTACH ... (TYPE adbc)` | `ConnectionGetOption("adbc.connection.catalog")`, `ConnectionGetObjects`, `ConnectionGetStatistics` |
| `INSERT` / `COPY` / `CREATE TABLE AS` on an attached table | Bulk ingest: `adbc.ingest.target_table`, `adbc.ingest.mode`, `StatementBindStream` |
| `CALL adbc_execute(profile, sql)` | Statements that are not scans: DDL, DML, scripts |
| Connection profiles | Every database-level `bigquery.*` option, and the `bigquery://` URI |
| `adbc_connection_pool_size` | Many independent `AdbcDatabase`/`AdbcConnection` pairs at once |

## Running

Needs the Rust toolchain, `uv`, ADC and `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` (required, no
default):

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients duckdb -- --ignored
PYTEST_ADDOPTS=tests/test_write.py cargo test --locked --test clients duckdb -- --ignored
PYTEST_ADDOPTS='-k catalog' cargo test --locked --test clients duckdb -- --ignored
uv sync --project integration/duckdb --locked  # refresh the environment alone
```

[`tests/clients/duckdb.rs`](../../tests/clients/duckdb.rs) builds the release `cdylib`, syncs the
locked Python 3.15 environment, downloads the community extension once, creates two datasets
(through [`tests/clients/datasets.rs`](../../tests/clients/datasets.rs), which drops only datasets
carrying this suite's owner label and run ID), runs pytest with `PYTEST_ADDOPTS`, and drops the
datasets, also on failure or interruption. DuckDB turns SIGINT into one failed query rather than
stopping pytest, so after Ctrl-C the test kills pytest within five seconds; Cargo returns at once
while the drop finishes. `PYTEST_ADDOPTS=--junitxml=results.xml` writes a JUnit report relative to
`integration/duckdb/`.

The offline tests need no credentials and run in CI through `scripts/run-offline-python-tests.sh`:

```sh
uv run --project integration/duckdb --locked \
    python -m pytest integration/duckdb/tests/test_sql.py integration/duckdb/tests/test_profiles.py
```

## How the driver is reached

The extension takes no driver path or option inline. Every database is `profile://<name>`, which
the driver manager resolves to `<name>.toml` on `ADBC_PROFILE_PATH`:

```toml
profile_version = 1
driver = "/abs/path/to/libadbc_driver_bigquery.so"

[Options]
uri = "bigquery:///my-test-project"
"bigquery.dataset_id" = "bigquery_adbc_duckdb_..._primary"
```

Each `[Options]` entry becomes an `AdbcDatabaseSetOption` call before `AdbcDatabaseInit`, so only
database-level options are reachable; connection and statement options (`bigquery.query.*`, the
ingest and job options) cannot be set through the extension. `{{ env_var(NAME) }}` interpolation
applies only to `[Options]`, not `driver`, so `tests/conftest.py`'s `make_profile` writes profiles
per test, and the suite builds the `cdylib` rather than a wheel. `sql.py` quotes for both dialects
in every statement: DuckDB outside, GoogleSQL inside the string literal.

| File | Role |
| --- | --- |
| `extension.py` | Opens DuckDB connections with the extension loaded. |
| `profiles.py` | Writes connection profiles. |
| `sql.py` | GoogleSQL and DuckDB quoting, and the `Session` helper. |
| `install_extension.py` | Downloads the community extension before the tests start. |
| `tests/` | The suite. |

## Extension behavior

The tests cover these behaviors of the current extension build for the pinned DuckDB release:

- `ATTACH` needs the backtick delimiter. Resolving an attached table runs
  `SELECT * FROM <schema>.<table>` with identifiers in the `ATTACH` delimiter pair, which defaults to
  double quotes; GoogleSQL reads those as a string literal, so every table appears not to exist.
  `sql.attach()` uses `ATTACH 'profile://bq' AS bq (TYPE adbc, DELIMITER '``');`.
- Errors during table resolution are swallowed: an auth failure, permission error or unsupported
  column type all report `Catalog Error: Table with name X does not exist!`.
- `SHOW TABLES` and `information_schema` over an attached catalog resolve every table in every
  schema, one remote metadata query each; the tests address tables by name instead.
- A failed table lookup triggers such a scan to build a "Did you mean ...?" suggestion, which can
  make it slow. `SET catalog_error_max_schemas = 0` turns the suggestion off and keeps the error;
  tests that look up a missing table set it.
- Attached-table scans always send `SELECT *`: there is no filter pushdown, and projection is
  pruned only after Arrow decoding. Use `read_adbc()` to push work into BigQuery.
- Only autocommit is supported. The extension requires `adbc.connection.autocommit` to read back
  `true` and never calls `AdbcConnectionCommit`; a DuckDB `ROLLBACK` does not undo a write that
  already reached BigQuery.
- Resolved tables are cached for the attachment's life, so a column added elsewhere stays invisible
  until `CALL adbc_clear_cache()`. A new table resolves on first use unless a whole-catalog scan
  already cached the table list.
- Writes ignore the catalog: the extension never sets `adbc.ingest.target_catalog`.

## Pinning

`.python-version` selects a Python 3.15 build with the GIL; DuckDB does not support free-threaded
Python. `duckdb` is pinned exactly in `pyproject.toml`, but `INSTALL adbc FROM community` takes the
current extension build for that release, so a new build can change results with no change here;
each run logs the build it used. Recheck the behaviors above when upgrading DuckDB:
`test_default_delimiter_resolves_no_table` and `test_missing_table_reports_a_catalog_error` depend
on them, so if either fails, check the extension for a behavior change before updating the test and
this guide.
