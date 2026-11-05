# DuckDB ADBC extension integration

This suite drives `bigquery-adbc` through the
[`adbc` DuckDB community extension](https://duckdb.org/community_extensions/extensions/adbc)
([source](https://github.com/columnar-tech/duckdb-adbc-client)), so a third-party client controls
the ADBC calls. It is for contributors checking the driver against that client.

| Extension surface | Driver surface it drives |
| --- | --- |
| `read_adbc(profile, sql)` | `StatementSetSqlQuery`, `StatementExecuteSchema`, `StatementExecuteQuery`, the Arrow stream |
| `ATTACH ... (TYPE adbc)` | `ConnectionGetOption("adbc.connection.catalog")`, `ConnectionGetObjects`, `ConnectionGetStatistics` |
| `INSERT` / `COPY` / `CREATE TABLE AS` on an attached table | Bulk ingest: `adbc.ingest.target_table`, `adbc.ingest.mode`, `StatementBindStream` |
| `CALL adbc_execute(profile, sql)` | Statements that are not scans: DDL, DML, scripts |
| Connection profiles | Every database-level `bigquery.*` option, and the `bigquery://` URI |
| `adbc_connection_pool_size` | Many independent `AdbcDatabase`/`AdbcConnection` pairs at once |

## Running

Install the Rust toolchain and `uv`.

Set `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` to a project your ADC principal can use.
It is required; missing or empty values fail, with no default.

```sh
gcloud auth application-default login
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
cargo test --locked --test clients duckdb -- --ignored
PYTEST_ADDOPTS=tests/test_write.py cargo test --locked --test clients duckdb -- --ignored
PYTEST_ADDOPTS='-k catalog' cargo test --locked --test clients duckdb -- --ignored
```

[`tests/clients/duckdb.rs`](../../tests/clients/duckdb.rs) drives the suite, and pytest reads its
options from `PYTEST_ADDOPTS`. The test builds the release `cdylib`, syncs the locked Python 3.15
environment, downloads the community extension once, creates two BigQuery datasets, runs pytest,
and drops the datasets, also when a test fails or the run is interrupted. DuckDB turns SIGINT into
one failed query rather than stopping pytest, so after Ctrl-C the test kills pytest within five
seconds. Cargo returns to the prompt at once, while the drop finishes.

To refresh the environment on its own:

```sh
uv sync --project integration/duckdb --locked
```

`PYTEST_ADDOPTS=--junitxml=results.xml` writes a JUnit report; relative paths are resolved from
`integration/duckdb/`.

## How the driver is reached

The extension takes no driver path or driver option inline. Every database is addressed as
`profile://<name>`, which the ADBC driver manager resolves to `<name>.toml` on
`ADBC_PROFILE_PATH`:

```toml
profile_version = 1
driver = "/abs/path/to/libadbc_driver_bigquery.so"

[Options]
uri = "bigquery:///my-test-project"
"bigquery.dataset_id" = "bigquery_adbc_duckdb_..._primary"
```

Each `[Options]` entry becomes an `AdbcDatabaseSetOption` call before `AdbcDatabaseInit`. Profiles
are the suite's only way to set driver options, and they reach only database-level options;
connection and statement options (`bigquery.query.*`, the ingest and job options) cannot be set
through the extension.

Profiles are generated per test, because the `driver` key is copied verbatim: the format's
`{{ env_var(NAME) }}` interpolation applies only to `[Options]`, so the path of the freshly built
library cannot be written statically. For the same reason this suite builds the `cdylib` rather
than a wheel, unlike the [Foundry suite](../foundry-validation/README.md).

`tests/conftest.py` provides the `make_profile` factory. `sql.py` holds quoting helpers for the two
dialects in every statement: DuckDB outside, GoogleSQL inside the string literal.

## Extension behavior

The tests cover these behaviors of the pinned extension version:

- `ATTACH` needs the backtick delimiter. Resolving an attached table runs
  `SELECT * FROM <schema>.<table>` with the identifiers wrapped in the `ATTACH` delimiter pair,
  which defaults to double quotes. GoogleSQL reads those as a string literal, so with the default
  every table appears not to exist. `sql.attach()` passes the backtick pair:

  ```sql
  ATTACH 'profile://bq' AS bq (TYPE adbc, DELIMITER '``');
  ```

- Errors during table resolution are swallowed. The extension reports DuckDB's
  `Catalog Error: Table with name X does not exist!` for every error there, so an auth failure, a
  permission error or an unsupported column type all look like a missing table.
- `SHOW TABLES` and `information_schema` over an attached catalog resolve every table in every
  schema, one remote metadata query each. The tests address tables by name instead.
- A failed table lookup triggers such a scan, because DuckDB builds a "Did you mean ...?"
  suggestion from every table in every dataset, which can make a simple lookup failure slow.
  `SET catalog_error_max_schemas = 0` turns the suggestion off and keeps the error; tests that
  deliberately look up a missing table set it.
- Attached-table scans read whole tables. There is no filter pushdown, and projection is pruned
  only after Arrow decoding, so the query sent is always `SELECT *`. Use `read_adbc()` to push work
  into BigQuery.
- Only autocommit is supported. The extension requires `adbc.connection.autocommit` to read back
  `true` and never calls `AdbcConnectionCommit`; a DuckDB `ROLLBACK` does not undo a write that
  already reached BigQuery.
- Resolved tables are cached for the life of the attachment, so a column added outside it stays
  invisible until `CALL adbc_clear_cache()`. A new table resolves on first use unless a
  whole-catalog scan has already cached the table list.
- Writes ignore the catalog. The extension sets the ingest target schema and table but never
  `adbc.ingest.target_catalog`.

## Layout

```
extension.py        opens DuckDB connections with the extension loaded
profiles.py         writes connection profiles
sql.py              GoogleSQL and DuckDB quoting, and the Session helper
install_extension.py downloads the community extension before the tests start
tests/              the suite itself
```

The run's two datasets are created and dropped by
[`tests/clients/datasets.rs`](../../tests/clients/datasets.rs), which drops only a dataset that
carries this suite's owner label and this run's ID.

## Offline tests

`tests/test_sql.py` and `tests/test_profiles.py` need no credentials and run in CI through
`scripts/run-offline-python-tests.sh`:

```sh
uv run --project integration/duckdb --locked \
    python -m pytest integration/duckdb/tests/test_sql.py integration/duckdb/tests/test_profiles.py
```

## Pinning

`.python-version` selects a Python 3.15 build with the GIL, because DuckDB does not support
free-threaded Python.

`duckdb` is pinned exactly in `pyproject.toml`. The community extension is published per DuckDB
version, so that pin also fixes the extension build under test; the suite loads the extension only
with `INSTALL adbc FROM community`.

Recheck the behaviors above when upgrading DuckDB. `test_default_delimiter_resolves_no_table` and
`test_missing_table_reports_a_catalog_error` depend on the current delimiter and missing-table
behavior. If either fails after an upgrade, check the extension for a behavior change before
updating the test and this guide.
