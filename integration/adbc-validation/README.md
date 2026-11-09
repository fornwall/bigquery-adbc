# ADBC C++ validation suite

Apache Arrow ADBC's C++ suite loads the Rust `cdylib` through `AdbcDriverBigqueryInit`, exercising
the C ABI, Arrow C Data/Stream interfaces, error ownership and release callbacks.

Requirements: Linux or macOS with the Rust toolchain, CMake 3.20 or newer, a C++20 compiler and Git;
Application Default Credentials (no other credentials are accepted); and
`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` naming a project where the ADC principal can create
and delete datasets, create, query and drop tables, read metadata, run jobs and transactions, and
use the Storage Write API.

## Running

```sh
# Required for live runs and offline drift checks; no project default.
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project

# Every test, isolated in its own dataset; failures are listed at the end.
cargo test --locked --test clients adbc_validation::suite_passes_against_bigquery -- --ignored

# Without ADC or BigQuery: build, enumerate exactly 100 tests and check the coverage baseline.
# CI runs this on every push.
cargo test --locked --test clients suite_matches_coverage_baseline -- --ignored
```

[`tests/clients/adbc_validation.rs`](../../tests/clients/adbc_validation.rs) drives the suite and
passes the adapter `ADBC_BIGQUERY_LIBRARY`, `ADBC_BIGQUERY_DATASET` and
`ADBC_BIGQUERY_SECONDARY_DATASET`. Plain `cargo test` needs neither CMake nor BigQuery and already
compares the baseline with the adapter source. Cargo reports a mistyped test name as success; to
reject that, use `BIGQUERY_ADBC_TEST_TARGET=clients scripts/run-one-live-test.sh <name>`.

Each case creates unique datasets through this driver's DDL, with identifying labels, one-day table
expiration and the required `bigquery_adbc_validation_` prefix. Cleanup drops every dataset whose
creation started, even after failed creation, test failure, panic, SIGINT or SIGTERM; after Ctrl-C
the running case stops within five seconds and deletion finishes after Cargo returns.

## Coverage

The suite expects 93 passes and 7 skips. `DriverQuirks` hooks relax five cases and skip six;
`SqlIngestUInt64` skips through a method override. `MetadataGetInfo` checks all five upstream
subcases, including vendor client-library version. Upstream's binary-view test omits empty values.

`coverage_baseline.txt` records each relaxation and skip. The offline check compares it with the
enumerated suite and `bigquery_validation.cc` in both directions, rejecting unrecorded relaxations,
obsolete records and a changed test count.

## Upstream revision and patch

`CMakeLists.txt` reads the single `apache/arrow-adbc` revision pinned in `../../Cargo.toml` (more
than one fails) and reconfigures when the manifest changes; the driver and vendor versions that
`MetadataGetInfo` expects come from the package version and the `google-cloud-bigquery` pin in
`Cargo.lock`. The build applies `patches/validation-quirks.patch`, which:

- adds hooks for temporal ingest values and for null lists stored as empty lists;
- gives repeated temporal subcases in one GTest distinct table names, because recreating one table
  repeatedly can exhaust the driver's visibility retries
  ([UP-28](../../docs/UPSTREAM.md#up-28-adbc-temporal-ingest-subcases-reuse-one-physical-table));
- scopes the table-name filter test to the configured catalog and schema, so an unrelated table
  matching its negative `asdf%` filter cannot fail it (UP-29,
  [apache/arrow-adbc#4916](https://github.com/apache/arrow-adbc/pull/4916)).

The Rust live test `bulk_ingests_after_rapid_table_recreation` covers rapid recreation without that
workaround: it reuses one table name for 16 temporal schemas, ingesting right after each drop with
no retries or pacing, so it can fail on the UP-26 service race and the
[Rust live-suite command](../../CONTRIBUTING.md#live-tests) skips it.

## Sanitizers

`ADBC_VALIDATION_SANITIZE` builds the C++ adapter, validation suite and driver manager with matching
sanitizer flags, catching allocator-visible errors at the C ABI boundary. The Rust `cdylib` is
built normally, so Rust code is not instrumented.

```sh
ADBC_VALIDATION_SANITIZE=address,undefined \
  cargo test --locked --test clients adbc_validation::suite_passes_against_bigquery -- --ignored
```
