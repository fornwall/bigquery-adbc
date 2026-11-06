# ADBC C++ validation suite

Apache Arrow ADBC's C++ suite loads the Rust `cdylib` through `AdbcDriverBigqueryInit`, exercising
the C ABI, Arrow C Data/Stream interfaces, error ownership and release callbacks.

## Requirements

- The Rust toolchain, CMake 3.20 or newer, a C++20 compiler and Git, on Linux or macOS.
- Google Cloud Application Default Credentials (ADC). The suite accepts no other credentials.
- `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` set to a project where the ADC principal can create and delete
  datasets, create, query and drop tables, read metadata, run jobs and transactions, and use the
  BigQuery Storage Write API.

## Running

```sh
# Required for live runs and offline drift checks; no project default.
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project

# Every test, isolated in its own dataset; failures are listed at the end
cargo test --locked --test clients suite_passes_against_bigquery -- --ignored

# Build, enumerate exactly 100 tests, and check the coverage baseline
# without ADC or BigQuery access. CI runs this on every push.
cargo test --locked --test clients suite_matches_coverage_baseline -- --ignored
```

[`tests/clients/adbc_validation.rs`](../../tests/clients/adbc_validation.rs) drives the suite.
Plain `cargo test` needs neither CMake nor BigQuery, and already compares the baseline with the
adapter source. Cargo reports a mistyped test name as success with zero tests; to reject that,
select one test with `BIGQUERY_ADBC_TEST_TARGET=clients scripts/run-one-live-test.sh <name>`.

The adapter receives `ADBC_BIGQUERY_LIBRARY`, `ADBC_BIGQUERY_DATASET` and
`ADBC_BIGQUERY_SECONDARY_DATASET`. A changed test count fails the offline check.

## Datasets and cleanup

Each case creates unique datasets through this driver's DDL, with identifying labels, one-day
table expiration and the required `bigquery_adbc_validation_` prefix. Cleanup drops every dataset
whose creation started, even after failed creation, test failure, panic, SIGINT or SIGTERM.
After Ctrl-C the running case stops within five seconds; deletion finishes after Cargo returns.

## Coverage

The suite expects 93 passes and 7 skips. `DriverQuirks` hooks relax five cases and skip six;
`SqlIngestUInt64` skips through a method override. `MetadataGetInfo` checks all five upstream
subcases, including vendor client-library version. Upstream's binary-view test omits empty values.

`coverage_baseline.txt` records each relaxation and skip. The offline check compares it with the
enumerated suite and `bigquery_validation.cc` in both directions, rejecting unrecorded relaxations
and obsolete records.

## Upstream revision and patch

`CMakeLists.txt` reads the `apache/arrow-adbc` revision from `../../Cargo.toml`, fails if the
manifest pins more than one, and reconfigures when the manifest changes, so the suite always
matches the driver's dependencies. It takes the driver and vendor versions that `MetadataGetInfo`
expects from the package version and the `google-cloud-bigquery` pin in `Cargo.lock` the same way.

The build applies `patches/validation-quirks.patch` to the fetched suite. It:

- adds hooks for temporal ingest values and for null lists stored as empty lists;
- gives the repeated temporal subcases inside one GTest distinct table names, because recreating
  one table repeatedly can exhaust the driver's visibility retries (UP-28 in
  [UPSTREAM.md](../../docs/UPSTREAM.md));
- scopes the table-name filter test to the configured catalog and schema, so an unrelated table
  matching its negative `asdf%` filter cannot fail it (UP-29,
  [apache/arrow-adbc#4916](https://github.com/apache/arrow-adbc/pull/4916)).

A separate Rust live test covers rapid table recreation without the patch's workaround:

```sh
scripts/run-one-live-test.sh bulk_ingests_after_rapid_table_recreation --nocapture
```

It ingests immediately after each acknowledged SQL drop, reusing one table name for 16 temporal
schemas, and checks row IDs, generations, nulls and values after every ingest. It neither retries
nor paces, so it can fail on the service race tracked in UP-26. The ordinary Rust live-suite
command in [CONTRIBUTING.md](../../CONTRIBUTING.md#live-tests) excludes it; selecting it explicitly
always runs it.

## Sanitizers

Set `ADBC_VALIDATION_SANITIZE` to build the C++ adapter, validation suite, and driver manager with
matching sanitizer flags:

```sh
ADBC_VALIDATION_SANITIZE=address,undefined \
  cargo test --locked --test clients suite_passes_against_bigquery -- --ignored
```

This catches allocator-visible errors at the C ABI boundary. The Rust `cdylib` is built normally,
so Rust code is not instrumented.
