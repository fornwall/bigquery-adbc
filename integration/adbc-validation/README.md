# ADBC C++ validation suite

This harness, for contributors, runs Apache Arrow ADBC's C++ validation suite against the driver's
C ABI. The ADBC driver manager loads the Rust `cdylib` through `AdbcDriverBigqueryInit`, so the
suite exercises the ABI, the Arrow C Data and Stream interfaces, error ownership, and release
callbacks.

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

The test passes the library and datasets to the adapter in `ADBC_BIGQUERY_LIBRARY`,
`ADBC_BIGQUERY_DATASET`, and `ADBC_BIGQUERY_SECONDARY_DATASET`.

It enumerates the canonical suite and runs all 100 cases. A changed test count fails the
offline check so dependency updates require a coverage review.

Cases for capabilities BigQuery lacks self-skip through `DriverQuirks` hooks.
`SqlIngestUInt64`, which no hook gates, skips through a test-method override in the
fixture, as upstream's own driver tests do. `MetadataGetInfo` checks all five upstream subcases,
including the vendor client-library version, because returning no expectation for a code would
silently skip its assertions.

## Datasets and cleanup

Each GoogleTest case runs in a fresh, uniquely named dataset that is deleted right afterward. The
one metadata case that needs a second schema gets a second unique dataset. Datasets are created and
dropped through this driver with BigQuery DDL.

Dataset names start with `bigquery_adbc_validation_`, and the datasets carry identifying labels and
a one-day default table expiration. Cleanup refuses to drop a dataset without that prefix.
The test drops every dataset whose creation it started, including failed creation requests, when
a case fails or panics and after SIGINT or SIGTERM. After Ctrl-C, the test stops the running case
within five seconds, and Cargo returns to the prompt at once, while the drop finishes. Unique
datasets keep concurrent runs apart.

## Coverage

The suite has 93 passing cases and 7 skips. Two `DriverQuirks` hooks relax the assertions of
five passing cases, and three more hooks make six cases self-skip; `SqlIngestUInt64` is the
seventh skip. No failures are expected. Upstream's binary-view ingestion test omits empty binary
values.

`coverage_baseline.txt` records the eleven relaxed or skipped cases, with the responsible
mechanism and what it stops testing, in the same way as
`integration/foundry-validation/skip_baseline.txt`. `suite_matches_coverage_baseline` checks every
record offline against the enumerated suite and `bigquery_validation.cc` in both directions: a
relaxation cannot be added without a record, and a record cannot outlive its relaxation.

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
