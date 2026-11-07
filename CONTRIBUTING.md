# Contributing

Please open an issue before starting a large change. Use the
[issue form](https://github.com/fornwall/bigquery-adbc/issues/new/choose) for bugs, removing
credentials and private data. Report vulnerabilities through [SECURITY.md](SECURITY.md).

## Getting started

Install [Rust](https://rustup.rs/); `rust-toolchain.toml` selects the pinned toolchain on the first
build. Python changes also need [uv](https://docs.astral.sh/uv/). The offline test suite needs no
Google Cloud account or credentials:

```sh
git clone https://github.com/fornwall/bigquery-adbc
cd bigquery-adbc
cargo test --locked --all-features
```

[ARCHITECTURE.md](docs/ARCHITECTURE.md) explains how the code fits together, and
[`examples/`](examples) has a small program for each supported language.

## Building

`Cargo.toml` declares only an `rlib`. Build the shared library with release link-time optimization:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
```

The integration runners below build the library or a wheel themselves.

## Local checks

### GitHub Actions

After editing workflows, run [actionlint](https://github.com/rhysd/actionlint):

```sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

### Rust

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-features
```

Live tests stay ignored. Check both feature sets: developer features can hide shipped-build issues.

For documentation-only changes, check affected links and `git diff --check`. Report validation,
distinguishing offline tests from live BigQuery tests.

### Python

For Python changes, install [uv](https://docs.astral.sh/uv/) and run:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
```

See [Python development](python/DEVELOPMENT.md#local-tests) for individual suites and local builds.
The scripts include the [dbt runner's offline tests](tests/harness/test_dbt_runner.py).

After changing an option key, value, or Python exposure, regenerate the Python enums
instead of editing `python/src/bigquery_adbc/_options.py`:

```sh
cargo run --locked --features python-option-codegen --bin generate-python-options
```

### Unsafe Rust

Unsafe Rust is confined to `src/ffi/` in the library. Every unsafe function or trait, including
private helpers, needs a `# Safety` section stating the caller's or implementor's obligations.
Put a `// SAFETY:` comment immediately before each unsafe block or implementation explaining
why those obligations hold at that site: allocation and alignment, lifetime, aliasing, ownership,
and synchronization where relevant. Null checks alone do not establish pointer validity. Keep
unsafe blocks narrow; use safe operations when possible. Cargo and Clippy reject implicit unsafe
operations in unsafe functions, unnecessary unsafe blocks, undocumented unsafe blocks, and
missing safety sections; `clippy.toml` enables documentation checks on private items too.

## Fuzzing and Miri

After IPC or REST decoder changes, run the affected fuzz targets; after FFI changes, run Miri.
Both need nightly; fuzzing also needs [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz).
[CI](.github/workflows/ci.yml) runs these commands with a pinned nightly, AddressSanitizer for
fuzzing, and 30-minute job timeouts. Crashes are saved as `fuzz-artifacts`.

```sh
(cd fuzz && cargo +nightly fuzz run ipc_batch -- -max_total_time=60)
(cd fuzz && cargo +nightly fuzz run ipc_schema -- -max_total_time=60)
(cd fuzz && cargo +nightly fuzz run ipc_session -- -max_total_time=60)
(cd fuzz && cargo +nightly fuzz run ipc_lz4 -- -max_total_time=60)
(cd fuzz && cargo +nightly fuzz run rest_rows -- -max_total_time=60)
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" cargo +nightly miri test --locked --lib ffi:: -- \
  --skip ffi::roundtrip --skip a_connection_validating_its_catalog
```

| Target | Input |
| --- | --- |
| `ipc_batch` | A Storage Read record-batch message against a schema built from fuzzer bytes. |
| `ipc_schema` | The Arrow schema message `CreateReadSession` returns. |
| `ipc_session` | A schema message followed by a record batch, as in a read session. |
| `ipc_lz4` | One `LZ4_FRAME` buffer. It has its own target because frame checksums stop IPC mutations from reaching the block parser. |
| `rest_rows` | A REST result page decoded against a schema derived from the input: raw `rows` JSON, or a well-formed page of hostile cells. Besides panics, it fails on an invalid Arrow batch, a value outside its BigQuery type's range, or an error that echoes a cell. |

Fuzzing covers the network parsers, not memory supplied through the C Data Interface. Runs add
inputs to `fuzz/corpus/` as untracked files; commit only curated seeds. The Miri command skips the
driver-manager round trips, which run foreign code, and one socket-mock test. It needs
`-Zmiri-ignore-leaks` because the shared runtime's worker threads outlive the tests by design; that
flag also hides leaked allocations, so an FFI change that could leak needs a Rust test asserting
the release, as the bind tests assert that refused inputs are consumed. Run other tests under Miri
only as filtered suites, because socket mocks and the Tokio reactor can hang.

A hosted runner can be nearly four times slower than usual, so keep the Miri suite far below the job
timeout. A test whose cost adds no memory coverage shrinks under `cfg!(miri)` or is ignored there;
the native suite still runs it in full.

## Live tests

Live tests are ignored by default. They create cloud resources and run billable queries using
Application Default Credentials (ADC); configure these with
`gcloud auth application-default login`.

Every live integration suite and dbt benchmark requires `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT`.
Missing or empty values fail before cloud access; the active gcloud project is not a default.

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
```

The Rust suite needs access to the existing `sq_scratch` dataset and, for Cloud Storage cases,
`<project>-adbc-scratch` bucket ([defaults](tests/bigquery/support.rs)). Run its ADC-only cases:

```sh
cargo test --locked --all-features --test bigquery -- --ignored \
  --skip queries_with_discrete_user_oauth_credentials \
  --skip user_oauth_refresh_scope_vs_scopes \
  --skip queries_with_service_account_key_file_and_json_string \
  --skip storage_read_permission_denied_falls_back_to_rest \
  --skip numeric_project_identifiers_work_for_queries_metadata_and_copies \
  --skip bulk_ingests_after_rapid_table_recreation
```

The numeric-project case also needs the project's numeric ID:

```sh
BIGQUERY_ADBC_PROJECT_NUMBER="$(gcloud projects describe "$BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT" --format='value(projectNumber)')" \
  scripts/run-one-live-test.sh numeric_project_identifiers_work_for_queries_metadata_and_copies
```

The separate [UP-26 service-race reproducer](docs/UPSTREAM.md#up-26-storage-write-can-retain-a-stale-table-lookup-after-recreation)
can fail on a known BigQuery issue. Run it with
`scripts/run-one-live-test.sh bulk_ingests_after_rapid_table_recreation`.

The exported-driver live round trips are in the library suite:

```sh
cargo test --locked --all-features --lib ffi::roundtrip:: -- --ignored
```

Run one test with `scripts/run-one-live-test.sh <name>`; it fails if the name matches nothing.
The regional recovery test needs permission to create a disposable dataset in `asia-northeast1`:

```sh
scripts/run-one-live-test.sh regional_recovery_survives_lost_submission_responses
```

It drops real submission responses through a loopback proxy, then checks 409 and cross-region
`jobs.list` recovery, identical retries, exactly-once mutations, and remote cancellation. It
cancels jobs and deletes resources on failure too, and refuses emulator mode.

These runners cover other credential types:

| Script | Requirements |
| --- | --- |
| `scripts/run-oauth-live-test.sh` | Authorized-user ADC from `gcloud auth application-default login`. Checks the same identity through discrete OAuth, authorized-user JSON file/string, and a temporary access token. |
| `scripts/run-oauth-live-test.sh --scopes --nocapture` | Authorized-user ADC granting `cloud-platform` and at least one other scope. Compares omitted `scope`, singular `scope`, and plural `scopes` in JSON refresh requests to Google's OAuth endpoint. Prints only returned scope names; creates no BigQuery resources. |
| `cargo +nightly -Zscript scripts/check-oauth-scope-narrowing.rs` | The same ADC grant. Standalone Cargo script with an embedded manifest pinning `google-cloud-auth` to an upstream commit. Compares the direct refresh requests with the SDK token's scopes from Google's `tokeninfo` endpoint and verifies that the SDK restricts the token to the requested subset. The file can be copied and run outside this repository. |
| `scripts/run-service-account-live-test.sh` | A service-account key at `BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE` (default `~/.config/gcloud/bigquery-adbc-integration-key.json`) for an account with `roles/bigquery.jobUser`. Tests file and inline credentials. If the key is missing, prints how to create one for `BIGQUERY_ADBC_SERVICE_ACCOUNT`. |
| `scripts/run-storage-read-permission-fallback-live-test.sh` | Permission to impersonate `BIGQUERY_ADBC_STORAGE_READ_DENIED_SERVICE_ACCOUNT`, an account that can run queries but lacks `bigquery.readsessions.create`. |

The account names default to `bigquery-adbc-integration` and `bigquery-adbc-no-readsession`
in the selected project; set the account variables to use different service accounts.
Client integration suites have their own guides: [Python](python/DEVELOPMENT.md#local-tests),
[ADBC C++ validation](integration/adbc-validation/README.md),
[Driver Foundry](integration/foundry-validation/README.md), [dbt](integration/dbt/README.md), and
[DuckDB](integration/duckdb/README.md). `cargo test --locked --test clients -- --ignored` runs the
ADBC, dbt, Foundry and DuckDB suites concurrently, which takes about an hour; pass a test name, such
as `duckdb`, to run one. `PYTEST_ADDOPTS` passes options to the pytest suites.

## BigQuery emulator

The Rust and Python DBAPI suites can use a local emulator; BigQuery remains the default. Run six
smoke tests for queries, dry runs, nested results, parameters, Storage Read and Storage Write:

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-emulator-project
export ADBC_BIGQUERY_EMULATOR_BIN=/path/to/emulator
scripts/run-emulator-tests.sh
```

The runner starts `ADBC_BIGQUERY_EMULATOR_BIN` on ephemeral loopback ports and stops it afterward.
It seeds `sq_scratch` in `US` and a one-row `bigquery-public-data.samples.shakespeare` fixture.

The executable must accept
`serve --host 127.0.0.1 --port 0 --grpc-port 0 --project <id> --log-requests`, with `--project`
repeated, and print `REST listening on http://127.0.0.1:<port>` then
`Storage gRPC listening on 127.0.0.1:<port>` on stdout. Use a launcher script to adapt another
command line or add emulator options. Python 3.11 or newer is required. Pass a command after `--`
to run a different selection from this repository's root:

```sh
scripts/run-emulator-tests.sh -- scripts/run-one-live-test.sh bulk_ingests_range_values
scripts/run-emulator-tests.sh -- scripts/run-python-tests.sh -m bigquery -q
scripts/run-emulator-tests.sh -- cargo test --locked --test bigquery parameters:: -- --ignored
```

To use an already running emulator, keep the project variable set and set both endpoints explicitly:

```sh
ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT=http://127.0.0.1:9050 \
ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT=http://127.0.0.1:9060 \
scripts/run-one-live-test.sh executes_scalar_query_and_dry_run_schema
```

Both must be HTTP(S) origins without paths or credentials. Invalid or partial configuration fails.
The test helpers use anonymous authentication and override every service endpoint, including
cleanup. Applications use the driver's endpoint options instead. C++, Foundry, DuckDB and dbt
runners, plus credential, IAM and Cloud Storage tests, still require Google Cloud. Other tests
retain their assertions and may fail on [emulator gaps](docs/LIMITATIONS.md#emulator-coverage).
Emulators do not validate IAM, quotas or billing.

## Changes and review

Add a regression test for each behavior fix. For performance and size changes, include
measurements and a reproducible command in [PERFORMANCE.md](docs/PERFORMANCE.md) or
[BINARY-SIZE.md](docs/BINARY-SIZE.md). Error messages must never echo credentials or row contents.
Only GoogleSQL is supported; bulk ingestion uses the Storage Write API. API compatibility is
not guaranteed while the project is experimental.

The active [Go driver](https://github.com/adbc-drivers/bigquery/tree/main/go) is the reference
implementation. Document behavior in [REFERENCE.md](docs/REFERENCE.md), differences from Go in
[GO-DRIVER-COMPARISON.md](docs/GO-DRIVER-COMPARISON.md), dbt-labs fork behavior in
[DBT-CORE.md](docs/DBT-CORE.md), dependency issues in [UPSTREAM.md](docs/UPSTREAM.md), and open
limitations in [LIMITATIONS.md](docs/LIMITATIONS.md).

## Distribution packaging

Build native release archives with the same license bundle as the Python wheels:

```sh
uv run --project python --locked python tools/package_native.py
```

This builds the locked release cdylib, checks its exports, and writes a platform-named archive in
`dist/` (`.tar.gz` on Linux/macOS, `.zip` on Windows). It includes the library, driver manifest,
`LICENSE`, `NOTICE`, dependency texts, Rust standard-library notices and source inventory.

[`tools/dbt-build`](tools/dbt-build/README.md) builds dbt patched to use this driver.
`--output-dir` changes the destination. `--library /absolute/path/to/library` or `--wheel path/to/wheel`
packages an existing library for this platform, using the manifest's expected filename. Like the
wheel's `BIGQUERY_ADBC_BUILD_LIBRARY` override, these inputs must use this checkout's locked
dependencies and pinned Rust toolchain.

After changing `Cargo.toml`, `Cargo.lock`, or the pinned Rust version, regenerate the checked-in
bundle. A change to this crate's own version alone needs no regeneration:

```sh
python3 tools/generate_licenses.py
python3 tools/generate_licenses.py --check
```

Generation uses locked Cargo metadata for all five release targets and includes normal and build
dependencies, nested notices and source copyright headers, deduplicated by hash. This includes
code that may not survive linking. The pinned compiler supplies `COPYRIGHT-library.html`;
Cargo may download dependency sources. Packaging checks the saved hashes offline. Source
distributions include the bundle and generator.

Review new dependencies' licenses. Missing texts fail generation: add a reviewed copy to
`tools/license-sources/` with its upstream revision and URL in `sources.json`. Supplements are
checked against Cargo VCS metadata; CI regenerates the bundle and builds reject stale or damaged
copies. These checks preserve notices but do not replace license review.

## Releasing

Pushing a `vX.Y.Z` tag runs the [wheel workflow](.github/workflows/wheels.yml). Once CI and every
platform build have passed on the tagged commit, it creates a GitHub release and then publishes the
wheels and source distribution to PyPI. CI builds and tests a wheel from the same source archive
that gets published. The tag must match the `version` in `Cargo.toml`, the package's single version
source. Move the [changelog](CHANGELOG.md)'s unreleased entries under the new version first:

```sh
git tag vX.Y.Z
git push origin vX.Y.Z
```

The GitHub release carries a [native archive](#distribution-packaging) for each platform, built from
the library in that platform's tested wheel, and a `SHA256SUMS` file. Its notes are the version's
changelog section, so the workflow stops before publishing anything if that section is missing, and
links in changelog entries must be absolute.

The release also carries each platform's compressed library, which release builds of
[patched dbt](tools/dbt-build/README.md) download, and the `install.sh` that installs and updates
them, from the tagged commit. Those builds hash the published file, so they
start after the release is created and attach their archives and a `dbt-SHA256SUMS` file about an
hour later. PyPI does not wait for them.

PyPI never accepts a version twice, so a bad release needs a new version; re-running the workflow
only finishes an interrupted upload. Publishing uses
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) from the `pypi` GitHub environment,
which only `v*` tags can deploy to; no token is stored. Other tag forms, including pre-releases, do
not publish.
