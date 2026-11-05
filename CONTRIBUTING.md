# Contributing

Thanks for taking a look at bigquery-adbc. Bug reports, fixes and improvements are welcome. The
project is experimental and has a single maintainer, so please open an issue before starting
anything large.

For bug reports, use the [issue form](https://github.com/fornwall/bigquery-adbc/issues/new/choose),
and remove credentials and private data. Report vulnerabilities privately, as described in
[SECURITY.md](SECURITY.md).

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

The Rust toolchain is pinned in [rust-toolchain.toml](rust-toolchain.toml). `Cargo.toml` declares
only the `rlib` that tests, benchmarks and fuzzing use. Build the shared library separately so
the release profile's link-time optimization applies:

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

From the repository root:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-features
```

These need no BigQuery credentials; live tests stay ignored. Clippy runs with default features too,
because developer features can hide problems in the shipped build.

For documentation-only changes, check affected links and run `git diff --check`; code tests are
unnecessary. In change descriptions, state which checks ran and distinguish unit or mock tests
from tests against BigQuery.

### Python

For Python changes, install [uv](https://docs.astral.sh/uv/) and run:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
```

The [Python development guide](python/DEVELOPMENT.md#local-tests) covers individual suites and
loading a local driver build. Offline unit tests for the dbt runner live in
[`tests/harness/test_dbt_runner.py`](tests/harness/test_dbt_runner.py); the dbt integration project
and live runner are in [`integration/dbt/`](integration/dbt/README.md).

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

After changing Arrow IPC parsing or REST row decoding, run the affected fuzz targets; after
changing the FFI layer, run its tests under Miri. Both need a nightly toolchain, and fuzzing also
needs [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz). On every pull request and push to
`main`, CI runs the filtered Miri suite below and each fuzz target for 60 seconds with AddressSanitizer.
The two jobs run in parallel with 30-minute timeouts and a nightly pinned in
[ci.yml](.github/workflows/ci.yml); CI saves crash inputs as the `fuzz-artifacts` artifact.

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

## Live tests

Live tests are ignored by default. They create cloud resources and run billable queries using
Application Default Credentials (ADC); configure these with
`gcloud auth application-default login`.

Every integration suite, including Rust, Python, C++ validation, Foundry, DuckDB, dbt and
its benchmarks, requires `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT`. There is no project
default: missing or empty values fail before cloud access, and the active gcloud project does not
select the test project.

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
```

The Rust suite uses the existing `sq_scratch` dataset in that project and, for Cloud Storage
cases, its `<project>-adbc-scratch` bucket. These names are defined in
[tests/bigquery/support.rs](tests/bigquery/support.rs). Grant your credentials access to those
resources, then run:

```sh
cargo test --locked --all-features --test bigquery -- --ignored \
  --skip queries_with_discrete_user_oauth_credentials \
  --skip user_oauth_refresh_scope_vs_scopes \
  --skip queries_with_service_account_key_file_and_json_string \
  --skip storage_read_permission_denied_falls_back_to_rest \
  --skip numeric_project_identifiers_work_for_queries_metadata_and_copies \
  --skip bulk_ingests_after_rapid_table_recreation
```

This runs the ADC-only integration cases. Run the credential-specific cases with the scripts
below. The numeric-project case also needs the project's numeric ID:

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

The Rust `tests/bigquery/` and Python DBAPI suites can opt into a local BigQuery emulator. Real
BigQuery remains the default.
Start a fresh emulator and run six existing smoke tests covering queries, dry runs, nested
results, parameters, Storage Read and Storage Write:

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-emulator-project
export ADBC_BIGQUERY_EMULATOR_BIN=/path/to/emulator
scripts/run-emulator-tests.sh
```

The runner starts the required `ADBC_BIGQUERY_EMULATOR_BIN` executable on ephemeral loopback ports
and stops it when tests finish. It creates `sq_scratch` in the explicitly selected project in `US`
and a synthetic one-row `bigquery-public-data.samples.shakespeare` table for catalog-switching
tests.

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

Both variables must be absolute `http://` or `https://` origins, without paths or credentials.
The test helpers select anonymous authentication and override all service endpoints, including
cleanup connections. A missing or invalid endpoint fails configuration. These variables affect
only the Rust and Python test suites; applications use the driver's endpoint options. The C++,
Foundry, DuckDB and dbt integration runners still require Google Cloud.

Other tests can fail on [emulator gaps or missing fixtures](docs/LIMITATIONS.md#emulator-coverage);
they retain their BigQuery assertions. Credential, IAM and Cloud Storage tests refuse emulator
mode. Emulator tests do not validate Google Cloud IAM, quotas or billing behavior.

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

The command builds the release cdylib with locked dependencies, checks its ADBC exports, and
writes a platform-named archive in `dist/` (`.tar.gz` on Linux/macOS, `.zip` on Windows).
The archive contains the library, `LICENSE`, `NOTICE`, and `licenses/` with the dependency
texts, Rust standard-library notices, and a source inventory. `--output-dir` selects another
directory. `--library /absolute/path/to/library` packages an existing library, and
`--wheel path/to/wheel` packages the library inside a wheel for this platform; either must have been
built from this checkout's locked dependencies and pinned Rust toolchain. The Python wheel's
`BIGQUERY_ADBC_BUILD_LIBRARY` override has the same provenance requirement.

After changing `Cargo.toml`, `Cargo.lock`, or the pinned Rust version, regenerate the checked-in
bundle. A change to this crate's own version alone needs no regeneration:

```sh
python3 tools/generate_licenses.py
python3 tools/generate_licenses.py --check
```

Generation uses locked Cargo metadata for the five release targets, traversing normal and build
dependencies, including nested license/notice files and leading source copyright notices.
It conservatively includes build dependencies and platform-specific components; it is not a list
of code proven to survive linking. Identical texts are stored once and referenced by hash.
The compiler's `COPYRIGHT-library.html` supplies the Rust standard-library notices. Generation
needs the pinned compiler and dependency sources (Cargo may download them); packaging validates
the checked-in input and content hashes without running the generator or accessing the network.
Source distributions include the bundle and regeneration tools.

Review licensing when adding dependencies. A missing license text fails generation: add a
reviewed copy under `tools/license-sources/` and record its exact upstream revision and URL in
`sources.json` when the published crate omits it. Existing supplements are checked against the
crate's Cargo VCS metadata. CI regenerates and compares the bundle, and package builds reject
stale or damaged copies. These checks preserve collected notices; they do not establish rights
to new source material or replace review of unusual license terms.

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

PyPI never accepts a version twice, so a bad release needs a new version; re-running the workflow
only finishes an interrupted upload. Publishing uses
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) from the `pypi` GitHub environment,
which only `v*` tags can deploy to; no token is stored. Other tag forms, including pre-releases, do
not publish.
