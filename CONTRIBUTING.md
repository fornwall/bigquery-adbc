# Contributing

Open an issue before starting a large change. Report bugs through the
[issue form](https://github.com/fornwall/bigquery-adbc/issues/new/choose), with credentials and
private data removed, and vulnerabilities through [SECURITY.md](SECURITY.md).

## Getting started

Install [Rust](https://rustup.rs/) (`rust-toolchain.toml` pins the toolchain) and, for Python
changes, [uv](https://docs.astral.sh/uv/). The offline suite needs no Google Cloud account:

```sh
git clone https://github.com/fornwall/bigquery-adbc
cd bigquery-adbc
cargo test --locked --all-features
```

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) and [`examples/`](examples). `Cargo.toml` declares only
an `rlib`; build the shared library with
`cargo rustc --release --locked --lib --crate-type cdylib`. Integration runners build the library or
a wheel themselves.

## Local checks

Rust, with both feature sets (developer features can hide shipped-build issues); live tests stay
ignored:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-features
```

Python, including the [dbt runner's offline tests](tests/harness/test_dbt_runner.py); see
[Python development](python/DEVELOPMENT.md#local-tests) for individual suites:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
```

Workflows, with [actionlint](https://github.com/rhysd/actionlint):

```sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

Documentation-only changes need link checks and `git diff --check`. Report validation,
distinguishing offline tests from live BigQuery tests.

The Python option enums in `python/src/bigquery_adbc/_options.py` are generated, and `cargo test`
checks that they are current. After changing an option key, value or Python exposure, regenerate
them instead of editing the file:

```sh
cargo run --locked --features python-option-codegen --bin generate-python-options
```

### Unsafe Rust

Unsafe Rust is confined to `src/ffi/`. Every unsafe function or trait, including private helpers,
needs a `# Safety` section stating the caller's or implementor's obligations, and each unsafe block
or implementation a preceding `// SAFETY:` comment explaining why they hold there (allocation and
alignment, lifetime, aliasing, ownership, synchronization). Null checks alone do not establish
pointer validity. Keep unsafe blocks narrow. Cargo and Clippy enforce these rules, on private items
too (`clippy.toml`).

## Fuzzing and Miri

After IPC or REST decoder changes, run the affected fuzz targets; after FFI changes, run Miri. Both
need nightly; fuzzing also needs [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz).
[CI](.github/workflows/ci.yml) runs them with a pinned nightly, AddressSanitizer for fuzzing, and
30-minute job timeouts, saving crashes as `fuzz-artifacts`.

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
| `ipc_lz4` | One `LZ4_FRAME` buffer; separate because frame checksums stop IPC mutations from reaching the block parser. |
| `rest_rows` | A REST result page (raw `rows` JSON, or a well-formed page of hostile cells) against a schema derived from the input. Fails on panics, invalid Arrow batches, values outside their BigQuery type's range, and errors that echo a cell. |

Fuzzing covers the network parsers, not memory supplied through the C Data Interface. Runs add
untracked inputs to `fuzz/corpus/`; commit only curated seeds.

The Miri command skips the driver-manager round trips (foreign code) and one socket-mock test.
`-Zmiri-ignore-leaks` is needed because the shared runtime's worker threads outlive the tests; it
also hides leaked allocations, so an FFI change that could leak needs a Rust test asserting the
release (as the bind tests assert that refused inputs are consumed). Run other tests under Miri only
as filtered suites: socket mocks and the Tokio reactor can hang. Hosted runners can be nearly four
times slower than usual, so keep the Miri suite far below the job timeout; tests whose cost adds no
memory coverage shrink under `cfg!(miri)` or are ignored there.

## Live tests

Live tests are ignored by default. They create cloud resources and run billable queries using
Application Default Credentials (`gcloud auth application-default login`). Every live suite and dbt
benchmark requires `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT`; missing or empty values fail before
cloud access, and the active gcloud project is not a default.

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-test-project
```

The Rust suite needs the existing `sq_scratch` dataset and, for Cloud Storage cases, the
`<project>-adbc-scratch` bucket ([defaults](tests/bigquery/support.rs)). Its ADC-only cases:

```sh
cargo test --locked --all-features --test bigquery -- --ignored \
  --skip queries_with_discrete_user_oauth_credentials \
  --skip user_oauth_refresh_scope_vs_scopes \
  --skip queries_with_service_account_key_file_and_json_string \
  --skip storage_read_permission_denied_falls_back_to_rest \
  --skip numeric_project_identifiers_work_for_queries_metadata_and_copies \
  --skip bulk_ingests_after_rapid_table_recreation
cargo test --locked --all-features --lib ffi::roundtrip:: -- --ignored  # exported-driver round trips
```

Run one test with `scripts/run-one-live-test.sh <name>`; it fails if the name matches nothing.
Separately run cases:

```sh
# Needs the project's numeric ID.
BIGQUERY_ADBC_PROJECT_NUMBER="$(gcloud projects describe "$BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT" --format='value(projectNumber)')" \
  scripts/run-one-live-test.sh numeric_project_identifiers_work_for_queries_metadata_and_copies
# Can fail on a known BigQuery issue (UP-26).
scripts/run-one-live-test.sh bulk_ingests_after_rapid_table_recreation
# Needs permission to create a disposable dataset in asia-northeast1; refuses emulator mode.
scripts/run-one-live-test.sh regional_recovery_survives_lost_submission_responses
```

The [UP-26 reproducer](docs/UPSTREAM.md#up-26-storage-write-can-retain-a-stale-table-lookup-after-recreation)
is a service race. The regional recovery test drops real submission responses through a loopback
proxy, then checks 409 and cross-region `jobs.list` recovery, identical retries, exactly-once
mutations and remote cancellation; it cancels jobs and deletes resources on failure too.

Other credential types:

| Script | Requirements |
| --- | --- |
| `scripts/run-oauth-live-test.sh` | Authorized-user ADC. Checks the same identity through discrete OAuth, authorized-user JSON file/string, and a temporary access token. |
| `scripts/run-oauth-live-test.sh --scopes --nocapture` | Authorized-user ADC granting `cloud-platform` and at least one other scope. Compares omitted `scope`, singular `scope` and plural `scopes` in JSON refresh requests to Google's OAuth endpoint; prints only returned scope names and creates no BigQuery resources. |
| `cargo +nightly -Zscript scripts/check-oauth-scope-narrowing.rs` | The same ADC grant. Standalone Cargo script (embedded manifest pins `google-cloud-auth` to an upstream commit; runs outside this repository too) that compares direct refresh requests with the SDK token's scopes from Google's `tokeninfo` endpoint and verifies the SDK restricts the token to the requested subset. |
| `scripts/run-service-account-live-test.sh` | A service-account key at `BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE` (default `~/.config/gcloud/bigquery-adbc-integration-key.json`) for an account with `roles/bigquery.jobUser`. Tests file and inline credentials; if the key is missing, prints how to create one for `BIGQUERY_ADBC_SERVICE_ACCOUNT`. |
| `scripts/run-storage-read-permission-fallback-live-test.sh` | Permission to impersonate `BIGQUERY_ADBC_STORAGE_READ_DENIED_SERVICE_ACCOUNT`, an account that can run queries but lacks `bigquery.readsessions.create`. |

The account variables default to `bigquery-adbc-integration` and `bigquery-adbc-no-readsession` in
the selected project.

Client suites have their own guides: [Python](python/DEVELOPMENT.md#local-tests),
[ADBC C++ validation](integration/adbc-validation/README.md),
[Driver Foundry](integration/foundry-validation/README.md), [dbt](integration/dbt/README.md) and
[DuckDB](integration/duckdb/README.md). `cargo test --locked --test clients -- --ignored` runs the
ADBC, dbt, Foundry and DuckDB suites concurrently (about an hour); pass a name such as `duckdb` to
run one. `PYTEST_ADDOPTS` passes options to the pytest suites.

## BigQuery emulator

The Rust and Python DB-API suites can use a local emulator (Python 3.11 or newer); BigQuery remains
the default. Run six smoke tests (queries, dry runs, nested results, parameters, Storage Read and
Storage Write), or pass another command after `--`:

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-emulator-project
export ADBC_BIGQUERY_EMULATOR_BIN=/path/to/emulator
scripts/run-emulator-tests.sh
scripts/run-emulator-tests.sh -- scripts/run-one-live-test.sh bulk_ingests_range_values
scripts/run-emulator-tests.sh -- scripts/run-python-tests.sh -m bigquery -q
scripts/run-emulator-tests.sh -- cargo test --locked --test bigquery parameters:: -- --ignored
```

The runner starts the emulator on ephemeral loopback ports, seeds `sq_scratch` in `US` and a one-row
`bigquery-public-data.samples.shakespeare` fixture, and stops it afterward. The executable must accept
`serve --host 127.0.0.1 --port 0 --grpc-port 0 --project <id> --log-requests` (with `--project`
repeated) and print `REST listening on http://127.0.0.1:<port>` then
`Storage gRPC listening on 127.0.0.1:<port>` on stdout; use a launcher script to adapt another
command line.

To use an already running emulator, set both endpoints (HTTP(S) origins without paths or
credentials; invalid or partial configuration fails):

```sh
ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT=http://127.0.0.1:9050 \
ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT=http://127.0.0.1:9060 \
scripts/run-one-live-test.sh executes_scalar_query_and_dry_run_schema
```

Test helpers then use anonymous authentication and override every service endpoint, including
cleanup; applications use the driver's endpoint options instead. C++, Foundry, DuckDB and dbt
runners, plus credential, IAM and Cloud Storage tests, still require Google Cloud. Other tests keep
their assertions and may fail on [emulator gaps](docs/LIMITATIONS.md#emulator-coverage). Emulators
do not validate IAM, quotas or billing.

## Changes and review

[AGENTS.md](AGENTS.md) lists the project rules and where each kind of documentation belongs. Add a
regression test for each behavior fix, back performance and size changes with measurements and
reproducible commands, and never echo credentials or row contents in error messages. API
compatibility is not guaranteed while the project is experimental.

## Distribution packaging

Build a native release archive with the same license bundle as the Python wheels:

```sh
uv run --project python --locked python tools/package_native.py
```

This builds the locked release cdylib, checks its exports, and writes a platform-named archive
(`.tar.gz` on Linux/macOS, `.zip` on Windows) with the library, driver manifest, `LICENSE`,
`NOTICE`, dependency texts, Rust standard-library notices and source inventory to `dist/`
(`--output-dir` changes it). `--library /absolute/path/to/library` or `--wheel path/to/wheel`
packages an existing library for this platform; like the wheel's `BIGQUERY_ADBC_BUILD_LIBRARY`,
it must come from this checkout's locked dependencies and pinned toolchain.
[`tools/dbt-build`](tools/dbt-build/README.md) builds dbt patched to use this driver.

After changing `Cargo.toml`, `Cargo.lock` or the pinned Rust version (not just this crate's own
version), regenerate the checked-in license bundle:

```sh
python3 tools/generate_licenses.py
python3 tools/generate_licenses.py --check
```

Generation uses locked Cargo metadata for all five release targets, covering normal and build
dependencies (including code that may not survive linking), nested notices and source copyright
headers, deduplicated by hash. The pinned compiler supplies `COPYRIGHT-library.html`; Cargo may
download dependency sources. Packaging checks the saved hashes offline, and source distributions
include the bundle and generator. Review new dependencies' licenses: missing texts fail
generation, so add a reviewed copy to `tools/license-sources/` with its upstream revision and URL
in `sources.json` (checked against Cargo VCS metadata). CI regenerates the bundle and builds reject
stale or damaged copies; these checks preserve notices but do not replace license review.

## Releasing

The tag must match `version` in `Cargo.toml`, the single version source. Move the
[changelog](CHANGELOG.md)'s unreleased entries under the new version (links in entries must be
absolute), then:

```sh
git tag vX.Y.Z
git push origin vX.Y.Z
```

The tag runs the [wheel workflow](.github/workflows/wheels.yml). Once CI and every platform build
pass on the tagged commit, it creates a GitHub release, with the version's changelog section as
notes (it stops before publishing anything if that section is missing), then publishes the wheels
and source distribution to PyPI. CI tests a wheel built from the same source archive that gets
published. The release carries a [native archive](#distribution-packaging) per platform, built from
the library in that platform's tested wheel, with `SHA256SUMS`, plus each platform's compressed
library and the `install.sh` that [patched dbt](tools/dbt-build/README.md) builds download. Those
builds hash the published file, so they start after the release exists and attach their archives
and `dbt-SHA256SUMS` about an hour later; PyPI does not wait for them.

PyPI never accepts a version twice, so a bad release needs a new version; re-running the workflow
only finishes an interrupted upload. Publishing uses
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) from the `pypi` GitHub environment,
which only `v*` tags can deploy to; no token is stored. Other tag forms, including pre-releases, do
not publish.
