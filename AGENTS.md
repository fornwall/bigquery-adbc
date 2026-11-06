# Agent guide

Rust ADBC driver for BigQuery, with a C interface and Python packaging. See
[CONTRIBUTING.md](CONTRIBUTING.md) for setup and checks, [REFERENCE.md](docs/REFERENCE.md) for public
behavior, and [ARCHITECTURE.md](docs/ARCHITECTURE.md) for code structure.

## Implementation rules

- Prioritize correctness and ADBC conformance. Backward compatibility is not required.
- Prefer simple APIs and implementations; minimize allocations, overhead and copying.
- Support GoogleSQL only; reject Legacy SQL and its exclusive options. Bulk ingestion uses only
  Storage Write; `bigquery.bulk_ingest.method=load` returns `NOT_IMPLEMENTED`.
- Comment on contracts, constraints and non-obvious decisions; preserve FFI safety explanations.
  Avoid code narration and development history.

## Workflow and validation

- Commit completed work frequently in small, focused checkpoints.
- Add regression coverage for behavior fixes: unit and mock tests beside `src/` implementations,
  live Rust tests in `tests/bigquery/`, client suites in `integration/` via `tests/clients/`.
- Run focused tests, then the required [local checks](CONTRIBUTING.md#local-checks) for the changed
  language and its harnesses, using the pinned toolchain and locked dependencies. Check Rust with
  default and all features; offline tests leave live tests ignored.
- Never hand-edit `python/src/bigquery_adbc/_options.py`: edit `src/python_options.rs` and relevant
  Rust option definitions, then run
  `cargo run --locked --features python-option-codegen --bin generate-python-options`.
- For IPC decoder, REST row decoder or FFI changes, run
  [fuzz targets and Miri](CONTRIBUTING.md#fuzzing-and-miri).
- Build the shared library with `cargo rustc --release --locked --lib --crate-type cdylib`;
  `cargo build` produces only the `rlib`.
- Live tests require `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` (no default) and ADC. Run focused
  Rust cases with `scripts/run-one-live-test.sh <name>` (rejects zero matches); scratch-resource
  defaults are in `tests/bigquery/support.rs`.
- Documentation-only changes need link checks and `git diff --check`, not code tests. Report
  checks run, distinguishing mock coverage from live validation.

## References and documentation

Use the active [Go driver](https://github.com/adbc-drivers/bigquery/tree/main/go) as the reference,
never the retired `apache/arrow-adbc` BigQuery driver or the C# driver. Update relevant docs with
behavior changes:

| File | What belongs there |
| --- | --- |
| [CHANGELOG.md](CHANGELOG.md) | User-visible changes under Unreleased. |
| [REFERENCE.md](docs/REFERENCE.md) | Public options, type mappings and behavior. |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Code structure and data flow; update when module roles change. |
| [GO-DRIVER-COMPARISON.md](docs/GO-DRIVER-COMPARISON.md) | All Go differences: bugs, missing features and deliberate changes, including behavior the dbt-labs fork inherits from Go. |
| [DBT-CORE.md](docs/DBT-CORE.md) | Behavior specific to `dbt-labs/bigquery-adbc`. |
| [UPSTREAM.md](docs/UPSTREAM.md) | Dependency issues only; submit upstream fixes when practical. |
| [LIMITATIONS.md](docs/LIMITATIONS.md) | Open limitations and validation gaps; completed work belongs in git history. |

Support performance and binary-size claims with measurements and reproducible commands in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md) or [docs/BINARY-SIZE.md](docs/BINARY-SIZE.md).
