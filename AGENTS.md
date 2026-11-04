# Agent guide

This is a Rust ADBC driver for BigQuery, with a C interface and Python packaging.
See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and validation commands, and
[docs/REFERENCE.md](docs/REFERENCE.md) for public behavior and options.

## Implementation rules

- Prioritize correctness and ADBC specification conformance.
- Backward compatibility is not required; prefer the cleanest current API and implementation.
- Keep code simple and minimize allocations, overhead, and data copying.
- Support GoogleSQL only. Legacy SQL and legacy-SQL-only options are unsupported.
- Bulk ingestion uses only the Storage Write API. For `bigquery.bulk_ingest.method=load`, return
  `NOT_IMPLEMENTED`.
- Keep comments about contracts, constraints, and non-obvious decisions. Preserve FFI safety
  explanations; avoid narrating code or recording development history in comments.

## Workflow and validation

- Commit completed work frequently in small, focused checkpoints.
- Add regression coverage for behavior fixes in the existing suites. Rust unit and mock tests
  live beside the implementation in `src/`; live Rust tests are in `tests/bigquery/`; client
  integration suites are in `integration/`, and `tests/clients/` drives them.
- Run focused tests while iterating, then the checks for the changed language in
  [CONTRIBUTING.md](CONTRIBUTING.md#local-checks). Use the pinned toolchain and locked dependencies.
  Check Rust with both default and all features; developer features can hide shipped-build issues.
- `cargo test --locked --all-features` runs offline tests and leaves live tests ignored. For
  Python and its integration harnesses, use `scripts/run-offline-python-tests.sh` and
  `scripts/run-python-lint.sh`.
- Python option enums are generated: edit `src/python_options.rs` and the relevant Rust option
  definitions, then run `cargo run --locked --features python-option-codegen --bin generate-python-options`.
  Do not hand-edit `python/src/bigquery_adbc/_options.py`.
- For IPC decoder, REST row decoder or FFI changes, run the
  [fuzz targets and Miri](CONTRIBUTING.md#fuzzing-and-miri).
- Build the shared library with `cargo rustc --release --locked --lib --crate-type cdylib`; the
  manifest declares only the `rlib`, so `cargo build` produces none.
- Live BigQuery tests require `BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` (no default) and
  Application Default Credentials (ADC).
  Use `scripts/run-one-live-test.sh <name>` for a focused Rust live test; it rejects zero matches.
  Scratch-resource defaults are in `tests/bigquery/support.rs`.
- For documentation-only changes, check affected links and `git diff --check`; code tests are
  unnecessary. Report which checks ran and distinguish mock coverage from live validation.

## References and documentation

Use the active [Go BigQuery ADBC driver](https://github.com/adbc-drivers/bigquery/tree/main/go)
as the reference implementation. Do not use the retired `apache/arrow-adbc` BigQuery driver or
the C# driver as references.

Update the relevant documentation with behavior changes:

| File | What belongs there |
| --- | --- |
| [docs/REFERENCE.md](docs/REFERENCE.md) | Public options, type mappings, and driver behavior. |
| [docs/GO-DRIVER-COMPARISON.md](docs/GO-DRIVER-COMPARISON.md) | All differences from the Go driver, including bugs, missing features, and deliberate differences. Also record behavior inherited by the dbt-labs fork from Go here. |
| [docs/DBT-CORE.md](docs/DBT-CORE.md) | Behavior specific to the `dbt-labs/bigquery-adbc` fork. |
| [docs/UPSTREAM.md](docs/UPSTREAM.md) | Relevant issues in dependencies only. Submit upstream fixes when practical. |
| [docs/REVIEW.md](docs/REVIEW.md) | Open limitations and validation gaps; keep completed work in git history. |

Support performance and binary-size claims with measurements and reproducible commands in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md) or [docs/BINARY-SIZE.md](docs/BINARY-SIZE.md).
