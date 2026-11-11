# Python development

Building and testing the `bigquery-adbc` Python package. For usage, see the
[package README](README.md); for general setup and option-enum regeneration, see
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Local builds

From the repository root, with [uv](https://docs.astral.sh/uv/):

```sh
scripts/build-python-package.sh
uvx twine check dist/*
uvx check-wheel-contents dist/*.whl
```

The script writes a source distribution and a wheel to `dist/`; `uv build` inside `python/` writes
them to `python/dist/`. Wheels carry `LICENSE`, `NOTICE`, the collected dependency license texts and
the Rust standard-library notices under `.dist-info/licenses/`, and source distributions the same
bundle; builds reject a bundle that no longer matches the dependency manifests, lockfile or Rust
version (see [distribution packaging](../CONTRIBUTING.md#distribution-packaging)).

Wheel builds run `cargo rustc --release --locked --lib --crate-type cdylib`, check the library's
format, architecture and ADBC exports, and derive the platform tag from it; explicit platform tags
and multi-architecture libraries are rejected.

| Environment variable | Effect |
| --- | --- |
| `BIGQUERY_ADBC_BUILD_LIBRARY` | At build time, package the library at this absolute path instead of running Cargo. The same checks run. |
| `BIGQUERY_ADBC_LIBRARY` | At runtime, load a development library from this path instead of the bundled one. |

Local Linux builds carry a `linux_<arch>` development tag. Portable release wheels (`py3-none`,
one architecture each, for Linux x86-64 and AArch64 on manylinux 2.28, macOS x86-64 and arm64, and
Windows x86-64) come from cibuildwheel, as in the [wheel workflow](../.github/workflows/wheels.yml):

```sh
uvx --from 'cibuildwheel[uv]==4.3.5' cibuildwheel --output-dir target/wheelhouse python
```

## Local tests

The offline checks CI runs need neither credentials nor a native build:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
uv run --project python --locked pytest -q python/tests/test_api.py  # one test file
uv run --project python --locked ruff format --config python/ruff.toml python integration scripts tests tools
```

`tests/test_installed_wheel.py` needs an installed wheel: CI installs one built from the sdist, and
the live runner below runs it too. Ruff is configured in `python/ruff.toml` and Pyrefly in
`python/pyproject.toml`. An editable install does not compile Rust; to use a native driver from it:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
export BIGQUERY_ADBC_LIBRARY="$PWD/target/release/libadbc_driver_bigquery.so"  # .dylib / .dll
uv run --project python --locked python
```

The live suite builds a fresh wheel and tests it in an isolated environment with ADC, using the
driver manager from the same pinned ADBC revision as the Rust tests (CI's installed-wheel tests
cover the released driver manager):

```sh
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-project
scripts/run-python-tests.sh
scripts/run-python-tests.sh -k parameter_binding
```

The project (required, see [live tests](../CONTRIBUTING.md#live-tests)) must allow queries and
dataset creation. Each run creates a uniquely named, labelled dataset whose tables expire after one
day; cleanup drops a dataset only if its prefix and ownership labels match.

To run against a running emulator, set both endpoints as for the
[Rust suite](../CONTRIBUTING.md#bigquery-emulator):

```sh
ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT=http://127.0.0.1:9050 \
ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT=http://127.0.0.1:9060 \
scripts/run-python-tests.sh -k select_through_low_level_and_dbapi
```

A partial or invalid configuration fails before the build. The runner then skips ADC checks and
sends all requests to the emulator with anonymous authentication; the package itself does not read
these variables. Without `-k` the complete suite runs, and emulator incompatibilities stay test
failures.
