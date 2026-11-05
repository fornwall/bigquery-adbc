# Python development

How to build and test the `bigquery-adbc` Python package from this repository. For installing and
using it, see the [package README](README.md). General project setup is in
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Local builds

Use [uv](https://docs.astral.sh/uv/) for builds and development. From the repository root:

```sh
scripts/build-python-package.sh
uvx twine check dist/*
uvx check-wheel-contents dist/*.whl
```

The script writes a source distribution and a wheel to `dist/`; `uv build` inside `python/` writes
them to `python/dist/`.

Wheels include `LICENSE`, `NOTICE`, the complete collected dependency license texts, and the
Rust standard-library notices under their `.dist-info/licenses/` directory. Source distributions
carry the same bundle. Builds reject a bundle that no longer matches the dependency manifests,
lockfile, or Rust version. See [distribution packaging](../CONTRIBUTING.md#distribution-packaging)
for regeneration and native release archives.

Wheel builds run `cargo rustc --release --locked --lib --crate-type cdylib`. The build checks the
library's format, architecture and ADBC exports, then derives the wheel's platform tag from it.
Explicit platform tags and multi-architecture libraries are rejected.

| Environment variable | Effect |
| --- | --- |
| `BIGQUERY_ADBC_BUILD_LIBRARY` | At build time, package the library at this absolute path instead of running Cargo. The same checks run. |
| `BIGQUERY_ADBC_LIBRARY` | At runtime, load a development library from this path instead of the bundled one. |

The enums in `python/src/bigquery_adbc/_options.py` are generated from the Rust option
declarations, and `cargo test` checks that they are current. After changing an option key, value
or Python exposure, regenerate them from the repository root:

```sh
cargo run --locked --features python-option-codegen --bin generate-python-options
```

Local Linux builds carry a `linux_<arch>` development tag. Build portable release wheels with
cibuildwheel:

```sh
uvx --from 'cibuildwheel[uv]==4.3.5' cibuildwheel --output-dir target/wheelhouse python
```

The [wheel workflow](../.github/workflows/wheels.yml)
builds and install-tests `py3-none` wheels for Linux x86-64 and AArch64 (manylinux 2.28), macOS
x86-64 and arm64, and Windows x86-64. Each wheel contains one architecture and works across the
supported Python versions.

## Local tests

From the repository root, run the same offline checks as CI:

```sh
scripts/run-python-lint.sh
scripts/run-offline-python-tests.sh
```

The scripts install locked dependencies and check the package and integration helpers. They need
neither credentials nor a native driver build. To run an individual package test or apply
formatting:

```sh
uv run --project python --locked pytest -q python/tests/test_api.py
uv run --project python --locked ruff format python
```

Ruff is configured in `python/ruff.toml` and Pyrefly in `python/pyproject.toml`.

An editable install does not compile Rust. To use a native driver from it, build the library from
the repository root and set its path:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
export BIGQUERY_ADBC_LIBRARY="$PWD/target/release/libadbc_driver_bigquery.so"
uv run --project python --locked python
```

Use `libadbc_driver_bigquery.dylib` on macOS or `adbc_driver_bigquery.dll` on Windows.

The live suite builds a fresh wheel and tests it in an isolated environment, using ADC. It
installs the driver manager from the same pinned ADBC revision as the Rust tests; CI's
installed-wheel tests separately cover the released driver manager.

```sh
gcloud auth application-default login
export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT=my-project
scripts/run-python-tests.sh
scripts/run-python-tests.sh -k parameter_binding
```

`BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT` must name a project where your credentials can run queries and
create datasets. It is required, including in emulator mode; missing or empty values fail, with
no default or fallback to the active gcloud project. Each run creates a uniquely named,
labelled dataset whose tables expire after one day. Cleanup drops a dataset only if its prefix and
ownership labels match.

To run the same Python suite against a running BigQuery emulator instead, explicitly set both
service endpoints. Real BigQuery remains the default:

```sh
ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT=http://127.0.0.1:9050 \
ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT=http://127.0.0.1:9060 \
scripts/run-python-tests.sh -k select_through_low_level_and_dbapi
```

Set both variables to HTTP or HTTPS origins, without paths or credentials; a partial or invalid
configuration fails before the build. The runner bypasses ADC checks and directs all service
requests to the emulator using anonymous authentication. The package itself does not read these
test-only variables. Dataset cleanup still checks ownership labels, but emulator fixtures omit
table expiration. Remove `-k` to attempt the complete suite; emulator incompatibilities remain
test failures. See the
[emulator instructions](../CONTRIBUTING.md#bigquery-emulator) for starting one and the current
validation scope.
