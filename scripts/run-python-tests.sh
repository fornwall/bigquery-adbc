#!/usr/bin/env bash
# Build this checkout's wheel and run python/tests against BigQuery, or against the emulator the
# ADBC_BIGQUERY_EMULATOR_* endpoints name. Arguments go to pytest.
set -euo pipefail

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR source=lib/built-wheel.sh
. "$repository_root/scripts/lib/built-wheel.sh"
# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$repository_root/scripts/lib/google-cloud-preflight.sh"

require_bigquery_project

python_executable=${BIGQUERY_ADBC_PYTHON:-python3}
test_backend=$("$python_executable" "$repository_root/python/tests/bigquery_backend.py")
# Integration validation uses the driver manager at the ADBC revision the Rust tests pin. CI's
# installed-wheel suite separately tests the released driver manager.
adbc_revision=$(sed -n 's|.*github\.com/apache/arrow-adbc"[^"]*rev = "\([0-9a-f]*\)".*|\1|p' \
    "$repository_root/Cargo.toml" | sort -u)
if [[ -z $adbc_revision || $adbc_revision == *$'\n'* ]]; then
    echo "Cargo.toml must pin all Apache Arrow ADBC crates to one revision" >&2
    exit 1
fi

require_command uv "build and run the Python integration suite"
if [[ $test_backend == bigquery ]]; then
    require_application_default_credentials "run the Python integration suite against BigQuery"
else
    echo ">> Python integration suite uses the configured emulator with anonymous authentication"
fi

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/bigquery-adbc-python-tests.XXXXXX")
trap 'rm -rf -- "$work_directory"' EXIT
# A caller's CARGO_TARGET_DIR keeps the release build incremental across runs.
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$work_directory/cargo-target}
# The suite tests the wheel built from this checkout, with its bundled library.
unset BIGQUERY_ADBC_BUILD_LIBRARY BIGQUERY_ADBC_LIBRARY

venv=$work_directory/venv
uv build --python "$python_executable" --wheel --out-dir "$work_directory/wheel" \
    "$repository_root/python"
wheel=$(built_wheel "$work_directory/wheel")
uv venv --python "$python_executable" "$venv"
uv pip install --python "$venv/bin/python" \
    "adbc-driver-manager[dbapi] @ git+https://github.com/apache/arrow-adbc.git@$adbc_revision#subdirectory=python/adbc_driver_manager" \
    pandas 'polars>=2.0.0' 'setuptools>=84.0.0' pytest
uv pip install --python "$venv/bin/python" --no-deps --force-reinstall "$wheel"
uv pip check --python "$venv/bin/python"

export ADBC_BIGQUERY_RUN_INTEGRATION=1
mkdir "$work_directory/run"
cd "$work_directory/run"
"$venv/bin/python" -I -m pytest "$repository_root/python/tests" "$@"
