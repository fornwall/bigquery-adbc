#!/usr/bin/env bash
set -euo pipefail

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR source=lib/built-wheel.sh
. "$repository_root/scripts/lib/built-wheel.sh"
# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$repository_root/scripts/lib/google-cloud-preflight.sh"

require_bigquery_project

python_executable=${BIGQUERY_ADBC_PYTHON:-python3}
test_backend=$("$python_executable" "$repository_root/python/tests/bigquery_backend.py")
adbc_revision=$("$python_executable" - "$repository_root/Cargo.toml" <<'PY'
import pathlib
import re
import sys

manifest = pathlib.Path(sys.argv[1])
revisions = set(re.findall(
    r'github\.com/apache/arrow-adbc"[^"\n]*rev = "([0-9a-f]+)"',
    manifest.read_text(),
))
if len(revisions) != 1:
    raise SystemExit(f"{manifest} must pin all Apache Arrow ADBC crates to one revision")
print(revisions.pop())
PY
)
temporary_root=${TMPDIR:-/tmp}
work_directory=$(mktemp -d "$temporary_root/bigquery-adbc-python-tests.XXXXXX")
# A caller's CARGO_TARGET_DIR keeps the release build incremental across runs.
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$work_directory/cargo-target}

cleanup() {
    status=$?
    trap - EXIT
    if ! rm -rf -- "$work_directory" && [[ $status -eq 0 ]]; then
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

require_command uv "build and run the Python integration suite"
if [[ $test_backend == bigquery ]]; then
    require_application_default_credentials "run the Python integration suite against BigQuery"
else
    echo ">> Python integration suite uses the configured emulator with anonymous authentication"
fi
# The suite tests the wheel built from this checkout, with its bundled library.
unset BIGQUERY_ADBC_BUILD_LIBRARY BIGQUERY_ADBC_LIBRARY

test_environment="$work_directory/test-environment"
wheel_directory="$work_directory/wheel"
run_directory="$work_directory/run"
mkdir -p "$wheel_directory" "$run_directory"

uv build --python "$python_executable" \
    --wheel \
    --out-dir "$wheel_directory" \
    "$repository_root/python"

wheel=$(built_wheel "$wheel_directory")

uv venv --python "$python_executable" "$test_environment"
# Keep integration validation on the same ADBC revision as the Rust tests.
# CI's installed-wheel suite separately tests the released driver manager.
uv pip install --python "$test_environment/bin/python" \
    "adbc-driver-manager[dbapi] @ git+https://github.com/apache/arrow-adbc.git@$adbc_revision#subdirectory=python/adbc_driver_manager" \
    'pandas' \
    'polars>=2.0.0' \
    'setuptools>=84.0.0' \
    pytest
uv pip install --python "$test_environment/bin/python" \
    --no-deps \
    --force-reinstall \
    "$wheel"
uv pip check --python "$test_environment/bin/python"

export ADBC_BIGQUERY_RUN_INTEGRATION=1

cd "$run_directory"
"$test_environment/bin/python" -I -m pytest \
    "$repository_root/python/tests" \
    "$@"
