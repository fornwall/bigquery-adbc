#!/usr/bin/env bash
# Run the Python tests that need no credentials, network access or built driver library; uv may
# download dependencies first. Each suite has its own environment, so to filter, run one of the
# commands below directly. Live suites use scripts/run-python-tests.sh, tests/clients/ and
# integration/dbt/run.py.
set -euo pipefail

if [ "$#" -ne 0 ]; then
  echo "usage: run-offline-python-tests.sh (takes no arguments)" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required; see https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi
unset GOOGLE_APPLICATION_CREDENTIALS GOOGLE_CLOUD_PROJECT \
  BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT ADBC_BIGQUERY_RUN_INTEGRATION

# CI runs the oldest and newest supported Python versions; unset, uv chooses. Suites that lock
# DuckDB pin a GIL build in their .python-version.
wheel_python=()
if [ -n "${BIGQUERY_ADBC_WHEEL_PYTHON:-}" ]; then
  wheel_python=(--python "$BIGQUERY_ADBC_WHEEL_PYTHON")
  echo ">> wheel-suite interpreter: $BIGQUERY_ADBC_WHEEL_PYTHON"
fi

echo ">> python/tests: offline wheel tests"
uv run --quiet --project python --locked ${wheel_python[@]+"${wheel_python[@]}"} \
  python -m pytest -q -p no:cacheprovider \
  python/tests/test_api.py \
  python/tests/test_build_backend.py \
  python/tests/test_options.py \
  python/tests/test_native_wheel.py \
  python/tests/test_bigquery_backend.py \
  python/tests/test_scripts.py

echo ">> integration/foundry-validation: query-fixture lint"
uv run --quiet --project integration/foundry-validation --locked \
  python -m pytest -q -p no:cacheprovider \
  integration/foundry-validation/tests/test_query.py::TestQuery::test_lint_query

echo ">> integration/duckdb: quoting and profile tests"
# The suite's conftest imports duckdb, so use the suite's own environment.
uv run --quiet --project integration/duckdb --locked \
  python -m pytest -q -p no:cacheprovider \
  integration/duckdb/tests/test_sql.py \
  integration/duckdb/tests/test_profiles.py

echo ">> tests/harness: dbt runner unit tests"
# Use the runner's locked dependencies and import its modules from its directory.
uv run --quiet --directory integration/dbt --locked \
  python -m unittest discover -s ../../tests/harness -p test_dbt_runner.py -q -b

echo ">> offline Python tests OK"
