#!/usr/bin/env bash
# Run an existing test command against an owned, temporary BigQuery emulator.
set -euo pipefail

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
exec "${BIGQUERY_ADBC_PYTHON:-python3}" "$repository_root/scripts/lib/emulator-tests.py" "$@"
