#!/usr/bin/env bash
# Build the Python sdist and wheel into dist/.
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: build-python-package.sh (takes no arguments)" >&2
    exit 2
fi

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR source=lib/built-wheel.sh
. "$repository_root/scripts/lib/built-wheel.sh"

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/bigquery-adbc-python-package.XXXXXX")
trap 'rm -rf -- "$work_directory"' EXIT
export CARGO_TARGET_DIR="$work_directory/cargo-target"

mkdir -p "$repository_root/dist"
uv build --python "${BIGQUERY_ADBC_PYTHON:-python3}" --sdist --wheel \
    --out-dir "$repository_root/dist" "$repository_root/python"

# Reject stale wheels in dist/ so the documented validation globs select this build.
built_wheel "$repository_root/dist" >/dev/null
