#!/usr/bin/env bash
set -euo pipefail

# Build both artifacts into dist/; this wrapper accepts no build arguments.
if [ "$#" -ne 0 ]; then
    echo "usage: build-python-package.sh (takes no arguments)" >&2
    exit 2
fi

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR source=lib/built-wheel.sh
. "$repository_root/scripts/lib/built-wheel.sh"

python_executable=${BIGQUERY_ADBC_PYTHON:-python3}
temporary_root=${TMPDIR:-/tmp}
work_directory=$(mktemp -d "$temporary_root/bigquery-adbc-python-package.XXXXXX")
export CARGO_TARGET_DIR="$work_directory/cargo-target"

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if ! rm -rf -- "$work_directory" && [ "$status" -eq 0 ]; then
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$repository_root/dist"
uv build --python "$python_executable" --sdist --wheel \
    --out-dir "$repository_root/dist" "$repository_root/python"

# Reject stale wheels in dist/ so the documented validation globs select this build.
built_wheel "$repository_root/dist" >/dev/null
