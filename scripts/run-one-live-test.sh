#!/usr/bin/env bash
# Usage: run-one-live-test.sh <test name> [test binary arguments...]
#
# Runs one ignored test by bare or module-qualified name, from tests/bigquery/ or the Cargo test
# target BIGQUERY_ADBC_TEST_TARGET names. Cargo also succeeds when nothing matches, so this
# requires that the test ran. Arguments after the name, such as --nocapture, go to the test binary.
set -euo pipefail

usage="usage: run-one-live-test.sh <test name> [test binary arguments...]"
case ${1:-} in
    -h | --help) echo "$usage" && exit 0 ;;
    "") echo "$usage" >&2 && exit 2 ;;
esac

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

test_name=$1
shift
test_target=${BIGQUERY_ADBC_TEST_TARGET:-bigquery}
log_file=$(mktemp "${TMPDIR:-/tmp}/bigquery-adbc-live-test.XXXXXX")
trap 'rm -f -- "$log_file"' EXIT
cd "$(dirname -- "$0")/.."

listing=$(cargo test --locked --test "$test_target" -- --list --ignored)
if ! qualified_name=$(printf '%s\n' "$listing" | awk -v name="$test_name" '
    sub(/: test$/, "") && ($0 == name || substr($0, length($0) - length(name) - 1) == "::" name) {
        found[++count] = $0
    }
    END {
        if (count == 1) { print found[1]; exit 0 }
        for (i = 1; i <= count; i++) print "  candidate: " found[i] > "/dev/stderr"
        exit 1
    }'); then
    echo "$test_name does not name exactly one ignored test in tests/$test_target/." >&2
    exit 1
fi

# pipefail reports a failure. The result line cannot: under --nocapture, stderr lands inside it.
# libtest prints this line before any test starts.
cargo test --locked --test "$test_target" "$qualified_name" \
    -- --ignored --exact "$@" 2>&1 | tee "$log_file"
if ! grep -qx 'running 1 test' "$log_file"; then
    echo "$qualified_name did not run" >&2
    exit 1
fi
