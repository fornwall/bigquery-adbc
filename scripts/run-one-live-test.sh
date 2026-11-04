#!/usr/bin/env bash
# Run one ignored test by bare or module-qualified name, from tests/bigquery/ or the Cargo test
# target named by BIGQUERY_ADBC_TEST_TARGET.
# Require a one-test success result: cargo also exits successfully for no matches.
#
# Usage: run-one-live-test.sh <test name> [cargo test arguments...]
set -euo pipefail

usage="usage: run-one-live-test.sh <test name> [cargo test arguments...]"
case ${1:-} in
    -h | --help)
        echo "$usage"
        exit 0
        ;;
    "")
        echo "$usage" >&2
        exit 2
        ;;
esac

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

test_name=$1
shift
test_target=${BIGQUERY_ADBC_TEST_TARGET:-bigquery}
repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
log_file=$(mktemp "${TMPDIR:-/tmp}/bigquery-adbc-live-test.XXXXXX")
trap 'rm -f -- "$log_file"' EXIT

cd "$repository_root"
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
    echo "$test_name does not name exactly one ignored live test." >&2
    echo "Check that tests/$test_target/ declares it under that name and with #[ignore]." >&2
    exit 1
fi

cargo test --locked --test "$test_target" "$qualified_name" \
    -- --ignored --exact "$@" 2>&1 | tee "$log_file"
if ! grep -q '^test result: ok\. 1 passed' "$log_file"; then
    echo "$qualified_name did not pass" >&2
    exit 1
fi
