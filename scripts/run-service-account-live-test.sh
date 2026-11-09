#!/usr/bin/env bash
# Test a service-account key as a JSON file and as an inline JSON string. The key at
# BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE is for an account with roles/bigquery.jobUser in the test
# project; BIGQUERY_ADBC_SERVICE_ACCOUNT names that account in the hint for creating one.
set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

service_account=${BIGQUERY_ADBC_SERVICE_ACCOUNT:-bigquery-adbc-integration@${BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT}.iam.gserviceaccount.com}
key_file=${BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE:-${CLOUDSDK_CONFIG:-$HOME/.config/gcloud}/bigquery-adbc-integration-key.json}
# The test runs from the repository root, so resolve a relative path from here.
[[ $key_file == /* ]] || key_file=$PWD/$key_file
if [[ ! -r $key_file ]]; then
    echo "no readable service-account key at $key_file" >&2
    echo "Run: gcloud iam service-accounts keys create $key_file --iam-account=$service_account" >&2
    echo "That account needs roles/bigquery.jobUser on the project." >&2
    exit 1
fi
export BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE=$key_file

exec "$(dirname -- "$0")/run-one-live-test.sh" queries_with_service_account_key_file_and_json_string "$@"
