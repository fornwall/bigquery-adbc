#!/usr/bin/env bash
set -euo pipefail

# Test REST fallback when Storage Read returns PERMISSION_DENIED. Set
# BIGQUERY_ADBC_STORAGE_READ_DENIED_SERVICE_ACCOUNT to an account that can create
# query jobs in the test project but lacks bigquery.readsessions.create. The caller
# needs permission to impersonate it. Pass the short-lived token through the
# environment without printing it.

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"

require_bigquery_project

service_account=${BIGQUERY_ADBC_STORAGE_READ_DENIED_SERVICE_ACCOUNT:-bigquery-adbc-no-readsession@${BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT}.iam.gserviceaccount.com}
require_command gcloud "impersonate $service_account"

export BIGQUERY_ADBC_STORAGE_READ_DENIED_ACCESS_TOKEN
BIGQUERY_ADBC_STORAGE_READ_DENIED_ACCESS_TOKEN=$(
    gcloud auth print-access-token \
        --impersonate-service-account="$service_account" \
        --quiet
)
if [[ -z $BIGQUERY_ADBC_STORAGE_READ_DENIED_ACCESS_TOKEN ]]; then
    echo "gcloud returned an empty access token for $service_account" >&2
    exit 1
fi

exec "$(dirname -- "$0")/run-one-live-test.sh" storage_read_permission_denied_falls_back_to_rest "$@"
