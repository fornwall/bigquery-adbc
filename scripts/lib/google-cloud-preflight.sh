# shellcheck shell=bash
# Shared prerequisites for BigQuery integration scripts. Sourced, not executed.

# Require a command on PATH, with its purpose included in the error message.
require_command() {
    local command_name=$1 purpose=$2
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "$command_name is required to $purpose" >&2
        return 1
    fi
}

# Verify ADC by minting a token; gcloud's CLI login is independent of ADC.
require_application_default_credentials() {
    local purpose=${1:-reach BigQuery}
    require_command gcloud "verify Google Cloud Application Default Credentials" || return 1
    if ! gcloud auth application-default print-access-token --quiet >/dev/null; then
        echo "Google Cloud Application Default Credentials are unavailable, and they are" >&2
        echo "required to $purpose." >&2
        echo "Run: gcloud auth application-default login" >&2
        return 1
    fi
}

# Require an explicit project and reject characters that alter dataset references or URIs.
require_bigquery_project() {
    if [[ -z ${BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT:-} ]]; then
        echo "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set to a non-empty project ID" >&2
        return 1
    fi
    export BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT
    if [[ $BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT == *[[:space:]]* ||
          $BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT == *['`/?#']* ]]; then
        echo "unsafe BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT value: $BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT" >&2
        return 1
    fi
}
