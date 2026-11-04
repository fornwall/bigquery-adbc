#!/usr/bin/env bash
set -euo pipefail

# Test an explicit service-account key as both a JSON file and an inline JSON string.
# Set BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE to an existing key; the account needs
# roles/bigquery.jobUser in the test project. BIGQUERY_ADBC_SERVICE_ACCOUNT selects
# the account shown in key-creation instructions when the key is missing or invalid.
# Pass the key path through the environment without printing its contents.

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

python_executable=${BIGQUERY_ADBC_PYTHON:-python3}
gcloud_configuration=${CLOUDSDK_CONFIG:-$HOME/.config/gcloud}
service_account=${BIGQUERY_ADBC_SERVICE_ACCOUNT:-bigquery-adbc-integration@${BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT}.iam.gserviceaccount.com}

key_file=${BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE:-$gcloud_configuration/bigquery-adbc-integration-key.json}
# The test runs from the repository root, so resolve a relative path from here.
if [[ $key_file != /* ]]; then
    key_file=$PWD/$key_file
fi
export BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE=$key_file

if [[ ! -r $key_file ]]; then
    echo "no readable service-account key at $key_file" >&2
    echo "Run: gcloud iam service-accounts keys create $key_file \\" >&2
    echo "         --iam-account=$service_account" >&2
    echo "That account needs roles/bigquery.jobUser on the project." >&2
    exit 1
fi

"$python_executable" - "$key_file" "$service_account" <<'PYTHON' || exit 1
import json
import sys

path, service_account = sys.argv[1], sys.argv[2]
try:
    with open(path, encoding="utf-8") as key_file:
        key = json.load(key_file)
except (OSError, ValueError) as error:
    sys.exit(f"{path} is not readable JSON: {error}")

create = (
    f"Run: gcloud iam service-accounts keys create {path} "
    f"--iam-account={service_account}"
)

kind = key.get("type", "unknown") if isinstance(key, dict) else "unknown"
if kind != "service_account":
    sys.exit(
        f"{path} holds {kind!r} credentials, "
        f"not the service-account key this test needs.\n{create}"
    )

client_email = key.get("client_email")
if not client_email:
    sys.exit(f"{path} has no client_email\n{create}")

# The live test expects SESSION_USER() to be this principal. Print nothing else from the key.
print(f"using the service-account key for {client_email}")
PYTHON

exec "$(dirname -- "$0")/run-one-live-test.sh" queries_with_service_account_key_file_and_json_string "$@"
