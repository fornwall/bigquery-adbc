#!/usr/bin/env bash
set -euo pipefail

# Test explicit OAuth client ID, client secret, and refresh-token database options.
# Read the values from an authorized-user ADC file unless all three
# BIGQUERY_ADBC_OAUTH_* variables are set. Pass credentials through the environment
# without printing them.
# With --scopes, compare omitted/singular/plural scope fields at Google's token endpoint.

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

test_name=queries_with_discrete_user_oauth_credentials
if [[ ${1:-} == --scopes ]]; then
    test_name=user_oauth_refresh_scope_vs_scopes
    shift
fi

python_executable=${BIGQUERY_ADBC_PYTHON:-python3}
gcloud_configuration=${CLOUDSDK_CONFIG:-$HOME/.config/gcloud}
credentials_file=${GOOGLE_APPLICATION_CREDENTIALS:-$gcloud_configuration/application_default_credentials.json}

if [[ -z ${BIGQUERY_ADBC_OAUTH_CLIENT_ID:-} ]] ||
    [[ -z ${BIGQUERY_ADBC_OAUTH_CLIENT_SECRET:-} ]] ||
    [[ -z ${BIGQUERY_ADBC_OAUTH_REFRESH_TOKEN:-} ]]; then
    if [[ ! -r $credentials_file ]]; then
        echo "no readable credential file at $credentials_file" >&2
        echo "Run: gcloud auth application-default login" >&2
        exit 1
    fi
    credential_exports=$("$python_executable" - "$credentials_file" <<'PYTHON'
import json
import shlex
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as credentials:
    credential = json.load(credentials)

if credential.get("type") != "authorized_user":
    sys.exit(
        f"{path} holds {credential.get('type', 'unknown')!r} credentials, "
        "not the authorized-user credential this test needs.\n"
        "Run: gcloud auth application-default login"
    )

for name, field in (
    ("BIGQUERY_ADBC_OAUTH_CLIENT_ID", "client_id"),
    ("BIGQUERY_ADBC_OAUTH_CLIENT_SECRET", "client_secret"),
    ("BIGQUERY_ADBC_OAUTH_REFRESH_TOKEN", "refresh_token"),
):
    value = credential.get(field)
    if not value:
        sys.exit(f"{path} has no {field}")
    print(f"export {name}={shlex.quote(value)}")
PYTHON
    ) || exit 1
    eval "$credential_exports"
fi

exec "$(dirname -- "$0")/run-one-live-test.sh" "$test_name" "$@"
