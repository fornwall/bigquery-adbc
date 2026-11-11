#!/usr/bin/env bash
# Test one user identity through the discrete OAuth client ID, secret and refresh-token options,
# authorized-user JSON as a file and a string, and a temporary access token. Unless all three
# BIGQUERY_ADBC_OAUTH_* variables are set, they are read from the authorized-user ADC file and
# passed through the environment without being printed.
set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=lib/google-cloud-preflight.sh
. "$(dirname -- "$0")/lib/google-cloud-preflight.sh"
require_bigquery_project

if [[ -z ${BIGQUERY_ADBC_OAUTH_CLIENT_ID:-} || -z ${BIGQUERY_ADBC_OAUTH_CLIENT_SECRET:-} ||
    -z ${BIGQUERY_ADBC_OAUTH_REFRESH_TOKEN:-} ]]; then
    credentials=${GOOGLE_APPLICATION_CREDENTIALS:-${CLOUDSDK_CONFIG:-$HOME/.config/gcloud}/application_default_credentials.json}
    exports=$("${BIGQUERY_ADBC_PYTHON:-python3}" - "$credentials" <<'PYTHON'
import json
import shlex
import sys

path = sys.argv[1]
login = "Run: gcloud auth application-default login"
try:
    with open(path, encoding="utf-8") as file:
        credential = json.load(file)
except (OSError, ValueError) as error:
    # Decoding errors name a position, never the document's contents.
    sys.exit(f"cannot read {path}: {error}\n{login}")
if not isinstance(credential, dict):
    sys.exit(f"{path} holds a {type(credential).__name__}, not an authorized_user credential.\n{login}")
if credential.get("type") != "authorized_user":
    sys.exit(f"{path} holds {credential.get('type')!r} credentials, not authorized_user.\n{login}")
for field in ("client_id", "client_secret", "refresh_token"):
    value = credential.get(field)
    if not value:
        sys.exit(f"{path} has no {field}")
    if not isinstance(value, str):
        sys.exit(f"{path}: {field} is not a string")
    print(f"export BIGQUERY_ADBC_OAUTH_{field.upper()}={shlex.quote(value)}")
PYTHON
    ) || exit 1
    eval "$exports"
fi

exec "$(dirname -- "$0")/run-one-live-test.sh" queries_with_discrete_user_oauth_credentials "$@"
