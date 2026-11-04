"""Opt-in emulator routing for the Python integration suite."""

from __future__ import annotations

import os
from collections.abc import Mapping
from urllib.parse import urlsplit

REST_ENDPOINT = "ADBC_BIGQUERY_EMULATOR_REST_ENDPOINT"
STORAGE_ENDPOINT = "ADBC_BIGQUERY_EMULATOR_STORAGE_ENDPOINT"


def integration_project(environment: Mapping[str, str] | None = None) -> str:
    environment = os.environ if environment is None else environment
    project = environment.get("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT")
    if not project:
        raise ValueError("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set to a non-empty project ID")
    if any(c.isspace() or c in "`/?#" for c in project):
        raise ValueError("unsafe BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT value")
    return project


def _endpoint(name: str, value: str) -> str:
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme in {"http", "https"}
            and parsed.hostname
            and parsed.username is None
            and parsed.password is None
            and parsed.path in {"", "/"}
            and not any(character.isspace() for character in value)
            and not any(character in value for character in "?#")
        )
        # Accessing port validates its syntax and range.
        _ = parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be an explicit http:// or https:// endpoint without a path or credentials")
    return value.rstrip("/")


def emulator_options(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return anonymous emulator options, or no overrides for real BigQuery.

    Both transports are required so a partial configuration cannot send Storage
    Read or Write traffic to Google. Auxiliary service endpoints stay local too.
    """

    environment = os.environ if environment is None else environment
    if REST_ENDPOINT not in environment and STORAGE_ENDPOINT not in environment:
        return {}
    if not environment.get(REST_ENDPOINT) or not environment.get(STORAGE_ENDPOINT):
        raise ValueError(f"set both {REST_ENDPOINT} and {STORAGE_ENDPOINT} to use an emulator")
    rest = _endpoint(REST_ENDPOINT, environment[REST_ENDPOINT])
    storage = _endpoint(STORAGE_ENDPOINT, environment[STORAGE_ENDPOINT])
    return {
        "bigquery.auth_type": "anonymous",
        "bigquery.endpoint": rest,
        "bigquery.storage_endpoint": storage,
        "bigquery.gcs_endpoint": rest,
        "bigquery.dataproc_endpoint": rest,
        "bigquery.vertex_endpoint": rest,
    }


if __name__ == "__main__":
    try:
        print("emulator" if emulator_options() else "bigquery")
    except ValueError as error:
        raise SystemExit(str(error)) from None
