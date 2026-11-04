"""Configuration shared by the dbt integration suite and benchmarks."""

from __future__ import annotations

import os


def require_project() -> str:
    project = os.environ.get("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT")
    if not project:
        raise RuntimeError(
            "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set to a non-empty project ID"
        )
    if any(c.isspace() or c in "`/?#" for c in project):
        raise RuntimeError("unsafe BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT value")
    return project
