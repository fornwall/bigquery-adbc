"""Download the `adbc` community extension once and log its build, before the tests start."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import extension

if __name__ == "__main__":
    with extension.connect() as connection:
        [(build,)] = connection.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ?",
            [extension.EXTENSION_NAME],
        ).fetchall()
    print(f">> using {extension.EXTENSION_NAME} extension build {build}", flush=True)
