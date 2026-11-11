"""Build the local driver, and locate it in dbt's driver cache."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
# A cdylib-only build gets LTO; a target that also builds an rlib does not.
BUILD_COMMAND = (
    "cargo",
    "rustc",
    "--release",
    "--locked",
    "--lib",
    "--crate-type",
    "cdylib",
    "--manifest-path",
    str(REPO_DIR / "Cargo.toml"),
    "--message-format=json-render-diagnostics",
)


def version() -> str:
    return os.environ.get("DBT_FOUNDRY_DRIVER_VERSION", "0.21.0.dev+dbt0.1.11")


def cache_directory(cache_home: Path) -> Path:
    machine = platform.machine().lower()
    if sys.platform != "linux" or machine not in ("x86_64", "aarch64"):
        raise RuntimeError(f"unsupported local-driver platform: {sys.platform} {machine}")
    return cache_home / "com.getdbt" / "adbc" / f"{machine}-manylinux_2_17-linux-gnu"


def cache_path(cache_home: Path) -> Path:
    return cache_directory(cache_home) / f"libadbc_driver_bigquery_foundry-{version()}.so"


def release_cache_path(cache_home: Path, release: str) -> Path:
    """Where a release build of tools/dbt-build caches the driver it downloads."""
    return cache_directory(cache_home) / f"libadbc_driver_bigquery_adbc-{release}.so"


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def build() -> Path:
    """Build the library, located from Cargo's artifact report: configured targets can move it."""
    messages = subprocess.run(BUILD_COMMAND, cwd=REPO_DIR, check=True, text=True, stdout=subprocess.PIPE).stdout
    libraries: list[Path] = []
    for line in messages.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if (
            message.get("reason") == "compiler-artifact"
            and message["target"]["name"] == "adbc_driver_bigquery"
            and "cdylib" in message["target"]["crate_types"]
        ):
            libraries.extend(
                Path(filename) for filename in message["filenames"] if filename.endswith((".so", ".dylib", ".dll"))
            )
    if len(libraries) != 1:
        raise RuntimeError(f"cargo must report exactly one BigQuery driver library, found {libraries}")
    return libraries[0]
