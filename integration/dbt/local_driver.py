"""Build and stage the local driver in dbt's Foundry cache."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
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


def cache_path(cache_home: Path) -> Path:
    machine = platform.machine().lower()
    platforms = {
        ("linux", "x86_64"): "x86_64-manylinux_2_17-linux-gnu",
        ("linux", "aarch64"): "aarch64-manylinux_2_17-linux-gnu",
    }
    try:
        cache_platform = platforms[(sys.platform, machine)]
    except KeyError as error:
        raise RuntimeError(
            f"unsupported local-driver platform: {sys.platform} {machine}"
        ) from error
    filename = f"libadbc_driver_bigquery_foundry-{version()}.so"
    return cache_home / "com.getdbt" / "adbc" / cache_platform / filename


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def built_driver_library(cargo_messages: str) -> Path:
    """Use Cargo's artifact report: configured targets can move the output path."""

    libraries: list[Path] = []
    for line in cargo_messages.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if (
            message.get("reason") == "compiler-artifact"
            and message["target"]["name"] == "adbc_driver_bigquery"
            and "cdylib" in message["target"]["crate_types"]
        ):
            libraries.extend(
                Path(filename)
                for filename in message["filenames"]
                if filename.endswith((".so", ".dylib", ".dll"))
            )
    if len(libraries) != 1:
        raise RuntimeError(
            f"cargo must report exactly one BigQuery driver library, found {libraries}"
        )
    return libraries[0]


def stage(cache_home: Path) -> tuple[Path, str, str]:
    if shutil.which("cargo") is None:
        raise RuntimeError("cargo is not on PATH")
    messages = subprocess.run(
        BUILD_COMMAND, cwd=REPO_DIR, check=True, text=True, stdout=subprocess.PIPE
    ).stdout
    source = built_driver_library(messages)
    destination = cache_path(cache_home)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination, sha256(destination), version()
