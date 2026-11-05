"""Validate the checked-in notices without requiring Cargo or network access."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from pathlib import Path

LICENSE_FILES = ("LICENSE", "NOTICE", "licenses/THIRD_PARTY_LICENSES.txt", "licenses/RUST-STD.html")
BUNDLE_FILES = ("THIRD_PARTY_LICENSES.txt", "RUST-STD.html")
REGENERATE = "Run python3 tools/generate_licenses.py from the repository checkout."
# Both Cargo.toml and Cargo.lock spell the crate's version this way. Releases change only this,
# and it cannot affect which notices the bundle needs.
CRATE_VERSION = re.compile(r'^(name = "bigquery-adbc"\nversion = )"[^"]*"$', re.MULTILINE)


def text_hash(path: Path) -> str:
    # Git may check text files out with CRLF on Windows.
    return hashlib.sha256(path.read_text(encoding="utf-8").encode()).hexdigest()


def input_hash(path: Path) -> str:
    text = CRATE_VERSION.sub(r"\1", path.read_text(encoding="utf-8"))
    return hashlib.sha256(text.encode()).hexdigest()


def bundle_inputs(root: Path) -> dict[str, str]:
    paths = [root / "Cargo.toml", root / "Cargo.lock"]
    inputs = {path.relative_to(root).as_posix(): input_hash(path) for path in paths}
    with (root / "rust-toolchain.toml").open("rb") as source:
        # The sdist omits development components from this file.
        inputs["rust-toolchain"] = tomllib.load(source)["toolchain"]["channel"]
    return inputs


def validate_bundle(root: Path) -> None:
    directory = root / "python/licenses"
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest["inputs"] != bundle_inputs(root):
            raise RuntimeError(f"Dependency license bundle is stale. {REGENERATE}")
        if set(manifest["files"]) != set(BUNDLE_FILES) or not manifest["packages"]:
            raise RuntimeError(f"Dependency license bundle is incomplete. {REGENERATE}")
        for name in BUNDLE_FILES:
            if manifest["files"][name] != text_hash(directory / name):
                raise RuntimeError(f"Dependency license bundle has changed: {name}. {REGENERATE}")
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError(f"Cannot read dependency license bundle: {error}. {REGENERATE}") from error
