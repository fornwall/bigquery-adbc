"""Collect the locked native dependencies' license texts and source notices."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from _licenses import bundle_inputs

ROOT = Path(__file__).resolve().parent.parent

# Keep this union aligned with the release wheel platforms. Build dependencies
# are included conservatively; development-only dependencies are not traversed.
TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
)
DOCUMENT_PREFIXES = ("LICENSE", "LICENCE", "UNLICENSE", "COPYING", "NOTICE", "COPYRIGHT")
SOURCE_SUFFIXES = {".rs", ".c", ".h", ".cc", ".cpp", ".inc", ".s", ".asm", ".pl"}


def run(*arguments: str) -> str:
    return subprocess.check_output(arguments, cwd=ROOT, text=True)


def normalized_text(text: str) -> str:
    # Preserve notice wording while avoiding trailing whitespace in generated files.
    return "\n".join(line.rstrip() for line in text.splitlines()) + "\n"


def reachable_packages(metadata: dict) -> set[str]:
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    pending = [metadata["resolve"]["root"]]
    visited = set()
    while pending:
        package = pending.pop()
        if package in visited:
            continue
        visited.add(package)
        pending.extend(
            dependency["pkg"]
            for dependency in nodes[package]["deps"]
            if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"])
        )
    return visited


def leading_notices(source: str, suffix: str) -> list[str]:
    """Retain copyright/license comment blocks before the first code token."""
    line_marker = r"\#" if suffix == ".pl" else ";" if suffix == ".asm" else "//"
    comment = re.compile(rf"\s*(/\*.*?\*/|(?:{line_marker}[^\n]*(?:\n|$))+)", re.DOTALL)
    notices = []
    position = 0
    while match := comment.match(source, position):
        text = match[1].strip()
        if re.search(r"copyright|SPDX-License-Identifier|permission is hereby granted", text, re.IGNORECASE):
            notices.append(text + "\n")
        position = match.end()
    return notices


def package_documents(package: dict, supplements: dict) -> dict[str, str]:
    directory = Path(package["manifest_path"]).parent
    documents = {}
    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(directory)
        if any(part.upper().startswith(DOCUMENT_PREFIXES) for part in relative.parts):
            documents[relative.as_posix()] = path.read_text(encoding="utf-8")
        elif path.suffix.lower() in SOURCE_SUFFIXES:
            for index, notice in enumerate(leading_notices(path.read_text(encoding="utf-8"), path.suffix.lower())):
                documents[f"{relative.as_posix()} (source notice {index + 1})"] = notice
    if package["license_file"]:
        path = directory / package["license_file"]
        documents[package["license_file"]] = path.read_text(encoding="utf-8")
    if package["source"].startswith("git+"):
        parent = directory
        depth = 0
        while True:
            for path in sorted(parent.iterdir()):
                if path.name.upper().startswith(DOCUMENT_PREFIXES):
                    for document in sorted(path.rglob("*")) if path.is_dir() else [path]:
                        if document.is_file():
                            label = "../" * depth + document.relative_to(parent).as_posix()
                            documents[f"upstream {label}"] = document.read_text(encoding="utf-8")
            if (parent / ".git").exists():
                break
            if parent == parent.parent:
                raise RuntimeError(f"Cannot locate Git checkout for {package['name']}")
            parent = parent.parent
            depth += 1
    key = f"{package['name']} {package['version']}"
    if supplement := supplements.get(key):
        vcs = json.loads((directory / ".cargo_vcs_info.json").read_text(encoding="utf-8"))
        if vcs["git"]["sha1"] != supplement["revision"]:
            raise RuntimeError(f"License supplement revision mismatch: {key}")
        for filename, url in supplement["files"].items():
            documents[url] = (ROOT / "tools/license-sources" / filename).read_text(encoding="utf-8")
    # A source header may carry attribution without the complete license terms.
    # The one source-only license below is tonic-prost's full MIT header.
    has_license = any(
        any(word in label.upper() for word in ("LICENSE", "LICENCE", "COPYING"))
        or "Permission is hereby granted" in text
        for label, text in documents.items()
    )
    if not has_license:
        raise RuntimeError(f"No complete license text found for {key}; add a reviewed license supplement")
    return documents


def generate() -> dict[str, str]:
    packages = {}
    selected = set()
    for target in TARGETS:
        metadata = json.loads(
            run("cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target)
        )
        packages.update((package["id"], package) for package in metadata["packages"])
        selected.update(reachable_packages(metadata))
    supplements = json.loads((ROOT / "tools/license-sources/sources.json").read_text(encoding="utf-8"))
    documents: dict[str, str] = {}
    inventory = []
    entries = []
    used_supplements = set()
    for package in sorted(
        (packages[key] for key in selected), key=lambda package: (package["name"], package["version"])
    ):
        if package["source"] is None:
            local = Path(package["manifest_path"]).relative_to(ROOT).as_posix()
            if local != "Cargo.toml":
                raise RuntimeError(f"Review licensing of new local dependency: {local}")
            continue
        if not package["license"]:
            raise RuntimeError(f"Missing license declaration: {package['name']}")
        label = f"{package['name']} {package['version']}"
        if label in supplements:
            used_supplements.add(label)
        inventory.append({key: package[key] for key in ("name", "version", "source", "license")})
        entries.extend(["", label, f"License declaration: {package['license']}", f"Source: {package['source']}"])
        for path, content in sorted(package_documents(package, supplements).items()):
            content = normalized_text(content)
            digest = hashlib.sha256(content.encode()).hexdigest()
            documents[digest] = content
            entries.append(f"  [{digest}] {path}")
    if used_supplements != supplements.keys():
        raise RuntimeError(f"Remove obsolete license supplements: {supplements.keys() - used_supplements}")
    preamble = [
        "Third-party licenses and notices for bigquery-adbc",
        "Generated by tools/generate_licenses.py; do not edit.",
        "",
        "This inventory covers the union of normal and build dependencies for the",
        "release targets below. Some components are build-only or platform-specific.",
        "License alternatives and additional notices are reproduced as supplied by",
        "upstream; their inclusion does not change the project's Apache-2.0 license.",
        "The Rust standard library's notices are provided separately in RUST-STD.html.",
        "",
        *TARGETS,
        "",
        "DEPENDENCIES AND DOCUMENT REFERENCES",
    ]
    for digest, content in sorted(documents.items()):
        entries.extend(["", "=" * 78, f"DOCUMENT {digest}", "=" * 78, content])
    toolchain = bundle_inputs(ROOT)["rust-toolchain"]
    version = run("rustc", "--version").split()[1]
    if version != toolchain:
        raise RuntimeError(f"Use the pinned Rust {toolchain}, found {version}")
    sysroot = Path(run("rustc", "--print", "sysroot").strip())
    outputs = {
        "THIRD_PARTY_LICENSES.txt": "\n".join(preamble + entries),
        "RUST-STD.html": (sysroot / "share/doc/rust/COPYRIGHT-library.html").read_text(encoding="utf-8"),
    }
    outputs = {name: normalized_text(text) for name, text in outputs.items()}
    manifest = {
        "inputs": bundle_inputs(ROOT),
        "targets": TARGETS,
        "packages": inventory,
        "files": {name: hashlib.sha256(text.encode()).hexdigest() for name, text in outputs.items()},
    }
    outputs["manifest.json"] = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the checked-in bundle differs")
    arguments = parser.parse_args()
    outputs = generate()
    directory = ROOT / "python/licenses"
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in outputs.items():
        path = directory / name
        if arguments.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != text:
                raise SystemExit(f"Stale {path.relative_to(ROOT)}; run python3 tools/generate_licenses.py")
        else:
            path.write_text(text, encoding="utf-8", newline="\n")
    print("Dependency license bundle verified" if arguments.check else "Dependency license bundle generated")


if __name__ == "__main__":
    main()
