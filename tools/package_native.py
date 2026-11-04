"""Build a native driver release archive with its license and dependency notices."""

from __future__ import annotations

import argparse
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from _build_support import build_or_find_library, cargo_version, validate_native_library
from _licenses import LICENSE_FILES, validate_bundle
from _native_wheel import inspect_native_binary

ROOT = Path(__file__).resolve().parent.parent


def package_library(root: Path, library: Path, output: Path) -> Path:
    validate_bundle(root)
    validate_native_library(library)
    binary = inspect_native_binary(library)
    platform = {"elf": "linux", "macho": "macos", "pe": "windows"}[binary.format]
    supported = {
        ("linux", "x86_64"),
        ("linux", "aarch64"),
        ("macos", "x86_64"),
        ("macos", "aarch64"),
        ("windows", "x86_64"),
    }
    if (platform, binary.architecture) not in supported:
        raise RuntimeError(f"No reviewed license bundle for {platform}/{binary.architecture}")
    name = f"bigquery-adbc-{cargo_version(root / 'Cargo.toml')}-{platform}-{binary.architecture}"
    filename = name + (".zip" if binary.format == "pe" else ".tar.gz")
    files = {library.name: library}
    files.update((path, root / "python" / path) for path in (*LICENSE_FILES, "licenses/manifest.json"))
    output.mkdir(parents=True, exist_ok=True)
    # Keep the temporary archive on the destination filesystem for atomic replacement.
    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-native-", dir=output) as directory:
        temporary = Path(directory) / filename
        if binary.format == "pe":
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for relative, path in files.items():
                    archive.write(path, f"{name}/{relative}")
        else:
            with tarfile.open(temporary, "w:gz") as archive:
                for relative, path in files.items():
                    archive.add(path, arcname=f"{name}/{relative}")
        temporary.replace(output / filename)
    return output / filename


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, help="package a library built from this checkout instead of building")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    arguments = parser.parse_args()
    validate_bundle(ROOT)
    library = arguments.library.resolve() if arguments.library else build_or_find_library(ROOT)
    print(package_library(ROOT, library, arguments.output_dir.resolve()))


if __name__ == "__main__":
    main()
