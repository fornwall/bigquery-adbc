"""Build a native driver release archive with its license and dependency notices."""

from __future__ import annotations

import argparse
import sys
import tarfile
import tempfile
import zipfile
from functools import partial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from _build_support import (
    PACKAGE,
    build_or_find_library,
    cargo_version,
    native_library_name,
    validate_native_library,
)
from _licenses import LICENSE_FILES, validate_bundle
from _native_wheel import NativeBinaryInfo, inspect_native_binary

ROOT = Path(__file__).resolve().parent.parent

# A driver's name is its manifest's file name, so this name can be installed beside another
# BigQuery driver's `bigquery.toml`.
MANIFEST_NAME = "bigquery-adbc.toml"
MANIFEST = r"""# ADBC driver manifest for the BigQuery driver. Install this file in a directory the driver
# manager searches to load the driver by name, as `bigquery-adbc`:
# https://arrow.apache.org/adbc/current/format/driver_manifests.html
#
# Edit `Driver.shared` if the library is elsewhere. A path must be absolute or on the dynamic
# loader's search path; it is not resolved relative to this file.

manifest_version = 1

name = 'BigQuery ADBC driver'
version = '{version}'
license = 'Apache-2.0'
url = 'https://github.com/fornwall/bigquery-adbc'

[ADBC]
version = '1.1.0'

[Driver]
entrypoint = 'AdbcDriverBigqueryInit'

[Driver.shared]
linux_amd64 = '/usr/local/lib/libadbc_driver_bigquery.so'
linux_arm64 = '/usr/local/lib/libadbc_driver_bigquery.so'
macos_amd64 = '/usr/local/lib/libadbc_driver_bigquery.dylib'
macos_arm64 = '/usr/local/lib/libadbc_driver_bigquery.dylib'
windows_amd64 = 'C:\Program Files\ADBC\Drivers\adbc_driver_bigquery.dll'
"""


def library_from_wheel(wheel: Path, directory: Path) -> Path:
    """Extract the driver from ``wheel`` into ``directory``."""

    member = f"{PACKAGE}/{native_library_name()}"
    with zipfile.ZipFile(wheel) as archive:
        if member not in archive.namelist():
            raise RuntimeError(f"{wheel} does not contain {member}")
        library = directory / native_library_name()
        library.write_bytes(archive.read(member))
    library.chmod(0o755)
    return library


def release_suffix(root: Path, binary: NativeBinaryInfo) -> str:
    """Return the version and platform that end the name of each of a library's release assets."""
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
    return f"{cargo_version(root / 'Cargo.toml')}-{platform}-{binary.architecture}"


def compress_library(root: Path, library: Path, output: Path) -> Path:
    """Write the asset that tools/dbt-build release builds download, in dbt's CDN format."""
    # In the standard library from Python 3.14, which the release workflow uses.
    from compression import zstd

    validate_native_library(library)
    name = Path(native_library_name())
    release = release_suffix(root, inspect_native_binary(library))
    output.mkdir(parents=True, exist_ok=True)
    destination = output / f"{name.stem}-{release}{name.suffix}.zst"
    destination.write_bytes(zstd.compress(library.read_bytes(), level=19))
    return destination


def _release_member(member: tarfile.TarInfo, *, mode: int) -> tarfile.TarInfo:
    # Extracting as root keeps an archive's owners, so the build user's IDs would hand the files,
    # and a library root may load, to whichever local account has them. The checkout's umask must
    # not decide who can read them either.
    member.uid = member.gid = 0
    member.uname = member.gname = ""
    member.mode = mode
    return member


def package_library(root: Path, library: Path, output: Path) -> Path:
    validate_bundle(root)
    validate_native_library(library)
    binary = inspect_native_binary(library)
    version = cargo_version(root / "Cargo.toml")
    name = f"bigquery-adbc-{release_suffix(root, binary)}"
    filename = name + (".zip" if binary.format == "pe" else ".tar.gz")
    files = {native_library_name(): library}
    files.update((path, root / "python" / path) for path in (*LICENSE_FILES, "licenses/manifest.json"))
    output.mkdir(parents=True, exist_ok=True)
    # Keep the temporary archive on the destination filesystem for atomic replacement.
    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-native-", dir=output) as directory:
        manifest = Path(directory) / MANIFEST_NAME
        manifest.write_text(MANIFEST.format(version=version), encoding="utf-8", newline="\n")
        files[MANIFEST_NAME] = manifest
        temporary = Path(directory) / filename
        if binary.format == "pe":
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for relative, path in files.items():
                    archive.write(path, f"{name}/{relative}")
        else:
            with tarfile.open(temporary, "w:gz") as archive:
                for relative, path in files.items():
                    mode = 0o755 if relative == native_library_name() else 0o644
                    archive.add(path, arcname=f"{name}/{relative}", filter=partial(_release_member, mode=mode))
        temporary.replace(output / filename)
    return output / filename


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--library", type=Path, help="package a library built from this checkout instead of building")
    source.add_argument("--wheel", type=Path, help="package the library in a wheel built from this checkout")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    parser.add_argument(
        "--compressed-library",
        action="store_true",
        help="also write the asset that tools/dbt-build release builds download; needs Python 3.14",
    )
    arguments = parser.parse_args()
    validate_bundle(ROOT)
    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-wheel-") as directory:
        if arguments.wheel:
            library = library_from_wheel(arguments.wheel.resolve(), Path(directory))
        elif arguments.library:
            library = arguments.library.resolve()
        else:
            library = build_or_find_library(ROOT)
        if arguments.compressed_library:
            print(compress_library(ROOT, library, arguments.output_dir.resolve()))
        print(package_library(ROOT, library, arguments.output_dir.resolve()))


if __name__ == "__main__":
    main()
