"""Setuptools commands that build the Rust driver into a platform wheel."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import tomllib
from pathlib import Path

from setuptools import Distribution
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.build_py import build_py as _build_py

from _native_wheel import exported_symbols, inspect_native_binary, platform_tag_for_binary

PACKAGE = "bigquery_adbc"
BUILD_BASE_ENV = "BIGQUERY_ADBC_BUILD_BASE"
LIBRARY_ENV = "BIGQUERY_ADBC_BUILD_LIBRARY"
_REQUIRED_SYMBOLS = frozenset(("AdbcDriverBigqueryInit", "AdbcDriverInit"))
# The native-library format and driver file name of each supported build host.
_HOST_LIBRARIES = {
    "Linux": ("elf", "libadbc_driver_bigquery.so"),
    "Darwin": ("macho", "libadbc_driver_bigquery.dylib"),
    "Windows": ("pe", "adbc_driver_bigquery.dll"),
}


def cargo_version(cargo_toml: Path) -> str:
    """Read the package version from the repository's single version source."""

    with cargo_toml.open("rb") as file:
        return tomllib.load(file)["package"]["version"]


def _host_library() -> tuple[str, str]:
    system = platform.system()
    try:
        return _HOST_LIBRARIES[system]
    except KeyError:
        raise RuntimeError(f"unsupported wheel build platform: {system}") from None


def native_library_name() -> str:
    return _host_library()[1]


def build_or_find_library(repository_root: Path) -> Path:
    override = os.environ.get(LIBRARY_ENV)
    if override:
        library = Path(override).expanduser().resolve()
        if not library.is_file():
            raise RuntimeError(f"{LIBRARY_ENV} is not a regular file: {library}")
    else:
        library = _cargo_build(repository_root)
    validate_native_library(library)
    return library


def _cargo_build(repository_root: Path) -> Path:
    """Build the release cdylib and return the path Cargo reports for it."""

    # The manifest declares only the rlib; a cdylib-only target also gets the profile's LTO.
    command = [
        "cargo",
        "rustc",
        "--release",
        "--locked",
        "--lib",
        "--crate-type",
        "cdylib",
        "--message-format=json-render-diagnostics",
    ]
    try:
        result = subprocess.run(command, cwd=repository_root, check=True, stdout=subprocess.PIPE, text=True)
    except FileNotFoundError as error:
        raise RuntimeError(
            "cargo was not found: building this package from source requires a "
            "Rust toolchain (install one from https://rustup.rs). Install a "
            "prebuilt bigquery-adbc wheel instead, or set "
            f"{LIBRARY_ENV} to an already-built driver library."
        ) from error
    # Cargo configuration and environment can move the target directory and add a
    # target triple, so use the artifact this invocation reports, not a guessed path.
    libraries = []
    for line in result.stdout.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "adbc_driver_bigquery":
            libraries.extend(
                Path(filename) for filename in message["filenames"] if filename.endswith((".dll", ".dylib", ".so"))
            )
    if len(libraries) != 1:
        raise RuntimeError(f"cargo must report exactly one BigQuery driver library, found {libraries}")
    return libraries[0]


def validate_native_library(library: Path) -> None:
    binary = inspect_native_binary(library)
    expected_format = _host_library()[0]
    if binary.format != expected_format:
        raise RuntimeError(
            f"native library format mismatch: host expects {expected_format}, {library} contains {binary.format}"
        )
    missing = _REQUIRED_SYMBOLS - exported_symbols(library, binary.format)
    if missing:
        raise RuntimeError(f"native library {library} does not export: {', '.join(sorted(missing))}")


def setup_options() -> dict[str, dict[str, str]]:
    """Point setuptools' build and metadata directories at the backend's temporary directory."""

    build_base = os.environ.get(BUILD_BASE_ENV)
    if not build_base:
        return {}
    # An explicit --egg-base, as when preparing wheel metadata, still takes precedence.
    return {"build": {"build_base": build_base}, "egg_info": {"egg_base": build_base}}


class BinaryDistribution(Distribution):
    """A platform distribution containing a native data file, not a Python extension."""

    def has_ext_modules(self) -> bool:
        return True


class bdist_wheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = "editable_wheel" in self.distribution.commands
        if self.plat_name_supplied:
            raise RuntimeError(
                "explicit wheel platform tags are unsupported; the build backend "
                "derives the tag from the native library and repair tools assign "
                "portable Linux tags"
            )

    def get_tag(self) -> tuple[str, str, str]:
        if "editable_wheel" in self.distribution.commands:
            # An editable wheel carries no native library (see build_backend.build_editable).
            return "py3", "none", "any"
        _python, _abi, platform_tag = super().get_tag()
        assert self.bdist_dir is not None
        binary = inspect_native_binary(Path(self.bdist_dir) / PACKAGE / native_library_name())
        return "py3", "none", platform_tag_for_binary(platform_tag, binary)


class build_py(_build_py):
    def run(self) -> None:
        super().run()
        if self.editable_mode:
            return
        library = build_or_find_library(Path(__file__).resolve().parent.parent)
        shutil.copy2(library, Path(self.build_lib) / PACKAGE / native_library_name())
