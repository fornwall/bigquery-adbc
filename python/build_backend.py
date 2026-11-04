"""PEP 517 backend for a setuptools package carrying the Rust ADBC library."""

from __future__ import annotations

import json
import os
import shutil
import tarfile
import tempfile
import tomllib
import zipfile
from collections.abc import Generator
from contextlib import chdir, contextmanager
from email import policy
from email.parser import BytesParser
from pathlib import Path
from unittest import mock

from packaging.utils import parse_wheel_filename
from setuptools import build_meta as _setuptools

from _build_support import BUILD_BASE_ENV, LIBRARY_ENV, PACKAGE, cargo_version
from _licenses import LICENSE_FILES, validate_bundle
from _native_wheel import inspect_native_binary, validate_platform_tag

HERE = Path(__file__).resolve().parent
REPOSITORY_ROOT = HERE.parent
SDIST_NAME = "bigquery_adbc"


@contextmanager
def _setuptools_context() -> Generator[None, None, None]:
    """Run setuptools from this directory, with build outputs in a fresh temporary directory."""

    validate_bundle(REPOSITORY_ROOT)
    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-python-build-") as directory:
        environment = {BUILD_BASE_ENV: directory}
        # Keep a relative library path relative to the frontend's working directory,
        # the source tree root, which is HERE's parent in an sdist.
        if library := os.environ.get(LIBRARY_ENV):
            environment[LIBRARY_ENV] = str(Path(library).expanduser().resolve())
        with mock.patch.dict(os.environ, environment), chdir(HERE):
            yield


def _absolute(directory: str) -> str:
    # The setuptools context changes into HERE, so resolve a frontend's relative
    # paths against its own working directory first.
    return str(Path(directory).resolve())


def get_requires_for_build_wheel(config_settings=None):
    with _setuptools_context():
        return _setuptools.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    # The sdist is staged directly and needs no additional build dependency.
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    metadata_directory = _absolute(metadata_directory)
    with _setuptools_context():
        return _setuptools.prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    wheel_directory = _absolute(wheel_directory)
    if metadata_directory is not None:
        metadata_directory = _absolute(metadata_directory)
    with _setuptools_context():
        wheel = _setuptools.build_wheel(wheel_directory, config_settings, metadata_directory)
    _verify_wheel(Path(wheel_directory) / wheel)
    return wheel


def get_requires_for_build_editable(config_settings=None):
    with _setuptools_context():
        return _setuptools.get_requires_for_build_editable(config_settings)


def prepare_metadata_for_build_editable(metadata_directory, config_settings=None):
    metadata_directory = _absolute(metadata_directory)
    with _setuptools_context():
        return _setuptools.prepare_metadata_for_build_editable(metadata_directory, config_settings)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    # Editable installs expose Python sources without compiling Rust. Developers
    # select a separately built native driver with BIGQUERY_ADBC_LIBRARY.
    wheel_directory = _absolute(wheel_directory)
    if metadata_directory is not None:
        metadata_directory = _absolute(metadata_directory)
    with _setuptools_context():
        return _setuptools.build_editable(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    del config_settings
    version = cargo_version(REPOSITORY_ROOT / "Cargo.toml")
    archive_name = f"{SDIST_NAME}-{version}.tar.gz"
    destination = Path(sdist_directory).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-python-sdist-") as directory:
        root = Path(directory) / f"{SDIST_NAME}-{version}"
        root.mkdir()
        _stage_sdist(root)
        _write_pkg_info(root)
        with tarfile.open(destination / archive_name, "w:gz", format=tarfile.PAX_FORMAT) as archive:
            archive.add(root, arcname=root.name)
    return archive_name


def _stage_sdist(root: Path) -> None:
    """Stage the Cargo package at the sdist root and this Python project below it."""

    for filename in ("Cargo.toml", "Cargo.lock", "README.md", "LICENSE", "NOTICE"):
        shutil.copy2(REPOSITORY_ROOT / filename, root / filename)
    # Cargo resolves every declared target's source path when parsing the manifest,
    # including benches and tools that are not built into the wheel.
    for directory in ("src", "benches", "tools"):
        shutil.copytree(
            REPOSITORY_ROOT / directory, root / directory, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
    shutil.copytree(
        HERE,
        root / "python",
        ignore=shutil.ignore_patterns(
            ".venv", "build", "dist", "__pycache__", ".*_cache", "*.dll", "*.dylib", "*.egg-info", "*.pyc", "*.so"
        ),
    )

    with (HERE / "pyproject.toml").open("rb") as file:
        requires = tomllib.load(file)["build-system"]["requires"]
    (root / "pyproject.toml").write_text(
        f"[build-system]\nrequires = {json.dumps(requires)}\n"
        'build-backend = "build_backend"\nbackend-path = ["python"]\n',
        encoding="utf-8",
    )

    # Keep the repository's compiler pin, without the development-only components.
    with (REPOSITORY_ROOT / "rust-toolchain.toml").open("rb") as file:
        channel = tomllib.load(file)["toolchain"]["channel"]
    (root / "rust-toolchain.toml").write_text(
        f'[toolchain]\nchannel = {json.dumps(channel)}\nprofile = "minimal"\n', encoding="utf-8"
    )


def _write_pkg_info(root: Path) -> None:
    """Write the same core metadata setuptools emits for the wheel."""

    with tempfile.TemporaryDirectory(prefix="bigquery-adbc-python-metadata-") as directory:
        with _setuptools_context():
            dist_info = _setuptools.prepare_metadata_for_build_wheel(directory)
        shutil.copyfile(Path(directory) / dist_info / "METADATA", root / "PKG-INFO")


def _verify_wheel(wheel: Path) -> None:
    """Check a built or repaired wheel's contents and tags against its native library."""

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        libraries = [
            name for name in names if name.startswith(f"{PACKAGE}/") and name.endswith((".dll", ".dylib", ".so"))
        ]
        if len(libraries) != 1:
            raise RuntimeError(f"wheel must contain exactly one native library, found {libraries}")
        if f"{PACKAGE}/py.typed" not in names:
            raise RuntimeError("wheel does not contain py.typed")
        # Setuptools adds PEP 639 license files only if project.license-files matches
        # files inside python/.
        for document in LICENSE_FILES:
            matches = [name for name in names if name.endswith(f".dist-info/licenses/{document}")]
            if len(matches) != 1:
                raise RuntimeError(
                    f"wheel carries no PEP 639 {document} file; check that python/{document} "
                    "exists and that project.license-files still names it"
                )
            expected = (HERE / document).read_text(encoding="utf-8")
            if archive.read(matches[0]).decode("utf-8").replace("\r\n", "\n") != expected:
                raise RuntimeError(f"wheel carries incorrect license contents: {document}")

        wheel_metadata_name = next(name for name in names if name.endswith(".dist-info/WHEEL"))
        wheel_metadata = BytesParser(policy=policy.default).parsebytes(archive.read(wheel_metadata_name))
        if wheel_metadata["Root-Is-Purelib"] != "false":
            raise RuntimeError("wheel was incorrectly marked as pure")
        metadata_tags = set(wheel_metadata.get_all("Tag", ()))
        _name, _version, _build, filename_tags = parse_wheel_filename(wheel.name)
        filename_tag_strings = {str(tag) for tag in filename_tags}
        if metadata_tags != filename_tag_strings:
            raise RuntimeError(
                f"wheel filename tags {sorted(filename_tag_strings)} do not match "
                f"WHEEL metadata tags {sorted(metadata_tags)}"
            )
        if any(tag.interpreter != "py3" or tag.abi != "none" for tag in filename_tags):
            raise RuntimeError("wheel does not have a py3-none-platform tag")

        with tempfile.TemporaryDirectory(prefix="bigquery-adbc-wheel-verify-") as directory:
            library = Path(directory) / Path(libraries[0]).name
            with archive.open(libraries[0]) as source, library.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            binary = inspect_native_binary(library)
    for tag in filename_tags:
        validate_platform_tag(tag.platform, binary)
