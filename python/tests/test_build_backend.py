from __future__ import annotations

import json
import shutil
import struct
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path, PureWindowsPath

import pytest
from packaging.markers import Marker
from packaging.requirements import Requirement

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import generate_licenses
import package_native

import _build_support
import build_backend
from _build_support import cargo_version
from _licenses import LICENSE_FILES, validate_bundle
from _native_wheel import NativeBinaryInfo

PYTHON_ROOT = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = PYTHON_ROOT.parent


@pytest.fixture(scope="module")
def sdist_archive(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("sdist")
    return directory / build_backend.build_sdist(directory)


def test_wheel_uses_cargo_artifact_in_configured_target_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(_build_support.LIBRARY_ENV, raising=False)
    filename = _build_support.native_library_name()
    stale = tmp_path / "target" / "release" / filename
    stale.parent.mkdir(parents=True)
    stale.touch()
    library = tmp_path / "configured-output" / "target-triple" / "release" / filename
    library.parent.mkdir(parents=True)
    library.touch()
    messages = [
        {"reason": "build-script-executed"},
        {
            "reason": "compiler-artifact",
            "target": {"name": "dependency", "crate_types": ["cdylib"]},
            "filenames": [str(stale)],
        },
        # What `cargo rustc --crate-type cdylib` reports for the driver.
        {
            "reason": "compiler-artifact",
            "target": {"name": "adbc_driver_bigquery", "kind": ["cdylib"], "crate_types": ["cdylib"]},
            "filenames": [str(library)],
        },
        {"reason": "build-finished", "success": True},
    ]

    def build(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command[:2] == ["cargo", "rustc"]
        assert "--lib" in command
        assert command[command.index("--crate-type") + 1] == "cdylib", "only a cdylib-only build gets LTO"
        assert "--message-format=json-render-diagnostics" in command
        assert kwargs["cwd"] == tmp_path
        return subprocess.CompletedProcess(command, 0, "macro output\n" + "\n".join(map(json.dumps, messages)))

    validated = []
    monkeypatch.setattr(_build_support.subprocess, "run", build)
    monkeypatch.setattr(_build_support, "validate_native_library", validated.append)

    assert _build_support.build_or_find_library(tmp_path) == library
    assert validated == [library]


def test_wheel_rejects_cargo_build_without_driver_artifact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(_build_support.LIBRARY_ENV, raising=False)
    stale = tmp_path / "target" / "release" / _build_support.native_library_name()
    stale.parent.mkdir(parents=True)
    stale.touch()
    monkeypatch.setattr(
        _build_support.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, ""),
    )

    with pytest.raises(RuntimeError, match="exactly one BigQuery driver library"):
        _build_support.build_or_find_library(tmp_path)


def test_build_context_preserves_relative_library_override_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    library = tmp_path / _build_support.native_library_name()
    library.touch()
    monkeypatch.setenv(_build_support.LIBRARY_ENV, library.name)
    monkeypatch.setenv(_build_support.BUILD_BASE_ENV, "previous-build-base")
    validated = []
    monkeypatch.setattr(_build_support, "validate_native_library", validated.append)
    failure = RuntimeError("build failed")

    def failing_build() -> None:
        with build_backend._setuptools_context():
            assert _build_support.build_or_find_library(REPOSITORY_ROOT) == library
            raise failure

    with pytest.raises(RuntimeError, match="build failed") as caught:
        failing_build()

    assert caught.value is failure
    assert validated == [library]
    assert Path.cwd() == tmp_path
    assert _build_support.os.environ[_build_support.LIBRARY_ENV] == library.name
    assert _build_support.os.environ[_build_support.BUILD_BASE_ENV] == "previous-build-base"


def _archive_file(archive: tarfile.TarFile, relative_path: str) -> bytes:
    root = next(member.name for member in archive.getmembers() if member.isdir())
    member = archive.getmember(f"{root}/{relative_path}")
    source = archive.extractfile(member)
    assert source is not None
    return source.read()


def _extra_requirement(requirement: str, extra: str) -> str:
    parsed = Requirement(requirement)
    extra_marker = f'extra == "{extra}"'
    marker = f"({parsed.marker}) and {extra_marker}" if parsed.marker else extra_marker
    parsed.marker = Marker(marker)
    return str(parsed)


def test_sdist_uses_canonical_pyproject_metadata(sdist_archive: Path) -> None:
    with (PYTHON_ROOT / "pyproject.toml").open("rb") as source:
        canonical = tomllib.load(source)
    with tarfile.open(sdist_archive) as archive:
        pkg_info = _archive_file(archive, "PKG-INFO")
        staged_pyproject = tomllib.loads(_archive_file(archive, "pyproject.toml").decode("utf-8"))
        copied_pyproject = _archive_file(archive, "python/pyproject.toml")

    assert copied_pyproject == (PYTHON_ROOT / "pyproject.toml").read_bytes()
    assert set(staged_pyproject) == {"build-system"}
    assert staged_pyproject["build-system"]["requires"] == canonical["build-system"]["requires"]
    assert staged_pyproject["build-system"]["build-backend"] == canonical["build-system"]["build-backend"]
    assert staged_pyproject["build-system"]["backend-path"] == ["python"]

    metadata = BytesParser(policy=policy.default).parsebytes(pkg_info)
    project = canonical["project"]
    assert not metadata.defects
    assert metadata["Name"] == project["name"]
    assert metadata["Version"] == cargo_version(REPOSITORY_ROOT / "Cargo.toml")
    assert metadata["Summary"] == project["description"]
    assert metadata["License-Expression"] == project["license"]
    assert metadata["Requires-Python"] == project["requires-python"]
    assert metadata["Description-Content-Type"] == "text/markdown"
    assert pkg_info.split(b"\n\n", 1)[1].rstrip(b"\n") == (PYTHON_ROOT / project["readme"]).read_bytes().rstrip(b"\n")
    assert metadata.get_all("Author", []) == [author["name"] for author in project["authors"]]
    assert metadata.get_all("Classifier", []) == project["classifiers"]

    urls = dict(value.split(", ", 1) for value in metadata.get_all("Project-URL", []))
    assert urls == project["urls"]
    assert set(metadata.get_all("Provides-Extra", [])) == set(project["optional-dependencies"])

    expected_requirements = {str(Requirement(requirement)) for requirement in project["dependencies"]}
    expected_requirements.update(
        _extra_requirement(requirement, extra)
        for extra, requirements in project["optional-dependencies"].items()
        for requirement in requirements
    )
    assert {
        str(Requirement(requirement)) for requirement in metadata.get_all("Requires-Dist", [])
    } == expected_requirements

    requirements = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
    for extra, expected in (
        ("", {"adbc-driver-manager"}),
        ("dbapi", {"adbc-driver-manager", "pyarrow"}),
        ("pandas", {"adbc-driver-manager", "pyarrow", "pandas"}),
    ):
        assert {
            requirement.name
            for requirement in requirements
            if requirement.marker is None or requirement.marker.evaluate({"extra": extra})
        } == expected


def test_license_copies_match_the_repository_files() -> None:
    # Copies keep license files inside the Python project and work on Windows.
    declared = tomllib.loads((PYTHON_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert declared["project"]["license-files"] == list(LICENSE_FILES)
    for document in ("LICENSE", "NOTICE"):
        assert (PYTHON_ROOT / document).read_bytes() == (REPOSITORY_ROOT / document).read_bytes()


def test_aarch64_wheels_drop_only_the_flag_gnu_ld_refuses() -> None:
    config = tomllib.loads((PYTHON_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["cibuildwheel"]
    (override,) = config["overrides"]
    assert override["select"] == "*-manylinux_aarch64"
    # Without this, the override replaces the Linux environment instead of extending it.
    assert override["inherit"]["environment"] == "append"
    assert list(override["environment"]) == ["RUSTFLAGS"]
    icf = " -Clink-arg=-Wl,--icf=all"
    linux = config["linux"]["environment"]["RUSTFLAGS"]
    assert linux.endswith(icf)
    assert override["environment"]["RUSTFLAGS"] == linux.removesuffix(icf)


def test_sdist_carries_the_license_files_setuptools_will_look_for(sdist_archive: Path) -> None:
    with tarfile.open(sdist_archive) as archive:
        for document in LICENSE_FILES:
            expected = (PYTHON_ROOT / document).read_bytes()
            # PyPI resolves License-File against PKG-INFO at the archive root.
            assert _archive_file(archive, document) == expected
            # Wheel builds resolve project.license-files against the staged python/ directory.
            assert _archive_file(archive, f"python/{document}") == expected
        assert (
            _archive_file(archive, "python/licenses/manifest.json")
            == (PYTHON_ROOT / "licenses/manifest.json").read_bytes()
        )
        pkg_info = _archive_file(archive, "PKG-INFO")

    metadata = BytesParser(policy=policy.default).parsebytes(pkg_info)
    assert metadata.get_all("License-File", []) == list(LICENSE_FILES)


def test_sdist_rust_toolchain_is_trimmed_to_a_build(sdist_archive: Path) -> None:
    with tarfile.open(sdist_archive) as archive:
        staged = tomllib.loads(_archive_file(archive, "rust-toolchain.toml").decode("utf-8"))
    with (REPOSITORY_ROOT / "rust-toolchain.toml").open("rb") as source:
        repository = tomllib.load(source)

    # Use the repository compiler without downloading development-only components.
    assert staged["toolchain"]["channel"] == repository["toolchain"]["channel"]
    assert staged["toolchain"]["profile"] == "minimal"
    assert "components" not in staged["toolchain"]


def test_cibuildwheel_pins_the_repository_toolchain() -> None:
    # Each cibuildwheel pin overrides rust-toolchain.toml rather than reading it, so a
    # toolchain bump that misses one would build release wheels with the old compiler.
    with (REPOSITORY_ROOT / "rust-toolchain.toml").open("rb") as source:
        channel = tomllib.load(source)["toolchain"]["channel"]
    with (PYTHON_ROOT / "pyproject.toml").open("rb") as source:
        cibuildwheel = tomllib.load(source)["tool"]["cibuildwheel"]

    assert cibuildwheel["environment"]["RUSTUP_TOOLCHAIN"] == channel
    assert cibuildwheel["linux"]["environment"]["RUSTUP_TOOLCHAIN"] == channel
    assert cibuildwheel["linux"]["before-all"].endswith(f"--default-toolchain {channel}")


def test_sdist_excludes_development_environment_and_tool_caches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    shutil.copy2(PYTHON_ROOT / "pyproject.toml", source / "pyproject.toml")
    (source / "licenses").mkdir()
    for document in LICENSE_FILES:
        (source / document).touch()
    for cache in (".venv", ".ruff_cache", "tests/.pytest_cache"):
        directory = source / cache / "0.1"
        directory.mkdir(parents=True)
        (directory / "cached.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(build_backend, "HERE", source)
    staged = tmp_path / "staged"
    staged.mkdir()

    build_backend._stage_sdist(staged)

    assert (staged / "python" / "pyproject.toml").is_file()
    assert not [path for path in (staged / "python").rglob("*") if path.name == ".venv" or path.name.endswith("_cache")]


@pytest.mark.parametrize("relative_paths", [False, True])
def test_editable_install_exposes_sources_without_building_rust(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, relative_paths: bool
) -> None:
    def unexpected_build(repository_root: Path) -> Path:
        pytest.fail("editable installs must not build the Rust driver")

    monkeypatch.setattr(_build_support, "build_or_find_library", unexpected_build)
    monkeypatch.chdir(tmp_path)
    output = Path() if relative_paths else tmp_path
    metadata_directory = output / "metadata"
    metadata_directory.mkdir()
    dist_info = build_backend.prepare_metadata_for_build_editable(str(metadata_directory))
    wheel = build_backend.build_editable(str(output), metadata_directory=str(metadata_directory / dist_info))
    assert Path.cwd() == tmp_path

    with zipfile.ZipFile(tmp_path / wheel) as archive:
        for document in LICENSE_FILES:
            assert archive.read(f"{dist_info}/licenses/{document}") == (PYTHON_ROOT / document).read_bytes()
        paths = [name for name in archive.namelist() if name.endswith(".pth")]
        assert len(paths) == 1
        assert str(PYTHON_ROOT / "src") in archive.read(paths[0]).decode("utf-8")
        metadata = BytesParser(policy=policy.default).parsebytes(archive.read(f"{dist_info}/WHEEL"))
        assert metadata["Root-Is-Purelib"] == "true"
        assert metadata.get_all("Tag") == ["py3-none-any"]


def test_sdist_carries_every_rust_target_the_manifest_declares(sdist_archive: Path) -> None:
    # Cargo resolves every declared target's source path while parsing Cargo.toml,
    # so any target missing from the sdist fails `pip install` with a manifest error.
    with tarfile.open(sdist_archive) as archive:
        root = next(member.name for member in archive.getmembers() if member.isdir())
        names = set(archive.getnames())
        manifest = tomllib.loads(_archive_file(archive, "Cargo.toml").decode("utf-8"))

    declared = 0
    conventional_directory = {
        "bin": "src/bin",
        "bench": "benches",
        "example": "examples",
        "test": "tests",
    }
    for kind, directory in conventional_directory.items():
        for target in manifest.get(kind, []):
            declared += 1
            candidates = (
                [target["path"]]
                if "path" in target
                else [
                    f"{directory}/{target['name']}.rs",
                    f"{directory}/{target['name']}/main.rs",
                ]
            )
            assert any(f"{root}/{candidate}" in names for candidate in candidates), (
                f"[[{kind}]] {target['name']} wants one of {candidates}, none staged"
            )
    # Zero would mean the loop read no targets, not that every target was staged.
    assert declared > 0


@pytest.fixture
def license_checkout(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    manifest = json.loads((PYTHON_ROOT / "licenses/manifest.json").read_text())
    for relative in (*[name for name in manifest["inputs"] if name != "rust-toolchain"], "rust-toolchain.toml"):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPOSITORY_ROOT / relative, destination)
    shutil.copytree(PYTHON_ROOT / "licenses", root / "python/licenses")
    return root


@pytest.mark.parametrize("document", ["Cargo.lock", "Cargo.toml"])
def test_license_bundle_rejects_changed_dependency_inputs(license_checkout: Path, document: str) -> None:
    validate_bundle(license_checkout)
    with (license_checkout / document).open("a") as source:
        source.write("\n# Changed dependency input\n")
    with pytest.raises(RuntimeError, match="license bundle is stale"):
        validate_bundle(license_checkout)


def test_license_bundle_ignores_the_crate_version(license_checkout: Path) -> None:
    version = cargo_version(REPOSITORY_ROOT / "Cargo.toml")
    for document in ("Cargo.lock", "Cargo.toml"):
        path = license_checkout / document
        text = path.read_text(encoding="utf-8")
        crate = 'name = "bigquery-adbc"\nversion = '
        bumped = text.replace(f'{crate}"{version}"', f'{crate}"99.0.0"')
        assert bumped != text
        path.write_text(bumped, encoding="utf-8")
    validate_bundle(license_checkout)


def test_license_bundle_rejects_a_changed_dependency_version(license_checkout: Path) -> None:
    path = license_checkout / "Cargo.lock"
    text = path.read_text(encoding="utf-8")
    changed = text.replace('name = "adbc_core"\nversion = "', 'name = "adbc_core"\nversion = "9', 1)
    assert changed != text
    path.write_text(changed, encoding="utf-8")
    with pytest.raises(RuntimeError, match="license bundle is stale"):
        validate_bundle(license_checkout)


def test_license_bundle_rejects_changed_toolchain(license_checkout: Path) -> None:
    (license_checkout / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "other"\n')
    with pytest.raises(RuntimeError, match="license bundle is stale"):
        validate_bundle(license_checkout)


@pytest.mark.parametrize("document", ["THIRD_PARTY_LICENSES.txt", "RUST-STD.html"])
def test_license_bundle_rejects_missing_or_changed_notices(license_checkout: Path, document: str) -> None:
    path = license_checkout / "python/licenses" / document
    path.write_text("incomplete notices")
    with pytest.raises(RuntimeError, match="license bundle has changed"):
        validate_bundle(license_checkout)
    path.unlink()
    with pytest.raises(RuntimeError, match="Cannot read dependency license bundle"):
        validate_bundle(license_checkout)


def test_license_hashes_accept_windows_line_endings(license_checkout: Path) -> None:
    for path in license_checkout.rglob("*"):
        if path.is_file():
            path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    validate_bundle(license_checkout)


def test_license_graph_includes_build_dependencies_but_not_development_only() -> None:
    def dependency(package: str, *kinds: str | None) -> dict:
        return {"pkg": package, "dep_kinds": [{"kind": kind} for kind in kinds]}

    metadata = {
        "resolve": {
            "root": "root",
            "nodes": [
                {
                    "id": "root",
                    "deps": [
                        dependency("runtime", None, "dev"),
                        dependency("build", "build"),
                        dependency("dev", "dev"),
                    ],
                },
                {"id": "runtime", "deps": [dependency("transitive", None)]},
                {"id": "build", "deps": []},
                {"id": "transitive", "deps": []},
                {"id": "dev", "deps": []},
            ],
        }
    }
    assert generate_licenses.reachable_packages(metadata) == {"root", "runtime", "build", "transitive"}


def test_collects_nested_licenses_notices_and_source_copyrights(tmp_path: Path) -> None:
    (tmp_path / "LICENSE").write_text("Top-level license terms\n")
    (tmp_path / "third_party").mkdir()
    (tmp_path / "third_party/NOTICE").write_text("Third-party attribution\n")
    (tmp_path / "third_party/COPYING").write_text("Additional license terms\n")
    (tmp_path / "UNLICENSE").write_text("Public domain dedication\n")
    (tmp_path / "native.c").write_text(
        "/* Copyright Example Authors\n * Permission is hereby granted...\n */\nint x;\n"
    )
    package = {
        "name": "fixture",
        "version": "1",
        "manifest_path": str(tmp_path / "Cargo.toml"),
        "source": "registry+fixture",
        "license_file": None,
    }
    documents = generate_licenses.package_documents(package, {})
    assert documents["third_party/NOTICE"] == "Third-party attribution\n"
    assert documents["third_party/COPYING"] == "Additional license terms\n"
    assert documents["UNLICENSE"] == "Public domain dedication\n"
    assert "Copyright Example Authors" in documents["native.c (source notice 1)"]
    assert "int x" not in documents["native.c (source notice 1)"]


def test_missing_dependency_license_requires_review(tmp_path: Path) -> None:
    (tmp_path / "lib.rs").write_text("// Copyright Example Authors\nfn main() {}\n")
    package = {
        "name": "fixture",
        "version": "1",
        "manifest_path": str(tmp_path / "Cargo.toml"),
        "source": "registry+fixture",
        "license_file": None,
    }
    with pytest.raises(RuntimeError, match="No complete license text found"):
        generate_licenses.package_documents(package, {})


@pytest.mark.parametrize(
    ("system", "binary_format", "platform_key", "filename"),
    [
        ("Linux", "elf", "linux_amd64", "libadbc_driver_bigquery.so"),
        ("Darwin", "macho", "macos_amd64", "libadbc_driver_bigquery.dylib"),
        ("Windows", "pe", "windows_amd64", "adbc_driver_bigquery.dll"),
    ],
)
def test_native_archives_carry_manifest_library_and_complete_licenses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    system: str,
    binary_format: str,
    platform_key: str,
    filename: str,
) -> None:
    library = tmp_path / "custom-driver-build"
    library.write_bytes(b"native fixture")
    monkeypatch.setattr(_build_support.platform, "system", lambda: system)
    monkeypatch.setattr(package_native, "validate_native_library", lambda path: None)
    monkeypatch.setattr(package_native, "inspect_native_binary", lambda path: NativeBinaryInfo(binary_format, "x86_64"))
    result = package_native.package_library(REPOSITORY_ROOT, library, tmp_path / "output")
    if binary_format == "pe":
        with zipfile.ZipFile(result) as archive:
            contents = {name.split("/", 1)[1]: archive.read(name) for name in archive.namelist()}
    else:
        with tarfile.open(result) as archive:
            contents = {}
            for member in archive.getmembers():
                source = archive.extractfile(member)
                assert source is not None
                contents[member.name.split("/", 1)[1]] = source.read()
    assert contents[filename] == b"native fixture"
    assert library.name not in contents
    for document in (*LICENSE_FILES, "licenses/manifest.json"):
        assert contents[document] == (PYTHON_ROOT / document).read_bytes()
    manifest = tomllib.loads(contents[package_native.MANIFEST_NAME].decode())
    assert manifest["version"] == cargo_version(REPOSITORY_ROOT / "Cargo.toml")
    assert manifest["Driver"]["entrypoint"] in _build_support._REQUIRED_SYMBOLS
    shared = manifest["Driver"]["shared"]
    assert set(shared) == {"linux_amd64", "linux_arm64", "macos_amd64", "macos_arm64", "windows_amd64"}
    assert PureWindowsPath(shared[platform_key]).name == filename


def test_compressed_library_uses_the_name_dbt_builds_download(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    zstd = pytest.importorskip("compression.zstd")
    library = tmp_path / "custom-driver-build"
    library.write_bytes(b"native fixture")
    monkeypatch.setattr(_build_support.platform, "system", lambda: "Linux")
    monkeypatch.setattr(package_native, "validate_native_library", lambda path: None)
    monkeypatch.setattr(package_native, "inspect_native_binary", lambda path: NativeBinaryInfo("elf", "x86_64"))
    result = package_native.compress_library(REPOSITORY_ROOT, library, tmp_path / "output")
    version = cargo_version(REPOSITORY_ROOT / "Cargo.toml")
    assert result.name == f"libadbc_driver_bigquery-{version}-linux-x86_64.so.zst"
    assert zstd.decompress(result.read_bytes()) == b"native fixture"


def test_native_archives_can_take_the_library_from_a_wheel(tmp_path: Path) -> None:
    filename = _build_support.native_library_name()
    wheel = tmp_path / "driver.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(f"{_build_support.PACKAGE}/{filename}", b"native fixture")
    library = package_native.library_from_wheel(wheel, tmp_path)
    assert library.name == filename
    assert library.read_bytes() == b"native fixture"


def test_native_archives_reject_a_wheel_without_the_library(tmp_path: Path) -> None:
    wheel = tmp_path / "driver.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("other/file", b"")
    with pytest.raises(RuntimeError, match="does not contain"):
        package_native.library_from_wheel(wheel, tmp_path)


def test_built_wheel_preserves_dependency_licenses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    library = tmp_path / _build_support.native_library_name()
    header = bytearray(20)
    header[:4] = b"\x7fELF"
    header[5] = 1
    struct.pack_into("<H", header, 18, 62)
    library.write_bytes(header)
    monkeypatch.setattr(_build_support, "build_or_find_library", lambda root: library)
    # This fixture is ELF regardless of the test host.
    monkeypatch.setattr(_build_support, "native_library_name", lambda: library.name)
    monkeypatch.setattr(_build_support.bdist_wheel, "get_tag", lambda self: ("py3", "none", "linux_x86_64"))
    wheel = tmp_path / build_backend.build_wheel(str(tmp_path))
    with zipfile.ZipFile(wheel) as archive:
        for document in LICENSE_FILES:
            name = next(name for name in archive.namelist() if name.endswith(f".dist-info/licenses/{document}"))
            assert archive.read(name) == (PYTHON_ROOT / document).read_bytes()


@pytest.mark.parametrize("corruption", ["missing", "modified"])
def test_wheel_verification_rejects_lost_dependency_notices(tmp_path: Path, corruption: str) -> None:
    wheel = tmp_path / "bigquery_adbc-0.1.0-py3-none-linux_x86_64.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("bigquery_adbc/driver.so", b"fixture")
        archive.writestr("bigquery_adbc/py.typed", b"")
        for document in LICENSE_FILES:
            if document == "licenses/THIRD_PARTY_LICENSES.txt" and corruption == "missing":
                continue
            contents = (PYTHON_ROOT / document).read_bytes()
            if document == "licenses/THIRD_PARTY_LICENSES.txt" and corruption == "modified":
                contents = b"incomplete notices"
            archive.writestr(f"bigquery_adbc-0.1.0.dist-info/licenses/{document}", contents)
    with pytest.raises(RuntimeError, match=r"license contents|carries no PEP 639"):
        build_backend._verify_wheel(wheel)


def test_git_dependency_keeps_each_ancestor_license(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "LICENSE").write_text("Repository license\n")
    (tmp_path / "NOTICE").write_text("Repository notice\n")
    crate = tmp_path / "crates/fixture"
    crate.mkdir(parents=True)
    (crate / "LICENSE").write_text("Crate license\n")
    (crate.parent / "LICENSE").write_text("Intermediate license\n")
    package = {
        "name": "fixture",
        "version": "1",
        "manifest_path": str(crate / "Cargo.toml"),
        "source": "git+fixture",
        "license_file": None,
    }
    documents = generate_licenses.package_documents(package, {})
    assert {"Repository license\n", "Repository notice\n", "Crate license\n", "Intermediate license\n"} <= set(
        documents.values()
    )
