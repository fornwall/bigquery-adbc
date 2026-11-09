from __future__ import annotations

import struct
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import _native_wheel
from _native_wheel import (
    NativeBinaryInfo,
    exported_symbols,
    inspect_native_binary,
    platform_tag_for_binary,
    validate_platform_tag,
)


@pytest.mark.parametrize(
    ("binary_format", "listing", "expected"),
    [
        ("elf", "1000 T AdbcDriverInit\n1008 W weak_symbol\n", {"AdbcDriverInit", "weak_symbol"}),
        ("macho", "1000 T _AdbcDriverInit\n", {"AdbcDriverInit"}),
    ],
)
def test_nm_lists_defined_exports_with_c_names(
    monkeypatch: pytest.MonkeyPatch, binary_format: str, listing: str, expected: set[str]
) -> None:
    commands: list[list[object]] = []

    def nm(command: list[object], **kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, listing)

    monkeypatch.setattr(_native_wheel.subprocess, "run", nm)

    assert exported_symbols(Path("driver"), binary_format) == expected
    # Undefined symbols are imports, not exports.
    assert "-gU" in commands[0] if binary_format == "macho" else "--defined-only" in commands[0]


def test_missing_nm_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(command: list[object], **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("nm")

    monkeypatch.setattr(_native_wheel.subprocess, "run", missing)

    with pytest.raises(RuntimeError, match="could not list the exports of driver with nm"):
        exported_symbols(Path("driver"), "elf")


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (b"\x7fELF\x00\x01" + bytes(12) + struct.pack("<H", 183), NativeBinaryInfo("elf", "aarch64")),
        (b"\xcf\xfa\xed\xfe" + struct.pack("<I", 0x0100000C), NativeBinaryInfo("macho", "aarch64")),
        (
            b"MZ" + bytes(58) + struct.pack("<I", 64) + b"PE\0\0" + struct.pack("<H", 0x8664),
            NativeBinaryInfo("pe", "x86_64"),
        ),
    ],
    ids=["elf", "macho", "pe"],
)
def test_inspects_architecture(tmp_path: Path, header: bytes, expected: NativeBinaryInfo) -> None:
    library = tmp_path / "driver"
    library.write_bytes(header)
    assert inspect_native_binary(library) == expected


def test_rejects_universal_macho_libraries(tmp_path: Path) -> None:
    universal = tmp_path / "universal.dylib"
    universal.write_bytes(b"\xca\xfe\xba\xbe" + struct.pack(">I", 2) + bytes(40))
    with pytest.raises(RuntimeError, match="universal Mach-O library"):
        inspect_native_binary(universal)


@pytest.mark.parametrize(
    ("platform_tag", "binary", "expected"),
    [
        ("macosx_11_0_x86_64", NativeBinaryInfo("macho", "aarch64"), "macosx_11_0_arm64"),
        ("macosx_11_0_arm64", NativeBinaryInfo("macho", "x86_64"), "macosx_11_0_x86_64"),
        ("linux_x86_64", NativeBinaryInfo("elf", "aarch64"), "linux_aarch64"),
        ("manylinux_2_28_x86_64", NativeBinaryInfo("elf", "x86_64"), "manylinux_2_28_x86_64"),
        ("win_amd64", NativeBinaryInfo("pe", "aarch64"), "win_arm64"),
        ("win32", NativeBinaryInfo("pe", "x86_64"), "win_amd64"),
    ],
)
def test_platform_tag_architecture_comes_from_native_library(
    platform_tag: str, binary: NativeBinaryInfo, expected: str
) -> None:
    assert platform_tag_for_binary(platform_tag, binary) == expected


@pytest.mark.parametrize(
    ("binary_format", "message"),
    [("macho", "expected 'macosx_11_0_arm64'"), ("elf", "requires a macho native library")],
)
def test_rejects_tag_that_does_not_match_the_library(binary_format: str, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        validate_platform_tag("macosx_11_0_x86_64", NativeBinaryInfo(binary_format, "aarch64"))


@pytest.mark.parametrize("platform_tag", ["any", "linux_i686", "macosx_11_0_universal2"])
def test_rejects_unsupported_platform_tags(platform_tag: str) -> None:
    binary = NativeBinaryInfo("macho" if platform_tag.startswith("macosx") else "elf", "x86_64")

    with pytest.raises(RuntimeError, match="unsupported wheel platform tag"):
        platform_tag_for_binary(platform_tag, binary)


def _synthesized_pe(path: Path, names: list[str]) -> Path:
    """Write a minimal PE library exporting ``names`` from a single section."""

    optional_size = 112 + 16 * 8
    section_offset = 88 + optional_size
    section_rva = 0x1000
    raw_offset = 0x400

    header = bytearray(raw_offset)
    header[:2] = b"MZ"
    struct.pack_into("<I", header, 60, 64)
    header[64:68] = b"PE\0\0"
    struct.pack_into("<HH", header, 68, 0x8664, 1)
    struct.pack_into("<H", header, 84, optional_size)
    struct.pack_into("<H", header, 88, 0x20B)
    directory_count_offset = 88 + 108
    struct.pack_into("<I", header, directory_count_offset, 16)
    struct.pack_into("<I", header, directory_count_offset + 4, section_rva)
    header[section_offset : section_offset + 8] = b".rdata\0\0"
    struct.pack_into("<IIII", header, section_offset + 8, 0x1000, section_rva, 0x1000, raw_offset)

    body = bytearray(0x1000)
    struct.pack_into("<I", body, 24, len(names))
    struct.pack_into("<I", body, 32, section_rva + 0x40)
    cursor = 0x80
    for index, name in enumerate(names):
        struct.pack_into("<I", body, 0x40 + index * 4, section_rva + cursor)
        encoded = name.encode("ascii") + b"\0"
        body[cursor : cursor + len(encoded)] = encoded
        cursor += len(encoded)

    path.write_bytes(bytes(header) + bytes(body))
    return path


def test_reads_pe_export_names(tmp_path: Path) -> None:
    library = _synthesized_pe(tmp_path / "driver.dll", ["AdbcDriverBigqueryInit", "AdbcDriverInit"])

    assert exported_symbols(library, "pe") == {"AdbcDriverBigqueryInit", "AdbcDriverInit"}
