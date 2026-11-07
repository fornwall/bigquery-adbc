"""Native-library inspection and wheel platform-tag derivation."""

from __future__ import annotations

import struct
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class NativeBinaryInfo:
    """The object format (``elf``, ``macho`` or ``pe``) and architecture of a native library."""

    format: str
    architecture: str


# Only the architectures the wheel pipeline builds: any other native library is a
# mistake to be reported, not a tag to be derived.
_ELF_MACHINES = {62: "x86_64", 183: "aarch64"}
_MACHO_CPU_TYPES = {0x01000007: "x86_64", 0x0100000C: "aarch64"}
_PE_MACHINES = {0x8664: "x86_64", 0xAA64: "aarch64"}
# How each platform-tag family spells those architectures.
_TAG_ARCHITECTURES = {
    "elf": {"x86_64": "x86_64", "aarch64": "aarch64"},
    "macho": {"x86_64": "x86_64", "aarch64": "arm64"},
    "pe": {"x86_64": "amd64", "aarch64": "arm64"},
}
_MACHO_MAGICS = {
    b"\xfe\xed\xfa\xce": ">",
    b"\xfe\xed\xfa\xcf": ">",
    b"\xce\xfa\xed\xfe": "<",
    b"\xcf\xfa\xed\xfe": "<",
}
_UNIVERSAL_MACHO_MAGICS = (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf", b"\xbe\xba\xfe\xca", b"\xbf\xba\xfe\xca")


def inspect_native_binary(library: Path) -> NativeBinaryInfo:
    """Read the object format and architecture from the headers of ``library``."""

    with library.open("rb") as file:
        header = file.read(64)
        magic = header[:4]
        if magic == b"\x7fELF":
            endian = {b"\x01": "<", b"\x02": ">"}.get(header[5:6])
            if endian is None:
                raise RuntimeError(f"invalid ELF byte order in {library}")
            machine = _unpack_from(f"{endian}H", header, 18, library)
            return NativeBinaryInfo("elf", _architecture(_ELF_MACHINES, machine, "ELF machine", library))
        if magic in _MACHO_MAGICS:
            cpu_type = _unpack_from(f"{_MACHO_MAGICS[magic]}I", header, 4, library)
            return NativeBinaryInfo("macho", _architecture(_MACHO_CPU_TYPES, cpu_type, "Mach-O CPU type", library))
        if magic in _UNIVERSAL_MACHO_MAGICS:
            raise RuntimeError(f"{library} is a universal Mach-O library; each wheel carries one architecture")
        if header.startswith(b"MZ"):
            file.seek(_unpack_from("<I", header, 60, library))
            signature = file.read(6)
            if signature[:4] != b"PE\0\0":
                raise RuntimeError(f"invalid PE header in {library}")
            machine = _unpack_from("<H", signature, 4, library)
            return NativeBinaryInfo("pe", _architecture(_PE_MACHINES, machine, "PE machine", library))
    raise RuntimeError(f"unrecognized native library format: {library}")


def exported_symbols(library: Path, binary_format: str) -> set[str]:
    """Return the names of the defined symbols ``library`` exports."""

    if binary_format == "pe":
        # Windows runners have no symbol dumper on PATH, so read the PE export table.
        return _pe_exported_symbols(library)
    command = ["nm", "-gU", library] if binary_format == "macho" else ["nm", "-gD", "--defined-only", library]
    try:
        listing = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError(f"could not list the exports of {library} with nm: {error}") from error
    names = [fields[-1] for line in listing.splitlines() if (fields := line.split())]
    if binary_format == "macho":
        # Mach-O symbol names carry a leading underscore that C names do not.
        return {name.removeprefix("_") for name in names}
    return set(names)


def _pe_exported_symbols(library: Path) -> set[str]:
    data = library.read_bytes()
    pe = _unpack_from("<I", data, 60, library)
    section_count = _unpack_from("<H", data, pe + 6, library)
    optional_size = _unpack_from("<H", data, pe + 20, library)
    optional = pe + 24
    magic = _unpack_from("<H", data, optional, library)
    if magic != 0x20B:  # PE32+, the only format of the 64-bit machines in _PE_MACHINES
        raise RuntimeError(f"unsupported PE optional header {magic:#x} in {library}")
    directory_count_offset = optional + 108
    if _unpack_from("<I", data, directory_count_offset, library) < 1:
        return set()
    export_address = _unpack_from("<I", data, directory_count_offset + 4, library)
    if export_address == 0:
        return set()

    # Each section maps (address, size) to a file offset.
    sections = []
    for index in range(section_count):
        entry = optional + optional_size + index * 40
        virtual_size, virtual_address, raw_size, raw_offset = (
            _unpack_from("<I", data, entry + offset, library) for offset in (8, 12, 16, 20)
        )
        sections.append((virtual_address, max(virtual_size, raw_size), raw_offset))

    def file_offset(address: int) -> int:
        for virtual_address, size, raw_offset in sections:
            if virtual_address <= address < virtual_address + size:
                return address - virtual_address + raw_offset
        raise RuntimeError(f"PE address {address:#x} is outside every section in {library}")

    export = file_offset(export_address)
    name_count = _unpack_from("<I", data, export + 24, library)
    names = file_offset(_unpack_from("<I", data, export + 32, library))
    symbols = set()
    for index in range(name_count):
        start = file_offset(_unpack_from("<I", data, names + index * 4, library))
        end = data.find(b"\0", start)
        if end < 0:
            raise RuntimeError(f"unterminated PE export name in {library}")
        symbols.add(data[start:end].decode("ascii", "replace"))
    return symbols


def platform_tag_for_binary(platform_tag: str, binary: NativeBinaryInfo) -> str:
    """Return ``platform_tag`` with its architecture taken from ``binary``."""

    # Accept both local tags and tags assigned by wheel repair tools.
    if platform_tag.startswith("macosx_"):
        tag_format = "macho"
    elif platform_tag.startswith(("linux_", "manylinux", "musllinux")):
        tag_format = "elf"
    elif platform_tag.startswith("win"):
        tag_format = "pe"
    else:
        raise RuntimeError(f"unsupported wheel platform tag: {platform_tag!r}")
    if binary.format != tag_format:
        raise RuntimeError(
            f"wheel platform tag {platform_tag!r} requires a {tag_format} native library, found {binary.format}"
        )

    spellings = _TAG_ARCHITECTURES[tag_format]
    if tag_format == "pe":
        prefix = "win"
    else:
        suffix = next((spelling for spelling in spellings.values() if platform_tag.endswith(f"_{spelling}")), None)
        if suffix is None:
            raise RuntimeError(f"unsupported wheel platform tag: {platform_tag!r}")
        prefix = platform_tag.removesuffix(f"_{suffix}")
    return f"{prefix}_{spellings[binary.architecture]}"


def validate_platform_tag(platform_tag: str, binary: NativeBinaryInfo) -> None:
    """Reject a platform tag that promises a different native artifact."""

    expected = platform_tag_for_binary(platform_tag, binary)
    if platform_tag != expected:
        raise RuntimeError(
            f"wheel platform tag {platform_tag!r} does not match the {binary.architecture} native library; "
            f"expected {expected!r}"
        )


def _architecture(architectures: dict[int, str], value: int, field: str, library: Path) -> str:
    try:
        return architectures[value]
    except KeyError:
        raise RuntimeError(f"unsupported {field} {value:#x} in {library}") from None


def _unpack_from(format_string: str, data: bytes, offset: int, library: Path) -> int:
    try:
        return int(struct.unpack_from(format_string, data, offset)[0])
    except struct.error as error:
        raise RuntimeError(f"truncated native library header in {library}") from error
