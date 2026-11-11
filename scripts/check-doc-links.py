#!/usr/bin/env python3
"""Check relative links and anchors in tracked Markdown, and `<file>.md#<anchor>` citations elsewhere.

Anchors follow GitHub's heading slugs. External URLs are not fetched, except that links to this
repository's files on `main` are checked like relative ones.
"""

import re
import subprocess
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = set(
    subprocess.run(["git", "-C", str(ROOT), "ls-files"], capture_output=True, text=True, check=True).stdout.split()
)
MARKDOWN = sorted(f for f in FILES if f.endswith(".md"))

FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
INLINE = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REFERENCE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")
SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
OWN_REPO = re.compile(r"^https://github\.com/fornwall/bigquery-adbc/(?:blob|tree)/main/([^#?]+)(?:#(.+))?$")
CITATION = re.compile(r"((?:[\w.-]+/)*[\w.-]+\.md)#([\w-]+)")


def prose_lines(text: str) -> list[str]:
    """The text's lines with fenced code blocks blanked, so line numbers still match."""
    lines, in_fence = [], False
    for line in text.split("\n"):
        if FENCE.match(line):
            in_fence = not in_fence
            lines.append("")
        else:
            lines.append("" if in_fence else line)
    return lines


def slug(heading: str) -> str:
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading.strip())
    text = re.sub(r"<[^>]+>", "", text).replace("`", "").lower()
    kept = (c for c in text if c in " -_" or unicodedata.category(c)[0] in "LN" or unicodedata.category(c) == "Mn")
    return "".join(kept).replace(" ", "-")


_anchors: dict[Path, set[str]] = {}


def anchors(path: Path) -> set[str]:
    if path not in _anchors:
        seen: dict[str, int] = {}
        found = set()
        for line in prose_lines(path.read_text(encoding="utf-8")):
            if match := HEADING.match(line):
                base = slug(match.group(2))
                count = seen.get(base, 0)
                found.add(base if count == 0 else f"{base}-{count}")
                seen[base] = count + 1
            found.update(re.findall(r'<a\s+(?:id|name)="([^"]+)"', line))
        _anchors[path] = found
    return _anchors[path]


def check_link(source: str, line: int, target: str, errors: list[str]) -> None:
    if SCHEME.match(target):
        match = OWN_REPO.match(target)
        if not match:
            return
        path, anchor = ROOT / match.group(1), match.group(2)
    elif target.startswith("#"):
        path, anchor = ROOT / source, target[1:]
    else:
        relative, _, anchor = target.partition("#")
        path = (ROOT / source).parent / relative.split("?")[0]
    path = path.resolve()
    if not path.exists():
        errors.append(f"{source}:{line}: missing target {target}")
        return
    if path.is_file() and path.is_relative_to(ROOT) and str(path.relative_to(ROOT)) not in FILES:
        errors.append(f"{source}:{line}: untracked target {target}")
    if anchor and path.suffix == ".md" and anchor not in anchors(path):
        errors.append(f"{source}:{line}: missing anchor #{anchor} in {path.relative_to(ROOT)}")


def main() -> int:
    errors: list[str] = []
    for source in MARKDOWN:
        for number, line in enumerate(prose_lines((ROOT / source).read_text(encoding="utf-8")), 1):
            line = re.sub(r"`[^`]*`", "", line)
            targets = [m.group(1) for m in INLINE.finditer(line)]
            if match := REFERENCE.match(line):
                targets.append(match.group(1))
            for target in targets:
                check_link(source, number, target, errors)

    # Source comments, error text and tests cite documentation sections as `<file>.md#<anchor>`.
    for source in sorted(FILES - set(MARKDOWN)):
        try:
            text = (ROOT / source).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for number, line in enumerate(text.split("\n"), 1):
            for match in CITATION.finditer(line):
                name, anchor = match.groups()
                candidates = [m for m in MARKDOWN if m == name or m.endswith("/" + name)] or [
                    m for m in MARKDOWN if Path(m).name == Path(name).name
                ]
                if not candidates:
                    errors.append(f"{source}:{number}: unknown file {name}#{anchor}")
                elif not any(anchor in anchors(ROOT / m) for m in candidates):
                    errors.append(f"{source}:{number}: missing anchor #{anchor} in {name}")

    for error in errors:
        print(error)
    print(f"{len(MARKDOWN)} Markdown files checked, {len(errors)} problems")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
