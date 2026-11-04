"""Verify the repaired wheel that cibuildwheel is about to publish."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from build_backend import _verify_wheel

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} WHEEL")
    _verify_wheel(Path(sys.argv[1]))
