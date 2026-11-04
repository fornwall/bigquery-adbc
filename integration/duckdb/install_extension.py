"""Download the `adbc` community extension once, before the tests start."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import extension

if __name__ == "__main__":
    extension.connect().close()
