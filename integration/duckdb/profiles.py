"""Write the ADBC connection profiles the DuckDB ADBC extension reads.

`ATTACH`, `read_adbc()`, and `adbc_execute()` address a database only as
`profile://<name>`, which the driver manager resolves to `<name>.toml` on
`ADBC_PROFILE_PATH`. Profiles are generated rather than checked in because the
`driver` key lacks the `{{ env_var(NAME) }}` interpolation that `[Options]`
supports, so the built library's path cannot be written statically.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# A profile name is both a URI component and a file name.
_NAME_CHARACTERS = set("abcdefghijklmnopqrstuvwxyz0123456789_")

ProfileOption = str | bool | int


def _toml_value(value: ProfileOption) -> str:
    # TOML accepts JSON's escapes but rejects surrogate pairs and an unescaped DEL.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", r"\u007f")


def render_profile(*, driver: str, options: dict[str, ProfileOption]) -> str:
    lines = ["profile_version = 1", f"driver = {_toml_value(driver)}", "", "[Options]"]
    lines.extend(f"{_toml_value(key)} = {_toml_value(value)}" for key, value in options.items())
    return "\n".join(lines) + "\n"


def write_profile(directory: Path, name: str, *, driver: str, options: dict[str, ProfileOption]) -> str:
    """Write one profile into `directory` and return its `profile://` URI."""
    if not name or not set(name) <= _NAME_CHARACTERS:
        raise ValueError(f"unsafe ADBC profile name: {name!r}")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{name}.toml").write_text(render_profile(driver=driver, options=options), encoding="utf-8")
    return f"profile://{name}"


def driver_library() -> str:
    library = os.environ.get("ADBC_BIGQUERY_LIBRARY")
    if not library:
        raise RuntimeError(
            "ADBC_BIGQUERY_LIBRARY must point at the built bigquery-adbc cdylib; "
            "run the suite through cargo test --test clients duckdb -- --ignored"
        )
    path = Path(library)
    if not path.is_file():
        raise RuntimeError(f"ADBC_BIGQUERY_LIBRARY is not a file: {library}")
    return str(path.resolve())
