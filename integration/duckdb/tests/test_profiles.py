"""Offline tests for the TOML profiles passed to the DuckDB ADBC extension."""

from __future__ import annotations

import tomllib

import profiles
import pytest


@pytest.mark.parametrize(
    "value",
    [
        'quotes " and backslashes \\',
        "first line\nsecond line\r\n\ttab",
        "".join(chr(code) for code in range(32)) + "\x7f",
        "名前 😀",
    ],
)
def test_profile_strings_round_trip(value: str) -> None:
    options: dict[str, profiles.ProfileOption] = {value: value, "enabled": True, "disabled": False, "count": 12}

    parsed = tomllib.loads(profiles.render_profile(driver=value, options=options))

    assert parsed == {"profile_version": 1, "driver": value, "Options": options}
