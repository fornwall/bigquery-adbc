"""Offline tests for the GoogleSQL quoting helpers."""

from __future__ import annotations

import pytest
from sql import identifier, qualified, string_literal


def test_identifier_and_literal_quoting() -> None:
    assert identifier("my_dataset") == "`my_dataset`"
    assert qualified("proj", "ds", "t") == "`proj`.`ds`.`t`"
    with pytest.raises(ValueError, match="unsafe BigQuery identifier"):
        identifier("back`tick")
    with pytest.raises(ValueError, match="unsafe BigQuery identifier"):
        identifier("")
    assert string_literal("it's") == r"'it\'s'"
    assert string_literal("a\nb") == r"'a\nb'"
