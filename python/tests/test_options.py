from __future__ import annotations

import enum

from bigquery_adbc import (
    AuthenticationType,
    BulkIngestMethod,
    ConnectionOptions,
    CreateDisposition,
    CredentialsType,
    DatabaseOptions,
    ParameterMode,
    QueryPriority,
    StatementOptions,
    WriteDisposition,
)

_OPTION_ENUMS = (DatabaseOptions, ConnectionOptions, StatementOptions)
_VALUE_ENUMS = (
    AuthenticationType,
    BulkIngestMethod,
    CreateDisposition,
    CredentialsType,
    ParameterMode,
    QueryPriority,
    WriteDisposition,
)


def _members(enum_types: tuple[type[enum.Enum], ...]) -> set[enum.Enum]:
    return {member for enum_type in enum_types for member in enum_type}


def test_generated_public_members_remain_wire_strings() -> None:
    for member in _members(_OPTION_ENUMS + _VALUE_ENUMS):
        assert isinstance(member, str)
        assert member == member.value
        assert hash(member) == hash(member.value)


def test_legacy_aliases_are_not_advertised_members() -> None:
    public_values = {member.value for member in _members(_OPTION_ENUMS + _VALUE_ENUMS)}
    assert not any(value.startswith("adbc.bigquery.sql.") for value in public_values)


def test_dbt_options_are_not_public() -> None:
    public_options = {member.value for member in _members(_OPTION_ENUMS)}
    assert not any(option.startswith("adbc.bigquery.") for option in public_options)


def test_string_conversions_use_wire_values_on_every_interpreter() -> None:
    for member in _members(_OPTION_ENUMS + _VALUE_ENUMS):
        assert str(member) == member.value
        assert "%s" % member == member.value  # noqa: UP031
        assert f"{member}" == member.value
        assert "{}".format(member) == member.value  # noqa: UP032
        assert f"{member:>80}" == f"{member.value:>80}"
        assert "".join([member]) == member.value
