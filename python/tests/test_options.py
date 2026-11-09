from __future__ import annotations

import bigquery_adbc

_ENUMS = (
    bigquery_adbc.DatabaseOptions,
    bigquery_adbc.ConnectionOptions,
    bigquery_adbc.StatementOptions,
    bigquery_adbc.AuthenticationType,
    bigquery_adbc.BulkIngestMethod,
    bigquery_adbc.CreateDisposition,
    bigquery_adbc.CredentialsType,
    bigquery_adbc.ParameterMode,
    bigquery_adbc.QueryPriority,
    bigquery_adbc.WriteDisposition,
)


def test_generated_members_are_public_wire_strings_on_every_interpreter() -> None:
    for member in (member for enum_type in _ENUMS for member in enum_type):
        assert isinstance(member, str)
        assert member == member.value
        assert hash(member) == hash(member.value)
        # Legacy aliases and dbt-only options are not public.
        assert not member.value.startswith("adbc.bigquery.")
        assert str(member) == member.value
        assert "%s" % member == member.value  # noqa: UP031
        assert f"{member}" == member.value
        assert "{}".format(member) == member.value  # noqa: UP032
        assert f"{member:>80}" == f"{member.value:>80}"
        assert "".join([member]) == member.value
