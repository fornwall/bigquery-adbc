# Security policy

bigquery-adbc is an experimental project with a single maintainer, and there is no guaranteed
response time. Only the latest release and `main` are supported.

## Reporting a vulnerability

Report vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/fornwall/bigquery-adbc/security/advisories/new).
Please don't open public issues for vulnerabilities. Include the driver revision, the impact and
how to reproduce it, and remove real credentials and private data.

## Scope

- **Credential handling:** credentials sent to a destination the
  [endpoint rules](docs/REFERENCE.md#endpoint-overrides) don't allow, or exposed through errors,
  diagnostics or option getters.
- **The C ABI:** memory unsafety, or a panic crossing the boundary, in calls that meet the ADBC
  contract.
- **Decoding server responses:** memory unsafety, crashes or unbounded resource use when decoding
  malformed Arrow IPC or REST JSON.

Behavior described in the [known limitations](docs/LIMITATIONS.md) or as an open exposure in the
endpoint rules is not a vulnerability, unless its impact is worse than documented. Report a
dependency's vulnerability to that project, unless the driver's use of the dependency causes it.
