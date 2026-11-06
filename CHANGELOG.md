# Changelog

Notable changes to bigquery-adbc. The project is experimental, so any release may include breaking
changes.

## [Unreleased]

### Added

- Runnable examples for C, Python and Rust in `examples/`.
- GitHub releases carry the shared library for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
  Windows x86-64, with SHA-256 checksums. Each archive has a driver manifest for loading the driver
  by name.

### Changed

- Statements share their SQL text with execution plans and parameter readers, avoiding repeated
  copies of compiled queries.

- The native library shares transaction-control and connection-cleanup task machinery to reduce
  binary size.

- Storage Write validation reuses sliced list offsets unless the list needs normalization,
  avoiding offset-buffer copies for canonical arrays.

- Diagnostics lines start with a UTC timestamp and are tagged `[bigquery-adbc]`.
- An empty `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` disables diagnostics.

### Fixed

- Notebook-template lookup follows empty result pages before reporting `NOT_FOUND` and rejects
  malformed listings as `INVALID_DATA`.
- REST requests refuse redirects, preventing query and upload bodies from being forwarded beyond
  the configured endpoint.
- Ingest rejects equal or reversed RANGE bounds before an invalid first batch can create or
  replace the target table.
- `GetObjects` reports repeated columns as non-nullable in both XDBC nullability fields.
- Dataset-location `ExecuteSchema` invalidates prior statement results, including unread bound
  parameter rows.
- Native archives package an explicitly supplied library under the filename expected by the
  bundled driver manifest, even when the source file has a different name.
- The REST conversion window counts retained row and nested-value allocations, including spare
  capacity, so uneven pages with a few wide rows no longer evade its memory accounting.

## [0.1.1] - 2026-11-05

### Fixed

- The source distribution puts its license files where its metadata declares them, so PyPI accepts
  it. Release 0.1.0 has wheels only.

## [0.1.0] - 2026-11-04

First public release, with wheels on PyPI for Linux x86-64 and AArch64, macOS x86-64 and arm64, and
Windows x86-64.

[Unreleased]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/fornwall/bigquery-adbc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fornwall/bigquery-adbc/tree/v0.1.0
