"""Python bindings for the BigQuery ADBC driver."""

from __future__ import annotations

import importlib.metadata
import os
from collections.abc import Mapping
from pathlib import Path

import adbc_driver_manager

from ._options import (
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

ENTRYPOINT = "AdbcDriverBigqueryInit"

_DISTRIBUTION_NAME = "bigquery-adbc"
_LIBRARY_ENV = "BIGQUERY_ADBC_LIBRARY"
_LIBRARY_FILENAMES = (
    "libadbc_driver_bigquery.so",
    "libadbc_driver_bigquery.dylib",
    "adbc_driver_bigquery.dll",
)
_PACKAGE_DIR = Path(__file__).resolve().parent
# The option values adbc_driver_manager accepts: a bool becomes "true" or "false", an int,
# float or bytes value sets a typed option, and a path is passed as its string.
_OptionValue = str | bytes | int | float | Path

try:
    __version__ = importlib.metadata.version(_DISTRIBUTION_NAME)
except importlib.metadata.PackageNotFoundError:
    # Only an uninstalled source tree lacks distribution metadata.
    __version__ = "0.0.0+unknown"


def _validated_library_path(path: Path, source: str) -> str:
    try:
        is_file = path.is_file()
    except OSError as error:
        raise FileNotFoundError(
            f"Could not inspect the BigQuery ADBC driver library from {source}: {path} ({error})"
        ) from error

    if not is_file:
        raise FileNotFoundError(f"The BigQuery ADBC driver library from {source} is not a regular file: {path}")

    return str(path.resolve(strict=True))


def driver_path() -> str:
    """Return the absolute path of the native BigQuery ADBC driver library.

    ``BIGQUERY_ADBC_LIBRARY``, if set, names a development build to use instead of
    the library bundled beside this module. It is read on every call, so a running
    session can switch builds.
    """

    override = os.environ.get(_LIBRARY_ENV)
    if override:
        return _validated_library_path(Path(override).expanduser(), f"the {_LIBRARY_ENV} environment variable")

    candidates = [path for filename in _LIBRARY_FILENAMES if (path := _PACKAGE_DIR / filename).is_file()]
    if not candidates:
        expected = ", ".join(_LIBRARY_FILENAMES)
        raise FileNotFoundError(
            "No bundled BigQuery ADBC driver library was found beside the Python "
            f"package (expected one of: {expected}). Install a platform wheel, or "
            f"set {_LIBRARY_ENV} to an absolute path for development."
        )
    if len(candidates) > 1:
        found = ", ".join(path.name for path in candidates)
        raise RuntimeError(f"Expected exactly one bundled BigQuery ADBC driver library, but found: {found}")

    return _validated_library_path(candidates[0], "the installed Python package")


def connect(
    uri: str | None = None,
    # Mapping keys are invariant, so enum-keyed mappings need their own member.
    db_kwargs: Mapping[str, _OptionValue] | Mapping[DatabaseOptions, _OptionValue] | None = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low-level ADBC database for BigQuery.

    Configure the driver with ``uri``, ``db_kwargs``, or both. Explicit options
    override URI parameters. Omitting ``uri`` leaves the native option unset.
    """

    options: dict[str, _OptionValue] = dict(db_kwargs.items()) if db_kwargs else {}
    configured_uri = options.get(DatabaseOptions.URI)
    if uri is not None and configured_uri is not None and configured_uri != uri:
        raise ValueError(
            "Conflicting BigQuery URIs were provided as 'uri' and db_kwargs['uri']; "
            "provide the URI once, or use the same value in both places"
        )
    reserved = {"driver", "entrypoint"}.intersection(options)
    if reserved:
        names = ", ".join(sorted(reserved))
        raise ValueError(
            f"db_kwargs must not set {names}: bigquery_adbc.connect() supplies the "
            f"bundled driver library path and the {ENTRYPOINT} entrypoint. Set "
            f"{_LIBRARY_ENV} to load a different library."
        )

    if uri is not None:
        options[DatabaseOptions.URI] = uri
    return adbc_driver_manager.AdbcDatabase(
        driver=driver_path(),
        entrypoint=ENTRYPOINT,
        **options,
    )


__all__ = [
    "ENTRYPOINT",
    "AuthenticationType",
    "BulkIngestMethod",
    "ConnectionOptions",
    "CreateDisposition",
    "CredentialsType",
    "DatabaseOptions",
    "ParameterMode",
    "QueryPriority",
    "StatementOptions",
    "WriteDisposition",
    "__version__",
    "connect",
    "driver_path",
]
