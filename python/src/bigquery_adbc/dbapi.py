"""PEP 249 facade for the BigQuery ADBC driver.

``autocommit=False`` (the PEP 249 default) opens a BigQuery session per connection.
Pass ``autocommit=True`` for independent queries.

Leaving ``with connect(...) as connection:`` closes the connection and rolls
back uncommitted work. Call ``connection.commit()`` before leaving the block,
or connect with ``autocommit=True``.

This facade requires PyArrow; the low-level :func:`bigquery_adbc.connect` API
does not.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING

try:
    # Match the driver manager's PyArrow availability check.
    import pyarrow.dataset
except ImportError as _error:  # pragma: no cover - depends on the installed extras
    raise ImportError(
        "bigquery_adbc.dbapi requires PyArrow. Install it with: "
        "pip install 'bigquery-adbc[dbapi]'. The low-level "
        "bigquery_adbc.connect() API works without PyArrow."
    ) from _error

import adbc_driver_manager
import pyarrow as _pyarrow
from adbc_driver_manager import dbapi as _dbapi

from . import ConnectionOptions, DatabaseOptions, _OptionValue
from . import connect as _driver_connect

if TYPE_CHECKING:
    # The manager's Connection declares its backend parameter with this private type.
    from adbc_driver_manager._dbapi_backend import DbapiBackend


class _TypeSet(type(_dbapi.STRING)):
    """A PEP 249 type object that compares an Arrow extension type by its storage type.

    BigQuery JSON columns are ``arrow.json`` strings, so they compare equal to ``STRING``.
    The manager's type objects inherit ``frozenset.__ne__``, which makes ``!=`` true
    for every Arrow type (docs/UPSTREAM.md UP-92).
    """

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _pyarrow.BaseExtensionType):
            other = other.storage_type
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


# PEP 249 globals and exception hierarchy.
apilevel = _dbapi.apilevel
threadsafety = _dbapi.threadsafety
paramstyle = _dbapi.paramstyle

Warning = _dbapi.Warning
Error = _dbapi.Error
InterfaceError = _dbapi.InterfaceError
DatabaseError = _dbapi.DatabaseError
DataError = _dbapi.DataError
OperationalError = _dbapi.OperationalError
IntegrityError = _dbapi.IntegrityError
InternalError = _dbapi.InternalError
ProgrammingError = _dbapi.ProgrammingError
NotSupportedError = _dbapi.NotSupportedError

# PEP 249 constructors and type objects.
Date = _dbapi.Date
Time = _dbapi.Time
Timestamp = _dbapi.Timestamp
DateFromTicks = _dbapi.DateFromTicks
TimeFromTicks = _dbapi.TimeFromTicks
TimestampFromTicks = _dbapi.TimestampFromTicks
STRING = _TypeSet(_dbapi.STRING)
BINARY = _TypeSet(_dbapi.BINARY)
# Include NUMERIC and BIGNUMERIC in PEP 249 numeric-column comparisons.
# Arrow's decimal type ids are independent of precision and scale.
NUMBER = _TypeSet(_dbapi.NUMBER | {_pyarrow.decimal128(38, 9).id, _pyarrow.decimal256(76, 38).id})
DATETIME = _TypeSet(_dbapi.DATETIME)
ROWID = _TypeSet(_dbapi.ROWID)


def Binary(value: str | bytes | bytearray | memoryview) -> bytes:
    """Construct immutable PEP 249 binary data, encoding string input as UTF-8."""

    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


Connection = _dbapi.Connection
Cursor = _dbapi.Cursor


def connect(
    uri: str | None = None,
    db_kwargs: Mapping[str, _OptionValue] | Mapping[DatabaseOptions, _OptionValue] | None = None,
    # The manager's Connection declares string connection options.
    conn_kwargs: Mapping[str, str] | Mapping[ConnectionOptions, str] | None = None,
    *,
    autocommit: bool = False,
    backend: DbapiBackend | None = None,
) -> Connection:
    """Connect to BigQuery through the ADBC DB-API facade.

    ``uri`` and ``db_kwargs`` configure the database, ``conn_kwargs`` the
    connection, and ``autocommit`` the transaction mode. Leaving a ``with`` block
    closes the connection without committing. ``backend`` is passed to
    :class:`adbc_driver_manager.dbapi.Connection`, whose default converts with PyArrow.
    """

    connection_options: dict[str, str] = dict(conn_kwargs.items()) if conn_kwargs else {}
    if ConnectionOptions.AUTOCOMMIT in connection_options:
        raise ValueError(
            f"conn_kwargs must not set {ConnectionOptions.AUTOCOMMIT.value!r}; pass autocommit= to connect() instead"
        )

    db = None
    conn = None
    try:
        db = _driver_connect(uri, db_kwargs=db_kwargs)
        conn = adbc_driver_manager.AdbcConnection(db, **connection_options)
        return Connection(
            db,
            conn,
            conn_kwargs=connection_options,
            autocommit=autocommit,
            backend=backend,
        )
    except BaseException:
        # Close every handle opened so far, and re-raise the original failure
        # (including KeyboardInterrupt) even if a close fails.
        for resource in (conn, db):
            if resource is not None:
                with suppress(BaseException):
                    resource.close()
        raise


__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]
