//! `AdbcConnection*` entry points.

use std::collections::HashSet;
use std::ffi::{c_char, c_int};

use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection};
use adbc_core::{Connection, Database};
use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use super::abi::{AdbcConnection, AdbcDatabase, AdbcError, AdbcStatusCode};
use super::guard::{byte_slice, optional_str, optional_str_list, required_output, required_str};
use super::handle::{Staged, install_cancel_handle};
use super::options::{FfiHandle, slot_of, with_handle, with_state, with_stream};
use super::stream::write_schema;
use crate::connection::BigQueryConnection;
use crate::error::invalid_argument;

pub(super) const KIND: &str = "connection";

pub(super) type State = Staged<OptionConnection, BigQueryConnection>;

// SAFETY: `AdbcConnection` is repr(C) with `private_data` first, and only `State` is installed there.
unsafe impl FfiHandle for AdbcConnection {
    type State = State;
    const KIND: &'static str = KIND;
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` must be null or point to a valid `AdbcConnection`, and `database` must be null or
/// point to a valid, initialized `AdbcDatabase` that outlives the connection.
pub(super) unsafe extern "C" fn connection_init(
    connection: *mut AdbcConnection,
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller keeps both typed handles live and grants error output access.
    unsafe {
        with_state(connection, error, |state| {
            let options = state.pending(KIND)?;
            // Building the connection can call BigQuery, so it uses a snapshot rather than
            // holding the database lock, which would serialize concurrent opens and block the
            // database's getters. `new_connection_with_opts` owns the option ordering and
            // validation rules.
            let ready = with_handle(database, |database| {
                Ok(database.ready(super::database::KIND)?.snapshot())
            })?
            .new_connection_with_opts(options.clone())?;
            // `connection_cancel` uses this handle without the dispatch turn. Install it before
            // publishing the ready state, since an operation can start as soon as the connection
            // is usable.
            install_cancel_handle::<State>(slot_of(connection), KIND, ready.get_cancel_handle())?;
            *state = State::Ready(ready);
            Ok(())
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_commit(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller provides a live connection slot and initialized, writable error output or
    // null.
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.commit()) }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_rollback(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller provides a live connection slot and initialized, writable error output or
    // null.
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.rollback()) }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_cancel(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Bypasses dispatch through the handle installed at init; see `handle::cancel`. With no
    // operation in flight it reports `InvalidState`, as ADBC requires.
    // SAFETY: The caller keeps the connection struct live and grants error output access.
    unsafe { super::handle::cancel::<State>(slot_of(connection), KIND, error) }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call, and `info_codes` must be null or
/// point to `info_codes_length` initialized `u32`s.
pub(super) unsafe extern "C" fn connection_get_info(
    connection: *mut AdbcConnection,
    info_codes: *const u32,
    info_codes_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live handle, readable u32 allocation and disjoint writable
    // stream/error outputs.
    unsafe {
        with_stream(connection, out, error, |state| {
            // A null array requests every code; an empty non-null array requests none.
            let codes = if info_codes.is_null() {
                None
            } else {
                Some(
                    std::slice::from_raw_parts(info_codes, info_codes_length)
                        .iter()
                        .copied()
                        .map(InfoCode::from)
                        .collect::<HashSet<_>>(),
                )
            };
            state.ready(KIND)?.get_info(codes)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call, the string arguments must be null or
/// NUL-terminated, and `table_type` must be null or a NULL-terminated array of such strings.
pub(super) unsafe extern "C" fn connection_get_objects(
    connection: *mut AdbcConnection,
    depth: c_int,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    table_type: *const *const c_char,
    column_name: *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller keeps strings and the terminated pointer list live and grants stream/error
    // output access.
    unsafe {
        with_stream(connection, out, error, |state| {
            // `ObjectDepth::try_from` reports `InvalidData` and names no valid depths, but this
            // is a bad argument. adbc.h defines ADBC_OBJECT_DEPTH_COLUMNS as ALL, hence no 4.
            let depth = ObjectDepth::try_from(depth).map_err(|_| {
                invalid_argument(format!(
                    "AdbcConnectionGetObjects depth {depth} is not a valid ADBC object depth; \
                     use 0 (all, including columns), 1 (catalogs), 2 (db_schemas), or 3 (tables)"
                ))
            })?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;
            let table_type = optional_str_list(table_type, "table_type")?;
            let column_name = optional_str(column_name, "column_name")?;

            state.ready(KIND)?.get_objects(
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            )
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `schema` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_table_schema(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies live strings/handle and disjoint writable schema/error outputs.
    unsafe {
        with_state(connection, error, |state| {
            required_output(schema, "schema")?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            // Unlike in GetObjects, the table name is a lookup key, not a filter: it is required.
            let table_name = required_str(table_name, "table_name")?;

            let found = state
                .ready(KIND)?
                .get_table_schema(catalog, db_schema, table_name)?;
            write_schema(schema, &found)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_table_types(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live connection and disjoint writable stream/error outputs.
    unsafe {
        with_stream(connection, out, error, |state| {
            state.ready(KIND)?.get_table_types()
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_statistics(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    approximate: c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies live strings/handle and disjoint writable stream/error outputs.
    unsafe {
        with_stream(connection, out, error, |state| {
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;

            state.ready(KIND)?.get_statistics(
                catalog,
                db_schema,
                table_name,
                // The header declares a C boolean as `char`; anything non-zero is true.
                approximate != 0,
            )
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_statistic_names(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live connection and disjoint writable stream/error outputs.
    unsafe {
        with_stream(connection, out, error, |state| {
            state.ready(KIND)?.get_statistic_names()
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` and `out` must be null or valid for the call, and `serialized_partition` must be
/// null or point to `serialized_length` initialized bytes.
pub(super) unsafe extern "C" fn connection_read_partition(
    connection: *mut AdbcConnection,
    serialized_partition: *const u8,
    serialized_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a readable partition allocation and disjoint writable
    // stream/error outputs.
    unsafe {
        with_stream(connection, out, error, |state| {
            let partition = byte_slice(
                serialized_partition,
                serialized_length,
                "serialized_partition",
            )?;
            state.ready(KIND)?.read_partition(partition)
        })
    }
}

#[cfg(test)]
mod tests;
