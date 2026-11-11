//! The ADBC C ABI export layer.
//!
//! * [`abi`]: the C header declarations.
//! * [`error`]: writes driver errors into the caller's `AdbcError` at its ABI revision.
//! * [`guard`]: panic containment and pointer/string conversions.
//! * [`handle`]: what lives behind `private_data`, and how calls reach it and survive a racing
//!   `Release`.
//! * [`options`]: the lifecycle and option entry points every object shares, generic over the
//!   handle type.
//! * [`database`], [`connection`], [`statement`]: the remaining entry points, each a thin
//!   translation onto the driver's own types.
//! * [`import`], [`stream`]: bound Arrow input checked before import, and result streams
//!   exported with panics contained.
//!
//! Nothing outside this module knows the driver is exported over C.
//!
//! # Why not `adbc_ffi`
//!
//! The ABI is hand-written because `adbc_ffi`'s generic exporter makes lifecycle decisions this
//! driver cannot override. It keeps one process-wide panic flag, so a single panic fails every
//! later call on every object, `Release` and cancellation included; here a panic poisons only the
//! objects whose state locks the call held. It requires `Default` and constructs a driver in each
//! `AdbcDatabaseInit`, which would mean a Tokio runtime per database and no way to report a failed
//! build; here every database shares one driver, and a failed build returns
//! `ADBC_STATUS_INTERNAL`. It also refuses every ADBC version but 1.1.0, while [`init`] serves
//! 1.0.0 callers too. `adbc_ffi` stays a dev-dependency only because the round-trip tests'
//! `ManagedDriver::load_static` takes its init function type.
//!
//! # Shared FFI safety contracts
//!
//! Every entry point requires non-null pointers to address live storage for the advertised type
//! and ABI revision, aligned wherever typed access requires it. Non-null input strings are
//! NUL-terminated and pointer/length pairs cover their length. Borrowed strings, pointer arrays
//! and buffers stay readable and unmodified for the call, each within one allocation and spanning
//! at most `isize::MAX` bytes. Writable outputs are exclusively accessible and do not overlap
//! inputs or each other. A non-null `error` is initialized or previously populated storage
//! satisfying [`error::export_error`].
//!
//! Handle `private_data` is null or installed by this driver's matching `New` entry point, and only
//! the driver accesses that field while calls are in flight. The handle struct stays allocated
//! for every call on it. Calls on one object may overlap, `Release` included: [`handle`] orders
//! them, and a call that loses a race with `Release` reports `INVALID_STATE`. Arrow inputs retain
//! their producer's allocation, callback and lifetime contracts even when their shape or values
//! are checked here; non-null pointers alone never establish readable buffer capacity.

mod abi;
mod connection;
mod database;
mod error;
mod guard;
mod handle;
mod import;
mod options;
#[cfg(test)]
mod roundtrip;
mod statement;
mod stream;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::ffi::{c_int, c_void};

use abi::{
    ADBC_DRIVER_1_0_0_SIZE, ADBC_STATUS_OK, ADBC_VERSION_1_0_0, ADBC_VERSION_1_1_0, AdbcDriver,
    AdbcError, AdbcStatusCode,
};
use handle::finish;

use crate::error::{invalid_state, not_implemented};

/// Build the vtable. A `None` slot reads as "not implemented" under the ABI. The generic
/// [`options`] entry points take their handle type from the slot's signature.
fn vtable() -> AdbcDriver {
    AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_driver),

        // --- ADBC 1.0.0 ---
        DatabaseInit: Some(database::database_init),
        DatabaseNew: Some(options::new_handle),
        DatabaseSetOption: Some(options::set_option),
        DatabaseRelease: Some(options::release_handle),
        ConnectionCommit: Some(connection::connection_commit),
        ConnectionGetInfo: Some(connection::connection_get_info),
        ConnectionGetObjects: Some(connection::connection_get_objects),
        ConnectionGetTableSchema: Some(connection::connection_get_table_schema),
        ConnectionGetTableTypes: Some(connection::connection_get_table_types),
        ConnectionInit: Some(connection::connection_init),
        ConnectionNew: Some(options::new_handle),
        ConnectionSetOption: Some(options::set_option),
        ConnectionReadPartition: Some(connection::connection_read_partition),
        ConnectionRelease: Some(options::release_handle),
        ConnectionRollback: Some(connection::connection_rollback),
        StatementBind: Some(statement::statement_bind),
        StatementBindStream: Some(statement::statement_bind_stream),
        StatementExecuteQuery: Some(statement::statement_execute_query),
        StatementExecutePartitions: Some(statement::statement_execute_partitions),
        StatementGetParameterSchema: Some(statement::statement_get_parameter_schema),
        StatementNew: Some(statement::statement_new),
        StatementPrepare: Some(statement::statement_prepare),
        StatementRelease: Some(options::release_handle),
        StatementSetOption: Some(options::set_option),
        StatementSetSqlQuery: Some(statement::statement_set_sql_query),
        StatementSetSubstraitPlan: Some(statement::statement_set_substrait_plan),

        // --- ADBC 1.1.0 ---
        ErrorGetDetailCount: Some(error::error_get_detail_count),
        ErrorGetDetail: Some(error::error_get_detail),
        ErrorFromArrayStream: Some(stream::error_from_array_stream),
        DatabaseGetOption: Some(options::get_option),
        DatabaseGetOptionBytes: Some(options::get_option_bytes),
        DatabaseGetOptionDouble: Some(options::get_option_double),
        DatabaseGetOptionInt: Some(options::get_option_int),
        DatabaseSetOptionBytes: Some(options::set_option_bytes),
        DatabaseSetOptionDouble: Some(options::set_option_double),
        DatabaseSetOptionInt: Some(options::set_option_int),
        ConnectionCancel: Some(connection::connection_cancel),
        ConnectionGetOption: Some(options::get_option),
        ConnectionGetOptionBytes: Some(options::get_option_bytes),
        ConnectionGetOptionDouble: Some(options::get_option_double),
        ConnectionGetOptionInt: Some(options::get_option_int),
        ConnectionGetStatistics: Some(connection::connection_get_statistics),
        ConnectionGetStatisticNames: Some(connection::connection_get_statistic_names),
        ConnectionSetOptionBytes: Some(options::set_option_bytes),
        ConnectionSetOptionDouble: Some(options::set_option_double),
        ConnectionSetOptionInt: Some(options::set_option_int),
        StatementCancel: Some(statement::statement_cancel),
        StatementExecuteSchema: Some(statement::statement_execute_schema),
        StatementGetOption: Some(options::get_option),
        StatementGetOptionBytes: Some(options::get_option_bytes),
        StatementGetOptionDouble: Some(statement::statement_get_option_double),
        StatementGetOptionInt: Some(options::get_option_int),
        StatementSetOptionBytes: Some(options::set_option_bytes),
        StatementSetOptionDouble: Some(options::set_option_double),
        StatementSetOptionInt: Some(options::set_option_int),
    }
}

/// Release the driver by clearing the `release` slot, so a second release reports `INVALID_STATE`.
///
/// A null `driver` is a no-op, unlike a null object passed to `*Release`. adbc.h leaves both
/// unspecified; driver release usually runs on teardown paths that may not know whether they
/// initialized a driver.
///
/// # Safety
/// The shared FFI safety contracts above apply.
/// `driver` must be null or point to an `AdbcDriver` this driver initialized, at either revision.
// Guard-exempt: touches only the caller's own struct with non-panicking pointer operations; the
// error path is contained inside `finish`.
unsafe extern "C" fn release_driver(
    driver: *mut AdbcDriver,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    if driver.is_null() {
        return ADBC_STATUS_OK;
    }
    // SAFETY: the caller grants exclusive access and both revisions contain release. Borrow only
    // that field so a 1.0.0 allocation does not need to contain the 1.1.0 tail.
    let release = unsafe { &mut (*driver).release };
    if release.take().is_none() {
        // SAFETY: the caller provides initialized, exclusively writable error storage or null.
        return unsafe { finish(Err(invalid_state("driver is already released")), error) };
    }
    ADBC_STATUS_OK
}

/// Populate the caller's vtable at the revision it asked for.
///
/// # Safety
/// The shared FFI safety contracts above apply.
/// `driver` must point to an `AdbcDriver` allocation of at least the size implied by `version`.
/// Initialization permits byte-aligned storage; later typed access to the vtable requires its
/// natural alignment. Unsupported versions are rejected without accessing the vtable.
// Guard-exempt (also covers the two exported wrappers below): registers a fork handler and builds
// a vtable of plain function pointers, copying it with non-panicking pointer operations; the error
// paths are contained inside `finish`.
unsafe fn init(version: c_int, driver: *mut c_void, error: *mut AdbcError) -> AdbcStatusCode {
    if driver.is_null() {
        // SAFETY: the caller provides initialized, exclusively writable error storage or null.
        return unsafe { finish(Err(guard::missing_argument("driver")), error) };
    }

    // Before the caller can create any handle.
    handle::track_forks();
    let table = vtable();
    match version {
        // SAFETY: the caller advertises writable space for the complete 1.1.0 vtable.
        ADBC_VERSION_1_1_0 => unsafe {
            std::ptr::write_unaligned(driver.cast::<AdbcDriver>(), table);
        },
        ADBC_VERSION_1_0_0 => {
            // SAFETY: the caller supplies a writable 1.0.0 prefix disjoint from our local table.
            // Copy only that prefix; the omitted function pointers own no resources.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (&raw const table).cast::<u8>(),
                    driver.cast::<u8>(),
                    ADBC_DRIVER_1_0_0_SIZE,
                );
            }
        }
        _ => {
            // The header tells callers to retry at a lower revision on NOT_IMPLEMENTED, so an
            // unsupported version must not be reported as a bad argument.
            // SAFETY: the caller provides initialized, exclusively writable error storage or null.
            return unsafe {
                finish(
                    Err(not_implemented(format!(
                        "unsupported ADBC version {version}; this driver implements \
                         {ADBC_VERSION_1_0_0} and {ADBC_VERSION_1_1_0}"
                    ))),
                    error,
                )
            };
        }
    }
    ADBC_STATUS_OK
}

/// The driver's entry point, named after the shared library.
///
/// # Unloading
///
/// The shared object must never be unloaded. The first `AdbcDatabaseInit` starts a process-global
/// Tokio runtime (`database::shared_driver`) whose worker threads live for the process;
/// `AdbcDriver.release` stops nothing, so a `dlclose` leaves those threads executing unmapped code
/// and crashes with SIGSEGV. The reference C driver manager never unloads
/// (`ManagedLibrary::Release` is a no-op), but other hosts may.
///
/// # Safety
/// See [`init`].
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcDriverBigqueryInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: this entry point forwards the caller's vtable size, output ownership and error
    // contract.
    unsafe { init(version, driver, error) }
}

/// The generic entry point, used by driver managers that were not told a symbol name.
///
/// # Safety
/// See [`init`]. The unloading constraint on [`AdbcDriverBigqueryInit`] applies here too.
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcDriverInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: this entry point forwards the caller's vtable size, output ownership and error
    // contract.
    unsafe { init(version, driver, error) }
}
