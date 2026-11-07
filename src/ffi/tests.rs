use super::*;
use crate::ffi::abi::ADBC_ERROR_INIT;
use crate::ffi::test_support::{error_message, null, release_error};

/// Every function slot in the vtable, named, in header order. A `None` slot is a null pointer the
/// host will call, so the whole table is swept.
fn slots(table: &AdbcDriver) -> Vec<(&'static str, bool)> {
    macro_rules! slots {
        ($($field:ident),+ $(,)?) => {
            vec![$((stringify!($field), table.$field.is_some())),+]
        };
    }
    slots![
        // --- ADBC 1.0.0 ---
        DatabaseInit,
        DatabaseNew,
        DatabaseSetOption,
        DatabaseRelease,
        ConnectionCommit,
        ConnectionGetInfo,
        ConnectionGetObjects,
        ConnectionGetTableSchema,
        ConnectionGetTableTypes,
        ConnectionInit,
        ConnectionNew,
        ConnectionSetOption,
        ConnectionReadPartition,
        ConnectionRelease,
        ConnectionRollback,
        StatementBind,
        StatementBindStream,
        StatementExecuteQuery,
        StatementExecutePartitions,
        StatementGetParameterSchema,
        StatementNew,
        StatementPrepare,
        StatementRelease,
        StatementSetOption,
        StatementSetSqlQuery,
        StatementSetSubstraitPlan,
        // --- ADBC 1.1.0 ---
        ErrorGetDetailCount,
        ErrorGetDetail,
        ErrorFromArrayStream,
        DatabaseGetOption,
        DatabaseGetOptionBytes,
        DatabaseGetOptionDouble,
        DatabaseGetOptionInt,
        DatabaseSetOptionBytes,
        DatabaseSetOptionDouble,
        DatabaseSetOptionInt,
        ConnectionCancel,
        ConnectionGetOption,
        ConnectionGetOptionBytes,
        ConnectionGetOptionDouble,
        ConnectionGetOptionInt,
        ConnectionGetStatistics,
        ConnectionGetStatisticNames,
        ConnectionSetOptionBytes,
        ConnectionSetOptionDouble,
        ConnectionSetOptionInt,
        StatementCancel,
        StatementExecuteSchema,
        StatementGetOption,
        StatementGetOptionBytes,
        StatementGetOptionDouble,
        StatementGetOptionInt,
        StatementSetOptionBytes,
        StatementSetOptionDouble,
        StatementSetOptionInt,
    ]
}

/// How many of [`slots`] a 1.0.0 caller's allocation covers.
const PREFIX_SLOTS: usize =
    (ADBC_DRIVER_1_0_0_SIZE - 3 * size_of::<*const c_void>()) / size_of::<*const c_void>();

/// Initialize a zeroed, full-size vtable at `version`, returning the status, the table and the
/// message of any error exported.
fn init_zeroed(version: c_int) -> (AdbcStatusCode, AdbcDriver, Option<String>) {
    let mut table = std::mem::MaybeUninit::<AdbcDriver>::zeroed();
    let mut error = ADBC_ERROR_INIT;
    // SAFETY: The table allocation covers every ABI version, and `error` is initialized and
    // writable.
    let status =
        unsafe { AdbcDriverBigqueryInit(version, table.as_mut_ptr().cast(), &raw mut error) };
    let message = error_message(&error);
    release_error(&mut error);
    // SAFETY: Zero is valid for every field, and init writes only valid function pointers.
    (status, unsafe { table.assume_init() }, message)
}

#[test]
fn populates_a_1_1_0_vtable() {
    let (status, table, _) = init_zeroed(ADBC_VERSION_1_1_0);
    assert_eq!(status, ADBC_STATUS_OK);
    let slots = slots(&table);
    // The list must cover every field but `private_data`, `private_manager` and `release`, or a
    // slot added to `AdbcDriver` and missing from `vtable()` would go unchecked.
    assert_eq!(
        slots.len(),
        size_of::<AdbcDriver>() / size_of::<*const c_void>() - 3,
        "every AdbcDriver function field must be listed"
    );
    for (name, populated) in slots {
        assert!(populated, "{name} is a null pointer the host would call");
    }
}

/// A 1.0.0 caller gets every slot it knows about, and the tail is left alone.
#[test]
fn populates_every_slot_a_1_0_0_caller_can_see() {
    let (status, table, _) = init_zeroed(ADBC_VERSION_1_0_0);
    assert_eq!(status, ADBC_STATUS_OK);
    for (index, (name, populated)) in slots(&table).into_iter().enumerate() {
        assert_eq!(populated, index < PREFIX_SLOTS, "{name}");
    }
}

/// `vtable()` is a struct literal, and the generic option entry points take their handle and value
/// types from each slot's signature, so only slots sharing a signature can be transposed
/// unnoticed. Check the database's `(handle, error)` lifecycle slots by their effect.
/// `ConnectionCommit`, `ConnectionRollback` and `ConnectionCancel` answer a pre-init connection
/// alike, so this cannot tell them apart.
#[test]
fn the_lifecycle_slots_do_what_their_names_say() {
    let (_, table, _) = init_zeroed(ADBC_VERSION_1_1_0);
    let mut database = abi::AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    // SAFETY: The callback comes from the initialized table and receives a writable, empty
    // database handle.
    let status = unsafe { table.DatabaseNew.unwrap()(&raw mut database, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(!database.private_data.is_null(), "New allocates");

    for (key, value) in [
        (c"uri", c"bigquery:///a-project"),
        (c"adbc.bigquery.sql.auth_type", c"anonymous"),
    ] {
        assert_eq!(
            // SAFETY: The allocated handle and both static C strings remain live for the call.
            unsafe {
                table.DatabaseSetOption.unwrap()(
                    &raw mut database,
                    key.as_ptr(),
                    value.as_ptr(),
                    null(),
                )
            },
            ADBC_STATUS_OK
        );
    }
    // SAFETY: The handle owns its allocation; the optional error output may be null.
    let status = unsafe { table.DatabaseInit.unwrap()(&raw mut database, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    // SAFETY: DatabaseNew installed a `database::State` in this live slot.
    let ready = unsafe {
        handle::with_locked::<database::State, _>(
            &raw const database.private_data,
            "database",
            |state| Ok(matches!(state, handle::Staged::Ready(_))),
        )
    };
    assert!(ready.unwrap(), "Init initializes");

    // SAFETY: The handle owns its driver allocation and no state guard survives this release.
    let status = unsafe { table.DatabaseRelease.unwrap()(&raw mut database, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(database.private_data.is_null(), "Release frees");
}

/// A 1.0.0 caller allocates only the prefix, so the bytes after it must be left untouched.
#[test]
fn writes_only_the_prefix_for_a_1_0_0_caller() {
    let mut buffer = [0xAA_u8; size_of::<AdbcDriver>()];
    // SAFETY: The byte buffer covers the 1.0 prefix copied as bytes (no typed access or alignment
    // needed); the error output may be null.
    let status =
        unsafe { AdbcDriverBigqueryInit(ADBC_VERSION_1_0_0, buffer.as_mut_ptr().cast(), null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    let (prefix, tail) = buffer.split_at(ADBC_DRIVER_1_0_0_SIZE);
    assert!(
        tail.iter().all(|byte| *byte == 0xAA),
        "the 1.1.0 tail is untouched"
    );
    assert!(
        prefix.iter().any(|byte| *byte != 0xAA),
        "the 1.0.0 prefix is populated"
    );
}

/// The same, through an allocation that ends at the prefix, so Miri reports any access by `init`
/// or the driver's `release` that assumes the 1.1.0 tail exists.
#[test]
fn a_1_0_0_caller_can_init_and_release_a_prefix_sized_vtable() {
    let layout =
        std::alloc::Layout::from_size_align(ADBC_DRIVER_1_0_0_SIZE, align_of::<AdbcDriver>())
            .unwrap();
    // SAFETY: The nonzero layout describes the exact size and alignment required for the ABI
    // prefix.
    let driver = unsafe { std::alloc::alloc_zeroed(layout) }.cast::<AdbcDriver>();
    assert!(!driver.is_null());
    // SAFETY: `driver` is non-null, aligned, and writable for the entire requested 1.0 prefix.
    let status = unsafe { AdbcDriverBigqueryInit(ADBC_VERSION_1_0_0, driver.cast(), null()) };
    assert_eq!(status, ADBC_STATUS_OK);

    // SAFETY: Successful init initialized release within the allocated prefix; field access creates
    // no full-table reference.
    let release = unsafe { (*driver).release }.unwrap();
    // SAFETY: The initialized callback accesses only the allocated prefix, and no driver handles
    // were created.
    assert_eq!(unsafe { release(driver, null()) }, ADBC_STATUS_OK);
    // SAFETY: Release leaves this prefix field initialized; the allocation remains live.
    assert!(unsafe { (*driver).release }.is_none());
    // SAFETY: This pointer was allocated with exactly `layout` and is no longer used after
    // deallocation.
    unsafe { std::alloc::dealloc(driver.cast(), layout) };
}

/// adbc.h tells a caller to retry at another revision on `NOT_IMPLEMENTED`, so an unknown version
/// must not be reported as a bad argument.
#[test]
fn rejects_an_unknown_version_as_not_implemented() {
    let (status, _, message) = init_zeroed(999);
    assert_eq!(status, abi::ADBC_STATUS_NOT_IMPLEMENTED);
    let message = message.unwrap();
    assert!(
        message.contains("unsupported ADBC version 999"),
        "{message}"
    );
}

#[test]
fn rejects_a_null_vtable() {
    let mut error = ADBC_ERROR_INIT;
    // SAFETY: Init explicitly rejects a null table; the initialized error destination is writable.
    let status = unsafe { AdbcDriverBigqueryInit(ADBC_VERSION_1_1_0, null(), &raw mut error) };
    assert_eq!(status, abi::ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(
        error_message(&error).as_deref(),
        Some("driver must not be null")
    );
    release_error(&mut error);
}

#[test]
fn releasing_the_driver_twice_reports_invalid_state() {
    let (_, mut table, _) = init_zeroed(ADBC_VERSION_1_1_0);
    let release = table.release.unwrap();
    // SAFETY: The table owns the initialized callback and no database/connection handles were
    // created.
    assert_eq!(unsafe { release(&raw mut table, null()) }, ADBC_STATUS_OK);
    let mut error = ADBC_ERROR_INIT;
    assert_eq!(
        // SAFETY: The table remains allocated with its release field cleared; this callback checks
        // the released state. The initialized error destination is writable.
        unsafe { release(&raw mut table, &raw mut error) },
        abi::ADBC_STATUS_INVALID_STATE
    );
    assert_eq!(
        error_message(&error).as_deref(),
        Some("driver is already released")
    );
    release_error(&mut error);
}
