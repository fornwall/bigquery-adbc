use std::ffi::CString;

use adbc_core::error::Status;
use adbc_core::options::OptionValue;

use super::super::abi::{
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND,
    ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK,
};
use super::super::handle::with_locked;
use super::super::options::with_handle;
use super::*;
use crate::ffi::abi::AdbcStatement;
use crate::ffi::options::{
    get_option, get_option_bytes, get_option_double, get_option_int, new_handle, release_handle,
    set_option, set_option_bytes, set_option_double, set_option_int,
};
use crate::ffi::test_support::{
    allocated, assert_refused, call, database_at, get, hold_the_dispatch_turn,
    initialized_database, null, release, set,
};
use crate::test_support::http::capture_requests_gated;

/// A handle as `AdbcConnectionNew` leaves it: allocated and not yet initialized. Argument
/// validation and the initialization check run first, so tests need no network.
fn pending() -> AdbcConnection {
    allocated()
}

fn set_int(connection: &mut AdbcConnection, key: &str, value: i64) -> AdbcStatusCode {
    let key = CString::new(key).unwrap();
    // SAFETY: The handle is exclusively borrowed and the key outlives the call.
    unsafe { set_option_int(connection, key.as_ptr(), value, null()) }
}

fn init(connection: &mut AdbcConnection, database: *mut AdbcDatabase) -> AdbcStatusCode {
    // SAFETY: The connection is exclusively borrowed; the database is null or a live handle the
    // test keeps for the connection's lifetime.
    unsafe { connection_init(connection, database, null()) }
}

/// Copy the buffered options out of a not-yet-initialized handle, to assert on what the
/// setters recorded.
fn buffered(connection: &AdbcConnection) -> Vec<(OptionConnection, OptionValue)> {
    // SAFETY: Test callers supply a live handle with a `State` installed.
    unsafe {
        with_locked::<State, _>(
            &raw const connection.private_data,
            KIND,
            |state| match state {
                State::Pending(options) => Ok(options.clone()),
                State::Ready(_) => panic!("expected a pending connection"),
            },
        )
    }
    .unwrap()
}

#[test]
fn a_null_handle_is_a_bad_argument_not_a_crash() {
    // SAFETY: Both entry points explicitly reject a null handle; the error output is optional.
    let statuses = unsafe {
        [
            new_handle::<AdbcConnection>(null(), null()),
            release_handle::<AdbcConnection>(null(), null()),
            connection_commit(null(), null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_ARGUMENT; 3]);
    // An unpopulated `private_data` is an object used out of order, not a bad pointer.
    let mut connection = AdbcConnection::default();
    // SAFETY: The handle structure is writable; commit checks its null private data.
    let status = unsafe { connection_commit(&raw mut connection, null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
}

/// A failed `Init` must keep the buffered options so the offending one can be corrected and
/// `Init` retried, rather than silently reinitializing from an empty buffer.
#[test]
fn a_failed_init_keeps_the_buffered_options_for_a_retry() {
    let mut database = initialized_database();
    let mut connection = pending();
    let prefetch = "bigquery.query.prefetch_concurrency";
    assert_eq!(
        set(&mut connection, "bigquery.query.unordered_read", "true").0,
        ADBC_STATUS_OK
    );
    assert_eq!(set_int(&mut connection, prefetch, 4), ADBC_STATUS_OK);
    for (key, value) in [
        ("adbc.bigquery.sql.query.prefetch_concurrency", "banana"),
        // Buffered without validation; rejected when Init applies it.
        ("adbc.connection.autocommit", "banana"),
    ] {
        assert_eq!(set(&mut connection, key, value).0, ADBC_STATUS_OK);
    }
    assert_ne!(init(&mut connection, &raw mut database), ADBC_STATUS_OK);

    // Correct the string alias through its canonical name and integer setter.
    assert_eq!(set_int(&mut connection, prefetch, 4), ADBC_STATUS_OK);
    let autocommit = set(&mut connection, "adbc.connection.autocommit", "true");
    assert_eq!(autocommit.0, ADBC_STATUS_OK);
    assert_eq!(init(&mut connection, &raw mut database), ADBC_STATUS_OK);

    let mut value = 0_i64;
    let key = CString::new(prefetch).unwrap();
    // SAFETY: The connection and key remain live; `value` is a writable scalar output.
    let status =
        unsafe { get_option_int(&raw mut connection, key.as_ptr(), &raw mut value, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(
        value, 4,
        "a corrected alias must replace its previously buffered value"
    );
    assert_eq!(
        get(&mut connection, "bigquery.query.unordered_read").unwrap(),
        "true",
        "unmodified options must survive a failed Init"
    );

    release(&mut connection);
    release(&mut database);
}

#[test]
fn options_set_before_init_are_buffered_in_order_with_last_write_winning() {
    let mut connection = pending();
    // Recognized keys with their types. Only the values wait for `Init` to be checked.
    for value in ["true", "false"] {
        let status = set(&mut connection, "adbc.connection.autocommit", value).0;
        assert_eq!(status, ADBC_STATUS_OK);
    }
    let prefetch = set_int(&mut connection, "bigquery.query.prefetch_concurrency", 4096);
    assert_eq!(prefetch, ADBC_STATUS_OK);
    // A value the key's validator will reject is still buffered; checking it needs the ordering
    // `Init` applies.
    let unordered = set(&mut connection, "bigquery.query.unordered_read", "perhaps");
    assert_eq!(unordered.0, ADBC_STATUS_OK);

    let entries = buffered(&connection);
    assert_eq!(entries.len(), 3, "the repeated key must not add an entry");
    assert_eq!(entries[0].0, OptionConnection::AutoCommit);
    // `OptionValue` is not `PartialEq`, so the payloads are matched structurally.
    assert!(matches!(&entries[0].1, OptionValue::String(value) if value == "false"));
    assert!(matches!(entries[1].1, OptionValue::Int(4096)));
    assert!(matches!(&entries[2].1, OptionValue::String(value) if value == "perhaps"));
    release(&mut connection);
}

/// A pre-`Init` set with the wrong type fails at that call, with the live setter's error, rather
/// than at `AdbcConnectionInit`.
///
/// A base-10 string stays a legal integer, since some transports carry only strings (dbt
/// 2.0's does).
#[test]
fn a_pre_init_set_of_the_wrong_value_type_is_refused_by_the_call_that_carried_it() {
    let mut connection = pending();
    let c = &raw mut connection;
    let string_key = c"adbc.connection.autocommit".as_ptr();
    let int_key = c"bigquery.query.prefetch_concurrency".as_ptr();
    // SAFETY: Each call gets the live handle, static keys, a byte literal holding the two bytes
    // passed, and a writable error.
    let refusals = unsafe {
        [
            (
                call(|error| set_option_int(c, string_key, 1, error)),
                "BigQuery option adbc.connection.autocommit requires a string value, received \
                 integer",
            ),
            (
                call(|error| set_option_double(c, int_key, 0.5, error)),
                "BigQuery option bigquery.query.prefetch_concurrency requires an integer value, \
                 received double",
            ),
            (
                call(|error| set_option_bytes(c, string_key, b"xy".as_ptr(), 2, error)),
                "BigQuery option adbc.connection.autocommit requires a string value, received \
                 bytes",
            ),
        ]
    };
    for (refusal, expected) in refusals {
        assert_eq!(
            refusal,
            (ADBC_STATUS_INVALID_ARGUMENT, Some(expected.to_owned()))
        );
    }

    let base_ten = set(
        &mut connection,
        "bigquery.query.prefetch_concurrency",
        "4096",
    );
    assert_eq!(base_ten.0, ADBC_STATUS_OK);
    assert_eq!(buffered(&connection).len(), 1, "only the accepted set");
    release(&mut connection);
}

#[test]
fn setters_reject_a_null_or_non_utf8_key_or_value() {
    let mut connection = pending();
    let c = &raw mut connection;
    let key = c"adbc.connection.autocommit".as_ptr();
    let value = c"true".as_ptr();
    // 0xFF cannot start a UTF-8 sequence, so this is a NUL-terminated non-UTF-8 string.
    let invalid = [0xFF_u8, 0x00];
    let invalid = invalid.as_ptr().cast::<c_char>();
    // SAFETY: The handle and every non-null string are live and NUL-terminated; each call's null
    // or non-UTF-8 argument, or null byte buffer with a nonzero length, is rejected.
    let statuses = unsafe {
        [
            set_option(c, null(), value, null()),
            set_option(c, key, null(), null()),
            set_option(c, invalid, value, null()),
            set_option_int(c, invalid, 1, null()),
            set_option_bytes(c, key, null(), 3, null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_ARGUMENT; 5]);
    assert!(
        buffered(&connection).is_empty(),
        "a rejected setter must not record anything"
    );
    release(&mut connection);
}

/// adbc.h documents only `ADBC_STATUS_NOT_FOUND` for the option getters, so a connection with
/// only buffered options reports the key as absent.
#[test]
fn getters_report_a_pre_init_connection_option_as_not_found() {
    let mut connection = pending();
    let c = &raw mut connection;
    let key = c"adbc.connection.autocommit".as_ptr();
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);
    // SAFETY: The handle and key are live; the scalar outputs are writable, and a null buffer
    // with a zero length is a size query.
    let statuses = unsafe {
        [
            get_option(c, key, null(), &raw mut length, null()),
            get_option_bytes(c, key, null(), &raw mut length, null()),
            get_option_int(c, key, &raw mut integer, null()),
            get_option_double(c, key, &raw mut double, null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_NOT_FOUND; 4]);
    // A bad key is rejected before the state is consulted, so it wins over the missing option.
    // SAFETY: The handle and scalar output are live; the null key is explicitly rejected.
    let status = unsafe { get_option_int(c, null(), &raw mut integer, null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    release(&mut connection);
}

#[test]
fn transaction_cancel_and_init_entry_points_check_their_state() {
    let mut connection = pending();
    let c = &raw mut connection;
    // SAFETY: The pending connection is live and no state guard is held; cancel checks whether a
    // cancel handle is installed, and init explicitly checks the null database.
    let statuses = unsafe {
        [
            connection_commit(c, null()),
            connection_rollback(c, null()),
            connection_cancel(c, null()),
            connection_init(c, null(), null()),
        ]
    };
    // A null database is a bad argument, like any other null handle.
    assert_eq!(
        statuses,
        [
            ADBC_STATUS_INVALID_STATE,
            ADBC_STATUS_INVALID_STATE,
            ADBC_STATUS_INVALID_STATE,
            ADBC_STATUS_INVALID_ARGUMENT
        ]
    );
    release(&mut connection);
}

#[test]
fn metadata_streams_require_an_initialized_connection() {
    let mut connection = pending();
    let c = &raw mut connection;
    let mut out = FFI_ArrowArrayStream::empty();
    let out = &raw mut out;
    // SAFETY: The pending connection and stream output are live; optional filters and info codes
    // may be null, and the partition literal covers its stated four bytes.
    let statuses = unsafe {
        [
            connection_get_info(c, null(), 0, out, null()),
            connection_get_table_types(c, out, null()),
            connection_get_statistic_names(c, out, null()),
            connection_get_statistics(c, null(), null(), null(), 1, out, null()),
            connection_read_partition(c, b"plan".as_ptr(), 4, out, null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_STATE; 5]);

    // A null partition with a length is a bad argument, caught before the state check.
    // SAFETY: The connection and output remain live; a null partition with a nonzero length is
    // explicitly rejected.
    let status = unsafe { connection_read_partition(c, null(), 7, out, null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    release(&mut connection);
}

#[test]
fn metadata_outputs_are_checked_before_initialization_or_network_work() {
    let mut connection = pending();
    let c = &raw mut connection;
    let table = c"orders".as_ptr();
    // SAFETY: Each call gets the live connection and a writable error; optional filters may be
    // null, and the null output is rejected.
    let stream_refusals = unsafe {
        [
            call(|e| connection_get_info(c, null(), 0, null(), e)),
            call(|e| {
                connection_get_objects(c, 0, null(), null(), null(), null(), null(), null(), e)
            }),
            call(|e| connection_get_table_types(c, null(), e)),
            call(|e| connection_get_statistics(c, null(), null(), null(), 1, null(), e)),
            call(|e| connection_get_statistic_names(c, null(), e)),
            call(|e| connection_read_partition(c, null(), 0, null(), e)),
        ]
    };
    for refusal in stream_refusals {
        assert_refused(
            refusal,
            ADBC_STATUS_INVALID_ARGUMENT,
            "out must not be null",
        );
    }
    // SAFETY: As above.
    let refusal =
        call(|e| unsafe { connection_get_table_schema(c, null(), null(), table, null(), e) });
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_ARGUMENT,
        "schema must not be null",
    );
    // The rejected calls preserve the connection's pre-init state and option buffer.
    assert!(buffered(&connection).is_empty());
    release(&mut connection);
}

#[test]
fn get_objects_accepts_every_documented_depth_and_rejects_the_rest() {
    let mut connection = pending();
    let mut out = FFI_ArrowArrayStream::empty();
    let type_list = [c"TABLE".as_ptr(), c"VIEW".as_ptr(), null()];
    let mut get_objects = |depth: c_int| {
        call(|error| {
            // SAFETY: The handle and output are live, and the type list is null-terminated.
            unsafe {
                connection_get_objects(
                    &raw mut connection,
                    depth,
                    null(),
                    null(),
                    null(),
                    type_list.as_ptr(),
                    null(),
                    &raw mut out,
                    error,
                )
            }
        })
    };

    // 0..=3 are ADBC_OBJECT_DEPTH_{ALL,CATALOGS,DB_SCHEMAS,TABLES}; COLUMNS is an alias of ALL,
    // so 4 is not a depth. Valid depths reach the state check.
    for depth in 0..=3 {
        assert_eq!(get_objects(depth).0, ADBC_STATUS_INVALID_STATE);
    }
    for depth in [-1, 4, 99] {
        assert_eq!(get_objects(depth).0, ADBC_STATUS_INVALID_ARGUMENT);
    }
    // The message must not suggest 4, which is rejected above.
    let message = get_objects(4).1.expect("the rejection carries a message");
    assert!(!message.contains("4 (columns)"), "{message}");
    assert!(message.contains("0 (all, including columns)"), "{message}");
    release(&mut connection);
}

#[test]
fn get_table_schema_requires_a_table_name() {
    let mut connection = pending();
    let mut schema = FFI_ArrowSchema::empty();
    let table = c"orders".as_ptr();
    let invalid = [0xFF_u8, 0x00];
    let mut get_table_schema = |db_schema: *const c_char, table_name: *const c_char| {
        // SAFETY: The handle and output are live; each name is null or a live NUL-terminated
        // string, and invalid UTF-8 still resides in readable storage.
        unsafe {
            connection_get_table_schema(
                &raw mut connection,
                null(),
                db_schema,
                table_name,
                &raw mut schema,
                null(),
            )
        }
    };

    assert_eq!(
        get_table_schema(null(), null()),
        ADBC_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        get_table_schema(invalid.as_ptr().cast(), table),
        ADBC_STATUS_INVALID_ARGUMENT,
        "a non-UTF-8 db_schema is still a bad argument even though the field is optional"
    );
    // With every argument acceptable, the uninitialized connection is what stops the call.
    assert_eq!(get_table_schema(null(), table), ADBC_STATUS_INVALID_STATE);
    release(&mut connection);
}

#[test]
fn a_second_handle_must_be_initialized_and_live() {
    let mut connection = pending();
    // The lock is available, but the state behind it is still pending.
    // SAFETY: The connection is a live handle with a `State` installed.
    let pending =
        unsafe { with_handle(&raw mut connection, |state| state.ready(KIND).map(|_| ())) };
    assert_eq!(pending.unwrap_err().status, Status::InvalidState);

    release(&mut connection);
    for (handle, status) in [
        (&raw mut connection, Status::InvalidState),
        (null(), Status::InvalidArguments),
    ] {
        // SAFETY: The released handle's slot is empty, and a null handle has none.
        let refused = unsafe { with_handle(handle, |_| Ok(())) }.unwrap_err();
        assert_eq!(refused.status, status);
    }
}

/// While a worker holds the dispatch turn, `connection_cancel` still returns promptly (here
/// `InvalidState`, since a pending connection has no cancel handle) instead of blocking or
/// reporting a panic.
#[test]
fn cancel_does_not_wait_for_the_connection_dispatch_turn() {
    let mut connection = pending();
    let release_turn = hold_the_dispatch_turn::<State>(&raw const connection.private_data);
    // SAFETY: The connection remains live throughout the worker; cancel uses its separate
    // synchronized state.
    let status = unsafe { connection_cancel(&raw mut connection, null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    release_turn();
    release(&mut connection);
}

/// Before `Init`, the live connection classifier decides the result for standard, vendor and
/// unknown keys, and unknown keys fail at `SetOption`, not at `Init`.
#[test]
#[cfg_attr(miri, ignore = "compares classifiers; unknown keys are slow in Miri")]
fn pre_init_recognition_uses_the_live_connection_classifier() {
    let mut connection = pending();
    let keys = [
        OptionConnection::AutoCommit,
        OptionConnection::CurrentCatalog,
        OptionConnection::CurrentSchema,
        OptionConnection::ReadOnly,
        OptionConnection::IsolationLevel,
        OptionConnection::Other("adbc.connection.no_such_option".to_owned()),
        OptionConnection::Other("bigquery.query.prefetc".to_owned()),
    ]
    .into_iter()
    .chain(
        crate::BigQueryConnectionOption::ALL
            .iter()
            .map(|option| OptionConnection::from(option.as_str())),
    );
    let mut accepted = 0;
    for key in keys {
        let expected = crate::connection::connection_option_kind(&key).is_some();
        let (status, _) = set(&mut connection, key.as_ref(), "true");
        assert_eq!(status == ADBC_STATUS_OK, expected, "{key:?}: {status}");
        accepted += usize::from(expected);
    }
    assert_eq!(buffered(&connection).len(), accepted);
    release(&mut connection);
}

/// `bigquery.project_id` and `bigquery.dataset_id` behave the same on both sides of `Init`: a set
/// is refused as read-only in the live setter's words, and a pre-`Init` get reports the value as
/// not yet readable rather than the key as unknown.
#[test]
fn the_namespace_aliases_answer_the_same_way_before_and_after_init() {
    let mut connection = pending();
    for (name, standard) in [
        ("bigquery.project_id", "adbc.connection.catalog"),
        ("bigquery.dataset_id", "adbc.connection.db_schema"),
        ("adbc.bigquery.sql.project_id", "adbc.connection.catalog"),
        ("adbc.bigquery.sql.dataset_id", "adbc.connection.db_schema"),
    ] {
        assert_eq!(
            set(&mut connection, name, "changed"),
            (
                ADBC_STATUS_NOT_IMPLEMENTED,
                Some(format!(
                    "the BigQuery connection option {name} is read-only; set {standard} instead"
                ))
            ),
        );
        assert_eq!(
            get(&mut connection, name).unwrap_err(),
            (
                ADBC_STATUS_NOT_FOUND,
                Some(format!(
                    "BigQuery connection option {name} cannot be read before the connection is \
                     initialized; initialize it first"
                ))
            ),
        );
    }
    assert!(
        buffered(&connection).is_empty(),
        "a read-only key must not be buffered"
    );
    release(&mut connection);
}

/// A released handle has a null `private_data`, which every entry point must report rather than
/// dereference.
#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = AdbcConnection::default();
    let c = &raw mut handle;
    let name = c"adbc.connection.autocommit".as_ptr();
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);
    let mut database = AdbcDatabase::default();

    // SAFETY: `c` points to a live handle struct whose null private data every entry point checks
    // before dispatch. The other arguments are live locals, static strings or literals covering
    // their stated lengths, or nulls the entry points reject.
    let statuses = unsafe {
        [
            connection_init(c, &raw mut database, null()),
            connection_commit(c, null()),
            connection_rollback(c, null()),
            connection_cancel(c, null()),
            connection_get_info(c, null(), 0, null(), null()),
            connection_get_objects(c, 0, null(), null(), null(), null(), null(), null(), null()),
            connection_get_table_schema(c, null(), null(), name, null(), null()),
            connection_get_table_types(c, null(), null()),
            connection_get_statistics(c, null(), null(), null(), 0, null(), null()),
            connection_get_statistic_names(c, null(), null()),
            connection_read_partition(c, b"x".as_ptr(), 1, null(), null()),
            set_option(c, name, name, null()),
            set_option_int(c, name, 1, null()),
            set_option_double(c, name, 1.0, null()),
            set_option_bytes(c, name, b"x".as_ptr(), 1, null()),
            get_option(c, name, null(), &raw mut length, null()),
            get_option_bytes(c, name, null(), &raw mut length, null()),
            get_option_int(c, name, &raw mut integer, null()),
            get_option_double(c, name, &raw mut double, null()),
            release_handle(c, null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_STATE; 20]);
}

/// `AdbcStatementNew` borrows the connection without dispatch, so it has its own state check.
#[test]
fn creating_a_statement_needs_an_initialized_connection() {
    let mut connection = pending();
    let mut statement = AdbcStatement::default();
    let refusal = call(|error| {
        // SAFETY: The connection, empty statement destination, and error are live writable
        // locals.
        unsafe {
            crate::ffi::statement::statement_new(&raw mut connection, &raw mut statement, error)
        }
    });
    assert_refused(refusal, ADBC_STATUS_INVALID_STATE, "connection");
    assert!(statement.private_data.is_null(), "nothing was allocated");
    release(&mut connection);
}

/// `AdbcConnectionInit` locks the database without dispatch, so it checks the database's state
/// itself. An allocated but uninitialized database fails differently from a null one.
#[test]
fn init_against_an_allocated_but_uninitialized_database_is_refused() {
    let mut connection = pending();
    let mut database: AdbcDatabase = allocated();
    let refusal = call(|error| {
        // SAFETY: Both handles own live driver allocations; the error destination is writable.
        unsafe { connection_init(&raw mut connection, &raw mut database, error) }
    });
    assert_refused(refusal, ADBC_STATUS_INVALID_STATE, "database");

    // The connection kept its pre-init state (`buffered` panics on a ready one), so it can retry
    // once the database is initialized.
    buffered(&connection);
    release(&mut database);
    release(&mut connection);
}

/// Building a connection can call BigQuery, here to validate a catalog set before `Init`, so it
/// must not hold the database lock: the database's getters, and other connections being opened,
/// stay available meanwhile.
#[test]
fn a_connection_validating_its_catalog_leaves_the_database_available() {
    use std::sync::mpsc::channel;
    use std::time::Duration;

    let (entered_sender, entered_receiver) = channel::<()>();
    let (resume_sender, resume_receiver) = channel::<()>();
    let (endpoint, requests) = capture_requests_gated(
        vec![(200, serde_json::json!({"datasets": []}))],
        move || {
            entered_sender.send(()).expect("test channel");
            resume_receiver.recv().expect("test channel");
            true
        },
    );
    let mut database = database_at(&endpoint);
    let mut connection = pending();
    let catalog = set(&mut connection, "adbc.connection.catalog", "other-project");
    assert_eq!(catalog.0, ADBC_STATUS_OK);

    let database_address = (&raw mut database).expose_provenance();
    let connection_address = (&raw mut connection).expose_provenance();
    // SAFETY: The parent keeps both handles alive until this thread joins.
    let opening = std::thread::spawn(move || unsafe {
        connection_init(
            std::ptr::with_exposed_provenance_mut(connection_address),
            std::ptr::with_exposed_provenance_mut(database_address),
            null(),
        )
    });
    entered_receiver
        .recv()
        .expect("the catalog validation request");

    let (read_sender, read_receiver) = channel();
    let reader = std::thread::spawn(move || {
        let mut length = 0_usize;
        // SAFETY: The parent keeps the database alive until this thread joins; a null buffer with
        // a zero length is a size query.
        let status = unsafe {
            get_option::<AdbcDatabase>(
                std::ptr::with_exposed_provenance_mut(database_address),
                c"adbc.bigquery.sql.api_endpoint".as_ptr(),
                null(),
                &raw mut length,
                null(),
            )
        };
        read_sender.send(status).expect("test channel");
    });
    let read = read_receiver.recv_timeout(Duration::from_secs(30));
    resume_sender.send(()).expect("test channel");
    assert_eq!(
        read,
        Ok(ADBC_STATUS_OK),
        "DatabaseGetOption waited for ConnectionInit's BigQuery request"
    );
    reader.join().unwrap();
    assert_eq!(opening.join().unwrap(), ADBC_STATUS_OK);
    let requests = requests.served(1);
    assert!(
        requests[0].path().contains("/datasets"),
        "{}",
        requests[0].path()
    );
    release(&mut connection);
    release(&mut database);
}
