use super::*;
use crate::ffi::abi::{
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND,
    ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK,
};
use crate::ffi::options::{get_option_int, new_handle, release_handle, set_option, set_option_int};
use crate::ffi::test_support::{
    allocated, assert_refused, call, get, initialized_database, null, release, set,
};

fn init(database: &mut AdbcDatabase) -> AdbcStatusCode {
    // SAFETY: The handle is exclusively borrowed; the error output is optional.
    unsafe { database_init(database, null()) }
}

/// A URI can percent-encode a NUL that no C string option can carry, and `AdbcDatabaseGetOption`
/// could not hand it back, so `Init` refuses it as a bad argument.
#[test]
fn a_uri_value_decoding_to_a_nul_fails_init() {
    let mut database: AdbcDatabase = allocated();
    let uri = "bigquery:///a-project?Location=EU%00";
    assert_eq!(set(&mut database, "uri", uri).0, ADBC_STATUS_OK);
    assert_eq!(init(&mut database), ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(
        get(&mut database, "bigquery.location").unwrap_err().0,
        ADBC_STATUS_NOT_FOUND
    );
    release(&mut database);
}

/// adbc.h requires a zero-initialized struct for `AdbcDatabaseNew`. Overwriting a populated slot
/// would leak its object; refusing leaves it reachable for release.
#[test]
fn a_second_new_on_a_populated_handle_is_refused() {
    let mut database: AdbcDatabase = allocated();
    let first = database.private_data;
    let refusal = call(|error| {
        // SAFETY: The handle is a writable local; new_handle checks the already-populated slot.
        unsafe { new_handle(&raw mut database, error) }
    });
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_STATE,
        "already holds a driver object",
    );
    assert_eq!(database.private_data, first);
    release(&mut database);
}

/// The buffer collapses repeats, so setting the URI twice must not look like the duplicate URI
/// the driver rejects.
#[test]
fn setting_the_same_option_twice_keeps_the_later_value() {
    let mut database: AdbcDatabase = allocated();
    for (key, value) in [
        ("uri", "bigquery:///first"),
        ("uri", "bigquery:///second"),
        ("adbc.bigquery.sql.auth_type", "anonymous"),
    ] {
        assert_eq!(set(&mut database, key, value).0, ADBC_STATUS_OK);
    }
    // Before Init the getters report `NOT_FOUND`, however many sets were buffered.
    assert_eq!(
        get(&mut database, "adbc.bigquery.sql.project_id")
            .unwrap_err()
            .0,
        ADBC_STATUS_NOT_FOUND
    );
    assert_eq!(init(&mut database), ADBC_STATUS_OK);
    assert_eq!(
        get(&mut database, "adbc.bigquery.sql.project_id").unwrap(),
        "second"
    );
    release(&mut database);
}

#[test]
fn pre_init_aliases_share_the_last_value_and_can_correct_a_failed_init() {
    for (first, second) in [
        ("bigquery.auth_type", "adbc.bigquery.sql.auth_type"),
        ("adbc.bigquery.sql.auth_type", "bigquery.auth_type"),
    ] {
        let mut database: AdbcDatabase = allocated();
        assert_eq!(set(&mut database, first, "no-such-auth").0, ADBC_STATUS_OK);
        assert_eq!(init(&mut database), ADBC_STATUS_INVALID_ARGUMENT);
        assert_eq!(set(&mut database, second, "anonymous").0, ADBC_STATUS_OK);
        assert_eq!(
            init(&mut database),
            ADBC_STATUS_OK,
            "correcting {first} through {second} must replace the invalid value"
        );
        assert_eq!(get(&mut database, first).unwrap(), "anonymous");
        release(&mut database);

        let mut database: AdbcDatabase = allocated();
        for (key, value) in [
            (first, "default"),
            (second, "application_default"),
            (first, "anonymous"),
        ] {
            assert_eq!(set(&mut database, key, value).0, ADBC_STATUS_OK);
        }
        assert_eq!(init(&mut database), ADBC_STATUS_OK);
        assert_eq!(get(&mut database, second).unwrap(), "anonymous");
        release(&mut database);
    }
}

/// A failed `Init` must keep the buffered options so the offending one can be corrected and
/// `Init` retried, rather than silently reinitializing from an empty buffer.
#[test]
fn a_failed_init_keeps_the_buffered_options_for_a_retry() {
    let mut database: AdbcDatabase = allocated();
    for (key, value) in [
        ("uri", "bigquery:///a-project"),
        ("adbc.bigquery.sql.api_endpoint", "http://127.0.0.1:1"),
        // Buffered without validation; rejected when Init applies it.
        ("adbc.bigquery.sql.auth_type", "no-such-auth"),
    ] {
        assert_eq!(set(&mut database, key, value).0, ADBC_STATUS_OK);
    }
    assert_ne!(init(&mut database), ADBC_STATUS_OK);

    let auth = set(&mut database, "adbc.bigquery.sql.auth_type", "anonymous");
    assert_eq!(auth.0, ADBC_STATUS_OK);
    assert_eq!(init(&mut database), ADBC_STATUS_OK);
    assert_eq!(
        get(&mut database, "adbc.bigquery.sql.project_id").unwrap(),
        "a-project"
    );
    assert_eq!(
        get(&mut database, "adbc.bigquery.sql.api_endpoint").unwrap(),
        "http://127.0.0.1:1"
    );
    release(&mut database);
}

#[test]
fn initializing_twice_is_an_invalid_state() {
    let mut database = initialized_database();
    assert_eq!(init(&mut database), ADBC_STATUS_INVALID_STATE);
    release(&mut database);
}

#[test]
fn a_released_handle_rejects_every_call_including_a_second_release() {
    let mut database = initialized_database();
    release(&mut database);
    assert!(database.private_data.is_null());
    let (status, _) = call(|error| {
        // SAFETY: The handle struct remains a writable local; release checks its cleared slot.
        unsafe { release_handle(&raw mut database, error) }
    });
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    let endpoint = "adbc.bigquery.sql.api_endpoint";
    assert_eq!(
        set(&mut database, endpoint, "http://127.0.0.1:2").0,
        ADBC_STATUS_INVALID_STATE
    );
    assert_eq!(
        get(&mut database, endpoint).unwrap_err().0,
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn null_handles_keys_and_values_are_rejected() {
    // SAFETY: new_handle explicitly rejects a null handle; the error output is optional.
    let status = unsafe { new_handle::<AdbcDatabase>(null(), null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);

    let mut database: AdbcDatabase = allocated();
    // SAFETY: The handle is live and the strings are static; each null is explicitly rejected.
    let statuses = unsafe {
        [
            set_option(&raw mut database, null(), c"x".as_ptr(), null()),
            set_option(&raw mut database, c"uri".as_ptr(), null(), null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_ARGUMENT; 2]);
    release(&mut database);
}

/// Every database option is a string, so the typed getters must refuse rather than return a
/// default.
#[test]
fn typed_getters_reject_string_options() {
    let mut database = initialized_database();
    let mut value = 0_i64;
    // SAFETY: The handle and static key are live, and the integer output is writable.
    let status = unsafe {
        get_option_int(
            &raw mut database,
            c"adbc.bigquery.sql.api_endpoint".as_ptr(),
            &raw mut value,
            null(),
        )
    };
    assert_ne!(status, ADBC_STATUS_OK);
    release(&mut database);
}

/// Before `Init`, the key and its type are checked in the live setter's words. Value checks wait
/// for `Init`, because they depend on the `adbc.uri` ordering it applies.
#[test]
fn a_typed_set_before_init_is_type_checked_and_then_validated_by_the_live_database() {
    let mut database: AdbcDatabase = allocated();
    let (status, message) = call(|error| {
        // SAFETY: The handle and static key remain live; the error output is writable.
        unsafe { set_option_int(&raw mut database, c"uri".as_ptr(), 1, error) }
    });
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(
        message.as_deref(),
        Some("BigQuery option uri requires a string value, received integer")
    );

    // A well-typed value the database refuses is deferred to `Init`.
    assert_eq!(set(&mut database, "uri", "nope://").0, ADBC_STATUS_OK);
    assert_eq!(init(&mut database), ADBC_STATUS_INVALID_ARGUMENT);
    let uri = set(&mut database, "uri", "bigquery:///a-project");
    assert_eq!(uri.0, ADBC_STATUS_OK);
    assert_eq!(init(&mut database), ADBC_STATUS_OK);
    release(&mut database);
}

/// adbc.h has `SetOption` return `ADBC_STATUS_NOT_IMPLEMENTED` for an unrecognized key, and
/// options may be set before `Init`, so that error belongs to the `SetOption` call, not `Init`.
#[test]
fn a_typo_before_init_is_reported_at_the_call_that_made_it() {
    let mut database: AdbcDatabase = allocated();
    assert_eq!(
        set(&mut database, "adbc.bigquery.sql.projct_id", "a-project").0,
        ADBC_STATUS_NOT_IMPLEMENTED
    );
    // Nothing was buffered, so `Init` succeeds on the options that were accepted.
    for (key, value) in [
        ("uri", "bigquery:///a-project"),
        ("adbc.bigquery.sql.auth_type", "anonymous"),
    ] {
        assert_eq!(set(&mut database, key, value).0, ADBC_STATUS_OK);
    }
    assert_eq!(init(&mut database), ADBC_STATUS_OK);
    release(&mut database);
}

/// Pre-`Init` recognition must agree with an initialized database for standard, vendor and
/// unknown keys; the live setter supplies the expected answer.
#[test]
#[cfg_attr(miri, ignore = "compares classifiers; unknown keys are slow in Miri")]
fn recognition_matches_the_live_database_across_init() {
    let mut live = initialized_database();
    let keys = [
        OptionDatabase::Uri,
        OptionDatabase::Username,
        OptionDatabase::Password,
        OptionDatabase::Other("adbc.bigquery.no.such.option".to_owned()),
    ]
    .into_iter()
    .chain(
        crate::BigQueryDatabaseOption::ALL
            .iter()
            .map(|option| OptionDatabase::from(option.as_str())),
    );
    for key in keys {
        let mut pending: AdbcDatabase = allocated();
        let (before, _) = set(&mut pending, key.as_ref(), "");
        release(&mut pending);
        let (after, _) = set(&mut live, key.as_ref(), "");
        assert_eq!(
            before == ADBC_STATUS_NOT_IMPLEMENTED,
            after == ADBC_STATUS_NOT_IMPLEMENTED,
            "{}: pre-init {before}, live {after}",
            key.as_ref()
        );
    }
    release(&mut live);
}
