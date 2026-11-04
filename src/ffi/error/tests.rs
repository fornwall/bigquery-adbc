use adbc_core::error::Status;

use super::*;
use crate::ffi::abi::AdbcDriver;
use crate::ffi::test_support::{c_string, error_message, null, release_error, zeroed_error};

fn export(out: &mut AdbcError, error: Error) {
    // SAFETY: `out` is initialized, exclusively borrowed and sized for the revision its vendor
    // code advertises; any previous payload is owned by its release callback.
    unsafe { export_error(out, error) };
}

/// The details `error_get_detail_count` and `error_get_detail` report, copied out.
fn details_of(error: &AdbcError) -> Vec<(String, Vec<u8>)> {
    // SAFETY: Test callers pass an error this driver populated or left empty, whose allocation
    // matches its vendor-code sentinel.
    let count = unsafe { error_get_detail_count(error) };
    (0..count)
        .map(|index| {
            // SAFETY: As above; the index is in range.
            let detail = unsafe { error_get_detail(error, index) };
            // SAFETY: The unreleased error owns the NUL-terminated key and the `value_length`
            // bytes of the value.
            unsafe {
                (
                    c_string(detail.key).unwrap(),
                    std::slice::from_raw_parts(detail.value, detail.value_length).to_vec(),
                )
            }
        })
        .collect()
}

/// A 1.0.0 error as its caller allocates it: no sentinel, and a tail outside the allocation that
/// must survive untouched, filled here with recognizable values.
fn error_v100() -> AdbcError {
    AdbcError {
        message: null(),
        vendor_code: 0,
        sqlstate: [0; 5],
        release: None,
        private_data: 0xf00d_usize as *mut c_void,
        private_driver: 0x5eed_usize as *const AdbcDriver,
    }
}

/// Release a 1.0.0 error through the callback the driver stored for that layout.
fn release_v100(error: &mut AdbcError) {
    // SAFETY: The driver stored a callback taking the repr(C) 1.0 prefix, which shares the C ABI.
    let release: unsafe extern "C" fn(*mut AdbcErrorV100) =
        unsafe { std::mem::transmute(error.release.unwrap()) };
    // SAFETY: The pointer addresses the initialized 1.0 prefix this callback owns.
    unsafe { release(std::ptr::from_mut(error).cast()) };
}

#[test]
fn exports_a_message_and_releases_idempotently() {
    let mut out = zeroed_error();
    export(&mut out, Error::with_message_and_status("boom", Status::IO));
    assert_eq!(error_message(&out).as_deref(), Some("boom"));
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    let release = out.release.expect("driver sets a release callback");
    // SAFETY: The callback belongs to `out`, which has not been released and is exclusively
    // writable here.
    unsafe { release(&raw mut out) };
    assert!(out.message.is_null());
    assert!(out.release.is_none());
    // A second release must be a no-op rather than a double free.
    // SAFETY: The first release cleared ownership fields; this driver callback accepts that
    // released state.
    unsafe { release(&raw mut out) };
}

#[test]
fn reusing_an_out_parameter_releases_the_previous_error() {
    let mut out = zeroed_error();
    export(
        &mut out,
        Error::with_message_and_status("first", Status::IO),
    );
    export(
        &mut out,
        Error::with_message_and_status("second", Status::Internal),
    );
    assert_eq!(error_message(&out).as_deref(), Some("second"));
    release_error(&mut out);
}

/// A caller that allocated only the 1.0.0 prefix must get the tail bytes back untouched, as in
/// the C++ suite's `StatementTest.ErrorCompatibility`.
#[test]
fn a_1_0_0_caller_keeps_its_trailing_bytes() {
    let mut out = error_v100();
    let mut error = Error::with_message_and_status("legacy", Status::NotFound);
    error.vendor_code = 42;
    export(&mut out, error);

    assert_eq!(error_message(&out).as_deref(), Some("legacy"));
    assert_eq!(out.vendor_code, 42);
    assert_eq!(
        (out.private_data, out.private_driver),
        (error_v100().private_data, error_v100().private_driver)
    );
    release_v100(&mut out);
    assert_eq!(out.private_data, error_v100().private_data);
}

/// A driver error may carry `i32::MIN` as its vendor code. Stored verbatim in a 1.0.0 struct, the
/// next export would read it as the 1.1.0 sentinel and write 48 bytes into the caller's 32. The
/// second export below is that call and must not touch the tail.
#[test]
fn a_1_0_0_caller_is_not_relabelled_by_a_sentinel_valued_vendor_code() {
    let mut out = error_v100();
    let mut error = Error::with_message_and_status("first", Status::Internal);
    error.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    export(&mut out, error);
    assert_ne!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    export(
        &mut out,
        Error::with_message_and_status("second", Status::Internal),
    );
    assert_eq!(error_message(&out).as_deref(), Some("second"));
    assert_eq!(
        (out.private_data, out.private_driver),
        (error_v100().private_data, error_v100().private_driver)
    );
    release_v100(&mut out);
}

/// A 1.1.0 error keeps the layout sentinel in `vendor_code`, which the driver manager checks
/// before calling `AdbcErrorGetDetail*`. The real vendor code is an error detail.
#[test]
fn a_1_1_0_caller_keeps_the_layout_sentinel_and_its_details_rather_than_a_vendor_code() {
    let mut error = Error::with_message_and_status("rejected", Status::NotFound);
    error.vendor_code = 404;
    error.details = Some(vec![(
        crate::error::DETAIL_HTTP_STATUS.to_string(),
        b"404".to_vec(),
    )]);
    let mut out = zeroed_error();
    export(&mut out, error);

    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
    assert_eq!(
        details_of(&out),
        [(crate::error::DETAIL_HTTP_STATUS.to_owned(), b"404".to_vec())]
    );
    release_error(&mut out);
}

#[test]
fn round_trips_error_details() {
    let mut error = Error::with_message_and_status("detailed", Status::Internal);
    error.details = Some(vec![
        ("k1".to_string(), b"v1".to_vec()),
        ("k2".to_string(), b"".to_vec()),
    ]);
    let mut out = zeroed_error();
    export(&mut out, error);
    assert_eq!(
        details_of(&out),
        [
            ("k1".to_owned(), b"v1".to_vec()),
            ("k2".to_owned(), Vec::new())
        ]
    );

    // Out-of-range indices, in both directions, must yield the documented empty detail.
    for index in [2, -1] {
        // SAFETY: `out` remains live; detail lookup explicitly handles an out-of-range index.
        let detail = unsafe { error_get_detail(&raw const out, index) };
        assert!(detail.key.is_null() && detail.value.is_null() && detail.value_length == 0);
    }

    release_error(&mut out);
    assert!(
        details_of(&out).is_empty(),
        "a released error has no details"
    );
}

/// The detail getters honour the sentinel like the export does: without it, `private_data` is
/// outside the caller's allocation.
#[test]
fn the_detail_getters_ignore_private_data_on_a_1_0_0_error() {
    let mut out = error_v100();
    let mut error = Error::with_message_and_status("legacy", Status::Internal);
    error.details = Some(vec![("k1".to_string(), b"v1".to_vec())]);
    export(&mut out, error);

    // The export left the tail alone, and the getters must not read it either.
    assert_eq!(out.private_data, error_v100().private_data);
    assert!(details_of(&out).is_empty());
    // SAFETY: `out` has a valid 1.0 prefix; its vendor code excludes the non-dereferenceable tail
    // fields.
    assert!(unsafe { error_get_detail(&raw const out, 0) }.key.is_null());
    release_v100(&mut out);
}

#[test]
fn null_out_parameters_are_tolerated() {
    // SAFETY: Export and both detail getters explicitly accept a null error.
    unsafe {
        export_error(
            null(),
            Error::with_message_and_status("dropped", Status::IO),
        );
        assert_eq!(error_get_detail_count(null()), 0);
        assert!(error_get_detail(null(), 0).key.is_null());
    }
}

#[test]
fn a_message_with_an_interior_nul_is_still_reported() {
    let mut out = zeroed_error();
    export(
        &mut out,
        Error::with_message_and_status("before\0after", Status::IO),
    );
    assert_eq!(error_message(&out).as_deref(), Some("before?after"));
    release_error(&mut out);
}

/// adbc.h:304: `private_data` "is NULLPTR iff the error is uninitialized/freed", so a populated
/// 1.1.0 error carries a detail box even when it has no details to put in it.
#[test]
fn a_detail_free_error_still_marks_itself_initialized() {
    let mut out = zeroed_error();
    export(
        &mut out,
        Error::with_message_and_status("plain", Status::IO),
    );
    assert!(!out.private_data.is_null());
    assert!(details_of(&out).is_empty());
    release_error(&mut out);
    assert!(out.private_data.is_null());
}

/// The driver manager sets `private_driver` before the call and reads it afterwards to route the
/// detail getters, so releasing a previous error must preserve it, even when that callback clears
/// the whole struct (as the reference C++ framework's does).
#[test]
fn a_foreign_release_cannot_take_private_driver_with_it() {
    /// # Safety
    /// `error` must point to exclusively writable storage for a full 1.1.0 error.
    unsafe extern "C" fn clearing_release(error: *mut AdbcError) {
        // SAFETY: This callback is installed only on the full, writable `AdbcError` below; zero is
        // valid for every field.
        unsafe { std::ptr::write_bytes(error.cast::<u8>(), 0, size_of::<AdbcError>()) };
    }

    let sentinel_driver = 0x5eed_usize as *const AdbcDriver;
    let mut out = AdbcError {
        release: Some(clearing_release),
        private_driver: sentinel_driver,
        ..zeroed_error()
    };
    export(&mut out, Error::with_message_and_status("boom", Status::IO));
    assert_eq!(out.private_driver, sentinel_driver);
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
    release_error(&mut out);
}

/// A generated client's `google.rpc.Status` details reach a 1.1.0 caller through the detail
/// getters, keyed by type name and valued with their ProtoJSON, after the code detail.
#[test]
fn a_service_failures_status_details_reach_the_detail_getters() {
    use google_cloud_gax::error::Error as GoogleError;
    use google_cloud_gax::error::rpc::{Code, Status as GoogleStatus, StatusDetails};

    let retry: StatusDetails = serde_json::from_value(serde_json::json!({
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": "2s",
    }))
    .unwrap();
    let error = crate::error::google_error(
        "failed to read rows",
        GoogleError::service(
            GoogleStatus::default()
                .set_code(Code::Unavailable)
                .set_details([retry]),
        ),
    );
    let mut out = zeroed_error();
    export(&mut out, error);

    let details = details_of(&out);
    assert_eq!(details.len(), 2);
    assert_eq!(details[1].0, "google.rpc.RetryInfo");
    assert_eq!(
        details[1].1,
        br#"{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"2s"}"#
    );
    release_error(&mut out);
}
