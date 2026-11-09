use super::*;
use adbc_core::error::Status;

#[test]
fn catches_a_panic_as_an_internal_error_and_passes_results_through() {
    let error = catch(|| -> Result<()> { panic!("kaboom") }).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("kaboom"), "{}", error.message);
    assert_eq!(catch(|| Ok(7)).unwrap(), 7);
    let failure =
        catch(|| -> Result<()> { Err(Error::with_message_and_status("nope", Status::NotFound)) });
    assert_eq!(failure.unwrap_err().status, Status::NotFound);
}

/// Both panic payload shapes `panic!` produces are rendered; anything else says so rather
/// than reporting an empty message.
#[test]
fn renders_both_panic_payload_shapes() {
    assert_eq!(panic_message(&"borrowed"), "borrowed");
    assert_eq!(panic_message(&"owned".to_string()), "owned");
    assert_eq!(panic_message(&7_u8), "unknown panic payload");
}

/// An interior NUL becomes a visible `?` rather than truncating what follows it.
#[test]
fn sanitized_cstring_replaces_interior_nuls_instead_of_truncating() {
    assert_eq!(
        sanitized_cstring("before\0after").to_bytes(),
        b"before?after"
    );
    assert_eq!(sanitized_cstring("clean").to_bytes(), b"clean");
    assert_eq!(sanitized_cstring("").to_bytes(), b"");
}

/// `write_string` into `buffer`, advertising `length` bytes of it, returning the size reported.
fn string_into(value: &str, buffer: &mut [c_char], length: usize) -> Result<usize> {
    assert!(length <= buffer.len());
    let mut length = length;
    // SAFETY: The buffer holds at least the advertised bytes and is disjoint from the live length
    // and input.
    unsafe { write_string(value, buffer.as_mut_ptr(), &raw mut length) }.map(|()| length)
}

#[test]
fn write_string_and_bytes_report_the_required_size_without_writing() {
    let mut buffer = [0_i8; 2];
    // "hello" plus NUL; nothing written because the buffer is too small.
    assert_eq!(string_into("hello", &mut buffer, 2).unwrap(), 6);
    assert_eq!(buffer, [0, 0]);

    let mut buffer = [0_i8; 6];
    assert_eq!(string_into("hello", &mut buffer, 6).unwrap(), 6);
    assert_eq!(
        // SAFETY: The successful write fit its local buffer and included the trailing NUL.
        unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_str().unwrap(),
        "hello"
    );

    // `write_bytes` follows the same protocol, without the trailing NUL.
    let mut buffer = [0_u8; 1];
    let mut length = buffer.len();
    // SAFETY: The local buffer has the advertised capacity, disjoint from the live length and
    // input.
    unsafe { write_bytes(b"abc", buffer.as_mut_ptr(), &raw mut length) }.unwrap();
    assert_eq!(length, 3);
    assert_eq!(buffer, [0]);
}

#[test]
fn write_string_preserves_utf8_and_terminates_empty_values() {
    let sentinel: c_char = 120;
    for value in ["", "smörgås"] {
        let mut buffer = [sentinel; 16];
        let length = string_into(value, &mut buffer, value.len() + 1).unwrap();
        assert_eq!(length, value.len() + 1);
        assert_eq!(
            // SAFETY: The successful write fit its local buffer and included the trailing NUL.
            unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_bytes(),
            value.as_bytes()
        );
        assert_eq!(buffer.get(length), Some(&sentinel));
    }

    let mut buffer = [sentinel; 16];
    let error = string_into("left\0right", &mut buffer, 16).unwrap_err();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("byte 4"), "{}", error.message);
    assert_eq!(buffer, [sentinel; 16]);
}

/// adbc.h treats a reported length within the passed one as "written", so a null buffer that
/// claims room is refused. A null buffer of length zero is a legal size probe.
#[test]
fn a_null_buffer_is_a_size_probe_only_when_it_says_it_holds_nothing() {
    let null_buffer = std::ptr::null_mut();
    let mut length = 64;
    // SAFETY: Null destinations are handled explicitly; length is a live, exclusively writable
    // local.
    let error = unsafe { write_string("hello", null_buffer, &raw mut length) }.unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("64"), "{}", error.message);
    assert_eq!(length, 64, "a rejected call reports no size");

    let mut length = 0;
    // SAFETY: As above.
    unsafe { write_string("hello", null_buffer, &raw mut length) }.unwrap();
    assert_eq!(length, 6);

    let mut length = 0;
    // SAFETY: As above.
    unsafe { write_bytes(b"abc", null_buffer.cast(), &raw mut length) }.unwrap();
    assert_eq!(length, 3);
    assert!(
        // SAFETY: As above.
        unsafe { write_bytes(b"", null_buffer.cast(), &raw mut length) }.is_err(),
        "the length is now 3, so the null buffer claims room it does not have"
    );
}

#[test]
fn optional_str_maps_null_to_none_and_required_str_rejects_it() {
    assert_eq!(
        // SAFETY: The helper explicitly handles null without dereferencing it.
        unsafe { optional_str(std::ptr::null(), "catalog") }.unwrap(),
        None
    );
    let value = CString::new("main").unwrap();
    assert_eq!(
        // SAFETY: The CString supplies a live NUL-terminated allocation for the assertion.
        unsafe { optional_str(value.as_ptr(), "catalog") }.unwrap(),
        Some("main")
    );

    // SAFETY: The helper explicitly handles null without dereferencing it.
    let error = unsafe { required_str(std::ptr::null(), "table_name") }.unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("table_name"), "{}", error.message);
}

#[test]
fn optional_str_list_reads_until_the_null_terminator() {
    let first = CString::new("TABLE").unwrap();
    let second = CString::new("VIEW").unwrap();
    let array = [first.as_ptr(), second.as_ptr(), std::ptr::null()];
    assert_eq!(
        // SAFETY: The live local pointer array ends in null, and both owned CStrings outlive
        // the borrow.
        unsafe { optional_str_list(array.as_ptr(), "table_type") }.unwrap(),
        Some(vec!["TABLE", "VIEW"])
    );
    assert_eq!(
        // SAFETY: The helper explicitly handles null without dereferencing it.
        unsafe { optional_str_list(std::ptr::null(), "table_type") }.unwrap(),
        None
    );
}

#[test]
fn byte_slice_accepts_null_only_when_empty() {
    assert_eq!(
        // SAFETY: The helper explicitly handles null without dereferencing it.
        unsafe { byte_slice(std::ptr::null(), 0, "plan") }.unwrap(),
        b""
    );
    // SAFETY: The helper explicitly handles null without dereferencing it.
    assert!(unsafe { byte_slice(std::ptr::null(), 1, "plan") }.is_err());
    let data = [1_u8, 2, 3];
    assert_eq!(
        // SAFETY: The local array supplies exactly data.len() initialized bytes for the
        // assertion.
        unsafe { byte_slice(data.as_ptr(), data.len(), "plan") }.unwrap(),
        &data
    );
}
