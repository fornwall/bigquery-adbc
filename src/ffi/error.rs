//! Writing a driver [`Error`] back through a caller-owned `AdbcError`.
//!
//! The caller allocates the struct, so its size depends on the caller's ADBC revision. A 1.1.0
//! caller presets [`ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA`] in `vendor_code`; any other value means
//! the 1.0.0 layout, which has no room for the trailing two fields.

use std::ffi::{CString, c_int, c_void};

use adbc_core::error::Error;

use super::abi::{ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, AdbcError, AdbcErrorDetail, AdbcErrorV100};
use super::guard::sanitized_cstring;

/// Backing store for the pairs returned by [`error_get_detail`], which must stay valid until
/// [`release_error`].
type ErrorDetails = Vec<(CString, Vec<u8>)>;

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.0.0 error into.
// Guard-exempt: freeing a `CString` cannot panic; the rest is pointer writes.
unsafe extern "C" fn release_error_v100(error: *mut AdbcErrorV100) {
    // SAFETY: our callback receives exclusive access to the 1.0.0 prefix we populated.
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.message.is_null() {
        // SAFETY: export_error stored CString::into_raw here; clearing it prevents a second free.
        drop(unsafe { CString::from_raw(error.message) });
        error.message = std::ptr::null_mut();
    }
    // Clearing this makes the callback idempotent, which the header requires.
    error.release = None;
}

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.1.0 error into.
// Guard-exempt: freeing a `CString` and a details box cannot panic; the rest is pointer writes.
unsafe extern "C" fn release_error(error: *mut AdbcError) {
    // SAFETY: A 1.1.0 error starts with the 1.0.0 prefix this driver wrote.
    unsafe { release_error_v100(error.cast()) };
    // SAFETY: our callback receives exclusive access to the full 1.1.0 struct we populated.
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.private_data.is_null() {
        // SAFETY: export_error allocated this ErrorDetails box; clearing it prevents a second free.
        drop(unsafe { Box::from_raw(error.private_data.cast::<ErrorDetails>()) });
        error.private_data = std::ptr::null_mut();
    }
}

/// Write `error` into the caller's out parameter, respecting whichever ABI revision it allocated.
///
/// # Safety
/// `out` must be null or point to a zero-initialized or previously-populated `AdbcError` whose
/// size matches the revision its `vendor_code` sentinel advertises.
/// The struct must be exclusively accessible and any release callback must own its payload.
pub(crate) unsafe fn export_error(out: *mut AdbcError, error: Error) {
    if out.is_null() {
        return;
    }

    // Preserve the ABI sentinel and manager's vtable before releasing a previous error:
    // foreign release callbacks may zero the entire struct. The manager needs the vtable to
    // route AdbcErrorGetDetail*, but only a 1.1.0 allocation contains that field.
    // SAFETY: out is non-null and both revisions contain vendor_code in their allocated prefix.
    let is_v110 = unsafe { (*out).vendor_code } == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    let private_driver = if is_v110 {
        // SAFETY: the caller's sentinel guarantees that the 1.1.0 tail exists.
        unsafe { (*out).private_driver }
    } else {
        std::ptr::null()
    };

    // SAFETY: release lies in both revisions' prefixes; the caller supplied an initialized struct.
    if let Some(release) = unsafe { (*out).release } {
        // SAFETY: this is the existing payload's callback, called once before overwriting it.
        unsafe { release(out) };
    }

    if is_v110 {
        // ADBC requires non-null private_data for an initialized 1.1.0 error, even without
        // details. release_error frees this box and clears the pointer.
        let details: ErrorDetails = error
            .details
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (sanitized_cstring(&key), value))
            .collect();
        let value = AdbcError {
            message: sanitized_cstring(&error.message).into_raw(),
            // The manager requires this sentinel to expose error details. HTTP and RPC codes
            // travel in bigquery.error.http_status / bigquery.error.rpc_code instead.
            vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: error.sqlstate,
            release: Some(release_error),
            private_data: Box::into_raw(Box::new(details)).cast::<c_void>(),
            private_driver,
        };
        // SAFETY: the sentinel guarantees a writable full 1.1.0 struct; the old payload is
        // released.
        unsafe { std::ptr::write_unaligned(out, value) };
    } else {
        let value = AdbcErrorV100 {
            message: sanitized_cstring(&error.message).into_raw(),
            // Never put the 1.1.0 sentinel in a 1.0.0 allocation: a later export would then
            // write past its end. Zero means no applicable vendor code.
            vendor_code: if error.vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
                0
            } else {
                error.vendor_code
            },
            sqlstate: error.sqlstate,
            release: Some(release_error_v100),
        };
        // SAFETY: the caller guarantees a writable 1.0.0 prefix; no tail bytes are accessed.
        unsafe { std::ptr::write_unaligned(out.cast::<AdbcErrorV100>(), value) };
    }
}

/// Borrow the detail box behind `error`, if its struct is 1.1.0-sized.
///
/// Fields are read through raw pointers so a 1.0.0 allocation never becomes a reference to the
/// larger struct.
///
/// # Safety
/// `error` must be null or point to an initialized empty error or one populated/released by this
/// driver, with allocation size matching its vendor-code sentinel. For 1.1.0, `private_data`
/// must be null or this driver's `ErrorDetails` box, retained unmodified for `'a`.
unsafe fn details<'a>(error: *const AdbcError) -> Option<&'a ErrorDetails> {
    if error.is_null() {
        return None;
    }
    // SAFETY: error is non-null and vendor_code belongs to both revisions' allocated prefixes.
    if unsafe { (*error).vendor_code } != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
        return None;
    }
    // SAFETY: the sentinel guarantees a full 1.1.0 allocation, including private_data, where our
    // error export stores an ErrorDetails box. The caller keeps it alive for 'a.
    unsafe { (*error).private_data.cast::<ErrorDetails>().as_ref() }
}

/// # Safety
/// `error` must satisfy [`details`]'s provenance and allocation contract for this call.
// Guard-exempt: pure reads of driver-owned allocations; the fallible conversion is saturating,
// so nothing here can panic.
pub(crate) unsafe extern "C" fn error_get_detail_count(error: *const AdbcError) -> c_int {
    // SAFETY: the caller supplies our initialized/released error and keeps its storage live.
    let Some(details) = (unsafe { details(error) }) else {
        return 0;
    };
    c_int::try_from(details.len()).unwrap_or(c_int::MAX)
}

/// # Safety
/// `error` must satisfy [`details`]'s provenance and allocation contract. Returned pointers may
/// only be read while the error remains unreleased and unmodified.
// Guard-exempt: pure reads of driver-owned allocations through checked lookups; nothing here can
// panic.
pub(crate) unsafe extern "C" fn error_get_detail(
    error: *const AdbcError,
    index: c_int,
) -> AdbcErrorDetail {
    // SAFETY: the caller supplies our initialized/released error and keeps its storage live.
    let detail =
        unsafe { details(error) }.and_then(|details| details.get(usize::try_from(index).ok()?));
    match detail {
        Some((key, value)) => AdbcErrorDetail {
            key: key.as_ptr(),
            value: value.as_ptr(),
            value_length: value.len(),
        },
        None => AdbcErrorDetail {
            key: std::ptr::null(),
            value: std::ptr::null(),
            value_length: 0,
        },
    }
}

#[cfg(test)]
mod tests;
