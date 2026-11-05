//! Panic containment and C argument conversion. [`super::handle`] poisons only the handles
//! borrowed by a panicking call; other handles remain usable.

use std::any::Any;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::AssertUnwindSafe;

use adbc_core::error::{Error, Result};

use crate::error::{driver_defect, invalid_argument};

/// Run `f`, converting a panic into an `Internal` error rather than letting it cross the ABI.
pub(crate) fn catch<T>(f: impl FnOnce() -> Result<T>) -> std::result::Result<Result<T>, Error> {
    std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(caught_panic)
}

// Out of line so every `catch` instantiation shares one copy, including the payload drop.
#[cold]
#[inline(never)]
fn caught_panic(cause: Box<dyn Any + Send>) -> Error {
    let failure = driver_defect(format_args!(
        "uncaught panic in the BigQuery driver: {}",
        panic_message(cause.as_ref())
    ));
    drop(cause);
    failure
}

/// Extract a string panic payload for entry-point and Arrow stream errors.
pub(crate) fn panic_message(cause: &(dyn Any + Send)) -> &str {
    cause
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| cause.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

/// Replace interior NULs with `?` so a vendor diagnostic survives C string conversion.
pub(crate) fn sanitized_cstring(text: &str) -> CString {
    CString::new(text).unwrap_or_else(|failure| {
        let mut bytes = failure.into_vec();
        for byte in &mut bytes {
            if *byte == 0 {
                *byte = b'?';
            }
        }
        CString::new(bytes).unwrap_or_default()
    })
}

/// Borrow a required C string argument.
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string. Its bytes through the terminator must
/// be readable within one allocation, span at most `isize::MAX` bytes, and stay unmodified for `'a`.
#[inline(never)]
pub(crate) unsafe fn required_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str> {
    if ptr.is_null() {
        return Err(missing_argument(name));
    }
    // SAFETY: null was rejected; the caller guarantees a readable NUL-terminated string for 'a.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|error| invalid_argument(format!("{name} must be valid UTF-8: {error}")))
}

/// Borrow an optional C string argument, where null means "not specified".
///
/// # Safety
/// `ptr` must satisfy [`required_str`]'s allocation and borrowing contract for `'a`.
#[inline(never)]
pub(crate) unsafe fn optional_str<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>> {
    if ptr.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller supplies the same string allocation and lifetime required by required_str.
    unsafe { required_str(ptr, name) }.map(Some)
}

/// Borrow a NULL-terminated array of C strings, as `AdbcConnectionGetObjects` takes for
/// `table_types`.
///
/// # Safety
/// `ptr` must be null or point to an aligned, NULL-terminated pointer array in one allocation,
/// readable and unmodified for this call. Each non-null entry must satisfy [`required_str`]'s
/// allocation and borrowing contract for `'a`.
pub(crate) unsafe fn optional_str_list<'a>(
    ptr: *const *const c_char,
    name: &str,
) -> Result<Option<Vec<&'a str>>> {
    if ptr.is_null() {
        return Ok(None);
    }
    let mut items = Vec::new();
    let mut cursor = ptr;
    loop {
        // SAFETY: the caller supplies a readable pointer array through its null terminator.
        let entry = unsafe { *cursor };
        if entry.is_null() {
            break;
        }
        // SAFETY: each non-null entry is a NUL-terminated string that outlives 'a.
        items.push(unsafe { required_str(entry, name) }?);
        // SAFETY: a non-null entry precedes the terminator, so the next element is in the array.
        cursor = unsafe { cursor.add(1) };
    }
    Ok(Some(items))
}

/// Borrow a pointer/length byte argument, treating a null pointer as empty only when the length
/// agrees.
///
/// # Safety
/// `ptr` must be null or point to `len` initialized bytes in one allocation that outlive `'a`.
/// The size must not exceed `isize::MAX`, and the bytes must not be mutated during the borrow.
pub(crate) unsafe fn byte_slice<'a>(ptr: *const u8, len: usize, name: &str) -> Result<&'a [u8]> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(invalid_argument(format!(
            "{name} must not be null for a length of {len}"
        )));
    }
    // SAFETY: null was handled above; the caller guarantees the readable allocation and lifetime.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Validate GetOption's buffer capacity. A null buffer is legal only for a zero-length size
/// probe: adbc.h:1185-1189 promises to write the value whenever it fits the advertised capacity.
///
/// # Safety
/// `length` must be null or point to a readable, writable `usize`.
unsafe fn available_space(dst: *const c_void, length: *mut usize) -> Result<usize> {
    if length.is_null() {
        return Err(missing_argument("length"));
    }
    // SAFETY: null was rejected and the caller guarantees an initialized, aligned usize.
    let available = unsafe { *length };
    if dst.is_null() && available > 0 {
        return Err(invalid_argument(format!(
            "value must not be null for a buffer length of {available}; pass a length of 0 to ask \
             only for the size required"
        )));
    }
    Ok(available)
}

/// Write a string using the ADBC GetOption protocol: `length` is in/out and always receives the
/// required size including the NUL; the buffer is written only if the value fits.
///
/// # Safety
/// `length` must be null or an exclusively accessible, initialized `usize`. `dst` must be null
/// or point to at least `*length` writable bytes disjoint from `value` and `length`.
pub(crate) unsafe fn write_string(value: &str, dst: *mut c_char, length: *mut usize) -> Result<()> {
    // SAFETY: the caller supplies readable/writable length storage or null, which is rejected.
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    let bytes = value.as_bytes();
    if let Some(position) = bytes.iter().position(|&byte| byte == 0) {
        return Err(driver_defect(format_args!(
            "option value contains an interior NUL at byte {position}, which cannot be \
             returned as a C string"
        )));
    }
    let required = bytes.len() + 1;
    // A buffer with room for the terminator is non-null after `available_space`.
    if required <= available {
        // SAFETY: the caller supplies a disjoint buffer of `available` bytes; the size check
        // includes the terminator, and available_space rejects a null buffer with capacity.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), dst, bytes.len());
            dst.add(bytes.len()).write(0);
        }
    }
    // SAFETY: available_space rejected null; the caller grants exclusive access to length.
    unsafe { *length = required };
    Ok(())
}

/// The byte-valued counterpart of [`write_string`]. No NUL terminator is involved.
///
/// # Safety
/// `length` must be null or an exclusively accessible, initialized `usize`. `dst` must be null
/// or point to at least `*length` writable bytes disjoint from `value` and `length`.
pub(crate) unsafe fn write_bytes(value: &[u8], dst: *mut u8, length: *mut usize) -> Result<()> {
    // SAFETY: the caller supplies readable/writable length storage or null, which is rejected.
    let available = unsafe { available_space(dst.cast::<c_void>(), length) }?;
    // `copy_nonoverlapping` requires a non-null destination even for an empty value.
    if !dst.is_null() && value.len() <= available {
        // SAFETY: the non-null destination has enough caller-provided, disjoint writable storage.
        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), dst, value.len()) };
    }
    // SAFETY: available_space rejected null; the caller grants exclusive access to length.
    unsafe { *length = value.len() };
    Ok(())
}

/// Reject a missing output before performing work that the caller cannot collect.
pub(crate) fn required_output<T>(dst: *mut T, name: &str) -> Result<()> {
    if dst.is_null() {
        return Err(missing_argument(name));
    }
    Ok(())
}

/// The error for a required pointer argument that is null, shared by every generic caller.
#[cold]
#[inline(never)]
pub(crate) fn missing_argument(name: &str) -> Error {
    invalid_argument(format!("{name} must not be null"))
}

/// Write a value into a caller-provided out parameter, rejecting a null pointer.
///
/// # Safety
/// `dst` must be null or point to a writable, correctly aligned `T`.
pub(crate) unsafe fn write_out<T>(dst: *mut T, value: T, name: &str) -> Result<()> {
    required_output(dst, name)?;
    // SAFETY: null was rejected and the caller guarantees writable storage for T.
    unsafe { std::ptr::write_unaligned(dst, value) };
    Ok(())
}

#[cfg(test)]
mod tests;
