//! Panic-contained Arrow C Stream export.
//!
//! Arrow-rs's exporter calls the [`RecordBatchReader`] directly from `extern "C"` callbacks, where
//! a reader panic aborts the host. Every driver stream uses the contained callbacks here instead.

use std::ffi::{CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};

use adbc_core::error::{Error, Status};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, Schema};

use libc::{EACCES, ECANCELED, EEXIST, EINVAL, EIO, ENOENT, ENOMEM, ENOSYS, ENOTSUP, ETIMEDOUT};

use super::abi::{
    ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, ADBC_STATUS_OK, AdbcDriver, AdbcError, AdbcStatusCode,
};
use super::error::export_error;
use super::guard::{panic_message, sanitized_cstring};
use crate::error::{chain, driver_defect};

/// Hand a driver-produced reader to the caller as an Arrow C stream.
///
/// The stream carries the handle's `private_driver` for [`error_from_array_stream`]; see
/// [`super::options::driver_of`].
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and `out` must be null or point to a
/// writable `FFI_ArrowArrayStream`.
pub(super) unsafe fn export_stream<H: super::options::FfiHandle>(
    handle: *mut H,
    reader: Box<dyn RecordBatchReader + Send>,
    out: *mut FFI_ArrowArrayStream,
) -> adbc_core::error::Result<()> {
    // SAFETY: The caller supplies a live handle; only its vtable pointer is read, never
    // dereferenced here.
    let stream = export_reader(reader, unsafe { super::options::driver_of(handle) });
    // On a null `out` the stream was never handed over, so it is dropped here, not leaked.
    // SAFETY: The caller grants writable stream output storage; ownership moves to it on success.
    unsafe { super::guard::write_out(out, stream, "out") }
}

/// Move a driver-produced schema into the caller's out parameter.
///
/// A schema the driver itself produced but cannot export is a driver-side failure, not a caller
/// mistake, so the `ArrowError` becomes `Status::Internal`.
///
/// # Safety
/// `out` must be null or point to writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe fn write_schema(
    out: *mut FFI_ArrowSchema,
    schema: &Schema,
) -> adbc_core::error::Result<()> {
    let exported = FFI_ArrowSchema::try_from(schema).map_err(|failure| {
        Error::with_message_and_status(
            format!("failed to export the schema over the C data interface: {failure}"),
            Status::Internal,
        )
    })?;
    // SAFETY: The caller grants writable schema output storage; ownership moves to it on success.
    unsafe { super::guard::write_out(out, exported, "schema") }
}

/// A failed callback, kept in both the shapes the ABI can ask for it in: the C string
/// `get_last_error` returns, and the structured error `error_from_array_stream` exports.
struct LastError {
    message: CString,
    error: Error,
}

/// The release callback of this module's streams, by which [`error_from_array_stream`] recognizes
/// them. Installing and comparing this one value, rather than reifying [`release`] at each site,
/// keeps the comparison exact where function addresses are not unique, as under Miri.
static RELEASE: unsafe extern "C" fn(*mut FFI_ArrowArrayStream) = release;

/// Tags [`PrivateData`] so [`error_from_array_stream`] can recognize this driver's own streams.
/// The release-callback address is the primary check; the magic guards against a linker folding
/// a byte-identical foreign release function onto that address.
const STREAM_MAGIC: u64 = 0x4249_4751_5354_524d; // "BIGQSTRM"

struct PrivateData {
    magic: u64,
    reader: Box<dyn RecordBatchReader + Send>,
    /// The producing handle's driver vtable, forwarded into the `AdbcError` that
    /// [`error_from_array_stream`] returns.
    private_driver: *const AdbcDriver,
    last_error: Option<LastError>,
    /// Backing store for the `AdbcError` that [`error_from_array_stream`] returns, which must stay
    /// valid until the stream is released.
    exported_error: Option<Box<AdbcError>>,
    panicked: bool,
}

impl Drop for PrivateData {
    fn drop(&mut self) {
        if let Some(mut exported) = self.exported_error.take()
            && let Some(release) = exported.release
        {
            // SAFETY: This owned error box was populated by export_error; its matching release runs
            // once before drop.
            unsafe { release(&raw mut *exported) };
        }
    }
}

pub(super) fn export_reader(
    reader: Box<dyn RecordBatchReader + Send>,
    private_driver: *const AdbcDriver,
) -> FFI_ArrowArrayStream {
    let private_data = Box::new(PrivateData {
        magic: STREAM_MAGIC,
        reader,
        private_driver,
        last_error: None,
        exported_error: None,
        panicked: false,
    });
    // SAFETY: each callback reads `private_data` as the `PrivateData` boxed here, which `release`
    // alone frees, once, before marking the stream released.
    unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            Some(get_schema),
            Some(get_next),
            Some(get_last_error),
            Some(RELEASE),
            Box::into_raw(private_data).cast::<c_void>(),
        )
    }
}

/// # Safety
/// `stream` must satisfy [`private_data`]'s contract for this call. `out` must be null or
/// exclusively writable schema storage, disjoint from the stream and its owned data.
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    // SAFETY: The caller grants exclusive access to our live stream and writable Arrow output; null
    // output is rejected.
    unsafe {
        catch_callback(stream, |private| {
            if out.is_null() {
                return private.fail("Arrow C Stream schema output must not be null");
            }
            match FFI_ArrowSchema::try_from(private.reader.schema().as_ref()) {
                Ok(schema) => {
                    std::ptr::write_unaligned(out, schema);
                    0
                }
                Err(error) => private.arrow_error(&error),
            }
        })
    }
}

/// # Safety
/// `stream` must satisfy [`private_data`]'s contract for this call. `out` must be null or
/// exclusively writable array storage, disjoint from the stream and its owned data.
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    // SAFETY: The caller grants exclusive access to our live stream and writable Arrow output; null
    // output is rejected.
    unsafe {
        catch_callback(stream, |private| {
            // After a successful `get_next`, `error_from_array_stream` must report OK
            // (adbc.h:398-399); failing exits record their own error. `get_schema` does not
            // clear, since it may follow a failed `get_next`.
            private.last_error = None;
            if out.is_null() {
                return private.fail("Arrow C Stream array output must not be null");
            }
            match private.reader.next() {
                None => {
                    std::ptr::write_unaligned(out, FFI_ArrowArray::empty());
                    0
                }
                Some(Ok(batch)) => {
                    let data = StructArray::from(batch).into_data();
                    std::ptr::write_unaligned(out, FFI_ArrowArray::new(&data));
                    0
                }
                Some(Err(error)) => private.arrow_error(&error),
            }
        })
    }
}

/// # Safety
/// `stream` must satisfy [`private_data`]'s contract for this call. A returned non-null message
/// may only be read until the next callback on the stream, including release.
// Guard-exempt: null checks and field reads, none of which can panic.
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // SAFETY: This callback is installed only on our streams; the caller keeps them live with
    // exclusive access.
    unsafe { private_data(stream) }
        .and_then(|private| private.last_error.as_ref())
        .map_or(std::ptr::null(), |error| error.message.as_ptr())
}

/// The ADBC 1.1.0 `ErrorFromArrayStream` entry point: expose the structured error behind a
/// failed stream callback, which the Arrow C Stream Interface itself can only report as an
/// errno and a message.
///
/// Recognize streams by their [`RELEASE`] callback and [`STREAM_MAGIC`]. Unrecognized or released
/// streams return null with `status` untouched.
///
/// Identical code folding (`-Wl,--icf=all`, see BINARY-SIZE.md) could merge [`release`] with
/// another stream release callback in this artifact, and the `fn_addr_eq` below would then read
/// that stream's private data as [`PrivateData`]. None exists; re-check `--print-icf-sections` if
/// one is added.
///
/// # Safety
/// `stream` must be null or point to a valid or released `ArrowArrayStream`; `status` must be
/// null or writable. The caller must serialize stream access and preserve the producer's
/// callback/private-data pairing; only streams from this module may carry its release callback.
pub(super) unsafe extern "C" fn error_from_array_stream(
    stream: *mut FFI_ArrowArrayStream,
    status: *mut AdbcStatusCode,
) -> *const AdbcError {
    // SAFETY: The caller keeps the initialized stream struct readable; as_ref handles null.
    let Some(stream_ref) = (unsafe { stream.as_ref() }) else {
        return std::ptr::null();
    };
    let ours = stream_ref
        .release()
        .is_some_and(|callback| std::ptr::fn_addr_eq(callback, RELEASE));
    if !ours {
        return std::ptr::null();
    }
    catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The matching release callback identifies our live PrivateData; the caller
        // serializes stream access.
        let Some(private) = (unsafe { private_data(stream) }) else {
            return std::ptr::null();
        };
        if private.magic != STREAM_MAGIC {
            return std::ptr::null();
        }
        // The error lives in `PrivateData` to outlive this call; each export releases the
        // previous one.
        let private_driver = private.private_driver;
        let exported = private.exported_error.get_or_insert_with(|| {
            Box::new(AdbcError {
                message: std::ptr::null_mut(),
                vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
                sqlstate: [0; 5],
                release: None,
                private_data: std::ptr::null_mut(),
                private_driver,
            })
        });
        match private.last_error.as_ref() {
            None => {
                // A recognized stream with nothing pending reports ADBC_STATUS_OK and an
                // empty error, mirroring the reference C++ drivers.
                if let Some(release) = exported.release {
                    // SAFETY: The owned error box retains the matching callback from its last
                    // export; release precedes reuse.
                    unsafe { release(&raw mut **exported) };
                }
                // release_error frees owned storage but leaves SQLSTATE; clear the stale code.
                exported.sqlstate = [0; 5];
                if !status.is_null() {
                    // SAFETY: Null was excluded and the caller grants writable status output
                    // storage.
                    unsafe { std::ptr::write_unaligned(status, ADBC_STATUS_OK) };
                }
            }
            Some(last) => {
                // SAFETY: This exclusively owned box has the full 1.1.0 layout and sentinel, with
                // matching release ownership.
                unsafe { export_error(&raw mut **exported, last.error.clone()) };
                if !status.is_null() {
                    // SAFETY: Null was excluded and the caller grants writable status output
                    // storage.
                    unsafe {
                        std::ptr::write_unaligned(status, AdbcStatusCode::from(last.error.status));
                    }
                }
            }
        }
        // Callers write through this pointer despite its `const`: the C driver manager sets
        // `private_driver`, and Python releases the error.
        std::ptr::from_mut::<AdbcError>(&mut **exported).cast_const()
    }))
    .unwrap_or(std::ptr::null())
}

/// # Safety
/// `stream` must be null or point to an exclusively accessible stream from [`export_reader`],
/// with its original callback/private-data pairing, or to its already-released struct.
unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    // SAFETY: Null was excluded; this callback receives our live, exclusively accessible stream
    // struct.
    let private_data = unsafe { (*stream).private_data() };
    // Marks the stream released, every callback null. Written rather than assigned: assigning
    // would drop the old value, and dropping calls this callback again.
    // SAFETY: The caller grants exclusive stream access. Overwrite without Drop to prevent
    // recursive release.
    unsafe { std::ptr::write(stream, FFI_ArrowArrayStream::empty()) };
    if !private_data.is_null() {
        // A reader can own arbitrary client code. Its destructor must not unwind through `release`.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: export_reader created this box; clearing the stream first transfers its sole
            // release ownership.
            drop(unsafe { Box::from_raw(private_data.cast::<PrivateData>()) });
        }));
    }
}

/// # Safety
/// `stream` must satisfy [`private_data`]'s contract for this call. `callback` must not access
/// the stream's private data through another pointer while it holds the mutable borrow.
unsafe fn catch_callback(
    stream: *mut FFI_ArrowArrayStream,
    callback: impl FnOnce(&mut PrivateData) -> c_int,
) -> c_int {
    // SAFETY: This callback is installed only on our streams; the caller keeps them live with
    // exclusive access.
    let Some(private) = (unsafe { private_data(stream) }) else {
        return EINVAL;
    };
    if private.panicked {
        return EINVAL;
    }
    match catch_unwind(AssertUnwindSafe(|| callback(private))) {
        Ok(code) => code,
        Err(cause) => {
            private.panicked = true;
            private.set_error(driver_defect(format_args!(
                "uncaught panic in the BigQuery Arrow stream: {}",
                panic_message(cause.as_ref())
            )));
            EINVAL
        }
    }
}

/// The [`PrivateData`] behind one of this module's streams, or `None` for a null stream and for
/// a released or zero-initialized one, whose `private_data` is null.
///
/// # Safety
/// `stream` must be null or point to a stream whose non-null `private_data` is a live
/// `PrivateData` box from [`export_reader`]. The caller grants exclusive access for `'a` and
/// keeps the stream and box alive throughout that borrow.
unsafe fn private_data<'a>(stream: *mut FFI_ArrowArrayStream) -> Option<&'a mut PrivateData> {
    // SAFETY: The caller supplies a live initialized stream for the returned borrow, or null.
    let stream = unsafe { stream.as_ref() }?;
    // SAFETY: The caller guarantees our live PrivateData box and exclusive access for the returned
    // lifetime.
    unsafe { stream.private_data().cast::<PrivateData>().as_mut() }
}

impl PrivateData {
    fn set_error(&mut self, error: Error) {
        self.last_error = Some(LastError {
            message: sanitized_cstring(&error.message),
            error,
        });
    }

    fn fail(&mut self, message: impl Into<String>) -> c_int {
        self.set_error(Error::with_message_and_status(
            message.into(),
            Status::InvalidArguments,
        ));
        EINVAL
    }

    fn arrow_error(&mut self, error: &ArrowError) -> c_int {
        // Preserve an embedded ADBC error's status and details without Arrow's Display wrapper.
        if let Some(embedded) = chain(error).find_map(|source| source.downcast_ref::<Error>()) {
            let errno = adbc_status_to_errno(embedded.status);
            self.set_error(embedded.clone());
            return errno;
        }

        // Only these variants say what failed reliably: the driver also raises `ComputeError` for
        // cancellation and `SchemaError` for unsupported types.
        let status = match error {
            ArrowError::NotYetImplemented(_) => Status::NotImplemented,
            ArrowError::MemoryError(_) => Status::Internal,
            ArrowError::IoError(_, _) => Status::IO,
            ArrowError::ParseError(_)
            | ArrowError::CastError(_)
            | ArrowError::IpcError(_)
            | ArrowError::DivideByZero
            | ArrowError::ArithmeticOverflow(_)
            | ArrowError::DictionaryKeyOverflowError
            | ArrowError::RunEndIndexOverflowError
            | ArrowError::OffsetOverflowError(_) => Status::InvalidData,
            _ => Status::Unknown,
        };
        // The errno follows the status as for an embedded ADBC error, except where Arrow's own
        // exporter has a dedicated code: NotYetImplemented is ENOSYS, where an ADBC NotImplemented
        // is ENOTSUP.
        let errno = match error {
            ArrowError::NotYetImplemented(_) => ENOSYS,
            ArrowError::MemoryError(_) => ENOMEM,
            _ => adbc_status_to_errno(status),
        };
        self.set_error(Error::with_message_and_status(error.to_string(), status));
        errno
    }
}

fn adbc_status_to_errno(status: Status) -> c_int {
    match status {
        Status::Ok | Status::Unknown => EIO,
        Status::NotImplemented => ENOTSUP,
        Status::NotFound => ENOENT,
        Status::AlreadyExists => EEXIST,
        Status::InvalidArguments | Status::InvalidState => EINVAL,
        Status::InvalidData | Status::Integrity | Status::Internal | Status::IO => EIO,
        Status::Cancelled => ECANCELED,
        Status::Timeout => ETIMEDOUT,
        Status::Unauthenticated | Status::Unauthorized => EACCES,
    }
}

#[cfg(test)]
mod tests;
