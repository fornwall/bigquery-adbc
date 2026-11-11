use std::ffi::CStr;
use std::sync::Arc;

use adbc_core::error::Error;
use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::*;
use crate::ffi::abi::{ADBC_STATUS_CANCELLED, ADBC_STATUS_INVALID_DATA, ADBC_STATUS_IO};
use crate::ffi::error::{error_get_detail, error_get_detail_count};
use crate::ffi::test_support::{c_string, null};

fn error_stream(error: ArrowError) -> FFI_ArrowArrayStream {
    export_reader(Box::new(RecordBatchIterator::new(
        std::iter::once(Err(error)),
        Arc::new(Schema::empty()),
    )))
}

fn one_row_batch() -> RecordBatch {
    RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
        .unwrap()
}

fn one_row_stream() -> FFI_ArrowArrayStream {
    let batch = one_row_batch();
    let schema = batch.schema();
    export_reader(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )))
}

/// Call `get_next` on one of this module's live streams, releasing any array it returns.
fn next(stream: &mut FFI_ArrowArrayStream) -> c_int {
    let mut out = FFI_ArrowArray::empty();
    // SAFETY: The callback belongs to this live stream and receives an empty, writable array
    // destination, which owns whatever the call returns.
    unsafe { stream.get_next().unwrap()(stream, &raw mut out) }
}

/// Call `get_schema` on one of this module's live streams, releasing any schema it returns.
fn schema_of(stream: &mut FFI_ArrowArrayStream) -> c_int {
    let mut out = FFI_ArrowSchema::empty();
    // SAFETY: The callback belongs to this live stream and receives an empty, writable schema
    // destination, which owns whatever the call returns.
    unsafe { stream.get_schema().unwrap()(stream, &raw mut out) }
}

/// What `get_last_error` reports for one of this module's live streams.
fn last_error(stream: &mut FFI_ArrowArrayStream) -> Option<String> {
    // SAFETY: The callback belongs to this live stream, and returns null or a NUL-terminated
    // message the stream keeps until its next call.
    unsafe { c_string(stream.get_last_error().unwrap()(stream)) }
}

/// `AdbcErrorFromArrayStream`'s status (`0xEE` if it wrote none) and error.
fn error_from(stream: &mut FFI_ArrowArrayStream) -> (AdbcStatusCode, *const AdbcError) {
    let mut status: AdbcStatusCode = 0xEE;
    // SAFETY: The stream struct is live and exclusively borrowed; `status` is a writable local.
    let error = unsafe { error_from_array_stream(stream, &raw mut status) };
    (status, error)
}

/// The message of an error `error_from` returned, which the stream still owns.
fn message_of(error: *const AdbcError) -> Option<String> {
    // SAFETY: Callers pass a non-null error owned by a stream that is still live, whose message
    // is null or a NUL-terminated string it owns.
    unsafe { c_string((*error).message) }
}

fn release(stream: &mut FFI_ArrowArrayStream) {
    // SAFETY: This live stream owns its callback and private data; exclusive access lets release
    // clear ownership.
    unsafe { stream.release().unwrap()(stream) };
}

struct PanickingReader {
    schema: SchemaRef,
    panic_in_schema: bool,
    panic_in_drop: bool,
}

impl Drop for PanickingReader {
    fn drop(&mut self) {
        assert!(!self.panic_in_drop, "drop failed");
    }
}

impl Iterator for PanickingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("next contains a NUL: \0")
    }
}

impl RecordBatchReader for PanickingReader {
    fn schema(&self) -> SchemaRef {
        assert!(!self.panic_in_schema, "schema failed");
        self.schema.clone()
    }
}

#[test]
fn exports_an_ordinary_reader() {
    let batch = one_row_batch();
    let mut stream = one_row_stream();
    // SAFETY: The stream is a valid Arrow export. `from_raw` moves its ownership into the reader
    // and clears the source, which remains allocated throughout the move.
    let mut reader = unsafe { RecordBatchStreamReader::from_raw(&raw mut stream) }.unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
}

#[test]
fn exports_adbc_statuses_as_errno() {
    for (status, expected) in [
        (Status::Ok, EIO),
        (Status::Unknown, EIO),
        (Status::NotImplemented, ENOTSUP),
        (Status::NotFound, ENOENT),
        (Status::AlreadyExists, EEXIST),
        (Status::InvalidArguments, EINVAL),
        (Status::InvalidState, EINVAL),
        (Status::InvalidData, EIO),
        (Status::Integrity, EIO),
        (Status::Internal, EIO),
        (Status::IO, EIO),
        (Status::Cancelled, ECANCELED),
        (Status::Timeout, ETIMEDOUT),
        (Status::Unauthenticated, EACCES),
        (Status::Unauthorized, EACCES),
    ] {
        let error = Error::with_message_and_status("ADBC stream failure", status);
        let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
        assert_eq!(next(&mut stream), expected, "{status:?}");
        // The driver's own message, without the Arrow `External error: ...` wrapping.
        assert_eq!(last_error(&mut stream).unwrap(), "ADBC stream failure");
    }

    // The ADBC status is found even when nested behind another external error.
    let error = Error::with_message_and_status("cancelled", Status::Cancelled);
    let error = ArrowError::ExternalError(Box::new(ArrowError::ExternalError(Box::new(error))));
    assert_eq!(next(&mut error_stream(error)), ECANCELED);

    // A plain Arrow error keeps its message.
    let mut stream = error_stream(ArrowError::NotYetImplemented("not supported".into()));
    assert_eq!(next(&mut stream), ENOSYS);
    let message = last_error(&mut stream).unwrap();
    assert!(message.contains("not supported"), "{message}");
}

/// A plain Arrow error gets the status its variant reliably implies, and the errno that status
/// has for an embedded ADBC error, except for Arrow's own ENOSYS and ENOMEM.
#[test]
fn exports_plain_arrow_errors_by_what_they_say_failed() {
    let io = || std::io::Error::other("connection reset");
    let x = || "x".to_owned();
    for (error, errno, status) in [
        (
            ArrowError::NotYetImplemented(x()),
            ENOSYS,
            Status::NotImplemented,
        ),
        (ArrowError::MemoryError(x()), ENOMEM, Status::Internal),
        (ArrowError::IoError(x(), io()), EIO, Status::IO),
        (ArrowError::ParseError(x()), EIO, Status::InvalidData),
        (ArrowError::CastError(x()), EIO, Status::InvalidData),
        (ArrowError::IpcError(x()), EIO, Status::InvalidData),
        (ArrowError::OffsetOverflowError(1), EIO, Status::InvalidData),
        // Variants the driver raises for unrelated failures say too little to be specific.
        (ArrowError::ComputeError(x()), EIO, Status::Unknown),
        (
            ArrowError::ExternalError(Box::new(io())),
            EIO,
            Status::Unknown,
        ),
    ] {
        let label = format!("{error:?}");
        let mut stream = error_stream(error);
        assert_eq!(next(&mut stream), errno, "{label}");
        assert_eq!(
            error_from(&mut stream).0,
            AdbcStatusCode::from(status),
            "{label}"
        );
        release(&mut stream);
    }
}

/// A nonconforming host may keep the callback pointer and call it after release, when
/// `private_data` is already null; that must yield a null message, not undefined behavior.
#[test]
fn get_last_error_tolerates_a_released_or_null_stream() {
    let mut stream = error_stream(ArrowError::ComputeError("boom".to_owned()));
    let saved = stream.get_last_error().unwrap();
    release(&mut stream);
    // SAFETY: The saved driver callback checks the cleared private-data pointer before using the
    // released stream.
    assert!(unsafe { saved(&raw mut stream) }.is_null());
    // SAFETY: This driver callback explicitly accepts a null stream.
    assert!(unsafe { saved(null()) }.is_null());
}

#[test]
fn error_from_array_stream_exposes_the_pending_error() {
    let mut error = Error::with_message_and_status("stream blew up", Status::Cancelled);
    error.details = Some(vec![("grpc-status".to_string(), b"1".to_vec())]);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
    assert_eq!(next(&mut stream), ECANCELED);

    let (status, exported) = error_from(&mut stream);
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_CANCELLED);
    assert_eq!(message_of(exported).unwrap(), "stream blew up");
    // SAFETY: `exported` points to the live stream-owned error, which retains its details through
    // these reads.
    let detail = unsafe {
        assert_eq!(error_get_detail_count(exported), 1);
        error_get_detail(exported, 0)
    };
    assert_eq!(
        // SAFETY: The detail key is a NUL-terminated string retained by the live stream-owned
        // error.
        unsafe { CStr::from_ptr(detail.key) }.to_str().unwrap(),
        "grpc-status"
    );
    assert_eq!(
        // SAFETY: The detail covers `value_length` initialized bytes retained by the live
        // stream-owned error.
        unsafe { std::slice::from_raw_parts(detail.value, detail.value_length) },
        b"1"
    );

    // Asking again re-exports the same error without leaking the previous export, and a null
    // status pointer is not written.
    // SAFETY: The stream is live and exclusively accessed; a null status output is explicitly
    // accepted.
    let exported = unsafe { error_from_array_stream(&raw mut stream, null()) };
    assert!(!exported.is_null());
    // SAFETY: `exported` points to the live stream-owned error, which retains its details through
    // this read.
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);

    // Releasing the stream invalidates the error and de-recognizes the stream.
    release(&mut stream);
    let (status, exported) = error_from(&mut stream);
    assert!(exported.is_null());
    assert_eq!(
        status, 0xEE,
        "status stays untouched for an unrecognized stream"
    );
}

/// adbc.h invalidates an error's fields only at its release callback, so asking again must return
/// what the first call did, not free it to export a copy.
#[test]
fn asking_again_keeps_the_error_already_exported() {
    let failure = Error::with_message_and_status("the read failed", Status::IO);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(failure)));
    assert_eq!(next(&mut stream), EIO);

    let (_, exported) = error_from(&mut stream);
    // SAFETY: The stream-owned message is live, writable and not empty. A fresh export would not
    // carry this mark.
    unsafe { (*exported).message.write(b'T'.cast_signed()) };
    let (status, again) = error_from(&mut stream);
    assert_eq!(status, ADBC_STATUS_IO);
    assert_eq!(message_of(again).unwrap(), "The read failed");
    release(&mut stream);
}

/// adbc.h says the caller must not release the error `AdbcErrorFromArrayStream` returns, but
/// Python's `adbc_driver_manager` does (`convert_error` in `_lib.pyx`). The stream must neither
/// free it a second time nor lose a later export.
#[test]
fn an_error_from_the_stream_survives_a_caller_releasing_it() {
    let failure = Error::with_message_and_status("the read failed", Status::IO);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(failure)));
    assert_eq!(next(&mut stream), EIO);

    for _ in 0..2 {
        let (status, exported) = error_from(&mut stream);
        assert_eq!(status, ADBC_STATUS_IO);
        assert_eq!(message_of(exported).unwrap(), "the read failed");
        // SAFETY: The stream-owned error is live and was populated with this driver's callback.
        unsafe { (*exported).release.unwrap()(exported.cast_mut()) };
        assert!(message_of(exported).is_none());
    }
    release(&mut stream);
}

#[test]
fn error_from_array_stream_reports_ok_when_nothing_failed() {
    let mut stream = export_reader(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::empty()),
    )));
    let (status, exported) = error_from(&mut stream);
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(message_of(exported).is_none());
    release(&mut stream);
}

#[test]
fn error_from_array_stream_rejects_foreign_and_null_streams() {
    /// # Safety
    /// `stream` must point to an exclusively writable, aligned stream struct.
    unsafe extern "C" fn foreign_release(stream: *mut FFI_ArrowArrayStream) {
        // SAFETY: This callback is used only by the live fixture below. It owns no payload;
        // overwriting its exclusively writable descriptor clears release without recursive Drop.
        unsafe { std::ptr::write(stream, FFI_ArrowArrayStream::empty()) };
    }

    let mut status: AdbcStatusCode = 0xEE;
    // SAFETY: A null stream is explicitly accepted, and `status` is writable.
    assert!(unsafe { error_from_array_stream(null(), &raw mut status) }.is_null());

    // A stream some other library produced: valid pointers, but not this driver's release.
    // SAFETY: The only callback clears this stream without dereferencing its borrowed private
    // pointer. No payload needs freeing, and the absent data callbacks are never invoked.
    let mut foreign = unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            None,
            None,
            None,
            Some(foreign_release),
            (&raw mut status).cast::<c_void>(),
        )
    };
    let (status, error) = error_from(&mut foreign);
    assert!(error.is_null());
    assert_eq!(
        status, 0xEE,
        "status stays untouched for an unrecognized stream"
    );
}

#[test]
fn contains_reader_panics_in_get_next() {
    let mut stream = export_reader(Box::new(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
        panic_in_drop: false,
    }));
    assert_eq!(next(&mut stream), EINVAL);
    let message = last_error(&mut stream).unwrap();
    assert!(message.contains("next contains a NUL: ?"), "{message}");

    // Once a reader has panicked, do not enter its potentially inconsistent state again.
    assert_eq!(next(&mut stream), EINVAL);
}

#[test]
fn contains_reader_panics_in_get_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mut stream = export_reader(Box::new(PanickingReader {
        schema,
        panic_in_schema: true,
        panic_in_drop: false,
    }));
    assert_eq!(schema_of(&mut stream), EINVAL);
    let message = last_error(&mut stream).unwrap();
    assert!(message.contains("schema failed"), "{message}");
}

#[test]
fn contains_reader_panics_in_release() {
    let mut stream = export_reader(Box::new(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
        panic_in_drop: true,
    }));
    release(&mut stream);
    assert!(stream.release().is_none());
    assert!(stream.private_data().is_null());
}

/// A reader that records when it is dropped.
struct DropRecorder(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropRecorder {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Iterator for DropRecorder {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for DropRecorder {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

/// A `fork` child releasing an inherited stream empties it without dropping its reader, whose
/// destructors could wait on a lock held by a thread the child lacks. In the process that
/// exported it, release drops the reader.
#[test]
fn releasing_an_inherited_stream_does_not_drop_its_reader() {
    for (inherited, expect_dropped) in [(false, true), (true, false)] {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = export_reader(Box::new(DropRecorder(dropped.clone())));
        if inherited {
            // SAFETY: The live stream's private data is the `PrivateData` export_reader boxed,
            // and nothing else accesses it here. No process has ID 0.
            unsafe { (*stream.private_data().cast::<PrivateData>()).process = 0 };
        }
        release(&mut stream);
        assert!(stream.release().is_none() && stream.private_data().is_null());
        assert_eq!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            expect_dropped,
            "inherited: {inherited}"
        );
    }
}

/// A nonconforming host may release a stream twice, or call callback pointers it copied before
/// the release. The driver must survive both.
#[test]
fn a_released_stream_survives_a_second_release_and_stale_callbacks() {
    let mut stream = one_row_stream();
    let (get_next, get_schema, release) = (
        stream.get_next().unwrap(),
        stream.get_schema().unwrap(),
        stream.release().unwrap(),
    );

    // SAFETY: This live stream owns its callback and private data; exclusive access lets release
    // clear ownership.
    unsafe { release(&raw mut stream) };
    assert!(stream.private_data().is_null());
    assert!(stream.get_next().is_none());
    assert!(stream.get_schema().is_none());
    assert!(stream.release().is_none());

    // A second release is a no-op, not a double free.
    // SAFETY: The first release cleared ownership, and this driver callback explicitly handles that
    // state.
    unsafe { release(&raw mut stream) };
    assert!(stream.private_data().is_null());

    // Stale callbacks report EINVAL rather than dereferencing the null they were left with.
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();
    // SAFETY: The stream struct and outputs are writable locals; the driver callbacks check the
    // released state and reject a null stream before touching an output, and release accepts
    // null.
    unsafe {
        assert_eq!(get_next(&raw mut stream, &raw mut array), EINVAL);
        assert_eq!(get_schema(&raw mut stream, &raw mut schema), EINVAL);
        assert_eq!(get_next(null(), &raw mut array), EINVAL);
        assert_eq!(get_schema(null(), &raw mut schema), EINVAL);
        release(null());
    }
}

/// A successful read clears the previous failure: `AdbcErrorFromArrayStream` reports "the ADBC
/// status code, or ADBC_STATUS_OK if there is no error".
///
/// The failure carries a SQLSTATE because `release_error` leaves that field alone, so clearing it
/// is the export side's job.
#[test]
fn a_successful_read_clears_the_previous_failure() {
    // "22000", data exception, in the ASCII spelling `Error::sqlstate` takes.
    const SQLSTATE: [c_char; 5] = [0x32, 0x32, 0x30, 0x30, 0x30];

    let batch = one_row_batch();
    let schema = batch.schema();
    let mut failure = Error::with_message_and_status("the read failed", Status::InvalidData);
    failure.sqlstate = SQLSTATE;
    let mut stream = export_reader(Box::new(RecordBatchIterator::new(
        [Err(ArrowError::ExternalError(Box::new(failure))), Ok(batch)],
        schema,
    )));

    assert_eq!(next(&mut stream), EIO);
    assert!(last_error(&mut stream).is_some());
    // Export while pending, so the reused storage below starts out holding it.
    let (status, failed) = error_from(&mut stream);
    assert_eq!(status, ADBC_STATUS_INVALID_DATA);
    // SAFETY: The status assertion proves the recognized stream returned its live, initialized
    // error storage.
    assert_eq!(unsafe { (*failed).sqlstate }, SQLSTATE);

    // A null output also fails; the next successful read must still clear that error.
    assert_eq!(
        // SAFETY: The stream is live; this driver callback rejects a null array output before
        // writing.
        unsafe { stream.get_next().unwrap()(&raw mut stream, null()) },
        EINVAL
    );
    assert!(last_error(&mut stream).is_some());

    assert_eq!(next(&mut stream), 0, "the read returns the batch");
    assert!(last_error(&mut stream).is_none());

    let (status, exported) = error_from(&mut stream);
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(message_of(exported).is_none());
    // adbc.h:293 requires an unset SQLSTATE to be all-NUL, so the failed read's code must be
    // cleared from this reused struct.
    // SAFETY: The status assertion proves the recognized stream returned its live, initialized
    // error storage.
    assert_eq!(unsafe { (*exported).sqlstate }, [0; 5]);
    release(&mut stream);
}

/// Reading the schema after a failed `get_next` must not make the failure disappear.
///
/// `get_schema` may be called at any time and consumes nothing, so its success says nothing about
/// the read.
#[test]
fn reading_the_schema_after_a_failed_read_keeps_the_failure() {
    let error = Error::with_message_and_status("the query was cancelled", Status::Cancelled);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
    assert_eq!(next(&mut stream), ECANCELED);

    assert_eq!(schema_of(&mut stream), 0, "the schema stays readable");

    let message = last_error(&mut stream).expect("the read failure was cleared by get_schema");
    assert!(message.contains("the query was cancelled"), "{message}");
    let (status, exported) = error_from(&mut stream);
    assert_eq!(
        status, ADBC_STATUS_CANCELLED,
        "the read failure was reported as ADBC_STATUS_OK"
    );
    assert!(message_of(exported).is_some());
    release(&mut stream);
}

/// The error `AdbcErrorFromArrayStream` returns stays valid until the stream is released, so a
/// later callback that leaves it alone must not invalidate the caller's pointer. Miri checks the
/// pointer's provenance across the `get_schema` in between.
#[test]
fn an_exported_error_survives_a_later_callback_that_leaves_it_alone() {
    let mut failure = Error::with_message_and_status("the query was cancelled", Status::Cancelled);
    failure.details = Some(vec![("grpc-status".to_owned(), b"1".to_vec())]);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(failure)));
    assert_eq!(next(&mut stream), ECANCELED);
    let (_, exported) = error_from(&mut stream);

    assert_eq!(schema_of(&mut stream), 0);
    // SAFETY: The stream is live, so the error it owns is too; the C driver manager writes this
    // field through the pointer it was returned.
    unsafe { (*exported.cast_mut()).private_driver = std::ptr::null() };
    assert_eq!(
        message_of(exported).as_deref(),
        Some("the query was cancelled")
    );
    // SAFETY: As above; the stream still owns the error and its details.
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);

    let (_, again) = error_from(&mut stream);
    assert_eq!(again, exported, "every call returns the same error");
    release(&mut stream);
}
