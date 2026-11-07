//! Helpers shared by the test modules across this export layer.

use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::channel;

use adbc_core::CancelHandle;
use adbc_core::error::Result;
use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{ArrayRef, Int64Array, StructArray};
use arrow_schema::Field;

use super::abi::{ADBC_ERROR_INIT, ADBC_STATUS_OK, AdbcDatabase, AdbcError, AdbcStatusCode};
use super::database::database_init;
use super::handle::dispatch;
use super::options::{FfiHandle, get_option, new_handle, release_handle, set_option};

/// Run the driver's release callback on an out parameter, when one was set.
pub(super) fn release_error(error: &mut AdbcError) {
    if let Some(release) = error.release {
        // SAFETY: Test callers pass live driver-produced errors; the callback owns this error and
        // receives exclusive access to its full allocation.
        unsafe { release(&raw mut *error) };
    }
}

/// The message the driver exported into an out parameter, or `None` when it set none.
pub(super) fn error_message(error: &AdbcError) -> Option<String> {
    // SAFETY: Test callers retain the unreleased driver error, which owns any message it holds.
    unsafe { c_string(error.message) }
}

/// A handle struct as a caller hands it to `New`: zeroed.
pub(super) fn zeroed_handle<H: Default>() -> H {
    H::default()
}

/// A database or connection as `New` leaves it: allocated and not yet initialized.
pub(super) fn allocated<H: Default + FfiHandle<State: Default>>() -> H {
    let mut handle = zeroed_handle();
    // SAFETY: The handle is a writable local whose ownership slot starts empty.
    let status = unsafe { new_handle(&raw mut handle, std::ptr::null_mut()) };
    assert_eq!(status, ADBC_STATUS_OK);
    handle
}

/// A database authenticating anonymously against an endpoint nothing listens on: initialized
/// without credentials or network access.
pub(super) fn initialized_database() -> AdbcDatabase {
    database_at("http://127.0.0.1:1")
}

/// An initialized database authenticating anonymously against `endpoint`, such as a mock.
pub(super) fn database_at(endpoint: &str) -> AdbcDatabase {
    let mut database = allocated();
    for (key, value) in [
        ("uri", "bigquery:///a-project"),
        ("adbc.bigquery.sql.auth_type", "anonymous"),
        ("adbc.bigquery.sql.api_endpoint", endpoint),
    ] {
        assert_eq!(set(&mut database, key, value).0, ADBC_STATUS_OK);
    }
    // SAFETY: The handle is exclusively borrowed; the error output is optional.
    let status = unsafe { database_init(&raw mut database, std::ptr::null_mut()) };
    assert_eq!(status, ADBC_STATUS_OK);
    database
}

/// Call an entry point with a fresh 1.1.0 error, returning its status and the message it exported.
pub(super) fn call(
    entry_point: impl FnOnce(*mut AdbcError) -> AdbcStatusCode,
) -> (AdbcStatusCode, Option<String>) {
    let mut error = ADBC_ERROR_INIT;
    let status = entry_point(&raw mut error);
    let message = error_message(&error);
    release_error(&mut error);
    (status, message)
}

/// Assert that a [`call`] failed with `status` and a message containing `fragment`.
#[track_caller]
pub(super) fn assert_refused(
    (actual, message): (AdbcStatusCode, Option<String>),
    status: AdbcStatusCode,
    fragment: &str,
) {
    assert_eq!(actual, status, "{message:?}");
    let message = message.expect("the refusal must say why");
    assert!(message.contains(fragment), "{message}");
}

/// `Adbc*SetOption` with a string value.
pub(super) fn set<H: FfiHandle>(
    handle: &mut H,
    key: &str,
    value: &str,
) -> (AdbcStatusCode, Option<String>) {
    let (key, value) = (CString::new(key).unwrap(), CString::new(value).unwrap());
    call(|error| {
        // SAFETY: The handle is exclusively borrowed, and both C strings outlive the call.
        unsafe { set_option(handle, key.as_ptr(), value.as_ptr(), error) }
    })
}

/// `Adbc*GetOption`, first asking for the size as a caller does, or the failing call's status and
/// message.
pub(super) fn get<H: FfiHandle>(
    handle: &mut H,
    key: &str,
) -> std::result::Result<String, (AdbcStatusCode, Option<String>)> {
    let key = CString::new(key).unwrap();
    let mut buffer: Vec<c_char> = Vec::new();
    let mut length = 0;
    loop {
        let (status, message) = call(|error| {
            // SAFETY: The handle is exclusively borrowed, the key outlives the call, and the
            // buffer holds `length` bytes; an empty one is null and asks only for the size.
            unsafe {
                let value = if buffer.is_empty() {
                    std::ptr::null_mut()
                } else {
                    buffer.as_mut_ptr()
                };
                get_option(handle, key.as_ptr(), value, &raw mut length, error)
            }
        });
        if status != ADBC_STATUS_OK {
            return Err((status, message));
        }
        if length <= buffer.len() {
            // SAFETY: A value that fits was written with its NUL terminator.
            return Ok(unsafe { c_string(buffer.as_ptr()) }.unwrap());
        }
        buffer.resize(length, 0);
    }
}

/// `Adbc*Release`, which must succeed.
pub(super) fn release<H: FfiHandle>(handle: &mut H) {
    let (status, message) = call(|error| {
        // SAFETY: The handle is exclusively borrowed, so no other call on it is in flight.
        unsafe { release_handle(handle, error) }
    });
    assert_eq!(status, ADBC_STATUS_OK, "{message:?}");
}

/// Copy a C string the driver returned, or `None` for null.
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that stays live for the call.
pub(super) unsafe fn c_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: Null was excluded; the caller guarantees a live NUL-terminated string.
    Some(
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// Hold the dispatch turn of the object in `slot` on a worker, like a blocked `ExecuteQuery`,
/// until the returned closure is called, which also joins the worker. The caller keeps the slot
/// alive until then.
pub(super) fn hold_the_dispatch_turn<S: 'static>(slot: *const *mut c_void) -> impl FnOnce() {
    let (hold_sender, hold_receiver) = channel::<()>();
    let (started_sender, started_receiver) = channel::<()>();
    let address = slot.expose_provenance();
    // SAFETY: The address is a slot installed with an `S`, which the caller keeps alive until the
    // returned closure joins this worker.
    let worker = std::thread::spawn(move || unsafe {
        let slot = std::ptr::with_exposed_provenance(address);
        dispatch::<S, _>(slot, "object", null(), |_| {
            started_sender.send(()).expect("test channel");
            hold_receiver.recv().expect("test channel");
            Ok(())
        })
    });
    started_receiver.recv().unwrap();
    move || {
        hold_sender.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), ADBC_STATUS_OK);
    }
}

/// A cancel handle that records whether the export layer reached it.
pub(super) struct RecordingCancel(pub(super) Arc<AtomicBool>);

impl CancelHandle for RecordingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// A typed null for whichever pointer parameter a call leaves unset.
///
/// `*mut T` coerces to `*const T` at a call site, so this spells both.
pub(super) fn null<T>() -> *mut T {
    std::ptr::null_mut()
}

/// A struct array of one row and one column, with one-field structs nested until the `Int64` leaf
/// is `depth` levels below the array itself, which stands for the batch.
pub(super) fn nested_struct_array(depth: usize) -> StructArray {
    let wrap = |column: ArrayRef| {
        let field = Field::new("c", column.data_type().clone(), true);
        StructArray::from(vec![(Arc::new(field), column)])
    };
    let mut array = wrap(Arc::new(Int64Array::from(vec![1_i64])));
    for _ in 1..depth {
        array = wrap(Arc::new(array));
    }
    array
}

/// An Arrow C schema's fields, which `FFI_ArrowSchema` keeps private, for [`nested_schema`].
#[repr(C)]
struct SchemaParts {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut Self,
    dictionary: *mut Self,
    release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>,
    private_data: *mut c_void,
}

const _: () = {
    assert!(size_of::<SchemaParts>() == size_of::<FFI_ArrowSchema>());
    assert!(align_of::<SchemaParts>() == align_of::<FFI_ArrowSchema>());
};

/// Every node below a [`nested_schema`] root. Flat, because Arrow's own release callback recurses
/// and would overflow the stack on the very schemas this builds.
struct NestedSchema {
    /// Reached only through `links`; never read directly.
    #[allow(dead_code, reason = "held only to keep the pointed-at nodes alive")]
    nodes: Vec<SchemaParts>,
    #[allow(dead_code, reason = "held only to keep the pointed-at links alive")]
    links: Vec<*mut SchemaParts>,
    released: &'static AtomicUsize,
}

/// # Safety
/// `schema` must be a live root from [`nested_schema`], whose nodes nothing still borrows.
unsafe extern "C" fn release_nested_schema(schema: *mut FFI_ArrowSchema) {
    let root = schema.cast::<SchemaParts>();
    // SAFETY: The caller grants exclusive access to the root, whose private_data is the box
    // nested_schema leaked; clearing release ensures this callback consumes it once.
    let owner = unsafe {
        (*root).release = None;
        Box::from_raw((*root).private_data.cast::<NestedSchema>())
    };
    owner.released.fetch_add(1, Ordering::SeqCst);
}

/// Children are freed with their root, but a non-null callback is what marks them live.
extern "C" fn release_nested_child(_: *mut FFI_ArrowSchema) {}

/// A well-formed schema of one-child structs ending in an `Int64` leaf `depth` levels below the
/// root, which counts `released` when its release callback runs. Unlike Arrow's exports, it can be
/// built and released at any depth without recursing.
pub(super) fn nested_schema(depth: usize, released: &'static AtomicUsize) -> FFI_ArrowSchema {
    let node = |format: &'static CStr, n_children, children| SchemaParts {
        format: format.as_ptr(),
        name: c"c".as_ptr(),
        metadata: std::ptr::null(),
        flags: 0,
        n_children,
        children,
        dictionary: std::ptr::null_mut(),
        release: Some(release_nested_child),
        private_data: std::ptr::null_mut(),
    };
    // links[index] points at nodes[index], and nodes[index] holds a pointer to the next link. Both
    // are sized up front: the addresses taken here must survive.
    let mut links: Vec<*mut SchemaParts> = vec![std::ptr::null_mut(); depth];
    let first_link = links.as_mut_ptr();
    let mut nodes: Vec<SchemaParts> = (1..depth)
        // SAFETY: index stays below links' length, so the pointer is inside its allocation.
        .map(|index| node(c"+s", 1, unsafe { first_link.add(index) }))
        .chain([node(c"l", 0, std::ptr::null_mut())])
        .collect();
    let first_node = nodes.as_mut_ptr();
    for index in 0..depth {
        // SAFETY: nodes and links have the same length, so index is inside both allocations.
        unsafe { first_link.add(index).write(first_node.add(index)) };
    }
    let mut root = node(c"+s", 1, first_link);
    root.release = Some(release_nested_schema);
    root.private_data = Box::into_raw(Box::new(NestedSchema {
        nodes,
        links,
        released,
    }))
    .cast::<c_void>();
    // SAFETY: SchemaParts has ArrowSchema's layout, and root is a live schema whose release
    // callback owns everything it reaches. from_raw moves it out, leaving root released.
    unsafe { FFI_ArrowSchema::from_raw((&raw mut root).cast::<FFI_ArrowSchema>()) }
}

/// An Arrow C stream's callback table and private data, for [`edit_stream`].
#[repr(C)]
pub(super) struct StreamParts {
    pub(super) get_schema: Option<
        unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut FFI_ArrowSchema) -> std::ffi::c_int,
    >,
    pub(super) get_next: Option<
        unsafe extern "C" fn(
            *mut FFI_ArrowArrayStream,
            *mut arrow_array::ffi::FFI_ArrowArray,
        ) -> std::ffi::c_int,
    >,
    pub(super) get_last_error:
        Option<unsafe extern "C" fn(*mut FFI_ArrowArrayStream) -> *const c_char>,
    pub(super) release: Option<unsafe extern "C" fn(*mut FFI_ArrowArrayStream)>,
    pub(super) private_data: *mut c_void,
}

const _: () = {
    assert!(size_of::<StreamParts>() == size_of::<FFI_ArrowArrayStream>());
    assert!(align_of::<StreamParts>() == align_of::<FFI_ArrowArrayStream>());
};

/// Edits a producer's C callback table to exercise validation of malformed streams.
///
/// # Safety
/// Retained callbacks must match the private data and be memory-safe for every invocation the
/// test makes. Callback outputs must retain readable backing allocations until released, even
/// when their metadata is deliberately invalid. If `edit` removes the release callback or
/// changes private data, the caller must retain and eventually release the original ownership.
pub(super) unsafe fn edit_stream(
    stream: &mut FFI_ArrowArrayStream,
    edit: impl FnOnce(&mut StreamParts),
) {
    // SAFETY: Both repr(C) structs have the Arrow C stream field order and identical field types;
    // size/alignment are checked above. The mutable borrow excludes concurrent callback access. The
    // caller preserves ownership and callback memory safety despite deliberate protocol errors.
    edit(unsafe { &mut *std::ptr::from_mut(stream).cast::<StreamParts>() });
}
