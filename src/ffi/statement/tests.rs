use std::ffi::CStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::ffi::to_ffi;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchIterator, StructArray};
use arrow_schema::Schema;

use super::*;
use crate::ffi::abi::{
    ADBC_STATUS_INTERNAL, ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_DATA,
    ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_IMPLEMENTED,
};
use crate::ffi::options::{
    get_option, get_option_bytes, get_option_int, release_handle, set_option, set_option_bytes,
    set_option_double, set_option_int,
};
use crate::ffi::test_support::{
    RecordingCancel, assert_refused, call, edit_stream, get, hold_the_dispatch_turn, nested_schema,
    nested_struct_array, null, release, set,
};
use crate::test_support::driver::{publish_progress, test_statement};
use crate::types::{MAX_SCHEMA_NESTING_DEPTH, validate_schema_nesting};

const TARGET_TABLE: &str = "adbc.ingest.target_table";
const PROGRESS: &CStr = c"adbc.statement.exec.progress";
const MAX_PROGRESS: &CStr = c"adbc.statement.exec.max_progress";

fn live_handle() -> AdbcStatement {
    handle_for(test_statement())
}

/// A handle as `statement_new` builds it, with the statement's cancel handle and progress cell
/// shared with the export layer, so cancellation and the double getter work without the state
/// lock.
fn handle_for(state: State) -> AdbcStatement {
    let cancel = state.get_cancel_handle();
    let progress = state.progress_handle();
    installed(Exported::new(state, Some(cancel), Some(progress)))
}

/// `AdbcStatementCancel` on an idle statement, which must reach the statement's own cancel handle
/// and get its refusal, not the export layer's for a handle without one.
fn assert_nothing_to_cancel(handle: &mut AdbcStatement) {
    let (status, message) = call(|error| {
        // SAFETY: The handle is a live local; the error output is fresh.
        unsafe { statement_cancel(handle, error) }
    });
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    assert_eq!(
        message.as_deref(),
        Some("no active BigQuery statement operation to cancel")
    );
}

fn installed(exported: Exported<State>) -> AdbcStatement {
    let mut handle = AdbcStatement::default();
    // SAFETY: The handle is a live local whose slot is empty.
    unsafe { install(&raw mut handle.private_data, KIND, exported) }.unwrap();
    handle
}

/// Read a progress key through the exported double getter, which must succeed.
fn progress_through_the_abi(handle: &mut AdbcStatement, key: &CStr) -> f64 {
    let mut value = -1.0_f64;
    // SAFETY: The handle is exclusively borrowed, and the key and scalar output are live.
    let status =
        unsafe { statement_get_option_double(handle, key.as_ptr(), &raw mut value, null()) };
    assert_eq!(status, ADBC_STATUS_OK, "{key:?}");
    value
}

fn set_sql(handle: &mut AdbcStatement, query: &CStr) -> AdbcStatusCode {
    // SAFETY: The handle is exclusively borrowed and the query outlives the call.
    unsafe { statement_set_sql_query(handle, query.as_ptr(), null()) }
}

fn prepare(handle: &mut AdbcStatement) -> (AdbcStatusCode, Option<String>) {
    // SAFETY: The handle is exclusively borrowed; the error output is fresh.
    call(|error| unsafe { statement_prepare(handle, error) })
}

/// `AdbcStatementBind`, which takes ownership of both structs whatever it returns.
fn bind(
    handle: &mut AdbcStatement,
    values: &mut FFI_ArrowArray,
    schema: &mut FFI_ArrowSchema,
) -> (AdbcStatusCode, Option<String>) {
    call(|error| {
        // SAFETY: The handle is exclusively borrowed, and each struct is released or a live Arrow
        // export whose ownership the call takes. A caller that edited an export's metadata makes
        // sure the driver refuses it before reading memory the edit misdescribes.
        unsafe { statement_bind(handle, values, schema, error) }
    })
}

/// `AdbcStatementBindStream`, which takes ownership of the stream whatever it returns.
fn bind_stream(
    handle: &mut AdbcStatement,
    stream: &mut FFI_ArrowArrayStream,
) -> (AdbcStatusCode, Option<String>) {
    call(|error| {
        // SAFETY: The handle is exclusively borrowed and the stream is live, with callbacks that
        // match its private data or that the driver rejects before calling them.
        unsafe { statement_bind_stream(handle, stream, error) }
    })
}

/// A well-formed bind on `handle`, which a statement that was not poisoned accepts.
fn bind_fresh(handle: &mut AdbcStatement) -> AdbcStatusCode {
    let (mut values, mut schema) = fresh_bind_arguments();
    bind(handle, &mut values, &mut schema).0
}

/// Check that a refused bind left `handle` usable, then release it: a poisoned statement refuses
/// a well-formed bind.
#[track_caller]
fn finish_usable(mut handle: AdbcStatement) {
    assert_eq!(bind_fresh(&mut handle), ADBC_STATUS_OK);
    release(&mut handle);
}

/// One row of one column, rebuilt per use because a bind consumes what it is handed.
fn fresh_bind_batch() -> RecordBatch {
    RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef)])
        .expect("one Int64 column is a valid record batch")
}

/// [`fresh_bind_batch`] exported over the C data interface: the smallest well-formed pair of
/// `Bind` arguments.
fn fresh_bind_arguments() -> (FFI_ArrowArray, FFI_ArrowSchema) {
    to_ffi(&StructArray::from(fresh_bind_batch()).to_data())
        .expect("a one-column struct array exports over the C data interface")
}

/// A fresh one-batch stream, rebuilt per use because a bind consumes it.
fn bindable_stream() -> FFI_ArrowArrayStream {
    let batch = fresh_bind_batch();
    let schema = batch.schema();
    FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )))
}

fn consumed(values: &FFI_ArrowArray, schema: &FFI_ArrowSchema) -> bool {
    values.release().is_none() && schema.release().is_none()
}

#[test]
fn rejects_a_null_statement_handle() {
    // SAFETY: Every entry point explicitly checks its null handle; the error output is optional.
    let statuses = unsafe {
        [
            statement_new(null(), null(), null()),
            release_handle::<AdbcStatement>(null(), null()),
            statement_prepare(null(), null()),
            statement_cancel(null(), null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_ARGUMENT; 4]);
}

/// The same refusal as `AdbcDatabaseNew`, for a costlier leak (live clients and a cancellation
/// scope). The connection is never consulted, so the error names the populated handle.
#[test]
fn a_second_new_on_a_populated_handle_is_refused() {
    let mut handle = live_handle();
    let first = handle.private_data;
    let refusal = call(|error| {
        // SAFETY: The populated handle is writable; the null connection is checked if
        // initialization reaches it.
        unsafe { statement_new(null(), &raw mut handle, error) }
    });
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_STATE,
        "already holds a driver object",
    );
    assert_eq!(handle.private_data, first);
    release(&mut handle);
}

#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = AdbcStatement::default();
    let s = &raw mut handle;
    let name = c"adbc.ingest.target_table".as_ptr();
    let progress = PROGRESS.as_ptr();
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);

    // SAFETY: `s` points to a live handle struct whose null private data every entry point checks
    // before dispatch. The other arguments are live locals, static strings or literals covering
    // their stated lengths, or nulls the entry points accept or reject.
    let statuses = unsafe {
        [
            statement_prepare(s, null()),
            statement_cancel(s, null()),
            statement_set_sql_query(s, name, null()),
            set_option(s, name, name, null()),
            set_option_int(s, name, 1, null()),
            set_option_double(s, name, 1.0, null()),
            set_option_bytes(s, name, b"x".as_ptr(), 1, null()),
            statement_set_substrait_plan(s, b"x".as_ptr(), 1, null()),
            get_option(s, name, null(), &raw mut length, null()),
            get_option_bytes(s, name, null(), &raw mut length, null()),
            get_option_int(s, name, &raw mut integer, null()),
            statement_get_option_double(s, name, &raw mut double, null()),
            // The progress keys answer without the state lock, so their refusal is its own path.
            statement_get_option_double(s, progress, &raw mut double, null()),
            statement_execute_query(s, null(), null(), null()),
            statement_execute_schema(s, null(), null()),
            statement_execute_partitions(s, null(), null(), null(), null()),
            statement_get_parameter_schema(s, null(), null()),
            statement_bind(s, null(), null(), null()),
            statement_bind_stream(s, null(), null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_STATE; 19]);
}

#[test]
fn round_trips_a_string_option_through_the_c_abi() {
    let mut handle = live_handle();
    assert_eq!(set(&mut handle, TARGET_TABLE, "events").0, ADBC_STATUS_OK);
    assert_eq!(get(&mut handle, TARGET_TABLE).unwrap(), "events");
    release(&mut handle);
}

#[test]
fn rejects_null_arguments_and_substrait_plans() {
    let mut handle = live_handle();
    let s = &raw mut handle;
    let name = c"adbc.ingest.target_table".as_ptr();
    // SAFETY: The handle and static strings are live; each null argument is explicitly rejected,
    // and the plan literal holds the four bytes passed.
    let statuses = unsafe {
        [
            set_option(s, null(), name, null()),
            set_option(s, name, null(), null()),
            // A non-empty plan may not be described by a null pointer.
            statement_set_substrait_plan(s, null(), 4, null()),
            statement_set_sql_query(s, null(), null()),
            // A well-formed plan is refused too: Substrait is not implemented.
            statement_set_substrait_plan(s, b"plan".as_ptr(), 4, null()),
        ]
    };
    assert_eq!(
        statuses,
        [
            ADBC_STATUS_INVALID_ARGUMENT,
            ADBC_STATUS_INVALID_ARGUMENT,
            ADBC_STATUS_INVALID_ARGUMENT,
            ADBC_STATUS_INVALID_ARGUMENT,
            ADBC_STATUS_NOT_IMPLEMENTED
        ]
    );
    release(&mut handle);
}

#[test]
fn prepare_requires_a_query_and_accepts_one() {
    let mut handle = live_handle();
    let (status, message) = prepare(&mut handle);
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    // Prepare runs no query, so the refusal names its own verb, not "execute".
    let message = message.unwrap();
    assert!(
        message.starts_with("cannot prepare a BigQuery statement without a SQL query"),
        "{message}"
    );

    // Through `get_cancel_handle`, an idle statement reports nothing to cancel rather than the
    // trait default's "not implemented".
    assert_nothing_to_cancel(&mut handle);

    assert_eq!(set_sql(&mut handle, c"SELECT 1;"), ADBC_STATUS_OK);
    assert_eq!(prepare(&mut handle).0, ADBC_STATUS_OK);
    release(&mut handle);
}

/// While a worker holds the dispatch turn, `statement_cancel` still reaches the driver's cancel
/// handle promptly. With no operation in flight it reports `InvalidState`.
#[test]
fn cancel_does_not_wait_for_the_statement_dispatch_turn() {
    let mut handle = live_handle();
    let release_turn = hold_the_dispatch_turn::<State>(&raw const handle.private_data);
    assert_nothing_to_cancel(&mut handle);
    release_turn();
    release(&mut handle);
}

/// adbc.h keeps option getters thread-safe during other calls, and a monitoring thread polls
/// `adbc.statement.exec.progress` while an execute runs. With a worker holding the dispatch turn,
/// both progress keys still return what the statement published.
#[test]
fn the_progress_getters_do_not_wait_for_the_statement_dispatch_turn() {
    let state = test_statement();
    publish_progress(&state, 42.0, 84.0);
    let mut handle = handle_for(state);
    let release_turn = hold_the_dispatch_turn::<State>(&raw const handle.private_data);
    assert_eq!(progress_through_the_abi(&mut handle, PROGRESS), 42.0);
    assert_eq!(progress_through_the_abi(&mut handle, MAX_PROGRESS), 84.0);
    release_turn();
    release(&mut handle);
}

/// The getter reads the progress object the statement writes, so an execute that clears it is
/// visible through the ABI. `execute_schema` clears it before finding there is no query.
#[test]
fn the_getter_reads_the_cell_the_statement_publishes_into() {
    let state = test_statement();
    publish_progress(&state, 42.0, 84.0);
    let mut handle = handle_for(state);
    assert_eq!(progress_through_the_abi(&mut handle, PROGRESS), 42.0);

    let mut schema = FFI_ArrowSchema::empty();
    // SAFETY: The handle remains live; the empty Arrow schema is an exclusively writable
    // destination.
    let status = unsafe { statement_execute_schema(&raw mut handle, &raw mut schema, null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    assert_eq!(progress_through_the_abi(&mut handle, PROGRESS), 0.0);
    release(&mut handle);
}

#[test]
fn binds_a_struct_array_and_rejects_anything_else() {
    let mut handle = live_handle();
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
    )])
    .unwrap();
    let (mut values, mut schema) = to_ffi(&StructArray::from(batch).to_data()).unwrap();
    assert_eq!(
        bind(&mut handle, &mut values, &mut schema).0,
        ADBC_STATUS_OK
    );
    // adbc.h gives `Bind` ownership of both structs, and callers free neither, so a schema left
    // populated here would leak once per bind.
    assert!(consumed(&values, &schema), "both structs must be consumed");

    // A bare array is not a set of parameter rows; it is refused, not wrapped in a struct.
    let (mut values, mut schema) = to_ffi(&Int64Array::from(vec![7_i64]).to_data()).unwrap();
    let (status, message) = bind(&mut handle, &mut values, &mut schema);
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(
        message.as_deref(),
        Some("bind values must be a struct array, not Int64")
    );
    release(&mut handle);
}

/// adbc.h:2100-2102 has the driver release `values`, with no exception for refused calls, so a
/// null schema must not leak the array, nor a null array the schema.
#[test]
fn a_refused_bind_still_releases_the_array_it_was_handed() {
    let mut handle = live_handle();
    let (mut values, mut schema) = fresh_bind_arguments();
    let (array_only, schema_only) = (
        call(|error| {
            // SAFETY: The handle and the live export are valid; bind consumes the array even
            // when rejecting the null schema.
            unsafe { statement_bind(&raw mut handle, &raw mut values, null(), error) }
        }),
        call(|error| {
            // SAFETY: As above, for the schema and a null array.
            unsafe { statement_bind(&raw mut handle, null(), &raw mut schema, error) }
        }),
    );
    for ((status, message), missing) in [(array_only, "schema"), (schema_only, "values")] {
        assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
        assert_eq!(message, Some(format!("{missing} must not be null")));
    }
    assert!(
        consumed(&values, &schema),
        "a refused bind must still consume what it was handed"
    );
    release(&mut handle);
}

/// The same holds when the handle, not an argument, is refused.
#[test]
fn binding_to_a_released_handle_still_releases_what_it_was_handed() {
    let mut handle = AdbcStatement::default();
    let (mut values, mut schema) = fresh_bind_arguments();
    let (status, _) = bind(&mut handle, &mut values, &mut schema);
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    assert!(consumed(&values, &schema));

    let mut stream = bindable_stream();
    let (status, _) = bind_stream(&mut handle, &mut stream);
    assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    assert!(stream.release().is_none());
}

/// Binding an already-released struct is a caller mistake: it is reported as one and leaves the
/// statement usable rather than poisoned by a panic inside the Arrow importer. Refused calls still
/// consume whatever was handed over (adbc.h:2100-2102).
#[test]
fn a_released_bind_argument_is_refused_without_poisoning_the_statement() {
    let mut handle = live_handle();
    let (mut values, mut schema) = fresh_bind_arguments();

    // Release the schema the way a producer does, leaving the caller's struct zeroed.
    let (status, message) = bind(&mut handle, &mut values, &mut FFI_ArrowSchema::empty());
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(message.as_deref(), Some("schema has already been released"));
    // Conversely, with a released array, the valid schema is consumed.
    let (status, message) = bind(&mut handle, &mut FFI_ArrowArray::empty(), &mut schema);
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    assert_eq!(message.as_deref(), Some("values has already been released"));
    assert!(
        consumed(&values, &schema),
        "a refused bind must still consume what it was handed"
    );

    assert_eq!(set_sql(&mut handle, c"SELECT 1;"), ADBC_STATUS_OK);
    assert_eq!(prepare(&mut handle).0, ADBC_STATUS_OK);
    finish_usable(handle);
}

#[test]
fn null_parameter_rows_do_not_poison_the_statement() {
    let mut handle = live_handle();
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![Some(1_i64), None])) as ArrayRef,
    )])
    .unwrap();
    let (fields, columns, _) = StructArray::from(batch).into_parts();
    let rows = Some(arrow_buffer::NullBuffer::from(vec![true, false]));
    let array = StructArray::new(fields.clone(), columns.clone(), rows);
    let (mut values, mut schema) = to_ffi(&array.to_data()).unwrap();
    let refusal = bind(&mut handle, &mut values, &mut schema);
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_ARGUMENT,
        "top-level null rows; put null values in the individual columns",
    );

    // Null columns are valid; only a null whole parameter row is unrepresentable.
    let array = StructArray::new(fields, columns, None);
    let (mut values, mut schema) = to_ffi(&array.to_data()).unwrap();
    assert_eq!(
        bind(&mut handle, &mut values, &mut schema).0,
        ADBC_STATUS_OK
    );
    release(&mut handle);
}

#[test]
fn binds_a_stream_and_leaves_the_callers_struct_released() {
    let mut handle = live_handle();
    let mut stream = bindable_stream();
    assert_eq!(bind_stream(&mut handle, &mut stream).0, ADBC_STATUS_OK);
    // Ownership moved to the driver, so the caller's struct must be safe to drop.
    assert!(stream.release().is_none());

    // SAFETY: The statement remains live; a null stream input is explicitly rejected.
    let status = unsafe { statement_bind_stream(&raw mut handle, null(), null()) };
    assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    release(&mut handle);
}

#[test]
fn a_bound_stream_error_without_a_description_does_not_poison_the_statement() {
    extern "C" fn no_description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        null()
    }

    let mut handle = live_handle();
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::once(Err(arrow_schema::ArrowError::IoError(
            "producer failure".to_owned(),
            std::io::Error::from_raw_os_error(libc::EIO),
        ))),
        Arc::new(Schema::empty()),
    )));
    // SAFETY: The replacement ignores private data and returns null; all ownership callbacks remain
    // those of the original producer.
    unsafe {
        edit_stream(&mut stream, |parts| {
            parts.get_last_error = Some(no_description);
        });
    }
    assert_eq!(bind_stream(&mut handle, &mut stream).0, ADBC_STATUS_OK);
    assert_eq!(set_sql(&mut handle, c"SELECT 1"), ADBC_STATUS_OK);
    let refusal = call(|error| {
        // SAFETY: The live handle and error are valid; a null result stream requests execution
        // without returned rows, and the row count output is optional.
        unsafe { statement_execute_query(&raw mut handle, null(), null(), error) }
    });
    let expected = format!("get_next failed with error code {}", libc::EIO);
    assert_refused(refusal, ADBC_STATUS_INVALID_DATA, &expected);
    assert_eq!(
        prepare(&mut handle).0,
        ADBC_STATUS_OK,
        "a producer failure must leave the statement usable"
    );

    let mut empty = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::empty()),
    )));
    assert_eq!(bind_stream(&mut handle, &mut empty).0, ADBC_STATUS_OK);
    let mut affected = -1;
    // SAFETY: The live handle and writable row count remain valid; a null stream requests
    // execution without a returned reader.
    let status =
        unsafe { statement_execute_query(&raw mut handle, null(), &raw mut affected, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(affected, 0);
    release(&mut handle);
}

/// A non-UTF-8 value long enough to live in a data buffer rather than inline in its view.
fn out_of_line_non_utf8_struct() -> arrow_data::ArrayData {
    use arrow_array::BinaryViewArray;
    use arrow_schema::{DataType, Field};

    let values = BinaryViewArray::from_iter_values([[0xff_u8; 16].as_slice()]);
    StructArray::new(
        vec![Field::new("payload", DataType::BinaryView, false)].into(),
        vec![Arc::new(values) as ArrayRef],
        None,
    )
    .to_data()
}

/// Arrow's C data importer ends in `ArrayData::new_unchecked`, and consumers trust the result:
/// `GenericByteViewArray::value` uses `buffers.get_unchecked` and string arrays use
/// `from_bytes_unchecked`. A malformed array is undefined behavior downstream, so the driver
/// validates it.
///
/// An out-of-bounds view cannot be built through safe Arrow APIs, so this sends a `BinaryView`
/// array under a `Utf8View` schema instead: the layouts are byte-identical and every buffer is
/// valid, and only the UTF-8 invariant is false.
#[test]
fn imported_arrays_are_validated_and_name_the_column_that_fails() {
    use arrow_schema::{DataType, Field};

    /// # Safety
    /// `out` must point to exclusively writable, aligned storage for an Arrow array.
    unsafe extern "C" fn non_utf8_view(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let (array, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
        // SAFETY: The reader provides writable, empty output storage; the binary array transfers
        // valid buffers, while the reader separately validates their declared UTF-8 type.
        unsafe { std::ptr::write(out, array) };
        0
    }

    let declared = vec![Field::new("payload", DataType::Utf8View, false)];

    // The bound-stream path checks every batch, not just the first.
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::new(declared.clone())),
    )));
    // SAFETY: The replacement returns owned buffers with valid view offsets and lengths; only their
    // declared UTF-8 content is invalid and is checked before string access. The original private
    // data and release callback remain paired.
    unsafe { edit_stream(&mut stream, |parts| parts.get_next = Some(non_utf8_view)) };
    // SAFETY: The stream retains producer ownership and correctly sized view buffers; importing
    // checks the mismatched UTF-8 declaration before exposing any string values.
    let mut reader =
        unsafe { BoundStreamReader::new(take_stream(&raw mut stream).unwrap()) }.unwrap();
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        failure.contains(r#"column "payload" is invalid"#),
        "{failure}"
    );
    assert!(failure.contains("non-UTF-8"), "{failure}");
    assert!(reader.next().is_none());

    // The single-batch bind path, which imports through `from_ffi` instead. The array has valid
    // view buffers matching the declared physical layout, and the driver validates UTF-8 before
    // exposing strings.
    let mut handle = live_handle();
    let (mut values, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(&DataType::Struct(declared.into())).unwrap();
    let refusal = bind(&mut handle, &mut values, &mut schema);
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_ARGUMENT,
        r#"column "payload" is invalid"#,
    );
    // A refused bind is a caller mistake and does not poison the statement.
    finish_usable(handle);
}

/// A malformed callback table is refused, named, and consumed: adbc.h:2114-2116 has the driver
/// release the stream with no exception for failures. A stream with no release callback is left
/// alone, since there is nothing to call.
#[test]
fn malformed_bind_streams_are_consumed_and_leave_the_statement_usable() {
    let mut handle = live_handle();
    for missing in ["get_schema", "get_next", "get_last_error", "release"] {
        let mut stream = bindable_stream();
        // Without a release callback the driver cannot consume the stream, so the test saves it
        // to release the producer's allocation afterwards.
        let mut owned = None;
        // SAFETY: Retained callbacks still match the original producer data. The driver rejects
        // omitted callbacks before invoking them; when release is removed, `owned` retains its
        // cleanup responsibility and invokes it below.
        unsafe {
            edit_stream(&mut stream, |parts| match missing {
                "get_schema" => parts.get_schema = None,
                "get_next" => parts.get_next = None,
                "get_last_error" => parts.get_last_error = None,
                _ => owned = parts.release.take(),
            });
        }
        let refusal = bind_stream(&mut handle, &mut stream);
        assert_refused(refusal, ADBC_STATUS_INVALID_ARGUMENT, missing);
        match owned {
            // Nothing was taken, so the caller still owns the stream.
            // SAFETY: This is the producer callback saved before removal; bind left its private
            // data owned by this stream, and the test invokes the callback exactly once.
            Some(release) => unsafe { release(&raw mut stream) },
            // Taken and released: the caller is left an empty struct.
            None => assert!(
                stream.release().is_none() && stream.private_data().is_null(),
                "a refused bind must still consume the stream it was handed"
            ),
        }
    }

    finish_usable(handle);
}

#[test]
fn null_schema_outputs_leave_statement_progress_unchanged() {
    let state = test_statement();
    publish_progress(&state, 3.0, 7.0);
    let mut handle = handle_for(state);
    for callback in [statement_execute_schema, statement_get_parameter_schema] {
        let (status, message) = call(|error| {
            // SAFETY: The handle and error are live; both callbacks explicitly reject a null
            // schema output.
            unsafe { callback(&raw mut handle, null(), error) }
        });
        assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
        assert_eq!(message.as_deref(), Some("schema must not be null"));
        assert_eq!(progress_through_the_abi(&mut handle, PROGRESS), 3.0);
        assert_eq!(progress_through_the_abi(&mut handle, MAX_PROGRESS), 7.0);
    }
    assert_eq!(set_sql(&mut handle, c"SELECT 1"), ADBC_STATUS_OK);
    assert_eq!(prepare(&mut handle).0, ADBC_STATUS_OK);
    release(&mut handle);
}

/// Release exported partitions through their own callback.
fn release_partitions(partitions: &mut AdbcPartitions) {
    // SAFETY: The partitions were exported by this module, which owns the callback and payload.
    unsafe { partitions.release.unwrap()(partitions) };
}

#[test]
fn exported_partitions_expose_their_payloads_and_release_idempotently() {
    let payloads = vec![b"first".to_vec(), Vec::new(), b"third-partition".to_vec()];
    let mut exported = export_partitions(payloads.clone());

    assert_eq!(exported.num_partitions, payloads.len());
    assert!(!exported.private_data.is_null());
    for (index, expected) in payloads.iter().enumerate() {
        // SAFETY: The export owns pointer and length arrays of num_partitions elements, and index
        // is in that range.
        let (pointer, length) = unsafe {
            (
                *exported.partitions.add(index),
                *exported.partition_lengths.add(index),
            )
        };
        assert_eq!(length, expected.len(), "partition {index}");
        // An empty payload's pointer may dangle; its zero length is enough.
        if !expected.is_empty() {
            // SAFETY: The nonempty payload has exactly length initialized bytes owned by the
            // unreleased export.
            let payload = unsafe { std::slice::from_raw_parts(pointer, length) };
            assert_eq!(payload, expected.as_slice(), "partition {index}");
        }
    }

    let release = exported.release.unwrap();
    release_partitions(&mut exported);
    assert!(exported.private_data.is_null());
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    assert_eq!(exported.num_partitions, 0);
    assert!(exported.release.is_none());

    // A second release, and a release of null, must be no-ops, not double frees.
    // SAFETY: The first release cleared ownership fields, and this callback explicitly handles a
    // released export and a null pointer.
    unsafe {
        release(&raw mut exported);
        release(null());
    }
    assert!(exported.private_data.is_null());
    assert_eq!(exported.num_partitions, 0);
}

#[test]
fn exporting_no_partitions_still_releases_cleanly() {
    let mut exported = export_partitions(Vec::new());
    assert_eq!(exported.num_partitions, 0);
    assert!(!exported.private_data.is_null());
    // An empty `Vec`'s pointer dangles; export null instead.
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    release_partitions(&mut exported);
    assert!(exported.private_data.is_null());
}

/// Why `Cargo.toml` keeps `panic = "unwind"`: a driver panic must reach the caller as
/// `ADBC_STATUS_INTERNAL` with a message, not abort the host. This checks the exported entry
/// point and what the panic leaves behind: the object refuses further work, but `Cancel` works.
#[test]
fn a_panic_in_driver_code_reaches_the_caller_as_an_internal_error() {
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handle = installed(Exported::new(
        test_statement(),
        Some(Box::new(RecordingCancel(cancelled.clone()))),
        None,
    ));

    assert_refused(
        set(&mut handle, PANIC_OPTION, "boom"),
        ADBC_STATUS_INTERNAL,
        "uncaught panic in the BigQuery driver: the option setter exploded",
    );

    // The poisoned state may hold torn invariants, so later dispatches are refused.
    assert_refused(
        set(&mut handle, TARGET_TABLE, "events"),
        ADBC_STATUS_INTERNAL,
        "unusable after an earlier panic",
    );

    // Cancel ignores poisoning, so it can stop work the panicked call left running.
    // SAFETY: The statement owns a live driver allocation; the error output is optional.
    let status = unsafe { statement_cancel(&raw mut handle, null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));

    // A poisoned object still releases its state.
    release(&mut handle);
    assert!(handle.private_data.is_null());
}

/// Arrow's importer `assert!`s on and indexes by an array's shape without checking it against
/// the type, so the driver refuses a mismatched array by name, without poisoning the statement.
#[test]
fn a_bind_whose_array_disagrees_with_its_schema_is_refused_without_poisoning() {
    use arrow_schema::{DataType, Field};

    let mut handle = live_handle();
    let int64 = Field::new("value", DataType::Int64, false);
    let list = Field::new_list("value", Field::new_list_field(DataType::Int64, true), false);
    for (declared, expected) in [
        (
            vec![int64, Field::new("extra", DataType::Int64, false)],
            "the array has 1 columns, but its schema declares 2",
        ),
        (
            vec![list],
            r#"column "value" is invalid: the List(Int64) array has 0 children, but its type has 1"#,
        ),
        (
            vec![Field::new("value", DataType::Utf8, false)],
            r#"column "value" is invalid: the Utf8 array has 2 buffers, but the Arrow C data interface specifies 3"#,
        ),
    ] {
        // The exported array keeps valid buffers and children; only the separate schema disagrees,
        // which the driver checks before importing.
        let (mut values, _) = fresh_bind_arguments();
        let mut schema = FFI_ArrowSchema::try_from(&DataType::Struct(declared.into())).unwrap();
        let refusal = bind(&mut handle, &mut values, &mut schema);
        assert_refused(refusal, ADBC_STATUS_INVALID_ARGUMENT, expected);
        assert!(consumed(&values, &schema));
    }
    finish_usable(handle);
}

/// A length too large for any buffer is refused by name; Arrow's importer would overflow
/// computing the buffer size and poison the statement with a panic.
#[test]
fn a_bind_whose_length_overflows_its_buffer_size_is_refused_without_poisoning() {
    /// The Arrow C array fields up to `children`.
    #[repr(C)]
    struct ArrayHead {
        length: i64,
        null_count: i64,
        offset: i64,
        n_buffers: i64,
        n_children: i64,
        buffers: *mut *const c_void,
        children: *mut *mut Self,
    }
    const _: () = {
        assert!(size_of::<ArrayHead>() <= size_of::<FFI_ArrowArray>());
        assert!(align_of::<ArrayHead>() <= align_of::<FFI_ArrowArray>());
    };

    let mut handle = live_handle();
    let (mut values, mut schema) = fresh_bind_arguments();
    // SAFETY: The repr(C) head matches ArrowArray's leading fields, and values is exclusively
    // writable. Its single child is a live export Arrow allocated, writable through the children
    // array. Only the length changes; the driver refuses it before reading any buffer, and
    // Arrow's release callback only uses private_data.
    unsafe {
        let column = *(*(&raw mut values).cast::<ArrayHead>()).children;
        (*column).length = 1 << 60;
    }
    let refusal = bind(&mut handle, &mut values, &mut schema);
    assert_refused(
        refusal,
        ADBC_STATUS_INVALID_ARGUMENT,
        r#"column "value" is invalid: the Int64 array's length"#,
    );
    assert!(consumed(&values, &schema));
    finish_usable(handle);
}

/// A struct schema with one column of `format` and no children of its own.
fn childless_column_schema(format: &str) -> FFI_ArrowSchema {
    let column = FFI_ArrowSchema::try_new(format, vec![], None)
        .and_then(|column| column.with_name("value"))
        .expect("Arrow builds a schema node from any format");
    FFI_ArrowSchema::try_new("+s", vec![column], None).expect("a struct of one column")
}

/// Arrow's schema importer indexes the children a nested format requires without checking that
/// they exist, so a malformed schema is refused by name instead of poisoning the statement.
#[test]
fn a_bind_or_bind_stream_with_a_malformed_schema_is_refused_without_poisoning() {
    /// # Safety
    /// `out` must point to exclusively writable, aligned storage for an Arrow schema.
    unsafe extern "C" fn childless_list(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        // SAFETY: The driver passes writable, empty schema storage it takes ownership of.
        unsafe { std::ptr::write(out, childless_column_schema("+l")) };
        0
    }

    let mut handle = live_handle();
    for (format, required) in [("+l", 1), ("+w:2", 1), ("+r", 2)] {
        let (mut values, _) = fresh_bind_arguments();
        let mut schema = childless_column_schema(format);
        let expected =
            format!("the {format:?} schema has 0 children, but its format requires {required}");
        let refusal = bind(&mut handle, &mut values, &mut schema);
        assert_refused(refusal, ADBC_STATUS_INVALID_ARGUMENT, &expected);
        assert!(consumed(&values, &schema));
    }

    let mut stream = bindable_stream();
    // SAFETY: The replacement callback writes an owned schema and touches no private data; the
    // original release callback keeps the producer's ownership.
    unsafe { edit_stream(&mut stream, |parts| parts.get_schema = Some(childless_list)) };
    assert_refused(
        bind_stream(&mut handle, &mut stream),
        ADBC_STATUS_INVALID_ARGUMENT,
        "the \"+l\" schema has 0 children, but its format requires 1",
    );
    assert!(stream.release().is_none());
    finish_usable(handle);
}

/// Bind must draw the same line as `validate_schema_nesting`, which every later use of the bound
/// data repeats: the deepest schema it accepts binds, and one level more is refused here.
#[test]
fn a_bind_or_bind_stream_is_accepted_at_the_nesting_limit_and_refused_one_past_it() {
    let mut handle = live_handle();
    for (depth, status) in [
        (MAX_SCHEMA_NESTING_DEPTH, ADBC_STATUS_OK),
        (MAX_SCHEMA_NESTING_DEPTH + 1, ADBC_STATUS_INVALID_ARGUMENT),
    ] {
        let columns = nested_struct_array(depth);
        assert_eq!(
            validate_schema_nesting(columns.fields(), "bound").is_ok(),
            status == ADBC_STATUS_OK
        );
        let expected = (status != ADBC_STATUS_OK).then(|| {
            format!(
                "a schema is nested {depth} levels deep; the maximum is \
                 {MAX_SCHEMA_NESTING_DEPTH}"
            )
        });
        let assert_outcome = |actual: AdbcStatusCode, message: Option<String>| {
            assert_eq!(actual, status, "{depth}: {message:?}");
            if let Some(expected) = &expected {
                let message = message.unwrap();
                assert!(message.contains(expected), "{message}");
            }
        };

        let (mut values, mut schema) = to_ffi(&columns.to_data()).unwrap();
        let (actual, message) = bind(&mut handle, &mut values, &mut schema);
        assert_outcome(actual, message);
        assert!(consumed(&values, &schema));

        let batch = RecordBatch::from(columns);
        let schema = batch.schema();
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )));
        let (actual, message) = bind_stream(&mut handle, &mut stream);
        assert_outcome(actual, message);
        assert!(stream.release().is_none());
    }
    release(&mut handle);
}

/// Far past what any recursive walk survives on a test thread's stack. Without the depth check
/// this aborts the test process, as it would the host, instead of failing an assertion. Miri only
/// needs a chain past the limit: building and freeing the full depth dominates its run time.
const STACK_OVERFLOWING_DEPTH: usize = if cfg!(miri) { 1_000 } else { 100_000 };

#[test]
fn a_bind_nested_deep_enough_to_overflow_the_stack_is_refused_and_released() {
    static RELEASED: AtomicUsize = AtomicUsize::new(0);

    let mut handle = live_handle();
    let (mut values, _) = fresh_bind_arguments();
    let mut schema = nested_schema(STACK_OVERFLOWING_DEPTH, &RELEASED);
    assert_refused(
        bind(&mut handle, &mut values, &mut schema),
        ADBC_STATUS_INVALID_ARGUMENT,
        "bind schema is not a valid Arrow schema: a schema is nested",
    );
    assert!(consumed(&values, &schema));
    assert_eq!(RELEASED.load(Ordering::SeqCst), 1);
    finish_usable(handle);
}

#[test]
fn a_bind_stream_nested_deep_enough_to_overflow_the_stack_is_refused_and_released() {
    static RELEASED: AtomicUsize = AtomicUsize::new(0);

    /// # Safety
    /// `out` must point to exclusively writable, aligned storage for an Arrow schema.
    unsafe extern "C" fn deep_schema(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        // SAFETY: The driver passes writable, empty schema storage it takes ownership of.
        unsafe { std::ptr::write(out, nested_schema(STACK_OVERFLOWING_DEPTH, &RELEASED)) };
        0
    }

    let mut handle = live_handle();
    let mut stream = bindable_stream();
    // SAFETY: The replacement callback writes an owned schema and touches no private data; the
    // original release callback keeps the producer's ownership.
    unsafe { edit_stream(&mut stream, |parts| parts.get_schema = Some(deep_schema)) };
    assert_refused(
        bind_stream(&mut handle, &mut stream),
        ADBC_STATUS_INVALID_ARGUMENT,
        "bound stream returned an invalid schema: a schema is nested",
    );
    assert!(stream.release().is_none());
    assert_eq!(RELEASED.load(Ordering::SeqCst), 1);
    finish_usable(handle);
}

#[test]
fn bind_rejects_invalid_null_counts_without_losing_null_rows() {
    // The Arrow C array starts with these two int64_t fields. Edit raw metadata rather than
    // calling set_null_count, whose contract requires a count matching the validity bitmap.
    #[repr(C)]
    struct ArrayPrefix {
        length: i64,
        null_count: i64,
    }
    const _: () = {
        assert!(size_of::<ArrayPrefix>() <= size_of::<FFI_ArrowArray>());
        assert!(align_of::<ArrayPrefix>() <= align_of::<FFI_ArrowArray>());
    };

    let mut handle = live_handle();
    for (null_count, expected) in [
        (-2, "null count -2; expected -1 or a count between 0 and 1"),
        (2, "null count 2; expected -1 or a count between 0 and 1"),
        (1, "declares 1 nulls but has no validity bitmap"),
    ] {
        let (mut values, mut schema) = fresh_bind_arguments();
        // This one-row struct has no validity bitmap; Arrow's importer would discard the count
        // and accept the row as valid.
        // SAFETY: The repr(C) prefix matches ArrowArray's first two i64 fields, and values is
        // exclusively writable. Buffers and ownership stay valid; the driver rejects this count
        // before import, and Arrow's release callback only uses private_data.
        unsafe { (*(&raw mut values).cast::<ArrayPrefix>()).null_count = null_count };
        let refusal = bind(&mut handle, &mut values, &mut schema);
        assert_refused(refusal, ADBC_STATUS_INVALID_ARGUMENT, expected);
        assert!(consumed(&values, &schema));
    }
    finish_usable(handle);
}
