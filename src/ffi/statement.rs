//! `AdbcStatement*` entry points.
//!
//! Statements are initialized by `AdbcStatementNew`, so options apply directly to
//! [`BigQueryStatement`] without the staging used by databases and connections.

use std::ffi::{c_char, c_void};

use adbc_core::error::{Error, Result};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::RecordBatch;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::DataType;

use super::abi::{
    ADBC_STATUS_OK, AdbcConnection, AdbcError, AdbcPartitions, AdbcStatement, AdbcStatusCode,
};
use super::guard::{byte_slice, catch, missing_argument, required_output, required_str, write_out};
use super::handle::{Exported, check_empty, finish, install, published_progress};
use super::import::{BoundStreamReader, check_schema, import_struct, take_stream};
use super::options::{FfiHandle, get_option_double, slot_of, with_handle, with_state};
use super::stream::{export_reader, write_schema};
use crate::error::invalid_argument;
use crate::query::ExecutionProgress;
use crate::statement::BigQueryStatement;

pub(super) const KIND: &str = "statement";

pub(super) type State = BigQueryStatement;

// SAFETY: `AdbcStatement` is repr(C) with `private_data` first, and only `State` is installed there.
unsafe impl FfiHandle for AdbcStatement {
    type State = State;
    const KIND: &'static str = KIND;
}

/// Test-only option for exercising panic containment through the C ABI.
#[cfg(test)]
pub(super) const PANIC_OPTION: &str = "adbc.bigquery.test.panic";

impl super::options::OptionTarget for BigQueryStatement {
    type Key = OptionStatement;
    type Options = Self;

    fn set(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        #[cfg(test)]
        if matches!(&key, OptionStatement::Other(name) if name == PANIC_OPTION) {
            panic!("the option setter exploded");
        }
        self.set_option(key, value)
    }

    fn options(&mut self, _kind: &str, _key: &OptionStatement) -> Result<&mut Self> {
        Ok(self)
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `connection` must be null or point to a valid, initialized `AdbcConnection`, and `statement`
/// must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_new(
    connection: *mut AdbcConnection,
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // A panic here precedes the statement handle, so the connection guard is held inside `catch`
    // to poison the connection instead.
    let created = catch(|| {
        let slot = slot_of(statement);
        // SAFETY: The caller supplies a live statement struct or null, which is rejected.
        unsafe { check_empty(slot, KIND) }?;
        // SAFETY: The caller supplies a live connection struct or null.
        let state = unsafe {
            with_handle(connection, |connection| {
                connection.ready(super::connection::KIND)?.new_statement()
            })
        }?;
        // Cancellation and progress reads must work while execution holds the state lock.
        let cancel = state.get_cancel_handle();
        let progress = state.progress_handle();
        let exported = Exported::new(state, Some(cancel), Some(progress));
        // SAFETY: The slot is the live field of the caller's statement.
        unsafe { install(slot, KIND, exported) }
    });
    // SAFETY: the caller provides initialized, exclusively writable error storage or null.
    unsafe { finish(created.unwrap_or_else(Err), error) }
}

/// Read progress without taking the state lock held during execution. Other double-valued
/// options use the usual dispatch path.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn statement_get_option_double(
    statement: *mut AdbcStatement,
    key: *const c_char,
    value: *mut f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let answered = catch(|| {
        // SAFETY: The caller keeps the NUL-terminated key readable for this call, or null is
        // rejected.
        let read: fn(&ExecutionProgress) -> f64 =
            match OptionStatement::from(unsafe { required_str(key, "key") }?) {
                OptionStatement::Progress => ExecutionProgress::current,
                OptionStatement::MaxProgress => ExecutionProgress::maximum,
                _ => return Ok(false),
            };
        // SAFETY: The caller keeps the statement struct live for the call.
        let found = unsafe { published_progress::<State>(slot_of(statement), KIND, read) }?;
        // SAFETY: The caller grants exclusive scalar output storage; write_out rejects null.
        unsafe { write_out(value, found, "value") }?;
        Ok(true)
    })
    .unwrap_or_else(Err);
    match answered {
        Ok(true) => ADBC_STATUS_OK,
        // SAFETY: The caller supplies a live handle/key and disjoint writable scalar/error outputs.
        Ok(false) => unsafe { get_option_double(statement, key, value, error) },
        // SAFETY: the caller provides initialized, exclusively writable error storage or null.
        Err(failure) => unsafe { finish(Err(failure), error) },
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `query` null or a
/// NUL-terminated string.
pub(super) unsafe extern "C" fn statement_set_sql_query(
    statement: *mut AdbcStatement,
    query: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller provides a live statement, NUL-terminated query and writable error output.
    unsafe {
        with_state(statement, error, |state| {
            state.set_sql_query(required_str(query, "query")?)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `plan` null or point to
/// `length` initialized bytes.
pub(super) unsafe extern "C" fn statement_set_substrait_plan(
    statement: *mut AdbcStatement,
    plan: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller provides a live statement, readable plan allocation and writable error
    // output.
    unsafe {
        with_state(statement, error, |state| {
            state.set_substrait_plan(byte_slice(plan, length, "plan")?)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_prepare(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller provides a live statement slot and initialized, writable error output or
    // null.
    unsafe { with_state(statement, error, State::prepare) }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_cancel(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Bypass the state lock held by execution. An idle statement reports `InvalidState`.
    // SAFETY: The caller keeps the statement struct live and grants error output access.
    unsafe { super::handle::cancel::<State>(slot_of(statement), KIND, error) }
}

/// `AdbcStatementBind`.
///
/// Take ownership of both inputs before validation, including when binding fails. Null or
/// released inputs have no ownership to transfer and are rejected before Arrow's importer.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `values`/`schema` null or
/// point to a valid Arrow C data interface array and schema.
pub(super) unsafe extern "C" fn statement_bind(
    statement: *mut AdbcStatement,
    values: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Do not hold a shared reference across `from_raw`: it writes to the source struct.
    // SAFETY: Short-circuiting excludes null; the caller keeps the initialized array struct
    // readable.
    let taken_values = if values.is_null() || unsafe { (*values).release().is_none() } {
        None
    } else {
        // SAFETY: The caller transfers this live array exclusively; from_raw marks its former
        // struct released.
        Some(unsafe { FFI_ArrowArray::from_raw(values) })
    };
    // SAFETY: Short-circuiting excludes null; the caller keeps the initialized schema struct
    // readable.
    let taken_schema = if schema.is_null() || unsafe { (*schema).release().is_none() } {
        None
    } else {
        // SAFETY: The caller transfers this live schema exclusively; from_raw marks its former
        // struct released.
        Some(unsafe { FFI_ArrowSchema::from_raw(schema) })
    };
    // SAFETY: The caller grants statement/error access; the moved arrays retain producer-owned
    // buffers matching fields.
    unsafe {
        with_state(statement, error, move |state| {
            let values =
                taken_values.ok_or_else(|| unusable_bind_argument("values", values.is_null()))?;
            let schema =
                taken_schema.ok_or_else(|| unusable_bind_argument("schema", schema.is_null()))?;
            let data_type = check_schema(&schema)
                .and_then(|()| DataType::try_from(&schema).map_err(|failure| failure.to_string()))
                .map_err(|failure| {
                    invalid_argument(format!(
                        "bind schema is not a valid Arrow schema: {failure}"
                    ))
                })?;
            let DataType::Struct(fields) = data_type else {
                return Err(invalid_argument(format!(
                    "bind values must be a struct array, not {data_type}"
                )));
            };
            let columns =
                import_struct(values, &fields, "bind values").map_err(invalid_argument)?;
            state.bind(RecordBatch::from(columns))
        })
    }
}

/// Distinguish missing inputs from inputs whose ownership was already released.
#[cold]
#[inline(never)]
fn unusable_bind_argument(name: &str, missing: bool) -> Error {
    if missing {
        missing_argument(name)
    } else {
        invalid_argument(format!("{name} has already been released"))
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `stream` null or point to a
/// valid, non-released Arrow C stream.
pub(super) unsafe extern "C" fn statement_bind_stream(
    statement: *mut AdbcStatement,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Taken before the handle is consulted, for the reason `take_stream` gives.
    // SAFETY: The caller transfers a live stream exclusively, retaining the producer
    // callback/buffer contract.
    let taken = unsafe { take_stream(stream) };
    // SAFETY: The caller grants statement/error access; the taken stream owns its callbacks and
    // backing data.
    unsafe {
        with_state(statement, error, move |state| {
            state.bind_stream(Box::new(BoundStreamReader::new(taken?)?))
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`; `out` must be null or point to
/// writable storage for an `FFI_ArrowArrayStream`, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_query(
    statement: *mut AdbcStatement,
    out: *mut FFI_ArrowArrayStream,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live statement and disjoint writable stream, row count and
    // error outputs.
    unsafe {
        with_state(statement, error, |state| {
            // A null output requests execution without a result stream.
            if out.is_null() {
                let affected = state.execute_update()?.unwrap_or(-1);
                write_rows_affected(rows_affected, affected);
                return Ok(());
            }
            let (reader, affected) = state.execute_with_rows_affected()?;
            std::ptr::write_unaligned(out, export_reader(reader));
            write_rows_affected(rows_affected, affected);
            Ok(())
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_execute_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live statement and disjoint writable schema/error outputs.
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            write_schema(schema, &state.execute_schema()?)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_get_parameter_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live statement and disjoint writable schema/error outputs.
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            write_schema(schema, &state.get_parameter_schema()?)
        })
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `statement` must be null or point to a valid `AdbcStatement`; `schema` and `partitions` must be
/// null or point to writable storage for their types, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_partitions(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    partitions: *mut AdbcPartitions,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller supplies a live statement and disjoint writable schema, partitions, row
    // count and error outputs.
    unsafe {
        with_state(statement, error, |state| {
            // Checked before executing, so a missing output never bills a query.
            required_output(schema, "schema")?;
            required_output(partitions, "partitions")?;

            let result = state.execute_partitions()?;
            // Export the schema first: it is the last fallible step, and failing after writing
            // the partitions would leave the caller a struct to release from a failed call.
            write_schema(schema, &result.schema)?;
            std::ptr::write_unaligned(partitions, export_partitions(result.partitions));
            write_rows_affected(rows_affected, result.rows_affected);
            Ok(())
        })
    }
}

/// Write the optional row count.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `dst` must be null or point to a writable `i64`.
unsafe fn write_rows_affected(dst: *mut i64, value: i64) {
    if !dst.is_null() {
        // SAFETY: Null was excluded and the caller grants writable storage for one i64.
        unsafe { std::ptr::write_unaligned(dst, value) };
    }
}

/// Own the payloads and parallel arrays until [`AdbcPartitions::release`] runs.
/// Retaining the vectors preserves their capacities for deallocation.
struct PartitionsOwner {
    /// Backs `pointers`; never read.
    #[allow(
        dead_code,
        reason = "held only to keep the pointed-at payload bytes alive"
    )]
    payloads: Vec<Vec<u8>>,
    pointers: Vec<*const u8>,
    lengths: Vec<usize>,
}

fn export_partitions(payloads: Vec<Vec<u8>>) -> AdbcPartitions {
    let num_partitions = payloads.len();
    let pointers: Vec<*const u8> = payloads.iter().map(|payload| payload.as_ptr()).collect();
    let lengths: Vec<usize> = payloads.iter().map(Vec::len).collect();
    let mut owner = Box::new(PartitionsOwner {
        payloads,
        pointers,
        lengths,
    });
    // Export null for empty arrays instead of Vec's dangling pointer.
    let (partitions, partition_lengths) = if num_partitions == 0 {
        (std::ptr::null_mut(), std::ptr::null_mut())
    } else {
        (owner.pointers.as_mut_ptr(), owner.lengths.as_mut_ptr())
    };
    AdbcPartitions {
        num_partitions,
        partitions,
        partition_lengths,
        private_data: Box::into_raw(owner).cast::<c_void>(),
        release: Some(release_partitions),
    }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
/// `partitions` must be null or point to an `AdbcPartitions` produced by [`export_partitions`].
// Guard-exempt: dropping the owner box frees plain memory and cannot panic; the rest is pointer
// writes.
unsafe extern "C" fn release_partitions(partitions: *mut AdbcPartitions) {
    // SAFETY: The caller grants exclusive access to the partitions struct exported by this module.
    let Some(partitions) = (unsafe { partitions.as_mut() }) else {
        return;
    };
    if !partitions.private_data.is_null() {
        // SAFETY: export_partitions stored this box; this callback consumes it once and clears
        // private_data.
        drop(unsafe { Box::from_raw(partitions.private_data.cast::<PartitionsOwner>()) });
        partitions.private_data = std::ptr::null_mut();
    }
    // The arrays pointed into the freed box.
    partitions.num_partitions = 0;
    partitions.partitions = std::ptr::null_mut();
    partitions.partition_lengths = std::ptr::null_mut();
    // Clearing this makes the callback idempotent, which the header requires.
    partitions.release = None;
}

#[cfg(test)]
mod tests;
