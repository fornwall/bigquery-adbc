//! Importing bound Arrow arrays and C streams, checked before Arrow's importer trusts them.

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi_and_data_type};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatch, RecordBatchOptions, RecordBatchReader, StructArray};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Fields, Schema, SchemaRef, UnionMode};

use super::guard::missing_argument;
use crate::error::invalid_argument;
use crate::types::MAX_SCHEMA_NESTING_DEPTH;

#[derive(Debug)]
pub(super) struct BoundStreamReader {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
    finished: bool,
}

/// Take the caller's bound stream, leaving its struct released.
///
/// ADBC transfers ownership even when binding fails. Call this before validating the handle,
/// callbacks or schema so every error path drops the stream. Null and released inputs have no
/// ownership to transfer.
///
/// # Safety
/// `stream` must be null or point to an exclusively accessible, aligned Arrow C stream. The
/// caller transfers ownership of its producer data and callbacks; their outputs must retain
/// valid backing allocations and physical types until released, even after this call returns.
pub(super) unsafe fn take_stream(
    stream: *mut FFI_ArrowArrayStream,
) -> Result<FFI_ArrowArrayStream> {
    // SAFETY: the caller keeps the initialized stream struct readable; as_ref handles null.
    let Some(stream_ref) = (unsafe { stream.as_ref() }) else {
        return Err(missing_argument("stream"));
    };
    if stream_ref.release().is_none() {
        return Err(invalid_argument("stream has already been released"));
    }
    // SAFETY: the caller grants exclusive ownership and the stream has a release callback.
    // The shared borrow above has ended; from_raw moves ownership and marks the source released.
    Ok(unsafe { FFI_ArrowArrayStream::from_raw(stream) })
}

impl BoundStreamReader {
    /// Validate a stream taken by [`take_stream`] and read its schema.
    ///
    /// # Safety
    /// The producer must uphold [`take_stream`]'s callback and allocation contract for the
    /// reader's lifetime. In particular, every batch's physical buffers must match the stream's
    /// declared schema; owning the callback table alone cannot establish this relationship.
    pub(super) unsafe fn new(mut stream: FFI_ArrowArrayStream) -> Result<Self> {
        let missing = |name| invalid_argument(format!("stream is missing its {name} callback"));
        let get_schema = stream.get_schema().ok_or_else(|| missing("get_schema"))?;
        stream.get_next().ok_or_else(|| missing("get_next"))?;
        stream
            .get_last_error()
            .ok_or_else(|| missing("get_last_error"))?;
        let mut schema = FFI_ArrowSchema::empty();
        // SAFETY: `stream` is a live Arrow C stream, and the callback came from its own table.
        let code = unsafe { get_schema(&raw mut stream, &raw mut schema) };
        if code != 0 {
            return Err(invalid_argument(stream_error(
                &mut stream,
                "get_schema",
                code,
            )));
        }
        if schema.release().is_none() {
            return Err(invalid_argument(
                "bound stream get_schema returned a released schema",
            ));
        }
        let schema = check_schema(&schema)
            .and_then(|()| Schema::try_from(&schema).map_err(|failure| failure.to_string()))
            .map_err(|failure| {
                invalid_argument(format!(
                    "bound stream returned an invalid schema: {failure}"
                ))
            })?;
        Ok(Self {
            stream,
            schema: Arc::new(schema),
            finished: false,
        })
    }
}

impl Iterator for BoundStreamReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut array = FFI_ArrowArray::empty();
        let get_next = self
            .stream
            .get_next()
            .expect("validated when the stream was bound");
        // SAFETY: self owns the live producer stream and its callback; array is writable output.
        let code = unsafe { get_next(&raw mut self.stream, &raw mut array) };
        if code != 0 {
            self.finished = true;
            return Some(Err(ArrowError::CDataInterface(stream_error(
                &mut self.stream,
                "get_next",
                code,
            ))));
        }
        if array.release().is_none() {
            self.finished = true;
            return None;
        }
        // SAFETY: a valid Arrow stream returns arrays whose allocations and physical types match
        // its schema and remain live until their release callbacks run. Shape checks cannot prove
        // this producer obligation, which the caller accepted when transferring the stream.
        let result = unsafe { import_struct(array, self.schema.fields(), "bound stream batches") }
            .map_err(ArrowError::CDataInterface)
            .and_then(|columns| {
                let row_count = columns.len();
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    columns.into_parts().1,
                    &RecordBatchOptions::new().with_row_count(Some(row_count)),
                )
            });
        self.finished = result.is_err();
        Some(result)
    }
}

impl RecordBatchReader for BoundStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Import a bound struct array whose columns are `fields`, refusing it (named `subject` in
/// errors) unless its shape and values are sound and no top-level row is null.
///
/// # Safety
/// The producer must provide live, readable buffer and child allocations for the physical types
/// in `fields`, including every byte implied by lengths and offsets. They must remain immutable
/// and alive until the array's matching release callback runs. Shape and value checks can reject
/// malformed descriptors, but cannot establish allocation capacity or pointer provenance.
pub(super) unsafe fn import_struct(
    array: FFI_ArrowArray,
    fields: &Fields,
    subject: &str,
) -> std::result::Result<StructArray, String> {
    let invalid = |failure: String| format!("{subject} are not valid Arrow data: {failure}");
    // SAFETY: The caller supplies the live descriptor tree and buffers for these physical types.
    unsafe { check_shape(&array, fields) }.map_err(invalid)?;
    // SAFETY: check_shape rejects missing pointers and incompatible layouts; the caller still
    // guarantees readable buffer capacities, physical types, lifetimes and release ownership.
    let data = unsafe { from_ffi_and_data_type(array, DataType::Struct(fields.clone())) }
        .map_err(|failure| invalid(failure.to_string()))?;
    validate_imported(&data, fields).map_err(invalid)?;
    if data.null_count() != 0 {
        return Err(format!(
            "{subject} must not contain top-level null rows; put null values in the individual \
             columns instead"
        ));
    }
    Ok(StructArray::from(data))
}

/// Validate imported values before safe Arrow consumers rely on their invariants.
///
/// Arrow's importer skips value validation, and invalid offsets, view buffer indices or UTF-8
/// cause undefined behavior in array accessors. Each column is scanned once, recursively.
///
/// The producer must still supply valid allocations: the C interface carries no buffer lengths,
/// and Arrow sizes string/binary buffers from their final offsets, so a false final offset can
/// read out of bounds during validation itself.
fn validate_imported(data: &ArrayData, fields: &Fields) -> std::result::Result<(), String> {
    // The struct has no values of its own; `validate_full` covers each column recursively.
    data.validate_data()
        .map_err(|failure| format!("the top-level struct is invalid: {failure}"))?;
    for (field, column) in fields.iter().zip(data.child_data()) {
        column
            .validate_full()
            .map_err(|failure| format!("column {:?} is invalid: {failure}", field.name()))?;
    }
    Ok(())
}

/// The Arrow C data interface's `struct ArrowSchema`, whose fields `FFI_ArrowSchema` keeps
/// private.
#[repr(C)]
struct RawSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *const *const Self,
    dictionary: *const Self,
    release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>,
    private_data: *const c_void,
}

const _: () = {
    assert!(size_of::<RawSchema>() == size_of::<FFI_ArrowSchema>());
    assert!(align_of::<RawSchema>() == align_of::<FFI_ArrowSchema>());
};

/// Checks what Arrow's schema importer trusts and panics on: a non-null UTF-8 format, a UTF-8
/// name, and every child its format indexes. It also refuses nesting past
/// [`MAX_SCHEMA_NESTING_DEPTH`].
///
/// Run this before anything else walks a schema. Arrow's importer has no depth limit, and a stack
/// overflow aborts the host instead of unwinding through the FFI guard. Everything that follows
/// recurses over the imported type, array descriptors included, so this one bound covers it all.
///
/// The root is the batch, at depth 0, and a child or dictionary is one level below its parent,
/// which is how [`validate_schema_nesting`](crate::types::validate_schema_nesting) counts the
/// imported fields. Children that import would discard, as under a leaf format, count here too.
pub(super) fn check_schema(schema: &FFI_ArrowSchema) -> std::result::Result<(), String> {
    // SAFETY: Both repr(C) structs mirror ArrowSchema's field types and layout. The owned
    // FFI_ArrowSchema retains its producer's live strings and descriptor tree during this borrow.
    unsafe { check_schema_node(&*std::ptr::from_ref(schema).cast::<RawSchema>(), 0) }
}

/// Check `node`, `depth` levels below the root. Nothing past the limit is read.
///
/// # Safety
/// Each non-null format/name pointer must address a readable NUL-terminated string in one
/// allocation. Each non-null children array must contain `n_children` readable pointers, and
/// each non-null child/dictionary must be an aligned `RawSchema` satisfying this contract
/// recursively. All reached storage must stay live and unmodified throughout this call.
unsafe fn check_schema_node(node: &RawSchema, depth: usize) -> std::result::Result<(), String> {
    if depth > MAX_SCHEMA_NESTING_DEPTH {
        return Err(format!(
            "a schema is nested {depth} levels deep; the maximum is {MAX_SCHEMA_NESTING_DEPTH}"
        ));
    }
    if node.format.is_null() {
        return Err("a schema has a null format".to_owned());
    }
    // SAFETY: the producer supplies a NUL-terminated format string.
    let format = unsafe { CStr::from_ptr(node.format) }
        .to_str()
        .map_err(|_| "a schema's format is not valid UTF-8".to_owned())?;
    // SAFETY: the producer supplies a null or NUL-terminated name.
    if !node.name.is_null() && unsafe { CStr::from_ptr(node.name) }.to_str().is_err() {
        return Err(format!("the {format:?} schema's name is not valid UTF-8"));
    }
    let n_children = usize::try_from(node.n_children)
        .map_err(|_| format!("the {format:?} schema has {} children", node.n_children))?;
    let required = match format {
        "+l" | "+L" | "+vl" | "+vL" | "+m" => Some(1),
        "+r" => Some(2),
        _ if format.starts_with("+w:") => Some(1),
        _ => None,
    };
    if let Some(required) = required
        && required != n_children
    {
        return Err(format!(
            "the {format:?} schema has {n_children} children, but its format requires {required}"
        ));
    }
    if n_children > 0 && node.children.is_null() {
        return Err(format!(
            "the {format:?} schema has children but a null children pointer"
        ));
    }
    for index in 0..n_children {
        // SAFETY: the producer supplies n_children readable pointers, each null or a live schema.
        let child = unsafe { node.children.add(index).read_unaligned().as_ref() }
            .ok_or_else(|| format!("the {format:?} schema's child {index} is a null pointer"))?;
        // SAFETY: The caller's descriptor-tree contract also covers this live child.
        unsafe { check_schema_node(child, depth + 1) }?;
    }
    // SAFETY: the producer's dictionary pointer is null or a live schema.
    if let Some(dictionary) = unsafe { node.dictionary.as_ref() } {
        // SAFETY: The caller's descriptor-tree contract also covers this live dictionary.
        unsafe { check_schema_node(dictionary, depth + 1) }?;
    }
    Ok(())
}

/// The Arrow C data interface's `struct ArrowArray`. `FFI_ArrowArray` is `#[repr(C)]` over exactly
/// this layout but keeps the fields [`check_shape`] needs private.
#[repr(C)]
struct RawArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *const *const c_void,
    children: *const *const Self,
    dictionary: *const Self,
    release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
    private_data: *const c_void,
}

const _: () = {
    assert!(size_of::<RawArray>() == size_of::<FFI_ArrowArray>());
    assert!(align_of::<RawArray>() == align_of::<FFI_ArrowArray>());
};

/// Check that an imported struct array has the shape `fields` requires (non-negative lengths, the
/// specified buffer and child counts per type, and non-null pointers wherever Arrow's importer
/// reads), naming the offending column.
///
/// Arrow's importer trusts the shape: it `assert!`s child counts, indexes buffers and children
/// unchecked, and reads a string column's offsets to size its values. A mismatched array would
/// panic inside the driver, poisoning the statement over a caller mistake, or crash it.
/// [`validate_imported`] checks the values afterwards.
///
/// # Safety
/// The descriptor tree must satisfy [`check_node`]'s allocation contract for a struct with
/// `fields`. Owning an `FFI_ArrowArray` alone does not establish that its buffers match `fields`.
unsafe fn check_shape(array: &FFI_ArrowArray, fields: &Fields) -> std::result::Result<(), String> {
    // SAFETY: Both repr(C) structs mirror ArrowArray's field types and layout; the borrow keeps
    // the descriptor alive. The cast does not borrow any of its pointees.
    let root = unsafe { &*std::ptr::from_ref(array).cast::<RawArray>() };
    // SAFETY: The caller supplies a live struct descriptor with readable buffer pointers.
    unsafe { check_buffers(root, &DataType::Struct(Fields::empty())) }
        .map_err(|failure| format!("the top-level struct is invalid: {failure}"))?;
    if usize::try_from(root.n_children) != Ok(fields.len()) {
        return Err(format!(
            "the array has {} columns, but its schema declares {}",
            root.n_children,
            fields.len()
        ));
    }
    for (index, field) in fields.iter().enumerate() {
        // SAFETY: n_children matches fields.len(); the caller supplies that many live pointers
        // and the descendant allocations required by each field's physical type.
        unsafe { child(root, index).and_then(|column| check_node(column, field.data_type())) }
            .map_err(|failure| format!("column {:?} is invalid: {failure}", field.name()))?;
    }
    Ok(())
}

/// # Safety
/// `node` must satisfy [`check_buffers`]'s contract. Each non-null children array must contain
/// `n_children` readable pointers, and each non-null child/dictionary must be an aligned
/// `RawArray` satisfying this contract for its corresponding physical type. All reached
/// storage must stay live and unmodified throughout this call.
unsafe fn check_node(node: &RawArray, data_type: &DataType) -> std::result::Result<(), String> {
    // SAFETY: The caller supplies the buffer allocations for this node's physical type.
    unsafe { check_buffers(node, data_type) }?;
    let child_count = match data_type {
        DataType::Struct(fields) => fields.len(),
        DataType::Union(fields, _) => fields.len(),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(..) => 1,
        DataType::RunEndEncoded(..) => 2,
        _ => 0,
    };
    if usize::try_from(node.n_children) != Ok(child_count) {
        return Err(format!(
            "the {data_type} array has {} children, but its type has {child_count}",
            node.n_children,
        ));
    }
    // SAFETY: Each index below is within child_count; the caller supplies the live descriptor
    // tree and buffer allocations for the corresponding child type.
    let check_child = |index, data_type| unsafe { check_node(child(node, index)?, data_type) };
    match data_type {
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                check_child(index, field.data_type())?;
            }
        }
        DataType::Union(fields, _) => {
            for (index, (_, field)) in fields.iter().enumerate() {
                check_child(index, field.data_type())?;
            }
        }
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::Map(item, _) => check_child(0, item.data_type())?,
        DataType::RunEndEncoded(run_ends, values) => {
            check_child(0, run_ends.data_type())?;
            check_child(1, values.data_type())?;
        }
        DataType::Dictionary(_, values) => {
            // SAFETY: the producer's dictionary pointer is null or a live child descriptor.
            let dictionary = unsafe { node.dictionary.as_ref() }
                .ok_or_else(|| format!("the {data_type} array has no dictionary"))?;
            // SAFETY: The caller's recursive contract covers this dictionary and its value type.
            unsafe { check_node(dictionary, values) }?;
        }
        _ => {}
    }
    Ok(())
}

/// # Safety
/// `node.n_children` must exceed `index`; its non-null children array must contain that many
/// readable pointers, each null or a live `RawArray`, all retained for the returned borrow.
unsafe fn child(node: &RawArray, index: usize) -> std::result::Result<&RawArray, String> {
    if node.children.is_null() {
        return Err("the array has children but a null children pointer".into());
    }
    // SAFETY: the caller guarantees this in-bounds pointer entry and its child's lifetime;
    // read_unaligned permits a producer pointer array without native pointer alignment.
    unsafe { node.children.add(index).read_unaligned().as_ref() }
        .ok_or_else(|| format!("child {index} is a null pointer"))
}

/// The buffer count the C data interface specifies for `data_type`, validity buffer included, and
/// whether it is a minimum because variadic data buffers may follow.
fn buffer_count(data_type: &DataType) -> (usize, bool) {
    match data_type {
        DataType::Null | DataType::RunEndEncoded(..) => (0, false),
        DataType::Struct(_) | DataType::FixedSizeList(..) => (1, false),
        DataType::Union(_, UnionMode::Sparse) => (1, false),
        DataType::Union(_, UnionMode::Dense) => (2, false),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(..) => (2, false),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::ListView(_)
        | DataType::LargeListView(_) => (3, false),
        // Validity, views, any number of data buffers, and their lengths.
        DataType::BinaryView | DataType::Utf8View => (3, true),
        DataType::Dictionary(key, _) => buffer_count(key),
    }
}

/// # Safety
/// A non-null `buffers` array must contain `n_buffers` readable pointers. Any offsets or view
/// lengths that pass the null, alignment and size checks must have readable allocations for
/// the ranges implied by `length`, `offset`, `n_buffers` and `data_type`. All these allocations
/// must stay live and unmodified throughout this call; structural checks cannot prove capacity.
unsafe fn check_buffers(node: &RawArray, data_type: &DataType) -> std::result::Result<(), String> {
    if let DataType::Dictionary(key, _) = data_type
        && !key.is_dictionary_key_type()
    {
        return Err(format!("dictionary key type {key} must be an integer type"));
    }
    if node.length < 0 || node.offset < 0 {
        return Err(format!(
            "the {data_type} array has length {} and offset {}; neither may be negative",
            node.length, node.offset
        ));
    }
    if node.null_count < -1 || node.null_count > node.length {
        return Err(format!(
            "the {data_type} array has null count {}; expected -1 or a count between 0 and {}",
            node.null_count, node.length
        ));
    }
    if let DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size) = data_type
        && *size < 0
    {
        return Err(format!("{data_type} declares a negative size"));
    }
    let (expected, variadic) = buffer_count(data_type);
    let n_buffers = usize::try_from(node.n_buffers).map_err(|_| {
        format!(
            "the {data_type} array has an invalid buffer count: {}",
            node.n_buffers
        )
    })?;
    if n_buffers != expected && !(variadic && n_buffers > expected) {
        return Err(format!(
            "the {data_type} array has {} buffers, but the Arrow C data interface specifies {}{expected}",
            node.n_buffers,
            if variadic { "at least " } else { "" },
        ));
    }
    if n_buffers > 0 && node.buffers.is_null() {
        return Err(format!("the {data_type} array has a null buffers pointer"));
    }
    // Arrow's importer discards a positive null count when the bitmap is absent. Check before
    // importing so malformed bindings cannot silently turn nulls into ordinary values.
    if node.null_count > 0
        && !matches!(
            data_type,
            DataType::Null | DataType::Union(..) | DataType::RunEndEncoded(..)
        )
        // SAFETY: applicable types have a checked buffer entry and buffers is non-null;
        // the producer must supply the actual readable pointer array.
        && unsafe { node.buffers.read_unaligned() }.is_null()
    {
        return Err(format!(
            "the {data_type} array declares {} nulls but has no validity bitmap",
            node.null_count
        ));
    }
    // Arrow dereferences offsets and view buffer lengths before validating the imported array.
    // Reject absent or misaligned pointers before that can abort the host process.
    let dereferenced = match data_type {
        DataType::Binary | DataType::Utf8 if node.length != 0 || node.offset != 0 => {
            Some((1, align_of::<i32>()))
        }
        DataType::LargeBinary | DataType::LargeUtf8 if node.length != 0 || node.offset != 0 => {
            Some((1, align_of::<i64>()))
        }
        DataType::BinaryView | DataType::Utf8View if n_buffers > expected => {
            Some((n_buffers - 1, align_of::<i64>()))
        }
        _ => None,
    };
    if let Some((index, alignment)) = dereferenced {
        // SAFETY: index is within the checked n_buffers; the producer supplies the pointer array.
        let buffer = unsafe { node.buffers.add(index).read_unaligned() };
        if buffer.is_null() {
            return Err(format!(
                "the {data_type} array's buffer {index} is a null pointer"
            ));
        }
        if !buffer.addr().is_multiple_of(alignment) {
            return Err(format!(
                "the {data_type} array's buffer {index} is not aligned to {alignment} bytes"
            ));
        }
        // Arrow turns these values into allocation sizes before `validate_full` runs, so reject
        // impossible sizes first. The producer still owns the allocation-size contract.
        if matches!(data_type, DataType::BinaryView | DataType::Utf8View) {
            for index in 0..n_buffers - expected {
                // SAFETY: alignment was checked; the producer supplies one i64 per data buffer.
                let length = unsafe { buffer.cast::<i64>().add(index).read() };
                if length < 0 {
                    return Err(format!(
                        "the {data_type} array's data buffer {index} has negative length {length}"
                    ));
                }
            }
        } else {
            let small_offsets = matches!(data_type, DataType::Utf8 | DataType::Binary);
            let width = if small_offsets {
                size_of::<i32>()
            } else {
                size_of::<i64>()
            };
            let end = node
                .length
                .checked_add(node.offset)
                .and_then(|end| usize::try_from(end).ok())
                .filter(|&end| end < isize::MAX.unsigned_abs() / width)
                .ok_or_else(|| format!("the {data_type} array's offsets buffer is too large"))?;
            let last = if small_offsets {
                // SAFETY: alignment and byte offset are checked; the producer guarantees end + 1
                // initialized i32 offsets in this allocation, independent of their values.
                i64::from(unsafe { buffer.cast::<i32>().add(end).read() })
            } else {
                // SAFETY: alignment and byte offset are checked; the producer guarantees end + 1
                // initialized i64 offsets in this allocation, independent of their values.
                unsafe { buffer.cast::<i64>().add(end).read() }
            };
            if last < 0 {
                return Err(format!(
                    "the {data_type} array's final offset is negative: {last}"
                ));
            }
        }
    }
    // Arrow's importer multiplies the length by these widths without overflow checks.
    let fits = node
        .length
        .checked_add(node.offset)
        .and_then(|end| usize::try_from(end).ok())
        .and_then(|end| end.checked_add(1))
        .and_then(|elements| elements.checked_mul(element_bits(data_type)))
        .is_some_and(|bits| bits <= isize::MAX.unsigned_abs());
    if !fits {
        return Err(format!(
            "the {data_type} array's length {} and offset {} are too large for its buffers",
            node.length, node.offset
        ));
    }
    Ok(())
}

/// Bits per element in `data_type`'s widest length-sized buffer, or a fixed-size list's values
/// per element.
fn element_bits(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean => 1,
        DataType::FixedSizeBinary(size) => usize::try_from(*size)
            .ok()
            .and_then(|size| size.checked_mul(8))
            .unwrap_or(usize::MAX),
        DataType::FixedSizeList(_, size) => usize::try_from(*size).unwrap_or(usize::MAX),
        DataType::Union(_, UnionMode::Sparse) => 8,
        DataType::Utf8
        | DataType::Binary
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::Map(..)
        | DataType::Union(_, UnionMode::Dense) => 32,
        DataType::LargeUtf8
        | DataType::LargeBinary
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => 64,
        DataType::Utf8View | DataType::BinaryView => 128,
        DataType::Dictionary(key, _) => element_bits(key),
        _ => data_type.primitive_width().map_or(0, |width| width * 8),
    }
}

fn stream_error(stream: &mut FFI_ArrowArrayStream, callback: &str, code: c_int) -> String {
    // SAFETY: stream owns the live producer callback; exclusive access serializes calls.
    let message = stream
        .get_last_error()
        .map_or(std::ptr::null(), |get_last_error| unsafe {
            get_last_error(stream)
        });
    let detail = if message.is_null() {
        "producer supplied no error description".into()
    } else {
        // SAFETY: a valid stream returns a NUL-terminated message retained until its next
        // callback; no callback occurs while this borrowed message is formatted.
        unsafe { CStr::from_ptr(message) }.to_string_lossy()
    };
    format!("bound Arrow C stream {callback} failed with error code {code}: {detail}")
}

#[cfg(test)]
mod tests;
