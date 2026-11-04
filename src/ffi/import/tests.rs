use adbc_core::error::Status;
use arrow_array::ffi::to_ffi;
use arrow_array::{ArrayRef, Int64Array, RecordBatchIterator};
use arrow_schema::Field;

use super::*;
use crate::ffi::test_support::{edit_stream, nested_struct_array, null};
use crate::test_support::errors::assert_message;

fn node(length: i64, buffers: &[*const c_void], children: &[*const RawArray]) -> RawArray {
    RawArray {
        length,
        null_count: 0,
        offset: 0,
        n_buffers: i64::try_from(buffers.len()).unwrap(),
        n_children: i64::try_from(children.len()).unwrap(),
        buffers: buffers.as_ptr(),
        children: children.as_ptr(),
        dictionary: std::ptr::null(),
        release: None,
        private_data: std::ptr::null(),
    }
}

/// # Safety
/// `root` must satisfy [`check_node`]'s allocation contract for a struct with one `column`.
unsafe fn shape_of(root: &RawArray, column: DataType) -> std::result::Result<(), String> {
    let fields = Fields::from(vec![Field::new("c", column, true)]);
    // SAFETY: RawArray mirrors the C layout; root and its caller-provided backing allocations
    // remain live for the check, which only borrows and never invokes the null release callback.
    unsafe { check_shape(&*std::ptr::from_ref(root).cast::<FFI_ArrowArray>(), &fields) }
}

#[test]
fn invalid_dictionary_keys_are_rejected_before_reading_buffers() {
    for key in [
        DataType::Null,
        DataType::Struct(Fields::empty()),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        let mut column = node(1, &[], &[]);
        column.null_count = 1;
        column.buffers = std::ptr::null();
        let expected = format!("dictionary key type {key} must be an integer type");
        assert_eq!(
            // SAFETY: The descriptor has no backing allocations; an invalid key is rejected
            // before any buffer or child pointer is read.
            unsafe {
                check_node(
                    &column,
                    &DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8)),
                )
            },
            Err(expected)
        );
    }
}

#[test]
fn negative_buffer_counts_are_rejected_before_reading_pointers() {
    for data_type in [DataType::Utf8View, DataType::BinaryView, DataType::Int64] {
        for count in [-1, i64::MIN] {
            let mut column = node(1, &[], &[]);
            column.n_buffers = count;
            column.buffers = std::ptr::null();
            assert_eq!(
                // SAFETY: Negative counts are rejected before reading any buffer pointers.
                unsafe { check_node(&column, &data_type) },
                Err(format!(
                    "the {data_type} array has an invalid buffer count: {count}"
                ))
            );
        }
    }
}

#[test]
fn negative_imported_buffer_sizes_are_rejected_before_arrow_uses_them() {
    let offsets32 = [0_i32, -1];
    let offsets64 = [0_i64, -1];
    let view_lengths = [0_i64, -1];
    let validity = [std::ptr::null::<c_void>()];
    for (data_type, sizes) in [
        (DataType::Utf8, offsets32.as_ptr().cast::<c_void>()),
        (DataType::Binary, offsets32.as_ptr().cast()),
        (DataType::LargeUtf8, offsets64.as_ptr().cast()),
        (DataType::LargeBinary, offsets64.as_ptr().cast()),
        (DataType::Utf8View, view_lengths.as_ptr().cast()),
        (DataType::BinaryView, view_lengths.as_ptr().cast()),
    ] {
        let views = matches!(data_type, DataType::Utf8View | DataType::BinaryView);
        let buffers = if views {
            vec![
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                sizes,
            ]
        } else {
            vec![std::ptr::null(), sizes, std::ptr::null()]
        };
        let column = node(1, &buffers, &[]);
        let columns = [&raw const column];
        let root = node(1, &validity, &columns);
        let reason = if views {
            "data buffer 1 has negative length -1"
        } else {
            "final offset is negative: -1"
        };
        assert_eq!(
            // SAFETY: Local descriptor/pointer arrays and correctly typed offset/length buffers
            // remain live; invalid values do not affect the capacity needed by this check.
            unsafe { shape_of(&root, data_type.clone()) },
            Err(format!(
                "column \"c\" is invalid: the {data_type} array's {reason}"
            ))
        );
    }
}

#[test]
fn unrepresentable_offset_buffer_sizes_are_rejected_before_reading() {
    let offsets = [0_i64];
    let buffers = [std::ptr::null(), offsets.as_ptr().cast(), std::ptr::null()];
    for data_type in [DataType::Utf8, DataType::LargeUtf8] {
        for (length, offset) in [(i64::MAX, 1), (i64::MAX, 0), (0, i64::MAX)] {
            let mut column = node(length, &buffers, &[]);
            column.offset = offset;
            assert_eq!(
                // SAFETY: The pointer array is live; the impossible offset range is rejected
                // before accessing the deliberately undersized offsets allocation.
                unsafe { check_node(&column, &data_type) },
                Err(format!(
                    "the {data_type} array's offsets buffer is too large"
                ))
            );
        }
    }
}

/// The buffer and child counts `check_shape` demands are the ones a conforming exporter
/// produces, for every layout the C data interface defines.
#[test]
fn accepts_every_layout_arrow_exports() {
    use arrow_array::{Array, ArrayRef, StringViewArray, StructArray, new_null_array};
    use arrow_schema::{IntervalUnit, TimeUnit, UnionFields};

    let item = || Arc::new(Field::new("item", DataType::Int32, true));
    let entries = Arc::new(Field::new_struct(
        "entries",
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ],
        false,
    ));
    let union_fields = || {
        UnionFields::try_new(
            [0, 1],
            [
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
        )
        .unwrap()
    };
    let types = [
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float64,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Date32,
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::FixedSizeBinary(4),
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, 38),
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::BinaryView,
        DataType::List(item()),
        DataType::LargeList(item()),
        DataType::FixedSizeList(item(), 2),
        DataType::ListView(item()),
        DataType::LargeListView(item()),
        DataType::Map(entries, false),
        DataType::Struct(vec![Field::new("inner", DataType::Utf8, true)].into()),
        DataType::Union(union_fields(), UnionMode::Sparse),
        DataType::Union(union_fields(), UnionMode::Dense),
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
    ];
    let mut fields: Vec<Field> = types
        .iter()
        .enumerate()
        .map(|(index, data_type)| Field::new(format!("c{index}"), data_type.clone(), true))
        .collect();
    let mut columns: Vec<ArrayRef> = types
        .iter()
        .map(|data_type| new_null_array(data_type, 3))
        .collect();
    // Long enough to live out of line, so the export carries variadic data buffers.
    fields.push(Field::new("view", DataType::Utf8View, false));
    columns.push(Arc::new(StringViewArray::from(vec![
        "a value longer than twelve bytes";
        3
    ])));
    let fields = Fields::from(fields);
    let array = StructArray::new(fields.clone(), columns, None);
    let (exported, _) = to_ffi(&array.to_data()).unwrap();
    // SAFETY: Arrow exported this array from exactly these fields and retains all its buffers.
    assert_eq!(unsafe { check_shape(&exported, &fields) }, Ok(()));
}

/// The pointers Arrow's importer would `unwrap` or read through are refused while they are
/// still only pointers.
#[test]
fn misaligned_pointers_arrow_would_read_through_are_refused() {
    let data = [0_u128; 4];
    let aligned = data.as_ptr().cast::<c_void>();
    let misaligned = data.as_ptr().cast::<u8>().wrapping_add(1).cast::<c_void>();
    let validity = [std::ptr::null::<c_void>()];
    let offsets = [std::ptr::null(), misaligned, aligned];
    let views = [std::ptr::null(), aligned, aligned, misaligned];
    for (data_type, buffers, index, alignment) in [
        (DataType::Utf8, offsets.as_slice(), 1, align_of::<i32>()),
        (DataType::Binary, offsets.as_slice(), 1, align_of::<i32>()),
        (
            DataType::LargeUtf8,
            offsets.as_slice(),
            1,
            align_of::<i64>(),
        ),
        (
            DataType::LargeBinary,
            offsets.as_slice(),
            1,
            align_of::<i64>(),
        ),
        (DataType::Utf8View, views.as_slice(), 3, align_of::<i64>()),
        (DataType::BinaryView, views.as_slice(), 3, align_of::<i64>()),
    ] {
        let column = node(1, buffers, &[]);
        let columns = [&raw const column];
        let root = node(1, &validity, &columns);
        let expected = format!(
            "column \"c\" is invalid: the {data_type} array's buffer {index} is not aligned to {alignment} bytes"
        );
        // SAFETY: Descriptor/pointer arrays remain live; misaligned buffers are rejected before
        // being dereferenced, so the check never reads their deliberately invalid layout.
        assert_eq!(unsafe { shape_of(&root, data_type.clone()) }, Err(expected));

        // Empty, unsliced strings do not dereference their offsets; views with no data
        // buffers do not dereference their lengths buffer.
        let column = node(0, &offsets, &[]);
        let columns = [&raw const column];
        let root = node(0, &validity, &columns);
        // SAFETY: The live descriptors are empty, so the check reads no offsets or view lengths.
        assert_eq!(unsafe { shape_of(&root, data_type) }, Ok(()));
    }
}

#[test]
fn null_pointers_arrow_would_read_through_are_refused() {
    let data = [0_u8; 16];
    let present = data.as_ptr().cast::<c_void>();
    // Every pointer array a node refers to is a local, so it outlives the node.
    let validity = [std::ptr::null::<c_void>()];
    let no_child = [std::ptr::null::<RawArray>()];

    let root = node(1, &validity, &no_child);
    assert_eq!(
        // SAFETY: Local pointer arrays remain live; the null child is rejected before access.
        unsafe { shape_of(&root, DataType::Int64) }.unwrap_err(),
        r#"column "c" is invalid: child 0 is a null pointer"#
    );

    let mut root = node(1, &validity, &no_child);
    root.children = std::ptr::null();
    assert_eq!(
        // SAFETY: The local descriptor is live; null children storage is rejected before access.
        unsafe { shape_of(&root, DataType::Int64) }.unwrap_err(),
        r#"column "c" is invalid: the array has children but a null children pointer"#
    );

    let missing_offsets = [std::ptr::null(), std::ptr::null(), present];
    let column = node(1, &missing_offsets, &[]);
    let columns = [&raw const column];
    let root = node(1, &validity, &columns);
    assert_eq!(
        // SAFETY: Local descriptors and pointer arrays remain live; null offsets are rejected
        // before reading any string data.
        unsafe { shape_of(&root, DataType::Utf8) }.unwrap_err(),
        r#"column "c" is invalid: the Utf8 array's buffer 1 is a null pointer"#
    );
    // An empty string column at offset zero is never read, so its offsets may be absent.
    let absent = [std::ptr::null(); 3];
    let column = node(0, &absent, &[]);
    let columns = [&raw const column];
    let root = node(0, &validity, &columns);
    // SAFETY: The live empty descriptors require no offsets or string data reads.
    assert_eq!(unsafe { shape_of(&root, DataType::Utf8) }, Ok(()));

    let values = [std::ptr::null(), present];
    let column = node(-1, &values, &[]);
    let columns = [&raw const column];
    let root = node(1, &validity, &columns);
    // SAFETY: All descriptors/pointer arrays are live; negative lengths are rejected before data.
    let failure = unsafe { shape_of(&root, DataType::Int64) }.unwrap_err();
    assert!(
        failure.contains("length -1 and offset 0; neither may be negative"),
        "{failure}"
    );

    // A leaf type has no children, so a producer claiming some disagrees with its schema.
    let column = node(1, &values, &no_child);
    let columns = [&raw const column];
    let root = node(1, &validity, &columns);
    assert_eq!(
        // SAFETY: All descriptors/pointer arrays are live; Int64 shape checks read no value data,
        // and the unexpected child count is rejected before traversing children.
        unsafe { shape_of(&root, DataType::Int64) }.unwrap_err(),
        r#"column "c" is invalid: the Int64 array has 1 children, but its type has 0"#
    );
}

fn schema_node(format: *const c_char, children: &[*const RawSchema]) -> RawSchema {
    RawSchema {
        format,
        name: std::ptr::null(),
        metadata: std::ptr::null(),
        flags: 0,
        n_children: i64::try_from(children.len()).unwrap(),
        children: children.as_ptr(),
        dictionary: std::ptr::null(),
        release: None,
        private_data: std::ptr::null(),
    }
}

/// Arrow's schema importer asserts a non-null format, `expect`s UTF-8 text and unwraps child
/// pointers; each of those is refused here instead.
#[test]
fn malformed_schemas_arrow_would_panic_on_are_refused() {
    let invalid_utf8 = [0xff_u8, 0];
    let invalid_utf8 = invalid_utf8.as_ptr().cast::<c_char>();
    let leaf = schema_node(c"l".as_ptr(), &[]);
    let leaves = [&raw const leaf];

    let mut named = schema_node(c"l".as_ptr(), &[]);
    named.name = invalid_utf8;
    let mut negative = schema_node(c"+s".as_ptr(), &[]);
    negative.n_children = -1;
    let mut no_children = schema_node(c"+s".as_ptr(), &leaves);
    no_children.children = std::ptr::null();
    let null_child = [std::ptr::null::<RawSchema>()];
    let null_child = schema_node(c"+s".as_ptr(), &null_child);
    let mut bad_dictionary = schema_node(c"i".as_ptr(), &[]);
    bad_dictionary.dictionary = &raw const named;
    for (node, expected) in [
        (
            &schema_node(std::ptr::null(), &[]),
            "a schema has a null format",
        ),
        (
            &schema_node(invalid_utf8, &[]),
            "a schema's format is not valid UTF-8",
        ),
        (&named, "the \"l\" schema's name is not valid UTF-8"),
        (&negative, "the \"+s\" schema has -1 children"),
        (
            &no_children,
            "the \"+s\" schema has children but a null children pointer",
        ),
        (&null_child, "the \"+s\" schema's child 0 is a null pointer"),
        (
            &schema_node(c"+m".as_ptr(), &[]),
            "the \"+m\" schema has 0 children, but its format requires 1",
        ),
        (
            &bad_dictionary,
            "the \"l\" schema's name is not valid UTF-8",
        ),
    ] {
        assert_eq!(
            // SAFETY: Strings are static or the live terminated invalid_utf8 buffer; local pointer
            // arrays and descriptors outlive this call. Nulls and negative counts are checked
            // first.
            unsafe { check_schema_node(node, 0) },
            Err(expected.to_owned())
        );
    }
    let column = schema_node(c"+l".as_ptr(), &leaves);
    let columns = [&raw const column];
    assert_eq!(
        // SAFETY: The descriptor tree consists of live locals and static terminated strings.
        unsafe { check_schema_node(&schema_node(c"+s".as_ptr(), &columns), 0) },
        Ok(())
    );
}

/// `inner` under one more level of the Arrow nesting type numbered `kind`.
fn nest(kind: usize, inner: DataType) -> DataType {
    let item = Arc::new(Field::new("item", inner.clone(), true));
    match kind {
        0 => DataType::Struct(vec![item].into()),
        1 => DataType::List(item),
        2 => DataType::LargeList(item),
        3 => DataType::FixedSizeList(item, 1),
        4 => DataType::ListView(item),
        5 => DataType::LargeListView(item),
        6 => DataType::Union(
            arrow_schema::UnionFields::try_new([0], [item.as_ref().clone()]).unwrap(),
            UnionMode::Sparse,
        ),
        7 => DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner)),
        8 => DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            item,
        ),
        // Two levels at once: the entries struct, then its key and value.
        9 => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", inner, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        _ => unreachable!("kind is below NESTING_KINDS"),
    }
}

const NESTING_KINDS: usize = 10;

/// The check at the C boundary and the one execution applies must draw the same line, or a schema
/// passes Bind and fails later, or an accepted one crashes the recursion the first was to bound.
/// Compare them on the exported form of every nesting type, alone and mixed, across the limit.
#[test]
fn the_bind_time_nesting_check_agrees_with_validate_schema_nesting() {
    for kinds in (0..NESTING_KINDS)
        .map(|kind| vec![kind])
        .chain([(0..NESTING_KINDS).collect()])
    {
        let mut data_type = DataType::Int64;
        let (mut accepted, mut refused) = (false, false);
        for level in 0..=MAX_SCHEMA_NESTING_DEPTH + 1 {
            let fields = Fields::from(vec![Field::new("c", data_type.clone(), true)]);
            let expected = crate::types::validate_schema_nesting(&fields, "bound").is_ok();
            let exported = FFI_ArrowSchema::try_from(&DataType::Struct(fields)).unwrap();
            assert_eq!(
                check_schema(&exported).is_ok(),
                expected,
                "{kinds:?} at {level}: {data_type}"
            );
            accepted |= expected;
            refused |= !expected;
            data_type = nest(kinds[level % kinds.len()], data_type);
        }
        assert!(accepted && refused, "{kinds:?} never crossed the limit");
    }
}

/// Check `depth` schemas chained below a root by `link`, ending in a leaf.
fn check_chain(
    depth: usize,
    link: fn(&mut RawSchema, &[*const RawSchema; 1]),
) -> std::result::Result<(), String> {
    let mut nodes: Vec<RawSchema> = (0..=depth)
        .map(|_| schema_node(c"i".as_ptr(), &[]))
        .collect();
    let first = nodes.as_mut_ptr();
    let links: Vec<[*const RawSchema; 1]> = (1..=depth)
        // SAFETY: index is within nodes, whose allocation is not resized below.
        .map(|index| [unsafe { first.add(index) }.cast_const()])
        .collect();
    for (index, target) in links.iter().enumerate() {
        // SAFETY: index is within nodes. Only this node is borrowed, for this call.
        link(unsafe { &mut *first.add(index) }, target);
    }
    // SAFETY: Every node and pointer array is a live local; each node's format is static.
    unsafe { check_schema_node(&*first, 0) }
}

/// Dictionaries chain through their own pointer rather than the children array, so they need the
/// same bound. A node's depth counts every link above it, whichever kind.
#[test]
fn schema_nesting_is_counted_through_children_and_dictionaries_alike() {
    fn through_child(node: &mut RawSchema, target: &[*const RawSchema; 1]) {
        node.format = c"+s".as_ptr();
        node.n_children = 1;
        node.children = target.as_ptr();
    }
    fn through_dictionary(node: &mut RawSchema, target: &[*const RawSchema; 1]) {
        node.dictionary = target[0];
    }
    for link in [through_child, through_dictionary] {
        assert_eq!(check_chain(MAX_SCHEMA_NESTING_DEPTH, link), Ok(()));
        assert_eq!(
            check_chain(MAX_SCHEMA_NESTING_DEPTH + 1, link),
            Err(format!(
                "a schema is nested {} levels deep; the maximum is {MAX_SCHEMA_NESTING_DEPTH}",
                MAX_SCHEMA_NESTING_DEPTH + 1
            ))
        );
    }
}

/// A batch at the limit runs the whole per-batch path: shape check, Arrow's import, and
/// `validate_full`, each recursing over the bound schema.
#[test]
fn a_bound_stream_at_the_nesting_limit_imports_its_batches() {
    let batch = RecordBatch::from(nested_struct_array(MAX_SCHEMA_NESTING_DEPTH));
    let schema = batch.schema();
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch.clone())),
        schema,
    )));
    // SAFETY: Arrow's own export is a live stream whose batches match its schema.
    let mut reader =
        unsafe { BoundStreamReader::new(take_stream(&raw mut stream).unwrap()) }.unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
}

/// Arrow's importer multiplies a length by its buffer's element width unchecked, and checks
/// a fixed-size list's child count with `expect`, so either overflow panics inside it.
#[test]
fn lengths_whose_buffer_sizes_overflow_are_refused_before_import() {
    let item = Arc::new(Field::new("item", DataType::Int8, true));
    let leaf_buffers = [std::ptr::null::<c_void>(); 2];
    let leaf = node(0, &leaf_buffers, &[]);
    let leaves = [&raw const leaf];
    let no_buffers = [std::ptr::null::<c_void>(); 3];
    for (data_type, length, n_buffers, children) in [
        (DataType::Int64, 1_i64 << 60, 2, &[][..]),
        (DataType::Decimal256(76, 0), 1_i64 << 57, 2, &[]),
        (DataType::FixedSizeBinary(i32::MAX), 1_i64 << 35, 2, &[]),
        (
            DataType::FixedSizeList(item.clone(), i32::MAX),
            1_i64 << 40,
            1,
            &leaves,
        ),
        (DataType::List(item.clone()), i64::MAX - 1, 2, &leaves),
        (DataType::ListView(item), 1_i64 << 62, 3, &leaves),
    ] {
        let column = node(length, &no_buffers[..n_buffers], children);
        assert_eq!(
            // SAFETY: Pointer arrays/descriptors are live; these types' shape checks read no
            // values and reject the oversized length before traversing children.
            unsafe { check_node(&column, &data_type) },
            Err(format!(
                "the {data_type} array's length {length} and offset 0 are too large for \
                 its buffers"
            ))
        );
    }
}

#[test]
fn bound_stream_schema_failures_preserve_diagnostics_and_release_ownership() {
    extern "C" fn failed_schema(_: *mut FFI_ArrowArrayStream, _: *mut FFI_ArrowSchema) -> c_int {
        libc::EIO
    }
    extern "C" fn description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        c"producer schema unavailable".as_ptr()
    }
    extern "C" fn no_description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        null()
    }
    extern "C" fn released_schema(_: *mut FFI_ArrowArrayStream, _: *mut FFI_ArrowSchema) -> c_int {
        0
    }

    for (get_schema, get_last_error, expected) in [
        (
            failed_schema as unsafe extern "C" fn(_, _) -> _,
            description as unsafe extern "C" fn(_) -> _,
            "producer schema unavailable",
        ),
        (failed_schema, no_description, "no error description"),
        (released_schema, no_description, "released schema"),
    ] {
        let lifetime = Arc::new(());
        let retained = lifetime.clone();
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::once_with(move || {
                drop(retained);
                Ok(batch)
            }),
            schema,
        )));
        // SAFETY: Replacement callbacks access no private data and return static or null messages.
        // The original release callback retains the producer's ownership; the deliberately empty
        // schema is rejected before any schema-dependent data access.
        unsafe {
            edit_stream(&mut stream, |parts| {
                parts.get_schema = Some(get_schema);
                parts.get_last_error = Some(get_last_error);
            });
        }
        assert_eq!(Arc::strong_count(&lifetime), 2);
        // SAFETY: The live stream owns its producer data and release callback; take_stream moves
        // them and clears the source before the reader validates the replacement schema callback.
        let failure =
            unsafe { BoundStreamReader::new(take_stream(&raw mut stream).unwrap()) }.unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_message(&failure, &[expected]);
        if expected != "released schema" {
            assert_message(&failure, &[&libc::EIO.to_string()]);
        }
        assert!(stream.release().is_none());
        assert_eq!(Arc::strong_count(&lifetime), 1);
    }
}

#[test]
fn imported_stream_batches_preserve_schema_rows_and_buffers_after_release() {
    use arrow_array::{RecordBatchOptions, RecordBatchReader};
    use arrow_schema::{DataType, Field};

    for fields in [vec![], vec![Field::new("value", DataType::Int64, true)]] {
        let schema = Arc::new(
            Schema::new(fields).with_metadata(std::collections::HashMap::from([(
                "source".to_owned(),
                "caller".to_owned(),
            )])),
        );
        let columns = if schema.fields().is_empty() {
            vec![]
        } else {
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef]
        };
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            [
                Ok(RecordBatch::new_empty(schema.clone())),
                Ok(batch.clone()),
            ],
            schema.clone(),
        )));
        // SAFETY: This valid producer stream is exclusively borrowed; take_stream moves ownership
        // and clears the source before the reader takes it.
        let mut reader =
            unsafe { BoundStreamReader::new(take_stream(&raw mut stream).unwrap()) }.unwrap();
        assert!(stream.release().is_none());
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        let found = reader.next().unwrap().unwrap();
        assert_eq!(found, batch);
        drop(batch);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(found.num_rows(), 3);
        assert_eq!(found.schema(), schema);
        if !found.columns().is_empty() {
            let values = found
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values, &Int64Array::from(vec![Some(1), None, Some(3)]));
        }
    }
}

#[test]
fn imported_streams_reject_null_rows_and_mismatched_column_counts() {
    use arrow_schema::{DataType, Field};

    /// # Safety
    /// `out` must point to exclusively writable, aligned storage for an Arrow array.
    unsafe extern "C" fn null_row(_: *mut FFI_ArrowArrayStream, out: *mut FFI_ArrowArray) -> c_int {
        let array =
            StructArray::new_null(vec![Field::new("value", DataType::Int64, true)].into(), 1);
        let (array, _) = to_ffi(&array.to_data()).unwrap();
        // SAFETY: The reader invokes this callback with writable, empty output storage; the
        // exported array transfers its own buffer ownership into that destination.
        unsafe { std::ptr::write(out, array) };
        0
    }

    for (fields, expected) in [
        (vec![], "the array has 1 columns, but its schema declares 0"),
        (
            vec![Field::new("value", DataType::Int64, true)],
            "top-level null rows",
        ),
    ] {
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            Arc::new(Schema::new(fields)),
        )));
        // SAFETY: The replacement exports a valid owned array into the reader's output; the
        // original private data and release callback remain paired.
        unsafe { edit_stream(&mut stream, |parts| parts.get_next = Some(null_row)) };
        // SAFETY: The stream retains its producer ownership and valid callback output allocations;
        // the reader checks the declared shape and null rows before using batches.
        let mut reader =
            unsafe { BoundStreamReader::new(take_stream(&raw mut stream).unwrap()) }.unwrap();
        let failure = reader.next().unwrap().unwrap_err();
        assert!(failure.to_string().contains(expected), "{failure}");
        assert!(reader.next().is_none());
    }
}
