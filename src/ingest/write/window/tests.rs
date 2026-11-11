use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, BinaryArray, DictionaryArray, Int8Array, Int64Array, LargeBinaryArray,
    LargeStringArray, ListArray, ListViewArray, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;

use super::*;

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("value", column)]).unwrap()
}

/// The bytes the window's rows of the batch's only column address.
fn window_bytes(batch: &RecordBatch, offset: usize, rows: usize) -> usize {
    addressed_bytes(
        batch.column(0).as_ref(),
        batch.column(0).data_type(),
        offset,
        rows,
        usize::MAX,
    )
}

/// Empty strings followed by long ones: the density of the first rows would put every remaining
/// row in one window, which normalization would copy whole, past 2 GiB at full scale (300,000
/// empty and 220,000 10 KB `LargeUtf8` values). The window stops at what the bytes allow.
#[test]
fn a_window_is_capped_by_the_bytes_its_rows_address() {
    let long = "x".repeat(10_000);
    let skewed = batch(Arc::new(LargeStringArray::from_iter_values(
        (0..3_000)
            .map(|_| "")
            .chain((0..200).map(|_| long.as_str())),
    )));
    let target = 100_000;

    // The density of an empty-string chunk suggests every remaining row.
    let rows = window_rows(&skewed, &skewed.schema(), 2_990, 1_000_000, target);
    assert!(rows < 210, "the window took {rows} rows");
    assert!(window_bytes(&skewed, 2_990, rows) <= 2 * target);
    // Empty strings still need offsets, but this many fit.
    assert_eq!(
        window_rows(&skewed, &skewed.schema(), 0, 1_000, target),
        1_000
    );
    // A single row is a window, however long.
    assert_eq!(window_rows(&skewed, &skewed.schema(), 3_000, 1, 1), 1);
}

/// Fixed-width values count at their width, not at the capacity of a buffer a slice shares with
/// rows outside it, which would shrink every window of a slice of a large table.
#[test]
fn a_slice_counts_only_its_own_rows() {
    let whole = batch(Arc::new(Int64Array::from_iter_values(0..1_000_000)));
    let slice = whole.slice(500_000, 10_000);
    assert_eq!(window_bytes(&slice, 0, 10_000), 80_000);
    assert_eq!(
        window_rows(&slice, &slice.schema(), 0, 10_000, 100_000),
        10_000
    );
}

/// Views, lists and structs count the values their rows reach.
#[test]
fn every_layout_counts_the_values_its_rows_reach() {
    let long = "y".repeat(1_000);
    let views = batch(Arc::new(StringViewArray::from_iter_values([
        "",
        long.as_str(),
        "",
        long.as_str(),
    ])));
    assert_eq!(window_bytes(&views, 1, 2), 1_000 + 2 * 4);

    let strings: ArrayRef = Arc::new(StringArray::from_iter_values(["a", "bb", "ccc", "dddd"]));
    let lists = batch(Arc::new(ListArray::new(
        Arc::new(Field::new("item", strings.data_type().clone(), false)),
        OffsetBuffer::from_lengths([1, 3]),
        strings.clone(),
        None,
    )));
    assert_eq!(window_bytes(&lists, 1, 1), 2 + 3 + 4 + 3 * 4 + 4);

    let structs = batch(Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("text", strings.data_type().clone(), false)),
            strings,
        ),
        (
            Arc::new(Field::new("number", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
    ])));
    assert_eq!(window_bytes(&structs, 2, 2), 3 + 4 + 2 * 4 + 16 + 1);
}

#[test]
fn dictionary_windows_count_selected_values_including_repeated_keys() {
    let long = "x".repeat(1_000);
    let values = Arc::new(StringArray::from_iter_values(
        std::iter::once(long.as_str()).chain(std::iter::repeat_n("", 99)),
    ));
    let keys = Int8Array::from_iter(
        [None, Some(99)]
            .into_iter()
            .chain(std::iter::repeat_n(Some(0), 1_000)),
    );
    let input = batch(Arc::new(DictionaryArray::<Int8Type>::new(keys, values)));
    assert_eq!(window_bytes(&input, 0, 2), 8);
    assert_eq!(window_bytes(&input, 2, 1_000), 1_004_000);
    let slice = input.slice(2, 1_000);
    let rows = window_rows(&slice, &slice.schema(), 10, 990, 100_000);
    assert!((100..=200).contains(&rows), "the window took {rows} rows");
}

/// Every string and byte layout a dictionary can hold counts each row's value and offset, and a
/// null key one offset.
#[test]
fn byte_dictionaries_count_each_rows_value() {
    let keys = Int8Array::from(vec![Some(2), None, Some(0), Some(2), Some(1)]);
    let texts = ["", "a", "long value"];
    for values in [
        Arc::new(StringArray::from_iter_values(texts)) as ArrayRef,
        Arc::new(LargeStringArray::from_iter_values(texts)),
        Arc::new(BinaryArray::from_iter_values(texts)),
        Arc::new(LargeBinaryArray::from_iter_values(texts)),
    ] {
        let input = batch(Arc::new(DictionaryArray::<Int8Type>::new(
            keys.clone(),
            values,
        )));
        assert_eq!(window_bytes(&input, 0, 5), 14 + 4 + 4 + 14 + 5);
        assert_eq!(window_bytes(&input, 1, 3), 4 + 4 + 14);
    }
}

#[test]
fn list_view_windows_count_overlapping_ranges_and_skip_null_lists() {
    let values = Arc::new(StringArray::from(vec!["x".repeat(1_000)]));
    let lists = ListViewArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        vec![0; 1_000].into(),
        vec![1; 1_000].into(),
        values,
        Some(NullBuffer::from_iter((0..1_000).map(|row| row >= 10))),
    );
    let input = batch(Arc::new(lists));
    assert_eq!(window_bytes(&input, 0, 10), 40);
    assert_eq!(window_bytes(&input, 10, 990), 990 * 1_008);
    let slice = input.slice(10, 990);
    let rows = window_rows(&slice, &slice.schema(), 10, 980, 100_000);
    assert!((99..=199).contains(&rows), "the window took {rows} rows");
}

#[test]
fn nested_repeated_ranges_stop_counting_once_over_budget() {
    let mut column: ArrayRef = Arc::new(Int64Array::from(vec![1; 1_000]));
    for _ in 0..6 {
        column = Arc::new(ListViewArray::new(
            Arc::new(Field::new("item", column.data_type().clone(), false)),
            vec![0; 1_000].into(),
            vec![1_000; 1_000].into(),
            column,
            None,
        ));
    }
    let bytes = addressed_bytes(column.as_ref(), column.data_type(), 0, 1_000, 100_000);
    assert!((100_001..200_000).contains(&bytes), "counted {bytes} bytes");
}

#[test]
fn null_dictionary_rows_count_offsets_including_struct_children() {
    for data_type in [
        DataType::Utf8,
        DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Binary, true),
            ]
            .into(),
        ),
    ] {
        let values = arrow_array::new_empty_array(&data_type);
        let keys = Int8Array::from(vec![None; 1_000]);
        let input = batch(Arc::new(DictionaryArray::<Int8Type>::new(keys, values)));
        let rows = window_rows(&input, &input.schema(), 0, 1_000, 100);
        assert!(rows <= 50, "{data_type} took {rows} null rows");
        assert!(window_bytes(&input, 0, rows) <= 200);
    }
}

#[test]
fn zero_width_fixed_layouts_still_count_offsets_and_struct_validity() {
    let binary = batch(arrow_array::new_null_array(
        &DataType::FixedSizeBinary(0),
        1_000,
    ));
    assert_eq!(window_bytes(&binary, 0, 1_000), 4_000);
    let structures = batch(Arc::new(StructArray::new_empty_fields(1_000, None)));
    assert_eq!(window_bytes(&structures, 0, 1_000), 125);
}
