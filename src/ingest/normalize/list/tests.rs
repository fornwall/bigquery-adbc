use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};

use super::{checked_list_length_add, compact_list_ranges, gather_retained_values};

#[test]
fn contiguous_and_empty_lists_keep_range_scratch_small() {
    let length = 16_384;
    let (offsets, ranges) =
        compact_list_ranges(length, length, "values", |row| Ok(Some((row, row + 1)))).unwrap();
    assert_eq!(offsets.len(), length + 1);
    assert_eq!(ranges, [(0, length)]);
    assert!(ranges.capacity() < length);

    let (offsets, ranges) = compact_list_ranges(length, 0, "values", |_| Ok(None)).unwrap();
    assert!(offsets.iter().all(|&offset| offset == 0));
    assert_eq!(ranges.capacity(), 0);
}

#[test]
fn gathered_list_values_match_arrow_select_concat() {
    use arrow_select::concat::concat;

    let sources: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("bcd"),
            Some("é"),
            None,
            Some("f"),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(i64::MAX),
            Some(-1),
            Some(0),
            None,
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
            Some(vec![Some(2), None]),
            None,
            Some(vec![Some(3)]),
        ])),
        Arc::new(StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "a",
                arrow_schema::DataType::Int64,
                true,
            )),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
        )])),
    ];
    for values in sources {
        // Coalesced, non-adjacent ranges are the only shape `compact_list_ranges` hands the
        // gather; a sliced source gathers through its logical window.
        for (source, ranges, label) in [
            (values.clone(), [(0, 2), (3, 4), (5, 6)].as_slice(), ""),
            (values.slice(1, 5), [(0, 1), (2, 4)].as_slice(), "sliced "),
        ] {
            let gathered =
                gather_retained_values(&source, ranges, source.data_type(), "values").unwrap();
            let slices = ranges
                .iter()
                .map(|&(start, end)| source.slice(start, end - start))
                .collect::<Vec<_>>();
            let expected = concat(
                &slices
                    .iter()
                    .map(|slice| slice.as_ref())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(
                gathered.as_ref(),
                expected.as_ref(),
                "{label}{}",
                source.data_type()
            );
        }
    }
}

#[test]
fn gathers_preserve_identity_and_emptiness_at_the_edges() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let full = gather_retained_values(&values, &[(0, 3)], values.data_type(), "values").unwrap();
    assert!(Arc::ptr_eq(&full, &values));
    let empty = gather_retained_values(&values, &[], values.data_type(), "values").unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.data_type(), values.data_type());
}

#[test]
fn rejects_canonical_list_offset_overflow() {
    for (current, additional) in [(i32::MAX, 1), (0, usize::MAX)] {
        let error = checked_list_length_add(current, additional, "values").unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
    }
}
