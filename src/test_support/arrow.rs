//! Downcast helpers for the Arrow arrays a reader under test produced, and the `BIGQUERY:type`
//! annotation tests put on the fields they feed in.

use std::collections::HashMap;

use arrow_array::{Array, ArrayRef, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields};

use crate::types::BIGQUERY_TYPE_METADATA_KEY;

/// Downcasts an array to the concrete type it is known to hold in the test.
///
/// Takes `&dyn Array` rather than `&ArrayRef` so that a child array reached through
/// `StructArray::column` or `ListArray::values` needs no reborrow at the call site; an
/// `&ArrayRef` still coerces to it.
pub(crate) fn typed<T: Array + 'static>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref::<T>().unwrap()
}

/// Downcasts a column to the `ListArray` it is known to be.
pub(crate) fn list(array: &ArrayRef) -> &ListArray {
    typed::<ListArray>(array)
}

/// The flattened `StructArray` child values of a list-of-structs column.
pub(crate) fn child_structs(array: &ArrayRef) -> &StructArray {
    typed::<StructArray>(list(array).values())
}

/// Every string of a `StringArray` column, in row order.
#[track_caller]
pub(crate) fn strings(array: &ArrayRef) -> Vec<&str> {
    let array = typed::<StringArray>(array);
    array
        .iter()
        .map(|value| value.expect("expected a non-null string"))
        .collect()
}

/// The string at `index` in a `StringArray` column.
#[track_caller]
pub(crate) fn string_at(array: &ArrayRef, index: usize) -> &str {
    let array = typed::<StringArray>(array);
    assert!(
        array.is_valid(index),
        "expected a non-null string at {index}"
    );
    array.value(index)
}

/// The nullable `start` and `end` bounds of a `RANGE` struct over `data_type`.
pub(crate) fn range_bounds(data_type: DataType) -> Fields {
    vec![
        Field::new("start", data_type.clone(), true),
        Field::new("end", data_type, true),
    ]
    .into()
}

/// `field` carrying the `BIGQUERY:type` annotation that names its GoogleSQL type.
pub(crate) fn bigquery_typed(field: Field, rich_type: impl Into<String>) -> Field {
    field.with_metadata(HashMap::from([(
        BIGQUERY_TYPE_METADATA_KEY.to_owned(),
        rich_type.into(),
    )]))
}

#[test]
fn string_assertions_reject_values_hidden_by_the_null_mask() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use arrow_buffer::NullBuffer;

    let (offsets, values, _) = StringArray::from(vec!["hidden"]).into_parts();
    let array: ArrayRef = Arc::new(StringArray::new(
        offsets,
        values,
        Some(NullBuffer::new_null(1)),
    ));
    assert!(catch_unwind(AssertUnwindSafe(|| strings(&array))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| string_at(&array, 0))).is_err());

    let array: ArrayRef = Arc::new(StringArray::from(vec!["", "visible"]));
    assert_eq!(strings(&array), ["", "visible"]);
    assert_eq!(string_at(&array, 0), "");
}
