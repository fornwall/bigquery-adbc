//! Rewriting any Arrow list-like layout as the one canonical list.
//!
//! Arrow has five array layouts (`List`, `LargeList`, `ListView`, `LargeListView`,
//! `FixedSizeList`), and a canonical `List` can still hide child values behind a NULL entry or
//! address only a window of its child. All are rewritten into one shape: i32 offsets, no null
//! buffer, and a child holding exactly the values the offsets address, in row order.
//!
//! The offset arithmetic lives in [`compact_list_ranges`]; every layout feeds it ranges.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::{
    Array, ArrayRef, GenericListViewArray, ListArray, OffsetSizeTrait, new_empty_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field};

use super::site::ValueSite;
use super::{downcast_array, normalize_column};
use crate::error::{driver_defect, error};

pub(super) fn normalize_list_column(
    column: &ArrayRef,
    array: &ListArray,
    item: &Arc<Field>,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    let field_name = site.name();
    // Gather before validation so NULL entries can hide invalid values. Use the original child
    // coordinates: compaction already builds fresh offsets and needs no intermediate window.
    if nulls_hide_values(array) {
        return normalize_noncanonical_list_column(
            array.values(),
            array.len(),
            item,
            site,
            |index| offset_value_range(array, array.value_offsets(), index, field_name),
        );
    }
    let (start, child) = list_value_window(array, field_name)?;
    // The child holds every entry's values end to end, so a rejection below this point is
    // located by the offsets that address them rather than by an index into the child.
    let values = normalize_column(
        &child,
        item,
        &site.list_elements(array.value_offsets(), start),
    )?;
    // NULL lists become empty arrays. Reuse unchanged lists, but compare the item field too:
    // normalization may attach decimal metadata without changing its physical values.
    let normalized = if array.null_count() == 0
        && Arc::ptr_eq(&child, &values)
        && matches!(array.data_type(), DataType::List(original_item) if original_item == item)
    {
        column.clone()
    } else {
        // Validation preserves the original offsets. Only a changed child or list needs offsets
        // rebased onto the child window, so unchanged request windows allocate no offset copy.
        let offsets = if start == 0 {
            array.offsets().clone()
        } else {
            let first = array.offsets().first();
            OffsetBuffer::new(ScalarBuffer::from(
                array
                    .value_offsets()
                    .iter()
                    .map(|offset| offset - first)
                    .collect::<Vec<i32>>(),
            ))
        };
        Arc::new(
            ListArray::try_new(item.clone(), offsets, values, None).map_err(|source| {
                error(
                    format!("BigQuery ingest list field {field_name:?} is invalid: {source}"),
                    Status::InvalidData,
                )
            })?,
        )
    };
    reject_null_list_elements(&normalized, site)?;
    Ok(normalized)
}

/// Whether any NULL list entry addresses child values, which a normalized list drops.
fn nulls_hide_values(array: &ListArray) -> bool {
    if array.null_count() == 0 {
        return false;
    }
    let windowed = array.value_offsets();
    windowed
        .iter()
        .zip(windowed.iter().skip(1))
        .enumerate()
        .any(|(index, (start, end))| array.is_null(index) && start != end)
}

/// The slice of a list's child array that its offsets actually address, and its starting index.
///
/// Arrow slices a `ListArray` by slicing only its offset and null buffers; `values()` still
/// returns the whole child. Checks and rebuilds must see only the window, or a NULL element or
/// out-of-calendar date outside this batch would be reported against it. An unsliced list keeps
/// both buffers, so the common case copies nothing.
fn list_value_window(array: &ListArray, field_name: &str) -> Result<(usize, ArrayRef)> {
    let offsets = array.value_offsets();
    let first = offsets.first().copied().unwrap_or(0);
    let start = usize_list_offset(first, field_name)?;
    let end = usize_list_offset(offsets.last().copied().unwrap_or(0), field_name)?;
    if start == 0 && end == array.values().len() {
        return Ok((start, array.values().clone()));
    }
    Ok((start, array.values().slice(start, end - start)))
}

/// BigQuery arrays hold no NULL elements, so a normalized list's child values must carry none.
///
/// The item field's nullability never reaches BigQuery (an ingest array is only a `REPEATED`
/// field), so the driver must reject a NULL element itself before sending.
fn reject_null_list_elements(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()> {
    let field_name = site.name();
    let list = downcast_array::<ListArray>(column, field_name)?;
    let values = list.values();
    if values.null_count() == 0 {
        return Ok(());
    }
    // A sliced list keeps the whole child array (see `list_value_window`), so only the elements
    // its offsets address belong to this batch. Those offsets are in the child's own coordinates,
    // which is what the element site needs to name the entry holding a NULL.
    let offsets = list.value_offsets();
    let start = usize_list_offset(offsets.first().copied().unwrap_or(0), field_name)?;
    let end = usize_list_offset(offsets.last().copied().unwrap_or(0), field_name)?;
    let Some(element) = (start..end).find(|&index| values.is_null(index)) else {
        return Ok(());
    };
    let row = site.list_elements(offsets, 0).row(element);
    Err(error(
        format!(
            "BigQuery ingest list field {field_name:?} contains a NULL array element at row \
             {row}; a BigQuery array cannot contain NULL"
        ),
        Status::InvalidData,
    ))
}

/// The compacted i32 offsets of a normalized list plus the retained child value ranges.
type CompactedListLayout = (Vec<i32>, Vec<(usize, usize)>);

/// Rebuilds a list layout that hides no values behind null entries: `value_range` yields the
/// child value range of each non-null list, and the result is the compacted i32 offsets plus
/// the retained child ranges, coalesced when adjacent. Every list normalization shares this
/// offset arithmetic.
fn compact_list_ranges<F>(
    length: usize,
    values_len: usize,
    field_name: &str,
    mut value_range: F,
) -> Result<CompactedListLayout>
where
    F: FnMut(usize) -> Result<Option<(usize, usize)>>,
{
    let mut normalized_offsets = Vec::with_capacity(length + 1);
    // Adjacent rows usually coalesce to one range; reserving one per row wastes 16 bytes per
    // list on 64-bit hosts, even when every list is empty.
    let mut retained_ranges: Vec<(usize, usize)> = Vec::new();
    let mut normalized_length = 0_i32;
    normalized_offsets.push(normalized_length);
    for index in 0..length {
        if let Some((start, end)) = value_range(index)? {
            let value_length = end.checked_sub(start).ok_or_else(|| {
                error(
                    format!(
                        "BigQuery ingest list field {field_name:?} has an invalid value range {start}..{end}"
                    ),
                    Status::InvalidData,
                )
            })?;
            if end > values_len {
                return Err(error(
                    format!(
                        "BigQuery ingest list field {field_name:?} value range {start}..{end} exceeds its child array length {values_len}"
                    ),
                    Status::InvalidData,
                ));
            }
            normalized_length =
                checked_list_length_add(normalized_length, value_length, field_name)?;
            if start != end {
                if let Some((_, retained_end)) = retained_ranges.last_mut()
                    && *retained_end == start
                {
                    *retained_end = end;
                } else {
                    retained_ranges.push((start, end));
                }
            }
        }
        normalized_offsets.push(normalized_length);
    }
    Ok((normalized_offsets, retained_ranges))
}

/// Gathers the retained child ranges of a compacted list into one contiguous values array.
///
/// This is `arrow_select::concat::concat` narrowed to the one gather the driver needs: slices of
/// a single source array. `MutableArrayData` copies ranges type-erased, so every value type stays
/// supported without linking arrow-select's per-type concat and interleave kernels.
/// [`compact_list_ranges`] has already validated every range against the child array, so the
/// slicing arithmetic here cannot go out of bounds.
fn gather_retained_values(
    values: &ArrayRef,
    ranges: &[(usize, usize)],
    item_type: &DataType,
    field_name: &str,
) -> Result<ArrayRef> {
    match ranges {
        [] => Ok(new_empty_array(item_type)),
        &[(0, end)] if end == values.len() => Ok(values.clone()),
        &[(start, end)] => Ok(values.slice(start, end - start)),
        ranges => {
            let data = values.to_data();
            let capacity = ranges.iter().map(|&(start, end)| end - start).sum();
            let mut gathered = MutableArrayData::new(vec![&data], false, capacity);
            for &(start, end) in ranges {
                gathered.try_extend(0, start, end).map_err(|source| {
                    driver_defect(format_args!(
                        "failed to gather BigQuery ingest list values in field \
                         {field_name:?}: {source}"
                    ))
                })?;
            }
            Ok(arrow_array::make_array(gathered.freeze()))
        }
    }
}

pub(super) fn normalize_noncanonical_list_column<F>(
    values: &ArrayRef,
    length: usize,
    item: &Arc<Field>,
    site: &ValueSite<'_>,
    value_range: F,
) -> Result<ArrayRef>
where
    F: FnMut(usize) -> Result<Option<(usize, usize)>>,
{
    let field_name = site.name();
    let (normalized_offsets, retained_ranges) =
        compact_list_ranges(length, values.len(), field_name, value_range)?;
    let values = gather_retained_values(values, &retained_ranges, item.data_type(), field_name)?;
    let values = if retained_ranges.is_empty() {
        // An empty gather already carries the normalized item type.
        values
    } else {
        // The gathered child no longer addresses anything the caller handed over, so only the
        // offsets built alongside it can put an element back on its batch row.
        normalize_column(&values, item, &site.list_elements(&normalized_offsets, 0))?
    };

    let normalized = ListArray::try_new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(normalized_offsets)),
        values,
        None,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|source| {
        error(
            format!(
                "failed to build normalized BigQuery ingest list field {field_name:?}: {source}"
            ),
            Status::InvalidData,
        )
    })?;
    reject_null_list_elements(&normalized, site)?;
    Ok(normalized)
}

/// Canonicalizes either Arrow list-view layout while preserving its row order and ranges.
///
/// List views may address their child values out of order or more than once. The range compactor
/// gathers each row's range in row order, so overlapping and non-monotonic views keep their
/// logical values in the canonical offset-encoded layout.
pub(super) fn normalize_list_view_column<O>(
    array: &GenericListViewArray<O>,
    item: &Arc<Field>,
    site: &ValueSite<'_>,
) -> Result<ArrayRef>
where
    O: OffsetSizeTrait + std::fmt::Display + TryInto<usize>,
{
    let field_name = site.name();
    normalize_noncanonical_list_column(array.values(), array.len(), item, site, |index| {
        if array.is_null(index) {
            return Ok(None);
        }
        let start = usize_list_offset(array.value_offset(index), field_name)?;
        let size = usize_list_offset(array.value_size(index), field_name)?;
        let end = start.checked_add(size).ok_or_else(|| {
            error(
                format!("BigQuery ingest list field {field_name:?} value offset and size overflow"),
                Status::InvalidData,
            )
        })?;
        Ok(Some((start, end)))
    })
}

fn checked_list_length_add(current: i32, additional: usize, field_name: &str) -> Result<i32> {
    let overflow = || {
        error(
            format!("BigQuery ingest list field {field_name:?} exceeds Arrow i32 offsets"),
            Status::InvalidData,
        )
    };
    let additional = i32::try_from(additional).map_err(|_| overflow())?;
    current.checked_add(additional).ok_or_else(overflow)
}

/// Returns the child value range of offset-encoded list entry `index`, `None` for a null entry.
pub(super) fn offset_value_range<O>(
    array: &dyn Array,
    offsets: &[O],
    index: usize,
    field_name: &str,
) -> Result<Option<(usize, usize)>>
where
    O: Copy + std::fmt::Display + TryInto<usize>,
{
    if array.is_null(index) {
        return Ok(None);
    }
    #[expect(
        clippy::indexing_slicing,
        reason = "a list array stores len + 1 offsets, and index < len"
    )]
    let (start, end) = (offsets[index], offsets[index + 1]);
    Ok(Some((
        usize_list_offset(start, field_name)?,
        usize_list_offset(end, field_name)?,
    )))
}

fn usize_list_offset<O>(offset: O, field_name: &str) -> Result<usize>
where
    O: Copy + std::fmt::Display + TryInto<usize>,
{
    offset.try_into().map_err(|_| {
        error(
            format!("BigQuery ingest list field {field_name:?} has an invalid offset {offset}"),
            Status::InvalidData,
        )
    })
}

#[cfg(test)]
mod tests;
