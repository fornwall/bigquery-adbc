//! How many input rows to normalize for the next AppendRows request.
//!
//! A window's rows come from the density of the chunk before it, but density says nothing about
//! the rows still to come: a column of empty strings followed by long ones would put every
//! remaining row in one window. Normalization copies a window whole, and its 32-bit offsets
//! address at most 2 GiB, so a window is also capped by the bytes its input rows address.

use arrow_array::cast::AsArray as _;
use arrow_array::types::{ArrowDictionaryKeyType, BinaryViewType, ByteViewType, StringViewType};
use arrow_array::{
    Array, DictionaryArray, GenericByteViewArray, GenericListViewArray, OffsetSizeTrait,
    RecordBatch, downcast_dictionary_array,
};
use arrow_schema::DataType;

/// The rows from `offset` the next window of `input` takes: `suggested`, or as many as fit
/// twice `target_size`, the slack leaving the chunk encoder to make the exact cut.
pub(super) fn window_rows(
    input: &RecordBatch,
    offset: usize,
    suggested: usize,
    target_size: usize,
) -> usize {
    let mut rows = suggested.clamp(1, input.num_rows() - offset);
    let limit = target_size.saturating_mul(2);
    loop {
        let bytes = input
            .columns()
            .iter()
            .map(|column| addressed_bytes(column.as_ref(), offset, rows, limit))
            .fold(0, usize::saturating_add);
        if rows == 1 || bytes <= limit {
            return rows;
        }
        rows = scaled(rows, target_size, bytes).clamp(1, rows - 1);
    }
}

/// `rows * target / actual`, rounded down.
pub(super) fn scaled(rows: usize, target: usize, actual: usize) -> usize {
    if actual == 0 {
        return rows;
    }
    let scaled = (rows as u128) * (target as u128) / (actual as u128);
    usize::try_from(scaled).unwrap_or(usize::MAX)
}

/// The bytes rows `offset..offset + rows` of `column` take once normalized, estimated from what
/// they address rather than from buffer capacities, which a slice shares with the rows around it.
/// Dictionaries and list views count repeated references separately because normalization
/// materializes each one. Their traversal stops above `limit`, so nested overlapping views
/// cannot make sizing as expensive as materializing the whole expansion.
fn addressed_bytes(column: &dyn Array, offset: usize, rows: usize, limit: usize) -> usize {
    let width = |bytes: usize| bytes.saturating_mul(rows);
    match column.data_type() {
        DataType::Utf8 => offset_span(column.as_string::<i32>().value_offsets(), offset, rows)
            .saturating_add(width(4)),
        DataType::LargeUtf8 => offset_span(column.as_string::<i64>().value_offsets(), offset, rows)
            .saturating_add(width(4)),
        DataType::Binary => offset_span(column.as_binary::<i32>().value_offsets(), offset, rows)
            .saturating_add(width(4)),
        DataType::LargeBinary => {
            offset_span(column.as_binary::<i64>().value_offsets(), offset, rows)
                .saturating_add(width(4))
        }
        DataType::FixedSizeBinary(size) => {
            width(usize::try_from(*size).unwrap_or_default().saturating_add(4))
        }
        DataType::Utf8View => view_bytes::<StringViewType>(column.as_string_view(), offset, rows),
        DataType::BinaryView => view_bytes::<BinaryViewType>(column.as_binary_view(), offset, rows),
        DataType::List(_) => list_bytes(column.as_list::<i32>(), offset, rows, limit),
        DataType::LargeList(_) => list_bytes(column.as_list::<i64>(), offset, rows, limit),
        DataType::ListView(_) => list_view_bytes(column.as_list_view::<i32>(), offset, rows, limit),
        DataType::LargeListView(_) => {
            list_view_bytes(column.as_list_view::<i64>(), offset, rows, limit)
        }
        DataType::FixedSizeList(_, size) => {
            let size = usize::try_from(*size).unwrap_or_default();
            addressed_bytes(
                column.as_fixed_size_list().values().as_ref(),
                offset.saturating_mul(size),
                rows.saturating_mul(size),
                limit,
            )
            .saturating_add(width(4))
        }
        DataType::Struct(_) => column
            .as_struct()
            .columns()
            .iter()
            .map(|child| addressed_bytes(child.as_ref(), offset, rows, limit))
            .fold(rows.div_ceil(8), usize::saturating_add),
        DataType::Dictionary(_, _) => {
            let values = column.as_any_dictionary().values();
            if let Some(bytes) = values.data_type().primitive_width() {
                return width(bytes);
            }
            downcast_dictionary_array!(
                column => dictionary_bytes(column, offset, rows, limit),
                _ => unreachable!("the column has a dictionary data type"),
            )
        }
        DataType::Boolean => rows.div_ceil(8),
        data_type => match data_type.primitive_width() {
            Some(bytes) => width(bytes),
            // Other layouts normalization rewrites: their buffers, in proportion.
            None => scaled(column.get_array_memory_size(), rows, column.len().max(1)),
        },
    }
}

fn dictionary_bytes<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    offset: usize,
    rows: usize,
    limit: usize,
) -> usize {
    let mut bytes = 0_usize;
    let null_width = if array.null_count() == 0 {
        0
    } else {
        null_bytes(array.values().data_type(), 1)
    };
    for row in offset..offset + rows {
        let added = match array.key(row) {
            Some(key) => addressed_bytes(array.values().as_ref(), key, 1, limit),
            None => null_width,
        };
        bytes = bytes.saturating_add(added);
        if bytes > limit {
            break;
        }
    }
    bytes
}

/// Null dictionary keys still materialize fixed-width slots and offsets, including a struct's
/// child buffers, even when the dictionary has no values to inspect.
fn null_bytes(data_type: &DataType, rows: usize) -> usize {
    match data_type {
        DataType::Dictionary(_, values) => null_bytes(values, rows),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| null_bytes(field.data_type(), rows))
            .fold(rows.div_ceil(8), usize::saturating_add),
        DataType::FixedSizeList(item, size) => {
            let size = usize::try_from(*size).unwrap_or_default();
            null_bytes(item.data_type(), rows.saturating_mul(size))
                .saturating_add(rows.saturating_mul(4))
        }
        DataType::FixedSizeBinary(size) => {
            rows.saturating_mul(usize::try_from(*size).unwrap_or_default().saturating_add(4))
        }
        DataType::Boolean => rows.div_ceil(8),
        data_type => rows.saturating_mul(data_type.primitive_width().unwrap_or(4)),
    }
}

fn list_view_bytes<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    offset: usize,
    rows: usize,
    limit: usize,
) -> usize {
    let mut bytes = rows.saturating_mul(4);
    for row in offset..offset + rows {
        if bytes > limit {
            break;
        }
        if array.is_valid(row) {
            bytes = bytes.saturating_add(addressed_bytes(
                array.values().as_ref(),
                array.value_offset(row).as_usize(),
                array.value_size(row).as_usize(),
                limit,
            ));
        }
    }
    bytes
}

/// The values a byte or list array's offsets address for rows `offset..offset + rows`.
fn offset_span<O: OffsetSizeTrait>(offsets: &[O], offset: usize, rows: usize) -> usize {
    match (offsets.get(offset), offsets.get(offset + rows)) {
        (Some(start), Some(end)) => end.as_usize().saturating_sub(start.as_usize()),
        _ => 0,
    }
}

fn view_bytes<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
    offset: usize,
    rows: usize,
) -> usize {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the low 32 bits of an Arrow view are the value's length by definition"
    )]
    array
        .views()
        .get(offset..offset + rows)
        .unwrap_or_default()
        .iter()
        .map(|view| *view as u32 as usize)
        .fold(rows.saturating_mul(4), usize::saturating_add)
}

fn list_bytes<O: OffsetSizeTrait>(
    array: &arrow_array::GenericListArray<O>,
    offset: usize,
    rows: usize,
    limit: usize,
) -> usize {
    let offsets = array.value_offsets();
    let (Some(start), Some(end)) = (offsets.get(offset), offsets.get(offset + rows)) else {
        return 0;
    };
    let (start, end) = (start.as_usize(), end.as_usize());
    addressed_bytes(
        array.values().as_ref(),
        start,
        end.saturating_sub(start),
        limit,
    )
    .saturating_add(rows.saturating_mul(4))
}

#[cfg(test)]
mod tests;
