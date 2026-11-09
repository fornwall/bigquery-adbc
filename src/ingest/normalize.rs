//! Converts Arrow batches to the canonical ingest schema, preserving unchanged arrays.
//!
//! [`list`] canonicalizes list layouts; [`validate`] checks BigQuery value domains; [`encode`]
//! handles duration, interval and datetime-range wire encodings. [`site`] maps nested failures
//! back to the caller's field and stream row.

use std::borrow::Cow;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, RecordBatch, StructArray, new_null_array,
};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::{DataType, Field, IntervalUnit, SchemaRef};

use crate::error::{driver_defect, error};
use crate::types::cast::cast_column;
use crate::types::range_metadata;

mod decimal;
mod encode;
mod list;
mod site;
mod validate;

use encode::{encode_duration_column, encode_range_datetime_column, truncate_interval_precision};
use list::{
    normalize_list_column, normalize_list_view_column, normalize_noncanonical_list_column,
    offset_value_range,
};
use site::ValueSite;
use validate::{
    declared_max_length, validate_max_length, validate_range_values, validate_scalar_values,
};

/// Views a normalized column as the concrete Arrow array its field type promises.
///
/// Every normalization step below has already agreed on the column's `DataType`, so a failed
/// downcast is a driver defect rather than anything the caller can act on.
fn downcast_array<'a, A: Array + 'static>(column: &'a ArrayRef, name: &str) -> Result<&'a A> {
    column
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| normalized_array_type_error(name, column.data_type()))
}

/// Normalizes a whole batch; ingest normalizes windows of one with [`normalize_rows`].
#[cfg(any(test, feature = "benchmark"))]
pub(crate) fn normalize_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    normalize_rows(batch, schema, 0)
}

/// [`normalize_batch`] for a window of the bound stream whose first row is stream row
/// `first_row`, which a rejection counts from.
pub(crate) fn normalize_rows(
    batch: RecordBatch,
    schema: SchemaRef,
    first_row: usize,
) -> Result<RecordBatch> {
    let schema_matches = batch.schema_ref() == &schema;
    if schema_matches
        && !batch
            .columns()
            .iter()
            .zip(schema.fields())
            .any(|(column, field)| requires_same_type_value_normalization(column, field))
    {
        return Ok(batch);
    }

    let mut columns = Cow::Borrowed(batch.columns());
    for (index, (column, field)) in batch.columns().iter().zip(schema.fields()).enumerate() {
        let normalized =
            normalize_column(column, field, &ValueSite::column(field.name(), first_row))?;
        if !Arc::ptr_eq(column, &normalized) {
            *columns
                .to_mut()
                .get_mut(index)
                .expect("index enumerates the original batch columns") = normalized;
        }
    }
    if schema_matches && matches!(columns, Cow::Borrowed(_)) {
        return Ok(batch);
    }
    // Keep every source column so a narrower schema cannot silently discard input data.
    RecordBatch::try_new(schema, columns.into_owned()).map_err(|source| {
        error(
            format!("failed to normalize BigQuery ingest batch: {source}"),
            Status::InvalidData,
        )
    })
}

fn requires_same_type_value_normalization(column: &ArrayRef, field: &Field) -> bool {
    match field.data_type() {
        // `Date32` and `Timestamp` need no rewriting, but their values must be checked against
        // BigQuery's calendar range before Storage Write, which may create or replace the table.
        DataType::Date32
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Interval(_) => true,
        DataType::Utf8 | DataType::Binary => declared_max_length(field).is_some(),
        DataType::Struct(fields) => {
            range_metadata(field).is_some_and(|value| value == "RANGE<DATETIME>")
                || column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .is_none_or(|array| {
                        array.columns().iter().zip(fields).any(|(column, field)| {
                            requires_same_type_value_normalization(column, field)
                        })
                    })
        }
        // A canonical list is rewritten when it has NULL entries, which become empty arrays; when
        // its child has NULLs, which BigQuery arrays cannot hold and have to be found; and when
        // its elements need value checks of their own. Its offsets are trusted: a `ListArray` is
        // only built with valid ones by safe code, and the C data interface validates every
        // imported batch in full (`src/ffi/import.rs`).
        DataType::List(item) => column
            .as_any()
            .downcast_ref::<ListArray>()
            .is_none_or(|list| {
                list.null_count() != 0
                    || list.values().null_count() != 0
                    || requires_same_type_value_normalization(list.values(), item)
            }),
        _ => false,
    }
}

fn mask_struct_child(column: &ArrayRef, parent_nulls: &NullBuffer, name: &str) -> Result<ArrayRef> {
    // A `Null` array hides no values and cannot carry a validity buffer.
    if column.data_type() == &DataType::Null
        || column
            .nulls()
            .is_some_and(|nulls| nulls.contains(parent_nulls))
    {
        return Ok(column.clone());
    }
    // A NULL struct hides its child values, including values outside BigQuery's domain.
    // Share their buffers and combine only the validity bits before validating or casting.
    let data = column
        .to_data()
        .into_builder()
        .nulls(NullBuffer::union(column.nulls(), Some(parent_nulls)))
        .build()
        .map_err(|source| {
            error(
                format!("BigQuery ingest struct child field {name:?} is invalid: {source}"),
                Status::InvalidData,
            )
        })?;
    Ok(arrow_array::make_array(data))
}

fn normalize_column(column: &ArrayRef, field: &Field, site: &ValueSite<'_>) -> Result<ArrayRef> {
    match (column.data_type(), field.data_type()) {
        // An all-null column whose type the target table settled: NULLs of that type, which
        // normalize further like any others (a NULL list becomes an empty array).
        (DataType::Null, data_type) if data_type != &DataType::Null => {
            normalize_column(&new_null_array(data_type, column.len()), field, site)
        }
        (DataType::Dictionary(_, value_type), _) => {
            let values = cast_column(column, value_type, site.name())?;
            normalize_column(&values, field, site)
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let encode_datetime_range =
                range_metadata(field).is_some_and(|value| value == "RANGE<DATETIME>");
            let array = downcast_array::<StructArray>(column, site.name())?;
            // Most canonical structs need validation only. Clone child references only when a
            // child changes or the target schema requires rebuilding the struct.
            let mut columns = Cow::Borrowed(array.columns());
            for (index, (column, field)) in array.columns().iter().zip(fields).enumerate() {
                let site = site.field(field.name());
                let masked = array
                    .nulls()
                    .map(|nulls| mask_struct_child(column, nulls, site.name()))
                    .transpose()?;
                let mut normalized =
                    normalize_column(masked.as_ref().unwrap_or(column), field, &site)?;
                if encode_datetime_range {
                    normalized = encode_range_datetime_column(&normalized, &site)?;
                }
                if !Arc::ptr_eq(column, &normalized) {
                    *columns
                        .to_mut()
                        .get_mut(index)
                        .expect("index enumerates the original child arrays") = normalized;
                }
            }
            if range_metadata(field).is_some() {
                validate_range_values(&columns, site)?;
            }
            if column.data_type() == field.data_type() && matches!(columns, Cow::Borrowed(_)) {
                return Ok(column.clone());
            }
            // Keep every source child so mismatched field counts fail instead of dropping data.
            StructArray::try_new(fields.clone(), columns.into_owned(), array.nulls().cloned())
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|source| {
                    error(
                        format!(
                            "BigQuery ingest struct field {:?} is invalid: {source}; the bound \
                             column needs one child array per struct field",
                            site.name()
                        ),
                        Status::InvalidData,
                    )
                })
        }
        (DataType::List(_), DataType::List(item)) => {
            let array = downcast_array::<ListArray>(column, site.name())?;
            normalize_list_column(column, array, item, site)
        }
        (DataType::LargeList(_), DataType::List(item)) => {
            let array = downcast_array::<LargeListArray>(column, site.name())?;
            normalize_noncanonical_list_column(array.values(), array.len(), item, site, |index| {
                offset_value_range(array, index)
            })
        }
        (DataType::ListView(_), DataType::List(item)) => {
            let array = downcast_array::<ListViewArray>(column, site.name())?;
            normalize_list_view_column(array, item, site)
        }
        (DataType::LargeListView(_), DataType::List(item)) => {
            let array = downcast_array::<LargeListViewArray>(column, site.name())?;
            normalize_list_view_column(array, item, site)
        }
        (DataType::FixedSizeList(_, size), DataType::List(item)) => {
            let array = downcast_array::<FixedSizeListArray>(column, site.name())?;
            let size = size.as_usize();
            normalize_noncanonical_list_column(array.values(), array.len(), item, site, |index| {
                array
                    .is_valid(index)
                    .then(|| (index * size, (index + 1) * size))
            })
        }
        (DataType::Duration(unit), DataType::Interval(IntervalUnit::MonthDayNano)) => {
            encode_duration_column(column, *unit, site)
        }
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => truncate_interval_precision(column, site),
        (source, _) if decimal::needs_rescale(source, field) => {
            validate_scalar_values(column, site)?;
            decimal::rescale_decimal_column(column, field, site)
        }
        _ => {
            validate_scalar_values(column, site)?;
            let column = if column.data_type() == field.data_type() {
                column.clone()
            } else {
                cast_column(column, field.data_type(), site.name())?
            };
            validate_max_length(&column, field, site)?;
            Ok(column)
        }
    }
}

fn normalized_array_type_error(name: &str, actual: &DataType) -> adbc_core::error::Error {
    driver_defect(format_args!(
        "normalized BigQuery ingest field {name:?} has unexpected Arrow array type {actual}"
    ))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod struct_tests;
