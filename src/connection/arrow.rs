//! Arrow builders for ADBC metadata lists, structs, and dense unions.

use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray, StructArray, UnionArray, new_empty_array,
};
use arrow_buffer::{NullBufferBuilder, OffsetBufferBuilder, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, SchemaRef, UnionMode};

/// Appends `value` to a dense union member, returning the offset it landed at.
pub(super) fn union_offset<T>(values: &mut Vec<T>, value: T) -> i32 {
    let offset = values.len();
    values.push(value);
    i32::try_from(offset).expect("metadata value counts fit i32 union offsets")
}

/// Builds a dense union from an `adbc_core::schemas` constant. Members absent from `member`
/// get empty arrays of their declared types. Both ADBC metadata unions are dense.
pub(super) fn dense_union(
    data_type: &DataType,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    member: &mut dyn FnMut(i8) -> Option<ArrayRef>,
) -> Result<UnionArray> {
    let DataType::Union(union_fields, UnionMode::Dense) = data_type else {
        unreachable!("ADBC metadata values use a dense union")
    };
    let children = union_fields
        .iter()
        .map(|(type_id, field)| {
            member(type_id).unwrap_or_else(|| new_empty_array(field.data_type()))
        })
        .collect::<Vec<_>>();
    Ok(UnionArray::try_new(
        union_fields.clone(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    )?)
}

/// Flattens each parent's children into a list column. Absent collections become null lists.
pub(super) fn nested_list_array<'a, P, C: 'a>(
    parents: &'a [P],
    children: impl Fn(&'a P) -> Option<&'a [C]>,
    item_type: &DataType,
    build: impl FnOnce(Vec<&'a C>) -> Result<ArrayRef>,
) -> Result<ListArray> {
    let child_count = parents
        .iter()
        .map(|parent| children(parent).map_or(0, <[C]>::len))
        .sum();
    let mut lengths = Vec::with_capacity(parents.len());
    let mut flattened = Vec::with_capacity(child_count);
    for parent in parents {
        let items = children(parent);
        lengths.push(items.map(<[C]>::len));
        if let Some(items) = items {
            flattened.extend(items);
        }
    }
    list_array(item_type, &lengths, build(flattened)?)
}

/// Builds the catalog-name and nested-schema columns for `GetObjects` or `GetStatistics`.
pub(super) fn catalog_batch<'a, C, S: 'a>(
    schema: &SchemaRef,
    catalogs: &'a [C],
    name: impl Fn(&'a C) -> &'a str,
    db_schemas: impl Fn(&'a C) -> Option<&'a [S]>,
    db_schema_type: &DataType,
    build: impl FnOnce(Vec<&'a S>) -> Result<ArrayRef>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let names = string_column(&mut catalogs.iter().map(name));
    let nested = nested_list_array(catalogs, db_schemas, db_schema_type, build)?;
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(names), Arc::new(nested)])?;
    Ok(single_batch_reader(batch))
}

/// Builds a struct containing a name and a nested list, used for each database schema.
pub(super) fn nested_struct<'a, P, C: 'a>(
    data_type: &DataType,
    parents: &'a [P],
    name: impl Fn(&'a P) -> &'a str,
    children: impl Fn(&'a P) -> Option<&'a [C]>,
    item_type: &DataType,
    build: impl FnOnce(Vec<&'a C>) -> Result<ArrayRef>,
) -> Result<StructArray> {
    let names = string_column(&mut parents.iter().map(name));
    let nested = nested_list_array(parents, children, item_type, build)?;
    Ok(StructArray::try_new(
        struct_fields(data_type),
        vec![Arc::new(names), Arc::new(nested)],
        None,
    )?)
}

/// Keeps list validation outside the generic builder to avoid duplicating it per metadata
/// level. `None` creates a null list; `Some(0)` creates an empty one.
fn list_array(
    item_type: &DataType,
    lengths: &[Option<usize>],
    values: ArrayRef,
) -> Result<ListArray> {
    let mut offsets = OffsetBufferBuilder::<i32>::new(lengths.len());
    let mut nulls = NullBufferBuilder::new(lengths.len());
    for length in lengths {
        offsets.push_length(length.unwrap_or_default());
        nulls.append(length.is_some());
    }
    Ok(ListArray::try_new(
        Arc::new(Field::new("item", item_type.clone(), true)),
        offsets.finish(),
        values,
        nulls.finish(),
    )?)
}

/// Erased iterators and `inline(never)` keep one copy of each Arrow builder instead of one
/// per column closure. These run once per metadata call; see BINARY-SIZE.md for measurements.
#[inline(never)]
pub(super) fn string_column(values: &mut dyn Iterator<Item = &str>) -> StringArray {
    StringArray::from_iter_values(values)
}

/// The nullable counterpart of [`string_column`].
#[inline(never)]
pub(super) fn optional_string_column(
    values: &mut dyn Iterator<Item = Option<&str>>,
) -> StringArray {
    values.collect()
}

#[inline(never)]
pub(super) fn int16_column(values: &mut dyn Iterator<Item = i16>) -> Int16Array {
    Int16Array::from_iter_values(values)
}

/// The nullable counterpart of [`int16_column`].
#[inline(never)]
pub(super) fn optional_int16_column(values: &mut dyn Iterator<Item = Option<i16>>) -> Int16Array {
    values.collect()
}

#[inline(never)]
pub(super) fn int32_column(values: &mut dyn Iterator<Item = i32>) -> Int32Array {
    Int32Array::from_iter_values(values)
}

/// The nullable counterpart of [`int32_column`].
#[inline(never)]
pub(super) fn optional_int32_column(values: &mut dyn Iterator<Item = Option<i32>>) -> Int32Array {
    values.collect()
}

#[inline(never)]
pub(super) fn boolean_column(values: &mut dyn Iterator<Item = bool>) -> BooleanArray {
    values.collect()
}

/// The fields of an ADBC nested metadata struct type.
pub(super) fn struct_fields(data_type: &DataType) -> Fields {
    let DataType::Struct(fields) = data_type else {
        unreachable!("ADBC nested metadata types are structs")
    };
    fields.clone()
}

/// A reader over exactly `batch`.
pub(super) fn single_batch_reader(
    batch: RecordBatch,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(std::iter::once(Ok(batch)), schema))
}
