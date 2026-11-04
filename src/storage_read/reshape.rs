//! Convert Storage Read batches to the canonical REST result schema.
//!
//! Plan once per session and reuse the plan for each batch. Incompatible schemas fall back to
//! REST before exposing a reader; identical schemas reuse the batch's columns vector.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, IntervalUnit, SchemaRef, TimeUnit};

/// A column conversion fixed for the lifetime of a read session.
#[derive(Debug)]
pub(super) enum ColumnPlan {
    /// The column already has the canonical type; the batch keeps the array itself.
    Identity,
    /// A list rebuilt with the canonical item field around a reshaped child.
    List(Box<Self>, FieldRef),
    /// A struct rebuilt with the canonical fields around reshaped children.
    Struct(Vec<Self>, Fields),
    /// A leaf the cast kernels turn into the canonical type.
    Cast(DataType),
}

/// The reshape for a complete batch, including the identity decision made at session setup.
#[derive(Debug)]
pub(super) struct ReshapePlan {
    columns: Box<[ColumnPlan]>,
    identity: bool,
}

/// Rebuilds a decoded batch as the canonical schema the REST path would have produced.
pub(super) fn reshape(
    batch: RecordBatch,
    schema: &SchemaRef,
    plan: &ReshapePlan,
) -> Result<RecordBatch, ArrowError> {
    // The decoder and plan both use the session schema.
    debug_assert_eq!(
        batch.num_columns(),
        plan.columns.len(),
        "a decoded batch has one column per session field, and the plan one per session field"
    );
    if plan.identity {
        return batch.with_schema(schema.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(&plan.columns)
        .zip(schema.fields())
        .map(|((column, plan), field)| reshape_column(column, plan, field.name()))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns)
}

/// `name` is the result column the value belongs to; nested reshapes keep reporting the
/// top-level column so a failure names something the caller can find in the schema.
fn reshape_column(
    column: &ArrayRef,
    plan: &ColumnPlan,
    name: &str,
) -> Result<ArrayRef, ArrowError> {
    match plan {
        ColumnPlan::Identity => Ok(column.clone()),
        ColumnPlan::List(item_plan, item) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_error("list"))?;
            let values = reshape_column(list.values(), item_plan, name)?;
            Ok(Arc::new(ListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        ColumnPlan::Struct(child_plans, fields) => {
            let structure = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_error("struct"))?;
            let children = structure
                .columns()
                .iter()
                .zip(child_plans)
                .map(|(child, plan)| reshape_column(child, plan, name))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                structure.nulls().cloned(),
            )?))
        }
        ColumnPlan::Cast(target) => crate::types::cast::cast_result_column(column, target, name),
    }
}

/// Plan the batch conversion, or report why the read must fall back to REST.
pub(super) fn reshape_plans(
    session_schema: &SchemaRef,
    schema: &SchemaRef,
) -> Result<ReshapePlan, String> {
    if session_schema.fields().len() != schema.fields().len() {
        return Err(format!(
            "the session has {} columns and the result has {}",
            session_schema.fields().len(),
            schema.fields().len()
        ));
    }
    let columns = session_schema
        .fields()
        .iter()
        .zip(schema.fields())
        .enumerate()
        .map(|(position, (source, target))| {
            if source.name() != target.name() {
                return Err(format!(
                    "column {position} arrives named {} and the result names it {}",
                    source.name(),
                    target.name()
                ));
            }
            field_plan(source, target).ok_or_else(|| {
                format!(
                    "column {} arrives as {} (nullable: {}) and cannot become {} (nullable: {})",
                    target.name(),
                    source.data_type(),
                    source.is_nullable(),
                    target.data_type(),
                    target.is_nullable()
                )
            })
        })
        .collect::<Result<Box<[_]>, _>>()?;
    // `with_schema` reuses the columns vector but requires a schema superset, including metadata.
    let identity = columns
        .iter()
        .all(|plan| matches!(plan, ColumnPlan::Identity))
        && schema.contains(session_schema);
    Ok(ReshapePlan { columns, identity })
}

/// Accept only lossless conversions supported by the result cast kernels.
fn column_plan(source: &DataType, target: &DataType) -> Option<ColumnPlan> {
    if source == target {
        return Some(ColumnPlan::Identity);
    }
    match (source, target) {
        (DataType::List(source_item), DataType::List(target_item)) => {
            let item = field_plan(source_item, target_item)?;
            Some(ColumnPlan::List(Box::new(item), target_item.clone()))
        }
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            if source_fields.len() != target_fields.len() {
                return None;
            }
            let children = source_fields
                .iter()
                .zip(target_fields)
                .map(|(source, target)| field_plan(source, target))
                .collect::<Option<Vec<_>>>()?;
            Some(ColumnPlan::Struct(children, target_fields.clone()))
        }
        // Scale widening is exact if the extra digits fit the target precision. Query sessions
        // currently report canonical decimal types; this also accepts stored-column parameters.
        (
            DataType::Decimal128(source_precision, source_scale),
            DataType::Decimal128(target_precision, target_scale)
            | DataType::Decimal256(target_precision, target_scale),
        )
        | (
            DataType::Decimal256(source_precision, source_scale),
            DataType::Decimal256(target_precision, target_scale),
        ) => {
            let added_scale = i16::from(*target_scale) - i16::from(*source_scale);
            let widened = i16::from(*source_precision) + added_scale;
            (added_scale >= 0 && widened <= i16::from(*target_precision))
                .then(|| ColumnPlan::Cast(target.clone()))
        }
        // Both spellings are the same UTC instant; only the zone label differs. A zoned and an
        // unzoned timestamp are TIMESTAMP and DATETIME, which must never be reconciled.
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
        )
        | (
            DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Some(ColumnPlan::Cast(target.clone())),
        _ => None,
    }
}

/// Check names and nullability before adopting any canonical field. Tightening nullability
/// could fail on a later batch, when REST fallback is no longer possible.
fn field_plan(source: &FieldRef, target: &FieldRef) -> Option<ColumnPlan> {
    if source.name() != target.name() || (source.is_nullable() && !target.is_nullable()) {
        return None;
    }
    column_plan(source.data_type(), target.data_type())
}

fn downcast_error(kind: &str) -> ArrowError {
    ArrowError::CastError(format!(
        "BigQuery Storage Read returned a value that is not a {kind} array"
    ))
}

#[cfg(test)]
mod tests;
