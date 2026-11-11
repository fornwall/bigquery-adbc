//! Parameter conversion tests, split by the module under test.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::{Float16Type, Int8Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Date64Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, NullArray, RecordBatch, StringArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow_buffer::{
    IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer, ScalarBuffer, i256,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use serde_json::json;

use crate::rest::model::{QueryParameter, QueryParameterType, QueryParameterValue};

use super::{
    ParameterMode, ParameterPlan, explain_untyped_null, parameter_schema, parameter_type,
    parameters_for_schema,
};
use crate::ingest::schema::arrow_schema_to_bigquery;
use crate::test_support::arrow::{bigquery_typed, range_bounds};
use crate::test_support::errors::{assert_failure, assert_message};
use crate::types::{BigQueryType, MAX_SCHEMA_NESTING_DEPTH};

/// The parameters of one row of a batch bound on its own.
fn parameters_for_row(
    batch: &RecordBatch,
    row: usize,
    mode: ParameterMode,
) -> adbc_core::error::Result<Vec<QueryParameter>> {
    ParameterPlan::try_new(batch.schema().as_ref(), mode)?.row(batch, row, row)
}

fn value(parameter: &QueryParameter) -> &QueryParameterValue {
    parameter.parameter_value.as_ref().unwrap()
}

fn ptype(parameter: &QueryParameter) -> &QueryParameterType {
    parameter.parameter_type.as_ref().unwrap()
}

fn value_of(parameter: &QueryParameter) -> Option<&str> {
    value(parameter).value.as_deref()
}

/// `half::f16`, reached through arrow rather than through a dev-dependency on `half`.
type F16 = <Float16Type as ArrowPrimitiveType>::Native;

fn values(parameters: &[QueryParameter]) -> Vec<Option<&str>> {
    parameters.iter().map(value_of).collect()
}

fn annotated(data_type: DataType, rich_type: &str) -> Field {
    bigquery_typed(Field::new("value", data_type, true), rich_type)
}

fn annotated_column(name: &str, array: &dyn Array, rich_type: &str) -> Field {
    bigquery_typed(Field::new(name, array.data_type().clone(), true), rich_type)
}

fn decimal128(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn decimal256(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(vec![i256::from_i128(value)])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn one_column_batch(array: ArrayRef) -> RecordBatch {
    let field = Field::new("value", array.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

fn one_parameter(array: ArrayRef) -> QueryParameter {
    parameters_for_row(&one_column_batch(array), 0, ParameterMode::Positional)
        .unwrap()
        .swap_remove(0)
}

fn one_parameter_error(array: ArrayRef) -> String {
    let error =
        parameters_for_row(&one_column_batch(array), 0, ParameterMode::Positional).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    error.message
}

fn col(array: impl Array + 'static) -> ArrayRef {
    Arc::new(array)
}

/// A one-row list of `values`, whose items are `item`.
fn list_of(item: Field, values: ArrayRef) -> ListArray {
    let lengths = [values.len()];
    ListArray::new(
        Arc::new(item),
        OffsetBuffer::from_lengths(lengths),
        values,
        None,
    )
}

/// One `RANGE<DATE>` item, as a list of ranges holds it: its field and its struct array.
fn date_range_item() -> (Field, ArrayRef) {
    let bounds = range_bounds(DataType::Date32);
    let item = Field::new("item", DataType::Struct(bounds.clone()), true);
    let range = StructArray::new(
        bounds,
        vec![
            col(Date32Array::from(vec![0])),
            col(Date32Array::from(vec![2])),
        ],
        None,
    );
    (item, Arc::new(range))
}

/// The element type of an ARRAY parameter type.
fn element(parameter_type: &QueryParameterType) -> &QueryParameterType {
    parameter_type.array_type.as_ref().unwrap()
}

mod inferred;
mod plan;
mod types;
mod values;
