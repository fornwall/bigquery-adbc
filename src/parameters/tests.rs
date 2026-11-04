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

use crate::rest::model::{
    QueryParameter, QueryParameterStructType, QueryParameterType, QueryParameterValue,
};

use super::{
    ParameterMode, ParameterPlan, parameter_schema, parameter_type, parameters_for_schema,
};
use crate::ingest::schema::arrow_schema_to_bigquery;
use crate::test_support::arrow::{bigquery_typed, range_bounds};
use crate::test_support::errors::assert_message;
use crate::types::{BIGQUERY_TYPE_METADATA_KEY, BigQueryType, MAX_SCHEMA_NESTING_DEPTH};

/// The parameters of one row of a batch bound on its own.
fn parameters_for_row(
    batch: &RecordBatch,
    row: usize,
    mode: ParameterMode,
) -> adbc_core::error::Result<Vec<QueryParameter>> {
    ParameterPlan::try_new(batch.schema().as_ref(), mode)?.row(batch, row, row)
}

/// The value payload every built parameter carries, present even for SQL NULL.
fn value(parameter: &QueryParameter) -> &QueryParameterValue {
    parameter.parameter_value.as_ref().unwrap()
}

/// The type payload every built parameter carries.
fn ptype(parameter: &QueryParameter) -> &QueryParameterType {
    parameter.parameter_type.as_ref().unwrap()
}

/// One built parameter's scalar value.
fn value_of(parameter: &QueryParameter) -> Option<&str> {
    value(parameter).value.as_deref()
}

/// A parameter type that is no more than a GoogleSQL type name.
fn scalar_type(kind: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: kind.to_owned(),
        ..Default::default()
    }
}

/// A parameter BigQuery described by type alone; an empty name is a positional parameter.
fn param(name: &str, parameter_type: QueryParameterType) -> QueryParameter {
    QueryParameter {
        name: name.to_owned(),
        parameter_type: Some(parameter_type),
        ..Default::default()
    }
}

/// `half::f16`, reached through arrow rather than through a dev-dependency on `half`.
type F16 = <Float16Type as ArrowPrimitiveType>::Native;

/// Every parameter's scalar value, in parameter order.
fn values(parameters: &[QueryParameter]) -> Vec<Option<&str>> {
    parameters.iter().map(value_of).collect()
}

/// A nullable `value` field of `data_type`, annotated as `rich_type`.
fn annotated(data_type: DataType, rich_type: &str) -> Field {
    bigquery_typed(Field::new("value", data_type, true), rich_type)
}

/// A nullable field named `name` of `array`'s type, annotated as `rich_type`.
fn annotated_column(name: &str, array: &dyn Array, rich_type: &str) -> Field {
    bigquery_typed(Field::new(name, array.data_type().clone(), true), rich_type)
}

/// A one-row `Decimal128Array` column.
fn decimal128(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// A one-row `Decimal256Array` column.
fn decimal256(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(vec![i256::from_i128(value)])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// A one-column batch, the shape almost every value conversion is exercised through.
fn one_column_batch(array: ArrayRef) -> RecordBatch {
    let field = Field::new("value", array.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

/// The single converted parameter of a one-column batch.
fn one_parameter(array: ArrayRef) -> QueryParameter {
    parameters_for_row(&one_column_batch(array), 0, ParameterMode::Positional)
        .unwrap()
        .swap_remove(0)
}

/// The driver's rejection of a one-column batch whose value it must refuse.
fn one_parameter_error(array: ArrayRef) -> String {
    let error =
        parameters_for_row(&one_column_batch(array), 0, ParameterMode::Positional).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    error.message
}

/// One parameter column.
fn col(array: impl Array + 'static) -> ArrayRef {
    Arc::new(array)
}

mod inferred;
mod plan;
mod types;
mod values;
