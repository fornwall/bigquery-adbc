//! Ingest schema tests, split by subject.
//!
//! Shared imports and fixtures live here; each submodule holds one subject.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::{DurationSecondType, Int8Type, Int64Type, Time64NanosecondType};
use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeListArray, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, RecordBatch, StringArray, StructArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{
    IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer, ScalarBuffer, i256,
};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};

use crate::BIGQUERY_FIELD_METADATA_MAX_LENGTH;
use crate::ingest::normalize::normalize_batch;
use crate::rest::metadata::TableId;
use crate::rest::model::{FieldElementType, TableFieldSchema, TableSchema};
use crate::test_support::arrow::{bigquery_typed, range_bounds, typed as array_as};
use crate::test_support::schema::{decimal, field, field_mode, record, table_schema};
use crate::types::{BIGQUERY_TYPE_METADATA_KEY, MAX_SCHEMA_NESTING_DEPTH};

use super::{
    arrow_schema_to_bigquery, canonical_mode, create_or_replace_table_ddl, normalize_schema,
    schemas_match, storage_write_schema, validate_existing_schema, validate_ingestible_target,
};
use crate::test_support::errors::{assert_failure, assert_message};

mod comparison;
mod conversion;
mod ddl;
mod decimals;
mod metadata;
mod ranges;
mod temporals;
mod values;

/// A one-column schema carrying one array's own Arrow type.
fn one_column_schema(name: &str, data_type: &DataType, nullable: bool) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        name,
        data_type.clone(),
        nullable,
    )]))
}

/// One column of a batch, viewed as the Arrow array type it must have.
fn column_as<T: Array + 'static>(batch: &RecordBatch, index: usize) -> &T {
    array_as(batch.column(index))
}

/// A `RANGE` column over `element`, the one shape that carries a range element type.
fn range(name: &str, element: &str) -> TableFieldSchema {
    TableFieldSchema {
        range_element_type: Some(FieldElementType {
            r#type: element.to_owned(),
        }),
        ..field(name, "RANGE")
    }
}
