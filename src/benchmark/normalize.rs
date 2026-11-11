//! Synthetic ingest batches for measuring Arrow normalization.

use std::sync::Arc;

use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, DictionaryArray,
    Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray,
    LargeStringArray, ListArray, RecordBatch, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{IntervalMonthDayNano, NullBuffer, OffsetBuffer, i256};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Whether normalization validates canonical arrays or coerces noncanonical Arrow types.
#[derive(Clone, Copy, Debug)]
pub enum NormalizeKind {
    /// The batch already has the target schema but contains types that require value validation.
    Canonical,
    /// DATE and TIMESTAMP columns requiring calendar range validation.
    Calendar,
    /// Nested structs containing canonical DATE and TIMESTAMP values.
    Nested,
    /// The batch contains narrower or large-offset arrays that must be cast to the target schema.
    Coercing,
    /// Sliced large-offset STRING and BYTES arrays narrow offsets while sharing their payloads.
    LargeOffsets,
    /// Sliced lists hide child values behind null entries and require gathering.
    ListGather,
    /// Sliced canonical lists validate only their addressed DATE or TIMESTAMP values.
    ListWindow,
    /// Dictionary keys contain ascending runs separated by null runs.
    DictionaryRuns,
    /// Scattered dictionary keys with four nulls per 32 rows.
    DictionaryScattered,
    /// Scattered dictionary keys alternating with null rows.
    DictionaryAlternating,
    /// A first column of 200-byte dictionary labels holds nearly every byte, beside BOOLEAN
    /// columns: the first request window is sized by walking its keys.
    DictionaryLabels,
    /// Decimal columns already at their wire scale declare a narrower precision.
    DecimalPrecision,
    /// Decimal columns already at their wire scale declare fewer fractional digits.
    DecimalScale,
}

/// The column count of a synthetic normalization batch.
#[derive(Clone, Copy, Debug)]
pub enum NormalizeWidth {
    /// Two columns.
    Narrow,
    /// Sixteen columns.
    Wide,
}

/// A reusable input batch and target schema for measuring ingest normalization.
#[derive(Debug)]
pub struct NormalizeFixture {
    batch: RecordBatch,
    schema: SchemaRef,
}

impl NormalizeFixture {
    /// Creates a fixture without cloning its input batch for any timed iteration.
    pub fn new(row_count: usize, kind: NormalizeKind, width: NormalizeWidth) -> Self {
        let column_count = match width {
            NormalizeWidth::Narrow => 2,
            NormalizeWidth::Wide => 16,
        };
        let (columns, fields): (Vec<_>, Vec<_>) = (0..column_count)
            .map(|index| match kind {
                NormalizeKind::Canonical => canonical_normalize_column(index, row_count),
                NormalizeKind::Calendar => calendar_normalize_column(index, row_count),
                NormalizeKind::Nested => nested_normalize_column(index, row_count),
                NormalizeKind::Coercing => coercing_normalize_column(index, row_count),
                NormalizeKind::LargeOffsets => large_offset_normalize_column(index, row_count),
                NormalizeKind::ListGather => list_gather_normalize_column(index, row_count),
                NormalizeKind::ListWindow => list_window_normalize_column(index, row_count),
                NormalizeKind::DictionaryRuns
                | NormalizeKind::DictionaryScattered
                | NormalizeKind::DictionaryAlternating => {
                    dictionary_normalize_column(index, row_count, kind)
                }
                NormalizeKind::DictionaryLabels => dictionary_labels_column(index, row_count),
                NormalizeKind::DecimalPrecision | NormalizeKind::DecimalScale => {
                    decimal_normalize_column(index, row_count, kind)
                }
            })
            .unzip();
        let schema = Arc::new(Schema::new(fields));
        let input_schema = Arc::new(Schema::new(
            columns
                .iter()
                .zip(schema.fields())
                .map(|(column, field)| {
                    Field::new(
                        field.name(),
                        column.data_type().clone(),
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(input_schema, columns)
            .expect("the static normalization benchmark batch is valid");
        Self { batch, schema }
    }

    /// Clones only the batch's Arrow references, outside the timed region.
    pub fn prepare_input(&self) -> RecordBatch {
        self.batch.clone()
    }

    /// Normalizes one prepared input batch.
    pub fn normalize(&self, batch: RecordBatch) -> adbc_core::error::Result<RecordBatch> {
        crate::ingest::normalize::normalize_batch(batch, self.schema.clone())
    }

    /// Encodes one prepared input batch into Storage Write requests: sizes each request's window
    /// of rows, normalizes it and frames it as Arrow IPC. Returns the bytes framed.
    pub fn encode(&self, batch: &RecordBatch) -> adbc_core::error::Result<usize> {
        crate::ingest::encode_for_benchmark(self.schema.clone(), batch)
    }
}

fn decimal_normalize_column(
    index: usize,
    row_count: usize,
    kind: NormalizeKind,
) -> (ArrayRef, Field) {
    let scale = if index.is_multiple_of(2) { 9 } else { 38 };
    let declared_scale = if matches!(kind, NormalizeKind::DecimalScale) {
        2
    } else {
        scale
    };
    let column: ArrayRef = if index.is_multiple_of(2) {
        Arc::new(
            Decimal128Array::from_iter(
                (0..row_count + 2).map(|row| (row % 16 != 1).then_some(1_230_000_000_i128)),
            )
            .with_precision_and_scale(38, scale)
            .expect("the benchmark NUMERIC shape is valid")
            .slice(1, row_count),
        )
    } else {
        let value = i256::from_i128(123)
            * i256::from_i128(10)
                .checked_pow(36)
                .expect("the benchmark value fits BIGNUMERIC");
        Arc::new(
            Decimal256Array::from_iter(
                (0..row_count + 2).map(|row| (row % 16 != 1).then_some(value)),
            )
            .with_precision_and_scale(76, scale)
            .expect("the benchmark BIGNUMERIC shape is valid")
            .slice(1, row_count),
        )
    };
    let field = Field::new(format!("column_{index}"), column.data_type().clone(), true)
        .with_metadata(std::collections::HashMap::from([
            (
                crate::BIGQUERY_FIELD_METADATA_PRECISION.to_owned(),
                (declared_scale + 3).to_string(),
            ),
            (
                crate::BIGQUERY_FIELD_METADATA_SCALE.to_owned(),
                declared_scale.to_string(),
            ),
        ]));
    (column, field)
}

fn dictionary_normalize_column(
    index: usize,
    row_count: usize,
    kind: NormalizeKind,
) -> (ArrayRef, Field) {
    let values: ArrayRef = if index.is_multiple_of(2) {
        Arc::new(Int64Array::from_iter_values(0..256))
    } else {
        Arc::new(StringArray::from_iter_values(
            (0..256).map(|row| format!("dictionary value {row}")),
        ))
    };
    let field = Field::new(format!("column_{index}"), values.data_type().clone(), true);
    let keys = Int32Array::from_iter((0..row_count + 2).map(|row| {
        let valid = match kind {
            NormalizeKind::DictionaryAlternating => row.is_multiple_of(2),
            _ => row % 32 < 28,
        };
        valid.then(|| {
            let key = match kind {
                NormalizeKind::DictionaryRuns => row % 256,
                _ => row * 73 % 256,
            };
            i32::try_from(key).expect("benchmark keys fit i32")
        })
    }));
    let dictionary = DictionaryArray::<Int32Type>::new(keys, values).slice(1, row_count);
    (Arc::new(dictionary), field)
}

fn dictionary_labels_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let column: ArrayRef = if index == 0 {
        let labels =
            StringArray::from_iter_values((0..1_000).map(|label| format!("{label:0>200}")));
        let keys = Int16Array::from_iter_values((0..row_count).map(|row| {
            i16::try_from(row * 7 % 1_000).expect("the benchmark keys index 1,000 labels")
        }));
        Arc::new(DictionaryArray::<Int16Type>::new(keys, Arc::new(labels)))
    } else {
        Arc::new(BooleanArray::from_iter(
            (0..row_count).map(|row| Some(row.is_multiple_of(3))),
        ))
    };
    let data_type = match column.data_type() {
        DataType::Dictionary(_, values) => values.as_ref().clone(),
        data_type => data_type.clone(),
    };
    (
        column,
        Field::new(format!("column_{index}"), data_type, true),
    )
}

fn nested_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let (mut column, _) = calendar_normalize_column(index, row_count);
    for name in ["value", "inner", "outer"] {
        let field = Field::new(name, column.data_type().clone(), false);
        column = Arc::new(StructArray::new(vec![field].into(), vec![column], None));
    }
    let field = Field::new(format!("column_{index}"), column.data_type().clone(), false);
    (column, field)
}

fn list_gather_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let length = row_count + 2;
    let values: ArrayRef = if index.is_multiple_of(2) {
        Arc::new(Int64Array::from_iter_values((0..length * 2).map(|value| {
            i64::try_from(value).expect("benchmark values fit i64")
        })))
    } else {
        Arc::new(StringArray::from_iter_values(
            (0..length * 2).map(|_| "gather benchmark"),
        ))
    };
    let item = Arc::new(Field::new("item", values.data_type().clone(), false));
    let list = ListArray::new(
        item,
        OffsetBuffer::from_lengths(std::iter::repeat_n(2, length)),
        values,
        Some(NullBuffer::from_iter((0..length).map(|row| row % 4 != 1))),
    )
    .slice(1, row_count);
    let field = Field::new(format!("column_{index}"), list.data_type().clone(), true);
    (Arc::new(list), field)
}

fn calendar_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let column: ArrayRef = if index.is_multiple_of(2) {
        Arc::new(Date32Array::from_iter_values((0..row_count).map(|row| {
            10_000 + i32::try_from(row % 10_000).expect("benchmark day offsets fit i32")
        })))
    } else {
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..row_count).map(|row| {
                946_684_800_000_000
                    + i64::try_from(row).expect("benchmark row counts fit i64") * 1_000_001
            }),
        ))
    };
    let field = Field::new(format!("column_{index}"), column.data_type().clone(), false);
    (column, field)
}

fn list_window_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let (values, _) = calendar_normalize_column(index, (row_count + 2) * 2);
    let item = Arc::new(Field::new("item", values.data_type().clone(), false));
    let list = ListArray::new(
        item,
        OffsetBuffer::from_lengths(std::iter::repeat_n(2, row_count + 2)),
        values,
        None,
    )
    .slice(1, row_count);
    let field = Field::new(format!("column_{index}"), list.data_type().clone(), false);
    (Arc::new(list), field)
}

fn canonical_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let column: ArrayRef = match index % 4 {
        0 => Arc::new(Time64MicrosecondArray::from(vec![3_723_000_004; row_count])),
        1 => Arc::new(
            Decimal128Array::from(vec![12_345_i128; row_count])
                .with_precision_and_scale(10, 2)
                .expect("the static decimal shape is valid"),
        ),
        2 => Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3_000);
            row_count
        ])),
        _ => Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..row_count).map(|row| {
                Some(vec![Some(
                    i64::try_from(row).expect("benchmark row counts fit i64"),
                )])
            }),
        )),
    };
    let field = Field::new(format!("column_{index}"), column.data_type().clone(), false);
    (column, field)
}

fn coercing_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    if index.is_multiple_of(2) {
        let column =
            Arc::new(Int32Array::from_iter_values((0..row_count).map(|row| {
                i32::try_from(row).expect("benchmark row counts fit i32")
            }))) as ArrayRef;
        (
            column,
            Field::new(format!("column_{index}"), DataType::Int64, false),
        )
    } else {
        let column = Arc::new(LargeStringArray::from_iter_values(
            (0..row_count).map(|_| "normalize benchmark"),
        )) as ArrayRef;
        (
            column,
            Field::new(format!("column_{index}"), DataType::Utf8, false),
        )
    }
}

fn large_offset_normalize_column(index: usize, row_count: usize) -> (ArrayRef, Field) {
    let value = |row: usize| match row % 4 {
        0 => Some("large offset benchmark"),
        1 => None,
        2 => Some(""),
        _ => Some("non-ASCII: é中"),
    };
    let (column, data_type): (ArrayRef, _) = if index.is_multiple_of(2) {
        (
            Arc::new(
                LargeStringArray::from_iter((0..row_count + 2).map(value)).slice(1, row_count),
            ),
            DataType::Utf8,
        )
    } else {
        (
            Arc::new(
                LargeBinaryArray::from_iter(
                    (0..row_count + 2).map(|row| value(row).map(str::as_bytes)),
                )
                .slice(1, row_count),
            ),
            DataType::Binary,
        )
    };
    (
        column,
        Field::new(format!("column_{index}"), data_type, true),
    )
}
