//! `decode_record_batch_message` over the body of a BigQuery `ReadRowsResponse`.
//!
//! Input: a field-count byte, one type byte per field, then the record-batch body. Building the
//! schema from bytes lets the fuzzer reach batch decoding without a valid schema message.
#![no_main]

use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use libfuzzer_sys::fuzz_target;

const MAX_FIELDS: u8 = 5;
const MAX_NESTING: u8 = 3;

/// Builds one of the Arrow types the Storage Read decoder accepts from a fuzzer byte.
fn data_type(code: u8, depth: u8) -> DataType {
    match code % 16 {
        0 => DataType::Utf8,
        1 => DataType::Binary,
        2 => DataType::Boolean,
        3 => DataType::Int64,
        4 => DataType::Float64,
        5 => DataType::Date32,
        6 => DataType::Time64(TimeUnit::Microsecond),
        7 => DataType::Timestamp(TimeUnit::Microsecond, None),
        8 => DataType::Decimal128(38, 9),
        9 => DataType::Decimal256(76, 38),
        // 4-, 8-, and 16-byte elements.
        10 => DataType::Interval(IntervalUnit::YearMonth),
        11 => DataType::Interval(IntervalUnit::DayTime),
        12 => DataType::Interval(IntervalUnit::MonthDayNano),
        13 if depth < MAX_NESTING => DataType::List(Arc::new(Field::new(
            "item",
            data_type(code / 16 + 1, depth + 1),
            true,
        ))),
        14 if depth < MAX_NESTING => DataType::Struct(
            vec![
                Field::new("a", data_type(code / 16 + 2, depth + 1), true),
                Field::new("b", data_type(code / 16 + 3, depth + 1), false),
            ]
            .into(),
        ),
        _ => DataType::Int64,
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&width, rest)) = data.split_first() else {
        return;
    };
    let field_count = usize::from(width % MAX_FIELDS);
    if rest.len() < field_count {
        return;
    }
    let (codes, body) = rest.split_at(field_count);
    let fields: Vec<Field> = codes
        .iter()
        .enumerate()
        .map(|(index, &code)| Field::new(format!("f{index}"), data_type(code, 0), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let _ = adbc_driver_bigquery::fuzz_shim::ipc_decode_record_batch(body, schema);
});
