use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::types::{
    ArrowPrimitiveType, TimestampMicrosecondType, TimestampMillisecondType, TimestampSecondType,
};
use arrow_array::{ArrayRef, Date32Array, PrimitiveArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};

use super::super::site::ValueSite;
use super::validate_scalar_values;
use crate::test_support::errors::{assert_message, assert_message_omits};

fn check_timestamp_boundaries<T: ArrowPrimitiveType<Native = i64>>(per_second: i64) {
    let minimum = -62_135_596_800 * per_second;
    let maximum = 253_402_300_800 * per_second - 1;
    let site = ValueSite::column("event_time", 0);
    let column: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::from(vec![i64::MIN, minimum, -1, 0, maximum, i64::MAX]),
        Some(NullBuffer::from(vec![false, true, true, true, true, false])),
    ));
    validate_scalar_values(&column, &site).unwrap();
    validate_scalar_values(&column.slice(1, 4), &site).unwrap();

    for value in [i64::MIN, minimum - 1, maximum + 1, i64::MAX] {
        let column: ArrayRef = Arc::new(PrimitiveArray::<T>::from_iter_values([minimum, value]));
        let error = validate_scalar_values(&column.slice(1, 1), &site).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert_eq!(
            error.message,
            "Arrow timestamp ingest field \"event_time\" contains an out-of-range value \
             at row 0; BigQuery supports years 1 to 9999"
        );
        assert_message_omits(&error, &[&value.to_string()]);
    }
}

#[test]
fn timestamp_calendar_boundaries_cover_each_unit_and_ignore_null_values() {
    check_timestamp_boundaries::<TimestampSecondType>(1);
    check_timestamp_boundaries::<TimestampMillisecondType>(1_000);
    check_timestamp_boundaries::<TimestampMicrosecondType>(1_000_000);
}

#[test]
fn date_calendar_boundaries_and_extreme_null_values() {
    let site = ValueSite::column("day", 0);
    let column: ArrayRef = Arc::new(Date32Array::new(
        ScalarBuffer::from(vec![i32::MIN, -719_162, 2_932_896, i32::MAX]),
        Some(NullBuffer::from(vec![false, true, true, false])),
    ));
    validate_scalar_values(&column, &site).unwrap();
    for value in [i32::MIN, -719_163, 2_932_897, i32::MAX] {
        let column: ArrayRef = Arc::new(Date32Array::from(vec![value]));
        let error = validate_scalar_values(&column, &site).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert_message(&error, &["\"day\"", "row 0", "years 1 to 9999"]);
        assert_message_omits(&error, &[&value.to_string()]);
    }
}
