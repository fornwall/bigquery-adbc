//! Encoding normalized values the way BigQuery's ingest wire format wants them.
//!
//! Three rewrites that Arrow itself has no notion of: a duration becomes the month-day-nano
//! interval BigQuery reads as `INTERVAL`, an interval is truncated to BigQuery's microsecond
//! precision, and a `RANGE<DATETIME>` bound is bit-packed into the single integer the Storage
//! Write API expects. Each one changes the values, never the row count or the null mask.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::builder::{IntervalMonthDayNanoBuilder, TimestampMicrosecondBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, IntervalMonthDayNanoType,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray, TimestampMicrosecondArray};
use arrow_schema::TimeUnit;

use super::downcast_array;
use super::site::ValueSite;
use super::validate::interval_out_of_range;
use crate::error::error;
use crate::types::temporal::{
    MICROSECONDS_PER_DAY, check_interval_micros, check_interval_months_days, civil_from_days,
    truncate_to_microsecond_precision,
};

pub(super) fn encode_duration_column(
    column: &ArrayRef,
    unit: TimeUnit,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    match unit {
        TimeUnit::Second => {
            encode_duration_values::<DurationSecondType>(column, 1_000_000_000, site)
        }
        TimeUnit::Millisecond => {
            encode_duration_values::<DurationMillisecondType>(column, 1_000_000, site)
        }
        TimeUnit::Microsecond => {
            encode_duration_values::<DurationMicrosecondType>(column, 1_000, site)
        }
        TimeUnit::Nanosecond => encode_duration_values::<DurationNanosecondType>(column, 1, site),
    }
}

fn encode_duration_values<T>(
    column: &ArrayRef,
    scale: i128,
    site: &ValueSite<'_>,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let array = downcast_array::<PrimitiveArray<T>>(column, site.name())?;
    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(array.len());
    for (index, value) in array.iter().enumerate() {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        let Some(nanoseconds) = duration_interval_nanoseconds(i128::from(value) * scale) else {
            let (name, row) = (site.name(), site.row(index));
            return Err(error(
                format!(
                    "Arrow duration ingest field {name:?} contains an out-of-range value \
                     at row {row}; the maximum is {MAX_DURATION_HOURS} hours"
                ),
                Status::InvalidData,
            ));
        };
        builder.append_value(arrow_buffer::IntervalMonthDayNano::new(0, 0, nanoseconds));
    }
    Ok(Arc::new(builder.finish()))
}

/// The whole hours of the longest duration an ingested INTERVAL carries, for the message that
/// refuses a longer one: `i64::MAX` nanoseconds.
const MAX_DURATION_HOURS: i64 = i64::MAX / (3_600 * 1_000_000_000);

/// The nanosecond component of the INTERVAL an Arrow duration of `nanoseconds` becomes, or `None`
/// when no ingested INTERVAL carries that span.
///
/// The whole span goes in the time component. A duration is exact elapsed time, and BigQuery
/// spells it that way too: `INTERVAL 172800 SECOND` and a TIMESTAMP difference both report 48 for
/// `EXTRACT(HOUR)` and 0 for `EXTRACT(DAY)`. This also matches `format_interval`
/// (`src/parameters/values.rs`) for the same Arrow value bound as a parameter.
///
/// The span is therefore bounded by the `i64` nanoseconds of a month-day-nano interval
/// ([`MAX_DURATION_HOURS`], narrower than BigQuery's +/-10,000 years); a longer duration is refused
/// rather than respelled in days.
fn duration_interval_nanoseconds(nanoseconds: i128) -> Option<i64> {
    // Truncating towards zero first keeps the sub-microsecond policy identical for the durations
    // that do fit, whichever unit they arrived in.
    let micros = i64::try_from(nanoseconds / 1_000).ok()?;
    check_interval_micros(micros).ok()?;
    micros.checked_mul(1_000)
}

pub(super) fn truncate_interval_precision(
    column: &ArrayRef,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    let array = downcast_array::<PrimitiveArray<IntervalMonthDayNanoType>>(column, site.name())?;
    // Both facts are folded over the whole column with no early exit, so the scan vectorizes; a
    // rejection's row is recovered by re-scanning the failing batch.
    //
    // Null slots join the alignment fold. At worst they cost an unneeded truncation pass: `unary`
    // rewrites values under the same null buffer, so no logical value changes.
    let mut aligned = true;
    let mut out_of_range = false;
    for value in array.values() {
        out_of_range |= check_interval_months_days(value.months, value.days).is_err();
        aligned &= value.nanoseconds % 1_000 == 0;
    }
    if out_of_range {
        for (index, value) in array.values().iter().enumerate() {
            if let Err(component) = check_interval_months_days(value.months, value.days)
                && array.is_valid(index)
            {
                return Err(interval_out_of_range(component, site, index));
            }
        }
    }
    if aligned {
        return Ok(column.clone());
    }
    Ok(Arc::new(array.unary::<_, IntervalMonthDayNanoType>(
        |value| {
            arrow_buffer::IntervalMonthDayNano::new(
                value.months,
                value.days,
                truncate_to_microsecond_precision(value.nanoseconds),
            )
        },
    )))
}

pub(super) fn encode_range_datetime_column(
    column: &ArrayRef,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    let array = downcast_array::<TimestampMicrosecondArray>(column, site.name())?;
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    for (index, value) in array.iter().enumerate() {
        match value {
            Some(value) => builder.append_value(pack_civil_datetime_micros(value, site, index)?),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn pack_civil_datetime_micros(value: i64, site: &ValueSite<'_>, index: usize) -> Result<i64> {
    const MICROS_PER_SECOND: i64 = 1_000_000;
    const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
    let days = value.div_euclid(MICROSECONDS_PER_DAY);
    let time = value.rem_euclid(MICROSECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    if !(1..=9_999).contains(&year) {
        let (name, row) = (site.name(), site.row(index));
        return Err(error(
            format!(
                "BigQuery RANGE<DATETIME> ingest field {name:?} contains an out-of-range value \
                 at row {row}; BigQuery supports years 1 to 9999"
            ),
            Status::InvalidData,
        ));
    }
    let hours = time / MICROS_PER_HOUR;
    let time = time % MICROS_PER_HOUR;
    let minutes = time / MICROS_PER_MINUTE;
    let time = time % MICROS_PER_MINUTE;
    let seconds = time / MICROS_PER_SECOND;
    let micros = time % MICROS_PER_SECOND;
    let packed_time = (((hours << 6) | minutes) << 6 | seconds) << 20 | micros;
    let packed_date = (((year << 4) | month) << 5) | day;
    Ok((packed_date << 37) | packed_time)
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::IntervalMonthDayNanoType;
    use arrow_array::{DurationNanosecondArray, DurationSecondArray};

    use super::*;

    /// A duration becomes an INTERVAL with the whole span in its time component (two days are 48
    /// hours), as `parameter_value` (`src/parameters.rs`) binds it, so an ingested duration
    /// compares equal to a parameter built from the same value.
    #[test]
    fn a_duration_longer_than_a_day_is_encoded_as_hours_rather_than_days() {
        let column: ArrayRef = Arc::new(DurationSecondArray::from(vec![
            Some(172_800),
            Some(-172_800),
            None,
        ]));
        let encoded =
            encode_duration_column(&column, TimeUnit::Second, &ValueSite::column("span", 0))
                .unwrap();
        let values = encoded.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(
            values.value(0),
            arrow_buffer::IntervalMonthDayNano::new(0, 0, 172_800_000_000_000)
        );
        assert_eq!(
            values.value(1),
            arrow_buffer::IntervalMonthDayNano::new(0, 0, -172_800_000_000_000)
        );
        assert!(values.is_null(2));
    }

    /// Sub-microsecond nanoseconds are truncated towards zero, the same policy every other
    /// temporal ingest column applies.
    #[test]
    fn duration_nanoseconds_below_a_microsecond_are_truncated_towards_zero() {
        let column: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![1_500, -1_500]));
        let encoded =
            encode_duration_column(&column, TimeUnit::Nanosecond, &ValueSite::column("span", 0))
                .unwrap();
        let values = encoded.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(
            values.value(0),
            arrow_buffer::IntervalMonthDayNano::new(0, 0, 1_000)
        );
        assert_eq!(
            values.value(1),
            arrow_buffer::IntervalMonthDayNano::new(0, 0, -1_000)
        );
    }

    /// A duration no INTERVAL can carry names the field and row without exposing the value.
    #[test]
    fn a_duration_beyond_the_interval_range_is_refused_by_field_and_row() {
        let column: ArrayRef = Arc::new(DurationSecondArray::from(vec![0, i64::MAX]));
        let failure =
            encode_duration_column(&column, TimeUnit::Second, &ValueSite::column("span", 0))
                .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            "Arrow duration ingest field \"span\" contains an out-of-range value at row 1; \
             the maximum is 2562047 hours"
        );
    }

    #[test]
    fn range_datetime_errors_do_not_reveal_bound_values() {
        let column: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            0,
            876_543_210_987_654_321,
        ]));
        let failure =
            encode_range_datetime_column(&column, &ValueSite::column("window", 0)).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        crate::test_support::errors::assert_message(
            &failure,
            &["RANGE<DATETIME>", "\"window\"", "row 1", "years 1 to 9999"],
        );
        crate::test_support::errors::assert_message_omits(&failure, &["876543210987654321"]);
    }
}
