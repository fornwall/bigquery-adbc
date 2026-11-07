//! Checking normalized values against the domains BigQuery accepts.
//!
//! A column that has the right Arrow type can still hold a value BigQuery has no room for: a date
//! outside its calendar, a `NUMERIC` spanning its full physical range rather than its declared
//! one, an interval component past its documented bound. Every such check happens here, before a
//! request is built, so the rejection names the column and the rule rather than arriving as a
//! service error against an opaque row.

use adbc_core::error::{Error, Result, Status};
use arrow_array::types::ByteArrayType;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, DecimalType, IntervalDayTimeType, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, BinaryArray, GenericByteArray, PrimitiveArray, StringArray};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};

use super::site::ValueSite;
use super::{downcast_array, normalized_array_type_error};
use crate::BIGQUERY_FIELD_METADATA_MAX_LENGTH;
use crate::error::error;
use crate::types::decimal::spans_full_physical_domain;
use crate::types::temporal::{
    IntervalOutOfRange, MICROSECONDS_PER_DAY, MILLISECONDS_PER_DAY, NANOSECONDS_PER_DAY,
    SECONDS_PER_DAY, check_interval_months_days,
};

/// Checks normalized RANGE bounds. Null bounds are unbounded; null parent structs have already
/// masked both children. Epoch microseconds and packed civil datetimes both preserve ordering.
pub(super) fn validate_range_values(columns: &[ArrayRef], site: &ValueSite<'_>) -> Result<()> {
    let [start, end] = columns else {
        return Err(crate::error::driver_defect(format_args!(
            "normalized BigQuery RANGE needs two bounds"
        )));
    };
    match start.data_type() {
        DataType::Date32 => validate_range_order::<Date32Type>(start, end, site),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            validate_range_order::<TimestampMicrosecondType>(start, end, site)
        }
        other => Err(normalized_array_type_error(site.name(), other)),
    }
}

fn validate_range_order<T: ArrowPrimitiveType>(
    start: &ArrayRef,
    end: &ArrayRef,
    site: &ValueSite<'_>,
) -> Result<()> {
    let start = downcast_array::<PrimitiveArray<T>>(start, site.name())?;
    let end = downcast_array::<PrimitiveArray<T>>(end, site.name())?;
    let invalid = start
        .iter()
        .zip(end)
        .position(|(start, end)| matches!((start, end), (Some(start), Some(end)) if start >= end));
    if let Some(index) = invalid {
        let (name, row) = (site.name(), site.row(index));
        return Err(error(
            format!(
                "BigQuery RANGE ingest field {name:?} has a lower bound greater than or equal \
                 to its upper bound at row {row}"
            ),
            Status::InvalidData,
        ));
    }
    Ok(())
}

// Unix day offsets for 0001-01-01 and 10000-01-01 (exclusive).
const CALENDAR_START_DAY: i64 = -719_162;
const CALENDAR_END_DAY: i64 = 2_932_897;

pub(super) fn validate_scalar_values(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()> {
    match column.data_type() {
        DataType::Date32 => validate_date_values(column, site),
        // A `Date64` is milliseconds since the epoch and is checked here, before `to_date32`
        // narrows it: that cast checks only day alignment and `i32` range, and every year past
        // 9999 is comfortably inside `i32`.
        DataType::Date64 => {
            validate_epoch_unit::<Date64Type>(column, site, "date", MILLISECONDS_PER_DAY)
        }
        DataType::Timestamp(unit, _) => validate_timestamp_values(column, site, *unit),
        // A `Time32` column counts the same day in the same unit, in a width the shared `i64`
        // constants do not have.
        DataType::Time32(TimeUnit::Second) => {
            validate_time_values::<Time32SecondType>(column, site, 86_400)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            validate_time_values::<Time32MillisecondType>(column, site, 86_400_000)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            validate_time_values::<Time64MicrosecondType>(column, site, MICROSECONDS_PER_DAY)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            validate_time_values::<Time64NanosecondType>(column, site, NANOSECONDS_PER_DAY)
        }
        DataType::Decimal32(_, _) => validate_decimal_values::<Decimal32Type>(column, site),
        DataType::Decimal64(_, _) => validate_decimal_values::<Decimal64Type>(column, site),
        DataType::Decimal128(_, _) => validate_decimal_values::<Decimal128Type>(column, site),
        DataType::Decimal256(_, _) => validate_decimal_values::<Decimal256Type>(column, site),
        DataType::Interval(IntervalUnit::YearMonth) => {
            validate_interval_year_month_values(column, site)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            validate_interval_day_time_values(column, site)
        }
        _ => Ok(()),
    }
}

fn out_of_calendar_range(kind: &str, site: &ValueSite<'_>, index: usize) -> Error {
    let (name, row) = (site.name(), site.row(index));
    error(
        format!(
            "Arrow {kind} ingest field {name:?} contains an out-of-range value at row {row}; \
             BigQuery supports years 1 to 9999"
        ),
        Status::InvalidData,
    )
}

/// Validates one primitive array with a fast whole-buffer scan followed by an exact diagnostic.
///
/// LLVM does not vectorize a loop with an early return, so violations are folded into one bit and
/// a rejection's row is recovered by re-scanning the failing batch.
///
/// The fast scan includes the physical values behind nulls, which are arbitrary but valid for
/// their type: they may trigger the reporting scan but never a rejection, because the second pass
/// applies the validity guard. `PrimitiveArray::values` is already offset for a sliced array, so
/// the index handed to `reject` is the value's position in the column.
fn validate_primitive_values<T, V>(
    array: &PrimitiveArray<T>,
    violation: impl Fn(&T::Native) -> Option<V>,
    reject: impl Fn(usize, V) -> Error,
) -> Result<()>
where
    T: ArrowPrimitiveType,
{
    let values = array.values();
    let any_violation = values
        .iter()
        .fold(false, |found, value| found | violation(value).is_some());
    if !any_violation {
        return Ok(());
    }
    for (index, value) in values.iter().enumerate() {
        if let Some(violation) = violation(value)
            && array.is_valid(index)
        {
            return Err(reject(index, violation));
        }
    }
    Ok(())
}

fn validate_date_values(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()> {
    let array = downcast_array::<PrimitiveArray<Date32Type>>(column, site.name())?;
    validate_primitive_values(
        array,
        |&days| (!(CALENDAR_START_DAY..CALENDAR_END_DAY).contains(&i64::from(days))).then_some(()),
        |index, ()| out_of_calendar_range("date", site, index),
    )
}

fn validate_timestamp_values(
    column: &ArrayRef,
    site: &ValueSite<'_>,
    unit: TimeUnit,
) -> Result<()> {
    match unit {
        TimeUnit::Second => {
            validate_epoch_unit::<TimestampSecondType>(column, site, "timestamp", SECONDS_PER_DAY)
        }
        TimeUnit::Millisecond => validate_epoch_unit::<TimestampMillisecondType>(
            column,
            site,
            "timestamp",
            MILLISECONDS_PER_DAY,
        ),
        TimeUnit::Microsecond => validate_epoch_unit::<TimestampMicrosecondType>(
            column,
            site,
            "timestamp",
            MICROSECONDS_PER_DAY,
        ),
        // Every `i64` nanosecond instant lies between 1677 and 2262, well inside BigQuery's
        // range, so there is nothing a nanosecond column can carry that needs rejecting.
        TimeUnit::Nanosecond => Ok(()),
    }
}

/// Checks an `i64` column of `per_day`-sized units since the epoch against BigQuery's calendar.
fn validate_epoch_unit<T>(
    column: &ArrayRef,
    site: &ValueSite<'_>,
    kind: &str,
    per_day: i64,
) -> Result<()>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let array = downcast_array::<PrimitiveArray<T>>(column, site.name())?;
    // Scale the bounds once; the per-value scan needs only comparisons, with no calendar
    // conversion or division. Every caller uses at most microseconds, so both bounds fit i64.
    let range = CALENDAR_START_DAY * per_day..CALENDAR_END_DAY * per_day;
    validate_primitive_values(
        array,
        |value| (!range.contains(value)).then_some(()),
        |index, ()| out_of_calendar_range(kind, site, index),
    )
}

fn validate_decimal_values<T>(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()>
where
    T: DecimalType,
{
    let array = downcast_array::<PrimitiveArray<T>>(column, site.name())?;
    if spans_full_physical_domain(array.precision(), array.scale()) {
        // The shared domain policy identifies parameter-free BIGNUMERIC, whose declared Arrow
        // precision understates the complete physical i256 domain this driver reads and writes.
        return Ok(());
    }
    // Arrow's precision errors include the value; use the predicate and report only the limit.
    let precision = array.precision();
    validate_primitive_values(
        array,
        |&value| (!T::is_valid_decimal_precision(value, precision)).then_some(()),
        |index, ()| {
            let (name, row) = (site.name(), site.row(index));
            error(
                format!(
                    "Arrow decimal ingest field {name:?} contains a value outside its declared \
                     precision {precision} at row {row}"
                ),
                Status::InvalidData,
            )
        },
    )
}

fn validate_interval_year_month_values(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()> {
    let array = downcast_array::<PrimitiveArray<IntervalYearMonthType>>(column, site.name())?;
    validate_primitive_values(
        array,
        |&months| check_interval_months_days(months, 0).err(),
        |index, component| interval_out_of_range(component, site, index),
    )
}

fn validate_interval_day_time_values(column: &ArrayRef, site: &ValueSite<'_>) -> Result<()> {
    let array = downcast_array::<PrimitiveArray<IntervalDayTimeType>>(column, site.name())?;
    validate_primitive_values(
        array,
        |value| check_interval_months_days(0, value.days).err(),
        |index, component| interval_out_of_range(component, site, index),
    )
}

/// Reports the INTERVAL component a validating scan found outside BigQuery's range.
pub(super) fn interval_out_of_range(
    component: IntervalOutOfRange,
    site: &ValueSite<'_>,
    index: usize,
) -> Error {
    let (name, row) = (site.name(), site.row(index));
    error(
        format!(
            "Arrow interval ingest field {name:?} has a {component} at row {row}, outside \
             BigQuery's supported range"
        ),
        Status::InvalidData,
    )
}

fn validate_time_values<T>(
    column: &ArrayRef,
    site: &ValueSite<'_>,
    upper_bound: T::Native,
) -> Result<()>
where
    T: ArrowPrimitiveType,
    T::Native: Default,
{
    let array = downcast_array::<PrimitiveArray<T>>(column, site.name())?;
    validate_primitive_values(
        array,
        |value| (!(T::Native::default()..upper_bound).contains(value)).then_some(()),
        |index, ()| {
            let (name, row) = (site.name(), site.row(index));
            error(
                format!(
                    "Arrow time ingest field {name:?} contains an out-of-range value at row \
                     {row}; a time must be within one day"
                ),
                Status::InvalidData,
            )
        },
    )
}

/// The `BIGQUERY:max_length` a normalized field declares. Schema conversion has already refused
/// one that is not a positive integer, before any batch is normalized.
pub(super) fn declared_max_length(field: &Field) -> Option<usize> {
    field
        .metadata()
        .get(BIGQUERY_FIELD_METADATA_MAX_LENGTH)?
        .parse()
        .ok()
}

/// Checks each value of a normalized `Utf8` or `Binary` column against the maximum length its
/// field declares: characters for a `STRING(L)`, bytes for a `BYTES(L)`, as BigQuery counts them.
///
/// Storage Write stores a value longer than its column allows without complaint (observed, not
/// documented), so a declared length constrains nothing unless it is checked here. A value's byte
/// length bounds its character count from above, so characters are counted only for a value whose
/// bytes already exceed the limit.
pub(super) fn validate_max_length(
    column: &ArrayRef,
    field: &Field,
    site: &ValueSite<'_>,
) -> Result<()> {
    let Some(limit) = declared_max_length(field) else {
        return Ok(());
    };
    let (violation, unit) = match column.data_type() {
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(column, site.name())?;
            let characters = |index| array.value(index).chars().count();
            (first_too_long(array, limit, characters), "characters")
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(column, site.name())?;
            let bytes = |index| array.value(index).len();
            (first_too_long(array, limit, bytes), "bytes")
        }
        _ => return Ok(()),
    };
    let Some((index, length)) = violation else {
        return Ok(());
    };
    let (name, row) = (site.name(), site.row(index));
    Err(error(
        format!(
            "BigQuery ingest field {name:?} holds a value of {length} {unit} at row {row}, longer \
             than its declared maximum length of {limit}"
        ),
        Status::InvalidData,
    ))
}

/// The first live value of `array` whose `length` exceeds `limit`, with that length.
fn first_too_long<T: ByteArrayType<Offset = i32>>(
    array: &GenericByteArray<T>,
    limit: usize,
    length: impl Fn(usize) -> usize,
) -> Option<(usize, usize)> {
    let limit_bytes = i32::try_from(limit).unwrap_or(i32::MAX);
    let offsets = array.value_offsets();
    offsets
        .iter()
        .zip(offsets.iter().skip(1))
        .enumerate()
        .filter(|&(index, (start, end))| end - start > limit_bytes && array.is_valid(index))
        .map(|(index, _)| (index, length(index)))
        .find(|&(_, length)| length > limit)
}

#[cfg(test)]
mod tests;
