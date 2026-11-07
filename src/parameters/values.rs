//! One bound row's values, rendered the way BigQuery spells its query parameter literals.

use std::collections::HashMap;

use adbc_core::error::{Error, Result, Status};
use arrow_array::downcast_dictionary_array;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, DecimalType, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, GenericListArray,
    GenericListViewArray, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
    LargeListViewArray, LargeStringArray, ListArray, ListViewArray, OffsetSizeTrait,
    PrimitiveArray, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::error::{driver_defect, error, not_implemented};
use crate::rest::model::{QueryParameterType, QueryParameterValue, RangeValue};
use crate::types::decimal::spans_full_physical_domain;
use crate::types::temporal::{
    IntervalOutOfRange, MICROSECONDS_PER_DAY, MILLISECONDS_PER_DAY, NANOSECONDS_PER_DAY,
    check_interval_micros, check_interval_months_days, civil_from_days,
    nanoseconds_to_microseconds,
};
use crate::types::{ValueAt, timestamp_is_zoned};

/// Renders row `row` of `array` as a value of `parameter_type`, the type the plan derived from the
/// array's field, which decides what a nested value is: a RANGE and a STRUCT share an Arrow layout.
pub(super) fn parameter_value(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    array: &dyn Array,
    row: usize,
) -> Result<QueryParameterValue> {
    // A NullArray's every row is null, though it has no validity buffer to say so.
    if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
        return Ok(QueryParameterValue::default());
    }
    match array.data_type() {
        DataType::List(_) => {
            offset_list_value(at, parameter_type, downcast::<ListArray>(array)?, row)
        }
        DataType::LargeList(_) => {
            offset_list_value(at, parameter_type, downcast::<LargeListArray>(array)?, row)
        }
        DataType::ListView(_) => {
            list_view_value(at, parameter_type, downcast::<ListViewArray>(array)?, row)
        }
        DataType::LargeListView(_) => list_view_value(
            at,
            parameter_type,
            downcast::<LargeListViewArray>(array)?,
            row,
        ),
        DataType::FixedSizeList(_, _) => {
            let list = downcast::<FixedSizeListArray>(array)?;
            let length = offset_to_usize(at, list.value_length(), "FixedSizeList")?;
            // `value_offset_at` multiplies unchecked; an offset the platform cannot address is
            // refused rather than wrapped.
            let start = row.checked_mul(length).ok_or_else(|| {
                invalid_value(format!(
                    "{at} is an Arrow FixedSizeList whose offset is too large to address"
                ))
            })?;
            let end = list_slice_end(at, start, length, "FixedSizeList")?;
            list_value(at, parameter_type, list.values().as_ref(), start, end)
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => dictionary_value(at, parameter_type, array, row),
            _ => Err(mismatched_array(array))
        },
        DataType::Struct(_) if parameter_type.range_element_type.is_some() => {
            range_value(at, downcast(array)?, row)
        }
        DataType::Struct(_) => struct_value(at, parameter_type, downcast(array)?, row),
        _ => Ok(QueryParameterValue {
            value: Some(scalar_value(at, array, row)?),
            ..Default::default()
        }),
    }
}

/// Renders one non-null scalar as the `value` string a `QueryParameterValue` carries.
fn scalar_value(at: &ValueAt<'_>, array: &dyn Array, row: usize) -> Result<String> {
    Ok(match array.data_type() {
        DataType::Boolean => downcast::<BooleanArray>(array)?.value(row).to_string(),
        DataType::Int8 => downcast::<Int8Array>(array)?.value(row).to_string(),
        DataType::Int16 => downcast::<Int16Array>(array)?.value(row).to_string(),
        DataType::Int32 => downcast::<Int32Array>(array)?.value(row).to_string(),
        DataType::Int64 => downcast::<Int64Array>(array)?.value(row).to_string(),
        DataType::UInt8 => downcast::<UInt8Array>(array)?.value(row).to_string(),
        DataType::UInt16 => downcast::<UInt16Array>(array)?.value(row).to_string(),
        DataType::UInt32 => downcast::<UInt32Array>(array)?.value(row).to_string(),
        DataType::UInt64 => {
            let value = downcast::<UInt64Array>(array)?.value(row);
            i64::try_from(value)
                .map_err(|_| {
                    invalid_value(format!(
                        "{at} is an Arrow UInt64 that exceeds BigQuery INT64; \
                         bind it as a STRING or NUMERIC parameter instead"
                    ))
                })?
                .to_string()
        }
        DataType::Float16 => float_value(f32::from(downcast::<Float16Array>(array)?.value(row))),
        DataType::Float32 => float_value(downcast::<Float32Array>(array)?.value(row)),
        DataType::Float64 => float_value(downcast::<Float64Array>(array)?.value(row)),
        DataType::Utf8 => downcast::<StringArray>(array)?.value(row).to_owned(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?.value(row).to_owned(),
        DataType::Utf8View => downcast::<StringViewArray>(array)?.value(row).to_owned(),
        DataType::Binary => BASE64.encode(downcast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => BASE64.encode(downcast::<LargeBinaryArray>(array)?.value(row)),
        DataType::BinaryView => BASE64.encode(downcast::<BinaryViewArray>(array)?.value(row)),
        DataType::FixedSizeBinary(_) => {
            BASE64.encode(downcast::<FixedSizeBinaryArray>(array)?.value(row))
        }
        DataType::Date32 => format_date(at, i64::from(downcast::<Date32Array>(array)?.value(row)))?,
        DataType::Date64 => {
            let millis = downcast::<Date64Array>(array)?.value(row);
            if millis.rem_euclid(MILLISECONDS_PER_DAY) != 0 {
                return Err(invalid_value(format!(
                    "{at} is an Arrow Date64 not aligned to a UTC day; a BigQuery DATE carries \
                     no time of day"
                )));
            }
            format_date(at, millis.div_euclid(MILLISECONDS_PER_DAY))?
        }
        DataType::Time32(TimeUnit::Second) => format_time(
            at,
            i64::from(downcast::<Time32SecondArray>(array)?.value(row)) * 1_000_000,
        )?,
        DataType::Time32(TimeUnit::Millisecond) => format_time(
            at,
            i64::from(downcast::<Time32MillisecondArray>(array)?.value(row)) * 1_000,
        )?,
        DataType::Time64(TimeUnit::Microsecond) => {
            format_time(at, downcast::<Time64MicrosecondArray>(array)?.value(row))?
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanoseconds = downcast::<Time64NanosecondArray>(array)?.value(row);
            if !(0..NANOSECONDS_PER_DAY).contains(&nanoseconds) {
                return Err(invalid_value(format!(
                    "{at} is an Arrow Time64(Nanosecond) outside a day"
                )));
            }
            format_time(at, nanoseconds_to_microseconds(nanoseconds))?
        }
        DataType::Timestamp(unit, timezone) => {
            let micros = temporal_micros::<
                TimestampSecondType,
                TimestampMillisecondType,
                TimestampMicrosecondType,
                TimestampNanosecondType,
            >(
                at,
                array,
                row,
                *unit,
                "timestamp",
                crate::types::temporal::instant_nanoseconds_to_microseconds,
            )?;
            let suffix = if timestamp_is_zoned(timezone.as_deref()) {
                "Z"
            } else {
                ""
            };
            format!(
                "{}T{}{suffix}",
                format_date(at, micros.div_euclid(MICROSECONDS_PER_DAY))?,
                format_time(at, micros.rem_euclid(MICROSECONDS_PER_DAY))?
            )
        }
        DataType::Decimal32(_, _) => decimal_value(at, downcast::<Decimal32Array>(array)?, row)?,
        DataType::Decimal64(_, _) => decimal_value(at, downcast::<Decimal64Array>(array)?, row)?,
        DataType::Decimal128(_, _) => decimal_value(at, downcast::<Decimal128Array>(array)?, row)?,
        DataType::Decimal256(_, _) => decimal_value(at, downcast::<Decimal256Array>(array)?, row)?,
        // A duration has no month or day component of its own: BigQuery normalizes the whole
        // span, so handing it over as hours:minutes:seconds is exact.
        DataType::Duration(unit) => {
            let micros = temporal_micros::<
                DurationSecondType,
                DurationMillisecondType,
                DurationMicrosecondType,
                DurationNanosecondType,
            >(
                at,
                array,
                row,
                *unit,
                "duration",
                nanoseconds_to_microseconds,
            )?;
            format_interval(at, 0, 0, micros)?
        }
        DataType::Interval(IntervalUnit::YearMonth) => format_interval(
            at,
            downcast::<IntervalYearMonthArray>(array)?.value(row),
            0,
            0,
        )?,
        DataType::Interval(IntervalUnit::DayTime) => {
            let value = downcast::<IntervalDayTimeArray>(array)?.value(row);
            format_interval(at, 0, value.days, i64::from(value.milliseconds) * 1_000)?
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = downcast::<IntervalMonthDayNanoArray>(array)?.value(row);
            format_interval(
                at,
                value.months,
                value.days,
                nanoseconds_to_microseconds(value.nanoseconds),
            )?
        }
        data_type => {
            return Err(not_implemented(format!(
                "{at} has Arrow type {data_type}, which no BigQuery query parameter type covers"
            )));
        }
    })
}

fn decimal_value<T: DecimalType>(
    at: &ValueAt<'_>,
    array: &PrimitiveArray<T>,
    row: usize,
) -> Result<String> {
    if !spans_full_physical_domain(T::MAX_PRECISION, array.precision(), array.scale()) {
        // Arrow's precision error includes the value. Report only the declared constraint.
        T::validate_decimal_precision(array.value(row), array.precision(), array.scale()).map_err(
            |_| {
                invalid_value(format!(
                    "{at} is an Arrow {} outside its declared precision {}",
                    T::PREFIX,
                    array.precision()
                ))
            },
        )?;
    }
    Ok(array.value_as_string(row))
}

fn dictionary_value<K: ArrowDictionaryKeyType>(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    array: &DictionaryArray<K>,
    row: usize,
) -> Result<QueryParameterValue> {
    let key = array.key(row).ok_or_else(|| {
        invalid_value(format!(
            "{at} is a non-null Arrow dictionary value with a null key"
        ))
    })?;
    parameter_value(at, parameter_type, array.values().as_ref(), key)
}

fn list_value(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    values: &dyn Array,
    start: usize,
    end: usize,
) -> Result<QueryParameterValue> {
    if start > end || end > values.len() {
        return Err(invalid_value(format!(
            "{at} spans {start}..{end}, outside its child array of length {}",
            values.len()
        )));
    }
    if start == end {
        // BigQuery treats empty `arrayValues` and `structValues` collections as absent, which
        // means SQL NULL. For a typed composite parameter, a present scalar marker instead
        // denotes the non-null empty value. The marker contents are self-describing even though
        // the parameter type determines how BigQuery interprets them.
        return Ok(QueryParameterValue {
            value: Some("[]".to_owned()),
            ..Default::default()
        });
    }
    let element_type = planned(parameter_type.array_type.as_deref(), values)?;
    let mut array_values = Vec::with_capacity(end - start);
    for (index, position) in (start..end).enumerate() {
        let element = at.element(index);
        let value = parameter_value(&element, element_type, values, position)?;
        // Checked on the converted value, so a dictionary entry pointing at NULL is caught too.
        if value == QueryParameterValue::default() {
            return Err(invalid_value(format!(
                "{element} is NULL; a BigQuery array cannot contain NULL"
            )));
        }
        array_values.push(value);
    }
    Ok(QueryParameterValue {
        array_values,
        ..Default::default()
    })
}

fn struct_value(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    array: &StructArray,
    row: usize,
) -> Result<QueryParameterValue> {
    if array.num_columns() == 0 {
        // The zero-field counterpart of an empty array's marker.
        return Ok(QueryParameterValue {
            value: Some("{}".to_owned()),
            ..Default::default()
        });
    }
    let mut struct_values = HashMap::with_capacity(array.num_columns());
    for ((field, column), member) in array
        .fields()
        .iter()
        .zip(array.columns())
        .zip(&parameter_type.struct_types)
    {
        let member_type = planned(member.r#type.as_deref(), column.as_ref())?;
        struct_values.insert(
            field.name().clone(),
            parameter_value(&at.member(field.name()), member_type, column.as_ref(), row)?,
        );
    }
    Ok(QueryParameterValue {
        struct_values,
        ..Default::default()
    })
}

/// A RANGE's bounds, of which a null one is unbounded. The plan admitted only a `{start, end}`
/// struct of scalar bounds.
fn range_value(at: &ValueAt<'_>, array: &StructArray, row: usize) -> Result<QueryParameterValue> {
    let [start, end] = array.columns() else {
        return Err(mismatched_array(array));
    };
    let bound = |name: &str, column: &dyn Array| -> Result<Option<Box<QueryParameterValue>>> {
        if column.is_null(row) {
            return Ok(None);
        }
        Ok(Some(Box::new(QueryParameterValue {
            value: Some(scalar_value(&at.member(name), column, row)?),
            ..Default::default()
        })))
    };
    Ok(QueryParameterValue {
        range_value: Some(Box::new(RangeValue {
            start: bound("start", start.as_ref())?,
            end: bound("end", end.as_ref())?,
        })),
        ..Default::default()
    })
}

/// One row of either offset-encoded list layout, `List` or `LargeList`.
fn offset_list_value<O: OffsetSizeTrait + std::fmt::Display + TryInto<usize>>(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    list: &GenericListArray<O>,
    row: usize,
) -> Result<QueryParameterValue> {
    let kind = if O::IS_LARGE { "LargeList" } else { "List" };
    let offsets = list.value_offsets();
    #[expect(
        clippy::indexing_slicing,
        reason = "a list array stores len + 1 offsets, and row < len"
    )]
    let (start, end) = (offsets[row], offsets[row + 1]);
    list_value(
        at,
        parameter_type,
        list.values().as_ref(),
        offset_to_usize(at, start, kind)?,
        offset_to_usize(at, end, kind)?,
    )
}

/// One row of either list-view layout, which addresses its values by start and size rather than
/// by consecutive offsets.
fn list_view_value<O: OffsetSizeTrait + std::fmt::Display + TryInto<usize>>(
    at: &ValueAt<'_>,
    parameter_type: &QueryParameterType,
    list: &GenericListViewArray<O>,
    row: usize,
) -> Result<QueryParameterValue> {
    let kind = if O::IS_LARGE {
        "LargeListView"
    } else {
        "ListView"
    };
    let start = offset_to_usize(at, list.value_offset(row), kind)?;
    let length = offset_to_usize(at, list.value_size(row), kind)?;
    let end = list_slice_end(at, start, length, kind)?;
    list_value(at, parameter_type, list.values().as_ref(), start, end)
}

fn list_slice_end(at: &ValueAt<'_>, start: usize, length: usize, kind: &str) -> Result<usize> {
    start.checked_add(length).ok_or_else(|| {
        invalid_value(format!(
            "{at} is an Arrow {kind} whose offset range is too large to address"
        ))
    })
}

fn offset_to_usize<O: Copy + std::fmt::Display + TryInto<usize>>(
    at: &ValueAt<'_>,
    offset: O,
    kind: &str,
) -> Result<usize> {
    offset.try_into().map_err(|_| {
        invalid_value(format!(
            "{at} is an Arrow {kind} with an invalid offset {offset}"
        ))
    })
}

/// Views a parameter column as the concrete Arrow array its `DataType` promises.
fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| mismatched_array(array))
}

/// The type the plan derived for a child of a nested value; it derives one for every child.
fn planned<'a>(
    parameter_type: Option<&'a QueryParameterType>,
    array: &dyn Array,
) -> Result<&'a QueryParameterType> {
    parameter_type.ok_or_else(|| mismatched_array(array))
}

/// A bound value BigQuery has no room for. Like an out-of-range ingested value or a NULL array
/// element, it is `INVALID_DATA`: the value is at fault rather than how the statement was set up,
/// which DB-API reports as `DataError`, the error PEP 249 names for a value out of range.
fn invalid_value(message: String) -> Error {
    error(message, Status::InvalidData)
}

/// An Arrow array whose layout is not the one its own `DataType` or its planned parameter type
/// promises: a driver defect, reported rather than panicking.
#[cold]
#[inline(never)]
fn mismatched_array(array: &dyn Array) -> Error {
    driver_defect(format_args!(
        "Arrow parameter array of type {} does not have the layout its type promises",
        array.data_type()
    ))
}

fn float_value<T: Into<f64>>(value: T) -> String {
    let value = value.into();
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

/// Scales one row of a four-unit Arrow temporal family (timestamp or duration) to microseconds.
fn temporal_micros<Second, Milli, Micro, Nano>(
    at: &ValueAt<'_>,
    array: &dyn Array,
    row: usize,
    unit: TimeUnit,
    kind: &str,
    narrow_nanoseconds: fn(i64) -> i64,
) -> Result<i64>
where
    Second: ArrowPrimitiveType<Native = i64>,
    Milli: ArrowPrimitiveType<Native = i64>,
    Micro: ArrowPrimitiveType<Native = i64>,
    Nano: ArrowPrimitiveType<Native = i64>,
{
    match unit {
        TimeUnit::Second => downcast::<PrimitiveArray<Second>>(array)?
            .value(row)
            .checked_mul(1_000_000),
        TimeUnit::Millisecond => downcast::<PrimitiveArray<Milli>>(array)?
            .value(row)
            .checked_mul(1_000),
        TimeUnit::Microsecond => Some(downcast::<PrimitiveArray<Micro>>(array)?.value(row)),
        TimeUnit::Nanosecond => Some(narrow_nanoseconds(
            downcast::<PrimitiveArray<Nano>>(array)?.value(row),
        )),
    }
    .ok_or_else(|| {
        invalid_value(format!(
            "{at} is an Arrow {kind} that overflows microsecond precision"
        ))
    })
}

fn format_date(at: &ValueAt<'_>, days: i64) -> Result<String> {
    let (year, month, day) = civil_from_days(days);
    if !(1..=9_999).contains(&year) {
        return Err(invalid_value(format!(
            "{at} is outside BigQuery's supported year range 1 to 9999"
        )));
    }
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

/// The `.ffffff` tail of a formatted TIME, DATETIME, or INTERVAL: BigQuery accepts a whole
/// second with no fractional part at all, so a zero remainder writes nothing.
struct FractionalSeconds(u64);

impl std::fmt::Display for FractionalSeconds {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            return Ok(());
        }
        write!(formatter, ".{:06}", self.0)
    }
}

fn format_time(at: &ValueAt<'_>, micros: i64) -> Result<String> {
    // A time of day is non-negative by definition, so the split runs on the unsigned value the
    // range check leaves behind rather than mixing signed and unsigned arithmetic.
    let time = u64::try_from(micros)
        .ok()
        .filter(|time| *time < MICROSECONDS_PER_DAY.unsigned_abs())
        .ok_or_else(|| invalid_value(format!("{at} is a time outside a day")))?;
    let hours = time / 3_600_000_000;
    let minutes = time / 60_000_000 % 60;
    let seconds = time / 1_000_000 % 60;
    let fractional = FractionalSeconds(time % 1_000_000);
    Ok(format!("{hours:02}:{minutes:02}:{seconds:02}{fractional}"))
}

fn format_interval(at: &ValueAt<'_>, months: i32, days: i32, micros: i64) -> Result<String> {
    let out_of_range = |component: IntervalOutOfRange| {
        invalid_value(format!(
            "{at} has a {component} outside BigQuery's supported interval range"
        ))
    };
    check_interval_months_days(months, days).map_err(out_of_range)?;
    check_interval_micros(micros).map_err(out_of_range)?;

    // GoogleSQL's canonical interval spelling is `[sign]Y-M [sign]D [sign]H:M:S[.F]`, and each
    // sign governs its whole group: ZetaSQL prints -1 microsecond as `0-0 0 -0:0:0.000001`. A
    // negative sub-hour interval therefore carries its sign on a zero hour.
    let absolute_months = months.unsigned_abs();
    let year_month_sign = if months < 0 { "-" } else { "" };
    let absolute_micros = micros.unsigned_abs();
    let time_sign = if micros < 0 { "-" } else { "" };
    let hours = absolute_micros / 3_600_000_000;
    let minutes = absolute_micros / 60_000_000 % 60;
    let seconds = absolute_micros / 1_000_000 % 60;
    let fractional = FractionalSeconds(absolute_micros % 1_000_000);
    Ok(format!(
        "{year_month_sign}{}-{} {days} {time_sign}{hours}:{minutes}:{seconds}{fractional}",
        absolute_months / 12,
        absolute_months % 12,
    ))
}
