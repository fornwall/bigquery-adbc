//! The Arrow casts BigQuery bulk ingest needs.
//!
//! Ingest only casts into the small closed set of types `normalized_data_type` produces.
//! `arrow-cast` would link every string, decimal and temporal parsing kernel to reach a handful of
//! type pairs; these kernels cover exactly those pairs.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::builder::GenericByteBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    BinaryType, BinaryViewType, ByteArrayType, ByteViewType, Date32Type, Date64Type, Decimal32Type,
    Decimal64Type, Decimal128Type, Decimal256Type, DecimalType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, LargeBinaryType, LargeUtf8Type,
    StringViewType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, Utf8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BinaryArray, FixedSizeBinaryArray,
    GenericByteArray, GenericByteViewArray, PrimitiveArray, downcast_dictionary_array,
};
use arrow_buffer::{IntervalMonthDayNano, NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::error::error;
use crate::types::decimal::spans_full_physical_domain;
use crate::types::temporal::{MILLISECONDS_PER_DAY, nanoseconds_to_microseconds};

/// A cast failure, phrased as the reason clause of the [`cast_column`] message.
type CastResult = std::result::Result<ArrayRef, String>;

/// Casts an ingest column to `data_type`, the normalized BigQuery Arrow type of its field.
pub(crate) fn cast_column(column: &ArrayRef, data_type: &DataType, name: &str) -> Result<ArrayRef> {
    cast_array(column, data_type).map_err(|reason| {
        error(
            format!(
                "failed to normalize BigQuery ingest column {name:?} from {} to {data_type}: {reason}",
                column.data_type()
            ),
            Status::InvalidData,
        )
    })
}

/// Casts a query result column to `data_type`, the canonical BigQuery Arrow type of its field.
///
/// Storage Read results use the service's Arrow spelling, which differs from the canonical one in
/// ways the caller has already vetted. The kernels are ingest's; only the failure wording differs,
/// since the value came from BigQuery rather than the caller.
pub(crate) fn cast_result_column(
    column: &ArrayRef,
    data_type: &DataType,
    name: &str,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    cast_array(column, data_type).map_err(|reason| {
        arrow_schema::ArrowError::CastError(format!(
            "failed to normalize BigQuery Storage Read column {name:?} from {} to {data_type}: {reason}",
            column.data_type()
        ))
    })
}

fn cast_array(column: &ArrayRef, data_type: &DataType) -> CastResult {
    if column.data_type() == data_type {
        return Ok(column.clone());
    }
    match (column.data_type(), data_type) {
        (DataType::Dictionary(_, _), _) => {
            let values = unpack_dictionary(column)?;
            cast_array(&values, data_type)
        }
        (_, DataType::Int64) => to_int64(column),
        (_, DataType::Float64) => to_float64(column),
        (_, DataType::Utf8) => to_utf8(column),
        (_, DataType::Binary) => to_binary(column),
        (DataType::Date64, DataType::Date32) => to_date32(column),
        (
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
            DataType::Decimal128(precision, scale),
        ) => to_decimal::<Decimal128Type>(column, *precision, *scale),
        (
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
            DataType::Decimal256(precision, scale),
        ) => to_decimal::<Decimal256Type>(column, *precision, *scale),
        (_, DataType::Time64(TimeUnit::Microsecond)) => to_time64_micros(column),
        (DataType::Timestamp(unit, _), DataType::Timestamp(TimeUnit::Microsecond, timezone)) => {
            to_timestamp_micros(column, *unit, timezone.clone())
        }
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(Arc::new(
            column
                .as_primitive::<IntervalYearMonthType>()
                .unary::<_, IntervalMonthDayNanoType>(|months| {
                    IntervalMonthDayNano::new(months, 0, 0)
                }),
        )),
        (
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(Arc::new(
            column
                .as_primitive::<IntervalDayTimeType>()
                .unary::<_, IntervalMonthDayNanoType>(|value| {
                    IntervalMonthDayNano::new(
                        0,
                        value.days,
                        i64::from(value.milliseconds) * 1_000_000,
                    )
                }),
        )),
        _ => Err(unsupported()),
    }
}

fn unsupported() -> String {
    "the cast is not supported".to_string()
}

#[cold]
#[inline(never)]
fn out_of_range() -> String {
    "value is out of range".to_owned()
}

/// Materializes a dictionary as its value type, the shape the Storage Write API expects.
///
/// This is `arrow_select::take::take(values, keys)` narrowed to the one gather the driver needs.
/// `MutableArrayData` copies whole rows type-erased, so every value type stays supported without
/// linking arrow-select's per-type take kernels.
fn unpack_dictionary(column: &ArrayRef) -> CastResult {
    downcast_dictionary_array!(
        column => expand_dictionary(column.values(), column.keys()),
        _ => Err(unsupported()),
    )
}

fn expand_dictionary<K>(values: &ArrayRef, keys: &PrimitiveArray<K>) -> CastResult
where
    K: arrow_array::types::ArrowDictionaryKeyType,
    K::Native: std::fmt::Display,
{
    use arrow_buffer::ArrowNativeType;

    // Consecutive keys already describe an Arrow slice. Preserve its buffers and nulls
    // instead of allocating a gather, including for sliced and empty dictionaries.
    if keys.null_count() == 0 {
        let start = keys
            .values()
            .first()
            .and_then(|key| key.to_usize())
            .unwrap_or(0);
        if start
            .checked_add(keys.len())
            .is_some_and(|end| end <= values.len())
            && keys
                .values()
                .iter()
                .enumerate()
                .all(|(row, key)| key.to_usize() == Some(start + row))
        {
            return Ok(values.slice(start, keys.len()));
        }
    }
    gather_dictionary(values, keys.len(), keys.nulls(), &|row| {
        let key = keys.value(row);
        key.to_usize().ok_or_else(|| key.to_string())
    })
}

/// Gathers `values` by the keys of `len` rows, `nulls` marking the null ones. `index` reads a
/// row's key as an index, or spells a negative key; it is the only step per key type, so the
/// gather is instantiated once.
fn gather_dictionary(
    values: &ArrayRef,
    len: usize,
    nulls: Option<&NullBuffer>,
    index: &dyn Fn(usize) -> std::result::Result<usize, String>,
) -> CastResult {
    let expand_failed =
        |source: arrow_schema::ArrowError| format!("failed to expand the dictionary: {source}");
    let value_count = values.len();
    // Arrow validates dictionary keys on construction, but arrays imported over the C data
    // interface arrive with whatever keys the producer wrote.
    let resolve = |row| {
        match index(row) {
            Ok(index) if index < value_count => Ok(index),
            Ok(index) => Err(index.to_string()),
            Err(key) => Err(key),
        }
        .map_err(|key| {
            format!("dictionary key {key} is out of bounds for {value_count} dictionary values")
        })
    };
    let nulls = nulls.filter(|nulls| nulls.null_count() > 0);
    let data = values.to_data();
    let mut expanded = MutableArrayData::new(vec![&data], nulls.is_some(), len);
    let gather_valid = |expanded: &mut MutableArrayData<'_>, rows: std::ops::Range<usize>| {
        // `try_extend` copies a range from every value buffer, so gather each ascending run of
        // keys in one call.
        let mut run: Option<(usize, usize)> = None;
        for row in rows {
            let index = resolve(row)?;
            match run {
                Some((start, end)) if index == end => run = Some((start, end + 1)),
                Some((start, end)) => {
                    expanded.try_extend(0, start, end).map_err(expand_failed)?;
                    run = Some((index, index + 1));
                }
                None => run = Some((index, index + 1)),
            }
        }
        if let Some((start, end)) = run {
            expanded.try_extend(0, start, end).map_err(expand_failed)?;
        }
        Ok::<_, String>(())
    };
    if let Some(nulls) = nulls {
        let mut end = 0;
        for (start, valid_end) in nulls.valid_slices() {
            if start != end {
                expanded
                    .try_extend_nulls(start - end)
                    .map_err(expand_failed)?;
            }
            gather_valid(&mut expanded, start..valid_end)?;
            end = valid_end;
        }
        if end != len {
            expanded
                .try_extend_nulls(len - end)
                .map_err(expand_failed)?;
        }
    } else {
        gather_valid(&mut expanded, 0..len)?;
    }
    Ok(arrow_array::make_array(expanded.freeze()))
}

fn to_int64(column: &ArrayRef) -> CastResult {
    let values = match column.data_type() {
        DataType::Int8 => widen::<Int8Type>(column),
        DataType::Int16 => widen::<Int16Type>(column),
        DataType::Int32 => widen::<Int32Type>(column),
        DataType::UInt8 => widen::<UInt8Type>(column),
        DataType::UInt16 => widen::<UInt16Type>(column),
        DataType::UInt32 => widen::<UInt32Type>(column),
        _ => return Err(unsupported()),
    };
    Ok(Arc::new(values))
}

/// Widens an integer that always fits [`i64`], so no value can be rejected.
fn widen<T>(column: &ArrayRef) -> PrimitiveArray<Int64Type>
where
    T: ArrowPrimitiveType,
    i64: From<T::Native>,
{
    column.as_primitive::<T>().unary::<_, Int64Type>(i64::from)
}

fn to_float64(column: &ArrayRef) -> CastResult {
    let values = match column.data_type() {
        DataType::Float16 => column
            .as_primitive::<Float16Type>()
            .unary::<_, Float64Type>(f64::from),
        DataType::Float32 => column
            .as_primitive::<Float32Type>()
            .unary::<_, Float64Type>(f64::from),
        _ => return Err(unsupported()),
    };
    Ok(Arc::new(values))
}

fn to_utf8(column: &ArrayRef) -> CastResult {
    match column.data_type() {
        DataType::LargeUtf8 => narrow_offsets::<LargeUtf8Type, Utf8Type>(column.as_string::<i64>()),
        DataType::Utf8View => from_view::<StringViewType, Utf8Type>(column.as_string_view()),
        _ => Err(unsupported()),
    }
}

fn to_binary(column: &ArrayRef) -> CastResult {
    match column.data_type() {
        DataType::LargeBinary => {
            narrow_offsets::<LargeBinaryType, BinaryType>(column.as_binary::<i64>())
        }
        DataType::BinaryView => from_view::<BinaryViewType, BinaryType>(column.as_binary_view()),
        DataType::FixedSizeBinary(_) => from_fixed_size_binary(column.as_fixed_size_binary()),
        _ => Err(unsupported()),
    }
}

/// Rebases 64-bit offsets onto the values they address so they fit 32-bit offsets whenever the
/// addressed range does, without copying the values themselves.
fn narrow_offsets<F, T>(array: &GenericByteArray<F>) -> CastResult
where
    F: ByteArrayType<Offset = i64>,
    T: ByteArrayType<Offset = i32, Native = F::Native>,
{
    let (offsets, values, nulls) = array.clone().into_parts();
    let first = offsets.first();
    let last = offsets.last();
    // Narrowing can fail even for valid offsets: a `LargeUtf8` column may address more than
    // `i32::MAX` bytes, and a sliced one starts partway into its values. The checks also hold
    // without `crate::ffi::import::validate_imported`: wild offsets would otherwise overflow `i64`
    // or panic in `Buffer::slice_with_length` instead of returning an ADBC error.
    let span = last.checked_sub(first).ok_or_else(out_of_range)?;
    let start = usize::try_from(first).map_err(|_| out_of_range())?;
    let length = usize::try_from(span).map_err(|_| out_of_range())?;
    if i32::try_from(length).is_err() {
        return Err(format!(
            "{length} bytes of values do not fit 32-bit offsets"
        ));
    }
    if start
        .checked_add(length)
        .is_none_or(|end| end > values.len())
    {
        return Err(format!(
            "{length} bytes of values at offset {start} are outside the {} bytes the array holds",
            values.len()
        ));
    }
    // Check the whole window before narrowing. A reduction has no per-offset error branch,
    // allowing validation and rebasing to vectorize; the payload remains shared.
    let (minimum, maximum) = offsets
        .iter()
        .fold((first, last), |(minimum, maximum), &offset| {
            (minimum.min(offset), maximum.max(offset))
        });
    if minimum < first || maximum > last {
        return Err(out_of_range());
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "every offset is in first..=last, whose nonnegative span fits i32"
    )]
    let offsets = offsets
        .iter()
        // The validated subtraction cannot wrap, including with release overflow checks.
        .map(|offset| offset.wrapping_sub(first) as i32)
        .collect::<Vec<_>>();
    let values = values.slice_with_length(start, length);
    GenericByteArray::<T>::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        nulls,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|source| format!("failed to narrow the offsets: {source}"))
}

/// Materializes a view array as the contiguous 32-bit-offset array of the same value type.
///
/// `V` is the source view type and `T` the contiguous target, sharing a `Native` value type. A view
/// array with no nulls is copied by index, skipping the iterator's validity checks.
///
/// Unlike the offset kernels, this cannot fail closed: reading a view uses `Buffer::get_unchecked`
/// and `from_bytes_unchecked`, so a malformed view is undefined behavior. Every view array reaching
/// here must already be validated, by `crate::ffi::import::validate_imported` at the C data
/// interface or by `ArrayDataBuilder::build` in the Storage Read IPC reader.
fn from_view<V, T>(array: &GenericByteViewArray<V>) -> CastResult
where
    V: ByteViewType,
    T: ByteArrayType,
    V::Native: AsRef<T::Native>,
{
    // Repeated views can expand a small source buffer beyond the target's i32 offsets. Check
    // before allocating, and ignore null slots because their payloads are never copied.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the low 32 bits of an Arrow view are the value's length by definition"
    )]
    let values_length = array
        .views()
        .iter()
        .enumerate()
        .try_fold(0_i32, |length, (index, view)| {
            if array.is_null(index) {
                return Some(length);
            }
            length.checked_add(i32::try_from(*view as u32).ok()?)
        })
        .ok_or_else(|| "view values do not fit 32-bit offsets".to_owned())?;
    let values_length = usize::try_from(values_length).map_err(|_| out_of_range())?;
    let mut builder = GenericByteBuilder::<T>::with_capacity(array.len(), values_length);
    if array.null_count() == 0 {
        for index in 0..array.len() {
            builder.append_value(array.value(index));
        }
    } else {
        for value in array {
            builder.append_option(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Reinterprets fixed-width values as variable-width ones by generating their implied offsets.
fn from_fixed_size_binary(array: &FixedSizeBinaryArray) -> CastResult {
    let (width, values, nulls) = array.clone().into_parts();
    let width = usize::try_from(width).map_err(|_| out_of_range())?;
    let length = array
        .len()
        .checked_mul(width)
        .filter(|length| i32::try_from(*length).is_ok())
        .ok_or_else(|| {
            format!(
                "{} values of {width} bytes do not fit 32-bit offsets",
                array.len()
            )
        })?;
    // Check the implied span, since slicing past the values panics; see `narrow_offsets`.
    if length > values.len() {
        return Err(format!(
            "{} values of {width} bytes are outside the {} bytes the array holds",
            array.len(),
            values.len()
        ));
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "index * width <= length, and length fits i32 per the check above"
    )]
    let offsets = (0..=array.len())
        .map(|index| (index * width) as i32)
        .collect::<Vec<_>>();
    BinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values.slice_with_length(0, length),
        nulls,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|source| format!("failed to generate the offsets: {source}"))
}

fn to_date32(column: &ArrayRef) -> CastResult {
    let values = column
        .as_primitive::<Date64Type>()
        .try_unary::<_, Date32Type, _>(|value| {
            if value.rem_euclid(MILLISECONDS_PER_DAY) != 0 {
                return Err("value is not aligned to an Arrow Date64 day".to_owned());
            }
            i32::try_from(value / MILLISECONDS_PER_DAY).map_err(|_| out_of_range())
        })?;
    Ok(Arc::new(values))
}

fn decimal_scale_up(data_type: &DataType, target_scale: i8) -> std::result::Result<u32, String> {
    let source_scale = match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        _ => return Err(unsupported()),
    };
    // Normalization only pads fractional digits, so the refusal names that rule.
    u32::try_from(i16::from(target_scale) - i16::from(source_scale)).map_err(|_| {
        format!(
            "decimal scale {source_scale} cannot be reduced to {target_scale} without dropping \
             fractional digits"
        )
    })
}

/// A decimal width a column is normalized into: NUMERIC's `Decimal128` or BIGNUMERIC's
/// `Decimal256`. Every narrower source widens into either infallibly; an `i256` source may not.
trait TargetDecimal: DecimalType<Native: From<i32> + From<i64> + From<i128>> {
    fn from_i256(value: i256) -> Option<Self::Native>;
}

impl TargetDecimal for Decimal128Type {
    fn from_i256(value: i256) -> Option<i128> {
        value.to_i128()
    }
}

impl TargetDecimal for Decimal256Type {
    fn from_i256(value: i256) -> Option<i256> {
        Some(value)
    }
}

/// Casts a decimal of any physical width into `T`, padding it to `scale` and validating every
/// value against `precision`.
///
/// A target whose physical domain outruns its declared precision is not validated, because no
/// precision expresses that domain; see `spans_full_physical_domain`.
fn to_decimal<T: TargetDecimal>(column: &ArrayRef, precision: u8, scale: i8) -> CastResult {
    let scale_up = decimal_scale_up(column.data_type(), scale)?;
    let multiplier = T::Native::from(10_i32)
        .pow_checked(scale_up)
        .map_err(|_| format!("decimal scale increase {scale_up} overflows {}", T::PREFIX))?;
    let validate = !spans_full_physical_domain(T::MAX_PRECISION, precision, scale);
    let invalid_precision = |_| format!("value exceeds decimal precision {precision}");
    // A same-width source at the target scale is only relabelled: validate in place and reuse
    // the buffers. As in the rescaling kernels, only non-null values are validated, since a null
    // slot's payload is arbitrary after an FFI import or a `MutableArrayData` gather.
    if scale_up == 0
        && let Some(array) = column.as_primitive_opt::<T>()
    {
        if validate {
            array
                .validate_decimal_precision(precision)
                .map_err(invalid_precision)?;
        }
        return with_precision_and_scale(array.clone(), precision, scale);
    }
    let rescale = |value: T::Native| -> std::result::Result<T::Native, String> {
        let value = value.mul_checked(multiplier).map_err(|_| out_of_range())?;
        if validate {
            T::validate_decimal_precision(value, precision, scale).map_err(invalid_precision)?;
        }
        Ok(value)
    };
    let values = match column.data_type() {
        DataType::Decimal32(_, _) => column
            .as_primitive::<Decimal32Type>()
            .try_unary::<_, T, _>(|value| rescale(value.into()))?,
        DataType::Decimal64(_, _) => column
            .as_primitive::<Decimal64Type>()
            .try_unary::<_, T, _>(|value| rescale(value.into()))?,
        DataType::Decimal128(_, _) => column
            .as_primitive::<Decimal128Type>()
            .try_unary::<_, T, _>(|value| rescale(value.into()))?,
        DataType::Decimal256(_, _) => column
            .as_primitive::<Decimal256Type>()
            .try_unary::<_, T, _>(|value| rescale(T::from_i256(value).ok_or_else(out_of_range)?))?,
        _ => return Err(unsupported()),
    };
    with_precision_and_scale(values, precision, scale)
}

fn with_precision_and_scale<T: DecimalType>(
    array: PrimitiveArray<T>,
    precision: u8,
    scale: i8,
) -> CastResult {
    array
        .with_precision_and_scale(precision, scale)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|source| source.to_string())
}

/// Rescales a time of day after ingest validates it against the BigQuery TIME range.
fn to_time64_micros(column: &ArrayRef) -> CastResult {
    let values =
        match column.data_type() {
            DataType::Time32(TimeUnit::Second) => {
                column
                    .as_primitive::<Time32SecondType>()
                    .unary::<_, Time64MicrosecondType>(|value| i64::from(value) * 1_000_000)
            }
            DataType::Time32(TimeUnit::Millisecond) => column
                .as_primitive::<Time32MillisecondType>()
                .unary::<_, Time64MicrosecondType>(|value| i64::from(value) * 1_000),
            DataType::Time64(TimeUnit::Nanosecond) => column
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, Time64MicrosecondType>(nanoseconds_to_microseconds),
            _ => return Err(unsupported()),
        };
    Ok(Arc::new(values))
}

/// Rescales an instant. Timestamp values are absolute, so the target time zone is metadata only:
/// ingest only ever relabels a zoned timestamp as UTC or drops an empty zone.
fn to_timestamp_micros(
    column: &ArrayRef,
    unit: TimeUnit,
    timezone: Option<Arc<str>>,
) -> CastResult {
    let values = match unit {
        TimeUnit::Second => scale_up::<TimestampSecondType>(column, 1_000_000)?,
        TimeUnit::Millisecond => scale_up::<TimestampMillisecondType>(column, 1_000)?,
        TimeUnit::Microsecond => column.as_primitive::<TimestampMicrosecondType>().clone(),
        TimeUnit::Nanosecond => column
            .as_primitive::<TimestampNanosecondType>()
            .unary::<_, TimestampMicrosecondType>(
            crate::types::temporal::instant_nanoseconds_to_microseconds,
        ),
    };
    Ok(Arc::new(values.with_timezone_opt(timezone)))
}

fn scale_up<T>(
    column: &ArrayRef,
    scale: i64,
) -> std::result::Result<PrimitiveArray<TimestampMicrosecondType>, String>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    column
        .as_primitive::<T>()
        .try_unary::<_, TimestampMicrosecondType, _>(|value| {
            value.checked_mul(scale).ok_or_else(out_of_range)
        })
}

#[cfg(test)]
mod tests;
