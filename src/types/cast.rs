//! The Arrow casts BigQuery bulk ingest and Storage Read results need.
//!
//! Ingest only casts into the small closed set of types `normalized_data_type` produces, and
//! Storage Read only the pairs its reshape plan accepts. `arrow-cast` would link every string,
//! decimal and temporal parsing kernel to reach a handful of type pairs; these kernels cover
//! exactly those pairs.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::builder::GenericByteBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    BinaryType, BinaryViewType, ByteArrayType, ByteViewType, Date32Type, Date64Type,
    Decimal128Type, Decimal256Type, DecimalType, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType,
    IntervalYearMonthType, LargeBinaryType, LargeUtf8Type, StringViewType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, Utf8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BinaryArray, FixedSizeBinaryArray,
    GenericByteArray, GenericByteViewArray, OffsetSizeTrait, PrimitiveArray,
    downcast_dictionary_array,
};
use arrow_buffer::{
    BooleanBuffer, Buffer, IntervalMonthDayNano, MutableBuffer, NullBuffer, OffsetBuffer,
    ScalarBuffer,
};
use arrow_data::ArrayData;
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
        (DataType::Decimal128(_, source_scale), DataType::Decimal128(precision, scale)) => {
            to_decimal::<Decimal128Type, Decimal128Type>(column, *source_scale, *precision, *scale)
        }
        (DataType::Decimal128(_, source_scale), DataType::Decimal256(precision, scale)) => {
            to_decimal::<Decimal128Type, Decimal256Type>(column, *source_scale, *precision, *scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Decimal256(precision, scale)) => {
            to_decimal::<Decimal256Type, Decimal256Type>(column, *source_scale, *precision, *scale)
        }
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
/// This is `arrow_select::take::take(values, keys)` narrowed to the one gather the driver needs,
/// without linking arrow-select's per-type take kernels. Primitive values (those with a
/// `primitive_width`) are copied a run of ascending keys at a time and byte values row by row;
/// other value types, Boolean and `FixedSizeBinary` included, and byte values in long runs, go
/// through `MutableArrayData`, one type-erased extend per run.
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
    // Only reading the keys is generic, so every gather below is instantiated once.
    let mut indices = keys
        .values()
        .iter()
        .map(|key| key.to_usize().unwrap_or(NULL_ROW))
        .collect::<Vec<_>>();
    if let Err(row) = resolve_indices(&mut indices, keys.nulls(), values) {
        return Err(key_out_of_bounds(&keys.value(row), values.len()));
    }
    match values.data_type() {
        DataType::Utf8 => {
            let text = values.as_string::<i32>();
            gather_bytes(values, text.value_offsets(), text.value_data(), &indices)
        }
        DataType::LargeUtf8 => {
            let text = values.as_string::<i64>();
            gather_bytes(values, text.value_offsets(), text.value_data(), &indices)
        }
        DataType::Binary => {
            let bytes = values.as_binary::<i32>();
            gather_bytes(values, bytes.value_offsets(), bytes.value_data(), &indices)
        }
        DataType::LargeBinary => {
            let bytes = values.as_binary::<i64>();
            gather_bytes(values, bytes.value_offsets(), bytes.value_data(), &indices)
        }
        data_type if data_type.primitive_width().is_some() => gather_fixed_width(values, &indices),
        _ => gather_runs(values, &indices),
    }
}

/// A row whose key, or the value its key names, is NULL, among the indices a gather copies.
const NULL_ROW: usize = usize::MAX;

/// Turns each row's key, read as an index ([`NULL_ROW`] for one that does not fit `usize`, such as
/// a negative one; a `u64::MAX` key reads the same), into the value index the gathers copy, or
/// [`NULL_ROW`] where the key or its value is NULL. A non-NULL key read as [`NULL_ROW`] is out of
/// bounds and refused as such.
///
/// Every key is checked before anything is copied, so a bad one fails first, naming its row:
/// Arrow validates dictionary keys on construction, but arrays imported over the C data interface
/// arrive with whatever keys the producer wrote.
fn resolve_indices(
    indices: &mut [usize],
    key_nulls: Option<&NullBuffer>,
    values: &ArrayRef,
) -> std::result::Result<(), usize> {
    let value_count = values.len();
    let key_nulls = key_nulls.filter(|nulls| nulls.null_count() > 0);
    let value_nulls = values.nulls().filter(|nulls| nulls.null_count() > 0);
    for (row, index) in indices.iter_mut().enumerate() {
        if key_nulls.is_some_and(|nulls| nulls.is_null(row)) {
            *index = NULL_ROW;
        } else if *index >= value_count {
            return Err(row);
        } else if value_nulls.is_some_and(|nulls| nulls.is_null(*index)) {
            *index = NULL_ROW;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn key_out_of_bounds(key: &dyn std::fmt::Display, value_count: usize) -> String {
    format!("dictionary key {key} is out of bounds for {value_count} dictionary values")
}

/// How many rows from `row` on continue the ascending run of value indices `row` starts; one for
/// a lone index, and for a [`NULL_ROW`].
fn run_length(indices: &[usize], row: usize) -> usize {
    match indices.get(row..) {
        Some([first, rest @ ..]) if *first != NULL_ROW => {
            1 + rest
                .iter()
                .zip(first + 1..)
                .take_while(|&(&index, expected)| index == expected)
                .count()
        }
        _ => 1,
    }
}

/// Gathers any value type by its value indices, one `MutableArrayData` extend per run of
/// ascending indices and one per run of [`NULL_ROW`]s.
fn gather_runs(values: &ArrayRef, indices: &[usize]) -> CastResult {
    let data = values.to_data();
    let mut expanded =
        MutableArrayData::new(vec![&data], indices.contains(&NULL_ROW), indices.len());
    let mut row = 0;
    while let Some(&index) = indices.get(row) {
        let extended = if index == NULL_ROW {
            let nulls = indices
                .get(row..)
                .unwrap_or_default()
                .iter()
                .take_while(|&&index| index == NULL_ROW)
                .count();
            row += nulls;
            expanded.try_extend_nulls(nulls)
        } else {
            let length = run_length(indices, row);
            row += length;
            expanded.try_extend(0, index, index + length)
        };
        extended.map_err(|source| format!("failed to expand the dictionary: {source}"))?;
    }
    Ok(arrow_array::make_array(expanded.freeze()))
}

/// The average run of ascending keys from which one `MutableArrayData` extend per run beats
/// appending a run's byte values one at a time.
const MIN_RUN_ROWS: usize = 8;

/// Gathers the string or binary `values`, whose offsets and bytes these are, refusing an expansion
/// past `O`'s offsets before copying any. Building the array checks it again, UTF-8 included,
/// which only a constructor without the checks, and so `unsafe`, would skip.
fn gather_bytes<O: OffsetSizeTrait>(
    values: &ArrayRef,
    offsets: &[O],
    bytes: &[u8],
    indices: &[usize],
) -> CastResult {
    let value = |index: usize| {
        let start = offsets.get(index)?.as_usize();
        bytes.get(start..offsets.get(index + 1)?.as_usize())
    };
    let mut length = 0_usize;
    let mut runs = 0_usize;
    let mut previous = NULL_ROW;
    for &index in indices {
        if index != NULL_ROW {
            let value = value(index).ok_or_else(out_of_range)?;
            length = length.checked_add(value.len()).ok_or_else(out_of_range)?;
            // `previous + 1` cannot overflow: a value index is below the value count.
            runs += usize::from(previous == NULL_ROW || index != previous + 1);
        }
        previous = index;
    }
    if O::from_usize(length).is_none() {
        return Err(format!(
            "{length} bytes of expanded dictionary values do not fit {}-bit offsets",
            size_of::<O>() * 8
        ));
    }
    if runs.saturating_mul(MIN_RUN_ROWS) <= indices.len() {
        return gather_runs(values, indices);
    }
    let mut gathered_offsets = Vec::with_capacity(indices.len() + 1);
    let mut gathered = Vec::with_capacity(length);
    gathered_offsets.push(O::usize_as(0));
    for &index in indices {
        if index != NULL_ROW {
            gathered.extend_from_slice(value(index).unwrap_or_default());
        }
        // Every prefix of the checked total fits `O`.
        gathered_offsets.push(O::usize_as(gathered.len()));
    }
    ArrayData::builder(values.data_type().clone())
        .len(indices.len())
        .add_buffer(Buffer::from_vec(gathered_offsets))
        .add_buffer(Buffer::from_vec(gathered))
        .nulls(gathered_nulls(indices))
        .build()
        .map(arrow_array::make_array)
        .map_err(|source| format!("failed to expand the dictionary: {source}"))
}

/// The NULL rows of a gather by `indices`.
fn gathered_nulls(indices: &[usize]) -> Option<NullBuffer> {
    // Sixty-four rows a word, the first in the lowest bit, as Arrow's bitmaps order them.
    let words = indices
        .chunks(64)
        .map(|rows| {
            rows.iter()
                .rev()
                .fold(0_u64, |word, &index| {
                    word << 1 | u64::from(index != NULL_ROW)
                })
                .to_le()
        })
        .collect::<Vec<_>>();
    let valid = BooleanBuffer::new(Buffer::from_vec(words), 0, indices.len());
    Some(NullBuffer::new(valid)).filter(|nulls| nulls.null_count() > 0)
}

/// Gathers primitive values, which share one layout per byte width whatever their type, copying
/// each run of ascending value indices at once.
fn gather_fixed_width(values: &ArrayRef, indices: &[usize]) -> CastResult {
    let data = values.to_data();
    let width = data.data_type().primitive_width().ok_or_else(unsupported)?;
    let source = data
        .buffers()
        .first()
        .and_then(|buffer| buffer.as_slice().get(data.offset().checked_mul(width)?..))
        .ok_or_else(out_of_range)?;
    let values_at = |index: usize, count: usize| {
        source.get(index.checked_mul(width)?..index.checked_add(count)?.checked_mul(width)?)
    };
    let mut gathered =
        MutableBuffer::new(indices.len().checked_mul(width).ok_or_else(out_of_range)?);
    let mut row = 0;
    while let Some(&index) = indices.get(row) {
        if index == NULL_ROW {
            gathered.extend_zeros(width);
            row += 1;
            continue;
        }
        // A lone index, as scattered keys make every one, skips measuring a run.
        let length = if indices.get(row + 1) == Some(&(index + 1)) {
            run_length(indices, row)
        } else {
            1
        };
        gathered.extend_from_slice(values_at(index, length).ok_or_else(out_of_range)?);
        row += length;
    }
    ArrayData::builder(values.data_type().clone())
        .len(indices.len())
        .add_buffer(gathered.into())
        .nulls(gathered_nulls(indices))
        .build()
        .map(arrow_array::make_array)
        .map_err(|source| format!("failed to expand the dictionary: {source}"))
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

/// Widens a decimal into `T`, padding it to `scale` and validating every value against
/// `precision`. Only a Storage Read column gets here: ingest rescales decimals to their field's
/// declared shape itself (`rescale_decimal_column`).
///
/// A target whose physical domain outruns its declared precision is not validated, because no
/// precision expresses that domain; see `spans_full_physical_domain`.
fn to_decimal<S, T>(column: &ArrayRef, source_scale: i8, precision: u8, scale: i8) -> CastResult
where
    S: DecimalType,
    T: DecimalType<Native: From<i32> + From<S::Native>>,
{
    let scale_up =
        u32::try_from(i16::from(scale) - i16::from(source_scale)).map_err(|_| unsupported())?;
    let multiplier = T::Native::from(10_i32)
        .pow_checked(scale_up)
        .map_err(|_| format!("decimal scale increase {scale_up} overflows {}", T::PREFIX))?;
    let validate = !spans_full_physical_domain(precision, scale);
    let invalid_precision = |_| format!("value exceeds decimal precision {precision}");
    // A same-width source at the target scale is only relabelled: validate in place and reuse
    // the buffers. As in the rescaling kernel, only non-null values are validated, since a null
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
    let values = column.as_primitive::<S>().try_unary::<_, T, _>(|value| {
        let value = T::Native::from(value)
            .mul_checked(multiplier)
            .map_err(|_| out_of_range())?;
        if validate {
            T::validate_decimal_precision(value, precision, scale).map_err(invalid_precision)?;
        }
        Ok::<_, String>(value)
    })?;
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
