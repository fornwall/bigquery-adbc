//! Converting decimals to their column's type exactly.
//!
//! A decimal column is written as NUMERIC's `Decimal128` or BIGNUMERIC's `Decimal256`, and its
//! values must fit the shape its field declares: the precision and scale of its Arrow type, or
//! `BIGQUERY:precision` and `BIGQUERY:scale` when the column is narrower than its wire type (an
//! array element, which Storage Write reads at its type's full scale, or a value appended into a
//! parameterized column). Storage Write rounds a value to its column's scale without complaint,
//! so a value that does not fit exactly is refused here instead.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::types::{
    ArrowPrimitiveType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_buffer::i256;
use arrow_schema::{DECIMAL256_MAX_PRECISION, DataType, Field};

use super::site::ValueSite;
use super::{downcast_array, normalized_array_type_error};
use crate::error::error;
use crate::types::decimal::spans_full_physical_domain;
use crate::{BIGQUERY_FIELD_METADATA_PRECISION, BIGQUERY_FIELD_METADATA_SCALE};

/// The precision and scale a decimal field's values must fit, and the scale they are written at.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Target {
    precision: u8,
    scale: i8,
    /// The field type's scale, which is never below `scale`.
    written_scale: i8,
    numeric: bool,
    /// Whether `precision` bounds the values. Only plain BIGNUMERIC's type reaches past its
    /// declared 76 digits (see `spans_full_physical_domain`); a column declaring
    /// `BIGNUMERIC(76, 38)` stops at 38 integer digits.
    bounded: bool,
}

impl Target {
    /// The shape `field` declares in its metadata, which schema normalization validated, else
    /// its type's; `None` for a field that is not NUMERIC's or BIGNUMERIC's type.
    fn of(field: &Field) -> Option<Self> {
        let (numeric, precision, scale) = match field.data_type() {
            DataType::Decimal128(precision, scale) => (true, *precision, *scale),
            DataType::Decimal256(precision, scale) => (false, *precision, *scale),
            _ => return None,
        };
        let metadata = field.metadata();
        let declared = metadata
            .get(BIGQUERY_FIELD_METADATA_PRECISION)
            .and_then(|value| value.parse().ok())
            .map(|precision| {
                let scale = metadata
                    .get(BIGQUERY_FIELD_METADATA_SCALE)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
                (precision, scale)
            });
        let (declared_precision, declared_scale) = declared.unwrap_or((precision, scale));
        Some(Self {
            precision: declared_precision,
            scale: declared_scale,
            written_scale: scale,
            numeric,
            bounded: declared.is_some()
                || !spans_full_physical_domain(DECIMAL256_MAX_PRECISION, precision, scale),
        })
    }

    /// Whether the field type alone checks what the declared shape requires: the same precision
    /// and scale, bounded the same way.
    fn is_own_shape(self, field: &Field) -> bool {
        matches!(
            field.data_type(),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)
                if (self.precision, self.scale) == (*precision, *scale)
                    && self.bounded
                        == !spans_full_physical_domain(DECIMAL256_MAX_PRECISION, *precision, *scale)
        )
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let family = if self.numeric {
            "NUMERIC"
        } else {
            "BIGNUMERIC"
        };
        write!(formatter, "{family}({}, {})", self.precision, self.scale)
    }
}

/// Whether a column of type `source` needs [`rescale_decimal_column`] to become `field`'s: it is
/// a decimal of another type, or `field` declares a narrower shape than its type.
pub(super) fn needs_rescale(source: &DataType, field: &Field) -> bool {
    matches!(
        source,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) && Target::of(field)
        .is_some_and(|target| source != field.data_type() || !target.is_own_shape(field))
}

/// Converts a decimal column to `field`'s type, refusing a value that does not fit the field's
/// declared shape exactly.
pub(super) fn rescale_decimal_column(
    column: &ArrayRef,
    field: &Field,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    let (Some(target), Some(source_scale)) = (Target::of(field), decimal_scale(column.data_type()))
    else {
        return Err(normalized_array_type_error(site.name(), column.data_type()));
    };
    let rescale = Rescale::new(source_scale, target);
    let failed = |index: usize, misfit: Misfit| misfit_error(misfit, target, site, index);
    match field.data_type() {
        DataType::Decimal128(precision, scale) => rescale_values::<Decimal128Type>(
            column,
            (*precision, *scale),
            &rescale,
            site,
            failed,
            |value| value.to_i128(),
        ),
        DataType::Decimal256(precision, scale) => rescale_values::<Decimal256Type>(
            column,
            (*precision, *scale),
            &rescale,
            site,
            failed,
            Some,
        ),
        other => Err(normalized_array_type_error(site.name(), other)),
    }
}

fn decimal_scale(data_type: &DataType) -> Option<i8> {
    match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => Some(*scale),
        _ => None,
    }
}

/// Why a value does not fit its column.
#[derive(Clone, Copy)]
enum Misfit {
    FractionalDigits,
    IntegerDigits,
}

/// The arithmetic that moves one value from its source scale to the target's, precomputed.
/// A power of ten past `i256` leaves a factor `None`, which only zero survives.
struct Rescale {
    /// Divides out the fractional digits the target drops; each must be zero.
    divisor: Option<Option<i256>>,
    /// Pads the fractional digits the target adds.
    multiplier: Option<i256>,
    /// The exclusive bound on the declared-scale value, `None` when the precision bounds nothing.
    limit: Option<Option<i256>>,
    /// Moves a declared-scale value to the written scale.
    widen: Option<i256>,
}

impl Rescale {
    fn new(source_scale: i8, target: Target) -> Self {
        let power = |exponent: i16| {
            u32::try_from(exponent)
                .ok()
                .and_then(|exponent| i256::from_i128(10).checked_pow(exponent))
        };
        let change = i16::from(target.scale) - i16::from(source_scale);
        Self {
            divisor: (change < 0).then(|| power(-change)),
            multiplier: power(change.max(0)),
            // Plain BIGNUMERIC reaches past its declared 76 digits.
            limit: target.bounded.then(|| power(i16::from(target.precision))),
            widen: power(i16::from(target.written_scale) - i16::from(target.scale)),
        }
    }

    fn apply(&self, value: i256) -> std::result::Result<i256, Misfit> {
        let value = match self.divisor {
            Some(divisor) => {
                let divisor = divisor.ok_or(Misfit::FractionalDigits)?;
                let (quotient, remainder) = value
                    .checked_div_rem(divisor)
                    .ok_or(Misfit::IntegerDigits)?;
                if remainder != i256::ZERO {
                    return Err(Misfit::FractionalDigits);
                }
                quotient
            }
            None => self
                .multiplier
                .and_then(|multiplier| value.checked_mul(multiplier))
                .ok_or(Misfit::IntegerDigits)?,
        };
        if let Some(limit) = self.limit {
            let fits = limit.is_some_and(|limit| {
                value < limit && value.checked_neg().is_some_and(|negated| negated < limit)
            });
            if !fits {
                return Err(Misfit::IntegerDigits);
            }
        }
        self.widen
            .and_then(|widen| value.checked_mul(widen))
            .ok_or(Misfit::IntegerDigits)
    }
}

/// Rescales every valid value of `column` into `T`; null slots, whose payloads are arbitrary,
/// are left alone. A failure is located by scanning again for the first value that fails.
fn rescale_values<T>(
    column: &ArrayRef,
    (precision, scale): (u8, i8),
    rescale: &Rescale,
    site: &ValueSite<'_>,
    failed: impl Fn(usize, Misfit) -> Error,
    narrow: impl Fn(i256) -> Option<T::Native> + Copy,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType + arrow_array::types::DecimalType,
    i256: From<T::Native>,
{
    let convert = |value: i256| {
        rescale
            .apply(value)
            .and_then(|value| narrow(value).ok_or(Misfit::IntegerDigits))
    };
    // At the same physical width and written scale, an exactly representable value is already
    // in its wire form. Declared-scale checks can divide and multiply it back, but need not
    // materialize that identical value buffer. Null payloads remain arbitrary and unexamined.
    if decimal_scale(column.data_type()) == Some(scale)
        && let Some(array) = column.as_any().downcast_ref::<PrimitiveArray<T>>()
    {
        for (index, value) in array.iter().enumerate() {
            if let Some(value) = value {
                rescale
                    .apply(value.into())
                    .map_err(|misfit| failed(index, misfit))?;
            }
        }
        if array.precision() == precision {
            return Ok(column.clone());
        }
        return decimal_array(array.clone(), precision, scale, site);
    }
    let values: PrimitiveArray<T> = match column.data_type() {
        DataType::Decimal32(_, _) => {
            convert_each::<Decimal32Type, T>(column, site, &failed, |value| {
                convert(i256::from_i128(i128::from(value)))
            })?
        }
        DataType::Decimal64(_, _) => {
            convert_each::<Decimal64Type, T>(column, site, &failed, |value| {
                convert(i256::from_i128(i128::from(value)))
            })?
        }
        DataType::Decimal128(_, _) => {
            convert_each::<Decimal128Type, T>(column, site, &failed, |value| {
                convert(i256::from_i128(value))
            })?
        }
        _ => convert_each::<Decimal256Type, T>(column, site, &failed, convert)?,
    };
    decimal_array(values, precision, scale, site)
}

fn decimal_array<T: arrow_array::types::DecimalType>(
    values: PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    site: &ValueSite<'_>,
) -> Result<ArrayRef> {
    values
        .with_precision_and_scale(precision, scale)
        .map(|values| Arc::new(values) as ArrayRef)
        .map_err(|source| {
            error(
                format!(
                    "failed to normalize BigQuery ingest decimal field {:?}: {source}",
                    site.name()
                ),
                Status::InvalidData,
            )
        })
}

fn convert_each<S, T>(
    column: &ArrayRef,
    site: &ValueSite<'_>,
    failed: &impl Fn(usize, Misfit) -> Error,
    convert: impl Fn(S::Native) -> std::result::Result<T::Native, Misfit>,
) -> Result<PrimitiveArray<T>>
where
    S: ArrowPrimitiveType,
    T: ArrowPrimitiveType,
{
    let array = downcast_array::<PrimitiveArray<S>>(column, site.name())?;
    array.try_unary::<_, T, _>(&convert).map_err(|misfit| {
        let index = (0..array.len())
            .find(|&index| array.is_valid(index) && convert(array.value(index)).is_err())
            .unwrap_or_default();
        failed(index, misfit)
    })
}

fn misfit_error(misfit: Misfit, target: Target, site: &ValueSite<'_>, index: usize) -> Error {
    let (name, row) = (site.name(), site.row(index));
    let reason = match misfit {
        Misfit::FractionalDigits => format!(
            "more than {} fractional digits, which its column's {target} cannot store without \
             rounding",
            target.scale
        ),
        Misfit::IntegerDigits => format!("more integer digits than its column's {target} holds"),
    };
    error(
        format!("Arrow decimal ingest field {name:?} holds a value at row {row} with {reason}"),
        Status::InvalidData,
    )
}

#[cfg(test)]
mod tests;
