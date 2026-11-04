//! Shared Arrow decimal and BigQuery decimal type semantics.

use crate::types::{BigQueryType, googlesql_spelling_of};
use arrow_schema::{
    DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType,
};

pub(crate) const NUMERIC_PRECISION: u8 = 38;
pub(crate) const NUMERIC_SCALE: i8 = 9;
const NUMERIC_INTEGER_DIGITS: u8 = 29;
pub(crate) const BIGNUMERIC_PRECISION: u8 = 76;
pub(crate) const BIGNUMERIC_SCALE: i8 = 38;
const BIGNUMERIC_INTEGER_DIGITS: u8 = 38;

/// The significant-digit budget for values carried by an Arrow decimal array.
///
/// Arrow's maximum `Decimal256` precision is 76, but a parameter-free BigQuery BIGNUMERIC spans
/// the complete `i256` domain: 39 integer and 38 fractional digits. The canonical Arrow
/// `Decimal256(76, 38)` representation therefore understates its own physical domain by one digit.
/// Every other decimal shape remains bounded by its declared precision.
pub(crate) const fn decimal_value_digit_budget(
    maximum_precision: u8,
    precision: u8,
    scale: i8,
) -> u8 {
    if maximum_precision == DECIMAL256_MAX_PRECISION
        && precision == BIGNUMERIC_PRECISION
        && scale == BIGNUMERIC_SCALE
    {
        BIGNUMERIC_PRECISION + 1
    } else {
        precision
    }
}

/// Whether an Arrow decimal type's real value domain is wider than the precision it declares.
///
/// Only a parameter-free BIGNUMERIC is; see [`decimal_value_digit_budget`].
/// `validate_decimal_precision` refuses precisions above `DECIMAL256_MAX_PRECISION`, so validators
/// skip the check for that shape rather than reject `i256` values BigQuery stores.
pub(crate) const fn spans_full_physical_domain(
    maximum_precision: u8,
    precision: u8,
    scale: i8,
) -> bool {
    decimal_value_digit_budget(maximum_precision, precision, scale) > precision
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BigQueryDecimalType {
    Numeric,
    BigNumeric,
}

impl BigQueryDecimalType {
    /// Parses caller-supplied `BIGQUERY:type` metadata, which uses the GoogleSQL names the driver
    /// emits. BigQuery's `DECIMAL` and `BIGDECIMAL` aliases are not among them.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match BigQueryType::from_sql(name)? {
            BigQueryType::Numeric => Some(Self::Numeric),
            BigQueryType::BigNumeric => Some(Self::BigNumeric),
            _ => None,
        }
    }

    pub(crate) const fn bigquery_type(self) -> BigQueryType {
        match self {
            Self::Numeric => BigQueryType::Numeric,
            Self::BigNumeric => BigQueryType::BigNumeric,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        self.bigquery_type().sql()
    }

    const fn maximum_precision(self) -> u8 {
        match self {
            Self::Numeric => NUMERIC_PRECISION,
            Self::BigNumeric => BIGNUMERIC_PRECISION,
        }
    }

    const fn maximum_scale(self) -> i8 {
        match self {
            Self::Numeric => NUMERIC_SCALE,
            Self::BigNumeric => BIGNUMERIC_SCALE,
        }
    }

    fn contains(self, precision: u8, scale: i8) -> bool {
        let maximum_integer_digits = match self {
            Self::Numeric => NUMERIC_INTEGER_DIGITS,
            Self::BigNumeric => BIGNUMERIC_INTEGER_DIGITS,
        };
        scale >= 0
            && scale <= self.maximum_scale()
            && precision <= self.maximum_precision()
            && i16::from(precision) - i16::from(scale) <= i16::from(maximum_integer_digits)
    }

    pub(crate) const fn storage_data_type(self, precision: u8, scale: i8) -> DataType {
        match self {
            Self::Numeric => DataType::Decimal128(precision, scale),
            Self::BigNumeric => DataType::Decimal256(precision, scale),
        }
    }
}

pub(crate) fn is_decimal_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => true,
        DataType::Dictionary(key_type, value_type) if key_type.is_dictionary_key_type() => {
            is_decimal_type(value_type)
        }
        _ => false,
    }
}

/// Resolves decimal type metadata on a decimal field or its enclosing array field.
pub(crate) fn decimal_type_hint(
    data_type: &DataType,
    metadata: Option<&str>,
    inherited: Option<BigQueryDecimalType>,
) -> Result<Option<BigQueryDecimalType>, String> {
    let metadata_type = if is_decimal_type(data_type) {
        metadata.map(|name| {
            BigQueryDecimalType::from_name(name)
                .ok_or_else(|| decimal_metadata_error(name, data_type, false))
        })
    } else if decimal_array_item_type(data_type).is_some_and(is_decimal_type) {
        metadata.map(|name| {
            name.strip_prefix("ARRAY<")
                .and_then(|name| name.strip_suffix('>'))
                .and_then(BigQueryDecimalType::from_name)
                .ok_or_else(|| decimal_metadata_error(name, data_type, true))
        })
    } else {
        None
    }
    .transpose()?;

    match (inherited, metadata_type) {
        (Some(inherited), Some(metadata)) if inherited != metadata => Err(format!(
            "BigQuery decimal type metadata conflicts with inherited {} array metadata",
            inherited.name()
        )),
        (Some(inherited), _) => Ok(Some(inherited)),
        (_, metadata) => Ok(metadata),
    }
}

/// Explains rejected decimal metadata, naming the GoogleSQL spelling when the value is only a name
/// BigQuery accepts elsewhere but this driver never emits, such as `DECIMAL` for `NUMERIC`.
fn decimal_metadata_error(name: &str, data_type: &DataType, array: bool) -> String {
    let element = if array {
        name.strip_prefix("ARRAY<")
            .and_then(|value| value.strip_suffix('>'))
            .unwrap_or(name)
    } else {
        name
    };
    match googlesql_spelling_of(element) {
        Some(googlesql) if array => format!(
            "BigQuery type metadata {name:?} is not a GoogleSQL type name; use \"ARRAY<{googlesql}>\""
        ),
        Some(googlesql) => {
            format!(
                "BigQuery type metadata {name:?} is not a GoogleSQL type name; use {googlesql:?}"
            )
        }
        None if array => format!(
            "BigQuery type metadata {name:?} is incompatible with Arrow decimal array type {data_type}"
        ),
        None => format!(
            "BigQuery type metadata {name:?} is incompatible with Arrow decimal type {data_type}"
        ),
    }
}

fn decimal_array_item_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => Some(item.data_type()),
        _ => None,
    }
}

/// The BigQuery logical type and canonical Storage Write representation of an Arrow decimal.
///
/// Arrow permits negative scales. BigQuery does not, so those values are represented exactly by
/// moving the implied trailing zeroes into the unscaled integer and using scale zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DecimalMapping {
    pub(crate) bigquery_type: BigQueryDecimalType,
    pub(crate) precision: u8,
    pub(crate) scale: i8,
}

impl DecimalMapping {
    pub(crate) const fn storage_data_type(self) -> DataType {
        self.bigquery_type
            .storage_data_type(self.precision, self.scale)
    }

    /// Whether declaring this mapping's precision and scale on a BigQuery column would make the
    /// column narrower than its Arrow values, so the parameter-free spelling must be emitted.
    ///
    /// Only `BIGNUMERIC(76, 38)` does: the parameterized type stops at 38 integer digits (~1e38),
    /// while a parameter-free BIGNUMERIC reaches ~5.79e38; see [`decimal_value_digit_budget`].
    /// `NUMERIC(38, 9)` is exactly plain NUMERIC's domain.
    pub(crate) const fn narrows_its_arrow_domain(self) -> bool {
        spans_full_physical_domain(DECIMAL256_MAX_PRECISION, self.precision, self.scale)
    }
}

/// Classifies an Arrow decimal by its value domain, independently of its physical bit width.
///
/// A requested logical type, normally supplied by `BIGQUERY:type` metadata, is retained when it
/// can represent the complete Arrow value domain. Without one, the narrowest exact BigQuery type
/// is selected.
pub(crate) fn decimal_mapping(
    data_type: &DataType,
    requested_type: Option<BigQueryDecimalType>,
) -> Result<DecimalMapping, String> {
    let (precision, scale, maximum_precision, maximum_scale) = match data_type {
        DataType::Decimal32(precision, scale) => (
            *precision,
            *scale,
            DECIMAL32_MAX_PRECISION,
            DECIMAL32_MAX_SCALE,
        ),
        DataType::Decimal64(precision, scale) => (
            *precision,
            *scale,
            DECIMAL64_MAX_PRECISION,
            DECIMAL64_MAX_SCALE,
        ),
        DataType::Decimal128(precision, scale) => (
            *precision,
            *scale,
            DECIMAL128_MAX_PRECISION,
            DECIMAL128_MAX_SCALE,
        ),
        DataType::Decimal256(precision, scale) => (
            *precision,
            *scale,
            DECIMAL256_MAX_PRECISION,
            DECIMAL256_MAX_SCALE,
        ),
        _ => return Err(format!("Arrow type {data_type} is not a decimal")),
    };

    if precision == 0
        || precision > maximum_precision
        || scale > maximum_scale
        || (scale > 0 && i16::from(scale) > i16::from(precision))
    {
        return Err(format!(
            "Arrow decimal type {data_type} has invalid precision or scale for its physical width"
        ));
    }

    let scale_up = scale.min(0).unsigned_abs();
    let scale = scale.max(0);
    // At most 76 + 128 digits, so this cannot saturate; a saturated value would fail below anyway.
    let precision = precision.saturating_add(scale_up);
    let inferred_type = if BigQueryDecimalType::Numeric.contains(precision, scale) {
        BigQueryDecimalType::Numeric
    } else if BigQueryDecimalType::BigNumeric.contains(precision, scale) {
        BigQueryDecimalType::BigNumeric
    } else {
        return Err(format!(
            "Arrow decimal type {data_type} exceeds BigQuery NUMERIC (29 integer and 9 fractional digits) and BIGNUMERIC (38 integer and 38 fractional digits)"
        ));
    };
    let bigquery_type = if let Some(requested_type) = requested_type {
        if !requested_type.contains(precision, scale) {
            return Err(format!(
                "Arrow decimal type {data_type} does not fit requested BigQuery {}",
                requested_type.name()
            ));
        }
        requested_type
    } else {
        inferred_type
    };

    Ok(DecimalMapping {
        bigquery_type,
        precision,
        scale,
    })
}

/// Validates a BigQuery decimal field and returns its canonical Arrow result representation.
pub(crate) fn bigquery_decimal_data_type(
    bigquery_type: BigQueryDecimalType,
    precision: i64,
    scale: i64,
) -> Result<DataType, String> {
    // A parameter-free NUMERIC or BIGNUMERIC field reports zero for both and means the maximums.
    let (precision, scale) = if precision == 0 && scale == 0 {
        (
            bigquery_type.maximum_precision(),
            bigquery_type.maximum_scale(),
        )
    } else {
        (
            u8::try_from(precision).map_err(|_| {
                format!(
                    "precision must be between 1 and {}",
                    bigquery_type.maximum_precision()
                )
            })?,
            i8::try_from(scale).map_err(|_| {
                format!(
                    "scale must be between 0 and {}",
                    bigquery_type.maximum_scale()
                )
            })?,
        )
    };
    if precision == 0
        || (scale > 0 && i16::from(scale) > i16::from(precision))
        || !bigquery_type.contains(precision, scale)
    {
        return Err(format!(
            "precision {precision} and scale {scale} do not fit {}",
            bigquery_type.name()
        ));
    }
    Ok(bigquery_type.storage_data_type(precision, scale))
}

#[cfg(test)]
mod tests;
