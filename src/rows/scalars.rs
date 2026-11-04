//! The value parsers: one BigQuery REST spelling of a scalar, straight into its Arrow value.
//!
//! Each function takes the text BigQuery sent and returns the value Arrow stores, without
//! touching builders, REST values or the schema, so scalar columns and RANGE bounds share one
//! grammar.

use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::{ArrowError, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};

use crate::types::decimal::decimal_value_digit_budget;
use crate::types::temporal::{civil_from_days, days_from_civil};

/// The longest decimal response accepted: a sign, the 77 digits of the widest BIGNUMERIC, a
/// decimal point, and an `E+38` exponent for the scientific form.
const MAX_DECIMAL_RESPONSE_LENGTH: usize = 1 + 77 + 1 + 4;

/// Converts the civil time of day of a `kind` value (TIME or DATETIME) to microseconds since
/// midnight. GoogleSQL has no leap second, so an hour of 24 or a second of 60 is a malformed
/// response, not a value to fold into the day.
fn time_of_day_micros(
    kind: &str,
    hours: i64,
    minutes: i64,
    seconds: i64,
    nanos: i64,
) -> Result<i64, ArrowError> {
    if !(0..24).contains(&hours)
        || !(0..60).contains(&minutes)
        || !(0..60).contains(&seconds)
        || !(0..1_000_000_000).contains(&nanos)
    {
        return Err(ArrowError::ParseError(format!(
            "BigQuery {kind} result value has a time of day outside 00:00:00 to 23:59:59.999999"
        )));
    }
    Ok((hours * 3_600 + minutes * 60 + seconds) * 1_000_000 + nanos / 1_000)
}

/// Splits the leading `-` that an INTERVAL's year-month, day, and time parts each carry for all of
/// their own components.
fn split_interval_sign(part: &[u8]) -> (bool, &[u8]) {
    match part.split_first() {
        Some((b'-', unsigned)) => (true, unsigned),
        _ => (false, part),
    }
}

/// Splits `bytes` around the first byte `is_separator` accepts, which the two halves exclude.
fn split_at_byte(bytes: &[u8], is_separator: impl Fn(u8) -> bool) -> Option<(&[u8], &[u8])> {
    let index = bytes.iter().position(|byte| is_separator(*byte))?;
    let (head, tail) = bytes.split_at_checked(index)?;
    Some((head, tail.get(1..)?))
}

/// [`split_at_byte`] around one fixed separator.
fn split_byte(bytes: &[u8], separator: u8) -> Option<(&[u8], &[u8])> {
    split_at_byte(bytes, |byte| byte == separator)
}

/// Splits an optional leading `-` or `+` off `bytes`, answering whether it was a `-`.
fn split_sign(bytes: &[u8]) -> (bool, &[u8]) {
    match bytes.split_first() {
        Some((b'-', unsigned)) => (true, unsigned),
        Some((b'+', unsigned)) => (false, unsigned),
        _ => (false, bytes),
    }
}

/// Parses a nonempty run of ASCII digits as a nonnegative `i64`. Unlike `str::parse` it rejects
/// a sign, which in BigQuery's temporal spellings belongs to the whole part it precedes.
fn parse_digits(digits: &[u8]) -> Option<i64> {
    if digits.is_empty() {
        return None;
    }
    let mut value = 0_i64;
    for byte in digits {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(value)
}

/// Parses an optionally signed run of ASCII digits as an `i64`, the spelling of a REST INT64 and
/// of the integer-microsecond TIMESTAMP: the grammar of `str::parse::<i64>`, accumulated in the
/// sign's direction so `i64::MIN` parses without forming its magnitude.
pub(super) fn parse_signed(value: &[u8]) -> Option<i64> {
    let (negative, digits) = split_sign(value);
    if digits.is_empty() {
        return None;
    }
    let mut parsed = 0_i64;
    for byte in digits {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        let shifted = parsed.checked_mul(10)?;
        parsed = if negative {
            shifted.checked_sub(i64::from(digit))?
        } else {
            shifted.checked_add(i64::from(digit))?
        };
    }
    Some(parsed)
}

/// Scales fractional-second digits to nanoseconds, truncating past the ninth.
fn parse_subsecond_nanos(fraction: &[u8]) -> Option<i64> {
    if fraction.is_empty() {
        return None;
    }
    let mut nanoseconds = 0_i64;
    let mut scaled = 0_u32;
    for byte in fraction {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        // Digits past the ninth are validated and dropped, which is the truncation this scale
        // performs; the ones short of it are made up by the trailing shift.
        if scaled < 9 {
            nanoseconds = nanoseconds * 10 + i64::from(digit);
            scaled += 1;
        }
    }
    Some(nanoseconds * 10_i64.pow(9 - scaled))
}

/// Splits the `H:M:S[.F]` clock part that BigQuery's TIME, DATETIME and INTERVAL all share into
/// unbounded `(hours, minutes, seconds, nanoseconds)`: an INTERVAL's hours routinely exceed 24,
/// and only a TIME's fields are zero-padded.
fn split_clock(value: &[u8]) -> Option<(i64, i64, i64, i64)> {
    let (hours, rest) = split_byte(value, b':')?;
    let (minutes, seconds) = split_byte(rest, b':')?;
    let (seconds, fraction) = match split_byte(seconds, b'.') {
        Some((seconds, fraction)) => (seconds, Some(fraction)),
        None => (seconds, None),
    };
    Some((
        parse_digits(hours)?,
        parse_digits(minutes)?,
        parse_digits(seconds)?,
        match fraction {
            Some(fraction) => parse_subsecond_nanos(fraction)?,
            None => 0,
        },
    ))
}

/// Splits BigQuery's fixed-width `YYYY-MM-DD` date spelling into its three components.
fn split_civil_date(value: &[u8]) -> Option<(i64, i64, i64)> {
    if value.len() != 10 || value.get(4) != Some(&b'-') || value.get(7) != Some(&b'-') {
        return None;
    }
    Some((
        parse_digits(value.get(..4)?)?,
        parse_digits(value.get(5..7)?)?,
        parse_digits(value.get(8..)?)?,
    ))
}

/// The days since the Unix epoch of a civil date BigQuery can hold, or `None` for one it cannot.
/// A nonexistent date is caught by converting back: `2021-02-30` returns as `2021-03-02`.
fn civil_date_to_days(year: i64, month: i64, day: i64) -> Option<i32> {
    // BigQuery DATE and DATETIME span 0001-01-01 to 9999-12-31.
    if !(1..=9_999).contains(&year) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    if civil_from_days(days) != (year, month, day) {
        return None;
    }
    i32::try_from(days).ok()
}

/// Refuses a scalar the result schema says should have parsed, naming the type and not the
/// server-supplied, unbounded value; the caller attaches the column.
#[cold]
#[inline(never)]
pub(super) fn invalid_scalar(kind: &str) -> ArrowError {
    ArrowError::ParseError(format!("BigQuery result value is not a valid {kind}"))
}

/// The epoch microseconds of `0001-01-01T00:00:00Z`, the first instant a BigQuery TIMESTAMP holds.
const MIN_TIMESTAMP_MICROS: i64 = -62_135_596_800_000_000;

/// The epoch microseconds of `9999-12-31T23:59:59.999999Z`, the last one.
const MAX_TIMESTAMP_MICROS: i64 = 253_402_300_799_999_999;

/// Parses BigQuery's integer-microsecond TIMESTAMP spelling (requested with `useInt64Timestamp`,
/// which also covers `RANGE<TIMESTAMP>` bounds) into epoch microseconds, rejecting instants outside
/// years 0001-9999 like every other temporal column.
pub(super) fn parse_timestamp(value: &[u8]) -> Result<i64, ArrowError> {
    let micros = parse_signed(value).ok_or_else(|| invalid_scalar("TIMESTAMP"))?;
    if !(MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS).contains(&micros) {
        return Err(invalid_scalar("TIMESTAMP"));
    }
    Ok(micros)
}

/// Parses BigQuery's `YYYY-MM-DD` DATE spelling into days since the Unix epoch.
pub(super) fn parse_date(value: &[u8]) -> Result<i32, ArrowError> {
    split_civil_date(value)
        .and_then(|(year, month, day)| civil_date_to_days(year, month, day))
        .ok_or_else(|| invalid_scalar("DATE"))
}

/// Parses BigQuery's `H:M:S[.F]` TIME spelling into microseconds since midnight.
pub(super) fn parse_time_of_day(value: &[u8]) -> Result<i64, ArrowError> {
    let (hours, minutes, seconds, nanos) =
        split_clock(value).ok_or_else(|| invalid_scalar("TIME"))?;
    time_of_day_micros("TIME", hours, minutes, seconds, nanos)
}

/// Parses BigQuery's `YYYY-MM-DDTH:M:S[.F]` DATETIME spelling into microseconds since the epoch.
pub(super) fn parse_datetime(value: &[u8]) -> Result<i64, ArrowError> {
    let (date, clock) = split_byte(value, b'T').ok_or_else(|| invalid_scalar("DATETIME"))?;
    let days = split_civil_date(date)
        .and_then(|(year, month, day)| civil_date_to_days(year, month, day))
        .ok_or_else(|| invalid_scalar("DATETIME"))?;
    let (hours, minutes, seconds, nanos) =
        split_clock(clock).ok_or_else(|| invalid_scalar("DATETIME"))?;
    let time = time_of_day_micros("DATETIME", hours, minutes, seconds, nanos)?;
    // A year of at most 9999 keeps the day count four orders of magnitude short of overflowing
    // the microsecond scale; the arithmetic stays checked because nothing here enforces that.
    i64::from(days)
        .checked_mul(86_400_000_000)
        .and_then(|day_micros| day_micros.checked_add(time))
        .ok_or_else(|| invalid_scalar("DATETIME"))
}

/// Parses BigQuery's INTERVAL spelling, `[-]Y-M [-]D [-]H:M:S[.F]`, into Arrow's
/// month/day/nanosecond interval. BigQuery puts a timestamp difference in the hours, never in
/// days: `TIMESTAMP '2020-02-01' - TIMESTAMP '2020-01-01'` is `0-0 0 744:0:0`.
pub(super) fn parse_interval(value: &[u8]) -> Result<IntervalMonthDayNano, ArrowError> {
    let invalid = || invalid_scalar("INTERVAL");
    // Drop the empty parts `[u8]::split` yields for repeated, leading or trailing separators, as
    // `str::split_ascii_whitespace` does.
    let mut parts = value
        .split(u8::is_ascii_whitespace)
        .filter(|part| !part.is_empty());
    let (Some(year_month), Some(day), Some(time), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };

    let (year_month_negative, year_month) = split_interval_sign(year_month);
    let (years, months) = split_byte(year_month, b'-').ok_or_else(invalid)?;
    let years = parse_digits(years).ok_or_else(invalid)?;
    let months = parse_digits(months).ok_or_else(invalid)?;
    let months = years
        .checked_mul(12)
        .and_then(|years| years.checked_add(months))
        .map(|months| if year_month_negative { -months } else { months })
        .and_then(|months| i32::try_from(months).ok())
        .ok_or_else(|| {
            ArrowError::ParseError("BigQuery INTERVAL months overflow i32".to_owned())
        })?;

    let (day_negative, day) = split_interval_sign(day);
    let days = parse_digits(day).ok_or_else(invalid)?;
    let days = i32::try_from(if day_negative { -days } else { days })
        .map_err(|_| ArrowError::ParseError("BigQuery INTERVAL days overflow i32".to_owned()))?;

    let (time_negative, time) = split_interval_sign(time);
    let (hours, minutes, seconds, subsecond) = split_clock(time).ok_or_else(invalid)?;
    let nanoseconds = hours
        .checked_mul(3_600)
        .and_then(|hours| minutes.checked_mul(60).and_then(|m| hours.checked_add(m)))
        .and_then(|elapsed| elapsed.checked_add(seconds))
        .and_then(|elapsed| elapsed.checked_mul(1_000_000_000))
        .and_then(|elapsed| elapsed.checked_add(subsecond))
        .map(|elapsed| if time_negative { -elapsed } else { elapsed })
        .ok_or_else(|| {
            // A legitimate BigQuery INTERVAL can exceed Arrow's i64-nanosecond time component.
            // Re-spelling the excess hours as days would change EXTRACT(DAY)/EXTRACT(HOUR), so
            // name the remedies instead. The value is not echoed: its fraction is unbounded.
            ArrowError::ParseError(
                "BigQuery INTERVAL result value does not fit Arrow's INTERVAL, whose time part \
                 stops at 2562047:47:16.854775807; select it as CAST(x AS STRING) or as the \
                 EXTRACT parts you need"
                    .to_owned(),
            )
        })?;

    Ok(IntervalMonthDayNano::new(months, days, nanoseconds))
}

/// How many decimal digits one accumulator chunk holds, the largest power of ten a `u64` carries.
const CHUNK_DIGITS: u32 = 18;

/// `10^index` for every chunk width, so a shift is a table read rather than a `pow` loop.
#[expect(
    clippy::indexing_slicing,
    reason = "an out-of-bounds index in a const initializer fails the build, not the driver"
)]
const POWERS_OF_TEN: [i128; CHUNK_DIGITS as usize + 1] = {
    let mut powers = [1; CHUNK_DIGITS as usize + 1];
    let mut index = 1;
    while index < powers.len() {
        powers[index] = powers[index - 1] * 10;
        index += 1;
    }
    powers
};

/// An integer wide enough for a scaled BigQuery decimal, accumulated a chunk of digits at a time.
pub(super) trait DecimalDigits: Copy {
    /// The BigQuery type this width decodes. The driver maps NUMERIC to `Decimal128` and
    /// BIGNUMERIC to `Decimal256` and nothing else to either, so the width names the type.
    const BIGQUERY_TYPE: &'static str;
    /// The widest precision Arrow declares for this width, which is what
    /// [`decimal_value_digit_budget`] needs to recognize a parameter-free BIGNUMERIC.
    const MAX_PRECISION: u8;
    const ZERO: Self;
    /// `self * 10^width + chunk`, or `self * 10^width - chunk` when `negative`, or `None` on
    /// overflow. `width` is at most [`CHUNK_DIGITS`] and `chunk` holds that many decimal digits.
    ///
    /// A negative value accumulates negatively, because the most negative BIGNUMERIC is exactly
    /// `i256::MIN`, whose magnitude is not an `i256`.
    ///
    /// Chunking keeps wide decimals cheap: a checked `i256` multiply-add per digit costs four
    /// 128-bit multiplies, while eighteen digits accumulate in a `u64` and reach the wide
    /// accumulator once. The result is the same: a magnitude only grows as digits are appended, so
    /// an overflow inside a chunk also overflows at the chunk boundary.
    fn push_chunk(self, chunk: u64, width: u32, negative: bool) -> Option<Self>;
}

impl DecimalDigits for i128 {
    const BIGQUERY_TYPE: &'static str = "NUMERIC";
    const MAX_PRECISION: u8 = DECIMAL128_MAX_PRECISION;
    const ZERO: Self = 0;

    fn push_chunk(self, chunk: u64, width: u32, negative: bool) -> Option<Self> {
        let shifted = self.checked_mul(*POWERS_OF_TEN.get(width as usize)?)?;
        if negative {
            shifted.checked_sub(Self::from(chunk))
        } else {
            shifted.checked_add(Self::from(chunk))
        }
    }
}

impl DecimalDigits for i256 {
    const BIGQUERY_TYPE: &'static str = "BIGNUMERIC";
    const MAX_PRECISION: u8 = DECIMAL256_MAX_PRECISION;
    const ZERO: Self = Self::ZERO;

    fn push_chunk(self, chunk: u64, width: u32, negative: bool) -> Option<Self> {
        let shifted = self.checked_mul(Self::from_i128(*POWERS_OF_TEN.get(width as usize)?))?;
        let chunk = Self::from_i128(i128::from(chunk));
        if negative {
            shifted.checked_sub(chunk)
        } else {
            shifted.checked_add(chunk)
        }
    }
}

/// Parses a BigQuery decimal response directly into an integer scaled by `10^scale`.
///
/// A value is measured against [`decimal_value_digit_budget`] of the column's declared
/// `precision`, which is one digit wider for a parameter-free BIGNUMERIC, whose
/// `Decimal256(76, 38)` understates its `i256` domain.
pub(super) fn parse_scaled_decimal<T: DecimalDigits>(
    value: &[u8],
    precision: u8,
    scale: i8,
) -> Result<T, ArrowError> {
    let budget = decimal_value_digit_budget(T::MAX_PRECISION, precision, scale);
    // `trim_ascii` suffices: BigQuery pads decimals only with ASCII spaces, and the grammar below
    // rejects non-ASCII bytes.
    let value = value.trim_ascii();
    if value.len() > MAX_DECIMAL_RESPONSE_LENGTH {
        return Err(ArrowError::ParseError(format!(
            "BigQuery {} response exceeds {MAX_DECIMAL_RESPONSE_LENGTH} bytes",
            T::BIGQUERY_TYPE
        )));
    }
    let (negative, unsigned) = split_sign(value);
    let (coefficient, exponent) = match split_at_byte(unsigned, |byte| matches!(byte, b'e' | b'E'))
    {
        Some((coefficient, exponent)) => (coefficient, parse_signed(exponent)),
        None => (unsigned, Some(0)),
    };
    let exponent = exponent.ok_or_else(|| invalid_scalar(T::BIGQUERY_TYPE))?;
    let (whole, fractional) = split_byte(coefficient, b'.').unwrap_or((coefficient, b""));
    let digits = || whole.iter().copied().chain(fractional.iter().copied());
    // The grammar and the all-zero shortcut share one pass over the digits.
    let digit_count = whole.len() + fractional.len();
    let mut all_zero = true;
    for byte in digits() {
        if !byte.is_ascii_digit() {
            return Err(invalid_scalar(T::BIGQUERY_TYPE));
        }
        all_zero &= byte == b'0';
    }
    if digit_count == 0 {
        return Err(invalid_scalar(T::BIGQUERY_TYPE));
    }
    if all_zero {
        return Ok(T::ZERO);
    }

    let unsupported_exponent = || {
        ArrowError::ParseError(format!(
            "BigQuery {} result value has an exponent outside the supported range",
            T::BIGQUERY_TYPE
        ))
    };
    let fractional_len = i64::try_from(fractional.len()).map_err(|_| unsupported_exponent())?;
    let shift = i64::from(scale)
        .checked_add(exponent)
        .and_then(|shift| shift.checked_sub(fractional_len))
        .ok_or_else(unsupported_exponent)?;
    let excessive_precision = || {
        ArrowError::ParseError(format!(
            "BigQuery {} result value needs more than the {budget} significant digits a result \
             column of precision {precision} can hold",
            T::BIGQUERY_TYPE
        ))
    };
    let excessive_scale = || {
        ArrowError::ParseError(format!(
            "BigQuery {} result value has more fractional digits than its result column's \
             declared scale of {scale}",
            T::BIGQUERY_TYPE
        ))
    };

    // A negative shift drops that many trailing digits, which must all be zero; a positive shift
    // appends that many zeros, which must leave the value within `budget` significant digits.
    let keep = if shift < 0 {
        let remove = usize::try_from(shift.unsigned_abs()).map_err(|_| excessive_scale())?;
        digit_count
            .checked_sub(remove)
            .ok_or_else(excessive_scale)?
    } else {
        digit_count
    };
    let mut magnitude = T::ZERO;
    let mut significant = 0_usize;
    let mut chunk = 0_u64;
    let mut width = 0_u32;
    for (index, byte) in digits().enumerate() {
        if index >= keep {
            if byte != b'0' {
                return Err(excessive_scale());
            }
            continue;
        }
        if significant > 0 || byte != b'0' {
            significant += 1;
        }
        chunk = chunk * 10 + u64::from(byte - b'0');
        width += 1;
        if width == CHUNK_DIGITS {
            magnitude = magnitude
                .push_chunk(chunk, width, negative)
                .ok_or_else(excessive_precision)?;
            (chunk, width) = (0, 0);
        }
    }
    if shift >= 0 {
        let maximum_shift = usize::from(budget)
            .checked_sub(significant)
            .ok_or_else(excessive_precision)?;
        let shift = usize::try_from(shift).map_err(|_| excessive_precision())?;
        if shift > maximum_shift {
            return Err(excessive_precision());
        }
        // The trailing zeros the scale appends are the same shift, taken a chunk at a time.
        for _ in 0..shift {
            chunk *= 10;
            width += 1;
            if width == CHUNK_DIGITS {
                magnitude = magnitude
                    .push_chunk(chunk, width, negative)
                    .ok_or_else(excessive_precision)?;
                (chunk, width) = (0, 0);
            }
        }
    } else if significant > usize::from(budget) {
        return Err(excessive_precision());
    }
    if width > 0 {
        magnitude = magnitude
            .push_chunk(chunk, width, negative)
            .ok_or_else(excessive_precision)?;
    }

    Ok(magnitude)
}

#[cfg(test)]
mod tests;
