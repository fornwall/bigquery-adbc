//! Shared temporal conversions: BigQuery's microsecond precision and the civil calendar.

const NANOSECONDS_PER_MICROSECOND: i64 = 1_000;

/// A civil day in each Arrow temporal unit. GoogleSQL has no leap seconds, so every day is
/// exactly this long.
pub(crate) const SECONDS_PER_DAY: i64 = 86_400;
/// See [`SECONDS_PER_DAY`].
pub(crate) const MILLISECONDS_PER_DAY: i64 = SECONDS_PER_DAY * 1_000;
/// See [`SECONDS_PER_DAY`].
pub(crate) const MICROSECONDS_PER_DAY: i64 = MILLISECONDS_PER_DAY * 1_000;
/// See [`SECONDS_PER_DAY`].
pub(crate) const NANOSECONDS_PER_DAY: i64 = MICROSECONDS_PER_DAY * 1_000;

/// Narrows nanoseconds to BigQuery microseconds, truncating towards zero.
#[inline]
pub(crate) const fn nanoseconds_to_microseconds(nanoseconds: i64) -> i64 {
    nanoseconds / NANOSECONDS_PER_MICROSECOND
}

/// Narrows a nanosecond instant to BigQuery microseconds, rounding down: truncating towards zero
/// would move a pre-1970 instant into a later microsecond, and -500 ns into 1970. Go's
/// `time.Time` formatting rounds down too.
#[inline]
pub(crate) const fn instant_nanoseconds_to_microseconds(nanoseconds: i64) -> i64 {
    nanoseconds.div_euclid(NANOSECONDS_PER_MICROSECOND)
}

/// Removes the sub-microsecond part while retaining nanosecond units.
#[inline]
pub(crate) const fn truncate_to_microsecond_precision(nanoseconds: i64) -> i64 {
    nanoseconds_to_microseconds(nanoseconds) * NANOSECONDS_PER_MICROSECOND
}

/// BigQuery bounds an INTERVAL to ±10,000 years: 120,000 months and 3,660,000 days.
const BIGQUERY_MAX_INTERVAL_MONTHS: u32 = 120_000;
/// See [`BIGQUERY_MAX_INTERVAL_MONTHS`].
pub(crate) const BIGQUERY_MAX_INTERVAL_DAYS: u32 = 3_660_000;
/// The same ±10,000 years as a time component: ±87,840,000 hours exactly.
///
/// BigQuery documents the second range as ±316,224,000,000.999999, but the service refuses
/// anything past the round number (observed, not documented): `INTERVAL '0-0 0 87840000:0:0'` is
/// accepted and `INTERVAL '0-0 0 87840000:0:0.000001'` is out of range.
const BIGQUERY_MAX_INTERVAL_MICROS: i64 = 316_224_000_000_000_000;

/// The INTERVAL component outside BigQuery's supported range, without its value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntervalOutOfRange {
    Months,
    Days,
    Microseconds,
}

impl std::fmt::Display for IntervalOutOfRange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Months => formatter.write_str("month component"),
            Self::Days => formatter.write_str("day component"),
            Self::Microseconds => formatter.write_str("time component"),
        }
    }
}

/// Checks an INTERVAL's month and day components against BigQuery's bounds.
///
/// The time component needs [`check_interval_micros`] only where its Arrow type can exceed the
/// bound: `i64` nanoseconds reach 2,562,048 hours and `i32` milliseconds 596,524, both far short
/// of 87,840,000.
pub(crate) fn check_interval_months_days(months: i32, days: i32) -> Result<(), IntervalOutOfRange> {
    if months.unsigned_abs() > BIGQUERY_MAX_INTERVAL_MONTHS {
        return Err(IntervalOutOfRange::Months);
    }
    if days.unsigned_abs() > BIGQUERY_MAX_INTERVAL_DAYS {
        return Err(IntervalOutOfRange::Days);
    }
    Ok(())
}

/// Checks an INTERVAL's time component against BigQuery's bound, for Arrow values wide enough to
/// exceed it (a duration in seconds to microseconds spans a whole `i64` of microseconds).
pub(crate) fn check_interval_micros(micros: i64) -> Result<(), IntervalOutOfRange> {
    if micros.unsigned_abs() > BIGQUERY_MAX_INTERVAL_MICROS.unsigned_abs() {
        return Err(IntervalOutOfRange::Microseconds);
    }
    Ok(())
}

/// The civil (proleptic Gregorian) date of a count of days since the Unix epoch, as
/// `(year, month, day)`.
///
/// Howard Hinnant's civil-calendar conversion; the inverse of [`days_from_civil`] over the dates
/// that exist. Total over `i64`: the epoch shift saturates, so an impossible day count yields a
/// year far outside 1 to 9999, which callers reject.
pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days.saturating_add(719_468);
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted.saturating_sub(146_096)
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = shifted_month + if shifted_month < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

/// A civil (proleptic Gregorian) date bound far wider than any calendar, past which
/// [`days_from_civil`] clamps rather than overflowing.
const CIVIL_COMPONENT_BOUND: i64 = 1 << 40;

/// The count of days since the Unix epoch of a civil (proleptic Gregorian) date.
///
/// Howard Hinnant's civil-calendar conversion; the inverse of [`civil_from_days`] over the dates
/// that exist. Total over `i64`: components are clamped to [`CIVIL_COMPONENT_BOUND`], so an
/// out-of-range component yields a day count that fails the round trip callers use to reject
/// dates like `2021-02-30`.
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let clamp = |value: i64| value.clamp(-CIVIL_COMPONENT_BOUND, CIVIL_COMPONENT_BOUND);
    let (year, month, day) = (clamp(year), clamp(month), clamp(day));
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests;
