//! Go-style duration parsing with a 12-hour impersonation lifetime limit, and ASCII formatting.

use std::fmt::Write as _;

use adbc_core::error::Result;

use crate::error::invalid_argument;

pub(crate) fn parse_impersonation_lifetime(key: &str, value: &str) -> Result<std::time::Duration> {
    // Go also accepts a sign and a unitless `0`; parsing them leaves the range check to say what is
    // wrong with them.
    let (negative, mut remaining) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut total_nanos = 0_u128;
    if remaining.is_empty() {
        return Err(invalid_duration(key));
    }
    if remaining == "0" {
        remaining = "";
    }
    while !remaining.is_empty() {
        let number_end = remaining
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(remaining.len());
        if number_end == 0 {
            return Err(invalid_duration(key));
        }
        let (number, rest) = remaining.split_at(number_end);
        let (rest, unit_nanos) = [
            ("ns", 1_u128),
            ("us", 1_000),
            ("µs", 1_000),
            ("μs", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60_000_000_000),
            ("h", 3_600_000_000_000),
        ]
        .into_iter()
        .find_map(|(unit, unit_nanos)| Some((rest.strip_prefix(unit)?, unit_nanos)))
        .ok_or_else(|| invalid_duration(key))?;
        remaining = rest;
        let component = decimal_nanos(number, unit_nanos).ok_or_else(|| invalid_duration(key))?;
        total_nanos = total_nanos
            .checked_add(component)
            .ok_or_else(|| invalid_duration(key))?;
    }
    let maximum = 43_200_u64 * 1_000_000_000;
    u64::try_from(total_nanos)
        .ok()
        .filter(|&nanos| !negative && nanos > 0 && nanos <= maximum)
        .map(std::time::Duration::from_nanos)
        .ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery option {key} must be greater than zero and at most 12 hours; above 1 \
                 hour, the organization policy \
                 constraints/iam.allowServiceAccountCredentialLifetimeExtension must allow the \
                 target service account"
            ))
        })
}

/// Converts a decimal count of units into an exact number of nanoseconds.
///
/// Sub-nanosecond fractions are rejected instead of truncated as in Go, so accepted values
/// round-trip without precision loss. Integer parsing rejects extra decimal points.
fn decimal_nanos(number: &str, unit_nanos: u128) -> Option<u128> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let whole_nanos = if whole.is_empty() {
        0
    } else {
        whole.parse::<u128>().ok()?.checked_mul(unit_nanos)?
    };
    // Go reads any number of fraction digits; trailing zeros must not overflow the denominator.
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        return Some(whole_nanos);
    }
    let denominator = 10_u128.checked_pow(fraction.len().try_into().ok()?)?;
    let numerator = fraction.parse::<u128>().ok()?.checked_mul(unit_nanos)?;
    if numerator % denominator != 0 {
        return None;
    }
    whole_nanos.checked_add(numerator / denominator)
}

fn invalid_duration(key: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery option {key} must be a Go duration such as 30m or 1h30m, no greater than \
         12 hours"
    ))
}

/// Formats a duration using Go's syntax with ASCII units.
///
/// Below one second, uses the largest integral unit: `1500ns` and `2us` rather than Go's
/// `1.5µs` and `2µs`. At one second and above, matches Go's rendering.
pub(crate) fn format_go_duration(duration: std::time::Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        // The unit scan would otherwise render zero as "0ms".
        return "0s".to_owned();
    }
    if nanos < 1_000_000_000 {
        for (divisor, suffix) in [(1_000_000_u128, "ms"), (1_000_u128, "us"), (1_u128, "ns")] {
            if nanos.is_multiple_of(divisor) {
                return format!("{}{}", nanos / divisor, suffix);
            }
        }
    }
    let seconds = duration.as_secs();
    let mut formatted = if seconds >= 3_600 {
        format!(
            "{}h{}m{}",
            seconds / 3_600,
            seconds % 3_600 / 60,
            seconds % 60
        )
    } else if seconds >= 60 {
        format!("{}m{}", seconds / 60, seconds % 60)
    } else {
        seconds.to_string()
    };
    let fraction = duration.subsec_nanos();
    if fraction != 0 {
        write!(formatted, ".{fraction:09}").expect("writing to a String cannot fail");
        formatted.truncate(formatted.trim_end_matches('0').len());
    }
    formatted.push('s');
    formatted
}

#[cfg(test)]
mod tests;
