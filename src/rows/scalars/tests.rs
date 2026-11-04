use crate::types::decimal::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, NUMERIC_PRECISION, NUMERIC_SCALE,
};

use super::*;

// The parsers take bytes (see `RestValue::as_bytes`). These `&str` wrappers shadow the
// glob-imported originals so each case spells its fixture as a wire string, including the
// non-ASCII ones, which must still be refused.
fn parse_timestamp(value: &str) -> Result<i64, ArrowError> {
    super::parse_timestamp(value.as_bytes())
}

fn parse_date(value: &str) -> Result<i32, ArrowError> {
    super::parse_date(value.as_bytes())
}

fn parse_time_of_day(value: &str) -> Result<i64, ArrowError> {
    super::parse_time_of_day(value.as_bytes())
}

fn parse_datetime(value: &str) -> Result<i64, ArrowError> {
    super::parse_datetime(value.as_bytes())
}

fn parse_interval(value: &str) -> Result<IntervalMonthDayNano, ArrowError> {
    super::parse_interval(value.as_bytes())
}

fn parse_scaled_decimal<T: DecimalDigits>(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<T, ArrowError> {
    super::parse_scaled_decimal(value.as_bytes(), precision, scale)
}

/// Asserts a decimal is rejected for `reason`, one of the parser's five messages, which a bare
/// `is_err` cannot tell apart. Each names the BigQuery type, which the Arrow width determines.
fn rejects_decimal128(value: &str, precision: u8, scale: i8, reason: &str) {
    let Err(error) = parse_scaled_decimal::<i128>(value, precision, scale) else {
        panic!("{value:?} was accepted as Decimal128({precision}, {scale})");
    };
    let message = error.to_string();
    assert!(message.contains(reason), "{value:?}: {message}");
    assert!(message.contains("NUMERIC"), "{value:?}: {message}");
}

/// See [`rejects_decimal128`].
fn rejects_decimal256(value: &str, precision: u8, scale: i8, reason: &str) {
    let Err(error) = parse_scaled_decimal::<i256>(value, precision, scale) else {
        panic!("{value:?} was accepted as Decimal256({precision}, {scale})");
    };
    let message = error.to_string();
    assert!(message.contains(reason), "{value:?}: {message}");
    assert!(message.contains("BIGNUMERIC"), "{value:?}: {message}");
}

/// What the hand-written parsers accept is exactly BigQuery's grammar: fixed-width date
/// fields, an optional fractional second of one to six digits, and nothing else.
#[test]
fn parses_bigquery_temporal_spellings_and_rejects_everything_else() {
    assert_eq!(parse_date("1970-01-01").unwrap(), 0);
    assert_eq!(parse_date("2000-02-29").unwrap(), 11_016);
    assert_eq!(parse_date("1969-12-31").unwrap(), -1);
    assert_eq!(parse_date("0001-01-01").unwrap(), -719_162);
    assert_eq!(parse_date("9999-12-31").unwrap(), 2_932_896);
    for malformed in [
        "",
        "2021-02-30",
        "2021-13-01",
        "2021-00-01",
        "2021-01-00",
        "0000-01-01",
        "2021-1-1",
        "2021-01-1",
        "21-01-01",
        "2021/01/01",
        "2021-01-01T00:00:00",
        "2021-01-01 ",
        "+021-01-01",
        "-021-01-01",
    ] {
        let error = parse_date(malformed).unwrap_err();
        assert!(
            error.to_string().contains("is not a valid DATE"),
            "{malformed:?}: {error}"
        );
    }

    assert_eq!(parse_time_of_day("00:00:00").unwrap(), 0);
    assert_eq!(parse_time_of_day("12:34:56").unwrap(), 45_296_000_000);
    assert_eq!(
        parse_time_of_day("12:34:56.789012").unwrap(),
        45_296_789_012
    );
    // A shorter fraction scales up and a longer one truncates to BigQuery's microseconds.
    assert_eq!(parse_time_of_day("00:00:00.5").unwrap(), 500_000);
    assert_eq!(parse_time_of_day("00:00:00.0000005").unwrap(), 0);
    assert_eq!(
        parse_time_of_day("23:59:59.999999").unwrap(),
        86_399_999_999
    );
    // BigQuery pads a TIME, but the same clock parser serves the unpadded INTERVAL spelling.
    assert_eq!(parse_time_of_day("1:2:3").unwrap(), 3_723_000_000);
    for malformed in [
        "",
        "12:34",
        "12:34:56.",
        "12:34:56:78",
        "12-34-56",
        "-1:00:00",
    ] {
        let error = parse_time_of_day(malformed).unwrap_err();
        assert!(
            error.to_string().contains("is not a valid TIME"),
            "{malformed:?}: {error}"
        );
    }
    // Out-of-day components are reported as such rather than as a grammar failure.
    for out_of_day in ["24:00:00", "23:60:00", "23:59:60"] {
        let error = parse_time_of_day(out_of_day).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("TIME result value has a time of day outside"),
            "{out_of_day:?}: {error}"
        );
    }

    assert_eq!(parse_datetime("1970-01-01T00:00:00").unwrap(), 0);
    assert_eq!(
        parse_datetime("2020-07-02T12:34:56.789012").unwrap(),
        1_593_693_296_789_012
    );
    assert_eq!(parse_datetime("1969-12-31T23:59:59.999999").unwrap(), -1);
    for malformed in [
        "",
        "2020-07-02",
        "2020-07-02 12:34:56",
        "2020-07-02T12:34",
        "2020-02-30T00:00:00",
        "2020-07-02T24:00:00",
    ] {
        let error = parse_datetime(malformed).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("is not a valid DATETIME") || message.contains("time of day"),
            "{malformed:?}: {message}"
        );
    }
}

/// BigQuery spells a TIMESTAMP cell, and a `RANGE<TIMESTAMP>` bound, as integer microseconds
/// since the epoch rather than as a formatted instant.
#[test]
fn parses_timestamps_as_epoch_microseconds() {
    assert_eq!(parse_timestamp("0").unwrap(), 0);
    assert_eq!(
        parse_timestamp("1593676800123456").unwrap(),
        1_593_676_800_123_456
    );
    // Both ends of BigQuery's calendar, which bounds this column as it bounds every other
    // temporal one.
    assert_eq!(
        parse_timestamp("-62135596800000000").unwrap(),
        MIN_TIMESTAMP_MICROS
    );
    assert_eq!(
        parse_timestamp("253402300799999999").unwrap(),
        MAX_TIMESTAMP_MICROS
    );
    for malformed in [
        "",
        " ",
        "1.5",
        "1e9",
        "not a timestamp",
        "9223372036854775808",
        "253402300800000000",
        "-62135596800000001",
    ] {
        let error = parse_timestamp(malformed).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("is not a valid TIMESTAMP"),
            "{malformed:?}: {message}"
        );
    }
}

/// An Arrow `Time64(Microsecond)` is a time of day, so a response claiming hour 24 or second
/// 60 has to fail rather than become an out-of-day value every consumer would misread.
#[test]
fn rejects_times_that_are_not_a_time_of_day() {
    assert_eq!(time_of_day_micros("TIME", 0, 0, 0, 0).unwrap(), 0);
    assert_eq!(
        time_of_day_micros("TIME", 23, 59, 59, 999_999_999).unwrap(),
        86_399_999_999
    );
    // Nanosecond precision truncates to BigQuery's microseconds rather than rounding.
    assert_eq!(time_of_day_micros("TIME", 0, 0, 0, 1_999).unwrap(), 1);
    for (hours, minutes, seconds, nanos) in [
        (24, 0, 0, 0),
        (23, 59, 60, 0),
        (23, 60, 0, 0),
        (-1, 0, 0, 0),
        (0, -1, 0, 0),
        (0, 0, -1, 0),
        (0, 0, 0, -1),
        (0, 0, 0, 1_000_000_000),
    ] {
        let error = time_of_day_micros("TIME", hours, minutes, seconds, nanos).unwrap_err();
        assert!(
            error.to_string().contains("time of day outside"),
            "{hours}:{minutes}:{seconds}.{nanos}: {error}"
        );
    }

    // The same bound guards the time half of a DATETIME, and says which type it was.
    let error = parse_datetime("2020-07-02T24:00:00").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DATETIME result value has a time of day outside"),
        "{error}"
    );
}

/// Result values may contain sensitive data and are unbounded, so scalar failures name only
/// the expected type and never the cell contents.
#[test]
fn scalar_parse_failures_never_echo_values() {
    let long = "9".repeat(10_000);
    let message = parse_date(&long).unwrap_err().to_string();
    assert!(message.contains("valid DATE"), "{message}");
    assert!(!message.contains(&long), "value was echoed: {message}");
    assert!(message.len() < 200, "{} bytes", message.len());
    // An over-cap INTERVAL passes every component check, and its fraction may be any length.
    let interval = format!("0-0 0 3000000:0:0.{long}");
    let message = parse_interval(&interval).unwrap_err().to_string();
    assert!(message.contains("does not fit"), "{message}");
    assert!(!message.contains(&long), "value was echoed: {message}");
    for (secret, error) in [
        (
            "not-a-secret-date",
            parse_date("not-a-secret-date").unwrap_err(),
        ),
        (
            "not-a-secret-decimal",
            parse_scaled_decimal::<i128>("not-a-secret-decimal", 38, 9).unwrap_err(),
        ),
    ] {
        let message = error.to_string();
        assert!(!message.contains(secret), "value was echoed: {message}");
    }
}

/// An INTERVAL's hour component is unbounded, so an hour of 24 or more parses.
#[test]
fn parses_intervals_whose_hours_exceed_a_clock_day() {
    assert_eq!(
        parse_interval("0-0 0 744:0:0").unwrap(),
        IntervalMonthDayNano::new(0, 0, 744 * 3_600 * 1_000_000_000)
    );
    assert_eq!(
        parse_interval("0-0 0 25:0:0").unwrap(),
        IntervalMonthDayNano::new(0, 0, 25 * 3_600 * 1_000_000_000)
    );
    assert_eq!(
        parse_interval("0-0 0 -744:0:0").unwrap(),
        IntervalMonthDayNano::new(0, 0, -744 * 3_600 * 1_000_000_000)
    );
    // Arrow's nanosecond interval spans roughly 292 years, which bounds the hour count this
    // can represent well below BigQuery's own INTERVAL range.
    assert_eq!(
        parse_interval("0-0 0 2562047:0:0").unwrap(),
        IntervalMonthDayNano::new(0, 0, 2_562_047 * 3_600 * 1_000_000_000)
    );
}

#[test]
fn parses_every_interval_sign_and_fraction() {
    assert_eq!(
        parse_interval("1-2 3 4:05:06.789123456").unwrap(),
        IntervalMonthDayNano::new(14, 3, 14_706_789_123_456)
    );
    assert_eq!(
        parse_interval("-1-2 -3 -4:5:6.123").unwrap(),
        IntervalMonthDayNano::new(-14, -3, -14_706_123_000_000)
    );
    // Each part carries its own sign; a mixed-sign INTERVAL keeps all three.
    assert_eq!(
        parse_interval("-1-2 3 -4:5:6").unwrap(),
        IntervalMonthDayNano::new(-14, 3, -14_706_000_000_000)
    );
    assert_eq!(
        parse_interval("0-0 0 0:00:00").unwrap(),
        IntervalMonthDayNano::new(0, 0, 0)
    );
    assert_eq!(
        parse_interval("0-0 0 1:1:1.000000001").unwrap(),
        IntervalMonthDayNano::new(0, 0, 3_661_000_000_001)
    );
    // A fraction shorter than nine digits scales up; a longer one truncates.
    assert_eq!(
        parse_interval("0-0 0 0:0:0.5").unwrap(),
        IntervalMonthDayNano::new(0, 0, 500_000_000)
    );
    assert_eq!(
        parse_interval("0-0 0 0:0:0.1234567899").unwrap(),
        IntervalMonthDayNano::new(0, 0, 123_456_789)
    );
}

#[test]
fn rejects_malformed_intervals_rather_than_decoding_them() {
    for malformed in [
        "",
        "1-2 3",
        "1-2 3 4:5:6 7",
        "1 3 4:5:6",
        "1-2 3 4:5",
        "1-2 3 4:5:6.",
        "1-2 3 4:5:x",
        "1-2 x 4:5:6",
        "1-2 3 -4:-5:6",
        "1-+2 3 4:5:6",
        "+1-2 3 4:5:6",
        // The hour count overflows the nanosecond scale rather than wrapping.
        "0-0 0 2562048:0:0",
    ] {
        let error = parse_interval(malformed).unwrap_err();
        assert!(
            matches!(error, ArrowError::ParseError(_)),
            "{malformed:?} produced {error}"
        );
    }
}

#[test]
fn rejects_intervals_whose_components_leave_their_arrow_field() {
    // Years alone overflow the month count Arrow carries in an `i32`.
    let error = parse_interval("200000000-0 0 0:0:0").unwrap_err();
    assert!(error.to_string().contains("months overflow i32"), "{error}");
    let error = parse_interval("-200000000-0 0 0:0:0").unwrap_err();
    assert!(error.to_string().contains("months overflow i32"), "{error}");
    let error = parse_interval("0-0 3000000000 0:0:0").unwrap_err();
    assert!(error.to_string().contains("days overflow i32"), "{error}");
    let error = parse_interval("0-0 -3000000000 0:0:0").unwrap_err();
    assert!(error.to_string().contains("days overflow i32"), "{error}");
    // Arrow's interval holds exactly i64 nanoseconds, so the largest representable time
    // component is 2562047:47:16.854775807 and one nanosecond more is not.
    assert_eq!(
        parse_interval("0-0 0 2562047:47:16.854775807").unwrap(),
        IntervalMonthDayNano::new(0, 0, i64::MAX)
    );
    // BigQuery's INTERVAL range is +/-87840000:0:0, far past Arrow's interval, and BigQuery
    // returns values above the cap: the first is the whole TIMESTAMP domain,
    // `TIMESTAMP '9999-12-31 23:59:59.999999+00' - TIMESTAMP '0001-01-01 00:00:00+00'`, and the
    // second is `INTERVAL 3000000 HOUR`. The synthetic i64-hour value only guards the arithmetic.
    for value in [
        "0-0 0 87649415:59:59.999999",
        "0-0 0 3000000:0:0",
        "0-0 0 -3000000:0:0",
        "0-0 0 2562047:47:16.854775808",
        "0-0 0 9223372036854775807:0:0",
    ] {
        let error = parse_interval(value).unwrap_err().to_string();
        // Respelling the excess hours as days would change EXTRACT(DAY) and EXTRACT(HOUR), so
        // the error carries the limit and a workaround.
        assert!(error.contains("2562047:47:16.854775807"), "{error}");
        assert!(error.contains("CAST(x AS STRING)"), "{error}");
        assert!(error.contains("EXTRACT"), "{error}");
    }
}

#[test]
fn parses_bigquery_decimals_at_arrow_scale() {
    assert_eq!(
        parse_scaled_decimal::<i128>("123.456", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        123_456_000_000
    );
    assert_eq!(
        parse_scaled_decimal::<i128>("-1.2e2", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        -120_000_000_000
    );
    assert_eq!(
        parse_scaled_decimal::<i256>("1.25", BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        i256::from_string("125000000000000000000000000000000000000").unwrap()
    );
    rejects_decimal128(
        "0.0000000001",
        10,
        2,
        "more fractional digits than its result column",
    );
    rejects_decimal128(
        "123456789.01",
        10,
        2,
        "significant digits a result column of precision",
    );
    rejects_decimal256("not a number", 40, 10, "is not a valid BIGNUMERIC");
}

#[test]
fn parses_negative_decimals_and_signed_zero() {
    assert_eq!(
        parse_scaled_decimal::<i128>("-123.456", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        -123_456_000_000
    );
    assert_eq!(
        parse_scaled_decimal::<i128>("-0", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        0
    );
    assert_eq!(parse_scaled_decimal::<i128>("+1.5", 10, 2).unwrap(), 150);
    assert_eq!(
        parse_scaled_decimal::<i256>("-0.000", BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        i256::ZERO
    );
    assert_eq!(
        parse_scaled_decimal::<i256>("-1.5", BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        i256::from_string("-150000000000000000000000000000000000000").unwrap()
    );
}

#[test]
fn parses_decimals_at_maximum_precision_and_rejects_one_past_it() {
    // NUMERIC's largest value: 29 whole and 9 fractional nines, 38 significant digits.
    let maximum = format!("{}.{}", "9".repeat(29), "9".repeat(9));
    assert_eq!(
        parse_scaled_decimal::<i128>(&maximum, NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        10_i128.pow(38) - 1
    );
    assert_eq!(
        parse_scaled_decimal::<i128>(&format!("-{maximum}"), NUMERIC_PRECISION, NUMERIC_SCALE)
            .unwrap(),
        -(10_i128.pow(38) - 1)
    );
    // One more whole digit overflows the 38-digit precision.
    rejects_decimal128(
        &format!("1{maximum}"),
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "significant digits a result column of precision",
    );

    // 38 whole and 38 fractional nines, the largest value Arrow's declared
    // `DECIMAL256_MAX_PRECISION` of 76 can describe. BIGNUMERIC itself reaches one digit
    // further; see `parses_bignumeric_across_its_full_physical_range`.
    let maximum = format!("{}.{}", "9".repeat(38), "9".repeat(38));
    let expected = i256::from_string(&"9".repeat(76)).unwrap();
    assert_eq!(
        parse_scaled_decimal::<i256>(&maximum, BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        expected
    );
}

/// A BIGNUMERIC is parsed across the whole `i256` range, one significant digit wider than
/// `Decimal256(76, 38)` declares; see [`decimal_value_digit_budget`].
#[test]
fn parses_bignumeric_across_its_full_physical_range() {
    let maximum = "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
    let minimum = "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";
    assert_eq!(
        parse_scaled_decimal::<i256>(maximum, BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        i256::MAX
    );
    assert_eq!(
        parse_scaled_decimal::<i256>(minimum, BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        i256::MIN
    );

    // The same two values in the scientific form the grammar also accepts. They are the longest
    // decimal responses the driver takes, and the length cap is derived so that neither is
    // refused before it is parsed.
    assert_eq!(
        parse_scaled_decimal::<i256>(
            "5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38",
            BIGNUMERIC_PRECISION,
            BIGNUMERIC_SCALE
        )
        .unwrap(),
        i256::MAX
    );
    assert_eq!(
        parse_scaled_decimal::<i256>(
            "-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38",
            BIGNUMERIC_PRECISION,
            BIGNUMERIC_SCALE
        )
        .unwrap(),
        i256::MIN
    );

    // `BIGNUMERIC '1e38'` is the smallest value that needs the 77th digit.
    let scaled = i256::from_i128(10_i128.pow(38))
        .checked_mul(i256::from_i128(10_i128.pow(38)))
        .unwrap();
    assert_eq!(
        parse_scaled_decimal::<i256>("1e38", BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE).unwrap(),
        scaled
    );
    assert_eq!(
        parse_scaled_decimal::<i256>(
            &format!("1{}.{}", "0".repeat(38), "0".repeat(38)),
            BIGNUMERIC_PRECISION,
            BIGNUMERIC_SCALE
        )
        .unwrap(),
        scaled
    );

    // One unit past either end of the `i256` range is refused.
    rejects_decimal256(
        "578960446186580977117854925043439539266.34992332820282019728792003956564819968",
        BIGNUMERIC_PRECISION,
        BIGNUMERIC_SCALE,
        "significant digits a result column of precision",
    );
    rejects_decimal256(
        "-578960446186580977117854925043439539266.34992332820282019728792003956564819969",
        BIGNUMERIC_PRECISION,
        BIGNUMERIC_SCALE,
        "significant digits a result column of precision",
    );

    // The wider budget applies only to BIGNUMERIC's own `Decimal256(76, 38)`; every other
    // decimal type stays bounded by its declared precision. Each of these scales to a
    // 77-digit integer that `i256` holds comfortably, and each is still refused.
    rejects_decimal256(
        "1e39",
        BIGNUMERIC_PRECISION,
        37,
        "significant digits a result column of precision",
    );
    rejects_decimal256(
        "1e38",
        75,
        BIGNUMERIC_SCALE,
        "significant digits a result column of precision",
    );
}

#[test]
fn drops_zero_digits_beyond_the_arrow_scale() {
    // Ten fractional digits against scale nine: the trailing zero is dropped exactly.
    assert_eq!(
        parse_scaled_decimal::<i128>("1.2300000000", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        1_230_000_000
    );
    assert_eq!(
        parse_scaled_decimal::<i128>("123000000000e-12", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        123_000_000
    );
    // A dropped digit that is not zero would change the value, so it is refused.
    rejects_decimal128(
        "1.2300000001",
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "more fractional digits than its result column",
    );
    // Leading zeros never count against precision.
    assert_eq!(
        parse_scaled_decimal::<i128>("000000000000123", 3, 0).unwrap(),
        123
    );
}

#[test]
fn rejects_decimal_expansion_beyond_arrow_precision() {
    rejects_decimal128(
        &"1".repeat(MAX_DECIMAL_RESPONSE_LENGTH + 1),
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        &format!("exceeds {MAX_DECIMAL_RESPONSE_LENGTH} bytes"),
    );
    rejects_decimal128(
        "1E100000",
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "significant digits a result column of precision",
    );
    rejects_decimal128(
        "1E2147483647",
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "significant digits a result column of precision",
    );
    rejects_decimal128(
        "1E9223372036854775807",
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "exponent outside the supported range",
    );
    rejects_decimal128(
        "1E-9223372036854775808",
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        "more fractional digits than its result column",
    );
    assert_eq!(
        parse_scaled_decimal::<i128>("0.000001E6", 3, 2).unwrap(),
        100
    );
}

/// A BigQuery decimal arrives as a JSON string, so every malformed shape (no digits, or digits the
/// grammar disallows) must be rejected.
#[test]
fn rejects_decimal_responses_that_are_not_decimals() {
    for malformed in [
        "",
        " ",
        ".",
        "-",
        "+",
        "-.",
        "NaN",
        "Infinity",
        "-Infinity",
        "1.2.3",
        "1e",
        "1e+",
        "1E-",
        "1 2",
        "0x10",
        "１",
        "1,5",
    ] {
        let error =
            parse_scaled_decimal::<i128>(malformed, NUMERIC_PRECISION, NUMERIC_SCALE).unwrap_err();
        assert!(
            matches!(error, ArrowError::ParseError(_)),
            "{malformed:?} produced {error}"
        );
        assert!(
            parse_scaled_decimal::<i256>(malformed, BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE)
                .is_err(),
            "{malformed:?} was accepted as a BIGNUMERIC"
        );
    }

    // Surrounding ASCII whitespace and an explicit sign are accepted; non-ASCII whitespace is
    // not trimmed, and the digit grammar rejects it.
    assert_eq!(
        parse_scaled_decimal::<i128>("  +1.5  ", NUMERIC_PRECISION, NUMERIC_SCALE).unwrap(),
        1_500_000_000
    );
    assert!(parse_scaled_decimal::<i128>("\u{a0}1.5", NUMERIC_PRECISION, NUMERIC_SCALE).is_err());
    assert!(parse_scaled_decimal::<i128>("1.5\u{a0}", NUMERIC_PRECISION, NUMERIC_SCALE).is_err());
}

/// [`parse_signed`] accepts the grammar of `str::parse::<i64>`: both ends of `i64`, an explicit
/// `+`, and nothing past the grammar's edges.
#[test]
fn parses_signed_integers_at_both_ends_of_i64() {
    assert_eq!(parse_signed(b"0"), Some(0));
    assert_eq!(parse_signed(b"-0"), Some(0));
    assert_eq!(parse_signed(b"+0"), Some(0));
    assert_eq!(parse_signed(b"+42"), Some(42));
    assert_eq!(parse_signed(b"9223372036854775807"), Some(i64::MAX));
    // The most negative `i64` has no positive magnitude, so it can only be reached by
    // accumulating in the sign's own direction.
    assert_eq!(parse_signed(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(parse_signed(b"0009223372036854775807"), Some(i64::MAX));
    for malformed in [
        &b""[..],
        b"-",
        b"+",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e9",
        b"9223372036854775808",
        b"-9223372036854775809",
        b"99999999999999999999999999",
        b"\x001",
        b"1\x00",
    ] {
        assert_eq!(parse_signed(malformed), None, "{malformed:?} was accepted");
    }
    // A TIMESTAMP is spelled with the same grammar, sign included.
    assert_eq!(
        parse_timestamp("+1593676800123456").unwrap(),
        1_593_676_800_123_456
    );
}

/// The temporal parsers split on raw bytes, so a value whose delimiter position holds part of a
/// multi-byte character, or an embedded NUL, must still be refused rather than mis-split.
#[test]
fn rejects_temporal_spellings_that_are_not_ascii() {
    for malformed in ["2021\u{2013}01-01", "2021-01\u{2013}01", "2021-01-0\u{0}"] {
        assert!(
            parse_date(malformed).is_err(),
            "{malformed:?} was accepted as a DATE"
        );
    }
    for malformed in ["12\u{a0}34:56", "12:34\u{a0}56", "12:34:5\u{0}"] {
        assert!(
            parse_time_of_day(malformed).is_err(),
            "{malformed:?} was accepted as a TIME"
        );
    }
    assert!(parse_datetime("2020-07-02\u{a0}12:34:56").is_err());
    // A NUL inside the fractional seconds is not a digit, so the subsecond scan refuses it.
    assert!(parse_time_of_day("12:34:56.78\u{0}").is_err());
}

/// The subsecond scale truncates past the ninth digit and pads short fractions, whichever side
/// of nine the response carries.
#[test]
fn scales_subsecond_digits_around_the_ninth() {
    assert_eq!(parse_time_of_day("00:00:00.1").unwrap(), 100_000);
    assert_eq!(parse_time_of_day("00:00:00.123456789").unwrap(), 123_456);
    // A tenth digit is validated and dropped rather than folded in.
    assert_eq!(parse_time_of_day("00:00:00.1234567899").unwrap(), 123_456);
    assert_eq!(parse_time_of_day("00:00:00.000000000000001").unwrap(), 0);
    assert!(parse_time_of_day("00:00:00.").is_err());
    assert!(parse_time_of_day("00:00:00.12a").is_err());
}

/// Decimal digits accumulate a chunk at a time, so a value whose digit count straddles a chunk
/// boundary must land on the same integer a digit-at-a-time accumulation would.
#[test]
fn accumulates_decimal_digits_across_chunk_boundaries() {
    for digits in 1..=38_u32 {
        let spelling = "9".repeat(digits as usize);
        let expected = (0..digits).fold(0_i128, |value, _| value * 10 + 9);
        assert_eq!(
            parse_scaled_decimal::<i128>(&spelling, 38, 0).unwrap(),
            expected,
            "{digits} nines"
        );
        assert_eq!(
            parse_scaled_decimal::<i128>(&format!("-{spelling}"), 38, 0).unwrap(),
            -expected,
            "-{digits} nines"
        );
    }
    // The same across the whole BIGNUMERIC width, where the chunking replaces four `i256`
    // multiplies per digit with three for the entire value.
    for digits in 1..=76_u32 {
        let spelling = "9".repeat(digits as usize);
        let expected = (0..digits).fold(i256::ZERO, |value, _| {
            value.checked_mul(i256::from_i128(10)).unwrap() + i256::from_i128(9)
        });
        assert_eq!(
            parse_scaled_decimal::<i256>(&spelling, 76, 0).unwrap(),
            expected,
            "{digits} nines"
        );
    }
    // Trailing zeros the scale appends cross the same boundary.
    assert_eq!(
        parse_scaled_decimal::<i128>("1", 38, 30).unwrap(),
        10_i128.pow(30)
    );
    assert_eq!(
        parse_scaled_decimal::<i256>("1", 76, 38).unwrap(),
        i256::from_i128(10_i128.pow(38))
    );
}
