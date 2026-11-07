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

fn parse_scaled_decimal<T: DecimalDigits>(value: &str) -> Result<T, ArrowError> {
    super::parse_scaled_decimal(value.as_bytes())
}

/// Asserts a decimal is rejected for `reason`, one of the parser's five messages, which a bare
/// `is_err` cannot tell apart. Each names the BigQuery type, which the width determines.
fn rejects_decimal<T: DecimalDigits>(value: &str, reason: &str) {
    let Err(error) = parse_scaled_decimal::<T>(value) else {
        panic!("{value:?} was accepted as a {}", T::BIGQUERY_TYPE);
    };
    let message = error.to_string();
    assert!(message.contains(reason), "{value:?}: {message}");
    assert!(message.contains(T::BIGQUERY_TYPE), "{value:?}: {message}");
}

const EXCESS_DIGITS: &str = "significant digits a result column of precision";
const EXCESS_SCALE: &str = "more fractional digits than its result column";

/// What the hand-written parsers accept is exactly BigQuery's grammar: fixed-width date
/// fields, an optional fractional second, and nothing else. The malformed spellings include ones
/// whose delimiter position holds part of a multi-byte character or a NUL, since the parsers split
/// on raw bytes.
#[test]
fn parses_bigquery_temporal_spellings_and_rejects_everything_else() {
    for (value, days) in [
        ("1970-01-01", 0),
        ("2000-02-29", 11_016),
        ("1969-12-31", -1),
        ("0001-01-01", -719_162),
        ("9999-12-31", 2_932_896),
    ] {
        assert_eq!(parse_date(value).unwrap(), days, "{value}");
    }
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
        "2021\u{2013}01-01",
        "2021-01\u{2013}01",
        "2021-01-0\u{0}",
    ] {
        let error = parse_date(malformed).unwrap_err();
        assert!(
            error.to_string().contains("is not a valid DATE"),
            "{malformed:?}: {error}"
        );
    }

    for (value, micros) in [
        ("00:00:00", 0),
        ("12:34:56", 45_296_000_000),
        ("12:34:56.789012", 45_296_789_012),
        ("23:59:59.999999", 86_399_999_999),
        // A shorter fraction scales up; a longer one truncates to BigQuery's microseconds, and a
        // digit past the ninth is validated and dropped rather than folded in.
        ("00:00:00.5", 500_000),
        ("00:00:00.1", 100_000),
        ("00:00:00.0000005", 0),
        ("00:00:00.123456789", 123_456),
        ("00:00:00.1234567899", 123_456),
        ("00:00:00.000000000000001", 0),
        // BigQuery pads a TIME, but the same clock parser serves the unpadded INTERVAL spelling.
        ("1:2:3", 3_723_000_000),
    ] {
        assert_eq!(parse_time_of_day(value).unwrap(), micros, "{value}");
    }
    for malformed in [
        "",
        "12:34",
        "12:34:56.",
        "12:34:56:78",
        "12-34-56",
        "-1:00:00",
        "00:00:00.12a",
        "12\u{a0}34:56",
        "12:34\u{a0}56",
        "12:34:5\u{0}",
        "12:34:56.78\u{0}",
    ] {
        let error = parse_time_of_day(malformed).unwrap_err();
        assert!(
            error.to_string().contains("is not a valid TIME"),
            "{malformed:?}: {error}"
        );
    }

    for (value, micros) in [
        ("1970-01-01T00:00:00", 0),
        ("2020-07-02T12:34:56.789012", 1_593_693_296_789_012),
        ("1969-12-31T23:59:59.999999", -1),
    ] {
        assert_eq!(parse_datetime(value).unwrap(), micros, "{value}");
    }
    for malformed in [
        "",
        "2020-07-02",
        "2020-07-02 12:34:56",
        "2020-07-02T12:34",
        "2020-02-30T00:00:00",
        "2020-07-02\u{a0}12:34:56",
    ] {
        let error = parse_datetime(malformed).unwrap_err();
        assert!(
            error.to_string().contains("is not a valid DATETIME"),
            "{malformed:?}: {error}"
        );
    }
}

/// BigQuery spells a TIMESTAMP cell, and a `RANGE<TIMESTAMP>` bound, as integer microseconds
/// since the epoch rather than as a formatted instant.
#[test]
fn parses_timestamps_as_epoch_microseconds() {
    for (value, micros) in [
        ("0", 0),
        ("1593676800123456", 1_593_676_800_123_456),
        ("+1593676800123456", 1_593_676_800_123_456),
        // Both ends of BigQuery's calendar, which bounds this column as it bounds every other
        // temporal one.
        ("-62135596800000000", MIN_TIMESTAMP_MICROS),
        ("253402300799999999", MAX_TIMESTAMP_MICROS),
    ] {
        assert_eq!(parse_timestamp(value).unwrap(), micros, "{value}");
    }
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
        assert!(
            error.to_string().contains("is not a valid TIMESTAMP"),
            "{malformed:?}: {error}"
        );
    }
}

/// An Arrow `Time64(Microsecond)` is a time of day, so a response claiming hour 24 or second
/// 60 has to fail rather than become an out-of-day value every consumer would misread, and it is
/// reported as such rather than as a grammar failure.
#[test]
fn rejects_times_that_are_not_a_time_of_day() {
    assert_eq!(
        time_of_day_micros("TIME", 23, 59, 59, 999_999_999).unwrap(),
        86_399_999_999
    );
    // Nanosecond precision truncates to BigQuery's microseconds rather than rounding.
    assert_eq!(time_of_day_micros("TIME", 0, 0, 0, 1_999).unwrap(), 1);
    for (hours, minutes, seconds, nanos) in [
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

    for clock in ["24:00:00", "23:60:00", "23:59:60"] {
        // The same bound guards the time half of a DATETIME, and says which type it was.
        for (error, kind) in [
            (parse_time_of_day(clock), "TIME"),
            (parse_datetime(&format!("2020-07-02T{clock}")), "DATETIME"),
        ] {
            let message = error.unwrap_err().to_string();
            assert!(
                message.contains(&format!("{kind} result value has a time of day outside")),
                "{clock:?}: {message}"
            );
        }
    }
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
            parse_scaled_decimal::<i128>("not-a-secret-decimal").unwrap_err(),
        ),
    ] {
        let message = error.to_string();
        assert!(!message.contains(secret), "value was echoed: {message}");
    }
}

#[test]
fn parses_every_interval_sign_and_fraction() {
    const HOUR: i64 = 3_600 * 1_000_000_000;
    for (value, months, days, nanoseconds) in [
        ("0-0 0 0:00:00", 0, 0, 0),
        ("1-2 3 4:05:06.789123456", 14, 3, 14_706_789_123_456),
        ("-1-2 -3 -4:5:6.123", -14, -3, -14_706_123_000_000),
        // Each part carries its own sign; a mixed-sign INTERVAL keeps all three.
        ("-1-2 3 -4:5:6", -14, 3, -14_706_000_000_000),
        ("0-0 0 1:1:1.000000001", 0, 0, 3_661_000_000_001),
        // A fraction shorter than nine digits scales up; a longer one truncates.
        ("0-0 0 0:0:0.5", 0, 0, 500_000_000),
        ("0-0 0 0:0:0.1234567899", 0, 0, 123_456_789),
        // An INTERVAL's hour component is unbounded, so an hour of 24 or more parses, up to the
        // roughly 292 years Arrow's i64 nanoseconds span.
        ("0-0 0 25:0:0", 0, 0, 25 * HOUR),
        ("0-0 0 744:0:0", 0, 0, 744 * HOUR),
        ("0-0 0 -744:0:0", 0, 0, -744 * HOUR),
        ("0-0 0 2562047:0:0", 0, 0, 2_562_047 * HOUR),
        ("0-0 0 2562047:47:16.854775807", 0, 0, i64::MAX),
    ] {
        assert_eq!(
            parse_interval(value).unwrap(),
            IntervalMonthDayNano::new(months, days, nanoseconds),
            "{value}"
        );
    }
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
    for (value, expected) in [
        ("200000000-0 0 0:0:0", "months overflow i32"),
        ("-200000000-0 0 0:0:0", "months overflow i32"),
        ("0-0 3000000000 0:0:0", "days overflow i32"),
        ("0-0 -3000000000 0:0:0", "days overflow i32"),
    ] {
        let error = parse_interval(value).unwrap_err().to_string();
        assert!(error.contains(expected), "{value}: {error}");
    }
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
fn parses_numeric_spellings_at_their_arrow_scale() {
    for (value, expected) in [
        ("123.456", 123_456_000_000),
        ("-123.456", -123_456_000_000),
        ("-1.2e2", -120_000_000_000),
        ("0.000001E6", 1_000_000_000),
        ("-0", 0),
        // Surrounding ASCII whitespace and an explicit sign are accepted.
        ("  +1.5  ", 1_500_000_000),
        // A dropped digit must be zero, so these are exact.
        ("1.2300000000", 1_230_000_000),
        ("123000000000e-12", 123_000_000),
    ] {
        assert_eq!(parse_scaled_decimal::<i128>(value).unwrap(), expected);
    }
    for (value, expected) in [
        ("1.25", "125000000000000000000000000000000000000"),
        ("-1.5", "-150000000000000000000000000000000000000"),
        ("-0.000", "0"),
    ] {
        assert_eq!(
            parse_scaled_decimal::<i256>(value).unwrap(),
            i256::from_string(expected).unwrap()
        );
    }
    rejects_decimal::<i128>("0.0000000001", EXCESS_SCALE);
    rejects_decimal::<i128>("1.2300000001", EXCESS_SCALE);
    rejects_decimal::<i256>("not a number", "is not a valid BIGNUMERIC");
}

#[test]
fn parses_decimals_at_maximum_precision_and_rejects_one_past_it() {
    // NUMERIC's largest value: 29 whole and 9 fractional nines, 38 significant digits.
    let maximum = format!("{}.{}", "9".repeat(29), "9".repeat(9));
    let expected = 10_i128.pow(38) - 1;
    assert_eq!(parse_scaled_decimal::<i128>(&maximum).unwrap(), expected);
    assert_eq!(
        parse_scaled_decimal::<i128>(&format!("-{maximum}")).unwrap(),
        -expected
    );
    // Leading zeros never count against precision.
    assert_eq!(
        parse_scaled_decimal::<i128>(&format!("{}{maximum}", "0".repeat(40))).unwrap(),
        expected
    );
    // One more whole digit overflows the 38-digit precision.
    rejects_decimal::<i128>(&format!("1{maximum}"), EXCESS_DIGITS);

    // 38 whole and 38 fractional nines, the largest value Arrow's declared precision of 76 can
    // describe. BIGNUMERIC itself reaches one digit further; see below.
    let maximum = format!("{}.{}", "9".repeat(38), "9".repeat(38));
    assert_eq!(
        parse_scaled_decimal::<i256>(&maximum).unwrap(),
        i256::from_string(&"9".repeat(76)).unwrap()
    );
}

/// A BIGNUMERIC is parsed across the whole `i256` range, one significant digit wider than
/// `Decimal256(76, 38)` declares; see `spans_full_physical_domain`.
#[test]
fn parses_bignumeric_across_its_full_physical_range() {
    // `BIGNUMERIC '1e38'` is the smallest value that needs the 77th digit.
    let scaled = i256::from_i128(10_i128.pow(38))
        .checked_mul(i256::from_i128(10_i128.pow(38)))
        .unwrap();
    for (value, expected) in [
        (
            "578960446186580977117854925043439539266.34992332820282019728792003956564819967",
            i256::MAX,
        ),
        (
            "-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
            i256::MIN,
        ),
        // The same two values in the scientific form the grammar also accepts. They are the
        // longest decimal responses the driver takes, and the length cap is derived so that
        // neither is refused before it is parsed.
        (
            "5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38",
            i256::MAX,
        ),
        (
            "-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38",
            i256::MIN,
        ),
        ("1e38", scaled),
        (
            "100000000000000000000000000000000000000.00000000000000000000000000000000000000",
            scaled,
        ),
    ] {
        assert_eq!(parse_scaled_decimal::<i256>(value).unwrap(), expected);
    }

    // One unit past either end of the `i256` range is refused.
    for value in [
        "578960446186580977117854925043439539266.34992332820282019728792003956564819968",
        "-578960446186580977117854925043439539266.34992332820282019728792003956564819969",
    ] {
        rejects_decimal::<i256>(value, EXCESS_DIGITS);
    }
}

#[test]
fn rejects_decimal_expansion_beyond_arrow_precision() {
    rejects_decimal::<i128>(
        &"1".repeat(MAX_DECIMAL_RESPONSE_LENGTH + 1),
        &format!("exceeds {MAX_DECIMAL_RESPONSE_LENGTH} bytes"),
    );
    rejects_decimal::<i128>("1E100000", EXCESS_DIGITS);
    rejects_decimal::<i128>("1E2147483647", EXCESS_DIGITS);
    rejects_decimal::<i128>(
        "1E9223372036854775807",
        "exponent outside the supported range",
    );
    rejects_decimal::<i128>("1E-9223372036854775808", EXCESS_SCALE);
}

/// A BigQuery decimal arrives as a JSON string, so every malformed shape (no digits, or digits the
/// grammar disallows) must be rejected. Non-ASCII whitespace is not trimmed, and the digit grammar
/// rejects it.
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
        "\u{a0}1.5",
        "1.5\u{a0}",
    ] {
        rejects_decimal::<i128>(malformed, "is not a valid NUMERIC");
        rejects_decimal::<i256>(malformed, "is not a valid BIGNUMERIC");
    }
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
}

/// Decimal digits accumulate a chunk at a time, so a value whose digit count straddles a chunk
/// boundary must land on the same integer a digit-at-a-time accumulation would. An exponent of
/// minus the scale spells the unscaled integer.
#[test]
fn accumulates_decimal_digits_across_chunk_boundaries() {
    for digits in 1..=38 {
        let spelling = format!("{}E-9", "9".repeat(digits));
        let expected = (0..digits).fold(0_i128, |value, _| value * 10 + 9);
        assert_eq!(
            parse_scaled_decimal::<i128>(&spelling).unwrap(),
            expected,
            "{digits} nines"
        );
        assert_eq!(
            parse_scaled_decimal::<i128>(&format!("-{spelling}")).unwrap(),
            -expected,
            "-{digits} nines"
        );
    }
    // The same across the whole BIGNUMERIC width, where the chunking replaces four `i256`
    // multiplies per digit with three for the entire value.
    for digits in 1..=76 {
        let expected = (0..digits).fold(i256::ZERO, |value, _| {
            value.checked_mul(i256::from_i128(10)).unwrap() + i256::from_i128(9)
        });
        assert_eq!(
            parse_scaled_decimal::<i256>(&format!("{}E-38", "9".repeat(digits))).unwrap(),
            expected,
            "{digits} nines"
        );
    }
    // Trailing zeros the scale appends cross the same boundary.
    assert_eq!(
        parse_scaled_decimal::<i128>("1E21").unwrap(),
        10_i128.pow(30)
    );
    assert_eq!(
        parse_scaled_decimal::<i256>("1").unwrap(),
        i256::from_i128(10_i128.pow(38))
    );
}
