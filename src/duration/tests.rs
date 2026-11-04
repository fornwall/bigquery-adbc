use std::time::Duration;

use super::{format_go_duration, parse_impersonation_lifetime};

const KEY: &str = "test.lifetime";

fn parse(value: &str) -> Duration {
    parse_impersonation_lifetime(KEY, value).expect(value)
}

#[test]
fn parses_every_unit_spelling_shape_and_combination() {
    for (value, expected) in [
        ("1ns", Duration::from_nanos(1)),
        ("5us", Duration::from_micros(5)),
        ("5\u{b5}s", Duration::from_micros(5)),
        ("5\u{3bc}s", Duration::from_micros(5)),
        ("250ms", Duration::from_millis(250)),
        ("45s", Duration::from_secs(45)),
        ("30m", Duration::from_secs(1_800)),
        ("2h", Duration::from_secs(7_200)),
        ("+1h", Duration::from_secs(3_600)),
        ("1h30m45s", Duration::from_secs(5_445)),
        ("1.5h", Duration::from_secs(5_400)),
        (".5h", Duration::from_secs(1_800)),
        ("0.5ms", Duration::from_micros(500)),
        ("1h0.5s", Duration::from_millis(3_600_500)),
        ("1.h", Duration::from_secs(3_600)),
        (
            "1.0000000000000000000000000000000000000000h",
            Duration::from_secs(3_600),
        ),
        ("12h", Duration::from_secs(43_200)),
        (
            "11h59m59.999999999s",
            Duration::from_nanos(43_199_999_999_999),
        ),
    ] {
        assert_eq!(parse(value), expected, "{value}");
    }
}

/// A fraction finer than a nanosecond is rejected rather than truncated, so that every
/// accepted lifetime reads back as the value that was set.
#[test]
fn rejects_what_it_cannot_represent_exactly_or_apply() {
    for value in [
        "",
        "+",
        "-",
        "0",
        "-0",
        "1",
        "h",
        "1h30",
        "-1h",
        "1h-30m",
        "1d",
        "1 h",
        "1e3s",
        "1.5.5h",
        "1..5h",
        "1.5.0h",
        "1.0.h",
        ".h",
        "1,5h",
        "0.5ns",
        "0.0001us",
        "0s",
        "0h0m0s",
        "13h",
        "12h0.000000001s",
        "20000000000000000000ns",
        "99999999999999999999999999999999999999h",
        concat!(
            "170141183460469231731687303715884105727ns",
            "170141183460469231731687303715884105727ns"
        ),
    ] {
        assert!(
            parse_impersonation_lifetime(KEY, value).is_err(),
            "{value:?} must be rejected"
        );
    }
}

/// A zero or negative lifetime is a valid Go duration out of range, so the refusal says so rather
/// than call it unparseable.
#[test]
fn a_zero_or_negative_lifetime_is_refused_as_out_of_range() {
    for value in ["0", "+0", "-0", "0s", "-1h", "-30m"] {
        let message = parse_impersonation_lifetime(KEY, value)
            .unwrap_err()
            .message;
        assert!(
            message.contains("must be greater than zero"),
            "{value:?}: {message}"
        );
    }
    for value in ["-", "-h", "--1h", "+-1h"] {
        let message = parse_impersonation_lifetime(KEY, value)
            .unwrap_err()
            .message;
        assert!(
            message.contains("must be a Go duration"),
            "{value:?}: {message}"
        );
    }
}

/// One second and up is Go's own rendering; below it the unit is the largest that divides the
/// value evenly, spelled in ASCII, which is where `1500ns` and `2us` depart from Go's `1.5µs`
/// and `2µs`. See `format_go_duration`.
#[test]
fn renders_whole_units_in_ascii_and_seconds_the_way_go_does() {
    for (duration, expected) in [
        (Duration::ZERO, "0s"),
        (Duration::from_nanos(1), "1ns"),
        (Duration::from_nanos(1_500), "1500ns"),
        (Duration::from_micros(2), "2us"),
        (Duration::from_millis(1), "1ms"),
        (Duration::from_millis(500), "500ms"),
        (Duration::from_nanos(1_000_000_001), "1.000000001s"),
        (Duration::from_secs(1), "1s"),
        (Duration::from_millis(1_500), "1.5s"),
        (Duration::from_secs(59), "59s"),
        (Duration::from_secs(60), "1m0s"),
        (Duration::from_millis(90_500), "1m30.5s"),
        (Duration::from_secs(2_700), "45m0s"),
        (Duration::from_secs(3_600), "1h0m0s"),
        (Duration::from_secs(3_661), "1h1m1s"),
        (Duration::from_secs(43_200), "12h0m0s"),
    ] {
        assert_eq!(format_go_duration(duration), expected, "{duration:?}");
    }
}

/// Rendering is what a caller reads an accepted lifetime back as, so it must re-parse to the
/// same duration.
#[test]
fn rendering_round_trips_through_the_parser() {
    for value in ["1ns", "500ms", "45s", "1m30.5s", "1h30m45s", "12h0m0s"] {
        assert_eq!(format_go_duration(parse(value)), value);
    }
}
