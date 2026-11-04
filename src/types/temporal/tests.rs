use super::{
    civil_from_days, days_from_civil, nanoseconds_to_microseconds,
    truncate_to_microsecond_precision,
};

#[test]
fn truncates_towards_zero() {
    for (nanoseconds, microseconds, aligned_nanoseconds) in [
        (1_999, 1, 1_000),
        (1_000, 1, 1_000),
        (999, 0, 0),
        (0, 0, 0),
        (-999, 0, 0),
        (-1_000, -1, -1_000),
        (-1_999, -1, -1_000),
    ] {
        assert_eq!(nanoseconds_to_microseconds(nanoseconds), microseconds);
        assert_eq!(
            truncate_to_microsecond_precision(nanoseconds),
            aligned_nanoseconds
        );
    }
}

#[test]
fn interval_bounds_are_inclusive() {
    use super::{IntervalOutOfRange, check_interval_months_days};

    assert_eq!(check_interval_months_days(120_000, 3_660_000), Ok(()));
    assert_eq!(check_interval_months_days(-120_000, -3_660_000), Ok(()));
    assert_eq!(
        check_interval_months_days(120_001, 0),
        Err(IntervalOutOfRange::Months)
    );
    assert_eq!(
        check_interval_months_days(0, -3_660_001),
        Err(IntervalOutOfRange::Days)
    );
    assert_eq!(IntervalOutOfRange::Months.to_string(), "month component");
    assert_eq!(IntervalOutOfRange::Days.to_string(), "day component");
}

#[test]
fn civil_conversions_are_inverses() {
    for (days, year, month, day) in [
        (0, 1970, 1, 1),
        (-1, 1969, 12, 31),
        (11_016, 2000, 2, 29),
        (11_017, 2000, 3, 1),
        (-719_162, 1, 1, 1),
        (2_932_896, 9999, 12, 31),
    ] {
        assert_eq!(civil_from_days(days), (year, month, day));
        assert_eq!(days_from_civil(year, month, day), days);
    }
    for days in -1_000_000..1_000_000 {
        let (year, month, day) = civil_from_days(days);
        assert_eq!(days_from_civil(year, month, day), days);
    }
}

/// Both conversions are total over `i64`.
///
/// No caller reaches these extremes, but the signatures take any `i64` and release builds panic on
/// overflow. An out-of-calendar argument yields an out-of-calendar year, which callers reject.
#[test]
fn civil_conversions_are_total_over_the_whole_integer_domain() {
    for days in [i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX] {
        let (year, _, _) = civil_from_days(days);
        assert!(!(1..=9_999).contains(&year), "{days} answered year {year}");
    }
    for (year, month, day) in [
        (i64::MIN, i64::MIN, i64::MIN),
        (i64::MAX, i64::MAX, i64::MAX),
        (i64::MIN, 1, 1),
        (9_999, i64::MAX, i64::MIN),
    ] {
        let days = days_from_civil(year, month, day);
        assert_ne!(civil_from_days(days), (year, month, day));
    }
}
