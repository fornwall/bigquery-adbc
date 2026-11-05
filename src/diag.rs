//! Opt-in operator diagnostics on standard error.
//!
//! Some events have no ADBC caller to report to, such as a Storage Read fallback, a failed
//! background cancel or a retried request. They are written here only when the process sets
//! [`LOG_LEVEL`] (the variable and values the Go driver reads), which is read once per process.
//! Each event is one `TIMESTAMP [bigquery-adbc] LEVEL message` line, with a UTC timestamp at
//! millisecond precision. A site whose service error text could echo SQL, such as a failed job
//! cancel, reports only the status.
//!
//! This is not a `tracing` subscriber: the driver's `tracing` is private to the shared library,
//! and raising its compile-time cap would link every dependency's events too (see
//! `docs/BINARY-SIZE.md`).

use std::fmt::Arguments;
use std::io::Write as _;
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::temporal::{SECONDS_PER_DAY, civil_from_days};

/// The environment variable selecting the most verbose level written.
const LOG_LEVEL: &str = "ADBC_DRIVER_BIGQUERY_LOG_LEVEL";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
        }
    }
}

/// The configured level, or `None` when diagnostics are off.
static CONFIGURED: OnceLock<Option<Level>> = OnceLock::new();

/// Read [`LOG_LEVEL`] at startup, so an unrecognized value is reported even if no event follows.
pub(crate) fn init() {
    configured();
}

fn configured() -> Option<Level> {
    *CONFIGURED.get_or_init(|| {
        let value = std::env::var_os(LOG_LEVEL)?;
        let parsed = value.to_str().and_then(parse);
        if parsed.is_none() && !value.is_empty() {
            write_line(format_args!(
                "WARN ignoring {LOG_LEVEL}={}: expected debug, info, warn or error",
                value.to_string_lossy()
            ));
        }
        parsed
    })
}

/// A [`LOG_LEVEL`] value, case-insensitively; `warning` is accepted as Go accepts it.
fn parse(value: &str) -> Option<Level> {
    [
        ("error", Level::Error),
        ("warn", Level::Warn),
        ("warning", Level::Warn),
        ("info", Level::Info),
        ("debug", Level::Debug),
    ]
    .into_iter()
    .find_map(|(name, level)| value.eq_ignore_ascii_case(name).then_some(level))
}

/// Whether events at `level` are written: one atomic load once the variable has been read.
pub(crate) fn enabled(level: Level) -> bool {
    #[cfg(test)]
    if lock_captured(&CAPTURED).0 > 0 {
        return true;
    }
    configured().is_some_and(|configured| level <= configured)
}

/// Writes one event line. Call through [`diag!`], which checks [`enabled`] first.
pub(crate) fn write(level: Level, message: Arguments<'_>) {
    #[cfg(test)]
    {
        let mut captured = lock_captured(&CAPTURED);
        if captured.0 > 0 {
            captured.1.push(format!("{} {message}", level.name()));
            return;
        }
    }
    write_line(format_args!("{} {message}", level.name()));
}

/// The number of [`capture`] calls in progress, and the events since the first of them began.
#[cfg(test)]
static CAPTURED: Mutex<(usize, Vec<String>)> = Mutex::new((0, Vec::new()));

#[cfg(test)]
fn lock_captured(state: &Mutex<(usize, Vec<String>)>) -> MutexGuard<'_, (usize, Vec<String>)> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
struct CaptureGuard<'a> {
    state: &'a Mutex<(usize, Vec<String>)>,
    start: usize,
}

#[cfg(test)]
impl<'a> CaptureGuard<'a> {
    fn new(state: &'a Mutex<(usize, Vec<String>)>) -> Self {
        let mut captured = lock_captured(state);
        captured.0 += 1;
        Self {
            state,
            start: captured.1.len(),
        }
    }

    fn events(&self) -> Vec<String> {
        lock_captured(self.state).1[self.start..].to_vec()
    }
}

#[cfg(test)]
impl Drop for CaptureGuard<'_> {
    fn drop(&mut self) {
        let mut captured = lock_captured(self.state);
        captured.0 -= 1;
        if captured.0 == 0 {
            captured.1.clear();
        }
    }
}

/// Runs `run` with every level enabled and returns its result with the events written meanwhile,
/// as `LEVEL message` lines, instead of writing them. Tests run in parallel, so the events of
/// others are among them: match on text only your own events contain, such as a job ID.
#[cfg(test)]
pub(crate) fn capture<R>(run: impl FnOnce() -> R) -> (R, Vec<String>) {
    let capture = CaptureGuard::new(&CAPTURED);
    let result = run();
    (result, capture.events())
}

/// Format the whole line first so one write keeps it intact among other threads' output. Write
/// failures are ignored; diagnostics must never fail the operation they describe.
fn write_line(line: Arguments<'_>) {
    let _ = std::io::stderr().write_all(format_line(SystemTime::now(), line).as_bytes());
}

fn format_line(timestamp: SystemTime, line: Arguments<'_>) -> String {
    // Diagnostics must not panic even if the system clock exceeds i64 epoch milliseconds.
    let millis = match timestamp.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
        Err(before) => {
            -i64::try_from(before.duration().as_nanos().div_ceil(1_000_000)).unwrap_or(i64::MAX)
        }
    };
    let seconds = millis.div_euclid(1_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(SECONDS_PER_DAY));
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z [bigquery-adbc] {line}\n",
        second_of_day / 3_600,
        second_of_day % 3_600 / 60,
        second_of_day % 60,
        millis.rem_euclid(1_000),
    )
}

/// Writes a diagnostic event: `diag!(Warn, "format {}", args)`.
macro_rules! diag {
    ($level:ident, $($message:tt)+) => {
        if $crate::diag::enabled($crate::diag::Level::$level) {
            $crate::diag::write($crate::diag::Level::$level, format_args!($($message)+));
        }
    };
}

pub(crate) use diag;

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{CaptureGuard, Level, Mutex, format_line, lock_captured, parse};

    #[test]
    fn event_lines_include_utc_milliseconds_and_the_driver_name() {
        for (timestamp, expected) in [
            (UNIX_EPOCH, "1970-01-01T00:00:00.000Z"),
            (
                UNIX_EPOCH + Duration::from_millis(951_827_696_789),
                "2000-02-29T12:34:56.789Z",
            ),
            (
                UNIX_EPOCH + Duration::from_millis(1_767_225_599_999),
                "2025-12-31T23:59:59.999Z",
            ),
            (
                UNIX_EPOCH + Duration::from_millis(1_767_225_600_000),
                "2026-01-01T00:00:00.000Z",
            ),
            (
                UNIX_EPOCH - Duration::from_nanos(1),
                "1969-12-31T23:59:59.999Z",
            ),
        ] {
            assert_eq!(
                format_line(timestamp, format_args!("WARN retrying job {}", "job-1")),
                format!("{expected} [bigquery-adbc] WARN retrying job job-1\n"),
            );
        }
    }

    #[test]
    fn a_panicking_capture_releases_its_registration() {
        for overlapping in [false, true] {
            let state = Mutex::new((0, Vec::new()));
            let outer = overlapping.then(|| CaptureGuard::new(&state));
            let result = std::panic::catch_unwind(|| {
                let _capture = CaptureGuard::new(&state);
                lock_captured(&state).1.push("before panic".to_owned());
                panic!("capture closure failed");
            });
            assert!(result.is_err());

            let captured = lock_captured(&state);
            assert_eq!(captured.0, usize::from(overlapping));
            assert_eq!(captured.1.is_empty(), !overlapping);
            drop(captured);
            drop(outer);
            assert_eq!(*lock_captured(&state), (0, Vec::new()));
        }
    }

    #[test]
    fn overlapping_captures_keep_the_buffer_until_the_last_finishes() {
        let state = Mutex::new((0, Vec::new()));
        let first = CaptureGuard::new(&state);
        lock_captured(&state).1.push("first only".to_owned());
        let second = CaptureGuard::new(&state);
        lock_captured(&state).1.push("both".to_owned());

        assert_eq!(first.events(), ["first only", "both"]);
        assert_eq!(second.events(), ["both"]);
        drop(first);
        lock_captured(&state).1.push("second only".to_owned());
        assert_eq!(second.events(), ["both", "second only"]);
        drop(second);
        assert_eq!(*lock_captured(&state), (0, Vec::new()));
    }

    #[test]
    fn parses_the_go_driver_values() {
        for (value, level) in [
            ("debug", Level::Debug),
            ("INFO", Level::Info),
            ("warn", Level::Warn),
            ("Warning", Level::Warn),
            ("error", Level::Error),
        ] {
            assert_eq!(parse(value), Some(level), "{value}");
        }
        for value in ["", "off", "trace", "warn ", "2"] {
            assert_eq!(parse(value), None, "{value}");
        }
    }

    #[test]
    fn a_level_admits_itself_and_everything_more_severe() {
        assert!(Level::Error < Level::Warn && Level::Warn < Level::Info);
        assert!(Level::Info < Level::Debug);
    }
}
