//! Opt-in operator diagnostics on standard error.
//!
//! Some events have no ADBC caller to report to, such as a Storage Read fallback, a failed
//! background cancel or a retried request. They are written here only when the process sets
//! [`LOG_LEVEL`] (the variable and values the Go driver reads), which is read once per process.
//! Each event is one `[bigquery] LEVEL message` line. A site whose service error text could echo
//! SQL, such as a failed job cancel, reports only the status.
//!
//! This is not a `tracing` subscriber: the driver's `tracing` is private to the shared library,
//! and raising its compile-time cap would link every dependency's events too (see
//! `docs/BINARY-SIZE.md`).

use std::fmt::Arguments;
use std::io::Write as _;
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard, PoisonError};

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
                "[bigquery] ignoring {LOG_LEVEL}={}: expected debug, info, warn or error\n",
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
    write_line(format_args!("[bigquery] {} {message}\n", level.name()));
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
    let _ = std::io::stderr().write_all(line.to_string().as_bytes());
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
    use super::{CaptureGuard, Level, Mutex, lock_captured, parse};

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
