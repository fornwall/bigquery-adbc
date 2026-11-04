//! Driver builds, child processes, run identifiers and interrupt handling shared by the suites.

use std::fmt::Display;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

pub(crate) const REPOSITORY_ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Reports progress on the real standard error: libtest holds back `eprintln!` until a test ends,
/// and these suites run for minutes.
pub(crate) fn progress(message: impl Display) {
    // A closed standard error must not fail a suite whose datasets still need dropping.
    let _ = writeln!(std::io::stderr(), ">> {message}");
}

/// Builds the driver's `cdylib` and returns the path Cargo reports for it.
///
/// The artifact can move with `CARGO_TARGET_DIR`, `build.target` and similar settings, and a
/// guessed path could select a stale library. The manifest declares only the rlib, so the
/// `cdylib` is requested here.
pub(crate) fn build_driver_library(cargo_arguments: &[&str]) -> PathBuf {
    let output = Command::new(env!("CARGO"))
        .args(["rustc", "--locked", "--lib", "--crate-type", "cdylib"])
        .arg("--message-format=json-render-diagnostics")
        .args(cargo_arguments)
        .current_dir(REPOSITORY_ROOT)
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to run cargo");
    assert!(output.status.success(), "cargo failed to build the driver");
    driver_library(&String::from_utf8(output.stdout).unwrap())
}

fn driver_library(cargo_messages: &str) -> PathBuf {
    let libraries: Vec<String> = cargo_messages
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|message| {
            message["reason"] == "compiler-artifact"
                && message["target"]["name"] == "adbc_driver_bigquery"
        })
        .flat_map(|message| message["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|filename| filename.as_str().map(str::to_owned))
        .filter(|filename| filename.ends_with(".so") || filename.ends_with(".dylib"))
        .collect();
    match <[String; 1]>::try_from(libraries) {
        Ok([library]) => PathBuf::from(library),
        Err(found) => panic!("cargo reported {} driver libraries: {found:?}", found.len()),
    }
}

/// Runs a command that must succeed, with its output on the terminal.
#[track_caller]
pub(crate) fn run_checked(command: &mut Command) {
    let program = command.get_program().to_owned();
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{} could not be started: {error}", program.display()));
    assert!(status.success(), "{command:?} failed with {status}");
}

/// A dataset name component unique across concurrent runs.
pub(crate) fn run_id() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let process = std::process::id();
    format!("{}_{process}_{:08x}", now.as_secs(), now.subsec_nanos())
}

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

extern "C" fn record_interrupt(_signal: libc::c_int) {
    INTERRUPTED.store(true, Ordering::SeqCst);
}

/// Turns SIGINT and SIGTERM into [`interrupted`] for the rest of the process.
///
/// Their default action ends the process without running `Drop`, which would leak the datasets of
/// the running case. A handler, unlike `SIG_IGN`, resets on `exec`, so a Ctrl-C still reaches the
/// client process. Cargo exits at once, so cleanup finishes after it returns.
pub(crate) fn defer_interrupts() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let handler = record_interrupt as extern "C" fn(libc::c_int) as libc::sighandler_t;
        for signal in [libc::SIGINT, libc::SIGTERM] {
            // SAFETY: the handler only stores to an atomic, which is async-signal-safe, and it
            // outlives the process as a static function.
            let previous = unsafe { libc::signal(signal, handler) };
            assert_ne!(previous, libc::SIG_ERR, "failed to handle signal {signal}");
        }
    });
}

pub(crate) fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

/// Runs a client until it exits or the run is interrupted.
pub(crate) fn run_client(command: &mut Command) -> std::io::Result<ExitStatus> {
    run_until_stopped(command, interrupted, Duration::from_secs(5))
}

/// Ends a client that outlives `stopped` by `grace`. A client cannot be left to stop by itself:
/// DuckDB turns SIGINT into one failed query, after which pytest moves on to the next test, and
/// Cargo has already given the terminal back, so a second Ctrl-C would not reach it.
fn run_until_stopped(
    command: &mut Command,
    stopped: impl Fn() -> bool,
    grace: Duration,
) -> std::io::Result<ExitStatus> {
    let mut child = command.spawn()?;
    let mut deadline = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if stopped() && Instant::now() >= *deadline.get_or_insert_with(|| Instant::now() + grace) {
            child.kill()?;
            return child.wait();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_client_that_survives_an_interrupt_is_killed() {
    let started = Instant::now();
    let mut client = Command::new("sh");
    client.args(["-c", "trap '' INT TERM; exec sleep 60"]);
    let status = run_until_stopped(&mut client, || true, Duration::ZERO).unwrap();
    assert!(!status.success());
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[test]
fn a_client_that_finishes_keeps_its_status() {
    let mut client = Command::new("sh");
    client.args(["-c", "exit 7"]);
    let status = run_until_stopped(&mut client, || false, Duration::ZERO).unwrap();
    assert_eq!(status.code(), Some(7));
}
