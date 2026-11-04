//! Write the Python option enums generated from the compiled Rust declarations.

#![allow(
    clippy::exit,
    clippy::print_stderr,
    reason = "this developer-only command reports CLI misuse and failed generation before exiting"
)]

use std::env;
use std::fs;

use adbc_driver_bigquery::python_options::{generated_path, render};

fn main() {
    // Staleness is checked by the `checked_python_options_match_compiled_rust_metadata` unit
    // test, which `cargo test --all-features` already runs; this binary only writes.
    if let Some(argument) = env::args().nth(1) {
        eprintln!("unexpected argument {argument:?}; this command takes none");
        std::process::exit(2);
    }

    let path = generated_path();
    if let Err(error) = fs::write(&path, render()) {
        eprintln!("could not write {}: {error}", path.display());
        std::process::exit(1);
    }
}
