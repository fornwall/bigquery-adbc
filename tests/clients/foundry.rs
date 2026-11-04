//! The pinned `adbc-drivers/validation` suite against an installed `bigquery-adbc` wheel.
//!
//! The suite is `integration/foundry-validation/`. One run at a time per checkout: the installed
//! wheel and Python environment are shared.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::datasets::LabelledDatasets;
use crate::project_config::project;
use crate::pytest::PytestSuite;
use crate::support::{REPOSITORY_ROOT, progress, run_checked, run_id};

const SUITE: PytestSuite = PytestSuite {
    directory: "foundry-validation",
    dataset_tag: "foundry",
    owner_label: "foundry_validation",
};

fn build_wheel(directory: &Path) -> PathBuf {
    progress("building release Python wheel");
    // Cargo checks freshness on every run, so this checkout's target directory is a safe cache.
    let target_directory = std::env::var_os("CARGO_TARGET_DIR")
        .unwrap_or_else(|| OsString::from(format!("{REPOSITORY_ROOT}/target")));
    run_checked(
        Command::new("uv")
            .args(["build", "--wheel", "--out-dir"])
            .arg(directory)
            .arg("python")
            .current_dir(REPOSITORY_ROOT)
            .env("CARGO_TARGET_DIR", target_directory)
            .env_remove("BIGQUERY_ADBC_BUILD_LIBRARY")
            .env_remove("BIGQUERY_ADBC_LIBRARY"),
    );
    let wheels: Vec<PathBuf> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "whl"))
        .collect();
    match <[PathBuf; 1]>::try_from(wheels) {
        Ok([wheel]) => wheel,
        Err(found) => panic!("expected one built wheel, found {found:?}"),
    }
}

fn install_wheel(python: &Path, wheel: &Path) {
    progress("installing current checkout wheel");
    run_checked(
        Command::new("uv")
            .args(["pip", "install", "--no-deps", "--reinstall", "--python"])
            .arg(python)
            .arg(wheel),
    );
    run_checked(
        Command::new("uv")
            .args(["pip", "check", "--python"])
            .arg(python),
    );
}

#[test]
#[ignore = "creates and drops BigQuery datasets; needs uv and Application Default Credentials"]
fn suite_passes_against_bigquery() {
    project();
    let run_id = run_id();
    let datasets = LabelledDatasets::connect(SUITE.owner_label, &run_id);

    let wheel_directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("wheel-{run_id}"));
    let wheel = build_wheel(&wheel_directory);
    let python = SUITE.sync_environment();
    install_wheel(&python, &wheel);
    fs::remove_dir_all(&wheel_directory).unwrap();

    SUITE.run_against_bigquery(datasets, &python, &run_id, &[]);
}

/// Runs pytest without a driver or credentials. Foundry resolves dataset names during
/// parametrization even without a connection, so they need placeholder values.
fn inspect(arguments: &[&str]) {
    project();
    let python = SUITE.sync_environment();
    run_checked(
        SUITE
            .pytest(&python)
            .args(arguments)
            .env("ADBC_BIGQUERY_DATASET", "foundry_collection_primary")
            .env(
                "ADBC_BIGQUERY_SECONDARY_DATASET",
                "foundry_collection_secondary",
            ),
    );
}

#[test]
#[ignore = "synchronizes the suite's Python environment, which needs uv"]
fn collection_matches_baseline() {
    inspect(&["--collect-only", "-q"]);
}

/// Prints the query fixtures, for selecting and editing them.
#[test]
#[ignore = "synchronizes the suite's Python environment, which needs uv"]
fn show_queries() {
    inspect(&["--show-queries", "-s"]);
}
