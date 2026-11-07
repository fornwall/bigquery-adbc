//! dbt 2.0 against the driver, through the dbt that `tools/dbt-build` builds.
//!
//! The suite is `integration/dbt/`, and `run.py` builds dbt and judges the results itself.

use std::process::Command;

use crate::project_config::project;
use crate::support::{REPOSITORY_ROOT, interrupted, progress, run_checked};

/// Owns no datasets, unlike the other suites: `run.py` retains them after any failure for
/// inspection, and its next run starts by dropping them. Ctrl-C therefore needs no deferring and
/// goes straight to `run.py`, which stops at once.
#[test]
#[ignore = "builds dbt and runs it against BigQuery; needs uv, protoc and Application Default Credentials"]
fn suite_passes_against_bigquery() {
    project();
    assert!(!interrupted(), "interrupted before dbt");
    progress("running the dbt suite");
    run_checked(
        Command::new("uv")
            .args([
                "run",
                "--directory",
                "integration/dbt",
                "--locked",
                "run.py",
            ])
            .current_dir(REPOSITORY_ROOT),
    );
}
