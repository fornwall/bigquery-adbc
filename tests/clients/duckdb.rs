//! The DuckDB `adbc` community extension, a third-party client that controls the ADBC calls.
//!
//! The suite is `integration/duckdb/`.

use crate::datasets::LabelledDatasets;
use crate::project_config::project;
use crate::pytest::PytestSuite;
use crate::support::{build_driver_library, progress, run_checked, run_id};

const SUITE: PytestSuite = PytestSuite {
    directory: "duckdb",
    dataset_tag: "duckdb",
    owner_label: "duckdb_integration",
};

/// The extension loads the driver by absolute path out of a connection profile, so the suite needs
/// the `cdylib` itself rather than an installed wheel.
#[test]
#[ignore = "creates and drops BigQuery datasets; needs uv and Application Default Credentials"]
fn suite_passes_against_bigquery() {
    project();
    let run_id = run_id();
    let datasets = LabelledDatasets::connect(SUITE.owner_label, &run_id);

    progress("building the release driver");
    let library = build_driver_library(&["--release"]);
    let python = SUITE.sync_environment();

    // Download the extension once, so the test processes only load it.
    progress("installing the DuckDB adbc community extension");
    run_checked(
        std::process::Command::new(&python)
            .arg("-I")
            .arg(SUITE.root().join("install_extension.py")),
    );

    SUITE.run_against_bigquery(
        datasets,
        &python,
        &run_id,
        &[("ADBC_BIGQUERY_LIBRARY", library.as_os_str())],
    );
}
