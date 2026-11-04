//! Client suites whose tests are a locked `uv` project running pytest.
//!
//! The environment variable `PYTEST_ADDOPTS` carries pytest options such as `-k` or `--junitxml`
//! into these tests; relative paths resolve from the suite's directory.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::datasets::{LabelledDatasets, OwnedDatasets};
use crate::support::{
    REPOSITORY_ROOT, defer_interrupts, interrupted, progress, run_checked, run_client,
};

#[derive(Debug)]
pub(crate) struct PytestSuite {
    /// Below `integration/`.
    pub(crate) directory: &'static str,
    /// Names the run's datasets, as `bigquery_adbc_<dataset_tag>_<run id>_<role>`.
    pub(crate) dataset_tag: &'static str,
    pub(crate) owner_label: &'static str,
}

impl PytestSuite {
    pub(crate) fn root(&self) -> PathBuf {
        Path::new(REPOSITORY_ROOT)
            .join("integration")
            .join(self.directory)
    }

    /// Synchronizes the suite's locked environment and returns its interpreter.
    pub(crate) fn sync_environment(&self) -> PathBuf {
        progress(format!(
            "synchronizing the locked {} environment",
            self.directory
        ));
        let root = self.root();
        run_checked(
            Command::new("uv")
                .args(["sync", "--locked", "--python", "3.15", "--project"])
                .arg(&root),
        );
        root.join(".venv/bin/python")
    }

    /// `-I` keeps the checkout and `PYTHON*` variables from changing what the suite imports.
    pub(crate) fn pytest(&self, python: &Path) -> Command {
        let mut command = Command::new(python);
        command
            .args(["-I", "-m", "pytest"])
            .current_dir(self.root());
        command
    }

    /// Runs pytest in a primary and a secondary dataset owned by this run.
    pub(crate) fn run_against_bigquery(
        &self,
        lifecycle: LabelledDatasets,
        python: &Path,
        run_id: &str,
        environment: &[(&str, &OsStr)],
    ) {
        defer_interrupts();
        assert!(!interrupted(), "interrupted before {}", self.directory);

        let tag = self.dataset_tag;
        let primary = format!("bigquery_adbc_{tag}_{run_id}_primary");
        let secondary = format!("bigquery_adbc_{tag}_{run_id}_secondary");
        let mut datasets = OwnedDatasets::new(lifecycle);
        datasets.create(&primary).unwrap();
        datasets.create(&secondary).unwrap();

        // A Ctrl-C during creation reached no client, so it ends the run here.
        let status = (!interrupted()).then(|| {
            progress(format!("running the {} suite", self.directory));
            run_client(
                self.pytest(python)
                    .envs(environment.iter().copied())
                    .env("ADBC_BIGQUERY_DATASET", &primary)
                    .env("ADBC_BIGQUERY_SECONDARY_DATASET", &secondary),
            )
            .expect("failed to run pytest")
        });

        // A test failure outranks a cleanup failure, which must still name what it left behind.
        let cleanup = datasets.release();
        if let Err(error) = &cleanup {
            progress(format!("leaked: {error}"));
        }
        let status = status.unwrap_or_else(|| panic!("interrupted before pytest started"));
        assert!(status.success(), "pytest failed with {status}");
        cleanup.unwrap();
    }
}
