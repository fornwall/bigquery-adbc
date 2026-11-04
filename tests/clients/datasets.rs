//! Ownership of the BigQuery datasets a suite runs in.

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_bigquery::{
    BigQueryConnection, BigQueryDatabase, BigQueryDatabaseOption, BigQueryDriver,
};
use arrow_array::{Array, StringArray};

use crate::project_config::project;
use crate::support::progress;

const OWNER_LABEL_KEY: &str = "bigquery_adbc";
const RUN_LABEL_KEY: &str = "run_id";
const LOCATION: &str = "US";

/// How a suite creates and drops one dataset.
pub(crate) trait DatasetLifecycle {
    fn create(&self, dataset: &str) -> Result<(), String>;
    fn drop_dataset(&self, dataset: &str) -> Result<(), String>;
}

/// Datasets to drop, including when a test panics. Ownership starts before creation, so that a
/// request that failed after reaching BigQuery is still dropped, and survives a failed drop for a
/// retry.
#[derive(Debug)]
pub(crate) struct OwnedDatasets<L: DatasetLifecycle> {
    lifecycle: L,
    names: Vec<String>,
}

impl<L: DatasetLifecycle> OwnedDatasets<L> {
    pub(crate) fn new(lifecycle: L) -> Self {
        Self {
            lifecycle,
            names: Vec::new(),
        }
    }

    pub(crate) fn create(&mut self, dataset: &str) -> Result<(), String> {
        self.names.push(dataset.to_owned());
        self.lifecycle.create(dataset)
    }

    /// Drops every owned dataset, newest first, and keeps ownership of those that remain.
    pub(crate) fn release(&mut self) -> Result<(), String> {
        let mut errors = Vec::new();
        let mut remaining = Vec::new();
        for dataset in std::mem::take(&mut self.names).into_iter().rev() {
            if let Err(error) = self.lifecycle.drop_dataset(&dataset) {
                errors.push(error);
                remaining.insert(0, dataset);
            }
        }
        self.names = remaining;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl<L: DatasetLifecycle> Drop for OwnedDatasets<L> {
    fn drop(&mut self) {
        if !self.names.is_empty()
            && let Err(error) = self.release()
        {
            progress(format!("leaked: {error}"));
        }
    }
}

/// Creates datasets labelled with their owner and run, and drops only datasets that still carry
/// both labels. The driver runs in this process, so an interrupt cannot end a drop halfway.
pub(crate) struct LabelledDatasets {
    database: BigQueryDatabase,
    owner: &'static str,
    run_id: String,
}

impl LabelledDatasets {
    /// Connects and runs a query, so that missing credentials fail before any long build.
    pub(crate) fn connect(owner: &'static str, run_id: &str) -> Self {
        let options: [(OptionDatabase, OptionValue); 1] =
            [(BigQueryDatabaseOption::ProjectId.into(), project().into())];
        let database = BigQueryDriver::try_new()
            .and_then(|mut driver| driver.new_database_with_opts(options))
            .unwrap();
        let datasets = Self {
            database,
            owner,
            run_id: run_id.to_owned(),
        };
        if let Err(error) = datasets.execute("SELECT 1") {
            panic!(
                "BigQuery is unavailable; check `gcloud auth application-default login`: {error}"
            );
        }
        datasets
    }

    /// Each statement gets its own connection: a suite can leave this one idle for an hour.
    fn connection(&self) -> Result<BigQueryConnection, Error> {
        self.database.new_connection()
    }

    fn execute(&self, sql: &str) -> Result<(), Error> {
        let mut connection = self.connection()?;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(sql)?;
        statement.execute_update().map(|_| ())
    }

    /// Asks the dataset itself: `INFORMATION_SCHEMA` can lag a `CREATE` that landed late, for
    /// example after an interrupted run.
    fn exists(&self, dataset: &str) -> Result<bool, Error> {
        let mut connection = self.connection()?;
        match connection.set_option(OptionConnection::CurrentSchema, dataset.into()) {
            Ok(()) => Ok(true),
            Err(error) if error.status == Status::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn labels(&self, dataset: &str) -> Result<Option<String>, Error> {
        let project = project();
        let region = format!("region-{}", LOCATION.to_lowercase());
        let mut connection = self.connection()?;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(format!(
            "SELECT option_value \
             FROM `{project}`.`{region}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS \
             WHERE schema_name = '{dataset}' AND option_name = 'labels'"
        ))?;
        for batch in statement.execute()? {
            let batch = batch?;
            let values = batch.column(0).as_any().downcast_ref::<StringArray>();
            if let Some(values) = values.filter(|values| !values.is_empty()) {
                return Ok(Some(values.value(0).to_owned()));
            }
        }
        Ok(None)
    }
}

impl DatasetLifecycle for LabelledDatasets {
    fn create(&self, dataset: &str) -> Result<(), String> {
        progress(format!("creating BigQuery dataset {dataset}"));
        let (project, owner, run_id) = (project(), self.owner, &self.run_id);
        // If a run dies before cleanup, BigQuery expires its tables; the empty dataset remains.
        self.execute(&format!(
            "CREATE SCHEMA `{project}.{dataset}` OPTIONS(location = '{LOCATION}', \
             default_table_expiration_days = 1, \
             labels = [('{OWNER_LABEL_KEY}', '{owner}'), ('{RUN_LABEL_KEY}', '{run_id}')])"
        ))
        .map_err(|error| format!("failed to create {dataset}: {error}"))
    }

    fn drop_dataset(&self, dataset: &str) -> Result<(), String> {
        progress(format!("dropping owned BigQuery dataset {dataset}"));
        let project = project();
        let failed = |error: Error| format!("failed to drop {dataset}: {error}");
        if !self.exists(dataset).map_err(failed)? {
            return Ok(());
        }
        let labels = self.labels(dataset).map_err(failed)?;
        check_ownership(labels.as_deref(), self.owner, &self.run_id)
            .map_err(|reason| format!("refusing to drop {project}.{dataset}: {reason}"))?;
        self.execute(&format!("DROP SCHEMA `{project}.{dataset}` CASCADE"))
            .map_err(failed)
    }
}

/// `DROP SCHEMA ... CASCADE` deletes everything in a dataset, so a dataset must prove it is this
/// run's. One without visible labels stays owned, which fails the run instead of guessing.
fn check_ownership(labels: Option<&str>, owner: &str, run_id: &str) -> Result<(), String> {
    let Some(labels) = labels else {
        return Err(
            "it exists, but INFORMATION_SCHEMA shows no labels for it yet; check its labels and \
             drop it by hand"
                .to_owned(),
        );
    };
    if has_label(labels, OWNER_LABEL_KEY, owner) && has_label(labels, RUN_LABEL_KEY, run_id) {
        Ok(())
    } else {
        Err(format!(
            "it is not labelled as owned by run {run_id} (labels: {labels})"
        ))
    }
}

/// Whether a `SCHEMATA_OPTIONS` labels value, an `ARRAY<STRUCT<STRING, STRING>>` literal, carries
/// one pair. BigQuery labels cannot contain whitespace.
fn has_label(labels: &str, key: &str, value: &str) -> bool {
    let labels: String = labels.split_whitespace().collect();
    labels.contains(&format!(r#"STRUCT("{key}","{value}")"#))
}

#[cfg(test)]
mod false_success {
    use std::cell::RefCell;

    use super::*;

    const OWNER: &str = "duckdb_integration";
    const RUN: &str = "1793800604_3394134_0adecd60";

    #[test]
    fn only_a_dataset_with_both_labels_can_be_dropped() {
        let owned = format!(r#"[STRUCT("bigquery_adbc", "{OWNER}"), STRUCT("run_id", "{RUN}")]"#);
        assert_eq!(check_ownership(Some(&owned), OWNER, RUN), Ok(()));

        let another_run = owned.replace(RUN, "1_1_00000000");
        for labels in [
            "[]",
            another_run.as_str(),
            r#"[STRUCT("bigquery_adbc", "foundry_validation")]"#,
            r#"[STRUCT("run_id", "1793800604_3394134_0adecd60")]"#,
            // Near matches of the key and of the value.
            r#"[STRUCT("bigquery_adbc_x", "duckdb_integration"), STRUCT("run_id", "1793800604_3394134_0adecd60")]"#,
            r#"[STRUCT("bigquery_adbc", "duckdb_integration_x"), STRUCT("run_id", "1793800604_3394134_0adecd60")]"#,
        ] {
            assert!(
                check_ownership(Some(labels), OWNER, RUN).is_err(),
                "{labels}"
            );
        }
    }

    #[test]
    fn a_dataset_without_visible_labels_is_not_dropped() {
        let error = check_ownership(None, OWNER, RUN).unwrap_err();
        assert!(error.contains("shows no labels"), "{error}");
    }

    #[derive(Default)]
    struct FakeLifecycle {
        failing: Vec<&'static str>,
        calls: RefCell<Vec<String>>,
    }

    impl FakeLifecycle {
        fn record(&self, action: &str, dataset: &str) -> Result<(), String> {
            self.calls.borrow_mut().push(format!("{action} {dataset}"));
            if self.failing.contains(&dataset) {
                Err(format!("{action} {dataset} failed"))
            } else {
                Ok(())
            }
        }
    }

    impl DatasetLifecycle for &FakeLifecycle {
        fn create(&self, dataset: &str) -> Result<(), String> {
            self.record("create", dataset)
        }

        fn drop_dataset(&self, dataset: &str) -> Result<(), String> {
            self.record("drop", dataset)
        }
    }

    #[test]
    fn a_failed_create_is_still_dropped_newest_first() {
        let lifecycle = FakeLifecycle {
            failing: vec!["second"],
            ..Default::default()
        };
        {
            let mut datasets = OwnedDatasets::new(&lifecycle);
            datasets.create("first").unwrap();
            assert!(datasets.create("second").is_err());
        }
        assert_eq!(
            *lifecycle.calls.borrow(),
            ["create first", "create second", "drop second", "drop first"]
        );
    }

    #[test]
    fn a_failed_drop_stays_owned_and_is_reported() {
        let lifecycle = FakeLifecycle {
            failing: vec!["first"],
            ..Default::default()
        };
        let mut datasets = OwnedDatasets::new(&lifecycle);
        datasets.names.push("first".to_owned());
        assert!(datasets.release().is_err());
        assert_eq!(datasets.names, ["first"]);
        assert!(datasets.release().is_err());
        datasets.names.clear();
    }
}
