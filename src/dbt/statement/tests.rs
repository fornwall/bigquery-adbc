use adbc_core::error::Status;
use adbc_core::options::OptionStatement;
use adbc_core::{Connection as _, Optionable as _, Statement as _};

use crate::BigQueryDatabaseOption as Database;
use crate::BigQueryStatementOption as Key;
use crate::test_support::driver::{test_connection_with, test_statement};
use crate::test_support::errors::assert_failure;
use crate::test_support::http::capture_requests;
use crate::test_support::options::{assert_string_option, set_option};

const AUTHORIZATION: &str =
    r#"{"data-project.analytics.granted_view":[{"project":"data-project","dataset":"raw"}]}"#;
const LOCATION: &str = r#"{"project":"data-project","dataset":"raw"}"#;

/// Every dbt execution mode and a bulk ingest replace one another, whichever is set first.
#[test]
fn each_dbt_operation_replaces_every_other_execution_mode() {
    let modes: [(OptionStatement, &str); 6] = [
        (Key::AuthorizeViewToDatasets.into(), AUTHORIZATION),
        (Key::GetDatasetLocation.into(), LOCATION),
        (Key::CsvFilepath.into(), "/tmp/seed.csv"),
        (Key::CopyTableSource.into(), "data-project.analytics.source"),
        (
            Key::CreateBatchParent.into(),
            "projects/data-project/locations/eu",
        ),
        (OptionStatement::TargetTable, "ingested"),
    ];
    for (first, first_value) in &modes {
        for (second, second_value) in &modes {
            if first == second {
                continue;
            }
            // A copy has two options, and leaving it clears both.
            let copy = *first == Key::CopyTableSource.into();
            let destination = OptionStatement::from(Key::CopyTableDestination);
            let mut statement = test_statement();
            set_option(&mut statement, first.clone(), *first_value);
            if copy {
                set_option(&mut statement, destination.clone(), "analytics.copy");
            }
            set_option(&mut statement, second.clone(), *second_value);
            assert_string_option(&statement, first.clone(), None);
            if copy {
                assert_string_option(&statement, destination, None);
            }
            assert!(statement.get_option_string(second.clone()).is_ok());
        }
    }
}

/// dbt 2.0 sends `sql = "none"` behind its query comment, so only that exact placeholder is
/// discarded. Any other query wins, leaving the authorization behind rather than being silently
/// ignored.
#[test]
fn only_the_dbt_placeholder_query_keeps_an_authorization() {
    let option = Key::AuthorizeViewToDatasets;
    let mut statement = test_statement();
    for query in [
        "none",
        "NONE",
        "/* bigquery-adbc: model.project.granted_view */\nnone",
        "none\n/* bigquery-adbc: model.project.granted_view */",
        "-- a line comment\nnone",
        "",
        // A terminator may precede trailing comments.
        "none; -- node comment",
        "none; # node comment",
        "/* leading */ none /* before terminator */; /* after terminator */",
    ] {
        set_option(&mut statement, option, AUTHORIZATION);
        statement.set_sql_query(query).unwrap();
        assert!(
            statement.get_option_string(option.into()).is_ok(),
            "dbt 2.0's placeholder must not discard the authorization: {query:?}"
        );
    }

    for query in [
        "select 1",
        "none of your business",
        "/* c */ select none",
        "none; /* comment */ SELECT 2",
    ] {
        set_option(&mut statement, option, AUTHORIZATION);
        statement.set_sql_query(query).unwrap();
        assert_string_option(&statement, option, None);
    }

    // The placeholder is dropped rather than stored, so clearing the option afterwards
    // reports a missing query instead of sending `none` to BigQuery.
    set_option(&mut statement, option, AUTHORIZATION);
    statement.set_sql_query("none").unwrap();
    set_option(&mut statement, option, "");
    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["without a SQL query"]);
}

/// A dry run validates without executing, so no dbt operation that changes something may run
/// under one: each is refused before it sends a request.
#[test]
fn a_dry_run_never_runs_an_operation_that_changes_something() {
    const REGION: (Key, &str) = (Key::DataprocComputeRegion, "us-central1");
    const PARENT: &str = "projects/exec-project/locations/us-central1";
    let operations: [(&str, &[(Key, &str)]); 7] = [
        (
            "table copy",
            &[
                (Key::CopyTableSource, "data-project.analytics.source"),
                (Key::CopyTableDestination, "data-project.analytics.copy"),
            ],
        ),
        (
            "authorized-view update",
            &[(Key::AuthorizeViewToDatasets, AUTHORIZATION)],
        ),
        (
            "table metadata update",
            &[
                (Key::DestinationTable, "data-project.analytics.documented"),
                (Key::UpdateDescription, "A documented table."),
            ],
        ),
        (
            "Cloud Storage object write",
            &[
                (Key::WriteGcsBucket, "dbt-python"),
                (Key::WriteGcsObjectName, "analytics/model.py"),
            ],
        ),
        (
            "Dataproc batch submission",
            &[
                REGION,
                (Key::CreateBatchParent, PARENT),
                (Key::CreateBatchBatchYml, "pyspark_batch: {}"),
            ],
        ),
        (
            "Dataproc job submission",
            &[
                REGION,
                (Key::DataprocProject, "exec-project"),
                (Key::DataprocSubmitJobClusterName, "dbt-cluster"),
                (Key::DataprocSubmitJobGcsPath, "gs://dbt-python/model.py"),
            ],
        ),
        (
            "Vertex AI notebook execution",
            &[
                (Key::CreateBatchBatchId, "notebook-run"),
                (Key::NotebookExecuteJobParent, PARENT),
                (Key::NotebookExecuteJobGscPath, "gs://dbt-python/model.py"),
                (Key::NotebookExecuteJobGscBucket, "dbt-python"),
                (Key::NotebookExecuteJobModelFileName, "analytics/model.py"),
                (Key::NotebookExecuteJobProject, "exec-project"),
                (Key::NotebookExecuteJobRegion, "us-central1"),
                (Key::NotebookExecuteJobTemplateId, "1234"),
            ],
        ),
    ];
    for (subject, options) in operations {
        // Every service is the mock, which expects no request at all.
        let (endpoint, server) = capture_requests(Vec::new());
        let services = [
            Database::GcsEndpoint,
            Database::DataprocEndpoint,
            Database::VertexEndpoint,
        ];
        let mut connection = test_connection_with(
            &endpoint,
            services.map(|service| (service.into(), endpoint.clone().into())),
        );
        let mut statement = connection.new_statement().unwrap();
        for (option, value) in options {
            set_option(&mut statement, *option, *value);
        }
        set_option(&mut statement, Key::DryRun, "true");
        // dbt 2.0's statement finalization, which every one of these keeps.
        statement.set_sql_query("").unwrap();

        let refused = statement.execute_update().unwrap_err();
        assert_failure(
            &refused,
            Status::NotImplemented,
            &[subject, "cannot be dry-run", Key::DryRun.as_str()],
        );
        assert!(server.join().unwrap().is_empty(), "{subject}");
    }
}

/// An operation without an option it needs names that option before any request.
#[test]
fn an_operation_missing_an_option_it_needs_names_it() {
    const SEED: (Key, &str) = (Key::CsvFilepath, "/tmp/seed.csv");
    const DESTINATION: (Key, &str) = (Key::DestinationTable, "data-project.analytics.seed");
    let assert_missing = |options: &[(Key, &str)], missing: Key, subject: &str| {
        let mut statement = test_statement();
        for (option, value) in options {
            set_option(&mut statement, *option, *value);
        }
        let error = statement.execute_update().unwrap_err();
        assert_failure(
            &error,
            Status::InvalidState,
            &[&format!(
                "BigQuery option {} is required by the BigQuery {subject}",
                missing.as_str()
            )],
        );
    };
    assert_missing(&[SEED], Key::DestinationTable, "CSV seed load");
    assert_missing(&[SEED, DESTINATION], Key::CsvSchema, "CSV seed load");
    assert_missing(
        &[(Key::UpdateDescription, "A documented table.")],
        Key::DestinationTable,
        "table metadata update",
    );
}
