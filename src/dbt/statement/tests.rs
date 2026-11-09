use adbc_core::error::Status;
use adbc_core::options::OptionStatement;
use adbc_core::{Optionable as _, Statement as _};

use crate::BigQueryStatementOption as Key;
use crate::test_support::driver::test_statement;
use crate::test_support::errors::assert_failure;
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
            let mut statement = test_statement();
            set_option(&mut statement, first.clone(), *first_value);
            set_option(&mut statement, second.clone(), *second_value);
            assert_string_option(&statement, first.clone(), None);
            assert!(statement.get_option_string(second.clone()).is_ok());
        }
    }
}

/// A table metadata update neither clears nor is cleared by another operation's setters, and the
/// dataset operations run before it, so they refuse the pair rather than silently dropping it.
#[test]
fn a_dataset_operation_cannot_be_combined_with_a_table_metadata_update() {
    for (option, value) in [
        (Key::AuthorizeViewToDatasets, AUTHORIZATION),
        (Key::GetDatasetLocation, LOCATION),
    ] {
        for (update, update_value) in [
            (
                Key::UpdateColumnsDescription,
                r#"{"id":"Required identifier."}"#,
            ),
            (Key::UpdateDescription, "A documented relation."),
        ] {
            let mut statement = test_statement();
            set_option(&mut statement, option, value);
            set_option(&mut statement, update, update_value);
            let error = statement.execute_update().unwrap_err();
            assert_failure(
                &error,
                Status::InvalidState,
                &["cannot be combined", option.as_str(), update.as_str()],
            );
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
    ] {
        set_option(&mut statement, option, AUTHORIZATION);
        statement.set_sql_query(query).unwrap();
        assert!(
            statement.get_option_string(option.into()).is_ok(),
            "dbt 2.0's placeholder must not discard the authorization: {query:?}"
        );
    }

    for query in ["select 1", "none of your business", "/* c */ select none"] {
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
