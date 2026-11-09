//! The one-execution-mode-at-a-time rule: which options leave which mode, which combinations are
//! rejected outright, and the placeholder queries dbt 2.0 finalizes an option-only statement
//! with.

use super::*;

#[test]
fn clearing_the_ingest_target_keeps_the_sql_query() {
    let mut statement = test_statement();
    statement.set_sql_query("SELECT 1").unwrap();
    set_option(&mut statement, OptionStatement::TargetTable, String::new());

    assert_eq!(statement.sql.as_deref(), Some("SELECT 1"));
    assert!(statement.ingest.target_table.is_none());

    // A non-empty target still switches the statement to ingest mode.
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    assert!(statement.sql.is_none());
    assert_eq!(statement.ingest.target_table.as_deref(), Some("events"));
}

#[test]
fn setting_a_sql_query_clears_the_whole_ingest_target() {
    let mut statement = test_statement();
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    set_option(
        &mut statement,
        OptionStatement::TargetCatalog,
        "fixture-project",
    );
    set_option(&mut statement, OptionStatement::TargetDbSchema, "dataset");

    statement.set_sql_query("SELECT 1").unwrap();

    assert!(statement.ingest.target_table.is_none());
    assert!(
        statement.ingest.target_catalog.is_none(),
        "a stale target catalog would silently redirect the next ingest"
    );
    assert!(statement.ingest.target_db_schema.is_none());
}

/// `submit_python_job` finalizes its statements with a placeholder `SELECT 1`, since dbt
/// 2.0 asserts a statement carries SQL or options. Only that exact statement is discarded; the
/// dbt-labs fork dispatches on the options first and so silently drops any query.
#[test]
fn the_select_one_placeholder_finalizes_a_python_model_while_real_sql_replaces_it() {
    let bucket = BqOption::WriteGcsBucket;
    let mut statement = test_statement();
    set_option(&mut statement, bucket, "dbt-python");

    for query in [
        "SELECT 1",
        "select 1",
        "  Select   1  ",
        "SELECT 1;",
        "/* bigquery-adbc: model.project.py_model */ select 1",
        "select 1 -- a trailing node comment",
        "select 1 # a trailing node comment",
        "select 1; -- a trailing node comment",
        "select 1; # a trailing node comment",
        "select 1 /* before terminator */; /* after terminator */",
        // A query carrying no statement finalizes it too, exactly as it finalizes a copy.
        "",
        "-- only a comment\n",
        "/* unterminated",
    ] {
        statement.set_sql_query(query).unwrap();
        assert_string_option(&statement, bucket, Some("dbt-python"));
    }

    // The placeholder is dropped rather than stored, so releasing the option afterwards
    // reports a missing query instead of sending `SELECT 1` to BigQuery.
    statement.set_sql_query("SELECT 1").unwrap();
    set_option(&mut statement, bucket, "");
    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["without a SQL query"]);

    // Any other real statement wins, as it wins over a configured table copy.
    for query in [
        "select 2",
        "select 1, 2",
        "select 1 union all select 1",
        "select 1 from t",
        "select 1; /* another statement */ select 2",
        "insert into t values (1)",
    ] {
        set_option(&mut statement, bucket, "dbt-python");
        statement.set_sql_query(query).unwrap();
        assert_string_option(&statement, bucket, None);
    }
}

#[test]
fn dbt_placeholders_keep_a_dataset_location_lookup_and_real_sql_replaces_it() {
    let option = BqOption::GetDatasetLocation;
    let value = r#"{"project":"data-project","dataset":"analytics"}"#;
    let mut statement = test_statement();

    for query in [
        "",
        "-- only a comment\n",
        "/* {\"app\": \"dbt\"} */\n",
        "none",
        "/* node comment */ none;",
    ] {
        set_option(&mut statement, option, value);
        statement.set_sql_query(query).unwrap();
        assert_string_option(&statement, option, Some(value));
        assert!(statement.sql.is_none(), "{query}");
    }

    statement.set_sql_query("SELECT 1").unwrap();
    assert_string_option(&statement, option, None);
    assert_eq!(statement.sql.as_deref(), Some("SELECT 1"));

    // Setting the option leaves the query behind, as every option-only operation does.
    set_option(&mut statement, option, value);
    assert!(statement.sql.is_none());
}

#[test]
fn the_shared_batch_id_and_pooling_timeout_select_no_operation() {
    let mut statement = test_statement();
    for (option, value) in [
        (BqOption::CreateBatchBatchId, "batch-0001"),
        (BqOption::DataprocPoolingTimeout, "86400"),
        (BqOption::DataprocComputeRegion, "europe-west1"),
        (BqOption::DataprocProject, "compute-project"),
    ] {
        set_option(&mut statement, option, value);
    }

    // No operation is selected, so the statement still needs SQL.
    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["without a SQL query"]);
    // Nor do they discard a real query, since they select nothing.
    statement.set_sql_query("select 1").unwrap();
    assert_string_option(&statement, BqOption::CreateBatchBatchId, None);
}

/// Selecting a python-model operation is what discards the placeholder, so the keys that do
/// select one each keep a `SELECT 1` from reaching BigQuery.
#[test]
fn each_python_model_selector_finalizes_the_placeholder_and_rejects_a_result_schema() {
    for (option, value) in [
        (BqOption::WriteGcsBucket, "dbt-python"),
        (
            BqOption::CreateBatchParent,
            "projects/compute-project/locations/europe-west1",
        ),
        (BqOption::DataprocSubmitJobClusterName, "dbt-cluster"),
        (
            BqOption::NotebookExecuteJobParent,
            "projects/compute-project/locations/europe-west1",
        ),
    ] {
        let mut statement = test_statement();
        set_option(&mut statement, option, value.to_owned());
        statement.set_sql_query("SELECT 1").unwrap();
        assert_string_option(&statement, option, Some(value));
        assert_eq!(
            statement.execute_schema().unwrap_err().status,
            Status::InvalidState,
            "a python-model operation produces no rows to describe: {}",
            option.as_str()
        );
    }
}

#[test]
fn python_model_execution_follows_precedence_as_selectors_are_cleared() {
    use BqOption as Key;

    let mut statement = test_statement();
    let selectors = [
        (Key::CreateBatchParent, Key::CreateBatchBatchYml),
        (
            Key::DataprocSubmitJobClusterName,
            Key::DataprocSubmitJobGcsPath,
        ),
        (
            Key::NotebookExecuteJobParent,
            Key::NotebookExecuteJobGscPath,
        ),
        (Key::WriteGcsBucket, Key::WriteGcsObjectName),
    ];
    // Configure lower-precedence families last: setter order must not choose what executes.
    for (selector, _) in selectors {
        set_option(&mut statement, selector, "configured");
    }
    statement.set_sql_query("SELECT 1").unwrap();

    for (selector, missing) in selectors {
        let failure = statement.execute_update().unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidState,
            &[missing.as_str(), "required"],
        );
        set_option(&mut statement, selector, "");
    }
    let failure = statement.execute_update().unwrap_err();
    assert_message(&failure, &["without a SQL query"]);
}

/// A table metadata update neither clears nor is cleared by another operation's setters, so the
/// pair is refused at execution, whichever was set first.
#[test]
fn a_table_metadata_update_cannot_be_combined_with_another_operation() {
    let authorization =
        r#"{"data-project.analytics.granted_view":[{"project":"data-project","dataset":"raw"}]}"#;
    for (option, value, family) in [
        (
            BqOption::AuthorizeViewToDatasets.into(),
            authorization,
            BqOption::AuthorizeViewToDatasets.as_str(),
        ),
        (
            BqOption::GetDatasetLocation.into(),
            r#"{"project":"data-project","dataset":"raw"}"#,
            BqOption::GetDatasetLocation.as_str(),
        ),
        (
            BqOption::WriteGcsBucket.into(),
            "dbt-python",
            "adbc.bigquery.write_gcs.*",
        ),
        (
            BqOption::CreateBatchParent.into(),
            "projects/compute-project/locations/europe-west1",
            "adbc.bigquery.create_batch.*",
        ),
        (
            BqOption::DataprocSubmitJobClusterName.into(),
            "dbt-cluster",
            "adbc.bigquery.dataproc.submit_job.*",
        ),
        (
            BqOption::NotebookExecuteJobParent.into(),
            "projects/compute-project/locations/europe-west1",
            "adbc.bigquery.notebook_execute_job.*",
        ),
        (
            BqOption::CopyTableSource.into(),
            "analytics.copied",
            "adbc.bigquery.copy_table.*",
        ),
        (
            BqOption::CopyTableDestination.into(),
            "analytics.copied",
            "adbc.bigquery.copy_table.*",
        ),
        // The execute ladder reaches the ingest and the seed before the update, so those two
        // have to refuse the pair rather than dispatch into it.
        (OptionStatement::TargetTable, "events", "adbc.ingest.*"),
        (
            BqOption::CsvFilepath.into(),
            "/tmp/seed.csv",
            "adbc.bigquery.ingest.csv_*",
        ),
    ] {
        for (update, update_first) in [
            (BqOption::UpdateDescription, true),
            (BqOption::UpdateDescription, false),
            (BqOption::UpdateColumnsDescription, true),
            (BqOption::UpdateColumnsDescription, false),
        ] {
            let update_value = if update == BqOption::UpdateDescription {
                "a combined table"
            } else {
                r#"{"id":"Required identifier."}"#
            };
            let mut statement = test_statement();
            let mut settings = [
                (OptionStatement::from(update), update_value),
                (option.clone(), value),
            ];
            if !update_first {
                settings.swap(0, 1);
            }
            for (key, value) in settings {
                set_option(&mut statement, key, value.to_owned());
            }
            let error = statement.execute_update().unwrap_err();
            assert_failure(&error, Status::InvalidState, &[family, update.as_str()]);
        }
    }
}

/// `set_sql_query` stores a query without clearing the update, so the pair is refused. The
/// comment-only placeholder dbt 2.0 finalizes an option-only statement with is not a query,
/// and still runs the update.
#[test]
fn a_table_metadata_update_cannot_be_combined_with_a_sql_query() {
    let update = BqOption::UpdateDescription;
    for query_first in [true, false] {
        let mut statement = test_statement();
        if query_first {
            statement.set_sql_query("SELECT 1").unwrap();
            set_option(&mut statement, update, "a queried table");
        } else {
            set_option(&mut statement, update, "a queried table");
            statement.set_sql_query("SELECT 1").unwrap();
        }
        let error = statement.execute_update().unwrap_err();
        assert_failure(
            &error,
            Status::InvalidState,
            &["a SQL query cannot be combined with", update.as_str()],
        );
    }

    // The comment-only placeholder carries no statement, so the update runs on to require its
    // destination table, past where a query is refused.
    let mut statement = test_statement();
    set_option(&mut statement, update, "a queried table");
    statement
        .set_sql_query("-- bigquery-adbc: model.project.table\n")
        .unwrap();
    let error = statement.execute_update().unwrap_err();
    assert_failure(
        &error,
        Status::InvalidState,
        &[BqOption::DestinationTable.as_str()],
    );
}

#[test]
fn strips_trailing_statement_terminators() {
    assert_eq!(normalize_sql("SELECT 1;;;"), "SELECT 1");
    assert_eq!(normalize_sql("SELECT 1; ;  "), "SELECT 1");
    assert_eq!(normalize_sql("SELECT ';'"), "SELECT ';'");
    assert_eq!(normalize_sql("SELECT 1"), "SELECT 1");
}

/// GoogleSQL ends a line comment at a carriage return too, so a statement after one must reach
/// BigQuery rather than be refused as comment-only text.
#[test]
fn a_line_comment_ends_at_a_carriage_return_or_a_newline() {
    for query in [
        "-- c\rSELECT 1",
        "# c\rSELECT 1",
        "-- c\r\nSELECT 1",
        "# c\nSELECT 1",
    ] {
        assert_eq!(skip_comments(query), "SELECT 1", "{query:?}");
    }
    assert_eq!(skip_comments("-- only a comment\r"), "");
}
