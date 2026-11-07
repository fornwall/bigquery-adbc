//! Bulk ingest and the dbt CSV seed load: what each execute entry point does with bound data,
//! what a dry run inspects, and which requests reach BigQuery before a table is written.

use super::*;

/// A bulk-ingest statement with `bigquery.query.dry_run` set, targeting the test
/// connection whose endpoint is unroutable: every local check runs, and the first request
/// fails.
fn dry_run_ingest_statement() -> BigQueryStatement {
    let mut statement = test_statement();
    set_option(&mut statement, OptionStatement::TargetDbSchema, "scratch");
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    set_option(&mut statement, BqOption::DryRun, "true");
    statement
}

fn empty_ingest_statement_at(endpoint: &str) -> BigQueryStatement {
    let mut statement = test_statement_at(endpoint);
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    set_option(&mut statement, OptionStatement::TargetDbSchema, "dataset");
    bind_no_rows(&mut statement);
    statement
}

fn table_not_found() -> (u16, serde_json::Value) {
    (
        404,
        json!({"error": {"code": 404, "message": "table not found", "status": "NOT_FOUND"}}),
    )
}

fn table_created() -> (u16, serde_json::Value) {
    ok(json!({
        "tableReference": {
            "projectId": "fixture-project",
            "datasetId": "dataset",
            "tableId": "events"
        }
    }))
}

/// A dry run reads only the bound schema, so every entry point reports its errors (here a column
/// name BigQuery cannot accept) without contacting BigQuery, and leaves the statement bound for
/// the real ingest.
#[test]
fn dry_run_ingest_reports_schema_errors_locally_and_keeps_the_bound_data() {
    for execute in EXECUTE_ENTRIES {
        let mut statement = dry_run_ingest_statement();
        statement.bind(int64_column("1invalid", vec![1])).unwrap();

        let error = execute(&mut statement).unwrap_err();
        assert_failure(&error, Status::InvalidArguments, &["1invalid"]);
        assert!(statement.parameters.schema().is_some());
    }
}

#[test]
fn dry_run_ingest_requires_bound_data() {
    // There is no schema to validate until something is bound, so the dry run reports the
    // same missing-data error a real ingest would.
    let error = dry_run_ingest_statement().execute_update().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["bind"]);
}

#[test]
fn dry_run_ingest_is_refused_in_a_manual_transaction() {
    // What a dry run would faithfully report here is the rejection itself, so the manual
    // transaction check runs ahead of it.
    let mut statement = dry_run_ingest_statement();
    statement.bind(int64_column("value", vec![1])).unwrap();
    statement.transaction.disable_autocommit().unwrap();

    let error = statement.execute_update().unwrap_err();
    assert_failure(
        &error,
        Status::InvalidState,
        &["adbc.connection.autocommit"],
    );
}

#[test]
fn clearing_the_ingest_target_returns_the_statement_to_the_query_dry_run_path() {
    let mut statement = dry_run_ingest_statement();
    statement.set_sql_query("SELECT 1").unwrap();

    // set_sql_query clears the ingest target, so the query path's dry-run rule applies again:
    // a dry run creates no job, so there is nothing to partition. An ingest dry run, by
    // contrast, reports zero partitions rather than refusing.
    let error = statement.execute_partitions().unwrap_err();
    assert_failure(&error, Status::InvalidState, &["bigquery.query.dry_run"]);
}

/// Create is one atomic table insertion: it does not first read the target, and even an empty
/// ingest reports 0 appended rows rather than the -1 that means "unknown".
#[test]
fn execute_reports_the_ingest_row_count_alongside_the_stream() {
    let (endpoint, requests) = capture_requests(vec![table_created()]);
    let mut statement = empty_ingest_statement_at(&endpoint);

    let (mut reader, affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(affected, 0, "an empty ingest appended exactly zero rows");
    // adbc.h tells an ingesting caller to pass NULL for `out`, but the Rust
    // `ManagedStatement::execute` never does, so it gets an empty stream rather than a refusal.
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none(), "ingest returns an empty stream");
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].line.starts_with("POST ")
            && requests[0]
                .path()
                .contains("/projects/fixture-project/datasets/dataset/tables")
    );
    assert_eq!(requests[0].body["tableReference"]["tableId"], "events");
}

#[test]
fn create_ingest_maps_insert_conflict_to_already_exists() {
    let (endpoint, requests) = capture_requests(vec![(
        409,
        json!({
            "error": {"code": 409, "message": "table already exists", "status": "ALREADY_EXISTS"}
        }),
    )]);
    let mut statement = empty_ingest_statement_at(&endpoint);

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists);
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].line.starts_with("POST "));
}

#[test]
fn create_ingest_dry_run_still_inspects_the_target_without_inserting() {
    let (endpoint, requests) = capture_requests(vec![table_not_found()]);
    let mut statement = empty_ingest_statement_at(&endpoint);
    set_option(&mut statement, BqOption::DryRun, "true");

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    assert!(statement.parameters.schema().is_some());
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].line.starts_with("GET "));
    assert!(requests[0].path().contains("/tables/events"));
}

/// An unqualified ingest target lands where an unqualified name in the statement's SQL
/// resolves: the statement's default project and dataset shadow the connection's, as in Go.
#[test]
fn the_ingest_target_falls_back_to_the_statement_defaults() {
    let (endpoint, requests) = capture_requests(vec![table_not_found()]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BqOption::DefaultProjectId,
        "statement-project",
    );
    set_option(
        &mut statement,
        BqOption::DefaultDatasetId,
        "statement_dataset",
    );
    set_option(&mut statement, OptionStatement::TargetTable, "events");
    set_option(&mut statement, BqOption::DryRun, "true");
    statement.bind(int64_column("value", vec![1])).unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .path()
            .contains("/projects/statement-project/datasets/statement_dataset/tables/events"),
        "{}",
        requests[0].path()
    );
}

#[test]
fn append_ingest_still_inspects_and_validates_the_existing_target() {
    let (endpoint, requests) = capture_requests(vec![ok(json!({
        "tableReference": {
            "projectId": "fixture-project",
            "datasetId": "dataset",
            "tableId": "events"
        },
        "schema": {"fields": [{"name": "value", "type": "INTEGER", "mode": "REQUIRED"}]}
    }))]);
    let mut statement = empty_ingest_statement_at(&endpoint);
    set_option(
        &mut statement,
        OptionStatement::IngestMode,
        "adbc.ingest.mode.append",
    );

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].line.starts_with("GET "));
    assert!(requests[0].path().contains("/tables/events"));
}

#[test]
fn create_append_and_replace_still_probe_before_creating_a_missing_target() {
    for mode in ["adbc.ingest.mode.create_append", "adbc.ingest.mode.replace"] {
        let (endpoint, requests) = capture_requests(vec![table_not_found(), table_created()]);
        let mut statement = empty_ingest_statement_at(&endpoint);
        set_option(&mut statement, OptionStatement::IngestMode, mode);

        assert_eq!(statement.execute_update().unwrap(), Some(0), "mode {mode}");
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 2, "mode {mode}");
        assert!(requests[0].line.starts_with("GET "), "mode {mode}");
        assert!(requests[1].line.starts_with("POST "), "mode {mode}");
    }
}

/// A dry run of a CSV seed validates the seed's declared schema and nothing else, so a seed that
/// can be gigabytes is opened but never read into memory.
#[test]
fn a_dry_run_csv_seed_validates_the_schema_without_reading_the_seed() {
    let schema = Schema::new(vec![bigquery_typed(
        Field::new("id", DataType::Utf8, true),
        "int64",
    )]);
    let encoded_schema = encode_schema_fixture(&schema);

    let directory = std::env::temp_dir().join(format!("bigquery-adbc-seed-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let filepath = directory.join("seed.csv");
    std::fs::write(&filepath, b"id\n1\n").unwrap();

    // The endpoint is unroutable, so a dry run that sent anything would fail rather than report 0.
    let mut statement = test_statement();
    for (option, value) in [
        (
            BqOption::CsvFilepath,
            OptionValue::String(filepath.to_str().unwrap().to_owned()),
        ),
        (BqOption::CsvSchema, OptionValue::Bytes(encoded_schema)),
        (BqOption::DestinationTable, "data_set.seed".into()),
        (BqOption::DryRun, "true".into()),
    ] {
        set_option(&mut statement, option, value);
    }
    // Nothing was loaded, so the row count is the unknown one rather than a claimed zero.
    assert_eq!(statement.execute_update().unwrap(), None);

    // A missing seed still fails the dry run: opening it is what a dry run is run to check.
    std::fs::remove_file(&filepath).unwrap();
    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::InvalidArguments
    );
    std::fs::remove_dir_all(&directory).unwrap();
}
