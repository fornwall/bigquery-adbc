//! `options.rs`: the current-namespace options and their `bigquery.*` aliases, the isolation
//! level, and the scope a connection option is allowed to reach.

use super::*;
use crate::test_support::options::{
    assert_string_option, option_string, set_option, set_option_error,
};

#[test]
fn metadata_options_are_connection_only_and_reject_invalid_values_without_changing_state() {
    use crate::BigQueryConnectionOption;
    use adbc_core::options::{OptionStatement, OptionValue};

    for (key, default) in [
        (
            BigQueryConnectionOption::GetObjectsSkipTableMetadata,
            "true",
        ),
        (
            BigQueryConnectionOption::GetObjectsDisableWildcards,
            "false",
        ),
    ] {
        let mut connection = crate::test_support::driver::test_connection();
        assert_eq!(option_string(&connection, key), default);
        for value in ["false", "true", "false"] {
            set_option(&mut connection, key, value);
            assert_eq!(option_string(&connection, key), value);
        }
        for value in [
            OptionValue::from("yes"),
            "1".into(),
            "".into(),
            1_i64.into(),
            1.0_f64.into(),
            vec![1_u8].into(),
        ] {
            assert_eq!(
                set_option_error(&mut connection, key, value).status,
                Status::InvalidArguments
            );
            assert_eq!(option_string(&connection, key), "false");
        }
        for failure in [
            connection.get_option_int(key.into()).unwrap_err(),
            connection.get_option_double(key.into()).unwrap_err(),
            connection.get_option_bytes(key.into()).unwrap_err(),
        ] {
            assert_eq!(failure.status, Status::NotFound);
            assert_message(&failure, &[key.as_str(), "type string"]);
        }
        let mut statement = connection.new_statement().unwrap();
        assert_eq!(
            set_option_error(
                &mut statement,
                OptionStatement::Other(key.as_str().to_owned()),
                "true"
            )
            .status,
            Status::NotImplemented
        );
    }
}

/// A refused namespace validation keeps BigQuery's status (not found, permission denied and a
/// transport failure differ) under a message naming the object that could not be read.
#[test]
fn a_refused_current_namespace_keeps_its_status_and_names_the_object() {
    for (option, value, context) in [
        (
            OptionConnection::CurrentCatalog,
            "other-project",
            "failed to validate the current BigQuery catalog other-project",
        ),
        (
            OptionConnection::CurrentSchema,
            "absent_dataset",
            "failed to validate the current BigQuery schema test-project.absent_dataset",
        ),
    ] {
        for (code, expected) in [
            (404, Status::NotFound),
            (403, Status::Unauthorized),
            (500, Status::IO),
        ] {
            // A metadata read retries a 5xx up to the shared attempt budget.
            let attempts = if code == 500 {
                usize::try_from(crate::rest::retry::MAX_ATTEMPTS).unwrap()
            } else {
                1
            };
            let (endpoint, requests) =
                capture_requests(vec![(code, not_found_response()); attempts]);
            let mut connection = anonymous_connection_at(&endpoint);

            let error = set_option_error(&mut connection, option.clone(), value);

            assert_eq!(requests.join().unwrap().len(), attempts);
            assert_eq!(error.status, expected, "{option:?} answered HTTP {code}");
            assert!(error.message.starts_with(context), "{}", error.message);
        }
    }
}

/// Setting the current catalog or schema to its current value builds no client and sends no
/// request.
#[test]
fn setting_a_current_namespace_to_its_existing_value_issues_nothing() {
    for (option, project, dataset, value) in [
        (OptionConnection::CurrentCatalog, None, None, ""),
        (
            OptionConnection::CurrentCatalog,
            Some("test-project"),
            None,
            "test-project",
        ),
        (
            OptionConnection::CurrentSchema,
            Some("test-project"),
            None,
            "",
        ),
        (
            OptionConnection::CurrentSchema,
            Some("test-project"),
            Some("existing_dataset"),
            "existing_dataset",
        ),
    ] {
        let (endpoint, requests) = capture_requests(Vec::new());
        let mut connection = anonymous_connection_at_with_defaults(&endpoint, project, dataset);

        set_option(&mut connection, option.clone(), value);

        assert!(
            !connection.context.state.initialized_clients().metadata,
            "{option:?} = {value:?}"
        );
        assert!(requests.join().unwrap().is_empty(), "{option:?}");
    }
}

/// A changed current catalog or schema is proved against BigQuery before it is accepted, with
/// exactly one request, and reads back afterwards.
#[test]
fn a_changed_current_namespace_is_validated_once() {
    for (option, value, path) in [
        (
            OptionConnection::CurrentCatalog,
            "other-project",
            "/bigquery/v2/projects/other-project/datasets?maxResults=1&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
        ),
        (
            OptionConnection::CurrentSchema,
            "updated_dataset",
            "/bigquery/v2/projects/test-project/datasets/updated_dataset?fields=datasetReference%28datasetId%29",
        ),
    ] {
        let (endpoint, requests) = capture_requests(vec![(200, json!({}))]);
        let mut connection = anonymous_connection_at_with_defaults(
            &endpoint,
            Some("test-project"),
            Some("existing_dataset"),
        );

        set_option(&mut connection, option.clone(), value);

        assert_eq!(option_string(&connection, option), value);
        assert!(connection.context.state.initialized_clients().metadata);
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path(), path);
    }
}

/// As in the Go driver (`go/connection.go` `GetOption`), `bigquery.project_id` and
/// `bigquery.dataset_id` on a connection alias the current catalog and schema. With no project
/// or no dataset configured there is no current one, which reads as `NOT_FOUND`, as every unset
/// option does, where Go returns the empty string.
#[test]
fn the_bigquery_namespace_aliases_read_the_current_namespace() {
    for (project, dataset) in [
        (None, None),
        (Some("test-project"), None),
        (Some("test-project"), Some("existing_dataset")),
    ] {
        let (endpoint, requests) = capture_requests(Vec::new());
        let connection = anonymous_connection_at_with_defaults(&endpoint, project, dataset);

        for (standard, aliases, expected) in [
            (
                OptionConnection::CurrentCatalog,
                ["bigquery.project_id", "adbc.bigquery.sql.project_id"],
                project,
            ),
            (
                OptionConnection::CurrentSchema,
                ["bigquery.dataset_id", "adbc.bigquery.sql.dataset_id"],
                dataset,
            ),
        ] {
            assert_string_option(&connection, standard, expected);
            for alias in aliases {
                assert_string_option(
                    &connection,
                    OptionConnection::Other(alias.to_owned()),
                    expected,
                );
            }
        }
        assert!(requests.join().unwrap().is_empty());
    }
}

/// The aliases hold no state of their own: a change made through the standard key is what the
/// BigQuery spelling reports afterwards, including a change back to unset, which the standard
/// keys accept as the empty string and report as `NOT_FOUND`.
#[test]
fn the_bigquery_namespace_aliases_follow_the_standard_keys() {
    let (endpoint, requests) = capture_requests(vec![(200, json!({})), (200, json!({}))]);
    let mut connection = anonymous_connection_at_with_defaults(
        &endpoint,
        Some("test-project"),
        Some("existing_dataset"),
    );
    let alias = |name: &str| OptionConnection::Other(name.to_owned());
    let read =
        |connection: &crate::BigQueryConnection, name: &str| option_string(connection, alias(name));

    set_option(
        &mut connection,
        OptionConnection::CurrentCatalog,
        "other-project",
    );
    assert_eq!(read(&connection, "bigquery.project_id"), "other-project");

    set_option(
        &mut connection,
        OptionConnection::CurrentSchema,
        "updated_dataset",
    );
    assert_eq!(read(&connection, "bigquery.dataset_id"), "updated_dataset");

    set_option(&mut connection, OptionConnection::CurrentSchema, "");
    assert_string_option(&connection, alias("bigquery.dataset_id"), None);

    assert_eq!(requests.join().unwrap().len(), 2);
}

/// A current catalog or schema change reaches statements created before it; see
/// [`CurrentNamespace`](crate::connection::CurrentNamespace).
#[test]
fn a_namespace_change_reaches_existing_statements() {
    let dry_run = || {
        (
            200,
            json!({
                "statistics": {"query": {"statementType": "SELECT"}},
                "status": {"state": "DONE"}
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![
        dry_run(),
        (200, json!({})),
        dry_run(),
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": {"projectId":"test-project","jobId":"namespace-job"}
            }),
        ),
    ]);
    let mut connection =
        anonymous_connection_at_with_defaults(&endpoint, Some("test-project"), Some("old_dataset"));
    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT 1").unwrap();
    statement.get_parameter_schema().unwrap();
    set_option(
        &mut connection,
        OptionConnection::CurrentSchema,
        "new_dataset",
    );
    assert_eq!(
        option_string(&statement, crate::BigQueryStatementOption::DefaultDatasetId),
        "new_dataset"
    );
    // The memoized parameter schema resolved names in the old dataset, so it is derived again.
    statement.get_parameter_schema().unwrap();
    statement.execute_update().unwrap();

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 4);
    let default_dataset = |request: &CapturedRequest| {
        let query = &request.body["configuration"]["query"];
        let body = if query.is_null() {
            &request.body
        } else {
            query
        };
        body["defaultDataset"]["datasetId"].clone()
    };
    assert_eq!(default_dataset(&requests[0]), "old_dataset");
    assert_eq!(default_dataset(&requests[2]), "new_dataset");
    assert_eq!(default_dataset(&requests[3]), "new_dataset");
}

/// As in Go, setting either alias is NOT_IMPLEMENTED; the message names the standard key. The
/// int, bytes and double getters are not aliased and answer NOT_FOUND, as Go's driverbase does.
#[test]
fn the_bigquery_namespace_aliases_are_read_only() {
    for (name, standard, kept) in [
        (
            "bigquery.project_id",
            "adbc.connection.catalog",
            "test-project",
        ),
        (
            "bigquery.dataset_id",
            "adbc.connection.db_schema",
            "existing_dataset",
        ),
    ] {
        let (endpoint, requests) = capture_requests(Vec::new());
        let mut connection = anonymous_connection_at_with_defaults(
            &endpoint,
            Some("test-project"),
            Some("existing_dataset"),
        );
        let key = OptionConnection::Other(name.to_owned());

        let error = set_option_error(&mut connection, key.clone(), "changed");
        assert_eq!(error.status, Status::NotImplemented, "{name}");
        assert_message(&error, &[name, standard]);
        assert_eq!(option_string(&connection, key.clone()), kept);

        // NOT_FOUND as in Go, with this driver's usual wrong-type message.
        for (failure, requested) in [
            (
                connection.get_option_int(key.clone()).unwrap_err(),
                "an integer",
            ),
            (
                connection.get_option_bytes(key.clone()).unwrap_err(),
                "bytes",
            ),
            (
                connection.get_option_double(key.clone()).unwrap_err(),
                "a double",
            ),
        ] {
            assert_eq!(failure.status, Status::NotFound, "{name}");
            assert_eq!(
                failure.message,
                format!("BigQuery option {name} has type string, but was requested as {requested}"),
                "{name}"
            );
        }
        assert!(requests.join().unwrap().is_empty(), "{name}");
    }
}

/// A dataset without a project is INVALID_STATE (the configuration is missing, not the ID
/// malformed), with one message naming every option that could supply the project. The
/// statement counterpart is
/// `statement::tests::options::naming_a_default_dataset_without_a_project_reports_the_options_that_supply_one`.
#[test]
fn naming_a_schema_without_a_project_reports_the_options_that_supply_one() {
    let (endpoint, requests) = capture_requests(Vec::new());
    let mut connection = anonymous_connection_at_with_defaults(&endpoint, None, None);
    let error = set_option_error(
        &mut connection,
        OptionConnection::CurrentSchema,
        "some_dataset",
    );
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(
        error.message,
        "a BigQuery project must be available before setting the connection option \
         adbc.connection.db_schema; set the connection option adbc.connection.catalog, or the \
         database option bigquery.project_id"
    );
    assert!(requests.join().unwrap().is_empty());
}

/// The current schema is validated in the current catalog, so a catalog and schema supplied
/// together at creation apply catalog first whatever their order: the C driver manager replays
/// pre-Init connection options in hash-map order.
#[test]
fn a_catalog_supplied_with_a_schema_at_creation_qualifies_it_whatever_the_order() {
    let (endpoint, requests) = capture_requests(vec![(200, json!({})), (200, json!({}))]);
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (
                BigQueryDatabaseOption::AuthenticationType.into(),
                BigQueryAuthenticationType::Anonymous.into(),
            ),
            (BigQueryDatabaseOption::Endpoint.into(), endpoint.into()),
        ])
        .unwrap();

    let connection = database
        .new_connection_with_opts([
            (OptionConnection::CurrentSchema, "other_dataset".into()),
            (OptionConnection::CurrentCatalog, "other-project".into()),
        ])
        .unwrap();

    assert_eq!(
        option_string(&connection, OptionConnection::CurrentCatalog),
        "other-project"
    );
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentSchema),
        "other_dataset"
    );
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::path)
            .collect::<Vec<_>>(),
        [
            "/bigquery/v2/projects/other-project/datasets?maxResults=1&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
            "/bigquery/v2/projects/other-project/datasets/other_dataset?fields=datasetReference%28datasetId%29",
        ]
    );
}

/// BigQuery transactions are snapshot-isolated, so the snapshot level is accepted. A rejection
/// names the key and both accepted values.
#[test]
fn isolation_level_accepts_the_default_and_snapshot_spellings() {
    let mut connection = crate::test_support::driver::test_connection();
    for level in [
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
    ] {
        set_option(&mut connection, OptionConnection::IsolationLevel, level);
    }

    let error = set_option_error(
        &mut connection,
        OptionConnection::IsolationLevel,
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
    );
    assert_eq!(error.status, Status::NotImplemented);
    for expected in [
        OptionConnection::IsolationLevel.as_ref(),
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
    ] {
        assert_message(&error, &[expected]);
    }

    // The getter reports the level BigQuery actually gives, and it round-trips.
    let level = option_string(&connection, OptionConnection::IsolationLevel);
    assert_eq!(level, ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT);
    set_option(&mut connection, OptionConnection::IsolationLevel, level);
}

/// A statement-scoped `bigquery.query.use_storage_api_disabled_client` stays inside the
/// statement. The recorded per-project refusal is shared by every connection of the database, so
/// only the connection setter clears it.
#[test]
fn a_statement_scoped_storage_read_option_does_not_reach_a_sibling_connection() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (
                BigQueryDatabaseOption::ProjectId.into(),
                "fixture-project".into(),
            ),
            (
                BigQueryDatabaseOption::AuthenticationType.into(),
                BigQueryAuthenticationType::Anonymous.into(),
            ),
            (
                BigQueryDatabaseOption::Endpoint.into(),
                crate::test_support::UNREACHABLE_ENDPOINT.into(),
            ),
        ])
        .unwrap();
    let mut connection = database.new_connection().unwrap();
    let sibling = database.new_connection().unwrap();
    let key = crate::BigQueryConnectionOption::UseStorageApiDisabledClient;
    let statement_key = crate::BigQueryStatementOption::UseStorageApiDisabledClient;

    connection
        .context
        .state
        .disable_storage_read("fixture-project");
    assert_eq!(option_string(&sibling, key), ADBC_OPTION_VALUE_DISABLED);

    let mut statement = connection.new_statement().unwrap();
    for value in ["true", "false"] {
        set_option(&mut statement, statement_key, value.to_owned());
        assert_eq!(option_string(&sibling, key), ADBC_OPTION_VALUE_DISABLED);
        assert!(
            sibling
                .context
                .state
                .storage_read_disabled("fixture-project")
        );
    }

    set_option(&mut connection, key, ADBC_OPTION_VALUE_ENABLED.to_owned());
    let inherited = connection.new_statement().unwrap();
    assert_eq!(
        option_string(&inherited, statement_key),
        ADBC_OPTION_VALUE_ENABLED
    );
    assert_eq!(
        option_string(&statement, statement_key),
        ADBC_OPTION_VALUE_DISABLED
    );
    assert!(
        sibling
            .context
            .state
            .storage_read_disabled("fixture-project")
    );

    set_option(&mut connection, key, ADBC_OPTION_VALUE_DISABLED.to_owned());
    assert!(
        !sibling
            .context
            .state
            .storage_read_disabled("fixture-project")
    );
    assert_eq!(option_string(&sibling, key), ADBC_OPTION_VALUE_DISABLED);
    assert_eq!(
        option_string(&inherited, statement_key),
        ADBC_OPTION_VALUE_ENABLED
    );
}

/// The "no current catalog" refusal names both options that would supply one, before any
/// request goes out.
#[test]
fn missing_current_catalog_names_the_options_that_would_set_it() {
    let (endpoint, requests) = capture_requests(Vec::new());
    let mut connection = anonymous_connection_at_with_defaults(&endpoint, None, None);
    let error = set_option_error(&mut connection, OptionConnection::CurrentSchema, "dataset");
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["adbc.connection.catalog", "bigquery.project_id"]);
    assert!(requests.join().unwrap().is_empty());
}

/// With the current catalog cleared, an unqualified name resolves in `bigquery.project_id`, as a
/// statement's query defaults and an ingest target do, rather than being refused with a message
/// that asks for the option already set.
#[test]
fn a_cleared_current_catalog_resolves_names_in_the_billing_project() {
    let (endpoint, requests) = capture_requests(vec![
        (200, json!({"type": "TABLE", "schema": {"fields": []}})),
        (200, json!({})),
    ]);
    let mut connection = anonymous_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::CurrentCatalog, "");

    connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap();
    set_option(&mut connection, OptionConnection::CurrentSchema, "dataset");
    // The current catalog reads back as the project names now resolve in.
    assert_string_option(
        &connection,
        OptionConnection::CurrentCatalog,
        Some("test-project"),
    );

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .path()
            .starts_with("/bigquery/v2/projects/test-project/datasets/dataset/tables/events?"),
        "{}",
        requests[0].path()
    );
    assert_eq!(
        requests[1].path(),
        "/bigquery/v2/projects/test-project/datasets/dataset?fields=datasetReference%28datasetId%29"
    );
}

/// The metadata listings consider the current project the catalog option reads back, so with the
/// current catalog cleared they list the billing project, by any filter that matches it, rather
/// than nothing.
#[test]
fn metadata_listings_follow_a_cleared_catalog_to_the_billing_project() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({"datasets": [{"datasetReference": {"datasetId": "dataset"}}]}),
    )]);
    let mut connection = anonymous_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::CurrentCatalog, "");

    for catalog in [None, Some("test-%"), Some("test-project")] {
        let batch = connection
            .get_objects(ObjectDepth::Catalogs, catalog, None, None, None, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1, "{catalog:?}");
        assert_eq!(string_at(batch.column(0), 0), "test-project", "{catalog:?}");
    }
    let batch = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        string_at(child_structs(batch.column(1)).column(0), 0),
        "dataset"
    );

    let requests = requests.join().unwrap();
    assert!(
        requests[0]
            .path()
            .starts_with("/bigquery/v2/projects/test-project/datasets?"),
        "{}",
        requests[0].path()
    );
}

/// A schema is validated in the catalog it is set under, so naming the current schema again after
/// a catalog change proves it exists in the new catalog rather than being skipped as unchanged.
#[test]
fn an_unchanged_schema_set_after_a_catalog_change_is_validated_in_the_new_catalog() {
    let (endpoint, requests) = capture_requests(vec![
        (200, json!({})),
        (404, not_found_response()),
        (200, json!({})),
    ]);
    let mut connection = anonymous_connection_at_with_defaults(
        &endpoint,
        Some("test-project"),
        Some("existing_dataset"),
    );
    set_option(
        &mut connection,
        OptionConnection::CurrentCatalog,
        "other-project",
    );

    let error = set_option_error(
        &mut connection,
        OptionConnection::CurrentSchema,
        "existing_dataset",
    );
    assert_eq!(error.status, Status::NotFound, "{}", error.message);
    // Once validated in the current catalog, setting it again is a no-op.
    set_option(
        &mut connection,
        OptionConnection::CurrentSchema,
        "existing_dataset",
    );
    set_option(
        &mut connection,
        OptionConnection::CurrentSchema,
        "existing_dataset",
    );

    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::path)
            .collect::<Vec<_>>(),
        [
            "/bigquery/v2/projects/other-project/datasets?maxResults=1&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
            "/bigquery/v2/projects/other-project/datasets/existing_dataset?fields=datasetReference%28datasetId%29",
            "/bigquery/v2/projects/other-project/datasets/existing_dataset?fields=datasetReference%28datasetId%29",
        ]
    );
}
