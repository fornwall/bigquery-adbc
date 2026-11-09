//! The dbt fork's opt-in literal GetObjects filters and their direct metadata lookups.

use super::*;
use crate::BigQueryConnectionOption;
use crate::test_support::options::set_option;

const OPTION: BigQueryConnectionOption = BigQueryConnectionOption::GetObjectsDisableWildcards;

/// Changing the option on an existing connection switches between the ADBC pattern walk and
/// a single dataset lookup, including for the underscores used in dbt dataset names.
#[test]
fn disabling_wildcards_switches_schema_discovery_to_exact_lookups() {
    let (endpoint, requests) = capture_requests(vec![
        (200, json!({})),
        (
            200,
            json!({"datasets": [
                {"datasetReference": {"datasetId": "a_b"}},
                {"datasetReference": {"datasetId": "axb"}},
                {"datasetReference": {"datasetId": "A_B"}}
            ]}),
        ),
        (200, json!({})),
    ]);
    let mut connection = anonymous_connection_at(&endpoint);
    for (enabled, expected) in [
        ("true", vec!["a_b"]),
        ("false", vec!["a_b", "axb"]),
        ("true", vec!["a_b"]),
    ] {
        set_option(&mut connection, OPTION, enabled);
        let batch = objects_batch(&connection, ObjectDepth::Schemas, Some("a_b"), None, None);
        let schemas = child_structs(batch.column(1));
        assert_eq!(strings(schemas.column(0)), expected);
        assert!(list(schemas.column(1)).is_null(0));
    }
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    for index in [0, 2] {
        assert_eq!(
            requests[index].path(),
            "/bigquery/v2/projects/test-project/datasets/a_b?fields=datasetReference%28datasetId%29"
        );
    }
    assert!(requests[1].path().contains("/datasets?maxResults=1000"));
}

/// The literal mode reaches all levels and preserves depth and skip-table-metadata semantics.
/// An exact table proves its dataset exists, so neither a dataset nor a table listing is needed.
#[test]
fn literal_table_and_column_filters_preserve_depth_and_metadata_options() {
    for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
        for skip in ["true", "false"] {
            for column_filter in [Some("column_%"), Some(r"column\_%"), None] {
                let (endpoint, requests) = capture_requests(vec![(
                    200,
                    json!({
                        "type": "TABLE",
                        "schema": {"fields": [
                            {"name": "column_%", "type": "INTEGER"},
                            {"name": "column_x", "type": "INTEGER"},
                            {"name": "COLUMN_%", "type": "INTEGER"},
                            {"name": "column\\_%", "type": "INTEGER"}
                        ]},
                        "tableConstraints": {"primaryKey": {"columns": ["column_%"]}}
                    }),
                )]);
                let mut connection = anonymous_connection_at(&endpoint);
                set_option(&mut connection, OPTION, "true");
                set_option(
                    &mut connection,
                    BigQueryConnectionOption::GetObjectsSkipTableMetadata,
                    skip,
                );
                let batch = first_batch(connection.get_objects(
                    depth,
                    Some("test-project"),
                    Some("a_b"),
                    Some("events_2026"),
                    Some(vec!["TABLE"]),
                    column_filter,
                ));
                let schemas = child_structs(batch.column(1));
                assert_eq!(schemas.len(), 1);
                assert_eq!(string_at(schemas.column(0), 0), "a_b");
                let tables = child_structs(schemas.column(1));
                assert_eq!(tables.len(), 1);
                assert_eq!(string_at(tables.column(0), 0), "events_2026");
                assert_eq!(
                    list(tables.column(3)).is_null(0),
                    depth == ObjectDepth::Tables && skip == "true"
                );
                if depth == ObjectDepth::Tables {
                    assert!(list(tables.column(2)).is_null(0));
                } else {
                    let columns = child_structs(tables.column(2));
                    assert_eq!(columns.len(), if column_filter.is_some() { 1 } else { 4 });
                    if let Some(name) = column_filter {
                        assert_eq!(string_at(columns.column(0), 0), name);
                    }
                }
                let requests = requests.join().unwrap();
                assert_eq!(requests.len(), 1);
                assert!(requests[0].path().starts_with(
                    "/bigquery/v2/projects/test-project/datasets/a_b/tables/events_2026?"
                ));
            }
        }
    }
}

#[test]
fn literal_catalog_filters_do_not_expand_wildcards() {
    let (endpoint, requests) = capture_requests(vec![
        (
            400,
            json!({"error": {"errors": [{
                "reason": "invalid", "message": "Invalid project ID supplied"
            }]}}),
        );
        4
    ]);
    let mut connection = anonymous_connection_at(&endpoint);
    set_option(&mut connection, OPTION, "true");
    for (filter, rows) in [
        (None, 1),
        (Some("test-project"), 1),
        (Some("%"), 0),
        (Some("test-%"), 0),
        (Some("test_project"), 0),
        (Some("TEST-PROJECT"), 0),
        (Some(""), 0),
    ] {
        let batch = first_batch(connection.get_objects(
            ObjectDepth::Catalogs,
            filter,
            None,
            None,
            None,
            None,
        ));
        assert_eq!(batch.num_rows(), rows, "{filter:?}");
    }
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 4);
    for (request, project) in
        requests
            .iter()
            .zip(["%25", "test-%25", "test_project", "TEST-PROJECT"])
    {
        assert!(
            request
                .path()
                .starts_with(&format!("/bigquery/v2/projects/{project}/datasets?"))
        );
    }
}

#[test]
fn disabling_wildcards_keeps_unfiltered_dataset_and_table_listings() {
    for schema_filter in [None, Some("a_b")] {
        let mut responses = Vec::new();
        if schema_filter.is_none() {
            responses.push(ok(
                json!({"datasets": [{"datasetReference": {"datasetId": "a_b"}}]}),
            ));
        }
        responses.push(ok(
            json!({"tables": [listed_table("events_2026"), listed_table("other")]}),
        ));
        let (endpoint, requests) = capture_requests(responses);
        let mut connection = anonymous_connection_at(&endpoint);
        set_option(&mut connection, OPTION, "true");
        let batch = objects_batch(&connection, ObjectDepth::Tables, schema_filter, None, None);
        let schemas = child_structs(batch.column(1));
        assert_eq!(schemas.len(), 1);
        assert_eq!(string_at(schemas.column(0), 0), "a_b");
        let tables = child_structs(schemas.column(1));
        assert_eq!(tables.len(), 2);
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), if schema_filter.is_none() { 2 } else { 1 });
        if schema_filter.is_none() {
            assert!(requests[0].path().contains("/datasets?maxResults=1000"));
        }
        assert!(
            requests
                .last()
                .unwrap()
                .path()
                .contains("/datasets/a_b/tables?maxResults=1000")
        );
    }
}

/// Literal misses retain discovery's empty-result semantics, while permission failures remain
/// errors. Percent signs in direct request paths must be encoded rather than reinterpreted.
#[test]
fn literal_dataset_filters_preserve_empty_and_missing_results_and_permission_errors() {
    for (name, code) in [("", 200), ("a_b%", 404), ("a_b", 403)] {
        let responses = if name.is_empty() {
            Vec::new()
        } else {
            vec![(code, not_found_response())]
        };
        let (endpoint, requests) = capture_requests(responses);
        let mut connection = anonymous_connection_at(&endpoint);
        set_option(&mut connection, OPTION, "true");
        let result =
            connection.get_objects(ObjectDepth::Schemas, None, Some(name), None, None, None);
        if code == 403 {
            let error = result.err().unwrap();
            assert_failure(&error, Status::Unauthorized, &["test-project.a_b"]);
        } else {
            let batch = result.unwrap().next().unwrap().unwrap();
            assert!(child_structs(batch.column(1)).is_empty());
        }
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), usize::from(!name.is_empty()));
        if code == 404 {
            assert!(requests[0].path().contains("/datasets/a_b%25?"));
        }
    }
}

/// Statistics shares the catalog, dataset and table walkers, but does not opt into this option.
#[test]
fn disabling_object_wildcards_does_not_change_statistics_filters() {
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({"datasets": [{"datasetReference": {"datasetId": "axb"}}]})),
        (200, json!({"tables": [listed_table("t123")]})),
        (200, json!({"type": "TABLE", "numRows": "42"})),
    ]);
    let mut connection = anonymous_connection_at(&endpoint);
    set_option(&mut connection, OPTION, "true");
    let batch =
        first_batch(connection.get_statistics(Some("test-%"), Some("a_b"), Some("t_%"), false));
    let schemas = child_structs(batch.column(1));
    assert_eq!(schemas.len(), 1);
    assert_eq!(string_at(schemas.column(0), 0), "axb");
    let statistics = child_structs(schemas.column(1));
    assert_eq!(statistics.len(), 1);
    assert_eq!(string_at(statistics.column(0), 0), "t123");
    requests.served(3);
}
