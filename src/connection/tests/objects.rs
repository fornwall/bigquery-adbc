//! `objects.rs`: `get_objects` depth truncation, the exact-lookup fast paths, the column filter,
//! and what an unmatched catalog answers.

use super::*;

/// The option changes constraint discovery only at table depth, including exact lookups.
#[test]
fn table_metadata_option_controls_describes_and_constraints_without_exposing_columns() {
    use crate::BigQueryConnectionOption;
    use crate::test_support::options::set_option;

    for skip in [true, false] {
        for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
            for exact in [true, false] {
                let describe = depth != ObjectDepth::Tables || !skip;
                let mut responses = Vec::new();
                if !exact {
                    responses.push((
                        200,
                        json!({"tables": [listed_table("events"), listed_table("excluded")]}),
                    ));
                }
                if exact || describe {
                    responses.push((
                        200,
                        json!({
                            "type": "TABLE",
                            "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
                            "tableConstraints": {"primaryKey": {"columns": ["id"]}}
                        }),
                    ));
                }
                let count = responses.len();
                let (endpoint, requests) = capture_requests(responses);
                let mut connection = anonymous_connection_at(&endpoint);
                set_option(
                    &mut connection,
                    BigQueryConnectionOption::GetObjectsSkipTableMetadata,
                    if skip { "true" } else { "false" },
                );
                let batch = objects_batch(
                    &connection,
                    depth,
                    Some("dataset"),
                    Some(if exact { "events" } else { "events%" }),
                    Some(vec!["TABLE"]),
                );
                let tables = child_structs(child_structs(batch.column(1)).column(1));
                assert_eq!(tables.len(), 1);
                assert_eq!(string_at(tables.column(0), 0), "events");
                assert_eq!(
                    list(tables.column(2)).is_null(0),
                    depth == ObjectDepth::Tables
                );
                assert_eq!(list(tables.column(3)).is_null(0), !describe);
                if describe {
                    let constraints = child_structs(tables.column(3));
                    assert_eq!(constraints.len(), 1);
                    assert_eq!(string_at(constraints.column(1), 0), "PRIMARY KEY");
                }
                let requests = requests.join().unwrap();
                assert_eq!(requests.len(), count);
                if exact || describe {
                    let fields = if depth != ObjectDepth::Tables {
                        "schema%2CtableConstraints%2CtableReference%28datasetId%2CtableId%29%2Ctype"
                    } else if skip {
                        "tableReference%28datasetId%2CtableId%29%2Ctype"
                    } else {
                        "tableConstraints%2CtableReference%28datasetId%2CtableId%29%2Ctype"
                    };
                    assert!(
                        requests
                            .last()
                            .unwrap()
                            .path()
                            .ends_with(&format!("view=BASIC&fields={fields}"))
                    );
                }
            }
        }
    }
}

#[test]
fn exact_get_objects_skips_the_dataset_request_at_table_depths() {
    for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
        let (endpoint, requests) = capture_requests(vec![(
            200,
            json!({
                "type": "TABLE",
                "schema": {"fields": [{"name": "id", "type": "INTEGER"}]}
            }),
        )]);
        let connection = anonymous_connection_at(&endpoint);

        let batch = objects_batch(
            &connection,
            depth,
            Some("dataset"),
            Some("events"),
            Some(vec!["TABLE"]),
        );
        let schemas = child_structs(batch.column(1));
        assert_eq!(schemas.len(), 1);
        assert_eq!(string_at(schemas.column(0), 0), "dataset");
        let tables = child_structs(schemas.column(1));
        assert_eq!(tables.len(), 1);
        assert_eq!(string_at(tables.column(0), 0), "events");
        let columns = list(tables.column(2));
        if depth == ObjectDepth::Tables {
            assert!(columns.is_null(0));
        } else {
            assert!(!columns.is_null(0));
            assert_eq!(child_structs(tables.column(2)).len(), 1);
        }

        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1, "{depth:?}");
        assert!(
            requests[0]
                .path()
                .contains("/datasets/dataset/tables/events"),
            "{}",
            requests[0].line
        );
        assert!(requests[0].path().contains("view=BASIC"));
    }
}

#[test]
fn exact_get_objects_type_mismatch_keeps_the_schema_without_a_fallback() {
    let (endpoint, requests) = capture_requests(vec![(200, json!({"type": "TABLE"}))]);
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(
        &connection,
        ObjectDepth::Tables,
        Some("dataset"),
        Some("events"),
        Some(vec!["VIEW"]),
    );
    let schemas = child_structs(batch.column(1));
    assert_eq!(schemas.len(), 1);
    assert_eq!(child_structs(schemas.column(1)).len(), 0);
    assert_eq!(requests.join().unwrap().len(), 1);
}

#[test]
fn a_described_table_type_supersedes_its_listing_type() {
    for (depth, table_types) in [
        (Some(ObjectDepth::Columns), None),
        (Some(ObjectDepth::Columns), Some(vec!["TABLE"])),
        (None, None),
    ] {
        let (endpoint, requests) = capture_requests(vec![
            (200, json!({"tables": [listed_table("events")]})),
            (
                200,
                json!({
                    "type": "VIEW",
                    "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
                    "numRows": "0"
                }),
            ),
        ]);
        let connection = anonymous_connection_at(&endpoint);
        let batch = match depth {
            Some(depth) => objects_batch(
                &connection,
                depth,
                Some("dataset"),
                None,
                table_types.clone(),
            ),
            None => connection
                .get_statistics(None, Some("dataset"), None, false)
                .unwrap()
                .next()
                .unwrap()
                .unwrap(),
        };
        let tables = child_structs(child_structs(batch.column(1)).column(1));
        if depth.is_some() && table_types.is_none() {
            assert_eq!(tables.len(), 1);
            assert_eq!(string_at(tables.column(1), 0), "VIEW");
        } else {
            assert!(tables.is_empty());
        }
        assert_eq!(requests.join().unwrap().len(), 2);
    }
}

/// The partial-response selector that asks `tables.get` and `tables.list` for the names BigQuery
/// resolved, percent-encoded as in a request path.
const TABLE_REFERENCE_FIELDS: &str = "tableReference%28datasetId%2CtableId%29";

/// How a dataset created with `isCaseInsensitive` names `DATASET.EVENTS`: as `dataset.events`.
fn resolved_table_reference() -> serde_json::Value {
    json!({"projectId": "test-project", "datasetId": "dataset", "tableId": "events"})
}

fn resolved_dataset() -> serde_json::Value {
    json!({"datasetReference": {"projectId": "test-project", "datasetId": "dataset"}})
}

/// A `tables.get` answer for any view, naming the table BigQuery resolved.
fn resolved_table() -> serde_json::Value {
    json!({
        "tableReference": resolved_table_reference(),
        "type": "TABLE",
        "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
        "numRows": "42"
    })
}

fn resolved_listing() -> serde_json::Value {
    json!({"tables": [{"tableReference": resolved_table_reference(), "type": "TABLE"}]})
}

/// Serves `count` metadata requests as a case-insensitive dataset answers them.
fn case_insensitive_dataset(count: usize) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    crate::test_support::http::capture_requests_responding(count, |request| {
        let path = request.path();
        let answer = if path.contains("/tables/") {
            resolved_table()
        } else if path.contains("/tables?") {
            resolved_listing()
        } else if path.contains("/datasets?") {
            json!({"datasets": [resolved_dataset()]})
        } else {
            resolved_dataset()
        };
        Some((200, answer))
    })
}

/// The dataset names a metadata batch reports, and for each the names leading its rows: table
/// names in `get_objects`, and each statistic's table name in `get_statistics`.
fn reported_names(batch: &arrow_array::RecordBatch) -> Vec<(String, Vec<String>)> {
    let schemas = child_structs(batch.column(1));
    let rows = list(schemas.column(1));
    (0..schemas.len())
        .map(|index| {
            let names = if rows.is_null(index) {
                Vec::new()
            } else {
                let rows = rows.value(index);
                let rows = typed::<arrow_array::StructArray>(rows.as_ref());
                strings(rows.column(0))
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            };
            (string_at(schemas.column(0), index).to_owned(), names)
        })
        .collect()
}

fn names(dataset: &str, tables: &[&str]) -> Vec<(String, Vec<String>)> {
    vec![(
        dataset.to_owned(),
        tables.iter().map(|table| (*table).to_owned()).collect(),
    )]
}

/// In a case-insensitive dataset BigQuery resolves `DATASET.EVENTS` to `dataset.events`. Both
/// listings report the resolved names, not the filters' spelling, at every depth that reports
/// names, and each lookup asks for the reference that carries them. A response without one leaves
/// the filter's spelling.
#[test]
fn an_exact_lookup_reports_the_names_bigquery_resolved_it_to() {
    use crate::BigQueryConnectionOption;
    use crate::test_support::options::set_option;

    let (endpoint, requests) = case_insensitive_dataset(1);
    let connection = anonymous_connection_at(&endpoint);
    let batch = objects_batch(
        &connection,
        ObjectDepth::Schemas,
        Some("DATASET"),
        None,
        None,
    );
    assert_eq!(reported_names(&batch), names("dataset", &[]));
    assert_eq!(
        requests.join().unwrap()[0].path(),
        "/bigquery/v2/projects/test-project/datasets/DATASET?fields=datasetReference%28datasetId%29"
    );

    // Each view a table is described with, two of them at table depth.
    for (depth, skip_table_metadata) in [
        (Some(ObjectDepth::Tables), "true"),
        (Some(ObjectDepth::Tables), "false"),
        (Some(ObjectDepth::Columns), "true"),
        (Some(ObjectDepth::All), "true"),
        (None, "true"),
    ] {
        let case = format!("{depth:?} skip={skip_table_metadata}");
        let (endpoint, requests) = case_insensitive_dataset(1);
        let mut connection = anonymous_connection_at(&endpoint);
        set_option(
            &mut connection,
            BigQueryConnectionOption::GetObjectsSkipTableMetadata,
            skip_table_metadata,
        );
        let batch = discover_metadata(&connection, depth, None, Some("DATASET"), Some("EVENTS"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            reported_names(&batch),
            names("dataset", &["events"]),
            "{case}"
        );
        if matches!(depth, Some(ObjectDepth::Columns | ObjectDepth::All)) {
            let tables = child_structs(child_structs(batch.column(1)).column(1));
            assert_eq!(strings(child_structs(tables.column(2)).column(0)), ["id"]);
        }
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1, "{case}");
        let path = requests[0].path();
        assert!(path.contains("/datasets/DATASET/tables/EVENTS?"), "{path}");
        assert!(path.contains(TABLE_REFERENCE_FIELDS), "{path}");
    }

    // A table the type filter excludes still names its dataset.
    let (endpoint, requests) = case_insensitive_dataset(1);
    let connection = anonymous_connection_at(&endpoint);
    let batch = objects_batch(
        &connection,
        ObjectDepth::Tables,
        Some("DATASET"),
        Some("EVENTS"),
        Some(vec!["VIEW"]),
    );
    assert_eq!(reported_names(&batch), names("dataset", &[]));
    assert_eq!(requests.join().unwrap().len(), 1);

    // Answers that name nothing.
    let (endpoint, requests) = capture_requests(vec![(200, json!({})), (200, described_table())]);
    let connection = anonymous_connection_at(&endpoint);
    let batch = objects_batch(
        &connection,
        ObjectDepth::Schemas,
        Some("DATASET"),
        None,
        None,
    );
    assert_eq!(reported_names(&batch), names("DATASET", &[]));
    let batch = objects_batch(
        &connection,
        ObjectDepth::Columns,
        Some("DATASET"),
        Some("EVENTS"),
        None,
    );
    assert_eq!(reported_names(&batch), names("DATASET", &["EVENTS"]));
    assert_eq!(requests.join().unwrap().len(), 2);
}

/// An exact table filter is looked up in each listed dataset, and reports the name BigQuery
/// resolved it to there.
#[test]
fn an_exact_table_in_a_listed_dataset_reports_the_name_bigquery_resolved_it_to() {
    for depth in [Some(ObjectDepth::Tables), Some(ObjectDepth::Columns), None] {
        let (endpoint, requests) = case_insensitive_dataset(2);
        let connection = anonymous_connection_at(&endpoint);
        let batch = discover_metadata(&connection, depth, None, Some("data%"), Some("EVENTS"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            reported_names(&batch),
            names("dataset", &["events"]),
            "{depth:?}"
        );
        let requests = requests.join().unwrap();
        let path = requests[1].path();
        assert!(path.contains("/datasets/dataset/tables/EVENTS?"), "{path}");
        assert!(path.contains(TABLE_REFERENCE_FIELDS), "{path}");
    }
}

/// An exact dataset filter at table depth is answered by the dataset's tables: their references
/// name the dataset as BigQuery resolved it. When no table answers, because the dataset is empty
/// or the exact table is missing, `datasets.get` names it instead.
#[test]
fn an_exact_dataset_reports_the_name_its_tables_or_its_lookup_give() {
    for depth in [Some(ObjectDepth::Tables), None] {
        for (case, (table_name, mut responses, tables)) in [
            ("ev%", vec![(200, resolved_listing())], &["events"][..]),
            (
                "ev%",
                vec![(200, json!({})), (200, resolved_dataset())],
                &[],
            ),
            (
                "EVENTS",
                vec![(404, not_found_response()), (200, resolved_dataset())],
                &[],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let case = format!("{depth:?} case {case}");
            // Statistics describe each listed table.
            if depth.is_none() && responses.len() == 1 {
                responses.push((200, resolved_table()));
            }
            let count = responses.len();
            let (endpoint, requests) = capture_requests(responses);
            let connection = anonymous_connection_at(&endpoint);
            let batch =
                discover_metadata(&connection, depth, None, Some("DATASET"), Some(table_name))
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
            assert_eq!(reported_names(&batch), names("dataset", tables), "{case}");
            let requests = requests.join().unwrap();
            assert_eq!(requests.len(), count, "{case}");
            if table_name == "ev%" {
                let path = requests[0].path();
                assert!(path.contains("/datasets/DATASET/tables?"), "{path}");
                assert!(path.contains(TABLE_REFERENCE_FIELDS), "{path}");
            }
            if count == 2 && tables.is_empty() {
                assert!(
                    requests[1]
                        .path()
                        .ends_with("/datasets/DATASET?fields=datasetReference%28datasetId%29"),
                    "{case}"
                );
            }
        }
    }
}

/// A caller-supplied name that spells a URL dot segment is never asked for directly.
///
/// URL parsing resolves `.` and `..` segments away, so `tables.get` for `..` would describe the
/// dataset as a table. Such a name takes the listing path, where it matches nothing.
#[test]
fn a_dot_segment_name_is_answered_by_a_listing_rather_than_a_direct_lookup() {
    // A dataset named outright is listed for tables directly, and the dot-segment table filter
    // matches nothing listed; a dot-segment dataset falls back to the dataset listing, which
    // cannot report it, and the walk ends there.
    for (db_schema, table_name) in [("dataset", ".."), ("dataset", "."), ("..", "events")] {
        let responses = if db_schema == "dataset" {
            vec![(200, json!({"tables": [listed_table("events")]}))]
        } else {
            vec![(
                200,
                json!({"datasets": [{"datasetReference": {"datasetId": "dataset"}}]}),
            )]
        };
        let (endpoint, requests) = capture_requests(responses);
        let connection = anonymous_connection_at(&endpoint);

        let batch = objects_batch(
            &connection,
            ObjectDepth::Tables,
            Some(db_schema),
            Some(table_name),
            None,
        );
        let schemas = child_structs(batch.column(1));
        assert_eq!(schemas.len(), usize::from(db_schema == "dataset"));
        if !schemas.is_empty() {
            assert_eq!(child_structs(schemas.column(1)).len(), 0);
        }

        for request in requests.join().unwrap() {
            assert!(!request.path().contains("/tables/"), "{}", request.line);
            assert!(!request.path().contains(".."), "{}", request.line);
        }
    }
}

/// `get_table_schema` names one table outright, so a dot segment cannot fall back to a listing
/// the way `get_objects` does; it is rejected before any request goes out.
#[test]
fn get_table_schema_rejects_a_dot_segment_name() {
    for (db_schema, table_name) in [("dataset", ".."), ("dataset", "."), ("..", "events")] {
        let (endpoint, requests) = capture_requests(Vec::new());
        let connection = anonymous_connection_at(&endpoint);

        let error = connection
            .get_table_schema(None, Some(db_schema), table_name)
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{error}");
        assert_message(&error, &["\"..\""]);
        assert!(requests.join().unwrap().is_empty());
    }
}

/// A 404 from `tables.get` does not say whether the table or the dataset is missing, so one
/// `datasets.get` settles it and nothing more goes out. Both metadata listings resolve an exact
/// `(dataset, table)` pair this way.
#[test]
fn an_exact_lookup_falls_back_only_to_distinguish_which_object_is_missing() {
    for listing in [objects_listing as ExactListing, statistics_listing] {
        for (dataset_response, expected_schemas) in [
            ((200, json!({})), 1_usize),
            ((404, not_found_response()), 0_usize),
        ] {
            let (endpoint, requests) =
                capture_requests(vec![(404, not_found_response()), dataset_response]);
            let connection = anonymous_connection_at(&endpoint);

            let batch = listing(&connection, "missing").next().unwrap().unwrap();
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), expected_schemas);
            if expected_schemas == 1 {
                assert_eq!(child_structs(schemas.column(1)).len(), 0);
            }

            let requests = requests.join().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[0].path().contains("/tables/missing"));
            assert!(requests[1].path().contains("/datasets/dataset"));
            assert!(!requests[1].path().contains("/tables/"));
        }
    }
}

/// `tables.get` answers a name that cannot be a table ID with 400 `invalid` rather than 404
/// (verified live: `a*b` and `a ?b#`). An exact filter naming one matches no table, as the same
/// name with a wildcard appended does, so both listings report it like a missing table.
#[test]
fn an_exact_filter_that_is_not_a_table_id_matches_no_table() {
    let invalid = json!({
        "error": {
            "code": 400,
            "message": "Invalid table ID \"a*b\".",
            "errors": [{"message": "Invalid table ID \"a*b\".", "domain": "global", "reason": "invalid"}],
            "status": "INVALID_ARGUMENT"
        }
    });
    for listing in [objects_listing as ExactListing, statistics_listing] {
        let (endpoint, requests) = capture_requests(vec![(400, invalid.clone()), (200, json!({}))]);
        let connection = anonymous_connection_at(&endpoint);

        let batch = listing(&connection, "a*b").next().unwrap().unwrap();
        let schemas = child_structs(batch.column(1));
        assert_eq!(schemas.len(), 1);
        assert_eq!(child_structs(schemas.column(1)).len(), 0);

        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].path().contains("/tables/a%2Ab"));
        assert!(requests[1].path().contains("/datasets/dataset"));
    }

    // Any other 400 is a request the driver got wrong, which must not read as a missing table.
    let malformed = json!({
        "error": {
            "code": 400,
            "message": "Invalid field selection nonsense",
            "errors": [{"message": "Invalid field selection nonsense", "domain": "global", "reason": "invalidParameter"}],
            "status": "INVALID_ARGUMENT"
        }
    });
    let (endpoint, requests) = capture_requests(vec![(400, malformed)]);
    let connection = anonymous_connection_at(&endpoint);
    let error = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            Some("dataset"),
            Some("table"),
            None,
            None,
        )
        .err()
        .unwrap();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// `tables.get` resolves a partition (`$`) or snapshot (`@`) decorator to a table description
/// (verified live, including a partition that holds no rows), but a table ID can contain neither,
/// so an exact filter carrying one matches no table, as the listing would. Only the dataset is
/// looked up.
#[test]
fn an_exact_filter_with_a_table_decorator_matches_no_table() {
    for listing in [objects_listing as ExactListing, statistics_listing] {
        for name in [
            "events$20260101",
            "events$2026",
            "events@0",
            "events@-3600000",
        ] {
            let (endpoint, requests) =
                crate::test_support::http::capture_requests_responding(1, |request| {
                    Some(if request.path().contains("/tables/") {
                        (200, json!({"type": "TABLE", "numRows": "0"}))
                    } else {
                        (200, json!({}))
                    })
                });
            let connection = anonymous_connection_at(&endpoint);

            let batch = listing(&connection, name).next().unwrap().unwrap();
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), 1, "{name}");
            assert_eq!(child_structs(schemas.column(1)).len(), 0, "{name}");

            let requests = requests.join().unwrap();
            assert_eq!(
                requests[0].path(),
                "/bigquery/v2/projects/test-project/datasets/dataset?fields=datasetReference%28datasetId%29",
                "{name}"
            );
        }
    }
}

/// `tables.list` answers a name that cannot be a dataset ID with 400 `invalid` (verified live:
/// `a b` and `sq_scratch@0`), where `datasets.get` answers 404. An exact dataset filter naming one
/// matches no dataset at table depth, as it does at schema depth and with a wildcard appended.
#[test]
fn an_exact_dataset_filter_that_is_not_a_dataset_id_matches_no_dataset() {
    let invalid = json!({
        "error": {
            "code": 400,
            "message": "Invalid dataset ID \"a b\".",
            "errors": [{"message": "Invalid dataset ID \"a b\".", "domain": "global", "reason": "invalid"}],
            "status": "INVALID_ARGUMENT"
        }
    });
    for depth in [Some(ObjectDepth::Tables), None] {
        let (endpoint, requests) = capture_requests(vec![(400, invalid.clone())]);
        let connection = anonymous_connection_at(&endpoint);

        let batch = discover_metadata(&connection, depth, None, Some("a b"), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(child_structs(batch.column(1)).len(), 0, "{depth:?}");

        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].path().contains("/datasets/a%20b/tables"));
    }
}

/// The two metadata listings that resolve an exact `(dataset, table)` pair, as one signature.
type ExactListing =
    fn(&crate::BigQueryConnection, &str) -> Box<dyn RecordBatchReader + Send + 'static>;

fn objects_listing(
    connection: &crate::BigQueryConnection,
    table_name: &str,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            Some("dataset"),
            Some(table_name),
            None,
            None,
        )
        .unwrap()
}

fn statistics_listing(
    connection: &crate::BigQueryConnection,
    table_name: &str,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    connection
        .get_statistics(None, Some("dataset"), Some(table_name), false)
        .unwrap()
}

/// The filter combinations that must never reach a `tables.list`, and the one that must.
///
/// Each case names an exact dataset, so the walk is one `datasets.get` unless the table filter
/// needs a listing: schema depth stops above tables, and an empty table-type, dataset or table
/// filter matches nothing. A wildcard table pattern cannot be resolved by name, so it lists, and
/// an empty listing leaves the `datasets.get` to name the dataset.
#[test]
fn get_objects_preserves_non_table_fast_path_boundaries() {
    /// What the one listed dataset carries, when there is one at all.
    #[derive(Debug, PartialEq)]
    enum Expected {
        NoDbSchema,
        NullTables,
        NoTables,
    }
    let paths = [
        "/bigquery/v2/projects/test-project/datasets/dataset?fields=datasetReference%28datasetId%29",
        "/bigquery/v2/projects/test-project/datasets/dataset/tables?maxResults=1000&fields=nextPageToken%2Ctables%28tableReference%28datasetId%2CtableId%29%2Ctype%29",
        "/bigquery/v2/projects/test-project/datasets/dataset?fields=datasetReference%28datasetId%29",
    ];
    for (depth, db_schema, table_name, table_types, requests, expected) in [
        (
            ObjectDepth::Schemas,
            "dataset",
            "events",
            None,
            &paths[..1],
            Expected::NullTables,
        ),
        (
            ObjectDepth::Tables,
            "dataset",
            "events",
            Some(Vec::new()),
            &paths[..1],
            Expected::NoTables,
        ),
        (
            ObjectDepth::Tables,
            "",
            "events",
            None,
            &[],
            Expected::NoDbSchema,
        ),
        (
            ObjectDepth::Tables,
            "dataset",
            "",
            None,
            &paths[..1],
            Expected::NoTables,
        ),
        (
            ObjectDepth::Tables,
            "dataset",
            "event%",
            None,
            &paths[1..],
            Expected::NoTables,
        ),
    ] {
        let (endpoint, captured) =
            capture_requests(std::iter::repeat_n((200, json!({})), requests.len()).collect());
        let connection = anonymous_connection_at(&endpoint);
        let batch = objects_batch(
            &connection,
            depth,
            Some(db_schema),
            Some(table_name),
            table_types,
        );

        let case = format!("{depth:?} {db_schema:?}.{table_name:?}");
        let db_schemas = child_structs(batch.column(1));
        assert_eq!(
            db_schemas.len(),
            usize::from(expected != Expected::NoDbSchema),
            "{case}"
        );
        match expected {
            Expected::NoDbSchema => {}
            Expected::NullTables => assert!(list(db_schemas.column(1)).is_null(0), "{case}"),
            Expected::NoTables => {
                assert_eq!(child_structs(db_schemas.column(1)).len(), 0, "{case}");
            }
        }
        assert_eq!(
            captured
                .join()
                .unwrap()
                .iter()
                .map(CapturedRequest::path)
                .collect::<Vec<_>>(),
            requests,
            "{case}"
        );
    }
}

/// Depth truncation nulls the level below the requested one (adbc.h) and keeps every level
/// above, and no request goes out for an unreported level. At table depth both table children,
/// columns and constraints, are null, as in the C++ driver framework.
#[test]
fn each_object_depth_truncates_below_itself_and_keeps_the_levels_above() {
    for (depth, requests) in [
        (ObjectDepth::Catalogs, 0),
        (ObjectDepth::Schemas, 1),
        (ObjectDepth::Tables, 2),
        (ObjectDepth::Columns, 3),
        (ObjectDepth::All, 3),
    ] {
        let walk = [
            json!({"datasets": [{"datasetReference": {"datasetId": "dataset"}}]}),
            json!({"tables": [listed_table("events")]}),
            described_table(),
        ];
        let (endpoint, captured) = capture_requests(
            walk.into_iter()
                .take(requests)
                .map(|response| (200, response))
                .collect(),
        );
        let connection = anonymous_connection_at(&endpoint);
        let batch = objects_batch(&connection, depth, None, None, None);

        assert_eq!(string_at(batch.column(0), 0), "test-project", "{depth:?}");
        if depth == ObjectDepth::Catalogs {
            assert!(list(batch.column(1)).is_null(0), "{depth:?}");
        } else {
            let db_schemas = child_structs(batch.column(1));
            assert_eq!(string_at(db_schemas.column(0), 0), "dataset", "{depth:?}");
            if depth == ObjectDepth::Schemas {
                assert!(list(db_schemas.column(1)).is_null(0), "{depth:?}");
            } else {
                let tables = child_structs(db_schemas.column(1));
                assert_eq!(string_at(tables.column(0), 0), "events", "{depth:?}");
                assert_eq!(string_at(tables.column(1), 0), "TABLE", "{depth:?}");
                let columns = list(tables.column(2));
                let constraints = list(tables.column(3));
                if depth == ObjectDepth::Tables {
                    assert!(columns.is_null(0) && constraints.is_null(0), "{depth:?}");
                } else {
                    assert_eq!(child_structs(tables.column(2)).len(), 1, "{depth:?}");
                    assert!(!constraints.is_null(0), "{depth:?}");
                }
            }
        }
        assert_eq!(captured.join().unwrap().len(), requests, "{depth:?}");
    }
}

/// adbc.h: ordinal_position is "The column's ordinal position in the table (starting from 1)",
/// so a column filter must not renumber the columns it keeps.
#[test]
fn column_ordinals_number_the_table_rather_than_the_filtered_result() {
    let metadata = || {
        Table::decode(
            br#"{"schema": {"fields": [
                {"name": "id", "type": "INT64"},
                {"name": "label", "type": "STRING"},
                {"name": "labelled_at", "type": "TIMESTAMP"}
            ]}}"#,
        )
        .unwrap()
    };
    let listed = |column_name| {
        object_table(
            "events".to_owned(),
            "TABLE".to_owned(),
            Some(metadata()),
            true,
            column_name,
        )
        .unwrap()
        .columns
        .expect("a described table lists its columns")
        .into_iter()
        .map(|column| (column.field.name, column.ordinal_position))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        listed(None),
        [
            ("id".to_owned(), 1),
            ("label".to_owned(), 2),
            ("labelled_at".to_owned(), 3)
        ]
    );
    assert_eq!(
        listed(Some(SqlPattern::new("label%"))),
        [("label".to_owned(), 2), ("labelled_at".to_owned(), 3)]
    );
}

#[test]
fn object_table_lists_an_object_whose_metadata_has_no_schema() {
    // A view whose SQL no longer compiles comes back without a schema; get_objects has no
    // per-object failure, so the view is listed with no columns.
    let table = object_table(
        "broken_view".to_owned(),
        "VIEW".to_owned(),
        Some(Table::default()),
        true,
        None,
    )
    .unwrap();
    assert_eq!(table.table_type, "VIEW");
    assert_eq!(table.columns.as_ref().map(Vec::len), Some(0));

    let table = object_table(
        "events".to_owned(),
        "TABLE".to_owned(),
        Some(
            Table::decode(br#"{"schema": {"fields": [{"name": "id", "type": "INT64"}]}}"#).unwrap(),
        ),
        true,
        None,
    )
    .unwrap();
    assert_eq!(table.columns.as_ref().map(Vec::len), Some(1));
}

/// Listing visible projects would need the Resource Manager API, so with no current project, or
/// a wildcard filter that does not match it, the listing is empty rather than an error, and no
/// request is sent. A filter naming one project is the next test's case.
#[test]
fn metadata_listings_answer_an_unmatched_catalog_with_an_empty_result() {
    for (configured_project, catalog) in
        [(None, None), (Some("test-project"), Some("other%project"))]
    {
        let (endpoint, requests) = capture_requests(Vec::new());
        let connection = anonymous_connection_at_with_defaults(&endpoint, configured_project, None);
        for depth in [
            ObjectDepth::Catalogs,
            ObjectDepth::Schemas,
            ObjectDepth::All,
        ] {
            let mut reader = connection
                .get_objects(depth, catalog, None, None, None, None)
                .unwrap();
            assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0, "{depth:?}");
            assert!(reader.next().is_none());
        }
        let mut reader = connection
            .get_statistics(catalog, None, None, false)
            .unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        assert!(reader.next().is_none());
        assert!(requests.join().unwrap().is_empty());
    }
}

/// A catalog filter naming one project enumerates it, even a foreign one: `datasets.list` takes
/// the project in its URL, and refusing would cost dbt-core 2.0 a billed
/// `INFORMATION_SCHEMA.TABLES` query per schema. The connection's own project need not exist.
#[test]
fn an_exactly_named_catalog_is_enumerated_even_when_it_is_not_the_current_one() {
    for configured_project in [Some("test-project"), None] {
        for (catalog, encoded_catalog) in [
            ("other-project", "other-project"),
            ("123456789012", "123456789012"),
            ("example.com:project", "example.com%3Aproject"),
        ] {
            let (endpoint, requests) = capture_requests(vec![
                (200, json!({})),
                (
                    200,
                    json!({"datasets": [{"datasetReference": {"datasetId": "analytics"}}]}),
                ),
            ]);
            let connection =
                anonymous_connection_at_with_defaults(&endpoint, configured_project, None);
            let mut reader = connection
                .get_objects(ObjectDepth::Schemas, Some(catalog), None, None, None, None)
                .unwrap();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(string_at(batch.column(0), 0), catalog);
            let schemas = child_structs(batch.column(1));
            assert_eq!(string_at(schemas.column(0), 0), "analytics");

            let requests = requests.join().unwrap();
            assert_eq!(requests.len(), 2, "{configured_project:?}");
            assert!(requests[0].path().contains("maxResults=1&"));
            assert!(
                requests[1]
                    .path()
                    .starts_with(&format!("/bigquery/v2/projects/{encoded_catalog}/datasets")),
                "{}",
                requests[1].path()
            );
        }
    }
}

const FOREIGN: Option<&str> = Some("other-project");

/// Missing descendants must not manufacture a foreign catalog, and catalog-depth discovery
/// must establish existence even though it never fetches any descendants.
#[test]
fn an_absent_foreign_catalog_leaves_no_metadata_at_any_depth() {
    for configured_project in [Some("test-project"), None] {
        for depth in [
            Some(ObjectDepth::Catalogs),
            Some(ObjectDepth::Schemas),
            Some(ObjectDepth::Tables),
            Some(ObjectDepth::Columns),
            Some(ObjectDepth::All),
            None,
        ] {
            for (schema, table) in [
                (None, None),
                (Some("dataset"), None),
                (Some("dataset"), Some("events")),
            ] {
                let (endpoint, requests) = capture_requests(vec![(404, not_found_response())]);
                let connection =
                    anonymous_connection_at_with_defaults(&endpoint, configured_project, None);
                let mut reader =
                    discover_metadata(&connection, depth, FOREIGN, schema, table).unwrap();
                assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0, "{depth:?}");
                assert!(reader.next().is_none());
                let requests = requests.join().unwrap();
                assert_eq!(requests.len(), 1);
                assert!(
                    requests[0]
                        .path()
                        .contains("/other-project/datasets?maxResults=1&")
                );
            }
        }
    }
}

#[test]
fn a_foreign_catalog_can_exist_without_any_matching_schemas() {
    for depth in [
        Some(ObjectDepth::Catalogs),
        Some(ObjectDepth::Schemas),
        None,
    ] {
        // The probe, then the missing dataset's `datasets.get` or `tables.list`.
        let mut responses = vec![(200, json!({}))];
        if depth != Some(ObjectDepth::Catalogs) {
            responses.push((404, not_found_response()));
        }
        let (endpoint, requests) = capture_requests(responses);
        let connection = anonymous_connection_at(&endpoint);
        let batch = discover_metadata(&connection, depth, FOREIGN, Some("missing"), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(string_at(batch.column(0), 0), "other-project");
        if depth == Some(ObjectDepth::Catalogs) {
            assert!(batch.column(1).is_null(0));
        } else {
            assert_eq!(child_structs(batch.column(1)).len(), 0);
        }
        let requests = requests.join().unwrap();
        assert!(requests[0].path().contains("maxResults=1&"));
        assert_eq!(
            requests.len(),
            if depth == Some(ObjectDepth::Catalogs) {
                1
            } else {
                2
            }
        );
    }
}

#[test]
fn exact_foreign_table_metadata_remains_discoverable_after_catalog_validation() {
    for depth in [Some(ObjectDepth::All), None] {
        let mut table = described_table();
        table["numRows"] = json!("42");
        let (endpoint, requests) = capture_requests(vec![(200, json!({})), (200, table)]);
        let connection = anonymous_connection_at(&endpoint);
        let batch = discover_metadata(&connection, depth, FOREIGN, Some("dataset"), Some("events"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(string_at(batch.column(0), 0), "other-project");
        let schemas = child_structs(batch.column(1));
        assert_eq!(string_at(schemas.column(0), 0), "dataset");
        assert!(!child_structs(schemas.column(1)).is_empty());
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].path().contains("maxResults=1&"));
        assert!(
            requests[1]
                .path()
                .contains("/other-project/datasets/dataset/tables/events?")
        );
    }
}

#[test]
fn foreign_catalog_validation_preserves_permission_quota_and_service_errors() {
    for depth in [Some(ObjectDepth::Catalogs), Some(ObjectDepth::All), None] {
        for (code, reason, status) in [
            (400, "invalid", Status::InvalidArguments),
            (403, "accessDenied", Status::Unauthorized),
            (403, "quotaExceeded", Status::IO),
            (500, "backendError", Status::IO),
        ] {
            // One answer per attempt the retry policy can make; a final failure is answered once.
            let (endpoint, server) = crate::test_support::http::capture_requests_until_stopped(
                vec![
                    (
                        code,
                        json!({"error": {"code": code, "message": "Refused", "errors": [{"reason": reason}]}}),
                    );
                    crate::rest::retry::MAX_ATTEMPTS as usize
                ],
            );
            let connection = anonymous_connection_at(&endpoint);
            let failure =
                discover_metadata(&connection, depth, FOREIGN, Some("dataset"), Some("events"))
                    .err()
                    .expect("only a missing project produces empty metadata");
            assert_eq!(failure.status, status, "{depth:?} {reason}");
            assert_message(&failure, &["catalog filter other-project", reason]);
            let requests = server.stop();
            assert!(!requests.is_empty());
            assert!(requests.iter().all(|request| {
                request
                    .path()
                    .starts_with("/bigquery/v2/projects/other-project/datasets?maxResults=1&")
            }));
        }
    }
}

#[test]
fn catalog_discovery_does_not_probe_the_current_project() {
    let (endpoint, requests) = capture_requests(Vec::new());
    let connection = anonymous_connection_at(&endpoint);
    for filter in [None, Some("test-project"), Some("test-%")] {
        let batch = connection
            .get_objects(ObjectDepth::Catalogs, filter, None, None, None, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(string_at(batch.column(0), 0), "test-project");
    }
    assert!(!connection.context.state.initialized_clients().metadata);
    assert!(requests.join().unwrap().is_empty());
}

#[test]
fn foreign_catalog_validation_can_be_cancelled_while_waiting_for_the_service() {
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::test_support::http::capture_requests_gated;

    let (seen, received) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (endpoint, requests) = capture_requests_gated(vec![(200, json!({}))], move || {
        seen.send(()).unwrap();
        released.recv().unwrap();
        false
    });
    let connection = anonymous_connection_at(&endpoint);
    let cancel = connection.get_cancel_handle();
    let (finished, result) = mpsc::channel();
    let worker = thread::spawn(move || {
        finished
            .send(discover_metadata(
                &connection,
                Some(ObjectDepth::Catalogs),
                FOREIGN,
                None,
                None,
            ))
            .unwrap();
    });
    received.recv_timeout(Duration::from_secs(10)).unwrap();
    cancel.try_cancel().unwrap();
    let result = result.recv_timeout(Duration::from_secs(10));
    release.send(()).unwrap();
    let failure = result
        .unwrap()
        .err()
        .expect("metadata lookup must be cancelled");
    assert_eq!(failure.status, Status::Cancelled);
    worker.join().unwrap();
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// BigQuery rejects malformed project spellings with 400. An exact catalog filter still matches
/// nothing, as the ADBC validation suite requires for "this catalog does not exist".
#[test]
fn a_catalog_filter_that_is_not_a_project_id_lists_nothing() {
    for (catalog, encoded_catalog) in [
        ("Nope", "Nope"),
        (
            "this catalog does not exist",
            "this%20catalog%20does%20not%20exist",
        ),
    ] {
        let rejection = json!({"error": {
            "code": 400,
            "errors": [{
                "reason": "invalid",
                "message": format!("Invalid project ID '{catalog}'.")
            }]
        }});
        let (endpoint, requests) =
            capture_requests(vec![(400, rejection.clone()), (400, rejection)]);
        let connection = anonymous_connection_at(&endpoint);
        let mut reader = connection
            .get_objects(ObjectDepth::All, Some(catalog), None, None, None, None)
            .unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0, "{catalog}");
        assert!(reader.next().is_none());
        let mut reader = connection
            .get_statistics(Some(catalog), None, None, false)
            .unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0, "{catalog}");
        assert!(reader.next().is_none());
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.path().starts_with(&format!(
                    "/bigquery/v2/projects/{encoded_catalog}/datasets?maxResults=1&"
                )))
        );
    }
}

/// adbc.h calls the table-type filter values "table types [that] can be fetched from
/// GetTableTypes", and dbt-core 2.0 panics on an unknown BigQuery type (G73).
///
/// Checked on returned rows, not on `bigquery_table_type` alone, because the `tables.list` entry
/// and the `tables.get` description each carry their own `type`. The type filter is checked too,
/// since it runs on the normalized type.
#[test]
fn objects_report_only_the_table_types_get_table_types_advertises() {
    use crate::connection::info::TABLE_TYPES;

    for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
        for reported in [
            "",
            "TABLE",
            "VIEW",
            "EXTERNAL",
            "MATERIALIZED_VIEW",
            "SNAPSHOT",
            // The sixth type the REST API has not grown yet; `Table.type` is a free-form
            // string in the discovery document, not an enum.
            "SEMANTIC_VIEW",
        ] {
            let expected = if TABLE_TYPES.contains(&reported) {
                reported
            } else {
                "TABLE"
            };
            let mut described = described_table();
            described["type"] = json!(reported);
            let mut listed = listed_table("events");
            listed["type"] = json!(reported);

            // Both routes to a table row: the dataset listing, and the exact-name `tables.get`
            // that skips it.
            for exact in [false, true] {
                let mut responses = if exact {
                    Vec::new()
                } else {
                    vec![(200, json!({"tables": [listed.clone()]}))]
                };
                if exact || depth != ObjectDepth::Tables {
                    responses.push((200, described.clone()));
                }
                let expected_requests = responses.len();
                let (endpoint, requests) = capture_requests(responses);
                let connection = anonymous_connection_at(&endpoint);

                let batch = objects_batch(
                    &connection,
                    depth,
                    Some("dataset"),
                    exact.then_some("events"),
                    Some(vec![expected]),
                );
                let tables = child_structs(child_structs(batch.column(1)).column(1));
                let context = format!("{depth:?} {reported:?} exact={exact}");
                assert_eq!(tables.len(), 1, "{context}");
                let row_type = string_at(tables.column(1), 0);
                assert_eq!(row_type, expected, "{context}");
                assert!(
                    TABLE_TYPES.contains(&row_type),
                    "{context}: {row_type:?} is not advertised by GetTableTypes"
                );
                assert_eq!(
                    requests.join().unwrap().len(),
                    expected_requests,
                    "{context}"
                );
            }
        }
    }
}

/// `xdbc_type_name` spells a column's type as `get_table_schema`'s `BIGQUERY:type` does. The
/// REST spelling (`INTEGER`, `FLOAT`, `BOOLEAN`, `RECORD`) is not GoogleSQL and drops
/// repeatedness, which no other `COLUMN_SCHEMA` field carries. Unlike the Go driver (G77).
#[test]
fn xdbc_type_names_are_the_googlesql_types_get_table_schema_reports() {
    let schema = json!({"fields": [
        {"name": "count", "type": "INTEGER"},
        {"name": "ratio", "type": "FLOAT"},
        {"name": "flag", "type": "BOOLEAN"},
        {"name": "nested", "type": "RECORD", "fields": [{"name": "inner", "type": "INTEGER"}]},
        {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        {"name": "label", "type": "STRING"},
    ]});
    let described = json!({"type": "TABLE", "schema": schema});
    let (endpoint, requests) = capture_requests(vec![
        // get_objects: one exact `tables.get`, then get_table_schema's own.
        (200, described.clone()),
        (200, described),
    ]);
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(
        &connection,
        ObjectDepth::Columns,
        Some("dataset"),
        Some("events"),
        None,
    );
    let columns = child_structs(child_structs(child_structs(batch.column(1)).column(1)).column(2));
    let reported = strings(columns.column(4));
    assert_eq!(
        reported,
        [
            "INT64",
            "FLOAT64",
            "BOOL",
            "STRUCT<`inner` INT64>",
            "ARRAY<STRING>",
            "STRING",
        ]
    );

    let arrow = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap();
    for (index, field) in arrow.fields().iter().enumerate() {
        assert_eq!(
            field
                .metadata()
                .get(crate::types::BIGQUERY_TYPE_METADATA_KEY),
            Some(&reported[index].to_owned()),
            "{} disagrees between get_objects and get_table_schema",
            field.name()
        );
    }
    assert_eq!(requests.join().unwrap().len(), 2);
}

/// A column of a type the driver cannot convert keeps its REST spelling rather than failing the
/// listing, since `get_objects` has no per-object failure; `get_table_schema` refuses the table.
/// The table's other columns keep their GoogleSQL spellings.
#[test]
fn an_unconvertible_column_type_falls_back_to_the_rest_spelling() {
    let described = json!({
        "type": "TABLE",
        "schema": {"fields": [
            {"name": "count", "type": "INTEGER"},
            {"name": "mystery", "type": "FOREIGN"},
        ]}
    });
    let (endpoint, requests) = capture_requests(vec![(200, described.clone()), (200, described)]);
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(
        &connection,
        ObjectDepth::Columns,
        Some("dataset"),
        Some("events"),
        None,
    );
    let columns = child_structs(child_structs(child_structs(batch.column(1)).column(1)).column(2));
    assert_eq!(strings(columns.column(4)), ["INT64", "FOREIGN"]);

    let failure = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(requests.join().unwrap().len(), 2);
}
