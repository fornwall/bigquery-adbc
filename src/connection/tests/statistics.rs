//! `statistics.rs`: `get_statistics`, and how discovery treats a table or dataset a 403 hides.

use super::*;
use crate::test_support::http::capture_requests_until_stopped;

fn forbidden_response(reason: Option<&str>) -> serde_json::Value {
    let mut response = json!({"error": {"code": 403, "message": "Forbidden"}});
    if let Some(reason) = reason {
        response["error"]["errors"] = json!([{ "reason": reason, "message": "Forbidden" }]);
    }
    response
}

/// Anonymous result tables can be listed but refuse metadata access to other users. Exercise
/// both the bounded fan-out and its final drain, preserving the dataset and every readable table.
#[test]
fn inaccessible_tables_leave_the_rest_of_the_dataset_discoverable() {
    for depth in [Some(ObjectDepth::Columns), Some(ObjectDepth::All), None] {
        for denied_index in [2, TABLES + 1] {
            let mut responses = crowded_dataset_responses(TABLES);
            // A hidden schema uses datasets.list because `_` is a SQL-pattern wildcard.
            responses.insert(
                0,
                ok(json!({"datasets": [{"datasetReference": {"datasetId": "_hidden"}}]})),
            );
            for (_, metadata) in &mut responses[2..] {
                metadata["numRows"] = json!("42");
            }
            responses[denied_index] = (403, forbidden_response(Some("accessDenied")));
            let (endpoint, requests) = capture_requests(responses);
            let connection = anonymous_connection_at(&endpoint);

            let batch = first_batch(discover_metadata(
                &connection,
                depth,
                None,
                Some("_hidden"),
                None,
            ));
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), 1);
            assert_eq!(string_at(schemas.column(0), 0), "_hidden");
            assert_eq!(child_structs(schemas.column(1)).len(), TABLES - 1);
            let requests = requests.join().unwrap();
            assert_eq!(requests.len(), TABLES + 2);
            assert!(requests[0].path().contains("all=true"));
        }
    }
}

/// A `tables.get` failure for an exact table filter does not say whether the table or the dataset
/// is missing, so one `datasets.get` settles it and nothing more goes out; an inaccessible table
/// never manufactures a dataset. `tables.get` answers a name that cannot be a table ID with 400
/// `invalid` rather than 404 (verified live: `a*b` and `a ?b#`), which matches no table either.
#[test]
fn an_unavailable_exact_table_falls_back_to_checking_its_dataset() {
    for depth in [Some(ObjectDepth::Tables), Some(ObjectDepth::Columns), None] {
        for table in [
            (403, forbidden_response(Some("accessDenied"))),
            (403, forbidden_response(None)),
            (404, not_found_response()),
            (400, error_body("invalid", "Invalid table ID \"a*b\".")),
        ] {
            for (status, response, expected_schemas) in
                [(200, json!({}), 1), (404, not_found_response(), 0)]
            {
                let (endpoint, requests) =
                    capture_requests(vec![table.clone(), (status, response)]);
                let connection = anonymous_connection_at(&endpoint);
                let batch = first_batch(discover_metadata(
                    &connection,
                    depth,
                    None,
                    Some("dataset"),
                    Some("secret"),
                ));
                let schemas = child_structs(batch.column(1));
                assert_eq!(schemas.len(), expected_schemas);
                if expected_schemas != 0 {
                    assert_eq!(child_structs(schemas.column(1)).len(), 0);
                }
                let requests = requests.join().unwrap();
                assert_eq!(requests.len(), 2);
                assert!(requests[0].path().contains("/tables/secret?"));
                assert!(!requests[1].path().contains("/tables"));
            }
        }
    }
}

/// Unlike the Go driver, which skips every HTTP 403: quota, billing and service-restriction
/// failures, and a disabled API reported only in `ErrorInfo`, do not mean a table is invisible,
/// and still fail both discovery paths.
#[test]
fn discovery_propagates_403_reasons_other_than_access_denied() {
    for depth in [Some(ObjectDepth::Columns), None] {
        for table_name in [None, Some("secret")] {
            let disabled = json!({"error": {
                "message": "BigQuery API is disabled",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "SERVICE_DISABLED",
                    "domain": "googleapis.com"
                }]
            }});
            for (reason, refusal, status) in [
                ("rateLimitExceeded", None, Status::IO),
                ("quotaExceeded", None, Status::IO),
                ("billingNotEnabled", None, Status::Unauthorized),
                ("blocked", None, Status::Unauthorized),
                ("responseTooLarge", None, Status::InvalidArguments),
                ("someFutureReason", None, Status::Unauthorized),
                (
                    "BigQuery API is disabled",
                    Some(&disabled),
                    Status::InvalidState,
                ),
            ] {
                let refusal =
                    refusal.map_or_else(|| forbidden_response(Some(reason)), Clone::clone);
                let mut responses = Vec::new();
                if table_name.is_none() {
                    responses.push((200, json!({"tables": [listed_table("secret")]})));
                }
                let described_from = responses.len();
                // One answer per attempt the retry policy can make; a final failure is answered
                // once.
                responses.extend(std::iter::repeat_n(
                    (403, refusal),
                    crate::rest::retry::MAX_ATTEMPTS as usize,
                ));
                let (endpoint, server) = capture_requests_until_stopped(responses);
                let connection = anonymous_connection_at(&endpoint);
                let failure =
                    discover_metadata(&connection, depth, None, Some("dataset"), table_name)
                        .err()
                        .expect("a service restriction must fail discovery");
                assert_eq!(failure.status, status, "{reason}");
                assert_message(&failure, &[reason, "test-project.dataset.secret"]);
                let requests = server.stop();
                assert!(requests.len() > described_from);
                assert!(
                    requests[described_from..]
                        .iter()
                        .all(|request| request.path().contains("/tables/secret?"))
                );
            }
        }
    }
}

/// Only discovery of objects inside a dataset suppresses access denials. Resolving the
/// `db_schema` filter (`datasets.get` for an exact name after a denied `tables.list`, or the
/// `datasets.list` walk for a pattern) and an explicit schema request still report the refusal.
#[test]
fn discovery_does_not_hide_dataset_denials_or_direct_schema_errors() {
    for depth in [Some(ObjectDepth::Columns), None] {
        for (db_schema, expected_requests) in [("dataset", 2), ("%", 1)] {
            let (endpoint, requests) =
                capture_requests(vec![
                    (403, forbidden_response(Some("accessDenied")));
                    expected_requests
                ]);
            let connection = anonymous_connection_at(&endpoint);
            let failure = discover_metadata(&connection, depth, None, Some(db_schema), None)
                .err()
                .expect("a refused dataset request must fail discovery");
            assert_eq!(failure.status, Status::Unauthorized);
            let requests = requests.join().unwrap();
            assert_eq!(requests.len(), expected_requests);
            assert!(!requests.last().unwrap().path().contains("/tables"));
        }
    }

    let (endpoint, requests) =
        capture_requests(vec![(403, forbidden_response(Some("accessDenied")))]);
    let connection = anonymous_connection_at(&endpoint);
    let failure = connection
        .get_table_schema(None, Some("dataset"), "secret")
        .unwrap_err();
    assert_eq!(failure.status, Status::Unauthorized);
    requests.served(1);
}

/// With `all=true`, the walk can return a dataset whose tables a custom role without
/// `bigquery.tables.list` cannot enumerate. It drops out and every other dataset still arrives;
/// a quota or billing 403 still fails the call.
#[test]
fn a_dataset_whose_tables_cannot_be_listed_drops_out_of_the_enumeration() {
    for depth in [Some(ObjectDepth::Tables), Some(ObjectDepth::Columns), None] {
        for reason in [Some("accessDenied"), None] {
            // The two datasets' listings run concurrently, so each request is answered by path.
            let expected_requests = if depth == Some(ObjectDepth::Tables) {
                3
            } else {
                4
            };
            let (endpoint, requests) = crate::test_support::http::capture_requests_responding(
                expected_requests,
                move |request| {
                    let path = request.path();
                    Some(if path.contains("/datasets/denied/") {
                        (403, forbidden_response(reason))
                    } else if path.contains("/datasets/visible/tables/events") {
                        (200, described_table())
                    } else if path.contains("/datasets/visible/tables") {
                        (200, json!({"tables": [listed_table("events")]}))
                    } else {
                        (
                            200,
                            json!({"datasets": [
                                {"datasetReference": {"datasetId": "denied"}},
                                {"datasetReference": {"datasetId": "visible"}}
                            ]}),
                        )
                    })
                },
            );
            let connection = anonymous_connection_at(&endpoint);

            let batch = first_batch(discover_metadata(&connection, depth, None, Some("%"), None));
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), 1, "{depth:?}");
            assert_eq!(string_at(schemas.column(0), 0), "visible");
            assert_eq!(requests.join().unwrap().len(), expected_requests);
        }

        // A 403 that does not mean "invisible" still fails the whole call.
        let (endpoint, requests) = capture_requests(vec![
            ok(json!({"datasets": [{"datasetReference": {"datasetId": "denied"}}]})),
            (403, forbidden_response(Some("quotaExceeded"))),
        ]);
        let connection = anonymous_connection_at(&endpoint);
        let failure = discover_metadata(&connection, depth, None, Some("%"), None)
            .err()
            .expect("a quota 403 is not an inaccessible dataset");
        assert_failure(&failure, Status::IO, &["test-project.denied"]);
        requests.served(2);
    }
}

/// A dataset named outright is scanned without the initial `datasets.get`. When its
/// `tables.list` is denied, `datasets.get` still runs so a refusal names the dataset, and a
/// dataset the principal may get but not list drops out, as from a pattern. A 404 needs no
/// second request.
#[test]
fn an_exact_dataset_is_checked_only_when_its_tables_cannot_be_listed() {
    for depth in [Some(ObjectDepth::Tables), Some(ObjectDepth::Columns), None] {
        for (listing, expected_requests) in [
            ((403, forbidden_response(Some("accessDenied"))), 2),
            ((404, not_found_response()), 1),
        ] {
            let (endpoint, server) =
                capture_requests_until_stopped(vec![listing, (200, json!({}))]);
            let connection = anonymous_connection_at(&endpoint);
            let batch = first_batch(discover_metadata(
                &connection,
                depth,
                None,
                Some("dataset"),
                None,
            ));
            assert_eq!(child_structs(batch.column(1)).len(), 0, "{depth:?}");
            let requests = server.stop();
            assert_eq!(requests.len(), expected_requests, "{depth:?}");
            assert!(requests[0].path().ends_with(
                "/datasets/dataset/tables?maxResults=1000&fields=nextPageToken%2Ctables%28tableReference%28datasetId%2CtableId%29%2Ctype%29"
            ));
            if expected_requests == 2 {
                assert!(
                    requests[1]
                        .path()
                        .ends_with("/datasets/dataset?fields=datasetReference%28datasetId%29")
                );
            }
        }
    }
}

/// Hidden datasets cannot be queried through INFORMATION_SCHEMA. Their table metadata still
/// supplies statistics, for both exact table names and patterns, regardless of approximation.
#[test]
fn hidden_dataset_statistics_use_table_metadata() {
    for approximate in [false, true] {
        for table_name in ["events", "ev%"] {
            let mut responses = vec![(
                200,
                json!({"datasets": [
                    {"datasetReference": {"datasetId": "visible"}},
                    {"datasetReference": {"datasetId": "_hidden"}}
                ]}),
            )];
            if table_name == "ev%" {
                responses.push((200, json!({"tables": [listed_table("events")]})));
            }
            responses.push((200, json!({"type": "TABLE", "numRows": "42"})));
            let (endpoint, requests) = capture_requests(responses);
            let connection = anonymous_connection_at(&endpoint);

            let mut reader = connection
                .get_statistics(None, Some("_hidden"), Some(table_name), approximate)
                .unwrap();
            let batch = reader.next().unwrap().unwrap();
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), 1);
            assert_eq!(string_at(schemas.column(0), 0), "_hidden");
            let statistics = child_structs(schemas.column(1));
            assert_eq!(statistics.len(), 1);
            assert_eq!(string_at(statistics.column(0), 0), "events");
            assert!(reader.next().is_none());

            let requests = requests.join().unwrap();
            assert!(requests[0].path().contains("&all=true"));
            let metadata = requests.last().unwrap();
            assert!(metadata.path().contains("/datasets/_hidden/tables/events?"));
            assert!(metadata.path().contains("view=STORAGE_STATS"));
            assert!(
                requests
                    .iter()
                    .all(|request| request.line.starts_with("GET "))
            );
        }
    }
}

/// BigQuery describes a view or external table with zero rows and zero bytes, so neither may
/// surface as an exact empty table. Listed ones are never described; an
/// exact one is described to learn its type and then left out, keeping its dataset.
#[test]
fn views_and_external_tables_report_no_statistics() {
    let listing = json!({"tables": [
        {"tableReference": {"tableId": "external"}, "type": "EXTERNAL"},
        {"tableReference": {"tableId": "stored"}, "type": "TABLE"},
        {"tableReference": {"tableId": "view"}, "type": "VIEW"}
    ]});
    let (endpoint, requests) = capture_requests(vec![
        (200, listing),
        (200, json!({"type": "TABLE", "numRows": "42"})),
    ]);
    let connection = anonymous_connection_at(&endpoint);
    let batch = first_batch(connection.get_statistics(None, Some("dataset"), None, false));
    let statistics = child_structs(child_structs(batch.column(1)).column(1));
    assert_eq!(statistics.len(), 1);
    assert_eq!(string_at(statistics.column(0), 0), "stored");
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].path().contains("/tables/stored?"));

    for table_type in ["VIEW", "EXTERNAL"] {
        let (endpoint, requests) = capture_requests(vec![ok(
            json!({"type": table_type, "numRows": "0", "numTotalLogicalBytes": "0"}),
        )]);
        let connection = anonymous_connection_at(&endpoint);
        let batch =
            first_batch(connection.get_statistics(None, Some("dataset"), Some("events"), false));
        let schemas = child_structs(batch.column(1));
        assert_eq!(schemas.len(), 1, "{table_type}");
        assert_eq!(child_structs(schemas.column(1)).len(), 0, "{table_type}");
        requests.served(1);
    }
}
