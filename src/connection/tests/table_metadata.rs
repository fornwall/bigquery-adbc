//! `table_metadata.rs`: the dataset and table walks, their page tokens, the describe fan-out,
//! and how a listing reports a metadata error.

use super::*;
use crate::test_support::errors::assert_message_omits;

/// Cancellation must propagate through the table task, retaining the most recent rejection.
#[test]
fn cancelled_table_discovery_keeps_the_last_retry_diagnostics() {
    use crate::test_support::http::capture_requests_responding;
    use std::sync::mpsc;
    use std::time::Duration;

    for exact in [false, true] {
        let (seen, received) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let count = 3;
        let mut attempts = 0;
        let (endpoint, requests) = capture_requests_responding(count, move |request| {
            if !request.path().contains("/tables") {
                return Some((200, json!({})));
            }
            attempts += 1;
            match attempts {
                1 => Some((
                    503,
                    json!({"error": {"message": "earlier backend failure"}}),
                )),
                2 => Some((
                    403,
                    json!({"error": {
                        "errors": [{"reason": "rateLimitExceeded", "message": "latest quota failure", "location": "tables.list"}],
                        "privatePayload": "must-not-be-exposed"
                    }}),
                )),
                _ => {
                    seen.send(()).unwrap();
                    released.recv().unwrap();
                    None
                }
            }
        });
        let connection = anonymous_connection_at(&endpoint);
        let cancel = connection.get_cancel_handle();
        let (finished, result) = mpsc::channel();
        let worker = thread::spawn(move || {
            let failure = connection
                .get_objects(
                    ObjectDepth::Tables,
                    None,
                    Some("dataset"),
                    exact.then_some("events"),
                    None,
                    None,
                )
                .err()
                .expect("discovery must be cancelled");
            finished.send(failure).unwrap();
        });
        received.recv_timeout(Duration::from_secs(30)).unwrap();
        cancel.try_cancel().unwrap();
        let result = result.recv_timeout(Duration::from_secs(30));
        release.send(()).unwrap();
        let failure = result.unwrap();
        worker.join().unwrap();
        assert_eq!(requests.join().unwrap().len(), count);
        assert_eq!(failure.status, Status::Cancelled);
        assert_eq!(failure.vendor_code, 403);
        assert_eq!(
            crate::error::failure_reason(&failure),
            Some("rateLimitExceeded")
        );
        assert_message(&failure, &["cancelled", "latest quota failure", "HTTP 403"]);
        assert_message_omits(
            &failure,
            &["earlier backend failure", "must-not-be-exposed"],
        );
    }
}

/// dbt 2.0 classifies a missing relation by the literal `404 Not Found:` substring in
/// the message (`is_bigquery_not_found_error`), ignoring the ADBC status, so both
/// missing-table shapes must contain that exact spelling.
#[test]
fn table_metadata_errors_keep_the_not_found_spelling_dbt_matches() {
    // A missing table, and a table ID BigQuery rejects, which ADBC GetTableSchema also reports
    // as missing. Both keep the rejection's structured details.
    for (status, body, reason, message) in [
        (
            reqwest::StatusCode::NOT_FOUND,
            br#"{"error": {"errors": [{"reason": "notFound", "message": "Not found: Table p:d.t"}], "message": "Not found: Table p:d.t"}}"#.as_slice(),
            "notFound",
            "failed to get BigQuery table metadata for p.d.t: 404 Not Found: notFound: Not found: \
             Table p:d.t (HTTP 404)",
        ),
        (
            reqwest::StatusCode::BAD_REQUEST,
            br#"{"error": {"errors": [{"reason": "invalid", "message": "Invalid table ID"}], "message": "Invalid table ID"}}"#.as_slice(),
            "invalid",
            "failed to get BigQuery table metadata for p.d.t: 404 Not Found: BigQuery rejected the \
             table ID: invalid: Invalid table ID (HTTP 400)",
        ),
    ] {
        let source = MetadataError::Rejected(status, bytes::Bytes::from_static(body));
        let converted = table_metadata_error("p", "d", "t", source);
        assert_eq!(converted.status, Status::NotFound);
        assert_eq!(converted.message, message);
        assert_eq!(
            converted.details,
            Some(vec![
                (
                    crate::error::DETAIL_REASON.to_owned(),
                    reason.as_bytes().to_vec()
                ),
                (
                    crate::error::DETAIL_HTTP_STATUS.to_owned(),
                    status.as_u16().to_string().into_bytes()
                ),
            ])
        );
    }

    let source = MetadataError::Rejected(
        reqwest::StatusCode::FORBIDDEN,
        bytes::Bytes::from_static(br#"{"error": {"message": "Access Denied"}}"#),
    );
    let converted = table_metadata_error("p", "d", "t", source);
    assert_eq!(converted.status, Status::Unauthorized);
    assert_message_omits(&converted, &["404 Not Found:"]);

    // A failure that never reached the service keeps its status, reported under the caller's
    // own account of the operation that was running.
    let converted = table_metadata_error(
        "p",
        "d",
        "t",
        MetadataError::Failed(crate::error::error(
            "the statement was cancelled",
            Status::Cancelled,
        )),
    );
    assert_eq!(converted.status, Status::Cancelled);
    assert_eq!(
        converted.message,
        "failed to get BigQuery table metadata for p.d.t: the statement was cancelled"
    );
}

/// dbt 2.0 panics on an unknown BigQuery table type, so only the five `GetTableTypes`
/// spellings pass through; anything else, including an omitted type, is `TABLE`.
#[test]
fn a_table_type_outside_the_advertised_set_is_reported_as_table() {
    for advertised in ["TABLE", "VIEW", "EXTERNAL", "MATERIALIZED_VIEW", "SNAPSHOT"] {
        assert_eq!(bigquery_table_type(advertised), advertised);
    }
    // The empty type BigQuery omits, a type it might grow, the INFORMATION_SCHEMA spellings this
    // listing never uses, and a casing BigQuery does not send.
    for unknown in [
        "",
        "SEMANTIC_VIEW",
        "BASE TABLE",
        "MATERIALIZED VIEW",
        "CLONE",
        "view",
    ] {
        assert_eq!(bigquery_table_type(unknown), "TABLE", "{unknown}");
    }
}

/// The dataset walk keeps every page's rows, sends the previous page's token, and stops only at
/// an empty token. Every page sends `all=true`, so hidden datasets reach GetObjects too.
#[test]
fn the_dataset_walk_follows_every_page_token() {
    let dataset = |dataset_id: &str| json!({"datasetReference": {"datasetId": dataset_id}});
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({"datasets": [dataset("alpha")], "nextPageToken": "page-2"})),
        ok(json!({"datasets": [dataset("_hidden")], "nextPageToken": "page-3"})),
        (200, json!({"datasets": [dataset("gamma")]})),
    ]);
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(&connection, ObjectDepth::Schemas, None, None, None);
    let schemas = child_structs(batch.column(1));
    assert_eq!(strings(schemas.column(0)), ["_hidden", "alpha", "gamma"]);

    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::path)
            .collect::<Vec<_>>(),
        [
            "/bigquery/v2/projects/test-project/datasets?maxResults=1000&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
            "/bigquery/v2/projects/test-project/datasets?maxResults=1000&pageToken=page-2&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
            "/bigquery/v2/projects/test-project/datasets?maxResults=1000&pageToken=page-3&fields=datasets%28datasetReference%28datasetId%29%29%2CnextPageToken&all=true",
        ]
    );
}

/// A repeated token would otherwise loop forever while the result grows without bound.
#[test]
fn a_repeated_dataset_page_token_is_refused_instead_of_walked_forever() {
    let dataset = |dataset_id: &str| json!({"datasetReference": {"datasetId": dataset_id}});
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({"datasets": [dataset("alpha")], "nextPageToken": "stuck"})),
        ok(json!({"datasets": [dataset("beta")], "nextPageToken": "stuck"})),
    ]);
    let connection = anonymous_connection_at(&endpoint);

    let failure = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .err()
        .expect("a repeated page token is refused");
    assert_failure(&failure, Status::InvalidData, &["repeated a page token"]);

    // The walk stopped at the repeat rather than requesting "stuck" a third time.
    requests.served(2);
}

/// The table walk's page loop interleaves with the describe fan-out; a dropped page, a reused
/// token or an early stop would lose tables from `get_objects`.
#[test]
fn the_table_walk_follows_every_page_token() {
    let (endpoint, requests) = capture_requests(vec![
        ok(json!({"tables": [listed_table("alpha")], "nextPageToken": "page-2"})),
        ok(json!({"tables": [listed_table("beta")], "nextPageToken": "page-3"})),
        (200, json!({"tables": [listed_table("gamma")]})),
    ]);
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(
        &connection,
        ObjectDepth::Tables,
        Some("dataset"),
        None,
        None,
    );
    let schemas = child_structs(batch.column(1));
    assert_eq!(schemas.len(), 1);
    let tables = child_structs(schemas.column(1));
    assert_eq!(strings(tables.column(0)), ["alpha", "beta", "gamma"]);

    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::path)
            .collect::<Vec<_>>(),
        [
            "/bigquery/v2/projects/test-project/datasets/dataset/tables?maxResults=1000&fields=nextPageToken%2Ctables%28tableReference%28datasetId%2CtableId%29%2Ctype%29",
            "/bigquery/v2/projects/test-project/datasets/dataset/tables?maxResults=1000&pageToken=page-2&fields=nextPageToken%2Ctables%28tableReference%28datasetId%2CtableId%29%2Ctype%29",
            "/bigquery/v2/projects/test-project/datasets/dataset/tables?maxResults=1000&pageToken=page-3&fields=nextPageToken%2Ctables%28tableReference%28datasetId%2CtableId%29%2Ctype%29",
        ]
    );
}

/// Every listed table must be described exactly once, however the fan-out schedules them.
#[test]
fn the_table_fan_out_describes_more_tables_than_it_runs_at_once() {
    let (endpoint, requests) = capture_requests(crowded_dataset_responses(TABLES));
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(
        &connection,
        ObjectDepth::Columns,
        Some("dataset"),
        None,
        None,
    );
    let tables = child_structs(child_structs(batch.column(1)).column(1));
    assert_eq!(
        strings(tables.column(0)),
        (0..TABLES)
            .map(|index| format!("table-{index:02}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(child_structs(tables.column(2)).len(), TABLES);

    let mut described = requests
        .join()
        .unwrap()
        .iter()
        .filter_map(|request| Some(request.path().split("/tables/").nth(1)?.to_owned()))
        .collect::<Vec<_>>();
    described.sort_unstable();
    assert_eq!(
        described,
        (0..TABLES)
            .map(|index| {
                format!("table-{index:02}?view=BASIC&fields=schema%2CtableConstraints%2CtableReference%28datasetId%2CtableId%29%2Ctype")
            })
            .collect::<Vec<_>>()
    );
}

/// A table dropped between `tables.list` and `tables.get` answers 404: it drops out and every
/// other table still arrives. An escaping 404 would lose the whole dataset, since a NOT_FOUND
/// listing means the dataset vanished (G72). Covers the bounded fan-out and its final drain,
/// under discovery and statistics alike.
#[test]
fn a_table_that_vanishes_mid_fan_out_leaves_the_rest_of_the_dataset_listed() {
    for depth in [Some(ObjectDepth::Columns), Some(ObjectDepth::All), None] {
        for vanished_index in [1, TABLES] {
            let mut responses = crowded_dataset_responses(TABLES);
            for (_, metadata) in &mut responses[1..] {
                metadata["numRows"] = json!("42");
            }
            responses[vanished_index] = (404, not_found_response());
            let (endpoint, requests) = capture_requests(responses);
            let connection = anonymous_connection_at(&endpoint);

            let batch = first_batch(discover_metadata(
                &connection,
                depth,
                None,
                Some("dataset"),
                None,
            ));
            let schemas = child_structs(batch.column(1));
            assert_eq!(schemas.len(), 1, "the dataset itself must still be listed");
            assert_eq!(string_at(schemas.column(0), 0), "dataset");
            // Which table loses the race is up to the fan-out; that exactly one is dropped is not.
            assert_eq!(child_structs(schemas.column(1)).len(), TABLES - 1);
            assert_eq!(requests.join().unwrap().len(), TABLES + 1);
        }
    }
}

/// Every dataset's `tables.list` is in flight at once. The server holds its answers until every
/// listing has arrived, which a sequential walk never reaches.
#[test]
fn datasets_are_listed_concurrently() {
    use crate::test_support::http::{
        accept, mock_listener, read_captured_request, retire, write_response,
    };

    const DATASETS: usize = 4;
    let (listener, endpoint) = mock_listener();
    let server = thread::spawn(move || {
        let mut held = Vec::new();
        while held.len() < DATASETS {
            let mut stream = accept(&listener, None).unwrap();
            let Some(request) = read_captured_request(&mut stream) else {
                continue;
            };
            let path = request.path();
            if let Some(dataset) = path
                .strip_prefix("/bigquery/v2/projects/test-project/datasets/")
                .and_then(|rest| rest.split_once("/tables?"))
            {
                held.push((stream, dataset.0.to_owned()));
            } else {
                let datasets = (0..DATASETS)
                    .map(|index| json!({"datasetReference": {"datasetId": format!("d{index}")}}))
                    .collect::<Vec<_>>();
                write_response(&mut stream, 200, &json!({ "datasets": datasets }));
            }
        }
        retire(listener);
        for (mut stream, dataset) in held {
            write_response(
                &mut stream,
                200,
                &json!({"tables": [listed_table(&format!("{dataset}_table"))]}),
            );
        }
    });
    let connection = anonymous_connection_at(&endpoint);

    let batch = objects_batch(&connection, ObjectDepth::Tables, None, None, None);
    server.join().unwrap();
    let schemas = child_structs(batch.column(1));
    let tables = child_structs(schemas.column(1));
    assert_eq!(
        strings(tables.column(0)),
        (0..DATASETS)
            .map(|index| format!("d{index}_table"))
            .collect::<Vec<_>>()
    );
}
