use serde_json::json;

use super::*;
use crate::rows::RestValue;
use crate::runtime::{SharedRuntime, new_runtime};
use crate::test_support::http::capture_requests;

fn client(endpoint: &str) -> (SharedRuntime, JobClient) {
    let runtime = new_runtime().unwrap();
    let client = JobClient::for_test(endpoint);
    (runtime, client)
}

fn row(value: &str) -> RestRow {
    RestRow::new(vec![RestValue::String(value.to_owned())])
}

fn values(rows: Vec<RestRow>) -> Vec<String> {
    rows.into_iter()
        .map(|row| {
            row.into_cells()
                .into_iter()
                .next()
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[test]
fn a_short_page_reuses_its_row_allocation() {
    let (runtime, client) = client(crate::test_support::UNREACHABLE_ENDPOINT);
    let rows = vec![row("first"), row("second")];
    let allocation = rows.as_ptr();
    let mut pager = QueryPager::new(
        client,
        None,
        None,
        QueryPage {
            rows,
            page_token: None,
            encoded_len: 0,
        },
    );
    let sink = ResultSink::for_test();
    let batch = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
    assert_eq!(batch.as_ptr(), allocation);
    assert_eq!(values(batch), ["first", "second"]);
}

#[test]
fn splits_resident_pages_without_skipping_or_repeating_rows() {
    for row_count in [
        BATCH_SIZE - 1,
        BATCH_SIZE,
        BATCH_SIZE + 1,
        2 * BATCH_SIZE + 1,
    ] {
        let (runtime, client) = client(crate::test_support::UNREACHABLE_ENDPOINT);
        let mut pager = QueryPager::new(
            client,
            None,
            None,
            QueryPage {
                rows: (0..row_count)
                    .map(|index| row(&index.to_string()))
                    .collect(),
                page_token: None,
                encoded_len: 100,
            },
        );
        let sink = ResultSink::for_test();
        let mut actual = Vec::new();
        loop {
            let batch = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
            if batch.is_empty() {
                assert_eq!(batch.capacity(), 0);
                break;
            }
            assert!(batch.len() <= BATCH_SIZE);
            actual.extend(values(batch));
        }
        let expected = (0..row_count).map(|index| index.to_string());
        assert_eq!(actual, expected.collect::<Vec<_>>());
    }
}

#[test]
fn consumes_initial_rows_and_opaque_pages_once_in_order() {
    let (endpoint, captured) = capture_requests(vec![
        (
            200,
            json!({
                "jobComplete":true,
                "jobReference":{"projectId":"project","jobId":"job","location":"EU"},
                "rows":[{"f":[{"v":"second"}]}],
                "pageToken":"next +/token"
            }),
        ),
        (
            200,
            json!({
                "jobComplete":true,
                "jobReference":{"projectId":"project","jobId":"job"},
                "rows":[],
                "pageToken":"last/token"
            }),
        ),
        (
            200,
            json!({
                "jobComplete":true,
                "rows":[{"f":[{"v":"third"}]}],
                "errors":[{"reason":"warning","message":"successful page warning"}]
            }),
        ),
    ]);
    let (runtime, client) = client(&endpoint);
    let reference = JobReference::new("project", Some("EU"), "job".to_owned());
    let mut pager = QueryPager::new(
        client,
        Some(reference),
        None,
        QueryPage {
            rows: vec![row("first")],
            page_token: Some("first/token ?".to_owned()),
            encoded_len: 0,
        },
    );
    let sink = ResultSink::for_test();
    for expected in ["first", "second", "third"] {
        let rows = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
        assert_eq!(values(rows), [expected]);
    }
    let rows = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
    assert!(rows.is_empty(), "the last page has no token to follow");

    let captured = captured.join().unwrap();
    assert_eq!(captured.len(), 3);
    let expected_prefix = "/bigquery/v2/projects/project/queries/job?maxResults=100000&timeoutMs=45000&formatOptions.useInt64Timestamp=true&location=EU&pageToken=";
    assert_eq!(
        captured[0].path(),
        format!("{expected_prefix}first%2Ftoken%20%3F")
    );
    assert_eq!(
        captured[1].path(),
        format!("{expected_prefix}next%20%2B%2Ftoken")
    );
    assert_eq!(captured[2].path(), format!("{expected_prefix}last%2Ftoken"));
}

#[test]
fn repeated_token_is_rejected_even_when_the_page_has_rows() {
    let (endpoint, captured) = capture_requests(vec![(
        200,
        json!({
            "jobComplete":true,
            "rows":[{"f":[{"v":"duplicate"}]}],
            "pageToken":"same"
        }),
    )]);
    let (runtime, client) = client(&endpoint);
    let mut pager = QueryPager::new(
        client,
        Some(JobReference::new("project", Some("EU"), "job".to_owned())),
        None,
        QueryPage {
            rows: vec![row("first")],
            page_token: Some("same".to_owned()),
            encoded_len: 0,
        },
    );
    let sink = ResultSink::for_test();
    let rows = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
    assert_eq!(values(rows), ["first"]);
    let failure = runtime.block_on(pager.next_batch(&sink)).unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("repeated a query result page token")
    );
    let ArrowError::ExternalError(source) = failure else {
        panic!("expected the original ADBC error");
    };
    let failure = source.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(failure.status, Status::IO);
    assert!(failure.message.ends_with(" (Query: https://console.cloud.google.com/bigquery?project=project&j=bq:EU:job&page=queryresults)"));
    assert_eq!(captured.join().unwrap().len(), 1);
}

#[test]
fn page_identity_mismatch_is_rejected_before_rows_are_delivered() {
    let (endpoint, captured) = capture_requests(vec![(
        200,
        json!({
            "jobComplete":true,
            "jobReference":{"projectId":"project","jobId":"other","location":"EU"},
            "rows":[{"f":[{"v":"poisoned"}]}]
        }),
    )]);
    let (runtime, client) = client(&endpoint);
    let mut pager = QueryPager::new(
        client,
        Some(JobReference::new("project", Some("EU"), "job".to_owned())),
        None,
        QueryPage {
            rows: Vec::new(),
            page_token: Some("next".to_owned()),
            encoded_len: 0,
        },
    );
    let sink = ResultSink::for_test();
    let failure = runtime.block_on(pager.next_batch(&sink)).unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("names job project:EU.other, but the request was for job project:EU.job"),
        "{failure}"
    );
    let ArrowError::ExternalError(source) = failure else {
        panic!("expected the original ADBC error");
    };
    let failure = source.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(failure.status, Status::InvalidData);

    // The refusal comes before the page is handed on: the rejected page's rows are dropped
    // rather than buffered, so the pager has nothing left to deliver and no token to follow.
    let rows = runtime.block_on(pager.next_batch(&sink)).unwrap().unwrap();
    assert!(rows.is_empty(), "a rejected page delivered its rows");
    assert_eq!(captured.join().unwrap().len(), 1);
}
