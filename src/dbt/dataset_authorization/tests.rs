use std::sync::{Arc, Barrier};
use std::thread;

use adbc_core::Statement as _;
use adbc_core::error::Status;
use serde_json::json;

use crate::test_support::driver::{test_statement, test_statement_at};
use crate::test_support::http::{
    capture_requests, capture_requests_gated, capture_requests_responding, error_body, ok,
};
use crate::{BigQueryStatement, BigQueryStatementOption};

use super::{ViewAuthorizations, parse_view_authorizations};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::options::{
    assert_string_option, option_string, set_option, set_option_error,
};

const OPTION: BigQueryStatementOption = BigQueryStatementOption::AuthorizeViewToDatasets;

/// The payload dbt 2.0 sends for one view: exactly one view mapped to exactly one dataset.
fn payload(view: &str, project: &str, dataset: &str) -> String {
    json!({view: [{"project": project, "dataset": dataset}]}).to_string()
}

/// Parses `value` with the `data-project` default project and `dataset`, if any.
fn parse(
    value: &str,
    dataset: Option<&str>,
) -> adbc_core::error::Result<Option<ViewAuthorizations>> {
    let project = crate::identifiers::ProjectIdentifier::try_new("data-project").unwrap();
    parse_view_authorizations(value, Some(&project), dataset)
}

/// Configures the one view every request scenario below authorizes on `data-project.raw`.
fn authorize_granted_view(statement: &mut BigQueryStatement) {
    set_option(
        statement,
        OPTION,
        payload("data-project.analytics.granted_view", "data-project", "raw"),
    );
}

#[test]
fn view_authorizations_round_trip_sorted_fully_qualified_and_deduplicated() {
    let mut statement = test_statement();
    assert_string_option(&statement, OPTION, None);

    let unsorted = json!({
        "data-project.analytics.z_view": [
            {"project": "data-project", "dataset": "raw_second"},
            {"project": "data-project", "dataset": "raw_first"},
            {"project": "data-project", "dataset": "raw_first"},
        ],
        "data-project.analytics.a_view": [
            {"project": "other-project", "dataset": "raw_first"},
        ],
    });
    set_option(&mut statement, OPTION, unsorted.to_string());
    assert_eq!(
        option_string(&statement, OPTION),
        r#"{"data-project.analytics.a_view":[{"dataset":"raw_first","project":"other-project"}],"data-project.analytics.z_view":[{"dataset":"raw_first","project":"data-project"},{"dataset":"raw_second","project":"data-project"}]}"#
    );

    // Both spellings of "no authorizations" clear the option.
    for value in ["", "{}"] {
        set_option(&mut statement, OPTION, value);
        assert_string_option(&statement, OPTION, None);
    }
}

#[test]
fn malformed_payloads_are_rejected_before_execution_without_echoing_them() {
    const SECRET: &str = "authorize-view-secret-sentinel";
    let mut statement = test_statement();
    let accepted = payload("data-project.analytics.model", "data-project", "raw");
    set_option(&mut statement, OPTION, accepted);

    for (name, value) in [
        ("not JSON", format!("{{{SECRET}")),
        ("not an object", json!([SECRET]).to_string()),
        ("datasets not an array", json!({"v": SECRET}).to_string()),
        (
            "dataset not an object",
            json!({"data-project.analytics.v": [SECRET]}).to_string(),
        ),
        (
            "missing project",
            json!({"data-project.analytics.v": [{"dataset": SECRET}]}).to_string(),
        ),
        (
            "empty dataset",
            json!({"data-project.analytics.v": [{"project": "data-project", "dataset": ""}]})
                .to_string(),
        ),
        (
            "dataset is not an identifier",
            json!({"data-project.analytics.v": [{"project": "data-project", "dataset": "raw-dataset"}]})
                .to_string(),
        ),
        (
            "project contains a resource separator",
            json!({"data-project.analytics.v": [{"project": "path/segment", "dataset": "raw"}]})
                .to_string(),
        ),
        (
            "unparsable view name",
            json!({"project..view": [{"project": "data-project", "dataset": "raw"}]}).to_string(),
        ),
    ] {
        let error = set_option_error(&mut statement, OPTION, value);
        assert_eq!(error.status, Status::InvalidArguments, "{name}");
        assert_message_omits(&error, &[SECRET]);
    }
    assert_eq!(
        option_string(&statement, OPTION),
        json!({"data-project.analytics.model": [{"dataset": "raw", "project": "data-project"}]})
            .to_string(),
        "a rejected payload must not replace the previous value"
    );
}

/// A payload that is not JSON reports where parsing stopped, without quoting the payload.
#[test]
fn an_unparsable_payload_reports_the_syntax_position() {
    let error =
        parse_view_authorizations("{\n  \"data-project.analytics.v\": [oops]\n}", None, None)
            .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        format!(
            "BigQuery option {} must be valid JSON: syntax error at line 2 column 32",
            OPTION.as_str()
        )
    );
}

/// A view mapped to no datasets is dropped while parsing, so it neither selects this operation
/// nor deselects another one. Its name is still validated.
#[test]
fn a_view_with_no_source_datasets_is_dropped_rather_than_silently_doing_nothing() {
    let mut statement = test_statement();
    set_option(
        &mut statement,
        OPTION,
        json!({"data-project.analytics.empty_view": []}).to_string(),
    );
    assert_string_option(&statement, OPTION, None);

    // Only the empty entry goes; the populated ones stay.
    let authorizations = json!({
        "data-project.analytics.empty_view": [],
        "data-project.analytics.granted_view": [
            {"project": "data-project", "dataset": "raw"},
        ],
    });
    set_option(&mut statement, OPTION, authorizations.to_string());
    assert_eq!(
        option_string(&statement, OPTION),
        r#"{"data-project.analytics.granted_view":[{"dataset":"raw","project":"data-project"}]}"#
    );

    // The name is parsed before the entry is dropped, so a view a bare name cannot resolve is
    // still reported.
    let error = parse(&json!({"solo_view": []}).to_string(), None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn view_names_resolve_one_two_and_three_part_spellings() {
    let views = parse(
        &json!({
            "solo_view": [{"project": "data-project", "dataset": "raw"}],
            "other_dataset.paired_view": [{"project": "data-project", "dataset": "raw"}],
            "other-project.other_dataset.full_view": [
                {"project": "data-project", "dataset": "raw"}
            ],
        })
        .to_string(),
        Some("analytics"),
    )
    .unwrap()
    .unwrap();
    let names = views
        .iter()
        .map(|(view, _)| view.as_option_value())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "data-project.analytics.solo_view",
            "data-project.other_dataset.paired_view",
            "other-project.other_dataset.full_view",
        ]
    );

    // A one-part name needs both defaults, a two-part name only the project.
    let error = parse(&payload("solo_view", "data-project", "raw"), None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

/// The complete dbt 2.0 contract: the option, its dummy `none` query behind a query comment,
/// and the job labels dbt 2.0 adds to every statement. One view is appended to one dataset.
#[test]
fn dbt_grant_access_to_appends_the_view_and_returns_the_dataset_etag() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "etag": "dataset-etag",
            "datasetReference": {"projectId": "data-project", "datasetId": "raw"},
            "access": [
                {"role": "OWNER", "userByEmail": "owner@example.com"},
                {"view": {
                    "projectId": "data-project",
                    "datasetId": "analytics",
                    "tableId": "another_view"
                }},
            ]
        })),
        ok(json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::Labels,
        r#"{"dbt_invocation_id":"abc"}"#,
    );
    authorize_granted_view(&mut statement);
    // dbt 2.0's `execute_with_options(sql = "none", ...)` behind its query comment.
    statement
        .set_sql_query("/* bigquery-adbc: model.project.granted_view */\nnone")
        .unwrap();

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, 0);
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2, "one dataset get and one dataset patch");
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/data-project/datasets/raw?fields=access%2Cetag"
    );
    assert_eq!(
        requests[1].path(),
        "/bigquery/v2/projects/data-project/datasets/raw?fields=etag"
    );
    let access = requests[1].body["access"]
        .as_array()
        .expect("the patch carries the complete access list");
    assert_eq!(
        access.len(),
        3,
        "existing entries must survive the read-modify-write: {access:?}"
    );
    assert_eq!(access[0]["role"], "OWNER");
    assert_eq!(access[1]["view"]["tableId"], "another_view");
    assert_eq!(
        access[2]["view"],
        json!({
            "projectId": "data-project",
            "datasetId": "analytics",
            "tableId": "granted_view"
        })
    );
    assert_eq!(
        access[2].get("role"),
        None,
        "an authorized view carries no role"
    );
    assert_eq!(
        requests[1].header("If-Match"),
        Some("dataset-etag"),
        "the patch must carry the fetched ETag so a concurrent ACL change is not lost"
    );
    assert_eq!(
        requests[1].body.get("etag"),
        None,
        "BigQuery ignores a body ETag, so sending one would only look like a precondition"
    );
}

#[test]
fn an_already_authorized_view_issues_no_patch() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({
        "etag": "dataset-etag",
        "access": [
            {"role": "READER", "userByEmail": "reader@example.com"},
            {"view": {
                "projectId": "data-project",
                "datasetId": "analytics",
                "tableId": "granted_view"
            }},
        ]
    }))]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "an idempotent re-run reads the ACL and leaves it alone"
    );
}

#[test]
fn domain_scoped_projects_survive_authorization_paths_and_view_references() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({"etag": "dataset-etag", "access": []})),
        ok(json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        OPTION,
        json!({
            "analytics.example.com:project.marts.customer_view": [{
                "project": "example.com:source",
                "dataset": "raw",
            }],
        })
        .to_string(),
    );
    statement.execute().unwrap();

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/example.com%3Asource/datasets/raw?fields=access%2Cetag"
    );
    assert_eq!(
        requests[1].body["access"][0]["view"],
        json!({
            "projectId": "analytics.example.com:project",
            "datasetId": "marts",
            "tableId": "customer_view",
        })
    );
}

/// dbt 2.0 sends one view per statement, but the option's shape is a map of lists; several
/// views and several datasets must each be visited exactly once, in a deterministic order.
#[test]
fn multiple_views_and_datasets_visit_every_pair() {
    let dataset = |table: &str| {
        json!({
            "etag": format!("{table}-etag"),
            "access": [{"view": {
                "projectId": "data-project",
                "datasetId": "analytics",
                "tableId": table
            }}]
        })
    };
    // Two views on two datasets each; every dataset already authorizes exactly one of them,
    // so half the pairs are appended and half are skipped.
    let (endpoint, captured) = capture_requests(vec![
        ok(dataset("first_view")),
        ok(dataset("second_view")),
        ok(json!({})),
        ok(dataset("first_view")),
        ok(json!({})),
        ok(dataset("second_view")),
    ]);
    let mut statement = test_statement_at(&endpoint);
    let authorizations = json!({
        "data-project.analytics.first_view": [
            {"project": "data-project", "dataset": "raw_alpha"},
            {"project": "data-project", "dataset": "raw_beta"},
        ],
        "data-project.analytics.second_view": [
            {"project": "data-project", "dataset": "raw_alpha"},
            {"project": "data-project", "dataset": "raw_beta"},
        ],
    });
    set_option(&mut statement, OPTION, authorizations.to_string());
    assert_eq!(statement.execute_update().unwrap(), Some(0));

    let requests = captured.join().unwrap();
    let lines = requests
        .iter()
        .map(|request| {
            let mut parts = request.line.split_ascii_whitespace();
            let method = parts.next().unwrap_or_default();
            let path = parts.next().unwrap_or_default();
            let dataset = path.rsplit('/').next().unwrap_or_default();
            format!("{method} {}", dataset.split('?').next().unwrap_or_default())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "GET raw_alpha",
            "GET raw_beta",
            "PATCH raw_beta",
            "GET raw_alpha",
            "PATCH raw_alpha",
            "GET raw_beta",
        ]
    );
    assert_eq!(requests[2].header("If-Match"), Some("second_view-etag"));
    assert_eq!(requests[4].header("If-Match"), Some("first_view-etag"));
}

/// An access-list entry other than the granted view, which every re-read must keep.
fn other_entry() -> serde_json::Value {
    json!({"role": "READER", "userByEmail": "analyst@example.com"})
}

/// The granted view as an existing access-list entry.
fn granted_entry() -> serde_json::Value {
    json!({"view": {"projectId": "data-project", "datasetId": "analytics",
        "tableId": "granted_view"}})
}

fn precondition_failed() -> (u16, serde_json::Value) {
    (
        412,
        json!({"error": {"code": 412, "message": "Precondition check failed."}}),
    )
}

/// A patch that lost the `If-Match` race re-reads the list and patches again under the fresh
/// ETag, keeping the entry the other writer added.
#[test]
fn a_lost_etag_race_is_patched_again_under_the_fresh_etag() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({"etag": "stale-etag", "access": []})),
        precondition_failed(),
        ok(json!({"etag": "fresh-etag", "access": [other_entry()]})),
        ok(json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    assert_eq!(statement.execute_update().unwrap(), Some(0));

    let requests = captured.join().unwrap();
    assert_eq!(requests[1].header("If-Match"), Some("stale-etag"));
    assert!(requests[2].line.starts_with("GET "), "{}", requests[2].line);
    assert_eq!(requests[3].header("If-Match"), Some("fresh-etag"));
    assert_eq!(
        requests[3].body["access"],
        json!([other_entry(), granted_entry()])
    );
}

/// A patch that may have applied unseen (a 503 or a lost answer) is not replayed: the re-read
/// shows the view listed, so nothing more is sent.
#[test]
fn an_ambiguous_access_list_patch_is_confirmed_by_a_reread() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({"etag": "dataset-etag", "access": []})),
        (503, error_body("backendError", "Backend error.")),
        ok(json!({"etag": "applied-etag", "access": [granted_entry()]})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        3,
        "a read, the patch, and the confirming read"
    );
    assert!(requests[2].line.starts_with("GET "), "{}", requests[2].line);
}

/// Losing every round to another writer is `InvalidState`, naming the view.
#[test]
fn an_access_list_that_keeps_changing_fails_after_bounded_rounds() {
    let mut responses = Vec::new();
    for round in 0..super::MAX_GRANT_ROUNDS {
        responses.push(ok(json!({"etag": format!("etag-{round}"), "access": []})));
        responses.push(precondition_failed());
    }
    let (endpoint, captured) = capture_requests(responses);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(error.vendor_code, 412);
    assert_message(
        &error,
        &[
            "kept changing concurrently",
            "data-project.analytics.granted_view",
        ],
    );
    captured.join().unwrap();
}

/// dbt grants each authorized view in its own statement, back to back, and BigQuery answers more
/// than about five access-list updates per dataset in ten seconds with `rateLimitExceeded`. That
/// rejection applied nothing, so the patch is resent under the same ETag instead of failing the
/// view model.
#[test]
fn a_rate_limited_access_list_patch_is_resent_under_the_same_etag() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({"etag": "dataset-etag", "access": []})),
        (
            403,
            error_body(
                "rateLimitExceeded",
                "Exceeded rate limits: too many dataset metadata update operations for this \
                 dataset.",
            ),
        ),
        ok(json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    assert_eq!(statement.execute_update().unwrap(), Some(0));

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 3);
    for patch in &requests[1..] {
        assert!(patch.line.starts_with("PATCH "), "{}", patch.line);
        assert_eq!(patch.header("If-Match"), Some("dataset-etag"));
        assert_eq!(
            patch.body["access"][0]["view"]["tableId"], "granted_view",
            "{}",
            patch.body
        );
    }
}

/// An access-list patch that never gets an answer is re-read and retried for a bounded number of
/// rounds, and its failure is named once, by the authorization.
#[test]
fn an_unanswered_access_list_patch_names_the_authorization_once() {
    let rounds = super::MAX_GRANT_ROUNDS as usize;
    let (endpoint, captured) = capture_requests_responding(2 * rounds, |request| {
        // The patch is read whole and its connection closed without an answer.
        request
            .line
            .starts_with("GET ")
            .then(|| ok(json!({"etag": "dataset-etag", "access": []})))
    });
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::IO);
    let context = "failed to authorize view data-project.analytics.granted_view on BigQuery \
                   dataset data-project.raw";
    assert!(
        error
            .message
            .starts_with(&format!("{context}: error sending request")),
        "{}",
        error.message
    );
    assert_eq!(
        error.message.matches(context).count(),
        1,
        "{}",
        error.message
    );
    assert_eq!(captured.join().unwrap().len(), 2 * rounds);
}

/// An empty ETag would send no `If-Match`, so a dataset that reports none is refused rather than
/// blind-written.
#[test]
fn a_dataset_without_an_etag_is_never_blind_written() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({"access": []}))]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    let error = statement.execute_update().unwrap_err();
    assert_failure(
        &error,
        Status::InvalidData,
        &["no ETag", "data-project.raw"],
    );
    assert_eq!(
        captured.join().unwrap().len(),
        1,
        "the get happened and no patch followed it"
    );
}

#[test]
fn a_missing_source_dataset_is_reported_as_not_found() {
    let (endpoint, captured) = capture_requests(vec![(
        404,
        json!({
            "error": {
                "code": 404,
                "message": "Not found: Dataset data-project:raw",
                "status": "NOT_FOUND",
                "errors": [
                    {"message": "Not found", "domain": "global", "reason": "notFound"}
                ]
            }
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);

    let error = statement.execute_update().unwrap_err();
    assert_failure(&error, Status::NotFound, &["data-project.raw"]);
    captured.join().unwrap();
}

#[test]
fn cancelling_an_authorization_interrupts_the_request() {
    let request_ready = Arc::new(Barrier::new(2));
    let cancellation_sent = Arc::new(Barrier::new(2));
    let server_ready = request_ready.clone();
    let server_cancelled = cancellation_sent.clone();
    let (endpoint, captured) = capture_requests_gated(vec![ok(json!({}))], move || {
        server_ready.wait();
        server_cancelled.wait();
        false
    });
    let mut statement = test_statement_at(&endpoint);
    authorize_granted_view(&mut statement);
    let handle = statement.get_cancel_handle();
    let canceller = thread::spawn(move || {
        request_ready.wait();
        handle.try_cancel().unwrap();
        cancellation_sent.wait();
    });

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::Cancelled);
    canceller.join().unwrap();
    captured.join().unwrap();
}
