use std::collections::BTreeMap;

use adbc_core::Statement as _;
use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use serde_json::json;

use crate::BigQueryStatement;
use crate::BigQueryStatementOption as Key;
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::test_support::driver::{test_statement, test_statement_at};
use crate::test_support::errors::{assert_failure, assert_message_omits};
use crate::test_support::http::{capture_requests, capture_requests_responding, error_body, ok};
use crate::test_support::options::{
    assert_string_option, option_string, set_option, set_option_error,
};

#[test]
fn table_metadata_options_are_typed_sorted_atomic_and_secret_safe() {
    const SECRET: &str = "column-description-secret-sentinel";
    let mut statement = test_statement();

    set_option(
        &mut statement,
        Key::UpdateColumnsDescription,
        r#"{"z":"last","a":"first"}"#,
    );
    assert_eq!(
        option_string(&statement, Key::UpdateColumnsDescription),
        r#"{"a":"first","z":"last"}"#
    );
    set_option(
        &mut statement,
        Key::UpdateColumnsPolicyTags,
        r#"{"a":["projects/p/locations/eu/taxonomies/t/policyTags/x"]}"#,
    );

    let error = set_option_error(
        &mut statement,
        Key::UpdateColumnsDescription,
        format!(r#"{{"a":42,"secret":"{SECRET}"}}"#),
    );
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message_omits(&error, &[SECRET]);
    assert_eq!(
        option_string(&statement, Key::UpdateColumnsDescription),
        r#"{"a":"first","z":"last"}"#,
        "a rejected payload must not replace the previous value"
    );
    assert_eq!(
        set_option_error(
            &mut statement,
            Key::UpdateColumnsPolicyTags,
            r#"{"a":"not-an-array"}"#
        )
        .status,
        Status::InvalidArguments
    );

    // Every other option is cleared by the empty string, and the fork cannot express a cleared
    // description either: it dispatches on a non-empty value.
    set_option(
        &mut statement,
        Key::UpdateDescription,
        "A documented relation.",
    );
    set_option(&mut statement, Key::UpdateDescription, "");
    assert_string_option(&statement, Key::UpdateDescription, None);
    assert_eq!(
        set_option_error(&mut statement, Key::UpdateDescription, OptionValue::Int(42)).status,
        Status::InvalidArguments
    );
}

#[test]
fn dbt_column_metadata_update_applies_dotted_paths_and_preserves_the_schema() {
    let (endpoint, captured) = capture_requests(vec![
        ok(json!({
            "etag": "must-not-be-sent-back",
            "schema": {
                "fields": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "mode": "REQUIRED",
                        "description": "old id",
                        "policyTags": {"names": ["old-id-tag"]}
                    },
                    {
                        "name": "my_array",
                        "type": "RECORD",
                        "mode": "REPEATED",
                        "description": "old array",
                        "policyTags": {"names": ["old-record-tag"]},
                        "fields": [
                            {
                                "name": "amount",
                                "type": "NUMERIC",
                                "mode": "REQUIRED",
                                "description": "nested amount",
                                "precision": "20",
                                "scale": "4"
                            },
                            {
                                "name": "detail",
                                "type": "RECORD",
                                "mode": "NULLABLE",
                                "description": "old detail",
                                "policyTags": {"names": ["old-detail-tag"]},
                                "fields": [
                                    {
                                        "name": "sku",
                                        "type": "STRING",
                                        "mode": "NULLABLE",
                                        "description": "old sku"
                                    },
                                    {
                                        "name": "note",
                                        "type": "STRING",
                                        "mode": "NULLABLE",
                                        "description": "old note"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "Customer",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [{
                            "name": "Email",
                            "type": "STRING",
                            "mode": "NULLABLE",
                            "description": "old email"
                        }]
                    }
                ]
            }
        })),
        ok(json!({})),
    ]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query(
            "-- adapter.update_columns via BigQuery REST API on `data-project.docs.model_array`",
        )
        .unwrap();
    set_option(
        &mut statement,
        Key::DestinationTable,
        "data-project.docs.model_array",
    );
    set_option(
        &mut statement,
        Key::UpdateColumnsDescription,
        r#"{"id":"Required identifier.","missing":"ignored","my_array":"Documented array.","my_array.amount":"Updated nested amount","my_array.detail.sku":"Deeply nested sku.","my_array.detail.note":"","my_array.absent":"ignored","customer.email":"Case-insensitively matched."}"#,
    );
    set_option(
        &mut statement,
        Key::UpdateColumnsPolicyTags,
        r#"{"id":["new-id-tag"],"my_array":["must-not-replace-record-tag"],"my_array.amount":["new-nested-tag"],"my_array.detail":["must-not-replace-nested-record-tag"],"my_array.detail.sku":["new-deep-tag"],"CUSTOMER.email":["new-case-tag"]}"#,
    );

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, 0);
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/data-project/datasets/docs/tables/model_array?view=BASIC&fields=schema"
    );
    assert!(requests[1].line.starts_with("PATCH "));
    assert_eq!(
        requests[1].path(),
        "/bigquery/v2/projects/data-project/datasets/docs/tables/model_array?fields=etag"
    );

    let fields = requests[1].body["schema"]["fields"]
        .as_array()
        .expect("the patch carries a full schema");
    assert_eq!(fields[0]["description"], "Required identifier.");
    assert_eq!(fields[0]["mode"], "REQUIRED");
    assert_eq!(fields[0]["policyTags"]["names"], json!(["new-id-tag"]));
    assert_eq!(fields[1]["description"], "Documented array.");
    assert_eq!(fields[1]["mode"], "REPEATED");
    assert_eq!(
        fields[1]["policyTags"]["names"],
        json!(["old-record-tag"]),
        "RECORD policy tags are preserved"
    );
    let nested = fields[1]["fields"]
        .as_array()
        .expect("the REPEATED RECORD keeps its nested schema");
    assert_eq!(nested.len(), 2, "an unknown dotted path adds no field");
    assert_eq!(nested[0]["name"], "amount", "field order is preserved");
    assert_eq!(nested[0]["description"], "Updated nested amount");
    assert_eq!(nested[0]["policyTags"]["names"], json!(["new-nested-tag"]));
    assert_eq!(nested[0]["mode"], "REQUIRED");
    assert_eq!(nested[0]["precision"], "20");
    assert_eq!(nested[0]["scale"], "4");
    assert_eq!(
        nested[1]["description"], "old detail",
        "an unnamed nested field keeps its metadata"
    );
    assert_eq!(
        nested[1]["policyTags"]["names"],
        json!(["old-detail-tag"]),
        "a nested RECORD is no more taggable than a top-level one"
    );
    let deep = &nested[1]["fields"];
    assert_eq!(deep[0]["description"], "Deeply nested sku.");
    assert_eq!(deep[0]["policyTags"]["names"], json!(["new-deep-tag"]));
    assert_eq!(
        deep[1].get("description"),
        None,
        "an empty description clears the old value by omitting it from the full schema patch"
    );
    let stored_case = &fields[2]["fields"][0];
    assert_eq!(
        stored_case["description"], "Case-insensitively matched.",
        "a dbt path resolves against the casing BigQuery stored, as BigQuery itself resolves it"
    );
    assert_eq!(stored_case["policyTags"]["names"], json!(["new-case-tag"]));
    assert_eq!(
        fields[2]["name"], "Customer",
        "the stored casing is left alone"
    );
    assert_eq!(stored_case["name"], "Email");
    assert_eq!(requests[1].body.get("etag"), None);
    assert_eq!(requests[1].body.get("description"), None);
}

/// Payload paths name the column BigQuery would resolve them to, under its full-uppercase
/// comparison: sibling columns it keeps distinct get their own descriptions, and a spelling it
/// folds onto a column describes that column.
#[test]
fn flexible_column_names_match_the_way_bigquery_folds_their_case() {
    // (column, the payload path naming it)
    let cases = [
        ("\u{df}", "SS"),         // ß, which BigQuery treats as SS
        ("\u{1e9e}", "\u{1e9e}"), // ẞ, distinct from ß
        ("k", "K"),               // Latin k
        ("\u{212a}", "\u{212a}"), // the Kelvin sign, distinct from k
        ("\u{131}", "I"),         // dotless ı, which BigQuery treats as I
        (
            "\u{39f}\u{394}\u{39f}\u{3a3}",
            "\u{3bf}\u{3b4}\u{3bf}\u{3c2}",
        ), // ΟΔΟΣ named with a final sigma
    ];
    let mut schema = TableSchema {
        fields: cases
            .iter()
            .map(|(column, _)| TableFieldSchema {
                name: (*column).to_owned(),
                r#type: "STRING".to_owned(),
                ..TableFieldSchema::default()
            })
            .collect(),
        ..TableSchema::default()
    };
    let descriptions: BTreeMap<String, String> = cases
        .iter()
        .map(|(column, path)| ((*path).to_owned(), format!("Column {column}.")))
        .collect();
    super::apply_column_metadata(&mut schema, Some(&descriptions), None);
    for (field, (column, _)) in schema.fields.iter().zip(cases) {
        assert_eq!(
            field.description.as_deref(),
            Some(format!("Column {column}.").as_str()),
            "{column:?}"
        );
    }
}

/// Both table metadata updates, which one statement runs columns first.
const BOTH_UPDATES: [(Key, &str); 2] = [
    (
        Key::UpdateColumnsDescription,
        r#"{"id":"Required identifier."}"#,
    ),
    (Key::UpdateDescription, "A documented relation."),
];

/// A statement against `endpoint` that updates `data-project.docs.documented_model` with
/// `updates`.
fn documented_model_update(endpoint: &str, updates: &[(Key, &str)]) -> BigQueryStatement {
    let mut statement = test_statement_at(endpoint);
    set_option(
        &mut statement,
        Key::DestinationTable,
        "data-project.docs.documented_model",
    );
    for &(option, value) in updates {
        set_option(&mut statement, option, value);
    }
    statement
}

/// The patch sets one scalar field, so an `If-Match` precondition would protect nothing a
/// concurrent writer set. It would also fail spuriously: a materialized view's automatic initial
/// refresh changes its ETag a second or two after creation, while ETag reads briefly still return
/// the old one, so an update sent in that window lost the precondition (HTTP 412).
#[test]
fn dbt_table_description_update_patches_only_the_description_without_a_precondition() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({}))]);
    let mut statement = documented_model_update(
        &endpoint,
        &[(Key::UpdateDescription, "A documented relation.")],
    );
    // dbt 2.0's `execute_with_options(sql = "", ...)` behind its query comment.
    statement
        .set_sql_query("/* bigquery-adbc: model.project.documented_model */\n")
        .unwrap();

    let (mut reader, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, 0);
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    let requests = captured.join().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "one table patch, with no ETag read before it"
    );
    assert!(
        requests[0].line.starts_with("PATCH "),
        "{}",
        requests[0].line
    );
    assert_eq!(
        requests[0].path(),
        "/bigquery/v2/projects/data-project/datasets/docs/tables/documented_model?fields=etag"
    );
    assert_eq!(
        requests[0].header("If-Match"),
        None,
        "a description-only patch is unconditional, so it can never lose a precondition"
    );
    assert_eq!(
        requests[0].body,
        json!({"description": "A documented relation."}),
        "only the description is patched, leaving the schema untouched"
    );
}

/// Both patches are blind, so replaying the same bytes after a 503 that may have applied leaves
/// the same table state (G82), and a rate-limit rejection (BigQuery answers more than about five
/// metadata updates per table in ten seconds with `rateLimitExceeded`) applied nothing. The
/// schema is read once.
#[test]
fn failed_table_patches_are_resent_with_the_same_body() {
    for failure in [
        (
            403,
            error_body(
                "rateLimitExceeded",
                "Exceeded rate limits: too many table updates.",
            ),
        ),
        (429, error_body("rateLimitExceeded", "Too many requests.")),
        (503, error_body("backendError", "Backend error.")),
    ] {
        let (endpoint, captured) = capture_requests(vec![
            ok(json!({"schema": {"fields": [{"name": "id", "type": "INTEGER"}]}})),
            failure.clone(),
            ok(json!({})),
            failure,
            ok(json!({})),
        ]);
        let mut statement = documented_model_update(&endpoint, &BOTH_UPDATES);

        assert_eq!(statement.execute_update().unwrap(), Some(0));

        let requests = captured.served(5);
        assert!(requests[0].line.starts_with("GET "), "{}", requests[0].line);
        for patch in &requests[1..] {
            assert!(patch.line.starts_with("PATCH "), "{}", patch.line);
            assert_eq!(patch.header("If-Match"), None);
        }
        assert_eq!(
            requests[1].body["schema"]["fields"][0]["description"],
            "Required identifier."
        );
        assert_eq!(requests[1].raw_body, requests[2].raw_body);
        assert_eq!(
            requests[3].body,
            json!({"description": "A documented relation."})
        );
        assert_eq!(requests[3].raw_body, requests[4].raw_body);
    }
}

/// A patch that never got an answer is named once, by the operation, and not again by the
/// request beneath it.
#[test]
fn a_table_patch_transport_failure_names_its_operation_once() {
    let attempts = usize::try_from(crate::rest::retry::MAX_ATTEMPTS).unwrap();
    for (update, schema_read, context) in [
        (
            (Key::UpdateDescription, "A documented relation."),
            false,
            "failed to update the description of BigQuery table data-project.docs.documented_model",
        ),
        (
            (
                Key::UpdateColumnsDescription,
                r#"{"id":"Required identifier."}"#,
            ),
            true,
            "failed to update BigQuery column metadata for data-project.docs.documented_model",
        ),
    ] {
        let mut served = 0;
        let (endpoint, captured) =
            capture_requests_responding(attempts + usize::from(schema_read), move |_| {
                served += 1;
                // The schema read is answered; every patch is read whole and its connection
                // closed without an answer.
                (schema_read && served == 1)
                    .then(|| ok(json!({"schema": {"fields": [{"name": "id", "type": "INTEGER"}]}})))
            });
        let mut statement = documented_model_update(&endpoint, &[update]);

        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::IO);
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
        assert_eq!(
            captured.join().unwrap().len(),
            attempts + usize::from(schema_read)
        );
    }
}

#[test]
fn table_metadata_updates_require_a_destination() {
    for (update, value) in BOTH_UPDATES {
        let mut statement = test_statement();
        set_option(&mut statement, update, value);
        let error = statement.execute_update().unwrap_err();
        assert_failure(
            &error,
            Status::InvalidState,
            &[Key::DestinationTable.as_str()],
        );
    }
}
