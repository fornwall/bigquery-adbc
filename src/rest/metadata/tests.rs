use adbc_core::error::Status;
use std::collections::BTreeMap;

use super::{
    Dataset, DatasetId, ListedTable, MetadataClient, Page, Table, TableId, TableView, listing_query,
};
use crate::test_support::errors::assert_message_omits;

fn client() -> MetadataClient {
    client_at(crate::test_support::UNREACHABLE_ENDPOINT)
}

/// An anonymous client on `endpoint`, with the test build's 1 ms retry delays.
fn client_at(endpoint: &str) -> MetadataClient {
    // Building any reqwest client needs the rustls provider the driver runtime installs.
    crate::runtime::install_crypto_provider();
    MetadataClient::new(
        google_cloud_auth::credentials::anonymous::Builder::new().build(),
        Some(endpoint),
        crate::rest::client::rest_client("metadata test").unwrap(),
    )
}

#[test]
fn interrupted_metadata_retries_keep_diagnostics_and_the_interruption_status() {
    for status in [Status::Cancelled, Status::Timeout] {
        let interruption = crate::error::error("retry interrupted", status);
        let sqlstate = interruption.sqlstate;
        let failure = super::MetadataError::Rejected(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            bytes::Bytes::from_static(br#"{"error":{"errors":[{"reason":"backendError","message":"backend unavailable","location":"table"}],"private":"do not expose"}}"#),
        ).interrupted(interruption).into_error("get objects");
        assert_eq!(failure.status, status);
        assert_eq!(failure.sqlstate, sqlstate);
        assert_eq!(failure.vendor_code, 503);
        assert_eq!(crate::error::failure_reason(&failure), Some("backendError"));
        assert_eq!(
            failure.message,
            "get objects: retry interrupted; last metadata request failed: backendError: backend unavailable (field: table) (HTTP 503)"
        );
        assert_message_omits(&failure, &["do not expose"]);
        let details = failure.details.unwrap();
        assert!(details.contains(&(crate::error::DETAIL_LOCATION.to_owned(), b"table".to_vec())));
        assert!(details.contains(&(crate::error::DETAIL_HTTP_STATUS.to_owned(), b"503".to_vec())));
    }
}

/// Every field `get_table_schema`, `get_objects`, and `get_statistics` report, decoded from the
/// spellings BigQuery sends (int64s as JSON strings).
#[test]
fn table_metadata_decodes_every_field_the_driver_reports() {
    let table = Table::decode(
        br#"{
            "kind": "bigquery#table",
            "etag": "table-etag",
            "type": "MATERIALIZED_VIEW",
            "description": "test table",
            "labels": {"env": "dev"},
            "resourceTags": {"tagKeys/1": "tagValues/2"},
            "clustering": {"fields": ["id"]},
            "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
            "timePartitioning": {"type": "DAY", "field": "event_date"},
            "rangePartitioning": {"field": "bucket", "range": {"start": "0", "end": "9"}},
            "materializedView": {"enableRefresh": true, "refreshIntervalMs": "1800000"},
            "encryptionConfiguration": {"kmsKeyName": "projects/p/cryptoKeys/k"},
            "tableConstraints": {"primaryKey": {"columns": ["id"]}},
            "maxStaleness": "0-0 0 8:0:0",
            "expirationTime": "1767225600000",
            "lastModifiedTime": "1756382400123",
            "numRows": "42",
            "numBytes": "1024",
            "numPhysicalBytes": "2048",
            "numLongTermBytes": "8",
            "numTotalLogicalBytes": "4096",
            "numActiveLogicalBytes": "512",
            "numLongTermLogicalBytes": "256",
            "numTotalPhysicalBytes": "8192",
            "numTimeTravelPhysicalBytes": "64",
            "numPartitions": "3",
            "aFieldBigQueryAddedLater": {"nested": ["anything"]}
        }"#,
    )
    .unwrap();

    assert_eq!(table.table_type, "MATERIALIZED_VIEW");
    assert_eq!(table.description, "test table");
    assert_eq!(table.labels.get("env").map(String::as_str), Some("dev"));
    assert_eq!(
        table.resource_tags.get("tagKeys/1").map(String::as_str),
        Some("tagValues/2")
    );
    assert_eq!(
        table.clustering.map(|clustering| clustering.fields),
        Some(vec!["id".to_owned()])
    );
    assert_eq!(
        table.schema.map(|schema| schema.fields.len()),
        Some(1),
        "the generated TableSchema keeps the type mapping the driver converts columns with"
    );
    let time_partitioning = table.time_partitioning.unwrap();
    assert_eq!(time_partitioning.r#type, "DAY");
    assert_eq!(time_partitioning.field.as_deref(), Some("event_date"));
    let range = table.range_partitioning.unwrap().range.unwrap();
    assert_eq!((range.start.as_str(), range.end.as_str()), ("0", "9"));
    let materialized_view = table.materialized_view.unwrap();
    assert_eq!(materialized_view.enable_refresh, Some(true));
    assert_eq!(materialized_view.refresh_interval_ms, Some(1_800_000));
    assert_eq!(
        table
            .encryption_configuration
            .and_then(|configuration| configuration.kms_key_name)
            .as_deref(),
        Some("projects/p/cryptoKeys/k")
    );
    assert_eq!(
        table
            .table_constraints
            .and_then(|constraints| constraints.primary_key)
            .map(|key| key.columns),
        Some(vec!["id".to_owned()])
    );
    assert_eq!(table.max_staleness, "0-0 0 8:0:0");
    assert_eq!(table.expiration_time, Some(1_767_225_600_000));
    assert_eq!(table.last_modified_time, 1_756_382_400_123);
    assert_eq!(table.num_rows, Some(42));
    assert_eq!(table.num_bytes, Some(1_024));
    assert_eq!(table.num_physical_bytes, Some(2_048));
    assert_eq!(table.num_long_term_bytes, Some(8));
    assert_eq!(table.num_total_logical_bytes, Some(4_096));
    assert_eq!(table.num_active_logical_bytes, Some(512));
    assert_eq!(table.num_long_term_logical_bytes, Some(256));
    assert_eq!(table.num_total_physical_bytes, Some(8_192));
    assert_eq!(table.num_time_travel_physical_bytes, Some(64));
    assert_eq!(table.num_partitions, Some(3));
}

/// A table described by its type alone (a view, a new table) decodes rather than failing.
#[test]
fn an_almost_empty_table_decodes_to_absent_fields() {
    let table = Table::decode(br#"{"type": "VIEW"}"#).unwrap();
    assert_eq!(table.table_type, "VIEW");
    assert!(table.schema.is_none());
    assert_eq!(table.last_modified_time, 0);
    assert_eq!(table.num_rows, None);
    assert_eq!(table.num_total_logical_bytes, None);
}

#[test]
fn malformed_metadata_scalars_are_not_silently_discarded() {
    for body in [
        br#"{"type": false}"#.as_slice(),
        br#"{"description": []}"#.as_slice(),
        br#"{"maxStaleness": {}}"#.as_slice(),
        br#"{"requirePartitionFilter": "false"}"#.as_slice(),
    ] {
        assert_eq!(Table::decode(body).unwrap_err().status, Status::InvalidData);
    }
    assert_eq!(
        Dataset::decode(br#"{"etag": false}"#).unwrap_err().status,
        Status::InvalidData
    );
    for required in [false, true] {
        let body = format!(r#"{{"requirePartitionFilter": {required}}}"#);
        assert_eq!(
            Table::decode(body.as_bytes())
                .unwrap()
                .require_partition_filter,
            Some(required)
        );
    }
}

#[test]
fn a_malformed_continuation_token_does_not_end_a_listing() {
    for token in ["false", "42", "[]", "{}"] {
        let body = format!(r#"{{"datasets": [], "nextPageToken": {token}}}"#);
        let failure = Page::<String>::decode(
            body.as_bytes(),
            "BigQuery dataset listing",
            "datasets",
            |_| Ok(String::new()),
        )
        .err()
        .expect("a malformed continuation token must not truncate the listing");
        assert_eq!(failure.status, Status::InvalidData);
    }
}

/// Integer fields decode from strings or numbers; anything else is a malformed document, not an
/// absent statistic.
#[test]
fn integers_decode_from_both_json_spellings_and_reject_anything_else() {
    let table = Table::decode(br#"{"numRows": 42, "numBytes": 1024}"#).unwrap();
    assert_eq!(table.num_rows, Some(42));
    assert_eq!(table.num_bytes, Some(1_024));

    for body in [
        br#"{"numRows": "not a number"}"#.as_slice(),
        br#"{"numRows": true}"#.as_slice(),
        br#"{"numRows": -1}"#.as_slice(),
        br#"{"numBytes": "12.5"}"#.as_slice(),
    ] {
        let failure = Table::decode(body).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData, "{failure:?}");
        assert!(failure.message.contains("non-integer"), "{failure:?}");
    }
}

/// An explicit `null` reads as absent for a scalar, a nested document, and a listing array.
#[test]
fn an_explicit_null_reads_as_an_absent_field() {
    let table = Table::decode(
        br#"{"type": null, "schema": null, "numRows": null,
            "labels": null, "requirePartitionFilter": null}"#,
    )
    .unwrap();
    assert!(table.table_type.is_empty());
    assert!(table.schema.is_none());
    assert_eq!(table.num_rows, None);
    assert!(table.labels.is_empty());
    assert_eq!(table.require_partition_filter, None);

    let page = Page::<String>::decode(
        br#"{"datasets": null, "nextPageToken": null}"#,
        "BigQuery dataset listing",
        "datasets",
        |_| Ok(String::new()),
    )
    .unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_page_token.is_empty());
}

/// Field names are borrowed from the body, but one spelled with a JSON escape is the same field.
#[test]
fn an_escaped_field_name_reads_as_the_field_it_spells() {
    let table = Table::decode(br#"{"typ\u0065": "VIEW", "numRows": "7"}"#).unwrap();
    assert_eq!(table.table_type, "VIEW");
    assert_eq!(table.num_rows, Some(7));

    let document = super::Document::decode(br#"{"plain": 1, "\u0065scaped": 2}"#, "it").unwrap();
    let borrowed = |name: &str| {
        let (key, _) = document.fields.get_key_value(name).unwrap();
        matches!(key.0, std::borrow::Cow::Borrowed(_))
    };
    assert!(borrowed("plain"));
    assert!(!borrowed("escaped"));
}

/// A listing whose entry array is a value of some other type is malformed rather than empty:
/// silently reporting no tables would look like an empty dataset.
#[test]
fn a_listing_whose_entries_are_not_an_array_is_rejected() {
    let Err(failure) = Page::<String>::decode(
        br#"{"datasets": {"first": {}}}"#,
        "BigQuery dataset listing",
        "datasets",
        |_| Ok(String::new()),
    ) else {
        panic!("a listing whose entries are not an array must not decode");
    };
    assert_eq!(failure.status, Status::InvalidData);
    assert!(failure.message.contains("is not an array"), "{failure:?}");
}

#[test]
fn a_response_that_is_not_a_json_object_is_rejected() {
    for body in [b"[]".as_slice(), b"7".as_slice(), b"not json".as_slice()] {
        assert_eq!(Table::decode(body).unwrap_err().status, Status::InvalidData);
    }
}

/// The access list is read-modify-written, so an entry BigQuery describes with a field this
/// driver has never heard of must survive the patch that adds an authorized view beside it.
#[test]
fn a_dataset_access_entry_round_trips_the_fields_the_driver_does_not_know() {
    let dataset = Dataset::decode(
        br#"{
            "etag": "dataset-etag",
            "kind": "bigquery#dataset",
            "access": [
                {"role": "READER", "userByEmail": "a@example.com", "futureGrant": {"x": 1}}
            ]
        }"#,
    )
    .unwrap();
    assert_eq!(dataset.etag, "dataset-etag");
    assert_eq!(dataset.access.len(), 1);

    let patched = serde_json::to_string(&serde_json::json!({"access": dataset.access})).unwrap();
    assert!(patched.contains(r#""role":"READER""#), "{patched}");
    assert!(
        patched.contains(r#""userByEmail":"a@example.com""#),
        "{patched}"
    );
    assert!(
        patched.contains(r#""futureGrant":{"x":1}"#),
        "a blind rewrite of the access list must not drop an entry's unknown fields: {patched}"
    );
}

#[test]
fn a_listing_page_decodes_its_entries_and_its_continuation() {
    let page = Page::<String>::decode(
        br#"{
            "kind": "bigquery#datasetList",
            "nextPageToken": "page-2",
            "datasets": [
                {"datasetReference": {"projectId": "p", "datasetId": "first"}},
                {"id": "p:second"}
            ]
        }"#,
        "BigQuery dataset listing",
        "datasets",
        |entry| {
            Ok(entry
                .value::<crate::rest::model::DatasetReference>("datasetReference")?
                .map(|reference| reference.dataset_id)
                .unwrap_or_default())
        },
    )
    .unwrap();
    assert_eq!(page.items, ["first".to_owned(), String::new()]);
    assert_eq!(page.next_page_token, "page-2");

    // The last page of an empty project carries neither an array nor a token.
    let page = Page::<String>::decode(b"{}", "BigQuery dataset listing", "datasets", |_| {
        Ok(String::new())
    })
    .unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_page_token.is_empty());

    let page = Page::<ListedTable>::decode(
        br#"{"tables": [{"tableReference": {"datasetId": "d", "tableId": "events"}, "type": "VIEW"}]}"#,
        "BigQuery table listing",
        "tables",
        |entry| {
            Ok(ListedTable {
                reference: entry.value("tableReference")?,
                table_type: entry.value_or_default("type")?,
            })
        },
    )
    .unwrap();
    let reference = page.items[0].reference.as_ref().unwrap();
    assert_eq!(
        (reference.dataset_id.as_str(), reference.table_id.as_str()),
        ("d", "events")
    );
    assert_eq!(page.items[0].table_type, "VIEW");
}

/// `tables.get` and `datasets.get` name the object BigQuery resolved the request to, which an
/// exact metadata filter reports; an answer without the reference names nothing.
#[test]
fn lookups_decode_the_names_bigquery_resolved() {
    let table = Table::decode(
        br#"{"tableReference": {"projectId": "p", "datasetId": "d", "tableId": "events"}, "type": "TABLE"}"#,
    )
    .unwrap();
    let reference = table.table_reference.unwrap();
    assert_eq!(
        (reference.dataset_id.as_str(), reference.table_id.as_str()),
        ("d", "events")
    );
    assert!(Table::decode(b"{}").unwrap().table_reference.is_none());

    let runtime = crate::runtime::new_runtime().unwrap();
    for (response, expected) in [
        (
            serde_json::json!({"datasetReference": {"projectId": "p", "datasetId": "d"}}),
            "d",
        ),
        (serde_json::json!({}), ""),
    ] {
        let (endpoint, captured) =
            crate::test_support::http::capture_requests(vec![(200, response)]);
        let client = client_at(&endpoint);
        let dataset = DatasetId {
            project_id: "p",
            dataset_id: "D",
        };
        let resolved = runtime
            .block_on(client.get_dataset(None, dataset))
            .ok()
            .unwrap();
        assert_eq!(resolved, expected);
        assert_eq!(
            captured.join().unwrap()[0].path(),
            "/bigquery/v2/projects/p/datasets/D?fields=datasetReference%28datasetId%29"
        );
    }
}

/// Every dataset has a location, so a response without one is malformed, not a dataset that runs
/// jobs anywhere.
#[test]
fn a_dataset_location_lookup_requires_a_location() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (response, expected) in [
        (
            serde_json::json!({"location": "asia-northeast1"}),
            Ok("asia-northeast1"),
        ),
        (serde_json::json!({}), Err(())),
        (serde_json::json!({"location": ""}), Err(())),
    ] {
        let (endpoint, captured) =
            crate::test_support::http::capture_requests(vec![(200, response)]);
        let client = client_at(&endpoint);
        let dataset = DatasetId {
            project_id: "p",
            dataset_id: "d",
        };
        let location = runtime.block_on(client.get_dataset_location(None, dataset));
        captured.join().unwrap();
        match (location, expected) {
            (Ok(location), Ok(expected)) => assert_eq!(location, expected),
            (Err(failure), Err(())) => {
                let failure = failure.into_error("context");
                assert_eq!(failure.status, Status::InvalidData);
                assert_eq!(
                    failure.message,
                    "context: BigQuery returned dataset metadata without a location"
                );
            }
            (location, _) => panic!("unexpected lookup result: {:?}", location.ok()),
        }
    }
}

/// `get_table_schema` takes its table name from the caller, so a name carrying a slash or a
/// query delimiter must stay one path segment rather than reshaping the request.
#[test]
fn urls_keep_a_caller_supplied_name_inside_its_own_path_segment() {
    let client = client();
    assert_eq!(
        client.table_url(TableId {
            project_id: "test-project",
            dataset_id: "dataset",
            table_id: "../../evil?view=FULL#",
        }),
        "http://127.0.0.1:1/bigquery/v2/projects/test-project/datasets/dataset/tables/\
         ..%2F..%2Fevil%3Fview%3DFULL%23"
    );
}

#[test]
fn listing_queries_carry_only_the_parameters_they_have() {
    assert_eq!(listing_query(None, ""), "");
    assert_eq!(listing_query(Some(1), ""), "?maxResults=1");
    assert_eq!(listing_query(None, "next/page"), "?pageToken=next%2Fpage");
    assert_eq!(
        listing_query(Some(1), "next page"),
        "?maxResults=1&pageToken=next%20page"
    );
}

/// Every metadata request selects only the fields its caller consumes, and the percent-encoded
/// selector parses back as one query value.
#[test]
fn metadata_urls_project_every_consumed_response_field() {
    let client = client();
    let dataset = DatasetId {
        project_id: "test-project",
        dataset_id: "dataset",
    };
    let table = TableId {
        project_id: "test-project",
        dataset_id: "dataset",
        table_id: "events",
    };
    let table_path = "/bigquery/v2/projects/test-project/datasets/dataset/tables/events";
    let dataset_path = "/bigquery/v2/projects/test-project/datasets/dataset";

    // Every view carries the BigQuery view it asks for.
    for (view, fields, view_parameter) in [
        // The views a metadata listing describes with carry the resolved names.
        (
            TableView::Type,
            "tableReference(datasetId,tableId),type",
            "BASIC",
        ),
        (TableView::Schema, "schema", "BASIC"),
        (
            TableView::Constraints,
            "tableConstraints,tableReference(datasetId,tableId),type",
            "BASIC",
        ),
        (
            TableView::Basic,
            "schema,tableConstraints,tableReference(datasetId,tableId),type",
            "BASIC",
        ),
        (
            TableView::StorageStats,
            "lastModifiedTime,numActiveLogicalBytes,numBytes,numLongTermBytes,\
             numLongTermLogicalBytes,numPartitions,numPhysicalBytes,numRows,\
             numTimeTravelPhysicalBytes,numTotalLogicalBytes,numTotalPhysicalBytes,\
             streamingBuffer,tableReference(datasetId,tableId),type",
            "STORAGE_STATS",
        ),
        (
            TableView::Configuration,
            "clustering,description,encryptionConfiguration,expirationTime,\
             externalDataConfiguration,labels,materializedView,maxStaleness,\
             rangePartitioning,requirePartitionFilter,resourceTags,schema,timePartitioning,\
             type,view",
            "BASIC",
        ),
    ] {
        assert_url(
            &client.table_get_url(table, view),
            table_path,
            &[("fields", fields), ("view", view_parameter)],
        );
    }
    assert_url(
        &client.table_insert_url(table),
        "/bigquery/v2/projects/test-project/datasets/dataset/tables",
        &[("fields", "etag")],
    );
    assert_url(
        &client.table_update_url(table),
        table_path,
        &[("fields", "etag")],
    );

    assert_url(
        &client.dataset_list_url("test-project", Some(1), "next/page"),
        "/bigquery/v2/projects/test-project/datasets",
        &[
            ("all", "true"),
            (
                "fields",
                "datasets(datasetReference(datasetId)),nextPageToken",
            ),
            ("maxResults", "1"),
            ("pageToken", "next/page"),
        ],
    );
    assert_url(
        &client.table_list_url(dataset, "next/page"),
        "/bigquery/v2/projects/test-project/datasets/dataset/tables",
        &[
            (
                "fields",
                "nextPageToken,tables(tableReference(datasetId,tableId),type)",
            ),
            ("maxResults", "1000"),
            ("pageToken", "next/page"),
        ],
    );
    assert_url(
        &client.dataset_get_url(dataset),
        dataset_path,
        &[("fields", "datasetReference(datasetId)")],
    );
    assert_url(
        &client.dataset_access_get_url(dataset),
        dataset_path,
        &[("fields", "access,etag")],
    );
    assert_url(
        &client.dataset_location_get_url(dataset),
        dataset_path,
        &[("fields", "location")],
    );
    assert_url(
        &client.dataset_access_update_url(dataset),
        dataset_path,
        &[("fields", "etag")],
    );
}

fn assert_url(url: &str, expected_path: &str, expected_query: &[(&str, &str)]) {
    let parsed = reqwest::Url::parse(url).unwrap();
    assert_eq!(parsed.path(), expected_path);
    let query = parsed
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<BTreeMap<_, _>>();
    let expected = expected_query
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(query, expected, "{url}");
}

/// A metadata read retries a 5xx or quota failure up to the attempt budget, and returns a verdict
/// on the resource at once.
#[test]
fn a_metadata_read_retries_transient_failures_but_not_a_verdict() {
    use crate::test_support::http::{capture_requests, error_body};
    use serde_json::json;

    let rejection = |code: u16, reason: &str| (code, error_body(reason, "refused"));
    let budget = usize::try_from(crate::rest::retry::MAX_ATTEMPTS).unwrap();
    for (responses, expected) in [
        (
            vec![
                rejection(503, "backendError"),
                rejection(403, "rateLimitExceeded"),
                (200, json!({})),
            ],
            None,
        ),
        (vec![rejection(404, "notFound")], Some(Status::NotFound)),
        (
            vec![rejection(403, "accessDenied")],
            Some(Status::Unauthorized),
        ),
        (
            vec![rejection(503, "backendError"); budget],
            Some(Status::IO),
        ),
    ] {
        let runtime = crate::runtime::new_runtime().unwrap();
        let requests = responses.len();
        let (endpoint, captured) = capture_requests(responses);
        let client = client_at(&endpoint);
        let dataset = DatasetId {
            project_id: "p",
            dataset_id: "d",
        };
        let status = runtime
            .block_on(client.get_dataset(None, dataset))
            .err()
            .map(|failure| failure.into_error("failed to check p.d").status);
        assert_eq!(status, expected);
        assert_eq!(captured.join().unwrap().len(), requests, "{expected:?}");
    }
}

/// A request that never reached an answer is named by the caller's context, and keeps the
/// status, details and vendor code it was already classified with.
#[test]
fn a_failed_read_keeps_its_classification_under_the_callers_context() {
    let mut failure = crate::error::error("refused", Status::IO);
    crate::error::record_http_status(&mut failure, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let converted =
        super::MetadataError::Failed(failure).into_error("failed to check ingest target p.d.t");
    assert_eq!(
        converted.message,
        "failed to check ingest target p.d.t: refused"
    );
    assert_eq!(converted.status, Status::IO);
    assert_eq!(converted.vendor_code, 503);
    assert_eq!(
        converted.details,
        Some(vec![(
            crate::error::DETAIL_HTTP_STATUS.to_owned(),
            b"503".to_vec()
        )])
    );
}

/// A blind patch waiting out its backoff after a 503 stops as soon as its operation is cancelled.
/// The hour-long backoff would otherwise outlast the test's wait.
#[test]
fn cancelling_a_blind_patch_during_its_backoff_returns_cancelled() {
    use std::sync::mpsc;
    use std::time::Duration;

    use adbc_core::CancelHandle as _;

    use crate::cancellation::{Cancellation, OperationCancelHandle};
    use crate::test_support::http::{capture_requests, error_body};

    let (endpoint, captured) =
        capture_requests(vec![(503, error_body("backendError", "Backend error."))]);
    let runtime = crate::runtime::new_runtime().unwrap();
    let client = client_at(&endpoint).with_flat_delays(Duration::from_secs(3600));
    let cancellation = Cancellation::new(
        runtime.clone(),
        crate::clients::ClientState::for_test(),
        "statement",
    );
    let operation = cancellation.begin();
    let (finished, result) = mpsc::channel();
    let patch_operation = operation.clone();
    let patch = std::thread::spawn(move || {
        let table = TableId {
            project_id: "p",
            dataset_id: "d",
            table_id: "t",
        };
        let outcome = runtime.block_on(client.patch_table_description(
            Some(&patch_operation),
            table,
            "A documented relation.",
        ));
        finished
            .send(
                outcome
                    .err()
                    .map(|failure| failure.into_error("update p.d.t")),
            )
            .unwrap();
    });

    // The mock has written the 503; give the driver a moment to read it and start its backoff.
    assert_eq!(captured.join().unwrap().len(), 1);
    std::thread::sleep(Duration::from_millis(100));
    OperationCancelHandle(cancellation).try_cancel().unwrap();

    let failure = result
        .recv_timeout(Duration::from_secs(30))
        .expect("cancellation must end the backoff")
        .expect("a cancelled patch must not succeed");
    assert_eq!(failure.status, Status::Cancelled, "{failure:?}");
    patch.join().unwrap();
    operation.finish();
}

/// A read that never got an answer is named once, by the caller's context, and not again by the
/// request beneath it.
#[test]
fn an_unanswered_read_is_named_only_by_the_callers_context() {
    use crate::test_support::http::capture_requests_responding;

    let attempts = usize::try_from(crate::rest::retry::MAX_ATTEMPTS).unwrap();
    // Every request is read whole and its connection closed without an answer.
    let (endpoint, captured) = capture_requests_responding(attempts, |_| None);
    let runtime = crate::runtime::new_runtime().unwrap();
    let client = client_at(&endpoint);
    let dataset = DatasetId {
        project_id: "p",
        dataset_id: "d",
    };
    let failure = runtime
        .block_on(client.get_dataset(None, dataset))
        .expect_err("an unanswered read must fail")
        .into_error("failed to check p.d");
    assert_eq!(failure.status, Status::IO);
    assert!(
        failure
            .message
            .starts_with("failed to check p.d: error sending request"),
        "{}",
        failure.message
    );
    assert_eq!(captured.join().unwrap().len(), attempts);
}

/// `tables.insert` is resent only after a rate-limit rejection, which applied nothing: a replay
/// after a 5xx might follow an unseen success and turn it into a 409.
#[test]
fn a_table_insert_is_resent_only_after_a_rate_limit() {
    use crate::test_support::http::{capture_requests, error_body};

    let table = TableId {
        project_id: "p",
        dataset_id: "d",
        table_id: "t",
    };
    let schema = crate::rest::model::TableSchema::default();
    for (responses, expected) in [
        (
            vec![
                (429, error_body("rateLimitExceeded", "slow down")),
                (403, error_body("rateLimitExceeded", "slow down")),
                (200, serde_json::json!({"etag": "e"})),
            ],
            None,
        ),
        (
            vec![(503, error_body("backendError", "unavailable"))],
            Some(Status::IO),
        ),
        (
            vec![(409, error_body("duplicate", "Already Exists: Table p:d.t"))],
            Some(Status::AlreadyExists),
        ),
    ] {
        let runtime = crate::runtime::new_runtime().unwrap();
        let requests = responses.len();
        let (endpoint, captured) = capture_requests(responses);
        let status = runtime
            .block_on(client_at(&endpoint).insert_table(None, table, &schema))
            .err()
            .map(|failure| failure.into_error("failed to create p.d.t").status);
        assert_eq!(status, expected);
        let captured = captured.join().unwrap();
        assert_eq!(captured.len(), requests, "{expected:?}");
        assert!(
            captured
                .iter()
                .all(|request| request.raw_body == captured[0].raw_body)
        );
    }
}
