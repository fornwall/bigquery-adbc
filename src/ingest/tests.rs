use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::CancelHandle as _;
use adbc_core::error::Status;
use adbc_core::options::IngestMode;
use arrow_array::{
    Array, ArrayRef, Date32Array, Int64Array, NullArray, RecordBatch, RecordBatchIterator,
    RecordBatchOptions, RecordBatchReader, StringArray, StructArray, new_empty_array,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use google_cloud_bigquery::model::row_error::RowErrorCode;
use google_cloud_bigquery::model::storage_error::StorageErrorCode;
use google_cloud_bigquery::model::{RowError, StorageError};
use serde_json::{Value as JsonValue, json};
use tokio::sync::Notify;

use super::schema::normalize_schema;
use super::{FirstBatch, parse_ingest_method, write_batches};
use crate::BigQueryBulkIngestMethod;
use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::{DatabaseConfig, ServiceEndpointKind};
use crate::error::{DETAIL_HTTP_STATUS, DETAIL_REASON};
use crate::identifiers::ProjectIdentifier;
use crate::ingest::{
    IngestContext, IngestOptions, TablePlan, execute_ingest, plan_table, prepare_table,
    recreate_table, truncate_table, validate_ingest,
};
use crate::rest::metadata::TableId;
use crate::rest::model::TableSchema;
use crate::runtime::SharedRuntime;
use crate::test_support::arrow::{bigquery_typed, range_bounds};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::grpc::{ABORTED, INVALID_ARGUMENT, MockPlan, StorageMock, UNAVAILABLE};
use crate::test_support::http::{
    CapturedRequest, capture_requests, capture_requests_gated, capture_requests_until_stopped,
    error_body, ok,
};
use crate::test_support::schema::{field_mode, table_schema};

/// Plans the target and prepares it as an execution does: a `create` reads nothing first.
pub(super) fn plan_and_prepare(
    context: &IngestContext<'_>,
    table: TableId<'_>,
    mode: IngestMode,
    schema: &TableSchema,
) -> adbc_core::error::Result<TablePlan> {
    let client = context.state.metadata_client(context.runtime)?;
    let plan = if mode == IngestMode::Create {
        TablePlan::Create
    } else {
        plan_table(context, &client, table, mode, schema)?
    };
    prepare_table(context, &client, table, mode, schema, plan)
}

/// Runs `run` against a mock BigQuery serving `responses`, and hands back what it captured.
fn against_bigquery<T>(
    responses: Vec<(u16, JsonValue)>,
    run: impl FnOnce(&IngestContext<'_>) -> T,
) -> (T, Vec<CapturedRequest>) {
    let (endpoint, captured) = capture_requests(responses);
    let runtime = crate::runtime::new_runtime().unwrap();
    let clients = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
    let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
    let operation = cancellation.begin();
    let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();

    let outcome = run(&IngestContext {
        runtime: &runtime,
        state: &clients,
        billing_project_id: Some(&billing_project),
        current_catalog: Some(&billing_project),
        current_schema: None,
        location: Some("EU"),
        operation: &operation,
    });
    operation.finish();
    (outcome, captured.join().unwrap())
}

const TARGET: TableId<'static> = TableId {
    project_id: "project",
    dataset_id: "dataset",
    table_id: "events",
};

/// One finished, empty `jobs.query` answer, which is what BigQuery returns for DDL.
fn ddl_answer() -> (u16, JsonValue) {
    (
        200,
        json!({
            "jobComplete": true,
            "jobReference": {"projectId": "billing-project", "jobId": "job-1"},
            "totalRows": "0",
            "schema": {"fields": []}
        }),
    )
}

/// Every statement query option that must not leak onto an ingest's own DDL job.
fn assert_carries_only_the_location(body: &JsonValue) {
    assert_eq!(body.get("location").and_then(JsonValue::as_str), Some("EU"));
    for leaked in [
        "destinationTable",
        "createDisposition",
        "writeDisposition",
        "maximumBytesBilled",
        "jobTimeoutMs",
        "priority",
        "createSession",
        "useQueryCache",
        "dryRun",
    ] {
        assert!(
            body.get(leaked).is_none(),
            "the ingest DDL request carried {leaked}: {body}"
        );
    }
}

/// The TRUNCATE that a REPLACE ingest runs must carry only the location. Every other
/// statement query option describes a user query: `destination_table` makes BigQuery
/// reject the DDL outright, `priority` can queue it behind a batch slot, and `create_session`
/// would leak a session per ingest.
#[test]
fn ingest_truncate_carries_only_the_location() {
    let (outcome, requests) = against_bigquery(vec![ddl_answer()], |context| {
        truncate_table(context, TARGET, Some(context.operation))
    });
    outcome.unwrap();

    let body = requests.into_iter().next().unwrap().body;
    assert_eq!(
        body.get("query").and_then(JsonValue::as_str),
        Some("TRUNCATE TABLE `project.dataset.events`")
    );
    assert_carries_only_the_location(&body);
}

/// Runs one whole ingest of what `reader` yields into [`TARGET`] in `mode`, against a
/// BigQuery mock serving up to `responses` and a Storage Write mock, and hands back the
/// outcome, the BigQuery requests sent, and the Storage Write mock.
fn ingest_against_mocks(
    responses: Vec<(u16, JsonValue)>,
    plan: MockPlan,
    mode: IngestMode,
    reader: Box<dyn RecordBatchReader + Send>,
) -> (
    adbc_core::error::Result<i64>,
    Vec<CapturedRequest>,
    StorageMock,
) {
    let (endpoint, server) = capture_requests_until_stopped(responses);
    let runtime = crate::runtime::new_runtime().unwrap();
    let storage = StorageMock::start(&runtime, plan);
    let mut config = DatabaseConfig {
        authentication_type: crate::BigQueryAuthenticationType::Anonymous,
        ..DatabaseConfig::default()
    };
    config
        .endpoints
        .set(ServiceEndpointKind::BigQuery, Some(Arc::from(endpoint)));
    config.endpoints.set(
        ServiceEndpointKind::Storage,
        Some(Arc::from(storage.endpoint())),
    );
    let clients = ClientState::new(&config);
    let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
    let operation = cancellation.begin();
    let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();
    let project = ProjectIdentifier::try_new(TARGET.project_id).unwrap();
    let outcome = execute_ingest(
        IngestContext {
            runtime: &runtime,
            state: &clients,
            billing_project_id: Some(&billing_project),
            current_catalog: Some(&project),
            current_schema: Some(TARGET.dataset_id),
            location: None,
            operation: &operation,
        },
        &IngestOptions {
            mode,
            target_table: Some(TARGET.table_id.to_owned()),
            ..IngestOptions::default()
        },
        reader,
    );
    operation.finish();
    (outcome, server.stop(), storage)
}

/// One batch holding `id` 1.
fn one_id_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap()
}

/// [`one_id_batch`], then a reader error.
fn fails_after_first_batch() -> Box<dyn RecordBatchReader + Send> {
    let batch = one_id_batch();
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(
        [
            Ok(batch),
            Err(ArrowError::ComputeError("the source broke".to_owned())),
        ],
        schema,
    ))
}

#[test]
fn a_failed_create_retains_the_table_and_reports_how_to_retry() {
    // The source fails after the first batch was written. A DROP job would receive a valid
    // answer too, so the request count detects accidental cleanup.
    let (outcome, requests, storage) = ingest_against_mocks(
        vec![(200, json!({})), ddl_answer()],
        MockPlan::default(),
        IngestMode::Create,
        fails_after_first_batch(),
    );
    let failure = outcome.unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidData,
        &[
            "the source broke",
            "committed no rows",
            "project.dataset.events",
            "adbc.ingest.mode.append",
            "drop the table and retry",
        ],
    );
    assert_eq!(requests.len(), 1);
    assert!(requests[0].path().contains("/datasets/dataset/tables"));
    assert_eq!(storage.finalized(), (Some(1), false));
}

/// A `replace` whose schema differs recreates the target before uploading, so a failure after
/// that, while uploading or at the commit, has to say the old table and its rows are gone.
#[test]
fn a_replace_that_fails_after_recreating_the_target_says_its_old_rows_are_gone() {
    let differing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "other", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let refused_commit = MockPlan {
        commit_errors: Some(vec![
            StorageError::new()
                .set_code(StorageErrorCode::InvalidStreamState)
                .set_error_message("stream not finalized"),
        ]),
        ..MockPlan::default()
    };
    for (plan, reader, status, cause) in [
        (
            MockPlan::default(),
            fails_after_first_batch(),
            Status::InvalidData,
            "the source broke",
        ),
        (
            refused_commit,
            Box::new(RecordBatchIterator::new(
                [Ok(one_id_batch())],
                one_id_batch().schema(),
            )) as Box<dyn RecordBatchReader + Send>,
            Status::InvalidState,
            "stream not finalized",
        ),
    ] {
        let (outcome, requests, _storage) = ingest_against_mocks(
            vec![(200, differing.clone()), ddl_answer()],
            plan,
            IngestMode::Replace,
            reader,
        );
        let failure = outcome.unwrap_err();
        assert_failure(
            &failure,
            status,
            &[
                cause,
                "already recreated project.dataset.events",
                "rows are gone",
            ],
        );
        assert!(
            requests[1]
                .body
                .get("query")
                .and_then(JsonValue::as_str)
                .is_some_and(|query| query.starts_with("CREATE OR REPLACE TABLE")),
            "{:?}",
            requests[1].body
        );
    }
}

/// Data no AppendRows request can carry or BigQuery would refuse is refused before the target
/// is created or replaced: a row too large on its own and an out-of-range value, even past the
/// first chunk of the first batch, which is normalized a chunk at a time, and a schema that
/// leaves no room for rows. The staged answers would let a premature mutation through.
#[test]
fn data_too_large_to_append_is_refused_before_the_target_changes() {
    let differing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "other", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let oversized_row = {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let large = "x".repeat(10_000_000);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["small", large.as_str()]))],
        )
        .unwrap()
    };
    let oversized_schema = {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([("pandas".to_owned(), "x".repeat(10_000_000))]),
        ));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap()
    };
    let late_invalid_value = RecordBatch::try_from_iter([
        (
            "label",
            Arc::new(StringArray::from(vec![
                "x".repeat(9_500_000),
                "y".to_owned(),
            ])) as ArrayRef,
        ),
        (
            "day",
            Arc::new(Date32Array::from(vec![0, 3_000_000])) as ArrayRef,
        ),
    ])
    .unwrap();
    let invalid_range = {
        let bounds = range_bounds(DataType::Date32);
        let field = bigquery_typed(
            Field::new("period", DataType::Struct(bounds.clone()), true),
            "RANGE<DATE>",
        );
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(StructArray::new(
                bounds,
                vec![
                    Arc::new(Date32Array::from(vec![1])),
                    Arc::new(Date32Array::from(vec![0])),
                ],
                None,
            ))],
        )
        .unwrap()
    };
    for (batch, status, cause) in [
        (
            oversized_row,
            Status::InvalidArguments,
            "row 1 of its input batch",
        ),
        (
            oversized_schema,
            Status::InvalidArguments,
            "Arrow schema serializes",
        ),
        (
            late_invalid_value,
            Status::InvalidData,
            "out-of-range value at row 1",
        ),
        (invalid_range, Status::InvalidData, "lower bound"),
    ] {
        for (mode, responses) in [
            (IngestMode::Create, vec![(200, json!({}))]),
            (
                IngestMode::Replace,
                vec![(200, differing.clone()), ddl_answer()],
            ),
        ] {
            let schema = batch.schema();
            let reader = RecordBatchIterator::new([Ok(batch.clone())], schema);
            let (outcome, requests, storage) =
                ingest_against_mocks(responses, MockPlan::default(), mode, Box::new(reader));
            let failure = outcome.unwrap_err();
            assert_failure(&failure, status, &[cause]);
            assert!(
                requests.is_empty(),
                "{mode:?} sent {:?}",
                requests
                    .iter()
                    .map(|request| &request.line)
                    .collect::<Vec<_>>()
            );
            assert_eq!(storage.write_streams(), (0, 0), "{mode:?}");
        }
    }
}

/// A `replace` over a table whose schema no longer matches is one atomic statement.
///
/// `tables.delete` then `tables.insert` would leave a window without the table, in which a
/// cancel, a rejected insert or a lost process destroys it. This is the dbt full-refresh path
/// for changed columns.
#[test]
fn replacing_a_mismatched_table_never_deletes_it_first() {
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "schema": {"fields": [{"name": "id", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let bound = table_schema([
        field_mode("id", "INTEGER", "REQUIRED"),
        field_mode("name", "STRING", "NULLABLE"),
    ]);
    let (outcome, requests) = against_bigquery(vec![(200, existing), ddl_answer()], |context| {
        plan_and_prepare(context, TARGET, IngestMode::Replace, &bound)
    });
    outcome.unwrap();

    assert_eq!(
        requests.len(),
        2,
        "the schema read and one atomic replace, and nothing else: {:?}",
        requests
            .iter()
            .map(|request| &request.line)
            .collect::<Vec<_>>()
    );
    assert!(requests[0].line.starts_with("GET "), "{}", requests[0].line);
    for request in &requests {
        assert!(
            !request.line.starts_with("DELETE "),
            "the target was deleted before it was recreated: {}",
            request.line
        );
    }
    let body = &requests[1].body;
    assert_eq!(
        body.get("query").and_then(JsonValue::as_str),
        Some(
            "CREATE OR REPLACE TABLE `project.dataset.events` \
                 (`id` INT64 NOT NULL, `name` STRING)"
        )
    );
    assert_carries_only_the_location(body);
}

/// A refused replace names the target, and leaves it standing.
#[test]
fn a_refused_replace_names_the_target_it_could_not_replace() {
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "schema": {"fields": [{"name": "id", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let bound = table_schema([field_mode("id", "INTEGER", "REQUIRED")]);
    let refusal = json!({"error": {"code": 403, "message": "Permission denied on table"}});
    let (outcome, _requests) = against_bigquery(vec![(200, existing), (403, refusal)], |context| {
        plan_and_prepare(context, TARGET, IngestMode::Replace, &bound)
    });
    let failure = outcome.unwrap_err();
    assert_message(
        &failure,
        &["failed to replace BigQuery ingest target project.dataset.events"],
    );
    assert_message_omits(&failure, &["previous rows"]);
}

/// A replace cancelled during its `CREATE OR REPLACE TABLE`, or the `TRUNCATE TABLE` an empty
/// replace runs, cannot say the old rows stand: BigQuery can finish a statement it was asked
/// to cancel. A cancel that came before the statement was sent changed nothing, and its
/// failure says nothing more.
#[test]
fn a_replace_cancelled_once_its_statement_was_sent_warns_that_the_old_rows_may_be_gone() {
    type Statement = fn(&IngestContext<'_>) -> adbc_core::error::Result<()>;
    let recreate: Statement = |context| {
        recreate_table(
            context,
            TARGET,
            &table_schema([field_mode("id", "INTEGER", "REQUIRED")]),
        )
    };
    let truncate: Statement = |context| truncate_table(context, TARGET, Some(context.operation));
    for (statement, label, hedge) in [
        (
            recreate,
            "failed to replace BigQuery ingest target project.dataset.events",
            "BigQuery may still have replaced project.dataset.events, in which case its \
                 previous rows are gone",
        ),
        (
            truncate,
            "failed to empty BigQuery ingest target project.dataset.events",
            "BigQuery may still have emptied project.dataset.events, in which case its \
                 previous rows are gone",
        ),
    ] {
        for sent in [false, true] {
            // The server cancels the statement once it has read its request, then hangs up.
            let cancellation_slot: Arc<Mutex<Option<Arc<Cancellation>>>> = Arc::default();
            let slot = cancellation_slot.clone();
            let (endpoint, captured) = if sent {
                capture_requests_gated(vec![ddl_answer()], move || {
                    if let Some(cancellation) = slot.lock().unwrap().take() {
                        OperationCancelHandle(cancellation).try_cancel().unwrap();
                    }
                    false
                })
            } else {
                capture_requests(Vec::new())
            };
            let runtime = crate::runtime::new_runtime().unwrap();
            let clients = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
            let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
            let operation = cancellation.begin();
            if sent {
                *cancellation_slot.lock().unwrap() = Some(cancellation);
            } else {
                OperationCancelHandle(cancellation).try_cancel().unwrap();
            }
            let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();
            let context = IngestContext {
                runtime: &runtime,
                state: &clients,
                billing_project_id: Some(&billing_project),
                current_catalog: None,
                current_schema: None,
                location: None,
                operation: &operation,
            };

            let failure = statement(&context).unwrap_err();
            operation.finish();
            let requests = captured.join().unwrap();
            assert_eq!(requests.len(), usize::from(sent), "{label}");
            assert_failure(&failure, Status::Cancelled, &[label]);
            assert_eq!(
                failure.message.contains(hedge),
                sent,
                "{label}, sent {sent}: {}",
                failure.message
            );
        }
    }
}

/// A statement whose answer is lost may still have run, like one cancelled once it was sent:
/// a `replace` whose `TRUNCATE` went unanswered, with its new rows still uncommitted, cannot
/// let the caller believe the old rows stand.
#[test]
fn a_truncate_without_an_answer_warns_that_the_old_rows_may_be_gone() {
    let (endpoint, captured) = capture_requests_gated(vec![ddl_answer()], || false);
    let runtime = crate::runtime::new_runtime().unwrap();
    let clients = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
    let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
    let operation = cancellation.begin();
    let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();
    let failure = truncate_table(
        &IngestContext {
            runtime: &runtime,
            state: &clients,
            billing_project_id: Some(&billing_project),
            current_catalog: None,
            current_schema: None,
            location: None,
            operation: &operation,
        },
        TARGET,
        None,
    )
    .unwrap_err();
    operation.finish();
    captured.join().unwrap();
    assert_failure(
        &failure,
        Status::IO,
        &[
            "failed to empty BigQuery ingest target project.dataset.events",
            "BigQuery may still have emptied project.dataset.events",
            "its previous rows are gone",
        ],
    );
}

/// The `TRUNCATE` before a `replace`'s commit runs outside cancellation, so only BigQuery can
/// end it: queued behind a busy reservation it would otherwise hold the statement forever. It
/// carries a job timeout of the driver's own, which no option named in the failure can change,
/// and the failure says the new rows went uncommitted.
#[test]
fn a_truncate_bigquery_stops_leaves_the_replace_uncommitted() {
    let matching = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]},
    });
    let stopped = "Job execution was cancelled: Job timed out after 600 sec";
    let (outcome, requests, storage) = ingest_against_mocks(
        vec![ok(matching), (499, error_body("stopped", stopped))],
        MockPlan::default(),
        IngestMode::Replace,
        Box::new(RecordBatchIterator::new(
            [Ok(one_id_batch())],
            one_id_batch().schema(),
        )),
    );
    let failure = outcome.unwrap_err();
    assert_failure(
        &failure,
        Status::Timeout,
        &[
            "failed to empty BigQuery ingest target project.dataset.events",
            "BigQuery may still have emptied project.dataset.events",
            "none of this ingest's rows were committed",
        ],
    );
    assert_message_omits(&failure, &["bigquery.query.job_timeout"]);
    assert_eq!(requests[1].sql(), "TRUNCATE TABLE `project.dataset.events`");
    assert_eq!(requests[1].body["jobTimeoutMs"], json!("600000"));
    assert_eq!(storage.finalized(), (Some(1), false));
}

/// An ingest whose bound schema cannot be spelled as DDL is refused before anything mutates.
#[test]
fn a_replace_with_no_columns_is_refused_before_the_target_changes() {
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "schema": {"fields": [{"name": "id", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let (outcome, requests) = against_bigquery(vec![(200, existing)], |context| {
        plan_and_prepare(
            context,
            TARGET,
            IngestMode::Replace,
            &TableSchema::default(),
        )
    });
    assert!(outcome.unwrap_err().message.contains("no fields"));
    assert_eq!(requests.len(), 1, "only the schema read was sent");
}

/// A bound schema with no columns is refused before any request in every mode. `tables.insert`
/// accepts it and creates a table with no schema, which no query can read and no later
/// ingest can append to.
#[test]
fn an_ingest_with_no_columns_is_refused_before_any_request() {
    let schema = Arc::new(Schema::empty());
    for mode in [
        IngestMode::Create,
        IngestMode::Append,
        IngestMode::CreateAppend,
        IngestMode::Replace,
    ] {
        let options = IngestOptions {
            mode,
            target_db_schema: Some(TARGET.dataset_id.to_owned()),
            target_table: Some(TARGET.table_id.to_owned()),
            ..IngestOptions::default()
        };
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();
        let (outcome, requests) = against_bigquery(Vec::new(), |context| {
            execute_ingest(
                *context,
                &options,
                Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone())),
            )
        });
        let failure = outcome.unwrap_err();
        assert_failure(&failure, Status::InvalidArguments, &["no fields"]);
        assert!(requests.is_empty(), "{mode:?}");

        let (outcome, requests) = against_bigquery(Vec::new(), |context| {
            validate_ingest(*context, &options, &schema)
        });
        let failure = outcome.unwrap_err();
        assert_failure(&failure, Status::InvalidArguments, &["no fields"]);
        assert!(requests.is_empty(), "{mode:?}");
    }
}

/// A `REPEATED TIMESTAMP` field, at any depth and from any Arrow list or timestamp layout that
/// normalizes to one, is refused before any request in every mode and in a dry run: Storage
/// Write fails every append carrying one (UP-90), after `create` has made the table. Repeated
/// DATE, DATETIME and TIME, and timestamps inside an array of structs, are not refused.
#[test]
fn an_array_of_timestamps_is_refused_before_any_request() {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let list = |item: DataType| DataType::new_list(item, true);
    let record = |name: &str, data_type: DataType| {
        DataType::Struct(Fields::from(vec![Field::new(name, data_type, true)]))
    };
    let refused = [
        ("ts", list(timestamp.clone())),
        (
            "ts",
            DataType::LargeList(Arc::new(Field::new(
                "element",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into())),
                false,
            ))),
        ),
        (
            "ts",
            DataType::FixedSizeList(
                Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    true,
                )),
                2,
            ),
        ),
        ("record.ts", record("ts", list(timestamp.clone()))),
        (
            "outer.inner.ts",
            record("inner", record("ts", list(timestamp.clone()))),
        ),
        ("items.ts", list(record("ts", list(timestamp.clone())))),
    ];
    let accepted = [
        list(record("ts", timestamp.clone())),
        list(DataType::Date32),
        list(DataType::Timestamp(TimeUnit::Microsecond, None)),
        list(DataType::Time64(TimeUnit::Microsecond)),
        record("ts", timestamp),
    ];

    let column = |path: &str, data_type: DataType| -> SchemaRef {
        let name = path.split('.').next().unwrap();
        Arc::new(Schema::new(vec![Field::new(name, data_type, true)]))
    };
    // Answers a request the driver should never send, so a regression is caught by the
    // capture rather than hidden in a connection failure.
    let missing = json!({"error": {"code": 404, "message": "Not found: Table"}});
    let run = |run: &dyn Fn(&IngestContext<'_>) -> adbc_core::error::Result<()>| {
        let (endpoint, server) = capture_requests_until_stopped(vec![(404, missing.clone())]);
        let runtime = crate::runtime::new_runtime().unwrap();
        let clients = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
        let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
        let operation = cancellation.begin();
        let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();
        let outcome = run(&IngestContext {
            runtime: &runtime,
            state: &clients,
            billing_project_id: Some(&billing_project),
            current_catalog: Some(&billing_project),
            current_schema: None,
            location: None,
            operation: &operation,
        });
        operation.finish();
        (outcome, server.stop())
    };
    let options = |mode| IngestOptions {
        mode,
        target_db_schema: Some(TARGET.dataset_id.to_owned()),
        target_table: Some(TARGET.table_id.to_owned()),
        ..IngestOptions::default()
    };

    for (path, data_type) in refused {
        let schema = column(path, data_type);
        for mode in [
            IngestMode::Create,
            IngestMode::Append,
            IngestMode::CreateAppend,
            IngestMode::Replace,
        ] {
            let options = options(mode);
            let (outcome, requests) = run(&|context| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![new_empty_array(schema.field(0).data_type())],
                )
                .unwrap();
                let reader = RecordBatchIterator::new([Ok(batch)], schema.clone());
                execute_ingest(*context, &options, Box::new(reader)).map(|_| ())
            });
            let (dry_run, dry_run_requests) =
                run(&|context| validate_ingest(*context, &options, &schema));
            for (failure, requests) in [(outcome, requests), (dry_run, dry_run_requests)] {
                let failure = failure.unwrap_err();
                assert_eq!(failure.status, Status::NotImplemented, "{path} {mode:?}");
                assert_eq!(
                    failure.message,
                    format!(
                        "ingest field {path:?} is ARRAY<TIMESTAMP>, which BigQuery Storage \
                             Write cannot append; ingest it as ARRAY<STRUCT<ts TIMESTAMP>> into \
                             a staging table and convert it with SQL"
                    )
                );
                let sent = requests.iter().map(|request| &request.line);
                assert_eq!(sent.collect::<Vec<_>>(), [""; 0], "{path} {mode:?}");
            }
        }
    }

    for data_type in accepted {
        let schema = column("values", data_type);
        let options = options(IngestMode::Create);
        let (outcome, requests) = run(&|context| validate_ingest(*context, &options, &schema));
        outcome.unwrap_or_else(|failure| panic!("{}: {failure}", schema.field(0)));
        assert_eq!(requests.len(), 1, "only the table lookup was sent");
    }
}

/// A table another writer creates between the preflight read and the insert is written into
/// rather than reported: `create_append` appends to it and `replace` truncates it, exactly as
/// they would have had the read seen it.
#[test]
fn a_target_created_concurrently_is_planned_again() {
    let missing = json!({"error": {"code": 404, "message": "Not found: Table"}});
    let conflict = json!({"error": {"code": 409, "message": "Already Exists: Table"}});
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]},
    });
    let bound = table_schema([field_mode("id", "INTEGER", "REQUIRED")]);
    for (mode, expected) in [
        (IngestMode::CreateAppend, TablePlan::Reuse),
        (IngestMode::Replace, TablePlan::Truncate),
    ] {
        let (outcome, requests) = against_bigquery(
            vec![
                (404, missing.clone()),
                (409, conflict.clone()),
                (200, existing.clone()),
            ],
            |context| plan_and_prepare(context, TARGET, mode, &bound),
        );
        assert_eq!(outcome.unwrap(), expected, "{mode:?}");
        let lines = requests
            .iter()
            .map(|request| request.line.split(' ').next().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(lines, ["GET", "POST", "GET"], "{mode:?}");
    }
}

/// An append of a column pandas typed `Null` (it holds only nulls) fills the table's column of
/// that name with NULLs, reading the table once, both to type the column and to plan.
#[test]
fn an_all_null_column_is_appended_as_the_tables_column() {
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "note", "type": "STRING", "mode": "NULLABLE"},
        ]},
    });
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("note", Arc::new(NullArray::new(2)) as ArrayRef),
    ])
    .unwrap();
    let schema = batch.schema();
    let (outcome, requests, storage) = ingest_against_mocks(
        vec![(200, existing)],
        MockPlan::default(),
        IngestMode::Append,
        Box::new(RecordBatchIterator::new([Ok(batch)], schema)),
    );
    assert_eq!(outcome.unwrap(), 2);
    assert_eq!(storage.written_rows(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.line.split(' ').next().unwrap())
            .collect::<Vec<_>>(),
        ["GET"]
    );
}

/// An all-null column the table cannot type is reported as adbc.h asks for an append: the
/// target's absence as `NOT_FOUND`, and a column it lacks as the `ALREADY_EXISTS` of a
/// schema mismatch, not as an untypable `Null`. A dry run reports the same.
#[test]
fn an_all_null_column_without_a_column_to_fill_reports_the_target() {
    let missing = json!({"error": {"code": 404, "message": "Not found: Table"}});
    let lacking = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]},
    });
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("extra", Arc::new(NullArray::new(1)) as ArrayRef),
    ])
    .unwrap();
    for (target, status, message) in [
        (
            (404, missing),
            Status::NotFound,
            "BigQuery ingest target project.dataset.events does not exist",
        ),
        (
            (200, lacking),
            Status::AlreadyExists,
            "bound column \"extra\" is not in the table",
        ),
    ] {
        let schema = batch.schema();
        let reader = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
        let (outcome, _, _) = ingest_against_mocks(
            vec![target.clone()],
            MockPlan::default(),
            IngestMode::Append,
            Box::new(reader),
        );
        let failure = outcome.unwrap_err();
        assert_failure(&failure, status, &[message]);

        let options = IngestOptions {
            mode: IngestMode::Append,
            target_db_schema: Some(TARGET.dataset_id.to_owned()),
            target_table: Some(TARGET.table_id.to_owned()),
            ..IngestOptions::default()
        };
        let (outcome, _) = against_bigquery(vec![target], |context| {
            validate_ingest(*context, &options, &schema)
        });
        assert_eq!(outcome.unwrap_err().status, status, "dry run");
    }
}

/// An append reads its target before its rows, so a target it cannot write into is reported
/// before the bound stream is read: the reader here fails if it is ever asked for a batch.
#[test]
fn an_append_into_a_mismatched_table_reads_no_rows() {
    let existing = json!({
        "tableReference": {
            "projectId": "project", "datasetId": "dataset", "tableId": "events",
        },
        "type": "TABLE",
        "schema": {"fields": [{"name": "other", "type": "STRING", "mode": "NULLABLE"}]},
    });
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let reader = RecordBatchIterator::new(
        [Err(ArrowError::ComputeError(
            "the rows were read".to_owned(),
        ))],
        schema,
    );
    let (outcome, _, storage) = ingest_against_mocks(
        vec![(200, existing)],
        MockPlan::default(),
        IngestMode::Append,
        Box::new(reader),
    );
    let failure = outcome.unwrap_err();
    assert_failure(
        &failure,
        Status::AlreadyExists,
        &["bound column \"id\" is not in the table"],
    );
    assert_eq!(storage.write_streams(), (0, 0));
}

/// Create mode skips the preflight read, so the insert's conflict has to give the remedy the
/// preflight would have, while keeping the service's reason and HTTP status as details.
#[test]
fn a_create_over_an_existing_table_names_the_modes_that_write_into_it() {
    let conflict = json!({"error": {
        "code": 409,
        "message": "Already Exists: Table project:dataset.events",
        "errors": [{
            "reason": "duplicate",
            "message": "Already Exists: Table project:dataset.events"
        }]
    }});
    let (outcome, requests) = against_bigquery(vec![(409, conflict)], |context| {
        plan_and_prepare(
            context,
            TARGET,
            IngestMode::Create,
            &table_schema([field_mode("id", "INTEGER", "REQUIRED")]),
        )
    });
    let failure = outcome.unwrap_err();
    assert_eq!(failure.status, Status::AlreadyExists);
    assert_eq!(
        failure.message,
        "BigQuery ingest target project.dataset.events already exists; set adbc.ingest.mode \
             to \"adbc.ingest.mode.append\" or \"adbc.ingest.mode.replace\" (HTTP 409)"
    );
    assert_eq!(
        failure.details,
        Some(vec![
            (DETAIL_REASON.to_owned(), b"duplicate".to_vec()),
            (DETAIL_HTTP_STATUS.to_owned(), b"409".to_vec()),
        ])
    );
    assert_eq!(requests.len(), 1, "only the insert was sent");
}

#[test]
fn ingest_method_accepts_only_storage_write() {
    const KEY: &str = "bigquery.bulk_ingest.method";
    for value in ["storage_write", "STORAGE_WRITE", "Storage_Write"] {
        assert_eq!(
            parse_ingest_method(KEY, value).unwrap(),
            BigQueryBulkIngestMethod::StorageWrite,
            "{value}"
        );
    }
    // The Go driver's value is refused by name in whatever case it was typed, so the caller
    // is told what to use instead rather than being handed the generic enum rejection.
    for value in ["load", "LOAD", "Load"] {
        let error = parse_ingest_method(KEY, value).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented, "{value}");
        assert!(error.message.contains("storage_write"), "{value}");
    }
    for value in ["", "StorageWrite", "storage-write"] {
        let error = parse_ingest_method(KEY, value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        // Every enum rejection names the option key; none of them echo the value.
        assert_message(&error, &[KEY]);
        assert!(!error.message.contains(value) || value.is_empty());
    }
    let secret = "ingest-method-option-payload-secret-sentinel";
    assert!(
        !parse_ingest_method(KEY, secret)
            .unwrap_err()
            .message
            .contains(secret)
    );
}

// Ingests driven end to end against a scripted Storage Write server.
//
// A live ingest cannot tear an `AppendRows` stream on demand, so these cover
// `reattach`, the writer replacement, and their interaction with the
// append pipeline and finalize. The mock keeps a real write cursor: it applies an append only at
// the cursor, answers `ALREADY_EXISTS` behind it, and reports its own row count at finalize, so
// a replay that wrote rows twice fails the finalize check.

const TABLE: &str = "projects/project/datasets/dataset/tables/events";
/// Five batches of two rows, so the ingest sends one append per batch at offsets 0, 2, 4, 6
/// and 8. That exceeds the append pipeline's depth, so a tear leaves successors queued behind
/// the failed append.
const BATCHES: i64 = 5;
const ROWS_PER_BATCH: i64 = 2;

fn ingest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn ingest_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
    (0..BATCHES)
        .map(|batch| {
            let ids = (0..ROWS_PER_BATCH)
                .map(|row| batch * ROWS_PER_BATCH + row)
                .collect::<Vec<_>>();
            let names = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// Runs one whole ingest transport against `mock`, from stream creation to commit.
fn ingest(
    runtime: &SharedRuntime,
    mock: &StorageMock,
    schema: &SchemaRef,
) -> adbc_core::error::Result<i64> {
    let batches = ingest_batches(schema);
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    ingest_from(runtime, mock, schema, Box::new(reader))
}

/// Like [`ingest`], with the batches `reader` yields.
fn ingest_from(
    runtime: &SharedRuntime,
    mock: &StorageMock,
    schema: &SchemaRef,
    mut reader: Box<dyn RecordBatchReader + Send>,
) -> adbc_core::error::Result<i64> {
    let clients = ClientState::for_test_at(ServiceEndpointKind::Storage, mock.endpoint());
    let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
    let operation = cancellation.begin();
    let client = clients.write_client(runtime).unwrap();
    let normalized_schema = Arc::new(normalize_schema(schema).unwrap());
    // `write_batches` takes the first batch already checked, because the real path checks
    // it before the target table is created or truncated.
    let first = reader.next().unwrap().unwrap();
    let first = FirstBatch::new(normalized_schema, first, &operation).unwrap();
    let result = runtime.block_on(async {
        write_batches(&client, TABLE, schema.clone(), first, reader, &operation)
            .await?
            .commit(TABLE)
            .await
    });
    operation.finish();
    result
}

#[test]
fn the_first_ingest_batch_is_released_before_reading_more_input() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let mock = StorageMock::start(&runtime, MockPlan::default());
    let array = Arc::new(Int64Array::from(vec![1; 1024 * 1024]).slice(0, 2));
    assert_eq!(array.get_buffer_memory_size(), 8 * 1024 * 1024);
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![array, Arc::new(StringArray::from(vec!["first", "second"]))],
    )
    .unwrap();
    let mut batches = std::iter::once(first).chain(ingest_batches(&schema));
    let mut previous: Option<arrow_buffer::Buffer> = None;
    let reader = RecordBatchIterator::new(
        std::iter::from_fn(move || {
            if let Some(buffer) = previous.as_ref() {
                assert_eq!(
                    buffer.strong_count(),
                    1,
                    "only the test's observer may retain the previous input buffer"
                );
            }
            let next = batches.next();
            previous = next
                .as_ref()
                .map(|batch| batch.column(0).to_data().buffers()[0].clone());
            next.map(Ok)
        }),
        schema.clone(),
    );

    let rows = ingest_from(&runtime, &mock, &schema, Box::new(reader)).unwrap();
    let total = (BATCHES + 1) * ROWS_PER_BATCH;
    assert_eq!(rows, total);
    assert_eq!(mock.finalized(), (Some(total), true));
}

/// A torn `AppendRows` stream reattaches to the same stream and finalizes the right row
/// count.
///
/// The mock writes the third append, then ends the call without acknowledging it. Recovery
/// must reattach to the same pending stream, replay the unacknowledged append and its queued
/// successors at their original offsets, and absorb the `ALREADY_EXISTS` for the landed one.
/// The mock's row count at finalize catches any double write.
#[test]
fn a_torn_append_stream_reattaches_and_finalizes_the_appended_rows() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            tear_append_after: Some(3),
            ..MockPlan::default()
        },
    );

    let rows = ingest(&runtime, &mock, &schema).unwrap();
    let total = BATCHES * ROWS_PER_BATCH;
    assert_eq!(rows, total);
    assert_eq!(mock.written_rows(), total, "the replay wrote rows twice");
    assert_eq!(mock.finalized(), (Some(total), true));
    assert_eq!(
        mock.write_streams(),
        (1, 1),
        "recovery must reattach to the one pending stream, not create another"
    );
    // The first call carries offsets 0, 2 and 4; the third is written but never acknowledged,
    // so the replay resends it and the successors queued behind it at the same offsets.
    assert_eq!(mock.append_offsets(), vec![0, 2, 4, 4, 6, 8]);
    // The replay of the landed append is behind the stream's cursor, so it is answered
    // ALREADY_EXISTS, which the ingest absorbed.
    assert_eq!(mock.already_exists_offsets(), vec![2 * ROWS_PER_BATCH]);
}

/// A rejected append is reported, not the rejections its queued successors then earn.
///
/// The mock refuses the append at offset 2, which leaves the appends queued behind it past the
/// stream's cursor, so each is answered `OUT_OF_RANGE`. `AppendPipeline::complete_oldest`
/// drops those successors, so the later drain in `append_all` cannot report one in place of
/// the refusal.
///
/// The pipeline depth is not set here, so the test first asserts that the mock did answer a
/// successor `OUT_OF_RANGE`; otherwise it would pass for a driver with nothing to drop. The
/// mock holds the refusal until a successor arrives (see `MockPlan::reject_at`), so that
/// record is complete when the driver returns.
#[test]
fn a_rejected_append_is_reported_rather_than_its_successors_rejections() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            reject_at: Some((ROWS_PER_BATCH, INVALID_ARGUMENT)),
            ..MockPlan::default()
        },
    );

    let failure = ingest(&runtime, &mock, &schema).unwrap_err();

    // The refused append reached the service, and at least one append queued behind it was
    // answered `OUT_OF_RANGE`, so there was a competing failure.
    let offsets = mock.append_offsets();
    assert!(
        offsets.contains(&ROWS_PER_BATCH),
        "the refused append was never sent: {offsets:?}"
    );
    let rejected_successors = mock.out_of_range_offsets();
    assert!(
        !rejected_successors.is_empty(),
        "no append was queued behind the refused one, so nothing competed with it: {offsets:?}"
    );
    assert!(
        rejected_successors
            .iter()
            .all(|offset| *offset > ROWS_PER_BATCH),
        "an append at or before the refused offset was answered OUT_OF_RANGE: \
             {rejected_successors:?}"
    );

    // What is reported is the refusal itself: its status, its target and its own code.
    assert_failure(&failure, Status::InvalidArguments, &[TABLE]);
    assert!(
        failure.message.contains("INVALID_ARGUMENT"),
        "the refused append was reported as {}",
        failure.message
    );
    // The RPC error itself, without upstream's `AppendError` wrapper prose.
    assert_message_omits(&failure, &["the operation failed"]);
    assert!(
        !failure.message.contains("OUT_OF_RANGE"),
        "a queued successor's rejection was reported instead: {}",
        failure.message
    );
}

/// An append refused with a transient status is replayed at its own offset, together with
/// the successors the refusal pushed past the stream's cursor, and every row lands once.
#[test]
fn a_transiently_refused_append_is_replayed_exactly_once() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            reject_at: Some((ROWS_PER_BATCH, ABORTED)),
            ..MockPlan::default()
        },
    );

    let total = BATCHES * ROWS_PER_BATCH;
    assert_eq!(ingest(&runtime, &mock, &ingest_schema()).unwrap(), total);
    assert_eq!(mock.written_rows(), total, "the replay wrote rows twice");
    assert_eq!(mock.finalized(), (Some(total), true));
    assert_eq!(mock.write_streams(), (1, 1));
    let offsets = mock.append_offsets();
    assert_eq!(
        offsets
            .iter()
            .filter(|&&offset| offset == ROWS_PER_BATCH)
            .count(),
        2,
        "the refused append was not replayed exactly once: {offsets:?}"
    );
}

#[test]
fn initial_and_pipelined_appends_stop_after_three_retries() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for offset in [0, ROWS_PER_BATCH] {
        let mock = StorageMock::start(
            &runtime,
            MockPlan {
                repeat_append_refusal: Some((offset, UNAVAILABLE)),
                ..MockPlan::default()
            },
        );
        let failure = ingest(&runtime, &mock, &ingest_schema()).unwrap_err();
        assert_eq!(failure.status, Status::IO);
        assert_eq!(
            mock.append_offsets()
                .iter()
                .filter(|&&sent| sent == offset)
                .count(),
            4,
            "one initial append and three retries at offset {offset}"
        );
        assert_eq!(mock.write_streams(), (1, 3));
        assert_eq!(mock.finalized(), (Some(offset), false));
    }
}

/// `ALREADY_EXISTS` on the first send of a driver-assigned offset is a failure: absorbing it
/// would turn the signal of drifted offset accounting into a silently short ingest.
#[test]
fn already_exists_on_a_first_send_is_reported() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            already_exists_at: Some(ROWS_PER_BATCH),
            ..MockPlan::default()
        },
    );

    let failure = ingest(&runtime, &mock, &schema).unwrap_err();
    assert_failure(&failure, Status::AlreadyExists, &[TABLE]);
    // The abandoned stream is finalized best-effort, and nothing is committed.
    let (finalized, committed) = mock.finalized();
    assert!(finalized.is_some(), "the abandoned stream was left open");
    assert!(!committed, "a failed ingest must not commit its stream");
}

#[test]
fn a_reader_failure_does_not_wait_for_a_stalled_append() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let gate = Arc::new((Notify::new(), Notify::new()));
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            append_gate: Some((ROWS_PER_BATCH, gate.clone())),
            ..MockPlan::default()
        },
    );
    let batches = ingest_batches(&schema);
    let (fail, failure_allowed) = std::sync::mpsc::channel();
    let reader = RecordBatchIterator::new(
        batches
            .into_iter()
            .take(2)
            .map(Ok)
            .chain(std::iter::once_with(move || {
                failure_allowed
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                Err(ArrowError::ComputeError("the source broke".to_owned()))
            })),
        schema.clone(),
    );
    let (sent, received) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            sent.send(ingest_from(&runtime, &mock, &schema, Box::new(reader)))
                .unwrap();
        });
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), gate.0.notified())
                .await
                .expect("the queued append never arrived");
        });
        fail.send(()).unwrap();
        let result = received.recv_timeout(Duration::from_secs(5));
        // Release the server even on failure, so the regression cannot hang the test.
        gate.1.notify_one();
        let failure = result
            .expect("input failure waited for an append")
            .unwrap_err();
        assert_failure(&failure, Status::InvalidData, &["the source broke"]);
        assert!(mock.finalized().0.is_some());
        assert!(!mock.finalized().1);
    });
}

#[test]
fn failed_finalize_attempts_cleanup_and_preserves_the_original_error() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for cleanup_fails in [false, true] {
        let mut finalize_refusals = vec![INVALID_ARGUMENT];
        if cleanup_fails {
            finalize_refusals.push(UNAVAILABLE);
        }
        let mock = StorageMock::start(
            &runtime,
            MockPlan {
                finalize_refusals,
                ..MockPlan::default()
            },
        );

        let failure = ingest(&runtime, &mock, &ingest_schema()).unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidArguments,
            &["failed to finalize", TABLE],
        );
        assert_eq!(mock.finalize_calls(), 2);
        assert_eq!(mock.written_rows(), BATCHES * ROWS_PER_BATCH);
        assert_eq!(
            mock.finalized(),
            ((!cleanup_fails).then_some(BATCHES * ROWS_PER_BATCH), false)
        );
    }
}

/// The stream's first append is sent alone, so it carries about 1 MB rather than a full
/// request; the rest of the batch then fills requests of their usual size.
#[test]
fn the_first_append_is_small_and_later_ones_fill_their_requests() {
    const ROWS: i64 = 300_000;

    let runtime = crate::runtime::new_runtime().unwrap();
    let schema = ingest_schema();
    let mock = StorageMock::start(&runtime, MockPlan::default());
    // About 22 bytes a row, so 6.6 MB in all.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|row| format!("{row:010}")),
            )),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new([Ok(batch)], schema.clone());

    assert_eq!(
        ingest_from(&runtime, &mock, &schema, Box::new(reader)).unwrap(),
        ROWS
    );
    let offsets = mock.append_offsets();
    assert_eq!(offsets.len(), 2, "{offsets:?}");
    assert!(
        (ROWS / 10..ROWS / 4).contains(&offsets[1]),
        "the first append carried {} of {ROWS} rows",
        offsets[1]
    );
}

#[test]
fn rejected_rows_keep_the_input_offset_and_are_never_replayed() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            reject_at: Some((ROWS_PER_BATCH, INVALID_ARGUMENT)),
            rejected_rows: vec![
                RowError::new()
                    .set_index(1)
                    .set_code(RowErrorCode::FieldsError)
                    .set_message("invalid timestamp"),
            ],
            ..MockPlan::default()
        },
    );
    let failure = ingest(&runtime, &mock, &ingest_schema()).unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidData,
        &["input row 3", "FIELDS_ERROR", "invalid timestamp"],
    );
    assert_eq!(mock.write_streams(), (1, 0));
    assert!(!mock.finalized().1);
}

#[test]
fn commit_refusals_and_missing_commit_time_remain_distinct() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (errors, status, message) in [
        (
            vec![
                StorageError::new()
                    .set_code(StorageErrorCode::InvalidStreamState)
                    .set_error_message("stream not finalized"),
            ],
            Status::InvalidState,
            "none of its 10 rows",
        ),
        (
            vec![],
            Status::IO,
            "whether its 10 rows were committed is unknown",
        ),
    ] {
        let mock = StorageMock::start(
            &runtime,
            MockPlan {
                commit_errors: Some(errors),
                ..MockPlan::default()
            },
        );
        let failure = ingest(&runtime, &mock, &ingest_schema()).unwrap_err();
        assert_failure(&failure, status, &[message]);
        assert!(!mock.finalized().1);
    }
}

#[test]
fn a_lost_commit_response_is_retried_without_duplicating_rows() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let mock = StorageMock::start(
        &runtime,
        MockPlan {
            lose_commit_response: true,
            commit_errors: Some(vec![
                StorageError::new().set_code(StorageErrorCode::StreamAlreadyCommitted),
            ]),
            ..MockPlan::default()
        },
    );
    let total = BATCHES * ROWS_PER_BATCH;
    assert_eq!(ingest(&runtime, &mock, &ingest_schema()).unwrap(), total);
    assert_eq!(mock.written_rows(), total);
    assert_eq!(mock.finalized(), (Some(total), true));
    assert_eq!(mock.append_offsets(), vec![0, 2, 4, 6, 8]);
}
