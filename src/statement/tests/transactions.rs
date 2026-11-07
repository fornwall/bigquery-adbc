//! Manual transactions at statement scope: what joins the connection's session, what is refused
//! outside autocommit, and what a failed lazy begin leaves behind.

use super::*;
use crate::test_support::http::capture_requests_gated;

#[test]
fn enabling_autocommit_commits_pending_manual_work() {
    let complete = || json!({"jobReference": job_reference("fixture-project", "transactions-job-1"), "jobComplete": true});
    let (endpoint, requests) = capture_requests(vec![
        completed_job(
            "transactions-job-2",
            json!({
                "sessionInfo": {"sessionId": "mock-session"}
            }),
        ),
        ok(complete()),
        ok(complete()),
        ok(complete()),
        ok(complete()),
    ]);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), None);

    set_option(&mut connection, OptionConnection::AutoCommit, true);
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );

    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "INSERT dataset.table VALUES (42)",
            "COMMIT TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
}

#[test]
fn failed_lazy_begin_does_not_execute_the_caller_query_or_enable_autocommit() {
    let (endpoint, requests) = capture_requests(vec![
        completed_job(
            "transactions-job-3",
            json!({
                "sessionInfo": {"sessionId": "mock-session"}
            }),
        ),
        (
            400,
            json!({
                "error": {
                    "code": 400,
                    "message": "injected BEGIN failure",
                    "status": "INVALID_ARGUMENT"
                }
            }),
        ),
        completed_job("transactions-job-4", json!({})),
    ]);
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();

    assert!(statement.execute_update().is_err());
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_DISABLED
    );
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')",
        ]
    );
}

/// A cancel that lands while the session-creating `jobs.query` is in flight still ends the
/// session BigQuery creates, and leaves the next statement to open a fresh one.
#[test]
fn cancelling_while_the_session_is_created_still_ends_the_session() {
    let (request_seen, wait_for_request) = std::sync::mpsc::sync_channel(0);
    let (release_response, response_released) = std::sync::mpsc::sync_channel(0);
    let (endpoint, requests) = capture_requests_gated(
        vec![
            completed_job(
                "transactions-job-5",
                json!({
                    "sessionInfo": {"sessionId": "mock-session"}
                }),
            ),
            completed_job("transactions-job-6", json!({})),
        ],
        move || {
            request_seen.send(()).unwrap();
            response_released.recv().unwrap();
            true
        },
    );
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();
    let cancel = statement.get_cancel_handle();
    let canceller = std::thread::spawn(move || {
        wait_for_request.recv().unwrap();
        cancel.try_cancel().unwrap();
        release_response.send(()).unwrap();
    });

    let failure = statement.execute_update().unwrap_err();
    canceller.join().unwrap();

    assert_eq!(failure.status, Status::Cancelled, "{failure}");
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        ["SELECT 1", "CALL BQ.ABORT_SESSION('mock-session')"]
    );
    assert_eq!(
        requests[1].body["connectionProperties"],
        json!([{"key": "session_id", "value": "mock-session"}])
    );
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// A connection released while a statement opens its transaction leaves nothing to commit that
/// transaction, so the statement reports the release and ends the session it opened.
#[test]
fn releasing_the_connection_while_a_statement_opens_its_transaction_ends_the_session() {
    let (request_seen, wait_for_request) = std::sync::mpsc::sync_channel(0);
    let (release_response, response_released) = std::sync::mpsc::sync_channel(0);
    let (endpoint, requests) = capture_requests_gated(
        vec![
            completed_job(
                "transactions-job-7",
                json!({
                    "sessionInfo": {"sessionId": "mock-session"}
                }),
            ),
            completed_job("transactions-job-8", json!({})),
            completed_job("transactions-job-9", json!({})),
        ],
        move || {
            request_seen.send(()).unwrap();
            response_released.recv().unwrap();
            true
        },
    );
    let mut connection = test_connection_at(&endpoint);
    set_option(&mut connection, OptionConnection::AutoCommit, false);
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (42)")
        .unwrap();
    let releaser = std::thread::spawn(move || {
        wait_for_request.recv().unwrap();
        drop(connection);
        release_response.send(()).unwrap();
    });

    let failure = statement.execute_update().unwrap_err();
    releaser.join().unwrap();

    assert_eq!(failure.status, Status::InvalidState, "{failure}");
    assert!(failure.message.contains("released"), "{failure}");
    let requests = requests.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(CapturedRequest::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT 1",
            "BEGIN TRANSACTION",
            "CALL BQ.ABORT_SESSION('mock-session')"
        ]
    );
    assert_eq!(
        requests[2].body["connectionProperties"],
        json!([{"key": "session_id", "value": "mock-session"}])
    );
    // The mock has finished, so a request would fail with an I/O error rather than this one.
    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
fn ingest_inside_a_transaction_is_rejected_by_every_execute_entrypoint() {
    let batch = int64_column("value", vec![1_i64]);
    let ingest_statement = || {
        let mut statement = test_statement();
        statement.transaction.disable_autocommit().unwrap();
        set_option(&mut statement, OptionStatement::TargetTable, "events");
        statement.bind(batch.clone()).unwrap();
        statement
    };

    for mode in [
        "adbc.ingest.mode.create",
        "adbc.ingest.mode.append",
        "adbc.ingest.mode.replace",
        "adbc.ingest.mode.create_append",
    ] {
        let mut statement = ingest_statement();
        set_option(&mut statement, OptionStatement::IngestMode, mode);
        let error = statement.execute_update().unwrap_err();
        assert_eq!(error.status, Status::InvalidState, "mode {mode}");
        assert!(
            error.message.contains("transaction")
                && error.message.contains("adbc.connection.autocommit"),
            "unexpected message for mode {mode}: {}",
            error.message
        );
    }

    assert_eq!(
        ingest_statement().execute().err().unwrap().status,
        Status::InvalidState
    );
    assert_eq!(
        ingest_statement().execute_partitions().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
fn ingest_transaction_rejection_precedes_the_unbound_data_error() {
    let mut statement = test_statement();
    statement.transaction.disable_autocommit().unwrap();
    set_option(&mut statement, OptionStatement::TargetTable, "events");

    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::InvalidState
    );
}

/// The manual-transaction refusal is about ingest alone: a query inside a transaction still runs,
/// and runs in the session the transaction opened.
/// A read has nothing a later commit or rollback could appear to cover, so it runs where the
/// option-only operations that change something are refused.
#[test]
fn a_dataset_location_lookup_runs_with_autocommit_disabled() {
    let (endpoint, captured) = capture_requests(vec![ok(json!({"location": "EU"}))]);
    let mut statement = test_statement_at(&endpoint);
    set_option(
        &mut statement,
        BigQueryStatementOption::GetDatasetLocation,
        r#"{"project":"data-project","dataset":"analytics"}"#,
    );
    statement.transaction.disable_autocommit().unwrap();

    let (_, rows_affected) = statement.execute_with_rows_affected().unwrap();

    assert_eq!(rows_affected, 1);
    captured.join().unwrap();
}

#[test]
fn queries_are_unaffected_by_the_ingest_transaction_rejection() {
    let (endpoint, requests) = capture_requests(vec![completed_job(
        "transactions-job-10",
        json!({"numDmlAffectedRows": "1"}),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.transaction.disable_autocommit().unwrap();
    let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap() else {
        panic!("expected a starting transaction")
    };
    start.started(Arc::from("fixture-session")).unwrap();
    statement
        .set_sql_query("INSERT dataset.events VALUES (1)")
        .unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(1));
    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].sql(), "INSERT dataset.events VALUES (1)");
    assert_eq!(
        requests[0].body["connectionProperties"],
        json!([{"key": "session_id", "value": "fixture-session"}])
    );
}

#[test]
fn empty_parameter_update_does_not_start_a_manual_transaction() {
    let mut statement = test_statement();
    statement.transaction.disable_autocommit().unwrap();
    statement
        .set_sql_query("INSERT dataset.table VALUES (?)")
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Int64, true)]));
    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            std::iter::empty::<Result<RecordBatch, ArrowError>>(),
            schema,
        )))
        .unwrap();

    assert_eq!(statement.execute_update().unwrap(), Some(0));
    assert!(!statement.transaction.is_autocommit().unwrap());
    assert!(statement.transaction.session_id().unwrap().is_none());
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

/// Every query in manual-commit mode runs in the transaction's session, and BigQuery refuses a
/// job that names a session and asks for a new one. Each query entry point refuses before it
/// begins the transaction or consumes the binding.
#[test]
fn session_creation_is_refused_inside_a_transaction_before_anything_runs() {
    let mut statement = test_statement();
    statement.transaction.disable_autocommit().unwrap();
    statement.set_sql_query("SELECT ?").unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::CreateSession,
        ADBC_OPTION_VALUE_ENABLED,
    );
    let batch = int64_column("value", vec![1_i64]);
    statement.bind(batch).unwrap();

    let failures = [
        statement.execute().err().unwrap(),
        statement.execute_update().unwrap_err(),
        statement.execute_partitions().unwrap_err(),
        statement.execute_schema().unwrap_err(),
        statement.get_parameter_schema().unwrap_err(),
    ];
    for failure in failures {
        assert_eq!(failure.status, Status::InvalidState, "{}", failure.message);
        assert_eq!(
            failure.message,
            "BigQuery option bigquery.query.create_session cannot be enabled while \
             adbc.connection.autocommit is disabled: the statement runs in the transaction's \
             BigQuery session; set bigquery.query.create_session to false or re-enable autocommit"
        );
    }
    assert!(
        statement.parameters.schema().is_some(),
        "the binding was consumed"
    );
    assert!(matches!(
        statement.transaction.begin_query().unwrap(),
        QueryTransaction::Start(None, _)
    ));
}

#[test]
fn each_bound_transaction_boundary_starts_the_next_transaction() {
    for boundary in ["COMMIT", "ROLLBACK"] {
        for entry in ["query", "update", "partitions"] {
            let finished = json!({
                "jobComplete": true,
                "statementType": format!("{boundary}_TRANSACTION"),
                "jobReference": {"projectId": "fixture-project", "jobId": "boundary", "location": "EU"}
            });
            let (endpoint, requests) = capture_requests(vec![
                (200, finished.clone()),
                completed_job("transactions-job-11", json!({})),
                (200, finished),
            ]);
            let mut statement = test_statement_at(&endpoint);
            statement.transaction.disable_autocommit().unwrap();
            let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap()
            else {
                panic!("expected a starting transaction")
            };
            start.started(Arc::from("bound-session")).unwrap();
            let sql = format!("{boundary} TRANSACTION");
            statement.set_sql_query(&sql).unwrap();
            statement
                .bind(
                    RecordBatch::try_new_with_options(
                        Arc::new(Schema::empty()),
                        Vec::new(),
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(2)),
                    )
                    .unwrap(),
                )
                .unwrap();

            match entry {
                "query" => {
                    statement
                        .execute()
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                }
                "update" => {
                    statement.execute_update().unwrap();
                }
                "partitions" => {
                    statement.execute_partitions().unwrap();
                }
                _ => unreachable!(),
            }
            let requests = requests.join().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .map(CapturedRequest::sql)
                    .collect::<Vec<_>>(),
                [sql.as_str(), "BEGIN TRANSACTION", sql.as_str()],
                "{entry}"
            );
            for request in &requests {
                assert_eq!(
                    request.body["connectionProperties"],
                    json!([{"key": "session_id", "value": "bound-session"}])
                );
            }
        }
    }
}

#[test]
fn releasing_a_manual_connection_prevents_later_parameter_rows() {
    let (endpoint, requests) = capture_requests(vec![completed_job(
        "transactions-job-12",
        json!({
            "statementType": "SELECT",
            "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "1"}]}]
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement.transaction.disable_autocommit().unwrap();
    let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap() else {
        panic!("expected a starting transaction")
    };
    start.started(Arc::from("bound-session")).unwrap();
    statement.set_sql_query("SELECT ? AS value").unwrap();
    statement.bind(int64_column("value", vec![1, 2])).unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
    statement.transaction.release().unwrap();

    let ArrowError::ExternalError(failure) = reader.next().unwrap().unwrap_err() else {
        panic!("expected transaction state error")
    };
    let failure = failure.downcast_ref::<Error>().unwrap();
    assert_eq!(failure.status, Status::InvalidState);
    assert!(failure.message.contains("released"), "{failure}");
    assert!(failure.message.contains("parameter row 1"), "{failure}");
    assert!(reader.next().is_none());
    assert_eq!(requests.join().unwrap().len(), 1);
}

#[test]
fn a_lazy_parameter_row_joins_the_transaction_after_an_adbc_boundary() {
    for boundary in ["commit", "rollback"] {
        let result = |value| {
            json!({
                "jobReference": job_reference("fixture-project", "transactions-job-13"),
                "statementType": "SELECT",
                "jobComplete": true,
                "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
                "rows": [{"f": [{"v": value}]}]
            })
        };
        let (endpoint, requests) = capture_requests(vec![
            ok(result("1")),
            completed_job("transactions-job-14", json!({})),
            ok(result("2")),
        ]);
        let mut statement = test_statement_at(&endpoint);
        statement.transaction.disable_autocommit().unwrap();
        let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap()
        else {
            panic!("expected a starting transaction")
        };
        start.started(Arc::from("bound-session")).unwrap();
        statement.set_sql_query("SELECT ? AS value").unwrap();
        statement.bind(int64_column("value", vec![1, 2])).unwrap();
        let mut reader = statement.execute().unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);

        // The state transition performed by a successful connection commit/rollback.
        let crate::transaction::FinishTransaction::Active(session, finish) =
            statement.transaction.begin_finish(boundary).unwrap()
        else {
            panic!("expected an active transaction")
        };
        finish.succeeded(session).unwrap();

        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().is_none());
        let requests = requests.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(CapturedRequest::sql)
                .collect::<Vec<_>>(),
            [
                "SELECT ? AS value",
                "BEGIN TRANSACTION",
                "SELECT ? AS value"
            ]
        );
        assert!(matches!(
            statement.transaction.begin_query().unwrap(),
            QueryTransaction::Active(_)
        ));
    }
}
