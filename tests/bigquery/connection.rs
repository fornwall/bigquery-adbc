//! Authentication, URI configuration, sessions and connection-level defaults.

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::Status;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Optionable, Statement};
use adbc_driver_bigquery::{
    BigQueryAuthenticationType, BigQueryCredentialsType, BigQueryDatabaseOption,
    BigQueryStatementOption,
};
use arrow_schema::DataType;

use crate::support::{
    DropTable, SCRATCH_DATASET, SHAKESPEARE_ROWS, TempFile, connect, db_option, execute_failure,
    execute_sql, i64_at, int64_batch, open_database, option_string, prepare_query, project,
    project_and_dataset_options, read_single_i64, read_single_string, scratch_table, set_option,
};

/// BigQuery resolves a numeric project to its assigned ID in returned job references. Both
/// query submission APIs and option-only jobs must keep working after that canonicalization.
#[test]
#[ignore = "requires Google Cloud ADC, BIGQUERY_ADBC_PROJECT_NUMBER and write access to the integration project scratch dataset"]
fn numeric_project_identifiers_work_for_queries_metadata_and_copies() {
    crate::backend::require_real_bigquery();
    let project_number = std::env::var("BIGQUERY_ADBC_PROJECT_NUMBER")
        .expect("BIGQUERY_ADBC_PROJECT_NUMBER must name the BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT project");
    assert!(
        !project_number.is_empty() && project_number.bytes().all(|byte| byte.is_ascii_digit()),
        "BIGQUERY_ADBC_PROJECT_NUMBER must contain decimal digits"
    );
    let database = open_database([
        db_option(BigQueryDatabaseOption::ProjectId, project_number.as_str()),
        db_option(BigQueryDatabaseOption::DatasetId, SCRATCH_DATASET),
    ]);
    let mut connection = database.new_connection().unwrap();
    let mut query = prepare_query(&mut connection, "SELECT @@project_id");
    assert_eq!(read_single_string(&mut query), project());

    let table = scratch_table("numeric_project");
    let copied = scratch_table("numeric_project_copy");
    let _drop_table = DropTable::table(&table);
    let _drop_copy = DropTable::table(&copied);
    let mut destination = prepare_query(&mut connection, "SELECT 42 AS answer");
    set_option(
        &mut destination,
        BigQueryStatementOption::DestinationTable,
        format!("{project_number}.{SCRATCH_DATASET}.{table}"),
    );
    assert!(destination.execute().unwrap().next().is_none());

    for catalog in [None, Some(project_number.as_str())] {
        let schema = connection
            .get_table_schema(catalog, Some(SCRATCH_DATASET), &table)
            .unwrap();
        assert_eq!(schema.field(0).name(), "answer");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    // Unqualified names use the numeric current catalog. With no configured location, the copy
    // also looks up that project's dataset location before submitting its job.
    let mut copy = connection.new_statement().unwrap();
    set_option(&mut copy, BigQueryStatementOption::CopyTableSource, table);
    set_option(
        &mut copy,
        BigQueryStatementOption::CopyTableDestination,
        copied.clone(),
    );
    copy.execute_update().unwrap();
    let mut verify = prepare_query(&mut connection, &format!("SELECT answer FROM `{copied}`"));
    assert_eq!(read_single_i64(&mut verify), 42);
}

/// `SESSION_USER()` on the integration project, authenticated by `options`.
#[track_caller]
fn session_user(
    what: &str,
    options: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
) -> String {
    let mut connection = open_database(
        [db_option(BigQueryDatabaseOption::ProjectId, project())]
            .into_iter()
            .chain(options),
    )
    .new_connection()
    .unwrap_or_else(|error| panic!("{what}: {error}"));
    read_single_string(&mut prepare_query(&mut connection, "SELECT SESSION_USER()"))
}

#[test]
#[ignore = "requires discrete Google OAuth user credentials and access to the integration project"]
fn queries_with_discrete_user_oauth_credentials() {
    crate::backend::require_real_bigquery();
    let credential = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let client_id = credential("BIGQUERY_ADBC_OAUTH_CLIENT_ID");
    let client_secret = credential("BIGQUERY_ADBC_OAUTH_CLIENT_SECRET");
    let refresh_token = credential("BIGQUERY_ADBC_OAUTH_REFRESH_TOKEN");
    let authorized_user = serde_json::json!({
        "type": "authorized_user",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    });

    let expected_principal = session_user(
        "discrete OAuth credentials",
        [
            db_option(
                BigQueryDatabaseOption::AuthenticationType,
                BigQueryAuthenticationType::UserAuthentication,
            ),
            db_option(BigQueryDatabaseOption::AuthenticationClientId, client_id),
            db_option(
                BigQueryDatabaseOption::AuthenticationClientSecret,
                client_secret,
            ),
            db_option(
                BigQueryDatabaseOption::AuthenticationRefreshToken,
                refresh_token,
            ),
        ],
    );
    assert!(!expected_principal.is_empty());

    let document = authorized_user.to_string();
    // The file holds a refresh token, so it is private and removed on unwind.
    let file = TempFile::new(
        &format!("{}.json", scratch_table("oauth")),
        document.as_bytes(),
    );
    for (credential_form, authentication_type, credentials) in [
        (
            "authorized-user JSON credential file",
            BigQueryAuthenticationType::JsonCredentialFile,
            file.0.to_str().expect("temporary credential path is UTF-8"),
        ),
        (
            "authorized-user JSON credential string",
            BigQueryAuthenticationType::JsonCredentialString,
            document.as_str(),
        ),
    ] {
        let principal = session_user(
            credential_form,
            [
                db_option(
                    BigQueryDatabaseOption::AuthenticationType,
                    authentication_type,
                ),
                db_option(
                    BigQueryDatabaseOption::AuthenticationCredentials,
                    credentials,
                ),
                db_option(
                    BigQueryDatabaseOption::AuthenticationCredentialsType,
                    BigQueryCredentialsType::AuthorizedUser,
                ),
            ],
        );
        assert_eq!(principal, expected_principal, "{credential_form}");
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let access_token = runtime.block_on(async {
        let credentials =
            google_cloud_auth::credentials::user_account::Builder::new(authorized_user)
                .build_access_token_credentials()
                .unwrap_or_else(|_| panic!("could not build the OAuth token provider"));
        credentials
            .access_token()
            .await
            // Upstream failures may echo credential material.
            .unwrap_or_else(|_| panic!("could not refresh the OAuth access token"))
            .token
    });
    let principal = session_user(
        "temporary access token",
        [
            db_option(
                BigQueryDatabaseOption::AuthenticationType,
                BigQueryAuthenticationType::TemporaryAccessToken,
            ),
            db_option(
                BigQueryDatabaseOption::AuthenticationAccessToken,
                access_token,
            ),
        ],
    );
    assert_eq!(principal, expected_principal);
}

#[test]
#[ignore = "requires a service-account JSON key and access to the integration project"]
fn queries_with_service_account_key_file_and_json_string() {
    crate::backend::require_real_bigquery();
    let key_path = std::env::var("BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE")
        .expect("BIGQUERY_ADBC_SERVICE_ACCOUNT_KEY_FILE must be set");
    let key_json = std::fs::read_to_string(&key_path).unwrap();
    let key: serde_json::Value = serde_json::from_str(&key_json).unwrap();
    let expected_principal = key
        .get("client_email")
        .and_then(serde_json::Value::as_str)
        .expect("the service-account key must contain client_email")
        .to_owned();

    for (credential_form, authentication_type, credentials) in [
        (
            "JSON credential file",
            BigQueryAuthenticationType::JsonCredentialFile,
            key_path,
        ),
        (
            "JSON credential string",
            BigQueryAuthenticationType::JsonCredentialString,
            key_json,
        ),
    ] {
        let principal = session_user(
            credential_form,
            [
                db_option(
                    BigQueryDatabaseOption::AuthenticationType,
                    authentication_type,
                ),
                db_option(
                    BigQueryDatabaseOption::AuthenticationCredentials,
                    credentials,
                ),
                db_option(
                    BigQueryDatabaseOption::AuthenticationCredentialsType,
                    BigQueryCredentialsType::ServiceAccount,
                ),
            ],
        );
        assert_eq!(principal, expected_principal, "{credential_form}");
    }
}

/// BigQuery uses the project and location parsed from the URI: a `US` dataset is reachable from
/// `US` but not from `EU`.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn queries_through_rich_uri_endpoint_configuration() {
    let project = project();
    let uri = |location| {
        open_database([(
            OptionDatabase::Uri,
            format!("bigquery://bigquery.googleapis.com/{project}?OAuthType=0&Location={location}")
                .into(),
        )])
        .new_connection()
        .unwrap()
    };
    const US_ONLY: &str = "SELECT COUNT(*) FROM `bigquery-public-data.samples.shakespeare`";

    let mut connection = uri("US");
    let mut statement = prepare_query(&mut connection, "SELECT @@project_id");
    assert_eq!(read_single_string(&mut statement), project);
    let mut statement = prepare_query(&mut connection, US_ONLY);
    assert_eq!(read_single_i64(&mut statement), SHAKESPEARE_ROWS);

    // BigQuery reports a dataset in another location as a permission error, not a location error.
    let mut connection = uri("EU");
    let mut statement = prepare_query(&mut connection, US_ONLY);
    let (status, message) = execute_failure(&mut statement);
    assert_eq!(status, Status::Unauthorized, "{message}");
    assert!(message.contains("samples.shakespeare"), "{message}");
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn manages_transactions_in_a_bigquery_session() {
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, "SELECT @@session_id AS session_id");

    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );
    set_option(
        &mut connection,
        OptionConnection::AutoCommit,
        ADBC_OPTION_VALUE_DISABLED,
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_DISABLED
    );

    let first_session = read_single_string(&mut statement);
    assert!(!first_session.is_empty());

    statement
        .set_sql_query("CREATE TEMP TABLE adbc_transaction_values (value INT64)")
        .unwrap();
    statement.execute_update().unwrap();
    connection.commit().unwrap();

    statement
        .set_sql_query("INSERT adbc_transaction_values VALUES (42)")
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(1));
    // A failed statement leaves BigQuery's transaction, and the work already done in it, open,
    // and a statement created during the transaction joins it.
    statement.set_sql_query("SELECT ERROR('fails')").unwrap();
    assert_eq!(
        statement.execute_update().unwrap_err().status,
        Status::InvalidArguments
    );
    let mut joined = prepare_query(
        &mut connection,
        "INSERT adbc_transaction_values VALUES (43)",
    );
    assert_eq!(joined.execute_update().unwrap(), Some(1));
    statement
        .set_sql_query("SELECT COUNT(*) FROM adbc_transaction_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 2);
    connection.rollback().unwrap();

    statement
        .set_sql_query("SELECT COUNT(*) FROM adbc_transaction_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 0);
    statement
        .set_sql_query("INSERT adbc_transaction_values VALUES (7)")
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(1));
    connection.commit().unwrap();

    statement
        .set_sql_query("SELECT value FROM adbc_transaction_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 7);

    // The caller's own COMMIT statement ends BigQuery's transaction; the next statement begins a
    // new one, so a later rollback still has a transaction to undo.
    statement.set_sql_query("COMMIT TRANSACTION").unwrap();
    statement.execute_update().unwrap();
    statement
        .set_sql_query("INSERT adbc_transaction_values VALUES (8)")
        .unwrap();
    statement.execute_update().unwrap();
    connection.rollback().unwrap();
    statement
        .set_sql_query("SELECT COUNT(*) FROM adbc_transaction_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 1);

    // Later parameter rows must enter a new transaction after the consumer commits.
    statement
        .set_sql_query("INSERT adbc_transaction_values VALUES (?); SELECT COUNT(*) FROM adbc_transaction_values")
        .unwrap();
    statement.bind(int64_batch("value", vec![8, 9])).unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(i64_at(&reader.next().unwrap().unwrap(), 0), 2);
    connection.commit().unwrap();
    assert_eq!(i64_at(&reader.next().unwrap().unwrap(), 0), 3);
    assert!(reader.next().is_none());
    connection.rollback().unwrap();
    statement
        .set_sql_query("SELECT COUNT(*) FROM adbc_transaction_values")
        .unwrap();
    assert_eq!(read_single_i64(&mut statement), 2);

    statement
        .set_sql_query("SELECT @@session_id AS session_id")
        .unwrap();
    assert_eq!(read_single_string(&mut statement), first_session);

    set_option(
        &mut connection,
        OptionConnection::AutoCommit,
        ADBC_OPTION_VALUE_ENABLED,
    );
    assert_eq!(
        option_string(&connection, OptionConnection::AutoCommit),
        ADBC_OPTION_VALUE_ENABLED
    );
    assert_eq!(
        connection.commit().unwrap_err().status,
        Status::InvalidState
    );
    assert_eq!(
        connection.rollback().unwrap_err().status,
        Status::InvalidState
    );
}

/// A committed transaction leaves its session for the next one. Once BigQuery ends that session
/// (here through `BQ.ABORT_SESSION`; idle sessions end after 24 hours), the next transaction
/// begins in a new session instead of failing.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn begins_the_next_transaction_in_a_new_session_once_the_last_one_ended() {
    const SESSION: &str = "SELECT @@session_id AS session_id";
    /// Retries `ended` once a second until it holds; BigQuery ends a session asynchronously.
    fn wait_until(mut ended: impl FnMut() -> bool) {
        let ended = (0..10).any(|attempt| {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            ended()
        });
        assert!(ended, "BigQuery did not end the aborted session");
    }
    let abort = |session: &str| {
        execute_sql(
            &mut connect(project()),
            &format!("CALL BQ.ABORT_SESSION('{session}')"),
        );
    };
    let mut connection = connect(project());
    set_option(
        &mut connection,
        OptionConnection::AutoCommit,
        ADBC_OPTION_VALUE_DISABLED,
    );
    let mut statement = prepare_query(
        &mut connection,
        "CREATE TEMP TABLE adbc_ended_session_marker AS SELECT 1 AS value",
    );
    statement.execute_update().unwrap();
    statement.set_sql_query(SESSION).unwrap();
    let first = read_single_string(&mut statement);
    connection.commit().unwrap();

    // A dry run of the session's temporary table describes it until the session ends. Then the
    // dry run is repeated outside the session, where the table does not exist, and the session is
    // forgotten, so the next transaction cannot reach it.
    statement
        .set_sql_query("SELECT value FROM adbc_ended_session_marker")
        .unwrap();
    statement.execute_schema().unwrap();
    abort(&first);
    wait_until(|| statement.execute_schema().is_err());
    statement.set_sql_query(SESSION).unwrap();
    let second = read_single_string(&mut statement);
    assert_ne!(second, first);
    connection.commit().unwrap();

    // Without a dry run first, `BEGIN` goes to the ended session. BigQuery refuses it, or accepts
    // it just before ending the session and refuses the statement after it; either way the
    // transaction begins in a new session. Until the session ends, both run in it, and the
    // session may end before the commit, which then reports the transaction lost.
    abort(&second);
    wait_until(|| {
        if read_single_string(&mut statement) != second {
            return true;
        }
        if let Err(failure) = connection.commit() {
            assert!(failure.message.contains("has expired"), "{failure}");
        }
        false
    });
    connection.commit().unwrap();
}

/// Without `bigquery.location`, BigQuery would create the transaction session in the US, where a
/// query on this EU dataset fails with `Access Denied`; the session follows the default dataset.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn creates_the_transaction_session_where_the_default_dataset_is() {
    let mut connection = connect(project());
    set_option(
        &mut connection,
        OptionConnection::CurrentCatalog,
        "bigquery-public-data",
    );
    set_option(
        &mut connection,
        OptionConnection::CurrentSchema,
        "covid19_ecdc_eu",
    );
    set_option(
        &mut connection,
        OptionConnection::AutoCommit,
        ADBC_OPTION_VALUE_DISABLED,
    );
    let mut statement = prepare_query(
        &mut connection,
        "SELECT COUNT(*) FROM covid_19_geographic_distribution_worldwide",
    );
    assert!(read_single_i64(&mut statement) > 0);
    connection.commit().unwrap();
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn uses_current_catalog_and_schema_as_query_defaults() {
    let database = open_database(project_and_dataset_options());
    assert_eq!(
        option_string(&database, BigQueryDatabaseOption::DatasetId),
        SCRATCH_DATASET
    );

    let mut connection = database.new_connection().unwrap();
    // Created before the namespace changes, as a Python dbapi cursor is: it must still resolve
    // `shakespeare` in the namespace current when it executes.
    let mut early = prepare_query(&mut connection, "SELECT COUNT(*) FROM shakespeare");
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentCatalog),
        project()
    );
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentSchema),
        SCRATCH_DATASET
    );
    set_option(
        &mut connection,
        OptionConnection::CurrentCatalog,
        "bigquery-public-data",
    );
    set_option(&mut connection, OptionConnection::CurrentSchema, "samples");
    // Project IDs cannot contain `google`, so this one can never exist and turn the 404 into a 403.
    assert_eq!(
        connection
            .set_option(
                OptionConnection::CurrentCatalog,
                "google-adbc-no-such-catalog".into(),
            )
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentCatalog),
        "bigquery-public-data"
    );
    assert_eq!(
        connection
            .set_option(
                OptionConnection::CurrentSchema,
                "bigquery_adbc_missing_dataset".into(),
            )
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentSchema),
        "samples"
    );

    let table_schema = connection
        .get_table_schema(None, None, "shakespeare")
        .unwrap();
    assert_eq!(table_schema.field(0).name(), "word");

    let mut statement = prepare_query(&mut connection, "SELECT COUNT(*) FROM shakespeare");
    assert_eq!(
        statement.execute_schema().unwrap().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(read_single_i64(&mut statement), SHAKESPEARE_ROWS);
    assert_eq!(read_single_i64(&mut early), SHAKESPEARE_ROWS);

    // Clearing the catalog makes unqualified names resolve in the billing project, which is then
    // what the current catalog reads back as.
    set_option(
        &mut connection,
        OptionConnection::CurrentCatalog,
        String::new(),
    );
    assert_eq!(
        option_string(&connection, OptionConnection::CurrentCatalog),
        project()
    );
}

/// An unqualified ingest target lands where the statement's unqualified SQL names resolve: its
/// default dataset shadows the connection's, which here has none.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project scratch dataset"]
fn ingest_target_follows_the_statement_default_dataset() {
    let mut connection = connect(project());
    let mut ingest = connection.new_statement().unwrap();
    set_option(
        &mut ingest,
        BigQueryStatementOption::DefaultDatasetId,
        SCRATCH_DATASET,
    );
    set_option(
        &mut ingest,
        OptionStatement::TargetTable,
        scratch_table("statement_default_ingest"),
    );
    // A dry run inspects the target and writes nothing, so no table is left behind.
    set_option(
        &mut ingest,
        BigQueryStatementOption::DryRun,
        ADBC_OPTION_VALUE_ENABLED,
    );
    ingest.bind(int64_batch("id", vec![1])).unwrap();
    assert_eq!(ingest.execute_update().unwrap(), Some(0));
}

/// Connections share no per-query state; dbt 2.0 runs one connection per worker thread.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn queries_concurrently_on_independent_connections() {
    let results = std::thread::scope(|scope| {
        #[allow(
            clippy::needless_collect,
            reason = "all workers must be spawned before the first one is joined"
        )]
        let running = (1..=4)
            .map(|n| {
                scope.spawn(move || {
                    let mut connection = connect(project());
                    let mut statement =
                        prepare_query(&mut connection, &format!("SELECT {n} * 7 AS value"));
                    read_single_i64(&mut statement)
                })
            })
            .collect::<Vec<_>>();
        running
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results, [7, 14, 21, 28]);
}
