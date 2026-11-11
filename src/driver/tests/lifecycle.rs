//! The runtime one driver shares with every object below it, and the client generation a
//! database replaces atomically.

use super::*;
use crate::test_support::options::set_option;

#[test]
fn driver_propagates_one_runtime_to_databases() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let first = driver.new_database().unwrap();
    let second = driver.new_database().unwrap();
    assert!(Arc::ptr_eq(&driver.runtime, &first.runtime));
    assert!(Arc::ptr_eq(&first.runtime, &second.runtime));
}

/// The C ABI's driver is a static a `fork` child inherits. A database opened there must get a
/// runtime with workers, through either constructor, and must not disturb the inherited one.
#[test]
fn a_forked_child_opens_databases_on_a_runtime_of_its_own() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    for options in [None, Some(Vec::new())] {
        let inherited = crate::runtime::orphaned_runtime();
        driver.runtime = inherited.clone();
        let database = match options {
            None => driver.new_database(),
            Some(options) => driver.new_database_with_opts(options),
        }
        .unwrap();
        assert!(!Arc::ptr_eq(&database.runtime, &inherited));
        assert_eq!(database.runtime.block_on(async { 1 }), 1);
    }
}

#[test]
fn anonymous_mock_configuration_builds_every_client_and_invalidates_atomically() {
    let mut database = new_database();
    database.config.authentication_type = BigQueryAuthenticationType::Anonymous;
    database.config.endpoints.set(
        ServiceEndpointKind::BigQuery,
        Some(Arc::from("http://127.0.0.1:1")),
    );
    database.config.endpoints.set(
        ServiceEndpointKind::Storage,
        Some(Arc::from("http://127.0.0.1:2")),
    );
    database.invalidate_clients();

    let original = database.clients().clone();
    assert_all_clients_build(&database);

    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::ApplicationDefault,
    );
    assert!(!Arc::ptr_eq(&original, database.clients()));
    assert_eq!(
        database.clients().initialized_clients(),
        crate::clients::InitializedClients::default()
    );
    assert!(original.initialized_clients().configuration);
}

#[test]
fn warmed_job_clients_keep_their_configuration_after_database_changes() {
    use crate::rest::jobs::JobReference;
    use crate::test_support::http::{capture_requests, ok};

    let (first_endpoint, first_requests) = capture_requests(vec![ok(serde_json::json!({})); 2]);
    let (second_endpoint, second_requests) = capture_requests(vec![ok(serde_json::json!({}))]);
    let mut database = new_database();
    set_option(
        &mut database,
        BigQueryDatabaseOption::AuthenticationType,
        BigQueryAuthenticationType::Anonymous,
    );
    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        first_endpoint,
    );
    let original = database.clients().clone();
    let first_client = original.rest_job_client(&database.runtime).unwrap();

    set_option(
        &mut database,
        BigQueryDatabaseOption::Endpoint,
        second_endpoint,
    );
    let reference = JobReference::new("billing-project", Some("US"), "job".to_owned());
    for client in [
        first_client,
        original.rest_job_client(&database.runtime).unwrap(),
        database
            .clients()
            .rest_job_client(&database.runtime)
            .unwrap(),
    ] {
        database
            .runtime
            .block_on(client.poll(None, &reference, "configuration test"))
            .unwrap();
    }
    for requests in [first_requests, second_requests] {
        for request in requests.join().unwrap() {
            assert_eq!(
                request.path(),
                "/bigquery/v2/projects/billing-project/jobs/job?location=US"
            );
        }
    }
}

/// A database opened after another closed reuses the connection its REST pool left idle, as one
/// per Python DB-API `connect()` does, while two open together keep connections of their own.
#[test]
fn a_database_reuses_the_connections_a_closed_one_left_idle() {
    use crate::rest::request::{Request, send};

    let (endpoint, finish) = crate::test_support::http::keep_alive_server();
    let mut driver = BigQueryDriver::try_new().unwrap();
    let mut open = || {
        let mut database = driver.new_database().unwrap();
        set_option(
            &mut database,
            BigQueryDatabaseOption::AuthenticationType,
            BigQueryAuthenticationType::Anonymous,
        );
        database
    };
    let get = |database: &BigQueryDatabase| {
        let state = database.clients();
        let configuration = state.configuration(&database.runtime).unwrap();
        database
            .runtime
            .block_on(send(
                None,
                &configuration.credentials,
                &state.rest_client().unwrap(),
                Request::get(&format!("{endpoint}/ping"), "ping"),
            ))
            .unwrap();
    };

    let first = open();
    get(&first);
    drop(first);
    let (second, third) = (open(), open());
    get(&second);
    get(&third);
    drop((second, third));

    let connections: Vec<_> = finish()
        .into_iter()
        .map(|(connection, _)| connection)
        .collect();
    assert_eq!(connections, [0, 0, 1]);
}
