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
    use crate::test_support::http::capture_requests;

    let (first_endpoint, first_requests) = capture_requests(vec![(200, serde_json::json!({})); 2]);
    let (second_endpoint, second_requests) = capture_requests(vec![(200, serde_json::json!({}))]);
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
