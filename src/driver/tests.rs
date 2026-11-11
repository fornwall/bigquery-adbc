//! Driver and database tests, split by subject.
//!
//! Shared imports and fixtures live here; each submodule covers one subject.

use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};

use super::{BigQueryDatabase, BigQueryDriver, new_runtime};
use crate::config::ServiceEndpointKind;
use crate::{BigQueryAuthenticationType, BigQueryCredentialsType, BigQueryDatabaseOption};

mod credentials;
mod dbt;
mod lifecycle;
mod options;
mod uri;

/// Its `token_uri` is [`UNREACHABLE_ENDPOINT`](crate::test_support::UNREACHABLE_ENDPOINT): a
/// built client may mint a token in the background, which must not reach Google. A test building
/// clients from it therefore sets `bigquery.allow_untrusted_endpoint`.
const AUTHORIZED_USER_JSON: &str = r#"{
    "type": "authorized_user",
    "client_id": "fixture-client-id",
    "client_secret": "fixture-client-secret",
    "refresh_token": "fixture-refresh-token",
    "token_uri": "http://127.0.0.1:1/token"
}"#;

/// A database on its own runtime, with no option set.
pub(super) fn new_database() -> BigQueryDatabase {
    BigQueryDatabase::new(
        new_runtime().unwrap(),
        crate::rest::client::IdlePools::new(),
    )
}

/// Builds every client the database can hand out and asserts the cache saw them all. The Storage
/// Read client is skipped because building it opens a connection.
fn assert_all_clients_build(database: &BigQueryDatabase) {
    let state = database.clients().clone();
    state.rest_job_client(&database.runtime).unwrap();
    state.metadata_client(&database.runtime).unwrap();
    state.rest_client().unwrap();
    state.write_client(&database.runtime, None).unwrap();
    assert_eq!(
        state.initialized_clients(),
        crate::clients::InitializedClients {
            configuration: true,
            metadata: true,
            rest: true,
            jobs: true,
            write: true,
            read: false,
        }
    );
}
