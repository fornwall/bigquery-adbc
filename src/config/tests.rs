//! Configuration tests, split by subject.
//!
//! Shared imports and fixtures live here; each submodule covers one subject.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::Status;
use adbc_core::options::OptionDatabase;
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable as _};

use super::{
    BigQueryAuthenticationType, BigQueryCredentialsType, ClientConfiguration, DatabaseConfig,
    Secret, ServiceEndpointKind,
};
use crate::identifiers::ProjectIdentifier;

mod authentication;
mod credentials;
mod impersonation;
mod proxy;
mod quota_project;
mod scopes;
mod trust;

fn settings(configure: impl FnOnce(&mut DatabaseConfig)) -> DatabaseConfig {
    let mut config = DatabaseConfig::default();
    configure(&mut config);
    config
}

/// A file holding `contents`, removed on drop even when an assertion fails. Lib tests share one
/// process, so the name carries the test's label and the process id. Write and removal failures
/// name the path.
struct TemporaryFile(std::path::PathBuf);

impl TemporaryFile {
    fn new(label: &str, contents: impl AsRef<[u8]>) -> Self {
        let path =
            std::env::temp_dir().join(format!("bigquery-adbc-{label}-{}.json", std::process::id()));
        std::fs::write(&path, contents).unwrap_or_else(|source| {
            panic!("failed to write the test credential file {path:?}: {source}")
        });
        Self(path)
    }

    fn path(&self) -> &str {
        self.0
            .to_str()
            .expect("the temporary directory path is valid UTF-8")
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        // A cleanup failure must report itself rather than panic: dropping while a failed
        // assertion unwinds would abort the whole test binary and hide that assertion.
        if let Err(source) = std::fs::remove_file(&self.0) {
            eprintln!(
                "failed to remove the test credential file {:?}: {source}",
                self.0
            );
        }
    }
}

/// A service-account key with the given email, whose private key is never used: building the
/// credentials only deserializes the document, and no test here mints a token from it.
fn service_account_key(client_email: &str) -> String {
    serde_json::json!({
        "type": "service_account",
        "project_id": "data-project",
        "private_key_id": "0123456789abcdef",
        "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
        "client_email": client_email,
    })
    .to_string()
}

/// Settings that authenticate with `document` as a JSON credential string.
fn json_credentials(document: impl Into<Secret>) -> DatabaseConfig {
    settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials = Some(document.into());
    })
}

/// User authentication whose refresh-token exchange goes to `token_endpoint`, with
/// `bigquery.allow_untrusted_endpoint` set to `opt_in`.
fn user_authentication_via(token_endpoint: &str, opt_in: bool) -> DatabaseConfig {
    settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::UserAuthentication;
        config.user_authentication.client_id = Some(Arc::from("client-id"));
        config.user_authentication.client_secret = Some(Secret::from("client-secret"));
        config.user_authentication.refresh_token = Some(Secret::from("refresh-token"));
        config.user_authentication.access_token_endpoint = Some(Arc::from(token_endpoint));
        config.allow_untrusted_endpoint = opt_in;
    })
}

/// Settings that authenticate with the static access token `token`.
fn temporary_access_token(token: &str) -> DatabaseConfig {
    settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::TemporaryAccessToken;
        config.access_token = Some(Secret::from(token));
    })
}

/// The `tables.get` answer [`read_table_schema`] reads.
fn table_resource() -> (u16, serde_json::Value) {
    crate::test_support::http::ok(serde_json::json!({
        "type": "TABLE", "schema": {"fields": [{"name": "id", "type": "INTEGER"}]}
    }))
}

/// Reads the schema of `data-project.dataset.table` through a database configured by raw option
/// keys, as a dbt profile configures one.
fn read_table_schema(options: &[(&str, &str)]) {
    let mut driver = crate::BigQueryDriver::try_new().unwrap();
    let mut database = driver.new_database().unwrap();
    for &(key, value) in options {
        database
            .set_option(OptionDatabase::Other(key.to_owned()), value.into())
            .unwrap();
    }
    let schema = database
        .new_connection()
        .unwrap()
        .get_table_schema(Some("data-project"), Some("dataset"), "table")
        .unwrap();
    assert_eq!(schema.fields().len(), 1);
}
