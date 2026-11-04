//! Explicit quota-project precedence at the authenticated HTTP request boundary.

use adbc_core::options::OptionDatabase;
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable as _};
use google_cloud_auth::credentials::CacheableResource;
use http::Extensions;
use serde_json::json;

use super::*;
use crate::BigQueryDriver;
use crate::test_support::credentials::TEST_PRIVATE_KEY;
use crate::test_support::http::capture_requests;

const ADC_CHILD: &str = "BIGQUERY_ADBC_QUOTA_TEST_CHILD";
const QUOTA_ENV: &str = "GOOGLE_CLOUD_QUOTA_PROJECT";

fn assert_adc_quota_projects() {
    let path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();
    let ambient = std::env::var(QUOTA_ENV).ok();
    for credential_type in [
        "service_account",
        "authorized_user",
        "impersonated_service_account",
    ] {
        for configured in [None, Some("profile-quota")] {
            for authentication_type in [None, Some("adbc.bigquery.sql.auth_type.auth_bigquery")] {
                let mut responses = Vec::new();
                if credential_type != "service_account" {
                    responses.push((
                        200,
                        json!({"access_token": "user-token", "expires_in": 3600, "token_type": "Bearer"}),
                    ));
                }
                if credential_type == "impersonated_service_account" {
                    responses.push((
                        200,
                        json!({"accessToken": "target-token", "expireTime": "2099-01-01T00:00:00Z"}),
                    ));
                }
                responses.push((
                    200,
                    json!({"type": "TABLE", "schema": {"fields": [{"name": "id", "type": "INTEGER"}]}}),
                ));
                let (endpoint, server) = capture_requests(responses);
                let user = json!({
                    "type": "authorized_user", "client_id": "client-id",
                    "client_secret": "client-secret", "refresh_token": "refresh-token",
                    "token_uri": format!("{endpoint}/token"), "quota_project_id": "file-quota",
                });
                let document = match credential_type {
                    "service_account" => json!({
                        "type": "service_account", "project_id": "data-project",
                        "private_key_id": "synthetic-quota-test-key", "private_key": TEST_PRIVATE_KEY,
                        "client_email": "quota-test@data-project.iam.gserviceaccount.com",
                    }),
                    "authorized_user" => user,
                    _ => json!({
                        "type": "impersonated_service_account", "source_credentials": user,
                        "quota_project_id": "file-quota",
                        "service_account_impersonation_url": format!(
                            "{endpoint}/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken"
                        ),
                    }),
                };
                // Only this isolated child uses the fixture ADC file, so each connection can
                // exercise SDK discovery with a different credential type and mock endpoint.
                std::fs::write(&path, document.to_string()).unwrap();
                let mut driver = BigQueryDriver::try_new().unwrap();
                let mut database = driver.new_database().unwrap();
                for (key, value) in [
                    ("adbc.bigquery.sql.project_id", "data-project"),
                    ("adbc.bigquery.sql.api_endpoint", endpoint.as_str()),
                    ("bigquery.allow_untrusted_endpoint", "true"),
                ] {
                    database
                        .set_option(OptionDatabase::Other(key.to_owned()), value.into())
                        .unwrap();
                }
                for (key, value) in [
                    ("adbc.bigquery.sql.auth_type", authentication_type),
                    ("adbc.bigquery.sql.auth.quota_project", configured),
                ] {
                    if let Some(value) = value {
                        database
                            .set_option(OptionDatabase::Other(key.to_owned()), value.into())
                            .unwrap();
                    }
                }
                database
                    .new_connection()
                    .unwrap()
                    .get_table_schema(Some("data-project"), Some("dataset"), "table")
                    .unwrap();
                let requests = server.join().unwrap();
                let request = requests.last().unwrap();
                let expected = configured
                    .or(ambient.as_deref())
                    .or_else(|| (credential_type != "service_account").then_some("file-quota"));
                assert_eq!(
                    request.header("x-goog-user-project"),
                    expected,
                    "{credential_type}, configured={configured:?}, ambient={ambient:?}"
                );
                let authorization = request.header("authorization").unwrap();
                match credential_type {
                    "authorized_user" => assert_eq!(authorization, "Bearer user-token"),
                    "impersonated_service_account" => {
                        assert_eq!(authorization, "Bearer target-token");
                        assert_eq!(
                            requests[1].header("authorization"),
                            Some("Bearer user-token")
                        );
                    }
                    _ => assert!(authorization.starts_with("Bearer ey")),
                }
            }
        }
    }
}

/// A subprocess isolates both ADC discovery and the environment variable from parallel tests.
#[test]
fn explicit_adc_quota_project_overrides_environment_on_requests() {
    if std::env::var_os(ADC_CHILD).is_some() {
        assert_adc_quota_projects();
        return;
    }
    let file = TemporaryFile::new("quota-project-adc", "{}");
    for ambient in [None, Some("ambient-quota"), Some("profile-quota")] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "config::tests::quota_project::explicit_adc_quota_project_overrides_environment_on_requests",
            ])
            .env(ADC_CHILD, "1")
            .env("GOOGLE_APPLICATION_CREDENTIALS", file.path());
        match ambient {
            Some(project) => child.env(QUOTA_ENV, project),
            None => child.env_remove(QUOTA_ENV),
        };
        assert!(child.status().unwrap().success(), "ambient={ambient:?}");
    }
}

/// Rewriting a header must leave SDK token refresh and cache validation intact.
#[test]
fn quota_override_preserves_refresh_cache_tags_and_universe() {
    let (endpoint, server) = capture_requests(
        ["first-token", "refreshed-token"]
            .map(|token| {
                (
                    200,
                    json!({"access_token": token, "expires_in": 3600, "token_type": "Bearer"}),
                )
            })
            .into(),
    );
    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let inhibitor = tokio::task::spawn_blocking(move || held.recv());
        let source = google_cloud_auth::credentials::user_account::Builder::new(json!({
            "type": "authorized_user", "client_id": "client-id",
            "client_secret": "client-secret", "refresh_token": "refresh-token",
            "quota_project_id": "ambient-quota",
        }))
        .with_token_uri(format!("{endpoint}/token"))
        .build()
        .unwrap();
        let credentials =
            crate::config::credentials::override_quota_project(source, "profile-quota").unwrap();
        assert_eq!(
            credentials.universe_domain().await.as_deref(),
            Some("googleapis.com")
        );
        let CacheableResource::New { entity_tag, data } =
            credentials.headers(Extensions::new()).await.unwrap()
        else {
            panic!("expected initial headers")
        };
        assert_eq!(data["x-goog-user-project"], "profile-quota");
        assert_eq!(data[http::header::AUTHORIZATION], "Bearer first-token");
        let mut extensions = Extensions::new();
        extensions.insert(entity_tag.clone());
        assert!(matches!(
            credentials.headers(extensions.clone()).await.unwrap(),
            CacheableResource::NotModified
        ));
        tokio::time::advance(Duration::from_secs(3_601)).await;
        let CacheableResource::New {
            entity_tag: refreshed,
            data,
        } = credentials.headers(extensions).await.unwrap()
        else {
            panic!("expected refreshed headers")
        };
        assert_ne!(entity_tag, refreshed);
        assert_eq!(data["x-goog-user-project"], "profile-quota");
        assert_eq!(data[http::header::AUTHORIZATION], "Bearer refreshed-token");
        drop(hold);
        let _ = inhibitor.await;
    });
    assert_eq!(server.join().unwrap().len(), 2);
}
