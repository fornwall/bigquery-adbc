//! The scopes sent by credentials configured through the driver and dbt's option aliases.

use base64::Engine as _;
use google_cloud_auth::credentials::CacheableResource;
use serde_json::{Value, json};

use super::*;
use crate::test_support::credentials::{authorized_user_json, service_account_json, token_grant};
use crate::test_support::http::capture_requests;

const DBT_SCOPES: &str = "https://www.googleapis.com/auth/bigquery,\
                        https://www.googleapis.com/auth/cloud-platform,\
                        https://www.googleapis.com/auth/drive,\
                        https://www.googleapis.com/auth/userinfo.email";
const CLOUD_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive";
const ADC_CHILD: &str = "BIGQUERY_ADBC_SCOPE_TEST_CHILD";

#[test]
fn user_refresh_requests_send_the_configured_scope_subset() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for authentication_type in [
        BigQueryAuthenticationType::UserAuthentication,
        BigQueryAuthenticationType::JsonCredentialString,
        BigQueryAuthenticationType::JsonCredentialFile,
    ] {
        for (fallback, explicit, expected) in [
            ("", "", None),
            (DBT_SCOPES, "", Some(DBT_SCOPES)),
            (DBT_SCOPES, DRIVE_SCOPE, Some(DRIVE_SCOPE)),
        ] {
            let (endpoint, server) = capture_requests(vec![token_grant("user-token")]);
            let mut document = authorized_user_json();
            document["token_uri"] = json!(endpoint);
            let document = document.to_string();
            let file = TemporaryFile::new("user-refresh-scopes", &document);
            let mut config = if authentication_type
                == BigQueryAuthenticationType::UserAuthentication
            {
                user_authentication_via(&endpoint, true)
            } else {
                settings(|config| {
                    config.authentication_type = authentication_type;
                    config.allow_untrusted_endpoint = true;
                    config.credentials = Some(Secret::from(
                        if authentication_type == BigQueryAuthenticationType::JsonCredentialFile {
                            file.path()
                        } else {
                            &document
                        },
                    ));
                })
            };
            config.scopes = explicit
                .split(',')
                .filter(|s| !s.is_empty())
                .map(Arc::from)
                .collect();
            config.impersonation.scopes = fallback
                .split(',')
                .filter(|s| !s.is_empty())
                .map(Arc::from)
                .collect();
            let configuration = ClientConfiguration::build(&config).unwrap();
            runtime
                .block_on(configuration.credentials.headers(http::Extensions::new()))
                .unwrap();
            let requests = server.served(1);
            assert_eq!(
                requests[0].body.get("scope").and_then(Value::as_str),
                expected.map(|scopes| scopes.replace(',', " ")).as_deref(),
            );
            assert!(requests[0].body.get("scopes").is_none());
        }
    }
}

fn signing_key() -> String {
    service_account_json("scope-test@data-project.iam.gserviceaccount.com").to_string()
}

/// Inspect the actual signed token. Checking only configured options would miss a builder that
/// accepts dbt's scope list but still mints its default token.
fn assert_jwt_scopes(authorization: &str, expected: &str) {
    let token = authorization.strip_prefix("Bearer ").unwrap();
    let payload = token.split('.').nth(1).expect("a signed JWT has a payload");
    let claims: Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .unwrap(),
    )
    .unwrap();
    let mut actual: Vec<_> = claims["scope"].as_str().unwrap().split(' ').collect();
    let mut expected: Vec<_> = expected.split(',').collect();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

fn assert_database_scopes(authentication_type: &str, credential: Option<&str>) {
    for (dbt_scopes, explicit_scopes, expected) in [
        ("", "", CLOUD_SCOPE),
        (DBT_SCOPES, "", DBT_SCOPES),
        (DBT_SCOPES, DRIVE_SCOPE, DRIVE_SCOPE),
    ] {
        let (endpoint, server) = capture_requests(vec![table_resource()]);
        let mut options = vec![
            ("adbc.bigquery.sql.auth_type", authentication_type),
            ("adbc.bigquery.sql.project_id", "data-project"),
            ("adbc.bigquery.sql.api_endpoint", endpoint.as_str()),
            ("bigquery.allow_untrusted_endpoint", "true"),
            ("adbc.bigquery.sql.impersonate.scopes", dbt_scopes),
            ("bigquery.auth.scopes", explicit_scopes),
        ];
        options.extend(
            credential.map(|credential| ("adbc.bigquery.sql.auth_credentials", credential)),
        );
        read_table_schema(&options);
        let requests = server.served(1);
        assert_jwt_scopes(requests[0].header("authorization").unwrap(), expected);
    }
}

#[test]
fn dbt_scopes_reach_service_account_file_and_string_tokens() {
    let document = signing_key();
    let file = TemporaryFile::new("dbt-service-account-scopes", &document);
    for (authentication_type, credential) in [
        (
            "adbc.bigquery.sql.auth_type.json_credential_string",
            document.as_str(),
        ),
        (
            "adbc.bigquery.sql.auth_type.json_credential_file",
            file.path(),
        ),
    ] {
        assert_database_scopes(authentication_type, Some(credential));
    }
}

/// ADC is process-wide, so only a child process receives the synthetic credential environment.
#[test]
fn dbt_scopes_reach_service_account_adc_tokens() {
    if std::env::var_os(ADC_CHILD).is_some() {
        assert_database_scopes("adbc.bigquery.sql.auth_type.auth_bigquery", None);
        return;
    }
    let file = TemporaryFile::new("dbt-adc-scopes", signing_key());
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "config::tests::scopes::dbt_scopes_reach_service_account_adc_tokens",
        ])
        .env(ADC_CHILD, "1")
        .env("GOOGLE_APPLICATION_CREDENTIALS", file.path())
        .env_remove("GOOGLE_CLOUD_QUOTA_PROJECT")
        .status()
        .unwrap();
    assert!(status.success());
}

/// The source credential needs IAM access, even when the impersonated target only needs Drive.
/// Explicit base scopes can override the source default independently of the target's scopes.
#[test]
fn impersonation_target_scopes_do_not_restrict_source_tokens() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for explicit in [None, Some("https://www.googleapis.com/auth/iam")] {
        let mut settings = json_credentials(signing_key());
        settings.impersonation.target_principal = Some(Arc::from("target@example.com"));
        settings.impersonation.scopes = vec![Arc::from(DRIVE_SCOPE)];
        settings.scopes = explicit.into_iter().map(Arc::from).collect();
        let (source, _) = crate::config::credentials::build_json_credentials(
            &settings,
            settings.credentials.as_deref().unwrap(),
        )
        .unwrap();
        let CacheableResource::New { data, .. } = runtime
            .block_on(source.headers(http::Extensions::new()))
            .unwrap()
        else {
            panic!("expected fresh source headers")
        };
        assert_jwt_scopes(
            data[http::header::AUTHORIZATION].to_str().unwrap(),
            explicit.unwrap_or(CLOUD_SCOPE),
        );
    }
}

#[test]
fn dbt_scopes_do_not_modify_a_temporary_access_token() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let mut settings = temporary_access_token("already-scoped-token");
    settings.impersonation.scopes = vec![Arc::from(DRIVE_SCOPE)];
    let configuration = ClientConfiguration::build(&settings).unwrap();
    let CacheableResource::New { data, .. } = runtime
        .block_on(configuration.credentials.headers(http::Extensions::new()))
        .unwrap()
    else {
        panic!("expected fresh token headers")
    };
    assert_eq!(
        data[http::header::AUTHORIZATION],
        "Bearer already-scoped-token"
    );
}
