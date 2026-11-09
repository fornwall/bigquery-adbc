//! The impersonation layer over the configured credential.

use super::*;
use crate::test_support::http::ok;

/// The driver must use the selected source credential even when it fails. A refused OAuth
/// refresh stops before IAM and retains the source's remedy without falling back to ADC.
#[test]
fn impersonation_uses_the_configured_source_and_preserves_its_failure() {
    use crate::test_support::errors::{assert_failure, assert_message_omits};

    let (endpoint, server) = crate::test_support::http::capture_requests(vec![
        (
            400,
            serde_json::json!({
                "error": "invalid_grant", "error_description": "source-secret-sentinel"
            }),
        );
        3
    ]);
    let document = serde_json::json!({
        "type": "authorized_user",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "refresh_token": "refresh-token",
        "token_uri": format!("{endpoint}/token"),
    })
    .to_string();
    let file = TemporaryFile::new("impersonation-source", &document);
    let mut file_settings = json_credentials(file.path());
    file_settings.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for (mut settings, remedy) in [
        (
            user_authentication_via(&format!("{endpoint}/token"), true),
            "the refresh token in bigquery.auth.refresh_token",
        ),
        (json_credentials(document), "the JSON credentials"),
        (file_settings, "the JSON credentials"),
    ] {
        settings.allow_untrusted_endpoint = true;
        settings.impersonation.target_principal = Some(Arc::from("target@example.com"));
        let configuration = ClientConfiguration::build(&settings).unwrap();
        let failure = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(60),
                configuration.credentials.headers(http::Extensions::new()),
            )
            .await
            .expect("the source credential exchange did not finish")
            .unwrap_err()
        });
        let failure = crate::error::google_error(
            "authenticate",
            google_cloud_gax::error::Error::authentication(failure),
        );
        assert_failure(&failure, Status::Unauthenticated, &[remedy]);
        assert_message_omits(
            &failure,
            &[
                "Application Default Credentials",
                "serviceAccountTokenCreator",
                "client-secret",
                "refresh-token",
                "source-secret-sentinel",
            ],
        );
    }
    let requests = server.served(3);
    for request in requests {
        assert_eq!(request.path(), "/token");
        for (field, expected) in [
            ("grant_type", "refresh_token"),
            ("client_id", "client-id"),
            ("client_secret", "client-secret"),
            ("refresh_token", "refresh-token"),
        ] {
            assert_eq!(request.body[field], expected);
        }
    }
}

/// An impersonated-service-account document whose source user credential and IAM Credentials
/// calls both go to the mock at `endpoint`, impersonating `target@example.com`.
fn impersonated_document(endpoint: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": format!(
            "{endpoint}/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken"
        ),
        "source_credentials": {
            "type": "authorized_user",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token",
            "token_uri": format!("{endpoint}/token"),
        },
    })
}

/// `generateAccessToken` takes each delegate as `projects/-/serviceAccounts/{email}` and IAM
/// refuses a bare email, which is what the option carries; upstream sends delegates verbatim.
#[test]
fn impersonation_delegates_are_sent_as_iam_resource_names() {
    let (endpoint, server) = crate::test_support::http::capture_requests(vec![
        ok(
            serde_json::json!({"access_token": "source-token", "expires_in": 3600, "token_type": "Bearer"}),
        ),
        ok(
            serde_json::json!({"accessToken": "target-token", "expireTime": "2099-01-01T00:00:00Z"}),
        ),
    ]);
    let settings = settings(|config| {
        config.impersonation.delegates = vec![
            Arc::from("first@example.com"),
            Arc::from("second@example.com"),
        ];
    });
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let credentials = crate::config::client::configure_impersonation(
        google_cloud_auth::credentials::impersonated::Builder::new(impersonated_document(
            &endpoint,
        )),
        &settings,
    )
    .build()
    .unwrap();
    runtime
        .block_on(credentials.headers(http::Extensions::new()))
        .unwrap();

    let requests = server.join().unwrap();
    assert_eq!(
        requests[1].body["delegates"],
        serde_json::json!([
            "projects/-/serviceAccounts/first@example.com",
            "projects/-/serviceAccounts/second@example.com",
        ])
    );
}

/// An impersonated credential refreshes with both the default and an explicit lifetime, without
/// being rebuilt, rather than becoming a static one-hour token (the Go driver's 41c8703). The
/// server's expiry, rather than the requested lifetime, determines when to refresh.
#[test]
fn impersonation_refreshes_with_default_and_explicit_lifetimes() {
    use google_cloud_auth::credentials::CacheableResource;
    use serde_json::json;

    for lifetime in [None, Some(3_600), Some(7_200)] {
        let mut index = 0;
        let (endpoint, server) =
            crate::test_support::http::capture_requests_responding(3, move |_request| {
                let response = match index {
                    0 => ok(json!({
                        "access_token": "source-token", "expires_in": 3600, "token_type": "Bearer"
                    })),
                    _ => {
                        let expiry = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            + 60;
                        let expiry = arrow_array::temporal_conversions::timestamp_s_to_datetime(
                            i64::try_from(expiry).unwrap(),
                        )
                        .unwrap()
                        .and_utc()
                        .to_rfc3339();
                        ok(json!({
                            "accessToken": format!("target-token-{index}"), "expireTime": expiry
                        }))
                    }
                };
                index += 1;
                Some(response)
            });
        let settings = settings(|config| {
            config.impersonation.target_principal = Some(Arc::from("target@example.com"));
            config.impersonation.lifetime = lifetime.map(Duration::from_secs);
        });
        crate::test_support::block_on_paused_clock(async {
            let credentials = crate::config::client::configure_impersonation(
                google_cloud_auth::credentials::impersonated::Builder::new(impersonated_document(
                    &endpoint,
                )),
                &settings,
            )
            .build()
            .unwrap();
            let CacheableResource::New { entity_tag, data } =
                credentials.headers(http::Extensions::new()).await.unwrap()
            else {
                panic!("expected initial headers")
            };
            assert_eq!(data[http::header::AUTHORIZATION], "Bearer target-token-1");
            tokio::time::advance(Duration::from_secs(61)).await;
            let mut extensions = http::Extensions::new();
            extensions.insert(entity_tag);
            let CacheableResource::New { data, .. } =
                credentials.headers(extensions).await.unwrap()
            else {
                panic!("expected refreshed headers for lifetime {lifetime:?}")
            };
            assert_eq!(data[http::header::AUTHORIZATION], "Bearer target-token-2");
        });
        let requests = server.join().unwrap();
        for request in &requests[1..] {
            assert_eq!(request.header("authorization"), Some("Bearer source-token"));
            assert_eq!(
                request.body["lifetime"],
                format!("{}s", lifetime.unwrap_or(3_600))
            );
            assert_eq!(
                request.path(),
                "/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken"
            );
        }
    }
}
