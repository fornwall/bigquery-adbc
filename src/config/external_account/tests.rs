use super::*;
use adbc_core::error::Status;
use google_cloud_auth::credentials::{CacheableResource, EntityTag};
use google_cloud_gax::error::CredentialsError;
use google_cloud_gax::retry_policy::NeverRetry;
use http::Extensions;
use serde_json::json;

use crate::BigQueryAuthenticationType;
use crate::config::redaction::redacted_credentials;
use crate::error::CredentialLayer;

use crate::test_support::errors::{assert_failure, assert_message_omits};
use crate::test_support::http::{
    CapturedRequest, accept, capture_requests, capture_requests_responding,
    capture_requests_until_stopped, loopback_listener, ok, read_captured_request, retire,
};

const AUDIENCE: &str = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider";
const REQUEST_DATA: &str = "client_id=client&client_secret=s%2Becret+value&grant_type=client_credentials&scope=api%3A%2F%2Fsubject%2F.default";

fn settings(endpoint: &str) -> DatabaseConfig {
    DatabaseConfig {
        authentication_type: BigQueryAuthenticationType::ExternalAccount,
        allow_untrusted_endpoint: true,
        external_account: ExternalAccountConfig {
            audience: Some(Arc::from(AUDIENCE)),
            request_url: Some(Secret::from(format!("{endpoint}/oauth?api_key=url-secret"))),
            request_data: Some(Secret::from(REQUEST_DATA)),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn credentials(settings: &DatabaseConfig, endpoint: &str) -> Credentials {
    redacted_credentials(
        external_account_builder(settings)
            .unwrap()
            .0
            .with_token_url(format!("{endpoint}/sts"))
            .with_retry_policy(NeverRetry)
            .build()
            .unwrap(),
        CredentialLayer::Authentication(BigQueryAuthenticationType::ExternalAccount),
    )
}

/// The failed first token of a credential whose identity provider and STS are both `endpoint`.
async fn mint_failure(endpoint: &str) -> CredentialsError {
    credentials(&settings(endpoint), endpoint)
        .headers(Extensions::new())
        .await
        .unwrap_err()
}

/// What a query reports for a failed token mint.
fn reported(source: CredentialsError) -> adbc_core::error::Error {
    crate::error::google_error(
        "failed to authenticate the request",
        google_cloud_gax::error::Error::authentication(source),
    )
}

fn identity_provider(endpoint: &str) -> IdentityProvider {
    IdentityProvider {
        client: reqwest::Client::new(),
        request_url: reqwest::Url::parse(endpoint).unwrap(),
        request_data: Bytes::from_static(REQUEST_DATA.as_bytes()),
    }
}

/// A form-encoded request body's fields.
fn form_fields(request: &CapturedRequest) -> std::collections::BTreeMap<String, String> {
    let body = std::str::from_utf8(&request.raw_body).unwrap();
    reqwest::Url::parse(&format!("http://localhost/?{body}"))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

fn sts_response() -> serde_json::Value {
    json!({"access_token":"google-access-token", "token_type":"Bearer", "expires_in":3600,
        "issued_token_type":"urn:ietf:params:oauth:token-type:access_token"})
}

#[test]
fn external_account_posts_opaque_form_exchanges_jwt_and_shares_cached_headers() {
    let (endpoint, server) = capture_requests(vec![
        ok(json!({"access_token":"idp.jwt-token", "id_token":"wrong-token"})),
        (200, sts_response()),
    ]);
    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        let mut settings = settings(&endpoint);
        settings.quota_project = Some(
            crate::identifiers::ProjectIdentifier::try_new_from("test", "quota-project").unwrap(),
        );
        settings.impersonation.scopes = vec![
            Arc::from("https://www.googleapis.com/auth/bigquery"),
            Arc::from("https://www.googleapis.com/auth/drive"),
        ];
        let credentials = credentials(&settings, &endpoint);
        let CacheableResource::New { entity_tag, data } =
            credentials.headers(Extensions::new()).await.unwrap()
        else {
            panic!("first headers must be new")
        };
        assert_eq!(
            data[http::header::AUTHORIZATION],
            "Bearer google-access-token"
        );
        assert_eq!(data["x-goog-user-project"], "quota-project");
        let mut extensions = Extensions::new();
        extensions.insert::<EntityTag>(entity_tag);
        assert!(matches!(
            credentials.clone().headers(extensions).await.unwrap(),
            CacheableResource::NotModified
        ));
        assert!(matches!(
            credentials.headers(Extensions::new()).await.unwrap(),
            CacheableResource::New { .. }
        ));
        assert!(!format!("{credentials:?}").contains("google-access-token"));
    });
    let requests = server.join().unwrap();
    let [idp, sts] = requests.as_slice() else {
        panic!("expected IdP and STS requests")
    };
    assert_eq!(idp.line, "POST /oauth?api_key=url-secret HTTP/1.1");
    assert_eq!(idp.header("accept"), Some("application/json"));
    assert_eq!(
        idp.header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq!(idp.raw_body, REQUEST_DATA.as_bytes());
    assert_eq!(idp.header("authorization"), None);
    assert_eq!(sts.line, "POST /sts HTTP/1.1");
    let fields = form_fields(sts);
    assert_eq!(fields["audience"], AUDIENCE);
    assert_eq!(fields["subject_token"], "idp.jwt-token");
    assert_eq!(
        fields["subject_token_type"],
        "urn:ietf:params:oauth:token-type:jwt"
    );
    assert_eq!(
        fields["grant_type"],
        "urn:ietf:params:oauth:grant-type:token-exchange"
    );
    assert_eq!(
        fields["scope"],
        "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/drive"
    );
}

#[test]
fn external_account_requires_discrete_inputs_and_rejects_unused_ones() {
    use BigQueryDatabaseOption as O;
    for option in [
        O::AuthenticationExternalAccountAudience,
        O::AuthenticationExternalAccountRequestUrl,
        O::AuthenticationExternalAccountRequestData,
    ] {
        let mut settings = settings("https://idp.example");
        match option {
            O::AuthenticationExternalAccountAudience => settings.external_account.audience = None,
            O::AuthenticationExternalAccountRequestUrl => {
                settings.external_account.request_url = None;
            }
            O::AuthenticationExternalAccountRequestData => {
                settings.external_account.request_data = None;
            }
            _ => unreachable!("only the three required options are dropped above"),
        }
        let error = external_account_builder(&settings).err().unwrap();
        assert_failure(&error, Status::InvalidArguments, &[option.as_str()]);
    }
    for (value, option) in settings("https://idp.example").external_account.values() {
        let mut config = DatabaseConfig::default();
        match option {
            O::AuthenticationExternalAccountAudience => {
                config.external_account.audience = value.map(Arc::from);
            }
            O::AuthenticationExternalAccountRequestUrl => {
                config.external_account.request_url = value.map(Secret::from);
            }
            O::AuthenticationExternalAccountRequestData => {
                config.external_account.request_data = value.map(Secret::from);
            }
            O::AuthenticationExternalAccountImpersonationUrl => {
                config.external_account.impersonation_url = Some(Arc::from(
                    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.iam.gserviceaccount.com:generateAccessToken",
                ));
            }
            // The two STS options are unset in `settings`, so each gets a value here: every
            // external-account option must be refused under another authentication type.
            O::AuthenticationExternalAccountSubjectTokenType => {
                config.external_account.subject_token_type =
                    Some(Arc::from(SUBJECT_TOKEN_TYPES[1]));
            }
            O::AuthenticationExternalAccountTokenUrl => {
                config.external_account.token_url =
                    Some(Arc::from("https://sts.googleapis.com/v1/token"));
            }
            _ => unreachable!("every external-account option is covered above"),
        }
        let error = config.validate_authentication().unwrap_err();
        assert_failure(&error, Status::InvalidArguments, &[option.as_str()]);
        assert_message_omits(&error, &[REQUEST_DATA]);
    }
}

#[test]
fn external_account_validates_urls_without_echoing_credentials() {
    for value in [
        "not-a-url-secret",
        "http://idp.example/token",
        "https://user:password@idp.example/token",
        "https://idp.example/token#secret",
        "file:///tmp/secret",
        "https://idp.example/\nsecret",
    ] {
        let error = parse_request_url(value, false).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(&error, &[value]);
    }
    assert!(parse_request_url("https://idp.example/token?secret=value", false).is_ok());
    assert!(parse_request_url("http://127.0.0.1/token", true).is_ok());
    for principal in [
        "test@example.iam.gserviceaccount.com",
        "test%40example.iam.gserviceaccount.com",
        "123456789",
    ] {
        let url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{principal}:generateAccessToken"
        );
        assert_eq!(impersonation_principal(&url).unwrap().0, principal);
    }
    for url in [
        "http://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test:generateAccessToken",
        "https://evil.example/v1/projects/-/serviceAccounts/test:generateAccessToken",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/:generateAccessToken",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test:generateAccessToken?secret",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/a%2Fb:generateAccessToken",
    ] {
        assert!(impersonation_principal(url).is_err());
    }
}

#[test]
fn external_account_rejects_bad_idp_responses_without_exposing_body_or_url() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (status, response) in [
        (401, json!({"error_description":REQUEST_DATA})),
        ok(json!({"id_token":"secret-id-token"})),
        ok(json!({"access_token":7})),
        ok(json!({"access_token":"  "})),
        ok(json!("secret-nonobject-response")),
    ] {
        let (endpoint, server) = capture_requests(vec![(status, response)]);
        runtime.block_on(async {
            let credentials = credentials(&settings(&endpoint), &endpoint);
            let error = credentials.headers(Extensions::new()).await.unwrap_err();
            let rendered = format!("{error:?} {error}");
            for secret in [
                REQUEST_DATA,
                "url-secret",
                "secret-id-token",
                "secret-nonobject-response",
            ] {
                assert!(!rendered.contains(secret));
            }
            assert!(!error.is_transient());
        });
        server.served(1);
    }
}

/// STS can echo the subject token, so its failure is redacted. A redacted failure still says
/// which credential could not mint, so the remedy names this authentication type's options rather
/// than Application Default Credentials.
#[test]
fn a_refused_sts_exchange_is_redacted_and_keeps_the_external_account_remedy() {
    let (endpoint, server) = capture_requests(vec![
        ok(json!({"access_token":"secret-subject-token"})),
        (
            400,
            json!({"error":"invalid_grant", "error_description":"secret-subject-token"}),
        ),
    ]);
    crate::runtime::new_runtime().unwrap().block_on(async {
        let source = mint_failure(&endpoint).await;
        for cause in crate::error::chain(&source) {
            assert!(!format!("{cause:?} {cause}").contains("secret-subject-token"));
        }
        let failure = reported(source);
        assert_failure(
            &failure,
            Status::Unauthenticated,
            &["the external-account token exchange could not mint an access token"],
        );
        assert_message_omits(&failure, &["gcloud auth application-default login"]);
    });
    server.served(2);
}

/// An identity provider answering with a retryable status is an outage, so the failure is `IO`
/// rather than a remedy for a credential that was never refused.
#[test]
fn an_unavailable_identity_provider_is_an_io_failure() {
    let (endpoint, server) = capture_requests(vec![(503, json!({}))]);
    crate::runtime::new_runtime().unwrap().block_on(async {
        assert_failure(
            &reported(mint_failure(&endpoint).await),
            Status::IO,
            &[
                "answered HTTP 503",
                "external-account identity provider rejected the token request (HTTP 503)",
            ],
        );
    });
    server.served(1);
}

#[test]
fn an_unreachable_identity_provider_is_an_io_failure_without_exposing_the_url() {
    crate::runtime::new_runtime().unwrap().block_on(async {
        let source = mint_failure("http://127.0.0.1:1").await;
        assert!(source.is_transient());
        let failure = reported(source);
        assert_failure(
            &failure,
            Status::IO,
            &[
                "could not complete a token exchange",
                "failed to communicate with the external-account identity provider",
            ],
        );
        assert_message_omits(
            &failure,
            &["127.0.0.1", "url-secret", REQUEST_DATA, "gcloud auth"],
        );
    });
}

#[test]
fn external_account_identity_provider_errors_classify_retryability() {
    let runtime = crate::runtime::new_runtime().unwrap();
    for (status, transient) in [
        (408, true),
        (429, true),
        (500, true),
        (503, true),
        (400, false),
        (403, false),
    ] {
        let (endpoint, server) = capture_requests(vec![(status, json!({}))]);
        runtime.block_on(async {
            let provider = identity_provider(&endpoint);
            let error = provider.subject_token().await.unwrap_err();
            assert_eq!(error.is_transient(), transient, "status {status}");
            assert!(error.to_string().contains(&status.to_string()));
        });
        server.served(1);
    }
}

/// A connection closed before the response is reqwest's `is_request` kind, not `is_connect`. A
/// permanent verdict would end upstream's token cache refresh loop, so one dropped connection
/// would fail every later query in the process.
#[test]
fn a_connection_dropped_before_the_identity_provider_answers_is_transient() {
    let (endpoint, server) = capture_requests_responding(1, |_| None);
    crate::runtime::new_runtime().unwrap().block_on(async {
        let provider = identity_provider(&endpoint);
        assert!(provider.subject_token().await.unwrap_err().is_transient());
    });
    server.served(1);
}

#[test]
fn external_account_retries_transient_identity_provider_failure() {
    let (endpoint, server) = capture_requests(vec![
        (
            503,
            json!({"error_description": "temporary provider failure"}),
        ),
        ok(json!({"access_token": "recovered-subject-token"})),
        (200, sts_response()),
    ]);
    crate::runtime::new_runtime().unwrap().block_on(async {
        let backoff = google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .build()
            .unwrap();
        let credentials = external_account_builder(&settings(&endpoint))
            .unwrap()
            .0
            .with_token_url(format!("{endpoint}/sts"))
            .with_backoff_policy(backoff)
            .build()
            .unwrap();
        let CacheableResource::New { data, .. } =
            credentials.headers(Extensions::new()).await.unwrap()
        else {
            panic!("expected headers")
        };
        assert_eq!(
            data[http::header::AUTHORIZATION],
            "Bearer google-access-token"
        );
    });
    let requests = server.served(3);
    assert_eq!(requests[0].raw_body, requests[1].raw_body);
}

#[test]
fn external_account_rejects_oversized_identity_provider_response() {
    let (endpoint, server) =
        capture_requests(vec![ok(json!({"access_token":"x".repeat(1024 * 1024)}))]);
    crate::runtime::new_runtime().unwrap().block_on(async {
        let error = mint_failure(&endpoint).await;
        assert!(
            crate::error::chain(&error).any(|cause| cause.to_string().contains("exceeds 1 MiB"))
        );
        assert!(!error.is_transient());
    });
    server.join().unwrap();
}

#[test]
fn external_account_does_not_follow_identity_provider_redirects() {
    use std::io::Write as _;

    let (redirect, target) = capture_requests_until_stopped(vec![ok(json!({}))]);
    let listener = loopback_listener();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        // `capture_requests` cannot send a `Location` header. Skip connections reqwest's pool
        // opens and closes unused, as it does.
        let request = loop {
            let mut stream = accept(&listener, None).unwrap();
            if let Some(request) = read_captured_request(&mut stream) {
                let _ = write!(
                    stream,
                    "HTTP/1.1 307 Temporary Redirect\r\nLocation: {redirect}/capture-secret\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                );
                break request;
            }
        };
        retire(listener);
        request
    });
    crate::runtime::new_runtime().unwrap().block_on(async {
        let error = mint_failure(&endpoint).await;
        assert!(crate::error::chain(&error).any(|cause| cause.to_string().contains("HTTP 307")));
    });
    assert_eq!(server.join().unwrap().raw_body, REQUEST_DATA.as_bytes());
    assert!(target.stop().is_empty());
}

/// Runs on a paused clock so the refresh upstream's token cache schedules ten seconds out is
/// reached by advancing time rather than waiting it out. The blocking task inhibits Tokio's
/// auto-advance, which would otherwise jump to the IdP client's connect and request timeouts
/// while a mock response is in flight; the clock therefore moves only when the test says so.
#[test]
fn external_account_refreshes_subject_and_google_tokens_without_rebuilding_credentials() {
    let mut short_lived = sts_response();
    short_lived["expires_in"] = json!(5);
    let mut refreshed = sts_response();
    refreshed["access_token"] = json!("refreshed-google-token");
    let (endpoint, server) = capture_requests(vec![
        ok(json!({"access_token":"initial-subject"})),
        (200, short_lived),
        ok(json!({"access_token":"refreshed-subject"})),
        (200, refreshed),
    ]);
    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let inhibitor = tokio::task::spawn_blocking(move || held.recv());
        let credentials = credentials(&settings(&endpoint), &endpoint);
        let CacheableResource::New { entity_tag, data } =
            credentials.headers(Extensions::new()).await.unwrap()
        else {
            panic!("expected initial headers")
        };
        assert_eq!(
            data[http::header::AUTHORIZATION],
            "Bearer google-access-token"
        );
        // Past the five-second expiry and the cache's ten-second refresh sleep: the expired
        // token makes this call wait for the refresh rather than return the cached headers.
        tokio::time::advance(Duration::from_secs(11)).await;
        let mut extensions = Extensions::new();
        extensions.insert(entity_tag);
        let CacheableResource::New { data, .. } = credentials.headers(extensions).await.unwrap()
        else {
            panic!("expected refreshed headers")
        };
        assert_eq!(
            data[http::header::AUTHORIZATION],
            "Bearer refreshed-google-token"
        );
        drop(hold);
        let _ = inhibitor.await;
    });
    let requests = server.join().unwrap();
    assert_eq!(requests[0].raw_body, requests[2].raw_body);
    assert_eq!(
        form_fields(&requests[3])["subject_token"],
        "refreshed-subject"
    );
}

/// The subject-token type and the STS endpoint are defaults rather than constants: an identity
/// provider answering a client-credentials grant (Okta, PingFederate) returns an opaque access
/// token, not a JWT, and upstream's STS default substitutes the credential's universe domain.
#[test]
fn an_opaque_subject_token_type_and_a_scope_list_reach_the_token_exchange() {
    let (endpoint, server) = capture_requests(vec![
        ok(json!({"access_token":"idp.opaque-token"})),
        (200, sts_response()),
    ]);
    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        let mut settings = settings(&endpoint);
        settings.external_account.subject_token_type = Some(Arc::from(SUBJECT_TOKEN_TYPES[1]));
        // `bigquery.impersonate.scopes` is the fallback for a dbt 2.0 profile that sets only
        // it; `bigquery.auth.scopes` wins when both are set.
        settings.impersonation.scopes = vec![Arc::from("https://www.googleapis.com/auth/bigquery")];
        settings.scopes = vec![Arc::from("https://www.googleapis.com/auth/drive")];
        credentials(&settings, &endpoint)
            .headers(Extensions::new())
            .await
            .unwrap();
    });
    let requests = server.join().unwrap();
    let [_, sts] = requests.as_slice() else {
        panic!("expected IdP and STS requests")
    };
    let fields = form_fields(sts);
    assert_eq!(fields["subject_token"], "idp.opaque-token");
    assert_eq!(
        fields["subject_token_type"],
        "urn:ietf:params:oauth:token-type:access_token"
    );
    assert_eq!(fields["scope"], "https://www.googleapis.com/auth/drive");
}
