//! Live OAuth endpoint contract behind UP-99; no BigQuery resources are needed.

use std::collections::BTreeSet;
use std::time::Duration;

const CLOUD_PLATFORM: &str = "https://www.googleapis.com/auth/cloud-platform";

#[test]
#[ignore = "requires user OAuth credentials granting cloud-platform and at least one other scope"]
fn user_oauth_refresh_scope_vs_scopes() {
    crate::backend::require_real_bigquery();
    let credential = |name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                panic!("{name} must be set; use scripts/run-oauth-live-test.sh --scopes")
            })
    };
    let mut request = serde_json::json!({
        "grant_type": "refresh_token",
        "client_id": credential("BIGQUERY_ADBC_OAUTH_CLIENT_ID"),
        "client_secret": credential("BIGQUERY_ADBC_OAUTH_CLIENT_SECRET"),
        "refresh_token": credential("BIGQUERY_ADBC_OAUTH_REFRESH_TOKEN"),
    });
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        // A redirected POST must never forward the refresh credentials to another endpoint.
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let original = refresh_scopes(&client, &request, "omitted").await;
        assert!(
            original.contains(CLOUD_PLATFORM) && original.len() > 1,
            "the original grant must include cloud-platform and another scope to prove narrowing"
        );

        request["scope"] = CLOUD_PLATFORM.into();
        let singular = refresh_scopes(&client, &request, "scope").await;
        request.as_object_mut().unwrap().remove("scope");
        request["scopes"] = CLOUD_PLATFORM.into();
        let plural = refresh_scopes(&client, &request, "scopes").await;

        assert_eq!(
            singular,
            BTreeSet::from([CLOUD_PLATFORM.to_owned()]),
            "singular scope must restrict the access token to the requested subset"
        );
        assert_eq!(plural, original, "plural scopes must behave like omission");
        assert_ne!(
            singular, plural,
            "the comparison must demonstrate narrowing"
        );
    });
}

#[expect(
    clippy::print_stderr,
    reason = "--nocapture reports only scope names for the live protocol comparison"
)]
async fn refresh_scopes(
    client: &reqwest::Client,
    request: &serde_json::Value,
    label: &str,
) -> BTreeSet<String> {
    // Never format the request, response body, or upstream errors: they may contain tokens.
    let body = serde_json::to_vec(request)
        .unwrap_or_else(|_| panic!("{label}: could not encode the refresh request"));
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .unwrap_or_else(|_| panic!("{label}: OAuth request failed"));
    assert!(
        response.status().is_success(),
        "{label}: OAuth endpoint returned HTTP {}",
        response.status()
    );
    let body = response
        .bytes()
        .await
        .unwrap_or_else(|_| panic!("{label}: could not read the OAuth response"));

    #[derive(serde::Deserialize)]
    struct RefreshResponse {
        scope: String,
        access_token: String,
    }

    let response: RefreshResponse = serde_json::from_slice(&body)
        .unwrap_or_else(|_| panic!("{label}: OAuth response lacks a valid token or scope field"));
    assert!(
        !response.access_token.is_empty(),
        "{label}: OAuth endpoint returned an empty access token"
    );
    let scopes = response
        .scope
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    eprintln!("{label}: {scopes:?}");
    scopes
}
