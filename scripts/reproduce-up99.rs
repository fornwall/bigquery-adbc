#!/usr/bin/env -S cargo +nightly -Zscript
---cargo
[package]
edition = "2024"

[dependencies]
google-cloud-auth = { git = "https://github.com/googleapis/google-cloud-rust", rev = "5a2297f47973775e925ce49d37971b03ce358fba", default-features = false }
reqwest = { version = "=0.13.5", default-features = false, features = ["json", "query", "rustls-no-provider", "system-proxy"] }
rustls = { version = "=0.23.46", default-features = false, features = ["ring", "std", "tls12"] }
serde_json = "=1.0.154"
tokio = { version = "=1.54.1", features = ["rt", "time"] }
---

//! Run: cargo +nightly -Zscript scripts/reproduce-up99.rs
//!
//! Requires authorized-user ADC from `gcloud auth application-default login`, granting
//! cloud-platform and at least one other scope. Honors GOOGLE_APPLICATION_CREDENTIALS and
//! CLOUDSDK_CONFIG; otherwise reads ~/.config/gcloud/application_default_credentials.json.
//! This file works outside the repository. Cargo downloads and builds its dependencies.
//! Only scope names are printed; credentials and tokens stay in memory.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use google_cloud_auth::credentials::user_account::Builder;
use reqwest::{Client, RequestBuilder};
use serde_json::{Value, json};

const REVISION: &str = "5a2297f47973775e925ce49d37971b03ce358fba";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const TIMEOUT: Duration = Duration::from_secs(30);

fn main() -> Result<(), String> {
    let adc = read_adc()?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "could not create the async runtime")?;
    runtime.block_on(reproduce(adc))
}

fn read_adc() -> Result<Value, String> {
    let path = if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
        PathBuf::from(path)
    } else {
        let directory = if let Some(path) = std::env::var_os("CLOUDSDK_CONFIG") {
            PathBuf::from(path)
        } else {
            PathBuf::from(std::env::var_os("HOME").ok_or("set GOOGLE_APPLICATION_CREDENTIALS")?)
                .join(".config/gcloud")
        };
        directory.join("application_default_credentials.json")
    };
    let bytes = std::fs::read(path)
        .map_err(|_| "cannot read ADC; run gcloud auth application-default login")?;
    let adc: Value = serde_json::from_slice(&bytes).map_err(|_| "ADC is not valid JSON")?;
    if adc["type"] != "authorized_user" {
        return Err("ADC must contain authorized_user credentials".into());
    }
    for key in ["client_id", "client_secret", "refresh_token"] {
        if adc[key].as_str().is_none_or(str::is_empty) {
            return Err(format!("ADC is missing {key}"));
        }
    }
    Ok(adc)
}

async fn reproduce(adc: Value) -> Result<(), String> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TIMEOUT)
        .build()
        .map_err(|_| "could not build the HTTP client")?;
    println!("google-cloud-auth revision: {REVISION}");
    println!("Requested scope: {SCOPE}\n");

    let mut request = json!({
        "grant_type": "refresh_token",
        "client_id": adc["client_id"],
        "client_secret": adc["client_secret"],
        "refresh_token": adc["refresh_token"],
    });
    let original = response_scopes(
        client.post(TOKEN_ENDPOINT).json(&request),
        "Direct JSON: scope omitted",
    )
    .await?;
    if !original.contains(SCOPE) || original.len() < 2 {
        return Err(
            "inconclusive: the original grant needs cloud-platform and another scope".into(),
        );
    }

    request["scope"] = SCOPE.into();
    let singular = response_scopes(
        client.post(TOKEN_ENDPOINT).json(&request),
        "Direct JSON: singular scope",
    )
    .await?;
    request
        .as_object_mut()
        .expect("request is an object")
        .remove("scope");
    request["scopes"] = SCOPE.into();
    let plural = response_scopes(
        client.post(TOKEN_ENDPOINT).json(&request),
        "Direct JSON: plural scopes",
    )
    .await?;

    let credentials = Builder::new(adc)
        .with_token_uri(TOKEN_ENDPOINT)
        .with_scopes([SCOPE])
        .build_access_token_credentials()
        .map_err(|_| "could not build SDK user credentials")?;
    let token = tokio::time::timeout(TIMEOUT, credentials.access_token())
        .await
        .map_err(|_| "SDK token refresh timed out")?
        .map_err(|_| "SDK token refresh failed")?;

    // The SDK discards the refresh response's scope field. Ask Google's tokeninfo endpoint
    // about its actual access token instead; never print this request URL or its errors.
    let sdk = response_scopes(
        client
            .get("https://oauth2.googleapis.com/tokeninfo")
            .query(&[("access_token", token.token.as_str())]),
        "Pinned SDK: with_scopes([cloud-platform]), checked with tokeninfo",
    )
    .await?;

    if singular != BTreeSet::from([SCOPE.to_owned()]) || plural != original {
        return Err(
            "the direct scope/scopes comparison did not reproduce the expected behavior".into(),
        );
    }
    if sdk != singular {
        return Err("the SDK token did not have exactly the requested scope subset".into());
    }
    println!("UP-99 fixed: singular scope and the SDK narrow the token; plural scopes does not.");
    Ok(())
}

async fn response_scopes(request: RequestBuilder, label: &str) -> Result<BTreeSet<String>, String> {
    // Raw HTTP/JSON errors and response bodies can contain credentials or tokens.
    let response = request
        .send()
        .await
        .map_err(|_| format!("{label}: request failed"))?;
    if !response.status().is_success() {
        return Err(format!("{label}: HTTP {}", response.status()));
    }
    let body: Value = response
        .json()
        .await
        .map_err(|_| format!("{label}: invalid JSON"))?;
    let scopes: BTreeSet<String> = body["scope"]
        .as_str()
        .ok_or_else(|| format!("{label}: missing scope field"))?
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    println!("{label}:");
    for scope in &scopes {
        println!("  {scope}");
    }
    println!();
    Ok(scopes)
}
