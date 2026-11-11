//! Which endpoints may receive a credential.

use serde_json::json;

use super::*;
use crate::test_support::credentials::service_account_json;
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::http::capture_requests;

/// Configuring a BigQuery REST endpoint authorizes only that service to receive credentials.
///
/// The rejection quotes the resolved host, which is safe because `normalize_endpoint` has
/// refused user information, whitespace, queries, and fragments.
#[test]
fn a_custom_rest_endpoint_does_not_authorize_other_services_to_receive_credentials() {
    for kind in ServiceEndpointKind::ALL {
        if kind == ServiceEndpointKind::BigQuery {
            continue;
        }
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
            config.endpoints.set(
                ServiceEndpointKind::BigQuery,
                Some(Arc::from("https://private.example")),
            );
            config
                .endpoints
                .set(kind, Some(Arc::from("https://evil.example.com")));
        });
        let error = ClientConfiguration::build(&settings).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let option = kind.option().as_str();
        // The rejection uses the "BigQuery option {key} must ..." form and names every remedy.
        assert!(
            error.message.starts_with(&format!(
                "BigQuery option {option} must be a Google API HTTPS endpoint, but resolves to \
                 host \"evil.example.com\" over https;"
            )),
            "{kind:?}: {}",
            error.message
        );
        assert_message(
            &error,
            &[
                BigQueryAuthenticationType::ApplicationDefault.as_str(),
                "bigquery.allow_untrusted_endpoint",
                BigQueryAuthenticationType::Anonymous.as_str(),
            ],
        );
    }
}

/// Look-alike hosts must not pass as Google API hosts, and plaintext even to the right host
/// still hands the token to anyone on the path. The rejection names the host it read, which is
/// the only thing that makes `googelapis.com` distinguishable from `googleapis.com` in a log.
#[test]
fn look_alike_hosts_and_plaintext_schemes_do_not_pass_as_google_api_endpoints() {
    for (scheme, host, accepted) in [
        ("https", "googleapis.com.evil.example", false),
        ("https", "notgoogleapis.com", false),
        // A trailing root label names the same host, so it stays trusted.
        ("https", "googleapis.com.", true),
        ("https", "evil.example.com", false),
        // Plaintext to the right host still hands the token to anyone on the path.
        ("http", "bigquery.googleapis.com", false),
    ] {
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
            config.endpoints.set(
                ServiceEndpointKind::Storage,
                Some(Arc::from(format!("{scheme}://{host}"))),
            );
        });
        let outcome = settings.validate_endpoints();
        if accepted {
            outcome.unwrap_or_else(|error| panic!("{scheme}://{host}: {error}"));
        } else {
            let error = outcome.unwrap_err();
            assert_failure(
                &error,
                Status::InvalidArguments,
                &[&format!("host {host:?} over {scheme}")],
            );
            // Plaintext is called out as such: the same host over HTTPS is trusted.
            assert_eq!(
                error.message.contains("in cleartext"),
                scheme == "http",
                "{scheme}://{host}: {}",
                error.message
            );
        }
    }
}

/// The OAuth token endpoint receives the client secret and the refresh token themselves, not a
/// token minted from them, so it retains its own trust rule even with a custom REST endpoint.
///
/// Without it, `bigquery.auth.access_token_endpoint=http://attacker.example/token` in a dbt profile
/// would be a permanent, cleartext refresh-token theft: `google-cloud-auth` POSTs
/// `{client_id, client_secret, refresh_token, grant_type}` to whatever it is given.
#[test]
fn an_oauth_token_endpoint_outside_googleapis_com_is_refused() {
    for endpoint in [
        "http://attacker.example/token",
        "https://attacker.example/token",
        "https://oauth2.googleapis.com.evil.example/token",
        "http://oauth2.googleapis.com/token",
        // A URL whose user information would make the quoted host ambiguous.
        "https://user:secret@oauth2.googleapis.com/token",
        "not-a-url",
    ] {
        let mut settings = user_authentication_via(endpoint, false);
        settings.endpoints.set(
            ServiceEndpointKind::BigQuery,
            Some(Arc::from("https://private.example")),
        );
        let error = settings.validate_endpoints().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{endpoint}");
        assert!(
            error
                .message
                .starts_with("BigQuery option bigquery.auth.access_token_endpoint must be an"),
            "{endpoint}: {}",
            error.message
        );
        // The two credentials the exchange would have handed over are named, and neither their
        // values nor the URL's user information appear.
        assert!(
            !error.message.contains("secret@") && !error.message.contains("client-secret"),
            "{endpoint}: {}",
            error.message
        );
    }

    let refused = user_authentication_via("https://attacker.example/token", false)
        .validate_endpoints()
        .unwrap_err();
    assert_eq!(
        refused.message,
        "BigQuery option bigquery.auth.access_token_endpoint must be an HTTPS endpoint on \
         googleapis.com or accounts.google.com, but resolves to host \"attacker.example\" over \
         https; the refresh-token exchange would send bigquery.auth.client_secret and \
         bigquery.auth.refresh_token there; use https://oauth2.googleapis.com/token, or set \
         bigquery.allow_untrusted_endpoint=true to permit another host"
    );

    // Google's own endpoint, any other googleapis.com host, and the explicit opt-in all pass.
    for (endpoint, opt_in) in [
        ("https://oauth2.googleapis.com/token", false),
        ("https://sts.googleapis.com/v1/token", false),
        ("http://127.0.0.1:9050/token", true),
    ] {
        user_authentication_via(endpoint, opt_in)
            .validate_endpoints()
            .unwrap_or_else(|error| panic!("{endpoint}: {error}"));
    }

    // Unset stays unset: the option is optional and Google's default is used.
    settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::UserAuthentication;
    })
    .validate_endpoints()
    .unwrap();
}

/// Older Google-issued `client_secret.json` files name the legacy `accounts.google.com` token
/// endpoint. Refusing it would force `bigquery.allow_untrusted_endpoint=true` just to talk to
/// Google, so it is trusted for the OAuth token endpoint only.
#[test]
fn the_legacy_google_oauth_token_endpoint_is_trusted_for_the_token_exchange_alone() {
    for endpoint in [
        "https://accounts.google.com/o/oauth2/token",
        // A trailing dot is the same host, as it is for the googleapis.com rule.
        "https://accounts.google.com./o/oauth2/token",
        "https://oauth2.googleapis.com/token",
    ] {
        user_authentication_via(endpoint, false)
            .validate_endpoints()
            .unwrap_or_else(|error| panic!("{endpoint}: {error}"));
    }

    // Neither http, nor another host on the same domain, nor a lookalike.
    for endpoint in [
        "http://accounts.google.com/o/oauth2/token",
        "https://myaccount.google.com/o/oauth2/token",
        "https://accounts.google.com.evil.example/o/oauth2/token",
    ] {
        let error = user_authentication_via(endpoint, false)
            .validate_endpoints()
            .unwrap_err();
        assert_failure(
            &error,
            Status::InvalidArguments,
            &[
                "BigQuery option bigquery.auth.access_token_endpoint must be an HTTPS endpoint \
                 on googleapis.com or accounts.google.com",
            ],
        );
    }

    // The Security Token Service has no endpoint on the accounts host, so the option that posts
    // the subject token there is not widened, and its refusal does not advertise the host.
    let sts = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::ExternalAccount;
        config.endpoints.set(
            ServiceEndpointKind::BigQuery,
            Some(Arc::from("https://private.example")),
        );
        config.external_account.token_url = Some(Arc::from("https://accounts.google.com/v1/token"));
    });
    let error = sts.validate_endpoints().unwrap_err();
    assert!(
        error.message.starts_with(
            "BigQuery option bigquery.auth.external_account.token_url must be an HTTPS endpoint \
             on googleapis.com, but resolves to host \"accounts.google.com\""
        ),
        "{}",
        error.message
    );
}

/// Endpoint configurations that must stay accepted, on top of Application Default
/// Credentials unless a case configures otherwise.
#[test]
fn trusted_or_opted_in_endpoints_are_accepted() {
    let accepted = |label: &str, configure: &dyn Fn(&mut DatabaseConfig)| {
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
            configure(config);
        });
        settings
            .validate_endpoints()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
    };
    // Google API HTTPS endpoints, whatever their casing, port, or service host, are accepted for
    // every service in the canonical inventory.
    for endpoint in [
        "https://bigquery.googleapis.com",
        "https://bigquery.googleapis.com:443",
        "https://BigQuery.GoogleAPIs.com",
        "https://bigquerystorage.googleapis.com",
    ] {
        for kind in ServiceEndpointKind::ALL {
            accepted(&format!("{kind:?}: {endpoint}"), &|config| {
                config.endpoints.set(kind, Some(Arc::from(endpoint)));
            });
        }
    }
    // An anonymous credential mints no token, so an emulator needs no opt-in.
    for kind in ServiceEndpointKind::ALL {
        accepted(&format!("{kind:?}: anonymous emulator"), &|config| {
            config.authentication_type = BigQueryAuthenticationType::Anonymous;
            config
                .endpoints
                .set(kind, Some(Arc::from("http://127.0.0.1:9050")));
        });
    }
    // The configured REST endpoint itself opts in, including HTTP proxies.
    for endpoint in ["https://private.example", "http://127.0.0.1:9050"] {
        accepted(endpoint, &|config| {
            config
                .endpoints
                .set(ServiceEndpointKind::BigQuery, Some(Arc::from(endpoint)));
        });
    }
    // The explicit opt-in accepts any endpoint.
    for kind in ServiceEndpointKind::ALL {
        accepted(&format!("{kind:?}: untrusted-endpoint opt-in"), &|config| {
            config
                .endpoints
                .set(kind, Some(Arc::from("https://private.example")));
            config.allow_untrusted_endpoint = true;
        });
    }
    // Unset endpoints need no opt-in.
    accepted("unset endpoints", &|_| ());
}

/// An endpoint attributed to a URI meets the rule auxiliary services meet: HTTPS alone is not
/// enough, nor is a Google host alone. The same value set as an option is trusted. Refusing `http`
/// here is defence in depth, since the URI grammar can only produce `https`.
#[test]
fn an_endpoint_from_a_uri_must_be_https_on_a_google_api_host() {
    let endpoint_from = |endpoint: &str, endpoint_from_uri: bool| {
        settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
            config
                .endpoints
                .set(ServiceEndpointKind::BigQuery, Some(Arc::from(endpoint)));
            config.endpoint_from_uri = endpoint_from_uri;
        })
    };
    for (endpoint, host, scheme) in [
        (
            "http://bigquery.googleapis.com",
            "bigquery.googleapis.com",
            "http",
        ),
        ("http://127.0.0.1:9050", "127.0.0.1", "http"),
        ("https://private.example:443", "private.example", "https"),
    ] {
        let error = endpoint_from(endpoint, true)
            .validate_endpoints()
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{endpoint}");
        assert!(
            error.message.starts_with(&format!(
                "BigQuery URI host must be a Google API HTTPS endpoint, but resolves to host \
                 {host:?} over {scheme};"
            )),
            "{endpoint}: {}",
            error.message
        );
        assert_eq!(
            error.message.contains("in cleartext"),
            scheme == "http",
            "{endpoint}: {}",
            error.message
        );
        endpoint_from(endpoint, false)
            .validate_endpoints()
            .unwrap_or_else(|error| panic!("{endpoint}: {error}"));
    }
    endpoint_from("https://bigquery.googleapis.com:443", true)
        .validate_endpoints()
        .unwrap();
}

#[test]
fn custom_rest_endpoint_options_send_credentials_without_an_extra_opt_in() {
    let service_account =
        service_account_json("endpoint-test@data-project.iam.gserviceaccount.com").to_string();
    for endpoint_option in [
        "bigquery.endpoint",
        "adbc.bigquery.sql.endpoint",
        "adbc.bigquery.sql.api_endpoint",
    ] {
        for (authentication_type, credential_option, credential) in [
            (
                "adbc.bigquery.sql.auth_type.temporary_access_token",
                "adbc.bigquery.sql.auth.access_token",
                "synthetic-endpoint-token",
            ),
            (
                "adbc.bigquery.sql.auth_type.json_credential_string",
                "adbc.bigquery.sql.auth_credentials",
                service_account.as_str(),
            ),
        ] {
            let (endpoint, server) = capture_requests(vec![table_resource()]);
            read_table_schema(&[
                ("adbc.bigquery.sql.auth_type", authentication_type),
                (credential_option, credential),
                ("adbc.bigquery.sql.project_id", "data-project"),
                (endpoint_option, endpoint.as_str()),
            ]);
            let requests = server.served(1);
            assert_eq!(
                requests[0].path().split('?').next(),
                Some("/bigquery/v2/projects/data-project/datasets/dataset/tables/table")
            );
            let authorization = requests[0].header("authorization").unwrap();
            if credential_option.ends_with("access_token") {
                assert_eq!(authorization, "Bearer synthetic-endpoint-token");
            } else {
                assert_eq!(
                    authorization
                        .strip_prefix("Bearer ")
                        .unwrap()
                        .split('.')
                        .count(),
                    3,
                    "expected a signed service-account JWT"
                );
            }
        }
    }
}

#[test]
fn a_custom_rest_endpoint_does_not_allow_plaintext_identity_provider_requests() {
    let settings = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::ExternalAccount;
        config.endpoints.set(
            ServiceEndpointKind::BigQuery,
            Some(Arc::from("http://127.0.0.1:9050")),
        );
        config.external_account.audience = Some(Arc::from(
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        ));
        config.external_account.request_url = Some(Secret::from("http://127.0.0.1:1/token"));
        config.external_account.request_data = Some(Secret::from("client_secret=synthetic-secret"));
    });
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let error = ClientConfiguration::build(&settings).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.starts_with(
        "BigQuery option bigquery.auth.external_account.request_url must be an absolute HTTPS URL"
    ));
}

#[test]
fn every_endpoint_is_copied_into_the_client_configuration() {
    let settings = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::Anonymous;
        for (index, kind) in ServiceEndpointKind::ALL.into_iter().enumerate() {
            config.endpoints.set(
                kind,
                Some(Arc::from(format!("http://127.0.0.1:{}", 9000 + index))),
            );
        }
    });
    let configuration = ClientConfiguration::build(&settings).unwrap();
    assert_eq!(configuration.endpoints, settings.endpoints);
}

/// A destination whose path and query must never reach an error. The auth library reads the
/// principal out of an impersonation URL, so this has the path shape it requires.
const ATTACKER_URL: &str =
    "https://attacker.example/secret-path/serviceAccounts/sa:generateAccessToken?key=secret-query";

const IMPERSONATION_URL: &str = "https://iamcredentials.googleapis.com/v1/projects/-/\
                                 serviceAccounts/target@data-project.iam.gserviceaccount.com:\
                                 generateAccessToken";

fn authorized_user_document() -> serde_json::Value {
    json!({
        "type": "authorized_user",
        "client_id": "document-client",
        "client_secret": "document-client-secret",
        "refresh_token": "document-refresh-token",
    })
}

fn service_account_document() -> serde_json::Value {
    serde_json::from_str(&service_account_key(
        "document@data-project.iam.gserviceaccount.com",
    ))
    .unwrap()
}

fn external_account_document() -> serde_json::Value {
    json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {"file": "/unused/subject-token"},
    })
}

/// Reachable only as a `source_credentials`: the driver refuses the type at the top level.
fn external_account_authorized_user_document() -> serde_json::Value {
    json!({
        "type": "external_account_authorized_user",
        "client_id": "document-client",
        "client_secret": "document-client-secret",
        "refresh_token": "document-refresh-token",
        "token_url": "https://sts.googleapis.com/v1/oauthtoken",
    })
}

fn impersonated_document(source: serde_json::Value) -> serde_json::Value {
    let mut document = json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": IMPERSONATION_URL,
    });
    document["source_credentials"] = source;
    document
}

fn with_field(
    mut document: serde_json::Value,
    field: &str,
    value: impl Into<serde_json::Value>,
) -> serde_json::Value {
    document[field] = value.into();
    document
}

/// Each field that sends a credential, named as the refusal names it, set to `url`.
fn documents_sending_a_credential_to(url: &str) -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "token_url",
            with_field(external_account_document(), "token_url", url),
        ),
        (
            "service_account_impersonation_url",
            with_field(
                external_account_document(),
                "service_account_impersonation_url",
                url,
            ),
        ),
        (
            "token_uri",
            with_field(service_account_document(), "token_uri", url),
        ),
        (
            "token_uri",
            with_field(authorized_user_document(), "token_uri", url),
        ),
        (
            "service_account_impersonation_url",
            with_field(
                impersonated_document(authorized_user_document()),
                "service_account_impersonation_url",
                url,
            ),
        ),
        (
            "source_credentials.token_uri",
            impersonated_document(with_field(authorized_user_document(), "token_uri", url)),
        ),
        (
            "source_credentials.token_uri",
            impersonated_document(with_field(service_account_document(), "token_uri", url)),
        ),
        (
            "source_credentials.token_url",
            impersonated_document(with_field(external_account_document(), "token_url", url)),
        ),
        (
            "source_credentials.service_account_impersonation_url",
            impersonated_document(with_field(
                external_account_document(),
                "service_account_impersonation_url",
                url,
            )),
        ),
        (
            "source_credentials.token_url",
            impersonated_document(with_field(
                external_account_authorized_user_document(),
                "token_url",
                url,
            )),
        ),
    ]
}

/// Settings reading `document` as a string, and as a file while the returned guard lives. Lib
/// tests share one process, so `label` must be unique to the calling test.
fn in_both_credential_modes(
    label: &str,
    document: &serde_json::Value,
) -> (TemporaryFile, [DatabaseConfig; 2]) {
    let json = document.to_string();
    let file = TemporaryFile::new(label, &json);
    let mut from_file = json_credentials(file.path());
    from_file.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
    (file, [json_credentials(json), from_file])
}

/// Everything a refusal must not repeat: the URL's path and query, and the document around it.
const NEVER_QUOTED: [&str; 7] = [
    "secret-path",
    "secret-query",
    "document-client-secret",
    "document-refresh-token",
    "not-a-real-key",
    "/unused/subject-token",
    "\"type\"",
];

#[test]
fn a_credential_document_cannot_send_a_credential_outside_google() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let mut documents = documents_sending_a_credential_to(ATTACKER_URL);
    // The library refuses to build this chain, so only the refusal is observable.
    documents.push((
        "source_credentials.source_credentials.token_uri",
        impersonated_document(impersonated_document(with_field(
            authorized_user_document(),
            "token_uri",
            ATTACKER_URL,
        ))),
    ));
    for (field, document) in documents {
        let (_file, modes) = in_both_credential_modes("document-refused", &document);
        for settings in modes {
            let error = ClientConfiguration::build(&settings).unwrap_err();
            assert_failure(
                &error,
                Status::InvalidArguments,
                &[&format!(
                    "its {field} field resolves to host \"attacker.example\" over https;"
                )],
            );
            assert_message(&error, &["bigquery.allow_untrusted_endpoint=true"]);
            for quoted in NEVER_QUOTED {
                assert_message_omits(&error, &[quoted]);
            }
        }
    }

    let error = ClientConfiguration::build(&json_credentials(
        with_field(authorized_user_document(), "token_uri", ATTACKER_URL).to_string(),
    ))
    .unwrap_err();
    assert_eq!(
        error.message,
        "the JSON credential document in BigQuery option bigquery.auth.credentials must send \
         credentials over HTTPS to googleapis.com or accounts.google.com, but its token_uri \
         field resolves to host \"attacker.example\" over https; a credential would be sent \
         there; use https://oauth2.googleapis.com/token, or set \
         bigquery.allow_untrusted_endpoint=true as a database option to permit another host"
    );
}

#[test]
fn the_untrusted_endpoint_option_permits_every_document_destination() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for (field, document) in documents_sending_a_credential_to(ATTACKER_URL) {
        let (_file, modes) = in_both_credential_modes("document-opted-in", &document);
        for mut settings in modes {
            settings.allow_untrusted_endpoint = true;
            ClientConfiguration::build(&settings)
                .unwrap_or_else(|error| panic!("{field}: {error}"));
        }
    }
}

/// The opt-in lifts the trust rule, not the URL checks: every URL the driver hands the auth
/// library is still parsed, and a host the `idna` stub would panic on in a debug build (UP-103)
/// is refused, from an option or a document, whichever field names it.
#[test]
fn the_untrusted_endpoint_option_still_requires_ascii_url_hosts() {
    const PANICKING_HOST: &str = "https://B\u{fc}.example/token";
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let mut refused = vec![user_authentication_via(PANICKING_HOST, true)];
    let mut external = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::ExternalAccount;
        config.external_account.audience = Some(Arc::from(
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/p",
        ));
        config.external_account.request_url = Some(Secret::from("https://idp.example/token"));
        config.external_account.request_data = Some(Secret::from("grant_type=x"));
        config.external_account.token_url = Some(Arc::from(PANICKING_HOST));
    });
    external.allow_untrusted_endpoint = true;
    refused.push(external);
    let mut guards = Vec::new();
    for (_, document) in documents_sending_a_credential_to(PANICKING_HOST) {
        let (file, modes) = in_both_credential_modes("document-opted-in-non-ascii", &document);
        guards.push(file);
        for mut settings in modes {
            settings.allow_untrusted_endpoint = true;
            refused.push(settings);
        }
    }
    for settings in refused {
        let error = ClientConfiguration::build(&settings).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
        assert_message_omits(&error, &["B\u{fc}"]);
    }
    drop(guards);
}

/// A `credential_source` names the identity provider, so no trust rule applies to its URLs, but
/// the auth library parses them: one whose host the `idna` stub would panic on in a debug build
/// (UP-103) is refused, with or without the opt-in, at any depth.
#[test]
fn a_credential_source_url_must_have_an_ascii_host() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for (field, source) in [
        ("url", json!({"url": "https://B\u{fc}.example/token"})),
        ("url", json!({"url": "https://B%C3%BC.example/token"})),
        (
            "region_url",
            json!({
                "environment_id": "aws1",
                "region_url": "http://B\u{fc}/latest/meta-data/placement/availability-zone",
                "regional_cred_verification_url":
                    "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15",
            }),
        ),
        (
            "regional_cred_verification_url",
            json!({
                "environment_id": "aws1",
                "regional_cred_verification_url": "https://sts.{region}.B\u{fc}.com?Action=x",
            }),
        ),
        (
            "imdsv2_session_token_url",
            json!({
                "environment_id": "aws1",
                "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=x",
                "imdsv2_session_token_url": "http://B\u{fc}/latest/api/token",
            }),
        ),
    ] {
        let document = with_field(external_account_document(), "credential_source", source);
        for (prefix, document) in [
            ("", document.clone()),
            ("source_credentials.", impersonated_document(document)),
        ] {
            for opted_in in [false, true] {
                let mut settings = json_credentials(document.to_string());
                settings.allow_untrusted_endpoint = opted_in;
                let error = ClientConfiguration::build(&settings).unwrap_err();
                assert_eq!(
                    error.message,
                    format!(
                        "the JSON credential document in BigQuery option bigquery.auth.credentials \
                         has a {prefix}credential_source.{field} field whose host is not ASCII or \
                         is percent-encoded (value not echoed); write an internationalized host \
                         in Punycode (xn--)"
                    )
                );
            }
        }
    }
}

/// Trusting a REST endpoint trusts that endpoint alone, and a document is not an option.
#[test]
fn a_custom_rest_endpoint_does_not_trust_a_documents_destinations() {
    let mut settings = json_credentials(
        with_field(authorized_user_document(), "token_uri", ATTACKER_URL).to_string(),
    );
    settings.endpoints.set(
        ServiceEndpointKind::BigQuery,
        Some(Arc::from("http://127.0.0.1:9050")),
    );
    let error = ClientConfiguration::build(&settings).unwrap_err();
    assert_message(&error, &["its token_uri field"]);
}

/// The rule is the one option-supplied URLs meet, look-alikes and plaintext included.
#[test]
fn document_destinations_follow_the_credential_url_rule() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for (url, host, scheme) in [
        (
            "http://oauth2.googleapis.com/token",
            "oauth2.googleapis.com",
            "http",
        ),
        ("http://127.0.0.1:9050/token", "127.0.0.1", "http"),
        (
            "https://oauth2.googleapis.com.attacker.example/token",
            "oauth2.googleapis.com.attacker.example",
            "https",
        ),
        (
            "https://notgoogleapis.com/token",
            "notgoogleapis.com",
            "https",
        ),
        // The host the request reaches: a backslash ends the authority here as in `reqwest`.
        (
            "https://attacker.example\\.googleapis.com/token",
            "attacker.example",
            "https",
        ),
    ] {
        for (field, document) in documents_sending_a_credential_to(url) {
            let error =
                ClientConfiguration::build(&json_credentials(document.to_string())).unwrap_err();
            assert_message(
                &error,
                &[&format!(
                    "its {field} field resolves to host {host:?} over {scheme};"
                )],
            );
            assert_eq!(
                error.message.contains("in cleartext"),
                scheme == "http",
                "{url}: {}",
                error.message
            );
        }
    }

    // Older Google-issued keys name the accounts host, which only OAuth token requests reach.
    for url in [
        "https://accounts.google.com/o/oauth2/token",
        "https://accounts.google.com./o/oauth2/token",
    ] {
        for (field, document) in documents_sending_a_credential_to(url) {
            let outcome = ClientConfiguration::build(&json_credentials(document.to_string()));
            if field.ends_with("token_uri") {
                outcome.unwrap_or_else(|error| panic!("{field}: {error}"));
            } else {
                let error = outcome.unwrap_err();
                assert_message(&error, &["over HTTPS to googleapis.com, but"]);
            }
        }
    }
}

/// A value that cannot be judged is refused unquoted, since it may be a misplaced secret.
#[test]
fn a_malformed_document_destination_is_refused_without_echoing_it() {
    for value in [
        "not-a-url-document-secret",
        "ftp://oauth2.googleapis.com/document-secret",
        "https://user:document-secret@oauth2.googleapis.com/token",
        "https://oauth2.googleapis.com/token#document-secret",
        "https://oauth2.googleapis.com/document secret",
        "",
    ] {
        for (field, document) in documents_sending_a_credential_to(value) {
            let error =
                ClientConfiguration::build(&json_credentials(document.to_string())).unwrap_err();
            assert_failure(
                &error,
                Status::InvalidArguments,
                &[&format!(
                    "has a {field} field that does not resolve to a host (value not echoed)"
                )],
            );
            assert!(
                !error.message.contains("document-secret")
                    && !error.message.contains("document secret"),
                "{value}: {}",
                error.message
            );
        }
    }
}

/// What Google issues passes untouched, including the fields that receive no credential.
#[test]
fn google_issued_credential_documents_are_accepted() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let mut key = service_account_document();
    key["token_uri"] = "https://oauth2.googleapis.com/token".into();
    key["auth_uri"] = "https://accounts.google.com/o/oauth2/auth".into();
    key["auth_provider_x509_cert_url"] = "https://www.googleapis.com/oauth2/v1/certs".into();
    key["client_x509_cert_url"] = "https://www.googleapis.com/robot/v1/metadata/x509/x".into();
    key["universe_domain"] = "googleapis.com".into();
    for document in [
        key.clone(),
        authorized_user_document(),
        with_field(
            authorized_user_document(),
            "token_uri",
            "https://oauth2.googleapis.com/token",
        ),
        external_account_document(),
        with_field(
            external_account_document(),
            "token_url",
            "https://sts.eu.rep.googleapis.com/v1/token",
        ),
        with_field(
            external_account_document(),
            "service_account_impersonation_url",
            IMPERSONATION_URL,
        ),
        impersonated_document(key),
        impersonated_document(authorized_user_document()),
        impersonated_document(external_account_document()),
        impersonated_document(external_account_authorized_user_document()),
    ] {
        ClientConfiguration::build(&json_credentials(document.to_string()))
            .unwrap_or_else(|error| panic!("{document}: {error}"));
    }
}

/// `credential_source` names the customer's identity provider, which hands a subject token to
/// the driver rather than receiving one. It legitimately lives anywhere, over any scheme.
#[test]
fn a_credential_source_is_not_a_credential_destination() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for source in [
        json!({"url": "http://169.254.169.254/token?resource=x"}),
        json!({
            "url": "https://idp.corp.example/token",
            "headers": {"Metadata": "True"},
            "format": {"type": "json", "subject_token_field_name": "access_token"},
        }),
        json!({"file": "/var/run/secrets/token"}),
        json!({
            "environment_id": "aws1",
            "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
            "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
            "regional_cred_verification_url":
                "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15",
            "imdsv2_session_token_url": "http://169.254.169.254/latest/api/token",
        }),
    ] {
        let document = with_field(external_account_document(), "credential_source", source);
        for document in [document.clone(), impersonated_document(document)] {
            ClientConfiguration::build(&json_credentials(document.to_string()))
                .unwrap_or_else(|error| panic!("{document}: {error}"));
        }
    }
}

/// A destination of the wrong JSON type is the builder's to refuse, in its own redacted words.
#[test]
fn a_document_destination_of_the_wrong_type_is_left_to_the_builder() {
    let error = ClientConfiguration::build(&json_credentials(
        with_field(authorized_user_document(), "token_uri", 7).to_string(),
    ))
    .unwrap_err();
    assert!(
        error
            .message
            .starts_with("the Google auth library could not use"),
        "{}",
        error.message
    );
}

/// `google-cloud-auth` sends the source token to `iamcredentials.{universe_domain}` when it
/// impersonates by target principal, which `bigquery.impersonate.target_principal` does. The
/// URL is parsed as `reqwest` parses it, so a value cannot dress another host up as Google's.
#[test]
fn a_documents_universe_cannot_redirect_impersonation() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let impersonating = |document: &serde_json::Value| {
        let mut settings = json_credentials(document.to_string());
        settings.impersonation.target_principal =
            Some(Arc::from("target@data-project.iam.gserviceaccount.com"));
        settings
    };
    let universe =
        |universe: &str| with_field(service_account_document(), "universe_domain", universe);

    for (field, document) in [
        ("universe_domain", universe("attacker.example")),
        (
            "universe_domain",
            with_field(
                external_account_document(),
                "universe_domain",
                "attacker.example",
            ),
        ),
        (
            "source_credentials.universe_domain",
            impersonated_document(universe("attacker.example")),
        ),
        (
            "universe_domain",
            universe("attacker.example/.googleapis.com"),
        ),
        (
            "universe_domain",
            universe("attacker.example\\.googleapis.com"),
        ),
    ] {
        let error = ClientConfiguration::build(&impersonating(&document)).unwrap_err();
        assert_failure(
            &error,
            Status::InvalidArguments,
            &[&format!(
                "its {field} field resolves to host \"iamcredentials.attacker.example\" over https"
            )],
        );
    }
    for value in ["user@attacker.example", "attacker.example#.googleapis.com"] {
        let error = ClientConfiguration::build(&impersonating(&universe(value))).unwrap_err();
        assert!(
            error.message.contains(
                "has a universe_domain field that does not resolve to a host (value not echoed)"
            ) && !error.message.contains("attacker.example"),
            "{value}: {}",
            error.message
        );
    }

    ClientConfiguration::build(&impersonating(&universe("googleapis.com"))).unwrap();
    let mut opted_in = impersonating(&universe("attacker.example"));
    opted_in.allow_untrusted_endpoint = true;
    ClientConfiguration::build(&opted_in).unwrap();
}

/// Without impersonation the universe picks no destination: the REST client uses its configured
/// endpoint, and the service clients refuse a universe the environment does not also name. dbt
/// sends `impersonate.scopes` on every connection, so that alone must not count.
#[test]
fn a_documents_universe_is_unchecked_without_impersonation() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let document = with_field(
        service_account_document(),
        "universe_domain",
        "other-universe.example",
    );
    let mut settings = json_credentials(document.to_string());
    ClientConfiguration::build(&settings).unwrap();
    settings.impersonation.scopes = vec![Arc::from("scope-one")];
    ClientConfiguration::build(&settings).unwrap();
}

const ADC_CHILD: &str = "BIGQUERY_ADBC_AMBIENT_ADC_TEST_CHILD";

/// Ambient credentials are the machine owner's own configuration, not a document a caller handed
/// the driver, so the document rule stops at `bigquery.auth.credentials`. The same document is
/// refused there. A subprocess isolates ADC discovery from parallel tests.
#[test]
fn ambient_application_default_credentials_are_not_checked_as_documents() {
    let document = with_field(authorized_user_document(), "token_uri", ATTACKER_URL).to_string();
    if std::env::var_os(ADC_CHILD).is_some() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let _guard = runtime.enter();
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
        });
        ClientConfiguration::build(&settings).unwrap();
        return;
    }
    ClientConfiguration::build(&json_credentials(document.as_str())).unwrap_err();
    let file = TemporaryFile::new("ambient-adc-document", &document);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "config::tests::trust::ambient_application_default_credentials_are_not_checked_as_documents",
        ])
        .env(ADC_CHILD, "1")
        .env("GOOGLE_APPLICATION_CREDENTIALS", file.path())
        .status()
        .unwrap();
    assert!(status.success());
}
