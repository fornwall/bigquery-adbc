//! Building each credential type, and what a rejected credential may say.

use super::*;
use crate::config::rejections::{
    application_default_credentials_failure, unreadable_credential_file,
};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};

/// The execution principal an operation may name, per authentication type.
///
/// Derived only from the configuration: no request is made and no OAuth scope is added, unlike
/// the fork's `oauth2/v2/userinfo` lookup.
#[test]
fn the_execution_principal_is_derived_only_where_the_credentials_name_one() {
    const EMAIL: &str = "dbt-runner@data-project.iam.gserviceaccount.com";
    // Impersonation names the principal the tokens are actually minted for, so it outranks
    // the source key's own email.
    const TARGET: &str = "impersonated@data-project.iam.gserviceaccount.com";

    // Building credentials arms a token cache, which needs a Tokio reactor.
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();

    let named = |label: &str, expected: Option<&str>, configure: &dyn Fn(&mut DatabaseConfig)| {
        let settings = settings(|config| configure(config));
        let configuration = ClientConfiguration::build(&settings)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        assert_eq!(
            configuration.execution_principal.as_deref(),
            expected,
            "{label}"
        );
    };

    named("a service-account key", Some(EMAIL), &|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials = Some(Secret::from(service_account_key(EMAIL)));
    });
    let key_file = TemporaryFile::new("execution-principal", service_account_key(EMAIL));
    named("a service-account key file", Some(EMAIL), &|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
        config.credentials = Some(Secret::from(key_file.path()));
    });
    // A key with no `client_email` names nobody rather than an empty principal.
    named("a service-account key without an email", None, &|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials = Some(Secret::from(
            r#"{"type":"service_account","project_id":"p","private_key_id":"k","private_key":"pem","client_email":""}"#,
        ));
    });
    // An authorized-user credential does not identify a service-account principal. Its asserted
    // type matches, so it is accepted.
    named("an authorized-user key", None, &|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials_type = Some(BigQueryCredentialsType::AuthorizedUser);
        config.credentials = Some(Secret::from(
            r#"{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"token"}"#,
        ));
    });
    named("user authentication", None, &|config| {
        config.authentication_type = BigQueryAuthenticationType::UserAuthentication;
        config.user_authentication.client_id = Some(Arc::from("id"));
        config.user_authentication.client_secret = Some(Secret::from("secret"));
        config.user_authentication.refresh_token = Some(Secret::from("token"));
    });
    named("a temporary access token", None, &|config| {
        config.authentication_type = BigQueryAuthenticationType::TemporaryAccessToken;
        config.access_token = Some(Secret::from("static-token-value"));
    });
    named("anonymous authentication", None, &|config| {
        config.authentication_type = BigQueryAuthenticationType::Anonymous;
    });

    named("an impersonation target", Some(TARGET), &|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials = Some(Secret::from(service_account_key(EMAIL)));
        config.impersonation.target_principal = Some(Arc::from(TARGET));
    });
}

/// A JSON impersonation target is the executing identity, even when the source is a different
/// service account. Custom endpoints remain usable without assuming what identity they mint.
#[test]
fn json_impersonation_credentials_name_their_target_principal() {
    const TARGET: &str = "target@data-project.iam.gserviceaccount.com";
    const OUTER: &str = "outer@data-project.iam.gserviceaccount.com";
    const IAM: &str = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";

    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    for credential_type in ["impersonated_service_account", "external_account"] {
        let mut document = if credential_type == "impersonated_service_account" {
            serde_json::json!({
                "type": credential_type,
                "source_credentials": serde_json::from_str::<serde_json::Value>(
                    &service_account_key("source@data-project.iam.gserviceaccount.com")
                ).unwrap(),
            })
        } else {
            serde_json::json!({
                "type": credential_type,
                "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": {"file": "/unused/subject-token"},
            })
        };
        for (url, expected) in [
            (format!("{IAM}{TARGET}:generateAccessToken"), Some(TARGET)),
            (
                format!("{IAM}{}:generateAccessToken", TARGET.replace('@', "%40")),
                Some(TARGET),
            ),
            (
                format!(
                    "https://custom.example/v1/projects/-/serviceAccounts/{TARGET}:generateAccessToken"
                ),
                None,
            ),
            (format!("{IAM}target%2Fother:generateAccessToken"), None),
        ] {
            document["service_account_impersonation_url"] = url.into();
            let json = document.to_string();
            let file = TemporaryFile::new("json-impersonation-principal", &json);
            for authentication_type in [
                BigQueryAuthenticationType::JsonCredentialFile,
                BigQueryAuthenticationType::JsonCredentialString,
            ] {
                let mut settings = settings(|config| {
                    config.authentication_type = authentication_type;
                    config.allow_untrusted_endpoint = true;
                    config.credentials = Some(Secret::from(
                        if authentication_type == BigQueryAuthenticationType::JsonCredentialFile {
                            file.path()
                        } else {
                            &json
                        },
                    ));
                });
                let configuration = ClientConfiguration::build(&settings).unwrap();
                assert_eq!(
                    configuration.execution_principal.as_deref(),
                    expected,
                    "{credential_type} via {authentication_type:?}"
                );

                settings.impersonation.target_principal = Some(Arc::from(OUTER));
                let configuration = ClientConfiguration::build(&settings).unwrap();
                assert_eq!(configuration.execution_principal.as_deref(), Some(OUTER));
            }
        }
        if credential_type == "external_account" {
            document
                .as_object_mut()
                .unwrap()
                .remove("service_account_impersonation_url");
            let configuration =
                ClientConfiguration::build(&json_credentials(document.to_string())).unwrap();
            assert_eq!(configuration.execution_principal, None);
        }
    }
}

/// The derived principal is an identifier, but the document it came from is not: a rejected
/// key must not put any of it in an error, and the redacted `Debug` must not either.
#[test]
fn a_rejected_credential_document_never_reaches_an_error_or_the_debug_output() {
    const EMAIL: &str = "dbt-runner@data-project.iam.gserviceaccount.com";
    const PRIVATE_KEY: &str = "MIIEvgIBADANBgkqhkiG9w0-secret-material";

    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();

    let truncated = json_credentials(format!(
        r#"{{"type":"service_account","client_email":"{EMAIL}","private_key":"{PRIVATE_KEY}""#
    ));
    let error = ClientConfiguration::build(&truncated).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message_omits(&error, &[PRIVATE_KEY, EMAIL]);

    let accepted = json_credentials(service_account_key(EMAIL));
    let rendered = format!("{:?}", ClientConfiguration::build(&accepted).unwrap());
    assert!(
        rendered.contains("credentials: \"<redacted>\""),
        "{rendered}"
    );
    assert!(
        rendered.contains(EMAIL),
        "the principal is an identifier an operator needs to see: {rendered}"
    );
    assert!(!rendered.contains("PRIVATE KEY"), "{rendered}");
}

/// A missing path is a permanent configuration mistake, not a retryable `Status::IO` that retry
/// loops would repeat. The error names the option the value came from, never the value.
#[test]
fn unreadable_credential_file_error_names_the_option_and_the_cause_but_not_the_path() {
    let path = "/nonexistent/bigquery-credentials.json";
    let settings = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
        config.credentials = Some(Secret::from(path));
    });
    let error = ClientConfiguration::build(&settings).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.starts_with(
            "BigQuery option bigquery.auth.credentials names a JSON credential file that could \
             not be read (value not echoed): "
        ),
        "{}",
        error.message
    );
    // The io::Error cause and the remedy.
    assert_message(&error, &["os error", "\"json_credential_string\""]);
    assert_message_omits(&error, &[path]);
}

/// Confusing `json_credential_file` with `json_credential_string` (or `OAuthType=1` with
/// `OAuthType=2` in a URI) hands a service-account key to the file open. The error must not echo
/// it, because ADBC errors reach `dbt.log`.
#[test]
fn a_credential_document_in_the_file_path_option_is_refused_without_echoing_it() {
    const EMAIL: &str = "dbt-runner@data-project.iam.gserviceaccount.com";

    for document in [
        service_account_key(EMAIL),
        // Pretty-printed, which is how a key file is read into a profile by hand: the newlines
        // alone disqualify it as a path even before its leading brace does.
        serde_json::to_string_pretty(&serde_json::json!({
            "type": "service_account",
            "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
        }))
        .unwrap(),
        // A bare PEM block is not JSON, and is refused by its newlines.
        "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n".to_owned(),
    ] {
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
            config.credentials = Some(Secret::from(document.clone()));
        });
        let error = ClientConfiguration::build(&settings).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(
            &error,
            &["private_key", "not-a-real-key", "BEGIN PRIVATE KEY", EMAIL],
        );
        assert!(
            error
                .message
                .contains("must be a JSON credential file path")
                && error.message.contains("\"json_credential_string\""),
            "the remedy names the option that takes a document: {}",
            error.message
        );
    }
}

/// A directory, a path through a file, an overlong name, a path the process may not open, and a
/// file that is not text are the same permanent mistake a missing path is. Anything else the
/// filesystem reports keeps `Status::IO`, which is the one class a retry can still clear.
#[test]
fn a_credential_file_failure_is_retryable_only_when_trying_again_could_help() {
    use std::io::ErrorKind as Kind;

    for (kind, expected) in [
        (Kind::NotFound, Status::InvalidArguments),
        (Kind::PermissionDenied, Status::InvalidArguments),
        (Kind::IsADirectory, Status::InvalidArguments),
        (Kind::NotADirectory, Status::InvalidArguments),
        (Kind::InvalidFilename, Status::InvalidArguments),
        (Kind::InvalidInput, Status::InvalidArguments),
        (Kind::InvalidData, Status::InvalidArguments),
        (Kind::Interrupted, Status::IO),
        (Kind::TimedOut, Status::IO),
    ] {
        let failure = unreadable_credential_file(&std::io::Error::from(kind));
        assert_eq!(failure.status, expected, "{kind:?}");
    }

    // A P12 key, Google's other key format, is binary, which reading it as text reports as
    // `InvalidData`.
    let p12_key = TemporaryFile::new("p12-key", b"\x30\x82\x09\xff\xfe");
    let failure = ClientConfiguration::build(&settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
        config.credentials = Some(Secret::from(p12_key.path()));
    }))
    .unwrap_err();
    assert_eq!(
        failure.status,
        Status::InvalidArguments,
        "{}",
        failure.message
    );
}

/// A credential file path can arrive in a URI, so it can name a log file or `/dev/zero`. No
/// credential document comes near 1 MiB, so a file that does is refused after reading only that
/// much instead of being read into memory whole, and the path is not echoed.
#[test]
fn a_credential_file_over_one_mebibyte_is_refused_after_a_bounded_read() {
    const LIMIT: usize = 1024 * 1024;

    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let file_settings = |file: &TemporaryFile| {
        settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
            config.credentials = Some(Secret::from(file.path()));
        })
    };
    // JSON allows leading whitespace, so padding keeps the document valid at any size.
    let padded = |size: usize| {
        let document = service_account_key("padded@data-project.iam.gserviceaccount.com");
        format!("{}{document}", " ".repeat(size - document.len()))
    };

    let at_limit = TemporaryFile::new("credential-at-limit", padded(LIMIT));
    ClientConfiguration::build(&file_settings(&at_limit)).unwrap();

    let oversized = TemporaryFile::new("credential-over-limit", padded(LIMIT + 1));
    let error = ClientConfiguration::build(&file_settings(&oversized)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.auth.credentials names a file larger than 1 MiB (value not \
         echoed), too large for a JSON credential document; set it to the path of a Google \
         Cloud JSON credential file"
    );
    assert_message_omits(&error, &[oversized.path()]);
}

/// Both halves of a credential-type rejection are type names rather than secrets: the assertion
/// the operator made, what the document turned out to be, and the set that exists at all. The auth
/// library's own error is dropped because it can quote the document, so a document it could not
/// use is answered with the fields that type requires.
#[test]
fn credential_type_rejections_name_the_types_involved() {
    const EMAIL: &str = "dbt-runner@data-project.iam.gserviceaccount.com";

    let mismatched = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials_type = Some(BigQueryCredentialsType::AuthorizedUser);
        config.credentials = Some(Secret::from(service_account_key(EMAIL)));
    });
    let error = ClientConfiguration::build(&mismatched).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.auth.credentials_type is \"authorized_user\" but the configured \
         JSON credentials are \"service_account\"; set it to \"service_account\", or supply \
         credentials of the type it asserts"
    );

    let unsupported = json_credentials(r#"{"type":"gce_metadata"}"#);
    let error = ClientConfiguration::build(&unsupported).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "the JSON credential document in BigQuery option bigquery.auth.credentials has a missing \
         or unsupported type; its type field must be exactly one of \"service_account\", \
         \"authorized_user\", \"impersonated_service_account\", \"external_account\""
    );

    let malformed = json_credentials(r#"{"private_key": hunter2"#);
    let error = ClientConfiguration::build(&malformed).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.auth.credentials must supply a JSON credential document, but it \
         has a syntax error at line 1 column 17"
    );

    // Option spellings match case-insensitively; the document's own `type` does not, because
    // Google matches it exactly and the builder's rejection would not mention case.
    let miscased = json_credentials(r#"{"type":"Service_Account"}"#);
    assert!(
        ClientConfiguration::build(&miscased)
            .unwrap_err()
            .message
            .contains("must be exactly one of")
    );

    let incomplete = json_credentials(
        r#"{"type":"service_account","client_email":"sa@example.com","private_key_id":"secret-id"}"#,
    );
    let error = ClientConfiguration::build(&incomplete).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "the Google auth library could not use the \"service_account\" credentials in BigQuery \
         option bigquery.auth.credentials; check that the document is as Google issued it and \
         has client_email, private_key_id, private_key and project_id"
    );
}

/// The static token is used exactly as given: one sensitive `Authorization` header, with the
/// not-modified contract honored for a caller that presents the entity tag back. A token that
/// cannot form a header is refused without being echoed.
///
/// `bigquery.auth.quota_project` becomes an `x-goog-user-project` header for every other
/// authentication type, because `google-cloud-auth` writes it. The static-token credential is the
/// driver's own, so it must write the header itself or the option would be silently dropped.
#[test]
fn temporary_access_token_authentication_builds_static_credentials() {
    use google_cloud_auth::credentials::CacheableResource;

    let runtime = crate::runtime::new_runtime().unwrap();
    for quota_project in [None, Some("quota-project")] {
        let mut settings = temporary_access_token("static-token-value");
        settings.quota_project =
            quota_project.map(|project| ProjectIdentifier::try_new(project).unwrap());
        let configuration = ClientConfiguration::build(&settings).unwrap();
        let CacheableResource::New { entity_tag, data } = runtime
            .block_on(configuration.credentials.headers(http::Extensions::new()))
            .unwrap()
        else {
            panic!("expected new headers from a fresh request");
        };
        let value = data.get(http::header::AUTHORIZATION).unwrap();
        assert!(value.is_sensitive());
        assert_eq!(value.to_str().unwrap(), "Bearer static-token-value");
        // Unset stays unset: the header is absent rather than empty.
        assert_eq!(
            data.get("x-goog-user-project")
                .map(|project| project.to_str().unwrap()),
            quota_project
        );

        let mut extensions = http::Extensions::new();
        extensions.insert(entity_tag);
        assert_eq!(
            runtime
                .block_on(configuration.credentials.headers(extensions))
                .unwrap(),
            CacheableResource::NotModified
        );
    }

    let error =
        ClientConfiguration::build(&temporary_access_token("line\nbreak-secret")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message_omits(&error, &["break-secret"]);
}

/// Each of the three causes gets its own remedy. When `GOOGLE_APPLICATION_CREDENTIALS` named the
/// document, the remedy is to repoint or unset it: `gcloud auth application-default login` writes
/// a file the variable overrides.
#[test]
fn application_default_credentials_that_cannot_be_loaded_report_the_cause_and_the_ways_out() {
    let alternatives = "or set bigquery.auth_type to \"json_credential_file\", \
                        \"json_credential_string\", or \"temporary_access_token\"";

    let absent = "/nonexistent/adc.json";
    // The operating system words the cause, and differently on each platform.
    let cause = std::fs::File::open(absent).unwrap_err();
    assert_eq!(cause.kind(), std::io::ErrorKind::NotFound);
    let missing = application_default_credentials_failure(Some(absent.as_ref()));
    assert_eq!(missing.status, Status::Unauthenticated);
    assert!(
        missing.message.starts_with(&format!(
            "the GOOGLE_APPLICATION_CREDENTIALS environment variable names a credential file \
             that could not be read (value not echoed): {cause}"
        )),
        "{}",
        missing.message
    );
    assert_message_omits(&missing, &[absent]);
    assert!(
        missing.message.ends_with(alternatives),
        "{}",
        missing.message
    );

    let readable = std::env::current_exe().unwrap();
    let unusable = application_default_credentials_failure(Some(readable.as_os_str()));
    assert_message(
        &unusable,
        &[
            "could not be loaded (value not echoed); a usable credential document is JSON whose type field is one of \"service_account\"",
        ],
    );
    assert_message_omits(&unusable, &[readable.to_str().unwrap()]);

    let unset = application_default_credentials_failure(None);
    assert!(
        unset.message.starts_with(
            "GOOGLE_APPLICATION_CREDENTIALS is unset, and the Application Default \
                 Credentials file `gcloud` writes could not be loaded"
        ),
        "{}",
        unset.message
    );
    assert_message(
        &unset,
        &["run `gcloud auth application-default login` to rewrite it"],
    );

    // A document where a path belongs is never echoed.
    let document = application_default_credentials_failure(Some(
        r#"{"type":"service_account","private_key":"secret"}"#.as_ref(),
    ));
    assert_message_omits(&document, &["secret"]);
    assert_message(&document, &["is not a file path"]);
}

/// A token endpoint answering with a retryable status is a service outage (`IO`, which the REST
/// retry loops retry), not a rejected credential; a refusal stays `UNAUTHENTICATED` with the
/// remedy for the credential.
#[test]
fn an_unavailable_token_endpoint_is_an_io_failure_and_a_refusal_is_not() {
    use crate::rest::request::{Request, send};

    let runtime = crate::runtime::new_runtime().unwrap();
    for (status, body, expected, remedy) in [
        (
            503,
            serde_json::json!({"error": "backend_error"}),
            Status::IO,
            "HTTP 503",
        ),
        (
            400,
            serde_json::json!({"error": "invalid_grant"}),
            Status::Unauthenticated,
            "could not be redeemed",
        ),
    ] {
        let (endpoint, captured) =
            crate::test_support::http::capture_requests(vec![(status, body)]);
        let settings = user_authentication_via(&format!("{endpoint}/token"), true);
        let configuration = {
            let _guard = runtime.enter();
            ClientConfiguration::build(&settings).unwrap()
        };
        let client = crate::rest::client::rest_client("test").unwrap();
        let failure = runtime
            .block_on(send(
                None,
                &configuration.credentials,
                &client,
                Request::get("http://127.0.0.1:1/unused", "describe it"),
            ))
            .unwrap_err();
        assert_eq!(failure.status, expected, "{status}: {}", failure.message);
        assert_message(&failure, &["failed to authenticate to describe it", remedy]);
        captured.served(1);
    }

    // Explicit user credentials cannot fall back to a metadata server. A connection failure
    // therefore needs transport recovery, not a replacement refresh token.
    let configuration = {
        let _guard = runtime.enter();
        ClientConfiguration::build(&user_authentication_via("http://127.0.0.1:1/token", true))
            .unwrap()
    };
    let failure = runtime
        .block_on(send(
            None,
            &configuration.credentials,
            &crate::rest::client::rest_client("test").unwrap(),
            Request::get("http://127.0.0.1:1/unused", "describe it"),
        ))
        .unwrap_err();
    assert_failure(
        &failure,
        Status::IO,
        &["could not complete a token exchange"],
    );
    assert_message_omits(&failure, &["refresh token", "127.0.0.1"]);
}
