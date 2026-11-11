//! Which options each authentication type accepts, and the refusals of the rest.

use super::*;

/// A JSON key configured without its authentication type must not fall through to Application
/// Default Credentials and connect as the ambient identity. The credentials row also carries a
/// non-default authentication type, so the assertion that the configured type is named in the
/// refusal is exercised against one.
#[test]
fn credential_options_without_their_authentication_type_are_rejected() {
    for (option, required, apply) in [
        (
            "bigquery.auth.credentials",
            "\"json_credential_file\", or \"json_credential_string\"",
            (|config: &mut DatabaseConfig| {
                config.authentication_type = BigQueryAuthenticationType::Anonymous;
                config.credentials = Some(Secret::from(r#"{"type":"service_account"}"#));
            }) as fn(&mut DatabaseConfig),
        ),
        (
            "bigquery.auth.credentials_type",
            "\"json_credential_file\", or \"json_credential_string\"",
            |config| {
                config.credentials_type = Some(BigQueryCredentialsType::ServiceAccount);
            },
        ),
        (
            "bigquery.auth.client_id",
            "\"user_authentication\"",
            |config| {
                config.user_authentication.client_id = Some(Arc::from("id"));
            },
        ),
        (
            "bigquery.auth.client_secret",
            "\"user_authentication\"",
            |config| {
                config.user_authentication.client_secret = Some(Secret::from("secret"));
            },
        ),
        (
            "bigquery.auth.refresh_token",
            "\"user_authentication\"",
            |config| {
                config.user_authentication.refresh_token = Some(Secret::from("token"));
            },
        ),
    ] {
        let settings = settings(apply);
        let error = ClientConfiguration::build(&settings).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{option}");
        assert!(
            error.message.contains(&format!("{option} is set"))
                && error.message.contains(required)
                && error
                    .message
                    .contains(settings.authentication_type.as_str()),
            "{option}: {}",
            error.message
        );
    }
}

#[test]
fn anonymous_authentication_still_rejects_quota_project_and_impersonation() {
    let quota = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::Anonymous;
        config.quota_project = Some(ProjectIdentifier::try_new("project").unwrap());
    });
    assert_eq!(
        ClientConfiguration::build(&quota).unwrap_err().message,
        "BigQuery option bigquery.auth.quota_project cannot be used with bigquery.auth_type \
         \"anonymous\", which mints no token to attribute quota to; unset the option, or use an \
         authentication type that mints tokens"
    );

    // The rejection names the impersonation option that was set, not the feature.
    for (option, configure) in [
        (
            "bigquery.impersonate.lifetime",
            &(|config: &mut DatabaseConfig| {
                config.impersonation.lifetime = Some(Duration::from_secs(60));
            }) as &dyn Fn(&mut DatabaseConfig),
        ),
        (
            "bigquery.impersonate.delegates",
            &|config: &mut DatabaseConfig| {
                config.impersonation.delegates = vec![Arc::from("first@example.com")];
            },
        ),
        (
            "bigquery.impersonate.target_principal",
            &|config: &mut DatabaseConfig| {
                config.impersonation.target_principal = Some(Arc::from("sa@example.com"));
            },
        ),
        // Scopes do not turn impersonation on (dbt 2.0 sets them on every connection), but
        // under `anonymous` they can shape nothing, so they are refused rather than dropped, like
        // `bigquery.auth.scopes`.
        (
            "bigquery.impersonate.scopes",
            &|config: &mut DatabaseConfig| {
                config.impersonation.scopes =
                    vec![Arc::from("https://www.googleapis.com/auth/bigquery")];
            },
        ),
    ] {
        let impersonation = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::Anonymous;
            configure(config);
        });
        let message = ClientConfiguration::build(&impersonation)
            .unwrap_err()
            .message;
        assert!(
            message.starts_with(&format!(
                "BigQuery option {option} cannot be used with bigquery.auth_type \"anonymous\""
            )),
            "{message}"
        );
    }
}

/// A missing impersonation target is a configuration mistake, reported before the source
/// credential file is read.
#[test]
fn a_missing_impersonation_target_is_refused_before_the_source_is_read() {
    let missing_target = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
        config.credentials = Some(Secret::from("/nonexistent/bigquery-adbc-key.json"));
        config.impersonation.lifetime = Some(Duration::from_secs(60));
    });
    let message = ClientConfiguration::build(&missing_target)
        .unwrap_err()
        .message;
    assert!(
        message.starts_with(
            "BigQuery option bigquery.impersonate.target_principal is required once any of"
        ),
        "{message}"
    );
}

/// Scopes reach the credential the authentication type builds, except for the two types that
/// mint no token, which refuse them rather than dropping them.
#[test]
fn credential_scopes_are_refused_by_the_types_that_mint_no_token() {
    for (authentication_type, configure) in [
        (
            BigQueryAuthenticationType::TemporaryAccessToken,
            &(|config: &mut DatabaseConfig| {
                config.access_token = Some(Secret::from("token"));
            }) as &dyn Fn(&mut DatabaseConfig),
        ),
        (BigQueryAuthenticationType::Anonymous, &|_| ()),
    ] {
        let settings = settings(|config| {
            config.authentication_type = authentication_type;
            config.scopes = vec![Arc::from("https://www.googleapis.com/auth/drive")];
            configure(config);
        });
        let error = ClientConfiguration::build(&settings).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.starts_with(&format!(
                "BigQuery option bigquery.auth.scopes cannot be used with bigquery.auth_type \
                 {:?}",
                authentication_type.as_str()
            )),
            "{}",
            error.message
        );
    }

    // A JSON service-account key takes them, for example `.../auth/drive` to read a Google
    // Sheets external table.
    let runtime = crate::runtime::new_runtime().unwrap();
    let _guard = runtime.enter();
    let scoped = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialString;
        config.credentials = Some(Secret::from(service_account_key("sa@example.com")));
        config.scopes = vec![
            Arc::from("https://www.googleapis.com/auth/bigquery"),
            Arc::from("https://www.googleapis.com/auth/drive"),
        ];
    });
    ClientConfiguration::build(&scoped).unwrap();
}

/// Every configuration rejection uses the "BigQuery option {key} must ..." form.
#[test]
fn configuration_rejections_name_the_option_the_way_every_other_one_does() {
    let unparseable = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
        config.endpoints.set(
            ServiceEndpointKind::BigQuery,
            Some(Arc::from("bigquery.googleapis.com")),
        );
    });
    assert_eq!(
        unparseable.validate_endpoints().unwrap_err().message,
        "BigQuery option bigquery.endpoint must be a valid HTTP or HTTPS endpoint"
    );

    let unused = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::ApplicationDefault;
        config.access_token = Some(Secret::from("token"));
    });
    assert_eq!(
        ClientConfiguration::build(&unused).unwrap_err().message,
        "BigQuery option bigquery.auth.access_token is set but bigquery.auth_type is \
         \"app_default_credentials\"; it requires bigquery.auth_type \"temporary_access_token\""
    );

    // The required types come from the enum rather than a prose list, and are quoted the same
    // way the configured one is.
    let json_only = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::UserAuthentication;
        config.credentials = Some(Secret::from("{}"));
    });
    assert_eq!(
        ClientConfiguration::build(&json_only).unwrap_err().message,
        "BigQuery option bigquery.auth.credentials is set but bigquery.auth_type is \
         \"user_authentication\"; it requires bigquery.auth_type \"json_credential_file\", or \
         \"json_credential_string\""
    );

    // A required option names the type that requires it, not the feature.
    let missing = settings(|config| {
        config.authentication_type = BigQueryAuthenticationType::JsonCredentialFile;
    });
    assert_eq!(
        ClientConfiguration::build(&missing).unwrap_err().message,
        "BigQuery option bigquery.auth.credentials is required for bigquery.auth_type \
         \"json_credential_file\"; set it to the path of a Google Cloud JSON credential file \
         for \"json_credential_file\", or to the document itself for \"json_credential_string\""
    );
}
