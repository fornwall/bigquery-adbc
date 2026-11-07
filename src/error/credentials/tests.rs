use super::*;
use crate::test_support::errors::assert_message;

const CONTEXT: &str = "BigQuery query execution failed";

/// A failure carrying only its layer's attribution.
fn attributed(layer: CredentialLayer) -> CredentialFailure {
    CredentialFailure::new(layer, None, None)
}

fn converted(failure: CredentialFailure) -> Error {
    crate::error::google_error(
        CONTEXT,
        GoogleError::authentication(CredentialsError::from_source(false, failure)),
    )
}

/// The remedy names only the auth types that can supply a credential.
fn adc_remedy() -> String {
    format!(
        "no usable Google Cloud Application Default Credentials, which are read from \
         {ADC_SOURCES}; run `gcloud auth application-default login`, or set bigquery.auth_type to \
         one of \"json_credential_file\", \"json_credential_string\", or \
         \"temporary_access_token\""
    )
}

/// A missing credential is recognized through the retry-exhausted wrapper it arrives in, and
/// answered with the remedy and the chain's root cause.
#[test]
fn google_error_reports_a_missing_credential_with_its_remedy() {
    let authentication = GoogleError::authentication(CredentialsError::new(
        true,
        "Fetching the token resulted in transient errors.",
        std::io::Error::other("failed to lookup address information"),
    ));

    let converted = crate::error::google_error(CONTEXT, GoogleError::exhausted(authentication));
    assert_eq!(converted.status, Status::Unauthenticated);
    assert_eq!(
        converted.message,
        format!(
            "{CONTEXT}: {} (underlying failure: failed to lookup address information)",
            adc_remedy()
        )
    );
}

/// Each authentication type that mints a token remotely gets its own remedy, naming the options
/// to check. A caller with a service-account key or refresh token is never told to configure
/// Application Default Credentials. `temporary_access_token` and `anonymous` never mint a token,
/// so they cannot reach here.
#[test]
fn a_failed_token_mint_is_answered_with_the_remedy_for_the_credential_that_failed() {
    use BigQueryAuthenticationType as Type;
    use CredentialLayer::{Authentication, Impersonation};

    let json = "the JSON credentials in bigquery.auth.credentials could not mint an access token; \
                check that the identity still exists, that its key or refresh token is not \
                disabled or revoked, and that the document is as Google issued it";
    for (layer, expected) in [
        (Authentication(Type::Default), adc_remedy()),
        (Authentication(Type::ApplicationDefault), adc_remedy()),
        (Authentication(Type::JsonCredentialFile), json.to_owned()),
        (Authentication(Type::JsonCredentialString), json.to_owned()),
        (
            Authentication(Type::UserAuthentication),
            "the refresh token in bigquery.auth.refresh_token could not be redeemed at the OAuth \
             token endpoint (bigquery.auth.access_token_endpoint, or Google's when unset); it may \
             be revoked or expired, or issued to a different bigquery.auth.client_id"
                .to_owned(),
        ),
        (
            Authentication(Type::ExternalAccount),
            "the external-account token exchange could not mint an access token; check \
             bigquery.auth.external_account.audience, \
             bigquery.auth.external_account.request_url and \
             bigquery.auth.external_account.subject_token_type, and that the workload identity \
             pool provider accepts the token bigquery.auth.external_account.request_url returns"
                .to_owned(),
        ),
        (
            Impersonation,
            "the impersonation exchange for bigquery.impersonate.target_principal could not mint \
             an access token; check that the source credentials have \
             roles/iam.serviceAccountTokenCreator on it and on each \
             bigquery.impersonate.delegates entry, and that bigquery.impersonate.lifetime is at \
             most 1h unless constraints/iam.allowServiceAccountCredentialLifetimeExtension allows \
             more"
                .to_owned(),
        ),
    ] {
        let failure = converted(attributed(layer));
        assert_eq!(failure.status, Status::Unauthenticated, "{layer:?}");
        assert_eq!(
            failure.message,
            format!("{CONTEXT}: {expected}"),
            "{layer:?}"
        );
    }
}

/// With impersonation there are two credentials, and only the innermost attribution names the one
/// that failed: a failing source credential reaches the impersonation wrapper already attributed
/// and keeps its own remedy.
#[test]
fn a_source_credential_failure_outranks_the_impersonation_layer_wrapped_around_it() {
    let source = attributed(CredentialLayer::Authentication(
        BigQueryAuthenticationType::UserAuthentication,
    ));
    let nested =
        CredentialFailure::new(CredentialLayer::Impersonation, Some(Box::new(source)), None);
    assert_message(
        &converted(nested),
        &["the refresh token in bigquery.auth.refresh_token"],
    );
}

/// Google's reauthentication demand survives redaction as a flag, and its remedy replaces the
/// layer's, because no credential option fixes it; the body carrying it still never appears.
#[test]
fn a_reauthentication_demand_is_answered_with_its_own_remedy() {
    let layer = CredentialLayer::Authentication(BigQueryAuthenticationType::UserAuthentication);
    let redacted = CredentialsError::new(
        false,
        "failed to refresh user access token, body=<{\"error\": \"invalid_grant\", \
         \"error_description\": \"reauth related error (invalid_rapt)\", \
         \"error_subtype\": \"invalid_rapt\", \"secret\": \"sentinel\"}>",
        std::io::Error::other("400 Bad Request"),
    );

    let failure = converted(attributed(layer).noting_reauthentication(&redacted));
    assert_eq!(failure.status, Status::Unauthenticated);
    assert_eq!(
        failure.message,
        format!("{CONTEXT}: {REAUTHENTICATION_REMEDY}")
    );

    let unrelated = std::io::Error::other("invalid_grant");
    assert!(
        !attributed(layer)
            .noting_reauthentication(&unrelated)
            .reauthentication
    );
}

/// The remedy replaces upstream's rendering, which doubles its own retryability clause onto
/// a sentence that already carries one.
#[test]
fn credential_failures_never_repeat_the_upstream_retry_wording() {
    let converted = crate::error::google_error(
        CONTEXT,
        GoogleError::authentication(CredentialsError::from_msg(
            true,
            "Fetching the token resulted in transient errors. Subsequent calls with this \
             credential might succeed.",
        )),
    );
    assert_eq!(converted.status, Status::Unauthenticated);
    assert_eq!(converted.message, format!("{CONTEXT}: {}", adc_remedy()));
}
