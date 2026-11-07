//! The BigQuery-specific option values.
//!
//! Each enum is the value set of one key in [`super::keys`] or of one standard ADBC option, and
//! converts into the `adbc_core` option value it is set with.

use adbc_core::options::OptionValue;

use super::string_enum;

string_enum! {
    /// BigQuery bulk-ingest implementation.
    ///
    /// Storage Write is the only supported implementation. The Go driver's `load` value is
    /// recognized by the option setters but returns `NOT_IMPLEMENTED`.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum BigQueryBulkIngestMethod {
        /// Append rows through the BigQuery Storage Write API. The default, and the only
        /// implemented method.
        #[default]
        StorageWrite => ("storage_write", []),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}

string_enum! {
    /// BigQuery destination-table creation behavior.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum BigQueryCreateDisposition {
        /// Create the destination table if it does not exist. The default.
        #[default]
        CreateIfNeeded => ("CREATE_IF_NEEDED", []),
        /// Require the destination table to exist already, failing the job if it does not.
        CreateNever => ("CREATE_NEVER", []),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}

string_enum! {
    /// BigQuery destination-table write behavior.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum BigQueryWriteDisposition {
        /// Append the rows to the destination table's existing data.
        WriteAppend => ("WRITE_APPEND", []),
        /// Replace the destination table's existing data with the rows written.
        WriteTruncate => ("WRITE_TRUNCATE", []),
        /// Fail the job unless the destination table is empty. The default.
        #[default]
        WriteEmpty => ("WRITE_EMPTY", []),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}

string_enum! {
    /// BigQuery query scheduling priorities.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum BigQueryQueryPriority {
        /// Queue the query until BigQuery has idle resources for it, outside the concurrent-rate
        /// limit interactive queries share.
        Batch => ("BATCH", []),
        /// Run the query as soon as possible. The default.
        #[default]
        Interactive => ("INTERACTIVE", []),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}

string_enum! {
    /// How the driver obtains the credentials it authenticates BigQuery requests with.
    ///
    /// The value of `bigquery.auth_type`. It selects the credential the driver builds and which
    /// other `bigquery.auth.*` options are accepted; an option the type would ignore is rejected,
    /// so a configured credential never falls back to the ambient identity.
    ///
    /// `Default` (`auth_bigquery`, what an unconfigured database uses) and `ApplicationDefault`
    /// (`app_default_credentials`) both resolve Application Default Credentials; the Go driver
    /// accepts both spellings.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum BigQueryAuthenticationType {
        /// Application Default Credentials, under the Go driver's `auth_bigquery` spelling. What
        /// an unconfigured database uses.
        #[default]
        Default => (
            "auth_bigquery",
            ["adbc.bigquery.sql.auth_type.auth_bigquery"]
        ),
        /// Application Default Credentials, under the `app_default_credentials` spelling.
        ApplicationDefault => (
            "app_default_credentials",
            ["adbc.bigquery.sql.auth_type.app_default_credentials"]
        ),
        /// A JSON credential document read from the file `bigquery.auth.credentials` names.
        JsonCredentialFile => (
            "json_credential_file",
            ["adbc.bigquery.sql.auth_type.json_credential_file"]
        ),
        /// A JSON credential document passed as the value of `bigquery.auth.credentials`.
        JsonCredentialString => (
            "json_credential_string",
            ["adbc.bigquery.sql.auth_type.json_credential_string"]
        ),
        /// An OAuth client and refresh token, from `bigquery.auth.client_id`,
        /// `bigquery.auth.client_secret` and `bigquery.auth.refresh_token`.
        UserAuthentication => (
            "user_authentication",
            ["adbc.bigquery.sql.auth_type.user_authentication"]
        ),
        /// Workload Identity Federation using an OAuth identity-provider token request.
        ExternalAccount => (
            "external_account",
            ["adbc.bigquery.sql.auth_type.external_account"]
        ),
        /// A caller-supplied short-lived OAuth access token, used as-is and never refreshed.
        /// Supports dbt-core 2.0 (Go driver) profiles.
        TemporaryAccessToken => (
            "temporary_access_token",
            ["adbc.bigquery.sql.auth_type.temporary_access_token"]
        ),
        /// No credentials: requests carry no token. For emulators and offline tests, and
        /// incompatible with a quota project and impersonation.
        Anonymous => (
            "anonymous",
            ["adbc.bigquery.sql.auth_type.anonymous"]
        ),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}

string_enum! {
    /// The `type` field of a Google Cloud JSON credential document.
    ///
    /// The four document shapes the JSON authentication types accept, spelled as Google writes
    /// them. The driver reads the field from the document to choose a credential builder.
    ///
    /// `bigquery.auth.credentials_type` is an optional assertion, never used to interpret a
    /// document: a mismatch with the detected type is rejected, so a swapped key fails as a
    /// configuration error rather than authenticating as an unintended identity.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BigQueryCredentialsType {
        /// A service-account key.
        ServiceAccount => ("service_account", []),
        /// OAuth user credentials, as `gcloud auth application-default login` writes them.
        AuthorizedUser => ("authorized_user", []),
        /// A source credential and the service account it impersonates.
        ImpersonatedServiceAccount => ("impersonated_service_account", []),
        /// A Workload or Workforce Identity Federation configuration.
        ExternalAccount => ("external_account", []),
    }
    into OptionValue => |value| OptionValue::String(value.as_str().to_owned());
}
