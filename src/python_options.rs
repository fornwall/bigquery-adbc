//! Checked-source generation for the Python option enums.
//!
//! This module is absent from normal driver builds. Its metadata refers to the compiled Rust
//! enums and standard ADBC option values directly, so the checked-in Python source never becomes
//! an independent source of wire strings.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement};

use crate::options::keys::OPTION_BIND_BY_NAME;
use crate::parameters::ParameterMode;
use crate::{
    BigQueryAuthenticationType, BigQueryBulkIngestMethod, BigQueryConnectionOption,
    BigQueryCreateDisposition, BigQueryCredentialsType, BigQueryDatabaseOption,
    BigQueryQueryPriority, BigQueryStatementOption, BigQueryWriteDisposition,
};

const GENERATED_PATH: &str = "python/src/bigquery_adbc/_options.py";

/// The width a rendered `#:` documentation line is wrapped to.
///
/// Below ruff's 120-column limit, which the longest rendered member line (108 columns)
/// approaches; prose reads better narrower.
const DOC_LINE_WIDTH: usize = 96;

/// One member of a generated Python enum.
///
/// The Python name is the whole exposure decision: an option with no name here is one only
/// dbt-core 2.0 sets. Such members are still carried so the uniqueness checks see their wire
/// strings, but nothing is rendered for them.
#[derive(Debug)]
struct PythonMember {
    name: Option<&'static str>,
    /// Rendered above the member as a Sphinx/IDE `#:` comment, the convention the upstream ADBC
    /// Python drivers use. Empty exactly when `name` is `None`, since nothing is rendered then.
    doc: &'static str,
    value: String,
}

impl PythonMember {
    fn new(name: Option<&'static str>, doc: &'static str, value: &str) -> Self {
        Self {
            name,
            doc,
            value: value.to_owned(),
        }
    }
}

#[derive(Debug)]
struct PythonEnum {
    name: &'static str,
    doc: &'static str,
    members: Vec<PythonMember>,
}

fn database_options() -> PythonEnum {
    use BigQueryDatabaseOption as O;

    let mut members = vec![PythonMember::new(
        Some("URI"),
        "The standard ADBC connection URI, `bigquery:///<project>?<key>=<value>`. Optional: \
         every setting a URI carries also has an option of its own.",
        OptionDatabase::Uri.as_ref(),
    )];
    members.extend(O::ALL.iter().copied().map(|option| {
        let (name, doc) = match option {
            O::AuthenticationType => (
                "AUTHENTICATION_TYPE",
                "How the driver authenticates: an `AuthenticationType` value. Unset resolves \
                 Google Cloud Application Default Credentials.",
            ),
            O::AuthenticationCredentials => (
                "AUTHENTICATION_CREDENTIALS",
                "The JSON credential file path or credential document selected by \
                 `AUTHENTICATION_TYPE`. Write-only and redacted from debug output; an error may \
                 quote a file path, never a document.",
            ),
            O::AuthenticationCredentialsType => (
                "AUTHENTICATION_CREDENTIALS_TYPE",
                "An optional `CredentialsType` value the JSON credential's `type` field must \
                 match; a mismatch is rejected. It does not change how the document is parsed.",
            ),
            O::AuthenticationClientId => (
                "AUTHENTICATION_CLIENT_ID",
                "The OAuth client ID required by `user_authentication`.",
            ),
            O::AuthenticationClientSecret => (
                "AUTHENTICATION_CLIENT_SECRET",
                "The OAuth client secret required by `user_authentication`. Write-only.",
            ),
            O::AuthenticationRefreshToken => (
                "AUTHENTICATION_REFRESH_TOKEN",
                "The OAuth refresh token required by `user_authentication`. Write-only.",
            ),
            O::AuthenticationAccessToken => (
                "AUTHENTICATION_ACCESS_TOKEN",
                "The OAuth access token `temporary_access_token` sends as-is. Write-only. The \
                 driver never refreshes it, so requests fail once it expires.",
            ),
            O::AuthenticationAccessTokenEndpoint => (
                "AUTHENTICATION_ACCESS_TOKEN_ENDPOINT",
                "The OAuth token endpoint `user_authentication` redeems the refresh token at; \
                 unset uses Google's default.",
            ),
            O::AuthenticationExternalAccountAudience => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_AUDIENCE",
                "The Workload Identity Federation provider resource required by \
                 `external_account`.",
            ),
            O::AuthenticationExternalAccountRequestUrl => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_REQUEST_URL",
                "The OAuth identity-provider URL required by `external_account`. Write-only, \
                 because its query can carry secrets. HTTPS is required unless \
                 `ALLOW_UNTRUSTED_ENDPOINT` is set.",
            ),
            O::AuthenticationExternalAccountRequestData => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_REQUEST_DATA",
                "The already form-encoded identity-provider POST body required by \
                 `external_account`, sent unchanged. Write-only.",
            ),
            O::AuthenticationExternalAccountImpersonationUrl => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_IMPERSONATION_URL",
                "An optional standard IAM Credentials `generateAccessToken` URL for \
                 `external_account`.",
            ),
            O::AuthenticationExternalAccountSubjectTokenType => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_SUBJECT_TOKEN_TYPE",
                "How Google's Security Token Service interprets the token the identity \
                 provider returns: `urn:ietf:params:oauth:token-type:jwt`, the default, or \
                 `urn:ietf:params:oauth:token-type:access_token` for an opaque token.",
            ),
            O::AuthenticationExternalAccountTokenUrl => (
                "AUTHENTICATION_EXTERNAL_ACCOUNT_TOKEN_URL",
                "Overrides `https://sts.googleapis.com/v1/token`, where the `external_account` \
                 subject token is exchanged. An override must still be HTTPS on \
                 `googleapis.com` unless `ALLOW_UNTRUSTED_ENDPOINT` is set.",
            ),
            O::AuthenticationQuotaProject => (
                "AUTHENTICATION_QUOTA_PROJECT",
                "The project API quota and billing are attributed to, applied to the shared \
                 credentials.",
            ),
            O::AuthenticationScopes => (
                "AUTHENTICATION_SCOPES",
                "Comma-separated base OAuth scopes. Without an impersonation layer, unset falls \
                 back to `IMPERSONATION_SCOPES`, then the credential default: `cloud-platform` \
                 for service accounts, or the original consent grant for user credentials. User \
                 credentials can request a subset of their consented scopes. \
                 External tables over Google Sheets or Drive also need \
                 `https://www.googleapis.com/auth/drive`, with prior consent for user credentials.",
            ),
            O::ProjectId => (
                "PROJECT_ID",
                "The project queries run and are billed in. Only a database sets it; on a \
                 connection it reads back the current catalog.",
            ),
            O::DatasetId => (
                "DATASET_ID",
                "The initial dataset unqualified table names resolve in. Only a database sets \
                 it; on a connection it reads back the current schema.",
            ),
            O::Endpoint => (
                "ENDPOINT",
                "Overrides `https://bigquery.googleapis.com`, the BigQuery REST endpoint. A \
                 bare `host[:port]` is HTTPS; an emulator needs an explicit `http://`. Set as an \
                 option, the endpoint is trusted to receive credentials on any host and scheme, \
                 unlike the host of a `bigquery://` URI.",
            ),
            O::StorageEndpoint => (
                "STORAGE_ENDPOINT",
                "Overrides the BigQuery Storage API endpoint, shared by the Read and Write \
                 halves of that API.",
            ),
            O::GcsEndpoint => (
                "GCS_ENDPOINT",
                "Overrides `https://storage.googleapis.com`, the Cloud Storage JSON API host \
                 the python-model upload is sent to.",
            ),
            O::DataprocEndpoint => (
                "DATAPROC_ENDPOINT",
                "Overrides `https://{region}-dataproc.googleapis.com`, the regional Dataproc \
                 host. An override replaces the whole host, region included.",
            ),
            O::VertexEndpoint => (
                "VERTEX_ENDPOINT",
                "Overrides `https://{region}-aiplatform.googleapis.com`, the regional Vertex \
                 AI host. An override replaces the whole host, region included.",
            ),
            O::AllowUntrustedEndpoint => (
                "ALLOW_UNTRUSTED_ENDPOINT",
                "Boolean string, default `false`: permits a `bigquery://` URI host, auxiliary \
                 service and credential-exchange endpoint overrides, and credential-document \
                 destinations that are not HTTPS Google API hosts. A BigQuery REST endpoint set \
                 as an option is already explicitly trusted. It cannot be set through a URI.",
            ),
            O::ImpersonationDelegates => (
                "IMPERSONATION_DELEGATES",
                "The comma-separated service-account delegation chain used to impersonate.",
            ),
            O::ImpersonationLifetime => (
                "IMPERSONATION_LIFETIME",
                "The impersonated token's lifetime, a positive Go duration up to 12 hours; \
                 unset is one hour. Over one hour also needs an organization policy, which IAM \
                 checks at the first query.",
            ),
            O::ImpersonationScopes => (
                "IMPERSONATION_SCOPES",
                "Comma-separated OAuth scopes for the impersonated token. Without an \
                 impersonation layer, also the base credential's fallback when \
                 `AUTHENTICATION_SCOPES` is unset. Discrete external-account credentials use \
                 that fallback even with an impersonation layer. Scopes alone do not turn \
                 impersonation on; temporary access tokens remain unchanged.",
            ),
            O::ImpersonationTargetPrincipal => (
                "IMPERSONATION_TARGET_PRINCIPAL",
                "The service account to impersonate; required when delegates or a lifetime \
                 are configured. Scopes alone do not enable impersonation.",
            ),
            O::Location => (
                "LOCATION",
                "The default location applied to every query and transaction path.",
            ),
            O::UserAgent => (
                "USER_AGENT",
                "Text such as `my-app/1.0` appended to the `bigquery-adbc/<version>` user agent \
                 of REST and Storage Read requests, which Cloud Audit Logs record.",
            ),
        };
        PythonMember::new(Some(name), doc, option.as_str())
    }));
    PythonEnum {
        name: "DatabaseOptions",
        doc: "Options accepted while initializing an ADBC database.",
        members,
    }
}

fn connection_options() -> PythonEnum {
    use BigQueryConnectionOption as O;

    let mut members = [
        (
            "AUTOCOMMIT",
            "Standard ADBC boolean string. With `false`, the first query that needs a session \
             opens one for the transaction, kept until autocommit is re-enabled or the \
             connection is released.",
            OptionConnection::AutoCommit,
        ),
        (
            "CURRENT_CATALOG",
            "Standard ADBC current catalog: the project unqualified names resolve in. A \
             connection also reports it under `DatabaseOptions.PROJECT_ID`.",
            OptionConnection::CurrentCatalog,
        ),
        (
            "CURRENT_DB_SCHEMA",
            "Standard ADBC current schema: the dataset unqualified names resolve in. A \
             connection also reports it under `DatabaseOptions.DATASET_ID`.",
            OptionConnection::CurrentSchema,
        ),
        (
            "READ_ONLY",
            "Standard ADBC boolean string. Only `false` is supported; use IAM permissions \
             to restrict writes.",
            OptionConnection::ReadOnly,
        ),
        (
            "ISOLATION_LEVEL",
            "Standard ADBC transaction isolation level. Accepts \
             `adbc.connection.transaction.isolation.default` or \
             `adbc.connection.transaction.isolation.snapshot`; always reports the latter.",
            OptionConnection::IsolationLevel,
        ),
    ]
    .into_iter()
    .map(|(name, doc, option)| PythonMember::new(Some(name), doc, option.as_ref()))
    .collect::<Vec<_>>();
    members.extend(O::ALL.iter().copied().map(|option| {
        let (name, doc) = match option {
            O::GetObjectsSkipTableMetadata => (
                "GET_OBJECTS_SKIP_TABLE_METADATA",
                "Boolean string, default `true`: list tables without fetching their constraints. \
                 Setting `false` includes constraints at tables depth. Column/all depth is unaffected.",
            ),
            O::GetObjectsDisableWildcards => (
                "GET_OBJECTS_DISABLE_WILDCARDS",
                "Boolean string, default `false`: match GetObjects filters as exact, case-sensitive \
                 names, treating `%` and `_` literally. GetStatistics is unaffected.",
            ),
            O::BulkIngestMethod => (
                "BULK_INGEST_METHOD",
                "The bulk-ingest implementation: a `BulkIngestMethod` value.",
            ),
            O::PrefetchConcurrency => (
                "QUERY_PREFETCH_CONCURRENCY",
                "The maximum number of REST row batches one reader converts concurrently: a \
                 positive integer, default `10`, also capped by available parallelism. Storage \
                 Read ignores it.",
            ),
            O::UnorderedRead => (
                "QUERY_UNORDERED_READ",
                "Boolean string, default `false`: lets the Storage Read path read several \
                 streams in parallel and return batches as they arrive, ignoring `ORDER BY`. \
                 Rows are never lost or duplicated.",
            ),
            O::UseStorageApiDisabledClient => (
                "QUERY_USE_STORAGE_API_DISABLED_CLIENT",
                "Boolean string, default `false`: forces query results through REST when `true`. \
                 Reports the configured value even after automatic fallback. Setting `false` \
                 on a connection also clears the billing project's cached Storage Read refusal.",
            ),
        };
        PythonMember::new(Some(name), doc, option.as_str())
    }));
    PythonEnum {
        name: "ConnectionOptions",
        doc: "Options accepted by an ADBC connection.",
        members,
    }
}

fn statement_options() -> PythonEnum {
    use BigQueryStatementOption as O;

    let mut members = vec![PythonMember::new(
        Some("BIND_BY_NAME"),
        "Standard ADBC boolean string, and the portable spelling of \
         `QUERY_PARAMETER_MODE`: `true` binds parameters by the bound stream's field names, \
         `false` by position. The two options read each other back.",
        OPTION_BIND_BY_NAME,
    )];
    members.extend(O::ALL.iter().copied().map(|option| {
        let (name, doc) = match option {
            O::BulkIngestMethod => (
                Some("BULK_INGEST_METHOD"),
                "The bulk-ingest implementation: a `BulkIngestMethod` value.",
            ),
            O::CreateDisposition => (
                Some("QUERY_CREATE_DISPOSITION"),
                "Whether the query may create its destination table: a `CreateDisposition` \
                 value.",
            ),
            O::CreateSession => (
                Some("QUERY_CREATE_SESSION"),
                "Boolean string: asks BigQuery to create a session, independently of the \
                 sessions ADBC transactions use.",
            ),
            O::DefaultDatasetId => (
                Some("QUERY_DEFAULT_DATASET_ID"),
                "The dataset unqualified names resolve in for this statement; empty restores \
                 the connection's current schema.",
            ),
            O::DefaultProjectId => (
                Some("QUERY_DEFAULT_PROJECT_ID"),
                "The project of this statement's default dataset; empty restores the \
                 connection's current catalog.",
            ),
            O::DestinationTable => (
                Some("QUERY_DESTINATION_TABLE"),
                "Where the query writes its result, as `[[project.]dataset.]table` completed \
                 from the effective defaults; empty clears it.",
            ),
            O::DisableQueryCache => (
                Some("QUERY_DISABLE_QUERY_CACHE"),
                "Boolean string: when true the query does not use BigQuery's query cache.",
            ),
            O::DryRun => (
                Some("QUERY_DRY_RUN"),
                "Boolean string: validates the statement without running it, returning an \
                 empty stream with the inferred schema. `execute_partitions` rejects it with \
                 `INVALID_STATE`.",
            ),
            O::JobTimeout => (
                Some("QUERY_JOB_TIMEOUT"),
                "The job timeout in milliseconds; zero clears the override and a negative \
                 value is rejected.",
            ),
            O::Reservation => (
                Some("QUERY_RESERVATION"),
                "Reservation resource name `projects/{project}/locations/{location}/reservations/{reservation}`. \
                 Empty clears the override; `none` requests on-demand billing when permitted by \
                 the project's or organization's reservation override policy. BigQuery validates the value.",
            ),
            O::Labels => (
                Some("QUERY_LABELS"),
                "A JSON object of string labels for every query job, such as \
                 `{\"team\":\"data\"}`; empty or `{}` clears them, and `key=value` or \
                 comma-separated strings are rejected. Reads back as canonical JSON sorted by \
                 key.",
            ),
            O::LinkFailedJob => (
                Some("QUERY_LINK_FAILED_JOB"),
                "Boolean string accepted for Go-driver compatibility, but a no-op: a failed \
                 job's error message ends with its console link either way.",
            ),
            O::MaximumBillingTier => (
                Some("QUERY_MAXIMUM_BILLING_TIER"),
                "Recognized but returns `NOT_IMPLEMENTED`, because Google deprecated the \
                 field. Cap cost with `QUERY_MAXIMUM_BYTES_BILLED` instead.",
            ),
            O::MaximumBytesBilled => (
                Some("QUERY_MAXIMUM_BYTES_BILLED"),
                "The maximum bytes the query may bill; zero clears the override and a negative \
                 value is rejected.",
            ),
            O::ParameterMode => (
                Some("QUERY_PARAMETER_MODE"),
                "How bound parameters are sent: a `ParameterMode` value, `positional` by \
                 default.",
            ),
            O::PrefetchConcurrency => (
                Some("QUERY_PREFETCH_CONCURRENCY"),
                "Overrides `ConnectionOptions.QUERY_PREFETCH_CONCURRENCY` for one statement.",
            ),
            O::Priority => (
                Some("QUERY_PRIORITY"),
                "The query's scheduling priority: a `QueryPriority` value.",
            ),
            O::UnorderedRead => (
                Some("QUERY_UNORDERED_READ"),
                "Overrides `ConnectionOptions.QUERY_UNORDERED_READ` for one statement.",
            ),
            O::UseStorageApiDisabledClient => (
                Some("QUERY_USE_STORAGE_API_DISABLED_CLIENT"),
                "Overrides `ConnectionOptions.QUERY_USE_STORAGE_API_DISABLED_CLIENT` for one statement.",
            ),
            O::WriteDisposition => (
                Some("QUERY_WRITE_DISPOSITION"),
                "How the query writes to an existing destination table: a `WriteDisposition` \
                 value.",
            ),
            O::AuthorizeViewToDatasets
            | O::CopyTableDestination
            | O::CopyTableSource
            | O::CopyTableWriteDisposition
            | O::CreateBatchBatchId
            | O::CreateBatchBatchYml
            | O::CreateBatchParent
            | O::CsvDelimiter
            | O::CsvFilepath
            | O::CsvSchema
            | O::DataprocComputeRegion
            | O::DataprocPoolingTimeout
            | O::DataprocProject
            | O::DataprocSubmitJobClusterName
            | O::DataprocSubmitJobGcsPath
            | O::GetDatasetLocation
            | O::NotebookExecuteJobGscBucket
            | O::NotebookExecuteJobGscPath
            | O::NotebookExecuteJobModelFileName
            | O::NotebookExecuteJobModelName
            | O::NotebookExecuteJobParent
            | O::NotebookExecuteJobProject
            | O::NotebookExecuteJobRegion
            | O::NotebookExecuteJobServiceAccount
            | O::NotebookExecuteJobTemplateId
            | O::UpdateColumnsDescription
            | O::UpdateColumnsPolicyTags
            | O::UpdateDescription
            | O::WriteGcsBucket
            | O::WriteGcsContent
            | O::WriteGcsObjectName => (None, ""),
        };
        PythonMember::new(name, doc, option.as_str())
    }));
    members.extend(
        [
            (
                "INGEST_MODE",
                "Standard ADBC bulk-ingest mode: whether the target table is created, appended \
                 to, replaced, or created and appended to.",
                OptionStatement::IngestMode,
            ),
            (
                "INGEST_TARGET_TABLE",
                "Standard ADBC bulk-ingest target table name.",
                OptionStatement::TargetTable,
            ),
            (
                "INGEST_TARGET_CATALOG",
                "Standard ADBC bulk-ingest target catalog: the BigQuery project.",
                OptionStatement::TargetCatalog,
            ),
            (
                "INGEST_TARGET_DB_SCHEMA",
                "Standard ADBC bulk-ingest target schema: the BigQuery dataset.",
                OptionStatement::TargetDbSchema,
            ),
            (
                "INGEST_TEMPORARY",
                "Standard ADBC boolean string. Only `false` is supported: bulk ingest writes \
                 into a named table, never a temporary one.",
                OptionStatement::Temporary,
            ),
            (
                "INCREMENTAL",
                "Standard ADBC boolean string. Only `false` is supported: execution returns \
                 all partitions together.",
                OptionStatement::Incremental,
            ),
            (
                "PROGRESS",
                "Read-only double: processed bytes reported at query response boundaries. \
                 Read with `get_option_float(member.value)`.",
                OptionStatement::Progress,
            ),
            (
                "MAX_PROGRESS",
                "Read-only double: final processed bytes, or `0.0` while unknown. Read with \
                 `get_option_float(member.value)`.",
                OptionStatement::MaxProgress,
            ),
        ]
        .into_iter()
        .map(|(name, doc, option)| PythonMember::new(Some(name), doc, option.as_ref())),
    );
    PythonEnum {
        name: "StatementOptions",
        doc: "Options accepted by an ADBC statement.",
        members,
    }
}

macro_rules! value_enum {
    ($name:literal, $doc:literal, $type:ty, {$($variant:path => ($member:literal, $member_doc:literal $(,)?)),+ $(,)?}) => {{
        let members = <$type>::ALL
            .iter()
            .copied()
            .map(|value| {
                let (name, doc) = match value {
                    $($variant => ($member, $member_doc)),+
                };
                PythonMember::new(Some(name), doc, value.as_str())
            })
            .collect();
        PythonEnum {
            name: $name,
            doc: $doc,
            members,
        }
    }};
}

fn python_enums() -> Vec<PythonEnum> {
    vec![
        database_options(),
        connection_options(),
        statement_options(),
        value_enum!(
            "ParameterMode",
            "BigQuery query parameter binding modes.",
            ParameterMode,
            {
                ParameterMode::Positional => (
                    "POSITIONAL",
                    "Send bound parameters in field order, for SQL that spells its \
                     placeholders `?`. The default.",
                ),
                ParameterMode::Named => (
                    "NAMED",
                    "Send each bound parameter under the name of the field it came from, for \
                     SQL that spells its placeholders `@name`.",
                ),
            }
        ),
        value_enum!(
            "BulkIngestMethod",
            "Supported BigQuery bulk-ingest implementations.",
            BigQueryBulkIngestMethod,
            {
                BigQueryBulkIngestMethod::StorageWrite => (
                    "STORAGE_WRITE",
                    "Append rows through the BigQuery Storage Write API. The default, and the \
                     only implemented method.",
                ),
            }
        ),
        value_enum!(
            "AuthenticationType",
            "Supported Google Cloud authentication modes.",
            BigQueryAuthenticationType,
            {
                BigQueryAuthenticationType::Default => (
                    "DEFAULT",
                    "Google Cloud Application Default Credentials, under the Go driver's \
                     `auth_bigquery` spelling. What an unconfigured database uses.",
                ),
                BigQueryAuthenticationType::ApplicationDefault => (
                    "APPLICATION_DEFAULT",
                    "Google Cloud Application Default Credentials, under the \
                     `app_default_credentials` spelling. The same code path as `DEFAULT`.",
                ),
                BigQueryAuthenticationType::JsonCredentialFile => (
                    "JSON_CREDENTIAL_FILE",
                    "A Google Cloud JSON credential document read from the file path in \
                     `DatabaseOptions.AUTHENTICATION_CREDENTIALS`.",
                ),
                BigQueryAuthenticationType::JsonCredentialString => (
                    "JSON_CREDENTIAL_STRING",
                    "A Google Cloud JSON credential document passed inline in \
                     `DatabaseOptions.AUTHENTICATION_CREDENTIALS`.",
                ),
                BigQueryAuthenticationType::ExternalAccount => (
                    "EXTERNAL_ACCOUNT",
                    "Workload Identity Federation using an OAuth identity-provider token \
                     request, configured with the `AUTHENTICATION_EXTERNAL_ACCOUNT_*` options.",
                ),
                BigQueryAuthenticationType::UserAuthentication => (
                    "USER_AUTHENTICATION",
                    "An OAuth client ID, client secret and refresh token, which the driver \
                     redeems for access tokens.",
                ),
                BigQueryAuthenticationType::TemporaryAccessToken => (
                    "TEMPORARY_ACCESS_TOKEN",
                    "A caller-supplied short-lived OAuth access token, used as-is. The driver \
                     never refreshes it, so requests fail once it expires.",
                ),
                BigQueryAuthenticationType::Anonymous => (
                    "ANONYMOUS",
                    "No credentials at all: requests carry no token. For emulators and for \
                     offline tests, and incompatible with quota project and impersonation.",
                ),
            }
        ),
        value_enum!(
            "CredentialsType",
            "Supported Google Cloud JSON credential document types.",
            BigQueryCredentialsType,
            {
                BigQueryCredentialsType::ServiceAccount => (
                    "SERVICE_ACCOUNT",
                    "A service-account key document.",
                ),
                BigQueryCredentialsType::AuthorizedUser => (
                    "AUTHORIZED_USER",
                    "A user credential document, as `gcloud auth application-default login` \
                     writes.",
                ),
                BigQueryCredentialsType::ImpersonatedServiceAccount => (
                    "IMPERSONATED_SERVICE_ACCOUNT",
                    "A service-account impersonation document, naming a source credential and \
                     a target principal.",
                ),
                BigQueryCredentialsType::ExternalAccount => (
                    "EXTERNAL_ACCOUNT",
                    "A Workload Identity Federation external-account document.",
                ),
            }
        ),
        value_enum!(
            "QueryPriority",
            "BigQuery query scheduling priorities.",
            BigQueryQueryPriority,
            {
                BigQueryQueryPriority::Batch => (
                    "BATCH",
                    "Queue the query until BigQuery has idle resources for it, outside the \
                     concurrent-rate limit interactive queries share.",
                ),
                BigQueryQueryPriority::Interactive => (
                    "INTERACTIVE",
                    "Run the query as soon as possible. The default.",
                ),
            }
        ),
        value_enum!(
            "CreateDisposition",
            "BigQuery destination-table creation behavior.",
            BigQueryCreateDisposition,
            {
                BigQueryCreateDisposition::CreateIfNeeded => (
                    "IF_NEEDED",
                    "Create the destination table if it does not exist. The default.",
                ),
                BigQueryCreateDisposition::CreateNever => (
                    "NEVER",
                    "Require the destination table to exist already, failing the job if it \
                     does not.",
                ),
            }
        ),
        value_enum!(
            "WriteDisposition",
            "BigQuery destination-table write behavior.",
            BigQueryWriteDisposition,
            {
                BigQueryWriteDisposition::WriteAppend => (
                    "APPEND",
                    "Append the rows to the destination table's existing data.",
                ),
                BigQueryWriteDisposition::WriteTruncate => (
                    "TRUNCATE",
                    "Replace the destination table's existing data with the rows written.",
                ),
                BigQueryWriteDisposition::WriteEmpty => (
                    "EMPTY",
                    "Fail the job unless the destination table is empty. The default.",
                ),
            }
        ),
    ]
}

/// Render a Python string literal, asserting `{:?}` is a faithful rendering of it.
///
/// Rust's `Debug` escaping is not Python's (`char::escape_debug` emits `\u{a0}`-style escapes),
/// but the two agree on the ASCII identifier-like strings every option name and value is. The
/// assertion keeps a future non-ASCII value from producing an invalid module.
fn python_literal(value: &str) -> String {
    assert!(
        !value.is_empty()
            && value
                .chars()
                .all(|character| character.is_ascii_alphanumeric()
                    || character == '.'
                    || character == '_'),
        "{value:?} cannot be rendered as a Python string literal with Rust's Debug escaping"
    );
    format!("{value:?}")
}

/// Wrap one member's documentation into `#:` comment lines at a fixed indent.
///
/// A word longer than the budget goes on a line of its own rather than being split: the long
/// tokens here are URLs and URNs, and breaking one would make it uncopyable.
fn doc_comment_lines(doc: &str, indent: &str) -> Vec<String> {
    let prefix = format!("{indent}#: ");
    let budget = DOC_LINE_WIDTH.saturating_sub(prefix.len());
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in doc.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.chars().count() + 1 + word.chars().count() <= budget {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(format!("{prefix}{current}"));
            current = word.to_owned();
        }
    }
    if !current.is_empty() {
        lines.push(format!("{prefix}{current}"));
    }
    lines
}

/// Render the complete checked-in Python option module deterministically.
pub fn render() -> String {
    let enumerations = python_enums();
    let mut output = String::from(
        r#""""Typed option names and values supported by the BigQuery driver.

Generated from compiled Rust option metadata; run
``cargo run --locked --features python-option-codegen --bin generate-python-options``
after changing an option declaration. Do not edit this file manually.

Every enum is a string: dictionary keys, option values, ``str()``, and formatting
all use its wire value on every supported Python version.
"""

from __future__ import annotations

import enum

"#,
    );

    for enumeration in &enumerations {
        writeln!(
            output,
            "\nclass {}(enum.StrEnum):\n    \"\"\"{}\"\"\"",
            enumeration.name, enumeration.doc
        )
        .expect("writing to a String is infallible");
        for member in &enumeration.members {
            let Some(name) = member.name else {
                continue;
            };
            assert!(
                !member.doc.is_empty(),
                "{}.{name} is exposed to Python without documentation",
                enumeration.name
            );
            output.push('\n');
            for line in doc_comment_lines(member.doc, "    ") {
                writeln!(output, "{line}").expect("writing to a String is infallible");
            }
            writeln!(output, "    {name} = {}", python_literal(&member.value))
                .expect("writing to a String is infallible");
        }
        output.push('\n');
    }

    // Derived from the rendered enums rather than restated, so a new enum cannot be exported
    // from the module and missing from `__all__`.
    let mut exported = enumerations
        .iter()
        .map(|enumeration| enumeration.name)
        .collect::<Vec<_>>();
    exported.sort_unstable();
    output.push_str("\n__all__ = [\n");
    for name in exported {
        writeln!(output, "    {},", python_literal(name))
            .expect("writing to a String is infallible");
    }
    output.push_str("]\n");
    output
}

/// Return the repository path of the checked-in generated module.
pub fn generated_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(GENERATED_PATH)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{DOC_LINE_WIDTH, generated_path, python_enums, render};

    #[test]
    fn checked_python_options_match_compiled_rust_metadata() {
        let path = generated_path();
        let checked = fs::read_to_string(&path).unwrap();
        assert_eq!(
            checked,
            render(),
            "{} is stale; run `cargo run --locked --features python-option-codegen --bin generate-python-options`",
            path.display()
        );
    }

    /// Wrapping leaves only a single over-long word able to overflow, which ruff
    /// (`line-length = 120`) would otherwise catch only in a separate job.
    #[test]
    fn rendered_documentation_comments_stay_within_the_line_budget() {
        for line in render().lines() {
            if line.trim_start().starts_with("#:") {
                assert!(
                    line.chars().count() <= DOC_LINE_WIDTH,
                    "documentation line exceeds {DOC_LINE_WIDTH} columns: {line}"
                );
            }
        }
    }

    /// Uniqueness holds over every member, hidden ones included: two variants that share a wire
    /// string would be one option the driver cannot tell apart, whether or not Python names it.
    /// Names are checked only where they exist, since a hidden member has none.
    #[test]
    fn python_metadata_has_unique_public_names_and_values_per_enum() {
        for enumeration in python_enums() {
            let mut names = std::collections::HashSet::new();
            let mut values = std::collections::HashSet::new();
            for member in &enumeration.members {
                if let Some(name) = member.name {
                    assert!(names.insert(name), "{}", enumeration.name);
                }
                assert!(values.insert(&member.value), "{}", enumeration.name);
            }
        }
    }
}
