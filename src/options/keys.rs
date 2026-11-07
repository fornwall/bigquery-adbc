//! The BigQuery-specific option keys.
//!
//! One variant per option, with its canonical spelling (what the driver reads back), the legacy
//! aliases the Go driver and its dbt-labs fork send, and the ADBC value kind
//! ([`OptionKind`](super::OptionKind)) it is set with. Enumerated values live in
//! [`super::values`].

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement};

use super::string_enum;

string_enum! {
    /// BigQuery-specific database option keys.
    ///
    /// Converts into [`adbc_core::options::OptionDatabase`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BigQueryDatabaseOption {
        /// How the driver authenticates: a `BigQueryAuthenticationType` value. Unset resolves
        /// Application Default Credentials.
        AuthenticationType => (
            "bigquery.auth_type",
            ["adbc.bigquery.sql.auth_type"],
            String
        ),
        /// The JSON credential file path or credential document selected by `bigquery.auth_type`.
        ///
        /// Write-only: the driver keeps it out of debug output and errors, file paths included.
        AuthenticationCredentials => (
            "bigquery.auth.credentials",
            ["adbc.bigquery.sql.auth_credentials"],
            String
        ),
        /// An optional `BigQueryCredentialsType` value the JSON credential's `type` field must
        /// match; a mismatch is rejected. It does not change how the document is parsed.
        AuthenticationCredentialsType => ("bigquery.auth.credentials_type", [], String),
        /// The OAuth client ID required by `user_authentication`.
        AuthenticationClientId => (
            "bigquery.auth.client_id",
            ["adbc.bigquery.sql.auth.client_id"],
            String
        ),
        /// The OAuth client secret required by `user_authentication`. Write-only.
        AuthenticationClientSecret => (
            "bigquery.auth.client_secret",
            ["adbc.bigquery.sql.auth.client_secret"],
            String
        ),
        /// The OAuth refresh token required by `user_authentication`. Write-only.
        AuthenticationRefreshToken => (
            "bigquery.auth.refresh_token",
            ["adbc.bigquery.sql.auth.refresh_token"],
            String
        ),
        /// The OAuth access token `temporary_access_token` sends as-is. Write-only.
        ///
        /// The driver never refreshes it, so requests fail once it expires.
        AuthenticationAccessToken => (
            "bigquery.auth.access_token",
            ["adbc.bigquery.sql.auth.access_token"],
            String
        ),
        /// The OAuth token endpoint `user_authentication` redeems the refresh token at; unset
        /// uses Google's default.
        AuthenticationAccessTokenEndpoint => (
            "bigquery.auth.access_token_endpoint",
            ["adbc.bigquery.sql.auth.access_token_endpoint"],
            String
        ),
        /// Workload Identity Federation provider resource required by `external_account`.
        AuthenticationExternalAccountAudience => (
            "bigquery.auth.external_account.audience",
            ["adbc.bigquery.sql.auth.external_account.audience"],
            String
        ),
        /// OAuth identity-provider URL required by `external_account`. Write-only because its
        /// query can contain secrets. HTTPS is required unless untrusted endpoints are enabled.
        AuthenticationExternalAccountRequestUrl => (
            "bigquery.auth.external_account.request_url",
            ["adbc.bigquery.sql.auth.external_account.request_url"],
            String
        ),
        /// Already form-encoded identity-provider POST body required by `external_account`.
        /// Sent unchanged. Write-only.
        AuthenticationExternalAccountRequestData => (
            "bigquery.auth.external_account.request_data",
            ["adbc.bigquery.sql.auth.external_account.request_data"],
            String
        ),
        /// Optional standard IAM Credentials `generateAccessToken` URL for `external_account`.
        AuthenticationExternalAccountImpersonationUrl => (
            "bigquery.auth.external_account.impersonation_url",
            ["adbc.bigquery.sql.auth.external_account.impersonation_url"],
            String
        ),
        /// How Google's Security Token Service interprets the token the identity provider returns,
        /// for `external_account`.
        ///
        /// `urn:ietf:params:oauth:token-type:jwt`, the default, or
        /// `urn:ietf:params:oauth:token-type:access_token` for an identity provider that returns
        /// an opaque token rather than a JWT.
        AuthenticationExternalAccountSubjectTokenType => (
            "bigquery.auth.external_account.subject_token_type",
            [],
            String
        ),
        /// Overrides `https://sts.googleapis.com/v1/token`, the Security Token Service endpoint
        /// the `external_account` subject token is exchanged at.
        ///
        /// Unset uses Google's default with the credential's universe domain substituted in. An
        /// override must still be HTTPS on `googleapis.com` unless
        /// `bigquery.allow_untrusted_endpoint` is set, because the subject token is sent there.
        AuthenticationExternalAccountTokenUrl => (
            "bigquery.auth.external_account.token_url",
            [],
            String
        ),
        /// The project API quota and billing are attributed to, applied to the shared
        /// credentials.
        AuthenticationQuotaProject => (
            "bigquery.auth.quota_project",
            ["adbc.bigquery.sql.auth.quota_project"],
            String
        ),
        /// Comma-separated OAuth scopes the configured credentials are minted with. When unset
        /// and no impersonation layer is configured, `bigquery.impersonate.scopes` supplies the
        /// base scopes; otherwise the credential's default applies: `cloud-platform` for service
        /// accounts, or the original consent grant for user credentials, which can request a
        /// subset of their consented scopes.
        ///
        /// Shapes the credential `bigquery.auth_type` builds, including Application Default
        /// Credentials and every JSON credential type. With an impersonation layer, this configures
        /// its source token independently of the impersonated token's scopes.
        ///
        /// A BigQuery external table over Google Sheets or Drive needs
        /// `https://www.googleapis.com/auth/drive`, which `cloud-platform` does not cover, with
        /// prior consent for user credentials.
        AuthenticationScopes => ("bigquery.auth.scopes", [], String),
        /// The project queries run and are billed in.
        ///
        /// A connection also *reads* this key, as another name for the standard current-catalog
        /// option; only a database sets it.
        ProjectId => (
            "bigquery.project_id",
            ["adbc.bigquery.sql.project_id"],
            String
        ),
        /// The initial dataset unqualified table names resolve in.
        ///
        /// A connection also *reads* this key, as another name for the standard current-schema
        /// option; only a database sets it.
        DatasetId => (
            "bigquery.dataset_id",
            ["adbc.bigquery.sql.dataset_id"],
            String
        ),
        /// The BigQuery REST endpoint, overriding `https://bigquery.googleapis.com`.
        ///
        /// A bare `host[:port]` is HTTPS; an emulator needs an explicit `http://`. Set as an option,
        /// the endpoint is trusted to receive the credentials on any host and scheme, unlike the
        /// host of a `bigquery://` URI. The `api_endpoint` alias is the retired `apache/arrow-adbc`
        /// spelling, which dbt-core 2.0 still sends.
        Endpoint => (
            "bigquery.endpoint",
            [
                "adbc.bigquery.sql.endpoint",
                "adbc.bigquery.sql.api_endpoint"
            ],
            String
        ),
        /// Overrides the BigQuery Storage API endpoint, shared by the Read and Write halves of
        /// that API.
        ///
        /// The `storage_read_api_endpoint` alias names only the Read half, but the dbt-labs fork
        /// of the Go driver, which spells it that way, overrides both, so this driver does too.
        StorageEndpoint => (
            "bigquery.storage_endpoint",
            [
                "adbc.bigquery.sql.storage_endpoint",
                "adbc.bigquery.sql.storage_read_api_endpoint"
            ],
            String
        ),
        /// Overrides `https://storage.googleapis.com`, the Cloud Storage JSON API host the
        /// `adbc.bigquery.write_gcs.*` upload is sent to.
        GcsEndpoint => ("bigquery.gcs_endpoint", [], String),
        /// Overrides `https://{region}-dataproc.googleapis.com`, the regional Dataproc host the
        /// `adbc.bigquery.create_batch.*` and `adbc.bigquery.dataproc.submit_job.*` operations
        /// are sent to. An override replaces the whole host, region included.
        DataprocEndpoint => ("bigquery.dataproc_endpoint", [], String),
        /// Overrides `https://{region}-aiplatform.googleapis.com`, the regional Vertex AI host
        /// the `adbc.bigquery.notebook_execute_job.*` operation is sent to. An override replaces
        /// the whole host, region included.
        VertexEndpoint => ("bigquery.vertex_endpoint", [], String),
        /// Boolean string, default `false`: permits a `bigquery://` URI host, auxiliary service and
        /// credential-exchange endpoint overrides, and credential-document destinations that are
        /// not HTTPS Google API hosts.
        ///
        /// Not a URI query parameter. A BigQuery REST endpoint set as an option is already
        /// explicitly trusted.
        AllowUntrustedEndpoint => ("bigquery.allow_untrusted_endpoint", [], String),
        /// The comma-separated service-account delegation chain used to impersonate.
        ImpersonationDelegates => (
            "bigquery.impersonate.delegates",
            ["adbc.bigquery.sql.impersonate.delegates"],
            String
        ),
        /// The impersonated token's lifetime, a positive Go duration up to 12 hours; unset is one
        /// hour.
        ///
        /// Over one hour, the IAM Credentials API also requires the organization policy
        /// `constraints/iam.allowServiceAccountCredentialLifetimeExtension` for the target service
        /// account. That cannot be checked offline, so IAM rejects a disallowed lifetime at the
        /// first query.
        ImpersonationLifetime => (
            "bigquery.impersonate.lifetime",
            ["adbc.bigquery.sql.impersonate.lifetime"],
            String
        ),
        /// Comma-separated OAuth scopes for the impersonated token. Without an impersonation
        /// layer, also the base credential's fallback when `bigquery.auth.scopes` is unset.
        ///
        /// Discrete external-account credentials use this fallback even with an impersonation
        /// layer. Scopes alone do not turn impersonation on, because dbt-core 2.0 sets them on
        /// every connection. A temporary access token is sent unchanged.
        ImpersonationScopes => (
            "bigquery.impersonate.scopes",
            ["adbc.bigquery.sql.impersonate.scopes"],
            String
        ),
        /// The service account to impersonate; required when delegates or a lifetime are
        /// configured. Scopes alone do not enable impersonation.
        ImpersonationTargetPrincipal => (
            "bigquery.impersonate.target_principal",
            ["adbc.bigquery.sql.impersonate.target_principal"],
            String
        ),
        /// The default location applied to every query and transaction path.
        Location => (
            "bigquery.location",
            ["adbc.bigquery.sql.location"],
            String
        ),
        /// Text such as `my-app/1.0` appended to the `bigquery-adbc/<version>` user agent of REST
        /// and Storage Read requests, which Cloud Audit Logs record.
        UserAgent => ("bigquery.user_agent", [], String),
    }
    into OptionDatabase => |value| OptionDatabase::Other(value.as_str().to_owned());
}

string_enum! {
    /// BigQuery-specific connection option keys.
    ///
    /// Query and ingest options are also [`BigQueryStatementOption`] values, where they
    /// override the connection value for one statement. Metadata options are connection-only.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BigQueryConnectionOption {
        /// The bulk-ingest implementation: a `BigQueryBulkIngestMethod` value.
        BulkIngestMethod => ("bigquery.bulk_ingest.method", [], String),
        /// Boolean string, default `true`: list tables without fetching their constraints.
        /// Setting `false` includes constraints at tables depth. Column/all depth is unaffected.
        GetObjectsSkipTableMetadata => ("bigquery.get_objects.skip_table_metadata", [], String),
        /// Boolean string, default `false`: match GetObjects filters as exact, case-sensitive
        /// names, treating `%` and `_` literally. GetStatistics is unaffected.
        GetObjectsDisableWildcards => ("bigquery.get_objects.disable_wildcards", [], String),
        /// The maximum number of REST row batches one reader converts concurrently, a positive
        /// integer defaulting to `10`.
        ///
        /// Conversion is also bounded by the process's available parallelism. The Storage Read
        /// path ignores this option.
        PrefetchConcurrency => (
            "bigquery.query.prefetch_concurrency",
            ["adbc.bigquery.sql.query.prefetch_concurrency"],
            Int
        ),
        /// Boolean string, default `false`: lets the Storage Read path read several streams in
        /// parallel and return batches in whatever order they arrive.
        ///
        /// Rows are never lost or duplicated, only reordered, so set it only when the result's
        /// order, including an `ORDER BY`, does not matter.
        UnorderedRead => ("bigquery.query.unordered_read", [], String),
        /// Boolean string, default `false`: forces query results through REST when `true`.
        ///
        /// Matches the dbt fork's option and reports the configured value. Automatic fallback
        /// after a Storage Read refusal does not change it. Setting `false` on a connection
        /// also clears the billing project's cached refusal so Storage Read can be tried again.
        UseStorageApiDisabledClient => (
            "bigquery.query.use_storage_api_disabled_client",
            ["adbc.bigquery.sql.query.use_storage_api_disabled_client"],
            String
        ),
    }
    into OptionConnection => |value| OptionConnection::Other(value.as_str().to_owned());
}

string_enum! {
    /// BigQuery-specific statement option keys.
    ///
    /// The `adbc.bigquery.*` keys are vendor extensions dbt-core 2.0 drives rather than options
    /// typed by hand; they are documented in the repository's `docs/DBT-CORE.md`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BigQueryStatementOption {
        /// A JSON object mapping each view to the datasets it is granted read access to, which
        /// is how dbt spells the `grant_access_to` model config.
        AuthorizeViewToDatasets => (
            "adbc.bigquery.dataset.authorize_view_to_datasets",
            [],
            String
        ),
        /// The bulk-ingest implementation: a `BigQueryBulkIngestMethod` value.
        BulkIngestMethod => ("bigquery.bulk_ingest.method", [], String),
        /// The destination of a copy-table job, as `[[project.]dataset.]table`.
        CopyTableDestination => ("adbc.bigquery.copy_table.destination", [], String),
        /// The source of a copy-table job, as `[[project.]dataset.]table`.
        ///
        /// Setting it selects the copy operation; the table ID passes through as given, so a
        /// partition decorator survives.
        CopyTableSource => ("adbc.bigquery.copy_table.source", [], String),
        /// How a copy-table job writes to an existing destination: a
        /// [`BigQueryWriteDisposition`](crate::BigQueryWriteDisposition) value.
        CopyTableWriteDisposition => (
            "adbc.bigquery.copy_table.write_disposition",
            [],
            String
        ),
        /// The ID of the Dataproc Serverless batch to create; unset lets Dataproc name it.
        ///
        /// Shared with `adbc.bigquery.notebook_execute_job.*`, which sends it as the notebook
        /// execution job's display name, so it selects no operation on its own.
        CreateBatchBatchId => ("adbc.bigquery.create_batch.batch_id", [], String),
        /// The YAML document describing the Dataproc Serverless batch to create.
        CreateBatchBatchYml => ("adbc.bigquery.create_batch.batch_yml", [], String),
        /// The `projects/{project}/locations/{region}` parent the Dataproc Serverless batch is
        /// created under.
        CreateBatchParent => ("adbc.bigquery.create_batch.parent", [], String),
        /// The field delimiter of the CSV file a dbt seed loads.
        CsvDelimiter => ("adbc.bigquery.ingest.csv_delimiter", [], String),
        /// The path of the CSV file a dbt seed loads, opened by the driver as given.
        CsvFilepath => ("adbc.bigquery.ingest.csv_filepath", [], String),
        /// The Arrow IPC stream schema describing the CSV file a dbt seed loads.
        CsvSchema => ("adbc.bigquery.ingest.csv_schema", [], Bytes),
        /// The region Dataproc and Vertex AI python-model jobs run in, which also selects the
        /// regional endpoint they are sent to.
        DataprocComputeRegion => ("adbc.bigquery.dataproc.compute_region", [], String),
        /// Seconds to wait for a submitted python-model job, spelled the way dbt-core 2.0 spells
        /// it. Selects no operation on its own.
        DataprocPoolingTimeout => ("adbc.bigquery.dataproc.pooling_timeout", [], Int),
        /// The project Dataproc python-model jobs are submitted to.
        DataprocProject => ("adbc.bigquery.dataproc.project", [], String),
        /// The existing Dataproc cluster a PySpark python-model job is submitted to.
        DataprocSubmitJobClusterName => (
            "adbc.bigquery.dataproc.submit_job.cluster_name",
            [],
            String
        ),
        /// The Cloud Storage path of the PySpark file a Dataproc job runs.
        DataprocSubmitJobGcsPath => ("adbc.bigquery.dataproc.submit_job.gcs_path", [], String),
        /// A JSON object `{"project": ..., "dataset": ...}` naming the dataset whose location the
        /// statement returns in a `location` column: one row, or none when the dataset is missing.
        GetDatasetLocation => ("bigquery.dataset.get_location", [], String),
        /// Whether the query may create its destination table: a `BigQueryCreateDisposition`
        /// value.
        CreateDisposition => (
            "bigquery.query.create_disposition",
            ["adbc.bigquery.sql.query.create_disposition"],
            String
        ),
        /// Boolean string: asks BigQuery to create a session, independently of the sessions ADBC
        /// transactions use.
        CreateSession => (
            "bigquery.query.create_session",
            ["adbc.bigquery.sql.query.create_session"],
            String
        ),
        /// The dataset unqualified names resolve in for this statement; empty restores the
        /// connection's current schema.
        DefaultDatasetId => (
            "bigquery.query.default_dataset_id",
            ["adbc.bigquery.sql.query.default_dataset_id"],
            String
        ),
        /// The project of this statement's default dataset; empty restores the connection's
        /// current catalog.
        DefaultProjectId => (
            "bigquery.query.default_project_id",
            ["adbc.bigquery.sql.query.default_project_id"],
            String
        ),
        /// Where the query writes its result, as `[[project.]dataset.]table` completed from the
        /// effective defaults; empty clears it.
        DestinationTable => (
            "bigquery.query.destination_table",
            ["adbc.bigquery.sql.query.destination_table"],
            String
        ),
        /// Boolean string: when true the query does not use BigQuery's query cache.
        DisableQueryCache => (
            "bigquery.query.disable_query_cache",
            ["adbc.bigquery.sql.query.disable_query_cache"],
            String
        ),
        /// Boolean string: validates the statement without running it, returning an empty stream
        /// carrying the inferred schema.
        ///
        /// A dry run creates no job to partition, so `execute_partitions` is rejected with
        /// `INVALID_STATE`.
        DryRun => (
            "bigquery.query.dry_run",
            ["adbc.bigquery.sql.query.dry_run"],
            String
        ),
        /// The job timeout in milliseconds; zero clears the override and a negative value is
        /// rejected.
        JobTimeout => (
            "bigquery.query.job_timeout",
            ["adbc.bigquery.sql.query.job_timeout"],
            Int
        ),
        /// Reservation resource name
        /// `projects/{project}/locations/{location}/reservations/{reservation}`. Empty clears the
        /// override; `none` requests on-demand billing when permitted by the
        /// project's or organization's reservation override policy. BigQuery validates the value.
        Reservation => ("bigquery.query.reservation", [], String),
        /// A JSON object of string job labels applied to every query job, such as
        /// `{"team":"data"}`; empty or `{}` clears them.
        ///
        /// Anything else, including a `key=value` or comma-separated string, is rejected. Reading
        /// the option back returns canonical JSON sorted by key.
        Labels => (
            "bigquery.query.labels",
            ["adbc.bigquery.sql.query.labels"],
            String
        ),
        /// Boolean string accepted for Go-driver compatibility, but a no-op: a failed job's error
        /// message ends with its console link either way.
        LinkFailedJob => (
            "bigquery.query.link_failed_job",
            ["adbc.bigquery.sql.query.link_failed_job"],
            String
        ),
        /// Recognized but returns `NOT_IMPLEMENTED`, because Google deprecated the field. Use
        /// `bigquery.query.max_bytes_billed`.
        MaximumBillingTier => (
            "bigquery.query.max_billing_tier",
            ["adbc.bigquery.sql.query.max_billing_tier"],
            Int
        ),
        /// The maximum bytes the query may bill; zero clears the override and a negative value
        /// is rejected.
        MaximumBytesBilled => (
            "bigquery.query.max_bytes_billed",
            ["adbc.bigquery.sql.query.max_bytes_billed"],
            Int
        ),
        /// The Cloud Storage bucket a notebook execution job reads its notebook from.
        ///
        /// Misspelled `gsc` in both dbt-core 2.0 and the dbt-labs fork; reproduced verbatim.
        NotebookExecuteJobGscBucket => (
            "adbc.bigquery.notebook_execute_job.gsc_bucket",
            [],
            String
        ),
        /// The Cloud Storage object path a notebook execution job reads its notebook from.
        ///
        /// Misspelled `gsc` in both dbt-core 2.0 and the dbt-labs fork; reproduced verbatim.
        NotebookExecuteJobGscPath => ("adbc.bigquery.notebook_execute_job.gsc_path", [], String),
        /// The file name of the notebook a notebook execution job runs.
        NotebookExecuteJobModelFileName => (
            "adbc.bigquery.notebook_execute_job.model_file_name",
            [],
            String
        ),
        /// The dbt model name a notebook execution job runs.
        NotebookExecuteJobModelName => (
            "adbc.bigquery.notebook_execute_job.model_name",
            [],
            String
        ),
        /// The `projects/{project}/locations/{region}` parent a notebook execution job is
        /// created under.
        NotebookExecuteJobParent => ("adbc.bigquery.notebook_execute_job.parent", [], String),
        /// The project a notebook execution job is submitted to.
        NotebookExecuteJobProject => ("adbc.bigquery.notebook_execute_job.project", [], String),
        /// The region a notebook execution job runs in, which also selects the Vertex AI host it
        /// is sent to.
        NotebookExecuteJobRegion => ("adbc.bigquery.notebook_execute_job.region", [], String),
        /// The principal the notebook execution job runs as, overriding the one the configured
        /// credentials name.
        ///
        /// Not sent by dbt-core 2.0. With plain Application Default Credentials nothing names a
        /// principal, so this is the only way to give the job one.
        NotebookExecuteJobServiceAccount => (
            "adbc.bigquery.notebook_execute_job.service_account",
            [],
            String
        ),
        /// The notebook runtime template a notebook execution job uses; omitted when unset.
        NotebookExecuteJobTemplateId => (
            "adbc.bigquery.notebook_execute_job.template_id",
            [],
            String
        ),
        /// How bound parameters are sent: `positional` (the default) or `named`.
        ParameterMode => (
            "bigquery.query.parameter_mode",
            ["adbc.bigquery.sql.query.parameter_mode"],
            String
        ),
        /// A JSON object of descriptions to set on the named top-level columns of the table
        /// [`Self::DestinationTable`] names, which is how dbt spells `persist_docs`.
        UpdateColumnsDescription => (
            "adbc.bigquery.table.update_columns_description",
            [],
            String
        ),
        /// A JSON object of policy tags to set on the named top-level columns of the table
        /// [`Self::DestinationTable`] names.
        UpdateColumnsPolicyTags => (
            "adbc.bigquery.table.update_columns_policy_tags",
            [],
            String
        ),
        /// The description to set on the relation [`Self::DestinationTable`] names. An empty
        /// string clears the option rather than the description.
        UpdateDescription => ("adbc.bigquery.table.update_description", [], String),
        /// Overrides the connection's `bigquery.query.prefetch_concurrency` for one statement.
        PrefetchConcurrency => (
            "bigquery.query.prefetch_concurrency",
            ["adbc.bigquery.sql.query.prefetch_concurrency"],
            Int
        ),
        /// The query's scheduling priority: a `BigQueryQueryPriority` value.
        Priority => (
            "bigquery.query.priority",
            ["adbc.bigquery.sql.query.priority"],
            String
        ),
        /// Overrides the connection's `bigquery.query.unordered_read` for one statement.
        UnorderedRead => ("bigquery.query.unordered_read", [], String),
        /// Overrides the connection's `bigquery.query.use_storage_api_disabled_client` for one
        /// statement.
        UseStorageApiDisabledClient => (
            "bigquery.query.use_storage_api_disabled_client",
            ["adbc.bigquery.sql.query.use_storage_api_disabled_client"],
            String
        ),
        /// How the query writes to an existing destination table: a `BigQueryWriteDisposition`
        /// value.
        WriteDisposition => (
            "bigquery.query.write_disposition",
            ["adbc.bigquery.sql.query.write_disposition"],
            String
        ),
        /// The Cloud Storage bucket a python model's source is uploaded to.
        WriteGcsBucket => ("adbc.bigquery.write_gcs.bucket", [], String),
        /// The bytes the python-model upload writes, sent as `text/plain; charset=utf-8`.
        WriteGcsContent => ("adbc.bigquery.write_gcs.content", [], String),
        /// The name of the Cloud Storage object the python-model upload writes.
        WriteGcsObjectName => ("adbc.bigquery.write_gcs.object_name", [], String),
    }
    into OptionStatement => |value| OptionStatement::Other(value.as_str().to_owned());
}

/// The ADBC SQLite driver's `adbc.statement.bind_by_name`, accepted as a boolean alias for
/// [`BigQueryStatementOption::ParameterMode`].
pub(crate) const OPTION_BIND_BY_NAME: &str = "adbc.statement.bind_by_name";
