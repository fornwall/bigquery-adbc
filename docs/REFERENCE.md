# Driver reference

Options, Arrow type mappings and behavior of this [ADBC][adbc-spec] driver. For installation, see
the [README](../README.md); for dbt 2.0, [DBT.md](DBT.md). `G<n>` and `UP-<n>` IDs refer to the
[Go driver comparison][go-comparison] and [UPSTREAM.md](UPSTREAM.md).

[adbc-spec]: https://arrow.apache.org/adbc/current/format/specification.html
[go-comparison]: GO-DRIVER-COMPARISON.md

## Rust API

The C ABI and the Python package are the supported interfaces. The Rust crate (library
`adbc_driver_bigquery`) is available only from Git, is unstable, and may change without notice.
Depend on it and on `adbc_core`, which defines the traits:

```toml
bigquery-adbc = { git = "https://github.com/fornwall/bigquery-adbc", default-features = false }
adbc_core = { git = "https://github.com/apache/arrow-adbc", rev = "aa2c1c2496d287e111d70d382e5d9af07568488b" }
```

- `default-features = false` leaves out the `ffi` feature (the C ABI), so a shared library built on
  this crate does not export `AdbcDriverBigqueryInit` and `AdbcDriverInit`.
- The `adbc_core` revision must match the pin in `Cargo.toml`; other revisions define incompatible
  trait types.
- Cargo does not apply a dependency's patches: copy every `arrow*` entry of this repository's
  [`[patch.crates-io]` table](../Cargo.toml) into your workspace root `Cargo.toml`, keep the Arrow
  crates on that revision (the Storage Read decoder relies on its offset-validation fixes), and
  commit your `Cargo.lock`.
- The crate enables the `release_max_level_off` features of `log` and `tracing`, which feature
  unification applies to the whole application. The driver's own [diagnostics](#diagnostics) are
  unaffected.
- The crate pins `idna_adapter` to 1.0.0, `idna`'s stub back end, which also applies to the whole
  application: `url` refuses non-ASCII host names instead of mapping them to Punycode.
- The API is synchronous. From async code, use `spawn_blocking`, or a multi-threaded Tokio runtime
  where the driver can use `block_in_place`; a call inside a current-thread Tokio runtime panics.
  Inside a Tokio runtime, `cancel` sends the remote job cancellation in the background and reports a
  failure only as a [diagnostic](#diagnostics).
- `BigQueryDatabaseOption`, `BigQueryConnectionOption` and `BigQueryStatementOption` are typed forms
  of the option keys below and convert into the `adbc_core` option types.

[`examples/query.rs`](../examples/query.rs) takes a project and query, connects with Application
Default Credentials (ADC; run `gcloud auth application-default login` once), and reads Arrow
batches. `execute()` returns a `RecordBatchReader` whose schema is available before the first batch.

## Connection URIs

The syntax matches the Go driver's:

```text
bigquery://[Host]:[Port]/ProjectID?OAuthType=...&DatasetId=...&Location=...
```

Host and port select an HTTPS REST endpoint, by default `bigquery.googleapis.com:443`, limited by the
[endpoint rules](#endpoint-overrides). `OAuthType` defaults to `0`:

| `OAuthType` | Authentication | Required parameters |
| --- | --- | --- |
| `0` | Application Default Credentials | none |
| `1` | JSON credential file | `AuthCredentials` |
| `2` | JSON credential string | `AuthCredentials` |
| `3` | Discrete user OAuth | `AuthClientId`, `AuthClientSecret`, `AuthRefreshToken` |

The other parameters are `DatasetId`, `Location`, `QuotaProject`, `ImpersonateTargetPrincipal`,
`ImpersonateDelegates`, `ImpersonateScopes` and `ImpersonateLifetime`; names are case-sensitive.
Duplicate, unknown, malformed, incompatible and incomplete parameters, user information, a fragment
and more than one path segment return `INVALID_ARGUMENT`. Values are decoded like Go's
`url.ParseQuery` (percent-escapes once, `+` as a space), so percent-encode credentials, including any
literal `+` in a base64 key. `%00` is rejected, because option values are C strings.

Set the URI with the database option `uri` (`ADBC_OPTION_URI`). It resets the configuration to
defaults before applying its values; options set before initialization apply after the URI whatever
their order. Reading `uri` returns it with `AuthCredentials`, `AuthClientSecret` and
`AuthRefreshToken` replaced by `<redacted>`; read individual options for the effective
configuration.

## Options

| Namespace | Contents |
| --- | --- |
| `bigquery.*` | The driver's own option keys. Use these. |
| `adbc.bigquery.sql.*` | Go-compatible aliases for most `bigquery.*` keys and some values; `adbc.bigquery.sql.api_endpoint`, which [dbt 2.0](DBT.md) sends, also aliases `bigquery.endpoint`. Getters return canonical values; errors name the key as the caller spelled it. |
| `adbc.bigquery.*` (no `sql`) | dbt 2.0 option-only operations with no `bigquery.*` equivalent; see [DBT.md](DBT.md). |

[Standard ADBC options](#standard-adbc-options) are also accepted. Use one spelling per option
during initialization, because driver managers can reorder options.

- Validation errors name the option and the expected type or values. They repeat the supplied value
  only for endpoint hosts and ports, since a value sent under the wrong key can be a credential. A
  credential file path, as an option or in `GOOGLE_APPLICATION_CREDENTIALS`, is not repeated
  either; its errors give the operating system's cause and the remedy.
- Optional string settings clear with `""`; JSON object options also accept `{}`. Booleans and enums
  with defaults require a listed value. Reading an unset option without a default returns
  `NOT_FOUND`, never `""`.
- Integer options accept integers or base-10 strings and are read with `GetOptionInt`:
  `bigquery.query.prefetch_concurrency`, `bigquery.query.job_timeout`,
  `bigquery.query.max_bytes_billed` and `adbc.bigquery.dataproc.pooling_timeout`. Their reset rules
  are listed below.
- Booleans accept `true` and `false` in any case and read back lowercase; `1`, `yes` and `on` are
  rejected.
- Project identifiers (modern IDs, numbers and legacy domain-scoped IDs) are opaque; BigQuery checks
  their validity and existence. An empty option value clears the option; otherwise an identifier
  must be one resource-name segment, so `/`, `\`, ASCII control characters, `.` and `..` are
  rejected. Qualified table options also accept `project.catalog.namespace.table`, a BigLake
  Iceberg table whose dataset ID is `catalog.namespace`, as in BigQuery's SQL. A domain-scoped
  project ends at the first dot after its colon, and any other project at the first dot.
- These Go driver options return `NOT_IMPLEMENTED` with a reason: `bigquery.query.use_legacy_sql`,
  `bigquery.query.allow_large_results`, `bigquery.query.disable_flattened_results`,
  `bigquery.query.result_buffer_size`, `bigquery.query.job_creation_mode`,
  `bigquery.query.results_format`, `bigquery.query.arrow_serialization_options.buffer_compression`
  and `bigquery.bulk_ingest.compression` (G15, G28, G29, G87). An option set on the wrong object
  also returns `NOT_IMPLEMENTED`, naming the object that takes it.

### Database options

| Option | Meaning |
| --- | --- |
| `bigquery.project_id` | Project that runs and is billed for jobs; empty clears it. |
| `bigquery.dataset_id` | Initial dataset for unqualified table names. |
| `bigquery.location` | Default location for every query, transaction session, and dbt 2.0 seed load and table copy. Without it, BigQuery infers each query's location, and a manual-commit transaction's session, a seed load and a table copy run in the default or destination dataset's location, cached per database for up to 32 datasets. |
| `bigquery.auth_type` | `auth_bigquery` (default), `app_default_credentials`, `json_credential_file`, `json_credential_string`, `user_authentication`, `external_account`, `temporary_access_token`, or `anonymous`. The first two both use ADC. |
| `bigquery.auth.credentials` | JSON credential file path or document, as selected by `auth_type`. Write-only. File mode rejects JSON documents, control characters and paths over 4,096 bytes; use `json_credential_string` for inline JSON. Destinations in the document follow the [endpoint rules](#endpoint-overrides). |
| `bigquery.auth.credentials_type` | Optional check of the JSON credential type: `service_account`, `authorized_user`, `impersonated_service_account`, or `external_account`. |
| `bigquery.auth.client_id` | OAuth client ID; required by `user_authentication`. |
| `bigquery.auth.client_secret` | OAuth client secret; required by `user_authentication`. Write-only. |
| `bigquery.auth.refresh_token` | OAuth refresh token; required by `user_authentication`. Write-only. |
| `bigquery.auth.access_token` | Access token; required by `temporary_access_token`. Write-only. Never refreshed, so requests fail after it expires. |
| `bigquery.auth.access_token_endpoint` | Where `user_authentication` redeems the refresh token; default `https://oauth2.googleapis.com/token`. Follows the [endpoint rules](#endpoint-overrides). |
| `bigquery.auth.external_account.audience` | Required workload identity provider, such as `//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider`. Workforce pools need a complete credential document in `json_credential_string`. |
| `bigquery.auth.external_account.request_url` | Required HTTPS identity-provider endpoint for `external_account`. HTTP requires `allow_untrusted_endpoint=true`; redirects are rejected. Write-only, since a URL query can contain secrets. |
| `bigquery.auth.external_account.request_data` | Required form-encoded POST body for the identity provider, sent unchanged; the response's `access_token` is exchanged. Write-only. |
| `bigquery.auth.external_account.impersonation_url` | Optional `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<principal>:generateAccessToken` URL. Custom impersonation endpoints are rejected. |
| `bigquery.auth.external_account.subject_token_type` | `urn:ietf:params:oauth:token-type:jwt` (default), or `urn:ietf:params:oauth:token-type:access_token` for a provider that returns an opaque token. |
| `bigquery.auth.external_account.token_url` | Security Token Service (STS) endpoint; default `https://sts.googleapis.com/v1/token` with the credential's universe domain substituted. Follows the [endpoint rules](#endpoint-overrides). |
| `bigquery.auth.quota_project` | Project ID or number sent as `x-goog-user-project`. Overrides `GOOGLE_CLOUD_QUOTA_PROJECT` with ADC; when unset, ADC keeps its normal environment and credential-file precedence. |
| `bigquery.auth.scopes` | Comma-separated base OAuth scopes ([authentication scopes](#authentication-scopes)). Rejected with `temporary_access_token` and `anonymous`, which mint no token. |
| `bigquery.impersonate.target_principal` | Target service account email; required when delegates or a lifetime is set. Only ASCII letters, digits and `@ . - _` are allowed. |
| `bigquery.impersonate.delegates` | Comma-separated delegation chain; each entry follows the `target_principal` rule. |
| `bigquery.impersonate.scopes` | Comma-separated OAuth scopes for the impersonated token ([authentication scopes](#authentication-scopes)). Scopes alone do not enable impersonation; a temporary access token remains unchanged. |
| `bigquery.impersonate.lifetime` | Positive Go duration up to 12 hours; default one hour. Above one hour, IAM requires `constraints/iam.allowServiceAccountCredentialLifetimeExtension` for the target account, checked at first use. |
| `bigquery.endpoint` | REST endpoint override, trusted with the credentials on any host and scheme ([endpoint rules](#endpoint-overrides)). A bare `host[:port]` uses HTTPS; use an explicit `http://` for an emulator or HTTP proxy. |
| `bigquery.storage_endpoint` | BigQuery Storage endpoint override, for both Read and Write. |
| `bigquery.gcs_endpoint` | Cloud Storage JSON API override for dbt python-model uploads; default `https://storage.googleapis.com`. |
| `bigquery.dataproc_endpoint` | Dataproc override for dbt python-model Spark submissions; default `https://{region}-dataproc.googleapis.com`. An override replaces the whole host, including the region. |
| `bigquery.vertex_endpoint` | Vertex AI override for dbt python-model notebook execution; default `https://{region}-aiplatform.googleapis.com`. An override replaces the whole host, including the region. |
| `bigquery.allow_untrusted_endpoint` | Boolean, default `false`. Lifts the Google-host requirement of the [endpoint rules](#endpoint-overrides). Cannot be set through a URI. |
| `bigquery.user_agent` | Printable ASCII text, such as `my-app/1.0`, appended to the driver's [user agent](#diagnostics). |

A credential option that the selected `bigquery.auth_type` does not read returns
`INVALID_ARGUMENT` when a connection is created. On a connection, `bigquery.project_id` and
`bigquery.dataset_id` are read-only aliases of `adbc.connection.catalog` and
`adbc.connection.db_schema`.

#### Authentication scopes

- The default scope is `https://www.googleapis.com/auth/cloud-platform`, except that user
  credentials keep their consent grant. External tables over Google Sheets or Drive also need
  `https://www.googleapis.com/auth/drive`.
- Without an impersonation layer, the base credential uses `bigquery.auth.scopes`, else
  `bigquery.impersonate.scopes`, else its default. `bigquery.impersonate.scopes` accepts dbt's
  `adbc.bigquery.sql.impersonate.scopes` alias, so dbt's default Drive scope reaches
  service-account keys and service-account ADC.
- With an impersonation target, `bigquery.auth.scopes` configures the source credential; unset, the
  source keeps its default scopes so it can call IAM even if the target only needs Drive.
  `bigquery.impersonate.scopes` configures the final token. Discrete `external_account` credentials
  fall back to `bigquery.impersonate.scopes` even with an impersonation layer.
- Service-account, impersonated and external-account credentials request the configured scopes.
  User credentials (`user_authentication`, and `authorized_user` JSON, including through ADC) can
  only request a subset of their original consent grant: the bundled SDK sends the OAuth `scope`
  field, and a refresh cannot add consent.

### Query and statement options

Options with scope "Connection, statement" can be set on either; a new statement copies the
connection's value. `bigquery.query.default_project_id` and `default_dataset_id` read back the
defaults in effect, including the connection's.

| Option | Scope | Behavior |
| --- | --- | --- |
| `bigquery.get_objects.skip_table_metadata` | Connection | Boolean, default `true`. At `GetObjects` tables depth, skips per-table describes and returns null columns and constraints. `false` fetches constraints; columns stay null. Column and all depths are unaffected. |
| `bigquery.get_objects.disable_wildcards` | Connection | Boolean, default `false`. `true` makes the `GetObjects` catalog, schema, table and column filters exact, case-sensitive names in which `%` and `_` match themselves, so names containing `_` get direct lookups ([metadata catalogs](#metadata-catalogs)). Unset filters still match everything; `GetStatistics` keeps pattern matching. |
| `bigquery.bulk_ingest.method` | Connection, statement | `storage_write` (default and only supported value). `load` returns `NOT_IMPLEMENTED`. |
| `bigquery.query.prefetch_concurrency` | Connection, statement | Positive integer, default `10`: the maximum REST row batches one reader converts concurrently. Fewer run once pending conversions hold 64 MiB of input, and driver-wide conversion is bounded by available parallelism. Not used by Storage Read. |
| `bigquery.query.unordered_read` | Connection, statement | Boolean, default `false`. Lets Storage Read read several streams in parallel and return batches in arrival order. Rows are never lost or duplicated, but `ORDER BY` order is not kept. |
| `bigquery.query.use_storage_api_disabled_client` | Connection, statement | Boolean, default `false`. `true` forces REST results; see [Storage Read](#reading-large-results-through-the-storage-read-api). |
| `bigquery.query.default_project_id` | Statement | Project for the default dataset; empty restores the connection's current catalog. Clearing it returns `INVALID_STATE` while a statement default dataset is set and no connection catalog or database project can replace it. |
| `bigquery.query.default_dataset_id` | Statement | Dataset for unqualified names; empty restores the connection's current schema. A non-empty value needs a project from the statement, connection or database, else `INVALID_STATE`. |
| `bigquery.query.destination_table` | Statement | `table`, `dataset.table`, `project.dataset.table`, or `project.catalog.namespace.table` for a BigLake Iceberg table, completed from the statement and connection defaults; empty clears it. Execution writes the result there and returns its schema with no rows; partitions from `AdbcStatementExecutePartitions` read the table. |
| `bigquery.query.create_disposition` | Statement | `CREATE_IF_NEEDED` (default) or `CREATE_NEVER`. |
| `bigquery.query.write_disposition` | Statement | `WRITE_EMPTY` (default), `WRITE_APPEND`, or `WRITE_TRUNCATE`. |
| `bigquery.query.max_bytes_billed` | Statement | Integer maximum bytes billed. Zero clears the override; negative values are rejected. |
| `bigquery.query.job_timeout` | Statement | Integer timeout in milliseconds. Zero clears the override; negative values are rejected. A job BigQuery stops at the timeout fails with `TIMEOUT`, naming this option; a cancelled job stays `CANCELLED`. |
| `bigquery.query.reservation` | Statement | `projects/{project}/locations/{location}/reservations/{reservation}`, or `none` for on-demand billing where policy allows it; empty (default) clears the override. BigQuery validates the value. Query jobs and dbt 2.0 seed load and table copy jobs carry it. |
| `bigquery.query.labels` | Statement | A JSON object of string job labels, such as `{"team":"data"}`, for every query job. Other formats, including `key=value`, return `INVALID_ARGUMENT`. Reads back as canonical JSON sorted by key. |
| `bigquery.query.link_failed_job` | Statement | Boolean, accepted for Go driver and dbt 2.0 compatibility but ignored: a failed job's error always ends with its console link. |
| `bigquery.query.priority` | Statement | `INTERACTIVE` (default) or `BATCH`. |
| `bigquery.query.disable_query_cache` | Statement | Boolean; `true` disables the BigQuery query cache. |
| `bigquery.query.dry_run` | Statement | Boolean. Validates without executing and returns an empty stream with the inferred schema and `-1` rows affected; `execute_partitions` returns `INVALID_STATE`, since a dry run creates no job. Bound parameters are typed from their schema and stay bound. For ingest statements, see [bulk ingest](#bulk-ingest). A dbt 2.0 seed only checks its file and schema; the other dbt operations that change something return `NOT_IMPLEMENTED`. |
| `bigquery.dataset.get_location` | Statement | JSON `{"project": ..., "dataset": ...}`. The statement reads that dataset's location with `datasets.get` instead of running SQL, returning a `location` column with one row, or none when the dataset is missing. See [dataset location](DBT.md#dataset-location). |
| `bigquery.query.create_session` | Statement | Boolean; asks BigQuery to create a session, independent of ADBC transaction sessions. Execution returns `INVALID_STATE` while `adbc.connection.autocommit` is `false`, because queries then run in the transaction's session. |
| `bigquery.query.parameter_mode` | Statement | `positional` (default) or `named`. `adbc.statement.bind_by_name` sets the same mode as a boolean; reading either key reports the current mode. |
| `bigquery.query.max_billing_tier` | Statement | Returns `NOT_IMPLEMENTED`, because Google deprecated the field. Use `bigquery.query.max_bytes_billed`. |

### Standard ADBC options

| Option | Scope | Behavior |
| --- | --- | --- |
| `adbc.connection.autocommit` | Connection | Boolean, default `true`; `false` selects manual commit, which the [transactions](#transactions) section describes. |
| `adbc.connection.readonly` | Connection | Only `false` is accepted, as a no-op; `true` returns `NOT_IMPLEMENTED`. Reads as `false`. |
| `adbc.connection.transaction.isolation_level` | Connection | `adbc.connection.transaction.isolation.default` and `adbc.connection.transaction.isolation.snapshot` are accepted as no-ops; other levels return `NOT_IMPLEMENTED`. Reads as the snapshot level, which BigQuery transactions provide. |
| `adbc.connection.catalog` | Connection | Current catalog: the project for unqualified names. Empty clears it; unqualified names, including a `GetTableSchema` table without a catalog, then resolve in `bigquery.project_id`, which the option then reads back and the metadata listings treat as the current project. |
| `adbc.connection.db_schema` | Connection | Current schema: the dataset for unqualified names. Empty clears it. A change to either option reaches existing statements at their next execution; a statement's `bigquery.query.default_*` options take precedence. |
| `adbc.statement.bind_by_name` | Statement | Boolean, default `false`. Alias of `bigquery.query.parameter_mode`: `true` is `named`, `false` is `positional`. |
| `adbc.ingest.mode` | Statement | `adbc.ingest.mode.` followed by `create` (default), `append`, `replace` or `create_append`. The DB-API accepts short names such as `mode="create"`. See [bulk ingest](#bulk-ingest). |
| `adbc.ingest.target_table` | Statement | Table to ingest into. Setting it puts the statement in ingest mode; empty clears the target without discarding a SQL query already set. |
| `adbc.ingest.target_catalog` | Statement | Project for the ingest target; empty clears it. Unset falls back to `bigquery.query.default_project_id`, then the connection's current catalog, then `bigquery.project_id`. |
| `adbc.ingest.target_db_schema` | Statement | Dataset for the ingest target; empty clears it. Unset falls back to `bigquery.query.default_dataset_id`, then the connection's current schema. |
| `adbc.ingest.temporary` | Statement | Only `false` is accepted, as a no-op; `true` returns `NOT_IMPLEMENTED`. |
| `adbc.statement.exec.incremental` | Statement | Only `false` is accepted, as a no-op; `true` returns `NOT_IMPLEMENTED`. |
| `adbc.statement.exec.progress` | Statement | Read-only double: bytes processed so far, from the responses execution already receives, then the final count once BigQuery reports it. Adds no polling requests. With several bound parameter rows every row's job counts, except that `ExecuteQuery` of a statement that returns rows counts the first. `GetParameterSchema` preserves the previous execution's progress, including when its dry run retries. |
| `adbc.statement.exec.max_progress` | Statement | Read-only double: the final processed-byte count once a terminal response reports it, otherwise `0` (adbc.h's "maximum not known"). A fresh statement, a dry run and an option-only operation report `0`. |

## Supported types

Every result column and `GetTableSchema` field carries its BigQuery type as a GoogleSQL type literal
in `BIGQUERY:type` field metadata. Bound parameters and bulk-ingest input accept the same spelling.

| BigQuery type | Arrow type | `BIGQUERY:type` |
| --- | --- | --- |
| `INT64` | `Int64` | `INT64` |
| `FLOAT64` | `Float64` | `FLOAT64` |
| `NUMERIC` | `Decimal128(38, 9)` | `NUMERIC` |
| `BIGNUMERIC` | `Decimal256(76, 38)` | `BIGNUMERIC` |
| `BOOL` | `Boolean` | `BOOL` |
| `STRING` | `Utf8` | `STRING` |
| `BYTES` | `Binary` | `BYTES` |
| `DATE` | `Date32` | `DATE` |
| `DATETIME` | `Timestamp(Microsecond, None)` | `DATETIME` |
| `TIME` | `Time64(Microsecond)` | `TIME` |
| `TIMESTAMP` | `Timestamp(Microsecond, "UTC")` | `TIMESTAMP` |
| `INTERVAL` | `Interval(MonthDayNano)` | `INTERVAL` |
| `GEOGRAPHY` | `Utf8`, extension type `geoarrow.wkt` | `GEOGRAPHY` |
| `JSON` | `Utf8`, extension type `arrow.json` | `JSON` |
| `RANGE<T>` | `Struct<start: T, end: T>`, both bounds nullable | `RANGE<DATE>`, `RANGE<DATETIME>`, `RANGE<TIMESTAMP>` |
| `ARRAY<T>` (a `REPEATED` field) | `List<T>` | `ARRAY<T>` on the list field, `T` on its item field |
| `STRUCT<...>` | `Struct<...>` | ``STRUCT<`x` DATE>`` |

Only `GEOGRAPHY` and `JSON` set `ARROW:extension:name`; `GEOGRAPHY` also sets
`ARROW:extension:metadata` to `{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}`.

Declared shapes live in metadata rather than the Arrow type, so `GetTableSchema` matches a
`SELECT *` of the table: a `STRING(L)` or `BYTES(L)` length in `BIGQUERY:max_length`, and the
precision and scale of a `NUMERIC(P, S)` or `BIGNUMERIC(P, S)` column (which keeps its family's full
Arrow type) in `BIGQUERY:precision` and `BIGQUERY:scale`. A repeated column has these keys on the
list field. `GetObjects` reports `P` and `S` as `xdbc_column_size` and `xdbc_decimal_digits`. Query
results never declare a decimal shape. Bulk ingest honors the keys on a decimal field, so a table's
schema ingested back creates the same column.

For Go driver consumers, fields also carry these keys when BigQuery reports the information;
unreported keys are omitted, never `"0"` or `""`. A list's `item` field carries none of them, only
`BIGQUERY:type` and any Arrow extension keys its type needs.

| Key | Present on | Value |
| --- | --- | --- |
| `Type` | Every field | The REST type spelling exactly as BigQuery reported it |
| `Repeated` | `REPEATED` fields | `true` |
| `Description` | Fields with a non-empty description | The description |
| `MaxLength` | Fields with a declared length | Same as `BIGQUERY:max_length` |
| `Collation` | Fields with a non-empty collation | The collation |
| `PolicyTags` | Columns with policy tags | The tag names as JSON, `{"names":[…]}` |

Conversion rules:

- `BIGQUERY:type` never uses the REST spellings `INTEGER`, `FLOAT`, `BOOLEAN`, `RECORD`, `DECIMAL`
  or `BIGDECIMAL`, which the driver accepts only from BigQuery itself. Caller-supplied metadata with
  a REST spelling, or contradicting the Arrow type it annotates, is rejected with an error naming
  the GoogleSQL type.
- Decimal parameters and ingest fields may be Arrow `Decimal32`, `Decimal64`, `Decimal128` or
  `Decimal256`. The driver picks the smallest BigQuery decimal type that holds the field's full
  precision and scale (`NUMERIC` allows 29 integer and 9 fractional digits, `BIGNUMERIC` 38 of
  each); compatible `BIGQUERY:type` metadata can select either explicitly. Negative Arrow scales are
  exact at scale zero. Values must fit their declared Arrow precision even when the destination is
  wider. An ingest field with explicit `BIGQUERY:precision` and `BIGQUERY:scale` accepts zero at any
  negative source scale. Unsupported shapes are rejected before any request is sent.
- BigQuery stores `TIME`, `DATETIME`, `TIMESTAMP` and `INTERVAL` at microsecond precision, so bound
  or ingested nanosecond values are truncated, nested values included. `DATETIME` and `TIMESTAMP`
  round down (`-1,999 ns` becomes `-2 us`); `TIME` and `INTERVAL` truncate towards zero.
- A `REPEATED` column has no null state, so a NULL list ingested into one reads back as an empty
  array. A NULL element inside a list is rejected.
- A `TIMESTAMP`, `DATETIME` or `TIME` column declaring more than six sub-second digits returns
  `NOT_IMPLEMENTED`, naming a `CAST` that makes it readable. BigQuery does not yet allow such
  columns.

## Behavior notes

### Query submission and cancellation

- Ordinary queries use [`jobs.query`](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query)
  with `JOB_CREATION_OPTIONAL` and `timeoutMs: 10000`, so a short query can return its results
  without a job. If BigQuery creates a job and the response is incomplete, the driver waits through
  `jobs.getQueryResults` (up to 45 seconds per request). `AdbcStatementExecutePartitions` requires
  job creation, because its descriptors need durable job references; dry runs and job-only options
  use `jobs.insert`. None of this is configurable, and only `bigquery.query.job_timeout` sets an
  execution deadline.
- Waiting for a query, table copy or CSV load outlasts ten minutes of consecutive retryable failures
  (transport errors, timeouts, retryable statuses such as 5xx). The window restarts whenever
  BigQuery reports the job unfinished or returns results, and is checked between rounds of
  requests, so a wait can overrun by one round (with every request at its 60-second timeout, about
  ten minutes for a query, five for a copy or load). A job `jobs.get` reports finished without an
  error completes without its results when a read could add nothing, as for DML and other
  statements that return no rows; its counts and statement type come from the job. A wait that
  gives up fails with the last error and says the job's outcome is unknown, so check the job before
  running it again. It requests cancellation of the unfinished job, except for a statement the
  driver runs outside any operation, such as a `COMMIT`.
- Cancellation stops local work immediately and
  [requests cancellation](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)
  of any unfinished remote job, which BigQuery handles asynchronously. During the initial
  `jobs.query`, the remote cancel waits for the response to name an unfinished job, so it can lag by
  the ten-second completion wait plus transport delays, and a query that completes first cannot be
  stopped. Ambiguous submissions keep recovering their job reference after cancellation; if none
  arrives, the remote work cannot be cancelled.
- BigQuery may ignore a `requestId` on read-only queries, so a resent submission may not recover the
  original execution. It ignores one on `BEGIN`, `COMMIT` and `ROLLBACK TRANSACTION`, so the driver
  submits its own with `jobs.insert`. Such a statement written in SQL still uses `jobs.query`: if
  its answer is lost, the retry fails with "No active transaction found" and, like any definitive
  answer to a retry, says the statement may still have run.
- `jobs.insert` uses a driver-chosen job ID, stable across submission retries. If its answer is lost
  and no location is configured, recovery tries the location named by a duplicate-job response, US
  and EU, and known dataset locations, then
  [lists the caller's recent jobs](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list)
  across regions for the exact ID, within 30 seconds; this needs no extra IAM permission. If
  recovery fails, the error names the job that may still be running. Setting `bigquery.location`
  avoids the search.
- After EOF, an error or cancellation, a result stream releases its resources, keeping its schema
  until the stream is released. An unpolled stream observes cancellation on its next read.

### Transactions

`adbc.connection.autocommit=false` selects manual commit; the BigQuery transaction session opens at
the next query that needs one.

- A `COMMIT TRANSACTION` or `ROLLBACK TRANSACTION` in SQL, alone or in a script, ends the
  transaction, and the next statement starts a new one.
- Commit and rollback ignore cancellation for up to a minute, plus ten seconds to end the session
  after a failure.
- A commit keeps the transaction open only when BigQuery provably refused it, which
  `bigquery.job.verdict` shows. Any other commit or rollback failure ends the session, or says that
  could not be done or confirmed; a commit without a verdict returns `IO` and says its outcome is
  unknown.
- Bulk ingest and dbt 2.0 option-only operations other than `bigquery.dataset.get_location` are
  refused in manual mode, and a statement that outlives a manual-mode connection returns
  `INVALID_STATE`.

### Errors

Messages say which step failed: `BigQuery rejected the query` for a submission BigQuery refused,
`BigQuery stopped the query` for one it stopped while answering the submission (such as at the job
timeout), and `failed to submit` for one that got no verdict. When `jobs.query` submission retries
end that way after an attempt that may have reached BigQuery, the message adds that BigQuery may
still have run the statement; a new submission would be a new execution, so check first. Attempts
that could not connect (DNS, TCP, TLS) reached nothing. A job that fails after acceptance is named as
`<project>:<location>.<id>`, and its message ends with the job's console link. BigQuery's errors
follow as `<reason>: <message>`. `AdbcErrorGetDetail` exposes these keys when available:

| Detail key | Contents |
| --- | --- |
| `bigquery.error.reason` | BigQuery's machine-readable error reason |
| `bigquery.error.location` | BigQuery's error location |
| `bigquery.job_id` | The job ID |
| `bigquery.job.verdict` | What the failure proves about its job: `failed` when the job reported it, `not_created` when BigQuery refused the submission or never received it. A failure without it proves neither |
| `bigquery.error.http_status` | The HTTP status of a REST call |
| `bigquery.error.rpc_code` | The status code of a gRPC call |
| A protobuf type name, such as `google.rpc.ErrorInfo` | A structured `google.rpc.Status` detail as UTF-8 ProtoJSON, including `@type`. Repeated types keep the service's order; untyped details are skipped. |

ADBC 1.0 callers also receive the numeric status in `AdbcError::vendor_code`; ADBC 1.1 reserves that
field for its layout sentinel. Credential failures do not forward structured `google.rpc.Status`
details.

| Condition | Status |
| --- | --- |
| A service API not enabled in the project (`accessNotConfigured`, or a `SERVICE_DISABLED` `google.rpc.ErrorInfo`) | `INVALID_STATE`, keeping the service's message on how to enable it |
| Credentials that cannot mint an access token | `UNAUTHENTICATED`, with a remedy for the configured credential |
| A token service answering with a temporary failure, such as HTTP 503 | `IO`, retried like other transient failures |
| A transient token-exchange failure for explicit credentials or impersonation, including a network failure | `IO`; a missing ambient ADC metadata server still returns `UNAUTHENTICATED` with setup advice |
| A token service that accepts a connection and never answers, for any credential that mints tokens | `IO` after about two minutes; retries and later requests fail within seconds until a token arrives. A wait for a running job outlasts this like any other retryable failure |
| A JSON response that cannot be decoded | `INVALID_DATA`, reporting the document, error category, and line and column, without response values |

`invalidQuery` and `accessDenied` set SQLSTATE `42000` (syntax error or access rule violation);
other failures leave SQLSTATE unset. Driver-side parameter and ingest validation errors describe the
field, type or constraint, and row where available, but never include rejected cell values, numeric
and temporal values included.

### Row counts

`AdbcStatementExecuteQuery` reports `rows_affected` before the result stream is read; the DB-API
exposes it as `cursor.rowcount`, with `-1` meaning unknown.

| Statement | `rows_affected` |
| --- | --- |
| Query, or a script with a result | BigQuery's `totalRows` for the result: the rows the stream will yield, `0` when empty |
| DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) | Rows affected, `0` included |
| DDL, a script without a result (even one ending in DML, which BigQuery reports no count for in a script), a dry run | `-1` |
| A query with `bigquery.query.destination_table` | `-1`: BigQuery's `totalRows` counts the whole destination table |
| Bound parameters | One row: that row's count, as above. Several rows of DML, DDL or a destination-table query: every row runs before the call returns, and the counts sum (`-1` if any is unknown). Several rows of a query returning results: `-1`, since rows after the first run while the stream is read. No row: `0` |
| Bulk ingest | Rows written; `0` for a dry run |
| `bigquery.dataset.get_location` | The rows returned: `1`, or `0` for a missing dataset |

A script's result is its last query's, unless DML, DDL or `ASSERT` ran after that query; procedural
statements such as `SET` and `IF` do not discard it.

`AdbcStatementExecutePartitions` runs every job before it returns and reports the same counts,
summed across parameter rows (`-1` if any is unknown). `AdbcStatementExecuteQuery` with a `NULL`
`out`, like the Rust API's `execute_update`, reports the DML count, summed across parameter rows,
and `-1` for any other SQL, a query included.

### Bound parameters

Placeholders are positional (`?`) by default; with `bigquery.query.parameter_mode=named` (or
`adbc.statement.bind_by_name=true`) each Arrow field binds the `@name` it is named after,
case-insensitively. Each bound row runs as its own query job, and every row must yield the same
result schema. `ExecuteQuery` of a statement that returns rows, including a stored-procedure `CALL`,
runs the first row before it returns and each later row once the previous result is read, so
results arrive in row order. DML, DDL, queries with an explicit destination table, and every other
execute run all bound rows before returning; closing their empty result stream does not skip writes.

- An execute consumes the binding, so executing again returns `INVALID_STATE` until the statement is
  rebound. `ExecuteSchema` and dry runs read no row and keep the binding. On an unbound statement,
  `ExecuteSchema` and `GetParameterSchema` let a dry run type the placeholders.
- `Bind` and `BindStream` return `INVALID_ARGUMENT` for a schema nested more than 32 levels below the
  batch, counting each child and dictionary as a level. BigQuery itself allows 15.
- A value its parameter type cannot hold, such as a date after year 9999, returns `INVALID_DATA`
  naming the parameter and row.
- BigQuery ignores bound parameters the SQL never references, so binding too many columns is not
  an error; a placeholder nothing binds returns `INVALID_ARGUMENT`.

| Arrow input | Parameter type |
| --- | --- |
| `Int8` to `Int64`, `UInt8` to `UInt64` | `INT64`; a `UInt64` above the `INT64` range is rejected |
| `Float16`, `Float32`, `Float64` | `FLOAT64`, widened exactly |
| `Boolean` | `BOOL` |
| `Utf8`, `LargeUtf8`, `Utf8View` | `STRING`, or `JSON` or `GEOGRAPHY` by extension type or `BIGQUERY:type` |
| `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | `BYTES` |
| `Date32`, `Date64` holding whole UTC days | `DATE` |
| `Time32`, `Time64` | `TIME` |
| `Timestamp` with a time zone, or without | `TIMESTAMP`, or `DATETIME` |
| `Decimal32` to `Decimal256` | `NUMERIC` or `BIGNUMERIC`, as in the conversion rules above |
| `Interval`, `Duration` (a duration's whole span is the time part) | `INTERVAL` |
| `Null`, as pyarrow infers for an all-`None` column | A NULL of the scalar type its `BIGQUERY:type` names, else a `STRING` NULL; where the SQL needs another type, annotate the field or write `CAST(? AS INT64)` |
| `List`, `LargeList`, `FixedSizeList`, `ListView`, `LargeListView` | `ARRAY`; a directly nested array or a NULL element is rejected |
| `Struct` | `STRUCT`, or `RANGE<T>` with that metadata and `start` and `end` fields |
| `Dictionary` with integer keys | Its value type |

### Reading large results through the Storage Read API

A completed query job's results, including a script's, are read through the
Storage Read API when the first response has a continuation token, or at any size when BigQuery
refuses the first page because a row exceeds its 20 MiB response limit. A complete first response
stays on REST. The path is chosen before the schema or any row is reported, and both return the
same schema and values.

REST is used instead when:

- BigQuery returns a complete result without creating a job;
- the `jobs.get` naming the job's result table fails;
- the results went to one partition of a destination table (`table$20240101`);
- the read session is empty, or its Arrow schema uses an encoding outside BigQuery's documented
  mapping or cannot be reconciled with the expected schema (a malformed schema is an error);
- `CreateReadSession` fails with `PERMISSION_DENIED` (no `bigquery.readsessions.create`),
  `UNIMPLEMENTED` (an emulator, or an endpoint without the Storage API), `RESOURCE_EXHAUSTED` (the
  rate limit), or `INVALID_ARGUMENT` (a table Storage Read refuses but REST reads, such as a
  destination table with `require_partition_filter`).

`CreateReadSession` retries `UNAVAILABLE` and connection failures for up to a minute; any other
status is an error, and a call unanswered for two minutes, even by a stalled TLS handshake, fails
with `TIMEOUT`. Fallback happens before the first batch reaches the caller, so no row is repeated,
and is reported only as a `warn` [diagnostic](#diagnostics). Readers never switch after delivering
rows, so with Storage Read disabled or unavailable, an oversized row on a later REST page fails the
stream. If BigQuery refuses the first page, disabling Storage Read fails with `INVALID_ARGUMENT`,
and a refused Storage Read attempt keeps that failure's status, vendor code, SQLSTATE and details
alongside the REST size-limit explanation.

A `PERMISSION_DENIED` or `UNIMPLEMENTED` refusal, or a permission denial of that `jobs.get`, is
cached per billing project for the whole database. Setting
`bigquery.query.use_storage_api_disabled_client` to `false` on a connection clears the cache, for
example after the permission is granted; setting it on a statement does not. The option reads back
as configured, regardless of automatic fallback.

Read sessions use one stream unless `bigquery.query.unordered_read` is `true` (G26). Reading a
query's anonymous result table is free; a partition of a query with
`bigquery.query.destination_table` reads that permanent table, billed at the data-extraction rate.
A partition is read with the `prefetch_concurrency`, `unordered_read` and
`use_storage_api_disabled_client` values of the statement that created it, whichever connection
reads it. Pass partition descriptors from `AdbcStatementExecutePartitions` to
`AdbcConnectionReadPartition` unchanged; malformed ones return `INVALID_ARGUMENT` before any request.

Readers buffer at most 200 completed batches and 64 MiB of their allocations, and pending REST
conversions at most 64 MiB of decoded input. A larger batch takes the whole budget and waits until
the reader's buffer is empty; a larger REST conversion can always proceed. These are not
process memory limits: response pages, conversions in progress and batches returned to the
application are outside them.

A `ReadRows` open unanswered for two minutes, or a stream that delivers no complete response for
two minutes while the reader has room, counts as a failed attempt and resumes at the next unread
row. Each consecutive failure doubles the stream's limit, to 32 minutes on the fifth attempt, and a
response carrying rows resets it. Five failures in a row fail the read with `TIMEOUT`: after about
10 minutes on a dead connection, and after up to 62 minutes on a stream that opens and then stays
silent.

A paused reader never trips its own limit, but every read on a database shares one HTTP/2
connection and its 5 MiB flow-control window, of which each stream can hold 2 MiB unread. Three
paused readers can therefore starve every other read and new `CreateReadSession` call on that
database, which fail with `TIMEOUT` if the readers stay paused. Finish or release a result before
starting another that must make progress.

### Result-schema metadata

`SELECT` results require a schema even without rows: a `SELECT` with no available result schema
returns `INVALID_DATA`, even when a later job-metadata lookup identifies the statement type.

Result schemas carry `BIGQUERY:query_id` (the job ID, or the `queryId` of a jobless response) and
the statistics BigQuery reported, as decimal integer strings except the statement type. Unreported
statistics are omitted, never written as zero. The schema `AdbcStatementExecutePartitions` returns
carries none of these keys, since its partitions can be separate jobs; each partition's stream
carries its own job's.

| Key | Notes |
| --- | --- |
| `BIGQUERY:statistics:query:total_bytes_processed` | |
| `BIGQUERY:statistics:total_bytes_processed` | |
| `BIGQUERY:statistics:query:total_bytes_billed` | |
| `BIGQUERY:statistics:query:slot_millis` | |
| `BIGQUERY:statistics:query:num_dml_affected_rows` | DML only |
| `BIGQUERY:statistics:query:statement_type` | GoogleSQL statement type as reported by BigQuery, including for jobless results; never inferred from SQL. |

Each statistic is also written under its PascalCase alias, such as
`BIGQUERY:Statistics:Query:StatementType`, which dbt 2.0 reads
([list of aliases](DBT.md#result-metadata)). The public Rust constants name the lowercase keys;
`BIGQUERY_SCHEMA_METADATA_JOB_KEYS` lists both spellings, for comparing schemas across jobs.

Statistics come from the query's own responses, plus best-effort `jobs.get` requests when those omit
the statement type or costs, or Storage Read needs the result table. Fetched metadata is reused. If
fetching fails, the keys only it would supply are absent and the statement still succeeds.

### Bulk ingest

Bulk ingest uses the Storage Write API.

| Mode | Table preparation and failure behavior |
| --- | --- |
| `create` | Requires a new table. If a later step fails, the created table is kept. A failure before commit reports that no rows were committed and suggests retrying with `append`. |
| `append` | Requires an existing compatible table. |
| `create_append` | Creates a missing table, or appends to an existing compatible one. |
| `replace` | With the same schema, uploads before truncating, so an upload failure or cancellation keeps the old rows; a failure after truncation can leave the table empty, and the error says so. With a different schema, recreates the table before uploading, so a later failure leaves the new, empty table, and the error says the old rows are gone. A cancel or lost response after a `TRUNCATE` or recreation is sent says the old rows may be gone, since BigQuery can finish such a statement. The `TRUNCATE` before the commit cannot be cancelled, and BigQuery stops it after ten minutes. |

Execution:

- Batches are converted and appended in request-sized windows.
- An append unanswered for five minutes is replayed on a new connection at its original offset, and
  a stream call unanswered for a minute fails with `TIMEOUT`, so a silently dead connection cannot
  hang an ingest.
- Bulk ingest returns `INVALID_STATE` while `adbc.connection.autocommit` is `false`, before any
  request is sent or bound data is consumed: a Storage Write stream cannot join a session, and
  BigQuery does not allow DDL that creates or drops permanent tables inside a transaction.
- With `bigquery.query.dry_run`, an ingest validates instead of writing: it resolves the target,
  converts the bound Arrow schema, checks the target against the ingest mode with one read-only
  table lookup, and reports zero rows. Row values are not validated, and the data stays bound, so
  clearing the option and executing again ingests it (G30).

Columns and values:

- Append matches columns by name, ignoring case and order. Omitted nullable columns become NULL,
  and omitted repeated columns become empty arrays. `JSON` and `GEOGRAPHY` columns also accept plain
  strings, which BigQuery parses as JSON text or WKT, and `STRING` columns accept `GEOGRAPHY` values
  as WKT text. Arrow nullability need not match `REQUIRED`, but Storage Write rejects actual NULL
  values in required columns.
- An appended field adopts what the target column declares, and every value is checked against it.
  A decimal takes the column's type, precision and scale; a value with a fractional digit the column
  would round away, or too many integer digits, returns `INVALID_DATA` naming its row in the bound
  stream. A string or bytes field without `BIGQUERY:max_length` takes the column's length, and a
  declared one must fit the column's; unrelated Arrow extension annotations do not change this. A
  field of Arrow type `Null`, which pandas and Polars infer for an all-null column, takes the
  column's type unless the column is `REQUIRED`.
- Column names must start with an ASCII letter or underscore and contain only ASCII letters, digits
  and underscores. Storage Write cannot write flexible column names (such as `first name`), so an
  append to a table with one at any depth returns `NOT_IMPLEMENTED`; `replace` recreates it (UP-96).
- NULL structs with nullable scalar children are supported, and their hidden child values are
  ignored. Storage Write rejects NULL structs with `REQUIRED` or `REPEATED` descendants, even when
  the hidden values are valid; the error names the required field or reports `Null lists are not
  allowed for repeated fields`.
- RANGE lower bounds must precede their upper bounds after conversion to microsecond precision.
  Equal or reversed bounds return `INVALID_DATA`; NULL bounds remain unbounded. A `RANGE<DATETIME>`
  bound after `3600-12-31 23:59:59.999999` is rejected by Storage Write as an invalid timestamp,
  with no driver workaround (UP-30).
- Storage Write fails every append carrying an `ARRAY<TIMESTAMP>` field with `INTERNAL` (UP-90), so
  bound data with one at any depth returns `NOT_IMPLEMENTED` naming the field before any request is
  sent, in every mode and in a dry run. An append that omits an existing `ARRAY<TIMESTAMP>` column
  is unaffected. To fill such a column, ingest the values as `ARRAY<STRUCT<ts TIMESTAMP>>` into a
  staging table and convert them with SQL:

  ```sql
  INSERT INTO target (id, ts)
  SELECT id, ARRAY(SELECT item.ts FROM UNNEST(ts) AS item WITH OFFSET AS position ORDER BY position)
  FROM staging
  ```

### Endpoint overrides

Endpoints receive the configured credentials. A URI or a credential document cannot send them
somewhere new, but an explicit option can. Unless `bigquery.allow_untrusted_endpoint` is `true`:

| Endpoint | Accepted |
| --- | --- |
| `bigquery.endpoint` or its Go/dbt aliases, set as an option | Any host and scheme, including an HTTP proxy; dbt's `api_endpoint` needs no other option. A value read back and set again counts as explicit, so copy options between databases only from a trusted source. |
| A `bigquery://` URI's host | HTTPS on `googleapis.com` or a subdomain, or any host with `anonymous` authentication. An explicit `bigquery.endpoint` overrides it. |
| Storage Read/Write, Cloud Storage, Dataproc and Vertex AI | HTTPS on `googleapis.com` or a subdomain, or any host with `anonymous` authentication (for emulators). A custom REST endpoint does not authorize these, and other Google Cloud universes need the opt-in. |
| `bigquery.auth.access_token_endpoint`, which receives the client secret and refresh token | HTTPS on `googleapis.com`, a subdomain, or `accounts.google.com`. |
| `bigquery.auth.external_account.token_url`, which receives the identity provider's subject token | HTTPS on `googleapis.com` or a subdomain. |
| `token_uri`, `token_url` and `service_account_impersonation_url` in a `bigquery.auth.credentials` document (string or file), including nested `source_credentials` | HTTPS on `googleapis.com` or a subdomain; `token_uri` also `accounts.google.com`. |

- With impersonation configured through `bigquery.impersonate.*`, a credential document's
  `universe_domain` must be `googleapis.com` too, because the source token goes to
  `iamcredentials.{universe_domain}`.
- Not checked: a document's `credential_source`, which names your identity provider (the subject
  token goes to `token_url`), and Application Default Credentials found through the environment or
  `gcloud`, which are the machine owner's configuration.
- Hosts are ASCII. Endpoint options and proxy variables accept only ASCII host names, and a URL in a
  credential document naming a non-ASCII host (an internationalized or fullwidth spelling) is
  refused; write an internationalized host in Punycode (`xn--`).
- Validation runs when clients are built, so options can be set in any order.
  `bigquery.allow_untrusted_endpoint` must be a database option; a URI cannot relax any check (G13).
- Two exposures remain: an application that lets its own users set `bigquery.endpoint` lets them
  receive its credentials, and an explicit `http://` endpoint sends a real token in cleartext.
- Driver-issued REST requests and the custom external-account identity-provider request refuse
  redirects. The bundled auth library still follows redirects during OAuth and STS exchanges, so a
  307/308 response can replay the credential-bearing body to another host
  ([UP-100](UPSTREAM.md#up-100-sdk-token-exchanges-follow-redirects)); endpoint checks cover only
  the initial URL of those exchanges.

### Table statistics

`GetStatistics` reports the standard `adbc.statistic.row_count` and the driver-defined statistics
below, which `GetStatisticNames` lists. Unreported statistics are omitted, never reported as zero.
Views and external tables have no statistics.

The row count is BigQuery's exact `numRows` as an `int64`. With a streaming buffer, the driver adds
BigQuery's estimate of the buffered rows and reports the sum as an approximate `float64`. A
materialized view's row count is also an approximate `float64`, since `numRows` counts what its last
refresh stored. The others are `int64` values:

| Name | Value | Reported as |
| --- | --- | --- |
| `bigquery.statistic.total_logical_bytes` | Total logical bytes. | approximate |
| `bigquery.statistic.total_physical_bytes` | Total physical (stored) bytes. | approximate |
| `bigquery.statistic.active_logical_bytes` | Logical bytes under active (short-term) storage pricing. | approximate |
| `bigquery.statistic.long_term_logical_bytes` | Logical bytes under long-term storage pricing. | approximate |
| `bigquery.statistic.time_travel_physical_bytes` | Physical bytes held by the time-travel window. | approximate |
| `bigquery.statistic.partition_count` | Number of partitions, which BigQuery updates with a delay. | approximate |
| `bigquery.statistic.last_modified_time` | Last modification, epoch milliseconds. | exact |

### Metadata catalogs

- A catalog filter naming a project makes `GetObjects` and `GetStatistics` enumerate that project,
  whether or not it is the connection's own. Project IDs cannot contain `%` or `_`, so the match is
  exact; a project that does not exist yields an empty result. A `NULL` or wildcard catalog filter
  reports only the connection's current catalog, although the [ADBC specification][adbc-spec]
  defines `NULL` as "do not filter": listing every visible project would need the Resource Manager
  API, which the driver does not use (G34, G75).
- `GetObjects` and `GetStatistics` include accessible hidden datasets (names starting with `_`) that
  match the filters. They skip a listed table or dataset that has since been dropped, or that
  BigQuery refuses with HTTP 403 `accessDenied` or no reason; other failures, and any failure of the
  dataset walk itself or of `GetTableSchema`, are reported (G62, G72).
- A table replaced between listing and describing is reported, and filtered, by its described type.
- `GetObjects` reports `REQUIRED` and `REPEATED` columns as non-nullable in `xdbc_nullable` and
  `xdbc_is_nullable`: a repeated column can be empty, but cannot contain a NULL array. Omitted modes
  default to nullable.
- Schema and table filters match case-sensitively. A filter without wildcards is looked up directly
  and reports the name BigQuery resolves it to, which differs from the filter only in a dataset
  created with `isCaseInsensitive`: there, `EVENTS` finds and reports `events` (G56, G74).

### Diagnostics

The driver writes diagnostics to standard error only if `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` is set when
the process creates its first driver, which the C API does at the first `AdbcDatabaseInit`. Values
are case-insensitive: `error`, `warn` (or `warning`), `info` and `debug`; an empty value disables
diagnostics, and any other value is reported once. Each event is one
`TIMESTAMP [bigquery-adbc] LEVEL message` line on file descriptor 2, with a UTC RFC 3339 timestamp
in milliseconds. Jupyter shows these lines in the kernel's console, not the cell.

```text
2026-11-05T14:23:45.123Z [bigquery-adbc] DEBUG reading BigQuery job project.US.job-1 through Storage Read
```

| Level | Events |
| --- | --- |
| `warn` | Storage Read fallbacks; failed `jobs.get` metadata reads; a failed background job cancel, naming the job that may keep running and billing; a script whose effect on the transaction could not be read; a failed session end; a transaction begun in a new session because BigQuery ended the previous one. |
| `debug` | The read path of a result that outgrows its first response, once per result and naming the job: further REST pages, or Storage Read. Request, poll and job-search retries, including replayed ingest appends and waits for a new ingest target to become visible to Storage Write; an unknown BigQuery table type reported as `TABLE`; a transaction session left for BigQuery to place because the default dataset's location could not be read. |

REST and Storage Read requests, including every row-stream open and resume, send
`User-Agent: bigquery-adbc/<version>`, followed by a space and the `bigquery.user_agent` text when
set, which Cloud Audit Logs record as `callerSuppliedUserAgent`. Storage Write requests cannot carry
it (UP-93).
