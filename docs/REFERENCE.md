# Driver reference

Options, Arrow type mappings and driver behavior, for developers using this
[ADBC][adbc-spec] driver from C, Python or Rust. For installation and a first query, see the
[README](../README.md). `G<n>` and `UP-<n>` IDs point to entries in the
[Go driver comparison][go-comparison] and [UPSTREAM.md](UPSTREAM.md).

- [Rust API](#rust-api)
- [Connection URIs](#connection-uris)
- [Options](#options): [database](#database-options), [query and statement](#query-and-statement-options), [standard ADBC](#standard-adbc-options)
- [Supported types](#supported-types)
- [Behavior notes](#behavior-notes): [submission and cancellation](#query-submission-and-cancellation), [errors](#errors), [row counts](#row-counts), [bound parameters](#bound-parameters), [Storage Read](#reading-large-results-through-the-storage-read-api), [result metadata](#result-schema-metadata), [bulk ingest](#bulk-ingest), [endpoint overrides](#endpoint-overrides), [table statistics](#table-statistics), [metadata catalogs](#metadata-catalogs), [diagnostics](#diagnostics)
- [dbt-core 2.0](#dbt-core-20)

[adbc-spec]: https://arrow.apache.org/adbc/current/format/specification.html
[go-comparison]: GO-DRIVER-COMPARISON.md

## Rust API

The C ABI and the Python package are the supported interfaces. The Rust crate is available only
from Git, is unstable, and may change without notice.

Depend on this crate (library name `adbc_driver_bigquery`) and on `adbc_core`, which defines the
traits:

```toml
bigquery-adbc = { git = "https://github.com/fornwall/bigquery-adbc", default-features = false }
adbc_core = { git = "https://github.com/apache/arrow-adbc", rev = "aa2c1c2496d287e111d70d382e5d9af07568488b" }
```

`default-features = false` leaves out the `ffi` feature, which holds the C ABI, so a shared library
built on this crate does not export `AdbcDriverBigqueryInit` and `AdbcDriverInit`.

The `adbc_core` revision must match the pin in `Cargo.toml`; different revisions define
incompatible trait types. Copy all `arrow*` entries from this repository's
[`[patch.crates-io]` table](../Cargo.toml) into your workspace root `Cargo.toml` too. Cargo does
not apply a dependency's patches. The pinned Arrow snapshot includes offset-validation fixes
used by the Storage Read decoder; keep the Arrow crates on that same revision and commit your
workspace's `Cargo.lock`.

The crate enables the `release_max_level_off` features of `log` and `tracing`. Cargo unifies
features, so this disables release-build logging through those crates in any application that
depends on this one. The driver's own [diagnostics](#diagnostics) are unaffected.

### Example

[`examples/query.rs`](../examples/query.rs) opens a connection and reads Arrow batches, taking the
project and query as arguments. Credentials default to Application Default Credentials (ADC); run
`gcloud auth application-default login` once, or see [connection URIs](#connection-uris) and
[database options](#database-options). `execute()` returns a `RecordBatchReader` whose schema is
available before the first batch.

The Rust API is synchronous. From async code, use `spawn_blocking`, or a multi-threaded Tokio
runtime where the driver can use `block_in_place`. Calling it directly inside a current-thread
Tokio runtime panics. Inside a Tokio runtime, `cancel` sends the remote job cancellation in the
background and reports a failure only as a [diagnostic](#diagnostics). `BigQueryDatabaseOption`,
`BigQueryConnectionOption` and `BigQueryStatementOption` are typed forms of the option keys below
and convert into the `adbc_core` option types.

## Connection URIs

The URI syntax is the same as the Go BigQuery ADBC driver's:

```text
bigquery://[Host]:[Port]/ProjectID?OAuthType=...&DatasetId=...&Location=...
```

Host and port select an HTTPS REST endpoint, by default `bigquery.googleapis.com:443`. The
[endpoint rules](#endpoint-overrides) limit a URI with real credentials to Google API hosts unless
`bigquery.allow_untrusted_endpoint` is set as a database option. `OAuthType` defaults to `0`:

| `OAuthType` | Authentication | Required parameters |
| --- | --- | --- |
| `0` | Application Default Credentials | none |
| `1` | JSON credential file | `AuthCredentials` |
| `2` | JSON credential string | `AuthCredentials` |
| `3` | Discrete user OAuth | `AuthClientId`, `AuthClientSecret`, `AuthRefreshToken` |

The other parameters are `DatasetId`, `Location`, `QuotaProject`, `ImpersonateTargetPrincipal`,
`ImpersonateDelegates`, `ImpersonateScopes` and `ImpersonateLifetime`, with case-sensitive names.
Duplicate, unknown, malformed, incompatible and incomplete parameters, user information, a fragment
and more than one path segment return `INVALID_ARGUMENT`. Values are decoded like Go's
`url.ParseQuery`: percent-escapes once, and `+` as a space, so percent-encode credentials,
including any literal `+` in a base64 key. A percent-encoded NUL (`%00`) is rejected, because
option values are C strings.

Set the URI with the standard database option `uri` (`ADBC_OPTION_URI`). Setting it resets the
configuration to defaults before applying its values, and options set before initialization apply
after the URI whatever their order. Reading `uri` returns it with the `AuthCredentials`,
`AuthClientSecret` and `AuthRefreshToken` values replaced by `<redacted>`; read individual options
for the effective configuration.

## Options

| Namespace | Contents |
| --- | --- |
| `bigquery.*` | The driver's own option keys. Use these. |
| `adbc.bigquery.sql.*` | Go-compatible aliases for most `bigquery.*` keys and some values; `adbc.bigquery.sql.api_endpoint`, which [dbt-core 2.0](#dbt-core-20) sends, also aliases `bigquery.endpoint`. Getters return canonical values; errors name the key as the caller spelled it. |
| `adbc.bigquery.*` (no `sql`) | dbt-core 2.0 option-only operations with no `bigquery.*` equivalent; see [DBT-CORE.md](DBT-CORE.md). |

[Standard ADBC options](#standard-adbc-options) are also accepted. Use one spelling per option
during initialization, because driver managers can reorder options. Validation errors name the
option and the expected type or values, and repeat the supplied value only for endpoint hosts and
ports, since a value sent under the wrong key can be a credential.
That includes a credential file path, here and in `GOOGLE_APPLICATION_CREDENTIALS`: its errors give
the operating system's cause and the remedy instead. Optional string settings clear with `""`; JSON
object options also accept `{}`. Booleans and enums with defaults require a listed value, and integer options have
the reset rules listed below. Reading an unset option without a default returns `NOT_FOUND`,
never `""`.

Integer options accept integers or base-10 strings and are read with `GetOptionInt`:
`bigquery.query.prefetch_concurrency`, `bigquery.query.job_timeout`,
`bigquery.query.max_bytes_billed` and `adbc.bigquery.dataproc.pooling_timeout`. Boolean options
accept `true` and `false` in any case and read back in lowercase; `1`, `yes` and `on` are rejected.

Project identifiers are opaque values whose validity and existence BigQuery checks. Project
numbers and legacy domain-scoped IDs are accepted alongside modern project IDs. A supplied
identifier must remain one resource-name segment: empty values, `/`, `\`, ASCII control
characters, `.` and `..` are rejected, except that an empty option value clears that option as
described above. REST paths are percent-encoded and generated SQL quotes identifiers. Qualified
table options split `project.dataset.table` at its final two dots, preserving any dots in the
project. A job response may resolve a numeric project to its canonical ID; subsequent job
requests use that returned identity while retaining the job-ID and location checks.

These Go driver options return `NOT_IMPLEMENTED` with a reason: `bigquery.query.use_legacy_sql`,
`bigquery.query.allow_large_results`, `bigquery.query.disable_flattened_results`,
`bigquery.query.result_buffer_size`, `bigquery.query.job_creation_mode`,
`bigquery.query.results_format`, `bigquery.query.arrow_serialization_options.buffer_compression`
and `bigquery.bulk_ingest.compression` (G15, G28, G29 and G87 in the
[Go driver comparison][go-comparison]). Setting an option on the wrong object also returns
`NOT_IMPLEMENTED`, naming the object that takes it.

### Database options

| Option | Meaning |
| --- | --- |
| `bigquery.project_id` | Project that runs and is billed for jobs; empty clears it. |
| `bigquery.dataset_id` | Initial dataset for unqualified table names. |
| `bigquery.location` | Default location for every query, transaction session, and dbt-core 2.0 seed load and table copy. Without it, BigQuery infers each query's location; a manual-commit transaction's session, a seed load and a table copy run in the location of the default or destination dataset. Dataset locations are cached across the database's connections for up to 32 datasets; the oldest inserted entry is evicted and looked up again if needed. A query job whose submission answer is lost is searched for (G48 in the [Go driver comparison][go-comparison]). |
| `bigquery.auth_type` | `auth_bigquery` (default), `app_default_credentials`, `json_credential_file`, `json_credential_string`, `user_authentication`, `external_account`, `temporary_access_token`, or `anonymous`. The first two both use ADC. |
| `bigquery.auth.credentials` | JSON credential file path or document, as selected by `auth_type`. Write-only. File mode rejects JSON documents, control characters and paths over 4,096 bytes; use `json_credential_string` for inline JSON. Destinations in the document are subject to the [endpoint rules](#endpoint-overrides). |
| `bigquery.auth.credentials_type` | Optional check of the JSON credential type: `service_account`, `authorized_user`, `impersonated_service_account`, or `external_account`. |
| `bigquery.auth.client_id` | OAuth client ID; required by `user_authentication`. |
| `bigquery.auth.client_secret` | OAuth client secret; required by `user_authentication`. Write-only. |
| `bigquery.auth.refresh_token` | OAuth refresh token; required by `user_authentication`. Write-only. |
| `bigquery.auth.access_token` | Access token; required by `temporary_access_token`. Write-only. Never refreshed, so requests fail after it expires. |
| `bigquery.auth.access_token_endpoint` | Where `user_authentication` redeems the refresh token; default `https://oauth2.googleapis.com/token`. Subject to the [endpoint rules](#endpoint-overrides), which also admit `accounts.google.com` here. |
| `bigquery.auth.external_account.audience` | Required workload identity provider, such as `//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider`. Workforce pools need a complete credential document in `json_credential_string`. |
| `bigquery.auth.external_account.request_url` | Required HTTPS identity-provider endpoint for `external_account`. HTTP requires `allow_untrusted_endpoint=true`; redirects are rejected. Write-only, since a URL query can contain secrets. |
| `bigquery.auth.external_account.request_data` | Required form-encoded POST body for the identity provider, sent unchanged; the response's `access_token` is exchanged. Write-only. |
| `bigquery.auth.external_account.impersonation_url` | Optional `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<principal>:generateAccessToken` URL. Custom impersonation endpoints are rejected. |
| `bigquery.auth.external_account.subject_token_type` | `urn:ietf:params:oauth:token-type:jwt` (default), or `urn:ietf:params:oauth:token-type:access_token` for a provider that returns an opaque token. |
| `bigquery.auth.external_account.token_url` | Security Token Service (STS) endpoint; default `https://sts.googleapis.com/v1/token` with the credential's universe domain substituted. Subject to the [endpoint rules](#endpoint-overrides). |
| `bigquery.auth.quota_project` | Project ID or number sent as `x-goog-user-project`. An explicit value overrides `GOOGLE_CLOUD_QUOTA_PROJECT` with ADC; when unset, ADC keeps its normal environment and credential-file precedence. |
| `bigquery.auth.scopes` | Comma-separated base OAuth scopes. Without an impersonation layer, unset falls back to `bigquery.impersonate.scopes`, then the credential default (`https://www.googleapis.com/auth/cloud-platform` except that user credentials preserve their consent grant). User credentials can request a subset of their original consent grant; see [authentication scopes](#authentication-scopes). Rejected with `temporary_access_token` and `anonymous`, which mint no token. |
| `bigquery.impersonate.target_principal` | Target service account email; required when delegates or a lifetime is set. Scopes alone do not enable impersonation. Only ASCII letters, digits and `@ . - _` are allowed. |
| `bigquery.impersonate.delegates` | Comma-separated delegation chain; each entry follows the `target_principal` rule. |
| `bigquery.impersonate.scopes` | Comma-separated OAuth scopes for the impersonated token; also the base credential's fallback without an impersonation layer when `bigquery.auth.scopes` is unset. Discrete `external_account` credentials use this fallback even with an impersonation layer. Scopes alone do not enable impersonation; a temporary access token remains unchanged. |
| `bigquery.impersonate.lifetime` | Positive Go duration up to 12 hours; default one hour. Above one hour, IAM requires `constraints/iam.allowServiceAccountCredentialLifetimeExtension` for the target account, checked at first use. |
| `bigquery.endpoint` | REST endpoint override. Set as an option, it is trusted to receive the configured credentials on any host and scheme, unlike a URI's host ([endpoint rules](#endpoint-overrides)). A bare `host[:port]` uses HTTPS; use an explicit `http://` for an emulator or HTTP proxy. |
| `bigquery.storage_endpoint` | BigQuery Storage endpoint override, for both Read and Write. |
| `bigquery.gcs_endpoint` | Cloud Storage JSON API override for dbt python-model uploads; default `https://storage.googleapis.com`. |
| `bigquery.dataproc_endpoint` | Dataproc override for dbt python-model Spark submissions; default `https://{region}-dataproc.googleapis.com`. An override replaces the whole host, including the region. |
| `bigquery.vertex_endpoint` | Vertex AI override for dbt python-model notebook execution; default `https://{region}-aiplatform.googleapis.com`. An override replaces the whole host, including the region. |
| `bigquery.allow_untrusted_endpoint` | Boolean, default `false`. Allows a URI host, auxiliary service and credential-exchange endpoints, and credential-document destinations outside HTTPS Google API hosts. `bigquery.endpoint` set as an option needs no opt-in. Cannot be set through a URI. |
| `bigquery.user_agent` | Printable ASCII text, such as `my-app/1.0`, appended to the driver's [user agent](#diagnostics). |

A credential option that the selected `bigquery.auth_type` does not read returns
`INVALID_ARGUMENT` when a connection is created. On a connection, `bigquery.project_id` and
`bigquery.dataset_id` are read-only aliases of `adbc.connection.catalog` and
`adbc.connection.db_schema`.

#### Authentication scopes

External tables over Google Sheets or Drive need `https://www.googleapis.com/auth/drive` in
addition to the scopes for BigQuery. Service-account, impersonated and external-account
credentials request the configured scopes. User credentials need those scopes in the original
consent grant; refreshing a token cannot add consent.

Without an impersonation layer, `bigquery.auth.scopes` takes precedence over
`bigquery.impersonate.scopes`. The latter accepts dbt's `adbc.bigquery.sql.impersonate.scopes`
alias, so dbt's default Drive scope reaches service-account keys and service-account ADC.

For `user_authentication` and `authorized_user` JSON, including ADC that loads an authorized-user
file, `bigquery.auth.scopes` requests a subset of the original consent grant. The bundled SDK
sends the OAuth `scope` field; it cannot add scopes that the user has not consented to.
With an impersonation target, `bigquery.auth.scopes` configures the source credential;
when unset, the source keeps its default scopes so it can call IAM even if the target only needs
Drive. `bigquery.impersonate.scopes` configures the final token. The existing discrete
`external_account` fallback applies before this layer as well.

### Query and statement options

Options with scope "Connection, statement" can be set on either; a new statement copies the
connection's value. `bigquery.query.default_project_id` and `default_dataset_id` read back the
defaults in effect, including the connection's.

| Option | Scope | Behavior |
| --- | --- | --- |
| `bigquery.get_objects.skip_table_metadata` | Connection | Boolean, default `true`. At `GetObjects` tables depth, skips per-table describes and returns null columns and constraints. `false` fetches constraints; columns stay null. Column and all depths are unaffected. |
| `bigquery.get_objects.disable_wildcards` | Connection | Boolean, default `false`. `true` makes the `GetObjects` catalog, schema, table and column filters exact, case-sensitive names in which `%` and `_` match themselves, so names containing `_` get direct lookups ([metadata catalogs](#metadata-catalogs)). Unset filters still match everything; `GetStatistics` keeps pattern matching. |
| `bigquery.bulk_ingest.method` | Connection, statement | `storage_write` (default and only supported value). `load` returns `NOT_IMPLEMENTED`. |
| `bigquery.query.prefetch_concurrency` | Connection, statement | Positive integer, default `10`: the maximum REST row batches one reader converts concurrently. Fewer run once pending conversions retain 64 MiB of decoded input allocations, and conversion across the driver is bounded by available parallelism. Not used by Storage Read. |
| `bigquery.query.unordered_read` | Connection, statement | Boolean, default `false`. Lets Storage Read read several streams in parallel and return batches in arrival order. Rows are never lost or duplicated, but `ORDER BY` order is not kept. |
| `bigquery.query.use_storage_api_disabled_client` | Connection, statement | Boolean, default `false`. `true` forces REST results; see [Storage Read](#reading-large-results-through-the-storage-read-api). |
| `bigquery.query.default_project_id` | Statement | Project for the default dataset; empty restores the connection's current catalog. Clearing it returns `INVALID_STATE` while a statement default dataset is set and no connection catalog or database project can replace it. |
| `bigquery.query.default_dataset_id` | Statement | Dataset for unqualified names; empty restores the connection's current schema. A non-empty value needs a project from the statement, connection or database, else `INVALID_STATE`. |
| `bigquery.query.destination_table` | Statement | `table`, `dataset.table`, or `project.dataset.table`, completed from the statement and connection defaults; empty clears it. Execution writes the result there and returns its schema with no rows; partitions from `AdbcStatementExecutePartitions` read the table. |
| `bigquery.query.create_disposition` | Statement | `CREATE_IF_NEEDED` (default) or `CREATE_NEVER`. |
| `bigquery.query.write_disposition` | Statement | `WRITE_EMPTY` (default), `WRITE_APPEND`, or `WRITE_TRUNCATE`. |
| `bigquery.query.max_bytes_billed` | Statement | Integer maximum bytes billed. Zero clears the override; negative values are rejected. |
| `bigquery.query.job_timeout` | Statement | Integer timeout in milliseconds. Zero clears the override; negative values are rejected. A job BigQuery stops at the timeout fails with `TIMEOUT`, naming this option; a cancelled job stays `CANCELLED`. |
| `bigquery.query.reservation` | Statement | `projects/{project}/locations/{location}/reservations/{reservation}`, or `none` for on-demand billing where policy allows it; empty (default) clears the override. BigQuery validates the value. Query jobs and dbt-core 2.0 seed load and table copy jobs carry it. |
| `bigquery.query.labels` | Statement | A JSON object of string job labels, such as `{"team":"data"}`, for every query job. Other formats, including `key=value`, return `INVALID_ARGUMENT`. Reads back as canonical JSON sorted by key. |
| `bigquery.query.link_failed_job` | Statement | Boolean, accepted for Go driver and dbt-core 2.0 compatibility but ignored: a failed job's error always ends with its console link. |
| `bigquery.query.priority` | Statement | `INTERACTIVE` (default) or `BATCH`. |
| `bigquery.query.disable_query_cache` | Statement | Boolean; `true` disables the BigQuery query cache. |
| `bigquery.query.dry_run` | Statement | Boolean. Validates without executing and returns an empty stream with the inferred schema and `-1` rows affected; `execute_partitions` returns `INVALID_STATE`, since a dry run creates no job. Bound parameters are typed from their schema and stay bound. For ingest statements, see [bulk ingest](#bulk-ingest). |
| `bigquery.dataset.get_location` | Statement | JSON `{"project": ..., "dataset": ...}`. The statement reads that dataset's location with `datasets.get` instead of running SQL, returning a `location` column with one row, or none when the dataset is missing. See [dataset location](DBT-CORE.md#dataset-location). |
| `bigquery.query.create_session` | Statement | Boolean; asks BigQuery to create a session, independent of ADBC transaction sessions. Execution returns `INVALID_STATE` while `adbc.connection.autocommit` is `false`, because queries then run in the transaction's session. |
| `bigquery.query.parameter_mode` | Statement | `positional` (default) or `named`. `adbc.statement.bind_by_name` sets the same mode as a boolean; reading either key reports the current mode. |
| `bigquery.query.max_billing_tier` | Statement | Returns `NOT_IMPLEMENTED`, because Google deprecated the field. Use `bigquery.query.max_bytes_billed`. |

### Standard ADBC options

| Option | Scope | Behavior |
| --- | --- | --- |
| `adbc.connection.autocommit` | Connection | Boolean, default `true`. `false` selects manual commit; the BigQuery transaction session opens at the next query that needs one. A `COMMIT TRANSACTION` or `ROLLBACK TRANSACTION` in SQL, alone or in a script, ends the transaction, and the next statement starts a new one. Commit and rollback ignore cancellation for up to a minute, plus ten seconds to end the session after a failure. A commit keeps the transaction open only when BigQuery provably refused it, which `bigquery.job.verdict` shows; any other commit or rollback failure ends the session, or says that could not be done or confirmed, and a commit without a verdict returns `IO` and says its outcome is unknown. Bulk ingest and dbt-core 2.0 option-only operations other than `bigquery.dataset.get_location` are refused while it is `false`, and a statement that outlives a manual-mode connection returns `INVALID_STATE`. |
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
| `adbc.statement.exec.progress` | Statement | Read-only double: bytes processed so far, from the responses execution already receives, then the final count once BigQuery reports it. Adds no polling requests. With several bound parameter rows every row's job counts, except that `ExecuteQuery` of a statement that returns rows counts the first. |
| `adbc.statement.exec.max_progress` | Statement | Read-only double: the final processed-byte count once a terminal response reports it, otherwise `0` (adbc.h's "maximum not known"). A fresh statement, a dry run and an option-only operation report `0`. |

`GetParameterSchema` preserves the previous execution's progress, including when its dry run retries.

## Supported types

Every result column and `GetTableSchema` field carries its BigQuery type as a GoogleSQL type
literal in `BIGQUERY:type` field metadata. Bound parameters and bulk-ingest input accept the same
spelling.

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

`GEOGRAPHY` and `JSON` are the only types with an Arrow extension annotation. Both set
`ARROW:extension:name`; `GEOGRAPHY` also sets `ARROW:extension:metadata` to
`{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}`.

Declared shapes are kept in metadata rather than in the Arrow type, so `GetTableSchema` matches a
`SELECT *` of the table: a `STRING(L)` or `BYTES(L)` length in `BIGQUERY:max_length`, and a
`NUMERIC(P, S)` or `BIGNUMERIC(P, S)` column, which keeps its family's full Arrow type, in
`BIGQUERY:precision` and `BIGQUERY:scale`. For a repeated column these keys are on the list field.
`GetObjects` reports `P` and `S` as `xdbc_column_size` and `xdbc_decimal_digits`. Query results
never declare a decimal shape. Bulk ingest honors the keys on a decimal field, so a table's schema
ingested back creates the same column.

For Go driver consumers, fields also carry these keys when BigQuery reports the information.
Unreported keys are omitted, never written as `"0"` or `""`.

| Key | Present on | Value |
| --- | --- | --- |
| `Type` | Every field | The REST type spelling exactly as BigQuery reported it |
| `Repeated` | `REPEATED` fields | `true` |
| `Description` | Fields with a non-empty description | The description |
| `MaxLength` | Fields with a declared length | Same as `BIGQUERY:max_length` |
| `Collation` | Fields with a non-empty collation | The collation |
| `PolicyTags` | Columns with policy tags | The tag names as JSON, `{"names":[…]}` |

The `item` field inside a list carries none of these, only `BIGQUERY:type` and any Arrow extension
keys its type needs.

Conversion rules:

- `BIGQUERY:type` never uses the REST spellings `INTEGER`, `FLOAT`, `BOOLEAN`, `RECORD`, `DECIMAL`
  or `BIGDECIMAL`, which the driver accepts only from BigQuery itself. Caller-supplied metadata with
  a REST spelling, or contradicting the Arrow type it annotates, is rejected with an error naming
  the GoogleSQL type.
- Decimal parameters and ingest fields may be Arrow `Decimal32`, `Decimal64`, `Decimal128` or
  `Decimal256`. The driver picks the smallest BigQuery decimal type that holds the field's full
  precision and scale (`NUMERIC` allows 29 integer and 9 fractional digits, `BIGNUMERIC` 38 of
  each); compatible `BIGQUERY:type` metadata can select either explicitly. Negative Arrow scales
  are exact at scale zero. Values must fit their declared Arrow precision even when the destination
  is wider. An ingest field with explicit `BIGQUERY:precision` and `BIGQUERY:scale` accepts zero
  at any negative source scale. Unsupported shapes are rejected before any request is sent.
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

Ordinary queries use [`jobs.query`](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query)
with `JOB_CREATION_OPTIONAL` and `timeoutMs: 10000`, allowing short queries to return their
results in the submission response without a job. The ten-second completion wait matches
BigQuery's default and returns early when the query finishes. If BigQuery creates a job and the
response is incomplete, the driver registers its reference for cancellation and waits through
`jobs.getQueryResults`, whose requests wait up to 45 seconds. `AdbcStatementExecutePartitions`
requires job creation because its descriptors need durable job references.
These waits do not set an execution deadline; `bigquery.query.job_timeout` does. Dry runs and
options available only on a job use `jobs.insert`. Job creation mode and completion waits are
not configurable.

Waiting for a query, table copy or CSV load to finish outlasts ten minutes of consecutive retryable
failures: transport errors, timeouts, and retryable statuses such as 5xx. The window restarts
whenever BigQuery reports the job unfinished or returns its results, so failed reads alone never
end the wait for a running job. A wait that gives up fails with the last error, says the job's
outcome is unknown, and requests cancellation of the job if it is unfinished, so check the job
before running it again.

Cancellation stops local work and [requests cancellation](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel)
of any unfinished remote job. Verified job metadata updates its regional identity and completed
status; later metadata cannot make a completed job eligible for cancellation again.
BigQuery handles cancellation asynchronously. If it arrives while the
initial `jobs.query` request is pending, the driver keeps that submission alive in the background
and requests cancellation when its response supplies an unfinished job's reference. Local work
stops immediately, but remote cancellation can be delayed by the initial completion wait and
transport delays. A query that completes before its reference arrives cannot be stopped this way.
Retries after definitive submission rejections stop on cancellation; ambiguous submissions keep
recovering their job reference. If retries end without supplying one, the driver cannot cancel the
remote work. BigQuery may ignore a `requestId` on read-only queries, so resending a submission
does not guarantee recovery of the original execution. It also ignores one on `BEGIN`, `COMMIT` and
`ROLLBACK TRANSACTION`, so the driver submits its own with `jobs.insert`. One written in SQL still
uses `jobs.query`: if its answer is lost, the retry fails with "No active transaction found" and
says the statement may still have run, as any definitive answer to a retry does.

`jobs.insert` uses a driver-chosen job ID that stays the same across submission retries. If an
answer is lost and no location was configured, recovery first tries the location named by a
duplicate-job response, US/EU lookup, and known dataset locations. As a final fallback, it
[lists the caller's recent jobs](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list)
across regions to find the exact ID. This search includes five minutes before submission for
clock skew and makes up to three scans, with 100 pages in total and a shared 30-second deadline
for listing, credentials and the final job fetch. Listing one's own jobs needs no additional IAM
permission. Recovery registers a job found through a duplicate response or listing for cancellation
before fetching its details, even after local cancellation. If recovery fails, the error identifies
the job that may still be running. Setting `bigquery.location` avoids the cross-region search.

After a result stream reports EOF, an error or cancellation, it releases its inner reader,
including queued results and unread bound inputs. Its schema remains available until the stream
is released. An unpolled stream observes cancellation on its next read.

### Errors

Error messages say which step failed: `BigQuery rejected the query` for a submission BigQuery
refused, `BigQuery stopped the query` for one it stopped while answering the submission (such as
at the job timeout), and `failed to submit` for one that got no verdict. When the retries of a
`jobs.query` submission end that way, the message adds that BigQuery may still have run the
statement; a new submission would be a new execution, so check first. A job that fails after it
was accepted is named as `<project>:<location>.<id>`, and its message ends with the job's console
link. BigQuery's errors follow as `<reason>: <message>`. `AdbcErrorGetDetail` exposes these keys
when available:

| Detail key | Contents |
| --- | --- |
| `bigquery.error.reason` | BigQuery's machine-readable error reason |
| `bigquery.error.location` | BigQuery's error location |
| `bigquery.job_id` | The job ID |
| `bigquery.job.verdict` | What the failure proves about its job: `failed` when the job reported it, `not_created` when BigQuery refused the submission or never received it. A failure without it proves neither |
| `bigquery.error.http_status` | The HTTP status of a REST call |
| `bigquery.error.rpc_code` | The status code of a gRPC call |
| A protobuf type name, such as `google.rpc.ErrorInfo` | A structured `google.rpc.Status` detail as UTF-8 ProtoJSON, including `@type`. Repeated types keep the service's order; untyped details are skipped. |

ADBC 1.0 callers also receive the numeric status in `AdbcError::vendor_code`; ADBC 1.1 reserves
that field for its layout sentinel. Credential failures do not forward structured
`google.rpc.Status` details.

| Condition | Status |
| --- | --- |
| A service API not enabled in the project (`accessNotConfigured`, or a `SERVICE_DISABLED` `google.rpc.ErrorInfo`) | `INVALID_STATE`, keeping the service's message on how to enable it |
| Credentials that cannot mint an access token | `UNAUTHENTICATED`, with a remedy for the configured credential |
| A token service answering with a temporary failure, such as HTTP 503 | `IO`, retried like other transient failures |
| A transient token-exchange failure for explicit credentials or impersonation, including a network failure | `IO`; a missing ambient ADC metadata server still returns `UNAUTHENTICATED` with setup advice |
| A token service that accepts a connection and never answers, for any credential that mints tokens | `IO` after about two minutes; retries and later requests fail within seconds until a token arrives. A wait for a running job outlasts this like any other retryable failure |

`invalidQuery` and `accessDenied` set SQLSTATE `42000` (syntax error or access rule violation);
other failures leave SQLSTATE unset.

JSON decode errors report the response document, error category, and line and column without
including response values. They return `INVALID_DATA`.

Driver-side parameter and ingest validation errors describe the field, type or constraint, and
row where available. They do not include rejected cell values, including numeric and temporal
values. BigQuery's own error messages are forwarded as described above.

### Row counts

`AdbcStatementExecuteQuery` reports the following `rows_affected` values before the result stream
is read. The DB-API exposes the same value as `cursor.rowcount`; `-1` means unknown.

| Statement | `rows_affected` |
| --- | --- |
| Query, or a script whose last statement returns rows | BigQuery's `totalRows` for the result: the rows the stream will yield, `0` when empty |
| DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) | Rows affected, `0` included |
| DDL, a script whose last statement returns no rows (even DML, which BigQuery reports no count for in a script), a dry run | `-1` |
| A query with `bigquery.query.destination_table` | `-1`: BigQuery's `totalRows` counts the whole destination table |
| Bound parameters | One row: that row's count, as above. Several rows of DML, DDL or a destination-table query: every row runs before the call returns, and the counts sum (`-1` if any is unknown). Several rows of a query returning results: `-1`, since rows after the first run while the stream is read. No row: `0` |
| Bulk ingest | Rows written; `0` for a dry run |
| `bigquery.dataset.get_location` | The rows returned: `1`, or `0` for a missing dataset |

`AdbcStatementExecutePartitions` runs every job before it returns and reports the same counts,
summed across parameter rows (`-1` if any is unknown). `AdbcStatementExecuteQuery` with a `NULL`
`out`, like the Rust API's `execute_update`, reports the DML count, summed across parameter rows,
and `-1` for any other SQL, a query included.

### Bound parameters

Placeholders are positional (`?`) by default; with `bigquery.query.parameter_mode=named` (or
`adbc.statement.bind_by_name=true`) each Arrow field binds the `@name` it is named after,
case-insensitively. Each bound row runs as its own query job, and every row must yield the same
result schema. `ExecuteQuery` of a statement that returns rows, including a stored-procedure
`CALL`, runs the first row before it returns and each later row once the previous result is read,
so results arrive in row order. DML, DDL,
queries with an explicit destination table, and every other execute run all bound rows before
returning. Closing their empty result stream does not skip writes.

Consumed input batches are released before the next batch is requested. Converting a batch's
final row also releases the batch before that row's remote execution.

An execute consumes the binding, so executing again returns `INVALID_STATE` until the statement is
rebound. `ExecuteSchema` and dry runs read no row and keep the binding. On an unbound statement,
`ExecuteSchema` and `GetParameterSchema` let a dry run type the placeholders.

`Bind` and `BindStream` return `INVALID_ARGUMENT` for a schema nested more than 32 levels below the
batch, counting each child and dictionary as a level. BigQuery itself allows 15.

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

A value its parameter type cannot hold, such as a date after year 9999, returns `INVALID_DATA`
naming the parameter and row.

### Reading large results through the Storage Read API

A completed query job's results, a script's last statement's included, are read through the Storage
Read API whenever the first response has a continuation token, or at any size when BigQuery refuses
to return the first page because a row exceeds its 20 MiB response limit. A complete first response
stays on REST. A narrow first page cannot predict whether a later row exceeds REST's limit, so the
path is chosen before the schema or any rows are reported. Both paths return the same schema and
values.

Pending REST conversions have a separate 64 MiB input window, charged by their actual row and cell
vectors, nested values, and heap strings, including spare capacity. Once pending inputs reach the
window, further conversions wait; one batch can exceed it. The current response page and output
arrays under construction remain outside this input budget.

Buffered readers limit completed batches to a 64 MiB buffer-allocation budget and 200 batches.
Slices charge their retained allocation capacity, counted once per allocation within a batch;
allocations shared across separate batches are charged separately. One batch larger than 64 MiB
can proceed when the budget is empty. This is a completed-buffer budget, not a process memory
limit: response pages, conversions in progress, array metadata, and batches already returned to
the application are outside it.

Each Storage Read stream reserves its single wire-queue slot before receiving another SDK
response, so a stalled decoder retains at most one additional materialized response in that
stage. The response being decoded and the SDK's transport buffers remain outside that slot.

A `ReadRows` call silent for two minutes while the reader has room counts as a failed attempt: it
resumes at the next unread row, and after five such attempts in a row the read fails with `TIMEOUT`.
A reader that has stopped reading never trips this limit.

REST is used instead when BigQuery returns a complete result without creating a job, when the
`jobs.get` naming the job's result table fails, when the results were written to one partition of
a destination table (`table$20240101`), when the read session is empty or its Arrow schema cannot
be decoded or reconciled with the expected schema, or when
`CreateReadSession` fails with:

- `PERMISSION_DENIED`: the caller lacks `bigquery.readsessions.create`.
- `UNIMPLEMENTED`: an emulator, or an endpoint that does not serve the Storage API.
- `RESOURCE_EXHAUSTED`: the rate limit.
- `INVALID_ARGUMENT`: a table Storage Read refuses but REST reads, such as a destination table with
  `require_partition_filter`.

`UNAVAILABLE` and connection failures are retried for up to a minute; any other status is an error,
and a call unanswered after two minutes, even by a stalled TLS handshake, fails with `TIMEOUT`.
Fallback happens before the first batch reaches the caller, so no row is repeated, and is reported
only as a `warn` [diagnostic](#diagnostics). A result with a row
over 20 MiB needs Storage Read. If BigQuery refuses its first page, disabling Storage Read fails
with `INVALID_ARGUMENT`, and a refused Storage Read attempt preserves that failure's status,
vendor code, SQLSTATE and structured error details alongside the REST size-limit explanation.
When disabled or unavailable Storage Read leaves a result on REST, an oversized row on a later
page fails the stream; readers are never switched after rows have been delivered.

A `PERMISSION_DENIED` or `UNIMPLEMENTED` refusal, or a permission denial of that `jobs.get`, is
cached per billing project for the whole database. Setting
`bigquery.query.use_storage_api_disabled_client` to `false` on a connection clears the cache, for
example after the permission is granted; setting it on a statement does not. The option reads back
as configured, regardless of automatic fallback.

Read sessions use one stream unless `bigquery.query.unordered_read` is `true` (G26 in the
[Go driver comparison][go-comparison]). Reading a query's anonymous result table is free; a
partition of a query with `bigquery.query.destination_table` reads that permanent table, which is
billed at the data-extraction rate. A partition is read with the
`bigquery.query.prefetch_concurrency`, `unordered_read` and `use_storage_api_disabled_client`
values of the statement that created it, whichever connection reads it.

Partition descriptors are opaque: pass the bytes returned by `AdbcStatementExecutePartitions`
to `AdbcConnectionReadPartition` unchanged. Malformed descriptors return `INVALID_ARGUMENT`
before a request is sent, including job IDs that would change the URL path after normalization.

### Result-schema metadata

`SELECT` results require a schema even when they contain no rows. A `SELECT` with no available
result schema returns `INVALID_DATA`, including when a later job-metadata lookup identifies the
statement type.

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
`BIGQUERY:Statistics:Query:StatementType`, which dbt-core 2.0 reads
([list of aliases](DBT-CORE.md#result-metadata)). The public Rust constants name the lowercase
keys; `BIGQUERY_SCHEMA_METADATA_JOB_KEYS` lists both spellings, for comparing schemas across jobs.

Statistics come from the query's own responses and best-effort `jobs.get` requests when those omit
the statement type or costs, or Storage Read needs the result table. Successfully fetched metadata
is reused. If fetching fails, the keys only it would supply are absent and the statement still succeeds.

### Bulk ingest

Bulk ingest uses the Storage Write API. Batches are normalized in request-sized windows, counting
destination type widths, expanded dictionary values, overlapping list-view ranges and offsets for
empty or null values. Null fields resolved from the destination schema count at their resolved width.
Input failures stop ingestion without waiting for queued append responses. Failed or cancelled
ingests release queued payloads before bounded remote stream cleanup. An append unanswered for five
minutes is replayed on a new connection at its original offset, and a stream call unanswered for a
minute fails with `TIMEOUT`, so a connection that dies silently cannot hang an ingest.

| Mode | Table preparation and failure behavior |
| --- | --- |
| `create` | Requires a new table. If a later step fails, the created table is kept. A failure before commit reports that no rows were committed and suggests retrying with `append`. |
| `append` | Requires an existing compatible table. |
| `create_append` | Creates a missing table, or appends to an existing compatible one. |
| `replace` | With the same schema, uploads before truncating, so an upload failure or cancellation keeps the old rows; a failure after truncation can leave the table empty, and the error says so. With a different schema, recreates the table before uploading, so a later failure leaves the new, empty table, and the error says the old rows are gone. A cancel or lost response after a `TRUNCATE` or recreation is sent says the old rows may be gone, since BigQuery can finish such a statement. The `TRUNCATE` before the commit cannot be cancelled, and BigQuery stops it after ten minutes. |

Append matches columns by name, ignoring case and order. Omitted nullable columns become NULL, and
omitted repeated columns become empty arrays. `JSON` and `GEOGRAPHY` columns also accept plain
strings, which BigQuery parses as JSON text or WKT, and `STRING` columns accept `GEOGRAPHY` values
as WKT text. Arrow nullability need not match `REQUIRED`, but Storage Write rejects actual NULL
values in required columns.

An appended field adopts what the target column declares, and every value is checked against it:

- A decimal takes the column's type, precision and scale. A value with a fractional digit the
  column would round away, or too many integer digits, returns `INVALID_DATA` naming its row.
- A string or bytes field without `BIGQUERY:max_length` takes the column's length; a declared one
  must fit the column's. Unrelated Arrow extension annotations do not change this rule.
- A field of Arrow type `Null`, which pandas and Polars infer for a column holding only nulls,
  takes the column's type, unless the column is `REQUIRED`.

Column names must start with an ASCII letter or underscore and contain only ASCII letters, digits
and underscores. Storage Write cannot write BigQuery's flexible column names (such as
`first name`), so an append to a table that has one at any depth returns `NOT_IMPLEMENTED`;
`replace` recreates it (UP-96 in [UPSTREAM.md](UPSTREAM.md)).

Bulk ingest returns `INVALID_STATE` while `adbc.connection.autocommit` is `false`: a Storage Write
stream cannot join a session, and BigQuery does not allow DDL that creates or drops permanent
tables inside a transaction. The check runs before any request is sent or bound data is consumed.

NULL structs with nullable scalar children are supported, and their hidden child values are
ignored. Storage Write rejects NULL structs with `REQUIRED` or `REPEATED` descendants, even when
the hidden values are valid; the error names the required field or reports `Null lists are not
allowed for repeated fields`.

RANGE lower bounds must precede their upper bounds after conversion to microsecond precision.
Equal or reversed bounds return `INVALID_DATA`; NULL bounds remain unbounded.

Storage Write fails every append carrying an `ARRAY<TIMESTAMP>` field with `INTERNAL` (UP-90 in
[UPSTREAM.md](UPSTREAM.md)), so bound data with one, at any depth, returns `NOT_IMPLEMENTED`
naming the field before any request is sent, in every mode and in a dry run. An append that omits
an existing `ARRAY<TIMESTAMP>` column is not affected. To fill such a column, ingest the values as
`ARRAY<STRUCT<ts TIMESTAMP>>` into a staging table and convert them with SQL:

```sql
INSERT INTO target (id, ts)
SELECT id, ARRAY(SELECT item.ts FROM UNNEST(ts) AS item WITH OFFSET AS position ORDER BY position)
FROM staging
```

A `RANGE<DATETIME>` bound after `3600-12-31 23:59:59.999999` is rejected by Storage Write as an
invalid timestamp, with no driver workaround (UP-30).

With `bigquery.query.dry_run`, an ingest validates instead of writing: it resolves the target,
converts the bound Arrow schema, checks the target against the ingest mode with one read-only table
lookup, and reports zero rows. Row values are not validated, and the data stays bound, so clearing
the option and executing again ingests it (G30 in the [Go driver comparison][go-comparison]).

### Endpoint overrides

Endpoints receive the configured credentials. The rule is that a URI or a credential document
cannot send credentials somewhere new, but an explicit option can.

Setting `bigquery.endpoint` or either Go/dbt alias as an option trusts that BigQuery REST endpoint
on any host and scheme, including an HTTP proxy. No additional database option is required for
dbt's `api_endpoint`. Two exposures stay open: an application that lets its own users set
`bigquery.endpoint` lets them receive its credentials, and an explicit `http://` endpoint sends a
real token in cleartext.

A `bigquery://` URI's host must be HTTPS on `googleapis.com` or one of its subdomains, unless
authentication is `anonymous` or `bigquery.allow_untrusted_endpoint` is `true`. An explicit
`bigquery.endpoint` overrides the URI's host and is trusted as an option. An endpoint read back
and set again counts as explicit, so copy options between databases only from a trusted source.

Storage Read/Write, Cloud Storage, Dataproc and Vertex AI endpoints must use HTTPS on
`googleapis.com` or one of its subdomains, unless authentication is `anonymous` (for emulators)
or `bigquery.allow_untrusted_endpoint` is `true`. Other Google Cloud universes need that opt-in
for these services. A custom REST endpoint does not authorize a custom endpoint for another
service. Validation runs when clients are built, so options can be set in any order.

The same rule covers `bigquery.auth.access_token_endpoint`, which receives the client secret and
refresh token, and `bigquery.auth.external_account.token_url`, which receives the identity
provider's subject token. The access-token endpoint also trusts `accounts.google.com`; the
external-account endpoint does not.

A document in `bigquery.auth.credentials`, as a string or a file, follows the same rule in
`token_uri`, `token_url` and `service_account_impersonation_url`, including in nested
`source_credentials`. Only `token_uri` also trusts `accounts.google.com`. With impersonation
configured through `bigquery.impersonate.*`, `universe_domain` must be `googleapis.com` too, because
the source token goes to `iamcredentials.{universe_domain}`. `credential_source` is not checked: it
names your identity provider, and the subject token is sent to `token_url`. Application Default
Credentials found through the environment or `gcloud` are the machine owner's configuration and are
not checked.

`bigquery.allow_untrusted_endpoint` must be set as a database option; a URI cannot relax any of
these checks (G13 in the [Go driver comparison][go-comparison]).

Driver-issued REST requests and the custom external-account identity-provider request refuse
redirects. The bundled auth library still follows redirects during OAuth and STS exchanges;
a 307/308 response can replay the credential-bearing body to another host
([UP-100](UPSTREAM.md#up-100-sdk-token-exchanges-follow-redirects)). Endpoint checks cover only the
initial URL for those exchanges.

### Table statistics

`GetStatistics` reports the standard `adbc.statistic.row_count` and seven driver-defined
statistics per table, which `GetStatisticNames` lists. Unreported statistics are omitted, never
reported as zero.

The row count is BigQuery's exact `numRows` as an `int64`. If the table has a streaming buffer,
the driver adds BigQuery's estimate of the buffered rows and reports the sum as an approximate
`float64`. A materialized view's row count is also an approximate `float64`, since `numRows` counts
what its last refresh stored. The other statistics are `int64` values:

| Name | Value | Reported as |
| --- | --- | --- |
| `bigquery.statistic.total_logical_bytes` | Total logical bytes. | approximate |
| `bigquery.statistic.total_physical_bytes` | Total physical (stored) bytes. | approximate |
| `bigquery.statistic.active_logical_bytes` | Logical bytes under active (short-term) storage pricing. | approximate |
| `bigquery.statistic.long_term_logical_bytes` | Logical bytes under long-term storage pricing. | approximate |
| `bigquery.statistic.time_travel_physical_bytes` | Physical bytes held by the time-travel window. | approximate |
| `bigquery.statistic.partition_count` | Number of partitions, which BigQuery updates with a delay. | approximate |
| `bigquery.statistic.last_modified_time` | Last modification, epoch milliseconds. | exact |

Views and external tables have no statistics.

### Metadata catalogs

`GetTableSchema` reads schema and relation configuration through `tables.get(view=BASIC)`, with
a field selector for the configuration it reports. It includes partitioning, clustering, labels,
and view definitions without requesting storage statistics; `GetStatistics` requests
`view=STORAGE_STATS` separately.

A catalog filter that names a project makes `GetObjects` and `GetStatistics` enumerate that
project, whether or not it is the connection's own. Project IDs cannot contain `%` or `_`, so the
match is exact; a project that does not exist yields an empty result. A `NULL` or wildcard catalog
filter reports only the connection's current catalog, although the [ADBC specification][adbc-spec]
defines `NULL` as "do not filter": listing every visible project would need the Resource Manager
API, which the driver does not use (G34 and G75 in the [Go driver comparison][go-comparison]).

`GetObjects` and `GetStatistics` include accessible hidden datasets (names starting with `_`) that
match the filters. They skip a listed table or dataset that has since been dropped, or that
BigQuery refuses with HTTP 403 `accessDenied` or no reason; other failures, and any failure of
the dataset walk itself or of `GetTableSchema`, are reported (G62 and G72 in the
[Go driver comparison][go-comparison]).

When discovery describes a listed table, the returned type supersedes the listing's type and is
checked against the type filter again. This keeps its type, schema and statistics consistent if
the table was replaced between requests.

`GetObjects` reports `REQUIRED` and `REPEATED` columns as non-nullable in `xdbc_nullable` and
`xdbc_is_nullable`. A repeated column can be empty, but cannot contain a NULL array. Omitted modes
default to nullable.

Schema and table filters match case-sensitively. A filter without wildcards is looked up directly
and reports the name BigQuery resolves it to, which differs from the filter only in a dataset
created with `isCaseInsensitive`: there, `EVENTS` finds and reports `events` (G56 and G74 in the
[Go driver comparison][go-comparison]).

### Diagnostics

The driver writes diagnostics to standard error only if `ADBC_DRIVER_BIGQUERY_LOG_LEVEL` is set
when the process creates its first driver, which the C API does at the first `AdbcDatabaseInit`.
Values are case-insensitive: `error`, `warn` (or `warning`), `info` and `debug`; an empty value
disables diagnostics, and any other value is reported once. Each event is one
`TIMESTAMP [bigquery-adbc] LEVEL message` line on file descriptor 2. The timestamp is UTC in
RFC 3339 format with milliseconds, for example:

```text
2026-11-05T14:23:45.123Z [bigquery-adbc] DEBUG reading BigQuery job project.US.job-1 through Storage Read
```

Jupyter shows these lines in the kernel's console, not the cell.

| Level | Events |
| --- | --- |
| `warn` | Storage Read fallbacks; failed `jobs.get` metadata reads; a failed background job cancel, naming the job that may keep running and billing; a script whose effect on the transaction could not be read; a failed session end; a transaction begun in a new session because BigQuery ended the previous one. |
| `debug` | The read path of a result that outgrows its first response, once per result and naming the job: further REST pages, or Storage Read. Request, poll and job-search retries, including replayed ingest appends and waits for a new ingest target to become visible to Storage Write; an unknown BigQuery table type reported as `TABLE`; a transaction session left for BigQuery to place because the default dataset's location could not be read. |

REST requests and Storage Read requests, including every row-stream open and resume, send
`User-Agent: bigquery-adbc/<version>`, followed by a space and the `bigquery.user_agent` text when
set, which Cloud Audit Logs record as `callerSuppliedUserAgent`. Storage Write requests cannot
carry it (UP-93 in [UPSTREAM.md](UPSTREAM.md)).

## dbt-core 2.0

The driver can replace the dbt-labs Go driver fork that dbt-core 2.0, dbt Labs' Rust engine,
loads. [DBT-CORE.md](DBT-CORE.md) covers loading the driver, the `adbc.bigquery.*` option
aliases and option-only operations, and the known gaps.
