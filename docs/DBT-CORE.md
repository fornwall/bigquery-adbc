# Using this driver with dbt-core 2.0

[dbt-core 2.0](https://github.com/dbt-labs/dbt) is dbt Labs' Rust engine; the older Python engine
does not use ADBC. This driver implements the option aliases and operations dbt expects from
[`dbt-labs/bigquery-adbc`](https://github.com/dbt-labs/bigquery-adbc), called the fork here.
Read the [migration review](#migration-review) before switching.

dbt defines its keys in `crates/dbt-adbc/src/bigquery.rs`. See [REFERENCE.md](REFERENCE.md#options)
for semantics, [GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md) for differences inherited from Go
(`G<n>`), and [UPSTREAM.md](UPSTREAM.md) for dependency issues (`UP-<n>`).

| dbt feature | Driver path | Section |
| --- | --- | --- |
| Profile connection and authentication | `adbc.bigquery.sql.*` database aliases | [Database options](#database-options) |
| SQL models, tests, snapshots, hooks, `run_query` | Query jobs with dbt's labels and job timeout | [Statement options](#statement-options) |
| Adapter response (`rows_affected`, bytes, `job_id`) | Result schema metadata | [Result metadata](#result-metadata) |
| `list_relations`, `get_relation`, source freshness | `GetObjects`, `GetTableSchema` | [Metadata](#metadata) |
| Seeds | CSV load job | [CSV seeds](#csv-seeds) |
| `persist_docs` | Column schema and table description patches | [Column metadata](#column-metadata) |
| `copy_partitions` on `insert_overwrite` | Copy job | [Table copy](#table-copy) |
| `grant_access_to` | Dataset access-list patch | [Authorized views](#authorized-views) |
| `adapter.get_dataset_location` | `datasets.get`, on dbt builds that send the option | [Dataset location](#dataset-location) |
| Python models | Cloud Storage upload, then Dataproc or Vertex AI | [Python models](#python-models) |

## Installing

The simplest way to use this driver is the patched dbt from
[`tools/dbt-build`](../tools/dbt-build/README.md). Each GitHub release attaches one, which downloads
this driver on first use. The rest of this section adds the driver to a stock dbt.

dbt loads the library `adbc_driver_bigquery` through the ADBC driver manager with the default
`AdbcDriverInit` entry point and ADBC 1.1.0. Release builds download drivers from the dbt Labs CDN
into `~/.cache/com.getdbt/adbc/<arch>-<os>/` on Linux (honoring `XDG_CACHE_HOME`) or
`~/Library/Caches/com.getdbt/adbc/` on macOS, and verify a checksum only when downloading, so a
library already cached under the expected name is loaded as is.

dbt's default `bigquery` channel, called the canonical driver here, uses `dbt-labs/arrow-adbc`.
The opt-in `bigquery_foundry` channel uses the fork and matches this driver's options. Cache this
library under the Foundry filename your dbt build expects (`0.21.0.dev+dbt0.1.11` for dbt 2.0.11).
On x86-64 Linux:

```sh
cargo rustc --release --locked --lib --crate-type cdylib
dbt_driver_dir="${XDG_CACHE_HOME:-$HOME/.cache}/com.getdbt/adbc/x86_64-manylinux_2_17-linux-gnu"
mkdir -p "$dbt_driver_dir"
cp target/release/libadbc_driver_bigquery.so \
  "$dbt_driver_dir/libadbc_driver_bigquery_foundry-0.21.0.dev+dbt0.1.11.so"
DBT_ADBC_BIGQUERY_USE_FOUNDRY=1 dbt build
```

On aarch64 Linux the directory is `aarch64-manylinux_2_17-linux-gnu`. `dbt.log` reports
`BigQuery driver resolved: bigquery_foundry <version>` when dbt loads it. The
[integration runner](../integration/dbt/README.md) stages the driver the same way in a
project-local cache.

## Database options

Each `adbc.bigquery.sql.*` database key dbt sets is an alias of the `bigquery.*` option with the
same suffix, except `api_endpoint` (`bigquery.endpoint`) and `auth_credentials`
(`bigquery.auth.credentials`), and each `adbc.bigquery.sql.auth_type.*` value maps to the
`bigquery.auth_type` value of the same name.

| dbt profile `method` | Driver authentication type |
| --- | --- |
| `oauth` | Default credentials |
| `service-account` | JSON credential file |
| `service-account-json` | JSON credential string |
| `oauth-secrets` with `refresh_token` | User authentication with the profile's `token_uri` |
| `oauth-secrets` with `token` | Temporary access token |
| `external-oauth-wif` | External account with an OAuth identity-provider token |

dbt always sets `impersonate.scopes`. Without an impersonation target, these configure base
credentials, including ADC, keys and discrete external accounts, unless `bigquery.auth.scopes`
overrides them (G76). With a target, they configure its token; source scopes stay at their default
unless overridden. Token exchanges and direct HTTPS requests honor `HTTPS_PROXY` and `NO_PROXY`.
The Python-model endpoint overrides `bigquery.gcs_endpoint`, `bigquery.dataproc_endpoint` and
`bigquery.vertex_endpoint` have no dbt profile key.

With `impersonate.target_principal`, this driver uses the configured authentication as the source
identity, including a profile's refresh token, JSON credentials or temporary access token. The
fork does too from [commit `9499ba3`](https://github.com/dbt-labs/bigquery-adbc/commit/9499ba31235a85d11045422ab6037b5d26c9113b),
while the active Go reference driver silently uses ADC instead
([G90](GO-DRIVER-COMPARISON.md#g90-configured-credentials-are-never-silently-replaced-or-ignored)).
The fork keeps Go's static token with an explicit impersonation lifetime
([G89](GO-DRIVER-COMPARISON.md#g89-impersonated-credentials-refresh-even-with-an-explicit-lifetime)).

For `external-oauth-wif` (Workload Identity Federation, WIF), the driver POSTs the profile's
`token_endpoint.request_data` unchanged as a form body to `token_endpoint.request_url`, and
exchanges the response's `access_token` at Google's Security Token Service (STS) as a JWT subject
token for `workload_pool_provider_path`, optionally
impersonating `service_account_impersonation_url`. The
[`bigquery.auth.external_account.*` options](REFERENCE.md#database-options) change the token type
and STS endpoint; for other token sources, pass a complete external-account credential document as
`bigquery.auth.credentials`.

## Statement options

| dbt key (`adbc.bigquery.sql.query.*`) | Notes |
| --- | --- |
| `labels` | dbt sets `dbt_invocation_id` and query-comment labels. |
| `job_timeout` | Milliseconds, from `job_execution_timeout_seconds`. |
| `link_failed_job` | No effect; dbt sets it on SQL and copy statements. |
| `dry_run` | dbt never sets it; `--empty` is a zero-row SQL wrapper. |
| `use_storage_api_disabled_client` | `true` forces REST results. |
| `priority`, `max_bytes_billed` | Sent on SQL and copy statements by dbt builds from its main branch, not by releases through 2.0.11. `BATCH` runs each query as a job, which works without a profile `location` ([G48](GO-DRIVER-COMPARISON.md#g48-jobs-submitted-without-bigquerylocation)). |

These alias `bigquery.query.*`. dbt 2.0.11 also sends `bigquery.query.reservation` on queries and
copies; it has no alias. CSV loads support reservation and job timeout, but dbt's seed caller
forwards neither. `dbt.*` statement options never reach the driver.

## Result metadata

Result schemas carry `BIGQUERY:query_id`, the query job's ID or a jobless response's `queryId`,
which dbt reports as the adapter response's `query_id` and `job_id`. The empty result of a seed
load or table copy carries only `BIGQUERY:query_id`, set to its job ID, which dbt discards (UP-86
in [UPSTREAM.md](UPSTREAM.md)). A python model's result carries the resource name of the Dataproc
batch or Vertex AI job it ran, or the Dataproc cluster job's ID.

dbt reads the PascalCase aliases of the [statistics keys](REFERENCE.md#result-schema-metadata):
`BIGQUERY:Statistics:Query:TotalBytesProcessed` (falling back to
`BIGQUERY:Statistics:TotalBytesProcessed`), `...:Query:TotalBytesBilled`, `...:Query:SlotMillis`,
`...:Query:NumDMLAffectedRows` (DML only; becomes `rows_affected`) and `...:Query:StatementType`.
Without `StatementType` dbt reports `SUCCESS` and the batch row count, so the driver supplies it
for every result, including jobless SELECTs and DML that returns no rows. Of the
[field metadata](REFERENCE.md#supported-types), dbt parses `BIGQUERY:type`, `Type`, `Repeated` and
`Description`.

## Metadata

`list_relations` calls `GetObjects` at tables depth. It includes hidden datasets (G60) and supports
other projects (G75). Table types are `TABLE`, `VIEW`, `MATERIALIZED_VIEW`, `EXTERNAL` or `SNAPSHOT`;
dbt panics on other values (G73, UP-79). The fork's `bigquery.get_objects.skip_table_metadata`
(default `true` here) and `bigquery.get_objects.disable_wildcards` options are supported.

Without `disable_wildcards`, ADBC's dataset filter is a `LIKE` pattern and dbt passes names
unescaped, so `_` in `dbt_example` matches any character: each listing walks the project's
datasets (G74), and dbt files the tables of a matching `dbtXexample` under the requested schema
(UP-78). [Upstream dbt commit `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables the literal option best-effort on upstream `main`. dbt 2.0.11 does not set it, and the
canonical `0.21.0.dev+dbt0.21.19` and Foundry `0.21.0.dev+dbt0.1.5` libraries
[reject it](PERFORMANCE.md#repeated-dataset-discovery). With this driver, enabling it avoids the
extra dataset scan when listing tables in a populated dataset.

`get_relation` and source freshness use `GetTableSchema`. A missing or invalid table is
`NOT_FOUND`, whose message also contains `404 Not Found:` (and `notFound:` for failed query jobs)
for dbt builds that match text. The schema carries the table's configuration under the fork's
unnamespaced keys (G65): `Type` (always present, default `TABLE`), `Description`,
`TimePartitioning.*`, `RangePartitioning.*`, `RequirePartitionFilter`, `Clustering.Fields`,
`Labels`, `ResourceTags`, `ExpirationTime`, `EncryptionConfig.KMSKeyName`, `ViewQuery`,
`UseLegacySQL`, `ExternalDataConfig.*` and `MaterializedView.*`.
These lookups request `tables.get(view=BASIC)` with the schema and configuration fields, avoiding
the storage statistics that the fork's default metadata view also requests.

## Option-only operations

The statement operations in `src/dbt/` share these rules, except [dataset location](#dataset-location):

- `execute`, `execute_update` and `execute_partitions` run the operation; `execute_schema` is
  `INVALID_STATE`, as is any operation while `adbc.connection.autocommit` is disabled.
- A statement runs one operation; setting another, an ingest target or real SQL replaces it. The
  two table metadata updates pair only with each other and run columns first; pairing either with
  anything else is `INVALID_STATE`.
- The placeholder SQL dbt finalizes each statement with is discarded: nothing but comments, the
  literal `none` (authorized views and dataset location) or `SELECT 1` (python models), with an
  optional semicolon.
- Failures keep the service's status, such as `NOT_FOUND`, `UNAUTHORIZED`, `TIMEOUT` or
  `CANCELLED`, and messages name the operation and resource without echoing a response body or
  option value. Table metadata patches are retried as G82 describes.
- Seed loads and table copies report BigQuery's row count, metadata patches and authorized views
  report `0`, and python models report an unknown count (`-1`).

### CSV seeds

dbt loads a non-empty seed through `adbc.bigquery.sql.query.destination_table` and
`adbc.bigquery.ingest.csv_filepath`, `.csv_delimiter` and `.csv_schema` (an Arrow IPC schema). The
driver streams the file unmodified to a load job with that schema, `skipLeadingRows: 1`,
`allowQuotedNewlines: true` and no autodetection. Quoted multiline fields are supported, matching
the dbt-labs fork's [CSV newline fix](https://github.com/dbt-labs/bigquery-adbc/commit/e6b41b936b04f3b1f60a967049e48139d7fbac86).
This is separate from ADBC bulk ingestion (G2).

- `column_types` overrides arrive as `BIGQUERY:type` metadata and become the load schema exactly
  as declared: `numeric` stays parameter-free, while `numeric(20,4)`, `bignumeric(76,38)` and
  `string(10)` keep their parameters. Unknown types are `NOT_IMPLEMENTED`. Other columns take the
  type their Arrow type implies.
- BigQuery parses the CSV: fields map to columns by position, jagged rows are rejected, and quoted
  newlines are accepted (CRLF becomes LF). An unquoted empty field is null, and `NULL` is text.
- The dispositions default to `CREATE_IF_NEEDED` and `WRITE_EMPTY`; dbt drops the seed table
  first, so a failed load leaves no table. The file is never resent; an ambiguous submission is
  resolved by fetching the job.
- A seed path that is missing, unreadable or not a regular file is `INVALID_ARGUMENT`; Unix FIFOs
  are rejected without waiting for a writer. A dry run checks that the file opens and that the
  declared schema is a valid load schema, without reading
  the file or sending a request, and reports `-1` rows.
- Without `bigquery.location` the load runs in the destination dataset's location (G48), whose
  lookup needs `bigquery.datasets.get`.

### Column metadata

`adbc.bigquery.table.update_columns_description` and `.update_columns_policy_tags` implement
`persist_docs: {columns: true}` on the table in `bigquery.query.destination_table`. Each is a JSON
object keyed by column path; the driver patches the full schema, changing only the named fields, so
`REPEATED` modes survive ([dbt #13657](https://github.com/dbt-labs/dbt/issues/13657)). Dotted paths
such as `orders.line_items.sku` reach nested fields and match names case-insensitively, as BigQuery
compares them; policy tags apply to leaf columns only, and unknown paths are ignored.

`adbc.bigquery.table.update_description` implements `persist_docs: {relation: true}`. It patches
only the description, without an `If-Match` precondition, since a new materialized view's initial
refresh briefly makes its ETag stale. An empty value clears the option, so it cannot clear a
description.

### Table copy

`adbc.bigquery.copy_table.source`, `.destination` and `.write_disposition` run a copy job, which
dbt uses for `copy_partitions: true` on the static `insert_overwrite` strategy with
`WRITE_TRUNCATE`. Both tables take `[[project.]dataset.]table`, including partition decorators such
as `model$20240101`. The job carries the statement's labels, job timeout and reservation, defaults
to `WRITE_EMPTY`, and runs in the destination's location as a seed load does. A dry run is
`NOT_IMPLEMENTED`, because BigQuery rejects dry-run copy jobs.

### Authorized views

`adbc.bigquery.dataset.authorize_view_to_datasets` implements `grant_access_to`. Its value maps
each view to the datasets it may read, and is validated when set:

```json
{"analytics-project.marts.customer_view": [{"project": "analytics-project", "dataset": "raw"}]}
```

For each dataset, the driver adds the view to the access list unless it is listed, patching under
an `If-Match` ETag so parallel builds keep each other's entries. A lost race or an ambiguous
failure backs off, re-reads the list and, unless the view is now listed, patches again under the
fresh ETag; after 16 rounds a lost race is `INVALID_STATE`.

Use the canonical project ID in an authorized view's name. Like the fork, the driver compares
existing view references literally, while BigQuery rewrites numeric project numbers to IDs in
the stored access list. Repeated authorization using a numeric view project can therefore append
duplicate entries. Source-dataset project numbers do not cause this.

### Dataset location

`bigquery.dataset.get_location` reads one dataset's location with `datasets.get`. It runs no job, so
unlike an `INFORMATION_SCHEMA.SCHEMATA` query it avoids the per-user `INFORMATION_SCHEMA` rate limit
and finds datasets outside the job's region. Its value is validated when set and reads back as set:

```json
{"project": "analytics-project", "dataset": "marts"}
```

`execute` returns one non-nullable string column, `location`: one row, or no rows for BigQuery's
`Not found: Dataset ...` 404, which also answers an ID BigQuery could never hold. Every other failure
is an error, including the 404 for a missing project, which only its message distinguishes. Malformed
JSON and an empty `project` or `dataset` are `INVALID_ARGUMENT`, as is a value that cannot stay one
URL path segment.

As a read it runs with `adbc.connection.autocommit` disabled. `execute` reports the row count,
`execute_update` reports `-1` as a query does. `execute_schema` returns the schema without a request
and invalidates prior statement results. `execute_partitions` is `INVALID_STATE`.

dbt 2.0.11 does not send this option: it runs `adapter.get_dataset_location` as an
`INFORMATION_SCHEMA.SCHEMATA` query, which is ordinary SQL here. dbt's main branch sends it from
[commit `3f1c248`](https://github.com/dbt-labs/dbt/commit/3f1c2489b9787d4b680a6a67c643c6e00f12d119).

### Python models

dbt runs a python model as two executions: it uploads the compiled source through
`adbc.bigquery.write_gcs.*` to `gs://{gcs_bucket}/{schema}/{alias}.py`, then submits a job and
waits for it. `submission_method` (model config, else profile, else `serverless`) selects the job:

| `submission_method` | Job | Option family |
| --- | --- | --- |
| `serverless` | Dataproc Serverless batch | `adbc.bigquery.create_batch.*` |
| `cluster` | PySpark job on a Dataproc cluster | `adbc.bigquery.dataproc.submit_job.*` |
| `bigframes` | Vertex AI notebook execution job | `adbc.bigquery.notebook_execute_job.*` |

| dbt key | Driver option |
| --- | --- |
| `compute_region` (profile, required) | `adbc.bigquery.dataproc.compute_region`, `adbc.bigquery.notebook_execute_job.region` |
| `gcs_bucket` (profile, required) | `adbc.bigquery.write_gcs.bucket`, `adbc.bigquery.notebook_execute_job.gsc_bucket` |
| `execution_project` (profile, default `database`) | `adbc.bigquery.dataproc.project`, `adbc.bigquery.notebook_execute_job.project` |
| `dataproc_cluster_name` (model, else profile) | `adbc.bigquery.dataproc.submit_job.cluster_name` |
| `jar_file_uri` (model), `dataproc_batch` (profile) | `adbc.bigquery.create_batch.batch_yml` |
| `batch_id` (model, else a fresh UUID) | `adbc.bigquery.create_batch.batch_id`, also the notebook job's display name |
| `notebook_template_id` (model) | `adbc.bigquery.notebook_execute_job.template_id` |
| `timeout` (model), else `job_execution_timeout_seconds`, else 3600 s (`bigframes`) or 86400 s | `adbc.bigquery.dataproc.pooling_timeout`, in seconds |

The `pooling_timeout`, `gsc_path` and `gsc_bucket` spellings are dbt's. `pooling_timeout` is the
client-side deadline for submission, polling and retries; unset or `0` means no deadline.
Retry delays are clamped to the remaining time, and no new request starts after expiry. An
in-flight request may still finish after the deadline. Polls outlast ten minutes of consecutive
transient failures, as a BigQuery job wait does.

- A missing required key is `INVALID_STATE` naming it. The result's `BIGQUERY:query_id` names the
  Dataproc batch, cluster job or notebook job.
- A batch must reach `SUCCEEDED`, a cluster job `DONE` and a notebook job `SUCCEEDED`; any other
  terminal state fails with the service's message.
- `AdbcStatementCancel` or an expired `pooling_timeout` stops the wait and asks the service to stop
  the job, resending after a transient failure, and the error says whether that failed. A cancel
  before a submission is sent stops it; once sent, it waits for the answer, up to the 60 s request
  timeout, because only that names the job. Vertex AI cannot stop a notebook job once its create
  operation is done, and a failed poll also leaves the job running; such errors say so.
- Dataproc submissions carry a request ID, so one that failed transiently, or whose answer cannot
  be read, is resent without starting a second job. The Vertex AI create has no such ID, so only a
  rate-limit rejection resends it. A submission that ends without a verdict says the job may have
  started.
- The upload is resent after a transient failure: it replaces the object atomically with the same
  bytes.
- The notebook job runs on `template_id`, else the project's first `ONE_CLICK` runtime template,
  following empty pages until a template is found or the listing ends with `NOT_FOUND`. Malformed
  listings return `INVALID_DATA`. It runs as `adbc.bigquery.notebook_execute_job.service_account`
  (which dbt does not send), else
  `bigquery.impersonate.target_principal`, else the service account of a JSON key or a standard
  IAM impersonation URL in JSON credentials or discrete WIF options, else as the template
  decides. Custom impersonation URLs in JSON credentials remain usable but do not identify a
  notebook service account locally.
- The upload needs the `devstorage.read_write` scope, and Dataproc and Vertex AI need
  `cloud-platform`. Google credentials and dbt's default scopes include `cloud-platform`; a
  narrower `bigquery.auth.scopes` or `impersonate.scopes` makes these operations `UNAUTHORIZED`.

## Differences from the dbt-labs fork

| Area | This driver | dbt-labs fork |
| --- | --- | --- |
| Option-only operations | Service status; every execute entry point | `UNKNOWN` (`INTERNAL` for patches); `ExecuteQuery` only, discarding SQL |
| CSV seeds | `WRITE_EMPTY`; explicit schema; create disposition honored; row count and job ID | `WRITE_APPEND`; autodetection without a schema; `0` rows |
| Seed column types | Loaded as declared; unknown types rejected | Unknown types become `STRING`; parameters dropped |
| Column metadata | Nested paths applied | Nested paths silently ignored |
| Table descriptions | Unconditional patch; runs beside a column update | `If-Match` that a new materialized view can fail; dropped beside a column update |
| Table copy | Labels, job timeout, reservation, row count, job ID; dry run refused; selected by either table | None of these; a dry run copies; selected by the source only |
| Authorized views | Validated when set; granted in sorted order | Parsed at execution, empty names sent; random order |
| Dataset location | Validated when set; refused beside a table update; a response without a location is `INVALID_DATA` | Parsed at execution; dropped beside a table update; such a response is an empty row |
| Python models | Implemented; successful jobs tested only with mocks | Fail with `NOT_IMPLEMENTED`: dbt sends `pooling_timeout` as a string |
| Metadata listing | `skip_table_metadata` defaults `true`; other projects listable | Defaults `false`; own project only |
| WIF | Configurable token type and STS URL; bounded HTTPS request | Fixed protocol; WIF options ignored under other auth types |

The fork also treats an unset `pooling_timeout` as an expired deadline, never cancels remote work,
never checks a batch's final state, and creates a permanent `ONE_CLICK` runtime template when none
exists. With `disable_wildcards`, it reports a missing dataset as `NOT_FOUND` and an unreadable one
as `IO` (this driver: an empty list or `UNAUTHORIZED`), and keeps the filter's spelling where this
driver reports the names BigQuery resolves (G56).

## Migration review

Compared against fork [revision `9499ba3`](https://github.com/dbt-labs/bigquery-adbc/tree/9499ba31235a85d11045422ab6037b5d26c9113b)
and dbt [revision `301b305`](https://github.com/dbt-labs/dbt/tree/301b305e063fc305ff3e46cb9497c1f606e3b2fa)
(Foundry `0.21.0.dev+dbt0.1.10`). Live comparisons used dbt 2.0.9 and a library built from that fork;
`integration/dbt/run.py --compare` instead uses dbt's canonical driver.

Before switching, review seed type parameters, job-ID consumers, destination-query macros, custom
authentication endpoints, copy-job settings and IAM permissions. The sections below distinguish
observed behavior from unvalidated deployment risks.

### 1. Base credentials preserve dbt's requested scopes

Both drivers apply dbt's BigQuery, Cloud Platform, Drive and userinfo.email scopes to base
credentials ([scope precedence](#database-options)). Authorized-user tokens need prior consent;
see section 7. No live Drive table has been tested.

Sources: [dbt scopes](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-auth/src/bigquery/mod.rs#L314-L368),
[fork base scopes](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/connection.go#L1143-L1151).

### 2. Paginated results select Storage Read before delivering rows

Paginated results select Storage Read before delivery: later rows may exceed REST's 20 MiB limit,
and switching APIs mid-read could duplicate or omit rows. Complete results stay on REST; the fork
normally uses Storage Read. This affects `run_query` and previews, not CTAS payloads. Oversized
rows still fail with Storage Read disabled or unavailable; small paginated results pay its setup
cost ([measurements](PERFORMANCE.md#choosing-storage-read-for-every-paginated-result)).

The [live regression](../tests/bigquery/storage_read.rs) checks 2,001 rows in order, ending with a
24,000,123-byte row. Compare the [fork reader](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/record_reader.go#L155-L197).

### 3. Enforcing seed type parameters changes values and accepted input

The fork [drops precision, scale and length](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/bulk_ingest.go#L33-L43)
from `column_types`; this driver's [CSV loads](#csv-seeds) preserve them:

| Declared type and CSV value | Fork | This driver |
| --- | --- | --- |
| `numeric(5,2)`, `123.456` | Stores `123.456` | Stores `123.46` |
| `numeric(5,2)`, `1234.56` | Succeeds | Fails: out of range |
| `string(3)`, `abcdef` | Succeeds | Fails: too long |

Use unparameterized types where intended. dbt drops the old seed table before loading, so rejection
can leave it absent. `varchar` becomes `STRING` in the fork and fails here, though dbt's parser
may reject it first.

### 4. Adapter-response job IDs are not always durable job IDs

The fork always creates a job. This driver can return a jobless `queryId`, which
[dbt copies to both `query_id` and `job_id`](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-adapter/src/response.rs#L179-L213).
`jobs.get` rejects such IDs with HTTP 400 `Invalid job ID`. Labels and timeouts do not force job
creation, and `bigquery.query.job_creation_mode` is unsupported. Adapt job-ID consumers accordingly.

### 5. Custom REST endpoint profiles work unchanged

Both drivers trust an explicit REST `api_endpoint`, including HTTP(S) proxies. URI authorities,
Storage, auxiliary services and authentication endpoints have separate
[trust rules](REFERENCE.md#endpoint-overrides). No corporate proxy has been tested.

Sources: [dbt endpoint mapping](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-auth/src/bigquery/mod.rs#L146-L175),
[endpoint tests](../src/config/tests/trust.rs).

### 6. An explicit quota project overrides the environment

For `method: oauth`, `quota_project` overrides `GOOGLE_CLOUD_QUOTA_PROJECT` in both drivers;
when unset, normal ADC precedence applies. [Local tests](../src/config/tests/quota_project.rs)
cover precedence, not live billing or IAM permissions. Impersonation differs; see section 11.

### 7. User OAuth scope lists restrict refreshed tokens

For `oauth-secrets` refresh tokens, authorized-user JSON and ADC, both drivers send requested
scopes as the OAuth refresh field `scope`. Refresh can narrow the original consent grant, not
extend it. [The SDK probe](../scripts/check-oauth-scope-narrowing.rs) checks the returned token's
`cloud-platform` scope through Google's `tokeninfo` endpoint.

Sources: [fork refresh request](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/connection.go#L1638-L1645),
[SDK fix](https://github.com/googleapis/google-cloud-rust/pull/7283).

### 8. Project identifiers are validated by BigQuery

Both drivers accept numeric and domain-scoped projects. This driver checks resource-name
structure (G20) and splits table references at the final two dots. The fork's
[domain-scoped fix](https://github.com/dbt-labs/bigquery-adbc/commit/b13496dc92b5f46cbf73a534e8b1c5c590f339c4)
rejects extra dots after the domain's colon. Multi-label domains, destinations, copies, authorized
views and partition decorators have local coverage; the [numeric-project live test](../CONTRIBUTING.md#live-tests)
covers queries, destinations, schemas and copies.

The driver adopts a canonical project ID returned for a numeric request, retaining job-ID and
location checks and rejecting different named projects or project numbers. Invalid metadata
catalog filters return an empty listing; connection catalog assignments report the service error.
Both drivers share the [numeric-project authorized-view limitation](#authorized-views).

### 9. Custom authentication endpoints have narrower compatibility

REST endpoint parity does not extend to token exchanges:

| Profile | Compatibility constraint |
| --- | --- |
| `oauth-secrets` | Standard Google HTTPS token URLs work. A loopback HTTP proxy requires `bigquery.allow_untrusted_endpoint=true`, which dbt does not expose. The fork accepts HTTP, but its default OAuth TLS server name (`accounts.google.com`) can also prevent arbitrary HTTPS proxies. |
| `service-account-json` | `token_uri` must use HTTPS `googleapis.com` or `accounts.google.com`; dbt cannot opt in to others. The driver signs tokens locally, so removing an unused custom `token_uri` restores compatibility. Impersonation also requires `universe_domain=googleapis.com`. |
| `external-oauth-wif` | The discrete impersonation URL must have the standard IAM Credentials form, even with the trust override. The identity-provider request requires HTTPS unless explicitly opted into HTTP and refuses redirects; the fork has neither restriction. |

Statement options cannot override database authentication. Full JSON credentials follow the
[credential-document rules](REFERENCE.md#endpoint-overrides); dbt's WIF mapping uses discrete
options. Sources: [dbt mapping](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-auth/src/bigquery/mod.rs#L91-L129),
[fork WIF](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/connection.go#L1073-L1084),
[driver WIF client](../src/config/external_account.rs).

### 10. Explicit destination queries intentionally return metadata only

`adapter.execute(..., fetch=true, options={'adbc.bigquery.sql.query.destination_table': ...})`
writes the destination in both drivers. The fork also returns rows; this driver returns metadata
only. Query the destination separately. dbt consumes `fetch` without forwarding it; ordinary
materializations do not use this combination.

Audit custom macros for unsupported legacy-SQL options, `result_buffer_size` and Go's result-format
and job-creation controls ([inherited differences](GO-DRIVER-COMPARISON.md)). Sources:
[dbt execution options](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-adapter/src/adapter/mod.rs#L3866-L3886),
[fork reader](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/record_reader.go#L138-L154).

### 11. Conditional job-option and IAM changes need deployment validation

These source differences have no reproduced live failure:

- Copies enforce dbt's timeout and reservation; the fork ignores them. Seeds receive neither
  setting from dbt, even though the driver supports both on load jobs.
- Without `location`, seeds and copies need `bigquery.datasets.get` to resolve their destination;
  the fork lets BigQuery infer it. Supply `location` for roles lacking that permission.
- This driver applies `quota_project` to impersonated credentials; the fork drops it. The target
  may need [`serviceusage.services.use`](https://docs.cloud.google.com/service-usage/docs/access-control)
  on that project, and quota attribution may change.
- Missing service cost statistics stay absent; the fork emits zero. dbt handles absent keys, but
  custom artifact consumers may not (G31).
- Initial `jobs.query` cancellation waits for a job reference, up to its ten-second completion
  wait plus transport time; without a recovered reference, remote work cannot be cancelled.
  Local cancellation is immediate. The fork normally knows the job ID from `jobs.insert` (G49).

Sources: [dbt copies](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-adapter/src/adapter/adapter_impl.rs#L5193),
[dbt seeds](https://github.com/dbt-labs/dbt/blob/301b305e063fc305ff3e46cb9497c1f606e3b2fa/crates/dbt-adapter/src/adapter/adapter_impl.rs#L3701-L3725),
[fork copies](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/table_ops.go#L55-L68),
[fork impersonation](https://github.com/dbt-labs/bigquery-adbc/blob/9499ba31235a85d11045422ab6037b5d26c9113b/go/connection.go#L1107-L1140).

### Coverage and remaining validation

The dbt 2.0.9 comparisons used ADC in `example-project` (anonymized) and release libraries.
A 23-field query matched Arrow types and values for numeric and temporal limits, floating-point
edge cases, intervals, ranges, JSON, geography, binary, nested/null/empty composites and collation.
Authentication comparisons used mocks. Repeat the integration suite with the
[runner's project and dataset configuration](../integration/dbt/README.md):

```sh
uv run --directory integration/dbt --locked run.py
```

A view without a schema errors here and returns an empty schema in the fork (G39). Deleting its
source table did not reproduce that case: both drivers rebuilt the view through dbt.
Linux compatibility still needs validation: these wheels target `manylinux_2_28`, dbt's libraries
`manylinux_2_17`. dbt's static cache keeps the library loaded, as this runtime requires.

## Known gaps

- dbt re-nests dotted `persist_docs` paths, so nested descriptions never reach the driver (UP-77),
  and omits removed policy tags, leaving them on the column (UP-91).
- dbt discards seed and copy result metadata, including their job IDs (UP-86). These and the
  metadata-patch issues above affect both drivers.
- Successful Dataproc/Vertex jobs, WIF and driver-managed impersonation have mock coverage only.
  The live suite checks a Cloud Storage upload and a rejected Dataproc cluster submission.
  The fork rejects dbt's string-valued `pooling_timeout` before submission, so accepting it alone
  does not establish Python-model parity.
- Cancellation during a Python-model submission can leave an unnamed remote job running; find it
  by `batch_id` or in the console.
- Run dbt from the project directory: it passes relative seed paths, which the driver opens
  relative to its working directory.
