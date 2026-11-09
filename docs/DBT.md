# dbt 2

Try this driver with `dbt-bigquery-adbc`, a build of [dbt 2](https://github.com/dbt-labs/dbt)
patched to use it for BigQuery. It runs alongside your normal `dbt` installation and uses your
existing project and profiles. Other adapters are unchanged.

## For dbt users

### Installing

On Linux or macOS:

```sh
curl -fsSL https://raw.githubusercontent.com/fornwall/bigquery-adbc/main/tools/dbt-build/install.sh | sh -s -- --update
```

This installs the latest released `dbt-bigquery-adbc` into `~/.local/bin`, verifies its checksum,
and configures your shell's `PATH`. It preserves your normal `dbt` binary, aliases and drivers.
The first BigQuery connection downloads and verifies the matching driver.

The script follows [dbt's installer](https://public.cdn.getdbt.com/fs/install/install.sh): add
`--prefix /path/to/prefix` to change the location, or `--version 0.5.0` to select a bigquery-adbc
release. Rerun it to update, or use `dbt-bigquery-adbc system update`.
Windows users can extract the `dbt-bigquery-adbc-…-windows-x86_64.zip` from
[GitHub releases](https://github.com/fornwall/bigquery-adbc/releases).

### Trying it in a project

Run from your dbt project directory, using your existing BigQuery profile:

```sh
dbt-bigquery-adbc --version
dbt-bigquery-adbc debug
dbt-bigquery-adbc build
```

`--version` identifies the driver release and patched dbt commit. To compare with dbt's own
BigQuery driver in the same build:

```sh
BIGQUERY_ADBC_USE_DBT_DRIVER=1 dbt-bigquery-adbc build
```

Both drivers stay cached separately. `dbt.log` records the choice as `BigQuery driver resolved:`.
See [build details](../tools/dbt-build/README.md) for driver selection and packaging.

### Compatibility notes

SQL models, tests, snapshots, seeds, `persist_docs`, partition copies and authorized views are
supported. This remains experimental; review these differences before switching:

- Seed `column_types` retain precision, scale and length: `numeric(5,2)` rounds `123.456` to
  `123.46` and rejects `1234.56`; `string(3)` rejects `abcdef`. dbt drops the old seed table before
  loading, so a failed load can leave it absent. Run from the project directory for relative CSV
  paths to resolve.
- An adapter response's `job_id` can be a jobless query ID, which `jobs.get` cannot fetch. Custom
  macros that set a destination table must query it separately to fetch its rows.
- Without a profile `location`, seeds and copies need `bigquery.datasets.get` to resolve their
  destination. Copies honor the timeout and reservation dbt sends; dbt does not forward these
  settings to seeds.
- Custom authentication endpoints have [additional constraints](REFERENCE.md#endpoint-overrides).
  An impersonated identity also uses the configured `quota_project` and may need permission to
  consume services there.
- Successful Python-model jobs, WIF and driver-managed impersonation have
  [validation gaps](LIMITATIONS.md#validation-gaps). Python-model cancellation can leave remote
  work running if its submission result was lost.
- dbt itself drops nested column descriptions, removed policy tags and seed/copy job IDs
  ([UP-77, UP-91 and UP-86](UPSTREAM.md)); these gaps affect both drivers.

## For developers

[`tools/dbt-build`](../tools/dbt-build/README.md#building) pins dbt and builds the patched binary.
The [dbt integration suite](../integration/dbt/README.md) covers real projects and supports
`--compare` against dbt's canonical driver. It needs ADC and an explicit test project; offline
harness tests and CI do not validate live BigQuery behavior. Python-model success paths currently
have mock coverage only.

The compatibility contract follows
[`dbt-labs/bigquery-adbc`](https://github.com/dbt-labs/bigquery-adbc), dbt's Go fork. dbt defines
its options in `crates/dbt-adbc/src/bigquery.rs`; this driver's option-only operations live in
[`src/dbt/`](../src/dbt). General option semantics belong in [REFERENCE.md](REFERENCE.md), and
inherited Go differences in [GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md).

### Database options

`adbc.bigquery.sql.*` aliases `bigquery.*`, except `api_endpoint` maps to `bigquery.endpoint`
and `auth_credentials` to `bigquery.auth.credentials`. Authentication type values accept the
`adbc.bigquery.sql.auth_type.*` prefix. dbt's `impersonate.scopes` configures base credentials
when no impersonation target is set, unless `bigquery.auth.scopes` overrides it.

The profile methods `oauth`, `service-account`, `service-account-json`, `oauth-secrets` and
`external-oauth-wif` map to ADC, JSON file, JSON string, refresh/access token and discrete WIF
credentials respectively. WIF exchanges the identity provider's `access_token` as a JWT by default.

### Statement operations

Query aliases carry dbt's labels, timeout, priority and maximum bytes billed; reservation uses
`bigquery.query.reservation`. `link_failed_job` is accepted without effect. dbt's `--empty` is SQL
rewriting, not a driver dry run.

| dbt feature | Option family / behavior |
| --- | --- |
| CSV seeds | `adbc.bigquery.ingest.csv_*`; see [below](#csv-seeds). |
| Column descriptions and policy tags | `adbc.bigquery.table.update_columns_description` / `.update_columns_policy_tags`; JSON maps keyed by column path. |
| Table descriptions | `adbc.bigquery.table.update_description`; may run together with a column update. |
| Partition copies | `adbc.bigquery.copy_table.*`; source, destination and write disposition, including partition decorators. Dry runs are refused. |
| Authorized views | `adbc.bigquery.dataset.authorize_view_to_datasets`; see [below](#authorized-views). |
| Dataset location | `bigquery.dataset.get_location`; see [below](#dataset-location). |
| Python-model upload | `adbc.bigquery.write_gcs.*`. |
| Python-model execution | `adbc.bigquery.create_batch.*`, `.dataproc.submit_job.*` or `.notebook_execute_job.*` for Dataproc Serverless, clusters or Vertex AI. |

Mutating option-only operations require autocommit and replace ordinary SQL. The driver discards
dbt's placeholder SQL, preserves service error statuses, and rejects `execute_schema` for them.
Python models retain dbt's `pooling_timeout`, `gsc_path` and `gsc_bucket` spellings; the timeout is
in seconds and covers submission, polling and retries. Failed terminal job states are errors.

### CSV seeds

A non-empty seed uses `adbc.bigquery.sql.query.destination_table` and
`adbc.bigquery.ingest.csv_filepath`, `.csv_delimiter` and `.csv_schema` (Arrow IPC). The driver
streams the CSV to a load job with the supplied schema, one header row and quoted newlines enabled.
Defaults are `CREATE_IF_NEEDED` and `WRITE_EMPTY`; this path is separate from ADBC bulk ingestion.
Declared `BIGQUERY:type` metadata retains type parameters; unsupported types are rejected.

### Authorized views

The option maps each view to its source datasets, for example:
`{"project.marts.my_view": [{"project": "project", "dataset": "raw"}]}`.
The driver patches each dataset's access list under an ETag and retries conflicting updates.
Use canonical project IDs: numeric view project spellings can duplicate grants, as in the fork.

### Dataset location

`bigquery.dataset.get_location` takes `{"project": "project", "dataset": "marts"}` and calls
`datasets.get`. It returns one non-nullable string column, `location`, with one row, or no rows
for a missing dataset. Other failures remain errors. It permits manual-commit mode and schema-only
execution, but not partitioned execution.

### Result metadata

`BIGQUERY:query_id` becomes dbt's `query_id` and `job_id`. Seeds and copies return their job ID,
which dbt currently discards. Python-model results name the remote job.

The statistic aliases dbt reads are `BIGQUERY:Statistics:Query:TotalBytesProcessed` (with fallback
to `BIGQUERY:Statistics:TotalBytesProcessed`), `...:Query:TotalBytesBilled`, `...:Query:SlotMillis`,
`...:Query:NumDMLAffectedRows` and `...:Query:StatementType`. See the
[result metadata contract](REFERENCE.md#result-schema-metadata).

### Metadata

`GetObjects` supports the fork's `skip_table_metadata` (default `true` here) and
`disable_wildcards` options. The latter prevents `_` in dbt dataset names from acting as a pattern.
`GetTableSchema` supplies dbt's unnamespaced keys: `Type`, `Description`, `TimePartitioning.*`,
`RangePartitioning.*`, `RequirePartitionFilter`, `Clustering.Fields`, `Labels`, `ResourceTags`,
`ExpirationTime`, `EncryptionConfig.KMSKeyName`, `ViewQuery`, `UseLegacySQL`, `ExternalDataConfig.*`
and `MaterializedView.*`. dbt also reads field metadata `BIGQUERY:type`, `Type`, `Repeated` and
`Description`. Missing-table errors retain the text dbt matches (`404 Not Found:` / `notFound:`).
