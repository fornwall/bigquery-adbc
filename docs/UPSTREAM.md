# Upstream issues

Issues in dependencies, services and test suites that affect this driver or need a local
workaround. An entry stays until the driver, or the client environments it supports, adopt an
upstream fix. Entries with no linked issue or PR are local findings. Differences from the Go
driver are in [GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md).

The findings apply to these revisions:

| Component | Revision | Relationship to this repository |
| --- | --- | --- |
| Google Cloud Rust | [`5a2297f`](https://github.com/googleapis/google-cloud-rust/commit/5a2297f47973775e925ce49d37971b03ce358fba) | Cargo pin; BigQuery 0.19.0, auth 1.18.0. |
| Apache Arrow ADBC | [`aa2c1c2`](https://github.com/apache/arrow-adbc/commit/aa2c1c2496d287e111d70d382e5d9af07568488b) | Rust, C++ validation and Python integration pins. Python packaging allows `adbc-driver-manager>=1.13,<2`. |
| Driver Foundry validation | [`5db1f44`](https://github.com/adbc-drivers/validation/commit/5db1f44782f4868c5ae9e89d6ac9812ca6e1758c) | Integration pin. |
| `h2` | 0.4.20 and [`e64c166`](https://github.com/hyperium/h2/commit/e64c1662ebb14794fc19cbcbb5b5dd0f67925f27) | Cargo uses 0.4.20; upstream `master` at this commit has UP-94. |
| dbt | [`b601ffe`](https://github.com/dbt-labs/dbt/commit/b601ffe469be8404e70b5e20f0ea0babc35ba6a8) | External client, not a Cargo dependency. |

Service findings come from observed failures; a driver test that normalizes or rejects the input
cannot show whether the service is fixed. Unmerged fixes are under
[Git snapshot candidates](#git-snapshot-candidates).

| ID | Subject | Upstream | Status | Issue or PR |
| --- | --- | --- | --- | --- |
| UP-19 | Day-time intervals corrupted on append | BigQuery Storage Write | Assigned; driver normalizes | [553218234](https://issuetracker.google.com/issues/553218234) |
| UP-20 | Duration arrays not accepted for `INTERVAL` | BigQuery Storage Write | Documented limitation; driver normalizes | None |
| UP-26 | Stale table lookup after recreation | BigQuery Storage Write | Assigned; driver retries | [553199047](https://issuetracker.google.com/issues/553199047) |
| UP-27 | Temporal inputs not exactly representable | Apache Arrow ADBC | Local patch | None |
| UP-28 | Temporal ingest subcases share one table | Apache Arrow ADBC | Local patch | None |
| UP-29 | Validation processes share namespaces | Apache Arrow ADBC | Local runner isolation and patch | [#4916](https://github.com/apache/arrow-adbc/pull/4916) (closed unmerged) |
| UP-30 | `RANGE<DATETIME>` needs an undocumented encoding | BigQuery Storage Write | Assigned; driver encodes | [553218235](https://issuetracker.google.com/issues/553218235) |
| UP-31 | Null arrays stored as empty cannot be declared | Apache Arrow ADBC | Local patch | [fornwall/arrow-adbc#43](https://github.com/fornwall/arrow-adbc/pull/43) (fork) |
| UP-38 | Approximate statistics assumed current | Driver Foundry | Local override | None |
| UP-41 | `StorageError` details dropped | `google-cloud-rust` | Open; driver checks schema first | [#6127](https://github.com/googleapis/google-cloud-rust/issues/6127) |
| UP-43 | No credential-recipient trust for endpoint overrides | `google-cloud-rust` | SDK limitation; driver validates endpoints | None |
| UP-46 | Option conversion errors echo rejected values | Apache Arrow ADBC (`adbc_core`) | Driver sanitizes errors | None |
| UP-56 | Generated clients link unused Storage RPCs | `google-cloud-bigquery` | SDK limitation; no workaround | None |
| UP-61 | Pending-stream appends have no timeout | `google-cloud-bigquery` | Open; fixed for default streams only | [#6781](https://github.com/googleapis/google-cloud-rust/issues/6781) |
| UP-66 | Pending-stream `AppendRows` runner never reconnects | `google-cloud-bigquery` | Open; driver replays | [#6851](https://github.com/googleapis/google-cloud-rust/issues/6851) |
| UP-68 | Unused query dependencies cannot be disabled | `google-cloud-bigquery` | SDK limitation | None |
| UP-69 | AWS credential formatting pulls in jiff | `google-cloud-auth` | SDK limitation; no workaround | None |
| UP-77 | Nested `persist_docs` paths dropped | `dbt-labs/dbt` | Open; fix PR pending | [#16487](https://github.com/dbt-labs/dbt/issues/16487), [#16488](https://github.com/dbt-labs/dbt/pull/16488) |
| UP-78 | Listed relations attributed to the requested schema | `dbt-labs/dbt` | Mainline mitigation; depends on client and driver | None |
| UP-79 | Unknown table type panics dbt | `dbt-labs/dbt` | Local finding; driver normalizes | None |
| UP-81 | `token_lifetime_seconds` silently ignored | `google-cloud-auth` | Local finding; no workaround | None |
| UP-82 | AWS `environment_id` never checked | `google-cloud-auth` | Local finding; no workaround | None |
| UP-83 | Storage Write control-plane RPCs not retried | `google-cloud-bigquery` | SDK limitation; driver retries | None |
| UP-84 | `adbc_read_partition` Ctrl-C cancels the statement | ADBC Python driver manager | Fix PR open | [#4814](https://github.com/apache/arrow-adbc/pull/4814) |
| UP-86 | Seed and copy job IDs missing from `run_results.json` | `dbt-labs/dbt` | Local finding | None |
| UP-87 | `adbc_clone` drops autocommit and the backend | ADBC Python driver manager | Local finding | None |
| UP-88 | A transient token failure is served from cache for 10 s | `google-cloud-auth` | SDK limitation; no workaround | None |
| UP-89 | Array decimal elements read at the full scale | BigQuery Storage Write | Local finding; driver rescales | None |
| UP-90 | `ARRAY<TIMESTAMP>` appends fail | BigQuery Storage Write | Local finding; driver refuses up front | None |
| UP-91 | Removed `persist_docs` policy tags are never cleared | `dbt-labs/dbt` | Local finding; no driver workaround | None |
| UP-92 | DB-API type objects answer `!=` with `True` | ADBC Python driver manager | Local finding; package wraps them | None |
| UP-93 | Storage Write appends take no request options | `google-cloud-bigquery` | SDK limitation; no workaround | None |
| UP-94 | Padded empty HTTP/2 DATA frames end the connection | `h2` | Open; candidate fix unmerged; gzip left off | [#961](https://github.com/hyperium/h2/issues/961), [#972](https://github.com/hyperium/h2/pull/972) |
| UP-95 | `adbc_ingest` keeps an earlier call's catalog and schema | ADBC Python driver manager | Local finding; documented workaround | None |
| UP-96 | Arrow appends refuse flexible column names | BigQuery Storage Write | Local finding; driver refuses up front | None |
| [UP-100](#up-100-sdk-token-exchanges-follow-redirects) | Token exchanges follow redirects | `google-cloud-auth` | Local finding; no SDK policy option | None |

## Git snapshot candidates

These changes are not adopted or integration-tested. A snapshot of
a PR branch is a fork revision; older PR branches may also omit changes
already in our pins, so porting their patches onto the current revision is preferable to a
wholesale downgrade.

| Issue | Snapshot | What adopting it would address |
| --- | --- | --- |
| UP-84 | [ADBC PR #4814, `3237467`](https://github.com/fornwall/arrow-adbc/commit/3237467b86536a008042beb2c64f696c2c6ba9b7) | Fixes the cancellation target for Python partition reads, including result consumption. Build/install `python/adbc_driver_manager`; changing the Rust ADBC pin alone does not update the installed Python manager. The PR is open. |
| UP-94 | [`h2` PR #972, `1050775`](https://github.com/Elrendio/h2/commit/1050775ac348f9ebe7e3e9d0c7e4875f7a2a1c82) | Replenishes the empty-frame budget with non-final payload frames, addressing the observed interleaved padding pattern without an application option. The PR is open; small payloads or payloads carried only in final frames can still exhaust the budget. Validate compressed BigQuery responses before enabling gzip. |
| UP-77 | [dbt PR #16488, `6ac48bb`](https://github.com/fornwall/dbt-core/commit/6ac48bb195f98583778bc82982714b22d81bd9f8) | Preserves dotted descriptions and policy-tag paths. Requires a rebuilt dbt client and a driver supporting nested updates, as this driver does. The PR is open; it does not fix removed policy tags (UP-91). |
| UP-78 | [dbt mainline mitigation, `7f68444`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b) or later dbt `main` | Requests literal dataset filters from supporting drivers. Mitigates the overmatching with this driver; the relation attribution code itself is unchanged. |
| UP-31 | [ADBC fork PR #43, `f11ad98`](https://github.com/fornwall/arrow-adbc/commit/f11ad988c6ae70fbed96bd8d8ce8ec68c6040024) | Adds `NullListForm` to C++ validation. Adopting it requires adapting the local quirks/patch; it replaces an existing workaround rather than fixing driver behavior. It is not an Apache upstream PR. |

No client snapshot fix was identified for the recorded BigQuery service failures.

## UP-19: BigQuery Storage Write corrupts Arrow day-time intervals

Storage Write misreads Arrow day-time interval arrays appended to `INTERVAL` columns, although its
Arrow type mapping lists them: 3 days and 14,706,008 ms are stored as `0-0 3 0:30:21.106112`. The
driver converts day-time arrays to month-day-nanosecond before serialization
(`src/types/cast.rs`).

## UP-20: BigQuery Storage Write does not accept Arrow duration arrays

The [Arrow type mapping](https://docs.cloud.google.com/bigquery/docs/supported-data-types#supported-apache-arrow-data-types)
lists no `Duration` unit for `INTERVAL`. The driver converts durations to month-day-nanosecond
intervals holding the whole span in the nanosecond component, truncating below microseconds and
rejecting spans over 2,562,047 hours (`src/ingest/normalize/encode.rs`).

## UP-26: Storage Write can retain a stale table lookup after recreation

After a table is dropped and recreated under the same name, `CreateWriteStream` or the first
append can return `NOT_FOUND` for longer than the driver's bounded retries (`pending_writer` in
`src/ingest/write/stream.rs`), even while the Tables API returns the new table.
Run the reproducer with
`scripts/run-one-live-test.sh bulk_ingests_after_rapid_table_recreation`.

## UP-27: ADBC validation cannot select exactly representable temporal inputs

[`TestSqlIngestTemporalType`](https://github.com/apache/arrow-adbc/blob/aa2c1c2496d287e111d70d382e5d9af07568488b/c/validation/adbc_validation_statement.cc#L370)
ingests `-42`, `0` and `42` in the selected unit, so nanosecond inputs cannot round-trip through
BigQuery's microsecond resolution. `validation-quirks.patch` adds `IngestTemporalValueMultiplier`,
which the adapter sets to 1,000 for nanosecond timestamps and durations.

## UP-28: ADBC temporal ingest subcases reuse one physical table

The temporal helper behind UP-27 drops and recreates `bulk_ingest` for every unit and timezone in
one GTest, which triggers UP-26. Reusing a table is valid, so no upstream fix is wanted;
`validation-quirks.patch` gives each subcase its own table name.

## UP-29: Isolated ADBC validation processes share database namespaces

GTests use fixed table names in the adapter's catalog and schema, and `MetadataGetObjectsTables`
assumes no table in any dataset matches `asdf%`. `tests/clients/adbc_validation.rs` creates fresh
datasets per GTest, and `validation-quirks.patch` scopes that test's metadata calls to the
configured namespace. PR #4916 was closed without merging; its namespace-scoping patch would not
replace the per-test dataset isolation.

## UP-30: `RANGE<DATETIME>` children need a civil-time encoding the Arrow mapping omits

BigQuery's Arrow type mapping describes `RANGE<DATETIME>` elements as `Timestamp(Microsecond)`, but
such values append and then fail at query time with a `civil_time` encoding error. The driver
encodes the `start` and `end` children in BigQuery's packed civil-time form
(`src/ingest/normalize/encode.rs`). Storage Write also checks the packed integer against the
`TIMESTAMP` range, so a bound after `3600-12-31 23:59:59.999999` is rejected as
`Invalid timestamp(s)`, with no workaround.

## UP-31: ADBC validation cannot describe null arrays stored as empty

ADBC's list ingest tests require null lists to read back as null, but BigQuery
[stores null arrays as empty](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#nulls_and_the_array_type).
`validation-quirks.patch` adds `ingest_null_lists_as_empty()`; the fork PR proposes a
`NullListForm` quirk instead.

## UP-38: Driver Foundry assumes approximate statistics are immediately current

[`test_get_statistics`](https://github.com/adbc-drivers/validation/blob/5db1f44782f4868c5ae9e89d6ac9812ca6e1758c/adbc_drivers_validation/tests/connection.py#L1188)
requires an approximate row count of at least three right after ingesting three rows, but
BigQuery's approximate metadata can report zero for over a minute. The override in
`integration/foundry-validation/tests/test_connection.py` accepts any nonnegative count.

## UP-41: Rust BigQuery Storage Write drops structured `StorageError` details

The gRPC status converter keeps `google.rpc.*` details but drops `StorageError`, so append errors
such as `SCHEMA_MISMATCH_EXTRA_FIELDS` cannot be classified as `AlreadyExists`. The driver checks
the table schema before writing (`src/ingest/schema.rs`), which misses a concurrent change.

## UP-43: Endpoint overrides do not enforce credential-recipient trust

The Storage clients' [host resolver](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/gax-internal/src/host.rs)
accepts any custom endpoint, including plain HTTP, and the transport sends credentials to it.
`DatabaseConfig::validate_endpoints` (`src/config/endpoints.rs`) restricts authenticated Storage
and other auxiliary service overrides to HTTPS `googleapis.com` hosts unless
`bigquery.allow_untrusted_endpoint=true`. A BigQuery REST endpoint set as an option is trusted
separately for Go/dbt profile compatibility; that does not relax the Storage checks.
[Impersonation by target principal](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/auth/src/credentials/impersonated.rs)
sends the source token to `iamcredentials.{universe_domain}` without the service clients'
universe check, so the driver also checks that field in credential documents.

## UP-46: Rust ADBC option conversions include rejected payloads in errors

`adbc_core`'s `bool` and `IngestMode`
[option conversions](https://github.com/apache/arrow-adbc/blob/aa2c1c2496d287e111d70d382e5d9af07568488b/rust/core/src/options.rs)
format the rejected value into their errors, which would expose a secret passed to the wrong
option. `src/options.rs` decodes them with payload-free errors instead.

## UP-56: Storage Write's generated client links the RPCs its own writer never calls

The [generated Storage clients](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/bigquery/src/write/generated/gapic_storage/client.rs)
hold a dynamic stub that links every RPC, including `FlushRows`, `SplitReadStream` and the
generated `AppendRows`, which the driver never calls
([size](BINARY-SIZE.md#upstream-only-weights)). `PendingWriter` and `Read::builder()` offer no way
around them.

## UP-61: Rust BigQuery's pending-stream appends still have no timeout

In `google-cloud-bigquery` 0.19.0, attempt timeouts and retry policies remain configurable only on
the default-stream `WriterBuilder`, so nothing bounds a pending-stream append. The driver bounds
finalize and commit calls, but a stalled append still requires cancellation.

## UP-66: The pending-stream `AppendRows` runner never reconnects

`run_stream_task`, which `PendingWriter` uses directly, exits after a stream-open failure or a
closed response stream. The driver reattaches to the same pending stream and replays appends at
their original offsets (`src/ingest/write/retry.rs`).

## UP-68: Rust BigQuery has no feature gating its unused query and generated surface

The [crate manifest](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/bigquery/Cargo.toml)
always builds the query modules and the `google-cloud-bigquery-v2` graph, though the driver uses
only Storage Read and Write. LTO keeps that code out of the artifact, but it is still compiled.

## UP-69: AWS credential formatting pulls in jiff

`google-cloud-auth` depends on `jiff` only to format two AWS SigV4 date strings in
[`aws_sourced.rs`](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/auth/src/credentials/external_account_sources/aws_sourced.rs).
It already depends on `time`, which could produce both and remove `jiff` and `jiff-core` from the
library ([size](BINARY-SIZE.md#yaml-and-authentication-dependencies)). Integration tests do not
exercise real AWS federation.

## UP-77: dbt-core 2.0 re-nests dotted `persist_docs` paths before sending them

[`update_columns_descriptions`](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-adapter/src/adapter/adapter_impl.rs#L2708)
in dbt-core re-nests dotted column paths to their top-level column, so nested descriptions and
policy tags are never sent, without a warning. The driver applies
dotted paths at any depth; `integration/dbt/run.py` fails once they arrive.

## UP-78: `list_relations_via_adbc` attributes every listed relation to the requested schema

Without the literal-filter option, dbt passes dataset names as ADBC `LIKE` patterns, so `_`
matches other datasets too, and `list_relations_via_adbc` files every match under the requested
dataset instead of its `db_schema_name`. The default driver behavior must retain ADBC wildcard
semantics. This driver's opt-in `bigquery.get_objects.disable_wildcards=true` makes the names
literal. [Upstream dbt commit `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables it best-effort on upstream `main`; the `2.0.11` release does not set it
([measurements](PERFORMANCE.md#repeated-dataset-discovery)).
The [adapter](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-adapter/src/engine/adbc.rs#L614)
skips this setup with dbt Cloud credentials and ignores option errors. Both the adapter and driver
must support it for the mitigation to apply; a source upgrade does not fix clients whose driver
rejects the option. The relation parser ignores the returned schema name.

## UP-79: an unrecognized BigQuery table type panics the dbt process

[`RelationType::from_adapter_type`](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-schemas/src/dbt_types.rs#L43)
in dbt-core panics on an unrecognized BigQuery table type, under an upstream `TODO` to
return an error. The driver reports any type outside the five that `GetTableTypes` advertises
as `TABLE` (`src/connection/table_metadata.rs`).

## UP-81: external-account `token_lifetime_seconds` is silently ignored

The [external-account configuration](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/auth/src/credentials/external_account.rs)
has no `service_account_impersonation.token_lifetime_seconds` field, so a credential document that
sets it gets the 3600-second default without a warning. This affects `json_credential_file` and
`json_credential_string`; the driver passes this field through without validating it.

## UP-82: the AWS credential source never checks `environment_id`

`external_account.rs` deserializes `credential_source.environment_id` but drops it when building
the AWS configuration consumed by `aws_sourced.rs`. It processes the document as `aws1` instead
of rejecting other versions. As with UP-81, this affects `type: external_account` JSON documents,
and the driver does not check the field.

## UP-83: the Storage Write retry policy never reaches the control plane

The crate's retry policy applies only to default-stream appends; `CreateWriteStream`,
`GetWriteStream`, `FinalizeWriteStream` and `BatchCommitWriteStreams` are sent without retries.
`control_plane_retry` (`src/ingest/write/retry.rs`) makes up to three attempts with 200 ms
doubling backoff on transport failures and transient statuses.

## UP-84: dbapi `adbc_read_partition` sends Ctrl-C to the statement, not the connection

`Cursor.adbc_read_partition` uses the statement's `cancel` as its SIGINT hook, so Ctrl-C gets
`INVALID_STATE` from this driver and the partition read keeps running. The driver cannot work
around it.

## UP-86: dbt-core 2.0 drops a seed's and a table copy's job ID

dbt-core's `load_dataframe` and `copy_table` discard the result batch whose `BIGQUERY:query_id`
carries the job ID, so seed and `copy_partitions` nodes have no `job_id` in `run_results.json`.
The driver already reports the ID ([DBT-CORE.md](DBT-CORE.md#result-metadata)).

## UP-87: dbapi `adbc_clone` drops autocommit and the backend

`Connection.adbc_clone` builds the clone with the `Connection` constructor's defaults, so a clone
of an `autocommit=True` connection has autocommit off and the default backend. Against this driver
the clone opens a transaction session at its first query and then refuses bulk ingest and
permanent-table DDL. Create a second connection with `dbapi.connect` instead of cloning.

## UP-88: a transient token failure is served from cache for 10 s

When a token fetch fails transiently with no valid token cached, the `TokenCache` in
`google-cloud-auth` hands that failure to every caller until its next attempt 10 seconds later, and
the default Application Default Credentials builder cannot be given a retry policy. The driver
reports an unavailable token service as `IO` and retries it, but its retries end within about 5
seconds, so it recovers only if the cache's next attempt falls inside that window and succeeds.

## UP-89: Storage Write reads array decimal elements at the type's full scale

Storage Write reads the elements of a `REPEATED` NUMERIC or BIGNUMERIC field at scale 9 or 38,
ignoring the scale their Arrow type declares: `0.01` appended as `Decimal128(10, 2)` into an
`ARRAY<NUMERIC(10, 2)>` column is stored as `0.000000001`. Scalar and STRUCT fields keep their
declared scale. The driver sends array elements as `Decimal128(38, 9)` or `Decimal256(76, 38)`
(`storage_write_schema` in `src/ingest/schema.rs`).

## UP-90: Storage Write fails every append to an `ARRAY<TIMESTAMP>` column

An append carrying a `REPEATED TIMESTAMP` field at any depth fails with `INTERNAL: Internal error
encountered`, even when every array is empty and whatever the element's unit or timezone, although
the Arrow mapping lists the type. Repeated DATE, DATETIME and TIME, `ARRAY<STRUCT<ts TIMESTAMP>>`,
and appends that omit an `ARRAY<TIMESTAMP>` column succeed. Because the failure arrives only after
about 130 s of retries, the driver refuses such an ingest with `NOT_IMPLEMENTED` before any request
(`reject_repeated_timestamps` in `src/ingest/schema.rs`).

## UP-91: dbt-core 2.0 never clears a removed `persist_docs` policy tag

`update_columns_descriptions` in dbt-core sends policy tags only for columns whose YAML lists at
least one tag name, and leaves every other column out of
`adbc.bigquery.table.update_columns_policy_tags`. A column whose tags were removed from its YAML
keeps them in BigQuery, where dbt Core's `update_columns` sends an empty list and clears them. The
driver changes only the columns the payload names, so it cannot tell a removed tag from an untagged
column.

## UP-92: the Python manager's DB-API type objects answer `!=` with `True`

`adbc_driver_manager.dbapi`'s `STRING`, `BINARY`, `NUMBER`, `DATETIME` and `ROWID` define `__eq__`
but inherit `frozenset.__ne__`, so `pyarrow.string() != STRING` is `True`, as is `!=` against any
other Arrow type. `bigquery_adbc.dbapi` wraps each type object with a consistent `__ne__`, and also
compares an extension type such as JSON's `arrow.json` by its storage type.

## UP-93: Storage Write appends take no request options

The writer's `AppendRows` transport (`write/stream.rs`) always sends `RequestOptions::default()`,
and neither `Write::builder()` nor a writer accepts options, so its requests cannot carry a user
agent. The driver's REST and Storage Read requests send `bigquery.user_agent`; its appends do not.

## UP-94: `h2` ends a connection after 100 padded empty DATA frames

`h2` 0.4.20 and upstream `master` count each non-final DATA frame whose payload is empty, padding
excluded, toward a fixed limit of 100 per connection (`MAX_RECV_EMPTY_DATA_FRAMES`). The 101st
such frame causes `GOAWAY ENHANCE_YOUR_CALM` and fails every request in flight on it.
Google's frontend sends such padded
frames in gzip responses that it produces slowly, such as large `jobs.query` answers, so two of
them can end the shared connection. REST requests therefore do not send `Accept-Encoding: gzip`
(`src/rest/client.rs`); uncompressed responses carry no padding-only frames.

Upstream issue #961 reports the same Google frontend failure, and open PR #972 proposes a
payload-replenished budget. Its snapshot is a candidate for removing this workaround after
validation; simply upgrading from 0.4.20 to current upstream `master` does not fix it.

## UP-95: `Cursor.adbc_ingest` keeps an earlier call's catalog and schema

`adbc_driver_manager.dbapi.Cursor.adbc_ingest` sets `adbc.ingest.target_catalog` and
`adbc.ingest.target_db_schema` only when `catalog_name` or `db_schema_name` is given, and statement
options persist, so a later call on the same cursor that omits them ingests into the earlier target.
Pass `catalog_name=""` and `db_schema_name=""`, which this driver reads as unset, or ingest through
a new cursor.

## UP-96: Storage Write's Arrow appends refuse flexible column names

BigQuery tables accept [flexible column names](https://docs.cloud.google.com/bigquery/docs/schemas#flexible-column-names)
such as `first name`, and Storage Write supports them for protobuf rows, but an Arrow append to a
table with one at any depth fails with `INTERNAL: BigQuery TableSchema failed to be initialized:
Illegal field name`, even when the append leaves that column out, and only after the append
retries. The driver accepts only `[A-Za-z_][A-Za-z0-9_]*` column names and refuses an append into
such a table with `NOT_IMPLEMENTED` before any row is sent (`unwritable_name` in
`src/ingest/schema.rs`).

## UP-100: SDK token exchanges follow redirects

The pinned auth library's [user OAuth refresh](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/auth/src/credentials/user_account.rs#L451-L470)
and [STS exchange](https://github.com/googleapis/google-cloud-rust/blob/5a2297f47973775e925ce49d37971b03ce358fba/src/auth/src/credentials/internal/sts_exchange.rs#L100-L128)
use reqwest's default redirect policy, with no credential-builder option to change it. HTTP 307
and 308 preserve the POST body, which contains a refresh token and client secret or a subject
token. The driver's initial endpoint allowlist therefore does not constrain every recipient of
these credentials. The SDK should disable redirects for token requests; the custom identity
provider in `src/config/external_account.rs` already does. No live Google redirect has been
observed.
