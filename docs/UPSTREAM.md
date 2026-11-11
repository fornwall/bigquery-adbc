# Upstream issues

Issues in dependencies, services and test suites that affect this driver or need a local
workaround. An entry stays until the driver, or the client environments it supports, adopt an
upstream fix; entries without an issue or PR are local findings. Go driver differences are in
[GO-DRIVER-COMPARISON.md](GO-DRIVER-COMPARISON.md) and dbt behavior in [DBT.md](DBT.md).

The findings apply to these revisions:

| Component | Revision | Relationship to this repository |
| --- | --- | --- |
| Google Cloud Rust | [`9ba3387`](https://github.com/googleapis/google-cloud-rust/commit/9ba338718561739ab257e87502516d4be42dd5d1) | Cargo pin; BigQuery 1.0.0, auth 1.18.0. |
| Apache Arrow ADBC | [`5565c52`](https://github.com/apache/arrow-adbc/commit/5565c52cb71006e3f6322c8821de6955775f631d) | Rust, C++ validation and Python integration pins. Python packaging allows `adbc-driver-manager>=1.13,<2`. |
| [Driver Foundry](https://github.com/adbc-drivers) validation | [`f063165`](https://github.com/adbc-drivers/validation/commit/f0631651f79bd01b67c5776be6fe3b7fbfbe083b) | Integration pin. |
| `h2` | 0.4.20 and [`e64c166`](https://github.com/hyperium/h2/commit/e64c1662ebb14794fc19cbcbb5b5dd0f67925f27) | Cargo uses 0.4.20; upstream `master` at this commit has UP-94. |
| dbt | [`b601ffe`](https://github.com/dbt-labs/dbt/commit/b601ffe469be8404e70b5e20f0ea0babc35ba6a8) | External client, not a Cargo dependency. |

Service findings come from observed failures; a driver test that normalizes or rejects the input
cannot show whether the service is fixed.

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
| UP-61 | Pending-stream appends have no timeout | `google-cloud-bigquery` | Open; fixed for default streams only; driver bounds the wait | [#6781](https://github.com/googleapis/google-cloud-rust/issues/6781) |
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
| UP-89 | Array decimal elements read at the full scale | BigQuery Storage Write | Service status unknown; driver rescales | None |
| UP-90 | `ARRAY<TIMESTAMP>` appends fail | BigQuery Storage Write | Service status unknown; driver refuses up front | None |
| UP-91 | Removed `persist_docs` policy tags are never cleared | `dbt-labs/dbt` | Local finding; no driver workaround | None |
| UP-92 | DB-API type objects answer `!=` with `True` | ADBC Python driver manager | Local finding; package wraps them | None |
| UP-93 | Storage Write exposes no user-agent configuration | `google-cloud-bigquery` | SDK limitation; no workaround | None |
| UP-94 | Padded empty HTTP/2 DATA frames end the connection | `h2` | Open; candidate fix unmerged; gzip left off | [#961](https://github.com/hyperium/h2/issues/961), [#972](https://github.com/hyperium/h2/pull/972) |
| UP-95 | `adbc_ingest` keeps an earlier call's catalog and schema | ADBC Python driver manager | Local finding; documented workaround | None |
| UP-96 | Arrow appends refuse flexible column names | BigQuery Storage Write | Service status unknown; driver refuses up front | None |
| UP-100 | Token exchanges follow redirects | `google-cloud-auth` | Local finding; no SDK policy option | None |
| UP-101 | Token exchanges have no timeout | `google-cloud-auth` | Local finding; driver bounds the caller's wait | None |
| UP-102 | gRPC server streams have no deadline or keepalive | `google-cloud-gax-internal` | Local finding; driver bounds `ReadRows` silence and Storage Write waits | None |

## Git snapshot candidates

These unmerged fixes are not adopted or integration-tested. PR branches may omit changes already
in our pins, so port their patches onto the current revision rather than downgrading wholesale. No
client snapshot fix is known for the BigQuery service failures.

| Issue | Snapshot | What adopting it would address |
| --- | --- | --- |
| UP-84 | [ADBC PR #4814, `3237467`](https://github.com/fornwall/arrow-adbc/commit/3237467b86536a008042beb2c64f696c2c6ba9b7) (open) | Cancellation target for Python partition reads, including result consumption. Build and install `python/adbc_driver_manager`; changing the Rust ADBC pin does not update it. |
| UP-94 | [`h2` PR #972, `1050775`](https://github.com/Elrendio/h2/commit/1050775ac348f9ebe7e3e9d0c7e4875f7a2a1c82) (open) | Replenishes the empty-frame budget with non-final payload frames, covering the observed interleaved padding; small payloads or payloads only in final frames can still exhaust it. Validate compressed BigQuery responses before enabling gzip. |
| UP-77 | [dbt PR #16488](https://github.com/dbt-labs/dbt/pull/16488) (open) | Preserves dotted description and policy-tag paths; needs a rebuilt dbt and a driver with nested updates, as this one has. Does not fix removed policy tags (UP-91). |
| UP-78 | [dbt `7f68444`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b) or later `main` | Requests literal dataset filters from supporting drivers, mitigating the overmatching with this driver; relation attribution itself is unchanged. |
| UP-31 | [ADBC fork PR #43, `f11ad98`](https://github.com/fornwall/arrow-adbc/commit/f11ad988c6ae70fbed96bd8d8ce8ec68c6040024) (not an Apache PR) | Adds `NullListForm` to C++ validation, replacing an existing workaround; requires adapting the local quirks patch. |

## UP-19: BigQuery Storage Write corrupts Arrow day-time intervals

Storage Write misreads Arrow day-time interval arrays appended to `INTERVAL` columns, although its
Arrow mapping lists them: 3 days and 14,706,008 ms are stored as `0-0 3 0:30:21.106112`. The driver
converts them to month-day-nanosecond before serialization (`src/types/cast.rs`).

## UP-20: BigQuery Storage Write does not accept Arrow duration arrays

The [Arrow type mapping](https://docs.cloud.google.com/bigquery/docs/supported-data-types#supported-apache-arrow-data-types)
lists no `Duration` unit for `INTERVAL`. The driver converts durations to month-day-nanosecond
intervals holding the whole span in nanoseconds, truncating below microseconds and rejecting spans
over 2,562,047 hours (`src/ingest/normalize/encode.rs`).

## UP-26: Storage Write can retain a stale table lookup after recreation

After a table is dropped and recreated under the same name, `CreateWriteStream` or the first append
can return `NOT_FOUND` for longer than the driver's bounded retries (`pending_writer` in
`src/ingest/write/stream.rs`), even while the Tables API returns the new table. Reproducer:
`scripts/run-one-live-test.sh bulk_ingests_after_rapid_table_recreation`.

## UP-27: ADBC validation cannot select exactly representable temporal inputs

[`TestSqlIngestTemporalType`](https://github.com/apache/arrow-adbc/blob/5565c52cb71006e3f6322c8821de6955775f631d/c/validation/adbc_validation_statement.cc#L370)
ingests `-42`, `0` and `42` in the selected unit, so nanosecond inputs cannot round-trip through
BigQuery's microseconds. `validation-quirks.patch` adds `IngestTemporalValueMultiplier`, which the
adapter sets to 1,000 for nanosecond timestamps and durations.

## UP-28: ADBC temporal ingest subcases reuse one physical table

The UP-27 helper drops and recreates `bulk_ingest` for every unit and timezone in one GTest, which
triggers UP-26. Reuse is valid, so no upstream fix is wanted; `validation-quirks.patch` gives each
subcase its own table.

## UP-29: Isolated ADBC validation processes share database namespaces

GTests use fixed table names in the adapter's catalog and schema, and `MetadataGetObjectsTables`
assumes no table in any dataset matches `asdf%`. `tests/clients/adbc_validation.rs` creates fresh
datasets per GTest, and `validation-quirks.patch` scopes that test's metadata calls to the
configured namespace. The closed PR's namespace scoping would not replace per-test isolation.

## UP-30: `RANGE<DATETIME>` children need a civil-time encoding the Arrow mapping omits

The Arrow mapping describes `RANGE<DATETIME>` elements as `Timestamp(Microsecond)`, but such values
append and then fail at query time with a `civil_time` encoding error, so the driver encodes
`start` and `end` in BigQuery's packed civil-time form (`src/ingest/normalize/encode.rs`). Storage
Write also checks the packed integer against the `TIMESTAMP` range, so a bound after
`3600-12-31 23:59:59.999999` is rejected as `Invalid timestamp(s)`, with no workaround.

## UP-31: ADBC validation cannot describe null arrays stored as empty

ADBC's list ingest tests require null lists to read back as null, but BigQuery
[stores null arrays as empty](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#nulls_and_the_array_type).
`validation-quirks.patch` adds `ingest_null_lists_as_empty()`; the fork PR proposes a
`NullListForm` quirk instead.

## UP-38: Driver Foundry assumes approximate statistics are immediately current

[`test_get_statistics`](https://github.com/adbc-drivers/validation/blob/f0631651f79bd01b67c5776be6fe3b7fbfbe083b/adbc_drivers_validation/tests/connection.py#L1188)
requires an approximate row count of at least three right after ingesting three rows, but
BigQuery's approximate metadata can report zero for over a minute. The override in
`integration/foundry-validation/tests/test_connection.py` accepts any nonnegative count.

## UP-41: Rust BigQuery Storage Write drops structured `StorageError` details

The gRPC status converter keeps `google.rpc.*` details but drops `StorageError`, so append errors
such as `SCHEMA_MISMATCH_EXTRA_FIELDS` cannot be classified as `AlreadyExists`. The driver checks
the table schema before writing (`src/ingest/schema.rs`), which misses a concurrent change.

## UP-43: Endpoint overrides do not enforce credential-recipient trust

The Storage clients' [host resolver](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/gax-internal/src/host.rs)
accepts any custom endpoint, including plain HTTP, and sends credentials to it, so
`DatabaseConfig::validate_endpoints` (`src/config.rs`) restricts authenticated Storage
and auxiliary overrides to HTTPS `googleapis.com` hosts unless
`bigquery.allow_untrusted_endpoint=true`; a REST endpoint set as an option is trusted separately
for Go/dbt compatibility. [Impersonation](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/auth/src/credentials/impersonated.rs)
sends the source token to `iamcredentials.{universe_domain}` without the service clients' universe
check, so the driver also checks that field in credential documents.

## UP-46: Rust ADBC option conversions include rejected payloads in errors

`adbc_core`'s `bool` and `IngestMode`
[option conversions](https://github.com/apache/arrow-adbc/blob/5565c52cb71006e3f6322c8821de6955775f631d/rust/core/src/options.rs)
format the rejected value into their errors, exposing a secret passed to the wrong option.
`src/options.rs` decodes them with payload-free errors instead.

## UP-56: Storage Write's generated client links the RPCs its own writer never calls

The [generated Storage clients](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/bigquery/src/write/generated/gapic_storage/client.rs)
hold a dynamic stub that links every RPC, including `FlushRows`, `SplitReadStream` and the
generated `AppendRows`, which the driver never calls ([size](BINARY-SIZE.md#upstream-only-weights)).
`PendingWriter` and `Read::builder()` offer no way around them.

## UP-61: Rust BigQuery's pending-stream appends still have no timeout

In `google-cloud-bigquery` 1.0.0, attempt timeouts and retry policies are configurable only on the
default-stream `WriterBuilder`, so nothing upstream bounds a pending-stream append. The driver
bounds finalize and commit calls, and treats an append unanswered for five minutes as dead,
reconnecting and replaying it at its offset.

## UP-66: The pending-stream `AppendRows` runner never reconnects

`run_stream_task`, which `PendingWriter` uses directly, exits after a stream-open failure or a
closed response stream. The driver reattaches to the same pending stream and replays appends at
their original offsets (`src/ingest/write/retry.rs`).

## UP-68: Rust BigQuery has no feature gating its unused query and generated surface

The [crate manifest](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/bigquery/Cargo.toml)
always builds the query modules, the `google-cloud-bigquery-v2` graph and `arrow` with IPC
compression (including zstd's C library), though the driver uses only Storage Read and Write. LTO
keeps that code out of the artifact, but it is still compiled.

## UP-69: AWS credential formatting pulls in jiff

`google-cloud-auth` depends on `jiff` only to format two AWS SigV4 date strings in
[`aws_sourced.rs`](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/auth/src/credentials/external_account_sources/aws_sourced.rs).
Its existing `time` dependency could produce both, removing `jiff` and `jiff-core`
([size](BINARY-SIZE.md#yaml-and-authentication-dependencies)). Integration tests do not exercise
real AWS federation.

## UP-77: dbt 2.0 re-nests dotted `persist_docs` paths before sending them

[`update_columns_descriptions`](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-adapter/src/adapter/adapter_impl.rs#L2708)
re-nests dotted column paths to their top-level column, so nested descriptions and policy tags are
silently never sent. The driver applies dotted paths at any depth; `integration/dbt/run.py` fails
once they arrive.

## UP-78: `list_relations_via_adbc` attributes every listed relation to the requested schema

Without the literal-filter option, dbt passes dataset names as ADBC `LIKE` patterns, so `_` matches
other datasets, and `list_relations_via_adbc` files every match under the requested dataset instead
of its `db_schema_name`; the relation parser ignores the returned schema name. The driver's default
must keep ADBC wildcard semantics, so its opt-in `bigquery.get_objects.disable_wildcards=true` makes
names literal. [dbt `7f6844449`](https://github.com/dbt-labs/dbt/commit/7f6844449b13b920dc9ea4da49806d1d835f718b)
enables it best-effort on `main` (not in `2.0.11`;
[measurements](PERFORMANCE.md#repeated-dataset-discovery)), but the
[adapter](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-adapter/src/engine/adbc.rs#L614)
skips it with dbt Cloud credentials and ignores option errors, so it applies only when both the
adapter and the driver support it.

## UP-79: an unrecognized BigQuery table type panics the dbt process

[`RelationType::from_adapter_type`](https://github.com/dbt-labs/dbt/blob/b601ffe469be8404e70b5e20f0ea0babc35ba6a8/crates/dbt-schemas/src/dbt_types.rs#L43)
panics on an unrecognized BigQuery table type, under an upstream `TODO` to return an error. The
driver reports any type outside the five `GetTableTypes` advertises as `TABLE`
(`src/connection/table_metadata.rs`).

## UP-81: external-account `token_lifetime_seconds` is silently ignored

The [external-account configuration](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/auth/src/credentials/external_account.rs)
has no `service_account_impersonation.token_lifetime_seconds` field, so a credential document that
sets it silently gets the 3600-second default. This affects `json_credential_file` and
`json_credential_string`; the driver passes the field through unvalidated.

## UP-82: the AWS credential source never checks `environment_id`

`external_account.rs` deserializes `credential_source.environment_id` but drops it when building
the AWS configuration for `aws_sourced.rs`, processing the document as `aws1` instead of rejecting
other versions. As with UP-81, this affects `type: external_account` JSON documents, and the driver
does not check the field.

## UP-83: the Storage Write retry policy never reaches the control plane

The crate's retry policy applies only to default-stream appends. `CreateWriteStream`,
`GetWriteStream` and `FinalizeWriteStream` get gax's default policy, which, as they are marked
non-idempotent, retries only failures before the RPC was sent; `BatchCommitWriteStreams` also
retries I/O failures and `UNAVAILABLE`. `control_plane_retry` (`src/ingest/write/retry.rs`) makes up
to three attempts with 200 ms doubling backoff on transport failures and transient statuses.

## UP-84: dbapi `adbc_read_partition` sends Ctrl-C to the statement, not the connection

`Cursor.adbc_read_partition` uses the statement's `cancel` as its SIGINT hook, so Ctrl-C gets
`INVALID_STATE` from this driver and the partition read keeps running. The driver cannot work
around it.

## UP-86: dbt 2.0 drops a seed's and a table copy's job ID

dbt 2.0's `load_dataframe` and `copy_table` discard the result batch whose `BIGQUERY:query_id`
carries the job ID, so seed and `copy_partitions` nodes have no `job_id` in `run_results.json`,
although the driver reports it ([DBT.md](DBT.md#result-metadata)).

## UP-87: dbapi `adbc_clone` drops autocommit and the backend

`Connection.adbc_clone` uses the `Connection` constructor's defaults, so a clone of an
`autocommit=True` connection has autocommit off and the default backend. Against this driver the
clone opens a transaction session at its first query and then refuses bulk ingest and
permanent-table DDL. Use `dbapi.connect` for a second connection instead.

## UP-88: a transient token failure is served from cache for 10 s

When a token fetch fails transiently with no valid token cached, `google-cloud-auth`'s `TokenCache`
hands that failure to every caller until its next attempt 10 seconds later, and the default ADC
builder takes no retry policy. The driver reports an unavailable token service as `IO` and retries,
but its retries end within about 5 seconds, so it recovers only if the cache's next attempt falls
inside that window and succeeds.

## UP-89: Storage Write reads array decimal elements at the type's full scale

Storage Write reads `REPEATED` NUMERIC or BIGNUMERIC elements at scale 9 or 38, ignoring their
Arrow scale: `0.01` appended as `Decimal128(10, 2)` into `ARRAY<NUMERIC(10, 2)>` is stored as
`0.000000001`; scalar and STRUCT fields keep their scale. The driver sends array elements as
`Decimal128(38, 9)` or `Decimal256(76, 38)` (`storage_write_schema` in `src/ingest/schema.rs`).

## UP-90: Storage Write fails every append to an `ARRAY<TIMESTAMP>` column

An append carrying a `REPEATED TIMESTAMP` field at any depth fails with `INTERNAL: Internal error
encountered`, even when every array is empty and whatever the unit or timezone, although the Arrow
mapping lists the type; repeated DATE, DATETIME and TIME, `ARRAY<STRUCT<ts TIMESTAMP>>`, and
appends omitting the column succeed. The failure arrives only after about 130 s of retries, so the
driver refuses such an ingest with `NOT_IMPLEMENTED` before any request
(`reject_repeated_timestamps` in `src/ingest/schema.rs`).

## UP-91: dbt 2.0 never clears a removed `persist_docs` policy tag

dbt 2.0's `update_columns_descriptions` puts in `adbc.bigquery.table.update_columns_policy_tags`
only columns whose YAML lists at least one tag, so a column whose tags were removed keeps them in
BigQuery; Python dbt Core's `update_columns` sends an empty list and clears them. The driver changes
only the columns the payload names, so it cannot tell a removed tag from an untagged column.

## UP-92: the Python manager's DB-API type objects answer `!=` with `True`

`adbc_driver_manager.dbapi`'s `STRING`, `BINARY`, `NUMBER`, `DATETIME` and `ROWID` define `__eq__`
but inherit `frozenset.__ne__`, so `pyarrow.string() != STRING` is `True`, as is `!=` against any
Arrow type. `bigquery_adbc.dbapi` wraps each with a consistent `__ne__`, and also compares an
extension type such as JSON's `arrow.json` by its storage type.

## UP-93: Storage Write exposes no user-agent configuration

The pinned writer has no user-agent setting on `Write::builder()` or its writers: the control-plane
RPCs build internal requests without caller options, and `AppendRows` (`write/stream.rs`) sends
`RequestOptions::default()`. They therefore keep the SDK's user agent instead of the driver's
identity and `bigquery.user_agent`, which REST and all Storage Read requests (including `ReadRows`
opens and resumes) carry. A fix needs an SDK hook reaching both the control plane and every append
stream.

## UP-94: `h2` ends a connection after 100 padded empty DATA frames

`h2` 0.4.20 and upstream `master` count each non-final DATA frame with an empty payload, padding
excluded, toward a per-connection limit of 100 (`MAX_RECV_EMPTY_DATA_FRAMES`); the 101st causes
`GOAWAY ENHANCE_YOUR_CALM` and fails every request in flight. Google's frontend sends such frames in
slowly produced gzip responses, such as large `jobs.query` answers, so two of them can end the
shared connection. REST requests therefore omit `Accept-Encoding: gzip` (`src/rest/client.rs`);
uncompressed responses carry no padding-only frames. Upgrading to current `master` does not fix
it; the open PR's [snapshot](#git-snapshot-candidates) could remove the workaround after validation.

## UP-95: `Cursor.adbc_ingest` keeps an earlier call's catalog and schema

`adbc_driver_manager.dbapi.Cursor.adbc_ingest` sets `adbc.ingest.target_catalog` and
`adbc.ingest.target_db_schema` only when `catalog_name` or `db_schema_name` is given, and statement
options persist, so a later call on the same cursor that omits them ingests into the earlier
target. Pass `catalog_name=""` and `db_schema_name=""`, which this driver reads as unset, or use a
new cursor.

## UP-96: Storage Write's Arrow appends refuse flexible column names

Storage Write supports [flexible column names](https://docs.cloud.google.com/bigquery/docs/schemas#flexible-column-names)
such as `first name` for protobuf rows, but an Arrow append to a table with one at any depth fails,
after retries, with `INTERNAL: BigQuery TableSchema failed to be initialized: Illegal field name`,
even when the append omits that column. The driver accepts only `[A-Za-z_][A-Za-z0-9_]*` column
names and refuses an append into such a table with `NOT_IMPLEMENTED` before any row is sent
(`unwritable_name` in `src/ingest/schema.rs`).

## UP-100: SDK token exchanges follow redirects

The pinned auth library's [user OAuth refresh](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/auth/src/credentials/user_account.rs#L451-L470)
and [STS exchange](https://github.com/googleapis/google-cloud-rust/blob/9ba338718561739ab257e87502516d4be42dd5d1/src/auth/src/credentials/internal/sts_exchange.rs#L100-L128)
use reqwest's default redirect policy, with no builder option to change it, and HTTP 307 and 308
resend the POST body with its refresh token and client secret or subject token. The driver's
endpoint allowlist therefore does not constrain every recipient of these credentials; its own
identity-provider client (`src/config/external_account.rs`) refuses redirects. No live Google
redirect has been observed.

## UP-101: SDK token exchanges have no timeout

The pinned auth library's user OAuth refresh, STS exchange and metadata-server requests each use a
`reqwest::Client` without a timeout, with no builder option for a deadline, so a token service
that accepts a connection and never answers hangs every waiting request. The driver bounds the wait
at two minutes (`src/config/redaction.rs`), but the library's background refresh stays blocked
until the connection dies: until then, retries and later requests fail at once with `IO` even if
the service has recovered. A wait for a running job keeps looking through its outage window.

## UP-102: gRPC server streams have no deadline or keepalive

`execute_server_streaming` applies no retry-loop deadline, and the tonic endpoint sets no HTTP/2 or
TCP keepalive or connect timeout, with no builder option for any of them. A reader with nothing
outstanding never learns its peer is gone, and since tonic applies request deadlines inside each
connection, a stalled TLS handshake outlasts even a unary deadline.
The driver limits `CreateReadSession` and `ReadRows` silence itself (`src/storage_read.rs`),
doubling a stream's limit because it sees only complete messages; a resume reuses the channel, so a
dead connection fails after about 10 minutes and a stream that opens and then stays silent after
up to 62. `AppendRows` has the same gap, so the driver bounds every Storage Write wait
(`src/ingest/write/retry.rs`); a keepalive and connect timeout would make these limits backstops.
