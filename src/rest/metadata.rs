//! BigQuery table and dataset metadata through compact REST models.
//!
//! Decoding only the fields the driver uses avoids linking the generated service models; see
//! `BINARY-SIZE.md`. Nested metadata types live in `src/rest/model.rs`.
//!
//! Unknown fields are ignored. Dataset access entries retain their original JSON so updating
//! an access list preserves fields this driver does not know.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use google_cloud_auth::credentials::Credentials;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use super::model::{
    Clustering, DatasetReference, EncryptionConfiguration, ExternalDataConfiguration,
    MaterializedViewDefinition, RangePartitioning, TableConstraints, TableReference, TableSchema,
    TimePartitioning, ViewDefinition,
};
use crate::cancellation::{self, Operation};
use crate::diag::diag;
use crate::error::{error, named};
use crate::rest::client::RestClient;
use crate::rest::json::{JsonInt, JsonIntValue, decode_json, encode_json};
use crate::rest::projects_prefix;
use crate::rest::rejection::{precondition_error, service_error, service_failure};
use crate::rest::request::{Request, send};
use crate::rest::retry::{MAX_ATTEMPTS, RetryClass, RetryDelays, retry_class};
use crate::rest::url::escape_url_component;

/// The `project.dataset` pair a dataset request names, serialized as a `DatasetReference`.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatasetId<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) dataset_id: &'a str,
}

/// The `project.dataset.table` triple a table request names, serialized as a `TableReference`.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableId<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) dataset_id: &'a str,
    pub(crate) table_id: &'a str,
}

impl<'a> TableId<'a> {
    /// The table's resource name in the Storage Read and Storage Write APIs.
    pub(crate) fn storage_path(self) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{}",
            self.project_id, self.dataset_id, self.table_id
        )
    }

    fn dataset(self) -> DatasetId<'a> {
        DatasetId {
            project_id: self.project_id,
            dataset_id: self.dataset_id,
        }
    }
}

impl std::fmt::Display for DatasetId<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.project_id, self.dataset_id)
    }
}

impl std::fmt::Display for TableId<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}.{}.{}",
            self.project_id, self.dataset_id, self.table_id
        )
    }
}

/// Which locally consumed projection a `tables.get` describes.
#[derive(Clone, Copy)]
pub(crate) enum TableView {
    /// The type and names of an exact object, which no listing supplied.
    Type,
    /// The schema alone, which a dbt-core 2.0 column metadata update rewrites.
    Schema,
    /// Constraints explicitly requested at ADBC table depth, without column schemas.
    Constraints,
    /// The schema and constraints an ADBC `get_objects` column listing reports, with the type an
    /// ingest checks its target against.
    Basic,
    /// The storage statistics `get_statistics` reports, and nothing else.
    StorageStats,
    /// The schema and configuration dbt-core 2.0 reconstructs a relation from.
    Configuration,
}

impl TableView {
    fn as_str(self) -> &'static str {
        match self {
            Self::Type | Self::Schema | Self::Constraints | Self::Basic | Self::Configuration => {
                "BASIC"
            }
            Self::StorageStats => "STORAGE_STATS",
        }
    }

    /// The response fields the caller of this view consumes.
    ///
    /// Every view `scan_tables` describes with carries `tableReference` too: an exact lookup
    /// reports the names BigQuery resolved, which differ from the filter's in a case-insensitive
    /// dataset.
    fn fields(self) -> &'static str {
        match self {
            Self::Type => "tableReference(datasetId,tableId),type",
            Self::Schema => "schema",
            Self::Constraints => "tableConstraints,tableReference(datasetId,tableId),type",
            Self::Basic => "schema,tableConstraints,tableReference(datasetId,tableId),type",
            // `type` is not a statistic, but an exact lookup in `scan_tables` classifies the
            // table from whichever view's response it gets.
            Self::StorageStats => {
                "lastModifiedTime,numActiveLogicalBytes,numBytes,numLongTermBytes,\
                 numLongTermLogicalBytes,numPartitions,numPhysicalBytes,numRows,\
                 numTimeTravelPhysicalBytes,numTotalLogicalBytes,numTotalPhysicalBytes,\
                 streamingBuffer,tableReference(datasetId,tableId),type"
            }
            Self::Configuration => {
                "clustering,description,encryptionConfiguration,expirationTime,\
                 externalDataConfiguration,labels,materializedView,maxStaleness,\
                 rangePartitioning,requirePartitionFilter,resourceTags,schema,timePartitioning,\
                 type,view"
            }
        }
    }
}

/// Partial-response selectors for the metadata calls whose shape is not a [`TableView`].
const ETAG_FIELDS: &str = "etag";
const DATASET_ACCESS_FIELDS: &str = "access,etag";
const DATASET_GET_FIELDS: &str = "datasetReference(datasetId)";
const DATASET_LOCATION_FIELDS: &str = "location";
const DATASET_LIST_FIELDS: &str = "datasets(datasetReference(datasetId)),nextPageToken";
const TABLE_LIST_FIELDS: &str = "nextPageToken,tables(tableReference(datasetId,tableId),type)";

/// Why one metadata request produced no document.
///
/// Kept apart because each call site words its own failure, and several treat a rejection (a
/// missing table, a dropped dataset) as an ordinary outcome. Metadata requests are sent unnamed,
/// so the call site's context is the only name a failure carries.
pub(crate) enum MetadataError {
    /// The request never reached an answer: cancelled, unauthenticated, a transport failure, or
    /// a response the driver could not decode. Worded without naming the request.
    Failed(Error),
    /// The service refused the request.
    Rejected(reqwest::StatusCode, bytes::Bytes),
}

impl MetadataError {
    /// Keep the last request's diagnostics while reporting why the retry was interrupted.
    fn interrupted(self, interruption: Error) -> Self {
        Self::Failed(interrupted_failure(self.into_error(""), interruption))
    }

    /// Whether the service answered that the named object does not exist.
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(self, Self::Rejected(status, _) if *status == reqwest::StatusCode::NOT_FOUND)
    }

    /// Whether `datasets.get` answered that the dataset is missing. A missing project is a 404
    /// with the same `notFound` reason, so only the message tells the two apart.
    pub(crate) fn is_missing_dataset(&self) -> bool {
        match self {
            Self::Rejected(status, body) if *status == reqwest::StatusCode::NOT_FOUND => {
                super::rejection::service_message(body)
                    .is_some_and(|message| message.starts_with("Not found: Dataset"))
            }
            _ => false,
        }
    }

    /// BigQuery rejects an invalid project spelling with 400 rather than 404. Such a catalog
    /// filter matches nothing; unrelated request errors must still reach the caller.
    pub(crate) fn is_invalid_project(&self) -> bool {
        match self {
            Self::Rejected(status, body) if *status == reqwest::StatusCode::BAD_REQUEST => {
                super::rejection::reason_message(body, "invalid")
                    .is_some_and(|message| message.starts_with("Invalid project ID "))
            }
            _ => false,
        }
    }

    /// The ADBC error, led by the caller's `context` ("failed to get BigQuery table metadata for
    /// p.d.t").
    ///
    /// A [`Self::Failed`] message names neither the request nor the operation, so `context` is
    /// prepended; its status, details, and SQLSTATE are kept. An empty `context` leaves the error
    /// unnamed (see [`crate::error::named`]).
    pub(crate) fn into_error(self, context: &str) -> Error {
        match self {
            Self::Failed(mut failure) => {
                failure.message = named(context, &failure.message);
                failure
            }
            Self::Rejected(status, body) => service_error(context, status, &body),
        }
    }
}

/// `failure`, the last answer a retry was waiting out, reported under the interruption that ended
/// the wait, with the diagnostics of both.
fn interrupted_failure(mut failure: Error, interruption: Error) -> Error {
    failure.message = format!(
        "{}; last metadata request failed: {}",
        interruption.message, failure.message
    );
    failure.status = interruption.status;
    failure.sqlstate = interruption.sqlstate;
    if let Some(details) = interruption.details {
        failure.details.get_or_insert_default().extend(details);
    }
    failure
}

type MetadataResult<T> = std::result::Result<T, MetadataError>;

/// One request [`MetadataClient::send_with_retry`] sends, and may resend.
#[derive(Clone, Copy)]
enum Call<'a> {
    Get,
    /// A `PATCH` of `body`, guarded by `etag` as an `If-Match` precondition unless it is empty.
    Patch {
        etag: &'a str,
        body: &'a bytes::Bytes,
    },
    /// A `POST` of `body` that creates a resource.
    Create(&'a bytes::Bytes),
}

impl Call<'_> {
    /// Whether a replay after a success that went unseen leaves the same state. A replayed
    /// creation would be refused with 409, and a replayed guarded patch with 412.
    fn idempotent(self) -> bool {
        match self {
            Self::Get => true,
            Self::Patch { etag, .. } => etag.is_empty(),
            Self::Create(_) => false,
        }
    }
}

/// Metadata retry schedule; tests use a flat 1 ms delay.
#[cfg(not(test))]
const METADATA_DELAYS: RetryDelays = RetryDelays::SERVICE;
#[cfg(test)]
const METADATA_DELAYS: RetryDelays = RetryDelays::flat(std::time::Duration::from_millis(1));

/// The transport for the driver's table and dataset requests. Clones are cheap and share one
/// connection pool.
#[derive(Clone)]
pub(crate) struct MetadataClient {
    http: RestClient,
    credentials: Credentials,
    /// The prefix every metadata URL is built on. See [`projects_prefix`].
    projects: Arc<str>,
    /// The backoff between attempts, [`METADATA_DELAYS`] unless a test replaces it.
    delays: RetryDelays,
}

impl MetadataClient {
    /// Builds the client for one database's credentials and endpoint.
    pub(crate) fn new(
        credentials: Credentials,
        configured_endpoint: Option<&str>,
        http: RestClient,
    ) -> Self {
        Self {
            http,
            credentials,
            projects: projects_prefix(configured_endpoint),
            delays: METADATA_DELAYS,
        }
    }

    /// Replaces the retry schedule with one flat, `delay`-bounded one. See [`RetryDelays::flat`].
    #[cfg(test)]
    pub(crate) fn with_flat_delays(mut self, delay: std::time::Duration) -> Self {
        self.delays = RetryDelays::flat(delay);
        self
    }

    fn dataset_url(&self, dataset: DatasetId<'_>) -> String {
        format!(
            "{}/{}/datasets/{}",
            self.projects,
            escape_url_component(dataset.project_id),
            escape_url_component(dataset.dataset_id)
        )
    }

    fn table_url(&self, table: TableId<'_>) -> String {
        format!(
            "{}/tables/{}",
            self.dataset_url(table.dataset()),
            escape_url_component(table.table_id)
        )
    }

    /// The URL of a table mutation whose successful response is ignored.
    fn table_update_url(&self, table: TableId<'_>) -> String {
        response_fields(self.table_url(table), ETAG_FIELDS)
    }

    /// The URL of an access-list mutation whose successful response is ignored.
    fn dataset_access_update_url(&self, dataset: DatasetId<'_>) -> String {
        response_fields(self.dataset_url(dataset), ETAG_FIELDS)
    }

    fn table_get_url(&self, table: TableId<'_>, view: TableView) -> String {
        response_fields(
            format!("{}?view={}", self.table_url(table), view.as_str()),
            view.fields(),
        )
    }

    fn table_insert_url(&self, table: TableId<'_>) -> String {
        response_fields(
            format!("{}/tables", self.dataset_url(table.dataset())),
            ETAG_FIELDS,
        )
    }

    fn dataset_list_url(
        &self,
        project_id: &str,
        max_results: Option<u32>,
        page_token: &str,
    ) -> String {
        let mut url = response_fields(
            format!(
                "{}/{}/datasets{}",
                self.projects,
                escape_url_component(project_id),
                listing_query(max_results, page_token)
            ),
            DATASET_LIST_FIELDS,
        );
        // ADBC metadata includes hidden datasets. INFORMATION_SCHEMA cannot describe them, but
        // `get_objects` and `get_statistics` use the table metadata API, which can.
        url.push_str("&all=true");
        url
    }

    fn table_list_url(&self, dataset: DatasetId<'_>, page_token: &str) -> String {
        response_fields(
            format!(
                "{}/tables{}",
                self.dataset_url(dataset),
                listing_query(Some(METADATA_PAGE_SIZE), page_token)
            ),
            TABLE_LIST_FIELDS,
        )
    }

    fn dataset_get_url(&self, dataset: DatasetId<'_>) -> String {
        response_fields(self.dataset_url(dataset), DATASET_GET_FIELDS)
    }

    fn dataset_access_get_url(&self, dataset: DatasetId<'_>) -> String {
        response_fields(self.dataset_url(dataset), DATASET_ACCESS_FIELDS)
    }

    fn dataset_location_get_url(&self, dataset: DatasetId<'_>) -> String {
        response_fields(self.dataset_url(dataset), DATASET_LOCATION_FIELDS)
    }

    /// `datasets.patch` of an access list, guarded by `etag` as an `If-Match` precondition.
    ///
    /// The body replaces the whole list, so the precondition keeps a stale read from dropping
    /// entries added since. A header, because BigQuery ignores an `etag` in the request body.
    /// `action` names the change in every message ("authorize view v on BigQuery dataset p.d");
    /// `changed` replaces the service's 412 message.
    pub(crate) async fn patch_dataset_access(
        &self,
        operation: &Operation,
        dataset: DatasetId<'_>,
        etag: &str,
        body: Vec<u8>,
        action: &str,
        changed: &str,
    ) -> Result<()> {
        if etag.is_empty() {
            return Err(error(
                format!("cannot {action}: BigQuery returned no ETag for the metadata precondition"),
                Status::InvalidData,
            ));
        }
        let url = self.dataset_access_update_url(dataset);
        let body = bytes::Bytes::from(body);
        let context = format!("failed to {action}");
        match self
            .send_with_retry(Some(operation), &url, Call::Patch { etag, body: &body })
            .await
        {
            Ok(_) => Ok(()),
            Err(MetadataError::Rejected(status, response)) => {
                Err(precondition_error(&context, status, &response, changed))
            }
            Err(failure) => Err(failure.into_error(&context)),
        }
    }

    /// The body of one successful `GET`. See [`Self::send_with_retry`].
    async fn read(&self, operation: Option<&Operation>, url: &str) -> MetadataResult<bytes::Bytes> {
        self.send_with_retry(operation, url, Call::Get).await
    }

    /// The body of one successful `call`, resent after failures that say nothing about the
    /// resource (5xx, quota, transport, timeout), as the Go client's `runWithRetry` does. The last
    /// failure is returned whole.
    ///
    /// A call that is not [idempotent](Call::idempotent) is resent only after a rate-limit
    /// rejection, which applied nothing. BigQuery allows about five metadata updates per table or
    /// dataset every ten seconds, and dbt sends them back to back, such as one access-list patch
    /// per authorized view.
    async fn send_with_retry(
        &self,
        operation: Option<&Operation>,
        url: &str,
        call: Call<'_>,
    ) -> MetadataResult<bytes::Bytes> {
        let mut attempt = 1;
        let mut last_failure: Option<MetadataError> = None;
        loop {
            let request = match call {
                Call::Get => Request::get(url, ""),
                Call::Patch { etag, body } => Request::patch(url, etag, body.clone(), ""),
                Call::Create(body) => Request::post_json(url, body.clone(), ""),
            };
            let answer = send(operation, &self.credentials, &self.http, request).await;
            let (class, failure) = match answer {
                Ok((status, body)) if status.is_success() => return Ok(body),
                Ok((status, body)) => {
                    let (rendered, reason) = service_failure("", status, &body);
                    (
                        retry_class(Some(status), reason.as_deref(), rendered.status),
                        MetadataError::Rejected(status, body),
                    )
                }
                Err(failure) if failure.status == Status::Cancelled => {
                    return Err(match last_failure {
                        Some(previous) => previous.interrupted(failure),
                        None => MetadataError::Failed(failure),
                    });
                }
                Err(failure) => (
                    retry_class(None, None, failure.status),
                    MetadataError::Failed(failure),
                ),
            };
            let resend = match class {
                RetryClass::Final => false,
                RetryClass::Transient => call.idempotent(),
                RetryClass::RateLimited => true,
            };
            if !resend || attempt == MAX_ATTEMPTS {
                return Err(failure);
            }
            diag!(
                Debug,
                "resending a BigQuery metadata request to {url} after attempt {attempt}"
            );
            if let Err(interruption) =
                cancellation::sleep(operation, self.delays.delay(class, attempt)).await
            {
                return Err(failure.interrupted(interruption));
            }
            last_failure = Some(failure);
            attempt += 1;
        }
    }

    /// `tables.get`, asking BigQuery for exactly the fields `view`'s consumer reads.
    pub(crate) async fn get_table(
        &self,
        operation: Option<&Operation>,
        table: TableId<'_>,
        view: TableView,
    ) -> MetadataResult<Table> {
        let url = self.table_get_url(table, view);
        let body = self.read(operation, &url).await?;
        Table::decode(&body).map_err(MetadataError::Failed)
    }

    /// `tables.insert` of an empty table with `schema`, resent only after a rate-limit
    /// rejection. The response is ignored.
    pub(crate) async fn insert_table(
        &self,
        operation: Option<&Operation>,
        table: TableId<'_>,
        schema: &TableSchema,
    ) -> MetadataResult<()> {
        let url = self.table_insert_url(table);
        let body = encode_json(
            &TableDefinition {
                table_reference: Some(table),
                schema,
            },
            "the BigQuery table definition",
        )
        .map_err(MetadataError::Failed)?;
        self.send_with_retry(operation, &url, Call::Create(&body.into()))
            .await
            .map(|_| ())
    }

    /// `tables.patch` of the table schema alone, sent blind: see the caller for why it carries no
    /// `If-Match`.
    pub(crate) async fn patch_table_schema(
        &self,
        operation: Option<&Operation>,
        table: TableId<'_>,
        schema: &TableSchema,
    ) -> MetadataResult<()> {
        let body = encode_json(
            &TableDefinition {
                table_reference: None,
                schema,
            },
            "the BigQuery column metadata",
        )
        .map_err(MetadataError::Failed)?;
        self.patch_table(operation, table, body).await
    }

    /// `tables.patch` of the table description alone, sent blind: see the caller for why it
    /// carries no `If-Match`.
    pub(crate) async fn patch_table_description(
        &self,
        operation: Option<&Operation>,
        table: TableId<'_>,
        description: &str,
    ) -> MetadataResult<()> {
        let body = encode_json(
            &TableDescription { description },
            "the BigQuery table description",
        )
        .map_err(MetadataError::Failed)?;
        self.patch_table(operation, table, body).await
    }

    /// One `tables.patch` without a precondition, resent like a read, with the last rejection
    /// handed back whole. See [`Self::send_with_retry`].
    async fn patch_table(
        &self,
        operation: Option<&Operation>,
        table: TableId<'_>,
        body: Vec<u8>,
    ) -> MetadataResult<()> {
        let url = self.table_update_url(table);
        let body = body.into();
        self.send_with_retry(
            operation,
            &url,
            Call::Patch {
                etag: "",
                body: &body,
            },
        )
        .await
        .map(|_| ())
    }

    /// One page of `datasets.list`, continuing `page_token` when the caller has one.
    ///
    /// `max_results` bounds the page; the current-catalog check asks for one dataset to prove the
    /// project is readable.
    pub(crate) async fn list_datasets(
        &self,
        operation: Option<&Operation>,
        project_id: &str,
        max_results: Option<u32>,
        page_token: &str,
    ) -> MetadataResult<Page<String>> {
        let url = self.dataset_list_url(project_id, max_results, page_token);
        let body = self.read(operation, &url).await?;
        Page::decode(&body, "BigQuery dataset listing", "datasets", |entry| {
            Ok(entry
                .value::<DatasetReference>("datasetReference")?
                .map(|reference| reference.dataset_id)
                .unwrap_or_default())
        })
        .map_err(MetadataError::Failed)
    }

    /// One page of `tables.list`, continuing `page_token` when the caller has one.
    pub(crate) async fn list_tables(
        &self,
        operation: Option<&Operation>,
        dataset: DatasetId<'_>,
        page_token: &str,
    ) -> MetadataResult<Page<ListedTable>> {
        let url = self.table_list_url(dataset, page_token);
        let body = self.read(operation, &url).await?;
        Page::decode(&body, "BigQuery table listing", "tables", |entry| {
            Ok(ListedTable {
                reference: entry.value("tableReference")?,
                table_type: entry.value_or_default("type")?,
            })
        })
        .map_err(MetadataError::Failed)
    }

    /// `datasets.get`, which proves that the named dataset exists and is readable.
    ///
    /// Returns the `datasetReference.datasetId` BigQuery resolved the name to, which differs from
    /// the request only in case, in a case-insensitive dataset; empty if the response omits it.
    pub(crate) async fn get_dataset(
        &self,
        operation: Option<&Operation>,
        dataset: DatasetId<'_>,
    ) -> MetadataResult<String> {
        let url = self.dataset_get_url(dataset);
        let body = self.read(operation, &url).await?;
        Document::decode(&body, "BigQuery dataset metadata")
            .and_then(|mut document| document.value::<DatasetReference>("datasetReference"))
            .map(|reference| {
                reference
                    .map(|reference| reference.dataset_id)
                    .unwrap_or_default()
            })
            .map_err(MetadataError::Failed)
    }

    /// `datasets.get` of the dataset's location, such as `US` or `asia-northeast1`.
    pub(crate) async fn get_dataset_location(
        &self,
        operation: Option<&Operation>,
        dataset: DatasetId<'_>,
    ) -> MetadataResult<String> {
        let url = self.dataset_location_get_url(dataset);
        let body = self.read(operation, &url).await?;
        let location = Document::decode(&body, "BigQuery dataset metadata")
            .and_then(|mut document| document.value_or_default::<String>("location"))
            .map_err(MetadataError::Failed)?;
        if location.is_empty() {
            return Err(MetadataError::Failed(error(
                "BigQuery returned dataset metadata without a location",
                Status::InvalidData,
            )));
        }
        Ok(location)
    }

    /// `datasets.get` for the access-list read half of a guarded read-modify-write.
    pub(crate) async fn get_dataset_access(
        &self,
        operation: Option<&Operation>,
        dataset: DatasetId<'_>,
    ) -> MetadataResult<Dataset> {
        let url = self.dataset_access_get_url(dataset);
        let body = self.read(operation, &url).await?;
        Dataset::decode(&body).map_err(MetadataError::Failed)
    }
}

/// Appends an encoded Google API partial-response selector to a URL with or without a query.
fn response_fields(mut url: String, fields: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    write!(url, "{separator}fields={}", escape_url_component(fields))
        .expect("writing to a String cannot fail");
    url
}

/// The page both metadata walks ask `datasets.list` and `tables.list` for.
///
/// Without `maxResults` the service picks the page size, and each listing walk is sequential. The
/// discovery document sets no maximum on either method; 1000 keeps a large dataset or project to
/// a few requests.
pub(crate) const METADATA_PAGE_SIZE: u32 = 1_000;

/// The query string one listing request carries, or the empty string when it needs neither a
/// page bound nor a continuation.
fn listing_query(max_results: Option<u32>, page_token: &str) -> String {
    match (max_results, page_token) {
        (None, "") => String::new(),
        (Some(max_results), "") => format!("?maxResults={max_results}"),
        (None, token) => format!("?pageToken={}", escape_url_component(token)),
        (Some(max_results), token) => format!(
            "?maxResults={max_results}&pageToken={}",
            escape_url_component(token)
        ),
    }
}

/// The `tables.insert` and `tables.patch` request bodies: a schema, and for a creation the
/// reference naming the table to create.
///
/// A struct rather than `serde_json::json!`, which would build a `Value` tree and link a second
/// serializer for `TableSchema` and everything under it.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TableDefinition<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_reference: Option<TableId<'a>>,
    schema: &'a TableSchema,
}

/// The `tables.patch` body that sets the description and nothing else.
#[derive(serde::Serialize)]
struct TableDescription<'a> {
    description: &'a str,
}

/// The `tables.get` fields the driver reads, and only those.
///
/// `get_table_schema` and `get_objects` report the configuration and columns, `get_statistics`
/// the storage counters, and the dbt-core 2.0 column metadata update the schema.
#[derive(Debug, Default)]
pub(crate) struct Table {
    /// The dataset and table IDs BigQuery resolved the requested names to.
    pub(crate) table_reference: Option<TableReference>,
    /// BigQuery's own name for the object kind: `TABLE`, `VIEW`, `MATERIALIZED_VIEW`, ...
    pub(crate) table_type: String,
    pub(crate) description: String,
    pub(crate) labels: HashMap<String, String>,
    pub(crate) resource_tags: HashMap<String, String>,
    pub(crate) clustering: Option<Clustering>,
    pub(crate) schema: Option<TableSchema>,
    pub(crate) time_partitioning: Option<TimePartitioning>,
    pub(crate) range_partitioning: Option<RangePartitioning>,
    pub(crate) require_partition_filter: Option<bool>,
    pub(crate) view: Option<ViewDefinition>,
    pub(crate) materialized_view: Option<MaterializedViewDefinition>,
    pub(crate) external_data_configuration: Option<ExternalDataConfiguration>,
    pub(crate) encryption_configuration: Option<EncryptionConfiguration>,
    pub(crate) table_constraints: Option<TableConstraints>,
    pub(crate) max_staleness: String,
    pub(crate) expiration_time: Option<i64>,
    pub(crate) last_modified_time: u64,
    pub(crate) num_rows: Option<u64>,
    /// `streamingBuffer.estimatedRows`, present only while the table has a streaming buffer,
    /// whose rows `numRows` excludes.
    pub(crate) streaming_buffer_rows: Option<u64>,
    pub(crate) num_bytes: Option<i64>,
    pub(crate) num_physical_bytes: Option<i64>,
    pub(crate) num_long_term_bytes: Option<i64>,
    pub(crate) num_total_logical_bytes: Option<i64>,
    pub(crate) num_active_logical_bytes: Option<i64>,
    pub(crate) num_long_term_logical_bytes: Option<i64>,
    pub(crate) num_total_physical_bytes: Option<i64>,
    pub(crate) num_time_travel_physical_bytes: Option<i64>,
    pub(crate) num_partitions: Option<i64>,
}

impl Table {
    pub(crate) fn decode(body: &[u8]) -> Result<Self> {
        let mut document = Document::decode(body, "BigQuery table metadata")?;
        let streaming_buffer_rows = match document.field("streamingBuffer") {
            None => None,
            // A buffer without an estimate still makes the row count approximate.
            Some(buffer) => Some(
                Document::object(buffer, "BigQuery table streaming buffer metadata")?
                    .integer("estimatedRows")?
                    .unwrap_or_default(),
            ),
        };
        Ok(Self {
            table_reference: document.value("tableReference")?,
            table_type: document.value_or_default("type")?,
            description: document.value_or_default("description")?,
            labels: document.value_or_default("labels")?,
            resource_tags: document.value_or_default("resourceTags")?,
            clustering: document.value("clustering")?,
            schema: document.value("schema")?,
            time_partitioning: document.value("timePartitioning")?,
            range_partitioning: document.value("rangePartitioning")?,
            require_partition_filter: document.value("requirePartitionFilter")?,
            view: document.value("view")?,
            materialized_view: document.value("materializedView")?,
            external_data_configuration: document.value("externalDataConfiguration")?,
            encryption_configuration: document.value("encryptionConfiguration")?,
            table_constraints: document.value("tableConstraints")?,
            max_staleness: document.value_or_default("maxStaleness")?,
            expiration_time: document.integer("expirationTime")?,
            last_modified_time: document.integer("lastModifiedTime")?.unwrap_or_default(),
            num_rows: document.integer("numRows")?,
            streaming_buffer_rows,
            num_bytes: document.integer("numBytes")?,
            num_physical_bytes: document.integer("numPhysicalBytes")?,
            num_long_term_bytes: document.integer("numLongTermBytes")?,
            num_total_logical_bytes: document.integer("numTotalLogicalBytes")?,
            num_active_logical_bytes: document.integer("numActiveLogicalBytes")?,
            num_long_term_logical_bytes: document.integer("numLongTermLogicalBytes")?,
            num_total_physical_bytes: document.integer("numTotalPhysicalBytes")?,
            num_time_travel_physical_bytes: document.integer("numTimeTravelPhysicalBytes")?,
            num_partitions: document.integer("numPartitions")?,
        })
    }
}

/// One listing page and its continuation token, which is empty on the last page.
pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next_page_token: String,
}

impl<T> Page<T> {
    /// Decodes one listing page, mapping each entry with `entry`.
    ///
    /// `key` names the entry array, which the service omits for an empty dataset or project.
    fn decode(
        body: &[u8],
        what: &'static str,
        key: &str,
        entry: impl Fn(&mut Document<'_>) -> Result<T>,
    ) -> Result<Self> {
        let mut document = Document::decode(body, what)?;
        let items = match document.field(key) {
            None => Vec::new(),
            Some(raw) => match serde_json::from_slice::<Vec<&RawValue>>(raw.get().as_bytes()) {
                Ok(entries) => entries
                    .into_iter()
                    .map(|item| entry(&mut Document::object(item, what)?))
                    .collect::<Result<Vec<T>>>()?,
                Err(_) => {
                    return Err(error(
                        format!("BigQuery returned {what} whose {key} is not an array"),
                        Status::InvalidData,
                    ));
                }
            },
        };
        Ok(Self {
            items,
            next_page_token: document.value_or_default("nextPageToken")?,
        })
    }
}

/// One entry of a `tables.list` page.
pub(crate) struct ListedTable {
    /// The listed `tableReference`, or `None` when the entry has none (reported differently from
    /// an empty table ID).
    pub(crate) reference: Option<TableReference>,
    pub(crate) table_type: String,
}

/// The `datasets.get` fields the driver reads.
#[derive(Debug, Default)]
pub(crate) struct Dataset {
    pub(crate) etag: String,
    /// The access control list, each entry as raw JSON.
    ///
    /// Authorizing a view appends an entry and sends the whole list back, so every entry must
    /// round-trip exactly, including unknown fields. Raw entries are never normalized or
    /// reordered, and no `Access` decoder is linked; the caller decodes only `view`.
    pub(crate) access: Vec<Box<RawValue>>,
}

impl Dataset {
    pub(crate) fn decode(body: &[u8]) -> Result<Self> {
        let mut document = Document::decode(body, "BigQuery dataset metadata")?;
        Ok(Self {
            etag: document.value_or_default("etag")?,
            access: document.value_or_default("access")?,
        })
    }
}

/// Borrowed JSON fields, removed as they are decoded. Raw values avoid an intermediate `Value`
/// tree and a second deserializer specialization for each nested type; see `BINARY-SIZE.md`.
struct Document<'a> {
    fields: HashMap<FieldName<'a>, &'a RawValue>,
    /// The document in error messages, as a noun phrase: "BigQuery table metadata".
    what: &'static str,
}

/// A document's field name, borrowed from the body unless it is spelled with an escape.
#[derive(PartialEq, Eq, Hash, serde::Deserialize)]
struct FieldName<'a>(#[serde(borrow)] Cow<'a, str>);

impl Borrow<str> for FieldName<'_> {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl<'a> Document<'a> {
    fn decode(body: &'a [u8], what: &'static str) -> Result<Self> {
        Self::object(decode_json(body, what)?, what)
    }

    fn object(document: &'a RawValue, what: &'static str) -> Result<Self> {
        // The value is already parsed, so the only way it fails to read as a map is not being one.
        let Ok(fields) = serde_json::from_slice(document.get().as_bytes()) else {
            return Err(error(
                format!("BigQuery returned {what} that is not a JSON object"),
                Status::InvalidData,
            ));
        };
        Ok(Self { fields, what })
    }

    /// A field that is present and not JSON `null`, still unparsed.
    ///
    /// An explicit `null` is treated as absent.
    fn field(&mut self, key: &str) -> Option<&'a RawValue> {
        self.fields.remove(key).filter(|raw| raw.get() != "null")
    }

    /// Decodes a present field. Wrong-typed fields must not become absent metadata.
    fn value<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>> {
        match self.field(key) {
            None => Ok(None),
            Some(raw) => decode_json(raw.get().as_bytes(), self.what).map(Some),
        }
    }

    /// Uses the default only for absent or null fields.
    fn value_or_default<T: DeserializeOwned + Default>(&mut self, key: &str) -> Result<T> {
        Ok(self.value(key)?.unwrap_or_default())
    }

    /// An integer field in either [`JsonInt`] spelling. Anything else is an error, not an absent
    /// field, so a malformed statistic is never mistaken for an unreported one.
    fn integer<T: JsonIntValue>(&mut self, key: &str) -> Result<Option<T>> {
        self.field(key)
            .map(|raw| {
                serde_json::from_slice::<JsonInt<T>>(raw.get().as_bytes())
                    .map(|value| value.0)
                    .map_err(|_| {
                        error(
                            format!("BigQuery returned {} with a non-integer {key}", self.what),
                            Status::InvalidData,
                        )
                    })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
