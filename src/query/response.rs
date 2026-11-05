//! Query response projections limited to rows, job identity, schema and execution statistics.

use adbc_core::error::{Result, Status};
use bytes::Bytes;

use serde::Deserialize;

use crate::error::{driver_defect, error};
use crate::rest::jobs::JobReference;
use crate::rest::json::{decode_failure, decode_json, optional_json_int};
use crate::rest::model::{ErrorProto, JobStatus, QueryParameter, TableReference, TableSchema};
use crate::rows::{RestRow, RestRows};

/// Small response envelopes can be decoded without handing work to the blocking pool.
pub(super) const INLINE_RESULT_BYTES: usize = 4 * 1024;

/// The most rows a complete result can hold and still skip the buffered pipeline.
pub(super) const INLINE_RESULT_ROWS: usize = 32;

/// One completed response page plus every summary field any query endpoint may return.
///
/// The same projection decodes `jobs.query` and `jobs.getQueryResults`: absent fields remain
/// absent and [`QueryMetadata::absorb_query`] keeps the richer value from an earlier response.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QueryResponse {
    #[serde(default, deserialize_with = "from_slice")]
    pub(super) schema: Option<TableSchema>,
    #[serde(default, deserialize_with = "from_slice")]
    pub(super) job_reference: Option<JobReference>,
    #[serde(default)]
    pub(super) query_id: String,
    #[serde(default)]
    pub(super) location: String,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_rows: Option<u64>,
    #[serde(default)]
    pub(super) page_token: String,
    #[serde(default)]
    pub(super) rows: RestRows,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_bytes_processed: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_bytes_billed: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_slot_ms: Option<i64>,
    #[serde(default)]
    pub(super) job_complete: bool,
    #[serde(default, deserialize_with = "from_slice")]
    pub(super) errors: Vec<ErrorProto>,
    #[serde(default, deserialize_with = "optional_json_int")]
    num_dml_affected_rows: Option<i64>,
    #[serde(default, deserialize_with = "from_slice")]
    session_info: Option<SessionInfo>,
    #[serde(default)]
    statement_type: String,
    #[serde(skip)]
    encoded_len: usize,
}

/// Decodes a field of the `from_str` row page with the slice reader its type uses elsewhere, so the
/// binary does not link a second, `StrRead` copy of that decoder.
fn from_slice<'de, D: serde::Deserializer<'de>, T: serde::de::DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<T, D::Error> {
    let raw = <&serde_json::value::RawValue>::deserialize(deserializer)?;
    serde_json::from_slice(raw.get().as_bytes()).map_err(serde::de::Error::custom)
}

impl QueryResponse {
    pub(super) fn decode(body: &[u8], what: &str) -> Result<Self> {
        // One UTF-8 check over the whole body, then `from_str`: `from_slice` validates each JSON
        // string separately, and a row page has thousands. This is the exception to "one JSON
        // decoder per type" in docs/BINARY-SIZE.md.
        let body = std::str::from_utf8(body).map_err(|source| {
            error(
                format!("failed to decode {what}: the response is not UTF-8: {source}"),
                Status::InvalidData,
            )
        })?;
        let mut response: Self =
            serde_json::from_str(body).map_err(|source| decode_failure(what, &source))?;
        response.encoded_len = body.len();
        Ok(response)
    }

    /// Decodes a response that may contain a full REST row page on the blocking pool, under one
    /// already-acquired driver-wide result-work permit.
    pub(super) async fn decode_bounded(
        body: Bytes,
        what: &'static str,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Self> {
        crate::runtime::run_bounded_result_work(permit, move || Self::decode(&body, what))
            .await
            .map_err(|source| {
                driver_defect(format_args!(
                    "the driver's task decoding {what} failed: {source}"
                ))
            })?
    }

    /// An incomplete response naming only `job`, for a job whose own response was lost.
    pub(super) fn naming(job: JobReference) -> Self {
        Self {
            job_reference: Some(job),
            ..Self::default()
        }
    }

    pub(super) fn page(&mut self) -> QueryPage {
        QueryPage {
            rows: std::mem::take(&mut self.rows).into_vec(),
            page_token: (!self.page_token.is_empty()).then(|| std::mem::take(&mut self.page_token)),
            encoded_len: self.encoded_len,
        }
    }

    pub(super) fn total_bytes_processed(&self) -> Option<i64> {
        self.total_bytes_processed
    }

    /// Validates a `getQueryResults` response against the job being polled and returns the most
    /// specific canonical reference without ever discarding a previously known location.
    pub(super) fn canonical_polled_reference(
        &self,
        expected: &JobReference,
    ) -> Result<JobReference> {
        const RESPONSE: &str = "the BigQuery jobs.getQueryResults response";
        let canonical = match self.job_reference.as_ref() {
            Some(returned) => expected.confirm(Some(returned), RESPONSE)?,
            None => expected.clone(),
        };
        canonical.with_location(
            (!self.location.is_empty()).then_some(self.location.as_str()),
            RESPONSE,
        )
    }
}

/// A query job's resource, as `jobs.insert` and `jobs.get` return it. Load and copy jobs read
/// their own projection, `crate::dbt::jobs::LoadOrCopyJob`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobResponse {
    #[serde(default)]
    pub(super) job_reference: Option<JobReference>,
    #[serde(default)]
    pub(super) configuration: Option<JobConfiguration>,
    #[serde(default)]
    pub(super) statistics: Option<JobStatistics>,
    #[serde(default)]
    pub(super) status: Option<JobStatus>,
}

impl JobResponse {
    pub(super) fn decode(body: &[u8], what: &str) -> Result<Self> {
        decode_json(body, what)
    }

    pub(super) fn is_done(&self) -> bool {
        self.status.as_ref().is_some_and(JobStatus::is_done)
    }

    /// The job resource's processed-byte count. Query-specific statistics win when both levels
    /// are present, as in `absorb_job`.
    pub(super) fn total_bytes_processed(&self) -> Option<i64> {
        self.statistics.as_ref().and_then(|statistics| {
            statistics
                .query
                .as_ref()
                .and_then(|query| query.total_bytes_processed)
                .or(statistics.total_bytes_processed)
        })
    }

    /// See [`JobStatus::take_terminal_errors`].
    pub(super) fn take_terminal_errors(&mut self) -> Option<Vec<ErrorProto>> {
        self.status.as_mut()?.take_terminal_errors()
    }

    /// The `statementType` this job resource names, empty where it carries no query
    /// statistics. It distinguishes a single statement from a multi-statement script.
    pub(super) fn statement_type(&self) -> &str {
        self.statistics
            .as_ref()
            .and_then(|statistics| statistics.query.as_ref())
            .map_or("", |query| query.statement_type.as_str())
    }

    pub(super) fn destination_table(&self) -> Option<&TableReference> {
        self.configuration
            .as_ref()
            .and_then(|configuration| configuration.query.as_ref())
            .and_then(|query| query.destination_table.as_ref())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobConfiguration {
    #[serde(default)]
    pub(super) query: Option<JobQueryConfiguration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobQueryConfiguration {
    #[serde(default)]
    pub(super) destination_table: Option<TableReference>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobStatistics {
    #[serde(default, deserialize_with = "optional_json_int")]
    total_bytes_processed: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_slot_ms: Option<i64>,
    #[serde(default)]
    pub(super) query: Option<QueryStatistics>,
    #[serde(default)]
    session_info: Option<SessionInfo>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QueryStatistics {
    #[serde(default, deserialize_with = "optional_json_int")]
    total_bytes_processed: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_bytes_billed: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    total_slot_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_json_int")]
    num_dml_affected_rows: Option<i64>,
    #[serde(default)]
    pub(super) statement_type: String,
    #[serde(default)]
    pub(super) schema: Option<TableSchema>,
    #[serde(default)]
    pub(super) undeclared_query_parameters: Option<Vec<QueryParameter>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionInfo {
    #[serde(default)]
    session_id: String,
}

/// Metadata accumulated without replacing a value that only the richer submission response had.
#[derive(Debug, Default)]
pub(super) struct QueryMetadata {
    pub(super) job_reference: Option<JobReference>,
    pub(super) query_id: String,
    pub(super) schema: Option<TableSchema>,
    pub(super) total_rows: Option<u64>,
    pub(super) total_bytes_processed: Option<i64>,
    pub(super) total_bytes_billed: Option<i64>,
    pub(super) total_slot_ms: Option<i64>,
    pub(super) num_dml_affected_rows: Option<i64>,
    pub(super) statement_type: String,
    pub(super) session_id: Option<String>,
    pub(super) undeclared_query_parameters: Option<Vec<QueryParameter>>,
    /// BigQuery refused to return a page of the result because a row exceeds its 20 MiB response
    /// limit, so only Storage Read can return the rows.
    pub(super) rows_exceed_rest: bool,
    /// The job completed through `jobs.getQueryResults`, which reports neither its billed bytes
    /// nor its slot time, so a result that reports them describes the job.
    pub(super) completed_by_poll: bool,
}

impl QueryMetadata {
    /// Takes owned metadata, leaving completion status, errors, cost and rows for the caller.
    pub(super) fn absorb_query(&mut self, response: &mut QueryResponse) {
        if let Some(mut reference) = response.job_reference.take() {
            if reference.location.as_deref().is_none_or(str::is_empty)
                && !response.location.is_empty()
            {
                reference.location = Some(std::mem::take(&mut response.location));
            }
            self.job_reference = Some(reference);
        }
        if !response.query_id.is_empty() {
            self.query_id = std::mem::take(&mut response.query_id);
        }
        absorb(&mut self.schema, response.schema.take());
        absorb(&mut self.total_rows, response.total_rows);
        absorb(
            &mut self.total_bytes_processed,
            response.total_bytes_processed,
        );
        absorb(&mut self.total_bytes_billed, response.total_bytes_billed);
        absorb(&mut self.total_slot_ms, response.total_slot_ms);
        absorb(
            &mut self.num_dml_affected_rows,
            response.num_dml_affected_rows,
        );
        if !response.statement_type.is_empty() {
            self.statement_type = std::mem::take(&mut response.statement_type);
        }
        if let Some(session) = response
            .session_info
            .take()
            .map(|session| session.session_id)
            .filter(|session_id| !session_id.is_empty())
        {
            self.session_id = Some(session);
        }
    }

    /// Absorbs a job resource and reports whether its query statistics supplied a schema, so the
    /// caller converts the schema again only when it was replaced. The schema and parameter list
    /// are moved out; `response`'s status, statistics and destination table stay in place.
    pub(super) fn absorb_job(&mut self, response: &mut JobResponse) -> bool {
        if self.job_reference.is_none() {
            self.job_reference.clone_from(&response.job_reference);
        }
        self.absorb_running_job(response);
        let Some(statistics) = response.statistics.as_mut() else {
            return false;
        };
        absorb(
            &mut self.total_bytes_processed,
            statistics.total_bytes_processed,
        );
        absorb(&mut self.total_slot_ms, statistics.total_slot_ms);
        let Some(query) = statistics.query.as_mut() else {
            return false;
        };
        let supplied_schema = query.schema.is_some();
        absorb(&mut self.schema, query.schema.take());
        absorb(&mut self.total_bytes_processed, query.total_bytes_processed);
        absorb(&mut self.total_bytes_billed, query.total_bytes_billed);
        absorb(&mut self.total_slot_ms, query.total_slot_ms);
        absorb(&mut self.num_dml_affected_rows, query.num_dml_affected_rows);
        // BigQuery omits this list when a dry run finds no parameters. Only missing query
        // statistics mean parameter discovery is unavailable.
        match query.undeclared_query_parameters.take() {
            Some(parameters) => self.undeclared_query_parameters = Some(parameters),
            None => {
                self.undeclared_query_parameters.get_or_insert_default();
            }
        }
        supplied_schema
    }

    /// Keeps what a job resource says for good even while its other statistics are a partial
    /// RUNNING snapshot: its statement type and session. `jobs.getQueryResults`, which completes
    /// the job, carries neither.
    pub(super) fn absorb_running_job(&mut self, response: &JobResponse) {
        let statement_type = response.statement_type();
        if !statement_type.is_empty() {
            self.statement_type = statement_type.to_owned();
        }
        if let Some(session) = response
            .statistics
            .as_ref()
            .and_then(|statistics| statistics.session_info.as_ref())
            .map(|session| session.session_id.as_str())
            .filter(|session_id| !session_id.is_empty())
        {
            self.session_id = Some(session.to_owned());
        }
    }

    pub(super) fn absorb_polled_query(
        &mut self,
        response: &mut QueryResponse,
        expected: &JobReference,
    ) -> Result<JobReference> {
        let canonical = response.canonical_polled_reference(expected)?;
        self.absorb_query(response);
        self.job_reference = Some(canonical.clone());
        Ok(canonical)
    }
}

fn absorb<T>(current: &mut Option<T>, update: Option<T>) {
    if update.is_some() {
        *current = update;
    }
}

#[derive(Debug, Default)]
pub(super) struct QueryPage {
    pub(super) rows: Vec<RestRow>,
    pub(super) page_token: Option<String>,
    /// The response size, which bounds whether a complete small page can use the resident reader.
    pub(super) encoded_len: usize,
}

/// Rows per REST response that carries rows: the first `jobs.query` page and every
/// `jobs.getQueryResults` page. BigQuery also ends a page at about 10 MB.
///
/// A page costs a round trip plus serialization time per row, so larger pages read faster, but a
/// first page whose rows Storage Read then reads again is wasted: uncapped, `jobs.query` returned
/// 350,000 narrow rows in 8.4-8.5 s where 100,000 took 3.1-3.2 s (docs/PERFORMANCE.md).
pub(crate) const RESULT_PAGE_ROWS: u32 = 100_000;

/// Whether a terminal failure permits a new job. Only the first entry is authoritative:
/// [`JobResponse::take_terminal_errors`] puts `status.errorResult` before secondary errors and
/// warnings, including a script's `stopped` statements.
///
/// `quotaExceeded` is excluded: a new job cannot fix the exhausted project quota that failed this
/// one. Request-level 403s with that reason are still retried.
pub(super) fn retryable_job_errors(errors: &[ErrorProto]) -> bool {
    errors.first().is_some_and(|error| {
        matches!(
            error.reason.as_str(),
            "backendError"
                | "jobBackendError"
                | "jobInternalError"
                | "rateLimitExceeded"
                | "jobRateLimitExceeded"
                | "tableUnavailable"
                | "internalError"
        )
    })
}

/// Whether the whole failed job can be reissued without duplicating committed writes.
/// Single statements fail atomically; scripts and procedures may have committed earlier
/// statements before failing. An absent `statementType` cannot establish which case applies.
pub(super) fn reissuable_statement(statement_type: &str) -> bool {
    !matches!(statement_type, "" | "SCRIPT" | "CALL")
}

#[cfg(test)]
mod tests;
