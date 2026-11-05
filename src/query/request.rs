//! Compact JSON request projections for the two BigQuery query submission endpoints.
//!
//! These types borrow a [`QueryPlan`], so serializing a retry does not clone its SQL, parameters,
//! labels or identity. Fields at BigQuery's default are omitted, except `useLegacySql: false`,
//! which must be explicit because the REST API defaults to legacy SQL.

use adbc_core::error::Result;
use serde::Serialize;

use crate::rest::jobs::{JobReference, JobRequest};
use crate::rest::json::JsonInt;
use crate::rest::metadata::{DatasetId, TableId};
use crate::rest::model::{JobOptions, QueryParameter};
use crate::{BigQueryCreateDisposition, BigQueryQueryPriority, BigQueryWriteDisposition};

use super::QualifiedTableName;
use super::QueryResultMode;
#[cfg(test)]
use super::plan::QueryIdentity;
use super::plan::{QueryDefaultDataset, QueryPlan, QueryRoute};

impl QueryPlan {
    /// The `jobs.insert` request for a plan on the job route, in a [`JobRequest`] envelope.
    pub(super) fn job_request(&self, job_id: &str) -> Result<JobRequest> {
        let request = JobRequest::new(
            JobReference::new(
                self.project_id.as_str(),
                self.location.as_deref(),
                job_id.to_owned(),
            ),
            self.dry_run,
            &QueryJobConfiguration::new(self),
            "BigQuery query job request",
        )?;
        let request = match &self.destination_table {
            Some(destination) => request.writing(destination.id()),
            None => request,
        };
        Ok(if self.infers_job_location() {
            // The job most likely runs with the table it writes, else with the default dataset
            // its unqualified names read.
            request.locatable_in(
                self.destination_table
                    .iter()
                    .map(QualifiedTableName::dataset_key)
                    .chain(self.default_dataset.iter().map(QueryDefaultDataset::key)),
            )
        } else {
            request
        })
    }

    /// Whether this plan submits a job without a location, letting BigQuery infer it.
    pub(super) fn infers_job_location(&self) -> bool {
        self.route() == QueryRoute::Insert && self.location.is_none() && !self.dry_run
    }

    #[cfg(test)]
    pub(super) fn jobs_query_request(&self) -> Option<JobsQueryRequest<'_>> {
        match self.identity() {
            QueryIdentity::Request(request_id) => Some(JobsQueryRequest::new(
                self,
                request_id,
                QueryResultMode::Rows,
            )),
            QueryIdentity::Job(_) => None,
        }
    }

    /// The encoded `jobs.insert` body of a plan on the job route.
    #[cfg(test)]
    pub(super) fn jobs_insert_body(&self) -> Option<serde_json::Value> {
        match self.identity() {
            QueryIdentity::Job(job_id) => {
                let request = self.job_request(job_id).unwrap();
                Some(serde_json::from_slice(request.body()).unwrap())
            }
            QueryIdentity::Request(_) => None,
        }
    }
}

/// The complete `jobs.query` JSON body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobsQueryRequest<'a> {
    #[serde(flatten)]
    query: QueryFields<'a>,
    request_id: &'a str,
    format_options: FormatOptions,
    job_creation_mode: &'static str,
    timeout_ms: u32,
    max_results: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<&'a str>,
    #[serde(flatten)]
    job: JobOptions<'a>,
}

impl<'a> JobsQueryRequest<'a> {
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        // SQL dominates a compiled dbt request. Reserve it plus the usual envelope so growing
        // the buffer does not copy it again; escaping, labels and parameters can still grow it.
        let mut body = Vec::with_capacity(self.query.query.len().saturating_add(512));
        serde_json::to_writer(&mut body, self).map_err(|source| {
            crate::rest::json::encode_failure("the BigQuery jobs.query request", &source)
        })?;
        Ok(body)
    }

    pub(super) fn new(
        plan: &'a QueryPlan,
        request_id: &'a str,
        result_mode: QueryResultMode,
    ) -> Self {
        Self {
            query: QueryFields::new(plan),
            request_id,
            format_options: FormatOptions {
                use_int64_timestamp: true,
            },
            job_creation_mode: plan.job_creation.rest_value(),
            // Match BigQuery's default completion wait so short queries can return inline.
            // Remote cancellation needs the job reference returned after this wait.
            timeout_ms: 10_000,
            max_results: result_mode.max_results(),
            location: plan.location.as_deref(),
            job: JobOptions::new(
                plan.job_timeout_ms,
                plan.labels.as_deref(),
                plan.reservation.as_deref(),
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn request_id(&self) -> &str {
        self.request_id
    }
}

/// The compact `configuration` accepted by the common `jobs.insert` transport.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryJobConfiguration<'a> {
    query: QueryJob<'a>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    dry_run: bool,
    #[serde(flatten)]
    job: JobOptions<'a>,
}

impl<'a> QueryJobConfiguration<'a> {
    fn new(plan: &'a QueryPlan) -> Self {
        Self {
            query: QueryJob::new(plan),
            dry_run: plan.dry_run,
            job: JobOptions::new(
                plan.job_timeout_ms,
                plan.labels.as_deref(),
                plan.reservation.as_deref(),
            ),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryJob<'a> {
    #[serde(flatten)]
    query: QueryFields<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_table: Option<TableId<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    create_disposition: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_disposition: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<&'static str>,
}

impl<'a> QueryJob<'a> {
    fn new(plan: &'a QueryPlan) -> Self {
        Self {
            query: QueryFields::new(plan),
            destination_table: plan.destination_table.as_ref().map(QualifiedTableName::id),
            create_disposition: (plan.create_disposition != BigQueryCreateDisposition::default())
                .then_some(plan.create_disposition.as_str()),
            write_disposition: (plan.write_disposition != BigQueryWriteDisposition::default())
                .then_some(plan.write_disposition.as_str()),
            priority: (plan.priority != BigQueryQueryPriority::default())
                .then_some(plan.priority.as_str()),
        }
    }
}

/// Query semantics shared by `jobs.query` and `configuration.query` in `jobs.insert`.
/// Endpoint-specific fields stay in their enclosing request types.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryFields<'a> {
    query: &'a str,
    /// BigQuery's REST default is legacy SQL, so `false` is always sent.
    use_legacy_sql: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_dataset: Option<DatasetId<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_properties: Option<[ConnectionProperty<'a>; 1]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameter_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "parameters_absent_or_empty")]
    query_parameters: Option<&'a [QueryParameter]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maximum_bytes_billed: Option<JsonInt<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    create_session: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    use_query_cache: Option<bool>,
}

impl<'a> QueryFields<'a> {
    fn new(plan: &'a QueryPlan) -> Self {
        let (parameter_mode, query_parameters) = match plan.parameters.as_ref() {
            Some(parameters) => (
                Some(parameters.mode.as_ref()),
                Some(parameters.values.as_ref()),
            ),
            None => (None, None),
        };
        Self {
            query: &plan.sql,
            use_legacy_sql: false,
            default_dataset: plan.default_dataset.as_ref().map(QueryDefaultDataset::id),
            connection_properties: connection_properties(plan.session_id.as_deref()),
            parameter_mode,
            query_parameters,
            maximum_bytes_billed: nonzero_i64(plan.maximum_bytes_billed),
            create_session: plan.create_session.then_some(true),
            use_query_cache: plan.disable_query_cache.then_some(false),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FormatOptions {
    use_int64_timestamp: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct ConnectionProperty<'a> {
    key: &'static str,
    value: &'a str,
}

fn connection_properties(session_id: Option<&str>) -> Option<[ConnectionProperty<'_>; 1]> {
    session_id.map(|value| {
        [ConnectionProperty {
            key: "session_id",
            value,
        }]
    })
}

fn nonzero_i64(value: i64) -> Option<JsonInt<i64>> {
    (value != 0).then_some(JsonInt(value))
}

fn parameters_absent_or_empty(value: &Option<&[QueryParameter]>) -> bool {
    value.is_none_or(<[QueryParameter]>::is_empty)
}

#[cfg(test)]
mod tests;
