//! Endpoint-independent planning for one logical BigQuery query execution.
//!
//! A plan owns everything that can change what BigQuery executes, including its submission
//! identity. HTTP retries resend a plan byte-for-byte; a whole-query reissue builds a new plan
//! with a new identity. BigQuery may ignore a request ID for a read-only query.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::rest::model::QueryParameter;
use adbc_core::error::Result;

use crate::clients::DatasetKey;
use crate::error::invalid_state;
use crate::identifiers::ProjectIdentifier;
use crate::rest::jobs::next_identity;
use crate::rest::metadata::DatasetId;
use crate::{BigQueryCreateDisposition, BigQueryQueryPriority, BigQueryWriteDisposition};

use super::{DestinationTable, QueryDefaults, StatementQueryOptions};

/// How the plan is submitted to BigQuery, which its [`QueryIdentity`] already decides: a
/// `jobs.query` request ID or a `jobs.insert` job ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueryRoute {
    /// `jobs.query`, which can return a complete jobless result in one round trip.
    Query,
    /// `jobs.insert`, required for dry runs and job-only query configuration.
    Insert,
}

/// Whether `jobs.query` may avoid creating a durable job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum QueryJobCreation {
    /// Let BigQuery omit a job for a query that completes within the submit request.
    #[default]
    Optional,
    /// Require the durable job reference used by ADBC partition descriptors.
    Required,
}

impl QueryJobCreation {
    pub(super) const fn rest_value(self) -> &'static str {
        match self {
            Self::Optional => "JOB_CREATION_OPTIONAL",
            Self::Required => "JOB_CREATION_REQUIRED",
        }
    }
}

/// The submission identity for one logical query execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum QueryIdentity {
    /// A `jobs.query` request ID.
    Request(Arc<str>),
    /// A caller-generated `jobs.insert` job ID.
    Job(Arc<str>),
}

impl QueryIdentity {
    #[cfg(test)]
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Request(value) | Self::Job(value) => value,
        }
    }
}

/// A resolved default dataset, with the billing project already applied as the final fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct QueryDefaultDataset {
    pub(super) project_id: ProjectIdentifier,
    pub(super) dataset_id: Arc<str>,
}

impl QueryDefaultDataset {
    pub(super) fn id(&self) -> DatasetId<'_> {
        DatasetId {
            project_id: self.project_id.as_str(),
            dataset_id: &self.dataset_id,
        }
    }

    /// The dataset as a key for state kept per dataset. Building it allocates nothing.
    pub(super) fn key(&self) -> DatasetKey {
        (self.project_id.clone(), self.dataset_id.clone())
    }
}

/// Shared by plan clones so a transport retry does not copy the parameter tree.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct QueryParameters {
    pub(super) mode: Arc<str>,
    pub(super) values: Arc<[QueryParameter]>,
}

/// One fully resolved logical query execution, independent of an HTTP endpoint or client.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct QueryPlan {
    pub(super) project_id: ProjectIdentifier,
    pub(super) sql: Arc<str>,
    pub(super) default_dataset: Option<QueryDefaultDataset>,
    pub(super) location: Option<Arc<str>>,
    pub(super) session_id: Option<Arc<str>>,
    pub(super) parameters: Option<QueryParameters>,
    pub(super) destination_table: Option<DestinationTable>,
    pub(super) create_disposition: BigQueryCreateDisposition,
    pub(super) write_disposition: BigQueryWriteDisposition,
    pub(super) maximum_bytes_billed: i64,
    pub(super) job_timeout_ms: i64,
    pub(super) reservation: Option<Arc<str>>,
    pub(super) labels: Option<Arc<BTreeMap<String, String>>>,
    pub(super) priority: BigQueryQueryPriority,
    pub(super) create_session: bool,
    pub(super) disable_query_cache: bool,
    pub(super) dry_run: bool,
    pub(super) job_creation: QueryJobCreation,
    identity: QueryIdentity,
}

impl QueryPlan {
    /// Resolves the context and statement options for one logical execution.
    ///
    /// `parameters` distinguishes no parameters from a parameter mode with no values: dry-run
    /// parameter discovery sends the mode and omits the empty `queryParameters`.
    #[allow(
        clippy::too_many_arguments,
        reason = "the plan resolves one explicit query execution context at a single boundary"
    )]
    pub(super) fn new(
        billing_project_id: Option<&ProjectIdentifier>,
        defaults: QueryDefaults<'_>,
        location: Option<&str>,
        session_id: Option<&str>,
        sql: &str,
        parameters: Option<(&str, Vec<QueryParameter>)>,
        options: &StatementQueryOptions,
        job_creation: QueryJobCreation,
    ) -> Result<Self> {
        let project_id = billing_project_id.cloned().ok_or_else(|| {
            invalid_state(
                "a BigQuery project must be configured with the uri or bigquery.project_id database option before executing SQL",
            )
        })?;
        let default_dataset = defaults.db_schema.map(|dataset_id| QueryDefaultDataset {
            project_id: defaults.catalog.unwrap_or(&project_id).clone(),
            dataset_id: Arc::from(dataset_id),
        });
        let parameters = parameters.map(|(mode, values)| QueryParameters {
            mode: Arc::from(mode),
            values: Arc::from(values),
        });
        let identity = new_identity(route(options));
        Ok(Self {
            project_id,
            sql: Arc::from(sql),
            default_dataset,
            location: location.map(Arc::from),
            session_id: session_id.map(Arc::from),
            parameters,
            destination_table: options.destination_table.clone(),
            create_disposition: options.create_disposition,
            write_disposition: options.write_disposition,
            maximum_bytes_billed: options.maximum_bytes_billed,
            job_timeout_ms: options.job_timeout_ms,
            reservation: options.reservation.clone(),
            labels: options.labels.clone(),
            priority: options.priority,
            create_session: options.create_session,
            disable_query_cache: options.disable_query_cache,
            dry_run: options.dry_run,
            job_creation,
            identity,
        })
    }

    pub(super) const fn route(&self) -> QueryRoute {
        match self.identity {
            QueryIdentity::Request(_) => QueryRoute::Query,
            QueryIdentity::Job(_) => QueryRoute::Insert,
        }
    }

    pub(super) fn identity(&self) -> &QueryIdentity {
        &self.identity
    }

    #[cfg(test)]
    pub(super) fn request_id(&self) -> Option<&str> {
        match &self.identity {
            QueryIdentity::Request(value) => Some(value),
            QueryIdentity::Job(_) => None,
        }
    }

    #[cfg(test)]
    pub(super) fn job_id(&self) -> Option<&str> {
        match &self.identity {
            QueryIdentity::Job(value) => Some(value),
            QueryIdentity::Request(_) => None,
        }
    }

    /// Clones the execution with a fresh idempotency identity, for a terminal job failure whose
    /// BigQuery reason permits running the SQL again. Transport retries reuse the original plan.
    pub(super) fn reissue(&self) -> Self {
        let mut plan = self.clone();
        plan.identity = new_identity(self.route());
        plan
    }
}

/// A default create/write disposition or priority does not select `jobs.insert`, because BigQuery
/// treats it as absent.
fn route(options: &StatementQueryOptions) -> QueryRoute {
    if options.dry_run
        || options.destination_table.is_some()
        || options.create_disposition != BigQueryCreateDisposition::default()
        || options.write_disposition != BigQueryWriteDisposition::default()
        || options.priority != BigQueryQueryPriority::default()
    {
        QueryRoute::Insert
    } else {
        QueryRoute::Query
    }
}

/// A fresh idempotency identity for `route`, prefixed so a job ID and a request ID are never
/// mistaken for one another in a log or a console.
fn new_identity(route: QueryRoute) -> QueryIdentity {
    match route {
        QueryRoute::Query => QueryIdentity::Request(next_identity("req_").into()),
        QueryRoute::Insert => QueryIdentity::Job(next_identity("job_").into()),
    }
}
