//! BigQuery job submission and control over the JSON REST API.
//!
//! There is no full `Job` model: callers serialize only the configuration they send, through
//! [`JobRequest`], and decode only the response fields they need, so a copy job does not link
//! every other job model.

use std::collections::hash_map::RandomState;
use std::fmt::Write as _;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adbc_core::error::{Result, Status};
use google_cloud_auth::credentials::Credentials;
use serde::Serialize;

use crate::BigQueryDatabaseOption;
use crate::cancellation::{self, Operation};
use crate::clients::{DatasetKey, DatasetLocations};
use crate::diag::diag;
use crate::error::{
    append_remedy, error, failure_reason, invalid_argument, job_name, with_job_link,
    with_job_verdict,
};
use crate::identifiers::ProjectIdentifier;
use crate::rest::client::RestClient;
use crate::rest::json::{decode_json, encode_json};
use crate::rest::projects_prefix;
use crate::rest::rejection::{reason_message, service_error};
use crate::rest::request::{Request, RequestStart, send};
use crate::rest::retry::{MAX_ATTEMPTS, RetryClass, RetryDelays, classify, retryable_failure};
use crate::rest::url::escape_url_component;
use crate::runtime::DetachedTask;

/// The server-side wait for a query whose job reference is already registered.
///
/// Sent by [`JobClient::query_results_url`]. Initial `jobs.query` submission waits up to ten
/// seconds for inline results. This completion wait must stay below the REST
/// client's 60 s request timeout; `crate::query::direct::poll`'s poll floor assumes this value.
pub(crate) const QUERY_COMPLETION_WAIT_MS: u32 = 45_000;

/// What [`JobClient::query`] got back.
#[derive(Debug)]
pub(crate) enum QueryAnswer {
    /// The `jobs.query` response document.
    Response(bytes::Bytes),
    /// The job an earlier attempt created, which a replay's 409 named instead of answering.
    Replayed(JobReference),
}

/// Which read of a query's results one [`JobClient::get_query_results`] call is.
///
/// Rendered into "failed to get the BigQuery {read}".
#[derive(Clone, Copy, Debug)]
pub(crate) enum QueryResultsRead {
    /// Waiting for the query to complete, which also returns its first page.
    Completion,
    /// One sequential page of an already completed query's results.
    Page,
}

impl std::fmt::Display for QueryResultsRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Completion => "query results",
            Self::Page => "query result page",
        })
    }
}

/// The `reason` values a rejected completion read can carry only as the query job's own
/// failure. `notFound`, `accessDenied` and `responseTooLarge` are left out: each may be about the
/// read itself, the last when one row exceeds the read's row limit.
const JOB_VERDICT_REASONS: [&str; 7] = [
    "billingTierLimitExceeded",
    "bytesBilledLimitExceeded",
    "invalid",
    "invalidQuery",
    "resourcesExceeded",
    "stopped",
    "timeout",
];

/// Attempts to find a job after an ambiguous submission; an accepted job can take a moment to
/// become visible to `jobs.get`.
pub(crate) const MAX_RECONCILIATION_ATTEMPTS: u32 = 3;

/// Bound the exceptional cross-region search, including credentials, pagination and the final
/// job fetch. The page cap also bounds work if a service repeats pagination tokens.
const JOB_SEARCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_JOB_SEARCH_PAGES: u32 = 100;
const JOB_SEARCH_CLOCK_SKEW: Duration = Duration::from_secs(300);

/// How long a wait for a job outlasts consecutive retryable failures. A request's own retries span
/// seconds, far less than a network blip, and ending the wait cancels the job. Go waits until its
/// context ends; this bound keeps a network that never returns from blocking, forever, a caller
/// that has no way to cancel. Tests give up at once unless they set their own limit.
pub(crate) const JOB_WAIT_OUTAGE: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(600)
};

/// A BigQuery job identity chosen before submission.
///
/// A caller-chosen ID lets a submission be retried after an ambiguous failure without duplicating
/// the job. Each new execution needs a fresh ID from [`next_identity`]; only the caller knows when
/// one begins.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobReference {
    #[serde(default)]
    pub(crate) project_id: String,
    #[serde(default)]
    pub(crate) job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) location: Option<String>,
}

impl JobReference {
    pub(crate) fn new(project_id: &str, location: Option<&str>, job_id: String) -> Self {
        Self {
            project_id: project_id.to_owned(),
            job_id,
            location: location
                .filter(|location| !location.is_empty())
                .map(ToOwned::to_owned),
        }
    }

    /// Checks that `returned`, the job reference in `response`, names this job, and returns the
    /// canonical identity. Later requests use that identity, so a mismatch is caught here.
    pub(crate) fn confirm(&self, returned: Option<&Self>, response: &str) -> Result<Self> {
        let returned = returned
            .ok_or_else(|| invalid_data(format!("{response} contains no job reference")))?;
        if returned.project_id.is_empty() || returned.job_id.is_empty() {
            return Err(invalid_data(format!(
                "{response} contains an incomplete job reference {}",
                job_name(returned)
            )));
        }
        if !self.matches_identity(returned) {
            return Err(invalid_data(format!(
                "{response} names job {}, but the request was for job {}",
                job_name(returned),
                job_name(self)
            )));
        }
        let mut confirmed = self.clone();
        confirmed.project_id.clone_from(&returned.project_id);
        confirmed.with_location(returned.location.as_deref(), response)
    }

    /// Matches the project and job ID; locations are checked separately.
    pub(crate) fn matches_identity(&self, returned: &Self) -> bool {
        self.job_id == returned.job_id && matches_project(&self.project_id, &returned.project_id)
    }

    /// Adopts `location`, which `response` names for this job: a location BigQuery inferred is
    /// kept, and one that differs from this reference's only in case is the same one.
    pub(crate) fn with_location(mut self, location: Option<&str>, response: &str) -> Result<Self> {
        let Some(location) = location else {
            return Ok(self);
        };
        if location.is_empty() {
            return Err(invalid_data(format!(
                "{response} names an empty location for job {}",
                job_name(&self)
            )));
        }
        if let Some(requested) = self.location.as_deref()
            && !requested.eq_ignore_ascii_case(location)
        {
            return Err(invalid_data(format!(
                "{response} names location {location} for job {}, but the request was for \
                 location {requested}",
                job_name(&self)
            )));
        }
        self.location = Some(location.to_owned());
        Ok(self)
    }
}

/// An authenticated response can resolve a project number to its textual ID. Once resolved,
/// later responses must name that exact project; a different number is never an alias.
fn matches_project(requested: &str, returned: &str) -> bool {
    let is_number =
        |value: &str| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit());
    requested == returned
        || (is_number(requested)
            && !is_number(returned)
            && ProjectIdentifier::try_new(returned).is_ok())
}

fn invalid_data(message: String) -> adbc_core::error::Error {
    error(message, Status::InvalidData)
}

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A process-unique request or job ID: `prefix` plus 32 hex digits. `RandomState` is seeded per
/// thread from the OS; the process ID is hashed too, so a child forked after initialization, which
/// inherits the seed and the counter, does not repeat its parent's IDs. The counter half keeps IDs
/// distinct within a process.
pub(crate) fn next_identity(prefix: &str) -> String {
    let sequence = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hash = RandomState::new().build_hasher();
    hash.write_u32(std::process::id());
    hash.write_u64(sequence);
    format!("{prefix}{:016x}{sequence:016x}", hash.finish())
}

/// An encoded `jobs.insert` request bound to its job ID.
///
/// The body is encoded once and replayed byte for byte; private fields keep a retry from pairing
/// the ID with another configuration.
pub(crate) struct JobRequest {
    reference: JobReference,
    dry_run: bool,
    body: bytes::Bytes,
    /// The table the job writes, which its submission messages name. See [`Self::writing`].
    destination: Option<String>,
    /// Where a job submitted without a location may be. See [`Self::locatable_in`].
    datasets: Vec<DatasetKey>,
}

impl JobRequest {
    /// Wraps the caller's compact configuration in the REST job envelope.
    pub(crate) fn new<C: Serialize>(
        reference: JobReference,
        dry_run: bool,
        configuration: &C,
        what: &str,
    ) -> Result<Self> {
        if reference.project_id.is_empty() {
            return Err(invalid_argument(
                "a BigQuery job submission requires a billing project ID",
            ));
        }
        if reference.job_id.is_empty() {
            return Err(invalid_argument(
                "a BigQuery job submission requires a caller-generated job ID",
            ));
        }
        let body = encode_json(
            &JobEnvelope {
                job_reference: &reference,
                configuration,
            },
            what,
        )?
        .into();
        Ok(Self {
            reference,
            dry_run,
            body,
            destination: None,
            datasets: Vec::new(),
        })
    }

    /// Names `destination`, the table the job writes, in its submission messages.
    pub(crate) fn writing(mut self, destination: impl std::fmt::Display) -> Self {
        self.destination = Some(destination.to_string());
        self
    }

    /// Names the datasets, in order, whose locations may hold this job when it is submitted
    /// without a location and the submission is ambiguous. BigQuery runs a job where the data it
    /// reads or writes is, so these are where it most likely ran. See [`JobClient::insert`].
    pub(crate) fn locatable_in(mut self, datasets: impl IntoIterator<Item = DatasetKey>) -> Self {
        for dataset in datasets {
            if !self.datasets.contains(&dataset) {
                self.datasets.push(dataset);
            }
        }
        self
    }

    pub(crate) fn reference(&self) -> &JobReference {
        &self.reference
    }

    /// The encoded job envelope, also used as the metadata part of media `jobs.insert`.
    pub(crate) fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobEnvelope<'a, C> {
    job_reference: &'a JobReference,
    configuration: &'a C,
}

/// The failed requests of one wait for a job to finish. See [`JOB_WAIT_OUTAGE`].
pub(crate) struct WaitOutage {
    limit: Duration,
    since: Option<tokio::time::Instant>,
    /// Whether ending the wait cancels the job: only an operation tracks one.
    cancels: bool,
}

impl WaitOutage {
    /// Records an answer about the job, which ends any outage.
    pub(crate) fn end(&mut self) {
        self.since = None;
    }

    /// Lets the wait for `job` continue past `failure`, or returns the failure that ends it. Only
    /// a retryable failure can be outlasted, and once it cannot, the error says the job's outcome
    /// is unknown: ending the wait cancels an unfinished job, but that request can fail too.
    ///
    /// An outage opens at `round_started`, when the failed requests began, since requests that
    /// hang spend minutes before they fail. It is only checked here, between rounds, so a wait can
    /// run over by one round.
    pub(crate) fn outlast(
        &mut self,
        mut failure: adbc_core::error::Error,
        job: &JobReference,
        round_started: tokio::time::Instant,
    ) -> Result<()> {
        if !retryable_failure(failure.status) {
            return Err(failure);
        }
        let waited = self.since.get_or_insert(round_started).elapsed();
        if waited < self.limit {
            return Ok(());
        }
        let cancelling = if self.cancels {
            " and is cancelling it if unfinished"
        } else {
            ""
        };
        append_remedy(
            &mut failure,
            &format!(
                "the driver stopped waiting for BigQuery job {} after {} s of failed \
                 requests{cancelling}; check the job before running this again",
                job_name(job),
                waited.as_secs()
            ),
        );
        Err(failure)
    }
}

/// A database-scoped transport for BigQuery's job collection.
#[derive(Clone)]
pub(crate) struct JobClient {
    credentials: Credentials,
    http: RestClient,
    /// The prefix every job URL is built on. See [`projects_prefix`].
    projects: Arc<str>,
    /// Retry schedules. See [`RetryDelays`].
    service_delays: RetryDelays,
    prompt_delays: RetryDelays,
    poll_delays: RetryDelays,
    /// See [`JOB_WAIT_OUTAGE`].
    wait_outage: Duration,
    /// Where to look for a job submitted without a location. See [`Self::insert`].
    dataset_locations: Option<DatasetLocations>,
}

impl JobClient {
    /// Builds a lightweight wrapper around the database's shared credentials and HTTP pool.
    pub(crate) fn new(
        credentials: Credentials,
        configured_endpoint: Option<&str>,
        http: RestClient,
    ) -> Self {
        Self {
            credentials,
            http,
            projects: projects_prefix(configured_endpoint),
            service_delays: RetryDelays::SERVICE,
            prompt_delays: RetryDelays::PROMPT,
            poll_delays: RetryDelays::POLL,
            wait_outage: JOB_WAIT_OUTAGE,
            dataset_locations: None,
        }
    }

    /// Lets an ambiguous submission without a location look for its job in the locations of
    /// [`JobRequest::locatable_in`]'s datasets.
    pub(crate) fn with_dataset_locations(mut self, locations: DatasetLocations) -> Self {
        self.dataset_locations = Some(locations);
        self
    }

    /// An anonymous client on `endpoint` whose retries wait a millisecond.
    #[cfg(test)]
    pub(crate) fn for_test(endpoint: &str) -> Self {
        Self::new(
            google_cloud_auth::credentials::anonymous::Builder::new().build(),
            Some(endpoint),
            crate::rest::client::rest_client("job client test").unwrap(),
        )
        .with_flat_delays(Duration::from_millis(1))
    }

    /// Replaces every retry schedule with one flat, `delay`-bounded one. See
    /// [`RetryDelays::flat`].
    #[cfg(test)]
    pub(crate) fn with_flat_delays(mut self, delay: Duration) -> Self {
        self.service_delays = RetryDelays::flat(delay);
        self.prompt_delays = RetryDelays::flat(delay);
        self.poll_delays = RetryDelays::flat(delay);
        self
    }

    /// Sets how long a wait outlasts failed requests. See [`JOB_WAIT_OUTAGE`].
    #[cfg(test)]
    pub(crate) fn with_wait_outage(mut self, limit: Duration) -> Self {
        self.wait_outage = limit;
        self
    }

    /// The outage state of a new wait for a job to finish.
    pub(crate) fn wait_outage(&self, operation: Option<&Operation>) -> WaitOutage {
        WaitOutage {
            limit: self.wait_outage,
            since: None,
            cancels: operation.is_some(),
        }
    }

    /// Submits one JSON job and returns the response document for the caller's own projection.
    ///
    /// A real job is registered for cancellation before the first POST. Retries resend the same
    /// body and job ID, so a lost response cannot duplicate the job: BigQuery answers the replay
    /// with 409, and fetching that job reconciles it as the original submission. A dry run creates
    /// no job, so there is nothing to reconcile: its 409 is about what the job would write, such as
    /// a `CREATE TABLE` of an existing table.
    ///
    /// A real job without a location runs where BigQuery infers, as `jobs.query` does, and the
    /// answer names that location. See [`InferredSubmission`].
    pub(crate) async fn insert(
        &self,
        operation: Option<&Operation>,
        request: &JobRequest,
        what: &str,
    ) -> Result<bytes::Bytes> {
        // A dry run creates no job, so only a real submission names one.
        let mut job = if request.dry_run {
            format!("dry-run {what} job")
        } else {
            format!("{what} job {}", job_name(&request.reference))
        };
        if let Some(destination) = &request.destination {
            write!(job, " into {destination}").expect("writing to a String cannot fail");
        }
        if !request.dry_run && request.reference.location.is_none() {
            let submission = InferredSubmission {
                client: self.clone(),
                operation: operation.cloned(),
                reference: request.reference.clone(),
                body: request.body.clone(),
                datasets: request.datasets.clone(),
                what: what.to_owned(),
                job,
                start: RequestStart::default(),
                submitted_at: SystemTime::now(),
            };
            return submission.insert().await;
        }
        let url = self.jobs_url(&request.reference);
        // `send` words transport failures from `send_context`; a rejection is the service's
        // verdict on the job.
        let send_context = format!("submit the BigQuery {job}");
        let error_context = format!("BigQuery rejected the {job}");
        if request.dry_run {
            let plan = Retry::new(self.service_delays, &error_context);
            return self
                .retrying(operation, plan, &mut || {
                    Request::post_json(&url, request.body.clone(), &send_context)
                })
                .await;
        }
        if let Some(operation) = operation {
            operation.register_provisional_job(&request.reference)?;
        }
        // A submission that stops with no attempt that may have created the job withdraws its
        // identity rather than confirming it.
        let start = RequestStart::default();
        let mut may_exist = false;
        for attempt in 1..=MAX_ATTEMPTS {
            let response = send(
                operation,
                &self.credentials,
                &self.http,
                Request::post_json(&url, request.body.clone(), &send_context)
                    .with_start_signal(&start),
            )
            .await;
            may_exist |= may_have_created(&start, &response);
            let (class, failure) = match response {
                Ok((reqwest::StatusCode::CONFLICT, body)) => {
                    let failure =
                        service_error(&error_context, reqwest::StatusCode::CONFLICT, &body);
                    confirm_submission(operation, &request.reference)?;
                    return self
                        .reconcile_submission(operation, &request.reference, what, Some(failure))
                        .await;
                }
                Err(failure) if failure.status == Status::Cancelled => {
                    // A cancel that raced job visibility may have received 404; confirming the
                    // identity now that the submission has stopped gives it one more try, and the
                    // operation suppresses a duplicate cancel.
                    settle_submission(operation, &request.reference, may_exist);
                    return Err(failure);
                }
                answer => match classify(answer, &error_context) {
                    Ok(body) => {
                        confirm_submission(operation, &request.reference)?;
                        return Ok(body);
                    }
                    Err(rejected) => rejected,
                },
            };
            if class == RetryClass::Final || (attempt == MAX_ATTEMPTS && !may_exist) {
                if !may_exist {
                    settle_submission(operation, &request.reference, false);
                    return Err(with_job_verdict(failure, "not_created"));
                }
                return Err(failure);
            }
            if attempt == MAX_ATTEMPTS {
                confirm_submission(operation, &request.reference)?;
                return self
                    .reconcile_submission(operation, &request.reference, what, Some(failure))
                    .await;
            }
            diag!(
                Debug,
                "retrying an ambiguous BigQuery job submission with the same job ID {} after \
                 attempt {attempt} ({:?})",
                request.reference.job_id,
                failure.status
            );
            if let Err(failure) =
                cancellation::sleep(operation, self.service_delays.delay(class, attempt)).await
            {
                if failure.status == Status::Cancelled {
                    // A POST before this backoff may have been accepted, and a visibility-raced
                    // cancel then gets its one post-confirmation generation.
                    settle_submission(operation, &request.reference, may_exist);
                }
                return Err(failure);
            }
        }
        unreachable!("the bounded insert loop returns on its final attempt")
    }

    /// Submits one encoded `jobs.query` request, replaying the exact request ID and body after an
    /// ambiguous transport failure.
    ///
    /// BigQuery answers a replay of a submission that created a job with 409 `duplicate` naming
    /// that job, not with its results, so a replay's 409 is the job to read instead. A first
    /// attempt's 409 is a request ID collision and stays an error.
    ///
    /// A 403 `responseTooLarge` (a result row over 20 MiB) also comes from a query that ran and
    /// kept its job, so it is replayed once to name that job, whose rows only Storage Read can
    /// return.
    ///
    /// Sends ungated so a cancelled caller can retain the answer and cancel its job. After a
    /// definitive rejection, cancellation stops retries; ambiguous attempts still recover the job.
    pub(crate) async fn query(
        &self,
        operation: Option<&Operation>,
        project_id: &str,
        body: bytes::Bytes,
        start: &RequestStart,
    ) -> Result<QueryAnswer> {
        let url = format!(
            "{}/{}/queries",
            self.projects,
            escape_url_component(project_id)
        );
        // A transport failure is a failure to submit; an error document is BigQuery rejecting the
        // query.
        let send_context = "submit the BigQuery jobs.query request";
        let error_context = "BigQuery rejected the query";
        // A caller that resubmits uses a new request ID, so it would run the statement again if an
        // attempt that got no verdict ran it. An answer to a replay is about the replay alone.
        let warn_it_may_have_run = |failure: &mut adbc_core::error::Error| {
            relead(failure, error_context, &format!("failed to {send_context}"));
            append_remedy(
                failure,
                "BigQuery may still have run this statement; check before running it again",
            );
        };
        let mut replayed_too_large = false;
        let mut may_exist = false;
        for attempt in 1..=MAX_ATTEMPTS {
            if !may_exist && let Some(operation) = operation {
                operation.check()?;
            }
            let request =
                Request::post_json(&url, body.clone(), send_context).with_start_signal(start);
            let response = send(None, &self.credentials, &self.http, request).await;
            may_exist |= may_have_created(start, &response);
            if attempt > 1
                && let Ok((reqwest::StatusCode::CONFLICT, body)) = &response
                && let Some(job) = duplicate_in(body, project_id)
            {
                return Ok(QueryAnswer::Replayed(job));
            }
            let (class, mut failure) = match classify(response, error_context) {
                Ok(body) => return Ok(QueryAnswer::Response(body)),
                Err(rejected) => rejected,
            };
            if !replayed_too_large
                && attempt < MAX_ATTEMPTS
                && failure_reason(&failure) == Some("responseTooLarge")
            {
                replayed_too_large = true;
                may_exist = true;
                continue;
            }
            if class == RetryClass::Final {
                // A stopped query (at its job timeout, for example) was accepted, not rejected.
                if failure_reason(&failure) == Some("stopped") {
                    relead(&mut failure, error_context, "BigQuery stopped the query");
                } else if attempt > 1 && may_exist {
                    warn_it_may_have_run(&mut failure);
                }
                return Err(failure);
            }
            if attempt == MAX_ATTEMPTS {
                if may_exist {
                    warn_it_may_have_run(&mut failure);
                }
                return Err(failure);
            }
            diag!(
                Debug,
                "retrying an ambiguous BigQuery jobs.query submission with the same request ID \
                 after attempt {attempt} ({:?})",
                failure.status
            );
            cancellation::sleep(
                operation.filter(|_| !may_exist),
                self.service_delays.delay(class, attempt),
            )
            .await?;
        }
        unreachable!("the bounded jobs.query loop returns on its final attempt")
    }

    /// Long-polls completion or fetches one sequential result page.
    ///
    /// Every call requests integer-microsecond timestamps, as the row decoder expects. A
    /// metadata-only completion passes zero `max_results` so the service serializes no rows.
    pub(crate) async fn get_query_results(
        &self,
        operation: Option<&Operation>,
        reference: &JobReference,
        page_token: Option<&str>,
        max_results: u32,
        read: QueryResultsRead,
    ) -> Result<bytes::Bytes> {
        let url = self.query_results_url(reference, page_token, max_results);
        let send_context = format!("get the BigQuery {read}");
        let error_context = format!("failed to get the BigQuery {read}");
        let plan = Retry::for_job(self.service_delays, &error_context);
        let mut failure = match self
            .retrying(operation, plan, &mut || Request::get(&url, &send_context))
            .await
        {
            Ok(body) => return Ok(body),
            Err(failure) => failure,
        };
        // BigQuery maps a failed job's `errorResult` onto every read of its results, so a
        // completion read rejected with a job-only reason reports the job's failure.
        if matches!(read, QueryResultsRead::Completion)
            && failure_reason(&failure).is_some_and(|reason| JOB_VERDICT_REASONS.contains(&reason))
        {
            let lead = format!("BigQuery query job {} failed", job_name(reference));
            relead(&mut failure, &error_context, &lead);
        }
        Err(failure)
    }

    /// Polls one job. A failed read says nothing about the job and a 404 means it is not yet
    /// visible, so both are retried on [`RetryDelays::POLL`].
    pub(crate) async fn poll(
        &self,
        operation: Option<&Operation>,
        reference: &JobReference,
        what: &str,
    ) -> Result<bytes::Bytes> {
        let url = self.job_url(reference, "");
        let send_context = format!("poll the BigQuery {what} job {}", job_name(reference));
        let error_context = format!("failed to {send_context}");
        let plan = Retry::for_job(self.poll_delays, &error_context);
        self.retrying(operation, plan, &mut || Request::get(&url, &send_context))
            .await
    }

    /// Lists the most recent child job of a script or procedure job; `jobs.list` sorts newest
    /// first. The full projection, because the minimal one omits `statementType` and
    /// `transactionInfo`. Needs `bigquery.jobs.list`, which running queries does not.
    pub(crate) async fn latest_child_job(
        &self,
        operation: Option<&Operation>,
        parent: &JobReference,
    ) -> Result<bytes::Bytes> {
        let url = format!(
            "{}?parentJobId={}&maxResults=1&projection=full",
            self.jobs_url(parent),
            escape_url_component(&parent.job_id)
        );
        let send_context = format!("list the child jobs of BigQuery job {}", job_name(parent));
        let error_context = format!("failed to {send_context}");
        let plan = Retry::new(self.poll_delays, &error_context);
        self.retrying(operation, plan, &mut || Request::get(&url, &send_context))
            .await
    }

    /// Requests cancellation of one exact job, ungated: a remote cancel is issued because the
    /// local operation was cancelled, so that same signal cannot stop it.
    pub(crate) async fn cancel(&self, reference: &JobReference, what: &str) -> Result<()> {
        let url = self.job_url(reference, "/cancel");
        let send_context = format!("cancel the BigQuery {what} job");
        let error_context = format!("failed to {send_context}");
        let plan = Retry::for_job(self.prompt_delays, &error_context);
        self.retrying(None, plan, &mut || Request::post_empty(&url, &send_context))
            .await
            .map(|_| ())
    }

    /// Fetches a job whose submission answer was ambiguous, retrying through propagation delay
    /// and transient failures. If the job never appears, `invisible` is returned, usually the
    /// original submission failure, or else the last `NOT_FOUND`. Any other final failure is
    /// returned as is, since the outcome is still unknown.
    pub(crate) async fn reconcile_submission(
        &self,
        operation: Option<&Operation>,
        reference: &JobReference,
        what: &str,
        invisible: Option<adbc_core::error::Error>,
    ) -> Result<bytes::Bytes> {
        let url = self.job_url(reference, "");
        let send_context = format!(
            "reconcile the submission of BigQuery {what} job {}",
            job_name(reference)
        );
        let error_context = format!("failed to {send_context}");
        let plan = Retry {
            attempts: MAX_RECONCILIATION_ATTEMPTS,
            invisible,
            ..Retry::for_job(self.prompt_delays, &error_context)
        };
        self.retrying(operation, plan, &mut || Request::get(&url, &send_context))
            .await
    }

    /// One bounded send/[`classify`]/retry loop, each backoff gated by `operation`.
    /// The borrowed request factory shares this async state machine across REST endpoints.
    ///
    /// A dry run creates no job, so it uses this loop. The loops that may create one stay
    /// separate from it and each other, because what each does around an attempt is its purpose:
    ///
    /// - [`JobClient::insert`] moves the cancellation identity through every outcome, and
    ///   reconciles an exhausted submission that may have created its job.
    /// - [`InferredSubmission::run`] sends and sleeps ungated, so that it can outlive a cancelled
    ///   caller, stops resubmitting once cancelled, and takes the job's location from a 409.
    /// - [`JobClient::query`] retains ambiguous attempts after cancellation, reads a replay's
    ///   409 as the job to read, and repeats a `responseTooLarge` once without a backoff.
    async fn retrying<'r>(
        &self,
        operation: Option<&Operation>,
        plan: Retry<'_>,
        request: &mut (dyn FnMut() -> Request<'r> + Send),
    ) -> Result<bytes::Bytes> {
        let mut invisible = plan.invisible;
        for attempt in 1..=plan.attempts {
            let attempted = request();
            let url = attempted.url;
            let response = send(operation, &self.credentials, &self.http, attempted).await;
            let (rejected, failure) = match classify(response, plan.error_context) {
                Ok(body) => return Ok(body),
                Err(rejected) => rejected,
            };
            // A 404 for an accepted job is propagation delay; only `existing_job` callers know
            // one was accepted.
            let invisible_job = plan.existing_job && failure.status == Status::NotFound;
            let class = if invisible_job {
                RetryClass::Transient
            } else if plan.existing_job && failure.vendor_code == 400 {
                // On an existing job, a 400 is the job's own failure mapped onto the read, so a
                // repeat only repeats the verdict. The Go client also retries these reasons only
                // on submission.
                RetryClass::Final
            } else {
                rejected
            };
            if class == RetryClass::Final {
                return Err(failure);
            }
            if attempt == plan.attempts {
                // A job that never became visible was not accepted; report the original
                // submission failure.
                return Err(match invisible.take() {
                    Some(original) if invisible_job => original,
                    _ => failure,
                });
            }
            diag!(
                Debug,
                "resending a BigQuery request to {url} after attempt {attempt} ({:?})",
                failure.status
            );
            cancellation::sleep(operation, plan.delays.delay(class, attempt)).await?;
        }
        unreachable!("the bounded retry loop returns on its final attempt")
    }

    fn jobs_url(&self, reference: &JobReference) -> String {
        format!(
            "{}/{}/jobs",
            self.projects,
            escape_url_component(&reference.project_id)
        )
    }

    fn job_url(&self, reference: &JobReference, suffix: &str) -> String {
        let mut url = format!(
            "{}/{}/jobs/{}{}",
            self.projects,
            escape_url_component(&reference.project_id),
            escape_url_component(&reference.job_id),
            suffix
        );
        append_parameter(&mut url, '?', "location", reference.location.as_deref());
        url
    }

    fn query_results_url(
        &self,
        reference: &JobReference,
        page_token: Option<&str>,
        max_results: u32,
    ) -> String {
        let mut url = format!(
            "{}/{}/queries/{}?maxResults={max_results}&timeoutMs={QUERY_COMPLETION_WAIT_MS}&formatOptions.useInt64Timestamp=true",
            self.projects,
            escape_url_component(&reference.project_id),
            escape_url_component(&reference.job_id),
        );
        append_parameter(&mut url, '&', "location", reference.location.as_deref());
        append_parameter(&mut url, '&', "pageToken", page_token);
        url
    }
}

/// Parameters of one [`JobClient::retrying`] loop.
struct Retry<'a> {
    /// Total attempts, including the first.
    attempts: u32,
    /// How the attempts are spaced. See [`RetryDelays`].
    delays: RetryDelays,
    /// The loop addresses a job known to be accepted, so a 404 means not yet visible and a 400
    /// is final.
    existing_job: bool,
    /// How a rejected attempt is reported: "failed to cancel the BigQuery copy job".
    error_context: &'a str,
    /// The failure to report instead when every attempt found no such job. See
    /// [`JobClient::reconcile_submission`].
    invisible: Option<adbc_core::error::Error>,
}

impl<'a> Retry<'a> {
    fn new(delays: RetryDelays, error_context: &'a str) -> Self {
        Self {
            attempts: MAX_ATTEMPTS,
            delays,
            existing_job: false,
            error_context,
            invisible: None,
        }
    }

    fn for_job(delays: RetryDelays, error_context: &'a str) -> Self {
        Self {
            existing_job: true,
            ..Self::new(delays, error_context)
        }
    }
}

/// Replaces the `from` that leads a rejection's message; a transport failure's has none.
fn relead(failure: &mut adbc_core::error::Error, from: &str, to: &str) {
    if failure.message.starts_with(from) {
        failure.message.replace_range(..from.len(), to);
    }
}

fn confirm_submission(operation: Option<&Operation>, reference: &JobReference) -> Result<()> {
    if let Some(operation) = operation {
        operation.register_job(reference)?;
    }
    Ok(())
}

/// Whether the submission attempt answered by `response` may have created the job.
///
/// It did not if no attempt reached BigQuery, which one still minting its token has not, or if
/// BigQuery refused it outright: a 4xx other than 409 (the job exists) and 408 (the request may
/// have been read whole), including the 401 `send` turns into an error to add its remedy. A 5xx
/// is ambiguous. `start` spans the submission's attempts, so a later attempt that fails before
/// reaching BigQuery errs towards the job existing.
pub(crate) fn may_have_created(
    start: &RequestStart,
    response: &Result<(reqwest::StatusCode, bytes::Bytes)>,
) -> bool {
    let refused = match response {
        Ok((status, _)) => {
            status.is_client_error()
                && *status != reqwest::StatusCode::CONFLICT
                && *status != reqwest::StatusCode::REQUEST_TIMEOUT
        }
        Err(failure) => failure.vendor_code == 401,
    };
    start.started() && !refused
}

/// Ends a stopped submission's provisional identity: confirmed when the job may exist, and
/// otherwise recorded as finished, so neither a cancel's retry nor the operation's end sends a
/// `jobs.cancel` that could only answer 404.
pub(crate) fn settle_submission(
    operation: Option<&Operation>,
    reference: &JobReference,
    may_exist: bool,
) {
    if let Some(operation) = operation {
        let _ = if may_exist {
            operation.register_job(reference)
        } else {
            operation.register_completed_job(reference)
        };
    }
}

/// A real `jobs.insert` without a location, which BigQuery runs where the data it reads or writes
/// is. Its answer names that location, and every later request about the job carries it.
///
/// When the answer is lost, the job is found without a new submission, stopping at the first
/// step that works:
///
/// 1. The retries replay the identical request, whose 409 names the job as
///    `project:location.job`. Job IDs are unique per location, and the identical request infers
///    the same location.
/// 2. `jobs.get` without a location, which finds a job in the `US` or `EU` multi-region.
/// 3. `jobs.get` in the location of each [`JobRequest::locatable_in`] dataset.
/// 4. `jobs.list` of the caller's recent jobs across regions, matching the exact job ID.
///
/// Otherwise BigQuery may still run the job, and the failure names it.
///
/// The submission owns its state and no request waits on the caller's operation, so a cancelled
/// caller can leave it to finish in the background: only its answer can say where the job to
/// cancel is. A cancelled submission is never sent again, since that could only start a job to
/// cancel.
struct InferredSubmission {
    client: JobClient,
    /// The caller's operation, which no request waits on. It stops resubmission once cancelled,
    /// and learns the job's identity as soon as a listing finds it.
    operation: Option<Operation>,
    reference: JobReference,
    body: bytes::Bytes,
    datasets: Vec<DatasetKey>,
    what: String,
    /// The job as [`JobClient::insert`] names it.
    job: String,
    /// Set once a submission may have reached BigQuery.
    start: RequestStart,
    /// Bounds the jobs.list search even if submission retries take a long time.
    submitted_at: SystemTime,
}

/// Why an [`InferredSubmission`] has no job with a known location.
struct Unlocated {
    failure: adbc_core::error::Error,
    /// Whether BigQuery may have accepted the job, which may then still run.
    job_may_exist: bool,
}

impl Unlocated {
    fn unknown(failure: adbc_core::error::Error) -> Self {
        Self {
            failure,
            job_may_exist: true,
        }
    }

    fn into_failure(self) -> adbc_core::error::Error {
        if self.job_may_exist {
            self.failure
        } else {
            with_job_verdict(self.failure, "not_created")
        }
    }
}

/// The job's identity with its location, and the job resource that named it.
type Located = std::result::Result<(JobReference, bytes::Bytes), Unlocated>;

impl InferredSubmission {
    /// Submits the job for a caller waiting under its operation.
    async fn insert(self) -> Result<bytes::Bytes> {
        let Some(operation) = self.operation.clone() else {
            return self
                .run()
                .await
                .map(|(_, body)| body)
                .map_err(Unlocated::into_failure);
        };
        // Until the location is known, a cancel addresses the job without one, which reaches a
        // job in the US or EU multi-region. The located identity replaces it.
        operation.register_provisional_job(&self.reference)?;
        let start = self.start.clone();
        let reference = self.reference.clone();
        let what = self.what.clone();
        let mut running = Box::pin(self.run());
        tokio::select! {
            biased;
            () = operation.cancelled() => {
                if start.started() {
                    let operation = operation.clone();
                    let task: DetachedTask = Box::pin(async move {
                        match running.await {
                            // Registration sees the cancelled operation and cancels the job.
                            Ok((located, _)) => {
                                let _ = operation.register_job(&located);
                            }
                            // Service error messages may contain SQL, so only the status.
                            Err(unlocated) if unlocated.job_may_exist => diag!(
                                Warn,
                                "the cancelled BigQuery {what} job {} may keep running and \
                                 billing: the driver could not recover the submitted job \
                                 (status {:?})",
                                job_name(&reference),
                                unlocated.failure.status
                            ),
                            Err(_) => {}
                        }
                    });
                    tokio::spawn(task);
                } else {
                    let _ = operation.register_completed_job(&reference);
                }
                Err(operation.cancelled_error())
            }
            located = &mut running => {
                let (located, body) = located.map_err(|unlocated| {
                    if !unlocated.job_may_exist {
                        let _ = operation.register_completed_job(&reference);
                    }
                    unlocated.into_failure()
                })?;
                operation.register_job(&located)?;
                Ok(body)
            }
        }
    }

    async fn run(self) -> Located {
        let url = self.client.jobs_url(&self.reference);
        let send_context = format!("submit the BigQuery {}", self.job);
        let error_context = format!("BigQuery rejected the {}", self.job);
        let mut attempt = 0;
        let mut may_exist = false;
        let ambiguous = loop {
            attempt += 1;
            let request = Request::post_json(&url, self.body.clone(), &send_context)
                .with_start_signal(&self.start);
            let response = send(None, &self.client.credentials, &self.client.http, request).await;
            may_exist |= may_have_created(&self.start, &response);
            let (class, failure) = match response {
                Ok((reqwest::StatusCode::CONFLICT, body)) => {
                    let failure =
                        service_error(&error_context, reqwest::StatusCode::CONFLICT, &body);
                    let Some(located) = duplicate(&body, &self.reference) else {
                        break failure;
                    };
                    // Keep the known location cancellable even if fetching the job fails.
                    let _ = confirm_submission(self.operation.as_ref(), &located);
                    // The job exists there, so one that never appears there is still the
                    // original submission's failure.
                    return match self
                        .client
                        .reconcile_submission(None, &located, &self.what, Some(failure))
                        .await
                    {
                        Ok(body) => found(&located, body),
                        Err(failure) => Err(Unlocated::unknown(failure)),
                    };
                }
                answer => match classify(answer, &error_context) {
                    Ok(body) => {
                        return match located_by(&self.reference, &body, ACCEPTED) {
                            Ok(located) => Ok((located, body)),
                            // Accepted, but the answer does not say where.
                            Err(failure) => self.locate(failure).await,
                        };
                    }
                    Err(rejected) => rejected,
                },
            };
            if class == RetryClass::Final {
                return Err(Unlocated {
                    failure,
                    job_may_exist: may_exist,
                });
            }
            if attempt == MAX_ATTEMPTS || self.cancelled() {
                break failure;
            }
            diag!(
                Debug,
                "retrying an ambiguous BigQuery job submission without a location, with the same \
                 job ID {}, after attempt {attempt} ({:?})",
                self.reference.job_id,
                failure.status
            );
            tokio::time::sleep(self.client.service_delays.delay(class, attempt)).await;
            if self.cancelled() {
                break failure;
            }
        };
        if !may_exist {
            return Err(Unlocated {
                failure: ambiguous,
                job_may_exist: false,
            });
        }
        self.locate(ambiguous).await
    }

    fn cancelled(&self) -> bool {
        self.operation.as_ref().is_some_and(Operation::is_cancelled)
    }

    /// Finds a job that BigQuery may have accepted, at steps 2–4 of [`InferredSubmission`].
    async fn locate(&self, original: adbc_core::error::Error) -> Located {
        match self
            .client
            .reconcile_submission(None, &self.reference, &self.what, None)
            .await
        {
            Ok(body) => return found(&self.reference, body),
            Err(failure) if failure.status == Status::NotFound => {}
            Err(failure) => return Err(Unlocated::unknown(failure)),
        }
        if let Some(locations) = &self.client.dataset_locations {
            let mut searched = Vec::with_capacity(self.datasets.len());
            for dataset in &self.datasets {
                let location = match locations.get(None, dataset).await {
                    Ok(location) => location,
                    Err(failure) => {
                        diag!(
                            Debug,
                            "could not look for BigQuery job {} in the location of dataset {}.{} \
                             (status {:?})",
                            job_name(&self.reference),
                            dataset.0,
                            dataset.1,
                            failure.into_error("").status
                        );
                        continue;
                    }
                };
                // The lookup without a location already searched the multi-regions.
                if location.eq_ignore_ascii_case("US")
                    || location.eq_ignore_ascii_case("EU")
                    || searched.contains(&location)
                {
                    continue;
                }
                let candidate = JobReference::new(
                    &self.reference.project_id,
                    Some(&location),
                    self.reference.job_id.clone(),
                );
                searched.push(location);
                match self
                    .client
                    .reconcile_submission(None, &candidate, &self.what, None)
                    .await
                {
                    Ok(body) => return found(&candidate, body),
                    Err(failure) if failure.status == Status::NotFound => {}
                    Err(failure) => return Err(Unlocated::unknown(failure)),
                }
            }
        }
        let mut failure = match self.locate_by_listing().await {
            Ok(Some((reference, body))) => return Ok((reference, body)),
            Ok(None) => original,
            Err(failure) => failure,
        };
        append_remedy(
            &mut failure,
            &format!(
                "BigQuery may still run job {}; recovery did not complete; \
                 check the job before running it again, or set {}",
                job_name(&self.reference),
                BigQueryDatabaseOption::Location.as_str()
            ),
        );
        Err(Unlocated::unknown(with_job_link(
            failure,
            Some(&self.reference),
        )))
    }

    async fn locate_by_listing(&self) -> Result<Option<(JobReference, bytes::Bytes)>> {
        let mut reference = self.reference.clone();
        tokio::time::timeout(JOB_SEARCH_TIMEOUT, self.search_recent_jobs(&mut reference))
            .await
            .unwrap_or_else(|_| {
                Err(error(
                    format!(
                        "BigQuery job location search deadline exceeded after {} s",
                        JOB_SEARCH_TIMEOUT.as_secs()
                    ),
                    Status::Timeout,
                ))
            })
            .map_err(|failure| with_job_link(failure, Some(&reference)))
    }

    /// Listing needs `bigquery.jobs.list` but no location. Keep the search independent of local
    /// cancellation: finding the reference lets us cancel remotely.
    async fn search_recent_jobs(
        &self,
        resolved: &mut JobReference,
    ) -> Result<Option<(JobReference, bytes::Bytes)>> {
        let earliest = self
            .submitted_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .saturating_sub(JOB_SEARCH_CLOCK_SKEW)
            .as_millis();
        let base = format!(
            "{}?allUsers=false&projection=minimal&maxResults=100&minCreationTime={earliest}",
            self.client.jobs_url(&self.reference)
        );
        let send_context = format!(
            "find the location of BigQuery job {}",
            job_name(&self.reference)
        );
        let error_context = format!("failed to {send_context}");
        let mut pages = 0;
        for scan in 1..=MAX_RECONCILIATION_ATTEMPTS {
            let mut page_token = String::new();
            loop {
                if pages == MAX_JOB_SEARCH_PAGES {
                    return Err(error(
                        format!("{error_context}: jobs.list exceeded the page limit"),
                        Status::IO,
                    ));
                }
                pages += 1;
                let url = if page_token.is_empty() {
                    base.clone()
                } else {
                    format!("{base}&pageToken={}", escape_url_component(&page_token))
                };
                let plan = Retry::new(self.client.prompt_delays, &error_context);
                let body = self
                    .client
                    .retrying(None, plan, &mut || Request::get(&url, &send_context))
                    .await?;
                let page: RecentJobs = decode_json(&body, "the BigQuery jobs.list response")?;
                for job in page.jobs {
                    let Some(reference) = job.job_reference else {
                        continue;
                    };
                    if reference.job_id != self.reference.job_id
                        || !matches_project(&self.reference.project_id, &reference.project_id)
                    {
                        continue;
                    }
                    let located = self
                        .reference
                        .confirm(Some(&reference), "the BigQuery jobs.list response")?;
                    if located.location.is_none() {
                        continue;
                    }
                    *resolved = located;
                    // A subsequent jobs.get failure must not prevent cancelling the job whose
                    // identity we now know. Registration also handles an already-cancelled caller.
                    let _ = confirm_submission(self.operation.as_ref(), resolved);
                    let body = self
                        .client
                        .reconcile_submission(None, resolved, &self.what, None)
                        .await?;
                    return found(resolved, body)
                        .map(Some)
                        .map_err(|unlocated| unlocated.failure);
                }
                page_token = page.next_page_token;
                if page_token.is_empty() {
                    break;
                }
            }
            // A just-created job, or a temporarily unreachable region, may be absent from a
            // complete listing. Retry the scan without ever resubmitting the cancelled job.
            if scan < MAX_RECONCILIATION_ATTEMPTS {
                tokio::time::sleep(self.client.prompt_delays.delay(RetryClass::Transient, scan))
                    .await;
            }
        }
        Ok(None)
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecentJobs {
    #[serde(default)]
    jobs: Vec<NamedJob>,
    #[serde(default)]
    next_page_token: String,
}

/// A job resource, reduced to the identity it names.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct NamedJob {
    #[serde(default)]
    job_reference: Option<JobReference>,
}

/// How an accepted submission's answer is named in errors.
const ACCEPTED: &str = "the BigQuery jobs.insert response";

/// `candidate`, which a fetched job resource `body` describes, with the location it names.
fn found(candidate: &JobReference, body: bytes::Bytes) -> Located {
    located_by(candidate, &body, "the BigQuery jobs.get response")
        .map(|located| (located, body))
        .map_err(Unlocated::unknown)
}

/// The identity `body`, a job resource for `requested`, gives the job, which must name its
/// location.
fn located_by(requested: &JobReference, body: &[u8], response: &str) -> Result<JobReference> {
    let job: NamedJob = decode_json(body, response)?;
    let located = requested.confirm(job.job_reference.as_ref(), response)?;
    if located.location.is_none() {
        return Err(invalid_data(format!(
            "{response} names no location for job {}",
            job_name(&located)
        )));
    }
    Ok(located)
}

/// The job in `project_id` a 409 says already exists, with its location.
///
/// BigQuery names it only in the message, as `Already Exists: Job project:location.job`, so any
/// other wording names no job.
fn duplicate_in(body: &[u8], project_id: &str) -> Option<JobReference> {
    let message = reason_message(body, "duplicate")?;
    let (returned_project, job) = message
        .strip_prefix("Already Exists: Job ")?
        .rsplit_once(':')?;
    if !matches_project(project_id, returned_project) {
        return None;
    }
    let (location, job_id) = job.split_once('.')?;
    let valid = |part: &str, also: u8| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == also)
    };
    (valid(location, b'-') && valid(job_id, b'_'))
        .then(|| JobReference::new(returned_project, Some(location), job_id.to_owned()))
}

/// [`duplicate_in`], when the 409 names exactly `reference`'s job.
fn duplicate(body: &[u8], reference: &JobReference) -> Option<JobReference> {
    duplicate_in(body, &reference.project_id).filter(|job| job.job_id == reference.job_id)
}

/// Appends `{separator}{name}={value}` (`separator` is `?` or `&`) when `value` is non-empty.
fn append_parameter(url: &mut String, separator: char, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        write!(url, "{separator}{name}={}", escape_url_component(value))
            .expect("writing to a String cannot fail");
    }
}

#[cfg(test)]
mod tests;
