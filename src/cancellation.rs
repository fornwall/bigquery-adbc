//! Thread-safe tracking and cancellation of an active BigQuery operation.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use adbc_core::CancelHandle;
use adbc_core::error::{Result, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use tokio::sync::Notify;

use crate::clients::ClientState;
use crate::diag::diag;
use crate::error::{error, invalid_state, job_name};
use crate::rest::jobs::JobReference;
use crate::runtime::{DetachedTask, INHERITED_ACROSS_FORK, SharedRuntime};

#[derive(Debug)]
pub(crate) struct Cancellation {
    runtime: SharedRuntime,
    clients: Arc<ClientState>,
    scope: &'static str,
    cancel_timeout: Duration,
    /// Every operation still running on this object, in the order they began. A list because
    /// partition streams run concurrently on one connection; Cancel ends them all.
    active: Mutex<Vec<Weak<OperationInner>>>,
}

impl Cancellation {
    pub(crate) fn new(
        runtime: SharedRuntime,
        clients: Arc<ClientState>,
        scope: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            clients,
            scope,
            cancel_timeout: Duration::from_secs(30),
            active: Mutex::new(Vec::new()),
        })
    }

    fn active(&self) -> MutexGuard<'_, Vec<Weak<OperationInner>>> {
        // The active list is plain data a panic cannot leave torn; recover a poisoned guard.
        self.active.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begins an operation that supersedes the ones already running on this object.
    ///
    /// adbc.h: executing a statement, preparing it, or asking for its schema "invalidates any
    /// prior result sets", so a statement runs one operation at a time.
    pub(crate) fn begin(self: &Arc<Self>) -> Operation {
        self.start(true)
    }

    /// Begins an operation that runs alongside every other operation on this object.
    ///
    /// adbc.h lets partitions be "fetched in parallel" from one connection, so a later call must
    /// not cut a partition stream short. Cancel still ends them.
    pub(crate) fn begin_concurrent(self: &Arc<Self>) -> Operation {
        self.start(false)
    }

    /// Runs a superseding operation to completion, finishing it on success or a returned error.
    /// Work that returns a result stream must use [`Self::begin`] and give the stream ownership
    /// of the operation instead.
    pub(crate) fn run<T>(
        self: &Arc<Self>,
        work: impl FnOnce(&Operation) -> Result<T>,
    ) -> Result<T> {
        let operation = self.begin();
        let result = work(&operation);
        operation.finish();
        result
    }

    fn start(self: &Arc<Self>, supersedes: bool) -> Operation {
        let inner = Arc::new(OperationInner {
            owner: self.clone(),
            scope: self.scope,
            supersedable: supersedes,
            cancelled: CancelFlag::default(),
            finished: AtomicBool::new(false),
            job: Mutex::new(None),
        });
        // Dropped after the lock: a concurrent drop can leave an upgraded entry as the last
        // reference, and `OperationInner::drop` takes the same lock.
        let mut previous = Vec::new();
        {
            let mut active = self.active();
            active.retain(|operation| {
                let Some(operation) = operation.upgrade() else {
                    return false;
                };
                let retain = !supersedes || !operation.supersedable;
                previous.push(operation);
                retain
            });
            active.push(Arc::downgrade(&inner));
        }
        for previous in previous {
            if supersedes && previous.supersedable {
                previous.cancel_local();
                self.cancel_job_in_background(&previous, Some(&previous));
            }
        }
        Operation { inner }
    }

    fn cancel_job_in_background(
        self: &Arc<Self>,
        operation: &OperationInner,
        retained: Option<&Arc<OperationInner>>,
    ) {
        let Some(reference) = operation.job_to_cancel() else {
            // If registration races with cancellation, register_job observes the local flag and
            // performs the remote cancellation itself once the reference becomes available.
            return;
        };
        let retained = retained.map(Arc::downgrade);
        self.cancel_in_background(
            reference,
            Box::new(move |reference| {
                retained
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .and_then(|operation| operation.retry_failed_cancel(reference))
            }),
        );
    }

    /// Delivers a cancellation already claimed while registering a late job reference.
    fn cancel_claimed_in_background(
        self: &Arc<Self>,
        operation: &Arc<OperationInner>,
        reference: JobReference,
    ) {
        // Retain the finite operation state until this cancel generation (and its one possible
        // post-confirm retry) finishes. The caller can return Cancelled and drop its handle while
        // the provisional cancel is still in flight; weak retention would lose the confirmation
        // bit before a visibility failure could consume it.
        let operation = operation.clone();
        self.cancel_in_background(
            reference,
            Box::new(move |reference| operation.retry_failed_cancel(reference)),
        );
    }

    /// Spawns cancellation delivery for a path with no caller waiting for its result.
    fn cancel_in_background(
        self: &Arc<Self>,
        mut reference: JobReference,
        mut retry: Box<RetryCancel<'static>>,
    ) {
        let cancellation = self.clone();
        let task: DetachedTask = Box::pin(async move {
            // Superseding, abandoning, and late registration have no caller to report a final
            // request failure to, so their delivery is best effort.
            if let Err(failure) = cancellation
                .cancel_until_settled(&mut reference, &mut *retry)
                .await
            {
                // Service error messages may contain SQL or other user data, so only the status.
                diag!(
                    Warn,
                    "failed to cancel the BigQuery job {} of a {} operation (status {:?}); it may \
                     keep running and billing",
                    job_name(&reference),
                    cancellation.scope,
                    failure.status
                );
            }
        });
        self.runtime.spawn(task);
    }

    /// Delivers a job cancellation and its one permitted post-confirmation retry.
    ///
    /// Background cancellation and the synchronous `Cancel` entry point differ in how they retain
    /// state and report a final failure, but the request loop and the rule that a failed
    /// provisional cancellation may be rearmed are the same.
    async fn cancel_until_settled(
        &self,
        reference: &mut JobReference,
        retry: &mut RetryCancel<'_>,
    ) -> Result<()> {
        // Bound credentials, HTTP requests and every retry together, independently of the
        // operation being cancelled. A per-request timeout would multiply across retries.
        let deadline = tokio::time::Instant::now() + self.cancel_timeout;
        loop {
            let result = tokio::time::timeout_at(deadline, self.cancel_job(reference))
                .await
                .unwrap_or_else(|_| {
                    Err(error(
                        format!(
                            "BigQuery job cancellation deadline exceeded after {} s for job {}; \
                             the job may still be running and billing, so stop it with \
                             `bq cancel`",
                            self.cancel_timeout.as_secs(),
                            job_name(reference),
                        ),
                        Status::Timeout,
                    ))
                });
            match result {
                Ok(()) => return Ok(()),
                Err(failure) => {
                    let Some(next) = retry(reference) else {
                        return Err(failure);
                    };
                    *reference = next;
                }
            }
        }
    }

    fn finish(&self, operation: &OperationInner) {
        self.active().retain(|active| {
            active.strong_count() > 0 && !std::ptr::eq(active.as_ptr(), operation)
        });
    }

    async fn cancel_job(&self, reference: &JobReference) -> Result<()> {
        self.clients
            .rest_job_client(&self.runtime)?
            .cancel(reference, self.scope)
            .await
    }

    fn try_cancel(self: &Arc<Self>) -> Result<()> {
        let mut operations = self
            .active()
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        // Drop rejected references outside the lock: OperationInner::drop also takes it.
        operations.retain(|operation| !operation.finished.load(Ordering::Acquire));
        if operations.is_empty() {
            return Err(invalid_state(format!(
                "no active BigQuery {} operation to cancel",
                self.scope
            )));
        }

        // Local cancellation is one phase for the whole object. A slow first remote request must
        // never leave a later concurrent partition reader running while Cancel is already active.
        let mut jobs = Vec::with_capacity(operations.len());
        for operation in &operations {
            operation.cancel_local();
            if let Some(reference) = operation.job_to_cancel() {
                let provisional = !operation.job_is_confirmed(&reference);
                jobs.push((operation.clone(), reference, provisional));
            }
        }

        // Then deliver every claimed remote cancellation. The first synchronous failure is
        // reported after all local operations have already stopped.
        let mut result = Ok(());
        for (operation, mut reference, mut provisional) in jobs {
            // On a host thread (the cdylib), the cancel RPC runs here and reports its outcome.
            // Inside a Tokio host such as dbt-core 2.0 it goes to the driver's runtime,
            // unreported: a current-thread runtime cannot block, and a worker should not block for
            // up to the cancel deadline. The claim above still delivers it exactly once.
            if tokio::runtime::Handle::try_current().is_ok() {
                self.cancel_claimed_in_background(&operation, reference);
            } else {
                let outcome = self.runtime.block_on(self.cancel_until_settled(
                    &mut reference,
                    &mut |reference| {
                        let retry = operation.retry_failed_cancel(reference);
                        if retry.is_some() {
                            provisional = false;
                        }
                        retry
                    },
                ));
                if let Err(mut source) = outcome
                    && result.is_ok()
                    && !(provisional && source.status == Status::NotFound)
                {
                    // adbc.h requires UNKNOWN when cancellation could not be delivered.
                    source.status = Status::Unknown;
                    result = Err(source);
                }
            }
        }
        result
    }
}

/// Answers the reference to cancel again after a failed cancellation, or `None` to give up.
/// A trait object, so the cancellation loop is compiled once rather than per caller.
type RetryCancel<'a> = dyn FnMut(&JobReference) -> Option<JobReference> + Send + 'a;

pub(crate) struct OperationCancelHandle(pub(crate) Arc<Cancellation>);

impl CancelHandle for OperationCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        self.0.try_cancel()
    }
}

/// Awaits `future` under the caller's cancellation, or plainly when it has none to observe.
///
/// Every await this driver makes on behalf of an operation goes through here, so a statement that
/// is cancelled stops waiting the same way whether it is waiting on the network, on a backoff, or
/// on a driver-wide result-work permit.
pub(crate) async fn gate<F: Future>(operation: Option<&Operation>, future: F) -> Result<F::Output> {
    match operation {
        Some(operation) => operation.wait(future).await,
        None => Ok(future.await),
    }
}

/// Sleeps `delay` under the caller's cancellation. See [`gate`].
pub(crate) async fn sleep(operation: Option<&Operation>, delay: Duration) -> Result<()> {
    gate(operation, tokio::time::sleep(delay)).await
}

#[derive(Clone)]
pub(crate) struct Operation {
    inner: Arc<OperationInner>,
}

impl Operation {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.is_set()
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(self.cancelled_error())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn wait<F: Future>(&self, future: F) -> Result<F::Output> {
        self.check()?;
        tokio::select! {
            biased;
            () = self.cancelled() => Err(self.cancelled_error()),
            result = future => Ok(result),
        }
    }

    pub(crate) async fn cancelled(&self) {
        self.inner.cancelled.wait().await;
    }

    /// Publishes a job identity that may not yet be visible because its mutating request is still
    /// in flight. Once the operation is cancelled this refuses instead, and the caller must not
    /// send the job: there is then nothing to cancel.
    pub(crate) fn register_provisional_job(&self, reference: &JobReference) -> Result<()> {
        self.publish_job(reference, JobState::Provisional)
    }

    /// Publishes a server-confirmed job identity.
    ///
    /// This also confirms that submission reached its terminal transport outcome for an already
    /// registered provisional identity. A provisional cancel that exhausted before the job became
    /// visible gets one newly bounded cancellation generation after this point.
    pub(crate) fn register_job(&self, reference: &JobReference) -> Result<()> {
        self.publish_job(reference, JobState::Running)
    }

    /// Records terminal status without ending the local operation or its result reader.
    pub(crate) fn register_completed_job(&self, reference: &JobReference) -> Result<()> {
        self.publish_job(reference, JobState::Finished)
    }

    fn publish_job(&self, reference: &JobReference, state: JobState) -> Result<()> {
        let cancel = self.inner.register_job(reference, state);
        if let Some(reference) = cancel {
            self.inner
                .owner
                .cancel_claimed_in_background(&self.inner, reference);
        }
        self.check()
    }

    pub(crate) fn cancelled_error(&self) -> adbc_core::error::Error {
        error(
            format!("BigQuery {} operation was cancelled", self.inner.scope),
            Status::Cancelled,
        )
    }

    pub(crate) fn finish(&self) {
        // A call ending (including an error or reader EOF) does not prove its remote job
        // finished. Terminal responses mark that separately; everything else needs cleanup.
        if self.inner.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.cancel_local();
        self.inner
            .owner
            .cancel_job_in_background(&self.inner, Some(&self.inner));
        self.inner.owner.finish(&self.inner);
    }
}

#[derive(Debug)]
struct OperationInner {
    // `Cancellation::active` holds only weak pointers, so this forms no cycle. It lets a late
    // jobs.query response register and cancel its remote job after the statement or connection
    // is released.
    owner: Arc<Cancellation>,
    scope: &'static str,
    /// Whether a later operation on the same object supersedes this one.
    supersedable: bool,
    cancelled: CancelFlag,
    finished: AtomicBool,
    job: Mutex<Option<RegisteredJob>>,
}

impl OperationInner {
    fn job(&self) -> MutexGuard<'_, Option<RegisteredJob>> {
        // The job slot is a plain Option a panic cannot leave torn; recover a poisoned guard.
        self.job.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cancel_local(&self) {
        self.cancelled.set();
    }

    /// Publishes a reference and claims its remote cancel when cancellation already won.
    ///
    /// A later response may add the location to an otherwise identical job. That canonical
    /// identity needs its own cancellation request: the earlier location-less request may have
    /// reached the wrong region. Re-registering the exact same identity remains idempotent here.
    fn register_job(&self, reference: &JobReference, state: JobState) -> Option<JobReference> {
        let mut job = self.job();
        // Decided under the job lock, which a cancel takes after setting the flag: either the
        // cancel finds this identity, or this sees the flag and leaves nothing to cancel.
        if state == JobState::Provisional && self.cancelled.is_set() {
            return None;
        }
        // Resolving a project number or location cannot make a completed job run again.
        let state = match job.as_ref() {
            Some(job)
                if job.reference.matches_identity(reference)
                    && job
                        .reference
                        .location
                        .as_deref()
                        .zip(reference.location.as_deref())
                        .is_none_or(|(old, new)| old.eq_ignore_ascii_case(new)) =>
            {
                job.state.max(state)
            }
            _ => state,
        };
        if job
            .as_ref()
            .is_none_or(|registered| registered.reference != *reference)
        {
            *job = Some(RegisteredJob {
                reference: reference.clone(),
                cancel_started: false,
                state,
                confirmation_retry_started: false,
            });
        } else if let Some(registered) = job.as_mut() {
            registered.state = state;
        }
        if !self.cancelled.is_set() {
            return None;
        }
        claim_remote_cancel(&mut job)
    }

    /// Claims at most one remote cancellation for the currently registered identity.
    fn job_to_cancel(&self) -> Option<JobReference> {
        claim_remote_cancel(&mut self.job())
    }

    fn job_is_confirmed(&self, reference: &JobReference) -> bool {
        self.job()
            .as_ref()
            .is_some_and(|job| job.reference == *reference && job.state != JobState::Provisional)
    }

    /// Rearms a failed provisional cancel, and claims the one post-confirmation retry when the
    /// submission outcome became known while that first cancel was in flight.
    fn retry_failed_cancel(&self, reference: &JobReference) -> Option<JobReference> {
        let mut job = self.job();
        let registered = job.as_mut()?;
        if registered.reference != *reference || registered.state == JobState::Finished {
            return None;
        }
        registered.cancel_started = false;
        if !self.cancelled.is_set()
            || registered.state == JobState::Provisional
            || registered.confirmation_retry_started
        {
            return None;
        }
        registered.cancel_started = true;
        registered.confirmation_retry_started = true;
        Some(registered.reference.clone())
    }
}

/// A one-way cancellation signal that can be awaited.
#[derive(Debug, Default)]
pub(crate) struct CancelFlag {
    set: AtomicBool,
    notify: Notify,
}

impl CancelFlag {
    pub(crate) fn set(&self) {
        self.set.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_set(&self) -> bool {
        self.set.load(Ordering::Acquire)
    }

    /// Returns once the flag is set.
    pub(crate) async fn wait(&self) {
        // `notify_waiters` reaches every future created before it, even before first poll.
        // Create the future before checking the flag so a concurrent `set` cannot be lost.
        let notified = self.notify.notified();
        if !self.is_set() {
            notified.await;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum JobState {
    Provisional,
    Running,
    Finished,
}

#[derive(Debug)]
struct RegisteredJob {
    reference: JobReference,
    cancel_started: bool,
    state: JobState,
    confirmation_retry_started: bool,
}

fn claim_remote_cancel(job: &mut Option<RegisteredJob>) -> Option<JobReference> {
    let registered = job.as_mut()?;
    if registered.cancel_started || registered.state == JobState::Finished {
        return None;
    }
    registered.cancel_started = true;
    if registered.state != JobState::Provisional {
        registered.confirmation_retry_started = true;
    }
    Some(registered.reference.clone())
}

impl Drop for OperationInner {
    fn drop(&mut self) {
        let was_finished = self.finished.swap(true, Ordering::AcqRel);
        if !was_finished {
            self.owner.cancel_job_in_background(self, None);
        }
        self.owner.finish(self);
    }
}

pub(crate) fn operation_reader(
    inner: Box<dyn RecordBatchReader + Send>,
    operation: Operation,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    tracked_operation_reader(inner, operation, true)
}

pub(crate) fn buffered_operation_reader(
    inner: Box<dyn RecordBatchReader + Send>,
    operation: Operation,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    // ADBC lets cancellation leave an already-produced stream readable, so this reader keeps
    // yielding but holds the operation until it is consumed or dropped. Readers that can do more
    // remote work must use `operation_reader`.
    tracked_operation_reader(inner, operation, false)
}

fn tracked_operation_reader(
    inner: Box<dyn RecordBatchReader + Send>,
    operation: Operation,
    return_cancellation: bool,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let schema = inner.schema();
    Box::new(OperationReader {
        inner: Some(inner),
        operation,
        schema,
        return_cancellation,
    })
}

struct OperationReader {
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    operation: Operation,
    schema: SchemaRef,
    return_cancellation: bool,
}

impl OperationReader {
    fn finish(&mut self) {
        self.operation.finish();
        // A terminal wrapper remains usable for schema queries, but must not retain queued
        // results or unread parameter buffers until the caller releases the stream itself.
        self.inner = None;
    }

    /// Ends a cancellation-returning reader when its operation was cancelled, yielding the item
    /// that reports the cancellation.
    fn cancellation_item(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        if !self.return_cancellation {
            return None;
        }
        let source = self.operation.check().err()?;
        self.finish();
        Some(Err(ArrowError::ExternalError(Box::new(source))))
    }
}

impl Iterator for OperationReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.as_ref()?;
        // Every reader handed out passes here. In a `fork` child, nothing can feed or close a
        // buffered reader's channel, and any other wait on the runtime would panic.
        if self.operation.inner.owner.runtime.orphaned() {
            let failure = invalid_state(INHERITED_ACROSS_FORK);
            return Some(Err(ArrowError::ExternalError(Box::new(failure))));
        }
        if let Some(cancelled) = self.cancellation_item() {
            return Some(cancelled);
        }

        let batch = self.inner.as_mut()?.next();
        if let Some(cancelled) = self.cancellation_item() {
            return Some(cancelled);
        }
        if batch.is_none() || batch.as_ref().is_some_and(|batch| batch.is_err()) {
            self.finish();
        }
        batch
    }
}

impl RecordBatchReader for OperationReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for OperationReader {
    fn drop(&mut self) {
        if self.inner.is_some() {
            self.operation.finish();
        }
    }
}

#[cfg(test)]
mod tests;
