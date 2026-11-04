//! Synchronous-to-asynchronous bridge shared by the entire driver hierarchy.

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};
use tokio::sync::Semaphore;
use tokio::task::JoinError;

use crate::error::error;

pub(crate) type SharedRuntime = Arc<DriverRuntime>;

/// A boxed spawned task with no output. Tokio instantiates its task harness per future type and
/// scheduler flavor, so boxing lets the driver's detached tasks share the instantiation the HTTP
/// stack already links.
pub(crate) type DetachedTask = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Debug)]
pub(crate) struct DriverRuntime {
    runtime: Option<Runtime>,
    result_conversion_permits: Arc<Semaphore>,
    result_conversion_concurrency: usize,
}

impl DriverRuntime {
    pub(crate) fn result_conversion_permits(&self) -> Arc<Semaphore> {
        self.result_conversion_permits.clone()
    }

    /// How many result batches may convert at once. A semaphore reports only its *free* permits,
    /// which is whatever concurrent queries have left, so the count is carried alongside it.
    pub(crate) fn result_conversion_concurrency(&self) -> usize {
        self.result_conversion_concurrency
    }

    /// Blocks on `future`, erasing its type to share Tokio's blocking machinery across callers.
    /// This shadows [`Runtime::block_on`] through [`Deref`]. Stack pinning keeps type erasure
    /// allocation-free; the future cannot outlive this synchronous call.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        let mut output = None;
        self.block_on_erased(std::pin::pin!(async {
            output = Some(future.await);
        }));
        output.expect("the erased future stores its output before it completes")
    }

    fn block_on_erased(&self, future: Pin<&mut dyn Future<Output = ()>>) {
        let runtime: &Runtime = self;
        block_in_place(|| runtime.block_on(future));
    }
}

/// Hands a multithreaded Tokio worker back to its host before blocking. Ordinary threads run
/// directly; callers in a current-thread runtime must use `spawn_blocking` instead.
pub(crate) fn block_in_place<T>(work: impl FnOnce() -> T) -> T {
    if Handle::try_current().is_ok_and(|host| host.runtime_flavor() == RuntimeFlavor::MultiThread) {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

/// Runs one CPU-heavy result operation on Tokio's blocking pool while holding its already-acquired
/// driver-wide permit.
///
/// The permit moves into the closure, so dropping the awaiting future (for example when an ADBC
/// operation is cancelled) cannot release it while the synchronous work still runs.
pub(crate) async fn run_bounded_result_work<T, F>(
    permit: tokio::sync::OwnedSemaphorePermit,
    work: F,
) -> std::result::Result<T, JoinError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
}

impl Deref for DriverRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        self.runtime
            .as_ref()
            .expect("the driver runtime is available until its destructor runs")
    }
}

impl Drop for DriverRuntime {
    fn drop(&mut self) {
        // Tokio's default `Runtime` destructor waits indefinitely for started blocking tasks and
        // panics in an async context. The last owner can be any driver object, including a result
        // reader dropped inside a host's runtime, so shut down in the background.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

// One runtime per driver. Its workers carry the I/O of concurrent queries and the 16 `tables.get`
// decodes `TABLE_METADATA_CONCURRENCY` fans out, so the count follows the host: at least two so
// independent readers progress in parallel, at most `MAX_WORKER_THREADS` because the workers
// mostly park on I/O. CPU-heavy Arrow conversion runs on the blocking pool under its own permits.
const MIN_WORKER_THREADS: usize = 2;
const MAX_WORKER_THREADS: usize = 8;

fn worker_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(MIN_WORKER_THREADS, usize::from)
        .clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

pub(crate) fn new_runtime() -> Result<SharedRuntime> {
    build_runtime(std::thread::available_parallelism().map_or(1, usize::from))
}

#[cfg(test)]
pub(crate) fn new_runtime_with_result_conversion_concurrency(
    result_conversion_concurrency: usize,
) -> Result<SharedRuntime> {
    build_runtime(result_conversion_concurrency)
}

/// Install rustls's `ring` crypto provider as the process default.
///
/// The Google clients are compiled without their default aws-lc-rs provider (see `Cargo.toml`),
/// so without one every TLS handshake fails. Every path (the FFI entry, Rust library users, tests)
/// builds a [`SharedRuntime`] before any TLS use, so the provider is installed here. A provider a
/// host process already installed is kept; any provider will do.
pub(crate) fn install_crypto_provider() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn build_runtime(result_conversion_concurrency: usize) -> Result<SharedRuntime> {
    install_crypto_provider();
    Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .thread_name("bigquery-adbc")
        .build()
        .map(|runtime| {
            Arc::new(DriverRuntime {
                runtime: Some(runtime),
                result_conversion_permits: Arc::new(Semaphore::new(result_conversion_concurrency)),
                result_conversion_concurrency,
            })
        })
        .map_err(|source| {
            error(
                format!("failed to create the BigQuery driver Tokio runtime: {source}"),
                Status::Internal,
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio::runtime::Builder;

    use super::{
        MAX_WORKER_THREADS, MIN_WORKER_THREADS, new_runtime,
        new_runtime_with_result_conversion_concurrency, worker_threads,
    };

    /// The worker count follows the host and stays inside the clamp on any machine, including a
    /// single-core one.
    #[test]
    fn runtime_worker_count_is_clamped_to_the_host() {
        let workers = worker_threads();
        assert!(
            (MIN_WORKER_THREADS..=MAX_WORKER_THREADS).contains(&workers),
            "{workers}"
        );
        let available =
            std::thread::available_parallelism().map_or(MIN_WORKER_THREADS, usize::from);
        assert_eq!(
            workers,
            available.clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
        );
        let runtime = new_runtime().unwrap();
        assert_eq!(runtime.metrics().num_workers(), workers);
    }

    #[test]
    fn result_conversion_limit_is_shared_across_the_driver_runtime() {
        let runtime = new_runtime_with_result_conversion_concurrency(2).unwrap();
        let permits = runtime.result_conversion_permits();
        let first = permits.try_acquire_owned().unwrap();
        let second = runtime
            .result_conversion_permits()
            .try_acquire_owned()
            .unwrap();
        assert!(runtime.result_conversion_permits().try_acquire().is_err());

        drop(first);
        assert!(runtime.result_conversion_permits().try_acquire().is_ok());
        drop(second);
    }

    #[test]
    fn bounded_result_work_runs_off_the_calling_executor_thread() {
        let runtime = new_runtime_with_result_conversion_concurrency(1).unwrap();
        let caller = std::thread::current().id();
        let permit = runtime
            .result_conversion_permits()
            .try_acquire_owned()
            .unwrap();
        let worker = runtime
            .block_on(super::run_bounded_result_work(permit, || {
                std::thread::current().id()
            }))
            .unwrap();

        assert_ne!(worker, caller);
    }

    #[test]
    fn runtime_drop_does_not_wait_for_spawn_blocking_work() {
        let runtime = new_runtime().unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(0);
        let (finish_sender, finish_receiver) = mpsc::sync_channel(0);
        runtime.spawn_blocking(move || {
            started_sender.send(()).unwrap();
            finish_receiver.recv().unwrap();
        });
        started_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("blocking task should start");

        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let drop_thread = std::thread::spawn(move || {
            drop(runtime);
            dropped_sender.send(()).unwrap();
        });
        let dropped_promptly = dropped_receiver.recv_timeout(Duration::from_secs(10));

        finish_sender.send(()).unwrap();
        drop_thread.join().unwrap();
        assert!(
            dropped_promptly.is_ok(),
            "dropping the runtime waited for blocking work to finish"
        );
    }

    /// A Rust host calling the driver from its own multi-threaded runtime, from a spawned task or
    /// directly under its `block_on`, must not hit Tokio's nested `block_on` panic.
    #[test]
    fn block_on_works_inside_a_multi_threaded_host_runtime() {
        let runtime = new_runtime().unwrap();
        let host = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let direct = host.block_on(async { runtime.block_on(async { 1 }) });
        let spawned = host
            .block_on(host.spawn({
                let runtime = runtime.clone();
                async move { runtime.block_on(async { 2 }) }
            }))
            .unwrap();
        let _entered = host.enter();
        let entered = runtime.block_on(async { 3 });

        assert_eq!((direct, spawned, entered), (1, 2, 3));
    }

    #[test]
    fn runtime_drop_does_not_panic_in_an_async_context() {
        let runtime = new_runtime().unwrap();
        let outer_runtime = Builder::new_current_thread().build().unwrap();

        outer_runtime.block_on(async move {
            drop(runtime);
        });
    }
}
