//! The wrapper that rebuilds a credential its token cache has given up on.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use adbc_core::error::Result;
use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider};
use google_cloud_gax::error::CredentialsError;
use http::{Extensions, HeaderMap};

use crate::diag::diag;

/// Bounds one rebuild and the wait for another request's. Building is local work, but it can open
/// files a machine's environment names, and one of them can be a pipe that never answers.
const REBUILD_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(30)
};

/// Wraps one token-minting layer of a credential stack so a permanent failure is not served
/// forever.
///
/// The auth library's token cache ends its refresh task at the first failure it classifies as
/// permanent and returns that failure for the life of the credential (UP-140). Tokens are first
/// requested when the credential is built, as a connection opens, so one refused exchange, such as
/// an IAM grant that had not yet propagated, would fail every later request from the database's
/// clients. Go's `oauth2` token sources ask again on the next request.
///
/// On a permanent failure this replaces the layer with one `rebuild` makes from the same validated
/// configuration, over the same layer beneath, and asks the new one once. So each failing request
/// repeats the failing layer's exchanges once, reusing a token the layer beneath still holds; a
/// credential the auth library builds whole is one layer, all of whose exchanges repeat. Requests
/// that fail together share one rebuild. A transient failure is returned as is: the token cache
/// keeps retrying it.
///
/// The rebuild runs on the blocking pool, under [`REBUILD_TIMEOUT`], so no request waits on a lock
/// a build holds. One that outlives the timeout fails its request with the original failure, and
/// no other starts while it runs.
pub(super) fn recovering_credentials(
    credentials: Credentials,
    rebuild: impl Fn() -> Result<Credentials> + Send + Sync + 'static,
) -> Credentials {
    Credentials::from(RecoveringCredentials {
        current: Mutex::new(Current {
            generation: 0,
            credentials,
        }),
        rebuilding: tokio::sync::Mutex::new(()),
        running: Arc::new(AtomicBool::new(false)),
        rebuild: Arc::new(rebuild),
    })
}

struct RecoveringCredentials<F> {
    /// Held only to read or replace the layer, never across a build or an exchange.
    current: Mutex<Current>,
    /// Taken by the request that rebuilds; the others that failed wait for it, then use its layer.
    rebuilding: tokio::sync::Mutex<()>,
    /// Whether a rebuild is still running on the blocking pool, as one past its timeout may be.
    running: Arc<AtomicBool>,
    rebuild: Arc<F>,
}

/// The layer in use, and how many times it has been replaced.
struct Current {
    generation: u64,
    credentials: Credentials,
}

/// Clears [`RecoveringCredentials::running`] when a rebuild ends, panicking or not.
struct Running(Arc<AtomicBool>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl<F> fmt::Debug for RecoveringCredentials<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveringCredentials { credentials: <redacted> }")
    }
}

impl<F> RecoveringCredentials<F>
where
    F: Fn() -> Result<Credentials> + Send + Sync + 'static,
{
    /// A poisoned lock is recovered: the slot is only ever assigned whole.
    fn current(&self) -> (u64, Credentials) {
        let current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        (current.generation, current.credentials.clone())
    }

    /// The layer to ask again after generation `failed` failed permanently: a new one, unless
    /// another request has replaced it already. `None` keeps the failed layer for the next request
    /// to try again: the configuration no longer builds, or a build is still running.
    async fn replacement(&self, failed: u64) -> Option<Credentials> {
        let _turn = tokio::time::timeout(REBUILD_TIMEOUT, self.rebuilding.lock())
            .await
            .ok()?;
        let (generation, credentials) = self.current();
        if generation != failed {
            return Some(credentials);
        }
        if self.running.swap(true, Ordering::AcqRel) {
            return None;
        }
        let running = Running(self.running.clone());
        let rebuild = self.rebuild.clone();
        let build = tokio::task::spawn_blocking(move || {
            let _running = running;
            rebuild()
        });
        let failure = match tokio::time::timeout(REBUILD_TIMEOUT, build).await {
            Ok(Ok(Ok(credentials))) => {
                let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
                current.generation += 1;
                current.credentials = credentials.clone();
                return Some(credentials);
            }
            Ok(Ok(Err(error))) => error.message,
            Ok(Err(_)) => "the rebuild panicked".to_owned(),
            Err(_) => format!(
                "the rebuild did not finish within {} s",
                REBUILD_TIMEOUT.as_secs()
            ),
        };
        diag!(
            Warn,
            "could not rebuild the credential after a refused token exchange: {failure}"
        );
        None
    }
}

impl<F> CredentialsProvider for RecoveringCredentials<F>
where
    F: Fn() -> Result<Credentials> + Send + Sync + 'static,
{
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError> {
        let (generation, credentials) = self.current();
        let failure = match credentials.headers(extensions.clone()).await {
            Err(failure) if !failure.is_transient() => failure,
            headers => return headers,
        };
        match self.replacement(generation).await {
            // The tag in `extensions` names the failed layer's headers, so the new layer answers
            // with new ones.
            Some(replacement) => replacement.headers(extensions).await,
            None => Err(failure),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        self.current().1.universe_domain().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use google_cloud_auth::credentials::EntityTag;
    use http::HeaderValue;
    use http::header::AUTHORIZATION;

    use super::*;

    /// A stack that refuses its first `refusals` requests, permanently or transiently, then
    /// answers with `token`.
    #[derive(Debug)]
    struct Refusing {
        refusals: AtomicUsize,
        transient: bool,
        token: &'static str,
        requests: Arc<AtomicUsize>,
    }

    impl CredentialsProvider for Refusing {
        async fn headers(
            &self,
            _: Extensions,
        ) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            // As an exchange would, so requests made together all see the same answer.
            tokio::task::yield_now().await;
            let refused = self
                .refusals
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(1)
                })
                .is_ok();
            if refused {
                return Err(CredentialsError::from_msg(self.transient, "refused"));
            }
            let mut data = HeaderMap::new();
            data.insert(AUTHORIZATION, HeaderValue::from_static(self.token));
            Ok(CacheableResource::New {
                entity_tag: EntityTag::new(),
                data,
            })
        }

        async fn universe_domain(&self) -> Option<String> {
            Some(self.token.to_owned())
        }
    }

    /// A wrapper whose first stack refuses `first` requests and whose rebuilds refuse `later`, all
    /// permanently unless `transient`, and the number of rebuilds and of requests made.
    fn recovering(
        first: usize,
        later: usize,
        transient: bool,
    ) -> (Credentials, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let stack = {
            let requests = requests.clone();
            move |refusals, token| {
                Credentials::from(Refusing {
                    refusals: AtomicUsize::new(refusals),
                    transient,
                    token,
                    requests: requests.clone(),
                })
            }
        };
        let counted = rebuilds.clone();
        let credentials = recovering_credentials(stack(first, "first"), move || {
            counted.fetch_add(1, Ordering::Relaxed);
            Ok(stack(later, "rebuilt"))
        });
        (credentials, rebuilds, requests)
    }

    fn authorization(headers: CacheableResource<HeaderMap>) -> String {
        let CacheableResource::New { data, .. } = headers else {
            panic!("expected new headers");
        };
        data[AUTHORIZATION].to_str().unwrap().to_owned()
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    /// A refusal that has since been resolved costs the request no failure: it is asked of a
    /// rebuilt stack, which then serves every later request.
    #[test]
    fn a_permanent_refusal_is_asked_again_of_a_rebuilt_stack() {
        let (credentials, rebuilds, requests) = recovering(1, 0, false);
        block_on(async {
            let headers = credentials.headers(Extensions::new()).await.unwrap();
            assert_eq!(authorization(headers), "rebuilt");
            assert_eq!(
                credentials.universe_domain().await.as_deref(),
                Some("rebuilt")
            );
            credentials.headers(Extensions::new()).await.unwrap();
        });
        assert_eq!(rebuilds.load(Ordering::Relaxed), 1);
        assert_eq!(requests.load(Ordering::Relaxed), 3);
    }

    /// A refusal that persists costs each request one rebuild, and fails it with the refusal the
    /// rebuilt stack returned.
    #[test]
    fn a_persistent_refusal_costs_each_request_one_rebuild() {
        let (credentials, rebuilds, requests) = recovering(1, usize::MAX, false);
        block_on(async {
            for _ in 0..3 {
                let failure = credentials.headers(Extensions::new()).await.unwrap_err();
                assert!(!failure.is_transient());
            }
        });
        assert_eq!(rebuilds.load(Ordering::Relaxed), 3);
        assert_eq!(requests.load(Ordering::Relaxed), 6);
    }

    /// The token cache retries a transient failure itself, so the stack is kept.
    #[test]
    fn a_transient_failure_keeps_the_stack() {
        let (credentials, rebuilds, _) = recovering(1, 0, true);
        block_on(async {
            assert!(
                credentials
                    .headers(Extensions::new())
                    .await
                    .unwrap_err()
                    .is_transient()
            );
            let headers = credentials.headers(Extensions::new()).await.unwrap();
            assert_eq!(authorization(headers), "first");
        });
        assert_eq!(rebuilds.load(Ordering::Relaxed), 0);
    }

    /// Requests that fail on the same stack share one replacement.
    #[test]
    fn requests_failing_together_rebuild_once() {
        let (credentials, rebuilds, _) = recovering(usize::MAX, 0, false);
        block_on(async {
            let (first, second) = tokio::join!(
                credentials.headers(Extensions::new()),
                credentials.headers(Extensions::new()),
            );
            assert_eq!(authorization(first.unwrap()), "rebuilt");
            assert_eq!(authorization(second.unwrap()), "rebuilt");
        });
        assert_eq!(rebuilds.load(Ordering::Relaxed), 1);
    }

    /// A configuration that no longer builds keeps the failed stack and its failure, and the next
    /// request tries the rebuild again.
    #[test]
    fn a_failed_rebuild_returns_the_original_failure() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        let credentials = recovering_credentials(
            Credentials::from(Refusing {
                refusals: AtomicUsize::new(usize::MAX),
                transient: false,
                token: "first",
                requests: Arc::default(),
            }),
            move || {
                counted.fetch_add(1, Ordering::Relaxed);
                Err(crate::error::invalid_argument("no longer builds"))
            },
        );
        block_on(async {
            for _ in 0..2 {
                let failure = credentials.headers(Extensions::new()).await.unwrap_err();
                assert!(!failure.is_transient());
                assert!(failure.to_string().contains("refused"), "{failure}");
            }
        });
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    /// A rebuild that never finishes, as one reading a pipe could, holds no lock a request needs:
    /// requests keep reaching the layer, the stuck one fails its request after the timeout, and no
    /// second starts behind it. Once it ends, the next failure rebuilds again.
    #[test]
    fn a_stuck_rebuild_stalls_no_request_and_starts_no_other() {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        let credentials = recovering_credentials(
            Credentials::from(Refusing {
                refusals: AtomicUsize::new(usize::MAX),
                transient: false,
                token: "first",
                requests: Arc::default(),
            }),
            move || {
                if counted.fetch_add(1, Ordering::Relaxed) == 0 {
                    let _ = released.lock().unwrap().recv();
                }
                Err(crate::error::invalid_argument("no longer builds"))
            },
        );
        block_on(async {
            let started = tokio::time::Instant::now();
            let (stuck, universe) = tokio::join!(credentials.headers(Extensions::new()), async {
                tokio::time::sleep(REBUILD_TIMEOUT / 4).await;
                credentials.universe_domain().await
            });
            assert!(stuck.is_err());
            assert!(started.elapsed() >= REBUILD_TIMEOUT);
            assert_eq!(universe.as_deref(), Some("first"));

            let started = tokio::time::Instant::now();
            assert!(credentials.headers(Extensions::new()).await.is_err());
            assert!(started.elapsed() < REBUILD_TIMEOUT);
            assert_eq!(attempts.load(Ordering::Relaxed), 1);

            release.send(()).unwrap();
            while attempts.load(Ordering::Relaxed) < 2 {
                assert!(credentials.headers(Extensions::new()).await.is_err());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
}
