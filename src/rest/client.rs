//! Shared HTTP connection pool with per-request deadlines and upload progress timeouts.

use std::borrow::Cow;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use reqwest::header::{HeaderValue, USER_AGENT as USER_AGENT_HEADER};

use crate::error::{driver_defect, error, failed_to, request_error};
use crate::runtime::process_id;

#[cfg(test)]
mod tests;

/// Bounds TCP/TLS setup, including a blackholed endpoint or hung proxy.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Covers connection setup through the last response byte. Must exceed the 45 s server wait
/// (`QUERY_COMPLETION_WAIT_MS`) and leave time to download a row page.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a connection with an open stream may stay silent before it is pinged. An unanswered
/// ping closes it within [`REQUEST_TIMEOUT`], so a retry can leave a peer whose socket lives on but
/// whose HTTP/2 layer stalled, such as a wedged proxy. Idle connections are not pinged.
const HTTP2_PING_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(30)
};

/// Bounds gaps between upload frames and the wait for response headers after the last frame.
/// Gigabyte CSV seeds need no whole-request deadline; reqwest's read timeout ignores body progress.
const UPLOAD_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounds the complete upload response body: one small `Job` or Cloud Storage `Object` document.
const MEDIA_RESPONSE_BODY_TIMEOUT: Duration = Duration::from_secs(120);

/// Raw-response limits, separate from the pipeline's decoded-batch budget. Media uploads return one
/// resource document. A query page holds about 10 MiB of row data, or one row of up to 20 MiB, but
/// BigQuery counts a string's own bytes and its JSON spells a control character, `<` or `>` in six
/// (`<`): one row of 20,000,000 U+0001 characters arrives as 120,000,768 bytes.
const REQUEST_RESPONSE_LIMIT: usize = 128 << 20;
const MEDIA_RESPONSE_LIMIT: usize = 8 << 20;

/// The deadline policy carried by an individual request over the shared pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TimeoutBudget {
    /// A deadline from connection start through the final response byte.
    Request,
    /// No whole-request deadline; the connect timeout and [`UPLOAD_PROGRESS_TIMEOUT`] apply.
    MediaUpload,
}

impl TimeoutBudget {
    pub(super) fn response_limit(self) -> usize {
        match self {
            Self::Request => REQUEST_RESPONSE_LIMIT,
            Self::MediaUpload => MEDIA_RESPONSE_LIMIT,
        }
    }

    /// An additional response-body deadline only for requests without a whole-request deadline.
    pub(super) fn response_body_timeout(self) -> Option<Duration> {
        match self {
            Self::Request => None,
            Self::MediaUpload => Some(MEDIA_RESPONSE_BODY_TIMEOUT),
        }
    }
}

/// Clones share one connection pool for all direct REST operations.
#[derive(Clone)]
pub(crate) struct RestClient {
    transport: Arc<Transport>,
    /// Sent with each request rather than set on the pool, so a pool can serve any database.
    user_agent: HeaderValue,
    /// HTTP 401 remedy for credentials without token-minting diagnostics.
    unauthenticated_remedy: Option<Arc<str>>,
    /// [`UPLOAD_PROGRESS_TIMEOUT`], which tests shorten.
    upload_progress_timeout: Duration,
}

/// A connection pool, which the databases of one driver reuse one after another.
///
/// When the last client of a database's client generation drops it, the pool and its open
/// connections join the driver's [`IdlePools`], and the next database the driver opens takes one
/// instead of building its own: a process that opens a database per connection, as the Python
/// DB-API does, then skips the TCP and TLS handshakes and reloading the system's root
/// certificates. A pool serves one generation at a time, so databases open together still get
/// connections of their own, as they did with a pool each: an HTTP/2 connection carries at most
/// the server's stream limit. A pool holds nothing that differs between databases, not even a
/// credential, which each request carries; its proxy settings are read when it is built, as Go
/// reads them once per process. Hyper closes a connection idle for 90 seconds.
struct Transport {
    http: reqwest::Client,
    /// Where the pool goes when no client uses it; `None` for a pool nothing reuses.
    idle: Option<Arc<IdlePools>>,
}

/// The idle connection pools of one driver runtime's databases, which only they may reuse: the
/// runtime's tasks drive the connections.
///
/// A `fork` child gives each database a new runtime and a new set, so it never writes to its
/// parent's sockets nor waits on this lock, which a thread the child lacks may have held.
pub(crate) struct IdlePools {
    pools: Mutex<Vec<reqwest::Client>>,
    /// The process that may use them, compared as `DriverRuntime::orphaned` compares its own.
    process: u32,
}

/// Enough for a few workers that each open and close databases in turn.
const IDLE_POOLS: usize = 4;

impl IdlePools {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pools: Mutex::new(Vec::new()),
            process: process_id(),
        })
    }

    /// The pools, unless a `fork` child inherited them. A poisoned lock is recovered: it is held
    /// only to move one entry.
    fn pools(&self) -> Option<MutexGuard<'_, Vec<reqwest::Client>>> {
        (self.process == process_id())
            .then(|| self.pools.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn take(&self) -> Option<reqwest::Client> {
        self.pools()?.pop()
    }
}

impl std::fmt::Debug for IdlePools {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("IdlePools")
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        if let Some(mut pools) = self.idle.as_deref().and_then(IdlePools::pools)
            && pools.len() < IDLE_POOLS
        {
            pools.push(self.http.clone());
        }
    }
}

impl RestClient {
    pub(crate) fn with_unauthenticated_remedy(mut self, remedy: Option<String>) -> Self {
        self.unauthenticated_remedy = remedy.map(Arc::from);
        self
    }

    pub(super) fn unauthenticated_remedy(&self) -> Option<&str> {
        self.unauthenticated_remedy.as_deref()
    }

    pub(super) fn upload_progress_timeout(&self) -> Duration {
        self.upload_progress_timeout
    }

    #[cfg(test)]
    pub(crate) fn with_upload_progress_timeout(mut self, timeout: Duration) -> Self {
        self.upload_progress_timeout = timeout;
        self
    }

    /// Applies the request's deadline and disables Google APIs' default JSON indentation.
    pub(super) fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        budget: TimeoutBudget,
    ) -> reqwest::RequestBuilder {
        let separator = if url.contains('?') { '&' } else { '?' };
        let request = self
            .transport
            .http
            .request(method, format!("{url}{separator}prettyPrint=false"))
            .header(USER_AGENT_HEADER, self.user_agent.clone());
        match budget {
            TimeoutBudget::Request => request.timeout(REQUEST_TIMEOUT),
            TimeoutBudget::MediaUpload => request,
        }
    }
}

/// Adds the timeout's kind and duration, which reqwest's error omits.
#[cold]
#[inline(never)]
fn timeout_detail(budget: TimeoutBudget, connecting: bool) -> String {
    if connecting {
        return format!(
            "the driver's fixed {} s HTTP connect timeout elapsed; the endpoint accepted no \
             connection",
            CONNECT_TIMEOUT.as_secs()
        );
    }
    if budget == TimeoutBudget::MediaUpload {
        return upload_progress_detail(UPLOAD_PROGRESS_TIMEOUT);
    }
    format!(
        "the driver's fixed {} s HTTP request timeout elapsed",
        REQUEST_TIMEOUT.as_secs()
    )
}

#[cold]
#[inline(never)]
fn upload_progress_detail(timeout: Duration) -> String {
    format!(
        "the driver's fixed {} s HTTP upload progress timeout elapsed; the upload sent \
         nothing, or got no answer, for that long",
        timeout.as_secs()
    )
}

#[cold]
#[inline(never)]
pub(super) fn upload_stalled_error(context: &str, timeout: Duration) -> Error {
    error(
        failed_to(context, upload_progress_detail(timeout)),
        Status::Timeout,
    )
}

/// `declared` is an oversized `Content-Length`, or `None` if the body exceeded the limit while read.
#[cold]
#[inline(never)]
pub(super) fn oversized_response_error(
    context: &str,
    declared: Option<u64>,
    budget: TimeoutBudget,
) -> Error {
    let detail = match declared {
        Some(declared) => format!("the service announced a {declared}-byte response body"),
        None => "the response body kept arriving".to_owned(),
    };
    error(
        failed_to(
            context,
            format_args!(
                "{detail}, over the driver's fixed {}-byte limit on a buffered response body",
                budget.response_limit()
            ),
        ),
        Status::InvalidData,
    )
}

#[cold]
#[inline(never)]
pub(super) fn response_body_timeout_error(context: &str, deadline: Duration) -> Error {
    error(
        failed_to(
            context,
            format_args!(
                "the driver's fixed {} s HTTP response-body timeout elapsed",
                deadline.as_secs()
            ),
        ),
        Status::Timeout,
    )
}

/// Classifies a transport failure and names any elapsed timeout.
#[cold]
#[inline(never)]
pub(super) fn transport_error(
    context: &str,
    source: &reqwest::Error,
    budget: TimeoutBudget,
) -> Error {
    if !source.is_timeout() {
        return request_error(context, source);
    }
    error(
        failed_to(context, timeout_detail(budget, source.is_connect())),
        Status::Timeout,
    )
}

/// A client on a pool of its own, which no other client reuses.
#[cfg(test)]
pub(crate) fn rest_client(what: &str) -> Result<RestClient> {
    build_client(what, reqwest::Client::builder())
}

/// A client sending `user_agent`. With `idle`, it takes one of those pools, or builds one that
/// joins them once the client and its clones are dropped.
pub(crate) fn pooled_rest_client(
    what: &str,
    user_agent: &str,
    idle: Option<&Arc<IdlePools>>,
) -> Result<RestClient> {
    let user_agent = HeaderValue::from_str(user_agent).map_err(|_| {
        driver_defect(format_args!(
            "the BigQuery {what} user agent is not an HTTP header value"
        ))
    })?;
    let http = match idle.and_then(|idle| idle.take()) {
        Some(http) => http,
        None => build_http(what, reqwest::Client::builder())?,
    };
    Ok(RestClient {
        transport: Arc::new(Transport {
            http,
            idle: idle.cloned(),
        }),
        user_agent,
        unauthenticated_remedy: None,
        upload_progress_timeout: UPLOAD_PROGRESS_TIMEOUT,
    })
}

/// The driver and its version, which Cloud Audit Logs record as `callerSuppliedUserAgent`.
const USER_AGENT: &str = concat!("bigquery-adbc/", env!("CARGO_PKG_VERSION"));

/// The `User-Agent` of a request: the driver's own, then the caller's `bigquery.user_agent` text.
pub(crate) fn user_agent(caller: Option<&str>) -> Cow<'static, str> {
    match caller {
        Some(caller) => Cow::Owned(format!("{USER_AGENT} {caller}")),
        None => Cow::Borrowed(USER_AGENT),
    }
}

/// A client on a pool built from `builder`, which no other client reuses.
#[cfg(test)]
fn build_client(what: &str, builder: reqwest::ClientBuilder) -> Result<RestClient> {
    Ok(RestClient {
        transport: Arc::new(Transport {
            http: build_http(what, builder)?,
            idle: None,
        }),
        user_agent: HeaderValue::from_static(USER_AGENT),
        unauthenticated_remedy: None,
        upload_progress_timeout: UPLOAD_PROGRESS_TIMEOUT,
    })
}

/// Responses are not gzip-encoded: Google pads gzip responses in ways that make `h2` end the
/// shared connection (UP-94).
fn build_http(what: &str, builder: reqwest::ClientBuilder) -> Result<reqwest::Client> {
    builder
        // Redirects can replay SQL and upload bodies outside the configured endpoint.
        .redirect(reqwest::redirect::Policy::none())
        // Google API hosts negotiate HTTP/2 via ALPN. An adaptive receive window lets a large
        // result page use the available bandwidth without reserving a large window for every
        // small control-plane response on the shared connection.
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(HTTP2_PING_INTERVAL)
        .http2_keep_alive_timeout(HTTP2_PING_INTERVAL / 2)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map_err(|source| {
            driver_defect(format_args!(
                "failed to initialize the BigQuery {what} HTTP client: {source}"
            ))
        })
}
