//! The shared `reqwest` client and per-request timeout budgets.
//!
//! A JSON request has a whole-request deadline; a media upload does not. The deadline is set per
//! request so both share one TLS configuration and connection pool.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};

use crate::error::{driver_defect, error, failed_to, request_error};

#[cfg(test)]
mod tests;

/// How long a directly issued request may spend establishing its TCP and TLS connection.
///
/// Without it, a blackholed SYN waits for the OS connect timeout (over two minutes on Linux), or
/// indefinitely behind a hung proxy.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a whole non-upload request may take, from connecting to the last response byte.
///
/// It must exceed `QUERY_COMPLETION_WAIT_MS` (45 s), the server-side wait `jobs.query` and
/// `jobs.getQueryResults` request; long-running work is polled, never awaited in one request.
/// What remains of the window after that wait also bounds the download of a row page, which
/// BigQuery caps at 10 MB.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a media upload may go without progress: without the transport taking more of its
/// body while it is sent, and without response headers once all of it has been taken.
///
/// A CSV seed can be gigabytes, so no whole-request deadline fits. reqwest's read timeout cannot
/// stand in: its first window starts with the request and ignores request-body progress.
const UPLOAD_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the *response body* of a media upload may take once its headers have arrived.
///
/// The body is one small `Job` or Cloud Storage `Object` document, sent after the upload.
const MEDIA_RESPONSE_BODY_TIMEOUT: Duration = Duration::from_secs(120);

/// The most response-body bytes a request on each budget may buffer.
///
/// Responses are buffered whole, and the byte budget in `src/pipeline.rs` charges only decoded
/// batches, so these limits are the only bound on buffered size. Both are far above any valid
/// response: BigQuery caps a `jobs.query`/`jobs.getQueryResults` page at 10 MB, and a media upload
/// returns one resource document.
const REQUEST_RESPONSE_LIMIT: usize = 64 << 20;
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
    /// The most response-body bytes this budget may buffer. See [`REQUEST_RESPONSE_LIMIT`].
    pub(super) fn response_limit(self) -> usize {
        match self {
            Self::Request => REQUEST_RESPONSE_LIMIT,
            Self::MediaUpload => MEDIA_RESPONSE_LIMIT,
        }
    }

    /// The whole-phase deadline on reading this budget's response body, where the budget itself
    /// does not already bound it. See [`MEDIA_RESPONSE_BODY_TIMEOUT`].
    pub(super) fn response_body_timeout(self) -> Option<Duration> {
        match self {
            Self::Request => None,
            Self::MediaUpload => Some(MEDIA_RESPONSE_BODY_TIMEOUT),
        }
    }
}

/// The reqwest client every direct REST operation shares. Clones share one host-keyed connection
/// pool.
#[derive(Clone)]
pub(crate) struct RestClient {
    http: reqwest::Client,
    /// Remedy added to an HTTP 401, for credentials that never mint their own token and so get no
    /// token diagnosis. See [`Self::with_unauthenticated_remedy`].
    unauthenticated_remedy: Option<Arc<str>>,
    /// [`UPLOAD_PROGRESS_TIMEOUT`], which tests shorten.
    upload_progress_timeout: Duration,
}

impl RestClient {
    /// Adds `remedy` to every HTTP 401 a request on this client is answered with.
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

    /// Starts one request with the deadline selected by that request's shape.
    ///
    /// Every request asks for compact JSON. Google APIs indent theirs unless sent
    /// `prettyPrint=false`, which makes every response larger to receive, buffer and decode.
    pub(super) fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        budget: TimeoutBudget,
    ) -> reqwest::RequestBuilder {
        let separator = if url.contains('?') { '&' } else { '?' };
        let request = self
            .http
            .request(method, format!("{url}{separator}prettyPrint=false"));
        match budget {
            TimeoutBudget::Request => request.timeout(REQUEST_TIMEOUT),
            TimeoutBudget::MediaUpload => request,
        }
    }
}

/// Names the driver timeout that fired, since `reqwest`'s own timeout text says neither which
/// budget elapsed nor how long it was.
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

/// Reports a media upload that stopped making progress. See [`UPLOAD_PROGRESS_TIMEOUT`].
#[cold]
#[inline(never)]
pub(super) fn upload_stalled_error(context: &str, timeout: Duration) -> Error {
    error(
        failed_to(context, upload_progress_detail(timeout)),
        Status::Timeout,
    )
}

/// Reports a response body this request refuses to hold, naming the ceiling that refused it.
/// `declared` is the `Content-Length` the service announced, or `None` when it announced none
/// and the body was stopped as it streamed.
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

/// Reports a response body that stopped arriving. See [`MEDIA_RESPONSE_BODY_TIMEOUT`].
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

/// Classifies a failed request, naming that request's timeout when that is what fired.
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

/// [`rest_client_with_user_agent`] naming only the driver, as a database without
/// `bigquery.user_agent` does.
#[cfg(test)]
pub(crate) fn rest_client(what: &str) -> Result<RestClient> {
    rest_client_with_user_agent(what, USER_AGENT)
}

/// The client shared by small JSON requests and media uploads, sending `user_agent` as
/// [`user_agent`] spells it.
pub(crate) fn rest_client_with_user_agent(what: &str, user_agent: &str) -> Result<RestClient> {
    build_client(what, reqwest::Client::builder().user_agent(user_agent))
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

/// Responses are not gzip-encoded: Google pads gzip responses in ways that make `h2` end the
/// shared connection (UP-94).
fn build_client(what: &str, builder: reqwest::ClientBuilder) -> Result<RestClient> {
    builder
        // Redirects can replay SQL and upload bodies outside the configured endpoint.
        .redirect(reqwest::redirect::Policy::none())
        // Google API hosts negotiate HTTP/2 via ALPN. An adaptive receive window lets a large
        // result page use the available bandwidth without reserving a large window for every
        // small control-plane response on the shared connection.
        .http2_adaptive_window(true)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map(|http| RestClient {
            http,
            unauthenticated_remedy: None,
            upload_progress_timeout: UPLOAD_PROGRESS_TIMEOUT,
        })
        .map_err(|source| {
            driver_defect(format_args!(
                "failed to initialize the BigQuery {what} HTTP client: {source}"
            ))
        })
}
