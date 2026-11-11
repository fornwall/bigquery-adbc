//! One authenticated request: credential, cancellation, and the raw response.
//!
//! [`RequestStart`] records whether a request reached the poll that may put bytes on the network,
//! which tells a cancelled operation whether remote work may exist. [`multipart_related`]
//! (buffered) and [`multipart_related_file`] (streamed) build media-upload bodies.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use adbc_core::error::Result;
use futures_core::Stream as _;
use google_cloud_auth::credentials::{CacheableResource, Credentials};
use google_cloud_gax::error::Error as GoogleError;
use tokio::io::AsyncReadExt as _;
use tokio_util::io::ReaderStream;

use crate::cancellation::{Operation, gate};
use crate::error::{append_remedy, driver_defect, google_error, invalid_argument};
use crate::rest::client::{
    RestClient, TimeoutBudget, oversized_response_error, response_body_timeout_error,
    transport_error, upload_stalled_error,
};
use crate::rest::rejection::service_error;

/// An HTTP request body and its exact wire length.
///
/// The body may be reusable bytes or a one-shot stream. Retrying callers build a fresh [`Request`]
/// per attempt from their own bytes; a streamed CSV upload reopens its file for each attempt.
pub(crate) struct RequestBody {
    body: reqwest::Body,
    content_length: u64,
}

impl<T: Into<bytes::Bytes>> From<T> for RequestBody {
    fn from(body: T) -> Self {
        let body = body.into();
        Self {
            content_length: body.len() as u64,
            body: body.into(),
        }
    }
}

#[cfg(test)]
impl RequestBody {
    pub(crate) fn is_streaming(&self) -> bool {
        self.body.as_bytes().is_none()
    }
}

/// A streamed CSV multipart body: prefix, file, closing boundary, and the exact bytes remaining.
struct MultipartRelatedFileBody {
    prefix: Option<bytes::Bytes>,
    media: ReaderStream<tokio::io::Take<tokio::fs::File>>,
    closing: bytes::Bytes,
    remaining: u64,
}

impl MultipartRelatedFileBody {
    fn frame(&mut self, bytes: bytes::Bytes) -> http_body::Frame<bytes::Bytes> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len() as u64)
            .expect("a streamed multipart chunk cannot exceed the body's declared remaining size");
        http_body::Frame::data(bytes)
    }
}

impl http_body::Body for MultipartRelatedFileBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if let Some(prefix) = self.prefix.take() {
            return Poll::Ready(Some(Ok(self.frame(prefix))));
        }
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        match ready!(Pin::new(&mut self.media).poll_next(context)) {
            Some(Ok(bytes)) => Poll::Ready(Some(Ok(self.frame(bytes)))),
            Some(Err(source)) => {
                self.remaining = 0;
                Poll::Ready(Some(Err(source)))
            }
            None if self.remaining == self.closing.len() as u64 => {
                let closing = std::mem::take(&mut self.closing);
                Poll::Ready(Some(Ok(self.frame(closing))))
            }
            None => {
                self.remaining = 0;
                Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "BigQuery CSV seed file ended before its recorded length; \
                     the file may have been truncated during upload",
                ))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// The largest frame a watched upload hands the transport at once, so that a buffered body
/// reports progress as the transport drains it rather than once for its whole length.
const UPLOAD_FRAME_BYTES: usize = 64 * 1024;

/// When a media upload last made progress, for the watchdog in [`send`].
struct UploadProgress {
    started: tokio::time::Instant,
    /// Milliseconds after `started`.
    moved: AtomicU64,
}

impl UploadProgress {
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            moved: AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.moved.store(elapsed, Ordering::Relaxed);
    }

    /// Completes once `timeout` passes without progress.
    ///
    /// The transport takes body frames only as it sends them, so a frame taken is progress; after
    /// the last one, the same window bounds the wait for the response headers.
    async fn stalled(&self, timeout: Duration) {
        loop {
            let moved = Duration::from_millis(self.moved.load(Ordering::Relaxed));
            let deadline = self.started + moved + timeout;
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

/// A media upload body that records each frame the transport takes from it.
struct WatchedUpload {
    body: reqwest::Body,
    /// The rest of the frame `body` last produced, handed on [`UPLOAD_FRAME_BYTES`] at a time.
    pending: bytes::Bytes,
    progress: Arc<UploadProgress>,
}

impl WatchedUpload {
    fn taken<T>(&self, polled: T) -> Poll<T> {
        self.progress.touch();
        Poll::Ready(polled)
    }
}

impl http_body::Body for WatchedUpload {
    type Data = bytes::Bytes;
    type Error = reqwest::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        if this.pending.is_empty() {
            match ready!(Pin::new(&mut this.body).poll_frame(context)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => this.pending = data,
                    Err(frame) => return this.taken(Some(Ok(frame))),
                },
                // The end of the body starts the wait for the response.
                other => return this.taken(other),
            }
        }
        let length = this.pending.len().min(UPLOAD_FRAME_BYTES);
        let data = this.pending.split_to(length);
        this.taken(Some(Ok(http_body::Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_empty() && self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let inner = self.body.size_hint();
        let pending = self.pending.len() as u64;
        let mut hint = http_body::SizeHint::new();
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper.saturating_add(pending));
        }
        hint.set_lower(inner.lower().saturating_add(pending));
        hint
    }
}

/// One directly issued authenticated request.
///
/// `context` is the verb phrase that names the request in its failures ("authorize view v"), or
/// empty when the caller names them itself (see [`crate::error::named`]); it never carries the
/// body.
pub(crate) struct Request<'a> {
    method: reqwest::Method,
    pub(crate) url: &'a str,
    /// The `Content-Type` and body, or `None` to send neither.
    body: Option<(&'a str, RequestBody)>,
    /// An `If-Match` precondition, or the empty string to send none.
    etag: &'a str,
    pub(crate) context: &'a str,
    timeout: TimeoutBudget,
    /// Set on the first poll of reqwest's network send future, never while credentials are still
    /// being acquired or when cancellation wins before that future is polled.
    start: Option<RequestStart>,
}

/// Whether a request reached the first poll that may put bytes on the network.
///
/// One signal spans every attempt of a submission. An attempt whose connection could not be
/// established sent nothing, so it clears the signal it set, but never one an earlier attempt set.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestStart(Arc<AtomicBool>);

impl RequestStart {
    pub(crate) fn started(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl<'a> Request<'a> {
    /// A request on the [`TimeoutBudget::Request`] budget, without a precondition or start
    /// signal.
    fn new(
        method: reqwest::Method,
        url: &'a str,
        body: Option<(&'a str, RequestBody)>,
        context: &'a str,
    ) -> Self {
        Self {
            method,
            url,
            body,
            etag: "",
            context,
            timeout: TimeoutBudget::Request,
            start: None,
        }
    }

    /// A bodyless `GET`.
    pub(crate) fn get(url: &'a str, context: &'a str) -> Self {
        Self::new(reqwest::Method::GET, url, None, context)
    }

    /// A `PATCH` of a JSON document, guarded by `etag` when the caller has one.
    pub(crate) fn patch(
        url: &'a str,
        etag: &'a str,
        body: impl Into<bytes::Bytes>,
        context: &'a str,
    ) -> Self {
        Self {
            etag,
            ..Self::new(
                reqwest::Method::PATCH,
                url,
                Some(("application/json", RequestBody::from(body))),
                context,
            )
        }
    }

    /// A `POST` of a JSON document.
    pub(crate) fn post_json(url: &'a str, body: impl Into<bytes::Bytes>, context: &'a str) -> Self {
        Self::new(
            reqwest::Method::POST,
            url,
            Some(("application/json", RequestBody::from(body))),
            context,
        )
    }

    /// A bodyless `POST`, as used by `jobs.cancel`.
    pub(super) fn post_empty(url: &'a str, context: &'a str) -> Self {
        // BigQuery answers an unframed POST with 411 Length Required; an empty body makes reqwest
        // send `Content-Length: 0`.
        Self::post_json(url, bytes::Bytes::new(), context)
    }

    /// A buffered or streamed media `POST`, without the normal whole-request deadline.
    pub(crate) fn post_media(
        url: &'a str,
        content_type: &'a str,
        body: impl Into<RequestBody>,
        context: &'a str,
    ) -> Self {
        Self {
            timeout: TimeoutBudget::MediaUpload,
            ..Self::new(
                reqwest::Method::POST,
                url,
                Some((content_type, body.into())),
                context,
            )
        }
    }

    /// Attaches the marker used when a caller must distinguish an unstarted request from an
    /// ambiguous in-flight one after cancellation.
    pub(crate) fn with_start_signal(mut self, start: &RequestStart) -> Self {
        self.start = Some(start.clone());
        self
    }
}

/// Sends one authenticated request and returns the response status and body uninterpreted. A
/// rejection body may reach an error only through [`service_error`], which never echoes it.
///
/// `operation` gates every await. `None` sends ungated, for the best-effort remote cancel of an
/// already-cancelled statement, or for a job submission whose answer alone says where the job to
/// cancel is.
pub(crate) async fn send(
    operation: Option<&Operation>,
    credentials: &Credentials,
    client: &RestClient,
    request: Request<'_>,
) -> Result<(reqwest::StatusCode, bytes::Bytes)> {
    let context = request.context;
    let start = request.start;
    let timeout = request.timeout;
    let headers = gate(operation, credentials.headers(http::Extensions::new()))
        .await?
        .map_err(|source| {
            // An unmintable credential is an authentication failure; only a token service that
            // answered with a transient failure is a retryable `Status::IO`. Wrapping it as the
            // gRPC path's authentication error reuses that classification and its remedies, so
            // both transports report it identically.
            google_error(
                &naming("failed to authenticate", context),
                GoogleError::authentication(source),
            )
        })?;
    let CacheableResource::New { data: headers, .. } = headers else {
        return Err(driver_defect(format_args!(
            "{}",
            naming(
                "BigQuery credentials returned no headers for a fresh request",
                context
            )
        )));
    };
    // A media upload has no whole-request deadline, so it is bounded by its progress instead.
    let progress = (timeout == TimeoutBudget::MediaUpload).then(|| Arc::new(UploadProgress::new()));
    let mut builder = client
        .request(request.method, request.url, timeout)
        .headers(headers);
    if let Some((content_type, body)) = request.body {
        let RequestBody {
            body,
            content_length,
        } = body;
        let body = match &progress {
            Some(progress) => reqwest::Body::wrap(WatchedUpload {
                body,
                pending: bytes::Bytes::new(),
                progress: progress.clone(),
            }),
            None => body,
        };
        builder = builder
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .header(reqwest::header::CONTENT_LENGTH, content_length)
            .body(body);
    }
    if !request.etag.is_empty() {
        builder = builder.header(reqwest::header::IF_MATCH, request.etag);
    }
    let network_send = builder.send();
    tokio::pin!(network_send);
    // Whether this attempt set `start`, rather than finding it set by an earlier one.
    let set_start = AtomicBool::new(false);
    let network_send = std::future::poll_fn(|task| {
        if let Some(start) = start.as_ref()
            && !start.0.swap(true, Ordering::AcqRel)
        {
            set_start.store(true, Ordering::Relaxed);
        }
        network_send.as_mut().poll(task)
    });
    let sent = async {
        network_send.await.map_err(|source| {
            // reqwest reports a connect error only for a connection it could not establish
            // (DNS, TCP, TLS or a proxy tunnel), before any byte of the request was written.
            if source.is_connect()
                && set_start.load(Ordering::Relaxed)
                && let Some(start) = start.as_ref()
            {
                start.0.store(false, Ordering::Release);
            }
            transport_error(context, &source, timeout)
        })
    };
    let response = gate(operation, async {
        let Some(progress) = progress else {
            return sent.await;
        };
        let window = client.upload_progress_timeout();
        tokio::select! {
            biased;
            response = sent => response,
            () = progress.stalled(window) => Err(upload_stalled_error(context, window)),
        }
    })
    .await??;
    let status = response.status();
    let body = gate(operation, read_body(context, response, timeout)).await??;
    if status == reqwest::StatusCode::UNAUTHORIZED
        && let Some(remedy) = client.unauthenticated_remedy()
    {
        let mut failure = service_error(
            &naming("BigQuery refused the credentials", context),
            status,
            &body,
        );
        append_remedy(&mut failure, remedy);
        return Err(failure);
    }
    Ok((status, body))
}

/// `lead` naming the request as `{lead} to {context}`, or alone for an unnamed request.
fn naming(lead: &str, context: &str) -> String {
    if context.is_empty() {
        lead.to_owned()
    } else {
        format!("{lead} to {context}")
    }
}

/// Buffers one response body, refusing to hold more than the request's budget allows.
///
/// Reads chunk by chunk against a running total, because `Response::bytes` is bounded only by the
/// request deadline. An announced `Content-Length` over the limit is refused before reading; a
/// body without one (common over HTTP/2) is stopped as it streams.
async fn read_body(
    context: &str,
    mut response: reqwest::Response,
    budget: TimeoutBudget,
) -> Result<bytes::Bytes> {
    let limit = budget.response_limit();
    let declared = response.content_length();
    if let Some(declared) = declared
        && declared > limit as u64
    {
        return Err(oversized_response_error(context, Some(declared), budget));
    }

    // One chunk is handed on untouched; only a body that arrives in pieces is concatenated.
    let mut first: Option<bytes::Bytes> = None;
    let mut collected: Option<bytes::BytesMut> = None;
    let mut total = 0_usize;
    // Formatted lazily; only a failed read uses it.
    let read_context = || naming("read the response", context);
    // One deadline for the whole body; a trickle never trips a per-read timeout.
    let body_timeout = budget.response_body_timeout();
    let body_deadline =
        body_timeout.map(|timeout| (tokio::time::Instant::now() + timeout, timeout));
    loop {
        let chunk = match body_deadline {
            Some((deadline, timeout)) => tokio::time::timeout_at(deadline, response.chunk())
                .await
                .map_err(|_| response_body_timeout_error(&read_context(), timeout))?,
            None => response.chunk().await,
        }
        .map_err(|source| transport_error(&read_context(), &source, budget))?;
        let Some(chunk) = chunk else { break };

        total = total.saturating_add(chunk.len());
        if total > limit {
            // Not `declared`: it was under the limit, so it does not describe what arrived.
            return Err(oversized_response_error(context, None, budget));
        }
        if let Some(collected) = collected.as_mut() {
            collected.extend_from_slice(&chunk);
        } else if let Some(earlier) = first.take() {
            let mut buffer = bytes::BytesMut::with_capacity(
                usize::try_from(declared.unwrap_or_default())
                    .unwrap_or(usize::MAX)
                    .clamp(earlier.len() + chunk.len(), limit),
            );
            buffer.extend_from_slice(&earlier);
            buffer.extend_from_slice(&chunk);
            collected = Some(buffer);
        } else {
            first = Some(chunk);
        }
    }
    Ok(collected.map_or_else(|| first.unwrap_or_default(), bytes::BytesMut::freeze))
}

/// Builds a `multipart/related` body: a JSON metadata part followed by one media part.
///
/// Both media uploads (`jobs.insert` for a CSV seed, `objects.insert` for a python-model source)
/// use this shape. `boundary` is not searched for in the parts; callers derive it from
/// [`crate::rest::jobs::next_identity`], whose random bits collide only by chance.
pub(crate) fn multipart_related(
    boundary: &str,
    metadata: &[u8],
    media: &[u8],
    media_content_type: &str,
) -> Vec<u8> {
    let (prefix, closing) = multipart_framing(boundary, metadata, media_content_type);
    let mut body = Vec::with_capacity(prefix.len() + media.len() + closing.len());
    body.extend_from_slice(&prefix);
    body.extend_from_slice(media);
    body.extend_from_slice(&closing);
    body
}

/// The bytes before and after the media part, shared by the buffered and streamed builders.
/// `src/dbt/csv_load/tests.rs` checks that the two agree byte for byte.
fn multipart_framing(
    boundary: &str,
    metadata: &[u8],
    media_content_type: &str,
) -> (bytes::Bytes, bytes::Bytes) {
    let opening = format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n");
    let separator = format!("\r\n--{boundary}\r\nContent-Type: {media_content_type}\r\n\r\n");
    let mut prefix = Vec::with_capacity(opening.len() + metadata.len() + separator.len());
    prefix.extend_from_slice(opening.as_bytes());
    prefix.extend_from_slice(metadata);
    prefix.extend_from_slice(separator.as_bytes());
    (
        bytes::Bytes::from(prefix),
        bytes::Bytes::from(format!("\r\n--{boundary}--\r\n")),
    )
}

/// Streams a `multipart/related` body around an already-open file.
///
/// Only the prefix and closing boundary are allocated; the file is read in 64 KiB chunks as
/// reqwest sends. It is capped at `media_length`, so the declared `Content-Length` stays exact if
/// the file grows during the upload.
pub(crate) fn multipart_related_file(
    boundary: &str,
    metadata: &[u8],
    media: std::fs::File,
    media_length: u64,
    media_content_type: &str,
) -> Result<RequestBody> {
    let (prefix, closing) = multipart_framing(boundary, metadata, media_content_type);
    let framing_length = (prefix.len() + closing.len()) as u64;
    let content_length = framing_length.checked_add(media_length).ok_or_else(|| {
        invalid_argument("BigQuery CSV seed multipart body exceeds the HTTP content-length limit")
    })?;

    let media = ReaderStream::with_capacity(
        tokio::fs::File::from_std(media).take(media_length),
        64 * 1024,
    );
    let body = reqwest::Body::wrap(MultipartRelatedFileBody {
        prefix: Some(prefix),
        media,
        closing,
        remaining: content_length,
    });
    Ok(RequestBody {
        body,
        content_length,
    })
}

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;
    use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider};
    use google_cloud_gax::error::CredentialsError;
    use http::Extensions;
    use http::header::HeaderMap;
    use http_body::Body as _;

    use super::{Request, RequestStart, send};
    use crate::rest::client::rest_client;

    /// A request whose connection was never established reached nothing, so it leaves the start
    /// signal unset; one that was sent sets it, and a later unconnected attempt on the same signal
    /// cannot clear what the earlier one may have started.
    #[test]
    fn only_a_request_that_may_have_left_sets_the_start_signal() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let client = rest_client("test").unwrap();
        let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
        let unreachable = format!("{}/unused", crate::test_support::UNREACHABLE_ENDPOINT);
        let attempt = |url: &str, start: &RequestStart| {
            runtime
                .block_on(send(
                    None,
                    &credentials,
                    &client,
                    Request::post_json(url, "{}", "submit it").with_start_signal(start),
                ))
                .unwrap_err()
        };

        let start = RequestStart::default();
        let refused = attempt(&unreachable, &start);
        assert_eq!(refused.status, Status::IO, "{}", refused.message);
        assert!(!start.started(), "{}", refused.message);

        // Read whole, then dropped unanswered: the request may have been acted on.
        let (endpoint, server) =
            crate::test_support::http::capture_requests_responding(1, |_| None);
        attempt(&format!("{endpoint}/submitted"), &start);
        server.served(1);
        assert!(start.started());
        attempt(&unreachable, &start);
        assert!(start.started());
    }

    #[test]
    fn a_truncated_csv_stops_before_the_closing_boundary_with_a_clear_error() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let path = std::env::temp_dir().join(crate::rest::jobs::next_identity("csv-truncated"));
        std::fs::write(&path, b"id\n1\n2\n").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let length = file.metadata().unwrap().len();
        let mut body = super::multipart_related_file("test", b"{}", file, length, "text/csv")
            .unwrap()
            .body;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();

        runtime.block_on(async {
            let prefix = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap();
            assert!(prefix.starts_with(b"--test\r\n"));
            let failure = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap_err();
            let failure = crate::error::request_error("upload CSV", &failure);
            assert_eq!(failure.status, Status::IO);
            assert!(failure.message.contains("file may have been truncated"));
            assert!(body.is_end_stream());
            assert!(
                std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                    .await
                    .is_none()
            );
        });
        std::fs::remove_file(path).unwrap();
    }

    /// Stands in for an environment with no usable Application Default Credentials: the token
    /// fetch fails before any request leaves the process.
    #[derive(Debug)]
    struct UnusableCredentials;

    impl CredentialsProvider for UnusableCredentials {
        async fn headers(
            &self,
            _extensions: Extensions,
        ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
            Err(CredentialsError::new(
                true,
                "Fetching the token resulted in transient errors.",
                std::io::Error::other("failed to lookup address information"),
            ))
        }

        async fn universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_owned())
        }
    }

    /// Credentials that never mint a token get no token diagnosis, so a 401 carries the client's
    /// remedy.
    #[test]
    fn a_401_carries_the_clients_remedy_for_credentials_that_never_mint() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let (endpoint, captured) = crate::test_support::http::capture_requests(vec![(
            401,
            serde_json::json!({"error": {"code": 401, "errors": [{"reason": "required",
                "message": "Login Required.", "location": "Authorization"}]}}),
        )]);
        let client = rest_client("test")
            .unwrap()
            .with_unauthenticated_remedy(Some("send some credentials".to_owned()));
        let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
        let failure = runtime
            .block_on(send(
                None,
                &credentials,
                &client,
                Request::get(&format!("{endpoint}/unused"), "describe it"),
            ))
            .unwrap_err();
        assert_eq!(captured.join().unwrap().len(), 1);
        assert_eq!(failure.status, Status::Unauthenticated);
        assert_eq!(
            failure.message,
            "BigQuery refused the credentials to describe it: required: Login Required. (field: \
             Authorization) (HTTP 401); send some credentials"
        );
    }

    /// An unmintable credential is `Unauthenticated` with the gRPC path's remedy, not a
    /// retryable `IO`.
    #[test]
    fn a_request_whose_credentials_cannot_be_minted_reports_the_authentication_remedy() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let client = rest_client("test").unwrap();
        let credentials = Credentials::from(UnusableCredentials);
        let failure = runtime
            .block_on(send(
                None,
                &credentials,
                &client,
                Request::get("https://bigquery.googleapis.com/unused", "describe it"),
            ))
            .unwrap_err();

        assert_eq!(failure.status, Status::Unauthenticated);
        assert!(
            failure.message.starts_with(
                "failed to authenticate to describe it: no usable Google Cloud \
                 Application Default Credentials, which are read from "
            ),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("bigquery.auth_type"),
            "{}",
            failure.message
        );
    }
}
