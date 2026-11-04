//! Tests for the shared client's connection pool and per-request timeout budgets.

use std::time::Duration;

use super::{
    CONNECT_TIMEOUT, MEDIA_RESPONSE_LIMIT, REQUEST_RESPONSE_LIMIT, REQUEST_TIMEOUT, TimeoutBudget,
    UPLOAD_PROGRESS_TIMEOUT, rest_client, timeout_detail,
};
use crate::rest::request::{Request, send};
use crate::test_support::errors::assert_message;

/// HTTP/2 prior knowledge matches production's ALPN without test certificates. A refused stream,
/// which reqwest may replay for any method, primes the pool as HTTP/2; the server then holds both
/// concurrent responses until it has seen both streams, which HTTP/1.1 could not carry on one
/// socket.
#[test]
fn json_and_media_requests_share_one_http2_connection() {
    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();

            let (refused, mut response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(refused.version(), http::Version::HTTP_2);
            assert_eq!(refused.uri().path(), "/prime");
            response.send_reset(h2::Reason::REFUSED_STREAM);

            let (prime, mut response) =
                tokio::time::timeout(Duration::from_secs(10), connection.accept())
                    .await
                    .expect("reqwest did not retry the safely refused HTTP/2 stream")
                    .unwrap()
                    .unwrap();
            assert_eq!(prime.version(), http::Version::HTTP_2);
            assert_eq!(prime.uri().path(), "/prime");
            response
                .send_response(http::Response::new(()), true)
                .unwrap();

            let first = tokio::time::timeout(Duration::from_secs(10), connection.accept())
                .await
                .expect("the first concurrent HTTP/2 stream did not arrive")
                .unwrap()
                .unwrap();
            let second = tokio::time::timeout(Duration::from_secs(10), connection.accept())
                .await
                .expect("the second request opened another connection instead of a stream")
                .unwrap()
                .unwrap();
            let mut paths = [first.0.uri().path(), second.0.uri().path()];
            paths.sort_unstable();
            assert_eq!(paths, ["/first", "/second"]);

            for (request, mut response) in [first, second] {
                assert_eq!(request.version(), http::Version::HTTP_2);
                let path = request.uri().path();
                let mut stream = response
                    .send_response(http::Response::new(()), false)
                    .unwrap();
                stream
                    .send_data(bytes::Bytes::copy_from_slice(path.as_bytes()), true)
                    .unwrap();
            }
            connection.graceful_shutdown();
            assert!(connection.accept().await.is_none());
        });

        let client = super::build_client(
            "HTTP/2 test",
            reqwest::Client::builder()
                .no_proxy()
                .http2_prior_knowledge(),
        )
        .unwrap();
        let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
        let (status, body) = send(
            None,
            &credentials,
            &client,
            Request::get(&format!("{endpoint}/prime"), "prime the connection"),
        )
        .await
        .unwrap();
        assert!(status.is_success());
        assert!(body.is_empty());

        let first_url = format!("{endpoint}/first");
        let second_url = format!("{endpoint}/second");
        let first = send(
            None,
            &credentials,
            &client,
            Request::get(&first_url, "send the first concurrent request"),
        );
        let media_client = client.clone();
        let second = send(
            None,
            &credentials,
            &media_client,
            Request::post_media(
                &second_url,
                "application/octet-stream",
                bytes::Bytes::new(),
                "send the concurrent media request",
            ),
        );
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().1, bytes::Bytes::from_static(b"/first"));
        assert_eq!(second.unwrap().1, bytes::Bytes::from_static(b"/second"));
        server.await.unwrap();
    });
}

/// Each request carries its own whole-request policy over one shared client, and each timeout
/// names itself, its length, and that no option sets it.
#[test]
fn per_request_timeouts_name_and_apply_their_own_budget() {
    // Building any reqwest client needs the rustls provider the driver runtime installs.
    let _runtime = crate::runtime::new_runtime().unwrap();
    let client = rest_client("test").unwrap();
    for (budget, connecting, expected) in [
        (TimeoutBudget::Request, true, "30 s HTTP connect timeout"),
        (TimeoutBudget::Request, false, "60 s HTTP request timeout"),
        (
            TimeoutBudget::MediaUpload,
            true,
            "30 s HTTP connect timeout",
        ),
        (
            TimeoutBudget::MediaUpload,
            false,
            "120 s HTTP upload progress timeout",
        ),
    ] {
        let detail = timeout_detail(budget, connecting);
        assert!(detail.contains(expected), "{detail}");
        assert!(detail.starts_with("the driver's fixed "), "{detail}");
    }

    let request = client
        .request(
            reqwest::Method::GET,
            "http://127.0.0.1:1/json",
            TimeoutBudget::Request,
        )
        .build()
        .unwrap();
    assert_eq!(request.timeout(), Some(&REQUEST_TIMEOUT));
    let upload = client
        .request(
            reqwest::Method::POST,
            "http://127.0.0.1:1/upload",
            TimeoutBudget::MediaUpload,
        )
        .build()
        .unwrap();
    assert_eq!(upload.timeout(), None);

    assert!(
        CONNECT_TIMEOUT < REQUEST_TIMEOUT && REQUEST_TIMEOUT < UPLOAD_PROGRESS_TIMEOUT,
        "a connect must give up before a whole request, and a request before a stalled upload"
    );
}

/// Both ways an endpoint can exceed the buffer limit are refused, naming the limit: an announced
/// `Content-Length` before any body byte is read, and an unannounced body as it streams.
#[test]
fn an_oversized_response_body_is_refused_by_the_requests_budget() {
    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let over = MEDIA_RESPONSE_LIMIT + (1 << 20);
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

            async fn answer(
                listener: &tokio::net::TcpListener,
                head: &str,
            ) -> tokio::net::TcpStream {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                }
                socket.write_all(head.as_bytes()).await.unwrap();
                socket
            }

            // Announced: a length over the ceiling, and then no body at all.
            let mut announced = answer(
                &listener,
                &format!("HTTP/1.1 200 OK\r\nContent-Length: {over}\r\n\r\n"),
            )
            .await;
            announced.flush().await.unwrap();

            // Streamed: no length at all, so only the ceiling can end it.
            let mut streamed =
                answer(&listener, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n").await;
            let chunk = vec![b'x'; 64 * 1024];
            for _ in 0..over.div_ceil(chunk.len()) {
                // The driver stops reading at its ceiling, so this write fails partway through.
                if streamed.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });

        // Plain HTTP/1.1 against a hand-written response, so the test owns the exact framing.
        let client =
            super::build_client("response limit test", reqwest::Client::builder().no_proxy())
                .unwrap();
        let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
        for (path, expected) in [
            (
                "/announced",
                format!("the service announced a {over}-byte response body"),
            ),
            ("/streamed", "the response body kept arriving".to_owned()),
        ] {
            let url = format!("{endpoint}{path}");
            let failure = send(
                None,
                &credentials,
                &client,
                Request::post_media(
                    &url,
                    "application/octet-stream",
                    bytes::Bytes::new(),
                    "upload the seed",
                ),
            )
            .await
            .unwrap_err();
            assert_eq!(failure.status, adbc_core::error::Status::InvalidData);
            assert_message(
                &failure,
                &[&expected, &format!("{MEDIA_RESPONSE_LIMIT}-byte limit")],
            );
            assert!(
                failure.message.starts_with("failed to upload the seed"),
                "{}",
                failure.message
            );
        }
        server.await.unwrap();
    });
}

/// The request ceiling is far above BigQuery's 10 MB result-page cap; the upload ceiling, for one
/// small resource document, is the tighter of the two.
#[test]
fn the_response_ceilings_clear_every_documented_bigquery_response() {
    const BIGQUERY_MAXIMUM_RESPONSE: usize = 10_000_000;
    assert!(TimeoutBudget::MediaUpload.response_limit() < TimeoutBudget::Request.response_limit());
    assert_eq!(
        TimeoutBudget::Request.response_limit(),
        REQUEST_RESPONSE_LIMIT
    );
    const { assert!(MEDIA_RESPONSE_LIMIT < BIGQUERY_MAXIMUM_RESPONSE) };
    const { assert!(REQUEST_RESPONSE_LIMIT > BIGQUERY_MAXIMUM_RESPONSE * 6) };
    assert!(TimeoutBudget::Request.response_body_timeout().is_none());
    assert!(TimeoutBudget::MediaUpload.response_body_timeout().is_some());
}

/// How the upload endpoint in [`upload_through`] treats the body it is sent.
#[derive(Clone, Copy)]
enum UploadEndpoint {
    /// Reads at most `bytes` every `pause`, then answers once it has all of them.
    Paced { bytes: usize, pause: Duration },
    /// Reads the request headers and nothing more.
    Stalled,
    /// Reads the whole body and never answers.
    Silent,
}

/// Sends a `size`-byte media upload to an endpoint that behaves as `endpoint` says, under a
/// shortened upload progress timeout.
fn upload_through(
    endpoint: UploadEndpoint,
    size: usize,
    progress_timeout: Duration,
) -> adbc_core::error::Result<reqwest::StatusCode> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let runtime = crate::runtime::new_runtime().unwrap();
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/upload", listener.local_addr().unwrap());
        let (finished, mut stop) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(socket.read_u8().await.unwrap());
            }
            let mut buffer = vec![0_u8; 1 << 20];
            let mut seen = 0;
            match endpoint {
                UploadEndpoint::Paced { bytes, pause } => {
                    while seen < size {
                        // A client that timed out closes or resets; the test reports its error.
                        let Ok(read @ 1..) = socket.read(&mut buffer[..bytes]).await else {
                            return;
                        };
                        seen += read;
                        tokio::time::sleep(pause).await;
                    }
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                        .await
                        .unwrap();
                }
                UploadEndpoint::Stalled => {}
                UploadEndpoint::Silent => {
                    while seen < size {
                        seen += socket.read(&mut buffer).await.unwrap();
                    }
                }
            }
            // Holds the connection open until the client has given up on it.
            let _ = (&mut stop).await;
        });

        let client = super::build_client("upload test", reqwest::Client::builder().no_proxy())
            .unwrap()
            .with_upload_progress_timeout(progress_timeout);
        let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
        let answer = send(
            None,
            &credentials,
            &client,
            Request::post_media(
                &url,
                "application/octet-stream",
                bytes::Bytes::from(vec![b'x'; size]),
                "upload the seed",
            ),
        )
        .await;
        let _ = finished.send(());
        server.await.unwrap();
        answer.map(|(status, _)| status)
    })
}

/// A seed can take far longer to send than any fixed deadline, so an upload that keeps moving
/// is never timed out, however long it takes.
///
/// The transport takes frames as the kernel frees buffer space, so gaps between frames grow with
/// the buffer (megabytes on macOS) over the read rate. The body and rate keep them under the
/// window.
#[test]
fn an_upload_that_keeps_moving_outlasts_the_progress_timeout() {
    let progress_timeout = Duration::from_secs(1);
    let started = std::time::Instant::now();
    let status = upload_through(
        UploadEndpoint::Paced {
            bytes: 256 * 1024,
            pause: Duration::from_millis(5),
        },
        128 << 20,
        progress_timeout,
    )
    .unwrap();
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        started.elapsed() > progress_timeout * 2,
        "the upload finished within {:?}, too fast to show anything",
        started.elapsed()
    );
}

/// An upload the endpoint stops taking, and one it takes whole but never answers, are both
/// timed out, naming the budget that fired.
#[test]
fn an_upload_that_stops_moving_is_timed_out() {
    for (endpoint, size) in [
        (UploadEndpoint::Stalled, 32 << 20),
        (UploadEndpoint::Silent, 1024),
    ] {
        let failure = upload_through(endpoint, size, Duration::from_millis(300)).unwrap_err();
        assert_eq!(failure.status, adbc_core::error::Status::Timeout);
        assert_message(
            &failure,
            &["failed to upload the seed", "HTTP upload progress timeout"],
        );
    }
}

/// Every request names the driver and its version, as the Go driver's requests name it, and asks
/// for compact JSON whether or not its URL already has a query.
#[test]
fn every_request_names_the_driver_and_asks_for_compact_json() {
    let runtime = crate::runtime::new_runtime().unwrap();
    let (endpoint, captured) = crate::test_support::http::capture_requests(vec![
        (200, serde_json::json!({})),
        (200, serde_json::json!({})),
    ]);
    let credentials = google_cloud_auth::credentials::anonymous::Builder::new().build();
    let client = rest_client("test").unwrap();
    for target in ["/agent", "/agent?fields=etag"] {
        let url = format!("{endpoint}{target}");
        runtime
            .block_on(send(
                None,
                &credentials,
                &client,
                Request::get(&url, "describe it"),
            ))
            .unwrap();
    }
    let captured = captured.join().unwrap();
    for (request, target) in captured.iter().zip(["/agent", "/agent?fields=etag"]) {
        assert_eq!(request.path(), target);
        assert!(request.compact, "{target}");
        assert_eq!(
            request.header("user-agent"),
            Some(concat!("bigquery-adbc/", env!("CARGO_PKG_VERSION")))
        );
    }
}
