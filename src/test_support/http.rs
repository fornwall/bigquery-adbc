//! Scripted HTTP responses and request capture on a loopback socket.
//!
//! Use [`capture_requests`] for fixed responses, [`capture_requests_responding`] for responses
//! derived from requests, and [`capture_requests_until_stopped`] for variable request counts.
//! [`CapturedServer::served`] then yields what the first two captured.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::ops::ControlFlow;
use std::sync::{OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value as JsonValue, json};

/// A `jobReference` in the driver fixtures' `EU` location.
pub(crate) fn job_reference(project_id: &str, job_id: &str) -> JsonValue {
    json!({"projectId": project_id, "jobId": job_id, "location": "EU"})
}

/// A BigQuery REST error with one reason and message.
pub(crate) fn error_body(reason: &str, message: &str) -> JsonValue {
    json!({"error": {"errors": [{"reason": reason, "message": message}]}})
}

/// A staged 200 answer. Unlike the bare tuple, rustfmt keeps a multi-line `json!` body on its line.
pub(crate) fn ok(body: JsonValue) -> (u16, JsonValue) {
    (200, body)
}

/// A staged 200 answer for finished job `job_id` of `fixture-project`: `fields`, usually `json!({})`
/// or a `statementType`, `schema` and `rows`, plus its `jobReference` and `jobComplete`.
pub(crate) fn completed_job(job_id: &str, mut fields: JsonValue) -> (u16, JsonValue) {
    fields["jobReference"] = job_reference("fixture-project", job_id);
    fields["jobComplete"] = json!(true);
    ok(fields)
}

/// One captured HTTP request: its request line, headers, raw body, and that body parsed as JSON
/// (`Null` when the body is empty or not JSON, as for a multipart upload).
pub(crate) struct CapturedRequest {
    /// Request line with the driver's trailing `prettyPrint=false` parameter removed.
    pub(crate) line: String,
    /// Whether the target ended with `prettyPrint=false`.
    pub(crate) compact: bool,
    headers: String,
    pub(crate) body: JsonValue,
    pub(crate) raw_body: Vec<u8>,
}

impl CapturedRequest {
    /// The request target from the request line, e.g. `/bigquery/v2/projects/p/jobs?...`.
    pub(crate) fn path(&self) -> &str {
        self.line
            .split_ascii_whitespace()
            .nth(1)
            .unwrap_or_default()
    }

    /// The SQL a `jobs.query` or `jobs.insert` request carries.
    pub(crate) fn sql(&self) -> &str {
        self.body["query"]
            .as_str()
            .or_else(|| self.body["configuration"]["query"]["query"].as_str())
            .expect("captured BigQuery request has SQL")
    }

    /// The `connectionProperties` a `jobs.query` or `jobs.insert` request carries.
    pub(crate) fn connection_properties(&self) -> &JsonValue {
        match &self.body["connectionProperties"] {
            JsonValue::Null => &self.body["configuration"]["query"]["connectionProperties"],
            properties => properties,
        }
    }

    /// The trimmed value of a request header, matched case-insensitively.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers.lines().find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

/// Reads one request with a `Content-Length` body. Chunked requests are unsupported.
///
/// Returns `None` for a connection closed before any bytes arrive: reqwest's pool can open
/// unused connections. A partial request panics.
pub(crate) fn read_captured_request(stream: &mut TcpStream) -> Option<CapturedRequest> {
    stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT)).unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let (header_end, content_length) = loop {
        let count = stream.read(&mut buffer).unwrap();
        if count == 0 {
            assert!(
                bytes.is_empty(),
                "HTTP request ended before its headers: {:?}",
                String::from_utf8_lossy(&bytes)
            );
            return None;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = header_end + 4;
            let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (header_end, content_length);
        }
    };
    while bytes.len() < header_end + content_length {
        let count = stream.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "HTTP request ended before its body");
        bytes.extend_from_slice(&buffer[..count]);
    }
    let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let raw_body = bytes[header_end..header_end + content_length].to_vec();
    let mut line = headers.lines().next().unwrap_or_default().to_owned();
    // The parameter's trailing space ends the target, and stays.
    let compact = ["?prettyPrint=false ", "&prettyPrint=false "]
        .iter()
        .any(|parameter| {
            line.find(parameter)
                .map(|at| line.replace_range(at..at + parameter.len() - 1, ""))
                .is_some()
        });
    Some(CapturedRequest {
        line,
        compact,
        headers: headers.to_owned(),
        body: serde_json::from_slice(&raw_body).unwrap_or(JsonValue::Null),
        raw_body,
    })
}

// Deadlock guards with room for CPU contention during concurrent builds and tests.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Accepts a connection or returns `None` on a stop signal. Panics after [`ACCEPT_TIMEOUT`].
///
/// The stream is blocking whatever the listener is: on Windows it inherits the listener's
/// non-blocking mode, and [`read_captured_request`] needs its reads to wait.
pub(crate) fn accept(
    listener: &TcpListener,
    stop: Option<&mpsc::Receiver<()>>,
) -> Option<TcpStream> {
    let deadline = Instant::now() + ACCEPT_TIMEOUT;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                break Some(stream);
            }
            Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
                if let Some(stop) = stop
                    && !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty))
                {
                    break None;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for captured BigQuery request"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Err(source) => panic!("accepting captured BigQuery request failed: {source}"),
        }
    }
}

/// Writes a JSON response, ignoring write errors because cancellation tests close the client.
pub(crate) fn write_response(stream: &mut TcpStream, status: u16, response: &JsonValue) {
    let response = serde_json::to_vec(response).unwrap();
    // HTTP/1.1 permits an empty reason phrase for codes without a canonical one.
    let code = reqwest::StatusCode::from_u16(status)
        .unwrap_or_else(|_| panic!("mock HTTP status {status} is not a status code"));
    let reason = code.canonical_reason().unwrap_or_default();
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.len()
    )
    .and_then(|()| stream.write_all(&response))
    .and_then(|()| stream.flush());
}

/// A mock that ends once it has served every request it was staged for.
pub(crate) struct CapturedServer(thread::JoinHandle<Vec<CapturedRequest>>);

impl CapturedServer {
    /// Waits for the server to end, yielding what it captured or the panic that ended it.
    pub(crate) fn join(self) -> thread::Result<Vec<CapturedRequest>> {
        self.0.join()
    }

    /// Waits for the server to end and asserts that it captured exactly `count` requests.
    #[track_caller]
    pub(crate) fn served(self, count: usize) -> Vec<CapturedRequest> {
        let requests = self.join().unwrap();
        assert_eq!(requests.len(), count);
        requests
    }
}

/// Serves the given `(status, JSON body)` responses in order, capturing each request.
///
/// Returns the endpoint to point a client at and the handle that yields the captured
/// requests once every response has been served.
pub(crate) fn capture_requests(responses: Vec<(u16, JsonValue)>) -> (String, CapturedServer) {
    capture_requests_gated(responses, || true)
}

/// Like [`capture_requests`], but calls `respond` after the first request has been read and
/// captured; returning `false` closes the connection without answering, ending the server.
pub(crate) fn capture_requests_gated(
    responses: Vec<(u16, JsonValue)>,
    respond: impl FnOnce() -> bool + Send + 'static,
) -> (String, CapturedServer) {
    serve_gated(false, responses, respond)
}

fn serve_gated(
    jobs: bool,
    responses: Vec<(u16, JsonValue)>,
    respond: impl FnOnce() -> bool + Send + 'static,
) -> (String, CapturedServer) {
    let mut respond = Some(respond);
    let (endpoint, handle) = serve(responses.len(), None, jobs, move |index, _| {
        if let Some(respond) = respond.take()
            && !respond()
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(Some(responses[index].clone()))
    });
    (endpoint, CapturedServer(handle))
}

/// Like [`capture_requests`], for a connection's statements, whose session control runs as jobs.
///
/// Each statement takes the next staged answer whichever route it arrives on: [`job_answer`] turns
/// one for a `jobs.insert` into the job BigQuery would name. Polls of a finished job are answered
/// here and neither captured nor staged, so the captured requests are the statements.
pub(crate) fn capture_statements(responses: Vec<(u16, JsonValue)>) -> (String, CapturedServer) {
    capture_statements_gated(responses, || true)
}

/// [`capture_requests_gated`] for [`capture_statements`].
pub(crate) fn capture_statements_gated(
    responses: Vec<(u16, JsonValue)>,
    respond: impl FnOnce() -> bool + Send + 'static,
) -> (String, CapturedServer) {
    serve_gated(true, responses, respond)
}

/// [`capture_requests_responding`] for [`capture_statements`].
pub(crate) fn capture_statements_responding(
    count: usize,
    mut respond: impl FnMut(&CapturedRequest) -> Option<(u16, JsonValue)> + Send + 'static,
) -> (String, CapturedServer) {
    let (endpoint, handle) = serve(count, None, true, move |_, request| {
        ControlFlow::Continue(respond(request))
    });
    (endpoint, CapturedServer(handle))
}

/// What BigQuery answers a real `jobs.insert` with, given the answer staged for its statement: a
/// 200 is the finished job, and a statement it refuses is a finished job with an `errorResult`. A
/// 409, a 5xx, a dry run and any other request keep the staged answer.
fn job_answer(request: &CapturedRequest, status: u16, answer: JsonValue) -> (u16, JsonValue) {
    let mut reference = request.body["jobReference"].clone();
    if reference.is_null()
        || request.body["configuration"]["dryRun"] == true
        || status == 409
        || status >= 500
    {
        return (status, answer);
    }
    if reference["location"].is_null() {
        reference["location"] = json!("EU");
    }
    let mut job = json!({"jobReference": reference, "status": {"state": "DONE"}});
    if status != 200 {
        let error = &answer["error"];
        job["status"]["errorResult"] = error["errors"]
            .get(0)
            .cloned()
            .unwrap_or_else(|| json!({"reason": "invalid", "message": error["message"]}));
    }
    (200, job)
}

/// Whether the driver polls the results of the job `response` names next. A dry run has none.
fn names_a_finished_job(request: &CapturedRequest, status: u16, response: &JsonValue) -> bool {
    status == 200
        && response["status"]["state"] == "DONE"
        && response["status"]["errorResult"].is_null()
        && request.body["configuration"]["dryRun"] != true
}

/// Derives each of `count` responses from its request, for example to echo a generated job ID.
/// Returning `None` closes that connection without answering, simulating a lost response.
pub(crate) fn capture_requests_responding(
    count: usize,
    mut respond: impl FnMut(&CapturedRequest) -> Option<(u16, JsonValue)> + Send + 'static,
) -> (String, CapturedServer) {
    let (endpoint, handle) = serve(count, None, false, move |_, request| {
        ControlFlow::Continue(respond(request))
    });
    (endpoint, CapturedServer(handle))
}

/// A mock stopped by the test, for loops whose request count depends on jittered backoff.
pub(crate) struct StoppableServer {
    stop: mpsc::Sender<()>,
    handle: thread::JoinHandle<Vec<CapturedRequest>>,
}

impl StoppableServer {
    /// Ends the server and yields the requests it captured before it was stopped.
    pub(crate) fn stop(self) -> Vec<CapturedRequest> {
        // A send failure means the server thread already finished: it served every staged
        // response, or it panicked and `join` reports that.
        let _ = self.stop.send(());
        self.handle.join().unwrap()
    }
}

/// Like [`capture_requests`], but serves *up to* the staged responses and stops when the test
/// says so. See [`StoppableServer`].
pub(crate) fn capture_requests_until_stopped(
    responses: Vec<(u16, JsonValue)>,
) -> (String, StoppableServer) {
    let (stop, stopped) = mpsc::channel();
    let (endpoint, handle) = serve(responses.len(), Some(stopped), false, move |index, _| {
        ControlFlow::Continue(Some(responses[index].clone()))
    });
    (endpoint, StoppableServer { stop, handle })
}

/// A staged 200 `jobs.get` answer for done job `job_id` of `fixture-project`: `fields`, usually
/// `json!({})` or `statistics`, plus its `jobReference` and `status.state`.
pub(crate) fn done_job(job_id: &str, mut fields: JsonValue) -> (u16, JsonValue) {
    fields["jobReference"] = job_reference("fixture-project", job_id);
    fields["status"]["state"] = json!("DONE");
    ok(fields)
}

/// A staged Google API rejection with HTTP status `code`, for a test whose document needs more
/// than [`error_body`]. An empty `status` or `reason` leaves that part out, as some APIs do.
pub(crate) fn google_rejection(
    code: u16,
    status: &str,
    reason: &str,
    message: &str,
) -> (u16, JsonValue) {
    let mut error = json!({"code": code, "message": message});
    if !status.is_empty() {
        error["status"] = json!(status);
    }
    if !reason.is_empty() {
        error["errors"] = json!([{"message": message, "domain": "global", "reason": reason}]);
    }
    (code, json!({ "error": error }))
}

/// Answers one request with an HTTP `status` redirect to `location`, which [`write_response`]
/// cannot send, yielding the request it answered.
pub(crate) fn redirect_once(
    status: u16,
    location: String,
) -> (String, thread::JoinHandle<CapturedRequest>) {
    let (listener, endpoint) = mock_listener();
    let handle = thread::spawn(move || {
        // Unused connections from reqwest's pool carry no request.
        let (mut stream, request) = loop {
            let mut stream = accept(&listener, None).unwrap();
            if let Some(request) = read_captured_request(&mut stream) {
                break (stream, request);
            }
        };
        retire(listener);
        let _ = write!(
            stream,
            "HTTP/1.1 {status} Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        request
    });
    (endpoint, handle)
}

/// An `https` endpoint whose first connection stalls in its TLS handshake until the test releases
/// it, then fails, so a request there has started but fails to connect: reqwest reports a
/// connection it could not establish, which proves no byte of the request was sent.
///
/// The receiver yields once the client's first handshake bytes arrive. Sending on the sender, or
/// dropping it, closes that connection unanswered; later connections are closed at once.
pub(crate) fn stalled_tls_handshake() -> (String, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (listener, endpoint) = mock_listener();
    let endpoint = endpoint.replacen("http://", "https://", 1);
    let (connecting, connected) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    thread::spawn(move || {
        if let Some(mut stream) = accept(&listener, None) {
            let _ = stream.read(&mut [0; 1]);
            let _ = connecting.send(());
            let _ = released.recv();
        }
        retire(listener);
    });
    (endpoint, connected, release)
}

/// A [`loopback_listener`] in the non-blocking mode [`accept`] polls, with its endpoint.
pub(crate) fn mock_listener() -> (TcpListener, String) {
    let listener = loopback_listener();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    (listener, endpoint)
}

/// A listener on a fresh loopback port.
///
/// Retries address-exhaustion errors for one second: concurrent suites can fill the ephemeral
/// port range with TIME_WAIT sockets.
pub(crate) fn loopback_listener() -> TcpListener {
    let mut attempts = 0;
    loop {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => return listener,
            Err(source)
                if attempts < 10
                    && matches!(
                        source.kind(),
                        std::io::ErrorKind::AddrInUse | std::io::ErrorKind::AddrNotAvailable
                    ) =>
            {
                attempts += 1;
                thread::sleep(Duration::from_millis(100));
            }
            Err(source) => panic!(
                "binding a mock endpoint to a loopback port failed: {source}; the host may be out \
                 of ephemeral ports (`ss -s` counts the TIME_WAIT sockets that hold them)"
            ),
        }
    }
}

/// Serves up to `count` requests on a fresh loopback listener, capturing each one.
///
/// `respond` sees each request with its index and either answers it (`Some`), closes its
/// connection unanswered (`None`), or ends the server (`Break`, before the request is answered).
/// Without a `stop` signal the server panics once [`ACCEPT_TIMEOUT`] passes with requests still
/// owed; with one, the signal ends the wait instead. `jobs` selects [`capture_statements`].
fn serve(
    count: usize,
    stop: Option<mpsc::Receiver<()>>,
    jobs: bool,
    mut respond: impl FnMut(usize, &CapturedRequest) -> ControlFlow<(), Option<(u16, JsonValue)>>
    + Send
    + 'static,
) -> (String, thread::JoinHandle<Vec<CapturedRequest>>) {
    let (listener, endpoint) = mock_listener();
    let handle = thread::spawn(move || {
        // Retire the port even after a panic, so stray requests cannot reach another test.
        let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut requests = Vec::with_capacity(count);
            let mut poll_owed = false;
            // Unused connections from reqwest's pool must not consume staged responses.
            while requests.len() < count || poll_owed {
                let Some(mut stream) = accept(&listener, stop.as_ref()) else {
                    break;
                };
                let Some(request) = read_captured_request(&mut stream) else {
                    continue;
                };
                if jobs && request.line.starts_with("GET") && request.path().contains("/queries/") {
                    poll_owed = false;
                    write_response(&mut stream, 200, &json!({"jobComplete": true}));
                    continue;
                }
                let reply = respond(requests.len(), &request);
                if let ControlFlow::Continue(Some((status, response))) = reply {
                    let (status, response) = if jobs {
                        job_answer(&request, status, response)
                    } else {
                        (status, response)
                    };
                    poll_owed = jobs && names_a_finished_job(&request, status, &response);
                    write_response(&mut stream, status, &response);
                } else if reply.is_break() {
                    requests.push(request);
                    break;
                }
                requests.push(request);
            }
            requests
        }));
        retire(listener);
        served.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    });
    (endpoint, handle)
}

/// Bounds how long each retired port holds a file descriptor.
const RETIREMENT: Duration = Duration::from_secs(10);

/// Keeps a finished mock's port bound for [`RETIREMENT`], closing late connections.
///
/// Background cancellation and retries can outlive a test. Reusing its port immediately could
/// send those requests to an unrelated mock and consume its staged responses. Tests that bind
/// their own listeners must retire them too.
pub(crate) fn retire(listener: TcpListener) {
    static RETIRED: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime = RETIRED.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("retired-mock-ports")
            .enable_all()
            .build()
            .unwrap()
    });
    listener.set_nonblocking(true).unwrap();
    runtime.spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let hang_up = async { while listener.accept().await.is_ok() {} };
        let _ = tokio::time::timeout(RETIREMENT, hang_up).await;
    });
}

#[test]
fn a_panicking_mock_still_retires_its_port() {
    let (endpoint, server) =
        capture_requests_responding(1, |_| panic!("a staged assertion failed"));
    let address = endpoint.trim_start_matches("http://");
    let mut client = TcpStream::connect(address).unwrap();
    client
        .write_all(b"GET /panic HTTP/1.1\r\nHost: mock\r\n\r\n")
        .unwrap();
    assert!(server.join().is_err());
    assert_eq!(
        TcpListener::bind(address).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
}
