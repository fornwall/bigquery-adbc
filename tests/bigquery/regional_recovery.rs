//! Drop real BigQuery submission responses to exercise recovery in a single region.

use std::io::{BufRead as _, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use adbc_core::error::Status;
use adbc_core::{Database as _, Statement as _};
use adbc_driver_bigquery::{
    BigQueryConnection, BigQueryDatabaseOption, BigQueryQueryPriority, BigQueryStatementOption,
};
use google_cloud_auth::credentials::{Builder, CacheableResource, Credentials};
use serde_json::{Value, json};

use crate::support::{
    connect_at_location, execute_sql, open_database, prepare_query, project, read_single_i64,
    report_failed_cleanup, scratch_table, set_option,
};

const REGION: &str = "asia-northeast1";
const WAIT: Duration = Duration::from_secs(90);

#[test]
#[ignore = "requires ADC and permission to create a disposable asia-northeast1 dataset"]
fn regional_recovery_survives_lost_submission_responses() {
    crate::backend::require_real_bigquery();
    let api = Arc::new(LiveApi::new());
    let dataset = scratch_table("regional_recovery");
    // Armed first: a create whose response was lost still made the dataset.
    let fixture = Fixture {
        api: api.clone(),
        dataset_path: format!("/bigquery/v2/projects/{}/datasets/{dataset}", project()),
        jobs: Arc::new(Mutex::new(Vec::new())),
    };
    let (status, _) = api
        .send(
            "POST",
            &format!("/bigquery/v2/projects/{}/datasets", project()),
            Some(json!({
                "datasetReference": {"projectId": project(), "datasetId": dataset},
                "location": REGION,
                "defaultTableExpirationMs": "3600000"
            })),
        )
        .unwrap();
    assert_eq!(status, 200, "create regional fixture dataset");
    let table = format!("{}.{dataset}.markers", project());
    let mut control = connect_at_location(project(), REGION);
    execute_sql(
        &mut control,
        &format!("CREATE TABLE `{table}` (marker STRING)"),
    );

    for (loss, marker) in [(Loss::First, "conflict"), (Loss::Every, "listing")] {
        let proxy = Proxy::new(api.clone(), fixture.jobs.clone(), loss);
        let mut connection = proxy.connection();
        let mut statement = prepare_query(
            &mut connection,
            &format!("INSERT INTO `{table}` VALUES ('{marker}')"),
        );
        job_options(&mut statement);
        assert_eq!(statement.execute_update().unwrap(), Some(1));
        let records = proxy.finish();
        assert_recovery(&records, loss);
        let mut count = prepare_query(
            &mut control,
            &format!("SELECT COUNT(*) FROM `{table}` WHERE marker = '{marker}'"),
        );
        assert_eq!(
            read_single_i64(&mut count),
            1,
            "recovery duplicated the mutation"
        );
    }

    let proxy = Proxy::new(api.clone(), fixture.jobs.clone(), Loss::Cancel);
    let mut connection = proxy.connection();
    // Reading the tiny regional table makes BigQuery infer its region. The generated cross join
    // leaves a window to cancel, without scanning a large billed table.
    let mut statement = prepare_query(
        &mut connection,
        &format!(
            "SELECT COUNT(*) FROM `{table}` CROSS JOIN UNNEST(GENERATE_ARRAY(1, 300000)) a \
             CROSS JOIN UNNEST(GENERATE_ARRAY(1, 300000)) b"
        ),
    );
    job_options(&mut statement);
    let cancellation = statement.get_cancel_handle();
    let execution = thread::spawn(move || statement.execute_update());
    let reference = proxy.accepted.recv_timeout(WAIT).unwrap();
    // The proxy drops the held submission response only after observing the first cancellation
    // request. Recovery therefore starts with neither a response nor a known job location.
    cancellation.try_cancel().unwrap();
    assert_eq!(
        execution.join().unwrap().unwrap_err().status,
        Status::Cancelled
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let (status, job) = api.send("GET", &job_path(&reference), None).unwrap();
        assert_eq!(status, 200);
        if job["status"]["state"] == "DONE" {
            assert_eq!(job["status"]["errorResult"]["reason"], "stopped");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remote job survived cancellation"
        );
        thread::sleep(Duration::from_millis(250));
    }
    assert_recovery(&proxy.finish(), Loss::Cancel);
    drop(fixture);
}

fn job_options(statement: &mut adbc_driver_bigquery::BigQueryStatement) {
    // Batch priority selects jobs.insert without supplying a destination/default dataset hint
    // that would let recovery avoid the cross-region listing.
    set_option(
        statement,
        BigQueryStatementOption::Priority,
        BigQueryQueryPriority::Batch,
    );
    set_option(statement, BigQueryStatementOption::JobTimeout, 120_000_i64);
    set_option(
        statement,
        BigQueryStatementOption::DisableQueryCache,
        "true",
    );
}

#[derive(Clone, Copy)]
enum Loss {
    First,
    Every,
    Cancel,
}

#[derive(Debug)]
struct Record {
    method: String,
    path: String,
    request: Vec<u8>,
    status: u16,
    response: Value,
    dropped: bool,
}

fn assert_recovery(records: &[Record], loss: Loss) {
    let inserts: Vec<_> = records
        .iter()
        .filter(|r| r.method == "POST" && r.path.split('?').next().unwrap().ends_with("/jobs"))
        .collect();
    let first = inserts.first().unwrap();
    assert_eq!(first.status, 200);
    assert!(first.dropped);
    let request: Value = serde_json::from_slice(&first.request).unwrap();
    let reference = first.response.get("jobReference").unwrap();
    let requested = request.get("jobReference").unwrap();
    assert_eq!(requested["projectId"], project());
    assert!(requested.get("location").is_none());
    assert_eq!(reference["projectId"], project());
    assert_eq!(reference["location"], REGION);
    assert_eq!(reference["jobId"], requested["jobId"]);
    for replay in inserts.iter().skip(1) {
        assert_eq!(replay.request, first.request, "submission identity changed");
        assert_eq!(
            replay.status, 409,
            "the real service must reject the duplicate"
        );
    }
    assert!(
        records.iter().any(|r| r.method == "GET"
            && r.status == 200
            && r.path.starts_with(&job_path(reference))),
        "no successful regional job lookup"
    );
    match loss {
        Loss::First => {
            assert_eq!(inserts.len(), 2);
            assert!(!inserts.get(1).unwrap().dropped);
            assert!(
                !records
                    .iter()
                    .any(|r| r.method == "GET" && r.path.contains("/jobs?"))
            );
        }
        Loss::Every | Loss::Cancel => {
            assert!(inserts.iter().all(|r| r.dropped));
            assert!(
                records.iter().any(|r| r.method == "GET"
                    && r.status == 200
                    && r.path.contains("/jobs?")
                    && r.response
                        .get("jobs")
                        .and_then(Value::as_array)
                        .is_some_and(|jobs| jobs
                            .iter()
                            .any(|job| job["jobReference"] == *reference))),
                "cross-region jobs.list never exposed the submitted job"
            );
            assert!(
                records.iter().any(|r| r.method == "GET"
                    && r.status == 404
                    && r.path.contains("/jobs/")
                    && !r.path.contains("location=")),
                "unlocated jobs.get unexpectedly found the regional job"
            );
            if matches!(loss, Loss::Cancel) {
                assert_eq!(inserts.len(), 1, "cancelled submission was replayed");
                let cancel = job_path(reference).replace("?location=", "/cancel?location=");
                assert!(
                    records.iter().any(|r| r.method == "POST"
                        && r.status == 200
                        && r.path.starts_with(&cancel)),
                    "no successful regional cancellation"
                );
            }
        }
    }
}

fn job_path(reference: &Value) -> String {
    format!(
        "/bigquery/v2/projects/{}/jobs/{}?location={}",
        reference["projectId"].as_str().unwrap(),
        reference["jobId"].as_str().unwrap(),
        reference["location"].as_str().unwrap()
    )
}

struct LiveApi {
    runtime: tokio::runtime::Runtime,
    client: reqwest::Client,
    credentials: Credentials,
}

impl LiveApi {
    fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let credentials = {
            let _entered = runtime.enter();
            Builder::default().build().unwrap()
        };
        Self {
            runtime,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            credentials,
        }
    }

    fn send(&self, method: &str, path: &str, body: Option<Value>) -> Result<(u16, Value), String> {
        let headers = self
            .runtime
            .block_on(self.credentials.headers(http::Extensions::new()))
            .map_err(|_| "could not obtain ADC headers")?;
        let CacheableResource::New { data: headers, .. } = headers else {
            return Err("ADC returned no headers".into());
        };
        self.forward(
            method,
            path,
            headers,
            body.map(|b| serde_json::to_vec(&b).unwrap())
                .unwrap_or_default(),
        )
    }

    fn forward(
        &self,
        method: &str,
        path: &str,
        headers: http::HeaderMap,
        body: Vec<u8>,
    ) -> Result<(u16, Value), String> {
        assert!(path.starts_with(&format!("/bigquery/v2/projects/{}/", project())));
        self.runtime.block_on(async {
            let response = self
                .client
                .request(
                    method.parse().unwrap(),
                    format!("https://bigquery.googleapis.com{path}"),
                )
                .headers(headers)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let status = response.status().as_u16();
            let bytes = response.bytes().await.map_err(|e| e.to_string())?;
            let body = if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?
            };
            Ok((status, body))
        })
    }
}

struct Fixture {
    api: Arc<LiveApi>,
    dataset_path: String,
    jobs: Arc<Mutex<Vec<Value>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mut failures = Vec::new();
        let mut check =
            |method, path: &str, expected: &[u16]| match self.api.send(method, path, None) {
                Ok((status, _)) if expected.contains(&status) => {}
                Ok((status, _)) => failures.push(format!("{method} {path} returned HTTP {status}")),
                Err(error) => failures.push(format!("{method} {path}: {error}")),
            };
        for reference in self.jobs.lock().unwrap().iter() {
            let path = job_path(reference).replace("?location=", "/cancel?location=");
            check("POST", &path, &[200]);
        }
        check(
            "DELETE",
            &format!("{}?deleteContents=true", self.dataset_path),
            &[204, 404],
        );
        check("GET", &self.dataset_path, &[404]);
        if !failures.is_empty() {
            report_failed_cleanup(
                &format!("clean up {} and its jobs", self.dataset_path),
                &failures.join("; "),
            );
        }
    }
}

struct Proxy {
    endpoint: String,
    accepted: mpsc::Receiver<Value>,
    stop: Option<mpsc::Sender<()>>,
    server: Option<thread::JoinHandle<Vec<Record>>>,
}

impl Proxy {
    fn new(api: Arc<LiveApi>, jobs: Arc<Mutex<Vec<Value>>>, loss: Loss) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = mpsc::channel();
        let (accepted, accepted_rx) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let released = Arc::new(Mutex::new(released));
        let server = thread::spawn(move || {
            let mut workers = Vec::new();
            let mut inserts = 0;
            loop {
                if stopped.try_recv().is_ok() {
                    break;
                }
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => panic!("proxy accept failed: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                let Some((method, path, headers, body)) = read_request(&mut stream) else {
                    continue;
                };
                let insert = method == "POST" && path.split('?').next().unwrap().ends_with("/jobs");
                if insert {
                    inserts += 1;
                }
                let first = insert && inserts == 1;
                if path.contains("/cancel") {
                    let _ = release.send(());
                }
                let api = api.clone();
                let jobs = jobs.clone();
                let accepted = accepted.clone();
                let released = released.clone();
                workers.push(thread::spawn(move || {
                    let (status, response) = api.forward(&method, &path, headers, body.clone()).unwrap();
                    if first && status == 200 {
                        let reference = response.get("jobReference").unwrap().clone();
                        jobs.lock().unwrap().push(reference.clone());
                        accepted.send(reference).unwrap();
                        if matches!(loss, Loss::Cancel) {
                            released.lock().unwrap().recv_timeout(WAIT).unwrap();
                        }
                    }
                    let dropped = insert && (first || !matches!(loss, Loss::First));
                    if !dropped {
                        let bytes = serde_json::to_vec(&response).unwrap();
                        let _ = write!(stream, "HTTP/1.1 {status} \r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", bytes.len())
                            .and_then(|()| stream.write_all(&bytes));
                    }
                    Record { method, path, request: body, status, response, dropped }
                }));
            }
            // Join every worker even after a panic: each may still register a job for cleanup.
            let results: Vec<_> = workers.into_iter().map(thread::JoinHandle::join).collect();
            results.into_iter().map(Result::unwrap).collect()
        });
        Self {
            endpoint,
            accepted: accepted_rx,
            stop: Some(stop),
            server: Some(server),
        }
    }

    fn connection(&self) -> BigQueryConnection {
        open_database([
            (BigQueryDatabaseOption::ProjectId.into(), project().into()),
            (
                BigQueryDatabaseOption::Endpoint.into(),
                self.endpoint.clone().into(),
            ),
        ])
        .new_connection()
        .unwrap()
    }

    fn finish(mut self) -> Vec<Record> {
        self.stop.take().unwrap().send(()).unwrap();
        self.server.take().unwrap().join().unwrap()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

/// Credentials are forwarded only to the fixed BigQuery origin and never retained in records.
fn read_request(stream: &mut TcpStream) -> Option<(String, String, http::HeaderMap, Vec<u8>)> {
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut reader = std::io::BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap() == 0 {
        return None;
    }
    let mut words = line.split_ascii_whitespace();
    let method = words.next().unwrap().to_owned();
    let path = words.next().unwrap().to_owned();
    let mut headers = http::HeaderMap::new();
    let mut length = 0;
    loop {
        line.clear();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        let (key, value) = line.split_once(':').unwrap();
        if key.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse::<usize>().unwrap();
        }
        assert!(!key.eq_ignore_ascii_case("transfer-encoding"));
        if !["host", "connection", "content-length"]
            .iter()
            .any(|h| key.eq_ignore_ascii_case(h))
        {
            headers.insert(
                http::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value.trim()).unwrap(),
            );
        }
    }
    assert!(length <= 1_048_576, "unexpectedly large test request");
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    Some((method, path, headers, body))
}
