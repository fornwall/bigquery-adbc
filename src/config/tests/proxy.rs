//! The token exchange follows the process's proxy environment.

use super::*;

const PROXY_TEST_CHILD: &str = "BIGQUERY_ADBC_PROXY_TEST_CHILD";
const PROXY_TEST_ENDPOINT: &str = "BIGQUERY_ADBC_PROXY_TEST_ENDPOINT";

/// Deadlock guard for accepting a proxy connection and capturing its first bytes.
/// Allow time for the child test process, Tokio runtime, and TLS handshake to start under load.
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// Captures the first connection to a local HTTP endpoint without requiring a test-only HTTP
/// dependency. For an HTTPS proxy this is the plaintext `CONNECT`; for a direct HTTPS endpoint it
/// is the TLS ClientHello. Closing the socket is enough: the route, not token success, is under
/// test.
///
/// `None` means nothing connected within [`PROXY_CONNECT_TIMEOUT`]; callers report that apart
/// from an empty capture (a connection that sent nothing), since only the first is about the
/// route.
fn capture_connection(listener: std::net::TcpListener) -> std::thread::JoinHandle<Option<Vec<u8>>> {
    use std::io::{Read, Write};

    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + PROXY_CONNECT_TIMEOUT;
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    // On Windows the stream inherits the listener's non-blocking mode.
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(PROXY_CONNECT_TIMEOUT))
                        .unwrap();
                    let mut bytes = vec![0; 4_096];
                    let count = stream.read(&mut bytes).unwrap_or(0);
                    bytes.truncate(count);
                    let _ =
                        stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
                    return Some(bytes);
                }
                Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(source) => panic!("failed to accept proxy test connection: {source}"),
            }
        }
        None
    })
}

/// Runs the credential request in a child process so changing the process-wide proxy environment
/// cannot race any other unit test. The parent observes which listener the request actually used.
fn run_proxy_test_child(endpoint: &str, proxy: &str, no_proxy: Option<&str>) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "config::tests::proxy::user_oauth_token_exchange_honors_https_proxy_and_no_proxy",
        ])
        .env(PROXY_TEST_CHILD, "1")
        .env(PROXY_TEST_ENDPOINT, endpoint);
    // Remove before setting: Windows variable names ignore case, so removing `https_proxy` would
    // also remove an `HTTPS_PROXY` set earlier on this command.
    for key in [
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ] {
        command.env_remove(key);
    }
    command.env("HTTPS_PROXY", proxy);
    if let Some(no_proxy) = no_proxy {
        command.env("NO_PROXY", no_proxy);
    }
    assert!(command.status().unwrap().success());
}

/// The refresh-token exchange must use HTTPS_PROXY, while NO_PROXY sends a matching endpoint
/// directly. reqwest's `system-proxy` feature supplies both behaviors to google-cloud-auth's
/// `Client::new()` token request.
///
/// The token endpoints are not Google hosts, so the child sets
/// `bigquery.allow_untrusted_endpoint`, as any caller using a test server must.
#[test]
fn user_oauth_token_exchange_honors_https_proxy_and_no_proxy() {
    if std::env::var_os(PROXY_TEST_CHILD).is_some() {
        let runtime = crate::runtime::new_runtime().unwrap();
        let _guard = runtime.enter();
        let settings = settings(|config| {
            config.authentication_type = BigQueryAuthenticationType::UserAuthentication;
            config.user_authentication.client_id = Some(Arc::from("test-client-id"));
            config.user_authentication.client_secret = Some(Secret::from("test-client-secret"));
            config.user_authentication.refresh_token = Some(Secret::from("test-refresh-token"));
            config.user_authentication.access_token_endpoint =
                Some(Arc::from(std::env::var(PROXY_TEST_ENDPOINT).unwrap()));
            config.allow_untrusted_endpoint = true;
        });
        let configuration = ClientConfiguration::build(&settings).unwrap();
        // The fake proxy/endpoint cannot complete TLS. Bound the credential retry
        // loop; the parent needs only the first connection to prove which route was selected.
        let _ = runtime.block_on(tokio::time::timeout(
            Duration::from_secs(2),
            configuration.credentials.headers(http::Extensions::new()),
        ));
        return;
    }

    let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let capture = capture_connection(proxy);
    run_proxy_test_child(
        "https://token-exchange.invalid/token",
        &format!("http://{proxy_address}"),
        None,
    );
    let request = capture
        .join()
        .unwrap()
        .expect("nothing connected to the HTTPS_PROXY listener before its deadline");
    assert!(
        request.starts_with(b"CONNECT token-exchange.invalid:443 HTTP/1.1\r\n"),
        "unexpected proxy request: {}",
        String::from_utf8_lossy(&request)
    );

    let endpoint = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint_address = endpoint.local_addr().unwrap();
    let direct_capture = capture_connection(endpoint);
    let bypassed_proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    bypassed_proxy.set_nonblocking(true).unwrap();
    let bypassed_proxy_address = bypassed_proxy.local_addr().unwrap();
    run_proxy_test_child(
        &format!("https://{endpoint_address}/token"),
        &format!("http://{bypassed_proxy_address}"),
        Some("127.0.0.1"),
    );
    let direct = direct_capture
        .join()
        .unwrap()
        .expect("nothing connected to the NO_PROXY token endpoint before its deadline");
    assert_eq!(direct.first(), Some(&0x16), "expected a TLS ClientHello");
    assert!(
        matches!(
            bypassed_proxy.accept(),
            Err(source) if source.kind() == std::io::ErrorKind::WouldBlock
        ),
        "NO_PROXY token exchange unexpectedly reached HTTPS_PROXY"
    );
}
