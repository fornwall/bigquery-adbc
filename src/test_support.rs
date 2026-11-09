//! Fixtures shared by tests across the crate.

pub(crate) mod arrow;
pub(crate) mod cancellation;
pub(crate) mod credentials;
pub(crate) mod driver;
pub(crate) mod errors;
pub(crate) mod grpc;
pub(crate) mod http;
pub(crate) mod identifiers;
pub(crate) mod options;
pub(crate) mod schema;

/// An endpoint that refuses every connection at once. No ephemeral bind is ever assigned port 1,
/// so unlike a freed ephemeral port it can never turn into another test's mock.
pub(crate) const UNREACHABLE_ENDPOINT: &str = "http://127.0.0.1:1";

/// Runs `future` on a current-thread runtime whose clock moves only by `tokio::time::advance`.
///
/// A pending blocking task inhibits tokio's auto-advance, which would otherwise jump the paused
/// clock whenever the runtime idles on mock socket I/O.
pub(crate) fn block_on_paused_clock<F: Future>(future: F) -> F::Output {
    crate::runtime::install_crypto_provider();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (hold, held) = std::sync::mpsc::channel::<()>();
        let inhibitor = tokio::task::spawn_blocking(move || held.recv());
        let output = future.await;
        drop(hold);
        let _ = inhibitor.await;
        output
    })
}
