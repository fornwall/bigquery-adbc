//! A configuration value that must never be rendered.
//!
//! A secret never appears in `Debug` output, so every structure holding secrets can derive
//! `Debug`. It does not zeroize: `fs::read_to_string`, `serde_json`, and the `google-cloud-auth`
//! builders have already copied the value, so wiping one buffer would promise a protection the
//! driver does not provide.

use std::sync::Arc;

/// A secret configuration value.
///
/// `Debug` prints `<redacted>`, and there is no `Display`, `Serialize`, or `AsRef<str>`: the value
/// is reachable only by dereferencing to `str`. The `Arc` keeps cloning a `DatabaseConfig` from
/// copying every secret.
///
/// `PartialEq` exists for test assertions; it is not constant time, so nothing may use it to check
/// a presented credential.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Secret(Arc<str>);

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl std::ops::Deref for Secret {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}
