//! Shared runtime configuration for live integration tests.

use std::sync::OnceLock;

pub(crate) fn project() -> &'static str {
    static PROJECT: OnceLock<String> = OnceLock::new();
    PROJECT.get_or_init(|| {
        let value = std::env::var("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT").ok();
        validate_project(value.as_deref()).expect("invalid integration test configuration");
        value.unwrap()
    })
}

fn validate_project(value: Option<&str>) -> Result<&str, &'static str> {
    let value = value.filter(|value| !value.is_empty()).ok_or(
        "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set to a non-empty project ID",
    )?;
    if value.contains(char::is_whitespace) || value.contains(['`', '/', '?', '#']) {
        return Err("unsafe BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT value");
    }
    Ok(value)
}

#[test]
fn integration_project_requires_an_explicit_nonempty_value() {
    for value in [
        None,
        Some(""),
        Some(" "),
        Some("project/dataset"),
        Some("`project"),
    ] {
        assert!(validate_project(value).is_err());
    }
    for value in ["my-integration-project", "123456789", "example.com:project"] {
        assert_eq!(validate_project(Some(value)), Ok(value));
    }
}
