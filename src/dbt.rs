//! Option-only `adbc.bigquery.*` operations for dbt-core 2.0.
//!
//! See `docs/DBT-CORE.md` for the supported operations and their option contracts.

pub(crate) mod csv_load;
pub(crate) mod csv_seed;
pub(crate) mod dataproc;
pub(crate) mod dataset_authorization;
pub(crate) mod dataset_location;
pub(crate) mod gcs;
mod jobs;
mod lro;
pub(crate) mod statement;
pub(crate) mod table_copy;
pub(crate) mod table_mutation;
pub(crate) mod vertex;

pub(crate) use jobs::FinishedJob;

use adbc_core::error::{Error, Result};

use crate::BigQueryStatementOption;
use crate::error::{invalid_argument, invalid_state};

/// Maps text options to fields in one place for both getters and setters.
macro_rules! text_slots {
    ($operation:ty { $($option:ident => $field:ident),+ $(,)? }) => {
        impl $operation {
            /// The field `option` names, or `None` when it names no text field of this operation.
            pub(crate) fn text_slot(
                &self,
                option: crate::BigQueryStatementOption,
            ) -> Option<&Option<String>> {
                match option {
                    $(crate::BigQueryStatementOption::$option => Some(&self.$field),)+
                    _ => None,
                }
            }

            pub(crate) fn text_slot_mut(
                &mut self,
                option: crate::BigQueryStatementOption,
            ) -> Option<&mut Option<String>> {
                match option {
                    $(crate::BigQueryStatementOption::$option => Some(&mut self.$field),)+
                    _ => None,
                }
            }

            /// A required option's value, or the [`crate::dbt::missing_option`] error naming it.
            pub(crate) fn required(
                &self,
                operation: &str,
                option: crate::BigQueryStatementOption,
            ) -> adbc_core::error::Result<&str> {
                self.text_slot(option)
                    .and_then(Option::as_deref)
                    .ok_or_else(|| crate::dbt::missing_option(operation, option))
            }

            /// Whether any text option is set, which selects this operation. A half-configured
            /// operation stays selected so it fails naming the missing option rather than
            /// falling through to the SQL path.
            #[allow(
                dead_code,
                reason = "the Dataproc options split activation between their two submissions, \
                          since the keys they share select neither"
            )]
            pub(crate) fn any_text_option_set(&self) -> bool {
                $(self.$field.is_some())||+
            }
        }
    };
}

pub(crate) use text_slots;

/// Reports the option a half-configured operation is missing.
///
/// `operation` ("Dataproc batch", "table copy") must be named because operations share option
/// keys: the key alone would tell a bigframes model to set a Dataproc option.
#[cold]
#[inline(never)]
fn missing_option(operation: &str, key: BigQueryStatementOption) -> Error {
    invalid_state(format!(
        "BigQuery option {} is required by the BigQuery {operation}",
        key.as_str()
    ))
}

/// Parses a JSON option payload. A syntax error reports its position, never the value.
fn parse_json_option(value: &str, key: BigQueryStatementOption) -> Result<serde_json::Value> {
    serde_json::from_slice(value.as_bytes()).map_err(|source| {
        invalid_argument(format!(
            "BigQuery option {} must be valid JSON: syntax error at line {} column {}",
            key.as_str(),
            source.line(),
            source.column()
        ))
    })
}

/// The non-empty string `name` of a dataset object in the JSON payload of option `key`.
fn required_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
    key: BigQueryStatementOption,
) -> Result<&'a str> {
    object
        .get(name)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            invalid_argument(format!(
                "every dataset in BigQuery option {} must carry a non-empty {name}",
                key.as_str()
            ))
        })
}

/// Rejects an option value that would change the shape of the URL it is interpolated into.
///
/// Dataproc and Vertex AI options reach the request path, where a query, fragment, escape, or
/// whitespace would address a different resource. `\` matters most: the WHATWG URL parser
/// `reqwest` uses ends a special scheme's host at a backslash, so one that reaches a host name
/// redirects the request. Empty, `.`, and `..` segments are refused because they reshape the path.
fn resource_path(value: &str, key: BigQueryStatementOption) -> Result<&str> {
    let reshapes = value.contains(['?', '#', '%', '\\'])
        || value.chars().any(char::is_whitespace)
        || value
            .split('/')
            .any(|segment| matches!(segment, "" | "." | ".."));
    if reshapes {
        return Err(invalid_argument(format!(
            "BigQuery option {} must be a Google Cloud resource path without `?`, `#`, `%`, \
             `\\`, whitespace, or empty, `.` or `..` segments",
            key.as_str()
        )));
    }
    Ok(value)
}

/// Like [`resource_path`], for a value that is one path segment rather than several.
fn resource_segment(value: &str, key: BigQueryStatementOption) -> Result<&str> {
    let value = resource_path(value, key)?;
    if value.contains('/') {
        return Err(invalid_argument(format!(
            "BigQuery option {} must be a single Google Cloud resource name",
            key.as_str()
        )));
    }
    Ok(value)
}

/// A compute region, interpolated into `https://{region}-{service}.googleapis.com`, a host that
/// receives a bearer token. Only ASCII letters, digits, and hyphens are allowed (an allow list,
/// not a list of characters a WHATWG host forbids); `ServiceEndpoints::dataproc` and `vertex`
/// then check the resulting host.
fn region_segment(value: &str, key: BigQueryStatementOption) -> Result<&str> {
    let named = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
    if !named {
        return Err(invalid_argument(format!(
            "BigQuery option {} must be a Google Cloud region name of ASCII letters, digits \
             and hyphens",
            key.as_str()
        )));
    }
    Ok(value)
}
