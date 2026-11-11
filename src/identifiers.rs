//! BigQuery identifier validation and GoogleSQL quoting.

use std::sync::Arc;

use adbc_core::error::Result;

use crate::error::invalid_argument;

/// An opaque project identifier that remains one segment in a Google resource name.
///
/// BigQuery resolves project IDs, project numbers and legacy domain-scoped IDs. Only structural
/// checks belong here: Storage API resource names interpolate this value without URL encoding.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ProjectIdentifier(Arc<str>);

impl ProjectIdentifier {
    /// Validates a project identifier for a caller that has no use for the rejection reason;
    /// [`Self::try_new_from`] is the one that reports it.
    pub(crate) fn try_new(value: impl Into<Arc<str>>) -> Option<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && !value.contains(['/', '\\'])
            && !value.bytes().any(|byte| byte.is_ascii_control())
            && !is_dot_segment(&value);
        valid.then_some(Self(value))
    }

    /// Validates a project identifier, naming where a rejected value came from.
    ///
    /// The value is never echoed, so `source` (the option key, URI component, or call argument)
    /// identifies the input.
    pub(crate) fn try_new_from(source: &str, value: impl Into<Arc<str>>) -> Result<Self> {
        Self::try_new(value).ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery project identifier from {source} must be nonempty, cannot contain \
                 slashes, backslashes or ASCII control characters, and cannot be \".\" or \"..\""
            ))
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for ProjectIdentifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for ProjectIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Validates a BigQuery dataset ID, naming what the rejected value was supplied as.
///
/// The value itself is never echoed, so the subject is the only thing that identifies the input.
pub(crate) fn validate_dataset_id(subject: &str, dataset_id: &str) -> Result<()> {
    let length = dataset_id.chars().count();
    if length > 1024 {
        // The length is reported, though the value is not, so a caller knows how much to trim.
        return Err(invalid_argument(format!(
            "{subject} is {length} characters long; it cannot exceed 1,024 characters"
        )));
    }
    if !dataset_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(invalid_argument(format!(
            "{subject} may contain only ASCII letters, digits, and underscores"
        )));
    }
    Ok(())
}

/// Validates a BigQuery table ID against only what BigQuery cannot accept under any spelling,
/// naming what the rejected value was supplied as.
///
/// Not the dataset rule, nor BigQuery's documented table-name grammar, which excludes the `$` of
/// partition decorators (`model$20240101` is part of the table ID a copy job addresses). Only
/// length, `/`, control characters, and dot segments are checked, so no table name BigQuery
/// accepts is rejected. The length limit counts UTF-8 bytes, not characters (verified live:
/// 512 `é` are accepted, 513 are not).
///
/// The value itself is never echoed, so the subject is the only thing that identifies the input.
pub(crate) fn validate_table_id(subject: &str, table_id: &str) -> Result<()> {
    if table_id.is_empty() {
        return Err(invalid_argument(format!("{subject} cannot be empty")));
    }
    let length = table_id.len();
    if length > 1024 {
        return Err(invalid_argument(format!(
            "{subject} is {length} bytes long in UTF-8; it cannot exceed 1,024 bytes"
        )));
    }
    if table_id.contains('/') || table_id.chars().any(char::is_control) {
        return Err(invalid_argument(format!(
            "{subject} cannot contain slashes or control characters"
        )));
    }
    if is_dot_segment(table_id) {
        return Err(invalid_argument(format!(
            "{subject} cannot be \".\" or \"..\""
        )));
    }
    Ok(())
}

/// Validates the catalog or namespace of a BigLake `catalog.namespace` dataset ID, naming what the
/// rejected value was supplied as. The caller has already rejected an empty component.
///
/// BigQuery does not hold these to the dataset rule (verified live: it resolves `my-catalog.my-ns`
/// but refuses the dataset `my-dataset`), so only `/` and control characters, which would break
/// the dataset's resource-name segment, are checked.
///
/// The value itself is never echoed, so the subject is the only thing that identifies the input.
pub(crate) fn validate_catalog_component(subject: &str, component: &str) -> Result<()> {
    if component.contains('/') || component.chars().any(char::is_control) {
        return Err(invalid_argument(format!(
            "{subject} cannot contain slashes or control characters"
        )));
    }
    Ok(())
}

/// Whether `name` is `.` or `..`, which a URL parser resolves away rather than reading as a name.
///
/// A request for such a resource would address its parent instead, and percent-encoding does
/// not help: the URL standard resolves `%2E` and `%2E%2E` the same way.
pub(crate) fn is_dot_segment(name: &str) -> bool {
    matches!(name, "." | "..")
}

/// Writes one backtick-quoted identifier, joining table-path components with dots.
pub(crate) fn write_quoted_identifier(sql: &mut String, components: &[&str]) {
    sql.push('`');
    for (index, name) in components.iter().enumerate() {
        if index != 0 {
            sql.push('.');
        }
        for character in name.chars() {
            if character == '`' || character == '\\' {
                sql.push('\\');
            }
            sql.push(character);
        }
    }
    sql.push('`');
}

#[cfg(test)]
mod tests;
