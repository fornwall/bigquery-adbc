//! URL escaping for resource paths and individual path/query components.

use percent_encoding::{AsciiSet, CONTROLS, NON_ALPHANUMERIC, utf8_percent_encode};

/// Escape a resource path while preserving its segment separators.
const RESOURCE_ESCAPE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Escape a multi-segment resource name.
pub(crate) fn path_component(value: &str) -> impl std::fmt::Display + '_ {
    utf8_percent_encode(value, RESOURCE_ESCAPE)
}

/// Only RFC 3986 unreserved characters pass through unchanged.
const URL_COMPONENT_ESCAPE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Escape one path segment or query value, including any `/` it contains.
pub(crate) fn query_component(value: &str) -> impl std::fmt::Display + '_ {
    utf8_percent_encode(value, URL_COMPONENT_ESCAPE)
}

#[cfg(test)]
mod tests;
