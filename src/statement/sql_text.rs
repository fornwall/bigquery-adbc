//! SQL text validation and comment skipping.

use std::sync::Arc;

use adbc_core::error::Result;

use crate::error::invalid_state;

/// Requires a query containing more than whitespace and comments.
///
/// Option-only operations handle dbt's comment-only placeholders before calling this.
/// `operation` names the entry point in the error: execute, prepare or describe.
pub(super) fn require_sql<'a>(
    query: Option<&'a Arc<str>>,
    operation: &str,
) -> Result<&'a Arc<str>> {
    query.filter(|sql| !has_no_statement(sql)).ok_or_else(|| {
        invalid_state(format!(
            "cannot {operation} a BigQuery statement without a SQL query: the statement text is \
             unset, empty or only comments; set one with AdbcStatementSetSqlQuery"
        ))
    })
}

pub(super) fn normalize_sql(query: &str) -> &str {
    query.trim_end_matches(|character: char| character == ';' || character.is_whitespace())
}

/// Whether the query contains only whitespace and comments.
pub(crate) fn has_no_statement(query: &str) -> bool {
    skip_comments(query).is_empty()
}

/// Skips leading whitespace and comments, treating an unterminated block comment as empty.
pub(crate) fn skip_comments(query: &str) -> &str {
    let mut rest = query;
    loop {
        rest = rest.trim_start();
        if let Some(tail) = rest.strip_prefix("--").or_else(|| rest.strip_prefix('#')) {
            // GoogleSQL ends a line comment at a carriage return as well as at a newline; one
            // with neither left runs to the end of the query.
            rest = tail.split_once(['\r', '\n']).map_or("", |(_, tail)| tail);
        } else if let Some(tail) = rest.strip_prefix("/*") {
            let Some((_, tail)) = tail.split_once("*/") else {
                return "";
            };
            rest = tail;
        } else {
            return rest;
        }
    }
}
