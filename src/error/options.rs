//! Diagnostics for an option key the driver will not take: where the key really belongs, what to
//! use instead, and the nearest spelling it does know.

use adbc_core::error::Error;

use crate::options::{OptionKind, StringEnum};
use crate::{BigQueryConnectionOption, BigQueryDatabaseOption, BigQueryStatementOption};

use super::{not_found, not_implemented};

/// A setter's refusal of `key`: NOT_IMPLEMENTED, as adbc.h asks of an unrecognized option.
pub(crate) fn option_set_refusal(kind: &str, key: &str) -> Error {
    not_implemented(option_rejection(kind, key, false))
}

/// A getter's refusal of `key`: NOT_FOUND, the only failure adbc.h documents for a getter.
pub(crate) fn option_get_refusal(kind: &str, key: &str) -> Error {
    not_found(option_rejection(kind, key, true))
}

/// `unsupported` for a key the driver recognizes and refuses at this scope, `unknown` for one it
/// has never heard of, then what to try next. `getter` is whether the key was being read.
fn option_rejection(kind: &str, key: &str, getter: bool) -> String {
    let (recognized, orientation) = option_orientation(kind, key, getter);
    let verdict = if recognized { "unsupported" } else { "unknown" };
    format!("{verdict} BigQuery {kind} option {key:?}{orientation}")
}

/// What to try next after a refused option key, and whether the driver recognizes it. In order:
/// a statement read of the Go driver's project or dataset key (G57); the scope the key really
/// belongs to; why a knowingly refused key is refused; the nearest known key, when close enough
/// to be a typo; otherwise how the driver's keys are spelled.
///
/// Only the key is echoed, never the value: a credential sent under the wrong key lands here. See
/// [`crate::options`].
fn option_orientation(kind: &str, key: &str, getter: bool) -> (bool, String) {
    // The Go driver reads these two on a statement too (G57); this driver's statement spelling of
    // the same value is its effective default.
    if getter && kind == "statement" {
        let default = match BigQueryDatabaseOption::from_name(key) {
            Some(BigQueryDatabaseOption::ProjectId) => {
                Some((BigQueryStatementOption::DefaultProjectId, "project"))
            }
            Some(BigQueryDatabaseOption::DatasetId) => {
                Some((BigQueryStatementOption::DefaultDatasetId, "dataset"))
            }
            _ => None,
        };
        if let Some((default, what)) = default {
            return (
                true,
                format!(
                    "; read {}, the statement's effective default {what}, or read this key \
                     from the connection",
                    default.as_str()
                ),
            );
        }
    }
    let access = if getter { "read it from" } else { "set it on" };
    // A real key at the wrong scope is not a typo, so check scope first. This answers
    // `bigquery.query.priority` on a database and `bigquery.auth_type` on a connection (the Go
    // driver takes both per connection) with where they go.
    match other_option_scopes(kind, key).as_slice() {
        [] => {}
        [scope] => {
            return (
                true,
                format!("; that is a BigQuery {scope} option, so {access} the {scope}"),
            );
        }
        scopes => {
            return (
                true,
                format!(
                    "; that is a BigQuery {} option, so {access} one of those",
                    scopes.join(" or ")
                ),
            );
        }
    }
    if let Some(refusal) =
        refused_standard_option(key).or_else(|| refused_go_option(key).map(str::to_owned))
    {
        return (true, refusal);
    }
    if let Some(&(_, scope)) = ADBC_OPTION_SCOPES
        .iter()
        .find(|&&(standard, scope)| standard == key && scope != kind)
    {
        return (
            true,
            format!("; that is an ADBC {scope} option, so {access} the {scope}"),
        );
    }
    let orientation = match nearest_option_key(kind, key) {
        Some((scope, nearest)) if scope == kind => format!("; did you mean {nearest:?}?"),
        Some((scope, nearest)) => format!("; did you mean the {scope} option {nearest:?}?"),
        // Telling a caller who wrote `bigquery.*` that the driver's keys are spelled that way
        // would read as "you spelled it right".
        None if has_driver_prefix(key) => format!("; no BigQuery {kind} option has that name"),
        None => format!(
            "; this driver takes the ADBC standard {kind} options plus its own, which are \
             spelled bigquery.* or adbc.bigquery.*"
        ),
    };
    (false, orientation)
}

/// The ADBC keys this driver takes, each with the one object that takes it, matched exactly as
/// adbc_core parses them. The read-only progress keys have [`refused_standard_option`] instead.
const ADBC_OPTION_SCOPES: [(&str, &str); 13] = {
    use adbc_core::constants::{
        ADBC_CONNECTION_OPTION_AUTOCOMMIT, ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
        ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA, ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
        ADBC_CONNECTION_OPTION_READ_ONLY, ADBC_INGEST_OPTION_MODE,
        ADBC_INGEST_OPTION_TARGET_CATALOG, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
        ADBC_INGEST_OPTION_TARGET_TABLE, ADBC_INGEST_OPTION_TEMPORARY, ADBC_OPTION_URI,
        ADBC_STATEMENT_OPTION_INCREMENTAL,
    };
    [
        (ADBC_OPTION_URI, "database"),
        (ADBC_CONNECTION_OPTION_AUTOCOMMIT, "connection"),
        (ADBC_CONNECTION_OPTION_READ_ONLY, "connection"),
        (ADBC_CONNECTION_OPTION_ISOLATION_LEVEL, "connection"),
        (ADBC_CONNECTION_OPTION_CURRENT_CATALOG, "connection"),
        (ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA, "connection"),
        (ADBC_INGEST_OPTION_TARGET_TABLE, "statement"),
        (ADBC_INGEST_OPTION_TARGET_CATALOG, "statement"),
        (ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, "statement"),
        (ADBC_INGEST_OPTION_TEMPORARY, "statement"),
        (ADBC_INGEST_OPTION_MODE, "statement"),
        (ADBC_STATEMENT_OPTION_INCREMENTAL, "statement"),
        (crate::options::keys::OPTION_BIND_BY_NAME, "statement"),
    ]
};

/// Whether `key` is spelled like one of this driver's own keys, whatever its case.
fn has_driver_prefix(key: &str) -> bool {
    ["bigquery.", "adbc.bigquery."].iter().any(|prefix| {
        key.get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    })
}

/// Guidance for the four ADBC standard option keys this driver refuses, which the fallback would
/// describe as accepted. Tools migrating from other drivers send username/password, and callers
/// polling progress try to set the read-only progress keys.
fn refused_standard_option(key: &str) -> Option<String> {
    use adbc_core::constants;

    match key {
        constants::ADBC_OPTION_USERNAME | constants::ADBC_OPTION_PASSWORD => Some(format!(
            "; BigQuery has no username/password authentication; set {} and the credential \
             option it names, such as {}",
            BigQueryDatabaseOption::AuthenticationType.as_str(),
            BigQueryDatabaseOption::AuthenticationCredentials.as_str(),
        )),
        constants::ADBC_STATEMENT_OPTION_PROGRESS
        | constants::ADBC_STATEMENT_OPTION_MAX_PROGRESS => Some(format!(
            "; {} and {} are read-only; read them with AdbcStatementGetOptionDouble while an \
             execute runs",
            constants::ADBC_STATEMENT_OPTION_PROGRESS,
            constants::ADBC_STATEMENT_OPTION_MAX_PROGRESS,
        )),
        _ => None,
    }
}

/// Why this driver refuses one of the Go driver's options, under its canonical or legacy spelling.
///
/// The fallback would claim no such option exists. `GO-DRIVER-COMPARISON.md` G15, G28, G29 and
/// G87 record the reasons.
fn refused_go_option(key: &str) -> Option<&'static str> {
    let name = ["bigquery.", "adbc.bigquery.sql."]
        .iter()
        .find_map(|prefix| {
            key.get(..prefix.len())
                .filter(|head| head.eq_ignore_ascii_case(prefix))
                .map(|_| &key[prefix.len()..])
        })?;
    let reason = match name.to_ascii_lowercase().as_str() {
        "query.use_legacy_sql" | "query.disable_flattened_results" => {
            "; that is a legacy SQL option, and this driver runs GoogleSQL only"
        }
        "query.allow_large_results" => {
            "; that is a legacy SQL option, and this driver runs GoogleSQL only, which needs no \
             flag for large results"
        }
        "query.result_buffer_size" => {
            "; the driver buffers results within a fixed memory budget; use \
             bigquery.query.prefetch_concurrency to bound REST conversions in flight"
        }
        "bulk_ingest.compression" => "; Storage Write appends are always sent uncompressed",
        "query.job_creation_mode" => {
            "; ordinary queries allow optional job creation; partitioned execution requires a job"
        }
        "query.results_format" | "query.arrow_serialization_options.buffer_compression" => {
            "; inline Arrow query results are not implemented; results arrive as REST rows or \
             an LZ4-compressed Storage Read stream"
        }
        _ => return None,
    };
    Some(reason)
}

/// The scopes other than `kind` at which `key` is a real option, in database-connection-statement
/// order.
///
/// Legacy aliases count: sending one to the wrong object is the same mistake.
fn other_option_scopes(kind: &str, key: &str) -> Vec<&'static str> {
    [
        ("database", BigQueryDatabaseOption::from_name(key).is_some()),
        (
            "connection",
            BigQueryConnectionOption::from_name(key).is_some(),
        ),
        (
            "statement",
            BigQueryStatementOption::from_name(key).is_some(),
        ),
    ]
    .into_iter()
    .filter(|&(scope, known)| known && scope != kind)
    .map(|(scope, _)| scope)
    .collect()
}

/// The key closest to `key` across all three scopes, with its scope, when one is close enough
/// to be a plausible typo: a mistyped key is as likely to be meant for another object as for the
/// one that rejected it. On a tie the rejecting scope wins.
fn nearest_option_key(kind: &str, key: &str) -> Option<(&'static str, &'static str)> {
    [
        ("database", nearest_key::<BigQueryDatabaseOption>(key)),
        ("connection", nearest_key::<BigQueryConnectionOption>(key)),
        ("statement", nearest_key::<BigQueryStatementOption>(key)),
    ]
    .into_iter()
    .filter_map(|(scope, nearest)| nearest.map(|(distance, key)| (distance, scope, key)))
    .min_by_key(|&(distance, scope, _)| (distance, scope != kind))
    .map(|(_, scope, key)| (scope, key))
}

/// The canonical key of `T` closest to `key`, and its distance, when close enough to suggest.
/// Legacy aliases are never suggested.
fn nearest_key<T: StringEnum>(key: &str) -> Option<(usize, &'static str)> {
    // At most a quarter of the key's length in edits, capped at four.
    let budget = (key.len() / 4).min(4);
    T::ALL
        .iter()
        .copied()
        .map(|option| (edit_distance(key, option.as_str()), option.as_str()))
        .filter(|&(distance, _)| distance <= budget)
        .min_by_key(|&(distance, _)| distance)
}

/// Levenshtein distance, for deciding whether an unknown option key is a plausible typo.
///
/// Two rows of the matrix are never needed at once: the cell above-left is carried in `diagonal`
/// and the cell to the left in `previous`, so one row is updated in place.
fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    if right.is_empty() {
        return left.chars().count();
    }
    let mut row: Vec<usize> = (0..=right.len()).collect();
    for (index, left_char) in left.chars().enumerate() {
        // Column zero of the row above, and of the row being written.
        let mut diagonal = index;
        let mut previous = index + 1;
        for (cell, &right_char) in row.iter_mut().skip(1).zip(right.iter()) {
            let candidate = (diagonal + usize::from(left_char != right_char))
                .min(previous + 1)
                .min(*cell + 1);
            diagonal = *cell;
            *cell = candidate;
            previous = candidate;
        }
    }
    row.last().copied().unwrap_or(0)
}

/// Report an option the caller read but never set, by its key: once a driver manager has lost the
/// call site, the key is all a caller has. adbc.h documents `ADBC_STATUS_NOT_FOUND` for a value a
/// getter cannot supply.
pub(crate) fn option_not_set(key: &str) -> Error {
    not_found(format!("BigQuery option {key} is not set"))
}

/// `requested` is the getter's spelling of its own type: "a string", "bytes", "an integer",
/// or "a double".
#[cold]
#[inline(never)]
pub(crate) fn wrong_option_type(key: &str, actual: OptionKind, requested: &str) -> Error {
    not_found(format!(
        "BigQuery option {key} has type {}, but was requested as {requested}",
        actual.as_str()
    ))
}

/// The fallback for a typed option accessor: a known option accessed through the wrong type
/// reports its actual type and the type it was requested as, anything else is an unknown option.
#[cold]
#[inline(never)]
pub(crate) fn unknown_or_wrong_type(
    scope: &str,
    key: &str,
    kind: Option<OptionKind>,
    requested: &str,
) -> Error {
    match kind {
        Some(actual) => wrong_option_type(key, actual, requested),
        None => option_get_refusal(scope, key),
    }
}

#[cfg(test)]
mod tests;
