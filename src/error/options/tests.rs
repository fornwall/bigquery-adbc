use super::*;
use adbc_core::error::Status;

/// adbc.h's status for each direction, whatever the wording.
#[test]
fn a_setter_refuses_with_not_implemented_and_a_getter_with_not_found() {
    for key in [
        "bigquery.auth_typ",
        "bigquery.query.priority",
        "bigquery.query.unordered_read",
        "bigquery.query.results_format",
        "uri",
        "username",
        "whatever",
    ] {
        assert_eq!(
            option_set_refusal("database", key).status,
            Status::NotImplemented,
            "{key}"
        );
        assert_eq!(
            option_get_refusal("database", key).status,
            Status::NotFound,
            "{key}"
        );
    }
}

/// A typo'd option key is the common case, and a bare rejection leaves the caller with no next
/// step. A typo is as likely to be meant for another object, which the suggestion then names.
/// Nothing close enough to be a typo gets a suggestion: a key already spelled like the driver's
/// own is told that no option has that name, and any other is told how they are spelled, even on
/// a scope with no key list of its own.
#[test]
fn an_unknown_option_gets_the_key_it_was_probably_meant_to_be_or_orientation() {
    let spelled = |kind: &str| {
        format!(
            "; this driver takes the ADBC standard {kind} options plus its own, which are \
             spelled bigquery.* or adbc.bigquery.*"
        )
    };
    for (kind, key, guidance) in [
        (
            "database",
            "bigquery.auth_typ",
            "; did you mean \"bigquery.auth_type\"?".to_owned(),
        ),
        (
            "statement",
            "bigquery.query.prioritty",
            "; did you mean \"bigquery.query.priority\"?".to_owned(),
        ),
        (
            "database",
            "bigquery.query.timeout",
            "; did you mean the statement option \"bigquery.query.job_timeout\"?".to_owned(),
        ),
        (
            "connection",
            "adbc.bigquery.nope",
            "; no BigQuery connection option has that name".to_owned(),
        ),
        ("database", "query.prioirty", spelled("database")),
        ("driver", "whatever", spelled("driver")),
    ] {
        let expected = format!("unknown BigQuery {kind} option {key:?}{guidance}");
        assert_eq!(option_set_refusal(kind, key).message, expected);
        assert_eq!(option_get_refusal(kind, key).message, expected);
    }
}

/// A real key on the wrong object is not a typo; the error names the scope it belongs to instead
/// of calling it unrecognized. The Go driver takes the authentication keys per connection and this
/// driver on the database, and a legacy alias counts because the caller's mistake is the object.
#[test]
fn an_option_known_at_another_scope_is_answered_with_the_scope_it_belongs_to() {
    for (kind, key, scope) in [
        ("database", "bigquery.query.priority", "statement"),
        ("connection", "bigquery.auth_type", "database"),
        ("connection", "adbc.bigquery.sql.auth_type", "database"),
    ] {
        let unsupported = format!("unsupported BigQuery {kind} option {key:?}");
        assert_eq!(
            option_set_refusal(kind, key).message,
            format!("{unsupported}; that is a BigQuery {scope} option, so set it on the {scope}")
        );
        assert_eq!(
            option_get_refusal(kind, key).message,
            format!(
                "{unsupported}; that is a BigQuery {scope} option, so read it from the {scope}"
            )
        );
    }
}

/// A key real at two scopes names both rather than picking one.
#[test]
fn an_option_known_at_two_other_scopes_names_both() {
    assert_eq!(
        option_get_refusal("database", "bigquery.query.unordered_read").message,
        "unsupported BigQuery database option \"bigquery.query.unordered_read\"; that is a BigQuery \
         connection or statement option, so read it from one of those"
    );
}

/// Go reads the project and dataset on a statement, so a statement read names this driver's
/// statement spelling. Setting one there is an ordinary mistake of scope.
#[test]
fn a_statement_read_of_gos_project_or_dataset_names_the_statement_default() {
    for (key, option, what) in [
        (
            "adbc.bigquery.sql.project_id",
            "default_project_id",
            "project",
        ),
        ("bigquery.dataset_id", "default_dataset_id", "dataset"),
    ] {
        assert_eq!(
            option_get_refusal("statement", key).message,
            format!(
                "unsupported BigQuery statement option {key:?}; read bigquery.query.{option}, the \
                 statement's effective default {what}, or read this key from the connection"
            )
        );
        assert!(
            option_set_refusal("statement", key)
                .message
                .ends_with("so set it on the database")
        );
    }
}

/// The ADBC standard keys are real keys too: one sent to the wrong object is answered with the
/// object that takes it, not called unknown.
#[test]
fn a_standard_option_at_another_scope_is_answered_with_the_scope_it_belongs_to() {
    for (kind, key, scope) in [
        ("database", "adbc.connection.catalog", "connection"),
        ("statement", "adbc.connection.catalog", "connection"),
        ("database", "adbc.ingest.mode", "statement"),
        ("connection", "adbc.ingest.mode", "statement"),
        ("connection", "uri", "database"),
        ("statement", "adbc.connection.autocommit", "connection"),
        (
            "connection",
            crate::options::keys::OPTION_BIND_BY_NAME,
            "statement",
        ),
    ] {
        let unsupported = format!("unsupported BigQuery {kind} option {key:?}");
        assert_eq!(
            option_set_refusal(kind, key).message,
            format!("{unsupported}; that is an ADBC {scope} option, so set it on the {scope}")
        );
        assert_eq!(
            option_get_refusal(kind, key).message,
            format!("{unsupported}; that is an ADBC {scope} option, so read it from the {scope}")
        );
    }
}

/// Keys this driver knowingly refuses say why, rather than how keys are spelled: the ADBC standard
/// keys, which the fallback would claim the driver takes, and the Go driver's options, spelled
/// like its own under either prefix and in any case. A key merely sharing a refused key's prefix
/// is unknown.
#[test]
fn a_knowingly_refused_option_says_why() {
    let legacy = "; that is a legacy SQL option, and this driver runs GoogleSQL only";
    let credentials = "; BigQuery has no username/password authentication; set bigquery.auth_type \
                       and the credential option it names, such as bigquery.auth.credentials";
    let progress = "; adbc.statement.exec.progress and adbc.statement.exec.max_progress are \
                    read-only; read them with AdbcStatementGetOptionDouble while an execute runs";
    let job_mode =
        "; ordinary queries allow optional job creation; partitioned execution requires a job";
    let arrow = "; inline Arrow query results are not implemented; results arrive as REST rows or \
                 an LZ4-compressed Storage Read stream";
    for (kind, key, reason) in [
        ("database", "username", credentials),
        ("database", "password", credentials),
        ("statement", "adbc.statement.exec.progress", progress),
        ("statement", "adbc.statement.exec.max_progress", progress),
        ("statement", "bigquery.query.use_legacy_sql", legacy),
        (
            "statement",
            "adbc.bigquery.sql.query.use_legacy_sql",
            legacy,
        ),
        (
            "statement",
            "BigQuery.Query.Disable_Flattened_Results",
            legacy,
        ),
        (
            "statement",
            "bigquery.query.allow_large_results",
            "; that is a legacy SQL option, and this driver runs GoogleSQL only, which needs no \
             flag for large results",
        ),
        (
            "connection",
            "adbc.bigquery.sql.query.result_buffer_size",
            "; the driver buffers results within a fixed memory budget; use \
             bigquery.query.prefetch_concurrency to bound REST conversions in flight",
        ),
        (
            "database",
            "bigquery.bulk_ingest.compression",
            "; Storage Write appends are always sent uncompressed",
        ),
        ("connection", "bigquery.query.job_creation_mode", job_mode),
        ("connection", "bigquery.query.results_format", arrow),
        (
            "connection",
            "bigquery.query.arrow_serialization_options.buffer_compression",
            arrow,
        ),
    ] {
        assert_eq!(
            option_set_refusal(kind, key).message,
            format!("unsupported BigQuery {kind} option {key:?}{reason}")
        );
    }
    assert!(
        option_set_refusal("statement", "bigquery.query.use_legacy")
            .message
            .starts_with("unknown BigQuery statement option")
    );
}

#[test]
fn edit_distance_counts_insertions_deletions_and_substitutions() {
    assert_eq!(edit_distance("", ""), 0);
    assert_eq!(edit_distance("abc", ""), 3);
    assert_eq!(edit_distance("", "abc"), 3);
    assert_eq!(edit_distance("abc", "abc"), 0);
    assert_eq!(edit_distance("kitten", "sitting"), 3);
    assert_eq!(edit_distance("flaw", "lawn"), 2);
}
