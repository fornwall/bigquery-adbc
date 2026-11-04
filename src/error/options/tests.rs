use super::*;
use crate::test_support::errors::assert_message;
use adbc_core::error::Status;

/// A typo'd option key is the common case, and a bare rejection leaves the caller with no
/// next step.
#[test]
fn an_unknown_option_points_at_the_key_it_was_probably_meant_to_be() {
    let failure = option_get_refusal("database", "bigquery.auth_typ");
    assert_eq!(failure.status, Status::NotFound);
    assert_eq!(
        failure.message,
        "unknown BigQuery database option \"bigquery.auth_typ\"; did you mean \
         \"bigquery.auth_type\"?"
    );

    let failure = option_set_refusal("statement", "bigquery.query.prioritty");
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "unknown BigQuery statement option \"bigquery.query.prioritty\"; did you mean \
         \"bigquery.query.priority\"?"
    );

    // A typo is as likely to be meant for another object, which the suggestion then names.
    let failure = option_set_refusal("database", "bigquery.query.timeout");
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "unknown BigQuery database option \"bigquery.query.timeout\"; did you mean the \
         statement option \"bigquery.query.job_timeout\"?"
    );
}

/// A real key set on the wrong object is not a typo; the error names the scope it belongs to
/// instead of calling it unrecognized.
#[test]
fn an_option_known_at_another_scope_is_answered_with_the_scope_it_belongs_to() {
    let failure = option_set_refusal("database", "bigquery.query.priority");
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "unsupported BigQuery database option \"bigquery.query.priority\"; that is a BigQuery \
         statement option, so set it on the statement"
    );

    // The Go driver takes the authentication keys per connection; this driver takes them on the
    // database, so the key that arrives at the wrong object is answered with the right one.
    let failure = option_set_refusal("connection", "bigquery.auth_type");
    assert_eq!(
        failure.message,
        "unsupported BigQuery connection option \"bigquery.auth_type\"; that is a BigQuery \
         database option, so set it on the database"
    );

    // A legacy alias counts: the caller's mistake is the object, not the spelling.
    let failure = option_get_refusal("connection", "adbc.bigquery.sql.auth_type");
    assert_eq!(
        failure.message,
        "unsupported BigQuery connection option \"adbc.bigquery.sql.auth_type\"; that is a BigQuery \
         database option, so read it from the database"
    );

    // Go reads the project and dataset on a statement; this driver names its statement spelling.
    let failure = option_get_refusal("statement", "adbc.bigquery.sql.project_id");
    assert_eq!(
        failure.message,
        "unsupported BigQuery statement option \"adbc.bigquery.sql.project_id\"; read \
         bigquery.query.default_project_id, the statement's effective default project, or read \
         this key from the connection"
    );
    assert!(
        option_get_refusal("statement", "bigquery.dataset_id")
            .message
            .contains("bigquery.query.default_dataset_id")
    );
    assert!(
        option_set_refusal("statement", "bigquery.dataset_id")
            .message
            .ends_with("so set it on the database")
    );

    // A key real at two scopes names both rather than picking one.
    let failure = option_get_refusal("database", "bigquery.query.unordered_read");
    assert_eq!(
        failure.status,
        Status::NotFound,
        "a getter's miss is NOT_FOUND whatever its wording"
    );
    assert_eq!(
        failure.message,
        "unsupported BigQuery database option \"bigquery.query.unordered_read\"; that is a BigQuery \
         connection or statement option, so read it from one of those"
    );
}

/// The ADBC standard keys are real keys too: one sent to the wrong object is answered with the
/// object that takes it, not called unknown.
#[test]
fn a_standard_option_at_another_scope_is_answered_with_the_scope_it_belongs_to() {
    use adbc_core::constants::{
        ADBC_CONNECTION_OPTION_AUTOCOMMIT, ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
        ADBC_INGEST_OPTION_MODE, ADBC_OPTION_URI,
    };

    for (kind, key, scope) in [
        (
            "database",
            ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            "connection",
        ),
        ("database", ADBC_INGEST_OPTION_MODE, "statement"),
        ("connection", ADBC_INGEST_OPTION_MODE, "statement"),
        ("connection", ADBC_OPTION_URI, "database"),
        ("statement", ADBC_CONNECTION_OPTION_AUTOCOMMIT, "connection"),
        (
            "connection",
            crate::options::keys::OPTION_BIND_BY_NAME,
            "statement",
        ),
    ] {
        let failure = option_set_refusal(kind, key);
        assert_eq!(failure.status, Status::NotImplemented, "{key}");
        assert_eq!(
            failure.message,
            format!(
                "unsupported BigQuery {kind} option {key:?}; that is an ADBC {scope} option, \
                 so set it on the {scope}"
            )
        );
    }
    assert!(
        option_get_refusal("statement", ADBC_CONNECTION_OPTION_CURRENT_CATALOG)
            .message
            .ends_with("so read it from the connection")
    );
}

/// Nothing close enough to be a typo gets a suggestion. A key already spelled like the
/// driver's own is told that no option has that name; any other is told how they are spelled.
#[test]
fn an_unrecognizable_option_key_gets_orientation_instead_of_a_guess() {
    let failure = option_get_refusal("connection", "adbc.bigquery.nope");
    assert_eq!(
        failure.message,
        "unknown BigQuery connection option \"adbc.bigquery.nope\"; no BigQuery connection \
         option has that name"
    );
    let failure = option_set_refusal("database", "query.prioirty");
    assert_eq!(
        failure.message,
        "unknown BigQuery database option \"query.prioirty\"; this driver takes the ADBC \
         standard database options plus its own, which are spelled bigquery.* or adbc.bigquery.*"
    );
    // A scope with no key list of its own still gets the orientation rather than nothing.
    assert!(
        option_get_refusal("driver", "whatever")
            .message
            .contains("bigquery.*")
    );
}

/// The ADBC standard keys the driver refuses get specific guidance, not the generic orientation
/// claiming the driver takes the standard options.
#[test]
fn a_knowingly_refused_standard_option_names_the_option_that_replaces_it() {
    let failure = option_set_refusal("database", "username");
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "unsupported BigQuery database option \"username\"; BigQuery has no username/password \
         authentication; set bigquery.auth_type and the credential option it names, such as \
         bigquery.auth.credentials"
    );

    let failure = option_set_refusal("database", "password");
    assert_eq!(
        failure.message,
        "unsupported BigQuery database option \"password\"; BigQuery has no username/password \
         authentication; set bigquery.auth_type and the credential option it names, such as \
         bigquery.auth.credentials"
    );

    for key in [
        "adbc.statement.exec.progress",
        "adbc.statement.exec.max_progress",
    ] {
        let failure = option_set_refusal("statement", key);
        assert_eq!(
            failure.message,
            format!(
                "unsupported BigQuery statement option {key:?}; adbc.statement.exec.progress and \
                 adbc.statement.exec.max_progress are read-only; read them with \
                 AdbcStatementGetOptionDouble while an execute runs"
            )
        );
    }
}

/// The Go driver's options this driver refuses are spelled like its own, so the error says why
/// rather than how keys are spelled.
#[test]
fn a_refused_go_driver_option_says_why_it_is_refused() {
    for key in [
        "bigquery.query.use_legacy_sql",
        "adbc.bigquery.sql.query.use_legacy_sql",
        "BigQuery.Query.Disable_Flattened_Results",
    ] {
        assert_eq!(
            option_set_refusal("statement", key).message,
            format!(
                "unsupported BigQuery statement option {key:?}; that is a legacy SQL option, and \
                 this driver runs GoogleSQL only"
            )
        );
    }
    assert!(
        option_set_refusal("statement", "bigquery.query.allow_large_results")
            .message
            .ends_with("which needs no flag for large results")
    );
    assert!(
        option_set_refusal("connection", "adbc.bigquery.sql.query.result_buffer_size")
            .message
            .contains("fixed memory budget")
    );
    assert!(
        option_set_refusal("database", "bigquery.bulk_ingest.compression")
            .message
            .contains("sent uncompressed")
    );
    for key in [
        "bigquery.query.job_creation_mode",
        "bigquery.query.results_format",
        "bigquery.query.arrow_serialization_options.buffer_compression",
    ] {
        let failure = option_set_refusal("connection", key);
        assert_eq!(failure.status, Status::NotImplemented);
        assert!(
            failure
                .message
                .starts_with("unsupported BigQuery connection option"),
            "{}",
            failure.message
        );
    }
    // A key merely sharing the prefix is unknown, not refused.
    assert!(
        option_set_refusal("statement", "bigquery.query.use_legacy")
            .message
            .starts_with("unknown BigQuery statement option")
    );
}

/// The option value never appears in an option error, since a credential may arrive under the
/// wrong key.
#[test]
fn an_option_rejection_quotes_the_key_and_nothing_else() {
    let failure = option_get_refusal("database", "bigquery.auth.credentials_typo");
    assert_message(&failure, &["\"bigquery.auth.credentials_typo\""]);
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
