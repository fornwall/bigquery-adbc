use adbc_core::error::Status;

use super::{ProjectIdentifier, validate_dataset_id, validate_table_id};
use crate::test_support::errors::assert_message;

#[test]
fn project_identifiers_preserve_values_for_service_validation() {
    for value in [
        "abcdef",
        "example-project",
        "a12345",
        "a-b-c-d-e-f",
        "a12345678901234567890123456789",
        "123456789012",
        "example.com:project",
        "x",
        "A_PROJECT-",
        "a123456789012345678901234567890",
        "a project",
        "abc?de#fg%hi",
        "åbcdef",
        "\u{85}",
        "...",
    ] {
        assert_eq!(ProjectIdentifier::try_new(value).unwrap().as_str(), value);
    }
}

#[test]
fn rejection_names_the_source_but_never_the_value() {
    let error = ProjectIdentifier::try_new_from("the bigquery.project_id option", "SECRET/value")
        .unwrap_err();
    assert_message(&error, &["the bigquery.project_id option"]);
    assert!(
        !error.message.contains("SECRET"),
        "a rejected value may be a misdirected secret and must not be echoed: {}",
        error.message
    );
}

#[test]
fn project_identifiers_reject_only_structurally_unsafe_values() {
    for value in [
        "",
        ".",
        "..",
        "p/datasets/x",
        "p\\datasets\\x",
        "abc\0de",
        "abc\nde",
        "abc\tde",
        "abc\u{7f}de",
    ] {
        assert!(ProjectIdentifier::try_new(value).is_err(), "{value:?}");
    }
}

#[test]
fn dataset_ids_are_validated_completely() {
    for valid in ["", "dataset", "Sample_Dataset1"] {
        validate_dataset_id("BigQuery dataset ID", valid).unwrap();
    }
    for invalid in ["dataset-name", "dataset.name", "valid_prefix' OR 1=1"] {
        assert_eq!(
            validate_dataset_id("BigQuery dataset ID", invalid)
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }
    assert_eq!(
        validate_dataset_id("BigQuery dataset ID", &"a".repeat(1025))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

/// The ingest target and the destination-table option share one validator; each rejection names
/// its own subject.
#[test]
fn table_ids_are_validated_completely_under_whichever_subject_named_them() {
    for valid in [
        "table",
        "table$20240101",
        "a table with spaces",
        "\u{e5}",
        "...",
        "a.b",
    ] {
        validate_table_id("BigQuery table ID", valid).unwrap();
    }
    // BigQuery's limit is 1,024 UTF-8 bytes (verified live), here 512 two-byte characters.
    validate_table_id("BigQuery table ID", &"\u{e5}".repeat(512)).unwrap();

    for (subject, value, expected) in [
        (
            "BigQuery table ID",
            String::new(),
            "BigQuery table ID cannot be empty",
        ),
        (
            "BigQuery table ID",
            "path/segment".to_owned(),
            "BigQuery table ID cannot contain slashes or control characters",
        ),
        (
            "BigQuery table ID",
            "tab\tulated".to_owned(),
            "BigQuery table ID cannot contain slashes or control characters",
        ),
        // A name a request URL cannot carry: parsing resolves the segment away rather than
        // asking for the table. A name that merely contains a dot is untouched, above.
        (
            "BigQuery table ID",
            ".".to_owned(),
            "BigQuery table ID cannot be \".\" or \"..\"",
        ),
        (
            "BigQuery table ID",
            "..".to_owned(),
            "BigQuery table ID cannot be \".\" or \"..\"",
        ),
        (
            "BigQuery table ID",
            "t".repeat(1025),
            "BigQuery table ID is 1025 bytes long in UTF-8; it cannot exceed 1,024 bytes",
        ),
        (
            "BigQuery table ID",
            "\u{e5}".repeat(513),
            "BigQuery table ID is 1026 bytes long in UTF-8; it cannot exceed 1,024 bytes",
        ),
        (
            "the table ID in BigQuery option bigquery.query.destination_table",
            "path/segment".to_owned(),
            "the table ID in BigQuery option bigquery.query.destination_table cannot contain \
             slashes or control characters",
        ),
        (
            "the table ID in BigQuery option bigquery.query.destination_table",
            "t".repeat(1025),
            "the table ID in BigQuery option bigquery.query.destination_table is 1025 bytes long \
             in UTF-8; it cannot exceed 1,024 bytes",
        ),
    ] {
        let failure = validate_table_id(subject, &value).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(failure.message, expected);
    }
}
