use adbc_core::error::Status;

use crate::{BigQueryDatabaseOption, BigQueryStatementOption};

use super::QualifiedTableName;

/// Parses `value` as the destination table option.
fn parse(
    value: &str,
    default_project_id: Option<&crate::identifiers::ProjectIdentifier>,
    default_dataset_id: Option<&str>,
) -> adbc_core::error::Result<QualifiedTableName> {
    QualifiedTableName::parse_option(
        BigQueryStatementOption::DestinationTable.as_str(),
        value,
        default_project_id,
        default_dataset_id,
    )
}
use crate::test_support::errors::{assert_message, assert_message_omits};
use crate::test_support::identifiers::project;

#[test]
fn parses_and_completes_destination_tables() {
    for (value, default_project_id, dataset, expected) in [
        (
            "table",
            Some("default-project"),
            Some("default_dataset"),
            "default-project.default_dataset.table",
        ),
        (
            "other_dataset.table",
            Some("default-project"),
            None,
            "default-project.other_dataset.table",
        ),
        (
            "other-project.other_dataset.table",
            None,
            None,
            "other-project.other_dataset.table",
        ),
        (
            "example.com:project.other_dataset.table",
            None,
            None,
            "example.com:project.other_dataset.table",
        ),
        (
            "123456789012.other_dataset.table",
            None,
            None,
            "123456789012.other_dataset.table",
        ),
        (
            "table",
            Some("example.com:project"),
            Some("default_dataset"),
            "example.com:project.default_dataset.table",
        ),
        (
            "other-project.my_catalog.my_namespace.table",
            None,
            None,
            "other-project.my_catalog.my_namespace.table",
        ),
        (
            "example.com:project.my_catalog.my_namespace.table",
            None,
            None,
            "example.com:project.my_catalog.my_namespace.table",
        ),
    ] {
        let default_project = default_project_id.map(project);
        assert_eq!(
            parse(value, default_project.as_ref(), dataset)
                .unwrap()
                .as_option_value(),
            expected
        );
    }

    for (value, default_project_id, dataset) in [
        ("table", None, Some("dataset")),
        ("table", Some("project"), None),
        ("dataset.table", None, None),
        ("project..table", None, None),
        (".dataset.table", None, None),
        ("project.dataset.", None, None),
        ("project.catalog.namespace.nested.table", None, None),
        (
            "example.com:project.catalog.namespace.nested.table",
            None,
            None,
        ),
        ("project.catalog..table", None, None),
        ("project..namespace.table", None, None),
        (".catalog.namespace.table", None, None),
        ("project.catalog.namespace.", None, None),
        ("example.com:project.table", None, None),
    ] {
        let default_project = default_project_id.map(project);
        assert_eq!(
            parse(value, default_project.as_ref(), dataset)
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }
}

#[test]
fn destination_projects_keep_their_spelling_for_server_validation() {
    for value in [
        "INVALID",
        "short",
        "example.com:project",
        "analytics.example.com:project",
    ] {
        for (path, dataset) in [
            ("dataset", "dataset"),
            ("catalog.namespace", "catalog.namespace"),
        ] {
            let table = parse(&format!("{value}.{path}.table"), None, None).unwrap();
            assert_eq!(table.project_id().as_str(), value);
            assert_eq!(table.dataset_id(), dataset);
            assert_eq!(table.table_id(), "table");
        }
    }
}

/// `project.catalog.namespace.table` names a BigLake Iceberg table, which BigQuery addresses with
/// the dataset ID `catalog.namespace`. A dotted project needs the colon of a domain-scoped ID, as
/// BigQuery refuses any other.
#[test]
fn a_four_part_reference_names_a_catalog_namespace_dataset() {
    for (value, project_id, dataset_id) in [
        (
            "project.catalog.namespace.table",
            "project",
            "catalog.namespace",
        ),
        (
            "my-project.my-catalog.my-namespace.table",
            "my-project",
            "my-catalog.my-namespace",
        ),
        ("project.with.dots.table", "project", "with.dots"),
        (
            "example.com:project.catalog.namespace.table",
            "example.com:project",
            "catalog.namespace",
        ),
    ] {
        let table = parse(value, Some(&project("default-project")), Some("default")).unwrap();
        assert_eq!(table.project_id().as_str(), project_id, "{value}");
        assert_eq!(table.dataset_id(), dataset_id, "{value}");
        assert_eq!(table.table_id(), "table", "{value}");
        assert_eq!(table.as_option_value(), value);
        assert_eq!(
            serde_json::to_value(table.id()).unwrap(),
            serde_json::json!({
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": "table",
            })
        );
    }

    // Three parts stay `project.dataset.table`, holding the dataset to BigQuery's dataset rule.
    let error = parse("project.my-catalog.table", None, None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message(&error, &["the dataset ID in BigQuery option"]);
}

/// A reference that needs a missing default names the options that would supply it.
#[test]
fn a_missing_default_names_the_options_that_supply_it() {
    for (value, default_project_id, missing, form, statement_option, database_option) in [
        (
            "table",
            Some("default-project"),
            "dataset",
            "dataset.table",
            BigQueryStatementOption::DefaultDatasetId,
            BigQueryDatabaseOption::DatasetId,
        ),
        (
            "dataset.table",
            None,
            "project",
            "project.dataset.table",
            BigQueryStatementOption::DefaultProjectId,
            BigQueryDatabaseOption::ProjectId,
        ),
    ] {
        let default_project = default_project_id.map(project);
        let error = parse(value, default_project.as_ref(), None).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        for expected in [
            BigQueryStatementOption::DestinationTable.as_str(),
            &format!("names no {missing}"),
            &format!("write it as {form}"),
            statement_option.as_str(),
            database_option.as_str(),
        ] {
            assert!(
                error.message.contains(expected),
                "{expected}: {}",
                error.message
            );
        }
    }
}

#[test]
fn an_unparsable_reference_names_the_accepted_forms_without_echoing_the_value() {
    let secret = "destination-option-payload-secret-sentinel..table";
    let error = parse(secret, None, None).unwrap_err();
    assert_message_omits(&error, &[secret]);
    assert_message(
        &error,
        &["expected table, dataset.table, project.dataset.table, or \
             project.catalog.namespace.table"],
    );
}

/// An invalid component is rejected before any request, naming the option, the component and the
/// rule.
#[test]
fn an_invalid_component_names_the_option_the_half_and_the_rule() {
    let key = BigQueryStatementOption::DestinationTable.as_str();
    let long = "t".repeat(1_025);
    for (value, expected) in [
        (
            "bad-dataset.table".to_owned(),
            format!(
                "the dataset ID in BigQuery option {key} may contain only ASCII letters, digits, \
                 and underscores"
            ),
        ),
        (
            format!("{long}.table"),
            format!(
                "the dataset ID in BigQuery option {key} is 1025 characters long; it cannot \
                 exceed 1,024 characters"
            ),
        ),
        (
            "dataset.path/segment".to_owned(),
            format!(
                "the table ID in BigQuery option {key} cannot contain slashes or control \
                 characters"
            ),
        ),
        (
            format!("dataset.{long}"),
            format!(
                "the table ID in BigQuery option {key} is 1025 bytes long in UTF-8; it cannot \
                 exceed 1,024 bytes"
            ),
        ),
        (
            "project.cat/alog.namespace.table".to_owned(),
            format!(
                "the catalog in BigQuery option {key} cannot contain slashes or control characters"
            ),
        ),
        (
            "project.catalog.name\nspace.table".to_owned(),
            format!(
                "the namespace in BigQuery option {key} cannot contain slashes or control \
                 characters"
            ),
        ),
    ] {
        let error = parse(&value, Some(&project("default-project")), None).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(error.message, expected);
    }
}

/// A partition decorator is part of the table ID, so table IDs are not held to BigQuery's
/// table-name characters, which exclude `$`.
#[test]
fn a_partition_decorator_survives_the_table_check() {
    for value in [
        "dataset.table$20240101",
        "analytics.example.com:project.dataset.table$20240101",
    ] {
        let default_project = project("analytics.example.com:project");
        let table = parse(value, Some(&default_project), None).unwrap();
        assert_eq!(table.project_id().as_str(), "analytics.example.com:project");
        assert_eq!(table.dataset_id(), "dataset");
        assert_eq!(table.table_id(), "table$20240101");
    }
}
