//! Resolving the table an ingest writes to.
//!
//! Each part of the target comes from the ingest options, else from the statement's defaults,
//! which already fall back to the connection's current catalog and schema and the billing
//! project. Every name was validated when the option holding it was set.

use adbc_core::error::Result;

use crate::error::invalid_state;
use crate::identifiers::ProjectIdentifier;
use crate::ingest::IngestOptions;
use crate::rest::metadata::TableId;

pub(super) fn resolve_target<'a>(
    default_catalog: Option<&'a ProjectIdentifier>,
    default_schema: Option<&'a str>,
    options: &'a IngestOptions,
) -> Result<TableId<'a>> {
    let project_id = options
        .target_catalog
        .as_ref()
        .or(default_catalog)
        .ok_or_else(|| {
            invalid_state(
                "bulk ingest needs a BigQuery project; set adbc.ingest.target_catalog or \
                 bigquery.project_id",
            )
        })?;
    let dataset_id = options
        .target_db_schema
        .as_deref()
        .or(default_schema)
        .ok_or_else(|| {
            invalid_state(
                "bulk ingest needs a BigQuery dataset; set adbc.ingest.target_db_schema or \
                 bigquery.dataset_id",
            )
        })?;
    let table_id = options.target_table.as_deref().ok_or_else(|| {
        invalid_state("bulk ingest needs a target table; set adbc.ingest.target_table")
    })?;
    Ok(TableId {
        project_id: project_id.as_str(),
        dataset_id,
        table_id,
    })
}

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;

    use super::*;

    #[test]
    fn ingest_options_name_the_target_over_the_statement_defaults() {
        let default_project = ProjectIdentifier::try_new("default-project").unwrap();
        let explicit_project = ProjectIdentifier::try_new("explicit-project").unwrap();
        let mut options = IngestOptions {
            target_table: Some("target table".to_owned()),
            ..IngestOptions::default()
        };
        let resolve = |options: &IngestOptions| {
            resolve_target(Some(&default_project), Some("default_dataset"), options)
                .map(|table| table.to_string())
        };
        assert_eq!(
            resolve(&options).unwrap(),
            "default-project.default_dataset.target table"
        );
        options.target_catalog = Some(explicit_project);
        options.target_db_schema = Some("explicit_dataset".to_owned());
        assert_eq!(
            resolve(&options).unwrap(),
            "explicit-project.explicit_dataset.target table"
        );

        // Each missing part names the options that would supply it.
        for (catalog, schema, table, message) in [
            (
                None,
                Some("dataset"),
                Some("table"),
                "bulk ingest needs a BigQuery project; set adbc.ingest.target_catalog or \
                 bigquery.project_id",
            ),
            (
                Some(&default_project),
                None,
                Some("table"),
                "bulk ingest needs a BigQuery dataset; set adbc.ingest.target_db_schema or \
                 bigquery.dataset_id",
            ),
            (
                Some(&default_project),
                Some("dataset"),
                None,
                "bulk ingest needs a target table; set adbc.ingest.target_table",
            ),
        ] {
            let options = IngestOptions {
                target_table: table.map(str::to_owned),
                ..IngestOptions::default()
            };
            let failure = resolve_target(catalog, schema, &options).unwrap_err();
            assert_eq!(failure.status, Status::InvalidState);
            assert_eq!(failure.message, message);
        }
    }
}
