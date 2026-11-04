//! The `bigquery.dataset.get_location` lookup behind dbt's `adapter.get_dataset_location`.
//!
//! It reads the dataset with `datasets.get`, so it runs no job. See `DBT-CORE.md`.

use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::{parse_json_option, required_string};
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::error::invalid_argument;
use crate::identifiers::{ProjectIdentifier, is_dot_segment};
use crate::query::ExecutionContext;
use crate::rest::metadata::DatasetId;

const OPTION: BigQueryStatementOption = BigQueryStatementOption::GetDatasetLocation;

/// The dataset lookup configured on an ADBC statement.
///
/// The payload is parsed and validated when set, so a malformed one fails immediately. An empty
/// string clears it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DatasetLocationOptions {
    lookup: Option<Lookup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Lookup {
    /// The option value as set, which is what reads back.
    payload: String,
    project_id: ProjectIdentifier,
    dataset_id: String,
}

impl DatasetLocationOptions {
    pub(crate) fn is_active(&self) -> bool {
        self.lookup.is_some()
    }

    pub(crate) fn set(&mut self, payload: String) -> Result<()> {
        self.lookup = if payload.is_empty() {
            None
        } else {
            Some(Lookup::parse(payload)?)
        };
        Ok(())
    }

    pub(crate) fn option_value(&self) -> Option<&str> {
        self.lookup.as_ref().map(|lookup| lookup.payload.as_str())
    }

    /// One `location` row for an existing dataset, none for a missing one. Any other failure,
    /// including a missing project, is an error: it says nothing about the dataset.
    pub(crate) fn execute(
        &self,
        context: &ExecutionContext,
        operation: &Operation,
    ) -> Result<RecordBatch> {
        let Some(lookup) = self.lookup.as_ref() else {
            return location_batch(None);
        };
        let dataset = DatasetId {
            project_id: lookup.project_id.as_str(),
            dataset_id: &lookup.dataset_id,
        };
        let client = context.state.metadata_client(&context.runtime)?;
        // Not the connection's location cache, which outlives a dropped dataset: this answers
        // whether the dataset exists now.
        let location = context.runtime.block_on(Box::pin(
            client.get_dataset_location(Some(operation), dataset),
        ));
        match location {
            Ok(location) => location_batch(Some(&location)),
            Err(source) if source.is_missing_dataset() => location_batch(None),
            Err(source) => Err(source.into_error(&format!(
                "failed to get the location of BigQuery dataset {dataset}"
            ))),
        }
    }
}

impl Lookup {
    /// Parses `{"project": ..., "dataset": ...}`.
    ///
    /// The dataset ID is not held to BigQuery's naming rules: `datasets.get` reports an ID it
    /// could never hold as a missing dataset, which is this lookup's answer for it. `.` and `..`
    /// are refused because URL parsing would resolve them, addressing a different resource.
    fn parse(payload: String) -> Result<Self> {
        let serde_json::Value::Object(object) = parse_json_option(&payload, OPTION)? else {
            return Err(invalid_argument(format!(
                "BigQuery option {} must be a JSON object {{\"project\": ..., \"dataset\": ...}}",
                OPTION.as_str()
            )));
        };
        let project_id = ProjectIdentifier::try_new_from(
            &format!("BigQuery option {}", OPTION.as_str()),
            required_string(&object, "project", OPTION)?,
        )?;
        let dataset_id = required_string(&object, "dataset", OPTION)?;
        if is_dot_segment(dataset_id) {
            return Err(invalid_argument(format!(
                "the dataset in BigQuery option {} cannot be \".\" or \"..\"",
                OPTION.as_str()
            )));
        }
        let dataset_id = dataset_id.to_owned();
        Ok(Self {
            payload,
            project_id,
            dataset_id,
        })
    }
}

/// The result schema of the lookup, known without a request.
pub(crate) fn location_schema() -> Schema {
    Schema::new(vec![Field::new("location", DataType::Utf8, false)])
}

fn location_batch(location: Option<&str>) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(location_schema()),
        vec![Arc::new(StringArray::from_iter_values(location))],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests;
