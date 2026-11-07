//! Connection tests, split by subject.
//!
//! Shared imports and fixtures live here; each submodule holds one subject.

use std::thread;

use adbc_core::constants::{
    ADBC_OPTION_ISOLATION_LEVEL_DEFAULT, ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
    ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT, ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED,
};
use adbc_core::error::Status;
use adbc_core::options::{ObjectDepth, OptionConnection, Statistics};
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable as _, Statement as _};
use arrow_array::{Array, BooleanArray, Int16Array, Int64Array, RecordBatchReader, UnionArray};
use serde_json::json;

use super::objects::object_table;
use super::table_metadata::bigquery_table_type;
use super::table_schema::format_rfc3339_millis;
use super::{SqlPattern, resolve_table_reference, table_metadata_error, table_schema_metadata};
use crate::rest::metadata::{MetadataError, Table};
use crate::test_support::arrow::{child_structs, list, string_at, strings, typed};
use crate::test_support::driver::test_connection_at;
use crate::test_support::errors::assert_message;
use crate::test_support::http::{CapturedRequest, CapturedServer, capture_requests};
use crate::test_support::identifiers::project;
use crate::{BigQueryAuthenticationType, BigQueryDatabaseOption, BigQueryDriver};

mod disable_wildcards;
mod lifecycle;
mod objects;
mod options;
mod partitions;
mod statistics;
mod table_metadata;
mod table_schema;

fn anonymous_connection_at(endpoint: &str) -> crate::BigQueryConnection {
    anonymous_connection_at_with_defaults(endpoint, Some("test-project"), None)
}

fn anonymous_connection_at_with_defaults(
    endpoint: &str,
    project_id: Option<&str>,
    dataset_id: Option<&str>,
) -> crate::BigQueryConnection {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let mut options = vec![
        (
            BigQueryDatabaseOption::AuthenticationType.into(),
            BigQueryAuthenticationType::Anonymous.into(),
        ),
        (
            BigQueryDatabaseOption::Endpoint.into(),
            endpoint.to_owned().into(),
        ),
    ];
    if let Some(project_id) = project_id {
        options.push((
            BigQueryDatabaseOption::ProjectId.into(),
            project_id.to_owned().into(),
        ));
    }
    if let Some(dataset_id) = dataset_id {
        options.push((
            BigQueryDatabaseOption::DatasetId.into(),
            dataset_id.to_owned().into(),
        ));
    }
    let database = driver.new_database_with_opts(options).unwrap();
    database.new_connection().unwrap()
}

fn not_found_response() -> serde_json::Value {
    json!({
        "error": {
            "code": 404,
            "message": "Not found",
            "status": "NOT_FOUND",
            "errors": [
                {"message": "Not found", "domain": "global", "reason": "notFound"}
            ]
        }
    })
}

/// One `tables.list` entry, as the service spells it.
fn listed_table(table_id: &str) -> serde_json::Value {
    json!({"tableReference": {"tableId": table_id}, "type": "TABLE"})
}

/// A `tables.get` answer with one column, enough for a column-depth listing to describe.
fn described_table() -> serde_json::Value {
    json!({
        "type": "TABLE",
        "schema": {"fields": [{"name": "id", "type": "INTEGER"}]}
    })
}

/// More tables than `TABLE_METADATA_CONCURRENCY`, so the fan-out must drain a finished
/// `tables.get` before spawning the next.
const TABLES: usize = 17;

/// The table listing a crowded dataset answers with, followed by `described` successful
/// `tables.get` responses.
fn crowded_dataset_responses(described: usize) -> Vec<(u16, serde_json::Value)> {
    let listing = (0..TABLES)
        .map(|index| listed_table(&format!("table-{index:02}")))
        .collect::<Vec<_>>();
    let mut responses = vec![(200, json!({"tables": listing}))];
    responses.extend(std::iter::repeat_n((200, described_table()), described));
    responses
}

/// The one batch `get_objects` answers for `depth`, filtered by dataset, table and table type.
fn objects_batch(
    connection: &crate::BigQueryConnection,
    depth: ObjectDepth,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_types: Option<Vec<&str>>,
) -> arrow_array::RecordBatch {
    connection
        .get_objects(depth, None, db_schema, table_name, table_types, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}

/// `None` selects statistics; a depth selects object discovery through the same table walker.
fn discover_metadata(
    connection: &crate::BigQueryConnection,
    depth: Option<ObjectDepth>,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
    match depth {
        Some(depth) => connection.get_objects(depth, catalog, db_schema, table_name, None, None),
        None => connection.get_statistics(catalog, db_schema, table_name, false),
    }
}
