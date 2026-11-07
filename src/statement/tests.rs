//! Statement tests, split by subject.
//!
//! Shared imports and helpers live here; each submodule holds one subject.

use std::sync::Arc;

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde_json::json;

use crate::arrow_ipc_bigquery::encode_schema_fixture;
use crate::parameters::ParameterMode;
use crate::test_support::arrow::bigquery_typed;
use crate::test_support::http::{
    CapturedRequest, capture_requests, capture_requests_responding, completed_job, job_reference,
    ok,
};
use crate::transaction::QueryTransaction;
use crate::{
    BigQueryBulkIngestMethod, BigQueryConnectionOption, BigQueryCreateDisposition,
    BigQueryQueryPriority, BigQueryStatementOption as BqOption, BigQueryWriteDisposition,
};

use super::BigQueryStatement;
use super::sql_text::{normalize_sql, skip_comments};
use crate::test_support::driver::{
    test_connection, test_connection_at, test_query_at, test_statement, test_statement_at,
};
use crate::test_support::errors::{assert_failure, assert_message, assert_message_omits};
use crate::test_support::options::{
    assert_string_option, option_int, option_string, set_option, set_option_error,
};

/// A one-column `Int64` batch, the shape most parameter-binding tests bind.
fn int64_column(name: &str, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([(name, Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

/// Binds a stream of no batches, with one `Int64` column.
fn bind_no_rows(statement: &mut BigQueryStatement) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            std::iter::empty::<Result<RecordBatch, ArrowError>>(),
            schema,
        )))
        .unwrap();
}

/// A result schema of one INTEGER column, the shape most staged answers carry.
fn integer_schema(name: &str) -> serde_json::Value {
    json!({"fields": [{"name": name, "type": "INTEGER"}]})
}

/// A staged finished `SELECT` of an `answer` column holding `values`.
fn select_answer(values: &[&str]) -> (u16, serde_json::Value) {
    let rows: Vec<_> = values
        .iter()
        .map(|value| json!({"f": [{"v": value}]}))
        .collect();
    completed_job(
        "select-job",
        json!({
            "statementType": "SELECT",
            "schema": integer_schema("answer"),
            "rows": rows,
            "totalRows": values.len().to_string(),
        }),
    )
}

/// A staged finished dry run carrying `query` as its query statistics.
fn dry_run_answer(query: serde_json::Value) -> (u16, serde_json::Value) {
    ok(json!({"statistics": {"query": query}, "status": {"state": "DONE"}}))
}

/// The rows left in `reader`.
fn row_count(reader: impl Iterator<Item = Result<RecordBatch, ArrowError>>) -> usize {
    reader.map(|batch| batch.unwrap().num_rows()).sum()
}

/// Puts a manual-commit statement's transaction in `session`, as a first query would.
fn start_transaction(statement: &BigQueryStatement, session: &str) {
    let QueryTransaction::Start(None, start) = statement.transaction.begin_query().unwrap() else {
        panic!("expected a starting transaction")
    };
    start.started(Arc::from(session)).unwrap();
}

/// The entry points that run a statement, for tests that hold of them all.
const EXECUTE_ENTRIES: [fn(&mut BigQueryStatement) -> adbc_core::error::Result<()>; 3] = [
    |statement| statement.execute().map(drop),
    |statement| statement.execute_update().map(drop),
    |statement| statement.execute_partitions().map(drop),
];

mod execution;
mod ingest;
mod job_metadata;
mod modes;
mod options;
mod transactions;
