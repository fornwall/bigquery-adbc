//! Statement tests, split by subject.
//!
//! Shared imports and helpers live here; each submodule holds one subject.

use std::sync::Arc;

use adbc_core::constants::{ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED};
use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
use adbc_core::{CancelHandle, Connection, Optionable, Statement};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde_json::json;

use crate::arrow_ipc_bigquery::encode_schema_fixture;
use crate::cancellation::{Cancellation, OperationCancelHandle, operation_reader};
use crate::parameters::ParameterMode;
use crate::runtime::new_runtime;
use crate::test_support::arrow::bigquery_typed;
use crate::test_support::http::{
    CapturedRequest, capture_requests, capture_requests_responding, completed_job, job_reference,
    ok,
};
use crate::transaction::QueryTransaction;
use crate::{
    BigQueryBulkIngestMethod, BigQueryConnectionOption, BigQueryCreateDisposition,
    BigQueryQueryPriority, BigQueryStatementOption, BigQueryWriteDisposition,
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

mod execution;
mod ingest;
mod job_metadata;
mod modes;
mod options;
mod transactions;
