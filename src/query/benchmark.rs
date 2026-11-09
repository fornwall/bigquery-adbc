//! Network-free measurements of the production query response and reader paths.

use arrow_array::RecordBatch;
use bytes::Bytes;
use std::sync::Arc;

use super::direct::{CompletedQuery, QueryResponseDecoder};
use super::response::{QueryMetadata, QueryResponse};
use super::{ExecutionContext, QueryResultMode, reader_from_rest_query};
use crate::cancellation::{Cancellation, Operation, operation_reader};
use crate::clients::ClientState;
use crate::config::DatabaseConfig;

/// A query submission with dbt-style labels and a configurable SQL length.
#[derive(Debug)]
pub struct QueryRequestFixture(super::plan::QueryPlan);

impl QueryRequestFixture {
    /// Builds a fresh execution plan from already-stored statement SQL and options.
    pub fn plan(&self) -> impl std::fmt::Debug {
        super::plan::QueryPlan::new(
            Some(&self.0.project_id),
            super::QueryDefaults {
                catalog: self
                    .0
                    .default_dataset
                    .as_ref()
                    .map(|dataset| &dataset.project_id),
                db_schema: self
                    .0
                    .default_dataset
                    .as_ref()
                    .map(|dataset| dataset.dataset_id.as_ref()),
            },
            self.0.location.as_deref(),
            None,
            self.0.sql.clone(),
            None,
            &self.0.options,
            super::plan::QueryJobCreation::Optional,
        )
        .expect("the benchmark query plans")
    }

    /// Prepares a SELECT and its plan outside the measured serialization work.
    pub fn new(sql_bytes: usize) -> Self {
        use super::plan::{QueryJobCreation, QueryPlan};
        use super::{QueryDefaults, StatementQueryOptions};
        use crate::identifiers::ProjectIdentifier;

        let mut sql =
            "SELECT 0 AS failures, false AS should_warn, false AS should_error".to_owned();
        if sql_bytes > sql.len() + 6 {
            sql.push_str(" /* ");
            sql.extend(std::iter::repeat_n('x', sql_bytes - sql.len() - 2));
            sql.push_str("*/");
        }
        let project = ProjectIdentifier::try_new("benchmark-project").expect("a valid project");
        Self(
            QueryPlan::new(
                Some(&project),
                QueryDefaults {
                    catalog: Some(&project),
                    db_schema: Some("dbt_benchmark"),
                },
                Some("US"),
                None,
                sql.into(),
                None,
                &StatementQueryOptions {
                    labels: Some(Arc::new(
                        [(
                            "dbt_invocation_id".to_owned(),
                            "benchmark-invocation".to_owned(),
                        )]
                        .into_iter()
                        .collect(),
                    )),
                    ..Default::default()
                },
                QueryJobCreation::Optional,
            )
            .expect("the benchmark query plans"),
        )
    }

    /// Serializes the production jobs.query request, without HTTP or server work.
    pub fn encode(&self) -> Vec<u8> {
        let super::plan::QueryIdentity::Request(request_id) = self.0.identity() else {
            unreachable!("the fixture uses jobs.query");
        };
        super::request::JobsQueryRequest::new(&self.0, request_id, QueryResultMode::Rows)
            .encode()
            .expect("the benchmark query serializes")
    }
}

/// A resident query result, with the integer and two booleans a dbt data test returns.
#[derive(Debug)]
pub struct QueryResultFixture {
    body: Bytes,
    execution: ExecutionContext,
    cancellation: Arc<Cancellation>,
}

/// One decoded response, prepared outside reader-only timing.
pub struct QueryResultInput(QueryResponse, Operation);

impl std::fmt::Debug for QueryResultInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("QueryResultInput")
            .field(&self.0)
            .finish_non_exhaustive()
    }
}

impl QueryResultFixture {
    /// Prepares one response and a warmed anonymous client without making network requests.
    pub fn new(row_count: usize) -> Self {
        let rows = (0..row_count)
            .map(|_| serde_json::json!({"f":[{"v":"0"},{"v":"false"},{"v":"false"}]}))
            .collect::<Vec<_>>();
        let body = serde_json::to_vec(&serde_json::json!({
            "jobComplete": true,
            "queryId": "benchmark-query",
            "location": "US",
            "statementType": "SELECT",
            "totalRows": row_count.to_string(),
            "totalBytesProcessed": "400000",
            "totalBytesBilled": "10485760",
            "totalSlotMs": "25",
            "schema": {"fields": [
                {"name":"failures","type":"INTEGER"},
                {"name":"should_warn","type":"BOOLEAN"},
                {"name":"should_error","type":"BOOLEAN"}
            ]},
            "rows": rows
        }))
        .expect("the benchmark response serializes");
        let runtime = crate::runtime::new_runtime().expect("the benchmark runtime starts");
        let state = ClientState::new(&DatabaseConfig {
            authentication_type: crate::BigQueryAuthenticationType::Anonymous,
            ..Default::default()
        });
        state
            .rest_job_client(&runtime)
            .expect("the anonymous client initializes");
        Self {
            body: Bytes::from(body),
            cancellation: Cancellation::new(runtime.clone(), state.clone(), "benchmark"),
            execution: ExecutionContext {
                runtime,
                state,
                billing_project_id: None,
                current_catalog: None,
                current_schema: None,
                location: None,
                result_options: Default::default(),
            },
        }
    }

    /// Decodes the response through the production scheduling and conversion permit.
    pub fn decode(&self) -> QueryResultInput {
        let decoder = QueryResponseDecoder::new(&self.execution.runtime, QueryResultMode::Rows);
        let operation = self.cancellation.begin();
        QueryResultInput(
            self.execution
                .runtime
                .block_on(decoder.decode(self.body.clone(), "benchmark response", Some(&operation)))
                .expect("the benchmark response decodes"),
            operation,
        )
    }

    /// Acquires the warmed REST job client used to submit each query, without network work.
    pub fn job_client(&self) -> impl Clone {
        self.execution
            .state
            .rest_job_client(&self.execution.runtime)
            .expect("the benchmark job client initializes")
    }

    /// Reads the decoded response to EOF through the production reader selection and pipeline.
    pub fn read(&self, input: QueryResultInput) -> Vec<RecordBatch> {
        let QueryResultInput(mut response, operation) = input;
        let mut metadata = QueryMetadata::default();
        metadata.absorb_query(&mut response);
        let complete = CompletedQuery {
            metadata,
            page: response.page(),
            completed_job: None,
            returns_rows: true,
        };
        let reader =
            reader_from_rest_query(&self.execution, complete, Some(operation.clone()), None)
                .expect("the benchmark reader opens");
        operation_reader(reader, operation)
            .collect::<Result<_, _>>()
            .expect("the benchmark rows convert")
    }
}
