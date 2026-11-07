//! Connection settings and per-execution state passed to the query layer.

use std::sync::Arc;

use crate::pipeline::QueryResultOptions;

use super::{QueryDefaults, StatementQueryOptions};
use crate::cancellation::Operation;
use crate::clients::ClientState;
use crate::identifiers::ProjectIdentifier;
use crate::query::ExecutionProgress;
use crate::runtime::SharedRuntime;
use crate::transaction::TransactionState;

/// Connection settings captured when a statement is created.
///
/// Later connection changes do not affect the statement, except for the current catalog and
/// schema, which are refreshed from the connection's `CurrentNamespace` before each use.
#[derive(Clone, Debug)]
pub(crate) struct ExecutionContext {
    pub(crate) runtime: SharedRuntime,
    /// Shared clients, per-project Storage Read availability, and cached dataset locations.
    pub(crate) state: Arc<ClientState>,
    pub(crate) billing_project_id: Option<ProjectIdentifier>,
    pub(crate) current_catalog: Option<ProjectIdentifier>,
    pub(crate) current_schema: Option<Arc<str>>,
    pub(crate) location: Option<Arc<str>>,
    pub(crate) result_options: QueryResultOptions,
}

impl ExecutionContext {
    /// The connection's default catalog and schema.
    pub(crate) fn defaults(&self) -> QueryDefaults<'_> {
        QueryDefaults {
            catalog: self.current_catalog.as_ref(),
            db_schema: self.current_schema.as_deref(),
        }
    }

    /// Resolves statement overrides against connection defaults.
    ///
    /// The billing project is the final catalog fallback: BigQuery requires a project alongside
    /// a default dataset, even when no catalog was selected.
    pub(crate) fn statement_defaults<'a>(
        &'a self,
        options: &'a StatementQueryOptions,
    ) -> QueryDefaults<'a> {
        QueryDefaults {
            catalog: options
                .default_project_id
                .as_ref()
                .or(self.current_catalog.as_ref())
                .or(self.billing_project_id.as_ref()),
            db_schema: options
                .default_dataset_id
                .as_deref()
                .or(self.current_schema.as_deref()),
        }
    }
}

/// Execution context of a query the driver issues itself (session control, ingest DDL).
#[derive(Clone, Copy)]
pub(crate) struct QueryContext<'a> {
    pub(crate) defaults: QueryDefaults<'a>,
    pub(crate) location: Option<&'a str>,
    pub(crate) session_id: Option<&'a str>,
    pub(crate) operation: Option<&'a Operation>,
}

/// Settings, cancellation and transaction state for a user statement.
#[derive(Clone, Copy)]
pub(crate) struct StatementQueryContext<'a> {
    pub(crate) execution: &'a ExecutionContext,
    pub(crate) session_id: Option<&'a str>,
    pub(crate) operation: &'a Operation,
    pub(crate) options: &'a StatementQueryOptions,
    /// Updated when the caller's SQL commits or rolls back a transaction.
    pub(crate) transaction: &'a TransactionState,
    /// Where query responses publish progress. `None` for the parameter rows a reader runs while
    /// it is drained, which leave the first job's progress in place.
    pub(crate) progress: Option<&'a ExecutionProgress>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;

    /// Formatting initialized clients must not expose credentials.
    #[test]
    fn debug_output_names_no_credential() {
        const TOKEN: &str = "ya29.credential-that-must-not-leak";
        let runtime = crate::runtime::new_runtime().unwrap();
        let config = DatabaseConfig {
            authentication_type: crate::BigQueryAuthenticationType::TemporaryAccessToken,
            access_token: Some(TOKEN.into()),
            ..DatabaseConfig::default()
        };
        let state = ClientState::new(&config);
        state.rest_job_client(&runtime).unwrap();
        runtime.block_on(state.read_client(&runtime, None)).unwrap();
        let context = ExecutionContext {
            runtime,
            state,
            billing_project_id: None,
            current_catalog: None,
            current_schema: None,
            location: None,
            result_options: QueryResultOptions::default(),
        };
        let debug = format!("{context:?} {context:#?}");
        assert!(!debug.contains(TOKEN), "{debug}");
    }
}
