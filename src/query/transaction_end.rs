//! Whether a statement in the driver's transaction session ended that transaction.
//!
//! A lone `COMMIT TRANSACTION` or `ROLLBACK TRANSACTION` says so in its statement type. A script
//! or procedure reports `SCRIPT` or `CALL` whatever it ran, so its newest child job decides: the
//! transaction ended if that child committed or rolled back, or ran outside any transaction
//! (BigQuery reports `statistics.transactionInfo` on each child that ran inside one). A script
//! that began a new transaction after ending the driver's leaves one active, which the driver's
//! commit or rollback then ends.

use adbc_core::error::{Result, Status};
use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{job_name, with_job_link};
use crate::rest::jobs::{JobClient, JobReference};
use crate::rest::json::decode_json;

use super::response::QueryMetadata;

/// Whether the completed statement `metadata` describes ended its session's transaction.
///
/// Only a script or procedure costs a request, a `jobs.list` that needs the
/// `bigquery.jobs.list` permission. If it fails, the transaction is assumed active, and a later
/// commit or rollback reports it lost. Only cancellation propagates: the statement already ran.
pub(super) async fn ended_transaction(
    client: &JobClient,
    operation: Option<&Operation>,
    metadata: &QueryMetadata,
) -> Result<bool> {
    match metadata.statement_type.as_str() {
        "COMMIT_TRANSACTION" | "ROLLBACK_TRANSACTION" => Ok(true),
        "SCRIPT" | "CALL" => match &metadata.job_reference {
            Some(script) => script_ended_transaction(client, operation, script).await,
            None => Ok(false),
        },
        _ => Ok(false),
    }
}

async fn script_ended_transaction(
    client: &JobClient,
    operation: Option<&Operation>,
    script: &JobReference,
) -> Result<bool> {
    let children = match client.latest_child_job(operation, script).await {
        Ok(body) => decode_json::<ChildJobs>(&body, "the BigQuery jobs.list response"),
        Err(failure) if failure.status == Status::Cancelled => {
            return Err(with_job_link(failure, Some(script)));
        }
        Err(failure) => Err(failure),
    };
    match children {
        // A script whose statements created no child job left the transaction as it was.
        Ok(children) => Ok(children
            .jobs
            .first()
            .is_some_and(ChildJob::ended_transaction)),
        Err(failure) => {
            diag!(
                Warn,
                "assuming the BigQuery transaction is still active: could not tell whether \
                 script job {} ended it (status {:?})",
                job_name(script),
                failure.status
            );
            Ok(false)
        }
    }
}

/// A `jobs.list` page, newest job first.
#[derive(Deserialize)]
struct ChildJobs {
    #[serde(default)]
    jobs: Vec<ChildJob>,
}

#[derive(Deserialize)]
struct ChildJob {
    #[serde(default)]
    statistics: Option<ChildStatistics>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChildStatistics {
    #[serde(default)]
    query: Option<ChildQueryStatistics>,
    #[serde(default)]
    transaction_info: Option<IgnoredAny>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChildQueryStatistics {
    #[serde(default)]
    statement_type: String,
}

impl ChildJob {
    /// A child without query statistics proves nothing, so it leaves the transaction active.
    fn ended_transaction(&self) -> bool {
        let Some(statistics) = &self.statistics else {
            return false;
        };
        let Some(query) = &statistics.query else {
            return false;
        };
        matches!(
            query.statement_type.as_str(),
            "COMMIT_TRANSACTION" | "ROLLBACK_TRANSACTION"
        ) || statistics.transaction_info.is_none()
    }
}

#[cfg(test)]
mod tests;
