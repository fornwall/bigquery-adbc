//! Sequential consumption of direct REST query result pages.

use std::collections::VecDeque;

use adbc_core::error::Status;
use arrow_schema::ArrowError;

use crate::cancellation::Operation;
use crate::diag::diag;
use crate::error::{error, job_name, with_job_link};
use crate::pipeline::{BATCH_SIZE, ResultSink};
use crate::rest::jobs::{JobClient, JobReference, QueryResultsRead};
use crate::rows::RestRow;

use super::response::{QueryPage, QueryResponse};

/// Owns the initial completion page and follows its page tokens.
///
/// Tokens are a sequential cursor, so pages are fetched one at a time; `rest_buffer` converts
/// fetched batches in parallel.
pub(super) struct QueryPager {
    client: JobClient,
    reference: Option<JobReference>,
    operation: Option<Operation>,
    rows: std::vec::IntoIter<RestRow>,
    page_token: Option<String>,
    /// Recent tokens detect short pagination cycles without retaining every token of a long scan.
    seen_tokens: VecDeque<String>,
}

/// Maximum pagination cycle length detected by the bounded token history.
const REMEMBERED_PAGE_TOKENS: usize = 16;

impl QueryPager {
    pub(super) fn new(
        client: JobClient,
        reference: Option<JobReference>,
        operation: Option<Operation>,
        page: QueryPage,
    ) -> Self {
        Self {
            client,
            reference,
            operation,
            rows: page.rows.into_iter(),
            page_token: page.page_token,
            seen_tokens: VecDeque::new(),
        }
    }

    /// Takes at most [`BATCH_SIZE`] rows from the current page, fetching only when it is empty.
    /// Short pages must be delivered before waiting for another network response. An empty batch
    /// ends the result, and `None` means cancellation.
    ///
    /// `sink` supplies the cancellation and the driver-wide conversion permit for page decoding;
    /// the pager delivers nothing itself.
    pub(super) async fn next_batch(
        &mut self,
        sink: &ResultSink,
    ) -> Result<Option<Vec<RestRow>>, ArrowError> {
        loop {
            if self.rows.len() != 0 {
                return Ok(Some(if self.rows.len() <= BATCH_SIZE {
                    // An intact short page reuses its allocation, avoiding a second row vector
                    // and space for a full-sized batch.
                    std::mem::take(&mut self.rows).collect()
                } else {
                    self.rows.by_ref().take(BATCH_SIZE).collect()
                }));
            }

            let Some(page_token) = self.page_token.take() else {
                return Ok(Some(Vec::new()));
            };
            let Some(reference) = self.reference.as_ref() else {
                return Err(external(error(
                    "BigQuery returned a result page token without a durable job reference",
                    Status::InvalidData,
                )));
            };
            let external = |source| external(with_job_link(source, Some(reference)));
            // Once per result: a token not yet followed means this is the first further page.
            if self.seen_tokens.is_empty() {
                diag!(
                    Debug,
                    "fetching further REST pages of BigQuery job {}",
                    job_name(reference)
                );
            }
            let fetch = self.client.get_query_results(
                self.operation.as_ref(),
                reference,
                Some(&page_token),
                super::response::RESULT_PAGE_ROWS,
                QueryResultsRead::Page,
            );
            let body = tokio::select! {
                biased;
                () = sink.cancellation.cancelled() => return Ok(None),
                result = fetch => result.map_err(external)?,
            };
            // Decoding a full page takes milliseconds of CPU, more than its Arrow conversion, so
            // it runs on the blocking pool under the conversion permit instead of holding a
            // runtime worker.
            let Some(decode_permit) = sink.conversion_permit().await? else {
                return Ok(None);
            };
            let mut response = QueryResponse::decode_bounded(
                body,
                "the BigQuery jobs.getQueryResults page response",
                decode_permit,
            )
            .await
            .map_err(external)?;
            if !response.job_complete {
                return Err(external(error(
                    "a BigQuery result pagination response unexpectedly reports an incomplete job",
                    Status::InvalidData,
                )));
            }
            // A later page's `errors` are ignored: the job has completed, and the field also
            // carries warnings that must not fail it.
            let canonical = response
                .canonical_polled_reference(reference)
                .map_err(external)?;
            let page = response.page();
            if self.seen_tokens.len() == REMEMBERED_PAGE_TOKENS {
                self.seen_tokens.pop_front();
            }
            self.seen_tokens.push_back(page_token);
            if page
                .page_token
                .as_ref()
                .is_some_and(|token| self.seen_tokens.contains(token))
            {
                return Err(external(error(
                    "BigQuery repeated a query result page token",
                    Status::IO,
                )));
            }
            if &canonical != reference {
                if let Some(operation) = self.operation.as_ref() {
                    operation
                        .register_completed_job(&canonical)
                        .map_err(external)?;
                }
                self.reference = Some(canonical);
            }
            self.rows = page.rows.into_iter();
            self.page_token = page.page_token;
        }
    }
}

fn external(source: adbc_core::error::Error) -> ArrowError {
    ArrowError::ExternalError(Box::new(source))
}

#[cfg(test)]
mod tests;
