//! dbt 2.0's option-only Cloud Storage object write.
//!
//! A dbt python model uploads its compiled source here before the Dataproc or Vertex AI job that
//! runs it is submitted. See `DBT.md`.

use adbc_core::error::Result;

use super::lro::{Delivery, LroPoller, PollBudget};
use super::{resource_segment, text_slots};
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::query::ExecutionContext;
use crate::rest::jobs::next_identity;
use crate::rest::json::encode_json;
use crate::rest::request::{Request, multipart_related};
use crate::rest::url::escape_url_component;

/// The operation's name in error messages.
const OPERATION: &str = "Cloud Storage object write";

/// A Cloud Storage object write configured on an ADBC statement by dbt 2.0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GcsWriteOptions {
    bucket: Option<String>,
    object_name: Option<String>,
    /// The object's bytes. An empty value clears the option; leaving it unset writes a zero-byte
    /// object.
    content: Option<String>,
}

text_slots!(GcsWriteOptions {
    WriteGcsBucket => bucket,
    WriteGcsObjectName => object_name,
    WriteGcsContent => content,
});

impl GcsWriteOptions {
    /// Uploads the configured content, resending after a transient failure: a multipart insert
    /// replaces the object atomically, and a replay writes the same bytes.
    ///
    /// Unlike the fork, both the bucket and the object name are required: the object is what the
    /// job submitted next reads, so writing to the wrong key silently runs stale code.
    pub(crate) fn execute(&self, context: &ExecutionContext, operation: &Operation) -> Result<()> {
        // The bucket is a path segment, which `.` or `..` would resolve away. The object name
        // travels in the query string, where no segment is resolved.
        let bucket = resource_segment(
            self.required(OPERATION, BigQueryStatementOption::WriteGcsBucket)?,
            BigQueryStatementOption::WriteGcsBucket,
        )?;
        let object_name = self.required(OPERATION, BigQueryStatementOption::WriteGcsObjectName)?;
        let content = self.content.as_deref().unwrap_or_default();

        let configuration = context.state.configuration(&context.runtime)?;
        let endpoint = configuration.endpoints.gcs();
        // One multipart insert, which is what the fork's Go client sends below its 16 MiB chunk
        // size, with that client's `projection=full` and compact response.
        let url = format!(
            "{endpoint}/upload/storage/v1/b/{}/o?uploadType=multipart&name={}\
             &projection=full&alt=json",
            escape_url_component(bucket),
            escape_url_component(object_name),
        );
        let metadata = encode_json(
            &serde_json::json!({"bucket": bucket, "name": object_name}),
            "the Cloud Storage object metadata",
        )?;
        // `text/plain` is what the Go client's content sniffer settles on for both a compiled
        // model and a notebook document.
        let boundary = next_identity("adbc_gcs_");
        let body = bytes::Bytes::from(multipart_related(
            &boundary,
            &metadata,
            content.as_bytes(),
            "text/plain; charset=utf-8",
        ));
        let content_type = format!("multipart/related; boundary={boundary}");
        let label = format!("write Cloud Storage object gs://{bucket}/{object_name}");
        let poller = LroPoller::new(
            operation,
            &configuration.credentials,
            context.state.rest_client()?,
            endpoint.to_owned(),
            PollBudget {
                what: OPERATION,
                ..PollBudget::default()
            },
        );
        // The object's size is whatever the model compiled to, so the request omits the normal
        // whole-request deadline.
        context
            .runtime
            .block_on(Box::pin(poller.request(Delivery::Idempotent, &|| {
                Request::post_media(&url, &content_type, body.clone(), &label)
            })))
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests;
