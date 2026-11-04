//! dbt-core 2.0's option-only Cloud Storage object write.
//!
//! A dbt python model uploads its compiled source here before the Dataproc or Vertex AI job that
//! runs it is submitted. See `DBT-CORE.md`.

use adbc_core::error::Result;

use super::text_slots;
use crate::BigQueryStatementOption;
use crate::cancellation::Operation;
use crate::query::ExecutionContext;
use crate::rest::jobs::next_identity;
use crate::rest::json::encode_json;
use crate::rest::rejection::service_error;
use crate::rest::request::{Request, multipart_related, send};
use crate::rest::url::query_component;

/// The operation's name in error messages.
const OPERATION: &str = "Cloud Storage object write";

/// A Cloud Storage object write configured on an ADBC statement by dbt-core 2.0.
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
    /// Whether any part of the write is configured.
    pub(crate) fn is_active(&self) -> bool {
        self.any_text_option_set()
    }

    /// Uploads the configured content.
    ///
    /// Unlike the fork, both the bucket and the object name are required: the object is what the
    /// job submitted next reads, so writing to the wrong key silently runs stale code.
    pub(crate) fn execute(&self, context: &ExecutionContext, operation: &Operation) -> Result<()> {
        let bucket = self.required(OPERATION, BigQueryStatementOption::WriteGcsBucket)?;
        let object_name = self.required(OPERATION, BigQueryStatementOption::WriteGcsObjectName)?;
        let content = self.content.as_deref().unwrap_or_default();

        let configuration = context.state.configuration(&context.runtime)?;
        let endpoint = configuration.endpoints.gcs();
        // One multipart insert, which is what the fork's Go client sends below its 16 MiB chunk
        // size, with that client's `projection=full` and compact response.
        let url = format!(
            "{endpoint}/upload/storage/v1/b/{}/o?uploadType=multipart&name={}\
             &projection=full&alt=json&prettyPrint=false",
            query_component(bucket),
            query_component(object_name),
        );
        let metadata = encode_json(
            &serde_json::json!({"bucket": bucket, "name": object_name}),
            "the Cloud Storage object metadata",
        )?;
        // `text/plain` is what the Go client's content sniffer settles on for both a compiled
        // model and a notebook document.
        let boundary = next_identity("adbc_gcs_");
        let body = multipart_related(
            &boundary,
            &metadata,
            content.as_bytes(),
            "text/plain; charset=utf-8",
        );
        let content_type = format!("multipart/related; boundary={boundary}");
        // The object's size is whatever the model compiled to, so the request omits the normal
        // whole-request deadline.
        let http = context.state.rest_client()?;
        let context_label = format!("write Cloud Storage object gs://{bucket}/{object_name}");

        let (status, response) = context.runtime.block_on(Box::pin(send(
            Some(operation),
            &configuration.credentials,
            &http,
            Request::post_media(&url, &content_type, body, &context_label),
        )))?;
        if status.is_success() {
            Ok(())
        } else {
            Err(service_error(
                &format!("failed to {context_label}"),
                status,
                &response,
            ))
        }
    }
}

#[cfg(test)]
mod tests;
