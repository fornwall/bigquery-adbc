//! `prepare_table`: what each ingest mode does to the table before any row is written.

use crate::rest::model::TableSchema;
use adbc_core::CancelHandle;
use adbc_core::error::Status;
use adbc_core::options::IngestMode;

use crate::cancellation::{Cancellation, OperationCancelHandle};
use crate::clients::ClientState;
use crate::config::ServiceEndpointKind;
use crate::identifiers::ProjectIdentifier;
use crate::ingest::IngestContext;
use crate::ingest::tests::plan_and_prepare;
use crate::rest::metadata::TableId;
use crate::test_support::http::capture_requests;

/// Cancel has to reach the table setup an ingest runs before its first append, not just
/// the appends themselves. `get_table` and `insert_table` carry the operation, and so does
/// the DDL a `replace` runs, so a cancelled ingest stops without issuing any of them.
#[test]
fn cancelling_an_ingest_stops_its_table_setup_requests() {
    for mode in [IngestMode::Create, IngestMode::Append, IngestMode::Replace] {
        let (endpoint, captured) = capture_requests(Vec::new());
        let runtime = crate::runtime::new_runtime().unwrap();
        let clients = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
        let cancellation = Cancellation::new(runtime.clone(), clients.clone(), "statement");
        let operation = cancellation.begin();
        let billing_project = ProjectIdentifier::try_new("billing-project").unwrap();
        let context = IngestContext {
            runtime: &runtime,
            state: &clients,
            billing_project_id: Some(&billing_project),
            current_catalog: None,
            current_schema: None,
            location: None,
            operation: &operation,
        };

        OperationCancelHandle(cancellation.clone())
            .try_cancel()
            .unwrap();
        let failure = plan_and_prepare(
            &context,
            TableId {
                project_id: "project",
                dataset_id: "dataset",
                table_id: "events",
            },
            mode,
            &TableSchema::default(),
        )
        .unwrap_err();
        operation.finish();

        assert_eq!(failure.status, Status::Cancelled, "{mode:?}: {failure}");
        assert!(
            captured.join().unwrap().is_empty(),
            "a cancelled {mode:?} ingest still sent a table setup request"
        );
    }
}
