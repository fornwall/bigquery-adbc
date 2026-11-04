//! Ownership of bound Arrow data across statement executions.

use adbc_core::error::Result;
use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;

use crate::error::invalid_state;

#[derive(Default)]
pub(super) enum ParameterBinding {
    #[default]
    Unbound,
    Bound(Box<dyn RecordBatchReader + Send>),
    Consumed,
}

impl ParameterBinding {
    /// Describing or dry-running a statement borrows only the schema and keeps the binding.
    pub(super) fn schema(&self) -> Option<SchemaRef> {
        match self {
            Self::Bound(reader) => Some(reader.schema()),
            Self::Unbound | Self::Consumed => None,
        }
    }

    /// Taking a binding also records that it cannot be executed again, including after failure.
    pub(super) fn take(&mut self) -> Option<Box<dyn RecordBatchReader + Send>> {
        match std::mem::replace(self, Self::Consumed) {
            Self::Bound(reader) => Some(reader),
            state => {
                *self = state;
                None
            }
        }
    }

    pub(super) fn require_available(&self) -> Result<()> {
        if matches!(self, Self::Consumed) {
            return Err(invalid_state(
                "BigQuery parameters were consumed by the previous execute; bind them again with AdbcStatementBind or AdbcStatementBindStream before executing again",
            ));
        }
        Ok(())
    }

    /// New SQL can run without a previous query's consumed binding, but keeps unread data.
    pub(super) fn reset_consumed(&mut self) {
        if matches!(self, Self::Consumed) {
            *self = Self::Unbound;
        }
    }
}

impl std::fmt::Debug for ParameterBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unbound => "Unbound",
            Self::Bound(_) => "Bound",
            Self::Consumed => "Consumed",
        })
    }
}
