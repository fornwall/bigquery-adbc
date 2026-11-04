//! Where a query publishes its execution progress for the statement to report.

use std::sync::atomic::{AtomicU64, Ordering};

/// Where a statement publishes `adbc.statement.exec.progress` and `.max_progress`.
///
/// A separate cell because adbc.h requires option getters to be thread-safe while another call
/// holds the statement: the export layer reads its own `Arc` clone without waiting for the
/// execute. Values are `f64` bits; a maximum of 0.0 means unknown, as in adbc.h. BigQuery may
/// report processed bytes before the terminal total exists.
///
/// An execute that runs one job per bound parameter row reports the sum over those jobs.
#[derive(Default)]
pub(crate) struct ExecutionProgress {
    current: AtomicU64,
    maximum: AtomicU64,
    /// What the execute's earlier jobs processed, which the running job's reports add to.
    earlier: AtomicU64,
}

impl ExecutionProgress {
    pub(crate) fn current(&self) -> f64 {
        f64::from_bits(self.current.load(Ordering::Acquire))
    }

    pub(crate) fn maximum(&self) -> f64 {
        f64::from_bits(self.maximum.load(Ordering::Acquire))
    }

    /// Starts a new execute.
    pub(crate) fn reset(&self) {
        self.earlier.store(0.0_f64.to_bits(), Ordering::Release);
        self.maximum.store(0.0_f64.to_bits(), Ordering::Release);
        self.current.store(0.0_f64.to_bits(), Ordering::Release);
    }

    /// Starts one job of the execute: what its earlier jobs processed stays counted, and the
    /// maximum is unknown until this one completes.
    pub(crate) fn begin_job(&self) {
        self.maximum.store(0.0_f64.to_bits(), Ordering::Release);
        self.earlier
            .store(self.current.load(Ordering::Acquire), Ordering::Release);
    }

    /// Discards what the running job reported, before its query is reissued.
    pub(crate) fn restart_job(&self) {
        self.maximum.store(0.0_f64.to_bits(), Ordering::Release);
        self.current
            .store(self.earlier.load(Ordering::Acquire), Ordering::Release);
    }

    pub(crate) fn publish_current(&self, value: i64) {
        // A nonnegative `f64`'s bits order exactly as its value does.
        if value >= 0 {
            self.current
                .fetch_max(self.with_earlier(value).to_bits(), Ordering::AcqRel);
        }
    }

    /// Sets current and maximum to a terminal response's nonnegative total; without one, the
    /// maximum stays unknown.
    pub(crate) fn complete(&self, value: Option<i64>) {
        let Some(value) = value.filter(|value| *value >= 0) else {
            return;
        };
        let bits = self.with_earlier(value).to_bits();
        self.current.store(bits, Ordering::Release);
        self.maximum.store(bits, Ordering::Release);
    }

    fn with_earlier(&self, value: i64) -> f64 {
        f64::from_bits(self.earlier.load(Ordering::Acquire)) + value as f64
    }

    #[cfg(all(test, feature = "ffi"))]
    pub(crate) fn publish_for_test(&self, current: f64, maximum: f64) {
        self.current.store(current.to_bits(), Ordering::Release);
        self.maximum.store(maximum.to_bits(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutionProgress;

    /// A reissued job's discarded attempt is not counted, but the jobs before it still are.
    #[test]
    fn a_reissue_discards_only_the_running_job() {
        let progress = ExecutionProgress::default();
        progress.begin_job();
        progress.complete(Some(100));
        progress.begin_job();
        assert_eq!((progress.current(), progress.maximum()), (100.0, 0.0));
        progress.publish_current(30);
        assert_eq!(progress.current(), 130.0);
        progress.restart_job();
        assert_eq!((progress.current(), progress.maximum()), (100.0, 0.0));
        progress.complete(Some(20));
        assert_eq!((progress.current(), progress.maximum()), (120.0, 120.0));
        progress.reset();
        assert_eq!((progress.current(), progress.maximum()), (0.0, 0.0));
    }
}
