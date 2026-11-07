//! `AdbcDatabase*` entry points.

use std::sync::OnceLock;

use adbc_core::Driver;
use adbc_core::error::Result;
use adbc_core::options::OptionDatabase;

use super::abi::{AdbcDatabase, AdbcError, AdbcStatusCode};
use super::handle::Staged;
use super::options::{FfiHandle, with_state};
use crate::driver::{BigQueryDatabase, BigQueryDriver};

pub(super) const KIND: &str = "database";

pub(super) type State = Staged<OptionDatabase, BigQueryDatabase>;

// SAFETY: `AdbcDatabase` is repr(C) with `private_data` first, and only `State` is installed there.
unsafe impl FfiHandle for AdbcDatabase {
    type State = State;
    const KIND: &'static str = KIND;
}

/// One driver, and so one Tokio runtime, shared by every database in the process, except that
/// each database a `fork` child opens starts a runtime of its own, the inherited one having no
/// workers. Runtime build failure is reported as `ADBC_STATUS_INTERNAL`.
///
/// The runtime lives for the process, so the shared object must not be unloaded; see
/// [`super::AdbcDriverBigqueryInit`]. `AdbcDriver.release` cannot tear it down, since nothing
/// tracks the handles and exported streams still using it.
fn shared_driver() -> Result<&'static BigQueryDriver> {
    static DRIVER: OnceLock<BigQueryDriver> = OnceLock::new();

    if let Some(driver) = DRIVER.get() {
        return Ok(driver);
    }
    // `get_or_init` cannot report failure, so build first; a lost race drops an idle runtime.
    let driver = BigQueryDriver::try_new()?;
    Ok(DRIVER.get_or_init(|| driver))
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn database_init(
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(database, error, |state| {
            let options = state.pending(KIND)?;
            // `new_database_with_opts` owns the ordering rules (URI first, explicit options
            // override it). It takes `&mut self`, so it runs on a clone of the shared driver.
            let database = shared_driver()?
                .clone()
                .new_database_with_opts(options.clone())?;
            *state = State::Ready(database);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
