//! Handle storage, locking, and per-object panic containment.
//!
//! Each `private_data` owns an [`Exported<S>`] reference, and every call takes a reference of its
//! own, so `Release` may race any call on the handle: the call either finds the slot empty and
//! reports `INVALID_STATE`, or keeps the object alive until it returns. A null handle has no slot
//! at all, which every entry point reports as `INVALID_ARGUMENT`. ADBC lets `Cancel` and the
//! option getters run during any other call, and Python's `adbc_driver_manager` does not order
//! `cancel()` with `close()`. Calls serialize on the state mutex; cancellation and progress reads
//! use separate fields so they remain available during execution.

use std::ffi::c_void;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::{catch, missing_argument};
use crate::error::{driver_defect, invalid_state};
use crate::query::ExecutionProgress;

/// Buffer `New`/`SetOption` options until `Init` passes them to the driver's constructor.
pub(crate) enum Staged<K, S> {
    /// Insertion order with aliases deduplicated. Constructors clone these options so a failed
    /// `Init` can be retried after correcting a value.
    Pending(Vec<(K, OptionValue)>),
    Ready(S),
}

/// The state `New` installs. Building it allocates nothing.
impl<K, S> Default for Staged<K, S> {
    fn default() -> Self {
        Self::Pending(Vec::new())
    }
}

impl<K, S> Staged<K, S> {
    /// Require an initialized state.
    pub(crate) fn ready(&mut self, kind: &str) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(uninitialized(kind)),
        }
    }

    /// Require the pre-init state.
    pub(crate) fn pending(&mut self, kind: &str) -> Result<&mut Vec<(K, OptionValue)>> {
        match self {
            Self::Pending(options) => Ok(options),
            Self::Ready(_) => Err(Error::with_message_and_status(
                format!(
                    "{kind} is already initialized; call Init only once, since options set \
                     after it take effect immediately"
                ),
                Status::InvalidState,
            )),
        }
    }
}

/// Shared handle storage. Access uses `&Self` and interior mutability because ADBC permits
/// concurrent cancellation and option getters; a reference to the whole struct must not be mutable.
pub(crate) struct Exported<S> {
    /// Installed during initialization; accessible while execution holds the state lock.
    cancel: Mutex<Option<Box<dyn CancelHandle>>>,
    /// Statement progress, readable during execution. Databases and connections publish none.
    progress: Option<Arc<ExecutionProgress>>,
    /// The driver object. Calls serialize on this lock; [`cancel`] and [`published_progress`]
    /// bypass it.
    state: Mutex<S>,
}

// Raw FFI pointers bypass auto-trait checks at call sites, so assert `Send + Sync` here.
fn _exported_is_shareable<S: Send>(exported: &Exported<S>) -> &(dyn Send + Sync) {
    exported
}

impl<S> Exported<S> {
    /// An object for [`install`]. Statements pass the handles [`cancel`] and
    /// [`published_progress`] read without the state lock; connections install theirs at `Init`.
    pub(crate) fn new(
        state: S,
        cancel: Option<Box<dyn CancelHandle>>,
        progress: Option<Arc<ExecutionProgress>>,
    ) -> Self {
        Self {
            cancel: Mutex::new(cancel),
            progress,
            state: Mutex::new(state),
        }
    }

    /// Take the state lock, refusing a poisoned object.
    ///
    /// A panic in a dispatched call or a construction-time borrow poisons every object whose
    /// state lock the call held.
    fn lock_state(&self, kind: &str) -> Result<MutexGuard<'_, S>> {
        self.state.lock().map_err(|_| poisoned(kind))
    }
}

/// Refuse uninitialized or released handles.
#[cold]
#[inline(never)]
fn uninitialized(kind: &str) -> Error {
    Error::with_message_and_status(
        format!(
            "{kind} is uninitialized or already released; create it with its New function, \
             call its Init if it has one, and do not use it after Release"
        ),
        Status::InvalidState,
    )
}

#[cold]
#[inline(never)]
fn poisoned(kind: &str) -> Error {
    Error::with_message_and_status(
        format!(
            "{kind} is unusable after an earlier panic in the driver; release it, create a \
             new one, and report the panic"
        ),
        Status::Internal,
    )
}

/// Orders every read of a handle's `private_data` with the writes of `New` and `Release`. Readers
/// hold it only while taking their reference, and `Release` only while emptying the slot, so the
/// object a slot names cannot be freed between a reader loading the pointer and counting itself.
static SLOTS: RwLock<()> = RwLock::new(());

/// Install `exported` in an empty slot, as `New` does.
///
/// # Safety
/// `slot` must point to a handle's live, aligned, writable `private_data` field, holding null or a
/// pointer from this function for the same `S`. All concurrent accesses to the field must go
/// through this module's slot-locking functions, without overlapping Rust references to it.
pub(crate) unsafe fn install<S: Send>(
    slot: *mut *mut c_void,
    kind: &str,
    exported: Exported<S>,
) -> Result<()> {
    let _writing = SLOTS.write().unwrap_or_else(PoisonError::into_inner);
    // SAFETY: The caller supplies the live field, and the write lock excludes other accesses.
    unsafe {
        if !slot.read().is_null() {
            return Err(already_populated(kind));
        }
        slot.write(Arc::into_raw(Arc::new(exported)).cast_mut().cast());
    }
    Ok(())
}

/// Whether a handle's slot already holds an object, which `New` refuses to overwrite.
///
/// # Safety
/// `slot` must be null or point to a handle's live, aligned, initialized `private_data` field,
/// with concurrent access synchronized as required by [`install`].
pub(crate) unsafe fn check_empty(slot: *const *mut c_void, kind: &str) -> Result<()> {
    let _reading = SLOTS.read().unwrap_or_else(PoisonError::into_inner);
    // SAFETY: The caller supplies a live field or null, and writers hold the write lock.
    match unsafe { slot.as_ref() } {
        Some(object) if !object.is_null() => Err(already_populated(kind)),
        _ => Ok(()),
    }
}

/// `*New` requires a zero-initialized handle. Overwriting a live slot would leak its object.
#[cold]
#[inline(never)]
fn already_populated(kind: &str) -> Error {
    invalid_state(format!(
        "{kind} already holds a driver object; release it first, or pass a zero-initialized \
         struct"
    ))
}

/// Take a reference to the object in a handle's slot, which keeps it alive however a concurrent
/// `Release` goes.
///
/// # Safety
/// `slot` must be null or point to a handle's live, aligned `private_data` field, holding null or
/// a pointer [`install`] stored for this `S`. Concurrent access must be synchronized as required
/// by [`install`].
unsafe fn acquire<S>(slot: *const *mut c_void, kind: &str) -> Result<Arc<Exported<S>>> {
    let _reading = SLOTS.read().unwrap_or_else(PoisonError::into_inner);
    // SAFETY: The caller supplies a live field or null, and writers hold the write lock.
    let Some(object) = (unsafe { slot.as_ref() }) else {
        return Err(missing_argument(kind));
    };
    let pointer = object.cast_const().cast::<Exported<S>>();
    if pointer.is_null() {
        return Err(uninitialized(kind));
    }
    // SAFETY: A non-null slot holds the handle's own `Arc::into_raw` reference, which `release`
    // gives up only under the write lock, so the count cannot reach zero before this increment.
    unsafe {
        Arc::increment_strong_count(pointer);
        Ok(Arc::from_raw(pointer))
    }
}

/// Run `f` against the object behind `slot`, containing panics and reporting errors through
/// `error`.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract, and `error` [`finish`]'s.
pub(crate) unsafe fn dispatch<S, F>(
    slot: *const *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
    f: F,
) -> AdbcStatusCode
where
    F: FnOnce(&mut S) -> Result<()>,
{
    // Erase the callback before the shared body runs; see [`dispatch_erased`].
    let mut once = Some(f);
    // SAFETY: slot and error keep the caller's provenance and lifetime; the closure is synchronous.
    unsafe {
        dispatch_erased(slot, kind, error, &mut |state| {
            once.take().map_or_else(redispatched, |f| f(state))
        })
    }
}

/// Share handle lookup, locking, panic containment and error export per state type instead of
/// per callback. The borrowed trait object allocates nothing; see BINARY-SIZE.md.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract, and `error` [`finish`]'s.
#[inline(never)]
unsafe fn dispatch_erased<S>(
    slot: *const *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
    f: &mut dyn FnMut(&mut S) -> Result<()>,
) -> AdbcStatusCode {
    // SAFETY: the caller supplies the slot's contract for this call.
    let outcome = catch(|| unsafe { with_locked(slot, kind, f) });
    // SAFETY: the caller supplies a valid optional error output for this call.
    unsafe { finish(outcome.unwrap_or_else(Err), error) }
}

/// Report a broken at-most-once dispatch invariant without panicking.
#[cold]
#[inline(never)]
fn redispatched() -> Result<()> {
    Err(driver_defect(format_args!(
        "the BigQuery driver dispatched one call twice"
    )))
}

/// Run `f` on the locked state behind a handle's slot. Entry points also use it for a second
/// handle besides the one they dispatch on, such as the database `ConnectionInit` reads, inside
/// their own panic containment. The caller must still check readiness.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract.
pub(crate) unsafe fn with_locked<S, R>(
    slot: *const *mut c_void,
    kind: &str,
    f: impl FnOnce(&mut S) -> Result<R>,
) -> Result<R> {
    // SAFETY: the caller supplies the slot's contract.
    let exported = unsafe { acquire::<S>(slot, kind) }?;
    let mut state = exported.lock_state(kind)?;
    f(&mut state)
}

/// Cancel without taking the state lock, as ADBC requires during execution. Cancellation remains
/// available after a panic so callers can stop work left running by a poisoned handle.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract, and `error` [`finish`]'s.
pub(crate) unsafe fn cancel<S>(
    slot: *const *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let outcome = catch(|| {
        // SAFETY: the caller supplies the slot's contract.
        let exported = unsafe { acquire::<S>(slot, kind) }?;
        let handle = exported
            .cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match handle.as_ref() {
            Some(handle) => handle.try_cancel(),
            None => Err(uninitialized(kind)),
        }
    });
    // SAFETY: the caller supplies a valid optional error output for this call.
    unsafe { finish(outcome.unwrap_or_else(Err), error) }
}

/// Read atomic progress without taking the state lock, including after a panic.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract.
pub(crate) unsafe fn published_progress<S>(
    slot: *const *mut c_void,
    kind: &str,
    read: fn(&ExecutionProgress) -> f64,
) -> Result<f64> {
    // SAFETY: the caller supplies the slot's contract.
    let exported = unsafe { acquire::<S>(slot, kind) }?;
    match exported.progress.as_ref() {
        Some(progress) => Ok(read(progress)),
        None => Err(uninitialized(kind)),
    }
}

/// Install cancellation before publishing an initialized state, so execution cannot become
/// available before cancellation does. Statements install it at construction instead.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract.
pub(crate) unsafe fn install_cancel_handle<S>(
    slot: *const *mut c_void,
    kind: &str,
    handle: Box<dyn CancelHandle>,
) -> Result<()> {
    // SAFETY: the caller supplies the slot's contract.
    let exported = unsafe { acquire::<S>(slot, kind) }?;
    *exported
        .cancel
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(handle);
    Ok(())
}

/// Empty the handle's slot and drop its reference, so a second release is an error rather than a
/// double free. A call in flight holds its own reference, and the last one to finish frees the
/// object.
///
/// # Safety
/// `slot` must satisfy [`acquire`]'s contract and permit writes under the slot lock; `error` must
/// satisfy [`finish`]'s contract.
pub(crate) unsafe fn release<S>(
    slot: *mut *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let taken = {
        let _writing = SLOTS.write().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: The caller supplies a live field or null, and the write lock excludes other
        // accesses.
        unsafe { slot.as_mut() }.map(|object| std::mem::replace(object, std::ptr::null_mut()))
    };
    let taken = match taken {
        Some(taken) if !taken.is_null() => taken,
        // SAFETY: the caller supplies a valid optional error output for this call.
        Some(_) => return unsafe { finish(Err(uninitialized(kind)), error) },
        // SAFETY: as above.
        None => return unsafe { finish(Err(missing_argument(kind)), error) },
    };
    // Dropping driver state can run arbitrary code (closing clients, joining tasks), so it is
    // contained like any other call.
    let outcome = catch(|| {
        // SAFETY: taken is the handle's Arc::into_raw reference for S, which emptying the slot
        // transferred to this call exactly once.
        drop(unsafe { Arc::from_raw(taken.cast_const().cast::<Exported<S>>()) });
        Ok(())
    });
    // SAFETY: the caller supplies a valid optional error output for this call.
    unsafe { finish(outcome.unwrap_or_else(Err), error) }
}

/// Convert a driver result into a status code, exporting the error when there is one.
///
/// # Safety
/// `error` must be null or an exclusively accessible error output satisfying [`export_error`].
pub(crate) unsafe fn finish(result: Result<()>, error: *mut AdbcError) -> AdbcStatusCode {
    match result {
        Ok(()) => ADBC_STATUS_OK,
        Err(failure) => {
            let status = AdbcStatusCode::from(failure.status);
            // Exporting allocates and may run a stale caller release callback, so contain it.
            // After a caught panic the caller still gets the status, with the error unchanged.
            let contained = catch(|| {
                // SAFETY: finish's caller guarantees the error allocation, revision and ownership.
                unsafe { export_error(error, failure) };
                Ok(())
            });
            drop(contained);
            status
        }
    }
}

#[cfg(test)]
mod tests;
