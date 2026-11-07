use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

use super::*;
use crate::ffi::abi::{
    ADBC_STATUS_INTERNAL, ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE,
    ADBC_STATUS_NOT_FOUND,
};
use crate::ffi::test_support::{RecordingCancel, hold_the_dispatch_turn, null};
use adbc_core::options::OptionDatabase;

/// A handle's `private_data` field holding a fresh object, boxed so that threads can share its
/// address.
fn installed<S: Send>(state: S, cancel: Option<Box<dyn CancelHandle>>) -> Box<*mut c_void> {
    let mut slot = Box::new(std::ptr::null_mut());
    // SAFETY: The boxed field is live and empty.
    unsafe {
        install(
            &raw mut *slot,
            "statement",
            Exported::new(state, cancel, None),
        )
    }
    .unwrap();
    slot
}

/// The address of a boxed slot, which a spawned thread can take.
fn exposed(slot: &mut Box<*mut c_void>) -> usize {
    (&raw mut **slot).expose_provenance()
}

/// The slot whose address [`exposed`] returned.
fn at(address: usize) -> *mut *mut c_void {
    std::ptr::with_exposed_provenance_mut(address)
}

fn run(slot: &mut Box<*mut c_void>, f: impl FnOnce(&mut u32) -> Result<()>) -> AdbcStatusCode {
    // SAFETY: Test slots passed here are installed with a `u32` state, or empty.
    unsafe { dispatch::<u32, _>(&raw const **slot, "statement", null(), f) }
}

fn cancel_slot(slot: &mut Box<*mut c_void>) -> AdbcStatusCode {
    // SAFETY: As in `run`.
    unsafe { cancel::<u32>(&raw const **slot, "statement", null()) }
}

fn release_slot(slot: &mut Box<*mut c_void>) -> AdbcStatusCode {
    // SAFETY: As in `run`.
    unsafe { release::<u32>(&raw mut **slot, "statement", null()) }
}

#[test]
fn a_pending_object_is_not_ready() {
    let mut staged: Staged<OptionDatabase, u8> = Staged::default();
    let error = staged.ready("database").unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    staged = Staged::Ready(3);
    assert_eq!(*staged.ready("database").unwrap(), 3);
}

#[test]
fn a_panicking_call_poisons_only_that_object() {
    let mut first = installed(0_u32, None);
    let mut second = installed(0_u32, None);
    assert_eq!(
        run(&mut first, |_| panic!("inside the driver")),
        ADBC_STATUS_INTERNAL
    );
    // The panicking object refuses further work, while an unrelated object is unaffected.
    let set = |state: &mut u32| {
        *state = 1;
        Ok(())
    };
    assert_eq!(run(&mut first, set), ADBC_STATUS_INTERNAL);
    assert_eq!(run(&mut second, set), ADBC_STATUS_OK);
    assert_eq!(release_slot(&mut first), ADBC_STATUS_OK);
    assert_eq!(release_slot(&mut second), ADBC_STATUS_OK);
}

#[test]
fn a_panic_while_borrowing_another_object_poisons_both() {
    let mut connection = installed(0_u32, None);
    let mut database = installed(0_u32, None);
    let database_slot = &raw const *database;
    let status = run(&mut connection, |_| {
        // SAFETY: The database slot is live and installed with a `u32` state.
        unsafe {
            with_locked::<u32, ()>(database_slot, "database", |state| {
                *state = 1;
                panic!("while constructing a connection");
            })
        }
    });
    assert_eq!(status, ADBC_STATUS_INTERNAL);

    for slot in [&mut connection, &mut database] {
        assert_eq!(run(slot, |_| Ok(())), ADBC_STATUS_INTERNAL);
        // SAFETY: As in `run`.
        let refused = unsafe { with_locked::<u32, ()>(&raw const **slot, "x", |_| Ok(())) };
        assert_eq!(refused.unwrap_err().status, Status::Internal);
        assert_eq!(release_slot(slot), ADBC_STATUS_OK);
    }
}

/// A cancel handle that parks inside `try_cancel` until the test releases it, so a `Release` is
/// sure to land while a cancel is running.
struct BlockingCancel {
    entered: Sender<()>,
    resume: StdMutex<Receiver<()>>,
}

impl CancelHandle for BlockingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.entered.send(()).expect("test channel");
        self.resume
            .lock()
            .expect("test mutex")
            .recv()
            .expect("test channel");
        Ok(())
    }
}

/// `Cancel` holds its own reference, so a `Release` that lands mid-cancel cannot free the handle
/// it is running.
#[test]
fn a_release_during_a_cancel_does_not_free_the_object_under_it() {
    let (entered_sender, entered_receiver) = channel::<()>();
    let (resume_sender, resume_receiver) = channel::<()>();
    let mut slot = installed(
        0_u32,
        Some(Box::new(BlockingCancel {
            entered: entered_sender,
            resume: StdMutex::new(resume_receiver),
        })),
    );

    let address = exposed(&mut slot);
    // SAFETY: The parent keeps the boxed slot alive until this thread joins.
    let canceller = std::thread::spawn(move || unsafe { cancel::<u32>(at(address), "s", null()) });
    // The cancel is now inside `try_cancel`.
    entered_receiver.recv().expect("test channel");
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
    assert!(slot.is_null());

    // The in-flight cancel still owns a live object; finishing it frees the object.
    resume_sender.send(()).expect("test channel");
    assert_eq!(canceller.join().expect("cancel thread"), ADBC_STATUS_OK);
    // The slot is empty, so a later call reports an error instead of reaching the freed object.
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_INVALID_STATE);
}

/// A state that records when it is dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// adbc.h lets the option getters run during any other call, `Release` included, so a release
/// that lands while a call holds the state lock must leave the object to that call, which frees
/// it on return.
#[test]
fn a_release_during_a_call_leaves_the_object_to_the_call() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut slot = installed(DropFlag(dropped.clone()), None);
    let (entered_sender, entered_receiver) = channel::<()>();
    let (resume_sender, resume_receiver) = channel::<()>();

    let address = exposed(&mut slot);
    // SAFETY: The parent keeps the boxed slot alive until this thread joins.
    let caller = std::thread::spawn(move || unsafe {
        dispatch::<DropFlag, _>(at(address), "statement", null(), |state| {
            entered_sender.send(()).expect("test channel");
            resume_receiver.recv().expect("test channel");
            assert!(
                !state.0.load(Ordering::SeqCst),
                "the state was dropped under the call"
            );
            Ok(())
        })
    });
    entered_receiver.recv().expect("test channel");

    // SAFETY: The slot was installed with a `DropFlag` state.
    let status = unsafe { release::<DropFlag>(&raw mut *slot, "statement", null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(slot.is_null());
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the call in flight still holds the object"
    );

    resume_sender.send(()).expect("test channel");
    assert_eq!(caller.join().expect("calling thread"), ADBC_STATUS_OK);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the last reference frees the object"
    );
}

/// Threads reading the slot while another empties it are ordered with that write, whichever
/// wins. Under Miri this also checks that no access to the slot races.
#[test]
fn a_release_racing_cancels_and_calls_is_ordered_with_them() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut slot = installed(0_u32, Some(Box::new(RecordingCancel(cancelled))));
    let address = exposed(&mut slot);
    let readers: Vec<_> = (0..2)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..3 {
                    // SAFETY: The parent keeps the boxed slot alive until this thread joins.
                    let statuses = unsafe {
                        [
                            cancel::<u32>(at(address), "statement", null()),
                            dispatch::<u32, _>(at(address), "statement", null(), |_| Ok(())),
                        ]
                    };
                    for status in statuses {
                        assert!(
                            [ADBC_STATUS_OK, ADBC_STATUS_INVALID_STATE].contains(&status),
                            "{status}"
                        );
                    }
                }
            })
        })
        .collect();
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
    for reader in readers {
        reader.join().unwrap();
    }
}

#[test]
fn returning_an_error_does_not_poison() {
    let mut slot = installed(0_u32, None);
    let status = run(&mut slot, |_| {
        Err(Error::with_message_and_status("nope", Status::NotFound))
    });
    assert_eq!(status, ADBC_STATUS_NOT_FOUND);
    let status = run(&mut slot, |state| {
        *state = 5;
        Ok(())
    });
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

#[test]
fn a_released_handle_is_an_invalid_state_and_a_null_one_an_invalid_argument() {
    let mut slot = installed(0_u32, None);
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
    assert!(slot.is_null());
    // A second release must report, not double free.
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_INVALID_STATE);
    assert_eq!(run(&mut slot, |_| Ok(())), ADBC_STATUS_INVALID_STATE);
    // A null handle has no slot at all.
    // SAFETY: A null slot pointer is accepted and reported as a bad argument.
    let statuses = unsafe {
        [
            dispatch::<u32, _>(null(), "statement", null(), |_| Ok(())),
            cancel::<u32>(null(), "statement", null()),
            release::<u32>(null(), "statement", null()),
        ]
    };
    assert_eq!(statuses, [ADBC_STATUS_INVALID_ARGUMENT; 3]);
}

/// `New` refuses a populated slot rather than leak the object in it.
#[test]
fn installing_into_a_populated_slot_is_refused() {
    let mut slot = installed(0_u32, None);
    let before = *slot;
    // SAFETY: The boxed slot is live and holds a `u32` object.
    let (checked, installed) = unsafe {
        (
            check_empty(&raw const *slot, "statement"),
            install(
                &raw mut *slot,
                "statement",
                Exported::new(1_u32, None, None),
            ),
        )
    };
    for refused in [checked, installed] {
        assert_eq!(refused.unwrap_err().status, Status::InvalidState);
    }
    assert_eq!(*slot, before);
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

/// The primary Cancel scenario from adbc.h: one thread blocks inside an operation while another
/// cancels it. Cancel must not wait for the dispatch turn or misreport the busy object.
#[test]
fn cancel_does_not_wait_for_a_held_dispatch_turn() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut slot = installed(0_u32, Some(Box::new(RecordingCancel(cancelled.clone()))));
    let release_turn = hold_the_dispatch_turn::<u32>(&raw const *slot);

    // The dispatch turn is held; cancel must still complete immediately.
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));

    release_turn();
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

/// Overlapping ordinary calls wait for each other on the state lock, rather than racing or
/// reporting a busy handle.
#[test]
fn concurrent_dispatches_serialize_instead_of_racing() {
    const THREADS: usize = 8;
    const CALLS: u32 = 100;

    let mut slot = installed(0_u32, None);
    let address = exposed(&mut slot);
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..CALLS {
                    // SAFETY: The parent keeps the boxed slot alive until this thread joins.
                    let status = unsafe {
                        dispatch::<u32, _>(at(address), "statement", null(), |state| {
                            *state += 1;
                            Ok(())
                        })
                    };
                    assert_eq!(status, ADBC_STATUS_OK);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    let mut total = 0;
    let status = run(&mut slot, |state| {
        total = *state;
        Ok(())
    });
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(total, u32::try_from(THREADS).unwrap() * CALLS);
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

#[test]
fn cancel_still_works_after_a_panic_poisoned_the_object() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut slot = installed(0_u32, Some(Box::new(RecordingCancel(cancelled.clone()))));
    assert_eq!(
        run(&mut slot, |_| panic!("inside the driver")),
        ADBC_STATUS_INTERNAL
    );
    // Dispatches are refused, but cancelling what the panicked call left behind still works.
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

#[test]
fn cancel_without_an_installed_handle_is_an_invalid_state() {
    // A live object whose init has not installed a handle yet reports as uninitialized.
    let mut slot = installed(0_u32, None);
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_INVALID_STATE);

    let cancelled = Arc::new(AtomicBool::new(false));
    let handle = Box::new(RecordingCancel(cancelled.clone()));
    // SAFETY: As in `run`.
    unsafe { install_cancel_handle::<u32>(&raw const *slot, "connection", handle) }.unwrap();
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}
