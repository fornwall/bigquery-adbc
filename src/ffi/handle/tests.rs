use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

use super::*;
use crate::ffi::abi::{ADBC_STATUS_INTERNAL, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND};
use crate::ffi::test_support::{RecordingCancel, call, hold_the_dispatch_turn, null};
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
    let handle: Box<dyn CancelHandle> = Box::new(RecordingCancel(cancelled.clone()));
    // SAFETY: As in `run`.
    let status =
        unsafe { dispatch_init::<u32>(&raw const *slot, "connection", null(), |_| Ok(handle)) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert_eq!(cancel_slot(&mut slot), ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));
    assert_eq!(release_slot(&mut slot), ADBC_STATUS_OK);
}

/// `fork` leaves a child no runtime workers. Using the object is the caller's mistake, so it is
/// refused as one: no panic, nothing poisoned, and `Release` still empties the slot. It does not
/// drop the object, whose destructors could wait on a lock held by a thread the child lacks.
#[test]
fn an_object_inherited_across_fork_is_refused_and_released_without_dropping_it() {
    let (cancelled, dropped) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let mut slot = Box::new(std::ptr::null_mut());
    let inherited = Exported {
        process: 0,
        ..Exported::new(
            DropFlag(Arc::clone(&dropped)),
            Some(Box::new(RecordingCancel(cancelled.clone()))),
            None,
        )
    };
    // SAFETY: The boxed field is live and empty.
    unsafe { install(&raw mut *slot, "statement", inherited) }.unwrap();

    for _ in 0..2 {
        let (status, message) = call(|error| {
            // SAFETY: The slot holds a `DropFlag` object and the error output is fresh.
            unsafe { dispatch::<DropFlag, _>(&raw const *slot, "statement", error, |_| Ok(())) }
        });
        assert_eq!(status, ADBC_STATUS_INVALID_STATE, "{message:?}");
        assert_eq!(message.as_deref(), Some(INHERITED_ACROSS_FORK));
    }
    // SAFETY: As above; cancellation and release take no state lock.
    unsafe {
        assert_eq!(
            cancel::<DropFlag>(&raw const *slot, "statement", null()),
            ADBC_STATUS_INVALID_STATE
        );
        assert_eq!(
            release::<DropFlag>(&raw mut *slot, "statement", null()),
            ADBC_STATUS_OK
        );
    }
    assert!(!cancelled.load(Ordering::SeqCst));
    assert!(slot.is_null(), "Release empties the slot");
    assert!(
        !dropped.load(Ordering::SeqCst),
        "a fork child ran the inherited object's destructors"
    );
}

/// Every exported call checks for `fork` against a cached process ID, so the `pthread_atfork`
/// handler must refresh it before a child can run driver code, or the child would take its
/// parent's handles for its own.
#[test]
#[cfg(unix)]
#[cfg_attr(miri, ignore = "Miri cannot fork")]
fn a_fork_child_sees_its_own_process_id() {
    track_forks();
    assert_eq!(process_id(), std::process::id());
    // SAFETY: The child makes only async-signal-safe calls (an atomic load, `getpid` and `_exit`),
    // which is all a child of this multithreaded test process may do.
    let child = unsafe { libc::fork() };
    if child == 0 {
        let refreshed = process_id() == std::process::id();
        // SAFETY: `_exit` ends the child without running exit handlers or the parent's
        // destructors.
        unsafe { libc::_exit(if refreshed { 0 } else { 1 }) };
    }
    assert!(child > 0, "fork failed");
    let mut status = 0;
    // SAFETY: `child` is this process's own unreaped child, and `status` a writable local.
    assert_eq!(unsafe { libc::waitpid(child, &raw mut status, 0) }, child);
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "the child kept its parent's process ID (wait status {status})"
    );
}

/// A child must not inherit the slot lock from a thread it lacks: every later call, even on a new
/// handle, would wait on it forever. Threads here hold it, shared and exclusive, most of the time,
/// so without the child handler nearly every child would.
#[test]
#[cfg(unix)]
#[cfg_attr(miri, ignore = "Miri cannot fork")]
fn a_fork_child_never_inherits_the_slot_lock() {
    track_forks();
    let stop = Arc::new(AtomicBool::new(false));
    let holders = [false, true].map(|exclusive| {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let held = if exclusive {
                    SLOTS.write()
                } else {
                    SLOTS.read()
                };
                for _ in 0..1_000 {
                    std::hint::spin_loop();
                }
                drop(held);
                std::thread::yield_now();
            }
        })
    });
    for _ in 0..20 {
        // SAFETY: The child makes only async-signal-safe calls (an atomic load and `_exit`), which
        // is all a child of this multithreaded test process may do.
        let child = unsafe { libc::fork() };
        if child == 0 {
            let free = SLOTS.0.load(Ordering::Relaxed) == 0;
            // SAFETY: `_exit` ends the child without running exit handlers or the parent's
            // destructors.
            unsafe { libc::_exit(i32::from(!free)) };
        }
        assert!(child > 0, "fork failed");
        let mut status = 0;
        // SAFETY: `child` is this process's own unreaped child, and `status` a writable local.
        assert_eq!(unsafe { libc::waitpid(child, &raw mut status, 0) }, child);
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "the child inherited the slot lock held (wait status {status})"
        );
    }
    stop.store(true, Ordering::Relaxed);
    for holder in holders {
        holder.join().unwrap();
    }
}

/// The slot lock is hand-written, so Miri's race detector checks its exclusion here: writers
/// exclude each other and readers, and readers see every completed write.
#[test]
fn the_slot_lock_excludes_writers_from_readers_and_each_other() {
    struct Guarded(std::cell::UnsafeCell<u64>);
    // SAFETY: Every access below holds `lock`, shared to read and exclusive to write.
    unsafe impl Sync for Guarded {}
    impl Guarded {
        // A method, so that closures capture the whole `Sync` wrapper rather than its field.
        fn cell(&self) -> *mut u64 {
            self.0.get()
        }
    }

    let lock = SlotLock::new();
    let value = Guarded(std::cell::UnsafeCell::new(0));
    let rounds = if cfg!(miri) { 20 } else { 10_000 };
    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                for _ in 0..rounds {
                    let _writing = lock.write();
                    // SAFETY: The write lock excludes every other access.
                    unsafe { *value.cell() += 1 };
                }
            });
            scope.spawn(|| {
                let mut last = 0;
                for _ in 0..rounds {
                    let _reading = lock.read();
                    // SAFETY: The read lock excludes writers.
                    let seen = unsafe { *value.cell() };
                    assert!(last <= seen && seen <= 2 * rounds, "{last} then {seen}");
                    last = seen;
                }
            });
        }
    });
    assert_eq!(value.0.into_inner(), 2 * rounds);
    assert_eq!(lock.0.into_inner(), 0, "every hold was released");
}
