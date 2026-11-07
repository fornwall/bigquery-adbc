//! Handle lifecycle and typed option entry points shared by every object.
//!
//! [`FfiHandle`] associates ABI structs with their state; [`OptionTarget`] handles option access
//! before and after initialization. The entry points are generic over the handle, and the vtable
//! instantiates them per object, so a slot cannot hold another object's entry point or one for
//! another value type.

use std::ffi::{c_char, c_void};

use adbc_core::Optionable;
use adbc_core::error::{Error, Result};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use arrow_array::RecordBatchReader;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use super::abi::{AdbcError, AdbcStatusCode};
use super::guard::{
    byte_slice, required_output, required_str, write_bytes, write_out, write_string,
};
use super::handle::{Exported, Staged, dispatch, finish, install, with_locked};
use super::stream::export_reader;
use crate::connection::{connection_option_kind, connection_set_refusal};
use crate::driver::database_option_kind;
use crate::error::{not_found, option_get_refusal, option_set_refusal};
use crate::options::{OptionKind, expected_type};
use crate::{BigQueryConnectionOption, BigQueryDatabaseOption};

/// Access to the driver object behind an exported ABI struct.
///
/// # Safety
/// `Self` must be `repr(C)` with `private_data` as its first field, as [`slot_of`] assumes, and
/// that field's driver allocation must use exactly `State`.
pub(super) unsafe trait FfiHandle {
    /// What lives behind `private_data`.
    type State: OptionTarget + Send;
    /// The object's name in diagnostics: "database", "connection" or "statement".
    const KIND: &'static str;
}

/// How options reach the state behind a handle. A [`Staged`] object buffers sets until `Init` and
/// refuses gets before it; a statement is live from the moment it exists and does neither.
pub(super) trait OptionTarget {
    /// The typed option key, parsed from the caller's C string.
    type Key: for<'a> From<&'a str>;
    /// The live object the typed getters read from.
    type Options: Optionable<Option = Self::Key>;

    /// Apply an option, buffering it when the object has a pre-init phase it is still in.
    fn set(&mut self, key: Self::Key, value: OptionValue) -> Result<()>;

    /// The live state the typed getters read `key` from, or `NOT_FOUND` (the status adbc.h gives
    /// those getters) before that state exists.
    fn options(&mut self, kind: &str, key: &Self::Key) -> Result<&mut Self::Options>;
}

/// Pre-init option classification. Unknown keys and wrong types fail at `SetOption`;
/// value validation waits until `Init` applies the URI and explicit overrides in order.
pub(super) trait RecognizedKey: AsRef<str> {
    /// The object's name in diagnostics: "database" or "connection".
    const SCOPE: &'static str;

    /// Identity for buffered options, including aliases. Keep the original key for diagnostics.
    fn canonical_name(&self) -> &str;

    /// The value type this key takes, or `None` for a key the driver does not know.
    fn kind(&self) -> Option<OptionKind>;

    /// The live setter's refusal for a key the driver knows but never writes, so a pre-`Init` set
    /// is refused in the same words.
    fn set_refusal(&self) -> Option<Error> {
        None
    }

    /// Whether a pre-`Init` set of this key with this value may be buffered.
    fn check_pre_init_set(&self, value: &OptionValue) -> Result<()> {
        if let Some(refusal) = self.set_refusal() {
            return Err(refusal);
        }
        let Some(kind) = self.kind() else {
            return Err(option_set_refusal(Self::SCOPE, self.as_ref()));
        };
        let accepted = match kind {
            OptionKind::String => matches!(value, OptionValue::String(_)),
            OptionKind::Bytes => matches!(value, OptionValue::Bytes(_)),
            // `options::integer_value` also accepts base-10 strings, since some transports carry
            // only strings (dbt-core 2.0's does).
            OptionKind::Int => matches!(value, OptionValue::Int(_) | OptionValue::String(_)),
        };
        if accepted {
            Ok(())
        } else {
            Err(expected_type(self.as_ref(), kind.as_str(), value))
        }
    }
}

impl RecognizedKey for OptionDatabase {
    const SCOPE: &'static str = super::database::KIND;

    fn canonical_name(&self) -> &str {
        let name = self.as_ref();
        BigQueryDatabaseOption::from_name(name).map_or(name, |key| key.as_str())
    }

    fn kind(&self) -> Option<OptionKind> {
        database_option_kind(self)
    }
}

impl RecognizedKey for OptionConnection {
    const SCOPE: &'static str = super::connection::KIND;

    fn canonical_name(&self) -> &str {
        let name = self.as_ref();
        BigQueryConnectionOption::from_name(name).map_or(name, |key| key.as_str())
    }

    fn kind(&self) -> Option<OptionKind> {
        connection_option_kind(self)
    }

    fn set_refusal(&self) -> Option<Error> {
        connection_set_refusal(self)
    }
}

impl<K, S> OptionTarget for Staged<K, S>
where
    K: for<'a> From<&'a str> + RecognizedKey,
    S: Optionable<Option = K>,
{
    type Key = K;
    type Options = S;

    fn set(&mut self, key: K, value: OptionValue) -> Result<()> {
        match self {
            Self::Pending(options) => {
                key.check_pre_init_set(&value)?;
                let canonical_name = key.canonical_name();
                if let Some(entry) = options
                    .iter_mut()
                    .find(|(existing, _)| existing.canonical_name() == canonical_name)
                {
                    *entry = (key, value);
                } else {
                    options.push((key, value));
                }
                Ok(())
            }
            Self::Ready(state) => state.set_option(key, value),
        }
    }

    /// Pending values are unvalidated; getters report `NOT_FOUND` until `Init` succeeds.
    fn options(&mut self, kind: &str, key: &K) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) if key.kind().is_some() => Err(not_found(format!(
                "BigQuery {kind} option {} cannot be read before the {kind} is initialized; \
                 initialize it first",
                key.as_ref()
            ))),
            Self::Pending(_) => Err(option_get_refusal(kind, key.as_ref())),
        }
    }
}

type KeyOf<H> = <<H as FfiHandle>::State as OptionTarget>::Key;

/// The address of a caller-supplied handle's `private_data` field, or null for a null handle.
///
/// Every read and write of that field goes through [`super::handle`], which orders `New` and
/// `Release` with the calls that may race them, so no reference to the whole struct is ever
/// formed. It also keeps two failures apart: a null slot is a bad argument (`INVALID_ARGUMENT`),
/// and an unpopulated one an object used out of order (`INVALID_STATE`).
pub(super) fn slot_of<H: FfiHandle>(handle: *mut H) -> *mut *mut c_void {
    handle.cast()
}

/// Run `f` against the state behind a caller-supplied handle, dispatching through
/// [`super::handle::dispatch`] for panic containment and the poison/lock discipline.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe fn with_state<H, F>(handle: *mut H, error: *mut AdbcError, f: F) -> AdbcStatusCode
where
    H: FfiHandle,
    F: FnOnce(&mut H::State) -> Result<()>,
{
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { dispatch::<H::State, _>(slot_of(handle), H::KIND, error, f) }
}

/// Run `f` against the state behind a caller-supplied handle and hand the reader it produces to
/// the caller as an Arrow C stream.
///
/// A null `out` is refused before `f` runs, so no query starts (or bills) without a destination
/// for its rows.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe fn with_stream<H, F>(
    handle: *mut H,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
    f: F,
) -> AdbcStatusCode
where
    H: FfiHandle,
    F: FnOnce(&mut H::State) -> Result<Box<dyn RecordBatchReader + Send + 'static>>,
{
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            required_output(out, "out")?;
            write_out(out, export_reader(f(state)?), "out")
        })
    }
}

/// `AdbcDatabaseNew` and `AdbcConnectionNew`: populate the caller's handle with a pre-init state.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
// Guard-exempt: nothing here can unwind. The default state allocates nothing, `Exported`
// allocation failure aborts, and the rest is pointer writes.
pub(super) unsafe extern "C" fn new_handle<H>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode
where
    H: FfiHandle<State: Default>,
{
    let exported = Exported::new(H::State::default(), None, None);
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { finish(install(slot_of(handle), H::KIND, exported), error) }
}

/// Every `*Release`: empty the slot and drop its reference, so a second release is an error
/// rather than a double free.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn release_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { super::handle::release::<H::State>(slot_of(handle), H::KIND, error) }
}

/// Run `f` on the locked state behind a second handle that an entry point uses besides the one
/// it dispatches on. The caller must still check readiness.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe fn with_handle<H: FfiHandle, R>(
    handle: *mut H,
    f: impl FnOnce(&mut H::State) -> Result<R>,
) -> Result<R> {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { with_locked::<H::State, R>(slot_of(handle), H::KIND, f) }
}

/// # Safety
/// The [shared FFI safety contracts](super) apply.
unsafe fn parse_key<H: FfiHandle>(key: *const c_char) -> Result<KeyOf<H>> {
    // SAFETY: The caller supplies a readable NUL-terminated key for this call, or null is rejected.
    Ok(KeyOf::<H>::from(unsafe { required_str(key, "key") }?))
}

/// Apply an option value converted from its C representation, or report why it could not be.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
unsafe fn apply_option<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: Result<OptionValue>,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // A malformed value is a bad argument whatever state the handle is in.
    let value = match value {
        Ok(value) => value,
        // SAFETY: the caller provides initialized, exclusively writable error storage or null.
        Err(failure) => return unsafe { finish(Err(failure), error) },
    };
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            state.set(parse_key::<H>(key)?, value)
        })
    }
}

/// `Adbc*SetOption`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn set_option<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        let value = required_str(value, "value").map(OptionValue::from);
        apply_option(handle, key, value, error)
    }
}

/// `Adbc*SetOptionBytes`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn set_option_bytes<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        let value = byte_slice(value, length, "value").map(OptionValue::from);
        apply_option(handle, key, value, error)
    }
}

/// `Adbc*SetOptionInt`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn set_option_int<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { apply_option(handle, key, Ok(value.into()), error) }
}

/// `Adbc*SetOptionDouble`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn set_option_double<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe { apply_option(handle, key, Ok(value.into()), error) }
}

/// `Adbc*GetOption`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn get_option<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_string(key)?;
            write_string(&found, value, length)
        })
    }
}

/// `Adbc*GetOptionBytes`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn get_option_bytes<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_bytes(key)?;
            write_bytes(&found, value, length)
        })
    }
}

/// `Adbc*GetOptionInt`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn get_option_int<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_int(key)?;
            write_out(value, found, "value")
        })
    }
}

/// `Adbc*GetOptionDouble`.
///
/// # Safety
/// The [shared FFI safety contracts](super) apply.
pub(super) unsafe extern "C" fn get_option_double<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // SAFETY: The caller upholds the shared contracts for every argument.
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_double(key)?;
            write_out(value, found, "value")
        })
    }
}
