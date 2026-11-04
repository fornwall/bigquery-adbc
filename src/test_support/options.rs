//! Option helpers that panic on unexpected results at the caller's location.

use adbc_core::Optionable;
use adbc_core::error::{Error, Status};
use adbc_core::options::OptionValue;

#[track_caller]
pub(crate) fn set_option<T: Optionable>(
    object: &mut T,
    key: impl Into<T::Option>,
    value: impl Into<OptionValue>,
) {
    object.set_option(key.into(), value.into()).unwrap();
}

#[track_caller]
pub(crate) fn set_option_error<T: Optionable>(
    object: &mut T,
    key: impl Into<T::Option>,
    value: impl Into<OptionValue>,
) -> Error {
    object.set_option(key.into(), value.into()).unwrap_err()
}

#[track_caller]
pub(crate) fn option_string<T: Optionable>(object: &T, key: impl Into<T::Option>) -> String {
    object.get_option_string(key.into()).unwrap()
}

#[track_caller]
pub(crate) fn option_int<T: Optionable>(object: &T, key: impl Into<T::Option>) -> i64 {
    object.get_option_int(key.into()).unwrap()
}

/// Asserts a string option reads back as `expected`, `None` meaning unset (NOT_FOUND).
#[track_caller]
pub(crate) fn assert_string_option<T: Optionable>(
    object: &T,
    key: impl Into<T::Option>,
    expected: Option<&str>,
) {
    let key = key.into();
    let name = key.as_ref().to_owned();
    match (object.get_option_string(key), expected) {
        (Ok(value), Some(expected)) => assert_eq!(value, expected, "{name}"),
        (Err(error), None) => assert_eq!(error.status, Status::NotFound, "{name}"),
        (actual, expected) => panic!("{name}: expected {expected:?}, got {actual:?}"),
    }
}
