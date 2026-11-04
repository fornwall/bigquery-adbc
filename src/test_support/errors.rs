//! Asserting on the text of a driver error.
//!
//! A failed `message.contains(..)` prints only `false`. These report the whole message and the
//! fragment that was missing or leaked.

use adbc_core::error::Error;

/// Asserts that `error`'s message contains every fragment, naming the missing one.
#[track_caller]
pub(crate) fn assert_message(error: &Error, fragments: &[&str]) {
    for fragment in fragments {
        assert!(
            error.message.contains(fragment),
            "expected {fragment:?} in: {}",
            error.message
        );
    }
}

/// Asserts that `error`'s message contains none of `fragments` (secrets or caller payloads it must
/// not echo), naming the one that leaked.
#[track_caller]
pub(crate) fn assert_message_omits(error: &Error, fragments: &[&str]) {
    for fragment in fragments {
        assert!(
            !error.message.contains(fragment),
            "{fragment:?} leaked into: {}",
            error.message
        );
    }
}
