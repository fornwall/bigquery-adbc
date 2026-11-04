//! Identifier constructors for valid test literals.

use crate::identifiers::ProjectIdentifier;

pub(crate) fn project(value: &str) -> ProjectIdentifier {
    ProjectIdentifier::try_new(value).unwrap()
}
