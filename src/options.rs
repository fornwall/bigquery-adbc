//! BigQuery option [`keys`], enumerated [`values`], and ADBC value decoders.
//!
//! Errors must not include option values: credentials can arrive under any key. JSON parse
//! errors report only line and column, since serde's error messages can quote the input.

pub(crate) mod keys;
#[cfg(test)]
mod tests;
pub(crate) mod values;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::num::{IntErrorKind, ParseIntError};
use std::sync::Arc;

use adbc_core::constants::{
    ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_CREATE,
    ADBC_INGEST_OPTION_MODE_CREATE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
    ADBC_OPTION_VALUE_DISABLED, ADBC_OPTION_VALUE_ENABLED,
};
use adbc_core::error::Result;
use adbc_core::options::{IngestMode, OptionValue};
use serde::de::DeserializeOwned;
use serde_json::error::Category;

use crate::error::{invalid_argument, invalid_state};

macro_rules! string_enum {
    (
        $(#[$enum_meta:meta])*
        $visibility:vis enum $name:ident {
            $(
                $(#[$($variant_meta:tt)*])*
                $variant:ident => ($value:literal, [$($alias:literal),*], $kind:ident)
            ),+ $(,)?
        }
        $(into $target:ty => |$input:ident| $conversion:expr;)?)
    => {
        $crate::options::string_enum! {
            $(#[$enum_meta])*
            $visibility enum $name {
                $(
                    $(#[$($variant_meta)*])*
                    $variant => ($value, [$($alias),*])
                ),+
            }
            $(into $target => |$input| $conversion;)?
        }

        impl $name {
            pub(crate) const fn option_kind(self) -> $crate::options::OptionKind {
                match self {
                    $(Self::$variant => $crate::options::OptionKind::$kind),+
                }
            }
        }
    };
    (
        $(#[$enum_meta:meta])*
        $visibility:vis enum $name:ident {
            $(
                $(#[$($variant_meta:tt)*])*
                $variant:ident => ($value:literal, [$($alias:literal),*])
            ),+ $(,)?
        }
        $(into $target:ty => |$input:ident| $conversion:expr;)?)
    => {
        $(#[$enum_meta])*
        $visibility enum $name {
            $(
                $(#[$($variant_meta)*])*
                #[doc = ""]
                #[doc = concat!("Spelled `", $value, "`", $(", or `", $alias, "`",)* ".")]
                $variant
            ),+
        }

        impl $name {
            /// Every value, in declaration order.
            pub(crate) const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Each value's Rust name, canonical spelling and documentation.
            #[cfg(any(test, feature = "python-option-codegen"))]
            pub(crate) const DECLARED: &'static [(&'static str, &'static str, &'static str)] = &[$((
                stringify!($variant),
                $value,
                concat!($($crate::options::doc_text!($($variant_meta)*)),*),
            )),+];

            /// The canonical spelling, which is what the driver reads an option back as.
            $visibility const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            /// The non-canonical spellings the driver also accepts, in declaration order.
            pub(crate) const fn legacy_aliases(self) -> &'static [&'static str] {
                match self {
                    $(Self::$variant => &[$($alias),*]),+
                }
            }

            /// Match a canonical spelling or legacy alias, ignoring ASCII case.
            ///
            /// Enum spellings must be distinct up to case, including aliases.
            $visibility fn from_name(value: &str) -> Option<Self> {
                $crate::options::enum_name_index(
                    value,
                    <Self as $crate::options::StringEnum>::NAMES,
                )
                .and_then(|index| Self::ALL.get(index).copied())
                .or_else(|| {
                    Self::ALL.iter().copied().find(|candidate| {
                        candidate
                            .legacy_aliases()
                            .iter()
                            .any(|alias| alias.eq_ignore_ascii_case(value))
                    })
                })
            }
        }

        impl $crate::options::StringEnum for $name {
            const ALL: &'static [Self] = Self::ALL;
            const NAMES: &'static [&'static str] = &[$($value),+];

            fn as_str(self) -> &'static str {
                $name::as_str(self)
            }

            fn from_name(value: &str) -> Option<Self> {
                $name::from_name(value)
            }
        }

        $(
            impl From<$name> for $target {
                fn from($input: $name) -> Self {
                    $conversion
                }
            }
        )?
    };
}

pub(crate) use string_enum;

/// The text of one `string_enum!` attribute: a doc comment's, and nothing for any other.
#[cfg(any(test, feature = "python-option-codegen"))]
macro_rules! doc_text {
    (doc = $text:literal) => {
        $text
    };
    ($($other:tt)*) => {
        ""
    };
}

#[cfg(any(test, feature = "python-option-codegen"))]
pub(crate) use doc_text;

// Canonical names and enum values have the same declaration order. Share their scan across
// enums without inlining a separate case-insensitive matcher into each option entry point.
#[inline(never)]
pub(crate) fn enum_name_index(value: &str, names: &[&str]) -> Option<usize> {
    names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(value))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OptionKind {
    String,
    Bytes,
    Int,
}

impl OptionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Int => "integer",
        }
    }
}

pub(crate) trait StringEnum: Copy + 'static {
    const ALL: &'static [Self];
    const NAMES: &'static [&'static str];

    fn as_str(self) -> &'static str;

    fn from_name(value: &str) -> Option<Self>;
}

pub(crate) fn valid_enum_values<T: StringEnum>() -> String {
    quoted_values(T::NAMES)
}

// Share one formatter across option enums and keep rejection formatting off success paths.
#[cold]
#[inline(never)]
fn quoted_values(values: &[&str]) -> String {
    let mut result = String::new();
    for value in values {
        if !result.is_empty() {
            result.push_str(", ");
        }
        write!(result, "{value:?}").expect("writing to a String cannot fail");
    }
    result
}

/// Joins a rejection's alternatives as `a, b, or c`. The caller renders the entries.
pub(crate) fn or_list<T: AsRef<str>>(values: &[T]) -> String {
    match values {
        [] => String::new(),
        [only] => only.as_ref().to_owned(),
        [rest @ .., last] => {
            let rest: Vec<&str> = rest.iter().map(AsRef::as_ref).collect();
            format!("{}, or {}", rest.join(", "), last.as_ref())
        }
    }
}

/// Return `InvalidState` when a dataset is set without a project. The dataset ID itself is valid;
/// `project_options` lists the options that can supply the missing project in this scope.
pub(crate) fn dataset_needs_a_project(
    dataset_option: &str,
    project_options: &[&str],
) -> adbc_core::error::Error {
    invalid_state(format!(
        "a BigQuery project must be available before setting {dataset_option}; set {}",
        or_list(project_options)
    ))
}

/// Decode a string option.
pub(crate) fn string_value(key: &str, value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        value => Err(expected_type(key, "string", &value)),
    }
}

/// Decode a bytes option.
pub(crate) fn bytes_value(key: &str, value: OptionValue) -> Result<Vec<u8>> {
    match value {
        OptionValue::Bytes(value) => Ok(value),
        value => Err(expected_type(key, "bytes", &value)),
    }
}

/// Decode an integer option, which a caller may spell as an integer or as a base-10 string.
///
/// Some transports carry only strings (a connection string, a YAML profile, a driver manager
/// without `SetOptionInt`); dbt 2.0 sends `adbc.bigquery.dataproc.pooling_timeout` as a
/// string through `AdbcStatementSetOption`.
pub(crate) fn integer_value(key: &str, value: OptionValue) -> Result<i64> {
    match value {
        OptionValue::Int(value) => Ok(value),
        OptionValue::String(value) => value.trim().parse().map_err(|source: ParseIntError| {
            let requirement = match source.kind() {
                IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
                    "must fit in a signed 64-bit integer"
                }
                _ => "must be a base-10 integer",
            };
            invalid_argument(format!("BigQuery option {key} {requirement}"))
        }),
        value => Err(expected_type(key, "integer", &value)),
    }
}

/// Decode a JSON object option, `{"key": value, ...}`, into a map of typed values.
///
/// The empty string and the empty object both mean "not configured", which is how such an option
/// is cleared. `requirement` completes "must be". A syntax error is located by line and column; a
/// well-formed document of the wrong shape is not, because serde's position for one points at
/// the end of the mismatched token, or at column 0 for the document itself.
pub(crate) fn json_object_value<V: DeserializeOwned>(
    key: &str,
    value: &str,
    requirement: &str,
) -> Result<Option<Arc<BTreeMap<String, V>>>> {
    if value.is_empty() {
        return Ok(None);
    }
    let map: BTreeMap<String, V> = serde_json::from_slice(value.as_bytes()).map_err(|source| {
        let problem = match source.classify() {
            Category::Data => "it is well-formed JSON of another shape".to_owned(),
            _ => format!(
                "it is not well-formed JSON: parsing stopped at line {} column {}",
                source.line(),
                source.column()
            ),
        };
        invalid_argument(format!(
            "BigQuery option {key} must be {requirement}; {problem}"
        ))
    })?;
    Ok((!map.is_empty()).then(|| Arc::new(map)))
}

/// Decode one of the driver's string enums.
pub(crate) fn enum_value<T: StringEnum>(key: &str, value: OptionValue) -> Result<T> {
    let value = string_value(key, value)?;
    T::from_name(&value).ok_or_else(|| invalid_enum_value::<T>(key))
}

/// Decode an optional string enum, treating the empty ADBC string as unset.
pub(crate) fn optional_enum_value<T: StringEnum>(
    key: &str,
    value: OptionValue,
) -> Result<Option<T>> {
    let value = string_value(key, value)?;
    if value.is_empty() {
        Ok(None)
    } else {
        T::from_name(&value)
            .map(Some)
            .ok_or_else(|| invalid_enum_value::<T>(key))
    }
}

/// Decode an ADBC boolean string, ignoring ASCII case.
///
/// Only the two ADBC spellings emitted by [`boolean_option`] are accepted.
pub(crate) fn boolean_value(key: &str, value: OptionValue) -> Result<bool> {
    let value = string_value(key, value)?;
    if value.eq_ignore_ascii_case(ADBC_OPTION_VALUE_ENABLED) {
        Ok(true)
    } else if value.eq_ignore_ascii_case(ADBC_OPTION_VALUE_DISABLED) {
        Ok(false)
    } else {
        Err(invalid_argument(format!(
            "BigQuery option {key} must be {ADBC_OPTION_VALUE_ENABLED:?} or {ADBC_OPTION_VALUE_DISABLED:?}"
        )))
    }
}

/// Render a boolean as the standard ADBC option string.
pub(crate) fn boolean_option(value: bool) -> String {
    if value {
        ADBC_OPTION_VALUE_ENABLED
    } else {
        ADBC_OPTION_VALUE_DISABLED
    }
    .to_owned()
}

/// Decode the standard ADBC ingest mode without using its payload-rendering error conversion.
pub(crate) fn ingest_mode_value(key: &str, value: OptionValue) -> Result<IngestMode> {
    let value = string_value(key, value)?;
    value.parse().map_err(|_| {
        invalid_argument(format!(
            "BigQuery option {key} must be one of {ADBC_INGEST_OPTION_MODE_CREATE:?}, \
             {ADBC_INGEST_OPTION_MODE_APPEND:?}, {ADBC_INGEST_OPTION_MODE_REPLACE:?}, or \
             {ADBC_INGEST_OPTION_MODE_CREATE_APPEND:?}"
        ))
    })
}

pub(crate) fn expected_type(
    key: &str,
    expected: &str,
    value: &OptionValue,
) -> adbc_core::error::Error {
    let article = match expected.as_bytes() {
        [b'a' | b'e' | b'i' | b'o' | b'u', ..] => "an",
        _ => "a",
    };
    let received = match value {
        OptionValue::String(_) => "string",
        OptionValue::Bytes(_) => "bytes",
        OptionValue::Int(_) => "integer",
        OptionValue::Double(_) => "double",
        _ => "unknown type",
    };
    invalid_argument(format!(
        "BigQuery option {key} requires {article} {expected} value, received {received}"
    ))
}

fn invalid_enum_value<T: StringEnum>(key: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery option {key} must be one of {}",
        valid_enum_values::<T>()
    ))
}
