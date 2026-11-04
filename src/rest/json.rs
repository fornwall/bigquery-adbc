//! JSON encoding and decoding for directly issued requests.
//!
//! Errors name the document rather than quoting it. [`JsonInt`] handles REST 64-bit integers.

use adbc_core::error::{Error, Result, Status};
use serde::{Deserialize, Serialize};

use crate::error::{driver_defect, error};

/// Encodes one request body. `what` names the document in the error, as a noun phrase: "the
/// BigQuery table description".
pub(crate) fn encode_json<T: Serialize + ?Sized>(value: &T, what: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|source| encode_failure(what, &source))
}

/// What a failed encode of `what` reports: a driver defect, since every body is one it built.
#[cold]
#[inline(never)]
pub(crate) fn encode_failure(what: &str, source: &serde_json::Error) -> Error {
    driver_defect(format_args!("failed to encode {what}: {source}"))
}

/// Decodes one response document. `what` names it in the error, as a noun phrase: "the BigQuery
/// CSV load response".
pub(crate) fn decode_json<'a, T: Deserialize<'a>>(body: &'a [u8], what: &str) -> Result<T> {
    serde_json::from_slice(body).map_err(|source| decode_failure(what, &source))
}

/// Keep the error category and location, since serde's diagnostic can quote response values.
#[cold]
#[inline(never)]
pub(crate) fn decode_failure(what: &str, source: &serde_json::Error) -> Error {
    let detail = match source.classify() {
        serde_json::error::Category::Io => "JSON input failure",
        serde_json::error::Category::Syntax => "invalid JSON syntax",
        serde_json::error::Category::Data => "unexpected JSON structure",
        serde_json::error::Category::Eof => "incomplete JSON",
    };
    error(
        format!(
            "failed to decode {what}: {detail} at line {} column {}",
            source.line(),
            source.column(),
        ),
        Status::InvalidData,
    )
}

/// One BigQuery REST 64-bit integer, on the wire and back.
///
/// The REST API encodes `int64` and `uint64` as decimal strings because JSON numbers are doubles,
/// but some fields also arrive as plain numbers (`jobs.query` and `jobs.getQueryResults`
/// differ). This writes a string and reads either. `T` sets the accepted range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JsonInt<T>(pub(crate) T);

impl<T: std::fmt::Display> Serialize for JsonInt<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// An integer type [`JsonInt`] can carry, with its out-of-range wording.
pub(crate) trait JsonIntValue: Copy + std::str::FromStr {
    /// The BigQuery type name, as its own diagnostics spell it.
    const NAME: &'static str;
    /// How this type describes a JSON number outside its range, as a verb phrase.
    const OUT_OF_RANGE: &'static str;

    fn from_i64(value: i64) -> Option<Self>;
    fn from_u64(value: u64) -> Option<Self>;
}

impl JsonIntValue for i64 {
    const NAME: &'static str = "int64";
    const OUT_OF_RANGE: &'static str = "is outside the supported range";

    fn from_i64(value: i64) -> Option<Self> {
        Some(value)
    }

    fn from_u64(value: u64) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

impl JsonIntValue for u64 {
    const NAME: &'static str = "uint64";
    const OUT_OF_RANGE: &'static str = "cannot be negative";

    fn from_i64(value: i64) -> Option<Self> {
        Self::try_from(value).ok()
    }

    fn from_u64(value: u64) -> Option<Self> {
        Some(value)
    }
}

impl<'de, T: JsonIntValue> Deserialize<'de> for JsonInt<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(JsonIntVisitor::<T>(std::marker::PhantomData))
    }
}

struct JsonIntVisitor<T>(std::marker::PhantomData<T>);

impl<T: JsonIntValue> serde::de::Visitor<'_> for JsonIntVisitor<T> {
    type Value = JsonInt<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "a BigQuery REST {} encoded as a decimal string or integer",
            T::NAME
        )
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
        T::from_i64(value)
            .map(JsonInt)
            .ok_or_else(|| E::custom(format!("BigQuery REST {} {}", T::NAME, T::OUT_OF_RANGE)))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        T::from_u64(value)
            .map(JsonInt)
            .ok_or_else(|| E::custom(format!("BigQuery REST {} {}", T::NAME, T::OUT_OF_RANGE)))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
        value.parse().map(JsonInt).map_err(|_| {
            E::custom(format!(
                "BigQuery REST {} is not a valid decimal integer",
                T::NAME
            ))
        })
    }
}

/// A [`JsonInt`] field that may be absent, unwrapped to the integer itself for `deserialize_with`.
pub(crate) fn optional_json_int<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: JsonIntValue,
{
    Ok(<Option<JsonInt<T>> as Deserialize>::deserialize(deserializer)?.map(|value| value.0))
}

#[cfg(test)]
mod tests {
    #[test]
    fn decode_failures_omit_unexpected_values() {
        for value in ["private-value".to_owned(), "\u{e9}".repeat(10_000)] {
            let body = serde_json::to_vec(&value).unwrap();
            let failure = super::decode_json::<u64>(&body, "the response").unwrap_err();
            assert_eq!(failure.status, adbc_core::error::Status::InvalidData);
            assert_eq!(
                failure.message,
                format!(
                    "failed to decode the response: unexpected JSON structure at line 1 column {}",
                    body.len(),
                )
            );
            crate::test_support::errors::assert_message_omits(&failure, &[&value]);
        }
    }

    #[test]
    fn decode_failures_keep_category_and_location() {
        for (body, detail) in [
            (
                b"[\n  true false]".as_slice(),
                "invalid JSON syntax at line 2 column 8",
            ),
            (b"[\n  true", "incomplete JSON at line 2 column 6"),
        ] {
            let failure =
                super::decode_json::<serde_json::Value>(body, "the response").unwrap_err();
            assert_eq!(failure.status, adbc_core::error::Status::InvalidData);
            assert_eq!(
                failure.message,
                format!("failed to decode the response: {detail}")
            );
        }
    }

    /// Out-of-range and malformed values are refused with each type's own range wording.
    #[test]
    fn a_rest_int64_is_written_as_a_string_and_read_from_either_spelling() {
        #[derive(Debug, serde::Deserialize)]
        struct Statistics {
            #[serde(default, deserialize_with = "super::optional_json_int")]
            signed: Option<i64>,
            #[serde(default, deserialize_with = "super::optional_json_int")]
            unsigned: Option<u64>,
        }

        assert_eq!(
            serde_json::to_string(&super::JsonInt(9_007_199_254_740_993_i64)).unwrap(),
            "\"9007199254740993\""
        );

        let decoded: Statistics =
            serde_json::from_str(r#"{"signed": "-12", "unsigned": 9007199254740993}"#).unwrap();
        assert_eq!(decoded.signed, Some(-12));
        assert_eq!(decoded.unsigned, Some(9_007_199_254_740_993));
        let absent: Statistics = serde_json::from_str("{}").unwrap();
        assert_eq!((absent.signed, absent.unsigned), (None, None));

        for (document, expected) in [
            (
                r#"{"unsigned": -1}"#,
                "BigQuery REST uint64 cannot be negative",
            ),
            (
                r#"{"signed": 18446744073709551615}"#,
                "BigQuery REST int64 is outside the supported range",
            ),
            (
                r#"{"signed": "twelve"}"#,
                "BigQuery REST int64 is not a valid decimal integer",
            ),
        ] {
            let failure = serde_json::from_str::<Statistics>(document).unwrap_err();
            assert!(failure.to_string().contains(expected), "{failure}");
        }
    }
}
