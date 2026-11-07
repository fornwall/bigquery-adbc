//! Compact positional representation of BigQuery's REST result-row wire format.
//!
//! BigQuery sends a row as `{ "f": [{ "v": ... }] }`. A nested record repeats the `f`
//! envelope and a repeated value is an array of `v`-wrapped cells. Values stay positional, which
//! is cheaper than name-keyed maps.

use std::cell::Cell as StateCell;
use std::fmt;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// The `rows` array of one REST result page.
///
/// Decoded as a whole so one row's cell count sizes the next row's vector: every row has the
/// schema's width, and serde_json's `SeqAccess::size_hint` is always `None`, so an unsized vector
/// would regrow on every row.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct RestRows(Vec<RestRow>);

impl RestRows {
    pub(crate) fn into_vec(self) -> Vec<RestRow> {
        self.0
    }
}

/// One BigQuery REST result row, with cells in result-schema order.
#[derive(Debug, PartialEq)]
pub(crate) struct RestRow {
    cells: Vec<RestValue>,
}

impl RestRow {
    #[cfg(test)]
    pub(crate) fn new(cells: Vec<RestValue>) -> Self {
        Self { cells }
    }

    pub(crate) fn len(&self) -> usize {
        self.cells.len()
    }

    pub(crate) fn into_cells(self) -> Vec<RestValue> {
        self.cells
    }

    /// Heap allocations owned by this row, including spare cell and nested-value capacity.
    /// The row itself is stored in its batch's separately charged vector.
    pub(crate) fn allocated_bytes(&self) -> usize {
        allocated_value_bytes(&self.cells)
    }
}

fn allocated_value_bytes(values: &Vec<RestValue>) -> usize {
    values.iter().fold(
        values.capacity().saturating_mul(size_of::<RestValue>()),
        |bytes, value| {
            bytes.saturating_add(match value {
                RestValue::String(value) => value.capacity(),
                RestValue::Repeated(values) | RestValue::Record(values) => {
                    allocated_value_bytes(values)
                }
                _ => 0,
            })
        },
    )
}

/// A value after removing only BigQuery's `f` and `v` envelopes.
///
/// Scalars retain their JSON primitive shape so malformed-but-decodable responses are diagnosed
/// against the result schema. Records and arrays own only values: field names already live in the
/// schema.
#[derive(Debug, PartialEq)]
pub(crate) enum RestValue {
    Null,
    String(String),
    // BigQuery spells even integers, booleans and temporal values as JSON strings, so most cells
    // are short enough to live in the enum's own 32 bytes rather than in a heap allocation.
    ShortString {
        bytes: [u8; INLINE_CAPACITY],
        length: u8,
    },
    Boolean(bool),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Repeated(Vec<Self>),
    Record(Vec<Self>),
}

/// The longest string [`RestValue::ShortString`] holds: the enum's 32 bytes (set by `String` and
/// `Vec`) minus the tag and the length. That covers a microsecond DATETIME (26 bytes), a
/// `RANGE<DATE>` literal (24), and every INT64 and integer TIMESTAMP.
const INLINE_CAPACITY: usize = 30;

impl RestValue {
    fn string(value: &str) -> Self {
        let mut bytes = [0; INLINE_CAPACITY];
        if let Some(target) = bytes.get_mut(..value.len()) {
            target.copy_from_slice(value.as_bytes());
            Self::ShortString {
                bytes,
                length: u8::try_from(value.len()).expect("an inline string fits a u8 length"),
            }
        } else {
            Self::String(value.to_owned())
        }
    }

    /// The cell's text as raw bytes, without the UTF-8 check [`Self::as_str`] repeats.
    ///
    /// Both variants were copied from a `&str`, so the bytes are valid; a STRING column checks
    /// its bytes once for the whole column instead.
    pub(crate) fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(value) => Some(value.as_bytes()),
            Self::ShortString { bytes, length } => bytes.get(..usize::from(*length)),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            // The inline bytes were copied from a `&str`, so the check cannot fail.
            Self::ShortString { .. } => Some(
                std::str::from_utf8(self.as_bytes()?)
                    .expect("inline strings copy a complete UTF-8 string"),
            ),
            _ => None,
        }
    }

    pub(crate) const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::String(_) | Self::ShortString { .. } => "a string",
            Self::Boolean(_) => "a boolean",
            Self::Integer(_) | Self::Unsigned(_) | Self::Float(_) => "a number",
            Self::Repeated(_) => "an array",
            Self::Record(_) => "an object",
        }
    }
}

impl<'de> Deserialize<'de> for RestRows {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `deserialize_any`, not `deserialize_seq`: for a non-array, serde_json's
        // `deserialize_seq` builds an `Unexpected` that echoes the value, which the visitors below
        // exist to prevent.
        deserializer.deserialize_any(RowsVisitor)
    }
}

impl<'de> Deserialize<'de> for RestValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

/// Override serde's shape errors because its default visitor includes a string's contents in an
/// `Unexpected::Str`, while result cell values may contain sensitive or unbounded data.
///
/// With a message alone it covers every shape for a visitor that decodes only a map; the
/// `@non_seq` form omits `visit_seq` for a visitor that decodes sequences itself.
macro_rules! wrong_shape_methods {
    ($message:literal) => {
        wrong_shape_methods!(@non_seq $message);

        fn visit_seq<A: SeqAccess<'de>>(self, _sequence: A) -> Result<Self::Value, A::Error> {
            Err(de::Error::custom($message))
        }
    };
    (@non_seq $message:literal) => {
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_bool<E: de::Error>(self, _value: bool) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_i64<E: de::Error>(self, _value: i64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_u64<E: de::Error>(self, _value: u64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_f64<E: de::Error>(self, _value: f64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_str<E: de::Error>(self, _value: &str) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }
    };
}

struct RowsVisitor;

impl<'de> Visitor<'de> for RowsVisitor {
    type Value = RestRows;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a BigQuery REST result row array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let width = StateCell::new(0);
        let mut rows = Vec::new();
        while let Some(row) = sequence.next_element_seed(RowVisitor(&width))? {
            rows.push(row);
        }
        Ok(RestRows(rows))
    }

    wrong_shape_methods!(@non_seq "BigQuery REST `rows` field is not a JSON array");
}

/// Decodes one row, sizing its cells from the width the previous row showed and updating it.
struct RowVisitor<'a>(&'a StateCell<usize>);

impl<'de> DeserializeSeed<'de> for RowVisitor<'_> {
    type Value = RestRow;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RowVisitor<'_> {
    type Value = RestRow;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a BigQuery REST row object")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        decode_cells(map, "row", Some(self.0)).map(|cells| RestRow { cells })
    }

    wrong_shape_methods!("BigQuery REST row is not a JSON object");
}

/// Unwraps one `v`-wrapped cell inside a row or repeated value.
struct CellVisitor;

impl<'de> DeserializeSeed<'de> for CellVisitor {
    type Value = RestValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for CellVisitor {
    type Value = RestValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a BigQuery REST cell object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut value = None;
        while let Some(field) = map.next_key::<EnvelopeField>()? {
            match field {
                EnvelopeField::Value => {
                    if value.is_some() {
                        return Err(de::Error::duplicate_field("v"));
                    }
                    value = Some(map.next_value()?);
                }
                EnvelopeField::Fields | EnvelopeField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        value.ok_or_else(|| de::Error::missing_field("v"))
    }

    wrong_shape_methods!("BigQuery REST cell is not a JSON object");
}

/// Decodes an `f` array straight into its unwrapped values.
///
/// The width is the page-wide row width when this is a row's own `f` array, and `None` for a
/// nested record, whose width is its own and says nothing about the next row.
struct CellsVisitor<'a>(Option<&'a StateCell<usize>>);

impl<'de> DeserializeSeed<'de> for CellsVisitor<'_> {
    type Value = Vec<RestValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for CellsVisitor<'_> {
    type Value = Vec<RestValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a BigQuery REST `f` cell array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(self.0.map_or(0, StateCell::get));
        while let Some(value) = sequence.next_element_seed(CellVisitor)? {
            values.push(value);
        }
        if let Some(width) = self.0 {
            width.set(values.len());
        }
        Ok(values)
    }

    wrong_shape_methods!(@non_seq "BigQuery REST `f` field is not a JSON array");
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = RestValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a BigQuery REST cell value")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(RestValue::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(RestValue::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(RestValue::Boolean(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(RestValue::Integer(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(RestValue::Unsigned(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(RestValue::Float(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(RestValue::string(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(CellVisitor)? {
            values.push(value);
        }
        Ok(RestValue::Repeated(values))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        decode_cells(map, "record", None).map(RestValue::Record)
    }
}

/// Extracts the positional `f` array shared by a top-level row and a nested record.
///
/// [`CellVisitor`] keeps its own copy of this loop: one generic loop serving both measured about
/// 15% slower in the `rest_rows` decode benchmark.
fn decode_cells<'de, A>(
    mut map: A,
    context: &'static str,
    width: Option<&StateCell<usize>>,
) -> Result<Vec<RestValue>, A::Error>
where
    A: MapAccess<'de>,
{
    let mut cells = None;
    while let Some(field) = map.next_key::<EnvelopeField>()? {
        match field {
            EnvelopeField::Fields => {
                if cells.is_some() {
                    return Err(de::Error::duplicate_field("f"));
                }
                cells = Some(map.next_value_seed(CellsVisitor(width))?);
            }
            EnvelopeField::Value | EnvelopeField::Other => {
                map.next_value::<IgnoredAny>()?;
            }
        }
    }
    cells.ok_or_else(|| {
        de::Error::custom(format_args!("BigQuery REST {context} is missing field `f`"))
    })
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum EnvelopeField {
    #[serde(rename = "f")]
    Fields,
    #[serde(rename = "v")]
    Value,
    #[serde(other)]
    Other,
}

#[cfg(test)]
mod tests;
