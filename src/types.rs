//! The two BigQuery type namespaces the driver reads and writes.
//!
//! BigQuery names most types twice. Its REST API reports `TableFieldSchema.type` as `INTEGER`,
//! `FLOAT`, `BOOLEAN`, and `RECORD`, while GoogleSQL knows those types as `INT64`, `FLOAT64`,
//! `BOOL`, and `STRUCT` and does not accept `FLOAT` or `RECORD` at all. `BIGQUERY:type` field
//! metadata is a GoogleSQL type literal, so a caller can cast to it, repeat it in DDL, or nest it
//! in an `ARRAY<...>` or `STRUCT<...>`.
//!
//! Both spellings share one table entry, so the name the driver emits and the name it accepts
//! cannot drift apart.
//!
//! [`schema`] converts a REST `TableSchema` into the canonical Arrow schema. [`decimal`] holds the
//! NUMERIC and BIGNUMERIC shapes, [`temporal`] BigQuery's microsecond precision and civil
//! calendar, and [`cast`] the Arrow casts into the canonical types.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use adbc_core::error::{Result, Status};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Metadata, TimeUnit};

use crate::error::{error, invalid_argument, not_implemented};

pub(crate) mod cast;
pub(crate) mod decimal;
pub(crate) mod schema;
pub(crate) mod temporal;

use decimal::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, BigQueryDecimalType, NUMERIC_PRECISION, NUMERIC_SCALE,
    decimal_type_hint,
};

/// The deepest schema the recursive converters accept. BigQuery caps STRUCT nesting at 15
/// levels, so any real schema fits; the bound exists because a stack overflow aborts the host
/// process instead of unwinding through the FFI `catch_unwind` guard.
pub(crate) const MAX_SCHEMA_NESTING_DEPTH: usize = 32;

/// A field's position in its field list, and its name.
pub(crate) type NamedField<'a> = (usize, &'a str);

/// A column, STRUCT field or parameter name as BigQuery compares names case-insensitively: by
/// full Unicode uppercase mapping (observed, not documented). `é` and `É`, `ı` and `I`, `ß` and
/// `SS`, and `ﬁ` and `FI` collide, while `ß` and `ẞ`, `k` and the Kelvin sign `K`, and `İ` and `i`
/// stay distinct, which lowercasing would get wrong in both directions.
pub(crate) fn folded_name(name: &str) -> String {
    name.to_uppercase()
}

/// The first field, in field order, whose name equals an earlier field's under [`folded_name`],
/// paired with the earliest such field: what a left-to-right scan would report.
pub(crate) fn case_duplicate(fields: &Fields) -> Option<(NamedField<'_>, NamedField<'_>)> {
    let mut seen = HashMap::with_capacity(fields.len());
    fields
        .iter()
        .map(|field| field.name().as_str())
        .enumerate()
        .find_map(|named| match seen.entry(folded_name(named.1)) {
            Entry::Occupied(first) => Some((*first.get(), named)),
            Entry::Vacant(slot) => {
                slot.insert(named);
                None
            }
        })
}

/// Rejects schemas nested deeper than [`MAX_SCHEMA_NESTING_DEPTH`], iteratively, before a
/// recursive converter sees a caller-supplied schema.
pub(crate) fn validate_schema_nesting(fields: &Fields, context: &str) -> Result<()> {
    let mut stack: Vec<(&str, &DataType, usize)> = fields
        .iter()
        .map(|field| (field.name().as_str(), field.data_type(), 1))
        .collect();
    while let Some((name, data_type, depth)) = stack.pop() {
        if depth > MAX_SCHEMA_NESTING_DEPTH {
            return Err(invalid_argument(format!(
                "{context} field {name:?} is nested {depth} levels deep; the maximum is \
                 {MAX_SCHEMA_NESTING_DEPTH}"
            )));
        }
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => {
                stack.push((item.name().as_str(), item.data_type(), depth + 1));
            }
            DataType::Struct(fields) => {
                for field in fields {
                    stack.push((field.name().as_str(), field.data_type(), depth + 1));
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    stack.push((field.name().as_str(), field.data_type(), depth + 1));
                }
            }
            DataType::Dictionary(_, value) => {
                // A dictionary adds no BigQuery nesting, but the recursive converters still
                // descend into its value type, so it counts toward the recursion bound.
                stack.push((name, value, depth + 1));
            }
            DataType::RunEndEncoded(run_ends, values) => {
                stack.push((run_ends.name().as_str(), run_ends.data_type(), depth + 1));
                stack.push((values.name().as_str(), values.data_type(), depth + 1));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Where inside a nested value a conversion failed: a caller-specific root, then each array
/// element and struct field descended into.
///
/// The chain is borrowed rather than formatted, so descending costs nothing until a failure
/// renders it. Bound parameters and REST result columns share it, each with its own `Root`.
#[derive(Clone, Copy)]
pub(crate) enum ValueAt<'a> {
    Root(&'a dyn std::fmt::Display),
    Element {
        outer: &'a dyn std::fmt::Display,
        index: usize,
    },
    Member {
        outer: &'a dyn std::fmt::Display,
        name: &'a str,
    },
}

impl ValueAt<'_> {
    pub(crate) fn element(&self, index: usize) -> ValueAt<'_> {
        ValueAt::Element { outer: self, index }
    }

    pub(crate) fn member<'b>(&'b self, name: &'b str) -> ValueAt<'b> {
        ValueAt::Member { outer: self, name }
    }
}

impl std::fmt::Display for ValueAt<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Root(root) => root.fmt(formatter),
            Self::Element { outer, index } => write!(formatter, "{outer}, element {index}"),
            Self::Member { outer, name } => write!(formatter, "{outer}, field {name:?}"),
        }
    }
}

/// The field-metadata key that carries a field's GoogleSQL type literal, on every field the
/// driver emits and on caller-supplied fields that override the inferred type.
pub(crate) const BIGQUERY_TYPE_METADATA_KEY: &str = "BIGQUERY:type";

/// The Arrow extension-type metadata keys defined by the Arrow specification.
pub(crate) const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// See [`ARROW_EXTENSION_NAME_KEY`].
pub(crate) const ARROW_EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

const JSON_EXTENSION_NAME: &str = "arrow.json";
const GEOGRAPHY_EXTENSION_NAME: &str = "geoarrow.wkt";
const GEOGRAPHY_EXTENSION_METADATA: &str =
    r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#;

/// Inserts the Arrow extension-type annotations a BigQuery GEOGRAPHY or JSON column carries;
/// no other type has any.
pub(crate) fn insert_extension_metadata(kind: BigQueryType, metadata: &mut Metadata) {
    match kind {
        BigQueryType::Geography => {
            metadata.insert(
                ARROW_EXTENSION_NAME_KEY.to_owned(),
                GEOGRAPHY_EXTENSION_NAME.to_owned(),
            );
            metadata.insert(
                ARROW_EXTENSION_METADATA_KEY.to_owned(),
                GEOGRAPHY_EXTENSION_METADATA.to_owned(),
            );
        }
        BigQueryType::Json => {
            metadata.insert(
                ARROW_EXTENSION_NAME_KEY.to_owned(),
                JSON_EXTENSION_NAME.to_owned(),
            );
        }
        _ => {}
    }
}

/// Whether a field's Arrow extension name selects its BigQuery type, as JSON and GEOGRAPHY do.
pub(crate) fn has_bigquery_extension(field: &Field) -> bool {
    extension_type(field).is_some()
}

/// Whether a field's `BIGQUERY:type` metadata names a type other than the one its Arrow extension
/// implies. The metadata decides the field's type (see [`scalar_arrow_field_type`]).
pub(crate) fn extension_overridden_by_metadata(field: &Field) -> bool {
    extension_type(field).is_some_and(|kind| {
        field
            .metadata()
            .get(BIGQUERY_TYPE_METADATA_KEY)
            .is_some_and(|name| BigQueryType::from_sql(name) != Some(kind))
    })
}

/// The BigQuery type a field's Arrow extension name implies; the inverse of
/// [`insert_extension_metadata`].
fn extension_type(field: &Field) -> Option<BigQueryType> {
    match field.metadata().get(ARROW_EXTENSION_NAME_KEY)?.as_str() {
        JSON_EXTENSION_NAME => Some(BigQueryType::Json),
        GEOGRAPHY_EXTENSION_NAME => Some(BigQueryType::Geography),
        _ => None,
    }
}

/// Names the input surface a rejected `BIGQUERY:type` value came from.
pub(crate) const INGEST_FIELD: &str = "bulk ingest field";
/// The parameter-binding counterpart of [`INGEST_FIELD`].
pub(crate) const PARAMETER_FIELD: &str = "query parameter field";

macro_rules! bigquery_types {
    ($($variant:ident => $sql:literal, $rest:literal, $scalar:literal, [$($alias:literal),*];)+) => {
        /// A BigQuery type, independent of how the current namespace spells it.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum BigQueryType {
            $($variant,)+
        }

        impl BigQueryType {
            const ALL: &'static [Self] = &[$(Self::$variant,)+];

            /// The GoogleSQL name, which is what `BIGQUERY:type` metadata carries. For a composite
            /// type this is only the head: the metadata spells out `STRUCT<...>` or `RANGE<...>`.
            pub(crate) const fn sql(self) -> &'static str {
                match self {
                    $(Self::$variant => $sql,)+
                }
            }

            /// The REST `TableFieldSchema.type` name, which is what BigQuery's JSON API uses.
            pub(crate) const fn rest(self) -> &'static str {
                match self {
                    $(Self::$variant => $rest,)+
                }
            }

            /// Whether the type is complete on its own, rather than parameterized by element
            /// types.
            const fn is_scalar(self) -> bool {
                match self {
                    $(Self::$variant => $scalar,)+
                }
            }

            /// Further REST spellings BigQuery accepts for this type but never reports.
            const fn rest_aliases(self) -> &'static [&'static str] {
                match self {
                    $(Self::$variant => &[$($alias,)*],)+
                }
            }
        }
    };
}

bigquery_types! {
    String     => "STRING",     "STRING",     true,     [];
    Bytes      => "BYTES",      "BYTES",      true,     [];
    Int64      => "INT64",      "INTEGER",    true,     [];
    Float64    => "FLOAT64",    "FLOAT",      true,     [];
    Bool       => "BOOL",       "BOOLEAN",    true,     [];
    Timestamp  => "TIMESTAMP",  "TIMESTAMP",  true,     [];
    Date       => "DATE",       "DATE",       true,     [];
    Time       => "TIME",       "TIME",       true,     [];
    DateTime   => "DATETIME",   "DATETIME",   true,     [];
    Numeric    => "NUMERIC",    "NUMERIC",    true,     ["DECIMAL"];
    BigNumeric => "BIGNUMERIC", "BIGNUMERIC", true,     ["BIGDECIMAL"];
    Interval   => "INTERVAL",   "INTERVAL",   true,     [];
    Geography  => "GEOGRAPHY",  "GEOGRAPHY",  true,     [];
    Json       => "JSON",       "JSON",       true,     [];
    Struct     => "STRUCT",     "RECORD",     false,    [];
    Range      => "RANGE",      "RANGE",      false,    [];
}

impl BigQueryType {
    /// The canonical Arrow type for a scalar BigQuery type: what a query result decodes into and
    /// what an inferred query parameter is built from. Decimals default to their full
    /// `(38, 9)` / `(76, 38)` shape; a result field carrying an explicit precision overrides that
    /// at the call site. `None` for the composite types, whose Arrow shape is parameterized.
    pub(crate) fn canonical_data_type(self) -> Option<DataType> {
        Some(match self {
            Self::String | Self::Geography | Self::Json => DataType::Utf8,
            Self::Bytes => DataType::Binary,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time64(TimeUnit::Microsecond),
            Self::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Numeric => DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            Self::BigNumeric => DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE),
            Self::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Struct | Self::Range => return None,
        })
    }

    /// Parses a GoogleSQL type name: the only spelling the driver emits, and therefore the only
    /// one it accepts in caller-supplied `BIGQUERY:type` metadata.
    ///
    /// Never inlined: the search unrolls into ~800 bytes of comparisons, and its callers resolve
    /// schemas, not rows.
    #[inline(never)]
    pub(crate) fn from_sql(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.sql() == name)
    }

    /// Parses a type name from a BigQuery REST response, which may use either namespace: the API
    /// reports the legacy names but accepts and echoes the GoogleSQL ones.
    pub(crate) fn from_rest(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| {
            kind.rest() == name || kind.sql() == name || kind.rest_aliases().contains(&name)
        })
    }

    /// Whether metadata naming this type may annotate an Arrow field the driver inferred as
    /// `inferred`. A BigQuery `JSON` or `GEOGRAPHY` column is an Arrow string.
    pub(crate) fn annotates(self, inferred: Self) -> bool {
        self == inferred
            || (matches!(self, Self::Json | Self::Geography) && inferred == Self::String)
    }
}

/// Resolves caller-supplied `BIGQUERY:type` metadata against the type the Arrow field already
/// implies.
///
/// Only the GoogleSQL names the driver emits are accepted, so the metadata that comes out of a
/// query result is exactly the metadata that goes back into an ingest. `subject` names the input
/// surface.
pub(crate) fn metadata_type(
    field: &Field,
    name: &str,
    inferred: Option<BigQueryType>,
    subject: &str,
) -> Result<BigQueryType> {
    if let Some(googlesql) = googlesql_spelling_of(name) {
        return Err(invalid_argument(format!(
            "BigQuery type metadata {name:?} for {subject} {:?} is not a GoogleSQL type name; use \
             {googlesql:?}",
            field.name()
        )));
    }
    let Some(kind) = BigQueryType::from_sql(name) else {
        return Err(error(
            format!(
                "BigQuery type {name:?} is not implemented for {subject} {:?}",
                field.name()
            ),
            Status::NotImplemented,
        ));
    };
    // A bare `STRUCT` or `RANGE` on a scalar field is a mismatch like `STRING` on an `Int64`, not
    // an unimplemented type; composites are resolved by their own converters from the
    // parameterized spelling.
    if !kind.is_scalar() || !inferred.is_some_and(|inferred| kind.annotates(inferred)) {
        return Err(incompatible_metadata_error(field, name));
    }
    Ok(kind)
}

/// The item field of an Arrow list as the type converters read it: under the list's name, so a
/// refusal names the column the caller bound rather than `item` or `element`, and carrying the
/// element type the list's `ARRAY<X>` metadata names when the item names none of its own.
///
/// A `STRUCT<...>` element is not carried: nothing parses a struct's field list out of metadata,
/// so the item's Arrow type describes it. List metadata other than `ARRAY<...>` is refused.
pub(crate) fn array_item_field(list: &Field, item: &Field) -> Result<Field> {
    let element = match list.metadata().get(BIGQUERY_TYPE_METADATA_KEY) {
        Some(rich_type) => Some(
            rich_type
                .strip_prefix("ARRAY<")
                .and_then(|element| element.strip_suffix('>'))
                .ok_or_else(|| incompatible_metadata_error(list, rich_type))?,
        ),
        None => None,
    };
    let mut item = item.clone().with_name(list.name());
    if let Some(element) = element.filter(|element| !element.starts_with("STRUCT<"))
        && !item.metadata().contains_key(BIGQUERY_TYPE_METADATA_KEY)
    {
        item.metadata_mut()
            .insert(BIGQUERY_TYPE_METADATA_KEY.to_owned(), element.to_owned());
    }
    Ok(item)
}

/// The refusal for an Arrow list whose items convert to arrays, which BigQuery has no type for.
pub(crate) fn nested_array_error(list: &Field) -> adbc_core::error::Error {
    not_implemented(format!(
        "Arrow field {:?} is a list of lists, but a BigQuery array cannot contain arrays \
         directly; wrap each inner list in a STRUCT",
        list.name()
    ))
}

/// A RANGE element type: DATE, DATETIME, and TIMESTAMP are the only ones BigQuery supports, and
/// a dedicated enum keeps every match exhaustive over exactly those three.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeElement {
    Date,
    DateTime,
    Timestamp,
}

impl RangeElement {
    const fn bigquery_type(self) -> BigQueryType {
        match self {
            Self::Date => BigQueryType::Date,
            Self::DateTime => BigQueryType::DateTime,
            Self::Timestamp => BigQueryType::Timestamp,
        }
    }

    /// The GoogleSQL element name, exactly as `RANGE<...>` metadata spells it.
    pub(crate) const fn sql(self) -> &'static str {
        self.bigquery_type().sql()
    }

    /// The REST `range_element_type` name.
    pub(crate) const fn rest(self) -> &'static str {
        self.bigquery_type().rest()
    }

    /// Parses a REST `range_element_type` name, in either namespace spelling.
    pub(crate) fn from_rest(name: &str) -> Option<Self> {
        match BigQueryType::from_rest(name)? {
            BigQueryType::Date => Some(Self::Date),
            BigQueryType::DateTime => Some(Self::DateTime),
            BigQueryType::Timestamp => Some(Self::Timestamp),
            _ => None,
        }
    }

    /// The canonical Arrow type of a range bound.
    pub(crate) fn canonical_arrow_type(self) -> DataType {
        self.bigquery_type().canonical_data_type().expect(
            "DATE, DATETIME and TIMESTAMP are scalar types, which all have a canonical Arrow type",
        )
    }
}

/// A field's `BIGQUERY:type` metadata when it names a parameterized RANGE. Matching the opening
/// angle bracket keeps a future type named `RANGEx` from being read as a range.
pub(crate) fn range_metadata(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get(BIGQUERY_TYPE_METADATA_KEY)
        .map(String::as_str)
        .filter(|rich_type| rich_type.starts_with("RANGE<"))
}

/// The element type of a RANGE struct field, which its `RANGE<...>` metadata names.
///
/// `Ok(None)` when the metadata names no RANGE; an error when it does but the struct is not
/// exactly `start` and `end` bounds whose Arrow types both map to the element type. Converting
/// the bound values to that element type is left to the caller.
pub(crate) fn range_field_element(
    field: &Field,
    fields: &Fields,
    subject: &str,
) -> Result<Option<RangeElement>> {
    let Some(element) = range_element_from_metadata(field, subject)? else {
        return Ok(None);
    };
    let holds_element =
        |bound: &Field| inferred_scalar_type(bound.data_type()) == Some(element.bigquery_type());
    let fields_valid = matches!(
        &fields[..],
        [start, end]
            if start.name() == "start"
                && end.name() == "end"
                && holds_element(start)
                && holds_element(end)
    );
    if !fields_valid {
        return Err(invalid_argument(format!(
            "BigQuery RANGE<{element}> {subject} {:?} must be an Arrow struct of exactly two \
             fields, start and end, each of an Arrow {element} type such as {}",
            field.name(),
            element.canonical_arrow_type(),
            element = element.sql(),
        )));
    }
    Ok(Some(element))
}

/// The element type named by a field's `RANGE<...>` metadata.
///
/// `Ok(None)` when the field carries no `BIGQUERY:type` metadata or the metadata names a
/// non-RANGE type; an error when the metadata is RANGE-shaped but malformed, or names an element
/// type BigQuery ranges do not support. The element is checked before the struct shape, so an
/// unsupported element is never reported as a shape problem.
fn range_element_from_metadata(field: &Field, subject: &str) -> Result<Option<RangeElement>> {
    let Some(rich_type) = field.metadata().get(BIGQUERY_TYPE_METADATA_KEY) else {
        return Ok(None);
    };
    // Looser than [`range_metadata`], so malformed range metadata such as `RANGE` or `RANGE<DATE`
    // is reported as a bad range rather than treated as some other type.
    if !rich_type.starts_with("RANGE") {
        return Ok(None);
    }
    let element_type = range_metadata(field)
        .and_then(|rich_type| rich_type.strip_prefix("RANGE<"))
        .and_then(|value| value.strip_suffix('>'))
        .ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery RANGE type metadata {rich_type:?} is invalid for {subject} {:?}",
                field.name()
            ))
        })?;
    // The metadata is GoogleSQL, and DATE, DATETIME and TIMESTAMP spell themselves identically
    // in both namespaces, so the REST parser accepts exactly the GoogleSQL element names here.
    RangeElement::from_rest(element_type)
        .map(Some)
        .ok_or_else(|| {
            invalid_argument(format!(
                "BigQuery RANGE element type {element_type:?} is invalid for {subject} {:?}; use \
                 DATE, DATETIME, or TIMESTAMP",
                field.name()
            ))
        })
}

/// Resolves a non-decimal scalar Arrow type to its BigQuery type, honouring compatible
/// `BIGQUERY:type` and Arrow extension metadata. `None` for decimals and composites, which their
/// callers resolve.
///
/// Query parameters, bulk ingest and CSV seeds share this mapping and the composite rules in this
/// module. They differ only where a parameter is one value and a column a declared domain:
///
/// - `UInt64` is INT64 here. A parameter checks its one value against INT64's range, while ingest
///   refuses the column, since INT64 would narrow the domain the caller declared.
/// - `Null` is a parameter's typed NULL (`STRING` unless annotated). An appended column takes the
///   type of the table column it fills, and any other is refused.
/// - `Duration` is INTERVAL for parameters and ingest, which convert each value. A CSV seed
///   refuses it unannotated, since BigQuery, not the driver, parses a seed's values.
pub(crate) fn scalar_arrow_field_type(
    data_type: &DataType,
    field: &Field,
    subject: &str,
) -> Result<Option<BigQueryType>> {
    let Some(inferred) = inferred_scalar_type(data_type) else {
        return Ok(None);
    };
    // Only an Arrow string can carry a BigQuery type its physical type does not imply, and only
    // when no `BIGQUERY:type` metadata already says what it is.
    if inferred == BigQueryType::String
        && !field.metadata().contains_key(BIGQUERY_TYPE_METADATA_KEY)
        && let Some(kind) = extension_type(field)
    {
        return Ok(Some(kind));
    }
    match field.metadata().get(BIGQUERY_TYPE_METADATA_KEY) {
        Some(name) => metadata_type(field, name, Some(inferred), subject).map(Some),
        None => Ok(Some(inferred)),
    }
}

/// The BigQuery type an Arrow physical type implies on its own, before any `BIGQUERY:type` or
/// extension metadata refines it.
///
/// `None` for every type whose BigQuery mapping needs more than the physical type: the decimals,
/// whose precision and scale participate; the composites, which are parameterized by their
/// elements; and the Arrow types BigQuery has no type for.
fn inferred_scalar_type(data_type: &DataType) -> Option<BigQueryType> {
    Some(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => BigQueryType::String,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => BigQueryType::Bytes,
        DataType::Boolean => BigQueryType::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => BigQueryType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => BigQueryType::Float64,
        DataType::Date32 | DataType::Date64 => BigQueryType::Date,
        // Arrow defines `Time32` only in seconds and milliseconds and `Time64` only in micro- and
        // nanoseconds. Another unit is an invalid type, refused here rather than per value.
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => BigQueryType::Time,
        DataType::Timestamp(_, timezone) => {
            if timestamp_is_zoned(timezone.as_deref()) {
                BigQueryType::Timestamp
            } else {
                BigQueryType::DateTime
            }
        }
        DataType::Interval(_) => BigQueryType::Interval,
        _ => return None,
    })
}

/// Whether an Arrow timestamp's timezone makes it a BigQuery TIMESTAMP rather than a DATETIME.
///
/// Used in both directions: for the BigQuery type an Arrow timestamp is written as, and for
/// whether the REST decoder reads epoch microseconds or a civil datetime. An empty timezone is no
/// zone, so `Timestamp(_, Some(""))` is a DATETIME like `Timestamp(_, None)`.
pub(crate) fn timestamp_is_zoned(timezone: Option<&str>) -> bool {
    timezone.is_some_and(|timezone| !timezone.is_empty())
}

/// The Arrow layout a scalar column is normalized to: the canonical layout of the BigQuery type
/// its physical Arrow type implies, which is also the layout a query result decodes into.
pub(crate) fn canonical_scalar_data_type(data_type: &DataType) -> Option<DataType> {
    inferred_scalar_type(data_type)?.canonical_data_type()
}

/// The decimal type a field's `BIGQUERY:type` metadata asks for, with errors naming `subject`
/// ([`INGEST_FIELD`] or [`PARAMETER_FIELD`]).
pub(crate) fn decimal_metadata_type_hint(
    data_type: &DataType,
    field: &Field,
    inherited: Option<BigQueryDecimalType>,
    subject: &str,
) -> Result<Option<BigQueryDecimalType>> {
    decimal_type_hint(
        data_type,
        field
            .metadata()
            .get(BIGQUERY_TYPE_METADATA_KEY)
            .map(String::as_str),
        inherited,
    )
    .map_err(|reason| {
        invalid_argument(format!(
            "Arrow {subject} {:?} has invalid decimal metadata: {reason}",
            field.name()
        ))
    })
}

/// The error for composite `BIGQUERY:type` metadata that cannot describe the Arrow field it
/// annotates.
pub(crate) fn incompatible_metadata_error(field: &Field, name: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery type metadata {name:?} is incompatible with Arrow field {:?} of type {}",
        field.name(),
        field.data_type()
    ))
}

/// The GoogleSQL name to use instead of `name`, when `name` is a spelling BigQuery understands but
/// this driver never emits, such as `FLOAT`, `RECORD` or `DECIMAL`.
pub(crate) fn googlesql_spelling_of(name: &str) -> Option<&'static str> {
    if BigQueryType::from_sql(name).is_some() {
        return None;
    }
    BigQueryType::from_rest(name).map(BigQueryType::sql)
}

#[cfg(test)]
mod tests;
