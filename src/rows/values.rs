//! REST JSON rows, decoded into Arrow record batches against the converted schema.
//!
//! One [`ColumnBuilder`] per result column decodes its cells, dispatching on the Arrow field that
//! [`crate::types::schema`] produced; the REST schema is not consulted again.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float64Builder, Int64Builder, IntervalMonthDayNanoBuilder, PrimitiveBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::{ArrowPrimitiveType, Date32Type, TimestampMicrosecondType};
use arrow_array::{ArrayRef, ListArray, RecordBatch, RecordBatchOptions, StringArray, StructArray};
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBufferBuilder, i256};
use arrow_schema::{ArrowError, DataType, Field, Fields, IntervalUnit, SchemaRef, TimeUnit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::pipeline::ResultPipelineCancellation;
use crate::types::decimal::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, NUMERIC_PRECISION, NUMERIC_SCALE,
};
use crate::types::{ValueAt, range_metadata, timestamp_is_zoned};

use super::scalars::{
    invalid_scalar, parse_date, parse_datetime, parse_interval, parse_scaled_decimal, parse_signed,
    parse_time_of_day, parse_timestamp,
};
use super::{RestRow, RestValue};

/// How many array elements one cancellation poll covers. An array is the only part of a row the
/// schema does not bound, so it is the only place polled below the per-row check; polling every
/// element would cost more than appending it.
const CANCELLATION_STRIDE: usize = 64;

/// How many elements one row of an ARRAY column is guessed to hold when sizing its child
/// builder: small enough that an all-empty array column wastes little, large enough to skip the
/// first few growth doublings.
const ESTIMATED_ARRAY_LENGTH: usize = 4;

/// The cap on that guess for one nested builder, since it compounds once per nesting level.
const MAX_ESTIMATED_ELEMENTS: usize = 16_384;

/// The capacity a list's child builder is sized with: [`ESTIMATED_ARRAY_LENGTH`] per row, capped
/// at [`MAX_ESTIMATED_ELEMENTS`], but never below one element per row.
fn estimated_child_capacity(capacity: usize) -> usize {
    capacity
        .saturating_mul(ESTIMATED_ARRAY_LENGTH)
        .min(MAX_ESTIMATED_ELEMENTS.max(capacity))
}

/// Converts compact, positional REST rows into one Arrow record batch.
///
/// Values stay positional from the JSON decoder through Arrow construction, preserving duplicate
/// and anonymous nested fields without rebuilding name-keyed maps. `first_row` is the result row
/// number of `rows[0]`, so a failure names the row within the whole result, not within the batch.
pub(crate) fn rest_rows_to_record_batch(
    rows: Vec<RestRow>,
    first_row: usize,
    schema: SchemaRef,
    cancellation: &ResultPipelineCancellation,
) -> Result<Option<RecordBatch>, ArrowError> {
    let capacity = rows.len();
    let fields = schema.fields();
    let mut builders = fields
        .iter()
        .map(|field| ColumnBuilder::new(field, capacity))
        .collect::<Result<Vec<_>, ArrowError>>()?;
    for (row, cells) in (first_row..).zip(rows) {
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        if cells.len() != builders.len() {
            return Err(row_width_error(row, builders.len(), cells.len()));
        }
        for ((builder, field), value) in builders.iter_mut().zip(fields).zip(cells.into_cells()) {
            if let Err(source) = append_cell(builder, field, &value, row, cancellation) {
                if cancellation.is_cancelled() {
                    return Ok(None);
                }
                return Err(source);
            }
        }
    }
    let arrays = builders
        .into_iter()
        .zip(fields)
        .map(|(builder, field)| {
            builder.finish().map_err(|source| {
                let column = ColumnAt {
                    name: field.name(),
                    row: None,
                };
                value_error(&ValueAt::Root(&column), &source)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(capacity));
    RecordBatch::try_new_with_options(schema, arrays, &options).map(Some)
}

/// Appends one cell of a result row to its column.
///
/// A scalar cell, the common case, goes straight to its [`LeafColumn`]: the composite arms of
/// [`ColumnBuilder::append`] make that a far heavier call. Arrow would refuse a NULL in a REQUIRED
/// column only when assembling the batch, without the row.
fn append_cell(
    builder: &mut ColumnBuilder,
    field: &Field,
    value: &RestValue,
    row: usize,
    cancellation: &ResultPipelineCancellation,
) -> Result<(), ArrowError> {
    let column = ColumnAt {
        name: field.name(),
        row: Some(row),
    };
    let at = ValueAt::Root(&column);
    if value.is_null() && !field.is_nullable() {
        return Err(value_error(&at, &required_null()));
    }
    match builder {
        ColumnBuilder::Leaf(leaf) if value.is_null() => {
            leaf.append_null();
            Ok(())
        }
        ColumnBuilder::Leaf(leaf) => leaf
            .append_value(value)
            .map_err(|source| value_error(&at, &source)),
        composite => composite.append(value, &at, cancellation),
    }
}

/// A column each of whose values is one unnested REST cell: every type but ARRAY and STRUCT.
///
/// A row's cell reaches it through one small virtual call. An array's elements arrive as one run,
/// which [`Self::append_run`] appends in a loop monomorphized per column type.
trait LeafColumn {
    /// Appends one non-null value.
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError>;
    fn append_null(&mut self);
    fn finish(&mut self) -> Result<ArrayRef, ArrowError>;

    /// Appends a run of values, failing with the index of the first one that cannot be decoded.
    fn append_run(&mut self, values: &[RestValue]) -> Result<(), (usize, ArrowError)> {
        for (index, value) in values.iter().enumerate() {
            if value.is_null() {
                self.append_null();
            } else {
                self.append_value(value).map_err(|source| (index, source))?;
            }
        }
        Ok(())
    }
}

/// A primitive column and how one cell becomes its value.
struct Primitive<T: ArrowPrimitiveType, P> {
    builder: PrimitiveBuilder<T>,
    parse: P,
}

impl<T, P> LeafColumn for Primitive<T, P>
where
    T: ArrowPrimitiveType,
    P: Fn(&RestValue) -> Result<T::Native, ArrowError>,
{
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError> {
        self.builder.append_value((self.parse)(value)?);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(self.builder.finish()))
    }
}

fn primitive<T: ArrowPrimitiveType>(
    builder: PrimitiveBuilder<T>,
    parse: impl Fn(&RestValue) -> Result<T::Native, ArrowError> + 'static,
) -> Box<dyn LeafColumn> {
    Box::new(Primitive { builder, parse })
}

impl LeafColumn for BooleanBuilder {
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError> {
        Self::append_value(self, boolean_value(value)?);
        Ok(())
    }

    fn append_null(&mut self) {
        Self::append_null(self);
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(Self::finish(self)))
    }
}

/// A STRING, JSON or GEOGRAPHY column, built from the cells' bytes and checked as UTF-8 once at
/// `finish`: checking each short cell costs more than its copy.
struct Utf8Column(BinaryBuilder);

impl LeafColumn for Utf8Column {
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError> {
        let text = bytes_value(value, "STRING")?;
        check_offset_room(self.0.values_slice().len(), text.len(), "STRING")?;
        self.0.append_value(text);
        Ok(())
    }

    fn append_null(&mut self) {
        self.0.append_null();
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        StringArray::try_from_binary(self.0.finish())
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|source| malformed_column(&source))
    }
}

/// A BYTES column. Cells decode into a scratch buffer the column keeps, so the column costs one
/// allocation rather than one per cell.
struct BytesColumn {
    builder: BinaryBuilder,
    scratch: Vec<u8>,
}

impl LeafColumn for BytesColumn {
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError> {
        let encoded = bytes_value(value, "BYTES")?;
        self.scratch.clear();
        BASE64
            .decode_vec(encoded, &mut self.scratch)
            .map_err(|_| invalid_scalar("BYTES encoded as base64"))?;
        check_offset_room(
            self.builder.values_slice().len(),
            self.scratch.len(),
            "BYTES",
        )?;
        self.builder.append_value(&self.scratch);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(self.builder.finish()))
    }
}

/// A RANGE column: its `start` and `end` builders, and the parser for its bounds, which is all
/// that differs between element types.
struct RangeColumn<T: ArrowPrimitiveType> {
    fields: Fields,
    start: PrimitiveBuilder<T>,
    end: PrimitiveBuilder<T>,
    nulls: NullBufferBuilder,
    parse: fn(&[u8]) -> Result<T::Native, ArrowError>,
}

impl<T: ArrowPrimitiveType> RangeColumn<T> {
    /// `bound_type` is the Arrow type of the `start` and `end` fields, so it is `T`'s by
    /// construction and carries the timezone that tells a TIMESTAMP bound from a DATETIME one.
    fn new(
        fields: &Fields,
        bound_type: &DataType,
        capacity: usize,
        parse: fn(&[u8]) -> Result<T::Native, ArrowError>,
    ) -> Self {
        let bound =
            || PrimitiveBuilder::<T>::with_capacity(capacity).with_data_type(bound_type.clone());
        Self {
            fields: fields.clone(),
            start: bound(),
            end: bound(),
            nulls: NullBufferBuilder::new(capacity),
            parse,
        }
    }
}

impl<T: ArrowPrimitiveType> LeafColumn for RangeColumn<T> {
    /// Appends the two bounds of one `[start, end)` RANGE literal, parsed exactly as the scalar
    /// column of the element type is.
    fn append_value(&mut self, value: &RestValue) -> Result<(), ArrowError> {
        let (start, end) = split_range(string_value(value, "RANGE")?)?;
        let parse = |bound: &str| (self.parse)(bound.as_bytes());
        self.start.append_option(start.map(parse).transpose()?);
        self.end.append_option(end.map(parse).transpose()?);
        self.nulls.append_non_null();
        Ok(())
    }

    fn append_null(&mut self) {
        self.start.append_null();
        self.end.append_null();
        self.nulls.append_null();
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let bounds: Vec<ArrayRef> =
            vec![Arc::new(self.start.finish()), Arc::new(self.end.finish())];
        finish_struct(self.fields.clone(), bounds, self.nulls.finish())
    }
}

/// Splits a `[start, end)` RANGE literal into its bounds, `None` for an unbounded one: BigQuery
/// spells that `UNBOUNDED`, and an empty bound is accepted as the same, as upstream's parser does.
fn split_range(literal: &str) -> Result<(Option<&str>, Option<&str>), ArrowError> {
    fn bound(text: &str) -> Option<&str> {
        let text = text.trim();
        (!text.is_empty() && text != "UNBOUNDED").then_some(text)
    }
    literal
        .trim()
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(')'))
        .and_then(|inner| inner.split_once(','))
        .map(|(start, end)| (bound(start), bound(end)))
        .ok_or_else(|| invalid_scalar("RANGE"))
}

enum ColumnBuilder {
    Leaf(Box<dyn LeafColumn>),
    List {
        item_field: Arc<Field>,
        offsets: OffsetBufferBuilder<i32>,
        values: Box<Self>,
    },
    Struct {
        fields: Fields,
        children: Vec<Self>,
        nulls: NullBufferBuilder,
    },
}

impl ColumnBuilder {
    /// Builds the decoder for one converted result field, dispatching on the field itself.
    ///
    /// Only shapes [`crate::types::schema`]'s field conversion produces are accepted; anything
    /// else is a defect in this module and is reported rather than decoded wrongly.
    fn new(field: &Field, capacity: usize) -> Result<Self, ArrowError> {
        let data_type = field.data_type();
        let leaf: Box<dyn LeafColumn> = match data_type {
            DataType::Utf8 => Box::new(Utf8Column(BinaryBuilder::with_capacity(
                capacity,
                16_usize.saturating_mul(capacity),
            ))),
            DataType::Binary => Box::new(BytesColumn {
                builder: BinaryBuilder::with_capacity(capacity, 16_usize.saturating_mul(capacity)),
                scratch: Vec::new(),
            }),
            DataType::Int64 => primitive(Int64Builder::with_capacity(capacity), integer_value),
            DataType::Float64 => primitive(Float64Builder::with_capacity(capacity), float_value),
            DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            DataType::Date32 => primitive(Date32Builder::with_capacity(capacity), |value| {
                parse_date(bytes_value(value, "DATE")?)
            }),
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive(Time64MicrosecondBuilder::with_capacity(capacity), |value| {
                    parse_time_of_day(bytes_value(value, "TIME")?)
                })
            }
            // A zoned timestamp is a BigQuery TIMESTAMP; an unzoned one is a DATETIME, which
            // arrives in a different REST spelling.
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                let builder = TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(data_type.clone());
                if timestamp_is_zoned(timezone.as_deref()) {
                    primitive(builder, |value| {
                        parse_timestamp(bytes_value(value, "TIMESTAMP")?)
                    })
                } else {
                    primitive(builder, |value| {
                        parse_datetime(bytes_value(value, "DATETIME")?)
                    })
                }
            }
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE) => primitive(
                Decimal128Builder::with_capacity(capacity).with_data_type(data_type.clone()),
                |value| parse_scaled_decimal::<i128>(bytes_value(value, "NUMERIC")?),
            ),
            DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE) => primitive(
                Decimal256Builder::with_capacity(capacity).with_data_type(data_type.clone()),
                |value| parse_scaled_decimal::<i256>(bytes_value(value, "BIGNUMERIC")?),
            ),
            DataType::Interval(IntervalUnit::MonthDayNano) => primitive(
                IntervalMonthDayNanoBuilder::with_capacity(capacity),
                |value| parse_interval(bytes_value(value, "INTERVAL")?),
            ),
            DataType::Struct(fields) => return Self::new_struct(field, fields, capacity),
            DataType::List(item_field) => {
                return Ok(Self::List {
                    item_field: item_field.clone(),
                    offsets: OffsetBufferBuilder::new(capacity),
                    values: Box::new(Self::new(item_field, estimated_child_capacity(capacity))?),
                });
            }
            _ => return Err(unsupported_column(field)),
        };
        Ok(Self::Leaf(leaf))
    }

    /// A struct field is either a BigQuery RANGE, marked by its `BIGQUERY:type` metadata and
    /// decoded from a `[start, end)` literal, or a STRUCT decoded from a JSON object.
    fn new_struct(field: &Field, fields: &Fields, capacity: usize) -> Result<Self, ArrowError> {
        if range_metadata(field).is_none() {
            let children = fields
                .iter()
                .map(|child| Self::new(child, capacity))
                .collect::<Result<Vec<_>, ArrowError>>()?;
            return Ok(Self::Struct {
                fields: fields.clone(),
                children,
                nulls: NullBufferBuilder::new(capacity),
            });
        }
        let range: Box<dyn LeafColumn> = match fields.first().map(|bound| bound.data_type()) {
            Some(bound @ DataType::Date32) => Box::new(RangeColumn::<Date32Type>::new(
                fields, bound, capacity, parse_date,
            )),
            Some(bound @ DataType::Timestamp(TimeUnit::Microsecond, timezone)) => {
                let parse = if timestamp_is_zoned(timezone.as_deref()) {
                    parse_timestamp
                } else {
                    parse_datetime
                };
                Box::new(RangeColumn::<TimestampMicrosecondType>::new(
                    fields, bound, capacity, parse,
                ))
            }
            _ => return Err(unsupported_column(field)),
        };
        Ok(Self::Leaf(range))
    }

    /// Appends the elements of the array at `at`.
    ///
    /// An array is the only part of a row the schema does not bound, so cancellation is polled
    /// once per [`CANCELLATION_STRIDE`] elements; the batch converter polls once per row.
    fn append_elements(
        &mut self,
        elements: &[RestValue],
        at: &ValueAt<'_>,
        cancellation: &ResultPipelineCancellation,
    ) -> Result<(), ArrowError> {
        for (chunk_index, chunk) in elements.chunks(CANCELLATION_STRIDE).enumerate() {
            if cancellation.is_cancelled() {
                return Err(ArrowError::ComputeError(
                    "BigQuery result conversion was cancelled".to_owned(),
                ));
            }
            let first = chunk_index * CANCELLATION_STRIDE;
            if let Self::Leaf(leaf) = self {
                leaf.append_run(chunk)
                    .map_err(|(index, source)| value_error(&at.element(first + index), &source))?;
            } else {
                for (index, value) in chunk.iter().enumerate() {
                    self.append(value, &at.element(first + index), cancellation)?;
                }
            }
        }
        Ok(())
    }

    /// Appends one REST value.
    fn append(
        &mut self,
        value: &RestValue,
        at: &ValueAt<'_>,
        cancellation: &ResultPipelineCancellation,
    ) -> Result<(), ArrowError> {
        if value.is_null() {
            self.append_null();
            return Ok(());
        }

        // Every failure below is raised where it happens, so `at` already names the whole path
        // down to it; a child's failure travels up already located and is never wrapped twice.
        let located = |source: ArrowError| value_error(at, &source);
        match self {
            Self::Leaf(leaf) => leaf.append_value(value).map_err(located)?,
            Self::List {
                offsets, values, ..
            } => {
                let RestValue::Repeated(items) = value else {
                    return Err(located(type_mismatch("array", value)));
                };
                // `OffsetBufferBuilder::finish` panics once the running element count leaves
                // `i32`, and it derefs to the offsets it has pushed so far.
                let appended = offsets.last().copied().unwrap_or_default();
                // An Arrow offset is never negative; reading a negative one as full fails closed.
                let appended = usize::try_from(appended).unwrap_or(usize::MAX);
                check_offset_room(appended, items.len(), "ARRAY").map_err(located)?;
                values.append_elements(items, at, cancellation)?;
                offsets.push_length(items.len());
            }
            Self::Struct {
                fields,
                children,
                nulls,
            } => {
                let RestValue::Record(items) = value else {
                    return Err(located(type_mismatch("object", value)));
                };
                if items.len() != children.len() {
                    return Err(located(record_width_error(children.len(), items.len())));
                }
                for ((child, field), item) in children.iter_mut().zip(fields.iter()).zip(items) {
                    let at = at.member(field.name());
                    if item.is_null() && !field.is_nullable() {
                        return Err(value_error(&at, &required_null()));
                    }
                    child.append(item, &at, cancellation)?;
                }
                nulls.append_non_null();
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Leaf(leaf) => leaf.append_null(),
            // BigQuery arrays themselves are never NULL: a NULL array expression is returned as
            // an empty array. Normalize the same shape defensively if a REST-compatible endpoint
            // sends a literal JSON null instead of BigQuery's canonical `[]`.
            Self::List { offsets, .. } => offsets.push_length(0),
            Self::Struct {
                children, nulls, ..
            } => {
                for child in children {
                    child.append_null();
                }
                nulls.append_null();
            }
        }
    }

    /// Assembles the column. Arrow's checked constructors stay in use, so a shape the decoding
    /// above failed to refuse is an error rather than a panic across the FFI boundary.
    fn finish(self) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Leaf(mut leaf) => leaf.finish(),
            Self::List {
                item_field,
                offsets,
                values,
            } => ListArray::try_new(item_field, offsets.finish(), values.finish()?, None)
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|source| malformed_column(&source)),
            Self::Struct {
                fields,
                children,
                mut nulls,
            } => {
                let columns = children
                    .into_iter()
                    .map(Self::finish)
                    .collect::<Result<Vec<_>, _>>()?;
                finish_struct(fields, columns, nulls.finish())
            }
        }
    }
}

fn finish_struct(
    fields: Fields,
    columns: Vec<ArrayRef>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    StructArray::try_new(fields, columns, nulls)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|source| malformed_column(&source))
}

/// A NULL where the result schema declares a REQUIRED column or STRUCT field; the caller locates
/// it.
#[cold]
fn required_null() -> ArrowError {
    ArrowError::ParseError("BigQuery returned a NULL for a REQUIRED field".to_owned())
}

/// Reports a column the response's values cannot be assembled into. A NULL in a REQUIRED field is
/// refused where it arrives, so this is only the assembly's own refusal, carried rather than
/// restated.
fn malformed_column(source: &ArrowError) -> ArrowError {
    ArrowError::ParseError(format!(
        "BigQuery returned values the result column's declared shape cannot hold: {source}"
    ))
}

/// The bytes of a string cell, which every column but FLOAT64 and RANGE decodes from.
fn bytes_value<'a>(value: &'a RestValue, kind: &str) -> Result<&'a [u8], ArrowError> {
    value
        .as_bytes()
        .ok_or_else(|| type_mismatch(&format!("string for {kind}"), value))
}

fn string_value<'a>(value: &'a RestValue, kind: &str) -> Result<&'a str, ArrowError> {
    value
        .as_str()
        .ok_or_else(|| type_mismatch(&format!("string for {kind}"), value))
}

fn integer_value(value: &RestValue) -> Result<i64, ArrowError> {
    match *value {
        RestValue::Integer(value) => Ok(value),
        RestValue::Unsigned(value) => i64::try_from(value).map_err(|_| invalid_scalar("INT64")),
        // A JSON number that is not an integer is a value failure rather than a shape one:
        // `type_mismatch` would report that the column wanted a number and was handed one.
        RestValue::Float(_) => Err(invalid_scalar("INT64")),
        // BigQuery spells every INT64 cell as a JSON string, so this is the common arm.
        // `str::parse` would reach `from_ascii_radix`, an out-of-line generic over the radix.
        RestValue::String(_) | RestValue::ShortString { .. } => {
            parse_signed(bytes_value(value, "INT64")?).ok_or_else(|| invalid_scalar("INT64"))
        }
        _ => Err(type_mismatch("number or string for INT64", value)),
    }
}

fn float_value(value: &RestValue) -> Result<f64, ArrowError> {
    match *value {
        RestValue::Integer(value) => Ok(value as f64),
        RestValue::Unsigned(value) => Ok(value as f64),
        RestValue::Float(value) => Ok(value),
        RestValue::String(_) | RestValue::ShortString { .. } => string_value(value, "FLOAT64")?
            .parse()
            .map_err(|_| invalid_scalar("FLOAT64")),
        _ => Err(type_mismatch("number or string for FLOAT64", value)),
    }
}

fn boolean_value(value: &RestValue) -> Result<bool, ArrowError> {
    match *value {
        RestValue::Boolean(value) => Ok(value),
        RestValue::String(_) | RestValue::ShortString { .. } => match bytes_value(value, "BOOL")? {
            b"true" => Ok(true),
            b"false" => Ok(false),
            _ => Err(invalid_scalar("BOOL")),
        },
        _ => Err(type_mismatch("boolean or string for BOOL", value)),
    }
}

/// The result column a decode failure came from, and the row being decoded, as the root of the
/// [`ValueAt`] path an error names. `finish` fails over a whole column, so it has no row.
struct ColumnAt<'a> {
    name: &'a str,
    row: Option<usize>,
}

impl std::fmt::Display for ColumnAt<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "failed to decode BigQuery result column {:?}",
            self.name
        )?;
        match self.row {
            Some(row) => write!(formatter, ", row {row}"),
            None => Ok(()),
        }
    }
}

/// Attaches where a decode failure happened (column, row and nested path) to its message.
///
/// A `ParseError` source contributes its bare message, so the result's own `Parser error: `
/// prefix is not repeated mid-message.
#[cold]
#[inline(never)]
fn value_error(at: &ValueAt<'_>, source: &ArrowError) -> ArrowError {
    match source {
        ArrowError::ParseError(message) => ArrowError::ParseError(format!("{at}: {message}")),
        source => ArrowError::ParseError(format!("{at}: {source}")),
    }
}

/// Reports a REST value whose JSON shape cannot be decoded against its column, by its JSON kind
/// only: the value is server-supplied and unbounded (one ARRAY cell can be megabytes), and the
/// kind is the whole diagnosis of a schema/row disagreement.
#[cold]
#[inline(never)]
fn type_mismatch(expected: &str, value: &RestValue) -> ArrowError {
    ArrowError::ParseError(format!(
        "BigQuery result value is {}, but its column is decoded from a JSON {expected}",
        value.kind()
    ))
}

fn row_width_error(row: usize, expected: usize, actual: usize) -> ArrowError {
    ArrowError::ParseError(format!(
        "BigQuery REST result row {row} has {actual} cells, but its schema has {expected} fields"
    ))
}

fn record_width_error(expected: usize, actual: usize) -> ArrowError {
    ArrowError::ParseError(format!(
        "BigQuery REST record has {actual} cells, but its schema has {expected} fields"
    ))
}

/// Refuses an append that would push a 32-bit Arrow offset past `i32::MAX`, where Arrow's
/// `GenericByteBuilder::append_value` and `OffsetBufferBuilder::<i32>::finish` panic with no
/// fallible form. `BATCH_SIZE` makes that pathological, not impossible.
fn check_offset_room(appended: usize, additional: usize, kind: &str) -> Result<(), ArrowError> {
    if appended
        .checked_add(additional)
        .is_none_or(|total| i32::try_from(total).is_err())
    {
        return Err(ArrowError::ParseError(format!(
            "BigQuery {kind} column exceeds the 32-bit Arrow offsets of one result batch"
        )));
    }
    Ok(())
}

fn unsupported_column(field: &Field) -> ArrowError {
    ArrowError::SchemaError(format!(
        "BigQuery result field {:?} has no REST decoder for Arrow type {}",
        field.name(),
        field.data_type()
    ))
}

#[cfg(test)]
mod tests;
