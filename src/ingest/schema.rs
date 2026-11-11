//! The BigQuery table schema an ingest needs, and whether the table it found carries it.
//!
//! Converts a caller's Arrow schema into the BigQuery table schema the rows are written under, and
//! compares that with an existing table's schema. The comparison is semantic (BigQuery spells one
//! type several ways and defaults a mode), so both directions share one canonicalization.

use std::sync::Arc;

use adbc_core::constants::{
    ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_REPLACE, ADBC_INGEST_OPTION_TARGET_TABLE,
};
use adbc_core::error::{Result, Status};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema};

use crate::error::{error, invalid_argument, not_implemented, summarize_errors};
use crate::identifiers::write_quoted_identifier;
use crate::rest::metadata::TableId;
use crate::rest::model::{FieldElementType, TableFieldSchema, TableSchema};
use crate::types::decimal::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, BigQueryDecimalType, NUMERIC_PRECISION, NUMERIC_SCALE,
    bigquery_decimal_data_type, decimal_mapping, is_decimal_type,
};
use crate::types::{
    ARROW_EXTENSION_METADATA_KEY, ARROW_EXTENSION_NAME_KEY, BIGQUERY_TYPE_METADATA_KEY,
    BigQueryType, INGEST_FIELD, RangeElement, array_item_field, canonical_scalar_data_type,
    case_duplicate, decimal_metadata_type_hint, extension_overridden_by_metadata, folded_name,
    has_bigquery_extension, incompatible_metadata_error, nested_array_error, range_field_element,
    scalar_arrow_field_type, validate_schema_nesting,
};
use crate::{
    BIGQUERY_FIELD_METADATA_MAX_LENGTH, BIGQUERY_FIELD_METADATA_PRECISION,
    BIGQUERY_FIELD_METADATA_SCALE,
};

pub(super) fn normalize_schema(schema: &Schema) -> Result<Schema> {
    validate_schema_nesting(schema.fields(), "BigQuery ingest")?;
    let fields = schema
        .fields()
        .iter()
        .map(normalize_column_field)
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The Arrow schema Storage Write receives for rows of `normalized`: every decimal that is a
/// direct array element widened to its BigQuery type's full `(38, 9)` or `(76, 38)` shape.
///
/// Storage Write reads a `REPEATED` NUMERIC or BIGNUMERIC element at that full scale, whatever
/// scale its Arrow type declares (UP-89), so `0.01` sent as `Decimal128(10, 2)` would be stored as
/// `0.000000001`. A scalar or STRUCT field keeps its declared scale. The table schema still comes
/// from `normalized`, so the column keeps its declared parameters.
pub(super) fn storage_write_schema(normalized: &Schema) -> Schema {
    let fields = normalized
        .fields()
        .iter()
        .map(|field| storage_write_field(field, false))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, normalized.metadata().clone())
}

fn storage_write_field(field: &Field, array_element: bool) -> Field {
    let data_type = match field.data_type() {
        DataType::List(item) => DataType::List(Arc::new(storage_write_field(item, true))),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| storage_write_field(field, false))
                .collect(),
        ),
        DataType::Decimal128(_, _) if array_element => {
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE)
        }
        DataType::Decimal256(_, _) if array_element => {
            DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE)
        }
        _ => return field.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// Normalizes a table column or struct field, whose name becomes a BigQuery column name.
fn normalize_column_field(field: &Arc<Field>) -> Result<Field> {
    validate_field_name(field.name())?;
    normalize_field_with_decimal_type(field, None)
}

/// Normalizes a field without checking its name. A list item's name is not a column name, and
/// Arrow writers spell it `item`, `element`, `$data$` or leave it empty.
fn normalize_field_with_decimal_type(
    field: &Field,
    inherited_decimal_type: Option<BigQueryDecimalType>,
) -> Result<Field> {
    let decimal_type = decimal_metadata_type_hint(
        field.data_type(),
        field,
        inherited_decimal_type,
        INGEST_FIELD,
    )?;
    let mut metadata = field.metadata().clone();
    let data_type = match declared_decimal_type(field, decimal_type)? {
        // The family goes on the field, since the declared shape alone can select the other.
        Some((family, data_type)) => {
            if !metadata.contains_key(BIGQUERY_TYPE_METADATA_KEY) {
                metadata.insert(
                    BIGQUERY_TYPE_METADATA_KEY.to_owned(),
                    family.name().to_owned(),
                );
            }
            data_type
        }
        None => normalized_data_type(field.data_type(), field, decimal_type)?,
    };
    // An extension annotation is a contract on its storage type, and Storage Write refuses a
    // known one, such as `arrow.uuid`, on storage that breaks it, or an `arrow.json` one on the
    // STRING column that `BIGQUERY:type` metadata chose instead.
    if (&data_type != field.data_type() && !has_bigquery_extension(field))
        || extension_overridden_by_metadata(field)
    {
        metadata.remove(ARROW_EXTENSION_NAME_KEY);
        metadata.remove(ARROW_EXTENSION_METADATA_KEY);
    }
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

/// The column type that `BIGQUERY:precision` and `BIGQUERY:scale` metadata declare for a decimal
/// field, as `GetTableSchema` reports a parameterized NUMERIC or BIGNUMERIC column under its
/// family's full Arrow type. The field's values are converted to it exactly, so the shape
/// survives a read and an ingest back. The family is the requested one, else the narrowest that
/// holds the shape.
fn declared_decimal_type(
    field: &Field,
    requested: Option<BigQueryDecimalType>,
) -> Result<Option<(BigQueryDecimalType, DataType)>> {
    let metadata = field.metadata();
    let Some(precision) = metadata.get(BIGQUERY_FIELD_METADATA_PRECISION) else {
        return Ok(None);
    };
    if !is_decimal_type(field.data_type()) {
        return Ok(None);
    }
    let invalid = |reason: &str| {
        invalid_argument(format!(
            "Arrow ingest field {:?} has invalid {BIGQUERY_FIELD_METADATA_PRECISION} and \
             {BIGQUERY_FIELD_METADATA_SCALE} metadata: {reason}",
            field.name()
        ))
    };
    let scale = metadata
        .get(BIGQUERY_FIELD_METADATA_SCALE)
        .map_or("0", String::as_str);
    let (Ok(precision), Ok(scale)) = (precision.parse::<i64>(), scale.parse::<i64>()) else {
        return Err(invalid("both must be integers"));
    };
    let family = requested.unwrap_or_else(|| {
        if bigquery_decimal_data_type(BigQueryDecimalType::Numeric, precision, scale).is_ok() {
            BigQueryDecimalType::Numeric
        } else {
            BigQueryDecimalType::BigNumeric
        }
    });
    bigquery_decimal_data_type(family, precision, scale)
        .map(|data_type| Some((family, data_type)))
        .map_err(|reason| invalid(&reason))
}

/// Normalizes an array's item field as [`array_item_field`] reads it, with the list field's other
/// metadata that describes its elements, and gives it its own name back.
///
/// The element type matters beyond the table schema: a `RANGE<...>` one decides whether
/// [`crate::ingest::normalize`] packs a struct's bounds into BigQuery's civil-datetime encoding,
/// without which an `ARRAY<RANGE<DATETIME>>` column would be filled with raw epoch microseconds.
/// A maximum length is checked against each string or byte element.
fn normalize_array_item(
    item: &Field,
    decimal_type: Option<BigQueryDecimalType>,
    list: &Field,
) -> Result<Field> {
    let mut element = array_item_field(list, item)?;
    // `GetTableSchema` declares a decimal array's shape on the array column.
    if is_decimal_type(item.data_type())
        && !item
            .metadata()
            .contains_key(BIGQUERY_FIELD_METADATA_PRECISION)
    {
        for key in [
            BIGQUERY_FIELD_METADATA_PRECISION,
            BIGQUERY_FIELD_METADATA_SCALE,
        ] {
            if let Some(value) = list.metadata().get(key) {
                element.metadata_mut().insert(key.to_owned(), value.clone());
            }
        }
    }
    let mut normalized =
        normalize_field_with_decimal_type(&element, decimal_type)?.with_name(item.name());
    // Only onto an element that can carry a length: on any other the list-level metadata is
    // rejected by `apply_max_length`, which names the list field rather than its `item`.
    if let Some(max_length) = list.metadata().get(BIGQUERY_FIELD_METADATA_MAX_LENGTH)
        && matches!(normalized.data_type(), DataType::Utf8 | DataType::Binary)
    {
        let metadata = normalized.metadata_mut();
        if !metadata.contains_key(BIGQUERY_FIELD_METADATA_MAX_LENGTH) {
            metadata.insert(BIGQUERY_FIELD_METADATA_MAX_LENGTH, max_length.clone());
        }
    }
    Ok(normalized)
}

/// The canonical Arrow type `data_type` normalizes to. `field` is the field that carries it, for
/// its name and metadata; a dictionary's value type is normalized against the same field.
fn normalized_data_type(
    data_type: &DataType,
    field: &Field,
    decimal_type: Option<BigQueryDecimalType>,
) -> Result<DataType> {
    let name = field.name();
    let data_type = match data_type {
        DataType::UInt64 => return Err(uint64_not_implemented(name)),
        // Two ingest rules stand outside the shared scalar table: a year-month interval is exact
        // and the writer encodes it as is, so it keeps its layout rather than widening to
        // month-day-nano; and an Arrow duration, which has no BigQuery type, becomes INTERVAL.
        DataType::Interval(IntervalUnit::YearMonth) => data_type.clone(),
        DataType::Duration(_) => DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Dictionary(key_type, value_type) if key_type.is_dictionary_key_type() => {
            normalized_data_type(value_type, field, decimal_type)?
        }
        DataType::Dictionary(key_type, _) => {
            return Err(invalid_argument(format!(
                "Arrow dictionary ingest field {name:?} has key type {key_type}, which is not an \
                 integer"
            )));
        }
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => {
            DataType::List(Arc::new(normalize_array_item(item, decimal_type, field)?))
        }
        DataType::FixedSizeList(item, length) if *length >= 0 => {
            DataType::List(Arc::new(normalize_array_item(item, decimal_type, field)?))
        }
        DataType::FixedSizeList(_, length) => {
            return Err(invalid_argument(format!(
                "Arrow fixed-size list ingest field {name:?} has length {length}, which cannot be \
                 negative"
            )));
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(normalize_column_field)
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        // DuckDB and pyarrow maps; BigQuery's own idiom for one is an array of key-value pairs.
        DataType::Map(_, _) => {
            return Err(not_implemented(format!(
                "Arrow ingest field {name:?} is a map, which BigQuery has no type for; ingest it \
                 as a list of key and value structs, ARRAY<STRUCT<key, value>>"
            )));
        }
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => decimal_mapping(data_type, decimal_type)
            .map_err(invalid_argument)?
            .storage_data_type(),
        // Everything else BigQuery has a scalar type for normalizes to that type's canonical
        // Arrow layout, which is the same table a query result decodes into.
        data_type => match canonical_scalar_data_type(data_type) {
            Some(canonical) => canonical,
            // Only the table an append fills can type it (see `crate::ingest::align`).
            None if data_type == &DataType::Null => DataType::Null,
            None => {
                return Err(not_implemented(format!(
                    "Arrow type {data_type} of ingest field {name:?} is not implemented for \
                     BigQuery bulk ingest"
                )));
            }
        },
    };
    Ok(data_type)
}

/// Rejects a column name the ingest cannot write, naming the rule and the character that broke it.
fn validate_field_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(invalid_argument("BigQuery column name cannot be empty"));
    }
    match unwritable_name(name) {
        Some(reason) => Err(invalid_argument(format!(
            "BigQuery column name {name:?} {reason}"
        ))),
        None => Ok(()),
    }
}

/// Why Storage Write cannot write a column named `name`; `None` if it can, or if `name` is empty.
///
/// BigQuery tables also take flexible column names (spaces, dashes, a leading digit, other
/// scripts), but Storage Write refuses every append to a table that has one, at any depth, even
/// an append that leaves that column out (UP-96). It applies the original rule: an ASCII letter
/// or underscore, then ASCII letters, digits and underscores, at most 300 characters.
fn unwritable_name(name: &str) -> Option<String> {
    let mut characters = name.chars();
    let first = characters.next()?;
    if first != '_' && !first.is_ascii_alphabetic() {
        return Some(format!(
            "starts with {:?}, but Storage Write needs a column name that starts with an ASCII \
             letter or underscore",
            first.to_string()
        ));
    }
    if let Some(character) =
        characters.find(|character| *character != '_' && !character.is_ascii_alphanumeric())
    {
        return Some(format!(
            "contains {:?}, but Storage Write accepts only ASCII letters, digits, and \
             underscores in a column name",
            character.to_string()
        ));
    }
    // Every accepted character is one ASCII byte, so the byte length is the character count.
    (name.len() > 300).then(|| {
        format!(
            "is {} characters long; a column name cannot exceed 300",
            name.len()
        )
    })
}

/// The `CREATE OR REPLACE TABLE` statement that atomically replaces `quoted_table`, already
/// backtick-quoted, with an empty table carrying `schema` (see `recreate_table` for why DDL).
///
/// Only properties [`arrow_schema_to_bigquery`] produces are rendered, so table-level ones
/// (partitioning, clustering, labels, description) are lost; a `replace` whose schema still
/// matches keeps them through `TRUNCATE` instead.
pub(super) fn create_or_replace_table_ddl(
    quoted_table: &str,
    schema: &TableSchema,
) -> Result<String> {
    let mut ddl = format!("CREATE OR REPLACE TABLE {quoted_table} (");
    write_field_list(&mut ddl, &schema.fields, "the bound data")?;
    ddl.push(')');
    Ok(ddl)
}

/// Writes one comma-separated `field_name column_schema` list, which is both a table's column
/// list and a `STRUCT`'s field list in BigQuery's DDL grammar.
fn write_field_list(ddl: &mut String, fields: &[TableFieldSchema], owner: &str) -> Result<()> {
    if fields.is_empty() {
        return Err(no_columns(owner));
    }
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            ddl.push_str(", ");
        }
        write_quoted_identifier(ddl, &[&field.name]);
        ddl.push(' ');
        write_column_schema(ddl, field)?;
    }
    Ok(())
}

/// Writes one `column_schema`: the field's type, and the `NOT NULL` its mode may add.
///
/// A `REPEATED` field is an `ARRAY<...>` with no element constraint, since BigQuery arrays cannot
/// hold NULL elements.
fn write_column_schema(ddl: &mut String, field: &TableFieldSchema) -> Result<()> {
    match field.mode.as_str() {
        "REPEATED" => {
            ddl.push_str("ARRAY<");
            write_type(ddl, field)?;
            ddl.push('>');
        }
        "REQUIRED" => {
            write_type(ddl, field)?;
            ddl.push_str(" NOT NULL");
        }
        // An absent mode is `NULLABLE`, which is the DDL default and spells nothing.
        _ => write_type(ddl, field)?,
    }
    Ok(())
}

fn write_type(ddl: &mut String, field: &TableFieldSchema) -> Result<()> {
    use std::fmt::Write as _;

    let cannot_render = "writing a BigQuery DDL type into a String cannot fail";
    let Some(kind) = BigQueryType::from_rest(&field.r#type) else {
        return Err(not_implemented(format!(
            "BigQuery type {:?} of column {:?} is not implemented for bulk ingest",
            field.r#type, field.name
        )));
    };
    match kind {
        BigQueryType::Struct => {
            ddl.push_str("STRUCT<");
            write_field_list(ddl, &field.fields, &format!("column {:?}", field.name))?;
            ddl.push('>');
        }
        BigQueryType::Range => {
            let element = field
                .range_element_type
                .as_ref()
                .and_then(|element| RangeElement::from_rest(&element.r#type))
                .ok_or_else(|| {
                    invalid_argument(format!(
                        "BigQuery RANGE column {:?} names no supported element type",
                        field.name
                    ))
                })?;
            write!(ddl, "RANGE<{}>", element.sql()).expect(cannot_render);
        }
        // A declared precision is part of the column's type: dropping it would create a column
        // the next ingest's schema comparison reports as a difference, and a narrower decimal
        // would silently widen to the full NUMERIC domain.
        BigQueryType::Numeric | BigQueryType::BigNumeric if field.precision != 0 => {
            write!(ddl, "{}({}, {})", kind.sql(), field.precision, field.scale)
                .expect(cannot_render);
        }
        BigQueryType::String | BigQueryType::Bytes if field.max_length != 0 => {
            write!(ddl, "{}({})", kind.sql(), field.max_length).expect(cannot_render);
        }
        _ => ddl.push_str(kind.sql()),
    }
    Ok(())
}

fn no_columns(owner: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "{owner} has no fields; a BigQuery table or STRUCT needs at least one"
    ))
}

pub(crate) fn arrow_schema_to_bigquery(schema: &Schema) -> Result<TableSchema> {
    validate_schema_nesting(schema.fields(), "BigQuery")?;
    // `tables.insert` accepts an empty schema, and the table it creates can be neither queried
    // nor appended to.
    if schema.fields().is_empty() {
        return Err(no_columns("the bound data"));
    }
    reject_duplicate_names(schema.fields(), "the bound data")?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| arrow_field_to_bigquery(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(TableSchema {
        fields,
        ..Default::default()
    })
}

/// Refuses a `REPEATED TIMESTAMP` field at any depth: Storage Write fails every append carrying
/// one with `INTERNAL`, and only after its retries run out (UP-90). In `ARRAY<STRUCT<ts
/// TIMESTAMP>>` the timestamp field itself is not repeated, and appends to it succeed.
pub(super) fn reject_repeated_timestamps(schema: &TableSchema) -> Result<()> {
    match repeated_timestamp_path(&schema.fields) {
        Some(path) => Err(not_implemented(format!(
            "ingest field {path:?} is ARRAY<TIMESTAMP>, which BigQuery Storage Write cannot \
             append; ingest it as ARRAY<STRUCT<ts TIMESTAMP>> into a staging table and convert \
             it with SQL"
        ))),
        None => Ok(()),
    }
}

/// The dotted path of the first `REPEATED TIMESTAMP` field in `fields`, allocated only when
/// there is one.
fn repeated_timestamp_path(fields: &[TableFieldSchema]) -> Option<String> {
    fields.iter().find_map(|field| {
        if field.mode == "REPEATED"
            && BigQueryType::from_rest(&field.r#type) == Some(BigQueryType::Timestamp)
        {
            return Some(field.name.clone());
        }
        repeated_timestamp_path(&field.fields).map(|path| format!("{}.{path}", field.name))
    })
}

fn arrow_field_to_bigquery(field: &Field) -> Result<TableFieldSchema> {
    if field.data_type() == &DataType::UInt64 {
        return Err(uint64_not_implemented(field.name()));
    }
    let mode = if field.is_nullable() {
        "NULLABLE"
    } else {
        "REQUIRED"
    };
    let mut result = TableFieldSchema {
        name: field.name().clone(),
        mode: mode.to_owned(),
        ..Default::default()
    };
    if let Some(kind) = scalar_arrow_field_type(field.data_type(), field, INGEST_FIELD)? {
        result.r#type = kind.rest().to_owned();
    } else {
        match field.data_type() {
            data_type @ (DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)) => {
                let mapping = decimal_mapping(
                    data_type,
                    decimal_metadata_type_hint(field.data_type(), field, None, INGEST_FIELD)?,
                )
                .map_err(invalid_argument)?;
                result.r#type = mapping.bigquery_type.name().to_owned();
                // Plain BIGNUMERIC carries the full i256 domain, including a partial 77th digit,
                // so its Arrow type declares (76, 38) only when its metadata says so.
                if !mapping.narrows_its_arrow_domain()
                    || field
                        .metadata()
                        .contains_key(BIGQUERY_FIELD_METADATA_PRECISION)
                {
                    result.precision = i64::from(mapping.precision);
                    result.scale = i64::from(mapping.scale);
                }
            }
            DataType::Struct(fields) => {
                if let Some(element) = range_field_element(field, fields, INGEST_FIELD)? {
                    result.r#type = BigQueryType::Range.rest().to_owned();
                    result.range_element_type = Some(FieldElementType {
                        r#type: element.rest().to_owned(),
                    });
                } else {
                    if let Some(rich_type) = field.metadata().get(BIGQUERY_TYPE_METADATA_KEY)
                        && !rich_type.starts_with("STRUCT<")
                    {
                        return Err(incompatible_metadata_error(field, rich_type));
                    }
                    result.r#type = BigQueryType::Struct.rest().to_owned();
                    let owner = format!("column {:?}", field.name());
                    // BigQuery refuses an empty RECORD ("type RECORD but has no schema"); a
                    // dry run sends no `tables.insert` and would accept it.
                    if fields.is_empty() {
                        return Err(no_columns(&owner));
                    }
                    reject_duplicate_names(fields, &owner)?;
                    result.fields = fields
                        .iter()
                        .map(|field| arrow_field_to_bigquery(field))
                        .collect::<Result<Vec<_>>>()?;
                }
            }
            DataType::List(item) => {
                let item = arrow_field_to_bigquery(&array_item_field(field, item)?)?;
                if item.mode == "REPEATED" {
                    return Err(nested_array_error(field));
                }
                result = TableFieldSchema {
                    mode: "REPEATED".to_owned(),
                    ..item
                };
            }
            // pandas, pyarrow and Polars infer `Null` for a column that holds only nulls.
            DataType::Null => {
                return Err(not_implemented(format!(
                    "Arrow ingest field {:?} has type Null, which no BigQuery column type \
                     matches; cast it to the column's intended type, or append it to a table \
                     that has the column",
                    field.name()
                )));
            }
            data_type => {
                return Err(not_implemented(format!(
                    "Arrow type {data_type} of ingest field {:?} is not implemented for BigQuery \
                     bulk ingest",
                    field.name()
                )));
            }
        }
    }
    apply_max_length(field, result)
}

/// Rejects sibling fields BigQuery would read as one column: the Storage Write API resolves an
/// Arrow field by name, so `id` and `ID` would both land in the same column of an existing table.
fn reject_duplicate_names(fields: &Fields, owner: &str) -> Result<()> {
    match case_duplicate(fields, folded_name) {
        Some(((_, first), (_, second))) => Err(invalid_argument(format!(
            "BigQuery column names are case-insensitive, so {owner} cannot carry both {first:?} \
             and {second:?}; rename one of them"
        ))),
        None => Ok(()),
    }
}

/// Rejects an Arrow integer whose complete domain BigQuery cannot represent, unlike a parameter
/// (see [`scalar_arrow_field_type`]).
fn uint64_not_implemented(name: &str) -> adbc_core::error::Error {
    not_implemented(format!(
        "Arrow ingest field {name:?} has type UInt64, but BigQuery has no unsigned integer type; \
         cast it to Int64 or Decimal128(20, 0)"
    ))
}

fn apply_max_length(field: &Field, mut result: TableFieldSchema) -> Result<TableFieldSchema> {
    let Some(value) = field.metadata().get(BIGQUERY_FIELD_METADATA_MAX_LENGTH) else {
        return Ok(result);
    };
    let max_length = value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            invalid_argument(format!(
                "{BIGQUERY_FIELD_METADATA_MAX_LENGTH} metadata {value:?} on Arrow field {:?} must \
                 be a positive integer",
                field.name()
            ))
        })?;
    if !matches!(
        BigQueryType::from_rest(&result.r#type),
        Some(BigQueryType::String | BigQueryType::Bytes)
    ) {
        return Err(invalid_argument(format!(
            "{BIGQUERY_FIELD_METADATA_MAX_LENGTH} metadata applies only to STRING and BYTES \
             columns, not Arrow field {:?} of type {}",
            field.name(),
            field.data_type()
        )));
    }
    if result.max_length != 0 && result.max_length != max_length {
        return Err(invalid_argument(format!(
            "{BIGQUERY_FIELD_METADATA_MAX_LENGTH} metadata on list field {:?} differs from its \
             item's",
            field.name()
        )));
    }
    result.max_length = max_length;
    Ok(result)
}

/// BigQuery's own `type` for the one object rows can be appended to.
const INGESTIBLE_TABLE_TYPE: &str = "TABLE";

/// Rejects an ingest target that exists but cannot take rows.
///
/// Views and external tables carry schemas like tables, so without this check a view would fail
/// inside Storage Write, or be offered the `replace` remedy that runs `CREATE OR REPLACE TABLE`
/// over it. Only `TABLE` is admitted, since a wrong guess in `replace` mode destroys the object.
///
/// The status is `INVALID_ARGUMENT`, not the schema mismatch's `ALREADY_EXISTS`: no mode or
/// schema makes this target writable. An empty type is left to the schema check.
pub(super) fn validate_ingestible_target(table: TableId<'_>, table_type: &str) -> Result<()> {
    if table_type.is_empty() || table_type == INGESTIBLE_TABLE_TYPE {
        return Ok(());
    }
    Err(invalid_argument(format!(
        "BigQuery ingest target {table} is a {table_type}, not a table; set \
         {ADBC_INGEST_OPTION_TARGET_TABLE} to a table, or drop it so the ingest can create one"
    )))
}

/// Reports whether an existing table can accept the bound data, naming every column that cannot.
///
/// The status is `ALREADY_EXISTS`, as adbc.h requires for a target that exists "but the schema
/// does not match the schema of the data to append"; the message names the columns at fault and
/// the remedy. A target with no schema gets [`schemaless_target`] instead, since `replace` is the
/// wrong remedy for it, and one with a column Storage Write cannot write is `NOT_IMPLEMENTED`,
/// since no bound schema helps.
pub(super) fn validate_existing_schema(
    table: TableId<'_>,
    actual: Option<&TableSchema>,
    expected: &TableSchema,
) -> Result<()> {
    let Some(actual) = actual else {
        return Err(schemaless_target(table));
    };
    if let Some((path, reason)) = unwritable_column(&actual.fields) {
        return Err(not_implemented(format!(
            "BigQuery ingest target {table} has column {path:?}, whose name {reason}; Storage \
             Write fails every append to such a table, so rename the column, or set \
             {ADBC_INGEST_OPTION_MODE} to {ADBC_INGEST_OPTION_MODE_REPLACE:?} to recreate the \
             table"
        )));
    }
    let mut differences = Vec::new();
    collect_append_differences(
        &mut String::new(),
        &actual.fields,
        &expected.fields,
        &mut differences,
    );
    if differences.is_empty() {
        return Ok(());
    }
    Err(error(
        format!(
            "BigQuery ingest target {table} has an incompatible schema: {}; change the bound \
             schema to match, or set {ADBC_INGEST_OPTION_MODE} to \
             {ADBC_INGEST_OPTION_MODE_REPLACE:?} to recreate the table",
            summarize_errors(differences.into_iter())
        ),
        Status::AlreadyExists,
    ))
}

/// The dotted path of the first column in `fields`, at any depth, whose name Storage Write cannot
/// write, with the reason.
fn unwritable_column(fields: &[TableFieldSchema]) -> Option<(String, String)> {
    fields
        .iter()
        .find_map(|field| match unwritable_name(&field.name) {
            Some(reason) => Some((field.name.clone(), reason)),
            None => unwritable_column(&field.fields)
                .map(|(path, reason)| (format!("{}.{path}", field.name), reason)),
        })
}

/// Reports a target BigQuery described without a schema.
///
/// A real table always has one, so this is some other object whose type BigQuery did not report
/// to [`validate_ingestible_target`]. The `replace` remedy is wrong for it.
pub(super) fn schemaless_target(table: TableId<'_>) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery ingest target {table} has no schema, so it is not a writable table; set \
         {ADBC_INGEST_OPTION_TARGET_TABLE} to a table, or drop it so the ingest can create one"
    ))
}

/// Whether `replace` can keep the table it found by truncating it: the same columns, in the same
/// order, under the same names and modes. Anything less is not the table `replace` would have
/// created from the bound data, so it is recreated instead.
pub(super) fn schemas_match(actual: &TableSchema, expected: &TableSchema) -> bool {
    fields_match_exactly(&actual.fields, &expected.fields)
}

fn fields_match_exactly(actual: &[TableFieldSchema], expected: &[TableFieldSchema]) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.name == expected.name
                && field_shape(actual, true) == field_shape(expected, true)
                && fields_match_exactly(&actual.fields, &expected.fields)
        })
}

/// The field of `fields` BigQuery resolves `name` to: the one at `index` when the two lists agree
/// there, which is the common case and costs one comparison, and otherwise a scan.
pub(super) fn resolve_column<'a>(
    fields: &'a [TableFieldSchema],
    index: usize,
    name: &str,
) -> Option<&'a TableFieldSchema> {
    let named = |field: &&TableFieldSchema| field.name.eq_ignore_ascii_case(name);
    fields
        .get(index)
        .filter(named)
        .or_else(|| fields.iter().find(named))
}

/// Walks the bound fields the way the Storage Write API resolves them, appending one message per
/// column it would refuse. `path` is the dotted prefix of the enclosing struct, reused across the
/// walk so a matching schema allocates nothing.
///
/// Storage Write matches Arrow fields to table columns by name, ignoring case and order, at every
/// struct level, and writes an omitted NULLABLE column as NULL and an omitted REPEATED one as an
/// empty array. It refuses a bound field the table lacks and an omitted REQUIRED column.
///
/// A `REQUIRED`/`NULLABLE` difference is not refused: Arrow nullability only says a column may
/// hold nulls, DuckDB, pyarrow and pandas mark every field nullable, and the service rejects a
/// null that reaches a `REQUIRED` column. Repeatedness stays strict.
fn collect_append_differences(
    path: &mut String,
    actual: &[TableFieldSchema],
    expected: &[TableFieldSchema],
    differences: &mut Vec<String>,
) {
    for (index, expected) in expected.iter().enumerate() {
        let Some(actual) = resolve_column(actual, index, &expected.name) else {
            differences.push(format!(
                "bound column {} is not in the table",
                quoted_column(path, &expected.name)
            ));
            continue;
        };
        let prefix = path.len();
        path.push_str(&actual.name);
        if field_shape(actual, false).accepts(&field_shape(expected, false)) {
            path.push('.');
            collect_append_differences(path, &actual.fields, &expected.fields, differences);
        } else {
            // Always spell the real modes out, whatever the comparison ignored.
            let actual_shape = field_shape(actual, true);
            let expected_shape = field_shape(expected, true);
            differences.push(format!(
                "column {path:?} is {actual_shape} in the table but {expected_shape} in the bound \
                 data"
            ));
        }
        path.truncate(prefix);
    }
    for (index, actual) in actual.iter().enumerate() {
        if canonical_mode(&actual.mode) == "REQUIRED"
            && resolve_column(expected, index, &actual.name).is_none()
        {
            differences.push(format!(
                "REQUIRED column {} is missing from the bound data",
                quoted_column(path, &actual.name)
            ));
        }
    }
}

/// The dotted path of one column, quoted as a whole: `"customer.name"`, not `customer."name"`.
fn quoted_column(path: &str, name: &str) -> String {
    format!("{:?}", format!("{path}{name}"))
}

/// One column's BigQuery type and mode: everything a comparison checks except the name and the
/// nested fields the caller recurses into. `exact_mode` keeps `REQUIRED` and `NULLABLE` apart;
/// without it only repeatedness counts.
///
/// Comparing the parts borrows rather than renders, so a schema that matches allocates nothing;
/// `Display` spells the column out only for the columns that do not.
#[derive(PartialEq, Eq)]
struct FieldShape<'a> {
    kind: &'a str,
    range_element: Option<&'a str>,
    decimal: Option<(i64, i64)>,
    max_length: i64,
    mode: &'a str,
}

impl std::fmt::Display for FieldShape<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.kind)?;
        if let Some(element) = self.range_element {
            write!(formatter, "<{element}>")?;
        }
        if let Some((precision, scale)) = self.decimal {
            write!(formatter, "({precision}, {scale})")?;
        }
        if self.max_length != 0 {
            write!(formatter, "({})", self.max_length)?;
        }
        write!(formatter, " {}", self.mode)
    }
}

impl FieldShape<'_> {
    /// Whether a table column of this shape can take every value of a bound column of shape
    /// `data` unchanged.
    ///
    /// Storage Write enforces neither column parameter (observed, not documented): it silently
    /// rounds a decimal to the column's scale and stores an over-long `STRING(L)` value. So a
    /// parameterized column accepts only data whose declared domain fits inside it (no more
    /// fractional or integer digits, no longer declared length), which also admits narrower data
    /// into a wider column, such as `NUMERIC(10, 2)` into plain `NUMERIC`.
    fn accepts(&self, data: &FieldShape<'_>) -> bool {
        let decimal_fits = match (self.decimal, data.decimal) {
            (Some((precision, scale)), Some((data_precision, data_scale))) => {
                data_scale <= scale && data_precision - data_scale <= precision - scale
            }
            // Plain BIGNUMERIC holds every parameterized BIGNUMERIC domain. The reverse
            // does not hold, even for (76, 38), which excludes its partial 77th digit.
            (None, Some(_)) => self.kind == "BIGNUMERIC",
            (table, data) => table == data,
        };
        let length_fits =
            self.max_length == 0 || (data.max_length != 0 && data.max_length <= self.max_length);
        // Storage Write parses a plain string as JSON text or WKT, as it parses one annotated
        // `arrow.json` or `geoarrow.wkt`, and keeps WKT as it is in a STRING column; it refuses an
        // `arrow.json` field for a STRING column (observed).
        let kind_fits = self.kind == data.kind
            || matches!(
                (self.kind, data.kind),
                ("JSON" | "GEOGRAPHY", "STRING") | ("STRING", "GEOGRAPHY")
            );
        kind_fits
            && self.range_element == data.range_element
            && self.mode == data.mode
            && decimal_fits
            && length_fits
    }
}

fn field_shape(field: &TableFieldSchema, exact_mode: bool) -> FieldShape<'_> {
    let mode = canonical_mode(&field.mode);
    FieldShape {
        kind: googlesql_type_name(&field.r#type),
        range_element: field
            .range_element_type
            .as_ref()
            .map(|element| googlesql_type_name(&element.r#type)),
        decimal: decimal_shape(field),
        max_length: field.max_length,
        // Collapsing onto one spelling, rather than dropping the mode, keeps a repeated column
        // distinguishable from a scalar one.
        mode: if exact_mode || mode == "REPEATED" {
            mode
        } else {
            "NULLABLE"
        },
    }
}

/// The GoogleSQL spelling of a type BigQuery reported, for messages a caller reads: the REST API
/// answers with the legacy names (`INTEGER`, `RECORD`) that no schema in this driver ever uses.
fn googlesql_type_name(value: &str) -> &str {
    match BigQueryType::from_rest(value) {
        Some(kind) => kind.sql(),
        None => value,
    }
}

pub(super) fn canonical_mode(value: &str) -> &str {
    if value.is_empty() { "NULLABLE" } else { value }
}

/// Decimal constraints, treating plain NUMERIC as (38, 9). Plain BIGNUMERIC has no
/// equivalent parameterized domain, so it retains `None` and compares separately.
fn decimal_shape(field: &TableFieldSchema) -> Option<(i64, i64)> {
    let (precision, scale) = match BigQueryType::from_rest(&field.r#type)? {
        BigQueryType::Numeric => (38, 9),
        BigQueryType::BigNumeric if field.precision == 0 && field.scale == 0 => return None,
        BigQueryType::BigNumeric => (76, 38),
        _ => return None,
    };
    Some(if field.precision == 0 {
        (
            precision,
            if field.scale == 0 { scale } else { field.scale },
        )
    } else {
        (field.precision, field.scale)
    })
}

#[cfg(test)]
mod tests;
