//! dbt 2.0's option-only CSV seed ingestion adapter.

use adbc_core::error::Result;
use arrow_schema::{DataType, Field};

use super::missing_option;
use super::statement::DbtOperation;
use crate::BigQueryStatementOption;
use crate::arrow_ipc_bigquery::reader::decode_schema as decode_ipc_schema;
use crate::error::{invalid_argument, not_implemented, unreadable_file};
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::types::decimal::{BigQueryDecimalType, bigquery_decimal_data_type, decimal_mapping};
use crate::types::{BIGQUERY_TYPE_METADATA_KEY, BigQueryType, scalar_arrow_field_type};

const DEFAULT_DELIMITER: u8 = b',';

/// An opened seed and the length snapshotted for its streamed multipart framing.
#[derive(Debug)]
pub(crate) struct CsvSeedFile<'a> {
    pub(super) file: std::fs::File,
    pub(super) length: u64,
    /// Where it was opened, so that a resent upload reads it from the start through a file
    /// description of its own.
    pub(super) path: &'a str,
}

impl<'a> CsvSeedFile<'a> {
    /// Opens the seed at `path` without reading it, recording the length that frames the streamed
    /// upload. The bytes are sent unmodified; BigQuery owns CSV parsing (quoting, nulls, numbers,
    /// temporals, malformed rows). A path naming something other than a regular file is refused
    /// here rather than failing mid-upload.
    pub(super) fn open(path: &'a str) -> Result<Self> {
        let unreadable = |source: std::io::Error| {
            unreadable_file(
                format!("failed to read BigQuery CSV seed file {path:?}: {source}"),
                &source,
            )
        };
        let mut options = std::fs::File::options();
        // Opening a FIFO must not wait for a writer before the regular-file check.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_NONBLOCK);
        let file = options.read(true).open(path).map_err(unreadable)?;
        let metadata = file.metadata().map_err(unreadable)?;
        if !metadata.is_file() {
            return Err(invalid_argument(format!(
                "BigQuery CSV seed file {path:?} is not a regular file"
            )));
        }
        Ok(Self {
            file,
            length: metadata.len(),
            path,
        })
    }
}

#[derive(Clone)]
pub(crate) struct CsvSeedOptions {
    filepath: Option<String>,
    delimiter: u8,
    encoded_schema: Option<Vec<u8>>,
    /// The load job's schema, built from the declared one when it is set.
    schema: Option<TableSchema>,
}

impl std::fmt::Debug for CsvSeedOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CsvSeedOptions")
            .field("filepath_set", &self.filepath.is_some())
            .field("delimiter", &char::from(self.delimiter))
            .field("schema_set", &self.schema.is_some())
            .finish()
    }
}

impl Default for CsvSeedOptions {
    fn default() -> Self {
        Self {
            filepath: None,
            delimiter: DEFAULT_DELIMITER,
            encoded_schema: None,
            schema: None,
        }
    }
}

impl CsvSeedOptions {
    pub(crate) fn is_active(&self) -> bool {
        self.filepath.is_some()
    }

    pub(crate) fn set_filepath(&mut self, value: String) {
        self.filepath = (!value.is_empty()).then_some(value);
    }

    pub(crate) fn filepath(&self) -> Option<&str> {
        self.filepath.as_deref()
    }

    pub(crate) fn set_delimiter(&mut self, value: &str) -> Result<()> {
        self.delimiter = if value.is_empty() {
            DEFAULT_DELIMITER
        } else {
            match value.as_bytes() {
                [delimiter] if delimiter.is_ascii() => *delimiter,
                _ => {
                    return Err(invalid_argument(format!(
                        "BigQuery option {} must be one ASCII character",
                        BigQueryStatementOption::CsvDelimiter.as_str()
                    )));
                }
            }
        };
        Ok(())
    }

    pub(crate) fn delimiter(&self) -> String {
        char::from(self.delimiter).to_string()
    }

    pub(crate) fn set_schema(&mut self, encoded: Vec<u8>) -> Result<()> {
        let schema = load_schema(&encoded)?;
        self.encoded_schema = Some(encoded);
        self.schema = Some(schema);
        Ok(())
    }

    pub(crate) fn encoded_schema(&self) -> Option<&[u8]> {
        self.encoded_schema.as_deref()
    }

    /// Opens the seed (see [`CsvSeedFile::open`]) and answers it with the load job's schema.
    pub(crate) fn open_seed(&self) -> Result<(CsvSeedFile<'_>, &TableSchema)> {
        let missing = |option| missing_option(DbtOperation::CsvSeed.subject(), option);
        let filepath = self
            .filepath
            .as_deref()
            .ok_or_else(|| missing(BigQueryStatementOption::CsvFilepath))?;
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| missing(BigQueryStatementOption::CsvSchema))?;
        Ok((CsvSeedFile::open(filepath)?, schema))
    }
}

/// The load job's schema for the declared Arrow schema.
///
/// Each column gets exactly the type its `BIGQUERY:type` metadata (dbt's `column_types`) declares,
/// parameters included, as `CREATE TABLE` would give it; BigQuery parses the CSV text itself, so
/// no Arrow value domain constrains the declaration. A column without that metadata takes the type
/// its Arrow type implies.
fn load_schema(encoded: &[u8]) -> Result<TableSchema> {
    let schema = decode_ipc_schema(encoded).map_err(|source| {
        invalid_argument(format!(
            "failed to decode the BigQuery CSV seed Arrow IPC schema: {source}"
        ))
    })?;
    if schema.fields().is_empty() {
        return Err(invalid_argument(
            "the BigQuery CSV seed Arrow IPC schema has no columns",
        ));
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| load_field(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(TableSchema {
        fields,
        ..TableSchema::default()
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TypeParameters {
    Decimal {
        decimal_type: BigQueryDecimalType,
        precision: u8,
        scale: i8,
    },
    MaxLength(i64),
}

fn load_field(field: &Field) -> Result<TableFieldSchema> {
    let logical_name = field.metadata().get(BIGQUERY_TYPE_METADATA_KEY);
    let (logical_type, parameters) = match logical_name {
        Some(logical_name) => parse_logical_type(logical_name).ok_or_else(|| {
            not_implemented(format!(
                "BigQuery CSV seed field {:?} has unsupported GoogleSQL type {logical_name:?}",
                field.name()
            ))
        })?,
        None => logical_type_from_physical(field)?,
    };
    if logical_type.canonical_data_type().is_none() {
        return Err(not_implemented(format!(
            "BigQuery CSV seed field {:?} has unsupported composite GoogleSQL type",
            field.name(),
        )));
    }
    let mut result = TableFieldSchema {
        name: field.name().clone(),
        r#type: logical_type.rest().to_owned(),
        mode: if field.is_nullable() {
            "NULLABLE"
        } else {
            "REQUIRED"
        }
        .to_owned(),
        ..TableFieldSchema::default()
    };
    match parameters {
        Some(TypeParameters::Decimal {
            decimal_type,
            precision,
            scale,
        }) => {
            // Zero precision would read as the parameter-free type.
            if precision == 0 {
                return Err(invalid_decimal(field, "precision must be at least 1"));
            }
            bigquery_decimal_data_type(decimal_type, i64::from(precision), i64::from(scale))
                .map_err(|reason| invalid_decimal(field, &reason))?;
            result.precision = i64::from(precision);
            result.scale = i64::from(scale);
        }
        Some(TypeParameters::MaxLength(max_length)) => result.max_length = max_length,
        None => {}
    }
    Ok(result)
}

#[cold]
fn invalid_decimal(field: &Field, reason: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "BigQuery CSV seed field {:?} has invalid decimal parameters: {reason}",
        field.name()
    ))
}

fn parse_logical_type(value: &str) -> Option<(BigQueryType, Option<TypeParameters>)> {
    let upper = value.trim().to_ascii_uppercase();
    let separator = upper.find(['<', '(']);
    let name = separator.map_or(upper.as_str(), |index| upper[..index].trim());
    let name = match name {
        "TEXT" => "STRING",
        "INTEGER" => "INT64",
        "FLOAT" => "FLOAT64",
        "BOOLEAN" => "BOOL",
        "DECIMAL" => "NUMERIC",
        "BIGDECIMAL" => "BIGNUMERIC",
        name => name,
    };
    let logical_type = BigQueryType::from_sql(name)?;
    let parameters = match separator {
        Some(index) if upper.as_bytes().get(index) == Some(&b'(') => {
            let parameters = upper.get(index + 1..)?.strip_suffix(')')?;
            match logical_type {
                BigQueryType::Numeric | BigQueryType::BigNumeric => {
                    let mut values = parameters.split(',').map(str::trim);
                    let precision = values.next()?.parse().ok()?;
                    let scale = values.next().map_or(Some(0), |value| value.parse().ok())?;
                    if values.next().is_some() {
                        return None;
                    }
                    Some(TypeParameters::Decimal {
                        decimal_type: BigQueryDecimalType::from_name(logical_type.sql())?,
                        precision,
                        scale,
                    })
                }
                BigQueryType::String | BigQueryType::Bytes => {
                    let max_length = parameters.trim().parse::<i64>().ok()?;
                    if max_length <= 0 {
                        return None;
                    }
                    Some(TypeParameters::MaxLength(max_length))
                }
                _ => return None,
            }
        }
        // Composite syntax is not valid for a scalar seed field. Composite CSV columns are not
        // implemented, so reject the whole spelling rather than silently discarding its suffix.
        Some(_) => return None,
        None => None,
    };
    Some((logical_type, parameters))
}

fn logical_type_from_physical(field: &Field) -> Result<(BigQueryType, Option<TypeParameters>)> {
    if let Some(logical_type) =
        scalar_arrow_field_type(field.data_type(), field, "BigQuery CSV seed field")?
    {
        return Ok((logical_type, None));
    }
    match field.data_type() {
        data_type @ (DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)) => {
            let mapping = decimal_mapping(data_type, None).map_err(|source| {
                invalid_argument(format!(
                    "BigQuery CSV seed field {:?} has invalid Arrow decimal type: {source}",
                    field.name()
                ))
            })?;
            // Without a declaration the Arrow type decides, as for an ingest: declaring the
            // widest BIGNUMERIC's (76, 38) would narrow the column below its Arrow domain.
            let parameters =
                (!mapping.narrows_its_arrow_domain()).then_some(TypeParameters::Decimal {
                    decimal_type: mapping.bigquery_type,
                    precision: mapping.precision,
                    scale: mapping.scale,
                });
            Ok((mapping.bigquery_type.bigquery_type(), parameters))
        }
        data_type => Err(not_implemented(format!(
            "BigQuery CSV seed field {:?} has unsupported Arrow type {data_type}",
            field.name()
        ))),
    }
}

#[cfg(test)]
mod tests;
