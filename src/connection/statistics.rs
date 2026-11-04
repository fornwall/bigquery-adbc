//! ADBC statistics metadata representation and Arrow conversion.

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::Statistics;
use adbc_core::schemas;
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray, StructArray,
};

use super::arrow::{
    boolean_column, catalog_batch, dense_union, int16_column, nested_struct, single_batch_reader,
    string_column, struct_fields, union_offset,
};
use super::info::MATERIALIZED_VIEW;
use crate::rest::metadata::Table;

const TOTAL_LOGICAL_BYTES_KEY: i16 = 1024;
const TOTAL_PHYSICAL_BYTES_KEY: i16 = 1026;
const ACTIVE_LOGICAL_BYTES_KEY: i16 = 1027;
const LONG_TERM_LOGICAL_BYTES_KEY: i16 = 1028;
const TIME_TRAVEL_PHYSICAL_BYTES_KEY: i16 = 1029;
const PARTITION_COUNT_KEY: i16 = 1030;
/// The table's last modification time, in milliseconds since the Unix epoch, as int64.
const LAST_MODIFIED_TIME_KEY: i16 = 1031;

const STATISTIC_NAMES: [(&str, i16); 7] = [
    (
        "bigquery.statistic.total_logical_bytes",
        TOTAL_LOGICAL_BYTES_KEY,
    ),
    (
        "bigquery.statistic.total_physical_bytes",
        TOTAL_PHYSICAL_BYTES_KEY,
    ),
    (
        "bigquery.statistic.active_logical_bytes",
        ACTIVE_LOGICAL_BYTES_KEY,
    ),
    (
        "bigquery.statistic.long_term_logical_bytes",
        LONG_TERM_LOGICAL_BYTES_KEY,
    ),
    (
        "bigquery.statistic.time_travel_physical_bytes",
        TIME_TRAVEL_PHYSICAL_BYTES_KEY,
    ),
    ("bigquery.statistic.partition_count", PARTITION_COUNT_KEY),
    (
        "bigquery.statistic.last_modified_time",
        LAST_MODIFIED_TIME_KEY,
    ),
];

#[derive(Debug)]
pub(crate) struct StatisticCatalog {
    pub(crate) name: String,
    pub(crate) db_schemas: Vec<StatisticDbSchema>,
}

#[derive(Debug)]
pub(crate) struct StatisticDbSchema {
    pub(crate) name: String,
    pub(crate) statistics: Vec<TableStatistic>,
}

#[derive(Debug)]
pub(crate) struct TableStatistic {
    pub(crate) table_name: Arc<str>,
    pub(crate) key: i16,
    pub(crate) value: StatisticValue,
    pub(crate) is_approximate: bool,
}

/// The statistic value union members the driver fills.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum StatisticValue {
    Int64(i64),
    /// Only the row count of a table with a streaming buffer, which adbc.h types as float64
    /// because it is approximate.
    Float64(f64),
}

pub(crate) fn statistic_names_reader() -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let batch = RecordBatch::try_new(
        schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        vec![
            Arc::new(string_column(
                &mut STATISTIC_NAMES.iter().map(|(name, _)| *name),
            )),
            Arc::new(int16_column(
                &mut STATISTIC_NAMES.iter().map(|(_, key)| *key),
            )),
        ],
    )?;
    Ok(single_batch_reader(batch))
}

pub(crate) fn statistics_reader(
    catalogs: &[StatisticCatalog],
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    catalog_batch(
        &schemas::GET_STATISTICS_SCHEMA,
        catalogs,
        |catalog| catalog.name.as_str(),
        |catalog| Some(catalog.db_schemas.as_slice()),
        &schemas::STATISTICS_DB_SCHEMA_SCHEMA,
        |db_schemas| Ok(Arc::new(build_db_schemas(&db_schemas)?) as ArrayRef),
    )
}

/// Appends the statistics BigQuery reported for one table, in ascending key order.
pub(crate) fn table_statistics(
    table_name: String,
    metadata: &Table,
    statistics: &mut Vec<TableStatistic>,
) {
    let table_name = Arc::<str>::from(table_name);
    let first = statistics.len();
    // adbc.h types an exact row count as int64 and an approximate one as float64. `numRows` is
    // exact but excludes streaming-buffer rows, which BigQuery only estimates. A materialized
    // view's `numRows` counts what its last refresh stored, while queries also read later
    // base-table changes.
    let materialized_view = metadata.table_type == MATERIALIZED_VIEW;
    let row_count = match (metadata.num_rows, metadata.streaming_buffer_rows) {
        (Some(rows), None) if !materialized_view => i64::try_from(rows)
            .ok()
            .map(|rows| (StatisticValue::Int64(rows), false)),
        #[allow(
            clippy::cast_precision_loss,
            reason = "an approximate count is what float64 is for"
        )]
        (Some(rows), buffered) => Some((
            StatisticValue::Float64(rows.saturating_add(buffered.unwrap_or(0)) as f64),
            true,
        )),
        (None, _) => None,
    };
    if let Some((value, is_approximate)) = row_count {
        statistics.push(TableStatistic {
            table_name: table_name.clone(),
            key: Statistics::RowCount.into(),
            value,
            is_approximate,
        });
    }
    // Storage estimates BigQuery recomputes in the background: the REST reference documents
    // `numPartitions` as delayed by seconds to minutes, too.
    for (key, value) in [
        (
            TOTAL_LOGICAL_BYTES_KEY,
            metadata.num_total_logical_bytes.or(metadata.num_bytes),
        ),
        (
            TOTAL_PHYSICAL_BYTES_KEY,
            metadata
                .num_total_physical_bytes
                .or(metadata.num_physical_bytes),
        ),
        (ACTIVE_LOGICAL_BYTES_KEY, metadata.num_active_logical_bytes),
        (
            LONG_TERM_LOGICAL_BYTES_KEY,
            metadata
                .num_long_term_logical_bytes
                .or(metadata.num_long_term_bytes),
        ),
        (
            TIME_TRAVEL_PHYSICAL_BYTES_KEY,
            metadata.num_time_travel_physical_bytes,
        ),
        (PARTITION_COUNT_KEY, metadata.num_partitions),
    ] {
        if let Some(value) = value {
            statistics.push(TableStatistic {
                table_name: table_name.clone(),
                key,
                value: StatisticValue::Int64(value),
                is_approximate: true,
            });
        }
    }

    // Reported only beside a storage statistic, as with Go's PARTITIONS-derived time, since alone
    // it describes no stored data. Kept as int64 epoch milliseconds so consumers compare it
    // without parsing text.
    if statistics.len() > first
        && metadata.last_modified_time != 0
        && let Ok(milliseconds) = i64::try_from(metadata.last_modified_time)
    {
        statistics.push(TableStatistic {
            table_name,
            key: LAST_MODIFIED_TIME_KEY,
            value: StatisticValue::Int64(milliseconds),
            is_approximate: false,
        });
    }
}

fn build_db_schemas(db_schemas: &[&StatisticDbSchema]) -> Result<StructArray> {
    nested_struct(
        &schemas::STATISTICS_DB_SCHEMA_SCHEMA,
        db_schemas,
        |schema| schema.name.as_str(),
        |schema| Some(schema.statistics.as_slice()),
        &schemas::STATISTICS_SCHEMA,
        |statistics| Ok(Arc::new(build_statistics(statistics)?) as ArrayRef),
    )
}

fn build_statistics(statistics: Vec<&TableStatistic>) -> Result<StructArray> {
    let table_names = string_column(
        &mut statistics
            .iter()
            .map(|statistic| statistic.table_name.as_ref()),
    );
    // Every statistic describes a whole table: the table resource carries no per-column ones.
    let column_names = StringArray::new_null(statistics.len());
    let keys = int16_column(&mut statistics.iter().map(|statistic| statistic.key));
    let approximate =
        boolean_column(&mut statistics.iter().map(|statistic| statistic.is_approximate));

    let mut int64_values = Vec::with_capacity(statistics.len());
    let mut float64_values = Vec::new();
    let mut type_ids = Vec::with_capacity(statistics.len());
    let mut offsets = Vec::with_capacity(statistics.len());
    for statistic in statistics {
        let (type_id, offset) = match statistic.value {
            StatisticValue::Int64(value) => (0, union_offset(&mut int64_values, value)),
            StatisticValue::Float64(value) => (2, union_offset(&mut float64_values, value)),
        };
        type_ids.push(type_id);
        offsets.push(offset);
    }
    // The uint64 and binary members carry nothing the driver reports, so `dense_union` builds
    // them empty from the schema constant.
    let values = dense_union(
        &schemas::STATISTIC_VALUE_SCHEMA,
        type_ids,
        offsets,
        &mut |type_id| match type_id {
            0 => Some(Arc::new(Int64Array::from(std::mem::take(&mut int64_values))) as ArrayRef),
            2 => Some(Arc::new(Float64Array::from(std::mem::take(
                &mut float64_values,
            )))),
            _ => None,
        },
    )?;

    Ok(StructArray::try_new(
        struct_fields(&schemas::STATISTICS_SCHEMA),
        vec![
            Arc::new(table_names),
            Arc::new(column_names),
            Arc::new(keys),
            Arc::new(values),
            Arc::new(approximate),
        ],
        None,
    )?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use adbc_core::options::Statistics;
    use adbc_core::schemas;
    use arrow_array::{
        Array, BinaryArray, Float64Array, Int16Array, Int64Array, StringArray, UInt64Array,
        UnionArray,
    };

    use crate::rest::metadata::Table;
    use crate::test_support::arrow::{child_structs, typed};

    use super::{
        LAST_MODIFIED_TIME_KEY, PARTITION_COUNT_KEY, StatisticCatalog, StatisticDbSchema,
        StatisticValue, TOTAL_LOGICAL_BYTES_KEY, TableStatistic, statistic_names_reader,
        statistics_reader, table_statistics,
    };

    #[test]
    fn statistic_names_use_standard_schema() {
        let mut reader = statistic_names_reader().unwrap();
        assert_eq!(reader.schema(), schemas::GET_STATISTIC_NAMES_SCHEMA.clone());
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 7);
        let names = typed::<StringArray>(batch.column(0));
        let keys = typed::<Int16Array>(batch.column(1));
        assert_eq!(keys.value(0), 1024);
        assert_eq!(keys.value(6), LAST_MODIFIED_TIME_KEY);
        assert_eq!(names.iter().collect::<HashSet<_>>().len(), batch.num_rows());
        assert_eq!(keys.iter().collect::<HashSet<_>>().len(), batch.num_rows());
        assert!(reader.next().is_none());
    }

    #[test]
    fn builds_complete_get_statistics_hierarchy() {
        let mut reader = statistics_reader(&[StatisticCatalog {
            name: "project".to_owned(),
            db_schemas: vec![StatisticDbSchema {
                name: "dataset".to_owned(),
                statistics: vec![
                    TableStatistic {
                        table_name: "records".into(),
                        key: Statistics::RowCount.into(),
                        value: StatisticValue::Float64(42.5),
                        is_approximate: true,
                    },
                    TableStatistic {
                        table_name: "records".into(),
                        key: LAST_MODIFIED_TIME_KEY,
                        value: StatisticValue::Int64(1_756_382_400_123),
                        is_approximate: false,
                    },
                ],
            }],
        }])
        .unwrap();
        assert_eq!(reader.schema(), schemas::GET_STATISTICS_SCHEMA.clone());
        let batch = reader.next().unwrap().unwrap();
        let db_schemas = child_structs(batch.column(1));
        let statistics = child_structs(db_schemas.column(1));
        assert_eq!(statistics.len(), 2);
        assert!(statistics.column(1).is_null(0));
        let values = typed::<UnionArray>(statistics.column(3));
        assert_eq!(values.type_id(0), 2);
        assert_eq!(
            typed::<Float64Array>(values.child(2)).value(values.value_offset(0)),
            42.5
        );
        // The members the driver never fills are present and empty, as the schema declares.
        assert_eq!(typed::<UInt64Array>(values.child(1)).len(), 0);
        assert_eq!(typed::<BinaryArray>(values.child(3)).len(), 0);
        assert_eq!(values.type_id(1), 0);
        assert_eq!(
            typed::<Int64Array>(values.child(0)).value(values.value_offset(1)),
            1_756_382_400_123
        );
        assert!(reader.next().is_none());
    }

    #[test]
    fn converts_bigquery_storage_metadata() {
        // BigQuery renders every one of these int64 counters as a JSON *string*; a decoder that
        // took them for numbers would silently report no statistics at all.
        let metadata = Table::decode(
            br#"{
                "numRows": "42",
                "numTotalLogicalBytes": "1024",
                "numTotalPhysicalBytes": "512",
                "numActiveLogicalBytes": "768",
                "numLongTermLogicalBytes": "256",
                "numTimeTravelPhysicalBytes": "64",
                "numPartitions": "3",
                "lastModifiedTime": "1756382400123"
            }"#,
        )
        .unwrap();
        let mut statistics = Vec::new();
        table_statistics("records".to_owned(), &metadata, &mut statistics);
        assert_eq!(statistics.len(), 8);
        // numRows is exact, so it is int64.
        assert_eq!(statistics[0].key, i16::from(Statistics::RowCount));
        assert_eq!(statistics[0].value, StatisticValue::Int64(42));
        assert!(!statistics[0].is_approximate);
        assert_eq!(statistics[1].key, TOTAL_LOGICAL_BYTES_KEY);
        assert!(statistics[1].is_approximate);
        // BigQuery documents `numPartitions` as delayed, like the byte counters.
        assert_eq!(statistics[6].key, PARTITION_COUNT_KEY);
        assert!(statistics[6].is_approximate);
        // Epoch milliseconds as int64, as BigQuery reports them.
        assert_eq!(statistics[7].key, LAST_MODIFIED_TIME_KEY);
        assert_eq!(
            statistics[7].value,
            StatisticValue::Int64(1_756_382_400_123)
        );
        assert!(!statistics[7].is_approximate);
    }

    /// `numRows` excludes the rows still in a streaming buffer, so a table with one reports the
    /// sum with BigQuery's estimate, typed float64 and flagged approximate as adbc.h requires.
    #[test]
    fn a_streaming_buffer_makes_the_row_count_approximate() {
        let metadata = Table::decode(
            br#"{"numRows": "42", "streamingBuffer": {"estimatedRows": "5", "estimatedBytes": "587"}}"#,
        )
        .unwrap();
        let mut statistics = Vec::new();
        table_statistics("records".to_owned(), &metadata, &mut statistics);
        assert_eq!(statistics[0].key, i16::from(Statistics::RowCount));
        assert_eq!(statistics[0].value, StatisticValue::Float64(47.0));
        assert!(statistics[0].is_approximate);
    }

    /// A materialized view's `numRows` is what its last refresh stored, not what a query of it
    /// returns, so its row count is approximate.
    #[test]
    fn a_materialized_view_row_count_is_approximate() {
        let metadata = Table::decode(br#"{"type": "MATERIALIZED_VIEW", "numRows": "42"}"#).unwrap();
        let mut statistics = Vec::new();
        table_statistics("summary".to_owned(), &metadata, &mut statistics);
        assert_eq!(statistics[0].key, i16::from(Statistics::RowCount));
        assert_eq!(statistics[0].value, StatisticValue::Float64(42.0));
        assert!(statistics[0].is_approximate);
    }
}
