//! Table schemas, relation metadata, `get_objects` and `get_statistics`.

use adbc_core::error::Status;
use adbc_core::options::{ObjectDepth, Statistics};
use adbc_core::schemas;
use adbc_core::{Connection, Optionable, Statement};
use adbc_driver_bigquery::{BigQueryConnection, BigQueryConnectionOption, BigQueryStatement};
use arrow_array::{
    Array, BooleanArray, Int16Array, Int64Array, ListArray, RecordBatch, RecordBatchReader,
    StringArray, StructArray, UnionArray,
};
use arrow_schema::{DataType, Metadata};

use crate::support::{
    DropTable, SCRATCH_DATASET, SHAKESPEARE_ROWS, all_struct_values, connect, connect_at_location,
    connect_with_dataset, execute_sql, nested_struct, prepare_query, project,
    report_failed_cleanup, scratch_bucket, scratch_table, str_at, str_value, typed,
};

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn gets_public_table_schema() {
    let connection = connect(project());

    let schema = connection
        .get_table_schema(Some("bigquery-public-data"), Some("samples"), "shakespeare")
        .unwrap();

    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "word");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "word_count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    // dbt 2.0's get_relation requires the relation type in the schema metadata.
    assert_eq!(
        schema.metadata().get("Type").map(String::as_str),
        Some("TABLE")
    );

    let missing = connection
        .get_table_schema(
            Some("bigquery-public-data"),
            Some("samples"),
            "__bigquery_adbc_missing_table__",
        )
        .unwrap_err();
    assert_eq!(missing.status, Status::NotFound);
    // dbt 2.0 matches this exact substring to classify missing relations.
    assert!(
        missing.message.contains("404 Not Found:"),
        "message: {}",
        missing.message
    );
    let details = missing.details.unwrap_or_default();
    assert!(
        details.contains(&("bigquery.error.reason".to_owned(), b"notFound".to_vec())),
        "details: {details:?}"
    );
}

/// `get_table_schema` metadata for each relation kind, in the Go driver's spelling: a table's
/// partitioning and clustering, view and materialized view SQL, and an external table's source.
/// Only a live `tables.get` verifies these definitions survive the BASIC metadata view.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn table_metadata_describes_what_defines_each_relation() {
    let scratch_bucket = scratch_bucket();
    let table = scratch_table("definition_metadata");
    let view = format!("{table}_view");
    let materialized_view = format!("{table}_mv");
    let external = format!("{table}_external");
    let _drop_table = DropTable::table(&table);
    let _drop_view = DropTable::new("VIEW", &view);
    let _drop_materialized_view = DropTable::new("MATERIALIZED VIEW", &materialized_view);
    let _drop_external = DropTable::new("EXTERNAL TABLE", &external);
    let mut connection = connect_with_dataset();

    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{table}` (event_date DATE, id INT64) \
             PARTITION BY event_date CLUSTER BY id \
             OPTIONS(partition_expiration_days=7, require_partition_filter=true, \
             description='definition metadata', labels=[('env', 'test')])"
        ),
    );
    // The scratch dataset may assign a default table expiration.
    execute_sql(
        &mut connection,
        &format!("ALTER TABLE `{table}` SET OPTIONS(expiration_timestamp=NULL)"),
    );
    let metadata = described(
        &connection,
        &table,
        &[
            ("Type", "TABLE"),
            ("Description", "definition metadata"),
            ("Labels", r#"{"env":"test"}"#),
            ("TimePartitioning.Type", "DAY"),
            ("TimePartitioning.Field", "event_date"),
            ("TimePartitioning.Expiration", "168h0m0s"),
            ("RequirePartitionFilter", "true"),
            ("Clustering.Fields", r#"["id"]"#),
        ],
    );
    // An unset expiration stays absent.
    assert_eq!(metadata.get("ExpirationTime"), None);
    assert_eq!(metadata.get("ViewQuery"), None);

    execute_sql(
        &mut connection,
        &format!("CREATE VIEW `{view}` AS SELECT 1 AS x"),
    );
    // This driver runs GoogleSQL only, and reports which dialect defines a view it describes.
    let metadata = described(
        &connection,
        &view,
        &[
            ("Type", "VIEW"),
            ("ViewQuery", "SELECT 1 AS x"),
            ("UseLegacySQL", "false"),
        ],
    );
    assert_eq!(metadata.get("Clustering.Fields"), None);

    // A materialized view's base table must be dataset-qualified even with a default dataset.
    let materialized_query = format!(
        "SELECT event_date, COUNT(*) AS rows_seen FROM `{SCRATCH_DATASET}.{table}` \
         GROUP BY event_date"
    );
    execute_sql(
        &mut connection,
        &format!(
            "CREATE MATERIALIZED VIEW `{materialized_view}` \
             OPTIONS(enable_refresh=true, refresh_interval_minutes=30) AS {materialized_query}"
        ),
    );
    described(
        &connection,
        &materialized_view,
        &[
            ("Type", "MATERIALIZED_VIEW"),
            ("MaterializedView.Query", materialized_query.as_str()),
            ("MaterializedView.EnableRefresh", "true"),
            ("MaterializedView.RefreshInterval", "30m0s"),
        ],
    );

    // The external table is never read, so its source objects need not exist.
    execute_sql(
        &mut connection,
        &format!(
            "CREATE EXTERNAL TABLE `{external}` (id INT64) \
             OPTIONS(format='CSV', uris=['gs://{scratch_bucket}/{external}/*.csv'], \
             compression='GZIP', ignore_unknown_values=true, max_bad_records=5)"
        ),
    );
    described(
        &connection,
        &external,
        &[
            ("Type", "EXTERNAL"),
            ("ExternalDataConfig.SourceFormat", "CSV"),
            (
                "ExternalDataConfig.SourceURIs",
                format!(r#"["gs://{scratch_bucket}/{external}/*.csv"]"#).as_str(),
            ),
            ("ExternalDataConfig.Compression", "GZIP"),
            ("ExternalDataConfig.IgnoreUnknownValues", "true"),
            ("ExternalDataConfig.MaxBadRecords", "5"),
        ],
    );
}

/// The `get_table_schema` metadata of `name`, checked to hold `expected`.
#[track_caller]
fn described(connection: &BigQueryConnection, name: &str, expected: &[(&str, &str)]) -> Metadata {
    let metadata = connection
        .get_table_schema(None, None, name)
        .unwrap()
        .metadata()
        .clone();
    for (key, value) in expected {
        assert_eq!(
            metadata.get(key).map(String::as_str),
            Some(*value),
            "{name}: {key}"
        );
    }
    metadata
}

#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn lists_filtered_public_objects() {
    let connection = connect("bigquery-public-data");

    let mut reader = connection
        .get_objects(
            ObjectDepth::All,
            Some("bigquery-public-%"),
            Some("samples"),
            Some("shake%"),
            Some(vec!["TABLE"]),
            Some("word"),
        )
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(str_at(&batch, 0), "bigquery-public-data");

    let db_schemas = nested_struct(batch.column(1));
    assert_eq!(db_schemas.len(), 1);
    assert_eq!(str_value(db_schemas.column(0)), "samples");

    // Other public tables may match `shake%`, so only shakespeare's presence is asserted.
    let tables = nested_struct(db_schemas.column(1));
    let names = typed::<StringArray>(tables.column(0).as_ref());
    let shakespeare = (0..tables.len())
        .find(|row| names.value(*row) == "shakespeare")
        .unwrap_or_else(|| panic!("the `shake%` filter did not match shakespeare"));
    for row in 0..tables.len() {
        assert!(
            names.value(row).starts_with("shake"),
            "the `shake%` filter matched {}",
            names.value(row)
        );
    }

    let columns = typed::<StructArray>(
        typed::<ListArray>(tables.column(2).as_ref())
            .value(shakespeare)
            .as_ref(),
    )
    .clone();
    assert_eq!(columns.len(), 1);
    assert_eq!(str_value(columns.column(0)), "word");
    assert!(reader.next().is_none());

    let mut exact = connection
        .get_objects(
            ObjectDepth::Tables,
            Some("bigquery-public-data"),
            Some("samples"),
            Some("shakespeare"),
            Some(vec!["TABLE"]),
            None,
        )
        .unwrap();
    let batch = exact.next().unwrap().unwrap();
    assert_eq!(str_at(&batch, 0), "bigquery-public-data");
    let schemas = all_struct_values(batch.column(1));
    assert_eq!(schemas.len(), 1);
    assert_eq!(str_value(schemas.column(0)), "samples");
    let tables = all_struct_values(schemas.column(1));
    assert_eq!(tables.len(), 1);
    assert_eq!(str_value(tables.column(0)), "shakespeare");
    assert_eq!(str_value(tables.column(1)), "TABLE");
    // `ObjectDepth::Tables` leaves the column list null; empty would claim there are no columns.
    assert!(typed::<ListArray>(tables.column(2).as_ref()).is_null(0));
    assert!(exact.next().is_none());
}

/// `tables.get` answers a name that cannot be a table ID with 400 rather than 404, and resolves a
/// snapshot decorator to the table; `tables.list` answers a name that cannot be a dataset ID with
/// 400. An exact filter naming any of them still matches nothing.
#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn an_exact_filter_that_is_not_an_id_matches_nothing() {
    let connection = connect("bigquery-public-data");

    for name in ["a*b", "shakespeare@0", "shakespeare@-3600000"] {
        let mut objects = connection
            .get_objects(
                ObjectDepth::Tables,
                None,
                Some("samples"),
                Some(name),
                None,
                None,
            )
            .unwrap();
        let mut statistics = connection
            .get_statistics(None, Some("samples"), Some(name), true)
            .unwrap();
        for batch in [objects.next(), statistics.next()] {
            let schemas = all_struct_values(batch.unwrap().unwrap().column(1));
            assert_eq!(schemas.len(), 1, "{name}");
            assert_eq!(str_value(schemas.column(0)), "samples");
            assert_eq!(all_struct_values(schemas.column(1)).len(), 0, "{name}");
        }
    }

    let mut objects = connection
        .get_objects(ObjectDepth::Tables, None, Some("a b"), None, None, None)
        .unwrap();
    let mut statistics = connection
        .get_statistics(None, Some("a b"), None, true)
        .unwrap();
    for batch in [objects.next(), statistics.next()] {
        assert_eq!(
            all_struct_values(batch.unwrap().unwrap().column(1)).len(),
            0
        );
    }
}

/// A foreign catalog is enumerated without a probe when its walk finds something, and probed when
/// it finds nothing. BigQuery answers a missing project with 404 and a malformed project ID with
/// 400 on every lookup, so neither may leave a catalog row, while a missing dataset in an existing
/// project leaves one with no schemas.
#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn foreign_catalogs_are_enumerated_only_where_they_exist() {
    let connection = connect(project());
    let objects = |depth, catalog: &str, db_schema: Option<&str>, table: Option<&str>| {
        let mut reader = connection
            .get_objects(depth, Some(catalog), db_schema, table, None, None)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        batch
    };
    let statistics = |catalog: &str, db_schema: Option<&str>, table: Option<&str>| {
        let mut reader = connection
            .get_statistics(Some(catalog), db_schema, table, true)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        batch
    };

    for (db_schema, table) in [
        (Some("samples"), Some("shakespeare")),
        (Some("samples"), None),
    ] {
        for batch in [
            objects(
                ObjectDepth::Tables,
                "bigquery-public-data",
                db_schema,
                table,
            ),
            statistics("bigquery-public-data", db_schema, table),
        ] {
            assert_eq!(str_at(&batch, 0), "bigquery-public-data");
            let schemas = all_struct_values(batch.column(1));
            assert_eq!(schemas.len(), 1);
            assert_eq!(str_value(schemas.column(0)), "samples");
            assert!(!all_struct_values(schemas.column(1)).is_empty());
        }
    }

    for batch in [
        objects(
            ObjectDepth::Schemas,
            "bigquery-public-data",
            Some("no_such_dataset_adbc"),
            None,
        ),
        objects(
            ObjectDepth::Tables,
            "bigquery-public-data",
            Some("no_such_dataset_adbc"),
            None,
        ),
        statistics("bigquery-public-data", Some("no_such_dataset_adbc"), None),
    ] {
        assert_eq!(str_at(&batch, 0), "bigquery-public-data");
        assert_eq!(all_struct_values(batch.column(1)).len(), 0);
    }

    for catalog in ["adbc-no-such-project-7f3k2", "Nope"] {
        for (db_schema, table) in [
            (None, None),
            (Some("samples"), None),
            (Some("samples"), Some("shakespeare")),
        ] {
            for depth in [ObjectDepth::Catalogs, ObjectDepth::All] {
                let batch = objects(depth, catalog, db_schema, table);
                assert_eq!(
                    batch.num_rows(),
                    0,
                    "{catalog} {depth:?} {db_schema:?} {table:?}"
                );
            }
            let batch = statistics(catalog, db_schema, table);
            assert_eq!(batch.num_rows(), 0, "{catalog} {db_schema:?} {table:?}");
        }
    }
}

/// The table name, value and approximation flag of the row-count statistic in a
/// `get_statistics` batch with one catalog and schema.
#[track_caller]
fn row_count_statistic(batch: &RecordBatch) -> (String, i64, bool) {
    let db_schemas = all_struct_values(batch.column(1));
    assert_eq!(db_schemas.len(), 1);
    let statistics = all_struct_values(db_schemas.column(1));
    let row = typed::<Int16Array>(statistics.column(2))
        .values()
        .iter()
        .position(|key| *key == i16::from(Statistics::RowCount))
        .unwrap();
    // The union layout is tested offline in `src/connection/statistics.rs`.
    let values = typed::<UnionArray>(statistics.column(3));
    assert_eq!(values.type_id(row), 0);
    (
        typed::<StringArray>(statistics.column(0))
            .value(row)
            .to_owned(),
        typed::<Int64Array>(values.child(0)).value(values.value_offset(row)),
        typed::<BooleanArray>(statistics.column(4)).value(row),
    )
}

#[test]
#[ignore = "requires Google Cloud ADC and access to bigquery-public-data"]
fn gets_public_table_statistics() {
    let connection = connect("bigquery-public-data");

    let mut reader = connection
        .get_statistics(
            Some("bigquery-public-data"),
            Some("samples"),
            Some("shakespeare"),
            false,
        )
        .unwrap();
    assert_eq!(reader.schema(), schemas::GET_STATISTICS_SCHEMA.clone());
    let (table, rows, _) = row_count_statistic(&reader.next().unwrap().unwrap());
    assert_eq!((table.as_str(), rows), ("shakespeare", SHAKESPEARE_ROWS));
    assert!(reader.next().is_none());
}

#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn gets_hidden_dataset_objects_and_statistics() {
    let project = project();
    // Own a unique dataset and clean up even when an assertion fails.
    struct DropSchema(BigQueryStatement);
    impl Drop for DropSchema {
        fn drop(&mut self) {
            if let Err(error) = self.0.execute_update() {
                report_failed_cleanup("drop the hidden statistics test dataset", &error);
            }
        }
    }

    let mut connection = connect_at_location(project, "US");
    let dataset = format!("_{}", scratch_table("statistics"));
    let qualified_dataset = format!("{project}.{dataset}");
    // Armed first: a create whose response was lost still made the dataset.
    let _cleanup = DropSchema(prepare_query(
        &mut connection,
        &format!("DROP SCHEMA IF EXISTS `{qualified_dataset}` CASCADE"),
    ));
    execute_sql(
        &mut connection,
        &format!(
            "CREATE SCHEMA `{qualified_dataset}` OPTIONS(location='US', default_table_expiration_days=1)"
        ),
    );
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{qualified_dataset}.events` AS SELECT 42 AS id"),
    );

    let mut objects = connection
        .get_objects(
            ObjectDepth::Tables,
            Some(project),
            Some(&dataset),
            None,
            None,
            None,
        )
        .unwrap();
    let batch = objects.next().unwrap().unwrap();
    let db_schemas = all_struct_values(batch.column(1));
    assert_eq!(db_schemas.len(), 1);
    assert_eq!(typed::<StringArray>(db_schemas.column(0)).value(0), dataset);
    let tables = all_struct_values(db_schemas.column(1));
    assert_eq!(tables.len(), 1);
    assert_eq!(typed::<StringArray>(tables.column(0)).value(0), "events");
    assert!(objects.next().is_none());

    for approximate in [false, true] {
        let mut reader = connection
            .get_statistics(Some(project), Some(&dataset), Some("ev%"), approximate)
            .unwrap();
        assert_eq!(reader.schema(), schemas::GET_STATISTICS_SCHEMA.clone());
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(
            str_value(all_struct_values(batch.column(1)).column(0)),
            dataset
        );
        assert_eq!(
            row_count_statistic(&batch),
            ("events".to_owned(), 1, false),
            "approximate: {approximate}"
        );
        assert!(reader.next().is_none());
    }
}

/// In a dataset created with `is_case_insensitive`, BigQuery resolves dataset and table names
/// without regard to case, so an exact filter in another case finds them, and reports the names
/// BigQuery resolved it to rather than the filter's spelling (G56, G74). Literal filters, since the
/// scratch names contain `_`, which a pattern filter would match as a wildcard.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project"]
fn exact_filters_in_a_case_insensitive_dataset_report_the_resolved_names() {
    let project = project();
    struct DropSchema(BigQueryStatement);
    impl Drop for DropSchema {
        fn drop(&mut self) {
            if let Err(error) = self.0.execute_update() {
                report_failed_cleanup("drop the case-insensitive test dataset", &error);
            }
        }
    }

    let mut connection = connect_at_location(project, "US");
    let dataset = scratch_table("case_insensitive");
    let qualified_dataset = format!("{project}.{dataset}");
    // Armed first: a create whose response was lost still made the dataset.
    let _cleanup = DropSchema(prepare_query(
        &mut connection,
        &format!("DROP SCHEMA IF EXISTS `{qualified_dataset}` CASCADE"),
    ));
    execute_sql(
        &mut connection,
        &format!(
            "CREATE SCHEMA `{qualified_dataset}` OPTIONS(location='US', \
             default_table_expiration_days=1, is_case_insensitive=TRUE)"
        ),
    );
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{qualified_dataset}.events` AS SELECT 42 AS id"),
    );
    connection
        .set_option(
            BigQueryConnectionOption::GetObjectsDisableWildcards.into(),
            "true".into(),
        )
        .unwrap();

    let filter = dataset.to_uppercase();
    for (depth, table) in [
        (ObjectDepth::Schemas, None),
        (ObjectDepth::Tables, None),
        (ObjectDepth::Tables, Some("EVENTS")),
        (ObjectDepth::All, Some("EVENTS")),
    ] {
        let mut reader = connection
            .get_objects(depth, Some(project), Some(&filter), table, None, None)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        let schemas = all_struct_values(batch.column(1));
        assert_eq!(schemas.len(), 1, "{depth:?} {table:?}");
        assert_eq!(str_value(schemas.column(0)), dataset, "{depth:?} {table:?}");
        if depth != ObjectDepth::Schemas {
            let tables = all_struct_values(schemas.column(1));
            assert_eq!(tables.len(), 1, "{depth:?} {table:?}");
            assert_eq!(str_value(tables.column(0)), "events", "{depth:?} {table:?}");
        }
    }

    let schema = connection
        .get_table_schema(Some(project), Some(&filter), "EVENTS")
        .unwrap();
    assert_eq!(schema.field(0).name(), "id");
}
