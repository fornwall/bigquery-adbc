//! `table_schema.rs`: `get_table_schema`, the Go-driver metadata keys it populates, and the
//! table reference its three components resolve to.

use super::*;

/// Table metadata, dry-run descriptions and executed results must expose the same temporal
/// types, including inside STRUCTs and arrays. Pin the units and zones independently as well:
/// agreement alone would miss all three paths returning the same incorrect type.
#[test]
fn temporal_schemas_agree_across_table_description_and_execution() {
    use arrow_array::{ListArray, StructArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, FieldRef, TimeUnit};

    let temporal_fields = json!([
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "dt", "type": "DATETIME"}
    ]);
    let wire_schema = json!({"fields": [
        temporal_fields[0],
        temporal_fields[1],
        {"name": "rec", "type": "RECORD", "fields": temporal_fields},
        {"name": "recs", "type": "RECORD", "mode": "REPEATED", "fields": temporal_fields}
    ]});
    let temporal_cells = json!([
        {"v": "1123456"},
        {"v": "1970-01-01T00:00:01.123456"}
    ]);
    let (endpoint, requests) = capture_requests(vec![
        (200, json!({"type": "TABLE", "schema": wire_schema})),
        (
            200,
            json!({
                "status": {"state": "DONE"},
                "statistics": {"query": {"statementType": "SELECT", "schema": wire_schema}}
            }),
        ),
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": {"projectId":"test-project","jobId":"temporal-job"},
                "statementType": "SELECT",
                "schema": wire_schema,
                "totalRows": "1",
                "rows": [{"f": [
                    temporal_cells[0],
                    temporal_cells[1],
                    {"v": {"f": temporal_cells}},
                    {"v": [{"v": {"f": temporal_cells}}]}
                ]}]
            }),
        ),
    ]);
    let mut connection = anonymous_connection_at(&endpoint);
    let table_schema = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("SELECT ts, dt, rec, recs FROM dataset.events")
        .unwrap();
    let described_schema = statement.execute_schema().unwrap();
    let mut reader = statement.execute().unwrap();
    let result_schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().is_none());

    let assert_temporal_fields = |fields: &[FieldRef]| {
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "ts");
        assert_eq!(fields[1].name(), "dt");
        assert_eq!(
            fields[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            fields[1].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    };
    assert_eq!(table_schema.fields().len(), 4);
    assert_temporal_fields(&table_schema.fields()[..2]);
    let DataType::Struct(fields) = table_schema.field(2).data_type() else {
        panic!("rec must be a STRUCT");
    };
    assert_temporal_fields(fields);
    let DataType::List(item) = table_schema.field(3).data_type() else {
        panic!("recs must be an ARRAY");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("recs elements must be STRUCTs");
    };
    assert_temporal_fields(fields);
    // Schema-level relation and job metadata legitimately differ across these APIs.
    for schema in [
        &described_schema,
        result_schema.as_ref(),
        batch.schema_ref(),
    ] {
        assert_eq!(schema.fields(), table_schema.fields());
    }

    // Exercise actual decoding too, with a value whose final digits would be lost at ms precision.
    let record = typed::<StructArray>(batch.column(2).as_ref());
    let records = typed::<ListArray>(batch.column(3).as_ref()).value(0);
    let records = typed::<StructArray>(records.as_ref());
    assert_eq!(records.len(), 1);
    for columns in [&batch.columns()[..2], record.columns(), records.columns()] {
        for column in columns {
            assert_eq!(
                typed::<TimestampMicrosecondArray>(column.as_ref()).value(0),
                1_123_456
            );
        }
    }

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[0]
            .path()
            .contains("/datasets/dataset/tables/events")
    );
    assert!(requests[1].path().ends_with("/jobs"));
    assert_eq!(requests[1].body["configuration"]["dryRun"], true);
    assert!(requests[2].path().ends_with("/queries"));
}

/// The dbt 2.0 contract: a missing table is NOT_FOUND, never a generic error.
#[test]
fn get_table_schema_reports_not_found_for_a_missing_table() {
    let (endpoint, request) = capture_requests(vec![(
        404,
        json!({
            "error": {
                "code": 404,
                "message": "Not found: Table test-project:dataset.missing",
                "status": "NOT_FOUND",
                "errors": [
                    {"message": "Not found", "domain": "global", "reason": "notFound"}
                ]
            }
        }),
    )]);
    let connection = anonymous_connection_at(&endpoint);
    let error = connection
        .get_table_schema(None, Some("dataset"), "missing")
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    let request_line = &request.join().unwrap()[0].line;
    assert!(
        request_line.contains("/datasets/dataset/tables/missing"),
        "{request_line}"
    );
}

/// Only BigQuery's `invalid` reason says the name cannot be a table ID. Any other 400 is a request
/// the driver got wrong, which must not read as a missing table, least of all to dbt 2.0,
/// which matches the `404 Not Found:` text.
#[test]
fn get_table_schema_keeps_other_bad_requests_as_errors() {
    let (endpoint, requests) = capture_requests(vec![(
        400,
        json!({
            "error": {
                "code": 400,
                "message": "Invalid field selection nonsense",
                "errors": [{"message": "Invalid field selection nonsense", "domain": "global", "reason": "invalidParameter"}],
                "status": "INVALID_ARGUMENT"
            }
        }),
    )]);
    let connection = anonymous_connection_at(&endpoint);
    let error = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
    crate::test_support::errors::assert_message_omits(&error, &["404 Not Found"]);
    requests.served(1);
}

/// `tables.get` omits `schema` for a table created without columns, which has an empty schema. A
/// view omits it when its SQL no longer compiles, which is reported rather than hidden.
#[test]
fn a_table_without_columns_has_an_empty_schema_and_a_broken_view_is_an_error() {
    let (endpoint, requests) = capture_requests(vec![
        (200, json!({"type": "TABLE"})),
        (200, json!({"type": "VIEW"})),
    ]);
    let connection = anonymous_connection_at(&endpoint);
    let schema = connection
        .get_table_schema(None, Some("dataset"), "empty")
        .unwrap();
    assert!(schema.fields().is_empty());
    assert_eq!(
        schema.metadata().get("Type").map(String::as_str),
        Some("TABLE")
    );
    let error = connection
        .get_table_schema(None, Some("dataset"), "broken_view")
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidData, "{}", error.message);
    assert!(error.message.contains("broken_view"), "{}", error.message);
    requests.served(2);
}

/// Fields and the schema itself carry the metadata dbt 2.0 reconstructs table
/// configuration from, in the exact key spellings and value formats it parses.
#[test]
fn get_table_schema_populates_go_driver_metadata() {
    let (endpoint, request) = capture_requests(vec![(
        200,
        json!({
            "schema": {
                "fields": [
                    {"name": "event_date", "type": "DATE"},
                    {"name": "id", "type": "INTEGER", "description": "row id"}
                ]
            },
            "type": "MATERIALIZED_VIEW",
            "description": "test table",
            "labels": {"env": "dev", "app": "reports"},
            "resourceTags": {"tagKeys/1": "tagValues/2"},
            "clustering": {"fields": ["id"]},
            "timePartitioning": {
                "type": "DAY",
                "field": "event_date",
                "expirationMs": "604800000"
            },
            "expirationTime": "1767225600000",
            "encryptionConfiguration": {
                "kmsKeyName": "projects/p/locations/l/keyRings/r/cryptoKeys/k"
            },
            "requirePartitionFilter": true,
            "materializedView": {
                "query": "SELECT event_date, id FROM dataset.events",
                "enableRefresh": true,
                "refreshIntervalMs": "1800000",
                "allowNonIncrementalDefinition": false
            },
            "maxStaleness": "0-0 0 8:0:0"
        }),
    )]);
    let connection = anonymous_connection_at(&endpoint);
    let schema = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap();

    for (key, expected) in [
        ("Type", "MATERIALIZED_VIEW"),
        ("Description", "test table"),
        ("Labels", r#"{"app":"reports","env":"dev"}"#),
        ("ResourceTags", r#"{"tagKeys/1":"tagValues/2"}"#),
        ("Clustering.Fields", r#"["id"]"#),
        ("TimePartitioning.Type", "DAY"),
        ("TimePartitioning.Field", "event_date"),
        ("TimePartitioning.Expiration", "168h0m0s"),
        ("RequirePartitionFilter", "true"),
        ("ExpirationTime", "2026-01-01T00:00:00Z"),
        (
            "EncryptionConfig.KMSKeyName",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k",
        ),
        (
            "MaterializedView.Query",
            "SELECT event_date, id FROM dataset.events",
        ),
        ("MaterializedView.EnableRefresh", "true"),
        ("MaterializedView.AllowNonIncrementalDefinition", "false"),
        ("MaterializedView.RefreshInterval", "30m0s"),
        ("MaterializedView.MaxStaleness", "0-0 0 8:0:0"),
    ] {
        assert_eq!(
            schema.metadata().get(key).map(String::as_str),
            Some(expected),
            "{key}"
        );
    }
    assert!(!schema.metadata().contains_key("RangePartitioning.Field"));
    assert_eq!(
        schema
            .field(1)
            .metadata()
            .get("Description")
            .map(String::as_str),
        Some("row id")
    );
    assert_eq!(
        schema.field(1).metadata().get("Type").map(String::as_str),
        Some("INTEGER")
    );
    let request_line = &request.join().unwrap()[0].line;
    assert!(
        request_line.contains("view=BASIC"),
        "relation configuration must not request storage statistics: {request_line}"
    );
}

/// Describing a relation needs its configuration, while statistics need a separate storage
/// accounting lookup. Using BASIC for the former must not change the latter's reported values.
#[test]
fn table_schema_and_statistics_request_the_metadata_each_consumes() {
    let (endpoint, requests) = capture_requests(vec![
        (
            200,
            json!({
                "type": "TABLE",
                "schema": {"fields": [{"name": "bucket", "type": "INTEGER"}]},
                "description": "partitioned events",
                "clustering": {"fields": ["bucket"]},
                "rangePartitioning": {
                    "field": "bucket",
                    "range": {"start": "0", "end": "1000", "interval": "10"}
                },
                "requirePartitionFilter": true
            }),
        ),
        (200, json!({"type": "TABLE", "numRows": "42"})),
    ]);
    let connection = anonymous_connection_at(&endpoint);
    let schema = connection
        .get_table_schema(None, Some("dataset"), "events")
        .unwrap();
    assert_eq!(schema.field(0).name(), "bucket");
    assert_eq!(
        schema.metadata().get("Description").map(String::as_str),
        Some("partitioned events")
    );

    let batch =
        first_batch(connection.get_statistics(None, Some("dataset"), Some("events"), false));
    let statistics = child_structs(child_structs(batch.column(1)).column(1));
    assert_eq!(statistics.len(), 1);
    assert_eq!(
        typed::<Int16Array>(statistics.column(2)).value(0),
        i16::from(Statistics::RowCount)
    );
    let values = typed::<UnionArray>(statistics.column(3));
    assert_eq!(values.type_id(0), 0);
    assert_eq!(
        typed::<Int64Array>(values.child(0)).value(values.value_offset(0)),
        42
    );
    assert!(!typed::<BooleanArray>(statistics.column(4)).value(0));

    let requests = requests.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].path().contains("view=BASIC"));
    assert!(!requests[0].path().contains("numRows"));
    assert!(requests[1].path().contains("view=STORAGE_STATS"));
    assert!(requests[1].path().contains("numRows"));
}

#[test]
fn range_partitioning_metadata_uses_the_go_driver_keys() {
    let table = Table::decode(
        br#"{
            "rangePartitioning": {
                "field": "bucket",
                "range": {"start": "0", "end": "1000", "interval": "10"}
            }
        }"#,
    )
    .unwrap();
    let metadata = table_schema_metadata(table);
    for (key, expected) in [
        ("RangePartitioning.Field", "bucket"),
        ("RangePartitioning.Range.Start", "0"),
        ("RangePartitioning.Range.End", "1000"),
        ("RangePartitioning.Range.Interval", "10"),
        // dbt 2.0's get_relation requires the key, so an empty table type falls back to TABLE.
        ("Type", "TABLE"),
    ] {
        assert_eq!(
            metadata.get(key).map(String::as_str),
            Some(expected),
            "{key}"
        );
    }
}

/// A view's SQL and an external table's source are reported with the Go driver's keys and value
/// formats; a flag BigQuery omits stays absent rather than becoming `false`.
#[test]
fn view_and_external_table_metadata_use_the_go_driver_keys() {
    let view = Table::decode(
        br#"{"type": "VIEW", "view": {"query": "SELECT 1 AS x", "useLegacySql": false}}"#,
    )
    .unwrap();
    let metadata = table_schema_metadata(view);
    assert_eq!(metadata.get("Type").map(String::as_str), Some("VIEW"));
    assert_eq!(
        metadata.get("ViewQuery").map(String::as_str),
        Some("SELECT 1 AS x")
    );
    assert_eq!(
        metadata.get("UseLegacySQL").map(String::as_str),
        Some("false")
    );
    assert!(!metadata.contains_key("RequirePartitionFilter"));
    assert!(!metadata.contains_key("Clustering.Fields"));

    let external = Table::decode(
        br#"{
            "type": "EXTERNAL",
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": ["gs://bucket/a.csv", "gs://bucket/b.csv"],
                "autodetect": true,
                "compression": "GZIP",
                "ignoreUnknownValues": false,
                "maxBadRecords": 5,
                "hivePartitioningOptions": {
                    "mode": "AUTO",
                    "sourceUriPrefix": "gs://bucket/",
                    "requirePartitionFilter": true
                },
                "decimalTargetTypes": ["NUMERIC", "STRING"],
                "connectionId": "project.region.connection",
                "referenceFileSchemaUri": "gs://bucket/schema.parquet",
                "metadataCacheMode": "AUTOMATIC"
            }
        }"#,
    )
    .unwrap();
    let metadata = table_schema_metadata(external);
    for (key, expected) in [
        ("Type", "EXTERNAL"),
        ("ExternalDataConfig.SourceFormat", "CSV"),
        (
            "ExternalDataConfig.SourceURIs",
            r#"["gs://bucket/a.csv","gs://bucket/b.csv"]"#,
        ),
        ("ExternalDataConfig.AutoDetect", "true"),
        ("ExternalDataConfig.Compression", "GZIP"),
        ("ExternalDataConfig.IgnoreUnknownValues", "false"),
        ("ExternalDataConfig.MaxBadRecords", "5"),
        ("ExternalDataConfig.HivePartitioningOptions.Mode", "AUTO"),
        (
            "ExternalDataConfig.HivePartitioningOptions.SourceURIPrefix",
            "gs://bucket/",
        ),
        (
            "ExternalDataConfig.HivePartitioningOptions.RequirePartitionFilter",
            "true",
        ),
        (
            "ExternalDataConfig.DecimalTargetTypes",
            r#"["NUMERIC","STRING"]"#,
        ),
        (
            "ExternalDataConfig.ConnectionID",
            "project.region.connection",
        ),
        (
            "ExternalDataConfig.ReferenceFileSchemaURI",
            "gs://bucket/schema.parquet",
        ),
        ("ExternalDataConfig.MetadataCacheMode", "AUTOMATIC"),
    ] {
        assert_eq!(
            metadata.get(key).map(String::as_str),
            Some(expected),
            "{key}"
        );
    }
    assert!(!metadata.contains_key("ViewQuery"));

    // An external table described only by its source gets no invented keys.
    let sparse =
        Table::decode(br#"{"type": "EXTERNAL", "externalDataConfiguration": {}}"#).unwrap();
    let metadata = table_schema_metadata(sparse);
    assert_eq!(
        metadata.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["Type"]
    );
}

#[test]
fn epoch_milliseconds_format_as_rfc3339() {
    for (epoch_ms, expected) in [
        (0_i64, "1970-01-01T00:00:00Z"),
        (1_767_225_600_000, "2026-01-01T00:00:00Z"),
        (951_827_696_789, "2000-02-29T12:34:56.789Z"),
    ] {
        assert_eq!(format_rfc3339_millis(epoch_ms), expected, "{epoch_ms}");
    }
}

/// `get_table_schema`'s three components each come from the argument when it is given and from
/// the connection or database default when it is not.
#[test]
fn table_reference_uses_explicit_or_configured_components() {
    let configured = project("configured");
    for (catalog, db_schema, expected) in [
        (
            Some("explicit"),
            Some("dataset"),
            ("explicit", "dataset", "table"),
        ),
        (None, Some("dataset"), ("configured", "dataset", "table")),
        (None, None, ("configured", "configured_dataset", "table")),
    ] {
        let resolved = resolve_table_reference(
            Some(&configured),
            Some("configured_dataset"),
            catalog,
            db_schema,
            "table",
        )
        .unwrap();
        assert_eq!(
            (resolved.0.as_str(), resolved.1, resolved.2),
            expected,
            "{catalog:?}.{db_schema:?}"
        );
    }

    // An empty catalog or db_schema names no project or dataset rather than the configured one,
    // and the refusal says how to ask for that one instead of claiming none is configured.
    for (catalog, db_schema, argument) in
        [(None, Some(""), "db_schema"), (Some(""), None, "catalog")]
    {
        let empty = resolve_table_reference(
            Some(&configured),
            Some("configured_dataset"),
            catalog,
            db_schema,
            "table",
        )
        .unwrap_err();
        assert_eq!(empty.status, Status::InvalidArguments);
        assert_message(
            &empty,
            &[
                &format!("{argument} argument must not be empty"),
                "pass NULL",
            ],
        );
    }
}

/// A bad argument is `INVALID_ARGUMENTS`; an absent component is `INVALID_STATE`, as an unset
/// ingest target or query project is, and the message names the options that would supply it.
#[test]
fn table_reference_refusals_name_the_options_that_would_supply_the_component() {
    let configured = project("project");
    for (configured_project, catalog, db_schema, table_name, expected) in [
        (
            None,
            None,
            Some("dataset"),
            "table",
            &["adbc.connection.catalog", "bigquery.project_id"][..],
        ),
        (
            Some(&configured),
            None,
            None,
            "table",
            &["adbc.connection.db_schema", "bigquery.dataset_id"][..],
        ),
        (
            Some(&configured),
            Some(""),
            Some("dataset"),
            "table",
            &[][..],
        ),
        (
            Some(&configured),
            Some("path/segment"),
            Some("dataset"),
            "table",
            &[][..],
        ),
        (Some(&configured), None, Some("dataset"), "", &[][..]),
    ] {
        let error =
            resolve_table_reference(configured_project, None, catalog, db_schema, table_name)
                .unwrap_err();
        let status = if expected.is_empty() {
            Status::InvalidArguments
        } else {
            Status::InvalidState
        };
        assert_eq!(error.status, status, "{}", error.message);
        for fragment in expected {
            assert_message(&error, &[fragment]);
        }
    }
}
