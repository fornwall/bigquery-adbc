use std::collections::HashMap;

use adbc_core::error::Status;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde_json::{Value as JsonValue, json};

use super::CsvSeedOptions;
use crate::arrow_ipc_bigquery::encode_schema_fixture;
use crate::test_support::arrow::bigquery_typed;
use crate::types::ARROW_EXTENSION_NAME_KEY;

fn field(name: &str, physical_type: DataType, logical_type: &str) -> Field {
    bigquery_typed(Field::new(name, physical_type, true), logical_type)
}

/// The load job's schema fields for a seed declaring `fields`, as the JSON the job sends.
fn load_fields(fields: Vec<Field>) -> JsonValue {
    let mut options = CsvSeedOptions::default();
    options
        .set_schema(encode_schema_fixture(&Schema::new(fields)))
        .unwrap();
    serde_json::to_value(options.schema.unwrap().as_ref()).unwrap()["fields"].take()
}

/// One nullable load column of `type`, with `parameters` such as a precision.
fn column(name: &str, r#type: &str, parameters: JsonValue) -> JsonValue {
    let mut column = json!({"name": name, "type": r#type, "mode": "NULLABLE"});
    if let (Some(column), JsonValue::Object(parameters)) = (column.as_object_mut(), parameters) {
        column.extend(parameters);
    }
    column
}

/// dbt's `column_types` reach the load job exactly as declared, parameters included and absent
/// parameters left absent, whatever Arrow type dbt inferred; nothing in the CSV is parsed.
#[test]
fn declared_column_types_become_the_load_schema_exactly() {
    let fields = load_fields(vec![
        field("id", DataType::Utf8, "int64"),
        field("amount", DataType::Utf8, "numeric(12, 2)"),
        field("whole", DataType::Utf8, "numeric(8)"),
        field("plain", DataType::Utf8, "numeric"),
        field("aliased", DataType::Utf8, "decimal(20,4)"),
        field("widest", DataType::Utf8, "bignumeric(76, 38)"),
        field("huge", DataType::Utf8, "bignumeric"),
        field("happened_at", DataType::Utf8, "timestamp"),
        field("day", DataType::Utf8, "date"),
        field("active", DataType::Utf8, "boolean"),
        field("label", DataType::Int64, "string(100)"),
        field("payload", DataType::Utf8, "bytes(16)"),
        field("duration", DataType::Utf8, "interval"),
    ]);
    assert_eq!(
        fields,
        json!([
            column("id", "INTEGER", json!({})),
            column(
                "amount",
                "NUMERIC",
                json!({"precision": "12", "scale": "2"})
            ),
            column("whole", "NUMERIC", json!({"precision": "8"})),
            column("plain", "NUMERIC", json!({})),
            column(
                "aliased",
                "NUMERIC",
                json!({"precision": "20", "scale": "4"})
            ),
            column(
                "widest",
                "BIGNUMERIC",
                json!({"precision": "76", "scale": "38"})
            ),
            column("huge", "BIGNUMERIC", json!({})),
            column("happened_at", "TIMESTAMP", json!({})),
            column("day", "DATE", json!({})),
            column("active", "BOOLEAN", json!({})),
            column("label", "STRING", json!({"maxLength": "100"})),
            column("payload", "BYTES", json!({"maxLength": "16"})),
            column("duration", "INTERVAL", json!({})),
        ])
    );
}

/// A seed path naming no readable regular file is `INVALID_ARGUMENT`, not the retryable `IO`,
/// since retrying cannot fix it, and a dry run reports it too although it never reads the file.
#[test]
fn a_missing_seed_file_is_rejected_without_reading_the_seed() {
    let mut options = CsvSeedOptions::default();
    options
        .set_schema(encode_schema_fixture(&Schema::new(vec![field(
            "id",
            DataType::Utf8,
            "int64",
        )])))
        .unwrap();
    let path = std::env::temp_dir().join(format!("adbc-missing-seed-{}.csv", std::process::id()));
    let _ = std::fs::remove_file(&path);
    options.set_filepath(path.to_string_lossy().into_owned());

    let mut directory = options.clone();
    directory.set_filepath(std::env::temp_dir().to_string_lossy().into_owned());
    // A directory opens on Unix and is refused as no regular file; Windows refuses to open it.
    for error in [
        options.load_schema().unwrap_err(),
        options.open_for_load().unwrap_err(),
        directory.load_schema().unwrap_err(),
        directory.open_for_load().unwrap_err(),
    ] {
        assert_eq!(error.status, Status::InvalidArguments, "{}", error.message);
    }
    let error = options.load_schema().unwrap_err();
    assert!(
        // The message quotes the path, which doubles a Windows path's backslashes.
        error
            .message
            .contains(&format!("{:?}", path.to_string_lossy())),
        "the seed path is what the operator has to fix: {}",
        error.message
    );

    std::fs::write(&path, b"id\n1\n").unwrap();
    assert_eq!(options.load_schema().unwrap().fields.len(), 1);
    let (mut seed, _, delimiter) = options.open_for_load().unwrap();
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut seed.file, &mut data).unwrap();
    assert_eq!(data, b"id\n1\n", "the seed reaches BigQuery byte for byte");
    assert_eq!(seed.length, data.len() as u64);
    assert_eq!(delimiter, ",");
    std::fs::remove_file(&path).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        use std::sync::mpsc;
        use std::time::Duration;

        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let (send, receive) = mpsc::channel();
        let worker = std::thread::spawn(move || send.send(options.open_for_load()).unwrap());
        let result = receive.recv_timeout(Duration::from_secs(1));
        // Release a blocking reader before asserting, so a regression cannot hang the suite.
        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            result
                .expect("opening a FIFO must not wait for a writer")
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
        worker.join().unwrap();
    }
}

#[test]
fn delimiter_and_schema_are_validated_when_set() {
    let mut options = CsvSeedOptions::default();
    assert_eq!(options.delimiter(), ",");
    options.set_delimiter("|").unwrap();
    assert_eq!(options.delimiter(), "|");
    options.set_delimiter("").unwrap();
    assert_eq!(options.delimiter(), ",");
    for delimiter in ["::", "§"] {
        let error = options.set_delimiter(delimiter).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(
            error.message,
            "BigQuery option adbc.bigquery.ingest.csv_delimiter must be one ASCII character"
        );
    }
    for encoded in [
        b"not IPC".to_vec(),
        encode_schema_fixture(&Schema::new(Vec::<Field>::new())),
    ] {
        assert_eq!(
            options.set_schema(encoded).unwrap_err().status,
            Status::InvalidArguments
        );
    }
}

/// A column without `column_types` takes the type its Arrow type implies, including extension
/// types, a decimal's own parameters and its nullability.
#[test]
fn undeclared_columns_take_the_type_their_arrow_type_implies() {
    let extension_field = |name: &str, data_type: DataType, extension: &str| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            ARROW_EXTENSION_NAME_KEY.to_owned(),
            extension.to_owned(),
        )]))
    };
    let fields = load_fields(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("active", DataType::Boolean, true),
        Field::new("day", DataType::Date32, true),
        Field::new("text", DataType::Utf8, true),
        field("overridden_text", DataType::Int64, "string"),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new(
            "local_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
            true,
        ),
        extension_field("document", DataType::LargeUtf8, "arrow.json"),
        extension_field("location", DataType::Utf8View, "geoarrow.wkt"),
    ]);
    assert_eq!(
        fields,
        json!([
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            column("active", "BOOLEAN", json!({})),
            column("day", "DATE", json!({})),
            column("text", "STRING", json!({})),
            column("overridden_text", "STRING", json!({})),
            column("amount", "NUMERIC", json!({"precision": "10", "scale": "2"})),
            column("local_time", "DATETIME", json!({})),
            column("document", "JSON", json!({})),
            column("location", "GEOGRAPHY", json!({})),
        ])
    );
}

/// An undeclared `Decimal256(76, 38)` carries values up to about 5.79e38, which a declared
/// `BIGNUMERIC(76, 38)` (38 integer digits) would refuse, so it loads as a plain BIGNUMERIC.
#[test]
fn the_widest_undeclared_decimal_loads_as_a_plain_bignumeric() {
    let fields = load_fields(vec![Field::new(
        "physical",
        DataType::Decimal256(76, 38),
        true,
    )]);
    assert_eq!(fields, json!([column("physical", "BIGNUMERIC", json!({}))]));
}

#[test]
fn dbt_bigquery_column_type_aliases_are_canonicalized() {
    let fields = load_fields(vec![
        field("text_value", DataType::Int64, "text"),
        field("integer_value", DataType::Utf8, "integer"),
        field("float_value", DataType::Utf8, "float"),
    ]);
    assert_eq!(
        fields,
        json!([
            column("text_value", "STRING", json!({})),
            column("integer_value", "INTEGER", json!({})),
            column("float_value", "FLOAT", json!({})),
        ])
    );
}

#[test]
fn malformed_type_parameters_are_rejected() {
    for (logical_type, status) in [
        ("NUMERIC()", Status::NotImplemented),
        ("NUMERIC(1, 2, 3)", Status::NotImplemented),
        ("STRING(0)", Status::NotImplemented),
        ("STRING<INT64>", Status::NotImplemented),
        ("BYTES(-1)", Status::NotImplemented),
        ("DATE(10)", Status::NotImplemented),
        ("ARRAY<INT64>", Status::NotImplemented),
        ("NUMERIC(0)", Status::InvalidArguments),
        ("NUMERIC(40, 2)", Status::InvalidArguments),
        ("NUMERIC(5, 6)", Status::InvalidArguments),
        ("BIGNUMERIC(77, 38)", Status::InvalidArguments),
    ] {
        let mut options = CsvSeedOptions::default();
        let error = options
            .set_schema(encode_schema_fixture(&Schema::new(vec![field(
                "value",
                DataType::Utf8,
                logical_type,
            )])))
            .unwrap_err();
        assert_eq!(error.status, status, "{logical_type}: {}", error.message);
    }
}
