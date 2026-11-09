//! The dbt 2.0 option-only operations: CSV seeds, table copies, table descriptions and
//! python-model uploads.

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use adbc_core::{Connection, Database, Statement};
use adbc_driver_bigquery::{
    BIGQUERY_SCHEMA_METADATA_QUERY_ID, BigQueryConnection, BigQueryDatabaseOption,
    BigQueryStatement, BigQueryStatementOption, BigQueryWriteDisposition,
};
use arrow_array::{Int64Array, StringArray};
use base64::Engine;

use crate::support::{
    DropTable, SCRATCH_DATASET, TempFile, connect_with_dataset, connect_with_dataset_at,
    count_rows, db_option, execute_sql, open_database, prepare_query, project, read_single_string,
    report_failed_cleanup, scratch_bucket, scratch_table, set_option, single_batch, typed,
};

/// An Arrow IPC stream with the CSV seed schema: `id`, `amount`, `civil`, `payload`, and `label`,
/// typed by `BIGQUERY:type` metadata as INT64, NUMERIC(12, 2), DATETIME, BYTES, and TEXT. Wire
/// bytes because the driver's IPC codec is crate-private.
const CSV_SEED_SCHEMA_IPC_BASE64: &str = concat!(
    "/////3gCAAAQAAAAAAAKAAwACgAJAAQACgAAABAAAAAAAQQACAAIAAAABAAIAAAABAAAAAUAAADIAQAA",
    "RAEAANwAAAB4AAAABAAAAF7+//8gAAAAUAAAAFgAAAAAAAIBBAAAAAUAAABsYWJlbAAAAAEAAAAEAAAA",
    "UP7//wgAAAAQAAAABAAAAFRFWFQAAAAADQAAAEJJR1FVRVJZOnR5cGUAAAAAAAAACAAMAAgABwAIAAAA",
    "AAAAAUAAAADO/v//IAAAAFAAAABQAAAAAAAFAQQAAAAHAAAAcGF5bG9hZAABAAAABAAAAMD+//8IAAAA",
    "EAAAAAUAAABCWVRFUwAAAA0AAABCSUdRVUVSWTp0eXBlAAAAAAAAALj+//8u////IAAAAFQAAABUAAAA",
    "AAAFAQQAAAAFAAAAY2l2aWwAAAABAAAABAAAACD///8IAAAAFAAAAAgAAABEQVRFVElNRQAAAAANAAAA",
    "QklHUVVFUlk6dHlwZQAAAAAAAAAc////kv///yAAAABYAAAAXAAAAAAABQEEAAAABgAAAGFtb3VudAAA",
    "AQAAAAQAAACE////CAAAABgAAAAOAAAATlVNRVJJQygxMiwgMikAAA0AAABCSUdRVUVSWTp0eXBlAAAA",
    "AAAAAAQABgAEAAAAAAASABgAFAATABIADAAAAAgABAASAAAAHAAAAFQAAABYAAAAAAAFAQQAAAACAAAA",
    "aWQAAAEAAAAMAAAACAAMAAgABAAIAAAACAAAABAAAAAFAAAASU5UNjQAAAANAAAAQklHUVVFUlk6dHlw",
    "ZQAAAAAAAAAEAAQABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP////8AAAAA",
);

/// A connection that bills Cloud Storage requests to the scratch bucket's project.
///
/// Cloud Storage bills the credentials' quota project, which for developer ADC may lack billing
/// and then refuses the upload; BigQuery bills the job's project instead.
fn connect_to_storage() -> BigQueryConnection {
    open_database([
        db_option(BigQueryDatabaseOption::ProjectId, project()),
        db_option(
            BigQueryDatabaseOption::AuthenticationQuotaProject,
            project(),
        ),
    ])
    .new_connection()
    .unwrap()
}

/// A statement uploading `content` to `bucket` as `object`, the way dbt 2.0 writes a python
/// model source.
fn gcs_upload(
    connection: &mut BigQueryConnection,
    bucket: &str,
    object: &str,
    content: &str,
) -> BigQueryStatement {
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteGcsBucket,
        bucket,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteGcsObjectName,
        object,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::WriteGcsContent,
        content,
    );
    statement
}

/// A unique object name shaped like dbt 2.0's python model source, `{schema}/{alias}.py`.
fn scratch_object(prefix: &str) -> String {
    format!("{SCRATCH_DATASET}/{}.py", scratch_table(prefix))
}

/// The unreserved characters, so an object name reaches Cloud Storage as one path segment with
/// its slashes escaped, as `src/dbt/gcs.rs` does for the upload's `name` parameter.
const OBJECT_PATH_ESCAPE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Issues one Cloud Storage JSON API request with ADC and returns its status and body. The driver
/// cannot read objects back, so verification needs its own client; the quota project is pinned
/// as in [`connect_to_storage`].
///
/// Returns errors rather than panicking so [`DeleteObject`] can use it while unwinding.
fn storage_request(
    method: reqwest::Method,
    url: String,
) -> Result<(reqwest::StatusCode, Vec<u8>), Box<dyn std::error::Error>> {
    // The driver installs this at runtime creation; a test may reach Cloud Storage first.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let credentials = google_cloud_auth::credentials::Builder::default()
            .with_quota_project_id(project())
            .build()?;
        let headers = credentials.headers(http::Extensions::new()).await?;
        let google_cloud_auth::credentials::CacheableResource::New { data: headers, .. } = headers
        else {
            return Err(
                "Application Default Credentials returned no headers for a fresh request"
                    .to_owned()
                    .into(),
            );
        };
        let response = reqwest::Client::builder()
            .build()?
            .request(method, url)
            .headers(headers)
            .send()
            .await?;
        let status = response.status();
        Ok((status, response.bytes().await?.to_vec()))
    })
}

/// The Cloud Storage JSON API URL of one object, with the name escaped into the path.
fn object_url(object_name: &str, suffix: &str) -> String {
    let scratch_bucket = scratch_bucket();
    format!(
        "https://storage.googleapis.com/storage/v1/b/{scratch_bucket}/o/{}{suffix}",
        percent_encoding::utf8_percent_encode(object_name, OBJECT_PATH_ESCAPE)
    )
}

/// Deletes a scratch object when the test that wrote it ends, assertion failure included.
struct DeleteObject(String);

impl Drop for DeleteObject {
    fn drop(&mut self) {
        let scratch_bucket = scratch_bucket();
        let deleted = match storage_request(reqwest::Method::DELETE, object_url(&self.0, "")) {
            Ok((status, _)) if status.is_success() || status == reqwest::StatusCode::NOT_FOUND => {
                return;
            }
            Ok((status, _)) => format!("HTTP {status}"),
            Err(error) => error.to_string(),
        };
        report_failed_cleanup(
            &format!("delete gs://{scratch_bucket}/{}", self.0),
            &deleted,
        );
    }
}

/// The two-row seed that [`CSV_SEED_SCHEMA_IPC_BASE64`] describes.
///
/// Load schemas map CSV fields by position after skipping the first line, so header names do not
/// matter. BigQuery rounds parameterized decimals, decodes BYTES as Base64, and keeps a literal
/// NULL as STRING data when no null marker is set.
const SEED_CSV: &[u8] = b"ignored,ignored,ignored,ignored,ignored\n\
    7,1.239,2026-01-02 03:04:05,SGk=,NULL\n\
    8,-1.235,2026-01-03 04:05:06,AAE=,\"\"\n";

/// A statement configured the way dbt 2.0 seeds `csv` into `table`, with the Arrow IPC `schema`,
/// by default [`CSV_SEED_SCHEMA_IPC_BASE64`].
fn seed_statement(
    connection: &mut BigQueryConnection,
    table: &str,
    csv: &TempFile,
    schema: Option<Vec<u8>>,
) -> BigQueryStatement {
    let schema = schema.unwrap_or_else(|| {
        base64::engine::general_purpose::STANDARD
            .decode(CSV_SEED_SCHEMA_IPC_BASE64)
            .unwrap()
    });
    let mut seed = connection.new_statement().unwrap();
    set_option(&mut seed, BigQueryStatementOption::DestinationTable, table);
    set_option(
        &mut seed,
        BigQueryStatementOption::CsvSchema,
        OptionValue::Bytes(schema),
    );
    set_option(
        &mut seed,
        BigQueryStatementOption::CsvFilepath,
        csv.0.to_string_lossy().into_owned(),
    );
    seed.set_sql_query("").unwrap();
    seed
}

#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn loads_dbt_csv_seeds_atomically_with_bigquery_csv_semantics() {
    let table = scratch_table("csv_seed");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset_at("US");
    let csv = TempFile::new(&format!("{table}.csv"), SEED_CSV);

    let mut seed = seed_statement(&mut connection, &table, &csv, None);
    let loaded = seed.execute_partitions().unwrap();
    assert_eq!(loaded.rows_affected, 2);
    let job_id = &loaded.schema.metadata()[BIGQUERY_SCHEMA_METADATA_QUERY_ID];
    assert!(job_id.starts_with("adbc_csv_"), "{job_id}");

    let mut verify = prepare_query(
        &mut connection,
        &format!(
            "SELECT id, CAST(amount AS STRING), CAST(civil AS STRING), TO_HEX(payload), label \
             FROM `{table}` ORDER BY id"
        ),
    );
    let batch = single_batch(&mut verify);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        typed::<Int64Array>(batch.column(0).as_ref()).values(),
        &[7, 8]
    );
    let strings = |column: usize| {
        typed::<StringArray>(batch.column(column).as_ref())
            .iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(strings(1), [Some("1.24"), Some("-1.24")]);
    assert_eq!(
        strings(2),
        [Some("2026-01-02 03:04:05"), Some("2026-01-03 04:05:06")]
    );
    assert_eq!(strings(3), [Some("4869"), Some("0001")]);
    assert_eq!(strings(4), [Some("NULL"), Some("")]);

    // The extension defaults to WRITE_EMPTY, unlike the Go fork's implicit WRITE_APPEND.
    assert_eq!(
        seed.execute_update().unwrap_err().status,
        Status::AlreadyExists
    );

    // A truncating reload, of a CRLF seed whose quoted fields hold newlines and quotes.
    let quoted = TempFile::new(
        &format!("{table}_newlines.csv"),
        b"id,amount,civil,payload,label\r\n\
          1,1.00,2026-01-02 03:04:05,SGk=,\"first\nline, with \"\"quotes\"\"\"\r\n\
          2,2.00,2026-01-02 03:04:05,SGk=,\"second\r\nline\"\r\n\
          3,3.00,2026-01-02 03:04:05,SGk=,plain\r\n",
    );
    set_option(
        &mut seed,
        BigQueryStatementOption::WriteDisposition,
        BigQueryWriteDisposition::WriteTruncate,
    );
    set_option(
        &mut seed,
        BigQueryStatementOption::CsvFilepath,
        quoted.0.to_string_lossy().into_owned(),
    );
    assert_eq!(seed.execute_update().unwrap(), Some(3));

    let mut verify = prepare_query(
        &mut connection,
        &format!("SELECT id, label FROM `{table}` ORDER BY id"),
    );
    let batch = single_batch(&mut verify);
    assert_eq!(
        typed::<Int64Array>(batch.column(0).as_ref()).values(),
        &[1, 2, 3]
    );
    assert_eq!(
        typed::<StringArray>(batch.column(1).as_ref())
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("first\nline, with \"quotes\""),
            Some("second\nline"),
            Some("plain"),
        ]
    );
}

/// dbt's `column_types` reach the table exactly as declared, as `CREATE TABLE` would declare them:
/// a plain `numeric` stays parameter-free and parameterized types keep their parameters.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn seed_column_types_are_declared_exactly() {
    let project = project();
    let table = scratch_table("csv_seed_types");
    let _drop_table = DropTable::table(&table);
    let mut connection = connect_with_dataset_at("US");
    let declared = [
        ("plain", "numeric"),
        ("parameterized", "numeric(20,4)"),
        ("widest", "bignumeric(76,38)"),
        ("huge", "bignumeric"),
        ("short", "string(10)"),
    ];
    let schema = arrow_schema::Schema::new(
        declared
            .iter()
            .map(|(name, column_type)| {
                arrow_schema::Field::new(*name, arrow_schema::DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::from([(
                        "BIGQUERY:type".to_owned(),
                        (*column_type).to_owned(),
                    )]),
                )
            })
            .collect::<Vec<_>>(),
    );
    let mut encoded_schema = Vec::new();
    arrow_ipc::writer::StreamWriter::try_new(&mut encoded_schema, &schema)
        .unwrap()
        .finish()
        .unwrap();
    let csv = TempFile::new(
        &format!("{table}.csv"),
        b"a,b,c,d,e\n1.5,1.2345,1.5,1.5,short\n",
    );

    let mut seed = seed_statement(&mut connection, &table, &csv, Some(encoded_schema));
    assert_eq!(seed.execute_update().unwrap(), Some(1));

    let mut columns = prepare_query(
        &mut connection,
        &format!(
            "SELECT STRING_AGG(CONCAT(column_name, ' ', data_type), ', ' ORDER BY \
             ordinal_position) FROM `{project}.{SCRATCH_DATASET}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE table_name = '{table}'"
        ),
    );
    assert_eq!(
        read_single_string(&mut columns),
        "plain NUMERIC, parameterized NUMERIC(20, 4), widest BIGNUMERIC(76, 38), huge \
         BIGNUMERIC, short STRING(10)"
    );
}

/// Runs one option-only table copy, the way dbt 2.0 configures it.
fn copy_table(
    connection: &mut BigQueryConnection,
    source: &str,
    destination: &str,
    write_disposition: BigQueryWriteDisposition,
) -> Result<Option<i64>, adbc_core::error::Error> {
    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableSource,
        source,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableDestination,
        destination,
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::CopyTableWriteDisposition,
        write_disposition,
    );
    // dbt 2.0 finalizes the operation with an empty query, never SQL.
    statement.set_sql_query("").unwrap();
    let copied = statement.execute_partitions()?;
    let job_id = &copied.schema.metadata()[BIGQUERY_SCHEMA_METADATA_QUERY_ID];
    assert!(job_id.starts_with("adbc_copy_"), "{job_id}");
    Ok((copied.rows_affected >= 0).then_some(copied.rows_affected))
}

/// dbt 2.0's `copy_partitions` path: partition-decorated copies, which no SQL statement can
/// address, plus the whole-table copies its `copy` materialization asks for.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn copies_partitions_and_tables_without_running_a_query() {
    let project = project();
    let source = scratch_table("copy_source");
    let target = scratch_table("copy_target");
    let created = scratch_table("copy_created");
    let _drop_source = DropTable::table(&source);
    let _drop_target = DropTable::table(&target);
    let _drop_created = DropTable::table(&created);
    let mut connection = connect_with_dataset_at("US");

    execute_sql(
        &mut connection,
        &format!(
            "CREATE TABLE `{source}` (day DATE, id INT64) PARTITION BY day CLUSTER BY id AS \
             SELECT DATE '2024-01-01', 1 UNION ALL SELECT DATE '2024-01-01', 2 \
             UNION ALL SELECT DATE '2024-01-02', 3"
        ),
    );
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{target}` (day DATE, id INT64) PARTITION BY day"),
    );

    // One partition of the source replaces the same partition of the target: 2 rows, not the
    // source's 3. Repeating it truncates rather than accumulating; appending then adds to the
    // same partition.
    for (disposition, rows) in [
        (BigQueryWriteDisposition::WriteTruncate, 2),
        (BigQueryWriteDisposition::WriteTruncate, 2),
        (BigQueryWriteDisposition::WriteAppend, 4),
    ] {
        assert_eq!(
            copy_table(
                &mut connection,
                &format!("{source}$20240101"),
                &format!("{target}$20240101"),
                disposition,
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(count_rows(&mut connection, &target), rows);
    }

    // A whole-table copy creates the destination and keeps the physical layout a CTAS would drop.
    assert_eq!(
        copy_table(
            &mut connection,
            &source,
            &created,
            BigQueryWriteDisposition::WriteTruncate,
        )
        .unwrap(),
        Some(3)
    );
    let mut layout = prepare_query(
        &mut connection,
        &format!(
            "SELECT STRING_AGG(column_name ORDER BY column_name) \
             FROM `{project}.{SCRATCH_DATASET}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE table_name = '{created}' \
               AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL)"
        ),
    );
    assert_eq!(read_single_string(&mut layout), "day,id");

    // WRITE_EMPTY is the default, so an unconfigured disposition refuses to overwrite data.
    let error = copy_table(
        &mut connection,
        &source,
        &created,
        BigQueryWriteDisposition::WriteEmpty,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists);

    // BigQuery rejects a missing source at submission, so the error carries the HTTP status
    // rather than a failed job's `{reason}: {message}`.
    let missing = scratch_table("copy_missing");
    let error = copy_table(
        &mut connection,
        &missing,
        &created,
        BigQueryWriteDisposition::WriteTruncate,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains(&missing), "{}", error.message);
}

/// dbt 2.0 sets a materialized view's description right after creating it. The view's
/// automatic initial refresh changes its ETag a second or two after creation, while ETag reads
/// briefly still return the old one, so an update guarded by `If-Match` failed in that window.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn updates_a_materialized_view_description_during_its_initial_refresh() {
    let project = project();
    const DESCRIPTION: &str = "Documented while the initial refresh runs.";
    let base = scratch_table("mv_description_base");
    let materialized_view = format!("{base}_mv");
    let _drop_base = DropTable::table(&base);
    let _drop_materialized_view = DropTable::new("MATERIALIZED VIEW", &materialized_view);
    let mut connection = connect_with_dataset_at("US");

    execute_sql(
        &mut connection,
        &format!("CREATE TABLE `{base}` AS SELECT 1 AS id"),
    );
    // A materialized view's base table must be dataset-qualified even with a default dataset.
    execute_sql(
        &mut connection,
        &format!(
            "CREATE MATERIALIZED VIEW `{materialized_view}` AS \
             SELECT id, COUNT(*) AS rows_seen FROM `{SCRATCH_DATASET}.{base}` GROUP BY id"
        ),
    );
    let created = std::time::Instant::now();
    std::thread::sleep(std::time::Duration::from_millis(1_500));

    let mut statement = connection.new_statement().unwrap();
    set_option(
        &mut statement,
        BigQueryStatementOption::DestinationTable,
        format!("{project}.{SCRATCH_DATASET}.{materialized_view}"),
    );
    set_option(
        &mut statement,
        BigQueryStatementOption::UpdateDescription,
        DESCRIPTION,
    );
    statement.set_sql_query("").unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(0));

    // Read back after the refresh has landed, so it cannot have replaced the description.
    std::thread::sleep(std::time::Duration::from_secs(6).saturating_sub(created.elapsed()));
    let metadata = connection
        .get_table_schema(None, None, &materialized_view)
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(
        metadata.get("Description").map(String::as_str),
        Some(DESCRIPTION)
    );
}

/// A dbt profile may omit `location`. A seed and a copy then run where the scratch dataset is,
/// which the driver looks up rather than requiring `bigquery.location`.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch dataset"]
fn seeds_and_copies_without_a_location_run_where_the_dataset_is() {
    let seeded = scratch_table("unlocated_seed");
    let copied = scratch_table("unlocated_copy");
    let _drop_seeded = DropTable::table(&seeded);
    let _drop_copied = DropTable::table(&copied);
    let mut connection = connect_with_dataset();
    let csv = TempFile::new(&format!("{seeded}.csv"), SEED_CSV);

    let mut seed = seed_statement(&mut connection, &seeded, &csv, None);
    assert_eq!(seed.execute_update().unwrap(), Some(2));
    assert_eq!(
        copy_table(
            &mut connection,
            &seeded,
            &copied,
            BigQueryWriteDisposition::WriteTruncate,
        )
        .unwrap(),
        Some(2)
    );
    assert_eq!(count_rows(&mut connection, &copied), 2);
}

/// The location lookup against real `datasets.get` responses. Telling a missing dataset from a
/// missing project depends on the wording of two BigQuery 404s, which only a live run can pin.
#[test]
#[ignore = "requires Google Cloud ADC and read access to the integration project"]
fn looks_up_dataset_locations_without_running_a_job() {
    crate::backend::require_real_bigquery();
    let mut connection = connect_with_dataset();
    let mut lookup = |project: &str, dataset: &str| {
        let mut statement = connection.new_statement().unwrap();
        set_option(
            &mut statement,
            BigQueryStatementOption::GetDatasetLocation,
            serde_json::json!({"project": project, "dataset": dataset}).to_string(),
        );
        statement.set_sql_query("").unwrap();
        statement.execute().map(|reader| {
            reader
                .map(|batch| batch.unwrap())
                .flat_map(|batch| {
                    let locations = typed::<StringArray>(batch.column(0));
                    locations
                        .iter()
                        .flatten()
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
    };

    assert_eq!(lookup("bigquery-public-data", "samples").unwrap(), ["US"]);
    let scratch = lookup(project(), SCRATCH_DATASET).unwrap();
    assert_eq!(scratch.len(), 1);
    assert!(!scratch[0].is_empty());

    // A name BigQuery could never hold draws the same 404 as an absent dataset.
    for dataset in ["adbc_no_such_dataset", "adbc-not-a-dataset-name"] {
        assert!(lookup(project(), dataset).unwrap().is_empty(), "{dataset}");
    }

    let failure = lookup("adbc-no-such-project-9182", "any").unwrap_err();
    assert_eq!(failure.status, Status::NotFound);
    assert!(
        failure
            .message
            .contains("Project adbc-no-such-project-9182 is not found"),
        "{}",
        failure.message
    );
}

/// dbt 2.0's Python-model upload against the Cloud Storage JSON API.
///
/// Checks the stored source bytes and nested object key, plus the unknown affected-row count.
/// Request construction is tested separately in `src/dbt/gcs/tests.rs`.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch bucket"]
fn writes_a_dbt_python_model_source_to_cloud_storage() {
    crate::backend::require_real_bigquery();
    // A compiled python model is arbitrary text: non-ASCII identifiers and comments, CRLF and LF
    // line endings, and a trailing newline all have to survive the multipart upload byte for byte.
    const CONTENT: &str = "# -*- coding: utf-8 -*-\n\
                           # naïve café — ünicode ✅ 汉字 \u{1f40d}\r\n\
                           def model(dbt, session):\n\
                               dbt.config(materialized=\"table\")\n\
                               return session.table(\"raw_τ\")\n";

    let object_name = scratch_object("write_gcs");
    // The name is a path, which the driver escapes whole into the upload's query string. Cloud
    // Storage must store it with its slashes intact rather than a literal `%2F` in the key.
    assert!(object_name.contains('/'), "{object_name}");
    let _delete_object = DeleteObject(object_name.clone());

    let mut statement = gcs_upload(
        &mut connect_to_storage(),
        scratch_bucket(),
        &object_name,
        CONTENT,
    );
    // dbt 2.0 finalizes the statement with a real, executable query, which must not run.
    statement.set_sql_query("SELECT 1").unwrap();

    // An object write has no row count: `None` here, -1 over the C ABI. The dbt-labs fork says 0.
    assert_eq!(
        statement.execute_update().unwrap(),
        None,
        "an object write affects an unknown number of rows, not zero"
    );

    let (status, metadata) =
        storage_request(reqwest::Method::GET, object_url(&object_name, "")).unwrap();
    assert!(status.is_success(), "HTTP {status}");
    let metadata: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
    assert_eq!(
        metadata["name"].as_str(),
        Some(object_name.as_str()),
        "the nested object name round-trips unmangled: {metadata}"
    );
    assert_eq!(metadata["bucket"].as_str(), Some(scratch_bucket()));
    assert_eq!(
        metadata["size"].as_str().map(str::parse::<usize>),
        Some(Ok(CONTENT.len())),
        "Cloud Storage stored a different number of bytes than were uploaded"
    );

    let (status, stored) =
        storage_request(reqwest::Method::GET, object_url(&object_name, "?alt=media")).unwrap();
    assert!(status.is_success(), "HTTP {status}");
    assert_eq!(
        std::str::from_utf8(&stored),
        Ok(CONTENT),
        "the model source must be stored verbatim"
    );
}

/// A bucket the caller cannot write to keeps the status Cloud Storage reported, where dbt
/// 2.0's default driver flattens every python-model failure to UNKNOWN. The bucket never exists.
#[test]
#[ignore = "requires Google Cloud ADC and write access to the integration project scratch bucket"]
fn a_bucket_the_caller_cannot_write_to_reports_the_cloud_storage_status() {
    crate::backend::require_real_bigquery();
    let bucket = scratch_table("write_gcs_missing").replace('_', "-");
    let mut statement = gcs_upload(
        &mut connect_to_storage(),
        &bucket,
        "sq_scratch/never_written.py",
        "print(1)\n",
    );

    let error = statement.execute_update().unwrap_err();
    assert!(
        matches!(error.status, Status::NotFound | Status::Unauthorized),
        "a bucket that cannot be written must keep the service's own status, not be flattened \
         to {:?}: {}",
        error.status,
        error.message
    );
    assert!(
        error.message.contains(&format!("gs://{bucket}/")),
        "{}",
        error.message
    );
}
