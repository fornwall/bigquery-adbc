//! Statement options: what each one round-trips through, what it rejects, and what an error is
//! allowed to say about a rejected value.

use super::*;
use crate::BigQueryStatementOption as BqOption;
use crate::dbt::statement::DbtOperation;
use crate::test_support::driver::test_statement_at;
use crate::test_support::errors::{assert_message, assert_message_omits};
use crate::test_support::http::job_reference;

#[test]
fn reservation_round_trips_clears_and_rejects_wrong_types_and_legacy_alias() {
    let mut statement = test_statement();
    let key = BqOption::Reservation;
    // Unset, and cleared by the empty string, it has no value to read.
    assert_string_option(&statement, key, None);
    for value in [
        "projects/billing-project/locations/EU/reservations/analytics",
        "none",
    ] {
        set_option(&mut statement, key, value);
        assert_string_option(&statement, key, Some(value));
    }
    set_option(&mut statement, key, "");
    assert_string_option(&statement, key, None);
    set_option(&mut statement, key, "none");
    for value in [
        OptionValue::Int(1),
        OptionValue::Bytes(vec![]),
        OptionValue::Double(1.0),
    ] {
        let error = set_option_error(&mut statement, key, value);
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(option_string(&statement, key), "none");
    }
    assert!(statement.get_option_int(key.into()).is_err());
    let legacy = OptionStatement::Other("adbc.bigquery.sql.query.reservation".to_owned());
    assert_eq!(
        set_option_error(&mut statement, legacy.clone(), "none").status,
        Status::NotImplemented
    );
    assert_string_option(&statement, legacy, None);
}

/// `bigquery.query.unordered_read` is plain result-path state: off by default, validated as
/// an ADBC boolean string, and read back through the key that set it.
#[test]
fn unordered_read_round_trips_and_rejects_invalid_values() {
    let mut connection = test_connection();
    let mut statement = connection.new_statement().unwrap();
    let key = BqOption::UnorderedRead;

    assert_eq!(
        option_string(&statement, key),
        ADBC_OPTION_VALUE_DISABLED,
        "results are ordered unless the caller opts out"
    );

    set_option(&mut statement, key, ADBC_OPTION_VALUE_ENABLED.to_owned());
    assert_eq!(option_string(&statement, key), ADBC_OPTION_VALUE_ENABLED);

    let error = set_option_error(&mut statement, key, "fast");
    assert_message(&error, &["bigquery.query.unordered_read"]);

    let error = set_option_error(&mut statement, key, OptionValue::Int(1));
    assert_message(&error, &["requires a string value"]);
}

/// The dbt option reports configuration even when automatic fallback has cached a refusal.
#[test]
fn storage_api_disabled_client_round_trips_without_changing_a_recorded_refusal() {
    let mut connection = test_connection();
    let mut statement = connection.new_statement().unwrap();
    let key = BqOption::UseStorageApiDisabledClient;

    assert_eq!(option_string(&statement, key), ADBC_OPTION_VALUE_DISABLED);
    assert!(statement.context.result_options.use_storage_read);
    let legacy = OptionStatement::Other(
        "adbc.bigquery.sql.query.use_storage_api_disabled_client".to_owned(),
    );
    set_option(&mut statement, legacy.clone(), "true");
    assert_eq!(option_string(&statement, key), "true");
    assert_eq!(option_string(&statement, legacy), "true");
    for (value, enabled) in [("true", false), ("false", true)] {
        set_option(&mut statement, key, value.to_owned());
        assert_eq!(option_string(&statement, key), value);
        assert_eq!(statement.context.result_options.use_storage_read, enabled);
    }
    for value in [
        OptionValue::String("invalid".to_owned()),
        OptionValue::Int(1),
    ] {
        assert_eq!(
            set_option_error(&mut statement, key, value).status,
            Status::InvalidArguments
        );
        assert_eq!(option_string(&statement, key), ADBC_OPTION_VALUE_DISABLED);
    }

    statement
        .context
        .state
        .disable_storage_read("fixture-project");
    for value in ["true", "false"] {
        set_option(&mut statement, key, value.to_owned());
        assert_eq!(option_string(&statement, key), value);
        assert!(
            statement
                .context
                .state
                .storage_read_disabled("fixture-project")
        );
    }
}

#[test]
fn connection_and_statement_option_errors_do_not_disclose_payloads() {
    const SECRET: &str = "object-option-payload-secret-sentinel";

    let mut connection = test_connection();
    for (key, value) in [
        (
            OptionConnection::AutoCommit,
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BigQueryConnectionOption::BulkIngestMethod.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BigQueryConnectionOption::PrefetchConcurrency.into(),
            OptionValue::Bytes(SECRET.as_bytes().to_vec()),
        ),
    ] {
        let rendered = format!("{value:?}");
        let error = set_option_error(&mut connection, key, value);
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(&error, &[SECRET, &rendered]);
    }

    let mut statement = connection.new_statement().unwrap();
    for (key, value) in [
        (
            OptionStatement::IngestMode,
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BqOption::Priority.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BqOption::CreateSession.into(),
            OptionValue::String(SECRET.to_owned()),
        ),
        (
            BqOption::MaximumBytesBilled.into(),
            OptionValue::Bytes(SECRET.as_bytes().to_vec()),
        ),
        (
            BqOption::DestinationTable.into(),
            OptionValue::String(format!("{SECRET}/path.dataset.table")),
        ),
    ] {
        let rendered = format!("{value:?}");
        let error = set_option_error(&mut statement, key, value);
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(&error, &[SECRET, &rendered]);
    }
}

#[test]
fn spec_default_standard_option_values_are_accepted_as_no_ops() {
    use adbc_core::constants::{
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT, ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
    };

    let mut connection = test_connection();
    set_option(&mut connection, OptionConnection::ReadOnly, "false");
    assert_eq!(
        set_option_error(&mut connection, OptionConnection::ReadOnly, "true").status,
        Status::NotImplemented
    );
    set_option(
        &mut connection,
        OptionConnection::IsolationLevel,
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
    );
    assert_eq!(
        set_option_error(
            &mut connection,
            OptionConnection::IsolationLevel,
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
        )
        .status,
        Status::NotImplemented
    );
    assert_eq!(
        option_string(&connection, OptionConnection::ReadOnly),
        ADBC_OPTION_VALUE_DISABLED
    );
    assert_eq!(
        option_string(&connection, OptionConnection::IsolationLevel),
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
        "BigQuery transactions run at snapshot isolation, which is what the getter names"
    );
    set_option(
        &mut connection,
        OptionConnection::IsolationLevel,
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
    );

    let mut statement = connection.new_statement().unwrap();
    set_option(&mut statement, OptionStatement::Incremental, "false");
    assert_eq!(
        set_option_error(&mut statement, OptionStatement::Incremental, "true").status,
        Status::NotImplemented
    );
    assert_eq!(
        option_string(&statement, OptionStatement::Incremental),
        ADBC_OPTION_VALUE_DISABLED
    );
}

/// Runs the bulk-ingest-method option assertions shared by both option scopes.
fn assert_bulk_ingest_method_scope<O: Optionable>(
    target: &mut O,
    key: impl Into<O::Option> + Copy,
) {
    assert_eq!(
        option_string(target, key),
        BigQueryBulkIngestMethod::StorageWrite.as_str()
    );
    set_option(target, key, BigQueryBulkIngestMethod::StorageWrite);
    for (value, expected) in [
        (
            OptionValue::String("load".to_owned()),
            Status::NotImplemented,
        ),
        (
            OptionValue::String("unknown".to_owned()),
            Status::InvalidArguments,
        ),
        (OptionValue::Int(1), Status::InvalidArguments),
    ] {
        assert_eq!(set_option_error(target, key, value).status, expected);
    }
    assert_eq!(
        target.get_option_int(key.into()).unwrap_err().status,
        Status::NotFound
    );
}

#[test]
fn bulk_ingest_method_is_storage_write_at_connection_and_statement_scope() {
    let connection_option = BigQueryConnectionOption::BulkIngestMethod;
    let statement_option = BqOption::BulkIngestMethod;
    let mut connection = test_connection();
    assert_bulk_ingest_method_scope(&mut connection, connection_option);
    let mut statement = connection.new_statement().unwrap();
    assert_bulk_ingest_method_scope(&mut statement, statement_option);

    let connection_key: OptionConnection = connection_option.into();
    let statement_key: OptionStatement = statement_option.into();
    assert_eq!(connection_key.as_ref(), statement_key.as_ref());
}

/// Rejections shared by both scopes: non-positive and wrongly typed prefetch values.
fn assert_prefetch_rejects_invalid<O: Optionable>(
    target: &mut O,
    key: impl Into<O::Option> + Copy,
) {
    for value in [
        OptionValue::Int(0),
        OptionValue::Int(-1),
        OptionValue::String("five".to_owned()),
    ] {
        assert_eq!(
            set_option_error(target, key, value).status,
            Status::InvalidArguments
        );
    }
}

#[test]
fn query_prefetch_concurrency_validates_round_trips_and_snapshots_connection_default() {
    let mut connection = test_connection();
    let connection_option = BigQueryConnectionOption::PrefetchConcurrency;
    let statement_option = BqOption::PrefetchConcurrency;
    assert_eq!(
        option_int(&connection, connection_option),
        crate::pipeline::QueryResultOptions::default().prefetch_concurrency_option_value()
    );
    assert_string_option(&connection, connection_option, None);

    set_option(
        &mut connection,
        OptionConnection::Other(connection_option.legacy_aliases()[0].to_owned()),
        OptionValue::Int(3),
    );
    let mut statement = connection.new_statement().unwrap();
    assert_eq!(option_int(&statement, statement_option), 3);

    set_option(&mut connection, connection_option, OptionValue::Int(11));
    assert_eq!(
        option_int(&statement, statement_option),
        3,
        "a statement snapshots the connection default at creation"
    );
    set_option(&mut statement, statement_option, OptionValue::Int(2));
    assert_eq!(option_int(&statement, statement_option), 2);

    assert_prefetch_rejects_invalid(&mut connection, connection_option);
    assert_prefetch_rejects_invalid(&mut statement, statement_option);
    assert_eq!(
        option_int(&connection, connection_option),
        11,
        "rejected values must not mutate the connection"
    );
    assert_eq!(
        option_int(&statement, statement_option),
        2,
        "rejected values must not mutate the statement"
    );
}

#[test]
fn result_buffer_size_is_not_exposed() {
    let mut connection = test_connection();
    let mut statement = connection.new_statement().unwrap();

    for name in [
        "bigquery.query.result_buffer_size",
        "adbc.bigquery.sql.query.result_buffer_size",
    ] {
        let connection_key = OptionConnection::Other(name.to_owned());
        assert_eq!(
            set_option_error(&mut connection, connection_key.clone(), OptionValue::Int(1)).status,
            Status::NotImplemented
        );
        assert_eq!(
            connection
                .get_option_int(connection_key)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        let statement_key = OptionStatement::Other(name.to_owned());
        assert_eq!(
            set_option_error(&mut statement, statement_key.clone(), OptionValue::Int(1)).status,
            Status::NotImplemented
        );
        assert_eq!(
            statement.get_option_int(statement_key).unwrap_err().status,
            Status::NotFound
        );
    }
}

/// Exhaustive so a new option must supply a round-trip sample.
fn option_sample(option: BigQueryStatementOption) -> OptionValue {
    match option {
        BqOption::AuthorizeViewToDatasets => {
            r#"{"fixture-project.analytics.model":[{"project":"fixture-project","dataset":"raw"}]}"#
                .into()
        }
        BqOption::BulkIngestMethod => BigQueryBulkIngestMethod::StorageWrite.into(),
        BqOption::CopyTableDestination => "data_set.copy_out".into(),
        BqOption::CopyTableSource => "data_set.copy_in".into(),
        BqOption::CopyTableWriteDisposition => BigQueryWriteDisposition::WriteTruncate.into(),
        BqOption::CreateBatchBatchId => "batch-id".into(),
        BqOption::CreateBatchBatchYml => "runtimeConfig: {}".into(),
        BqOption::CreateBatchParent => "projects/fixture-project/locations/eu".into(),
        BqOption::GetDatasetLocation => r#"{"project":"fixture-project","dataset":"raw"}"#.into(),
        BqOption::CsvDelimiter => ";".into(),
        BqOption::CsvFilepath => "/tmp/seed.csv".into(),
        BqOption::CsvSchema => {
            encode_schema_fixture(&Schema::new(vec![Field::new("id", DataType::Int64, true)]))
                .into()
        }
        BqOption::DataprocComputeRegion | BqOption::NotebookExecuteJobRegion => {
            "europe-west1".into()
        }
        BqOption::DataprocPoolingTimeout => 30.into(),
        BqOption::DataprocProject => "dataproc-project".into(),
        BqOption::DataprocSubmitJobClusterName => "cluster".into(),
        BqOption::DataprocSubmitJobGcsPath => "gs://bucket/job.py".into(),
        BqOption::CreateDisposition => BigQueryCreateDisposition::CreateNever.into(),
        BqOption::CreateSession
        | BqOption::DisableQueryCache
        | BqOption::DryRun
        | BqOption::LinkFailedJob
        | BqOption::UnorderedRead
        | BqOption::UseStorageApiDisabledClient => true.into(),
        BqOption::DefaultDatasetId => "data_set".into(),
        BqOption::DefaultProjectId => "data-project".into(),
        BqOption::DestinationTable => "data_set.result".into(),
        BqOption::JobTimeout => 12_345.into(),
        BqOption::Reservation => "none".into(),
        BqOption::Labels => r#"{"team":"data"}"#.into(),
        BqOption::MaximumBillingTier => 1.into(),
        BqOption::MaximumBytesBilled => 1_000_000.into(),
        BqOption::NotebookExecuteJobGscBucket => "notebook-bucket".into(),
        BqOption::NotebookExecuteJobGscPath => "models/model.ipynb".into(),
        BqOption::NotebookExecuteJobModelFileName => "model.ipynb".into(),
        BqOption::NotebookExecuteJobModelName => "model".into(),
        BqOption::NotebookExecuteJobParent => {
            "projects/fixture-project/locations/europe-west1".into()
        }
        BqOption::NotebookExecuteJobProject => "notebook-project".into(),
        BqOption::NotebookExecuteJobServiceAccount => {
            "runner@fixture-project.iam.gserviceaccount.com".into()
        }
        BqOption::NotebookExecuteJobTemplateId => "template-id".into(),
        BqOption::ParameterMode => ParameterMode::Named.as_str().into(),
        BqOption::UpdateColumnsDescription => r#"{"a":"first"}"#.into(),
        BqOption::UpdateColumnsPolicyTags => {
            r#"{"a":["projects/p/locations/eu/taxonomies/t/policyTags/x"]}"#.into()
        }
        BqOption::UpdateDescription => "the model table".into(),
        BqOption::PrefetchConcurrency => 4.into(),
        BqOption::Priority => BigQueryQueryPriority::Batch.into(),
        BqOption::WriteDisposition => BigQueryWriteDisposition::WriteAppend.into(),
        BqOption::WriteGcsBucket => "dbt-python".into(),
        BqOption::WriteGcsContent => "print(1)".into(),
        BqOption::WriteGcsObjectName => "models/model.py".into(),
    }
}

/// Each option gets its own statement because setters can switch execution modes.
#[test]
fn every_statement_option_round_trips_through_its_declared_kind() {
    use crate::options::OptionKind;

    let mut connection = test_connection();
    for &option in BqOption::ALL {
        let mut statement = connection.new_statement().unwrap();
        let name = option.as_str();
        let value = option_sample(option);
        // Deprecated by Google; recognized but refused in both directions.
        if option == BqOption::MaximumBillingTier {
            assert_eq!(
                set_option_error(&mut statement, option, value).status,
                Status::NotImplemented,
                "{name}"
            );
            continue;
        }
        set_option(&mut statement, option, value.clone());
        match value {
            OptionValue::String(value) => {
                let expected = match option {
                    BqOption::AuthorizeViewToDatasets => {
                        r#"{"fixture-project.analytics.model":[{"dataset":"raw","project":"fixture-project"}]}"#
                    }
                    BqOption::CopyTableSource => "fixture-project.data_set.copy_in",
                    BqOption::CopyTableDestination => "fixture-project.data_set.copy_out",
                    BqOption::DestinationTable => "fixture-project.data_set.result",
                    _ => &value,
                };
                assert_eq!(option.option_kind(), OptionKind::String, "{name}");
                assert_eq!(option_string(&statement, option), expected, "{name}");
            }
            OptionValue::Int(value) => {
                assert_eq!(option.option_kind(), OptionKind::Int, "{name}");
                assert_eq!(option_int(&statement, option), value, "{name}");
            }
            OptionValue::Bytes(value) => {
                assert_eq!(option.option_kind(), OptionKind::Bytes, "{name}");
                assert_eq!(statement.get_option_bytes(option.into()).unwrap(), value);
            }
            other => panic!("unexpected sample for {name}: {other:?}"),
        }
    }
}

/// What a statement answers before any setter runs, and what a cleared option falls back to.
/// Kept apart from the exhaustive round trip because both depend on the connection and on the
/// order the options are applied in.
#[test]
fn statement_options_report_their_defaults_and_clear_back_to_them() {
    let mut statement = test_statement();
    assert_eq!(statement.context.location.as_deref(), Some("EU"));

    for (option, default) in [
        (BqOption::DisableQueryCache, Some("false")),
        (
            BqOption::Priority,
            Some(BigQueryQueryPriority::Interactive.as_str()),
        ),
        (
            BqOption::CreateDisposition,
            Some(BigQueryCreateDisposition::CreateIfNeeded.as_str()),
        ),
        (
            BqOption::WriteDisposition,
            Some(BigQueryWriteDisposition::WriteEmpty.as_str()),
        ),
        (BqOption::CreateSession, Some("false")),
        (BqOption::DryRun, Some("false")),
        (BqOption::DefaultProjectId, Some("fixture-project")),
        (BqOption::DefaultDatasetId, None),
        (BqOption::DestinationTable, None),
        (BqOption::WriteGcsBucket, None),
        (BqOption::CreateBatchParent, None),
    ] {
        assert_string_option(&statement, option, default);
    }

    // A destination table resolves against whatever project and dataset the statement carries
    // when it is set, so these three are applied in order.
    for (option, value) in [
        (BqOption::DefaultProjectId, "data-project"),
        (BqOption::DefaultDatasetId, "data_set"),
        (BqOption::DestinationTable, "result_table"),
        (BqOption::CreateSession, "true"),
        (BqOption::DryRun, "true"),
        (BqOption::WriteGcsBucket, "dbt-python"),
        (
            BqOption::CreateBatchParent,
            "projects/data-project/locations/eu",
        ),
    ] {
        set_option(&mut statement, option, value);
    }
    assert_string_option(
        &statement,
        BqOption::DestinationTable,
        Some("data-project.data_set.result_table"),
    );

    // An integer option defaults to zero, and zero sets it back, so a getter's value is always
    // one the setter takes.
    for option in [BqOption::MaximumBytesBilled, BqOption::JobTimeout] {
        assert_eq!(option_int(&statement, option), 0);
        set_option(&mut statement, option, OptionValue::Int(1_000_000));
        set_option(&mut statement, option, OptionValue::Int(0));
        assert_eq!(option_int(&statement, option), 0, "{}", option.as_str());
    }

    // (option, clear value, cleared read-back), in order: the destination and dataset are
    // released before the project, whose cleared value resolves back to the connection's.
    for (option, clear_value, cleared) in [
        (BqOption::CreateSession, "false", Some("false")),
        (BqOption::DryRun, "false", Some("false")),
        (BqOption::DestinationTable, "", None),
        (BqOption::DefaultProjectId, "", Some("fixture-project")),
        (BqOption::DefaultDatasetId, "", None),
        (BqOption::WriteGcsBucket, "", None),
        (BqOption::CreateBatchParent, "", None),
    ] {
        set_option(&mut statement, option, clear_value.to_owned());
        assert_string_option(&statement, option, cleared);
    }
}

/// Key spelling is not asserted here: `no_option_key_or_alias_is_spelled_twice` covers every
/// option scope at once.
#[test]
fn statement_options_reject_invalid_values() {
    let mut statement = test_statement();
    let rows: [(BigQueryStatementOption, OptionValue); 17] = [
        (BqOption::MaximumBytesBilled, OptionValue::Int(-1)),
        (BqOption::JobTimeout, OptionValue::Int(-1)),
        (BqOption::Priority, "batch_mode".into()),
        (BqOption::DisableQueryCache, OptionValue::Int(1)),
        (BqOption::CreateSession, OptionValue::Int(1)),
        (BqOption::DryRun, OptionValue::Int(1)),
        (BqOption::CreateDisposition, "CREATE_SOMETIMES".into()),
        (BqOption::WriteDisposition, "WRITE_TRUNCATE_DATA".into()),
        (BqOption::DestinationTable, "table.".into()),
        (BqOption::DestinationTable, ".table".into()),
        (BqOption::DestinationTable, "a..table".into()),
        (BqOption::DestinationTable, "a/b.dataset.table".into()),
        (BqOption::CopyTableSource, "a/b.dataset.table".into()),
        (
            BqOption::CopyTableWriteDisposition,
            "WRITE_SOMETIMES".into(),
        ),
        (BqOption::DataprocPoolingTimeout, "3600 seconds".into()),
        (BqOption::DataprocPoolingTimeout, OptionValue::Int(-1)),
        (BqOption::WriteGcsBucket, OptionValue::Int(1)),
    ];
    for (option, value) in rows {
        assert_eq!(
            set_option_error(&mut statement, option, value).status,
            Status::InvalidArguments,
            "{}",
            option.as_str()
        );
    }
    assert_eq!(
        set_option_error(&mut statement, BqOption::MaximumBillingTier, 1).status,
        Status::NotImplemented
    );

    // No rejection mutated the statement: the destination table and the copy endpoints are
    // still unset, and the deprecated billing tier is unreadable through the typed getter too.
    for option in [
        BqOption::DestinationTable,
        BqOption::CopyTableSource,
        BqOption::CopyTableDestination,
        BqOption::WriteGcsBucket,
    ] {
        assert_string_option(&statement, option, None);
    }
    assert_eq!(
        option_int(&statement, BqOption::DataprocPoolingTimeout),
        0,
        "a rejected timeout leaves the statement without a client-side deadline"
    );
    // And that unset `0` is exactly what setting the option back to zero asks for, so the
    // value a getter reports is always one the setter accepts.
    set_option(
        &mut statement,
        BqOption::DataprocPoolingTimeout,
        OptionValue::Int(0),
    );
    // The setter refuses the deprecated option with NotImplemented, but adbc.h gives
    // `AdbcStatementGetOptionInt` only NotFound.
    assert_eq!(
        statement
            .get_option_int(BqOption::MaximumBillingTier.into())
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

/// Every option key dbt-core 2.0 sends for a python model, in its exact spelling: unset reads
/// back empty, a value round-trips through the canonical key, and the empty string clears it.
///
/// `gsc` is dbt-core 2.0's misspelling of `gcs`, kept verbatim as part of the public contract.
#[test]
fn python_model_options_round_trip_and_clear_under_the_dbt_key_spellings() {
    const KEYS: [(&str, &str); 19] = [
        ("adbc.bigquery.write_gcs.bucket", "dbt-python"),
        ("adbc.bigquery.write_gcs.object_name", "analytics/model.py"),
        (
            "adbc.bigquery.write_gcs.content",
            "def model(dbt, session):",
        ),
        ("adbc.bigquery.dataproc.compute_region", "europe-west1"),
        ("adbc.bigquery.dataproc.project", "compute-project"),
        (
            "adbc.bigquery.dataproc.submit_job.cluster_name",
            "dbt-cluster",
        ),
        (
            "adbc.bigquery.dataproc.submit_job.gcs_path",
            "gs://dbt-python/analytics/model.py",
        ),
        (
            "adbc.bigquery.create_batch.parent",
            "projects/compute-project/locations/europe-west1",
        ),
        (
            "adbc.bigquery.create_batch.batch_yml",
            "runtime_config:\n  version: \"1.1\"\n",
        ),
        ("adbc.bigquery.create_batch.batch_id", "batch-0001"),
        (
            "adbc.bigquery.notebook_execute_job.gsc_path",
            "gs://dbt-python/analytics/model.py",
        ),
        (
            "adbc.bigquery.notebook_execute_job.gsc_bucket",
            "dbt-python",
        ),
        (
            "adbc.bigquery.notebook_execute_job.model_file_name",
            "analytics/model.py",
        ),
        ("adbc.bigquery.notebook_execute_job.model_name", "model"),
        (
            "adbc.bigquery.notebook_execute_job.parent",
            "projects/compute-project/locations/europe-west1",
        ),
        (
            "adbc.bigquery.notebook_execute_job.project",
            "compute-project",
        ),
        ("adbc.bigquery.notebook_execute_job.region", "europe-west1"),
        // Not a key dbt-core 2.0 sends: an explicit execution principal for the notebook job, for
        // credentials that name none of their own.
        (
            "adbc.bigquery.notebook_execute_job.service_account",
            "dbt-runner@compute-project.iam.gserviceaccount.com",
        ),
        ("adbc.bigquery.notebook_execute_job.template_id", "12345"),
    ];

    let mut statement = test_statement();
    for (key, value) in KEYS {
        let Some(option) = BqOption::from_name(key) else {
            panic!("{key} is not a registered statement option")
        };
        assert_eq!(
            option.as_str(),
            key,
            "the dbt-core 2.0 spelling is the canonical key"
        );
        assert!(option.legacy_aliases().is_empty(), "{key}");
        assert_string_option(&statement, option, None);
        set_option(
            &mut statement,
            OptionStatement::Other(key.to_owned()),
            value.to_owned(),
        );
        assert_string_option(&statement, option, Some(value));
    }

    // The twentieth key is the only integer one, and reads back through the typed getter.
    let timeout = BqOption::DataprocPoolingTimeout;
    assert_eq!(timeout.as_str(), "adbc.bigquery.dataproc.pooling_timeout");
    assert_eq!(option_int(&statement, timeout), 0);
    set_option(&mut statement, timeout, "86400");
    assert_eq!(option_int(&statement, timeout), 86_400);

    for (key, _) in KEYS {
        set_option(&mut statement, OptionStatement::Other(key.to_owned()), "");
    }
    set_option(&mut statement, timeout, "");
    for (key, _) in KEYS {
        let Some(option) = BqOption::from_name(key) else {
            panic!("{key}")
        };
        assert_string_option(&statement, option, None);
    }
    assert_eq!(option_int(&statement, timeout), 0);
    // Every python-model option is released, so the statement needs SQL again.
    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_message(&error, &["without a SQL query"]);
}

/// dbt-core 2.0 sends the timeout as a string, which the dbt-labs fork rejects, so its python
/// models never reach a service. Both forms are accepted here, and the value reads back as an
/// integer.
#[test]
fn pooling_timeout_is_accepted_as_a_string_and_as_an_integer() {
    const SECRET: &str = "pooling-timeout-payload-sentinel";
    let key = BqOption::DataprocPoolingTimeout;
    let mut statement = test_statement();

    set_option(&mut statement, key, "3600");
    assert_eq!(option_int(&statement, key), 3600);
    set_option(&mut statement, key, OptionValue::Int(86_400));
    assert_eq!(option_int(&statement, key), 86_400);

    // An integer option is not readable as a string, exactly like bigquery.query.job_timeout.
    assert_string_option(&statement, key, None);

    // Garbage and a negative deadline are INVALID_ARGUMENT; zero is not, and is covered by the
    // clear block below.
    for value in [
        OptionValue::String(SECRET.to_owned()),
        OptionValue::String("3600s".to_owned()),
        OptionValue::String("1.5".to_owned()),
        OptionValue::String("-1".to_owned()),
        OptionValue::Int(-30),
        OptionValue::Double(3600.0),
    ] {
        let error = set_option_error(&mut statement, key, value);
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            !error.message.contains(SECRET),
            "an option value could be a credential sent under the wrong key: {}",
            error.message
        );
    }
    assert_eq!(
        option_int(&statement, key),
        86_400,
        "a rejected value leaves the configured deadline alone"
    );

    // An unset `pooling_timeout` reads back as `0`, so the setter must accept `0`. Zero, `"0"`
    // and the empty string all mean no client-side deadline, as for `bigquery.query.job_timeout`;
    // in the dbt-labs fork, zero is an already-expired deadline.
    for clearing in [
        OptionValue::Int(0),
        OptionValue::String("0".to_owned()),
        OptionValue::String(String::new()),
    ] {
        set_option(&mut statement, key, OptionValue::Int(3600));
        assert_eq!(option_int(&statement, key), 3600);
        set_option(&mut statement, key, clearing);
        assert_eq!(
            option_int(&statement, key),
            0,
            "zero and the empty string are the same clear"
        );
    }
    // And the getter's own reading of an unset deadline goes straight back through the setter.
    let unset = option_int(&statement, key);
    assert_eq!(unset, 0);
    set_option(&mut statement, key, unset);
    assert_eq!(option_int(&statement, key), 0);
}

/// The statement options dbt-core 2.0 sets on nearly every query: job labels, the job timeout,
/// and the accepted-but-inert link_failed_job flag, all through the dbt-core 2.0 key spellings.
#[test]
fn dbt_statement_options_round_trip() {
    let mut statement = test_statement();
    assert_string_option(&statement, BqOption::Labels, None);
    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.sql.query.labels".to_owned()),
        r#"{"team":"analytics","dbt_invocation_id":"abc"}"#.to_owned(),
    );
    assert_eq!(
        option_string(&statement, BqOption::Labels),
        r#"{"dbt_invocation_id":"abc","team":"analytics"}"#,
        "labels are reported sorted and through the canonical key"
    );
    for cleared in ["", "{}"] {
        set_option(&mut statement, BqOption::Labels, cleared.to_owned());
        assert_string_option(&statement, BqOption::Labels, None);
    }

    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.sql.query.job_timeout".to_owned()),
        OptionValue::Int(90_000),
    );
    assert_eq!(option_int(&statement, BqOption::JobTimeout), 90_000);

    assert_string_option(
        &statement,
        BqOption::LinkFailedJob,
        Some(ADBC_OPTION_VALUE_DISABLED),
    );
    set_option(
        &mut statement,
        OptionStatement::Other("adbc.bigquery.sql.query.link_failed_job".to_owned()),
        "true",
    );
    assert_string_option(
        &statement,
        BqOption::LinkFailedJob,
        Some(ADBC_OPTION_VALUE_ENABLED),
    );
}

#[test]
fn dbt_csv_seed_options_round_trip_and_switch_operations_cleanly() {
    let schema = Schema::new(vec![bigquery_typed(
        Field::new("id", DataType::Utf8, true),
        "int64",
    )]);
    let encoded_schema = encode_schema_fixture(&schema);

    let mut statement = test_statement();
    set_option(
        &mut statement,
        BqOption::DestinationTable,
        "seed_dataset.seed_table",
    );
    set_option(&mut statement, BqOption::CsvDelimiter, "|");
    set_option(&mut statement, BqOption::CsvSchema, encoded_schema.clone());
    set_option(&mut statement, BqOption::CsvFilepath, "seeds/example.csv");
    statement.set_sql_query("").unwrap();

    assert_eq!(
        statement.dbt.active_operation(),
        Some(DbtOperation::CsvSeed)
    );
    assert_eq!(statement.sql.as_deref(), Some(""));
    assert_eq!(statement.ingest.target_table, None);
    assert_string_option(
        &statement,
        BqOption::DestinationTable,
        Some("fixture-project.seed_dataset.seed_table"),
    );
    assert_eq!(option_string(&statement, BqOption::CsvDelimiter), "|");
    assert_string_option(&statement, BqOption::CsvFilepath, Some("seeds/example.csv"));
    assert_eq!(
        statement
            .get_option_bytes(BqOption::CsvSchema.into())
            .unwrap(),
        encoded_schema
    );

    statement.set_sql_query("SELECT 1").unwrap();
    assert_string_option(&statement, BqOption::CsvFilepath, None);
    assert!(
        statement
            .get_option_bytes(BqOption::CsvSchema.into())
            .is_err()
    );

    set_option(&mut statement, BqOption::CsvFilepath, "seeds/again.csv");
    set_option(
        &mut statement,
        OptionStatement::TargetDbSchema,
        "seed_dataset",
    );
    set_option(
        &mut statement,
        OptionStatement::TargetTable,
        "standard_ingest",
    );
    assert_string_option(&statement, BqOption::CsvFilepath, None);
    assert_eq!(
        statement.ingest.target_table.as_deref(),
        Some("standard_ingest")
    );
}

#[test]
fn dbt_csv_seed_options_reject_wrong_accessor_types() {
    let mut statement = test_statement();
    for (option, value) in [
        (BqOption::CsvFilepath, OptionValue::Bytes(Vec::new())),
        (BqOption::CsvDelimiter, OptionValue::Bytes(Vec::new())),
        (BqOption::CsvSchema, OptionValue::String(String::new())),
    ] {
        assert_eq!(
            set_option_error(&mut statement, option, value).status,
            Status::InvalidArguments
        );
    }
    assert_string_option(&statement, BqOption::CsvSchema, None);
    assert_eq!(
        statement
            .get_option_bytes(BqOption::CsvFilepath.into())
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[test]
fn job_labels_reject_non_object_values_without_echoing_them() {
    const SECRET: &str = "job-label-payload-secret-sentinel";

    let mut statement = test_statement();
    for invalid in [
        format!("[\"{SECRET}\"]"),
        format!("\"{SECRET}\""),
        format!("{{\"key\":42,\"other\":\"{SECRET}\"}}"),
        format!("not json {SECRET}"),
    ] {
        let error = set_option_error(
            &mut statement,
            BqOption::Labels,
            OptionValue::String(invalid),
        );
        assert_eq!(error.status, Status::InvalidArguments);
        assert_message_omits(&error, &[SECRET]);
    }
    assert!(statement.query_options.labels.is_none());
}

#[test]
fn project_options_preserve_identifiers_and_reject_unsafe_resource_segments() {
    let mut statement = test_statement();
    for option in [
        BqOption::DefaultProjectId.into(),
        OptionStatement::TargetCatalog,
    ] {
        for value in [
            "123456789012",
            "example.com:project",
            "INVALID",
            "abc_def",
            "x",
        ] {
            set_option(&mut statement, option.clone(), value);
            assert_eq!(option_string(&statement, option.clone()), value);
        }
    }
    for (option, value) in [
        (BqOption::DefaultProjectId.into(), "SECRET/path"),
        (OptionStatement::TargetCatalog, "SECRET\\path"),
        (
            BqOption::DestinationTable.into(),
            "SECRET/path.dataset.table",
        ),
    ] {
        let error = set_option_error(&mut statement, option, value);
        assert_eq!(error.status, Status::InvalidArguments, "accepted {value:?}");
        assert_message_omits(&error, &["SECRET"]);
    }

    let mut connection = test_connection();
    assert_eq!(
        set_option_error(
            &mut connection,
            OptionConnection::CurrentCatalog,
            "unsafe/path"
        )
        .status,
        Status::InvalidArguments,
    );
}

/// These two options run the same dataset and table validators as `adbc.connection.db_schema`
/// and `bigquery.dataset_id`, when set rather than at the first append.
#[test]
fn ingest_target_names_are_validated_when_they_are_set() {
    let mut statement = test_statement();
    for (option, value, expected) in [
        (
            OptionStatement::TargetDbSchema,
            "not-a-dataset",
            "BigQuery option adbc.ingest.target_db_schema may contain only ASCII letters, digits, \
             and underscores",
        ),
        (
            OptionStatement::TargetTable,
            "events/rollup",
            "BigQuery option adbc.ingest.target_table cannot contain slashes or control characters",
        ),
    ] {
        let error = set_option_error(&mut statement, option, value);
        assert_eq!(error.status, Status::InvalidArguments, "{value:?}");
        assert_eq!(error.message, expected);
    }
    assert!(statement.ingest.target_db_schema.is_none());
    assert!(statement.ingest.target_table.is_none());
}

/// The statement default dataset is checked by the validator `adbc.connection.db_schema` and
/// `bigquery.dataset_id` use, so an unusable dataset ID is refused under the option's own name.
#[test]
fn the_statement_default_dataset_is_validated_like_every_other_dataset_id() {
    let mut statement = test_statement();
    let failure = set_option_error(&mut statement, BqOption::DefaultDatasetId, "bad-dataset");
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "BigQuery option bigquery.query.default_dataset_id may contain only ASCII letters, \
         digits, and underscores"
    );

    let failure = set_option_error(
        &mut statement,
        BqOption::DefaultDatasetId,
        "d".repeat(1_025),
    );
    assert_eq!(
        failure.message,
        "BigQuery option bigquery.query.default_dataset_id is 1025 characters long; it \
         cannot exceed 1,024 characters"
    );

    // A valid one still applies, and the empty string still clears it.
    let key = BqOption::DefaultDatasetId;
    set_option(&mut statement, key, "good_dataset");
    assert_string_option(&statement, key, Some("good_dataset"));
    set_option(&mut statement, key, "");
    assert_string_option(&statement, key, None);
}

/// A refusal names the key the caller set, alias spellings included, and a removed option is
/// refused whatever its value, before the value is parsed.
#[test]
fn option_refusals_name_the_key_the_caller_used() {
    let alias = |name: &str| OptionStatement::Other(format!("adbc.bigquery.sql.query.{name}"));
    let mut statement = test_statement();
    let error = set_option_error(&mut statement, alias("default_project_id"), "unsafe/path");
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message(
        &error,
        &["the adbc.bigquery.sql.query.default_project_id option"],
    );
    let error = set_option_error(
        &mut statement,
        alias("destination_table"),
        "a/b.dataset.table",
    );
    assert_eq!(error.status, Status::InvalidArguments);
    assert_message(&error, &["adbc.bigquery.sql.query.destination_table"]);
    for value in [OptionValue::Int(1), "abc".into()] {
        let error = set_option_error(&mut statement, alias("max_billing_tier"), value);
        assert_eq!(error.status, Status::NotImplemented);
        assert_message(
            &error,
            &["adbc.bigquery.sql.query.max_billing_tier is deprecated"],
        );
    }
    let error = statement
        .get_option_int(alias("max_billing_tier"))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_message(
        &error,
        &["adbc.bigquery.sql.query.max_billing_tier is deprecated"],
    );

    let mut statement = statement_without_a_project();
    let error = set_option_error(&mut statement, alias("default_dataset_id"), "some_dataset");
    assert_eq!(error.status, Status::InvalidState);
    assert_message(
        &error,
        &["setting the statement option adbc.bigquery.sql.query.default_dataset_id;"],
    );
}

/// A statement with no project anywhere: no database `bigquery.project_id` and no connection
/// catalog, so nothing can qualify a dataset name.
fn statement_without_a_project() -> BigQueryStatement {
    use adbc_core::{Database as _, Driver as _};

    let mut driver = crate::BigQueryDriver::try_new().unwrap();
    let database = driver
        .new_database_with_opts([
            (
                crate::BigQueryDatabaseOption::AuthenticationType.into(),
                crate::BigQueryAuthenticationType::Anonymous.into(),
            ),
            (
                crate::BigQueryDatabaseOption::Endpoint.into(),
                "http://127.0.0.1:1".into(),
            ),
        ])
        .unwrap();
    database.new_connection().unwrap().new_statement().unwrap()
}

/// A dataset without a project gets the same status and message at both scopes; the connection
/// counterpart is
/// `connection::tests::options::naming_a_schema_without_a_project_reports_the_options_that_supply_one`.
#[test]
fn naming_a_default_dataset_without_a_project_reports_the_options_that_supply_one() {
    let mut statement = statement_without_a_project();
    let error = set_option_error(&mut statement, BqOption::DefaultDatasetId, "some_dataset");
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(
        error.message,
        "a BigQuery project must be available before setting the statement option \
         bigquery.query.default_dataset_id; set the statement option \
         bigquery.query.default_project_id, the connection option adbc.connection.catalog, or the \
         database option bigquery.project_id"
    );

    // Clearing the statement project a default dataset depends on is the same condition.
    for (option, value) in [
        (BqOption::DefaultProjectId, "some-project"),
        (BqOption::DefaultDatasetId, "some_dataset"),
    ] {
        set_option(&mut statement, option, value);
    }
    let error = set_option_error(&mut statement, BqOption::DefaultProjectId, "");
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(
        error.message,
        "BigQuery option bigquery.query.default_project_id cannot be cleared while the statement \
         option bigquery.query.default_dataset_id is set and no other project is configured; \
         clear bigquery.query.default_dataset_id first, or set the connection option \
         adbc.connection.catalog or the database option bigquery.project_id"
    );
}

/// adbc.h's `adbc.statement.exec.progress` and `adbc.statement.exec.max_progress`: after a query
/// finishes, progress is its maximum and both are the bytes the job processed.
#[test]
fn execution_progress_reports_the_bytes_the_query_processed() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobReference": job_reference("fixture-project", "options-job-1"),
            "jobComplete": true,
            "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
            "statementType": "SELECT",
            "totalBytesProcessed": "4096",
            "totalRows": "0"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        assert_eq!(
            statement.get_option_double(key).unwrap(),
            0.0,
            "a statement that has executed nothing knows no progress"
        );
    }

    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    let reader = statement.execute().unwrap();
    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        assert_eq!(statement.get_option_double(key).unwrap(), 4096.0);
    }
    // Progress is of execution, not of consumption: draining the reader does not change it.
    drop(reader);
    assert_eq!(
        statement
            .get_option_double(OptionStatement::Progress)
            .unwrap(),
        4096.0
    );

    // The two are doubles, and say so through every other accessor.
    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        for error in [
            statement.get_option_string(key.clone()).unwrap_err(),
            statement.get_option_int(key.clone()).unwrap_err(),
            statement.get_option_bytes(key.clone()).unwrap_err(),
        ] {
            assert_eq!(error.status, Status::NotFound);
            assert_message(&error, &["double"]);
        }
    }
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// An update runs a job like any query, so it publishes the bytes that job processed.
#[test]
fn an_update_publishes_the_bytes_its_job_processed() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "jobReference": job_reference("fixture-project", "options-job-2"),
            "jobComplete": true,
            "statementType": "UPDATE",
            "totalBytesProcessed": "4096",
            "numDmlAffectedRows": "3"
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    statement
        .set_sql_query("UPDATE dataset.table SET value = 1 WHERE true")
        .unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(3));

    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        assert_eq!(statement.get_option_double(key).unwrap(), 4096.0);
    }
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// A dry run reports an *estimate* of the bytes a query would process. It executed nothing, so it
/// publishes no progress: adbc.h's "the maximum is not known".
#[test]
fn a_dry_run_publishes_no_execution_progress() {
    let (endpoint, requests) = capture_requests(vec![(
        200,
        json!({
            "statistics": {
                "query": {
                    "statementType": "SELECT",
                    "totalBytesProcessed": "4096",
                    "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]}
                }
            },
            "status": {"state": "DONE"}
        }),
    )]);
    let mut statement = test_statement_at(&endpoint);
    set_option(&mut statement, BqOption::DryRun, "true");
    statement.set_sql_query("SELECT 21 + 21 AS answer").unwrap();
    drop(statement.execute().unwrap());

    for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
        assert_eq!(statement.get_option_double(key).unwrap(), 0.0);
    }
    assert_eq!(requests.join().unwrap().len(), 1);
}

/// `ExecuteUpdate` and `ExecutePartitions` run the job of every bound parameter row before they
/// return, so their progress is the bytes all of those jobs processed, as their row counts are
/// sums.
#[test]
fn several_parameter_rows_publish_the_bytes_all_their_jobs_processed() {
    let completed = |job_id: &str, statement_type: &str, bytes: &str| {
        (
            200,
            json!({
                "jobComplete": true,
                "jobReference": job_reference("fixture-project", job_id),
                "schema": {"fields": [{"name": "answer", "type": "INTEGER"}]},
                "statementType": statement_type,
                "numDmlAffectedRows": "1",
                "totalBytesProcessed": bytes
            }),
        )
    };
    let (endpoint, requests) = capture_requests(vec![
        completed("update-1", "UPDATE", "1024"),
        completed("update-2", "UPDATE", "2048"),
        completed("select-1", "SELECT", "4096"),
        completed("select-2", "SELECT", "8192"),
    ]);
    let mut statement = test_statement_at(&endpoint);
    let assert_progress = |statement: &BigQueryStatement, expected: f64, what: &str| {
        for key in [OptionStatement::Progress, OptionStatement::MaxProgress] {
            assert_eq!(
                statement.get_option_double(key).unwrap(),
                expected,
                "{what}"
            );
        }
    };

    statement
        .set_sql_query("UPDATE dataset.table SET x = ? WHERE true")
        .unwrap();
    statement.bind(int64_column("x", vec![1, 2])).unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(2));
    assert_progress(&statement, 3072.0, "ExecuteUpdate");

    statement.set_sql_query("SELECT ? AS answer").unwrap();
    statement.bind(int64_column("answer", vec![1, 2])).unwrap();
    assert_eq!(statement.execute_partitions().unwrap().partitions.len(), 2);
    assert_progress(&statement, 12288.0, "ExecutePartitions");
    assert_eq!(requests.join().unwrap().len(), 4);
}

/// Unset, write-only, and incorrectly typed options must name the requested key in their errors.
/// See `src/driver/tests/options.rs` for the corresponding database-option test.
#[test]
fn unreadable_statement_options_are_reported_by_their_own_key() {
    let statement = test_statement();
    let standard: [OptionStatement; 7] = [
        OptionStatement::TargetTable,
        OptionStatement::TargetCatalog,
        OptionStatement::TargetDbSchema,
        OptionStatement::Temporary,
        OptionStatement::Incremental,
        // The two double-typed keys, which every other getter has to refuse by name.
        OptionStatement::Progress,
        OptionStatement::MaxProgress,
    ];
    let keys = BqOption::ALL
        .iter()
        .map(|option| (option.as_str(), OptionStatement::from(*option)))
        .chain(standard.iter().map(|key| (key.as_ref(), key.clone())));

    for (name, key) in keys {
        for failure in [
            statement.get_option_string(key.clone()).err(),
            statement.get_option_bytes(key.clone()).err(),
            statement.get_option_int(key.clone()).err(),
            statement.get_option_double(key.clone()).err(),
        ]
        .into_iter()
        .flatten()
        {
            assert_eq!(
                failure.status,
                Status::NotFound,
                "{name}: {}",
                failure.message
            );
            assert!(
                failure.message.contains(name),
                "{name}: {}",
                failure.message
            );
        }
    }
    assert_eq!(
        statement
            .get_option_int(OptionStatement::Progress)
            .unwrap_err()
            .message,
        "BigQuery option adbc.statement.exec.progress has type double, but was requested as an \
         integer"
    );
}
