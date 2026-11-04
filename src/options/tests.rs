use super::*;
use crate::test_support::errors::assert_message_omits;
use crate::{
    BigQueryAuthenticationType, BigQueryBulkIngestMethod, BigQueryConnectionOption,
    BigQueryCreateDisposition, BigQueryCredentialsType, BigQueryDatabaseOption,
    BigQueryQueryPriority, BigQueryStatementOption, BigQueryWriteDisposition,
};

const SECRET: &str = "option-payload-secret-sentinel";

#[test]
fn enum_alternatives_keep_declaration_order_and_quote_each_spelling() {
    assert_eq!(
        valid_enum_values::<BigQueryQueryPriority>(),
        "\"BATCH\", \"INTERACTIVE\""
    );
    assert_eq!(quoted_values(&[]), "");
    assert_eq!(quoted_values(&[""]), "\"\"");
    assert_eq!(
        quoted_values(&["first", "a\"b", "c\\d", "\n"]),
        "\"first\", \"a\\\"b\", \"c\\\\d\", \"\\n\""
    );
}

#[test]
fn type_errors_report_only_the_value_kind() {
    for (value, kind) in [
        (OptionValue::String(SECRET.to_owned()), "string"),
        (OptionValue::Bytes(SECRET.as_bytes().to_vec()), "bytes"),
        (OptionValue::Int(42), "integer"),
        (OptionValue::Double(1.5), "double"),
    ] {
        let error = expected_type("test.option", "different", &value);
        assert_eq!(
            error.message,
            format!("BigQuery option test.option requires a different value, received {kind}")
        );
        assert_message_omits(&error, &[SECRET, &format!("{value:?}")]);
    }
}

#[test]
fn type_errors_name_the_expected_type_grammatically() {
    assert_eq!(
        integer_value("test.integer", OptionValue::Double(1.5))
            .unwrap_err()
            .message,
        "BigQuery option test.integer requires an integer value, received double"
    );
    assert_eq!(
        string_value("test.string", OptionValue::Int(42))
            .unwrap_err()
            .message,
        "BigQuery option test.string requires a string value, received integer"
    );
    assert_eq!(
        bytes_value("test.bytes", OptionValue::Int(42))
            .unwrap_err()
            .message,
        "BigQuery option test.bytes requires a bytes value, received integer"
    );
}

#[test]
fn content_errors_do_not_report_the_string_value() {
    for error in [
        boolean_value("test.boolean", OptionValue::String(SECRET.to_owned())).unwrap_err(),
        ingest_mode_value("test.mode", OptionValue::String(SECRET.to_owned())).unwrap_err(),
        enum_value::<BigQueryAuthenticationType>(
            "test.enum",
            OptionValue::String(SECRET.to_owned()),
        )
        .unwrap_err(),
    ] {
        assert_message_omits(&error, &[SECRET]);
    }
}

#[test]
fn integers_accept_both_spellings_without_echoing_a_rejected_value() {
    assert_eq!(
        integer_value("test.integer", OptionValue::Int(-7)).unwrap(),
        -7
    );
    assert_eq!(
        integer_value("test.integer", OptionValue::String(" 42 ".to_owned())).unwrap(),
        42
    );
    for value in [
        OptionValue::String(SECRET.to_owned()),
        OptionValue::String("1.5".to_owned()),
        OptionValue::String(String::new()),
        OptionValue::Double(1.5),
    ] {
        let error = integer_value("test.integer", value).unwrap_err();
        assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
        assert_message_omits(&error, &[SECRET]);
    }
}

#[test]
fn an_overflowing_integer_string_is_told_the_range() {
    for value in ["9223372036854775808", "-9223372036854775809"] {
        assert_eq!(
            integer_value("test.integer", value.into())
                .unwrap_err()
                .message,
            "BigQuery option test.integer must fit in a signed 64-bit integer",
            "{value}"
        );
    }
}

#[test]
fn booleans_accept_the_spec_spellings_in_any_case() {
    for value in ["true", "True", "TRUE"] {
        assert!(
            boolean_value("test.boolean", value.into()).unwrap(),
            "{value}"
        );
    }
    for value in ["false", "False", "FALSE"] {
        assert!(
            !boolean_value("test.boolean", value.into()).unwrap(),
            "{value}"
        );
    }
}

#[test]
fn boolean_rejections_name_the_accepted_spellings_only() {
    for value in ["1", "yes", "on", " true", SECRET] {
        let error = boolean_value("test.boolean", value.into()).unwrap_err();
        assert_eq!(
            error.message, "BigQuery option test.boolean must be \"true\" or \"false\"",
            "{value}"
        );
    }
}

#[test]
fn valid_values_decode() {
    assert_eq!(
        string_value("test.string", OptionValue::String("value".to_owned())).unwrap(),
        "value"
    );
    assert_eq!(
        integer_value("test.integer", OptionValue::Int(42)).unwrap(),
        42
    );
    assert!(boolean_value("test.boolean", true.into()).unwrap());
    assert!(!boolean_value("test.boolean", false.into()).unwrap());
    assert_eq!(
        ingest_mode_value("test.mode", ADBC_INGEST_OPTION_MODE_CREATE.into()).unwrap(),
        IngestMode::Create
    );
    assert_eq!(
        enum_value::<BigQueryAuthenticationType>(
            "test.enum",
            BigQueryAuthenticationType::Anonymous.into(),
        )
        .unwrap(),
        BigQueryAuthenticationType::Anonymous
    );
    assert_eq!(
        optional_enum_value::<BigQueryAuthenticationType>("test.enum", "".into()).unwrap(),
        None
    );
}

/// Case-insensitive `from_name` is unambiguous only while no enum spells two of its values
/// the same way up to ASCII case, aliases included.
macro_rules! assert_spellings_are_distinct_ignoring_case {
    ($($name:ty),+ $(,)?) => {$({
        let mut seen = std::collections::HashSet::new();
        for value in <$name>::ALL.iter().copied() {
            for spelling in std::iter::once(value.as_str())
                .chain(value.legacy_aliases().iter().copied())
            {
                assert!(
                    seen.insert(spelling.to_ascii_lowercase()),
                    "{} spells {spelling:?} twice up to ASCII case",
                    stringify!($name)
                );
                for input in [
                    spelling.to_owned(),
                    spelling.to_ascii_lowercase(),
                    spelling.to_ascii_uppercase(),
                ] {
                    assert_eq!(<$name>::from_name(&input), Some(value), "{input}");
                }
                assert_eq!(<$name>::from_name(&format!("{spelling} ")), None);
            }
        }
        assert_eq!(<$name>::from_name(""), None);
        assert_eq!(<$name>::from_name("not-an-option-or-value"), None);
    })+};
}

#[test]
fn every_enum_spells_its_values_distinctly_ignoring_case() {
    assert_spellings_are_distinct_ignoring_case!(
        BigQueryAuthenticationType,
        BigQueryBulkIngestMethod,
        BigQueryConnectionOption,
        BigQueryCreateDisposition,
        BigQueryCredentialsType,
        BigQueryDatabaseOption,
        BigQueryQueryPriority,
        BigQueryStatementOption,
        BigQueryWriteDisposition,
        crate::parameters::ParameterMode,
    );
}

#[test]
fn names_and_legacy_aliases_match_ignoring_case() {
    assert_eq!(
        BigQueryWriteDisposition::from_name("write_append"),
        Some(BigQueryWriteDisposition::WriteAppend)
    );
    assert_eq!(
        BigQueryQueryPriority::from_name("Interactive"),
        Some(BigQueryQueryPriority::Interactive)
    );
    assert_eq!(
        BigQueryBulkIngestMethod::from_name("STORAGE_WRITE"),
        Some(BigQueryBulkIngestMethod::StorageWrite)
    );
    assert_eq!(
        BigQueryAuthenticationType::from_name("Service_Account"),
        None
    );
    assert_eq!(
        BigQueryDatabaseOption::from_name("BigQuery.Project_Id"),
        Some(BigQueryDatabaseOption::ProjectId)
    );
    assert_eq!(
        BigQueryDatabaseOption::from_name("ADBC.BIGQUERY.SQL.PROJECT_ID"),
        Some(BigQueryDatabaseOption::ProjectId)
    );
    assert_eq!(BigQueryDatabaseOption::from_name("bigquery.unknown"), None);
}

/// Every key of the active Go driver's `optionRemapping`, grouped by support and option scope.
///
/// Transcribed from the Go driver's `go/driver.go`. The count assertion checks that the four
/// groups cover the whole map; losing a supported alias would break profiles written for the Go
/// driver.
#[test]
fn every_go_option_remapping_key_resolves() {
    const DATABASE: &[(&str, &str)] = &[
        (
            "adbc.bigquery.sql.auth.client_id",
            "bigquery.auth.client_id",
        ),
        (
            "adbc.bigquery.sql.auth.client_secret",
            "bigquery.auth.client_secret",
        ),
        (
            "adbc.bigquery.sql.auth.quota_project",
            "bigquery.auth.quota_project",
        ),
        (
            "adbc.bigquery.sql.auth.refresh_token",
            "bigquery.auth.refresh_token",
        ),
        (
            "adbc.bigquery.sql.auth_credentials",
            "bigquery.auth.credentials",
        ),
        ("adbc.bigquery.sql.auth_type", "bigquery.auth_type"),
        ("adbc.bigquery.sql.dataset_id", "bigquery.dataset_id"),
        ("adbc.bigquery.sql.endpoint", "bigquery.endpoint"),
        (
            "adbc.bigquery.sql.impersonate.delegates",
            "bigquery.impersonate.delegates",
        ),
        (
            "adbc.bigquery.sql.impersonate.lifetime",
            "bigquery.impersonate.lifetime",
        ),
        (
            "adbc.bigquery.sql.impersonate.scopes",
            "bigquery.impersonate.scopes",
        ),
        (
            "adbc.bigquery.sql.impersonate.target_principal",
            "bigquery.impersonate.target_principal",
        ),
        ("adbc.bigquery.sql.location", "bigquery.location"),
        ("adbc.bigquery.sql.project_id", "bigquery.project_id"),
        (
            "adbc.bigquery.sql.storage_endpoint",
            "bigquery.storage_endpoint",
        ),
    ];
    const STATEMENT: &[(&str, &str)] = &[
        (
            "adbc.bigquery.sql.query.create_disposition",
            "bigquery.query.create_disposition",
        ),
        (
            "adbc.bigquery.sql.query.create_session",
            "bigquery.query.create_session",
        ),
        (
            "adbc.bigquery.sql.query.default_dataset_id",
            "bigquery.query.default_dataset_id",
        ),
        (
            "adbc.bigquery.sql.query.default_project_id",
            "bigquery.query.default_project_id",
        ),
        (
            "adbc.bigquery.sql.query.destination_table",
            "bigquery.query.destination_table",
        ),
        (
            "adbc.bigquery.sql.query.disable_query_cache",
            "bigquery.query.disable_query_cache",
        ),
        ("adbc.bigquery.sql.query.dry_run", "bigquery.query.dry_run"),
        (
            "adbc.bigquery.sql.query.job_timeout",
            "bigquery.query.job_timeout",
        ),
        (
            "adbc.bigquery.sql.query.max_billing_tier",
            "bigquery.query.max_billing_tier",
        ),
        (
            "adbc.bigquery.sql.query.max_bytes_billed",
            "bigquery.query.max_bytes_billed",
        ),
        (
            "adbc.bigquery.sql.query.parameter_mode",
            "bigquery.query.parameter_mode",
        ),
        (
            "adbc.bigquery.sql.query.prefetch_concurrency",
            "bigquery.query.prefetch_concurrency",
        ),
        (
            "adbc.bigquery.sql.query.priority",
            "bigquery.query.priority",
        ),
        (
            "adbc.bigquery.sql.query.write_disposition",
            "bigquery.query.write_disposition",
        ),
    ];
    const AUTHENTICATION_TYPE_VALUES: &[(&str, &str)] = &[
        ("adbc.bigquery.sql.auth_type.anonymous", "anonymous"),
        (
            "adbc.bigquery.sql.auth_type.app_default_credentials",
            "app_default_credentials",
        ),
        ("adbc.bigquery.sql.auth_type.auth_bigquery", "auth_bigquery"),
        (
            "adbc.bigquery.sql.auth_type.json_credential_file",
            "json_credential_file",
        ),
        (
            "adbc.bigquery.sql.auth_type.json_credential_string",
            "json_credential_string",
        ),
        (
            "adbc.bigquery.sql.auth_type.user_authentication",
            "user_authentication",
        ),
    ];
    const PARAMETER_MODE_VALUES: &[(&str, &str)] = &[
        ("adbc.bigquery.sql.query.parameter_mode_named", "named"),
        (
            "adbc.bigquery.sql.query.parameter_mode_positional",
            "positional",
        ),
    ];
    // Go remaps these but resolves none of them to behavior this driver offers: the two
    // auth-type values are vestigial constants its own auth switch rejects as "unknown auth
    // type", legacy SQL is out of scope, `allow_large_results` and
    // `disable_flattened_results` only shape legacy-SQL results, and the REST row buffer is
    // an implementation detail here rather than a tunable.
    const UNSUPPORTED: &[&str] = &[
        "adbc.bigquery.sql.auth_type.json_credentials",
        "adbc.bigquery.sql.auth_type.oauth_client_ids",
        "adbc.bigquery.sql.query.allow_large_results",
        "adbc.bigquery.sql.query.disable_flattened_results",
        "adbc.bigquery.sql.query.result_buffer_size",
        "adbc.bigquery.sql.query.use_legacy_sql",
    ];

    for &(alias, canonical) in DATABASE {
        let Some(option) = BigQueryDatabaseOption::from_name(alias) else {
            panic!("{alias} resolves to no database option")
        };
        assert_eq!(option.as_str(), canonical, "{alias}");
    }
    for &(alias, canonical) in STATEMENT {
        let Some(option) = BigQueryStatementOption::from_name(alias) else {
            panic!("{alias} resolves to no statement option")
        };
        assert_eq!(option.as_str(), canonical, "{alias}");
    }
    for &(alias, canonical) in AUTHENTICATION_TYPE_VALUES {
        let Some(value) = BigQueryAuthenticationType::from_name(alias) else {
            panic!("{alias} resolves to no authentication type")
        };
        assert_eq!(value.as_str(), canonical, "{alias}");
    }
    for &(alias, canonical) in PARAMETER_MODE_VALUES {
        let Some(mode) = crate::parameters::ParameterMode::from_name(alias) else {
            panic!("{alias} resolves to no parameter mode")
        };
        assert_eq!(mode.as_str(), canonical, "{alias}");
    }
    for &key in UNSUPPORTED {
        assert_eq!(BigQueryDatabaseOption::from_name(key), None, "{key}");
        assert_eq!(BigQueryConnectionOption::from_name(key), None, "{key}");
        assert_eq!(BigQueryStatementOption::from_name(key), None, "{key}");
    }

    assert_eq!(
        DATABASE.len()
            + STATEMENT.len()
            + AUTHENTICATION_TYPE_VALUES.len()
            + PARAMETER_MODE_VALUES.len()
            + UNSUPPORTED.len(),
        43,
        "the Go driver's optionRemapping has 43 entries; re-read it before changing this"
    );
}

/// The aliases this driver accepts that the active Go driver does not remap, each traced to
/// the fork or to dbt-core 2.0, so an unexplained one is a mistake rather than compatibility.
#[test]
fn aliases_outside_the_go_remapping_are_accounted_for() {
    const FORK_ONLY: &[(&str, &str)] = &[
        // A backward-compat alias the fork keeps for the retired apache/arrow-adbc key.
        ("adbc.bigquery.sql.api_endpoint", "bigquery.endpoint"),
        // The fork's second spelling of the storage endpoint; it overrides both halves of
        // the Storage API despite naming only the Read one.
        (
            "adbc.bigquery.sql.storage_read_api_endpoint",
            "bigquery.storage_endpoint",
        ),
        (
            "adbc.bigquery.sql.auth.access_token",
            "bigquery.auth.access_token",
        ),
        (
            "adbc.bigquery.sql.auth.access_token_endpoint",
            "bigquery.auth.access_token_endpoint",
        ),
        (
            "adbc.bigquery.sql.auth_type.temporary_access_token",
            "temporary_access_token",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.audience",
            "bigquery.auth.external_account.audience",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.request_url",
            "bigquery.auth.external_account.request_url",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.request_data",
            "bigquery.auth.external_account.request_data",
        ),
        (
            "adbc.bigquery.sql.auth.external_account.impersonation_url",
            "bigquery.auth.external_account.impersonation_url",
        ),
        (
            "adbc.bigquery.sql.auth_type.external_account",
            "external_account",
        ),
        ("adbc.bigquery.sql.query.labels", "bigquery.query.labels"),
        (
            "adbc.bigquery.sql.query.link_failed_job",
            "bigquery.query.link_failed_job",
        ),
    ];

    for &(alias, canonical) in FORK_ONLY {
        let resolved = BigQueryDatabaseOption::from_name(alias)
            .map(BigQueryDatabaseOption::as_str)
            .or_else(|| {
                BigQueryStatementOption::from_name(alias).map(BigQueryStatementOption::as_str)
            })
            .or_else(|| {
                BigQueryAuthenticationType::from_name(alias).map(BigQueryAuthenticationType::as_str)
            });
        assert_eq!(resolved, Some(canonical), "{alias}");
    }
}

/// serde's position for a well-formed document of the wrong shape is no help (column 0 for
/// `[1]`), so only a syntax error is located.
#[test]
fn a_json_object_option_tells_a_wrong_shape_from_a_syntax_error() {
    let rejection = |value: &str| {
        json_object_value::<String>("bigquery.query.labels", value, "a JSON object of strings")
            .unwrap_err()
            .message
    };
    for wrong_shape in ["[1]", "5", "null", r#"{"a": 1}"#] {
        assert_eq!(
            rejection(wrong_shape),
            "BigQuery option bigquery.query.labels must be a JSON object of strings; it is \
             well-formed JSON of another shape",
            "{wrong_shape}"
        );
    }
    assert_eq!(
        rejection(r#"{"a" "secret"}"#),
        "BigQuery option bigquery.query.labels must be a JSON object of strings; it is not \
         well-formed JSON: parsing stopped at line 1 column 6"
    );
    assert_eq!(json_object_value::<String>("k", "", "x").unwrap(), None);
    assert_eq!(json_object_value::<String>("k", "{}", "x").unwrap(), None);
}
