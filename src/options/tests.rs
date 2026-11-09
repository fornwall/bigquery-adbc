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

/// An overflowing integer string is told the range.
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
fn booleans_accept_only_the_spec_spellings_in_any_case() {
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
    assert_eq!(
        BigQueryAuthenticationType::from_name("Service_Account"),
        None
    );
}

/// Assert a literal list of Go names under its legacy and canonical prefixes.
fn assert_remappings<T: StringEnum>(legacy: &str, canonical: &str, names: &[&str]) -> usize {
    for name in names {
        let alias = format!("{legacy}{name}");
        let expected = format!("{canonical}{name}");
        assert_eq!(
            T::from_name(&alias).map(T::as_str),
            Some(expected.as_str()),
            "{alias}"
        );
    }
    names.len()
}

/// The 43 entries in the active Go driver's `go/driver.go` optionRemapping.
#[test]
fn every_go_option_remapping_key_resolves() {
    // Go remaps these vestigial auth values, legacy SQL options, and its REST buffer setting.
    const UNSUPPORTED: &[&str] = &[
        "adbc.bigquery.sql.auth_type.json_credentials",
        "adbc.bigquery.sql.auth_type.oauth_client_ids",
        "adbc.bigquery.sql.query.allow_large_results",
        "adbc.bigquery.sql.query.disable_flattened_results",
        "adbc.bigquery.sql.query.result_buffer_size",
        "adbc.bigquery.sql.query.use_legacy_sql",
    ];
    let database = assert_remappings::<BigQueryDatabaseOption>(
        "adbc.bigquery.sql.",
        "bigquery.",
        &[
            "auth.client_id",
            "auth.client_secret",
            "auth.quota_project",
            "auth.refresh_token",
            "auth_type",
            "dataset_id",
            "endpoint",
            "impersonate.delegates",
            "impersonate.lifetime",
            "impersonate.scopes",
            "impersonate.target_principal",
            "location",
            "project_id",
            "storage_endpoint",
        ],
    );
    // This alias also changes the separator within the option name.
    assert_eq!(
        BigQueryDatabaseOption::from_name("adbc.bigquery.sql.auth_credentials"),
        Some(BigQueryDatabaseOption::AuthenticationCredentials)
    );
    let statement = assert_remappings::<BigQueryStatementOption>(
        "adbc.bigquery.sql.query.",
        "bigquery.query.",
        &[
            "create_disposition",
            "create_session",
            "default_dataset_id",
            "default_project_id",
            "destination_table",
            "disable_query_cache",
            "dry_run",
            "job_timeout",
            "max_billing_tier",
            "max_bytes_billed",
            "parameter_mode",
            "prefetch_concurrency",
            "priority",
            "write_disposition",
        ],
    );
    let authentication = assert_remappings::<BigQueryAuthenticationType>(
        "adbc.bigquery.sql.auth_type.",
        "",
        &[
            "anonymous",
            "app_default_credentials",
            "auth_bigquery",
            "json_credential_file",
            "json_credential_string",
            "user_authentication",
        ],
    );
    let parameters = assert_remappings::<crate::parameters::ParameterMode>(
        "adbc.bigquery.sql.query.parameter_mode_",
        "",
        &["named", "positional"],
    );
    for &key in UNSUPPORTED {
        assert_eq!(BigQueryDatabaseOption::from_name(key), None, "{key}");
        assert_eq!(BigQueryConnectionOption::from_name(key), None, "{key}");
        assert_eq!(BigQueryStatementOption::from_name(key), None, "{key}");
    }
    assert_eq!(
        database + 1 + statement + authentication + parameters + UNSUPPORTED.len(),
        43,
        "the Go driver's optionRemapping has 43 entries; re-read it before changing this"
    );
}

/// Additional aliases accepted by the dbt-labs fork.
#[test]
fn aliases_outside_the_go_remapping_are_accounted_for() {
    for (alias, option) in [
        ("api_endpoint", BigQueryDatabaseOption::Endpoint),
        (
            "storage_read_api_endpoint",
            BigQueryDatabaseOption::StorageEndpoint,
        ),
    ] {
        assert_eq!(
            BigQueryDatabaseOption::from_name(&format!("adbc.bigquery.sql.{alias}")),
            Some(option)
        );
    }
    assert_remappings::<BigQueryDatabaseOption>(
        "adbc.bigquery.sql.auth.",
        "bigquery.auth.",
        &[
            "access_token",
            "access_token_endpoint",
            "external_account.audience",
            "external_account.request_url",
            "external_account.request_data",
            "external_account.impersonation_url",
        ],
    );
    assert_remappings::<BigQueryAuthenticationType>(
        "adbc.bigquery.sql.auth_type.",
        "",
        &["temporary_access_token", "external_account"],
    );
    assert_remappings::<BigQueryStatementOption>(
        "adbc.bigquery.sql.query.",
        "bigquery.query.",
        &["labels", "link_failed_job"],
    );
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
