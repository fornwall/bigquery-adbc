use std::collections::HashSet;

use adbc_core::constants;
use adbc_core::options::InfoCode;
use adbc_core::schemas;
use arrow_array::{Array, BooleanArray, Int64Array, StringArray, UInt32Array, UnionArray};

use super::{INFO, TABLE_TYPES, VENDOR_VERSION, info_reader, table_types_reader};
use crate::test_support::arrow::typed;

#[test]
fn info_returns_all_known_codes_with_standard_schema() {
    let mut reader = info_reader(None).unwrap();
    assert_eq!(reader.schema(), schemas::GET_INFO_SCHEMA.clone());
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), INFO.len());

    let names = typed::<UInt32Array>(batch.column(0));
    assert!(
        names.values().windows(2).all(|pair| pair[0] < pair[1]),
        "info codes are reported once each, in code order"
    );
    let row = |code: u32| {
        names
            .values()
            .iter()
            .position(|name| *name == code)
            .unwrap_or_else(|| panic!("info code {code} is reported"))
    };
    let values = typed::<UnionArray>(batch.column(1));
    let strings = typed::<StringArray>(values.child(0));
    let booleans = typed::<BooleanArray>(values.child(1));
    let integers = typed::<Int64Array>(values.child(2));
    let string_at = |code: u32| strings.value(values.value_offset(row(code)));
    let bool_at = |code: u32| booleans.value(values.value_offset(row(code)));
    let integer_at = |code: u32| integers.value(values.value_offset(row(code)));

    assert_eq!(
        string_at(constants::ADBC_INFO_VENDOR_NAME),
        "Google BigQuery"
    );
    assert_eq!(
        string_at(constants::ADBC_INFO_VENDOR_VERSION),
        VENDOR_VERSION
    );
    // A recognized code with no value is a null row; adbc.h omits only unrecognized codes.
    for code in [
        constants::ADBC_INFO_VENDOR_ARROW_VERSION,
        constants::ADBC_INFO_VENDOR_SUBSTRAIT_MIN_VERSION,
        constants::ADBC_INFO_VENDOR_SUBSTRAIT_MAX_VERSION,
    ] {
        assert!(
            strings.is_null(values.value_offset(row(code))),
            "code {code}"
        );
    }
    assert!(bool_at(constants::ADBC_INFO_VENDOR_SQL));
    assert!(!bool_at(constants::ADBC_INFO_VENDOR_SUBSTRAIT));
    assert_eq!(
        string_at(constants::ADBC_INFO_DRIVER_ARROW_VERSION),
        format!("v{}", arrow_array::ARROW_VERSION)
    );
    assert_eq!(
        integer_at(constants::ADBC_INFO_DRIVER_ADBC_VERSION),
        i64::from(constants::ADBC_VERSION_1_1_0)
    );

    // The XDBC range adbc.h reserves for Flight SQL's GetSqlInfo metadata.
    assert!(names.values().iter().all(|code| *code < 1_000));
    assert_eq!(string_at(504), "`", "GoogleSQL quotes with backticks");
    assert_eq!(string_at(529), "dataset");
    assert_eq!(string_at(531), "project");
    assert!(bool_at(532), "qualified names start with the project");
    assert!(!bool_at(500), "GoogleSQL has no CREATE/DROP for projects");
    assert!(bool_at(515), "GoogleSQL has SELECT expression AS alias");
    assert!(bool_at(516), "GoogleSQL propagates NULL through || and +");
    assert!(!bool_at(523), "GoogleSQL LIKE takes no ESCAPE clause");
    assert!(bool_at(524), "CREATE TABLE takes a NOT NULL attribute");
    assert!(bool_at(563), "manual commit runs BEGIN/COMMIT TRANSACTION");
    assert_eq!(integer_at(543), 300);
    assert!(reader.next().is_none());
}

/// Guards the hand-maintained [`VENDOR_VERSION`] against a bumped Cargo pin.
#[test]
fn vendor_version_matches_the_pinned_client_library() {
    let lock = include_str!("../../../Cargo.lock");
    let mut in_package = false;
    let mut version = None;
    for line in lock.lines().map(str::trim) {
        if line == "[[package]]" {
            in_package = false;
        } else if line == "name = \"google-cloud-bigquery\"" {
            in_package = true;
        } else if in_package
            && let Some(found) = line
                .strip_prefix("version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        {
            assert!(
                version.replace(found).is_none(),
                "Cargo.lock pins more than one google-cloud-bigquery version"
            );
        }
    }
    let version = version.expect("Cargo.lock pins google-cloud-bigquery");
    assert_eq!(
        VENDOR_VERSION,
        format!("google-cloud-bigquery {version}"),
        "update VENDOR_VERSION in src/connection/info.rs to match the Cargo.toml pin"
    );
}

/// A requested code the driver does not answer is omitted, as adbc.h requires of unrecognized
/// codes, including the XDBC ones it leaves out: 513 (SQL_SEARCH_STRING_ESCAPE) escapes catalog
/// search patterns, which ADBC defines as unescapable; 537 (SQL_STORED_PROCEDURES_SUPPORTED) asks
/// about the JDBC `{call ...}` syntax, which GoogleSQL's `CALL` is not; 554
/// (SQL_MAX_CATALOG_NAME_LENGTH) has no uniform limit for modern and legacy domain-scoped projects.
#[test]
fn info_filters_and_ignores_unknown_codes() {
    let requested = HashSet::from([
        InfoCode::DriverName,
        InfoCode::Other(10_042),
        InfoCode::VendorSql,
        InfoCode::Other(503),
        InfoCode::Other(504),
        InfoCode::Other(513),
        InfoCode::Other(537),
        InfoCode::Other(554),
    ]);
    let batch = info_reader(Some(&requested))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let names = typed::<UInt32Array>(batch.column(0));
    assert_eq!(
        names.values(),
        &[
            constants::ADBC_INFO_VENDOR_SQL,
            constants::ADBC_INFO_DRIVER_NAME,
            504
        ]
    );
}

// That `get_objects` reports only these types is tested on returned rows by
// `objects_report_only_the_table_types_get_table_types_advertises`, since a `tables.get`
// description carries its own un-normalized `type`.

#[test]
fn table_types_use_standard_schema() {
    let mut reader = table_types_reader().unwrap();
    assert_eq!(reader.schema(), schemas::GET_TABLE_TYPES_SCHEMA.clone());
    let batch = reader.next().unwrap().unwrap();
    let values = typed::<StringArray>(batch.column(0));
    assert_eq!(values.iter().flatten().collect::<Vec<_>>(), TABLE_TYPES);
    assert!(reader.next().is_none());
}
