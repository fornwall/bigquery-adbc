use arrow_schema::{IntervalUnit, TimeUnit};

use crate::test_support::schema::{field, field_mode, range_field, record, table_schema};
use crate::types::decimal::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, NUMERIC_PRECISION, NUMERIC_SCALE,
};

use super::*;
use crate::test_support::errors::assert_message;

/// One metadata value of a converted field, as the `&str` every assertion compares against.
fn meta<'a>(field: &'a Field, key: &str) -> Option<&'a str> {
    field.metadata().get(key).map(String::as_str)
}

#[test]
fn converts_supported_bigquery_scalar_schema() {
    let types = [
        "STRING",
        "BYTES",
        "INT64",
        "FLOAT64",
        "BOOL",
        "TIMESTAMP",
        "DATE",
        "TIME",
        "DATETIME",
        "NUMERIC",
        "BIGNUMERIC",
        "INTERVAL",
        "GEOGRAPHY",
        "JSON",
    ];
    let table = table_schema(types.iter().enumerate().map(|(index, kind)| {
        let mode = if index == 0 { "REQUIRED" } else { "NULLABLE" };
        field_mode(format!("column_{index}"), *kind, mode)
    }));
    let schema = convert_schema(&table).unwrap();
    assert_eq!(schema.fields().len(), types.len());
    assert!(!schema.field(0).is_nullable());
    for (index, kind) in types.iter().enumerate() {
        assert_eq!(
            meta(schema.field(index), BIGQUERY_TYPE_METADATA_KEY),
            Some(*kind)
        );
    }
    assert_eq!(
        schema.field(5).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(6).data_type(), &DataType::Date32);
    assert_eq!(
        schema.field(7).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        schema.field(9).data_type(),
        &DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE)
    );
    assert_eq!(
        schema.field(10).data_type(),
        &DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE)
    );
    assert_eq!(
        schema.field(11).data_type(),
        &DataType::Interval(IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        meta(schema.field(12), "ARROW:extension:name"),
        Some("geoarrow.wkt")
    );
    assert_eq!(
        meta(schema.field(13), "ARROW:extension:name"),
        Some("arrow.json")
    );
}

#[test]
fn maps_rest_type_names_to_googlesql_type_metadata() {
    let table = table_schema([
        field("i", "INTEGER"),
        field("f", "FLOAT"),
        field("b", "BOOLEAN"),
        record("r", "RECORD", [field("f", "FLOAT")]),
        field_mode("fs", "FLOAT", "REPEATED"),
    ]);
    let schema = convert_schema(&table).unwrap();
    let rich_type = |index: usize| meta(schema.field(index), BIGQUERY_TYPE_METADATA_KEY).unwrap();
    assert_eq!(rich_type(0), "INT64");
    assert_eq!(rich_type(1), "FLOAT64");
    assert_eq!(rich_type(2), "BOOL");
    assert_eq!(rich_type(3), "STRUCT<`f` FLOAT64>");
    assert_eq!(rich_type(4), "ARRAY<FLOAT64>");
}

/// A STRUCT's rich type is a GoogleSQL literal a consumer may use in a CAST, so child names are
/// quoted identifiers with string-literal escapes: the backslash is doubled before the backtick,
/// or a name ending in a backslash would escape the closing backtick. BigQuery's flexible column
/// names admit neither character, so this checks the escaping, not a reachable decode.
#[test]
fn escapes_backslashes_and_backticks_in_struct_child_names() {
    let table = table_schema([record(
        "r",
        "RECORD",
        [
            field("a\\", "INT64"),
            field("b`c", "INT64"),
            field("d\\`e", "INT64"),
        ],
    )]);
    let schema = convert_schema(&table).unwrap();
    assert_eq!(
        meta(schema.field(0), BIGQUERY_TYPE_METADATA_KEY),
        Some("STRUCT<`a\\\\` INT64, `b\\`c` INT64, `d\\\\\\`e` INT64>")
    );
}

#[test]
fn preserves_constrained_bigquery_string_types() {
    let bounded = |name: &str, kind: &str, max_length: i64| TableFieldSchema {
        max_length,
        ..field(name, kind)
    };
    let table = table_schema([
        bounded("text", "STRING", 32),
        bounded("data", "BYTES", 64),
        TableFieldSchema {
            max_length: 16,
            ..field_mode("tags", "STRING", "REPEATED")
        },
        record("record", "RECORD", [bounded("label", "STRING", 8)]),
    ]);
    let schema = convert_schema(&table).unwrap();
    assert_eq!(
        meta(schema.field(0), BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some("32")
    );
    assert_eq!(
        meta(schema.field(1), BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some("64")
    );
    assert_eq!(
        meta(schema.field(2), BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some("16")
    );
    let DataType::List(item) = schema.field(2).data_type() else {
        panic!("expected repeated STRING to be represented as an Arrow list");
    };
    assert!(
        !item
            .metadata()
            .contains_key(BIGQUERY_FIELD_METADATA_MAX_LENGTH)
    );
    let DataType::Struct(children) = schema.field(3).data_type() else {
        panic!("expected RECORD to be represented as an Arrow struct");
    };
    assert_eq!(
        meta(&children[0], BIGQUERY_FIELD_METADATA_MAX_LENGTH),
        Some("8")
    );
    // The Go driver's key for the same value, on the same field, including the outer field of a
    // repeated column, where Go puts it too.
    for (index, expected) in [(0, "32"), (1, "64"), (2, "16")] {
        assert_eq!(meta(schema.field(index), "MaxLength"), Some(expected));
    }
    assert_eq!(meta(&children[0], "MaxLength"), Some("8"));
    assert!(!item.metadata().contains_key("MaxLength"));
}

/// A column BigQuery reports no length, collation or policy tags for carries none of those keys,
/// like every other optional key. The Go driver instead writes `MaxLength` as `"0"` and
/// `Collation` as `""` on every STRING.
#[test]
fn unconstrained_columns_carry_no_length_collation_or_policy_tags() {
    let schema = convert_schema(&table_schema([field("text", "STRING")])).unwrap();
    for key in [
        BIGQUERY_FIELD_METADATA_MAX_LENGTH,
        "MaxLength",
        "Collation",
        "PolicyTags",
    ] {
        assert_eq!(meta(schema.field(0), key), None, "{key}");
    }
}

/// A column's collation and policy tags are decoded for schema patches either way, and the Go
/// driver publishes both. Policy tags are emitted as BigQuery spells them, `{"names":[...]}`,
/// rather than as the Go client struct's `{"Names":[...]}`.
#[test]
fn a_column_publishes_its_collation_and_policy_tags() {
    let table = table_schema([
        TableFieldSchema {
            collation: Some(serde_json::json!("und:ci")),
            policy_tags: Some(crate::rest::model::PolicyTagList {
                names: vec![
                    "projects/p/locations/l/taxonomies/t/policyTags/1".to_owned(),
                    "projects/p/locations/l/taxonomies/t/policyTags/2".to_owned(),
                ],
            }),
            ..field("text", "STRING")
        },
        TableFieldSchema {
            collation: Some(serde_json::json!("")),
            policy_tags: Some(crate::rest::model::PolicyTagList { names: Vec::new() }),
            ..field("plain", "STRING")
        },
    ]);
    let schema = convert_schema(&table).unwrap();
    assert_eq!(meta(schema.field(0), "Collation"), Some("und:ci"));
    assert_eq!(
        meta(schema.field(0), "PolicyTags"),
        Some(
            r#"{"names":["projects/p/locations/l/taxonomies/t/policyTags/1","projects/p/locations/l/taxonomies/t/policyTags/2"]}"#
        )
    );
    // An empty collation and an empty tag list say nothing, so neither is published.
    assert_eq!(meta(schema.field(1), "Collation"), None);
    assert_eq!(meta(schema.field(1), "PolicyTags"), None);
}

/// A table's schema converts to what reading the table returns. Query results report every
/// decimal without its declared shape, so a declared one converts to the same full type, and
/// clients that bind a scan from `GetTableSchema` and check the stream against it agree. The
/// declared shape moves to metadata; BigQuery omits a zero scale (`NUMERIC(10)`).
#[test]
fn a_declared_decimal_shape_is_metadata_on_the_type_query_results_use() {
    let declared = |name: &str, kind: &str, precision: i64, scale: i64| TableFieldSchema {
        precision,
        scale,
        ..field(name, kind)
    };
    let schema = convert_schema(&table_schema([
        declared("numeric", "NUMERIC", 10, 2),
        declared("integral", "NUMERIC", 10, 0),
        declared("bignumeric", "BIGNUMERIC", 20, 4),
        field("plain", "NUMERIC"),
    ]))
    .unwrap();
    let numeric = DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE);
    let bignumeric = DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE);
    for (index, data_type, shape) in [
        (0, &numeric, Some(("10", "2"))),
        (1, &numeric, Some(("10", "0"))),
        (2, &bignumeric, Some(("20", "4"))),
        (3, &numeric, None),
    ] {
        let field = schema.field(index);
        assert_eq!(field.data_type(), data_type, "{}", field.name());
        let declared = meta(field, BIGQUERY_FIELD_METADATA_PRECISION)
            .zip(meta(field, BIGQUERY_FIELD_METADATA_SCALE));
        assert_eq!(declared, shape, "{}", field.name());
    }
}

#[test]
fn rejects_invalid_bigquery_decimal_constraints() {
    for (data_type, precision, scale) in [
        ("NUMERIC", 0, 1),
        ("NUMERIC", 10, 10),
        ("NUMERIC", 31, 1),
        ("BIGNUMERIC", 40, 40),
        ("BIGNUMERIC", 77, 38),
    ] {
        let column = TableFieldSchema {
            precision,
            scale,
            ..field("invalid_decimal", data_type)
        };
        let error = convert_schema(&table_schema([column])).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        // Every sibling failure in `convert_field` shares the status, so the message has to
        // carry what a caller acts on: which column, and which constraint it violated.
        for fragment in [
            "invalid_decimal",
            data_type,
            "invalid decimal shape",
            &precision.to_string(),
            &scale.to_string(),
        ] {
            assert!(
                error.message.contains(fragment),
                "{data_type}({precision}, {scale}): {fragment:?} missing from {}",
                error.message
            );
        }
    }
}

#[test]
fn rejects_invalid_bigquery_string_constraints() {
    for (name, kind, max_length) in [("negative", "STRING", -1), ("integer", "INTEGER", 8)] {
        let column = TableFieldSchema {
            max_length,
            ..field(name, kind)
        };
        let error = convert_schema(&table_schema([column])).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        for fragment in [name, kind, "invalid max length", &max_length.to_string()] {
            assert!(
                error.message.contains(fragment),
                "{name}: {fragment:?} missing from {}",
                error.message
            );
        }
    }
}

/// The Go driver's legacy field metadata dbt-core 2.0 parses: `Type` carries the REST spelling
/// exactly as BigQuery reported it, `Repeated` marks arrays, `Description` carries comments.
#[test]
fn fields_carry_legacy_rest_metadata_for_dbt() {
    let table = table_schema([
        field_mode("tags", "INTEGER", "REPEATED"),
        TableFieldSchema {
            description: Some("nested payload".to_owned()),
            ..record("payload", "RECORD", [field("inner", "STRING")])
        },
    ]);
    let schema = convert_schema(&table).unwrap();

    let tags = schema.field(0);
    assert_eq!(meta(tags, "Type"), Some("INTEGER"));
    assert_eq!(meta(tags, "Repeated"), Some("true"));
    assert_eq!(meta(tags, BIGQUERY_TYPE_METADATA_KEY), Some("ARRAY<INT64>"));

    let payload = schema.field(1);
    assert_eq!(meta(payload, "Type"), Some("RECORD"));
    assert_eq!(meta(payload, "Repeated"), None);
    assert_eq!(meta(payload, "Description"), Some("nested payload"));
    let DataType::Struct(children) = schema.field(1).data_type() else {
        panic!("expected a struct field");
    };
    assert_eq!(meta(&children[0], "Type"), Some("STRING"));
    assert_eq!(meta(&children[0], "Description"), None);
}

#[test]
fn converts_bigquery_range_schema() {
    let table = table_schema([
        range_field("dates", "DATE"),
        range_field("datetimes", "DATETIME"),
        range_field("timestamps", "TIMESTAMP"),
    ]);
    let schema = convert_schema(&table).unwrap();
    assert_eq!(schema.fields().len(), 3);

    for (index, (element_type, rich_type)) in [
        (DataType::Date32, "RANGE<DATE>"),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "RANGE<DATETIME>",
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "RANGE<TIMESTAMP>",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let field = schema.field(index);
        assert_eq!(meta(field, BIGQUERY_TYPE_METADATA_KEY), Some(rich_type));
        let DataType::Struct(bounds) = field.data_type() else {
            panic!("expected a struct field");
        };
        assert_eq!(bounds.len(), 2);
        assert_eq!(bounds[0].name(), "start");
        assert_eq!(bounds[0].data_type(), &element_type);
        assert!(bounds[0].is_nullable());
        assert_eq!(bounds[1].name(), "end");
        assert_eq!(bounds[1].data_type(), &element_type);
        assert!(bounds[1].is_nullable());
    }
}

#[test]
fn converts_nested_and_repeated_schema() {
    let table = table_schema([
        field_mode("locations", "GEOGRAPHY", "REPEATED"),
        TableFieldSchema {
            fields: vec![
                field_mode("required", "INT64", "REQUIRED"),
                field_mode("jsons", "JSON", "REPEATED"),
            ],
            ..field_mode("record", "RECORD", "NULLABLE")
        },
    ]);
    let schema = convert_schema(&table).unwrap();
    assert_eq!(schema.fields().len(), 2);

    let locations = schema.field(0);
    assert_eq!(
        meta(locations, BIGQUERY_TYPE_METADATA_KEY),
        Some("ARRAY<GEOGRAPHY>")
    );
    assert!(!locations.metadata().contains_key("ARROW:extension:name"));
    assert!(
        !locations
            .metadata()
            .contains_key("ARROW:extension:metadata")
    );
    let DataType::List(location_item) = locations.data_type() else {
        panic!("expected a list field");
    };
    assert_eq!(location_item.data_type(), &DataType::Utf8);
    assert_eq!(
        meta(location_item, BIGQUERY_TYPE_METADATA_KEY),
        Some("GEOGRAPHY")
    );
    assert_eq!(
        meta(location_item, "ARROW:extension:name"),
        Some("geoarrow.wkt")
    );

    // The CRS must travel with the extension name onto the item (Go driver PR #313).
    let geography_metadata: serde_json::Value =
        serde_json::from_str(meta(location_item, "ARROW:extension:metadata").unwrap()).unwrap();
    assert_eq!(
        geography_metadata,
        serde_json::json!({
            "crs": "EPSG:4326",
            "crs_type": "authority_code",
            "edges": "spherical",
        })
    );

    let record = schema.field(1);
    assert_eq!(
        meta(record, BIGQUERY_TYPE_METADATA_KEY),
        Some("STRUCT<`required` INT64, `jsons` ARRAY<JSON>>")
    );
    let DataType::Struct(fields) = record.data_type() else {
        panic!("expected a struct field");
    };
    assert!(!fields[0].is_nullable());
    let DataType::List(json_item) = fields[1].data_type() else {
        panic!("expected a nested list field");
    };
    assert_eq!(meta(json_item, "ARROW:extension:name"), Some("arrow.json"));
    assert_eq!(meta(json_item, BIGQUERY_TYPE_METADATA_KEY), Some("JSON"));
}

/// `get_objects` spells a column's type without converting it; the literal must be the one the
/// converted field carries, and `None` once any nested type is unimplemented.
#[test]
fn a_type_literal_matches_the_converted_fields_metadata() {
    let nested = TableFieldSchema {
        fields: vec![
            field_mode("ids", "INTEGER", "REPEATED"),
            range_field("period", "DATETIME"),
            record("inner", "RECORD", [field("flag", "BOOLEAN")]),
        ],
        ..field_mode("record", "RECORD", "REPEATED")
    };
    let columns = [
        field("ratio", "FLOAT"),
        field_mode("tags", "STRING", "REPEATED"),
        range_field("window", "TIMESTAMP"),
        nested,
    ];
    let schema = convert_schema(&table_schema(columns.clone())).unwrap();
    for (column, converted) in columns.iter().zip(schema.fields()) {
        assert_eq!(
            googlesql_type_literal(column).as_deref(),
            meta(converted, BIGQUERY_TYPE_METADATA_KEY),
            "{}",
            column.name
        );
    }
    assert_eq!(
        googlesql_type_literal(&columns[3]).as_deref(),
        Some(
            "ARRAY<STRUCT<`ids` ARRAY<INT64>, `period` RANGE<DATETIME>, `inner` STRUCT<`flag` \
             BOOL>>>"
        )
    );

    assert_eq!(googlesql_type_literal(&field("mystery", "FOREIGN")), None);
    assert_eq!(
        googlesql_type_literal(&record("outer", "STRUCT", [field("mystery", "FOREIGN")])),
        None
    );
    assert_eq!(
        googlesql_type_literal(&range_field("period", "INT64")),
        None
    );
}

#[test]
fn rejects_invalid_range_schema() {
    for (column, fragments) in [
        (
            field_mode("missing", "RANGE", "NULLABLE"),
            ["missing", "is missing its element type"].as_slice(),
        ),
        (
            range_field("unsupported", "STRING"),
            ["unsupported", "unsupported element type STRING"].as_slice(),
        ),
    ] {
        let error = convert_schema(&table_schema([column])).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        for fragment in fragments {
            assert!(
                error.message.contains(fragment),
                "{fragment:?} missing from {}",
                error.message
            );
        }
    }
}

#[test]
fn rejects_composite_metadata_on_the_wrong_type() {
    let child = field("child", "STRING");
    for (column, expected) in [
        (
            record("scalar_with_fields", "STRING", [child.clone()]),
            "nested fields",
        ),
        (
            TableFieldSchema {
                fields: vec![child],
                ..range_field("range_with_fields", "DATE")
            },
            "nested fields",
        ),
        (
            TableFieldSchema {
                r#type: "INT64".to_owned(),
                ..range_field("scalar_with_range_element", "DATE")
            },
            "RANGE element type",
        ),
        (
            TableFieldSchema {
                r#type: "RECORD".to_owned(),
                ..range_field("record_with_range_element", "DATE")
            },
            "RANGE element type",
        ),
    ] {
        let name = column.name.clone();
        let error = convert_schema(&table_schema([column])).unwrap_err();
        assert_eq!(error.status, Status::InvalidData);
        assert_message(&error, &[&name, expected]);
    }
}

#[test]
fn rejects_unknown_field_modes() {
    let error =
        convert_schema(&table_schema([field_mode("value", "STRING", "UNKNOWN")])).unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert_message(&error, &["\"UNKNOWN\"", "value"]);
}

/// A temporal column declaring more sub-second digits than Arrow's microsecond type holds would
/// decode wrongly, so it is refused, as on the parameter path, with an error naming the CAST that
/// makes it readable.
#[test]
fn a_temporal_column_finer_than_microseconds_is_refused_rather_than_decoded() {
    let precise = |kind: &str, precision: serde_json::Value| TableFieldSchema {
        timestamp_precision: Some(precision),
        ..field(format!("{}_column", kind.to_lowercase()), kind)
    };

    for kind in ["TIMESTAMP", "DATETIME", "TIME"] {
        // BigQuery spells an int64 as a JSON string; a number is accepted the same way.
        for precision in [serde_json::json!("12"), serde_json::json!(9)] {
            let failure = convert_schema(&table_schema([precise(kind, precision.clone())]))
                .expect_err(&format!("{kind} {precision}"));
            assert_eq!(failure.status, Status::NotImplemented);
            assert!(
                failure.message.contains("at most microsecond precision")
                    && failure.message.contains(&format!("CAST it to {kind}(6)")),
                "{kind} {precision}: {failure}"
            );
        }
    }

    // Six digits or fewer is exactly what the canonical type holds, and a precision spelled as
    // something other than an integer declares nothing.
    for precision in [
        serde_json::json!("6"),
        serde_json::json!(0),
        serde_json::json!("not a number"),
    ] {
        let schema = convert_schema(&table_schema([precise("TIMESTAMP", precision.clone())]))
            .unwrap_or_else(|failure| panic!("{precision}: {failure}"));
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }
}

/// Schema conversion and the `ColumnBuilder` walks recurse once per STRUCT level of a
/// server-supplied schema. BigQuery caps nesting at 15 levels, but a stack overflow would abort
/// the host process rather than unwind through the FFI guard, so conversion checks depth before
/// visiting children.
#[test]
fn a_result_schema_nested_past_the_supported_depth_is_refused_rather_than_recursed_into() {
    let nested = |depth: usize| {
        let mut field = field("leaf", "STRING");
        for level in (1..depth).rev() {
            field = record(format!("level{level}"), "RECORD", [field]);
        }
        field
    };

    // The deepest schema the driver converts, and the first one past it.
    let deepest = nested(MAX_SCHEMA_NESTING_DEPTH);
    assert!(convert_field(&deepest).is_ok());
    assert!(convert_schema(&table_schema([deepest])).is_ok());
    let too_deep = nested(MAX_SCHEMA_NESTING_DEPTH + 1);
    for failure in [
        convert_field(&too_deep).unwrap_err(),
        convert_schema(&table_schema([too_deep])).unwrap_err(),
    ] {
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!(
                "BigQuery result field \"leaf\" is nested {} levels deep; the maximum is \
                 {MAX_SCHEMA_NESTING_DEPTH}",
                MAX_SCHEMA_NESTING_DEPTH + 1
            )
        );
    }

    // Even a much deeper input stops at the first unsupported level, before the type-literal
    // formatter or the row builders can walk it.
    let hostile = nested(256);
    let failure = convert_field(&hostile).unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert!(failure.message.contains(&format!(
        "is nested {} levels deep",
        MAX_SCHEMA_NESTING_DEPTH + 1
    )));

    // Siblings each start at their parent's depth, including repeated STRUCTs.
    let mut repeated = nested(MAX_SCHEMA_NESTING_DEPTH);
    repeated.mode = "REPEATED".to_owned();
    assert!(
        convert_schema(&table_schema([
            nested(MAX_SCHEMA_NESTING_DEPTH),
            repeated,
            field("last", "INTEGER"),
        ]))
        .is_ok()
    );
}
