//! Round-trip tests for the driver's own REST message shapes.

use super::*;
use crate::test_support::schema::{decimal, field, field_mode, record, table_schema};
use std::collections::HashMap;

/// A scalar parameter type.
fn ptype(kind: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: kind.to_owned(),
        ..QueryParameterType::default()
    }
}

/// A scalar parameter value.
fn pvalue(value: &str) -> QueryParameterValue {
    QueryParameterValue {
        value: Some(value.to_owned()),
        ..QueryParameterValue::default()
    }
}

/// The exact JSON is the `tables.insert` and `tables.patch` contract: defaulted fields are
/// omitted, and the three integer constraints are decimal strings.
#[test]
fn a_schema_renders_the_fields_bigquery_documents_and_omits_the_rest() {
    let schema = table_schema([
        field_mode("id", "INTEGER", "REQUIRED"),
        TableFieldSchema {
            description: Some(String::new()),
            max_length: 64,
            ..field_mode("label", "STRING", "NULLABLE")
        },
        decimal(field("amount", "NUMERIC"), 38, 9),
        TableFieldSchema {
            range_element_type: Some(FieldElementType {
                r#type: "DATETIME".to_owned(),
            }),
            ..field("span", "RANGE")
        },
        TableFieldSchema {
            policy_tags: Some(PolicyTagList {
                names: vec!["projects/p/policyTags/1".to_owned()],
            }),
            ..record("nested", "RECORD", [field("inner", "STRING")])
        },
    ]);
    assert_eq!(
        serde_json::to_string(&schema).unwrap(),
        r#"{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING","mode":"NULLABLE","description":"","maxLength":"64"},{"name":"amount","type":"NUMERIC","precision":"38","scale":"9"},{"name":"span","type":"RANGE","rangeElementType":{"type":"DATETIME"}},{"name":"nested","type":"RECORD","fields":[{"name":"inner","type":"STRING"}],"policyTags":{"names":["projects/p/policyTags/1"]}}]}"#
    );
}

/// BigQuery renders the integer constraints as strings and may render them as numbers; both
/// decode, and a schema decoded from either renders back to the string spelling.
#[test]
fn integer_constraints_decode_from_strings_and_numbers() {
    let from_strings: TableSchema = serde_json::from_str(
        r#"{"fields":[{"name":"a","type":"STRING","maxLength":"10"},
             {"name":"b","type":"NUMERIC","precision":"38","scale":"9"}]}"#,
    )
    .unwrap();
    let from_numbers: TableSchema = serde_json::from_str(
        r#"{"fields":[{"name":"a","type":"STRING","maxLength":10},
             {"name":"b","type":"NUMERIC","precision":38,"scale":9}]}"#,
    )
    .unwrap();
    assert_eq!(from_strings, from_numbers);
    assert_eq!(from_strings.fields[0].max_length, 10);
    assert_eq!(from_strings.fields[1].precision, 38);
    assert_eq!(from_strings.fields[1].scale, 9);
    assert_eq!(
        serde_json::to_string(&from_numbers).unwrap(),
        r#"{"fields":[{"name":"a","type":"STRING","maxLength":"10"},{"name":"b","type":"NUMERIC","precision":"38","scale":"9"}]}"#
    );
}

/// A Google JSON API omits a default-valued field, and an explicit `null` says the same thing.
#[test]
fn null_reads_as_the_default_of_every_field() {
    let schema: TableSchema = serde_json::from_str(
        r#"{"fields":[{"name":null,"type":null,"mode":null,"fields":null,"maxLength":null,
             "precision":null,"scale":null,"description":null,"policyTags":null,
             "rangeElementType":null}]}"#,
    )
    .unwrap();
    assert_eq!(schema.fields, vec![TableFieldSchema::default()]);
    assert_eq!(serde_json::to_string(&schema.fields[0]).unwrap(), "{}");

    let status: JobStatus =
        serde_json::from_str(r#"{"errorResult":null,"errors":null,"state":null}"#).unwrap();
    assert!(status.error_result.is_none() && status.errors.is_empty() && status.state.is_empty());
}

/// `tables.patch` replaces the whole schema, so a column or schema property the driver never
/// inspects, including one BigQuery adds later, has to survive the read-modify-write
/// `persist_docs` performs.
#[test]
fn unexamined_column_properties_survive_a_schema_round_trip() {
    let body = r#"{"fields":[{"name":"c","type":"STRING","collation":"und:ci","roundingMode":"ROUND_HALF_EVEN","defaultValueExpression":"'x'","timestampPrecision":"12","dataPolicies":[{"name":"p"}],"dataPolicyList":{"dataPolicies":[]},"dataGovernanceTagsInfo":{"tags":[]},"foreignTypeDefinition":"d","generatedColumn":{"expression":"1"},"somethingNew":{"a":1}}],"foreignTypeInfo":{"typeSystem":"HIVE"},"newTop":1}"#;
    let mut schema: TableSchema = serde_json::from_str(body).unwrap();
    // What `persist_docs` does between the get and the patch.
    schema.fields[0].description = Some("Documented.".to_owned());
    let mut expected = serde_json::from_str::<serde_json::Value>(body).unwrap();
    expected["fields"][0]["description"] = serde_json::json!("Documented.");
    // Field order is the struct's, not the response's, so the documents are compared as values.
    assert_eq!(serde_json::to_value(&schema).unwrap(), expected);
}

/// A key repeated in one object is a malformed document, not a last-writer-wins merge.
#[test]
fn a_duplicate_field_is_refused() {
    let decoded = serde_json::from_str::<TableSchema>(r#"{"fields":[],"fields":[]}"#);
    assert!(decoded.is_err());
}

/// The `queryParameters` rendering contract: a positional parameter has no name, and a `NULL`
/// one has an empty value object rather than none.
#[test]
fn a_positional_null_parameter_renders_as_an_empty_value() {
    let parameter = QueryParameter {
        name: String::new(),
        parameter_type: Some(ptype("INT64")),
        parameter_value: Some(QueryParameterValue::default()),
    };
    assert_eq!(
        serde_json::to_string(&parameter).unwrap(),
        r#"{"parameterType":{"type":"INT64"},"parameterValue":{}}"#
    );
}

/// `ARRAY`, `STRUCT` and `RANGE` parameters nest, and every level renders the same way.
#[test]
fn composite_parameters_render_their_nesting() {
    let array = QueryParameter {
        name: "xs".to_owned(),
        parameter_type: Some(QueryParameterType {
            array_type: Some(Box::new(ptype("INT64"))),
            ..ptype("ARRAY")
        }),
        parameter_value: Some(QueryParameterValue {
            array_values: vec![pvalue("1")],
            ..QueryParameterValue::default()
        }),
    };
    assert_eq!(
        serde_json::to_string(&array).unwrap(),
        r#"{"name":"xs","parameterType":{"type":"ARRAY","arrayType":{"type":"INT64"}},"parameterValue":{"arrayValues":[{"value":"1"}]}}"#
    );

    let structure = QueryParameter {
        name: String::new(),
        parameter_type: Some(QueryParameterType {
            struct_types: vec![QueryParameterStructType {
                name: "a".to_owned(),
                r#type: Some(Box::new(ptype("STRING"))),
            }],
            ..ptype("STRUCT")
        }),
        parameter_value: Some(QueryParameterValue {
            struct_values: HashMap::from([("a".to_owned(), pvalue("x"))]),
            ..QueryParameterValue::default()
        }),
    };
    assert_eq!(
        serde_json::to_string(&structure).unwrap(),
        r#"{"parameterType":{"type":"STRUCT","structTypes":[{"name":"a","type":{"type":"STRING"}}]},"parameterValue":{"structValues":{"a":{"value":"x"}}}}"#
    );

    let range = QueryParameter {
        name: String::new(),
        parameter_type: Some(QueryParameterType {
            range_element_type: Some(Box::new(ptype("DATE"))),
            ..ptype("RANGE")
        }),
        parameter_value: Some(QueryParameterValue {
            range_value: Some(Box::new(RangeValue {
                start: Some(Box::new(pvalue("2026-01-01"))),
                end: None,
            })),
            ..QueryParameterValue::default()
        }),
    };
    assert_eq!(
        serde_json::to_string(&range).unwrap(),
        r#"{"parameterType":{"type":"RANGE","rangeElementType":{"type":"DATE"}},"parameterValue":{"rangeValue":{"start":{"value":"2026-01-01"}}}}"#
    );
}

/// The parameter types BigQuery infers for an undeclared parameter come back on the response,
/// with `timestampPrecision` spelled as a decimal string.
#[test]
fn inferred_parameter_types_decode_from_a_response() {
    let parameters: Vec<QueryParameter> = serde_json::from_str(
        r#"[{"name":"t","parameterType":{"type":"TIMESTAMP","timestampPrecision":"12"}},
            {"parameterType":{"type":"ARRAY","arrayType":{"type":"STRING"}},"parameterValue":null}]"#,
    )
    .unwrap();
    assert_eq!(parameters[0].name, "t");
    let inferred = parameters[0].parameter_type.as_ref().unwrap();
    assert_eq!(inferred.r#type, "TIMESTAMP");
    assert_eq!(inferred.timestamp_precision, Some(12));
    assert_eq!(
        parameters[1]
            .parameter_type
            .as_ref()
            .unwrap()
            .array_type
            .as_deref()
            .unwrap()
            .r#type,
        "STRING"
    );
    assert_eq!(parameters[1].parameter_value, None);
}
