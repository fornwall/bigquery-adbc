//! The parameter type a bound Arrow field maps to, and the field metadata that refines it.

use super::*;

/// A dictionary key type BigQuery cannot index by names the field it was found on, like every
/// other per-field rejection in this module.
#[test]
fn a_non_integer_dictionary_key_names_its_parameter_field() {
    let schema = Schema::new(vec![Field::new(
        "label",
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
        true,
    )]);
    let failure = parameters_for_schema(&schema, ParameterMode::Named).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter 0: Arrow dictionary parameter field \"label\" has key type \
         Utf8, which is not an integer"
    );
}

#[test]
fn array_parameters_require_a_struct_between_nested_arrays() {
    let lists = |element| {
        let item = Arc::new(Field::new("item", element, true));
        [
            DataType::List(item.clone()),
            DataType::LargeList(item.clone()),
            DataType::ListView(item.clone()),
            DataType::LargeListView(item.clone()),
            DataType::FixedSizeList(item, 1),
        ]
    };
    for inner in lists(DataType::Int64) {
        for inner in [
            inner.clone(),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(inner)),
        ] {
            for outer in lists(inner.clone()) {
                let schema = Schema::new(vec![Field::new("matrix", outer, true)]);
                for mode in [ParameterMode::Named, ParameterMode::Positional] {
                    let failure = parameters_for_schema(&schema, mode).unwrap_err();
                    assert_failure(
                        &failure,
                        Status::NotImplemented,
                        &["parameter 0", "matrix", "STRUCT"],
                    );
                }
            }

            let record = DataType::Struct(vec![Field::new("values", inner, true)].into());
            for outer in lists(record) {
                let schema = Schema::new(vec![Field::new("matrix", outer, true)]);
                let parameters = parameters_for_schema(&schema, ParameterMode::Named).unwrap();
                let record = element(ptype(&parameters[0]));
                assert_eq!(record.r#type, "STRUCT");
                assert_eq!(
                    record.struct_types[0].r#type.as_ref().unwrap().r#type,
                    "ARRAY"
                );
            }
        }
    }
}

/// One table for every Arrow decimal parameter input: width classification, negative-scale
/// and high-scale formatting, explicit `BIGQUERY:type` metadata, and precision validation.
#[test]
fn converts_and_validates_decimal_parameters() {
    let batch = |array: ArrayRef, rich_type: Option<&str>| {
        let mut field = Field::new("value", array.data_type().clone(), true);
        if let Some(rich_type) = rich_type {
            field = bigquery_typed(field, rich_type);
        }
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
    };

    // (label, column, BIGQUERY:type metadata, expected type, expected value)
    let successes: [(&str, ArrayRef, Option<&str>, &str, &str); 6] = [
        (
            "decimal32 fits NUMERIC",
            Arc::new(
                Decimal32Array::from(vec![123])
                    .with_precision_and_scale(3, 2)
                    .unwrap(),
            ),
            None,
            "NUMERIC",
            "1.23",
        ),
        (
            "decimal64 with a negative scale fits NUMERIC",
            Arc::new(
                Decimal64Array::from(vec![-12])
                    .with_precision_and_scale(2, -3)
                    .unwrap(),
            ),
            None,
            "NUMERIC",
            "-12000",
        ),
        (
            "decimal128 scale beyond NUMERIC needs BIGNUMERIC",
            decimal128(123, 38, 20),
            None,
            "BIGNUMERIC",
            "0.00000000000000000123",
        ),
        (
            "decimal256 within NUMERIC bounds stays NUMERIC",
            decimal256(123, 38, 9),
            None,
            "NUMERIC",
            "0.000000123",
        ),
        (
            "decimal256 value within its precision",
            decimal256(12_345, 5, 2),
            None,
            "NUMERIC",
            "123.45",
        ),
        (
            "explicit BIGNUMERIC metadata is preserved",
            decimal256(123, 38, 9),
            Some("BIGNUMERIC"),
            "BIGNUMERIC",
            "0.000000123",
        ),
    ];
    for (label, array, rich_type, expected_type, expected_value) in successes {
        let parameters = parameters_for_row(&batch(array, rich_type), 0, ParameterMode::Named)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        assert_eq!(ptype(&parameters[0]).r#type, expected_type, "{label}");
        assert_eq!(value_of(&parameters[0]), Some(expected_value), "{label}");
    }

    // Metadata the Arrow type cannot carry is an invalid argument.
    let error = parameters_for_row(
        &batch(decimal128(123, 38, 20), Some("NUMERIC")),
        0,
        ParameterMode::Positional,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);

    // A value its type cannot hold is invalid data: (label, column, lowercased message fragments).
    for (label, array, fragments) in [
        (
            "decimal128 value exceeds its precision",
            decimal128(12_345_678, 5, 2),
            ["row 0", "precision 5", "value", "decimal128"],
        ),
        (
            "decimal256 value exceeds its precision",
            decimal256(12_345_678, 5, 2),
            ["row 0", "precision 5", "value", "decimal256"],
        ),
    ] {
        let error =
            parameters_for_row(&batch(array, None), 0, ParameterMode::Positional).unwrap_err();
        assert_eq!(error.status, Status::InvalidData, "{label}");
        crate::test_support::errors::assert_message_omits(&error, &["123456.78", "12345678"]);
        for fragment in fragments {
            assert!(
                error.message.to_ascii_lowercase().contains(fragment),
                "{label}: {fragment:?} missing from {}",
                error.message
            );
        }
    }

    // ARRAY<BIGNUMERIC> metadata reaches the element type of a repeated parameter.
    let repeated = bigquery_typed(
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal256(38, 9),
                true,
            ))),
            false,
        ),
        "ARRAY<BIGNUMERIC>",
    );
    let repeated_type = parameter_type(&repeated).unwrap();
    assert_eq!(repeated_type.r#type, "ARRAY");
    assert_eq!(repeated_type.array_type.unwrap().r#type, "BIGNUMERIC");
}

/// `Duration` and `Null` are the two Arrow types `inferred_scalar_type` does not map, so this
/// module supplies their BigQuery type. A duration resolves `BIGQUERY:type` metadata like every
/// other type: annotated `STRING`, it is refused, as the same annotation on an `Int64` is. A `Null`
/// array holds only NULLs, which every scalar type has, so any scalar annotation types it: binding
/// `None` to an `INT64` column needs no typed array.
#[test]
fn duration_and_null_parameters_resolve_their_type_metadata() {
    // Unannotated, each keeps the type its Arrow type implies.
    for (data_type, expected) in [
        (DataType::Duration(TimeUnit::Second), "INTERVAL"),
        (DataType::Null, "STRING"),
    ] {
        let field = Field::new("value", data_type.clone(), true);
        assert_eq!(parameter_type(&field).unwrap().r#type, expected);
        // An annotation agreeing with it, as a query result carries, is honored.
        assert_eq!(
            parameter_type(&annotated(data_type, expected))
                .unwrap()
                .r#type,
            expected
        );
    }
    for rich_type in ["INT64", "DATE", "NUMERIC", "JSON", "TIMESTAMP", "INTERVAL"] {
        let parameter = parameter_type(&annotated(DataType::Null, rich_type)).unwrap();
        assert_eq!(parameter.r#type, rich_type);
    }

    // A contradicting annotation is refused, not ignored, and a composite name is no scalar type.
    for (data_type, rich_type) in [
        (DataType::Duration(TimeUnit::Second), "STRING"),
        (DataType::Duration(TimeUnit::Nanosecond), "INT64"),
        (DataType::Null, "STRUCT"),
        (DataType::Null, "RANGE"),
    ] {
        let failure = parameter_type(&annotated(data_type, rich_type)).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments, "{rich_type}");
        assert_message(&failure, &["is incompatible with Arrow field"]);
    }

    // A name no BigQuery type spells is still unimplemented rather than incompatible, and a REST
    // spelling still points at its GoogleSQL name.
    assert_eq!(
        parameter_type(&annotated(DataType::Null, "NOT_A_TYPE"))
            .unwrap_err()
            .status,
        Status::NotImplemented
    );
    let rest_spelling = parameter_type(&annotated(DataType::Null, "INTEGER")).unwrap_err();
    assert_message(&rest_spelling, &["INT64"]);
}

#[test]
fn parameter_metadata_must_use_the_googlesql_names_the_driver_emits() {
    // A query result annotates its fields with GoogleSQL names, so binding that result back
    // must keep working.
    for (data_type, rich_type) in [
        (DataType::Int64, "INT64"),
        (DataType::Float64, "FLOAT64"),
        (DataType::Boolean, "BOOL"),
        (DataType::Date32, "DATE"),
        (DataType::Binary, "BYTES"),
        (DataType::Interval(IntervalUnit::MonthDayNano), "INTERVAL"),
    ] {
        assert_eq!(
            parameter_type(&annotated(data_type, rich_type))
                .unwrap()
                .r#type,
            rich_type
        );
    }

    for (data_type, rest, googlesql) in [
        (DataType::Int64, "INTEGER", "INT64"),
        (DataType::Float64, "FLOAT", "FLOAT64"),
        (DataType::Boolean, "BOOLEAN", "BOOL"),
    ] {
        let failure = parameter_type(&annotated(data_type, rest)).unwrap_err();
        assert_failure(&failure, Status::InvalidArguments, &[googlesql]);
    }

    let mislabelled = parameter_type(&annotated(DataType::Int64, "STRING")).unwrap_err();
    assert_failure(&mislabelled, Status::InvalidArguments, &["incompatible"]);

    assert_eq!(
        parameter_type(&annotated(DataType::Int64, "NOT_A_TYPE"))
            .unwrap_err()
            .status,
        Status::NotImplemented
    );

    let list = annotated(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        "INT64",
    );
    assert_eq!(
        parameter_type(&list).unwrap_err().status,
        Status::InvalidArguments
    );

    let record = annotated(
        DataType::Struct(vec![Field::new("child", DataType::Int64, true)].into()),
        "INT64",
    );
    assert_eq!(
        parameter_type(&record).unwrap_err().status,
        Status::InvalidArguments
    );
}

/// An `ARRAY<X>` annotation names its element type, and the rich element types (RANGE, JSON,
/// GEOGRAPHY) are the ones the item's Arrow type cannot imply. Ingest reads the annotation from the
/// enclosing field (`normalize_array_item` in `src/ingest/schema.rs`), so parameter binding must
/// too, or `ARRAY<RANGE<DATE>>` would bind as `ARRAY<STRUCT<start DATE, end DATE>>`.
#[test]
fn an_array_annotation_types_an_item_that_carries_no_metadata_of_its_own() {
    let (item, range) = date_range_item();
    let ranges = list_of(item, range);
    let documents = list_of(
        Field::new("item", DataType::Utf8, true),
        col(StringArray::from(vec!["{}"])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            annotated_column("periods", &ranges, "ARRAY<RANGE<DATE>>"),
            annotated_column("documents", &documents, "ARRAY<JSON>"),
        ])),
        vec![Arc::new(ranges), Arc::new(documents)],
    )
    .unwrap();

    let parameters = parameters_for_row(&batch, 0, ParameterMode::Named).unwrap();
    assert_eq!(
        json!(ptype(&parameters[0])),
        json!({
            "type": "ARRAY",
            "arrayType": {"type": "RANGE", "rangeElementType": {"type": "DATE"}}
        })
    );
    assert_eq!(
        json!(value(&parameters[0])),
        json!({"arrayValues": [{"rangeValue": {
            "start": {"value": "1970-01-01"}, "end": {"value": "1970-01-03"}
        }}]})
    );
    assert_eq!(element(ptype(&parameters[1])).r#type, "JSON");

    // An item that spells its own type keeps it: that is what a query result round-trips through.
    let strings = list_of(
        bigquery_typed(Field::new("item", DataType::Utf8, true), "STRING"),
        col(StringArray::from(vec!["text"])),
    );
    let field = annotated_column("labels", &strings, "ARRAY<JSON>");
    assert_eq!(element(&parameter_type(&field).unwrap()).r#type, "STRING");
}

/// One hand-written `ARRAY<X>` schema drives both annotation surfaces: the table an ingest would
/// create and the type a bound parameter would carry must name the same element type, since both
/// read the annotation through `crate::types::array_item_field`.
///
/// For `ARRAY<STRUCT<...>>`, neither surface parses a struct's field list from metadata, so both
/// use the item's Arrow type: a `List<Utf8>` annotated that way is a STRING element on both sides.
/// The list's element type overrides the item's Arrow extension as it would on the item itself, so
/// an `arrow.json` item under `ARRAY<STRING>` is a STRING element.
#[test]
fn ingest_and_parameter_binding_read_one_array_annotation_the_same_way() {
    let strings = || {
        list_of(
            Field::new("item", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["text"])),
        )
    };
    let json_strings = list_of(
        Field::new("item", DataType::Utf8, true).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_owned(),
            "arrow.json".to_owned(),
        )])),
        Arc::new(StringArray::from(vec!["{}"])),
    );
    let (item, range) = date_range_item();
    let periods = list_of(item, range);
    let members: arrow_schema::Fields =
        vec![Arc::new(Field::new("a", DataType::Int64, true))].into();
    let records = list_of(
        Field::new("item", DataType::Struct(members.clone()), true),
        Arc::new(StructArray::new(
            members,
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        )),
    );
    let columns: Vec<(Field, ArrayRef)> = vec![
        (
            annotated_column("tags", &strings(), "ARRAY<STRUCT<a INT64>>"),
            Arc::new(strings()),
        ),
        (
            annotated_column("documents", &strings(), "ARRAY<JSON>"),
            Arc::new(strings()),
        ),
        (
            annotated_column("places", &strings(), "ARRAY<GEOGRAPHY>"),
            Arc::new(strings()),
        ),
        (
            annotated_column("texts", &json_strings, "ARRAY<STRING>"),
            Arc::new(json_strings),
        ),
        (
            annotated_column("periods", &periods, "ARRAY<RANGE<DATE>>"),
            Arc::new(periods),
        ),
        (
            annotated_column("records", &records, "ARRAY<STRUCT<a INT64>>"),
            Arc::new(records),
        ),
    ];
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

    let ingest = arrow_schema_to_bigquery(&batch.schema()).unwrap();
    let parameters = parameters_for_row(&batch, 0, ParameterMode::Named).unwrap();
    for (column, parameter) in ingest.fields.iter().zip(&parameters) {
        let name = &column.name;
        assert_eq!(column.mode, "REPEATED", "{name}");
        assert_eq!(ptype(parameter).r#type, "ARRAY", "{name}");
        let element = element(ptype(parameter));
        assert_eq!(
            BigQueryType::from_sql(&element.r#type),
            BigQueryType::from_rest(&column.r#type),
            "{name}: parameter element {} against ingest column {}",
            element.r#type,
            column.r#type
        );
        assert_eq!(
            element
                .range_element_type
                .as_ref()
                .and_then(|bound| BigQueryType::from_sql(&bound.r#type)),
            column
                .range_element_type
                .as_ref()
                .and_then(|bound| BigQueryType::from_rest(&bound.r#type)),
            "{name}"
        );
    }
    // The element types the item's Arrow type cannot imply, spelled out so a rename of either
    // walk has to restate them.
    let element_types: Vec<&str> = parameters
        .iter()
        .map(|parameter| element(ptype(parameter)).r#type.as_str())
        .collect();
    assert_eq!(
        element_types,
        ["STRING", "JSON", "GEOGRAPHY", "STRING", "RANGE", "STRUCT"]
    );

    // And an element annotation neither surface can reconcile with the item's Arrow type is
    // refused by both, against the column the caller annotated rather than its `item` child.
    let counts = list_of(
        Field::new("item", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![1])),
    );
    let mislabelled = annotated_column("counts", &counts, "ARRAY<DATE>");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![mislabelled])),
        vec![Arc::new(counts) as ArrayRef],
    )
    .unwrap();
    let refusal = "BigQuery type metadata \"DATE\" is incompatible with Arrow field \"counts\" of \
                   type Int64";
    let failure = parameters_for_row(&batch, 0, ParameterMode::Positional).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        format!("bound BigQuery parameter 0: {refusal}")
    );
    let ingest_failure = arrow_schema_to_bigquery(&batch.schema()).unwrap_err();
    assert_eq!(ingest_failure.status, Status::InvalidArguments);
    assert_eq!(ingest_failure.message, refusal);
}

/// A struct parameter's field names are sent to BigQuery, so they must be unique (as
/// `validates_named_parameter_field_names` checks for top-level names), in positional mode too.
#[test]
fn rejects_duplicate_struct_fields() {
    let record = |first: &str, second: &str| {
        let members = vec![
            Field::new(first, DataType::Int64, false),
            Field::new(second, DataType::Utf8, false),
        ];
        let field = Field::new("record", DataType::Struct(members.into()), false);
        Schema::new(vec![field])
    };
    let failure =
        parameters_for_schema(&record("same", "same"), ParameterMode::Positional).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter 0: Arrow struct parameter field \"record\" has more than one \
         child named \"same\"; BigQuery struct field names are case-insensitive and must be \
         unique"
    );
    let failure =
        parameters_for_schema(&record("Same", "sAME"), ParameterMode::Positional).unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidArguments,
        &["children named \"Same\" and \"sAME\""],
    );
    // Unlike parameter names, struct fields fold beyond ASCII: BigQuery refuses `é` and `É` as
    // "Duplicate field name", and fails `ß` and `SS` with an internal error (observed).
    for (first, second) in [("\u{e9}", "\u{c9}"), ("\u{df}", "SS")] {
        let failure =
            parameters_for_schema(&record(first, second), ParameterMode::Named).unwrap_err();
        assert_failure(
            &failure,
            Status::InvalidArguments,
            &[&format!("children named {first:?} and {second:?}")],
        );
    }
}

/// A type no BigQuery parameter covers is refused by the field that carries it, which inside a
/// struct is not the bound parameter's own.
#[test]
fn an_unsupported_nested_type_names_its_field() {
    let schema = Schema::new(vec![Field::new(
        "record",
        DataType::Struct(
            vec![Arc::new(Field::new(
                "tags",
                DataType::Union(
                    arrow_schema::UnionFields::empty(),
                    arrow_schema::UnionMode::Dense,
                ),
                true,
            ))]
            .into(),
        ),
        true,
    )]);
    let failure = parameters_for_schema(&schema, ParameterMode::Positional).unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "bound BigQuery parameter 0: Arrow field \"tags\" has type Union(Dense), which no \
         BigQuery query parameter type covers"
    );
}
