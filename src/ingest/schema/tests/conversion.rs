//! Arrow schema to BigQuery table schema: the shapes that convert, the ones that are refused,
//! and what a refusal names.

use super::*;

#[test]
fn a_query_result_schema_ingests_back_into_the_same_bigquery_types() {
    let original = table_schema([
        field("i", "INTEGER"),
        field("f", "FLOAT"),
        field("b", "BOOLEAN"),
        field("s", "STRING"),
        field("j", "JSON"),
        field("g", "GEOGRAPHY"),
        field("d", "DATE"),
        field("t", "TIMESTAMP"),
        field("dt", "DATETIME"),
        field("iv", "INTERVAL"),
        decimal(field("n", "NUMERIC"), 38, 9),
        field_mode("fs", "FLOAT", "REPEATED"),
        record("r", "RECORD", [field("child", "FLOAT")]),
        range("range", "DATE"),
    ]);

    // A query result carries GoogleSQL type metadata; ingesting it must reproduce the very
    // BigQuery types it was read from.
    let arrow = crate::types::schema::convert_schema(&original).unwrap();
    let round_tripped = arrow_schema_to_bigquery(&arrow).unwrap();

    let shape = |schema: &TableSchema| {
        schema
            .fields
            .iter()
            .map(|field| {
                (
                    field.name.clone(),
                    field.r#type.clone(),
                    canonical_mode(&field.mode).to_owned(),
                    field
                        .fields
                        .iter()
                        .map(|child| (child.name.clone(), child.r#type.clone()))
                        .collect::<Vec<_>>(),
                    field
                        .range_element_type
                        .as_ref()
                        .map(|element| element.r#type.clone()),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(shape(&round_tripped), shape(&original));
}

#[test]
fn rejects_deeply_nested_ingest_schemas() {
    // A struct chain 100 levels deep; the iterative depth bound must reject it before the
    // recursive schema normalizer walks it and overflows the stack.
    let mut data_type = DataType::Utf8;
    for _ in 0..100 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
    }
    let schema = Schema::new(vec![Field::new("record", data_type, true)]);

    let error = normalize_schema(&schema).unwrap_err();
    assert_failure(
        &error,
        Status::InvalidArguments,
        &["nested", &MAX_SCHEMA_NESTING_DEPTH.to_string()],
    );

    // A schema at the bound still normalizes.
    let mut data_type = DataType::Utf8;
    for _ in 0..MAX_SCHEMA_NESTING_DEPTH - 1 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
    }
    let schema = Schema::new(vec![Field::new("record", data_type, true)]);
    normalize_schema(&schema).unwrap();
}

/// `arrow_schema_to_bigquery` also converts the fields an append took from its table, which
/// normalization never saw, so it enforces the depth bound itself rather than trust its callers.
#[test]
fn rejects_deeply_nested_table_schemas() {
    let mut data_type = DataType::Utf8;
    for _ in 0..100 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
    }
    let schema = Schema::new(vec![Field::new("record", data_type, true)]);

    let error = arrow_schema_to_bigquery(&schema).unwrap_err();
    assert_failure(
        &error,
        Status::InvalidArguments,
        &[&MAX_SCHEMA_NESTING_DEPTH.to_string()],
    );

    let mut data_type = DataType::Utf8;
    for _ in 0..MAX_SCHEMA_NESTING_DEPTH - 1 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
    }
    let schema = Schema::new(vec![Field::new("record", data_type, true)]);
    arrow_schema_to_bigquery(&schema).unwrap();
}

#[test]
fn rejects_uint64_instead_of_implicitly_narrowing_it_to_int64() {
    let assert_rejected = |schema: Schema, name: &str| {
        for error in [
            normalize_schema(&schema).unwrap_err(),
            arrow_schema_to_bigquery(&schema).unwrap_err(),
        ] {
            assert_eq!(error.status, Status::NotImplemented);
            assert_eq!(
                error.message,
                format!(
                    "Arrow ingest field {name:?} has type UInt64, but BigQuery has no unsigned \
                     integer type; cast it to Int64 or Decimal128(20, 0)"
                )
            );
        }
    };

    assert_rejected(
        Schema::new(vec![Field::new("value", DataType::UInt64, true)]),
        "value",
    );
    // An element is named by its list column: the item's own name is nothing the caller bound.
    assert_rejected(
        Schema::new(vec![Field::new(
            "values",
            DataType::new_list(DataType::UInt64, true),
            true,
        )]),
        "values",
    );
}

#[test]
fn normalizes_ingest_types_and_converts_table_schema() {
    let json = bigquery_typed(Field::new("payload", DataType::LargeUtf8, true), "JSON");
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/Stockholm".into())),
            true,
        ),
        json,
        Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
    ]);
    let normalized = normalize_schema(&schema).unwrap();
    assert_eq!(normalized.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        normalized.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(normalized.field(2).data_type(), &DataType::Utf8);

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(bigquery.fields[0].r#type, "INTEGER");
    assert_eq!(bigquery.fields[0].mode, "REQUIRED");
    assert_eq!(bigquery.fields[1].r#type, "TIMESTAMP");
    assert_eq!(bigquery.fields[2].r#type, "JSON");
    assert_eq!(bigquery.fields[3].r#type, "STRING");
    assert_eq!(bigquery.fields[3].mode, "REPEATED");
}

#[test]
fn canonicalizes_list_like_schemas_for_bigquery() {
    let item = |data_type| Arc::new(Field::new("item", data_type, true));
    let nested_fields: Fields = vec![Field::new(
        "fixed_values",
        DataType::FixedSizeList(item(DataType::Boolean), 3),
        true,
    )]
    .into();
    let schema = Schema::new(vec![
        Field::new(
            "large_values",
            DataType::LargeList(item(DataType::Int32)),
            true,
        ),
        Field::new(
            "view_values",
            DataType::ListView(item(DataType::LargeUtf8)),
            true,
        ),
        Field::new(
            "large_view_values",
            DataType::LargeListView(item(DataType::UInt32)),
            true,
        ),
        Field::new("record", DataType::Struct(nested_fields), true),
    ]);

    let normalized = normalize_schema(&schema).unwrap();
    for (index, element) in [
        (0, DataType::Int64),
        (1, DataType::Utf8),
        (2, DataType::Int64),
    ] {
        let DataType::List(item) = normalized.field(index).data_type() else {
            panic!("expected canonical list")
        };
        assert_eq!(item.data_type(), &element, "column {index}");
    }
    let DataType::Struct(fields) = normalized.field(3).data_type() else {
        panic!("expected struct")
    };
    assert!(matches!(fields[0].data_type(), DataType::List(_)));

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(bigquery.fields[0].r#type, "INTEGER");
    assert_eq!(bigquery.fields[0].mode, "REPEATED");
    assert_eq!(bigquery.fields[1].r#type, "STRING");
    assert_eq!(bigquery.fields[1].mode, "REPEATED");
    assert_eq!(bigquery.fields[2].r#type, "INTEGER");
    assert_eq!(bigquery.fields[2].mode, "REPEATED");
    assert_eq!(bigquery.fields[3].r#type, "RECORD");
    assert_eq!(bigquery.fields[3].fields[0].r#type, "BOOLEAN");
    assert_eq!(bigquery.fields[3].fields[0].mode, "REPEATED");
}

/// A list item's name is not a BigQuery column name, so it is never checked as one: Arrow Java
/// names items `$data$`, Parquet-derived schemas `element`, and some writers leave them empty.
/// Storage Write ignores the name (verified live). A struct inside the list still names columns.
#[test]
fn list_item_names_are_not_checked_as_column_names() {
    let list = |name: &str, item_name: &str, data_type: DataType| {
        let item = Arc::new(Field::new(item_name, data_type, true));
        [
            Field::new(name, DataType::List(item.clone()), true),
            Field::new(
                format!("{name}_large"),
                DataType::LargeList(item.clone()),
                true,
            ),
            Field::new(
                format!("{name}_view"),
                DataType::ListView(item.clone()),
                true,
            ),
            Field::new(
                format!("{name}_fixed"),
                DataType::FixedSizeList(item, 2),
                true,
            ),
        ]
    };
    for item_name in ["$data$", "", "element", "2nd item"] {
        let schema = Schema::new(list("values", item_name, DataType::Int64).to_vec());
        let normalized = normalize_schema(&schema)
            .unwrap_or_else(|failure| panic!("{item_name:?} must be accepted: {failure}"));
        let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
        for column in &bigquery.fields {
            assert_eq!(column.r#type, "INTEGER");
            assert_eq!(column.mode, "REPEATED");
        }
    }

    let record = DataType::Struct(vec![Field::new("total-sales", DataType::Int64, true)].into());
    let failure =
        normalize_schema(&Schema::new(list("records", "$data$", record).to_vec())).unwrap_err();
    assert_failure(
        &failure,
        Status::InvalidArguments,
        &["\"total-sales\" contains \"-\""],
    );
}

/// An Arrow extension annotation describes its storage type, and Storage Write checks the ones it
/// knows against the storage it receives: `arrow.uuid` on the `Binary` a `FixedSizeBinary(16)`
/// normalizes to fails every append (verified live). So a field whose type normalization changes
/// loses its annotation, except the JSON and GEOGRAPHY ones that select its BigQuery type.
#[test]
fn normalization_drops_extensions_whose_storage_type_it_changed() {
    let extension = |field: Field, name: &str| {
        field.with_metadata(HashMap::from([
            ("ARROW:extension:name".to_owned(), name.to_owned()),
            ("ARROW:extension:metadata".to_owned(), String::new()),
        ]))
    };
    let uuid = extension(
        Field::new("id", DataType::FixedSizeBinary(16), true),
        "arrow.uuid",
    );
    let flag = extension(Field::new("flag", DataType::Int8, true), "arrow.bool8");
    let schema = Schema::new(vec![
        uuid.clone(),
        Field::new("record", DataType::Struct(vec![flag.clone()].into()), true),
        Field::new(
            "ids",
            DataType::List(Arc::new(uuid.clone().with_name("item"))),
            true,
        ),
        extension(
            Field::new("document", DataType::LargeUtf8, true),
            "arrow.json",
        ),
        extension(Field::new("kept", DataType::Int64, true), "example.custom"),
    ]);

    let normalized = normalize_schema(&schema).unwrap();
    let has_extension = |field: &Field| field.metadata().contains_key("ARROW:extension:name");
    assert!(!has_extension(normalized.field(0)));
    let DataType::Struct(children) = normalized.field(1).data_type() else {
        panic!("expected a struct")
    };
    assert!(!has_extension(&children[0]));
    let DataType::List(item) = normalized.field(2).data_type() else {
        panic!("expected a list")
    };
    assert!(!has_extension(item));
    assert!(has_extension(normalized.field(3)));
    assert!(has_extension(normalized.field(4)));

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    let types = bigquery
        .fields
        .iter()
        .map(|field| field.r#type.as_str())
        .collect::<Vec<_>>();
    assert_eq!(types, ["BYTES", "RECORD", "BYTES", "JSON", "INTEGER"]);

    // The batch takes the normalized schema, nested fields included.
    let ids = Arc::new(
        arrow_array::FixedSizeBinaryArray::try_from_iter([[7_u8; 16]].into_iter()).unwrap(),
    ) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            ids.clone(),
            Arc::new(StructArray::from(vec![(
                Arc::new(flag),
                Arc::new(arrow_array::Int8Array::from(vec![1])) as ArrayRef,
            )])),
            Arc::new(ListArray::new(
                Arc::new(uuid.with_name("item")),
                OffsetBuffer::from_lengths([1]),
                ids,
                None,
            )),
            Arc::new(arrow_array::LargeStringArray::from(vec!["{}"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let normalized = Arc::new(normalized);
    assert_eq!(
        normalize_batch(batch, normalized.clone()).unwrap().schema(),
        normalized
    );
}

/// `BIGQUERY:type` metadata decides the column type over an `arrow.json` or `geoarrow.wkt`
/// annotation, and Storage Write refuses an `arrow.json` field for the STRING column that
/// creates, so normalization drops an annotation the metadata overrides, nested ones included,
/// and an item's annotation that its list's `ARRAY<...>` element type overrides.
#[test]
fn normalization_drops_extensions_that_bigquery_type_metadata_overrides() {
    fn extension(field: &Field) -> Option<&str> {
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str)
    }
    let annotated = |name: &str, extension: &str, kind: Option<&str>| {
        let mut metadata = HashMap::from([
            ("ARROW:extension:name".to_owned(), extension.to_owned()),
            ("ARROW:extension:metadata".to_owned(), String::new()),
        ]);
        if let Some(kind) = kind {
            metadata.insert(BIGQUERY_TYPE_METADATA_KEY.to_owned(), kind.to_owned());
        }
        Field::new(name, DataType::Utf8, true).with_metadata(metadata)
    };
    let overridden = annotated("document", "arrow.json", Some("STRING"));
    let schema = Schema::new(vec![
        overridden.clone(),
        annotated("shape", "geoarrow.wkt", Some("JSON")),
        annotated("agreed", "arrow.json", Some("JSON")),
        annotated("plain", "geoarrow.wkt", None),
        Field::new(
            "nested",
            DataType::Struct(vec![overridden.clone()].into()),
            true,
        ),
        Field::new(
            "listed",
            DataType::List(Arc::new(overridden.with_name("item"))),
            true,
        ),
        bigquery_typed(
            Field::new(
                "elements",
                DataType::List(Arc::new(annotated("item", "arrow.json", None))),
                true,
            ),
            "ARRAY<STRING>",
        ),
    ]);

    let normalized = normalize_schema(&schema).unwrap();
    assert_eq!(extension(normalized.field(0)), None);
    assert_eq!(extension(normalized.field(1)), None);
    assert_eq!(extension(normalized.field(2)), Some("arrow.json"));
    assert_eq!(extension(normalized.field(3)), Some("geoarrow.wkt"));
    let DataType::Struct(children) = normalized.field(4).data_type() else {
        panic!("expected a struct")
    };
    assert_eq!(extension(&children[0]), None);
    for index in [5, 6] {
        let DataType::List(item) = normalized.field(index).data_type() else {
            panic!("expected a list")
        };
        assert_eq!(extension(item), None);
    }
    assert!(
        !normalized
            .field(0)
            .metadata()
            .contains_key("ARROW:extension:metadata"),
        "the annotation's metadata outlived its name"
    );

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    let types = bigquery
        .fields
        .iter()
        .map(|field| field.r#type.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        [
            "STRING",
            "JSON",
            "JSON",
            "GEOGRAPHY",
            "RECORD",
            "STRING",
            "STRING"
        ]
    );
}

/// BigQuery has no array of arrays, so a list of lists in any Arrow layout is refused with the
/// same remedy as a nested array parameter.
#[test]
fn rejects_lists_of_lists() {
    let lists = |element| {
        let item = Arc::new(Field::new("item", element, true));
        [
            DataType::List(item.clone()),
            DataType::LargeList(item.clone()),
            DataType::FixedSizeList(item, 1),
        ]
    };
    for inner in lists(DataType::Int64) {
        for outer in lists(inner) {
            let schema = Schema::new(vec![Field::new("matrix", outer, true)]);
            let failure =
                arrow_schema_to_bigquery(&normalize_schema(&schema).unwrap()).unwrap_err();
            assert_eq!(failure.status, Status::NotImplemented);
            assert_eq!(
                failure.message,
                "Arrow field \"matrix\" is a list of lists, but a BigQuery array cannot contain \
                 arrays directly; wrap each inner list in a STRUCT"
            );
        }
    }
}

#[test]
fn rejects_negative_fixed_size_list_schema_length() {
    let schema = Schema::new(vec![Field::new(
        "values",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), -1),
        true,
    )]);
    let failure = normalize_schema(&schema).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "Arrow fixed-size list ingest field \"values\" has length -1, which cannot be negative"
    );
}

/// A dictionary BigQuery cannot index names the column it was found on, like every other
/// per-field rejection in this module.
#[test]
fn rejects_dictionary_ingest_keys_that_are_not_integers() {
    let schema = Schema::new(vec![Field::new(
        "label",
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
        true,
    )]);
    let failure = normalize_schema(&schema).unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "Arrow dictionary ingest field \"label\" has key type Utf8, which is not an integer"
    );
}

/// Each column-name rejection names the rule, and the character, that failed. BigQuery tables
/// also take flexible column names, but Storage Write cannot write to a table with one, so the
/// rule is Storage Write's.
#[test]
fn a_rejected_column_name_reports_the_rule_it_broke() {
    let normalize =
        |name: &str| normalize_schema(&Schema::new(vec![Field::new(name, DataType::Int64, true)]));
    for (name, message) in [
        ("", "BigQuery column name cannot be empty"),
        (
            "2024_revenue",
            "BigQuery column name \"2024_revenue\" starts with \"2\", but Storage Write needs a \
             column name that starts with an ASCII letter or underscore",
        ),
        (
            "total-sales",
            "BigQuery column name \"total-sales\" contains \"-\", but Storage Write accepts only \
             ASCII letters, digits, and underscores in a column name",
        ),
        (
            "total sales",
            "BigQuery column name \"total sales\" contains \" \", but Storage Write accepts only \
             ASCII letters, digits, and underscores in a column name",
        ),
    ] {
        let failure = normalize(name).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments, "{name:?}");
        assert_eq!(failure.message, message, "{name:?}");
    }

    let too_long = "a".repeat(301);
    assert_eq!(
        normalize(&too_long).unwrap_err().message,
        format!(
            "BigQuery column name {too_long:?} is 301 characters long; a column name cannot \
             exceed 300"
        )
    );

    // An underscore may lead, digits may follow, and 300 characters still fit.
    for name in ["_private", "revenue_2024", &"a".repeat(300)] {
        normalize(name).unwrap_or_else(|failure| panic!("{name:?} must be accepted: {failure}"));
    }
}

#[test]
fn converts_interval_and_duration_ingest_schemas() {
    let repeated_intervals = bigquery_typed(
        Field::new(
            "intervals",
            DataType::new_list(DataType::Interval(IntervalUnit::MonthDayNano), true),
            true,
        ),
        "ARRAY<INTERVAL>",
    );
    let repeated_durations =
        ListArray::from_iter_primitive::<DurationSecondType, _, _>(vec![Some(vec![Some(1), None])]);
    let schema = Schema::new(vec![
        Field::new(
            "year_month",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
        Field::new("day_time", DataType::Interval(IntervalUnit::DayTime), true),
        Field::new(
            "month_day_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        repeated_intervals,
        Field::new("seconds", DataType::Duration(TimeUnit::Second), false),
        Field::new(
            "milliseconds",
            DataType::Duration(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "microseconds",
            DataType::Duration(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "nanoseconds",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "repeated_durations",
            repeated_durations.data_type().clone(),
            true,
        ),
    ]);
    let normalized = normalize_schema(&schema).unwrap();
    // Year-month intervals keep their Arrow type; day-time intervals and every duration
    // width become month-day-nano intervals, inside lists included.
    assert_eq!(
        normalized.field(0).data_type(),
        &DataType::Interval(IntervalUnit::YearMonth)
    );
    for index in [1, 2, 4, 5, 6, 7] {
        assert_eq!(
            normalized.field(index).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano),
            "{}",
            normalized.field(index).name()
        );
    }
    for index in [3, 8] {
        let DataType::List(item) = normalized.field(index).data_type() else {
            panic!(
                "expected a normalized list for {}",
                normalized.field(index).name()
            )
        };
        assert_eq!(
            item.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert!(
        bigquery
            .fields
            .iter()
            .all(|field| field.r#type == "INTERVAL")
    );
    assert_eq!(bigquery.fields[0].mode, "REQUIRED");
    assert_eq!(bigquery.fields[1].mode, "NULLABLE");
    assert_eq!(bigquery.fields[2].mode, "NULLABLE");
    assert_eq!(bigquery.fields[3].mode, "REPEATED");
    assert_eq!(bigquery.fields[4].mode, "REQUIRED");
    assert_eq!(bigquery.fields[8].mode, "REPEATED");
}

#[test]
fn converts_constrained_string_ingest_schema() {
    let constrained = |name: &str, data_type: DataType, max_length: &str| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
            max_length.to_owned(),
        )]))
    };
    let schema = Schema::new(vec![
        constrained("text", DataType::Utf8, "32"),
        constrained("data", DataType::Binary, "64"),
        constrained("tags", DataType::new_list(DataType::Utf8, true), "16"),
        Field::new(
            "record",
            DataType::Struct(vec![constrained("label", DataType::Utf8, "8")].into()),
            true,
        ),
    ]);
    let normalized = normalize_schema(&schema).unwrap();
    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(bigquery.fields[0].max_length, 32);
    assert_eq!(bigquery.fields[1].max_length, 64);
    assert_eq!(bigquery.fields[2].max_length, 16);
    assert_eq!(bigquery.fields[3].fields[0].max_length, 8);
}

#[test]
fn rejects_invalid_string_ingest_constraints() {
    let field = |name: &str, data_type: DataType, max_length: &str| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            BIGQUERY_FIELD_METADATA_MAX_LENGTH.to_owned(),
            max_length.to_owned(),
        )]))
    };
    for invalid in [
        field("zero", DataType::Utf8, "0"),
        field("negative", DataType::Binary, "-1"),
        field("not_a_number", DataType::Utf8, "many"),
        field("integer", DataType::Int64, "8"),
    ] {
        let schema = normalize_schema(&Schema::new(vec![invalid])).unwrap();
        assert_eq!(
            arrow_schema_to_bigquery(&schema).unwrap_err().status,
            Status::InvalidArguments
        );
    }
}

/// BigQuery column names are case-insensitive and Storage Write resolves Arrow fields by name, so
/// two siblings differing only in case would both land in one column of an existing table.
#[test]
fn rejects_sibling_columns_that_differ_only_in_case() {
    let failure = arrow_schema_to_bigquery(&Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ID", DataType::Int64, true),
    ]))
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidArguments);
    assert_eq!(
        failure.message,
        "BigQuery column names are case-insensitive, so the bound data cannot carry both \"id\" \
         and \"ID\"; rename one of them"
    );

    let nested = Field::new(
        "customer",
        DataType::Struct(Fields::from(vec![
            Field::new("Email", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ])),
        true,
    );
    assert_eq!(
        arrow_schema_to_bigquery(&Schema::new(vec![nested]))
            .unwrap_err()
            .message,
        "BigQuery column names are case-insensitive, so column \"customer\" cannot carry both \
         \"Email\" and \"email\"; rename one of them"
    );
}

/// An unsupported type names the field that carries it, and the all-null column pandas and
/// pyarrow infer as `Null` gets the one remedy that fixes it.
#[test]
fn an_unsupported_type_names_its_field() {
    // An array element is named by its list column, not by the item's own name.
    for (field, name) in [
        (Field::new("blank", DataType::Null, true), "blank"),
        (
            Field::new_list("blanks", Field::new("item", DataType::Null, true), true),
            "blanks",
        ),
    ] {
        // Normalization keeps the type, for an append's table to settle (see `ingest::align`).
        let normalized = normalize_schema(&Schema::new(vec![field])).unwrap();
        let failure = arrow_schema_to_bigquery(&normalized).unwrap_err();
        assert_eq!(failure.status, Status::NotImplemented);
        assert_eq!(
            failure.message,
            format!(
                "Arrow ingest field {name:?} has type Null, which no BigQuery column type \
                 matches; cast it to the column's intended type, or append it to a table that \
                 has the column"
            )
        );
    }

    let failure = normalize_schema(&Schema::new(vec![Field::new(
        "lookup",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        ),
        true,
    )]))
    .unwrap_err();
    assert_eq!(failure.status, Status::NotImplemented);
    assert_eq!(
        failure.message,
        "Arrow ingest field \"lookup\" is a map, which BigQuery has no type for; ingest it as a \
         list of key and value structs, ARRAY<STRUCT<key, value>>"
    );
}

/// BigQuery refuses a RECORD with no fields, so an empty Arrow struct is refused while the
/// schema is converted, where a dry run sees it too, rather than by `tables.insert`.
#[test]
fn an_empty_struct_is_refused_by_its_column() {
    let empty = DataType::Struct(Fields::empty());
    for (field, owner) in [
        (Field::new("blank", empty.clone(), true), "blank"),
        (
            Field::new_list("blanks", Field::new("item", empty.clone(), true), true),
            "blanks",
        ),
        (
            Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![Field::new("inner", empty, true)])),
                true,
            ),
            "inner",
        ),
    ] {
        let failure = arrow_schema_to_bigquery(&Schema::new(vec![field])).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            format!(
                "column {owner:?} has no fields; a BigQuery table or STRUCT needs at least one"
            )
        );
    }
}
