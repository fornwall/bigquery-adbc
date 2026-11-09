use super::*;
use crate::test_support::arrow::bigquery_typed;
use crate::test_support::errors::assert_failure;

/// The exact metadata strings are a wire-format contract with consumers; a typo in a hoisted
/// const must fail here rather than pass every test that uses the const on both sides.
#[test]
fn metadata_contract_strings_are_pinned() {
    assert_eq!(BIGQUERY_TYPE_METADATA_KEY, "BIGQUERY:type");
    assert_eq!(ARROW_EXTENSION_NAME_KEY, "ARROW:extension:name");
    assert_eq!(ARROW_EXTENSION_METADATA_KEY, "ARROW:extension:metadata");

    let extension = |kind| {
        let mut metadata = Metadata::new();
        insert_extension_metadata(kind, &mut metadata);
        metadata
    };
    let geography = extension(BigQueryType::Geography);
    assert_eq!(
        geography.get("ARROW:extension:name").map(String::as_str),
        Some("geoarrow.wkt")
    );
    assert_eq!(
        geography
            .get("ARROW:extension:metadata")
            .map(String::as_str),
        Some(r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#)
    );
    let json = extension(BigQueryType::Json);
    assert_eq!(
        json.get("ARROW:extension:name").map(String::as_str),
        Some("arrow.json")
    );
    assert_eq!(json.len(), 1);
    assert!(extension(BigQueryType::Int64).is_empty());
}

#[test]
fn extension_names_imply_their_bigquery_types() {
    let string_field = |extension: Option<&str>| {
        let mut metadata = Metadata::new();
        if let Some(extension) = extension {
            metadata.insert("ARROW:extension:name".to_owned(), extension.to_owned());
        }
        Field::new("value", DataType::Utf8, true).with_metadata(metadata)
    };
    for (extension, kind) in [
        (Some("arrow.json"), Some(BigQueryType::Json)),
        (Some("geoarrow.wkt"), Some(BigQueryType::Geography)),
        (Some("other.extension"), None),
        (None, None),
    ] {
        assert_eq!(
            extension_type(&string_field(extension)),
            kind,
            "{extension:?}"
        );
    }
}

#[test]
fn range_metadata_names_its_element_type() {
    use arrow_schema::{DataType, Fields};

    let field = |metadata: Option<&str>| {
        let mut map = Metadata::new();
        if let Some(value) = metadata {
            map.insert(BIGQUERY_TYPE_METADATA_KEY.to_owned(), value.to_owned());
        }
        Field::new("period", DataType::Struct(Fields::empty()), true).with_metadata(map)
    };

    for (metadata, element) in [
        ("RANGE<DATE>", RangeElement::Date),
        ("RANGE<DATETIME>", RangeElement::DateTime),
        ("RANGE<TIMESTAMP>", RangeElement::Timestamp),
    ] {
        assert_eq!(
            range_element_from_metadata(&field(Some(metadata)), "test field").unwrap(),
            Some(element)
        );
    }
    assert_eq!(
        range_element_from_metadata(&field(None), "test field").unwrap(),
        None
    );
    assert_eq!(
        range_element_from_metadata(&field(Some("STRUCT<a INT64>")), "test field").unwrap(),
        None
    );
    // Malformed RANGE metadata and unsupported element types are errors, and an unsupported
    // element is reported as an element-type problem in both conversion directions.
    for metadata in ["RANGE<DATE", "RANGE", "RANGE<INT64>", "RANGE<date>"] {
        let failure =
            range_element_from_metadata(&field(Some(metadata)), "test field").unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments, "{metadata}");
    }
    assert_eq!(RangeElement::Date.sql(), "DATE");
    assert_eq!(RangeElement::DateTime.rest(), "DATETIME");
    assert_eq!(RangeElement::Timestamp.sql(), "TIMESTAMP");
}

/// Parameters and ingest share one RANGE shape rule: exactly `start` and `end`, each of an Arrow
/// type that maps to the element type, in any unit or zone the value conversion accepts.
#[test]
fn range_fields_need_start_and_end_bounds_of_the_element_type() {
    use crate::test_support::arrow::range_bounds;

    let range = |rich_type: &str, fields: Fields| {
        let field = bigquery_typed(
            Field::new("period", DataType::Struct(fields.clone()), true),
            rich_type,
        );
        range_field_element(&field, &fields, INGEST_FIELD)
    };
    for (rich_type, bound, element) in [
        ("RANGE<DATE>", DataType::Date64, RangeElement::Date),
        (
            "RANGE<DATETIME>",
            DataType::Timestamp(TimeUnit::Second, Some("".into())),
            RangeElement::DateTime,
        ),
        (
            "RANGE<TIMESTAMP>",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into())),
            RangeElement::Timestamp,
        ),
    ] {
        assert_eq!(
            range(rich_type, range_bounds(bound)).unwrap(),
            Some(element)
        );
    }
    assert_eq!(range("STRUCT<a INT64>", Fields::empty()).unwrap(), None);

    let reversed: Fields = vec![
        Field::new("end", DataType::Date32, true),
        Field::new("start", DataType::Date32, true),
    ]
    .into();
    for fields in [
        range_bounds(DataType::Timestamp(TimeUnit::Microsecond, None)),
        range_bounds(DataType::Utf8),
        reversed,
        Fields::empty(),
    ] {
        let failure = range("RANGE<TIMESTAMP>", fields).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            "BigQuery RANGE<TIMESTAMP> bulk ingest field \"period\" must be an Arrow struct of \
             exactly two fields, start and end, each of an Arrow TIMESTAMP type such as \
             Timestamp(µs, \"UTC\")"
        );
    }
}

#[test]
fn every_spelling_round_trips_to_its_own_type() {
    for kind in BigQueryType::ALL.iter().copied() {
        assert_eq!(BigQueryType::from_sql(kind.sql()), Some(kind));
        assert_eq!(BigQueryType::from_rest(kind.rest()), Some(kind));
        assert_eq!(BigQueryType::from_rest(kind.sql()), Some(kind));
        for alias in kind.rest_aliases() {
            assert_eq!(BigQueryType::from_rest(alias), Some(kind));
        }
    }
}

#[test]
fn rest_only_spellings_are_not_googlesql_and_name_their_replacement() {
    for (rest, googlesql) in [
        ("INTEGER", "INT64"),
        ("FLOAT", "FLOAT64"),
        ("BOOLEAN", "BOOL"),
        ("RECORD", "STRUCT"),
        ("DECIMAL", "NUMERIC"),
        ("BIGDECIMAL", "BIGNUMERIC"),
    ] {
        assert_eq!(
            BigQueryType::from_sql(rest),
            None,
            "{rest} is not GoogleSQL"
        );
        assert_eq!(googlesql_spelling_of(rest), Some(googlesql));
    }
    assert_eq!(BigQueryType::from_sql("float64"), None);
    assert_eq!(BigQueryType::from_sql("ARRAY<INT64>"), None);
    for name in ["FLOAT64", "STRING", "NOT_A_TYPE"] {
        assert_eq!(googlesql_spelling_of(name), None, "{name}");
    }
}

#[test]
fn string_typed_metadata_annotates_json_and_geography() {
    assert!(BigQueryType::Json.annotates(BigQueryType::String));
    assert!(BigQueryType::Geography.annotates(BigQueryType::String));
    assert!(BigQueryType::Float64.annotates(BigQueryType::Float64));
    assert!(!BigQueryType::String.annotates(BigQueryType::Json));
    assert!(!BigQueryType::Json.annotates(BigQueryType::Int64));
    assert!(!BigQueryType::Float64.annotates(BigQueryType::Int64));
}

/// A name the driver has no type for is unimplemented; a known type this field cannot be is a
/// mismatch. A bare `STRUCT` or `RANGE` on a scalar Arrow field is the latter.
#[test]
fn composite_metadata_on_a_scalar_field_is_a_mismatch_not_a_missing_type() {
    let annotated =
        |rich_type: &str| bigquery_typed(Field::new("value", DataType::Int64, true), rich_type);

    for rich_type in ["STRUCT", "RANGE"] {
        let field = annotated(rich_type);
        let failure = metadata_type(
            &field,
            rich_type,
            Some(BigQueryType::Int64),
            PARAMETER_FIELD,
        )
        .unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            format!(
                "BigQuery type metadata {rich_type:?} is incompatible with Arrow field \"value\" \
                 of type Int64"
            )
        );
    }

    let field = annotated("NOT_A_TYPE");
    let failure = metadata_type(
        &field,
        "NOT_A_TYPE",
        Some(BigQueryType::Int64),
        PARAMETER_FIELD,
    )
    .unwrap_err();
    assert_failure(&failure, Status::NotImplemented, &["is not implemented"]);
}

/// Ingest normalization and query result decoding must reach the same Arrow layout for a type.
#[test]
fn the_canonical_scalar_table_answers_for_every_physical_scalar_type() {
    for (data_type, expected) in [
        (DataType::Utf8View, DataType::Utf8),
        (DataType::LargeBinary, DataType::Binary),
        (DataType::UInt32, DataType::Int64),
        (DataType::Float16, DataType::Float64),
        (DataType::Date64, DataType::Date32),
        (
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
        ),
        (
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        // An empty timezone is DATETIME, as on the read path.
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ),
        (DataType::Boolean, DataType::Boolean),
    ] {
        assert_eq!(
            canonical_scalar_data_type(&data_type),
            Some(expected),
            "{data_type}"
        );
    }

    // Types whose mapping needs more than the physical type are `None`, left for ingest to
    // decide: decimals (precision and scale), durations (no BigQuery type) and composites. So is
    // a time unit Arrow does not define for its width, which no value of the type can carry.
    for data_type in [
        DataType::Decimal128(38, 9),
        DataType::Duration(TimeUnit::Second),
        DataType::Struct(Fields::empty()),
        DataType::Null,
        DataType::Time32(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Millisecond),
    ] {
        assert_eq!(canonical_scalar_data_type(&data_type), None, "{data_type}");
    }
}

/// Ingest and parameter binding share the decimal-metadata check and differ only in the subject
/// they name.
#[test]
fn decimal_metadata_rejections_name_their_input_surface() {
    let field = bigquery_typed(
        Field::new("amount", DataType::Decimal128(38, 9), true),
        "STRING",
    );
    for (subject, expected) in [
        (INGEST_FIELD, "Arrow bulk ingest field \"amount\""),
        (PARAMETER_FIELD, "Arrow query parameter field \"amount\""),
    ] {
        let failure =
            decimal_metadata_type_hint(field.data_type(), &field, None, subject).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert!(
            failure
                .message
                .starts_with(&format!("{expected} has invalid decimal metadata: ")),
            "{}",
            failure.message
        );
    }

    let numeric = Field::new("amount", DataType::Decimal128(38, 9), true);
    assert_eq!(
        decimal_metadata_type_hint(numeric.data_type(), &numeric, None, INGEST_FIELD).unwrap(),
        None
    );
}

#[test]
fn canonical_arrow_types_cover_every_scalar_type_and_range_element() {
    for kind in BigQueryType::ALL {
        assert_eq!(
            kind.canonical_data_type().is_some(),
            kind.is_scalar(),
            "{kind:?}"
        );
    }
    for element in [
        RangeElement::Date,
        RangeElement::DateTime,
        RangeElement::Timestamp,
    ] {
        // `canonical_arrow_type`'s `expect` holds: every range element is a scalar type.
        assert!(element.bigquery_type().is_scalar(), "{element:?}");
        assert_eq!(RangeElement::from_rest(element.rest()), Some(element));
        assert_eq!(RangeElement::from_rest(element.sql()), Some(element));
    }
    assert_eq!(RangeElement::from_rest("STRING"), None);
    assert_eq!(RangeElement::from_rest("not a type"), None);
}

/// Zoned means TIMESTAMP, otherwise DATETIME, and the REST decoder reads epoch microseconds or a
/// civil datetime accordingly; a disagreement would silently misread a whole column. An empty
/// timezone is unzoned (see also the canonical scalar table above).
#[test]
fn an_empty_arrow_timezone_is_unzoned() {
    assert!(!timestamp_is_zoned(None));
    assert!(!timestamp_is_zoned(Some("")));
    assert!(timestamp_is_zoned(Some("UTC")));
    assert!(timestamp_is_zoned(Some("+02:00")));
}

/// The location chain renders the root, then each element and field descended into, in order.
#[test]
fn a_value_location_renders_its_root_and_every_step_below_it() {
    let root = ValueAt::Root(&"bound BigQuery parameter 0 \"orders\" at row 7");
    assert_eq!(
        root.to_string(),
        "bound BigQuery parameter 0 \"orders\" at row 7"
    );
    let element = root.element(3);
    let member = element.member("shipment");
    let leaf = member.member("delivered_on");
    assert_eq!(
        leaf.to_string(),
        "bound BigQuery parameter 0 \"orders\" at row 7, element 3, field \"shipment\", field \
         \"delivered_on\""
    );
}

#[test]
fn case_duplicates_are_reported_in_scan_order() {
    let duplicate = |names: &[&str]| {
        let fields: Fields = names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect();
        case_duplicate(&fields).map(|(first, second)| (first.0, second.0))
    };
    assert_eq!(duplicate(&["a", "b", "c"]), None);
    // BigQuery folds by full Unicode uppercase mapping, which these live probes pin.
    for (left, right, collide) in [
        ("é", "É", true),
        ("ß", "SS", true),
        ("ı", "I", true),
        ("ß", "ẞ", false),
        ("k", "\u{212a}", false),
        ("i", "İ", false),
    ] {
        assert_eq!(
            duplicate(&[left, right]),
            collide.then_some((0, 1)),
            "{left:?} and {right:?}"
        );
    }
    assert_eq!(duplicate(&["id", "x", "ID"]), Some((0, 2)));
    // Two pairs: the one a left-to-right scan completes first, not the alphabetically first.
    assert_eq!(duplicate(&["b", "a", "B", "A"]), Some((0, 2)));
    assert_eq!(duplicate(&["a", "b", "B", "A"]), Some((1, 2)));
    // A third spelling does not displace the two earliest.
    assert_eq!(duplicate(&["x", "X", "x"]), Some((0, 1)));
}
