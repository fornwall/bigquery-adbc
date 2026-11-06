//! Decimal ingest types: the value domain each Arrow width normalizes to, and the explicit
//! annotations that survive it.

use super::*;

#[test]
fn normalizes_every_arrow_decimal_width_by_value_domain() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal32Array::from(vec![123])
                .with_precision_and_scale(3, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![-12])
                .with_precision_and_scale(2, -3)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![123])
                .with_precision_and_scale(38, 20)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![i256::from_i128(-123)])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Field::new(
                    format!("decimal_{index}"),
                    column.data_type().clone(),
                    false,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
    assert_eq!(
        normalized_schema
            .fields()
            .iter()
            .map(|field| field.data_type())
            .collect::<Vec<_>>(),
        [
            &DataType::Decimal128(3, 2),
            &DataType::Decimal128(5, 0),
            &DataType::Decimal256(38, 20),
            &DataType::Decimal128(38, 9),
        ]
    );

    let bigquery = arrow_schema_to_bigquery(&normalized_schema).unwrap();
    assert_eq!(
        bigquery
            .fields
            .iter()
            .map(|field| (field.r#type.as_str(), field.precision, field.scale))
            .collect::<Vec<_>>(),
        [
            ("NUMERIC", 3, 2),
            ("NUMERIC", 5, 0),
            ("BIGNUMERIC", 38, 20),
            ("NUMERIC", 38, 9),
        ]
    );

    let normalized = normalize_batch(
        RecordBatch::try_new(schema, columns).unwrap(),
        normalized_schema,
    )
    .unwrap();
    assert_eq!(column_as::<Decimal128Array>(&normalized, 0).value(0), 123);
    assert_eq!(
        column_as::<Decimal128Array>(&normalized, 1).value(0),
        -12_000
    );
    assert_eq!(
        column_as::<Decimal256Array>(&normalized, 2).value(0),
        i256::from_i128(123)
    );
    assert_eq!(column_as::<Decimal128Array>(&normalized, 3).value(0), -123);
}

#[test]
fn preserves_explicit_decimal_ingest_types() {
    let scalar = bigquery_typed(
        Field::new("scalar", DataType::Decimal256(38, 9), false),
        "BIGNUMERIC",
    );
    let repeated = bigquery_typed(
        Field::new(
            "repeated",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal256(38, 9),
                true,
            ))),
            false,
        ),
        "ARRAY<BIGNUMERIC>",
    );
    let normalized = normalize_schema(&Schema::new(vec![scalar, repeated])).unwrap();

    assert_eq!(
        normalized.field(0).data_type(),
        &DataType::Decimal256(38, 9)
    );
    let DataType::List(item) = normalized.field(1).data_type() else {
        panic!("expected a normalized decimal list")
    };
    assert_eq!(item.data_type(), &DataType::Decimal256(38, 9));
    assert_eq!(
        item.metadata()
            .get(BIGQUERY_TYPE_METADATA_KEY)
            .map(String::as_str),
        Some("BIGNUMERIC")
    );

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(bigquery.fields[0].r#type, "BIGNUMERIC");
    assert_eq!(bigquery.fields[1].r#type, "BIGNUMERIC");

    let incompatible = Schema::new(vec![bigquery_typed(
        Field::new("value", DataType::Decimal128(38, 20), false),
        "NUMERIC",
    )]);
    assert_eq!(
        normalize_schema(&incompatible).unwrap_err().status,
        Status::InvalidArguments
    );
}

/// The canonical BIGNUMERIC Arrow shape creates a parameter-free column, and a narrower
/// `Decimal256` still declares its parameters.
///
/// `BIGNUMERIC(76, 38)` stops just short of 1e38, while the `Decimal256(76, 38)` a plain
/// BIGNUMERIC reads into spans the whole `i256` domain (~5.79e38), so declaring the parameters
/// would refuse values the same query returned.
#[test]
fn the_widest_bignumeric_ingests_back_as_a_parameter_free_column() {
    let normalized = normalize_schema(&Schema::new(vec![
        Field::new("widest", DataType::Decimal256(76, 38), false),
        Field::new(
            "widest_array",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal256(76, 38),
                true,
            ))),
            true,
        ),
        Field::new("narrower", DataType::Decimal256(75, 38), true),
    ]))
    .unwrap();

    let bigquery = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(
        bigquery
            .fields
            .iter()
            .map(|field| (field.r#type.as_str(), field.precision, field.scale))
            .collect::<Vec<_>>(),
        [
            ("BIGNUMERIC", 0, 0),
            ("BIGNUMERIC", 0, 0),
            ("BIGNUMERIC", 75, 38),
        ]
    );
    assert_eq!(
        create_or_replace_table_ddl("`p.d.t`", &bigquery).unwrap(),
        "CREATE OR REPLACE TABLE `p.d.t` (`widest` BIGNUMERIC NOT NULL, \
         `widest_array` ARRAY<BIGNUMERIC>, `narrower` BIGNUMERIC(75, 38))"
    );

    // Replace must recreate a parameterized target to preserve the full input domain.
    let column = |precision, scale| {
        table_schema([TableFieldSchema {
            precision,
            scale,
            ..field_mode("widest", "BIGNUMERIC", "REQUIRED")
        }])
    };
    let expected = table_schema([bigquery.fields[0].clone()]);
    assert!(schemas_match(&column(0, 0), &expected));
    assert!(!schemas_match(&column(76, 38), &expected));
    assert!(!schemas_match(&column(75, 38), &expected));
}

/// Storage Write reads a REPEATED NUMERIC or BIGNUMERIC element at the type's full scale (UP-89),
/// so array elements are sent rescaled to it, while the table keeps the declared parameters and
/// scalar and STRUCT decimals keep their declared scale.
#[test]
fn array_element_decimals_are_written_at_their_type_s_full_scale() {
    let list_of = |values: ArrayRef| -> ArrayRef {
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        Arc::new(ListArray::new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            values,
            None,
        ))
    };
    let numeric = list_of(Arc::new(
        Decimal128Array::from(vec![1, -9_999_999_999])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ));
    let bignumeric = list_of(Arc::new(
        Decimal256Array::from(vec![i256::ONE, i256::MINUS_ONE])
            .with_precision_and_scale(48, 10)
            .unwrap(),
    ));
    let nested_values = list_of(Arc::new(
        Decimal32Array::from(vec![1, 2])
            .with_precision_and_scale(9, 2)
            .unwrap(),
    ));
    let nested = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("xs", nested_values.data_type().clone(), true),
            Field::new("x", DataType::Decimal128(10, 2), true),
        ]),
        vec![
            nested_values,
            Arc::new(
                Decimal128Array::from(vec![1])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
        None,
    )) as ArrayRef;
    let scalar = Arc::new(
        Decimal128Array::from(vec![1])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ) as ArrayRef;
    let batch = RecordBatch::try_from_iter([
        ("numeric", numeric),
        ("bignumeric", bignumeric),
        ("nested", nested),
        ("scalar", scalar),
    ])
    .unwrap();

    let normalized = normalize_schema(batch.schema_ref()).unwrap();
    let table = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(
        create_or_replace_table_ddl("`p.d.t`", &table).unwrap(),
        "CREATE OR REPLACE TABLE `p.d.t` (`numeric` ARRAY<NUMERIC(10, 2)>, \
         `bignumeric` ARRAY<BIGNUMERIC(48, 10)>, \
         `nested` STRUCT<`xs` ARRAY<NUMERIC(9, 2)>, `x` NUMERIC(10, 2)> NOT NULL, \
         `scalar` NUMERIC(10, 2) NOT NULL)"
    );

    let written = normalize_batch(batch, Arc::new(storage_write_schema(&normalized))).unwrap();
    let elements = |column: &ArrayRef| array_as::<ListArray>(column).values().clone();
    let numeric = elements(written.column(0));
    assert_eq!(numeric.data_type(), &DataType::Decimal128(38, 9));
    assert_eq!(
        array_as::<Decimal128Array>(&numeric).values(),
        &[10_000_000, -99_999_999_990_000_000]
    );
    let bignumeric = elements(written.column(1));
    assert_eq!(bignumeric.data_type(), &DataType::Decimal256(76, 38));
    let ten_to_the_28 = i256::from_i128(10_i128.pow(28));
    assert_eq!(
        array_as::<Decimal256Array>(&bignumeric).values(),
        &[ten_to_the_28, ten_to_the_28.wrapping_neg()]
    );
    let nested = array_as::<StructArray>(written.column(2));
    let nested_elements = elements(nested.column(0));
    assert_eq!(
        array_as::<Decimal128Array>(&nested_elements).values(),
        &[10_000_000, 20_000_000]
    );
    assert_eq!(nested.column(1).data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(written.column(3).data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(array_as::<Decimal128Array>(written.column(3)).value(0), 1);
}

/// A field declaring `(precision, scale)` in `BIGQUERY:precision` and `BIGQUERY:scale`, as
/// `GetTableSchema` reports a parameterized decimal column.
fn declaring(field: Field, precision: &str, scale: &str) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        crate::BIGQUERY_FIELD_METADATA_PRECISION.to_owned(),
        precision.to_owned(),
    );
    metadata.insert(
        crate::BIGQUERY_FIELD_METADATA_SCALE.to_owned(),
        scale.to_owned(),
    );
    field.with_metadata(metadata)
}

/// `GetTableSchema` reports a `NUMERIC(10, 2)` column as NUMERIC's full type with its declared
/// shape in metadata, so ingesting that schema creates the same column, and its values are
/// narrowed to it exactly, never rounded.
#[test]
fn a_declared_decimal_shape_survives_a_read_and_an_ingest_back() {
    let numeric = declaring(
        bigquery_typed(
            Field::new("amount", DataType::Decimal128(38, 9), true),
            "NUMERIC",
        ),
        "10",
        "2",
    );
    // A BIGNUMERIC whose shape NUMERIC could hold keeps its family.
    let bignumeric = declaring(
        bigquery_typed(
            Field::new("big", DataType::Decimal256(76, 38), true),
            "BIGNUMERIC",
        ),
        "10",
        "2",
    );
    // An array declares its elements' shape on the array column.
    let amounts = declaring(
        bigquery_typed(
            Field::new_list(
                "amounts",
                Field::new("item", DataType::Decimal128(38, 9), true),
                true,
            ),
            "ARRAY<NUMERIC>",
        ),
        "10",
        "2",
    );
    let schema = Arc::new(Schema::new(vec![numeric, bignumeric, amounts]));
    let normalized = normalize_schema(&schema).unwrap();
    assert_eq!(
        normalized.field(0).data_type(),
        &DataType::Decimal128(10, 2)
    );
    assert_eq!(
        normalized.field(1).data_type(),
        &DataType::Decimal256(10, 2)
    );
    let table = arrow_schema_to_bigquery(&normalized).unwrap();
    assert_eq!(
        table_schema_shapes(&table),
        [
            ("NUMERIC", "NULLABLE", 10, 2),
            ("BIGNUMERIC", "NULLABLE", 10, 2),
            ("NUMERIC", "REPEATED", 10, 2),
        ]
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(1_230_000_000)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![Some(
                    i256::from_i128(45) * i256::from_i128(10_i128.pow(36)),
                )])
                .with_precision_and_scale(76, 38)
                .unwrap(),
            ),
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Decimal128(38, 9), true)),
                OffsetBuffer::from_lengths([1]),
                Arc::new(
                    Decimal128Array::from(vec![Some(12_340_000_000)])
                        .with_precision_and_scale(38, 9)
                        .unwrap(),
                ),
                None,
            )),
        ],
    )
    .unwrap();
    // Each value has at most the two fractional digits its column declares.
    let wire = Arc::new(storage_write_schema(&normalized));
    let written = normalize_batch(batch.clone(), wire.clone()).unwrap();
    assert_eq!(column_as::<Decimal128Array>(&written, 0).value(0), 123);
    assert_eq!(
        column_as::<Decimal256Array>(&written, 1).value(0),
        i256::from_i128(45)
    );
    // An element travels at NUMERIC's full scale, where Storage Write reads it.
    let elements = column_as::<ListArray>(&written, 2).value(0);
    assert_eq!(
        array_as::<Decimal128Array>(&elements).value(0),
        12_340_000_000
    );

    let mut too_precise = batch.columns().to_vec();
    too_precise[0] = Arc::new(
        Decimal128Array::from(vec![Some(1_234_000_000)])
            .with_precision_and_scale(38, 9)
            .unwrap(),
    );
    let failure = normalize_batch(
        RecordBatch::try_new(batch.schema(), too_precise).unwrap(),
        wire,
    )
    .unwrap_err();
    assert_eq!(failure.status, Status::InvalidData);
    assert_eq!(
        failure.message,
        "Arrow decimal ingest field \"amount\" holds a value at row 0 with more than 2 \
         fractional digits, which its column's NUMERIC(10, 2) cannot store without rounding"
    );
}

/// A declared shape the column type cannot have is refused by its field.
#[test]
fn an_invalid_declared_decimal_shape_names_its_field() {
    for (precision, scale, reason) in [
        ("ten", "2", "both must be integers"),
        ("80", "2", "precision 80 and scale 2 do not fit BIGNUMERIC"),
    ] {
        let field = declaring(
            Field::new("amount", DataType::Decimal128(38, 9), true),
            precision,
            scale,
        );
        let failure = normalize_schema(&Schema::new(vec![field])).unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            format!(
                "Arrow ingest field \"amount\" has invalid BIGQUERY:precision and BIGQUERY:scale \
                 metadata: {reason}"
            )
        );
    }
}

#[test]
fn declared_decimal_shapes_accept_zero_at_extreme_source_scales() {
    for value in [0, 1, -1] {
        let column: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(value), None])
                .with_precision_and_scale(1, i8::MIN)
                .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![declaring(
            Field::new("amount", column.data_type().clone(), true),
            "10",
            "2",
        )]));
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        let result = normalize_batch(
            RecordBatch::try_new(schema, vec![column]).unwrap(),
            normalized_schema,
        );
        if value == 0 {
            let batch = result.unwrap();
            let column = column_as::<Decimal128Array>(&batch, 0);
            assert_eq!(column.data_type(), &DataType::Decimal128(10, 2));
            assert_eq!(column.iter().collect::<Vec<_>>(), [Some(0), None]);
        } else {
            assert_eq!(result.unwrap_err().status, Status::InvalidData);
        }
    }
}

#[test]
fn declared_decimal_shapes_do_not_bypass_source_precision() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal32Array::from(vec![Some(0), None, Some(10)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![Some(0), None, Some(10)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(0), None, Some(10)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![Some(i256::ZERO), None, Some(i256::from_i128(10))])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
    ];
    for column in columns {
        let schema = Arc::new(Schema::new(vec![declaring(
            Field::new("amount", column.data_type().clone(), true),
            "2",
            "0",
        )]));
        let normalized_schema = Arc::new(normalize_schema(&schema).unwrap());
        let failure = normalize_batch(
            RecordBatch::try_new(schema, vec![column]).unwrap(),
            normalized_schema,
        )
        .unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            "Arrow decimal ingest field \"amount\" contains a value outside its declared precision 1 at row 2"
        );
    }
}

/// Each column's type, mode and declared precision and scale.
fn table_schema_shapes(table: &TableSchema) -> Vec<(&str, &str, i64, i64)> {
    table
        .fields
        .iter()
        .map(|field| {
            (
                field.r#type.as_str(),
                field.mode.as_str(),
                field.precision,
                field.scale,
            )
        })
        .collect()
}
