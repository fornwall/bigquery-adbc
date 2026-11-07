use arrow_schema::DataType;

use super::{
    BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE, BigQueryDecimalType, bigquery_decimal_data_type,
    decimal_mapping, decimal_type_hint,
};

/// A parameter-free BIGNUMERIC is the only mapping whose declared parameters would narrow the
/// BigQuery column below the Arrow values it carries, so it is the only one ingest declares
/// without them.
#[test]
fn only_the_widest_bignumeric_mapping_narrows_its_arrow_domain() {
    let mapping = |data_type| decimal_mapping(&data_type, None).unwrap();
    assert!(
        mapping(DataType::Decimal256(BIGNUMERIC_PRECISION, BIGNUMERIC_SCALE))
            .narrows_its_arrow_domain()
    );
    for data_type in [
        DataType::Decimal256(75, BIGNUMERIC_SCALE),
        DataType::Decimal256(38, 0),
        // Plain NUMERIC has no such gap: `Decimal128(38, 9)` is exactly its domain.
        DataType::Decimal128(38, 9),
    ] {
        assert!(
            !mapping(data_type.clone()).narrows_its_arrow_domain(),
            "{data_type}"
        );
    }
}

#[test]
fn decimal_metadata_uses_the_googlesql_names_the_driver_emits() {
    let decimal = DataType::Decimal128(38, 9);
    assert_eq!(
        decimal_type_hint(&decimal, Some("NUMERIC"), None).unwrap(),
        Some(BigQueryDecimalType::Numeric)
    );

    let alias = decimal_type_hint(&decimal, Some("DECIMAL"), None).unwrap_err();
    assert!(alias.contains("NUMERIC"), "{alias}");
    assert!(alias.contains("GoogleSQL"), "{alias}");

    let array = DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
        "item",
        DataType::Decimal256(76, 38),
        true,
    )));
    let array_alias = decimal_type_hint(&array, Some("ARRAY<BIGDECIMAL>"), None).unwrap_err();
    assert!(array_alias.contains("ARRAY<BIGNUMERIC>"), "{array_alias}");

    let unrelated = decimal_type_hint(&decimal, Some("NOT_A_TYPE"), None).unwrap_err();
    assert!(unrelated.contains("incompatible"), "{unrelated}");
}

#[test]
fn classifies_arrow_decimals_by_precision_and_scale() {
    for (data_type, kind, storage_type) in [
        (
            DataType::Decimal32(9, 2),
            BigQueryDecimalType::Numeric,
            DataType::Decimal128(9, 2),
        ),
        (
            DataType::Decimal64(18, 0),
            BigQueryDecimalType::Numeric,
            DataType::Decimal128(18, 0),
        ),
        (
            DataType::Decimal128(38, 0),
            BigQueryDecimalType::BigNumeric,
            DataType::Decimal256(38, 0),
        ),
        (
            DataType::Decimal128(38, 20),
            BigQueryDecimalType::BigNumeric,
            DataType::Decimal256(38, 20),
        ),
        (
            DataType::Decimal256(38, 9),
            BigQueryDecimalType::Numeric,
            DataType::Decimal128(38, 9),
        ),
        (
            DataType::Decimal64(18, -9),
            BigQueryDecimalType::Numeric,
            DataType::Decimal128(27, 0),
        ),
    ] {
        let mapping = decimal_mapping(&data_type, None).unwrap();
        assert_eq!(mapping.bigquery_type, kind);
        assert_eq!(mapping.storage_data_type(), storage_type);
    }
}

#[test]
fn rejects_unrepresentable_and_malformed_arrow_decimals() {
    for data_type in [
        DataType::Decimal128(0, 0),
        DataType::Decimal32(10, 0),
        DataType::Decimal64(18, 19),
        DataType::Decimal256(76, 37),
        DataType::Decimal128(38, -1),
        // The widest negative scale an `i8` holds: the implied trailing zeroes move into the
        // unscaled integer, and 76 + 128 digits fit no BigQuery decimal.
        DataType::Decimal256(76, -128),
    ] {
        assert!(
            decimal_mapping(&data_type, None)
                .unwrap_err()
                .contains("Arrow decimal")
        );
    }
}

#[test]
fn honors_compatible_explicit_bigquery_type() {
    let mapping = decimal_mapping(
        &DataType::Decimal256(38, 9),
        Some(BigQueryDecimalType::BigNumeric),
    )
    .unwrap();
    assert_eq!(mapping.bigquery_type, BigQueryDecimalType::BigNumeric);
    assert_eq!(mapping.storage_data_type(), DataType::Decimal256(38, 9));

    assert!(
        decimal_mapping(
            &DataType::Decimal128(38, 20),
            Some(BigQueryDecimalType::Numeric),
        )
        .unwrap_err()
        .contains("requested BigQuery NUMERIC")
    );
}

#[test]
fn validates_bigquery_decimal_shapes_including_all_fractional_values() {
    assert_eq!(
        bigquery_decimal_data_type(BigQueryDecimalType::Numeric, 9, 9).unwrap(),
        DataType::Decimal128(9, 9)
    );
    assert_eq!(
        bigquery_decimal_data_type(BigQueryDecimalType::BigNumeric, 38, 38).unwrap(),
        DataType::Decimal256(38, 38)
    );
    assert!(bigquery_decimal_data_type(BigQueryDecimalType::Numeric, 31, 1).is_err());
    assert!(bigquery_decimal_data_type(BigQueryDecimalType::BigNumeric, 76, 37).is_err());
}

/// A precision or scale outside what BigQuery can express reports BigQuery's limit, not the
/// Rust type the driver happens to store it in.
#[test]
fn out_of_range_decimal_parameters_report_the_bigquery_limits() {
    assert_eq!(
        bigquery_decimal_data_type(BigQueryDecimalType::BigNumeric, 300, 1).unwrap_err(),
        "precision must be between 1 and 76"
    );
    assert_eq!(
        bigquery_decimal_data_type(BigQueryDecimalType::BigNumeric, 76, 300).unwrap_err(),
        "scale must be between 0 and 38"
    );
}
