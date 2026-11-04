use std::collections::HashMap;

use arrow_array::ArrowNativeTypeOp as _;
use arrow_array::cast::AsArray as _;
use arrow_array::types::Decimal128Type;
use arrow_array::{Decimal32Array, Decimal128Array, Decimal256Array};
use arrow_schema::Field;

use super::*;

fn decimal128(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// A field of `data_type` declaring `(precision, scale)` in its metadata.
fn declaring(data_type: DataType, precision: u8, scale: i8) -> Field {
    Field::new("amount", data_type, true).with_metadata(HashMap::from([
        (
            BIGQUERY_FIELD_METADATA_PRECISION.to_owned(),
            precision.to_string(),
        ),
        (BIGQUERY_FIELD_METADATA_SCALE.to_owned(), scale.to_string()),
    ]))
}

fn rescale(column: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    assert!(needs_rescale(column.data_type(), field));
    rescale_decimal_column(column, field, &ValueSite::column("amount", 0))
}

/// A value read as NUMERIC's full `(38, 9)` fits a `NUMERIC(10, 2)` column when its dropped
/// digits are zeroes; nulls, whose payloads are arbitrary, are not looked at.
#[test]
fn a_wider_decimal_narrows_exactly_into_its_columns_shape() {
    let column = decimal128(
        vec![Some(1_230_000_000), None, Some(-99_999_999_990_000_000)],
        38,
        9,
    );
    let narrowed = rescale(
        &column,
        &Field::new("amount", DataType::Decimal128(10, 2), true),
    )
    .unwrap();
    let narrowed = narrowed.as_primitive::<Decimal128Type>();
    assert_eq!(narrowed.data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(
        narrowed.iter().collect::<Vec<_>>(),
        [Some(123), None, Some(-9_999_999_999)]
    );
}

/// Storage Write would round a value with more fractional digits than its column's scale, or
/// store one with more integer digits, so each is refused by row rather than altered.
#[test]
fn a_value_that_does_not_fit_its_columns_shape_is_refused_by_row() {
    let field = Field::new("amount", DataType::Decimal128(10, 2), true);
    for (value, reason) in [
        (
            1_234_000_000,
            "more than 2 fractional digits, which its column's NUMERIC(10, 2) cannot store \
             without rounding",
        ),
        (
            100_000_000_000_000_000,
            "more integer digits than its column's NUMERIC(10, 2) holds",
        ),
    ] {
        let column = decimal128(vec![Some(0), None, Some(value)], 38, 9);
        let failure = rescale(&column, &field).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert_eq!(
            failure.message,
            format!("Arrow decimal ingest field \"amount\" holds a value at row 2 with {reason}")
        );
    }
}

/// An array element travels at its type's full scale (Storage Write reads it there), but has to
/// fit the shape its column declares.
#[test]
fn an_element_keeps_its_declared_shape_on_its_full_scale_wire_type() {
    let wire = declaring(DataType::Decimal128(38, 9), 10, 2);
    let column = decimal128(vec![Some(123)], 10, 2);
    let widened = rescale(&column, &wire).unwrap();
    assert_eq!(
        widened.as_primitive::<Decimal128Type>().value(0),
        1_230_000_000
    );

    // Already on the wire type, a value still has to fit the declared shape.
    let on_the_wire = decimal128(vec![Some(1_234_000_000)], 38, 9);
    assert!(rescale(&on_the_wire, &wire).is_err());
    assert!(!needs_rescale(
        &DataType::Decimal128(38, 9),
        &Field::new("amount", DataType::Decimal128(38, 9), true)
    ));
}

/// Every Arrow width reaches either column width, and plain BIGNUMERIC's values beyond its
/// declared 76 digits are not refused.
#[test]
fn decimals_cross_widths_both_ways() {
    let narrow: ArrayRef = Arc::new(
        Decimal32Array::from(vec![Some(-12_345)])
            .with_precision_and_scale(7, 3)
            .unwrap(),
    );
    let widened = rescale(
        &narrow,
        &Field::new("amount", DataType::Decimal256(76, 38), true),
    )
    .unwrap();
    assert_eq!(
        widened.as_primitive::<Decimal256Type>().value(0),
        i256::from_i128(-12_345) * i256::from_i128(10).pow_wrapping(35)
    );

    let huge = i256::MAX;
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(huge)])
            .with_precision_and_scale(76, 38)
            .unwrap(),
    );
    let numeric = Field::new("amount", DataType::Decimal128(38, 9), true);
    assert!(rescale(&wide, &numeric).is_err());
    let small: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(
            i256::from_i128(5) * i256::from_i128(10).pow_wrapping(37),
        )])
        .with_precision_and_scale(76, 38)
        .unwrap(),
    );
    assert_eq!(
        rescale(&small, &numeric)
            .unwrap()
            .as_primitive::<Decimal128Type>()
            .value(0),
        500_000_000
    );
}

/// Plain BIGNUMERIC's type reaches past its 76 declared digits, but a column declaring
/// `BIGNUMERIC(76, 38)` stops at 38 integer digits, so a field declaring it is checked even on
/// the type it shares with plain BIGNUMERIC.
#[test]
fn a_declared_bignumeric_76_38_is_bounded_by_its_precision() {
    let ten = i256::from_i128(10);
    // A value with `digits` integer digits, 2 followed by zeroes.
    let column = |digits: u32| -> ArrayRef {
        Arc::new(
            Decimal256Array::from(vec![Some(
                i256::from_i128(2) * ten.pow_wrapping(digits - 1) * ten.pow_wrapping(38),
            )])
            .with_precision_and_scale(76, 38)
            .unwrap(),
        )
    };
    let plain = Field::new("amount", DataType::Decimal256(76, 38), true);
    let declared = declaring(DataType::Decimal256(76, 38), 76, 38);

    assert!(!needs_rescale(&DataType::Decimal256(76, 38), &plain));
    assert!(rescale(&column(38), &declared).is_ok());
    let failure = rescale(&column(39), &declared).unwrap_err();
    assert_eq!(
        failure.message,
        "Arrow decimal ingest field \"amount\" holds a value at row 0 with more integer digits \
         than its column's BIGNUMERIC(76, 38) holds"
    );
}

#[test]
fn unchanged_decimal_scales_share_values_and_preserve_sliced_nulls() {
    let numeric = decimal128(vec![Some(0), Some(123), None, Some(-456)], 38, 2).slice(1, 3);
    let bignumeric: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::ZERO),
            Some(i256::from_i128(123)),
            None,
            Some(i256::from_i128(-456)),
        ])
        .with_precision_and_scale(76, 2)
        .unwrap()
        .slice(1, 3),
    );
    for (column, data_type) in [
        (numeric, DataType::Decimal128(10, 2)),
        (bignumeric, DataType::Decimal256(10, 2)),
    ] {
        let field = Field::new("amount", data_type, true);
        let normalized = rescale(&column, &field).unwrap();
        assert_eq!(normalized.data_type(), field.data_type());
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized.nulls(), column.nulls());
        assert_eq!(
            normalized.to_data().buffers()[0].as_ptr(),
            column.to_data().buffers()[0].as_ptr()
        );
    }
}

#[test]
fn narrower_decimal_metadata_retains_the_wire_array_after_validation() {
    let values =
        arrow_buffer::ScalarBuffer::from(vec![0, 1_230_000_000, i128::MAX, -4_560_000_000]);
    let column: ArrayRef = Arc::new(
        Decimal128Array::new(
            values,
            Some(arrow_buffer::NullBuffer::from(vec![
                true, true, false, true,
            ])),
        )
        .with_precision_and_scale(38, 9)
        .unwrap()
        .slice(1, 3),
    );
    let field = declaring(DataType::Decimal128(38, 9), 10, 2);
    let normalized = rescale(&column, &field).unwrap();
    assert!(Arc::ptr_eq(&normalized, &column));

    for (value, reason) in [
        (1_234_000_000, "fractional digits"),
        (100_000_000_000_000_000, "integer digits"),
    ] {
        let column = decimal128(vec![Some(0), None, Some(value)], 38, 9).slice(1, 2);
        let failure = rescale(&column, &field).unwrap_err();
        assert_eq!(failure.status, Status::InvalidData);
        assert!(failure.message.contains("row 1"));
        assert!(failure.message.contains(reason));
    }
}
