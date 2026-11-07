use arrow_array::types::{IntervalMonthDayNanoType, IntervalYearMonthType};
use arrow_array::{
    Decimal128Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, ListArray, TimestampMicrosecondArray,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::{Field, Schema};

use super::*;

fn field_of(data_type: DataType) -> Field {
    Field::new("value", data_type, true)
}

#[test]
fn timestamp_zone_labels_preserve_the_logical_type() {
    for source_zone in [None, Some(""), Some("UTC"), Some("America/New_York")] {
        for target_zone in [None, Some(""), Some("UTC"), Some("America/New_York")] {
            let source = TimestampMicrosecondArray::from(vec![Some(-1), None, Some(1)])
                .with_timezone_opt(source_zone);
            let expected = TimestampMicrosecondArray::from(vec![Some(-1), None, Some(1)])
                .with_timezone_opt(target_zone);
            let session = Arc::new(Schema::new(vec![field_of(source.data_type().clone())]));
            let canonical = Arc::new(Schema::new(vec![field_of(expected.data_type().clone())]));
            let plan = reshape_plans(&session, &canonical);
            if matches!(source_zone, Some("UTC" | "America/New_York"))
                != matches!(target_zone, Some("UTC" | "America/New_York"))
            {
                assert!(plan.is_err(), "{source_zone:?} became {target_zone:?}");
                continue;
            }
            let batch = RecordBatch::try_new(session, vec![Arc::new(source)]).unwrap();
            let reshaped = reshape(batch, &canonical, &plan.unwrap()).unwrap();
            assert_eq!(reshaped.column(0).as_ref(), &expected as &dyn Array);
        }
    }
}

#[test]
fn an_identity_batch_adopts_the_canonical_schema_without_reallocating_columns() {
    let session = Arc::new(Schema::new(vec![field_of(DataType::Int64)]));
    let canonical =
        Arc::new(Schema::new(vec![field_of(DataType::Int64)]).with_metadata([("query-id", "job")]));
    let plan = reshape_plans(&session, &canonical).unwrap();
    assert!(plan.identity);

    let column: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(session, vec![column.clone()]).unwrap();
    let columns = batch.columns().as_ptr();
    let reshaped = reshape(batch, &canonical, &plan).unwrap();

    assert!(Arc::ptr_eq(reshaped.schema_ref(), &canonical));
    assert_eq!(reshaped.schema().field(0).name(), "value");
    assert_eq!(reshaped.schema().metadata()["query-id"], "job");
    assert_eq!(reshaped.num_rows(), 3);
    assert_eq!(reshaped.columns().as_ptr(), columns);
    assert!(Arc::ptr_eq(reshaped.column(0), &column));
}

#[test]
fn source_only_metadata_preserves_the_checked_rebuild_path() {
    let session = Arc::new(
        Schema::new(vec![field_of(DataType::Int64)]).with_metadata([("source-only", "value")]),
    );
    let canonical = Arc::new(Schema::new(vec![field_of(DataType::Int64)]));
    let plan = reshape_plans(&session, &canonical).unwrap();
    assert!(!plan.identity);

    let batch = RecordBatch::try_new(
        session,
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let reshaped = reshape(batch, &canonical, &plan).unwrap();

    assert!(Arc::ptr_eq(reshaped.schema_ref(), &canonical));
}

#[test]
fn a_mixed_plan_preserves_identity_columns_and_casts_the_rest() {
    let source_decimal = DataType::Decimal128(10, 2);
    let target_decimal = DataType::Decimal256(20, 2);
    let session = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", source_decimal, true),
    ]));
    let canonical = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("amount", target_decimal.clone(), true),
    ]));
    let plan = reshape_plans(&session, &canonical).unwrap();
    assert!(!plan.identity);

    let id: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(vec![123_i128])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let batch = RecordBatch::try_new(session, vec![id.clone(), amount]).unwrap();
    let reshaped = reshape(batch, &canonical, &plan).unwrap();

    assert!(Arc::ptr_eq(reshaped.column(0), &id));
    assert_eq!(reshaped.column(1).data_type(), &target_decimal);
}

#[test]
fn interval_plans_cast_sliced_values_exactly() {
    let month_day_nano = DataType::Interval(IntervalUnit::MonthDayNano);
    let session = Arc::new(Schema::new(vec![
        Field::new(
            "year_month",
            DataType::Interval(IntervalUnit::YearMonth),
            true,
        ),
        Field::new("day_time", DataType::Interval(IntervalUnit::DayTime), true),
    ]));
    let canonical = Arc::new(Schema::new(vec![
        Field::new("year_month", month_day_nano.clone(), true),
        Field::new("day_time", month_day_nano, true),
    ]));
    let plan = reshape_plans(&session, &canonical).unwrap();

    let year_month =
        IntervalYearMonthArray::from(vec![Some(999), Some(14), None, Some(-25), Some(888)]);
    let day_time = IntervalDayTimeArray::from(vec![
        Some(IntervalDayTime::new(99, 99)),
        Some(IntervalDayTime::new(2, 3)),
        None,
        Some(IntervalDayTime::new(-4, -5)),
        Some(IntervalDayTime::new(88, 88)),
    ]);
    let batch = RecordBatch::try_new(
        session,
        vec![
            Arc::new(year_month.slice(1, 3)) as ArrayRef,
            Arc::new(day_time.slice(1, 3)) as ArrayRef,
        ],
    )
    .unwrap();
    let reshaped = reshape(batch, &canonical, &plan).unwrap();

    assert_eq!(
        reshaped.column(0).as_ref(),
        &IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(14, 0, 0)),
            None,
            Some(IntervalMonthDayNano::new(-25, 0, 0)),
        ]) as &dyn Array
    );
    assert_eq!(
        reshaped.column(1).as_ref(),
        &IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 2, 3_000_000)),
            None,
            Some(IntervalMonthDayNano::new(0, -4, -5_000_000)),
        ]) as &dyn Array
    );
}

#[test]
fn nested_year_month_intervals_reshape_to_the_canonical_unit() {
    let source = ListArray::from_iter_primitive::<IntervalYearMonthType, _, _>([
        Some(vec![Some(14), None]),
        None,
        Some(vec![Some(-25)]),
    ]);
    let expected = ListArray::from_iter_primitive::<IntervalMonthDayNanoType, _, _>([
        Some(vec![Some(IntervalMonthDayNano::new(14, 0, 0)), None]),
        None,
        Some(vec![Some(IntervalMonthDayNano::new(-25, 0, 0))]),
    ]);
    let session = Arc::new(Schema::new(vec![field_of(source.data_type().clone())]));
    let canonical = Arc::new(Schema::new(vec![field_of(expected.data_type().clone())]));
    let plan = reshape_plans(&session, &canonical).unwrap();
    let batch = RecordBatch::try_new(session, vec![Arc::new(source)]).unwrap();

    let reshaped = reshape(batch, &canonical, &plan).unwrap();

    assert_eq!(reshaped.column(0).as_ref(), &expected as &dyn Array);
}

/// A parameter-free BIGNUMERIC spans the whole `i256`, one digit past the `Decimal256(76, 38)`
/// that describes it. Widening a narrower Storage Read decimal into it must accept every value
/// the REST path decodes, or a query would succeed or fail by the size of its result.
#[test]
fn widening_into_bignumeric_accepts_its_full_physical_range() {
    use arrow_array::Decimal256Array;
    use arrow_buffer::i256;

    let target = DataType::Decimal256(76, 38);
    let plan = column_plan(&DataType::Decimal256(38, 38), &target)
        .expect("a narrower BIGNUMERIC widens at the same scale");
    assert!(matches!(plan, ColumnPlan::Cast(_)), "{plan:?}");

    let column: ArrayRef = Arc::new(
        Decimal256Array::from(vec![i256::MIN, i256::ZERO, i256::MAX])
            .with_precision_and_scale(38, 38)
            .unwrap(),
    );
    let reshaped = reshape_column(&column, &plan, "value").unwrap();
    assert_eq!(reshaped.data_type(), &target);
    let values = reshaped
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .expect("the reshape keeps the physical width");
    assert_eq!(values.value(0), i256::MIN);
    assert_eq!(values.value(2), i256::MAX);
}

/// A session declaring a column's own `NUMERIC(10, 2)` where the canonical type is the bare
/// NUMERIC's `Decimal128(38, 9)` widens exactly rather than falling back to REST.
#[test]
fn a_narrower_scale_widens_into_the_canonical_numeric_exactly() {
    let target = DataType::Decimal128(38, 9);
    let plan = column_plan(&DataType::Decimal128(10, 2), &target)
        .expect("a parameterized NUMERIC widens into the canonical one");
    assert!(matches!(plan, ColumnPlan::Cast(_)), "{plan:?}");

    // 12345 at scale 2 is 123.45, which is 123_450_000_000 at scale 9.
    let column: ArrayRef = Arc::new(
        Decimal128Array::from(vec![12_345_i128, -12_345_i128, 0])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let reshaped = reshape_column(&column, &plan, "amount").unwrap();
    assert_eq!(reshaped.data_type(), &target);
    let values = reshaped
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("the reshape keeps the physical width");
    assert_eq!(values.value(0), 123_450_000_000);
    assert_eq!(values.value(1), -123_450_000_000);
    assert_eq!(values.value(2), 0);
}

/// Padding fractional digits is exact; dropping them is not, and neither is padding past what the
/// target precision can hold. Both are refused, which falls the query back to REST rather than
/// changing a value.
#[test]
fn scale_changes_that_would_lose_or_overflow_digits_are_refused() {
    // A narrower target scale would drop digits that cannot be recovered.
    assert!(column_plan(&DataType::Decimal128(38, 9), &DataType::Decimal128(38, 2)).is_none());
    // Widening 10 integer-and-fraction digits by 7 more needs 17; the target holds 12.
    assert!(column_plan(&DataType::Decimal128(10, 2), &DataType::Decimal128(12, 9)).is_none());
    // Exactly enough room is enough.
    assert!(column_plan(&DataType::Decimal128(10, 2), &DataType::Decimal128(17, 9)).is_some());
    // The same rule across the physical widths the session may hand over.
    assert!(column_plan(&DataType::Decimal128(10, 2), &DataType::Decimal256(76, 38)).is_some());
    assert!(column_plan(&DataType::Decimal256(50, 0), &DataType::Decimal256(76, 38)).is_none());
}

#[test]
fn reshape_failures_name_the_column() {
    use arrow_array::BooleanArray;

    let column: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    let plan = ColumnPlan::Cast(DataType::Date32);
    let error = reshape_column(&column, &plan, "flag").unwrap_err();
    assert!(
        error.to_string().contains("column \"flag\""),
        "the failing column must be named: {error}"
    );

    // A failure nested in a struct still names the top-level result column.
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field_of(DataType::Boolean)),
        column,
    )]));
    let plan = ColumnPlan::Struct(
        vec![ColumnPlan::Cast(DataType::Date32)],
        Fields::from(vec![field_of(DataType::Date32)]),
    );
    let error = reshape_column(&nested, &plan, "record").unwrap_err();
    assert!(
        error.to_string().contains("column \"record\""),
        "the failing column must be named: {error}"
    );
}

#[test]
fn irreconcilable_names_the_column_and_the_shapes() {
    let session = Arc::new(Schema::new(vec![field_of(DataType::Utf8)]));
    let target = Arc::new(Schema::new(vec![field_of(DataType::Utf8)]));
    assert!(reshape_plans(&session, &target).is_ok());

    let target = Arc::new(Schema::new(vec![field_of(DataType::Boolean)]));
    let reason = reshape_plans(&session, &target).expect_err("the shapes do not reconcile");
    assert!(reason.contains("value"), "{reason}");
    assert!(reason.contains("Utf8"), "{reason}");
    assert!(reason.contains("Boolean"), "{reason}");

    let target = Arc::new(Schema::new(vec![
        field_of(DataType::Utf8),
        field_of(DataType::Utf8),
    ]));
    let reason = reshape_plans(&session, &target).expect_err("the column counts differ");
    assert!(reason.contains("1 columns"), "{reason}");
}

/// Two same-typed columns delivered in a different order than the canonical schema would be
/// relabelled by a purely positional reshape, putting values under the wrong column name.
#[test]
fn differently_named_columns_do_not_reconcile() {
    let session = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let target = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Utf8, true),
        Field::new("a", DataType::Utf8, true),
    ]));
    let reason = reshape_plans(&session, &target).expect_err("the column names differ");
    assert!(reason.contains('a'), "{reason}");
    assert!(reason.contains('b'), "{reason}");
    assert!(reshape_plans(&session, &session.clone()).is_ok());
}

#[test]
fn unreconcilable_shapes_are_rejected() {
    assert!(column_plan(&DataType::Utf8, &DataType::Boolean).is_none());
    assert!(
        column_plan(
            &DataType::List(Arc::new(field_of(DataType::Utf8))),
            &DataType::List(Arc::new(field_of(DataType::Boolean))),
        )
        .is_none()
    );

    // Struct children are rebuilt with the canonical names, so a reordered pair of same-typed
    // children must not reconcile either.
    let structure = |first: &str, second: &str| {
        DataType::Struct(
            vec![
                Field::new(first, DataType::Utf8, true),
                Field::new(second, DataType::Utf8, true),
            ]
            .into(),
        )
    };
    assert!(column_plan(&structure("a", "b"), &structure("a", "b")).is_some());
    assert!(column_plan(&structure("a", "b"), &structure("b", "a")).is_none());
    let one_child = DataType::Struct(vec![Field::new("a", DataType::Utf8, true)].into());
    assert!(column_plan(&one_child, &structure("a", "b")).is_none());
}

/// A list is rebuilt with the canonical item field, so the item is checked like a struct's
/// children, not by type alone. A differing item name would be relabelled silently, and a
/// non-nullable canonical item over nullable values fails `ListArray::try_new` per batch, after
/// REST fallback is gone, so such a pair must fail at plan time.
#[test]
fn a_list_item_field_is_checked_before_it_is_adopted() {
    // Item types that differ, so the pair is planned by the list arm rather than by identity.
    let list = |name: &str, nullable: bool, item: DataType| {
        DataType::List(Arc::new(Field::new(name, item, nullable)))
    };
    let narrow = || DataType::Decimal128(10, 2);
    let canonical = || DataType::Decimal128(38, 9);

    for (source, target, reconciles, why) in [
        (("item", true), ("item", true), true, "the same item"),
        // Non-nullable values are a subset of what a nullable canonical item accepts.
        (("item", false), ("item", true), true, "a loosened item"),
        (
            ("element", true),
            ("item", true),
            false,
            "a relabelled item",
        ),
        (("item", true), ("item", false), false, "a tightened item"),
    ] {
        let plan = column_plan(
            &list(source.0, source.1, narrow()),
            &list(target.0, target.1, canonical()),
        );
        assert_eq!(plan.is_some(), reconciles, "{why}");
    }
}

#[test]
fn nullability_cannot_be_tightened_at_any_nesting_level() {
    let schema = |nullable, data_type, nesting| {
        let field = Field::new("value", data_type, nullable);
        let field = match nesting {
            0 => field,
            1 => field_of(DataType::Struct(vec![field].into())),
            2 => field_of(DataType::List(Arc::new(field))),
            _ => field_of(DataType::List(Arc::new(field_of(DataType::Struct(
                vec![field].into(),
            ))))),
        };
        Arc::new(Schema::new(vec![field]))
    };
    for nesting in 0..4 {
        for (source_type, target_type) in [
            (DataType::Int64, DataType::Int64),
            (DataType::Decimal128(10, 2), DataType::Decimal128(38, 9)),
        ] {
            let source = schema(true, source_type.clone(), nesting);
            let target = schema(false, target_type.clone(), nesting);
            let reason = reshape_plans(&source, &target).unwrap_err();
            assert!(reason.contains("value"), "{reason}");
            assert!(reason.contains("nullable"), "{reason}");

            let source = schema(false, source_type, nesting);
            let target = schema(true, target_type, nesting);
            assert!(reshape_plans(&source, &target).is_ok());
        }
    }
}
