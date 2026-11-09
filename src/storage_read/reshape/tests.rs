use arrow_array::types::{IntervalMonthDayNanoType, IntervalYearMonthType};
use arrow_array::{Decimal128Array, Int64Array, ListArray, TimestampMicrosecondArray};
use arrow_buffer::IntervalMonthDayNano;
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

/// Leaf pairs become casts only where every value converts exactly; `types::cast` tests the
/// values. Padding fractional digits is exact, as is widening into a parameter-free BIGNUMERIC,
/// whose domain is the whole `i256`. Dropping fractional digits is not exact, and neither is
/// padding past what the target precision can hold; refusing those falls the query back to REST
/// rather than changing a value.
#[test]
fn only_lossless_leaf_conversions_are_planned() {
    let month_day_nano = DataType::Interval(IntervalUnit::MonthDayNano);
    for (source, target, planned) in [
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal128(38, 9),
            true,
        ),
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal128(17, 9),
            true,
        ),
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal128(12, 9),
            false,
        ),
        (
            DataType::Decimal128(38, 9),
            DataType::Decimal128(38, 2),
            false,
        ),
        (
            DataType::Decimal128(10, 2),
            DataType::Decimal256(76, 38),
            true,
        ),
        (
            DataType::Decimal256(38, 38),
            DataType::Decimal256(76, 38),
            true,
        ),
        (
            DataType::Decimal256(50, 0),
            DataType::Decimal256(76, 38),
            false,
        ),
        (
            DataType::Decimal256(10, 2),
            DataType::Decimal128(38, 9),
            false,
        ),
        (
            DataType::Interval(IntervalUnit::YearMonth),
            month_day_nano.clone(),
            true,
        ),
        (
            DataType::Interval(IntervalUnit::DayTime),
            month_day_nano.clone(),
            true,
        ),
        (
            month_day_nano,
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
    ] {
        let plan = column_plan(&source, &target);
        assert_eq!(
            matches!(plan, Some(ColumnPlan::Cast(_))),
            planned,
            "{source} to {target}: {plan:?}"
        );
    }
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
