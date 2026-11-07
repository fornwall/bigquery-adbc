use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal256Type, Float64Type, Int64Type, IntervalMonthDayNanoType,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::Schema;

use crate::pipeline::BATCH_SIZE;
use crate::rest::model::TableFieldSchema;
use crate::rows::RestRows;
use crate::test_support::schema::{field, field_mode, range_field, record, table_schema};
use crate::types::schema::convert_schema;

use super::*;

/// The column a unit test driving one [`ColumnBuilder`] directly reports failures against; the
/// whole-batch tests get theirs from the schema instead.
const TEST_COLUMN: ColumnAt<'static> = ColumnAt {
    name: "column",
    row: Some(0),
};

fn text(value: &str) -> RestValue {
    RestValue::String(value.to_owned())
}

fn uncancelled() -> ResultPipelineCancellation {
    ResultPipelineCancellation::new(None)
}

/// Appends one value to a builder a test drives directly, located at [`TEST_COLUMN`].
#[expect(
    clippy::needless_pass_by_value,
    reason = "taking ownership keeps the call sites, which build the value inline, brief"
)]
fn append(builder: &mut ColumnBuilder, value: RestValue) -> Result<(), ArrowError> {
    builder.append(&value, &ValueAt::Root(&TEST_COLUMN), &uncancelled())
}

fn column_builders(schema: &Schema) -> Vec<ColumnBuilder> {
    schema
        .fields()
        .iter()
        .map(|field| ColumnBuilder::new(field, BATCH_SIZE).unwrap())
        .collect()
}

fn decoded_rows_to_record_batch(
    rows: Vec<Vec<RestValue>>,
    schema: SchemaRef,
    cancellation: &ResultPipelineCancellation,
) -> Result<Option<RecordBatch>, ArrowError> {
    rest_rows_to_record_batch(
        rows.into_iter().map(RestRow::new).collect(),
        0,
        schema,
        cancellation,
    )
}

#[test]
fn converts_small_and_full_decoded_batches() {
    let table = table_schema([
        field_mode("number", "INTEGER", "REQUIRED"),
        field_mode("text", "STRING", "REQUIRED"),
    ]);
    let schema = convert_schema(&table).unwrap();
    let cancellation = uncancelled();

    for row_count in [1, 32, BATCH_SIZE] {
        let rows = (0..row_count)
            .map(|index| {
                vec![
                    RestValue::Integer(i64::try_from(index).unwrap()),
                    RestValue::String(format!("row {index}")),
                ]
            })
            .collect();
        let batch = decoded_rows_to_record_batch(rows, schema.clone(), &cancellation)
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), row_count);
        let numbers = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(
            numbers.value(row_count - 1),
            i64::try_from(row_count - 1).unwrap()
        );
        let text = batch.column(1).as_string::<i32>();
        assert_eq!(text.value(row_count - 1), format!("row {}", row_count - 1));
    }
}

/// STRING, JSON and GEOGRAPHY columns are assembled from the cells' bytes and checked as UTF-8
/// once per column, so multi-byte text must survive whether a cell was stored inline or on the
/// heap, beside empty and NULL cells.
#[test]
fn decodes_text_columns_of_any_length_and_encoding() {
    let schema = convert_schema(&table_schema(
        ["STRING", "JSON", "GEOGRAPHY"].map(|kind| field(kind, kind)),
    ))
    .unwrap();
    let texts = [
        Some(""),
        Some("é"),
        None,
        Some("🦀 a cell long enough to be stored on the heap 🦀"),
        Some("\0\u{7f}\u{80}\u{10ffff}"),
    ];
    // Decoded from the wire, as a page is, so short cells take the inline representation.
    let cell = |text: Option<&str>| {
        text.map_or(RestValue::Null, |text| {
            serde_json::from_str(&serde_json::to_string(text).unwrap()).unwrap()
        })
    };
    let rows = texts
        .iter()
        .map(|text| (0..3).map(|_| cell(*text)).collect())
        .collect();
    let batch = decoded_rows_to_record_batch(rows, schema, &uncancelled())
        .unwrap()
        .unwrap();
    for column in batch.columns() {
        assert_eq!(column.as_string::<i32>().iter().collect::<Vec<_>>(), texts);
    }
}

/// BigQuery sends every INT64 and FLOAT64 as a JSON string, which is the only encoding that
/// carries `i64::MIN`, `i64::MAX`, and the three non-finite floats.
#[test]
fn decodes_extreme_integers_and_non_finite_floats() {
    let table = table_schema([
        field_mode("number", "INTEGER", "REQUIRED"),
        field_mode("value", "FLOAT", "REQUIRED"),
    ]);
    let schema = convert_schema(&table).unwrap();
    let cancellation = uncancelled();
    let rows = [
        (i64::MIN, "Infinity"),
        (i64::MAX, "-Infinity"),
        (0, "NaN"),
        (1, "-0"),
    ]
    .into_iter()
    .map(|(number, value)| vec![RestValue::String(number.to_string()), text(value)])
    .collect();

    let batch = decoded_rows_to_record_batch(rows, schema, &cancellation)
        .unwrap()
        .unwrap();

    let numbers = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!(numbers.values(), &[i64::MIN, i64::MAX, 0, 1]);
    let values = batch.column(1).as_primitive::<Float64Type>();
    assert_eq!(values.value(0), f64::INFINITY);
    assert_eq!(values.value(1), f64::NEG_INFINITY);
    assert!(values.value(2).is_nan());
    assert!(values.value(3) == 0.0 && values.value(3).is_sign_negative());
}

/// BigQuery spells a BOOL `"true"` or `"false"`; anything else, other capitalizations included,
/// is a malformed value rather than a boolean.
#[test]
fn decodes_exactly_the_two_bool_spellings() {
    let schema = convert_schema(&table_schema([field("flag", "BOOLEAN")])).unwrap();
    let rows = [text("true"), text("false"), RestValue::Boolean(true)]
        .into_iter()
        .map(|cell| vec![cell])
        .collect();
    let batch = decoded_rows_to_record_batch(rows, schema.clone(), &uncancelled())
        .unwrap()
        .unwrap();
    let flags = batch.column(0).as_boolean();
    assert_eq!(
        flags.iter().collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true)]
    );

    for spelling in ["TRUE", "True", "1", "", " true"] {
        let message = decoded_rows_to_record_batch(
            vec![vec![text(spelling)]],
            schema.clone(),
            &uncancelled(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            message.contains("is not a valid BOOL"),
            "{spelling:?}: {message}"
        );
    }
}

/// A JSON number an INT64 column cannot hold is a value failure, not a shape mismatch.
#[test]
fn reports_non_integral_numbers_as_invalid_int64_values() {
    let table = table_schema([field_mode("number", "INTEGER", "REQUIRED")]);
    let schema = convert_schema(&table).unwrap();

    for cell in [
        RestValue::Float(1.5),
        RestValue::Unsigned(u64::try_from(i64::MAX).unwrap() + 1),
        text("1.5"),
    ] {
        let message =
            decoded_rows_to_record_batch(vec![vec![cell]], schema.clone(), &uncancelled())
                .unwrap_err()
                .to_string();
        assert!(message.contains("\"number\""), "{message}");
        assert!(message.contains("is not a valid INT64"), "{message}");
    }

    // A shape the column genuinely cannot be decoded from still reports the shape.
    let message =
        decoded_rows_to_record_batch(vec![vec![RestValue::Boolean(true)]], schema, &uncancelled())
            .unwrap_err()
            .to_string();
    assert!(message.contains("is a boolean"), "{message}");
}

#[test]
fn builds_nested_and_repeated_arrays() {
    let table = table_schema([TableFieldSchema {
        fields: vec![
            field_mode("id", "INT64", "REQUIRED"),
            field_mode("tags", "STRING", "REPEATED"),
        ],
        ..field_mode("records", "RECORD", "REPEATED")
    }]);
    let schema = convert_schema(&table).unwrap();
    let mut builder = ColumnBuilder::new(schema.field(0), 1).unwrap();
    append(
        &mut builder,
        RestValue::Repeated(vec![
            RestValue::Record(vec![
                RestValue::Integer(1),
                RestValue::Repeated(vec![text("a"), text("b")]),
            ]),
            RestValue::Record(vec![RestValue::Integer(2), RestValue::Repeated(Vec::new())]),
        ]),
    )
    .unwrap();
    append(&mut builder, RestValue::Repeated(Vec::new())).unwrap();
    append(&mut builder, RestValue::Null).unwrap();

    let array = builder.finish().unwrap();
    let records = array.as_list::<i32>();
    assert_eq!(records.value_offsets(), &[0, 2, 2, 2]);
    assert!(!records.is_null(2));
    let values = records.values();
    let structs = values.as_struct();
    assert_eq!(
        structs.column(0).as_primitive::<Int64Type>().values(),
        &[1, 2]
    );
    let tags = structs.column(1).as_list::<i32>();
    assert_eq!(tags.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        tags.values().as_string::<i32>().iter().collect::<Vec<_>>(),
        vec![Some("a"), Some("b")]
    );
}

/// The whole REST decode path for the temporal, INTERVAL and RANGE columns, not only the parsers:
/// each parsed value has to land in the Arrow column its converted field declares, beside a NULL.
#[test]
fn decodes_temporal_interval_and_range_columns() {
    let cells = [
        "2020-07-02",
        "12:34:56.789012",
        "2020-07-02T12:34:56.789012",
        "1593676800123456",
        "1-2 3 4:05:06.007008009",
        "[1970-01-01, 1970-01-03)",
        "[1970-01-01T00:00:01.123456, UNBOUNDED)",
        "[1000001, 2123456)",
    ];
    let schema = convert_schema(&table_schema([
        field("date", "DATE"),
        field("time", "TIME"),
        field("datetime", "DATETIME"),
        field("moment", "TIMESTAMP"),
        field("duration", "INTERVAL"),
        range_field("dates", "DATE"),
        range_field("datetimes", "DATETIME"),
        range_field("timestamps", "TIMESTAMP"),
    ]))
    .unwrap();
    let rows = vec![
        cells.map(text).into(),
        cells.map(|_| RestValue::Null).into(),
    ];
    let batch = decoded_rows_to_record_batch(rows, schema, &uncancelled())
        .unwrap()
        .unwrap();
    assert!(batch.columns().iter().all(|column| column.is_null(1)));

    let micros = |array: &ArrayRef| array.as_primitive::<TimestampMicrosecondType>().value(0);
    assert_eq!(
        batch.column(0).as_primitive::<Date32Type>().value(0),
        18_445
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<Time64MicrosecondType>()
            .value(0),
        45_296_789_012
    );
    assert_eq!(micros(batch.column(2)), 1_593_693_296_789_012);
    assert_eq!(micros(batch.column(3)), 1_593_676_800_123_456);
    assert_eq!(
        batch
            .column(4)
            .as_primitive::<IntervalMonthDayNanoType>()
            .value(0),
        IntervalMonthDayNano::new(14, 3, 14_706_007_008_009)
    );

    let dates = batch.column(5).as_struct();
    assert_eq!(dates.column(0).as_primitive::<Date32Type>().value(0), 0);
    assert_eq!(dates.column(1).as_primitive::<Date32Type>().value(0), 2);
    let datetimes = batch.column(6).as_struct();
    assert_eq!(micros(datetimes.column(0)), 1_123_456);
    assert!(datetimes.column(1).is_null(0));
    let timestamps = batch.column(7).as_struct();
    assert_eq!(micros(timestamps.column(0)), 1_000_001);
    assert_eq!(micros(timestamps.column(1)), 2_123_456);
}

/// BigQuery spells a RANGE as `[start, end)` and an absent bound as `UNBOUNDED`; an empty bound is
/// also accepted, as upstream's parser does. The driver splits the literal itself, so every
/// malformed shape is checked here.
#[test]
fn decodes_unbounded_and_malformed_range_literals() {
    let table = table_schema([range_field("dates", "DATE")]);
    let schema = convert_schema(&table).unwrap();
    let mut builder = ColumnBuilder::new(schema.field(0), 4).unwrap();
    for literal in [
        "[UNBOUNDED, UNBOUNDED)",
        "[UNBOUNDED, 1970-01-03)",
        "[1970-01-01, UNBOUNDED)",
        "[, )",
    ] {
        append(&mut builder, text(literal)).unwrap();
    }
    let array = builder.finish().unwrap();
    let ranges = array.as_struct();
    let bounds = |index: usize| ranges.column(index).as_primitive::<Date32Type>().clone();
    let (start, end) = (bounds(0), bounds(1));
    // An unbounded bound is a null bound, not a null range.
    assert!(ranges.is_valid(0) && start.is_null(0) && end.is_null(0));
    assert!(start.is_null(1));
    assert_eq!(end.value(1), 2);
    assert_eq!(start.value(2), 0);
    assert!(end.is_null(2));
    assert!(start.is_null(3) && end.is_null(3));

    let mut builder = ColumnBuilder::new(schema.field(0), 1).unwrap();
    for malformed in [
        "",
        "1970-01-01, 1970-01-03",
        "[1970-01-01, 1970-01-03]",
        "[1970-01-01)",
    ] {
        let error = append(&mut builder, text(malformed)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("is not a valid RANGE"), "{message}");
    }
}

/// A NULL in a REQUIRED column or STRUCT field is a malformed response, which Arrow would refuse
/// only when assembling the batch, without a row. It is a decode failure (`INVALID_DATA` at the C
/// interface) naming the column, the row and the field path. A NULL STRUCT's REQUIRED fields are
/// absent rather than NULL, so they are not refused.
#[test]
fn reports_a_null_in_a_required_column_or_field_by_its_row() {
    let schema = convert_schema(&table_schema([
        field_mode("id", "INTEGER", "REQUIRED"),
        TableFieldSchema {
            fields: vec![field_mode("child", "INTEGER", "REQUIRED")],
            ..field_mode("record", "RECORD", "NULLABLE")
        },
    ]))
    .unwrap();
    let row = |id: RestValue, record: RestValue| vec![id, record];
    let present = || text("1");
    let convert = |rows| {
        decoded_rows_to_record_batch(rows, schema.clone(), &uncancelled())
            .unwrap_err()
            .to_string()
    };

    assert_eq!(
        convert(vec![
            row(present(), RestValue::Null),
            row(RestValue::Null, RestValue::Null),
        ]),
        "Parser error: failed to decode BigQuery result column \"id\", row 1: BigQuery returned \
         a NULL for a REQUIRED field"
    );
    assert_eq!(
        convert(vec![
            row(present(), RestValue::Null),
            row(present(), RestValue::Record(vec![RestValue::Null])),
        ]),
        "Parser error: failed to decode BigQuery result column \"record\", row 1, field \
         \"child\": BigQuery returned a NULL for a REQUIRED field"
    );
}

/// Cells stay positional, so names are metadata rather than identity: anonymous, duplicate and
/// unsorted STRUCT children remain separate Arrow children in the order BigQuery returned them.
#[test]
fn preserves_struct_fields_by_position() {
    for children in [["", ""], ["a", "a"], ["z", "a"]] {
        let table = table_schema([record("s", "RECORD", children.map(|n| field(n, "INT64")))]);
        let schema = convert_schema(&table).unwrap();
        let mut builder = ColumnBuilder::new(schema.field(0), 2).unwrap();
        append(
            &mut builder,
            RestValue::Record(vec![RestValue::Integer(7), RestValue::Integer(9)]),
        )
        .unwrap();
        append(&mut builder, RestValue::Null).unwrap();
        let array = builder.finish().unwrap();
        let structs = array.as_struct();
        for (index, expected) in [7, 9].into_iter().enumerate() {
            assert_eq!(
                structs.column(index).as_primitive::<Int64Type>().value(0),
                expected
            );
        }
        assert!(structs.is_null(1));
    }
}

/// Arrow's 32-bit offset builders panic on overflow rather than reporting it, so the bound is
/// enforced where each value arrives. A panic crossing the ADBC FFI boundary aborts the
/// caller's process; an error status does not.
#[test]
fn refuses_appends_that_would_overflow_32_bit_arrow_offsets() {
    let limit = usize::try_from(i32::MAX).unwrap();
    check_offset_room(limit - 1, 1, "STRING").unwrap();
    check_offset_room(limit, 0, "STRING").unwrap();
    let error = check_offset_room(limit, 1, "STRING").unwrap_err();
    assert!(
        error.to_string().contains("32-bit Arrow offsets"),
        "{error}"
    );
    // The running total itself must not wrap before it is compared.
    check_offset_room(usize::MAX, 1, "ARRAY").unwrap_err();
}

/// The wrong-shape message names the column and the JSON kinds, never the server-supplied value:
/// one ARRAY cell can be megabytes.
#[test]
fn reports_wrong_value_shapes_by_kind_without_echoing_them() {
    let table = table_schema([
        field_mode("tags", "STRING", "REPEATED"),
        record("record", "RECORD", [field("id", "INT64")]),
    ]);
    let schema = convert_schema(&table).unwrap();
    let secret = "s".repeat(4096);
    let convert = |row: Vec<RestValue>| {
        decoded_rows_to_record_batch(vec![row], schema.clone(), &uncancelled())
            .unwrap_err()
            .to_string()
    };

    // A REPEATED field whose cell arrives as a scalar.
    let message = convert(vec![
        RestValue::String(secret.clone()),
        RestValue::Record(vec![RestValue::Integer(1)]),
    ]);
    assert!(message.contains("\"tags\""), "{message}");
    assert!(message.contains("is a string"), "{message}");
    assert!(message.contains("JSON array"), "{message}");
    assert!(
        !message.contains(&secret),
        "the value was echoed: {message}"
    );

    // A RECORD whose cell arrives as a string.
    let message = convert(vec![
        RestValue::Repeated(vec![text("a")]),
        RestValue::String(secret.clone()),
    ]);
    assert!(message.contains("\"record\""), "{message}");
    assert!(message.contains("is a string"), "{message}");
    assert!(message.contains("JSON object"), "{message}");
    assert!(
        !message.contains(&secret),
        "the value was echoed: {message}"
    );

    // A large ARRAY under a scalar column is the unbounded case the cap exists for.
    let long_array = RestValue::Repeated(
        std::iter::repeat_with(|| RestValue::String(secret.clone()))
            .take(64)
            .collect::<Vec<_>>(),
    );
    let message = convert(vec![RestValue::Repeated(vec![text("a")]), long_array]);
    assert!(
        !message.contains(&secret),
        "the value was echoed: {message}"
    );
    assert!(message.len() < 200, "{} bytes: {message}", message.len());
}

/// A STRUCT cell with the wrong number of cells is a schema/row disagreement, not a null.
#[test]
fn rejects_struct_values_with_missing_or_extra_cells() {
    let children = [field("id", "INT64"), field("label", "STRING")];
    let table = table_schema([record("record", "RECORD", children)]);
    let schema = convert_schema(&table).unwrap();
    for values in [
        vec![RestValue::Integer(1)],
        vec![RestValue::Integer(1), text("label"), text("extra")],
    ] {
        let mut builder = ColumnBuilder::new(schema.field(0), 1).unwrap();
        let error = append(&mut builder, RestValue::Record(values)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("schema has 2 fields"), "{message}");
    }
}

/// A RANGE bound is parsed by the same function as a scalar column of its element type, so
/// neither the calendar nor the grammar can differ between the two spellings of a value.
#[test]
fn rejects_range_bounds_outside_the_calendar() {
    let table = table_schema([
        range_field("dates", "DATE"),
        range_field("datetimes", "DATETIME"),
        range_field("timestamps", "TIMESTAMP"),
    ]);
    let schema = convert_schema(&table).unwrap();
    let mut builders = column_builders(&schema);
    for (index, literal, kind) in [
        (0, "[0000-01-01, UNBOUNDED)", "DATE"),
        (1, "[UNBOUNDED, 0000-01-01T00:00:00)", "DATETIME"),
        (2, "[UNBOUNDED, 253402300800000000)", "TIMESTAMP"),
    ] {
        let error = append(&mut builders[index], text(literal)).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(&format!("is not a valid {kind}")),
            "{message}"
        );
    }
}

/// The whole REST decode path for BIGNUMERIC's physical extremes, not only the parser: the
/// builder must keep them too, because the Storage Read path delivers them without any value
/// validation at all. A rejection quotes both the 77-digit limit and the declared precision of 76
/// the caller can act on.
#[test]
fn decodes_bignumeric_to_its_physical_extremes_and_refuses_past_them() {
    let maximum = "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
    let minimum = "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";
    let table = table_schema([field_mode("value", "BIGNUMERIC", "REQUIRED")]);
    let schema = convert_schema(&table).unwrap();
    let rows = vec![vec![text(maximum)], vec![text(minimum)]];
    let batch = decoded_rows_to_record_batch(rows, schema.clone(), &uncancelled())
        .unwrap()
        .unwrap();
    let values = batch.column(0).as_primitive::<Decimal256Type>();
    assert_eq!(values.value(0), i256::MAX);
    assert_eq!(values.value(1), i256::MIN);

    // Scaled by BIGNUMERIC's 38 fractional digits, `1e39` needs 78 significant digits.
    let error =
        decoded_rows_to_record_batch(vec![vec![text("1e39")]], schema, &uncancelled()).unwrap_err();
    assert!(
        error.to_string().contains(
            "BigQuery BIGNUMERIC result value needs more than the 77 significant digits a \
             result column of precision 76 can hold"
        ),
        "{error}"
    );
}

#[test]
fn decodes_rest_wire_scalars_and_composites_into_arrow() {
    let scalar = |name: &str, kind: &str| field_mode(name, kind, "NULLABLE");
    let table = table_schema([
        scalar("text", "STRING"),
        scalar("data", "BYTES"),
        scalar("number", "INT64"),
        scalar("float", "FLOAT64"),
        scalar("flag", "BOOL"),
        scalar("moment", "TIMESTAMP"),
        scalar("date", "DATE"),
        scalar("time", "TIME"),
        scalar("datetime", "DATETIME"),
        scalar("numeric", "NUMERIC"),
        scalar("bignumeric", "BIGNUMERIC"),
        scalar("duration", "INTERVAL"),
        scalar("geography", "GEOGRAPHY"),
        scalar("json", "JSON"),
        range_field("dates", "DATE"),
        record(
            "anonymous",
            "RECORD",
            [scalar("", "INT64"), scalar("", "STRING")],
        ),
        field_mode("numbers", "INT64", "REPEATED"),
    ]);
    let schema = convert_schema(&table).unwrap();
    let rows: RestRows = serde_json::from_str(
        r#"[{"f":[
            {"v":"text"},
            {"v":"AQID"},
            {"v":"-7"},
            {"v":"Infinity"},
            {"v":"true"},
            {"v":"1593676800123456"},
            {"v":"2020-07-02"},
            {"v":"12:34:56.789012"},
            {"v":"2020-07-02T12:34:56.789012"},
            {"v":"123.45"},
            {"v":"12345678901234567890.12345678901234567890"},
            {"v":"1-2 3 4:05:06.007008009"},
            {"v":"POINT(1 2)"},
            {"v":"{\"key\":true}"},
            {"v":"[1970-01-01, 1970-01-03)"},
            {"v":{"f":[{"v":"11"},{"v":"second"}]}},
            {"v":[{"v":"1"},{"v":null},{"v":"3"}]}
        ]}]"#,
    )
    .unwrap();
    let batch = rest_rows_to_record_batch(rows.into_vec(), 0, schema, &uncancelled())
        .unwrap()
        .unwrap();

    assert_eq!(batch.column(0).as_string::<i32>().value(0), "text");
    assert_eq!(batch.column(1).as_binary::<i32>().value(0), &[1, 2, 3]);
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), -7);
    assert!(
        batch
            .column(3)
            .as_primitive::<Float64Type>()
            .value(0)
            .is_infinite()
    );
    assert!(batch.column(4).as_boolean().value(0));
    assert_eq!(
        batch
            .column(15)
            .as_struct()
            .column(1)
            .as_string::<i32>()
            .value(0),
        "second"
    );
    let numbers = batch.column(16).as_list::<i32>();
    assert_eq!(numbers.value_offsets(), &[0, 3]);
    let values = numbers.values().as_primitive::<Int64Type>();
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 3);
}

#[test]
fn preserves_row_count_for_zero_column_batches() {
    let schema = convert_schema(&table_schema([])).unwrap();
    for row_count in [0, 3] {
        let rows = (0..row_count).map(|_| RestRow::new(Vec::new())).collect();
        let batch = rest_rows_to_record_batch(rows, 0, schema.clone(), &uncancelled())
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), row_count);
    }
}

#[test]
fn rejects_top_level_rows_with_missing_or_extra_cells() {
    let table = table_schema([field("a", "INT64"), field("b", "STRING")]);
    let schema = convert_schema(&table).unwrap();
    for (cells, actual) in [
        (vec![RestValue::Integer(1)], 1),
        (vec![RestValue::Integer(1), text("b"), text("extra")], 3),
    ] {
        let error =
            rest_rows_to_record_batch(vec![RestRow::new(cells)], 0, schema.clone(), &uncancelled())
                .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(&format!("has {actual} cells")),
            "{message}"
        );
        assert!(message.contains("schema has 2 fields"), "{message}");
    }
}

#[test]
fn direct_conversion_observes_pipeline_cancellation_before_rows() {
    let schema = convert_schema(&table_schema([field("value", "INT64")])).unwrap();
    let cancellation = uncancelled();
    cancellation.cancel_local();
    let converted = rest_rows_to_record_batch(
        vec![RestRow::new(vec![RestValue::Integer(1)])],
        0,
        schema,
        &cancellation,
    )
    .unwrap();
    assert!(converted.is_none());
}

/// Anything but a shape schema conversion produces is a defect here, reported rather than decoded
/// wrongly.
#[test]
fn refuses_columns_schema_conversion_cannot_produce() {
    for data_type in [
        DataType::Decimal128(10, 2),
        DataType::Decimal256(20, 4),
        DataType::Int32,
    ] {
        let Err(error) = ColumnBuilder::new(&Field::new("column", data_type, true), 1) else {
            panic!("the column was accepted");
        };
        assert!(error.to_string().contains("has no REST decoder"), "{error}");
    }
}

/// An array is the one unbounded part of a row, so a cancellation has to land inside it.
#[test]
fn a_long_array_is_polled_for_cancellation() {
    let schema =
        convert_schema(&table_schema([field_mode("numbers", "INT64", "REPEATED")])).unwrap();
    let mut builder = ColumnBuilder::new(schema.field(0), 1).unwrap();
    let cancellation = uncancelled();
    cancellation.cancel_local();
    let error = builder
        .append(
            &RestValue::Repeated(vec![RestValue::Integer(1)]),
            &ValueAt::Root(&TEST_COLUMN),
            &cancellation,
        )
        .unwrap_err();
    assert!(error.to_string().contains("was cancelled"), "{error}");
}

/// A decode failure names the path to the failed cell: column, row, and each array element and
/// nested field. The value itself stays out of the message (see
/// `scalar_parse_failures_never_echo_values`).
#[test]
fn a_nested_decode_failure_names_the_column_the_row_and_the_field_path() {
    let table = table_schema([TableFieldSchema {
        mode: "REPEATED".to_owned(),
        ..record(
            "orders",
            "RECORD",
            [record(
                "shipment",
                "RECORD",
                [field("delivered_on", "DATE")],
            )],
        )
    }]);
    let schema = convert_schema(&table).unwrap();
    let order = |date: &str| {
        RestValue::Repeated(vec![RestValue::Record(vec![RestValue::Record(vec![
            text(date),
        ])])])
    };
    let rows = vec![vec![order("2026-09-17")], vec![order("not a date")]];
    let error = decoded_rows_to_record_batch(rows, schema, &uncancelled()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Parser error: failed to decode BigQuery result column \"orders\", row 1, element 0, \
         field \"shipment\", field \"delivered_on\": BigQuery result value is not a valid DATE"
    );
}

/// A failure in a column with nothing nested inside it carries the column and the row and nothing
/// else. A result is converted in batches, so that row is the one within the whole result, which
/// the caller can find, rather than its position within the batch.
#[test]
fn a_decode_failure_names_the_row_within_the_result() {
    let schema = convert_schema(&table_schema([field("moment", "TIMESTAMP")])).unwrap();
    let convert = |rows: Vec<RestRow>| {
        rest_rows_to_record_batch(rows, 2 * BATCH_SIZE, schema.clone(), &uncancelled())
            .unwrap_err()
            .to_string()
    };

    let message = convert(vec![
        RestRow::new(vec![RestValue::Null]),
        RestRow::new(vec![text("not a timestamp")]),
    ]);
    assert_eq!(
        message,
        "Parser error: failed to decode BigQuery result column \"moment\", row 2049: BigQuery \
         result value is not a valid TIMESTAMP"
    );

    let message = convert(vec![
        RestRow::new(vec![RestValue::Null]),
        RestRow::new(Vec::new()),
    ]);
    assert!(message.contains("result row 2049 has 0 cells"), "{message}");
}
