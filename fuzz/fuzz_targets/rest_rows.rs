//! A REST result page, `jobs.query`'s or `jobs.getQueryResults`'s, decoded and converted into
//! Arrow against its schema.
//!
//! Input: a schema line, then the page. The line spells one field per letter of [`SCALARS`], with
//! `{`...`}` for a RECORD, `(`...`)` for one spelled `STRUCT`, and `<` plus a letter for a RANGE of
//! that element (a bare `<` has none). A suffix modifies the field before it: `*` REPEATED,
//! `!` REQUIRED, `=` NULLABLE, `~` an unknown mode, `p` and `P` a valid and an invalid decimal
//! shape, `m` a maximum length, `%` sub-microsecond precision, `+` a description, collation and
//! policy tag, `&` a RANGE element type, `'` a name that needs quoting and `_` an empty name.
//!
//! A line starting with `#` makes the rest of the input fuzzer choices for a well-formed page of
//! hostile cells: values at and past each type's range and grammar, NULLs and wrong JSON shapes.
//! Otherwise the rest is the `rows` JSON itself, which reaches malformed JSON and envelopes.
//!
//! Beyond never panicking, a decoded batch must be valid Arrow holding only values BigQuery can,
//! and no failure may echo a cell: cells carrying [`CANARY`] check that.
#![no_main]

use std::fmt::{Debug, Write as _};
use std::ops::RangeInclusive;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_schema::DataType;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;

/// Schema letters and the REST type names they spell, aliases and unknown names included.
const SCALARS: &[(u8, &str)] = &[
    (b's', "STRING"),
    (b'y', "BYTES"),
    (b'i', "INTEGER"),
    (b'I', "INT64"),
    (b'f', "FLOAT"),
    (b'F', "FLOAT64"),
    (b'b', "BOOLEAN"),
    (b'B', "BOOL"),
    (b'T', "TIMESTAMP"),
    (b'd', "DATE"),
    (b't', "TIME"),
    (b'D', "DATETIME"),
    (b'n', "NUMERIC"),
    (b'c', "DECIMAL"),
    (b'N', "BIGNUMERIC"),
    (b'C', "BIGDECIMAL"),
    (b'v', "INTERVAL"),
    (b'g', "GEOGRAPHY"),
    (b'j', "JSON"),
    (b'?', "GEOMETRY"),
    (b'x', ""),
];

/// Records open at once: one past the driver's nesting limit, so the schema check that guards the
/// decoder's recursion is reached.
const MAX_DEPTH: usize = 33;

/// Rows per generated page, which keeps one run fast.
const MAX_ROWS: usize = 64;

/// Text no field name or driver message contains, so a message that does echoes a cell.
const CANARY: &str = "c4n4ry";

const MIN_DATE_DAYS: i32 = -719_162;
const MAX_DATE_DAYS: i32 = 2_932_896;
const MIN_MICROS: i64 = -62_135_596_800_000_000;
const MAX_MICROS: i64 = 253_402_300_799_999_999;
const DAY_MICROS: i64 = 86_400_000_000;

#[derive(Default)]
struct Field {
    name: String,
    kind: &'static str,
    mode: &'static str,
    range_element: Option<&'static str>,
    /// Further JSON members, each with its leading comma.
    attributes: String,
    children: Vec<Field>,
}

impl Field {
    fn new(kind: &'static str, index: usize) -> Self {
        Self {
            name: format!("c{index}"),
            kind,
            ..Self::default()
        }
    }

    fn modify(&mut self, letter: u8) {
        match letter {
            b'*' => self.mode = "REPEATED",
            b'!' => self.mode = "REQUIRED",
            b'=' => self.mode = "NULLABLE",
            b'~' => self.mode = "SOMETIMES",
            b'p' => self.attributes.push_str(r#","precision":"10","scale":"2""#),
            b'P' => self
                .attributes
                .push_str(r#","precision":"99","scale":"-1""#),
            b'm' => self.attributes.push_str(r#","maxLength":"8""#),
            b'%' => self.attributes.push_str(r#","timestampPrecision":"12""#),
            b'+' => self.attributes.push_str(
                r#","description":"d","collation":"und:ci","policyTags":{"names":["t"]}"#,
            ),
            b'&' => self
                .attributes
                .push_str(r#","rangeElementType":{"type":"DATE"}"#),
            b'\'' => self.name = "q`\"\\é".to_owned(),
            b'_' => self.name.clear(),
            _ => {}
        }
    }
}

fn scalar(letter: u8) -> Option<&'static str> {
    SCALARS
        .iter()
        .find(|(candidate, _)| *candidate == letter)
        .map(|(_, kind)| *kind)
}

fn parse_schema(spec: &[u8]) -> Vec<Field> {
    // The root's children are the columns; every other entry is a RECORD still open.
    let mut open = vec![Field::default()];
    let mut letters = spec.iter().copied().peekable();
    while let Some(letter) = letters.next() {
        let depth = open.len();
        let level = open.last_mut().expect("the root is never closed");
        let index = level.children.len();
        match letter {
            b'{' | b'(' if depth <= MAX_DEPTH => {
                open.push(Field::new(
                    if letter == b'{' { "RECORD" } else { "STRUCT" },
                    index,
                ));
            }
            b'}' | b')' if depth > 1 => close(&mut open),
            b'<' => {
                let element = letters.peek().copied().and_then(scalar);
                if element.is_some() {
                    letters.next();
                }
                level.children.push(Field {
                    range_element: element,
                    ..Field::new("RANGE", index)
                });
            }
            _ => match scalar(letter) {
                Some(kind) => level.children.push(Field::new(kind, index)),
                None => {
                    if let Some(field) = level.children.last_mut() {
                        field.modify(letter);
                    }
                }
            },
        }
    }
    while open.len() > 1 {
        close(&mut open);
    }
    open.pop().expect("the root remains").children
}

fn close(open: &mut Vec<Field>) {
    let record = open.pop().expect("a RECORD is open");
    open.last_mut()
        .expect("the root remains")
        .children
        .push(record);
}

fn render_fields(fields: &[Field], out: &mut String) {
    out.push('[');
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(r#"{"name":"#);
        push_json_string(&field.name, out);
        write!(out, r#","type":"{}""#, field.kind).unwrap();
        if !field.mode.is_empty() {
            write!(out, r#","mode":"{}""#, field.mode).unwrap();
        }
        if let Some(element) = field.range_element {
            write!(out, r#","rangeElementType":{{"type":"{element}"}}"#).unwrap();
        }
        out.push_str(&field.attributes);
        if !field.children.is_empty() {
            out.push_str(r#","fields":"#);
            render_fields(&field.children, out);
        }
        out.push('}');
    }
    out.push(']');
}

fn push_json_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            control if control < ' ' => write!(out, "\\u{:04x}", u32::from(control)).unwrap(),
            other => out.push(other),
        }
    }
    out.push('"');
}

/// Renders a page of rows from fuzzer choices, returning how many rows it holds.
fn render_page(fields: &[Field], u: &mut Unstructured, out: &mut String) -> usize {
    out.push('[');
    let mut rows = 0;
    while rows < MAX_ROWS && u.bytes(1).is_ok() {
        if rows > 0 {
            out.push(',');
        }
        out.push_str(r#"{"f":["#);
        push_cells(fields, u, out);
        out.push_str("]}");
        rows += 1;
    }
    out.push(']');
    rows
}

fn push_cells(fields: &[Field], u: &mut Unstructured, out: &mut String) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(r#"{"v":"#);
        if field.mode == "REPEATED" {
            push_shaped(u, out, |u, out| {
                out.push('[');
                for index in 0..u.int_in_range(0..=4_u8).unwrap_or(0) {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(r#"{"v":"#);
                    push_shaped(u, out, |u, out| push_value(field, u, out));
                    out.push('}');
                }
                out.push(']');
            });
        } else {
            push_shaped(u, out, |u, out| push_value(field, u, out));
        }
        out.push('}');
    }
}

/// Pushes a NULL, a value of some JSON shape, or mostly the value `push` writes. An exhausted
/// input chooses NULL, which bounds the page by the input.
fn push_shaped(
    u: &mut Unstructured,
    out: &mut String,
    push: impl FnOnce(&mut Unstructured, &mut String),
) {
    match u.int_in_range(0..=31_u8).unwrap_or(0) {
        0 => out.push_str("null"),
        1 => out.push_str(one_of(
            u,
            &[
                "7",
                "-7",
                "-1.5",
                "1e400",
                "true",
                "[]",
                r#"{"f":[]}"#,
                r#""""#,
            ],
        )),
        _ => push(u, out),
    }
}

fn one_of(u: &mut Unstructured, choices: &[&'static str]) -> &'static str {
    u.choose(choices).copied().unwrap_or_default()
}

fn push_value(field: &Field, u: &mut Unstructured, out: &mut String) {
    if matches!(field.kind, "RECORD" | "STRUCT") {
        out.push_str(r#"{"f":["#);
        push_cells(&field.children, u, out);
        out.push_str("]}");
        return;
    }
    let mut text = cell_text(field.kind, field.range_element, u);
    if u.ratio(1, 16).unwrap_or(false) {
        text.push_str(CANARY);
    }
    push_json_string(&text, out);
}

/// A cell's text near the grammar and range of `kind`, or now and then any text at all.
fn cell_text(kind: &str, range_element: Option<&str>, u: &mut Unstructured) -> String {
    if u.ratio(1, 8).unwrap_or(true) {
        return u.arbitrary::<&str>().unwrap_or_default().to_owned();
    }
    match kind {
        "INTEGER" | "INT64" => near(u, i64::MIN, i64::MAX).to_string(),
        "FLOAT" | "FLOAT64" => {
            let value = u.arbitrary::<f64>().unwrap_or_default();
            match u.int_in_range(0..=3_u8).unwrap_or(0) {
                0 => value.to_string(),
                1 => format!("{value:e}"),
                _ => {
                    one_of(u, &["NaN", "Infinity", "-Infinity", "-0", "1e309", "0x1p3"]).to_owned()
                }
            }
        }
        "BOOLEAN" | "BOOL" => one_of(u, &["true", "false", "TRUE", "1"]).to_owned(),
        "TIMESTAMP" => near(u, MIN_MICROS, MAX_MICROS).to_string(),
        "DATE" => date(u),
        "TIME" => clock(u),
        "DATETIME" => format!("{}T{}", date(u), clock(u)),
        "NUMERIC" | "DECIMAL" | "BIGNUMERIC" | "BIGDECIMAL" => decimal(u),
        "INTERVAL" => {
            let sign = |u: &mut Unstructured| one_of(u, &["", "-"]);
            format!(
                "{}{}-{} {}{} {}{}:{}:{}{}",
                sign(u),
                near(u, 0, 10_000),
                near(u, 0, 11),
                sign(u),
                near(u, 0, 3_660_000),
                sign(u),
                near(u, 0, 87_840_000),
                near(u, 0, 59),
                near(u, 0, 59),
                fraction(u),
            )
        }
        "BYTES" => base64(u.arbitrary().unwrap_or_default()),
        "RANGE" => {
            let element = range_element.unwrap_or_default();
            let bound = |u: &mut Unstructured| match u.int_in_range(0..=4_u8).unwrap_or(0) {
                0 => "UNBOUNDED".to_owned(),
                1 => String::new(),
                _ => cell_text(element, None, u),
            };
            format!("[{}, {})", bound(u), bound(u))
        }
        _ => u.arbitrary::<&str>().unwrap_or_default().to_owned(),
    }
}

/// An integer at either end of `low..=high`, one past either end, inside it, or anywhere,
/// including the ends of `i64` where unchecked arithmetic overflows.
fn near(u: &mut Unstructured, low: i64, high: i64) -> i64 {
    match u.int_in_range(0..=7_u8).unwrap_or(0) {
        0 => low,
        1 => high,
        2 => low.saturating_sub(1),
        3 => high.saturating_add(1),
        4 => i64::MIN,
        5 => i64::MAX,
        6 => u.arbitrary().unwrap_or_default(),
        _ => u.int_in_range(low..=high).unwrap_or(low),
    }
}

fn date(u: &mut Unstructured) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        near(u, 1, 9_999),
        near(u, 1, 12),
        near(u, 1, 31)
    )
}

fn clock(u: &mut Unstructured) -> String {
    format!(
        "{:02}:{:02}:{:02}{}",
        near(u, 0, 23),
        near(u, 0, 59),
        near(u, 0, 59),
        fraction(u)
    )
}

fn fraction(u: &mut Unstructured) -> String {
    let digits = digits(u, 12);
    if digits.is_empty() && u.arbitrary().unwrap_or(true) {
        digits
    } else {
        format!(".{digits}")
    }
}

fn digits(u: &mut Unstructured, most: u8) -> String {
    (0..u.int_in_range(0..=most).unwrap_or(0))
        .map(|_| char::from(b'0' + u.arbitrary::<u8>().unwrap_or(0) % 10))
        .collect()
}

/// A decimal in the grammar BigQuery's NUMERIC and BIGNUMERIC cells use, plus the exponent form.
fn decimal(u: &mut Unstructured) -> String {
    let mut text = one_of(u, &["", "-", "+"]).to_owned();
    text.push_str(&digits(u, 45));
    if u.arbitrary().unwrap_or(false) {
        text.push('.');
        text.push_str(&digits(u, 45));
    }
    if u.ratio(1, 4).unwrap_or(false) {
        write!(text, "e{}", near(u, -80, 80)).unwrap();
    }
    text
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::new();
    for chunk in bytes.chunks(3) {
        let bits = chunk
            .iter()
            .zip([16, 8, 0])
            .fold(0_u32, |bits, (byte, shift)| {
                bits | u32::from(*byte) << shift
            });
        for index in 0..4 {
            text.push(if index <= chunk.len() {
                char::from(ALPHABET[(bits >> (18 - 6 * index) & 63) as usize])
            } else {
                '='
            });
        }
    }
    text
}

/// Fails on a decoded value outside its BigQuery type's range: a parser that accepts one returns a
/// wrong result instead of an error.
fn check_values(array: &dyn Array) {
    match array.data_type() {
        DataType::Date32 => in_range(
            array.as_primitive::<Date32Type>(),
            MIN_DATE_DAYS..=MAX_DATE_DAYS,
        ),
        DataType::Time64(_) => in_range(
            array.as_primitive::<Time64MicrosecondType>(),
            0..=DAY_MICROS - 1,
        ),
        DataType::Timestamp(..) => in_range(
            array.as_primitive::<TimestampMicrosecondType>(),
            MIN_MICROS..=MAX_MICROS,
        ),
        &DataType::Decimal128(precision, _) => array
            .as_primitive::<Decimal128Type>()
            .validate_decimal_precision(precision)
            .unwrap(),
        DataType::List(_) => check_values(array.as_list::<i32>().values()),
        DataType::Struct(_) => {
            for column in array.as_struct().columns() {
                check_values(column);
            }
        }
        _ => {}
    }
}

fn in_range<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, range: RangeInclusive<T::Native>)
where
    T::Native: PartialOrd + Debug,
{
    for value in array.iter().flatten() {
        assert!(range.contains(&value), "{value:?} is outside {range:?}");
    }
}

fuzz_target!(|data: &[u8]| {
    let (spec, body) = match data.iter().position(|&byte| byte == b'\n') {
        Some(end) => (&data[..end], &data[end + 1..]),
        None => (data, &[][..]),
    };
    let fields = parse_schema(spec);
    let mut schema = String::from(r#"{"fields":"#);
    render_fields(&fields, &mut schema);
    schema.push('}');

    let mut page = String::new();
    let (rows, page) = if spec.first() == Some(&b'#') {
        let rows = render_page(&fields, &mut Unstructured::new(body), &mut page);
        (Some(rows), page.as_bytes())
    } else {
        (None, body)
    };

    match adbc_driver_bigquery::fuzz_shim::rest_rows(schema.as_bytes(), page) {
        Ok(batch) => {
            let batch = batch.expect("nothing cancels the conversion");
            if let Some(rows) = rows {
                assert_eq!(batch.num_rows(), rows);
            }
            for column in batch.columns() {
                column.to_data().validate_full().unwrap();
                check_values(column);
            }
        }
        Err(message) => assert!(
            !message.contains(CANARY),
            "a failure echoes a cell: {message}"
        ),
    }
});
