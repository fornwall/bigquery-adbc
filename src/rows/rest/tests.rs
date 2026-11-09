use super::*;

fn decode_row(json: &str) -> serde_json::Result<RestRow> {
    let page: RestRows = serde_json::from_str(&format!("[{json}]"))?;
    Ok(page.into_vec().remove(0))
}

fn decode(json: &str) -> RestRow {
    decode_row(json).unwrap()
}

/// Unknown envelope fields are skipped without disturbing positions.
#[test]
fn decodes_scalars_nulls_and_empty_composites() {
    for (json, cells) in [
        (
            r#"{"f":[{"v":"text"},{"v":true},{"v":-7},{"v":9},{"v":1.25},{"v":null}]}"#,
            vec![
                RestValue::string("text"),
                RestValue::Boolean(true),
                RestValue::Integer(-7),
                RestValue::Unsigned(9),
                RestValue::Float(1.25),
                RestValue::Null,
            ],
        ),
        (r#"{"f":[]}"#, Vec::new()),
        (
            r#"{"f":[{"v":[]},{"v":{"f":[]}}]}"#,
            vec![
                RestValue::Repeated(Vec::new()),
                RestValue::Record(Vec::new()),
            ],
        ),
        (
            r#"{"before":{"large":[1,2]},"f":[{"ignored":"x","v":"ok"}],"after":7}"#,
            vec![RestValue::string("ok")],
        ),
    ] {
        assert_eq!(decode(json), RestRow::new(cells), "{json}");
    }
}

#[test]
fn decodes_nested_repeated_and_positional_records() {
    let row = decode(
        r#"{
            "kind":"bigquery#tableRow",
            "f":[
                {"v":[{"v":"a"},{"v":null},{"v":"b"}]},
                {"v":{"f":[
                    {"v":"first anonymous"},
                    {"v":{"f":[{"v":"nested"}]}},
                    {"v":[{"v":{"f":[{"v":"1"},{"v":"x"}]}}]}
                ]}}
            ]
        }"#,
    );
    assert_eq!(
        row,
        RestRow::new(vec![
            RestValue::Repeated(vec![
                RestValue::string("a"),
                RestValue::Null,
                RestValue::string("b"),
            ]),
            RestValue::Record(vec![
                RestValue::string("first anonymous"),
                RestValue::Record(vec![RestValue::string("nested")]),
                RestValue::Repeated(vec![RestValue::Record(vec![
                    RestValue::string("1"),
                    RestValue::string("x"),
                ])]),
            ]),
        ])
    );
}

/// One row's width sizes the next row's cells; rows of differing widths check that the width is
/// only a hint.
#[test]
fn a_page_decodes_rows_of_every_width_after_any_other_width() {
    let page: RestRows = serde_json::from_str(
        r#"[
            {"f":[{"v":"a"},{"v":"b"},{"v":"c"}]},
            {"f":[{"v":"d"}]},
            {"f":[]},
            {"f":[{"v":"e"},{"v":"f"}]}
        ]"#,
    )
    .unwrap();
    let widths = page
        .into_vec()
        .into_iter()
        .map(|row| {
            row.into_cells()
                .into_iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        widths,
        vec![
            vec!["a", "b", "c"],
            vec!["d"],
            Vec::<&str>::new(),
            vec!["e", "f"],
        ]
    );
}

#[test]
fn rejects_missing_duplicate_and_malformed_envelopes() {
    for (json, expected) in [
        (r#"{}"#, "row is missing field `f`"),
        (r#"{"f":null}"#, "`f` field is not a JSON array"),
        (r#"{"f":[{}]}"#, "missing field `v`"),
        (r#"{"f":[{"v":1,"v":2}]}"#, "duplicate field `v`"),
        (r#"{"f":[],"f":[]}"#, "duplicate field `f`"),
        (r#"{"f":[{"v":{}}]}"#, "record is missing field `f`"),
        (r#"{"f":[{"v":[{}]}]}"#, "missing field `v`"),
        (r#"[]"#, "row is not a JSON object"),
        (r#"{"f":["not a cell"]}"#, "cell is not a JSON object"),
    ] {
        let message = decode_row(json).unwrap_err().to_string();
        assert!(message.contains(expected), "{json}: {message}");
    }
}

#[test]
fn decoder_errors_never_echo_cell_contents() {
    let secret = "server-secret-that-must-not-cross-the-ffi-boundary";
    for json in [format!(r#""{secret}""#), format!(r#"{{"f":["{secret}"]}}"#)] {
        let message = decode_row(&json).unwrap_err().to_string();
        assert!(!message.contains(secret), "value was echoed: {message}");
    }
    let message = serde_json::from_str::<RestRows>(&format!(r#""{secret}""#))
        .unwrap_err()
        .to_string();
    assert!(!message.contains(secret), "value was echoed: {message}");
    assert!(
        message.contains("`rows` field is not a JSON array"),
        "{message}"
    );
}

#[test]
fn deeply_nested_values_fail_at_the_json_recursion_limit() {
    let mut value = "null".to_owned();
    for _ in 0..256 {
        value = format!(r#"[{{"v":{value}}}]"#);
    }
    let json = format!(r#"{{"f":[{{"v":{value}}}]}}"#);
    let message = decode_row(&json).unwrap_err().to_string();
    assert!(message.contains("recursion limit exceeded"), "{message}");
}

/// The inline string rides in space the heap variants already occupy; growing it past that would
/// grow every cell of every page.
#[test]
fn a_rest_value_is_no_larger_than_its_heap_variants_require() {
    assert_eq!(size_of::<RestValue>(), 32);
}

#[test]
fn retained_bytes_include_nested_allocations_and_spare_capacity() {
    let mut text = String::with_capacity(128);
    text.push_str("short but heap allocated");
    let text_bytes = text.capacity();
    let mut repeated = Vec::with_capacity(8);
    repeated.push(RestValue::String(text));
    repeated.push(RestValue::string("inline"));
    let repeated_bytes = repeated.capacity() * size_of::<RestValue>();
    let mut record = Vec::with_capacity(4);
    record.push(RestValue::Repeated(repeated));
    let record_bytes = record.capacity() * size_of::<RestValue>();
    let mut cells = Vec::with_capacity(16);
    cells.push(RestValue::Record(record));
    cells.push(RestValue::Null);
    let cell_bytes = cells.capacity() * size_of::<RestValue>();

    assert_eq!(
        RestRow::new(cells).allocated_bytes(),
        cell_bytes + record_bytes + repeated_bytes + text_bytes,
    );
    assert_eq!(RestRow::new(Vec::new()).allocated_bytes(), 0);
}

#[test]
fn inline_and_heap_strings_preserve_utf8_escapes_and_boundaries() {
    for text in [
        String::new(),
        "x".repeat(INLINE_CAPACITY - 1),
        "x".repeat(INLINE_CAPACITY),
        "x".repeat(INLINE_CAPACITY + 1),
        "é".repeat(INLINE_CAPACITY / 2),
        format!("{}x", "é".repeat(INLINE_CAPACITY / 2)),
        "é".repeat(INLINE_CAPACITY / 2 + 1),
        "🦀".repeat(INLINE_CAPACITY / 4 + 1),
        "\0\n\r\t\"\\".to_owned(),
        "a long string with escaped \"quotes\" and 🦀".to_owned(),
    ] {
        let value = {
            let wire = serde_json::to_string(&text).unwrap();
            serde_json::from_str::<RestValue>(&wire).unwrap()
        };
        assert_eq!(value.as_str(), Some(text.as_str()));
        assert_eq!(value.kind(), "a string");
        assert_eq!(
            matches!(value, RestValue::ShortString { .. }),
            text.len() <= INLINE_CAPACITY
        );
    }
    let value: RestValue = serde_json::from_str(r#""\ud83e\udd80\u0000""#).unwrap();
    assert_eq!(value.as_str(), Some("🦀\0"));
}
