//! Binding plans: parameter names and modes, and the nesting bound.

use super::*;

#[test]
fn validates_named_parameter_field_names() {
    use arrow_schema::Fields;

    use crate::parameters::validate_parameter_fields;

    fn fields(names: &[&str]) -> Fields {
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect()
    }

    // A character beyond U+FFFF, two UTF-16 code units.
    let snake = "\u{1F40D}";
    // Everything BigQuery accepts must pass: a quoted `@`identifier`` reference reaches
    // names that are not unquoted identifiers (observed, not documented).
    validate_parameter_fields(
        &fields(&[
            "my param",
            "1abc",
            "from",
            "a`b",
            "a\nb",
            "\u{fc}ber",
            &"x".repeat(127),
            // The limit counts UTF-16 code units, not bytes: 127 two-byte characters pass, and so
            // do 63 characters beyond U+FFFF and one more character.
            &"\u{fc}".repeat(127),
            &format!("{}x", snake.repeat(63)),
        ]),
        ParameterMode::Named,
    )
    .unwrap();

    let too_long = "x".repeat(128);
    let too_long_beyond_u_ffff = snake.repeat(64);
    let duplicate = "\"a\" at parameter 2 duplicates \"a\" at parameter 0";
    let case_duplicate = "\"A\" at parameter 2 duplicates \"a\" at parameter 0";
    let length = "128 characters long; it cannot exceed 127 characters";
    let refusals: [(&[&str], &[&str]); 5] = [
        (&[&too_long], &[&too_long, length]),
        // BigQuery refuses 64 such characters as too long (observed).
        (
            &[&too_long_beyond_u_ffff],
            &[
                "128 characters long, counting each character beyond U+FFFF as two; it cannot \
                 exceed 127 characters",
            ],
        ),
        (&["a", ""], &["parameter 1 has an empty field name"]),
        (&["a", "b", "a"], &[duplicate]),
        // BigQuery matches parameter names ignoring ASCII case ("Duplicate parameter name a").
        (&["a", "b", "A"], &[case_duplicate, "ignoring ASCII case"]),
    ];
    for (names, fragments) in refusals {
        let error = validate_parameter_fields(&fields(names), ParameterMode::Named).unwrap_err();
        assert_failure(&error, Status::InvalidArguments, fragments);
    }
    // Only ASCII case: BigQuery binds each of these pairs as two parameters, and `@É` does not
    // reference a parameter named `é` (observed), unlike columns and STRUCT fields, which fold
    // the full Unicode case mapping.
    for pair in [
        ["\u{e9}", "\u{c9}"],
        ["\u{df}", "SS"],
        ["\u{131}", "I"],
        ["\u{fb01}", "FI"],
        ["k", "\u{212a}"],
    ] {
        validate_parameter_fields(&fields(&pair), ParameterMode::Named).unwrap();
    }

    // Positional parameters carry no names, so nothing is validated.
    validate_parameter_fields(&fields(&["", &"x".repeat(128)]), ParameterMode::Positional).unwrap();
}

#[test]
fn rejects_deeply_nested_parameter_schemas() {
    // A list-of-struct-of-list chain 100 levels deep; the iterative depth bound must reject
    // it before the recursive parameter type builder walks it and overflows the stack.
    let mut data_type = DataType::Utf8;
    for _ in 0..50 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
        data_type = DataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    let schema = Schema::new(vec![Field::new("parameter", data_type, true)]);

    let error = ParameterPlan::try_new(&schema, ParameterMode::Positional)
        .err()
        .expect("deeply nested parameter schemas must be rejected");
    assert_failure(
        &error,
        Status::InvalidArguments,
        &["nested", &MAX_SCHEMA_NESTING_DEPTH.to_string()],
    );

    // A schema at the bound still produces a plan.
    let mut data_type = DataType::Utf8;
    for _ in 0..MAX_SCHEMA_NESTING_DEPTH - 1 {
        data_type = DataType::Struct(vec![Arc::new(Field::new("child", data_type, true))].into());
    }
    let schema = Schema::new(vec![Field::new("parameter", data_type, true)]);
    ParameterPlan::try_new(&schema, ParameterMode::Positional).unwrap();
}

/// The console link a failed job's error ends with, as `with_link` appends it.
const LINK: &str =
    " (Query: https://console.cloud.google.com/bigquery?project=p&j=bq:EU:j&page=queryresults)";

fn with_link(failure: adbc_core::error::Error) -> adbc_core::error::Error {
    let reference = crate::rest::jobs::JobReference::new("p", Some("EU"), "j".to_owned());
    crate::error::with_job_link(failure, Some(&reference))
}

/// BigQuery refuses placeholders of the other style with a bare message that never names the
/// driver's option; only the mode that sent the parameters knows which value to suggest. Sent
/// without parameters, a `?` draws the same refusal, which only binding answers.
#[test]
fn a_refused_placeholder_style_names_the_parameter_mode_to_set() {
    let refused = |message: &str| crate::error::error(message, Status::InvalidArguments);
    let named = "invalidQuery: Named parameters are not supported at [1:8] (HTTP 400)";
    let positional = "invalidQuery: Positional parameters are not supported at [1:8] (HTTP 400)";
    for (sent, message, hint) in [
        (
            Some(ParameterMode::Positional),
            named,
            "set bigquery.query.parameter_mode to \"named\"",
        ),
        (
            Some(ParameterMode::Named),
            positional,
            "set bigquery.query.parameter_mode to \"positional\"",
        ),
        (
            None,
            positional,
            "no parameters are bound to the statement: bind a value for each ? placeholder with \
             AdbcStatementBind or AdbcStatementBindStream",
        ),
    ] {
        let explained = ParameterMode::explain_refusal(sent, refused(message));
        assert_eq!(explained.status, Status::InvalidArguments);
        assert_eq!(explained.message, format!("{message}; {hint}"));
        // A failed job's console link stays last.
        let linked = ParameterMode::explain_refusal(sent, with_link(refused(message)));
        assert_eq!(linked.message, format!("{message}; {hint}{LINK}"));
    }
    // Any other failure, and a refusal the mode cannot have caused, pass through untouched.
    for (sent, message) in [
        (Some(ParameterMode::Named), named),
        (Some(ParameterMode::Positional), positional),
        (None, named),
        (
            Some(ParameterMode::Positional),
            "invalidQuery: Query parameter 'x' not found at [1:8] (HTTP 400)",
        ),
        (
            None,
            "invalidQuery: Query parameter 'x' not found at [1:8] (HTTP 400)",
        ),
    ] {
        assert_eq!(
            ParameterMode::explain_refusal(sent, refused(message)).message,
            message
        );
    }
}

/// A `None` parameter arrives as an Arrow `Null` field and binds as a STRING NULL, which BigQuery's
/// type refusals name without saying where the STRING came from; the driver names the field.
#[test]
fn a_type_refusal_names_the_untyped_null_field_it_came_from() {
    let refusal = "Value has type STRING which cannot be inserted into column a, which has type \
                   INT64 at [1:30]";
    let failure = |status| crate::error::error(refusal, status);
    let schema = |fields: Vec<Field>| Schema::new(fields);
    let untyped = schema(vec![
        Field::new("0", DataType::Int64, true),
        Field::new("1", DataType::Null, true),
    ]);
    let explained = explain_untyped_null(&untyped, failure(Status::InvalidArguments));
    assert_eq!(explained.status, Status::InvalidArguments);
    let hint = "bound parameter 1 \"1\" is an Arrow Null field, which binds as a STRING NULL: \
                where the query needs another type, CAST its placeholder to that type or give the \
                field BIGQUERY:type metadata";
    assert_eq!(explained.message, format!("{refusal}; {hint}"));
    // A failed job's console link stays last.
    let linked = explain_untyped_null(&untyped, with_link(failure(Status::InvalidArguments)));
    assert_eq!(linked.message, format!("{refusal}; {hint}{LINK}"));
    // BigQuery's message places the refused value only by its position in the SQL, so every
    // untyped field is named.
    let several = schema(vec![
        Field::new("s", DataType::Null, true),
        Field::new("i", DataType::Int64, true),
        Field::new("n", DataType::Null, true),
    ]);
    assert_eq!(
        explain_untyped_null(&several, failure(Status::InvalidArguments)).message,
        format!(
            "{refusal}; bound parameters 0 \"s\", 2 \"n\" are Arrow Null fields, which bind as \
             STRING NULLs: where the query needs another type, CAST their placeholders to that \
             type or give the fields BIGQUERY:type metadata"
        )
    );
    // An annotated Null field already says what it is, a refusal without a STRING is about
    // something else, and only a refused query is explained.
    let annotated = schema(vec![bigquery_typed(
        Field::new("a", DataType::Null, true),
        "INT64",
    )]);
    let other_refusal = "Query parameter 'x' not found at [1:8]";
    for (schema, failure) in [
        (&annotated, failure(Status::InvalidArguments)),
        (
            &untyped,
            crate::error::error(other_refusal, Status::InvalidArguments),
        ),
        (&untyped, failure(Status::IO)),
        (
            &schema(vec![Field::new("s", DataType::Utf8, true)]),
            failure(Status::InvalidArguments),
        ),
    ] {
        let message = failure.message.clone();
        assert_eq!(explain_untyped_null(schema, failure).message, message);
    }
}
