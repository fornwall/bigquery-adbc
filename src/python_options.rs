//! Checked-source generation for the Python option enums.
//!
//! This module is absent from normal driver builds. Every member comes from a compiled
//! `string_enum!` declaration or a standard ADBC option, so the checked-in Python source never
//! becomes an independent source of wire strings or documentation.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement};

use crate::options::keys::OPTION_BIND_BY_NAME;
use crate::parameters::ParameterMode;
use crate::{
    BigQueryAuthenticationType, BigQueryBulkIngestMethod, BigQueryConnectionOption,
    BigQueryCreateDisposition, BigQueryCredentialsType, BigQueryDatabaseOption,
    BigQueryQueryPriority, BigQueryStatementOption, BigQueryWriteDisposition,
};

const GENERATED_PATH: &str = "python/src/bigquery_adbc/_options.py";

/// The width a rendered `#:` documentation line is wrapped to.
///
/// Below ruff's 120-column limit, which the longest rendered member line (108 columns)
/// approaches; prose reads better narrower.
const DOC_LINE_WIDTH: usize = 96;

/// A `string_enum!` value's Rust name, canonical spelling and documentation.
type Declared = (&'static str, &'static str, &'static str);

/// A Python enum member's name, documentation and wire value. The documentation is rendered as a
/// Sphinx/IDE `#:` comment, the convention the upstream ADBC Python drivers use.
type Member = (String, String, String);

struct PythonEnum {
    name: &'static str,
    doc: &'static str,
    members: Vec<Member>,
}

/// Python spells a Rust variant's name in screaming snake case.
fn screaming_snake(variant: &str) -> String {
    let mut name = String::new();
    for character in variant.chars() {
        if character.is_ascii_uppercase() && !name.is_empty() {
            name.push('_');
        }
        name.push(character.to_ascii_uppercase());
    }
    name
}

/// Python's enums drop the Rust types' `BigQuery` prefix, so their documentation does too.
fn member(name: String, doc: &str, value: &str) -> Member {
    (name, doc.replace("`BigQuery", "`"), value.to_owned())
}

/// An enum of standard ADBC options followed by the exposed driver options. Only dbt-core 2.0 sets
/// the `adbc.bigquery.*` operations and the dataset location lookup, so Python names neither.
fn options<K: AsRef<str>>(
    name: &'static str,
    doc: &'static str,
    standard: impl IntoIterator<Item = (&'static str, &'static str, K)>,
    declared: &[Declared],
) -> PythonEnum {
    let standard = standard
        .into_iter()
        .map(|(name, doc, key)| member(name.to_owned(), doc, key.as_ref()));
    let exposed = declared
        .iter()
        .filter(|(_, value, _)| {
            !value.starts_with("adbc.bigquery.")
                && *value != BigQueryStatementOption::GetDatasetLocation.as_str()
        })
        .map(|(variant, value, doc)| {
            let prefix = if value.starts_with("bigquery.query.") {
                "QUERY_"
            } else {
                ""
            };
            member(format!("{prefix}{}", screaming_snake(variant)), doc, value)
        });
    PythonEnum {
        name,
        doc,
        members: standard.chain(exposed).collect(),
    }
}

/// An enum of option values, each named without the `redundant` prefix its enum's name implies.
fn values(
    name: &'static str,
    doc: &'static str,
    declared: &[Declared],
    redundant: &str,
) -> PythonEnum {
    let members = declared
        .iter()
        .map(|(variant, value, doc)| {
            let name = screaming_snake(variant);
            let name = name.strip_prefix(redundant).unwrap_or(&name).to_owned();
            member(name, doc, value)
        })
        .collect();
    PythonEnum { name, doc, members }
}

fn python_enums() -> Vec<PythonEnum> {
    vec![
        options(
            "DatabaseOptions",
            "Options accepted while initializing an ADBC database.",
            [(
                "URI",
                "The standard ADBC connection URI, `bigquery:///<project>?<key>=<value>`. Optional: \
                 every setting a URI carries also has an option of its own.",
                OptionDatabase::Uri,
            )],
            BigQueryDatabaseOption::DECLARED,
        ),
        options(
            "ConnectionOptions",
            "Options accepted by an ADBC connection.",
            [
                (
                    "AUTOCOMMIT",
                    "Standard ADBC boolean string. With `false`, the first query that needs a \
                     session opens one for the transaction, kept until autocommit is re-enabled or \
                     the connection is released.",
                    OptionConnection::AutoCommit,
                ),
                (
                    "CURRENT_CATALOG",
                    "Standard ADBC current catalog: the project unqualified names resolve in. A \
                     connection also reports it under `DatabaseOptions.PROJECT_ID`.",
                    OptionConnection::CurrentCatalog,
                ),
                (
                    "CURRENT_DB_SCHEMA",
                    "Standard ADBC current schema: the dataset unqualified names resolve in. A \
                     connection also reports it under `DatabaseOptions.DATASET_ID`.",
                    OptionConnection::CurrentSchema,
                ),
                (
                    "READ_ONLY",
                    "Standard ADBC boolean string. Only `false` is supported; use IAM permissions \
                     to restrict writes.",
                    OptionConnection::ReadOnly,
                ),
                (
                    "ISOLATION_LEVEL",
                    "Standard ADBC transaction isolation level. Accepts \
                     `adbc.connection.transaction.isolation.default` or \
                     `adbc.connection.transaction.isolation.snapshot`; always reports the latter.",
                    OptionConnection::IsolationLevel,
                ),
            ],
            BigQueryConnectionOption::DECLARED,
        ),
        options(
            "StatementOptions",
            "Options accepted by an ADBC statement.",
            [
                (
                    "BIND_BY_NAME",
                    "Standard ADBC boolean string, and the portable spelling of \
                     `QUERY_PARAMETER_MODE`: `true` binds parameters by the bound stream's field \
                     names, `false` by position. The two options read each other back.",
                    OptionStatement::Other(OPTION_BIND_BY_NAME.to_owned()),
                ),
                (
                    "INGEST_MODE",
                    "Standard ADBC bulk-ingest mode: whether the target table is created, appended \
                     to, replaced, or created and appended to.",
                    OptionStatement::IngestMode,
                ),
                (
                    "INGEST_TARGET_TABLE",
                    "Standard ADBC bulk-ingest target table name.",
                    OptionStatement::TargetTable,
                ),
                (
                    "INGEST_TARGET_CATALOG",
                    "Standard ADBC bulk-ingest target catalog: the BigQuery project.",
                    OptionStatement::TargetCatalog,
                ),
                (
                    "INGEST_TARGET_DB_SCHEMA",
                    "Standard ADBC bulk-ingest target schema: the BigQuery dataset.",
                    OptionStatement::TargetDbSchema,
                ),
                (
                    "INGEST_TEMPORARY",
                    "Standard ADBC boolean string. Only `false` is supported: bulk ingest writes \
                     into a named table, never a temporary one.",
                    OptionStatement::Temporary,
                ),
                (
                    "INCREMENTAL",
                    "Standard ADBC boolean string. Only `false` is supported: execution returns \
                     all partitions together.",
                    OptionStatement::Incremental,
                ),
                (
                    "PROGRESS",
                    "Read-only double: processed bytes reported at query response boundaries. \
                     Read with `get_option_float(member.value)`.",
                    OptionStatement::Progress,
                ),
                (
                    "MAX_PROGRESS",
                    "Read-only double: final processed bytes, or `0.0` while unknown. Read with \
                     `get_option_float(member.value)`.",
                    OptionStatement::MaxProgress,
                ),
            ],
            BigQueryStatementOption::DECLARED,
        ),
        values(
            "ParameterMode",
            "BigQuery query parameter binding modes.",
            ParameterMode::DECLARED,
            "",
        ),
        values(
            "BulkIngestMethod",
            "Supported BigQuery bulk-ingest implementations.",
            BigQueryBulkIngestMethod::DECLARED,
            "",
        ),
        values(
            "AuthenticationType",
            "Supported Google Cloud authentication modes.",
            BigQueryAuthenticationType::DECLARED,
            "",
        ),
        values(
            "CredentialsType",
            "Supported Google Cloud JSON credential document types.",
            BigQueryCredentialsType::DECLARED,
            "",
        ),
        values(
            "QueryPriority",
            "BigQuery query scheduling priorities.",
            BigQueryQueryPriority::DECLARED,
            "",
        ),
        values(
            "CreateDisposition",
            "BigQuery destination-table creation behavior.",
            BigQueryCreateDisposition::DECLARED,
            "CREATE_",
        ),
        values(
            "WriteDisposition",
            "BigQuery destination-table write behavior.",
            BigQueryWriteDisposition::DECLARED,
            "WRITE_",
        ),
    ]
}

/// Render a Python string literal, asserting `{:?}` is a faithful rendering of it.
///
/// Rust's `Debug` escaping is not Python's (`char::escape_debug` emits `\u{a0}`-style escapes),
/// but the two agree on the ASCII identifier-like strings every option name and value is. The
/// assertion keeps a future non-ASCII value from producing an invalid module.
fn python_literal(value: &str) -> String {
    assert!(
        !value.is_empty()
            && value
                .chars()
                .all(|character| character.is_ascii_alphanumeric()
                    || character == '.'
                    || character == '_'),
        "{value:?} cannot be rendered as a Python string literal with Rust's Debug escaping"
    );
    format!("{value:?}")
}

/// Wrap one member's documentation into `#:` comment lines at a fixed indent.
///
/// A word longer than the budget goes on a line of its own rather than being split: the long
/// tokens here are URLs and URNs, and breaking one would make it uncopyable.
fn doc_comment_lines(doc: &str, indent: &str) -> Vec<String> {
    let prefix = format!("{indent}#: ");
    let budget = DOC_LINE_WIDTH.saturating_sub(prefix.len());
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in doc.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.chars().count() + 1 + word.chars().count() <= budget {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(format!("{prefix}{current}"));
            current = word.to_owned();
        }
    }
    if !current.is_empty() {
        lines.push(format!("{prefix}{current}"));
    }
    lines
}

/// Render the complete checked-in Python option module deterministically.
pub fn render() -> String {
    let enumerations = python_enums();
    let mut output = String::from(
        r#""""Typed option names and values supported by the BigQuery driver.

Generated from compiled Rust option metadata; run
``cargo run --locked --features python-option-codegen --bin generate-python-options``
after changing an option declaration. Do not edit this file manually.

Every enum is a string: dictionary keys, option values, ``str()``, and formatting
all use its wire value on every supported Python version.
"""

from __future__ import annotations

import enum

"#,
    );

    for enumeration in &enumerations {
        writeln!(
            output,
            "\nclass {}(enum.StrEnum):\n    \"\"\"{}\"\"\"",
            enumeration.name, enumeration.doc
        )
        .expect("writing to a String is infallible");
        for (name, doc, value) in &enumeration.members {
            output.push('\n');
            for line in doc_comment_lines(doc, "    ") {
                writeln!(output, "{line}").expect("writing to a String is infallible");
            }
            writeln!(output, "    {name} = {}", python_literal(value))
                .expect("writing to a String is infallible");
        }
        output.push('\n');
    }

    // Derived from the rendered enums rather than restated, so a new enum cannot be exported
    // from the module and missing from `__all__`.
    let mut exported = enumerations
        .iter()
        .map(|enumeration| enumeration.name)
        .collect::<Vec<_>>();
    exported.sort_unstable();
    output.push_str("\n__all__ = [\n");
    for name in exported {
        writeln!(output, "    {},", python_literal(name))
            .expect("writing to a String is infallible");
    }
    output.push_str("]\n");
    output
}

/// Return the repository path of the checked-in generated module.
pub fn generated_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(GENERATED_PATH)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{DOC_LINE_WIDTH, generated_path, python_enums, render};

    #[test]
    fn checked_python_options_match_compiled_rust_metadata() {
        let path = generated_path();
        let checked = fs::read_to_string(&path).unwrap();
        assert_eq!(
            checked,
            render(),
            "{} is stale; run `cargo run --locked --features python-option-codegen --bin generate-python-options`",
            path.display()
        );
    }

    /// Wrapping leaves only a single over-long word able to overflow, which ruff
    /// (`line-length = 120`) would otherwise catch only in a separate job.
    #[test]
    fn rendered_documentation_comments_stay_within_the_line_budget() {
        for line in render().lines() {
            if line.trim_start().starts_with("#:") {
                assert!(
                    line.chars().count() <= DOC_LINE_WIDTH,
                    "documentation line exceeds {DOC_LINE_WIDTH} columns: {line}"
                );
            }
        }
    }

    /// Exposed members share their Rust documentation, so it must exist and must not lean on
    /// rustdoc links, which Python would show as raw markup.
    #[test]
    fn python_members_are_unique_and_documented_in_plain_text() {
        for enumeration in python_enums() {
            let mut names = std::collections::HashSet::new();
            let mut values = std::collections::HashSet::new();
            for (name, doc, value) in &enumeration.members {
                let member = format!("{}.{name}", enumeration.name);
                assert!(names.insert(name), "{member}");
                assert!(values.insert(value), "{member}");
                assert!(!doc.trim().is_empty(), "{member} has no documentation");
                assert!(!doc.contains("[`"), "{member} has a rustdoc link: {doc}");
            }
        }
    }
}
