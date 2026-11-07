//! A synthetic BigQuery REST result page, decoded and converted as the query path does.

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::pipeline::ResultPipelineCancellation;
use crate::rest::model::{TableFieldSchema, TableSchema};
use crate::rows::{RestRow, RestRows};

/// The column shape of a synthetic BigQuery REST result.
#[derive(Clone, Copy, Debug)]
pub enum RestRowsWidth {
    /// One integer and one short string column.
    Narrow,
    /// Eight integer and eight short string columns.
    Wide,
    /// One column of every REST type whose decoding does real per-cell work.
    ///
    /// INTEGER and STRING cost a move and a memcpy, so the narrow and wide shapes measure the
    /// builder machinery rather than any decoder. This shape measures the decoders: the temporal
    /// parsers, the two decimal parsers, base64 BYTES, a nested STRUCT and a repeated INT64.
    Typed,
}

/// One synthetic BigQuery REST result page, held as the JSON bytes the service answers with.
///
/// The page is the `rows` array of a `jobs.getQueryResults` response in BigQuery's spelling
/// (`{"f":[{"v":...}]}` per row, every scalar a JSON string), so [`decode`](Self::decode) and
/// [`convert`](Self::convert) are measured over realistic bytes. The envelope's other fields are
/// omitted because only the row array's cost scales with the result.
#[derive(Debug)]
pub struct RestRowsFixture {
    schema: SchemaRef,
    page: Vec<u8>,
}

/// The rows one page decode produced, consumed by one conversion iteration.
#[derive(Debug)]
pub struct RestRowsInput(Vec<RestRow>);

impl RestRowsFixture {
    /// Renders the page's JSON bytes once, outside any measured region.
    pub fn new(row_count: usize, width: RestRowsWidth) -> Self {
        let (fields, row) = match width {
            RestRowsWidth::Narrow => integer_and_string_columns(2),
            RestRowsWidth::Wide => integer_and_string_columns(16),
            RestRowsWidth::Typed => typed_columns(),
        };
        let schema = crate::types::schema::convert_schema(&TableSchema {
            fields,
            ..Default::default()
        })
        .expect("the static REST benchmark schema is valid");
        Self {
            schema,
            page: render_page(&row, row_count),
        }
    }

    /// Decodes the page's row array, the stage that dominates a REST result page's CPU cost.
    pub fn decode(&self) -> RestRowsInput {
        RestRowsInput(
            // As `QueryResponse::decode` does: validate the page once, then decode a `&str`.
            serde_json::from_str::<RestRows>(
                std::str::from_utf8(&self.page).expect("the benchmark page is UTF-8"),
            )
            .expect("the static REST benchmark page is valid BigQuery result JSON")
            .into_vec(),
        )
    }

    /// Converts one decoded page into an Arrow record batch, as the conversion tasks do.
    pub fn convert(&self, input: RestRowsInput) -> Result<Option<RecordBatch>, ArrowError> {
        crate::rows::rest_rows_to_record_batch(
            input.0,
            0,
            self.schema.clone(),
            &ResultPipelineCancellation::new(None),
        )
    }
}

/// One cell of a REST row template, in the `v`-wrapped spelling BigQuery answers with.
#[derive(Debug)]
enum Cell {
    /// A scalar, which BigQuery always sends as a JSON string.
    Scalar(&'static str),
    /// A nested RECORD, which repeats the row's own `f` envelope.
    Record(Vec<Self>),
    /// A REPEATED field, an array of `v`-wrapped elements.
    Repeated(Vec<Self>),
}

impl Cell {
    fn render(&self, out: &mut String) {
        out.push_str(r#"{"v":"#);
        match self {
            Self::Scalar(text) => out.push_str(
                &serde_json::to_string(text).expect("a static benchmark string renders as JSON"),
            ),
            Self::Record(cells) => {
                out.push_str(r#"{"f":"#);
                render_cells(cells, out);
                out.push('}');
            }
            Self::Repeated(cells) => render_cells(cells, out),
        }
        out.push('}');
    }
}

fn render_cells(cells: &[Cell], out: &mut String) {
    out.push('[');
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        cell.render(out);
    }
    out.push(']');
}

/// Renders `row_count` copies of one row template as a result page's JSON `rows` array.
fn render_page(row: &[Cell], row_count: usize) -> Vec<u8> {
    let mut rendered_row = String::new();
    rendered_row.push_str(r#"{"f":"#);
    render_cells(row, &mut rendered_row);
    rendered_row.push('}');

    let mut page = String::with_capacity(row_count * (rendered_row.len() + 1) + 2);
    page.push('[');
    for index in 0..row_count {
        if index > 0 {
            page.push(',');
        }
        page.push_str(&rendered_row);
    }
    page.push(']');
    page.into_bytes()
}

fn required(name: &str, kind: &str) -> TableFieldSchema {
    TableFieldSchema {
        name: name.to_owned(),
        r#type: kind.to_owned(),
        mode: "REQUIRED".to_owned(),
        ..Default::default()
    }
}

/// Alternating INTEGER and STRING columns, and the one row template that fills them.
fn integer_and_string_columns(column_count: usize) -> (Vec<TableFieldSchema>, Vec<Cell>) {
    let fields = (0..column_count)
        .map(|index| {
            let kind = if index.is_multiple_of(2) {
                "INTEGER"
            } else {
                "STRING"
            };
            required(&format!("column_{index}"), kind)
        })
        .collect();
    let row = (0..column_count)
        .map(|index| {
            // BigQuery sends an INT64 as a JSON string like every other scalar, so both columns
            // reach the decoder as strings and only the builder they feed differs.
            Cell::Scalar(if index.is_multiple_of(2) {
                "42"
            } else {
                "small REST value"
            })
        })
        .collect();
    (fields, row)
}

/// One column per REST decoder that does real per-cell work, and the row template that fills them.
///
/// Every value is spelled the way BigQuery's REST rows spell it on the wire: scalars are JSON
/// strings, a REPEATED field is an array of `v`-wrapped elements, and a RECORD repeats the row's
/// positional `f` envelope.
fn typed_columns() -> (Vec<TableFieldSchema>, Vec<Cell>) {
    let fields = vec![
        required("timestamp_value", "TIMESTAMP"),
        required("date_value", "DATE"),
        required("time_value", "TIME"),
        required("datetime_value", "DATETIME"),
        required("numeric_value", "NUMERIC"),
        required("bignumeric_value", "BIGNUMERIC"),
        required("bytes_value", "BYTES"),
        required("interval_value", "INTERVAL"),
        TableFieldSchema {
            fields: vec![
                required("id", "INTEGER"),
                required("label", "STRING"),
                required("weight", "NUMERIC"),
            ],
            ..required("struct_value", "RECORD")
        },
        TableFieldSchema {
            mode: "REPEATED".to_owned(),
            ..required("array_value", "INTEGER")
        },
    ];
    let row = vec![
        Cell::Scalar("1593676800123456"),
        Cell::Scalar("2020-07-02"),
        Cell::Scalar("12:34:56.789012"),
        Cell::Scalar("2020-07-02T12:34:56.789012"),
        Cell::Scalar("12345.678901"),
        Cell::Scalar("12345678901234567890.12345678901234567890"),
        // Sixteen bytes, so the base64 decode carries padding like a real BYTES cell.
        Cell::Scalar("c21hbGwgUkVTVCBieXRlcyE="),
        Cell::Scalar("1-2 3 4:05:06.789123"),
        Cell::Record(vec![
            Cell::Scalar("42"),
            Cell::Scalar("small REST value"),
            Cell::Scalar("0.500000"),
        ]),
        Cell::Repeated(["0", "1", "2", "3"].into_iter().map(Cell::Scalar).collect()),
    ];
    (fields, row)
}
