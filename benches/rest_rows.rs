//! Microbenchmark for a REST result page: decoding its JSON rows, then converting them to Arrow.

mod support;

use adbc_driver_bigquery::benchmark::{RestRowsFixture, RestRowsWidth};

use support::{print_header, run_case};

fn run_rest_rows_case(name: &str, rows: usize, width: RestRowsWidth) {
    let fixture = RestRowsFixture::new(rows, width);
    // Production runs the two stages on different threads, so they are timed separately.
    run_case(
        &format!("{name}/decode"),
        rows,
        || (),
        |()| fixture.decode(),
    );
    run_case(
        &format!("{name}/convert"),
        rows,
        || fixture.decode(),
        |input| {
            fixture
                .convert(input)
                .expect("the benchmark fixture must convert")
                .expect("an uncancelled benchmark conversion must produce a batch")
        },
    );
}

fn main() {
    print_header();
    for rows in [1, 32, 1024] {
        run_rest_rows_case("narrow", rows, RestRowsWidth::Narrow);
        run_rest_rows_case("wide", rows, RestRowsWidth::Wide);
        run_rest_rows_case("typed", rows, RestRowsWidth::Typed);
    }
}
