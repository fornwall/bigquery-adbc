//! Query result scheduling and allocation overhead, with no HTTP or server work.

mod support;

use adbc_driver_bigquery::benchmark::{QueryRequestFixture, QueryResultFixture};
use support::{print_header, run_case};

fn main() {
    print_header();
    for sql_bytes in [65, 1024, 65536] {
        let fixture = QueryRequestFixture::new(sql_bytes);
        run_case(
            &format!("query/request-{sql_bytes}"),
            0,
            || (),
            |()| fixture.encode(),
        );
    }
    let fixture = QueryResultFixture::new(0);
    run_case("query/client", 0, || (), |()| fixture.job_client());
    for rows in [0, 1, 32, 1024] {
        let fixture = QueryResultFixture::new(rows);
        run_case("query/decode", rows, || (), |()| fixture.decode());
        run_case(
            "query/read",
            rows,
            || fixture.decode(),
            |input| fixture.read(input),
        );
        run_case(
            "query/complete",
            rows,
            || (),
            |()| fixture.read(fixture.decode()),
        );
    }
}
