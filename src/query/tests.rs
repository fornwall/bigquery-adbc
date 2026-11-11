use adbc_core::Statement as _;
use serde_json::json;

use crate::test_support::driver::test_query_at;
use crate::test_support::http::{capture_requests, completed_job};

/// adbc.h's `rows_affected` is a count or -1 for unknown, so a negative `numDmlAffectedRows`
/// from the wire is reported as unknown, through `AdbcStatementExecuteQuery` and through
/// `ExecuteUpdate` alike, rather than passed on as some other negative.
#[test]
fn a_negative_dml_count_is_reported_as_unknown() {
    let answer = || {
        completed_job(
            "dml-job",
            json!({"statementType": "DELETE", "numDmlAffectedRows": "-5"}),
        )
    };
    let (endpoint, requests) = capture_requests(vec![answer(), answer()]);
    let mut statement = test_query_at(&endpoint, "DELETE FROM dataset.table WHERE true");

    let (_, rows_affected) = statement.execute_with_rows_affected().unwrap();
    assert_eq!(rows_affected, -1);
    assert_eq!(statement.execute_update().unwrap(), None);
    requests.served(2);
}
