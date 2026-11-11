//! What the REST transport accepts from BigQuery.

use adbc_core::constants::ADBC_OPTION_VALUE_ENABLED;
use adbc_driver_bigquery::BigQueryStatementOption;
use arrow_array::Array as _;

use crate::support::{concatenated, connect, prepare_query, project, read_all, set_option, typed};

/// BigQuery's 20 MiB row limit counts a string's own bytes, but its JSON spells a control
/// character, `<` or `>` as six (`\u0001`), so one row of 20 MB arrives as a 120 MB response. The
/// first response is complete, so REST returns it even with Storage Read enabled.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn reads_a_row_whose_escaped_json_is_six_times_its_size() {
    const SQL: &str = "SELECT ARRAY(SELECT REPEAT(CODE_POINTS_TO_STRING([1]), 1000000) \
        FROM UNNEST(GENERATE_ARRAY(1, 20))) AS escaped";
    let mut connection = connect(project());
    let mut statement = prepare_query(&mut connection, SQL);
    set_option(
        &mut statement,
        BigQueryStatementOption::UseStorageApiDisabledClient,
        ADBC_OPTION_VALUE_ENABLED,
    );
    let (schema, batches) = read_all(&mut statement);
    let columns = concatenated(&schema, &batches);
    let rows = typed::<arrow_array::ListArray>(columns[0].as_ref());
    assert_eq!(rows.len(), 1);
    let row = rows.value(0);
    let values = typed::<arrow_array::StringArray>(row.as_ref());
    assert_eq!(values.len(), 20);
    for value in values {
        let value = value.unwrap();
        assert_eq!(value.len(), 1_000_000);
        assert!(value.bytes().all(|byte| byte == 1));
    }
}
