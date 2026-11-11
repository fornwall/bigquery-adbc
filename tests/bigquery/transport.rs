//! What the REST transport accepts from BigQuery.

use std::time::{Duration, Instant};

use adbc_core::constants::ADBC_OPTION_VALUE_ENABLED;
use adbc_core::{Database as _, Driver as _};
use adbc_driver_bigquery::{BigQueryDatabaseOption, BigQueryDriver, BigQueryStatementOption};
use arrow_array::Array as _;

use crate::support::{
    concatenated, connect, db_option, prepare_query, project, read_all, row_count, set_option,
    typed,
};

/// BigQuery's 20 MiB row limit counts a string's own bytes, but its JSON spells a control
/// character, `<` or `>` as six (`\u0001`), so one row of 20 MB arrives as a 120 MB response.
/// Storage Read is disabled, so REST reads that whole response.
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

/// A database opened after another on the same driver closed takes the REST connection pool it
/// left, so its first query skips the TCP and TLS handshakes; the rest of its first-query penalty
/// is its credential's first token. Prints the median penalty of the databases after the first,
/// which `docs/PERFORMANCE.md` (Sequential databases) records: run with `--nocapture`.
#[test]
#[ignore = "requires Google Cloud ADC and access to the integration project"]
fn sequential_databases_measurement() {
    let mut driver = BigQueryDriver::try_new().unwrap();
    let mut penalties = Vec::new();
    for round in 0..12 {
        let database = driver
            .new_database_with_opts(
                [db_option(BigQueryDatabaseOption::ProjectId, project())]
                    .into_iter()
                    .chain(crate::backend::database_options().unwrap()),
            )
            .unwrap();
        let mut connection = database.new_connection().unwrap();
        let mut elapsed = [Duration::ZERO; 2];
        for query in &mut elapsed {
            let started = Instant::now();
            let mut statement = prepare_query(&mut connection, "SELECT 1 AS n");
            let (_, batches) = read_all(&mut statement);
            assert_eq!(row_count(&batches), 1);
            *query = started.elapsed();
        }
        if round > 0 {
            penalties.push(elapsed[0].saturating_sub(elapsed[1]));
        }
    }
    penalties.sort_unstable();
    eprintln!(
        "a later database's first SELECT 1 minus its second, median of {}: {:?}",
        penalties.len(),
        penalties[penalties.len() / 2]
    );
}
