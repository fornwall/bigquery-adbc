//! Runs a query and prints the size of each result batch. Credentials come from Application
//! Default Credentials (ADC):
//!
//! ```sh
//! cargo run --locked --example query -- my-project "SELECT 1 AS n"
//! ```

use std::env;
use std::error::Error;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_bigquery::BigQueryDriver;

#[allow(clippy::print_stdout, reason = "printing the result is the example")]
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let (Some(project), Some(sql), None) = (args.next(), args.next(), args.next()) else {
        return Err("usage: query PROJECT SQL".into());
    };

    let mut driver = BigQueryDriver::try_new()?;
    let database = driver.new_database_with_opts([(
        OptionDatabase::Uri,
        OptionValue::String(format!("bigquery:///{project}")),
    )])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;

    for batch in statement.execute()? {
        let batch = batch?;
        println!(
            "{} rows x {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    Ok(())
}
