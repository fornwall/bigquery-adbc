//! Driver and database objects: their lifecycle, and the clients a database hands out.
//!
//! The option surface lives in [`options`] and URI parsing in [`uri`].

mod options;
mod uri;

use std::sync::{Arc, OnceLock};

use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};

use crate::clients::ClientState;
use crate::config::DatabaseConfig;
use crate::config::secret::Secret;
use crate::connection::BigQueryConnection;
use crate::runtime::{SharedRuntime, new_runtime};

#[cfg(feature = "ffi")]
pub(crate) use options::database_option_kind;

/// The entry point for the BigQuery ADBC driver.
///
/// Each driver owns one Tokio multi-thread runtime, shared by every object descended from it, so
/// the C entry points keep a single driver for the whole process.
///
/// There is no `Default`: building the runtime can fail, and [`try_new`](Self::try_new) reports
/// that instead of panicking across the C ABI. A clone shares the runtime.
#[derive(Clone, Debug)]
pub struct BigQueryDriver {
    runtime: SharedRuntime,
}

impl BigQueryDriver {
    /// Create a driver and its shared Tokio runtime.
    pub fn try_new() -> Result<Self> {
        crate::diag::init();
        Ok(Self {
            runtime: new_runtime()?,
        })
    }

    /// The runtime for a new database. A `fork` child inherits the C ABI's process-wide driver
    /// without the runtime's workers, so each database it opens starts a runtime of its own.
    fn runtime(&mut self) -> Result<SharedRuntime> {
        if self.runtime.orphaned() {
            self.runtime = new_runtime()?;
        }
        Ok(self.runtime.clone())
    }
}

impl Driver for BigQueryDriver {
    type DatabaseType = BigQueryDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(BigQueryDatabase::new(self.runtime()?))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = BigQueryDatabase::new(self.runtime()?);
        // The URI goes first so explicit options override it whatever their order; of several
        // URIs the last wins, as repeated set_option calls would.
        let mut uri = None;
        let mut explicit = Vec::new();
        for (key, value) in opts {
            match key {
                OptionDatabase::Uri => uri = Some(value),
                key => explicit.push((key, value)),
            }
        }
        if let Some(uri) = uri {
            database.set_option(OptionDatabase::Uri, uri)?;
        }
        for (key, value) in explicit {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

/// An ADBC database: BigQuery configuration and one generation of lazily-created clients.
///
/// Connections share the generation's clients and connection pools. A client-affecting option
/// discards the generation, and the next connection builds a new one from the configuration;
/// connections already opened keep their internally consistent prior generation.
#[derive(Debug)]
pub struct BigQueryDatabase {
    runtime: SharedRuntime,
    config: DatabaseConfig,
    clients: OnceLock<Arc<ClientState>>,
    /// The last `uri` the caller set, with its credential values redacted so the standard option
    /// can be read back. Still a [`Secret`], so a parameter that escaped redaction cannot reach a
    /// log through `Debug`.
    uri: Option<Secret>,
}

impl BigQueryDatabase {
    pub(crate) fn new(runtime: SharedRuntime) -> Self {
        Self {
            runtime,
            config: DatabaseConfig::default(),
            clients: OnceLock::new(),
            uri: None,
        }
    }

    fn clients(&self) -> &Arc<ClientState> {
        self.clients
            .get_or_init(|| ClientState::for_database(&self.config, &self.runtime))
    }

    /// A copy sharing this database's runtime and client generation, from which connections can
    /// be built without holding whatever guards the database: building one can create credentials
    /// and validate the current catalog and schema over the network.
    #[cfg(feature = "ffi")]
    pub(crate) fn snapshot(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            clients: OnceLock::from(Arc::clone(self.clients())),
            uri: self.uri.clone(),
        }
    }

    fn invalidate_clients(&mut self) {
        self.clients.take();
    }
}

impl Database for BigQueryDatabase {
    type ConnectionType = BigQueryConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        // Build the shared credential now, so malformed authentication fails the connection
        // rather than its first statement.
        let clients = self.clients();
        clients.configuration(&self.runtime)?;
        Ok(BigQueryConnection::new(
            self.runtime.clone(),
            clients.clone(),
            self.config.project_id.clone(),
            self.config.dataset_id.clone(),
            self.config.location.clone(),
        ))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        // The current schema is validated in the current catalog, so the catalog goes first
        // whatever the order: the C driver manager replays pre-Init options in hash-map order.
        let (catalog, rest): (Vec<_>, Vec<_>) = opts
            .into_iter()
            .partition(|(key, _)| *key == OptionConnection::CurrentCatalog);
        for (key, value) in catalog.into_iter().chain(rest) {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests;
