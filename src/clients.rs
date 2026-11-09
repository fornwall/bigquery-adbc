//! Lazily initialized clients sharing one immutable configuration snapshot.

use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use adbc_core::error::Result;
use google_cloud_bigquery::client::{Read, Write};

use crate::cancellation::{Operation, gate};
use crate::config::{ClientConfiguration, DatabaseConfig, ServiceEndpointKind};
use crate::error::{chain, invalid_argument};
use crate::identifiers::ProjectIdentifier;
use crate::rest::client::{RestClient, rest_client_with_user_agent, user_agent};
use crate::rest::jobs::JobClient;
use crate::rest::metadata::{DatasetId, MetadataClient, MetadataError};
use crate::runtime::SharedRuntime;

#[derive(Default)]
struct ClientCache {
    configuration: OnceLock<Arc<ClientConfiguration>>,
    metadata: OnceLock<MetadataClient>,
    /// Transport shared by metadata, jobs and media uploads.
    rest: OnceLock<RestClient>,
    jobs: OnceLock<JobClient>,
    write: OnceLock<Arc<Write>>,
    read: OnceLock<Read>,
}

/// Clients for one immutable database configuration. Changing database options replaces this
/// state for new connections; existing connections keep their credentials and endpoints.
pub(crate) struct ClientState {
    settings: DatabaseConfig,
    cache: ClientCache,
    /// Projects with a permanent Storage Read refusal. Shared across connections because
    /// `bigquery.readsessions.create` is granted per project. Automatic fallback leaves the
    /// configured opt-out value unchanged; re-enabling Storage Read clears the project's entry.
    storage_read_disabled: Mutex<HashSet<String>>,
    /// Dataset locations shared across connections. Locations are fixed at dataset creation;
    /// `crate::dbt::jobs::in_job_location` invalidates entries after a failed job.
    dataset_locations: Mutex<DatasetLocationCache>,
}

/// A dataset's project and dataset IDs.
pub(crate) type DatasetKey = (ProjectIdentifier, Arc<str>);

/// FIFO eviction bounds retained state; lookups scan at most 32 entries.
const DATASET_LOCATION_CACHE_ENTRIES: usize = 32;

#[derive(Default)]
struct DatasetLocationCache(VecDeque<(DatasetKey, Arc<str>)>);

impl DatasetLocationCache {
    fn insert(&mut self, dataset: DatasetKey, location: Arc<str>) {
        if let Some((_, cached)) = self.0.iter_mut().find(|(key, _)| key == &dataset) {
            *cached = location;
            return;
        }
        if self.0.len() == DATASET_LOCATION_CACHE_ENTRIES {
            self.0.pop_front();
        }
        self.0.push_back((dataset, location));
    }

    fn remove(&mut self, dataset: &DatasetKey) {
        self.0.retain(|(key, _)| key != dataset);
    }
}

/// Cached dataset-location lookups.
///
/// Loads, copies and transactions resolve their location before submission. A query job submitted
/// without a location carries this to find the job when its submission was ambiguous.
#[derive(Clone)]
pub(crate) struct DatasetLocations {
    state: Arc<ClientState>,
    metadata: MetadataClient,
}

impl DatasetLocations {
    /// Recovery passes no operation: a cancelled submission still needs the location to find the
    /// job it may have started. Callers add context to lookup errors.
    pub(crate) async fn get(
        &self,
        operation: Option<&Operation>,
        dataset: &DatasetKey,
    ) -> std::result::Result<Arc<str>, MetadataError> {
        if let Some(location) = self.state.dataset_location(dataset) {
            return Ok(location);
        }
        let id = DatasetId {
            project_id: dataset.0.as_str(),
            dataset_id: &dataset.1,
        };
        let location = Arc::<str>::from(self.metadata.get_dataset_location(operation, id).await?);
        self.state
            .remember_dataset_location(dataset.clone(), location.clone());
        Ok(location)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct InitializedClients {
    pub(crate) configuration: bool,
    pub(crate) metadata: bool,
    pub(crate) rest: bool,
    pub(crate) jobs: bool,
    pub(crate) write: bool,
    pub(crate) read: bool,
}

impl ClientState {
    pub(crate) fn new(config: &DatabaseConfig) -> Arc<Self> {
        Arc::new(Self {
            settings: config.clone(),
            cache: ClientCache::default(),
            storage_read_disabled: Mutex::new(HashSet::new()),
            dataset_locations: Mutex::new(DatasetLocationCache::default()),
        })
    }

    pub(crate) fn configuration(
        &self,
        runtime: &SharedRuntime,
    ) -> Result<Arc<ClientConfiguration>> {
        // Credential construction may read a JSON file or discover Application Default
        // Credentials. Build outside the OnceLock so a concurrent cancellation can initialize its
        // job transport without waiting on this builder.
        cached(&self.cache.configuration, || {
            let _runtime_guard = runtime.enter();
            ClientConfiguration::build(&self.settings).map(Arc::new)
        })
    }

    pub(crate) fn metadata_client(&self, runtime: &SharedRuntime) -> Result<MetadataClient> {
        cached(&self.cache.metadata, || {
            let configuration = self.configuration(runtime)?;
            Ok(MetadataClient::new(
                configuration.credentials.clone(),
                configuration.endpoints.get(ServiceEndpointKind::BigQuery),
                self.rest_client()?,
            ))
        })
    }

    /// Reuses one TLS configuration and connection pool for all REST operations. Per-request
    /// deadlines in `rest::request` let media uploads use the same pool.
    pub(crate) fn rest_client(&self) -> Result<RestClient> {
        cached(&self.cache.rest, || {
            Ok(
                rest_client_with_user_agent("direct REST transport", &self.user_agent())?
                    .with_unauthenticated_remedy(unauthenticated_remedy(&self.settings)),
            )
        })
    }

    pub(crate) fn user_agent(&self) -> Cow<'static, str> {
        user_agent(self.settings.user_agent.as_deref())
    }

    /// Dataset-location resolvers are attached only to returned clones: attaching one to the
    /// cached client would create an `Arc` cycle back to this state.
    pub(crate) fn rest_job_client(&self, runtime: &SharedRuntime) -> Result<JobClient> {
        cached(&self.cache.jobs, || {
            let configuration = self.configuration(runtime)?;
            Ok(JobClient::new(
                configuration.credentials.clone(),
                configuration.endpoints.get(ServiceEndpointKind::BigQuery),
                self.rest_client()?,
            ))
        })
    }

    /// `operation` gates the build: a metadata-server credential looks up its universe here, and
    /// that request can stall.
    pub(crate) fn write_client(
        &self,
        runtime: &SharedRuntime,
        operation: Option<&Operation>,
    ) -> Result<Arc<Write>> {
        cached(&self.cache.write, || {
            let configuration = self.configuration(runtime)?;
            let mut builder = Write::builder().with_credentials(configuration.credentials.clone());
            if let Some(endpoint) = configuration.endpoints.get(ServiceEndpointKind::Storage) {
                builder = builder.with_endpoint(endpoint);
            }
            runtime
                .block_on(gate(operation, Box::pin(builder.build())))?
                .map(Arc::new)
                .map_err(|source| storage_client_error("Storage Write", &source))
        })
    }

    /// Async because Storage Read may initialize from a task already on the shared runtime.
    /// `operation` gates the build, as in [`Self::write_client`].
    pub(crate) async fn read_client(
        &self,
        runtime: &SharedRuntime,
        operation: Option<&Operation>,
    ) -> Result<Read> {
        if let Some(client) = self.cache.read.get() {
            return Ok(client.clone());
        }
        let configuration = self.configuration(runtime)?;
        let mut builder = Read::builder().with_credentials(configuration.credentials.clone());
        if let Some(endpoint) = configuration.endpoints.get(ServiceEndpointKind::Storage) {
            builder = builder.with_endpoint(endpoint);
        }
        let client = gate(operation, Box::pin(builder.build()))
            .await?
            .map_err(|source| storage_client_error("Storage Read", &source))?;
        Ok(self.cache.read.get_or_init(|| client).clone())
    }

    /// A poisoned lock is recovered: no panic can leave a set of project IDs inconsistent.
    pub(crate) fn storage_read_disabled(&self, project: &str) -> bool {
        self.storage_read_disabled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(project)
    }

    pub(crate) fn disable_storage_read(&self, project: &str) {
        self.storage_read_disabled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(project.to_owned());
    }

    pub(crate) fn enable_storage_read(&self, project: &str) {
        self.storage_read_disabled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(project);
    }

    /// A poisoned lock is recovered because each cache entry is written whole.
    pub(crate) fn dataset_location(&self, dataset: &DatasetKey) -> Option<Arc<str>> {
        self.dataset_locations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .iter()
            .find(|(key, _)| key == dataset)
            .map(|(_, location)| location.clone())
    }

    fn remember_dataset_location(&self, dataset: DatasetKey, location: Arc<str>) {
        self.dataset_locations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(dataset, location);
    }

    pub(crate) fn forget_dataset_location(&self, dataset: &DatasetKey) {
        self.dataset_locations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(dataset);
    }

    pub(crate) fn dataset_locations(
        self: &Arc<Self>,
        runtime: &SharedRuntime,
    ) -> Result<DatasetLocations> {
        Ok(DatasetLocations {
            metadata: self.metadata_client(runtime)?,
            state: self.clone(),
        })
    }

    pub(crate) fn initialized_clients(&self) -> InitializedClients {
        InitializedClients {
            configuration: self.cache.configuration.get().is_some(),
            metadata: self.cache.metadata.get().is_some(),
            rest: self.cache.rest.get().is_some(),
            jobs: self.cache.jobs.get().is_some(),
            write: self.cache.write.get().is_some(),
            read: self.cache.read.get().is_some(),
        }
    }

    /// Anonymous credentials and unreachable endpoints keep incidental work, such as background
    /// cancellation, from contacting Google during tests.
    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
        Self::new(&test_config())
    }

    /// Points one service at a mock; all other services stay unreachable.
    #[cfg(test)]
    pub(crate) fn for_test_at(kind: ServiceEndpointKind, endpoint: &str) -> Arc<Self> {
        let mut config = test_config();
        config.endpoints.set(kind, Some(Arc::from(endpoint)));
        Self::new(&config)
    }
}

/// The cell's value, built if it has none. A failure is not cached, so the next caller tries again;
/// a build that loses a race is dropped. `OnceLock::get_or_try_init` is unstable.
fn cached<T: Clone>(cell: &OnceLock<T>, build: impl FnOnce() -> Result<T>) -> Result<T> {
    if let Some(value) = cell.get() {
        return Ok(value.clone());
    }
    let value = build()?;
    Ok(cell.get_or_init(|| value).clone())
}

/// The remedy for an HTTP 401 under the two credential types that never mint a token, where the
/// rejection is the only sign of a bad credential.
fn unauthenticated_remedy(settings: &DatabaseConfig) -> Option<String> {
    use crate::{BigQueryAuthenticationType as Type, BigQueryDatabaseOption as O};

    match settings.authentication_type {
        Type::TemporaryAccessToken => Some(format!(
            "the token set as {} was rejected; set a fresh token with the BigQuery scope, \
             since the driver never refreshes it",
            O::AuthenticationAccessToken.as_str()
        )),
        Type::Anonymous => Some(format!(
            "{} is {:?}, which sends no credentials",
            O::AuthenticationType.as_str(),
            Type::Anonymous.as_str()
        )),
        _ => None,
    }
}

#[cfg(test)]
fn test_config() -> DatabaseConfig {
    let mut config = DatabaseConfig {
        authentication_type: crate::BigQueryAuthenticationType::Anonymous,
        ..DatabaseConfig::default()
    };
    for kind in ServiceEndpointKind::ALL {
        config.endpoints.set(
            kind,
            Some(Arc::from(crate::test_support::UNREACHABLE_ENDPOINT)),
        );
    }
    config
}

impl std::fmt::Debug for ClientState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientState")
            .field("settings", &self.settings)
            .field("initialized", &self.initialized_clients())
            .finish()
    }
}

/// Builder failures concern credentials, endpoints or transport setup and are not retryable.
/// Include the source chain: upstream's `Display` reports only the error kind.
fn storage_client_error(
    client: &str,
    source: &(dyn std::error::Error + 'static),
) -> adbc_core::error::Error {
    let mut detail = String::new();
    for cause in chain(source) {
        if !detail.is_empty() {
            detail.push_str(": ");
        }
        write!(detail, "{cause}").expect("writing to a String cannot fail");
    }
    invalid_argument(format!(
        "failed to initialize the configured Google Cloud BigQuery {client} client: {detail}; \
         check the configured credentials and BigQuery option {}",
        ServiceEndpointKind::Storage.option().as_str()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use adbc_core::error::Status;
    use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider};
    use google_cloud_gax::error::CredentialsError;

    use super::{ClientState, DATASET_LOCATION_CACHE_ENTRIES, storage_client_error};
    use crate::cancellation::Cancellation;
    use crate::config::{ClientConfiguration, ServiceEndpointKind, ServiceEndpoints};
    use crate::runtime::new_runtime;
    use crate::test_support::cancellation::operation;
    use crate::test_support::http::capture_requests;
    use crate::test_support::identifiers::project;

    /// Building a Storage client asks a metadata-server credential for its universe, which can
    /// stall, so the build has to stop for a cancel.
    #[test]
    fn building_a_storage_client_stops_for_a_cancelled_operation() {
        #[derive(Debug)]
        struct Stalled;

        impl CredentialsProvider for Stalled {
            async fn headers(
                &self,
                _: http::Extensions,
            ) -> Result<CacheableResource<http::HeaderMap>, CredentialsError> {
                std::future::pending().await
            }

            async fn universe_domain(&self) -> Option<String> {
                std::future::pending().await
            }
        }

        let runtime = new_runtime().unwrap();
        let state = ClientState::for_test();
        let configuration = ClientConfiguration {
            credentials: Credentials::from(Stalled),
            execution_principal: None,
            endpoints: ServiceEndpoints::default(),
        };
        state
            .cache
            .configuration
            .set(Arc::new(configuration))
            .unwrap();
        let (_cancellation, operation) = operation(&runtime, state.clone());
        operation.finish();

        let write = state.write_client(&runtime, Some(&operation));
        let read = runtime.block_on(state.read_client(&runtime, Some(&operation)));
        assert_eq!(write.err().unwrap().status, Status::Cancelled);
        assert_eq!(read.err().unwrap().status, Status::Cancelled);
    }

    #[test]
    fn a_job_location_resolver_does_not_keep_the_client_cache_alive() {
        let runtime = new_runtime().unwrap();
        let state = ClientState::for_test();
        let weak = Arc::downgrade(&state);
        let client = state
            .rest_job_client(&runtime)
            .unwrap()
            .with_dataset_locations(state.dataset_locations(&runtime).unwrap());
        drop(state);
        assert!(weak.upgrade().is_some());
        drop(client);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn location_recovery_outlives_cancellation_and_shares_the_resolved_location() {
        let (endpoint, requests) = capture_requests(vec![(
            200,
            serde_json::json!({"location": "asia-northeast1"}),
        )]);
        let runtime = new_runtime().unwrap();
        let state = ClientState::for_test_at(ServiceEndpointKind::BigQuery, &endpoint);
        let locations = state.dataset_locations(&runtime).unwrap();
        let dataset = (project("data-project"), Arc::from("reporting"));
        let cancellation = Cancellation::new(runtime.clone(), state.clone(), "statement");
        let cancelled = cancellation.begin();
        cancelled.finish();

        let failure = runtime
            .block_on(locations.get(Some(&cancelled), &dataset))
            .map_err(|failure| failure.into_error("lookup"))
            .unwrap_err();
        assert_eq!(failure.status, Status::Cancelled);
        assert!(state.dataset_location(&dataset).is_none());

        let recovered = runtime
            .block_on(locations.get(None, &dataset))
            .map_err(|failure| failure.into_error("recovery"))
            .unwrap();
        let other = state.dataset_locations(&runtime).unwrap();
        let cached = runtime
            .block_on(other.get(None, &dataset))
            .map_err(|failure| failure.into_error("cached lookup"))
            .unwrap();
        assert_eq!(&*recovered, "asia-northeast1");
        assert!(Arc::ptr_eq(&recovered, &cached));
        let requests = requests.join().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].path(),
            "/bigquery/v2/projects/data-project/datasets/reporting?fields=location"
        );
    }

    #[test]
    fn dataset_location_cache_releases_evicted_entries_and_stays_bounded() {
        const DATASETS: usize = 2 * DATASET_LOCATION_CACHE_ENTRIES;
        let state = ClientState::for_test();
        let first_dataset = Arc::<str>::from("first");
        let first_location = Arc::<str>::from("asia-northeast1");
        let dataset_owner = Arc::downgrade(&first_dataset);
        let location_owner = Arc::downgrade(&first_location);
        state.remember_dataset_location((project("data-project"), first_dataset), first_location);

        for index in 1..DATASETS {
            state.remember_dataset_location(
                (
                    project("data-project"),
                    Arc::from(format!("scratch_{index}")),
                ),
                Arc::from("US"),
            );
            let cache = state.dataset_locations.lock().unwrap();
            assert_eq!(
                cache.0.len(),
                (index + 1).min(DATASET_LOCATION_CACHE_ENTRIES)
            );
        }

        assert!(dataset_owner.upgrade().is_none());
        assert!(location_owner.upgrade().is_none());
        let oldest_retained = DATASETS - DATASET_LOCATION_CACHE_ENTRIES;
        assert!(
            state
                .dataset_location(&(
                    project("data-project"),
                    Arc::from(format!("scratch_{}", oldest_retained - 1)),
                ))
                .is_none()
        );
        assert_eq!(
            state
                .dataset_location(&(
                    project("data-project"),
                    Arc::from(format!("scratch_{oldest_retained}")),
                ))
                .as_deref(),
            Some("US")
        );
    }

    #[test]
    fn dataset_location_replacement_and_invalidation_preserve_eviction_order() {
        let state = ClientState::for_test();
        let datasets = (0..=DATASET_LOCATION_CACHE_ENTRIES)
            .map(|index| {
                (
                    project("data-project"),
                    Arc::<str>::from(format!("scratch_{index}")),
                )
            })
            .collect::<Vec<_>>();
        for dataset in &datasets[..DATASET_LOCATION_CACHE_ENTRIES] {
            state.remember_dataset_location(dataset.clone(), Arc::from("US"));
        }
        state.remember_dataset_location(datasets[0].clone(), Arc::from("EU"));
        assert_eq!(state.dataset_location(&datasets[0]).as_deref(), Some("EU"));
        assert_eq!(state.dataset_location(&datasets[1]).as_deref(), Some("US"));

        state.forget_dataset_location(&datasets[0]);
        assert!(state.dataset_location(&datasets[0]).is_none());
        state.remember_dataset_location(datasets[0].clone(), Arc::from("asia-northeast1"));
        state.remember_dataset_location(
            datasets[DATASET_LOCATION_CACHE_ENTRIES].clone(),
            Arc::from("US"),
        );
        assert_eq!(
            state.dataset_location(&datasets[0]).as_deref(),
            Some("asia-northeast1")
        );
        assert!(state.dataset_location(&datasets[1]).is_none());
        let cache = state.dataset_locations.lock().unwrap();
        assert_eq!(cache.0.len(), DATASET_LOCATION_CACHE_ENTRIES);
    }

    /// A refused configuration is an invalid argument, and the message carries the whole source
    /// chain plus the endpoint option.
    #[test]
    fn a_refused_client_configuration_names_the_endpoint_option_and_the_whole_chain() {
        let source = google_cloud_gax::client_builder::Error::cred(std::io::Error::other(
            "the ADC file names no supported credential type",
        ));
        let failure = storage_client_error("Storage Write", &source);

        assert_eq!(failure.status, Status::InvalidArguments);
        assert_eq!(
            failure.message,
            "failed to initialize the configured Google Cloud BigQuery Storage Write client: \
             could not create default credentials: the ADC file names no supported credential \
             type; check the configured credentials and BigQuery option bigquery.storage_endpoint"
        );
    }
}
