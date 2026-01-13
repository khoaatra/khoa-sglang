// factory.rs
//
// Factory function to create storage backends based on configuration.

use std::sync::Arc;

use tracing::info;
use url::Url;

use super::{
    core::{ConversationItemStorage, ConversationStorage, ResponseStorage},
    memory::{MemoryConversationItemStorage, MemoryConversationStorage, MemoryResponseStorage},
    noop::{NoOpConversationItemStorage, NoOpConversationStorage, NoOpResponseStorage},
    oracle::{OracleConversationItemStorage, OracleConversationStorage, OracleResponseStorage},
    oci_oracle::{DefaultSecretFetcher, OciOracleStore},
};
use crate::{
    config::{HistoryBackend, OracleConfig, PostgresConfig, RouterConfig},
    data_connector::{
        oci_oracle::OciOracleConfig,
        postgres::{
            PostgresConversationItemStorage, PostgresConversationStorage, PostgresResponseStorage,
            PostgresStore,
        },
    },
};

/// Type alias for the storage tuple returned by factory functions.
/// This avoids clippy::type_complexity warnings while keeping Arc explicit.
pub type StorageTuple = (
    Arc<dyn ResponseStorage>,
    Arc<dyn ConversationStorage>,
    Arc<dyn ConversationItemStorage>,
);

/// Create all three storage backends based on router configuration.
///
/// # Arguments
/// * `config` - Router configuration containing history_backend and oracle settings
///
/// # Returns
/// Tuple of (response_storage, conversation_storage, conversation_item_storage)
///
/// # Errors
/// Returns error string if Oracle configuration is missing or initialization fails
pub fn create_storage(config: &RouterConfig) -> Result<StorageTuple, String> {
    match config.history_backend {
        HistoryBackend::Memory => {
            info!("Initializing data connector: Memory");
            Ok((
                Arc::new(MemoryResponseStorage::new()),
                Arc::new(MemoryConversationStorage::new()),
                Arc::new(MemoryConversationItemStorage::new()),
            ))
        }
        HistoryBackend::None => {
            info!("Initializing data connector: None (no persistence)");
            Ok((
                Arc::new(NoOpResponseStorage::new()),
                Arc::new(NoOpConversationStorage::new()),
                Arc::new(NoOpConversationItemStorage::new()),
            ))
        }
        HistoryBackend::Oracle => {
            let oracle_cfg = config
                .oracle
                .clone()
                .ok_or("oracle configuration is required when history_backend=oracle")?;

            info!(
                "Initializing data connector: Oracle ATP (pool: {}-{})",
                oracle_cfg.pool_min, oracle_cfg.pool_max
            );

            let storages = create_oracle_storage(&oracle_cfg)?;

            info!("Data connector initialized successfully: Oracle ATP");
            Ok(storages)
        }
        HistoryBackend::OciOracle => {
            let oci_oracle_cfg = config
                .oci_oracle
                .clone()
                .ok_or("OCI Oracle configuration is required when history_backend=oci_oracle")?;

            info!(
                "Initializing data connector: OCI Oracle ATP (pool_max: {})",
                oci_oracle_cfg.pool_max
            );

            let storages = create_oci_oracle_storage(&oci_oracle_cfg)?;

            info!("Data connector initialized successfully: OCI Oracle ATP");
            Ok(storages)
        }
        HistoryBackend::Postgres => {
            let postgres_cfg = config
                .postgres
                .clone()
                .ok_or("Postgres configuration is required when history_backend=postgres")?;

            let log_db_url = match Url::parse(&postgres_cfg.db_url) {
                Ok(mut url) => {
                    if url.password().is_some() {
                        let _ = url.set_password(Some("****"));
                    }
                    url.to_string()
                }
                Err(_) => "<redacted>".to_string(),
            };

            info!(
                "Initializing data connector: Postgres (db_url: {}, pool_max: {})",
                log_db_url, postgres_cfg.pool_max
            );

            let storages = create_postgres_storage(&postgres_cfg)?;

            info!("Data connector initialized successfully: Postgres");

            Ok(storages)
        }
    }
}

/// Create Oracle storage backends
fn create_oracle_storage(oracle_cfg: &OracleConfig) -> Result<StorageTuple, String> {
    let response_storage = OracleResponseStorage::new(oracle_cfg.clone())
        .map_err(|err| format!("failed to initialize Oracle response storage: {err}"))?;

    let conversation_storage = OracleConversationStorage::new(oracle_cfg.clone())
        .map_err(|err| format!("failed to initialize Oracle conversation storage: {err}"))?;

    let conversation_item_storage = OracleConversationItemStorage::new(oracle_cfg.clone())
        .map_err(|err| format!("failed to initialize Oracle conversation item storage: {err}"))?;

    Ok((
        Arc::new(response_storage),
        Arc::new(conversation_storage),
        Arc::new(conversation_item_storage),
    ))
}

fn create_oci_oracle_storage(oci_oracle_cfg: &OciOracleConfig) -> Result<StorageTuple, String> {
    use super::oci_oracle::{DefaultSecretFetcher, OciOracleStore, OciOracleResponseStorage, OciOracleConversationStorage, OciOracleConversationItemStorage};

    info!("Creating OCI Oracle storage with Agent Runtime schema");

    // Build config from environment variables using the new fallback logic
    let config = oci_oracle_cfg.clone().with_env_defaults()
        .map_err(|e| format!("Failed to configure OCI Oracle from environment: {e}"))?;

    // Create the store with Agent Runtime schema
    let secret_fetcher = Arc::new(DefaultSecretFetcher);
    let store = Arc::new(OciOracleStore::new_with_agent_runtime_schema(config, secret_fetcher)
        .map_err(|e| format!("Failed to create OCI Oracle store: {e}"))?);

    info!("OCI Oracle storage initialized successfully with Agent Runtime schema");

    // Create actual OCI Oracle storage implementations
    let response_storage = Arc::new(OciOracleResponseStorage::new(store.clone()));
    let conversation_storage = Arc::new(OciOracleConversationStorage::new(store.clone()));
    let conversation_item_storage = Arc::new(OciOracleConversationItemStorage::new(store));

    Ok((
        response_storage,
        conversation_storage,
        conversation_item_storage,
    ))
}

fn create_postgres_storage(postgres_cfg: &PostgresConfig) -> Result<StorageTuple, String> {
    let store = PostgresStore::new(postgres_cfg.clone())?;
    let postgres_resp = PostgresResponseStorage::new(store.clone())
        .map_err(|err| format!("failed to initialize Postgres response storage: {err}"))?;
    let postgres_conv = PostgresConversationStorage::new(store.clone())
        .map_err(|err| format!("failed to initialize Postgres conversation storage: {err}"))?;
    let postgres_item = PostgresConversationItemStorage::new(store.clone())
        .map_err(|err| format!("failed to initialize Postgres conversation item storage: {err}"))?;

    Ok((
        Arc::new(postgres_resp),
        Arc::new(postgres_conv),
        Arc::new(postgres_item),
    ))
}
