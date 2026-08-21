//! The query engine used by the all-in-one binary: a [`TrinoEngine`] whose
//! Glue catalog is re-snapshotted from in-process fakecloud state before
//! every query.
//!
//! [`GlueCatalogProvider`] snapshots the database and table *listings* when
//! it is built (DataFusion's catalog traits are synchronous). In the
//! all-in-one binary those listings change at runtime — a test creates a
//! Glue table and queries it a millisecond later — so the snapshot is
//! rebuilt per query. Against in-process state that is a handful of map
//! reads; table *metadata* resolution was already live.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use glaux_athena::{EngineError, QueryEngine, QueryOutput, QueryRequest, TrinoEngine};
use glaux_catalog::{GlueApi, GlueCatalogProvider, StorageBackend};

/// The DataFusion catalog name Athena's `AwsDataCatalog` maps to.
pub const CATALOG_NAME: &str = "awsdatacatalog";

/// A [`TrinoEngine`] that refreshes its Glue catalog snapshot per query.
pub struct LiveCatalogEngine {
    inner: TrinoEngine,
    glue: Arc<dyn GlueApi>,
    storage: Arc<dyn StorageBackend>,
}

impl std::fmt::Debug for LiveCatalogEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveCatalogEngine")
            .field("catalog", &CATALOG_NAME)
            .finish()
    }
}

impl LiveCatalogEngine {
    /// Build the engine over `glue` metadata and `storage` data.
    pub fn new(glue: Arc<dyn GlueApi>, storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner: TrinoEngine::new(SessionContext::new(), CATALOG_NAME),
            glue,
            storage,
        }
    }

    /// Snapshot the current Glue listings into the session's catalog list,
    /// replacing the previous snapshot.
    async fn refresh_catalog(&self) -> Result<(), EngineError> {
        let provider =
            GlueCatalogProvider::try_new(Arc::clone(&self.glue), Arc::clone(&self.storage))
                .await
                .map_err(|e| {
                    EngineError::Execution(format!("refreshing the Glue catalog failed: {e}"))
                })?;
        self.inner
            .context()
            .register_catalog(CATALOG_NAME, Arc::new(provider));
        Ok(())
    }
}

#[async_trait]
impl QueryEngine for LiveCatalogEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        self.refresh_catalog().await?;
        self.inner.execute(request).await
    }
}
