//! [`GlueBackedEngine`]: the query engine the standalone server drives —
//! a [`TrinoEngine`] whose Glue catalog is refreshed on every query.
//!
//! [`GlueCatalogProvider`] snapshots the Glue database and table listings
//! when it is built (DataFusion's catalog traits are synchronous). A
//! long-running server cannot use one snapshot: tables and databases are
//! created while it runs — by Terraform, by a test fixture, by the Firehose
//! flow itself. So each `StartQueryExecution` rebuilds the provider from
//! Glue before planning. Two Glue calls per database per query is cheap
//! locally and guarantees a query never fails to see (or sees a stale
//! version of) a table that exists in the catalog.

use std::sync::Arc;

use async_trait::async_trait;
use glaux_athena::{EngineError, QueryEngine, QueryOutput, QueryRequest, TrinoEngine};
use glaux_catalog::{GlueApi, GlueCatalogProvider, StorageBackend};

/// A Trino-dialect engine that re-reads the Glue catalog before each query.
pub struct GlueBackedEngine {
    inner: TrinoEngine,
    glue: Arc<dyn GlueApi>,
    storage: Arc<dyn StorageBackend>,
    catalog_name: String,
}

impl std::fmt::Debug for GlueBackedEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueBackedEngine")
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl GlueBackedEngine {
    /// Wrap `inner`, registering a fresh Glue catalog under `catalog_name`
    /// (the name `inner` was built with as its default catalog) before
    /// every query.
    pub fn new(
        inner: TrinoEngine,
        glue: Arc<dyn GlueApi>,
        storage: Arc<dyn StorageBackend>,
        catalog_name: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            glue,
            storage,
            catalog_name: catalog_name.into(),
        }
    }

    /// Snapshot Glue and install the snapshot as the catalog. Concurrent
    /// queries each install their own snapshot; a query already planning
    /// holds its own `Arc` to the provider it resolved, so a swap underneath
    /// it is harmless.
    async fn refresh_catalog(&self) -> Result<(), EngineError> {
        let provider =
            GlueCatalogProvider::try_new(Arc::clone(&self.glue), Arc::clone(&self.storage))
                .await
                .map_err(|e| {
                    EngineError::Execution(format!(
                        "could not read the Glue Data Catalog before planning the query: {e}"
                    ))
                })?;
        self.inner
            .context()
            .register_catalog(&self.catalog_name, Arc::new(provider));
        Ok(())
    }
}

#[async_trait]
impl QueryEngine for GlueBackedEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        self.refresh_catalog().await?;
        self.inner.execute(request).await
    }
}
