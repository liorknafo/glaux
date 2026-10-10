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
use datafusion::prelude::SessionContext;
use glaux_athena::{EngineError, QueryEngine, QueryOutput, QueryRequest, TrinoEngine};
use glaux_catalog::{GlueApi, GlueCatalogProvider, StorageBackend};

/// A Trino-dialect engine that re-reads the Glue catalog before each query.
pub struct GlueBackedEngine {
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
    /// An engine that registers a fresh Glue catalog under `catalog_name`
    /// (Athena's `AwsDataCatalog`) for every query.
    pub fn new(
        glue: Arc<dyn GlueApi>,
        storage: Arc<dyn StorageBackend>,
        catalog_name: impl Into<String>,
    ) -> Self {
        Self {
            glue,
            storage,
            catalog_name: catalog_name.into(),
        }
    }
}

#[async_trait]
impl QueryEngine for GlueBackedEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        let provider =
            GlueCatalogProvider::try_new(Arc::clone(&self.glue), Arc::clone(&self.storage))
                .await
                .map_err(|e| {
                    EngineError::Execution(format!(
                        "could not read the Glue Data Catalog before planning the query: {e}"
                    ))
                })?;
        // A fresh context per query, not a re-registration on a shared one:
        // per-query `SessionState` clones share the catalog list, and Trino
        // planning looks the catalog up more than once (the resolve probe,
        // then the real plan), so an overlapping query swapping the snapshot
        // in between would split one query across two snapshots.
        let ctx = SessionContext::new();
        ctx.register_catalog(&self.catalog_name, Arc::new(provider));
        TrinoEngine::new(ctx, self.catalog_name.clone())
            .execute(request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use bytes::Bytes;
    use glaux_catalog::{
        GlueColumn, GlueDatabase, GluePartition, GlueSerDeInfo, GlueStorageDescriptor, GlueTable,
        ObjectSummary,
    };
    use object_store::ObjectStore;
    use tokio::sync::Notify;

    use super::*;

    /// Glue whose every `GetDatabases` returns a different snapshot
    /// (`db0`, `db1`, ...), each with one empty Parquet table `t`. The first
    /// `GetTable` on `db0` parks until released, holding that query
    /// mid-planning.
    #[derive(Default)]
    struct SnapshotGlue {
        refreshes: AtomicUsize,
        parked_once: AtomicBool,
        parked: Notify,
        release: Notify,
    }

    fn table(database: &str) -> GlueTable {
        GlueTable {
            name: "t".to_string(),
            database_name: Some(database.to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: Some(GlueStorageDescriptor {
                columns: vec![GlueColumn {
                    name: "x".to_string(),
                    column_type: Some("int".to_string()),
                    comment: None,
                }],
                location: Some(format!("s3://lake/{database}/")),
                serde_info: Some(GlueSerDeInfo {
                    serialization_library: Some(
                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".to_string(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            partition_keys: vec![],
            parameters: HashMap::new(),
        }
    }

    #[async_trait]
    impl GlueApi for SnapshotGlue {
        async fn get_databases(&self) -> glaux_catalog::Result<Vec<GlueDatabase>> {
            let n = self.refreshes.fetch_add(1, Ordering::SeqCst);
            Ok(vec![GlueDatabase {
                name: format!("db{n}"),
                description: None,
                location_uri: None,
                parameters: HashMap::new(),
            }])
        }
        async fn get_tables(&self, database: &str) -> glaux_catalog::Result<Vec<GlueTable>> {
            Ok(vec![table(database)])
        }
        async fn get_table(&self, database: &str, _: &str) -> glaux_catalog::Result<GlueTable> {
            if database == "db0" && !self.parked_once.swap(true, Ordering::SeqCst) {
                self.parked.notify_one();
                self.release.notified().await;
            }
            Ok(table(database))
        }
        async fn get_partitions(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
        ) -> glaux_catalog::Result<Vec<GluePartition>> {
            Ok(vec![])
        }
    }

    /// S3 with no objects: every table scans as empty.
    struct EmptyStorage;

    #[async_trait]
    impl StorageBackend for EmptyStorage {
        async fn get_object(&self, _: &str, _: &str) -> glaux_catalog::Result<Bytes> {
            unreachable!()
        }
        async fn get_object_range(
            &self,
            _: &str,
            _: &str,
            _: Range<u64>,
        ) -> glaux_catalog::Result<Bytes> {
            unreachable!()
        }
        async fn get_object_suffix(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> glaux_catalog::Result<Bytes> {
            unreachable!()
        }
        async fn put_object(&self, _: &str, _: &str, _: Bytes) -> glaux_catalog::Result<()> {
            unreachable!()
        }
        async fn delete_object(&self, _: &str, _: &str) -> glaux_catalog::Result<()> {
            unreachable!()
        }
        async fn list_objects(
            &self,
            _: &str,
            _: &str,
        ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
            Ok(vec![])
        }
        fn object_store(&self, _: &str) -> glaux_catalog::Result<Arc<dyn ObjectStore>> {
            unreachable!()
        }
    }

    /// A `USING` join makes Trino planning look the catalog up twice (the
    /// resolve probe, then the real plan).
    fn request(database: &str) -> QueryRequest {
        QueryRequest {
            sql: format!("SELECT x FROM {database}.t a JOIN {database}.t b USING (x)"),
            catalog: None,
            database: None,
        }
    }

    #[tokio::test]
    async fn overlapping_query_cannot_swap_the_catalog_mid_planning() {
        let glue = Arc::new(SnapshotGlue::default());
        let engine = Arc::new(GlueBackedEngine::new(
            Arc::clone(&glue) as Arc<dyn GlueApi>,
            Arc::new(EmptyStorage),
            "awsdatacatalog",
        ));
        let first = tokio::spawn({
            let engine = Arc::clone(&engine);
            async move { engine.execute(request("db0")).await }
        });
        // The first query has snapshot `db0` and is parked mid-planning.
        glue.parked.notified().await;
        // The second query's snapshot (`db1`) has no `db0`.
        engine.execute(request("db1")).await.expect("second query");
        glue.release.notify_one();
        first
            .await
            .unwrap()
            .expect("first query must plan against its own snapshot");
    }
}
