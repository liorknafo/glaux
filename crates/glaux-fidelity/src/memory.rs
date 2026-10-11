//! In-memory S3 and Glue for the offline (replay) side: a multi-bucket
//! [`StorageBackend`] over `object_store`'s `InMemory`, and a [`GlueApi`]
//! serving a fixed set of tables.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use glaux_catalog::{
    CatalogError, GlueApi, GlueDatabase, GluePartition, GlueTable, ObjectSummary, StorageBackend,
};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutPayload};

/// In-memory S3: one `InMemory` store per bucket, created on first use.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    buckets: Mutex<HashMap<String, Arc<InMemory>>>,
}

impl MemoryStorage {
    /// Empty storage.
    pub fn new() -> Self {
        Self::default()
    }

    fn bucket(&self, name: &str) -> Arc<InMemory> {
        Arc::clone(
            self.buckets
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(InMemory::new())),
        )
    }

    /// Every key in `bucket`, sorted.
    pub async fn keys(&self, bucket: &str) -> Vec<String> {
        let mut keys: Vec<String> = self
            .list_objects(bucket, "")
            .await
            .expect("in-memory list cannot fail")
            .into_iter()
            .map(|o| o.key)
            .collect();
        keys.sort();
        keys
    }
}

fn storage_error(
    operation: &'static str,
    bucket: &str,
    key: &str,
) -> impl FnOnce(object_store::Error) -> CatalogError {
    let bucket = bucket.to_string();
    let key = key.to_string();
    move |source| CatalogError::Storage {
        operation,
        bucket,
        key,
        source: Box::new(source),
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn get_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<Bytes> {
        self.bucket(bucket)
            .get(&ObjectPath::from(key))
            .await
            .map_err(storage_error("get", bucket, key))?
            .bytes()
            .await
            .map_err(storage_error("get", bucket, key))
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: Range<u64>,
    ) -> glaux_catalog::Result<Bytes> {
        self.bucket(bucket)
            .get_range(&ObjectPath::from(key), range)
            .await
            .map_err(storage_error("get_range", bucket, key))
    }

    async fn get_object_suffix(
        &self,
        bucket: &str,
        key: &str,
        length: u64,
    ) -> glaux_catalog::Result<Bytes> {
        let options = GetOptions {
            range: Some(object_store::GetRange::Suffix(length)),
            ..Default::default()
        };
        self.bucket(bucket)
            .get_opts(&ObjectPath::from(key), options)
            .await
            .map_err(storage_error("get_suffix", bucket, key))?
            .bytes()
            .await
            .map_err(storage_error("get_suffix", bucket, key))
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> glaux_catalog::Result<()> {
        self.bucket(bucket)
            .put(&ObjectPath::from(key), PutPayload::from(data))
            .await
            .map_err(storage_error("put", bucket, key))?;
        Ok(())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<()> {
        match self.bucket(bucket).delete(&ObjectPath::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(storage_error("delete", bucket, key)(e)),
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
        let prefix_path = (!prefix.is_empty()).then(|| ObjectPath::from(prefix));
        let metas: Vec<ObjectMeta> = self
            .bucket(bucket)
            .list(prefix_path.as_ref())
            .try_collect()
            .await
            .map_err(storage_error("list", bucket, prefix))?;
        Ok(metas
            .into_iter()
            .map(|m| ObjectSummary {
                key: m.location.to_string(),
                size: m.size,
            })
            .collect())
    }

    fn object_store(&self, bucket: &str) -> glaux_catalog::Result<Arc<dyn ObjectStore>> {
        Ok(self.bucket(bucket) as Arc<dyn ObjectStore>)
    }
}

/// A fixed, unpartitioned Glue catalog.
#[derive(Debug, Default)]
pub struct StaticGlue {
    databases: Vec<GlueDatabase>,
    tables: HashMap<(String, String), GlueTable>,
}

impl StaticGlue {
    /// A catalog with one database holding `tables` (each table's
    /// `database_name` must be `database`).
    pub fn new(database: &str, tables: Vec<GlueTable>) -> Self {
        let mut map = HashMap::new();
        for table in tables {
            debug_assert_eq!(table.database_name.as_deref(), Some(database));
            map.insert((database.to_string(), table.name.clone()), table);
        }
        Self {
            databases: vec![GlueDatabase {
                name: database.to_string(),
                description: None,
                location_uri: None,
                parameters: HashMap::new(),
            }],
            tables: map,
        }
    }
}

#[async_trait]
impl GlueApi for StaticGlue {
    async fn get_databases(&self) -> glaux_catalog::Result<Vec<GlueDatabase>> {
        Ok(self.databases.clone())
    }

    async fn get_tables(&self, database: &str) -> glaux_catalog::Result<Vec<GlueTable>> {
        Ok(self
            .tables
            .iter()
            .filter(|((db, _), _)| db == database)
            .map(|(_, t)| t.clone())
            .collect())
    }

    async fn get_table(&self, database: &str, table: &str) -> glaux_catalog::Result<GlueTable> {
        self.tables
            .get(&(database.to_string(), table.to_string()))
            .cloned()
            .ok_or_else(|| CatalogError::GlueEntityNotFound {
                message: format!("Table {table} not found in database {database}"),
            })
    }

    async fn get_partitions(
        &self,
        _database: &str,
        _table: &str,
        _expression: Option<&str>,
    ) -> glaux_catalog::Result<Vec<GluePartition>> {
        Ok(Vec::new())
    }
}
