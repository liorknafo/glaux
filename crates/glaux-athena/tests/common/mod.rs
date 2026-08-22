//! Shared test support: an in-memory [`StorageBackend`] that captures the
//! result objects the service writes, plus the corpus fixture tables.

#![allow(dead_code)]

pub mod fixtures;

use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use glaux_catalog::{CatalogError, ObjectSummary, StorageBackend};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutPayload};

/// In-memory S3. Keys are namespaced by bucket. Two bucket names inject
/// write failures so the service's failure paths can be exercised:
///
/// - `missing-bucket`: every put fails (the bucket does not exist).
/// - `no-csv`: puts of keys ending in `.csv` fail, after the `.csv.metadata`
///   companion has already been accepted. Its deletes are deliberately slow,
///   so a cleanup that is not awaited would still be in flight when the
///   client observes the terminal state.
/// - `hang-csv`: puts of keys ending in `.csv` never complete, parking the
///   writing task between the two result writes so a cancellation can land
///   there.
#[derive(Debug)]
pub struct MemoryStorage {
    store: Arc<InMemory>,
}

/// How long a delete against `no-csv` takes. Long enough that a fire-and-
/// forget cleanup would lose the race against a client polling every 10ms.
const SLOW_DELETE: Duration = Duration::from_millis(200);

fn path(bucket: &str, key: &str) -> ObjectPath {
    ObjectPath::from(format!("{bucket}/{key}"))
}

fn storage_error(
    op: &'static str,
    bucket: &str,
    key: &str,
) -> impl FnOnce(object_store::Error) -> CatalogError {
    let bucket = bucket.to_string();
    let key = key.to_string();
    move |source| CatalogError::Storage {
        operation: op,
        bucket,
        key,
        source: Box::new(source),
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
        }
    }

    /// Poll until nothing is left under `prefix` (the service removes
    /// partial objects on a spawned task) or fail after a few seconds.
    pub async fn wait_until_empty(&self, bucket: &str, prefix: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let left = self.list_objects(bucket, prefix).await.unwrap();
            if left.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "objects still present under s3://{bucket}/{prefix}: {left:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn get_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<Bytes> {
        self.store
            .get(&path(bucket, key))
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
        self.store
            .get_range(&path(bucket, key), range)
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
        self.store
            .get_opts(&path(bucket, key), options)
            .await
            .map_err(storage_error("get_suffix", bucket, key))?
            .bytes()
            .await
            .map_err(storage_error("get_suffix", bucket, key))
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> glaux_catalog::Result<()> {
        if bucket == "hang-csv" && key.ends_with(".csv") {
            // Never resolves: the caller stays parked here until its task is
            // aborted, which is exactly the window the cleanup guard covers.
            std::future::pending::<()>().await;
        }
        let refused = match bucket {
            "missing-bucket" => Some("bucket does not exist"),
            "no-csv" if key.ends_with(".csv") => Some("injected failure writing the CSV"),
            _ => None,
        };
        if let Some(reason) = refused {
            return Err(CatalogError::Storage {
                operation: "put",
                bucket: bucket.to_string(),
                key: key.to_string(),
                source: Box::new(object_store::Error::NotFound {
                    path: key.to_string(),
                    source: reason.into(),
                }),
            });
        }
        self.store
            .put(&path(bucket, key), PutPayload::from(data))
            .await
            .map_err(storage_error("put", bucket, key))?;
        Ok(())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<()> {
        if bucket == "no-csv" {
            tokio::time::sleep(SLOW_DELETE).await;
        }
        match self.store.delete(&path(bucket, key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(storage_error("delete", bucket, key)(e)),
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
        let prefix_path = path(bucket, prefix);
        let metas: Vec<ObjectMeta> = self
            .store
            .list(Some(&prefix_path))
            .try_collect()
            .await
            .map_err(storage_error("list", bucket, prefix))?;
        let strip = format!("{bucket}/");
        Ok(metas
            .into_iter()
            .map(|m| ObjectSummary {
                key: m
                    .location
                    .to_string()
                    .strip_prefix(&strip)
                    .unwrap_or_default()
                    .to_string(),
                size: m.size,
            })
            .collect())
    }

    fn object_store(&self, _bucket: &str) -> glaux_catalog::Result<Arc<dyn ObjectStore>> {
        Ok(Arc::clone(&self.store) as Arc<dyn ObjectStore>)
    }
}
