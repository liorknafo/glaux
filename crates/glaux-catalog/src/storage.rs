//! S3-shaped storage access: the [`StorageBackend`] trait and its network
//! implementation over [`object_store`].
//!
//! Two backends exist by design (see the v0.1 spec): **network** (this
//! module — any S3-compatible endpoint: fakecloud over HTTP, MinIO, real
//! AWS) and **in-process** (direct fakecloud state calls, supplied by the
//! AGPL all-in-one binary from its side of the dependency boundary).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _};

use crate::config::GlauxConfig;
use crate::error::{CatalogError, Result};

/// Summary of one object returned by [`StorageBackend::list_objects`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSummary {
    /// Full object key (no leading slash, no bucket).
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
}

/// S3-shaped storage operations used by the Athena and Firehose engines.
///
/// All byte-returning methods return exactly the requested bytes or an
/// explicit error — a missing object or failed range read never yields
/// empty or truncated data.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Fetch an entire object.
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes>;

    /// Fetch the byte range `[range.start, range.end)` of an object
    /// (half-open, as in `Range: bytes=start-(end-1)`).
    async fn get_object_range(&self, bucket: &str, key: &str, range: Range<u64>) -> Result<Bytes>;

    /// Fetch the final `length` bytes of an object (suffix range request,
    /// `Range: bytes=-length`) — the access pattern Parquet footers need.
    async fn get_object_suffix(&self, bucket: &str, key: &str, length: u64) -> Result<Bytes>;

    /// Write an object, replacing any existing object at the key.
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> Result<()>;

    /// List objects under a key prefix (recursive, no delimiter).
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectSummary>>;

    /// An [`ObjectStore`] handle scoped to `bucket`, for registration with
    /// DataFusion's runtime or use by Parquet readers.
    fn object_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>>;
}

/// Placeholder identity used against custom (emulator) endpoints when no
/// credentials are configured anywhere. Emulators ignore the signature but
/// several (fakecloud included) route requests by the SigV4 credential
/// scope, so an Authorization header must always be present.
pub(crate) const PLACEHOLDER_ACCESS_KEY: &str = "glaux-local";
pub(crate) const PLACEHOLDER_SECRET_KEY: &str = "glaux-local-secret";

/// [`StorageBackend`] over the network for any S3-compatible endpoint.
///
/// Built from [`GlauxConfig`]: a custom `s3_endpoint` switches the client to
/// path-style addressing (`http://host:port/bucket/key`) and permits plain
/// HTTP; with no endpoint the client targets real AWS S3, where ambient
/// `AWS_*` environment credentials also apply.
#[derive(Debug)]
pub struct NetworkStorageBackend {
    endpoint: Option<String>,
    region: String,
    credentials: Option<crate::config::AwsCredentials>,
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl NetworkStorageBackend {
    /// Build a backend from the unified configuration.
    pub fn new(config: &GlauxConfig) -> Self {
        Self {
            endpoint: config.s3_endpoint.clone(),
            region: config.region.clone(),
            credentials: config.credentials.clone(),
            stores: RwLock::new(HashMap::new()),
        }
    }

    /// Build (or fetch from cache) the object store for one bucket.
    fn store_for(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self
            .stores
            .read()
            .expect("store cache lock poisoned")
            .get(bucket)
        {
            return Ok(Arc::clone(store));
        }

        // Start from the ambient AWS environment so standard AWS_* variables
        // (credentials, profiles behind AWS_ACCESS_KEY_ID/SECRET) pass
        // through, then apply glaux configuration on top.
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(self.region.clone());

        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_endpoint(endpoint.clone())
                // `from_env` may have picked up AWS_ENDPOINT_URL_S3, which
                // wins over `with_endpoint` at build time; set it too so the
                // glaux configuration is what actually takes effect.
                .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint.clone())
                // Emulators serve every bucket on one host; virtual-hosted
                // style would require per-bucket DNS.
                .with_virtual_hosted_style_request(false)
                .with_allow_http(endpoint.starts_with("http://"));
        }

        match &self.credentials {
            Some(creds) => {
                builder = builder
                    .with_access_key_id(creds.access_key_id.clone())
                    .with_secret_access_key(creds.secret_access_key.clone());
                if let Some(token) = &creds.session_token {
                    builder = builder.with_token(token.clone());
                }
            }
            None => {
                // Against a custom endpoint with no ambient credentials,
                // fall back to a placeholder identity so requests still
                // carry the Authorization header emulators route on.
                if self.endpoint.is_some() && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
                    builder = builder
                        .with_access_key_id(PLACEHOLDER_ACCESS_KEY)
                        .with_secret_access_key(PLACEHOLDER_SECRET_KEY);
                }
            }
        }

        let store: Arc<dyn ObjectStore> =
            Arc::new(
                builder
                    .build()
                    .map_err(|source| CatalogError::StorageClient {
                        bucket: bucket.to_string(),
                        source: Box::new(source),
                    })?,
            );
        self.stores
            .write()
            .expect("store cache lock poisoned")
            .insert(bucket.to_string(), Arc::clone(&store));
        Ok(store)
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
impl StorageBackend for NetworkStorageBackend {
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let store = self.store_for(bucket)?;
        let path = ObjectPath::from(key);
        store
            .get(&path)
            .await
            .map_err(storage_error("get", bucket, key))?
            .bytes()
            .await
            .map_err(storage_error("get", bucket, key))
    }

    async fn get_object_range(&self, bucket: &str, key: &str, range: Range<u64>) -> Result<Bytes> {
        let store = self.store_for(bucket)?;
        let path = ObjectPath::from(key);
        store
            .get_range(&path, range)
            .await
            .map_err(storage_error("get_range", bucket, key))
    }

    async fn get_object_suffix(&self, bucket: &str, key: &str, length: u64) -> Result<Bytes> {
        let store = self.store_for(bucket)?;
        let path = ObjectPath::from(key);
        let options = GetOptions {
            range: Some(GetRange::Suffix(length)),
            ..Default::default()
        };
        store
            .get_opts(&path, options)
            .await
            .map_err(storage_error("get_suffix", bucket, key))?
            .bytes()
            .await
            .map_err(storage_error("get_suffix", bucket, key))
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> Result<()> {
        let store = self.store_for(bucket)?;
        let path = ObjectPath::from(key);
        store
            .put(&path, data.into())
            .await
            .map_err(storage_error("put", bucket, key))?;
        Ok(())
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectSummary>> {
        let store = self.store_for(bucket)?;
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };
        let metas: Vec<object_store::ObjectMeta> = store
            .list(prefix_path.as_ref())
            .try_collect()
            .await
            .map_err(storage_error("list", bucket, prefix))?;
        Ok(metas
            .into_iter()
            .map(|meta| ObjectSummary {
                key: meta.location.to_string(),
                size: meta.size,
            })
            .collect())
    }

    fn object_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        self.store_for(bucket)
    }
}
