//! In-process [`StorageBackend`] over fakecloud's S3 state.
//!
//! Reads (`GetObject`, ranged and suffix reads, listing) go straight to
//! fakecloud's [`S3State`] behind its `RwLock` — no HTTP, no serialization,
//! no copy beyond the returned slice. Writes and deletes are routed through
//! fakecloud's own [`S3Service`] handler with a synthesized [`AwsRequest`]
//! (still in-process, still no socket) so every S3 side effect fakecloud
//! implements — bucket notifications to SQS/SNS, versioning, object lock,
//! persistence write-through, ETag computation — happens exactly as it would
//! for a client on the wire.
//!
//! # Never silently wrong
//!
//! A missing bucket or key is an explicit [`CatalogError::Storage`] carrying
//! an [`object_store::Error::NotFound`] source; a write fakecloud refuses
//! surfaces fakecloud's own error code and message. Nothing is defaulted to
//! empty bytes.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use fakecloud_core::service::{AwsRequest, AwsService, ResponseBody};
use fakecloud_s3::{S3Object, S3Service, S3State, SharedS3State};
use futures::stream::{self, BoxStream, StreamExt};
use glaux_catalog::{CatalogError, ObjectSummary, StorageBackend};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use object_store::path::Path as ObjectPath;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};

/// Metadata of one S3 object, copied out from under the state lock.
#[derive(Debug, Clone)]
struct ObjectInfo {
    size: u64,
    etag: String,
    last_modified: DateTime<Utc>,
}

/// [`StorageBackend`] over in-process fakecloud S3 state.
pub struct InProcessStorage {
    state: SharedS3State,
    service: Arc<S3Service>,
    account_id: String,
    region: String,
}

impl fmt::Debug for InProcessStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessStorage")
            .field("account_id", &self.account_id)
            .field("region", &self.region)
            .finish()
    }
}

fn not_found(what: &str, path: &str) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.to_string(),
        source: what.into(),
    }
}

fn storage_error(
    operation: &'static str,
    bucket: &str,
    key: &str,
    source: object_store::Error,
) -> CatalogError {
    CatalogError::Storage {
        operation,
        bucket: bucket.to_string(),
        key: key.to_string(),
        source: Box::new(source),
    }
}

fn generic(message: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "fakecloud-s3",
        source: message.into(),
    }
}

impl InProcessStorage {
    /// Build a backend over `state`, routing writes through `service`.
    /// `account_id` selects the fakecloud account whose buckets are visible
    /// (fakecloud partitions S3 state per account).
    pub fn new(
        state: SharedS3State,
        service: Arc<S3Service>,
        account_id: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            state,
            service,
            account_id: account_id.into(),
            region: region.into(),
        }
    }

    /// Run `f` against the stored object under the state read lock.
    fn with_object<T>(
        &self,
        bucket: &str,
        key: &str,
        f: impl FnOnce(&S3State, &S3Object) -> Result<T, object_store::Error>,
    ) -> Result<T, object_store::Error> {
        let accounts = self.state.read();
        let account = accounts
            .get(&self.account_id)
            .ok_or_else(|| not_found("NoSuchBucket", &format!("{bucket}/{key}")))?;
        let bucket_state = account
            .buckets
            .get(bucket)
            .ok_or_else(|| not_found("NoSuchBucket", &format!("{bucket}/{key}")))?;
        let object = bucket_state
            .objects
            .get(key)
            .filter(|o| !o.is_delete_marker)
            .ok_or_else(|| not_found("NoSuchKey", &format!("{bucket}/{key}")))?;
        f(account, object)
    }

    fn object_info(&self, bucket: &str, key: &str) -> Result<ObjectInfo, object_store::Error> {
        self.with_object(bucket, key, |_, object| {
            Ok(ObjectInfo {
                size: object.size,
                etag: object.etag.clone(),
                last_modified: object.last_modified,
            })
        })
    }

    fn read_full(&self, bucket: &str, key: &str) -> Result<Bytes, object_store::Error> {
        self.with_object(bucket, key, |state, object| {
            state
                .read_body(&object.body)
                .map_err(|e| generic(format!("reading s3://{bucket}/{key}: {e}")))
        })
    }

    /// Read `[start, end)`; an out-of-bounds range is an explicit error.
    fn read_range(
        &self,
        bucket: &str,
        key: &str,
        range: Range<u64>,
    ) -> Result<Bytes, object_store::Error> {
        self.with_object(bucket, key, |state, object| {
            if range.start > range.end || range.end > object.size {
                return Err(generic(format!(
                    "range {}..{} is out of bounds for s3://{bucket}/{key} ({} bytes)",
                    range.start, range.end, object.size
                )));
            }
            state
                .read_body_range(&object.body, range.start, range.end - range.start)
                .map_err(|e| generic(format!("reading range of s3://{bucket}/{key}: {e}")))
        })
    }

    fn read_suffix(
        &self,
        bucket: &str,
        key: &str,
        length: u64,
    ) -> Result<Bytes, object_store::Error> {
        let info = self.object_info(bucket, key)?;
        let start = info.size.saturating_sub(length);
        self.read_range(bucket, key, start..info.size)
    }

    /// List objects under `prefix` (no delimiter). A missing bucket is an
    /// error; an existing bucket with no matches is an empty list.
    fn list_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, ObjectInfo)>, object_store::Error> {
        let accounts = self.state.read();
        let bucket_state = accounts
            .get(&self.account_id)
            .and_then(|account| account.buckets.get(bucket))
            .ok_or_else(|| not_found("NoSuchBucket", &format!("{bucket}/{prefix}")))?;
        Ok(bucket_state
            .objects
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(_, object)| !object.is_delete_marker)
            .map(|(key, object)| {
                (
                    key.clone(),
                    ObjectInfo {
                        size: object.size,
                        etag: object.etag.clone(),
                        last_modified: object.last_modified,
                    },
                )
            })
            .collect())
    }

    /// Synthesize the request fakecloud's dispatcher would have built for a
    /// path-style S3 call and hand it to the S3 service handler directly.
    async fn invoke_s3(
        &self,
        method: Method,
        bucket: &str,
        key: &str,
        body: Bytes,
    ) -> Result<(), object_store::Error> {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("s3.glaux.local"));
        if method == Method::PUT {
            headers.insert(
                "content-length",
                HeaderValue::from_str(&body.len().to_string())
                    .expect("decimal length is a valid header value"),
            );
        }
        // fakecloud percent-decodes the key it slices out of `raw_path`, so
        // the only byte that must be escaped to round-trip is `%` itself.
        let raw_path = format!("/{bucket}/{}", key.replace('%', "%25"));
        let path_segments = std::iter::once(bucket.to_string())
            .chain(key.split('/').map(str::to_string))
            .collect();
        // fakecloud dispatches `PUT /bucket/key` as a streaming route: the
        // handler reads `body_stream` (and refuses a buffered body), so hand
        // it the bytes as a one-chunk stream exactly as the dispatcher would.
        let (body, body_stream) = if method == Method::PUT {
            (Bytes::new(), Some(axum::body::Body::from(body)))
        } else {
            (body, None)
        };
        let request = AwsRequest {
            service: "s3".to_string(),
            action: String::new(),
            region: self.region.clone(),
            account_id: self.account_id.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers,
            query_params: HashMap::new(),
            body,
            body_stream: parking_lot::Mutex::new(body_stream),
            path_segments,
            raw_path,
            raw_query: String::new(),
            method: method.clone(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let response = self.service.handle(request).await.map_err(|e| {
            generic(format!(
                "fakecloud S3 refused {method} s3://{bucket}/{key}: {}: {}",
                e.code(),
                e.message()
            ))
        })?;
        if response.status.is_success() {
            return Ok(());
        }
        let detail = match &response.body {
            ResponseBody::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            ResponseBody::File { size, .. } => format!("<{size}-byte file body>"),
        };
        if response.status == StatusCode::NOT_FOUND {
            return Err(not_found(&detail, &format!("{bucket}/{key}")));
        }
        Err(generic(format!(
            "fakecloud S3 answered {} to {method} s3://{bucket}/{key}: {detail}",
            response.status
        )))
    }
}

#[async_trait]
impl StorageBackend for InProcessStorage {
    async fn get_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<Bytes> {
        self.read_full(bucket, key)
            .map_err(|e| storage_error("get", bucket, key, e))
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: Range<u64>,
    ) -> glaux_catalog::Result<Bytes> {
        self.read_range(bucket, key, range)
            .map_err(|e| storage_error("get_range", bucket, key, e))
    }

    async fn get_object_suffix(
        &self,
        bucket: &str,
        key: &str,
        length: u64,
    ) -> glaux_catalog::Result<Bytes> {
        self.read_suffix(bucket, key, length)
            .map_err(|e| storage_error("get_suffix", bucket, key, e))
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> glaux_catalog::Result<()> {
        self.invoke_s3(Method::PUT, bucket, key, data)
            .await
            .map_err(|e| storage_error("put", bucket, key, e))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<()> {
        // S3 `DeleteObject` on a missing key succeeds; on a missing bucket
        // it is `NoSuchBucket`, which fakecloud reports and we propagate.
        self.invoke_s3(Method::DELETE, bucket, key, Bytes::new())
            .await
            .map_err(|e| storage_error("delete", bucket, key, e))
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
        Ok(self
            .list_prefix(bucket, prefix)
            .map_err(|e| storage_error("list", bucket, prefix, e))?
            .into_iter()
            .map(|(key, info)| ObjectSummary {
                key,
                size: info.size,
            })
            .collect())
    }

    fn object_store(&self, bucket: &str) -> glaux_catalog::Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(InProcessObjectStore {
            storage: Arc::new(Self {
                state: Arc::clone(&self.state),
                service: Arc::clone(&self.service),
                account_id: self.account_id.clone(),
                region: self.region.clone(),
            }),
            bucket: bucket.to_string(),
        }))
    }
}

/// [`ObjectStore`] view of one bucket of in-process fakecloud S3 state, for
/// DataFusion's runtime and the Parquet/CSV/JSON readers behind it.
///
/// Only the operations the read engines and result writers need are
/// implemented; everything else (multipart uploads, copies, conditional
/// puts) returns [`object_store::Error::NotImplemented`] naming the
/// operation rather than pretending to succeed. `head` and `delete` come
/// from the trait's defaults over `get_opts` / `delete_stream`.
pub struct InProcessObjectStore {
    storage: Arc<InProcessStorage>,
    bucket: String,
}

impl fmt::Debug for InProcessObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessObjectStore")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl fmt::Display for InProcessObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InProcessObjectStore(s3://{})", self.bucket)
    }
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_string(),
        implementer: "glaux InProcessObjectStore".to_string(),
    }
}

fn meta(key: &str, info: &ObjectInfo) -> ObjectMeta {
    ObjectMeta {
        location: ObjectPath::from(key),
        last_modified: info.last_modified,
        size: info.size,
        e_tag: Some(info.etag.clone()),
        version: None,
    }
}

#[async_trait]
impl ObjectStore for InProcessObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => return Err(not_implemented("put_opts(mode = Create)")),
            PutMode::Update(_) => return Err(not_implemented("put_opts(mode = Update)")),
        }
        let key = location.as_ref();
        self.storage
            .invoke_s3(Method::PUT, &self.bucket, key, Bytes::from(payload))
            .await?;
        let info = self.storage.object_info(&self.bucket, key)?;
        Ok(PutResult {
            e_tag: Some(info.etag),
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let key = location.as_ref();
        let info = self.storage.object_info(&self.bucket, key)?;
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return Err(not_implemented("get_opts with preconditions or versions"));
        }
        let range = match options.range {
            None => 0..info.size,
            Some(GetRange::Bounded(r)) => r,
            Some(GetRange::Offset(start)) => start..info.size,
            Some(GetRange::Suffix(len)) => info.size.saturating_sub(len)..info.size,
        };
        let bytes = if options.head {
            Bytes::new()
        } else {
            self.storage.read_range(&self.bucket, key, range.clone())?
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta: meta(key, &info),
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        locations
            .then(move |location| {
                let storage = Arc::clone(&storage);
                let bucket = bucket.clone();
                async move {
                    let location = location?;
                    storage
                        .invoke_s3(Method::DELETE, &bucket, location.as_ref(), Bytes::new())
                        .await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // object_store paths never carry a trailing slash, but S3 prefixes
        // used as "directories" do: restore it so `events` does not match
        // `events_archive/...`.
        let prefix = prefix
            .map(|p| format!("{}/", p.as_ref()))
            .unwrap_or_default();
        match self.storage.list_prefix(&self.bucket, &prefix) {
            Ok(entries) => stream::iter(
                entries
                    .into_iter()
                    .map(|(key, info)| Ok(meta(&key, &info)))
                    .collect::<Vec<_>>(),
            )
            .boxed(),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let prefix = prefix
            .map(|p| format!("{}/", p.as_ref()))
            .unwrap_or_default();
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        for (key, info) in self.storage.list_prefix(&self.bucket, &prefix)? {
            let rest = &key[prefix.len()..];
            match rest.find('/') {
                Some(idx) => {
                    let common = ObjectPath::from(&key[..prefix.len() + idx]);
                    if common_prefixes.last() != Some(&common) {
                        common_prefixes.push(common);
                    }
                }
                None => objects.push(meta(&key, &info)),
            }
        }
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_implemented("copy_opts"))
    }
}
