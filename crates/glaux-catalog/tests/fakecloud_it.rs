//! Integration test against a real fakecloud process: S3 ranged reads over
//! `object_store` and the Glue read surface, exactly as the acceptance
//! criteria of LIO-19 describe.
//!
//! The fakecloud binary is taken from `PATH` when available, otherwise
//! downloaded once from GitHub releases into a cached temp location. When
//! neither works (e.g. offline CI), the test **skips with a clear message**
//! rather than failing — but it never fakes a result.

use std::net::TcpListener;
use std::ops::Range;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use glaux_catalog::{
    AwsCredentials, CatalogError, ConfigOverrides, GlauxConfig, GlueApi, NetworkGlueApi,
    NetworkStorageBackend, StorageBackend,
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde_json::json;

const FAKECLOUD_VERSION: &str = "v0.44.10";

/// Kills the fakecloud child process when the test ends (pass or fail).
struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn release_platform() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("darwin-arm64"),
        ("macos", "x86_64") => Some("darwin-amd64"),
        ("linux", "aarch64") => Some("linux-arm64"),
        ("linux", "x86_64") => Some("linux-amd64"),
        _ => None,
    }
}

/// Find `fakecloud` on PATH.
fn fakecloud_on_path() -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join("fakecloud"))
        .find(|candidate| candidate.is_file())
}

/// Download and extract the fakecloud release into a cached temp dir.
/// Returns `None` (after printing the reason) when the download fails —
/// typically because the environment is offline.
fn download_fakecloud() -> Option<PathBuf> {
    let platform = release_platform()?;
    let cache_dir = std::env::temp_dir().join(format!("glaux-fakecloud-{FAKECLOUD_VERSION}"));
    let binary = cache_dir
        .join(format!("fakecloud-{FAKECLOUD_VERSION}-{platform}"))
        .join("fakecloud");
    if binary.is_file() {
        return Some(binary);
    }
    std::fs::create_dir_all(&cache_dir).ok()?;

    let url = format!(
        "https://github.com/faiscadev/fakecloud/releases/download/{FAKECLOUD_VERSION}/fakecloud-{FAKECLOUD_VERSION}-{platform}.tar.gz"
    );
    let archive = cache_dir.join("fakecloud.tar.gz");
    eprintln!("downloading fakecloud {FAKECLOUD_VERSION} ({platform}) from GitHub releases...");
    let curl = Command::new("curl")
        .args(["-sSL", "--fail", "--max-time", "300", "-o"])
        .arg(&archive)
        .arg(&url)
        .status();
    match curl {
        Ok(status) if status.success() => {}
        Ok(status) => {
            eprintln!("curl exited with {status} downloading {url}");
            return None;
        }
        Err(e) => {
            eprintln!("could not run curl: {e}");
            return None;
        }
    }
    let tar = Command::new("tar")
        .arg("-xzf")
        .arg(&archive)
        .arg("-C")
        .arg(&cache_dir)
        .status();
    let _ = std::fs::remove_file(&archive);
    match tar {
        Ok(status) if status.success() && binary.is_file() => Some(binary),
        Ok(_) | Err(_) => {
            eprintln!("failed to extract fakecloud archive into {}", cache_dir.display());
            None
        }
    }
}

fn locate_fakecloud() -> Option<PathBuf> {
    fakecloud_on_path().or_else(download_fakecloud)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port 0")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn wait_for_health(port: u16) -> bool {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/_fakecloud/health");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// Build parquet bytes for a small two-column batch; returns (bytes, batch).
fn make_parquet() -> (Bytes, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["athena", "firehose", "glue"])),
        ],
    )
    .expect("record batch");
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    (Bytes::from(buf), batch)
}

/// An async parquet reader whose every fetch goes through the
/// [`StorageBackend`] ranged-read API, recording each range so the test can
/// assert real ranged access (footer + page reads, never a full-object get).
struct RecordingReader {
    storage: Arc<NetworkStorageBackend>,
    bucket: String,
    key: String,
    file_size: u64,
    reads: Arc<Mutex<Vec<Range<u64>>>>,
}

impl RecordingReader {
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, Result<Bytes, ParquetError>> {
        self.reads.lock().unwrap().push(range.clone());
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        async move {
            storage
                .get_object_range(&bucket, &key, range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }
}

/// Adapter so the footer loader also fetches through the recorded path.
struct FooterFetch<'a>(&'a RecordingReader);

impl MetadataFetch for FooterFetch<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        self.0.fetch(range)
    }
}

impl AsyncFileReader for RecordingReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        RecordingReader::fetch(self, range)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(FooterFetch(self), self.file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

#[tokio::test]
async fn fakecloud_s3_and_glue_end_to_end() {
    let Some(binary) = locate_fakecloud() else {
        eprintln!(
            "SKIPPED: fakecloud integration test — fakecloud is not on PATH and could not be \
             downloaded from GitHub releases (offline or unsupported platform \
             {}/{}). Install fakecloud or restore network access to run this test.",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        return;
    };

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(&binary)
        .args(["--addr", &addr, "--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fakecloud");
    let _guard = ProcessGuard(child);
    assert!(
        wait_for_health(port).await,
        "fakecloud did not become healthy on {addr} within 30s"
    );
    let endpoint = format!("http://{addr}");

    let overrides = ConfigOverrides {
        s3_endpoint: Some(endpoint.clone()),
        glue_endpoint: Some(endpoint.clone()),
        region: Some("us-east-1".to_string()),
        credentials: Some(AwsCredentials {
            access_key_id: "glaux-it".to_string(),
            secret_access_key: "glaux-it-secret".to_string(),
            session_token: None,
        }),
        ..Default::default()
    };
    let config = GlauxConfig::load(None, &overrides).expect("config");

    // ---- S3: put parquet bytes, ranged reads, list, full parquet decode ----

    let bucket = "glaux-it";
    // Bucket creation is test fixture setup, outside the StorageBackend
    // surface: raw S3 CreateBucket with a SigV4-shaped Authorization header
    // (fakecloud routes multiplexed services by credential scope).
    let http = reqwest::Client::new();
    let created = http
        .put(format!("{endpoint}/{bucket}"))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=glaux-it/20260101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fixture",
        )
        .send()
        .await
        .expect("create bucket request");
    assert!(
        created.status().is_success(),
        "CreateBucket failed: {}",
        created.status()
    );

    let storage = NetworkStorageBackend::new(&config);
    let (parquet_bytes, original_batch) = make_parquet();
    let key = "data/dt=2026-08-01/part-0.parquet";
    storage
        .put_object(bucket, key, parquet_bytes.clone())
        .await
        .expect("put_object");

    // Ranged read: parquet leading magic.
    let head = storage
        .get_object_range(bucket, key, 0..4)
        .await
        .expect("ranged read");
    assert_eq!(head.as_ref(), b"PAR1", "leading magic via Range header");

    // Suffix read: parquet footer magic (the access pattern readers use).
    let tail = storage
        .get_object_suffix(bucket, key, 4)
        .await
        .expect("suffix read");
    assert_eq!(tail.as_ref(), b"PAR1", "trailing magic via suffix Range");

    // Interior ranged read matches the same slice of the original bytes.
    let mid = storage
        .get_object_range(bucket, key, 10..42)
        .await
        .expect("interior ranged read");
    assert_eq!(mid.as_ref(), &parquet_bytes[10..42]);

    // Full read round-trips byte-for-byte.
    let full = storage.get_object(bucket, key).await.expect("get_object");
    assert_eq!(full, parquet_bytes);

    // Listing sees the object with its exact size.
    let listed = storage
        .list_objects(bucket, "data/")
        .await
        .expect("list_objects");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);
    assert_eq!(listed[0].size, parquet_bytes.len() as u64);

    // Decode the parquet file through a real async parquet reader whose I/O
    // goes exclusively through StorageBackend ranged reads, recording every
    // range so the test can assert the ranged access pattern was observed.
    let reads: Arc<Mutex<Vec<Range<u64>>>> = Arc::new(Mutex::new(Vec::new()));
    let reader = RecordingReader {
        storage: Arc::new(NetworkStorageBackend::new(&config)),
        bucket: bucket.to_string(),
        key: key.to_string(),
        file_size: parquet_bytes.len() as u64,
        reads: Arc::clone(&reads),
    };
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .expect("parquet footer via ranged reads")
        .build()
        .expect("build parquet stream");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("decode parquet");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], original_batch, "decoded batch must round-trip");

    let observed = reads.lock().unwrap().clone();
    assert!(
        observed.len() >= 2,
        "decoding must issue multiple ranged reads (footer + pages), got: {observed:?}"
    );
    let file_size = parquet_bytes.len() as u64;
    assert!(
        observed.iter().all(|r| r.end <= file_size && r.start < r.end),
        "every read must be a proper sub-range: {observed:?}"
    );
    assert!(
        observed.iter().any(|r| r.end == file_size && r.start > 0),
        "the footer must be fetched with a trailing ranged read: {observed:?}"
    );
    assert!(
        observed.iter().all(|r| (r.end - r.start) < file_size),
        "no read may span the whole object — ranged access only: {observed:?}"
    );

    // ---- Glue: provision fixtures, then exercise the read surface ----

    let glue = NetworkGlueApi::new(&config);
    glue.invoke("CreateDatabase", json!({ "DatabaseInput": { "Name": "events" } }))
        .await
        .expect("CreateDatabase");
    glue.invoke(
        "CreateTable",
        json!({
            "DatabaseName": "events",
            "TableInput": {
                "Name": "clicks",
                "TableType": "EXTERNAL_TABLE",
                "PartitionKeys": [{ "Name": "dt", "Type": "string" }],
                "StorageDescriptor": {
                    "Columns": [
                        { "Name": "id", "Type": "bigint" },
                        { "Name": "name", "Type": "string" },
                    ],
                    "Location": format!("s3://{bucket}/data/"),
                    "SerdeInfo": {
                        "SerializationLibrary":
                            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            },
        }),
    )
    .await
    .expect("CreateTable");
    for dt in ["2026-08-01", "2026-08-02"] {
        glue.invoke(
            "CreatePartition",
            json!({
                "DatabaseName": "events",
                "TableName": "clicks",
                "PartitionInput": {
                    "Values": [dt],
                    "StorageDescriptor": {
                        "Location": format!("s3://{bucket}/data/dt={dt}/"),
                    },
                },
            }),
        )
        .await
        .expect("CreatePartition");
    }

    let databases = glue.get_databases().await.expect("get_databases");
    assert!(
        databases.iter().any(|d| d.name == "events"),
        "created database must be listed, got: {databases:?}"
    );

    let tables = glue.get_tables("events").await.expect("get_tables");
    assert_eq!(
        tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
        vec!["clicks"]
    );

    let table = glue.get_table("events", "clicks").await.expect("get_table");
    assert_eq!(table.name, "clicks");
    assert_eq!(table.partition_keys.len(), 1);
    assert_eq!(table.partition_keys[0].name, "dt");
    let sd = table.storage_descriptor.expect("storage descriptor");
    assert_eq!(sd.location.as_deref(), Some(format!("s3://{bucket}/data/").as_str()));
    assert_eq!(
        sd.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(
        sd.serde_info
            .expect("serde info")
            .serialization_library
            .as_deref(),
        Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
    );

    let mut partitions = glue
        .get_partitions("events", "clicks", None)
        .await
        .expect("get_partitions");
    partitions.sort_by(|a, b| a.values.cmp(&b.values));
    assert_eq!(
        partitions.iter().map(|p| p.values.clone()).collect::<Vec<_>>(),
        vec![vec!["2026-08-01".to_string()], vec!["2026-08-02".to_string()]]
    );

    // A missing table errors explicitly — never an empty fabricated result.
    let err = glue
        .get_table("events", "does_not_exist")
        .await
        .expect_err("missing table must error");
    assert!(
        matches!(
            err,
            CatalogError::GlueEntityNotFound { .. } | CatalogError::GlueApi { .. }
        ),
        "got: {err}"
    );
}
