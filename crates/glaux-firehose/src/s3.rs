//! The S3 delivery sink: flushed buffers become real S3 objects.
//!
//! For every [`FlushBatch`] the sink:
//!
//! 1. resolves the data prefix ([`prefix::resolve_data_prefix`]) and the
//!    object name (`<stream>-<version>-<yyyy-MM-dd-HH-mm-ss>-<uuid>`) from
//!    the flush time;
//! 2. without format conversion, concatenates the records exactly as sent
//!    (Firehose adds no delimiters), applies `CompressionFormat` (GZIP adds
//!    `.gz`; `FileExtension` overrides the suffix) and writes one object;
//! 3. with format conversion, fetches the Glue table named by
//!    `SchemaConfiguration`, converts the records to one Parquet object
//!    ([`convert::convert`]) and writes it with a `.parquet` suffix, then
//!    writes every failed record — wrapped in Firehose's
//!    format-conversion-failure envelope, one JSON line each — to a second
//!    object under `ErrorOutputPrefix`.
//!
//! Any failure to write returns a [`SinkError`], so the buffer keeps the
//! records and retries; the sink never acknowledges a batch it did not
//! fully persist. Objects written before the failure are left in place
//! (S3 has no transactions; the retry writes new, uniquely named objects).

use std::io::Write as _;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use glaux_catalog::{GlueApi, StorageBackend};

use crate::convert::{self, FailedRecord};
use crate::model::{CompressionFormat, ExtendedS3DestinationDescription, SchemaConfiguration};
use crate::prefix::{self, ErrorOutputType};
use crate::sink::{DeliverySink, FlushBatch, SinkError};

/// Delivers flushed buffers to S3 through a [`StorageBackend`], resolving
/// conversion schemas through a [`GlueApi`].
pub struct S3DeliverySink {
    storage: Arc<dyn StorageBackend>,
    glue: Arc<dyn GlueApi>,
}

impl std::fmt::Debug for S3DeliverySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DeliverySink").finish_non_exhaustive()
    }
}

/// What one delivery wrote, for logging and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryReport {
    /// `s3://bucket/key` of the data object, if any records were delivered.
    pub data_key: Option<String>,
    /// Records in the data object.
    pub delivered_records: usize,
    /// `s3://bucket/key` of the error object, if any records failed.
    pub error_key: Option<String>,
    /// Records in the error object.
    pub failed_records: usize,
}

impl S3DeliverySink {
    /// Build a sink writing through `storage` and reading schemas from
    /// `glue`.
    pub fn new(storage: Arc<dyn StorageBackend>, glue: Arc<dyn GlueApi>) -> Self {
        Self { storage, glue }
    }

    /// Deliver one batch, returning what was written.
    pub async fn deliver_batch(&self, batch: &FlushBatch) -> Result<DeliveryReport, SinkError> {
        let destination = &batch.destination;
        let bucket = destination.bucket().to_string();
        let at: DateTime<Utc> = batch.flushed_at.into();
        let name = prefix::object_name(&batch.stream_name, &batch.stream_version, at);
        let data_prefix = prefix::resolve_data_prefix(destination.prefix.as_deref(), at)
            .map_err(|e| SinkError::new(format!("Prefix: {e}")))?;

        if !destination.conversion_enabled() {
            // Compressing (or even concatenating) up to a 128 MB flush is
            // CPU-bound and synchronous; on a Tokio worker it would stall
            // every other stream's flush timer scheduled there.
            let records = batch.records.clone();
            let compression = destination.compression_format;
            let (body, suffix) =
                tokio::task::spawn_blocking(move || encode_raw(&records, compression))
                    .await
                    .map_err(|e| {
                        SinkError::new(format!("S3 delivery encode task failed: {e}"))
                    })??;
            let key = format!("{data_prefix}{name}{}", file_suffix(destination, suffix));
            self.put(&bucket, &key, body).await?;
            return Ok(DeliveryReport {
                data_key: Some(format!("s3://{bucket}/{key}")),
                delivered_records: batch.records.len(),
                error_key: None,
                failed_records: 0,
            });
        }

        let config = destination
            .data_format_conversion_configuration
            .as_ref()
            .expect("conversion_enabled implies configuration");
        let schema = config
            .schema_configuration
            .as_ref()
            .ok_or_else(|| SinkError::new("SchemaConfiguration is missing from the destination"))?;
        let table = self.resolve_table(schema).await?;
        // JSON decode plus Parquet encode: likewise CPU-bound, and heavier
        // than gzip. Keep it off the async worker.
        let records = batch.records.clone();
        let config = config.clone();
        let converted =
            tokio::task::spawn_blocking(move || convert::convert(&records, &table, &config))
                .await
                .map_err(|e| SinkError::new(format!("S3 delivery conversion task failed: {e}")))?
                .map_err(|e| {
                    SinkError::new(format!(
                        "record format conversion for stream {} failed: {e}",
                        batch.stream_name
                    ))
                })?;

        let mut report = DeliveryReport {
            data_key: None,
            delivered_records: converted.converted_records,
            error_key: None,
            failed_records: converted.failed.len(),
        };
        if let Some(parquet) = converted.parquet {
            let key = format!(
                "{data_prefix}{name}{}",
                file_suffix(destination, ".parquet")
            );
            self.put(&bucket, &key, parquet).await?;
            report.data_key = Some(format!("s3://{bucket}/{key}"));
        }
        if !converted.failed.is_empty() {
            let error_prefix = prefix::resolve_error_prefix(
                destination.error_output_prefix.as_deref(),
                at,
                ErrorOutputType::FormatConversionFailed,
            )
            .map_err(|e| SinkError::new(format!("ErrorOutputPrefix: {e}")))?;
            let key = format!("{error_prefix}{name}");
            let body = error_object(&converted.failed, batch.opened_at, batch.flushed_at, schema);
            self.put(&bucket, &key, body).await?;
            report.error_key = Some(format!("s3://{bucket}/{key}"));
        }
        tracing::info!(
            stream = %batch.stream_name,
            reason = ?batch.reason,
            data = ?report.data_key,
            errors = ?report.error_key,
            delivered = report.delivered_records,
            failed = report.failed_records,
            "firehose batch delivered"
        );
        Ok(report)
    }

    async fn resolve_table(
        &self,
        schema: &SchemaConfiguration,
    ) -> Result<glaux_catalog::GlueTable, SinkError> {
        let database = schema
            .database_name
            .as_deref()
            .ok_or_else(|| SinkError::new("SchemaConfiguration.DatabaseName is missing"))?;
        let table = schema
            .table_name
            .as_deref()
            .ok_or_else(|| SinkError::new("SchemaConfiguration.TableName is missing"))?;
        if let Some(version) = schema.version_id.as_deref()
            && version != "LATEST"
        {
            return Err(SinkError::new(format!(
                "SchemaConfiguration.VersionId {version:?} is not supported by glaux: only \
                 LATEST can be resolved"
            )));
        }
        self.glue.get_table(database, table).await.map_err(|e| {
            SinkError::new(format!(
                "failed to resolve Glue table {database}.{table} for record format conversion: \
                 {e}"
            ))
        })
    }

    async fn put(&self, bucket: &str, key: &str, body: Bytes) -> Result<(), SinkError> {
        self.storage
            .put_object(bucket, key, body)
            .await
            .map_err(|e| SinkError::new(format!("failed to write s3://{bucket}/{key}: {e}")))
    }
}

/// Concatenate records and apply `CompressionFormat`, returning the body
/// and the file suffix the format implies.
fn encode_raw(
    records: &[Bytes],
    compression: CompressionFormat,
) -> Result<(Bytes, &'static str), SinkError> {
    let total: usize = records.iter().map(Bytes::len).sum();
    match compression {
        CompressionFormat::Uncompressed => {
            let mut body = Vec::with_capacity(total);
            for record in records {
                body.extend_from_slice(record);
            }
            Ok((Bytes::from(body), ""))
        }
        CompressionFormat::Gzip => {
            let mut encoder = GzEncoder::new(Vec::with_capacity(total / 2), Compression::default());
            for record in records {
                encoder
                    .write_all(record)
                    .map_err(|e| SinkError::new(format!("gzip failed: {e}")))?;
            }
            let body = encoder
                .finish()
                .map_err(|e| SinkError::new(format!("gzip failed: {e}")))?;
            Ok((Bytes::from(body), ".gz"))
        }
        other => Err(SinkError::new(format!(
            "CompressionFormat {} is not implemented by glaux: use UNCOMPRESSED or GZIP",
            other.as_str()
        ))),
    }
}

/// `FileExtension` overrides the format-implied suffix when set.
fn file_suffix<'a>(destination: &'a ExtendedS3DestinationDescription, implied: &'a str) -> &'a str {
    destination.file_extension.as_deref().unwrap_or(implied)
}

/// The error object body: one envelope per line.
fn error_object(
    failed: &[FailedRecord],
    opened_at: SystemTime,
    flushed_at: SystemTime,
    schema: &SchemaConfiguration,
) -> Bytes {
    let mut body = Vec::new();
    for record in failed {
        let envelope = record.envelope(opened_at, flushed_at, schema);
        serde_json::to_writer(&mut body, &envelope).expect("serde_json::Value always serializes");
        body.push(b'\n');
    }
    Bytes::from(body)
}

#[async_trait]
impl DeliverySink for S3DeliverySink {
    async fn deliver(&self, batch: FlushBatch) -> Result<(), SinkError> {
        self.deliver_batch(&batch).await.map(drop)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Read as _;
    use std::ops::Range;
    use std::sync::Mutex;
    use std::time::Duration;

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Int64Type};
    use base64::Engine as _;
    use chrono::TimeZone;
    use flate2::read::GzDecoder;
    use glaux_catalog::{
        CatalogError, GlueColumn, GlueDatabase, GluePartition, GlueStorageDescriptor, GlueTable,
        ObjectSummary,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::{Value, json};

    use super::*;
    use crate::sink::FlushReason;

    /// In-memory S3: a map of `bucket/key` → bytes. A bucket named
    /// `missing-bucket` rejects every put.
    #[derive(Default, Debug)]
    struct MemoryStorage {
        objects: Mutex<BTreeMap<String, Bytes>>,
    }

    impl MemoryStorage {
        fn keys(&self, bucket: &str) -> Vec<String> {
            let prefix = format!("{bucket}/");
            self.objects
                .lock()
                .unwrap()
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
                .collect()
        }
        fn get(&self, bucket: &str, key: &str) -> Bytes {
            self.objects
                .lock()
                .unwrap()
                .get(&format!("{bucket}/{key}"))
                .cloned()
                .unwrap_or_else(|| panic!("no object s3://{bucket}/{key}"))
        }
    }

    fn not_found(bucket: &str, key: &str) -> CatalogError {
        CatalogError::Storage {
            operation: "get",
            bucket: bucket.to_string(),
            key: key.to_string(),
            source: Box::new(object_store_not_found(key)),
        }
    }

    fn object_store_not_found(key: &str) -> object_store::Error {
        object_store::Error::NotFound {
            path: key.to_string(),
            source: "missing".into(),
        }
    }

    #[async_trait]
    impl StorageBackend for MemoryStorage {
        async fn get_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<Bytes> {
            self.objects
                .lock()
                .unwrap()
                .get(&format!("{bucket}/{key}"))
                .cloned()
                .ok_or_else(|| not_found(bucket, key))
        }
        async fn get_object_range(
            &self,
            bucket: &str,
            key: &str,
            range: Range<u64>,
        ) -> glaux_catalog::Result<Bytes> {
            let all = self.get_object(bucket, key).await?;
            Ok(all.slice(range.start as usize..range.end as usize))
        }
        async fn get_object_suffix(
            &self,
            bucket: &str,
            key: &str,
            length: u64,
        ) -> glaux_catalog::Result<Bytes> {
            let all = self.get_object(bucket, key).await?;
            let start = all.len().saturating_sub(length as usize);
            Ok(all.slice(start..))
        }
        async fn put_object(
            &self,
            bucket: &str,
            key: &str,
            data: Bytes,
        ) -> glaux_catalog::Result<()> {
            if bucket == "missing-bucket" {
                return Err(CatalogError::Storage {
                    operation: "put",
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    source: Box::new(object_store_not_found(key)),
                });
            }
            self.objects
                .lock()
                .unwrap()
                .insert(format!("{bucket}/{key}"), data);
            Ok(())
        }
        async fn delete_object(&self, bucket: &str, key: &str) -> glaux_catalog::Result<()> {
            self.objects
                .lock()
                .unwrap()
                .remove(&format!("{bucket}/{key}"));
            Ok(())
        }
        async fn list_objects(
            &self,
            bucket: &str,
            prefix: &str,
        ) -> glaux_catalog::Result<Vec<ObjectSummary>> {
            let full = format!("{bucket}/{prefix}");
            Ok(self
                .objects
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.starts_with(&full))
                .map(|(k, v)| ObjectSummary {
                    key: k[bucket.len() + 1..].to_string(),
                    size: v.len() as u64,
                })
                .collect())
        }
        fn object_store(
            &self,
            bucket: &str,
        ) -> glaux_catalog::Result<Arc<dyn object_store::ObjectStore>> {
            Err(CatalogError::StorageClient {
                bucket: bucket.to_string(),
                source: Box::new(object_store::Error::Generic {
                    store: "memory",
                    source: "not an object_store".into(),
                }),
            })
        }
    }

    /// A Glue catalog holding a fixed set of tables.
    struct FakeGlue {
        tables: Vec<GlueTable>,
    }

    #[async_trait]
    impl GlueApi for FakeGlue {
        async fn get_databases(&self) -> glaux_catalog::Result<Vec<GlueDatabase>> {
            Ok(vec![])
        }
        async fn get_tables(&self, _database: &str) -> glaux_catalog::Result<Vec<GlueTable>> {
            Ok(self.tables.clone())
        }
        async fn get_table(&self, database: &str, table: &str) -> glaux_catalog::Result<GlueTable> {
            self.tables
                .iter()
                .find(|t| t.name == table && t.database_name.as_deref() == Some(database))
                .cloned()
                .ok_or_else(|| CatalogError::GlueEntityNotFound {
                    message: format!("Entity Not Found: {database}.{table}"),
                })
        }
        async fn get_partitions(
            &self,
            _database: &str,
            _table: &str,
            _expression: Option<&str>,
        ) -> glaux_catalog::Result<Vec<GluePartition>> {
            Ok(vec![])
        }
    }

    fn orders_table() -> GlueTable {
        let column = |name: &str, ty: &str| GlueColumn {
            name: name.to_string(),
            column_type: Some(ty.to_string()),
            comment: None,
        };
        GlueTable {
            name: "orders".to_string(),
            database_name: Some("sales".to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: Some(GlueStorageDescriptor {
                columns: vec![
                    column("id", "bigint"),
                    column("customer", "string"),
                    column("total", "decimal(10,2)"),
                ],
                ..Default::default()
            }),
            partition_keys: vec![],
            parameters: Default::default(),
        }
    }

    fn destination(overrides: Value) -> Arc<ExtendedS3DestinationDescription> {
        let mut base = json!({
            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
            "BucketARN": "arn:aws:s3:::events",
            "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
            "CompressionFormat": "UNCOMPRESSED",
            "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
        });
        for (k, v) in overrides.as_object().unwrap() {
            base[k] = v.clone();
        }
        Arc::new(serde_json::from_value(base).unwrap())
    }

    fn conversion() -> Value {
        json!({
            "Enabled": true,
            "SchemaConfiguration": {"DatabaseName": "sales", "TableName": "orders", "Region": "us-east-1"},
            "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
            "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}},
        })
    }

    fn flushed_at() -> SystemTime {
        Utc.with_ymd_and_hms(2026, 8, 21, 13, 7, 9).unwrap().into()
    }

    fn batch(
        destination: Arc<ExtendedS3DestinationDescription>,
        records: Vec<&'static [u8]>,
    ) -> FlushBatch {
        FlushBatch {
            stream_name: "orders".to_string(),
            stream_arn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/orders".to_string(),
            stream_version: "2".to_string(),
            destination,
            records: records.into_iter().map(Bytes::from_static).collect(),
            reason: FlushReason::Interval,
            opened_at: flushed_at() - Duration::from_secs(120),
            flushed_at: flushed_at(),
        }
    }

    fn sink() -> (S3DeliverySink, Arc<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::default());
        let glue = Arc::new(FakeGlue {
            tables: vec![orders_table()],
        });
        (
            S3DeliverySink::new(Arc::clone(&storage) as Arc<dyn StorageBackend>, glue),
            storage,
        )
    }

    fn assert_object_name(key: &str, prefix: &str, suffix: &str) {
        let name = key
            .strip_prefix(prefix)
            .unwrap_or_else(|| panic!("{key} does not start with {prefix}"));
        let name = name
            .strip_suffix(suffix)
            .unwrap_or_else(|| panic!("{key} does not end with {suffix:?}"));
        let expected_head = "orders-2-2026-08-21-13-07-09-";
        assert!(name.starts_with(expected_head), "{name}");
        uuid::Uuid::parse_str(&name[expected_head.len()..]).expect("uuid suffix");
    }

    #[tokio::test]
    async fn raw_records_land_under_the_default_prefix() {
        let (sink, storage) = sink();
        let report = sink
            .deliver_batch(&batch(
                destination(json!({})),
                vec![b"{\"a\":1}\n", b"{\"a\":2}\n"],
            ))
            .await
            .unwrap();
        let keys = storage.keys("events");
        assert_eq!(keys.len(), 1);
        assert_object_name(&keys[0], "2026/08/21/13/", "");
        assert_eq!(storage.get("events", &keys[0]), "{\"a\":1}\n{\"a\":2}\n");
        assert_eq!(report.delivered_records, 2);
        assert_eq!(
            report.data_key.as_deref(),
            Some(&*format!("s3://events/{}", keys[0]))
        );
    }

    #[tokio::test]
    async fn custom_prefix_and_gzip_round_trip() {
        let (sink, storage) = sink();
        let dest = destination(json!({
            "Prefix": "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/",
            "CompressionFormat": "GZIP",
        }));
        sink.deliver_batch(&batch(dest, vec![b"hello ", b"world"]))
            .await
            .unwrap();
        let keys = storage.keys("events");
        assert_object_name(&keys[0], "raw/year=2026/month=08/", ".gz");
        let mut decoded = String::new();
        GzDecoder::new(&storage.get("events", &keys[0])[..])
            .read_to_string(&mut decoded)
            .unwrap();
        assert_eq!(decoded, "hello world");
    }

    #[tokio::test]
    async fn file_extension_overrides_the_suffix() {
        let (sink, storage) = sink();
        let dest = destination(json!({"Prefix": "x/", "FileExtension": ".jsonl"}));
        sink.deliver_batch(&batch(dest, vec![b"{}"])).await.unwrap();
        assert_object_name(&storage.keys("events")[0], "x/2026/08/21/13/", ".jsonl");
    }

    #[tokio::test]
    async fn unimplemented_compression_is_an_error_not_a_plain_object() {
        let (sink, storage) = sink();
        let dest = destination(json!({"CompressionFormat": "Snappy"}));
        let err = sink
            .deliver_batch(&batch(dest, vec![b"{}"]))
            .await
            .unwrap_err();
        assert!(err.message.contains("Snappy"), "{err}");
        assert!(storage.keys("events").is_empty());
    }

    #[tokio::test]
    async fn storage_failure_is_reported_so_the_buffer_retries() {
        let (sink, _storage) = sink();
        let dest = destination(json!({"BucketARN": "arn:aws:s3:::missing-bucket"}));
        let err = sink
            .deliver_batch(&batch(dest, vec![b"{}"]))
            .await
            .unwrap_err();
        assert!(err.message.contains("missing-bucket"), "{err}");
    }

    #[tokio::test]
    async fn conversion_writes_parquet_and_error_objects() {
        let (sink, storage) = sink();
        let dest = destination(json!({
            "Prefix": "tables/orders/",
            "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/!{timestamp:yyyy-MM-dd}/",
            "DataFormatConversionConfiguration": conversion(),
        }));
        let report = sink
            .deliver_batch(&batch(
                dest,
                vec![
                    br#"{"id": 1, "customer": "ada", "total": "12.50"}"#,
                    br#"{"id": "one", "customer": "bad"}"#,
                    b"garbage",
                    br#"{"id": 2, "customer": "bob", "total": 3}"#,
                ],
            ))
            .await
            .unwrap();
        assert_eq!(report.delivered_records, 2);
        assert_eq!(report.failed_records, 2);

        let keys = storage.keys("events");
        assert_eq!(keys.len(), 2, "{keys:?}");
        let data_key = keys.iter().find(|k| k.starts_with("tables/")).unwrap();
        let error_key = keys.iter().find(|k| k.starts_with("errors/")).unwrap();
        assert_object_name(data_key, "tables/orders/2026/08/21/13/", ".parquet");
        assert_object_name(error_key, "errors/format-conversion-failed/2026-08-21/", "");

        // Parquet read-back: schema follows the Glue types, values the JSON.
        let parquet = storage.get("events", data_key);
        let reader = ParquetRecordBatchReaderBuilder::try_new(parquet).unwrap();
        let schema = Arc::clone(reader.schema());
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(10, 2));
        let batches: Vec<_> = reader.build().unwrap().map(Result::unwrap).collect();
        let rb = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);
        assert_eq!(rb.column(1).as_string::<i32>().value(1), "bob");
        let totals = rb
            .column(2)
            .as_primitive::<arrow::datatypes::Decimal128Type>();
        assert_eq!(totals.value(0), 1250);
        assert_eq!(totals.value(1), 300);

        // Error object: one Firehose envelope per failed record, raw bytes
        // preserved in base64.
        let errors = storage.get("events", error_key);
        let lines: Vec<Value> = std::str::from_utf8(&errors)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0]["lastErrorCode"],
            "DataFormatConversion.InvalidSchemaMapping"
        );
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(lines[0]["rawData"].as_str().unwrap())
                .unwrap(),
            br#"{"id": "one", "customer": "bad"}"#
        );
        assert_eq!(lines[1]["lastErrorCode"], "DataFormatConversion.ParseError");
        assert_eq!(lines[1]["dataCatalogTable"]["tableName"], "orders");
        assert_eq!(lines[1]["attemptsMade"], 1);
        assert!(lines[1]["arrivalTimestamp"].is_u64());
    }

    #[tokio::test]
    async fn conversion_with_only_bad_records_writes_only_the_error_object() {
        let (sink, storage) = sink();
        let dest = destination(json!({"DataFormatConversionConfiguration": conversion()}));
        let report = sink
            .deliver_batch(&batch(dest, vec![b"nope"]))
            .await
            .unwrap();
        assert!(report.data_key.is_none());
        let keys = storage.keys("events");
        assert_eq!(keys.len(), 1);
        assert_object_name(&keys[0], "format-conversion-failed/2026/08/21/13/", "");
    }

    #[tokio::test]
    async fn missing_glue_table_fails_the_batch() {
        let (sink, storage) = sink();
        let mut conv = conversion();
        conv["SchemaConfiguration"]["TableName"] = json!("nope");
        let dest = destination(json!({"DataFormatConversionConfiguration": conv}));
        let err = sink
            .deliver_batch(&batch(dest, vec![b"{\"id\":1}"]))
            .await
            .unwrap_err();
        assert!(err.message.contains("sales.nope"), "{err}");
        assert!(storage.keys("events").is_empty());
    }

    // -----------------------------------------------------------------------
    // End to end: PutRecordBatch -> buffer -> flush -> S3 objects
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn service_flushes_through_the_s3_sink_end_to_end() {
        use crate::service::{FirehoseService, FirehoseServiceConfig};

        let (s3, storage) = sink();
        let service = FirehoseService::new(
            FirehoseServiceConfig::default(),
            Arc::new(s3) as Arc<dyn DeliverySink>,
        );
        let create = json!({
            "DeliveryStreamName": "orders",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                "BucketARN": "arn:aws:s3:::events",
                "Prefix": "tables/orders/!{timestamp:yyyy/MM/dd}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
                "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 900},
                "DataFormatConversionConfiguration": conversion(),
            }
        });
        service
            .handle("CreateDeliveryStream", create.to_string().as_bytes())
            .await
            .unwrap();
        let encode = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
        let put = json!({
            "DeliveryStreamName": "orders",
            "Records": [
                {"Data": encode(r#"{"id": 10, "customer": "x", "total": 1}"#)},
                {"Data": encode("broken")},
            ]
        });
        let out = service
            .handle("PutRecordBatch", put.to_string().as_bytes())
            .await
            .unwrap();
        assert_eq!(out["FailedPutCount"], 0);
        assert!(
            storage.keys("events").is_empty(),
            "nothing flushes before a trigger"
        );

        // Delete flushes the buffer through the sink.
        service
            .handle(
                "DeleteDeliveryStream",
                br#"{"DeliveryStreamName":"orders"}"#,
            )
            .await
            .unwrap();
        let keys = storage.keys("events");
        assert_eq!(keys.len(), 2, "{keys:?}");
        let today = Utc::now().format("tables/orders/%Y/%m/%d/").to_string();
        let data_key = keys.iter().find(|k| k.starts_with(&today)).unwrap();
        assert!(data_key.ends_with(".parquet"), "{data_key}");
        assert!(
            data_key[today.len()..].starts_with("orders-1-"),
            "{data_key}"
        );
        let error_key = keys
            .iter()
            .find(|k| k.starts_with("errors/format-conversion-failed/orders-1-"))
            .unwrap();

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(storage.get("events", data_key)).unwrap();
        let rb = reader.build().unwrap().next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.column(0).as_primitive::<Int64Type>().value(0), 10);

        let errors = storage.get("events", error_key);
        let line: Value = serde_json::from_slice(errors.trim_ascii_end()).unwrap();
        assert_eq!(line["rawData"], encode("broken"));
    }

    #[tokio::test]
    async fn configuration_glaux_cannot_deliver_is_rejected_at_create() {
        use crate::service::{FirehoseService, FirehoseServiceConfig};
        use crate::sink::RecordingSink;

        let service = FirehoseService::new(
            FirehoseServiceConfig::default(),
            Arc::new(RecordingSink::new()) as Arc<dyn DeliverySink>,
        );
        let base = |overrides: Value| {
            let mut dest = json!({
                "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                "BucketARN": "arn:aws:s3:::events",
            });
            for (k, v) in overrides.as_object().unwrap() {
                dest[k] = v.clone();
            }
            json!({"DeliveryStreamName": "s", "ExtendedS3DestinationConfiguration": dest})
        };
        for (overrides, needle) in [
            (
                json!({"CompressionFormat": "Snappy"}),
                "CompressionFormat Snappy",
            ),
            (json!({"CompressionFormat": "ZIP"}), "CompressionFormat ZIP"),
            (
                json!({"Prefix": "a/!{partitionKeyFromQuery:x}/"}),
                "dynamic partitioning",
            ),
            (
                json!({"Prefix": "a/!{timestamp:yyyyQ}/"}),
                "pattern letter 'Q'",
            ),
            (
                json!({"Prefix": "a/!{firehose:error-output-type}/"}),
                "only valid in ErrorOutputPrefix",
            ),
            (
                json!({"ErrorOutputPrefix": "e/!{nope:x}/"}),
                "ErrorOutputPrefix",
            ),
            (json!({"FileExtension": "txt"}), "FileExtension"),
        ] {
            let err = service
                .handle(
                    "CreateDeliveryStream",
                    base(overrides).to_string().as_bytes(),
                )
                .await
                .unwrap_err();
            assert!(err.message().contains(needle), "{err}");
        }
        // GZIP and a valid expression prefix are fine.
        service
            .handle(
                "CreateDeliveryStream",
                base(json!({
                    "CompressionFormat": "GZIP",
                    "Prefix": "a/!{timestamp:yyyy-MM-dd'T'HH}/",
                    "ErrorOutputPrefix": "e/!{firehose:error-output-type}/",
                    "FileExtension": ".json.gz",
                }))
                .to_string()
                .as_bytes(),
            )
            .await
            .unwrap();
    }
}
