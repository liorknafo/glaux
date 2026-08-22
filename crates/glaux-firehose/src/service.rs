//! The Firehose service: delivery stream registry, request validation, and
//! the action dispatcher.
//!
//! [`FirehoseService::handle`] is the transport-independent entry point
//! (action name + JSON body → JSON body or [`FirehoseError`]); the axum
//! layer in [`crate::http`] and fakecloud's `AwsService` adapter both sit
//! on top of it.
//!
//! Every stream owns a [`StreamBuffer`]; `PutRecord` / `PutRecordBatch`
//! push into it and the buffer hands flushed batches to the configured
//! [`DeliverySink`]. [`FirehoseService::shutdown`] flushes every stream.
//!
//! # Never silently wrong
//!
//! Configuration glaux cannot honor — Kinesis/MSK sources, non-S3
//! destinations, Lambda processing, dynamic partitioning, source backup,
//! KMS encryption, customer-managed SSE keys — is rejected at `CreateDeliveryStream` /
//! `UpdateDestination` time with an error naming the construct, rather than
//! being accepted and ignored.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::buffer::StreamBuffer;
use crate::convert;
use crate::error::FirehoseError;
use crate::model::*;
use crate::prefix::{PrefixError, validate_prefix};
use crate::sink::{DeliverySink, FlushReason, SinkError};

const DEFAULT_LIST_LIMIT: usize = 10;
const MAX_LIST_LIMIT: usize = 10_000;
const DESTINATION_ID: &str = "destinationId-000000000001";
const STREAM_TYPE_DIRECT_PUT: &str = "DirectPut";

/// Service-level settings.
#[derive(Debug, Clone)]
pub struct FirehoseServiceConfig {
    /// Region used in stream ARNs.
    pub region: String,
    /// Account id used in stream ARNs and error messages.
    pub account_id: String,
    /// Maximum decoded size of one record, in bytes (AWS: 1 MiB).
    pub max_record_bytes: usize,
    /// Maximum number of records per `PutRecordBatch` (AWS: 500).
    pub max_batch_records: usize,
    /// Maximum decoded payload per `PutRecordBatch`, in bytes (AWS: 4 MiB).
    pub max_batch_bytes: usize,
    /// Maximum number of delivery streams per account (AWS default: 5000).
    pub max_streams: usize,
}

impl Default for FirehoseServiceConfig {
    fn default() -> Self {
        Self::from_limits(
            "us-east-1",
            "000000000000",
            &glaux_catalog::FirehoseLimits::default(),
        )
    }
}

impl FirehoseServiceConfig {
    /// Build from the shared [`glaux_catalog::FirehoseLimits`].
    pub fn from_limits(
        region: impl Into<String>,
        account_id: impl Into<String>,
        limits: &glaux_catalog::FirehoseLimits,
    ) -> Self {
        let clamp = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
        Self {
            region: region.into(),
            account_id: account_id.into(),
            max_record_bytes: clamp(limits.max_record_kib.saturating_mul(1024)),
            max_batch_records: clamp(limits.max_batch_records),
            max_batch_bytes: clamp(limits.max_batch_mib.saturating_mul(1024 * 1024)),
            max_streams: 5000,
        }
    }
}

impl From<&glaux_catalog::GlauxConfig> for FirehoseServiceConfig {
    fn from(config: &glaux_catalog::GlauxConfig) -> Self {
        Self::from_limits(&config.region, &config.account_id, &config.firehose)
    }
}

struct StreamEntry {
    description: DeliveryStreamDescription,
    tags: Vec<Tag>,
    buffer: Arc<StreamBuffer>,
}

/// The Firehose service. Cheap to share behind an `Arc`.
pub struct FirehoseService {
    config: FirehoseServiceConfig,
    sink: Arc<dyn DeliverySink>,
    streams: Mutex<BTreeMap<String, StreamEntry>>,
}

impl std::fmt::Debug for FirehoseService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirehoseService")
            .field("config", &self.config)
            .field("streams", &self.streams.lock().unwrap().len())
            .finish_non_exhaustive()
    }
}

fn now_epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, FirehoseError> {
    let body = if body.is_empty() { b"{}" } else { body };
    serde_json::from_slice(body).map_err(|e| FirehoseError::Serialization {
        message: format!("request body is not valid JSON for this action: {e}"),
    })
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, FirehoseError> {
    serde_json::to_value(value)
        .map_err(|e| FirehoseError::internal(format!("failed to serialize response: {e}")))
}

fn is_valid_stream_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')
}

/// Validate a `DeliveryStreamName` against the API model: 1–64 characters
/// from `[a-zA-Z0-9_.-]`.
pub fn validate_stream_name(name: Option<&str>) -> Result<&str, FirehoseError> {
    let name = name.ok_or_else(|| {
        FirehoseError::validation(
            "1 validation error detected: Value null at 'deliveryStreamName' failed to satisfy \
             constraint: Member must not be null",
        )
    })?;
    if name.is_empty() || name.len() > 64 {
        return Err(FirehoseError::validation(format!(
            "1 validation error detected: Value '{name}' at 'deliveryStreamName' failed to \
             satisfy constraint: Member must have length between 1 and 64"
        )));
    }
    if !name.chars().all(is_valid_stream_name_char) {
        return Err(FirehoseError::validation(format!(
            "1 validation error detected: Value '{name}' at 'deliveryStreamName' failed to \
             satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )));
    }
    Ok(name)
}

fn validate_bucket_arn(arn: &str) -> Result<(), FirehoseError> {
    let bucket = arn.strip_prefix("arn:").and_then(|rest| {
        // arn:<partition>:s3:::<bucket>
        let mut parts = rest.splitn(5, ':');
        let _partition = parts.next()?;
        let service = parts.next()?;
        let region = parts.next()?;
        let account = parts.next()?;
        let bucket = parts.next()?;
        (service == "s3" && region.is_empty() && account.is_empty()).then_some(bucket)
    });
    match bucket {
        Some(bucket) if !bucket.is_empty() && !bucket.contains('/') => Ok(()),
        _ => Err(FirehoseError::invalid_argument(format!(
            "BucketARN {arn:?} is not a valid S3 bucket ARN (expected arn:aws:s3:::<bucket>)"
        ))),
    }
}

fn validate_hints(
    hints: BufferingHints,
    conversion_enabled: bool,
) -> Result<BufferingHints, FirehoseError> {
    let hints = hints.resolved();
    let size = hints.size_in_m_bs.unwrap_or_default();
    let interval = hints.interval_in_seconds.unwrap_or_default();
    if !(BufferingHints::MIN_SIZE_MB..=BufferingHints::MAX_SIZE_MB).contains(&size) {
        return Err(FirehoseError::validation(format!(
            "BufferingHints.SizeInMBs {size} is out of range: must be between {} and {}",
            BufferingHints::MIN_SIZE_MB,
            BufferingHints::MAX_SIZE_MB
        )));
    }
    if interval > BufferingHints::MAX_INTERVAL_SECONDS {
        return Err(FirehoseError::validation(format!(
            "BufferingHints.IntervalInSeconds {interval} is out of range: must be between 0 and {}",
            BufferingHints::MAX_INTERVAL_SECONDS
        )));
    }
    if conversion_enabled && size < BufferingHints::MIN_SIZE_MB_WITH_CONVERSION {
        return Err(FirehoseError::invalid_argument(format!(
            "BufferingHints.SizeInMBs must be at least {} when DataFormatConversionConfiguration \
             is enabled (got {size})",
            BufferingHints::MIN_SIZE_MB_WITH_CONVERSION
        )));
    }
    Ok(hints)
}

fn validate_conversion(
    conversion: &DataFormatConversionConfiguration,
) -> Result<(), FirehoseError> {
    if !conversion.is_enabled() {
        return Ok(());
    }
    let schema = conversion.schema_configuration.as_ref().ok_or_else(|| {
        FirehoseError::invalid_argument(
            "DataFormatConversionConfiguration.SchemaConfiguration is required when conversion \
             is enabled",
        )
    })?;
    for (field, value) in [
        ("DatabaseName", &schema.database_name),
        ("TableName", &schema.table_name),
    ] {
        if value.as_deref().is_none_or(str::is_empty) {
            return Err(FirehoseError::invalid_argument(format!(
                "DataFormatConversionConfiguration.SchemaConfiguration.{field} is required when \
                 conversion is enabled"
            )));
        }
    }
    if let Some(version) = schema.version_id.as_deref()
        && version != "LATEST"
    {
        return Err(FirehoseError::invalid_argument(format!(
            "DataFormatConversionConfiguration.SchemaConfiguration.VersionId {version:?} is not \
             supported by glaux: only LATEST can be resolved from the Glue catalog"
        )));
    }
    let deserializer = conversion
        .input_format_configuration
        .as_ref()
        .and_then(|c| c.deserializer.as_ref())
        .and_then(Value::as_object);
    let Some(map) = deserializer else {
        return Err(FirehoseError::invalid_argument(
            "DataFormatConversionConfiguration.InputFormatConfiguration.Deserializer is \
             required when conversion is enabled",
        ));
    };
    // Exactly one SerDe, with an options object: a map naming both would
    // otherwise skip the HiveJsonSerDe checks below.
    let (name, options) =
        convert::select_deserializer(map).map_err(FirehoseError::invalid_argument)?;
    if name == convert::HIVE_JSON_SERDE
        && options
            .get("TimestampFormats")
            .and_then(Value::as_array)
            .is_some_and(|f| !f.is_empty())
    {
        return Err(FirehoseError::invalid_argument(
            "InputFormatConfiguration.Deserializer.HiveJsonSerDe.TimestampFormats is not \
             implemented by glaux: timestamps must be epoch numbers or ISO-8601 strings",
        ));
    }
    let serializer = conversion
        .output_format_configuration
        .as_ref()
        .and_then(|c| c.serializer.as_ref())
        .and_then(Value::as_object);
    match serializer {
        Some(map) if map.contains_key("ParquetSerDe") => Ok(()),
        Some(map) => {
            let names: Vec<_> = map.keys().cloned().collect();
            Err(FirehoseError::invalid_argument(format!(
                "unsupported OutputFormatConfiguration.Serializer {names:?}: glaux supports \
                 ParquetSerDe only (ORC output is not implemented)"
            )))
        }
        None => Err(FirehoseError::invalid_argument(
            "DataFormatConversionConfiguration.OutputFormatConfiguration.Serializer is required \
             when conversion is enabled",
        )),
    }
}

/// Map `DeliveryStreamEncryptionConfigurationInput` onto what the stream
/// will report. `AWS_OWNED_CMK` is honoured (recorded and echoed as
/// `ENABLED`); `CUSTOMER_MANAGED_CMK` — which would require a real KMS key
/// — is rejected by name rather than silently downgraded.
fn resolve_stream_encryption(
    input: Option<&DeliveryStreamEncryptionConfigurationInput>,
) -> Result<DeliveryStreamEncryptionConfiguration, FirehoseError> {
    let Some(input) = input else {
        return Ok(DeliveryStreamEncryptionConfiguration::disabled());
    };
    match input.key_type.as_deref() {
        Some("AWS_OWNED_CMK") => Ok(DeliveryStreamEncryptionConfiguration::aws_owned_cmk()),
        Some("CUSTOMER_MANAGED_CMK") => Err(FirehoseError::invalid_argument(
            "DeliveryStreamEncryptionConfigurationInput.KeyType CUSTOMER_MANAGED_CMK is not \
             supported by glaux: there is no KMS to hold the key; use AWS_OWNED_CMK or omit \
             encryption",
        )),
        Some(other) => Err(FirehoseError::invalid_argument(format!(
            "DeliveryStreamEncryptionConfigurationInput.KeyType {other:?} is not valid: expected \
             AWS_OWNED_CMK or CUSTOMER_MANAGED_CMK"
        ))),
        None => Err(FirehoseError::invalid_argument(
            "DeliveryStreamEncryptionConfigurationInput.KeyType is required",
        )),
    }
}

/// Resolve a configuration (create) or an update layered on an existing
/// description into a fully-validated description.
fn resolve_destination(
    config: &ExtendedS3DestinationConfiguration,
    current: Option<&ExtendedS3DestinationDescription>,
) -> Result<ExtendedS3DestinationDescription, FirehoseError> {
    let role_arn = config
        .role_arn
        .clone()
        .or_else(|| current.map(|c| c.role_arn.clone()))
        .ok_or_else(|| {
            FirehoseError::validation("ExtendedS3DestinationConfiguration.RoleARN is required")
        })?;
    let bucket_arn = config
        .bucket_arn
        .clone()
        .or_else(|| current.map(|c| c.bucket_arn.clone()))
        .ok_or_else(|| {
            FirehoseError::validation("ExtendedS3DestinationConfiguration.BucketARN is required")
        })?;
    validate_bucket_arn(&bucket_arn)?;

    let conversion = config
        .data_format_conversion_configuration
        .clone()
        .or_else(|| current.and_then(|c| c.data_format_conversion_configuration.clone()));
    if let Some(conversion) = &conversion {
        validate_conversion(conversion)?;
    }
    let conversion_enabled = conversion
        .as_ref()
        .is_some_and(DataFormatConversionConfiguration::is_enabled);

    // Hints merge field-wise: an update naming only IntervalInSeconds keeps
    // the current SizeInMBs.
    let requested = config.buffering_hints.unwrap_or_default();
    let existing = current.map(|c| c.buffering_hints).unwrap_or_default();
    let hints = BufferingHints {
        size_in_m_bs: requested.size_in_m_bs.or(existing.size_in_m_bs),
        interval_in_seconds: requested
            .interval_in_seconds
            .or(existing.interval_in_seconds),
    };
    let buffering_hints = validate_hints(hints, conversion_enabled)?;

    let compression_format = config
        .compression_format
        .or_else(|| current.map(|c| c.compression_format))
        .unwrap_or_default();
    if !matches!(
        compression_format,
        CompressionFormat::Uncompressed | CompressionFormat::Gzip
    ) {
        return Err(FirehoseError::invalid_argument(format!(
            "CompressionFormat {} is not implemented by glaux: use UNCOMPRESSED or GZIP",
            compression_format.as_str()
        )));
    }
    if conversion_enabled && compression_format != CompressionFormat::Uncompressed {
        return Err(FirehoseError::invalid_argument(format!(
            "CompressionFormat must be UNCOMPRESSED when DataFormatConversionConfiguration is \
             enabled (got {}); Parquet applies its own compression",
            compression_format.as_str()
        )));
    }

    let prefix = config
        .prefix
        .clone()
        .or_else(|| current.and_then(|c| c.prefix.clone()));
    if let Some(prefix) = &prefix {
        validate_prefix("Prefix", prefix, false)
            .map_err(|PrefixError(message)| FirehoseError::invalid_argument(message))?;
    }
    let error_output_prefix = config
        .error_output_prefix
        .clone()
        .or_else(|| current.and_then(|c| c.error_output_prefix.clone()));
    if let Some(prefix) = &error_output_prefix {
        validate_prefix("ErrorOutputPrefix", prefix, true)
            .map_err(|PrefixError(message)| FirehoseError::invalid_argument(message))?;
    }
    let file_extension = config
        .file_extension
        .clone()
        .or_else(|| current.and_then(|c| c.file_extension.clone()));
    if let Some(ext) = &file_extension
        && (!ext.starts_with('.') || ext.len() < 2 || ext.contains('/'))
    {
        return Err(FirehoseError::validation(format!(
            "FileExtension {ext:?} is invalid: it must start with '.' and contain no '/'"
        )));
    }

    let encryption_configuration = config
        .encryption_configuration
        .clone()
        .or_else(|| current.map(|c| c.encryption_configuration.clone()))
        .unwrap_or_default();
    if encryption_configuration.kms_encryption_config.is_some() {
        return Err(FirehoseError::invalid_argument(
            "EncryptionConfiguration.KMSEncryptionConfig is not supported by glaux: only \
             NoEncryptionConfig is implemented",
        ));
    }

    let processing_configuration = config
        .processing_configuration
        .clone()
        .or_else(|| current.and_then(|c| c.processing_configuration.clone()));
    if processing_configuration
        .as_ref()
        .is_some_and(|p| p.enabled.unwrap_or(false))
    {
        return Err(FirehoseError::invalid_argument(
            "ProcessingConfiguration.Enabled=true (Lambda data transformation) is not supported \
             by glaux v0.1",
        ));
    }

    let s3_backup_mode = config
        .s3_backup_mode
        .clone()
        .or_else(|| current.and_then(|c| c.s3_backup_mode.clone()));
    match s3_backup_mode.as_deref() {
        None | Some("Disabled") => {}
        Some(other) => {
            return Err(FirehoseError::invalid_argument(format!(
                "S3BackupMode {other:?} is not supported by glaux v0.1: only Disabled is \
                 implemented"
            )));
        }
    }
    if config.s3_backup_configuration.is_some() || config.s3_backup_update.is_some() {
        return Err(FirehoseError::invalid_argument(
            "S3BackupConfiguration/S3BackupUpdate is not supported by glaux v0.1: source-record \
             backup is not implemented, so the backup destination cannot be honoured",
        ));
    }

    let dynamic_partitioning_configuration = config
        .dynamic_partitioning_configuration
        .clone()
        .or_else(|| current.and_then(|c| c.dynamic_partitioning_configuration.clone()));
    if dynamic_partitioning_configuration
        .as_ref()
        .is_some_and(|d| d.enabled.unwrap_or(false))
    {
        return Err(FirehoseError::invalid_argument(
            "DynamicPartitioningConfiguration.Enabled=true is not supported by glaux v0.1",
        ));
    }

    let custom_time_zone = config
        .custom_time_zone
        .clone()
        .or_else(|| current.and_then(|c| c.custom_time_zone.clone()));
    if let Some(tz) = custom_time_zone.as_deref()
        && tz != "UTC"
    {
        return Err(FirehoseError::invalid_argument(format!(
            "CustomTimeZone {tz:?} is not supported by glaux v0.1: only UTC is implemented"
        )));
    }

    Ok(ExtendedS3DestinationDescription {
        role_arn,
        bucket_arn,
        prefix: config
            .prefix
            .clone()
            .or_else(|| current.and_then(|c| c.prefix.clone())),
        error_output_prefix: config
            .error_output_prefix
            .clone()
            .or_else(|| current.and_then(|c| c.error_output_prefix.clone())),
        buffering_hints,
        compression_format,
        encryption_configuration,
        cloud_watch_logging_options: config
            .cloud_watch_logging_options
            .clone()
            .or_else(|| current.and_then(|c| c.cloud_watch_logging_options.clone())),
        processing_configuration,
        s3_backup_mode: Some(s3_backup_mode.unwrap_or_else(|| "Disabled".to_string())),
        data_format_conversion_configuration: conversion,
        dynamic_partitioning_configuration,
        file_extension: config
            .file_extension
            .clone()
            .or_else(|| current.and_then(|c| c.file_extension.clone())),
        custom_time_zone,
    })
}

/// Names of destination configuration keys other than the supported
/// `ExtendedS3DestinationConfiguration`, so an unsupported destination is
/// rejected by name.
fn unsupported_destination_keys(map: &serde_json::Map<String, Value>) -> Vec<&str> {
    map.keys()
        .filter(|k| k.ends_with("DestinationConfiguration") || k.ends_with("DestinationUpdate"))
        .map(String::as_str)
        .collect()
}

fn decode_record(record: &Record) -> Result<Bytes, FirehoseError> {
    let data = record.data.as_deref().ok_or_else(|| {
        FirehoseError::validation(
            "1 validation error detected: Value null at 'record.data' failed to satisfy \
             constraint: Member must not be null",
        )
    })?;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map(Bytes::from)
        .map_err(|e| FirehoseError::Serialization {
            message: format!("Record.Data is not valid base64: {e}"),
        })
}

fn new_record_id() -> String {
    // Real record ids are long opaque strings; two UUIDs keep them unique
    // and visibly distinct from stream ids.
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

impl FirehoseService {
    /// Build a service delivering flushed buffers to `sink`.
    pub fn new(config: FirehoseServiceConfig, sink: Arc<dyn DeliverySink>) -> Self {
        Self {
            config,
            sink,
            streams: Mutex::new(BTreeMap::new()),
        }
    }

    /// The service configuration.
    pub fn config(&self) -> &FirehoseServiceConfig {
        &self.config
    }

    /// Every `X-Amz-Target` action this service answers.
    pub const SUPPORTED_ACTIONS: &'static [&'static str] = &[
        "CreateDeliveryStream",
        "DescribeDeliveryStream",
        "ListDeliveryStreams",
        "UpdateDestination",
        "DeleteDeliveryStream",
        "PutRecord",
        "PutRecordBatch",
    ];

    /// Dispatch one action. `action` is the part after `Firehose_20150804.`
    /// in `X-Amz-Target`; `body` is the raw JSON request body.
    pub async fn handle(&self, action: &str, body: &[u8]) -> Result<Value, FirehoseError> {
        match action {
            "CreateDeliveryStream" => to_value(self.create_delivery_stream(parse_body(body)?)?),
            "DescribeDeliveryStream" => to_value(self.describe_delivery_stream(parse_body(body)?)?),
            "ListDeliveryStreams" => to_value(self.list_delivery_streams(parse_body(body)?)?),
            "UpdateDestination" => {
                self.update_destination(parse_body(body)?)?;
                Ok(Value::Object(Default::default()))
            }
            "DeleteDeliveryStream" => {
                self.delete_delivery_stream(parse_body(body)?).await?;
                Ok(Value::Object(Default::default()))
            }
            "PutRecord" => to_value(self.put_record(parse_body(body)?).await?),
            "PutRecordBatch" => to_value(self.put_record_batch(parse_body(body)?).await?),
            other => Err(FirehoseError::UnknownOperation {
                action: other.to_string(),
            }),
        }
    }

    fn stream_arn(&self, name: &str) -> String {
        format!(
            "arn:aws:firehose:{}:{}:deliverystream/{name}",
            self.config.region, self.config.account_id
        )
    }

    // -----------------------------------------------------------------------
    // Control plane
    // -----------------------------------------------------------------------

    /// `CreateDeliveryStream`. The stream is `ACTIVE` as soon as the call
    /// returns (real Firehose spends a while in `CREATING`; clients that
    /// poll for `ACTIVE` see it immediately).
    pub fn create_delivery_stream(
        &self,
        input: CreateDeliveryStreamInput,
    ) -> Result<CreateDeliveryStreamOutput, FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?.to_string();

        match input.delivery_stream_type.as_deref() {
            None | Some(STREAM_TYPE_DIRECT_PUT) => {}
            Some(other) => {
                return Err(FirehoseError::invalid_argument(format!(
                    "DeliveryStreamType {other:?} is not supported by glaux v0.1: only DirectPut \
                     sources are implemented"
                )));
            }
        }
        if input.kinesis_stream_source_configuration.is_some() {
            return Err(FirehoseError::invalid_argument(
                "KinesisStreamSourceConfiguration is not supported by glaux v0.1: only DirectPut \
                 sources are implemented",
            ));
        }
        if input.msk_source_configuration.is_some() {
            return Err(FirehoseError::invalid_argument(
                "MSKSourceConfiguration is not supported by glaux v0.1: only DirectPut sources \
                 are implemented",
            ));
        }
        if input.s3_destination_configuration.is_some() {
            return Err(FirehoseError::invalid_argument(
                "S3DestinationConfiguration is deprecated and not supported by glaux: use \
                 ExtendedS3DestinationConfiguration",
            ));
        }
        let unsupported = unsupported_destination_keys(&input.other);
        if !unsupported.is_empty() {
            return Err(FirehoseError::invalid_argument(format!(
                "unsupported destination {unsupported:?}: glaux v0.1 implements \
                 ExtendedS3DestinationConfiguration only"
            )));
        }
        let config = input
            .extended_s3_destination_configuration
            .as_ref()
            .ok_or_else(|| {
                FirehoseError::invalid_argument(
                    "ExtendedS3DestinationConfiguration is required: glaux v0.1 implements the \
                     S3 destination only",
                )
            })?;
        let destination = Arc::new(resolve_destination(config, None)?);
        let encryption = resolve_stream_encryption(
            input
                .delivery_stream_encryption_configuration_input
                .as_ref(),
        )?;

        let mut streams = self.streams.lock().unwrap();
        if streams.contains_key(&name) {
            return Err(FirehoseError::ResourceInUse {
                message: format!(
                    "Firehose {name} under accountId {} already exists",
                    self.config.account_id
                ),
            });
        }
        if streams.len() >= self.config.max_streams {
            return Err(FirehoseError::LimitExceeded {
                message: format!(
                    "You have already consumed your firehose quota of {} hoses for this region",
                    self.config.max_streams
                ),
            });
        }

        let arn = self.stream_arn(&name);
        let now = now_epoch_seconds();
        let description = DeliveryStreamDescription {
            delivery_stream_name: name.clone(),
            delivery_stream_arn: arn.clone(),
            delivery_stream_status: DeliveryStreamStatus::Active,
            delivery_stream_type: STREAM_TYPE_DIRECT_PUT.to_string(),
            version_id: "1".to_string(),
            create_timestamp: now,
            last_update_timestamp: now,
            destinations: vec![DestinationDescription {
                destination_id: DESTINATION_ID.to_string(),
                extended_s3_destination_description: (*destination).clone(),
            }],
            has_more_destinations: false,
            delivery_stream_encryption_configuration: encryption,
        };
        let buffer = StreamBuffer::spawn(&name, &arn, destination, Arc::clone(&self.sink));
        streams.insert(
            name,
            StreamEntry {
                description,
                tags: input.tags.unwrap_or_default(),
                buffer,
            },
        );
        Ok(CreateDeliveryStreamOutput {
            delivery_stream_arn: arn,
        })
    }

    /// `DescribeDeliveryStream`.
    pub fn describe_delivery_stream(
        &self,
        input: DescribeDeliveryStreamInput,
    ) -> Result<DescribeDeliveryStreamOutput, FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?;
        let streams = self.streams.lock().unwrap();
        let entry = streams
            .get(name)
            .ok_or_else(|| FirehoseError::stream_not_found(name, &self.config.account_id))?;
        Ok(DescribeDeliveryStreamOutput {
            delivery_stream_description: entry.description.clone(),
        })
    }

    /// Tags recorded at creation (there is no `ListTagsForDeliveryStream`
    /// action yet; embedders can read them here).
    pub fn tags(&self, name: &str) -> Option<Vec<Tag>> {
        self.streams
            .lock()
            .unwrap()
            .get(name)
            .map(|e| e.tags.clone())
    }

    /// `ListDeliveryStreams`: names in lexicographic order, paged by
    /// `ExclusiveStartDeliveryStreamName`.
    pub fn list_delivery_streams(
        &self,
        input: ListDeliveryStreamsInput,
    ) -> Result<ListDeliveryStreamsOutput, FirehoseError> {
        let limit = match input.limit {
            None => DEFAULT_LIST_LIMIT,
            Some(l) if l >= 1 && usize::try_from(l).unwrap_or(usize::MAX) <= MAX_LIST_LIMIT => {
                usize::try_from(l).unwrap_or(usize::MAX)
            }
            Some(l) => {
                return Err(FirehoseError::validation(format!(
                    "1 validation error detected: Value '{l}' at 'limit' failed to satisfy \
                     constraint: Member must have value between 1 and {MAX_LIST_LIMIT}"
                )));
            }
        };
        match input.delivery_stream_type.as_deref() {
            None | Some(STREAM_TYPE_DIRECT_PUT) => {}
            Some("KinesisStreamAsSource") | Some("MSKAsSource") => {
                // A valid filter that can never match anything here.
                return Ok(ListDeliveryStreamsOutput {
                    delivery_stream_names: Vec::new(),
                    has_more_delivery_streams: false,
                });
            }
            Some(other) => {
                return Err(FirehoseError::validation(format!(
                    "1 validation error detected: Value '{other}' at 'deliveryStreamType' failed \
                     to satisfy constraint: Member must satisfy enum value set: [DirectPut, \
                     KinesisStreamAsSource, MSKAsSource]"
                )));
            }
        }
        let streams = self.streams.lock().unwrap();
        let start = input.exclusive_start_delivery_stream_name.as_deref();
        let mut names: Vec<String> = streams
            .keys()
            .filter(|k| start.is_none_or(|s| k.as_str() > s))
            .take(limit + 1)
            .cloned()
            .collect();
        let has_more = names.len() > limit;
        names.truncate(limit);
        Ok(ListDeliveryStreamsOutput {
            delivery_stream_names: names,
            has_more_delivery_streams: has_more,
        })
    }

    /// `UpdateDestination`: optimistic-concurrency update of the single S3
    /// destination. Unset fields keep their values; the new thresholds
    /// apply to the open buffer immediately.
    pub fn update_destination(&self, input: UpdateDestinationInput) -> Result<(), FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?;
        let version = input
            .current_delivery_stream_version_id
            .as_deref()
            .ok_or_else(|| {
                FirehoseError::validation("CurrentDeliveryStreamVersionId is required")
            })?;
        let destination_id = input
            .destination_id
            .as_deref()
            .ok_or_else(|| FirehoseError::validation("DestinationId is required"))?;
        if input.s3_destination_update.is_some() {
            return Err(FirehoseError::invalid_argument(
                "S3DestinationUpdate is deprecated and not supported by glaux: use \
                 ExtendedS3DestinationUpdate",
            ));
        }
        let unsupported = unsupported_destination_keys(&input.other);
        if !unsupported.is_empty() {
            return Err(FirehoseError::invalid_argument(format!(
                "unsupported destination update {unsupported:?}: glaux v0.1 implements \
                 ExtendedS3DestinationUpdate only"
            )));
        }
        let update = input
            .extended_s3_destination_update
            .as_ref()
            .ok_or_else(|| {
                FirehoseError::invalid_argument(
                    "ExtendedS3DestinationUpdate is required: glaux v0.1 implements the S3 \
                     destination only",
                )
            })?;

        let mut streams = self.streams.lock().unwrap();
        let entry = streams
            .get_mut(name)
            .ok_or_else(|| FirehoseError::stream_not_found(name, &self.config.account_id))?;
        if entry.description.delivery_stream_status != DeliveryStreamStatus::Active {
            return Err(FirehoseError::ResourceInUse {
                message: format!(
                    "Firehose {name} under account {} is in state {:?} and cannot be updated",
                    self.config.account_id, entry.description.delivery_stream_status
                ),
            });
        }
        if entry.description.version_id != version {
            return Err(FirehoseError::ConcurrentModification {
                message: format!(
                    "Version {version} does not match the current version \
                     {} of delivery stream {name}",
                    entry.description.version_id
                ),
            });
        }
        let current = &entry.description.destinations[0];
        if current.destination_id != destination_id {
            return Err(FirehoseError::InvalidArgument {
                message: format!(
                    "Destination id {destination_id} does not exist on delivery stream {name} \
                     (the only destination is {})",
                    current.destination_id
                ),
            });
        }
        let resolved = Arc::new(resolve_destination(
            update,
            Some(&current.extended_s3_destination_description),
        )?);
        entry.description.destinations[0].extended_s3_destination_description = (*resolved).clone();
        let next_version = entry
            .description
            .version_id
            .parse::<u64>()
            .unwrap_or_default()
            + 1;
        entry.description.version_id = next_version.to_string();
        entry.description.last_update_timestamp = now_epoch_seconds();
        entry
            .buffer
            .update_destination(resolved, entry.description.version_id.clone());
        Ok(())
    }

    /// `DeleteDeliveryStream`: flushes whatever is buffered (regardless of
    /// `AllowForceDelete` — glaux never drops accepted records), then
    /// removes the stream. A flush failure leaves the stream in `DELETING`
    /// with its records intact and surfaces as `ServiceUnavailableException`.
    pub async fn delete_delivery_stream(
        &self,
        input: DeleteDeliveryStreamInput,
    ) -> Result<(), FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?.to_string();
        let buffer = {
            let mut streams = self.streams.lock().unwrap();
            let entry = streams
                .get_mut(&name)
                .ok_or_else(|| FirehoseError::stream_not_found(&name, &self.config.account_id))?;
            entry.description.delivery_stream_status = DeliveryStreamStatus::Deleting;
            Arc::clone(&entry.buffer)
        };
        match buffer.close_and_flush(FlushReason::Delete).await {
            Ok(()) => {
                self.streams.lock().unwrap().remove(&name);
                Ok(())
            }
            Err(err) => {
                tracing::error!(stream = %name, error = %err, "flush on delete failed");
                Err(FirehoseError::ServiceUnavailable {
                    message: format!(
                        "could not flush {} buffered record(s) of delivery stream {name} before \
                         deletion: {err}",
                        buffer.buffered_records()
                    ),
                })
            }
        }
    }

    // -----------------------------------------------------------------------
    // Data plane
    // -----------------------------------------------------------------------

    fn active_buffer(&self, name: &str) -> Result<Arc<StreamBuffer>, FirehoseError> {
        let streams = self.streams.lock().unwrap();
        let entry = streams
            .get(name)
            .ok_or_else(|| FirehoseError::stream_not_found(name, &self.config.account_id))?;
        if entry.description.delivery_stream_status != DeliveryStreamStatus::Active {
            return Err(FirehoseError::ResourceInUse {
                message: format!(
                    "Firehose {name} under account {} is in state {:?} and cannot accept records",
                    self.config.account_id, entry.description.delivery_stream_status
                ),
            });
        }
        Ok(Arc::clone(&entry.buffer))
    }

    fn check_record_size(&self, bytes: usize, field: &str) -> Result<(), FirehoseError> {
        if bytes > self.config.max_record_bytes {
            return Err(FirehoseError::validation(format!(
                "1 validation error detected: Value at '{field}' failed to satisfy constraint: \
                 Member must have length less than or equal to {} (got {bytes} bytes)",
                self.config.max_record_bytes
            )));
        }
        Ok(())
    }

    fn sink_unavailable(err: SinkError) -> FirehoseError {
        FirehoseError::ServiceUnavailable {
            message: format!("record could not be delivered: {err}"),
        }
    }

    /// `PutRecord`.
    pub async fn put_record(
        &self,
        input: PutRecordInput,
    ) -> Result<PutRecordOutput, FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?;
        let record = input.record.as_ref().ok_or_else(|| {
            FirehoseError::validation(
                "1 validation error detected: Value null at 'record' failed to satisfy \
                 constraint: Member must not be null",
            )
        })?;
        let data = decode_record(record)?;
        self.check_record_size(data.len(), "record.data")?;
        let buffer = self.active_buffer(name)?;
        buffer.push(data).await.map_err(Self::sink_unavailable)?;
        Ok(PutRecordOutput {
            record_id: new_record_id(),
            encrypted: false,
        })
    }

    /// `PutRecordBatch`: batch-level limits reject the whole request;
    /// per-record problems (oversized record, delivery failure) are reported
    /// in `RequestResponses` with the request succeeding, as in AWS. Once
    /// delivery fails, that record and every later one are reported as
    /// `ServiceUnavailableException` so the client retries exactly those.
    pub async fn put_record_batch(
        &self,
        input: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, FirehoseError> {
        let name = validate_stream_name(input.delivery_stream_name.as_deref())?;
        let records = input.records.as_deref().unwrap_or_default();
        if records.is_empty() {
            return Err(FirehoseError::validation(
                "1 validation error detected: Value '[]' at 'records' failed to satisfy \
                 constraint: Member must have length greater than or equal to 1",
            ));
        }
        if records.len() > self.config.max_batch_records {
            return Err(FirehoseError::validation(format!(
                "1 validation error detected: Value at 'records' failed to satisfy constraint: \
                 Member must have length less than or equal to {} (got {})",
                self.config.max_batch_records,
                records.len()
            )));
        }
        let decoded = records
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        let total: usize = decoded.iter().map(Bytes::len).sum();
        if total > self.config.max_batch_bytes {
            return Err(FirehoseError::invalid_argument(format!(
                "Records size exceeds {} MB limit (got {total} bytes)",
                self.config.max_batch_bytes / (1024 * 1024)
            )));
        }
        let buffer = self.active_buffer(name)?;

        let mut responses = Vec::with_capacity(decoded.len());
        let mut failed = 0u64;
        let mut sink_failure: Option<String> = None;
        for data in decoded {
            let entry = if let Some(message) = &sink_failure {
                Err(("ServiceUnavailableException", message.clone()))
            } else if let Err(err) = self.check_record_size(data.len(), "records.data") {
                Err(("ValidationException", err.message()))
            } else {
                match buffer.push(data).await {
                    Ok(()) => Ok(new_record_id()),
                    Err(err) => {
                        let message = format!("record could not be delivered: {err}");
                        sink_failure = Some(message.clone());
                        Err(("ServiceUnavailableException", message))
                    }
                }
            };
            responses.push(match entry {
                Ok(id) => PutRecordBatchResponseEntry {
                    record_id: Some(id),
                    error_code: None,
                    error_message: None,
                },
                Err((code, message)) => {
                    failed += 1;
                    PutRecordBatchResponseEntry {
                        record_id: None,
                        error_code: Some(code.to_string()),
                        error_message: Some(message),
                    }
                }
            });
        }
        Ok(PutRecordBatchOutput {
            failed_put_count: failed,
            encrypted: false,
            request_responses: responses,
        })
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Number of records currently buffered for `name`, if it exists.
    pub fn buffered_records(&self, name: &str) -> Option<usize> {
        self.streams
            .lock()
            .unwrap()
            .get(name)
            .map(|e| e.buffer.buffered_records())
    }

    /// Stop every stream's timer and flush every pending buffer. After
    /// this, streams refuse new records. Returns one entry per stream whose
    /// flush failed (its records stay in memory; nothing is dropped
    /// silently).
    ///
    /// Every stream is closed first and the flushes then run concurrently:
    /// serialising them would make shutdown cost one sink round-trip per
    /// stream, and `max_streams` allows 5000 of them, so a shutdown grace
    /// period could expire before the last buffer was written.
    pub async fn shutdown(&self) -> Vec<(String, SinkError)> {
        let buffers: Vec<Arc<StreamBuffer>> = self
            .streams
            .lock()
            .unwrap()
            .values()
            .map(|e| Arc::clone(&e.buffer))
            .collect();
        // Close every timer up front so no buffer keeps scheduling work
        // while the others are still flushing.
        for buffer in &buffers {
            buffer.close();
        }
        let mut tasks = tokio::task::JoinSet::new();
        for buffer in buffers {
            tasks.spawn(async move {
                let result = buffer.close_and_flush(FlushReason::Shutdown).await;
                (buffer, result)
            });
        }
        let mut failures = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let (buffer, result) = match joined {
                Ok(joined) => joined,
                Err(join) => {
                    // A flush task can only end abnormally if the runtime
                    // itself is going away; nothing is silently dropped.
                    tracing::error!(error = %join, "shutdown flush task ended abnormally");
                    continue;
                }
            };
            if let Err(err) = result {
                tracing::error!(stream = buffer.stream_name(), error = %err, "shutdown flush failed");
                failures.push((buffer.stream_name().to_string(), err));
            }
        }
        failures.sort_by(|a, b| a.0.cmp(&b.0));
        failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::RecordingSink;
    use serde_json::json;

    fn service() -> (FirehoseService, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::new());
        let service = FirehoseService::new(
            FirehoseServiceConfig::default(),
            Arc::clone(&sink) as Arc<dyn DeliverySink>,
        );
        (service, sink)
    }

    fn conversion(size_mb: u64, compression: &str, serializer: &str) -> Value {
        json!({
            "DeliveryStreamName": "conv",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                "BucketARN": "arn:aws:s3:::lake",
                "Prefix": "events/!{timestamp:yyyy}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
                "BufferingHints": {"SizeInMBs": size_mb, "IntervalInSeconds": 60},
                "CompressionFormat": compression,
                "DataFormatConversionConfiguration": {
                    "Enabled": true,
                    "SchemaConfiguration": {"DatabaseName": "analytics", "TableName": "events"},
                    "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
                    "OutputFormatConfiguration": {"Serializer": {serializer: {}}}
                }
            }
        })
    }

    async fn create(service: &FirehoseService, body: Value) -> Result<Value, FirehoseError> {
        service
            .handle("CreateDeliveryStream", body.to_string().as_bytes())
            .await
    }

    #[tokio::test]
    async fn conversion_configuration_is_stored_verbatim_and_echoed() {
        let (service, _sink) = service();
        create(&service, conversion(64, "UNCOMPRESSED", "ParquetSerDe"))
            .await
            .unwrap();
        let described = service
            .handle(
                "DescribeDeliveryStream",
                br#"{"DeliveryStreamName":"conv"}"#,
            )
            .await
            .unwrap();
        let dest = &described["DeliveryStreamDescription"]["Destinations"][0]["ExtendedS3DestinationDescription"];
        assert_eq!(dest["Prefix"], "events/!{timestamp:yyyy}/");
        assert_eq!(
            dest["ErrorOutputPrefix"],
            "errors/!{firehose:error-output-type}/"
        );
        assert_eq!(
            dest["DataFormatConversionConfiguration"]["SchemaConfiguration"]["TableName"],
            "events"
        );
        assert_eq!(
            dest["DataFormatConversionConfiguration"]["OutputFormatConfiguration"]["Serializer"],
            json!({"ParquetSerDe": {}})
        );
        assert_eq!(dest["S3BackupMode"], "Disabled");
        assert_eq!(
            dest["EncryptionConfiguration"],
            json!({"NoEncryptionConfig": "NoEncryption"})
        );
    }

    #[tokio::test]
    async fn conversion_rules_are_enforced_by_name() {
        let (service, _sink) = service();
        let err = create(&service, conversion(5, "UNCOMPRESSED", "ParquetSerDe"))
            .await
            .unwrap_err();
        assert!(
            err.message().contains("SizeInMBs must be at least 64"),
            "{err}"
        );

        let err = create(&service, conversion(64, "GZIP", "ParquetSerDe"))
            .await
            .unwrap_err();
        assert!(
            err.message()
                .contains("CompressionFormat must be UNCOMPRESSED"),
            "{err}"
        );

        let err = create(&service, conversion(64, "UNCOMPRESSED", "OrcSerDe"))
            .await
            .unwrap_err();
        assert!(err.message().contains("OrcSerDe"), "{err}");
        assert!(err.message().contains("ParquetSerDe only"), "{err}");

        let mut missing_schema = conversion(64, "UNCOMPRESSED", "ParquetSerDe");
        missing_schema["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]
            ["SchemaConfiguration"] = json!({});
        let err = create(&service, missing_schema).await.unwrap_err();
        assert!(
            err.message().contains("SchemaConfiguration.DatabaseName"),
            "{err}"
        );

        // A disabled block is accepted without schema checks.
        let mut disabled = conversion(5, "GZIP", "OrcSerDe");
        disabled["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]["Enabled"] =
            json!(false);
        create(&service, disabled).await.unwrap();

        // Unknown compression formats are refused rather than defaulted.
        let mut bad = conversion(5, "LZ4", "ParquetSerDe");
        bad["DeliveryStreamName"] = json!("lz4");
        bad["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]["Enabled"] =
            json!(false);
        let err = create(&service, bad).await.unwrap_err();
        assert_eq!(err.code(), "SerializationException");
        assert!(err.message().contains("LZ4"), "{err}");
    }

    #[tokio::test]
    async fn ambiguous_or_malformed_deserializers_are_refused() {
        let (service, _sink) = service();

        // Both SerDes named: accepting one by precedence would skip the
        // other's validation, so the whole configuration is refused.
        let mut both = conversion(64, "UNCOMPRESSED", "ParquetSerDe");
        both["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]["InputFormatConfiguration"]
            ["Deserializer"] =
            json!({"OpenXJsonSerDe": {}, "HiveJsonSerDe": {"TimestampFormats": ["yyyy"]}});
        let err = create(&service, both).await.unwrap_err();
        assert_eq!(err.code(), "InvalidArgumentException");
        assert!(err.message().contains("exactly one"), "{err}");

        // A member that is not an options object is refused rather than
        // treated as an empty one.
        let mut scalar = conversion(64, "UNCOMPRESSED", "ParquetSerDe");
        scalar["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]["InputFormatConfiguration"]
            ["Deserializer"] = json!({"OpenXJsonSerDe": "yes"});
        let err = create(&service, scalar).await.unwrap_err();
        assert!(err.message().contains("must be an object"), "{err}");

        // An explicitly null member is what some SDKs send for "unset".
        let mut null_member = conversion(64, "UNCOMPRESSED", "ParquetSerDe");
        null_member["ExtendedS3DestinationConfiguration"]["DataFormatConversionConfiguration"]["InputFormatConfiguration"]
            ["Deserializer"] = json!({"HiveJsonSerDe": {}, "OpenXJsonSerDe": null});
        create(&service, null_member).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn update_destination_changes_the_live_buffer_thresholds() {
        let (service, sink) = service();
        create(
            &service,
            json!({
                "DeliveryStreamName": "live",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                    "BucketARN": "arn:aws:s3:::lake",
                    "BufferingHints": {"SizeInMBs": 128, "IntervalInSeconds": 900}
                }
            }),
        )
        .await
        .unwrap();
        let data = base64::engine::general_purpose::STANDARD.encode(b"{}");
        service
            .handle(
                "PutRecord",
                json!({"DeliveryStreamName": "live", "Record": {"Data": data}})
                    .to_string()
                    .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::advance(std::time::Duration::from_secs(100)).await;

        service
            .handle(
                "UpdateDestination",
                json!({
                    "DeliveryStreamName": "live",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": "destinationId-000000000001",
                    "ExtendedS3DestinationUpdate": {
                        "BufferingHints": {"IntervalInSeconds": 120}
                    }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .unwrap();

        // Opened 100 s ago with a 120 s interval: flushes 20 s from now.
        tokio::time::advance(std::time::Duration::from_secs(19)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(sink.is_empty());
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(sink.len(), 1);
        assert_eq!(
            sink.batches()[0].destination.buffering_hints.size_in_m_bs,
            Some(128),
            "unset field kept across the update"
        );
    }

    fn plain(name: &str) -> Value {
        json!({
            "DeliveryStreamName": name,
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                "BucketARN": "arn:aws:s3:::lake"
            }
        })
    }

    async fn describe(service: &FirehoseService, name: &str) -> Value {
        service
            .handle(
                "DescribeDeliveryStream",
                json!({"DeliveryStreamName": name}).to_string().as_bytes(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sse_with_aws_owned_cmk_is_honoured_and_echoed() {
        let (service, _sink) = service();
        let mut body = plain("sse");
        body["DeliveryStreamEncryptionConfigurationInput"] = json!({"KeyType": "AWS_OWNED_CMK"});
        create(&service, body).await.unwrap();
        let described = describe(&service, "sse").await;
        assert_eq!(
            described["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"],
            json!({"Status": "ENABLED", "KeyType": "AWS_OWNED_CMK"})
        );

        create(&service, plain("nosse")).await.unwrap();
        let described = describe(&service, "nosse").await;
        assert_eq!(
            described["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"],
            json!({"Status": "DISABLED"})
        );
    }

    #[tokio::test]
    async fn sse_with_customer_managed_cmk_is_rejected_by_name() {
        let (service, _sink) = service();
        let mut body = plain("cmk");
        body["DeliveryStreamEncryptionConfigurationInput"] = json!({
            "KeyType": "CUSTOMER_MANAGED_CMK",
            "KeyARN": "arn:aws:kms:us-east-1:000000000000:key/abc"
        });
        let err = create(&service, body).await.unwrap_err();
        assert_eq!(err.code(), "InvalidArgumentException");
        assert!(err.message().contains("CUSTOMER_MANAGED_CMK"), "{err}");
        assert!(
            service.handle("ListDeliveryStreams", b"{}").await.unwrap()["DeliveryStreamNames"]
                .as_array()
                .unwrap()
                .is_empty(),
            "rejected stream must not be created"
        );
    }

    #[tokio::test]
    async fn s3_backup_configuration_is_rejected_by_name() {
        let (service, _sink) = service();
        let mut body = plain("bk");
        body["ExtendedS3DestinationConfiguration"]["S3BackupConfiguration"] = json!({
            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
            "BucketARN": "arn:aws:s3:::backup"
        });
        let err = create(&service, body).await.unwrap_err();
        assert_eq!(err.code(), "InvalidArgumentException");
        assert!(err.message().contains("S3BackupConfiguration"), "{err}");
    }

    #[tokio::test]
    async fn shutdown_flushes_every_stream() {
        let (service, sink) = service();
        let data = base64::engine::general_purpose::STANDARD.encode(b"payload");
        for i in 0..8 {
            let name = format!("s{i}");
            create(&service, plain(&name)).await.unwrap();
            service
                .handle(
                    "PutRecord",
                    json!({"DeliveryStreamName": name, "Record": {"Data": data}})
                        .to_string()
                        .as_bytes(),
                )
                .await
                .unwrap();
        }
        assert!(service.shutdown().await.is_empty());
        let mut flushed: Vec<String> = sink
            .batches()
            .iter()
            .map(|b| b.stream_name.clone())
            .collect();
        flushed.sort();
        assert_eq!(
            flushed,
            (0..8).map(|i| format!("s{i}")).collect::<Vec<_>>(),
            "every stream must be flushed exactly once"
        );
    }

    #[tokio::test]
    async fn s3_backup_update_is_rejected_by_name() {
        let (service, _sink) = service();
        create(&service, plain("bk")).await.unwrap();
        let err = service
            .handle(
                "UpdateDestination",
                json!({
                    "DeliveryStreamName": "bk",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": "destinationId-000000000001",
                    "ExtendedS3DestinationUpdate": {
                        "S3BackupUpdate": {
                            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                            "BucketARN": "arn:aws:s3:::backup"
                        }
                    }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), "InvalidArgumentException");
        assert!(err.message().contains("S3BackupUpdate"), "{err}");
    }
}
