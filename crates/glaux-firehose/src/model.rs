//! Wire-level request and response shapes for the Firehose JSON 1.1 protocol
//! (`Firehose_20150804`).
//!
//! Field names follow the AWS API model exactly (PascalCase), so these types
//! serialize to what `aws-sdk-*` and `boto3` expect. Timestamps are
//! epoch-seconds numbers; record `Data` blobs are base64 strings.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Buffering & destination configuration
// ---------------------------------------------------------------------------

/// `BufferingHints`: the flush thresholds. Either may be omitted, in which
/// case the service applies the AWS defaults (5 MiB / 300 s).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BufferingHints {
    /// Buffer size threshold in MiB (1..=128).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_in_m_bs: Option<u64>,
    /// Buffer age threshold in seconds (0..=900).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_in_seconds: Option<u64>,
}

impl BufferingHints {
    /// AWS default for `SizeInMBs`.
    pub const DEFAULT_SIZE_MB: u64 = 5;
    /// AWS default for `IntervalInSeconds`.
    pub const DEFAULT_INTERVAL_SECONDS: u64 = 300;
    /// Smallest `SizeInMBs` Firehose accepts.
    pub const MIN_SIZE_MB: u64 = 1;
    /// Largest `SizeInMBs` Firehose accepts.
    pub const MAX_SIZE_MB: u64 = 128;
    /// Smallest `SizeInMBs` when record format conversion is enabled.
    pub const MIN_SIZE_MB_WITH_CONVERSION: u64 = 64;
    /// Largest `IntervalInSeconds` Firehose accepts.
    pub const MAX_INTERVAL_SECONDS: u64 = 900;

    /// Fill in AWS defaults for any omitted field.
    pub fn resolved(self) -> Self {
        Self {
            size_in_m_bs: Some(self.size_in_m_bs.unwrap_or(Self::DEFAULT_SIZE_MB)),
            interval_in_seconds: Some(
                self.interval_in_seconds
                    .unwrap_or(Self::DEFAULT_INTERVAL_SECONDS),
            ),
        }
    }

    /// The size threshold in bytes (MiB, as the service counts).
    pub fn size_bytes(self) -> u64 {
        self.size_in_m_bs.unwrap_or(Self::DEFAULT_SIZE_MB) * 1024 * 1024
    }

    /// The age threshold.
    pub fn interval(self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.interval_in_seconds
                .unwrap_or(Self::DEFAULT_INTERVAL_SECONDS),
        )
    }
}

/// `CompressionFormat` for S3 objects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionFormat {
    /// No compression (the default).
    #[default]
    #[serde(rename = "UNCOMPRESSED")]
    Uncompressed,
    /// GZIP.
    #[serde(rename = "GZIP")]
    Gzip,
    /// ZIP.
    #[serde(rename = "ZIP")]
    Zip,
    /// Snappy (framed).
    #[serde(rename = "Snappy")]
    Snappy,
    /// Hadoop-compatible Snappy.
    #[serde(rename = "HADOOP_SNAPPY")]
    HadoopSnappy,
}

impl CompressionFormat {
    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uncompressed => "UNCOMPRESSED",
            Self::Gzip => "GZIP",
            Self::Zip => "ZIP",
            Self::Snappy => "Snappy",
            Self::HadoopSnappy => "HADOOP_SNAPPY",
        }
    }
}

/// `EncryptionConfiguration`. glaux only supports `NoEncryptionConfig`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EncryptionConfiguration {
    /// Always `NoEncryption` when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_encryption_config: Option<String>,
    /// KMS encryption is not supported; its presence is an explicit error.
    #[serde(
        default,
        rename = "KMSEncryptionConfig",
        skip_serializing_if = "Option::is_none"
    )]
    pub kms_encryption_config: Option<Value>,
}

impl Default for EncryptionConfiguration {
    fn default() -> Self {
        Self {
            no_encryption_config: Some("NoEncryption".to_string()),
            kms_encryption_config: None,
        }
    }
}

/// `CloudWatchLoggingOptions`. Accepted and echoed; glaux logs through
/// `tracing` instead of CloudWatch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudWatchLoggingOptions {
    /// Whether logging is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Log group name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_group_name: Option<String>,
    /// Log stream name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_stream_name: Option<String>,
}

/// `ProcessingConfiguration` (Lambda transforms). Out of scope for v0.1:
/// an enabled configuration is rejected explicitly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ProcessingConfiguration {
    /// Whether processing is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Processor list (opaque to glaux).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processors: Option<Vec<Value>>,
}

/// `DynamicPartitioningConfiguration`. Out of scope for v0.1: an enabled
/// configuration is rejected explicitly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DynamicPartitioningConfiguration {
    /// Whether dynamic partitioning is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Retry options (opaque to glaux).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_options: Option<Value>,
}

/// `SchemaConfiguration`: the Glue table whose schema drives record format
/// conversion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SchemaConfiguration {
    /// IAM role (accepted, unused locally).
    #[serde(default, rename = "RoleARN", skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    /// Glue catalog id (defaults to the account).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_id: Option<String>,
    /// Glue database name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
    /// Glue table name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    /// Glue region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Table version id (`LATEST` by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// `InputFormatConfiguration`: how incoming records are deserialized.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InputFormatConfiguration {
    /// `{"OpenXJsonSerDe": {...}}` or `{"HiveJsonSerDe": {...}}`, stored
    /// verbatim for the conversion layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deserializer: Option<Value>,
}

/// `OutputFormatConfiguration`: how converted records are serialized.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputFormatConfiguration {
    /// `{"ParquetSerDe": {...}}` or `{"OrcSerDe": {...}}`, stored verbatim
    /// for the conversion layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serializer: Option<Value>,
}

/// `DataFormatConversionConfiguration`: JSON → Parquet/ORC conversion
/// driven by a Glue table schema. Accepted and stored here; the conversion
/// itself is the format-conversion story's job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DataFormatConversionConfiguration {
    /// Whether conversion is enabled (defaults to `true` when the block is
    /// present, as in AWS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Schema source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_configuration: Option<SchemaConfiguration>,
    /// Input deserializer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_format_configuration: Option<InputFormatConfiguration>,
    /// Output serializer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_format_configuration: Option<OutputFormatConfiguration>,
}

impl DataFormatConversionConfiguration {
    /// Whether conversion is in effect (`Enabled` defaults to `true`).
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
}

/// `ExtendedS3DestinationConfiguration` as sent in `CreateDeliveryStream`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ExtendedS3DestinationConfiguration {
    /// IAM role (accepted, unused locally).
    #[serde(default, rename = "RoleARN", skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    /// `arn:aws:s3:::bucket`.
    #[serde(default, rename = "BucketARN", skip_serializing_if = "Option::is_none")]
    pub bucket_arn: Option<String>,
    /// Object key prefix (may contain `!{...}` expressions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Prefix for records that failed processing or conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_output_prefix: Option<String>,
    /// Flush thresholds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffering_hints: Option<BufferingHints>,
    /// Object compression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_format: Option<CompressionFormat>,
    /// Encryption (only `NoEncryptionConfig` supported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    /// CloudWatch logging options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_watch_logging_options: Option<CloudWatchLoggingOptions>,
    /// Lambda processing (rejected when enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_configuration: Option<ProcessingConfiguration>,
    /// `Disabled` | `Enabled` (source-record backup; rejected when enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_backup_mode: Option<String>,
    /// Backup destination (only meaningful with backup enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_backup_configuration: Option<Value>,
    /// Record format conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_format_conversion_configuration: Option<DataFormatConversionConfiguration>,
    /// Dynamic partitioning (rejected when enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_partitioning_configuration: Option<DynamicPartitioningConfiguration>,
    /// File extension override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_extension: Option<String>,
    /// Custom time zone for prefix timestamps (`UTC` by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_time_zone: Option<String>,
}

/// `ExtendedS3DestinationUpdate` as sent in `UpdateDestination`: every
/// field optional, unset fields keep their current value.
pub type ExtendedS3DestinationUpdate = ExtendedS3DestinationConfiguration;

/// `ExtendedS3DestinationDescription`: the stored, fully-resolved
/// destination as returned by `DescribeDeliveryStream`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ExtendedS3DestinationDescription {
    /// IAM role.
    #[serde(rename = "RoleARN")]
    pub role_arn: String,
    /// `arn:aws:s3:::bucket`.
    #[serde(rename = "BucketARN")]
    pub bucket_arn: String,
    /// Object key prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Error output prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_output_prefix: Option<String>,
    /// Resolved flush thresholds (both fields always set).
    pub buffering_hints: BufferingHints,
    /// Object compression.
    pub compression_format: CompressionFormat,
    /// Encryption.
    pub encryption_configuration: EncryptionConfiguration,
    /// CloudWatch logging options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_watch_logging_options: Option<CloudWatchLoggingOptions>,
    /// Lambda processing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_configuration: Option<ProcessingConfiguration>,
    /// Backup mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_backup_mode: Option<String>,
    /// Record format conversion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_format_conversion_configuration: Option<DataFormatConversionConfiguration>,
    /// Dynamic partitioning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_partitioning_configuration: Option<DynamicPartitioningConfiguration>,
    /// File extension override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_extension: Option<String>,
    /// Custom time zone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_time_zone: Option<String>,
}

impl ExtendedS3DestinationDescription {
    /// The bucket name from `BucketARN` (`arn:aws:s3:::bucket` → `bucket`).
    pub fn bucket(&self) -> &str {
        self.bucket_arn
            .rsplit_once(":::")
            .map(|(_, b)| b)
            .unwrap_or(&self.bucket_arn)
    }

    /// Whether record format conversion is enabled on this destination.
    pub fn conversion_enabled(&self) -> bool {
        self.data_format_conversion_configuration
            .as_ref()
            .is_some_and(DataFormatConversionConfiguration::is_enabled)
    }
}

// ---------------------------------------------------------------------------
// Stream description
// ---------------------------------------------------------------------------

/// `DeliveryStreamStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeliveryStreamStatus {
    /// Being created.
    Creating,
    /// Creation failed.
    CreatingFailed,
    /// Being deleted (pending buffers are being flushed).
    Deleting,
    /// Deletion failed.
    DeletingFailed,
    /// Ready for `PutRecord`.
    Active,
}

/// One entry in `DeliveryStreamDescription.Destinations`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DestinationDescription {
    /// `destinationId-000000000001`.
    pub destination_id: String,
    /// The S3 destination.
    #[serde(rename = "ExtendedS3DestinationDescription")]
    pub extended_s3_destination_description: ExtendedS3DestinationDescription,
}

/// `DeliveryStreamEncryptionConfiguration` (always disabled locally).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeliveryStreamEncryptionConfiguration {
    /// `DISABLED`.
    pub status: String,
}

/// `DeliveryStreamDescription`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeliveryStreamDescription {
    /// Stream name.
    pub delivery_stream_name: String,
    /// `arn:aws:firehose:<region>:<account>:deliverystream/<name>`.
    #[serde(rename = "DeliveryStreamARN")]
    pub delivery_stream_arn: String,
    /// Lifecycle status.
    pub delivery_stream_status: DeliveryStreamStatus,
    /// `DirectPut`.
    pub delivery_stream_type: String,
    /// Monotonic version, bumped by `UpdateDestination`.
    pub version_id: String,
    /// Creation time (epoch seconds).
    pub create_timestamp: f64,
    /// Last update time (epoch seconds).
    pub last_update_timestamp: f64,
    /// Destinations (exactly one).
    pub destinations: Vec<DestinationDescription>,
    /// Always `false`.
    pub has_more_destinations: bool,
    /// Server-side encryption status.
    pub delivery_stream_encryption_configuration: DeliveryStreamEncryptionConfiguration,
}

// ---------------------------------------------------------------------------
// Action inputs / outputs
// ---------------------------------------------------------------------------

/// A `Tag`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    /// Key.
    pub key: String,
    /// Value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// `CreateDeliveryStream` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateDeliveryStreamInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// `DirectPut` (default) or `KinesisStreamAsSource` (rejected).
    #[serde(default)]
    pub delivery_stream_type: Option<String>,
    /// Kinesis source (rejected).
    #[serde(default)]
    pub kinesis_stream_source_configuration: Option<Value>,
    /// MSK source (rejected).
    #[serde(default, rename = "MSKSourceConfiguration")]
    pub msk_source_configuration: Option<Value>,
    /// The supported destination.
    #[serde(default, rename = "ExtendedS3DestinationConfiguration")]
    pub extended_s3_destination_configuration: Option<ExtendedS3DestinationConfiguration>,
    /// Legacy S3 destination (rejected: use the extended form).
    #[serde(default, rename = "S3DestinationConfiguration")]
    pub s3_destination_configuration: Option<Value>,
    /// Other destinations, all rejected by name.
    #[serde(flatten)]
    pub other: serde_json::Map<String, Value>,
    /// Tags.
    #[serde(default)]
    pub tags: Option<Vec<Tag>>,
    /// Server-side encryption input (accepted; only disabled state stored).
    #[serde(default)]
    pub delivery_stream_encryption_configuration_input: Option<Value>,
}

/// `CreateDeliveryStream` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateDeliveryStreamOutput {
    /// The stream ARN.
    #[serde(rename = "DeliveryStreamARN")]
    pub delivery_stream_arn: String,
}

/// `DescribeDeliveryStream` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeDeliveryStreamInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// Destination page size (accepted; there is only one destination).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Destination pagination cursor.
    #[serde(default)]
    pub exclusive_start_destination_id: Option<String>,
}

/// `DescribeDeliveryStream` output.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeDeliveryStreamOutput {
    /// The description.
    pub delivery_stream_description: DeliveryStreamDescription,
}

/// `ListDeliveryStreams` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListDeliveryStreamsInput {
    /// Page size (default 10).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Filter by type.
    #[serde(default)]
    pub delivery_stream_type: Option<String>,
    /// Pagination cursor (names sort lexicographically).
    #[serde(default)]
    pub exclusive_start_delivery_stream_name: Option<String>,
}

/// `ListDeliveryStreams` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListDeliveryStreamsOutput {
    /// Names in this page.
    pub delivery_stream_names: Vec<String>,
    /// Whether another page exists.
    pub has_more_delivery_streams: bool,
}

/// `DeleteDeliveryStream` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteDeliveryStreamInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// Accepted; glaux always flushes pending data before deleting.
    #[serde(default)]
    pub allow_force_delete: Option<bool>,
}

/// `UpdateDestination` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateDestinationInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// Must equal the stream's current `VersionId`.
    #[serde(default)]
    pub current_delivery_stream_version_id: Option<String>,
    /// Must name the stream's single destination.
    #[serde(default)]
    pub destination_id: Option<String>,
    /// The update.
    #[serde(default, rename = "ExtendedS3DestinationUpdate")]
    pub extended_s3_destination_update: Option<ExtendedS3DestinationUpdate>,
    /// Legacy S3 update (rejected).
    #[serde(default, rename = "S3DestinationUpdate")]
    pub s3_destination_update: Option<Value>,
    /// Other destination updates, rejected by name.
    #[serde(flatten)]
    pub other: serde_json::Map<String, Value>,
}

/// A `Record`: `Data` is base64 on the wire.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Record {
    /// Base64-encoded payload.
    #[serde(default)]
    pub data: Option<String>,
}

/// `PutRecord` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// The record.
    #[serde(default)]
    pub record: Option<Record>,
}

/// `PutRecord` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordOutput {
    /// Opaque record id.
    pub record_id: String,
    /// Always `false`.
    pub encrypted: bool,
}

/// `PutRecordBatch` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordBatchInput {
    /// Stream name.
    #[serde(default)]
    pub delivery_stream_name: Option<String>,
    /// The records.
    #[serde(default)]
    pub records: Option<Vec<Record>>,
}

/// One entry of `PutRecordBatch.RequestResponses`: either a `RecordId` or
/// an `ErrorCode` + `ErrorMessage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordBatchResponseEntry {
    /// Set when the record was accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    /// Set when the record was rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// Set when the record was rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// `PutRecordBatch` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordBatchOutput {
    /// Number of entries carrying an error.
    pub failed_put_count: u64,
    /// Always `false`.
    pub encrypted: bool,
    /// One entry per input record, in order.
    pub request_responses: Vec<PutRecordBatchResponseEntry>,
}
