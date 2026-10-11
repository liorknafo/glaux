//! Record format conversion: buffered JSON records → one Parquet object,
//! using the Glue table schema named by `SchemaConfiguration`.
//!
//! Every record is converted individually against the Arrow schema derived
//! from the Glue columns (via [`glaux_catalog::hive_type_to_arrow`]).
//! Records that are not valid JSON objects, or whose values cannot be
//! coerced to the column types, are reported as [`FailedRecord`]s carrying
//! Firehose's format-conversion-failure envelope; they never make it into
//! the Parquet output and are never silently dropped.
//!
//! The `OpenXJsonSerDe` options Firehose exposes are honoured:
//! `CaseInsensitive` (default `true`: JSON keys are lower-cased before
//! matching, as Glue column names are lower-case),
//! `ConvertDotsInJsonKeysToUnderscores`, and `ColumnToJsonKeyMappings`.
//! `HiveJsonSerDe` is accepted when it carries no `TimestampFormats`
//! (custom Joda patterns are not implemented, and the service rejects them
//! at configuration time).

use std::sync::Arc;
use std::time::SystemTime;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::json::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use bytes::Bytes;
use glaux_catalog::{GlueColumn, GlueTable, hive_type_to_arrow};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use serde_json::{Map, Value, json};

use crate::model::{DataFormatConversionConfiguration, SchemaConfiguration};

/// Conversion could not run at all (as opposed to individual records
/// failing): a schema glaux cannot map, a serializer option it does not
/// implement, a Parquet writer failure. The batch is not delivered and the
/// buffer retries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConversionError(pub String);

/// One record that failed conversion, with the diagnostics Firehose puts
/// in the error-output envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedRecord {
    /// The record exactly as the producer sent it.
    pub raw: Bytes,
    /// `DataFormatConversion.ParseError` (not a JSON object) or
    /// `DataFormatConversion.InvalidSchemaMapping` (a value could not be
    /// coerced to its column type).
    pub error_code: &'static str,
    /// The parser or coercion diagnostic.
    pub error_message: String,
}

impl FailedRecord {
    /// Firehose's format-conversion-failure envelope, as one line of the
    /// error object: `attemptsMade`, `arrivalTimestamp`, `lastErrorCode`,
    /// `lastErrorMessage`, `attemptEndingTimestamp`, `rawData` (base64) and
    /// `dataCatalogTable`. `arrival` is the time the buffer holding the
    /// record was opened — records are not individually timestamped in the
    /// buffer, so this is the earliest the record can have arrived.
    pub fn envelope(
        &self,
        arrival: SystemTime,
        attempt_end: SystemTime,
        schema: &SchemaConfiguration,
    ) -> Value {
        json!({
            "attemptsMade": 1,
            "arrivalTimestamp": epoch_millis(arrival),
            "lastErrorCode": self.error_code,
            "lastErrorMessage": self.error_message,
            "attemptEndingTimestamp": epoch_millis(attempt_end),
            "rawData": base64::engine::general_purpose::STANDARD.encode(&self.raw),
            "dataCatalogTable": {
                "catalogId": schema.catalog_id,
                "databaseName": schema.database_name,
                "tableName": schema.table_name,
                "region": schema.region,
                "versionId": schema.version_id.as_deref().unwrap_or("LATEST"),
                "roleArn": schema.role_arn,
            },
        })
    }
}

fn epoch_millis(at: SystemTime) -> u128 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}

/// The outcome of converting one batch.
#[derive(Debug)]
pub struct Converted {
    /// The Parquet file holding every successfully converted record, in
    /// input order; `None` when no record converted.
    pub parquet: Option<Bytes>,
    /// Number of records in `parquet`.
    pub converted_records: usize,
    /// Records that failed, in input order.
    pub failed: Vec<FailedRecord>,
}

/// The two `Deserializer` members glaux implements.
pub(crate) const OPENX_JSON_SERDE: &str = "OpenXJsonSerDe";
pub(crate) const HIVE_JSON_SERDE: &str = "HiveJsonSerDe";

/// Pick the single SerDe named by an `InputFormatConfiguration.Deserializer`.
///
/// Firehose's `Deserializer` names exactly one SerDe. A map naming both, or
/// naming a member whose value is not an options object, is ambiguous: rather
/// than resolving it by an arbitrary precedence rule — which would silently
/// skip the other member's validation — glaux rejects it. Explicit `null`
/// members are ignored, since some SDKs serialise the unset member that way.
pub(crate) fn select_deserializer(
    deserializer: &Map<String, Value>,
) -> Result<(&'static str, &Map<String, Value>), String> {
    let present: Vec<&str> = deserializer
        .iter()
        .filter(|(_, value)| !value.is_null())
        .map(|(name, _)| name.as_str())
        .collect();
    let supported: Vec<&str> = present
        .iter()
        .copied()
        .filter(|name| *name == OPENX_JSON_SERDE || *name == HIVE_JSON_SERDE)
        .collect();
    let name = match supported.as_slice() {
        [OPENX_JSON_SERDE] => OPENX_JSON_SERDE,
        [HIVE_JSON_SERDE] => HIVE_JSON_SERDE,
        [] => {
            return Err(format!(
                "unsupported InputFormatConfiguration.Deserializer {present:?}: glaux supports \
                 OpenXJsonSerDe and HiveJsonSerDe"
            ));
        }
        _ => {
            return Err(format!(
                "InputFormatConfiguration.Deserializer names {supported:?}: exactly one \
                 deserializer must be given"
            ));
        }
    };
    if present.len() > supported.len() {
        let extra: Vec<&str> = present
            .iter()
            .copied()
            .filter(|n| !supported.contains(n))
            .collect();
        return Err(format!(
            "unsupported InputFormatConfiguration.Deserializer members {extra:?}: glaux supports \
             OpenXJsonSerDe and HiveJsonSerDe"
        ));
    }
    deserializer[name]
        .as_object()
        .map(|options| (name, options))
        .ok_or_else(|| {
            format!(
                "InputFormatConfiguration.Deserializer.{name} must be an object, got {}",
                json_kind(&deserializer[name])
            )
        })
}

/// The JSON deserializer options in effect.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InputOptions {
    case_insensitive: bool,
    dots_to_underscores: bool,
    /// column name → JSON key.
    column_mappings: Vec<(String, String)>,
}

impl InputOptions {
    fn from_config(config: &DataFormatConversionConfiguration) -> Result<Self, ConversionError> {
        let deserializer = config
            .input_format_configuration
            .as_ref()
            .and_then(|c| c.deserializer.as_ref())
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ConversionError(
                    "InputFormatConfiguration.Deserializer is missing from the destination"
                        .to_string(),
                )
            })?;
        let (name, options) = select_deserializer(deserializer).map_err(ConversionError)?;
        if name == OPENX_JSON_SERDE {
            let case_insensitive = options
                .get("CaseInsensitive")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let dots_to_underscores = options
                .get("ConvertDotsInJsonKeysToUnderscores")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let column_mappings = options
                .get("ColumnToJsonKeyMappings")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .map(|(column, key)| {
                            key.as_str()
                                .map(|k| (column.clone(), k.to_string()))
                                .ok_or_else(|| {
                                    ConversionError(format!(
                                        "OpenXJsonSerDe.ColumnToJsonKeyMappings[{column:?}] must \
                                         be a string, got {key}"
                                    ))
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            return Ok(Self {
                case_insensitive,
                dots_to_underscores,
                column_mappings,
            });
        }
        let formats = options
            .get("TimestampFormats")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        if formats > 0 {
            return Err(ConversionError(
                "HiveJsonSerDe.TimestampFormats is not implemented by glaux: timestamps \
                 must be epoch numbers or ISO-8601 / `yyyy-MM-dd HH:mm:ss` strings"
                    .to_string(),
            ));
        }
        Ok(Self {
            case_insensitive: true,
            dots_to_underscores: false,
            column_mappings: Vec::new(),
        })
    }

    /// Point every `ColumnToJsonKeyMappings` target at the Arrow field it
    /// names.
    ///
    /// [`Self::normalize`] inserts the mapped value under the *column* name,
    /// and the JSON decoder matches that name against the Arrow field, whose
    /// spelling comes from the Glue column. Under `CaseInsensitive` the record
    /// keys have already been lower-cased, so a mapping whose column is spelt
    /// with a different case than the Glue column would insert a key no field
    /// matches — and, with strict mode off, the column would decode as null
    /// with no error. Resolve the spelling against the schema once, up front.
    fn resolve_columns(&mut self, schema: &SchemaRef) {
        if !self.case_insensitive {
            return;
        }
        for (column, _) in &mut self.column_mappings {
            if schema.column_with_name(column).is_some() {
                continue;
            }
            let mut matches = schema
                .fields()
                .iter()
                .filter(|f| f.name().eq_ignore_ascii_case(column));
            if let (Some(field), None) = (matches.next(), matches.next()) {
                *column = field.name().clone();
            }
        }
    }

    /// Apply key normalisation to one parsed record.
    fn normalize(&self, value: Value) -> Value {
        let value = if self.case_insensitive || self.dots_to_underscores {
            normalize_keys(value, self.case_insensitive, self.dots_to_underscores)
        } else {
            value
        };
        if self.column_mappings.is_empty() {
            return value;
        }
        let Value::Object(mut map) = value else {
            return value;
        };
        for (column, key) in &self.column_mappings {
            let key = if self.case_insensitive {
                key.to_lowercase()
            } else {
                key.clone()
            };
            if let Some(v) = map.remove(&key) {
                map.insert(column.clone(), v);
            }
        }
        Value::Object(map)
    }
}

fn normalize_keys(value: Value, lowercase: bool, dots: bool) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    let mut k = if lowercase { k.to_lowercase() } else { k };
                    if dots {
                        k = k.replace('.', "_");
                    }
                    (k, normalize_keys(v, lowercase, dots))
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|v| normalize_keys(v, lowercase, dots))
                .collect(),
        ),
        other => other,
    }
}

/// The Parquet writer options from `ParquetSerDe`.
#[derive(Debug, Clone)]
struct OutputOptions {
    properties: WriterProperties,
}

impl OutputOptions {
    fn from_config(config: &DataFormatConversionConfiguration) -> Result<Self, ConversionError> {
        let serializer = config
            .output_format_configuration
            .as_ref()
            .and_then(|c| c.serializer.as_ref())
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ConversionError(
                    "OutputFormatConfiguration.Serializer is missing from the destination"
                        .to_string(),
                )
            })?;
        let Some(parquet) = serializer.get("ParquetSerDe") else {
            let names: Vec<_> = serializer.keys().cloned().collect();
            return Err(ConversionError(format!(
                "unsupported OutputFormatConfiguration.Serializer {names:?}: glaux implements \
                 ParquetSerDe only (ORC output is not implemented)"
            )));
        };
        let compression = match parquet.get("Compression").and_then(Value::as_str) {
            None | Some("SNAPPY") => Compression::SNAPPY,
            Some("GZIP") => Compression::GZIP(Default::default()),
            Some("UNCOMPRESSED") => Compression::UNCOMPRESSED,
            Some(other) => {
                return Err(ConversionError(format!(
                    "unsupported ParquetSerDe.Compression {other:?}: expected SNAPPY, GZIP, or \
                     UNCOMPRESSED"
                )));
            }
        };
        let writer_version = match parquet.get("WriterVersion").and_then(Value::as_str) {
            None | Some("V1") => WriterVersion::PARQUET_1_0,
            Some("V2") => WriterVersion::PARQUET_2_0,
            Some(other) => {
                return Err(ConversionError(format!(
                    "unsupported ParquetSerDe.WriterVersion {other:?}: expected V1 or V2"
                )));
            }
        };
        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_writer_version(writer_version)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_created_by("glaux-firehose".to_string());
        if let Some(enabled) = parquet
            .get("EnableDictionaryCompression")
            .and_then(Value::as_bool)
        {
            builder = builder.set_dictionary_enabled(enabled);
        }
        if let Some(page) = parquet.get("PageSizeBytes").and_then(Value::as_u64) {
            builder = builder.set_data_page_size_limit(usize::try_from(page).unwrap_or(usize::MAX));
        }
        // BlockSizeBytes and MaxPaddingBytes are HDFS block-layout hints
        // with no equivalent in arrow-rs' writer; they do not affect the
        // data and are accepted as-is.
        Ok(Self {
            properties: builder.build(),
        })
    }
}

/// Build the Arrow schema for a Glue table's data columns (partition keys
/// are not part of the file, as in Firehose).
pub fn table_schema(table: &GlueTable) -> Result<SchemaRef, ConversionError> {
    let columns: &[GlueColumn] = table
        .storage_descriptor
        .as_ref()
        .map(|sd| sd.columns.as_slice())
        .unwrap_or_default();
    if columns.is_empty() {
        return Err(ConversionError(format!(
            "Glue table {}.{} has no columns in its StorageDescriptor; record format conversion \
             needs a schema",
            table.database_name.as_deref().unwrap_or("?"),
            table.name
        )));
    }
    let fields = columns
        .iter()
        .map(|column| {
            let type_string = column.column_type.as_deref().ok_or_else(|| {
                ConversionError(format!(
                    "Glue column {:?} of table {} has no Type",
                    column.name, table.name
                ))
            })?;
            let data_type = hive_type_to_arrow(&column.name, type_string)
                .map_err(|e| ConversionError(e.to_string()))?;
            reject_unwritable(&column.name, &data_type)?;
            Ok(Field::new(column.name.clone(), data_type, true))
        })
        .collect::<Result<Vec<_>, ConversionError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// arrow-json cannot decode every Arrow type; name the ones it cannot so
/// the failure is a configuration error rather than a per-record mystery.
fn reject_unwritable(column: &str, data_type: &DataType) -> Result<(), ConversionError> {
    match data_type {
        DataType::Struct(fields) => {
            for f in fields {
                reject_unwritable(&format!("{column}.{}", f.name()), f.data_type())?;
            }
            Ok(())
        }
        DataType::List(f) | DataType::LargeList(f) => {
            reject_unwritable(&format!("{column}[]"), f.data_type())
        }
        DataType::Map(f, _) => reject_unwritable(&format!("{column}<map>"), f.data_type()),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Err(ConversionError(format!(
                "column {column:?} has type {data_type}, which glaux cannot convert from JSON \
                 (binary columns are not supported by the JSON deserializer)"
            )))
        }
        _ => Ok(()),
    }
}

/// Convert a batch of JSON records to Parquet.
pub fn convert(
    records: &[Bytes],
    table: &GlueTable,
    config: &DataFormatConversionConfiguration,
) -> Result<Converted, ConversionError> {
    let schema = table_schema(table)?;
    let mut input = InputOptions::from_config(config)?;
    input.resolve_columns(&schema);
    let output = OutputOptions::from_config(config)?;

    let mut parsed: Vec<Result<Value, FailedRecord>> = records
        .iter()
        .map(|raw| {
            parse_record(raw, &input).map_err(|error_message| FailedRecord {
                raw: raw.clone(),
                error_code: "DataFormatConversion.ParseError",
                error_message,
            })
        })
        .collect();

    // Decode every parseable record through one decoder. The per-record pass
    // — which exists only to name the record a coercion failure belongs to —
    // runs as a fallback, so an all-good flush builds one decoder and
    // re-encodes each record once instead of twice.
    let batch = match decode_parsed(&schema, &parsed) {
        Ok(batch) => batch,
        Err(_) => {
            for (raw, entry) in records.iter().zip(parsed.iter_mut()) {
                let failure = match entry {
                    Ok(value) => decode_one(&schema, value).err(),
                    Err(_) => None,
                };
                if let Some(error_message) = failure {
                    *entry = Err(FailedRecord {
                        raw: raw.clone(),
                        error_code: "DataFormatConversion.InvalidSchemaMapping",
                        error_message,
                    });
                }
            }
            decode_parsed(&schema, &parsed).map_err(ConversionError)?
        }
    };

    let Some(batch) = batch else {
        return Ok(Converted {
            parquet: None,
            converted_records: 0,
            failed: parsed.into_iter().filter_map(Result::err).collect(),
        });
    };
    let converted_records = batch.num_rows();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, Arc::clone(&schema), Some(output.properties))
        .map_err(|e| ConversionError(format!("failed to open Parquet writer: {e}")))?;
    writer
        .write(&batch)
        .and_then(|()| writer.close())
        .map_err(|e| ConversionError(format!("failed to write Parquet: {e}")))?;
    Ok(Converted {
        parquet: Some(Bytes::from(out)),
        converted_records,
        failed: parsed.into_iter().filter_map(Result::err).collect(),
    })
}

/// Decode every record that parsed, through one decoder. `None` when none
/// did.
fn decode_parsed(
    schema: &SchemaRef,
    parsed: &[Result<Value, FailedRecord>],
) -> Result<Option<RecordBatch>, String> {
    let good: Vec<&Value> = parsed
        .iter()
        .filter_map(|entry| entry.as_ref().ok())
        .collect();
    if good.is_empty() {
        return Ok(None);
    }
    decode_batch(schema, &good).map(Some)
}

/// Parse one record as a single JSON object and apply key normalisation.
fn parse_record(raw: &Bytes, input: &InputOptions) -> Result<Value, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("record is not UTF-8: {e}"))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("record is empty".to_string());
    }
    let value: Value = serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"))?;
    match value {
        Value::Object(_) => Ok(input.normalize(value)),
        other => Err(format!(
            "record must be a single JSON object, got {}",
            json_kind(&other)
        )),
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Decode one record on its own so a coercion failure is attributed to it.
fn decode_one(schema: &SchemaRef, value: &Value) -> Result<(), String> {
    decode_batch(schema, &[value]).map(drop)
}

fn decode_batch(schema: &SchemaRef, values: &[&Value]) -> Result<RecordBatch, String> {
    // Feed the decoder JSON *text* rather than handing it `Value`s through
    // serde: the workspace enables `serde_json/arbitrary_precision` (the Glue
    // key rewriter needs it to keep high-precision decimals byte-for-byte),
    // which makes `Value`'s `Serialize` emit a private number marker that
    // arrow's serde bridge rejects. Text carries the original literal exactly
    // and is the form Firehose receives anyway.
    let mut json = Vec::new();
    for value in values {
        serde_json::to_writer(&mut json, value)
            .map_err(|e| format!("failed to re-encode record as JSON: {e}"))?;
        json.push(b'\n');
    }
    let mut decoder = ReaderBuilder::new(Arc::clone(schema))
        .with_strict_mode(false)
        .with_coerce_primitive(false)
        // One flush must yield every row handed in.
        .with_batch_size(values.len().max(1))
        .build_decoder()
        .map_err(|e| format!("failed to build JSON decoder: {e}"))?;
    let mut rest = json.as_slice();
    while !rest.is_empty() {
        let consumed = decoder.decode(rest).map_err(|e| format!("{e}"))?;
        if consumed == 0 {
            return Err("JSON decoder stalled before consuming every record".to_string());
        }
        rest = &rest[consumed..];
    }
    decoder
        .flush()
        .map_err(|e| format!("{e}"))?
        .ok_or_else(|| "JSON decoder produced no rows".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Int64Type, TimestampNanosecondType};
    use glaux_catalog::GlueStorageDescriptor;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn column(name: &str, ty: &str) -> GlueColumn {
        GlueColumn {
            name: name.to_string(),
            column_type: Some(ty.to_string()),
            comment: None,
        }
    }

    fn table(columns: Vec<GlueColumn>) -> GlueTable {
        GlueTable {
            name: "orders".to_string(),
            database_name: Some("sales".to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: Some(GlueStorageDescriptor {
                columns,
                ..Default::default()
            }),
            partition_keys: vec![column("dt", "string")],
            parameters: Default::default(),
        }
    }

    fn config(deserializer: Value, serializer: Value) -> DataFormatConversionConfiguration {
        serde_json::from_value(json!({
            "Enabled": true,
            "SchemaConfiguration": {"DatabaseName": "sales", "TableName": "orders"},
            "InputFormatConfiguration": {"Deserializer": deserializer},
            "OutputFormatConfiguration": {"Serializer": serializer},
        }))
        .unwrap()
    }

    fn openx() -> DataFormatConversionConfiguration {
        config(json!({"OpenXJsonSerDe": {}}), json!({"ParquetSerDe": {}}))
    }

    fn read_back(parquet: &Bytes) -> Vec<RecordBatch> {
        ParquetRecordBatchReaderBuilder::try_new(parquet.clone())
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn converts_good_records_and_reports_bad_ones() {
        let table = table(vec![
            column("id", "bigint"),
            column("name", "string"),
            column("amount", "double"),
            column("ok", "boolean"),
            column("at", "timestamp"),
            column("tags", "array<string>"),
            column("addr", "struct<city:string,zip:int>"),
        ]);
        let records = vec![
            Bytes::from_static(
                br#"{"ID": 1, "Name": "a", "amount": 1.5, "ok": true, "at": "2026-08-21T13:07:09Z", "tags": ["x","y"], "addr": {"City": "Tel Aviv", "zip": 123}}"#,
            ),
            Bytes::from_static(b"not json"),
            Bytes::from_static(br#"{"id": "not a number"}"#),
            Bytes::from_static(br#"[1,2,3]"#),
            Bytes::from_static(br#"{"id": 2, "extra": "ignored"}"#),
        ];
        let out = convert(&records, &table, &openx()).unwrap();
        assert_eq!(out.converted_records, 2);
        assert_eq!(out.failed.len(), 3);
        assert_eq!(out.failed[0].error_code, "DataFormatConversion.ParseError");
        assert!(out.failed[0].error_message.contains("invalid JSON"));
        assert_eq!(
            out.failed[1].error_code,
            "DataFormatConversion.InvalidSchemaMapping"
        );
        assert_eq!(out.failed[1].raw, records[2]);
        assert!(out.failed[2].error_message.contains("got an array"));

        let batches = read_back(out.parquet.as_ref().unwrap());
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(
            batch.schema().field(4).data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );
        let ids = batch.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.values(), &[1, 2]);
        let names = batch.column(1).as_string::<i32>();
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(1));
        let at = batch.column(4).as_primitive::<TimestampNanosecondType>();
        let expected = chrono::DateTime::parse_from_rfc3339("2026-08-21T13:07:09Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        assert_eq!(at.value(0), expected);
        let addr = batch.column(6).as_struct();
        assert_eq!(addr.column(0).as_string::<i32>().value(0), "Tel Aviv");
    }

    #[test]
    fn all_records_failing_yields_no_parquet() {
        let table = table(vec![column("id", "bigint")]);
        let out = convert(&[Bytes::from_static(b"{")], &table, &openx()).unwrap();
        assert!(out.parquet.is_none());
        assert_eq!(out.failed.len(), 1);
    }

    #[test]
    fn openx_options_are_honoured() {
        let table = table(vec![column("id", "bigint"), column("user_name", "string")]);
        let cfg = config(
            json!({"OpenXJsonSerDe": {
                "CaseInsensitive": false,
                "ConvertDotsInJsonKeysToUnderscores": true,
                "ColumnToJsonKeyMappings": {"id": "Id"},
            }}),
            json!({"ParquetSerDe": {"Compression": "GZIP", "WriterVersion": "V2"}}),
        );
        let out = convert(
            &[Bytes::from_static(br#"{"Id": 7, "user.name": "lior"}"#)],
            &table,
            &cfg,
        )
        .unwrap();
        assert_eq!(out.converted_records, 1);
        let batch = &read_back(out.parquet.as_ref().unwrap())[0];
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 7);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "lior");
    }

    #[test]
    fn column_mappings_resolve_against_the_schema_case_insensitively() {
        // `CaseInsensitive` lower-cases the record keys, so a mapping whose
        // column is spelt differently from the Glue column has to be resolved
        // against the schema; inserting it verbatim would leave the column
        // null with no error.
        let table = table(vec![column("id", "bigint")]);
        let cfg = config(
            json!({"OpenXJsonSerDe": {
                "CaseInsensitive": true,
                "ColumnToJsonKeyMappings": {"ID": "OrderId"},
            }}),
            json!({"ParquetSerDe": {}}),
        );
        let out = convert(&[Bytes::from_static(br#"{"OrderId": 7}"#)], &table, &cfg).unwrap();
        assert_eq!(out.converted_records, 1);
        let batch = &read_back(out.parquet.as_ref().unwrap())[0];
        let ids = batch.column(0).as_primitive::<Int64Type>();
        assert!(!ids.is_null(0), "mapped column decoded as null");
        assert_eq!(ids.value(0), 7);
    }

    #[test]
    fn ambiguous_or_malformed_deserializers_are_refused() {
        let orders = table(vec![column("id", "bigint")]);

        // Naming both SerDes would otherwise resolve by an arbitrary
        // precedence and skip the other one's validation.
        let err = convert(
            &[],
            &orders,
            &config(
                json!({"OpenXJsonSerDe": {}, "HiveJsonSerDe": {"TimestampFormats": ["yyyy"]}}),
                json!({"ParquetSerDe": {}}),
            ),
        )
        .unwrap_err();
        assert!(err.0.contains("exactly one"), "{err}");

        let err = convert(
            &[],
            &orders,
            &config(
                json!({"OpenXJsonSerDe": "yes"}),
                json!({"ParquetSerDe": {}}),
            ),
        )
        .unwrap_err();
        assert!(err.0.contains("must be an object"), "{err}");

        // An explicitly null member is what some SDKs send for "unset".
        let out = convert(
            &[Bytes::from_static(br#"{"id": 1}"#)],
            &orders,
            &config(
                json!({"OpenXJsonSerDe": {}, "HiveJsonSerDe": null}),
                json!({"ParquetSerDe": {}}),
            ),
        )
        .unwrap();
        assert_eq!(out.converted_records, 1);
    }

    #[test]
    fn configuration_problems_are_explicit() {
        let orders = table(vec![column("id", "bigint")]);
        let err = convert(
            &[],
            &orders,
            &config(json!({"OpenXJsonSerDe": {}}), json!({"OrcSerDe": {}})),
        )
        .unwrap_err();
        assert!(err.0.contains("OrcSerDe"), "{err}");

        let err = convert(
            &[],
            &orders,
            &config(
                json!({"HiveJsonSerDe": {"TimestampFormats": ["yyyy"]}}),
                json!({"ParquetSerDe": {}}),
            ),
        )
        .unwrap_err();
        assert!(err.0.contains("TimestampFormats"), "{err}");

        let err = convert(&[], &table(vec![column("blob", "binary")]), &openx()).unwrap_err();
        assert!(err.0.contains("\"blob\""), "{err}");

        let err = convert(&[], &table(vec![]), &openx()).unwrap_err();
        assert!(err.0.contains("no columns"), "{err}");

        let err = convert(&[], &table(vec![column("x", "mystery")]), &openx()).unwrap_err();
        assert!(err.0.contains("mystery"), "{err}");
    }

    #[test]
    fn envelope_has_the_firehose_shape() {
        let failed = FailedRecord {
            raw: Bytes::from_static(b"oops"),
            error_code: "DataFormatConversion.ParseError",
            error_message: "invalid JSON".to_string(),
        };
        let schema: SchemaConfiguration = serde_json::from_value(json!({
            "DatabaseName": "sales", "TableName": "orders", "Region": "us-east-1"
        }))
        .unwrap();
        let at = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);
        let env = failed.envelope(at, at, &schema);
        assert_eq!(env["attemptsMade"], 1);
        assert_eq!(env["arrivalTimestamp"], 1_700_000_000_123u64);
        assert_eq!(env["lastErrorCode"], "DataFormatConversion.ParseError");
        assert_eq!(env["rawData"], "b29wcw==");
        assert_eq!(env["dataCatalogTable"]["tableName"], "orders");
        assert_eq!(env["dataCatalogTable"]["versionId"], "LATEST");
    }
}
