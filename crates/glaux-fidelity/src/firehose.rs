//! Firehose artifact checks: the S3 objects glaux's delivery engine writes,
//! snapshotted with their variable parts masked.
//!
//! Three scenarios run through the `FirehoseService` JSON surface and the
//! real `S3DeliverySink` into in-memory S3:
//!
//! - `firehose_raw_default_prefix` — no `Prefix`, uncompressed: records are
//!   concatenated verbatim under `YYYY/MM/DD/HH/`;
//! - `firehose_custom_prefix_gzip` — a `!{timestamp:...}` prefix with GZIP:
//!   the object gets `.gz` and gunzips back to the records;
//! - `firehose_parquet_conversion` — JSON → Parquet against the `orders`
//!   Glue table, with two malformed records routed to `ErrorOutputPrefix`
//!   in Firehose's error envelope.
//!
//! Every object key is checked against AWS's naming contract
//! (`<prefix><stream>-<version>-<yyyy-MM-dd-HH-mm-ss>-<uuid>[.ext]`, the
//! prefix resolved from the *same* flush instant as the name) and then
//! masked to its template, so a snapshot is stable across runs. Object
//! contents are decoded (gunzipped, or Parquet → Athena text form) and
//! recorded line by line; error envelopes have their clock fields masked.
//!
//! Recording these against a real Firehose stream needs an IAM role and a
//! 60 s+ buffering window; that is deliberately not automated here (see the
//! crate docs), so the snapshots carry `UNVERIFIED` provenance until it is.

use std::io::Read as _;
use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::read::GzDecoder;
use glaux_athena::encode_result_set;
use glaux_catalog::{GlueApi, StorageBackend};
use glaux_firehose::prefix::{ErrorOutputType, resolve_data_prefix, resolve_error_prefix};
use glaux_firehose::{DeliverySink, FirehoseService, FirehoseServiceConfig, S3DeliverySink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::corpus::Compare;
use crate::memory::{MemoryStorage, StaticGlue};
use crate::snapshot::{Column, Outcome, Provenance, Snapshot, diff};
use crate::suite::{CaseResult, today};
use crate::{DATABASE, HarnessError, Result, fixtures};

/// Bucket every scenario delivers into.
pub const BUCKET: &str = "glaux-fidelity-firehose";

/// One scenario's verdict.
#[derive(Debug, Clone)]
pub struct ArtifactReport {
    /// Scenario (snapshot) name.
    pub name: String,
    /// What glaux produced.
    pub actual: Outcome,
    /// Snapshot provenance, when one exists.
    pub provenance: Option<Provenance>,
    /// The verdict.
    pub result: CaseResult,
}

/// Scenario names, in run order.
pub const SCENARIOS: &[&str] = &[
    "firehose_raw_default_prefix",
    "firehose_custom_prefix_gzip",
    "firehose_parquet_conversion",
];

struct Scenario {
    name: &'static str,
    stream: &'static str,
    destination: Value,
    records: Vec<Bytes>,
}

fn scenario(name: &str) -> Result<Scenario> {
    let role = "arn:aws:iam::000000000000:role/firehose";
    let bucket_arn = format!("arn:aws:s3:::{BUCKET}");
    Ok(match name {
        "firehose_raw_default_prefix" => Scenario {
            name: "firehose_raw_default_prefix",
            stream: "clicks-raw",
            destination: json!({
                "RoleARN": role,
                "BucketARN": bucket_arn,
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
                "CompressionFormat": "UNCOMPRESSED",
            }),
            records: vec![
                Bytes::from_static(b"{\"id\":1,\"event\":\"click\"}\n"),
                Bytes::from_static(b"{\"id\":2,\"event\":\"view\"}\n"),
                Bytes::from_static(b"no trailing newline"),
            ],
        },
        "firehose_custom_prefix_gzip" => Scenario {
            name: "firehose_custom_prefix_gzip",
            stream: "clicks-gz",
            destination: json!({
                "RoleARN": role,
                "BucketARN": bucket_arn,
                "Prefix": "events/!{timestamp:yyyy/MM/dd}/hour=!{timestamp:HH}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
                "CompressionFormat": "GZIP",
            }),
            records: vec![
                Bytes::from_static(b"{\"id\":1}\n"),
                Bytes::from_static("{\"id\":2,\"note\":\"\u{dc}berweisung\"}\n".as_bytes()),
            ],
        },
        "firehose_parquet_conversion" => {
            let orders = fixtures::table("orders")?;
            let mut records: Vec<Bytes> = std::str::from_utf8(&orders.bytes)
                .map_err(|e| HarnessError::new(format!("orders NDJSON is not UTF-8: {e}")))?
                .lines()
                .map(|l| Bytes::from(l.to_string()))
                .collect();
            records.push(Bytes::from_static(
                b"{\"id\": \"not-a-number\", \"status\": \"bad\"}",
            ));
            records.push(Bytes::from_static(b"definitely not json"));
            Scenario {
                name: "firehose_parquet_conversion",
                stream: "orders-parquet",
                destination: json!({
                    "RoleARN": role,
                    "BucketARN": bucket_arn,
                    "Prefix": "tables/orders/!{timestamp:yyyy/MM/dd}/",
                    "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/!{timestamp:yyyy-MM-dd}/",
                    "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 900},
                    "DataFormatConversionConfiguration": {
                        "Enabled": true,
                        "SchemaConfiguration": {
                            "DatabaseName": DATABASE,
                            "TableName": "orders",
                            "Region": "us-east-1",
                            "RoleARN": role,
                        },
                        "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
                        "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {}}},
                    },
                }),
                records,
            }
        }
        other => {
            return Err(HarnessError::new(format!(
                "unknown Firehose scenario {other}"
            )));
        }
    })
}

/// Run one scenario and describe the resulting objects.
pub async fn run(name: &str) -> Result<Outcome> {
    let scenario = scenario(name)?;
    let storage = Arc::new(MemoryStorage::new());
    let glue = Arc::new(StaticGlue::new(
        DATABASE,
        fixtures::tables()?
            .iter()
            .map(|t| t.glue_table(DATABASE, "unused-bucket"))
            .collect(),
    ));
    let sink = S3DeliverySink::new(
        Arc::clone(&storage) as Arc<dyn StorageBackend>,
        glue as Arc<dyn GlueApi>,
    );
    let service = FirehoseService::new(
        FirehoseServiceConfig::default(),
        Arc::new(sink) as Arc<dyn DeliverySink>,
    );
    let call = |action: &'static str, body: Value| {
        let service = &service;
        async move {
            service
                .handle(action, body.to_string().as_bytes())
                .await
                .map_err(|e| HarnessError::new(format!("{action}: {e}")))
        }
    };
    call(
        "CreateDeliveryStream",
        json!({
            "DeliveryStreamName": scenario.stream,
            "ExtendedS3DestinationConfiguration": scenario.destination,
        }),
    )
    .await?;
    let b64 = base64::engine::general_purpose::STANDARD;
    let put = call(
        "PutRecordBatch",
        json!({
            "DeliveryStreamName": scenario.stream,
            "Records": scenario.records.iter().map(|r| json!({"Data": b64.encode(r)})).collect::<Vec<_>>(),
        }),
    )
    .await?;
    if put["FailedPutCount"] != 0 {
        return Err(HarnessError::new(format!(
            "{}: PutRecordBatch rejected records: {put}",
            scenario.name
        )));
    }
    let before = storage.keys(BUCKET).await;
    if !before.is_empty() {
        return Err(HarnessError::new(format!(
            "{}: objects written before any flush trigger: {before:?}",
            scenario.name
        )));
    }
    // Shutdown flushes every buffer through the sink.
    let failures = service.shutdown().await;
    if !failures.is_empty() {
        return Err(HarnessError::new(format!(
            "{}: shutdown flush failed: {failures:?}",
            scenario.name
        )));
    }
    let destination = &scenario.destination;
    let mut rows = Vec::new();
    for key in storage.keys(BUCKET).await {
        let body = storage
            .get_object(BUCKET, &key)
            .await
            .map_err(|e| HarnessError::new(e.to_string()))?;
        let masked = mask_key(&key, scenario.stream, destination)?;
        let (kind, lines) = decode(&key, &body)?;
        for (i, line) in lines.into_iter().enumerate() {
            rows.push(vec![
                Some(masked.clone()),
                Some(kind.to_string()),
                Some(i.to_string()),
                Some(line),
            ]);
        }
    }
    Ok(Outcome::Succeeded {
        columns: vec![
            Column {
                name: "object".into(),
                type_name: "varchar".into(),
            },
            Column {
                name: "kind".into(),
                type_name: "varchar".into(),
            },
            Column {
                name: "line".into(),
                type_name: "bigint".into(),
            },
            Column {
                name: "content".into(),
                type_name: "varchar".into(),
            },
        ],
        rows,
    })
}

/// Validate `key` against the Firehose naming contract for `stream` and
/// return its template form with the flush timestamp and UUID masked.
fn mask_key(key: &str, stream: &str, destination: &Value) -> Result<String> {
    let err = |what: String| HarnessError::new(format!("object key {key:?}: {what}"));
    let (dir, file) = key
        .rsplit_once('/')
        .map(|(d, f)| (format!("{d}/"), f))
        .unwrap_or((String::new(), key));
    // <stream>-<version>-<yyyy-MM-dd-HH-mm-ss>-<uuid>[.ext]
    let rest = file
        .strip_prefix(stream)
        .and_then(|r| r.strip_prefix('-'))
        .ok_or_else(|| err(format!("object name does not start with {stream}-")))?;
    let (version, rest) = rest
        .split_once('-')
        .ok_or_else(|| err("missing stream version".into()))?;
    if version.is_empty() || !version.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err(format!("stream version {version:?} is not numeric")));
    }
    if rest.len() < 19 {
        return Err(err("object name too short for a timestamp".into()));
    }
    let (stamp, rest) = rest.split_at(19);
    let at = NaiveDateTime::parse_from_str(stamp, "%Y-%m-%d-%H-%M-%S")
        .map_err(|e| err(format!("timestamp {stamp:?}: {e}")))?;
    let at: DateTime<Utc> = at.and_utc();
    let age = Utc::now().signed_duration_since(at);
    if age.num_seconds().abs() > 600 {
        return Err(err(format!("object timestamp {stamp} is not close to now")));
    }
    let rest = rest
        .strip_prefix('-')
        .ok_or_else(|| err("missing uuid".into()))?;
    let (uuid_text, suffix) = rest.split_at(rest.len().min(36));
    uuid::Uuid::parse_str(uuid_text).map_err(|e| err(format!("uuid {uuid_text:?}: {e}")))?;

    // The directory must be the configured template resolved at the same
    // instant as the object name — data prefix or error prefix.
    let data_template = destination["Prefix"].as_str();
    let error_template = destination["ErrorOutputPrefix"].as_str();
    let data_dir = resolve_data_prefix(data_template, at).map_err(|e| err(e.0))?;
    let template = if dir == data_dir {
        data_template
            .unwrap_or("!{timestamp:yyyy/MM/dd/HH}/")
            .to_string()
    } else {
        let error_dir =
            resolve_error_prefix(error_template, at, ErrorOutputType::FormatConversionFailed)
                .map_err(|e| err(e.0))?;
        if dir != error_dir {
            return Err(err(format!(
                "directory {dir:?} is neither the data prefix {data_dir:?} nor the error prefix {error_dir:?} for {stamp}"
            )));
        }
        if !suffix.is_empty() {
            return Err(err(format!(
                "error objects carry no suffix, got {suffix:?}"
            )));
        }
        error_template
            .map(|t| t.replace("!{firehose:error-output-type}", "format-conversion-failed"))
            .unwrap_or_else(|| "format-conversion-failed/!{timestamp:yyyy/MM/dd/HH}/".to_string())
    };
    Ok(format!(
        "{template}{stream}-{version}-<timestamp>-<uuid>{suffix}"
    ))
}

/// Decode one object into `(kind, lines)`.
fn decode(key: &str, body: &Bytes) -> Result<(&'static str, Vec<String>)> {
    let err = |what: String| HarnessError::new(format!("object {key}: {what}"));
    if key.ends_with(".gz") {
        let mut text = String::new();
        GzDecoder::new(body.as_ref())
            .read_to_string(&mut text)
            .map_err(|e| err(format!("gunzip: {e}")))?;
        return Ok(("gzip", split_lines(&text)));
    }
    if key.ends_with(".parquet") {
        let reader = ParquetRecordBatchReaderBuilder::try_new(body.clone())
            .map_err(|e| err(format!("parquet: {e}")))?;
        let schema = Arc::clone(reader.schema());
        let batches = reader
            .build()
            .map_err(|e| err(format!("parquet: {e}")))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| err(format!("parquet: {e}")))?;
        let encoded = encode_result_set(&schema, &batches).map_err(|e| err(e.to_string()))?;
        let mut lines = vec![
            encoded
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "{} {}",
                        c.name,
                        crate::glaux::type_text(
                            &c.type_name,
                            Some(c.precision.into()),
                            Some(c.scale.into())
                        )
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        ];
        for row in &encoded.rows[1..] {
            let cells: Vec<Value> = row
                .iter()
                .map(|c| c.clone().map(Value::String).unwrap_or(Value::Null))
                .collect();
            lines.push(Value::Array(cells).to_string());
        }
        return Ok(("parquet", lines));
    }
    let text = std::str::from_utf8(body).map_err(|e| err(format!("not UTF-8: {e}")))?;
    if key.contains("format-conversion-failed/") {
        let mut lines = Vec::new();
        for line in text.lines() {
            let mut envelope: Value =
                serde_json::from_str(line).map_err(|e| err(format!("error envelope: {e}")))?;
            for clock in ["arrivalTimestamp", "attemptEndingTimestamp"] {
                if !envelope[clock].is_u64() {
                    return Err(err(format!("error envelope lacks numeric {clock}: {line}")));
                }
                envelope[clock] = Value::String("<epoch-millis>".into());
            }
            lines.push(envelope.to_string());
        }
        return Ok(("error", lines));
    }
    Ok(("raw", split_lines(text)))
}

/// Lines including a marker for a missing trailing newline, so the exact
/// bytes are pinned.
fn split_lines(text: &str) -> Vec<String> {
    let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
    match lines.pop() {
        Some(last) if last.is_empty() => {}
        Some(last) => lines.push(format!("{last}<no newline at end>")),
        None => {}
    }
    lines
}

/// Write every scenario's artifact snapshot as `UNVERIFIED`, skipping
/// scenarios whose snapshot was recorded against real Firehose.
pub async fn self_record(snapshot_dir: &Path) -> Result<Vec<String>> {
    let mut written = Vec::new();
    for name in SCENARIOS {
        if let Some(existing) = Snapshot::read(snapshot_dir, name)?
            && existing.provenance.is_verified()
        {
            continue;
        }
        Snapshot {
            case: (*name).to_string(),
            provenance: Provenance::Unverified { recorded: today() },
            compare: Compare::Ordered,
            outcome: run(name).await?,
        }
        .write(snapshot_dir)?;
        written.push((*name).to_string());
    }
    Ok(written)
}

/// Run every scenario and diff it with its snapshot.
pub async fn replay(snapshot_dir: &Path) -> Result<Vec<ArtifactReport>> {
    let mut reports = Vec::new();
    for name in SCENARIOS {
        let actual = run(name).await?;
        let (provenance, result) = match Snapshot::read(snapshot_dir, name)? {
            None => (None, CaseResult::MissingSnapshot),
            Some(snap) => {
                let differences = diff(&snap.outcome, &actual, Compare::Ordered, None);
                let result = if differences.is_empty() {
                    CaseResult::Match
                } else {
                    CaseResult::Mismatch(differences)
                };
                (Some(snap.provenance), result)
            }
        };
        reports.push(ArtifactReport {
            name: (*name).to_string(),
            actual,
            provenance,
            result,
        });
    }
    Ok(reports)
}
