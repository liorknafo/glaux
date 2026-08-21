//! End-to-end: the real `glaux-server` binary against a real fakecloud
//! process, driven by the real AWS SDKs exactly as the AWS CLI would with
//! `--endpoint-url`:
//!
//! 1. Glue: database + Parquet table (fixture, via fakecloud's Glue).
//! 2. Firehose: `CreateDeliveryStream` with JSON → Parquet conversion,
//!    `PutRecordBatch`, then a flush (delete-triggered) into S3.
//! 3. Athena: `StartQueryExecution` over the table, poll to `SUCCEEDED`,
//!    `GetQueryResults` returns the delivered rows.
//! 4. Graceful shutdown: records buffered in a second stream are flushed to
//!    S3 when the server receives `SIGTERM`, and the process exits 0.
//!
//! The fakecloud binary is taken from `PATH` when available, otherwise
//! downloaded once from GitHub releases into a cached temp location (the
//! LIO-19 pattern). When neither works, the test **skips with a clear
//! message** rather than failing — but it never fakes a result.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use aws_credential_types::Credentials;
use aws_sdk_athena::types::QueryExecutionState;
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    BufferingHints, CompressionFormat, DataFormatConversionConfiguration, Deserializer,
    ExtendedS3DestinationConfiguration, InputFormatConfiguration, OpenXJsonSerDe,
    OutputFormatConfiguration, ParquetSerDe, Record, SchemaConfiguration, Serializer,
};
use glaux_catalog::{
    AwsCredentials, ConfigOverrides, GlauxConfig, NetworkGlueApi, NetworkStorageBackend,
    StorageBackend,
};
use serde_json::json;

const FAKECLOUD_VERSION: &str = "v0.44.10";
const BUCKET: &str = "lake";
const REGION: &str = "us-east-1";

/// Kills a child process when the test ends (pass or fail).
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

fn fakecloud_on_path() -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join("fakecloud"))
        .find(|candidate| candidate.is_file())
}

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
            eprintln!(
                "failed to extract fakecloud archive into {}",
                cache_dir.display()
            );
            None
        }
    }
}

fn locate_fakecloud() -> Option<PathBuf> {
    fakecloud_on_path().or_else(download_fakecloud)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_http_ok(url: &str, timeout: Duration) -> bool {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

fn glaux_config(endpoint: &str) -> GlauxConfig {
    GlauxConfig::load(
        None,
        &ConfigOverrides {
            s3_endpoint: Some(endpoint.to_string()),
            glue_endpoint: Some(endpoint.to_string()),
            region: Some(REGION.to_string()),
            credentials: Some(AwsCredentials {
                access_key_id: "glaux-it".to_string(),
                secret_access_key: "glaux-it-secret".to_string(),
                session_token: None,
            }),
            ..Default::default()
        },
    )
    .unwrap()
}

/// Provision the bucket, the Glue database, and the Parquet table the
/// Firehose stream converts into and Athena reads from.
async fn provision_fixtures(endpoint: &str) {
    let http = reqwest::Client::new();
    let created = http
        .put(format!("{endpoint}/{BUCKET}"))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=glaux-it/20260101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=fixture",
        )
        .send()
        .await
        .expect("create bucket");
    assert!(created.status().is_success(), "CreateBucket: {created:?}");

    let glue = NetworkGlueApi::new(&glaux_config(endpoint));
    glue.invoke(
        "CreateDatabase",
        json!({ "DatabaseInput": { "Name": "analytics" } }),
    )
    .await
    .expect("CreateDatabase");
    for (table, prefix) in [("events", "events"), ("late_events", "late")] {
        glue.invoke(
            "CreateTable",
            json!({
                "DatabaseName": "analytics",
                "TableInput": {
                    "Name": table,
                    "TableType": "EXTERNAL_TABLE",
                    "StorageDescriptor": {
                        "Columns": [
                            { "Name": "id", "Type": "bigint" },
                            { "Name": "name", "Type": "string" },
                            { "Name": "amount", "Type": "double" },
                        ],
                        "Location": format!("s3://{BUCKET}/{prefix}/"),
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
    }
}

fn firehose_client(endpoint: &str) -> aws_sdk_firehose::Client {
    let config = aws_sdk_firehose::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_firehose::config::Region::new(REGION))
        .credentials_provider(Credentials::new(
            "glaux-it",
            "glaux-it-secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(endpoint)
        .build();
    aws_sdk_firehose::Client::from_conf(config)
}

fn athena_client(endpoint: &str) -> aws_sdk_athena::Client {
    let config = aws_sdk_athena::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_athena::config::Region::new(REGION))
        .credentials_provider(Credentials::new(
            "glaux-it",
            "glaux-it-secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(endpoint)
        .build();
    aws_sdk_athena::Client::from_conf(config)
}

fn parquet_destination(prefix: &str, table: &str) -> ExtendedS3DestinationConfiguration {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::000000000000:role/firehose")
        .bucket_arn(format!("arn:aws:s3:::{BUCKET}"))
        .prefix(format!("{prefix}/"))
        .error_output_prefix(format!("errors/{prefix}/"))
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(64)
                .interval_in_seconds(300)
                .build(),
        )
        .compression_format(CompressionFormat::Uncompressed)
        .data_format_conversion_configuration(
            DataFormatConversionConfiguration::builder()
                .enabled(true)
                .schema_configuration(
                    SchemaConfiguration::builder()
                        .database_name("analytics")
                        .table_name(table)
                        .build(),
                )
                .input_format_configuration(
                    InputFormatConfiguration::builder()
                        .deserializer(
                            Deserializer::builder()
                                .open_x_json_ser_de(OpenXJsonSerDe::builder().build())
                                .build(),
                        )
                        .build(),
                )
                .output_format_configuration(
                    OutputFormatConfiguration::builder()
                        .serializer(
                            Serializer::builder()
                                .parquet_ser_de(ParquetSerDe::builder().build())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

fn record(id: i64, name: &str, amount: f64) -> Record {
    let line = format!("{}\n", json!({ "id": id, "name": name, "amount": amount }));
    Record::builder().data(Blob::new(line)).build().unwrap()
}

async fn parquet_objects(storage: &NetworkStorageBackend, prefix: &str) -> Vec<String> {
    storage
        .list_objects(BUCKET, prefix)
        .await
        .expect("list objects")
        .into_iter()
        .map(|o| o.key)
        .filter(|k| k.ends_with(".parquet"))
        .collect()
}

#[tokio::test]
async fn firehose_put_flush_then_athena_query_round_trip() {
    let Some(fakecloud) = locate_fakecloud() else {
        eprintln!(
            "SKIPPED: glaux-server round-trip test — fakecloud is not on PATH and could not be \
             downloaded from GitHub releases (offline or unsupported platform {}/{}). Install \
             fakecloud or restore network access to run this test.",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        return;
    };

    // ---- fakecloud ----
    let fc_port = free_port();
    let fc_addr = format!("127.0.0.1:{fc_port}");
    let fc = Command::new(&fakecloud)
        .args(["--addr", &fc_addr, "--log-level", "warn"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fakecloud");
    let _fc_guard = ProcessGuard(fc);
    assert!(
        wait_for_http_ok(
            &format!("http://{fc_addr}/_fakecloud/health"),
            Duration::from_secs(30)
        )
        .await,
        "fakecloud did not become healthy on {fc_addr}"
    );
    let fc_endpoint = format!("http://{fc_addr}");
    provision_fixtures(&fc_endpoint).await;

    // ---- glaux-server ----
    let gs_port = free_port();
    let gs_addr = format!("127.0.0.1:{gs_port}");
    let server = Command::new(env!("CARGO_BIN_EXE_glaux-server"))
        .args([
            "--listen",
            &gs_addr,
            "--s3-endpoint",
            &fc_endpoint,
            "--glue-endpoint",
            &fc_endpoint,
            "--athena-output-location",
            &format!("s3://{BUCKET}/results/"),
            "--access-key-id",
            "glaux-it",
            "--secret-access-key",
            "glaux-it-secret",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn glaux-server");
    let mut server = ProcessGuard(server);
    let gs_endpoint = format!("http://{gs_addr}");
    assert!(
        wait_for_http_ok(&format!("{gs_endpoint}/health"), Duration::from_secs(60)).await,
        "glaux-server did not become healthy on {gs_addr}"
    );

    let storage = NetworkStorageBackend::new(&glaux_config(&fc_endpoint));
    let firehose = firehose_client(&gs_endpoint);
    let athena = athena_client(&gs_endpoint);

    // ---- Firehose: create, put a batch, flush via delete ----
    firehose
        .create_delivery_stream()
        .delivery_stream_name("events")
        .extended_s3_destination_configuration(parquet_destination("events", "events"))
        .send()
        .await
        .expect("CreateDeliveryStream");

    let put = firehose
        .put_record_batch()
        .delivery_stream_name("events")
        .set_records(Some(vec![
            record(1, "signup", 0.0),
            record(2, "purchase", 19.99),
            record(3, "refund", -5.5),
        ]))
        .send()
        .await
        .expect("PutRecordBatch");
    assert_eq!(put.failed_put_count, 0, "{put:?}");
    assert_eq!(put.request_responses.len(), 3);
    assert!(
        parquet_objects(&storage, "events/").await.is_empty(),
        "nothing may be written before the buffer flushes"
    );

    // Delete flushes the buffer (FlushReason::Delete) before the stream goes.
    firehose
        .delete_delivery_stream()
        .delivery_stream_name("events")
        .send()
        .await
        .expect("DeleteDeliveryStream");
    let objects = parquet_objects(&storage, "events/").await;
    assert_eq!(objects.len(), 1, "one Parquet object expected: {objects:?}");
    assert!(
        objects[0].starts_with("events/20"),
        "default YYYY/MM/DD/HH/ prefix expected: {}",
        objects[0]
    );
    assert!(
        storage
            .list_objects(BUCKET, "errors/")
            .await
            .unwrap()
            .is_empty(),
        "no records may land in the error prefix"
    );

    // ---- Athena: query what Firehose delivered ----
    let started = athena
        .start_query_execution()
        .query_string(
            "SELECT id, name, amount FROM analytics.events WHERE amount >= 0 ORDER BY id DESC",
        )
        .send()
        .await
        .expect("StartQueryExecution");
    let query_id = started.query_execution_id.unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let execution = loop {
        let execution = athena
            .get_query_execution()
            .query_execution_id(&query_id)
            .send()
            .await
            .expect("GetQueryExecution")
            .query_execution
            .unwrap();
        let state = execution.status().unwrap().state().unwrap().clone();
        match state {
            QueryExecutionState::Queued | QueryExecutionState::Running => {
                assert!(Instant::now() < deadline, "query did not finish in time");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => break execution,
        }
    };
    let status = execution.status().unwrap();
    assert_eq!(
        status.state(),
        Some(&QueryExecutionState::Succeeded),
        "query failed: {:?}",
        status.state_change_reason()
    );
    let output_location = execution
        .result_configuration()
        .and_then(|c| c.output_location())
        .unwrap();
    assert!(
        output_location.starts_with(&format!("s3://{BUCKET}/results/")),
        "{output_location}"
    );
    assert!(output_location.ends_with(".csv"), "{output_location}");

    let results = athena
        .get_query_results()
        .query_execution_id(&query_id)
        .send()
        .await
        .expect("GetQueryResults");
    let rows: Vec<Vec<String>> = results
        .result_set()
        .unwrap()
        .rows()
        .iter()
        .map(|row| {
            row.data()
                .iter()
                .map(|d| d.var_char_value().unwrap_or("").to_string())
                .collect()
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            vec!["id", "name", "amount"],
            vec!["2", "purchase", "19.99"],
            vec!["1", "signup", "0.0"],
        ]
    );

    // The CSV Athena-style tooling reads from S3 is really there.
    let csv_key = output_location
        .strip_prefix(&format!("s3://{BUCKET}/"))
        .unwrap();
    let csv = storage.get_object(BUCKET, csv_key).await.unwrap();
    let csv = String::from_utf8(csv.to_vec()).unwrap();
    assert!(csv.contains("\"purchase\""), "{csv}");
    assert!(
        storage
            .get_object(BUCKET, &format!("{csv_key}.metadata"))
            .await
            .is_ok(),
        "metadata companion must exist"
    );

    // ---- Graceful shutdown flushes buffered records ----
    firehose
        .create_delivery_stream()
        .delivery_stream_name("late")
        .extended_s3_destination_configuration(parquet_destination("late", "late_events"))
        .send()
        .await
        .expect("CreateDeliveryStream late");
    firehose
        .put_record()
        .delivery_stream_name("late")
        .record(record(42, "buffered-at-shutdown", 1.0))
        .send()
        .await
        .expect("PutRecord late");
    assert!(parquet_objects(&storage, "late/").await.is_empty());

    let pid = server.0.id().to_string();
    let killed = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("send SIGTERM");
    assert!(killed.success());
    let status = server.0.wait().expect("wait for glaux-server");
    assert_eq!(status.code(), Some(0), "server must exit 0 after flushing");

    let late = parquet_objects(&storage, "late/").await;
    assert_eq!(late.len(), 1, "shutdown must flush the buffer: {late:?}");

    // The server is gone: its endpoint no longer answers.
    assert!(
        reqwest::get(format!("{gs_endpoint}/health")).await.is_err(),
        "server must have stopped listening"
    );
}
