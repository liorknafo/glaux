//! The flagship pipeline against the all-in-one `glaux` binary:
//!
//! ```text
//! EventBridge PutEvents → rule → SQS → consumer → Firehose PutRecordBatch
//!     → Parquet in S3 → Athena SELECT returns the events
//! ```
//!
//! `glaux` serves S3, Glue, Firehose, and Athena. fakecloud's EventBridge is
//! not embeddable next to DataFusion (`links = "lzma"` clash, see
//! `crates/glaux/README.md`), so the EventBridge → SQS hop runs on a separate
//! fakecloud process: taken from `PATH`, otherwise downloaded once from
//! GitHub releases. When neither works the test **skips with a clear
//! message** — unless `GLAUX_REQUIRE_FAKECLOUD=1` (set in CI), in which case
//! it fails. It never fakes a result.
//!
//! The consumer is the piece a real application would own: it drains the
//! queue, unwraps the EventBridge envelope, and forwards `detail` to
//! Firehose. Everything downstream — buffering, JSON→Parquet conversion with
//! the Glue schema, the S3 write, SQL — is glaux, and the assertions compare
//! Athena's answer with the events that were published.
//!
//! `examples/flagship-pipeline.sh` is the same flow driven by the AWS CLI;
//! the release workflow runs that script against the built binary.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::{Value, json};

/// fakecloud release used for the EventBridge hop. Kept in step with the
/// crates the binary embeds (`glaux::FAKECLOUD_VERSION`).
const FAKECLOUD_VERSION: &str = "v0.44.10";

struct Process {
    child: Child,
    base: String,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

/// Download and extract the fakecloud release into a cached temp dir
/// (shared with `glaux-catalog`'s fakecloud test). `None` when offline.
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
    if !matches!(curl, Ok(status) if status.success()) {
        eprintln!("could not download {url}: {curl:?}");
        return None;
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
        other => {
            eprintln!("failed to extract fakecloud archive: {other:?}");
            None
        }
    }
}

fn wait_for_health(url: &str, what: &str) {
    let client = reqwest::blocking::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(resp) = client.get(url).send()
            && resp.status().is_success()
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} did not become healthy at {url}");
}

fn start_fakecloud(binary: &PathBuf) -> Process {
    let port = free_port();
    let child = Command::new(binary)
        .args(["--addr", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fakecloud");
    let base = format!("http://127.0.0.1:{port}");
    wait_for_health(&format!("{base}/_fakecloud/health"), "fakecloud");
    Process { child, base }
}

fn start_glaux() -> Process {
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_glaux"))
        .args([
            "--addr",
            &format!("127.0.0.1:{port}"),
            "--athena-output-location",
            "s3://athena-results/",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn glaux");
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "glaux did not report listening");
        match lines.next() {
            Some(Ok(line)) if line.contains("listening on") => break,
            Some(Ok(_)) => continue,
            other => panic!("glaux exited before listening: {other:?}"),
        }
    }
    std::thread::spawn(move || for _ in lines {});
    Process {
        child,
        base: format!("http://127.0.0.1:{port}"),
    }
}

/// Minimal AWS client over the wire protocols the two processes speak.
struct Aws {
    http: reqwest::blocking::Client,
}

impl Aws {
    fn new() -> Self {
        Self {
            http: reqwest::blocking::Client::new(),
        }
    }

    fn sigv4(service: &str) -> String {
        format!(
            "AWS4-HMAC-SHA256 Credential=glaux-it/20260821/us-east-1/{service}/aws4_request, \
             SignedHeaders=host, Signature=0000"
        )
    }

    /// AWS JSON call (`AmazonSQS.*` is JSON 1.0, the rest JSON 1.1).
    fn json(&self, base: &str, target: &str, body: Value) -> (u16, Value) {
        let (service, version) = match target.split('.').next().unwrap() {
            "AmazonSQS" => ("sqs", "1.0"),
            "AWSEvents" => ("events", "1.1"),
            "AWSGlue" => ("glue", "1.1"),
            "Firehose_20150804" => ("firehose", "1.1"),
            "AmazonAthena" => ("athena", "1.1"),
            other => panic!("unknown target prefix {other}"),
        };
        let response = self
            .http
            .post(base)
            .header("x-amz-target", target)
            .header("content-type", format!("application/x-amz-json-{version}"))
            .header("authorization", Self::sigv4(service))
            .body(body.to_string())
            .send()
            .expect("http call");
        let status = response.status().as_u16();
        let text = response.text().unwrap();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    fn json_ok(&self, base: &str, target: &str, body: Value) -> Value {
        let (status, value) = self.json(base, target, body);
        assert_eq!(status, 200, "{target} failed: {value}");
        value
    }

    fn s3_create_bucket(&self, base: &str, bucket: &str) {
        let status = self
            .http
            .put(format!("{base}/{bucket}"))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .unwrap()
            .status();
        assert!(status.is_success(), "CreateBucket {bucket}: {status}");
    }

    fn s3_list_keys(&self, base: &str, bucket: &str, prefix: &str) -> Vec<String> {
        let text = self
            .http
            .get(format!("{base}/{bucket}?list-type=2&prefix={prefix}"))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .unwrap()
            .text()
            .unwrap();
        text.split("<Key>")
            .skip(1)
            .map(|rest| rest.split("</Key>").next().unwrap().to_string())
            .collect()
    }

    fn s3_get(&self, base: &str, bucket: &str, key: &str) -> Vec<u8> {
        let response = self
            .http
            .get(format!("{base}/{bucket}/{key}"))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .unwrap();
        assert_eq!(response.status(), 200, "GetObject {bucket}/{key}");
        response.bytes().unwrap().to_vec()
    }
}

/// One published order: what goes into EventBridge and what Athena must
/// give back.
#[derive(Clone)]
struct Order {
    order_id: i64,
    country: &'static str,
    amount: f64,
}

fn orders() -> Vec<Order> {
    ["DE", "US", "US", "FR", "DE", "US"]
        .iter()
        .enumerate()
        .map(|(i, country)| Order {
            order_id: i as i64 + 1,
            country,
            amount: (i as f64 + 1.0) * 10.0 + 0.5,
        })
        .collect()
}

#[test]
fn events_published_to_eventbridge_come_back_from_athena_as_parquet_rows() {
    let Some(fakecloud_bin) = fakecloud_on_path().or_else(download_fakecloud) else {
        let message = "fakecloud binary not available (not on PATH, download failed): \
                       the EventBridge hop of the flagship pipeline cannot run";
        if std::env::var_os("GLAUX_REQUIRE_FAKECLOUD").is_some() {
            panic!("{message}");
        }
        eprintln!("SKIPPED: {message}");
        return;
    };

    let fakecloud = start_fakecloud(&fakecloud_bin);
    let glaux = start_glaux();
    let aws = Aws::new();
    let (fc, gx) = (fakecloud.base.as_str(), glaux.base.as_str());

    // 1. Storage and catalog on glaux: the Glue table is both the Parquet
    //    conversion schema and the table Athena queries.
    aws.s3_create_bucket(gx, "lake");
    aws.s3_create_bucket(gx, "athena-results");
    aws.json_ok(
        gx,
        "AWSGlue.CreateDatabase",
        json!({ "DatabaseInput": { "Name": "shop" } }),
    );
    aws.json_ok(
        gx,
        "AWSGlue.CreateTable",
        json!({
            "DatabaseName": "shop",
            "TableInput": {
                "Name": "orders",
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Columns": [
                        { "Name": "order_id", "Type": "bigint" },
                        { "Name": "country", "Type": "string" },
                        { "Name": "amount", "Type": "double" },
                    ],
                    "Location": "s3://lake/orders/",
                    "SerdeInfo": {
                        "SerializationLibrary":
                            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
            },
        }),
    );

    // 2. Firehose stream on glaux, JSON → Parquet, zero-second buffering so
    //    the pipeline is observable without waiting for an interval.
    aws.json_ok(
        gx,
        "Firehose_20150804.CreateDeliveryStream",
        json!({
            "DeliveryStreamName": "orders",
            "DeliveryStreamType": "DirectPut",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/firehose",
                "BucketARN": "arn:aws:s3:::lake",
                "Prefix": "orders/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
                "BufferingHints": { "SizeInMBs": 64, "IntervalInSeconds": 0 },
                "DataFormatConversionConfiguration": {
                    "Enabled": true,
                    "SchemaConfiguration": { "DatabaseName": "shop", "TableName": "orders" },
                    "InputFormatConfiguration": { "Deserializer": { "OpenXJsonSerDe": {} } },
                    "OutputFormatConfiguration": { "Serializer": { "ParquetSerDe": {} } },
                },
            },
        }),
    );

    // 3. EventBridge rule → SQS queue on fakecloud.
    let queue_url = aws.json_ok(
        fc,
        "AmazonSQS.CreateQueue",
        json!({ "QueueName": "orders" }),
    )["QueueUrl"]
        .as_str()
        .unwrap()
        .to_string();
    let queue_arn = aws.json_ok(
        fc,
        "AmazonSQS.GetQueueAttributes",
        json!({ "QueueUrl": queue_url, "AttributeNames": ["QueueArn"] }),
    )["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_string();
    aws.json_ok(
        fc,
        "AWSEvents.PutRule",
        json!({
            "Name": "orders-to-sqs",
            "EventPattern": json!({ "source": ["shop.orders"], "detail-type": ["OrderPlaced"] }).to_string(),
        }),
    );
    let targets = aws.json_ok(
        fc,
        "AWSEvents.PutTargets",
        json!({ "Rule": "orders-to-sqs", "Targets": [{ "Id": "orders-queue", "Arn": queue_arn }] }),
    );
    assert_eq!(targets["FailedEntryCount"], 0, "{targets}");

    let published = orders();
    let entries: Vec<Value> = published
        .iter()
        .map(|o| {
            json!({
                "Source": "shop.orders",
                "DetailType": "OrderPlaced",
                "Detail": json!({ "order_id": o.order_id, "country": o.country, "amount": o.amount }).to_string(),
            })
        })
        .collect();
    // A second event type the rule must not route.
    let mut entries_with_noise = entries.clone();
    entries_with_noise.push(json!({
        "Source": "shop.orders",
        "DetailType": "OrderCancelled",
        "Detail": json!({ "order_id": 999, "country": "XX", "amount": 1.0 }).to_string(),
    }));
    let put = aws.json_ok(
        fc,
        "AWSEvents.PutEvents",
        json!({ "Entries": entries_with_noise }),
    );
    assert_eq!(put["FailedEntryCount"], 0, "{put}");

    // 4. The consumer: drain SQS, unwrap the envelope, forward `detail` to
    //    Firehose, delete what was forwarded.
    let mut forwarded = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    while forwarded < published.len() {
        assert!(
            Instant::now() < deadline,
            "consumer forwarded only {forwarded} of {} events",
            published.len()
        );
        let received = aws.json_ok(
            fc,
            "AmazonSQS.ReceiveMessage",
            json!({ "QueueUrl": queue_url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 1 }),
        );
        let Some(messages) = received["Messages"].as_array().filter(|m| !m.is_empty()) else {
            continue;
        };
        let records: Vec<Value> = messages
            .iter()
            .map(|m| {
                let envelope: Value = serde_json::from_str(m["Body"].as_str().unwrap()).unwrap();
                assert_eq!(envelope["source"], "shop.orders", "{envelope}");
                assert_eq!(envelope["detail-type"], "OrderPlaced", "{envelope}");
                let line = format!("{}\n", envelope["detail"]);
                json!({ "Data": base64::engine::general_purpose::STANDARD.encode(line) })
            })
            .collect();
        let put = aws.json_ok(
            gx,
            "Firehose_20150804.PutRecordBatch",
            json!({ "DeliveryStreamName": "orders", "Records": records }),
        );
        assert_eq!(put["FailedPutCount"], 0, "{put}");
        for m in messages {
            aws.json_ok(
                fc,
                "AmazonSQS.DeleteMessage",
                json!({ "QueueUrl": queue_url, "ReceiptHandle": m["ReceiptHandle"] }),
            );
        }
        forwarded += messages.len();
    }
    assert_eq!(
        forwarded,
        published.len(),
        "the rule must not route OrderCancelled"
    );
    // Keep draining for a short window: the queue must now be empty. Any
    // further message means the rule routed something it must not (the
    // OrderCancelled event) and the pipeline would be forwarding wrong data.
    let quiet_until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < quiet_until {
        let received = aws.json_ok(
            fc,
            "AmazonSQS.ReceiveMessage",
            json!({ "QueueUrl": queue_url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 1 }),
        );
        if let Some(messages) = received["Messages"].as_array().filter(|m| !m.is_empty()) {
            let bodies: Vec<&str> = messages
                .iter()
                .map(|m| m["Body"].as_str().unwrap_or(""))
                .collect();
            panic!(
                "rule routed {} unexpected event(s) to the queue after all OrderPlaced events were consumed: {bodies:?}",
                messages.len()
            );
        }
    }

    // 5. Parquet objects under the prefix; nothing under the error prefix.
    let deadline = Instant::now() + Duration::from_secs(30);
    let keys = loop {
        let keys = aws.s3_list_keys(gx, "lake", "orders/");
        if !keys.is_empty() {
            break keys;
        }
        assert!(
            Instant::now() < deadline,
            "Firehose delivered nothing under orders/"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    for key in &keys {
        assert!(
            key.ends_with(".parquet"),
            "converted object must be Parquet: {key}"
        );
        let bytes = aws.s3_get(gx, "lake", key);
        assert_eq!(&bytes[..4], b"PAR1", "{key} is not a Parquet file");
        assert_eq!(
            &bytes[bytes.len() - 4..],
            b"PAR1",
            "{key} is not a Parquet file"
        );
    }
    assert!(
        aws.s3_list_keys(gx, "lake", "errors/").is_empty(),
        "records landed under the error prefix"
    );

    // 6. Athena reads the events back — and agrees with what was published.
    let started = aws.json_ok(
        gx,
        "AmazonAthena.StartQueryExecution",
        json!({
            "QueryString": "SELECT country, count(*) AS orders, round(sum(amount), 2) AS revenue \
                            FROM orders GROUP BY country ORDER BY country",
            "QueryExecutionContext": { "Database": "shop" },
        }),
    );
    let id = started["QueryExecutionId"].as_str().unwrap().to_string();
    let deadline = Instant::now() + Duration::from_secs(30);
    let execution = loop {
        let execution = aws.json_ok(
            gx,
            "AmazonAthena.GetQueryExecution",
            json!({ "QueryExecutionId": id }),
        );
        let state = execution["QueryExecution"]["Status"]["State"]
            .as_str()
            .unwrap();
        if state != "QUEUED" && state != "RUNNING" {
            break execution;
        }
        assert!(Instant::now() < deadline, "query still {state}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        execution["QueryExecution"]["Status"]["State"], "SUCCEEDED",
        "{execution}"
    );
    let results = aws.json_ok(
        gx,
        "AmazonAthena.GetQueryResults",
        json!({ "QueryExecutionId": id }),
    );
    let rows: Vec<Vec<String>> = results["ResultSet"]["Rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row["Data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d["VarCharValue"].as_str().unwrap_or("").to_string())
                .collect()
        })
        .collect();

    let mut expected: BTreeMap<&str, (usize, f64)> = BTreeMap::new();
    for o in &published {
        let entry = expected.entry(o.country).or_default();
        entry.0 += 1;
        entry.1 += o.amount;
    }
    let mut expected_rows = vec![vec![
        "country".to_string(),
        "orders".to_string(),
        "revenue".to_string(),
    ]];
    expected_rows.extend(expected.iter().map(|(country, (count, revenue))| {
        vec![
            country.to_string(),
            count.to_string(),
            format!("{revenue:.1}"),
        ]
    }));
    assert_eq!(rows, expected_rows, "{results}");

    // Every individual event is there too, in order.
    let started = aws.json_ok(
        gx,
        "AmazonAthena.StartQueryExecution",
        json!({
            "QueryString": "SELECT order_id, country, amount FROM shop.orders ORDER BY order_id",
        }),
    );
    let id = started["QueryExecutionId"].as_str().unwrap().to_string();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let execution = aws.json_ok(
            gx,
            "AmazonAthena.GetQueryExecution",
            json!({ "QueryExecutionId": id }),
        );
        match execution["QueryExecution"]["Status"]["State"]
            .as_str()
            .unwrap()
        {
            "SUCCEEDED" => break,
            "QUEUED" | "RUNNING" => {
                assert!(Instant::now() < deadline, "query still running");
                std::thread::sleep(Duration::from_millis(20));
            }
            other => panic!("query {other}: {execution}"),
        }
    }
    let results = aws.json_ok(
        gx,
        "AmazonAthena.GetQueryResults",
        json!({ "QueryExecutionId": id }),
    );
    let rows = results["ResultSet"]["Rows"].as_array().unwrap();
    assert_eq!(rows.len(), published.len() + 1, "{results}");
    for (row, o) in rows.iter().skip(1).zip(&published) {
        let cells: Vec<&str> = row["Data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["VarCharValue"].as_str().unwrap())
            .collect();
        assert_eq!(cells[0], o.order_id.to_string());
        assert_eq!(cells[1], o.country);
        assert_eq!(cells[2].parse::<f64>().unwrap(), o.amount);
    }

    // And, as on real Athena, the CSV landed in the OutputLocation.
    let csv = aws.s3_get(gx, "athena-results", &format!("{id}.csv"));
    assert_eq!(
        String::from_utf8(csv).unwrap().lines().count(),
        published.len() + 1
    );
}
