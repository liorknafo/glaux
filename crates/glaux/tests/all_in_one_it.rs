//! End-to-end tests of the assembled all-in-one server, in one process:
//! S3 data and Glue metadata are created through fakecloud's wire protocols
//! on the single port, then glaux's Athena and Firehose operate on them —
//! with every HTTP request on that port recorded so the tests can prove the
//! engines reach S3/Glue in-process rather than over loopback.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use base64::Engine as _;
use glaux::{EMBEDDED_FAKECLOUD_SERVICES, Glaux, ServeOptions};
use glaux_athena::AthenaService;
use glaux_catalog::GlauxConfig;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

/// One recorded HTTP request: the AWS service it was routed to (from the
/// `X-Amz-Target` prefix or the SigV4 credential scope) plus method + path.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    service: String,
    method: String,
    path: String,
}

type Log = Arc<Mutex<Vec<Seen>>>;

fn classify(req: &Request) -> Seen {
    let headers = req.headers();
    let service = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|t| t.split('.').next())
        .map(|prefix| match prefix {
            "AmazonAthena" => "athena".to_string(),
            "AWSGlue" => "glue".to_string(),
            "AmazonSQS" => "sqs".to_string(),
            p if p.starts_with("Firehose_") => "firehose".to_string(),
            other => other.to_string(),
        })
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|a| a.split("Credential=").nth(1))
                .and_then(|c| c.split('/').nth(3))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "<unknown>".to_string());
    Seen {
        service,
        method: req.method().to_string(),
        path: req.uri().path().to_string(),
    }
}

async fn record(State(log): State<Log>, req: Request, next: Next) -> Response {
    log.lock().unwrap().push(classify(&req));
    next.run(req).await
}

struct Harness {
    base: String,
    client: reqwest::Client,
    log: Log,
    glaux_registry: Arc<fakecloud_core::registry::ServiceRegistry>,
}

impl Harness {
    async fn start() -> Self {
        let config = GlauxConfig {
            athena: glaux_catalog::AthenaConfig {
                output_location: Some("s3://results/".to_string()),
                workgroup: "primary".to_string(),
            },
            ..GlauxConfig::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let glaux = Glaux::build(&config, &ServeOptions { addr }).expect("build glaux");
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::clone(&glaux.registry);
        let router = glaux
            .router
            .layer(middleware::from_fn_with_state(Arc::clone(&log), record));
        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Self {
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
            log,
            glaux_registry: registry,
        }
    }

    fn sigv4(service: &str) -> String {
        format!(
            "AWS4-HMAC-SHA256 Credential=glaux-it/20260821/us-east-1/{service}/aws4_request, \
             SignedHeaders=host, Signature=0000"
        )
    }

    /// AWS JSON 1.1 call. Returns `(status, parsed body)`.
    async fn json(&self, target: &str, service: &str, body: Value) -> (u16, Value) {
        let response = self
            .client
            .post(&self.base)
            .header("x-amz-target", target)
            .header("content-type", "application/x-amz-json-1.1")
            .header("authorization", Self::sigv4(service))
            .body(body.to_string())
            .send()
            .await
            .expect("http call");
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, value)
    }

    async fn json_ok(&self, target: &str, service: &str, body: Value) -> Value {
        let (status, value) = self.json(target, service, body).await;
        assert_eq!(status, 200, "{target} failed: {value}");
        value
    }

    async fn s3_create_bucket(&self, bucket: &str) {
        let status = self
            .client
            .put(format!("{}/{bucket}", self.base))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .await
            .unwrap()
            .status();
        assert!(status.is_success(), "CreateBucket {bucket}: {status}");
    }

    async fn s3_put(&self, bucket: &str, key: &str, body: Vec<u8>) {
        let status = self
            .client
            .put(format!("{}/{bucket}/{key}", self.base))
            .header("authorization", Self::sigv4("s3"))
            .body(body)
            .send()
            .await
            .unwrap()
            .status();
        assert!(status.is_success(), "PutObject {bucket}/{key}: {status}");
    }

    async fn s3_get(&self, bucket: &str, key: &str) -> (u16, String) {
        let response = self
            .client
            .get(format!("{}/{bucket}/{key}", self.base))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.text().await.unwrap())
    }

    async fn s3_list_keys(&self, bucket: &str, prefix: &str) -> Vec<String> {
        let text = self
            .client
            .get(format!(
                "{}/{bucket}?list-type=2&prefix={prefix}",
                self.base
            ))
            .header("authorization", Self::sigv4("s3"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        text.split("<Key>")
            .skip(1)
            .map(|rest| rest.split("</Key>").next().unwrap().to_string())
            .collect()
    }

    async fn glue_table(&self, database: &str, table: &str, location: &str, serde: &str) {
        let _ = self
            .json(
                "AWSGlue.CreateDatabase",
                "glue",
                json!({ "DatabaseInput": { "Name": database } }),
            )
            .await;
        self.json_ok(
            "AWSGlue.CreateTable",
            "glue",
            json!({
                "DatabaseName": database,
                "TableInput": {
                    "Name": table,
                    "TableType": "EXTERNAL_TABLE",
                    "StorageDescriptor": {
                        "Columns": [
                            { "Name": "country", "Type": "string" },
                            { "Name": "amount", "Type": "bigint" },
                        ],
                        "Location": location,
                        "SerdeInfo": { "SerializationLibrary": serde },
                    },
                },
            }),
        )
        .await;
    }

    /// Run a query to completion; returns the execution id and the final
    /// `QueryExecution` document.
    async fn athena_run(&self, database: &str, sql: &str) -> (String, Value) {
        let started = self
            .json_ok(
                "AmazonAthena.StartQueryExecution",
                "athena",
                json!({
                    "QueryString": sql,
                    "QueryExecutionContext": { "Database": database },
                }),
            )
            .await;
        let id = started["QueryExecutionId"].as_str().unwrap().to_string();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let execution = self
                .json_ok(
                    "AmazonAthena.GetQueryExecution",
                    "athena",
                    json!({ "QueryExecutionId": id }),
                )
                .await;
            let state = execution["QueryExecution"]["Status"]["State"]
                .as_str()
                .unwrap()
                .to_string();
            if state == "SUCCEEDED" || state == "FAILED" || state == "CANCELLED" {
                return (id, execution);
            }
            assert!(Instant::now() < deadline, "query {id} still {state}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn athena_rows(&self, id: &str) -> Vec<Vec<String>> {
        let results = self
            .json_ok(
                "AmazonAthena.GetQueryResults",
                "athena",
                json!({ "QueryExecutionId": id }),
            )
            .await;
        results["ResultSet"]["Rows"]
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
            .collect()
    }

    fn seen_since(&self, mark: usize) -> Vec<Seen> {
        self.log.lock().unwrap()[mark..].to_vec()
    }

    fn mark(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

fn parquet_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["DE", "US", "DE", "FR", "US", "US"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )
    .unwrap();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    out
}

#[tokio::test]
async fn athena_queries_parquet_in_embedded_s3_through_glue_without_http_hops() {
    let h = Harness::start().await;
    h.s3_create_bucket("data").await;
    h.s3_create_bucket("results").await;
    h.s3_put("data", "events/part-0.parquet", parquet_bytes())
        .await;
    h.glue_table(
        "analytics",
        "events",
        "s3://data/events/",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    )
    .await;

    let mark = h.mark();
    let (id, execution) = h
        .athena_run(
            "analytics",
            "SELECT country, count(*) AS n, sum(amount) AS total \
             FROM events GROUP BY country ORDER BY country",
        )
        .await;
    assert_eq!(
        execution["QueryExecution"]["Status"]["State"], "SUCCEEDED",
        "{execution}"
    );
    let during_query = h.seen_since(mark);

    // Real SQL over real Parquet: an aggregation with the right numbers.
    let rows = h.athena_rows(&id).await;
    assert_eq!(
        rows,
        vec![
            vec!["country", "n", "total"],
            vec!["DE", "2", "40"],
            vec!["FR", "1", "40"],
            vec!["US", "3", "130"],
        ]
    );

    // The in-process backends measurably skipped HTTP: while the query ran,
    // the only traffic on the port was the Athena polling itself — no S3
    // reads of the Parquet file, no Glue metadata calls, and no S3 write of
    // the result CSV.
    let non_athena: Vec<&Seen> = during_query
        .iter()
        .filter(|s| s.service != "athena")
        .collect();
    assert!(
        non_athena.is_empty(),
        "expected zero S3/Glue HTTP traffic during the query, saw {non_athena:?}"
    );
    assert!(
        during_query
            .iter()
            .filter(|s| s.service == "athena")
            .count()
            >= 2,
        "the Athena calls themselves must have been recorded: {during_query:?}"
    );

    // ... yet the result CSV exists in the embedded S3, reachable over the
    // wire like real Athena's OutputLocation.
    let output = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        .as_str()
        .unwrap();
    let key = output.strip_prefix("s3://results/").unwrap();
    let (status, csv) = h.s3_get("results", key).await;
    assert_eq!(status, 200, "result CSV must be readable over S3: {csv}");
    assert_eq!(
        csv,
        "\"country\",\"n\",\"total\"\n\"DE\",\"2\",\"40\"\n\"FR\",\"1\",\"40\"\n\"US\",\"3\",\"130\"\n"
    );
    let (status, metadata) = h.s3_get("results", &format!("{key}.metadata")).await;
    assert_eq!(status, 200, "metadata companion must exist: {metadata}");
}

#[tokio::test]
async fn firehose_delivers_into_embedded_s3_and_athena_reads_it_back() {
    let h = Harness::start().await;
    h.s3_create_bucket("lake").await;
    h.s3_create_bucket("results").await;

    h.json_ok(
        "Firehose_20150804.CreateDeliveryStream",
        "firehose",
        json!({
            "DeliveryStreamName": "clicks",
            "DeliveryStreamType": "DirectPut",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/firehose",
                "BucketARN": "arn:aws:s3:::lake",
                "Prefix": "clicks/",
                "ErrorOutputPrefix": "errors/",
                "BufferingHints": { "SizeInMBs": 64, "IntervalInSeconds": 900 },
            },
        }),
    )
    .await;

    let records: Vec<Value> = ["DE", "US", "US"]
        .iter()
        .enumerate()
        .map(|(i, country)| {
            let line = format!(
                "{}\n",
                json!({ "country": country, "amount": (i as i64 + 1) * 100 })
            );
            json!({ "Data": base64::engine::general_purpose::STANDARD.encode(line) })
        })
        .collect();
    let put = h
        .json_ok(
            "Firehose_20150804.PutRecordBatch",
            "firehose",
            json!({ "DeliveryStreamName": "clicks", "Records": records }),
        )
        .await;
    assert_eq!(put["FailedPutCount"], 0, "{put}");

    // Deleting the stream flushes its buffer to S3.
    h.json_ok(
        "Firehose_20150804.DeleteDeliveryStream",
        "firehose",
        json!({ "DeliveryStreamName": "clicks" }),
    )
    .await;
    let keys = h.s3_list_keys("lake", "clicks/").await;
    assert_eq!(
        keys.len(),
        1,
        "one delivered object under the prefix: {keys:?}"
    );
    let (status, body) = h.s3_get("lake", &keys[0]).await;
    assert_eq!(status, 200);
    assert_eq!(body.lines().count(), 3, "{body}");

    h.glue_table(
        "lake",
        "clicks",
        "s3://lake/clicks/",
        "org.openx.data.jsonserde.JsonSerDe",
    )
    .await;
    let (id, execution) = h
        .athena_run(
            "lake",
            "SELECT country, sum(amount) AS total FROM clicks GROUP BY country ORDER BY country",
        )
        .await;
    assert_eq!(
        execution["QueryExecution"]["Status"]["State"], "SUCCEEDED",
        "{execution}"
    );
    assert_eq!(
        h.athena_rows(&id).await,
        vec![
            vec!["country", "total"],
            vec!["DE", "100"],
            vec!["US", "500"],
        ]
    );
}

#[tokio::test]
async fn unsupported_sql_fails_loudly_naming_the_construct() {
    let h = Harness::start().await;
    h.s3_create_bucket("data").await;
    h.s3_create_bucket("results").await;
    h.s3_put("data", "events/part-0.parquet", parquet_bytes())
        .await;
    h.glue_table(
        "analytics",
        "events",
        "s3://data/events/",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    )
    .await;

    let (_, execution) = h
        .athena_run(
            "analytics",
            "SELECT * FROM events TABLESAMPLE BERNOULLI (10)",
        )
        .await;
    let status = &execution["QueryExecution"]["Status"];
    assert_eq!(status["State"], "FAILED", "{execution}");
    let reason = status["StateChangeReason"].as_str().unwrap_or("");
    assert!(
        reason.contains("TABLESAMPLE"),
        "failure must name the construct, got: {reason}"
    );

    let (_, execution) = h
        .athena_run("analytics", "SELECT * FROM no_such_table")
        .await;
    assert_eq!(
        execution["QueryExecution"]["Status"]["State"], "FAILED",
        "{execution}"
    );
    let reason = execution["QueryExecution"]["Status"]["StateChangeReason"]
        .as_str()
        .unwrap_or("");
    assert!(reason.contains("no_such_table"), "{reason}");
}

#[tokio::test]
async fn registry_serves_fakecloud_services_with_glaux_athena_and_firehose_in_their_slots() {
    let h = Harness::start().await;

    let mut names: Vec<&str> = h.glaux_registry.service_names();
    names.sort();
    for expected in EMBEDDED_FAKECLOUD_SERVICES
        .iter()
        .chain(["athena", "firehose"].iter())
    {
        assert!(
            names.contains(expected),
            "{expected} missing from {names:?}"
        );
    }

    // The `athena` slot is glaux's implementation: it advertises exactly
    // glaux's action set (fakecloud's stub advertises a different one).
    let athena = h.glaux_registry.get("athena").unwrap();
    assert_eq!(athena.supported_actions(), AthenaService::SUPPORTED_ACTIONS);
    let firehose = h.glaux_registry.get("firehose").unwrap();
    assert_eq!(
        firehose.supported_actions(),
        glaux_firehose::FirehoseService::SUPPORTED_ACTIONS
    );

    // And the wire reaches them: an Athena workgroup listing reports glaux's
    // engine, an unknown Athena action is an explicit error, not a stub.
    let workgroups = h
        .json_ok("AmazonAthena.ListWorkGroups", "athena", json!({}))
        .await;
    assert_eq!(
        workgroups["WorkGroups"][0]["Name"], "primary",
        "{workgroups}"
    );
    let (status, body) = h
        .json("AmazonAthena.CreateNamedQuery", "athena", json!({}))
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body.to_string().contains("CreateNamedQuery"),
        "error must name the action: {body}"
    );

    // fakecloud's own control plane is live on the same port.
    let queue = h
        .json_ok(
            "AmazonSQS.CreateQueue",
            "sqs",
            json!({ "QueueName": "pipeline" }),
        )
        .await;
    assert!(
        queue["QueueUrl"].as_str().unwrap().ends_with("/pipeline"),
        "{queue}"
    );
}

#[test]
fn external_endpoints_are_refused_explicitly() {
    let config = GlauxConfig {
        s3_endpoint: Some("http://minio:9000".to_string()),
        ..GlauxConfig::default()
    };
    let err = Glaux::build(&config, &ServeOptions::default())
        .err()
        .unwrap();
    assert!(
        err.contains("minio:9000") && err.contains("glaux-server"),
        "{err}"
    );

    let config = GlauxConfig {
        glue_endpoint: Some("http://glue:1".to_string()),
        ..GlauxConfig::default()
    };
    let err = Glaux::build(&config, &ServeOptions::default())
        .err()
        .unwrap();
    assert!(
        err.contains("glue:1") && err.contains("glaux-server"),
        "{err}"
    );
}
