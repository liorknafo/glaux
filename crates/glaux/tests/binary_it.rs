//! The `glaux` binary itself: it starts, serves fakecloud's S3 and glaux's
//! Athena on one port, and executes real SQL.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Server {
    child: Child,
    base: String,
}

impl Drop for Server {
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

fn start() -> Server {
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_glaux"))
        .args([
            "--addr",
            &format!("127.0.0.1:{port}"),
            "--athena-output-location",
            "s3://results/",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn glaux");
    let stderr = child.stderr.take().unwrap();
    // Wait for the listening line so the test never races the bind.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut lines = BufReader::new(stderr).lines();
    loop {
        assert!(Instant::now() < deadline, "glaux did not report listening");
        match lines.next() {
            Some(Ok(line)) if line.contains("listening on") => break,
            Some(Ok(_)) => continue,
            other => panic!("glaux exited before listening: {other:?}"),
        }
    }
    // Keep draining stderr so the child never blocks on a full pipe.
    std::thread::spawn(move || for _ in lines {});
    Server {
        child,
        base: format!("http://127.0.0.1:{port}"),
    }
}

fn sigv4(service: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=glaux-it/20260821/us-east-1/{service}/aws4_request, \
         SignedHeaders=host, Signature=0000"
    )
}

fn json_call(base: &str, target: &str, service: &str, body: Value) -> (u16, Value) {
    let response = reqwest::blocking::Client::new()
        .post(base)
        .header("x-amz-target", target)
        .header("content-type", "application/x-amz-json-1.1")
        .header("authorization", sigv4(service))
        .body(body.to_string())
        .send()
        .unwrap();
    let status = response.status().as_u16();
    let text = response.text().unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

#[test]
fn binary_serves_embedded_s3_and_executes_real_sql_on_one_port() {
    let server = start();
    let client = reqwest::blocking::Client::new();

    let health = client
        .get(format!("{}/_glaux/health", server.base))
        .send()
        .unwrap();
    assert_eq!(health.status(), 200);

    // fakecloud S3 on the port...
    let status = client
        .put(format!("{}/results", server.base))
        .header("authorization", sigv4("s3"))
        .send()
        .unwrap()
        .status();
    assert!(status.is_success(), "CreateBucket: {status}");

    // ...and glaux Athena on the same port, executing real SQL.
    let (status, started) = json_call(
        &server.base,
        "AmazonAthena.StartQueryExecution",
        "athena",
        json!({ "QueryString": "SELECT 40 + 2 AS answer, upper('glaux') AS name" }),
    );
    assert_eq!(status, 200, "{started}");
    let id = started["QueryExecutionId"].as_str().unwrap().to_string();

    let deadline = Instant::now() + Duration::from_secs(30);
    let state = loop {
        let (_, execution) = json_call(
            &server.base,
            "AmazonAthena.GetQueryExecution",
            "athena",
            json!({ "QueryExecutionId": id }),
        );
        let state = execution["QueryExecution"]["Status"]["State"]
            .as_str()
            .unwrap()
            .to_string();
        if state != "QUEUED" && state != "RUNNING" {
            break (state, execution);
        }
        assert!(Instant::now() < deadline, "query still {state}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(state.0, "SUCCEEDED", "{}", state.1);

    let (_, results) = json_call(
        &server.base,
        "AmazonAthena.GetQueryResults",
        "athena",
        json!({ "QueryExecutionId": id }),
    );
    let rows = &results["ResultSet"]["Rows"];
    assert_eq!(rows[0]["Data"][0]["VarCharValue"], "answer", "{results}");
    assert_eq!(rows[1]["Data"][0]["VarCharValue"], "42", "{results}");
    assert_eq!(rows[1]["Data"][1]["VarCharValue"], "GLAUX", "{results}");
}

#[test]
fn unknown_flags_exit_with_usage_and_version_names_fakecloud() {
    let output = Command::new(env!("CARGO_BIN_EXE_glaux"))
        .arg("--bogus")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown argument \"--bogus\""), "{stderr}");
    assert!(stderr.contains("USAGE"), "{stderr}");

    let output = Command::new(env!("CARGO_BIN_EXE_glaux"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("fakecloud {}", glaux::FAKECLOUD_VERSION)),
        "{stdout}"
    );
}
