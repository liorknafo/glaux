//! In-process router tests: `/health`, `X-Amz-Target` routing to each
//! service, and the explicit refusal of targets neither service owns.
//! These need no S3/Glue: the actions exercised never touch storage.

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use glaux_athena::{AthenaService, AthenaServiceConfig, TrinoEngine};
use glaux_catalog::{ConfigOverrides, GlauxConfig, NetworkStorageBackend, StorageBackend};
use glaux_firehose::{DeliverySink, FirehoseService, FirehoseServiceConfig, RecordingSink};
use glaux_server::app::{AppState, router};
use serde_json::Value;

async fn serve() -> SocketAddr {
    let config = GlauxConfig::load(
        None,
        &ConfigOverrides {
            s3_endpoint: Some("http://127.0.0.1:1".to_string()),
            glue_endpoint: Some("http://127.0.0.1:1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(NetworkStorageBackend::new(&config));
    let athena = Arc::new(AthenaService::new(
        AthenaServiceConfig::default(),
        Arc::new(TrinoEngine::new(SessionContext::new(), "datafusion")),
        storage,
    ));
    let sink: Arc<dyn DeliverySink> = Arc::new(RecordingSink::new());
    let firehose = Arc::new(FirehoseService::new(FirehoseServiceConfig::default(), sink));
    let state = AppState::new(athena, firehose, &config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, router(state)).into_future());
    addr
}

async fn post(addr: SocketAddr, target: Option<&str>, body: &str) -> (u16, Value, Option<String>) {
    let mut req = reqwest::Client::new()
        .post(format!("http://{addr}/"))
        .header("content-type", "application/x-amz-json-1.1")
        .body(body.to_string());
    if let Some(t) = target {
        req = req.header("x-amz-target", t);
    }
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    let errtype = resp
        .headers()
        .get("x-amzn-errortype")
        .map(|v| v.to_str().unwrap().to_string());
    let body: Value = resp.json().await.unwrap();
    (status, body, errtype)
}

#[tokio::test]
async fn health_reports_configuration_and_both_services() {
    let addr = serve().await;
    let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["service"], "glaux-server");
    assert_eq!(body["s3_endpoint"], "http://127.0.0.1:1");
    assert_eq!(body["glue_endpoint"], "http://127.0.0.1:1");
    assert_eq!(body["services"]["athena"]["target_prefix"], "AmazonAthena.");
    assert!(
        body["services"]["athena"]["actions"]
            .as_array()
            .unwrap()
            .contains(&Value::from("StartQueryExecution"))
    );
    assert!(
        body["services"]["firehose"]["actions"]
            .as_array()
            .unwrap()
            .contains(&Value::from("PutRecordBatch"))
    );
}

#[tokio::test]
async fn athena_targets_reach_the_athena_service() {
    let addr = serve().await;
    let (status, body, _) = post(addr, Some("AmazonAthena.ListWorkGroups"), "{}").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["WorkGroups"][0]["Name"], "primary");
}

#[tokio::test]
async fn firehose_targets_reach_the_firehose_service() {
    let addr = serve().await;
    let (status, body, _) = post(addr, Some("Firehose_20150804.ListDeliveryStreams"), "{}").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["DeliveryStreamNames"], Value::Array(vec![]));
    assert_eq!(body["HasMoreDeliveryStreams"], false);
}

#[tokio::test]
async fn unknown_actions_within_a_service_are_named() {
    let addr = serve().await;
    let (status, body, errtype) = post(addr, Some("AmazonAthena.CreateNamedQuery"), "{}").await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(errtype.as_deref(), Some("UnknownOperationException"));
    // Athena's wire shape capitalizes the key.
    assert!(
        body["Message"]
            .as_str()
            .unwrap()
            .contains("CreateNamedQuery"),
        "{body}"
    );
}

#[tokio::test]
async fn foreign_targets_and_missing_targets_are_refused_explicitly() {
    let addr = serve().await;

    let (status, body, errtype) = post(addr, Some("AWSGlue.GetDatabases"), "{}").await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(errtype.as_deref(), Some("UnknownOperationException"));
    assert_eq!(body["__type"], "UnknownOperationException");
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("AWSGlue.GetDatabases"), "{message}");
    assert!(message.contains("AmazonAthena."), "{message}");
    assert!(message.contains("Firehose_20150804."), "{message}");

    let (status, body, _) = post(addr, None, "{}").await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("missing X-Amz-Target"),
        "{body}"
    );
}
