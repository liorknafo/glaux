//! Wire-level tests for `NetworkGlueApi` against an in-test mock Glue
//! endpoint: request shape (JSON 1.1 headers, SigV4 signature), pagination,
//! and error mapping.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use glaux_catalog::{CatalogError, ConfigOverrides, GlauxConfig, GlueApi, NetworkGlueApi};
use serde_json::{Value, json};

/// One request the mock server saw, as observed on the wire.
#[derive(Debug, Clone)]
struct RecordedRequest {
    target: String,
    content_type: String,
    authorization: String,
    body: Value,
}

#[derive(Clone)]
struct MockGlue {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

async fn handle(
    State(state): State<MockGlue>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    let target = header("x-amz-target");
    let body: Value = serde_json::from_slice(&body).expect("mock received non-JSON body");
    state.requests.lock().unwrap().push(RecordedRequest {
        target: target.clone(),
        content_type: header("content-type"),
        authorization: header("authorization"),
        body: body.clone(),
    });

    match target.as_str() {
        "AWSGlue.GetDatabases" => match body.get("NextToken").and_then(Value::as_str) {
            None => (
                StatusCode::OK,
                json!({
                    "DatabaseList": [{ "Name": "db_page_one" }],
                    "NextToken": "page-two",
                })
                .to_string(),
            ),
            Some("page-two") => (
                StatusCode::OK,
                json!({ "DatabaseList": [{ "Name": "db_page_two" }] }).to_string(),
            ),
            Some(other) => panic!("mock got unexpected NextToken {other:?}"),
        },
        "AWSGlue.GetTable" => {
            let name = body.get("Name").and_then(Value::as_str).unwrap_or_default();
            if name == "missing" {
                (
                    StatusCode::BAD_REQUEST,
                    json!({
                        "__type": "com.amazonaws.glue#EntityNotFoundException",
                        "Message": "Table missing not found in database events",
                    })
                    .to_string(),
                )
            } else {
                (
                    StatusCode::OK,
                    json!({
                        "Table": {
                            "Name": name,
                            "DatabaseName": body.get("DatabaseName"),
                            "TableType": "EXTERNAL_TABLE",
                            "PartitionKeys": [{ "Name": "dt", "Type": "string" }],
                            "StorageDescriptor": {
                                "Columns": [
                                    { "Name": "id", "Type": "bigint" },
                                    { "Name": "payload", "Type": "string" },
                                ],
                                "Location": "s3://data/events/",
                                "SerdeInfo": {
                                    "SerializationLibrary":
                                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                },
                            },
                        }
                    })
                    .to_string(),
                )
            }
        }
        "AWSGlue.GetPartitions" => (
            StatusCode::OK,
            json!({
                "Partitions": [
                    {
                        "Values": ["2026-08-01"],
                        "StorageDescriptor": { "Location": "s3://data/events/dt=2026-08-01/" },
                    },
                ],
            })
            .to_string(),
        ),
        "AWSGlue.GetTables" => (
            StatusCode::BAD_REQUEST,
            json!({
                "__type": "InvalidInputException",
                "message": "DatabaseName must not be empty",
            })
            .to_string(),
        ),
        other => panic!("mock got unexpected target {other:?}"),
    }
}

/// Start the mock server; returns its address and the recorded requests.
async fn start_mock() -> (SocketAddr, Arc<Mutex<Vec<RecordedRequest>>>) {
    let state = MockGlue {
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let requests = Arc::clone(&state.requests);
    let app = axum::Router::new()
        .route("/", post(handle))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("mock local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server");
    });
    (addr, requests)
}

fn client_for(addr: SocketAddr) -> NetworkGlueApi {
    let overrides = ConfigOverrides {
        glue_endpoint: Some(format!("http://{addr}")),
        region: Some("eu-central-1".to_string()),
        credentials: Some(glaux_catalog::AwsCredentials {
            access_key_id: "AKIDTEST".to_string(),
            secret_access_key: "test-secret".to_string(),
            session_token: None,
        }),
        ..Default::default()
    };
    let config = GlauxConfig::load(None, &overrides).expect("test config");
    NetworkGlueApi::new(&config)
}

#[tokio::test]
async fn get_databases_follows_pagination_and_signs_requests() {
    let (addr, requests) = start_mock().await;
    let glue = client_for(addr);

    let databases = glue.get_databases().await.expect("get_databases");
    assert_eq!(
        databases
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        vec!["db_page_one", "db_page_two"],
        "both pages must be merged in order"
    );

    let recorded = requests.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "pagination must issue exactly two calls");
    for req in &recorded {
        assert_eq!(req.target, "AWSGlue.GetDatabases");
        assert_eq!(req.content_type, "application/x-amz-json-1.1");
        // SigV4 signature over the glue service in the configured region —
        // the same code path real AWS requires.
        assert!(
            req.authorization.starts_with("AWS4-HMAC-SHA256"),
            "got: {}",
            req.authorization
        );
        assert!(
            req.authorization
                .contains("/eu-central-1/glue/aws4_request"),
            "got: {}",
            req.authorization
        );
        assert!(
            req.authorization.contains("AKIDTEST"),
            "configured credentials must be used: {}",
            req.authorization
        );
    }
    assert_eq!(recorded[0].body.get("NextToken"), None);
    assert_eq!(
        recorded[1].body.get("NextToken").and_then(Value::as_str),
        Some("page-two")
    );
}

#[tokio::test]
async fn get_table_parses_the_full_table_shape() {
    let (addr, _requests) = start_mock().await;
    let glue = client_for(addr);

    let table = glue.get_table("events", "clicks").await.expect("get_table");
    assert_eq!(table.name, "clicks");
    assert_eq!(table.database_name.as_deref(), Some("events"));
    assert_eq!(table.table_type.as_deref(), Some("EXTERNAL_TABLE"));
    assert_eq!(table.partition_keys.len(), 1);
    assert_eq!(table.partition_keys[0].name, "dt");
    assert_eq!(
        table.partition_keys[0].column_type.as_deref(),
        Some("string")
    );
    let sd = table.storage_descriptor.expect("storage descriptor");
    assert_eq!(sd.location.as_deref(), Some("s3://data/events/"));
    assert_eq!(
        sd.columns
            .iter()
            .map(|c| (c.name.as_str(), c.column_type.as_deref().unwrap()))
            .collect::<Vec<_>>(),
        vec![("id", "bigint"), ("payload", "string")]
    );
    assert_eq!(
        sd.serde_info
            .expect("serde info")
            .serialization_library
            .as_deref(),
        Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
    );
}

#[tokio::test]
async fn get_partitions_passes_the_expression_through() {
    let (addr, requests) = start_mock().await;
    let glue = client_for(addr);

    let partitions = glue
        .get_partitions("events", "clicks", Some("dt >= '2026-08-01'"))
        .await
        .expect("get_partitions");
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].values, vec!["2026-08-01"]);
    assert_eq!(
        partitions[0]
            .storage_descriptor
            .as_ref()
            .and_then(|sd| sd.location.as_deref()),
        Some("s3://data/events/dt=2026-08-01/")
    );

    let recorded = requests.lock().unwrap().clone();
    assert_eq!(
        recorded[0].body.get("Expression").and_then(Value::as_str),
        Some("dt >= '2026-08-01'")
    );
    assert_eq!(
        recorded[0].body.get("TableName").and_then(Value::as_str),
        Some("clicks")
    );
}

#[tokio::test]
async fn entity_not_found_maps_to_the_dedicated_variant() {
    let (addr, _requests) = start_mock().await;
    let glue = client_for(addr);

    let err = glue
        .get_table("events", "missing")
        .await
        .expect_err("missing table must error");
    match err {
        CatalogError::GlueEntityNotFound { message } => {
            assert!(
                message.contains("Table missing not found"),
                "got: {message}"
            );
        }
        other => panic!("expected GlueEntityNotFound, got: {other}"),
    }
}

#[tokio::test]
async fn modeled_glue_errors_surface_code_and_message() {
    let (addr, _requests) = start_mock().await;
    let glue = client_for(addr);

    let err = glue
        .get_tables("events")
        .await
        .expect_err("mock rejects GetTables");
    match err {
        CatalogError::GlueApi { code, message } => {
            assert_eq!(code, "InvalidInputException");
            assert!(message.contains("DatabaseName"), "got: {message}");
        }
        other => panic!("expected GlueApi, got: {other}"),
    }
}
