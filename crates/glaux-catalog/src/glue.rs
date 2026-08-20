//! Glue Data Catalog access: the [`GlueApi`] trait, the Glue data model
//! types glaux consumes, and a lightweight network client.
//!
//! The network client speaks the Glue wire protocol directly (AWS JSON 1.1:
//! `POST /` with an `X-Amz-Target: AWSGlue.<Action>` header) over `reqwest`,
//! signing requests with SigV4 via `aws-sigv4`. This deliberately avoids the
//! full `aws-sdk-glue`/smithy client stack: Glue's JSON protocol is trivial,
//! `reqwest` is already in the dependency tree via `object_store`, and the
//! only piece that genuinely needs battle-tested code — request signing for
//! real AWS — is exactly what `aws-sigv4` provides.

use std::time::SystemTime;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::GlauxConfig;
use crate::error::{CatalogError, Result};
use crate::storage::{PLACEHOLDER_ACCESS_KEY, PLACEHOLDER_SECRET_KEY};

/// A Glue database.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueDatabase {
    /// Database name.
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,
    /// Optional default storage location (`s3://...`).
    #[serde(default)]
    pub location_uri: Option<String>,
    /// Free-form parameters.
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// A column of a Glue table or partition, with its Hive type string
/// (e.g. `bigint`, `string`, `array<struct<x:int>>`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueColumn {
    /// Column name.
    pub name: String,
    /// Hive type string. Glue models this as optional; a missing type is
    /// surfaced as `None` so downstream mapping can error explicitly rather
    /// than guess.
    #[serde(rename = "Type", default)]
    pub column_type: Option<String>,
    /// Optional comment.
    #[serde(default)]
    pub comment: Option<String>,
}

/// SerDe information from a storage descriptor.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct GlueSerDeInfo {
    /// SerDe name.
    #[serde(default)]
    pub name: Option<String>,
    /// Fully-qualified SerDe class,
    /// e.g. `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`.
    #[serde(default)]
    pub serialization_library: Option<String>,
    /// SerDe parameters (delimiters, JSON mappings, ...).
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// Physical storage description of a table or partition.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct GlueStorageDescriptor {
    /// Data columns (partition keys are separate, on the table).
    #[serde(default)]
    pub columns: Vec<GlueColumn>,
    /// Data location (`s3://bucket/prefix/`).
    #[serde(default)]
    pub location: Option<String>,
    /// Hadoop input format class.
    #[serde(default)]
    pub input_format: Option<String>,
    /// Hadoop output format class.
    #[serde(default)]
    pub output_format: Option<String>,
    /// SerDe configuration.
    #[serde(default)]
    pub serde_info: Option<GlueSerDeInfo>,
    /// Whether the data is compressed.
    #[serde(default)]
    pub compressed: bool,
    /// Free-form parameters.
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// A Glue table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueTable {
    /// Table name.
    pub name: String,
    /// Owning database (present in `GetTable(s)` responses).
    #[serde(default)]
    pub database_name: Option<String>,
    /// Table type, e.g. `EXTERNAL_TABLE`.
    #[serde(default)]
    pub table_type: Option<String>,
    /// Physical storage description.
    #[serde(default)]
    pub storage_descriptor: Option<GlueStorageDescriptor>,
    /// Partition key columns, in partition order.
    #[serde(default)]
    pub partition_keys: Vec<GlueColumn>,
    /// Table parameters (classification, partition projection config, ...).
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// A Glue partition of a table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GluePartition {
    /// Partition values, in `partition_keys` order.
    pub values: Vec<String>,
    /// Physical storage description of this partition.
    #[serde(default)]
    pub storage_descriptor: Option<GlueStorageDescriptor>,
    /// Free-form parameters.
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// Read-side Glue Data Catalog operations used by the glaux engines.
///
/// Implementations must be complete or fail loudly: a database or table
/// that cannot be fetched is an error, never an empty result.
#[async_trait]
pub trait GlueApi: Send + Sync {
    /// List all databases (`GetDatabases`, fully paginated).
    async fn get_databases(&self) -> Result<Vec<GlueDatabase>>;

    /// List all tables of a database (`GetTables`, fully paginated).
    async fn get_tables(&self, database: &str) -> Result<Vec<GlueTable>>;

    /// Fetch one table (`GetTable`). A missing table yields
    /// [`CatalogError::GlueEntityNotFound`].
    async fn get_table(&self, database: &str, table: &str) -> Result<GlueTable>;

    /// List the partitions of a table (`GetPartitions`, fully paginated),
    /// optionally server-side filtered with a Glue partition `Expression`.
    async fn get_partitions(
        &self,
        database: &str,
        table: &str,
        expression: Option<&str>,
    ) -> Result<Vec<GluePartition>>;
}

/// Network [`GlueApi`] client for any Glue-compatible endpoint (fakecloud,
/// real AWS, ...). Built from [`GlauxConfig`]: `glue_endpoint` overrides the
/// default `https://glue.<region>.amazonaws.com`.
#[derive(Debug)]
pub struct NetworkGlueApi {
    endpoint: String,
    region: String,
    credentials: Credentials,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct GlueErrorBody {
    #[serde(rename = "__type", default)]
    error_type: Option<String>,
    #[serde(rename = "message", alias = "Message", default)]
    message: Option<String>,
}

impl NetworkGlueApi {
    /// Build a client from the unified configuration.
    pub fn new(config: &GlauxConfig) -> Self {
        let endpoint = config
            .glue_endpoint
            .clone()
            .unwrap_or_else(|| format!("https://glue.{}.amazonaws.com", config.region));
        let credentials = match &config.credentials {
            Some(c) => Credentials::new(
                c.access_key_id.clone(),
                c.secret_access_key.clone(),
                c.session_token.clone(),
                None,
                "glaux-config",
            ),
            None => match (
                std::env::var("AWS_ACCESS_KEY_ID"),
                std::env::var("AWS_SECRET_ACCESS_KEY"),
            ) {
                // Ambient AWS environment credentials pass through.
                (Ok(ak), Ok(sk)) => Credentials::new(
                    ak,
                    sk,
                    std::env::var("AWS_SESSION_TOKEN").ok(),
                    None,
                    "glaux-env",
                ),
                // No credentials anywhere: sign with a placeholder identity.
                // Emulators ignore the signature (some route on the
                // credential scope); real AWS rejects it explicitly with an
                // authentication error — never a silent wrong answer.
                _ => Credentials::new(
                    PLACEHOLDER_ACCESS_KEY,
                    PLACEHOLDER_SECRET_KEY,
                    None,
                    None,
                    "glaux-placeholder",
                ),
            },
        };
        Self {
            endpoint,
            region: config.region.clone(),
            credentials,
            client: reqwest::Client::new(),
        }
    }

    /// Invoke a raw Glue action (`X-Amz-Target: AWSGlue.<action>`) with a
    /// JSON payload and return the parsed JSON response.
    ///
    /// The typed [`GlueApi`] methods are built on this; it is public as an
    /// escape hatch for operations outside the v0.1 read surface (tests use
    /// it to provision `CreateDatabase`/`CreateTable` fixtures).
    pub async fn invoke(&self, action: &'static str, payload: Value) -> Result<Value> {
        let body = serde_json::to_vec(&payload).expect("serde_json::Value always serializes");
        let target = format!("AWSGlue.{action}");

        let mut request = http::Request::builder()
            .method(http::Method::POST)
            .uri(&self.endpoint)
            .header("content-type", "application/x-amz-json-1.1")
            .header("x-amz-target", &target)
            .body(body)
            .map_err(|e| CatalogError::Signing {
                service: "glue",
                message: format!("failed to build request: {e}"),
            })?;

        self.sign_request(&mut request)?;

        let reqwest_request =
            reqwest::Request::try_from(request).map_err(|source| CatalogError::GlueTransport {
                action,
                endpoint: self.endpoint.clone(),
                source,
            })?;
        let response = self
            .client
            .execute(reqwest_request)
            .await
            .map_err(|source| CatalogError::GlueTransport {
                action,
                endpoint: self.endpoint.clone(),
                source,
            })?;

        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|source| CatalogError::GlueTransport {
                action,
                endpoint: self.endpoint.clone(),
                source,
            })?;

        if !status.is_success() {
            return Err(Self::error_from_response(status, &bytes));
        }

        serde_json::from_slice(&bytes).map_err(|e| CatalogError::GlueResponseParse {
            action,
            message: format!("invalid JSON in {status} response: {e}"),
        })
    }

    fn sign_request(&self, request: &mut http::Request<Vec<u8>>) -> Result<()> {
        let identity: Identity = self.credentials.clone().into();
        let settings = SigningSettings::default();
        let params: aws_sigv4::http_request::SigningParams<'_> = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("glue")
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .map_err(|e| CatalogError::Signing {
                service: "glue",
                message: e.to_string(),
            })?
            .into();

        let signable = SignableRequest::new(
            request.method().as_str(),
            request.uri().to_string(),
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.to_str().unwrap_or_default())),
            SignableBody::Bytes(request.body()),
        )
        .map_err(|e| CatalogError::Signing {
            service: "glue",
            message: e.to_string(),
        })?;

        let (instructions, _signature) = sign(signable, &params)
            .map_err(|e| CatalogError::Signing {
                service: "glue",
                message: e.to_string(),
            })?
            .into_parts();
        instructions.apply_to_request_http1x(request);
        Ok(())
    }

    fn error_from_response(status: http::StatusCode, bytes: &[u8]) -> CatalogError {
        let parsed: Option<GlueErrorBody> = serde_json::from_slice(bytes).ok();
        let (code, message) = match parsed {
            Some(body) => {
                // The type may be namespaced: `com.amazonaws.glue#Exception`.
                let code = body
                    .error_type
                    .map(|t| t.rsplit('#').next().unwrap_or_default().to_string())
                    .filter(|t| !t.is_empty())
                    .unwrap_or_else(|| format!("HTTP {status}"));
                let message = body
                    .message
                    .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned());
                (code, message)
            }
            None => (
                format!("HTTP {status}"),
                String::from_utf8_lossy(bytes).into_owned(),
            ),
        };
        if code == "EntityNotFoundException" {
            CatalogError::GlueEntityNotFound { message }
        } else {
            CatalogError::GlueApi { code, message }
        }
    }

    /// Invoke a paginated Glue list action, following `NextToken` until the
    /// listing is complete, and collect the items under `list_key`.
    async fn invoke_paginated<T: serde::de::DeserializeOwned>(
        &self,
        action: &'static str,
        base_payload: Value,
        list_key: &str,
    ) -> Result<Vec<T>> {
        let mut items = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut payload = base_payload.clone();
            if let Some(token) = &next_token {
                payload
                    .as_object_mut()
                    .expect("glue payloads are JSON objects")
                    .insert("NextToken".to_string(), Value::String(token.clone()));
            }
            let mut response = self.invoke(action, payload).await?;
            let page = response
                .get_mut(list_key)
                .map(Value::take)
                .unwrap_or(Value::Array(Vec::new()));
            let page: Vec<T> =
                serde_json::from_value(page).map_err(|e| CatalogError::GlueResponseParse {
                    action,
                    message: format!("invalid {list_key} entry: {e}"),
                })?;
            items.extend(page);
            next_token = response
                .get("NextToken")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string);
            if next_token.is_none() {
                return Ok(items);
            }
        }
    }
}

#[async_trait]
impl GlueApi for NetworkGlueApi {
    async fn get_databases(&self) -> Result<Vec<GlueDatabase>> {
        self.invoke_paginated("GetDatabases", json!({}), "DatabaseList")
            .await
    }

    async fn get_tables(&self, database: &str) -> Result<Vec<GlueTable>> {
        self.invoke_paginated(
            "GetTables",
            json!({ "DatabaseName": database }),
            "TableList",
        )
        .await
    }

    async fn get_table(&self, database: &str, table: &str) -> Result<GlueTable> {
        let mut response = self
            .invoke(
                "GetTable",
                json!({ "DatabaseName": database, "Name": table }),
            )
            .await?;
        let table_value = response.get_mut("Table").map(Value::take).ok_or_else(|| {
            CatalogError::GlueResponseParse {
                action: "GetTable",
                message: "response is missing the Table field".to_string(),
            }
        })?;
        serde_json::from_value(table_value).map_err(|e| CatalogError::GlueResponseParse {
            action: "GetTable",
            message: format!("invalid Table entry: {e}"),
        })
    }

    async fn get_partitions(
        &self,
        database: &str,
        table: &str,
        expression: Option<&str>,
    ) -> Result<Vec<GluePartition>> {
        let mut payload = json!({ "DatabaseName": database, "TableName": table });
        if let Some(expr) = expression {
            payload
                .as_object_mut()
                .expect("payload is a JSON object")
                .insert("Expression".to_string(), Value::String(expr.to_string()));
        }
        self.invoke_paginated("GetPartitions", payload, "Partitions")
            .await
    }
}
