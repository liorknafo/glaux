//! The offline side: glaux's Athena service over the fixture files.
//!
//! The stack is exactly what a deployed glaux runs — `GlueCatalogProvider`
//! over a `StorageBackend` holding real Parquet/NDJSON/CSV objects,
//! `TrinoEngine`, and `AthenaService` — driven through the JSON action
//! surface (`StartQueryExecution` → `GetQueryExecution` polling →
//! paginated `GetQueryResults`), so the recorded snapshot is compared with
//! what an SDK client would see.

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::prelude::SessionContext;
use glaux_athena::{AthenaService, AthenaServiceConfig, TrinoEngine};
use glaux_catalog::{GlueApi, GlueCatalogProvider, StorageBackend};
use serde_json::{Value, json};

use crate::fixtures;
use crate::memory::{MemoryStorage, StaticGlue};
use crate::snapshot::{Column, Outcome};
use crate::{DATABASE, HarnessError, Result};

/// Bucket the fixture files live in.
pub const DATA_BUCKET: &str = "glaux-fidelity-data";
/// Bucket query results are written to.
pub const RESULTS_BUCKET: &str = "glaux-fidelity-results";
/// DataFusion catalog name Athena's `AwsDataCatalog` maps to.
const CATALOG: &str = "awsdatacatalog";

/// glaux's Athena over the fixture data.
pub struct GlauxAthena {
    service: Arc<AthenaService>,
    storage: Arc<MemoryStorage>,
}

impl GlauxAthena {
    /// Materialize the fixtures into in-memory S3, register them in an
    /// in-memory Glue, and bring the service up.
    pub async fn new() -> Result<Self> {
        let storage = Arc::new(MemoryStorage::new());
        let tables = fixtures::tables()?;
        for table in &tables {
            storage
                .put_object(DATA_BUCKET, &table.key, table.bytes.clone())
                .await
                .map_err(|e| HarnessError::new(format!("seeding {}: {e}", table.key)))?;
        }
        let glue = Arc::new(StaticGlue::new(
            DATABASE,
            tables
                .iter()
                .map(|t| t.glue_table(DATABASE, DATA_BUCKET))
                .collect(),
        ));
        let catalog = GlueCatalogProvider::try_new(
            Arc::clone(&glue) as Arc<dyn GlueApi>,
            Arc::clone(&storage) as Arc<dyn StorageBackend>,
        )
        .await
        .map_err(|e| HarnessError::new(format!("building the Glue catalog provider: {e}")))?;
        let ctx = SessionContext::new();
        ctx.register_catalog(CATALOG, Arc::new(catalog));
        let service = Arc::new(AthenaService::new(
            AthenaServiceConfig {
                default_workgroup: "primary".to_string(),
                default_output_location: Some(format!("s3://{RESULTS_BUCKET}/")),
            },
            Arc::new(TrinoEngine::new(ctx, CATALOG)),
            Arc::clone(&storage) as Arc<dyn StorageBackend>,
        ));
        Ok(Self { service, storage })
    }

    /// The in-memory storage (fixtures, results).
    pub fn storage(&self) -> &Arc<MemoryStorage> {
        &self.storage
    }

    async fn call(&self, action: &str, body: Value) -> std::result::Result<Value, String> {
        self.service
            .handle(action, body.to_string().as_bytes())
            .await
            .map_err(|e| e.to_string())
    }

    /// Run `sql` in the fixture database to completion.
    pub async fn run(&self, sql: &str) -> Result<Outcome> {
        let started = match self
            .call(
                "StartQueryExecution",
                json!({
                    "QueryString": sql,
                    "QueryExecutionContext": { "Database": DATABASE },
                }),
            )
            .await
        {
            Ok(v) => v,
            // A synchronous rejection is still an explicit failure.
            Err(message) => return Ok(Outcome::Failed { message }),
        };
        let id = started["QueryExecutionId"]
            .as_str()
            .ok_or_else(|| HarnessError::new(format!("no QueryExecutionId in {started}")))?
            .to_string();
        let deadline = Instant::now() + Duration::from_secs(60);
        let execution = loop {
            let out = self
                .call("GetQueryExecution", json!({ "QueryExecutionId": id }))
                .await
                .map_err(HarnessError::new)?;
            let qe = out["QueryExecution"].clone();
            match qe["Status"]["State"].as_str() {
                Some("SUCCEEDED" | "FAILED" | "CANCELLED") => break qe,
                _ if Instant::now() > deadline => {
                    return Err(HarnessError::new(format!("{id} never finished: {qe}")));
                }
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        };
        if execution["Status"]["State"] != "SUCCEEDED" {
            let status = &execution["Status"];
            let message = status["AthenaError"]["ErrorMessage"]
                .as_str()
                .or_else(|| status["StateChangeReason"].as_str())
                .unwrap_or("query failed without a message")
                .to_string();
            return Ok(Outcome::Failed { message });
        }
        self.results(&id).await
    }

    async fn results(&self, id: &str) -> Result<Outcome> {
        let mut rows = Vec::new();
        let mut columns: Option<Vec<Column>> = None;
        let mut token: Option<String> = None;
        let mut first_page = true;
        loop {
            let mut body = json!({ "QueryExecutionId": id, "MaxResults": 1000 });
            if let Some(t) = &token {
                body["NextToken"] = Value::String(t.clone());
            }
            let out = self
                .call("GetQueryResults", body)
                .await
                .map_err(HarnessError::new)?;
            if columns.is_none() {
                columns = Some(parse_columns(
                    &out["ResultSet"]["ResultSetMetadata"]["ColumnInfo"],
                )?);
            }
            let page = out["ResultSet"]["Rows"]
                .as_array()
                .ok_or_else(|| HarnessError::new("GetQueryResults without Rows"))?;
            for (i, row) in page.iter().enumerate() {
                // Like Athena, the first row of the first page is the header.
                if first_page && i == 0 {
                    continue;
                }
                rows.push(parse_row(row)?);
            }
            first_page = false;
            token = out["NextToken"].as_str().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok(Outcome::Succeeded {
            columns: columns.unwrap_or_default(),
            rows,
        })
    }
}

/// `ColumnInfo` JSON → [`Column`]s, decimals carrying precision/scale.
pub fn parse_columns(info: &Value) -> Result<Vec<Column>> {
    info.as_array()
        .ok_or_else(|| HarnessError::new("ColumnInfo is not an array"))?
        .iter()
        .map(|c| {
            let name = c["Name"]
                .as_str()
                .ok_or_else(|| HarnessError::new("ColumnInfo without Name"))?;
            let ty = c["Type"]
                .as_str()
                .ok_or_else(|| HarnessError::new("ColumnInfo without Type"))?;
            Ok(Column {
                name: name.to_string(),
                type_name: type_text(ty, c["Precision"].as_i64(), c["Scale"].as_i64()),
            })
        })
        .collect()
}

/// Athena type text: decimals are `decimal(p,s)`, everything else the
/// bare type name.
pub fn type_text(type_name: &str, precision: Option<i64>, scale: Option<i64>) -> String {
    match type_name {
        "decimal" => format!(
            "decimal({},{})",
            precision.unwrap_or_default(),
            scale.unwrap_or_default()
        ),
        other => other.to_string(),
    }
}

fn parse_row(row: &Value) -> Result<Vec<Option<String>>> {
    Ok(row["Data"]
        .as_array()
        .ok_or_else(|| HarnessError::new("Row without Data"))?
        .iter()
        .map(|d| d["VarCharValue"].as_str().map(str::to_string))
        .collect())
}
