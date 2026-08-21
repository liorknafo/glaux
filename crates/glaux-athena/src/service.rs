//! The Athena service: query lifecycle state machine, workgroups, and the
//! action dispatcher.
//!
//! [`AthenaService::handle`] is the transport-independent entry point
//! (action name + JSON body → JSON body or [`AthenaError`]); the axum layer
//! in [`crate::http`] and fakecloud's `AwsService` adapter both sit on top
//! of it.
//!
//! # Lifecycle
//!
//! `StartQueryExecution` records the query as `QUEUED` and spawns a tokio
//! task that moves it to `RUNNING`, drives the [`QueryEngine`], encodes the
//! results, writes `<id>.csv` and `<id>.csv.metadata` to the
//! `OutputLocation`, and lands on `SUCCEEDED` or `FAILED`.
//! `StopQueryExecution` aborts that task (which drops the DataFusion stream
//! mid-flight) and marks the query `CANCELLED`. Terminal states are sticky:
//! a task that completes concurrently with a cancellation does not
//! resurrect the query.
//!
//! # Result objects
//!
//! A query that does not reach `SUCCEEDED` leaves no result objects behind:
//! the two writes run under a guard that deletes whatever was written if
//! either write fails or the task is aborted between them. Like real Athena,
//! `QueryExecution.ResultConfiguration.OutputLocation` reports the full
//! path of the CSV (`s3://bucket/prefix/<id>.csv`), not the location the
//! client asked for.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use glaux_catalog::StorageBackend;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::task::AbortHandle;

use crate::engine::{EngineError, QueryEngine, QueryRequest};
use crate::error::AthenaError;
use crate::metadata::encode_metadata;
use crate::model::*;
use crate::results::{EncodedResultSet, ResultError, encode_result_set};

/// Engine version glaux advertises. Athena engine version 3 is Trino-based,
/// which is the dialect glaux targets.
pub const ENGINE_VERSION: &str = "Athena engine version 3";

const DEFAULT_RESULTS_PAGE: usize = 1000;
const MAX_RESULTS_PAGE: usize = 1000;
const DEFAULT_LIST_PAGE: usize = 50;
const MAX_LIST_PAGE: usize = 50;
const MAX_BATCH_GET: usize = 50;

/// Service-level settings.
#[derive(Debug, Clone)]
pub struct AthenaServiceConfig {
    /// Default workgroup name (`primary` on real Athena).
    pub default_workgroup: String,
    /// `OutputLocation` the default workgroup starts with.
    pub default_output_location: Option<String>,
}

impl Default for AthenaServiceConfig {
    fn default() -> Self {
        Self {
            default_workgroup: "primary".to_string(),
            default_output_location: None,
        }
    }
}

impl From<&glaux_catalog::AthenaConfig> for AthenaServiceConfig {
    fn from(config: &glaux_catalog::AthenaConfig) -> Self {
        Self {
            default_workgroup: config.workgroup.clone(),
            default_output_location: config.output_location.clone(),
        }
    }
}

/// A successful engine run, ready to be recorded: the encoded results plus
/// `(engine time in millis, bytes scanned)`.
struct Completed {
    results: Arc<EncodedResultSet>,
    engine_millis: u64,
    scanned_bytes: Option<u64>,
}

/// Per-query record: the wire-visible execution plus the pieces the
/// service needs to page results and cancel work.
struct QueryRecord {
    execution: QueryExecution,
    submitted_at: Instant,
    started_at: Option<Instant>,
    results: Option<Arc<EncodedResultSet>>,
    abort: Option<AbortHandle>,
    client_request_token: Option<String>,
}

#[derive(Default)]
struct State {
    queries: HashMap<String, QueryRecord>,
    /// Submission order, oldest first.
    order: Vec<String>,
    tokens: HashMap<String, String>,
    workgroups: HashMap<String, WorkGroup>,
}

/// The Athena service. Cheap to share behind an `Arc`.
pub struct AthenaService {
    config: AthenaServiceConfig,
    engine: Arc<dyn QueryEngine>,
    storage: Arc<dyn StorageBackend>,
    state: Mutex<State>,
}

impl std::fmt::Debug for AthenaService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AthenaService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn now_epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn engine_version() -> EngineVersion {
    EngineVersion {
        selected_engine_version: "AUTO".to_string(),
        effective_engine_version: ENGINE_VERSION.to_string(),
    }
}

/// Athena's `StatementType` / `SubstatementType` classification from the
/// leading keyword.
fn classify(sql: &str) -> (&'static str, Option<&'static str>) {
    let first = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" => ("DML", Some("SELECT")),
        "INSERT" => ("DML", Some("INSERT")),
        "UPDATE" => ("DML", Some("UPDATE")),
        "DELETE" => ("DML", Some("DELETE")),
        "MERGE" => ("DML", Some("MERGE")),
        "UNLOAD" => ("DML", Some("UNLOAD")),
        "CREATE" | "ALTER" | "DROP" | "MSCK" => ("DDL", None),
        "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "USE" | "PREPARE" | "EXECUTE" | "DEALLOCATE" => {
            ("UTILITY", None)
        }
        _ => ("DML", None),
    }
}

/// Split `s3://bucket/prefix` into `(bucket, prefix)`; the prefix always
/// ends in `/` unless empty, so result keys are `prefix + id + ".csv"`.
fn parse_output_location(location: &str) -> Result<(String, String), AthenaError> {
    let rest = location.strip_prefix("s3://").ok_or_else(|| {
        AthenaError::invalid_request(format!(
            "The OutputLocation {location:?} is not a valid S3 path: it must start with s3://"
        ))
    })?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(AthenaError::invalid_request(format!(
            "The OutputLocation {location:?} is not a valid S3 path: the bucket name is empty"
        )));
    }
    let mut prefix = prefix.trim_start_matches('/').to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    Ok((bucket.to_string(), prefix))
}

fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, AthenaError> {
    let body = if body.is_empty() { b"{}" } else { body };
    serde_json::from_slice(body).map_err(|e| AthenaError::Serialization {
        message: format!("request body is not valid JSON for this action: {e}"),
    })
}

fn page_size(
    requested: Option<i64>,
    default: usize,
    max: usize,
    field: &str,
) -> Result<usize, AthenaError> {
    match requested {
        None => Ok(default),
        Some(n) if n >= 1 && (n as usize) <= max => Ok(n as usize),
        Some(n) => Err(AthenaError::invalid_request(format!(
            "{field} must be between 1 and {max}, got {n}"
        ))),
    }
}

fn parse_offset_token(token: Option<&str>) -> Result<usize, AthenaError> {
    match token {
        None => Ok(0),
        Some(t) => t.parse().map_err(|_| {
            AthenaError::invalid_request_with_code(
                format!("NextToken {t:?} is not a token this service issued"),
                "INVALID_NEXT_TOKEN",
            )
        }),
    }
}

fn not_found(id: &str) -> AthenaError {
    AthenaError::invalid_request(format!("QueryExecution {id} was not found"))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, AthenaError> {
    serde_json::to_value(value)
        .map_err(|e| AthenaError::internal(format!("failed to serialize response: {e}")))
}

impl AthenaService {
    /// Build a service over `engine`, writing result CSVs through `storage`.
    pub fn new(
        config: AthenaServiceConfig,
        engine: Arc<dyn QueryEngine>,
        storage: Arc<dyn StorageBackend>,
    ) -> Self {
        let mut state = State::default();
        state.workgroups.insert(
            config.default_workgroup.clone(),
            WorkGroup {
                name: config.default_workgroup.clone(),
                state: "ENABLED".to_string(),
                configuration: WorkGroupConfiguration {
                    result_configuration: Some(ResultConfiguration {
                        output_location: config.default_output_location.clone(),
                        encryption_configuration: None,
                    }),
                    enforce_work_group_configuration: Some(false),
                    publish_cloud_watch_metrics_enabled: Some(false),
                    engine_version: Some(engine_version()),
                },
                description: None,
                creation_time: now_epoch_seconds(),
            },
        );
        Self {
            config,
            engine,
            storage,
            state: Mutex::new(state),
        }
    }

    /// Every `X-Amz-Target` action this service answers.
    pub const SUPPORTED_ACTIONS: &'static [&'static str] = &[
        "StartQueryExecution",
        "GetQueryExecution",
        "GetQueryResults",
        "StopQueryExecution",
        "ListQueryExecutions",
        "BatchGetQueryExecution",
        "CreateWorkGroup",
        "GetWorkGroup",
        "ListWorkGroups",
    ];

    /// Dispatch one action. `action` is the part after `AmazonAthena.` in
    /// `X-Amz-Target`; `body` is the raw JSON request body.
    pub async fn handle(self: &Arc<Self>, action: &str, body: &[u8]) -> Result<Value, AthenaError> {
        match action {
            "StartQueryExecution" => to_value(self.start_query_execution(parse_body(body)?)?),
            "GetQueryExecution" => to_value(self.get_query_execution(parse_body(body)?)?),
            "GetQueryResults" => to_value(self.get_query_results(parse_body(body)?)?),
            "StopQueryExecution" => {
                self.stop_query_execution(parse_body(body)?)?;
                Ok(Value::Object(Default::default()))
            }
            "ListQueryExecutions" => to_value(self.list_query_executions(parse_body(body)?)?),
            "BatchGetQueryExecution" => {
                to_value(self.batch_get_query_execution(parse_body(body)?)?)
            }
            "CreateWorkGroup" => {
                self.create_work_group(parse_body(body)?)?;
                Ok(Value::Object(Default::default()))
            }
            "GetWorkGroup" => to_value(self.get_work_group(parse_body(body)?)?),
            "ListWorkGroups" => to_value(self.list_work_groups(parse_body(body)?)?),
            other => Err(AthenaError::UnknownOperation {
                action: other.to_string(),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // StartQueryExecution + the background lifecycle
    // -----------------------------------------------------------------------

    /// `StartQueryExecution`.
    pub fn start_query_execution(
        self: &Arc<Self>,
        input: StartQueryExecutionInput,
    ) -> Result<StartQueryExecutionOutput, AthenaError> {
        let sql = input
            .query_string
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| AthenaError::invalid_request("QueryString must not be empty"))?;
        if let Some(params) = &input.execution_parameters
            && !params.is_empty()
        {
            return Err(AthenaError::invalid_request(
                "ExecutionParameters (parameterized queries) are not supported by glaux v0.1; \
                 inline the values into QueryString",
            ));
        }

        let mut state = self.state.lock().expect("athena state poisoned");

        let workgroup_name = input
            .work_group
            .clone()
            .unwrap_or_else(|| self.config.default_workgroup.clone());
        let workgroup = state.workgroups.get(&workgroup_name).ok_or_else(|| {
            AthenaError::invalid_request(format!("WorkGroup {workgroup_name} is not found."))
        })?;
        let wg_result = workgroup
            .configuration
            .result_configuration
            .clone()
            .unwrap_or_default();
        let enforce = workgroup
            .configuration
            .enforce_work_group_configuration
            .unwrap_or(false);

        let requested = input.result_configuration.clone().unwrap_or_default();
        let output_location = if enforce {
            wg_result.output_location.clone()
        } else {
            requested
                .output_location
                .clone()
                .or(wg_result.output_location.clone())
        };
        let output_location = output_location.ok_or_else(|| {
            AthenaError::invalid_request(
                "No output location provided. An output location is required either through \
                 the Workgroup result configuration setting or as an API input.",
            )
        })?;
        let (bucket, prefix) = parse_output_location(&output_location)?;

        // Idempotency: the same token replays the same id; a different
        // query under a reused token is a client bug Athena rejects.
        if let Some(token) = &input.client_request_token
            && let Some(existing_id) = state.tokens.get(token)
        {
            let existing = &state.queries[existing_id];
            if existing.execution.query == sql {
                return Ok(StartQueryExecutionOutput {
                    query_execution_id: existing_id.clone(),
                });
            }
            return Err(AthenaError::invalid_request(
                "Idempotent parameters do not match: the ClientRequestToken was already used \
                 for a different QueryString",
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let context = input.query_execution_context.unwrap_or_default();
        let (statement_type, substatement) = classify(&sql);
        let mut execution = QueryExecution {
            query_execution_id: id.clone(),
            query: sql.clone(),
            statement_type: statement_type.to_string(),
            result_configuration: ResultConfiguration {
                output_location: Some(output_location),
                encryption_configuration: requested.encryption_configuration,
            },
            query_execution_context: context.clone(),
            status: QueryExecutionStatus {
                state: QueryState::Queued,
                state_change_reason: None,
                submission_date_time: now_epoch_seconds(),
                completion_date_time: None,
                athena_error: None,
            },
            statistics: QueryExecutionStatistics::default(),
            work_group: workgroup_name,
            engine_version: engine_version(),
            substatement_type: substatement.map(str::to_string),
        };

        let request = QueryRequest {
            sql,
            catalog: context.catalog,
            database: context.database,
        };
        let service = Arc::clone(self);
        let task_id = id.clone();
        let csv_key = format!("{prefix}{id}.csv");
        execution.result_configuration.output_location = Some(format!("s3://{bucket}/{csv_key}"));
        let handle = tokio::spawn(async move {
            service.run_query(task_id, request, bucket, csv_key).await;
        });

        state.queries.insert(
            id.clone(),
            QueryRecord {
                execution,
                submitted_at: Instant::now(),
                started_at: None,
                results: None,
                abort: Some(handle.abort_handle()),
                client_request_token: input.client_request_token.clone(),
            },
        );
        state.order.push(id.clone());
        if let Some(token) = input.client_request_token {
            state.tokens.insert(token, id.clone());
        }
        Ok(StartQueryExecutionOutput {
            query_execution_id: id,
        })
    }

    /// The background task body: `QUEUED → RUNNING → SUCCEEDED | FAILED`.
    /// `csv_key` is the result CSV's key in `bucket`; the metadata file
    /// sits next to it as `<csv_key>.metadata`.
    async fn run_query(
        self: Arc<Self>,
        id: String,
        request: QueryRequest,
        bucket: String,
        csv_key: String,
    ) {
        if !self.mark_running(&id) {
            return;
        }
        let engine_result = self.engine.execute(request).await;
        let processing_started = Instant::now();
        let outcome = match engine_result {
            Ok(output) => {
                let (engine_millis, scanned_bytes) =
                    (output.engine_time_millis, output.data_scanned_bytes);
                match encode_result_set(&output.schema, &output.batches) {
                    Ok(encoded) => {
                        self.write_results(&bucket, &csv_key, &encoded)
                            .await
                            .map(|()| Completed {
                                results: Arc::new(encoded),
                                engine_millis,
                                scanned_bytes,
                            })
                    }
                    Err(e) => Err(Failure::Encode(e)),
                }
            }
            Err(e) => Err(Failure::Engine(e)),
        };
        self.finish(&id, outcome, processing_started.elapsed());
    }

    /// Write `<id>.csv.metadata` then `<id>.csv`. Metadata goes first so a
    /// reader that sees the CSV can rely on its companion existing. Both
    /// objects are removed again if either write fails or this future is
    /// dropped (cancellation) part-way through.
    async fn write_results(
        &self,
        bucket: &str,
        csv_key: &str,
        encoded: &EncodedResultSet,
    ) -> Result<(), Failure> {
        let metadata_key = format!("{csv_key}.metadata");
        let mut cleanup = ResultCleanup {
            storage: Arc::clone(&self.storage),
            bucket: bucket.to_string(),
            keys: vec![metadata_key.clone(), csv_key.to_string()],
            armed: true,
        };
        let write_error = |key: &str, e: glaux_catalog::CatalogError| {
            Failure::ResultWrite(format!(
                "failed to write query results to s3://{bucket}/{key}: {e}"
            ))
        };
        self.storage
            .put_object(
                bucket,
                &metadata_key,
                Bytes::from(encode_metadata(&encoded.columns)),
            )
            .await
            .map_err(|e| write_error(&metadata_key, e))?;
        self.storage
            .put_object(bucket, csv_key, Bytes::from(encoded.to_csv()))
            .await
            .map_err(|e| write_error(csv_key, e))?;
        cleanup.armed = false;
        Ok(())
    }

    /// Move `id` to `RUNNING`; `false` if it was cancelled while queued.
    fn mark_running(&self, id: &str) -> bool {
        let mut state = self.state.lock().expect("athena state poisoned");
        let Some(record) = state.queries.get_mut(id) else {
            return false;
        };
        if record.execution.status.state != QueryState::Queued {
            return false;
        }
        record.execution.status.state = QueryState::Running;
        record.started_at = Some(Instant::now());
        record.execution.statistics.query_queue_time_in_millis =
            Some(millis(record.submitted_at.elapsed()));
        true
    }

    fn finish(&self, id: &str, outcome: Result<Completed, Failure>, processing: Duration) {
        let mut state = self.state.lock().expect("athena state poisoned");
        let Some(record) = state.queries.get_mut(id) else {
            return;
        };
        if record.execution.status.state != QueryState::Running {
            // Cancelled concurrently; terminal states are sticky.
            return;
        }
        record.abort = None;
        let status = &mut record.execution.status;
        status.completion_date_time = Some(now_epoch_seconds());
        let stats = &mut record.execution.statistics;
        stats.total_execution_time_in_millis = Some(millis(record.submitted_at.elapsed()));
        stats.service_processing_time_in_millis = Some(millis(processing));
        match outcome {
            Ok(completed) => {
                stats.engine_execution_time_in_millis = Some(completed.engine_millis as i64);
                stats.data_scanned_in_bytes = Some(completed.scanned_bytes.unwrap_or(0) as i64);
                status.state = QueryState::Succeeded;
                record.results = Some(completed.results);
            }
            Err(failure) => {
                let (category, error_type, message) = failure.details();
                stats.engine_execution_time_in_millis = record
                    .started_at
                    .map(|s| millis(s.elapsed().saturating_sub(processing)));
                stats.data_scanned_in_bytes = Some(0);
                status.state = QueryState::Failed;
                status.state_change_reason = Some(message.clone());
                status.athena_error = Some(AthenaErrorDetails {
                    error_category: category,
                    error_type,
                    retryable: false,
                    error_message: message,
                });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Read-side actions
    // -----------------------------------------------------------------------

    fn require_id(id: Option<String>) -> Result<String, AthenaError> {
        id.filter(|s| !s.is_empty())
            .ok_or_else(|| AthenaError::invalid_request("QueryExecutionId must not be empty"))
    }

    /// `GetQueryExecution`.
    pub fn get_query_execution(
        &self,
        input: QueryExecutionIdInput,
    ) -> Result<GetQueryExecutionOutput, AthenaError> {
        let id = Self::require_id(input.query_execution_id)?;
        let state = self.state.lock().expect("athena state poisoned");
        let record = state.queries.get(&id).ok_or_else(|| not_found(&id))?;
        Ok(GetQueryExecutionOutput {
            query_execution: record.execution.clone(),
        })
    }

    /// `BatchGetQueryExecution`.
    pub fn batch_get_query_execution(
        &self,
        input: BatchGetQueryExecutionInput,
    ) -> Result<BatchGetQueryExecutionOutput, AthenaError> {
        let ids = input.query_execution_ids.unwrap_or_default();
        if ids.is_empty() || ids.len() > MAX_BATCH_GET {
            return Err(AthenaError::invalid_request(format!(
                "QueryExecutionIds must contain between 1 and {MAX_BATCH_GET} ids, got {}",
                ids.len()
            )));
        }
        let state = self.state.lock().expect("athena state poisoned");
        let mut out = BatchGetQueryExecutionOutput {
            query_executions: Vec::new(),
            unprocessed_query_execution_ids: Vec::new(),
        };
        for id in ids {
            match state.queries.get(&id) {
                Some(record) => out.query_executions.push(record.execution.clone()),
                None => out
                    .unprocessed_query_execution_ids
                    .push(UnprocessedQueryExecutionId {
                        error_code: "InvalidRequestException".to_string(),
                        error_message: format!("QueryExecution {id} was not found"),
                        query_execution_id: id,
                    }),
            }
        }
        Ok(out)
    }

    /// `ListQueryExecutions` — ids newest first.
    pub fn list_query_executions(
        &self,
        input: ListQueryExecutionsInput,
    ) -> Result<ListQueryExecutionsOutput, AthenaError> {
        let page = page_size(
            input.max_results,
            DEFAULT_LIST_PAGE,
            MAX_LIST_PAGE,
            "MaxResults",
        )?;
        let offset = parse_offset_token(input.next_token.as_deref())?;
        let state = self.state.lock().expect("athena state poisoned");
        if let Some(wg) = &input.work_group
            && !state.workgroups.contains_key(wg)
        {
            return Err(AthenaError::invalid_request(format!(
                "WorkGroup {wg} is not found."
            )));
        }
        let all: Vec<&String> = state
            .order
            .iter()
            .rev()
            .filter(|id| {
                input
                    .work_group
                    .as_ref()
                    .is_none_or(|wg| &state.queries[*id].execution.work_group == wg)
            })
            .collect();
        let end = (offset + page).min(all.len());
        let ids = all
            .get(offset.min(all.len())..end)
            .unwrap_or_default()
            .iter()
            .map(|s| (*s).clone())
            .collect();
        Ok(ListQueryExecutionsOutput {
            query_execution_ids: ids,
            next_token: (end < all.len()).then(|| end.to_string()),
        })
    }

    /// `GetQueryResults`.
    pub fn get_query_results(
        &self,
        input: GetQueryResultsInput,
    ) -> Result<GetQueryResultsOutput, AthenaError> {
        let id = Self::require_id(input.query_execution_id)?;
        let page = page_size(
            input.max_results,
            DEFAULT_RESULTS_PAGE,
            MAX_RESULTS_PAGE,
            "MaxResults",
        )?;
        let offset = parse_offset_token(input.next_token.as_deref())?;
        let state = self.state.lock().expect("athena state poisoned");
        let record = state.queries.get(&id).ok_or_else(|| not_found(&id))?;
        let results = match record.execution.status.state {
            QueryState::Succeeded => record
                .results
                .as_ref()
                .ok_or_else(|| AthenaError::internal("SUCCEEDED query has no result set"))?,
            state @ (QueryState::Queued | QueryState::Running) => {
                return Err(AthenaError::invalid_request(format!(
                    "Query has not yet finished. Current state: {}",
                    state.as_str()
                )));
            }
            state @ (QueryState::Failed | QueryState::Cancelled) => {
                return Err(AthenaError::invalid_request(format!(
                    "Query did not finish successfully. Final query state: {}",
                    state.as_str()
                )));
            }
        };
        if offset > results.rows.len() {
            return Err(AthenaError::invalid_request_with_code(
                format!("NextToken {offset} is past the end of the result set"),
                "INVALID_NEXT_TOKEN",
            ));
        }
        let end = (offset + page).min(results.rows.len());
        Ok(GetQueryResultsOutput {
            update_count: None,
            result_set: ResultSet {
                rows: results.rows_for_page(offset, end),
                result_set_metadata: ResultSetMetadata {
                    column_info: results.columns.clone(),
                },
            },
            next_token: (end < results.rows.len()).then(|| end.to_string()),
        })
    }

    /// `StopQueryExecution`. Idempotent: stopping a finished query is a
    /// no-op success, exactly like Athena.
    pub fn stop_query_execution(&self, input: QueryExecutionIdInput) -> Result<(), AthenaError> {
        let id = Self::require_id(input.query_execution_id)?;
        let mut state = self.state.lock().expect("athena state poisoned");
        let record = state.queries.get_mut(&id).ok_or_else(|| not_found(&id))?;
        if record.execution.status.state.is_terminal() {
            return Ok(());
        }
        if let Some(abort) = record.abort.take() {
            abort.abort();
        }
        let status = &mut record.execution.status;
        status.state = QueryState::Cancelled;
        status.state_change_reason = Some("Query was cancelled by user".to_string());
        status.completion_date_time = Some(now_epoch_seconds());
        let stats = &mut record.execution.statistics;
        stats.total_execution_time_in_millis = Some(millis(record.submitted_at.elapsed()));
        stats.engine_execution_time_in_millis = record.started_at.map(|s| millis(s.elapsed()));
        stats.data_scanned_in_bytes = Some(0);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Workgroups
    // -----------------------------------------------------------------------

    /// `CreateWorkGroup`.
    pub fn create_work_group(&self, input: CreateWorkGroupInput) -> Result<(), AthenaError> {
        let name = input
            .name
            .filter(|n| !n.is_empty())
            .ok_or_else(|| AthenaError::invalid_request("Name must not be empty"))?;
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            || name.len() > 128
        {
            return Err(AthenaError::invalid_request(format!(
                "WorkGroup name {name:?} is invalid: use 1-128 characters from [a-zA-Z0-9._-]"
            )));
        }
        let mut state = self.state.lock().expect("athena state poisoned");
        if state.workgroups.contains_key(&name) {
            return Err(AthenaError::invalid_request(format!(
                "WorkGroup {name} is already created"
            )));
        }
        let mut configuration = input.configuration.unwrap_or_default();
        if let Some(rc) = &configuration.result_configuration
            && let Some(loc) = &rc.output_location
        {
            parse_output_location(loc)?;
        }
        configuration
            .engine_version
            .get_or_insert_with(engine_version);
        state.workgroups.insert(
            name.clone(),
            WorkGroup {
                name,
                state: "ENABLED".to_string(),
                configuration,
                description: input.description,
                creation_time: now_epoch_seconds(),
            },
        );
        Ok(())
    }

    /// `GetWorkGroup`.
    pub fn get_work_group(
        &self,
        input: GetWorkGroupInput,
    ) -> Result<GetWorkGroupOutput, AthenaError> {
        let name = input
            .work_group
            .filter(|n| !n.is_empty())
            .ok_or_else(|| AthenaError::invalid_request("WorkGroup must not be empty"))?;
        let state = self.state.lock().expect("athena state poisoned");
        // Athena models only InvalidRequestException for a missing
        // workgroup here (ResourceNotFoundException is reserved for
        // capacity reservations and similar resources).
        let wg = state.workgroups.get(&name).ok_or_else(|| {
            AthenaError::invalid_request(format!("WorkGroup {name} is not found."))
        })?;
        Ok(GetWorkGroupOutput {
            work_group: wg.clone(),
        })
    }

    /// `ListWorkGroups` — sorted by name.
    pub fn list_work_groups(
        &self,
        input: ListWorkGroupsInput,
    ) -> Result<ListWorkGroupsOutput, AthenaError> {
        let page = page_size(
            input.max_results,
            DEFAULT_LIST_PAGE,
            MAX_LIST_PAGE,
            "MaxResults",
        )?;
        let offset = parse_offset_token(input.next_token.as_deref())?;
        let state = self.state.lock().expect("athena state poisoned");
        let mut all: Vec<&WorkGroup> = state.workgroups.values().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let end = (offset + page).min(all.len());
        let work_groups = all
            .get(offset.min(all.len())..end)
            .unwrap_or_default()
            .iter()
            .map(|wg| WorkGroupSummary {
                name: wg.name.clone(),
                state: wg.state.clone(),
                description: wg.description.clone(),
                creation_time: wg.creation_time,
                engine_version: wg
                    .configuration
                    .engine_version
                    .clone()
                    .unwrap_or_else(engine_version),
            })
            .collect();
        Ok(ListWorkGroupsOutput {
            work_groups,
            next_token: (end < all.len()).then(|| end.to_string()),
        })
    }

    /// The `ClientRequestToken` a query was started with, if any (exposed
    /// for tests and adapters).
    pub fn client_request_token(&self, id: &str) -> Option<String> {
        let state = self.state.lock().expect("athena state poisoned");
        state
            .queries
            .get(id)
            .and_then(|r| r.client_request_token.clone())
    }
}

/// Deletes the result objects of a query that did not complete its writes.
/// Dropped armed on the error path and when the writing task is aborted;
/// disarmed once both objects are in place.
struct ResultCleanup {
    storage: Arc<dyn StorageBackend>,
    bucket: String,
    keys: Vec<String>,
    armed: bool,
}

impl Drop for ResultCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // `Drop` cannot await; hand the deletes to the runtime. If the
        // runtime is already gone (process shutdown) there is nothing to
        // clean up against either.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let storage = Arc::clone(&self.storage);
        let bucket = std::mem::take(&mut self.bucket);
        let keys = std::mem::take(&mut self.keys);
        handle.spawn(async move {
            for key in keys {
                if let Err(e) = storage.delete_object(&bucket, &key).await {
                    tracing::warn!(bucket, key, error = %e, "failed to remove partial result object");
                }
            }
        });
    }
}

/// Why a query landed on `FAILED`.
enum Failure {
    Engine(EngineError),
    Encode(ResultError),
    ResultWrite(String),
}

impl Failure {
    /// `(ErrorCategory, ErrorType, message)`.
    fn details(&self) -> (i32, i32, String) {
        match self {
            Self::Engine(e) => (e.category(), e.error_type(), e.to_string()),
            Self::Encode(e) => (2, 1003, format!("NOT_SUPPORTED: {e}")),
            Self::ResultWrite(m) => (1, 1, format!("GENERIC_INTERNAL_ERROR: {m}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_locations_split_into_bucket_and_slash_terminated_prefix() {
        assert_eq!(
            parse_output_location("s3://results/").unwrap(),
            ("results".into(), String::new())
        );
        assert_eq!(
            parse_output_location("s3://results/athena/out").unwrap(),
            ("results".into(), "athena/out/".into())
        );
        assert!(parse_output_location("results/").is_err());
        assert!(parse_output_location("s3:///x").is_err());
    }

    #[test]
    fn statements_are_classified_like_athena() {
        assert_eq!(classify("  select 1"), ("DML", Some("SELECT")));
        assert_eq!(
            classify("WITH t AS (SELECT 1) SELECT * FROM t"),
            ("DML", Some("SELECT"))
        );
        assert_eq!(classify("CREATE TABLE t (a int)"), ("DDL", None));
        assert_eq!(classify("SHOW TABLES"), ("UTILITY", None));
        assert_eq!(classify("(SELECT 1)"), ("DML", None));
    }
}
