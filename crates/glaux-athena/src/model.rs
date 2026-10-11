//! Wire-level request and response shapes for the Athena JSON 1.1 protocol.
//!
//! Field names follow the AWS API model exactly (PascalCase), so these types
//! serialize to what `aws-sdk-*` and `boto3` expect. Timestamps are
//! epoch-seconds numbers, as in every AWS JSON protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Shared shapes
// ---------------------------------------------------------------------------

/// The Athena query lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryState {
    /// Accepted and waiting for execution.
    Queued,
    /// Executing on the engine.
    Running,
    /// Finished; results are available.
    Succeeded,
    /// Finished with an error (see [`QueryExecutionStatus::athena_error`]).
    Failed,
    /// Stopped by `StopQueryExecution`.
    Cancelled,
}

impl QueryState {
    /// Whether the query can no longer change state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// The wire spelling (`QUEUED`, `RUNNING`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

/// `QueryExecutionContext`: the catalog/database a query resolves
/// unqualified table names against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryExecutionContext {
    /// Database (Glue database / DataFusion schema) name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Data catalog name (defaults to `AwsDataCatalog`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
}

/// `ResultConfiguration`: where query results are written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResultConfiguration {
    /// S3 URI (`s3://bucket/prefix/`) results are written under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_location: Option<String>,
    /// Encryption settings. Accepted and echoed back; glaux never encrypts
    /// local result objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<Value>,
}

/// `AthenaError`: the structured failure details on a `FAILED` query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AthenaErrorDetails {
    /// `1` = system, `2` = user, `3` = other.
    pub error_category: i32,
    /// Finer-grained numeric type within the category.
    pub error_type: i32,
    /// Whether a retry could succeed.
    pub retryable: bool,
    /// Human-readable message naming the failure.
    pub error_message: String,
}

/// `QueryExecutionStatus`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryExecutionStatus {
    /// Current lifecycle state.
    pub state: QueryState,
    /// Why the state changed (the failure message for `FAILED`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_change_reason: Option<String>,
    /// Epoch seconds when the query was submitted.
    pub submission_date_time: f64,
    /// Epoch seconds when the query reached a terminal state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_date_time: Option<f64>,
    /// Structured failure details (only on `FAILED`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub athena_error: Option<AthenaErrorDetails>,
}

/// `QueryExecutionStatistics`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryExecutionStatistics {
    /// Time spent executing on the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_execution_time_in_millis: Option<i64>,
    /// Bytes read from storage by the query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_scanned_in_bytes: Option<i64>,
    /// Submission to completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_execution_time_in_millis: Option<i64>,
    /// Time spent in `QUEUED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_queue_time_in_millis: Option<i64>,
    /// Time spent after the engine finished: result encoding and the S3
    /// result write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_processing_time_in_millis: Option<i64>,
}

/// `EngineVersion`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EngineVersion {
    /// Requested version.
    pub selected_engine_version: String,
    /// Version that actually ran the query.
    pub effective_engine_version: String,
}

/// `QueryExecution`: everything `GetQueryExecution` returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryExecution {
    /// Unique id.
    pub query_execution_id: String,
    /// The SQL text as submitted.
    pub query: String,
    /// `DML`, `DDL`, or `UTILITY`.
    pub statement_type: String,
    /// Effective result configuration.
    pub result_configuration: ResultConfiguration,
    /// Effective execution context.
    pub query_execution_context: QueryExecutionContext,
    /// Lifecycle status.
    pub status: QueryExecutionStatus,
    /// Execution statistics.
    pub statistics: QueryExecutionStatistics,
    /// Workgroup the query ran in.
    pub work_group: String,
    /// Engine version.
    pub engine_version: EngineVersion,
    /// Substatement kind (`SELECT`, `CREATE_TABLE_AS_SELECT`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substatement_type: Option<String>,
}

// ---------------------------------------------------------------------------
// StartQueryExecution
// ---------------------------------------------------------------------------

/// `StartQueryExecution` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StartQueryExecutionInput {
    /// The SQL text.
    #[serde(default)]
    pub query_string: Option<String>,
    /// Idempotency token.
    #[serde(default)]
    pub client_request_token: Option<String>,
    /// Catalog/database context.
    #[serde(default)]
    pub query_execution_context: Option<QueryExecutionContext>,
    /// Result location.
    #[serde(default)]
    pub result_configuration: Option<ResultConfiguration>,
    /// Workgroup name (defaults to the configured default workgroup).
    #[serde(default)]
    pub work_group: Option<String>,
    /// Parameterized-query values. Not supported in v0.1: rejected
    /// explicitly rather than ignored.
    #[serde(default)]
    pub execution_parameters: Option<Vec<String>>,
    /// Result reuse settings. Accepted; glaux never reuses results.
    #[serde(default)]
    pub result_reuse_configuration: Option<Value>,
}

/// `StartQueryExecution` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StartQueryExecutionOutput {
    /// The new query's id.
    pub query_execution_id: String,
}

// ---------------------------------------------------------------------------
// GetQueryExecution / BatchGetQueryExecution / StopQueryExecution
// ---------------------------------------------------------------------------

/// `GetQueryExecution` / `StopQueryExecution` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryExecutionIdInput {
    /// The query id.
    #[serde(default)]
    pub query_execution_id: Option<String>,
}

/// `GetQueryExecution` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueryExecutionOutput {
    /// The query.
    pub query_execution: QueryExecution,
}

/// `BatchGetQueryExecution` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BatchGetQueryExecutionInput {
    /// Ids to look up (1..=50).
    #[serde(default)]
    pub query_execution_ids: Option<Vec<String>>,
}

/// One id `BatchGetQueryExecution` could not resolve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UnprocessedQueryExecutionId {
    /// The id.
    pub query_execution_id: String,
    /// Error code.
    pub error_code: String,
    /// Error message.
    pub error_message: String,
}

/// `BatchGetQueryExecution` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BatchGetQueryExecutionOutput {
    /// Resolved queries, in request order.
    pub query_executions: Vec<QueryExecution>,
    /// Ids that did not resolve.
    pub unprocessed_query_execution_ids: Vec<UnprocessedQueryExecutionId>,
}

// ---------------------------------------------------------------------------
// ListQueryExecutions
// ---------------------------------------------------------------------------

/// `ListQueryExecutions` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListQueryExecutionsInput {
    /// Pagination token.
    #[serde(default)]
    pub next_token: Option<String>,
    /// Page size (1..=50, default 50).
    #[serde(default)]
    pub max_results: Option<i64>,
    /// Restrict to one workgroup.
    #[serde(default)]
    pub work_group: Option<String>,
}

/// `ListQueryExecutions` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListQueryExecutionsOutput {
    /// Ids, newest first.
    pub query_execution_ids: Vec<String>,
    /// Token for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

// ---------------------------------------------------------------------------
// GetQueryResults
// ---------------------------------------------------------------------------

/// `GetQueryResults` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueryResultsInput {
    /// The query id.
    #[serde(default)]
    pub query_execution_id: Option<String>,
    /// Pagination token.
    #[serde(default)]
    pub next_token: Option<String>,
    /// Page size (1..=1000, default 1000).
    #[serde(default)]
    pub max_results: Option<i64>,
}

/// One value in a result row. Athena encodes every value as a string in
/// `VarCharValue`; `NULL` is a `Datum` with no `VarCharValue` at all.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Datum {
    /// The value's string form, absent for `NULL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub var_char_value: Option<String>,
}

/// One result row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Row {
    /// Values, one per column.
    pub data: Vec<Datum>,
}

/// `ColumnInfo`: result column metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ColumnInfo {
    /// Always `hive` for Athena results.
    pub catalog_name: String,
    /// Empty string for computed results.
    pub schema_name: String,
    /// Empty string for computed results.
    pub table_name: String,
    /// Column name.
    pub name: String,
    /// Column label (same as name).
    pub label: String,
    /// Athena type name (`varchar`, `bigint`, `timestamp`, ...).
    #[serde(rename = "Type")]
    pub type_name: String,
    /// Type precision.
    pub precision: i32,
    /// Type scale.
    pub scale: i32,
    /// `NOT_NULL`, `NULLABLE`, or `UNKNOWN`.
    pub nullable: String,
    /// Whether comparisons are case sensitive.
    pub case_sensitive: bool,
}

/// `ResultSetMetadata`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResultSetMetadata {
    /// Column metadata in result order.
    pub column_info: Vec<ColumnInfo>,
}

/// `ResultSet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResultSet {
    /// The page's rows (the first page starts with the header row).
    pub rows: Vec<Row>,
    /// Column metadata.
    pub result_set_metadata: ResultSetMetadata,
}

/// `GetQueryResults` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueryResultsOutput {
    /// Rows affected, for DML writes only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_count: Option<i64>,
    /// The result page.
    pub result_set: ResultSet,
    /// Token for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

// ---------------------------------------------------------------------------
// Workgroups
// ---------------------------------------------------------------------------

/// `WorkGroupConfiguration` (the subset glaux honors).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WorkGroupConfiguration {
    /// Default result configuration for queries in the workgroup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_configuration: Option<ResultConfiguration>,
    /// Whether workgroup settings override per-query settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enforce_work_group_configuration: Option<bool>,
    /// Whether CloudWatch metrics are published (accepted, no-op locally).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_cloud_watch_metrics_enabled: Option<bool>,
    /// Engine version selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_version: Option<EngineVersion>,
}

/// `WorkGroup`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WorkGroup {
    /// Name.
    pub name: String,
    /// `ENABLED` or `DISABLED`.
    pub state: String,
    /// Configuration.
    pub configuration: WorkGroupConfiguration,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Epoch seconds of creation.
    pub creation_time: f64,
}

/// `WorkGroupSummary` (the `ListWorkGroups` element).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WorkGroupSummary {
    /// Name.
    pub name: String,
    /// `ENABLED` or `DISABLED`.
    pub state: String,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Epoch seconds of creation.
    pub creation_time: f64,
    /// Engine version.
    pub engine_version: EngineVersion,
}

/// `CreateWorkGroup` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateWorkGroupInput {
    /// Name.
    #[serde(default)]
    pub name: Option<String>,
    /// Configuration.
    #[serde(default)]
    pub configuration: Option<WorkGroupConfiguration>,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
}

/// `GetWorkGroup` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetWorkGroupInput {
    /// Name.
    #[serde(default)]
    pub work_group: Option<String>,
}

/// `GetWorkGroup` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetWorkGroupOutput {
    /// The workgroup.
    pub work_group: WorkGroup,
}

/// `ListWorkGroups` input.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListWorkGroupsInput {
    /// Pagination token.
    #[serde(default)]
    pub next_token: Option<String>,
    /// Page size (1..=50).
    #[serde(default)]
    pub max_results: Option<i64>,
}

/// `ListWorkGroups` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListWorkGroupsOutput {
    /// Workgroups.
    pub work_groups: Vec<WorkGroupSummary>,
    /// Token for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}
