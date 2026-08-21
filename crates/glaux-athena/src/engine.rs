//! The execution seam between the Athena API lifecycle and the SQL engine.
//!
//! [`QueryEngine`] is the trait the service drives: it receives the SQL text
//! plus the execution context and returns Arrow batches with scan
//! statistics. [`DataFusionEngine`] hands the SQL straight to DataFusion's
//! planner (DataFusion's own dialect); [`crate::dialect::TrinoEngine`] wraps
//! it with the Trino-dialect translation layer Athena clients need and is
//! what the binaries should use.
//!
//! # Never silently wrong
//!
//! - A query that fails to parse or plan fails with DataFusion's real
//!   diagnostic — it is never rewritten or swallowed.
//! - Only read statements run. DDL, DML writes, `COPY`, and session
//!   statements are rejected by name: DataFusion *could* execute a
//!   `CREATE EXTERNAL TABLE` against local state, but that would diverge
//!   from Athena (no Glue registration, no S3 write), so the engine refuses.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor, accept, collect};
use datafusion::prelude::SessionContext;
use datafusion_datasource::file_scan_config::FileScanConfig;

/// What the service asks the engine to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRequest {
    /// The SQL text exactly as the client submitted it.
    pub sql: String,
    /// `QueryExecutionContext.Catalog`, if the client set one.
    pub catalog: Option<String>,
    /// `QueryExecutionContext.Database`, if the client set one.
    pub database: Option<String>,
}

/// What the engine hands back on success.
#[derive(Debug, Clone)]
pub struct QueryOutput {
    /// Result schema (authoritative even when there are zero batches).
    pub schema: SchemaRef,
    /// Result batches.
    pub batches: Vec<RecordBatch>,
    /// Bytes read from storage, when the engine can account for them.
    pub data_scanned_bytes: Option<u64>,
    /// Wall-clock time spent planning and executing on the engine.
    pub engine_time_millis: u64,
}

/// Why a query failed on the engine. Each variant maps to an Athena error
/// category (`1` system, `2` user) in the query's `AthenaError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// The SQL could not be parsed or planned (a user error).
    #[error("SYNTAX_ERROR: {0}")]
    Plan(String),
    /// The statement kind is valid SQL but not something glaux executes.
    #[error("NOT_SUPPORTED: {construct} is not supported: {message}")]
    Unsupported {
        /// The construct, e.g. `CREATE TABLE`.
        construct: String,
        /// Why / what to do instead.
        message: String,
    },
    /// A function was called with arguments it cannot handle (bad format
    /// string, unknown unit, invalid JSON, ...), detected at translation or
    /// at runtime. A user error.
    #[error("INVALID_FUNCTION_ARGUMENT: {function}: {message}")]
    InvalidArgument {
        /// The function.
        function: String,
        /// What was wrong.
        message: String,
    },
    /// An operator was applied to operand types Athena rejects (`varchar =
    /// integer`). A user error.
    #[error("TYPE_MISMATCH: {0}")]
    TypeMismatch(String),
    /// A value failed at runtime for a reason that is the query's fault:
    /// an invalid cast, bigint overflow, division by zero, an unparsable
    /// date. `code` is the Trino error name. A user error.
    #[error("{code}: {message}")]
    Data {
        /// Trino's error code name (`INVALID_CAST_ARGUMENT`, ...).
        code: String,
        /// The engine's diagnostic.
        message: String,
    },
    /// Execution failed for reasons outside the query's control (I/O,
    /// resources, engine internals).
    #[error("GENERIC_INTERNAL_ERROR: {0}")]
    Execution(String),
}

impl EngineError {
    /// Athena `ErrorCategory`: `2` for user errors, `1` for system errors.
    pub fn category(&self) -> i32 {
        match self {
            Self::Plan(_)
            | Self::Unsupported { .. }
            | Self::InvalidArgument { .. }
            | Self::TypeMismatch(_)
            | Self::Data { .. } => 2,
            Self::Execution(_) => 1,
        }
    }

    /// Athena `ErrorType` within the category. Athena documents `1001` as
    /// the generic user/syntax error type and `1xxx` for the user
    /// category; glaux uses `1001` for plan failures, `1003` for
    /// unsupported constructs, and `1` for engine failures.
    pub fn error_type(&self) -> i32 {
        match self {
            Self::Plan(_)
            | Self::InvalidArgument { .. }
            | Self::TypeMismatch(_)
            | Self::Data { .. } => 1001,
            Self::Unsupported { .. } => 1003,
            Self::Execution(_) => 1,
        }
    }
}

/// A SQL engine the Athena service can drive.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Plan and execute `request` to completion. Dropping the returned
    /// future (which the service does on `StopQueryExecution`) must abort
    /// the work.
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError>;
}

/// v0.1 passthrough engine: DataFusion's own SQL dialect, straight to the
/// planner. Catalogs (e.g. a `GlueCatalogProvider`) are whatever the wrapped
/// [`SessionContext`] has registered.
pub struct DataFusionEngine {
    ctx: SessionContext,
    default_catalog: String,
}

impl std::fmt::Debug for DataFusionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionEngine")
            .field("default_catalog", &self.default_catalog)
            .finish_non_exhaustive()
    }
}

impl DataFusionEngine {
    /// Wrap a context. `default_catalog` is the DataFusion catalog name that
    /// Athena's `AwsDataCatalog` maps to (i.e. the name the Glue catalog
    /// provider was registered under).
    pub fn new(ctx: SessionContext, default_catalog: impl Into<String>) -> Self {
        Self {
            ctx,
            default_catalog: default_catalog.into(),
        }
    }

    /// The wrapped context.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// A per-query context whose default catalog/schema follow the
    /// request's `QueryExecutionContext`.
    pub(crate) fn context_for(
        &self,
        request: &QueryRequest,
    ) -> Result<SessionContext, EngineError> {
        let catalog = match request.catalog.as_deref() {
            None => self.default_catalog.clone(),
            // Athena's built-in catalog name is case-insensitive on the wire.
            Some(c) if c.eq_ignore_ascii_case("awsdatacatalog") => self.default_catalog.clone(),
            Some(other) => other.to_string(),
        };
        let mut state = self.ctx.state();
        {
            let options = state.config_mut().options_mut();
            options.catalog.default_catalog = catalog.clone();
            if let Some(database) = &request.database {
                options.catalog.default_schema = database.clone();
            }
        }
        let ctx = SessionContext::new_with_state(state);
        if ctx.catalog(&catalog).is_none() {
            return Err(EngineError::Plan(format!(
                "catalog {catalog:?} does not exist"
            )));
        }
        if let Some(database) = &request.database
            && ctx
                .catalog(&catalog)
                .and_then(|c| c.schema(database))
                .is_none()
        {
            return Err(EngineError::Plan(format!(
                "SCHEMA_NOT_FOUND: Schema {database} does not exist in catalog {catalog}"
            )));
        }
        Ok(ctx)
    }
}

/// Reject every statement kind that is not a read. Returns the construct
/// name for the error message.
fn reject_non_read(plan: &LogicalPlan) -> Result<(), EngineError> {
    let construct = match plan {
        LogicalPlan::Ddl(ddl) => Some(ddl.name().to_string()),
        LogicalPlan::Dml(dml) => Some(format!("{}", dml.op)),
        LogicalPlan::Copy(_) => Some("COPY".to_string()),
        LogicalPlan::Statement(statement) => Some(statement.name().to_string()),
        _ => None,
    };
    match construct {
        Some(construct) => Err(EngineError::Unsupported {
            construct,
            message: "glaux v0.1 executes read-only queries (SELECT / WITH / VALUES / EXPLAIN); \
                      writes and DDL arrive in v0.2"
                .to_string(),
        }),
        None => Ok(()),
    }
}

/// Recover a [`GlauxSqlError`](crate::dialect::GlauxSqlError) raised inside
/// a UDF at runtime (carried as `DataFusionError::External`).
fn glaux_error(err: &DataFusionError) -> Option<EngineError> {
    match err.find_root() {
        DataFusionError::External(inner) => inner
            .downcast_ref::<crate::dialect::GlauxSqlError>()
            .cloned()
            .map(EngineError::from),
        _ => None,
    }
}

/// Classify a failure the way Athena does: anything the query's own text or
/// data caused is a user error (category 2) with a Trino error code;
/// only I/O, resource, and engine-internal failures are system errors.
///
/// Arrow reports data problems as typed `ArrowError`s (cast, parse,
/// overflow, divide-by-zero), so the classification keys on those. Errors
/// arrive wrapped in `Context` / `Diagnostic` layers (the optimizer's
/// constant folder adds one, for instance), so the root cause decides.
fn classify(err: DataFusionError) -> EngineError {
    if let Some(user) = glaux_error(&err) {
        return user;
    }
    // The root cause without the optimizer's context wrappers ("Optimizer
    // rule 'simplify_expressions' failed, caused by ..."), which would leak
    // engine internals into user-facing messages.
    let root_message = err.find_root().to_string();
    let data = |code: &str| EngineError::Data {
        code: code.to_string(),
        message: root_message.clone(),
    };
    match err.find_root() {
        DataFusionError::SQL(..)
        | DataFusionError::Plan(..)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(..) => {
            // DataFusion's coercion failure for operand types it does not
            // combine is Trino's TYPE_MISMATCH ("Cannot apply operator:
            // boolean = integer"), not a syntax error.
            if let Some(rest) = root_message.strip_prefix(
                "Error during planning: Cannot infer common argument \
                     type for comparison operation ",
            ) {
                return EngineError::TypeMismatch(format!(
                    "Cannot apply operator: {}",
                    trino_type_tokens(rest)
                ));
            }
            // Same for its arithmetic coercion failure ("Cannot coerce
            // arithmetic expression Int64 + Utf8 to valid types"), which
            // DataFusion raises while typing the projection — before the
            // strict operand checker ever sees the plan.
            if let Some(rest) = root_message
                .strip_prefix("Error during planning: Cannot coerce arithmetic expression ")
                .and_then(|text| text.strip_suffix(" to valid types"))
            {
                return EngineError::TypeMismatch(format!(
                    "Cannot apply operator: {}",
                    trino_type_tokens(rest)
                ));
            }
            // And for a `VALUES` row list whose columns it will not unify
            // ("Inconsistent data type across values list at row 1 column
            // 0. Was Date32 but found Utf8"), which is Trino's `Values rows
            // have mismatched types`. The pairs DataFusion *does* unify are
            // caught by the strict checker instead.
            if let Some(rest) = root_message.strip_prefix(
                "Error during planning: Inconsistent data type across \
                     values list at row ",
            ) && let Some((_, types)) = rest.split_once(". Was ")
                && let Some((was, found)) = types.split_once(" but found ")
            {
                return EngineError::TypeMismatch(format!(
                    "Values rows have mismatched types: row({}) vs row({})",
                    trino_type_tokens(was),
                    trino_type_tokens(found.trim_end_matches('.'))
                ));
            }
            // Trino types the operands of `AND` / `OR`, `NOT`, and a
            // `WHERE` / `HAVING` predicate as boolean and reports
            // TYPE_MISMATCH with the sentences below; DataFusion phrases the
            // same refusals in its own words and Arrow type names.
            if let Some(rest) = root_message.strip_prefix(
                "Error during planning: Cannot infer common argument type \
                     for logical boolean operation ",
            ) {
                let actual = rest
                    .split(' ')
                    .find(|token| *token != "Boolean" && !matches!(*token, "AND" | "OR"))
                    .unwrap_or(rest);
                return EngineError::TypeMismatch(format!(
                    "Logical expression term must evaluate to a boolean (actual: {})",
                    trino_type_tokens(actual)
                ));
            }
            if let Some(rest) = root_message.strip_prefix(
                "Error during planning: Unary operator 'NOT' requires a \
                     boolean expression, got ",
            ) {
                return EngineError::TypeMismatch(format!(
                    "Value of logical NOT expression must evaluate to a boolean (actual: {})",
                    trino_type_tokens(rest)
                ));
            }
            if root_message.starts_with(
                "Error during planning: Cannot create filter with non-boolean predicate",
            ) && let Some((_, actual)) = root_message.rsplit_once(" returning ")
            {
                return EngineError::TypeMismatch(format!(
                    "WHERE clause must evaluate to a boolean: actual type {}",
                    trino_type_tokens(actual)
                ));
            }
            // DataFusion does not name the operand type of a unary minus it
            // refuses, so neither can the message.
            if root_message.starts_with(
                "Error during planning: Unary operator '-' only supports signed numeric",
            ) {
                return EngineError::TypeMismatch(
                    "Cannot negate the operand of unary '-': Trino has a negation \
                     operator only for numeric and interval types"
                        .to_string(),
                );
            }
            // Function resolution: DataFusion appends "No function matches
            // the given name and argument types 'abs(Utf8)'" (plus an
            // invitation to file a DataFusion bug report, for aggregates).
            // Every name that gets this far is in glaux's coverage table, so
            // the failure is always the argument types or the arity —
            // Trino's `Unexpected parameters (...) for function ...`.
            if let Some(call) = root_message
                .split("No function matches the given name and argument types '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                && let Some((name, args)) = call.strip_suffix(')').and_then(|c| c.split_once('('))
            {
                return EngineError::TypeMismatch(format!(
                    "Unexpected parameters ({}) for function {name}",
                    trino_type_list(args)
                ));
            }
            // A window function written without `OVER`: DataFusion cannot
            // resolve it as a scalar or aggregate and says `Invalid
            // function 'rank'.`. Every name that reaches the planner is in
            // glaux's coverage table, so the missing `OVER` is the cause.
            if let Some(name) = root_message
                .strip_prefix("Error during planning: Invalid function '")
                .and_then(|rest| rest.split('\'').next())
            {
                return EngineError::InvalidArgument {
                    function: name.to_string(),
                    message: format!(
                        "{name} is a window function and requires an OVER clause, as in Trino"
                    ),
                };
            }
            if let Some(name) = root_message
                .split("Function '")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                && root_message.contains("failed to match any signature")
            {
                return EngineError::TypeMismatch(format!(
                    "Unexpected parameters for function {name}: no signature accepts these                      argument types"
                ));
            }
            EngineError::Plan(err.to_string())
        }
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::CastError(_) => data("INVALID_CAST_ARGUMENT"),
            ArrowError::ParseError(_) => data("INVALID_FUNCTION_ARGUMENT"),
            ArrowError::DivideByZero => data("DIVISION_BY_ZERO"),
            ArrowError::ArithmeticOverflow(_) => data("NUMERIC_VALUE_OUT_OF_RANGE"),
            ArrowError::ComputeError(_)
            | ArrowError::InvalidArgumentError(_)
            | ArrowError::NotYetImplemented(_)
            | ArrowError::SchemaError(_) => data("GENERIC_USER_ERROR"),
            _ => EngineError::Execution(err.to_string()),
        },
        // DataFusion raises `Execution` for data problems its kernels
        // detect themselves (format-string parse failures, bad function
        // arguments at runtime). Engine-internal failures use `Internal`.
        DataFusionError::Execution(_) => data("GENERIC_USER_ERROR"),
        _ => EngineError::Execution(err.to_string()),
    }
}

/// Best-effort mapping of the Arrow type names in a coercion diagnostic
/// ("Boolean = Int64") onto Trino's ("boolean = bigint"). Unknown tokens
/// pass through unchanged.
fn trino_type_tokens(text: &str) -> String {
    text.split(' ')
        .map(|token| match token {
            "Boolean" => "boolean",
            "Int8" => "tinyint",
            "Int16" => "smallint",
            "Int32" => "integer",
            "Int64" => "bigint",
            "Float32" => "real",
            "Float64" => "double",
            "Utf8" | "LargeUtf8" | "Utf8View" => "varchar",
            "Date32" | "Date64" => "date",
            other => other,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The Trino names of a comma-separated Arrow type list ("Utf8, Int64").
fn trino_type_list(text: &str) -> String {
    text.split(", ")
        .map(trino_type_tokens)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn execution_error(err: DataFusionError) -> EngineError {
    classify(err)
}

pub(crate) fn plan_error(err: DataFusionError) -> EngineError {
    classify(err)
}

/// Sums the bytes a physical plan read from files: the Parquet reader's
/// `bytes_scanned` metric where present (ranged reads), otherwise the full
/// size of every file the scan covered (CSV/JSON read whole files, which is
/// exactly how Athena bills them).
#[derive(Default)]
struct ScanAccountant {
    bytes: u64,
}

impl ExecutionPlanVisitor for ScanAccountant {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(exec) = (plan as &dyn Any).downcast_ref::<DataSourceExec>() {
            let source: &dyn Any = exec.data_source().as_ref();
            if let Some(config) = source.downcast_ref::<FileScanConfig>() {
                let metered = plan
                    .metrics()
                    .and_then(|m| m.sum_by_name("bytes_scanned"))
                    .map(|v| v.as_usize() as u64);
                self.bytes += metered.unwrap_or_else(|| {
                    config
                        .file_groups
                        .iter()
                        .flat_map(|group| group.iter())
                        .map(|file| file.object_meta.size)
                        .sum()
                });
            }
        }
        Ok(true)
    }
}

/// Gate, optimise, and execute a logical plan, accounting for scanned bytes.
/// `started` is when the engine began work on the request, so the reported
/// engine time covers parsing and planning too.
///
/// Planning happens first, then the read-only gate: `SessionContext::sql`
/// would *execute* DDL and SET statements eagerly, so the gate must sit
/// between planning and execution.
pub(crate) async fn run_logical_plan(
    ctx: &SessionContext,
    logical_plan: LogicalPlan,
    started: Instant,
) -> Result<QueryOutput, EngineError> {
    reject_non_read(&logical_plan)?;
    let df = ctx
        .execute_logical_plan(logical_plan)
        .await
        .map_err(plan_error)?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.map_err(plan_error)?;
    let schema = plan.schema();
    let batches = collect(Arc::clone(&plan), task_ctx)
        .await
        .map_err(execution_error)?;
    let mut accountant = ScanAccountant::default();
    accept(plan.as_ref(), &mut accountant).map_err(|e| EngineError::Execution(e.to_string()))?;
    Ok(QueryOutput {
        schema,
        batches,
        data_scanned_bytes: Some(accountant.bytes),
        engine_time_millis: started.elapsed().as_millis() as u64,
    })
}

#[async_trait]
impl QueryEngine for DataFusionEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        let started = Instant::now();
        let ctx = self.context_for(&request)?;
        let logical_plan = ctx
            .state()
            .create_logical_plan(&request.sql)
            .await
            .map_err(plan_error)?;
        run_logical_plan(&ctx, logical_plan, started).await
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;

    use super::*;

    fn engine() -> DataFusionEngine {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "people",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        DataFusionEngine::new(ctx, "datafusion")
    }

    fn request(sql: &str) -> QueryRequest {
        QueryRequest {
            sql: sql.to_string(),
            catalog: None,
            database: None,
        }
    }

    #[tokio::test]
    async fn select_runs_and_reports_zero_scanned_bytes_for_memory_tables() {
        let out = engine()
            .execute(request("SELECT count(*) AS n FROM people"))
            .await
            .unwrap();
        assert_eq!(out.schema.field(0).name(), "n");
        assert_eq!(out.batches[0].num_rows(), 1);
        assert_eq!(out.data_scanned_bytes, Some(0));
    }

    #[tokio::test]
    async fn awsdatacatalog_and_database_context_resolve() {
        let out = engine()
            .execute(QueryRequest {
                sql: "SELECT name FROM people WHERE id = 2".into(),
                catalog: Some("AwsDataCatalog".into()),
                database: Some("public".into()),
            })
            .await
            .unwrap();
        assert_eq!(out.batches[0].num_rows(), 1);

        let err = engine()
            .execute(QueryRequest {
                sql: "SELECT 1".into(),
                catalog: None,
                database: Some("nope".into()),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Plan(m) if m.contains("Schema nope does not exist")));
    }

    #[tokio::test]
    async fn parse_and_plan_failures_surface_the_real_diagnostic() {
        let err = engine().execute(request("SELEC 1")).await.unwrap_err();
        assert!(
            matches!(&err, EngineError::Plan(m) if m.contains("SELEC")),
            "{err}"
        );
        assert_eq!(err.category(), 2);

        let err = engine()
            .execute(request("SELECT nope FROM people"))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::Plan(m) if m.contains("nope")),
            "{err}"
        );
    }

    #[test]
    fn coercion_failures_map_to_type_mismatch_with_trino_type_names() {
        let err = classify(DataFusionError::Plan(
            "Cannot infer common argument type for comparison operation Boolean = Int64"
                .to_string(),
        ));
        assert_eq!(
            err.to_string(),
            "TYPE_MISMATCH: Cannot apply operator: boolean = bigint"
        );
        // Data errors report the root cause, not the optimizer context that
        // wrapped it.
        let err = classify(DataFusionError::Context(
            "Optimizer rule 'simplify_expressions' failed".to_string(),
            Box::new(DataFusionError::ArrowError(
                Box::new(ArrowError::CastError(
                    "Cannot cast string '9999999999' to value of Int32 type".to_string(),
                )),
                None,
            )),
        ));
        assert_eq!(
            err.to_string(),
            "INVALID_CAST_ARGUMENT: Arrow error: Cast error: Cannot cast string '9999999999' to value of Int32 type"
        );
    }

    #[tokio::test]
    async fn writes_and_ddl_are_rejected_by_name() {
        for (sql, construct) in [
            ("CREATE TABLE t AS SELECT 1", "CreateMemoryTable"),
            ("INSERT INTO people VALUES (9, 'z')", "Insert Into"),
            ("DROP TABLE people", "DropTable"),
            ("SET datafusion.execution.batch_size = 1", "SetVariable"),
        ] {
            let err = engine().execute(request(sql)).await.unwrap_err();
            match err {
                EngineError::Unsupported { construct: c, .. } => {
                    assert_eq!(c, construct, "{sql}")
                }
                other => panic!("{sql}: expected Unsupported, got {other:?}"),
            }
        }
        // The rejected INSERT must not have touched the table.
        let out = engine()
            .execute(request("SELECT count(*) FROM people"))
            .await
            .unwrap();
        let n = out.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
    }
}
