//! The Trino dialect layer: Athena SQL → DataFusion logical plans.
//!
//! Athena engine version 3 speaks Trino SQL. This module parses that
//! dialect with `sqlparser` (under an [`AthenaDialect`] that enables the
//! Trino syntax the generic dialect leaves off, such as lambdas, so they can
//! be refused *by name*), rewrites the AST onto DataFusion's functions and
//! syntax via the [shim registry](registry), registers the Rust [UDFs](udf)
//! DataFusion lacks, and plans the result with DataFusion's own SQL planner.
//!
//! # Never silently wrong
//!
//! - Every function call is checked against the registry. Unknown names,
//!   including DataFusion-only names, are refused with
//!   [`GlauxSqlError::UnknownFunction`].
//! - Constructs with no faithful translation (lambda expressions,
//!   `AT TIME ZONE`, zoned timestamp literals, ROW/MAP values, `date -
//!   date`, multi-statement batches) and syntax Trino does not have
//!   (`DISTINCT ON`, `QUALIFY`, `[1, 2]`, `::`, ...) are refused with
//!   [`GlauxSqlError::Unsupported`] naming the construct.
//! - Trino's operand-type rules are enforced on the planned query
//!   ([`strict`]), so queries Athena rejects with `TYPE_MISMATCH` are not
//!   quietly coerced.
//! - Format strings are translated specifier by specifier and unknown
//!   specifiers are errors, never dropped.
//! - `docs/sql-coverage.md` is rendered from the same registry, so the docs
//!   cannot drift from the engine.

pub mod error;
pub mod formats;
pub mod naming;
pub mod registry;
pub mod rewrite;
pub mod strict;
pub mod udf;

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

pub use error::GlauxSqlError;
pub use naming::OutputRenames;

use crate::engine::{DataFusionEngine, EngineError, QueryEngine, QueryOutput, QueryRequest};

/// The parsing dialect: `sqlparser`'s generic rules plus the Trino syntax
/// Athena queries use (lambda arrows, `FILTER (WHERE ...)`, expression
/// `GROUP BY`, `ARRAY[...]` literals via the generic dialect).
#[derive(Debug, Default, Clone, Copy)]
pub struct AthenaDialect;

impl Dialect for AthenaDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        false
    }

    /// Trino's `EXTRACT(DAY_OF_WEEK FROM x)` etc. are not sqlparser
    /// keywords; let them through as custom fields for the rewriter.
    fn allow_extract_custom(&self) -> bool {
        true
    }
}

/// A translated statement plus the output-column renames to undo after
/// execution (see [`naming`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translation {
    /// The DataFusion-dialect statement.
    pub statement: Statement,
    /// Placeholder aliases → Athena output names.
    pub renames: OutputRenames,
}

/// Parse one Trino statement and rewrite it for DataFusion.
///
/// The returned statement renders (via `Display`) as DataFusion-dialect
/// SQL, which is handy for debugging; [`TrinoEngine`] hands the AST to the
/// planner directly so nothing is re-parsed.
pub fn translate(sql: &str) -> Result<Statement, GlauxSqlError> {
    translate_full(sql).map(|t| t.statement)
}

/// [`translate`], also returning the output renames the engine must apply.
pub fn translate_full(sql: &str) -> Result<Translation, GlauxSqlError> {
    let mut statements =
        Parser::parse_sql(&AthenaDialect, sql).map_err(|e| GlauxSqlError::Parse {
            message: e.to_string(),
        })?;
    match statements.len() {
        0 => {
            return Err(GlauxSqlError::Parse {
                message: "the query is empty".to_string(),
            });
        }
        1 => {}
        n => {
            return Err(GlauxSqlError::unsupported(
                "Multiple statements",
                format!("got {n} statements; Athena runs exactly one per query execution"),
            ));
        }
    }
    let mut statement = statements.remove(0);
    // Naming runs first so it sees aliases as written (case preserved) and
    // can map the folded names back; the rewriter then folds everything.
    let renames = naming::name_outputs(&mut statement);
    rewrite::rewrite_statement(&mut statement)?;
    Ok(Translation { statement, renames })
}

/// Rename result columns back to the names Athena reports.
fn restore_names(mut output: QueryOutput, renames: &OutputRenames) -> QueryOutput {
    if renames.is_empty() {
        return output;
    }
    let fields: Vec<Field> = output
        .schema
        .fields()
        .iter()
        .map(|f| match renames.original(f.name()) {
            Some(original) => f.as_ref().clone().with_name(original),
            None => f.as_ref().clone(),
        })
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        output.schema.metadata().clone(),
    ));
    output.batches = output
        .batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                .expect("renaming fields keeps the batch valid")
        })
        .collect();
    output.schema = schema;
    output
}

/// The Trino-dialect engine: [`translate`] + DataFusion planning and
/// execution through the wrapped [`DataFusionEngine`].
pub struct TrinoEngine {
    inner: DataFusionEngine,
}

impl std::fmt::Debug for TrinoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoEngine")
            .field("inner", &self.inner)
            .finish()
    }
}

impl TrinoEngine {
    /// Wrap a context, registering the Trino UDFs on it and configuring
    /// DataFusion for Trino semantics (decimal literals, overflow-checked
    /// integer arithmetic). `default_catalog` is the DataFusion catalog name
    /// Athena's `AwsDataCatalog` maps to.
    pub fn new(ctx: SessionContext, default_catalog: impl Into<String>) -> Self {
        for udf in udf::all() {
            ctx.register_udf(udf);
        }
        for udaf in udf::all_aggregates() {
            ctx.register_udaf(udaf);
        }
        let mut state = ctx.state();
        // Trino: `1.5` is DECIMAL(2,1), not DOUBLE.
        state
            .config_mut()
            .options_mut()
            .sql_parser
            .parse_float_as_decimal = true;
        let state = SessionStateBuilder::new_from_existing(state)
            .with_analyzer_rule(Arc::new(udf::arithmetic::CheckedIntegerArithmetic))
            .build();
        Self {
            inner: DataFusionEngine::new(SessionContext::new_with_state(state), default_catalog),
        }
    }

    /// The wrapped context.
    pub fn context(&self) -> &SessionContext {
        self.inner.context()
    }
}

#[async_trait]
impl QueryEngine for TrinoEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        let started = Instant::now();
        let ctx = self.inner.context_for(&request)?;
        let Translation { statement, renames } = translate_full(&request.sql)?;
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(statement)))
            .await
            .map_err(crate::engine::plan_error)?;
        strict::check(&plan)?;
        let output = crate::engine::run_logical_plan(&ctx, plan, started).await?;
        Ok(restore_names(output, &renames))
    }
}

/// Convenience: a [`TrinoEngine`] over `ctx` as a shareable trait object.
pub fn trino_engine(
    ctx: SessionContext,
    default_catalog: impl Into<String>,
) -> Arc<dyn QueryEngine> {
    Arc::new(TrinoEngine::new(ctx, default_catalog))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_rejects_batches_and_empty_input_by_name() {
        let err = translate("SELECT 1; SELECT 2").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "Multiple statements"),
            "{err}"
        );
        assert!(matches!(translate("   "), Err(GlauxSqlError::Parse { .. })));
        assert!(
            matches!(translate("SELEC 1"), Err(GlauxSqlError::Parse { message }) if message.contains("SELEC"))
        );
    }

    #[test]
    fn lambdas_parse_under_the_athena_dialect_and_are_refused_by_name() {
        let err = translate("SELECT transform(a, x -> x + 1) FROM t").unwrap_err();
        // `transform` itself is refused first (post-order: the lambda is an
        // argument, so it is visited before the call).
        assert!(err.to_string().contains("lambda"), "{err}");
        let err = translate("SELECT array_sort(a, (x, y) -> 1) FROM t").unwrap_err();
        assert!(err.to_string().contains("lambda"), "{err}");
    }

    #[test]
    fn translated_sql_renders_datafusion_dialect() {
        let stmt = translate("SELECT if(x > 1, 'a') AS v, TRY_CAST(s AS BIGINT) FROM t").unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT CASE WHEN x > 1 THEN 'a' END AS v, TRY_CAST(trino_round_for_cast(s) AS BIGINT) AS _col1 FROM t"
        );
    }
}
