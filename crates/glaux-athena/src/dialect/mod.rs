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
//!   `AT TIME ZONE`, ROW/MAP values, multi-statement batches) are refused
//!   with [`GlauxSqlError::Unsupported`] naming the construct.
//! - Format strings are translated specifier by specifier and unknown
//!   specifiers are errors, never dropped.
//! - `docs/sql-coverage.md` is rendered from the same registry, so the docs
//!   cannot drift from the engine.

pub mod error;
pub mod formats;
pub mod registry;
pub mod rewrite;
pub mod udf;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

pub use error::GlauxSqlError;

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
}

/// Parse one Trino statement and rewrite it for DataFusion.
///
/// The returned statement renders (via `Display`) as DataFusion-dialect
/// SQL, which is handy for debugging; [`TrinoEngine`] hands the AST to the
/// planner directly so nothing is re-parsed.
pub fn translate(sql: &str) -> Result<Statement, GlauxSqlError> {
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
    rewrite::rewrite_statement(&mut statement)?;
    Ok(statement)
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
    /// Wrap a context, registering the Trino UDFs on it. `default_catalog`
    /// is the DataFusion catalog name Athena's `AwsDataCatalog` maps to.
    pub fn new(ctx: SessionContext, default_catalog: impl Into<String>) -> Self {
        for udf in udf::all() {
            ctx.register_udf(udf);
        }
        Self {
            inner: DataFusionEngine::new(ctx, default_catalog),
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
        let statement = translate(&request.sql)?;
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(statement)))
            .await
            .map_err(crate::engine::plan_error)?;
        crate::engine::run_logical_plan(&ctx, plan, started).await
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
            "SELECT CASE WHEN x > 1 THEN 'a' END AS v, TRY_CAST(s AS BIGINT) FROM t"
        );
    }
}
