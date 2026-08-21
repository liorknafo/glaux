//! Catalog-aware resolution: the rewrites that need to know a `FROM`
//! clause's columns, which only the DataFusion planner (and the Glue
//! catalog behind it) can tell.
//!
//! The AST rewriter ([`super::rewrite`]) is context-free. Two Trino rules
//! depend on the source columns:
//!
//! - `SELECT *` over a `JOIN ... USING` lists the join columns first, once,
//!   then the remaining left columns, then the remaining right columns.
//!   DataFusion keeps the left table's order with the join column in place
//!   (and, for outer joins, may keep the *right* side's copy), so
//!   positional consumers (CSV, pandas) would see different columns.
//! - An unqualified reference to a `USING` column (`WHERE k IS NOT NULL`)
//!   is ambiguous to DataFusion's filter planner; it is qualified with the
//!   join's left side here (both copies carry Trino's value, see
//!   [`super::udf::arithmetic`]).
//! - `GROUP BY` / `HAVING` see only the source columns: `SELECT status s
//!   ... GROUP BY s` is `Column 's' cannot be resolved` on Trino unless the
//!   source has a column `s`. DataFusion resolves the output alias.
//!
//! [`resolve`] plans a probe `SELECT * FROM <the select's FROM clause>`
//! (with the CTEs in scope) for every select that needs it, reads the
//! columns and the join tree off the probe plan, and then expands the
//! wildcard in Trino's order and checks the alias references. Nothing is
//! executed; a probe that fails to plan is skipped, and the real planning
//! reports the error.

use std::ops::ControlFlow;

use datafusion::common::TableReference;
use datafusion::logical_expr::{JoinConstraint, JoinType, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Cte, Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableWithJoins,
    Visit, VisitMut, Visitor, VisitorMut, With,
};

use super::error::GlauxSqlError;
use super::rewrite::{has_using_join, selects_of, selects_of_mut};

/// What one select needs from the catalog.
struct Job {
    /// `SELECT * FROM <from clause>` with the CTEs in scope.
    probe: Statement,
    /// The select's `FROM` has a `USING` join.
    using_join: bool,
    /// The select has an unqualified `*` over a `USING` join.
    expand_star: bool,
    /// Output aliases referenced from `GROUP BY` / `HAVING` (lower-case).
    aliases: Vec<String>,
}

/// A column of `SELECT *` in Trino's order.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StarColumn {
    /// For a `USING` column: the join's left-side copy.
    qualifier: Option<TableReference>,
    name: String,
    /// A `USING` column: listed first by `SELECT *` (the reference is then
    /// qualified like every other reference to it).
    using: bool,
}

/// What the probe plan told us.
struct FromInfo {
    /// Every column name the `FROM` clause exposes (lower-case).
    columns: Vec<String>,
    /// `SELECT *` in Trino's order.
    star: Vec<StarColumn>,
}

/// Apply the catalog-aware rewrites to a translated statement.
pub async fn resolve(ctx: &SessionContext, statement: &mut Statement) -> Result<(), GlauxSqlError> {
    let jobs = collect_jobs(statement);
    if jobs.iter().all(Option::is_none) {
        return Ok(());
    }
    let mut infos: Vec<Option<(Job, FromInfo)>> = Vec::with_capacity(jobs.len());
    for job in jobs {
        let Some(job) = job else {
            infos.push(None);
            continue;
        };
        let planned = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(job.probe.clone())))
            .await;
        match planned {
            Ok(plan) => {
                let info = FromInfo {
                    columns: plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_lowercase())
                        .collect(),
                    star: star_columns(&plan),
                };
                infos.push(Some((job, info)));
            }
            // The real planning will report the problem.
            Err(_) => infos.push(None),
        }
    }
    apply(statement, infos)
}

// ---------------------------------------------------------------------------
// Pass 1: collect the probes
// ---------------------------------------------------------------------------

fn collect_jobs(statement: &Statement) -> Vec<Option<Job>> {
    struct Collector {
        ctes: Vec<Vec<Cte>>,
        jobs: Vec<Option<Job>>,
    }
    impl Visitor for Collector {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            self.ctes.push(
                query
                    .with
                    .as_ref()
                    .map(|w| w.cte_tables.clone())
                    .unwrap_or_default(),
            );
            for select in selects_of(&query.body) {
                self.jobs.push(job_for(select, &self.ctes));
            }
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.ctes.pop();
            ControlFlow::Continue(())
        }
    }
    let mut collector = Collector {
        ctes: vec![],
        jobs: vec![],
    };
    let _ = statement.visit(&mut collector);
    collector.jobs
}

fn job_for(select: &Select, ctes: &[Vec<Cte>]) -> Option<Job> {
    let using_join = has_using_join(&select.from);
    let expand_star = using_join
        && select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));
    let aliases = grouping_alias_references(select);
    if !using_join && aliases.is_empty() {
        return None;
    }
    Some(Job {
        probe: probe_statement(&select.from, ctes),
        using_join,
        expand_star,
        aliases,
    })
}

/// Output aliases of `select` that `GROUP BY` / `HAVING` reference as bare
/// identifiers (subqueries inside them are not inspected).
fn grouping_alias_references(select: &Select) -> Vec<String> {
    let aliases: Vec<String> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { alias, expr } => {
                // `SELECT status AS status` names a source column anyway.
                let same_column = matches!(expr, Expr::Identifier(id) if id.value.eq_ignore_ascii_case(&alias.value));
                (!same_column).then(|| alias.value.to_lowercase())
            }
            _ => None,
        })
        .collect();
    if aliases.is_empty() {
        return vec![];
    }
    struct Identifiers {
        depth: usize,
        found: Vec<String>,
    }
    impl Visitor for Identifiers {
        type Break = ();
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if self.depth == 0
                && let Expr::Identifier(id) = expr
            {
                self.found.push(id.value.to_lowercase());
            }
            ControlFlow::Continue(())
        }
    }
    let mut identifiers = Identifiers {
        depth: 0,
        found: vec![],
    };
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for expr in exprs {
            let _ = expr.visit(&mut identifiers);
        }
    }
    if let Some(having) = &select.having {
        let _ = having.visit(&mut identifiers);
    }
    let mut referenced: Vec<String> = identifiers
        .found
        .into_iter()
        .filter(|name| aliases.contains(name))
        .collect();
    referenced.sort();
    referenced.dedup();
    referenced
}

/// `WITH <ctes in scope> SELECT * FROM <from>`.
fn probe_statement(from: &[TableWithJoins], ctes: &[Vec<Cte>]) -> Statement {
    // Inner CTEs shadow outer ones of the same name.
    let mut scoped: Vec<Cte> = Vec::new();
    for level in ctes {
        for cte in level {
            scoped.retain(|c| c.alias.name.value != cte.alias.name.value);
            scoped.push(cte.clone());
        }
    }
    let with = (!scoped.is_empty()).then(|| With {
        with_token: AttachedToken::empty(),
        recursive: false,
        cte_tables: scoped,
    });
    let select = Select {
        select_token: AttachedToken::empty(),
        optimizer_hints: vec![],
        distinct: None,
        select_modifiers: None,
        top: None,
        top_before_distinct: false,
        projection: vec![SelectItem::Wildcard(Default::default())],
        exclude: None,
        into: None,
        from: from.to_vec(),
        lateral_views: vec![],
        prewhere: None,
        selection: None,
        connect_by: vec![],
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        flavor: sqlparser::ast::SelectFlavor::Standard,
    };
    Statement::Query(Box::new(Query {
        with,
        body: Box::new(SetExpr::Select(Box::new(select))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }))
}

// ---------------------------------------------------------------------------
// Pass 2: read the probe plan
// ---------------------------------------------------------------------------

/// `SELECT *` in Trino's order for the relation a plan produces: a `USING`
/// join lists its join columns (unqualified) first, then the left input's
/// remaining columns, then the right input's; every other join concatenates
/// its inputs; a table, subquery, or anything else lists its schema.
fn star_columns(plan: &LogicalPlan) -> Vec<StarColumn> {
    match plan {
        // The probe's own `SELECT *`.
        LogicalPlan::Projection(projection) => star_columns(&projection.input),
        LogicalPlan::Join(join)
            if join.join_constraint == JoinConstraint::Using
                && matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) =>
        {
            let using: Vec<(String, Option<TableReference>)> = join
                .on
                .iter()
                .filter_map(|(left, _)| {
                    left.try_as_col()
                        .map(|c| (c.name.clone(), c.relation.clone()))
                })
                .collect();
            let mut out: Vec<StarColumn> = using
                .iter()
                .map(|(name, qualifier)| StarColumn {
                    qualifier: qualifier.clone(),
                    name: name.clone(),
                    using: true,
                })
                .collect();
            let using: Vec<String> = using.into_iter().map(|(name, _)| name).collect();
            for input in [&join.left, &join.right] {
                out.extend(
                    star_columns(input)
                        .into_iter()
                        .filter(|c| !using.contains(&c.name)),
                );
            }
            out
        }
        LogicalPlan::Join(join) => {
            let mut out = star_columns(&join.left);
            out.extend(star_columns(&join.right));
            out
        }
        other => other
            .schema()
            .iter()
            .map(|(qualifier, field)| StarColumn {
                qualifier: qualifier.cloned(),
                name: field.name().clone(),
                using: false,
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Pass 3: apply
// ---------------------------------------------------------------------------

fn apply(
    statement: &mut Statement,
    infos: Vec<Option<(Job, FromInfo)>>,
) -> Result<(), GlauxSqlError> {
    struct Applier {
        infos: std::vec::IntoIter<Option<(Job, FromInfo)>>,
    }
    impl VisitorMut for Applier {
        type Break = Box<GlauxSqlError>;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            let Query { body, order_by, .. } = query;
            let mut selects = selects_of_mut(body);
            // A set operation's ORDER BY names the union's output, not a
            // select's source columns.
            let mut order_by = if selects.len() == 1 {
                order_by.as_mut()
            } else {
                None
            };
            for (i, select) in selects.iter_mut().enumerate() {
                let Some(Some((job, info))) = self.infos.next() else {
                    continue;
                };
                let order_by = if i == 0 {
                    order_by.as_deref_mut()
                } else {
                    None
                };
                if let Err(err) = apply_to_select(select, order_by, &job, &info) {
                    return ControlFlow::Break(Box::new(err));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut applier = Applier {
        infos: infos.into_iter(),
    };
    match statement.visit(&mut applier) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(*err),
    }
}

fn apply_to_select(
    select: &mut Select,
    order_by: Option<&mut sqlparser::ast::OrderBy>,
    job: &Job,
    info: &FromInfo,
) -> Result<(), GlauxSqlError> {
    for alias in &job.aliases {
        if !info.columns.contains(alias) {
            return Err(GlauxSqlError::Parse {
                message: format!(
                    "Column '{alias}' cannot be resolved (GROUP BY and HAVING see the source \
                     columns, not the output aliases, on Trino)"
                ),
            });
        }
    }
    if job.expand_star {
        let mut expanded = Vec::with_capacity(select.projection.len() + info.star.len());
        for item in select.projection.drain(..) {
            if matches!(item, SelectItem::Wildcard(_)) {
                expanded.extend(info.star.iter().map(star_item));
            } else {
                expanded.push(item);
            }
        }
        select.projection = expanded;
    }
    if job.using_join {
        qualify_using_references(select, order_by, &info.star);
    }
    Ok(())
}

/// Qualify bare references to `USING` columns with the join's left side,
/// outside subqueries (a correlated reference stays as written and is
/// refused by the planner as ambiguous rather than guessed).
fn qualify_using_references(
    select: &mut Select,
    order_by: Option<&mut sqlparser::ast::OrderBy>,
    star: &[StarColumn],
) {
    let using: Vec<(String, TableReference)> = star
        .iter()
        .filter(|c| c.using)
        .filter_map(|c| c.qualifier.clone().map(|q| (c.name.clone(), q)))
        .collect();
    if using.is_empty() {
        return;
    }
    struct Qualifier<'a> {
        depth: usize,
        using: &'a [(String, TableReference)],
    }
    impl VisitorMut for Qualifier<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if self.depth == 0
                && let Expr::Identifier(id) = expr
                && let Some((name, qualifier)) = self
                    .using
                    .iter()
                    .find(|(name, _)| id.value.eq_ignore_ascii_case(name))
            {
                *expr = star_item_expr(&StarColumn {
                    qualifier: Some(qualifier.clone()),
                    name: name.clone(),
                    using: false,
                });
            }
            ControlFlow::Continue(())
        }
    }
    let mut qualifier = Qualifier {
        depth: 0,
        using: &using,
    };
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let _ = expr.visit(&mut qualifier);
            }
            _ => {}
        }
    }
    if let Some(selection) = &mut select.selection {
        let _ = selection.visit(&mut qualifier);
    }
    if let GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
        for expr in exprs {
            let _ = expr.visit(&mut qualifier);
        }
    }
    if let Some(having) = &mut select.having {
        let _ = having.visit(&mut qualifier);
    }
    // ORDER BY sees the output aliases first.
    let aliases: Vec<String> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_lowercase()),
            _ => None,
        })
        .collect();
    if let Some(order_by) = order_by
        && let sqlparser::ast::OrderByKind::Expressions(items) = &mut order_by.kind
    {
        let unshadowed: Vec<(String, TableReference)> = using
            .iter()
            .filter(|(name, _)| !aliases.contains(name))
            .cloned()
            .collect();
        let mut qualifier = Qualifier {
            depth: 0,
            using: &unshadowed,
        };
        for item in items {
            let _ = VisitMut::visit(&mut item.expr, &mut qualifier);
        }
    }
}

fn star_item(column: &StarColumn) -> SelectItem {
    SelectItem::UnnamedExpr(star_item_expr(column))
}

/// `"q"."name"` for a qualified column, `"name"` for a `USING` column.
fn star_item_expr(column: &StarColumn) -> Expr {
    let quoted = |s: &str| Ident::with_quote('"', s);
    match (&column.qualifier, column.using) {
        (Some(qualifier), false) => {
            let mut parts: Vec<Ident> = [qualifier.catalog(), qualifier.schema()]
                .into_iter()
                .flatten()
                .map(quoted)
                .collect();
            parts.push(quoted(qualifier.table()));
            parts.push(quoted(&column.name));
            Expr::CompoundIdentifier(parts)
        }
        _ => Expr::Identifier(quoted(&column.name)),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use sqlparser::parser::Parser;

    use super::*;
    use crate::dialect::AthenaDialect;

    async fn context() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE a (v INT, k VARCHAR, x INT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE b (k VARCHAR, w INT, x INT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE c (w INT, z INT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx
    }

    async fn resolved(sql: &str) -> Result<String, GlauxSqlError> {
        let ctx = context().await;
        let mut statement = Parser::parse_sql(&AthenaDialect, sql).unwrap().remove(0);
        resolve(&ctx, &mut statement).await?;
        Ok(statement.to_string())
    }

    #[tokio::test]
    async fn star_over_using_lists_join_columns_first() {
        assert_eq!(
            resolved("SELECT * FROM a JOIN b USING (k)").await.unwrap(),
            "SELECT \"a\".\"k\", \"a\".\"v\", \"a\".\"x\", \"b\".\"w\", \"b\".\"x\" FROM a JOIN b USING(k)"
        );
        assert_eq!(
            resolved("SELECT 1, * FROM a LEFT JOIN b USING (k, x) WHERE v > 1")
                .await
                .unwrap(),
            "SELECT 1, \"a\".\"k\", \"a\".\"x\", \"a\".\"v\", \"b\".\"w\" FROM a LEFT JOIN b USING(k, x) WHERE v > 1"
        );
        // Chained joins: each USING puts its columns first.
        assert_eq!(
            resolved("SELECT * FROM a JOIN b USING (k) JOIN c USING (w)")
                .await
                .unwrap(),
            "SELECT \"b\".\"w\", \"a\".\"k\", \"a\".\"v\", \"a\".\"x\", \"b\".\"x\", \"c\".\"z\" FROM a JOIN b USING(k) JOIN c USING(w)"
        );
        // An ambiguous USING column is left for the planner to refuse.
        assert_eq!(
            resolved("SELECT * FROM a JOIN b USING (k) JOIN a AS c USING (x)")
                .await
                .unwrap(),
            "SELECT * FROM a JOIN b USING(k) JOIN a AS c USING(x)"
        );
        // ON joins and selects without a wildcard are untouched.
        assert_eq!(
            resolved("SELECT * FROM a JOIN b ON a.k = b.k")
                .await
                .unwrap(),
            "SELECT * FROM a JOIN b ON a.k = b.k"
        );
        // Bare references to the USING column are qualified with the left
        // side (DataFusion's filter planner finds them ambiguous), except
        // inside subqueries and where an output alias shadows them.
        assert_eq!(
            resolved("SELECT k, count(*) FROM a JOIN b USING (k) WHERE k > 'a' AND EXISTS (SELECT 1 FROM c WHERE k = c.w) GROUP BY k HAVING k <> 'z' ORDER BY k")
                .await
                .unwrap(),
            "SELECT \"a\".\"k\", count(*) FROM a JOIN b USING(k) WHERE \"a\".\"k\" > 'a' AND EXISTS (SELECT 1 FROM c WHERE k = c.w) GROUP BY \"a\".\"k\" HAVING \"a\".\"k\" <> 'z' ORDER BY \"a\".\"k\""
        );
        assert_eq!(
            resolved("SELECT v AS k FROM a JOIN b USING (k) ORDER BY k")
                .await
                .unwrap(),
            "SELECT v AS k FROM a JOIN b USING(k) ORDER BY k"
        );
        // Inside CTEs, subqueries and set operations too.
        assert_eq!(
            resolved("WITH c AS (SELECT * FROM a JOIN b USING (k)) SELECT * FROM c UNION ALL SELECT * FROM (SELECT * FROM b JOIN a USING (x)) s")
                .await
                .unwrap(),
            "WITH c AS (SELECT \"a\".\"k\", \"a\".\"v\", \"a\".\"x\", \"b\".\"w\", \"b\".\"x\" FROM a JOIN b USING(k)) SELECT * FROM c UNION ALL SELECT * FROM (SELECT \"b\".\"x\", \"b\".\"k\", \"b\".\"w\", \"a\".\"v\", \"a\".\"k\" FROM b JOIN a USING(x)) s"
        );
    }

    #[tokio::test]
    async fn group_by_and_having_cannot_see_output_aliases() {
        for sql in [
            "SELECT k AS s, count(*) FROM a GROUP BY s",
            "SELECT k, count(*) AS c FROM a GROUP BY k HAVING c > 1",
            "SELECT v + 1 AS n FROM a GROUP BY ROLLUP (n)",
        ] {
            let err = resolved(sql).await.unwrap_err();
            assert!(
                err.to_string().contains("cannot be resolved"),
                "{sql}: {err}"
            );
        }
        // A source column of the same name resolves to the column.
        resolved("SELECT v AS k, count(*) FROM a GROUP BY k")
            .await
            .unwrap();
        resolved("SELECT k AS k, count(*) FROM a GROUP BY k")
            .await
            .unwrap();
        // ORDER BY may use output aliases.
        resolved("SELECT k AS s FROM a ORDER BY s").await.unwrap();
    }
}
