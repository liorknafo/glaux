//! AST rewriter: Trino function calls and constructs → DataFusion's.
//!
//! Runs as a post-order [`VisitorMut`] over the parsed statement so inner
//! calls are translated before the calls that wrap them. Every
//! `Expr::Function` is checked against the [registry](super::registry):
//! passthroughs and UDFs are left alone, rewrites are applied in place,
//! unsupported entries and unknown names abort the translation with an
//! error naming the function.
//!
//! Besides functions, the rewriter handles the syntax whose DataFusion
//! semantics differ from Trino's: identifiers and aliases (folded to lower
//! case, quoted or not, as Trino resolves them), `ORDER BY` without an
//! explicit null placement (Trino sorts NULLs last in both directions;
//! DataFusion follows PostgreSQL), `CAST` to integer / `VARCHAR` targets,
//! `EXTRACT`, `SUBSTRING`, `POSITION`, array subscripts, exponent literals
//! (`1e2` is a `double` in Trino, a decimal in DataFusion), anonymous
//! columns of nested queries and `VALUES` (`_colN`), and it refuses the
//! syntax DataFusion accepts but Trino does not (`DISTINCT ON`, `QUALIFY`,
//! `GROUP BY ALL`, `[1, 2]` literals, `::` casts, ...), so a query that
//! would fail on Athena never quietly runs here.

use std::ops::ControlFlow;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    AccessExpr, BinaryOperator, CaseWhen, CastKind, CeilFloorKind, CharacterLength, DataType,
    DateTimeField, Distinct, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArgumentList, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, NamedWindowExpr, ObjectName, ObjectNamePart, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement,
    Subscript, TableAlias, TableFactor, TableWithJoins, TimezoneInfo, TrimWhereField,
    UnaryOperator, Value, Visit, VisitMut, Visitor, VisitorMut, WindowType,
};

use super::error::GlauxSqlError;
use super::formats::{Direction, joda_to_chrono, mysql_to_chrono};
use super::naming;
use super::registry::{self, ShimKind};
use super::udf::timestamps::parse_trino_timestamp;

/// Rewrite `statement` in place. On error the statement is left partially
/// rewritten and must not be used.
pub fn rewrite_statement(statement: &mut Statement) -> Result<(), GlauxSqlError> {
    let mut visitor = Rewriter::default();
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(*err),
    }
}

/// Alias of the derived table [`wrap_values`] synthesises around a `VALUES`
/// body; its inner query must not be wrapped again.
const VALUES_WRAPPER_ALIAS: &str = "__glaux_values";

#[derive(Default)]
struct Rewriter {
    /// Set when entering the synthetic `VALUES` wrapper, consumed by the
    /// very next `pre_visit_query` (the wrapper's inner query).
    inside_values_wrapper: bool,
}

impl VisitorMut for Rewriter {
    type Break = Box<GlauxSqlError>;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        fold_negative_integer_literal(expr);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match rewrite_expr(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(Box::new(e)),
        }
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Trino resolves table names case-insensitively; Glue stores them
        // lower-case.
        for part in &mut relation.0 {
            if let ObjectNamePart::Identifier(id) = part {
                fold_ident(id);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let wrap_values = !std::mem::take(&mut self.inside_values_wrapper);
        match rewrite_query(query, wrap_values) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(Box::new(e)),
        }
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Derived {
                alias: Some(alias), ..
            } if alias.name.value == VALUES_WRAPPER_ALIAS => {
                self.inside_values_wrapper = true;
            }
            TableFactor::Table {
                alias: Some(alias), ..
            }
            | TableFactor::Derived {
                alias: Some(alias), ..
            }
            | TableFactor::NestedJoin {
                alias: Some(alias), ..
            } => {
                if let Err(err) = check_alias_quoting(alias.name.clone()) {
                    return ControlFlow::Break(Box::new(err));
                }
                fold_alias(alias);
            }
            _ => {}
        }
        let refused = match table_factor {
            TableFactor::UNNEST { .. } => Some((
                "UNNEST",
                "`CROSS JOIN UNNEST(...)` is not supported in v0.1",
            )),
            // Under the generic parser `UNNEST(arr)` in a FROM clause can
            // also arrive as a table function call.
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            } if name.to_string().eq_ignore_ascii_case("unnest") => Some((
                "UNNEST",
                "`CROSS JOIN UNNEST(...)` is not supported in v0.1",
            )),
            TableFactor::Table {
                sample: Some(_), ..
            }
            | TableFactor::Derived {
                sample: Some(_), ..
            } => Some((
                "TABLESAMPLE",
                "Trino's `TABLESAMPLE BERNOULLI / SYSTEM` has no DataFusion equivalent",
            )),
            TableFactor::Table { args: Some(_), .. } => {
                Some(("table function", "table functions are not supported"))
            }
            TableFactor::JsonTable { .. } => Some(("JSON_TABLE", "not supported")),
            TableFactor::Pivot { .. } => Some(("PIVOT", "not supported")),
            TableFactor::Unpivot { .. } => Some(("UNPIVOT", "not supported")),
            TableFactor::MatchRecognize { .. } => Some(("MATCH_RECOGNIZE", "not supported")),
            _ => None,
        };
        match refused {
            Some((construct, message)) => {
                ControlFlow::Break(Box::new(GlauxSqlError::unsupported(construct, message)))
            }
            None => ControlFlow::Continue(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Queries and select lists
// ---------------------------------------------------------------------------

/// Query-level rewrites: CTE names, `ORDER BY` null placement, `VALUES`
/// column names (unless `wrap_values` is off because this *is* the
/// wrapper's inner query), and the per-select rewrites of every select in
/// the body.
fn rewrite_query(query: &mut Query, wrap_values: bool) -> Result<(), GlauxSqlError> {
    // CTE names are identifiers too.
    if let Some(with) = &mut query.with {
        if with.recursive {
            return Err(GlauxSqlError::unsupported(
                "WITH RECURSIVE",
                "recursive CTEs are not supported in v0.1: DataFusion's recursive execution has \
                 not been vetted against Trino's semantics (its type unification differs); \
                 rewrite the recursion as an explicit UNION ALL of the levels",
            ));
        }
        for cte in &mut with.cte_tables {
            fold_alias(&mut cte.alias);
        }
    }
    rewrite_fetch(query)?;
    if !query.locks.is_empty() {
        return Err(GlauxSqlError::unsupported(
            "FOR UPDATE / FOR SHARE",
            "row locking clauses are not Trino syntax",
        ));
    }
    if query.for_clause.is_some() || query.settings.is_some() || query.format_clause.is_some() {
        return Err(GlauxSqlError::unsupported(
            "FOR XML / SETTINGS / FORMAT",
            "these query suffixes are not Trino syntax",
        ));
    }
    if !query.pipe_operators.is_empty() {
        return Err(GlauxSqlError::unsupported(
            "pipe operator (|>)",
            "pipe syntax is not Trino syntax",
        ));
    }
    if let Some(order_by) = &mut query.order_by {
        match &mut order_by.kind {
            OrderByKind::All(_) => {
                return Err(GlauxSqlError::unsupported(
                    "ORDER BY ALL",
                    "not Trino syntax; list the sort columns",
                ));
            }
            OrderByKind::Expressions(items) => default_nulls_last(items)?,
        }
    }
    if wrap_values && matches!(query.body.as_ref(), SetExpr::Values(_)) {
        wrap_values_body(&mut query.body);
    }
    for select in selects_of(&query.body) {
        check_using_references(select, query.order_by.as_ref())?;
    }
    rewrite_set_expr(&mut query.body)
}

/// `FETCH FIRST n ROWS ONLY` is Trino syntax equivalent to `LIMIT n`;
/// DataFusion's planner does not implement the `fetch` clause, so it is
/// rewritten onto the limit clause here. `WITH TIES` (keep the rows tying
/// with the last one, per the `ORDER BY`) and `PERCENT` have no DataFusion
/// equivalent and are refused by name.
fn rewrite_fetch(query: &mut Query) -> Result<(), GlauxSqlError> {
    let Some(fetch) = query.fetch.take() else {
        return Ok(());
    };
    if fetch.with_ties {
        return Err(GlauxSqlError::unsupported(
            "FETCH FIRST n ROWS WITH TIES",
            "ties have no DataFusion equivalent; use `FETCH FIRST n ROWS ONLY` / LIMIT, or rank \
             with a window function and filter",
        ));
    }
    if fetch.percent {
        return Err(GlauxSqlError::unsupported(
            "FETCH FIRST n PERCENT ROWS",
            "not Trino syntax (Trino's FETCH takes a row count)",
        ));
    }
    // `FETCH FIRST ROW ONLY` (no quantity) is one row.
    let quantity = fetch.quantity.unwrap_or_else(|| num_lit(1));
    match &mut query.limit_clause {
        None => {
            query.limit_clause = Some(sqlparser::ast::LimitClause::LimitOffset {
                limit: Some(quantity),
                offset: None,
                limit_by: vec![],
            });
            Ok(())
        }
        Some(sqlparser::ast::LimitClause::LimitOffset {
            limit: limit @ None,
            limit_by,
            ..
        }) if limit_by.is_empty() => {
            *limit = Some(quantity);
            Ok(())
        }
        Some(_) => Err(GlauxSqlError::Parse {
            message: "LIMIT and FETCH cannot be combined".to_string(),
        }),
    }
}

/// The selects of a query body: the select itself or, for a set
/// operation, every select on either side (nested queries are visited on
/// their own).
pub(crate) fn selects_of(body: &SetExpr) -> Vec<&Select> {
    match body {
        SetExpr::Select(select) => vec![select],
        SetExpr::SetOperation { left, right, .. } => {
            let mut out = selects_of(left);
            out.extend(selects_of(right));
            out
        }
        _ => vec![],
    }
}

/// Mutable [`selects_of`].
pub(crate) fn selects_of_mut(body: &mut SetExpr) -> Vec<&mut Select> {
    match body {
        SetExpr::Select(select) => vec![select],
        SetExpr::SetOperation { left, right, .. } => {
            let mut out = selects_of_mut(left);
            out.extend(selects_of_mut(right));
            out
        }
        _ => vec![],
    }
}

/// The `USING` column names of a join, if it is one.
pub(crate) fn join_using_columns(operator: &JoinOperator) -> Option<&[ObjectName]> {
    match operator {
        JoinOperator::Join(JoinConstraint::Using(columns))
        | JoinOperator::Inner(JoinConstraint::Using(columns))
        | JoinOperator::Left(JoinConstraint::Using(columns))
        | JoinOperator::LeftOuter(JoinConstraint::Using(columns))
        | JoinOperator::Right(JoinConstraint::Using(columns))
        | JoinOperator::RightOuter(JoinConstraint::Using(columns))
        | JoinOperator::FullOuter(JoinConstraint::Using(columns)) => Some(columns),
        _ => None,
    }
}

/// Whether the `FROM` clause has a `USING` join anywhere in its join trees
/// (not inside derived tables, whose output is already projected).
pub(crate) fn has_using_join(from: &[TableWithJoins]) -> bool {
    fn table_has_using(table: &TableWithJoins) -> bool {
        let nested = |factor: &TableFactor| match factor {
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => table_has_using(table_with_joins),
            _ => false,
        };
        nested(&table.relation)
            || table.joins.iter().any(|join| {
                join_using_columns(&join.join_operator).is_some() || nested(&join.relation)
            })
    }
    from.iter().any(table_has_using)
}

/// The `USING` column names and the relation names / aliases of a `FROM`
/// clause (lower-case), through nested joins.
fn using_scope(from: &[TableWithJoins]) -> (Vec<String>, Vec<String>) {
    fn factor(factor: &TableFactor, columns: &mut Vec<String>, relations: &mut Vec<String>) {
        match factor {
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                walk(table_with_joins, columns, relations);
                if let Some(alias) = alias {
                    relations.push(alias.name.value.to_lowercase());
                }
            }
            TableFactor::Table { name, alias, .. } => {
                if let Some(ObjectNamePart::Identifier(id)) = name.0.last() {
                    relations.push(id.value.to_lowercase());
                }
                if let Some(alias) = alias {
                    relations.push(alias.name.value.to_lowercase());
                }
            }
            TableFactor::Derived {
                alias: Some(alias), ..
            } => relations.push(alias.name.value.to_lowercase()),
            _ => {}
        }
    }
    fn walk(table: &TableWithJoins, columns: &mut Vec<String>, relations: &mut Vec<String>) {
        factor(&table.relation, columns, relations);
        for join in &table.joins {
            factor(&join.relation, columns, relations);
            if let Some(using) = join_using_columns(&join.join_operator) {
                for column in using {
                    if let Some(ObjectNamePart::Identifier(id)) = column.0.last() {
                        columns.push(id.value.to_lowercase());
                    }
                }
            }
        }
    }
    let (mut columns, mut relations) = (vec![], vec![]);
    for table in from {
        walk(table, &mut columns, &mut relations);
    }
    (columns, relations)
}

/// Trino exposes the columns of a `JOIN ... USING (k)` only unqualified
/// (a single `k`, `coalesce(l.k, r.k)` for outer joins): `SELECT a.k` is
/// `Column 'a.k' cannot be resolved`. DataFusion would return one side's
/// raw value, so the qualified reference is refused here.
fn check_using_references(
    select: &Select,
    order_by: Option<&sqlparser::ast::OrderBy>,
) -> Result<(), GlauxSqlError> {
    let (columns, relations) = using_scope(&select.from);
    if columns.is_empty() {
        return Ok(());
    }
    struct Checker {
        columns: Vec<String>,
        relations: Vec<String>,
    }
    impl Visitor for Checker {
        type Break = GlauxSqlError;
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<GlauxSqlError> {
            if let Expr::CompoundIdentifier(ids) = expr
                && let [.., relation, column] = ids.as_slice()
                && self.columns.contains(&column.value.to_lowercase())
                && self.relations.contains(&relation.value.to_lowercase())
            {
                return ControlFlow::Break(GlauxSqlError::Parse {
                    message: format!(
                        "Column '{}.{}' cannot be resolved: a JOIN ... USING column is only available \
                         unqualified on Trino",
                        relation.value, column.value
                    ),
                });
            }
            ControlFlow::Continue(())
        }
    }
    let mut checker = Checker { columns, relations };
    let mut result = select.visit(&mut checker);
    if let (ControlFlow::Continue(()), Some(order_by)) = (&result, order_by) {
        result = order_by.visit(&mut checker);
    }
    match result {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

/// A select-item or table alias written as a string literal (`SELECT 'a'
/// 'b'`, which Trino rejects and sqlparser reads as `'a' AS "b"`).
fn check_alias_quoting(alias: Ident) -> Result<(), GlauxSqlError> {
    if alias.quote_style == Some('\'') {
        return Err(GlauxSqlError::Parse {
            message: format!(
                "mismatched input '{}': a string literal cannot be used as an alias (write AS \
                 \"{}\" or separate the values with a comma)",
                alias.value, alias.value
            ),
        });
    }
    Ok(())
}

/// Trino sorts NULLs last whatever the direction; DataFusion (like
/// PostgreSQL) sorts them first for `DESC`. Make the default explicit.
fn default_nulls_last(items: &mut [OrderByExpr]) -> Result<(), GlauxSqlError> {
    for item in items {
        if item.with_fill.is_some() {
            return Err(GlauxSqlError::unsupported(
                "ORDER BY ... WITH FILL",
                "not Trino syntax",
            ));
        }
        if item.options.nulls_first.is_none() {
            item.options.nulls_first = Some(false);
        }
    }
    Ok(())
}

/// Trino names the columns of an anonymous `VALUES` table `_col0`, `_col1`,
/// … where DataFusion uses `column1`, `column2`, …. Wrap the rows in a
/// select that renames them; a column alias list on the derived table
/// (`AS t(a, b)`) still applies to the wrapper's output.
fn wrap_values_body(body: &mut Box<SetExpr>) {
    let SetExpr::Values(values) = body.as_ref() else {
        return;
    };
    let width = values.rows.first().map_or(0, |row| row.len());
    let projection = (0..width)
        .map(|i| SelectItem::ExprWithAlias {
            expr: Expr::Identifier(ident(&format!("column{}", i + 1))),
            alias: ident(&format!("_col{i}")),
        })
        .collect();
    let inner = Query {
        with: None,
        body: std::mem::replace(
            body,
            Box::new(SetExpr::Values(sqlparser::ast::Values {
                explicit_row: false,
                value_keyword: false,
                rows: vec![],
            })),
        ),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    };
    let mut select = Select {
        select_token: AttachedToken::empty(),
        optimizer_hints: vec![],
        distinct: None,
        select_modifiers: None,
        top: None,
        top_before_distinct: false,
        projection,
        exclude: None,
        into: None,
        from: vec![sqlparser::ast::TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(inner),
                alias: Some(TableAlias {
                    at: None,
                    explicit: true,
                    name: ident(VALUES_WRAPPER_ALIAS),
                    columns: vec![],
                }),
                sample: None,
            },
            joins: vec![],
        }],
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
    // Defensive: an empty VALUES has no columns to rename.
    if width == 0 {
        select.projection = vec![SelectItem::Wildcard(Default::default())];
    }
    **body = SetExpr::Select(Box::new(select));
}

fn rewrite_set_expr(body: &mut SetExpr) -> Result<(), GlauxSqlError> {
    match body {
        SetExpr::Select(select) => rewrite_select(select),
        SetExpr::SetOperation {
            left,
            right,
            op,
            set_quantifier,
        } => {
            check_set_operation(*op, *set_quantifier)?;
            rewrite_set_expr(left)?;
            rewrite_set_expr(right)
        }
        // Nested queries get their own `pre_visit_query`.
        SetExpr::Query(_) | SetExpr::Values(_) => Ok(()),
        SetExpr::Table(_) => Err(GlauxSqlError::unsupported(
            "TABLE statement",
            "`TABLE t` is not Trino syntax; use `SELECT * FROM t`",
        )),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
            Err(GlauxSqlError::unsupported(
                "DDL / DML / CTAS / INSERT / UNLOAD",
                "writes arrive in v0.2",
            ))
        }
    }
}

/// `EXCEPT ALL` is bag difference in Trino (`{1, 1, 1} EXCEPT ALL {1}` is
/// `{1, 1}`); DataFusion plans it as an anti-join, which removes every
/// left row whose value appears on the right at all, so it is refused
/// rather than run with the wrong multiplicities. `INTERSECT ALL` is
/// correct in DataFusion. The `BY NAME` quantifiers are not Trino syntax.
fn check_set_operation(op: SetOperator, quantifier: SetQuantifier) -> Result<(), GlauxSqlError> {
    match (op, quantifier) {
        (SetOperator::Except, SetQuantifier::All) => Err(GlauxSqlError::unsupported(
            "EXCEPT ALL",
            "Trino's EXCEPT ALL keeps the left rows by multiplicity (bag difference); DataFusion \
             runs it as an anti-join and would drop every matching row. Use EXCEPT (distinct), \
             or count the rows per value with a GROUP BY and row_number() on each side",
        )),
        (_, SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName) => {
            Err(GlauxSqlError::unsupported(
                format!("{op} {quantifier}"),
                "`BY NAME` set operations are not Trino syntax; set operations match columns by \
             position",
            ))
        }
        _ => Ok(()),
    }
}

/// Per-select rewrites: refuse non-Trino clauses, fold aliases, default the
/// window `ORDER BY` null placement, and name anonymous columns.
fn rewrite_select(select: &mut Select) -> Result<(), GlauxSqlError> {
    let refused: Option<(&str, &str)> = if matches!(select.distinct, Some(Distinct::On(_))) {
        Some((
            "DISTINCT ON",
            "not Trino syntax; use `row_number() OVER (PARTITION BY ...)`",
        ))
    } else if select.top.is_some() {
        Some(("TOP", "not Trino syntax; use LIMIT"))
    } else if select.into.is_some() {
        Some(("SELECT INTO", "not Trino syntax"))
    } else if select.qualify.is_some() {
        Some((
            "QUALIFY",
            "not Trino syntax; filter window results in an outer query",
        ))
    } else if matches!(select.group_by, GroupByExpr::All(_)) {
        Some((
            "GROUP BY ALL",
            "not Trino syntax; list the grouping columns",
        ))
    } else if !select.lateral_views.is_empty() {
        Some(("LATERAL VIEW", "not Trino syntax"))
    } else if select.prewhere.is_some() {
        Some(("PREWHERE", "not Trino syntax"))
    } else if !select.connect_by.is_empty() {
        Some(("CONNECT BY", "not Trino syntax"))
    } else if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        Some(("CLUSTER BY / DISTRIBUTE BY / SORT BY", "not Trino syntax"))
    } else if select.value_table_mode.is_some() {
        Some(("SELECT AS STRUCT / VALUE", "not Trino syntax"))
    } else if select.exclude.is_some() {
        Some(("SELECT * EXCLUDE", "not Trino syntax"))
    } else if select.select_modifiers.is_some() || !select.optimizer_hints.is_empty() {
        Some(("SELECT modifiers / optimizer hints", "not Trino syntax"))
    } else if !matches!(select.flavor, sqlparser::ast::SelectFlavor::Standard) {
        Some(("FROM-first SELECT", "not Trino syntax"))
    } else {
        None
    };
    if let Some((construct, message)) = refused {
        return Err(GlauxSqlError::unsupported(construct, message));
    }
    // Trino's `GROUP BY ()` is the empty grouping set — one global group,
    // exactly what no GROUP BY means. sqlparser parses it as an empty tuple
    // expression, which DataFusion refuses with `This feature is not
    // implemented: Empty tuple not supported yet`.
    if let GroupByExpr::Expressions(keys, _) = &mut select.group_by {
        for key in keys.iter_mut() {
            if matches!(key, Expr::Tuple(items) if items.is_empty()) {
                *key = Expr::GroupingSets(vec![vec![]]);
            }
        }
    }
    for table in &select.from {
        for join in &table.joins {
            check_join(&join.join_operator)?;
        }
    }
    for window in &mut select.named_window {
        fold_ident(&mut window.0);
        match &mut window.1 {
            NamedWindowExpr::WindowSpec(spec) => default_nulls_last(&mut spec.order_by)?,
            NamedWindowExpr::NamedWindow(name) => fold_ident(name),
        }
    }
    naming::name_nested_select(select);
    for item in &mut select.projection {
        if let SelectItem::ExprWithAlias { alias, .. } = item {
            check_alias_quoting(alias.clone())?;
            fold_ident(alias);
        }
    }
    Ok(())
}

fn check_join(operator: &JoinOperator) -> Result<(), GlauxSqlError> {
    let constraint = match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c) => c,
        JoinOperator::Semi(_)
        | JoinOperator::LeftSemi(_)
        | JoinOperator::RightSemi(_)
        | JoinOperator::Anti(_)
        | JoinOperator::LeftAnti(_)
        | JoinOperator::RightAnti(_) => {
            return Err(GlauxSqlError::unsupported(
                "SEMI / ANTI JOIN",
                "not Trino syntax; use `EXISTS` / `NOT EXISTS`",
            ));
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => {
            return Err(GlauxSqlError::unsupported(
                "CROSS APPLY / OUTER APPLY",
                "not Trino syntax",
            ));
        }
        JoinOperator::AsOf { .. } => {
            return Err(GlauxSqlError::unsupported("ASOF JOIN", "not Trino syntax"));
        }
        JoinOperator::StraightJoin(_) => {
            return Err(GlauxSqlError::unsupported(
                "STRAIGHT_JOIN",
                "not Trino syntax",
            ));
        }
        JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin => {
            return Err(GlauxSqlError::unsupported(
                "ARRAY JOIN",
                "not Trino syntax; UNNEST is not supported in v0.1 either",
            ));
        }
    };
    if matches!(constraint, JoinConstraint::Natural) {
        return Err(GlauxSqlError::unsupported(
            "NATURAL JOIN",
            "not Trino syntax; write the join condition with ON or USING",
        ));
    }
    if matches!(constraint, JoinConstraint::None) && !matches!(operator, JoinOperator::CrossJoin(_))
    {
        return Err(GlauxSqlError::unsupported(
            "JOIN without ON or USING",
            "not Trino syntax (DataFusion would run it as a cross join); write CROSS JOIN or \
             a join condition",
        ));
    }
    Ok(())
}

/// Default the null placement of the `ORDER BY` clauses attached to a call:
/// the window specification, `WITHIN GROUP`, and aggregate `ORDER BY`
/// arguments (`array_agg(x ORDER BY y)`).
fn default_call_nulls_last(f: &mut Function) -> Result<(), GlauxSqlError> {
    if let Some(WindowType::WindowSpec(spec)) = &mut f.over {
        default_nulls_last(&mut spec.order_by)?;
    }
    if let Some(WindowType::NamedWindow(name)) = &mut f.over {
        fold_ident(name);
    }
    default_nulls_last(&mut f.within_group)?;
    if let FunctionArguments::List(list) = &mut f.args {
        for clause in &mut list.clauses {
            if let FunctionArgumentClause::OrderBy(items) = clause {
                default_nulls_last(items)?;
            }
        }
    }
    Ok(())
}

/// Typed literals. `TIMESTAMP '...'` and `DATE '...'` go through glaux's
/// strict parsers (DataFusion's would accept zone suffixes, `T` separators,
/// and trailing time parts Trino rejects, and would overflow on years
/// outside Arrow's nanosecond window); `DECIMAL '1.5'` becomes the
/// `decimal(2,1)` literal Trino types it as (DataFusion would make it
/// `decimal(38,10)`); zoned timestamp / time literals and every other typed
/// literal are refused by name.
fn rewrite_typed_string(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    let Expr::TypedString(typed) = expr else {
        unreachable!("rewrite_typed_string called on a non-typed-string expression");
    };
    let text = match &typed.value.value {
        Value::SingleQuotedString(s) => s.clone(),
        other => {
            return Err(GlauxSqlError::unsupported(
                format!("{} literal", typed.data_type),
                format!("`{other}` is not a single-quoted string"),
            ));
        }
    };
    let zoned_type = matches!(
        typed.data_type,
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz)
            | DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz)
    );
    let has_zone_suffix = match typed.data_type {
        DataType::Timestamp(..) => parse_trino_timestamp(&text).is_some_and(|p| p.has_zone),
        DataType::Time(..) => literal_has_zone(&text),
        _ => false,
    };
    if zoned_type || has_zone_suffix {
        return Err(GlauxSqlError::unsupported(
            "timestamp with time zone literal",
            format!(
                "`{typed}` carries a time zone; glaux handles timestamps as zone-less UTC \
                 instants in v0.1"
            ),
        ));
    }
    match &typed.data_type {
        DataType::Timestamp(precision, _) => {
            if let Some(p) = precision
                && *p != 3
            {
                return Err(GlauxSqlError::unsupported(
                    format!("TIMESTAMP({p}) literal"),
                    "glaux only carries timestamp(3), Athena's precision",
                ));
            }
            *expr = func("trino_timestamp_literal", vec![str_lit(&text)]);
            Ok(())
        }
        DataType::Date => {
            *expr = func("trino_date", vec![str_lit(&text)]);
            Ok(())
        }
        DataType::Time(..) => Ok(()),
        DataType::Decimal(_) | DataType::Numeric(_) | DataType::Dec(_) => {
            *expr = decimal_literal(&text).ok_or_else(|| {
                GlauxSqlError::unsupported(
                    "DECIMAL literal",
                    format!("`{typed}` is not a plain decimal number (digits with an optional sign and point)"),
                )
            })?;
            Ok(())
        }
        other => Err(GlauxSqlError::unsupported(
            format!("{other} literal"),
            format!(
                "`{typed}`: typed literals other than DATE, TIME, TIMESTAMP, and DECIMAL are not supported"
            ),
        )),
    }
}

/// `DECIMAL 'text'` → `CAST('text' AS DECIMAL(p, s))` with Trino's
/// precision (all digits, ignoring leading zeros, at least 1) and scale
/// (digits after the point). `None` when the text is not a decimal number.
fn decimal_literal(text: &str) -> Option<Expr> {
    let trimmed = text.trim();
    let unsigned = trimmed.strip_prefix(['-', '+']).unwrap_or(trimmed);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) => (i, f),
        None => (unsigned, ""),
    };
    if int_part.is_empty() && frac_part.is_empty()
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = int_part.trim_start_matches('0').len() + frac_part.len();
    let precision = digits.max(1) as u64;
    let scale = frac_part.len() as i64;
    if precision > 38 {
        return None;
    }
    Some(cast_to(
        str_lit(trimmed),
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(precision, scale)),
    ))
}

/// Whether a timestamp / time literal text has a trailing zone (`Z`, an
/// offset, or a region name) after its time-of-day.
fn literal_has_zone(text: &str) -> bool {
    let text = text.trim();
    // Split off the date part if present; the remainder is the time part.
    let time_part = match text.split_once(' ') {
        Some((first, rest)) if first.contains('-') => rest.trim_start(),
        _ => text,
    };
    // A time-of-day is digits, ':' and '.'; anything after that is a zone.
    let end = time_part
        .find(|c: char| !(c.is_ascii_digit() || c == ':' || c == '.'))
        .unwrap_or(time_part.len());
    !time_part[end..].trim().is_empty()
}

// ---------------------------------------------------------------------------
// Expression constructors
// ---------------------------------------------------------------------------

fn ident(name: &str) -> Ident {
    Ident::new(name)
}

fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(ident(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn str_lit(s: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
}

fn num_lit(n: i64) -> Expr {
    Expr::Value(Value::Number(n.to_string(), false).with_empty_span())
}

fn cast_to(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array: false,
        format: None,
    }
}

fn bigint() -> DataType {
    DataType::BigInt(None)
}

fn double() -> DataType {
    DataType::Double(ExactNumberInfo::None)
}

fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn case_when(condition: Expr, result: Expr, otherwise: Option<Expr>) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen { condition, result }],
        else_result: otherwise.map(Box::new),
    }
}

fn date_part(unit: &str, expr: Expr) -> Expr {
    cast_to(func("date_part", vec![str_lit(unit), expr]), bigint())
}

/// Trino's `day_of_week`: 1 = Monday … 7 = Sunday (DataFusion's `dow` is
/// 0 = Sunday … 6 = Saturday).
fn day_of_week(expr: Expr) -> Expr {
    let dow = date_part("dow", expr);
    case_when(
        binary(dow.clone(), BinaryOperator::Eq, num_lit(0)),
        num_lit(7),
        Some(dow),
    )
}

/// Lower-case an identifier in place. Trino identifiers are
/// case-insensitive whether or not they are quoted (a quoted `"Name"`
/// resolves to column `name`); DataFusion keeps quoted identifiers
/// case-sensitive, so the fold happens here.
fn fold_ident(id: &mut Ident) {
    if id.value.chars().any(char::is_uppercase) {
        id.value = id.value.to_lowercase();
    }
}

/// Lower-case a table / CTE alias and its column aliases.
fn fold_alias(alias: &mut TableAlias) {
    fold_ident(&mut alias.name);
    for column in &mut alias.columns {
        fold_ident(&mut column.name);
    }
}

/// Take an expression out of its slot, leaving a placeholder that the caller
/// overwrites immediately.
fn take(expr: &mut Expr) -> Expr {
    std::mem::replace(expr, Expr::Value(Value::Null.with_empty_span()))
}

/// Passthrough / renamed functions whose DataFusion result is a 32-bit or
/// unsigned integer where Trino's is `bigint`. Their calls are wrapped in
/// `CAST(... AS BIGINT)` so the Athena result type matches (and so the
/// result encoder, which refuses `UInt64`, never sees one).
const BIGINT_RESULT: &[&str] = &[
    "approx_distinct",
    "dense_rank",
    "length",
    "levenshtein_distance",
    "ntile",
    "rank",
    "row_number",
    "strpos",
];

// ---------------------------------------------------------------------------
// Argument inspection
// ---------------------------------------------------------------------------

fn function_name(f: &Function) -> Result<String, GlauxSqlError> {
    match f.name.0.as_slice() {
        [ObjectNamePart::Identifier(id)] => Ok(id.value.to_ascii_lowercase()),
        _ => Err(GlauxSqlError::unsupported(
            format!("qualified function name {}", f.name),
            "Athena functions are unqualified",
        )),
    }
}

/// The plain positional argument expressions of a call, or an error for
/// named arguments, wildcards, `DISTINCT`, and `ORDER BY`-style clauses
/// (which only the passthrough aggregates accept).
fn plain_args(name: &str, f: &Function) -> Result<Vec<Expr>, GlauxSqlError> {
    let list = match &f.args {
        FunctionArguments::None => return Ok(vec![]),
        FunctionArguments::Subquery(_) => {
            return Err(GlauxSqlError::invalid_arguments(
                name,
                "a bare subquery argument is not supported",
            ));
        }
        FunctionArguments::List(list) => list,
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(GlauxSqlError::invalid_arguments(
            name,
            "DISTINCT / ORDER BY / LIMIT clauses are not supported in this call",
        ));
    }
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
            FunctionArg::Unnamed(_) => Err(GlauxSqlError::invalid_arguments(
                name,
                "wildcard arguments are not supported in this call",
            )),
            FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => Err(
                GlauxSqlError::invalid_arguments(name, "named arguments are not supported"),
            ),
        })
        .collect()
}

fn arity(name: &str, args: &[Expr], allowed: &[usize]) -> Result<(), GlauxSqlError> {
    if allowed.contains(&args.len()) {
        Ok(())
    } else {
        Err(GlauxSqlError::invalid_arguments(
            name,
            format!(
                "expected {} argument(s), got {}",
                allowed
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" or "),
                args.len()
            ),
        ))
    }
}

/// Error when a call that is being replaced by non-function syntax carries
/// clauses that would be lost.
fn require_scalar_call(name: &str, f: &Function) -> Result<(), GlauxSqlError> {
    if f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
        return Err(GlauxSqlError::invalid_arguments(
            name,
            "OVER / FILTER / WITHIN GROUP clauses are not valid on this function",
        ));
    }
    Ok(())
}

fn string_literal(name: &str, what: &str, expr: &Expr) -> Result<String, GlauxSqlError> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Ok(s.clone()),
            _ => Err(GlauxSqlError::invalid_arguments(
                name,
                format!("{what} must be a string literal"),
            )),
        },
        _ => Err(GlauxSqlError::invalid_arguments(
            name,
            format!(
                "{what} must be a string literal (column or expression formats cannot be \
                 translated ahead of execution)"
            ),
        )),
    }
}

fn integer_literal(name: &str, what: &str, expr: &Expr) -> Result<i64, GlauxSqlError> {
    let bad = || {
        GlauxSqlError::invalid_arguments(
            name,
            format!("{what} must be a non-negative integer literal"),
        )
    };
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse::<i64>().map_err(|_| bad()),
            _ => Err(bad()),
        },
        _ => Err(bad()),
    }
}

/// Rename a call in place, keeping its clauses.
fn rename(f: &mut Function, name: &str) {
    f.name = ObjectName(vec![ObjectNamePart::Identifier(ident(name))]);
}

/// Replace a call's positional arguments in place, keeping its clauses.
fn set_args(f: &mut Function, args: Vec<Expr>) {
    let unnamed = args
        .into_iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
        .collect();
    match &mut f.args {
        FunctionArguments::List(list) => list.args = unnamed,
        other => {
            *other = FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: unnamed,
                clauses: vec![],
            })
        }
    }
}

// ---------------------------------------------------------------------------
// The rules
// ---------------------------------------------------------------------------

fn rewrite_expr(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    match expr {
        Expr::Function(_) => rewrite_function(expr),
        // sqlparser reads the bare keyword as an identifier.
        Expr::Identifier(id)
            if id.quote_style.is_none() && id.value.eq_ignore_ascii_case("localtimestamp") =>
        {
            *expr = func("trino_timestamp", vec![func("now", vec![])]);
            Ok(())
        }
        Expr::Identifier(id) => {
            fold_ident(id);
            Ok(())
        }
        Expr::CompoundIdentifier(ids) => {
            ids.iter_mut().for_each(fold_ident);
            Ok(())
        }
        // Trino: a literal with an exponent is a DOUBLE; without one it is
        // a DECIMAL (which DataFusion's `parse_float_as_decimal` handles).
        Expr::Value(v) if is_exponent_literal(&v.value) => {
            let Value::Number(text, _) = &v.value else {
                unreachable!()
            };
            // The same `trino_double` shim a written-out `CAST(... AS
            // DOUBLE)` becomes, so the plan never carries a bare
            // `CAST(<varchar literal> AS DOUBLE)` that glaux cannot tell
            // apart from one DataFusion's own coercion inserted.
            *expr = func("trino_double", vec![str_lit(text)]);
            Ok(())
        }
        // An integer literal beyond bigint is a DECIMAL in Trino; DataFusion
        // would type it as an unsigned 64-bit integer (which Athena cannot
        // return) or refuse it.
        Expr::Value(v) if is_oversized_integer_literal(&v.value) => {
            let Value::Number(text, _) = &v.value else {
                unreachable!()
            };
            let digits = text
                .trim_start_matches(['-', '+'])
                .trim_start_matches('0')
                .len()
                .max(1);
            if digits > 38 {
                return Err(GlauxSqlError::unsupported(
                    "DECIMAL precision above 38",
                    format!(
                        "the literal {text} has {digits} digits; Trino decimals hold at most 38"
                    ),
                ));
            }
            *expr = cast_to(
                str_lit(text),
                DataType::Decimal(ExactNumberInfo::PrecisionAndScale(digits as u64, 0)),
            );
            Ok(())
        }
        Expr::Value(v) if matches!(v.value, Value::HexStringLiteral(_)) => {
            Err(GlauxSqlError::unsupported(
                "binary literal",
                "`0x1F` is not Trino syntax and `X'1F'` varbinary literals are not supported in v0.1",
            ))
        }
        // Under a dialect without lambda support `x -> ...` reads as the
        // PostgreSQL `->` operator; name the construct Trino users mean.
        Expr::BinaryOp {
            op: BinaryOperator::Arrow,
            ..
        } => Err(GlauxSqlError::unsupported(
            "lambda expression",
            "`x -> ...` arguments are not translated; express the logic with explicit SQL",
        )),
        // `interval * n` / `interval / n` are valid Trino (an interval
        // result, which glaux cannot return in v0.1); DataFusion's planner
        // would fail with an unnamed `Cannot get result type for temporal
        // operation` error, so they are refused by name here.
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Multiply | BinaryOperator::Divide)
                && (is_interval_operand(left) || is_interval_operand(right)) =>
        {
            Err(GlauxSqlError::unsupported(
                "interval * n",
                "multiplying or dividing an interval produces an INTERVAL in Trino, which glaux \
                 cannot return in v0.1; use date_add with a computed count instead",
            ))
        }
        Expr::BinaryOp { op, .. }
            if !matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
                    | BinaryOperator::StringConcat
                    | BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::And
                    | BinaryOperator::Or
            ) =>
        {
            Err(GlauxSqlError::unsupported(
                format!("operator {op}"),
                "not a Trino operator (Trino has no bitwise, regex-match, or PostgreSQL \
                 operators; use the equivalent function: bitwise_and, regexp_like, ...)",
            ))
        }
        Expr::Interval(interval) => check_interval(interval),
        Expr::Cast {
            kind: CastKind::DoubleColon,
            ..
        } => Err(GlauxSqlError::unsupported(
            ":: cast",
            "`x::type` is not Trino syntax; use CAST(x AS type)",
        )),
        Expr::Cast {
            kind: CastKind::SafeCast,
            ..
        } => Err(GlauxSqlError::unsupported(
            "SAFE_CAST",
            "not Trino syntax; use TRY_CAST",
        )),
        Expr::Cast { .. } => rewrite_cast(expr),
        Expr::Extract { .. } => rewrite_extract(expr),
        Expr::TypedString(_) => rewrite_typed_string(expr),
        Expr::Array(array) if !array.named => Err(GlauxSqlError::unsupported(
            "[...] array literal",
            "`[1, 2]` is not Trino syntax; use ARRAY[1, 2]",
        )),
        Expr::UnaryOp { op, .. }
            if !matches!(
                op,
                UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not
            ) =>
        {
            Err(GlauxSqlError::unsupported(
                format!("unary operator {op}"),
                "not Trino syntax; use the equivalent function (sqrt, cbrt, abs, ...)",
            ))
        }
        Expr::Substring {
            expr: source,
            substring_from,
            substring_for,
            ..
        } => {
            let Some(from) = substring_from.take() else {
                return Err(GlauxSqlError::invalid_arguments(
                    "substring",
                    "a start position is required",
                ));
            };
            let mut args = vec![take(source), *from];
            if let Some(len) = substring_for.take() {
                args.push(*len);
            }
            *expr = func("trino_substr", args);
            Ok(())
        }
        Expr::Trim {
            expr: source,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return Err(GlauxSqlError::unsupported(
                    "TRIM(x, chars)",
                    "not Trino syntax; use TRIM(BOTH chars FROM x)",
                ));
            }
            let name = match trim_where {
                None | Some(TrimWhereField::Both) => "trino_trim",
                Some(TrimWhereField::Leading) => "trino_ltrim",
                Some(TrimWhereField::Trailing) => "trino_rtrim",
            };
            let mut args = vec![take(source)];
            if let Some(what) = trim_what.take() {
                args.push(*what);
            }
            *expr = func(name, args);
            Ok(())
        }
        Expr::Position { expr: needle, r#in } => {
            let args = vec![take(r#in), take(needle)];
            *expr = cast_to(func("strpos", args), bigint());
            Ok(())
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            let mut current = take(root);
            for access in std::mem::take(access_chain) {
                current = match access {
                    AccessExpr::Subscript(Subscript::Index { index }) => {
                        func("trino_subscript", vec![current, index])
                    }
                    AccessExpr::Subscript(Subscript::Slice { .. }) => {
                        return Err(GlauxSqlError::unsupported(
                            "array slice subscript",
                            "`arr[a:b]` is not Trino syntax; use `slice(arr, start, length)`",
                        ));
                    }
                    AccessExpr::Dot(_) => {
                        return Err(GlauxSqlError::unsupported(
                            "ROW / MAP literals and types",
                            "field access on ROW values is not supported in v0.1",
                        ));
                    }
                };
            }
            *expr = current;
            Ok(())
        }
        // sqlparser reads `ceil(x)` / `floor(x)` into dedicated nodes (for
        // the `CEIL(x TO unit)` syntax), so they never reach the function
        // registry; route them to the Trino UDFs here.
        Expr::Ceil { .. } | Expr::Floor { .. } => {
            let name = if matches!(expr, Expr::Ceil { .. }) {
                "ceil"
            } else {
                "floor"
            };
            let (Expr::Ceil { expr: inner, field } | Expr::Floor { expr: inner, field }) = expr
            else {
                unreachable!()
            };
            if !matches!(
                field,
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
            ) {
                return Err(GlauxSqlError::unsupported(
                    format!("{}(x TO unit) / {}(x, scale)", name.to_uppercase(), name),
                    "not Trino syntax; use date_trunc or round",
                ));
            }
            let arg = take(inner);
            *expr = func(&format!("trino_{name}"), vec![arg]);
            Ok(())
        }
        Expr::Like {
            any,
            pattern,
            escape_char,
            ..
        } => {
            if *any {
                return Err(GlauxSqlError::unsupported(
                    "LIKE ANY",
                    "not Trino syntax; combine LIKE predicates with OR",
                ));
            }
            rewrite_like_pattern(pattern, escape_char.take())
        }
        Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_) => {
            let form = match expr {
                Expr::IsTrue(_) => "IS TRUE",
                Expr::IsNotTrue(_) => "IS NOT TRUE",
                Expr::IsFalse(_) => "IS FALSE",
                Expr::IsNotFalse(_) => "IS NOT FALSE",
                Expr::IsUnknown(_) => "IS UNKNOWN",
                _ => "IS NOT UNKNOWN",
            };
            Err(GlauxSqlError::unsupported(
                form,
                "not Trino syntax (its predicates are IS [NOT] NULL and IS [NOT] DISTINCT \
                 FROM); write `x = true`, `coalesce(x, false)`, `x IS NULL`, ... instead",
            ))
        }
        Expr::IsNormalized { .. } => Err(GlauxSqlError::unsupported(
            "IS NORMALIZED",
            "not Trino syntax; use normalize(x) = x",
        )),
        Expr::ILike { .. } => Err(GlauxSqlError::unsupported(
            "ILIKE",
            "not Trino syntax; use lower(x) LIKE lower(pattern)",
        )),
        Expr::SimilarTo { .. } | Expr::RLike { .. } => Err(GlauxSqlError::unsupported(
            "SIMILAR TO / RLIKE",
            "not Trino syntax; use regexp_like",
        )),
        // A scalar subquery over a non-nullable source (`VALUES`, a
        // literal, a NOT NULL column) is NULL when it returns no rows, but
        // DataFusion carries the source's nullability into the output
        // schema and fails at execution (`declared as non-nullable but
        // contains null values`). The wrapper declares a nullable result.
        Expr::Subquery(_) => {
            let subquery = take(expr);
            *expr = func("trino_nullable", vec![subquery]);
            Ok(())
        }
        Expr::Lambda(_) => Err(GlauxSqlError::unsupported(
            "lambda expression",
            "`x -> ...` arguments are not translated; express the logic with explicit SQL",
        )),
        Expr::AtTimeZone { .. } => Err(GlauxSqlError::unsupported(
            "AT TIME ZONE",
            "time-zone conversion is not supported in v0.1; timestamps are UTC instants",
        )),
        Expr::Struct { .. } | Expr::Dictionary(_) | Expr::Map(_) => {
            Err(GlauxSqlError::unsupported(
                "ROW / MAP literals and types",
                "ROW and MAP values are not supported in v0.1",
            ))
        }
        _ => Ok(()),
    }
}

/// Trino interval literals are `INTERVAL '<n>' <unit>` with a whole number
/// (a decimal fraction is allowed for `SECOND`); the unit is mandatory.
/// DataFusion also accepts PostgreSQL strings (`INTERVAL '1 day'`, `'1
/// hour 30 minutes'`) and the range forms (`'1-2' YEAR TO MONTH`), which
/// glaux refuses by name.
fn check_interval(interval: &sqlparser::ast::Interval) -> Result<(), GlauxSqlError> {
    let text = match interval.value.as_ref() {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => s.clone(),
            other => {
                return Err(GlauxSqlError::Parse {
                    message: format!(
                        "INTERVAL {other}: the interval value must be a quoted string \
                         (INTERVAL '1' DAY)"
                    ),
                });
            }
        },
        other => {
            return Err(GlauxSqlError::Parse {
                message: format!(
                    "INTERVAL {other}: the interval value must be a string literal (INTERVAL \
                     '1' DAY)"
                ),
            });
        }
    };
    let Some(unit) = &interval.leading_field else {
        return Err(GlauxSqlError::unsupported(
            "PostgreSQL interval string",
            format!(
                "INTERVAL '{text}' has no unit; Trino requires INTERVAL '<n>' DAY / HOUR / \
                 MINUTE / SECOND / MONTH / YEAR"
            ),
        ));
    };
    if let Some(to) = &interval.last_field {
        return Err(GlauxSqlError::unsupported(
            format!("INTERVAL ... {unit} TO {to}"),
            "interval range literals (YEAR TO MONTH, DAY TO SECOND, ...) are not supported in \
             v0.1; add the parts separately",
        ));
    }
    if interval.leading_precision.is_some() || interval.fractional_seconds_precision.is_some() {
        return Err(GlauxSqlError::unsupported(
            "INTERVAL with precision",
            "`INTERVAL '1' SECOND(3)` is not Trino syntax",
        ));
    }
    let allowed = matches!(
        unit,
        DateTimeField::Year
            | DateTimeField::Month
            | DateTimeField::Day
            | DateTimeField::Hour
            | DateTimeField::Minute
            | DateTimeField::Second
    );
    if !allowed {
        return Err(GlauxSqlError::unsupported(
            format!("INTERVAL ... {unit}"),
            "Trino interval units are YEAR, MONTH, DAY, HOUR, MINUTE, SECOND",
        ));
    }
    let body = text.trim();
    let unsigned = body.strip_prefix(['-', '+']).unwrap_or(body);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((i, f)) if matches!(unit, DateTimeField::Second) => (i, Some(f)),
        Some(_) => {
            return Err(GlauxSqlError::Parse {
                message: format!(
                    "Invalid INTERVAL {unit} value: '{text}' (a fraction is only \
                     allowed for SECOND)"
                ),
            });
        }
        None => (unsigned, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(int_part) || frac_part.is_some_and(|f| !digits(f)) {
        return Err(GlauxSqlError::Parse {
            message: format!(
                "Invalid INTERVAL {unit} value: '{text}' (Trino expects a whole number, as in \
                 INTERVAL '1' {unit}; PostgreSQL strings such as '1 day' are not accepted)"
            ),
        });
    }
    Ok(())
}

/// `LIKE` patterns: Trino has no default escape character, so `\` is a
/// literal backslash unless an `ESCAPE` clause names it; DataFusion (and
/// Arrow) always treat `\` as the escape. A literal pattern is rewritten so
/// DataFusion's backslash-escaped form means what Trino's pattern meant:
/// every backslash is doubled, and with an `ESCAPE` clause the escape
/// sequences (`#%`, `#_`, `##`) become `\%`, `\_`, `#`; any other use of
/// the escape character is an error, as in Trino. Computed patterns get the
/// same backslash doubling from the `TrinoSemantics` analyzer, once their
/// type is known.
fn rewrite_like_pattern(
    pattern: &mut Expr,
    escape: Option<sqlparser::ast::ValueWithSpan>,
) -> Result<(), GlauxSqlError> {
    let escape = match escape {
        None => None,
        Some(v) => match &v.value {
            Value::SingleQuotedString(e) => {
                let mut chars = e.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => Some(c),
                    _ => {
                        return Err(GlauxSqlError::invalid_arguments(
                            "LIKE",
                            format!("Escape string must be a single character, got {e:?}"),
                        ));
                    }
                }
            }
            other => {
                return Err(GlauxSqlError::invalid_arguments(
                    "LIKE",
                    format!("the ESCAPE clause must be a string literal, got {other}"),
                ));
            }
        },
    };
    let literal = match pattern {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(p) => Some(p.clone()),
            _ => None,
        },
        _ => None,
    };
    match (literal, escape) {
        (Some(text), escape) => {
            *pattern = str_lit(&translate_like_pattern(&text, escape)?);
        }
        (None, None) => {
            // A computed pattern (or a non-varchar literal) is left for the
            // plan-level passes, where its type is known: the strict
            // checker refuses non-varchar patterns with Trino's diagnostic,
            // and the `TrinoSemantics` analyzer doubles the backslashes of
            // varchar patterns at run time.
        }
        (None, Some(_)) => {
            return Err(GlauxSqlError::unsupported(
                "LIKE ... ESCAPE with a non-literal pattern",
                "the pattern must be a string literal for glaux to translate the escape \
                 character",
            ));
        }
    }
    Ok(())
}

/// Translate a Trino `LIKE` pattern (with optional escape character) to
/// DataFusion's backslash-escaped form.
fn translate_like_pattern(pattern: &str, escape: Option<char>) -> Result<String, GlauxSqlError> {
    let mut out = String::with_capacity(pattern.len() + 4);
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            match chars.next() {
                Some(next @ ('%' | '_')) => {
                    out.push('\\');
                    out.push(next);
                }
                Some(next) if next == c => {
                    if c == '\\' {
                        out.push_str("\\\\");
                    } else {
                        out.push(c);
                    }
                }
                _ => {
                    return Err(GlauxSqlError::invalid_arguments(
                        "LIKE",
                        format!(
                            "Escape character must be followed by '%', '_' or the escape \
                             character itself (pattern {pattern:?}, escape {c:?})"
                        ),
                    ));
                }
            }
        } else if c == '\\' {
            out.push_str("\\\\");
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// Trino's grammar reads `-<digits>` as one literal (`MINUS? INTEGER_VALUE`),
/// so `-9223372036854775808` is a `bigint` and `-9223372036854775808 - 1`
/// overflows. sqlparser reads a unary minus applied to the unsigned
/// literal, which glaux would type as `decimal(19,0)` (the magnitude is
/// beyond bigint) before the minus is seen. Folded before the children are
/// visited so the literal rules see the signed text.
fn fold_negative_integer_literal(expr: &mut Expr) {
    if let Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr: inner,
    } = expr
        && let Expr::Value(v) = inner.as_ref()
        && let Value::Number(text, long) = &v.value
        && !text.contains(['e', 'E', '.'])
        && !text.starts_with(['-', '+'])
    {
        let folded = Value::Number(format!("-{text}"), *long).with_span(v.span);
        *expr = Expr::Value(folded);
    }
}

/// Whether an operand is an interval literal (possibly parenthesised).
fn is_interval_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Interval(_) => true,
        Expr::Nested(inner) => is_interval_operand(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr: inner,
        } => is_interval_operand(inner),
        _ => false,
    }
}

fn is_exponent_literal(value: &Value) -> bool {
    matches!(value, Value::Number(text, _) if text.contains(['e', 'E']))
}

/// A plain integer literal that does not fit in a signed 64-bit integer.
fn is_oversized_integer_literal(value: &Value) -> bool {
    matches!(
        value,
        Value::Number(text, _)
            if !text.contains(['e', 'E', '.']) && text.parse::<i64>().is_err()
    )
}

fn is_row_or_map(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(..) | DataType::Map(..) | DataType::Tuple(..)
    )
}

/// `CAST` / `TRY_CAST` with Trino semantics: HALF_UP rounding into integer
/// targets, Trino's text forms (and truncation) for `VARCHAR` targets,
/// refusal of targets glaux cannot map.
fn rewrite_cast(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    let Expr::Cast {
        kind,
        expr: inner,
        data_type,
        ..
    } = expr
    else {
        unreachable!("rewrite_cast called on a non-cast expression");
    };
    if is_row_or_map(data_type) {
        return Err(GlauxSqlError::unsupported(
            "ROW / MAP literals and types",
            "casting to ROW or MAP is not supported in v0.1",
        ));
    }
    match data_type {
        DataType::Varbinary(_) | DataType::Binary(_) | DataType::Blob(_) => {
            Err(GlauxSqlError::unsupported(
                "CAST(... AS VARBINARY)",
                "glaux does not map VARBINARY cast targets in v0.1; note that Trino has no \
                 varchar → varbinary cast either",
            ))
        }
        DataType::BigInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::SmallInt(_)
        | DataType::TinyInt(_) => {
            // Rounds floating/decimal operands half away from zero and
            // leaves every other type alone; the Arrow cast then sees
            // integral values only. Applied once even if the same node is
            // visited again.
            if !matches!(inner.as_ref(), Expr::Function(f) if f.name.to_string() == "trino_round_for_cast")
            {
                let operand = take(inner);
                **inner = func("trino_round_for_cast", vec![operand]);
            }
            Ok(())
        }
        DataType::Char(_) | DataType::Character(_) => Err(GlauxSqlError::unsupported(
            "CAST(... AS CHAR(n))",
            "Trino's CHAR type pads to n characters and compares ignoring trailing spaces; \
             glaux does not model it. Use VARCHAR(n)",
        )),
        DataType::Timestamp(precision, TimezoneInfo::None) => {
            if let Some(p) = precision
                && *p != 3
            {
                return Err(GlauxSqlError::unsupported(
                    format!("CAST(... AS TIMESTAMP({p}))"),
                    "glaux only carries timestamp(3), Athena's precision",
                ));
            }
            let name = match kind {
                CastKind::TryCast => "trino_try_timestamp",
                _ => "trino_timestamp",
            };
            *expr = func(name, vec![take(inner)]);
            Ok(())
        }
        DataType::Timestamp(_, _)
        | DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            Err(GlauxSqlError::unsupported(
                "CAST(... AS TIMESTAMP WITH TIME ZONE)",
                "time-zone aware casts are not supported in v0.1",
            ))
        }
        DataType::Date => {
            let name = match kind {
                CastKind::TryCast => "trino_try_date",
                _ => "trino_date",
            };
            *expr = func(name, vec![take(inner)]);
            Ok(())
        }
        DataType::Boolean | DataType::Bool => {
            let name = match kind {
                CastKind::TryCast => "trino_try_boolean",
                _ => "trino_boolean",
            };
            *expr = func(name, vec![take(inner)]);
            Ok(())
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::None => (38, 0),
                ExactNumberInfo::Precision(p) => (*p as i64, 0),
                ExactNumberInfo::PrecisionAndScale(p, s) => (*p as i64, *s),
            };
            let name = match kind {
                CastKind::TryCast => "trino_try_to_decimal",
                _ => "trino_to_decimal",
            };
            *expr = func(name, vec![take(inner), num_lit(precision), num_lit(scale)]);
            Ok(())
        }
        DataType::Varchar(length) | DataType::CharacterVarying(length) => {
            let limit = match length {
                None | Some(CharacterLength::Max) => None,
                Some(CharacterLength::IntegerLength { length, .. }) => Some(*length),
            };
            let mut args = vec![take(inner)];
            if let Some(n) = limit {
                args.push(num_lit(n as i64));
            }
            // TRY_CAST matters for bounded targets: a non-varchar value whose
            // text exceeds `n` is an error under CAST and NULL under TRY_CAST.
            let name = match kind {
                CastKind::TryCast => "trino_try_varchar",
                _ => "trino_varchar",
            };
            *expr = func(name, args);
            Ok(())
        }
        DataType::String(_) | DataType::Text => {
            *expr = func("trino_varchar", vec![take(inner)]);
            Ok(())
        }
        DataType::Double(_) | DataType::DoublePrecision | DataType::Real => {
            let real = matches!(data_type, DataType::Real);
            let name = match (kind, real) {
                (CastKind::TryCast, false) => "trino_try_double",
                (CastKind::TryCast, true) => "trino_try_real",
                (_, false) => "trino_double",
                (_, true) => "trino_real",
            };
            *expr = func(name, vec![take(inner)]);
            Ok(())
        }
        DataType::Float(_) | DataType::Float4 | DataType::Float8 | DataType::Float64 => {
            Err(GlauxSqlError::unsupported(
                format!("CAST(... AS {data_type})"),
                "not a Trino type name; use DOUBLE or REAL",
            ))
        }
        _ => Ok(()),
    }
}

/// `EXTRACT(field FROM x)` → `date_part` with Trino's field names and
/// numbering (`DOW`: 1 = Monday … 7 = Sunday).
fn rewrite_extract(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    let Expr::Extract {
        field,
        expr: source,
        ..
    } = expr
    else {
        unreachable!("rewrite_extract called on a non-extract expression");
    };
    let unit = match field {
        DateTimeField::Year => "year",
        DateTimeField::Quarter => "quarter",
        DateTimeField::Month => "month",
        DateTimeField::Week(None) => "week",
        DateTimeField::Day => "day",
        DateTimeField::DayOfYear | DateTimeField::Doy => "doy",
        DateTimeField::Hour => "hour",
        DateTimeField::Minute => "minute",
        DateTimeField::Second => "second",
        DateTimeField::DayOfWeek | DateTimeField::Dow => {
            *expr = day_of_week(take(source));
            return Ok(());
        }
        DateTimeField::Custom(id) => match id.value.to_ascii_lowercase().as_str() {
            "day_of_month" => "day",
            "day_of_week" => {
                *expr = day_of_week(take(source));
                return Ok(());
            }
            "day_of_year" => "doy",
            "week_of_year" => "week",
            other => {
                return Err(GlauxSqlError::unsupported(
                    format!("EXTRACT({})", other.to_uppercase()),
                    "this field is not supported by glaux (supported: YEAR, QUARTER, MONTH, \
                     WEEK, DAY, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY, HOUR, MINUTE, \
                     SECOND)",
                ));
            }
        },
        other => {
            return Err(GlauxSqlError::unsupported(
                format!("EXTRACT({other})"),
                "this field is not supported by glaux (supported: YEAR, QUARTER, MONTH, WEEK, \
                 DAY, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY, HOUR, MINUTE, SECOND)",
            ));
        }
    };
    *expr = date_part(unit, take(source));
    Ok(())
}

fn rewrite_function(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    let Expr::Function(f) = expr else {
        unreachable!("rewrite_function called on a non-function expression");
    };
    let name = function_name(f)?;
    default_call_nulls_last(f)?;
    let Some(shim) = registry::lookup(&name) else {
        return Err(GlauxSqlError::UnknownFunction { name });
    };
    match shim.kind {
        ShimKind::Passthrough | ShimKind::Udf => {
            check_passthrough_arity(&name, f)?;
        }
        ShimKind::Unsupported => {
            return Err(GlauxSqlError::unsupported(
                format!("function {name}"),
                shim.translation,
            ));
        }
        ShimKind::Rewrite => {
            if let Some(replacement) = rewrite_call(&name, f)? {
                *expr = replacement;
            }
        }
    }
    if BIGINT_RESULT.contains(&name.as_str()) {
        let call = take(expr);
        *expr = cast_to(call, bigint());
    }
    Ok(())
}

/// The window offset arguments Trino validates (`LeadFunction` /
/// `NthValueFunction` / `NtileFunction` raise `INVALID_FUNCTION_ARGUMENT`)
/// but DataFusion silently reinterprets: `lead(x, -1)` runs as `lag`,
/// `lead(x, NULL)` returns `x`, `nth_value(x, 0)` returns NULL, `ntile(0)`
/// fails with the wrong error code. Validated here on the literal; a
/// non-literal offset is refused, because it could only be validated row by
/// row at execution.
fn check_window_offset(name: &str, f: &Function) -> Result<(), GlauxSqlError> {
    let (index, minimum) = match name {
        "lead" | "lag" => (1, 0),
        "nth_value" => (1, 1),
        "ntile" => (0, 1),
        _ => return Ok(()),
    };
    let FunctionArguments::List(list) = &f.args else {
        return Ok(());
    };
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(offset))) = list.args.get(index) else {
        return Ok(());
    };
    let what = if name == "ntile" { "Buckets" } else { "Offset" };
    match offset {
        Expr::Value(v) => match &v.value {
            Value::Number(text, _) => match text.parse::<i64>() {
                Ok(n) if n >= minimum => Ok(()),
                Ok(_) => Err(GlauxSqlError::invalid_arguments(
                    name,
                    format!("{what} must be at least {minimum}"),
                )),
                Err(_) => Err(GlauxSqlError::invalid_arguments(
                    name,
                    format!("{what} must be an integer, got {text}"),
                )),
            },
            Value::Null => Err(GlauxSqlError::invalid_arguments(
                name,
                format!("{what} must not be null"),
            )),
            other => Err(GlauxSqlError::invalid_arguments(
                name,
                format!("{what} must be an integer literal, got {other}"),
            )),
        },
        _ => Err(GlauxSqlError::invalid_arguments(
            name,
            format!(
                "the {} argument must be an integer literal (glaux validates it at translation, \
                 where Trino would check every row at execution)",
                what.to_lowercase()
            ),
        )),
    }
}

/// Passthroughs whose Trino overloads go beyond what DataFusion implements.
fn check_passthrough_arity(name: &str, f: &Function) -> Result<(), GlauxSqlError> {
    check_window_offset(name, f)?;
    let count = match &f.args {
        FunctionArguments::List(list) => list.args.len(),
        _ => return Ok(()),
    };
    match (name, count) {
        ("strpos", 3) => Err(GlauxSqlError::invalid_arguments(
            name,
            "the 3-argument form strpos(string, substring, instance) is not supported",
        )),
        // Trino has only `log(base, x)`; DataFusion's one-argument `log`
        // is `log10`.
        ("log", n) if n != 2 => Err(GlauxSqlError::invalid_arguments(
            name,
            format!(
                "Unexpected parameters ({n} argument(s)) for function log: Trino has only \
                 log(base, x); use log10(x), log2(x), or ln(x)"
            ),
        )),
        _ => Ok(()),
    }
}

/// Apply a rewrite. `Ok(None)` means the call was modified in place;
/// `Ok(Some(expr))` replaces the whole call.
fn rewrite_call(name: &str, f: &mut Function) -> Result<Option<Expr>, GlauxSqlError> {
    // Rewrites that keep the call (and its OVER / FILTER / DISTINCT).
    match name {
        "approx_percentile" => {
            let args = plain_args(name, f)?;
            if args.len() != 2 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "only approx_percentile(x, percentage) is supported (no weights, no \
                     arrays of percentages)",
                ));
            }
            rename(f, "trino_approx_percentile");
            return Ok(None);
        }
        "arbitrary" => {
            rename(f, "first_value");
            return Ok(None);
        }
        "every" => {
            rename(f, "bool_and");
            return Ok(None);
        }
        "variance" => {
            rename(f, "var_samp");
            return Ok(None);
        }
        "count_if" => {
            let args = plain_args(name, f)?;
            arity(name, &args, &[1])?;
            rename(f, "count");
            set_args(f, vec![case_when(args[0].clone(), num_lit(1), None)]);
            return Ok(None);
        }
        _ => {}
    }

    let args = plain_args(name, f)?;
    let simple_rename =
        |new_name: &str, allowed: &[usize]| -> Result<Option<Expr>, GlauxSqlError> {
            arity(name, &args, allowed)?;
            Ok(Some(func(new_name, args.clone())))
        };

    let replacement = match name {
        // Conditional
        "if" => {
            require_scalar_call(name, f)?;
            arity(name, &args, &[2, 3])?;
            let mut it = args.into_iter();
            let (cond, then) = (it.next().unwrap(), it.next().unwrap());
            case_when(cond, then, it.next())
        }
        // String
        "codepoint" => return simple_rename("trino_codepoint", &[1]),
        "trim" | "ltrim" | "rtrim" => {
            if args.len() != 1 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    format!(
                        "{name} takes one argument in Trino; to strip specific characters use \
                         TRIM({} 'chars' FROM x)",
                        match name {
                            "ltrim" => "LEADING",
                            "rtrim" => "TRAILING",
                            _ => "BOTH",
                        }
                    ),
                ));
            }
            return simple_rename(&format!("trino_{name}"), &[1]);
        }
        "lpad" => return simple_rename("trino_lpad", &[3]),
        "rpad" => return simple_rename("trino_rpad", &[3]),
        "upper" => return simple_rename("trino_upper", &[1]),
        "lower" => return simple_rename("trino_lower", &[1]),
        "replace" => {
            arity(name, &args, &[2, 3])?;
            let mut full = args.clone();
            if full.len() == 2 {
                full.push(str_lit(""));
            }
            func("trino_replace", full)
        }
        "levenshtein_distance" => return simple_rename("levenshtein", &[2]),
        "substr" | "substring" => return simple_rename("trino_substr", &[2, 3]),
        "split_part" => return simple_rename("trino_split_part", &[3]),
        "concat" => {
            require_scalar_call(name, f)?;
            if args.len() < 2 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "concat needs at least two arguments",
                ));
            }
            let mut it = args.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, e| binary(acc, BinaryOperator::StringConcat, e))
        }
        "split" => {
            if args.len() == 3 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "the 3-argument form split(string, delimiter, limit) is not supported",
                ));
            }
            return simple_rename("trino_split", &[2]);
        }
        "reverse" => return simple_rename("trino_reverse", &[1]),
        // Regular expressions
        "regexp_replace" => {
            arity(name, &args, &[2, 3])?;
            if matches!(args.get(2), Some(Expr::Lambda(_))) {
                return Err(GlauxSqlError::unsupported(
                    "lambda expression",
                    "regexp_replace with a lambda replacement is not translated",
                ));
            }
            return simple_rename("trino_regexp_replace", &[2, 3]);
        }
        "regexp_like" => return simple_rename("trino_regexp_like", &[2]),
        "regexp_extract" => {
            arity(name, &args, &[2, 3])?;
            if let Some(g) = args.get(2) {
                integer_literal(name, "group", g)?;
            }
            return simple_rename("trino_regexp_extract", &[2, 3]);
        }
        // Date and time
        "date" => return simple_rename("trino_date", &[1]),
        // Trino's `current_timestamp` / `now()` are `timestamp(3) with time
        // zone` (UTC on Athena); `localtimestamp` is a zone-less
        // `timestamp(3)`. DataFusion's `now()` is nanosecond-precise.
        "current_timestamp" | "now" => {
            arity(name, &args, &[0])?;
            func(
                "arrow_cast",
                vec![
                    func("trino_timestamp_millis", vec![func("now", vec![])]),
                    str_lit("Timestamp(Millisecond, Some(\"UTC\"))"),
                ],
            )
        }
        "localtimestamp" => {
            arity(name, &args, &[0])?;
            func("trino_timestamp", vec![func("now", vec![])])
        }
        "date_parse" | "parse_datetime" => {
            arity(name, &args, &[2])?;
            let fmt = string_literal(name, "format", &args[1])?;
            let chrono = if name == "date_parse" {
                mysql_to_chrono(name, &fmt, Direction::Parse)?
            } else {
                joda_to_chrono(name, &fmt, Direction::Parse)?
            };
            // `date_parse` is a zone-less `timestamp(3)`; `parse_datetime`
            // is a `timestamp(3) with time zone` (UTC). DataFusion's
            // `to_timestamp` is nanosecond-precise; the cast truncates like
            // Joda's fraction parser.
            let target = if name == "date_parse" {
                "Timestamp(Millisecond, None)"
            } else {
                "Timestamp(Millisecond, Some(\"UTC\"))"
            };
            // chrono accepts a leap second (`:60`) and DataFusion would
            // roll it over into the next minute; Joda rejects it. The
            // check UDF returns its input unchanged.
            let checked = func(
                "trino_check_parsed_time",
                vec![args[0].clone(), str_lit(&chrono)],
            );
            func(
                "arrow_cast",
                vec![
                    func("to_timestamp", vec![checked, str_lit(&chrono)]),
                    str_lit(target),
                ],
            )
        }
        "date_format" | "format_datetime" => {
            arity(name, &args, &[2])?;
            let fmt = string_literal(name, "format", &args[1])?;
            let chrono = if name == "date_format" {
                mysql_to_chrono(name, &fmt, Direction::Format)?
            } else {
                joda_to_chrono(name, &fmt, Direction::Format)?
            };
            func("to_char", vec![args[0].clone(), str_lit(&chrono)])
        }
        "date_trunc" => return simple_rename("trino_date_trunc", &[2]),
        "from_unixtime" => {
            if args.len() > 1 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "the time-zone forms from_unixtime(x, zone) / from_unixtime(x, hours, \
                     minutes) are not supported",
                ));
            }
            arity(name, &args, &[1])?;
            // Trino rounds to the millisecond (`from_unixtime(1.9999)` is
            // `...:02.000`); a plain cast would truncate. The argument is a
            // double in Trino, so an integer argument is widened first
            // (integer arithmetic would be overflow-checked).
            let millis = cast_to(
                func(
                    "round",
                    vec![binary(
                        func("trino_double", vec![args.into_iter().next().unwrap()]),
                        BinaryOperator::Multiply,
                        num_lit(1000),
                    )],
                ),
                bigint(),
            );
            // Trino's result is a `timestamp(3) with time zone`, UTC on
            // Athena (printed `... UTC`), like `parse_datetime` and `now()`.
            func(
                "arrow_cast",
                vec![millis, str_lit("Timestamp(Millisecond, Some(\"UTC\"))")],
            )
        }
        "to_unixtime" => return simple_rename("trino_to_unixtime", &[1]),
        "year" | "month" | "day" | "day_of_month" | "hour" | "minute" | "second" | "quarter"
        | "week" | "week_of_year" | "day_of_year" | "doy" => {
            arity(name, &args, &[1])?;
            let unit = match name {
                "day_of_month" => "day",
                "week_of_year" => "week",
                "day_of_year" | "doy" => "doy",
                other => other,
            };
            date_part(unit, args.into_iter().next().unwrap())
        }
        "day_of_week" | "dow" => {
            arity(name, &args, &[1])?;
            day_of_week(args.into_iter().next().unwrap())
        }
        // Math
        "mod" => {
            require_scalar_call(name, f)?;
            arity(name, &args, &[2])?;
            let mut it = args.into_iter();
            binary(
                it.next().unwrap(),
                BinaryOperator::Modulo,
                it.next().unwrap(),
            )
        }
        "ceiling" | "ceil" => return simple_rename("trino_ceil", &[1]),
        "floor" => return simple_rename("trino_floor", &[1]),
        "round" => return simple_rename("trino_round", &[1, 2]),
        // Trino: power(x, p) → double, whatever the argument types;
        // DataFusion keeps integer arguments integral.
        "pow" | "power" => {
            arity(name, &args, &[2])?;
            func(
                "trino_power",
                args.into_iter().map(|a| cast_to(a, double())).collect(),
            )
        }
        // Trino: NULL if any argument is NULL; DataFusion skips NULLs.
        "greatest" | "least" => {
            require_scalar_call(name, f)?;
            if args.len() < 2 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    format!("{name} needs at least two arguments"),
                ));
            }
            let mut any_null = args
                .iter()
                .map(|a| Expr::IsNull(Box::new(a.clone())))
                .reduce(|acc, e| binary(acc, BinaryOperator::Or, e))
                .unwrap();
            any_null = Expr::Nested(Box::new(std::mem::replace(
                &mut any_null,
                Expr::Value(Value::Null.with_empty_span()),
            )));
            case_when(
                any_null,
                Expr::Value(Value::Null.with_empty_span()),
                Some(func(name, args)),
            )
        }
        "rand" => return simple_rename("random", &[0]),
        // Trino has a bounded overload, `random(n) -> [0, n)`, that
        // DataFusion's nullary `random()` does not; the draw itself stays
        // DataFusion's.
        "random" => {
            arity(name, &args, &[0, 1])?;
            match args.into_iter().next() {
                Some(bound) => func("trino_random", vec![bound, func("random", vec![])]),
                None => func("random", vec![]),
            }
        }
        "sign" => return simple_rename("trino_sign", &[1]),
        "sqrt" => return simple_rename("trino_sqrt", &[1]),
        "truncate" => return simple_rename("trino_truncate", &[1, 2]),
        // Arrays
        "array_join" => return simple_rename("trino_array_join", &[2, 3]),
        "array_max" => return simple_rename("trino_array_max", &[1]),
        "array_min" => return simple_rename("trino_array_min", &[1]),
        // The Rust UDF: NULL for a NULL array or element argument, 0 when
        // absent, IEEE equality for float elements (DataFusion's
        // `array_position` would return NULL when absent and treat NaN as
        // equal to NaN).
        "array_position" => return simple_rename("trino_array_position", &[2]),
        "array_remove" => return simple_rename("trino_array_remove", &[2]),
        "array_sort" => {
            if args.len() == 2 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "the comparator-lambda form is not supported",
                ));
            }
            arity(name, &args, &[1])?;
            // Trino sorts NULL elements last; DataFusion's default is
            // first. Elements that are themselves arrays rank through
            // Trino's array ordering operator, which refuses NULLs inside
            // them, so the argument goes through the guard first.
            let mut full = vec![func(
                "trino_array_element_sort_key",
                vec![args.into_iter().next().expect("arity checked")],
            )];
            full.push(str_lit("ASC"));
            full.push(str_lit("NULLS LAST"));
            func("array_sort", full)
        }
        "arrays_overlap" => return simple_rename("trino_arrays_overlap", &[2]),
        "cardinality" => {
            arity(name, &args, &[1])?;
            cast_to(func("cardinality", args), bigint())
        }
        "contains" => return simple_rename("trino_contains", &[2]),
        "element_at" => return simple_rename("trino_element_at", &[2]),
        other => {
            // Every Rewrite entry in the registry must have a rule here; a
            // test enforces it, and this arm keeps the failure loud.
            return Err(GlauxSqlError::unsupported(
                format!("function {other}"),
                "registered as a rewrite but no rewrite rule exists (glaux bug)",
            ));
        }
    };
    Ok(Some(replacement))
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    fn rewrite(sql: &str) -> Result<String, GlauxSqlError> {
        let mut stmts = Parser::parse_sql(&GenericDialect, sql).expect("parses");
        let mut stmt = stmts.remove(0);
        rewrite_statement(&mut stmt)?;
        Ok(stmt.to_string())
    }

    #[test]
    fn every_rewrite_entry_has_a_rule() {
        for shim in registry::FUNCTIONS
            .iter()
            .filter(|s| s.kind == ShimKind::Rewrite)
        {
            // Call with a plausible arity; any error other than the "no
            // rewrite rule" marker means the rule exists.
            let sql = format!("SELECT {}('x', 'y')", shim.name);
            match rewrite(&sql) {
                Ok(_) => {}
                Err(e) => assert!(
                    !e.to_string().contains("no rewrite rule"),
                    "{}: {e}",
                    shim.name
                ),
            }
        }
    }

    #[test]
    fn renames_keep_aggregate_clauses() {
        assert_eq!(
            rewrite("SELECT count_if(x > 1) FILTER (WHERE y) OVER (PARTITION BY z) FROM t")
                .unwrap(),
            "SELECT count(CASE WHEN x > 1 THEN 1 END) FILTER (WHERE y) OVER (PARTITION BY z) AS _col0 FROM t"
        );
        assert_eq!(
            rewrite(
                "SELECT arbitrary(x) AS a, every(b) AS e, approx_percentile(v, 0.9) AS p FROM t"
            )
            .unwrap(),
            "SELECT first_value(x) AS a, bool_and(b) AS e, trino_approx_percentile(v, 0.9) AS p FROM t"
        );
    }

    #[test]
    fn nested_calls_are_rewritten_inside_out() {
        assert_eq!(
            rewrite("SELECT if(cardinality(split(s, ',')) > 1, 'many', 'one') AS c FROM t")
                .unwrap(),
            "SELECT CASE WHEN CAST(cardinality(trino_split(s, ',')) AS BIGINT) > 1 THEN 'many' ELSE 'one' END AS c FROM t"
        );
    }

    #[test]
    fn concat_becomes_null_propagating_operator() {
        assert_eq!(
            rewrite("SELECT concat(a, '-', b) AS c FROM t").unwrap(),
            "SELECT a || '-' || b AS c FROM t"
        );
    }

    #[test]
    fn regexp_shims_route_to_the_checked_udfs() {
        assert_eq!(
            rewrite("SELECT regexp_replace(s, 'a+') AS a, regexp_extract(s, '\\d+') AS b, regexp_extract(s, '(a)(b)', 2) AS c, regexp_like(s, 'x') AS d FROM t").unwrap(),
            "SELECT trino_regexp_replace(s, 'a+') AS a, trino_regexp_extract(s, '\\d+') AS b, trino_regexp_extract(s, '(a)(b)', 2) AS c, trino_regexp_like(s, 'x') AS d FROM t"
        );
        let err = rewrite("SELECT regexp_extract(s, 'x', n) FROM t").unwrap_err();
        assert!(err.to_string().contains("integer literal"), "{err}");
    }

    #[test]
    fn date_functions_translate_formats_and_units() {
        assert_eq!(
            rewrite("SELECT date_parse(s, '%Y-%m-%d %H:%i:%s') AS a, format_datetime(ts, 'yyyy-MM-dd') AS b, parse_datetime(s, 'yyyy') AS c FROM t").unwrap(),
            "SELECT arrow_cast(to_timestamp(trino_check_parsed_time(s, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'), 'Timestamp(Millisecond, None)') AS a, to_char(ts, '%Y-%m-%d') AS b, arrow_cast(to_timestamp(trino_check_parsed_time(s, '%Y'), '%Y'), 'Timestamp(Millisecond, Some(\"UTC\"))') AS c FROM t"
        );
        assert_eq!(
            rewrite("SELECT day_of_week(d) AS a, month(d) AS b FROM t").unwrap(),
            "SELECT CASE WHEN CAST(date_part('dow', d) AS BIGINT) = 0 THEN 7 ELSE CAST(date_part('dow', d) AS BIGINT) END AS a, CAST(date_part('month', d) AS BIGINT) AS b FROM t"
        );
        assert_eq!(
            rewrite("SELECT from_unixtime(t) AS a, to_unixtime(ts) AS b, date_trunc('day', ts) AS c FROM t").unwrap(),
            "SELECT arrow_cast(CAST(round(trino_double(t) * 1000) AS BIGINT), 'Timestamp(Millisecond, Some(\"UTC\"))') AS a, trino_to_unixtime(ts) AS b, trino_date_trunc('day', ts) AS c FROM t"
        );
    }

    #[test]
    fn identifiers_fold_to_lower_case_quoted_or_not() {
        assert_eq!(
            rewrite(
                "SELECT \"K\", T.Name AS \"CustomerName\" FROM \"Customers\" T WHERE Country = 'US'"
            )
            .unwrap(),
            "SELECT \"k\", t.name AS \"customername\" FROM \"customers\" t WHERE country = 'US'"
        );
    }

    #[test]
    fn casts_get_trino_semantics() {
        assert_eq!(
            rewrite("SELECT CAST(x AS BIGINT) AS a, TRY_CAST(y AS INTEGER) AS b, CAST(z AS VARCHAR) AS c, CAST(z AS VARCHAR(2)) AS d FROM t").unwrap(),
            "SELECT CAST(trino_round_for_cast(x) AS BIGINT) AS a, TRY_CAST(trino_round_for_cast(y) AS INTEGER) AS b, trino_varchar(z) AS c, trino_varchar(z, 2) AS d FROM t"
        );
        let err = rewrite("SELECT CAST(x AS VARBINARY) FROM t").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "CAST(... AS VARBINARY)"),
            "{err}"
        );
        // Exponent literals are doubles; plain decimals are left for
        // DataFusion's decimal parsing.
        assert_eq!(
            rewrite("SELECT 1e2 AS a, 1.5 AS b, 10 AS c").unwrap(),
            "SELECT trino_double('1e2') AS a, 1.5 AS b, 10 AS c"
        );
    }

    #[test]
    fn syntax_forms_map_to_trino_udfs() {
        assert_eq!(
            rewrite("SELECT EXTRACT(DOW FROM d) AS a, EXTRACT(YEAR FROM d) AS b, substring(s FROM 2 FOR 3) AS c, substr(s, -3) AS d, position('a' IN s) AS e, a[1][2] AS f, split_part(s, ',', 2) AS g, element_at(a, -1) AS h FROM t").unwrap(),
            "SELECT CASE WHEN CAST(date_part('dow', d) AS BIGINT) = 0 THEN 7 ELSE CAST(date_part('dow', d) AS BIGINT) END AS a, CAST(date_part('year', d) AS BIGINT) AS b, trino_substr(s, 2, 3) AS c, trino_substr(s, -3) AS d, CAST(strpos(s, 'a') AS BIGINT) AS e, trino_subscript(trino_subscript(a, 1), 2) AS f, trino_split_part(s, ',', 2) AS g, trino_element_at(a, -1) AS h FROM t"
        );
        let err = rewrite("SELECT EXTRACT(EPOCH FROM d) FROM t").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "EXTRACT(EPOCH)"),
            "{err}"
        );
    }

    #[test]
    fn bigint_results_are_cast_for_narrow_or_unsigned_datafusion_types() {
        assert_eq!(
            rewrite("SELECT length(s) AS a, row_number() OVER (ORDER BY s) AS b, approx_distinct(s) AS c FROM t")
                .unwrap(),
            "SELECT CAST(length(s) AS BIGINT) AS a, CAST(row_number() OVER (ORDER BY s NULLS LAST) AS BIGINT) AS b, CAST(approx_distinct(s) AS BIGINT) AS c FROM t"
        );
    }

    #[test]
    fn order_by_defaults_to_nulls_last_everywhere() {
        assert_eq!(
            rewrite(
                "SELECT rank() OVER (ORDER BY a DESC) AS r, array_agg(b ORDER BY b DESC) AS g, \
                 sum(c) OVER w AS s FROM t WINDOW w AS (PARTITION BY p ORDER BY q) \
                 ORDER BY a DESC, b NULLS FIRST, c ASC"
            )
            .unwrap(),
            "SELECT CAST(rank() OVER (ORDER BY a DESC NULLS LAST) AS BIGINT) AS r, \
             array_agg(b ORDER BY b DESC NULLS LAST) AS g, sum(c) OVER w AS s FROM t \
             WINDOW w AS (PARTITION BY p ORDER BY q NULLS LAST) \
             ORDER BY a DESC NULLS LAST, b NULLS FIRST, c ASC NULLS LAST"
        );
        // Nested queries too.
        assert_eq!(
            rewrite("SELECT * FROM (SELECT a FROM t ORDER BY a DESC LIMIT 1) s").unwrap(),
            "SELECT * FROM (SELECT a FROM t ORDER BY a DESC NULLS LAST LIMIT 1) s"
        );
    }

    #[test]
    fn nested_anonymous_columns_and_values_get_col_n() {
        assert_eq!(
            rewrite("SELECT * FROM (SELECT 1, count(*), x AS \"Y\" FROM t) s").unwrap(),
            "SELECT * FROM (SELECT 1 AS _col0, count(*) AS _col1, x AS \"y\" FROM t) s"
        );
        assert_eq!(
            rewrite("SELECT * FROM (VALUES (1, 'a'), (2, 'b'))").unwrap(),
            "SELECT * FROM (SELECT column1 AS _col0, column2 AS _col1 FROM (VALUES (1, 'a'), (2, 'b')) AS __glaux_values)"
        );
        // Column aliases on the derived table still win.
        assert_eq!(
            rewrite("SELECT k FROM (VALUES (1)) AS t (k)").unwrap(),
            "SELECT k FROM (SELECT column1 AS _col0 FROM (VALUES (1)) AS __glaux_values) AS t (k)"
        );
    }

    #[test]
    fn null_sensitive_shims_wrap_their_calls() {
        assert_eq!(
            rewrite("SELECT greatest(a, b) AS g, least(a, b, c) AS l FROM t").unwrap(),
            "SELECT CASE WHEN (a IS NULL OR b IS NULL) THEN NULL ELSE greatest(a, b) END AS g, \
             CASE WHEN (a IS NULL OR b IS NULL OR c IS NULL) THEN NULL ELSE least(a, b, c) END AS l FROM t"
        );
        assert_eq!(
            rewrite("SELECT array_sort(a) AS s, reverse(a) AS r, contains(a, 1) AS c, arrays_overlap(a, b) AS o, split(s, ',') AS p FROM t").unwrap(),
            "SELECT array_sort(trino_array_element_sort_key(a), 'ASC', 'NULLS LAST') AS s, trino_reverse(a) AS r, trino_contains(a, 1) AS c, trino_arrays_overlap(a, b) AS o, trino_split(s, ',') AS p FROM t"
        );
        let err = rewrite("SELECT array_sort(a, (x, y) -> 1) FROM t").unwrap_err();
        assert!(err.to_string().contains("lambda"), "{err}");
    }

    #[test]
    fn non_trino_syntax_is_refused_by_name() {
        for (sql, construct) in [
            ("SELECT DISTINCT ON (a) a FROM t", "DISTINCT ON"),
            (
                "SELECT a FROM t QUALIFY row_number() OVER (ORDER BY a) = 1",
                "QUALIFY",
            ),
            ("SELECT a FROM t GROUP BY ALL", "GROUP BY ALL"),
            ("SELECT a FROM t TABLESAMPLE BERNOULLI (50)", "TABLESAMPLE"),
            ("SELECT a FROM t FOR UPDATE", "FOR UPDATE / FOR SHARE"),
            ("SELECT * FROM t NATURAL JOIN u", "NATURAL JOIN"),
            (
                "SELECT * FROM t LEFT SEMI JOIN u ON t.a = u.a",
                "SEMI / ANTI JOIN",
            ),
            (
                "SELECT * FROM t LEFT ANTI JOIN u ON t.a = u.a",
                "SEMI / ANTI JOIN",
            ),
            ("SELECT [1, 2]", "[...] array literal"),
            ("SELECT a IS TRUE FROM t", "IS TRUE"),
            ("SELECT a FROM t WHERE a IS NOT FALSE", "IS NOT FALSE"),
            ("SELECT a IS UNKNOWN FROM t", "IS UNKNOWN"),
            ("SELECT 1 EXCEPT ALL SELECT 1", "EXCEPT ALL"),
            ("SELECT 1 UNION BY NAME SELECT 1", "UNION BY NAME"),
            ("SELECT 1::INT", ":: cast"),
            ("SELECT TOP 1 a FROM t", "TOP"),
            (
                "SELECT TIMESTAMP '2024-01-05 10:00:00 America/New_York'",
                "timestamp with time zone literal",
            ),
            (
                "SELECT TIMESTAMP '2024-01-05 10:00:00+02:00'",
                "timestamp with time zone literal",
            ),
            (
                "SELECT TIMESTAMP '2024-01-05T10:00:00Z'",
                "timestamp with time zone literal",
            ),
            (
                "SELECT TIMESTAMP WITH TIME ZONE '2024-01-05 10:00:00'",
                "timestamp with time zone literal",
            ),
        ] {
            let err = rewrite(sql).unwrap_err();
            assert!(
                matches!(&err, GlauxSqlError::Unsupported { construct: c, .. } if c == construct),
                "{sql}: {err}"
            );
        }
        // Zone-less literals are fine.
        rewrite("SELECT TIMESTAMP '2024-01-05 10:00:00.123', DATE '2024-01-05', TIME '10:00:00'")
            .unwrap();
        assert!(!literal_has_zone("2024-01-05 10:00:00.123"));
        assert!(!literal_has_zone("10:00:00"));
        assert!(literal_has_zone("2024-01-05 10:00:00 UTC"));
        assert!(literal_has_zone("2024-01-05 10:00:00 -05:00"));
    }

    #[test]
    fn unary_minus_folds_into_integer_literals_only() {
        assert_eq!(
            rewrite("SELECT -9223372036854775808 AS a, -1 AS b, -(1) AS c, 3 -1 AS d, -1.5 AS e, -1e2 AS f, -x AS g FROM t")
                .unwrap(),
            "SELECT -9223372036854775808 AS a, -1 AS b, -(1) AS c, 3 - 1 AS d, -1.5 AS e, -trino_double('1e2') AS f, -x AS g FROM t"
        );
        // Beyond bigint either way: a decimal with the digit count of the
        // magnitude.
        assert_eq!(
            rewrite("SELECT -99999999999999999999 AS a").unwrap(),
            "SELECT CAST('-99999999999999999999' AS DECIMAL(20,0)) AS a"
        );
        // Set operations other than EXCEPT ALL keep their quantifiers.
        rewrite("SELECT 1 INTERSECT ALL SELECT 1").unwrap();
        rewrite("SELECT 1 EXCEPT SELECT 1").unwrap();
        rewrite("SELECT 1 EXCEPT DISTINCT SELECT 1").unwrap();
    }

    #[test]
    fn one_argument_log_is_refused() {
        let err = rewrite("SELECT log(100)").unwrap_err();
        assert!(err.to_string().contains("log(base, x)"), "{err}");
        rewrite("SELECT log(10, 100)").unwrap();
    }

    #[test]
    fn scalar_subqueries_are_wrapped_nullable() {
        assert_eq!(
            rewrite("SELECT (SELECT max(a) FROM t) AS m, b IN (SELECT a FROM t) AS i FROM u")
                .unwrap(),
            "SELECT trino_nullable((SELECT max(a) AS _col0 FROM t)) AS m, b IN (SELECT a FROM t) AS i FROM u"
        );
    }

    #[test]
    fn unsupported_and_unknown_functions_are_named() {
        let err = rewrite("SELECT repeat('a', 3)").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "function repeat"),
            "{err}"
        );
        let err = rewrite("SELECT frobnicate(1)").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::UnknownFunction { name } if name == "frobnicate"),
            "{err}"
        );
        // DataFusion-only names are not accepted either.
        let err = rewrite("SELECT array_element(a, 1) FROM t").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::UnknownFunction { name } if name == "array_element"),
            "{err}"
        );
        let err = rewrite("SELECT date_parse(s, fmt) FROM t").unwrap_err();
        assert!(err.to_string().contains("string literal"), "{err}");
        let err = rewrite("SELECT strpos(s, 'a', 2) FROM t").unwrap_err();
        assert!(err.to_string().contains("3-argument"), "{err}");
        let err = rewrite("SELECT if(x, 1, 2) OVER (PARTITION BY y) FROM t").unwrap_err();
        assert!(err.to_string().contains("OVER"), "{err}");
    }
}
