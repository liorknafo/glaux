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
    AccessExpr, BinaryOperator, CaseWhen, CastKind, CharacterLength, DataType, DateTimeField,
    Distinct, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArgumentList, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, NamedWindowExpr, ObjectName, ObjectNamePart, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement, Subscript, TableAlias, TableFactor,
    TimezoneInfo, TypedString, UnaryOperator, Value, VisitMut, VisitorMut, WindowType,
};

use super::error::GlauxSqlError;
use super::formats::{Direction, joda_to_chrono, mysql_to_chrono};
use super::naming;
use super::registry::{self, ShimKind};

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
            } => fold_alias(alias),
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
        for cte in &mut with.cte_tables {
            fold_alias(&mut cte.alias);
        }
    }
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
    rewrite_set_expr(&mut query.body)
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
        SetExpr::SetOperation { left, right, .. } => {
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

/// Refuse `TIMESTAMP '... <zone>'` literals: DataFusion would silently
/// convert the instant to UTC and drop the zone, while `AT TIME ZONE` and
/// zoned arithmetic are refused, so the value could never be interpreted
/// the way the query meant it.
fn check_typed_string(typed: &TypedString) -> Result<(), GlauxSqlError> {
    let zoned_type = matches!(
        typed.data_type,
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz)
            | DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz)
    );
    let text = match &typed.value.value {
        Value::SingleQuotedString(s) => s.as_str(),
        _ => "",
    };
    let has_zone_suffix = matches!(
        typed.data_type,
        DataType::Timestamp(..) | DataType::Time(..)
    ) && literal_has_zone(text);
    if zoned_type || has_zone_suffix {
        return Err(GlauxSqlError::unsupported(
            "timestamp with time zone literal",
            format!(
                "`{typed}` carries a time zone; glaux handles timestamps as zone-less UTC \
                 instants in v0.1"
            ),
        ));
    }
    Ok(())
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
            *expr = func("now", vec![]);
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
            *expr = cast_to(str_lit(text), double());
            Ok(())
        }
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
        Expr::TypedString(typed) => check_typed_string(typed),
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

fn is_exponent_literal(value: &Value) -> bool {
    matches!(value, Value::Number(text, _) if text.contains(['e', 'E']))
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
        DataType::Varchar(length)
        | DataType::CharacterVarying(length)
        | DataType::Char(length)
        | DataType::Character(length) => {
            let limit = match length {
                None | Some(CharacterLength::Max) => None,
                Some(CharacterLength::IntegerLength { length, .. }) => Some(*length),
            };
            let mut args = vec![take(inner)];
            if let Some(n) = limit {
                args.push(num_lit(n as i64));
            }
            // TRY_CAST(x AS VARCHAR) cannot fail for castable types; the
            // type errors it would mask are refused at plan time either way.
            let _ = kind;
            *expr = func("trino_varchar", args);
            Ok(())
        }
        DataType::String(_) | DataType::Text => {
            *expr = func("trino_varchar", vec![take(inner)]);
            Ok(())
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

/// Passthroughs whose Trino overloads go beyond what DataFusion implements.
fn check_passthrough_arity(name: &str, f: &Function) -> Result<(), GlauxSqlError> {
    let count = match &f.args {
        FunctionArguments::List(list) => list.args.len(),
        _ => return Ok(()),
    };
    match (name, count) {
        ("strpos", 3) => Err(GlauxSqlError::invalid_arguments(
            name,
            "the 3-argument form strpos(string, substring, instance) is not supported",
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
            rename(f, "approx_percentile_cont");
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
        "codepoint" => return simple_rename("ascii", &[1]),
        "replace" => {
            arity(name, &args, &[2, 3])?;
            let mut full = args.clone();
            if full.len() == 2 {
                full.push(str_lit(""));
            }
            func("replace", full)
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
            let mut it = args.into_iter();
            let (s, p) = (it.next().unwrap(), it.next().unwrap());
            let r = it.next().unwrap_or_else(|| str_lit(""));
            if matches!(r, Expr::Lambda(_)) {
                return Err(GlauxSqlError::unsupported(
                    "lambda expression",
                    "regexp_replace with a lambda replacement is not translated",
                ));
            }
            // Java replacement syntax ($1, \$) → Rust regex syntax.
            let r = func("trino_regexp_replacement", vec![r]);
            func("regexp_replace", vec![s, p, r, str_lit("g")])
        }
        "regexp_extract" => {
            arity(name, &args, &[2, 3])?;
            let group = match args.get(2) {
                Some(g) => integer_literal(name, "group", g)?,
                None => 0,
            };
            let wrapped = match &args[1] {
                Expr::Value(v) if matches!(v.value, Value::SingleQuotedString(_)) => {
                    let Value::SingleQuotedString(p) = &v.value else {
                        unreachable!()
                    };
                    str_lit(&format!("({p})"))
                }
                other => binary(
                    binary(str_lit("("), BinaryOperator::StringConcat, other.clone()),
                    BinaryOperator::StringConcat,
                    str_lit(")"),
                ),
            };
            func(
                "array_element",
                vec![
                    func("regexp_match", vec![args[0].clone(), wrapped]),
                    num_lit(group + 1),
                ],
            )
        }
        // Date and time
        "date" => {
            arity(name, &args, &[1])?;
            cast_to(args.into_iter().next().unwrap(), DataType::Date)
        }
        "date_parse" | "parse_datetime" => {
            arity(name, &args, &[2])?;
            let fmt = string_literal(name, "format", &args[1])?;
            let chrono = if name == "date_parse" {
                mysql_to_chrono(name, &fmt, Direction::Parse)?
            } else {
                joda_to_chrono(name, &fmt, Direction::Parse)?
            };
            func("to_timestamp", vec![args[0].clone(), str_lit(&chrono)])
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
        "localtimestamp" => return simple_rename("now", &[0]),
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
            // `...:02.000`); a plain cast would truncate.
            let millis = cast_to(
                func(
                    "round",
                    vec![binary(
                        args.into_iter().next().unwrap(),
                        BinaryOperator::Multiply,
                        num_lit(1000),
                    )],
                ),
                bigint(),
            );
            func(
                "arrow_cast",
                vec![millis, str_lit("Timestamp(Millisecond, None)")],
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
        "ceiling" => return simple_rename("ceil", &[1]),
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
        "sign" => return simple_rename("signum", &[1]),
        "truncate" => return simple_rename("trunc", &[1, 2]),
        // Arrays
        "array_join" => return simple_rename("array_to_string", &[2, 3]),
        "array_position" => {
            arity(name, &args, &[2])?;
            let array = args[0].clone();
            case_when(
                Expr::IsNull(Box::new(array.clone())),
                Expr::Value(Value::Null.with_empty_span()),
                Some(func(
                    "coalesce",
                    vec![
                        cast_to(
                            func("array_position", vec![array, args[1].clone()]),
                            bigint(),
                        ),
                        num_lit(0),
                    ],
                )),
            )
        }
        "array_remove" => return simple_rename("array_remove_all", &[2]),
        "array_sort" => {
            if args.len() == 2 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "the comparator-lambda form is not supported",
                ));
            }
            arity(name, &args, &[1])?;
            // Trino sorts NULL elements last; DataFusion's default is first.
            let mut full = args;
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
            "SELECT first_value(x) AS a, bool_and(b) AS e, approx_percentile_cont(v, 0.9) AS p FROM t"
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
    fn regexp_shims_add_global_flag_and_group_wrapping() {
        assert_eq!(
            rewrite("SELECT regexp_replace(s, 'a+') AS a, regexp_extract(s, '\\d+') AS b, regexp_extract(s, '(a)(b)', 2) AS c FROM t").unwrap(),
            "SELECT regexp_replace(s, 'a+', trino_regexp_replacement(''), 'g') AS a, array_element(regexp_match(s, '(\\d+)'), 1) AS b, array_element(regexp_match(s, '((a)(b))'), 3) AS c FROM t"
        );
    }

    #[test]
    fn date_functions_translate_formats_and_units() {
        assert_eq!(
            rewrite("SELECT date_parse(s, '%Y-%m-%d %H:%i:%s') AS a, format_datetime(ts, 'yyyy-MM-dd') AS b FROM t").unwrap(),
            "SELECT to_timestamp(s, '%Y-%m-%d %H:%M:%S') AS a, to_char(ts, '%Y-%m-%d') AS b FROM t"
        );
        assert_eq!(
            rewrite("SELECT day_of_week(d) AS a, month(d) AS b FROM t").unwrap(),
            "SELECT CASE WHEN CAST(date_part('dow', d) AS BIGINT) = 0 THEN 7 ELSE CAST(date_part('dow', d) AS BIGINT) END AS a, CAST(date_part('month', d) AS BIGINT) AS b FROM t"
        );
        assert_eq!(
            rewrite("SELECT from_unixtime(t) AS a, to_unixtime(ts) AS b, date_trunc('day', ts) AS c FROM t").unwrap(),
            "SELECT arrow_cast(CAST(round(t * 1000) AS BIGINT), 'Timestamp(Millisecond, None)') AS a, trino_to_unixtime(ts) AS b, trino_date_trunc('day', ts) AS c FROM t"
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
            "SELECT CAST('1e2' AS DOUBLE) AS a, 1.5 AS b, 10 AS c"
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
            "SELECT array_sort(a, 'ASC', 'NULLS LAST') AS s, trino_reverse(a) AS r, trino_contains(a, 1) AS c, trino_arrays_overlap(a, b) AS o, trino_split(s, ',') AS p FROM t"
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
