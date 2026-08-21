//! AST rewriter: Trino function calls and constructs → DataFusion's.
//!
//! Runs as a post-order [`VisitorMut`] over the parsed statement so inner
//! calls are translated before the calls that wrap them. Every
//! `Expr::Function` is checked against the [registry](super::registry):
//! passthroughs and UDFs are left alone, rewrites are applied in place,
//! unsupported entries and unknown names abort the translation with an
//! error naming the function.

use std::ops::ControlFlow;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, CastKind, DataType, ExactNumberInfo, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Statement, TableFactor, Value, VisitMut, VisitorMut,
};

use super::error::GlauxSqlError;
use super::formats::{joda_to_chrono, mysql_to_chrono};
use super::registry::{self, ShimKind};

/// Rewrite `statement` in place. On error the statement is left partially
/// rewritten and must not be used.
pub fn rewrite_statement(statement: &mut Statement) -> Result<(), GlauxSqlError> {
    let mut visitor = Rewriter;
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(*err),
    }
}

struct Rewriter;

impl VisitorMut for Rewriter {
    type Break = Box<GlauxSqlError>;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match rewrite_expr(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(Box::new(e)),
        }
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        let refused = match table_factor {
            TableFactor::UNNEST { .. } => Some((
                "UNNEST",
                "`CROSS JOIN UNNEST(...)` is not supported in v0.1",
            )),
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
        Expr::Cast { data_type, .. } if is_row_or_map(data_type) => {
            Err(GlauxSqlError::unsupported(
                "ROW / MAP literals and types",
                "casting to ROW or MAP is not supported in v0.1",
            ))
        }
        _ => Ok(()),
    }
}

fn is_row_or_map(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(..) | DataType::Map(..) | DataType::Tuple(..)
    )
}

fn rewrite_function(expr: &mut Expr) -> Result<(), GlauxSqlError> {
    let Expr::Function(f) = expr else {
        unreachable!("rewrite_function called on a non-function expression");
    };
    let name = function_name(f)?;
    let Some(shim) = registry::lookup(&name) else {
        return Err(GlauxSqlError::UnknownFunction { name });
    };
    match shim.kind {
        ShimKind::Passthrough | ShimKind::Udf => {
            check_passthrough_arity(&name, f)?;
            Ok(())
        }
        ShimKind::Unsupported => Err(GlauxSqlError::unsupported(
            format!("function {name}"),
            shim.translation,
        )),
        ShimKind::Rewrite => {
            if let Some(replacement) = rewrite_call(&name, f)? {
                *expr = replacement;
            }
            Ok(())
        }
    }
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
        ("array_sort", 2) => Err(GlauxSqlError::invalid_arguments(
            name,
            "the comparator-lambda form is not supported",
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
            return simple_rename("string_to_array", &[2]);
        }
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
                mysql_to_chrono(name, &fmt)?
            } else {
                joda_to_chrono(name, &fmt)?
            };
            func("to_timestamp", vec![args[0].clone(), str_lit(&chrono)])
        }
        "date_format" | "format_datetime" => {
            arity(name, &args, &[2])?;
            let fmt = string_literal(name, "format", &args[1])?;
            let chrono = if name == "date_format" {
                mysql_to_chrono(name, &fmt)?
            } else {
                joda_to_chrono(name, &fmt)?
            };
            func("to_char", vec![args[0].clone(), str_lit(&chrono)])
        }
        "localtimestamp" => return simple_rename("now", &[0]),
        "from_iso8601_date" => return simple_rename("to_date", &[1]),
        "from_iso8601_timestamp" => return simple_rename("to_timestamp_millis", &[1]),
        "from_unixtime" => {
            if args.len() > 1 {
                return Err(GlauxSqlError::invalid_arguments(
                    name,
                    "the time-zone forms from_unixtime(x, zone) / from_unixtime(x, hours, \
                     minutes) are not supported",
                ));
            }
            arity(name, &args, &[1])?;
            let millis = cast_to(
                binary(
                    args.into_iter().next().unwrap(),
                    BinaryOperator::Multiply,
                    num_lit(1000),
                ),
                bigint(),
            );
            func(
                "arrow_cast",
                vec![millis, str_lit("Timestamp(Millisecond, None)")],
            )
        }
        "to_unixtime" => {
            arity(name, &args, &[1])?;
            let micros = func(
                "arrow_cast",
                vec![
                    func(
                        "arrow_cast",
                        vec![
                            args.into_iter().next().unwrap(),
                            str_lit("Timestamp(Microsecond, None)"),
                        ],
                    ),
                    str_lit("Int64"),
                ],
            );
            binary(
                cast_to(micros, double()),
                BinaryOperator::Divide,
                num_lit(1_000_000),
            )
        }
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
            // DataFusion: 0 = Sunday … 6 = Saturday. Trino: 1 = Monday … 7 = Sunday.
            let dow = date_part("dow", args.into_iter().next().unwrap());
            case_when(
                binary(dow.clone(), BinaryOperator::Eq, num_lit(0)),
                num_lit(7),
                Some(dow),
            )
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
        "rand" => return simple_rename("random", &[0]),
        "sign" => return simple_rename("signum", &[1]),
        "truncate" => return simple_rename("trunc", &[1]),
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
                        func("array_position", vec![array, args[1].clone()]),
                        num_lit(0),
                    ],
                )),
            )
        }
        "array_remove" => return simple_rename("array_remove_all", &[2]),
        "cardinality" => {
            arity(name, &args, &[1])?;
            cast_to(func("cardinality", args), bigint())
        }
        "contains" => return simple_rename("array_has", &[2]),
        "element_at" => return simple_rename("array_element", &[2]),
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
            "SELECT count(CASE WHEN x > 1 THEN 1 END) FILTER (WHERE y) OVER (PARTITION BY z) FROM t"
        );
        assert_eq!(
            rewrite("SELECT arbitrary(x), every(b), approx_percentile(v, 0.9) FROM t").unwrap(),
            "SELECT first_value(x), bool_and(b), approx_percentile_cont(v, 0.9) FROM t"
        );
    }

    #[test]
    fn nested_calls_are_rewritten_inside_out() {
        assert_eq!(
            rewrite("SELECT if(cardinality(split(s, ',')) > 1, 'many', 'one') FROM t").unwrap(),
            "SELECT CASE WHEN CAST(cardinality(string_to_array(s, ',')) AS BIGINT) > 1 THEN 'many' ELSE 'one' END FROM t"
        );
    }

    #[test]
    fn concat_becomes_null_propagating_operator() {
        assert_eq!(
            rewrite("SELECT concat(a, '-', b) FROM t").unwrap(),
            "SELECT a || '-' || b FROM t"
        );
    }

    #[test]
    fn regexp_shims_add_global_flag_and_group_wrapping() {
        assert_eq!(
            rewrite("SELECT regexp_replace(s, 'a+'), regexp_extract(s, '\\d+'), regexp_extract(s, '(a)(b)', 2) FROM t").unwrap(),
            "SELECT regexp_replace(s, 'a+', '', 'g'), array_element(regexp_match(s, '(\\d+)'), 1), array_element(regexp_match(s, '((a)(b))'), 3) FROM t"
        );
    }

    #[test]
    fn date_functions_translate_formats_and_units() {
        assert_eq!(
            rewrite("SELECT date_parse(s, '%Y-%m-%d %H:%i:%s'), format_datetime(ts, 'yyyy-MM-dd') FROM t").unwrap(),
            "SELECT to_timestamp(s, '%Y-%m-%d %H:%M:%S'), to_char(ts, '%Y-%m-%d') FROM t"
        );
        assert_eq!(
            rewrite("SELECT day_of_week(d), month(d) FROM t").unwrap(),
            "SELECT CASE WHEN CAST(date_part('dow', d) AS BIGINT) = 0 THEN 7 ELSE CAST(date_part('dow', d) AS BIGINT) END, CAST(date_part('month', d) AS BIGINT) FROM t"
        );
        assert_eq!(
            rewrite("SELECT from_unixtime(t), to_unixtime(ts) FROM t").unwrap(),
            "SELECT arrow_cast(CAST(t * 1000 AS BIGINT), 'Timestamp(Millisecond, None)'), CAST(arrow_cast(arrow_cast(ts, 'Timestamp(Microsecond, None)'), 'Int64') AS DOUBLE) / 1000000 FROM t"
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
