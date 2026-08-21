//! Trino's operand-type rules, enforced on the planned query.
//!
//! DataFusion coerces freely: `'1' = 1` is true, `'a' || 1` is `'a1'`, and
//! `WHERE x = '1'` matches an integer column. Athena refuses all of these
//! with `TYPE_MISMATCH: Cannot apply operator: varchar = integer`, so a
//! query that works on glaux but fails on Athena (or, worse, matches
//! different rows) would be silently wrong. [`check`] walks the logical
//! plan DataFusion produced *before* its type-coercion pass and rejects
//! comparisons, arithmetic, concatenation, `LIKE` over non-varchar
//! operands, `IN` lists and subqueries,
//! `BETWEEN`, join conditions (`ON` and `USING`), simple `CASE` operands
//! and `CASE` / `if` results, `nullif` / `coalesce` / `greatest` / `least`
//! arguments, set-operation columns, and varchar arguments to the
//! date-part functions, wherever the operand classes are ones Trino does
//! not combine.
//!
//! The rules are deliberately the permissive end of Trino's: every
//! numeric type compares with every other numeric type, `date` with
//! `timestamp`, and `NULL` with anything. Only the combinations Trino
//! always refuses are refused here.
//!
//! `date - date` and `timestamp - timestamp` are refused too, for a
//! different reason: Trino returns an `interval`, which glaux cannot carry
//! in v0.1 (DataFusion would return a bigint day count or a duration).
//! Comparisons between intervals are refused as well: Trino compares the
//! normalised value (`INTERVAL '1' DAY = INTERVAL '24' HOUR` is true),
//! DataFusion the month/day/nanosecond triple.

use arrow::datatypes::DataType;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};

use super::error::GlauxSqlError;
use super::udf::arithmetic::input_schema;
use super::udf::casts::trino_type_name;

/// Coarse type classes; Trino only mixes classes in the cases listed in
/// [`compatible`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Null,
    String,
    Number,
    Boolean,
    Date,
    Timestamp,
    Time,
    Interval,
    Binary,
    Array,
    Other,
}

fn class(data_type: &DataType) -> Class {
    match data_type {
        DataType::Null => Class::Null,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Class::String,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Class::Number,
        DataType::Boolean => Class::Boolean,
        DataType::Date32 | DataType::Date64 => Class::Date,
        DataType::Timestamp(_, _) => Class::Timestamp,
        DataType::Time32(_) | DataType::Time64(_) => Class::Time,
        DataType::Interval(_) | DataType::Duration(_) => Class::Interval,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Class::Binary,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => Class::Array,
        DataType::Dictionary(_, value) => class(value),
        _ => Class::Other,
    }
}

/// Whether Trino applies `op` to operands of these classes.
fn compatible(left: Class, right: Class, op: Operator) -> bool {
    use Class::*;
    if left == Null || right == Null || left == Other || right == Other {
        return true;
    }
    match op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => {
            left == right || matches!((left, right), (Date, Timestamp) | (Timestamp, Date))
        }
        Operator::Plus => matches!(
            (left, right),
            (Number, Number)
                | (Date | Timestamp | Time, Interval)
                | (Interval, Date | Timestamp | Time)
                | (Interval, Interval)
        ),
        Operator::Minus => matches!(
            (left, right),
            (Number, Number)
                | (Date | Timestamp | Time, Interval)
                | (Interval, Interval)
                | (Date, Date)
                | (Timestamp, Timestamp)
                | (Timestamp, Date)
                | (Date, Timestamp)
        ),
        Operator::Multiply => matches!(
            (left, right),
            (Number, Number) | (Interval, Number) | (Number, Interval)
        ),
        Operator::Divide => matches!((left, right), (Number, Number) | (Interval, Number)),
        Operator::Modulo => matches!((left, right), (Number, Number)),
        Operator::StringConcat => {
            matches!((left, right), (String, String)) || left == Array || right == Array
        }
        _ => true,
    }
}

/// Whether Trino compares values of these two types (`=`, `IN`, `CASE x
/// WHEN`, `contains`, ...).
pub(crate) fn comparable(left: &DataType, right: &DataType) -> bool {
    compatible(class(left), class(right), Operator::Eq)
}

/// The Trino name of a DataFusion function the rewriter emitted.
fn trino_function_name(name: &str) -> &str {
    match name {
        "character_length" => "length",
        "levenshtein" => "levenshtein_distance",
        "to_timestamp" => "date_parse",
        other => other,
    }
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn operator_text(op: Operator) -> String {
    match op {
        Operator::IsDistinctFrom => "IS DISTINCT FROM".to_string(),
        Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM".to_string(),
        other => other.to_string(),
    }
}

fn check_operands(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &DFSchema,
) -> Result<(), GlauxSqlError> {
    // If a type cannot be resolved here, DataFusion's own planner will
    // report it; this pass only refuses what it can prove.
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return Ok(());
    };
    check_types(&left_type, op, &right_type)
}

fn check_types(left: &DataType, op: Operator, right: &DataType) -> Result<(), GlauxSqlError> {
    let (l, r) = (class(left), class(right));
    if op == Operator::Minus
        && matches!(l, Class::Date | Class::Timestamp)
        && matches!(r, Class::Date | Class::Timestamp)
    {
        return Err(GlauxSqlError::unsupported(
            "date subtraction",
            format!(
                "`{} - {}` produces an INTERVAL in Trino, which glaux cannot return in v0.1; \
                 use date_diff(unit, a, b)",
                trino_type_name(left),
                trino_type_name(right)
            ),
        ));
    }
    if is_comparison(op) && (l == Class::Interval || r == Class::Interval) {
        // Trino compares a day-to-second interval by its total milliseconds
        // and a year-to-month one by its total months (`INTERVAL '1' DAY =
        // INTERVAL '24' HOUR` is true) and refuses to mix the two kinds;
        // DataFusion compares its month/day/nanosecond triple structurally,
        // so the same query would be false. Comparison operators on
        // intervals are refused rather than answered differently.
        return Err(GlauxSqlError::unsupported(
            "interval comparison",
            format!(
                "`{} {} {}`: Trino compares intervals by their normalised value (day-to-second \
                 in milliseconds, year-to-month in months), which DataFusion's structural \
                 interval comparison does not reproduce; compare the dates or timestamps the \
                 intervals are applied to, or use date_diff",
                trino_type_name(left),
                operator_text(op),
                trino_type_name(right)
            ),
        ));
    }
    if compatible(l, r, op) {
        return Ok(());
    }
    Err(GlauxSqlError::type_mismatch(format!(
        "Cannot apply operator: {} {} {}",
        trino_type_name(left),
        operator_text(op),
        trino_type_name(right)
    )))
}

/// Trino requires the results of `CASE` / `if` / `coalesce` / `nullif` /
/// `greatest` / `least` to share a type; `what` names the construct in the
/// diagnostic.
fn check_common_type(what: &str, exprs: &[&Expr], schema: &DFSchema) -> Result<(), GlauxSqlError> {
    let mut types: Vec<DataType> = Vec::new();
    for e in exprs {
        if let Ok(t) = e.get_type(schema) {
            types.push(t);
        }
    }
    let Some(first) = types.iter().find(|t| class(t) != Class::Null) else {
        return Ok(());
    };
    for t in &types {
        if !comparable(first, t) {
            return Err(GlauxSqlError::type_mismatch(format!(
                "All {what} must be the same type or coercible to a common type. Cannot find \
                 common type between {} and {}",
                trino_type_name(first),
                trino_type_name(t)
            )));
        }
    }
    Ok(())
}

/// DataFusion functions the rewriter emits for Trino's date-part functions
/// (`year(x)`, `date_format(x, ...)`, `EXTRACT`); Trino refuses varchar
/// arguments where DataFusion parses them.
fn date_argument(name: &str) -> Option<usize> {
    match name {
        "date_part" => Some(1),
        "to_char" => Some(0),
        _ => None,
    }
}

/// DataFusion string functions the registry passes through, with the
/// argument positions Trino types as `varchar`. DataFusion would stringify
/// a number or a date there (`length(123)` is 3); Trino has no such
/// signature.
fn string_arguments(name: &str) -> &'static [usize] {
    match name {
        "character_length" => &[0],
        "starts_with" | "strpos" | "levenshtein" | "regexp_match" => &[0, 1],
        "replace" | "translate" => &[0, 1, 2],
        // `date_parse(x, fmt)`: the parsed text must be a varchar.
        "to_timestamp" => &[0],
        _ => &[],
    }
}

/// Whether Trino has a cast between these type classes (varchar sources are
/// validated at run time; the pairs here are the ones Trino refuses at
/// planning, such as `CAST(DATE ... AS BIGINT)` or `CAST(12 AS DATE)`).
fn castable(source: Class, target: Class) -> bool {
    use Class::*;
    matches!(
        (source, target),
        (Null | Other, _)
            | (_, Other)
            | (String, _)
            | (Number, Number | String | Boolean)
            | (Boolean, Boolean | Number | String)
            | (Date, Date | Timestamp | String)
            | (Timestamp, Timestamp | Date | Time | String)
            | (Time, Time | String)
            | (Interval, Interval | String)
            | (Array, Array)
            | (Binary, Binary)
    )
}

fn check_cast(source: &Expr, target: &DataType, schema: &DFSchema) -> Result<(), GlauxSqlError> {
    let Ok(source_type) = source.get_type(schema) else {
        return Ok(());
    };
    if castable(class(&source_type), class(target)) {
        return Ok(());
    }
    Err(GlauxSqlError::type_mismatch(format!(
        "Cannot cast {} to {}",
        trino_type_name(&source_type),
        trino_type_name(target)
    )))
}

fn check_expr(expr: &Expr, schema: &DFSchema) -> Result<(), GlauxSqlError> {
    match expr {
        Expr::BinaryExpr(binary) => check_operands(&binary.left, binary.op, &binary.right, schema),
        // Trino types both sides of `LIKE` as varchar; DataFusion's
        // type-coercion pass would fail later with its own planner text
        // ("There isn't a common type to coerce Int32 and Utf8 in LIKE
        // expression"), so refuse here with Trino's diagnostic.
        Expr::Like(like) | Expr::SimilarTo(like) => {
            if let Ok(t) = like.expr.get_type(schema)
                && !matches!(class(&t), Class::String | Class::Null)
            {
                return Err(GlauxSqlError::type_mismatch(format!(
                    "Left side of LIKE expression must evaluate to a varchar (actual: {})",
                    trino_type_name(&t)
                )));
            }
            if let Ok(t) = like.pattern.get_type(schema)
                && !matches!(class(&t), Class::String | Class::Null)
            {
                return Err(GlauxSqlError::type_mismatch(format!(
                    "Pattern for LIKE expression must evaluate to a varchar (actual: {})",
                    trino_type_name(&t)
                )));
            }
            Ok(())
        }
        Expr::InList(in_list) => {
            for item in &in_list.list {
                check_operands(&in_list.expr, Operator::Eq, item, schema)?;
            }
            Ok(())
        }
        Expr::InSubquery(in_subquery) => {
            let Ok(left) = in_subquery.expr.get_type(schema) else {
                return Ok(());
            };
            let Some(field) = in_subquery.subquery.subquery.schema().fields().first() else {
                return Ok(());
            };
            check_types(&left, Operator::Eq, field.data_type()).map_err(|_| {
                GlauxSqlError::type_mismatch(format!(
                    "value and result of subquery must be of the same type for IN expression: \
                     {} vs {}",
                    trino_type_name(&left),
                    trino_type_name(field.data_type())
                ))
            })?;
            // Trino evaluates `IN (subquery)` with the EQUAL operator (NaN
            // matches nothing); DataFusion decorrelates it into a hash
            // semi-join whose key equality is Arrow's (NaN matches NaN),
            // and the join is built after this check runs, so a float
            // operand is refused rather than risked.
            if matches!(
                left,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) || matches!(
                field.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) {
                return Err(GlauxSqlError::unsupported(
                    "IN (subquery) over DOUBLE / REAL",
                    "Trino compares double values with IEEE equality (NaN never matches), which \
                     DataFusion's semi-join does not reproduce; use a JOIN with an explicit ON \
                     equality instead",
                ));
            }
            Ok(())
        }
        Expr::Between(between) => {
            check_operands(&between.expr, Operator::GtEq, &between.low, schema)?;
            check_operands(&between.expr, Operator::LtEq, &between.high, schema)
        }
        Expr::Case(case) => {
            if let Some(operand) = &case.expr {
                for (when, _) in &case.when_then_expr {
                    check_operands(operand, Operator::Eq, when, schema)?;
                }
            }
            let mut results: Vec<&Expr> = case
                .when_then_expr
                .iter()
                .map(|(_, t)| t.as_ref())
                .collect();
            if let Some(otherwise) = &case.else_expr {
                results.push(otherwise);
            }
            check_common_type("CASE results", &results, schema)
        }
        Expr::Cast(cast) => check_cast(&cast.expr, cast.field.data_type(), schema),
        Expr::TryCast(cast) => check_cast(&cast.expr, cast.field.data_type(), schema),
        Expr::ScalarFunction(call) => {
            let name = call.func.name();
            match name {
                "nullif" | "coalesce" | "greatest" | "least" => {
                    let args: Vec<&Expr> = call.args.iter().collect();
                    check_common_type(&format!("{} operands", name.to_uppercase()), &args, schema)
                }
                _ => {
                    if let Some(index) = date_argument(name)
                        && let Some(arg) = call.args.get(index)
                        && let Ok(t) = arg.get_type(schema)
                        && !matches!(
                            class(&t),
                            Class::Date
                                | Class::Timestamp
                                | Class::Time
                                | Class::Interval
                                | Class::Null
                        )
                    {
                        return Err(GlauxSqlError::type_mismatch(format!(
                            "Unexpected parameters ({}) for date/time function: expected date, \
                             timestamp, or interval (Trino does not parse varchar here; use \
                             date_parse or CAST)",
                            trino_type_name(&t)
                        )));
                    }
                    for index in string_arguments(name) {
                        if let Some(arg) = call.args.get(*index)
                            && let Ok(t) = arg.get_type(schema)
                            && !matches!(class(&t), Class::String | Class::Null)
                            && !(name == "character_length" && class(&t) == Class::Binary)
                        {
                            return Err(GlauxSqlError::type_mismatch(format!(
                                "Unexpected parameters ({}) for function {}: expected varchar \
                                 (Trino does not convert {} to varchar implicitly; use CAST)",
                                trino_type_name(&t),
                                trino_function_name(name),
                                trino_type_name(&t)
                            )));
                        }
                    }
                    Ok(())
                }
            }
        }
        _ => Ok(()),
    }
}

/// Plan-level checks: join conditions and set-operation columns.
fn check_plan_node(node: &LogicalPlan, schema: &DFSchema) -> Result<(), GlauxSqlError> {
    match node {
        LogicalPlan::Join(join) => {
            for (left, right) in &join.on {
                check_operands(left, Operator::Eq, right, schema)?;
            }
            Ok(())
        }
        LogicalPlan::Union(union) => {
            let Some(first) = union.inputs.first() else {
                return Ok(());
            };
            for other in union.inputs.iter().skip(1) {
                for (i, (a, b)) in first
                    .schema()
                    .fields()
                    .iter()
                    .zip(other.schema().fields())
                    .enumerate()
                {
                    if !comparable(a.data_type(), b.data_type()) {
                        return Err(GlauxSqlError::type_mismatch(format!(
                            "column {} in UNION query has incompatible types: {}, {}",
                            i + 1,
                            trino_type_name(a.data_type()),
                            trino_type_name(b.data_type())
                        )));
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// `IN (subquery)` / `EXISTS` used as a *value* (in the select list, a sort
/// key, a join condition — anywhere but a `WHERE` / `HAVING` predicate,
/// which plans as a `Filter`): DataFusion cannot evaluate the expression
/// (`Physical plan does not support logical expression InSubquery`), so it
/// is refused by name here instead of leaking that error.
fn check_subquery_position(node: &LogicalPlan, expr: &Expr) -> Result<(), GlauxSqlError> {
    if matches!(node, LogicalPlan::Filter(_)) {
        return Ok(());
    }
    let construct = match expr {
        Expr::InSubquery(_) => "IN (subquery) as a value",
        Expr::Exists(_) => "EXISTS as a value",
        _ => return Ok(()),
    };
    Err(GlauxSqlError::unsupported(
        construct,
        "DataFusion only decorrelates IN / EXISTS subqueries used as WHERE / HAVING predicates; \
         use them there, or rewrite with a JOIN",
    ))
}

/// Reject operator applications Trino refuses. Run on the freshly planned
/// (not yet analyzed/optimized) plan so operand types are still the
/// original ones.
pub fn check(plan: &LogicalPlan) -> Result<(), GlauxSqlError> {
    let mut failure: Option<GlauxSqlError> = None;
    let visit = plan.apply_with_subqueries(|node| {
        let schema = input_schema(node);
        if let Err(err) = check_plan_node(node, &schema) {
            failure = Some(err);
            return Ok(TreeNodeRecursion::Stop);
        }
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                let checked =
                    check_subquery_position(node, e).and_then(|()| check_expr(e, &schema));
                match checked {
                    Ok(()) => Ok(TreeNodeRecursion::Continue),
                    Err(err) => {
                        failure = Some(err);
                        Ok(TreeNodeRecursion::Stop)
                    }
                }
            })
        })
    });
    // The closure never returns a DataFusion error.
    debug_assert!(visit.is_ok());
    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cast_matrix_follows_trino() {
        use Class::*;
        assert!(castable(String, Timestamp));
        assert!(castable(Number, String));
        assert!(castable(Number, Boolean));
        assert!(castable(Date, Timestamp));
        assert!(castable(Timestamp, Date));
        assert!(!castable(Number, Date));
        assert!(!castable(Number, Timestamp));
        assert!(!castable(Date, Number));
        assert!(!castable(Timestamp, Number));
        assert!(!castable(Boolean, Date));
        assert!(castable(Null, Date));
    }

    #[test]
    fn class_compatibility_follows_trino() {
        use Class::*;
        assert!(compatible(Number, Number, Operator::Eq));
        assert!(compatible(Date, Timestamp, Operator::Lt));
        assert!(compatible(Null, String, Operator::Eq));
        assert!(!compatible(String, Number, Operator::Eq));
        assert!(!compatible(Date, String, Operator::Eq));
        assert!(!compatible(Boolean, Number, Operator::Eq));
        assert!(compatible(Timestamp, Interval, Operator::Plus));
        assert!(compatible(Timestamp, Timestamp, Operator::Minus));
        assert!(!compatible(Timestamp, Timestamp, Operator::Plus));
        assert!(!compatible(String, Number, Operator::Plus));
        assert!(compatible(String, String, Operator::StringConcat));
        assert!(compatible(Array, Number, Operator::StringConcat));
        assert!(!compatible(String, Number, Operator::StringConcat));
        assert!(!compatible(Number, Number, Operator::StringConcat));
        assert!(compatible(Interval, Number, Operator::Multiply));
        assert!(!compatible(Number, Interval, Operator::Divide));
    }
}
