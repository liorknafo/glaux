//! Trino's operand-type rules, enforced on the planned query.
//!
//! DataFusion coerces freely: `'1' = 1` is true, `'a' || 1` is `'a1'`, and
//! `WHERE x = '1'` matches an integer column. Athena refuses all of these
//! with `TYPE_MISMATCH: Cannot apply operator: varchar = integer`, so a
//! query that works on glaux but fails on Athena (or, worse, matches
//! different rows) would be silently wrong. [`check`] walks the logical
//! plan DataFusion produced *before* its type-coercion pass and rejects
//! comparisons, arithmetic, concatenation, `IN` lists, and `BETWEEN` whose
//! operand classes Trino does not combine.
//!
//! The rules are deliberately the permissive end of Trino's: every
//! numeric type compares with every other numeric type, `date` with
//! `timestamp`, and `NULL` with anything. Only the combinations Trino
//! always refuses are refused here.

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
    if compatible(class(&left_type), class(&right_type), op) {
        return Ok(());
    }
    Err(GlauxSqlError::type_mismatch(format!(
        "Cannot apply operator: {} {} {}",
        trino_type_name(&left_type),
        operator_text(op),
        trino_type_name(&right_type)
    )))
}

fn check_expr(expr: &Expr, schema: &DFSchema) -> Result<(), GlauxSqlError> {
    match expr {
        Expr::BinaryExpr(binary) => check_operands(&binary.left, binary.op, &binary.right, schema),
        Expr::InList(in_list) => {
            for item in &in_list.list {
                check_operands(&in_list.expr, Operator::Eq, item, schema)?;
            }
            Ok(())
        }
        Expr::Between(between) => {
            check_operands(&between.expr, Operator::GtEq, &between.low, schema)?;
            check_operands(&between.expr, Operator::LtEq, &between.high, schema)
        }
        _ => Ok(()),
    }
}

/// Reject operator applications Trino refuses. Run on the freshly planned
/// (not yet analyzed/optimized) plan so operand types are still the
/// original ones.
pub fn check(plan: &LogicalPlan) -> Result<(), GlauxSqlError> {
    let mut failure: Option<GlauxSqlError> = None;
    let visit = plan.apply_with_subqueries(|node| {
        let schema = input_schema(node);
        node.apply_expressions(|expr| {
            expr.apply(|e| match check_expr(e, &schema) {
                Ok(()) => Ok(TreeNodeRecursion::Continue),
                Err(err) => {
                    failure = Some(err);
                    Ok(TreeNodeRecursion::Stop)
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
