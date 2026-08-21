//! Overflow-checked integer arithmetic.
//!
//! DataFusion evaluates `bigint` `+`, `-`, `*`, and `sum` with wrapping
//! arithmetic, so `9223372036854775807 + 1` quietly becomes
//! `-9223372036854775808`. Trino raises `NUMERIC_VALUE_OUT_OF_RANGE`. The
//! [`CheckedIntegerArithmetic`] analyzer rule runs after DataFusion's type
//! coercion (so operand types are known) and replaces every integer-typed
//! `+ - *` with the checked scalar UDFs here, and every integer `sum` with
//! the checked [`CheckedIntSum`] aggregate. Output column names are
//! preserved, so the substitution is invisible except when it fails.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use arrow::error::ArrowError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, ExprSchemable, LogicalPlan,
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    WindowFunctionDefinition,
};
use datafusion::optimizer::analyzer::AnalyzerRule;

use super::{data_error, is_integer};

/// The checked scalar UDFs (`trino_checked_add` / `_sub` / `_mul`).
pub fn scalar_udfs() -> Vec<ScalarUDF> {
    [Operator::Plus, Operator::Minus, Operator::Multiply]
        .into_iter()
        .map(|op| ScalarUDF::new_from_impl(CheckedArithmetic::new(op)))
        .collect()
}

/// The checked aggregate UDFs (`trino_checked_sum`).
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::new_from_impl(CheckedIntSum::new())]
}

fn overflow(op: &str) -> DataFusionError {
    data_error(
        "NUMERIC_VALUE_OUT_OF_RANGE",
        format!("bigint {op} overflow"),
    )
}

/// `trino_checked_add(a, b)` etc.: Arrow's checked kernels, which error on
/// overflow instead of wrapping.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedArithmetic {
    signature: Signature,
    op: Operator,
}

impl CheckedArithmetic {
    /// New instance for `+`, `-`, or `*`.
    pub fn new(op: Operator) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            op,
        }
    }

    fn verb(&self) -> &'static str {
        match self.op {
            Operator::Plus => "addition",
            Operator::Minus => "subtraction",
            _ => "multiplication",
        }
    }
}

impl ScalarUDFImpl for CheckedArithmetic {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "trino_checked_add",
            Operator::Minus => "trino_checked_sub",
            _ => "trino_checked_mul",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let kernel = match self.op {
            Operator::Plus => numeric::add,
            Operator::Minus => numeric::sub,
            _ => numeric::mul,
        };
        match kernel(&lhs, &rhs) {
            Ok(array) => Ok(ColumnarValue::Array(array)),
            Err(ArrowError::ArithmeticOverflow(_)) => Err(overflow(self.verb())),
            Err(e) => Err(e.into()),
        }
    }
}

/// `trino_checked_sum(bigint)`: `sum` that fails on overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedIntSum {
    signature: Signature,
}

impl Default for CheckedIntSum {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckedIntSum {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for CheckedIntSum {
    fn name(&self) -> &str {
        "trino_checked_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(if args.is_distinct {
            Box::new(DistinctCheckedSum::default())
        } else {
            Box::new(CheckedSum::default())
        })
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(if args.is_distinct {
            vec![Arc::new(Field::new_list(
                format!("{}[checked sum distinct]", args.name),
                Field::new_list_field(DataType::Int64, true),
                false,
            ))]
        } else {
            vec![Arc::new(Field::new(
                format!("{}[checked sum]", args.name),
                DataType::Int64,
                true,
            ))]
        })
    }

    fn create_sliding_accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CheckedSum::default()))
    }
}

/// Running checked sum; `None` until a non-null value arrives (so an
/// all-null input sums to `NULL`, like Trino).
#[derive(Debug, Default)]
struct CheckedSum {
    sum: Option<i64>,
}

impl CheckedSum {
    fn add(&mut self, value: i64) -> Result<()> {
        self.sum = Some(
            self.sum
                .unwrap_or(0)
                .checked_add(value)
                .ok_or_else(|| overflow("sum"))?,
        );
        Ok(())
    }
}

impl Accumulator for CheckedSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in values[0].as_primitive::<Int64Type>().iter().flatten() {
            self.add(v)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(self.sum))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(self.sum)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in values[0].as_primitive::<Int64Type>().iter().flatten() {
            self.sum = Some(
                self.sum
                    .unwrap_or(0)
                    .checked_sub(v)
                    .ok_or_else(|| overflow("sum"))?,
            );
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// `sum(DISTINCT bigint)` with overflow checking at evaluation.
#[derive(Debug, Default)]
struct DistinctCheckedSum {
    values: HashSet<i64>,
}

impl Accumulator for DistinctCheckedSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values
            .extend(values[0].as_primitive::<Int64Type>().iter().flatten());
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Int64(None));
        }
        let mut sum = 0i64;
        for v in &self.values {
            sum = sum.checked_add(*v).ok_or_else(|| overflow("sum"))?;
        }
        Ok(ScalarValue::Int64(Some(sum)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<i64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &values,
            &DataType::Int64,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for list in states[0].as_list::<i32>().iter().flatten() {
            self.values
                .extend(list.as_primitive::<Int64Type>().iter().flatten());
        }
        Ok(())
    }
}

/// Analyzer rule: route integer `+ - *` and `sum` through the checked UDFs.
#[derive(Debug, Default)]
pub struct CheckedIntegerArithmetic;

fn checked_scalar(op: Operator) -> ScalarUDF {
    ScalarUDF::new_from_impl(CheckedArithmetic::new(op))
}

/// The merged schema of a node's inputs, which is what its expressions
/// resolve against.
pub(crate) fn input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.transform_up(|e| {
        match &e {
            Expr::BinaryExpr(binary)
                if matches!(
                    binary.op,
                    Operator::Plus | Operator::Minus | Operator::Multiply
                ) =>
            {
                let (Ok(left), Ok(right)) =
                    (binary.left.get_type(schema), binary.right.get_type(schema))
                else {
                    return Ok(Transformed::no(e));
                };
                if is_integer(&left) && is_integer(&right) {
                    let replacement = Expr::ScalarFunction(ScalarFunction::new_udf(
                        Arc::new(checked_scalar(binary.op)),
                        vec![binary.left.as_ref().clone(), binary.right.as_ref().clone()],
                    ));
                    return Ok(Transformed::yes(replacement));
                }
            }
            Expr::AggregateFunction(agg) if agg.func.name() == "sum" => {
                if let [arg] = agg.params.args.as_slice()
                    && arg.get_type(schema).is_ok_and(|t| t == DataType::Int64)
                {
                    let mut replacement = agg.clone();
                    replacement.func = Arc::new(AggregateUDF::new_from_impl(CheckedIntSum::new()));
                    return Ok(Transformed::yes(Expr::AggregateFunction(replacement)));
                }
            }
            Expr::WindowFunction(window) => {
                if let WindowFunctionDefinition::AggregateUDF(func) = &window.fun
                    && func.name() == "sum"
                    && let [arg] = window.params.args.as_slice()
                    && arg.get_type(schema).is_ok_and(|t| t == DataType::Int64)
                {
                    let mut replacement: WindowFunction = window.as_ref().clone();
                    replacement.fun = WindowFunctionDefinition::AggregateUDF(Arc::new(
                        AggregateUDF::new_from_impl(CheckedIntSum::new()),
                    ));
                    return Ok(Transformed::yes(Expr::WindowFunction(Box::new(
                        replacement,
                    ))));
                }
            }
            _ => {}
        }
        Ok(Transformed::no(e))
    })
}

impl AnalyzerRule for CheckedIntegerArithmetic {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| {
            let schema = input_schema(&node);
            let name_preserver = NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let original = name_preserver.save(&expr);
                rewrite_expr(expr, &schema).map(|t| t.update_data(|e| original.restore(e)))
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "trino_checked_integer_arithmetic"
    }
}
