//! Trino's numeric typing and overflow rules, applied to the planned query.
//!
//! The [`TrinoSemantics`] analyzer rule runs *before* DataFusion's type
//! coercion (so it sees the operand types as Trino would) and rewrites:
//!
//! - integer literals that fit in 32 bits to `integer` (DataFusion types
//!   `1` as `bigint`; Trino and Athena report `integer`, and
//!   `2147483647 + 1` overflows);
//! - integer `+ - *` and `sum` to overflow-checked UDFs (DataFusion wraps
//!   around; Trino raises `NUMERIC_VALUE_OUT_OF_RANGE`), widening mixed
//!   widths to the wider type;
//! - decimal `+ - * /`, `sum`, and `avg` to UDFs with Trino's result types
//!   and HALF_UP rounding (see [`super::decimal`]);
//! - `date ± interval` to a UDF that refuses sub-day intervals;
//! - `sum` / `avg` over `real` to UDFs returning `real` (DataFusion gives
//!   a `double`);
//! - array `=`, `<>`, and ordering operators to UDFs with Trino's NULL-
//!   element rules, and array `ORDER BY` keys to a check that refuses NULL
//!   elements;
//! - set operations and joins get their schema recomputed so the narrowed
//!   literal types show (`SELECT 1 UNION SELECT 1` is `integer`);
//! - `greatest` / `least` over a mix of double and exact numbers to double
//!   (Trino's common supertype; DataFusion picks a wide decimal);
//! - table scans whose timestamp columns are not millisecond-precise to a
//!   projection that rounds them HALF_UP to milliseconds, as Athena's
//!   `timestamp(3)` readers do.
//!
//! Output column names are preserved, so the substitutions are invisible
//! except when they fail.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type, TimeUnit};
use arrow::error::ArrowError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, ExprSchemable, LogicalPlan,
    LogicalPlanBuilder, Operator, Projection, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Union, Volatility, WindowFunctionDefinition,
};
use datafusion::optimizer::analyzer::AnalyzerRule;

use super::arrays::{ArrayCmp, TrinoArrayCompare, TrinoArraySortKey};
use super::casts::trino_type_name;
use super::decimal::{
    DecimalAgg, TrinoDecimalAgg, TrinoDecimalDiv, decimal_binary, decimal_result_type,
    decimal_type_of, is_exact_numeric,
};
use super::timestamps::{TrinoDateInterval, TrinoTimestampMillis};
use super::{data_error, is_integer, type_mismatch};

/// The checked scalar UDFs (`trino_checked_add` / `_sub` / `_mul`).
pub fn scalar_udfs() -> Vec<ScalarUDF> {
    [Operator::Plus, Operator::Minus, Operator::Multiply]
        .into_iter()
        .map(|op| ScalarUDF::new_from_impl(CheckedArithmetic::new(op)))
        .collect()
}

/// The checked aggregate UDFs (`trino_checked_sum`) and the `real`
/// aggregates (`trino_real_sum`, `trino_real_avg`).
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::new_from_impl(CheckedIntSum::new()),
        AggregateUDF::new_from_impl(RealAgg::new(false)),
        AggregateUDF::new_from_impl(RealAgg::new(true)),
    ]
}

fn overflow(type_name: &str, op: &str) -> DataFusionError {
    data_error(
        "NUMERIC_VALUE_OUT_OF_RANGE",
        format!("{type_name} {op} overflow"),
    )
}

/// The wider of two signed integer types.
fn wider_integer(a: &DataType, b: &DataType) -> DataType {
    fn width(t: &DataType) -> u8 {
        match t {
            DataType::Int8 => 8,
            DataType::Int16 => 16,
            DataType::Int32 => 32,
            _ => 64,
        }
    }
    if width(a) >= width(b) {
        a.clone()
    } else {
        b.clone()
    }
}

/// `trino_checked_add(a, b)` etc.: overflow-checked integer arithmetic
/// (Arrow's checked kernels on the wider operand type) and Trino's decimal
/// arithmetic for decimal operands.
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
        let (a, b) = (&arg_types[0], &arg_types[1]);
        if is_integer(a) && is_integer(b) {
            return Ok(wider_integer(a, b));
        }
        match (decimal_type_of(a), decimal_type_of(b)) {
            (Some(x), Some(y)) => {
                let (p, s) = decimal_result_type(self.op, x, y)?;
                Ok(DataType::Decimal128(p, s))
            }
            _ => Err(type_mismatch(format!(
                "Cannot apply operator: {} {} {}",
                trino_type_name(a),
                self.op,
                trino_type_name(b)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        if !(is_integer(lhs.data_type()) && is_integer(rhs.data_type())) {
            return Ok(ColumnarValue::Array(decimal_binary(self.op, &lhs, &rhs)?));
        }
        let target = wider_integer(lhs.data_type(), rhs.data_type());
        let lhs = cast(&lhs, &target)?;
        let rhs = cast(&rhs, &target)?;
        let kernel = match self.op {
            Operator::Plus => numeric::add,
            Operator::Minus => numeric::sub,
            _ => numeric::mul,
        };
        match kernel(&lhs, &rhs) {
            Ok(array) => Ok(ColumnarValue::Array(array)),
            Err(ArrowError::ArithmeticOverflow(_)) => {
                Err(overflow(&trino_type_name(&target), self.verb()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// `trino_checked_sum(integer)`: `sum` over any integer width that fails on
/// bigint overflow.
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
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if is_integer(&arg_types[0]) {
            Ok(DataType::Int64)
        } else {
            Err(type_mismatch(format!(
                "expected an integer argument, got {}",
                trino_type_name(&arg_types[0])
            )))
        }
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

fn int64_values(array: &ArrayRef) -> Result<arrow::array::Int64Array> {
    Ok(cast(array, &DataType::Int64)?
        .as_primitive::<Int64Type>()
        .clone())
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
                .ok_or_else(|| overflow("bigint", "sum"))?,
        );
        Ok(())
    }
}

impl Accumulator for CheckedSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in int64_values(&values[0])?.iter().flatten() {
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
        for v in int64_values(&values[0])?.iter().flatten() {
            self.sum = Some(
                self.sum
                    .unwrap_or(0)
                    .checked_sub(v)
                    .ok_or_else(|| overflow("bigint", "sum"))?,
            );
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// `sum(DISTINCT integer)` with overflow checking at evaluation.
#[derive(Debug, Default)]
struct DistinctCheckedSum {
    values: HashSet<i64>,
}

impl Accumulator for DistinctCheckedSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values
            .extend(int64_values(&values[0])?.iter().flatten());
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Int64(None));
        }
        let mut sum = 0i64;
        for v in &self.values {
            sum = sum
                .checked_add(*v)
                .ok_or_else(|| overflow("bigint", "sum"))?;
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

// ---------------------------------------------------------------------------
// sum / avg over REAL
// ---------------------------------------------------------------------------

/// `trino_real_sum(real)` / `trino_real_avg(real)`: Trino accumulates a
/// `real` sum in a double and returns a `real` (`sum` of `1.1` and `2.2`
/// prints `3.3000002`); DataFusion's `sum` / `avg` return a `double`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RealAgg {
    signature: Signature,
    avg: bool,
}

impl RealAgg {
    /// New instance; `avg` selects the average.
    pub fn new(avg: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            avg,
        }
    }
}

impl AggregateUDFImpl for RealAgg {
    fn name(&self) -> &str {
        if self.avg {
            "trino_real_avg"
        } else {
            "trino_real_sum"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == DataType::Float32 {
            Ok(DataType::Float32)
        } else {
            Err(type_mismatch(format!(
                "expected a real argument, got {}",
                trino_type_name(&arg_types[0])
            )))
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RealAccumulator {
            avg: self.avg,
            distinct: args.is_distinct,
            sum: 0.0,
            count: 0,
            seen: HashSet::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[real sum]", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format!("{}[real count]", args.name),
                DataType::Int64,
                true,
            )),
            Arc::new(Field::new_list(
                format!("{}[real distinct]", args.name),
                Field::new_list_field(DataType::Float32, true),
                true,
            )),
        ])
    }
}

/// Double-precision running sum and count over `real` values; `seen` holds
/// the distinct values (as bit patterns) for `DISTINCT`.
#[derive(Debug)]
struct RealAccumulator {
    avg: bool,
    distinct: bool,
    sum: f64,
    count: i64,
    seen: HashSet<u32>,
}

impl RealAccumulator {
    fn add(&mut self, value: f32) {
        if self.distinct && !self.seen.insert(value.to_bits()) {
            return;
        }
        self.sum += f64::from(value);
        self.count += 1;
    }
}

impl Accumulator for RealAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let floats = cast(&values[0], &DataType::Float32)?;
        for v in floats
            .as_primitive::<arrow::datatypes::Float32Type>()
            .iter()
            .flatten()
        {
            self.add(v);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::Float32(None));
        }
        let value = if self.avg {
            self.sum / self.count as f64
        } else {
            self.sum
        };
        Ok(ScalarValue::Float32(Some(value as f32)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.seen.capacity() * size_of::<u32>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let seen: Vec<ScalarValue> = self
            .seen
            .iter()
            .map(|bits| ScalarValue::Float32(Some(f32::from_bits(*bits))))
            .collect();
        Ok(vec![
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::Int64(Some(self.count)),
            ScalarValue::List(ScalarValue::new_list_nullable(&seen, &DataType::Float32)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if self.distinct {
            for list in states[2].as_list::<i32>().iter().flatten() {
                for v in list
                    .as_primitive::<arrow::datatypes::Float32Type>()
                    .iter()
                    .flatten()
                {
                    self.add(v);
                }
            }
            return Ok(());
        }
        let sums = states[0].as_primitive::<arrow::datatypes::Float64Type>();
        let counts = states[1].as_primitive::<Int64Type>();
        for i in 0..sums.len() {
            if !sums.is_null(i) {
                self.sum += sums.value(i);
                self.count += counts.value(i);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The analyzer rule
// ---------------------------------------------------------------------------

/// Analyzer rule: see the module docs.
#[derive(Debug, Default)]
pub struct TrinoSemantics;

/// The merged schema of a node's inputs, which is what its expressions
/// resolve against.
pub(crate) fn input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn udf_call(udf: impl ScalarUDFImpl + 'static, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(udf)),
        args,
    ))
}

fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
}

fn is_date(t: &DataType) -> bool {
    matches!(t, DataType::Date32 | DataType::Date64)
}

fn is_interval(t: &DataType) -> bool {
    matches!(t, DataType::Interval(_) | DataType::Duration(_))
}

fn to_double(expr: &Expr) -> Expr {
    Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(expr.clone()),
        DataType::Float64,
    ))
}

/// Trino's common supertype of a double and an exact number is double;
/// DataFusion widens both into a wide decimal (`1.5 + 2e0` would be a
/// `decimal(30,15)`). Returns the expressions with the exact-numeric ones
/// cast to double when the mix occurs, else `None`.
fn widen_exact_to_double(exprs: &[&Expr], schema: &DFSchema) -> Option<Vec<Expr>> {
    let types: Vec<DataType> = exprs
        .iter()
        .filter_map(|e| e.get_type(schema).ok())
        .collect();
    if types.len() != exprs.len()
        || !types.iter().any(is_float)
        || !types.iter().any(|t| matches!(t, DataType::Decimal128(..)))
    {
        return None;
    }
    Some(
        exprs
            .iter()
            .zip(&types)
            .map(|(e, t)| {
                if is_exact_numeric(t) {
                    to_double(e)
                } else {
                    (*e).clone()
                }
            })
            .collect(),
    )
}

fn is_list(t: &DataType) -> bool {
    matches!(
        t,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

fn rewrite_binary(left: &Expr, op: Operator, right: &Expr, schema: &DFSchema) -> Option<Expr> {
    let (Ok(l), Ok(r)) = (left.get_type(schema), right.get_type(schema)) else {
        return None;
    };
    // Array comparison: Trino's NULL-element rules (see `arrays::ArrayCmp`).
    if is_list(&l) && is_list(&r) {
        let cmp = match op {
            Operator::Eq => ArrayCmp::Eq,
            Operator::NotEq => ArrayCmp::NotEq,
            Operator::Lt => ArrayCmp::Lt,
            Operator::LtEq => ArrayCmp::LtEq,
            Operator::Gt => ArrayCmp::Gt,
            Operator::GtEq => ArrayCmp::GtEq,
            _ => return None,
        };
        return Some(udf_call(
            TrinoArrayCompare::new(cmp),
            vec![left.clone(), right.clone()],
        ));
    }
    if let Some(widened) = widen_exact_to_double(&[left, right], schema) {
        let mut it = widened.into_iter();
        return Some(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(it.next().expect("two operands")),
            op,
            Box::new(it.next().expect("two operands")),
        )));
    }
    let args = vec![left.clone(), right.clone()];
    match op {
        Operator::Plus | Operator::Minus | Operator::Multiply => {
            if (is_integer(&l) && is_integer(&r))
                || (is_exact_numeric(&l)
                    && is_exact_numeric(&r)
                    && (matches!(l, DataType::Decimal128(..))
                        || matches!(r, DataType::Decimal128(..))))
            {
                return Some(udf_call(CheckedArithmetic::new(op), args));
            }
            if op != Operator::Multiply {
                if is_date(&l) && is_interval(&r) {
                    return Some(udf_call(TrinoDateInterval::new(op == Operator::Plus), args));
                }
                if op == Operator::Plus && is_interval(&l) && is_date(&r) {
                    return Some(udf_call(
                        TrinoDateInterval::new(true),
                        vec![right.clone(), left.clone()],
                    ));
                }
            }
            None
        }
        Operator::Divide
            if is_exact_numeric(&l)
                && is_exact_numeric(&r)
                && (matches!(l, DataType::Decimal128(..))
                    || matches!(r, DataType::Decimal128(..))) =>
        {
            Some(udf_call(TrinoDecimalDiv::new(), args))
        }
        _ => None,
    }
}

/// The Trino substitute for `sum` / `avg` over an argument of type `t`:
/// overflow-checked for integers, Trino's decimal typing for decimals, a
/// `real` result for `real` inputs. `None` keeps DataFusion's aggregate.
fn trino_sum_avg(name: &str, t: &DataType) -> Option<AggregateUDF> {
    Some(match (name, t) {
        ("sum", t) if is_integer(t) => AggregateUDF::new_from_impl(CheckedIntSum::new()),
        ("sum", DataType::Decimal128(..)) => {
            AggregateUDF::new_from_impl(TrinoDecimalAgg::new(DecimalAgg::Sum))
        }
        ("avg", DataType::Decimal128(..)) => {
            AggregateUDF::new_from_impl(TrinoDecimalAgg::new(DecimalAgg::Avg))
        }
        ("sum", DataType::Float32) => AggregateUDF::new_from_impl(RealAgg::new(false)),
        ("avg", DataType::Float32) => AggregateUDF::new_from_impl(RealAgg::new(true)),
        _ => return None,
    })
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.transform_up(|e| {
        match &e {
            // Trino types an integer literal as `integer` when it fits.
            Expr::Literal(ScalarValue::Int64(Some(v)), metadata) => {
                if let Ok(narrow) = i32::try_from(*v) {
                    return Ok(Transformed::yes(Expr::Literal(
                        ScalarValue::Int32(Some(narrow)),
                        metadata.clone(),
                    )));
                }
            }

            Expr::BinaryExpr(binary) => {
                if let Some(replacement) =
                    rewrite_binary(&binary.left, binary.op, &binary.right, schema)
                {
                    return Ok(Transformed::yes(replacement));
                }
            }
            Expr::AggregateFunction(agg) if matches!(agg.func.name(), "sum" | "avg") => {
                if let [arg] = agg.params.args.as_slice()
                    && let Ok(t) = arg.get_type(schema)
                {
                    if let Some(func) = trino_sum_avg(agg.func.name(), &t) {
                        let mut replacement = agg.clone();
                        replacement.func = Arc::new(func);
                        return Ok(Transformed::yes(Expr::AggregateFunction(replacement)));
                    }
                }
            }
            Expr::WindowFunction(window) => {
                if let WindowFunctionDefinition::AggregateUDF(func) = &window.fun
                    && matches!(func.name(), "sum" | "avg")
                    && let [arg] = window.params.args.as_slice()
                    && let Ok(t) = arg.get_type(schema)
                {
                    if let Some(func) = trino_sum_avg(func.name(), &t) {
                        let mut replacement: WindowFunction = window.as_ref().clone();
                        replacement.fun = WindowFunctionDefinition::AggregateUDF(Arc::new(func));
                        return Ok(Transformed::yes(Expr::WindowFunction(Box::new(
                            replacement,
                        ))));
                    }
                }
            }
            // Trino's common supertype of double and an exact number is
            // double; DataFusion widens both into a decimal.
            Expr::ScalarFunction(call)
                if matches!(
                    call.func.name(),
                    "greatest" | "least" | "coalesce" | "nullif"
                ) =>
            {
                let args: Vec<&Expr> = call.args.iter().collect();
                if let Some(widened) = widen_exact_to_double(&args, schema) {
                    let mut replacement = call.clone();
                    replacement.args = widened;
                    return Ok(Transformed::yes(Expr::ScalarFunction(replacement)));
                }
            }
            Expr::Case(case) => {
                let mut results: Vec<&Expr> = case
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| t.as_ref())
                    .collect();
                if let Some(otherwise) = &case.else_expr {
                    results.push(otherwise);
                }
                if let Some(mut widened) = widen_exact_to_double(&results, schema) {
                    let mut replacement = case.clone();
                    let else_expr = if case.else_expr.is_some() {
                        widened.pop().map(Box::new)
                    } else {
                        None
                    };
                    for ((_, then), new_then) in replacement.when_then_expr.iter_mut().zip(widened)
                    {
                        **then = new_then;
                    }
                    replacement.else_expr = else_expr;
                    return Ok(Transformed::yes(Expr::Case(replacement)));
                }
            }
            _ => {}
        }
        Ok(Transformed::no(e))
    })
}

/// The rows of a `VALUES` after the literal narrowing, ready to be
/// re-planned: the name-preserving aliases are stripped (the builder would
/// otherwise cast the aliased literal), the planner's own casts of integer
/// literals to the column type it had inferred from `bigint` literals
/// (`VALUES (1, 2.5), (NULL, 3)`) are dropped so the builder re-infers it
/// from the `integer` ones (`decimal(11,1)`, as on Trino; a user-written
/// cast carries a `trino_` wrapper and is kept), and a column whose values
/// are all `integer` or typed NULLs gets `integer` NULLs.
fn narrow_values(rows: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    // A numeric literal, or the `CAST('1e2' AS DOUBLE)` an exponent literal
    // arrives as.
    let is_numeric_literal = |e: &Expr| match e {
        Expr::Literal(
            ScalarValue::Int32(_)
            | ScalarValue::Int64(None)
            | ScalarValue::Decimal128(..)
            | ScalarValue::Float64(_),
            _,
        ) => true,
        Expr::Cast(cast) => {
            cast.field.data_type() == &DataType::Float64
                && matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Utf8(_), _))
        }
        _ => false,
    };
    let strip_planner_cast = |e: Expr| match e {
        Expr::Cast(cast)
            if matches!(
                cast.field.data_type(),
                DataType::Int64 | DataType::Decimal128(..) | DataType::Float64
            ) && is_numeric_literal(&cast.expr) =>
        {
            *cast.expr
        }
        other => other,
    };
    let mut rows: Vec<Vec<Expr>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|e| strip_planner_cast(e.clone().unalias_nested().data))
                .collect()
        })
        .collect();
    let width = rows.first().map_or(0, Vec::len);
    for column in 0..width {
        let is_typed_null = |e: &Expr| matches!(e, Expr::Literal(ScalarValue::Int64(None), _));
        let all_integer = rows.iter().all(|row| {
            matches!(row[column], Expr::Literal(ScalarValue::Int32(_), _))
                || is_typed_null(&row[column])
        });
        let any_integer = rows
            .iter()
            .any(|row| matches!(row[column], Expr::Literal(ScalarValue::Int32(_), _)));
        if all_integer && any_integer {
            for row in &mut rows {
                if is_typed_null(&row[column]) {
                    row[column] = Expr::Literal(ScalarValue::Int32(None), None);
                }
            }
        }
        // A column mixing doubles and exact numbers is double on Trino.
        let literal_type = |e: &Expr| match e {
            Expr::Literal(v, _) => Some(v.data_type()),
            // `1e2` arrives as `CAST('1e2' AS DOUBLE)`.
            Expr::Cast(cast) if matches!(cast.expr.as_ref(), Expr::Literal(..)) => {
                Some(cast.field.data_type().clone())
            }
            _ => None,
        };
        let types: Vec<DataType> = rows
            .iter()
            .filter_map(|row| literal_type(&row[column]))
            .collect();
        if types.iter().any(is_float) && types.iter().any(is_exact_numeric) {
            for row in &mut rows {
                if literal_type(&row[column]).is_some_and(|t| is_exact_numeric(&t)) {
                    row[column] = to_double(&row[column]);
                }
            }
        }
    }
    rows
}

/// Trino's common supertype of a double and an exact number is double, also
/// across set operations (`SELECT 1.5 UNION SELECT 2e0` is a `double`);
/// DataFusion would widen both into a wide decimal. Wraps the inputs whose
/// column is exact, where another input's is double, in a projection that
/// casts it to double.
fn widen_union_to_double(inputs: Vec<Arc<LogicalPlan>>) -> Result<Vec<Arc<LogicalPlan>>> {
    let width = inputs
        .first()
        .map_or(0, |input| input.schema().fields().len());
    let mut widen = vec![false; width];
    for (i, flag) in widen.iter_mut().enumerate() {
        let types: Vec<&DataType> = inputs
            .iter()
            .filter_map(|input| input.schema().fields().get(i))
            .map(|f| f.data_type())
            .collect();
        *flag = types.iter().any(|t| is_float(t)) && types.iter().any(|t| is_exact_numeric(t));
    }
    if !widen.iter().any(|w| *w) {
        return Ok(inputs);
    }
    inputs
        .into_iter()
        .map(|input| {
            let needs = input
                .schema()
                .fields()
                .iter()
                .zip(&widen)
                .any(|(f, w)| *w && is_exact_numeric(f.data_type()));
            if !needs {
                return Ok(input);
            }
            let exprs: Vec<Expr> = input
                .schema()
                .iter()
                .zip(&widen)
                .map(|((qualifier, field), w)| {
                    let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
                    if *w && is_exact_numeric(field.data_type()) {
                        to_double(&column).alias_qualified(qualifier.cloned(), field.name())
                    } else {
                        column
                    }
                })
                .collect();
            Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
                exprs, input,
            )?)))
        })
        .collect()
}

/// Wrap a scan whose schema has non-millisecond timestamps in a projection
/// that rounds them, keeping every (qualifier, name) pair.
fn round_scan_timestamps(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::TableScan(scan) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let needs_rounding = scan.projected_schema.fields().iter().any(
        |f| matches!(f.data_type(), DataType::Timestamp(unit, _) if *unit != TimeUnit::Millisecond),
    );
    if !needs_rounding {
        return Ok(Transformed::no(plan));
    }
    let exprs: Vec<Expr> = scan
        .projected_schema
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
            match field.data_type() {
                DataType::Timestamp(unit, _) if *unit != TimeUnit::Millisecond => {
                    udf_call(TrinoTimestampMillis::new(), vec![column])
                        .alias_qualified(qualifier.cloned(), field.name())
                }
                _ => column,
            }
        })
        .collect();
    let projection = Projection::try_new(exprs, Arc::new(plan))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

impl AnalyzerRule for TrinoSemantics {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| {
            // LIMIT / OFFSET counts must stay bigint literals.
            if matches!(node, LogicalPlan::Limit(_)) {
                return Ok(Transformed::no(node));
            }
            if let LogicalPlan::TableScan(_) = &node {
                return round_scan_timestamps(node);
            }
            let schema = input_schema(&node);
            // Sorting arrays uses Trino's ordering operator, which refuses
            // NULL elements.
            let node = if let LogicalPlan::Sort(sort) = node {
                let mut sort = sort;
                for item in &mut sort.expr {
                    if item.expr.get_type(&schema).is_ok_and(|t| is_list(&t)) {
                        item.expr = udf_call(TrinoArraySortKey::new(), vec![item.expr.clone()]);
                    }
                }
                LogicalPlan::Sort(sort)
            } else {
                node
            };
            let name_preserver = NamePreserver::new(&node);
            let mut transformed = node.map_expressions(|expr| {
                let original = name_preserver.save(&expr);
                rewrite_expr(expr, &schema).map(|t| t.update_data(|e| original.restore(e)))
            })?;
            // Nodes keep the schema computed at planning, where the literals
            // were still `bigint`; recompute it from the rewritten
            // expressions and inputs so the narrowed types show through
            // set operations and joins (`SELECT 1 UNION SELECT 1` stays
            // `integer`, as on Trino: DataFusion's coercion derives the
            // union type from the input schemas).
            transformed.data = match transformed.data {
                LogicalPlan::Union(union) => LogicalPlan::Union(Union::try_new_with_loose_types(
                    widen_union_to_double(union.inputs)?,
                )?),
                other => other.recompute_schema()?,
            };
            // `VALUES` keeps its planned schema; rebuild it so the narrowed
            // literal types show.
            if let LogicalPlan::Values(values) = &transformed.data {
                let rows = narrow_values(&values.values);
                if rows != values.values {
                    let rebuilt = LogicalPlanBuilder::values(rows)?.build()?;
                    return Ok(Transformed::yes(rebuilt));
                }
            }
            Ok(transformed)
        })
        .map(|t| {
            if std::env::var_os("GLAUX_DEBUG_PLAN").is_some() {
                eprintln!("after trino_semantics:\n{}", t.data.display_indent_schema());
            }
            t.data
        })
    }

    fn name(&self) -> &str {
        "trino_semantics"
    }
}
