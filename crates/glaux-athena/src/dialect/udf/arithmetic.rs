//! Trino's numeric typing and overflow rules, applied to the planned query.
//!
//! The [`TrinoSemantics`] analyzer rule runs *before* DataFusion's type
//! coercion (so it sees the operand types as Trino would) and rewrites:
//!
//! - integer literals that fit in 32 bits to `integer` (DataFusion types
//!   `1` as `bigint`; Trino and Athena report `integer`, and
//!   `2147483647 + 1` overflows);
//! - integer `+ - * /` and `sum` to overflow-checked UDFs (DataFusion wraps
//!   around, and Arrow's diagnostic names neither the SQL type nor the
//!   operands; Trino raises `NUMERIC_VALUE_OUT_OF_RANGE` naming both),
//!   widening mixed widths to the wider type;
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
//! - outer `JOIN ... USING` to a projection giving the join column Trino's
//!   value (`coalesce(l.k, r.k)` for a full join);
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
use datafusion::common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, ExprSchemable, Join,
    JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Union, Volatility,
    WindowFunctionDefinition,
};
use datafusion::optimizer::analyzer::AnalyzerRule;

use super::arrays::{ArrayCmp, TrinoArrayCompare, TrinoArraySortKey};
use super::casts::trino_type_name;
use super::decimal::{
    DecimalAgg, TrinoDecimalAgg, TrinoDecimalDiv, decimal_binary, decimal_result_type,
    decimal_type_of, is_exact_numeric,
};
use super::floats::{TrinoFloatGreatest, TrinoFloatMax, TrinoIeeeCmp};
use super::timestamps::{TrinoDateInterval, TrinoTimestampMillis};
use super::{data_error, is_integer, type_mismatch, unsupported_error};

/// The checked scalar UDFs (`trino_checked_add` / `_sub` / `_mul` / `_div`
/// / `_neg`).
pub fn scalar_udfs() -> Vec<ScalarUDF> {
    [
        Operator::Plus,
        Operator::Minus,
        Operator::Multiply,
        Operator::Divide,
    ]
    .into_iter()
    .map(|op| ScalarUDF::new_from_impl(CheckedArithmetic::new(op)))
    .chain([ScalarUDF::new_from_impl(CheckedNegate::new())])
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

/// The same diagnostic with the operands Trino always names
/// (`BigintOperators.add`: `bigint addition overflow: 9223372036854775807 +
/// 1`), as the negation path does.
fn overflow_at(type_name: &str, op: &str, left: i64, symbol: &str, right: i64) -> DataFusionError {
    data_error(
        "NUMERIC_VALUE_OUT_OF_RANGE",
        format!("{type_name} {op} overflow: {left} {symbol} {right}"),
    )
}

/// The inclusive range of a signed integer type, as `i64`.
fn integer_range(t: &DataType) -> (i64, i64) {
    match t {
        DataType::Int8 => (i64::from(i8::MIN), i64::from(i8::MAX)),
        DataType::Int16 => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DataType::Int32 => (i64::from(i32::MIN), i64::from(i32::MAX)),
        _ => (i64::MIN, i64::MAX),
    }
}

/// The first row whose `op` overflows `target`. Arrow's kernel reports that
/// *a* row overflowed without saying which, so the operation is replayed in
/// 64-bit arithmetic — every glaux integer type widens into `i64` losslessly
/// — to recover the operands Trino names.
fn first_overflowing_row(
    op: Operator,
    target: &DataType,
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Option<(i64, i64)> {
    let lhs = cast(lhs, &DataType::Int64).ok()?;
    let rhs = cast(rhs, &DataType::Int64).ok()?;
    let lhs = lhs.as_primitive::<Int64Type>();
    let rhs = rhs.as_primitive::<Int64Type>();
    let (low, high) = integer_range(target);
    (0..lhs.len().min(rhs.len())).find_map(|i| {
        if lhs.is_null(i) || rhs.is_null(i) {
            return None;
        }
        let (left, right) = (lhs.value(i), rhs.value(i));
        // A zero divisor is a division-by-zero error, not an overflow.
        if op == Operator::Divide && right == 0 {
            return None;
        }
        let value = match op {
            Operator::Plus => left.checked_add(right),
            Operator::Minus => left.checked_sub(right),
            Operator::Multiply => left.checked_mul(right),
            _ => left.checked_div(right),
        };
        match value {
            Some(v) if (low..=high).contains(&v) => None,
            _ => Some((left, right)),
        }
    })
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
/// (Arrow's checked kernels on the wider operand type, with Trino's
/// diagnostic naming the type and the operands) and Trino's decimal
/// arithmetic for decimal operands. `trino_checked_div` covers integer
/// division only; decimal division is [`TrinoDecimalDiv`], which rounds.
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
            Operator::Divide => "division",
            _ => "multiplication",
        }
    }

    fn symbol(&self) -> &'static str {
        match self.op {
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Divide => "/",
            _ => "*",
        }
    }
}

impl ScalarUDFImpl for CheckedArithmetic {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "trino_checked_add",
            Operator::Minus => "trino_checked_sub",
            Operator::Divide => "trino_checked_div",
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
            Operator::Divide => numeric::div,
            _ => numeric::mul,
        };
        match kernel(&lhs, &rhs) {
            Ok(array) => Ok(ColumnarValue::Array(array)),
            Err(ArrowError::ArithmeticOverflow(_)) => {
                let name = trino_type_name(&target);
                Err(match first_overflowing_row(self.op, &target, &lhs, &rhs) {
                    Some((left, right)) => {
                        overflow_at(&name, self.verb(), left, self.symbol(), right)
                    }
                    None => overflow(&name, self.verb()),
                })
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// `trino_checked_neg(x)`: unary minus over an integer, which overflows for
/// exactly one value per type — the type's minimum, whose positive is one
/// past the maximum. Trino names the type and the value (`tinyint negation
/// overflow: -128`); Arrow's kernel says `Overflow happened on: - -128`,
/// naming neither.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedNegate {
    signature: Signature,
}

impl Default for CheckedNegate {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckedNegate {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CheckedNegate {
    fn name(&self) -> &str {
        "trino_checked_neg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if is_integer(&arg_types[0]) {
            Ok(arg_types[0].clone())
        } else {
            Err(type_mismatch(format!(
                "Cannot negate {}",
                trino_type_name(&arg_types[0])
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        match numeric::neg(&input) {
            Ok(array) => Ok(ColumnarValue::Array(array)),
            Err(ArrowError::ArithmeticOverflow(_)) => {
                let minimum = match input.data_type() {
                    DataType::Int8 => i64::from(i8::MIN),
                    DataType::Int16 => i64::from(i16::MIN),
                    DataType::Int32 => i64::from(i32::MIN),
                    _ => i64::MIN,
                };
                Err(data_error(
                    "NUMERIC_VALUE_OUT_OF_RANGE",
                    format!(
                        "{} negation overflow: {minimum}",
                        trino_type_name(input.data_type())
                    ),
                ))
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
        // Trino's `LongSumAggregation` adds through `BigintOperators.add`,
        // so an overflowing sum carries that operator's diagnostic — the
        // running total and the value that broke it — not a `sum`-specific
        // one.
        let sum = self.sum.unwrap_or(0);
        self.sum = Some(
            sum.checked_add(value)
                .ok_or_else(|| overflow_at("bigint", "addition", sum, "+", value))?,
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
        // The mirror image, through `BigintOperators.subtract` — what
        // Trino's `@RemoveInputFunction` for `sum(bigint)` goes through as
        // a window frame slides.
        for v in int64_values(&values[0])?.iter().flatten() {
            let sum = self.sum.unwrap_or(0);
            self.sum = Some(
                sum.checked_sub(v)
                    .ok_or_else(|| overflow_at("bigint", "subtraction", sum, "-", v))?,
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
                .ok_or_else(|| overflow_at("bigint", "addition", sum, "+", *v))?;
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

/// A type Trino compares with a double (the IEEE comparison UDF casts it).
fn numeric_or_null(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
    )
}

/// Whether a `=` / `<` … between these types must use IEEE double
/// semantics (Trino's `DoubleType` operators) instead of Arrow's
/// total-order kernels: any comparison with a float operand.
fn needs_ieee_comparison(l: &DataType, r: &DataType) -> bool {
    (is_float(l) || is_float(r)) && numeric_or_null(l) && numeric_or_null(r)
}

fn ieee_comparison_op(op: Operator) -> Option<Operator> {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
    .then_some(op)
}

/// `trino_ieee_<op>(left, right)`.
fn ieee_cmp(op: Operator, left: Expr, right: Expr) -> Expr {
    udf_call(TrinoIeeeCmp::new(op), vec![left, right])
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
    // Comparisons with a float operand: Trino's operators are Java's
    // primitive double operators (NaN compares false, `-0.0 = 0.0` is
    // true); Arrow's kernels use a total order where NaN equals NaN.
    if let Some(op) = ieee_comparison_op(op)
        && needs_ieee_comparison(&l, &r)
    {
        return Some(ieee_cmp(op, left.clone(), right.clone()));
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
        // Integer division overflows for exactly one pair per type (the
        // type's minimum over -1); the UDF names the type and the operands
        // as Trino's `BigintOperators.divide` does, where Arrow's kernel
        // says only `Overflow happened on: ...`.
        Operator::Divide if is_integer(&l) && is_integer(&r) => {
            Some(udf_call(CheckedArithmetic::new(op), args))
        }
        _ => None,
    }
}

/// The Trino substitute for `sum` / `avg` / `max` over an argument of type
/// `t`: overflow-checked for integers, Trino's decimal typing for decimals,
/// a `real` result for `real` inputs, NaN-smallest `max` for floats. `None`
/// keeps DataFusion's aggregate.
fn trino_aggregate(name: &str, t: &DataType) -> Option<AggregateUDF> {
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
        // Trino's `max` ranks NaN smallest (`COMPARISON_UNORDERED_FIRST`);
        // Arrow's ranks it largest. `min` agrees between the two.
        ("max", DataType::Float32 | DataType::Float64) => {
            AggregateUDF::new_from_impl(TrinoFloatMax::new())
        }
        _ => return None,
    })
}

/// Wrap an expression in the guard that refuses arrays with NULL elements
/// before they are ranked (a no-op for every other type).
fn array_sort_key(expr: &Expr) -> Expr {
    udf_call(TrinoArraySortKey::new(), vec![expr.clone()])
}

fn is_array_sort_key(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.func.name() == "trino_array_sort_key")
}

/// An aggregate whose array arguments or `ORDER BY` keys rank through
/// Trino's array ordering operator, with the NULL-element guard applied.
fn guard_array_ordering(
    agg: &datafusion::logical_expr::expr::AggregateFunction,
    schema: &DFSchema,
) -> Option<datafusion::logical_expr::expr::AggregateFunction> {
    let ranks_argument = matches!(agg.func.name(), "max" | "min");
    let is_array = |e: &Expr| e.get_type(schema).is_ok_and(|t| is_list(&t));
    let guard_needed = |e: &Expr| is_array(e) && !is_array_sort_key(e);
    let order_by = agg.params.order_by.as_slice();
    let argument_needs = ranks_argument && agg.params.args.iter().any(guard_needed);
    let order_needs = order_by.iter().any(|s| guard_needed(&s.expr));
    if !argument_needs && !order_needs {
        return None;
    }
    let mut replacement = agg.clone();
    if argument_needs {
        replacement.params.args = agg
            .params
            .args
            .iter()
            .map(|a| {
                if is_array(a) {
                    array_sort_key(a)
                } else {
                    a.clone()
                }
            })
            .collect();
    }
    if order_needs {
        for sort in &mut replacement.params.order_by {
            if is_array(&sort.expr) {
                sort.expr = array_sort_key(&sort.expr);
            }
        }
    }
    Some(replacement)
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
            // Unary minus over an integer: Trino checks the overflow at the
            // type's minimum and names the type in the diagnostic, where
            // DataFusion's negation leaks Arrow's wording. (A negated
            // literal is folded into the literal itself during the AST
            // rewrite, so this only sees computed operands.)
            Expr::Negative(inner) => {
                if inner.get_type(schema).is_ok_and(|t| is_integer(&t)) {
                    return Ok(Transformed::yes(udf_call(
                        CheckedNegate::new(),
                        vec![inner.as_ref().clone()],
                    )));
                }
            }
            // A correlated scalar subquery Trino runs but DataFusion's
            // decorrelation needs an aggregate for (see `udf::subquery`).
            Expr::ScalarSubquery(subquery) => {
                if let Some(replacement) =
                    super::subquery::aggregate_correlated_scalar_subquery(subquery)?
                {
                    return Ok(Transformed::yes(replacement));
                }
            }
            // A computed `LIKE` pattern: Trino has no default escape
            // character (`\` is a literal backslash), where DataFusion
            // always treats `\` as the escape. Literal patterns were
            // translated during the AST rewrite; a computed varchar pattern
            // doubles its backslashes at run time. (Non-varchar patterns
            // never get here: the strict checker refused them before the
            // analyzer ran.)
            Expr::Like(like) if !matches!(like.pattern.as_ref(), Expr::Literal(..)) => {
                if let Ok(t) = like.pattern.get_type(schema)
                    && matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
                {
                    let mut replacement = like.clone();
                    replacement.pattern = Box::new(Expr::ScalarFunction(ScalarFunction::new_udf(
                        datafusion::functions::string::replace(),
                        vec![
                            like.pattern.as_ref().clone(),
                            Expr::Literal(ScalarValue::from("\\"), None),
                            Expr::Literal(ScalarValue::from("\\\\"), None),
                        ],
                    )));
                    return Ok(Transformed::yes(Expr::Like(replacement)));
                }
            }
            // `max` / `min` over arrays rank by Trino's array ordering
            // operator, which refuses NULL elements — as `ORDER BY` on an
            // array already does. Arrow's kernels would answer instead, and
            // disagree with `array_max` / `array_min` while they were at it.
            // An aggregate's own `ORDER BY` (`array_agg(x ORDER BY x)`) is
            // the same ordering and gets the same guard.
            Expr::AggregateFunction(agg) => {
                if let Some(replacement) = guard_array_ordering(agg, schema) {
                    return Ok(Transformed::yes(Expr::AggregateFunction(replacement)));
                }
                if matches!(agg.func.name(), "sum" | "avg" | "max")
                    && let [arg] = agg.params.args.as_slice()
                    && let Ok(t) = arg.get_type(schema)
                    && let Some(func) = trino_aggregate(agg.func.name(), &t)
                {
                    let mut replacement = agg.clone();
                    replacement.func = Arc::new(func);
                    return Ok(Transformed::yes(Expr::AggregateFunction(replacement)));
                }
            }
            Expr::WindowFunction(window) => {
                // The window forms of `max` / `min` and the window
                // `ORDER BY` rank arrays through Trino's array ordering
                // operator, same as their aggregate counterparts.
                let ranks_argument = matches!(
                    &window.fun,
                    WindowFunctionDefinition::AggregateUDF(f) if matches!(f.name(), "max" | "min")
                );
                let is_array = |e: &Expr| e.get_type(schema).is_ok_and(|t| is_list(&t));
                let guard_needed = |e: &Expr| is_array(e) && !is_array_sort_key(e);
                let argument_needs = ranks_argument && window.params.args.iter().any(guard_needed);
                let order_needs = window.params.order_by.iter().any(|s| guard_needed(&s.expr));
                if argument_needs || order_needs {
                    let mut replacement: WindowFunction = window.as_ref().clone();
                    if argument_needs {
                        for arg in &mut replacement.params.args {
                            if is_array(arg) {
                                *arg = array_sort_key(arg);
                            }
                        }
                    }
                    if order_needs {
                        for sort in &mut replacement.params.order_by {
                            if is_array(&sort.expr) {
                                sort.expr = array_sort_key(&sort.expr);
                            }
                        }
                    }
                    return Ok(Transformed::yes(Expr::WindowFunction(Box::new(
                        replacement,
                    ))));
                }
                if let WindowFunctionDefinition::AggregateUDF(func) = &window.fun
                    && matches!(func.name(), "sum" | "avg" | "max")
                    && let [arg] = window.params.args.as_slice()
                    && let Ok(t) = arg.get_type(schema)
                    && let Some(func) = trino_aggregate(func.name(), &t)
                {
                    let mut replacement: WindowFunction = window.as_ref().clone();
                    replacement.fun = WindowFunctionDefinition::AggregateUDF(Arc::new(func));
                    return Ok(Transformed::yes(Expr::WindowFunction(Box::new(
                        replacement,
                    ))));
                }
            }
            // `ARRAY[...]` (DataFusion's `make_array`) unifies its
            // elements the same way: a double mixed with a decimal is a
            // double array on Trino, where DataFusion's element coercion
            // lands on `decimal(38,15)` and prints `1.000000000000000`.
            Expr::ScalarFunction(call) if call.func.name() == "make_array" => {
                let arg_refs: Vec<&Expr> = call.args.iter().collect();
                if let Some(widened) = widen_exact_to_double(&arg_refs, schema) {
                    let mut replacement = call.clone();
                    replacement.args = widened;
                    return Ok(Transformed::yes(Expr::ScalarFunction(replacement)));
                }
            }
            // Trino's common supertype of double and an exact number is
            // double; DataFusion widens both into a decimal. `nullif` and
            // `greatest` additionally need IEEE / NaN-smallest semantics
            // when a float is involved (see `floats`).
            Expr::ScalarFunction(call)
                if matches!(
                    call.func.name(),
                    "greatest" | "least" | "coalesce" | "nullif"
                ) =>
            {
                let name = call.func.name().to_string();
                let arg_refs: Vec<&Expr> = call.args.iter().collect();
                let widened = widen_exact_to_double(&arg_refs, schema);
                let args: Vec<Expr> = widened.clone().unwrap_or_else(|| call.args.to_vec());
                let any_float = call
                    .args
                    .iter()
                    .filter_map(|a| a.get_type(schema).ok())
                    .any(|t| is_float(&t));
                // Trino types `nullif(a, b)` as the *first* argument's type
                // and coerces only the comparison to the common supertype
                // (ExpressionAnalyzer.visitNullIfExpression), so
                // `nullif(2, 1.0)` is integer 2; DataFusion's `nullif`
                // widens the result to the supertype too. Mixed-type calls
                // become `CASE WHEN a = b THEN NULL ELSE a END` with the
                // coercion confined to the `WHEN`. With a float operand the
                // comparison further uses Trino's IEEE EQUAL operator
                // (`nullif(0e0, -0e0)` is NULL, `nullif(NaN, NaN)` is NaN;
                // DataFusion's kernel compares bit patterns).
                if name == "nullif" && args.len() == 2 {
                    let types: Vec<DataType> = call
                        .args
                        .iter()
                        .filter_map(|a| a.get_type(schema).ok())
                        .collect();
                    let mixed = types.len() == 2
                        && types[0] != types[1]
                        && !types.iter().any(|t| matches!(t, DataType::Null));
                    if any_float || mixed {
                        let comparison = if any_float {
                            ieee_cmp(Operator::Eq, args[0].clone(), args[1].clone())
                        } else {
                            Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                                Box::new(args[0].clone()),
                                Operator::Eq,
                                Box::new(args[1].clone()),
                            ))
                        };
                        let case = datafusion::logical_expr::expr::Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(comparison),
                                Box::new(Expr::Literal(ScalarValue::Null, None)),
                            )],
                            // The *original* first argument, not the widened
                            // copy the comparison uses: the result keeps its
                            // type.
                            else_expr: Some(Box::new(call.args[0].clone())),
                        };
                        return Ok(Transformed::yes(Expr::Case(case)));
                    }
                }
                // `greatest` / `least` over arrays rank by Trino's array
                // ordering operator too: NULL elements are an error, not a
                // silent (and self-contradictory) winner.
                if matches!(name.as_str(), "greatest" | "least")
                    && call
                        .args
                        .iter()
                        .filter_map(|a| a.get_type(schema).ok())
                        .any(|t| is_list(&t))
                    && !call.args.iter().any(is_array_sort_key)
                {
                    let mut replacement = call.clone();
                    replacement.args = args.iter().map(array_sort_key).collect();
                    return Ok(Transformed::yes(Expr::ScalarFunction(replacement)));
                }
                // Trino's `greatest` ranks NaN smallest; DataFusion's ranks
                // it largest. (`least` agrees between the two.)
                if name == "greatest" && any_float {
                    return Ok(Transformed::yes(udf_call(TrinoFloatGreatest::new(), args)));
                }
                if let Some(widened) = widened {
                    let mut replacement = call.clone();
                    replacement.args = widened;
                    return Ok(Transformed::yes(Expr::ScalarFunction(replacement)));
                }
            }
            // Trino's `||` on arrays is NULL-propagating for the *array*
            // operands (`ARRAY[1] || NULL-array` is NULL, while `ARRAY[1] ||
            // NULL-element` appends a NULL element); DataFusion's
            // `array_concat` treats a NULL array as empty.
            Expr::ScalarFunction(call)
                if matches!(
                    call.func.name(),
                    "array_concat" | "array_append" | "array_prepend"
                ) =>
            {
                let array_args: Vec<&Expr> = match call.func.name() {
                    "array_append" => call.args.first().into_iter().collect(),
                    "array_prepend" => call.args.get(1).into_iter().collect(),
                    _ => call.args.iter().collect(),
                };
                let condition = array_args
                    .into_iter()
                    .map(|a| Expr::IsNull(Box::new(a.clone())))
                    .reduce(Expr::or);
                if let Some(condition) = condition {
                    let case = datafusion::logical_expr::expr::Case {
                        expr: None,
                        when_then_expr: vec![(
                            Box::new(condition),
                            Box::new(Expr::Literal(ScalarValue::Null, None)),
                        )],
                        else_expr: Some(Box::new(e.clone())),
                    };
                    return Ok(Transformed::yes(Expr::Case(case)));
                }
            }
            // `x IN (a, b)` over floats: Trino evaluates it with the EQUAL
            // operator; Arrow's `InList` kernel would treat NaN as equal to
            // NaN. Expanded to an OR of IEEE equalities (three-valued logic
            // included).
            Expr::InList(in_list) => {
                let mut types: Vec<DataType> = Vec::new();
                for item in std::iter::once(&in_list.expr)
                    .map(|b| b.as_ref())
                    .chain(in_list.list.iter())
                {
                    let Ok(t) = item.get_type(schema) else {
                        types.clear();
                        break;
                    };
                    types.push(t);
                }
                if !types.is_empty()
                    && types.iter().any(is_float)
                    && types.iter().all(numeric_or_null)
                {
                    let chain = in_list
                        .list
                        .iter()
                        .map(|item| {
                            ieee_cmp(Operator::Eq, in_list.expr.as_ref().clone(), item.clone())
                        })
                        .reduce(Expr::or)
                        .expect("IN lists are non-empty");
                    let replacement = if in_list.negated {
                        Expr::Not(Box::new(chain))
                    } else {
                        chain
                    };
                    return Ok(Transformed::yes(replacement));
                }
            }
            // `x BETWEEN a AND b` over floats: expanded to IEEE `>=` / `<=`
            // (exactly Trino's definition), so NaN bounds compare false.
            Expr::Between(between) => {
                let types: Vec<DataType> = [&between.expr, &between.low, &between.high]
                    .iter()
                    .filter_map(|b| b.get_type(schema).ok())
                    .collect();
                if types.len() == 3
                    && types.iter().any(is_float)
                    && types.iter().all(numeric_or_null)
                {
                    let conjunction = ieee_cmp(
                        Operator::GtEq,
                        between.expr.as_ref().clone(),
                        between.low.as_ref().clone(),
                    )
                    .and(ieee_cmp(
                        Operator::LtEq,
                        between.expr.as_ref().clone(),
                        between.high.as_ref().clone(),
                    ));
                    let replacement = if between.negated {
                        Expr::Not(Box::new(conjunction))
                    } else {
                        conjunction
                    };
                    return Ok(Transformed::yes(replacement));
                }
                // Over arrays it expands to Trino's array `>=` / `<=`, so
                // the NULL-element rules apply here too (`ARRAY['a', 'b']
                // BETWEEN ARRAY['a', NULL] AND ARRAY['z']` used to answer
                // true while the bare `>=` errored).
                if types.len() == 3 && types.iter().all(is_list) {
                    let bound = |op, other: &Expr| {
                        rewrite_binary(&between.expr, op, other, schema)
                            .expect("array operands rewrite")
                    };
                    let conjunction = bound(Operator::GtEq, &between.low)
                        .and(bound(Operator::LtEq, &between.high));
                    let replacement = if between.negated {
                        Expr::Not(Box::new(conjunction))
                    } else {
                        conjunction
                    };
                    return Ok(Transformed::yes(replacement));
                }
            }
            Expr::Case(case) => {
                // A simple CASE whose operand involves floats compares with
                // the EQUAL operator on Trino; converted to the searched
                // form over IEEE equality.
                let mut converted: Option<datafusion::logical_expr::expr::Case> = None;
                if let Some(operand) = &case.expr {
                    let mut types: Vec<DataType> = Vec::new();
                    for item in std::iter::once(operand.as_ref())
                        .chain(case.when_then_expr.iter().map(|(w, _)| w.as_ref()))
                    {
                        let Ok(t) = item.get_type(schema) else {
                            types.clear();
                            break;
                        };
                        types.push(t);
                    }
                    if !types.is_empty()
                        && types.iter().any(is_float)
                        && types.iter().all(numeric_or_null)
                    {
                        converted = Some(datafusion::logical_expr::expr::Case {
                            expr: None,
                            when_then_expr: case
                                .when_then_expr
                                .iter()
                                .map(|(when, then)| {
                                    (
                                        Box::new(ieee_cmp(
                                            Operator::Eq,
                                            operand.as_ref().clone(),
                                            when.as_ref().clone(),
                                        )),
                                        then.clone(),
                                    )
                                })
                                .collect(),
                            else_expr: case.else_expr.clone(),
                        });
                    }
                }
                let current = converted.as_ref().unwrap_or(case);
                let mut results: Vec<&Expr> = current
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| t.as_ref())
                    .collect();
                if let Some(otherwise) = &current.else_expr {
                    results.push(otherwise);
                }
                if let Some(mut widened) = widen_exact_to_double(&results, schema) {
                    let mut replacement = current.clone();
                    let else_expr = if current.else_expr.is_some() {
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
                if let Some(converted) = converted {
                    return Ok(Transformed::yes(Expr::Case(converted)));
                }
            }
            _ => {}
        }
        Ok(Transformed::no(e))
    })
}

/// A `VALUES` row expression with DataFusion's own coercion cast peeled
/// off, so both the strict checker and the row rebuilder below see the type
/// the user wrote rather than the column type the planner unified on.
///
/// Every user-written `CAST` reaches the plan as a `trino_*` UDF call, or —
/// for integer targets only — as `CAST(trino_round_for_cast(x) AS ...)`, so
/// a plain `Expr::Cast` to a varchar or numeric type is always the
/// planner's. `TRY_CAST` is user syntax and never stripped.
pub(crate) fn values_row_expr(expr: &Expr) -> &Expr {
    let Expr::Cast(cast) = expr else {
        return expr;
    };
    let coercion_target = matches!(
        cast.field.data_type(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(..)
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    );
    let user_cast = matches!(cast.expr.as_ref(), Expr::ScalarFunction(call)
        if matches!(call.func.name(), "trino_round_for_cast" | "trino_try_round_for_cast"));
    if coercion_target && !user_cast {
        values_row_expr(&cast.expr)
    } else {
        expr
    }
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
    // A numeric literal, or the `trino_double('1e2')` an exponent literal
    // arrives as.
    let is_numeric_literal = |e: &Expr| match e {
        Expr::Literal(
            ScalarValue::Int32(_)
            | ScalarValue::Int64(None)
            | ScalarValue::Decimal128(..)
            | ScalarValue::Float64(_),
            _,
        ) => true,
        _ => is_double_literal(e),
    };
    // The planner also widened a *user* cast to a narrow integer type
    // (`VALUES (CAST(NULL AS INTEGER)), (5)`: the row was planned as
    // `CAST(CAST(... AS Int32) AS Int64)` because the bare literal was
    // still `bigint` at planning time). Dropping the outer cast lets the
    // builder unify `integer` with `integer` again, as Trino does; a
    // user-written `CAST(x AS BIGINT)` is a `trino_round_for_cast` call
    // under the cast, never a cast, so it is left alone.
    let widened_narrow_integer_cast = |cast: &Cast| {
        cast.field.data_type() == &DataType::Int64
            && matches!(
                cast.expr.as_ref(),
                Expr::Cast(inner)
                    if matches!(
                        inner.field.data_type(),
                        DataType::Int8 | DataType::Int16 | DataType::Int32
                    )
            )
    };
    let strip_planner_cast = |e: Expr| match e {
        Expr::Cast(cast)
            if (matches!(
                cast.field.data_type(),
                DataType::Int64 | DataType::Decimal128(..) | DataType::Float64
            ) && is_numeric_literal(&cast.expr))
                || widened_narrow_integer_cast(&cast) =>
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
        // The planner typed a bare NULL after the column type it had
        // inferred from the `bigint` literals; an untyped NULL lets the
        // builder infer the type from the narrowed literals again.
        let is_typed_null = |e: &Expr| {
            matches!(
                e,
                Expr::Literal(
                    ScalarValue::Int64(None)
                        | ScalarValue::Int32(None)
                        | ScalarValue::Decimal128(None, ..)
                        | ScalarValue::Float64(None),
                    _
                )
            )
        };
        let any_numeric_value = rows.iter().any(|row| {
            matches!(
                &row[column],
                Expr::Literal(
                    ScalarValue::Int32(Some(_))
                        | ScalarValue::Int64(Some(_))
                        | ScalarValue::Decimal128(Some(_), ..)
                        | ScalarValue::Float64(Some(_)),
                    _
                ) | Expr::Cast(_)
            ) || is_double_literal(&row[column])
        });
        if any_numeric_value {
            for row in &mut rows {
                if is_typed_null(&row[column]) {
                    row[column] = Expr::Literal(ScalarValue::Null, None);
                }
            }
        }
        // A column mixing doubles and exact numbers is double on Trino,
        // whatever the rows are: literals, `CAST(1 AS DOUBLE)`, or any
        // other expression. DataFusion would unify the column into a wide
        // decimal instead (`decimal(30,15)`), which prints differently.
        let empty = DFSchema::empty();
        let cell_type = |e: &Expr| values_row_expr(e).get_type(&empty).ok();
        let types: Vec<DataType> = rows
            .iter()
            .filter_map(|row| cell_type(&row[column]))
            .collect();
        if types.iter().any(is_float) && types.iter().any(is_exact_numeric) {
            for row in &mut rows {
                // Drop DataFusion's coercion cast first: casting the
                // already-widened decimal back to double would round-trip
                // the value through `decimal(30,15)` and lose magnitude.
                let cell = values_row_expr(&row[column]).clone();
                row[column] = if cell_type(&cell).is_some_and(|t| is_exact_numeric(&t)) {
                    to_double(&cell)
                } else {
                    cell
                };
            }
        }
    }
    rows
}

/// The `trino_double('1e2')` shim an exponent literal is rewritten to.
fn is_double_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call)
        if call.func.name() == "trino_double"
            && matches!(call.args.as_slice(), [Expr::Literal(ScalarValue::Utf8(_), _)]))
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

/// Equi-join keys of `DOUBLE` / `REAL` type cannot go through DataFusion's
/// hash join: its key equality is Arrow's (NaN matches NaN), while Trino
/// joins with the EQUAL operator (NaN matches nothing). The float pairs are
/// moved out of `on` into the join filter as IEEE-equality UDF calls, which
/// forces a nested-loop join with Trino's semantics. `USING` float keys are
/// refused instead: the `USING` projection logic needs the pairs in `on`.
fn move_float_join_keys(join: &Join) -> Result<Option<LogicalPlan>> {
    let schema = {
        let mut merged = DFSchema::empty();
        merged.merge(join.left.schema());
        merged.merge(join.right.schema());
        merged
    };
    let float_pair = |l: &Expr, r: &Expr| {
        let (Ok(lt), Ok(rt)) = (l.get_type(&schema), r.get_type(&schema)) else {
            return false;
        };
        is_float(&lt) || is_float(&rt)
    };
    if !join.on.iter().any(|(l, r)| float_pair(l, r)) {
        return Ok(None);
    }
    if join.join_constraint == JoinConstraint::Using {
        return Err(unsupported_error(
            "JOIN ... USING on DOUBLE / REAL keys",
            "Trino compares double join keys with IEEE equality (NaN never matches), which \
             DataFusion's hash join does not reproduce; write the join condition with ON",
        ));
    }
    let mut moved = join.clone();
    let (float, keep): (Vec<_>, Vec<_>) = moved.on.into_iter().partition(|(l, r)| float_pair(l, r));
    moved.on = keep;
    let condition = float
        .into_iter()
        .map(|(l, r)| ieee_cmp(Operator::Eq, l, r))
        .reduce(Expr::and)
        .expect("at least one float pair");
    moved.filter = Some(match moved.filter.take() {
        Some(filter) => filter.and(condition),
        None => condition,
    });
    Ok(Some(LogicalPlan::Join(moved)))
}

/// Trino's `JOIN ... USING (k)` exposes one `k`: the left value for an inner
/// or left join, the right value for a right join, `coalesce(l.k, r.k)` for
/// a full join. DataFusion keeps both `l.k` and `r.k` and resolves an
/// unqualified `k` (and `SELECT *`) to whichever copy it picks, which is
/// NULL on the unmatched side of an outer join. Wraps an outer `USING` join
/// in a projection that gives *both* copies Trino's value, so every
/// reference sees it (qualified references are refused at translation, as
/// on Trino).
fn using_join_projection(join: &Join) -> Result<Option<LogicalPlan>> {
    if join.join_constraint != JoinConstraint::Using
        || !matches!(
            join.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full
        )
    {
        return Ok(None);
    }
    let pairs: Vec<(Column, Column)> = join
        .on
        .iter()
        .filter_map(|(l, r)| Some((l.try_as_col()?.clone(), r.try_as_col()?.clone())))
        .collect();
    if pairs.len() != join.on.len() {
        return Ok(None);
    }
    let value_of = |left: &Column, right: &Column| -> Expr {
        let (l, r) = (Expr::Column(left.clone()), Expr::Column(right.clone()));
        match join.join_type {
            JoinType::Left => l,
            JoinType::Right => r,
            _ => datafusion::functions::core::expr_fn::coalesce(vec![l, r]),
        }
    };
    let exprs: Vec<Expr> = join
        .schema
        .iter()
        .map(|(qualifier, field)| {
            let column = Column::from((qualifier, field));
            match pairs.iter().find(|(l, r)| *l == column || *r == column) {
                Some((l, r)) => value_of(l, r).alias_qualified(qualifier.cloned(), field.name()),
                None => Expr::Column(column),
            }
        })
        .collect();
    let projection = Projection::try_new(exprs, Arc::new(LogicalPlan::Join(join.clone())))?;
    Ok(Some(LogicalPlan::Projection(projection)))
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
            if let LogicalPlan::Join(join) = &transformed.data
                && let Some(fixed) = move_float_join_keys(join)?
            {
                transformed = Transformed::yes(fixed);
            }
            if let LogicalPlan::Join(join) = &transformed.data
                && let Some(projection) = using_join_projection(join)?
            {
                transformed = Transformed::yes(projection);
            }
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
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "trino_semantics"
    }
}
