//! IEEE semantics for `DOUBLE` / `REAL` special values (`NaN`, `-0.0`).
//!
//! Trino's comparison operators on doubles are Java's primitive operators
//! (`DoubleType`: `EQUAL` is `left == right`, `LESS_THAN` is `left <
//! right`), so every `=`, `<`, `<=`, `>`, `>=` involving `NaN` is `false`,
//! `NaN <> NaN` is `true`, and `-0.0 = 0.0` is `true`. Arrow's comparison
//! kernels use a total order where `NaN` equals `NaN` and exceeds every
//! number, so DataFusion would answer differently. The
//! [`TrinoSemantics`](super::arithmetic::TrinoSemantics) analyzer routes
//! float comparisons (including `IN` lists, `BETWEEN`, simple `CASE`
//! operands, `nullif`, and equi-join keys) through [`TrinoIeeeCmp`].
//!
//! Trino's max-side extremes (`greatest`, `max`, `array_max`) use
//! `COMPARISON_UNORDERED_FIRST`, which ranks `NaN` *smallest*, so `max`
//! over `{1.0, NaN}` is `1.0` and is `NaN` only when every value is `NaN`.
//! (`least` / `min` / `array_min` rank `NaN` largest — `UNORDERED_LAST` —
//! which coincides with Arrow's total order, so DataFusion's own functions
//! already match.) [`TrinoFloatGreatest`] and [`TrinoFloatMax`] implement
//! the max side; ties between `-0.0` and `0.0` follow `Double.compare`
//! (`total_cmp`), as in Trino.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, Float64Builder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::casts::trino_type_name;
use super::type_mismatch;

/// The float scalar UDFs.
pub fn all() -> Vec<ScalarUDF> {
    let mut udfs: Vec<ScalarUDF> = [
        Operator::Eq,
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
    ]
    .into_iter()
    .map(|op| ScalarUDF::new_from_impl(TrinoIeeeCmp::new(op)))
    .collect();
    udfs.push(ScalarUDF::new_from_impl(TrinoFloatGreatest::new()));
    udfs
}

/// The float aggregate UDFs.
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::new_from_impl(TrinoFloatMax::new())]
}

/// Whether Trino's max side (`COMPARISON_UNORDERED_FIRST`: `NaN` smallest,
/// `-0.0 < 0.0`) prefers `candidate` over `best`.
fn max_prefers(best: f64, candidate: f64) -> bool {
    if candidate.is_nan() {
        false
    } else if best.is_nan() {
        true
    } else {
        candidate.total_cmp(&best) == std::cmp::Ordering::Greater
    }
}

/// `trino_ieee_eq(a, b)` and friends: comparison of two numeric values with
/// Java's primitive double semantics. Both arguments are evaluated as
/// doubles; NULL propagates.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoIeeeCmp {
    signature: Signature,
    op: Operator,
}

impl TrinoIeeeCmp {
    /// New instance for one of `= <> < <= > >=`.
    pub fn new(op: Operator) -> Self {
        debug_assert!(matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ));
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            op,
        }
    }
}

impl ScalarUDFImpl for TrinoIeeeCmp {
    fn name(&self) -> &str {
        match self.op {
            Operator::Eq => "trino_ieee_eq",
            Operator::NotEq => "trino_ieee_neq",
            Operator::Lt => "trino_ieee_lt",
            Operator::LtEq => "trino_ieee_lte",
            Operator::Gt => "trino_ieee_gt",
            _ => "trino_ieee_gte",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        for t in arg_types {
            if !matches!(
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
            ) {
                return Err(type_mismatch(format!(
                    "Cannot apply operator: {} {} {}",
                    trino_type_name(&arg_types[0]),
                    self.op,
                    trino_type_name(&arg_types[1])
                )));
            }
        }
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let left = cast(&args.args[0].to_array(rows)?, &DataType::Float64)?;
        let right = cast(&args.args[1].to_array(rows)?, &DataType::Float64)?;
        let (left, right) = (
            left.as_primitive::<Float64Type>(),
            right.as_primitive::<Float64Type>(),
        );
        let mut out = BooleanBuilder::with_capacity(rows);
        for i in 0..rows {
            if left.is_null(i) || right.is_null(i) {
                out.append_null();
                continue;
            }
            let (a, b) = (left.value(i), right.value(i));
            out.append_value(match self.op {
                Operator::Eq => a == b,
                Operator::NotEq => a != b,
                Operator::Lt => a < b,
                Operator::LtEq => a <= b,
                Operator::Gt => a > b,
                _ => a >= b,
            });
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `trino_float_greatest(a, b, ...)`: `greatest` with `NaN` ranked smallest
/// (Trino's `COMPARISON_UNORDERED_FIRST`), so `greatest(1e0, NaN)` is `1.0`
/// and `NaN` only results when every value is `NaN`. NULL handling is the
/// caller's: the rewriter already wraps `greatest` in a CASE that returns
/// NULL when any argument is NULL, so this function simply skips NULLs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoFloatGreatest {
    signature: Signature,
}

impl Default for TrinoFloatGreatest {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoFloatGreatest {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

/// The float type `greatest` returns for these argument types: `real` only
/// when every argument is `real`, else `double` (matching DataFusion's
/// coercion for float/integer mixes).
fn greatest_return_type(arg_types: &[DataType]) -> DataType {
    if arg_types.iter().all(|t| *t == DataType::Float32) {
        DataType::Float32
    } else {
        DataType::Float64
    }
}

impl ScalarUDFImpl for TrinoFloatGreatest {
    fn name(&self) -> &str {
        "trino_float_greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(greatest_return_type(arg_types))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let target = args.return_type().clone();
        let columns: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| Ok(cast(&a.to_array(rows)?, &DataType::Float64)?))
            .collect::<Result<_>>()?;
        let mut out = Float64Builder::with_capacity(rows);
        for i in 0..rows {
            let mut best: Option<f64> = None;
            for column in &columns {
                let column = column.as_primitive::<Float64Type>();
                if column.is_null(i) {
                    continue;
                }
                let v = column.value(i);
                best = Some(match best {
                    None => v,
                    Some(b) if max_prefers(b, v) => v,
                    Some(b) => b,
                });
            }
            out.append_option(best);
        }
        let result: ArrayRef = Arc::new(out.finish());
        Ok(ColumnarValue::Array(if target == DataType::Float64 {
            result
        } else {
            cast(&result, &target)?
        }))
    }
}

/// `trino_float_max(x)`: `max` over `DOUBLE` / `REAL` with `NaN` ranked
/// smallest, as Trino's `max` (`MinMaxCompare` uses
/// `COMPARISON_UNORDERED_FIRST`); Arrow's `max` ranks `NaN` largest and
/// would return `NaN` whenever one appears. (`min` needs no substitute:
/// both engines rank `NaN` largest there.)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoFloatMax {
    signature: Signature,
}

impl Default for TrinoFloatMax {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoFloatMax {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for TrinoFloatMax {
    fn name(&self) -> &str {
        "trino_float_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            t @ (DataType::Float32 | DataType::Float64) => Ok(t.clone()),
            other => Err(type_mismatch(format!(
                "expected a double or real argument, got {}",
                trino_type_name(other)
            ))),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FloatMaxAccumulator {
            real: args.return_field.data_type() == &DataType::Float32,
            best: None,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[float max]", args.name),
            DataType::Float64,
            true,
        ))])
    }
}

/// Running Trino-style float max; `None` until a value arrives.
#[derive(Debug)]
struct FloatMaxAccumulator {
    real: bool,
    best: Option<f64>,
}

impl FloatMaxAccumulator {
    fn add(&mut self, value: f64) {
        self.best = Some(match self.best {
            None => value,
            Some(b) if max_prefers(b, value) => value,
            Some(b) => b,
        });
    }
}

impl Accumulator for FloatMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let floats = cast(&values[0], &DataType::Float64)?;
        for v in floats.as_primitive::<Float64Type>().iter().flatten() {
            self.add(v);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(if self.real {
            ScalarValue::Float32(self.best.map(|v| v as f32))
        } else {
            ScalarValue::Float64(self.best)
        })
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(self.best)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_ranks_nan_smallest_and_negative_zero_below_zero() {
        assert!(max_prefers(f64::NAN, 1.0));
        assert!(!max_prefers(1.0, f64::NAN));
        assert!(!max_prefers(f64::NAN, f64::NAN));
        assert!(max_prefers(1.0, 2.0));
        assert!(!max_prefers(2.0, 1.0));
        assert!(max_prefers(-0.0, 0.0));
        assert!(!max_prefers(0.0, -0.0));
    }

    #[test]
    fn float_max_accumulates_like_trino() {
        let mut acc = FloatMaxAccumulator {
            real: false,
            best: None,
        };
        for v in [f64::NAN, 1.0, f64::NAN, 0.5] {
            acc.add(v);
        }
        assert_eq!(acc.best, Some(1.0));
        let mut all_nan = FloatMaxAccumulator {
            real: false,
            best: None,
        };
        all_nan.add(f64::NAN);
        assert!(all_nan.best.unwrap().is_nan());
    }

    #[test]
    fn greatest_return_type_is_real_only_when_all_real() {
        assert_eq!(
            greatest_return_type(&[DataType::Float32, DataType::Float32]),
            DataType::Float32
        );
        assert_eq!(
            greatest_return_type(&[DataType::Float32, DataType::Float64]),
            DataType::Float64
        );
        assert_eq!(
            greatest_return_type(&[DataType::Int32, DataType::Float64]),
            DataType::Float64
        );
    }
}
