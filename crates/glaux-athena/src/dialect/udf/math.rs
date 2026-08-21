//! Math functions whose DataFusion namesakes return a different type or a
//! different value from Trino's.
//!
//! - `round(double[, n])`: Trino computes `Math.round(x · 10ⁿ) / 10ⁿ` in
//!   floating point (sign-flipped for negatives, with a BigInteger fallback
//!   when `Math.round` saturates), so the double product decides:
//!   `round(2.675, 2)` is `2.68` (the product is exactly `267.5`) but
//!   `round(1.005, 2)` is `1.0` (the product is `100.49999999999999`).
//!   DataFusion rounds the exact decimal instead and gives `2.67` / `1.01`.
//!   Integer inputs keep their type
//!   (`round(bigint, -2)` rounds the integer); decimals round HALF_UP
//!   exactly into Trino's result type.
//! - `floor` / `ceil` / `truncate` / `sign`: the result has the argument's
//!   type in Trino (`floor(5)` is the bigint `5`, `sign(2.5)` is
//!   `decimal(1,0)`); DataFusion returns a double for integers and keeps
//!   the scale for decimals.
//! - `truncate(double, n)` is refused: the two-argument overload is
//!   DECIMAL-only in both Trino (Athena engine v3) and Presto 0.217
//!   (engine v2), so Athena answers it with a function-resolution error.
//!   DataFusion's two-argument `trunc` would happily return a value
//!   instead.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, i256,
};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::casts::trino_type_name;
use super::decimal::MAX_PRECISION;
use super::{data_error, is_integer, type_mismatch, unsupported_error};

/// The math UDFs.
pub fn all() -> Vec<ScalarUDF> {
    [
        MathOp::Round,
        MathOp::Floor,
        MathOp::Ceil,
        MathOp::Truncate,
        MathOp::Sign,
    ]
    .into_iter()
    .map(|op| ScalarUDF::new_from_impl(TrinoMath::new(op)))
    .chain(std::iter::once(ScalarUDF::new_from_impl(TrinoSqrt::new())))
    .chain(std::iter::once(ScalarUDF::new_from_impl(
        TrinoRandomBound::new(),
    )))
    .collect()
}

/// `trino_random(n, r)`: Trino's bounded `random(n)` overload — a uniform
/// value in `[0, n)` with `n`'s own integer type, an
/// `INVALID_FUNCTION_ARGUMENT` for `n <= 0`. `r` is DataFusion's `random()`
/// draw in `[0, 1)`, which the rewriter passes in so the randomness (and
/// the per-row volatility) stays DataFusion's.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoRandomBound {
    signature: Signature,
}

impl Default for TrinoRandomBound {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoRandomBound {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for TrinoRandomBound {
    fn name(&self) -> &str {
        "trino_random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            // Trino declares `random(integer) -> integer` and
            // `random(bigint) -> bigint`; the narrower integer types widen
            // to `integer`, as they do everywhere else.
            DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function random: expected random() or \
                 random(integer) / random(bigint)",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let wide = args.return_field.data_type() == &DataType::Int64;
        let bounds = cast(&args.args[0].to_array(rows)?, &DataType::Int64)?;
        let bounds = bounds.as_primitive::<Int64Type>();
        let draws = cast(&args.args[1].to_array(rows)?, &DataType::Float64)?;
        let draws = draws.as_primitive::<Float64Type>();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            if bounds.is_null(i) {
                out.push(None);
                continue;
            }
            let bound = bounds.value(i);
            if bound <= 0 {
                return Err(super::user_error(
                    "random",
                    format!("bound must be positive (got {bound})"),
                ));
            }
            // `floor(r · bound)` with r in [0, 1) is uniform on [0, bound);
            // the clamp guards the rounding edge only.
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            let value = ((draws.value(i) * bound as f64) as i64).clamp(0, bound - 1);
            out.push(Some(value));
        }
        let array: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from_iter(out));
        Ok(ColumnarValue::Array(if wide {
            array
        } else {
            cast(&array, &DataType::Int32)?
        }))
    }
}

/// `trino_sqrt(x)`: Java's `Math.sqrt` — `NaN` for a negative argument
/// (DataFusion raises "cannot take square root of a negative number").
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoSqrt {
    signature: Signature,
}

impl Default for TrinoSqrt {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoSqrt {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoSqrt {
    fn name(&self) -> &str {
        "trino_sqrt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            t if is_integer(t) => Ok(DataType::Float64),
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) | DataType::Null => {
                Ok(DataType::Float64)
            }
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function sqrt. Expected: sqrt(double)",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let doubles = cast(&input, &DataType::Float64)?;
        let out: PrimitiveArray<Float64Type> =
            doubles.as_primitive::<Float64Type>().unary(f64::sqrt);
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Which function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MathOp {
    Round,
    Floor,
    Ceil,
    Truncate,
    Sign,
}

impl MathOp {
    fn trino_name(self) -> &'static str {
        match self {
            Self::Round => "round",
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Truncate => "truncate",
            Self::Sign => "sign",
        }
    }
}

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoMath {
    signature: Signature,
    op: MathOp,
}

impl TrinoMath {
    /// New instance.
    pub fn new(op: MathOp) -> Self {
        let signature = match op {
            MathOp::Round | MathOp::Truncate => Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            _ => Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        };
        Self { signature, op }
    }
}

/// Java's `Math.pow(10, n)`, the `factor` in Trino's `round`: the
/// correctly rounded double for `10ⁿ` (`0` on underflow, infinity on
/// overflow). Going through Rust's decimal parser rather than `f64::powi`
/// is what makes it correctly rounded — `powi` rounds at every
/// multiplication and drifts.
///
/// For `0 <= n <= 22`, `10ⁿ` is exactly representable and `Math.pow` is
/// specified to return it exactly ("if both arguments are integers, then
/// the result is exactly equal to the mathematical result … if that result
/// can in fact be represented exactly as a double"), so we provably agree.
/// Outside that range Java only promises to be within 1 ulp, so a JVM
/// could in principle hand Trino a `factor` one ulp off the correctly
/// rounded one we use. Such an `n` rounds a double at a decimal place it
/// has no significant digits in, where the result is the input back or a
/// zero either way, which is why this is a documented tolerance and not a
/// refusal.
fn pow10_f64(n: i64) -> f64 {
    format!("1e{n}").parse::<f64>().unwrap_or(f64::INFINITY)
}

/// Java's `Math.round(double)`: `floor(a + ½)` on the exact real value,
/// with ties rounding toward positive infinity, saturating at the `long`
/// range and returning 0 for NaN.
fn java_math_round(a: f64) -> i64 {
    if a.is_nan() {
        return 0;
    }
    if a.abs() < (1i64 << 52) as f64 {
        // `a - floor(a)` is exact below 2⁵², so the tie test is exact —
        // `java_math_round(0.49999999999999994)` is 0, where
        // `floor(a + 0.5)` in doubles would round up to 1.
        let t = a.floor();
        let r = if a - t >= 0.5 { t + 1.0 } else { t };
        r as i64
    } else {
        // Integral already; Rust's saturating cast matches Java's `(long)`.
        a as i64
    }
}

/// Trino's `round(double, n)`, statement for statement from
/// `MathFunctions.round(double, long)`: `factor = Math.pow(10, n)`, then
/// `Math.round(|x| · factor) / factor` with the sign reapplied. The
/// rounding therefore sees the *double product*, not the exact decimal:
/// `round(2.675, 2)` is `2.68` (the product is exactly `267.5`, though the
/// double `2.675` is below 2.675) but `round(1.005, 2)` is `1.0` (the
/// product is `100.49999999999999`).
///
/// Three edge branches come straight from Trino and each returns a value —
/// the function is declared `neverFails = true` there, so it has no error
/// path at all:
///
/// - `factor == 0` (n ≲ -324, where `10ⁿ` underflows): every finite `x`
///   rounds to `sign · 0.0`, so negatives give `-0.0`.
/// - `Math.round` saturating at `Long.MAX_VALUE` with a *finite* product:
///   Guava's `DoubleMath.roundToBigInteger(…, HALF_UP)`. A finite double
///   ≥ 2⁶³ is already integral, so that round-trip is the identity and the
///   result is just the division.
/// - `Math.round` saturating with an *infinite* product: `x` unchanged
///   (Trino's comment notes rounding is a no-op at that magnitude).
pub(crate) fn trino_round_double(x: f64, decimals: i64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    let factor = pow10_f64(decimals);
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    if factor == 0.0 {
        return sign * 0.0;
    }
    let rescaled = sign * x * factor;
    let rounded = java_math_round(rescaled);
    if rounded != i64::MAX {
        return sign * (rounded as f64 / factor);
    }
    if rescaled.is_infinite() {
        return x;
    }
    sign * (rescaled / factor)
}

/// Trino's single-argument `truncate(double)`:
/// `Math.signum(x) · Math.floor(|x|)`. The two-argument DOUBLE / REAL form
/// does not exist on Trino and is refused in [`TrinoMath::return_type`].
pub(crate) fn trino_truncate_double(x: f64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    x.signum() * x.abs().floor()
}

fn pow10_i256(n: u32) -> i256 {
    i256::from_i128(10).checked_pow(n).expect("fits")
}

/// Divide rounding half away from zero.
fn round_half_away(value: i256, divisor: i256) -> i256 {
    let quotient = value.wrapping_div(divisor);
    let remainder = value.wrapping_rem(divisor);
    if remainder.wrapping_abs().wrapping_mul(i256::from_i128(2)) >= divisor {
        quotient.wrapping_add(value.signum())
    } else {
        quotient
    }
}

/// Trino's result type for `op` on `decimal(p, s)` with `decimals` places.
fn decimal_result(op: MathOp, p: u8, s: i8, decimals: Option<i64>) -> DataType {
    let integral_precision =
        (u32::from(p) - s as u32 + u32::from(s > 0)).min(u32::from(MAX_PRECISION)) as u8;
    // Trino's one-argument `truncate` declares `decimal(max(1, p - s), 0)`
    // (`MathFunctions.Truncate`'s `@Constraint(variable = "rp", expression =
    // "max(1, p - s)")`), one digit narrower than `ceiling` / `floor`'s
    // `p - s + min(s, 1)` whenever the argument has a scale: `truncate(1.98)`
    // is `decimal(1,0)`, not `decimal(2,0)`.
    let truncated_precision = u32::from(p)
        .saturating_sub(s.max(0) as u32)
        .clamp(1, u32::from(MAX_PRECISION)) as u8;
    match (op, decimals) {
        (MathOp::Sign, _) => DataType::Decimal128(1, 0),
        (MathOp::Round, Some(_)) => DataType::Decimal128((p + 1).min(MAX_PRECISION), s),
        (MathOp::Truncate, Some(_)) => DataType::Decimal128(p, s),
        (MathOp::Truncate, None) => DataType::Decimal128(truncated_precision, 0),
        _ => DataType::Decimal128(integral_precision, 0),
    }
}

fn decimal_value(op: MathOp, v: i128, s: i8, decimals: Option<i64>) -> Result<i128> {
    let value = i256::from_i128(v);
    let scale = pow10_i256(s as u32);
    let out = match (op, decimals) {
        (MathOp::Sign, _) => value.signum(),
        (MathOp::Floor, _) => {
            let q = value.wrapping_div(scale);
            if value.is_negative() && value.wrapping_rem(scale) != i256::from_i128(0) {
                q.wrapping_sub(i256::ONE)
            } else {
                q
            }
        }
        (MathOp::Ceil, _) => {
            let q = value.wrapping_div(scale);
            if value.is_positive() && value.wrapping_rem(scale) != i256::from_i128(0) {
                q.wrapping_add(i256::ONE)
            } else {
                q
            }
        }
        (MathOp::Truncate, None) => value.wrapping_div(scale),
        (MathOp::Round, None) => round_half_away(value, scale),
        (MathOp::Round | MathOp::Truncate, Some(n)) => {
            // Keep `n` fractional digits (n < 0 zeroes integer digits).
            let drop = i64::from(s) - n;
            if drop <= 0 {
                value
            } else if drop > 76 {
                i256::from_i128(0)
            } else {
                let unit = pow10_i256(drop as u32);
                let kept = if op == MathOp::Round {
                    round_half_away(value, unit)
                } else {
                    value.wrapping_div(unit)
                };
                kept.checked_mul(unit)
                    .ok_or_else(|| data_error("NUMERIC_VALUE_OUT_OF_RANGE", "Decimal overflow"))?
            }
        }
    };
    out.to_i128()
        .ok_or_else(|| data_error("NUMERIC_VALUE_OUT_OF_RANGE", "Decimal overflow"))
}

fn round_integer(op: MathOp, v: i64, decimals: Option<i64>) -> Result<i64> {
    match (op, decimals) {
        (MathOp::Sign, _) => Ok(v.signum()),
        (MathOp::Round, Some(n)) if n < 0 => {
            let Some(factor) = 10i64.checked_pow(n.unsigned_abs().min(u32::MAX as u64) as u32)
            else {
                return Ok(0);
            };
            let quotient = v / factor;
            let remainder = v % factor;
            let rounded = if remainder.unsigned_abs() * 2 >= factor.unsigned_abs() {
                quotient + v.signum()
            } else {
                quotient
            };
            rounded.checked_mul(factor).ok_or_else(|| {
                data_error("NUMERIC_VALUE_OUT_OF_RANGE", "integer overflow in round")
            })
        }
        _ => Ok(v),
    }
}

fn decimals_arg(op: MathOp, args: &ScalarFunctionArgs) -> Result<Option<i64>> {
    let Some(arg) = args.args.get(1) else {
        return Ok(None);
    };
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => Ok(Some(*n)),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => Ok(Some(i64::from(*n))),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(n))) => Ok(Some(i64::from(*n))),
        ColumnarValue::Scalar(ScalarValue::Int8(Some(n))) => Ok(Some(i64::from(*n))),
        ColumnarValue::Scalar(s) if s.is_null() => Ok(None),
        other => Err(super::user_error(
            op.trino_name(),
            format!(
                "the number of decimal places must be an integer literal, got {}",
                trino_type_name(&other.data_type())
            ),
        )),
    }
}

fn map_primitive<T: ArrowPrimitiveType>(
    input: &ArrayRef,
    f: impl Fn(T::Native) -> Result<T::Native>,
) -> Result<ArrayRef> {
    let array = input.as_primitive::<T>();
    let mut out: Vec<Option<T::Native>> = Vec::with_capacity(array.len());
    for v in array.iter() {
        out.push(v.map(&f).transpose()?);
    }
    Ok(Arc::new(PrimitiveArray::<T>::from_iter(out)))
}

impl ScalarUDFImpl for TrinoMath {
    fn name(&self) -> &str {
        match self.op {
            MathOp::Round => "trino_round",
            MathOp::Floor => "trino_floor",
            MathOp::Ceil => "trino_ceil",
            MathOp::Truncate => "trino_truncate",
            MathOp::Sign => "trino_sign",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(n) = arg_types.get(1)
            && !is_integer(n)
            && !matches!(n, DataType::Null)
        {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}, {}) for function {}: the number of decimal places must be an integer",
                trino_type_name(&arg_types[0]),
                trino_type_name(n),
                self.op.trino_name()
            )));
        }
        if self.op == MathOp::Truncate
            && arg_types.len() > 1
            && matches!(arg_types[0], DataType::Float32 | DataType::Float64)
        {
            return Err(unsupported_error(
                "truncate(double, n)",
                "Trino (Athena engine v3) has no two-argument truncate for DOUBLE / REAL \
                 (only DECIMAL), and Presto 0.217 (engine v2) computed a different value; \
                 truncate a DECIMAL, or use the one-argument truncate(x)",
            ));
        }
        match &arg_types[0] {
            t @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(t.clone())
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 | DataType::Null => Ok(DataType::Float64),
            DataType::Decimal128(p, s) => {
                Ok(decimal_result(self.op, *p, *s, arg_types.get(1).map(|_| 0)))
            }
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function {}. Expected a numeric argument",
                trino_type_name(other),
                self.op.trino_name()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let decimals = decimals_arg(self.op, &args)?;
        let op = self.op;
        let out: ArrayRef = match input.data_type() {
            DataType::Null => cast(&input, &DataType::Float64)?,
            DataType::Float64 => map_primitive::<Float64Type>(&input, |x| {
                Ok(match (op, decimals) {
                    (MathOp::Round, n) => trino_round_double(x, n.unwrap_or(0)),
                    (MathOp::Floor, _) => x.floor(),
                    (MathOp::Ceil, _) => x.ceil(),
                    (MathOp::Truncate, _) => trino_truncate_double(x),
                    (MathOp::Sign, _) => {
                        if x.is_nan() {
                            x
                        } else {
                            x.signum() * f64::from(u8::from(x != 0.0))
                        }
                    }
                })
            })?,
            DataType::Float32 => map_primitive::<Float32Type>(&input, |x| {
                let x = f64::from(x);
                Ok(match (op, decimals) {
                    (MathOp::Round, n) => trino_round_double(x, n.unwrap_or(0)) as f32,
                    (MathOp::Floor, _) => x.floor() as f32,
                    (MathOp::Ceil, _) => x.ceil() as f32,
                    (MathOp::Truncate, _) => trino_truncate_double(x) as f32,
                    (MathOp::Sign, _) => {
                        if x.is_nan() {
                            x as f32
                        } else {
                            (x.signum() * f64::from(u8::from(x != 0.0))) as f32
                        }
                    }
                })
            })?,
            DataType::Int64 => {
                map_primitive::<Int64Type>(&input, |v| round_integer(op, v, decimals))?
            }
            DataType::Int32 => map_primitive::<Int32Type>(&input, |v| {
                round_integer(op, i64::from(v), decimals).map(|r| r as i32)
            })?,
            DataType::Int16 => map_primitive::<Int16Type>(&input, |v| {
                round_integer(op, i64::from(v), decimals).map(|r| r as i16)
            })?,
            DataType::Int8 => map_primitive::<Int8Type>(&input, |v| {
                round_integer(op, i64::from(v), decimals).map(|r| r as i8)
            })?,
            DataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                let DataType::Decimal128(rp, rs) = decimal_result(op, p, s, decimals.map(|_| 0))
                else {
                    unreachable!()
                };
                let values = input.as_primitive::<Decimal128Type>();
                let mut out = Vec::with_capacity(rows);
                for v in values.iter() {
                    out.push(v.map(|v| decimal_value(op, v, s, decimals)).transpose()?);
                }
                Arc::new(
                    PrimitiveArray::<Decimal128Type>::from(out).with_precision_and_scale(rp, rs)?,
                )
            }
            other => {
                return plan_err!(
                    "{}: unsupported argument type {}",
                    op.trino_name(),
                    trino_type_name(other)
                );
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_double_matches_trino_math_round() {
        let round = trino_round_double;
        // The double product re-rounds: 2.675 · 100 is exactly 267.5.
        assert_eq!(round(2.675, 2), 2.68);
        assert_eq!(round(1.115, 2), 1.12);
        assert_eq!(round(-2.675, 2), -2.68);
        assert_eq!(round(0.015, 2), 0.02); // 0.015 · 100 is exactly 1.5; the tie rounds up
        assert_eq!(round(0.285, 2), 0.28); // 0.285 · 100 = 28.499999999999996
        assert_eq!(round(1.005, 2), 1.0); // 1.005 · 100 = 100.49999999999999
        assert_eq!(round(2.5, 0), 3.0);
        assert_eq!(round(-2.5, 0), -3.0);
        assert_eq!(round(0.125, 2), 0.13);
        // Math.round's tie test is exact, not `floor(x + 0.5)` in doubles.
        assert_eq!(round(0.49999999999999994, 0), 0.0);
        assert_eq!(round(1234.5, -2), 1200.0);
        assert_eq!(round(1250.0, -2), 1300.0);
        // Math.round saturates at 2^63; the BigInteger fallback divides.
        assert_eq!(round(1.0e20, 2), 1.0e20);
        assert_eq!(round(1.5, 99), 1.4999999999999998);
        assert_eq!(round(123.456, 10), 123.456);
        assert_eq!(round(0.0, 400), 0.0); // 0 · Infinity is NaN; Math.round(NaN) = 0
        assert!(round(f64::NAN, 2).is_nan());
        assert_eq!(round(f64::INFINITY, 2), f64::INFINITY);
        // A product that overflows to infinity returns the input unchanged
        // (rounding is a no-op at that magnitude), it does not fail: Trino
        // declares round `neverFails = true`.
        assert_eq!(round(1.0e308, 2), 1.0e308);
        // `10ⁿ` underflowing to 0 rounds every finite input to a signed
        // zero — the branch that would otherwise divide 0 by 0 and give NaN.
        assert_eq!(round(1.5, -400), 0.0);
        assert!(round(1.5, -400).is_sign_positive());
        assert!(round(-1.5, -400).is_sign_negative());
    }

    #[test]
    fn truncate_double_rounds_toward_zero() {
        assert_eq!(trino_truncate_double(2.7), 2.0);
        assert_eq!(trino_truncate_double(-2.7), -2.0);
        assert_eq!(trino_truncate_double(0.0), 0.0);
        assert_eq!(trino_truncate_double(1.0e20), 1.0e20);
        assert!(trino_truncate_double(f64::NAN).is_nan());
    }

    #[test]
    fn decimal_and_integer_forms_keep_trino_types() {
        assert_eq!(
            decimal_result(MathOp::Floor, 2, 1, None),
            DataType::Decimal128(2, 0)
        );
        assert_eq!(
            decimal_result(MathOp::Floor, 10, 0, None),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            decimal_result(MathOp::Round, 4, 3, Some(2)),
            DataType::Decimal128(5, 3)
        );
        assert_eq!(
            decimal_result(MathOp::Truncate, 4, 3, Some(2)),
            DataType::Decimal128(4, 3)
        );
        assert_eq!(
            decimal_result(MathOp::Sign, 4, 3, None),
            DataType::Decimal128(1, 0)
        );
        assert_eq!(decimal_value(MathOp::Floor, -25, 1, None).unwrap(), -3);
        assert_eq!(decimal_value(MathOp::Ceil, -25, 1, None).unwrap(), -2);
        assert_eq!(decimal_value(MathOp::Ceil, 25, 1, None).unwrap(), 3);
        assert_eq!(decimal_value(MathOp::Round, 25, 1, None).unwrap(), 3);
        assert_eq!(decimal_value(MathOp::Round, -25, 1, None).unwrap(), -3);
        assert_eq!(
            decimal_value(MathOp::Round, 2789, 3, Some(2)).unwrap(),
            2790
        );
        assert_eq!(
            decimal_value(MathOp::Truncate, 2789, 3, Some(2)).unwrap(),
            2780
        );
        assert_eq!(decimal_value(MathOp::Truncate, 2789, 3, None).unwrap(), 2);
        assert_eq!(decimal_value(MathOp::Sign, -2789, 3, None).unwrap(), -1);
        assert_eq!(round_integer(MathOp::Round, 1250, Some(-2)).unwrap(), 1300);
        assert_eq!(
            round_integer(MathOp::Round, -1250, Some(-2)).unwrap(),
            -1300
        );
        assert_eq!(round_integer(MathOp::Round, 1249, Some(-2)).unwrap(), 1200);
        assert_eq!(round_integer(MathOp::Floor, 7, None).unwrap(), 7);
    }
}
