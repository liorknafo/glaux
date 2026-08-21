//! Math functions whose DataFusion namesakes return a different type or a
//! different value from Trino's.
//!
//! - `round(double[, n])`: Trino computes `Math.round(x · 10ⁿ) / 10ⁿ` in
//!   floating point (sign-flipped for negatives), so `round(2.675, 2)` is
//!   `2.67` because `2.675 · 100` is `267.49999999999997`; DataFusion gives
//!   `2.68`. Integer inputs keep their type (`round(bigint, -2)` rounds the
//!   integer); decimals round HALF_UP exactly into Trino's result type.
//! - `floor` / `ceil` / `truncate` / `sign`: the result has the argument's
//!   type in Trino (`floor(5)` is the bigint `5`, `sign(2.5)` is
//!   `decimal(1,0)`); DataFusion returns a double for integers and keeps
//!   the scale for decimals.
//! - `truncate(double, n)` truncates the shortest round-trip decimal text
//!   (Java's `BigDecimal.valueOf(double).setScale(n, DOWN)`).

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
use super::decimal::{MAX_PRECISION, double_to_unscaled};
use super::{data_error, is_integer, type_mismatch};
use crate::results::java_double_text;

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
    .collect()
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

/// Trino's `round(double, n)`: the exact binary value of the double rounded
/// HALF_UP at `n` decimal places (`new BigDecimal(x).setScale(n, HALF_UP)`),
/// converted back to the nearest double. `round(2.675, 2)` is therefore
/// `2.67`: the double written `2.675` is `2.67499999999999982…`.
pub(crate) fn trino_round_double(x: f64, decimals: i64) -> Result<f64> {
    if !x.is_finite() || x == 0.0 {
        return Ok(x);
    }
    if !(-60..=60).contains(&decimals) {
        return Err(super::user_error(
            "round",
            format!("rounding a double to {decimals} decimal places is not supported (|n| <= 60)"),
        ));
    }
    let scale = decimals.max(0) as u8;
    let Some(unscaled) = double_to_unscaled(x, scale) else {
        return Err(super::user_error(
            "round",
            format!(
                "cannot round {} to {decimals} decimal places",
                java_double_text(x)
            ),
        ));
    };
    let unscaled = if decimals < 0 {
        let unit = pow10_i256((-decimals) as u32);
        round_half_away(unscaled, unit).wrapping_mul(unit)
    } else {
        unscaled
    };
    // Parse the exact decimal text so the conversion to double rounds once.
    let text = format!("{unscaled}e-{scale}");
    text.parse::<f64>().map_err(|_| {
        super::user_error(
            "round",
            format!(
                "cannot round {} to {decimals} decimal places",
                java_double_text(x)
            ),
        )
    })
}

/// Trino's `truncate(double, n)`: `BigDecimal.valueOf(x).setScale(n, DOWN)`.
pub(crate) fn trino_truncate_double(x: f64, decimals: i64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    if decimals == 0 {
        return x.signum() * x.abs().floor();
    }
    let text = java_double_text(x);
    // Expand Java's shortest text to plain digits: sign, integer digits,
    // fraction digits.
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, text),
    };
    let (mantissa, exponent) = match text.split_once('E') {
        Some((m, e)) => (m.to_string(), e.parse::<i32>().unwrap_or(0)),
        None => (text, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((&mantissa, ""));
    let mut digits: Vec<u8> = format!("{int_part}{frac_part}").into_bytes();
    let mut point = int_part.len() as i32 + exponent;
    while point < 0 {
        digits.insert(0, b'0');
        point += 1;
    }
    while (point as usize) > digits.len() {
        digits.push(b'0');
    }
    let keep = (point as i64 + decimals).clamp(0, digits.len() as i64) as usize;
    for d in &mut digits[keep..] {
        *d = b'0';
    }
    let int_digits: String = String::from_utf8_lossy(&digits[..point as usize]).into_owned();
    let frac_digits: String = String::from_utf8_lossy(&digits[point as usize..]).into_owned();
    let plain = format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        if int_digits.is_empty() {
            "0"
        } else {
            &int_digits
        },
        if frac_digits.is_empty() {
            "0"
        } else {
            &frac_digits
        }
    );
    plain.parse().unwrap_or(x)
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
    match (op, decimals) {
        (MathOp::Sign, _) => DataType::Decimal128(1, 0),
        (MathOp::Round, Some(_)) => DataType::Decimal128((p + 1).min(MAX_PRECISION), s),
        (MathOp::Truncate, Some(_)) => DataType::Decimal128(p, s),
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
                    (MathOp::Round, n) => trino_round_double(x, n.unwrap_or(0))?,
                    (MathOp::Floor, _) => x.floor(),
                    (MathOp::Ceil, _) => x.ceil(),
                    (MathOp::Truncate, n) => trino_truncate_double(x, n.unwrap_or(0)),
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
                    (MathOp::Round, n) => trino_round_double(x, n.unwrap_or(0))? as f32,
                    (MathOp::Floor, _) => x.floor() as f32,
                    (MathOp::Ceil, _) => x.ceil() as f32,
                    (MathOp::Truncate, n) => trino_truncate_double(x, n.unwrap_or(0)) as f32,
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
    fn round_double_rounds_the_exact_binary_value() {
        let round = |x, n| trino_round_double(x, n).unwrap();
        assert_eq!(round(2.675, 2), 2.67);
        assert_eq!(round(1.115, 2), 1.11);
        assert_eq!(round(-2.675, 2), -2.67);
        assert_eq!(round(0.285, 2), 0.28);
        assert_eq!(round(1.005, 2), 1.0);
        assert_eq!(round(2.5, 0), 3.0);
        assert_eq!(round(-2.5, 0), -3.0);
        assert_eq!(round(0.125, 2), 0.13);
        assert_eq!(round(0.49999999999999994, 0), 0.0);
        assert_eq!(round(1234.5, -2), 1200.0);
        assert_eq!(round(1250.0, -2), 1300.0);
        assert_eq!(round(1.0e20, 2), 1.0e20);
        assert_eq!(round(123.456, 10), 123.456);
        assert!(round(f64::NAN, 2).is_nan());
        assert!(trino_round_double(1.5, 99).is_err());
    }

    #[test]
    fn truncate_double_cuts_the_shortest_text() {
        assert_eq!(trino_truncate_double(2.789, 2), 2.78);
        assert_eq!(trino_truncate_double(0.29, 2), 0.29);
        assert_eq!(trino_truncate_double(-2.789, 1), -2.7);
        assert_eq!(trino_truncate_double(2.7, 0), 2.0);
        assert_eq!(trino_truncate_double(-2.7, 0), -2.0);
        assert_eq!(trino_truncate_double(1234.5678, -2), 1200.0);
        assert_eq!(trino_truncate_double(1.5e-5, 6), 1.5e-5);
        assert_eq!(trino_truncate_double(1.5e-5, 5), 1.0e-5);
        assert_eq!(trino_truncate_double(1.0e20, 2), 1.0e20);
        assert_eq!(trino_truncate_double(2.789, 5), 2.789);
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
