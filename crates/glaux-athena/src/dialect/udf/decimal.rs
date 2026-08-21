//! Trino's `DECIMAL` arithmetic, casts, and aggregates.
//!
//! DataFusion's decimal rules differ from Trino's in ways that change the
//! *value*, not only the text: division keeps extra fractional digits
//! (`1.5 / 2` is `0.75000` instead of `0.8`), `avg` adds four decimal
//! places, `decimal(38,0)` addition wraps past the precision, and
//! `CAST(double AS DECIMAL(38,37))` scales in floating point. The UDFs here
//! implement Trino's result types (`decimal_result_type`), HALF_UP rounding
//! on exact 256-bit intermediates, and overflow errors.
//!
//! - `trino_checked_add/sub/mul` (in [`super::arithmetic`]) call
//!   [`decimal_binary`] for decimal operands.
//! - `trino_decimal_div(a, b)`: `decimal / decimal` (integers are decimals of
//!   their natural precision).
//! - `trino_to_decimal(x, p, s)` / `trino_try_to_decimal`: `CAST(x AS
//!   DECIMAL(p, s))` from double (exact binary expansion, like Java's `new
//!   BigDecimal(double)`), decimal, integer, boolean, and varchar.
//! - `trino_decimal_sum` / `trino_decimal_avg`: `sum` (`decimal(38, s)`) and
//!   `avg` (`decimal(p, s)`, HALF_UP) over decimals, overflow-checked.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, PrimitiveArray,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int64Type, i256,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::casts::trino_type_name;
use super::{data_error, string_array, type_mismatch};

/// The decimal scalar UDFs.
pub fn scalar_udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoDecimalDiv::new()),
        ScalarUDF::new_from_impl(TrinoToDecimal::new(false)),
        ScalarUDF::new_from_impl(TrinoToDecimal::new(true)),
    ]
}

/// The decimal aggregate UDFs.
pub fn aggregate_udfs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::new_from_impl(TrinoDecimalAgg::new(DecimalAgg::Sum)),
        AggregateUDF::new_from_impl(TrinoDecimalAgg::new(DecimalAgg::Avg)),
    ]
}

/// Trino's maximum decimal precision.
pub const MAX_PRECISION: u8 = 38;

/// `(precision, scale)` of a type in Trino's decimal arithmetic: decimals
/// as declared, integers as the decimal that holds them (`bigint` is
/// `decimal(19,0)`). `None` for non-exact types.
pub(crate) fn decimal_type_of(data_type: &DataType) -> Option<(u8, i8)> {
    Some(match data_type {
        DataType::Decimal128(p, s) => (*p, *s),
        DataType::Int8 => (3, 0),
        DataType::Int16 => (5, 0),
        DataType::Int32 => (10, 0),
        DataType::Int64 => (19, 0),
        _ => return None,
    })
}

/// Whether a type takes part in decimal arithmetic.
pub(crate) fn is_exact_numeric(data_type: &DataType) -> bool {
    decimal_type_of(data_type).is_some()
}

/// Trino's result type for `op` on two decimals.
pub(crate) fn decimal_result_type(op: Operator, a: (u8, i8), b: (u8, i8)) -> Result<(u8, i8)> {
    let (p1, s1) = (i32::from(a.0), i32::from(a.1));
    let (p2, s2) = (i32::from(b.0), i32::from(b.1));
    let (p, s) = match op {
        Operator::Plus | Operator::Minus => {
            let s = s1.max(s2);
            ((p1 - s1).max(p2 - s2) + s + 1, s)
        }
        Operator::Multiply => (p1 + p2, s1 + s2),
        Operator::Divide => (p1 + s2 + (s2 - s1).max(0), s1.max(s2)),
        Operator::Modulo => ((p1 - s1).min(p2 - s2) + s1.max(s2), s1.max(s2)),
        other => return plan_err!("decimal arithmetic: unsupported operator {other}"),
    };
    if s > i32::from(MAX_PRECISION) {
        return Err(type_mismatch(format!(
            "DECIMAL scale {s} must be in range [0, {MAX_PRECISION}] (the result of decimal({p1},{s1}) {op} decimal({p2},{s2}))"
        )));
    }
    Ok((p.min(i32::from(MAX_PRECISION)) as u8, s as i8))
}

fn pow10(n: u32) -> i256 {
    i256::from_i128(10)
        .checked_pow(n)
        .expect("10^n fits in 256 bits for the scales used here")
}

fn overflow(what: &str) -> DataFusionError {
    data_error(
        "NUMERIC_VALUE_OUT_OF_RANGE",
        format!("Decimal overflow: {what}"),
    )
}

/// `value` fits in `precision` digits.
fn fits(value: i256, precision: u8) -> bool {
    value
        .checked_abs()
        .is_some_and(|v| v < pow10(u32::from(precision)))
}

/// Unscaled decimal values, one per row.
pub(crate) type Unscaled = Vec<Option<i256>>;

/// The unscaled values of an exact-numeric array as `i256`, plus its
/// decimal type.
pub(crate) fn unscaled_values(array: &ArrayRef) -> Result<(Unscaled, (u8, i8))> {
    let Some(decimal_type) = decimal_type_of(array.data_type()) else {
        return Err(type_mismatch(format!(
            "expected a decimal or integer, got {}",
            trino_type_name(array.data_type())
        )));
    };
    let values = match array.data_type() {
        DataType::Decimal128(_, _) => array
            .as_primitive::<Decimal128Type>()
            .iter()
            .map(|v| v.map(i256::from_i128))
            .collect(),
        _ => cast(array, &DataType::Int64)?
            .as_primitive::<Int64Type>()
            .iter()
            .map(|v| v.map(|v| i256::from_i128(i128::from(v))))
            .collect(),
    };
    Ok((values, decimal_type))
}

fn decimal_array(values: Vec<Option<i256>>, (p, s): (u8, i8)) -> Result<ArrayRef> {
    let narrowed = values
        .into_iter()
        .map(|v| match v {
            None => Ok(None),
            Some(v) if fits(v, p) => Ok(v.to_i128()),
            Some(v) => Err(overflow(&format!("{v} does not fit in decimal({p},{s})"))),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        PrimitiveArray::<Decimal128Type>::from(narrowed).with_precision_and_scale(p, s)?,
    ))
}

/// Divide rounding half away from zero (Trino's `divideRoundUp`).
fn divide_round_up(dividend: i256, divisor: i256) -> Option<i256> {
    let quotient = dividend.checked_div(divisor)?;
    let remainder = dividend.checked_rem(divisor)?;
    let twice = remainder.checked_abs()?.checked_mul(i256::from_i128(2))?;
    if twice >= divisor.checked_abs()? {
        let away = if (dividend.is_negative()) != (divisor.is_negative()) {
            i256::from_i128(-1)
        } else {
            i256::ONE
        };
        quotient.checked_add(away)
    } else {
        Some(quotient)
    }
}

/// `a op b` for decimal operands with Trino's result type and overflow
/// checks.
pub(crate) fn decimal_binary(op: Operator, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
    let (a_values, a_type) = unscaled_values(a)?;
    let (b_values, b_type) = unscaled_values(b)?;
    let (p, s) = decimal_result_type(op, a_type, b_type)?;
    let verb = match op {
        Operator::Plus => "addition",
        Operator::Minus => "subtraction",
        Operator::Multiply => "multiplication",
        _ => "division",
    };
    let out = a_values
        .into_iter()
        .zip(b_values)
        .map(|(x, y)| {
            let (Some(x), Some(y)) = (x, y) else {
                return Ok(None);
            };
            let result = match op {
                Operator::Plus | Operator::Minus => {
                    let x = x.checked_mul(pow10((s - a_type.1) as u32));
                    let y = y.checked_mul(pow10((s - b_type.1) as u32));
                    match (x, y, op) {
                        (Some(x), Some(y), Operator::Plus) => x.checked_add(y),
                        (Some(x), Some(y), _) => x.checked_sub(y),
                        _ => None,
                    }
                }
                Operator::Multiply => x.checked_mul(y),
                Operator::Divide => {
                    if y == i256::from_i128(0) {
                        return Err(data_error("DIVISION_BY_ZERO", "Division by zero"));
                    }
                    let rescale = (s - a_type.1 + b_type.1) as u32;
                    x.checked_mul(pow10(rescale))
                        .and_then(|dividend| divide_round_up(dividend, y))
                }
                _ => return plan_err!("decimal arithmetic: unsupported operator {op}"),
            };
            match result {
                Some(v) if fits(v, p) => Ok(Some(v)),
                _ => Err(overflow(&format!(
                    "decimal {verb} result does not fit in decimal({p},{s})"
                ))),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    decimal_array(out, (p, s))
}

// ---------------------------------------------------------------------------
// trino_decimal_div(a, b)
// ---------------------------------------------------------------------------

/// `a / b` on decimals: see the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDecimalDiv {
    signature: Signature,
}

impl Default for TrinoDecimalDiv {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoDecimalDiv {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoDecimalDiv {
    fn name(&self) -> &str {
        "trino_decimal_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (Some(a), Some(b)) = (
            decimal_type_of(&arg_types[0]),
            decimal_type_of(&arg_types[1]),
        ) else {
            return Err(type_mismatch(format!(
                "Cannot apply operator: {} / {}",
                trino_type_name(&arg_types[0]),
                trino_type_name(&arg_types[1])
            )));
        };
        let (p, s) = decimal_result_type(Operator::Divide, a, b)?;
        Ok(DataType::Decimal128(p, s))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let a = args.args[0].to_array(rows)?;
        let b = args.args[1].to_array(rows)?;
        Ok(ColumnarValue::Array(decimal_binary(
            Operator::Divide,
            &a,
            &b,
        )?))
    }
}

// ---------------------------------------------------------------------------
// CAST(x AS DECIMAL(p, s))
// ---------------------------------------------------------------------------

/// Exact decimal expansion of a double, rounded half up to `scale` digits:
/// `x = m · 2^e` exactly, so `x · 10^s = m · 10^s · 2^e`, computed in 256
/// bits (what Java's `new BigDecimal(double).setScale(s, HALF_UP)` gives).
/// `None` when the value does not fit in 256 bits (it cannot fit any
/// decimal either).
pub(crate) fn double_to_unscaled(value: f64, scale: u8) -> Option<i256> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(i256::from_i128(0));
    }
    let bits = value.to_bits();
    let negative = (bits >> 63) == 1;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, exponent) = if exponent_bits == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1u64 << 52), exponent_bits - 1075)
    };
    let scaled = i256::from_i128(i128::from(mantissa)).checked_mul(pow10(u32::from(scale)))?;
    let magnitude = if exponent >= 0 {
        if exponent > 200 {
            return None;
        }
        scaled.checked_mul(i256::from_i128(2).checked_pow(exponent as u32)?)?
    } else {
        let shift = (-exponent) as u32;
        if shift > 250 {
            // Smaller than 2^-197 times 10^scale: rounds to zero.
            i256::from_i128(0)
        } else {
            let divisor = i256::from_i128(2).checked_pow(shift)?;
            divide_round_up(scaled, divisor)?
        }
    };
    Some(if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    })
}

/// Rescale an unscaled decimal value from `from` to `to` fractional digits,
/// rounding half away from zero when digits are dropped.
pub(crate) fn rescale(value: i256, from: i8, to: i8) -> Option<i256> {
    if to >= from {
        value.checked_mul(pow10((to - from) as u32))
    } else {
        divide_round_up(value, pow10((from - to) as u32))
    }
}

/// Parse a decimal string (`[+-]digits[.digits][e[+-]digits]`) to an
/// unscaled value at `scale`, rounding half away from zero. Surrounding
/// whitespace is rejected: Trino's varchar → decimal cast goes through
/// Java's `new BigDecimal(String)`, which refuses `' 1.5 '` (unlike its
/// integer and date casts, which trim).
fn parse_decimal_text(text: &str, scale: u8) -> Option<i256> {
    let (mantissa_text, exponent) = match text.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (text, 0),
    };
    let (negative, unsigned) = match mantissa_text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (
            false,
            mantissa_text.strip_prefix('+').unwrap_or(mantissa_text),
        ),
    };
    let (int_part, frac_part) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if int_part.is_empty() && frac_part.is_empty()
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let digits = digits.trim_start_matches('0');
    if digits.len() > 76 {
        return None;
    }
    let mut value = i256::from_i128(0);
    for b in digits.bytes() {
        value = value
            .checked_mul(i256::from_i128(10))?
            .checked_add(i256::from_i128(i128::from(b - b'0')))?;
    }
    // The text's scale is the fraction length minus the exponent.
    let text_scale = frac_part.len() as i64 - i64::from(exponent);
    let text_scale = i8::try_from(text_scale.clamp(-127, 127)).ok()?;
    let value = if text_scale < 0 {
        value.checked_mul(pow10((-text_scale) as u32))?
    } else {
        value
    };
    let value = rescale(value, text_scale.max(0), scale as i8)?;
    Some(if negative {
        value.wrapping_neg()
    } else {
        value
    })
}

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoToDecimal {
    signature: Signature,
    try_cast: bool,
}

impl TrinoToDecimal {
    /// New instance.
    pub fn new(try_cast: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
            try_cast,
        }
    }
}

fn target_type(args: &[Option<&ScalarValue>]) -> Result<(u8, i8)> {
    let literal = |i: usize| -> Option<i64> {
        match args.get(i)? {
            Some(ScalarValue::Int64(Some(v))) => Some(*v),
            Some(ScalarValue::Int32(Some(v))) => Some(i64::from(*v)),
            _ => None,
        }
    };
    let (Some(p), Some(s)) = (literal(1), literal(2)) else {
        return plan_err!("CAST(... AS DECIMAL(p, s)): precision and scale must be literals");
    };
    if !(1..=i64::from(MAX_PRECISION)).contains(&p) {
        return Err(type_mismatch(format!(
            "DECIMAL precision must be in range [1, {MAX_PRECISION}]: {p}"
        )));
    }
    if s < 0 || s > p {
        return Err(type_mismatch(format!(
            "DECIMAL scale must be in range [0, precision ({p})]: {s}"
        )));
    }
    Ok((p as u8, s as i8))
}

impl ScalarUDFImpl for TrinoToDecimal {
    fn name(&self) -> &str {
        if self.try_cast {
            "trino_try_to_decimal"
        } else {
            "trino_to_decimal"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!("trino_to_decimal: return_field_from_args is used")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = target_type(args.scalar_arguments)?;
        match args.arg_fields[0].data_type() {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => Ok(Arc::new(Field::new(
                self.name(),
                DataType::Decimal128(p, s),
                true,
            ))),
            other => Err(type_mismatch(format!(
                "Cannot cast {} to decimal({p},{s})",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let DataType::Decimal128(p, s) = args.return_type().clone() else {
            unreachable!("return type is the target decimal");
        };
        let mut failed: Option<String> = None;
        let mut failure = |text: String| {
            if failed.is_none() {
                failed = Some(text);
            }
            None
        };
        let values: Vec<Option<i256>> = match input.data_type() {
            DataType::Null => vec![None; rows],
            DataType::Boolean => input
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean")
                .iter()
                .map(|v| {
                    v.map(|b| {
                        i256::from_i128(i128::from(b))
                            .checked_mul(pow10(s as u32))
                            .expect("fits")
                    })
                })
                .collect(),
            DataType::Float64 | DataType::Float32 => {
                let doubles: Vec<Option<f64>> = if input.data_type() == &DataType::Float64 {
                    input.as_primitive::<Float64Type>().iter().collect()
                } else {
                    input
                        .as_primitive::<Float32Type>()
                        .iter()
                        .map(|v| v.map(f64::from))
                        .collect()
                };
                doubles
                    .into_iter()
                    .map(|v| {
                        let v = v?;
                        match double_to_unscaled(v, s as u8) {
                            Some(u) if fits(u, p) => Some(u),
                            _ => failure(format!(
                                "Cannot cast DOUBLE '{}' to DECIMAL({p}, {s})",
                                crate::results::java_double_text(v)
                            )),
                        }
                    })
                    .collect()
            }
            DataType::Decimal128(from_precision, from_scale) => {
                let (from_precision, from_scale) = (*from_precision, *from_scale);
                let decimals = input.as_primitive::<Decimal128Type>();
                (0..rows)
                    .map(|i| {
                        if decimals.is_null(i) {
                            return None;
                        }
                        match rescale(i256::from_i128(decimals.value(i)), from_scale, s) {
                            Some(u) if fits(u, p) => Some(u),
                            _ => failure(format!(
                                "Cannot cast DECIMAL({from_precision}, {from_scale}) '{}' to DECIMAL({p}, {s})",
                                decimals.value_as_string(i)
                            )),
                        }
                    })
                    .collect()
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let (ints, _) = unscaled_values(&input)?;
                ints.into_iter()
                    .map(|v| {
                        let v = v?;
                        match v.checked_mul(pow10(s as u32)) {
                            Some(u) if fits(u, p) => Some(u),
                            _ => failure(format!(
                                "Cannot cast {} '{v}' to DECIMAL({p}, {s})",
                                trino_type_name(input.data_type()).to_uppercase()
                            )),
                        }
                    })
                    .collect()
            }
            _ => {
                let strings = string_array("CAST", &args.args[0], rows)?;
                (0..rows)
                    .map(|i| {
                        if strings.is_null(i) {
                            return None;
                        }
                        let text = strings.value(i);
                        match parse_decimal_text(text, s as u8) {
                            Some(u) if fits(u, p) => Some(u),
                            _ => failure(format!(
                                "Cannot cast VARCHAR '{text}' to DECIMAL({p}, {s})"
                            )),
                        }
                    })
                    .collect()
            }
        };
        if let Some(message) = failed
            && !self.try_cast
        {
            return Err(data_error("INVALID_CAST_ARGUMENT", message));
        }
        decimal_array(values, (p, s)).map(ColumnarValue::Array)
    }
}

// ---------------------------------------------------------------------------
// sum / avg over decimals
// ---------------------------------------------------------------------------

/// Which aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalAgg {
    /// `sum(decimal(p, s)) → decimal(38, s)`.
    Sum,
    /// `avg(decimal(p, s)) → decimal(p, s)`, HALF_UP.
    Avg,
}

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDecimalAgg {
    signature: Signature,
    kind: DecimalAgg,
}

impl TrinoDecimalAgg {
    /// New instance.
    pub fn new(kind: DecimalAgg) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            kind,
        }
    }
}

impl AggregateUDFImpl for TrinoDecimalAgg {
    fn name(&self) -> &str {
        match self.kind {
            DecimalAgg::Sum => "trino_decimal_sum",
            DecimalAgg::Avg => "trino_decimal_avg",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0], self.kind) {
            (DataType::Decimal128(_, s), DecimalAgg::Sum) => {
                Ok(DataType::Decimal128(MAX_PRECISION, *s))
            }
            (DataType::Decimal128(p, s), DecimalAgg::Avg) => Ok(DataType::Decimal128(*p, *s)),
            (other, _) => Err(type_mismatch(format!(
                "expected a decimal argument, got {}",
                trino_type_name(other)
            ))),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let DataType::Decimal128(p, s) = args.return_field.data_type().clone() else {
            return plan_err!("decimal aggregate: return type is not a decimal");
        };
        Ok(if args.is_distinct {
            Box::new(DistinctDecimalAccumulator {
                kind: self.kind,
                result: (p, s),
                values: HashSet::new(),
            })
        } else {
            Box::new(DecimalAccumulator {
                kind: self.kind,
                result: (p, s),
                sum: i256::from_i128(0),
                count: 0,
            })
        })
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let DataType::Decimal128(_, s) = args.return_field.data_type().clone() else {
            return plan_err!("decimal aggregate: return type is not a decimal");
        };
        Ok(if args.is_distinct {
            vec![Arc::new(Field::new_list(
                format!("{}[distinct decimals]", args.name),
                Field::new_list_field(DataType::Decimal128(MAX_PRECISION, s), true),
                false,
            ))]
        } else {
            vec![
                Arc::new(Field::new(
                    format!("{}[sum]", args.name),
                    DataType::FixedSizeBinary(32),
                    true,
                )),
                Arc::new(Field::new(
                    format!("{}[count]", args.name),
                    DataType::Int64,
                    true,
                )),
            ]
        })
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }
}

fn finish(kind: DecimalAgg, (p, s): (u8, i8), sum: i256, count: i64) -> Result<ScalarValue> {
    if count == 0 {
        return Ok(ScalarValue::Decimal128(None, p, s));
    }
    let value = match kind {
        DecimalAgg::Sum => sum,
        DecimalAgg::Avg => divide_round_up(sum, i256::from_i128(i128::from(count)))
            .ok_or_else(|| overflow("average"))?,
    };
    if !fits(value, p) {
        return Err(overflow(&format!(
            "{} does not fit in decimal({p},{s})",
            match kind {
                DecimalAgg::Sum => "sum",
                DecimalAgg::Avg => "average",
            }
        )));
    }
    Ok(ScalarValue::Decimal128(value.to_i128(), p, s))
}

#[derive(Debug)]
struct DecimalAccumulator {
    kind: DecimalAgg,
    result: (u8, i8),
    sum: i256,
    count: i64,
}

fn decimal_values(array: &ArrayRef) -> Result<Decimal128Array> {
    Ok(cast(
        array,
        &DataType::Decimal128(
            MAX_PRECISION,
            decimal_type_of(array.data_type()).map_or(0, |t| t.1),
        ),
    )?
    .as_primitive::<Decimal128Type>()
    .clone())
}

impl Accumulator for DecimalAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in decimal_values(&values[0])?.iter().flatten() {
            self.sum = self
                .sum
                .checked_add(i256::from_i128(v))
                .ok_or_else(|| overflow("sum"))?;
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        finish(self.kind, self.result, self.sum, self.count)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::FixedSizeBinary(32, Some(self.sum.to_le_bytes().to_vec())),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = states[0]
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| DataFusionError::Internal("decimal sum state".into()))?;
        let counts = states[1].as_primitive::<Int64Type>();
        for i in 0..sums.len() {
            if sums.is_null(i) {
                continue;
            }
            let bytes: [u8; 32] = sums.value(i).try_into().expect("32-byte state");
            self.sum = self
                .sum
                .checked_add(i256::from_le_bytes(bytes))
                .ok_or_else(|| overflow("sum"))?;
            self.count += counts.value(i);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for v in decimal_values(&values[0])?.iter().flatten() {
            self.sum = self
                .sum
                .checked_sub(i256::from_i128(v))
                .ok_or_else(|| overflow("sum"))?;
            self.count -= 1;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct DistinctDecimalAccumulator {
    kind: DecimalAgg,
    result: (u8, i8),
    values: HashSet<i128>,
}

impl Accumulator for DistinctDecimalAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values
            .extend(decimal_values(&values[0])?.iter().flatten());
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut sum = i256::from_i128(0);
        for v in &self.values {
            sum = sum
                .checked_add(i256::from_i128(*v))
                .ok_or_else(|| overflow("sum"))?;
        }
        finish(self.kind, self.result, sum, self.values.len() as i64)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<i128>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let s = self.result.1;
        let values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|v| ScalarValue::Decimal128(Some(*v), MAX_PRECISION, s))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &values,
            &DataType::Decimal128(MAX_PRECISION, s),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for list in states[0].as_list::<i32>().iter().flatten() {
            self.values
                .extend(list.as_primitive::<Decimal128Type>().iter().flatten());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(v: i128) -> i256 {
        i256::from_i128(v)
    }

    #[test]
    fn result_types_follow_trino() {
        assert_eq!(
            decimal_result_type(Operator::Divide, (2, 1), (10, 0)).unwrap(),
            (2, 1)
        );
        assert_eq!(
            decimal_result_type(Operator::Divide, (10, 0), (2, 1)).unwrap(),
            (12, 1)
        );
        assert_eq!(
            decimal_result_type(Operator::Divide, (38, 0), (10, 0)).unwrap(),
            (38, 0)
        );
        assert_eq!(
            decimal_result_type(Operator::Plus, (38, 0), (10, 0)).unwrap(),
            (38, 0)
        );
        assert_eq!(
            decimal_result_type(Operator::Plus, (2, 1), (10, 0)).unwrap(),
            (12, 1)
        );
        assert_eq!(
            decimal_result_type(Operator::Multiply, (10, 2), (10, 2)).unwrap(),
            (20, 4)
        );
    }

    #[test]
    fn division_rounds_half_up_and_overflows_loudly() {
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(15), Some(10), Some(-15)])
                .with_precision_and_scale(2, 1)
                .unwrap(),
        );
        let b: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![2, 3, 2]));
        let out = decimal_binary(Operator::Divide, &a, &b).unwrap();
        let out = out.as_primitive::<Decimal128Type>();
        assert_eq!(out.data_type(), &DataType::Decimal128(2, 1));
        assert_eq!(out.values().as_ref(), &[8, 3, -8]);

        let big: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(
                99_999_999_999_999_999_999_999_999_999_999_999_999,
            )])
            .with_precision_and_scale(38, 0)
            .unwrap(),
        );
        let one: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![1]));
        let err = decimal_binary(Operator::Plus, &big, &one).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");
        let zero: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![0]));
        let err = decimal_binary(Operator::Divide, &big, &zero).unwrap_err();
        assert!(err.to_string().contains("Division by zero"), "{err}");
    }

    #[test]
    fn doubles_expand_exactly() {
        // 1.0 at scale 37: exactly 1 followed by 37 zeros.
        assert_eq!(double_to_unscaled(1.0, 37), Some(pow10(37)));
        // 1/3 as a double is 0.333333333333333314829616256247390992939472198486328125.
        assert_eq!(
            double_to_unscaled(1.0 / 3.0, 20),
            i256::from_string("33333333333333331483")
        );
        assert_eq!(double_to_unscaled(2.5, 0), Some(d(3)));
        assert_eq!(double_to_unscaled(-2.5, 0), Some(d(-3)));
        assert_eq!(double_to_unscaled(0.125, 2), Some(d(13)));
        assert_eq!(double_to_unscaled(1e-30, 5), Some(d(0)));
        assert_eq!(double_to_unscaled(f64::NAN, 2), None);
        assert_eq!(double_to_unscaled(1e300, 2), None);
    }

    #[test]
    fn decimal_text_parses_exactly() {
        assert_eq!(parse_decimal_text("1.5", 2), Some(d(150)));
        assert_eq!(parse_decimal_text("-1.25", 1), Some(d(-13)));
        // Trino's varchar → decimal cast rejects surrounding whitespace
        // (java.math.BigDecimal), unlike its integer casts, which trim.
        assert_eq!(parse_decimal_text(" 1.5 ", 2), None);
        assert_eq!(parse_decimal_text(" -1.25 ", 1), None);
        assert_eq!(parse_decimal_text("1e2", 0), Some(d(100)));
        assert_eq!(parse_decimal_text("1.5E-1", 3), Some(d(150)));
        assert_eq!(parse_decimal_text("abc", 1), None);
        assert_eq!(parse_decimal_text("", 1), None);
        assert_eq!(rescale(d(125), 2, 1), Some(d(13)));
        assert_eq!(rescale(d(-125), 2, 1), Some(d(-13)));
        assert_eq!(rescale(d(12), 0, 2), Some(d(1200)));
    }
}
