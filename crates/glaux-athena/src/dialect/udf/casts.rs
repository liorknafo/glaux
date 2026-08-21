//! `CAST` helpers the rewriter inserts so Trino's cast semantics survive
//! DataFusion's Arrow-based casts.
//!
//! - `trino_round_for_cast(x)`: Trino rounds `double`/`decimal` → integer
//!   HALF_UP (`CAST(2.5 AS BIGINT)` is `3`, `CAST(-2.5 AS BIGINT)` is `-3`);
//!   Arrow truncates. The rewriter wraps the operand of every integer-target
//!   `CAST` in this function, which rounds floating/decimal values and
//!   passes everything else through unchanged, so the Arrow cast that
//!   follows only ever sees integral values. Out-of-range values still fail
//!   in that cast (`INVALID_CAST_ARGUMENT`) or become `NULL` under
//!   `TRY_CAST`, as in Trino.
//! - `trino_varchar(x[, n])` / `trino_try_varchar`: `CAST(x AS
//!   VARCHAR[(n)])` with Trino's text forms — timestamps as `2024-01-05
//!   10:30:00.000`, doubles as Java prints them. A bounded target truncates
//!   a *varchar* source to `n` characters but refuses any other source whose
//!   text is longer (`CAST(12345 AS VARCHAR(2))` is an error on Trino).
//! - `trino_boolean(x)` / `trino_try_boolean`: `CAST(x AS BOOLEAN)`: only
//!   `true` / `false` / `t` / `f` / `1` / `0` (any case) from varchar, where
//!   DataFusion also accepts `yes` / `on`; numbers are `!= 0`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, PrimitiveArray, StringArray, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Float32Type, Float64Type, Int64Type,
};
use arrow::util::display::ArrayFormatter;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, int64_array, string_array, type_mismatch, user_err};
use crate::results::{format_options, java_double_text, java_float_text, timestamp_text};

/// The cast UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoRoundForCast::new()),
        ScalarUDF::new_from_impl(TrinoVarchar::new(false)),
        ScalarUDF::new_from_impl(TrinoVarchar::new(true)),
        ScalarUDF::new_from_impl(TrinoBoolean::new(false)),
        ScalarUDF::new_from_impl(TrinoBoolean::new(true)),
        ScalarUDF::new_from_impl(TrinoFloat::new(false, false)),
        ScalarUDF::new_from_impl(TrinoFloat::new(false, true)),
        ScalarUDF::new_from_impl(TrinoFloat::new(true, false)),
        ScalarUDF::new_from_impl(TrinoFloat::new(true, true)),
    ]
}

/// Java's `Double.parseDouble` / `Float.parseFloat` grammar, which Trino's
/// varchar → `DOUBLE` / `REAL` casts use: surrounding ASCII control
/// characters and spaces are trimmed, the special values are exactly `NaN`,
/// `Infinity`, `+Infinity`, `-Infinity` (Rust would also accept `nan`,
/// `inf`, `infinity` in any case), an optional `d` / `f` suffix is allowed,
/// and hexadecimal floats are refused. `None` for text Java rejects.
pub(crate) fn parse_java_double_text(text: &str) -> Option<String> {
    let trimmed = text.trim_matches(|c: char| c <= ' ');
    match trimmed {
        "NaN" => return Some("NaN".to_string()),
        "Infinity" | "+Infinity" => return Some("inf".to_string()),
        "-Infinity" => return Some("-inf".to_string()),
        _ => {}
    }
    let (sign, body) = match trimmed.strip_prefix(['+', '-']) {
        Some(rest) => (&trimmed[..1], rest),
        None => ("", trimmed),
    };
    let body = body.strip_suffix(['d', 'D', 'f', 'F']).unwrap_or(body);
    // Digits [. Digits] [(e|E) [+-] Digits] with at least one mantissa digit.
    let (mantissa, exponent) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e)),
        None => (body, None),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if int_part.is_empty() && frac_part.is_empty()
        || !digits(int_part)
        || !digits(frac_part)
        || mantissa.matches('.').count() > 1
    {
        return None;
    }
    if let Some(e) = exponent {
        let e_digits = e.strip_prefix(['+', '-']).unwrap_or(e);
        if e_digits.is_empty() || !digits(e_digits) {
            return None;
        }
    }
    Some(format!("{sign}{body}"))
}

/// `trino_double(x)` / `trino_real(x)` (and the `try_` forms): `CAST(x AS
/// DOUBLE / REAL)` with Java's text grammar for varchar sources; every
/// other source goes through Arrow's cast.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoFloat {
    signature: Signature,
    try_cast: bool,
    real: bool,
}

impl TrinoFloat {
    /// New instance.
    pub fn new(try_cast: bool, real: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            try_cast,
            real,
        }
    }

    fn target(&self) -> DataType {
        if self.real {
            DataType::Float32
        } else {
            DataType::Float64
        }
    }

    fn type_name(&self) -> &'static str {
        if self.real { "REAL" } else { "DOUBLE" }
    }
}

impl ScalarUDFImpl for TrinoFloat {
    fn name(&self) -> &str {
        match (self.try_cast, self.real) {
            (false, false) => "trino_double",
            (false, true) => "trino_real",
            (true, false) => "trino_try_double",
            (true, true) => "trino_try_real",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
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
            | DataType::Utf8View => Ok(self.target()),
            other => Err(type_mismatch(format!(
                "Cannot cast {} to {}",
                trino_type_name(other),
                self.type_name().to_lowercase()
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        if !matches!(
            input.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            let options = arrow::compute::CastOptions {
                safe: self.try_cast,
                format_options: Default::default(),
            };
            return Ok(ColumnarValue::Array(arrow::compute::cast_with_options(
                &input,
                &self.target(),
                &options,
            )?));
        }
        let strings = string_array("CAST", &args.args[0], rows)?;
        let mut cleaned = StringBuilder::new();
        for i in 0..rows {
            if strings.is_null(i) {
                cleaned.append_null();
                continue;
            }
            match parse_java_double_text(strings.value(i)) {
                Some(text) => cleaned.append_value(text),
                None if self.try_cast => cleaned.append_null(),
                None => {
                    return Err(data_error(
                        "INVALID_CAST_ARGUMENT",
                        format!("Cannot cast '{}' to {}", strings.value(i), self.type_name()),
                    ));
                }
            }
        }
        let cleaned: ArrayRef = Arc::new(cleaned.finish());
        Ok(ColumnarValue::Array(cast(&cleaned, &self.target())?))
    }
}

/// `trino_round_for_cast(x)`: rounds floating-point and decimal values
/// half away from zero (Java's `HALF_UP`) and returns every other type
/// unchanged. See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoRoundForCast {
    signature: Signature,
}

impl Default for TrinoRoundForCast {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoRoundForCast {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

fn round_floats<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits_round::Round,
{
    array.unary(num_traits_round::Round::round_half_away)
}

/// Tiny local trait so the float rounding is generic over `f32`/`f64`
/// without pulling in `num-traits`.
mod num_traits_round {
    pub trait Round: Copy {
        fn round_half_away(self) -> Self;
    }
    impl Round for f64 {
        fn round_half_away(self) -> Self {
            // `f64::round` rounds half away from zero, which is Java's
            // HALF_UP.
            self.round()
        }
    }
    impl Round for f32 {
        fn round_half_away(self) -> Self {
            self.round()
        }
    }
}

/// Round a scaled decimal integer `value` (with `scale` fractional digits)
/// half away from zero to a whole number, keeping the scale.
pub fn round_decimal_half_up(value: i128, scale: i8) -> Option<i128> {
    if scale <= 0 {
        return Some(value);
    }
    let factor = 10i128.checked_pow(u32::from(scale as u8))?;
    let quotient = value / factor;
    let remainder = value % factor;
    let rounded = if remainder.unsigned_abs() * 2 >= factor.unsigned_abs() {
        quotient + value.signum()
    } else {
        quotient
    };
    rounded.checked_mul(factor)
}

impl ScalarUDFImpl for TrinoRoundForCast {
    fn name(&self) -> &str {
        "trino_round_for_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = match &args.args[0] {
            ColumnarValue::Array(a) => a.clone(),
            scalar @ ColumnarValue::Scalar(_) => scalar.to_array(rows)?,
        };
        let out: ArrayRef = match input.data_type() {
            DataType::Float64 => Arc::new(round_floats(input.as_primitive::<Float64Type>())),
            DataType::Float32 => Arc::new(round_floats(input.as_primitive::<Float32Type>())),
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) = (*precision, *scale);
                let decimals = input.as_primitive::<Decimal128Type>();
                let mut rounded = Vec::with_capacity(rows);
                for i in 0..rows {
                    if decimals.is_null(i) {
                        rounded.push(None);
                        continue;
                    }
                    match round_decimal_half_up(decimals.value(i), scale) {
                        Some(v) => rounded.push(Some(v)),
                        None => {
                            return Err(data_error(
                                "NUMERIC_VALUE_OUT_OF_RANGE",
                                format!(
                                    "cannot round {} to an integer: out of range",
                                    decimals.value_as_string(i)
                                ),
                            ));
                        }
                    }
                }
                Arc::new(
                    PrimitiveArray::<Decimal128Type>::from(rounded)
                        .with_precision_and_scale(precision, scale)?,
                )
            }
            _ => input,
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// `trino_varchar(x[, n])`: `CAST(x AS VARCHAR[(n)])` in Trino's text forms.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoVarchar {
    signature: Signature,
    try_cast: bool,
}

impl TrinoVarchar {
    /// New instance.
    pub fn new(try_cast: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            try_cast,
        }
    }
}

/// Trino's name for an Arrow type in `Cannot cast` diagnostics.
pub(crate) fn trino_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "bigint".into(),
        DataType::Float16 | DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "varbinary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, None) => "timestamp".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone".into(),
        DataType::Time32(_) | DataType::Time64(_) => "time".into(),
        DataType::Interval(_) | DataType::Duration(_) => "interval".into(),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f) => format!("array({})", trino_type_name(f.data_type())),
        DataType::Struct(_) => "row".into(),
        DataType::Map(_, _) => "map".into(),
        DataType::Dictionary(_, v) => trino_type_name(v),
        other => other.to_string(),
    }
}

/// Convert any castable array to Trino's `varchar` text.
pub(crate) fn to_varchar(input: &ArrayRef) -> Result<StringArray> {
    let rows = input.len();
    let text: StringArray = match input.data_type() {
        DataType::Utf8 => input.as_string::<i32>().clone(),
        DataType::LargeUtf8 | DataType::Utf8View => {
            cast(input, &DataType::Utf8)?.as_string::<i32>().clone()
        }
        DataType::Null => StringArray::new_null(rows),
        DataType::Float64 => {
            let floats = input.as_primitive::<Float64Type>();
            let mut out = StringBuilder::new();
            for i in 0..rows {
                if floats.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(java_double_text(floats.value(i)));
                }
            }
            out.finish()
        }
        DataType::Float32 => {
            let floats = input.as_primitive::<Float32Type>();
            let mut out = StringBuilder::new();
            for i in 0..rows {
                if floats.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(java_float_text(floats.value(i)));
                }
            }
            out.finish()
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64 => cast(input, &DataType::Utf8)?.as_string::<i32>().clone(),
        DataType::Timestamp(unit, tz) => {
            let ints = cast(input, &DataType::Int64)?;
            let ints = ints.as_primitive::<Int64Type>();
            let mut out = StringBuilder::new();
            for v in ints.iter() {
                out.append_option(v.map(|v| timestamp_text(v, *unit, tz.as_deref())));
            }
            out.finish()
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            let options = format_options();
            let formatter = ArrayFormatter::try_new(input.as_ref(), &options)?;
            let mut out = StringBuilder::new();
            for i in 0..rows {
                if input.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(formatter.value(i).try_to_string()?);
                }
            }
            out.finish()
        }
        other => {
            return Err(data_error(
                "INVALID_CAST_ARGUMENT",
                format!("Cannot cast {} to varchar", trino_type_name(other)),
            ));
        }
    };
    Ok(text)
}

impl ScalarUDFImpl for TrinoVarchar {
    fn name(&self) -> &str {
        if self.try_cast {
            "trino_try_varchar"
        } else {
            "trino_varchar"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(
            arg_types[0],
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        ) {
            return plan_err!("Cannot cast {} to varchar", trino_type_name(&arg_types[0]));
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let text = to_varchar(&input)?;
        let Some(limit) = args.args.get(1) else {
            return Ok(ColumnarValue::Array(Arc::new(text)));
        };
        let limits: PrimitiveArray<Int64Type> = int64_array("CAST", "varchar length", limit, rows)?;
        let source_is_varchar = matches!(
            input.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        );
        let mut out = StringBuilder::new();
        for i in 0..rows {
            if text.is_null(i) || limits.is_null(i) {
                out.append_null();
                continue;
            }
            let n = limits.value(i);
            if n < 0 {
                return user_err!("CAST", "varchar length must not be negative, got {n}");
            }
            let value = text.value(i);
            if !source_is_varchar && value.chars().count() > n as usize {
                // Trino truncates varchar → varchar(n) but refuses to
                // shorten the text of any other type.
                if self.try_cast {
                    out.append_null();
                    continue;
                }
                return Err(data_error(
                    "INVALID_CAST_ARGUMENT",
                    format!("Value {value} cannot be represented as varchar({n})"),
                ));
            }
            out.append_value(value.chars().take(n as usize).collect::<String>());
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `trino_boolean(x)` / `trino_try_boolean(x)`: `CAST(x AS BOOLEAN)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoBoolean {
    signature: Signature,
    try_cast: bool,
}

impl TrinoBoolean {
    /// New instance.
    pub fn new(try_cast: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            try_cast,
        }
    }
}

/// Trino's varchar → boolean rule (no trimming, case-insensitive).
pub(crate) fn parse_trino_boolean(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

impl ScalarUDFImpl for TrinoBoolean {
    fn name(&self) -> &str {
        if self.try_cast {
            "trino_try_boolean"
        } else {
            "trino_boolean"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
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
            | DataType::Utf8View => Ok(DataType::Boolean),
            other => Err(type_mismatch(format!(
                "Cannot cast {} to boolean",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let out: ArrayRef = match input.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let strings = string_array("CAST", &args.args[0], rows)?;
                let mut out = BooleanBuilder::with_capacity(rows);
                for i in 0..rows {
                    if strings.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    match parse_trino_boolean(strings.value(i)) {
                        Some(b) => out.append_value(b),
                        None if self.try_cast => out.append_null(),
                        None => {
                            return Err(data_error(
                                "INVALID_CAST_ARGUMENT",
                                format!("Cannot cast '{}' to BOOLEAN", strings.value(i)),
                            ));
                        }
                    }
                }
                Arc::new(out.finish())
            }
            // Trino: a decimal is true when non-zero.
            DataType::Decimal128(_, _) => {
                let decimals = input.as_primitive::<Decimal128Type>();
                let mut out = BooleanBuilder::with_capacity(rows);
                for v in decimals.iter() {
                    out.append_option(v.map(|v| v != 0));
                }
                Arc::new(out.finish())
            }
            // Arrow's numeric → boolean cast is `!= 0`, as Trino's.
            _ => cast(&input, &DataType::Boolean)?,
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, TimestampMillisecondArray};

    use super::*;

    #[test]
    fn decimal_rounding_is_half_away_from_zero() {
        // 2.5 at scale 1 → 3.0; -2.5 → -3.0; 2.4 → 2.0; 120.5 at scale 1 → 121.0
        assert_eq!(round_decimal_half_up(25, 1), Some(30));
        assert_eq!(round_decimal_half_up(-25, 1), Some(-30));
        assert_eq!(round_decimal_half_up(24, 1), Some(20));
        assert_eq!(round_decimal_half_up(1205, 1), Some(1210));
        assert_eq!(round_decimal_half_up(1234, 0), Some(1234));
        assert_eq!(round_decimal_half_up(-149, 2), Some(-100));
        assert_eq!(round_decimal_half_up(-150, 2), Some(-200));
    }

    #[test]
    fn floats_round_half_away_from_zero() {
        let rounded = round_floats(&Float64Array::from(vec![2.5, -2.5, 120.5, 0.49, -0.5]));
        assert_eq!(rounded.values().as_ref(), &[3.0, -3.0, 121.0, 0.0, -1.0]);
    }

    #[test]
    fn boolean_text_follows_trino() {
        for (text, expected) in [
            ("true", Some(true)),
            ("TRUE", Some(true)),
            ("t", Some(true)),
            ("1", Some(true)),
            ("false", Some(false)),
            ("F", Some(false)),
            ("0", Some(false)),
            ("yes", None),
            ("on", None),
            (" true", None),
            ("", None),
        ] {
            assert_eq!(parse_trino_boolean(text), expected, "{text}");
        }
    }

    #[test]
    fn double_text_follows_java() {
        for (text, expected) in [
            ("1.5", Some("1.5")),
            (" 1.5 ", Some("1.5")),
            ("\t-2e3\n", Some("-2e3")),
            ("+.5", Some("+.5")),
            ("1.", Some("1.")),
            ("1.e5", Some("1.e5")),
            ("1.5d", Some("1.5")),
            ("2F", Some("2")),
            ("NaN", Some("NaN")),
            ("Infinity", Some("inf")),
            ("+Infinity", Some("inf")),
            ("-Infinity", Some("-inf")),
            ("nan", None),
            ("inf", None),
            ("infinity", None),
            ("INFINITY", None),
            ("-inf", None),
            ("0x1p3", None),
            ("1e", None),
            ("e5", None),
            (".", None),
            ("", None),
            ("1_000", None),
            ("1,5", None),
        ] {
            assert_eq!(
                parse_java_double_text(text).as_deref(),
                expected,
                "{text:?}"
            );
        }
    }

    #[test]
    fn varchar_text_uses_athena_forms() {
        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(1_704_450_600_000),
            None,
        ]));
        let text = to_varchar(&ts).unwrap();
        assert_eq!(text.value(0), "2024-01-05 10:30:00.000");
        assert!(text.is_null(1));

        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 1e20, 2.0]));
        let text = to_varchar(&doubles).unwrap();
        assert_eq!(
            (0..3).map(|i| text.value(i)).collect::<Vec<_>>(),
            ["1.5", "1.0E20", "2.0"]
        );
    }
}
