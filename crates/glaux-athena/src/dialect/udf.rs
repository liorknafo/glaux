//! Rust implementations of Trino functions DataFusion has no equivalent
//! for: calendar-aware `date_add` / `date_diff` and the JSON accessors.
//!
//! These are registered on the session by [`super::TrinoEngine`] under their
//! Trino names. Every one of them fails loudly on inputs it cannot handle
//! (unknown units, non-literal units, unsupported JSONPath features,
//! invalid JSON) instead of returning a best-effort value.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Builder, PrimitiveArray, StringArray, StringBuilder,
    TimestampNanosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit, TimestampNanosecondType};
use chrono::{DateTime, Datelike, Months, NaiveDateTime, TimeDelta, Timelike};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};

use super::error::GlauxSqlError;

/// A runtime failure caused by the query's arguments (bad unit, invalid
/// JSON, unsupported JSONPath). Carried as a [`GlauxSqlError`] so the engine
/// reports it as a user error naming the function, not as an internal
/// failure.
fn user_error(function: &str, message: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::invalid_arguments(
        function, message,
    )))
}

macro_rules! user_err {
    ($function:expr, $($arg:tt)*) => {
        Err(user_error($function, format!($($arg)*)))
    };
}
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// All UDFs the Trino layer registers.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(DateAdd::new()),
        ScalarUDF::new_from_impl(DateDiff::new()),
        ScalarUDF::new_from_impl(JsonExtractScalar::new()),
        ScalarUDF::new_from_impl(JsonExtract::new()),
        ScalarUDF::new_from_impl(JsonParse::new()),
        ScalarUDF::new_from_impl(JsonFormat::new()),
        ScalarUDF::new_from_impl(JsonArrayLength::new()),
        ScalarUDF::new_from_impl(JsonSize::new()),
    ]
}

// ---------------------------------------------------------------------------
// Date/time units
// ---------------------------------------------------------------------------

/// The units Trino's `date_add` / `date_diff` accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    fn parse(function: &str, unit: &str) -> Result<Self> {
        Ok(match unit.to_ascii_lowercase().as_str() {
            "millisecond" => Self::Millisecond,
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            other => {
                return user_err!(
                    function,
                    "unit {other:?} is not supported (expected one of millisecond, \
                     second, minute, hour, day, week, month, quarter, year)"
                );
            }
        })
    }

    /// Whether the unit is whole days or coarser (valid on `DATE` values).
    fn is_calendar(self) -> bool {
        matches!(
            self,
            Self::Day | Self::Week | Self::Month | Self::Quarter | Self::Year
        )
    }

    /// Length in nanoseconds for fixed-length units.
    fn fixed_nanos(self) -> Option<i64> {
        Some(match self {
            Self::Millisecond => 1_000_000,
            Self::Second => 1_000_000_000,
            Self::Minute => 60_000_000_000,
            Self::Hour => 3_600_000_000_000,
            Self::Day => 86_400_000_000_000,
            Self::Week => 7 * 86_400_000_000_000,
            Self::Month | Self::Quarter | Self::Year => return None,
        })
    }
}

/// The literal unit string from the first argument.
fn unit_arg(function: &str, arg: &ColumnarValue) -> Result<Unit> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Unit::parse(function, s),
        ColumnarValue::Scalar(other) if other.is_null() => {
            user_err!(function, "unit must be a string literal, got NULL")
        }
        other => user_err!(
            function,
            "unit must be a string literal, got {}",
            other.data_type()
        ),
    }
}

/// Cast a `DATE`/`TIMESTAMP` column to nanosecond timestamps (keeping its
/// timezone) so the arithmetic below has one representation to handle.
fn to_nanos(
    function: &str,
    array: &ArrayRef,
) -> Result<(PrimitiveArray<TimestampNanosecondType>, Option<Arc<str>>)> {
    let tz = match array.data_type() {
        DataType::Date32 | DataType::Date64 => None,
        DataType::Timestamp(_, tz) => tz.clone(),
        other => {
            return user_err!(
                function,
                "expected a DATE or TIMESTAMP argument, got {other}"
            );
        }
    };
    let casted = cast(
        array,
        &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
    )?;
    Ok((casted.as_primitive::<TimestampNanosecondType>().clone(), tz))
}

fn naive(nanos: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_nanos(nanos).naive_utc().into()
}

fn shift(ts: NaiveDateTime, unit: Unit, n: i64) -> Option<NaiveDateTime> {
    match unit {
        Unit::Month | Unit::Quarter | Unit::Year => {
            let months = match unit {
                Unit::Month => n,
                Unit::Quarter => n.checked_mul(3)?,
                _ => n.checked_mul(12)?,
            };
            let months_u32 = u32::try_from(months.unsigned_abs()).ok()?;
            if months >= 0 {
                ts.checked_add_months(Months::new(months_u32))
            } else {
                ts.checked_sub_months(Months::new(months_u32))
            }
        }
        fixed => {
            let nanos = fixed.fixed_nanos()?.checked_mul(n)?;
            ts.checked_add_signed(TimeDelta::nanoseconds(nanos))
        }
    }
}

/// Joda-style whole months between two instants (sign-aware, truncating).
fn months_between(a: NaiveDateTime, b: NaiveDateTime) -> i64 {
    let mut months = (i64::from(b.year()) - i64::from(a.year())) * 12
        + (i64::from(b.month()) - i64::from(a.month()));
    let a_rest = (a.day(), a.num_seconds_from_midnight(), a.nanosecond());
    let b_rest = (b.day(), b.num_seconds_from_midnight(), b.nanosecond());
    if months > 0 && b_rest < a_rest {
        months -= 1;
    } else if months < 0 && b_rest > a_rest {
        months += 1;
    }
    months
}

// ---------------------------------------------------------------------------
// date_add(unit, value, timestamp)
// ---------------------------------------------------------------------------

/// Trino `date_add(unit, value, timestamp)`: adds `value` units, with
/// calendar semantics for month/quarter/year (end-of-month clamping) and
/// fixed lengths for the rest. Returns the input's type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateAdd {
    signature: Signature,
}

impl Default for DateAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl DateAdd {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateAdd {
    fn name(&self) -> &str {
        "date_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[2] {
            t @ (DataType::Date32 | DataType::Timestamp(_, _)) => Ok(t.clone()),
            other => plan_err!("date_add: third argument must be a DATE or TIMESTAMP, got {other}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit = unit_arg("date_add", &args.args[0])?;
        let rows = args.number_rows;
        let values = cast(&args.args[1].to_array(rows)?, &DataType::Int64)?;
        let values = values.as_primitive::<arrow::datatypes::Int64Type>();
        let input = args.args[2].to_array(rows)?;
        let input_type = input.data_type().clone();
        if matches!(input_type, DataType::Date32) && !unit.is_calendar() {
            return user_err!(
                "date_add",
                "unit {unit:?} cannot be added to a DATE; cast to TIMESTAMP first"
            );
        }
        let (nanos, tz) = to_nanos("date_add", &input)?;
        let mut out = Vec::with_capacity(rows);
        for i in 0..rows {
            if nanos.is_null(i) || values.is_null(i) {
                out.push(None);
                continue;
            }
            let shifted = naive(nanos.value(i))
                .and_then(|ts| shift(ts, unit, values.value(i)))
                .and_then(|ts| ts.and_utc().timestamp_nanos_opt());
            match shifted {
                Some(v) => out.push(Some(v)),
                None => {
                    return user_err!(
                        "date_add",
                        "{} {unit:?} from {} overflows the timestamp range",
                        values.value(i),
                        naive(nanos.value(i))
                            .map(|t| t.to_string())
                            .unwrap_or_default()
                    );
                }
            }
        }
        let result = TimestampNanosecondArray::from(out).with_timezone_opt(tz);
        let result = cast(&(Arc::new(result) as ArrayRef), &input_type)?;
        Ok(ColumnarValue::Array(result))
    }
}

// ---------------------------------------------------------------------------
// date_diff(unit, timestamp1, timestamp2)
// ---------------------------------------------------------------------------

/// Trino `date_diff(unit, a, b)`: `b - a` expressed in whole units
/// (truncated toward zero; calendar months for month/quarter/year).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateDiff {
    signature: Signature,
}

impl Default for DateDiff {
    fn default() -> Self {
        Self::new()
    }
}

impl DateDiff {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateDiff {
    fn name(&self) -> &str {
        "date_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        for (i, t) in arg_types[1..].iter().enumerate() {
            if !matches!(t, DataType::Date32 | DataType::Timestamp(_, _)) {
                return plan_err!(
                    "date_diff: argument {} must be a DATE or TIMESTAMP, got {t}",
                    i + 2
                );
            }
        }
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit = unit_arg("date_diff", &args.args[0])?;
        let rows = args.number_rows;
        let (a, _) = to_nanos("date_diff", &args.args[1].to_array(rows)?)?;
        let (b, _) = to_nanos("date_diff", &args.args[2].to_array(rows)?)?;
        let mut out = Int64Builder::with_capacity(rows);
        for i in 0..rows {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            let (x, y) = (a.value(i), b.value(i));
            let diff = match unit.fixed_nanos() {
                Some(len) => (y - x) / len,
                None => {
                    let (Some(x), Some(y)) = (naive(x), naive(y)) else {
                        return user_err!("date_diff", "timestamp out of range");
                    };
                    let months = months_between(x, y);
                    match unit {
                        Unit::Month => months,
                        Unit::Quarter => months / 3,
                        _ => months / 12,
                    }
                }
            };
            out.append_value(diff);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

/// One step of the JSONPath subset glaux implements.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Key(String),
    Index(usize),
}

/// Parse the JSONPath subset Trino users actually write: `$`, `.key`,
/// `["key"]` / `['key']`, `[n]`. Wildcards, recursive descent, slices,
/// filters, and functions are refused by name.
fn parse_json_path(function: &str, path: &str) -> Result<Vec<Step>> {
    let refuse = |what: &str| {
        user_err!(
            function,
            "JSONPath {path:?} uses {what}, which glaux does not support \
             (supported: $, .key, [\"key\"], [n])"
        )
    };
    let Some(rest) = path.strip_prefix('$') else {
        return user_err!(function, "JSONPath {path:?} must start with '$'");
    };
    let chars: Vec<char> = rest.chars().collect();
    let mut steps = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                if i < chars.len() && chars[i] == '.' {
                    return refuse("recursive descent (..)");
                }
                if i < chars.len() && chars[i] == '*' {
                    return refuse("a wildcard (*)");
                }
                let start = i;
                while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                    i += 1;
                }
                if start == i {
                    return user_err!(function, "JSONPath {path:?} has an empty key after '.'");
                }
                steps.push(Step::Key(chars[start..i].iter().collect()));
            }
            '[' => {
                i += 1;
                if i >= chars.len() {
                    return user_err!(function, "JSONPath {path:?} has an unterminated '['");
                }
                match chars[i] {
                    q @ ('"' | '\'') => {
                        i += 1;
                        let start = i;
                        while i < chars.len() && chars[i] != q {
                            i += 1;
                        }
                        if i >= chars.len() || chars.get(i + 1) != Some(&']') {
                            return user_err!(
                                function,
                                "JSONPath {path:?} has an unterminated quoted key"
                            );
                        }
                        steps.push(Step::Key(chars[start..i].iter().collect()));
                        i += 2;
                    }
                    '*' => return refuse("a wildcard ([*])"),
                    '?' => return refuse("a filter expression ([?(...)])"),
                    _ => {
                        let start = i;
                        while i < chars.len() && chars[i] != ']' {
                            i += 1;
                        }
                        if i >= chars.len() {
                            return user_err!(
                                function,
                                "JSONPath {path:?} has an unterminated '['"
                            );
                        }
                        let text: String = chars[start..i].iter().collect();
                        if text.contains(':') || text.contains(',') {
                            return refuse("an array slice or union");
                        }
                        let Ok(index) = text.trim().parse::<usize>() else {
                            return user_err!(
                                function,
                                "JSONPath {path:?} has a non-numeric array index {text:?}"
                            );
                        };
                        steps.push(Step::Index(index));
                        i += 1;
                    }
                }
            }
            other => {
                return user_err!(
                    function,
                    "JSONPath {path:?} has unexpected character {other:?}"
                );
            }
        }
    }
    Ok(steps)
}

fn navigate<'a>(value: &'a serde_json::Value, steps: &[Step]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for step in steps {
        current = match step {
            Step::Key(k) => current.as_object()?.get(k)?,
            Step::Index(i) => current.as_array()?.get(*i)?,
        };
    }
    Some(current)
}

fn parse_json(function: &str, text: &str) -> Result<serde_json::Value> {
    serde_json::from_str(text).map_err(|e| user_error(function, format!("invalid JSON input: {e}")))
}

/// Coerce any string-typed column to `Utf8`.
fn string_array(function: &str, value: &ColumnarValue, rows: usize) -> Result<StringArray> {
    let array = value.to_array(rows)?;
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(cast(&array, &DataType::Utf8)?.as_string::<i32>().clone())
        }
        other => user_err!(function, "expected a VARCHAR argument, got {other}"),
    }
}

/// Evaluate a `(json, path) -> T` function row by row. `f` receives the
/// navigated value (`None` when the path does not exist).
fn json_path_map<T>(
    function: &str,
    args: &ScalarFunctionArgs,
    mut f: impl FnMut(Option<&serde_json::Value>) -> Option<T>,
    mut append: impl FnMut(Option<T>),
) -> Result<()> {
    let rows = args.number_rows;
    let json = string_array(function, &args.args[0], rows)?;
    let paths = string_array(function, &args.args[1], rows)?;
    let mut cached: Option<(String, Vec<Step>)> = None;
    for i in 0..rows {
        if json.is_null(i) || paths.is_null(i) {
            append(None);
            continue;
        }
        let path = paths.value(i);
        if cached.as_ref().is_none_or(|(p, _)| p != path) {
            cached = Some((path.to_string(), parse_json_path(function, path)?));
        }
        let steps = &cached.as_ref().expect("cached path").1;
        let value = parse_json(function, json.value(i))?;
        append(f(navigate(&value, steps)));
    }
    Ok(())
}

fn json_signature(arg_count: usize) -> Signature {
    Signature::string(arg_count, Volatility::Immutable)
}

/// Trino `json_extract_scalar(json, path)`: the scalar at `path` as text
/// (`NULL` for missing paths, JSON nulls, objects, and arrays).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtractScalar {
    signature: Signature,
}

impl Default for JsonExtractScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExtractScalar {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(2),
        }
    }
}

impl ScalarUDFImpl for JsonExtractScalar {
    fn name(&self) -> &str {
        "json_extract_scalar"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = StringBuilder::new();
        json_path_map(
            "json_extract_scalar",
            &args,
            |v| match v? {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                serde_json::Value::Bool(b) => Some(b.to_string()),
                serde_json::Value::Null
                | serde_json::Value::Object(_)
                | serde_json::Value::Array(_) => None,
            },
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino `json_extract(json, path)`: the JSON value at `path`, serialised
/// (glaux represents Trino's `JSON` type as its text).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtract {
    signature: Signature,
}

impl Default for JsonExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExtract {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(2),
        }
    }
}

impl ScalarUDFImpl for JsonExtract {
    fn name(&self) -> &str {
        "json_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = StringBuilder::new();
        json_path_map(
            "json_extract",
            &args,
            |v| v.map(|v| v.to_string()),
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino `json_size(json, path)`: number of members of the object/array
/// at `path`, `0` for scalars, `NULL` when the path is missing.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonSize {
    signature: Signature,
}

impl Default for JsonSize {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonSize {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(2),
        }
    }
}

impl ScalarUDFImpl for JsonSize {
    fn name(&self) -> &str {
        "json_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = Int64Builder::new();
        json_path_map(
            "json_size",
            &args,
            |v| match v? {
                serde_json::Value::Object(o) => Some(o.len() as i64),
                serde_json::Value::Array(a) => Some(a.len() as i64),
                _ => Some(0),
            },
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Evaluate a `(json) -> T` function row by row over validated JSON.
fn json_map<T>(
    function: &str,
    args: &ScalarFunctionArgs,
    mut f: impl FnMut(serde_json::Value) -> Option<T>,
    mut append: impl FnMut(Option<T>),
) -> Result<()> {
    let rows = args.number_rows;
    let json = string_array(function, &args.args[0], rows)?;
    for i in 0..rows {
        if json.is_null(i) {
            append(None);
            continue;
        }
        append(f(parse_json(function, json.value(i))?));
    }
    Ok(())
}

/// Trino `json_parse(varchar)`: validates the text as JSON. glaux keeps the
/// JSON type as text, so the output is the input re-serialised compactly;
/// invalid JSON is an error, as in Trino.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonParse {
    signature: Signature,
}

impl Default for JsonParse {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonParse {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(1),
        }
    }
}

impl ScalarUDFImpl for JsonParse {
    fn name(&self) -> &str {
        "json_parse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = StringBuilder::new();
        json_map(
            "json_parse",
            &args,
            |v| Some(v.to_string()),
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino `json_format(json)`: the JSON value as text.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonFormat {
    signature: Signature,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonFormat {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(1),
        }
    }
}

impl ScalarUDFImpl for JsonFormat {
    fn name(&self) -> &str {
        "json_format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = StringBuilder::new();
        json_map(
            "json_format",
            &args,
            |v| Some(v.to_string()),
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino `json_array_length(json)`: element count, `NULL` if the value is
/// not an array.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonArrayLength {
    signature: Signature,
}

impl Default for JsonArrayLength {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonArrayLength {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: json_signature(1),
        }
    }
}

impl ScalarUDFImpl for JsonArrayLength {
    fn name(&self) -> &str {
        "json_array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut out = Int64Builder::new();
        json_map(
            "json_array_length",
            &args,
            |v| v.as_array().map(|a| a.len() as i64),
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn month_arithmetic_clamps_to_month_end_like_trino() {
        assert_eq!(
            shift(dt("2024-01-31 10:00:00"), Unit::Month, 1).unwrap(),
            dt("2024-02-29 10:00:00")
        );
        assert_eq!(
            shift(dt("2024-03-31 00:00:00"), Unit::Month, -1).unwrap(),
            dt("2024-02-29 00:00:00")
        );
        assert_eq!(
            shift(dt("2024-02-29 00:00:00"), Unit::Year, 1).unwrap(),
            dt("2025-02-28 00:00:00")
        );
        assert_eq!(
            shift(dt("2024-01-01 00:00:00"), Unit::Week, 2).unwrap(),
            dt("2024-01-15 00:00:00")
        );
    }

    #[test]
    fn months_between_truncates_partial_months() {
        assert_eq!(
            months_between(dt("2024-01-31 00:00:00"), dt("2024-02-29 00:00:00")),
            0
        );
        assert_eq!(
            months_between(dt("2024-01-31 00:00:00"), dt("2024-03-01 00:00:00")),
            1
        );
        assert_eq!(
            months_between(dt("2024-01-15 00:00:00"), dt("2024-03-15 00:00:00")),
            2
        );
        assert_eq!(
            months_between(dt("2024-03-15 00:00:00"), dt("2024-01-16 00:00:00")),
            -1
        );
        assert_eq!(
            months_between(dt("2024-03-15 00:00:00"), dt("2024-01-15 00:00:00")),
            -2
        );
    }

    #[test]
    fn json_path_subset_parses_and_refuses_the_rest() {
        assert_eq!(
            parse_json_path("json_extract", "$.a.b[2][\"c d\"]['e']").unwrap(),
            vec![
                Step::Key("a".into()),
                Step::Key("b".into()),
                Step::Index(2),
                Step::Key("c d".into()),
                Step::Key("e".into()),
            ]
        );
        assert!(parse_json_path("json_extract", "$").unwrap().is_empty());
        for (path, what) in [
            ("$..a", "recursive descent"),
            ("$.*", "wildcard"),
            ("$[*]", "wildcard"),
            ("$[0:2]", "slice"),
            ("$[?(@.a)]", "filter"),
            ("a.b", "must start with '$'"),
        ] {
            let err = parse_json_path("json_extract", path)
                .unwrap_err()
                .to_string();
            assert!(err.contains(what), "{path}: {err}");
        }
    }
}
