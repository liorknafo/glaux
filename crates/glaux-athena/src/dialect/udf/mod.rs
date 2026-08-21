//! Rust implementations of Trino functions DataFusion has no faithful
//! equivalent for.
//!
//! This file holds calendar-aware `date_add` / `date_diff` and the JSON
//! accessors; the submodules hold the rest:
//!
//! - [`strings`]: `substr` (Trino's start ≤ 0 rules), `split_part` (NULL
//!   out of range), Java-syntax `regexp_replace` replacement strings;
//! - [`casts`]: `CAST` helpers — HALF_UP rounding for double → integer and
//!   Trino's text forms for `CAST(... AS VARCHAR)`;
//! - [`arrays`]: `element_at` and `arr[i]` with Trino's index rules,
//!   `reverse` for arrays, `contains` / `arrays_overlap` with NULL results;
//! - [`datetime`]: `date_trunc` keeping a `DATE` input's type and
//!   `to_unixtime` refusing varchar input;
//! - [`iso8601`]: strict `from_iso8601_timestamp` / `from_iso8601_date`;
//! - [`arithmetic`]: overflow-checked bigint `+ - *` and `sum`.
//!
//! The Trino-named ones (`date_add`, `json_extract`, ...) are registered
//! under their Trino names; the helpers the rewriter inserts carry a
//! `trino_` prefix and are not callable from user SQL because the registry
//! refuses unknown names. Every one of them fails loudly on inputs it cannot
//! handle instead of returning a best-effort value.

pub mod arithmetic;
pub mod arrays;
pub mod casts;
pub mod datetime;
pub mod decimal;
pub mod floats;
pub mod iso8601;
pub mod json;
pub mod math;
pub mod nullable;
pub mod regex;
pub mod strings;
pub mod subquery;
pub mod tdigest;
pub mod timestamps;

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Builder, PrimitiveArray, StringArray, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
use datafusion::common::{DataFusionError, Result, ScalarValue};

use super::error::GlauxSqlError;
use casts::trino_type_name;
use timestamps::{MILLIS_PER_DAY, epoch_date};

/// A runtime failure caused by the query's arguments (bad unit, invalid
/// JSON, unsupported JSONPath). Carried as a [`GlauxSqlError`] so the engine
/// reports it as a user error naming the function, not as an internal
/// failure.
pub(crate) fn user_error(function: &str, message: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::invalid_arguments(
        function, message,
    )))
}

/// A planning failure because the argument types are ones Trino refuses
/// (`year('2024-01-05')`, `contains(ARRAY[1], 'a')`). Carried as a
/// [`GlauxSqlError::TypeMismatch`] so the client sees `TYPE_MISMATCH`, as
/// on Athena.
pub(crate) fn type_mismatch(message: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::type_mismatch(message)))
}

/// A runtime failure caused by the data (overflow, a bad subscript). Carried
/// as a [`GlauxSqlError::Runtime`] with Trino's error code.
pub(crate) fn data_error(code: &str, message: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::runtime(code, message)))
}

/// A construct glaux refuses by name, raised from planning-time code that
/// must return a [`DataFusionError`]. Carried as a
/// [`GlauxSqlError::Unsupported`] so the client sees `NOT_SUPPORTED` naming
/// the construct.
pub(crate) fn unsupported_error(
    construct: impl Into<String>,
    message: impl Into<String>,
) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::unsupported(construct, message)))
}

macro_rules! user_err {
    ($function:expr, $($arg:tt)*) => {
        Err($crate::dialect::udf::user_error($function, format!($($arg)*)))
    };
}
pub(crate) use user_err;

use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// All scalar UDFs the Trino layer registers.
pub fn all() -> Vec<ScalarUDF> {
    let mut udfs = vec![
        ScalarUDF::new_from_impl(DateAdd::new()),
        ScalarUDF::new_from_impl(DateDiff::new()),
        ScalarUDF::new_from_impl(JsonExtractScalar::new()),
        ScalarUDF::new_from_impl(JsonExtract::new()),
        ScalarUDF::new_from_impl(JsonParse::new()),
        ScalarUDF::new_from_impl(JsonFormat::new()),
        ScalarUDF::new_from_impl(JsonArrayLength::new()),
        ScalarUDF::new_from_impl(JsonSize::new()),
    ];
    udfs.extend(strings::all());
    udfs.extend(casts::all());
    udfs.extend(arrays::all());
    udfs.extend(datetime::all());
    udfs.extend(iso8601::all());
    udfs.extend(timestamps::all());
    udfs.extend(decimal::scalar_udfs());
    udfs.extend(floats::all());
    udfs.extend(math::all());
    udfs.extend(nullable::all());
    udfs.extend(regex::all());
    udfs.extend(arithmetic::scalar_udfs());
    udfs.extend(subquery::all());
    udfs
}

/// All aggregate UDFs the Trino layer registers.
pub fn all_aggregates() -> Vec<AggregateUDF> {
    let mut udafs = arithmetic::aggregate_udfs();
    udafs.extend(decimal::aggregate_udfs());
    udafs.extend(floats::aggregate_udfs());
    udafs.extend(subquery::aggregate_udfs());
    udafs.extend(tdigest::aggregate_udfs());
    udafs
}

/// `true` for Arrow's signed integer types (Trino's tinyint … bigint).
pub(crate) fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// Coerce any string-typed column to `Utf8`.
pub(crate) fn string_array(
    function: &str,
    value: &ColumnarValue,
    rows: usize,
) -> Result<StringArray> {
    let array = value.to_array(rows)?;
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(cast(&array, &DataType::Utf8)?.as_string::<i32>().clone())
        }
        DataType::Null => Ok(StringArray::new_null(rows)),
        other => user_err!(function, "expected a VARCHAR argument, got {other}"),
    }
}

/// Coerce an integer-typed argument to `Int64`; refuses fractional types
/// because Trino does (`substr('abc', 1.5)` is a type error there).
pub(crate) fn int64_array(
    function: &str,
    what: &str,
    value: &ColumnarValue,
    rows: usize,
) -> Result<PrimitiveArray<arrow::datatypes::Int64Type>> {
    let array = value.to_array(rows)?;
    if !is_integer(array.data_type()) && !matches!(array.data_type(), DataType::Null) {
        return user_err!(
            function,
            "{what} must be an integer (got {}); Trino does not coerce fractional values here",
            array.data_type()
        );
    }
    Ok(cast(&array, &DataType::Int64)?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .clone())
}

// ---------------------------------------------------------------------------
// Date/time units
// ---------------------------------------------------------------------------

/// The units Trino's `date_add` / `date_diff` / `date_trunc` accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// `millisecond`
    Millisecond,
    /// `second`
    Second,
    /// `minute`
    Minute,
    /// `hour`
    Hour,
    /// `day`
    Day,
    /// `week`
    Week,
    /// `month`
    Month,
    /// `quarter`
    Quarter,
    /// `year`
    Year,
}

impl Unit {
    pub(crate) fn parse(function: &str, unit: &str) -> Result<Self> {
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
    pub(crate) fn is_calendar(self) -> bool {
        matches!(
            self,
            Self::Day | Self::Week | Self::Month | Self::Quarter | Self::Year
        )
    }

    /// Length in milliseconds for fixed-length units.
    pub(crate) fn fixed_millis(self) -> Option<i64> {
        Some(match self {
            Self::Millisecond => 1,
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => MILLIS_PER_DAY,
            Self::Week => 7 * MILLIS_PER_DAY,
            Self::Month | Self::Quarter | Self::Year => return None,
        })
    }
}

/// The literal unit string from the first argument.
pub(crate) fn unit_arg(function: &str, arg: &ColumnarValue) -> Result<Unit> {
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

/// Decode a `DATE` / `TIMESTAMP` column to naive (UTC wall-clock) date-times,
/// one per row, so the calendar arithmetic below has one representation to
/// handle and no Arrow-unit range limit (`DATE '9999-12-31'` is an ordinary
/// value here, where a nanosecond timestamp would overflow).
pub(crate) fn to_naive(function: &str, array: &ArrayRef) -> Result<Vec<Option<NaiveDateTime>>> {
    let out_of_range = |what: String| user_error(function, format!("{what} is out of range"));
    Ok(match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<arrow::datatypes::Date32Type>()
            .iter()
            .map(|d| {
                d.map(|d| {
                    epoch_date()
                        .checked_add_signed(TimeDelta::days(i64::from(d)))
                        .map(|d| d.and_time(NaiveTime::MIN))
                        .ok_or_else(|| out_of_range(format!("date (day {d})")))
                })
                .transpose()
            })
            .collect::<Result<_>>()?,
        DataType::Date64 => array
            .as_primitive::<arrow::datatypes::Date64Type>()
            .iter()
            .map(|ms| {
                ms.map(|ms| {
                    DateTime::from_timestamp_millis(ms)
                        .map(|t| t.naive_utc())
                        .ok_or_else(|| out_of_range(format!("date ({ms} ms)")))
                })
                .transpose()
            })
            .collect::<Result<_>>()?,
        DataType::Timestamp(unit, _) => {
            let values = cast(array, &DataType::Int64)?;
            let values = values.as_primitive::<arrow::datatypes::Int64Type>();
            let unit = *unit;
            values
                .iter()
                .map(|v| {
                    v.map(|v| {
                        let (secs, nanos) = match unit {
                            TimeUnit::Second => (v, 0),
                            TimeUnit::Millisecond => (
                                v.div_euclid(1_000),
                                (v.rem_euclid(1_000) * 1_000_000) as u32,
                            ),
                            TimeUnit::Microsecond => (
                                v.div_euclid(1_000_000),
                                (v.rem_euclid(1_000_000) * 1_000) as u32,
                            ),
                            TimeUnit::Nanosecond => (
                                v.div_euclid(1_000_000_000),
                                v.rem_euclid(1_000_000_000) as u32,
                            ),
                        };
                        DateTime::from_timestamp(secs, nanos)
                            .map(|t| t.naive_utc())
                            .ok_or_else(|| out_of_range(format!("timestamp ({v} {unit:?})")))
                    })
                    .transpose()
                })
                .collect::<Result<_>>()?
        }
        other => {
            return user_err!(
                function,
                "expected a DATE or TIMESTAMP argument, got {other}"
            );
        }
    })
}

/// Encode naive date-times back into `data_type` (the input's type).
pub(crate) fn from_naive(
    function: &str,
    values: Vec<Option<NaiveDateTime>>,
    data_type: &DataType,
) -> Result<ArrayRef> {
    let out_of_range = |t: &NaiveDateTime| {
        user_error(
            function,
            format!("{t} is out of range for {}", trino_type_name(data_type)),
        )
    };
    Ok(match data_type {
        DataType::Date32 => {
            let days = values
                .into_iter()
                .map(|v| {
                    v.map(|t| {
                        i32::try_from((t.date() - epoch_date()).num_days())
                            .map_err(|_| out_of_range(&t))
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(arrow::array::Date32Array::from(days))
        }
        DataType::Timestamp(unit, tz) => {
            let unit = *unit;
            let ints = values
                .into_iter()
                .map(|v| {
                    v.map(|t| {
                        let utc = t.and_utc();
                        match unit {
                            TimeUnit::Second => Some(utc.timestamp()),
                            TimeUnit::Millisecond => Some(utc.timestamp_millis()),
                            TimeUnit::Microsecond => Some(utc.timestamp_micros()),
                            TimeUnit::Nanosecond => utc.timestamp_nanos_opt(),
                        }
                        .ok_or_else(|| out_of_range(&t))
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let array = PrimitiveArray::<arrow::datatypes::Int64Type>::from(ints);
            cast(
                &(Arc::new(array) as ArrayRef),
                &DataType::Timestamp(unit, tz.clone()),
            )?
        }
        other => {
            return user_err!(
                function,
                "expected a DATE or TIMESTAMP argument, got {other}"
            );
        }
    })
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
            let millis = fixed.fixed_millis()?.checked_mul(n)?;
            ts.checked_add_signed(TimeDelta::try_milliseconds(millis)?)
        }
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    };
    next.and_then(|n| n.pred_opt())
        .map(|d| d.day())
        .unwrap_or(31)
}

fn is_leap_year(year: i32) -> bool {
    NaiveDate::from_ymd_opt(year, 2, 29).is_some()
}

/// Joda-Time's `BasicMonthOfYearDateTimeField.getDifferenceAsLong(minuend,
/// subtrahend)`, which Trino's `date_diff('month', ...)` uses: whole months,
/// where a minuend on the last day of its month counts a subtrahend later in
/// its own month as a full month (`2024-01-31 → 2024-02-29` is 1).
pub(crate) fn joda_months_between(minuend: NaiveDateTime, subtrahend: NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -joda_months_between(subtrahend, minuend);
    }
    let mut difference = (i64::from(minuend.year()) - i64::from(subtrahend.year())) * 12
        + (i64::from(minuend.month()) - i64::from(subtrahend.month()));
    let minuend_dom = minuend.day();
    let mut subtrahend = subtrahend;
    if minuend_dom == days_in_month(minuend.year(), minuend.month())
        && subtrahend.day() > minuend_dom
    {
        subtrahend = subtrahend
            .with_day(minuend_dom)
            .expect("clamping to an existing day of the month");
    }
    if (minuend.day(), minuend.time()) < (subtrahend.day(), subtrahend.time()) {
        difference -= 1;
    }
    difference
}

/// Joda-Time's `BasicChronology.getYearDifference`, which Trino's
/// `date_diff('year', ...)` uses: whole years comparing the offsets into the
/// year, with February 29 balanced against non-leap years.
pub(crate) fn joda_years_between(minuend: NaiveDateTime, subtrahend: NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -joda_years_between(subtrahend, minuend);
    }
    let remainder = |t: NaiveDateTime| {
        let start = NaiveDate::from_ymd_opt(t.year(), 1, 1)
            .expect("January 1st exists")
            .and_time(NaiveTime::MIN);
        (t - start).num_milliseconds()
    };
    const FEB_29: i64 = (31 + 29 - 1) * MILLIS_PER_DAY;
    let mut minuend_rem = remainder(minuend);
    let mut subtrahend_rem = remainder(subtrahend);
    if subtrahend_rem >= FEB_29 {
        if is_leap_year(subtrahend.year()) {
            if !is_leap_year(minuend.year()) {
                subtrahend_rem -= MILLIS_PER_DAY;
            }
        } else if minuend_rem >= FEB_29 && is_leap_year(minuend.year()) {
            minuend_rem -= MILLIS_PER_DAY;
        }
    }
    let mut difference = i64::from(minuend.year()) - i64::from(subtrahend.year());
    if minuend_rem < subtrahend_rem {
        difference -= 1;
    }
    difference
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
        if !is_integer(&arg_types[1]) && !matches!(arg_types[1], DataType::Null) {
            return Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function date_add: the value must be an \
                 integer (Trino does not truncate fractional values; use date_add with a \
                 finer unit instead)",
                trino_type_name(&arg_types[1])
            )));
        }
        match &arg_types[2] {
            t @ (DataType::Date32 | DataType::Timestamp(_, _)) => Ok(t.clone()),
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function date_add: the third argument must \
                 be a date or timestamp",
                trino_type_name(other)
            ))),
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
        let inputs = to_naive("date_add", &input)?;
        let mut out = Vec::with_capacity(rows);
        for (i, ts) in inputs.into_iter().enumerate() {
            let (Some(ts), false) = (ts, values.is_null(i)) else {
                out.push(None);
                continue;
            };
            match shift(ts, unit, values.value(i)) {
                Some(v) => out.push(Some(v)),
                None => {
                    return user_err!(
                        "date_add",
                        "{} {unit:?} from {ts} overflows the {} range",
                        values.value(i),
                        trino_type_name(&input_type)
                    );
                }
            }
        }
        Ok(ColumnarValue::Array(from_naive(
            "date_add",
            out,
            &input_type,
        )?))
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
                return Err(type_mismatch(format!(
                    "Unexpected parameters ({}) for function date_diff: argument {} must be \
                     a date or timestamp",
                    trino_type_name(t),
                    i + 2
                )));
            }
        }
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit = unit_arg("date_diff", &args.args[0])?;
        let rows = args.number_rows;
        let a = to_naive("date_diff", &args.args[1].to_array(rows)?)?;
        let b = to_naive("date_diff", &args.args[2].to_array(rows)?)?;
        let mut out = Int64Builder::with_capacity(rows);
        for (x, y) in a.into_iter().zip(b) {
            let (Some(x), Some(y)) = (x, y) else {
                out.append_null();
                continue;
            };
            let diff = match unit.fixed_millis() {
                Some(len) => (y - x).num_milliseconds() / len,
                None => match unit {
                    Unit::Month => joda_months_between(y, x),
                    Unit::Quarter => joda_months_between(y, x) / 3,
                    _ => joda_years_between(y, x),
                },
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

fn navigate<'a>(value: &'a json::Json, steps: &[Step]) -> Option<&'a json::Json> {
    let mut current = value;
    for step in steps {
        current = match step {
            Step::Key(k) => current.get(k)?,
            Step::Index(i) => current.index(*i)?,
        };
    }
    Some(current)
}

/// Trino's `JsonFunctions.jsonParse` catches every parse failure and
/// reports `Cannot convert '<text>' to JSON`; serde-style offsets ("expected
/// `null` at offset 0") are the parser's wording, not Athena's.
fn parse_json(_function: &str, text: &str) -> Result<json::Json> {
    json::parse(text).map_err(|_| {
        data_error(
            "INVALID_FUNCTION_ARGUMENT",
            format!("Cannot convert '{text}' to JSON"),
        )
    })
}

/// Evaluate a `(json, path) -> T` function row by row. `f` receives the
/// navigated value (`None` when the path does not exist).
fn json_path_map<T>(
    function: &str,
    args: &ScalarFunctionArgs,
    mut f: impl FnMut(Option<&json::Json>) -> Option<T>,
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
        // Trino's varchar overloads return NULL for text that is not JSON
        // (only `json_parse` raises); a bad path is still an error.
        let Ok(value) = json::parse(json.value(i)) else {
            append(None);
            continue;
        };
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
                json::Json::String(s) => Some(s.clone()),
                json::Json::Number(token) => Some(token.clone()),
                json::Json::Bool(b) => Some(b.to_string()),
                json::Json::Null | json::Json::Object(_) | json::Json::Array(_) => None,
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
            |v| v.map(json::Json::compact),
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
                json::Json::Object(o) => Some(o.len() as i64),
                json::Json::Array(a) => Some(a.len() as i64),
                _ => Some(0),
            },
            |v| out.append_option(v),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Evaluate a `(json) -> T` function row by row. Text that is not JSON is
/// an error when `strict` (`json_parse`) and NULL otherwise
/// (`json_array_length`), as in Trino's varchar overloads.
fn json_map<T>(
    function: &str,
    strict: bool,
    args: &ScalarFunctionArgs,
    mut f: impl FnMut(json::Json) -> Option<T>,
    mut append: impl FnMut(Option<T>),
) -> Result<()> {
    let rows = args.number_rows;
    let json = string_array(function, &args.args[0], rows)?;
    for i in 0..rows {
        if json.is_null(i) {
            append(None);
            continue;
        }
        let parsed = if strict {
            parse_json(function, json.value(i))?
        } else {
            match json::parse(json.value(i)) {
                Ok(v) => v,
                Err(_) => {
                    append(None);
                    continue;
                }
            }
        };
        append(f(parsed));
    }
    Ok(())
}

/// Trino `json_parse(varchar)`: validates the text as JSON. glaux keeps the
/// JSON type as text, so the output is Trino's canonical form (sorted keys,
/// last duplicate key wins, exact integers, Java double text for other
/// numbers); invalid JSON is an error, as in Trino.
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
            true,
            &args,
            |v| Some(v.canonical()),
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
            true,
            &args,
            |v| Some(v.canonical()),
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
            false,
            &args,
            |v| match v {
                json::Json::Array(a) => Some(a.len() as i64),
                _ => None,
            },
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
    fn month_and_year_differences_follow_joda() {
        // date_diff('month', a, b) = joda_months_between(b, a)
        let months = |a: &str, b: &str| joda_months_between(dt(b), dt(a));
        assert_eq!(months("2024-01-31 00:00:00", "2024-02-29 00:00:00"), 1);
        assert_eq!(months("2024-01-30 00:00:00", "2024-02-29 00:00:00"), 1);
        assert_eq!(months("2024-03-31 00:00:00", "2024-04-30 00:00:00"), 1);
        assert_eq!(months("2024-02-29 00:00:00", "2024-01-31 00:00:00"), -1);
        assert_eq!(months("2024-01-31 00:00:00", "2024-03-01 00:00:00"), 1);
        assert_eq!(months("2024-01-15 00:00:00", "2024-03-15 00:00:00"), 2);
        assert_eq!(months("2024-01-15 00:00:01", "2024-03-15 00:00:00"), 1);
        assert_eq!(months("2024-03-15 00:00:00", "2024-01-16 00:00:00"), -1);
        assert_eq!(months("2024-03-15 00:00:00", "2024-01-15 00:00:00"), -2);
        assert_eq!(months("2024-01-31 00:00:00", "2024-04-30 00:00:00") / 3, 1);
        let years = |a: &str, b: &str| joda_years_between(dt(b), dt(a));
        assert_eq!(years("2023-01-01 00:00:00", "2024-01-01 00:00:00"), 1);
        assert_eq!(years("2023-01-01 00:00:01", "2024-01-01 00:00:00"), 0);
        assert_eq!(years("2024-02-29 00:00:00", "2025-02-28 00:00:00"), 1);
        assert_eq!(years("2024-02-29 00:00:00", "2025-03-01 00:00:00"), 1);
        assert_eq!(years("2023-03-01 00:00:00", "2024-02-29 00:00:00"), 0);
        assert_eq!(years("2023-03-01 00:00:00", "2024-03-01 00:00:00"), 1);
        assert_eq!(years("2024-03-01 00:00:00", "2023-03-01 00:00:00"), -1);
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
