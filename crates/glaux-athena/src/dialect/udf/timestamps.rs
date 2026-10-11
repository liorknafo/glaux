//! Trino's `timestamp(3)` and `date` value rules: literal and cast parsing,
//! millisecond rounding, and `date ± interval`.
//!
//! - `trino_timestamp(x)` / `trino_try_timestamp(x)`: `CAST(x AS TIMESTAMP)`.
//!   A varchar is parsed with Trino's cast pattern (`YYYY-MM-DD[ HH:MM[:SS
//!   [.fraction]]]`, no zone, no `T`), the fraction rounded HALF_UP to
//!   milliseconds; a date becomes midnight; a timestamp of any precision is
//!   rounded to milliseconds. Every other source type is a `TYPE_MISMATCH`
//!   (Trino has no `bigint → timestamp` cast).
//! - `trino_timestamp_literal(x)`: `TIMESTAMP '...'`, which also accepts a
//!   date-only or `HH:MM` text, but refuses zones and more than three
//!   fractional digits (a `timestamp(p > 3)` literal would print its extra
//!   digits on Trino).
//! - `trino_date(x)` / `trino_try_date(x)`: `CAST(x AS DATE)` and `DATE
//!   '...'`: a varchar must be exactly `YYYY-MM-DD` (a trailing time part
//!   is an error, not ignored as Arrow's parser does), a timestamp is
//!   truncated to its day.
//! - `trino_timestamp_millis(x)`: rounds a timestamp of any precision to
//!   milliseconds (Trino's `roundDiv`), keeping its zone; applied to every
//!   scanned non-millisecond timestamp column and to `now()`.
//! - `trino_date_plus_interval(date, interval)` / `trino_date_minus_interval`:
//!   `date ± interval`, which Trino refuses when the interval has an hour,
//!   minute, or second part (DataFusion would silently drop it).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Builder, PrimitiveArray, TimestampMillisecondBuilder,
};
use arrow::compute::kernels::numeric;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, string_array, type_mismatch};
use crate::dialect::error::GlauxSqlError;
use crate::dialect::udf::casts::trino_type_name;

/// The timestamp / date UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoTimestamp::new(TimestampMode::Cast { try_cast: false })),
        ScalarUDF::new_from_impl(TrinoTimestamp::new(TimestampMode::Cast { try_cast: true })),
        ScalarUDF::new_from_impl(TrinoTimestamp::new(TimestampMode::Literal)),
        ScalarUDF::new_from_impl(TrinoDate::new(false)),
        ScalarUDF::new_from_impl(TrinoDate::new(true)),
        ScalarUDF::new_from_impl(TrinoTimestampMillis::new()),
        ScalarUDF::new_from_impl(TrinoDateInterval::new(true)),
        ScalarUDF::new_from_impl(TrinoDateInterval::new(false)),
    ]
}

pub(crate) const MILLIS_PER_DAY: i64 = 86_400_000;

/// Trino's `roundDiv`: `value / factor` rounded half up (towards positive
/// infinity on a tie, as Java's `Math.round`).
pub(crate) fn round_div(value: i64, factor: i64) -> i64 {
    if factor == 1 {
        return value;
    }
    if value >= 0 {
        (value + factor / 2) / factor
    } else {
        (value + 1 - factor / 2) / factor
    }
}

/// Milliseconds since the epoch of a timestamp value in `unit`, rounded half
/// up to the millisecond.
pub(crate) fn to_millis_rounded(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => value.saturating_mul(1000),
        TimeUnit::Millisecond => value,
        TimeUnit::Microsecond => round_div(value, 1_000),
        TimeUnit::Nanosecond => round_div(value, 1_000_000),
    }
}

pub(crate) fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")
}

fn invalid_cast(value: &str, target: &str) -> datafusion::common::DataFusionError {
    data_error(
        "INVALID_CAST_ARGUMENT",
        format!("Value cannot be cast to {target}: {value}"),
    )
}

/// Split `[-+]?\d{4,}-\d{1,2}-\d{1,2}` off the front of `s`; returns the
/// date and the remainder.
fn parse_date_prefix(s: &str) -> Option<(NaiveDate, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let negative = match bytes.first() {
        Some(b'-') => {
            i = 1;
            true
        }
        Some(b'+') => {
            i = 1;
            false
        }
        _ => false,
    };
    let year_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i - year_start < 4 {
        return None;
    }
    let year: i32 = s[year_start..i].parse().ok()?;
    let year = if negative { -year } else { year };
    let (month, rest) = parse_field(&s[i..], '-')?;
    let (day, rest) = parse_field(rest, '-')?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    Some((date, rest))
}

/// `sep` followed by one or two digits.
fn parse_field(s: &str, sep: char) -> Option<(u32, &str)> {
    let rest = s.strip_prefix(sep)?;
    let digits = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    if !(1..=2).contains(&digits) {
        return None;
    }
    Some((rest[..digits].parse().ok()?, &rest[digits..]))
}

/// A parsed Trino timestamp text: the wall-clock value at millisecond
/// precision plus the number of fractional digits written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParsedTimestamp {
    pub value: NaiveDateTime,
    pub fraction_digits: usize,
    pub has_zone: bool,
}

/// Parse Trino's timestamp pattern: `date[ HH:MM[:SS[.fraction]]][zone]`.
/// The fraction is rounded half up to milliseconds. `None` when the text
/// does not match.
pub(crate) fn parse_trino_timestamp(text: &str) -> Option<ParsedTimestamp> {
    let text = text.trim();
    let (date, rest) = parse_date_prefix(text)?;
    let mut fraction_digits = 0;
    let (time, rest) = if let Some(after_space) = rest.strip_prefix(' ') {
        let (hour, rest) = {
            let digits = after_space
                .bytes()
                .take_while(|b| b.is_ascii_digit())
                .count();
            if !(1..=2).contains(&digits) {
                return None;
            }
            (
                after_space[..digits].parse::<u32>().ok()?,
                &after_space[digits..],
            )
        };
        let (minute, rest) = parse_field(rest, ':')?;
        let (second, fraction, rest) = match parse_field(rest, ':') {
            Some((second, rest)) => match rest.strip_prefix('.') {
                Some(after_dot) => {
                    let digits = after_dot.bytes().take_while(|b| b.is_ascii_digit()).count();
                    if digits == 0 {
                        return None;
                    }
                    (second, Some(&after_dot[..digits]), &after_dot[digits..])
                }
                None => (second, None, rest),
            },
            None => (0, None, rest),
        };
        let mut millis = 0u32;
        let mut carry = 0i64;
        if let Some(fraction) = fraction {
            fraction_digits = fraction.len();
            if fraction_digits > 12 {
                return None;
            }
            let padded = format!("{fraction:0<4}");
            millis = padded[..3].parse().ok()?;
            if padded.as_bytes()[3] >= b'5' {
                millis += 1;
                if millis == 1000 {
                    millis = 0;
                    carry = 1;
                }
            }
        }
        let time = NaiveTime::from_hms_milli_opt(hour, minute, second, millis)?;
        (
            NaiveDateTime::new(date, time) + TimeDelta::seconds(carry),
            rest,
        )
    } else {
        (NaiveDateTime::new(date, NaiveTime::MIN), rest)
    };
    let has_zone = !rest.trim().is_empty();
    Some(ParsedTimestamp {
        value: time,
        fraction_digits,
        has_zone,
    })
}

/// Parse Trino's date text: `[-+]?\d{4,}-\d{1,2}-\d{1,2}` and nothing else.
pub(crate) fn parse_trino_date(text: &str) -> Option<NaiveDate> {
    let (date, rest) = parse_date_prefix(text.trim())?;
    rest.is_empty().then_some(date)
}

pub(crate) fn date_to_days(date: NaiveDate) -> Result<i32> {
    i32::try_from((date - epoch_date()).num_days()).map_err(|_| {
        data_error(
            "INVALID_CAST_ARGUMENT",
            format!("date {date} is out of range"),
        )
    })
}

/// Milliseconds of a timestamp column, rounded half up to the millisecond,
/// one per row (`None` for NULL).
fn timestamp_millis(array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    fn collect<T: ArrowPrimitiveType<Native = i64>>(
        array: &PrimitiveArray<T>,
        unit: TimeUnit,
    ) -> Vec<Option<i64>> {
        array
            .iter()
            .map(|v| v.map(|v| to_millis_rounded(v, unit)))
            .collect()
    }
    Ok(match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => collect(
            array.as_primitive::<TimestampSecondType>(),
            TimeUnit::Second,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => collect(
            array.as_primitive::<TimestampMillisecondType>(),
            TimeUnit::Millisecond,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => collect(
            array.as_primitive::<TimestampMicrosecondType>(),
            TimeUnit::Microsecond,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => collect(
            array.as_primitive::<TimestampNanosecondType>(),
            TimeUnit::Nanosecond,
        ),
        other => {
            return Err(type_mismatch(format!(
                "expected a timestamp, got {}",
                trino_type_name(other)
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// CAST(x AS TIMESTAMP) / TIMESTAMP 'x'
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampMode {
    /// `CAST` / `TRY_CAST`.
    Cast { try_cast: bool },
    /// `TIMESTAMP '...'`.
    Literal,
}

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoTimestamp {
    signature: Signature,
    mode: TimestampMode,
}

impl TrinoTimestamp {
    /// New instance.
    pub fn new(mode: TimestampMode) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            mode,
        }
    }
}

fn literal_error(text: &str, why: &str) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(GlauxSqlError::unsupported(
        "timestamp literal",
        format!("`TIMESTAMP '{text}'`: {why}"),
    )))
}

impl ScalarUDFImpl for TrinoTimestamp {
    fn name(&self) -> &str {
        match self.mode {
            TimestampMode::Cast { try_cast: false } => "trino_timestamp",
            TimestampMode::Cast { try_cast: true } => "trino_try_timestamp",
            TimestampMode::Literal => "trino_timestamp_literal",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _) => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
            other => Err(type_mismatch(format!(
                "Cannot cast {} to timestamp",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let mut out = TimestampMillisecondBuilder::with_capacity(rows);
        match input.data_type() {
            DataType::Null => out.append_nulls(rows),
            DataType::Date32 | DataType::Date64 => {
                let days = arrow::compute::cast(&input, &DataType::Date32)?;
                for v in days.as_primitive::<arrow::datatypes::Date32Type>() {
                    out.append_option(v.map(|d| i64::from(d) * MILLIS_PER_DAY));
                }
            }
            DataType::Timestamp(_, _) => {
                for v in timestamp_millis(&input)? {
                    out.append_option(v);
                }
            }
            _ => {
                let strings = string_array("CAST", &args.args[0], rows)?;
                for i in 0..rows {
                    if strings.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let text = strings.value(i);
                    let parsed = parse_trino_timestamp(text);
                    match self.mode {
                        TimestampMode::Literal => {
                            let Some(parsed) = parsed else {
                                return Err(literal_error(
                                    text,
                                    "not a valid timestamp literal (expected `YYYY-MM-DD[ \
                                     HH:MM[:SS[.fff]]]`)",
                                ));
                            };
                            if parsed.has_zone {
                                return Err(literal_error(
                                    text,
                                    "carries a time zone; glaux handles timestamps as zone-less \
                                     UTC instants in v0.1",
                                ));
                            }
                            if parsed.fraction_digits > 3 {
                                return Err(literal_error(
                                    text,
                                    &format!(
                                        "has {} fractional digits, which makes it a timestamp({}) \
                                         on Trino; glaux only carries timestamp(3) (round or \
                                         truncate the literal, or CAST the text, which rounds)",
                                        parsed.fraction_digits, parsed.fraction_digits
                                    ),
                                ));
                            }
                            out.append_value(parsed.value.and_utc().timestamp_millis());
                        }
                        TimestampMode::Cast { try_cast } => match parsed {
                            Some(parsed) if !parsed.has_zone => {
                                out.append_value(parsed.value.and_utc().timestamp_millis());
                            }
                            _ if try_cast => out.append_null(),
                            _ => return Err(invalid_cast(text, "timestamp")),
                        },
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// CAST(x AS DATE) / DATE 'x'
// ---------------------------------------------------------------------------

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDate {
    signature: Signature,
    try_cast: bool,
}

impl TrinoDate {
    /// New instance.
    pub fn new(try_cast: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            try_cast,
        }
    }
}

impl ScalarUDFImpl for TrinoDate {
    fn name(&self) -> &str {
        if self.try_cast {
            "trino_try_date"
        } else {
            "trino_date"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _) => Ok(DataType::Date32),
            other => Err(type_mismatch(format!(
                "Cannot cast {} to date",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let mut out = Date32Builder::with_capacity(rows);
        match input.data_type() {
            DataType::Null => out.append_nulls(rows),
            DataType::Date32 | DataType::Date64 => {
                let days = arrow::compute::cast(&input, &DataType::Date32)?;
                for v in days.as_primitive::<arrow::datatypes::Date32Type>() {
                    out.append_option(v);
                }
            }
            DataType::Timestamp(_, _) => {
                for v in timestamp_millis(&input)? {
                    out.append_option(v.map(|ms| ms.div_euclid(MILLIS_PER_DAY) as i32));
                }
            }
            _ => {
                let strings = string_array("CAST", &args.args[0], rows)?;
                for i in 0..rows {
                    if strings.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let text = strings.value(i);
                    match parse_trino_date(text) {
                        Some(date) => out.append_value(date_to_days(date)?),
                        None if self.try_cast => out.append_null(),
                        None => return Err(invalid_cast(text, "date")),
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// trino_timestamp_millis(x)
// ---------------------------------------------------------------------------

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoTimestampMillis {
    signature: Signature,
}

impl Default for TrinoTimestampMillis {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoTimestampMillis {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoTimestampMillis {
    fn name(&self) -> &str {
        "trino_timestamp_millis"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Timestamp(_, tz) => {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))
            }
            other => Err(type_mismatch(format!(
                "expected a timestamp, got {}",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let DataType::Timestamp(_, tz) = input.data_type().clone() else {
            unreachable!("return_type checked the argument type");
        };
        let millis = timestamp_millis(&input)?;
        let array = PrimitiveArray::<TimestampMillisecondType>::from(millis).with_timezone_opt(tz);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

// ---------------------------------------------------------------------------
// date ± interval
// ---------------------------------------------------------------------------

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDateInterval {
    signature: Signature,
    plus: bool,
}

impl TrinoDateInterval {
    /// New instance for `+` (`plus`) or `-`.
    pub fn new(plus: bool) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            plus,
        }
    }
}

fn sub_day_error() -> datafusion::common::DataFusionError {
    data_error(
        "INVALID_FUNCTION_ARGUMENT",
        "Cannot add hour, minutes or seconds to a date",
    )
}

/// Whether every non-null interval has a whole number of days.
fn check_whole_days(interval: &ArrayRef) -> Result<()> {
    match interval.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values = interval.as_primitive::<arrow::datatypes::IntervalMonthDayNanoType>();
            for v in values.iter().flatten() {
                let IntervalMonthDayNano { nanoseconds, .. } = v;
                if nanoseconds % (MILLIS_PER_DAY * 1_000_000) != 0 {
                    return Err(sub_day_error());
                }
            }
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let values = interval.as_primitive::<arrow::datatypes::IntervalDayTimeType>();
            for v in values.iter().flatten() {
                let IntervalDayTime { milliseconds, .. } = v;
                if milliseconds % (MILLIS_PER_DAY as i32) != 0 {
                    return Err(sub_day_error());
                }
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => {}
        DataType::Duration(_) => return Err(sub_day_error()),
        other => {
            return Err(type_mismatch(format!(
                "expected an interval, got {}",
                trino_type_name(other)
            )));
        }
    }
    Ok(())
}

impl ScalarUDFImpl for TrinoDateInterval {
    fn name(&self) -> &str {
        if self.plus {
            "trino_date_plus_interval"
        } else {
            "trino_date_minus_interval"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let date = args.args[0].to_array(rows)?;
        let interval = args.args[1].to_array(rows)?;
        check_whole_days(&interval)?;
        let date = arrow::compute::cast(&date, &DataType::Date32)?;
        let result = if self.plus {
            numeric::add(&date, &interval)?
        } else {
            numeric::sub(&date, &interval)?
        };
        let result: ArrayRef = if result.data_type() == &DataType::Date32 {
            result
        } else {
            arrow::compute::cast(&result, &DataType::Date32)?
        };
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }

    #[test]
    fn round_div_matches_trino() {
        assert_eq!(round_div(1499, 1000), 1);
        assert_eq!(round_div(1500, 1000), 2);
        assert_eq!(round_div(-1500, 1000), -1);
        assert_eq!(round_div(-1501, 1000), -2);
        assert_eq!(round_div(-1499, 1000), -1);
        assert_eq!(to_millis_rounded(999_600_000, TimeUnit::Nanosecond), 1000);
        assert_eq!(to_millis_rounded(123_456_789, TimeUnit::Nanosecond), 123);
        assert_eq!(to_millis_rounded(1_234_500, TimeUnit::Microsecond), 1_235);
    }

    #[test]
    fn timestamp_text_follows_trino_cast_pattern() {
        let p =
            |s: &str| parse_trino_timestamp(s).map(|p| (p.value, p.fraction_digits, p.has_zone));
        assert_eq!(
            p("2024-01-05 10:00:00.9999"),
            Some((dt("2024-01-05 10:00:01.000"), 4, false))
        );
        assert_eq!(
            p("2024-01-05 10:00:00.1235"),
            Some((dt("2024-01-05 10:00:00.124"), 4, false))
        );
        assert_eq!(
            p("2024-01-05 10:00:00.1234"),
            Some((dt("2024-01-05 10:00:00.123"), 4, false))
        );
        assert_eq!(
            p("2024-01-05 23:59:59.9995"),
            Some((dt("2024-01-06 00:00:00.000"), 4, false))
        );
        assert_eq!(p("2024-01-05"), Some((dt("2024-01-05 00:00:00"), 0, false)));
        assert_eq!(
            p("2024-1-5 1:2"),
            Some((dt("2024-01-05 01:02:00"), 0, false))
        );
        assert_eq!(
            p(" 2024-01-05 10:00 "),
            Some((dt("2024-01-05 10:00:00"), 0, false))
        );
        assert_eq!(
            p("10000-01-01 00:00:00"),
            Some((dt("+10000-01-01 00:00:00"), 0, false))
        );
        assert_eq!(p("2024-01-05 10:00:00+05:00").map(|t| t.2), Some(true));
        assert_eq!(p("2024-01-05 10:00:00 UTC").map(|t| t.2), Some(true));
        assert_eq!(
            p("2024-01-05 10:00:00 America/New_York").map(|t| t.2),
            Some(true)
        );
        // A `T` separator leaves `T10:00:00` as an (invalid) zone, as in
        // Trino's pattern; the caller refuses zoned values.
        assert_eq!(p("2024-01-05T10:00:00").map(|t| t.2), Some(true));
        for bad in [
            "2024-01-05 10",
            "2024-01-05 24:00:00",
            "2024-13-01",
            "24-01-05",
            "2024-01-05 10:00:00.",
            "abc",
            "",
        ] {
            assert_eq!(p(bad), None, "{bad}");
        }
    }

    #[test]
    fn date_text_is_exactly_a_calendar_date() {
        assert_eq!(
            parse_trino_date("2024-01-05"),
            NaiveDate::from_ymd_opt(2024, 1, 5)
        );
        assert_eq!(
            parse_trino_date(" 2024-1-5 "),
            NaiveDate::from_ymd_opt(2024, 1, 5)
        );
        assert_eq!(
            parse_trino_date("9999-12-31"),
            NaiveDate::from_ymd_opt(9999, 12, 31)
        );
        assert_eq!(parse_trino_date("2024-01-05 10:00:00"), None);
        assert_eq!(parse_trino_date("2024-01-05T00:00:00"), None);
        assert_eq!(parse_trino_date("2024-02-30"), None);
        assert_eq!(parse_trino_date("20240105"), None);
    }
}
