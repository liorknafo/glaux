//! Date/time functions whose DataFusion namesakes accept or return types
//! Trino's do not.
//!
//! - `date_trunc(unit, x)` returns `x`'s type: a `DATE` stays a `DATE`
//!   (DataFusion widens it to a timestamp), sub-day units on a `DATE` are
//!   errors, and varchar input is refused (DataFusion would parse it).
//! - `to_unixtime(timestamp)` refuses varchar input for the same reason.
//! - `from_unixtime(double)` rounds with Java's `Math.round` (half towards
//!   positive infinity, `Math.round(-0.5)` is `0`), which DataFusion's
//!   `round` (half away from zero) gets wrong for every negative
//!   half-millisecond epoch, and refuses the non-numeric arguments Trino
//!   has no overload for.
//! - `trino_date_parse(text, chrono_format, function, trino_format)` is
//!   `date_parse` / `parse_datetime`. It replaces DataFusion's
//!   `to_timestamp`, which resolves the parsed fields with chrono's
//!   `Parsed::to_naive_datetime_with_offset` and *falls back to midnight*
//!   whenever the time is incomplete — so `%H` without a minute, and a
//!   12-hour field without AM/PM, silently dropped the whole time of day.
//!   Trino runs Joda, whose parse bucket starts at `1970-01-01T00:00:00`
//!   and applies the parsed fields on top, so every field the format does
//!   not name keeps its epoch default. This UDF does the same: it resolves
//!   the date with chrono where the parsed fields are complete and fills
//!   year / month / day / hour / minute / second / nanosecond from the
//!   epoch otherwise. A parsed second of 60 (chrono's leap second, which
//!   Joda refuses) raises `Value 60 for secondOfMinute must be in the
//!   range [0,59]`.

use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Array, TimestampMillisecondBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, TimeUnit, TimestampMicrosecondType};
use chrono::format::{Parsed, StrftimeItems};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::math::java_math_round;
use super::{
    Unit, data_error, from_naive, string_array, to_naive, type_mismatch, unit_arg, user_err,
};
use crate::dialect::udf::casts::trino_type_name;

/// The date/time UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoDateTrunc::new()),
        ScalarUDF::new_from_impl(TrinoToUnixtime::new()),
        ScalarUDF::new_from_impl(TrinoDateParse::new()),
        ScalarUDF::new_from_impl(TrinoFromUnixtime::new()),
    ]
}

/// Trino packs a `timestamp with time zone` as milliseconds shifted left by
/// the 12-bit zone key, so a value outside the remaining 52 signed bits is
/// `Millis overflow` (the diagnostic Athena engine v3 raises too).
const MAX_PACKED_MILLIS: i64 = (1 << 51) - 1;

/// `trino_from_unixtime(x)`: Trino's `from_unixtime(double)` —
/// `packDateTimeWithZone(Math.round(unixTime * 1000), UTC)`, a
/// `timestamp(3) with time zone` at UTC.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoFromUnixtime {
    signature: Signature,
}

impl Default for TrinoFromUnixtime {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoFromUnixtime {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoFromUnixtime {
    fn name(&self) -> &str {
        "trino_from_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Trino declares `from_unixtime(double)` only; an exact numeric
        // argument is coerced, a varchar or boolean one is not.
        if matches!(
            arg_types[0],
            DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        ) {
            Ok(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into()),
            ))
        } else {
            Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function from_unixtime. Expected: \
                 from_unixtime(double)",
                trino_type_name(&arg_types[0])
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let seconds = cast(&input, &DataType::Float64)?;
        let seconds = seconds.as_primitive::<Float64Type>();
        let mut out = TimestampMillisecondBuilder::with_capacity(rows);
        for i in 0..rows {
            if seconds.is_null(i) {
                out.append_null();
                continue;
            }
            let millis = java_math_round(seconds.value(i) * 1000.0);
            if millis.abs() > MAX_PACKED_MILLIS {
                return Err(data_error(
                    "INVALID_FUNCTION_ARGUMENT",
                    format!("Millis overflow: {millis}"),
                ));
            }
            out.append_value(millis);
        }
        Ok(ColumnarValue::Array(Arc::new(
            out.finish().with_timezone("UTC"),
        )))
    }
}

/// Joda's parse bucket starts at the epoch, so a field the pattern does
/// not name keeps its `1970-01-01T00:00:00.000` default.
const EPOCH_YEAR: i64 = 1970;

/// Resolve the date the way Joda does: chrono's own resolution when the
/// parsed fields determine a date, and the epoch defaults for the fields
/// the format left out (`%Y-%m` is the first of the month, a
/// time-of-day-only format is 1970-01-01). `None` when the remaining
/// fields still cannot be resolved — a weekday without a full date, say —
/// which is refused loudly rather than guessed at.
fn resolve_date(parsed: &Parsed) -> Option<NaiveDate> {
    if let Ok(date) = parsed.to_naive_date() {
        return Some(date);
    }
    let mut p = parsed.clone();
    if p.year().is_none()
        && p.year_mod_100().is_none()
        && p.isoyear().is_none()
        && p.isoyear_mod_100().is_none()
    {
        p.set_year(EPOCH_YEAR).ok()?;
    }
    if let Ok(date) = p.to_naive_date() {
        return Some(date);
    }
    if p.month().is_none()
        && p.ordinal().is_none()
        && p.isoweek().is_none()
        && p.week_from_mon().is_none()
        && p.week_from_sun().is_none()
    {
        p.set_month(1).ok()?;
    }
    if let Ok(date) = p.to_naive_date() {
        return Some(date);
    }
    if p.day().is_none() && p.ordinal().is_none() {
        p.set_day(1).ok()?;
    }
    p.to_naive_date().ok()
}

/// Resolve the time of day with Joda's defaults: an unset hour, minute,
/// second, or fraction is the epoch's zero. `%I` / `hh` without an AM/PM
/// field leaves `hour_div_12` unset, which is Joda's AM default — where
/// chrono's own `to_naive_time` refuses the whole time.
fn resolve_time(parsed: &Parsed) -> Option<NaiveTime> {
    let hour = parsed.hour_div_12().unwrap_or(0) * 12 + parsed.hour_mod_12().unwrap_or(0);
    NaiveTime::from_hms_nano_opt(
        hour,
        parsed.minute().unwrap_or(0),
        parsed.second().unwrap_or(0),
        parsed.nanosecond().unwrap_or(0),
    )
}

/// Which century a two-digit year token belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoDigitYear {
    /// Trino's MySQL-style formatter (`date_parse` / `date_format`) builds
    /// `%y` as `appendTwoDigitYear(PIVOT_YEAR)` with `PIVOT_YEAR = 2020`:
    /// the fixed window 1970..=2069, which is exactly what chrono's `%y`
    /// resolves to on its own.
    Fixed,
    /// Joda's `DateTimeFormat` (`parse_datetime` / `format_datetime`)
    /// builds a two-character `y` / `Y` token as `appendTwoDigitYear(new
    /// DateTime().getYear() - 30)` (and `xx` as `appendTwoDigitWeekyear(new
    /// DateTime().getWeekyear() - 30)`): a window that moves with the wall
    /// clock — in 2026 it is 1946..=2045, so `yy` reads `46` as 1946 where
    /// chrono would say 2046.
    JodaMovingPivot,
}

/// The year Joda's `appendTwoDigitYear(pivot)` parses `two_digit` as: the
/// value in the 100-year window `[pivot - 50, pivot + 49]` whose last two
/// digits are `two_digit` (`TwoDigitYear.parseInto`).
fn pivoted_year(two_digit: i32, pivot: i32) -> i32 {
    let low = pivot - 50;
    low + (two_digit - low).rem_euclid(100)
}

/// Re-resolve a two-digit year / week-year against Joda's moving pivot.
/// chrono has already read the digits into `year_mod_100` (`%y`) or
/// `isoyear_mod_100` (`%g`) and would resolve them against its own fixed
/// 1970..=2069 window, so the full year is pinned here before resolution.
fn apply_joda_pivot(parsed: &mut Parsed) {
    let now = chrono::Utc::now();
    if parsed.year().is_none()
        && let Some(two_digit) = parsed.year_mod_100()
    {
        let year = pivoted_year(two_digit, now.year() - 30);
        let _ = parsed.set_year(i64::from(year));
    }
    if parsed.isoyear().is_none()
        && let Some(two_digit) = parsed.isoyear_mod_100()
    {
        let year = pivoted_year(two_digit, now.iso_week().year() - 30);
        let _ = parsed.set_isoyear(i64::from(year));
    }
}

/// Parse `text` with the chrono `format`, filling every unnamed field from
/// Joda's epoch defaults. `Ok(None)` means the text does not parse.
pub fn parse_joda(
    text: &str,
    format: &str,
    two_digit_year: TwoDigitYear,
) -> Result<Option<NaiveDateTime>> {
    let mut parsed = Parsed::new();
    if chrono::format::parse(&mut parsed, text, StrftimeItems::new(format)).is_err() {
        return Ok(None);
    }
    if two_digit_year == TwoDigitYear::JodaMovingPivot {
        apply_joda_pivot(&mut parsed);
    }
    // chrono accepts a leap second (`10:30:60`); Joda raises.
    if parsed.second() == Some(60) {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            "Value 60 for secondOfMinute must be in the range [0,59]",
        ));
    }
    Ok(resolve_date(&parsed)
        .zip(resolve_time(&parsed))
        .map(|(date, time)| date.and_time(time)))
}

/// `trino_date_parse(text, chrono_format, function, trino_format)`: see the
/// module docs. The result is a zone-less `timestamp(3)`; `parse_datetime`
/// casts it to UTC in the rewriter.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDateParse {
    signature: Signature,
}

impl Default for TrinoDateParse {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoDateParse {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(4), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoDateParse {
    fn name(&self) -> &str {
        "trino_date_parse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
            }
            other => Err(type_mismatch(format!(
                "Unexpected parameters ({}, varchar) for function date_parse. Expected: \
                 date_parse(varchar, varchar)",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let texts = string_array("date_parse", &args.args[0], rows)?;
        let formats = string_array("date_parse", &args.args[1], rows)?;
        let functions = string_array("date_parse", &args.args[2], rows)?;
        let originals = string_array("date_parse", &args.args[3], rows)?;
        let mut out = TimestampMillisecondBuilder::with_capacity(rows);
        for i in 0..rows {
            if texts.is_null(i) || formats.is_null(i) {
                out.append_null();
                continue;
            }
            let function = if functions.is_null(i) {
                "date_parse"
            } else {
                functions.value(i)
            };
            // The two format languages pivot two-digit years differently;
            // only the Joda one (`parse_datetime`) moves with the clock.
            let two_digit_year = if function == "parse_datetime" {
                TwoDigitYear::JodaMovingPivot
            } else {
                TwoDigitYear::Fixed
            };
            match parse_joda(texts.value(i), formats.value(i), two_digit_year)? {
                Some(ts) => out.append_value(ts.and_utc().timestamp_millis()),
                None => {
                    return user_err!(
                        function,
                        "cannot parse '{}' with format '{}'",
                        texts.value(i),
                        originals.value(i)
                    );
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn is_date_or_timestamp(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
}

/// Truncate a naive timestamp to `unit` (weeks start on Monday, as in
/// Trino).
pub fn truncate(ts: NaiveDateTime, unit: Unit) -> Option<NaiveDateTime> {
    let day = ts.date();
    Some(match unit {
        Unit::Millisecond | Unit::Second | Unit::Minute | Unit::Hour | Unit::Day => {
            let millis = unit.fixed_millis()?;
            let since_midnight = (ts - day.and_hms_opt(0, 0, 0)?).num_milliseconds();
            day.and_hms_opt(0, 0, 0)?
                + TimeDelta::milliseconds(since_midnight - since_midnight % millis)
        }
        Unit::Week => {
            let monday = day - TimeDelta::days(i64::from(day.weekday().num_days_from_monday()));
            monday.and_hms_opt(0, 0, 0)?
        }
        Unit::Month => NaiveDate::from_ymd_opt(day.year(), day.month(), 1)?.and_hms_opt(0, 0, 0)?,
        Unit::Quarter => {
            let month = (day.month() - 1) / 3 * 3 + 1;
            NaiveDate::from_ymd_opt(day.year(), month, 1)?.and_hms_opt(0, 0, 0)?
        }
        Unit::Year => NaiveDate::from_ymd_opt(day.year(), 1, 1)?.and_hms_opt(0, 0, 0)?,
    })
}

/// `trino_date_trunc(unit, x)`: see the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoDateTrunc {
    signature: Signature,
}

impl Default for TrinoDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoDateTrunc {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoDateTrunc {
    fn name(&self) -> &str {
        "trino_date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[1] {
            t if is_date_or_timestamp(t) => Ok(t.clone()),
            other => Err(type_mismatch(format!(
                "Unexpected parameters (varchar, {}) for function date_trunc. Expected: \
                 date_trunc(varchar, date), date_trunc(varchar, timestamp)",
                trino_type_name(other)
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let unit = unit_arg("date_trunc", &args.args[0])?;
        let rows = args.number_rows;
        let input = args.args[1].to_array(rows)?;
        let input_type = input.data_type().clone();
        if matches!(input_type, DataType::Date32) && !unit.is_calendar() {
            return user_err!(
                "date_trunc",
                "{} is not a valid DATE field; cast to TIMESTAMP first",
                format!("{unit:?}").to_lowercase()
            );
        }
        let mut out = Vec::with_capacity(rows);
        for ts in to_naive("date_trunc", &input)? {
            match ts {
                None => out.push(None),
                Some(ts) => match truncate(ts, unit) {
                    Some(v) => out.push(Some(v)),
                    None => return user_err!("date_trunc", "{ts} is out of range"),
                },
            }
        }
        Ok(ColumnarValue::Array(from_naive(
            "date_trunc",
            out,
            &input_type,
        )?))
    }
}

/// `trino_to_unixtime(timestamp)`: seconds since the epoch as a double,
/// with microsecond resolution.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoToUnixtime {
    signature: Signature,
}

impl Default for TrinoToUnixtime {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoToUnixtime {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoToUnixtime {
    fn name(&self) -> &str {
        "trino_to_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if is_date_or_timestamp(&arg_types[0]) {
            Ok(DataType::Float64)
        } else {
            Err(type_mismatch(format!(
                "Unexpected parameters ({}) for function to_unixtime. Expected: \
                 to_unixtime(timestamp)",
                trino_type_name(&arg_types[0])
            )))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let input = args.args[0].to_array(rows)?;
        let micros = cast(&input, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
        let micros = micros.as_primitive::<TimestampMicrosecondType>();
        let out: Float64Array = micros
            .iter()
            .map(|v| v.map(|us| us as f64 / 1_000_000.0))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
    }

    #[test]
    fn leap_seconds_are_refused_like_joda() {
        parse_joda(
            "2024-01-05 10:30:59",
            "%Y-%m-%d %H:%M:%S",
            TwoDigitYear::Fixed,
        )
        .unwrap();
        assert_eq!(
            parse_joda("garbage", "%Y-%m-%d %H:%M:%S", TwoDigitYear::Fixed).unwrap(),
            None
        );
        let err = parse_joda(
            "2024-01-05 10:30:60",
            "%Y-%m-%d %H:%M:%S",
            TwoDigitYear::Fixed,
        )
        .unwrap_err();
        assert!(err.to_string().contains("secondOfMinute"), "{err}");
    }

    #[test]
    fn unnamed_fields_keep_jodas_epoch_defaults() {
        // An hour with no minute, and a 12-hour field with no AM/PM, are
        // exactly the shapes chrono cannot resolve on its own; Joda keeps
        // the parsed hour and defaults the rest.
        for (text, format, expected) in [
            ("2024-01-05 10", "%Y-%m-%d %H", ts(2024, 1, 5, 10, 0, 0)),
            (
                "2024-01-05 10:30",
                "%Y-%m-%d %I:%M",
                ts(2024, 1, 5, 10, 30, 0),
            ),
            (
                "2024-01-05 10 PM",
                "%Y-%m-%d %I %p",
                ts(2024, 1, 5, 22, 0, 0),
            ),
            ("12:30 AM", "%I:%M %p", ts(1970, 1, 1, 0, 30, 0)),
            ("2024-01", "%Y-%m", ts(2024, 1, 1, 0, 0, 0)),
            ("2024", "%Y", ts(2024, 1, 1, 0, 0, 0)),
            ("03-15", "%m-%d", ts(1970, 3, 15, 0, 0, 0)),
            ("2024-032", "%Y-%j", ts(2024, 2, 1, 0, 0, 0)),
            (
                "2024-01-05 10:30:15",
                "%Y-%m-%d %H:%M:%S",
                ts(2024, 1, 5, 10, 30, 15),
            ),
        ] {
            assert_eq!(
                parse_joda(text, format, TwoDigitYear::Fixed).unwrap(),
                Some(expected),
                "{text} / {format}"
            );
        }
    }
    #[test]
    fn two_digit_years_follow_the_pivot_of_their_format_language() {
        // Joda's window is [now - 80, now + 19] and moves every year, so
        // the expectation is computed the way `TwoDigitYear.parseInto`
        // does rather than hard-coded.
        let low = chrono::Utc::now().year() - 80;
        for two_digit in 0..100 {
            let expected = low + (two_digit - low).rem_euclid(100);
            assert_eq!(pivoted_year(two_digit, low + 50), expected);
            assert!((low..low + 100).contains(&expected));
            let text = format!("{two_digit:02}-01-05");
            assert_eq!(
                parse_joda(&text, "%y-%m-%d", TwoDigitYear::JodaMovingPivot).unwrap(),
                Some(ts(expected, 1, 5, 0, 0, 0)),
                "{text}"
            );
            // Trino's MySQL-style formatter pivots on a *fixed* 2020, which
            // is chrono's own 1970..=2069 window: `date_parse` must not
            // move with the clock.
            let fixed = if two_digit < 70 { 2000 } else { 1900 } + two_digit;
            assert_eq!(
                parse_joda(&text, "%y-%m-%d", TwoDigitYear::Fixed).unwrap(),
                Some(ts(fixed, 1, 5, 0, 0, 0)),
                "{text}"
            );
        }
        // A four-digit year is untouched by either pivot.
        for treatment in [TwoDigitYear::Fixed, TwoDigitYear::JodaMovingPivot] {
            assert_eq!(
                parse_joda("2046-01-05", "%Y-%m-%d", treatment).unwrap(),
                Some(ts(2046, 1, 5, 0, 0, 0))
            );
        }
    }

    #[test]
    fn truncation_follows_trino_units() {
        let t = ts(2024, 2, 14, 8, 30, 45);
        assert_eq!(
            truncate(t, Unit::Second).unwrap(),
            ts(2024, 2, 14, 8, 30, 45)
        );
        assert_eq!(
            truncate(t, Unit::Minute).unwrap(),
            ts(2024, 2, 14, 8, 30, 0)
        );
        assert_eq!(truncate(t, Unit::Hour).unwrap(), ts(2024, 2, 14, 8, 0, 0));
        assert_eq!(truncate(t, Unit::Day).unwrap(), ts(2024, 2, 14, 0, 0, 0));
        // 2024-02-14 is a Wednesday; the week starts Monday the 12th.
        assert_eq!(truncate(t, Unit::Week).unwrap(), ts(2024, 2, 12, 0, 0, 0));
        assert_eq!(truncate(t, Unit::Month).unwrap(), ts(2024, 2, 1, 0, 0, 0));
        assert_eq!(truncate(t, Unit::Quarter).unwrap(), ts(2024, 1, 1, 0, 0, 0));
        assert_eq!(
            truncate(ts(2024, 11, 30, 1, 2, 3), Unit::Quarter).unwrap(),
            ts(2024, 10, 1, 0, 0, 0)
        );
        assert_eq!(truncate(t, Unit::Year).unwrap(), ts(2024, 1, 1, 0, 0, 0));
    }
}
