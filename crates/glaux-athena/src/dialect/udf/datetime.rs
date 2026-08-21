//! Date/time functions whose DataFusion namesakes accept or return types
//! Trino's do not.
//!
//! - `date_trunc(unit, x)` returns `x`'s type: a `DATE` stays a `DATE`
//!   (DataFusion widens it to a timestamp), sub-day units on a `DATE` are
//!   errors, and varchar input is refused (DataFusion would parse it).
//! - `to_unixtime(timestamp)` refuses varchar input for the same reason.
//! - `trino_check_parsed_time(text, chrono_format)` guards `date_parse` /
//!   `parse_datetime`: chrono parses a leap second (`10:30:60`) that
//!   DataFusion's `to_timestamp` then rolls over to `10:31:00`, where Joda
//!   raises `Value 60 for secondOfMinute must be in the range [0,59]`. The
//!   function returns its text unchanged or raises that error.

use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use chrono::format::{Parsed, StrftimeItems};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeDelta};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{
    Unit, data_error, from_naive, string_array, to_naive, type_mismatch, unit_arg, user_err,
};
use crate::dialect::udf::casts::trino_type_name;

/// The date/time UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoDateTrunc::new()),
        ScalarUDF::new_from_impl(TrinoToUnixtime::new()),
        ScalarUDF::new_from_impl(TrinoCheckParsedTime::new()),
    ]
}

/// Whether `text` parsed with the chrono `format` carries a field value
/// Joda rejects: a second of 60 (chrono's leap second). `Ok(())` when the
/// text does not parse at all — `to_timestamp` reports that itself.
pub fn check_parsed_time(text: &str, format: &str) -> Result<()> {
    let mut parsed = Parsed::new();
    let items = StrftimeItems::new(format);
    if chrono::format::parse(&mut parsed, text, items).is_err() {
        return Ok(());
    }
    if parsed.second() == Some(60) {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            "Value 60 for secondOfMinute must be in the range [0,59]",
        ));
    }
    Ok(())
}

/// `trino_check_parsed_time(text, format)`: see the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoCheckParsedTime {
    signature: Signature,
}

impl Default for TrinoCheckParsedTime {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoCheckParsedTime {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoCheckParsedTime {
    fn name(&self) -> &str {
        "trino_check_parsed_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let texts = string_array("date_parse", &args.args[0], rows)?;
        let formats = string_array("date_parse", &args.args[1], rows)?;
        for i in 0..rows {
            if texts.is_null(i) || formats.is_null(i) {
                continue;
            }
            check_parsed_time(texts.value(i), formats.value(i))?;
        }
        Ok(args.args[0].clone())
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
        check_parsed_time("2024-01-05 10:30:59", "%Y-%m-%d %H:%M:%S").unwrap();
        check_parsed_time("garbage", "%Y-%m-%d %H:%M:%S").unwrap();
        let err = check_parsed_time("2024-01-05 10:30:60", "%Y-%m-%d %H:%M:%S").unwrap_err();
        assert!(err.to_string().contains("secondOfMinute"), "{err}");
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
