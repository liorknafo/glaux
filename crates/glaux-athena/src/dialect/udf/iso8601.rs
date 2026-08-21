//! Strict ISO-8601 parsing for `from_iso8601_timestamp` and
//! `from_iso8601_date`.
//!
//! DataFusion's `to_timestamp` / `to_date` accept many layouts Trino
//! rejects (`2024-01-01 10:00:00` with a space, `2024-1-1`), so these parse
//! exactly the calendar-date subset of ISO-8601 that Trino's Joda parsers
//! do: `YYYY[-MM[-DD]]`, optionally followed by `T` + `HH[:mm[:ss[.fff]]]`
//! and an offset (`Z`, `±HH:mm`, `±HHmm`, `±HH`). Anything else is an
//! error naming the input.

use std::sync::Arc;

use arrow::array::{Array, Date32Builder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, string_array};

/// The ISO-8601 UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(FromIso8601Timestamp::new()),
        ScalarUDF::new_from_impl(FromIso8601Date::new()),
    ]
}

fn invalid(function: &str, input: &str, why: &str) -> datafusion::common::DataFusionError {
    data_error(
        "INVALID_FUNCTION_ARGUMENT",
        format!("{function}: {input:?} is not an ISO-8601 value: {why}"),
    )
}

/// Exactly `width` ASCII digits.
fn digits(s: &str, width: usize) -> Option<u32> {
    if s.len() != width || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `YYYY`, `YYYY-MM`, or `YYYY-MM-DD` (missing parts default to 1, as in
/// Joda's date-element parser).
pub fn parse_iso_date(function: &str, input: &str) -> Result<NaiveDate> {
    let mut parts = input.split('-');
    let year = parts
        .next()
        .and_then(|y| digits(y, 4))
        .ok_or_else(|| invalid(function, input, "expected a 4-digit year"))?;
    let month = match parts.next() {
        Some(m) => {
            digits(m, 2).ok_or_else(|| invalid(function, input, "expected a 2-digit month"))?
        }
        None => 1,
    };
    let day = match parts.next() {
        Some(d) => {
            digits(d, 2).ok_or_else(|| invalid(function, input, "expected a 2-digit day"))?
        }
        None => 1,
    };
    if parts.next().is_some() {
        return Err(invalid(function, input, "too many date components"));
    }
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .ok_or_else(|| invalid(function, input, "not a valid calendar date"))
}

/// `HH`, `HH:mm`, `HH:mm:ss`, or `HH:mm:ss.f{1,9}` → time plus whole
/// milliseconds (fractions beyond milliseconds are truncated, matching
/// Trino's `timestamp(3)` result).
fn parse_iso_time(function: &str, input: &str, time: &str) -> Result<NaiveTime> {
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time, None),
    };
    let mut parts = hms.split(':');
    let hour = parts
        .next()
        .and_then(|h| digits(h, 2))
        .ok_or_else(|| invalid(function, input, "expected a 2-digit hour"))?;
    let minute = match parts.next() {
        Some(m) => {
            digits(m, 2).ok_or_else(|| invalid(function, input, "expected 2-digit minutes"))?
        }
        None => 0,
    };
    let second = match parts.next() {
        Some(s) => {
            digits(s, 2).ok_or_else(|| invalid(function, input, "expected 2-digit seconds"))?
        }
        None => 0,
    };
    if parts.next().is_some() {
        return Err(invalid(function, input, "too many time components"));
    }
    let millis = match fraction {
        Some(f) => {
            if f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid(function, input, "expected 1-9 fractional digits"));
            }
            let padded = format!("{f:0<3}");
            padded[..3].parse::<u32>().expect("three digits")
        }
        None => 0,
    };
    NaiveTime::from_hms_milli_opt(hour, minute, second, millis)
        .ok_or_else(|| invalid(function, input, "not a valid time of day"))
}

/// Offset in seconds east of UTC from `Z`, `±HH:mm`, `±HHmm`, or `±HH`.
fn parse_offset(function: &str, input: &str, offset: &str) -> Result<i64> {
    if offset == "Z" {
        return Ok(0);
    }
    let (sign, rest) = match offset.as_bytes().first() {
        Some(b'+') => (1, &offset[1..]),
        Some(b'-') => (-1, &offset[1..]),
        _ => return Err(invalid(function, input, "expected a zone offset")),
    };
    let (hours, minutes) = match rest.len() {
        2 => (digits(rest, 2), Some(0)),
        4 => (digits(&rest[..2], 2), digits(&rest[2..], 2)),
        5 if rest.as_bytes()[2] == b':' => (digits(&rest[..2], 2), digits(&rest[3..], 2)),
        _ => (None, None),
    };
    match (hours, minutes) {
        (Some(h), Some(m)) if h <= 18 && m < 60 => {
            Ok(sign * (i64::from(h) * 3600 + i64::from(m) * 60))
        }
        _ => Err(invalid(function, input, "malformed zone offset")),
    }
}

/// Parse a Trino ISO-8601 timestamp to milliseconds since the epoch (UTC).
pub fn parse_iso_timestamp_millis(function: &str, input: &str) -> Result<i64> {
    let (date, rest) = match input.split_once('T') {
        Some((date, rest)) => (date, Some(rest)),
        None => (input, None),
    };
    let date = parse_iso_date(function, date)?;
    let (time, offset_seconds) = match rest {
        None => (NaiveTime::MIN, 0),
        Some(rest) => {
            let zone_at = rest.find(['Z', '+', '-']);
            let (time, offset) = match zone_at {
                Some(i) => (&rest[..i], Some(&rest[i..])),
                None => (rest, None),
            };
            let time = parse_iso_time(function, input, time)?;
            let offset = match offset {
                Some(o) => parse_offset(function, input, o)?,
                None => 0,
            };
            (time, offset)
        }
    };
    let local = NaiveDateTime::new(date, time);
    Ok(local.and_utc().timestamp_millis() - offset_seconds * 1000)
}

/// `from_iso8601_timestamp(varchar)`: a UTC instant at millisecond
/// precision (the input's offset is applied, not preserved).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromIso8601Timestamp {
    signature: Signature,
}

impl Default for FromIso8601Timestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIso8601Timestamp {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FromIso8601Timestamp {
    fn name(&self) -> &str {
        "from_iso8601_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let inputs = string_array("from_iso8601_timestamp", &args.args[0], rows)?;
        let mut out = TimestampMillisecondBuilder::with_capacity(rows);
        for i in 0..rows {
            if inputs.is_null(i) {
                out.append_null();
            } else {
                out.append_value(parse_iso_timestamp_millis(
                    "from_iso8601_timestamp",
                    inputs.value(i),
                )?);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `from_iso8601_date(varchar)`: strict `YYYY-MM-DD`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromIso8601Date {
    signature: Signature,
}

impl Default for FromIso8601Date {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIso8601Date {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FromIso8601Date {
    fn name(&self) -> &str {
        "from_iso8601_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let inputs = string_array("from_iso8601_date", &args.args[0], rows)?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let mut out = Date32Builder::with_capacity(rows);
        for i in 0..rows {
            if inputs.is_null(i) {
                out.append_null();
            } else {
                let date = parse_iso_date("from_iso8601_date", inputs.value(i))?;
                out.append_value((date - epoch).num_days() as i32);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> Result<i64> {
        parse_iso_timestamp_millis("from_iso8601_timestamp", s)
    }

    #[test]
    fn accepts_the_iso_layouts_trino_accepts() {
        assert_eq!(ts("2024-01-01T10:00:00Z").unwrap(), 1_704_103_200_000);
        assert_eq!(ts("2024-01-01T10:00:00").unwrap(), 1_704_103_200_000);
        assert_eq!(ts("2024-01-01T12:00:00+02:00").unwrap(), 1_704_103_200_000);
        assert_eq!(ts("2024-01-01T08:00:00-0200").unwrap(), 1_704_103_200_000);
        assert_eq!(ts("2024-01-01T08:00-02").unwrap(), 1_704_103_200_000);
        assert_eq!(ts("2024-01-01T10:00:00.1234Z").unwrap(), 1_704_103_200_123);
        assert_eq!(ts("2024-01-01T10:00:00.5").unwrap(), 1_704_103_200_500);
        assert_eq!(ts("2024-01-01").unwrap(), 1_704_067_200_000);
        assert_eq!(ts("2024-01").unwrap(), 1_704_067_200_000);
        assert_eq!(ts("2024").unwrap(), 1_704_067_200_000);
        assert_eq!(ts("2024-01-01T10").unwrap(), 1_704_103_200_000);
    }

    #[test]
    fn rejects_what_trino_rejects() {
        for (input, why) in [
            ("2024-01-01 10:00:00", "2-digit day"),
            ("2024-1-1", "2-digit month"),
            ("garbage", "4-digit year"),
            ("2024-02-30", "valid calendar date"),
            ("2024-01-01T25:00:00", "valid time of day"),
            ("2024-01-01T10:00:00.", "fractional"),
            ("2024-01-01T10:00:00+2", "zone offset"),
            ("2024-01-01T10:00:00+99:00", "zone offset"),
            ("", "4-digit year"),
        ] {
            let err = ts(input).unwrap_err().to_string();
            assert!(err.contains(why), "{input}: {err}");
            assert!(err.contains("from_iso8601_timestamp"), "{err}");
        }
        let err = parse_iso_date("from_iso8601_date", "2024-1-1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("from_iso8601_date"), "{err}");
    }
}
