//! Athena-style **partition projection**: partitions computed locally from
//! Glue table parameters, with zero `GetPartitions` calls.
//!
//! A table opts in with `projection.enabled = true` and per-column
//! configuration (`projection.<col>.type`, `.range`, `.values`, `.format`,
//! `.interval`, `.interval.unit`, `.digits`) plus an optional
//! `storage.location.template`. glaux supports the `enum`, `integer`, and
//! `date` projection types; anything else (including `injected`) errors
//! explicitly naming the property.

use chrono::format::{Parsed, StrftimeItems, parse};
use chrono::{Days, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Utc};
use std::collections::HashMap;

use crate::error::{CatalogError, Result};
use crate::glue::GlueTable;

/// Hard cap on locally computed partitions, so a misconfigured range cannot
/// enumerate forever. Exceeding it is an explicit error.
const MAX_PROJECTED_PARTITIONS: usize = 100_000;

/// Parsed partition-projection configuration for one table.
#[derive(Debug, Clone)]
pub(crate) struct ProjectionConfig {
    database: String,
    table: String,
    /// Per partition column, in partition-key order.
    columns: Vec<(String, ColumnProjection)>,
    /// Optional `storage.location.template` with `${col}` placeholders.
    location_template: Option<String>,
}

#[derive(Debug, Clone)]
enum ColumnProjection {
    Enum {
        values: Vec<String>,
    },
    Integer {
        min: i64,
        max: i64,
        interval: i64,
        digits: usize,
    },
    Date {
        start: DateBound,
        end: DateBound,
        /// chrono format string (converted from the Java pattern).
        format: String,
        /// Whether the format carries time-of-day tokens.
        has_time: bool,
        interval: i64,
        unit: DateUnit,
    },
}

/// One end of a date projection range: a literal date or a `NOW`-relative
/// offset resolved at query time.
#[derive(Debug, Clone, Copy)]
enum DateBound {
    Literal(NaiveDateTime),
    /// `NOW`, `NOW-3DAYS`, `NOW+2HOURS`, ... (signed amount + unit).
    NowOffset(i64, DateUnit),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateUnit {
    Years,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    Seconds,
}

impl DateUnit {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().trim_end_matches('S') {
            "YEAR" => Some(Self::Years),
            "MONTH" => Some(Self::Months),
            "WEEK" => Some(Self::Weeks),
            "DAY" => Some(Self::Days),
            "HOUR" => Some(Self::Hours),
            "MINUTE" => Some(Self::Minutes),
            "SECOND" => Some(Self::Seconds),
            _ => None,
        }
    }

    /// Add `amount` of this unit to a timestamp; `None` on overflow.
    fn add_to(self, t: NaiveDateTime, amount: i64) -> Option<NaiveDateTime> {
        match self {
            Self::Years => Self::Months.add_to(t, amount.checked_mul(12)?),
            Self::Months => {
                let months = u32::try_from(amount.unsigned_abs()).ok()?;
                if amount >= 0 {
                    t.checked_add_months(Months::new(months))
                } else {
                    t.checked_sub_months(Months::new(months))
                }
            }
            Self::Weeks => Self::Days.add_to(t, amount.checked_mul(7)?),
            Self::Days => {
                let days = amount.unsigned_abs();
                if amount >= 0 {
                    t.checked_add_days(Days::new(days))
                } else {
                    t.checked_sub_days(Days::new(days))
                }
            }
            Self::Hours => t.checked_add_signed(TimeDelta::hours(amount)),
            Self::Minutes => t.checked_add_signed(TimeDelta::minutes(amount)),
            Self::Seconds => t.checked_add_signed(TimeDelta::seconds(amount)),
        }
    }
}

/// A locally computed partition: its column values (in partition-key order)
/// and its storage location URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedPartition {
    pub values: Vec<String>,
    pub location: String,
}

impl ProjectionConfig {
    /// Returns `Some` if the table has `projection.enabled = true`.
    /// Parsing errors (missing or unsupported properties) are reported
    /// eagerly so a broken projection table fails at resolution, not with
    /// wrong results.
    pub fn from_table(database: &str, table: &GlueTable) -> Option<Result<Self>> {
        let enabled = table
            .parameters
            .get("projection.enabled")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if !enabled {
            return None;
        }
        Some(Self::parse(database, table))
    }

    fn parse(database: &str, table: &GlueTable) -> Result<Self> {
        let err = |message: String| CatalogError::PartitionProjection {
            database: database.to_string(),
            table: table.name.clone(),
            message,
        };
        if table.partition_keys.is_empty() {
            return Err(err(
                "projection.enabled is true but the table has no partition keys".to_string(),
            ));
        }
        let params = &table.parameters;
        let mut columns = Vec::with_capacity(table.partition_keys.len());
        for key in &table.partition_keys {
            let col = &key.name;
            let projection = parse_column(params, col).map_err(&err)?;
            columns.push((col.clone(), projection));
        }
        let location_template = params.get("storage.location.template").cloned();
        if let Some(template) = &location_template {
            for (col, _) in &columns {
                if !template.contains(&format!("${{{col}}}")) {
                    return Err(err(format!(
                        "storage.location.template {template:?} is missing the \
                         ${{{col}}} placeholder for partition column {col}"
                    )));
                }
            }
        }
        Ok(Self {
            database: database.to_string(),
            table: table.name.clone(),
            columns,
            location_template,
        })
    }

    /// Enumerate every projected partition. `table_location` is the base
    /// for the default Hive-style layout when no
    /// `storage.location.template` is configured.
    pub fn enumerate(&self, table_location: &str) -> Result<Vec<ProjectedPartition>> {
        let err = |message: String| CatalogError::PartitionProjection {
            database: self.database.clone(),
            table: self.table.clone(),
            message,
        };
        let now = Utc::now().naive_utc();
        let mut per_column: Vec<Vec<String>> = Vec::with_capacity(self.columns.len());
        let mut total: usize = 1;
        for (col, projection) in &self.columns {
            let values = enumerate_column(col, projection, now).map_err(&err)?;
            total = total.saturating_mul(values.len());
            if total > MAX_PROJECTED_PARTITIONS {
                return Err(err(format!(
                    "projection would compute more than {MAX_PROJECTED_PARTITIONS} partitions"
                )));
            }
            per_column.push(values);
        }

        // Cartesian product over the per-column value lists.
        let mut partitions = vec![Vec::new()];
        for values in &per_column {
            let mut next = Vec::with_capacity(partitions.len() * values.len());
            for prefix in &partitions {
                for value in values {
                    let mut row: Vec<String> = prefix.clone();
                    row.push(value.clone());
                    next.push(row);
                }
            }
            partitions = next;
        }

        let base = table_location.trim_end_matches('/');
        Ok(partitions
            .into_iter()
            .map(|values| {
                let location = match &self.location_template {
                    Some(template) => {
                        let mut location = template.clone();
                        for ((col, _), value) in self.columns.iter().zip(&values) {
                            location = location.replace(&format!("${{{col}}}"), value);
                        }
                        location
                    }
                    None => {
                        let mut location = base.to_string();
                        for ((col, _), value) in self.columns.iter().zip(&values) {
                            location.push('/');
                            location.push_str(col);
                            location.push('=');
                            location.push_str(value);
                        }
                        location
                    }
                };
                ProjectedPartition { values, location }
            })
            .collect())
    }
}

/// Internal parse/enumerate errors are plain messages; callers wrap them
/// with database/table context.
type ProjResult<T> = std::result::Result<T, String>;

fn parse_column(params: &HashMap<String, String>, col: &str) -> ProjResult<ColumnProjection> {
    let get = |suffix: &str| params.get(&format!("projection.{col}.{suffix}"));
    let projection_type = get("type")
        .ok_or_else(|| format!("missing projection.{col}.type for partition column {col}"))?;
    match projection_type.to_ascii_lowercase().as_str() {
        "enum" => {
            let values = get("values")
                .ok_or_else(|| format!("enum projection requires projection.{col}.values"))?;
            let values: Vec<String> = values
                .split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect();
            if values.is_empty() {
                return Err(format!("projection.{col}.values contains no values"));
            }
            Ok(ColumnProjection::Enum { values })
        }
        "integer" => {
            let range = get("range")
                .ok_or_else(|| format!("integer projection requires projection.{col}.range"))?;
            let (min, max) = split_range(col, range)?;
            let min: i64 = min
                .trim()
                .parse()
                .map_err(|_| format!("projection.{col}.range minimum {min:?} is not an integer"))?;
            let max: i64 = max
                .trim()
                .parse()
                .map_err(|_| format!("projection.{col}.range maximum {max:?} is not an integer"))?;
            if min > max {
                return Err(format!(
                    "projection.{col}.range minimum {min} exceeds maximum {max}"
                ));
            }
            let interval = parse_interval(get("interval"), col)?;
            let digits = match get("digits") {
                Some(d) => d
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| format!("projection.{col}.digits {d:?} is not an integer"))?,
                None => 0,
            };
            Ok(ColumnProjection::Integer {
                min,
                max,
                interval,
                digits,
            })
        }
        "date" => {
            let range = get("range")
                .ok_or_else(|| format!("date projection requires projection.{col}.range"))?;
            let java_format = get("format").map(String::as_str).unwrap_or("yyyy-MM-dd");
            let (format, has_time) = java_date_format_to_chrono(java_format)
                .map_err(|m| format!("projection.{col}.format: {m}"))?;
            let (start, end) = split_range(col, range)?;
            let start = parse_date_bound(col, start.trim(), &format, has_time)?;
            let end = parse_date_bound(col, end.trim(), &format, has_time)?;
            let interval = parse_interval(get("interval"), col)?;
            let unit = match get("interval.unit") {
                Some(u) => DateUnit::parse(u).ok_or_else(|| {
                    format!("projection.{col}.interval.unit {u:?} is not a supported unit")
                })?,
                None => {
                    if has_time {
                        // Athena requires an explicit unit for sub-day formats.
                        return Err(format!(
                            "projection.{col}.interval.unit is required when the date \
                             format {java_format:?} includes time-of-day"
                        ));
                    }
                    DateUnit::Days
                }
            };
            Ok(ColumnProjection::Date {
                start,
                end,
                format,
                has_time,
                interval,
                unit,
            })
        }
        "injected" => Err(format!(
            "projection.{col}.type = injected is not supported in glaux v0.1"
        )),
        other => Err(format!(
            "projection.{col}.type = {other:?} is not a supported projection type \
             (supported: enum, integer, date)"
        )),
    }
}

fn split_range<'a>(col: &str, range: &'a str) -> ProjResult<(&'a str, &'a str)> {
    range
        .split_once(',')
        .ok_or_else(|| format!("projection.{col}.range {range:?} must be \"<start>,<end>\""))
}

fn parse_interval(value: Option<&String>, col: &str) -> ProjResult<i64> {
    match value {
        Some(i) => {
            let interval = i
                .trim()
                .parse::<i64>()
                .map_err(|_| format!("projection.{col}.interval {i:?} is not an integer"))?;
            if interval < 1 {
                return Err(format!("projection.{col}.interval must be at least 1"));
            }
            Ok(interval)
        }
        None => Ok(1),
    }
}

fn parse_date_bound(
    col: &str,
    value: &str,
    chrono_format: &str,
    has_time: bool,
) -> ProjResult<DateBound> {
    let upper = value.to_ascii_uppercase();
    if let Some(rest) = upper.strip_prefix("NOW") {
        let rest = rest.trim();
        if rest.is_empty() {
            return Ok(DateBound::NowOffset(0, DateUnit::Days));
        }
        let (sign, rest) = match rest.as_bytes()[0] {
            b'+' => (1i64, rest[1..].trim()),
            b'-' => (-1i64, rest[1..].trim()),
            _ => {
                return Err(format!(
                    "projection.{col}.range bound {value:?}: expected NOW, NOW+<n><unit>, \
                     or NOW-<n><unit>"
                ));
            }
        };
        let digits_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let amount: i64 = rest[..digits_end].parse().map_err(|_| {
            format!("projection.{col}.range bound {value:?}: missing offset amount after NOW")
        })?;
        let unit = DateUnit::parse(rest[digits_end..].trim()).ok_or_else(|| {
            format!(
                "projection.{col}.range bound {value:?}: {:?} is not a supported unit",
                &rest[digits_end..]
            )
        })?;
        return Ok(DateBound::NowOffset(sign * amount, unit));
    }
    parse_datetime(value, chrono_format, has_time)
        .map(DateBound::Literal)
        .map_err(|m| format!("projection.{col}.range bound {value:?}: {m}"))
}

fn parse_datetime(value: &str, chrono_format: &str, has_time: bool) -> ProjResult<NaiveDateTime> {
    let mismatch = |e: chrono::ParseError| format!("does not match format {chrono_format:?}: {e}");
    if !has_time {
        return NaiveDate::parse_from_str(value, chrono_format)
            .map(|d| d.and_time(NaiveTime::MIN))
            .map_err(mismatch);
    }
    // Time-of-day formats may carry only some components (`yyyy/MM/dd/HH`
    // is the common hourly layout); chrono's `parse_from_str` insists on
    // minutes, so parse into `Parsed` and default the missing fields to 0,
    // exactly as Java's SimpleDateFormat does.
    let mut parsed = Parsed::new();
    parse(&mut parsed, value, StrftimeItems::new(chrono_format)).map_err(mismatch)?;
    if parsed.hour_div_12().is_none() && parsed.hour_mod_12().is_none() {
        parsed.set_hour(0).map_err(mismatch)?;
    }
    if parsed.minute().is_none() {
        parsed.set_minute(0).map_err(mismatch)?;
    }
    if parsed.second().is_none() {
        parsed.set_second(0).map_err(mismatch)?;
    }
    parsed.to_naive_datetime_with_offset(0).map_err(mismatch)
}

fn enumerate_column(
    col: &str,
    projection: &ColumnProjection,
    now: NaiveDateTime,
) -> ProjResult<Vec<String>> {
    match projection {
        ColumnProjection::Enum { values } => Ok(values.clone()),
        ColumnProjection::Integer {
            min,
            max,
            interval,
            digits,
        } => {
            let mut values = Vec::new();
            let mut current = *min;
            while current <= *max {
                if values.len() >= MAX_PROJECTED_PARTITIONS {
                    return Err(format!(
                        "projection.{col} enumerates more than {MAX_PROJECTED_PARTITIONS} values"
                    ));
                }
                values.push(if *digits > 0 {
                    format!("{current:0width$}", width = digits)
                } else {
                    current.to_string()
                });
                current = match current.checked_add(*interval) {
                    Some(next) => next,
                    None => break,
                };
            }
            Ok(values)
        }
        ColumnProjection::Date {
            start,
            end,
            format,
            has_time,
            interval,
            unit,
        } => {
            let resolve = |bound: &DateBound| -> ProjResult<NaiveDateTime> {
                match bound {
                    DateBound::Literal(t) => Ok(*t),
                    DateBound::NowOffset(amount, offset_unit) => {
                        let resolved = offset_unit
                            .add_to(now, *amount)
                            .ok_or_else(|| format!("projection.{col}: NOW offset overflowed"))?;
                        if *has_time {
                            Ok(resolved)
                        } else {
                            // Date-only formats truncate NOW to the day.
                            Ok(resolved.date().and_time(NaiveTime::MIN))
                        }
                    }
                }
            };
            let start = resolve(start)?;
            let end = resolve(end)?;
            if start > end {
                return Err(format!(
                    "projection.{col}.range start {start} is after end {end}"
                ));
            }
            let mut values = Vec::new();
            let mut current = start;
            while current <= end {
                if values.len() >= MAX_PROJECTED_PARTITIONS {
                    return Err(format!(
                        "projection.{col} enumerates more than {MAX_PROJECTED_PARTITIONS} values"
                    ));
                }
                values.push(if *has_time {
                    current.format(format).to_string()
                } else {
                    current.date().format(format).to_string()
                });
                current = unit.add_to(current, *interval).ok_or_else(|| {
                    format!("projection.{col}: date range enumeration overflowed")
                })?;
            }
            Ok(values)
        }
    }
}

/// Convert a Java `SimpleDateFormat` pattern (what Athena projection uses)
/// to a chrono format string. Returns `(chrono_format, has_time_tokens)`.
///
/// Supported tokens: `yyyy`, `yy`, `MM`, `dd`, `HH`, `mm`, `ss`; literal
/// punctuation and spaces pass through. Anything else errors naming the
/// token.
fn java_date_format_to_chrono(java: &str) -> ProjResult<(String, bool)> {
    let mut out = String::new();
    let mut has_time = false;
    let bytes: Vec<char> = java.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let run_len = bytes[i..].iter().take_while(|&&x| x == c).count();
        match (c, run_len) {
            ('y', 4) => out.push_str("%Y"),
            ('y', 2) => out.push_str("%y"),
            ('M', 2) => out.push_str("%m"),
            ('d', 2) => out.push_str("%d"),
            ('H', 2) => {
                out.push_str("%H");
                has_time = true;
            }
            ('m', 2) => {
                out.push_str("%M");
                has_time = true;
            }
            ('s', 2) => {
                out.push_str("%S");
                has_time = true;
            }
            (c, _) if c.is_ascii_alphanumeric() => {
                return Err(format!(
                    "unsupported date format token {:?} in {java:?}",
                    c.to_string().repeat(run_len)
                ));
            }
            ('%', _) => return Err(format!("literal '%' is not supported in {java:?}")),
            (c, _) => {
                for _ in 0..run_len {
                    out.push(c);
                }
            }
        }
        i += run_len;
    }
    Ok((out, has_time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glue::GlueColumn;

    fn projected_table(params: &[(&str, &str)], partition_keys: &[&str]) -> GlueTable {
        let mut parameters: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        parameters.insert("projection.enabled".to_string(), "true".to_string());
        GlueTable {
            name: "t".to_string(),
            database_name: Some("db".to_string()),
            table_type: Some("EXTERNAL_TABLE".to_string()),
            storage_descriptor: None,
            partition_keys: partition_keys
                .iter()
                .map(|name| GlueColumn {
                    name: name.to_string(),
                    column_type: Some("string".to_string()),
                    comment: None,
                })
                .collect(),
            parameters,
        }
    }

    fn config(params: &[(&str, &str)], partition_keys: &[&str]) -> ProjectionConfig {
        ProjectionConfig::from_table("db", &projected_table(params, partition_keys))
            .expect("projection should be enabled")
            .expect("projection config should parse")
    }

    #[test]
    fn disabled_when_parameter_absent() {
        let mut table = projected_table(&[], &["day"]);
        table.parameters.remove("projection.enabled");
        assert!(ProjectionConfig::from_table("db", &table).is_none());
    }

    #[test]
    fn date_range_enumerates_days() {
        let config = config(
            &[
                ("projection.day.type", "date"),
                ("projection.day.range", "2024-01-30,2024-02-02"),
                ("projection.day.format", "yyyy-MM-dd"),
            ],
            &["day"],
        );
        let partitions = config.enumerate("s3://bucket/tbl/").unwrap();
        assert_eq!(
            partitions,
            vec![
                ProjectedPartition {
                    values: vec!["2024-01-30".into()],
                    location: "s3://bucket/tbl/day=2024-01-30".into(),
                },
                ProjectedPartition {
                    values: vec!["2024-01-31".into()],
                    location: "s3://bucket/tbl/day=2024-01-31".into(),
                },
                ProjectedPartition {
                    values: vec!["2024-02-01".into()],
                    location: "s3://bucket/tbl/day=2024-02-01".into(),
                },
                ProjectedPartition {
                    values: vec!["2024-02-02".into()],
                    location: "s3://bucket/tbl/day=2024-02-02".into(),
                },
            ]
        );
    }

    #[test]
    fn hourly_projection_with_custom_format() {
        let config = config(
            &[
                ("projection.hour.type", "date"),
                ("projection.hour.range", "2024/06/01/00,2024/06/01/03"),
                ("projection.hour.format", "yyyy/MM/dd/HH"),
                ("projection.hour.interval", "1"),
                ("projection.hour.interval.unit", "HOURS"),
            ],
            &["hour"],
        );
        let partitions = config.enumerate("s3://b/t").unwrap();
        let values: Vec<&str> = partitions.iter().map(|p| p.values[0].as_str()).collect();
        assert_eq!(
            values,
            vec![
                "2024/06/01/00",
                "2024/06/01/01",
                "2024/06/01/02",
                "2024/06/01/03"
            ]
        );
    }

    #[test]
    fn now_relative_range_resolves_at_enumeration() {
        let config = config(
            &[
                ("projection.day.type", "date"),
                ("projection.day.range", "NOW-2DAYS,NOW"),
            ],
            &["day"],
        );
        let partitions = config.enumerate("s3://b/t").unwrap();
        assert_eq!(partitions.len(), 3);
        let today = Utc::now().naive_utc().date();
        assert_eq!(
            partitions[2].values[0],
            today.format("%Y-%m-%d").to_string()
        );
    }

    #[test]
    fn integer_projection_with_digits_and_interval() {
        let config = config(
            &[
                ("projection.shard.type", "integer"),
                ("projection.shard.range", "0,10"),
                ("projection.shard.interval", "5"),
                ("projection.shard.digits", "3"),
            ],
            &["shard"],
        );
        let partitions = config.enumerate("s3://b/t").unwrap();
        let values: Vec<&str> = partitions.iter().map(|p| p.values[0].as_str()).collect();
        assert_eq!(values, vec!["000", "005", "010"]);
    }

    #[test]
    fn enum_and_template_location() {
        let config = config(
            &[
                ("projection.region.type", "enum"),
                ("projection.region.values", "us-east-1, eu-west-1"),
                ("storage.location.template", "s3://bucket/data/${region}/v1"),
            ],
            &["region"],
        );
        let partitions = config.enumerate("s3://bucket/ignored").unwrap();
        assert_eq!(
            partitions,
            vec![
                ProjectedPartition {
                    values: vec!["us-east-1".into()],
                    location: "s3://bucket/data/us-east-1/v1".into(),
                },
                ProjectedPartition {
                    values: vec!["eu-west-1".into()],
                    location: "s3://bucket/data/eu-west-1/v1".into(),
                },
            ]
        );
    }

    #[test]
    fn multi_column_cartesian_product() {
        let config = config(
            &[
                ("projection.day.type", "date"),
                ("projection.day.range", "2024-01-01,2024-01-02"),
                ("projection.kind.type", "enum"),
                ("projection.kind.values", "a,b"),
            ],
            &["day", "kind"],
        );
        let partitions = config.enumerate("s3://b/t").unwrap();
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].values, vec!["2024-01-01", "a"]);
        assert_eq!(partitions[0].location, "s3://b/t/day=2024-01-01/kind=a");
        assert_eq!(partitions[3].values, vec!["2024-01-02", "b"]);
    }

    #[test]
    fn injected_projection_errors_explicitly() {
        let table = projected_table(&[("projection.user.type", "injected")], &["user"]);
        let err = ProjectionConfig::from_table("db", &table)
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("injected"), "{err}");
    }

    #[test]
    fn missing_type_errors_naming_the_column() {
        let table = projected_table(&[], &["day"]);
        let err = ProjectionConfig::from_table("db", &table)
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("projection.day.type"), "{err}");
    }

    #[test]
    fn unsupported_format_token_errors() {
        let table = projected_table(
            &[
                ("projection.day.type", "date"),
                ("projection.day.range", "2024-W01,2024-W02"),
                ("projection.day.format", "yyyy-'W'ww"),
            ],
            &["day"],
        );
        let err = ProjectionConfig::from_table("db", &table)
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("date format token"), "{err}");
    }

    #[test]
    fn oversized_range_errors() {
        let table = projected_table(
            &[
                ("projection.n.type", "integer"),
                ("projection.n.range", "0,10000000"),
            ],
            &["n"],
        );
        let config = ProjectionConfig::from_table("db", &table).unwrap().unwrap();
        let err = config.enumerate("s3://b/t").unwrap_err();
        assert!(err.to_string().contains("100000"), "{err}");
    }

    #[test]
    fn template_missing_placeholder_errors() {
        let table = projected_table(
            &[
                ("projection.day.type", "date"),
                ("projection.day.range", "2024-01-01,2024-01-02"),
                ("storage.location.template", "s3://bucket/data/static"),
            ],
            &["day"],
        );
        let err = ProjectionConfig::from_table("db", &table)
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("${day}"), "{err}");
    }
}
