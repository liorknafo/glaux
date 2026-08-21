//! S3 key prefix semantics: the default `YYYY/MM/DD/HH/` layout, custom
//! `Prefix` / `ErrorOutputPrefix` expressions, and object naming.
//!
//! Firehose evaluates `!{namespace:value}` expressions inside prefixes:
//!
//! - `!{timestamp:<java-format>}` — the flush time (UTC unless a custom
//!   time zone is configured) rendered with a Java `DateTimeFormatter`
//!   pattern. The pattern letters Firehose documents are implemented
//!   (`y`, `M`, `d`, `D`, `H`, `h`, `k`, `K`, `m`, `s`, `S`, `a`, `E`,
//!   `u`, `n`, quoted literals); anything else is an explicit error.
//! - `!{firehose:error-output-type}` — only in `ErrorOutputPrefix`; the
//!   failure class of the records in the object.
//! - `!{firehose:random-string}` — an 11-character lowercase alphanumeric
//!   string.
//! - `!{partitionKeyFromQuery:..}` / `!{partitionKeyFromLambda:..}` —
//!   dynamic partitioning, which glaux v0.1 rejects by name.
//!
//! A prefix without any expression gets the default timestamp layout
//! appended, exactly as the real service does (`events/` becomes
//! `events/2026/08/21/13/`); a prefix with at least one expression is used
//! verbatim. `ErrorOutputPrefix` is always used verbatim.

use chrono::{DateTime, Datelike, Timelike, Utc};

/// Why records landed under `ErrorOutputPrefix`; the value substituted for
/// `!{firehose:error-output-type}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorOutputType {
    /// Record format conversion (JSON → Parquet) failed.
    FormatConversionFailed,
    /// Lambda processing failed (never produced by glaux v0.1, listed for
    /// completeness of the expression grammar).
    ProcessingFailed,
}

impl ErrorOutputType {
    /// The value Firehose substitutes into the prefix.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FormatConversionFailed => "format-conversion-failed",
            Self::ProcessingFailed => "processing-failed",
        }
    }
}

/// A prefix expression could not be honoured. Always names the construct.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PrefixError(pub String);

/// Validate a `Prefix` (`is_error_prefix = false`) or `ErrorOutputPrefix`
/// (`true`) at configuration time, so a stream whose prefix glaux cannot
/// evaluate is rejected at `CreateDeliveryStream` rather than at first
/// flush.
pub fn validate_prefix(
    field: &str,
    prefix: &str,
    is_error_prefix: bool,
) -> Result<(), PrefixError> {
    let probe = DateTime::<Utc>::from_timestamp(0, 0).expect("epoch is representable");
    let error_type = if is_error_prefix {
        Some(ErrorOutputType::FormatConversionFailed)
    } else {
        None
    };
    evaluate(prefix, probe, error_type)
        .map(drop)
        .map_err(|PrefixError(message)| PrefixError(format!("{field}: {message}")))
}

/// Resolve the data prefix for a flush: the configured `Prefix` with its
/// expressions evaluated, or the default `YYYY/MM/DD/HH/` layout appended
/// when the prefix has no expressions (or is absent).
pub fn resolve_data_prefix(prefix: Option<&str>, at: DateTime<Utc>) -> Result<String, PrefixError> {
    let prefix = prefix.unwrap_or("");
    if prefix.contains("!{") {
        evaluate(prefix, at, None)
    } else {
        Ok(format!("{prefix}{}", default_timestamp_prefix(at)))
    }
}

/// Resolve the error prefix for a flush. With no `ErrorOutputPrefix`
/// configured, failed records go under
/// `<error-output-type>/YYYY/MM/DD/HH/` at the bucket root — Firehose's
/// behaviour when the field is omitted.
pub fn resolve_error_prefix(
    prefix: Option<&str>,
    at: DateTime<Utc>,
    error_type: ErrorOutputType,
) -> Result<String, PrefixError> {
    match prefix {
        Some(prefix) => evaluate(prefix, at, Some(error_type)),
        None => Ok(format!(
            "{}/{}",
            error_type.as_str(),
            default_timestamp_prefix(at)
        )),
    }
}

/// The default `YYYY/MM/DD/HH/` prefix.
pub fn default_timestamp_prefix(at: DateTime<Utc>) -> String {
    at.format("%Y/%m/%d/%H/").to_string()
}

/// The object name component Firehose uses:
/// `<DeliveryStreamName>-<DeliveryStreamVersion>-<yyyy-MM-dd-HH-mm-ss>-<uuid>`.
pub fn object_name(stream_name: &str, stream_version: &str, at: DateTime<Utc>) -> String {
    format!(
        "{stream_name}-{stream_version}-{}-{}",
        at.format("%Y-%m-%d-%H-%M-%S"),
        uuid::Uuid::new_v4()
    )
}

/// Substitute every `!{...}` expression in `template`.
pub fn evaluate(
    template: &str,
    at: DateTime<Utc>,
    error_type: Option<ErrorOutputType>,
) -> Result<String, PrefixError> {
    let mut out = String::with_capacity(template.len() + 16);
    let mut rest = template;
    while let Some(start) = rest.find("!{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find('}').ok_or_else(|| {
            PrefixError(format!(
                "unterminated expression {:?}: expected a closing '}}'",
                &rest[start..]
            ))
        })?;
        let expression = &after[..end];
        out.push_str(&evaluate_expression(expression, at, error_type)?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn evaluate_expression(
    expression: &str,
    at: DateTime<Utc>,
    error_type: Option<ErrorOutputType>,
) -> Result<String, PrefixError> {
    let (namespace, value) = expression.split_once(':').unwrap_or((expression, ""));
    match namespace {
        "timestamp" => format_java_timestamp(value, at)
            .map_err(|message| PrefixError(format!("!{{timestamp:{value}}}: {message}"))),
        "firehose" => match value {
            "error-output-type" => match error_type {
                Some(error_type) => Ok(error_type.as_str().to_string()),
                None => Err(PrefixError(
                    "!{firehose:error-output-type} is only valid in ErrorOutputPrefix, not in \
                     Prefix"
                        .to_string(),
                )),
            },
            "random-string" => Ok(random_string()),
            other => Err(PrefixError(format!(
                "unknown firehose namespace value !{{firehose:{other}}}: expected \
                 error-output-type or random-string"
            ))),
        },
        "partitionKeyFromQuery" | "partitionKeyFromLambda" => Err(PrefixError(format!(
            "!{{{expression}}} requires dynamic partitioning, which glaux v0.1 does not implement"
        ))),
        other => Err(PrefixError(format!(
            "unknown prefix expression namespace !{{{other}:...}}: expected timestamp, firehose, \
             partitionKeyFromQuery, or partitionKeyFromLambda"
        ))),
    }
}

/// Firehose's `!{firehose:random-string}`: 11 lowercase alphanumerics.
fn random_string() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().into_bytes();
    bytes
        .iter()
        .take(11)
        .map(|b| ALPHABET[usize::from(*b) % ALPHABET.len()] as char)
        .collect()
}

/// Render `at` with a Java `DateTimeFormatter`-style pattern (the subset
/// Firehose documents). Unknown pattern letters error by name.
pub fn format_java_timestamp(pattern: &str, at: DateTime<Utc>) -> Result<String, String> {
    if pattern.is_empty() {
        return Err("empty timestamp pattern".to_string());
    }
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            // Quoted literal; '' is an escaped quote.
            if chars.get(i + 1) == Some(&'\'') {
                out.push('\'');
                i += 2;
                continue;
            }
            let mut j = i + 1;
            let mut literal = String::new();
            loop {
                match chars.get(j) {
                    None => return Err(format!("unterminated quote in pattern {pattern:?}")),
                    Some('\'') if chars.get(j + 1) == Some(&'\'') => {
                        literal.push('\'');
                        j += 2;
                    }
                    Some('\'') => break,
                    Some(ch) => {
                        literal.push(*ch);
                        j += 1;
                    }
                }
            }
            out.push_str(&literal);
            i = j + 1;
            continue;
        }
        if !c.is_ascii_alphabetic() {
            out.push(c);
            i += 1;
            continue;
        }
        let mut count = 1;
        while chars.get(i + count) == Some(&c) {
            count += 1;
        }
        out.push_str(&format_field(c, count, at, pattern)?);
        i += count;
    }
    Ok(out)
}

fn pad(value: impl std::fmt::Display, width: usize) -> String {
    format!("{value:0>width$}")
}

fn format_field(
    letter: char,
    count: usize,
    at: DateTime<Utc>,
    pattern: &str,
) -> Result<String, String> {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    const DAYS: [&str; 7] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    Ok(match letter {
        'y' | 'u' => {
            let year = at.year();
            if count == 2 {
                pad(year.rem_euclid(100), 2)
            } else {
                pad(year, count)
            }
        }
        'M' | 'L' => {
            let month = at.month();
            match count {
                1 | 2 => pad(month, count),
                3 => MONTHS[month as usize - 1][..3].to_string(),
                _ => MONTHS[month as usize - 1].to_string(),
            }
        }
        'd' => pad(at.day(), count),
        'D' => pad(at.ordinal(), count),
        'H' => pad(at.hour(), count),
        'k' => pad(if at.hour() == 0 { 24 } else { at.hour() }, count),
        'h' => pad(at.hour12().1, count),
        'K' => pad(at.hour() % 12, count),
        'm' => pad(at.minute(), count),
        's' => pad(at.second(), count),
        'S' => {
            // Fraction of second, `count` digits (nanos has 9).
            let nanos = pad(at.nanosecond() % 1_000_000_000, 9);
            if count <= 9 {
                nanos[..count].to_string()
            } else {
                format!("{nanos}{}", "0".repeat(count - 9))
            }
        }
        'n' => at.nanosecond().to_string(),
        'a' => if at.hour12().0 { "PM" } else { "AM" }.to_string(),
        'E' => {
            let day = DAYS[at.weekday().num_days_from_monday() as usize];
            if count <= 3 {
                day[..3].to_string()
            } else {
                day.to_string()
            }
        }
        other => {
            return Err(format!(
                "unsupported pattern letter {other:?} in timestamp pattern {pattern:?}: glaux \
                 implements y u M L d D H k h K m s S n a E and quoted literals"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 21, 13, 7, 9).unwrap()
    }

    #[test]
    fn default_prefix_is_appended_when_no_expression() {
        assert_eq!(resolve_data_prefix(None, at()).unwrap(), "2026/08/21/13/");
        assert_eq!(
            resolve_data_prefix(Some("events/"), at()).unwrap(),
            "events/2026/08/21/13/"
        );
    }

    #[test]
    fn custom_expression_is_used_verbatim() {
        assert_eq!(
            resolve_data_prefix(
                Some("events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"),
                at()
            )
            .unwrap(),
            "events/year=2026/month=08/day=21/"
        );
        assert_eq!(
            evaluate("!{timestamp:yyyy-MM-dd'T'HH:mm:ss}", at(), None).unwrap(),
            "2026-08-21T13:07:09"
        );
        assert_eq!(
            evaluate(
                "!{timestamp:yy/M/d/H/D/MMM/MMMM/EEE/EEEE/a/h/K}",
                at(),
                None
            )
            .unwrap(),
            "26/8/21/13/233/Aug/August/Fri/Friday/PM/1/1"
        );
        assert_eq!(evaluate("!{timestamp:SSS}", at(), None).unwrap(), "000");
    }

    #[test]
    fn error_output_type_only_in_error_prefix() {
        assert_eq!(
            resolve_error_prefix(
                Some("errors/!{firehose:error-output-type}/!{timestamp:yyyy}/"),
                at(),
                ErrorOutputType::FormatConversionFailed
            )
            .unwrap(),
            "errors/format-conversion-failed/2026/"
        );
        assert_eq!(
            resolve_error_prefix(None, at(), ErrorOutputType::FormatConversionFailed).unwrap(),
            "format-conversion-failed/2026/08/21/13/"
        );
        let err = resolve_data_prefix(Some("x/!{firehose:error-output-type}/"), at()).unwrap_err();
        assert!(err.0.contains("only valid in ErrorOutputPrefix"), "{err}");
        // Verbatim: no timestamp appended to an error prefix.
        assert_eq!(
            resolve_error_prefix(
                Some("errors/"),
                at(),
                ErrorOutputType::FormatConversionFailed
            )
            .unwrap(),
            "errors/"
        );
    }

    #[test]
    fn unsupported_constructs_are_named() {
        let err = validate_prefix("Prefix", "a/!{partitionKeyFromQuery:x}/", false).unwrap_err();
        assert!(err.0.contains("dynamic partitioning"), "{err}");
        let err = validate_prefix("Prefix", "a/!{timestamp:yyyy", false).unwrap_err();
        assert!(err.0.contains("unterminated"), "{err}");
        let err = validate_prefix("Prefix", "a/!{timestamp:yyyyQQ}/", false).unwrap_err();
        assert!(err.0.contains("unsupported pattern letter 'Q'"), "{err}");
        let err = validate_prefix("Prefix", "a/!{bogus:x}/", false).unwrap_err();
        assert!(
            err.0.contains("unknown prefix expression namespace"),
            "{err}"
        );
        assert!(
            validate_prefix(
                "ErrorOutputPrefix",
                "e/!{firehose:error-output-type}/",
                true
            )
            .is_ok()
        );
    }

    #[test]
    fn random_string_is_eleven_alphanumerics() {
        let s = evaluate("!{firehose:random-string}", at(), None).unwrap();
        assert_eq!(s.len(), 11);
        assert!(
            s.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn object_names_follow_the_aws_layout() {
        let name = object_name("orders", "3", at());
        let parts: Vec<&str> = name.splitn(3, '-').collect();
        assert_eq!(parts[0], "orders");
        assert_eq!(parts[1], "3");
        assert!(parts[2].starts_with("2026-08-21-13-07-09-"), "{name}");
        let uuid = &parts[2]["2026-08-21-13-07-09-".len()..];
        assert!(uuid::Uuid::parse_str(uuid).is_ok(), "{uuid}");
    }
}
