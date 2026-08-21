//! Date/time format-string translation.
//!
//! Trino has two format languages: MySQL-style specifiers (`%Y-%m-%d`) used
//! by `date_parse` / `date_format`, and Joda-Time patterns (`yyyy-MM-dd`)
//! used by `format_datetime` / `parse_datetime`. DataFusion's `to_char` /
//! `to_timestamp` speak chrono `strftime`. Both translators are total
//! functions over the specifiers they know and *refuse* everything else,
//! because a misread specifier would silently mangle every row.

use super::error::GlauxSqlError;

/// Which direction a format string is used in. Parsing and printing differ
/// for fractional seconds: Trino prints `%f` as 6 digits but accepts 1-9
/// digits when parsing, and chrono's fixed-width `%6f` would reject
/// `.123`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `date_parse` / `parse_datetime`: string → timestamp.
    Parse,
    /// `date_format` / `format_datetime`: timestamp → string.
    Format,
}

/// Translate a MySQL-style format (Trino `date_parse` / `date_format`) to a
/// chrono `strftime` format. `function` is used for error messages.
pub fn mysql_to_chrono(
    function: &str,
    format: &str,
    direction: Direction,
) -> Result<String, GlauxSqlError> {
    let mut out = String::with_capacity(format.len() + 8);
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err(GlauxSqlError::invalid_arguments(
                function,
                format!("format {format:?} ends with a dangling '%'"),
            ));
        };
        let mapped: &str = match spec {
            'Y' => "%Y",       // 4-digit year
            'y' => "%y",       // 2-digit year
            'm' => "%m",       // month 01-12
            'c' => "%-m",      // month 1-12
            'M' => "%B",       // month name
            'b' => "%b",       // abbreviated month name
            'd' => "%d",       // day 01-31
            'e' => "%-d",      // day 1-31
            'j' => "%j",       // day of year 001-366
            'H' => "%H",       // hour 00-23
            'k' => "%-H",      // hour 0-23
            'h' | 'I' => "%I", // hour 01-12
            'l' => "%-I",      // hour 1-12
            'i' => "%M",       // minutes 00-59
            's' | 'S' => "%S", // seconds 00-59
            // Fraction of a second: printed as 6 digits. When parsing,
            // Trino accepts 1-9 digits; chrono's flexible fraction parser
            // is `%.f`, which also consumes the preceding '.', so `.%f`
            // becomes `%.f`. (chrono's bare `%f` reads a nanosecond count,
            // which would make `.123` mean 123 ns.)
            'f' => match direction {
                Direction::Format => "%6f",
                Direction::Parse => {
                    if out.pop() != Some('.') {
                        return Err(GlauxSqlError::invalid_arguments(
                            function,
                            format!(
                                "format {format:?}: %f is only supported directly after a '.' \
                                 when parsing (e.g. '%s.%f')"
                            ),
                        ));
                    }
                    "%.f"
                }
            },
            'p' => "%p", // AM/PM
            'r' => "%I:%M:%S %p",
            'T' => "%H:%M:%S",
            'W' => "%A", // weekday name
            'a' => "%a", // abbreviated weekday name
            'w' => "%w", // day of week 0=Sunday
            'v' => "%V", // ISO week number
            '%' => "%%",
            other => {
                return Err(GlauxSqlError::invalid_arguments(
                    function,
                    format!(
                        "format specifier %{other} in {format:?} is not supported by glaux \
                         (supported: %Y %y %m %c %M %b %d %e %j %H %k %h %I %l %i %s %S %f %p \
                         %r %T %W %a %w %v %%)"
                    ),
                ));
            }
        };
        out.push_str(mapped);
    }
    Ok(out)
}

/// Translate a Joda-Time pattern (Trino `format_datetime` /
/// `parse_datetime`) to a chrono `strftime` format.
pub fn joda_to_chrono(
    function: &str,
    pattern: &str,
    direction: Direction,
) -> Result<String, GlauxSqlError> {
    let mut out = String::with_capacity(pattern.len() + 8);
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Quoted literal text: 'text', with '' as an escaped quote.
        if c == '\'' {
            i += 1;
            if i < chars.len() && chars[i] == '\'' {
                out.push('\'');
                i += 1;
                continue;
            }
            let mut closed = false;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        out.push('\'');
                        i += 2;
                        continue;
                    }
                    closed = true;
                    i += 1;
                    break;
                }
                push_literal(&mut out, chars[i]);
                i += 1;
            }
            if !closed {
                return Err(GlauxSqlError::invalid_arguments(
                    function,
                    format!("pattern {pattern:?} has an unterminated quoted literal"),
                ));
            }
            continue;
        }
        if !c.is_ascii_alphabetic() {
            push_literal(&mut out, c);
            i += 1;
            continue;
        }
        // Pattern letters repeat; the run length selects the form.
        let start = i;
        while i < chars.len() && chars[i] == c {
            i += 1;
        }
        let n = i - start;
        let mapped: &str = match (c, n) {
            ('y', _) | ('Y', _) if n == 2 => "%y",
            ('y', _) | ('Y', _) => "%Y",
            ('x', 2) => "%g",
            ('x', _) => "%G",
            ('M', 1) => "%-m",
            ('M', 2) => "%m",
            ('M', 3) => "%b",
            ('M', _) => "%B",
            ('d', 1) => "%-d",
            ('d', _) => "%d",
            ('D', _) => "%j",
            ('E', n) if n < 4 => "%a",
            ('E', _) => "%A",
            ('e', _) => "%u",
            ('w', _) => "%V",
            ('a', _) => "%p",
            ('H', 1) => "%-H",
            ('H', _) => "%H",
            ('h', 1) => "%-I",
            ('h', _) => "%I",
            ('m', 1) => "%-M",
            ('m', _) => "%M",
            ('s', 1) => "%-S",
            ('s', _) => "%S",
            ('S', 3) => "%3f",
            ('S', 6) => "%6f",
            ('S', 9) => "%9f",
            // Zone offset / id. glaux timestamps are UTC instants (AT TIME
            // ZONE is refused), so the offset is always zero; chrono's `%z`
            // cannot format a zone-less timestamp at all.
            ('Z', 1) if direction == Direction::Format => "+0000",
            ('Z', 2) if direction == Direction::Format => "+00:00",
            ('Z', _) if direction == Direction::Format => "UTC",
            ('Z', _) => "%z",
            ('z', _) | ('K', _) | ('k', _) | ('G', _) | ('C', _) | ('S', _) => {
                return Err(GlauxSqlError::invalid_arguments(
                    function,
                    format!(
                        "Joda pattern letter '{}' (x{n}) in {pattern:?} is not supported by glaux",
                        c
                    ),
                ));
            }
            _ => {
                return Err(GlauxSqlError::invalid_arguments(
                    function,
                    format!("Joda pattern letter '{c}' in {pattern:?} is not supported by glaux"),
                ));
            }
        };
        out.push_str(mapped);
    }
    Ok(out)
}

fn push_literal(out: &mut String, c: char) {
    if c == '%' {
        out.push_str("%%");
    } else {
        out.push(c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_formats_translate_specifier_by_specifier() {
        assert_eq!(
            mysql_to_chrono("date_parse", "%Y-%m-%d %H:%i:%s", Direction::Parse).unwrap(),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            mysql_to_chrono("date_format", "%d/%b/%Y %T %p 100%%", Direction::Format).unwrap(),
            "%d/%b/%Y %H:%M:%S %p 100%%"
        );
        assert_eq!(
            mysql_to_chrono("date_format", "%Y%m%d%H%i%s.%f", Direction::Format).unwrap(),
            "%Y%m%d%H%M%S.%6f"
        );
        // Parsing accepts 1-9 fractional digits: chrono's `%.f` takes the
        // dot and the digits.
        assert_eq!(
            mysql_to_chrono("date_parse", "%Y%m%d%H%i%s.%f", Direction::Parse).unwrap(),
            "%Y%m%d%H%M%S%.f"
        );
        let err = mysql_to_chrono("date_parse", "%Y%m%d%H%i%s%f", Direction::Parse).unwrap_err();
        assert!(err.to_string().contains("after a '.'"), "{err}");
    }

    #[test]
    fn unknown_mysql_specifiers_are_refused_by_name() {
        let err = mysql_to_chrono("date_parse", "%Y-%Q", Direction::Parse).unwrap_err();
        assert!(err.to_string().contains("%Q"), "{err}");
        assert!(err.to_string().contains("date_parse"), "{err}");
        let err = mysql_to_chrono("date_parse", "%Y-%", Direction::Parse).unwrap_err();
        assert!(err.to_string().contains("dangling"), "{err}");
    }

    #[test]
    fn joda_patterns_translate_by_run_length() {
        assert_eq!(
            joda_to_chrono("format_datetime", "yyyy-MM-dd HH:mm:ss", Direction::Format).unwrap(),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            joda_to_chrono(
                "format_datetime",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                Direction::Format
            )
            .unwrap(),
            "%Y-%m-%dT%H:%M:%S.%3f"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "EEE, d MMM yy h:mm a", Direction::Format).unwrap(),
            "%a, %-d %b %y %-I:%M %p"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "'o''clock'", Direction::Format).unwrap(),
            "o'clock"
        );
        // Zone offsets print as UTC when formatting (timestamps are UTC
        // instants) and parse as offsets.
        assert_eq!(
            joda_to_chrono("format_datetime", "HH:mm Z", Direction::Format).unwrap(),
            "%H:%M +0000"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "HH:mm ZZ", Direction::Format).unwrap(),
            "%H:%M +00:00"
        );
        assert_eq!(
            joda_to_chrono("parse_datetime", "HH:mm Z", Direction::Parse).unwrap(),
            "%H:%M %z"
        );
    }

    #[test]
    fn unknown_joda_letters_are_refused_by_name() {
        let err = joda_to_chrono("format_datetime", "yyyy z", Direction::Format).unwrap_err();
        assert!(err.to_string().contains("'z'"), "{err}");
        let err =
            joda_to_chrono("format_datetime", "yyyy 'unterminated", Direction::Format).unwrap_err();
        assert!(err.to_string().contains("unterminated"), "{err}");
        let err = joda_to_chrono("format_datetime", "SSSS", Direction::Format).unwrap_err();
        assert!(err.to_string().contains("'S'"), "{err}");
    }
}
