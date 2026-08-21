//! Date/time format-string translation.
//!
//! Trino has two format languages: MySQL-style specifiers (`%Y-%m-%d`) used
//! by `date_parse` / `date_format`, and Joda-Time patterns (`yyyy-MM-dd`)
//! used by `format_datetime` / `parse_datetime`. DataFusion's `to_char` /
//! `to_timestamp` speak chrono `strftime`. Both translators are total
//! functions over the specifiers they know and *refuse* everything else,
//! because a misread specifier would silently mangle every row.

use super::error::GlauxSqlError;

/// Translate a MySQL-style format (Trino `date_parse` / `date_format`) to a
/// chrono `strftime` format. `function` is used for error messages.
pub fn mysql_to_chrono(function: &str, format: &str) -> Result<String, GlauxSqlError> {
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
            'f' => "%6f",      // microseconds (Trino: fraction of second, 6 digits)
            'p' => "%p",       // AM/PM
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
pub fn joda_to_chrono(function: &str, pattern: &str) -> Result<String, GlauxSqlError> {
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
            mysql_to_chrono("date_parse", "%Y-%m-%d %H:%i:%s").unwrap(),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            mysql_to_chrono("date_format", "%d/%b/%Y %T %p 100%%").unwrap(),
            "%d/%b/%Y %H:%M:%S %p 100%%"
        );
        assert_eq!(
            mysql_to_chrono("date_parse", "%Y%m%d%H%i%s%f").unwrap(),
            "%Y%m%d%H%M%S%6f"
        );
    }

    #[test]
    fn unknown_mysql_specifiers_are_refused_by_name() {
        let err = mysql_to_chrono("date_parse", "%Y-%Q").unwrap_err();
        assert!(err.to_string().contains("%Q"), "{err}");
        assert!(err.to_string().contains("date_parse"), "{err}");
        let err = mysql_to_chrono("date_parse", "%Y-%").unwrap_err();
        assert!(err.to_string().contains("dangling"), "{err}");
    }

    #[test]
    fn joda_patterns_translate_by_run_length() {
        assert_eq!(
            joda_to_chrono("format_datetime", "yyyy-MM-dd HH:mm:ss").unwrap(),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS").unwrap(),
            "%Y-%m-%dT%H:%M:%S.%3f"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "EEE, d MMM yy h:mm a").unwrap(),
            "%a, %-d %b %y %-I:%M %p"
        );
        assert_eq!(
            joda_to_chrono("format_datetime", "'o''clock'").unwrap(),
            "o'clock"
        );
    }

    #[test]
    fn unknown_joda_letters_are_refused_by_name() {
        let err = joda_to_chrono("format_datetime", "yyyy z").unwrap_err();
        assert!(err.to_string().contains("'z'"), "{err}");
        let err = joda_to_chrono("format_datetime", "yyyy 'unterminated").unwrap_err();
        assert!(err.to_string().contains("unterminated"), "{err}");
        let err = joda_to_chrono("format_datetime", "SSSS").unwrap_err();
        assert!(err.to_string().contains("'S'"), "{err}");
    }
}
