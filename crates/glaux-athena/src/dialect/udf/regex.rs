//! `regexp_like` / `regexp_replace` / `regexp_extract` with Trino's
//! argument checks.
//!
//! DataFusion's regexp functions silently expand a reference to a missing
//! capture group to the empty string (`regexp_replace('abc', '(b)', '$2')`
//! gives `'ac'`; Trino raises `No group 2`) and surface invalid patterns as
//! internal errors. These UDFs compile the pattern with the `regex` crate
//! (a close superset of Java's syntax for common patterns; look-around and
//! back-references are compile errors, reported as user errors), validate
//! every group reference against the compiled pattern, and replace all
//! matches as Trino does. Patterns are translated from Java syntax first
//! ([`translate_java_pattern`]): Trino's engine reads `\d`, `\w`, `\s`,
//! and `\b` as ASCII classes and `$` as "end of text or before a final
//! newline", where Rust's defaults are Unicode-aware and strict.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use regex::Regex;

use super::casts::trino_type_name;
use super::{data_error, int64_array, string_array, type_mismatch};

/// The regexp UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoRegexp::new(RegexpOp::Like)),
        ScalarUDF::new_from_impl(TrinoRegexp::new(RegexpOp::Replace)),
        ScalarUDF::new_from_impl(TrinoRegexp::new(RegexpOp::Extract)),
    ]
}

/// Which function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegexpOp {
    Like,
    Replace,
    Extract,
}

impl RegexpOp {
    fn trino_name(self) -> &'static str {
        match self {
            Self::Like => "regexp_like",
            Self::Replace => "regexp_replace",
            Self::Extract => "regexp_extract",
        }
    }
}

fn invalid(function: &str, message: impl Into<String>) -> DataFusionError {
    data_error(
        "INVALID_FUNCTION_ARGUMENT",
        format!("{function}: {}", message.into()),
    )
}

/// Java's ASCII character classes, as Trino's regex engine (Joni in Java
/// syntax) reads them. Rust's `regex` crate makes `\d`, `\w`, `\s`, and
/// `\b` Unicode-aware, so `regexp_like('٣', '\d')` would be true and
/// `regexp_replace('José', '\W', '')` would keep the `é`.
const ASCII_DIGIT: &str = "0-9";
const ASCII_WORD: &str = "a-zA-Z0-9_";
const ASCII_SPACE: &str = " \\t\\n\\x0B\\f\\r";
/// Java's `\h` (horizontal whitespace) and `\v` (vertical whitespace)
/// classes; Rust reads `\v` as a single `U+000B`.
const HORIZONTAL_SPACE: &str =
    " \\t\\xA0\\x{1680}\\x{180e}\\x{2000}-\\x{200a}\\x{202f}\\x{205f}\\x{3000}";
const VERTICAL_SPACE: &str = "\\n\\x0B\\f\\r\\x85\\x{2028}\\x{2029}";

/// Translate a Java-syntax pattern (what Trino accepts) to the equivalent
/// Rust `regex` pattern, or refuse constructs whose meaning would differ:
///
/// - `\d \w \s \h \v` and their negations become explicit ASCII classes
///   (inside a bracket class too), `\b` / `\B` the ASCII word boundary;
/// - `$` outside a class (and `\Z`) matches at the end of the text or
///   before a final newline, as Joni's does in single-line mode; `\A`, `\z`,
///   and `^` already agree;
/// - the inline flags `u` and `U` are refused: Java's (Unicode case /
///   Unicode classes) and Rust's (Unicode mode / lazy quantifiers) differ;
///   `m`, `s`, `i`, `x` agree, and an `m` flag leaves `$` alone.
///
/// - possessive quantifiers (`a*+`, `a++`, `a?+`, `a{n,m}+`) are refused:
///   Rust's `regex` would read the trailing `+` as a second, ordinary
///   (backtracking) quantifier, so `regexp_like('aaa', 'a*+a')` would be
///   true where Java's possessive repetition never gives back and returns
///   false.
///
/// Everything else is passed through; Java syntax Rust lacks (look-around,
/// back-references, atomic groups, `\Q..\E`) fails at compile time as an
/// invalid pattern, never silently.
pub fn translate_java_pattern(pattern: &str) -> std::result::Result<String, String> {
    let multiline = has_multiline_flag(pattern);
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut chars = pattern.chars().peekable();
    // Bracket-class nesting depth (Java and Rust both allow `[a[b]]`).
    let mut depth = 0usize;
    // Just after `[` or `[^`, where `]` is a literal in Java.
    let mut class_start = false;
    // The previous character (outside a class) was a quantifier (`*`, `+`,
    // `?`, or the `}` of `{n,m}`): a `+` here is a possessive suffix.
    let mut after_quantifier = false;
    while let Some(c) = chars.next() {
        if depth == 0 {
            let was_after_quantifier = std::mem::take(&mut after_quantifier);
            match c {
                '+' if was_after_quantifier => {
                    return Err(format!(
                        "possessive quantifier (`{}+`) in pattern {pattern:?}: Java's \
                         possessive repetition never backtracks, which glaux's regex engine \
                         cannot express",
                        out.chars().last().unwrap_or('?')
                    ));
                }
                '*' | '+' | '?' | '}' if !was_after_quantifier => after_quantifier = true,
                // A lazy `*?` / `+?` / `??` is a complete quantifier too;
                // `?` after `?` would be Java's lazy-optional form.
                '?' => after_quantifier = false,
                _ => {}
            }
        }
        match c {
            '\\' => {
                let Some(escaped) = chars.next() else {
                    return Err("pattern ends with a dangling backslash".to_string());
                };
                let in_class = depth > 0;
                let class = |set: &str, negated: bool| {
                    if in_class {
                        if negated {
                            format!("[^{set}]")
                        } else {
                            set.to_string()
                        }
                    } else if negated {
                        format!("[^{set}]")
                    } else {
                        format!("[{set}]")
                    }
                };
                match escaped {
                    'd' => out.push_str(&class(ASCII_DIGIT, false)),
                    'D' => out.push_str(&class(ASCII_DIGIT, true)),
                    'w' => out.push_str(&class(ASCII_WORD, false)),
                    'W' => out.push_str(&class(ASCII_WORD, true)),
                    's' => out.push_str(&class(ASCII_SPACE, false)),
                    'S' => out.push_str(&class(ASCII_SPACE, true)),
                    'h' => out.push_str(&class(HORIZONTAL_SPACE, false)),
                    'H' => out.push_str(&class(HORIZONTAL_SPACE, true)),
                    'v' => out.push_str(&class(VERTICAL_SPACE, false)),
                    'V' => out.push_str(&class(VERTICAL_SPACE, true)),
                    'b' if !in_class => out.push_str("(?-u:\\b)"),
                    'B' if !in_class => out.push_str("(?-u:\\B)"),
                    'Z' if !in_class => out.push_str("(?:\\n?\\z)"),
                    'p' | 'P' => {
                        // `\p{Alpha}` etc. are Java's POSIX names (ASCII);
                        // Rust would read some of them as Unicode scripts or
                        // refuse them. Only the shared Unicode category names
                        // pass through unchanged.
                        out.push('\\');
                        out.push(escaped);
                        let mut name = String::new();
                        if chars.peek() == Some(&'{') {
                            for inner in chars.by_ref() {
                                name.push(inner);
                                if inner == '}' {
                                    break;
                                }
                            }
                        } else if let Some(single) = chars.next() {
                            name.push(single);
                        }
                        let body = name.trim_matches(['{', '}']);
                        let body = body.strip_prefix("Is").unwrap_or(body);
                        if !is_unicode_category(body) {
                            return Err(format!(
                                "\\{escaped}{name} is a Java-specific character class that glaux \
                                 does not translate"
                            ));
                        }
                        out.push_str(&name);
                    }
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
                class_start = false;
            }
            '[' => {
                depth += 1;
                out.push('[');
                if chars.peek() == Some(&'^') {
                    chars.next();
                    out.push('^');
                }
                class_start = true;
                continue;
            }
            ']' if depth > 0 && class_start => {
                // Java: a `]` right after `[` or `[^` is literal.
                out.push_str("\\]");
                class_start = false;
            }
            ']' if depth > 0 => {
                depth -= 1;
                out.push(']');
            }
            '$' if depth == 0 && !multiline => out.push_str("(?:\\n?\\z)"),
            '(' if depth == 0 && chars.peek() == Some(&'?') => {
                out.push('(');
                // Inline flags: `(?flags)` or `(?flags:...)`.
                let rest: String = chars.clone().collect();
                let flags_end = rest[1..]
                    .find(|ch: char| !(ch.is_ascii_alphabetic() || ch == '-'))
                    .map(|i| i + 1);
                if let Some(end) = flags_end
                    && matches!(&rest[end..end + 1], ")" | ":")
                    && end > 1
                {
                    let flags = &rest[1..end];
                    if flags.contains(['u', 'U', 'd']) {
                        return Err(format!(
                            "the inline flag group (?{flags}) uses a flag whose meaning differs \
                             between Java and glaux's regex engine (u, U, d)"
                        ));
                    }
                }
                class_start = false;
                continue;
            }
            other => {
                out.push(other);
                class_start = false;
            }
        }
        if c != '[' {
            class_start = false;
        }
    }
    Ok(out)
}

/// Whether the pattern enables the `m` (MULTILINE) flag anywhere; then `$`
/// means end-of-line in both engines and is left alone.
fn has_multiline_flag(pattern: &str) -> bool {
    let mut rest = pattern;
    while let Some(i) = rest.find("(?") {
        let after = &rest[i + 2..];
        let flags: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphabetic() || *c == '-')
            .collect();
        let enabled = flags.split('-').next().unwrap_or("");
        if enabled.contains('m') && after[flags.len()..].starts_with([')', ':']) {
            return true;
        }
        rest = after;
    }
    false
}

/// Unicode general categories, spelled the same in Java (`\p{Lu}`,
/// `\p{IsLu}`) and Rust (`\p{Lu}`). Java's POSIX names (`\p{Alpha}`),
/// binary properties (`\p{IsAlphabetic}`), scripts (`\p{IsGreek}`), and
/// blocks (`\p{InGreek}`) are refused rather than guessed.
fn is_unicode_category(name: &str) -> bool {
    matches!(
        name,
        "L" | "Lu"
            | "Ll"
            | "Lt"
            | "Lm"
            | "Lo"
            | "M"
            | "Mn"
            | "Mc"
            | "Me"
            | "N"
            | "Nd"
            | "Nl"
            | "No"
            | "P"
            | "Pc"
            | "Pd"
            | "Ps"
            | "Pe"
            | "Pi"
            | "Pf"
            | "Po"
            | "S"
            | "Sm"
            | "Sc"
            | "Sk"
            | "So"
            | "Z"
            | "Zs"
            | "Zl"
            | "Zp"
            | "C"
            | "Cc"
            | "Cf"
            | "Co"
            | "Cn"
    )
}

/// Compile `pattern` (Java syntax, translated), caching by text (patterns
/// are usually constant).
fn compile<'a>(
    function: &str,
    cache: &'a mut HashMap<String, Regex>,
    pattern: &str,
) -> Result<&'a Regex> {
    if !cache.contains_key(pattern) {
        let translated = translate_java_pattern(pattern).map_err(|e| {
            invalid(
                function,
                format!("regular expression {pattern:?} is not supported: {e}"),
            )
        })?;
        let compiled = Regex::new(&translated).map_err(|e| {
            invalid(
                function,
                format!(
                    "invalid regular expression {pattern:?}: {e} (glaux translates Java \
                     syntax to Rust regex syntax, which lacks Java's look-around and \
                     back-references)"
                ),
            )
        })?;
        cache.insert(pattern.to_string(), compiled);
    }
    Ok(&cache[pattern])
}

/// One piece of a translated replacement string.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Literal(String),
    Group(usize),
}

/// Parse a Java replacement string (`$1`, `${name}`, `\x`) against the
/// compiled pattern: numbered and named references must exist.
fn parse_replacement(regex: &Regex, replacement: &str) -> Result<Vec<Piece>> {
    let function = "regexp_replace";
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut chars = replacement.chars().peekable();
    let groups = regex.captures_len();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(escaped) => literal.push(escaped),
                None => {
                    return Err(invalid(
                        function,
                        format!("replacement {replacement:?} ends with a dangling backslash"),
                    ));
                }
            },
            '$' => {
                if !literal.is_empty() {
                    pieces.push(Piece::Literal(std::mem::take(&mut literal)));
                }
                match chars.peek() {
                    Some(d) if d.is_ascii_digit() => {
                        // Java takes the longest group number that exists.
                        let mut number: usize = 0;
                        let mut taken = false;
                        while let Some(d) = chars.peek().and_then(|d| d.to_digit(10)) {
                            let candidate = number * 10 + d as usize;
                            if taken && candidate >= groups {
                                break;
                            }
                            number = candidate;
                            taken = true;
                            chars.next();
                        }
                        if number >= groups {
                            return Err(invalid(function, format!("No group {number}")));
                        }
                        pieces.push(Piece::Group(number));
                    }
                    Some('{') => {
                        chars.next();
                        let mut name = String::new();
                        let mut closed = false;
                        for inner in chars.by_ref() {
                            if inner == '}' {
                                closed = true;
                                break;
                            }
                            name.push(inner);
                        }
                        if !closed {
                            return Err(invalid(
                                function,
                                format!(
                                    "named capturing group is missing trailing '}}' in {replacement:?}"
                                ),
                            ));
                        }
                        // Java: a group name starts with a Latin letter.
                        if !name.starts_with(|c: char| c.is_ascii_alphabetic()) {
                            return Err(invalid(
                                function,
                                format!(
                                    "capturing group name {{{name}}} does not start with a Latin letter"
                                ),
                            ));
                        }
                        let index = regex
                            .capture_names()
                            .position(|n| n == Some(name.as_str()))
                            .ok_or_else(|| {
                                invalid(function, format!("No group with name {{{name}}}"))
                            })?;
                        pieces.push(Piece::Group(index));
                    }
                    _ => {
                        return Err(invalid(
                            function,
                            format!(
                                "Illegal group reference in replacement {replacement:?} (write \\$ \
                                 for a literal dollar sign)"
                            ),
                        ));
                    }
                }
            }
            other => literal.push(other),
        }
    }
    if !literal.is_empty() {
        pieces.push(Piece::Literal(literal));
    }
    Ok(pieces)
}

fn replace_all(regex: &Regex, text: &str, pieces: &[Piece]) -> String {
    regex
        .replace_all(text, |caps: &regex::Captures| {
            let mut out = String::new();
            for piece in pieces {
                match piece {
                    Piece::Literal(s) => out.push_str(s),
                    Piece::Group(i) => {
                        if let Some(m) = caps.get(*i) {
                            out.push_str(m.as_str());
                        }
                    }
                }
            }
            out
        })
        .into_owned()
}

/// See the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoRegexp {
    signature: Signature,
    op: RegexpOp,
}

impl TrinoRegexp {
    /// New instance.
    pub fn new(op: RegexpOp) -> Self {
        let signature = match op {
            RegexpOp::Like => Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            _ => Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        };
        Self { signature, op }
    }
}

impl ScalarUDFImpl for TrinoRegexp {
    fn name(&self) -> &str {
        match self.op {
            RegexpOp::Like => "trino_regexp_like",
            RegexpOp::Replace => "trino_regexp_replace",
            RegexpOp::Extract => "trino_regexp_extract",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let string_args = match self.op {
            RegexpOp::Extract => 2,
            _ => arg_types.len(),
        };
        for t in &arg_types[..string_args] {
            if !matches!(
                t,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            ) {
                return Err(type_mismatch(format!(
                    "Unexpected parameters ({}) for function {}. Expected varchar arguments",
                    arg_types
                        .iter()
                        .map(trino_type_name)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.op.trino_name()
                )));
            }
        }
        Ok(match self.op {
            RegexpOp::Like => DataType::Boolean,
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let function = self.op.trino_name();
        let rows = args.number_rows;
        let texts = string_array(function, &args.args[0], rows)?;
        let patterns = string_array(function, &args.args[1], rows)?;
        let mut cache = HashMap::new();
        match self.op {
            RegexpOp::Like => {
                let mut out = BooleanBuilder::with_capacity(rows);
                for i in 0..rows {
                    if texts.is_null(i) || patterns.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let regex = compile(function, &mut cache, patterns.value(i))?;
                    out.append_value(regex.is_match(texts.value(i)));
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            RegexpOp::Replace => {
                let replacements = match args.args.get(2) {
                    Some(r) => Some(string_array(function, r, rows)?),
                    None => None,
                };
                let mut out = StringBuilder::new();
                for i in 0..rows {
                    let replacement_null = replacements.as_ref().is_some_and(|r| r.is_null(i));
                    if texts.is_null(i) || patterns.is_null(i) || replacement_null {
                        out.append_null();
                        continue;
                    }
                    let regex = compile(function, &mut cache, patterns.value(i))?;
                    let replacement = replacements.as_ref().map_or("", |r| r.value(i));
                    let pieces = parse_replacement(regex, replacement)?;
                    out.append_value(replace_all(regex, texts.value(i), &pieces));
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            RegexpOp::Extract => {
                let groups = match args.args.get(2) {
                    Some(g) => Some(int64_array(function, "group", g, rows)?),
                    None => None,
                };
                let mut out = StringBuilder::new();
                for i in 0..rows {
                    let group_null = groups.as_ref().is_some_and(|g| g.is_null(i));
                    if texts.is_null(i) || patterns.is_null(i) || group_null {
                        out.append_null();
                        continue;
                    }
                    let regex = compile(function, &mut cache, patterns.value(i))?;
                    let group = groups.as_ref().map_or(0, |g| g.value(i));
                    let available = regex.captures_len() as i64 - 1;
                    if group < 0 {
                        return Err(invalid(function, "Group cannot be negative"));
                    }
                    if group > available {
                        return Err(invalid(
                            function,
                            format!("Pattern has {available} groups. Cannot access group {group}"),
                        ));
                    }
                    match regex.captures(texts.value(i)) {
                        Some(caps) => {
                            out.append_option(caps.get(group as usize).map(|m| m.as_str()))
                        }
                        None => out.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn possessive_quantifiers_are_refused_and_lazy_ones_kept() {
        for pattern in [
            "a*+a", "a++a", "a?+ab", "a{1,2}+a", "[ab]++", "(ab)*+", "\\d++",
        ] {
            let err = translate_java_pattern(pattern).unwrap_err();
            assert!(err.contains("possessive quantifier"), "{pattern}: {err}");
        }
        for pattern in [
            "a*?a", "a+?", "a??b", "a{1,2}?", "\\++", "a+b+", "[+]+", "a+\\+",
        ] {
            translate_java_pattern(pattern).unwrap_or_else(|e| panic!("{pattern}: {e}"));
        }
    }
    #[test]
    fn replacement_references_are_validated_against_the_pattern() {
        let re = Regex::new("(?<x>b)(c)?").unwrap();
        assert_eq!(
            parse_replacement(&re, "$1x\\$0${x}").unwrap(),
            vec![
                Piece::Group(1),
                Piece::Literal("x$0".into()),
                Piece::Group(1)
            ]
        );
        assert_eq!(
            replace_all(&re, "abc", &parse_replacement(&re, "[$2$1]").unwrap()),
            "a[cb]"
        );
        for (replacement, needle) in [
            ("$3", "No group 3"),
            ("${y}", "No group with name {y}"),
            ("${2}", "does not start with a Latin letter"),
            ("$", "Illegal group reference"),
            ("cost: $x", "Illegal group reference"),
            ("a\\", "dangling backslash"),
        ] {
            let err = parse_replacement(&re, replacement).unwrap_err().to_string();
            assert!(err.contains(needle), "{replacement}: {err}");
        }
        // `$12` is group 1 then '2' when there is no group 12.
        assert_eq!(
            parse_replacement(&re, "$12").unwrap(),
            vec![Piece::Group(1), Piece::Literal("2".into())]
        );
        let plain = Regex::new("b").unwrap();
        let err = parse_replacement(&plain, "$1").unwrap_err().to_string();
        assert!(err.contains("No group 1"), "{err}");
    }

    #[test]
    fn java_classes_become_ascii_and_dollar_allows_a_final_newline() {
        let t = |p: &str| translate_java_pattern(p).unwrap();
        assert_eq!(t(r"\d+"), "[0-9]+");
        assert_eq!(t(r"[\d_]"), "[0-9_]");
        assert_eq!(t(r"[^\w]"), "[^a-zA-Z0-9_]");
        assert_eq!(t(r"\W"), "[^a-zA-Z0-9_]");
        assert_eq!(t(r"[\D]"), "[[^0-9]]");
        assert_eq!(t(r"\bx\b"), r"(?-u:\b)x(?-u:\b)");
        assert_eq!(t(r"b$"), r"b(?:\n?\z)");
        assert_eq!(t(r"[$]"), "[$]");
        assert_eq!(t(r"\$"), r"\$");
        assert_eq!(t(r"(?m)b$"), "(?m)b$");
        assert_eq!(t(r"(?i)ab"), "(?i)ab");
        assert_eq!(t(r"[]a]"), r"[\]a]");
        assert_eq!(t(r"\p{L}+"), r"\p{L}+");
        for bad in [r"(?u)a", r"(?U)a", r"\p{Alpha}", r"\p{IsAlphabetic}", r"a\"] {
            assert!(translate_java_pattern(bad).is_err(), "{bad}");
        }
        let re = Regex::new(&t(r"b$")).unwrap();
        assert!(re.is_match("ab\n"));
        assert!(!re.is_match("ab\n\n"));
        assert!(Regex::new(&t(r"\d")).unwrap().is_match("3"));
        assert!(!Regex::new(&t(r"\d")).unwrap().is_match("٣"));
        assert!(!Regex::new(&t(r"\w")).unwrap().is_match("é"));
        assert!(!Regex::new(&t(r"\s")).unwrap().is_match("\u{a0}"));
        assert_eq!(
            Regex::new(&t(r"\b")).unwrap().replace_all("aé b", "|"),
            "|a|é |b|"
        );
        assert_eq!(
            Regex::new(&t(r"\W")).unwrap().replace_all("José", ""),
            "Jos"
        );
    }

    #[test]
    fn invalid_patterns_are_user_errors() {
        let mut cache = HashMap::new();
        let err = compile("regexp_like", &mut cache, "[a-").unwrap_err();
        assert!(
            err.to_string().contains("invalid regular expression"),
            "{err}"
        );
        let err = compile("regexp_like", &mut cache, "(?=b)").unwrap_err();
        assert!(err.to_string().contains("look-around"), "{err}");
    }
}
