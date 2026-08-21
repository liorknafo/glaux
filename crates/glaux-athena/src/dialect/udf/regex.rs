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
//! matches as Trino does.

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

/// Compile `pattern`, caching by text (patterns are usually constant).
fn compile<'a>(
    function: &str,
    cache: &'a mut HashMap<String, Regex>,
    pattern: &str,
) -> Result<&'a Regex> {
    if !cache.contains_key(pattern) {
        let compiled = Regex::new(pattern).map_err(|e| {
            invalid(
                function,
                format!(
                    "invalid regular expression {pattern:?}: {e} (glaux uses Rust regex syntax, \
                     which lacks Java's look-around and back-references)"
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
