//! String functions whose DataFusion namesakes differ from Trino at the
//! edges: `substr` for start ≤ 0, `split_part` past the last field,
//! `split` of an empty string or by an empty delimiter, and the
//! replacement-string syntax of `regexp_replace`.

use std::sync::Arc;

use arrow::array::{Array, ListBuilder, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{data_error, int64_array, string_array};

/// The string UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoSubstr::new()),
        ScalarUDF::new_from_impl(TrinoSplitPart::new()),
        ScalarUDF::new_from_impl(TrinoSplit::new()),
        ScalarUDF::new_from_impl(TrinoRegexpReplacement::new()),
    ]
}

/// Trino's `split(string, delimiter)`: `split('', ',')` is `['']` and an
/// empty delimiter splits into single characters (DataFusion's
/// `string_to_array` gives `[]` and `['abc']` respectively).
pub fn trino_split(s: &str, delimiter: &str) -> Vec<String> {
    if delimiter.is_empty() {
        return s.chars().map(String::from).collect();
    }
    s.split(delimiter).map(String::from).collect()
}

/// `trino_split(string, delimiter)`: see [`trino_split`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoSplit {
    signature: Signature,
}

impl Default for TrinoSplit {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoSplit {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoSplit {
    fn name(&self) -> &str {
        "trino_split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let strings = string_array("split", &args.args[0], rows)?;
        let delimiters = string_array("split", &args.args[1], rows)?;
        let mut out = ListBuilder::new(StringBuilder::new());
        for i in 0..rows {
            if strings.is_null(i) || delimiters.is_null(i) {
                out.append(false);
                continue;
            }
            for part in trino_split(strings.value(i), delimiters.value(i)) {
                out.values().append_value(part);
            }
            out.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino's `substr(string, start[, length])` on code points.
///
/// Positions are 1-based; a negative `start` counts from the end; `start =
/// 0`, a non-positive `length`, or a start beyond either end yields `''`
/// (DataFusion follows PostgreSQL instead, where `substr('hello', -3)` is
/// `'hello'`).
pub fn trino_substr(s: &str, start: i64, length: Option<i64>) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    if start == 0 || len == 0 || length.is_some_and(|l| l <= 0) {
        return String::new();
    }
    let index_start = if start > 0 { start - 1 } else { len + start };
    if index_start < 0 || index_start >= len {
        return String::new();
    }
    let index_end = match length {
        Some(l) => (index_start.saturating_add(l)).min(len),
        None => len,
    };
    chars[index_start as usize..index_end as usize]
        .iter()
        .collect()
}

/// `trino_substr(string, start[, length])`: see [`trino_substr`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoSubstr {
    signature: Signature,
}

impl Default for TrinoSubstr {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoSubstr {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TrinoSubstr {
    fn name(&self) -> &str {
        "trino_substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let strings = string_array("substr", &args.args[0], rows)?;
        let starts = int64_array("substr", "start", &args.args[1], rows)?;
        let lengths = match args.args.get(2) {
            Some(l) => Some(int64_array("substr", "length", l, rows)?),
            None => None,
        };
        let mut out = StringBuilder::new();
        for i in 0..rows {
            let length_null = lengths.as_ref().is_some_and(|l| l.is_null(i));
            if strings.is_null(i) || starts.is_null(i) || length_null {
                out.append_null();
                continue;
            }
            let length = lengths.as_ref().map(|l| l.value(i));
            out.append_value(trino_substr(strings.value(i), starts.value(i), length));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Trino's `split_part(string, delimiter, index)`: `NULL` when `index`
/// exceeds the number of fields (DataFusion returns `''`), an error for
/// `index < 1`, and an empty delimiter splits into single characters.
pub fn trino_split_part(s: &str, delimiter: &str, index: i64) -> Result<Option<String>> {
    if index < 1 {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            format!("split_part: index must be greater than zero, got {index}"),
        ));
    }
    let index = index as usize;
    if delimiter.is_empty() {
        return Ok(s.chars().nth(index - 1).map(String::from));
    }
    Ok(s.split(delimiter).nth(index - 1).map(str::to_string))
}

/// `trino_split_part(string, delimiter, index)`: see [`trino_split_part`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoSplitPart {
    signature: Signature,
}

impl Default for TrinoSplitPart {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoSplitPart {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoSplitPart {
    fn name(&self) -> &str {
        "trino_split_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let strings = string_array("split_part", &args.args[0], rows)?;
        let delimiters = string_array("split_part", &args.args[1], rows)?;
        let indexes = int64_array("split_part", "index", &args.args[2], rows)?;
        let mut out = StringBuilder::new();
        for i in 0..rows {
            if strings.is_null(i) || delimiters.is_null(i) || indexes.is_null(i) {
                out.append_null();
                continue;
            }
            out.append_option(trino_split_part(
                strings.value(i),
                delimiters.value(i),
                indexes.value(i),
            )?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Translate a Java/Trino `regexp_replace` replacement string to the Rust
/// `regex` crate's syntax.
///
/// Java: `$1` / `$12` is a numbered group, `${name}` a named group, `\x` a
/// literal `x`. Rust: `$1x` would be the group *named* `1x`, and a literal
/// `$` is `$$`. So numbered references become `${n}`, backslash escapes
/// become literals, and a `$` that is not a group reference (which Java
/// rejects with "Illegal group reference") is an error.
pub fn java_replacement_to_rust(replacement: &str) -> Result<String> {
    let mut out = String::with_capacity(replacement.len() + 4);
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('$') => out.push_str("$$"),
                Some(escaped) => out.push(escaped),
                None => {
                    return Err(data_error(
                        "INVALID_FUNCTION_ARGUMENT",
                        format!(
                            "regexp_replace: replacement {replacement:?} ends with a dangling \
                             backslash"
                        ),
                    ));
                }
            },
            '$' => match chars.peek() {
                Some(d) if d.is_ascii_digit() => {
                    out.push_str("${");
                    while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                        out.push(*d);
                        chars.next();
                    }
                    out.push('}');
                }
                Some('{') => {
                    out.push('$');
                    for inner in chars.by_ref() {
                        out.push(inner);
                        if inner == '}' {
                            break;
                        }
                    }
                }
                _ => {
                    return Err(data_error(
                        "INVALID_FUNCTION_ARGUMENT",
                        format!(
                            "regexp_replace: illegal group reference in replacement \
                             {replacement:?} (write \\$ for a literal dollar sign)"
                        ),
                    ));
                }
            },
            other => out.push(other),
        }
    }
    Ok(out)
}

/// `trino_regexp_replacement(replacement)`: see [`java_replacement_to_rust`].
/// The rewriter wraps the replacement argument of every `regexp_replace`
/// call in it; for literal replacements DataFusion folds it at plan time.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoRegexpReplacement {
    signature: Signature,
}

impl Default for TrinoRegexpReplacement {
    fn default() -> Self {
        Self::new()
    }
}

impl TrinoRegexpReplacement {
    /// New instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TrinoRegexpReplacement {
    fn name(&self) -> &str {
        "trino_regexp_replacement"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let replacements = string_array("regexp_replace", &args.args[0], rows)?;
        let mut out = StringBuilder::new();
        for i in 0..rows {
            if replacements.is_null(i) {
                out.append_null();
            } else {
                out.append_value(java_replacement_to_rust(replacements.value(i))?);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_keeps_empty_fields_and_splits_characters_on_empty_delimiter() {
        assert_eq!(trino_split("", ","), vec![""]);
        assert_eq!(trino_split("a,b,,c", ","), vec!["a", "b", "", "c"]);
        assert_eq!(trino_split("abc", ""), vec!["a", "b", "c"]);
        assert_eq!(trino_split("Über", ""), vec!["Ü", "b", "e", "r"]);
        assert_eq!(trino_split("a::b", "::"), vec!["a", "b"]);
    }

    #[test]
    fn substr_follows_trino_for_non_positive_starts() {
        assert_eq!(trino_substr("hello", 2, None), "ello");
        assert_eq!(trino_substr("hello", -3, None), "llo");
        assert_eq!(trino_substr("hello", 0, None), "");
        assert_eq!(trino_substr("hello", 0, Some(3)), "");
        assert_eq!(trino_substr("hello", -3, Some(2)), "ll");
        assert_eq!(trino_substr("hello", -10, None), "");
        assert_eq!(trino_substr("hello", 6, None), "");
        assert_eq!(trino_substr("hello", 2, Some(0)), "");
        assert_eq!(trino_substr("hello", 2, Some(-1)), "");
        assert_eq!(trino_substr("hello", 4, Some(10)), "lo");
        assert_eq!(trino_substr("Überweisung", 1, Some(4)), "Über");
        assert_eq!(trino_substr("", 1, None), "");
    }

    #[test]
    fn split_part_is_null_past_the_last_field_and_errors_below_one() {
        assert_eq!(
            trino_split_part("a,b,,c", ",", 3).unwrap(),
            Some(String::new())
        );
        assert_eq!(trino_split_part("abc", ",", 1).unwrap(), Some("abc".into()));
        assert_eq!(trino_split_part("abc", ",", 2).unwrap(), None);
        assert_eq!(trino_split_part("abc", "", 2).unwrap(), Some("b".into()));
        assert_eq!(trino_split_part("abc", "", 4).unwrap(), None);
        let err = trino_split_part("abc", ",", 0).unwrap_err().to_string();
        assert!(err.contains("greater than zero"), "{err}");
    }

    #[test]
    fn java_replacement_syntax_translates_to_rust() {
        assert_eq!(java_replacement_to_rust("$1x").unwrap(), "${1}x");
        assert_eq!(java_replacement_to_rust("$2=$1").unwrap(), "${2}=${1}");
        assert_eq!(java_replacement_to_rust("$12-$0").unwrap(), "${12}-${0}");
        assert_eq!(java_replacement_to_rust("${name}!").unwrap(), "${name}!");
        assert_eq!(java_replacement_to_rust("\\$5").unwrap(), "$$5");
        assert_eq!(java_replacement_to_rust("a\\\\b").unwrap(), "a\\b");
        assert_eq!(java_replacement_to_rust("plain").unwrap(), "plain");
        assert_eq!(java_replacement_to_rust("").unwrap(), "");
        let err = java_replacement_to_rust("cost: $").unwrap_err().to_string();
        assert!(err.contains("illegal group reference"), "{err}");
        let err = java_replacement_to_rust("$x").unwrap_err().to_string();
        assert!(err.contains("illegal group reference"), "{err}");
    }
}
