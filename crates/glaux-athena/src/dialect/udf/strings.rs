//! String functions whose DataFusion namesakes differ from Trino at the
//! edges: `substr` for start ≤ 0, `split_part` past the last field,
//! `split` of an empty string or by an empty delimiter, `lpad` / `rpad`
//! with an empty pad string (an error in Trino) or a size below the length
//! (truncation), `codepoint` of more than one character (a type error in
//! Trino), and `upper` / `lower`, which Trino maps per code point (`ß` stays
//! `ß`) where Rust applies the full Unicode mapping.

use std::sync::Arc;

use arrow::array::{Array, Int32Builder, ListBuilder, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::casts::trino_type_name;
use super::{data_error, int64_array, string_array, type_mismatch};

/// The string UDFs.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(TrinoSubstr::new()),
        ScalarUDF::new_from_impl(TrinoSplitPart::new()),
        ScalarUDF::new_from_impl(TrinoSplit::new()),
        ScalarUDF::new_from_impl(TrinoStringOp::new(StringOp::Lpad)),
        ScalarUDF::new_from_impl(TrinoStringOp::new(StringOp::Rpad)),
        ScalarUDF::new_from_impl(TrinoStringOp::new(StringOp::Codepoint)),
        ScalarUDF::new_from_impl(TrinoStringOp::new(StringOp::Upper)),
        ScalarUDF::new_from_impl(TrinoStringOp::new(StringOp::Lower)),
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

// ---------------------------------------------------------------------------
// lpad / rpad / codepoint / upper / lower
// ---------------------------------------------------------------------------

/// Trino's `lpad` / `rpad`: `size` is a code-point count, a longer input is
/// truncated to `size`, an empty pad string is an error, and so is a
/// negative size.
pub fn trino_pad(function: &str, s: &str, size: i64, pad: &str, left: bool) -> Result<String> {
    if size < 0 || size > i64::from(i32::MAX) {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            format!("{function}: Target length must be in the range [0..2147483647], got {size}"),
        ));
    }
    let size = size as usize;
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= size {
        return Ok(chars[..size].iter().collect());
    }
    if pad.is_empty() {
        return Err(data_error(
            "INVALID_FUNCTION_ARGUMENT",
            format!("{function}: Padding string must not be empty"),
        ));
    }
    let padding: String = pad.chars().cycle().take(size - chars.len()).collect();
    Ok(if left {
        format!("{padding}{s}")
    } else {
        format!("{s}{padding}")
    })
}

/// Unicode simple (1:1) case mapping per code point, as Java's
/// `Character.toUpperCase(int)` / `toLowerCase(int)` which Trino applies:
/// `ß` stays `ß`, `ﬁ` stays `ﬁ`, `İ` lowers to a plain `i`, a final sigma
/// is not special-cased. Rust's `char::to_uppercase` gives the *full*
/// mapping (`ß` → `SS`); where the two differ the simple mapping is looked
/// up here (SpecialCasing.txt's unconditional entries).
pub fn simple_upper(c: char) -> char {
    let mut full = c.to_uppercase();
    match (full.next(), full.next()) {
        (Some(single), None) => single,
        _ => match c as u32 {
            // Greek letters with ypogegrammeni: the simple uppercase is the
            // titlecase form with prosgegrammeni.
            0x1F80..=0x1F87 | 0x1F90..=0x1F97 | 0x1FA0..=0x1FA7 => {
                char::from_u32(c as u32 + 8).unwrap_or(c)
            }
            0x1FB3 => '\u{1FBC}',
            0x1FC3 => '\u{1FCC}',
            0x1FF3 => '\u{1FFC}',
            _ => c,
        },
    }
}

/// See [`simple_upper`].
pub fn simple_lower(c: char) -> char {
    let mut full = c.to_lowercase();
    match (full.next(), full.next()) {
        (Some(single), None) => single,
        // LATIN CAPITAL LETTER I WITH DOT ABOVE: full mapping is `i̇`, simple
        // mapping is `i`.
        _ if c == '\u{0130}' => 'i',
        _ => c,
    }
}

/// Which string function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StringOp {
    Lpad,
    Rpad,
    Codepoint,
    Upper,
    Lower,
}

impl StringOp {
    fn trino_name(self) -> &'static str {
        match self {
            Self::Lpad => "lpad",
            Self::Rpad => "rpad",
            Self::Codepoint => "codepoint",
            Self::Upper => "upper",
            Self::Lower => "lower",
        }
    }
}

/// `trino_lpad` / `trino_rpad` / `trino_codepoint` / `trino_upper` /
/// `trino_lower`: see the module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoStringOp {
    signature: Signature,
    op: StringOp,
}

impl TrinoStringOp {
    /// New instance.
    pub fn new(op: StringOp) -> Self {
        let arity = match op {
            StringOp::Lpad | StringOp::Rpad => 3,
            _ => 1,
        };
        Self {
            signature: Signature::new(TypeSignature::Any(arity), Volatility::Immutable),
            op,
        }
    }
}

fn is_string_or_null(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
    )
}

impl ScalarUDFImpl for TrinoStringOp {
    fn name(&self) -> &str {
        match self.op {
            StringOp::Lpad => "trino_lpad",
            StringOp::Rpad => "trino_rpad",
            StringOp::Codepoint => "trino_codepoint",
            StringOp::Upper => "trino_upper",
            StringOp::Lower => "trino_lower",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let string_ok = match self.op {
            StringOp::Lpad | StringOp::Rpad => {
                is_string_or_null(&arg_types[0])
                    && is_string_or_null(&arg_types[2])
                    && (super::is_integer(&arg_types[1]) || arg_types[1] == DataType::Null)
            }
            _ => is_string_or_null(&arg_types[0]),
        };
        if !string_ok {
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
        Ok(match self.op {
            StringOp::Codepoint => DataType::Int32,
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let function = self.op.trino_name();
        let rows = args.number_rows;
        let strings = string_array(function, &args.args[0], rows)?;
        match self.op {
            StringOp::Lpad | StringOp::Rpad => {
                let sizes = int64_array(function, "size", &args.args[1], rows)?;
                let pads = string_array(function, &args.args[2], rows)?;
                let mut out = StringBuilder::new();
                for i in 0..rows {
                    if strings.is_null(i) || sizes.is_null(i) || pads.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    out.append_value(trino_pad(
                        function,
                        strings.value(i),
                        sizes.value(i),
                        pads.value(i),
                        self.op == StringOp::Lpad,
                    )?);
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            StringOp::Codepoint => {
                let mut out = Int32Builder::with_capacity(rows);
                for i in 0..rows {
                    if strings.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let mut chars = strings.value(i).chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => out.append_value(c as i32),
                        _ => {
                            return Err(type_mismatch(format!(
                                "Unexpected parameters (varchar({})) for function codepoint. \
                                 Expected: codepoint(varchar(1))",
                                strings.value(i).chars().count()
                            )));
                        }
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            StringOp::Upper | StringOp::Lower => {
                let map = if self.op == StringOp::Upper {
                    simple_upper
                } else {
                    simple_lower
                };
                let mut out = StringBuilder::new();
                for i in 0..rows {
                    if strings.is_null(i) {
                        out.append_null();
                    } else {
                        out.append_value(strings.value(i).chars().map(map).collect::<String>());
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
    fn padding_truncates_and_refuses_empty_pads() {
        assert_eq!(trino_pad("lpad", "abc", 5, "xy", true).unwrap(), "xyabc");
        assert_eq!(trino_pad("rpad", "abc", 6, "xy", false).unwrap(), "abcxyx");
        assert_eq!(trino_pad("lpad", "abcdef", 3, "x", true).unwrap(), "abc");
        assert_eq!(trino_pad("lpad", "Über", 5, "é", true).unwrap(), "éÜber");
        assert_eq!(trino_pad("lpad", "abc", 3, "", true).unwrap(), "abc");
        let err = trino_pad("lpad", "abc", 5, "", true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Padding string must not be empty"), "{err}");
        let err = trino_pad("lpad", "abc", -1, "x", true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Target length"), "{err}");
    }

    #[test]
    fn case_mapping_is_per_code_point() {
        let upper = |s: &str| s.chars().map(simple_upper).collect::<String>();
        let lower = |s: &str| s.chars().map(simple_lower).collect::<String>();
        assert_eq!(upper("straße"), "STRAßE");
        assert_eq!(upper("ﬁ"), "ﬁ");
        assert_eq!(upper("ᾀ"), "ᾈ");
        assert_eq!(upper("hello wörld"), "HELLO WÖRLD");
        assert_eq!(lower("İstanbul"), "istanbul");
        assert_eq!(lower("ΟΔΥΣΣΕΥΣ"), "οδυσσευσ");
        assert_eq!(lower("ÀB"), "àb");
    }
}
