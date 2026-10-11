//! A small JSON reader and writer with the properties Trino's JSON functions
//! rely on and `serde_json` does not offer out of the box:
//!
//! - number tokens are kept as written, so `json_extract_scalar` returns
//!   `1.50`, `1e2`, or a 30-digit integer exactly (Trino returns the raw
//!   token);
//! - object members are kept in order with duplicates, so a JSONPath lookup
//!   returns the *first* match as Trino's extractor does, while
//!   `json_parse` keeps the *last* one like Jackson's map binding;
//! - the canonical form `json_parse` / `json_format` produce sorts object
//!   keys, keeps integers exact, and prints other numbers as Java doubles
//!   (`100.0`, `1.0E-7`), as Trino's sorted object mapper does.

use std::fmt::Write as _;

use crate::results::java_double_text;

/// A parsed JSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// The number token exactly as written.
    Number(String),
    String(String),
    Array(Vec<Json>),
    /// Members in document order, duplicates included.
    Object(Vec<(String, Json)>),
}

impl Json {
    /// The first member named `key`.
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Element `index` of an array.
    pub fn index(&self, index: usize) -> Option<&Json> {
        match self {
            Json::Array(items) => items.get(index),
            _ => None,
        }
    }

    /// Whether the number token is an integer (no fraction or exponent).
    fn is_integer_token(token: &str) -> bool {
        !token.contains(['.', 'e', 'E'])
    }

    /// Trino's canonical text: sorted keys, last duplicate wins, exact
    /// integers, Java double text for other numbers.
    pub fn canonical(&self) -> String {
        let mut out = String::new();
        self.write_canonical(&mut out);
        out
    }

    fn write_canonical(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Number(token) => {
                if Self::is_integer_token(token) {
                    let trimmed = token.strip_prefix('-').unwrap_or(token);
                    if trimmed.bytes().all(|b| b == b'0') {
                        out.push('0');
                    } else {
                        out.push_str(token);
                    }
                } else {
                    match token.parse::<f64>() {
                        Ok(v) => out.push_str(&java_double_text(v)),
                        Err(_) => out.push_str(token),
                    }
                }
            }
            Json::String(s) => write_string(out, s),
            Json::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_canonical(out);
                }
                out.push(']');
            }
            Json::Object(members) => {
                // Last duplicate wins, then sort by key (UTF-16 order, as
                // Java's String ordering).
                let mut unique: Vec<(&String, &Json)> = Vec::with_capacity(members.len());
                for (k, v) in members {
                    if let Some(slot) = unique.iter_mut().find(|(key, _)| *key == k) {
                        slot.1 = v;
                    } else {
                        unique.push((k, v));
                    }
                }
                unique.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
                out.push('{');
                for (i, (k, v)) in unique.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(out, k);
                    out.push(':');
                    v.write_canonical(out);
                }
                out.push('}');
            }
        }
    }

    /// The value re-serialised compactly in document order (for
    /// `json_extract`, which copies the matched subtree).
    pub fn compact(&self) -> String {
        let mut out = String::new();
        self.write_compact(&mut out);
        out
    }

    fn write_compact(&self, out: &mut String) {
        match self {
            Json::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_compact(out);
                }
                out.push(']');
            }
            Json::Object(members) => {
                out.push('{');
                for (i, (k, v)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(out, k);
                    out.push(':');
                    v.write_compact(out);
                }
                out.push('}');
            }
            other => other.write_canonical(out),
        }
    }
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Parse a JSON document. The error text names the position.
pub fn parse(text: &str) -> Result<Json, String> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        pos: 0,
        depth: 0,
    };
    parser.skip_whitespace();
    let value = parser.value()?;
    parser.skip_whitespace();
    if parser.pos != parser.bytes.len() {
        return Err(parser.error("trailing characters after the JSON value"));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    pos: usize,
    depth: usize,
}

impl Parser<'_> {
    fn error(&self, message: &str) -> String {
        format!("{message} at offset {}", self.pos)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len()
            && matches!(self.bytes[self.pos], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn expect_literal(&mut self, literal: &str) -> Result<(), String> {
        if self.bytes[self.pos..].starts_with(literal.as_bytes()) {
            self.pos += literal.len();
            Ok(())
        } else {
            Err(self.error(&format!("expected `{literal}`")))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        match self.peek() {
            None => Err(self.error("unexpected end of input")),
            Some(b'n') => {
                self.expect_literal("null")?;
                Ok(Json::Null)
            }
            Some(b't') => {
                self.expect_literal("true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal("false")?;
                Ok(Json::Bool(false))
            }
            Some(b'"') => Ok(Json::String(self.string()?)),
            Some(b'[') => self.array(),
            Some(b'{') => self.object(),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(other) => Err(self.error(&format!("unexpected character {:?}", other as char))),
        }
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > 512 {
            return Err(self.error("nesting deeper than 512 levels"));
        }
        Ok(())
    }

    fn array(&mut self) -> Result<Json, String> {
        self.enter()?;
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_whitespace();
            items.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.error("expected `,` or `]`")),
            }
        }
        self.depth -= 1;
        Ok(Json::Array(items))
    }

    fn object(&mut self) -> Result<Json, String> {
        self.enter()?;
        self.pos += 1;
        let mut members = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(Json::Object(members));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.error("expected a string key"));
            }
            let key = self.string()?;
            self.skip_whitespace();
            if self.peek() != Some(b':') {
                return Err(self.error("expected `:`"));
            }
            self.pos += 1;
            self.skip_whitespace();
            let value = self.value()?;
            members.push((key, value));
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.error("expected `,` or `}`")),
            }
        }
        self.depth -= 1;
        Ok(Json::Object(members))
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => self.digits(),
            _ => return Err(self.error("expected a digit")),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("expected a digit after `.`"));
            }
            self.digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("expected a digit in the exponent"));
            }
            self.digits();
        }
        Ok(Json::Number(self.text[start..self.pos].to_string()))
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            let Some(b) = self.peek() else {
                return Err(self.error("unterminated string"));
            };
            match b {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.pos += 1;
                    let Some(e) = self.peek() else {
                        return Err(self.error("unterminated escape"));
                    };
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let first = self.hex4()?;
                            let code = if (0xD800..0xDC00).contains(&first) {
                                if !self.bytes[self.pos..].starts_with(b"\\u") {
                                    return Err(self.error("unpaired surrogate"));
                                }
                                self.pos += 2;
                                let second = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&second) {
                                    return Err(self.error("unpaired surrogate"));
                                }
                                0x10000 + ((first - 0xD800) << 10) + (second - 0xDC00)
                            } else {
                                first
                            };
                            out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
                        }
                        other => {
                            return Err(
                                self.error(&format!("invalid escape `\\{}`", other as char))
                            );
                        }
                    }
                }
                b if b < 0x20 => return Err(self.error("control character in string")),
                _ => {
                    // Copy one UTF-8 character.
                    let ch = self.text[self.pos..].chars().next().expect("in bounds");
                    out.push(ch);
                    self.pos += ch.len_utf8();
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let end = self.pos + 4;
        let Some(hex) = self.text.get(self.pos..end) else {
            return Err(self.error("truncated \\u escape"));
        };
        let value = u32::from_str_radix(hex, 16).map_err(|_| self.error("invalid \\u escape"))?;
        self.pos = end;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_keep_their_tokens_and_objects_keep_order() {
        let v = parse(r#"{"b": 1.50, "a": 1e2, "a": 2, "c": [1E+2, 100000000000000000000000]}"#)
            .unwrap();
        assert_eq!(v.get("b"), Some(&Json::Number("1.50".into())));
        assert_eq!(v.get("a"), Some(&Json::Number("1e2".into())));
        assert_eq!(
            v.get("c").unwrap().index(1),
            Some(&Json::Number("100000000000000000000000".into()))
        );
        assert_eq!(
            v.canonical(),
            r#"{"a":2,"b":1.5,"c":[100.0,100000000000000000000000]}"#
        );
        assert_eq!(
            v.compact(),
            r#"{"b":1.5,"a":100.0,"a":2,"c":[100.0,100000000000000000000000]}"#
        );
        assert_eq!(parse("1.0e-7").unwrap().canonical(), "1.0E-7");
        assert_eq!(parse("-0").unwrap().canonical(), "0");
        assert_eq!(
            parse(r#""a\"bé😀\n""#).unwrap(),
            Json::String("a\"bé😀\n".into())
        );
        assert_eq!(
            Json::String("tab\t\"q\"\u{1}é".into()).canonical(),
            r#""tab\t\"q\"\u0001é""#
        );
    }

    #[test]
    fn invalid_documents_are_rejected() {
        for bad in [
            "",
            "{",
            "[1,]",
            "{\"a\" 1}",
            "01",
            "1.",
            "1e",
            "\"abc",
            "tru",
            "{\"a\":1} x",
            "NaN",
            "'a'",
            "\"\u{1}\"",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
    }
}
