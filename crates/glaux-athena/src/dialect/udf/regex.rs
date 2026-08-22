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
//! ([`translate_java_pattern`]): Trino's engine (Joni, in Java syntax with
//! Unicode character tables) reads `$` as "end of text or before a final
//! newline", where Rust's default is strict; its `\d`, `\w`, `\s`, and
//! `\b` are Unicode-aware, as Rust's are, and pass through unchanged.
//!
//! Joni's `$` is a zero-width assertion: it *matches at* the position
//! before a final newline without consuming it, so `regexp_extract('ab\n',
//! 'b$')` is `'b'` and `regexp_replace('ab\n', 'b$', 'x')` is `'ax\n'`.
//! Rust's `regex` has no look-around, so `$` cannot be translated to a
//! consuming `(?:\n?\z)` group without eating that newline (which is what
//! glaux used to do). Instead `$` and `\Z` become `\z` and a text whose
//! last character is a newline is searched twice — once whole, once with
//! that newline removed — which is exactly the pair of positions Joni's
//! `$` can assert at (`Compiled::captures_at`); the leftmost match wins,
//! as in any leftmost engine. When both readings match at the same
//! position with different extents the pattern is ambiguous under this
//! emulation and glaux refuses it rather than guessing which one Joni's
//! backtracking would have reached first.

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
use crate::dialect::error::GlauxSqlError;

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

/// Translate a Java-syntax pattern (what Trino accepts) to the equivalent
/// Rust `regex` pattern, or refuse constructs whose meaning would differ:
///
/// - `\d \w \s \b` and their negations pass through: Trino runs Joni with
///   Unicode character tables (`regexp_like('٣', '\d')` is true, `(?i)` is
///   Unicode-aware), which is what Rust's `regex` does by default;
/// - `\h \H \v \V` are refused: Joni does not read them as
///   `java.util.regex`'s whitespace classes (`\v` is a vertical tab, `\h`
///   a literal `h`), so neither reading can be trusted;
/// - `$` outside a class (and `\Z`) becomes `\z` and reports that the
///   pattern is anchored at the end of the text: the caller searches a
///   text ending in a newline twice, so the assertion holds at both of the
///   positions Joni's single-line `$` accepts (see the module docs).
///   `\A`, `\z`, and `^` already agree. A `$` that something else in the
///   pattern can follow (`(a$)b`, `(a$|b)c`, `b$\n`) is refused: the
///   two-search emulation only reproduces Joni when the assertion ends the
///   match;
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
///
/// Returns the translated pattern and whether it contains a single-line
/// `$` / `\Z` (which the caller reproduces with a second search; see the
/// module docs).
pub fn translate_java_pattern(pattern: &str) -> std::result::Result<(String, bool), String> {
    let multiline = has_multiline_flag(pattern);
    // A single-line `$` or a `\Z` was translated to `\z`.
    let mut text_end_anchor = false;
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
                match escaped {
                    'h' | 'H' | 'v' | 'V' => {
                        return Err(format!(
                            "\\{escaped} is read differently by Trino's regex engine (Joni) and \
                             java.util.regex; write the whitespace characters explicitly"
                        ));
                    }
                    'Z' if !in_class => {
                        end_anchor(pattern, "\\Z", &chars)?;
                        text_end_anchor = true;
                        out.push_str("\\z");
                    }
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
                // A `[` *inside* a class opens a nested class in both
                // syntaxes — except that Rust's `regex` reads
                // `[[:alpha:]]` as a POSIX bracket expression, which
                // Trino's engine does not have (see `posix_class_name`).
                if depth > 0
                    && let Some(name) = posix_class_name(&chars.clone().collect::<String>())
                {
                    return Err(format!(
                        "POSIX bracket expression `[:{name}:]` in pattern {pattern:?}: Trino \
                         runs Joni with Java syntax, whose operator set has no POSIX bracket \
                         expressions, so `[[:{name}:]]` there is a character class over the \
                         characters that spell it (`regexp_like('ABC', '[[:alpha:]]+')` is \
                         false on Trino) — while glaux's regex engine reads it as the POSIX \
                         class it looks like. Write the characters out (`[A-Za-z]`) rather \
                         than have glaux pick one of the two meanings"
                    ));
                }
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
            '$' if depth == 0 && !multiline => {
                end_anchor(pattern, "$", &chars)?;
                text_end_anchor = true;
                out.push_str("\\z");
            }
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
    Ok((out, text_end_anchor))
}

/// The POSIX class name `rest` starts with (`alpha` for the text after the
/// second `[` of `[[:alpha:]]`, `^alpha` for a negated one), if Rust's
/// `regex` would read a POSIX bracket expression there.
///
/// Trino runs Joni with `Syntax.JAVA`, whose operator flags do not include
/// `OP_POSIX_BRACKET`, so `[:alpha:]` is not a class name there at all —
/// the two engines cannot agree, and glaux refuses instead of picking one.
fn posix_class_name(rest: &str) -> Option<&str> {
    let rest = rest.strip_prefix(':')?;
    let name = &rest[..rest.find(":]")?];
    let bare = name.strip_prefix('^').unwrap_or(name);
    (!bare.is_empty() && bare.chars().all(|c| c.is_ascii_alphabetic())).then_some(name)
}

/// Refuse a single-line `$` / `\Z` that is not the last thing its branch
/// matches. glaux emulates Joni's zero-width `$` by searching the text with
/// and without its final newline, which reproduces Joni only when the
/// assertion ends the match: with something after it (`b$\n`, `(a$)b`,
/// `(a$|b)c`) the two searches would disagree about which characters the
/// rest of the pattern sees.
fn end_anchor(
    pattern: &str,
    anchor: &str,
    rest: &std::iter::Peekable<std::str::Chars<'_>>,
) -> std::result::Result<(), String> {
    let rest: String = rest.clone().collect();
    if nothing_can_follow(&rest) {
        return Ok(());
    }
    Err(format!(
        "`{anchor}` in pattern {pattern:?} is followed by a part of the pattern that can match \
         text: Joni asserts `{anchor}` at the end of the text or before a final newline without \
         consuming it, which glaux's regex engine (no look-around) can only reproduce for an \
         assertion that ends the match"
    ))
}

/// Whether nothing in `rest` — the pattern text after a `$` — can match
/// characters after it: the end of the pattern, closing parentheses of
/// unquantified groups, and sibling alternation branches are all fine.
fn nothing_can_follow(rest: &str) -> bool {
    let mut rest = rest;
    loop {
        match rest.chars().next() {
            // End of the pattern: nothing follows.
            None => return true,
            // Leaving a group: the group must not repeat, and whatever
            // comes after it does follow the anchor.
            Some(')') => {
                let after = &rest[1..];
                if after.starts_with(['*', '+', '?', '{']) {
                    return false;
                }
                rest = after;
            }
            // The branch ends here; the sibling branches are alternatives,
            // not followers, so skip to the end of the enclosing group.
            Some('|') => match skip_to_group_end(&rest[1..]) {
                None => return true,
                Some(after) => {
                    if after.starts_with(['*', '+', '?', '{']) {
                        return false;
                    }
                    rest = after;
                }
            },
            // Anything else is a term that can match after the anchor.
            Some(_) => return false,
        }
    }
}

/// The pattern text after the `)` that closes the group `rest` starts
/// inside, or `None` when the group is the whole rest of the pattern.
fn skip_to_group_end(rest: &str) -> Option<&str> {
    let mut depth = 0usize;
    let mut class = 0usize;
    let mut class_start = false;
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                chars.next();
            }
            '[' if class == 0 || !class_start => {
                class += 1;
                class_start = true;
                continue;
            }
            ']' if class > 0 && !class_start => class -= 1,
            '(' if class == 0 => depth += 1,
            ')' if class == 0 => {
                if depth == 0 {
                    return Some(&rest[i + 1..]);
                }
                depth -= 1;
            }
            _ => {}
        }
        class_start = false;
    }
    None
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

/// A compiled pattern and how to search with it.
#[derive(Debug)]
struct Compiled {
    regex: Regex,
    /// The pattern ends a branch with a single-line `$` / `\Z`, so a text
    /// ending in a newline has to be searched twice (see the module docs).
    text_end_anchor: bool,
    /// The Java-syntax pattern, for diagnostics.
    source: String,
}

impl Compiled {
    /// The text with its final newline removed, when the pattern's `$` can
    /// assert there.
    fn stripped<'t>(&self, text: &'t str) -> Option<&'t str> {
        if self.text_end_anchor {
            text.strip_suffix('\n')
        } else {
            None
        }
    }

    /// Whether the pattern matches anywhere in `text`, as Joni reads it.
    fn is_match(&self, text: &str) -> bool {
        self.regex.is_match(text)
            || self
                .stripped(text)
                .is_some_and(|short| self.regex.is_match(short))
    }

    /// The leftmost match at or after `start`, as Joni would find it: the
    /// leftmost of the two readings of a single-line `$`. Two readings that
    /// match at the same position with different extents are refused rather
    /// than guessed (see the module docs).
    fn captures_at<'t>(
        &self,
        function: &str,
        text: &'t str,
        start: usize,
    ) -> Result<Option<regex::Captures<'t>>> {
        let whole = self.regex.captures_at(text, start);
        let Some(short) = self
            .stripped(text)
            .filter(|short| start <= short.len())
            .and_then(|short| self.regex.captures_at(short, start))
        else {
            return Ok(whole);
        };
        let Some(whole) = whole else {
            return Ok(Some(short));
        };
        let (w, s) = (
            whole.get(0).expect("group 0 always matches"),
            short.get(0).expect("group 0 always matches"),
        );
        if w.start() != s.start() {
            // Leftmost wins, as in any leftmost-first engine; only one
            // reading can match at that position (the other search would
            // have found it there too).
            return Ok(Some(if w.start() < s.start() { whole } else { short }));
        }
        let spans = |caps: &regex::Captures<'_>| {
            caps.iter()
                .map(|m| m.map(|m| (m.start(), m.end())))
                .collect::<Vec<_>>()
        };
        if spans(&whole) == spans(&short) {
            return Ok(Some(whole));
        }
        Err(ambiguous_final_newline(function, &self.source))
    }
}

/// Both readings of a single-line `$` match at the same position with
/// different extents: which one Joni returns depends on the order its
/// backtracking tries them, which glaux cannot reproduce.
fn ambiguous_final_newline(function: &str, pattern: &str) -> DataFusionError {
    DataFusionError::External(Box::new(GlauxSqlError::unsupported(
        "`$` against text ending in a newline",
        format!(
            "{function}: pattern {pattern:?} can match this text both up to and before its \
             final newline, and Trino's regex engine (Joni) asserts `$` at either position; \
             glaux refuses rather than return the wrong one of the two. Anchor the pattern \
             with `\\z`, or match `(?m)$` per line"
        ),
    )))
}

/// Compile `pattern` (Java syntax, translated), caching by text (patterns
/// are usually constant).
fn compile<'a>(
    function: &str,
    cache: &'a mut HashMap<String, Compiled>,
    pattern: &str,
) -> Result<&'a Compiled> {
    if !cache.contains_key(pattern) {
        let (translated, text_end_anchor) = translate_java_pattern(pattern).map_err(|e| {
            invalid(
                function,
                format!("regular expression {pattern:?} is not supported: {e}"),
            )
        })?;
        let regex = Regex::new(&translated).map_err(|e| {
            invalid(
                function,
                format!(
                    "invalid regular expression {pattern:?}: {e} (glaux translates Java \
                     syntax to Rust regex syntax, which lacks Java's look-around and \
                     back-references)"
                ),
            )
        })?;
        cache.insert(
            pattern.to_string(),
            Compiled {
                regex,
                text_end_anchor,
                source: pattern.to_string(),
            },
        );
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

/// Replace every match, advancing the way Trino's `JoniRegexpFunctions`
/// does.
///
/// Rust's `Regex::replace_all` skips an empty match that starts where the
/// previous match ended, so a pattern that can match the empty string
/// produced one replacement too few at every such boundary
/// (`regexp_replace('aaa', 'a*', 'X')` was `'X'`). Joni advances with
/// `getNextStart`, which only skips forward when the match *itself* was
/// zero-width, so the empty match right after a non-empty one is still
/// emitted — `'XX'`, and `"abc".replaceAll("b*", "X")` is `"XaXXcX"` in
/// Java for the same reason. The search also has to run once *at* the end
/// of the text, which is where the final empty match comes from. A text
/// ending in a newline is searched twice when the pattern's `$` can assert
/// before it; the tail after the last match carries that newline through.
fn replace_all(function: &str, regex: &Compiled, text: &str, pieces: &[Piece]) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut last_end = 0;
    let mut next_start = 0;
    while next_start <= text.len() {
        let Some(caps) = regex.captures_at(function, text, next_start)? else {
            break;
        };
        let matched = caps.get(0).expect("group 0 always matches");
        out.push_str(&text[last_end..matched.start()]);
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
        last_end = matched.end();
        next_start = if matched.is_empty() {
            // Zero-width: step over one code point, or past the end.
            matched.end()
                + text[matched.end()..]
                    .chars()
                    .next()
                    .map_or(1, char::len_utf8)
        } else {
            matched.end()
        };
    }
    out.push_str(&text[last_end..]);
    Ok(out)
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
                    let pieces = parse_replacement(&regex.regex, replacement)?;
                    out.append_value(replace_all(function, regex, texts.value(i), &pieces)?);
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
                    let available = regex.regex.captures_len() as i64 - 1;
                    if group < 0 {
                        return Err(invalid(function, "Group cannot be negative"));
                    }
                    if group > available {
                        return Err(invalid(
                            function,
                            format!("Pattern has {available} groups. Cannot access group {group}"),
                        ));
                    }
                    match regex.captures_at(function, texts.value(i), 0)? {
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
    fn posix_bracket_expressions_are_refused_by_name() {
        for pattern in [
            "[[:alpha:]]+",
            "[[:digit:]]",
            "[a-z[:space:]]",
            "[[:^alpha:]]",
            "x[[:alnum:]]*y",
        ] {
            let err = translate_java_pattern(pattern).unwrap_err();
            assert!(err.contains("POSIX bracket expression"), "{pattern}: {err}");
        }
        // Not a POSIX bracket expression: an ordinary nested class, a
        // colon in a class, and a top-level `[:alpha:]` (a class over the
        // characters that spell it in *both* engines).
        for pattern in ["[a[bc]]", "[a:b]", "[:alpha:]", "[a[:b]]", "[[]]"] {
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
        let compiled = Compiled {
            regex: re.clone(),
            text_end_anchor: false,
            source: "(?<x>b)(c)?".to_string(),
        };
        assert_eq!(
            replace_all(
                "regexp_replace",
                &compiled,
                "abc",
                &parse_replacement(&re, "[$2$1]").unwrap()
            )
            .unwrap(),
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
    fn java_classes_stay_unicode_and_dollar_allows_a_final_newline() {
        let t = |p: &str| translate_java_pattern(p).unwrap().0;
        assert_eq!(t(r"\d+"), r"\d+");
        assert_eq!(t(r"[\d_]"), r"[\d_]");
        assert_eq!(t(r"[^\w]"), r"[^\w]");
        assert_eq!(t(r"\bx\b"), r"\bx\b");
        assert_eq!(t(r"b$"), r"b\z");
        assert_eq!(t(r"[$]"), "[$]");
        assert_eq!(t(r"\$"), r"\$");
        assert_eq!(t(r"(?m)b$"), "(?m)b$");
        assert_eq!(t(r"(?i)ab"), "(?i)ab");
        assert_eq!(t(r"[]a]"), r"[\]a]");
        assert_eq!(t(r"\p{L}+"), r"\p{L}+");
        for bad in [
            r"(?u)a",
            r"(?U)a",
            r"\p{Alpha}",
            r"\p{IsAlphabetic}",
            r"a\",
            r"\h",
            r"[\v]",
            // A `$` something else can match after.
            r"b$\n",
            r"(a$)b",
            r"(a$|b)c",
            r"(a$)*",
            r"a\Zb",
        ] {
            assert!(translate_java_pattern(bad).is_err(), "{bad}");
        }
        // A `$` that ends its branch is fine, however it is nested.
        for good in [r"^a$|^b$", r"(?:ab$)", r"a(b$|c$)", r"x(?:y$)", r"\Az\Z"] {
            translate_java_pattern(good).unwrap_or_else(|e| panic!("{good}: {e}"));
        }
        // `$` is an assertion: it holds before a final newline without
        // consuming it, so the match is `b`, not `b\n`.
        let mut cache = HashMap::new();
        let dollar = compile("regexp_extract", &mut cache, "b$").unwrap();
        assert!(dollar.is_match("ab\n"));
        assert!(dollar.is_match("ab"));
        assert!(!dollar.is_match("ab\n\n"));
        let matched = dollar
            .captures_at("regexp_extract", "ab\n", 0)
            .unwrap()
            .unwrap();
        assert_eq!(matched.get(0).unwrap().as_str(), "b");
        assert_eq!(
            replace_all(
                "regexp_replace",
                dollar,
                "ab\n",
                &[Piece::Literal("x".into())]
            )
            .unwrap(),
            "ax\n"
        );
        // Joni with Unicode tables: Arabic-Indic digits, accented letters,
        // and NBSP are digits, word characters, and whitespace.
        assert!(Regex::new(&t(r"\d")).unwrap().is_match("٣"));
        assert!(Regex::new(&t(r"\w")).unwrap().is_match("é"));
        assert!(Regex::new(&t(r"\s")).unwrap().is_match("\u{a0}"));
        assert_eq!(
            Regex::new(&t(r"\b")).unwrap().replace_all("aé b", "|"),
            "|aé| |b|"
        );
        assert_eq!(
            Regex::new(&t(r"\W")).unwrap().replace_all("José", ""),
            "José"
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
