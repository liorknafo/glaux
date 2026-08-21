//! The Trino function shim registry and SQL construct coverage table.
//!
//! This table is the single source of truth for what the dialect layer
//! accepts: the rewriter consults it for every function call, and
//! `docs/sql-coverage.md` is rendered from it (a test asserts the checked-in
//! file is fresh). A function that is not listed here is refused up front —
//! even when DataFusion happens to have a function of the same name —
//! because same-named functions with different semantics (`concat` and
//! NULLs, `regexp_replace` and global replacement, `repeat` producing a
//! string instead of an array) are exactly how an emulator ends up silently
//! wrong.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::LazyLock;

/// How a Trino function reaches DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShimKind {
    /// Same name, same semantics: the call is handed to DataFusion as-is.
    Passthrough,
    /// Rewritten at the AST level onto DataFusion functions or syntax.
    Rewrite,
    /// Implemented as a Rust UDF registered under the Trino name.
    Udf,
    /// Recognised and refused with an explicit error naming the function.
    Unsupported,
}

impl ShimKind {
    /// Label used in the coverage table.
    pub fn label(self) -> &'static str {
        match self {
            Self::Passthrough => "passthrough",
            Self::Rewrite => "rewrite",
            Self::Udf => "udf",
            Self::Unsupported => "unsupported",
        }
    }
}

/// One row of the function shim table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunctionShim {
    /// Lower-case Trino function name.
    pub name: &'static str,
    /// Trino signature, for the docs.
    pub signature: &'static str,
    /// Coverage table category.
    pub category: &'static str,
    /// How the call is handled.
    pub kind: ShimKind,
    /// Human description of the translation (or of why it is refused).
    pub translation: &'static str,
    /// DataFusion functions the translation emits (checked to exist by a
    /// test, so a DataFusion upgrade that renames one fails loudly).
    pub df_functions: &'static [&'static str],
}

/// Whether a SQL construct (as opposed to a function) is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstructStatus {
    /// Parses and plans; exercised by the corpus.
    Supported,
    /// Refused with an explicit error naming the construct.
    Unsupported,
}

/// One row of the construct coverage table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Construct {
    /// Construct name as it appears in error messages.
    pub name: &'static str,
    /// Category for the docs.
    pub category: &'static str,
    /// Supported or refused.
    pub status: ConstructStatus,
    /// Notes (semantics, caveats, what to use instead).
    pub notes: &'static str,
    /// A substring that must appear in at least one corpus query (upper-
    /// cased comparison) so the "supported" claim is backed by a test.
    pub corpus_marker: &'static str,
}

macro_rules! shim {
    ($name:literal, $sig:literal, $cat:literal, $kind:ident, $how:literal, [$($df:literal),*]) => {
        FunctionShim {
            name: $name,
            signature: $sig,
            category: $cat,
            kind: ShimKind::$kind,
            translation: $how,
            df_functions: &[$($df),*],
        }
    };
}

/// The function shim table. Keep it sorted by category, then name, so the
/// rendered docs stay diff-friendly.
pub static FUNCTIONS: &[FunctionShim] = &[
    // --- Aggregate ---------------------------------------------------------
    shim!(
        "approx_distinct",
        "approx_distinct(x) → bigint",
        "Aggregate",
        Passthrough,
        "`CAST(approx_distinct(x) AS BIGINT)` (HyperLogLog; estimates differ from Trino's within the usual error bound)",
        ["approx_distinct"]
    ),
    shim!(
        "approx_percentile",
        "approx_percentile(x, percentage) → same as x",
        "Aggregate",
        Rewrite,
        "`trino_approx_percentile(x, percentage)`, a port of airlift's `TDigest` (the structure Trino uses, same merge rule and `valueAt` interpolation; compression 100), so small inputs give exactly Trino's answer and large ones the same approximation scheme, where Trino's own result already depends on how the input was split across workers. DataFusion's `approx_percentile_cont` is not used: its t-digest interpolates differently (`approx_percentile(amount, 0.9)` over the corpus `orders` gives `216.1` there and `240.0` on Trino). Overloads: `bigint` (result `Math.round`-ed), `real`, `double`; a DECIMAL argument is refused, as are the weighted and array-of-percentages forms.",
        ["trino_approx_percentile"]
    ),
    shim!(
        "arbitrary",
        "arbitrary(x) → same as x",
        "Aggregate",
        Rewrite,
        "`first_value(x)`",
        ["first_value"]
    ),
    shim!(
        "array_agg",
        "array_agg(x) → array",
        "Aggregate",
        Passthrough,
        "DataFusion `array_agg` (supports `ORDER BY` inside the call and `DISTINCT`)",
        ["array_agg"]
    ),
    shim!(
        "avg",
        "avg(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `avg`; over DECIMAL inputs glaux substitutes its own aggregate so the result is `decimal(p, s)` rounded HALF_UP, as in Trino (DataFusion would add four decimal places: `avg` of `1.5` and `2.5` is `2.0`, not `2.00000`), and over REAL inputs one that returns `real` (a double accumulator cast to float at the end, as Trino's).",
        ["avg", "trino_decimal_avg", "trino_real_avg"]
    ),
    shim!(
        "bool_and",
        "bool_and(boolean) → boolean",
        "Aggregate",
        Passthrough,
        "DataFusion `bool_and`",
        ["bool_and"]
    ),
    shim!(
        "bool_or",
        "bool_or(boolean) → boolean",
        "Aggregate",
        Passthrough,
        "DataFusion `bool_or`",
        ["bool_or"]
    ),
    shim!(
        "corr",
        "corr(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `corr`",
        ["corr"]
    ),
    shim!(
        "count",
        "count(*) / count(x) / count(DISTINCT x) → bigint",
        "Aggregate",
        Passthrough,
        "DataFusion `count`",
        ["count"]
    ),
    shim!(
        "count_if",
        "count_if(boolean) → bigint",
        "Aggregate",
        Rewrite,
        "`count(CASE WHEN x THEN 1 END)` (keeps `FILTER` / `OVER` clauses)",
        ["count"]
    ),
    shim!(
        "covar_pop",
        "covar_pop(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `covar_pop`",
        ["covar_pop"]
    ),
    shim!(
        "covar_samp",
        "covar_samp(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `covar_samp`",
        ["covar_samp"]
    ),
    shim!(
        "every",
        "every(boolean) → boolean",
        "Aggregate",
        Rewrite,
        "`bool_and(x)`",
        ["bool_and"]
    ),
    shim!(
        "listagg",
        "listagg(x, separator) WITHIN GROUP (...)",
        "Aggregate",
        Unsupported,
        "Refused. Use `array_join(array_agg(x ORDER BY ...), separator)`.",
        []
    ),
    shim!(
        "max",
        "max(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `max`; over DOUBLE / REAL inputs glaux substitutes its own aggregate ranking NaN smallest (`max` of `{1.0, NaN}` is `1.0`, NaN only when every value is NaN), matching Trino's `COMPARISON_UNORDERED_FIRST`; Arrow's `max` would return NaN. `min` needs no substitute: both engines rank NaN largest there. Over arrays it goes through Trino's array ordering operator, which raises `ARRAY comparison not supported for arrays with null elements` once a shared prefix forces it to read a NULL element (Arrow's kernels would rank the NULL and answer). The same holds for the window form and for an aggregate's own `ORDER BY` (`array_agg(x ORDER BY x)`).",
        ["max"]
    ),
    shim!(
        "max_by",
        "max_by(x, y)",
        "Aggregate",
        Unsupported,
        "Refused: DataFusion 55 has no `max_by`. Rewrite with a window function (`row_number() OVER (ORDER BY y DESC)`).",
        []
    ),
    shim!(
        "min",
        "min(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `min`. Over arrays it goes through Trino's array ordering operator, which raises `ARRAY comparison not supported for arrays with null elements` once a shared prefix forces it to read a NULL element (Arrow's kernels would rank the NULL and answer).",
        ["min"]
    ),
    shim!(
        "min_by",
        "min_by(x, y)",
        "Aggregate",
        Unsupported,
        "Refused: DataFusion 55 has no `min_by`. Rewrite with a window function.",
        []
    ),
    shim!(
        "stddev",
        "stddev(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev` (sample)",
        ["stddev"]
    ),
    shim!(
        "stddev_pop",
        "stddev_pop(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev_pop`",
        ["stddev_pop"]
    ),
    shim!(
        "stddev_samp",
        "stddev_samp(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev_samp`",
        ["stddev_samp"]
    ),
    shim!(
        "sum",
        "sum(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `sum`; for integer inputs glaux substitutes an overflow-checked sum so a bigint overflow is an error (`NUMERIC_VALUE_OUT_OF_RANGE`), for DECIMAL inputs a `decimal(38, s)` sum with Trino's overflow check, and for REAL inputs a sum accumulated in double and returned as `real` (`3.3000002`, not a `double`), as in Trino.",
        [
            "sum",
            "trino_checked_sum",
            "trino_decimal_sum",
            "trino_real_sum"
        ]
    ),
    shim!(
        "var_pop",
        "var_pop(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `var_pop`",
        ["var_pop"]
    ),
    shim!(
        "var_samp",
        "var_samp(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `var_samp`",
        ["var_samp"]
    ),
    shim!(
        "variance",
        "variance(x) → double",
        "Aggregate",
        Rewrite,
        "`var_samp(x)`",
        ["var_samp"]
    ),
    // --- Window ------------------------------------------------------------
    shim!(
        "cume_dist",
        "cume_dist() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `cume_dist`",
        ["cume_dist"]
    ),
    shim!(
        "dense_rank",
        "dense_rank() OVER (...)",
        "Window",
        Passthrough,
        "`CAST(dense_rank(...) AS BIGINT)` — DataFusion's result is unsigned",
        ["dense_rank"]
    ),
    shim!(
        "first_value",
        "first_value(x) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `first_value`",
        ["first_value"]
    ),
    shim!(
        "lag",
        "lag(x[, offset[, default]]) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `lag`. The offset must be a non-negative integer literal: a negative offset is `INVALID_FUNCTION_ARGUMENT: Offset must be at least 0` and a NULL offset `Offset must not be null`, as on Trino (DataFusion would silently run `lag(x, -1)` as `lead`).",
        ["lag"]
    ),
    shim!(
        "last_value",
        "last_value(x) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `last_value`",
        ["last_value"]
    ),
    shim!(
        "lead",
        "lead(x[, offset[, default]]) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `lead`. The offset must be a non-negative integer literal: a negative offset is `INVALID_FUNCTION_ARGUMENT: Offset must be at least 0` and a NULL offset `Offset must not be null`, as on Trino (DataFusion would silently run `lead(x, -1)` as `lag`).",
        ["lead"]
    ),
    shim!(
        "nth_value",
        "nth_value(x, n) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `nth_value`. `n` must be a positive integer literal: `nth_value(x, 0)` is `INVALID_FUNCTION_ARGUMENT: Offset must be at least 1` and a NULL `n` `Offset must not be null`, as on Trino (DataFusion would return NULL).",
        ["nth_value"]
    ),
    shim!(
        "ntile",
        "ntile(n) OVER (...)",
        "Window",
        Passthrough,
        "`CAST(ntile(...) AS BIGINT)` — DataFusion's result is unsigned. `n` must be a positive integer literal (`ntile(0)` is `INVALID_FUNCTION_ARGUMENT`, as on Trino).",
        ["ntile"]
    ),
    shim!(
        "percent_rank",
        "percent_rank() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `percent_rank`",
        ["percent_rank"]
    ),
    shim!(
        "rank",
        "rank() OVER (...)",
        "Window",
        Passthrough,
        "`CAST(rank(...) AS BIGINT)` — DataFusion's result is unsigned",
        ["rank"]
    ),
    shim!(
        "row_number",
        "row_number() OVER (...)",
        "Window",
        Passthrough,
        "`CAST(row_number(...) AS BIGINT)` — DataFusion's result is unsigned",
        ["row_number"]
    ),
    // --- Conditional -------------------------------------------------------
    shim!(
        "coalesce",
        "coalesce(a, b, ...)",
        "Conditional",
        Passthrough,
        "DataFusion `coalesce`",
        ["coalesce"]
    ),
    shim!(
        "if",
        "if(condition, true_value[, false_value])",
        "Conditional",
        Rewrite,
        "`CASE WHEN condition THEN true_value ELSE false_value END` (`ELSE NULL` when omitted)",
        []
    ),
    shim!(
        "nullif",
        "nullif(a, b)",
        "Conditional",
        Passthrough,
        "DataFusion `nullif`. Trino types the expression as the *first* argument's type and coerces only the comparison (`nullif(2, 1.0)` is integer 2), where DataFusion widens the result to the common supertype, so glaux rewrites mixed-type calls to `CASE WHEN a = b THEN NULL ELSE a END` with the coercion confined to the `WHEN`. Over DOUBLE / REAL arguments the substituted comparison uses IEEE equality, so `nullif(0e0, -0e0)` is NULL and `nullif(NaN, NaN)` is NaN, as on Trino (DataFusion's kernel compares bit patterns).",
        ["nullif"]
    ),
    shim!(
        "try",
        "try(expr)",
        "Conditional",
        Unsupported,
        "Refused: DataFusion has no error-suppressing wrapper. `TRY_CAST` covers the cast case.",
        []
    ),
    shim!(
        "typeof",
        "typeof(expr) → varchar",
        "Conditional",
        Unsupported,
        "Refused: DataFusion's type names (`Utf8`, `Int64`) are not Trino's.",
        []
    ),
    // --- String ------------------------------------------------------------
    shim!(
        "chr",
        "chr(n) → varchar",
        "String",
        Passthrough,
        "DataFusion `chr`",
        ["chr"]
    ),
    shim!(
        "codepoint",
        "codepoint(varchar) → integer",
        "String",
        Rewrite,
        "Rust UDF `trino_codepoint`: the code point of a one-character string; longer (or empty) strings are a `TYPE_MISMATCH`, as Trino only accepts `varchar(1)`.",
        ["trino_codepoint"]
    ),
    shim!(
        "concat",
        "concat(a, b, ...) → varchar | array",
        "String",
        Rewrite,
        "`a || b || ...`. DataFusion's own `concat` skips NULL arguments where Trino returns NULL; the operator form propagates NULL like Trino.",
        []
    ),
    shim!(
        "length",
        "length(varchar) → bigint",
        "String",
        Passthrough,
        "`CAST(length(x) AS BIGINT)` (characters, not bytes). Non-varchar arguments are a `TYPE_MISMATCH` (DataFusion would stringify `length(123)`).",
        ["length"]
    ),
    shim!(
        "levenshtein_distance",
        "levenshtein_distance(a, b) → bigint",
        "String",
        Rewrite,
        "`CAST(levenshtein(a, b) AS BIGINT)`",
        ["levenshtein"]
    ),
    shim!(
        "lower",
        "lower(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_lower`: Unicode simple (per-code-point) case mapping, as Java's `Character.toLowerCase`: `lower('İstanbul')` is `istanbul`, a final sigma is not special-cased (Rust's full mapping would differ).",
        ["trino_lower"]
    ),
    shim!(
        "lpad",
        "lpad(varchar, size, padstring)",
        "String",
        Rewrite,
        "Rust UDF `trino_lpad`: `size` counts code points, a longer input is truncated to `size`, an empty pad string is an error (`Padding string must not be empty`), as in Trino.",
        ["trino_lpad"]
    ),
    shim!(
        "ltrim",
        "ltrim(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_ltrim`: strips every Java whitespace code point (tab, LF, CR, VT, FF, `U+001C`–`U+001F`, the Unicode space separators, `U+2028`, `U+2029`; not NBSP), as Trino does, where DataFusion's `ltrim` strips only the ASCII space. `ltrim(x, chars)` is refused (not a Trino signature); use `TRIM(LEADING chars FROM x)`.",
        ["trino_ltrim"]
    ),
    shim!(
        "replace",
        "replace(varchar, search[, replacement])",
        "String",
        Rewrite,
        "Rust UDF `trino_replace`; the 2-argument (delete) form becomes `replace(x, search, '')`. An **empty** `search` follows Trino's separate branch, which inserts the replacement in front of every code point and at the end: `replace('abc', '', 'X')` is `'XaXbXcX'`, `replace('', '', 'X')` is `'X'`, `replace('a👍', '', '-')` is `'-a-👍-'` (DataFusion's `replace` returned the input unchanged).",
        ["trino_replace"]
    ),
    shim!(
        "reverse",
        "reverse(varchar) → varchar, reverse(array) → array",
        "String",
        Rewrite,
        "Rust UDF `trino_reverse`: reverses a string's code points or an array's elements (DataFusion's `reverse` is string-only and would stringify an array)",
        ["trino_reverse"]
    ),
    shim!(
        "rpad",
        "rpad(varchar, size, padstring)",
        "String",
        Rewrite,
        "Rust UDF `trino_rpad`: `size` counts code points, a longer input is truncated to `size`, an empty pad string is an error (`Padding string must not be empty`), as in Trino.",
        ["trino_rpad"]
    ),
    shim!(
        "rtrim",
        "rtrim(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_rtrim`: strips every Java whitespace code point, like `ltrim`. `rtrim(x, chars)` is refused (not a Trino signature); use `TRIM(TRAILING chars FROM x)`.",
        ["trino_rtrim"]
    ),
    shim!(
        "split",
        "split(varchar, delimiter) → array(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_split`: `split('', ',')` is `['']` (DataFusion's `string_to_array` gives `[]`) and an empty delimiter is an `INVALID_FUNCTION_ARGUMENT` error (`The delimiter may not be the empty string`), as in Trino — only `split_part` splits into characters on it. The 3-argument `split(x, delimiter, limit)` form is refused.",
        ["trino_split"]
    ),
    shim!(
        "split_part",
        "split_part(varchar, delimiter, index) → varchar",
        "String",
        Rewrite,
        "Rust UDF `trino_split_part`: 1-based; NULL past the last field (DataFusion returns `''`); `index < 1` is an error; an empty delimiter splits into characters.",
        ["trino_split_part"]
    ),
    shim!(
        "starts_with",
        "starts_with(varchar, prefix) → boolean",
        "String",
        Passthrough,
        "DataFusion `starts_with`",
        ["starts_with"]
    ),
    shim!(
        "strpos",
        "strpos(varchar, substring) → bigint",
        "String",
        Passthrough,
        "`CAST(strpos(x, sub) AS BIGINT)` (1-based, 0 when absent). The 3-argument `strpos(x, sub, instance)` form is refused.",
        ["strpos"]
    ),
    shim!(
        "substr",
        "substr(varchar, start[, length])",
        "String",
        Rewrite,
        "Rust UDF `trino_substr` with Trino's rules: 1-based; negative `start` counts from the end; `start = 0`, a non-positive `length`, or a start past either end gives `''` (DataFusion's `substr` follows PostgreSQL, where `substr('hello', -3)` is `'hello'`).",
        ["trino_substr"]
    ),
    shim!(
        "substring",
        "substring(varchar, start[, length])",
        "String",
        Rewrite,
        "Same as `substr`, for both the call and the `SUBSTRING(x FROM s FOR n)` syntax.",
        ["trino_substr"]
    ),
    shim!(
        "translate",
        "translate(varchar, from, to)",
        "String",
        Passthrough,
        "DataFusion `translate`",
        ["translate"]
    ),
    shim!(
        "trim",
        "trim(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_trim`: strips every Java whitespace code point, like `ltrim`. The `TRIM([BOTH | LEADING | TRAILING] [chars] FROM x)` syntax strips any code point of `chars` (Trino's set semantics); the two-argument call `trim(x, chars)` is refused.",
        ["trino_trim"]
    ),
    shim!(
        "upper",
        "upper(varchar)",
        "String",
        Rewrite,
        "Rust UDF `trino_upper`: Unicode simple (per-code-point) case mapping, as Java's `Character.toUpperCase`: `upper('straße')` keeps `ß` and `upper('ﬁ')` keeps the ligature (Rust's full mapping gives `SS` / `FI`).",
        ["trino_upper"]
    ),
    // --- Regular expression ------------------------------------------------
    shim!(
        "regexp_extract",
        "regexp_extract(varchar, pattern[, group]) → varchar",
        "Regular expression",
        Rewrite,
        "Rust UDF `trino_regexp_extract`: `group` must be a literal and is checked against the pattern's capture groups (`Pattern has 1 groups. Cannot access group 2`, as in Trino). Patterns use Java syntax translated to Rust `regex` syntax: `\\d`, `\\w`, `\\s`, `\\b`, and `(?i)` are Unicode-aware (Trino runs Joni with Unicode character tables, which Rust's defaults match: `regexp_like('٣', '\\d')` is true) and `$` also matches before a final newline, as in Trino's engine; `\\h` / `\\v` (which Joni reads differently from `java.util.regex`), look-around, back-references, possessive quantifiers (`a*+`, `a++`, `a{n,m}+` — Rust's engine would backtrack where Java's does not), `\\p{Alpha}`-style POSIX classes, and the `u` / `U` inline flags are `INVALID_FUNCTION_ARGUMENT` errors.",
        ["trino_regexp_extract"]
    ),
    shim!(
        "regexp_extract_all",
        "regexp_extract_all(varchar, pattern[, group]) → array(varchar)",
        "Regular expression",
        Unsupported,
        "Refused: DataFusion's `regexp_match` returns only the first match.",
        []
    ),
    shim!(
        "regexp_like",
        "regexp_like(varchar, pattern) → boolean",
        "Regular expression",
        Rewrite,
        "Rust UDF `trino_regexp_like`. Patterns use Java syntax translated to Rust `regex` syntax: `\\d`, `\\w`, `\\s`, `\\b`, and `(?i)` are Unicode-aware (Trino runs Joni with Unicode character tables, which Rust's defaults match: `regexp_like('٣', '\\d')` is true) and `$` also matches before a final newline, as in Trino's engine; `\\h` / `\\v` (which Joni reads differently from `java.util.regex`), look-around, back-references, possessive quantifiers (`a*+`, `a++`, `a{n,m}+` — Rust's engine would backtrack where Java's does not), `\\p{Alpha}`-style POSIX classes, and the `u` / `U` inline flags are `INVALID_FUNCTION_ARGUMENT` errors.",
        ["trino_regexp_like"]
    ),
    shim!(
        "regexp_replace",
        "regexp_replace(varchar, pattern[, replacement]) → varchar",
        "Regular expression",
        Rewrite,
        "Rust UDF `trino_regexp_replace`: every match is replaced, advancing the way Trino's `JoniRegexpFunctions` does — `getNextStart` only skips forward when the match *itself* was zero-width, so an empty match landing right after a non-empty one is still replaced (`regexp_replace('aaa', 'a*', 'X')` is `XX` and `regexp_replace('abc', 'b*', 'X')` is `XaXXcX`, exactly as Java's `Matcher.replaceAll`), where Rust's `Regex::replace_all` skips it and lost one replacement per such boundary; the replacement uses Java syntax (`$1x` is group 1 then `x`, `${name}` a named group, `\\$` a literal dollar) and every group reference is validated against the pattern (`No group 2`, `No group with name {y}`, as in Trino). Patterns use Java syntax translated to Rust `regex` syntax: `\\d`, `\\w`, `\\s`, `\\b`, and `(?i)` are Unicode-aware (Trino runs Joni with Unicode character tables, which Rust's defaults match: `regexp_like('٣', '\\d')` is true) and `$` also matches before a final newline, as in Trino's engine; `\\h` / `\\v` (which Joni reads differently from `java.util.regex`), look-around, back-references, possessive quantifiers (`a*+`, `a++`, `a{n,m}+` — Rust's engine would backtrack where Java's does not), `\\p{Alpha}`-style POSIX classes, and the `u` / `U` inline flags are `INVALID_FUNCTION_ARGUMENT` errors. The lambda form is refused.",
        ["trino_regexp_replace"]
    ),
    shim!(
        "regexp_split",
        "regexp_split(varchar, pattern) → array(varchar)",
        "Regular expression",
        Unsupported,
        "Refused: no DataFusion equivalent.",
        []
    ),
    // --- Date and time -----------------------------------------------------
    shim!(
        "at_timezone",
        "at_timezone(timestamp, zone)",
        "Date and time",
        Unsupported,
        "Refused in v0.1 along with `AT TIME ZONE`.",
        []
    ),
    shim!(
        "current_date",
        "current_date → date",
        "Date and time",
        Passthrough,
        "DataFusion `current_date`",
        ["current_date"]
    ),
    shim!(
        "current_time",
        "current_time → time",
        "Date and time",
        Unsupported,
        "Refused: Trino's `current_time` is a `time(3) with time zone`, which glaux cannot return in v0.1.",
        []
    ),
    shim!(
        "current_timestamp",
        "current_timestamp → timestamp",
        "Date and time",
        Rewrite,
        "`now()` rounded to milliseconds as a `timestamp(3) with time zone` at UTC (Athena prints `2024-01-05 10:30:00.000 UTC`).",
        ["now", "trino_timestamp_millis", "arrow_cast"]
    ),
    shim!(
        "date",
        "date(x) → date",
        "Date and time",
        Rewrite,
        "Same as `CAST(x AS DATE)` (see `CAST`).",
        ["trino_date"]
    ),
    shim!(
        "date_add",
        "date_add(unit, value, timestamp) → same type",
        "Date and time",
        Udf,
        "Rust UDF: calendar arithmetic for month/quarter/year (clamps to month end), fixed lengths otherwise, on the calendar value itself so dates such as `9999-12-31` work (no Arrow nanosecond range limit). Units: millisecond … year; adding sub-day units to a DATE is an error, and so is a fractional `value` (Trino requires bigint).",
        ["date_add"]
    ),
    shim!(
        "date_diff",
        "date_diff(unit, timestamp1, timestamp2) → bigint",
        "Date and time",
        Udf,
        "Rust UDF: `timestamp2 - timestamp1` in whole units, truncated toward zero for fixed units; month/quarter/year follow Joda-Time's `getDifference` as Trino does (`date_diff('month', DATE '2024-01-31', DATE '2024-02-29')` is 1; years balance February 29 against non-leap years). No Arrow nanosecond range limit.",
        ["date_diff"]
    ),
    shim!(
        "date_format",
        "date_format(timestamp, format) → varchar",
        "Date and time",
        Rewrite,
        "`to_char(x, <strftime>)` with the MySQL-style format translated specifier by specifier; the format must be a literal and unknown specifiers are refused. `%v` (Monday-first week) and `%x` (the week-year it belongs to) map onto chrono's ISO `%V` / `%G`.",
        ["to_char"]
    ),
    shim!(
        "date_parse",
        "date_parse(varchar, format) → timestamp",
        "Date and time",
        Rewrite,
        "Rust UDF `trino_date_parse(x, <strftime>, ...)` with the MySQL-style format translated; the format must be a literal and the input a varchar, and the result is a zone-less `timestamp(3)` (fractions beyond milliseconds are truncated, as Joda does). Every field the format does not name keeps the epoch default Joda's parse bucket starts from, so `date_parse('2024-01-05 10', '%Y-%m-%d %H')` is `10:00:00`, `date_parse('2024-01-05 10:30', '%Y-%m-%d %h:%i')` is `10:30:00` (a 12-hour field with no `%p` is AM, as Joda's `clockhourOfHalfday` default is), `date_parse('12:30 AM', '%h:%i %p')` is `1970-01-01 00:30:00`, and `date_parse('2024-01', '%Y-%m')` is the first of the month. DataFusion's `to_timestamp` is not used: chrono's field resolution needs hour + minute (and AM/PM for a 12-hour field) and silently fell back to midnight for the rest, dropping the whole time of day. `%f` accepts 1-9 fractional digits when parsing, like Trino, but only directly after a `.`. A second of `60` (chrono's leap second, which DataFusion would roll over to the next minute) is refused with Joda's `Value 60 for secondOfMinute must be in the range [0,59]`. A format that names a weekday without pinning the date down (`%W` alone, which Joda resolves against its epoch base) is refused rather than guessed at.",
        ["trino_date_parse"]
    ),
    shim!(
        "date_trunc",
        "date_trunc(unit, x) → same as x",
        "Date and time",
        Rewrite,
        "Rust UDF `trino_date_trunc`: a `date` input stays a `date` (DataFusion's `date_trunc` returns a timestamp), sub-day units on a `date` are refused, and varchar input is refused (`TYPE_MISMATCH`, as in Trino). Weeks start on Monday.",
        ["trino_date_trunc"]
    ),
    shim!(
        "day",
        "day(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('day', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "day_of_month",
        "day_of_month(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('day', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "day_of_week",
        "day_of_week(x) → bigint (1 = Monday … 7 = Sunday)",
        "Date and time",
        Rewrite,
        "`date_part('dow', x)` remapped from DataFusion's 0 = Sunday to Trino's ISO numbering",
        ["date_part"]
    ),
    shim!(
        "day_of_year",
        "day_of_year(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('doy', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "dow",
        "dow(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `day_of_week`",
        ["date_part"]
    ),
    shim!(
        "doy",
        "doy(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `day_of_year`",
        ["date_part"]
    ),
    shim!(
        "format_datetime",
        "format_datetime(timestamp, pattern) → varchar",
        "Date and time",
        Rewrite,
        "`to_char(x, <strftime>)` with the Joda pattern translated; the pattern must be a literal and unknown pattern letters are refused. Numeric fields honour Joda's letter count as a minimum digit count (`D` prints `5`, `DDD` `005`, `w` `1`, `ww` `01`, `H:m:s` `14:5:9`); counts chrono cannot pad to (`DD`, `yyyyy`, `ddd`, `ee`, ...) are refused by name. `Z` / `ZZ` / `ZZZ` print `+0000` / `+00:00` / `UTC` (timestamps are UTC instants).",
        ["to_char"]
    ),
    shim!(
        "from_iso8601_date",
        "from_iso8601_date(varchar) → date",
        "Date and time",
        Udf,
        "Rust UDF: strict ISO-8601 calendar date (`YYYY-MM-DD`, also `YYYY-MM` / `YYYY`); anything else (`2024-1-1`, ordinal or week dates) is an error.",
        ["from_iso8601_date"]
    ),
    shim!(
        "from_iso8601_timestamp",
        "from_iso8601_timestamp(varchar) → timestamp with time zone",
        "Date and time",
        Udf,
        "Rust UDF: strict ISO-8601 (`YYYY-MM-DD[THH[:mm[:ss[.fff]]]][Z|+00:00]`); a space separator or single-digit fields are errors, as in Trino. The result is a `timestamp(3) with time zone` at UTC (Athena prints `... UTC`); an input with a non-zero offset is refused, because Trino keeps the offset and glaux cannot.",
        ["from_iso8601_timestamp"]
    ),
    shim!(
        "from_unixtime",
        "from_unixtime(double) → timestamp(3) with time zone",
        "Date and time",
        Rewrite,
        "`arrow_cast(CAST(round(x * 1000) AS BIGINT), 'Timestamp(Millisecond, Some(\"UTC\"))')`: a `timestamp(3) with time zone` at UTC (Athena prints `1970-01-01 00:00:00.000 UTC`), rounded to the millisecond like Athena (`from_unixtime(1.9999)` is `…:02.000`). The zone-argument forms are refused.",
        ["arrow_cast", "round"]
    ),
    shim!(
        "hour",
        "hour(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('hour', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "last_day_of_month",
        "last_day_of_month(x) → date",
        "Date and time",
        Unsupported,
        "Refused: no DataFusion equivalent. Use `date_add('day', -1, date_add('month', 1, date_trunc('month', x)))`.",
        []
    ),
    shim!(
        "localtimestamp",
        "localtimestamp → timestamp",
        "Date and time",
        Rewrite,
        "`now()` rounded to milliseconds as a zone-less `timestamp(3)` (UTC).",
        ["now", "trino_timestamp"]
    ),
    shim!(
        "minute",
        "minute(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('minute', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "month",
        "month(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('month', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "now",
        "now() → timestamp with time zone",
        "Date and time",
        Rewrite,
        "Same as `current_timestamp`.",
        ["now", "trino_timestamp_millis", "arrow_cast"]
    ),
    shim!(
        "parse_datetime",
        "parse_datetime(varchar, pattern) → timestamp",
        "Date and time",
        Rewrite,
        "Rust UDF `trino_date_parse` (see `date_parse`, including Joda's epoch defaults for the fields the pattern leaves out: `parse_datetime('2024-01-05 10', 'yyyy-MM-dd HH')` is `10:00:00` and `'yyyy-MM-dd hh a'` reads `10 PM` as `22:00:00`) with the Joda pattern translated, re-tagged as a `timestamp(3) with time zone` at UTC (Athena prints `... UTC`); the pattern must be a literal. `SSS` / `SSSSSS` parse exactly that many fractional digits (Joda accepts fewer). Zone letters (`Z`, `z`) are refused: Trino would keep the parsed offset, which glaux cannot.",
        ["trino_date_parse", "arrow_cast"]
    ),
    shim!(
        "quarter",
        "quarter(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('quarter', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "second",
        "second(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('second', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "to_iso8601",
        "to_iso8601(x) → varchar",
        "Date and time",
        Unsupported,
        "Refused: the output depends on the argument type. Use `format_datetime(x, 'yyyy-MM-dd''T''HH:mm:ss.SSS')` or `CAST(x AS VARCHAR)`.",
        []
    ),
    shim!(
        "to_unixtime",
        "to_unixtime(timestamp) → double",
        "Date and time",
        Rewrite,
        "Rust UDF `trino_to_unixtime` (microsecond resolution). Varchar input is refused (`TYPE_MISMATCH`, as in Trino).",
        ["trino_to_unixtime"]
    ),
    shim!(
        "week",
        "week(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('week', x) AS BIGINT)` (ISO week)",
        ["date_part"]
    ),
    shim!(
        "week_of_year",
        "week_of_year(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `week`",
        ["date_part"]
    ),
    shim!(
        "with_timezone",
        "with_timezone(timestamp, zone)",
        "Date and time",
        Unsupported,
        "Refused in v0.1 along with `AT TIME ZONE`.",
        []
    ),
    shim!(
        "year",
        "year(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('year', x) AS BIGINT)`",
        ["date_part"]
    ),
    // --- Math --------------------------------------------------------------
    shim!(
        "abs",
        "abs(x)",
        "Math",
        Passthrough,
        "DataFusion `abs`",
        ["abs"]
    ),
    shim!(
        "cbrt",
        "cbrt(x) → double",
        "Math",
        Passthrough,
        "DataFusion `cbrt`",
        ["cbrt"]
    ),
    shim!(
        "ceil",
        "ceil(x)",
        "Math",
        Rewrite,
        "Rust UDF `trino_ceil`: keeps an integer argument's type (`ceil(5)` is the bigint `5`, not `5.0`) and returns `decimal(p - s + min(s, 1), 0)` for a `decimal(p, s)` (`ceil(2.5)` is `3`), as in Trino.",
        ["trino_ceil"]
    ),
    shim!(
        "ceiling",
        "ceiling(x)",
        "Math",
        Rewrite,
        "Same as `ceil`.",
        ["trino_ceil"]
    ),
    shim!(
        "exp",
        "exp(x) → double",
        "Math",
        Passthrough,
        "DataFusion `exp`",
        ["exp"]
    ),
    shim!(
        "floor",
        "floor(x)",
        "Math",
        Rewrite,
        "Rust UDF `trino_floor`: keeps an integer argument's type (`floor(5)` is the bigint `5`) and returns `decimal(p - s + min(s, 1), 0)` for a `decimal(p, s)` (`floor(-2.5)` is `-3`), as in Trino.",
        ["trino_floor"]
    ),
    shim!(
        "greatest",
        "greatest(a, b, ...)",
        "Math",
        Rewrite,
        "`CASE WHEN a IS NULL OR b IS NULL ... THEN NULL ELSE greatest(a, b, ...) END`: NULL if any argument is NULL, as in Trino (DataFusion skips NULLs). Arguments must share a type. Over DOUBLE / REAL arguments glaux substitutes its own function ranking NaN smallest (`greatest(1e0, NaN)` is `1.0`), matching Trino's `COMPARISON_UNORDERED_FIRST`; `least` needs no substitute (both engines rank NaN largest there). Over arrays it goes through Trino's array ordering operator, which raises `ARRAY comparison not supported for arrays with null elements` once a shared prefix forces it to read a NULL element (Arrow's kernels would rank the NULL and answer).",
        ["greatest"]
    ),
    shim!(
        "least",
        "least(a, b, ...)",
        "Math",
        Rewrite,
        "`CASE WHEN a IS NULL OR b IS NULL ... THEN NULL ELSE least(a, b, ...) END`: NULL if any argument is NULL, as in Trino (DataFusion skips NULLs). Arguments must share a type. Over arrays it goes through Trino's array ordering operator, which raises `ARRAY comparison not supported for arrays with null elements` once a shared prefix forces it to read a NULL element (Arrow's kernels would rank the NULL and answer).",
        ["least"]
    ),
    shim!(
        "ln",
        "ln(x) → double",
        "Math",
        Passthrough,
        "DataFusion `ln`",
        ["ln"]
    ),
    shim!(
        "log",
        "log(base, x) → double",
        "Math",
        Passthrough,
        "DataFusion `log(base, x)` (same argument order). Trino has only the two-argument form: `log(x)` is refused (DataFusion would run it as `log10`); use `log10`, `log2`, or `ln`.",
        ["log"]
    ),
    shim!(
        "log10",
        "log10(x) → double",
        "Math",
        Passthrough,
        "DataFusion `log10`",
        ["log10"]
    ),
    shim!(
        "log2",
        "log2(x) → double",
        "Math",
        Passthrough,
        "DataFusion `log2`",
        ["log2"]
    ),
    shim!("mod", "mod(n, m)", "Math", Rewrite, "`n % m`", []),
    shim!(
        "infinity",
        "infinity() → double",
        "Math",
        Rewrite,
        "`trino_double('Infinity')` — glaux's `CAST(varchar AS DOUBLE)`, which follows Java's `Double.parseDouble`. There is no infinite literal to fold onto, and DataFusion has no such function. Negative infinity is `-infinity()`.",
        ["trino_double"]
    ),
    shim!(
        "pi",
        "pi() → double",
        "Math",
        Passthrough,
        "DataFusion `pi`",
        ["pi"]
    ),
    shim!(
        "nan",
        "nan() → double",
        "Math",
        Rewrite,
        "`trino_double('NaN')` — glaux's `CAST(varchar AS DOUBLE)`, which follows Java's `Double.parseDouble`. There is no NaN literal to fold onto, and DataFusion has no such function; `0e0 / 0e0` is the same value.",
        ["trino_double"]
    ),
    shim!(
        "pow",
        "pow(x, p) → double",
        "Math",
        Rewrite,
        "Rust UDF `trino_power(CAST(x AS DOUBLE), CAST(p AS DOUBLE))`: always a double, as in Trino (DataFusion keeps integer arguments integral), and Java's `Math.pow` value throughout — `power(0, -1)` is `Infinity` and `power(-0e0, -1)` is `-Infinity`, where DataFusion carries PostgreSQL's `zero raised to a negative power is undefined` guard. The four cases where `Math.pow` departs from C's `pow` are reproduced too: `p = 0` is `1.0` for any `x`, `p = 1` is `x`, a NaN `p` is NaN (so `power(1, nan())` is NaN, not `1.0`), and `|x| = 1` with an infinite `p` is NaN.",
        ["trino_power"]
    ),
    shim!(
        "power",
        "power(x, p) → double",
        "Math",
        Rewrite,
        "Rust UDF `trino_power(CAST(x AS DOUBLE), CAST(p AS DOUBLE))`: always a double, as in Trino (DataFusion keeps integer arguments integral), and Java's `Math.pow` value throughout — `power(0, -1)` is `Infinity` and `power(-0e0, -1)` is `-Infinity`, where DataFusion carries PostgreSQL's `zero raised to a negative power is undefined` guard. The four cases where `Math.pow` departs from C's `pow` are reproduced too: `p = 0` is `1.0` for any `x`, `p = 1` is `x`, a NaN `p` is NaN (so `power(1, nan())` is NaN, not `1.0`), and `|x| = 1` with an infinite `p` is NaN.",
        ["trino_power"]
    ),
    shim!(
        "rand",
        "rand() → double",
        "Math",
        Rewrite,
        "`random()`",
        ["random"]
    ),
    shim!(
        "random",
        "random() → double, random(n) → same integer type as n",
        "Math",
        Rewrite,
        "DataFusion `random` for the nullary form; the bounded overload becomes the Rust UDF `trino_random(n, random())`, a uniform value in `[0, n)` with `n`'s own type and an `INVALID_FUNCTION_ARGUMENT` for `n <= 0`, as in Trino.",
        ["random", "trino_random"]
    ),
    shim!(
        "round",
        "round(x[, d])",
        "Math",
        Rewrite,
        "Rust UDF `trino_round`: for doubles exactly Trino's `Math.round(x · 10ⁿ) / 10ⁿ` (sign-flipped for negatives, Trino's BigInteger fallback when `Math.round` saturates), so the double product decides the tie: `round(2.675, 2)` is `2.68` (the product is exactly `267.5`, though the double `2.675` is below 2.675) but `round(1.005, 2)` is `1.0` (the product is `100.49999999999999`); DataFusion rounds the exact decimal instead and answers `2.67` / `1.01`. Trino declares the double overload `neverFails`, so the edge branches return values rather than erroring: a product that overflows to infinity gives `x` back (`round(1e308, 2)` is `1e308`) and an `n` so negative that `10ⁿ` underflows to zero gives a signed zero (`round(-1.5e0, -400)` is `-0.0`). Integers keep their type (`round(1250, -2)` is `1300`); a `decimal(p, s)` rounds HALF_UP to `decimal(p - s + min(s, 1), 0)` with one argument and to `decimal(p + 1, s)` with two (`round(2.789, 2)` is `2.790`).",
        ["trino_round"]
    ),
    shim!(
        "sign",
        "sign(x)",
        "Math",
        Rewrite,
        "Rust UDF `trino_sign`: the argument's type for integers and doubles, `decimal(1, 0)` for decimals, as in Trino.",
        ["trino_sign"]
    ),
    shim!(
        "sqrt",
        "sqrt(x) → double",
        "Math",
        Rewrite,
        "Rust UDF `trino_sqrt`: `NaN` for a negative argument, as Java's `Math.sqrt` (DataFusion raises an error).",
        ["trino_sqrt"]
    ),
    shim!(
        "truncate",
        "truncate(x[, n])",
        "Math",
        Rewrite,
        "Rust UDF `trino_truncate`: integers keep their type; a `decimal(p, s)` becomes `decimal(max(1, p - s), 0)` with one argument (Trino's `@Constraint(variable = \"rp\", expression = \"max(1, p - s)\")`, one digit narrower than `ceiling` / `floor`'s `p - s + min(s, 1)`, so `truncate(1.98)` is `decimal(1,0)`) and keeps `decimal(p, s)` with two (`truncate(2.789, 2)` is `2.780`); single-argument `truncate(double)` is `signum(x) · floor(|x|)`. `truncate(double, n)` is refused by name: the two-argument overload exists only for DECIMAL — neither Trino (Athena engine v3) nor Presto 0.217 (engine v2) declares one for DOUBLE / REAL, so Athena answers it with a function-resolution error, and DataFusion's own two-argument trunc would silently return a value Athena never would.",
        ["trino_truncate"]
    ),
    // --- Array -------------------------------------------------------------
    shim!(
        "all_match",
        "all_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "any_match",
        "any_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "array_distinct",
        "array_distinct(array) → array",
        "Array",
        Passthrough,
        "DataFusion `array_distinct`",
        ["array_distinct"]
    ),
    shim!(
        "array_join",
        "array_join(array, delimiter[, null_replacement]) → varchar",
        "Array",
        Rewrite,
        "Rust UDF `trino_array_join`: elements are rendered in Trino's text forms (`1.0`, `2024-01-05 10:00:00.000`); NULL elements are skipped unless a replacement is given, like Trino.",
        ["trino_array_join"]
    ),
    shim!(
        "array_max",
        "array_max(array)",
        "Array",
        Rewrite,
        "Rust UDF `trino_array_max`: NULL when the array is empty or has a NULL element, as in Trino (DataFusion skips NULL elements); NaN ranks smallest (`array_max(ARRAY[1e0, NaN])` is `1.0`), matching Trino's `COMPARISON_UNORDERED_FIRST`. Ranking elements that are themselves arrays uses Trino's array ordering operator, so a NULL *inside* one of them raises `ARRAY comparison not supported for arrays with null elements`.",
        ["trino_array_max"]
    ),
    shim!(
        "array_min",
        "array_min(array)",
        "Array",
        Rewrite,
        "Rust UDF `trino_array_min`: NULL when the array is empty or has a NULL element, as in Trino (DataFusion skips NULL elements). Ranking elements that are themselves arrays uses Trino's array ordering operator, so a NULL *inside* one of them raises `ARRAY comparison not supported for arrays with null elements`.",
        ["trino_array_min"]
    ),
    shim!(
        "array_position",
        "array_position(array, element) → bigint",
        "Array",
        Rewrite,
        "Rust UDF `trino_array_position`: 0 for a missing element (DataFusion returns NULL), NULL for a NULL array or element argument, and Trino's EQUAL semantics for float elements (`array_position(ARRAY[NaN], NaN)` is 0; NaN equals nothing).",
        ["trino_array_position"]
    ),
    shim!(
        "array_remove",
        "array_remove(array, element) → array",
        "Array",
        Rewrite,
        "Rust UDF `trino_array_remove`: every occurrence is removed, NULL elements are kept (`array_remove(ARRAY[1, NULL], 1)` is `[NULL]`), a NULL element argument gives NULL, as in Trino. Float elements match with Trino's EQUAL semantics, so NaN is never removed.",
        ["trino_array_remove"]
    ),
    shim!(
        "array_sort",
        "array_sort(array) → array",
        "Array",
        Rewrite,
        "`array_sort(x, 'ASC', 'NULLS LAST')`: ascending with NULL elements last, as in Trino (DataFusion's default puts them first). Sorting elements that are themselves arrays uses Trino's array ordering operator, so a NULL *inside* one of them raises `ARRAY comparison not supported for arrays with null elements`. The comparator-lambda form is refused.",
        ["array_sort"]
    ),
    shim!(
        "array_union",
        "array_union(a, b) → array",
        "Array",
        Passthrough,
        "DataFusion `array_union`",
        ["array_union"]
    ),
    shim!(
        "arrays_overlap",
        "arrays_overlap(a, b) → boolean",
        "Array",
        Rewrite,
        "Rust UDF `trino_arrays_overlap`: NULL (not false) when no element matches but either array has a NULL element, as in Trino. Element types must be comparable.",
        ["trino_arrays_overlap"]
    ),
    shim!(
        "cardinality",
        "cardinality(array) → bigint",
        "Array",
        Rewrite,
        "`CAST(cardinality(array) AS BIGINT)` (DataFusion returns an unsigned integer)",
        ["cardinality"]
    ),
    shim!(
        "contains",
        "contains(array, element) → boolean",
        "Array",
        Rewrite,
        "Rust UDF `trino_contains`: NULL (not false) when the element is not found but the array has a NULL element, or when the element is NULL, as in Trino. The element type must be comparable with the array's. Float elements match with Trino's EQUAL semantics (`contains(ARRAY[NaN], NaN)` is false, `contains(ARRAY[0e0], -0e0)` is true).",
        ["trino_contains"]
    ),
    shim!(
        "element_at",
        "element_at(array, index)",
        "Array",
        Rewrite,
        "Rust UDF `trino_element_at`: 1-based, negative indexes count from the end, NULL past either end, `index = 0` is an error (`SQL array indices start at 1`). `element_at` on maps is refused.",
        ["trino_element_at"]
    ),
    shim!(
        "filter",
        "filter(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "flatten",
        "flatten(array(array)) → array",
        "Array",
        Passthrough,
        "DataFusion `flatten`",
        ["flatten"]
    ),
    shim!(
        "none_match",
        "none_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "reduce",
        "reduce(array, initial, lambda, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "repeat",
        "repeat(element, count) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `repeat` repeats a *string* (`repeat('a', 3) = 'aaa'`) where Trino builds an array. Silently wrong if passed through.",
        []
    ),
    shim!(
        "sequence",
        "sequence(start, stop[, step]) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `generate_series` / `range` differ in inclusivity and date handling.",
        []
    ),
    shim!(
        "slice",
        "slice(array, start, length) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `array_slice` takes an end index, not a length, and handles negative starts differently.",
        []
    ),
    shim!(
        "transform",
        "transform(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "zip",
        "zip(a, b, ...) → array(row)",
        "Array",
        Unsupported,
        "Refused: ROW types are not supported in v0.1.",
        []
    ),
    // --- Row and map -------------------------------------------------------
    shim!(
        "map",
        "map(keys, values) → map",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_concat",
        "map_concat(a, b) → map",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_keys",
        "map_keys(map) → array",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_values",
        "map_values(map) → array",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "row",
        "ROW(a, b, ...) → row",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    // --- JSON --------------------------------------------------------------
    shim!(
        "json_array_contains",
        "json_array_contains(json, value) → boolean",
        "JSON",
        Unsupported,
        "Refused in v0.1. Use `json_extract_scalar` on known indexes.",
        []
    ),
    shim!(
        "json_array_get",
        "json_array_get(json, index) → json",
        "JSON",
        Unsupported,
        "Refused in v0.1. Use `json_extract(json, '$[index]')`.",
        []
    ),
    shim!(
        "json_array_length",
        "json_array_length(json) → bigint",
        "JSON",
        Udf,
        "Rust UDF: NULL when the value is not an array, and NULL for text that is not valid JSON (Trino's varchar overload; only `json_parse` raises).",
        ["json_array_length"]
    ),
    shim!(
        "json_extract",
        "json_extract(json, json_path) → json",
        "JSON",
        Udf,
        "Rust UDF; the JSON type is represented as its text. JSONPath subset: `$`, `.key`, `[\"key\"]`, `[n]` — wildcards, recursive descent, slices and filters are refused. Text that is not valid JSON gives NULL (Trino's varchar overload). A duplicate key resolves to its first occurrence, as in Trino; the matched value is re-serialised compactly in document order with non-integer numbers in Java double text (`2.50` becomes `2.5`).",
        ["json_extract"]
    ),
    shim!(
        "json_extract_scalar",
        "json_extract_scalar(json, json_path) → varchar",
        "JSON",
        Udf,
        "Rust UDF; same JSONPath subset. NULL for missing paths, JSON nulls, objects and arrays, and for text that is not valid JSON (Trino's varchar overload; only `json_parse` raises). Numbers are returned as written (`1.50`, `1e2`, a 30-digit integer), and a duplicate key resolves to its first occurrence, as in Trino.",
        ["json_extract_scalar"]
    ),
    shim!(
        "json_format",
        "json_format(json) → varchar",
        "JSON",
        Udf,
        "Rust UDF: Trino's canonical JSON text (sorted keys, last duplicate key wins, exact integers, Java double text for other numbers).",
        ["json_format"]
    ),
    shim!(
        "json_parse",
        "json_parse(varchar) → json",
        "JSON",
        Udf,
        "Rust UDF: validates the text (invalid JSON is an error, like Trino) and keeps it as Trino's canonical text: sorted keys, last duplicate key wins, integers exact at any size, other numbers in Java double text (`1e2` becomes `100.0`).",
        ["json_parse"]
    ),
    shim!(
        "json_size",
        "json_size(json, json_path) → bigint",
        "JSON",
        Udf,
        "Rust UDF: member count of the object/array at the path, 0 for scalars, NULL for text that is not valid JSON.",
        ["json_size"]
    ),
    // --- Misc --------------------------------------------------------------
    shim!(
        "uuid",
        "uuid() → uuid",
        "Misc",
        Passthrough,
        "DataFusion `uuid` (returned as varchar)",
        ["uuid"]
    ),
];

/// The SQL construct coverage table.
pub static CONSTRUCTS: &[Construct] = &[
    Construct {
        name: "SELECT / DISTINCT / WHERE",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Full projection, `SELECT DISTINCT`, arbitrary predicates. Anonymous output columns are named `_col0`, `_col1`, … by position (also inside derived tables and CTEs) and duplicate output names are allowed, as on Athena. `DISTINCT ON`, `QUALIFY`, `GROUP BY ALL`, `TOP`, `SELECT INTO`, `TABLESAMPLE`, `FOR UPDATE`, and `SELECT * EXCLUDE` are refused as non-Trino syntax.",
        corpus_marker: "SELECT DISTINCT",
    },
    Construct {
        name: "JOIN (INNER, LEFT, RIGHT, FULL, CROSS)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`ON` and `USING` forms; join keys must have comparable types (`TYPE_MISMATCH` otherwise). `JOIN ... USING (k)` follows Trino: one `k` column (the left value for inner / left joins, the right value for right joins, `coalesce(l.k, r.k)` for full joins — DataFusion alone would return one side's NULL), `SELECT *` lists the `USING` columns first, then the remaining left columns, then the remaining right columns, and a qualified `a.k` is refused (`Column 'a.k' cannot be resolved`, as on Trino). A `JOIN` with neither `ON` nor `USING` is refused (DataFusion would run a cross join); write `CROSS JOIN`. Equi-join keys of `DOUBLE` / `REAL` type run through a nested-loop join with IEEE equality, because Trino's join equality never matches NaN while DataFusion's hash join would (`USING` over float keys is refused; write `ON`). `NATURAL`, `SEMI` / `ANTI`, `APPLY`, and `ASOF` joins are refused as non-Trino syntax.",
        corpus_marker: "FULL OUTER JOIN",
    },
    Construct {
        name: "Common table expressions (WITH)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Including multiple and chained CTEs.",
        corpus_marker: "WITH ",
    },
    Construct {
        name: "Subqueries (derived tables, scalar, IN, EXISTS)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Correlated `EXISTS` / `IN` are decorrelated by DataFusion. A scalar subquery that returns no rows is NULL, also over a non-nullable source such as `VALUES` or a literal (DataFusion alone would fail with `declared as non-nullable but contains null values`). A **correlated scalar subquery that is not itself an aggregate** — `SELECT (SELECT c.id FROM customers c WHERE c.id = o.customer_id) FROM orders o`, valid Trino — is rewritten into `trino_scalar_subquery((SELECT trino_single_value(x) …), (SELECT trino_group_rows(x) …))`: DataFusion's decorrelation needs an aggregate, and the row count is checked *outside* the subquery so that only groups an outer row really matches raise `SUBQUERY_MULTIPLE_ROWS`, as on Trino (checking inside the aggregate would refuse a group no outer row selects). Refused by name, not answered differently: a correlated scalar subquery in `ORDER BY` or a `JOIN` condition (Trino allows it; DataFusion decorrelates only in `SELECT` / `WHERE` / `GROUP BY`), one carrying an `ORDER BY` or `LIMIT` (Trino applies it per outer row, which decorrelation cannot express), one whose correlation condition compares `DOUBLE` / `REAL`, and one whose correlation conditions name two columns with the same bare name from different relations (`… WHERE c.id = o.customer_id AND x.id = o.id`) — DataFusion re-qualifies correlated columns by their bare name, so the two would collapse onto one join key and every row would come back wrong. `IN (subquery)` / `EXISTS` used as a *value* (in the select list or any position other than a `WHERE` / `HAVING` predicate) is refused by name — DataFusion cannot evaluate it as an expression. `IN (subquery)` over `DOUBLE` / `REAL` operands is refused by name: Trino compares with IEEE equality (NaN never matches), which DataFusion's semi-join does not reproduce. The quantified comparison predicates `> ALL` / `= ANY` / `< SOME` are refused by name; see their own row.",
        corpus_marker: "EXISTS (",
    },
    Construct {
        name: "Window functions",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` and named windows. Ranking functions return `bigint` (cast from DataFusion's unsigned result). Window `ORDER BY` sorts NULLs last by default, as in Trino. The offset arguments of `lead` / `lag` / `nth_value` / `ntile` are validated as on Trino (`lead(x, -1)` is `INVALID_FUNCTION_ARGUMENT: Offset must be at least 0`; DataFusion would run it as `lag`).",
        corpus_marker: "OVER (",
    },
    Construct {
        name: "GROUP BY / HAVING / ROLLUP / CUBE / GROUPING SETS",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`GROUP BY` and `HAVING` resolve against the source columns only, as on Trino: `SELECT status s, count(*) FROM t GROUP BY s` and `HAVING c > 1` over an alias `c` are `Column cannot be resolved` unless the source has a column of that name (DataFusion would resolve the output alias). `ORDER BY` may use output aliases. `GROUP BY ()` — Trino's empty grouping set, one global group — is planned as `GROUPING SETS (())`; sqlparser parses it as an empty tuple, which DataFusion refused with `Empty tuple not supported yet`.",
        corpus_marker: "ROLLUP",
    },
    Construct {
        name: "ORDER BY / LIMIT / OFFSET",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`NULLS FIRST/LAST` honoured. Trino's default — NULLs last whatever the direction, also for window `ORDER BY` and aggregate `ORDER BY` arguments — is applied when unspecified (DataFusion's own default would sort NULLs first under `DESC`). An `ORDER BY` name matching several output columns (`SELECT id x, amount x ... ORDER BY x`) is refused as ambiguous, as on Trino. `ORDER BY ALL` is refused. `FETCH FIRST n ROWS ONLY` (Trino's standard form of LIMIT) is rewritten onto LIMIT; `WITH TIES` and `PERCENT` are refused by name. A negative `LIMIT` / `OFFSET` is refused up front, as Trino's grammar does (it has no sign there); DataFusion accepted the literal and failed inside an optimizer rule, naming the rule.",
        corpus_marker: "OFFSET",
    },
    Construct {
        name: "UNION / UNION ALL / INTERSECT / EXCEPT",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Corresponding columns must have comparable types (`SELECT 1 UNION SELECT 'a'` is a `TYPE_MISMATCH`, as on Athena). The result types follow Trino: `SELECT 1 UNION SELECT 1` is `integer`, `1 UNION ALL 1.5` is `decimal(11,1)`, and `1.5 UNION 2e0` is `double` (DataFusion alone would report `bigint`, `decimal(21,1)`, and `decimal(30,15)`). `INTERSECT ALL` keeps the minimum multiplicity of each row, as on Trino; `EXCEPT ALL` (bag difference on Trino: `{1, 1, 1} EXCEPT ALL {1}` is `{1, 1}`) is refused by name because DataFusion plans it as an anti-join that drops every matching row. `FETCH FIRST n ROWS WITH TIES` is refused, as are the `BY NAME` quantifiers.",
        corpus_marker: "UNION ALL",
    },
    Construct {
        name: "VALUES",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Inline tables, also as a `FROM` source with column aliases; anonymous columns are `_col0`, `_col1`, … as on Athena. Bare (unparenthesised) row expressions — `VALUES 1, 2`, valid Trino — are wrapped for sqlparser at the token level. The rows must share a type, as on Trino: `(VALUES (1), ('2'))` is `TYPE_MISMATCH: Values rows have mismatched types: row(integer) vs row(varchar(1))` — the literals are named with the types Trino gives them, not the `bigint` / unbounded `varchar` DataFusion planned them as (DataFusion alone coerced it into a bigint column with the rows `1, 2`), and the pairs DataFusion refuses itself carry the same diagnostic instead of its `Inconsistent data type across values list` text. Column types follow Trino: `(VALUES (1), (2))` is `integer`, `(VALUES (1), (1.5))` is `decimal(11,1)`, and a column mixing a double with an exact number is `double` (DataFusion alone would report `decimal(30,15)`).",
        corpus_marker: "VALUES",
    },
    Construct {
        name: "CASE",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Simple and searched forms. The operand must be comparable with the `WHEN` values and all results must share a type (`TYPE_MISMATCH` otherwise, as on Athena); the same applies to `if`, `nullif`, and `coalesce`.",
        corpus_marker: "CASE WHEN",
    },
    Construct {
        name: "CAST",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Trino type names (`VARCHAR[(n)]`, `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`, `DOUBLE`, `REAL`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`) map to Arrow types; casts Trino does not define (`CAST(12 AS DATE)`, `CAST(DATE ... AS BIGINT)`, `CAST(TIMESTAMP ... AS DOUBLE)`) are a `TYPE_MISMATCH` instead of running with DataFusion's semantics. The `x::type` form is refused as non-Trino syntax. Double/decimal → integer rounds half away from zero (`CAST(2.5 AS BIGINT)` is 3) and fails on overflow (`INVALID_CAST_ARGUMENT`; NULL under `TRY_CAST`). `CAST(... AS VARCHAR)` uses Trino's text forms (`2024-01-05 10:30:00.000`, `1.0E20`); `VARCHAR(n)` truncates a varchar source but refuses a longer text of any other type (`CAST(12345 AS VARCHAR(2))` fails, as on Trino). Varchar → `TIMESTAMP` follows Trino's pattern (`YYYY-MM-DD[ HH:MM[:SS[.fraction]]]`, rounded HALF_UP to milliseconds, zone suffixes refused); varchar → `DATE` must be exactly a calendar date (`'2024-01-05 10:00:00'` fails, as on Trino); varchar → `BOOLEAN` accepts only `true`/`false`/`t`/`f`/`1`/`0`; varchar → `DOUBLE` / `REAL` follows Java's `Double.parseDouble` (`NaN`, `Infinity`, `-Infinity` exactly — `'nan'`, `'inf'`, `'infinity'` are `INVALID_CAST_ARGUMENT`; surrounding whitespace and a `d` / `f` suffix are accepted; hexadecimal floats are refused). `FLOAT` is not a Trino type name and is refused. Double → `DECIMAL(p, s)` uses the exact binary expansion with HALF_UP rounding (`CAST(1e0 AS DECIMAL(38,37))` is exactly 1). `CHAR(n)`, `TIMESTAMP(p ≠ 3)`, `VARBINARY`, `JSON`, `ROW`, `MAP` targets are refused by name — `JSON` because glaux carries the JSON type as the varchar holding its text while Trino's `CAST(x AS JSON)` builds the JSON *value* of x (`CAST('abc' AS JSON)` is the JSON string `\"abc\"`, and casting a column of documents would quote each document), so returning the text would be a different answer; use `json_parse` / `json_format` instead. Array types use Trino's `ARRAY(T)` syntax (`CAST(NULL AS ARRAY(INTEGER))`); Hive's `ARRAY<T>` is refused as non-Trino syntax.",
        corpus_marker: "CAST(",
    },
    Construct {
        name: "TRY_CAST",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "NULL on conversion failure.",
        corpus_marker: "TRY_CAST(",
    },
    Construct {
        name: "INTERVAL literals",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`INTERVAL '<n>' YEAR | MONTH | DAY | HOUR | MINUTE | SECOND` (a whole number, optionally signed; a fraction only for `SECOND`) and `timestamp ± interval` arithmetic. PostgreSQL interval strings DataFusion would accept (`INTERVAL '1 day'`, `'1 hour 30 minutes'`) are refused as syntax errors, and the range forms (`INTERVAL '1-2' YEAR TO MONTH`, `'1 02:03:04' DAY TO SECOND`) and `interval * n` / `interval / n` (an `interval` result in Trino) are refused by name. `date ± interval` requires a whole number of days (`DATE '2024-01-05' + INTERVAL '1' HOUR` is an error, as on Trino, where DataFusion would drop the hour). `date - date`, `timestamp - timestamp`, and `interval + interval` (an `interval` result in Trino) are refused; use `date_diff`. Comparing intervals (`INTERVAL '1' DAY = INTERVAL '24' HOUR`, true on Trino, which compares the normalised milliseconds / months and refuses mixed kinds) is refused by name, because DataFusion compares its month/day/nanosecond triple structurally and would answer false.",
        corpus_marker: "INTERVAL '",
    },
    Construct {
        name: "String concatenation (||)",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "NULL-propagating, like Trino.",
        corpus_marker: "||",
    },
    Construct {
        name: "BETWEEN / IN (list) / LIKE / IS [NOT] NULL / IS DISTINCT FROM",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`LIKE` has no default escape character, as in Trino: a backslash in the pattern is literal (`'a_c' LIKE 'a\\_c'` is false) unless an `ESCAPE` clause names it; the escape character must precede `%`, `_`, or itself. `ILIKE`, `SIMILAR TO`, `LIKE ANY`, and the `IS [NOT] TRUE` / `IS [NOT] FALSE` / `IS [NOT] UNKNOWN` predicates (not in Trino's grammar; DataFusion would evaluate them) are refused as non-Trino syntax. Array comparison follows Trino's NULL-element rules: `ARRAY[1, NULL] = ARRAY[1, NULL]` is NULL (a definite element mismatch or a length mismatch is `false`), and ordering arrays with NULL elements is an error, `ARRAY comparison not supported for arrays with null elements`. Every path that ranks arrays raises it: `<` / `<=` / `>` / `>=`, `BETWEEN`, `ORDER BY`, `max` / `min` (aggregate and window), `greatest` / `least`, `array_max` / `array_min`, `array_sort`, and an aggregate's own `ORDER BY`.",
        corpus_marker: "IS DISTINCT FROM",
    },
    Construct {
        name: "ARRAY[...] literals and 1-based subscripts",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`arr[1]` is the first element; `arr[0]`, negative, and out-of-range subscripts are errors, as in Trino (use `element_at` for NULL instead). The bare `[1, 2]` form is refused as non-Trino syntax. The elements must share a type, like every other operand list: `ARRAY[1, '2']` is `TYPE_MISMATCH: All ARRAY elements must be the same type or coercible to a common type. Cannot find common type between integer and varchar(1)` at planning (DataFusion alone answered `[1, 2]`, or failed at run time with an Arrow cast error for `ARRAY[1, 'a']`). The element type follows Trino too: a double mixed with an exact number is a `double` array, where DataFusion would unify on `decimal(38,15)`.",
        corpus_marker: "ARRAY[",
    },
    Construct {
        name: "EXTRACT(field FROM x) / POSITION / SUBSTRING / TRIM syntax",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`EXTRACT` fields: YEAR, QUARTER, MONTH, WEEK, DAY, DAY_OF_MONTH, DAY_OF_WEEK/DOW (1 = Monday … 7 = Sunday, Trino numbering), DAY_OF_YEAR/DOY, HOUR, MINUTE, SECOND; other fields are refused by name. `SUBSTRING` follows `substr`'s rules; `POSITION` returns bigint. `TRIM(LEADING | TRAILING | BOTH FROM x)` without trim characters (valid Trino, and the one `TRIM` spelling sqlparser cannot parse) is turned into the equivalent `ltrim` / `rtrim` / `trim` call at the token level.",
        corpus_marker: "EXTRACT(",
    },
    Construct {
        name: "Identifiers",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Identifiers are case-insensitive whether quoted or not, as in Trino: `\"Name\"` and `name` resolve to the same column. Output aliases are reported in the case they were written (`AS \"Total\"` is column `Total`) and resolve case-insensitively (`ORDER BY total`, `ORDER BY \"TOTAL\"`), also from outer queries.",
        corpus_marker: "\"",
    },
    Construct {
        name: "Numeric literals",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`1` is `INTEGER` (when it fits in 32 bits, else `BIGINT`), `1.5` and `DECIMAL '1.5'` are `DECIMAL(2,1)`, and `1e2` is `DOUBLE`, so `0.1 + 0.2` is exactly `0.3` and `SELECT 1` reports `integer`. A unary minus directly on an integer literal is part of the literal, as in Trino's grammar (`number : MINUS? INTEGER_VALUE #integerLiteral`): `-9223372036854775808` is a `bigint` (not a `decimal(19,0)`) and `-9223372036854775808 - 1` overflows. The same rule makes a digits-only literal **beyond bigint** a parse error — `AstBuilder` builds a `LongLiteral`, whose constructor parses with `Long.parseLong` and raises `Invalid numeric literal: …`, and the `#decimalLiteral` production needs a decimal point — so `SELECT 12345678901234567890` is refused rather than answered as a `decimal(20,0)` (which glaux used to do: a value where Athena refuses the query). Write `DECIMAL '12345678901234567890'` for a decimal of that value. Decimal arithmetic follows Trino's result types and rounding: `+ - *` use Trino's precision/scale with `Decimal overflow` errors past 38 digits, and `/` rounds HALF_UP to `max(s1, s2)` places (`1.5 / 2` is `0.8`, `10.00 / 3` is `3.33`). A double mixed with an exact number (`least(1.5, 2e0)`) is a double — in `coalesce` / `CASE` / `greatest` / `least`, in `ARRAY[...]`, in a `VALUES` column, and across set operations alike. Decimal literals above 38 digits are refused by name.",
        corpus_marker: "0.5",
    },
    Construct {
        name: "Integer overflow",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`integer` and `bigint` `+`, `-`, `*`, and `sum` fail with `NUMERIC_VALUE_OUT_OF_RANGE` on overflow, as in Trino (`2147483647 + 1` overflows the 32-bit `integer` literals; DataFusion alone widens or wraps around). Division by zero is `DIVISION_BY_ZERO`.",
        corpus_marker: "9223372036854775807",
    },
    Construct {
        name: "Operator type checking",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Comparisons, arithmetic (`1 + '2'`), `||`, `LIKE` over non-varchar operands (`1 LIKE '1'`, refused with Trino's \"must evaluate to a varchar\" diagnostic), `IN` (lists and subqueries), `BETWEEN`, join keys, simple `CASE` operands, `CASE` / `if` / `nullif` / `coalesce` / `greatest` / `least` results, and set-operation columns between types Trino does not combine (`varchar = integer`, `'a' || 1`, `date = varchar`) are refused with `TYPE_MISMATCH` instead of being coerced, and the date-part functions (`year`, `date_trunc`, `date_format`, `to_unixtime`, `EXTRACT`) refuse varchar arguments. Numeric types compare with each other and `date` with `timestamp`, as in Trino. The operand types are named the way Trino types them, literals included: `1 = '1'` is `Cannot apply operator: integer = varchar(1)`, not DataFusion's `bigint = varchar`. Function arguments are checked the same way: an argument type Trino has no overload for is `TYPE_MISMATCH: Unexpected parameters (varchar(1)) for function abs` (aggregates included — `sum(varchar)` used to leak DataFusion's `Internal error: Function 'sum' failed to match any signature ...`, which ends in an invitation to file a DataFusion bug report). The boolean contexts follow Trino too: `true AND 1` is `Logical expression term must evaluate to a boolean (actual: bigint)`, `NOT 1` is `Value of logical NOT expression must evaluate to a boolean (actual: bigint)`, and `WHERE 1` is `WHERE clause must evaluate to a boolean: actual type bigint`. A unary `-` over a non-numeric operand is refused as well (DataFusion does not name the operand type there, so the message cannot either), and a window function written without `OVER` is refused by name instead of DataFusion's `Invalid function 'rank'.`. A window function or an aggregate *inside* a `WHERE` (or a window function inside a `HAVING`) is refused at planning with Trino's `EXPRESSION_NOT_SCALAR: WHERE clause cannot contain aggregations, window functions or grouping operations`; DataFusion planned it and then dumped the Rust `Debug` of the window expression from the physical planner. A `SELECT` item that is neither grouped nor aggregated says Trino's `'orders.id' must be an aggregate expression or appear in GROUP BY clause` (DataFusion's text talked about `While expanding wildcard` for queries with no wildcard), and a column alias list of the wrong width says `Column alias list has 1 entries but relation has 2 columns`.",
        corpus_marker: "'1' = 1",
    },
    Construct {
        name: "Runtime errors",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Failures caused by the query's data (an invalid cast, an unparsable date, a bad subscript, an invalid regular expression, a missing regexp group) are user errors (Athena `ErrorCategory` 2) with Trino's error code; only I/O and engine failures are category 1. The codes follow Trino: integer overflow in a kernel is `NUMERIC_VALUE_OUT_OF_RANGE` (`abs(-9223372036854775808)`), `chr` outside the Unicode range and an unparsable `date_parse` input are `INVALID_FUNCTION_ARGUMENT`, and a scalar subquery returning several rows is `SUBQUERY_MULTIPLE_ROWS`. The messages name the reason alone: Arrow's and DataFusion's layer prefixes (`Arrow error: Compute error: `, `Execution error: `) and array-type names are stripped, and division by zero says `Division by zero` whether the operands are integers, doubles, or decimals. Integer casts carry Trino's wording rather than Arrow's: `CAST('1.5' AS INTEGER)` is `Cannot cast '1.5' to integer` and `CAST(2147483648 AS INTEGER)` is `Out of range for integer: 2147483648` (Arrow said `Cannot cast string '1.5' to value of Int32 type` and `Can't cast value ... to type Int32`).",
        corpus_marker: "CAST('abc' AS INTEGER)",
    },
    Construct {
        name: "Timestamp precision",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Every timestamp is a `timestamp(3)`, as on Athena: a scanned column with microsecond or nanosecond values is rounded HALF_UP to milliseconds before any function sees it (so `second(x)` and the printed text agree, and `10:00:00.9996` is `10:00:01.000`), `CAST(varchar AS TIMESTAMP)` rounds the fraction, and `now()` is rounded too. `timestamp with time zone` values (`current_timestamp`, `parse_datetime`, `from_iso8601_timestamp`) are UTC and print as `2024-01-05 10:00:00.000 UTC`; `TIME` values print with milliseconds. Calendar arithmetic (`date_add`, `date_diff`, `date_trunc`) works on the calendar value, so dates outside Arrow's nanosecond window (`DATE '9999-12-31'`, `1583-01-01`) are ordinary values; `date_parse` / `parse_datetime` build the millisecond value themselves, so they have no Arrow nanosecond range limit either (`date_parse('1000-01-01', '%Y-%m-%d')` is an ordinary value).",
        corpus_marker: "TIMESTAMP '",
    },
    Construct {
        name: "DOUBLE / REAL special values (NaN, -0.0)",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Comparisons involving a float operand use Trino's IEEE operators (`DoubleType`: Java's primitive `==`, `<`, …): every `=` / `<` / `<=` / `>` / `>=` with NaN is false, `NaN <> NaN` is true, and `-0.0 = 0.0` is true — where Arrow's kernels use a total order (NaN equal to NaN and above every number). The same routing covers `IN` lists, `BETWEEN`, simple `CASE` operands, `nullif` (`nullif(0e0, -0e0)` is NULL, `nullif(NaN, NaN)` is NaN), and equi-join keys (nested-loop joined). `greatest` / `max` / `array_max` rank NaN smallest (`COMPARISON_UNORDERED_FIRST`), so `max` of `{1.0, NaN}` is `1.0`; the min side needs no substitute. `contains` / `array_position` / `array_remove` match with EQUAL semantics, so NaN is never found or removed. `ORDER BY`, `GROUP BY`, `DISTINCT`, and `array_distinct` group and order NaN like Trino already. Ordering *arrays* with NaN elements (`ARRAY[NaN] < ...`) is refused: Trino's per-element IEEE ordering has no total-order equivalent. The values themselves are written with Trino's constructors, `nan()` and `infinity()` / `-infinity()` (`0e0 / 0e0` and `1e0 / 0e0` are the same values), and print as Java does: `NaN`, `Infinity`, `-Infinity`.",
        corpus_marker: "NaN",
    },
    Construct {
        name: "Read-only statements",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`SELECT`, `WITH`, `VALUES`, `EXPLAIN` (DataFusion's plan text, not Trino's).",
        corpus_marker: "SELECT",
    },
    Construct {
        name: "Quantified comparison (ALL / ANY / SOME)",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`x > ALL (subquery)`, `x = ANY (subquery)`, `x <> SOME (VALUES ...)` — valid Trino — are refused by name on the token stream, before parsing. DataFusion has no equivalent predicate: its planner rewrites the shapes sqlparser parses into `cardinality` / `array_max` / `array_min` / `array_has` over the subquery, which does not reproduce Trino's rules for an empty or NULL-bearing operand (`x > ALL (empty)` is true, `x = ANY (…NULL…)` is NULL rather than false), and it fails naming those internal helpers instead of the predicate. sqlparser cannot parse the `(VALUES 1, 2)` operand at all. Rewrite them: `= ANY` is `IN (subquery)`, `<> ALL` is `NOT IN (subquery)`, and the ordering forms are a comparison against `(SELECT max(y) …)` / `(SELECT min(y) …)` or an `EXISTS` carrying the comparison.",
        corpus_marker: "",
    },
    Construct {
        name: "lambda expression",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`x -> ...` arguments (`transform`, `filter`, `reduce`, comparator sorts) are refused by name.",
        corpus_marker: "",
    },
    Construct {
        name: "AT TIME ZONE",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Refused in v0.1; timestamps are handled as UTC instants.",
        corpus_marker: "",
    },
    Construct {
        name: "timestamp with time zone literal",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`TIMESTAMP '2024-01-05 10:00:00 America/New_York'` (and `Z` / offset suffixes) are refused rather than silently converted to a zone-less UTC instant. Zone-less literals follow Trino's pattern: `TIMESTAMP '2024-01-05'` and `TIMESTAMP '2024-01-05 10:00'` are valid; more than three fractional digits (a `timestamp(4+)` on Trino) are refused, as glaux only carries `timestamp(3)`.",
        corpus_marker: "",
    },
    Construct {
        name: "date subtraction",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`date - date` and `timestamp - timestamp` produce an `interval` in Trino, which glaux cannot return in v0.1; use `date_diff(unit, a, b)`.",
        corpus_marker: "",
    },
    Construct {
        name: "interval result",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "A query whose output column is an `interval` (`INTERVAL '1' DAY + INTERVAL '2' HOUR`) is refused at planning: Athena has no result encoding glaux can reproduce for it.",
        corpus_marker: "",
    },
    Construct {
        name: "binary literal",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`X'1F'` varbinary literals are not supported in v0.1 (and `0x1F` is not Trino syntax at all).",
        corpus_marker: "",
    },
    Construct {
        name: "timestamp with time zone",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Values with a non-UTC zone or offset cannot be represented: `from_iso8601_timestamp` with a non-zero offset, `parse_datetime` with a zone pattern letter, `current_time`, and `AT TIME ZONE` are refused rather than shifted to UTC (which would change `hour(x)` and the printed text).",
        corpus_marker: "",
    },
    Construct {
        name: "WITH RECURSIVE",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Recursive CTEs (valid Trino) are refused by name in v0.1: DataFusion's recursive execution has not been vetted against Trino's semantics (its type unification differs and genuinely recursive queries fail with planner errors). Rewrite the recursion as an explicit `UNION ALL` of the levels.",
        corpus_marker: "",
    },
    Construct {
        name: "Non-Trino syntax",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Syntax DataFusion accepts but Trino does not is refused by name instead of running with DataFusion semantics: `DISTINCT ON`, `QUALIFY`, `GROUP BY ALL`, `ORDER BY ALL`, `TABLESAMPLE`, `FOR UPDATE`, `NATURAL` / `SEMI` / `ANTI` / `APPLY` / `ASOF` joins, `[1, 2]` array literals, `x::type` casts, `TOP`, `SELECT INTO`, `SELECT * EXCLUDE`, `ILIKE`, `IS [NOT] TRUE` / `IS [NOT] FALSE` / `IS [NOT] UNKNOWN`, the operators Trino lacks (`&`, `|`, `^`, `~`, `==`, `<=>`, `->`, and the other PostgreSQL operators; Trino spells these as functions — `regexp_like` for `~` is supported, while the `bitwise_and` / `bitwise_or` / `bitwise_xor` family is not yet in glaux's coverage table and is refused by name), a string literal as an alias (`SELECT 'a' 'b'`), PostgreSQL interval strings (`INTERVAL '1 day'`), `JOIN` without `ON` / `USING`, `FLOAT` as a type name, `ARRAY<T>` type syntax (Trino writes `ARRAY(T)`), `0x1F` literals, and a number glued to identifier characters (`1_000`, which Trino rejects and sqlparser would read as `1 AS _000`).",
        corpus_marker: "",
    },
    Construct {
        name: "ROW / MAP literals and types",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`ROW(...)`, `MAP(...)`, `CAST(... AS ROW(...))` are refused.",
        corpus_marker: "",
    },
    Construct {
        name: "UNNEST",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`CROSS JOIN UNNEST(...)` is refused in v0.1 (Trino's `WITH ORDINALITY` and multi-array forms have no direct DataFusion mapping).",
        corpus_marker: "",
    },
    Construct {
        name: "Multiple statements",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "One statement per query execution, like Athena.",
        corpus_marker: "",
    },
    Construct {
        name: "DDL / DML / CTAS / INSERT / UNLOAD",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Writes arrive in v0.2; refused by statement kind.",
        corpus_marker: "",
    },
];

/// Look up a Trino function by (lower-case) name.
pub fn lookup(name: &str) -> Option<&'static FunctionShim> {
    FUNCTIONS.iter().find(|s| s.name == name)
}

/// Every DataFusion function name the translations emit or pass through.
pub fn datafusion_targets() -> &'static HashSet<&'static str> {
    static TARGETS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
        FUNCTIONS
            .iter()
            .flat_map(|s| s.df_functions.iter().copied())
            .collect()
    });
    &TARGETS
}

const DOC_HEADER: &str = "<!-- GENERATED by glaux-athena's shim registry (crates/glaux-athena/src/dialect/registry.rs).\n     Do not edit by hand: run `GLAUX_REGEN_DOCS=1 cargo test -p glaux-athena coverage_doc`. -->\n";

/// Render `docs/sql-coverage.md` from the registry.
pub fn render_coverage() -> String {
    let mut out = String::new();
    out.push_str(DOC_HEADER);
    out.push_str("# glaux Athena SQL coverage\n\n");
    out.push_str(
        "glaux executes Athena (Trino-dialect) SQL by translating it onto Apache DataFusion. \
         This table is generated from the shim registry that drives the translator, so it is \
         exactly what the engine accepts: anything not listed is refused with an error naming \
         the construct — never silently approximated.\n\n",
    );
    let (supported, refused): (Vec<&FunctionShim>, Vec<&FunctionShim>) = FUNCTIONS
        .iter()
        .partition(|s| s.kind != ShimKind::Unsupported);
    let _ = writeln!(
        out,
        "**Functions:** {} supported ({} passthrough, {} rewritten, {} Rust UDFs), {} refused by name.\n",
        supported.len(),
        supported
            .iter()
            .filter(|s| s.kind == ShimKind::Passthrough)
            .count(),
        supported
            .iter()
            .filter(|s| s.kind == ShimKind::Rewrite)
            .count(),
        supported.iter().filter(|s| s.kind == ShimKind::Udf).count(),
        refused.len(),
    );

    out.push_str("## SQL constructs\n\n");
    out.push_str("| Construct | Category | Status | Notes |\n|---|---|---|---|\n");
    for c in CONSTRUCTS {
        let status = match c.status {
            ConstructStatus::Supported => "supported",
            ConstructStatus::Unsupported => "refused",
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            escape(c.name),
            c.category,
            status,
            escape(c.notes)
        );
    }

    out.push_str("\n## Functions\n");
    let mut categories: Vec<&str> = Vec::new();
    for s in FUNCTIONS {
        if !categories.contains(&s.category) {
            categories.push(s.category);
        }
    }
    for category in categories {
        let _ = write!(out, "\n### {category}\n\n");
        out.push_str(
            "| Function | Trino signature | Handling | Translation |\n|---|---|---|---|\n",
        );
        for s in FUNCTIONS.iter().filter(|s| s.category == category) {
            let _ = writeln!(
                out,
                "| `{}` | `{}` | {} | {} |",
                s.name,
                s.signature,
                s.kind.label(),
                escape(s.translation)
            );
        }
    }
    out.push_str(
        "\n## Legend\n\n\
         - **passthrough** — same name and semantics in DataFusion; the call is handed over unchanged.\n\
         - **rewrite** — rewritten at the AST level onto DataFusion functions or syntax.\n\
         - **udf** — implemented in Rust inside glaux and registered under the Trino name.\n\
         - **unsupported** — recognised and refused with an explicit error naming the function.\n\n\
         Functions not in this table are refused with `FUNCTION_NOT_FOUND`, even when DataFusion \
         has a function of the same name, because same-named functions with different semantics \
         are the main way an emulator becomes silently wrong.\n",
    );
    out
}

fn escape(s: &str) -> String {
    s.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_names_are_unique_lowercase_and_sorted_within_category() {
        let mut seen = HashSet::new();
        for s in FUNCTIONS {
            assert_eq!(
                s.name,
                s.name.to_lowercase(),
                "{} must be lower-case",
                s.name
            );
            assert!(seen.insert(s.name), "duplicate registry entry {}", s.name);
            match s.kind {
                ShimKind::Unsupported => assert!(
                    s.df_functions.is_empty(),
                    "{} is unsupported but lists targets",
                    s.name
                ),
                ShimKind::Passthrough | ShimKind::Udf => assert!(
                    !s.df_functions.is_empty(),
                    "{} must name the DataFusion function it maps to",
                    s.name
                ),
                ShimKind::Rewrite => {}
            }
        }
        let mut by_category: Vec<(&str, Vec<&str>)> = Vec::new();
        for s in FUNCTIONS {
            match by_category.last_mut() {
                Some((c, names)) if *c == s.category => names.push(s.name),
                _ => by_category.push((s.category, vec![s.name])),
            }
        }
        for (category, names) in by_category {
            let mut sorted = names.clone();
            sorted.sort_unstable();
            assert_eq!(names, sorted, "category {category} is not sorted by name");
        }
        assert_eq!(lookup("date_add").map(|s| s.kind), Some(ShimKind::Udf));
        assert!(lookup("frobnicate").is_none());
    }

    #[test]
    fn coverage_doc_lists_every_entry() {
        let doc = render_coverage();
        for s in FUNCTIONS {
            assert!(
                doc.contains(&format!("| `{}` |", s.name)),
                "{} missing",
                s.name
            );
        }
        for c in CONSTRUCTS {
            assert!(doc.contains(&escape(c.name)), "{} missing", c.name);
        }
        assert!(doc.starts_with(DOC_HEADER));
    }
}
